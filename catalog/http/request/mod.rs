//! HttpRequest: generic outbound HTTP client. Enough for REST APIs
//! whose auth is one key in one header (the optional `connection`);
//! anything richer gets its own node.

use std::collections::HashMap;

use async_trait::async_trait;
use reqwest::header::{CONTENT_DISPOSITION, CONTENT_TYPE};
use reqwest::Method;
use serde_json::Value;

use weft::storage::{self, diff::is_text_mime, KeepTtl, StorageScope};
use weft::{Access, ExecutionContext, Node, NodeErrExt, NodeManifest, WeftError, WeftResult};
use weft::node::NodeOutput;

#[derive(NodeManifest)]
pub struct HttpRequestNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for HttpRequestNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        // A failure is no answer at all or an unreadable body. An answer
        // with any status is a success: `status` and `ok` say it.
        let url: String = ctx.inputs.get("url")?;
        let method_str: String = ctx.inputs.get("method")?;
        let method = Method::from_bytes(method_str.as_bytes())
            .map_err(|_| WeftError::Input(format!("bad method '{method_str}'")))?;

        let body: Option<Value> = ctx.inputs.opt("body")?;
        let headers: Option<HashMap<String, String>> = ctx.inputs.opt("headers")?;

        // A connection carries a key and the header it goes in, so a
        // key never sits in plain `headers` (which travel the journal
        // and show in the inspector). Its recipe sets the header itself;
        // the node reads only the non-secret header NAME, to refuse the
        // same header written twice.
        let connection: Option<Access> = ctx.inputs.opt("connection")?;
        let (client, auth_header) = match &connection {
            Some(access) => {
                let opened = ctx.open(access).await?;
                (opened.client().clone(), Some(opened.value("header")?.to_string()))
            }
            None => (ctx.http(), None),
        };

        let mut req = client.request(method, &url);
        if let Some(map) = headers {
            for (k, v) in map {
                if auth_header.as_deref().is_some_and(|h| h.eq_ignore_ascii_case(&k)) {
                    return Err(WeftError::Input(format!(
                        "'{k}' is set both in `headers` and by the connection; drop it from `headers`"
                    )));
                }
                req = req.header(k, v);
            }
        }
        if let Some(b) = body {
            req = req.json(&b);
        }

        let resp = req.send().await.node_err("http send")?;
        let status = resp.status();
        let out = NodeOutput::new().set("status", status.as_u16()).set("ok", status.is_success());
        let content_type = resp.headers().get(CONTENT_TYPE).and_then(|v| v.to_str().ok()).map(str::to_string);
        let filename = resp
            .headers()
            .get(CONTENT_DISPOSITION)
            .and_then(|v| v.to_str().ok())
            .and_then(storage::filename_from_disposition)
            .unwrap_or_else(|| storage::filename_from_url(&url));
        let files = ctx.storage(StorageScope::Execution);
        let out = match content_type {
            // A body that says it is not text is bytes, and decoding it
            // as text would corrupt it: it is stored whole, as a file.
            Some(ct) if !is_text_mime(&ct) => {
                let mime = storage::normalize_content_type(Some(&ct));
                let file = files
                    .put_stream(storage::response_stream(resp), &mime, &filename, Some(KeepTtl::Default))
                    .await?;
                out.set("file", file)
            }
            Some(_) => out.set("body", text_body(resp.text().await.node_err("http body read")?)),
            // A body that names no type is text when it reads as text,
            // and a file otherwise, typed by its first bytes.
            None => {
                let bytes = resp.bytes().await.node_err("http body read")?;
                match std::str::from_utf8(&bytes) {
                    Ok(text) => out.set("body", text_body(text.to_string())),
                    Err(_) => {
                        let mime = storage::sniff_mime(&bytes).unwrap_or("application/octet-stream");
                        out.set("file", files.put(bytes, mime, &filename, Some(KeepTtl::Default)).await?)
                    }
                }
            }
        };
        ctx.pulse_downstream(out).await
    }
}

/// A text body as the `body` output, declared `JsonDict | String`: a
/// JSON OBJECT stays a dict; anything else (an array, a scalar, text
/// that is not JSON) is the verbatim string.
fn text_body(raw: String) -> Value {
    match serde_json::from_str::<Value>(&raw) {
        Ok(v @ Value::Object(_)) => v,
        _ => Value::String(raw),
    }
}
