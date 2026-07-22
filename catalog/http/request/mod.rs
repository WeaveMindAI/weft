//! HttpRequest: generic outbound HTTP client. Enough for REST APIs
//! that don't need custom auth plumbing (those get their own node).

use std::collections::HashMap;

use async_trait::async_trait;
use reqwest::Method;
use serde_json::Value;

use weft_core::{ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};
use weft_core::node::NodeOutput;

#[derive(NodeManifest)]
pub struct HttpRequestNode;

#[async_trait]
impl Node for HttpRequestNode {
    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let url: String = ctx.ports.get("url")?;
        let method_str: String = ctx.config.get("method")?;
        let method = Method::from_bytes(method_str.as_bytes())
            .node_err(format!("bad method '{method_str}'"))?;

        let body: Option<Value> = ctx.ports.opt("body")?;
        let headers: Option<HashMap<String, String>> = ctx.ports.opt("headers")?;

        let mut req = ctx.http().request(method, &url);
        if let Some(map) = headers {
            for (k, v) in map {
                req = req.header(k, v);
            }
        }
        if let Some(b) = body {
            req = req.json(&b);
        }

        let resp = req.send().await.node_err("http send")?;
        let status = resp.status();
        // Decode the body as text, then attempt JSON. The `body`
        // output is declared `JsonDict | String`, so the value we emit
        // must be exactly one of those: a JSON OBJECT stays a dict; a
        // non-object (array, scalar, or non-JSON text) is surfaced as
        // its verbatim string. This keeps the declared type honest so
        // a downstream consumer's runtime type check never vetoes a
        // legitimate response (the earlier `JsonDict`-only declaration
        // nulled every non-object body).
        let raw_body = resp.text().await.node_err("http body read")?;
        let response_body = match serde_json::from_str::<Value>(&raw_body) {
            Ok(v @ Value::Object(_)) => v,
            // Valid JSON but not an object, or not JSON at all: emit the
            // raw text. The String arm of the union covers it.
            _ => Value::String(raw_body),
        };

        ctx.pulse_downstream(NodeOutput::new()
            .set("status", status.as_u16())
            .set("body", response_body)
            .set("ok", status.is_success())).await
    }
}
