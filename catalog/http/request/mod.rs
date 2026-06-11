//! HttpRequest: generic outbound HTTP client. Enough for REST APIs
//! that don't need custom auth plumbing (those get their own node).

use std::collections::HashMap;

use async_trait::async_trait;
use reqwest::Method;
use serde_json::Value;

use weft_core::{ExecutionContext, Node, NodeMetadata, WeftResult};
use weft_core::error::WeftError;
use weft_core::node::NodeOutput;

pub struct HttpRequestNode;

const METADATA_JSON: &str = include_str!("metadata.json");

#[async_trait]
impl Node for HttpRequestNode {
    fn node_type(&self) -> &'static str {
        "HttpRequest"
    }

    fn metadata(&self) -> NodeMetadata {
        serde_json::from_str(METADATA_JSON).expect("HttpRequest metadata.json must be valid")
    }

    async fn execute(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let url: String = ctx.input.get("url")?;
        let method_str: String = ctx.config.get("method")?;
        let method = Method::from_bytes(method_str.as_bytes())
            .map_err(|e| WeftError::Config(format!("bad method '{method_str}': {e}")))?;

        let body: Option<Value> = ctx.input.get_optional("body")?;
        let headers: Option<HashMap<String, String>> = ctx.input.get_optional("headers")?;

        let client = reqwest::Client::new();
        let mut req = client.request(method, &url);
        if let Some(map) = headers {
            for (k, v) in map {
                req = req.header(k, v);
            }
        }
        if let Some(b) = body {
            req = req.json(&b);
        }

        let resp = req
            .send()
            .await
            .map_err(|e| WeftError::NodeExecution(format!("http send: {e}")))?;
        let status = resp.status();
        // Decode the body as text, then attempt JSON. The `body`
        // output is declared `JsonDict | String`, so the value we emit
        // must be exactly one of those: a JSON OBJECT stays a dict; a
        // non-object (array, scalar, or non-JSON text) is surfaced as
        // its verbatim string. This keeps the declared type honest so
        // a downstream consumer's runtime type check never vetoes a
        // legitimate response (the earlier `JsonDict`-only declaration
        // nulled every non-object body).
        let raw_body = resp
            .text()
            .await
            .map_err(|e| WeftError::NodeExecution(format!("http body read: {e}")))?;
        let response_body = match serde_json::from_str::<Value>(&raw_body) {
            Ok(v @ Value::Object(_)) => v,
            // Valid JSON but not an object, or not JSON at all: emit the
            // raw text. The String arm of the union covers it.
            _ => Value::String(raw_body),
        };

        ctx.pulse_downstream(NodeOutput::empty()
            .set("status", Value::from(status.as_u16()))
            .set("body", response_body)
            .set("ok", Value::from(status.is_success()))).await
    }
}
