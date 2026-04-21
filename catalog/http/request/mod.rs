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

    async fn execute(&self, ctx: ExecutionContext) -> WeftResult<NodeOutput> {
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
        let response_body: Value = resp
            .json()
            .await
            .unwrap_or(Value::Null); // Non-JSON response => null body, still return status.

        Ok(NodeOutput::empty()
            .set("status", Value::from(status.as_u16()))
            .set("body", response_body)
            .set("ok", Value::from(status.is_success())))
    }
}
