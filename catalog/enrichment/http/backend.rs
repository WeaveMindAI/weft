//! HTTP Node - Make HTTP requests

use async_trait::async_trait;
use crate::node::{Node, NodeMetadata, NodeFeatures, PortDef, ExecutionContext, FieldDef};
use crate::{NodeResult, register_node};

/// HTTP node for making HTTP requests.
#[derive(Default)]
pub struct HttpNode;

#[async_trait]
impl Node for HttpNode {
    fn node_type(&self) -> &'static str {
        "Http"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "HTTP",
            inputs: vec![
                PortDef::new("body", "JsonDict", false),
                PortDef::new("headers", "Dict[String, String]", false),
            ],
            outputs: vec![
                PortDef::new("body", "String", false),
                PortDef::new("status", "Number", false),
                PortDef::new("success", "Boolean", false),
            ],
            features: NodeFeatures {
                ..Default::default()
            },
            fields: vec![
                FieldDef::text("url"),
                FieldDef::select("method", vec!["GET", "POST", "PUT", "DELETE"]),
            ],
        }
    }

    async fn execute(&self, ctx: ExecutionContext) -> NodeResult {
        let url = ctx.config.get("url")
            .or_else(|| ctx.input.get("url"))
            .and_then(|v| v.as_str())
            .unwrap_or("https://httpbin.org/get");

        let method = ctx.config.get("method")
            .and_then(|v| v.as_str())
            .unwrap_or("GET");

        tracing::info!("HTTP request: {} {}", method, url);

        let mut header_map = reqwest::header::HeaderMap::new();
        if let Some(headers_obj) = ctx.input.get("headers").and_then(|v| v.as_object()) {
            for (key, value) in headers_obj {
                if let (Ok(name), Some(val)) = (
                    reqwest::header::HeaderName::from_bytes(key.as_bytes()),
                    value.as_str().and_then(|v| reqwest::header::HeaderValue::from_str(v).ok()),
                ) {
                    header_map.insert(name, val);
                }
            }
        }

        let client = &ctx.http_client;

        let result = match method.to_uppercase().as_str() {
            "POST" => {
                let body = ctx.input.get("body").cloned().unwrap_or(serde_json::json!({}));
                client.post(url).headers(header_map).json(&body).send().await
            }
            "PUT" => {
                let body = ctx.input.get("body").cloned().unwrap_or(serde_json::json!({}));
                client.put(url).headers(header_map).json(&body).send().await
            }
            "DELETE" => client.delete(url).headers(header_map).send().await,
            _ => client.get(url).headers(header_map).send().await,
        };

        match result {
            Ok(response) => {
                let status = response.status().as_u16();
                let success = (200..300).contains(&status);
                let body = response.text().await.unwrap_or_default();
                NodeResult::completed(serde_json::json!({
                    "body": body,
                    "status": status,
                    "success": success,
                }))
            }
            Err(e) => {
                tracing::error!("HTTP error: {}", e);
                NodeResult::failed(&e.to_string())
            }
        }
    }
}

register_node!(HttpNode);
