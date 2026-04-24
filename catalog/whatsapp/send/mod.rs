//! WhatsAppSend: POSTs a `sendMessage` action to the project's
//! WhatsApp bridge sidecar. Pure Fire-phase; no registration or
//! infra lifecycle of its own.

use async_trait::async_trait;

use weft_core::error::WeftError;
use weft_core::node::NodeOutput;
use weft_core::{ExecutionContext, Node, NodeMetadata, WeftResult};

pub struct WhatsAppSendNode;

const METADATA_JSON: &str = include_str!("metadata.json");

#[async_trait]
impl Node for WhatsAppSendNode {
    fn node_type(&self) -> &'static str {
        "WhatsAppSend"
    }

    fn metadata(&self) -> NodeMetadata {
        serde_json::from_str(METADATA_JSON).expect("WhatsAppSend metadata.json must be valid")
    }

    async fn execute(&self, ctx: ExecutionContext) -> WeftResult<NodeOutput> {
        let endpoint_url = ctx
            .input
            .values
            .get("endpointUrl")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                WeftError::Input(
                    "endpointUrl is required (connect a WhatsAppBridge output)".into(),
                )
            })?
            .to_string();
        let to = ctx
            .input
            .values
            .get("to")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .ok_or_else(|| WeftError::Input("'to' is required".into()))?;
        let message = ctx
            .input
            .values
            .get("message")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .ok_or_else(|| WeftError::Input("'message' is required".into()))?;

        let body = serde_json::json!({
            "action": "sendMessage",
            "payload": { "to": to, "text": message },
        });
        // The bridge sidecar mounts the action router at `/action`.
        // `endpointUrl` from WhatsAppBridge is the bare service
        // DNS (http://<svc>.<ns>.svc.cluster.local:PORT) so we
        // append the path here. Trim to keep idempotent even if
        // a future version starts emitting an already-suffixed URL.
        let action_url = {
            let base = endpoint_url.trim_end_matches('/').trim_end_matches("/action");
            format!("{base}/action")
        };
        let resp = reqwest::Client::new()
            .post(&action_url)
            .json(&body)
            .timeout(std::time::Duration::from_secs(30))
            .send()
            .await
            .map_err(|e| WeftError::NodeExecution(format!("POST {action_url}: {e}")))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(WeftError::NodeExecution(format!(
                "bridge returned {status}: {text}"
            )));
        }
        let parsed: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| WeftError::NodeExecution(format!("parse bridge response: {e}")))?;
        let result = parsed.get("result").cloned().unwrap_or_default();
        if let Some(err) = result.get("error").and_then(|v| v.as_str()) {
            return Ok(NodeOutput::empty()
                .set("messageId", serde_json::Value::String(String::new()))
                .set("success", serde_json::Value::Bool(false))
                .set("error", serde_json::Value::String(err.to_string())));
        }
        let message_id = result
            .get("messageId")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        Ok(NodeOutput::empty()
            .set("messageId", serde_json::Value::String(message_id))
            .set("success", serde_json::Value::Bool(true)))
    }
}
