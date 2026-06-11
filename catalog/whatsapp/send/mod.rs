//! WhatsAppSend: POSTs a `sendMessage` action to the project's
//! WhatsApp bridge. Pure Fire-phase; no registration or infra
//! lifecycle of its own.

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

    async fn execute(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let endpoint_url = ctx
            .input
            .required_str("endpointUrl", "endpointUrl (connect a WhatsAppBridge output)")?;
        let to = ctx.input.required_str("to", "'to' recipient")?;
        let message = ctx.input.required_str("message", "'message' body")?;

        let body = serde_json::json!({
            "action": "sendMessage",
            "payload": { "to": to, "text": message },
        });
        // The bridge mounts the action router at `/action`. The
        // contract for `endpointUrl` (WhatsAppBridge's output) is
        // bare service DNS, so we append the path here. No
        // defensive trim: a contract violation should surface, not
        // be papered over.
        let action_url = format!("{}/action", endpoint_url.trim_end_matches('/'));
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
        // The bridge signals SOFT failures (e.g. "WhatsApp not
        // connected" when the phone isn't paired) as a 200 with
        // `result.error` (only THROWN errors become non-2xx, handled
        // above). Surface that reason loudly instead of letting it fall
        // through to a misleading "missing messageId".
        let result = parsed.get("result");
        if let Some(err) = result.and_then(|r| r.get("error")).and_then(|v| v.as_str()) {
            return Err(WeftError::NodeExecution(format!("bridge: {err}")));
        }
        let message_id = result
            .and_then(|r| r.get("messageId"))
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                WeftError::NodeExecution(format!(
                    "bridge send response missing result.messageId: {parsed}"
                ))
            })?;
        // Only emit `messageId`. The previous `success: true` port was
        // an always-true constant (every failure path errors above), so
        // its mere presence on the wire was the meaningful signal. The
        // `messageId` emission already conveys "send succeeded"; if a
        // user wires a downstream `success` consumer they wire it
        // against `messageId` instead.
        ctx.pulse_downstream(NodeOutput::empty()
            .set("messageId", serde_json::Value::String(message_id.to_string()))).await
    }
}
