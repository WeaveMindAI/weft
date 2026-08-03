//! WhatsAppGroupKick Node - Remove participants from a WhatsApp group.

use async_trait::async_trait;
use crate::node::{Node, NodeMetadata, NodeFeatures, PortDef, ExecutionContext};
use crate::{NodeResult, register_node};
use weft_core::sidecar::ActionRequest;

pub struct WhatsAppGroupKickNode;

#[async_trait]
impl Node for WhatsAppGroupKickNode {
    fn node_type(&self) -> &'static str {
        "WhatsAppGroupKick"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "WhatsApp Group Kick",
            inputs: vec![
                PortDef::new("endpointUrl", "String", true),
                PortDef::new("groupId", "String", true),
                PortDef::new("participants", "List[String]", true),
            ],
            outputs: vec![
                PortDef::new("success", "Boolean", false),
            ],
            features: NodeFeatures { ..Default::default() },
            fields: vec![],
        }
    }

    async fn execute(&self, ctx: ExecutionContext) -> NodeResult {
        let endpoint_url = match ctx.input.get("endpointUrl").and_then(|v| v.as_str()) {
            Some(url) if !url.is_empty() => url,
            _ => return NodeResult::failed("No endpointUrl provided. Connect a WhatsAppBridge node."),
        };
        let group_id = ctx.input.get("groupId").and_then(|v| v.as_str()).unwrap_or("");
        let participants = ctx.input.get("participants").cloned().unwrap_or(serde_json::json!([]));

        if group_id.is_empty() { return NodeResult::failed("Group ID is required"); }

        let action_req = ActionRequest {
            action: "groupKick".to_string(),
            payload: serde_json::json!({ "groupId": group_id, "participants": participants }),
        };

        let client = reqwest::Client::new();
        let resp = client.post(endpoint_url).json(&action_req)
            .timeout(std::time::Duration::from_secs(30)).send().await;

        match resp {
            Ok(r) if r.status().is_success() => {
                NodeResult::completed(serde_json::json!({ "success": true }))
            }
            Ok(r) => {
                let text = r.text().await.unwrap_or_default();
                NodeResult::completed(serde_json::json!({ "success": false, "error": text }))
            }
            Err(e) => NodeResult::failed(&format!("Failed to reach bridge: {}", e)),
        }
    }
}

register_node!(WhatsAppGroupKickNode);
