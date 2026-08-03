//! WhatsAppCreateGroup Node - Create a new WhatsApp group.

use async_trait::async_trait;
use crate::node::{Node, NodeMetadata, NodeFeatures, PortDef, ExecutionContext};
use crate::{NodeResult, register_node};
use weft_core::sidecar::ActionRequest;

pub struct WhatsAppCreateGroupNode;

#[async_trait]
impl Node for WhatsAppCreateGroupNode {
    fn node_type(&self) -> &'static str {
        "WhatsAppCreateGroup"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "WhatsApp Create Group",
            inputs: vec![
                PortDef::new("endpointUrl", "String", true),
                PortDef::new("name", "String", true),
                PortDef::new("participants", "List[String]", true),
            ],
            outputs: vec![
                PortDef::new("groupId", "String", false),
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
        let name = ctx.input.get("name").and_then(|v| v.as_str()).unwrap_or("");
        let participants = ctx.input.get("participants").cloned().unwrap_or(serde_json::json!([]));

        if name.is_empty() { return NodeResult::failed("Group name is required"); }

        let action_req = ActionRequest {
            action: "createGroup".to_string(),
            payload: serde_json::json!({ "name": name, "participants": participants }),
        };

        let client = reqwest::Client::new();
        let resp = client.post(endpoint_url).json(&action_req)
            .timeout(std::time::Duration::from_secs(30)).send().await;

        match resp {
            Ok(r) if r.status().is_success() => {
                let body: serde_json::Value = r.json().await.unwrap_or_default();
                let result = body.get("result").cloned().unwrap_or_default();
                let group_id = result.get("groupId").and_then(|v| v.as_str()).unwrap_or("").to_string();
                NodeResult::completed(serde_json::json!({ "groupId": group_id, "success": true }))
            }
            Ok(r) => {
                let text = r.text().await.unwrap_or_default();
                NodeResult::failed(&format!("Bridge error: {}", text))
            }
            Err(e) => NodeResult::failed(&format!("Failed to reach bridge: {}", e)),
        }
    }
}

register_node!(WhatsAppCreateGroupNode);
