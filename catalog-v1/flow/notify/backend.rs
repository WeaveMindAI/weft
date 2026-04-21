//! Notify Node - Send a URL to the extension for the user to open
//!
//! Simple fire-and-forget node that sends a URL to the extension.
//! The user clicks on it to open the link. No approve/reject needed.

use async_trait::async_trait;
use crate::node::{Node, NodeMetadata, NodeFeatures, PortDef, ExecutionContext};
use crate::{NodeResult, register_node};

#[derive(Default)]
pub struct NotifyNode;

#[async_trait]
impl Node for NotifyNode {
    fn node_type(&self) -> &'static str {
        "Notify"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "Send URL",
            inputs: vec![
                PortDef::new("url", "String", true),
            ],
            outputs: vec![
                PortDef::new("sent", "Boolean", false),
            ],
            features: NodeFeatures {
                ..Default::default()
            },
            fields: vec![],
        }
    }

    async fn execute(&self, ctx: ExecutionContext) -> NodeResult {
        let url = ctx.input.get("url")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        if url.is_empty() {
            return NodeResult::failed("URL is required");
        }

        let mut output = serde_json::json!({ "sent": true });
        let action = ctx.notify_action(url);
        // Merge the action into output
        if let (Some(out_obj), Some(action_obj)) = (output.as_object_mut(), action.as_object()) {
            for (k, v) in action_obj {
                out_obj.insert(k.clone(), v.clone());
            }
        }

        NodeResult::completed(output)
    }
}

register_node!(NotifyNode);
