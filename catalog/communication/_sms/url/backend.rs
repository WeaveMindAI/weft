//! SMS URL Node - Generate sms: links

use async_trait::async_trait;
use crate::node::{Node, NodeMetadata, NodeFeatures, PortDef, ExecutionContext};
use crate::{NodeResult, register_node};

#[derive(Default)]
pub struct SmsUrlNode;

#[async_trait]
impl Node for SmsUrlNode {
    fn node_type(&self) -> &'static str {
        "SmsUrl"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "SMS URL",
            inputs: vec![
                PortDef::new("phone", "String", true),
                PortDef::new("message", "String", true),
            ],
            outputs: vec![
                PortDef::new("url", "String", false),
            ],
            features: NodeFeatures {
                ..Default::default()
            },
            fields: vec![],
        }
    }

    async fn execute(&self, ctx: ExecutionContext) -> NodeResult {
        let phone = ctx.input.get("phone")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        let message = ctx.input.get("message")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        // Clean phone number - keep only digits and +
        let clean_phone: String = phone.chars()
            .filter(|c| c.is_ascii_digit() || *c == '+')
            .collect();

        let mut url = format!("sms:{}", clean_phone);
        
        if !message.is_empty() {
            url.push_str(&format!("?body={}", urlencoding::encode(message)));
        }

        NodeResult::completed(serde_json::json!({
            "url": url,
        }))
    }
}

register_node!(SmsUrlNode);
