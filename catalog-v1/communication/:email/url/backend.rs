//! Email URL Node - Generate mailto links

use async_trait::async_trait;
use crate::node::{Node, NodeMetadata, NodeFeatures, PortDef, ExecutionContext};
use crate::{NodeResult, register_node};

#[derive(Default)]
pub struct EmailUrlNode;

#[async_trait]
impl Node for EmailUrlNode {
    fn node_type(&self) -> &'static str {
        "EmailUrl"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "Email URL",
            inputs: vec![
                PortDef::new("to", "String", true),
                PortDef::new("subject", "String", true),
                PortDef::new("body", "String", true),
                PortDef::new("cc", "String", false),
                PortDef::new("bcc", "String", false),
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
        let to = ctx.input.get("to")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        let subject = ctx.input.get("subject")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        let body = ctx.input.get("body")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        let cc = ctx.input.get("cc")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        let bcc = ctx.input.get("bcc")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        let mut url = format!("mailto:{}", urlencoding::encode(to));
        let mut params = Vec::new();
        
        if !subject.is_empty() {
            params.push(format!("subject={}", urlencoding::encode(subject)));
        }
        if !body.is_empty() {
            params.push(format!("body={}", urlencoding::encode(body)));
        }
        if !cc.is_empty() {
            params.push(format!("cc={}", urlencoding::encode(cc)));
        }
        if !bcc.is_empty() {
            params.push(format!("bcc={}", urlencoding::encode(bcc)));
        }
        
        if !params.is_empty() {
            url.push('?');
            url.push_str(&params.join("&"));
        }

        NodeResult::completed(serde_json::json!({
            "url": url,
        }))
    }
}

register_node!(EmailUrlNode);
