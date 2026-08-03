//! EmailConfig Node - Email provider credentials and server settings (IMAP/SMTP)

use async_trait::async_trait;
use crate::node::{Node, NodeMetadata, NodeFeatures, PortDef, ExecutionContext, FieldDef};
use crate::{NodeResult, register_node};

#[derive(Default)]
pub struct EmailConfigNode;

#[async_trait]
impl Node for EmailConfigNode {
    fn node_type(&self) -> &'static str {
        "EmailConfig"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "Email Config",
            inputs: vec![],
            outputs: vec![
                PortDef::new("config", "Dict[String, String]", false),
            ],
            features: NodeFeatures {
                ..Default::default()
            },
            fields: vec![
                FieldDef::select("protocol", vec!["imap", "smtp"]),
                FieldDef::text("host"),
                FieldDef::text("port"),
                FieldDef::select("security", vec!["tls", "starttls", "none"]),
                FieldDef::text("username"),
                FieldDef::password("password"),
                FieldDef::text("mailbox"),
                FieldDef::select("tlsAcceptInvalid", vec!["false", "true"]),
            ],
        }
    }

    async fn execute(&self, ctx: ExecutionContext) -> NodeResult {
        NodeResult::completed(serde_json::json!({ "config": ctx.config }))
    }
}

register_node!(EmailConfigNode);
