use async_trait::async_trait;

use weft_core::node::NodeOutput;
use weft_core::primitive::{FormSchema, WakeSignalKind, WakeSignalSpec};
use weft_core::{ExecutionContext, Node, NodeMetadata, WeftResult};

use super::form_helpers::{build_runtime_field, map_response_to_ports, parse_form_fields};

pub struct HumanQueryNode;

const METADATA_JSON: &str = include_str!("metadata.json");

#[async_trait]
impl Node for HumanQueryNode {
    fn node_type(&self) -> &'static str {
        "HumanQuery"
    }

    fn metadata(&self) -> NodeMetadata {
        serde_json::from_str(METADATA_JSON).expect("HumanQuery metadata.json must be valid")
    }

    async fn execute(&self, ctx: ExecutionContext) -> WeftResult<NodeOutput> {
        let raw_fields = parse_form_fields(&ctx.config.values);

        let title = ctx
            .config
            .values
            .get("title")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let description = ctx
            .config
            .values
            .get("description")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let schema = FormSchema {
            title: title.clone(),
            description: description.clone(),
            fields: raw_fields.iter().filter_map(build_runtime_field).collect(),
        };

        let spec = WakeSignalSpec {
            kind: WakeSignalKind::Form {
                form_type: "human-query".to_string(),
                schema,
                title: if title.is_empty() { None } else { Some(title) },
                description,
            },
            is_resume: true,
        };
        let submission = ctx.await_signal(spec).await?;
        Ok(map_response_to_ports(&submission, &raw_fields))
    }
}
