use async_trait::async_trait;
use serde_json::Value;

use weft_core::node::NodeOutput;
use weft_core::{ExecutionContext, Node, NodeMetadata, WeftResult};

use super::form_helpers::{map_response_to_ports, parse_form_fields};

pub struct HumanTriggerNode;

const METADATA_JSON: &str = include_str!("metadata.json");

#[async_trait]
impl Node for HumanTriggerNode {
    fn node_type(&self) -> &'static str {
        "HumanTrigger"
    }

    fn metadata(&self) -> NodeMetadata {
        serde_json::from_str(METADATA_JSON).expect("HumanTrigger metadata.json must be valid")
    }

    async fn execute(&self, ctx: ExecutionContext) -> WeftResult<NodeOutput> {
        // Trigger nodes receive the firing payload on their
        // `__seed__` port. The dispatcher wraps the form submission
        // as `{ body: <submission> }` (webhook-fire conventions);
        // the submission object lives under `body`.
        let payload = ctx
            .input
            .values
            .get("__seed__")
            .cloned()
            .unwrap_or(Value::Null);
        let submission = payload
            .get("body")
            .cloned()
            .unwrap_or(payload);
        let raw_fields = parse_form_fields(&ctx.config.values);
        Ok(map_response_to_ports(&submission, &raw_fields))
    }
}
