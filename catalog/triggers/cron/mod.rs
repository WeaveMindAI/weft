//! Cron: fires an execution on a schedule. The actual schedule
//! evaluation happens in the dispatcher (cron ticker hits the
//! entry token when the time comes); the node itself just emits
//! the fire timestamp on activation.

use async_trait::async_trait;
use serde_json::Value;

use weft_core::{ExecutionContext, Node, NodeMetadata, WeftResult};
use weft_core::node::NodeOutput;

pub struct CronNode;

const METADATA_JSON: &str = include_str!("metadata.json");

#[async_trait]
impl Node for CronNode {
    fn node_type(&self) -> &'static str {
        "Cron"
    }

    fn metadata(&self) -> NodeMetadata {
        serde_json::from_str(METADATA_JSON).expect("Cron metadata.json must be valid")
    }

    async fn execute(&self, ctx: ExecutionContext) -> WeftResult<NodeOutput> {
        let fired_at = ctx
            .input
            .raw("firedAt")
            .cloned()
            .unwrap_or_else(|| Value::String(chrono::Utc::now().to_rfc3339()));
        Ok(NodeOutput::empty().set("firedAt", fired_at))
    }
}
