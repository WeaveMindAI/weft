//! Cron: fires an execution on a schedule. When triggered, the
//! dispatcher populates `scheduledTime` (when the tick was due) and
//! `actualTime` (when it actually fired, useful for lag analysis)
//! on the input bag; we pass those through to the outputs.

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
        let now = chrono::Utc::now().to_rfc3339();
        let scheduled = ctx
            .input
            .raw("scheduledTime")
            .cloned()
            .unwrap_or_else(|| Value::String(now.clone()));
        let actual = ctx
            .input
            .raw("actualTime")
            .cloned()
            .unwrap_or(Value::String(now));
        Ok(NodeOutput::empty()
            .set("scheduledTime", scheduled)
            .set("actualTime", actual))
    }
}
