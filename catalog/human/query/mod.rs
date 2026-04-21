//! HumanQuery: ask a human a question mid-execution, wait for their
//! response. The single demonstration of `ctx.await_form` in the
//! scaffold, exercises the entire suspend-and-resume path.

use async_trait::async_trait;
use serde_json::Value;

use weft_core::{ExecutionContext, FormSchema, Node, NodeMetadata, WeftResult};
use weft_core::node::NodeOutput;

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
        let schema: FormSchema = ctx.config.get("formSchema")?;

        // await_form journals the suspension. The worker may exit while
        // waiting; on submit, a new worker resumes here with the
        // submission populated.
        let submission = ctx.await_form(schema).await?;

        Ok(NodeOutput::with("submission", serde_json::to_value(submission).unwrap_or(Value::Null)))
    }
}
