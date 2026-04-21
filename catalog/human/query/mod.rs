//! HumanQuery: ask a human a question mid-execution, wait for their
//! response. The single demonstration of `ctx.await_form` in the
//! scaffold, exercises the entire suspend-and-resume path.

use async_trait::async_trait;
use serde_json::Value;

use weft_core::node::{Diagnostic, NodeOutput, Severity};
use weft_core::project::{NodeDefinition, ProjectDefinition};
use weft_core::{ExecutionContext, FormSchema, Node, NodeMetadata, WeftResult};

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

    fn validate(&self, node: &NodeDefinition, _project: &ProjectDefinition) -> Vec<Diagnostic> {
        let mut d = Vec::new();
        let line = node.header_span.map(|s| s.start_line).unwrap_or(0);
        // At least one form field declared. Without fields the
        // human gets a meaningless empty form.
        let has_fields = node
            .config
            .get("formSchema")
            .and_then(|v| v.get("fields"))
            .and_then(|v| v.as_array())
            .map(|arr| !arr.is_empty())
            .unwrap_or(false);
        if !has_fields {
            d.push(Diagnostic {
                line,
                column: 0,
                severity: Severity::Error,
                message: format!(
                    "HumanQuery '{}' has no form fields; the human would see an empty form.",
                    node.id
                ),
                code: Some("humanquery-empty-form".into()),
            });
        }
        d
    }
}
