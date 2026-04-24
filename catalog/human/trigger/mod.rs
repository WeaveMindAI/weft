//! HumanTrigger: starts an execution when a human submits a form.
//! Two phases:
//!
//!   - `Phase::TriggerSetup`: build a Form wake signal from the
//!     node's config and register it.
//!
//!   - `Phase::Fire`: the submission landed on `__seed__` as
//!     `{body: <submission>}`. Map the submitted values to output
//!     ports per the form field definitions.

use async_trait::async_trait;
use serde_json::Value;

use weft_core::context::Phase;
use weft_core::node::NodeOutput;
use weft_core::primitive::{FormField, FormSchema, WakeSignalKind, WakeSignalSpec};
use weft_core::{ExecutionContext, Node, NodeMetadata, WeftResult};

use super::form_helpers::{build_runtime_field, map_response_to_ports, parse_form_fields};

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
        match ctx.phase {
            Phase::TriggerSetup => {
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
                let fields: Vec<FormField> = raw_fields
                    .iter()
                    .filter_map(build_runtime_field)
                    .collect();
                let schema = FormSchema {
                    title: title.clone(),
                    description: description.clone(),
                    fields,
                };
                ctx.register_signal(WakeSignalSpec {
                    kind: WakeSignalKind::Form {
                        form_type: "human-trigger".into(),
                        schema,
                        title: if title.is_empty() { None } else { Some(title) },
                        description,
                    },
                    is_resume: false,
                })
                .await?;
                Ok(NodeOutput::empty())
            }
            Phase::Fire => {
                let payload = ctx
                    .input
                    .values
                    .get("__seed__")
                    .cloned()
                    .unwrap_or(Value::Null);
                let submission = payload.get("body").cloned().unwrap_or(payload);
                let raw_fields = parse_form_fields(&ctx.config.values);
                Ok(map_response_to_ports(&submission, &raw_fields))
            }
            Phase::InfraSetup => Ok(NodeOutput::empty()),
        }
    }
}
