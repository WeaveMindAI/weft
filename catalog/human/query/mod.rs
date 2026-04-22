//! HumanQuery: pause execution, present a form to a human, resume
//! with the submission mapped to output ports.
//!
//! The graph-side contract comes from `form_field_specs` (declared
//! in the stdlib catalog and applied by the compiler's enrich pass):
//! each form field contributes inputs and outputs based on its
//! `fieldType`. This node's runtime half is responsible for
//! translating the user's config into a concrete FormSchema, calling
//! `await_form`, and on resume mapping the submission values to
//! output ports exactly as v1 did
//! (catalog-v1/feedback/:human/query/backend.rs:map_response_to_ports).

use async_trait::async_trait;
use serde_json::Value;

use weft_core::node::{Diagnostic, NodeOutput, Severity};
use weft_core::primitive::{FormField, FormFieldType, FormSchema};
use weft_core::project::{NodeDefinition, ProjectDefinition};
use weft_core::{ExecutionContext, Node, NodeMetadata, WeftResult};

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
            title,
            description,
            fields: raw_fields
                .iter()
                .filter_map(build_runtime_field)
                .collect(),
        };

        let submission = ctx.await_form(schema).await?;
        Ok(map_response_to_ports(&submission.values, &raw_fields))
    }

    fn validate(&self, node: &NodeDefinition, _project: &ProjectDefinition) -> Vec<Diagnostic> {
        let mut d = Vec::new();
        let line = node.header_span.map(|s| s.start_line).unwrap_or(0);
        let has_fields = node
            .config
            .get("fields")
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

fn parse_form_fields(config: &std::collections::HashMap<String, Value>) -> Vec<Value> {
    match config.get("fields") {
        Some(Value::Array(arr)) => arr.clone(),
        Some(Value::String(s)) => serde_json::from_str::<Vec<Value>>(s).unwrap_or_default(),
        _ => Vec::new(),
    }
}

/// Map a user-declared form field to a v2 runtime FormField. v1
/// supports 11 fieldTypes, v2's enum supports 7; a few collapse:
///   approve_reject → select(["approve","reject"])
///   display / display_image → Text (readonly render is a UI hint;
///     runtime value just echoes). `required` becomes false so the
///     UI doesn't block on them.
///   editable_* → Textarea/Text
///   select_input / multi_select_input → Select / Multiselect with
///     runtime options from the matching input bag entry (we don't
///     get the input here, so fall back to empty options and expect
///     the frontend to dynamically hydrate).
fn build_runtime_field(raw: &Value) -> Option<FormField> {
    let key = raw.get("key").and_then(|v| v.as_str())?.to_string();
    let label = raw
        .get("label")
        .and_then(|v| v.as_str())
        .unwrap_or(&key)
        .to_string();
    let required = raw
        .get("required")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    let field_type = raw
        .get("fieldType")
        .and_then(|v| v.as_str())
        .unwrap_or("text_input");
    let config = raw.get("config");
    let options_from_config: Vec<String> = config
        .and_then(|c| c.get("options"))
        .and_then(|o| o.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();

    let ft = match field_type {
        "text_input" | "editable_text_input" => FormFieldType::Text,
        "textarea" | "editable_textarea" => FormFieldType::Textarea,
        "select" | "select_input" => FormFieldType::Select {
            options: options_from_config,
        },
        "multi_select" | "multi_select_input" => FormFieldType::Multiselect {
            options: options_from_config,
        },
        "approve_reject" => FormFieldType::Select {
            options: vec!["approve".into(), "reject".into()],
        },
        "display" | "display_image" => FormFieldType::Text,
        other => {
            tracing::warn!("HumanQuery: unknown fieldType '{}', falling back to text", other);
            FormFieldType::Text
        }
    };

    Some(FormField {
        key,
        label,
        field_type: ft,
        required,
        default: raw.get("default").cloned(),
    })
}

fn map_response_to_ports(response: &Value, raw_fields: &[Value]) -> NodeOutput {
    let mut output = NodeOutput::empty();
    for field in raw_fields {
        let field_type = field
            .get("fieldType")
            .and_then(|v| v.as_str())
            .unwrap_or("display");
        let Some(key) = field.get("key").and_then(|v| v.as_str()) else {
            continue;
        };
        match field_type {
            "display" | "display_image" => {}
            "approve_reject" => {
                let is_approved = response
                    .get(key)
                    .map(|v| match v {
                        Value::Bool(b) => *b,
                        Value::String(s) => s == "approve" || s == "approved" || s == "true",
                        _ => false,
                    })
                    .unwrap_or(false);
                let approve_key = format!("{key}_approved");
                let reject_key = format!("{key}_rejected");
                if is_approved {
                    output = output.set(approve_key, Value::Bool(true));
                    output = output.set(reject_key, Value::Null);
                } else {
                    output = output.set(approve_key, Value::Null);
                    output = output.set(reject_key, Value::Bool(true));
                }
            }
            _ => {
                let value = response.get(key).cloned().unwrap_or(Value::Null);
                output = output.set(key.to_string(), value);
            }
        }
    }
    output
}
