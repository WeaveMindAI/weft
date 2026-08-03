//! Human Node - Wait for human review with a structured form.
//!
//! The form schema is defined at build time in the node config as a list of
//! fields. Display fields show runtime data to the reviewer. Interactive fields
//! collect responses and map to typed output ports.
//!
//! Field types:
//!   - "display"              : read-only, shows a runtime value (input port)
//!   - "approve_reject"       : approve/reject buttons (output ports: {key}_approved, {key}_rejected)
//!   - "select"               : single choice from static options (output port, String)
//!   - "multi_select"         : multiple choices from static options (output port, List)
//!   - "select_input"         : single choice from input port options (input + output port, String)
//!   - "multi_select_input"   : multiple choices from input port options (input + output port, List)
//!   - "text_input"           : short free text (output port, String)
//!   - "textarea"             : long free text (output port, String)
//!   - "editable_textarea"    : textarea pre-filled from input port (input + output port, String)
//!
//! Input ports are created for fields that receive runtime data (display, multi_select_input, editable_textarea).
//! Output ports are created for each interactive field (named by field key).
//! The null-cuts-flow contract: approve_reject sends null on the inactive path,
//! cutting downstream flow via required-port propagation.
//!
//! The node registers its form with `{ "source": "human" }` metadata so that
//! the browser extension (and other human-facing consumers) can filter for it.

use async_trait::async_trait;
use weft_core::{FormSchema, FormField};
use crate::node::{Node, NodeMetadata, NodeFeatures, ExecutionContext, PortDef, FormFieldSpec, FormFieldPort};
use crate::form_input::FormInputRequest;
use crate::{NodeResult, register_node};

#[derive(Default)]
pub struct HumanNode;

/// Metadata tag for human-facing forms. Used by both HumanQuery and HumanTrigger.
pub fn human_metadata() -> serde_json::Value {
    serde_json::json!({ "source": "human" })
}

pub fn parse_form_fields(config: &serde_json::Value) -> Vec<serde_json::Value> {
    match config.get("fields") {
        Some(serde_json::Value::Array(arr)) => arr.clone(),
        Some(serde_json::Value::String(s)) => {
            serde_json::from_str::<Vec<serde_json::Value>>(s).unwrap_or_default()
        }
        _ => Vec::new(),
    }
}

pub fn build_form_schema(
    raw_fields: &[serde_json::Value],
    input: &serde_json::Value,
    specs: &[FormFieldSpec],
) -> Option<FormSchema> {
    let spec_map: std::collections::HashMap<&str, &FormFieldSpec> = specs.iter()
        .map(|s| (s.field_type, s))
        .collect();

    let form_fields: Vec<FormField> = raw_fields.iter().filter_map(|f| {
        let field_type = f.get("fieldType").and_then(|v| v.as_str()).unwrap_or("display").to_string();
        let key = f.get("key").and_then(|v| v.as_str())?.to_string();
        let field_config = f.get("config").cloned().unwrap_or(serde_json::Value::Object(serde_json::Map::new()));

        // Use explicit render from field JSON, or fall back to the spec's default render
        let render = f.get("render").cloned().unwrap_or_else(|| {
            spec_map.get(field_type.as_str())
                .map(|s| s.render.clone())
                .unwrap_or(serde_json::Value::Object(serde_json::Map::new()))
        });

        let component = render.get("component").and_then(|v| v.as_str());
        let needs_input = render.get("source").and_then(|v| v.as_str()) == Some("input")
            || render.get("prefilled").and_then(|v| v.as_bool()) == Some(true)
            || component == Some("readonly")
            || component == Some("image");
        let value = if needs_input { input.get(&key).cloned() } else { None };

        Some(FormField {
            fieldType: field_type,
            key,
            render,
            value,
            config: field_config,
        })
    }).collect();

    if form_fields.is_empty() { None } else { Some(FormSchema { fields: form_fields }) }
}

/// Map form responses to output port values.
pub fn map_response_to_ports(response: &serde_json::Value, raw_fields: &[serde_json::Value]) -> serde_json::Value {
    let mut output = serde_json::Map::new();

    for field in raw_fields {
        let field_type = field.get("fieldType").and_then(|v| v.as_str()).unwrap_or("display");
        let key = match field.get("key").and_then(|v| v.as_str()) {
            Some(k) => k,
            None => continue,
        };

        match field_type {
            "display" => {}
            "approve_reject" => {
                let is_approved = response.get(key).and_then(|v| v.as_bool()).unwrap_or(false);
                let approve_key = format!("{}_approved", key);
                let reject_key = format!("{}_rejected", key);
                if is_approved {
                    output.insert(approve_key, serde_json::Value::Bool(true));
                    output.insert(reject_key, serde_json::Value::Null);
                } else {
                    output.insert(approve_key, serde_json::Value::Null);
                    output.insert(reject_key, serde_json::Value::Bool(true));
                }
            }
            _ => {
                let value = response.get(key).cloned().unwrap_or(serde_json::Value::Null);
                output.insert(key.to_string(), value);
            }
        }
    }

    serde_json::Value::Object(output)
}

/// Human form field specs shared by HumanQuery and HumanTrigger.
/// Must stay in sync with HUMAN_FORM_FIELD_SPECS in frontend.ts.
pub fn human_form_field_specs() -> Vec<FormFieldSpec> {
    vec![
        FormFieldSpec {
            field_type: "display",
            render: serde_json::json!({ "component": "readonly" }),
            adds_inputs: vec![FormFieldPort::any("{key}")],
            adds_outputs: vec![],
        },
        FormFieldSpec {
            field_type: "display_image",
            render: serde_json::json!({ "component": "image" }),
            adds_inputs: vec![FormFieldPort::new("{key}", "Image")],
            adds_outputs: vec![],
        },
        FormFieldSpec {
            field_type: "approve_reject",
            render: serde_json::json!({ "component": "buttons", "source": "static" }),
            adds_inputs: vec![],
            adds_outputs: vec![
                FormFieldPort::new("{key}_approved", "Boolean"),
                FormFieldPort::new("{key}_rejected", "Boolean"),
            ],
        },
        FormFieldSpec {
            field_type: "select",
            render: serde_json::json!({ "component": "select", "source": "static" }),
            adds_inputs: vec![],
            adds_outputs: vec![FormFieldPort::new("{key}", "String")],
        },
        FormFieldSpec {
            field_type: "multi_select",
            render: serde_json::json!({ "component": "select", "source": "static", "multiple": true }),
            adds_inputs: vec![],
            adds_outputs: vec![FormFieldPort::new("{key}", "List[String]")],
        },
        FormFieldSpec {
            field_type: "select_input",
            render: serde_json::json!({ "component": "select", "source": "input" }),
            adds_inputs: vec![FormFieldPort::new("{key}", "List[String]")],
            adds_outputs: vec![FormFieldPort::new("{key}", "String")],
        },
        FormFieldSpec {
            field_type: "multi_select_input",
            render: serde_json::json!({ "component": "select", "source": "input", "multiple": true }),
            adds_inputs: vec![FormFieldPort::new("{key}", "List[String]")],
            adds_outputs: vec![FormFieldPort::new("{key}", "List[String]")],
        },
        FormFieldSpec {
            field_type: "text_input",
            render: serde_json::json!({ "component": "text" }),
            adds_inputs: vec![],
            adds_outputs: vec![FormFieldPort::new("{key}", "String")],
        },
        FormFieldSpec {
            field_type: "textarea",
            render: serde_json::json!({ "component": "textarea" }),
            adds_inputs: vec![],
            adds_outputs: vec![FormFieldPort::new("{key}", "String")],
        },
        FormFieldSpec {
            field_type: "editable_text_input",
            render: serde_json::json!({ "component": "text", "prefilled": true }),
            adds_inputs: vec![FormFieldPort::new("{key}", "String")],
            adds_outputs: vec![FormFieldPort::new("{key}", "String")],
        },
        FormFieldSpec {
            field_type: "editable_textarea",
            render: serde_json::json!({ "component": "textarea", "prefilled": true }),
            adds_inputs: vec![FormFieldPort::new("{key}", "String")],
            adds_outputs: vec![FormFieldPort::new("{key}", "String")],
        },
    ]
}

#[async_trait]
impl Node for HumanNode {
    fn node_type(&self) -> &'static str {
        "HumanQuery"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "Human",
            inputs: vec![
                PortDef::new("context", "String", false),
            ],
            outputs: vec![],
            features: NodeFeatures {
                hasFormSchema: true,
                ..Default::default()
            },
            fields: vec![],
        }
    }

    fn form_field_specs(&self) -> Vec<FormFieldSpec> {
        human_form_field_specs()
    }

    async fn execute(&self, ctx: ExecutionContext) -> NodeResult {
        let raw_fields = parse_form_fields(&ctx.config);

        let title = ctx.config.get("title")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let description = ctx.config.get("description")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let form_schema = build_form_schema(&raw_fields, &ctx.input, &self.form_field_specs());

        let form = match form_schema {
            Some(f) => f,
            None => {
                tracing::warn!("Human node {} has no form fields configured", ctx.nodeId);
                return NodeResult::completed(ctx.input);
            }
        };

        // Build metadata: source=human, plus context if provided
        let mut meta = human_metadata();
        if let Some(context) = ctx.input.get("context").and_then(|v| v.as_str()) {
            meta.as_object_mut().unwrap().insert("context".to_string(), serde_json::Value::String(context.to_string()));
        }

        let mut request = FormInputRequest::new(form).with_metadata(meta);
        if let Some(t) = title { request = request.with_title(t); }
        if let Some(d) = description { request = request.with_description(d); }

        match ctx.request_form_input(request).await {
            Ok(response) => {
                let output = map_response_to_ports(&response, &raw_fields);
                NodeResult::completed(output)
            }
            Err(e) => {
                tracing::error!("Human node {} failed to get input: {}", ctx.nodeId, e);
                NodeResult::failed(&e)
            }
        }
    }
}

register_node!(HumanNode);
