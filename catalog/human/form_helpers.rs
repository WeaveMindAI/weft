use serde_json::Value;
use weft_core::node::NodeOutput;
use weft_core::primitive::{FormField, FormFieldType};

pub fn parse_form_fields(config: &std::collections::HashMap<String, Value>) -> Vec<Value> {
    match config.get("fields") {
        Some(Value::Array(arr)) => arr.clone(),
        Some(Value::String(s)) => serde_json::from_str::<Vec<Value>>(s).unwrap_or_default(),
        _ => Vec::new(),
    }
}

pub fn build_runtime_field(raw: &Value) -> Option<FormField> {
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
    // Accept the canonical v2 shape (`field_type: { kind: "..." }`) and
    // the v1 flat string (`fieldType: "..."`). Historical node configs
    // used the flat form; enrich reads the canonical form.
    let field_type = raw
        .get("field_type")
        .and_then(|v| v.get("kind"))
        .and_then(|v| v.as_str())
        .or_else(|| raw.get("fieldType").and_then(|v| v.as_str()))
        .unwrap_or("text_input");
    let options_from_config: Vec<String> = raw
        .get("config")
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
            tracing::warn!("human: unknown fieldType '{}', falling back to text", other);
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

pub fn map_response_to_ports(response: &Value, raw_fields: &[Value]) -> NodeOutput {
    let mut output = NodeOutput::empty();
    for field in raw_fields {
        let field_type = field
            .get("field_type")
            .and_then(|v| v.get("kind"))
            .and_then(|v| v.as_str())
            .or_else(|| field.get("fieldType").and_then(|v| v.as_str()))
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
