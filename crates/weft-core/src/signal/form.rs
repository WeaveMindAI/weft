//! Form submission with a rendered schema. The consumer (the browser
//! extension) reads `schema` to render a form; submission
//! flows back through the dispatcher's task-callback URL.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::Signal;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Form {
    /// Routes the form to the right UI panel ("human-trigger" vs
    /// "human-query"). Hardcoded by the node author; not pulled
    /// from config.
    pub form_type: String,
    pub schema: FormSchema,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Consumer label for token-scoped enumeration. Consumers (the
    /// browser extension) fetch signals tagged with the
    /// matching string (e.g. `"human_in_the_loop"`). `None` = not
    /// listed in any consumer surface.
    ///
    /// `serde(skip)` so the value lives only at the top of
    /// `SignalSpec` (lifted there by `signal::to_spec` via the
    /// `Signal::consumer_kind` trait method). Carrying it twice on
    /// the wire was a redundancy.
    #[serde(skip)]
    pub consumer_kind: Option<String>,
}

// SYNC: FormSchema/FormField wire shape <->
//       extension-browser/src/lib/api.ts (FormSchema/FormField)
// (catalog/human/form_helpers.rs constructs these structs directly, so
// the compiler keeps it in step; only the TS restatement can drift.)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FormSchema {
    /// Just the fields: the form's `title`/`description` live on
    /// [`Form`] itself, the single home (a second copy here was a
    /// duplicate nobody read).
    pub fields: Vec<FormField>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FormField {
    pub field_type: String,
    pub key: String,
    pub label: String,
    /// Render hint copied from the spec (component name + flags),
    /// typed so a field can never ship without one (a form field with
    /// no render is undrawable and refused at build time). The
    /// browser extension reads `render.component` to pick the UI
    /// primitive.
    pub render: crate::node::FormFieldRender,
    /// Pre-fill value for fields that need an upstream input port
    /// value (display, display_image, editable_*, *_input). None
    /// for purely interactive fields.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<Value>,
    /// Per-field config from the source (options, labels, etc).
    #[serde(default)]
    pub config: serde_json::Map<String, Value>,
}

impl Signal for Form {
    const TAG: &'static str = "form";

    fn validate(&self) -> Result<(), String> {
        if self.form_type.trim().is_empty() {
            return Err("form.form_type must not be empty".into());
        }
        Ok(())
    }

    fn consumer_kind(&self) -> Option<&str> {
        self.consumer_kind.as_deref()
    }
}

crate::register_signal_kind!(Form);

#[cfg(test)]
mod wire_tests {
    use super::*;

    fn field() -> FormField {
        FormField {
            field_type: "text_input".into(),
            key: "answer".into(),
            label: "Answer".into(),
            render: crate::node::FormFieldRender {
                component: "text".into(),
                source: None,
                multiple: false,
                prefilled: false,
            },
            value: None,
            config: serde_json::Map::new(),
        }
    }

    /// The wire is what the browser extension's TS restatement reads
    /// and what registered signals persist, so the EXACT JSON is the
    /// contract, keys and absences included.
    #[test]
    fn form_wire_shape_with_title_and_description() {
        let form = Form {
            form_type: "human-query".into(),
            schema: FormSchema { fields: vec![field()] },
            title: Some("Ask".into()),
            description: Some("Why".into()),
            consumer_kind: Some("human_in_the_loop".into()),
        };
        let json = serde_json::to_value(&form).expect("serialize");
        assert_eq!(
            json,
            serde_json::json!({
                "form_type": "human-query",
                "schema": { "fields": [{
                    "fieldType": "text_input",
                    "key": "answer",
                    "label": "Answer",
                    "render": { "component": "text" },
                    "config": {}
                }] },
                "title": "Ask",
                "description": "Why"
            })
        );
        let back: Form = serde_json::from_value(json).expect("round-trip");
        assert_eq!(back.title.as_deref(), Some("Ask"));
        assert_eq!(back.schema.fields.len(), 1);
        // `consumer_kind` is serde(skip): it never rides the wire.
        assert_eq!(back.consumer_kind, None);
    }

    #[test]
    fn form_wire_shape_without_optionals() {
        let form = Form {
            form_type: "human-trigger".into(),
            schema: FormSchema { fields: vec![] },
            title: None,
            description: None,
            consumer_kind: None,
        };
        let json = serde_json::to_value(&form).expect("serialize");
        // Absent, never null: the TS `title?`/`description?` fields
        // depend on it.
        assert_eq!(
            json,
            serde_json::json!({
                "form_type": "human-trigger",
                "schema": { "fields": [] }
            })
        );
        let back: Form = serde_json::from_value(json).expect("round-trip");
        assert_eq!(back.title, None);
        assert_eq!(back.description, None);
    }
}
