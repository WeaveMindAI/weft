//! Form submission with a rendered schema. The consumer (browser
//! extension, dashboard) reads `schema` to render a form; submission
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
    /// Consumer label for token-scoped enumeration. Browser
    /// extension, dashboard, etc. fetch signals tagged with the
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FormSchema {
    pub title: String,
    pub description: Option<String>,
    pub fields: Vec<FormField>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FormField {
    pub field_type: String,
    pub key: String,
    pub label: String,
    /// Render hint copied from the spec (component name + flags).
    /// The dashboard / browser extension reads `render.component`
    /// to pick the UI primitive.
    #[serde(default)]
    pub render: Value,
    /// Pre-fill value for fields that need an upstream input port
    /// value (display, display_image, editable_*, *_input). None
    /// for purely interactive fields.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<Value>,
    /// Per-field config from the source (options, labels, etc).
    #[serde(default)]
    pub config: Value,
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
