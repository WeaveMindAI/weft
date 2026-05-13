//! Form handler. Like Webhook, the dispatcher hosts the public URL;
//! the listener registers an in-RAM entry. Forms are TaskCallback
//! style: each fire is a one-shot reply tied to a specific token.
//!
//! Resume forms (HumanQuery style) take the resume path generically
//! in `kinds::process`; entry forms (HumanTrigger) route to `Entry`.

use std::sync::Arc;

use dashmap::DashMap;
use serde_json::Value;
use tokio::task::JoinHandle;
use anyhow::Result;
use weft_core::primitive::{SignalAuth, SignalRouting, SignalSpec, SignalSurface};
use weft_core::signal::{Form, Signal};

use crate::config::ListenerConfig;
use crate::fire_sink::FireSignalSink;
use crate::protocol::{ProcessOutcome, ProcessTarget};
use crate::registry::RegisteredSignal;

use super::KindHandler;

pub struct FormHandler;

impl KindHandler for FormHandler {
    fn tag(&self) -> &'static str {
        Form::TAG
    }

    fn compute_routing(
        &self,
        _token: &str,
        _spec: &SignalSpec,
        _secret_cache: &Arc<DashMap<String, String>>,
    ) -> Result<SignalRouting> {
        Ok(SignalRouting {
            surface: SignalSurface::TaskCallback,
            auth: SignalAuth::None,
            auth_config: Value::Null,
        })
    }

    fn spawn_task(
        &self,
        _token: &str,
        _spec: &SignalSpec,
        _kind_state: &Value,
        _sink: FireSignalSink,
        _config: Arc<ListenerConfig>,
    ) -> Result<Option<JoinHandle<()>>> {
        Ok(None)
    }

    fn process_entry(
        &self,
        _sig: &RegisteredSignal,
        payload: Value,
    ) -> ProcessOutcome {
        ProcessOutcome {
            value: payload,
            target: ProcessTarget::Entry,
        }
    }

    fn render(&self, token: &str, sig: &RegisteredSignal) -> Result<Option<Value>> {
        let form = parse(&sig.spec)?;
        let mut obj = serde_json::Map::new();
        obj.insert("token".into(), Value::String(token.to_string()));
        obj.insert("nodeId".into(), Value::String(sig.node_id.clone()));
        obj.insert("kind".into(), Value::String(Form::TAG.into()));
        if let Some(ck) = &sig.spec.consumer_kind {
            obj.insert("consumerKind".into(), Value::String(ck.clone()));
        }
        let resolved_title = form
            .title
            .clone()
            .filter(|t| !t.trim().is_empty())
            .unwrap_or_else(|| form.schema.title.clone());
        let resolved_title = if resolved_title.trim().is_empty() {
            format!("Input for {}", sig.node_id)
        } else {
            resolved_title
        };
        obj.insert("title".into(), Value::String(resolved_title));
        if let Some(d) = form.description {
            obj.insert("description".into(), Value::String(d));
        }
        if let Ok(schema_json) = serde_json::to_value(&form.schema) {
            obj.insert("formSchema".into(), schema_json);
        }
        Ok(Some(Value::Object(obj)))
    }
}

/// Parse a Form spec's typed config. Fails loudly on malformed input
/// so callers (compute_routing, render) surface the error to the
/// register/display caller rather than rendering empty.
fn parse(spec: &SignalSpec) -> Result<Form> {
    serde_json::from_value(spec.config.clone())
        .map_err(|e| anyhow::anyhow!("malformed form spec: {e}"))
}

inventory::submit!(&FormHandler as &dyn KindHandler);

#[cfg(test)]
mod tests {
    use super::*;
    use weft_core::primitive::SignalSurface;
    use weft_core::signal::FormSchema;

    fn form_spec() -> SignalSpec {
        weft_core::signal::to_spec(Form {
            form_type: "human-query".into(),
            schema: FormSchema {
                title: "T".into(),
                description: None,
                fields: vec![],
            },
            title: None,
            description: None,
            consumer_kind: None,
        })
    }

    #[test]
    fn form_yields_task_callback() {
        let spec = form_spec();
        let cache = Arc::new(DashMap::new());
        let r = FormHandler.compute_routing("tok", &spec, &cache).expect("routing ok");
        assert!(matches!(r.surface, SignalSurface::TaskCallback));
        assert!(matches!(r.auth, weft_core::primitive::SignalAuth::None));
        assert!(cache.is_empty(), "form mints no plaintext");
    }

    #[test]
    fn render_includes_form_schema() {
        let spec = form_spec();
        let sig = RegisteredSignal {
            spec,
            node_id: "node-7".into(),
            is_resume: true,
            color: Some("c".into()),
            task: None,
            routing: SignalRouting {
                surface: SignalSurface::TaskCallback,
                auth: SignalAuth::None,
                auth_config: Value::Null,
            },
        };
        let rendered = FormHandler
            .render("tok", &sig)
            .expect("render ok")
            .expect("renders");
        let obj = rendered.as_object().expect("object");
        assert_eq!(obj["nodeId"], serde_json::json!("node-7"));
        assert_eq!(obj["kind"], serde_json::json!("form"));
        assert!(obj.contains_key("formSchema"));
    }

    #[test]
    fn no_actions_defined() {
        let sig = RegisteredSignal {
            spec: form_spec(),
            node_id: "n".into(),
            is_resume: true,
            color: Some("c".into()),
            task: None,
            routing: SignalRouting {
                surface: SignalSurface::TaskCallback,
                auth: SignalAuth::None,
                auth_config: Value::Null,
            },
        };
        let cache = Arc::new(DashMap::new());
        let err = FormHandler
            .handle_action("tok", "regenerate_api_key", Value::Null, &sig, &cache)
            .expect_err("no actions");
        assert!(err.to_string().contains("no action"));
    }
}
