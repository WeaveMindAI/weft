//! Form handler. Like the live-caller kinds, the dispatcher hosts the
//! public URL; the listener registers an in-RAM entry. Forms are
//! TaskCallback style: each fire is a one-shot reply tied to a token.
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

use crate::protocol::{ProcessOutcome, ProcessTarget};
use crate::registry::RegisteredSignal;

use async_trait::async_trait;

use super::{KindHandler, SpawnCtx};

pub struct FormHandler;

#[async_trait]
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

    async fn spawn_task(
        &self,
        _spec: &SignalSpec,
        _kind_state: &Value,
        _ctx: SpawnCtx,
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
        // SYNC: consumer payload keys <->
        //       extension-browser/src/lib/api.ts (PendingTask),
        //       crates/weft-dispatcher/src/api/signal.rs (the isResume stamp)
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
            .unwrap_or_else(|| format!("Input for {}", sig.node_id));
        obj.insert("title".into(), Value::String(resolved_title));
        if let Some(d) = form.description {
            obj.insert("description".into(), Value::String(d));
        }
        // Loud on a schema that cannot serialize: silently omitting
        // `formSchema` would hand the consumer an actionless card
        // ("no form fields configured") over a real broken form.
        let consumer_schema = form
            .schema
            .for_consumer()
            .map_err(|e| anyhow::anyhow!("form schema for its consumer: {e}"))?;
        let schema_json = serde_json::to_value(consumer_schema)
            .map_err(|e| anyhow::anyhow!("serialize form schema: {e}"))?;
        obj.insert("formSchema".into(), schema_json);
        Ok(Some(Value::Object(obj)))
    }
}

/// Parse a Form spec's typed config. Fails loudly on malformed input
/// so callers (compute_routing, render) surface the error to the
/// register/display caller rather than rendering empty.
fn parse(spec: &SignalSpec) -> Result<Form> {
    serde_json::from_value(spec.config.clone()).map_err(|e| {
        anyhow::anyhow!(
            "malformed form spec (its stored config does not match the current Form shape; \
             re-register the signal by re-running the project): {e}"
        )
    })
}

inventory::submit!(&FormHandler as &dyn KindHandler);

#[cfg(test)]
mod tests {
    use super::*;
    use weft_core::primitive::SignalSurface;
    use weft_core::signal::FormSchema;

    fn form_spec_with_title(title: Option<String>) -> SignalSpec {
        weft_core::signal::to_spec(Form {
            form_type: "human-query".into(),
            schema: FormSchema { fields: vec![] },
            title,
            description: None,
            consumer_kind: None,
        })
    }

    fn form_spec() -> SignalSpec {
        form_spec_with_title(None)
    }

    fn registered(spec: SignalSpec) -> RegisteredSignal {
        RegisteredSignal {
            spec,
            node_id: "node-7".into(),
            tenant_id: "t".into(),
            is_resume: true,
            color: Some("c".into()),
            placement_generation: 0,
            task: None,
            routing: SignalRouting {
                surface: SignalSurface::TaskCallback,
                auth: SignalAuth::None,
                auth_config: Value::Null,
            },
            serving: Default::default(),
        }
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
        let sig = registered(form_spec());
        let rendered = FormHandler
            .render("tok", &sig)
            .expect("render ok")
            .expect("renders");
        let obj = rendered.as_object().expect("object");
        assert_eq!(obj["nodeId"], serde_json::json!("node-7"));
        assert_eq!(obj["kind"], serde_json::json!("form"));
        assert!(obj.contains_key("formSchema"));
        // No title on the spec: the fallback names the node.
        assert_eq!(obj["title"], serde_json::json!("Input for node-7"));
    }

    /// A stored file among the fields reaches the consumer as its
    /// facts, never as the marker with the storage key (the files door
    /// hands out the link).
    #[test]
    fn render_keeps_the_storage_key_out_of_the_consumer_payload() {
        let spec = weft_core::signal::to_spec(Form {
            form_type: "human-query".into(),
            schema: FormSchema {
                fields: vec![weft_core::signal::FormField {
                    field_type: "display_image".into(),
                    key: "pic".into(),
                    label: String::new(),
                    render: weft_core::node::FormFieldRender {
                        component: "image".into(),
                        source: None,
                        multiple: false,
                        prefilled: false,
                    },
                    value: Some(serde_json::json!({ "__weft_image__": {
                        "key": "t/project/p/cat", "mimeType": "image/png", "sizeBytes": 7, "filename": "cat.png"
                    } })),
                    config: Default::default(),
                }],
            },
            title: None,
            description: None,
            consumer_kind: None,
        });
        let rendered = FormHandler.render("tok", &registered(spec)).expect("render ok").expect("renders");
        let value = &rendered["formSchema"]["fields"][0]["value"];
        assert_eq!(value, &serde_json::json!({ "mimeType": "image/png", "sizeBytes": 7, "filename": "cat.png" }));
        assert!(!rendered.to_string().contains("t/project/p/cat"), "{rendered}");
    }

    #[test]
    fn render_title_blank_and_set() {
        // A whitespace-only title is no title: the fallback fires.
        let sig = registered(form_spec_with_title(Some("  ".into())));
        let rendered =
            FormHandler.render("tok", &sig).expect("render ok").expect("renders");
        assert_eq!(rendered["title"], serde_json::json!("Input for node-7"));

        let sig = registered(form_spec_with_title(Some("Ask".into())));
        let rendered =
            FormHandler.render("tok", &sig).expect("render ok").expect("renders");
        assert_eq!(rendered["title"], serde_json::json!("Ask"));
    }

    #[test]
    fn no_actions_defined() {
        let sig = registered(form_spec());
        let cache = Arc::new(DashMap::new());
        let err = FormHandler
            .handle_action("tok", "regenerate_api_key", Value::Null, &sig, &cache)
            .expect_err("no actions");
        assert!(err.to_string().contains("no action"));
    }
}
