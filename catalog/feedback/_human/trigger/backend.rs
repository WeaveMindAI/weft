//! Human Trigger Node - A trigger that fires when a human submits a form.
//!
//! Uses the same form system as HumanQuery but operates as a trigger:
//! during trigger setup, it registers a persistent form in the TaskRegistry.
//! When a human submits the form, it fires a TriggerEvent with the form
//! data as payload. The form stays registered for repeated submissions.
//!
//! The form is registered with `{ "source": "human" }` metadata so the
//! browser extension can discover and display it.

use async_trait::async_trait;

use crate::node::{
    ExecutionContext, Node, NodeFeatures, NodeMetadata, PortDef,
    TriggerCategory, TriggerContext, TriggerError, TriggerHandle,
    TriggerStartConfig, FormFieldSpec,
};
use crate::{register_node, NodeResult};

// Reuse shared helpers from HumanQuery
use crate::nodes::human_query::{
    human_metadata, human_form_field_specs, parse_form_fields,
    build_form_schema, map_response_to_ports,
};

pub struct HumanTriggerNode;

#[async_trait]
impl Node for HumanTriggerNode {
    fn node_type(&self) -> &'static str {
        "HumanTrigger"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "Human Trigger",
            inputs: vec![
                PortDef::new("context", "String", false),
            ],
            outputs: vec![],
            features: NodeFeatures {
                isTrigger: true,
                triggerCategory: Some(TriggerCategory::Manual),
                hasFormSchema: true,
                requiresRunningInstance: true,
                ..Default::default()
            },
            fields: vec![],
        }
    }

    fn form_field_specs(&self) -> Vec<FormFieldSpec> {
        human_form_field_specs()
    }

    async fn execute(&self, ctx: ExecutionContext) -> NodeResult {
        if ctx.isTriggerSetup {
            // During trigger setup, pass through config so keep_alive can use it.
            // Include context input if provided.
            let mut config = ctx.config.clone();
            if let Some(context) = ctx.input.get("context") {
                if let Some(o) = config.as_object_mut() {
                    o.insert("context".to_string(), context.clone());
                }
            }
            return NodeResult::completed(config);
        }

        // Normal execution: extract trigger payload and map to output ports.
        let payload = ctx.input.get("triggerPayload")
            .cloned()
            .unwrap_or(ctx.input.clone());

        let raw_fields = parse_form_fields(&ctx.config);
        let output = map_response_to_ports(&payload, &raw_fields);
        NodeResult::completed(output)
    }

    async fn keep_alive(&self,
        config: TriggerStartConfig,
        ctx: TriggerContext,
    ) -> Result<TriggerHandle, TriggerError> {
        let registrar = ctx.form_registrar.clone()
            .ok_or_else(|| TriggerError::Config("HumanTrigger requires a FormRegistrar".to_string()))?;

        // Build the form schema from config
        let raw_fields = parse_form_fields(&config.config);
        let empty_input = serde_json::Value::Object(serde_json::Map::new());
        let form_schema = build_form_schema(&raw_fields, &empty_input, &human_form_field_specs())
            .ok_or_else(|| TriggerError::Config("HumanTrigger has no form fields configured".to_string()))?;

        let title = config.config.get("title")
            .and_then(|v| v.as_str())
            .unwrap_or("Submit")
            .to_string();

        let description = config.config.get("description")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let mut meta = human_metadata();
        if let Some(context) = config.config.get("context").and_then(|v| v.as_str()) {
            meta.as_object_mut().unwrap().insert("context".to_string(), serde_json::Value::String(context.to_string()));
        }

        ctx.spawn(&config, TriggerCategory::Manual, move |emit, shutdown| async move {
            if let Err(e) = registrar.register_form(title, description, form_schema, meta).await {
                tracing::error!("[HumanTrigger] Failed to register form: {}", e);
                return Err(TriggerError::Connection(format!("Failed to register form: {}", e)));
            }

            tokio::pin!(shutdown);

            loop {
                tokio::select! {
                    submission = registrar.wait_for_submission() => {
                        match submission {
                            Some(sub) => {
                                emit.emit(sub.data)?;
                            }
                            None => {
                                tracing::info!("[HumanTrigger] Submission channel closed");
                                break;
                            }
                        }
                    }
                    _ = &mut shutdown => {
                        registrar.unregister_form().await;
                        return Ok(());
                    }
                }
            }

            registrar.unregister_form().await;
            Ok(())
        })
    }
}

register_node!(HumanTriggerNode);
