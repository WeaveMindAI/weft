//! HumanTrigger: starts an execution when a human submits a form.
//! Two phases:
//!
//!   - `Phase::TriggerSetup`: build a Form signal from the node's
//!     config and register it.
//!
//!   - `Phase::Fire`: the wake payload is the raw form payload (a
//!     flat JSON object keyed by field key). Map the submitted values
//!     to output ports per the form field definitions.

use async_trait::async_trait;
use serde_json::Value;

use weft_core::context::Phase;
use weft_core::signal::{Form, FormSchema};
use weft_core::{ExecutionContext, Node, NodeManifest, WeftResult};

use super::form_helpers::{build_form_fields, map_response_to_ports, parse_form_fields};

#[derive(NodeManifest)]
pub struct HumanTriggerNode;

#[async_trait]
impl Node for HumanTriggerNode {
    async fn execute(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let specs = &self.manifest().form_field_specs;
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
                // Triggers don't have upstream input ports at setup
                // time; pass an empty object so the helper doesn't
                // try to project a value into prefilled / display
                // fields.
                let empty_input = Value::Object(serde_json::Map::new());
                let fields = build_form_fields(&raw_fields, specs, &empty_input);
                let schema = FormSchema {
                    title: title.clone(),
                    description: description.clone(),
                    fields,
                };
                ctx.register_signal(Form {
                    form_type: "human-trigger".into(),
                    schema,
                    title: if title.is_empty() { None } else { Some(title) },
                    description,
                    // Browser extension / human-in-the-loop
                    // processors enumerate this consumer kind.
                    consumer_kind: Some("human_in_the_loop".into()),
                })
                .await?;
                Ok(())
            }
            Phase::Fire => {
                // The wake payload is the form submission (a JSON
                // object keyed by field key). Missing or non-object
                // means the dispatcher's form-submission delivery
                // broke its contract: fail loud, matching the cron /
                // WhatsAppReceive bar. Substituting an
                // empty object would silently fire a fake "all fields
                // empty" submission downstream (which for approve/
                // reject fields would synthesize a `rejected: true`
                // pulse).
                let submission = ctx.wake_payload_object()?;
                let raw_fields = parse_form_fields(&ctx.config.values);
                ctx.pulse_downstream(map_response_to_ports(submission, &raw_fields, specs)).await
            }
            Phase::InfraSetup => Ok(()),
        }
    }
}
