//! HumanTrigger: starts an execution when a human submits a form.
//!
//!   - `setup_trigger`: build a Form signal from the node's config and
//!     register it.
//!
//!   - `run`: the wake payload is the raw form payload (a flat JSON
//!     object keyed by field key). Map the submitted values to output
//!     ports per the form field definitions.

use async_trait::async_trait;
use serde_json::Value;

use weft_core::signal::{Form, FormSchema};
use weft_core::{ExecutionContext, Node, NodeManifest, WeftResult};

use super::form_helpers::{build_form_fields, map_response_to_ports, parse_form_fields};

#[derive(NodeManifest)]
pub struct HumanTriggerNode;

#[async_trait]
impl Node for HumanTriggerNode {
    async fn setup_trigger(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let specs = &self.manifest().form_field_specs;
        let raw_fields = parse_form_fields(ctx.inputs.object()?);
        let title: String = ctx.inputs.get_or("title", String::new())?;
        let description: Option<String> = ctx.inputs.opt("description")?;
        // Triggers don't have upstream input ports at setup time; pass
        // an empty object so the helper doesn't try to project a value
        // into prefilled / display fields.
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
            // Browser extension / human-in-the-loop processors
            // enumerate this consumer kind.
            consumer_kind: Some("human_in_the_loop".into()),
        })
        .await
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        // The wake record is the form submission, keyed by field key.
        // `object()` fails loud on a broken delivery: an empty substitute
        // would silently fire a fake "all fields empty" submission (for
        // an approve/reject field, a synthesized `rejected: true` pulse).
        let submission = Value::Object(ctx.wake.object()?.clone());
        let specs = &self.manifest().form_field_specs;
        let raw_fields = parse_form_fields(ctx.inputs.object()?);
        ctx.pulse_downstream(map_response_to_ports(&submission, &raw_fields, specs)).await
    }
}
