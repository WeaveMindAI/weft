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

use weft::{ExecutionContext, Node, NodeManifest, WeftResult};

use super::form_helpers::{build_form, map_response_to_ports, parse_form_fields};

#[derive(NodeManifest)]
pub struct HumanTriggerNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for HumanTriggerNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn setup_trigger(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let specs = &self.manifest().form_field_specs;
        // Triggers have no upstream input ports at setup time; an
        // empty prefill means no value is projected into prefilled /
        // display fields.
        let empty_prefill = Value::Object(serde_json::Map::new());
        let form = build_form(&ctx.inputs, specs, "human-trigger", &empty_prefill)?;
        ctx.register_signal(form).await
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        // The wake record is the form submission, keyed by field key.
        // `object()` fails loud on a broken delivery: an empty substitute
        // would silently fire a fake "all fields empty" submission (for
        // an approve/reject field, a synthesized `rejected: true` pulse).
        let submission = ctx.wake.record()?;
        let specs = &self.manifest().form_field_specs;
        let raw_fields = parse_form_fields(ctx.inputs.object()?);
        ctx.pulse_downstream(map_response_to_ports(&submission, &raw_fields, specs)).await
    }
}
