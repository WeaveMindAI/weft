//! HumanTrigger: starts a run when a human submits a form.
//!
//!   - `setup_trigger`: build a Form signal from the node's config and
//!     register it.
//!
//!   - `run`: the wake payload is the raw form payload (a flat JSON
//!     object keyed by field key). Read it against the same fields and
//!     map the answers to output ports.

use async_trait::async_trait;
use serde_json::Value;

use weft::signal::{ConfigForm, Form, FormSchema};
use weft::{RunContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct HumanTriggerNode;

#[cfg(feature = "node-tests")]
mod tests;

/// Triggers have no upstream input ports, so no value is projected into
/// display or prefilled fields.
fn no_prefill() -> Value {
    Value::Object(serde_json::Map::new())
}

#[async_trait]
impl Node for HumanTriggerNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn setup_trigger(&self, ctx: RunContext) -> WeftResult<()> {
        let config = ConfigForm::of(self.manifest(), &ctx.inputs)?;
        let form = Form::for_person(
            "human-trigger",
            ctx.inputs.opt("title")?,
            ctx.inputs.opt("description")?,
            config.fields(&no_prefill())?,
        );
        ctx.register_signal(form).await
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        // The wake record is the form submission, keyed by field key.
        // `record()` fails loud on a broken delivery: an empty substitute
        // would silently fire a fake "all fields empty" submission (for
        // an approve/reject field, a synthesized `rejected: true` pulse).
        let submission = ctx.wake.record()?;
        let config = ConfigForm::of(self.manifest(), &ctx.inputs)?;
        let schema = FormSchema { fields: config.fields(&no_prefill())? };
        ctx.pulse_downstream(config.outputs(&schema.answers(&submission)?)?).await
    }
}
