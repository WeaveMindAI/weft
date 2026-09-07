//! HumanQuery: pauses mid-execution waiting for a human form
//! submission. Single body: `run` builds a Form signal with
//! `is_resume=true`, awaits its fire, maps the submission to outputs.

use async_trait::async_trait;

use weft::{ExecutionContext, Node, NodeManifest, WeftResult};

use super::form_helpers::{
    build_form, form_specs, map_response_to_ports, parse_form_fields,
};

#[derive(NodeManifest)]
pub struct HumanQueryNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for HumanQueryNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let raw_fields = parse_form_fields(ctx.inputs.object()?);
        let specs = form_specs(self.manifest())?;

        // Project the node's DATA inputs into a flat {key: value} map so
        // display / prefilled / source=input fields can lift them out by
        // key. `custom()` is exactly the form-derived ports plus wired
        // values: the node's own settings (title/description/fields)
        // are not prefill data and never appear in it.
        let mut input_obj = serde_json::Map::new();
        for (k, v) in ctx.inputs.custom() {
            input_obj.insert(k.clone(), v.clone());
        }
        // A stored file among the prefills parks as it is: the listener
        // hands the consumer its facts, and the files door hands out a
        // fresh link each time the form is shown.
        let prefill = serde_json::Value::Object(input_obj);

        let form = build_form(&ctx.inputs, specs, "human-query", &prefill)?;
        let submission = ctx.await_signal(form).await?;
        ctx.pulse_downstream(map_response_to_ports(&submission, &raw_fields, specs)?).await
    }
}
