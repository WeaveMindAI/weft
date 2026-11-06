//! HumanQuery: pauses mid-run waiting for a human form
//! submission. Single body: `run` builds a Form signal, waits on it,
//! maps the submission to outputs. The submission is all the state
//! keeps: a body run again after the pause comes back to the wait, which
//! hands it over.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use weft::signal::{ConfigForm, Form};
use weft::{RunContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct HumanQueryNode;

/// Whether the form came back, and what it said.
#[derive(Default, Serialize, Deserialize)]
struct Asked {
    submission: Option<Value>,
}

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for HumanQueryNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let config = ConfigForm::of(self.manifest(), &ctx.inputs)?;

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
        let prefill = Value::Object(input_obj);

        let form = Form::for_person(
            "human-query",
            ctx.inputs.opt("title")?,
            ctx.inputs.opt("description")?,
            config.fields(&prefill)?,
        );
        let schema = form.schema.clone();
        let mut state = ctx.keep(Asked::default()).await?;
        let submission = state.wait(|s| &mut s.submission, form).await?;
        ctx.pulse_downstream(config.outputs(&schema.answers(&submission)?)?).await
    }
}
