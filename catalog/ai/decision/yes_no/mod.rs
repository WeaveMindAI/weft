//! DecisionYesNo: ask a decision model whether a condition holds about
//! the state the program's added ports carry (the Decisions API's
//! `noul` question), and hand back the probability of yes plus that
//! probability held against a threshold.
//!
//! The state, the provider and the call are the package's `decision.rs`.

use async_trait::async_trait;
use serde_json::json;

use weft::node::NodeOutput;
use weft::{RunContext, Node, NodeManifest, WeftResult};

use super::decision;

#[derive(NodeManifest)]
pub struct DecisionYesNoNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for DecisionYesNoNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let instructions: String = ctx.inputs.get("instructions")?;
        let when_true: Option<String> = ctx.inputs.opt("whenTrue")?;
        let when_false: Option<String> = ctx.inputs.opt("whenFalse")?;
        // Held to 0..1 by its widget's range, written or wired.
        let threshold: f64 = ctx.inputs.get("threshold")?;
        let mut question = json!({ "type": "noul", "instructions": instructions });
        match (when_true, when_false) {
            (Some(yes), Some(no)) => question["criteria"] = json!({ "true": yes, "false": no }),
            (None, None) => {}
            _ => weft::node_bail!(
                "whenTrue and whenFalse go together: the model reads one against the other, so \
                 write both or neither"
            ),
        }
        let state = decision::state(&ctx).await?;
        let answer = decision::ask(&ctx, state, question).await?;
        let probability = decision::number(&answer, "noul")?;
        ctx.pulse_downstream(
            NodeOutput::new().set("probability", probability).set("yes", probability >= threshold),
        )
        .await
    }
}
