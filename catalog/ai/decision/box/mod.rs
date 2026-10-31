//! DecisionBox: ask a decision model where one described thing is in an
//! image the state carries (the Decisions API's `box` question), and
//! hand back its box as fractions of the image.
//!
//! A box always comes back, found or not, so a state with no image at
//! all is refused before anything is paid. The state, the provider and
//! the call are the package's `decision.rs`.

use async_trait::async_trait;
use serde_json::json;

use weft::node::NodeOutput;
use weft::{RunContext, Node, NodeManifest, WeftResult};

use super::decision;

#[derive(NodeManifest)]
pub struct DecisionBoxNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for DecisionBoxNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let instructions: String = ctx.inputs.get("instructions")?;
        let state = decision::state(&ctx).await?;
        if state.images == 0 {
            weft::node_bail!(
                "there is no image to look in: add a port typed Image (`DecisionBox(photo: Image)`) \
                 and wire the picture to it"
            );
        }
        let answer = decision::ask(&ctx, state, json!({ "type": "box", "instructions": instructions })).await?;
        let found = &answer["box"];
        let [top, bottom, left, right] = ["top", "bottom", "left", "right"].map(|edge| decision::number(found, edge));
        let (top, bottom, left, right) = (top?, bottom?, left?, right?);
        let within = |edge: f64| (0.0..=1.0).contains(&edge);
        if !(within(top) && within(bottom) && within(left) && within(right) && top <= bottom && left <= right) {
            weft::node_bail!(
                "openrouter: the decision model answered a box that is no box (top {top}, bottom {bottom}, \
                 left {left}, right {right}): each edge runs from 0 to 1, top above bottom, left of right"
            );
        }
        let edges = json!({ "top": top, "bottom": bottom, "left": left, "right": right });
        let confidence = decision::number(&answer, "confidence")?;
        ctx.pulse_downstream(
            NodeOutput::new().set("box", edges).set("confidence", confidence),
        )
        .await
    }
}
