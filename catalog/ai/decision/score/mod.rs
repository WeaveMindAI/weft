//! DecisionScore: ask a decision model where the state the program's
//! added ports carry falls on an ordered scale (the Decisions API's
//! `score` question), and hand back the probability-weighted position,
//! each level's probability, and the most likely level.
//!
//! The state, the provider and the call are the package's `decision.rs`.

use async_trait::async_trait;
use serde_json::json;

use weft::node::NodeOutput;
use weft::{RunContext, Node, NodeManifest, WeftResult};

use super::decision;

#[derive(NodeManifest)]
pub struct DecisionScoreNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for DecisionScoreNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let instructions: String = ctx.inputs.get("instructions")?;
        let levels: Vec<String> = ctx.inputs.get("levels")?;
        if levels.len() < 2 {
            weft::node_bail!("levels holds {} level(s), and a scale needs at least two", levels.len());
        }
        let state = decision::state(&ctx).await?;
        let question = json!({ "type": "score", "instructions": instructions, "criteria": levels });
        let answer = decision::ask(&ctx, state, question).await?;
        let score = decision::number(&answer, "score")?;
        let top = (levels.len() - 1) as f64;
        if !(0.0..=top).contains(&score) {
            weft::node_bail!("openrouter: the decision model answered a score of {score}, outside the levels' 0 to {top}");
        }
        let probabilities = level_probabilities(decision::probabilities(&answer)?, levels.len())?;
        let confidence = decision::number(&answer, "confidence")?;
        let level = &levels[most_likely(&probabilities)];
        ctx.pulse_downstream(
            NodeOutput::new()
                .set("score", score)
                .set("level", level.as_str())
                .set("probabilities", json!(probabilities))
                .set("confidence", confidence),
        )
        .await
    }
}

/// The answer's probabilities, keyed by level index (`"0"`, `"1"`...),
/// as a list in level order. Every level must have one.
pub fn level_probabilities(
    answered: &serde_json::Map<String, serde_json::Value>,
    levels: usize,
) -> WeftResult<Vec<f64>> {
    (0..levels)
        .map(|i| {
            answered.get(&i.to_string()).and_then(|p| p.as_f64()).ok_or_else(|| {
                weft::node_error(format!("openrouter: the decision model answered no probability for level {i}"))
            })
        })
        .collect()
}

/// The index of the most likely level; on a tie, the lower one.
pub fn most_likely(probabilities: &[f64]) -> usize {
    probabilities
        .iter()
        .enumerate()
        .fold(0, |best, (i, p)| if *p > probabilities[best] { i } else { best })
}
