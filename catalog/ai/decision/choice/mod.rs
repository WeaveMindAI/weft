//! DecisionChoice: ask a decision model which one of several options
//! fits the state the program's added ports carry (the Decisions API's
//! `choice` question), and hand back the option with each option's
//! probability.
//!
//! The state, the provider and the call are the package's `decision.rs`.

use async_trait::async_trait;
use serde_json::{json, Map, Value};

use weft::node::NodeOutput;
use weft::{RunContext, Node, NodeManifest, WeftResult};

use super::decision;

#[derive(NodeManifest)]
pub struct DecisionChoiceNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for DecisionChoiceNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let instructions: String = ctx.inputs.get("instructions")?;
        let options = options(ctx.inputs.get("options")?)?;
        let state = decision::state(&ctx).await?;
        let question = json!({ "type": "choice", "instructions": instructions, "criteria": options });
        let answer = decision::ask(&ctx, state, question).await?;
        let Some(choice) = answer["choice"].as_str() else {
            weft::node_bail!("openrouter: the decision model's answer carries no `choice`");
        };
        if !options.contains_key(choice) {
            weft::node_bail!("openrouter: the decision model chose `{choice}`, which is not one of the options");
        }
        let probabilities = decision::probabilities(&answer)?;
        if let Some(missing) = options.keys().find(|option| !probabilities.contains_key(*option)) {
            weft::node_bail!("openrouter: the decision model answered no probability for `{missing}`");
        }
        if let Some(stray) = probabilities.keys().find(|name| !options.contains_key(*name)) {
            weft::node_bail!("openrouter: the decision model answered a probability for `{stray}`, which is not one of the options");
        }
        let confidence = decision::number(&answer, "confidence")?;
        ctx.pulse_downstream(
            NodeOutput::new()
                .set("choice", choice)
                .set("probabilities", Value::Object(probabilities.clone()))
                .set("confidence", confidence),
        )
        .await
    }
}

/// The `options` input as the question's criteria: an object of option
/// to what it covers as it is, a list of names as options with nothing
/// written for them.
pub fn options(written: Value) -> WeftResult<Map<String, Value>> {
    let options: Map<String, Value> = match written {
        Value::Object(options) => options,
        Value::Array(names) => {
            let mut options = Map::new();
            for name in names {
                let Value::String(name) = name else {
                    weft::node_bail!("options lists {name}, and an option is named by a string");
                };
                if options.insert(name.clone(), Value::Null).is_some() {
                    weft::node_bail!("options lists `{name}` twice");
                }
            }
            options
        }
        other => weft::node_bail!("options is {other}; it takes a list of names or an object of name to description"),
    };
    if options.len() < 2 {
        weft::node_bail!("options holds {} option(s), and a choice needs at least two", options.len());
    }
    Ok(options)
}
