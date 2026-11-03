//! AwaitInteraction: e2e probe for the awaited-provider-push path.
//! Parks on the connection's `interactions` topic with a FIXED
//! correlation id (the rig's synthetic payload carries the same one)
//! and emits the clicked action on resume. The Slack-message posting
//! that the real approval node does is deliberately absent: this
//! fixture proves the park + signed-push + resume machinery alone.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use weft::node::NodeOutput;
use weft::signal::{Predicate, PredicateOp, ProviderEvents};
use weft::{Access, RunContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct AwaitInteractionNode;

/// The click, once it came: the body run again after the park finds the
/// wait answered.
#[derive(Default, Serialize, Deserialize)]
struct Clicked {
    click: Option<Value>,
}

#[async_trait]
impl Node for AwaitInteractionNode {
    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let access: Access = ctx.inputs.get("account")?;
        let mut state = ctx.keep(Clicked::default()).await?;
        let click = state
            .wait(
                |s| &mut s.click,
                ProviderEvents::new(
                    &access,
                    "interactions",
                    vec![Predicate {
                        field: "callbackId".into(),
                        op: PredicateOp::Eq,
                        value: Some("cb-e2e".into()),
                    }],
                ),
            )
            .await?;
        let action = click["action"].as_str().unwrap_or_default().to_string();
        let user = click["user"].as_str().unwrap_or_default().to_string();
        ctx.pulse_downstream(NodeOutput::new().set("action", action).set("user", user)).await
    }
}
