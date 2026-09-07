//! SlackReceiveMessage: fires when a message lands in a Slack channel.
//!
//!   - `setup_trigger`: translate the node's plain filter inputs into
//!     predicates over the service's NAMED event fields and register
//!     one event subscription. Which transport serves it (the app's
//!     own socket, or Slack pushing to this weft) is the language's
//!     decision, made from the connection; this node never knows.
//!
//!   - `run`: the wake payload is the named event; fan the declared
//!     output ports.

use async_trait::async_trait;

use weft::signal::{Predicate, ProviderEvents};
use weft::{Access, ExecutionContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct SlackReceiveMessageNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for SlackReceiveMessageNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn setup_trigger(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let channel: String = ctx.inputs.get("channel")?;
        let keyword: Option<String> = ctx.inputs.opt("keyword")?;
        let pattern: Option<String> = ctx.inputs.opt("pattern")?;
        let from_user: Option<String> = ctx.inputs.opt("fromUser")?;
        let include_bots: bool = ctx.inputs.get("includeBots")?;
        let replies: String = ctx.inputs.get("replies")?;

        let mut filters = vec![Predicate::eq("type", "message"), Predicate::eq("channel", channel)];
        if let Some(k) = keyword.filter(|k| !k.trim().is_empty()) {
            filters.push(Predicate::contains("text", k));
        }
        if let Some(p) = pattern.filter(|p| !p.trim().is_empty()) {
            filters.push(Predicate::regex("text", p));
        }
        if let Some(u) = from_user.filter(|u| !u.trim().is_empty()) {
            filters.push(Predicate::eq("user", u));
        }
        if !include_bots {
            // A bot message carries the `bot` field; a human's does not.
            filters.push(Predicate::not_exists("bot"));
        }
        match replies.as_str() {
            // A thread reply carries the `thread` field; a top-level
            // message does not.
            "top_level" => filters.push(Predicate::not_exists("thread")),
            "thread_replies" => filters.push(Predicate::exists("thread")),
            "all" => {}
            other => weft::node_bail!("replies must be top_level, thread_replies or all, got {other:?}"),
        }

        ctx.register_signal(ProviderEvents::new(&account, "messages", filters)).await
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        // The wake payload is the service's named event; fan the
        // declared output ports. Missing fields stay un-mentioned and
        // close at termination.
        ctx.pulse_downstream(ctx.fan_declared(&ctx.wake.record()?)).await
    }
}
