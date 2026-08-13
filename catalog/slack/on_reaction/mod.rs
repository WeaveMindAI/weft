//! SlackOnReaction: fires when an emoji reaction is added to (or
//! removed from) a message. The emoji-driven workflow trigger: mark a
//! message with :eyes: and something happens.
//!
//! Same two-phase shape as SlackReceiveMessage: `setup_trigger`
//! translates the filter inputs into predicates over the service's
//! NAMED event fields and registers one subscription on the
//! `reactions` topic; `run` fans the wake payload's declared ports.
//!
//! The event only carries the message's ADDRESS (channel + ts), never
//! its text; wire SlackGetThread (or a message read) downstream when
//! the content is needed.

use async_trait::async_trait;

use weft::signal::{Predicate, ProviderEvents};
use weft::{Access, ExecutionContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct SlackOnReactionNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for SlackOnReactionNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn setup_trigger(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let channel: Option<String> = ctx.inputs.opt("channel")?;
        let emoji: Option<String> = ctx.inputs.opt("emoji")?;
        let direction: String = ctx.inputs.get("direction")?;

        let mut filters = vec![match direction.as_str() {
            "added" => Predicate::eq("type", "reaction_added"),
            "removed" => Predicate::eq("type", "reaction_removed"),
            // Both directions: one anchored pattern instead of two
            // subscriptions.
            _ => Predicate::regex("type", "^reaction_(added|removed)$"),
        }];
        if let Some(c) = channel.filter(|c| !c.trim().is_empty()) {
            filters.push(Predicate::eq("channel", c));
        }
        if let Some(e) = emoji.filter(|e| !e.trim().is_empty()) {
            filters.push(Predicate::eq("emoji", e.trim_matches(':')));
        }

        ctx.register_signal(ProviderEvents::new(&account, "reactions", filters)).await
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        ctx.pulse_downstream(ctx.fan_declared(&ctx.wake.record()?)).await
    }
}
