//! SlackAppMessages: fires for messages in EVERY workspace that
//! installed your Slack app.
//!
//! The APP-OWNER expectation, deliberately a separate node from
//! SlackReceiveMessage (the your-own-bot expectation): here every
//! install's events are yours and the payload says WHICH workspace,
//! there a single workspace's channel is picked and watched. One node
//! auto-detecting between the two would answer different questions
//! depending on the wiring, which is exactly what a node must not do.
//!
//!   - `setup_trigger`: register one APP-WIDE event subscription. The
//!     app-wide scope only works over the service's dial-out socket
//!     (weft holds the line with the app's own token), so activation
//!     tells the user exactly what to paste when the connection
//!     cannot serve it.
//!
//!   - `run`: fan the named event; `workspace` is which install.

use async_trait::async_trait;
use serde_json::Value;

use weft::signal::{Predicate, ProviderEvents};
use weft::{Access, ExecutionContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct SlackAppMessagesNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for SlackAppMessagesNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn setup_trigger(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let keyword: Option<String> = ctx.inputs.opt("keyword")?;
        let include_bots: bool = ctx.inputs.get("includeBots")?;

        let mut filters = vec![Predicate::eq("type", "message")];
        if let Some(k) = keyword.filter(|k| !k.trim().is_empty()) {
            filters.push(Predicate::contains("text", k));
        }
        if !include_bots {
            filters.push(Predicate::not_exists("bot"));
        }
        ctx.register_signal(ProviderEvents::new(&account, "messages", filters).app_wide()).await
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        ctx.pulse_downstream(ctx.fan_declared(&ctx.wake.record()?)).await
    }
}
