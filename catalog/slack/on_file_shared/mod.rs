//! SlackOnFileShared: fires when a file is shared in a channel. The
//! event names the file (F... id) and where it landed, never the
//! bytes; wire SlackDownloadFile downstream to pull the content.

use async_trait::async_trait;

use weft::signal::{Predicate, ProviderEvents};
use weft::{Access, ExecutionContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct SlackOnFileSharedNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for SlackOnFileSharedNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn setup_trigger(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let channel: Option<String> = ctx.inputs.opt("channel")?;

        let mut filters = vec![Predicate::eq("type", "file_shared")];
        if let Some(c) = channel.filter(|c| !c.trim().is_empty()) {
            filters.push(Predicate::eq("channel", c));
        }

        ctx.register_signal(ProviderEvents::new(&account, "files", filters)).await
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        ctx.pulse_downstream(ctx.fan_declared(&ctx.wake.record()?)).await
    }
}
