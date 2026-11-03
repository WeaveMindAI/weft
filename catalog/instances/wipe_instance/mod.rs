//! WipeInstance: remove everything an instance has in this program, in
//! the order that leaves nothing half-attached: its triggers first
//! (refusing what arrives, what they held dropped, its runs cancelled), then the listed infra copies (terminated
//! with every disk, the ones the node keeps through a terminate too), the
//! values it was given, its connections, its tokens, its files, and last
//! its runs. Each step is safe to repeat, so a wipe that stopped part way
//! finishes when run again.

use async_trait::async_trait;

use weft::instance::InstanceId;
use weft::node::NodeOutput;
use weft::{RunContext, Node, NodeManifest, WeftResult};

use weft::storage::{FileHandle, StorageScope};
use weft::program::StillGoing;
use weft::{StopSelf, TakeDown};

use super::lifecycle::stop_self;

#[derive(NodeManifest)]
pub struct WipeInstanceNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for WipeInstanceNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let instance = ctx.inputs.get::<InstanceId>("instance")?;
        let infra: Vec<String> = ctx.inputs.opt("infra")?.unwrap_or_default();
        // Nothing kept: the door refuses, what is held is dropped, the
        // runs going are cancelled.
        let wipe = TakeDown::REMOVE.choice();
        // Every step but the last keeps this run: it still has work to
        // do. Whether it goes at the end is the node's choice.
        ctx.triggers().instance(instance.clone()).deactivate(wipe, StopSelf::Keep).await?;
        for node in infra {
            ctx.infra(node).instance(instance.clone()).wipe(wipe, StopSelf::Keep).await?;
        }
        ctx.values().instance(instance.clone()).forget().await?;
        ctx.connections().instance(instance.clone()).forget().await?;
        ctx.tokens().instance(instance.clone()).revoke().await?;
        let files = ctx.storage(StorageScope::instance_of(instance.clone()));
        for file in files.list().await? {
            files.delete(&FileHandle::key(file.key)).await?;
        }
        ctx.runs().instance(instance).clean(StillGoing::Cancel, stop_self(&ctx)?).await?;
        ctx.pulse_downstream(NodeOutput::new().set("done", true)).await
    }
}
