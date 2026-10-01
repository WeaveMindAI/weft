//! WipeInstance: remove everything an instance has in this program, in
//! the order that leaves nothing half-attached: its triggers first
//! (wiped, its runs cancelled), then the listed infra copies (terminated
//! with every disk, the ones the node keeps through a terminate too), the
//! values it was given, its connections, its tokens, its files, and last
//! its runs. Each step is safe to repeat, so a wipe that stopped part way
//! finishes when run again.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::{ExecutionContext, Node, NodeManifest, WeftResult};

use weft::storage::{FileHandle, StorageScope};
use weft::{DeactivateSpec, DeactivationMode, RunningPolicy, StopSelf};

use super::lifecycle::{instance, stop_self};

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

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let instance = instance(&ctx)?;
        let infra: Vec<String> = ctx.inputs.opt("infra")?.unwrap_or_default();
        let wipe = DeactivateSpec {
            mode: DeactivationMode::Wipe,
            grace_minutes: 0,
            running_policy: RunningPolicy::Cancel,
            drain_timeout_secs: None,
        };
        // Every step but the last keeps this run: it still has work to
        // do. Whether it goes at the end is the node's choice.
        ctx.triggers().instance(instance.clone()).deactivate(wipe.clone(), StopSelf::Keep).await?;
        for node in infra {
            ctx.infra(node).instance(instance.clone()).wipe(wipe.clone(), StopSelf::Keep).await?;
        }
        ctx.values().instance(instance.clone()).forget().await?;
        ctx.connections().instance(instance.clone()).forget().await?;
        ctx.tokens().instance(instance.clone()).revoke().await?;
        let files = ctx.storage(StorageScope::instance_of(instance.clone()));
        for file in files.list().await? {
            files.delete(&FileHandle::Key(file.key)).await?;
        }
        ctx.runs().instance(instance).clean(RunningPolicy::Cancel, stop_self(&ctx)?).await?;
        ctx.pulse_downstream(NodeOutput::new().set("done", true)).await
    }
}
