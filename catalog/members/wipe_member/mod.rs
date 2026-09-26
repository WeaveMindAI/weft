//! WipeMember: remove everything a member has in this program, in the
//! order that leaves nothing half-attached: their triggers first (wiped,
//! their runs cancelled), then the listed infra copies (terminated with
//! their disks), the values they gave, their connections, their tokens,
//! their files, and last their runs. Each step is safe to repeat, so a wipe
//! that stopped part way finishes when run again.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::{ExecutionContext, Node, NodeManifest, WeftResult};

use weft::storage::{FileHandle, StorageScope};
use weft::{DeactivateSpec, DeactivationMode, RunningPolicy, StopSelf};

use super::lifecycle::{member, stop_self};

#[derive(NodeManifest)]
pub struct WipeMemberNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for WipeMemberNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let member = member(&ctx)?;
        let infra: Vec<String> = ctx.inputs.opt("infra")?.unwrap_or_default();
        let wipe = DeactivateSpec {
            mode: DeactivationMode::Wipe,
            grace_minutes: 0,
            running_policy: RunningPolicy::Cancel,
            drain_timeout_secs: None,
        };
        // Every step but the last keeps this run: it still has work to
        // do. Whether it goes at the end is the node's choice.
        ctx.triggers().member(member.clone()).deactivate(wipe.clone(), StopSelf::Keep).await?;
        for node in infra {
            ctx.infra(node).member(member.clone()).terminate(wipe.clone(), StopSelf::Keep).await?;
        }
        ctx.values().member(member.clone()).forget().await?;
        ctx.connections().member(member.clone()).forget().await?;
        ctx.tokens().member(member.clone()).revoke().await?;
        let files = ctx.storage(StorageScope::member_of(member.clone()));
        for file in files.list().await? {
            files.delete(&FileHandle::Key(file.key)).await?;
        }
        ctx.runs().member(member).clean(RunningPolicy::Cancel, stop_self(&ctx)?).await?;
        ctx.pulse_downstream(NodeOutput::new().set("done", true)).await
    }
}
