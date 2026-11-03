//! UploadSlot: hand a page a slot its visitor uploads one file through.
//! The upload and its receiving are the storage handle's
//! (`StorageHandle::upload_slot`, `claim_uploads`); this node only reads
//! its settings into one.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::{Node, NodeManifest, RunContext, WeftResult};

use super::lifetime;

/// One megabyte, as `maxMb` counts it.
const MB: u64 = 1024 * 1024;

#[derive(NodeManifest)]
pub struct UploadSlotNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for UploadSlotNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let max_mb: u64 = ctx.inputs.get("maxMb")?;
        let accepts: String = ctx.inputs.get("types")?;
        let minutes: u64 = ctx.inputs.get("minutes")?;
        let scope = ctx.inputs.get::<lifetime::Scope>("scope")?;
        // No default on purpose: absent is the scope's own lifetime.
        let ttl_days: Option<u64> = ctx.inputs.opt("ttlDays")?;
        let slot = ctx
            .storage(scope.into())
            .upload_slot(max_mb * MB, &accepts, minutes, ttl_days.map(lifetime::ttl_of_days))
            .await?;
        ctx.pulse_downstream(NodeOutput::new().set("slot", slot)).await
    }
}
