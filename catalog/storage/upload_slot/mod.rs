//! UploadSlot: hand a page a slot its visitor uploads one file through.
//! The upload and its receiving are the storage handle's
//! (`StorageHandle::upload_slot`, `claim_uploads`); this node only reads
//! its settings into one.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::storage::StorageScope;
use weft::{Node, NodeManifest, RunContext, WeftError, WeftResult};

use super::lifetime;

/// One megabyte, as `maxMb` counts it.
const MB: f64 = 1024.0 * 1024.0;

#[derive(NodeManifest)]
pub struct UploadSlotNode;

#[cfg(feature = "node-tests")]
mod tests;

/// The scope a `scope` setting names: `project`, `instance` (the run's
/// own) or `run`.
fn slot_scope(name: &str) -> WeftResult<StorageScope> {
    StorageScope::named(name)
        .ok_or_else(|| WeftError::Input(format!("`scope` is '{name}'; it is `project`, `instance` or `run`")))
}

#[async_trait]
impl Node for UploadSlotNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let max_mb: f64 = ctx.inputs.get("maxMb")?;
        if !(max_mb.is_finite() && max_mb > 0.0) {
            return Err(WeftError::Input(format!("`maxMb` is {max_mb}; a file weighs more than nothing")));
        }
        let accepts: String = ctx.inputs.get("types")?;
        let minutes: u64 = ctx.inputs.get("minutes")?;
        let scope = slot_scope(&ctx.inputs.get::<String>("scope")?)?;
        // No default on purpose: absent is the scope's own lifetime.
        let ttl_days: Option<u64> = ctx.inputs.opt("ttlDays")?;
        let slot = ctx
            .storage(scope)
            .upload_slot((max_mb * MB).ceil() as u64, &accepts, minutes, ttl_days.map(lifetime::ttl_of_days))
            .await?;
        ctx.pulse_downstream(NodeOutput::new().set("slot", slot)).await
    }
}
