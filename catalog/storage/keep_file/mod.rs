//! KeepFile: flag a stored execution file to survive the end-of-run
//! sweep, then pass the reference through. The keep flag is additive
//! (no un-keep), so a "create many, keep the good one" pipeline ends
//! by routing the keeper through this node; everything un-kept is
//! swept when the run terminates.

use async_trait::async_trait;

use weft_core::node::NodeOutput;
use weft_core::storage::{KeepTtl, StorageScope};
use weft_core::{ExecutionContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct KeepFileNode;

#[async_trait]
impl Node for KeepFileNode {
    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let file: serde_json::Value = ctx.inputs.get("file")?;
        // 0 days = never expire; otherwise a fixed-day window that any
        // access renews. The scope is Execution because keep only
        // applies there (the box rejects keep on project/shared keys);
        // the key itself carries its scope, so the handle's scope here
        // is just the verb's home.
        // `ttl_days` declares a metadata default, so the bag always
        // holds a value; a required read keeps the default in ONE place.
        let ttl_days: u64 = ctx.inputs.get("ttl_days")?;
        let ttl = if ttl_days == 0 {
            KeepTtl::Never
        } else {
            KeepTtl::Secs { secs: ttl_days * 24 * 3600 }
        };
        ctx.storage(StorageScope::Execution).keep(&file, ttl).await?;
        ctx.pulse_downstream(NodeOutput::new().set("file", file)).await
    }
}
