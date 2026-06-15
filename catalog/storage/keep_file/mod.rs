//! KeepFile: flag a stored execution file to survive the end-of-run
//! sweep, then pass the reference through. The keep flag is additive
//! (no un-keep), so a "create many, keep the good one" pipeline ends
//! by routing the keeper through this node; everything un-kept is
//! swept when the run terminates.

use async_trait::async_trait;

use weft_core::node::NodeOutput;
use weft_core::storage::{KeepTtl, StorageScope};
use weft_core::{ExecutionContext, Node, NodeMetadata, WeftResult};

pub struct KeepFileNode;

const METADATA_JSON: &str = include_str!("metadata.json");

#[async_trait]
impl Node for KeepFileNode {
    fn node_type(&self) -> &'static str {
        "KeepFile"
    }

    fn metadata(&self) -> NodeMetadata {
        serde_json::from_str(METADATA_JSON).expect("KeepFile metadata.json must be valid")
    }

    async fn execute(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let file = ctx.input.raw("file").cloned().ok_or_else(|| {
            weft_core::WeftError::Input("KeepFile: no value on input port 'file'".into())
        })?;
        // 0 days = never expire; otherwise a fixed-day window that any
        // access renews. The scope is Execution because keep only
        // applies there (the box rejects keep on project/shared keys);
        // the key itself carries its scope, so the handle's scope here
        // is just the verb's home.
        let ttl_days: u64 = ctx.config.get("ttl_days").unwrap_or(30);
        let ttl = if ttl_days == 0 {
            KeepTtl::Never
        } else {
            KeepTtl::Secs { secs: ttl_days * 24 * 3600 }
        };
        ctx.storage(StorageScope::Execution).keep(&file, ttl).await?;
        ctx.pulse_downstream(NodeOutput::with("file", file)).await
    }
}
