//! KeepFile: make a stored execution file outlive its run, one of two
//! ways. `scope: execution` flags the file to survive the end-of-run
//! sweep (additive, no un-keep) and passes the reference through, so
//! a "create many, keep the good one" pipeline ends by routing the
//! keeper through this node. `scope: project` copies the file into
//! the project scope and emits the NEW reference: an execution file
//! is walled to the run that made it, so the copy is the only form a
//! later run can read. Either way `ttl_days` is how long the kept file
//! lives once nobody touches it.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::storage::{FileHandle, KeepTtl, StorageScope, StoredFile};
use weft::{ExecutionContext, Node, NodeManifest, WeftResult};

use super::lifetime;

#[derive(NodeManifest)]
pub struct KeepFileNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for KeepFileNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let file: FileHandle = ctx.inputs.get("file")?;
        // `ttl_days` has no metadata default on purpose: absent means
        // the scope's own answer (the storage default for a kept run
        // file, no expiry for a project copy).
        let ttl_days: Option<u64> = ctx.inputs.opt("ttl_days")?;
        let scope_name: String = ctx.inputs.get("scope")?;
        let output = match lifetime::scope_named(&scope_name)? {
            StorageScope::Execution => {
                // Absent = the storage default (30 days, owned there);
                // 0 days = never expire; otherwise a fixed-day window
                // that any access renews. The key carries its own
                // scope, so the handle's scope here is just the verb's
                // home.
                let ttl = ttl_days.map_or(KeepTtl::Default, lifetime::ttl_of_days);
                ctx.storage(StorageScope::Execution).keep(&file, ttl).await?;
                // Pass the MARKER through untouched: downstream gets
                // exactly the reference that arrived, not a
                // reconstruction.
                ctx.inputs.raw("file").cloned().expect("file was just read")
            }
            // The copy is a NEW file under the project prefix; the
            // run-scoped original is still swept with its run. With no
            // `ttl_days` it lives until deleted.
            scope => ctx.storage(scope).copy(&file, ttl_days.map(lifetime::ttl_of_days)).await?,
        };
        ctx.pulse_downstream(NodeOutput::stored_file(StoredFile::from_value(&output)?)).await
    }
}
