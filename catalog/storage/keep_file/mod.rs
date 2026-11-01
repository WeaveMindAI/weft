//! KeepFile: make a stored file last longer, one of two ways.
//! `scope: project` copies the file into the project's files and emits
//! the NEW reference: a run's file goes when its run's record is deleted,
//! a project's when the project is. `scope: run` leaves the file where it
//! is and sets its deadline instead. Either way `ttlDays` is the deadline
//! (the file is deleted once nobody touched it that long, or with its
//! owner if that comes first); empty or `0`, it has none.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::storage::{FileHandle, KeepTtl, StorageScope, StoredFile};
use weft::{RunContext, Node, NodeManifest, WeftResult};

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

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let file: FileHandle = ctx.inputs.get("file")?;
        let ttl_days: Option<u64> = ctx.inputs.opt("ttlDays")?;
        // No deadline unless `ttlDays` names one: the file then lives as
        // long as its scope says.
        let ttl = ttl_days.map_or(KeepTtl::Never, lifetime::ttl_of_days);
        let scope_name: String = ctx.inputs.get("scope")?;
        let output = match lifetime::scope_named(&scope_name)? {
            StorageScope::Run => {
                // The key carries its own scope, so the handle's scope here
                // is just the verb's home.
                ctx.storage(StorageScope::Run).keep(&file, ttl).await?;
                // Pass the MARKER through untouched: downstream gets
                // exactly the reference that arrived, not a
                // reconstruction.
                ctx.inputs.raw("file").cloned().expect("file was just read")
            }
            // The copy is a NEW file under the project prefix; the
            // original is left as it was, deleted with its own owner.
            scope => ctx.storage(scope).copy(&file, Some(ttl)).await?,
        };
        ctx.pulse_downstream(NodeOutput::stored_file(StoredFile::from_value(&output)?)).await
    }
}
