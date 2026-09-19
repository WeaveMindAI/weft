//! KeepFile: make a stored execution file outlive its run, one of two
//! ways. `scope: execution` flags the file to survive the end-of-run
//! sweep (additive, no un-keep) and passes the reference through, so
//! a "create many, keep the good one" pipeline ends by routing the
//! keeper through this node. `scope: project` copies the file into
//! the project scope and emits the NEW reference: an execution file
//! is walled to the run that made it, so the copy is the only form a
//! later run can read.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::storage::{FileHandle, KeepTtl, StorageScope};
use weft::{ExecutionContext, Node, NodeManifest, WeftResult};

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
        // the storage service's own default (30 days, owned there),
        // and "was it set" is what the project branch refuses on.
        let ttl_days: Option<u64> = ctx.inputs.opt("ttl_days")?;
        let scope_name: String = ctx.inputs.get("scope")?;
        let output = match scope_name.as_str() {
            "execution" => {
                // 0 days = never expire; otherwise a fixed-day window
                // that any access renews. Keep only applies to
                // execution files (the box rejects it on project/shared
                // keys); the key carries its own scope, so the handle's
                // scope here is just the verb's home.
                let ttl = match ttl_days {
                    None => KeepTtl::Default,
                    Some(0) => KeepTtl::Never,
                    Some(days) => KeepTtl::Secs { secs: days * 24 * 3600 },
                };
                ctx.storage(StorageScope::Execution).keep(&file, ttl).await?;
                // Pass the MARKER through untouched: downstream gets
                // exactly the reference that arrived, not a
                // reconstruction.
                ctx.inputs.raw("file").cloned().expect("file was just read")
            }
            "project" => {
                // A project file is persistent already, so a ttl with
                // it is a contradiction the author should see rather
                // than a number quietly dropped.
                if ttl_days.is_some() {
                    weft::node_bail!(
                        "`ttl_days` is set with `scope: project`; a project file already outlives \
                         the run, so drop `ttl_days` or use `scope: execution`"
                    );
                }
                // The copy is a NEW file under the project prefix; the
                // run-scoped original is still swept with its run.
                ctx.storage(StorageScope::Project).copy(&file, None).await?
            }
            other => weft::node_bail!("`scope` is '{other}'; it is `execution` or `project`"),
        };
        ctx.pulse_downstream(NodeOutput::new().set("file", output)).await
    }
}
