//! FolderWatch: fires a run when a file in a folder on this machine is
//! created, changed or removed.
//!
//!   - `setup_trigger`: register a folder_watch signal for the folder,
//!     with the name pattern and the changes that fire. The listener
//!     refuses it on an install whose holders are not on this machine,
//!     and when the folder is not there.
//!
//!   - `run`: the listener delivers `{path, event, sizeBytes?}` as the
//!     wake payload, fanned onto the outputs.

use async_trait::async_trait;

use weft::signal::{FolderEvent, FolderWatch};
use weft::{RunContext, Node, NodeManifest, WeftResult};

#[cfg(feature = "node-tests")]
mod tests;

#[derive(NodeManifest)]
pub struct FolderWatchNode;

#[async_trait]
impl Node for FolderWatchNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn setup_trigger(&self, ctx: RunContext) -> WeftResult<()> {
        let folder: String = ctx.inputs.get("folder")?;
        let recursive: bool = ctx.inputs.get("recursive")?;
        let pattern: Option<String> = ctx.inputs.opt("pattern")?;
        // Left unset is the kind's default; a list emptied by hand goes
        // through as it is, and the kind refuses it.
        let events = ctx.inputs.get_or::<Vec<FolderEvent>>("events", FolderWatch::every_event())?;
        // Registers the signal; setup emits nothing downstream.
        ctx.register_signal(FolderWatch {
            folder,
            recursive,
            pattern,
            events,
        })
        .await
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        // `path` and `event` are on every fire (`firesWith` refuses a
        // delivery without one); `sizeBytes` is absent for a removed file,
        // so its port closes then.
        ctx.pulse_downstream(ctx.fan_declared(&ctx.wake.record()?)).await
    }
}
