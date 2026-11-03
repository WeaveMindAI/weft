//! LinkOptions: set how long the links made from a stored file live.
//! The lifetime rides the file's value (`linkSecs`), never the stored
//! file, so this writes nothing anywhere: it emits the same value with
//! the new setting.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::storage::StoredFile;
use weft::weft_type::FileKind;
use weft::{Node, NodeManifest, RunContext, WeftResult, WeftType};

#[derive(NodeManifest)]
pub struct LinkOptionsNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for LinkOptionsNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let raw = ctx.inputs.raw("file").ok_or_else(|| weft::node_error("no file was given"))?;
        let mut file = StoredFile::from_value(raw)?;
        // The kind the value came in as, never one read again from the
        // mime: a Blob holding a png stays a Blob, as the output declares.
        let kind = raw.as_object().and_then(FileKind::from_marker_obj).expect("a stored-file value carries its marker");
        let minutes: Option<u64> = ctx.inputs.opt("minutes")?;
        file.link_secs = minutes.map(|minutes| minutes * 60);
        let file = WeftType::file_marker(kind, serde_json::to_value(&file).expect("StoredFile serializes"));
        ctx.pulse_downstream(NodeOutput::new().set("file", file)).await
    }
}
