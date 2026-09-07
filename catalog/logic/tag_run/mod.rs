//! TagRun: label this run so a later run can stop it.
//!
//! The tags are whatever the user wired onto the node (each created
//! input is one tag) plus the optional `tags` list, so a program says
//! `TagRun(sender: String)` and wires the sender in; nothing is
//! assembled by hand, and any string works (see `steering::safe_tag`).
//! The node exists so the debounce shape (a new message stops the
//! answer to the previous one) is two catalog nodes and no Rust: this
//! one, then `StopTagged`.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::{ExecutionContext, Node, NodeManifest, WeftResult};

use super::steering::tags_from_inputs;

#[derive(NodeManifest)]
pub struct TagRunNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for TagRunNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let tags = tags_from_inputs(&ctx)?;
        ctx.tag_execution(tags.iter().map(String::as_str)).await?;
        ctx.pulse_downstream(NodeOutput::new().set("done", true)).await
    }
}
