//! StopTagged: stop every live run of the project carrying a tag.
//!
//! The tags are whatever the user wired onto the node (each created
//! input is one tag) plus the optional `tags` list, read exactly the way
//! `TagRun` reads its own. `includeSelf` picks `StopSelf::Include` over
//! the default `Keep`. Each tag is one `ctx.stop_tagged` call; the
//! runtime orders them so a stop only reaches runs tagged before this
//! one was, which is what makes the debounce (a new message stopping the
//! answer to the previous one) correct when two messages land a moment
//! apart.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::{ExecutionContext, Node, NodeManifest, StopSelf, WeftResult};

use super::steering::tags_from_inputs;

#[derive(NodeManifest)]
pub struct StopTaggedNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for StopTaggedNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let include_self: bool = ctx.inputs.get("includeSelf")?;
        let stop_self = if include_self { StopSelf::Include } else { StopSelf::Keep };
        let tags = tags_from_inputs(&ctx)?;
        for tag in &tags {
            ctx.stop_tagged(tag.as_str(), stop_self).await?;
        }
        ctx.pulse_downstream(NodeOutput::new().set("done", true)).await
    }
}
