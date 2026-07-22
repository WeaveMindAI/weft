//! Text: emit a literal string configured at design time.

use async_trait::async_trait;

use weft_core::{ExecutionContext, Node, NodeManifest, WeftResult};
use weft_core::node::NodeOutput;

#[derive(NodeManifest)]
pub struct TextNode;

#[async_trait]
impl Node for TextNode {
    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let value: String = ctx.config.get("value")?;
        ctx.pulse_downstream(NodeOutput::new().set("value", value)).await
    }
}
