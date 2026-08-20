//! RowFeed: the motivating Generator producer. One body walks the rows
//! with a plain Rust cursor, DECIDES per row whether to yield it
//! (comments are skipped), and each yield waits for its pull, so the
//! feed runs in lock-step with whatever consumes the stream. When the
//! body returns, the engine closes the stream.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::{ExecutionContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct RowFeedNode;

#[async_trait]
impl Node for RowFeedNode {
    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let rows: Vec<String> = ctx.inputs.get("rows")?;
        for row in rows {
            if row.starts_with('#') {
                continue;
            }
            ctx.yield_downstream(NodeOutput::new().set("kept", row)).await?;
        }
        Ok(())
    }
}
