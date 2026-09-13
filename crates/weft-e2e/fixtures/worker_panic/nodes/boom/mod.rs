//! A node that panics, on purpose.
//!
//! Node code is user code, and user code panics: an unwrap on an empty
//! option, an index past the end. The worker has to treat that as a
//! failed execution rather than a dead pod, and it has to release what
//! the execution registered even though an unwind skips the ordinary
//! path. Nothing else in the rig can produce a real panic inside a
//! node, so this fixture is the only way to prove either.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::{ExecutionContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct BoomNode;

#[async_trait]
impl Node for BoomNode {
    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let explode: bool = ctx.inputs.get_or("explode", true)?;
        if explode {
            panic!("boom: this node panics on purpose (e2e)");
        }
        ctx.pulse_downstream(NodeOutput::new().set("done", "no boom".to_string())).await
    }
}
