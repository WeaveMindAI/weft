//! Shout: a node that lives beside the file that uses it (`src/lib/`),
//! not under `nodes/`. Upper-cases its text. Exists to prove a node
//! beside the code is found by the catalog and compiled into the worker.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::{ExecutionContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct ShoutNode;

#[async_trait]
impl Node for ShoutNode {
    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let text: String = ctx.inputs.get("text")?;
        ctx.pulse_downstream(NodeOutput::new().set("loud", text.to_uppercase())).await
    }
}
