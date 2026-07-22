//! LiveSocket: trigger node that turns a weft program into a live WebSocket
//! endpoint. An outside caller opens a WebSocket at the project's gateway
//! URL; the dispatcher routes the held socket to a worker; this trigger
//! fires a fresh execution whose nodes hold a two-way conversation with the
//! caller via `ctx.caller()` (send / receive / request / close).
//!
//!   - `setup_trigger`: build a `LiveSocket` signal from the node's
//!     fields and register it. The dispatcher mounts the public route.
//!   - `run`: the caller is already attached for this run; this node
//!     just kicks the graph (downstream nodes drive `ctx.caller()`),
//!     emitting a `started` pulse.

use async_trait::async_trait;

use weft_core::node::NodeOutput;
use weft_core::signal::{LiveConnectionConfig, LiveSocket};
use weft_core::{ExecutionContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct LiveSocketNode;

#[async_trait]
impl Node for LiveSocketNode {
    async fn setup_trigger(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let common = LiveConnectionConfig::from_node_fields(ctx.config.object()?);
        ctx.register_signal(LiveSocket { common }).await
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        ctx.pulse_downstream(NodeOutput::new().set("started", true)).await
    }
}
