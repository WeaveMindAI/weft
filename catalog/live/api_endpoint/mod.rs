//! ApiEndpoint: trigger node that turns a weft program into a live HTTP
//! endpoint. An outside caller makes a request at the project's gateway
//! URL; the dispatcher holds the connection open and routes it to a worker;
//! this trigger fires a fresh execution whose nodes reply or stream back to
//! the caller via `ctx.caller()`.
//!
//!   - `setup_trigger`: build an `ApiEndpoint` signal from the node's
//!     fields and register it. The dispatcher mounts the public route.
//!   - `run`: the caller is already attached for this run; this node
//!     just kicks the graph (downstream nodes drive `ctx.caller()`),
//!     emitting a `started` pulse.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::signal::{ApiEndpoint, LiveConnectionConfig};
use weft::{ExecutionContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct ApiEndpointNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for ApiEndpointNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn setup_trigger(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let common = LiveConnectionConfig::from_node_fields(ctx.inputs.object()?);
        ctx.register_signal(ApiEndpoint { common }).await
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        ctx.pulse_downstream(NodeOutput::new().set("started", true)).await
    }
}
