//! Socket: trigger node that turns a weft program into a WebSocket
//! endpoint. An outside caller opens a socket at the project's live URL;
//! the dispatcher matches the path, checks the caller against the auth,
//! and routes the held socket to a worker; this trigger fires a fresh
//! execution and stays alive for the socket's life.
//!
//!   - `setup_trigger`: build a `Socket` signal from the node's fields
//!     and register it. The dispatcher mounts the public route.
//!   - `run`: the caller is attached for this run. The opening request
//!     (`ctx.wake`) fans onto the fixed ports; then every message the
//!     caller sends is yielded on `inbound` (a `Generator`, so a Loop
//!     over it runs once per message, in lock-step) until the caller
//!     disconnects, which ends the stream. Nothing here answers the
//!     caller; Reply and Close do, or a custom node through
//!     `ctx.ws_caller()`.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::signal::{LiveConnectionConfig, Socket};
use weft::{ExecutionContext, Node, NodeManifest, WeftResult};

use super::wire;

#[derive(NodeManifest)]
pub struct SocketNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for SocketNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn setup_trigger(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let common = LiveConnectionConfig::from_node_fields(ctx.inputs.object()?).map_err(weft::node_error)?;
        ctx.register_signal(Socket { common }).await
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let request = wire::opening_request(&ctx)?;
        let ws = ctx.ws_caller().await?;
        // The opening request first: everything downstream that is not
        // per-message can start. No `method` port: an upgrade is a GET.
        let fixed = wire::request_ports(&request)
            .into_iter()
            .filter(|(port, _)| *port != "method")
            .fold(NodeOutput::new(), |out, (port, value)| out.set(port, value));
        ctx.pulse_downstream(fixed).await?;

        // One yield per message, waiting for the pull each time; the
        // stream ends (the port closes at termination) when the caller
        // is gone. A byte frame is stored as a file named by its turn.
        let mut turn: u64 = 0;
        while let Some(message) = ws.recv_next().await? {
            turn += 1;
            // No content type on a socket: a frame is bytes and nothing
            // else, so what the bytes themselves say is the only word on
            // their kind. `inbound` is a stream, so what each message is
            // held to is the element type.
            let declared = ctx
                .declared_outputs()
                .get("inbound")
                .map(|ty| ty.as_generator().unwrap_or(ty).clone());
            let value = wire::value_of(
                &ctx,
                message,
                None,
                &format!("message-{turn}"),
                declared.as_ref(),
            )
            .await?;
            // A JSON message's picture on `inbound` declared as a file
            // kind is stored on the way in, like a route's body key.
            let output = wire::inline_files(&ctx, NodeOutput::new().set("inbound", value)).await?;
            ctx.yield_downstream(output).await?;
        }
        Ok(())
    }
}
