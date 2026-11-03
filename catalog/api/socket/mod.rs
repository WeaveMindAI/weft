//! Socket: trigger node that turns a weft program into a WebSocket
//! endpoint. An outside caller opens a socket at the project's live URL;
//! the dispatcher matches the path, checks the caller against the auth,
//! and routes the held socket to a worker; this trigger fires a fresh
//! run and stays alive for the socket's life.
//!
//!   - `setup_trigger`: build a `Socket` signal from the node's fields
//!     and register it. The dispatcher mounts the public route.
//!   - `run`: the caller is attached for this run. The opening request
//!     (`ctx.wake`) fans onto the fixed ports; then every message the
//!     caller sends is yielded on `inbound` (a `Generator`, so a Loop
//!     over it runs once per message, in lock-step) until the caller
//!     disconnects, which ends the stream. Each message is held to the
//!     item type the program declared (`-> (inbound:
//!     Generator[Move])`); one that does not fit is refused back to the
//!     caller by name and the session goes on. Nothing else here
//!     answers the caller; Reply and Close do, or a custom node through
//!     `ctx.ws_caller()`.

use async_trait::async_trait;

use weft::caller::OutboundChunk;
use weft::node::NodeOutput;
use weft::signal::{DataType, LiveConnectionConfig, Socket};
use weft::{RunContext, Node, NodeManifest, WeftResult};

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

    async fn setup_trigger(&self, ctx: RunContext) -> WeftResult<()> {
        let fields = ctx.inputs.object()?;
        let common = LiveConnectionConfig::from_node_fields(fields).map_err(weft::node_error)?;
        ctx.register_signal(Socket { common }).await
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let request = wire::opening_request(&ctx)?;
        let ws = ctx.ws_caller().await?;
        // The opening request first: everything downstream that is not
        // per-message can start. Socket declares no `method` port (an
        // upgrade is a GET), so `request_ports` leaves it out.
        let fixed = wire::request_ports(self.manifest(), &request)
            .into_iter()
            .fold(NodeOutput::new(), |out, (port, value)| out.set(port, value));
        ctx.pulse_downstream(fixed).await?;

        // One yield per message, waiting for the pull each time; the
        // stream ends (the port closes at termination) when the caller
        // is gone. Each message is held to the item type the program
        // declared on `inbound` (`Generator[JsonDict]` when it declared
        // none); a byte frame is stored as a file named by its turn, and
        // no content type comes with it, so what the bytes themselves say
        // is the only word on their kind. A message that does not fit is
        // the caller's mistake: the caller is told what is wrong and the
        // session goes on, the way a route answers such a body 400.
        let declared = ctx.output_type("inbound")?;
        let item = declared.as_generator().cloned().unwrap_or(declared);
        let data_type = ctx.caller_data_type().unwrap_or_default();
        // A socket's session cannot be picked up from the middle: the node
        // holds its run for the socket's whole life.
        let mut holding = ctx.hold();
        let mut turn: u64 = 0;
        while let Some(message) = ws.recv_next().await? {
            turn += 1;
            match wire::take_message(&ctx, message, None, &format!("message-{turn}"), &item).await? {
                Ok(value) => ctx.yield_downstream(NodeOutput::new().set("inbound", value), &mut holding).await?,
                Err(why) => {
                    let refusal = match data_type {
                        DataType::Json => OutboundChunk::Json(serde_json::json!({ "error": why })),
                        DataType::Text | DataType::Bytes => OutboundChunk::Text(why),
                    };
                    ws.send(refusal).await?;
                }
            }
        }
        Ok(())
    }
}
