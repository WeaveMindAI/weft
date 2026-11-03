//! Close: end the exchange with whichever live caller this run has, and
//! say why. WebSocket: the close frame with `code` and `reason`. HTTP:
//! the end of the response; with nothing sent yet and a `reason`, the
//! reason IS the body under `status` (sent the way Reply sends a
//! String, through weft's `OutboundChunk::of_input`), with no reason a bodiless
//! answer under `status`; after a Stream the head already went out and
//! only the stream ends (the connection says which: `wire_started`).
//!
//! `headers` go out with whichever head this node sends (the reason's
//! or the bodiless one), so an ending can carry its own header: a 304
//! with its `etag`, a 429 with its `retry-after`. After a Stream they
//! are refused, because that head already went out.
//!
//! `code` and `status` carry no metadata default: each is one wire's
//! word for how the exchange ended, and an absent one is "the author
//! said nothing", which is what lets this node default per wire (1000,
//! 204, or 200 once there is a body) and refuse the other wire's field
//! instead of dropping it.

use async_trait::async_trait;
use serde_json::Value;

use weft::caller::{CallerHandle, CloseReason, OutboundChunk, ResponseHead};
use weft::node::NodeOutput;
use weft::{RunContext, Node, NodeManifest, WeftResult};

use super::api;

#[derive(NodeManifest)]
pub struct CloseNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for CloseNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let code: Option<u16> = ctx.inputs.opt("code")?;
        let status: Option<u16> = ctx.inputs.opt("status")?;
        let reason: Option<String> = ctx.inputs.opt("reason")?;
        let headers = api::headers(&ctx).await?;
        match ctx.live_caller().await? {
            CallerHandle::Http(http) => {
                if let Some(code) = code {
                    weft::node_bail!(
                        "`code` ({code}) is a WebSocket close code and this caller is a Route; \
                         say how it ended with `status` (and `reason`) instead"
                    );
                }
                if http.wire_started() {
                    // Same reason the `reason` below is refused: the head
                    // carrying these went out with the stream's first
                    // chunk, so a header set here would silently never
                    // reach the caller.
                    if !headers.is_empty() {
                        weft::node_bail!(
                            "`headers` cannot reach this caller: a Stream already sent the \
                             response head, so only its end is left. Set them on the Stream, \
                             or drop `headers`"
                        );
                    }
                    // After a Stream the head and the body already went
                    // out, so only the end is left; a reason would have
                    // nowhere to go, and dropping it would hide the
                    // message from the caller it was written for.
                    if let Some(reason) = reason {
                        weft::node_bail!(
                            "`reason` ({reason:?}) cannot reach this caller: a Stream already \
                             sent the response, so only its end is left. Put the message in \
                             the stream, or drop `reason`"
                        );
                    }
                    http.close().await?;
                } else if let Some(reason) = reason {
                    // The reason IS the answer's body, so one node both
                    // ends the exchange and says why.
                    // A 204 carries no body: the metadata refuses a 204
                    // with a reason, wired or written.
                    let data_type = ctx.caller_data_type().unwrap_or_default();
                    let chunk = OutboundChunk::of_input(&ctx, "reason", Value::String(reason), data_type).await?;
                    http.respond_with(ResponseHead::new(status.unwrap_or(200)).with_headers(headers), chunk).await?;
                } else {
                    http.close_with(ResponseHead::new(status.unwrap_or(204)).with_headers(headers)).await?;
                }
            }
            CallerHandle::Websocket(ws) => {
                if let Some(status) = status {
                    weft::node_bail!(
                        "`status` ({status}) is an HTTP status and this caller is a Socket; a \
                         close frame carries `code` and `reason` instead"
                    );
                }
                api::refuse_head_on_socket("Close", 200, &headers)?;
                ws.close_with(CloseReason { code: code.unwrap_or(1000), reason: reason.unwrap_or_default() }).await?;
            }
        }
        ctx.pulse_downstream(NodeOutput::new().set("done", true)).await
    }
}
