//! Stream: pipe a bus to whichever live caller this run has, one chunk
//! per message, until the bus closes, through weft's
//! `HttpCaller::stream_bus` / `WsCaller::stream_bus`. HTTP: the head goes
//! out with the first chunk (the format decides the content type), the
//! body streams, and the response ends when the bus closes. WebSocket:
//! one socket message per bus message, the socket stays open (`format`,
//! `status` and `headers` have no meaning there; a status or headers set
//! behind a Socket is refused loud, the format is ignored).
//!
//! A bus that ended failed (its producer broke off) is never ended as a
//! whole answer: the exchange fails instead, and `done` stays absent.

use async_trait::async_trait;
use serde_json::Value;

use weft::caller::{CallerHandle, Format, ResponseHead};
use weft::node::NodeOutput;
use weft::signal::DataType;
use weft::{RunContext, Node, NodeManifest, WeftResult};

use super::api;

#[derive(NodeManifest)]
pub struct StreamNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for StreamNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let format: Format = ctx.inputs.get("format")?;
        let status: u16 = ctx.inputs.get("status")?;
        let headers = api::headers(&ctx).await?;
        let caller = ctx.live_caller().await?;
        let data_type = match ctx.inputs.opt::<DataType>("answerAs")? {
            Some(data_type) => data_type,
            None => ctx.caller_data_type().unwrap_or_default(),
        };
        let bus = ctx.bus_from_input("bus")?;
        // The header every streaming API has, sent ahead of the feed
        // and framed exactly like it: `{"id":7}` then the deltas is one
        // stream, not two.
        let first = ctx.inputs.opt::<Value>("first")?.map(|value| ("first", value));
        let whole = match caller {
            CallerHandle::Http(http) => {
                let head = ResponseHead::new(status).with_headers(headers);
                http.stream_bus(&ctx, &bus, head, format, data_type, first).await?
            }
            CallerHandle::Websocket(ws) => {
                api::refuse_head_on_socket("Stream", status, &headers)?;
                ws.stream_bus(&ctx, &bus, data_type, first).await?
            }
        };
        // A bus that broke off failed the exchange, and the caller was
        // told so instead of being handed `done`.
        if !whole {
            return Ok(());
        }
        ctx.pulse_downstream(NodeOutput::new().set("done", true)).await
    }
}
