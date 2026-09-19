//! Stream: pipe a bus to whichever live caller this run has, one chunk
//! per message, until the bus closes. HTTP: the head goes out with the
//! first chunk (the format decides the content type), the body streams,
//! and the response ends when the bus closes. WebSocket: one socket
//! message per bus message, the socket stays open (`format`, `status`
//! and `headers` have no meaning there; a status or headers set behind
//! a Socket is refused loud, the format is ignored).
//!
//! The bus is read from the earliest message still buffered, so a
//! producer that started before this node attached loses nothing. The
//! framing is the package's pure `framing.rs`; a raw payload rides per
//! the trigger's data type through `wire::chunk_for`.

use async_trait::async_trait;
use serde_json::Value;

use weft::bus::{BusEntryKind, WirePayload};
use weft::caller::{CallerHandle, HttpCaller, OutboundChunk, ResponseHead};
use weft::node::NodeOutput;
use weft::{ExecutionContext, Node, NodeManifest, WeftResult};

use super::framing::{ndjson_line, sse_event, Format};
use super::wire;

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

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let format: String = ctx.inputs.get("format")?;
        let format = Format::parse(&format).map_err(weft::node_error)?;
        let status: u16 = ctx.inputs.get("status")?;
        let headers: Option<Value> = ctx.inputs.opt("headers")?;
        let caller = ctx.live_caller().await?;
        let data_type = ctx.caller_data_type().unwrap_or_default();
        // An observer, not a participant: it reads what is retained from
        // the start (a producer that already finished, an LLM whose
        // reply came back fast, is still fully readable) and never
        // registers (a closed bus refuses a registration, and closing it
        // is the producer's act).
        let bus = ctx.bus_from_input("bus")?;
        let mut messages = bus.cursor_from_start();
        // The header every streaming API has, sent ahead of the feed
        // and framed exactly like it. It rides the same path as a bus
        // message so the caller cannot tell the two apart, which is the
        // point: `{"id":7}` then the deltas is one stream, not two.
        let first: Option<Value> = ctx.inputs.opt("first")?;

        match caller {
            CallerHandle::Http(http) => {
                let mut head = Some(wire::head_for(status, headers.as_ref())?);
                if let Some(ct) = format.content_type() {
                    head = head.map(|h| if h.has_content_type() { h } else { h.with_header("content-type", ct) });
                }
                // The framing's filler rides the head: the worker writes
                // it while the bus is quiet, and a caller who left is
                // found by that write instead of by the next message.
                if let Some(filler) = format.keepalive() {
                    head = head.map(|h| h.with_keepalive(filler));
                }
                if let Some(value) = &first {
                    let chunk = framed(&ctx, format, data_type, WirePayload::Json(value.clone().into())).await?;
                    write_chunk(&http, &mut head, chunk).await?;
                }
                while let Some(entry) = messages.next().await {
                    let BusEntryKind::Message { payload: Some(payload), .. } = entry.kind else { continue };
                    let chunk = framed(&ctx, format, data_type, payload).await?;
                    write_chunk(&http, &mut head, chunk).await?;
                }
                // The bus closed: end the response. A stream that never
                // carried a message still answers, with the head alone.
                match head.take() {
                    Some(h) => http.close_with(h).await?,
                    None => http.close().await?,
                }
            }
            CallerHandle::Websocket(ws) => {
                wire::refuse_head_on_socket("Stream", status, headers.as_ref())?;
                if let Some(value) = &first {
                    let chunk = framed(&ctx, Format::Raw, data_type, WirePayload::Json(value.clone().into())).await?;
                    ws.send(chunk).await?;
                }
                while let Some(entry) = messages.next().await {
                    let BusEntryKind::Message { payload: Some(payload), .. } = entry.kind else { continue };
                    // A socket message is a frame: no HTTP framing.
                    let chunk = framed(&ctx, Format::Raw, data_type, payload).await?;
                    ws.send(chunk).await?;
                }
            }
        }
        ctx.pulse_downstream(NodeOutput::new().set("done", true)).await
    }
}

/// Write one chunk, the held head riding the first one.
async fn write_chunk(
    http: &HttpCaller,
    head: &mut Option<ResponseHead>,
    chunk: OutboundChunk,
) -> WeftResult<()> {
    match head.take() {
        Some(h) => http.write_with(h, chunk).await,
        None => http.write(chunk).await,
    }
}

/// One bus payload as the chunk to send: framed text for `sse` /
/// `ndjson`, the payload itself per the data type for `raw` (a byte
/// payload rides as bytes whatever the format, it has no text form).
async fn framed(
    ctx: &ExecutionContext,
    format: Format,
    data_type: weft::signal::DataType,
    payload: WirePayload,
) -> WeftResult<OutboundChunk> {
    match payload {
        WirePayload::Bytes(b) => Ok(OutboundChunk::Bytes(b.to_vec())),
        WirePayload::Json(v) => match format {
            // A stored file inside a framed message goes out as a link,
            // the same as inside a raw JSON answer.
            Format::Sse => Ok(OutboundChunk::Text(sse_event(&wire::link_files(ctx, v).await?))),
            Format::Ndjson => Ok(OutboundChunk::Text(ndjson_line(&wire::link_files(ctx, v).await?))),
            Format::Raw => wire::chunk_for(ctx, data_type, v).await,
        },
    }
}
