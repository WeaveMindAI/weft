//! Reply: answer whichever live caller this run has with one message.
//! HTTP: the final body under the given status and headers; terminal,
//! the first terminal wins. WebSocket: one message, the socket stays
//! open. That is the one difference between the two wires, and the
//! node says so: a status or headers set behind a Socket is a program
//! bug, refused loud rather than silently dropped.
//!
//! The body travels per `answerAs`, else the trigger's declared data
//! type, through the package's `wire::chunk_for`, so this node never
//! re-decides how bytes or text ride the wire. `answerAs: "redirect"`
//! answers a file (a stored one or a url file) as a redirect to its
//! external link instead (HTTP only): the caller's browser fetches the
//! file from there, with `Range` and all.

use async_trait::async_trait;
use serde_json::Value;

use weft::caller::CallerHandle;
use weft::node::NodeOutput;
use weft::signal::DataType;
use weft::storage::{StorageScope, StoredFile};
use weft::{RunContext, Node, NodeManifest, WeftResult};

use super::wire;

#[derive(NodeManifest)]
pub struct ReplyNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for ReplyNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let body: Value = ctx.inputs.get("body")?;
        let status: u16 = ctx.inputs.get("status")?;
        let headers = wire::headers(&ctx).await?;
        let caller = ctx.live_caller().await?;
        let data_type = match wire::answer(&ctx)? {
            wire::Answer::Data(data_type) => data_type,
            wire::Answer::Redirect => {
                let CallerHandle::Http(http) = caller else {
                    weft::node_bail!("Reply behind a Socket sends messages; a redirect is an HTTP answer. Put the Reply behind a Route");
                };
                let Some(file) = wire::file_value(&body) else {
                    weft::node_bail!("a redirect answers a file, got {}", wire::kind_of(&body));
                };
                let file = file.map_err(|e| weft::node_error(format!("a redirect answers a file; {e}")))?;
                let link = ctx.storage(StorageScope::Run).external_link(&file).await?;
                let head = wire::head_for(wire::redirect_status(status)?, headers.as_ref())?.with_header("location", link.url);
                http.respond_with(head, weft::caller::OutboundChunk::Bytes(Vec::new())).await?;
                return ctx.pulse_downstream(NodeOutput::new().set("done", true)).await;
            }
        };
        // Read before the body is spent: a file sent as bytes names its
        // own type and filename on the response.
        let file = (data_type == DataType::Bytes).then(|| StoredFile::from_value(&body).ok()).flatten();
        let made = wire::revealing(&ctx, "body", body, |body| wire::chunk_for(&ctx, data_type, body)).await;
        match caller {
            CallerHandle::Http(http) => {
                // The whole answer: one that cannot be made is this
                // node's failure, whatever the route skips in a stream.
                let chunk = made?;
                let mut head = wire::head_for(status, headers.as_ref())?;
                if let Some(file) = &file {
                    head = wire::with_file_head(head, file);
                }
                http.respond_with(head, chunk).await?;
            }
            CallerHandle::Websocket(ws) => {
                wire::refuse_head_on_socket("Reply", status, headers.as_ref())?;
                // One message of many: one that cannot be made fails the
                // session, or is skipped when the socket's
                // `whenChunkFails` says so, `done` then staying absent.
                let Some(chunk) = ws.drop_or_fail(made)? else { return Ok(()) };
                ws.send(chunk).await?;
            }
        }
        ctx.pulse_downstream(NodeOutput::new().set("done", true)).await
    }
}
