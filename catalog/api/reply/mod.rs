//! Reply: answer whichever live caller this run has with one message.
//! HTTP: the final body under the given status and headers; terminal,
//! the first terminal wins. WebSocket: one message, the socket stays
//! open. That is the one difference between the two wires, and the
//! node says so: a status or headers set behind a Socket is a program
//! bug, refused loud rather than silently dropped.
//!
//! The body travels per `answerAs`, else the trigger's declared data
//! type, through weft's `OutboundChunk::of_input`, so this node never
//! re-decides how bytes or text ride the wire. `answerAs: redirect`
//! answers a file (a stored one or a url file) as a redirect to its
//! external link instead (HTTP only): the caller's browser fetches the
//! file from there, with `Range` and all.

use async_trait::async_trait;
use serde_json::Value;

use weft::caller::{CallerHandle, OutboundChunk, ResponseHead};
use weft::node::NodeOutput;
use weft::signal::DataType;
use weft::storage::{FileHandle, StorageScope, StoredFile};
use weft::weft_type::FileKind;
use weft::{RunContext, Node, NodeManifest, WeftResult};

use super::api;

#[derive(NodeManifest)]
pub struct ReplyNode;

/// The `answerAs` select: a shape of the wire, or a redirect to a file's
/// link. Unset follows the trigger's `dataType`.
#[derive(Debug, Clone, Copy, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
enum AnswerAs {
    Json,
    Text,
    Bytes,
    Redirect,
}

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
        let headers = api::headers(&ctx).await?;
        let caller = ctx.live_caller().await?;
        let data_type = match ctx.inputs.opt::<AnswerAs>("answerAs")? {
            None => ctx.caller_data_type().unwrap_or_default(),
            Some(AnswerAs::Json) => DataType::Json,
            Some(AnswerAs::Text) => DataType::Text,
            Some(AnswerAs::Bytes) => DataType::Bytes,
            Some(AnswerAs::Redirect) => {
                let CallerHandle::Http(http) = caller else {
                    weft::node_bail!("Reply behind a Socket sends messages; a redirect is an HTTP answer. Put the Reply behind a Route");
                };
                // A file value is a marker object; `FileHandle::from_value`
                // would read a bare string as a storage key.
                if body.as_object().and_then(FileKind::from_marker_obj).is_none() {
                    weft::node_bail!("a redirect answers a file (a stored one or a url file), and `body` is not one");
                }
                let file = FileHandle::from_value(&body).map_err(|e| weft::node_error(format!("a redirect answers a file; {e}")))?;
                let link = ctx.storage(StorageScope::Run).external_link(&file).await?;
                let head = ResponseHead::redirect(status, link.url)?.with_headers(headers);
                http.respond_with(head, OutboundChunk::Bytes(Vec::new())).await?;
                return ctx.pulse_downstream(NodeOutput::new().set("done", true)).await;
            }
        };
        // Read before the body is spent: a file sent as bytes names its
        // own type and filename on the response.
        let file = (data_type == DataType::Bytes).then(|| StoredFile::from_value(&body).ok()).flatten();
        let made = OutboundChunk::of_input(&ctx, "body", body, data_type).await;
        match caller {
            CallerHandle::Http(http) => {
                // The whole answer: one that cannot be made is this
                // node's failure, whatever the route skips in a stream.
                let chunk = made?;
                let mut head = ResponseHead::new(status).with_headers(headers);
                if let Some(file) = &file {
                    head = head.with_file(file);
                }
                http.respond_with(head, chunk).await?;
            }
            CallerHandle::Websocket(ws) => {
                api::refuse_head_on_socket("Reply", status, &headers)?;
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
