//! Reply: answer whichever live caller this run has with one message.
//! HTTP: the final body under the given status and headers; terminal,
//! the first terminal wins. WebSocket: one message, the socket stays
//! open. That is the one difference between the two wires, and the
//! node says so: a status or headers set behind a Socket is a program
//! bug, refused loud rather than silently dropped.
//!
//! The body travels per `answerAs`, else the trigger's declared data
//! type, through the package's `wire::chunk_for`, so this node never
//! re-decides how bytes or text ride the wire.

use async_trait::async_trait;
use serde_json::Value;

use weft::caller::CallerHandle;
use weft::node::NodeOutput;
use weft::signal::DataType;
use weft::storage::StoredFile;
use weft::{ExecutionContext, Node, NodeManifest, WeftResult};

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

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let body: Value = ctx.inputs.get("body")?;
        let status: u16 = ctx.inputs.get("status")?;
        let headers: Option<Value> = ctx.inputs.opt("headers")?;
        let caller = ctx.live_caller().await?;
        let data_type = wire::answer_type(&ctx)?;
        // Read before the body is spent: a file sent as bytes names its
        // own type and filename on the response.
        let file = (data_type == DataType::Bytes).then(|| StoredFile::from_value(&body).ok()).flatten();
        let chunk = wire::chunk_for(&ctx, data_type, body).await?;
        match caller {
            CallerHandle::Http(http) => {
                let mut head = wire::head_for(status, headers.as_ref())?;
                if let Some(file) = &file {
                    head = wire::with_file_head(head, file);
                }
                http.respond_with(head, chunk).await?;
            }
            CallerHandle::Websocket(ws) => {
                wire::refuse_head_on_socket("Reply", status, headers.as_ref())?;
                ws.send(chunk).await?;
            }
        }
        ctx.pulse_downstream(NodeOutput::new().set("done", true)).await
    }
}
