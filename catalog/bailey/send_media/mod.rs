//! BaileySendMedia: POSTs a `sendMedia` action to the project's
//! WhatsApp bridge. The stored file travels as its public link when
//! the install serves one, else inline as a data: URL; the bridge
//! reads both. Pure Fire-phase; no infra lifecycle of its own.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::storage::FileHandle;
use weft::{ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct BaileySendMediaNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for BaileySendMediaNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let endpoint_url: String = ctx.inputs.get("endpointUrl")?;
        let to: String = ctx.inputs.get("to")?;
        let file: FileHandle = ctx.inputs.get("file")?;
        let caption: Option<String> = ctx.inputs.opt("caption")?;

        let media = super::bridge_api::bridge_media(&ctx, &file).await?;
        let mut payload = serde_json::json!({
            "to": to,
            "mediaUrl": media.url,
            "mimetype": media.mime_type,
            "filename": media.filename,
        });
        if let Some(caption) = caption.filter(|c| !c.is_empty()) {
            payload["caption"] = serde_json::json!(caption);
        }

        let result =
            super::bridge_api::action(&ctx, &endpoint_url, "sendMedia", payload).await?;
        let message_id = result["messageId"]
            .as_str()
            .node_err(format!("bridge send response missing result.messageId: {result}"))?;
        ctx.pulse_downstream(NodeOutput::new().set("messageId", message_id)).await
    }
}
