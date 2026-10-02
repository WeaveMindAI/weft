//! BaileySend: POSTs a `sendMessage` action to the project's
//! WhatsApp bridge. Pure Fire-phase; no registration or infra
//! lifecycle of its own.

use async_trait::async_trait;

use weft::infra::InfraHandle;
use weft::node::NodeOutput;
use weft::{ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct BaileySendNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for BaileySendNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let bridge: InfraHandle = ctx.inputs.get("bridge")?;
        let to: String = ctx.inputs.get("to")?;
        let message: String = ctx.inputs.get("message")?;

        let result = ctx.endpoint_of(&bridge)
            .await?
            .action("sendMessage", serde_json::json!({ "to": to, "text": message }))
            .await?;
        let message_id = result["messageId"]
            .as_str()
            .node_err(format!("bridge send response missing result.messageId: {result}"))?;
        ctx.pulse_downstream(NodeOutput::new().set("messageId", message_id)).await
    }
}
