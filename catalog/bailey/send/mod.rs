//! BaileySend: POSTs a `sendMessage` action to the project's
//! WhatsApp bridge. Pure Fire-phase; no registration or infra
//! lifecycle of its own.

use async_trait::async_trait;

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
        let endpoint_url: String = ctx.inputs.get("endpointUrl")?;
        let to: String = ctx.inputs.get("to")?;
        let message: String = ctx.inputs.get("message")?;

        let result = super::bridge_api::action(
            &ctx,
            &endpoint_url,
            "sendMessage",
            serde_json::json!({ "to": to, "text": message }),
        )
        .await?;
        let message_id = result["messageId"]
            .as_str()
            .node_err(format!("bridge send response missing result.messageId: {result}"))?;
        // Only emit `messageId`. The previous `success: true` port was
        // an always-true constant (every failure path errors above), so
        // its mere presence on the wire was the meaningful signal. The
        // `messageId` emission already conveys "send succeeded"; if a
        // user wires a downstream `success` consumer they wire it
        // against `messageId` instead.
        ctx.pulse_downstream(NodeOutput::new().set("messageId", message_id)).await
    }
}
