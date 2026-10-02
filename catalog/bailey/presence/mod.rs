//! BaileyPresence: POSTs a `sendPresenceUpdate` action to the
//! project's WhatsApp bridge (typing dots, recording, online,
//! offline). Pure Fire-phase.

use async_trait::async_trait;

use weft::infra::InfraHandle;
use weft::node::NodeOutput;
use weft::{ExecutionContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct BaileyPresenceNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for BaileyPresenceNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let bridge: InfraHandle = ctx.inputs.get("bridge")?;
        let chat_id: String = ctx.inputs.get("chatId")?;
        let presence: String = ctx.inputs.get("presence")?;

        ctx.endpoint_of(&bridge)
            .await?
            .action("sendPresenceUpdate", serde_json::json!({ "chatId": chat_id, "presence": presence }))
            .await?;
        ctx.pulse_downstream(NodeOutput::new().set("done", true)).await
    }
}
