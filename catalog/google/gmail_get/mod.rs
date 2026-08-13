//! GmailGet: fetch one message by id and decompose it: headers, the
//! best text body, and (optionally) every attachment pulled into
//! storage as file references.

use async_trait::async_trait;

use weft::{Access, ExecutionContext, Node, NodeManifest, WeftResult};

use super::gmail::read_message;

#[derive(NodeManifest)]
pub struct GmailGetNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for GmailGetNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let id: String = ctx.inputs.get("id")?;
        let include_attachments: bool = ctx.inputs.get("includeAttachments")?;

        let http = ctx.client(&account).await?;
        let read = read_message(&ctx, &http, &id, include_attachments).await?;
        ctx.pulse_downstream(read.into_output()).await
    }
}
