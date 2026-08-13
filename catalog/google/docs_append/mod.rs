//! GoogleDocsAppend: append text at the end of a Google Doc.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::{Access, ExecutionContext, Node, NodeManifest, WeftResult};

use super::docs::append_text;

#[derive(NodeManifest)]
pub struct GoogleDocsAppendNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for GoogleDocsAppendNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let doc_id: String = ctx.inputs.get("documentId")?;
        let text: String = ctx.inputs.get("text")?;

        let http = ctx.client(&account).await?;
        append_text(&http, &doc_id, &text).await?;
        ctx.pulse_downstream(NodeOutput::new().set("done", true)).await
    }
}
