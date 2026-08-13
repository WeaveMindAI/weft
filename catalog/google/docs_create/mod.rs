//! GoogleDocsCreate: create a Google Doc with a title and initial
//! text (the report-generation sink: an LLM writes, the doc lands in
//! Drive).

use async_trait::async_trait;
use serde_json::json;

use weft::node::NodeOutput;
use weft::{Access, ExecutionContext, Node, NodeManifest, WeftResult};

use super::docs::append_text;

#[derive(NodeManifest)]
pub struct GoogleDocsCreateNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for GoogleDocsCreateNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let title: String = ctx.inputs.get("title")?;
        let text: Option<String> = ctx.inputs.opt("text")?;

        let http = ctx.client(&account).await?;
        let answer = weft::access::client::json_call(
            http.post("https://docs.googleapis.com/v1/documents")
                .json(&json!({ "title": title })),
            "create the document",
        )
        .await?;
        let doc_id = weft::access::client::required_str(&answer, "create the document", "documentId")?
            .to_string();

        if let Some(t) = text.filter(|t| !t.is_empty()) {
            append_text(&http, &doc_id, &t).await?;
        }

        ctx.pulse_downstream(
            NodeOutput::new()
                .set("documentId", doc_id.clone())
                .set("link", format!("https://docs.google.com/document/d/{doc_id}/edit")),
        )
        .await
    }
}
