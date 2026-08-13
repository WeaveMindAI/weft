//! GitHubCreateIssue: one authenticated POST. The access hands back a
//! client with the auth already applied (`ctx.client`); the body never
//! sees a token and never names a service.

use async_trait::async_trait;
use serde_json::Value;

use weft::node::NodeOutput;
use weft::{Access, ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct GitHubCreateIssueNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for GitHubCreateIssueNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let access: Access = ctx.inputs.get("account")?;
        let repo: String = ctx.inputs.get("repo")?;
        let title: String = ctx.inputs.get("title")?;
        let body: String = ctx.inputs.get_or("body", String::new())?;

        let gh = ctx.client(&access).await?;
        // GitHub's error envelope is a top-level `message`, which
        // `json_call` quotes; the Accept header pins the API version.
        let answer = weft::access::client::json_call(
            gh.post(format!("https://api.github.com/repos/{repo}/issues"))
                .header("Accept", "application/vnd.github+json")
                .json(&serde_json::json!({ "title": title, "body": body })),
            "github: create the issue",
        )
        .await?;
        let url = weft::access::client::required_str(&answer, "create the issue", "html_url")?
            .to_string();
        let number = answer
            .get("number")
            .and_then(Value::as_i64)
            .node_err("github: created issue carries no number")?;
        ctx.pulse_downstream(NodeOutput::new().set("url", url).set("number", number)).await
    }
}
