//! GitHubCreateIssue: one authenticated POST. The access hands back a
//! client with the auth already applied (`ctx.client`); the body never
//! sees a token and never names a service.

use async_trait::async_trait;
use serde_json::Value;

use weft::node::NodeOutput;
use weft::{Access, ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct GitHubCreateIssueNode;

#[async_trait]
impl Node for GitHubCreateIssueNode {
    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let access: Access = ctx.inputs.get("account")?;
        let repo: String = ctx.inputs.get("repo")?;
        let title: String = ctx.inputs.get("title")?;
        let body: String = ctx.inputs.get_or("body", String::new())?;

        let gh = ctx.client(&access).await?;
        let resp = gh
            .post(format!("https://api.github.com/repos/{repo}/issues"))
            .header("Accept", "application/vnd.github+json")
            .json(&serde_json::json!({ "title": title, "body": body }))
            .send()
            .await
            .node_err("github: create issue")?;
        let status = resp.status();
        let answer: Value = resp.json().await.node_err("github: read create-issue response")?;
        if !status.is_success() {
            weft::node_bail!(
                "github answered {status} creating the issue: {}",
                answer.get("message").and_then(Value::as_str).unwrap_or("no detail")
            );
        }
        let url = answer
            .get("html_url")
            .and_then(Value::as_str)
            .node_err("github: created issue carries no html_url")?
            .to_string();
        let number = answer
            .get("number")
            .and_then(Value::as_i64)
            .node_err("github: created issue carries no number")?;
        ctx.pulse_downstream(NodeOutput::new().set("url", url).set("number", number)).await
    }
}
