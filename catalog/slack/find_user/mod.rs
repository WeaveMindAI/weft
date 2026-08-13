//! SlackFindUser: resolve a person to their Slack identity. By email
//! (the CRM/ticket bridge case) or by user id (enrich an event's bare
//! `U...` into name/profile). Exactly one lookup key.

use async_trait::async_trait;
use serde_json::{json, Value};

use weft::node::NodeOutput;
use weft::{Access, ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

use super::api;

#[derive(NodeManifest)]
pub struct SlackFindUserNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for SlackFindUserNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let access: Access = ctx.inputs.get("account")?;
        let email: Option<String> = ctx.inputs.opt("email")?;
        let id: Option<String> = ctx.inputs.opt("id")?;

        let user: Value = match (email, id) {
            (Some(e), None) => {
                let answer =
                    api::get(&ctx, &access, "users.lookupByEmail", &[("email", e)]).await?;
                answer["user"].clone()
            }
            (None, Some(u)) => {
                let answer = api::get(&ctx, &access, "users.info", &[("user", u)]).await?;
                answer["user"].clone()
            }
            (Some(_), Some(_)) => {
                weft::node_bail!("pick ONE lookup key: an email or a user id, not both")
            }
            (None, None) => weft::node_bail!("pick a lookup key: an email or a user id"),
        };
        if user.is_null() {
            weft::node_bail!("slack returned no user object for the lookup");
        }

        let id = user["id"]
            .as_str()
            .node_err("slack: the user object carries no id")?
            .to_string();
        let name = user
            .pointer("/profile/display_name")
            .and_then(Value::as_str)
            .filter(|n| !n.is_empty())
            .or_else(|| user.pointer("/profile/real_name").and_then(Value::as_str))
            .unwrap_or_default()
            .to_string();
        let email = user
            .pointer("/profile/email")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let tz = user["tz"].as_str().unwrap_or_default().to_string();

        ctx.pulse_downstream(
            NodeOutput::new()
                .set("id", id)
                .set("name", name)
                .set("email", email)
                .set("timezone", tz)
                .set("user", json!(user)),
        )
        .await
    }
}
