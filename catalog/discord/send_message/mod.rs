//! DiscordSendMessage: post a message to the connected channel
//! webhook. The connection's base URL IS the webhook, so the node
//! addresses the bare base with `wait=true` (Discord then answers the
//! created message instead of a 204).

use async_trait::async_trait;
use serde_json::{json, Value};

use weft::access::client::json_call;
use weft::node::NodeOutput;
use weft::{Access, ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct DiscordSendMessageNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for DiscordSendMessageNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let content: Option<String> = ctx.inputs.opt("content")?;
        let username: Option<String> = ctx.inputs.opt("username")?;
        let embeds = ctx.inputs.raw("embeds").cloned();

        let mut body = json!({});
        if let Some(c) = content.as_deref().filter(|c| !c.trim().is_empty()) {
            body["content"] = json!(c);
        }
        if let Some(u) = username.as_deref().filter(|u| !u.trim().is_empty()) {
            body["username"] = json!(u);
        }
        if let Some(e) = &embeds {
            let Some(list) = e.as_array() else {
                weft::node_bail!("embeds must be a list of Discord embed objects");
            };
            if !list.is_empty() {
                body["embeds"] = json!(list);
            }
        }
        if body.as_object().expect("object").is_empty() {
            weft::node_bail!("nothing to send: set content or embeds");
        }

        // The connection's base URL step aims this at the stored
        // webhook; the node only writes the query.
        let http = ctx.client(&account).await?;
        let message: Value = json_call(
            http.post("https://webhook.discord.invalid/?wait=true").json(&body),
            "discord: post the message",
        )
        .await?;
        let id = message["id"].as_str().node_err("discord: the message answered no id")?;
        ctx.pulse_downstream(
            NodeOutput::new().set("messageId", id).set("message", message.clone()),
        )
        .await
    }
}
