//! SlackFindChannel: resolve a channel name to its id (paging
//! `conversations.list` until found). The name-to-id bridge for
//! workflows configured with human-readable channel names.

use async_trait::async_trait;
use serde_json::Value;

use weft::node::NodeOutput;
use weft::{Access, ExecutionContext, Node, NodeManifest, WeftResult};

use super::api;

#[derive(NodeManifest)]
pub struct SlackFindChannelNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for SlackFindChannelNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let access: Access = ctx.inputs.get("account")?;
        let name: String = ctx.inputs.get("name")?;
        let wanted = name.trim_start_matches('#').to_string();

        // Slack has no name-lookup endpoint, so resolving a name means
        // scanning the listing (bounded by the shared paging cap; past
        // it the node fails loudly naming the recovery: paste the id).
        let query = [
            ("exclude_archived", "true".to_string()),
            ("types", "public_channel,private_channel".to_string()),
        ];
        let hit = api::paged(
            &ctx,
            &access,
            "conversations.list",
            &query,
            "channels",
            |channels| {
                Ok(channels
                    .iter()
                    .find(|c| c["name"].as_str() == Some(wanted.as_str()))
                    .cloned())
            },
            |scanned| {
                format!(
                    "scanned {scanned} channels without finding '{wanted}'; paste the \
                     channel id directly instead of the name"
                )
            },
        )
        .await?;
        let Some(hit) = hit else {
            weft::node_bail!(
                "no channel named '{wanted}' is visible to this app (private channels \
                 only appear once the app is invited into them)"
            );
        };
        let id = api::required_str(&hit, "conversations.list", "id")?.to_string();
        let is_private = hit["is_private"].as_bool().unwrap_or(false);
        ctx.pulse_downstream(
            NodeOutput::new().set("id", id).set("name", wanted).set("private", is_private),
        )
        .await
    }
}
