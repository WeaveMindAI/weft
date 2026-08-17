//! BaileyManageGroup: one node for the bridge's group operations
//! (create / add / kick / promote / demote / rename / describe). The
//! selected action decides which inputs are required and which bridge
//! action rides the wire; a missing requirement refuses loudly before
//! anything is sent. Emits the group's id either way (`create` mints
//! it, everything else passes the input through, so a chain of group
//! edits wires naturally). Pure Fire-phase.

use async_trait::async_trait;
use serde_json::json;

use weft::node::NodeOutput;
use weft::{ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct BaileyManageGroupNode;

#[cfg(feature = "node-tests")]
mod tests;

/// The wire form of one selected action: the bridge action name and
/// its payload. Pure, so every action's requirements are pinned by
/// plain tests.
fn bridge_call(
    action: &str,
    group_id: Option<&str>,
    name: Option<&str>,
    description: Option<&str>,
    participants: &[String],
) -> WeftResult<(&'static str, serde_json::Value)> {
    let need_group = || group_id.map(str::to_string).node_err(format!("'{action}' needs groupId"));
    let need_participants = || {
        if participants.is_empty() {
            weft::node_bail!("'{action}' needs at least one participant");
        }
        Ok(participants)
    };
    match action {
        "create" => {
            let name = name.filter(|n| !n.is_empty()).node_err("'create' needs a group name")?;
            let participants = need_participants()?;
            Ok(("createGroup", json!({ "name": name, "participants": participants })))
        }
        "add" => Ok(("groupAdd", json!({ "groupId": need_group()?, "participants": need_participants()? }))),
        "kick" => Ok(("groupKick", json!({ "groupId": need_group()?, "participants": need_participants()? }))),
        "promote" => Ok(("groupPromote", json!({ "groupId": need_group()?, "participants": need_participants()? }))),
        "demote" => Ok(("groupDemote", json!({ "groupId": need_group()?, "participants": need_participants()? }))),
        "rename" => {
            let subject = name.filter(|n| !n.is_empty()).node_err("'rename' needs the new name")?;
            Ok(("groupUpdateSubject", json!({ "groupId": need_group()?, "subject": subject })))
        }
        "describe" => Ok((
            "groupUpdateDescription",
            json!({ "groupId": need_group()?, "description": description.unwrap_or("") }),
        )),
        other => weft::node_bail!("'{other}' is not a group action this node knows"),
    }
}

#[async_trait]
impl Node for BaileyManageGroupNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let endpoint_url: String = ctx.inputs.get("endpointUrl")?;
        let action: String = ctx.inputs.get("action")?;
        let group_id: Option<String> = ctx.inputs.opt("groupId")?;
        let name: Option<String> = ctx.inputs.opt("name")?;
        let description: Option<String> = ctx.inputs.opt("description")?;
        let participants: Vec<String> = ctx.inputs.opt("participants")?.unwrap_or_default();

        let (bridge_action, payload) = bridge_call(
            &action,
            group_id.as_deref(),
            name.as_deref(),
            description.as_deref(),
            &participants,
        )?;
        let result =
            super::bridge_api::action(&ctx, &endpoint_url, bridge_action, payload).await?;
        // `create` answers the minted id; every other action passes the
        // addressed group through.
        let out_group = result["groupId"]
            .as_str()
            .map(str::to_string)
            .or(group_id)
            .node_err(format!("bridge {bridge_action} answered no groupId: {result}"))?;
        ctx.pulse_downstream(NodeOutput::new().set("groupId", out_group)).await
    }
}
