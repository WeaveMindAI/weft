//! BaileyManageGroup: one node for the bridge's group operations
//! (create / add / kick / promote / demote / rename / describe). The
//! selected action decides which inputs are required and which bridge
//! action rides the wire; a missing requirement refuses loudly before
//! anything is sent (the node's rules). Emits the group's id either way (`create` mints
//! it, everything else passes the input through, so a chain of group
//! edits wires naturally). Pure Fire-phase.

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, Value};

use weft::infra::InfraHandle;
use weft::node::NodeOutput;
use weft::{RunContext, Node, NodeErrExt, NodeManifest, ValueBag, WeftResult};

#[derive(NodeManifest)]
pub struct BaileyManageGroupNode;

#[cfg(feature = "node-tests")]
mod tests;

/// The group actions, as the `action` select spells them.
#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum Action {
    Create,
    Add,
    Kick,
    Promote,
    Demote,
    Rename,
    Describe,
}

/// The wire form of the selected action: the bridge action name and its
/// payload, read off the node's inputs. The node's rules refuse an
/// action without the inputs it reads (a group, a name, at least one
/// participant), written or wired, so each read here finds its value.
fn bridge_call(inputs: &ValueBag) -> WeftResult<(&'static str, Value)> {
    let group = || inputs.get::<String>("groupId");
    let participants = || inputs.get::<Vec<String>>("participants");
    Ok(match inputs.get::<Action>("action")? {
        Action::Create => ("createGroup", json!({ "name": inputs.get::<String>("name")?, "participants": participants()? })),
        Action::Add => ("groupAdd", json!({ "groupId": group()?, "participants": participants()? })),
        Action::Kick => ("groupKick", json!({ "groupId": group()?, "participants": participants()? })),
        Action::Promote => ("groupPromote", json!({ "groupId": group()?, "participants": participants()? })),
        Action::Demote => ("groupDemote", json!({ "groupId": group()?, "participants": participants()? })),
        Action::Rename => ("groupUpdateSubject", json!({ "groupId": group()?, "subject": inputs.get::<String>("name")? })),
        // No description clears it.
        Action::Describe => (
            "groupUpdateDescription",
            json!({ "groupId": group()?, "description": inputs.opt::<String>("description")?.unwrap_or_default() }),
        ),
    })
}

#[async_trait]
impl Node for BaileyManageGroupNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let bridge: InfraHandle = ctx.inputs.get("bridge")?;
        let group_id: Option<String> = ctx.inputs.opt("groupId")?;
        let (bridge_action, payload) = bridge_call(&ctx.inputs)?;
        let result = ctx.endpoint_of(&bridge).await?.action(bridge_action, payload).await?;
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
