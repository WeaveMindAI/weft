//! ListMembers self-tests: every member comes out as the runtime holds
//! them, the ids follow, and a member with fires waiting on a value is
//! named in `waiting`.

use serde_json::json;

use weft::program::ProgramCall;
use weft::{FakeRig, NodeTest, WeftResult};

use super::ListMembersNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("lists_every_member", lists_every_member),
        NodeTest::fake("no_member_lists_empty", no_member),
    ]
}

async fn lists_every_member(rig: FakeRig) -> WeftResult<()> {
    let members = json!([
        {
            "member": "ada", "values": 2, "connections": 1, "tokens": 1,
            "copies": [{ "node": "bot.bridge", "member": "ada", "status": "running" }],
            "triggers": [{ "trigger": "bot.inbox", "mode": "active" }]
        },
        {
            "member": "bob", "values": 0, "connections": 0, "tokens": 0,
            "copies": [],
            "triggers": [{
                "trigger": "bot.inbox", "mode": "active",
                "waiting": { "fires": 3, "reason": "member 'bob' at 'bot.answer': 'key' is not filled" }
            }]
        }
    ]);
    rig.answer_program_call("weft.members.list", members.clone());
    let outcome = rig.run(&ListMembersNode, json!({})).await.ok()?;
    assert_eq!(outcome.outputs["members"], members);
    assert_eq!(outcome.outputs["ids"], json!(["ada", "bob"]));
    assert_eq!(outcome.outputs["waiting"], json!(["bob"]));
    assert!(matches!(rig.program_calls()[0].0, ProgramCall::MembersList));
    Ok(())
}

async fn no_member(rig: FakeRig) -> WeftResult<()> {
    rig.answer_program_call("weft.members.list", json!([]));
    let outcome = rig.run(&ListMembersNode, json!({})).await.ok()?;
    assert_eq!(outcome.outputs["members"], json!([]));
    assert_eq!(outcome.outputs["ids"], json!([]));
    assert_eq!(outcome.outputs["waiting"], json!([]));
    Ok(())
}
