//! CurrentMember self-tests: a member's run fires their id alone, and a
//! run for nobody fires `nobody` alone.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::CurrentMemberNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("a_members_run_names_them", a_members_run),
        NodeTest::fake("a_run_for_nobody_says_so", a_run_for_nobody),
    ]
}

async fn a_members_run(rig: FakeRig) -> WeftResult<()> {
    rig.member("user-42");
    let outcome = rig.run(&CurrentMemberNode, json!({})).await.ok()?;
    assert_eq!(outcome.outputs["member"], json!("user-42"));
    assert!(outcome.outputs.get("nobody").is_none(), "{:?}", outcome.outputs);
    Ok(())
}

async fn a_run_for_nobody(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig.run(&CurrentMemberNode, json!({})).await.ok()?;
    assert_eq!(outcome.outputs["nobody"], json!(true));
    assert!(outcome.outputs.get("member").is_none(), "{:?}", outcome.outputs);
    Ok(())
}
