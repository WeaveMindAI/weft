//! ListMemberCopies self-tests: every copy comes out as it is, and the
//! member list leaves the shared copy out.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::ListMemberCopiesNode;

pub fn tests() -> Vec<NodeTest> {
    vec![NodeTest::fake("lists_every_copy", lists_every_copy)]
}

async fn lists_every_copy(rig: FakeRig) -> WeftResult<()> {
    let copies = json!([
        { "node": "bridge", "status": "running" },
        { "node": "bridge", "member": "ada", "status": "stopped" },
        { "node": "bridge", "member": "bob", "status": "failed", "failure": "image pull failed" }
    ]);
    rig.answer_program_call("weft.infra.copies", copies.clone());
    let outcome = rig.run(&ListMemberCopiesNode, json!({ "node": "bridge" })).await.ok()?;
    assert_eq!(outcome.outputs["copies"], copies);
    assert_eq!(outcome.outputs["members"], json!(["ada", "bob"]));
    Ok(())
}
