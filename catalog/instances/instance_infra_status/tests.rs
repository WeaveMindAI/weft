//! InstanceInfraStatus self-tests: a copy's state comes out as it is, and
//! no copy reads `none`.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::InstanceInfraStatusNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("a_running_copy_says_so", running),
        NodeTest::fake("no_copy_reads_none", none),
    ]
}

async fn running(rig: FakeRig) -> WeftResult<()> {
    rig.answer_program_call("weft.infra.status", json!({ "node": "bridge", "instance": "ada", "status": "running" }));
    let outcome = rig.run(&InstanceInfraStatusNode, json!({ "node": "bridge", "instance": "ada" })).await.ok()?;
    assert_eq!(outcome.outputs["status"], json!("running"));
    assert_eq!(outcome.outputs["running"], json!(true));
    Ok(())
}

async fn none(rig: FakeRig) -> WeftResult<()> {
    rig.answer_program_call("weft.infra.status", json!(null));
    let outcome = rig.run(&InstanceInfraStatusNode, json!({ "node": "bridge", "instance": "ada" })).await.ok()?;
    assert_eq!(outcome.outputs["status"], json!("none"));
    assert_eq!(outcome.outputs["running"], json!(false));
    Ok(())
}
