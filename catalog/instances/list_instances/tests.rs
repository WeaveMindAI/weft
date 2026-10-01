//! ListInstances self-tests: every instance comes out as the runtime
//! holds it, the ids follow, and an instance with fires waiting on a
//! value is named in `waiting`.

use serde_json::json;

use weft::program::ProgramCall;
use weft::{FakeRig, NodeTest, WeftResult};

use super::ListInstancesNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("lists_every_instance", lists_every_instance),
        NodeTest::fake("no_instance_lists_empty", no_instance),
    ]
}

async fn lists_every_instance(rig: FakeRig) -> WeftResult<()> {
    let instances = json!([
        {
            "instance": "ada", "values": 2, "connections": 1, "tokens": 1,
            "copies": [{ "node": "bot.bridge", "instance": "ada", "status": "running" }],
            "triggers": [{ "trigger": "bot.inbox", "mode": "active" }]
        },
        {
            "instance": "bob", "values": 0, "connections": 0, "tokens": 0,
            "copies": [],
            "triggers": [{
                "trigger": "bot.inbox", "mode": "active",
                "waiting": { "fires": 3, "reason": "instance 'bob' at 'bot.answer': 'key' is not filled" }
            }]
        }
    ]);
    rig.answer_program_call("weft.instances.list", instances.clone());
    let outcome = rig.run(&ListInstancesNode, json!({})).await.ok()?;
    assert_eq!(outcome.outputs["instances"], instances);
    assert_eq!(outcome.outputs["ids"], json!(["ada", "bob"]));
    assert_eq!(outcome.outputs["waiting"], json!(["bob"]));
    assert!(matches!(rig.program_calls()[0].0, ProgramCall::InstancesList));
    Ok(())
}

async fn no_instance(rig: FakeRig) -> WeftResult<()> {
    rig.answer_program_call("weft.instances.list", json!([]));
    let outcome = rig.run(&ListInstancesNode, json!({})).await.ok()?;
    assert_eq!(outcome.outputs["instances"], json!([]));
    assert_eq!(outcome.outputs["ids"], json!([]));
    assert_eq!(outcome.outputs["waiting"], json!([]));
    Ok(())
}
