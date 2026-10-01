//! CurrentInstance self-tests: an instance's run fires its id alone, and
//! a run for no instance fires `nobody` alone.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::CurrentInstanceNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("an_instances_run_names_it", an_instances_run),
        NodeTest::fake("a_run_for_nobody_says_so", a_run_for_nobody),
    ]
}

async fn an_instances_run(rig: FakeRig) -> WeftResult<()> {
    rig.instance("user-42");
    let outcome = rig.run(&CurrentInstanceNode, json!({})).await.ok()?;
    assert_eq!(outcome.outputs["instance"], json!("user-42"));
    assert!(outcome.outputs.get("nobody").is_none(), "{:?}", outcome.outputs);
    Ok(())
}

async fn a_run_for_nobody(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig.run(&CurrentInstanceNode, json!({})).await.ok()?;
    assert_eq!(outcome.outputs["nobody"], json!(true));
    assert!(outcome.outputs.get("instance").is_none(), "{:?}", outcome.outputs);
    Ok(())
}
