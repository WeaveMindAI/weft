//! BaileyManageGroup self-tests: every action's wire form (pure), one
//! full create round through the fake bridge, and the soft-error path.
//! What each action requires is the node's rules, which the compiler
//! and the run-time recheck hold, not this rig.

use serde_json::{json, Value};

use weft::{EndpointMethod, FakeRig, NodeTest, ValueBag, WeftResult};

use super::{bridge_call, BaileyManageGroupNode};

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::basic("every_action_maps_to_its_bridge_call", maps),
        NodeTest::fake("a_create_round_trip_emits_the_minted_id", creates),
        NodeTest::fake("an_edit_passes_the_group_id_through", edits),
        NodeTest::fake("a_soft_bridge_error_fails_loud", soft_error),
        NodeTest::fake("a_bridge_refusal_comes_out_on_error_when_it_is_wired", refused_wired),
    ]
}

async fn refused_wired(rig: FakeRig) -> WeftResult<()> {
    let bridge = rig.declare_infra("bridge", "api", "http://bridge.example:8090");
    rig.answer_infra("bridge", "api", EndpointMethod::Post, "/action", json!({ "result": { "error": "WhatsApp not connected" } }));
    rig.wire_output("_error");
    let outcome = rig
        .run(
            &BaileyManageGroupNode,
            json!({
                "bridge": bridge,
                "action": "add",
                "groupId": "123@g.us",
                "participants": ["49@s.whatsapp.net"],
            }),
        )
        .await
        .ok()?;
    let error = outcome.output("_error")?.as_str().expect("error is a string").to_string();
    assert!(error.contains("WhatsApp not connected"), "{error}");
    assert!(!outcome.outputs.contains_key("groupId"), "a caught failure emits nothing on groupId");
    Ok(())
}

/// The inputs of one firing, as the node reads them.
fn inputs(values: Value) -> ValueBag {
    ValueBag::inputs(values.as_object().expect("an object").clone(), Default::default(), Vec::new())
}

fn maps() -> WeftResult<()> {
    let ps = vec!["49@s.whatsapp.net".to_string()];
    let call = |a: &str| {
        bridge_call(&inputs(json!({ "action": a, "groupId": "g@g.us", "name": "Team", "description": "desc", "participants": ps })))
            .unwrap()
    };
    assert_eq!(call("create").0, "createGroup");
    assert_eq!(call("add").0, "groupAdd");
    assert_eq!(call("kick").0, "groupKick");
    assert_eq!(call("promote").0, "groupPromote");
    assert_eq!(call("demote").0, "groupDemote");
    let (a, p) = call("rename");
    assert_eq!((a, p["subject"].as_str()), ("groupUpdateSubject", Some("Team")));
    let (a, p) = call("describe");
    assert_eq!((a, p["description"].as_str()), ("groupUpdateDescription", Some("desc")));
    assert_eq!(call("add").1["participants"], json!(ps));
    // describe with no description clears it.
    let (_, p) = bridge_call(&inputs(json!({ "action": "describe", "groupId": "g" })))?;
    assert_eq!(p["description"], json!(""));
    Ok(())
}

async fn creates(rig: FakeRig) -> WeftResult<()> {
    let bridge = rig.declare_infra("bridge", "api", "http://bridge.example:8090");
    rig.answer_infra("bridge", "api", EndpointMethod::Post, "/action", json!({ "result": { "groupId": "123@g.us" } }));
    let outcome = rig
        .run(
            &BaileyManageGroupNode,
            json!({
                "bridge": bridge,
                "action": "create",
                "name": "Team",
                "participants": ["49151@s.whatsapp.net"],
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["groupId"], json!("123@g.us"));
    let body = rig.endpoint_calls()[0].body.clone().expect("action body");
    assert_eq!(body["action"], json!("createGroup"));
    assert_eq!(body["payload"]["name"], json!("Team"));
    Ok(())
}

async fn edits(rig: FakeRig) -> WeftResult<()> {
    let bridge = rig.declare_infra("bridge", "api", "http://bridge.example:8090");
    rig.answer_infra("bridge", "api", EndpointMethod::Post, "/action", json!({ "result": { "success": true } }));
    let outcome = rig
        .run(
            &BaileyManageGroupNode,
            json!({
                "bridge": bridge,
                "action": "rename",
                "groupId": "123@g.us",
                "name": "New Name",
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["groupId"], json!("123@g.us"));
    let body = rig.endpoint_calls()[0].body.clone().expect("action body");
    assert_eq!(body["action"], json!("groupUpdateSubject"));
    assert_eq!(body["payload"]["subject"], json!("New Name"));
    Ok(())
}

async fn soft_error(rig: FakeRig) -> WeftResult<()> {
    let bridge = rig.declare_infra("bridge", "api", "http://bridge.example:8090");
    rig.answer_infra("bridge", "api", EndpointMethod::Post, "/action", json!({ "result": { "error": "WhatsApp not connected" } }));
    let outcome = rig
        .run(
            &BaileyManageGroupNode,
            json!({
                "bridge": bridge,
                "action": "add",
                "groupId": "123@g.us",
                "participants": ["49@s.whatsapp.net"],
            }),
        )
        .await;
    let err = outcome.result.expect_err("a soft error must refuse").to_string();
    assert!(err.contains("WhatsApp not connected"), "{err}");
    Ok(())
}
