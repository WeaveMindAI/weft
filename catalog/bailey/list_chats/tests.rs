//! BaileyListChats self-tests: the list emission and the soft-error
//! path.

use serde_json::json;

use weft::{EndpointMethod, FakeRig, NodeTest, WeftResult};

use super::BaileyListChatsNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("lists_the_chats", lists),
        NodeTest::fake("a_soft_bridge_error_fails_loud", soft_error),
    ]
}

async fn lists(rig: FakeRig) -> WeftResult<()> {
    let bridge = rig.declare_infra("bridge", "api", "http://bridge.example:8090");
    rig.answer_infra("bridge", "api", EndpointMethod::Post, "/action", json!({ "result": { "chats": [
            { "id": "123@g.us", "name": "Team", "participantCount": 4 },
            { "id": "456@g.us", "name": "Family", "participantCount": 7 },
        ] } }));
    let outcome = rig
        .run(&BaileyListChatsNode, json!({ "bridge": bridge }))
        .await
        .ok()?;
    assert_eq!(outcome.outputs["count"], json!(2.0));
    assert_eq!(outcome.outputs["chats"][1]["name"], json!("Family"));
    let body = rig.endpoint_calls()[0].body.clone().expect("action body");
    assert_eq!(body["action"], json!("getChats"));
    Ok(())
}

async fn soft_error(rig: FakeRig) -> WeftResult<()> {
    let bridge = rig.declare_infra("bridge", "api", "http://bridge.example:8090");
    rig.answer_infra("bridge", "api", EndpointMethod::Post, "/action", json!({ "result": { "error": "WhatsApp not connected" } }));
    let outcome =
        rig.run(&BaileyListChatsNode, json!({ "bridge": bridge })).await;
    let err = outcome.result.expect_err("a soft error must refuse").to_string();
    assert!(err.contains("WhatsApp not connected"), "{err}");
    Ok(())
}
