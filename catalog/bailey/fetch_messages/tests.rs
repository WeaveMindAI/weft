//! BaileyFetchMessages self-tests: the history emission and the
//! soft-error path.

use serde_json::json;

use weft::{EndpointMethod, FakeRig, NodeTest, WeftResult};

use super::BaileyFetchMessagesNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("fetches_the_history", fetches),
        NodeTest::fake("a_soft_bridge_error_fails_loud", soft_error),
    ]
}

async fn fetches(rig: FakeRig) -> WeftResult<()> {
    let bridge = rig.declare_infra("bridge", "api", "http://bridge.example:8090");
    rig.answer_infra("bridge", "api", EndpointMethod::Post, "/action", json!({ "result": { "messages": [
            { "from": "49151@s.whatsapp.net", "pushName": "Ada", "content": "hi",
              "messageType": "text", "messageId": "wa-1", "timestamp": 1755, "fromMe": false },
            { "from": "49151@s.whatsapp.net", "pushName": null, "content": "yo",
              "messageType": "text", "messageId": "wa-2", "timestamp": 1756, "fromMe": true },
        ] } }));
    let outcome = rig
        .run(
            &BaileyFetchMessagesNode,
            json!({
                "bridge": bridge,
                "chatId": "49151@s.whatsapp.net",
                "count": 2,
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["count"], json!(2.0));
    assert_eq!(outcome.outputs["messages"][0]["content"], json!("hi"));
    let body = rig.endpoint_calls()[0].body.clone().expect("action body");
    assert_eq!(body["action"], json!("fetchMessages"));
    assert_eq!(body["payload"]["count"], json!(2.0));
    Ok(())
}

async fn soft_error(rig: FakeRig) -> WeftResult<()> {
    let bridge = rig.declare_infra("bridge", "api", "http://bridge.example:8090");
    rig.answer_infra("bridge", "api", EndpointMethod::Post, "/action", json!({ "result": { "error": "chatId is required" } }));
    let outcome = rig
        .run(
            &BaileyFetchMessagesNode,
            json!({ "bridge": bridge, "chatId": "c", "count": 5 }),
        )
        .await;
    let err = outcome.result.expect_err("a soft error must refuse").to_string();
    assert!(err.contains("chatId is required"), "{err}");
    Ok(())
}
