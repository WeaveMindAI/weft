//! BaileyMarkRead self-tests: the action payload and the soft-error path.

use serde_json::json;

use weft::{EndpointMethod, FakeRig, NodeTest, WeftResult};

use super::BaileyMarkReadNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("posts_the_read_receipt", marks_read),
        NodeTest::fake("a_soft_bridge_error_fails_loud", soft_error),
    ]
}

async fn marks_read(rig: FakeRig) -> WeftResult<()> {
    let bridge = rig.declare_infra("bridge", "api", "http://bridge.example:8090");
    rig.answer_infra("bridge", "api", EndpointMethod::Post, "/action", json!({ "result": { "success": true } }));
    let outcome = rig
        .run(
            &BaileyMarkReadNode,
            json!({
                "bridge": bridge,
                "chatId": "49151@s.whatsapp.net",
                "messageId": "wa-7",
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["done"], json!(true));
    let body = rig.endpoint_calls()[0].body.clone().expect("action body");
    assert_eq!(body["action"], json!("readMessages"));
    assert_eq!(body["payload"]["chatId"], json!("49151@s.whatsapp.net"));
    assert_eq!(body["payload"]["messageId"], json!("wa-7"));
    Ok(())
}

async fn soft_error(rig: FakeRig) -> WeftResult<()> {
    let bridge = rig.declare_infra("bridge", "api", "http://bridge.example:8090");
    rig.answer_infra("bridge", "api", EndpointMethod::Post, "/action", json!({ "result": { "error": "WhatsApp not connected" } }));
    let outcome = rig
        .run(
            &BaileyMarkReadNode,
            json!({ "bridge": bridge, "chatId": "c", "messageId": "m" }),
        )
        .await;
    let err = outcome.result.expect_err("a soft error must refuse").to_string();
    assert!(err.contains("WhatsApp not connected"), "{err}");
    Ok(())
}
