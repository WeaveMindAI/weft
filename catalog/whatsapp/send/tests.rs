//! WhatsAppSend self-tests: the bridge action call and the soft-error
//! surfacing.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::WhatsAppSendNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("posts_the_action_and_emits_the_message_id", sends),
        NodeTest::fake("a_soft_bridge_error_fails_loud", soft_error),
    ]
}

async fn sends(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "POST",
        "/action",
        json!({ "result": { "messageId": "wa-1" } }),
    );
    let outcome = rig
        .run(
            &WhatsAppSendNode,
            json!({
                "endpointUrl": "http://bridge.example:8090",
                "to": "4915112345678",
                "message": "hello",
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["messageId"], json!("wa-1"));
    let body = rig.requests()[0].body.clone().expect("action body");
    assert_eq!(body["action"], json!("sendMessage"));
    assert_eq!(body["payload"]["to"], json!("4915112345678"));
    Ok(())
}

async fn soft_error(rig: FakeRig) -> WeftResult<()> {
    // Soft failures ride a 200 with result.error.
    rig.respond(
        "POST",
        "/action",
        json!({ "result": { "error": "WhatsApp not connected" } }),
    );
    let outcome = rig
        .run(
            &WhatsAppSendNode,
            json!({ "endpointUrl": "http://bridge.example:8090", "to": "49151", "message": "x" }),
        )
        .await;
    let err = outcome.result.expect_err("a soft error must refuse").to_string();
    assert!(err.contains("WhatsApp not connected"), "{err}");
    Ok(())
}
