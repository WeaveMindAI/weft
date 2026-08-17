//! BaileyPresence self-tests: the action payload and the soft-error
//! path.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::BaileyPresenceNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("posts_the_presence", posts),
        NodeTest::fake("a_soft_bridge_error_fails_loud", soft_error),
    ]
}

async fn posts(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", "/action", json!({ "result": { "success": true } }));
    let outcome = rig
        .run(
            &BaileyPresenceNode,
            json!({
                "endpointUrl": "http://bridge.example:8090",
                "chatId": "49151@s.whatsapp.net",
                "presence": "composing",
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["done"], json!(true));
    let body = rig.requests()[0].body.clone().expect("action body");
    assert_eq!(body["action"], json!("sendPresenceUpdate"));
    assert_eq!(body["payload"]["presence"], json!("composing"));
    Ok(())
}

async fn soft_error(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "POST",
        "/action",
        json!({ "result": { "error": "WhatsApp not connected" } }),
    );
    let outcome = rig
        .run(
            &BaileyPresenceNode,
            json!({ "endpointUrl": "http://b:1", "chatId": "c", "presence": "composing" }),
        )
        .await;
    let err = outcome.result.expect_err("a soft error must refuse").to_string();
    assert!(err.contains("WhatsApp not connected"), "{err}");
    Ok(())
}
