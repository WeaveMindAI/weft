//! BaileyReact self-tests: the action payload and the soft-error path.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::BaileyReactNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("posts_the_reaction", reacts),
        NodeTest::fake("a_soft_bridge_error_fails_loud", soft_error),
    ]
}

async fn reacts(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", "/action", json!({ "result": { "success": true } }));
    let outcome = rig
        .run(
            &BaileyReactNode,
            json!({
                "endpointUrl": "http://bridge.example:8090",
                "chatId": "49151@s.whatsapp.net",
                "messageId": "wa-7",
                "emoji": "👍",
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["done"], json!(true));
    let body = rig.requests()[0].body.clone().expect("action body");
    assert_eq!(body["action"], json!("sendReaction"));
    assert_eq!(body["payload"]["messageId"], json!("wa-7"));
    assert_eq!(body["payload"]["emoji"], json!("👍"));
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
            &BaileyReactNode,
            json!({ "endpointUrl": "http://b:1", "chatId": "c", "messageId": "m", "emoji": "x" }),
        )
        .await;
    let err = outcome.result.expect_err("a soft error must refuse").to_string();
    assert!(err.contains("WhatsApp not connected"), "{err}");
    Ok(())
}
