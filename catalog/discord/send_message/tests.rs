//! DiscordSendMessage self-tests.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::DiscordSendMessageNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("posts_and_emits_the_message", posts),
        NodeTest::fake("nothing_to_send_refuses", empty),
    ]
}

async fn posts(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", "/?wait=true", json!({ "id": "msg-1", "content": "hi" }));
    let outcome = rig
        .run(
            &DiscordSendMessageNode,
            json!({
                "account": rig.access("discord_webhook"),
                "content": "hi",
                "username": "Weft bot",
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["messageId"], json!("msg-1"));
    let body = rig.requests()[0].body.clone().expect("json payload");
    assert_eq!(body["content"], json!("hi"));
    assert_eq!(body["username"], json!("Weft bot"));
    assert!(body.get("embeds").is_none());
    Ok(())
}

async fn empty(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig
        .run(
            &DiscordSendMessageNode,
            json!({ "account": rig.access("discord_webhook") }),
        )
        .await;
    let err = outcome.result.expect_err("an empty message must refuse").to_string();
    assert!(err.contains("nothing to send"), "{err}");
    Ok(())
}
