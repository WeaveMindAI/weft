//! TelegramReceiveMessage self-tests: the registered getUpdates poll
//! (cursor semantics included) and a fire's decomposition.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::TelegramReceiveMessageNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("setup_registers_the_update_poll_with_its_cursor", setup_registers),
        NodeTest::fake("a_fire_decomposes_the_update", fire_decomposes),
    ]
}

async fn setup_registers(rig: FakeRig) -> WeftResult<()> {
    rig.run_setup_trigger(
        &TelegramReceiveMessageNode,
        json!({ "account": rig.access("telegram"), "chatId": "12345" }),
    )
    .await
    .ok()?;
    let registered = rig.registered_signals();
    assert_eq!(registered.len(), 1, "one poll signal");
    let spec = &registered[0].0;
    assert_eq!(spec.kind, "poll_endpoint");
    assert!(
        spec.config["url"].as_str().expect("poll url").contains("/getUpdates"),
        "{}", spec.config
    );
    let delta = &spec.config["delta"];
    assert_eq!(delta["items"], json!("result"));
    assert_eq!(delta["cursor_field"], json!("update_id"), "the cursor walks update ids");
    // Telegram wants last_update_id + 1 as the offset; the priming
    // poll drains the backlog with -1.
    assert_eq!(delta["cursor_param"]["name"], json!("offset"));
    assert_eq!(delta["cursor_param"]["offset"], json!(1));
    assert_eq!(delta["cursor_param"]["prime"], json!(-1));
    let filters = serde_json::to_value(&spec.match_predicates).expect("filters serialize");
    assert_eq!(filters[0]["field"], json!("item.message"), "only message updates fire");
    assert_eq!(filters[0]["op"], json!("exists"));
    assert_eq!(filters[1]["field"], json!("item.message.chat.id"), "the chat filter narrows");
    assert_eq!(filters[1]["value"], json!("12345"));
    Ok(())
}

async fn fire_decomposes(rig: FakeRig) -> WeftResult<()> {
    rig.wake(json!({ "item": {
        "update_id": 9,
        "message": {
            "message_id": 42,
            "text": "hi bot",
            "chat": { "id": 12345 },
            "from": { "username": "ada" },
        },
    }}));
    let outcome = rig
        .run(&TelegramReceiveMessageNode, json!({ "account": rig.access("telegram") }))
        .await
        .ok()?;
    assert_eq!(outcome.outputs["text"], json!("hi bot"));
    assert_eq!(outcome.outputs["chatId"], json!("12345"));
    assert_eq!(outcome.outputs["user"], json!("ada"));
    assert_eq!(outcome.outputs["messageId"], json!(42));
    Ok(())
}
