//! TelegramEditMessage self-tests: the one edit call.

use serde_json::json;

use weft::{fixture_spec, FakeRig, LiveRig, NodeTest, WeftResult};

use crate::telegram_send_message::TelegramSendMessageNode;

use super::TelegramEditMessageNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("edits_by_chat_and_message_id", edits),
        NodeTest::live("one_real_send_then_edit", "telegram", live_edit).with_fixture(
            fixture_spec(
                "TELEGRAM_CHAT_ID",
                "Chat id",
                "The chat the test sends into. A bot cannot start a chat, so message \
                 the bot once and use that chat's id.",
            ),
        ),
    ]
}

/// Send into the fixture chat, then rewrite the message. The package
/// has no delete node, and the edited message IS the proof, so it
/// stays.
async fn live_edit(rig: LiveRig) -> WeftResult<()> {
    let chat_id = rig.fixture("TELEGRAM_CHAT_ID")?;
    let sent = rig
        .run(
            &TelegramSendMessageNode,
            json!({
                "account": rig.access("telegram"),
                "chatId": chat_id,
                "text": "weft node test: before the edit",
            }),
        )
        .await
        .ok()?;
    let message_id = sent.output("messageId")?.as_i64().expect("message id");
    let edited = rig
        .run(
            &TelegramEditMessageNode,
            json!({
                "account": rig.access("telegram"),
                "chatId": chat_id,
                "messageId": message_id,
                "text": "weft node test: after the edit",
            }),
        )
        .await
        .ok()?;
    assert_eq!(edited.output("done")?, &json!(true));
    Ok(())
}

async fn edits(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", "/editMessageText", json!({ "ok": true, "result": {} }));
    let outcome = rig
        .run(
            &TelegramEditMessageNode,
            json!({
                "account": rig.access("telegram"),
                "chatId": "12345",
                "messageId": 42,
                "text": "edited",
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["done"], json!(true));
    let body = rig.requests()[0].body.clone().expect("edit body");
    assert_eq!(body, json!({ "chat_id": "12345", "message_id": 42, "text": "edited" }));
    Ok(())
}
