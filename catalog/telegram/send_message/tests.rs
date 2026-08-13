//! TelegramSendMessage self-tests: the send body, buttons, and the
//! live send (needs the chat id fixture: a bot cannot start a chat).

use serde_json::json;

use weft::{fixture_spec, FakeRig, LiveRig, NodeTest, WeftResult};

use super::TelegramSendMessageNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("sends_with_buttons_and_reply_threading", sends),
        NodeTest::fake("a_button_without_a_url_refuses", bad_button),
        NodeTest::live("one_real_send", "telegram", live_send).with_fixture(fixture_spec(
            "TELEGRAM_CHAT_ID",
            "Chat id",
            "The chat the test sends into. A bot cannot start a chat, so message \
             the bot once and use that chat's id.",
        )),
    ]
}

async fn sends(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "POST",
        "/sendMessage",
        json!({ "ok": true, "result": { "message_id": 42 } }),
    );
    let outcome = rig
        .run(
            &TelegramSendMessageNode,
            json!({
                "account": rig.access("telegram"),
                "chatId": "12345",
                "text": "hello",
                "replyTo": 7,
                "buttons": [{ "label": "Docs", "url": "https://example.com" }],
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["messageId"], json!(42));
    let body = rig.requests()[0].body.clone().expect("send body");
    assert_eq!(body["reply_parameters"]["message_id"], json!(7));
    assert_eq!(
        body["reply_markup"]["inline_keyboard"][0][0]["text"],
        json!("Docs")
    );
    Ok(())
}

async fn bad_button(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig
        .run(
            &TelegramSendMessageNode,
            json!({
                "account": rig.access("telegram"),
                "chatId": "12345",
                "text": "hello",
                "buttons": [{ "label": "no url" }],
            }),
        )
        .await;
    let err = outcome.result.expect_err("a url-less button must refuse").to_string();
    assert!(err.contains("label and a url"), "{err}");
    Ok(())
}

async fn live_send(rig: LiveRig) -> WeftResult<()> {
    // A bot cannot open a chat with you; the target chat is a fixture.
    let chat_id = rig.fixture("TELEGRAM_CHAT_ID")?;
    let outcome = rig
        .run(
            &TelegramSendMessageNode,
            json!({
                "account": rig.access("telegram"),
                "chatId": chat_id,
                "text": "weft node test: hello",
            }),
        )
        .await
        .ok()?;
    assert!(outcome.output("messageId")?.is_i64(), "a real message id came back");
    Ok(())
}
