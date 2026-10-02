//! TelegramSendMessage self-tests: the send body, buttons, and the
//! live send (needs the chat id fixture: a bot cannot start a chat).

use serde_json::json;

use weft::{fixture_spec, FakeRig, LiveRig, NodeTest, WeftResult};

use super::TelegramSendMessageNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("sends_with_buttons_and_reply_threading", sends),
        NodeTest::fake("a_refused_send_fails_the_run_when_error_is_unwired", refused_unwired),
        NodeTest::fake("a_refused_send_comes_out_on_error_when_it_is_wired", refused_wired),
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

/// Telegram refusing the send, the failure both `error` tests read.
fn refuse_the_send(rig: &FakeRig) {
    rig.respond(
        "POST",
        "/sendMessage",
        json!({ "ok": false, "description": "Forbidden: bot was blocked by the user" }),
    );
}

fn send_inputs(rig: &FakeRig) -> serde_json::Value {
    json!({ "account": rig.access("telegram"), "chatId": "12345", "text": "hello" })
}

async fn refused_unwired(rig: FakeRig) -> WeftResult<()> {
    refuse_the_send(&rig);
    let err = rig.run(&TelegramSendMessageNode, send_inputs(&rig)).await.failure()?;
    assert!(err.contains("bot was blocked"), "{err}");
    Ok(())
}

async fn refused_wired(rig: FakeRig) -> WeftResult<()> {
    refuse_the_send(&rig);
    rig.wire_output("error");
    let outcome = rig.run(&TelegramSendMessageNode, send_inputs(&rig)).await.ok()?;
    let error = outcome.output("error")?.as_str().expect("error is a string").to_string();
    assert!(error.contains("bot was blocked"), "{error}");
    assert!(!outcome.outputs.contains_key("messageId"), "a caught failure emits no messageId");
    Ok(())
}
