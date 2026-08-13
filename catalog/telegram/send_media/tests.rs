//! TelegramSendMedia self-tests: the multipart framing per media kind
//! and the unknown-kind refusal.

use serde_json::json;

use weft::{fixture_spec, FakeRig, LiveRig, NodeTest, WeftResult};

use super::TelegramSendMediaNode;

/// A tiny (98-byte) VALID 32x32 PNG, enough for Telegram to accept as
/// a real photo.
const TINY_PNG: &[u8] = include_bytes!("fixture.png");

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("frames_the_photo_as_multipart", sends_photo),
        NodeTest::fake("an_unknown_kind_refuses", unknown_kind),
        NodeTest::live("one_real_photo_send", "telegram", live_send).with_fixture(fixture_spec(
            "TELEGRAM_CHAT_ID",
            "Chat id",
            "The chat the test sends into. A bot cannot start a chat, so message \
             the bot once and use that chat's id.",
        )),
    ]
}

/// Send the tiny photo to the fixture chat (a bot cannot start a
/// chat). The delivered photo IS the proof, so it stays.
async fn live_send(rig: LiveRig) -> WeftResult<()> {
    let chat_id = rig.fixture("TELEGRAM_CHAT_ID")?;
    let file = rig.store_file("fixture.png", "image/png", TINY_PNG.to_vec()).await?;
    let outcome = rig
        .run(
            &TelegramSendMediaNode,
            json!({
                "account": rig.access("telegram"),
                "chatId": chat_id,
                "file": file,
                "kind": "photo",
                "caption": "weft node test: photo",
            }),
        )
        .await
        .ok()?;
    assert!(outcome.output("messageId")?.is_i64(), "a real message id came back");
    Ok(())
}

async fn sends_photo(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", "/sendPhoto", json!({ "ok": true, "result": { "message_id": 7 } }));
    let file = rig.store_file("cat.png", "image/png", b"PNGBYTES".to_vec());
    let outcome = rig
        .run(
            &TelegramSendMediaNode,
            json!({
                "account": rig.access("telegram"),
                "chatId": "12345",
                "file": file,
                "kind": "photo",
                "caption": "a cat",
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["messageId"], json!(7));
    let sent = &rig.requests()[0];
    let body = sent.body_text.as_ref().expect("multipart body is buffered");
    assert!(body.contains("name=\"chat_id\"\r\n\r\n12345"), "{body}");
    assert!(body.contains("name=\"caption\"\r\n\r\na cat"), "{body}");
    assert!(body.contains("name=\"photo\"; filename=\"cat.png\""), "{body}");
    assert!(body.contains("PNGBYTES"), "the stored bytes ride the media part");
    Ok(())
}

async fn unknown_kind(rig: FakeRig) -> WeftResult<()> {
    let file = rig.store_file("cat.png", "image/png", b"x".to_vec());
    let outcome = rig
        .run(
            &TelegramSendMediaNode,
            json!({
                "account": rig.access("telegram"),
                "chatId": "12345",
                "file": file,
                "kind": "hologram",
            }),
        )
        .await;
    let err = outcome.result.expect_err("unknown kind must refuse").to_string();
    assert!(err.contains("unknown media kind"), "{err}");
    Ok(())
}
