//! GmailSend self-tests: the assembled raw message and the recipient
//! rule. The MIME builder itself (CRLF sanitizing, multipart shapes)
//! is tested where it lives, in the package's `gmail.rs`.

use base64::Engine as _;
use serde_json::json;

use weft::{fixture_spec, FakeRig, LiveRig, NodeTest, WeftResult};

use super::GmailSendNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("sends_the_assembled_raw_message", sends),
        NodeTest::fake("attachments_and_reply_threading_ride_the_raw_message", attachments_and_reply),
        NodeTest::fake("no_recipient_refuses_before_any_call", no_recipient),
        NodeTest::live("one_real_send", "google", live_send).with_fixture(fixture_spec(
            "GMAIL_TO",
            "Recipient address",
            "The address the test sends to; the connected account's own address \
             keeps the send self-contained.",
        )),
    ]
}

async fn attachments_and_reply(rig: FakeRig) -> WeftResult<()> {
    // The replied-to message: its Message-ID feeds In-Reply-To /
    // References, its threadId pins the thread.
    rig.respond(
        "GET",
        "/gmail/v1/users/me/messages/orig-1?format=metadata&metadataHeaders=Message-ID",
        json!({
            "threadId": "t-orig",
            "payload": { "headers": [{ "name": "Message-ID", "value": "<abc@mail>" }] }
        }),
    );
    rig.respond(
        "POST",
        "/gmail/v1/users/me/messages/send",
        json!({ "id": "m2", "threadId": "t-orig" }),
    );
    let file = rig.store_file("report.pdf", "application/pdf", b"PDFDATA".to_vec());
    rig.run(
        &GmailSendNode,
        json!({
            "account": rig.access("google"),
            "to": "ada@example.com",
            "cc": "bob@example.com",
            "subject": "re: hello",
            "text": "see attached",
            "attachments": file,
            "replyTo": "orig-1",
        }),
    )
    .await
    .ok()?;

    let sent = rig.requests();
    assert_eq!(sent.len(), 2, "read the original, then send");
    let body = sent[1].body.clone().expect("send body");
    assert_eq!(body["threadId"], json!("t-orig"), "the reply stays in the thread");
    let mime = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(body["raw"].as_str().expect("raw message"))
        .expect("raw is base64url");
    let mime = String::from_utf8_lossy(&mime);
    assert!(mime.contains("In-Reply-To: <abc@mail>"), "reply header missing:\n{mime}");
    assert!(mime.contains("Cc: bob@example.com"), "cc missing:\n{mime}");
    assert!(
        mime.contains("Content-Type: application/pdf") && mime.contains("report.pdf"),
        "attachment part missing:\n{mime}"
    );
    // The attachment's bytes ride base64-encoded inside the multipart.
    let b64 = base64::engine::general_purpose::STANDARD.encode(b"PDFDATA");
    assert!(mime.contains(&b64), "attachment bytes missing:\n{mime}");
    Ok(())
}

async fn sends(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "POST",
        "/gmail/v1/users/me/messages/send",
        json!({ "id": "m1", "threadId": "t1" }),
    );
    let outcome = rig
        .run(
            &GmailSendNode,
            json!({
                "account": rig.access("google"),
                "to": "ada@example.com",
                "subject": "hello",
                "text": "body text",
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["id"], json!("m1"));
    assert_eq!(outcome.outputs["threadId"], json!("t1"));

    let body = rig.requests()[0].body.clone().expect("send body");
    let raw = body["raw"].as_str().expect("raw message");
    let mime = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(raw)
        .expect("raw is base64url");
    let mime = String::from_utf8_lossy(&mime);
    assert!(mime.contains("To: ada@example.com"), "{mime}");
    assert!(mime.contains("Subject: hello"), "{mime}");
    assert!(mime.contains("body text"), "{mime}");
    Ok(())
}

async fn no_recipient(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig
        .run(
            &GmailSendNode,
            json!({ "account": rig.access("google"), "subject": "s", "text": "t" }),
        )
        .await;
    let err = outcome.result.expect_err("no recipient must refuse").to_string();
    assert!(err.contains("no recipient"), "{err}");
    assert!(rig.requests().is_empty(), "nothing was sent");
    Ok(())
}

async fn live_send(rig: LiveRig) -> WeftResult<()> {
    // A mailbox cannot be self-provisioned: the recipient is a
    // fixture. The sent mail stays (the artifact IS the proof).
    let to = rig.fixture("GMAIL_TO")?;
    let outcome = rig
        .run(
            &GmailSendNode,
            json!({
                "account": rig.access("google"),
                "to": to,
                "subject": "weft-node-tests",
                "text": "sent by the GmailSend live self-test",
            }),
        )
        .await
        .ok()?;
    assert!(
        !outcome.output("id")?.as_str().expect("message id").is_empty(),
        "the send minted a message id"
    );
    assert!(
        !outcome.output("threadId")?.as_str().expect("thread id").is_empty(),
        "the send landed in a thread"
    );
    Ok(())
}
