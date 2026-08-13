//! GmailGet self-tests: one full read decomposed into headers + body.

use base64::Engine as _;
use serde_json::json;

use weft::{fixture_spec, FakeRig, LiveRig, NodeTest, WeftResult};

use super::super::gmail_send::GmailSendNode;
use super::GmailGetNode;

fn b64(text: &str) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(text.as_bytes())
}

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("decomposes_headers_and_the_text_body", decomposes),
        NodeTest::live("one_real_read_of_a_sent_message", "google", live_get).with_fixture(
            fixture_spec(
                "GMAIL_TO",
                "Recipient address",
                "The address the test sends to; the connected account's own address \
                 keeps the send self-contained.",
            ),
        ),
    ]
}

async fn decomposes(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "GET",
        "/gmail/v1/users/me/messages/m1?format=full",
        json!({
            "threadId": "t1",
            "snippet": "hi there...",
            "labelIds": ["INBOX"],
            "payload": {
                "mimeType": "text/plain",
                "headers": [
                    { "name": "Subject", "value": "hello" },
                    { "name": "From", "value": "ada@example.com" },
                    { "name": "To", "value": "me@example.com" },
                    { "name": "Date", "value": "Sun, 9 Aug 2026 10:00:00 +0000" },
                ],
                "body": { "data": b64("hi there, this is the body") },
            },
        }),
    );
    let outcome = rig
        .run(&GmailGetNode, json!({ "account": rig.access("google"), "id": "m1" }))
        .await
        .ok()?;
    assert_eq!(outcome.outputs["subject"], json!("hello"));
    assert_eq!(outcome.outputs["from"], json!("ada@example.com"));
    assert_eq!(outcome.outputs["body"], json!("hi there, this is the body"));
    assert_eq!(outcome.outputs["bodyIsHtml"], json!(false));
    assert_eq!(outcome.outputs["threadId"], json!("t1"));
    assert_eq!(outcome.outputs["labels"], json!(["INBOX"]));
    Ok(())
}

async fn live_get(rig: LiveRig) -> WeftResult<()> {
    // A message id cannot be guessed, so the test self-provisions one:
    // it sends a mail to the fixture address and reads that message
    // back. The sent mail stays (the artifact IS the proof).
    let to = rig.fixture("GMAIL_TO")?;
    let marker = format!("weft-node-tests {}", uuid::Uuid::new_v4());
    let sent = rig
        .run(
            &GmailSendNode,
            json!({
                "account": rig.access("google"),
                "to": to,
                "subject": marker,
                "text": "sent by the GmailGet live self-test",
            }),
        )
        .await
        .ok()?;
    let id = sent.output("id")?.as_str().expect("message id").to_string();

    let outcome = rig
        .run(&GmailGetNode, json!({ "account": rig.access("google"), "id": id }))
        .await
        .ok()?;
    assert_eq!(
        outcome.output("subject")?.as_str().expect("subject"),
        marker,
        "the read answers the message just sent"
    );
    assert!(
        outcome.output("body")?.as_str().expect("body").contains("live self-test"),
        "the body round-tripped"
    );
    assert!(
        !outcome.output("threadId")?.as_str().expect("thread id").is_empty(),
        "the message carries its thread"
    );
    Ok(())
}
