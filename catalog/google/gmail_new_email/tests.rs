//! GmailNewEmail self-tests: the registered seen-set poll and a
//! fire's full read.

use base64::Engine as _;
use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::GmailNewEmailNode;

fn b64(text: &str) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(text.as_bytes())
}

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("setup_registers_the_message_poll", setup_registers),
        NodeTest::fake("a_fire_reads_the_new_message_in_full", fire_reads),
    ]
}

async fn setup_registers(rig: FakeRig) -> WeftResult<()> {
    rig.run_setup_trigger(
        &GmailNewEmailNode,
        json!({ "account": rig.access("google"), "query": "is:unread" }),
    )
    .await
    .ok()?;
    let registered = rig.registered_signals();
    assert_eq!(registered.len(), 1);
    let spec = serde_json::to_value(&registered[0].0).expect("spec serializes").to_string();
    assert!(spec.contains("/messages?maxResults=25&q=is%3Aunread"), "query rides the url: {spec}");
    assert!(spec.contains("\"set\""), "seen-set delta mode: {spec}");
    Ok(())
}

async fn fire_reads(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "GET",
        "/gmail/v1/users/me/messages/m7?format=full",
        json!({
            "threadId": "t7",
            "snippet": "...",
            "labelIds": ["INBOX", "UNREAD"],
            "payload": {
                "mimeType": "text/plain",
                "headers": [{ "name": "Subject", "value": "fresh mail" }],
                "body": { "data": b64("the new message") },
            },
        }),
    );
    rig.wake(json!({ "item": { "id": "m7" } }));
    let outcome = rig
        .run(&GmailNewEmailNode, json!({ "account": rig.access("google") }))
        .await
        .ok()?;
    assert_eq!(outcome.outputs["id"], json!("m7"));
    assert_eq!(outcome.outputs["subject"], json!("fresh mail"));
    assert_eq!(outcome.outputs["body"], json!("the new message"));
    Ok(())
}
