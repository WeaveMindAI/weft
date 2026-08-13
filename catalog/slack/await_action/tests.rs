//! SlackAwaitAction self-tests: post -> park -> click -> retire ->
//! decision, against a canned click.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::SlackAwaitActionNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("posts_parks_and_delivers_the_decision", full_flow),
        NodeTest::fake("empty_buttons_refuse_before_posting", empty_buttons),
    ]
}

async fn full_flow(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "POST",
        "/api/chat.postMessage",
        json!({ "ok": true, "ts": "1.2", "channel": "C1" }),
    );
    rig.respond("POST", "/api/chat.update", json!({ "ok": true, "ts": "1.2" }));
    // The click that resumes the park.
    rig.signal(json!({ "action": "approve", "user": "U7", "userName": "ada" }));

    let outcome = rig
        .run(
            &SlackAwaitActionNode,
            json!({
                "account": rig.access("slack"),
                "channel": "C1",
                "text": "Ship it?",
                "buttons": [
                    { "id": "approve", "label": "Approve", "style": "primary" },
                    { "id": "reject", "label": "Reject" },
                ],
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["action"], json!("approve"));
    assert_eq!(outcome.outputs["user"], json!("U7"));
    assert_eq!(outcome.outputs["userName"], json!("ada"));
    assert_eq!(outcome.outputs["ts"], json!("1.2"));

    let sent = rig.requests();
    assert_eq!(sent.len(), 2, "post + retire");
    let post = sent[0].body.as_ref().expect("post body");
    assert_eq!(post["blocks"][1]["elements"][0]["action_id"], json!("approve"));
    assert_eq!(
        post["metadata"]["event_type"],
        json!("weft_await_action"),
        "the correlation id rides the message metadata"
    );
    let retire = sent[1].body.as_ref().expect("retire body");
    assert_eq!(retire["ts"], json!("1.2"), "the retire targets the posted message");
    let retired_text = retire["text"].as_str().expect("retire text");
    assert!(retired_text.contains("Approve"), "the decision is written back: {retired_text}");
    Ok(())
}

async fn empty_buttons(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig
        .run(
            &SlackAwaitActionNode,
            json!({
                "account": rig.access("slack"),
                "channel": "C1",
                "text": "Ship it?",
                "buttons": [],
            }),
        )
        .await;
    let err = outcome.result.expect_err("empty buttons must refuse").to_string();
    assert!(err.contains("non-empty"), "{err}");
    assert!(rig.requests().is_empty(), "nothing was posted");
    Ok(())
}
