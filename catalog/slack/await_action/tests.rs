//! SlackAwaitAction self-tests: post -> park -> click -> retire ->
//! decision, against a canned click, and the body run again after the
//! park posting nothing.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::SlackAwaitActionNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("posts_parks_and_delivers_the_decision", full_flow),
        NodeTest::fake("a_resumed_body_posts_nothing_and_waits_on_the_same_id", resumed),
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

/// The first run pauses in its wait (no click yet): the state holds the
/// id and the posted message. The run again comes back to that wait on
/// the same id, without posting a second message.
async fn resumed(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "POST",
        "/api/chat.postMessage",
        json!({ "ok": true, "ts": "1.2", "channel": "C1" }),
    );
    rig.respond("POST", "/api/chat.update", json!({ "ok": true, "ts": "1.2" }));
    let inputs = json!({
        "account": rig.access("slack"),
        "channel": "C1",
        "text": "Ship it?",
        "buttons": [{ "id": "approve", "label": "Approve" }],
    });

    rig.signal_paused();
    let paused = rig.run(&SlackAwaitActionNode, inputs.clone()).await;
    assert!(paused.paused(), "the first run pauses in its wait: {:?}", paused.result);
    let saved = rig.saved_state(&SlackAwaitActionNode).expect("the post saved the state");
    assert_eq!(saved["posted"], json!({ "channel": "C1", "ts": "1.2" }));
    assert!(saved["click"].is_null(), "no click yet: {saved}");

    rig.signal(json!({ "action": "approve", "user": "U7", "userName": "ada" }));
    let outcome = rig.run(&SlackAwaitActionNode, inputs).await.ok()?;
    assert_eq!(outcome.outputs["action"], json!("approve"));
    assert_eq!(outcome.outputs["ts"], json!("1.2"));

    let paths: Vec<String> = rig.requests().iter().map(|r| r.path.clone()).collect();
    assert_eq!(paths, vec!["/api/chat.postMessage", "/api/chat.update"], "one post across the two runs");
    let waits = rig.awaited_signals();
    assert_eq!(waits.len(), 2, "the paused wait, then the same wait answered");
    assert_eq!(waits[0].match_predicates, waits[1].match_predicates, "both on the one callback id");
    Ok(())
}
