//! SlackUpdateMessage self-tests: the rewrite and its no-content
//! refusal.

use serde_json::json;

use weft::{fixture_spec, FakeRig, LiveRig, NodeTest, WeftResult};

use crate::slack_delete_message::SlackDeleteMessageNode;
use crate::slack_send_message::SlackSendMessageNode;

use super::SlackUpdateMessageNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("rewrites_the_message_text", rewrites),
        NodeTest::fake("nothing_to_update_with_refuses", nothing_to_update),
        NodeTest::live("one_real_rewrite_then_delete", "slack", live_rewrite).with_fixture(
            fixture_spec(
                "SLACK_CHANNEL_ID",
                "Channel id",
                "The channel the test posts in; invite the connected bot to it first.",
            ),
        ),
    ]
}

/// Post into the fixture channel, rewrite the post, then delete it:
/// the pair-plus-edit is self-cleaning.
async fn live_rewrite(rig: LiveRig) -> WeftResult<()> {
    let channel = rig.fixture("SLACK_CHANNEL_ID")?;
    let sent = rig
        .run(
            &SlackSendMessageNode,
            json!({
                "account": rig.access("slack"),
                "channel": channel,
                "text": "weft node test: before the edit",
            }),
        )
        .await
        .ok()?;
    let ts = sent.output("ts")?.as_str().expect("message ts").to_string();
    let updated = rig
        .run(
            &SlackUpdateMessageNode,
            json!({
                "account": rig.access("slack"),
                "channel": channel,
                "ts": ts,
                "text": "weft node test: after the edit",
            }),
        )
        .await
        .ok()?;
    // Harvest, then DELETE, then assert: the delete is the cleanup and
    // must not be skipped by a failing assertion above it.
    let updated_ts = updated.output("ts")?.as_str().expect("edited ts").to_string();
    rig.run(
        &SlackDeleteMessageNode,
        json!({ "account": rig.access("slack"), "channel": channel, "ts": ts.clone() }),
    )
    .await
    .ok()?;
    assert_eq!(updated_ts, ts);
    Ok(())
}

async fn rewrites(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", "/api/chat.update", json!({ "ok": true, "ts": "1.2" }));
    let outcome = rig
        .run(
            &SlackUpdateMessageNode,
            json!({
                "account": rig.access("slack"),
                "channel": "C1",
                "ts": "1.2",
                "text": "new text",
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["ts"], json!("1.2"));
    let body = rig.requests()[0].body.clone().expect("update body");
    assert_eq!(body["text"], json!("new text"));
    Ok(())
}

async fn nothing_to_update(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig
        .run(
            &SlackUpdateMessageNode,
            json!({ "account": rig.access("slack"), "channel": "C1", "ts": "1.2" }),
        )
        .await;
    let err = outcome.result.expect_err("no text, no blocks must refuse").to_string();
    assert!(err.contains("nothing to update"), "{err}");
    Ok(())
}
