//! SlackReact self-tests: add/remove and the colon-stripping.

use serde_json::json;

use weft::{fixture_spec, FakeRig, LiveRig, NodeTest, WeftResult};

use crate::slack_delete_message::SlackDeleteMessageNode;
use crate::slack_send_message::SlackSendMessageNode;

use super::SlackReactNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("adds_the_reaction_with_the_bare_name", adds),
        NodeTest::fake("remove_calls_the_remove_method", removes),
        NodeTest::live("one_real_reaction_on_a_deleted_message", "slack", live_react)
            .with_fixture(fixture_spec(
                "SLACK_CHANNEL_ID",
                "Channel id",
                "The channel the test posts in; invite the connected bot to it first.",
            )),
    ]
}

/// Post into the fixture channel, react to the post, then delete it:
/// the reaction dies with its message, so nothing accumulates.
async fn live_react(rig: LiveRig) -> WeftResult<()> {
    let channel = rig.fixture("SLACK_CHANNEL_ID")?;
    let sent = rig
        .run(
            &SlackSendMessageNode,
            json!({
                "account": rig.access("slack"),
                "channel": channel,
                "text": "weft node test: react target",
            }),
        )
        .await
        .ok()?;
    let ts = sent.output("ts")?.as_str().expect("message ts").to_string();
    let reacted = rig
        .run(
            &SlackReactNode,
            json!({
                "account": rig.access("slack"),
                "channel": channel,
                "ts": ts,
                "emoji": "white_check_mark",
            }),
        )
        .await
        .ok()?;
    // Harvest, then DELETE, then assert: the delete is the cleanup and
    // must not be skipped by a failing assertion above it.
    let reacted_done = reacted.output("done")?.clone();
    rig.run(
        &SlackDeleteMessageNode,
        json!({ "account": rig.access("slack"), "channel": channel, "ts": ts }),
    )
    .await
    .ok()?;
    assert_eq!(reacted_done, json!(true));
    Ok(())
}

async fn adds(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", "/api/reactions.add", json!({ "ok": true }));
    let outcome = rig
        .run(
            &SlackReactNode,
            json!({
                "account": rig.access("slack"),
                "channel": "C1",
                "ts": "1.2",
                "emoji": ":thumbsup:",
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["done"], json!(true));
    let body = rig.requests()[0].body.clone().expect("react body");
    assert_eq!(body["name"], json!("thumbsup"), "the pasted :colons: are stripped");
    Ok(())
}

async fn removes(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", "/api/reactions.remove", json!({ "ok": true }));
    rig.run(
        &SlackReactNode,
        json!({
            "account": rig.access("slack"),
            "channel": "C1",
            "ts": "1.2",
            "emoji": "eyes",
            "remove": true,
        }),
    )
    .await
    .ok()?;
    assert_eq!(rig.requests()[0].path, "/api/reactions.remove");
    Ok(())
}
