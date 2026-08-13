//! SlackGetThread self-tests: whole-thread paging in order.

use serde_json::json;

use weft::{fixture_spec, FakeRig, LiveRig, NodeTest, WeftResult};

use crate::slack_delete_message::SlackDeleteMessageNode;
use crate::slack_send_message::SlackSendMessageNode;

use super::SlackGetThreadNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("pages_the_whole_thread_in_order", pages),
        NodeTest::live("one_real_thread_read_then_delete", "slack", live_thread).with_fixture(
            fixture_spec(
                "SLACK_CHANNEL_ID",
                "Channel id",
                "The channel the test posts in; invite the connected bot to it first.",
            ),
        ),
    ]
}

/// Post a parent and a threaded reply into the fixture channel, read
/// the thread back, then delete both posts: self-cleaning.
async fn live_thread(rig: LiveRig) -> WeftResult<()> {
    let channel = rig.fixture("SLACK_CHANNEL_ID")?;
    let account = || rig.access("slack");
    let parent = rig
        .run(
            &SlackSendMessageNode,
            json!({
                "account": account(),
                "channel": channel,
                "text": "weft node test: thread parent",
            }),
        )
        .await
        .ok()?;
    let parent_ts = parent.output("ts")?.as_str().expect("parent ts").to_string();
    let reply = rig
        .run(
            &SlackSendMessageNode,
            json!({
                "account": account(),
                "channel": channel,
                "text": "weft node test: thread reply",
                "threadTs": parent_ts,
            }),
        )
        .await
        .ok()?;
    let reply_ts = reply.output("ts")?.as_str().expect("reply ts").to_string();
    let thread = rig
        .run(
            &SlackGetThreadNode,
            json!({ "account": account(), "channel": channel, "ts": parent_ts }),
        )
        .await
        .ok()?;
    // Harvest, then DELETE both messages, then assert: the deletes are
    // the cleanup and must not be skipped by a failing assertion.
    let count = thread.output("count")?.as_f64().expect("message count");
    for ts in [reply_ts, parent_ts] {
        rig.run(
            &SlackDeleteMessageNode,
            json!({ "account": account(), "channel": channel, "ts": ts }),
        )
        .await
        .ok()?;
    }
    assert!(count >= 2.0, "the parent and its reply are in the thread, got {count}");
    Ok(())
}

const REPLIES: &str = "/api/conversations.replies?channel=C1&ts=1.0&limit=200";

async fn pages(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "GET",
        REPLIES,
        json!({
            "ok": true,
            "messages": [{ "ts": "1.0", "text": "parent" }, { "ts": "1.1", "text": "first" }],
            "response_metadata": { "next_cursor": "cur2" },
        }),
    );
    rig.respond(
        "GET",
        &format!("{REPLIES}&cursor=cur2"),
        json!({ "ok": true, "messages": [{ "ts": "1.2", "text": "second" }] }),
    );
    let outcome = rig
        .run(
            &SlackGetThreadNode,
            json!({ "account": rig.access("slack"), "channel": "C1", "ts": "1.0" }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["count"], json!(3.0));
    assert_eq!(outcome.outputs["messages"][0]["text"], json!("parent"), "parent first");
    assert_eq!(outcome.outputs["messages"][2]["text"], json!("second"));
    Ok(())
}
