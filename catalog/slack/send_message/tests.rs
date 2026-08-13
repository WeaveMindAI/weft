//! SlackSendMessage self-tests: the post path against canned Slack
//! answers (exact request payloads pinned), and the destination rules.

use serde_json::json;

use weft::context::LogLevel;
use weft::{fixture_spec, FakeRig, LiveRig, NodeTest, WeftResult};

use crate::slack_delete_message::SlackDeleteMessageNode;

use super::SlackSendMessageNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("posts_to_a_channel_and_emits_the_permalink", posts_to_channel),
        NodeTest::fake("a_failed_permalink_read_never_fails_the_post", permalink_failure),
        NodeTest::fake("a_user_destination_opens_the_dm_first", dm_destination),
        NodeTest::fake("blocks_and_thread_ts_ride_the_post", blocks_and_thread),
        NodeTest::fake("a_post_at_schedules_instead_of_posting", scheduled_post),
        NodeTest::fake("refuses_two_destinations", refuses_two_destinations),
        NodeTest::live("one_real_send_then_delete", "slack", live_send_then_delete).with_fixture(
            fixture_spec(
                "SLACK_CHANNEL_ID",
                "Channel id",
                "The channel the test posts in; invite the connected bot to it first.",
            ),
        ),
    ]
}

/// Post into the fixture channel, then delete the post: the pair is
/// self-cleaning, so repeated runs leave nothing behind.
async fn live_send_then_delete(rig: LiveRig) -> WeftResult<()> {
    // The workspace's channels cannot be self-provisioned (there is no
    // create-channel node), so the target channel is a fixture.
    let channel = rig.fixture("SLACK_CHANNEL_ID")?;
    let outcome = rig
        .run(
            &SlackSendMessageNode,
            json!({
                "account": rig.access("slack"),
                "channel": channel,
                "text": "weft node test: send then delete",
            }),
        )
        .await
        .ok()?;
    // Harvest, then DELETE, then assert: an assertion between the send
    // and the delete would leave the message in the channel when it
    // fails. The delete is the cleanup, so it must be unskippable.
    let ts = outcome.output("ts")?.as_str().expect("message ts").to_string();
    let permalink = outcome.output("permalink")?.as_str().expect("permalink").to_string();
    let deleted = rig
        .run(
            &SlackDeleteMessageNode,
            json!({ "account": rig.access("slack"), "channel": channel, "ts": ts }),
        )
        .await
        .ok()?;
    assert!(permalink.starts_with("https://"), "a real permalink came back");
    assert_eq!(deleted.output("done")?, &json!(true));
    Ok(())
}

async fn blocks_and_thread(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "POST",
        "/api/chat.postMessage",
        json!({ "ok": true, "ts": "5.6", "channel": "C1" }),
    );
    rig.respond(
        "GET",
        "/api/chat.getPermalink",
        json!({ "ok": true, "permalink": "https://acme.slack.com/archives/C1/p56" }),
    );
    let blocks = json!([{ "type": "section", "text": { "type": "mrkdwn", "text": "*hi*" } }]);
    rig.run(
        &SlackSendMessageNode,
        json!({
            "account": rig.access("slack"),
            "channel": "C1",
            "text": "hi",
            "blocks": blocks,
            "threadTs": "1.2",
        }),
    )
    .await
    .ok()?;

    // The Block Kit payload rides verbatim next to the fallback text,
    // and the reply lands in the thread.
    let sent = rig.requests();
    let body = sent[0].body.as_ref().expect("post payload");
    assert_eq!(body["blocks"], blocks);
    assert_eq!(body["text"], json!("hi"));
    assert_eq!(body["thread_ts"], json!("1.2"));
    Ok(())
}

async fn dm_destination(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "POST",
        "/api/conversations.open",
        json!({ "ok": true, "channel": { "id": "D99" } }),
    );
    rig.respond(
        "POST",
        "/api/chat.postMessage",
        json!({ "ok": true, "ts": "3.4", "channel": "D99" }),
    );
    rig.respond(
        "GET",
        "/api/chat.getPermalink",
        json!({ "ok": true, "permalink": "https://acme.slack.com/archives/D99/p34" }),
    );

    let outcome = rig
        .run(
            &SlackSendMessageNode,
            json!({
                "account": rig.access("slack"),
                "user": "U7",
                "text": "hi there",
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["channel"], json!("D99"), "posted into the opened DM");

    let sent = rig.requests();
    assert_eq!(
        sent[0].body.as_ref().expect("open payload"),
        &json!({ "users": "U7" }),
        "the DM opens on the user id"
    );
    assert_eq!(
        sent[1].body.as_ref().expect("post payload")["channel"],
        json!("D99"),
        "the post lands on the returned conversation id"
    );
    Ok(())
}

async fn scheduled_post(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "POST",
        "/api/chat.scheduleMessage",
        json!({ "ok": true, "scheduled_message_id": "Q123" }),
    );

    let outcome = rig
        .run(
            &SlackSendMessageNode,
            json!({
                "account": rig.access("slack"),
                "channel": "C42",
                "text": "later",
                "postAt": 1790000000,
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["scheduledId"], json!("Q123"));
    assert!(
        !outcome.outputs.contains_key("ts"),
        "a scheduled message emits only its schedule id (nothing posted yet)"
    );

    let sent = rig.requests();
    assert_eq!(sent.len(), 1, "one schedule call, no post, no permalink read");
    assert_eq!(
        sent[0].body.as_ref().expect("schedule payload")["post_at"],
        json!(1790000000_i64),
        "post_at goes out as an integer"
    );
    Ok(())
}

async fn posts_to_channel(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "POST",
        "/api/chat.postMessage",
        json!({ "ok": true, "ts": "111.222", "channel": "C42" }),
    );
    rig.respond(
        "GET",
        "/api/chat.getPermalink",
        json!({ "ok": true, "permalink": "https://acme.slack.com/archives/C42/p111222" }),
    );

    let outcome = rig
        .run(
            &SlackSendMessageNode,
            json!({
                "account": rig.access("slack"),
                "channel": "C42",
                "text": "hello",
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["ts"], json!("111.222"));
    assert_eq!(outcome.outputs["channel"], json!("C42"));
    assert_eq!(
        outcome.outputs["permalink"],
        json!("https://acme.slack.com/archives/C42/p111222")
    );

    rig.assert_sent("POST", "/api/chat.postMessage");
    let post = &rig.requests()[0];
    assert_eq!(
        post.body.as_ref().expect("json payload"),
        &json!({ "channel": "C42", "text": "hello" }),
        "exactly the fields the user set, nothing invented"
    );
    Ok(())
}

async fn permalink_failure(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "POST",
        "/api/chat.postMessage",
        json!({ "ok": true, "ts": "111.222", "channel": "C42" }),
    );
    rig.respond(
        "GET",
        "/api/chat.getPermalink",
        json!({ "ok": false, "error": "message_not_found" }),
    );

    // The message is already up: the run succeeds, the message outputs
    // emit, and the permalink port stays un-emitted (downstream reads
    // its closure as absence). The failure surfaces in the log.
    let outcome = rig
        .run(
            &SlackSendMessageNode,
            json!({
                "account": rig.access("slack"),
                "channel": "C42",
                "text": "hello",
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["ts"], json!("111.222"));
    assert_eq!(outcome.outputs["channel"], json!("C42"));
    assert!(
        !outcome.outputs.contains_key("permalink"),
        "a failed permalink read emits nothing on the port"
    );
    let logged = rig.logs();
    assert!(
        logged
            .iter()
            .any(|(level, message)| *level == LogLevel::Error
                && message.contains("message_not_found")),
        "the permalink failure lands in the log: {logged:?}"
    );
    Ok(())
}

async fn refuses_two_destinations(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig
        .run(
            &SlackSendMessageNode,
            json!({
                "account": rig.access("slack"),
                "channel": "C42",
                "user": "U7",
                "text": "hello",
            }),
        )
        .await;
    let err = outcome.result.expect_err("both destinations must refuse").to_string();
    assert!(err.contains("ONE destination"), "{err}");
    assert!(rig.requests().is_empty(), "refused before any call");
    Ok(())
}
