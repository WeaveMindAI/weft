//! LlmStream self-tests: deltas land on the bus as they arrive, the
//! close ends the stream (failed when the reply broke off), the whole
//! reply still pulses, and a call refused before its first chunk opens
//! no bus and is caught on a wired `_error`.

use serde_json::json;

use weft::bus::StreamEnd;
use weft::stream_journal::JournalPolicy;
use weft::context::{HarnessRead, ERROR_PORT};
use weft::{FakeRig, LiveRig, NodeTest, WeftResult};

use super::super::testing::sse;
use super::LlmStreamNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("deltas_ride_the_bus_then_the_reply_pulses", streams),
        NodeTest::fake("the_journal_settings_reach_the_bus", journal_settings),
        NodeTest::fake("a_dropped_stream_fails_loud_after_partial_deltas", truncated),
        NodeTest::fake("a_call_refused_before_its_first_chunk_opens_no_bus", refused),
        NodeTest::live("one_real_streamed_completion", "openrouter", live_stream),
    ]
}

/// One real streamed completion on the cheapest routed model: the
/// deltas ride a real SSE wire (what the fake tier cannot prove) and
/// the whole reply still pulses. A one-word answer keeps the cost at
/// a fraction of a cent.
async fn live_stream(rig: LiveRig) -> WeftResult<()> {
    let outcome = rig
        .run(
            &LlmStreamNode,
            json!({
                "provider": {
                    "kind": "openrouter",
                    "model": "openai/gpt-4o-mini",
                    "account": rig.access("openrouter"),
                },
                "prompt": "Answer with the single word: pong",
            }),
        )
        .await
        .ok()?;
    let text = outcome.output("response")?.as_str().expect("text reply").to_lowercase();
    assert!(text.contains("pong"), "unexpected reply: {text}");
    Ok(())
}

/// A body that stops mid-stream (no finish reason, no `[DONE]`): the
/// run must fail loudly rather than pulse a half-answer, even though
/// deltas already rode the bus.
async fn truncated(rig: FakeRig) -> WeftResult<()> {
    rig.respond_raw(
        "POST",
        "/api/v1/chat/completions",
        200,
        "text/event-stream",
        format!(
            "data: {}\n\n",
            json!({ "id": "gen-1", "choices": [{ "delta": { "content": "half" } }] })
        )
        .into_bytes(),
    );
    let outcome = rig
        .run(
            &LlmStreamNode,
            json!({
                "provider": {
                    "kind": "openrouter",
                    "model": "test/model",
                    "account": rig.access("openrouter"),
                },
                "prompt": "say hello",
            }),
        )
        .await;
    let err = outcome.result.expect_err("a dropped stream must refuse").to_string();
    assert!(err.contains("truncated"), "{err}");

    // The bus ends with the body, the failure path included, and says
    // so: the marker pulsed before the error, so a reader may be parked
    // on it, and the failed end both releases it and tells it the reply
    // is cut: the read past the last message is that failure. Draining
    // to it proves termination; the metadata names the model, as on the
    // happy path.
    let bus = rig.bus(&outcome.outputs["stream"])?;
    assert_eq!(bus.meta()["model"], json!("test/model"));
    let mut cursor = bus.cursor_from_start().unwrap();
    let ended = loop {
        match cursor.next(&mut HarnessRead).await {
            Ok(Some(_)) => continue,
            Ok(None) => panic!("a cut reply must not read as a clean end"),
            Err(e) => break e.to_string(),
        }
    };
    assert!(ended.contains("truncated"), "{ended}");
    assert!(
        matches!(bus.end(), Some(StreamEnd::Failed { error }) if error.contains("truncated")),
        "{:?}",
        bus.end()
    );
    Ok(())
}

/// A call the provider refuses answers before any chunk: the bus never
/// opened, so no reader ever started, and the failure is the node's
/// outcome to catch on a wired `_error`, where a backup branch can
/// answer instead.
async fn refused(rig: FakeRig) -> WeftResult<()> {
    rig.respond_raw(
        "POST",
        "/api/v1/chat/completions",
        401,
        "application/json",
        json!({ "error": { "message": "no such key", "code": 401 } }).to_string().into_bytes(),
    );
    rig.wire_output(ERROR_PORT);
    let outcome = rig
        .run(
            &LlmStreamNode,
            json!({
                "provider": {
                    "kind": "openrouter",
                    "model": "test/model",
                    "account": rig.access("openrouter"),
                },
                "prompt": "say hello",
            }),
        )
        .await
        .ok()?;
    assert!(outcome.outputs.get("stream").is_none(), "no bus opened: {:?}", outcome.outputs);
    let caught = outcome.outputs[ERROR_PORT].as_str().expect("the caught failure is text");
    assert!(caught.contains("401") || caught.contains("no such key"), "{caught}");
    Ok(())
}

async fn streams(rig: FakeRig) -> WeftResult<()> {
    rig.respond_raw(
        "POST",
        "/api/v1/chat/completions",
        200,
        "text/event-stream",
        sse(&["Hel", "lo"], Some("stop")),
    );
    let outcome = rig
        .run(
            &LlmStreamNode,
            json!({
                "provider": {
                    "kind": "openrouter",
                    "model": "test/model",
                    "account": rig.access("openrouter"),
                },
                "prompt": "say hello",
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["response"], json!("Hello"));

    // The bus behind the emitted marker: both deltas, then the close.
    let bus = rig.bus(&outcome.outputs["stream"])?;
    let mut cursor = bus.cursor_from_start().unwrap();
    let mut deltas: Vec<String> = Vec::new();
    while let Some(entry) = cursor.next(&mut HarnessRead).await? {
        if let Some((msg_kind, v)) = entry.as_json() {
            assert_eq!(msg_kind, "delta");
            deltas.push(v.as_str().expect("delta text").to_string());
        }
    }
    assert_eq!(deltas, vec!["Hel".to_string(), "lo".to_string()]);
    assert_eq!(bus.end(), Some(StreamEnd::Finished));
    assert_eq!(bus.journal(), JournalPolicy::default(), "unset, the bus is journaled on the default window");
    Ok(())
}

/// What the node's users set on the journal settings is the bus's
/// journal: written down or not, and how long one row gathers deltas.
async fn journal_settings(rig: FakeRig) -> WeftResult<()> {
    rig.respond_raw("POST", "/api/v1/chat/completions", 200, "text/event-stream", sse(&["Hi"], Some("stop")));
    let outcome = rig
        .run(
            &LlmStreamNode,
            json!({
                "provider": { "kind": "openrouter", "model": "test/model", "account": rig.access("openrouter") },
                "prompt": "say hi",
                "journalEphemeral": true,
                "journalWindowMs": 200,
            }),
        )
        .await
        .ok()?;
    let journal = rig.bus(&outcome.outputs["stream"])?.journal();
    assert!(journal.is_ephemeral());
    assert_eq!(journal.window, std::time::Duration::from_millis(200));
    Ok(())
}
