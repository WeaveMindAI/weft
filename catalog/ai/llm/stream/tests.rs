//! LlmStream self-tests: deltas land on the bus as they arrive, the
//! close ends the stream, and the whole reply still pulses.

use serde_json::json;

use weft::bus::BusEntryKind;
use weft::{FakeRig, NodeTest, WeftResult};

use super::LlmStreamNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("deltas_ride_the_bus_then_the_reply_pulses", streams),
        NodeTest::fake("a_dropped_stream_fails_loud_after_partial_deltas", truncated),
    ]
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

    // The guard closes the bus on EVERY exit, the failure path
    // included: the marker pulsed before the error, so a reader may be
    // parked on it, and only the close releases it. Draining to the
    // end proves termination (an unclosed bus would park this cursor
    // forever); the metadata names the model, as on the happy path.
    let bus = rig.bus(&outcome.outputs["stream"])?;
    assert_eq!(bus.meta()["model"], json!("test/model"));
    let mut cursor = bus.cursor_from_start();
    while cursor.next().await.is_some() {}
    Ok(())
}

/// OpenAI-style SSE chunks ending in a finish reason and `[DONE]`.
fn sse(chunks: &[&str], finish: Option<&str>) -> Vec<u8> {
    let mut body = String::new();
    for c in chunks {
        body.push_str(&format!(
            "data: {}\n\n",
            json!({ "id": "gen-1", "choices": [{ "delta": { "content": c } }] })
        ));
    }
    if let Some(reason) = finish {
        body.push_str(&format!(
            "data: {}\n\n",
            json!({ "id": "gen-1", "choices": [{ "delta": {}, "finish_reason": reason }] })
        ));
    }
    body.push_str("data: [DONE]\n\n");
    body.into_bytes()
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
    let mut cursor = bus.cursor_from_start();
    let mut deltas: Vec<String> = Vec::new();
    while let Some(entry) = cursor.next().await {
        if let BusEntryKind::Message { msg_kind, payload, .. } = &entry.kind {
            assert_eq!(msg_kind, "delta");
            if let Some(weft::bus::WirePayload::Json(v)) = payload {
                deltas.push(v.as_str().expect("delta text").to_string());
            }
        }
    }
    assert_eq!(deltas, vec!["Hel".to_string(), "lo".to_string()]);
    Ok(())
}
