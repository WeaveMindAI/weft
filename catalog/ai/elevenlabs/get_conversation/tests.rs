//! ElevenLabsGetConversation self-tests.

use serde_json::{json, Map, Value};

use weft::storage::StorageScope;
use weft::{FakeRig, NodeTest, WeftResult};

use crate::testing::emitted_file;

use super::ElevenLabsGetConversationNode;

pub fn tests() -> Vec<NodeTest> {
    // Live coverage rides the agent_call round trip: its live test
    // reads the conversation it just dialed back THROUGH THIS NODE
    // (mint agent -> call -> Get Conversation -> delete agent). A
    // standalone live read cannot exist on a self-cleaning account:
    // every past conversation's agent is deleted by the tests that
    // minted it, and ElevenLabs answers 404 for a conversation whose
    // agent is gone, so there is never an old readable conversation
    // to pick up.
    vec![
        NodeTest::fake("waits_then_reads_and_shapes_a_done_conversation", reads),
        NodeTest::fake("a_resumed_run_has_the_conversation_its_wait_ended_on", resumed),
        NodeTest::fake("without_waiting_reads_the_call_as_it_stands", reads_now),
        NodeTest::fake("the_whole_conversation_is_stored_only_when_something_reads_it", conversation_only_when_read),
        NodeTest::fake("the_trace_is_read_only_when_something_reads_it", trace),
        NodeTest::fake("a_trace_elevenlabs_did_not_answer_fails_loud", trace_missing),
        NodeTest::fake("an_unexpected_status_fails_loud", unexpected_status),
        NodeTest::fake("a_conversation_id_can_only_ever_name_a_conversation", conversation_id_encoded),
    ]
}

/// The JSON a node emitted on `port` as a stored file, parsed, and the
/// file's stored name; loud when the file is not JSON.
fn json_file(rig: &FakeRig, outputs: &Map<String, Value>, port: &str) -> WeftResult<(Value, String)> {
    let (bytes, filename, mime) = emitted_file(rig, outputs, port)?;
    assert_eq!(mime, "application/json", "`{port}` is a JSON file");
    Ok((serde_json::from_slice(&bytes).expect("the file holds JSON"), filename))
}

async fn reads(rig: FakeRig) -> WeftResult<()> {
    rig.wire_output("conversation");
    rig.signal(json!({ "status": "done" }));
    rig.respond("GET", "/v1/convai/conversations/conv-1", json!({
        "status": "done",
        "transcript": [
            { "role": "agent", "message": "Hello!", "extra": "dropped" },
            { "role": "user", "message": "Hi." },
        ],
        "analysis": {
            "transcript_summary": "A greeting.",
            "call_summary_title": "Greeting",
            "call_successful": "success",
            "call_success_score": 87.5,
            "data_collection_results": {
                "customer_name": { "data_collection_id": "customer_name", "value": "Ada", "rationale": "She said so." },
                "party_size": { "data_collection_id": "party_size", "value": 4, "rationale": "Four people." },
            },
            "evaluation_criteria_results": {
                "booked": { "criteria_id": "booked", "result": "success", "rationale": "A table was booked." },
            },
        },
        "metadata": {
            "start_time_unix_secs": 1760000000,
            "call_duration_secs": 42,
            "termination_reason": "Call ended by remote party",
            "cost_fiat": 0.12,
        },
    }));
    rig.respond_raw(
        "GET",
        "/v1/convai/conversations/conv-1/audio",
        200,
        "audio/mpeg",
        b"call-audio".to_vec(),
    );
    let outcome = rig
        .run(
            &ElevenLabsGetConversationNode,
            json!({
                "account": rig.access("elevenlabs"),
                "conversationId": "conv-1",
                "waitUntilDone": true,
                "includeAudio": true,
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["status"], json!("done"));
    let (transcript, filename) = json_file(&rig, &outcome.outputs, "transcript")?;
    assert_eq!(filename, "conversation_conv-1_transcript.json");
    assert_eq!(
        transcript,
        json!({
            "text": "agent: Hello!\nuser: Hi.",
            "turns": [
                { "role": "agent", "message": "Hello!" },
                { "role": "user", "message": "Hi." },
            ],
        }),
        "each turn's role and message, and the whole transcript as lines"
    );
    assert_eq!(outcome.outputs["summary"], json!("A greeting."));
    assert_eq!(outcome.outputs["summaryTitle"], json!("Greeting"));
    assert_eq!(outcome.outputs["successful"], json!("success"));
    assert_eq!(outcome.outputs["successScore"], json!(87.5));
    assert_eq!(outcome.outputs["dataCollection"], json!({ "customer_name": "Ada", "party_size": 4 }));
    assert_eq!(outcome.outputs["evaluation"], json!({ "booked": "success" }));
    assert_eq!(outcome.outputs["durationSecs"], json!(42));
    assert_eq!(outcome.outputs["terminationReason"], json!("Call ended by remote party"));
    assert_eq!(outcome.outputs["costUsd"], json!(0.12));
    let (conversation, filename) = json_file(&rig, &outcome.outputs, "conversation")?;
    assert_eq!(filename, "conversation_conv-1.json");
    assert_eq!(conversation["metadata"]["call_duration_secs"], json!(42), "the whole object");
    assert_eq!(conversation["transcript"][0]["extra"], json!("dropped"), "exactly as ElevenLabs answered it");
    assert!(outcome.outputs["audio"].is_object(), "the recording lands as a stored file");
    assert!(!outcome.outputs.contains_key("otlpTrace"), "nothing reads the trace, so it is not asked for");

    let awaited = rig.awaited_signals();
    assert_eq!(awaited.len(), 1, "one wait");
    assert_eq!(awaited[0].kind, "poll_endpoint");
    assert_eq!(
        awaited[0].config["url"],
        json!("https://api.elevenlabs.io/v1/convai/conversations/conv-1")
    );
    assert!(awaited[0].access.is_some(), "each poll is signed by the elevenlabs connection");
    assert_eq!(rig.requests().len(), 2, "the finished conversation read after the wait, and its audio");
    Ok(())
}

/// A resumed body runs from its top with the status its wait ended on in
/// its state: the wait is not made again, and the finished conversation,
/// read again, gives the same outputs.
async fn resumed(rig: FakeRig) -> WeftResult<()> {
    rig.signal(json!({ "status": "done" }));
    rig.respond("GET", "/v1/convai/conversations/conv-1", json!({ "status": "done", "transcript": [{ "role": "agent", "message": "Hello!" }] }));
    let given = json!({
        "account": rig.access("elevenlabs"),
        "conversationId": "conv-1",
        "waitUntilDone": true,
        "includeAudio": false,
    });
    rig.run(&ElevenLabsGetConversationNode, given.clone()).await.ok()?;
    let again = rig.run(&ElevenLabsGetConversationNode, given).await.ok()?;
    assert_eq!(again.outputs["status"], json!("done"));
    assert_eq!(rig.awaited_signals().len(), 1, "the answered wait is not made again");
    assert_eq!(rig.requests().len(), 2, "the finished conversation is read once per run");
    Ok(())
}

/// Not waiting reads the conversation once and emits whatever state it
/// is in, parking on nothing; the analysis ports stay closed until the
/// analysis exists.
async fn reads_now(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "GET",
        "/v1/convai/conversations/conv-2",
        json!({
            "status": "in-progress",
            "transcript": [{ "role": "agent", "message": "Hi" }],
            "metadata": { "call_duration_secs": 5, "termination_reason": "" },
        }),
    );
    let outcome = rig
        .run(
            &ElevenLabsGetConversationNode,
            json!({
                "account": rig.access("elevenlabs"),
                "conversationId": "conv-2",
                "waitUntilDone": false,
                "includeAudio": false,
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["status"], json!("in-progress"));
    let (transcript, _) = json_file(&rig, &outcome.outputs, "transcript")?;
    assert_eq!(transcript["text"], json!("agent: Hi"), "the transcript so far");
    assert_eq!(outcome.outputs["durationSecs"], json!(5));
    for port in ["summary", "summaryTitle", "successful", "successScore", "dataCollection", "evaluation", "terminationReason", "costUsd"] {
        assert!(!outcome.outputs.contains_key(port), "'{port}' stays closed while the call runs");
    }
    assert!(rig.awaited_signals().is_empty(), "no wait when not asked to");
    Ok(())
}

/// With nothing reading `conversation`, the whole object is never
/// stored; the transcript still is, since it is the node's product.
async fn conversation_only_when_read(rig: FakeRig) -> WeftResult<()> {
    rig.respond("GET", "/v1/convai/conversations/conv-6", json!({ "status": "done", "transcript": [] }));
    let outcome = rig
        .run(
            &ElevenLabsGetConversationNode,
            json!({
                "account": rig.access("elevenlabs"),
                "conversationId": "conv-6",
                "waitUntilDone": false,
                "includeAudio": false,
            }),
        )
        .await
        .ok()?;
    assert!(!outcome.outputs.contains_key("conversation"), "nothing reads the whole object");
    let stored: Vec<String> =
        rig.stored_files(&StorageScope::Run)?.into_iter().map(|f| f.filename).collect();
    assert_eq!(stored, vec!["conversation_conv-6_transcript.json".to_string()], "only the transcript is stored");
    let (transcript, _) = json_file(&rig, &outcome.outputs, "transcript")?;
    assert_eq!(transcript, json!({ "text": "", "turns": [] }), "a call with no turns yet");
    Ok(())
}

/// With `otlpTrace` read downstream, the trace comes from its own read
/// (`format=opentelemetry`) and lands as a JSON file; the conversation
/// itself is still the plain read.
async fn trace(rig: FakeRig) -> WeftResult<()> {
    rig.wire_output("otlpTrace");
    rig.wire_output("conversation");
    let spans = json!({ "resourceSpans": [{ "scopeSpans": [{ "spans": [{ "name": "conversation" }] }] }] });
    rig.respond("GET", "/v1/convai/conversations/conv-3", json!({ "status": "done", "transcript": [] }));
    rig.respond(
        "GET",
        "/v1/convai/conversations/conv-3?format=opentelemetry",
        json!({ "status": "done", "transcript": [], "otlp_traces": spans }),
    );
    let outcome = rig
        .run(
            &ElevenLabsGetConversationNode,
            json!({
                "account": rig.access("elevenlabs"),
                "conversationId": "conv-3",
                "waitUntilDone": false,
                "includeAudio": false,
            }),
        )
        .await
        .ok()?;
    let (trace, filename) = json_file(&rig, &outcome.outputs, "otlpTrace")?;
    assert_eq!(trace, spans);
    assert_eq!(filename, "conversation_conv-3_otlp.json");
    let (conversation, _) = json_file(&rig, &outcome.outputs, "conversation")?;
    assert!(conversation.get("otlp_traces").is_none(), "the conversation is the plain read");
    let requests = rig.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[1].query.as_deref(), Some("format=opentelemetry"));
    Ok(())
}

async fn trace_missing(rig: FakeRig) -> WeftResult<()> {
    rig.wire_output("otlpTrace");
    rig.respond("GET", "/v1/convai/conversations/conv-4", json!({ "status": "done", "transcript": [] }));
    rig.respond(
        "GET",
        "/v1/convai/conversations/conv-4?format=opentelemetry",
        json!({ "status": "done", "transcript": [] }),
    );
    let err = rig
        .run(
            &ElevenLabsGetConversationNode,
            json!({
                "account": rig.access("elevenlabs"),
                "conversationId": "conv-4",
                "waitUntilDone": false,
                "includeAudio": false,
            }),
        )
        .await
        .failure()?;
    assert!(err.contains("no otlp_traces for conversation conv-4"), "{err}");
    Ok(())
}

/// A status the node does not know ends the wait (it is not in flight)
/// and fails loud, instead of being emitted as though the call ended.
async fn unexpected_status(rig: FakeRig) -> WeftResult<()> {
    rig.signal(json!({ "status": "archived" }));
    rig.respond("GET", "/v1/convai/conversations/conv-5", json!({ "status": "archived", "transcript": [] }));
    let err = rig
        .run(
            &ElevenLabsGetConversationNode,
            json!({
                "account": rig.access("elevenlabs"),
                "conversationId": "conv-5",
                "waitUntilDone": true,
                "includeAudio": false,
            }),
        )
        .await
        .failure()?;
    assert!(err.contains("unexpected status 'archived'"), "{err}");
    Ok(())
}

/// A typed conversation id that looks like a path stays one path
/// segment, so it cannot reach another ElevenLabs route.
async fn conversation_id_encoded(rig: FakeRig) -> WeftResult<()> {
    rig.respond("GET", "/v1/convai/conversations/..%2Fagents", json!({ "status": "done", "transcript": [] }));
    rig.run(
        &ElevenLabsGetConversationNode,
        json!({
            "account": rig.access("elevenlabs"),
            "conversationId": "../agents",
            "waitUntilDone": false,
            "includeAudio": false,
        }),
    )
    .await
    .ok()?;
    assert_eq!(rig.requests()[0].path, "/v1/convai/conversations/..%2Fagents");
    Ok(())
}
