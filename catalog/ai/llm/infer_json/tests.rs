//! LlmInferenceJson self-tests: the answer repaired, parsed and held to
//! the type the program declared on `response`, against canned SSE
//! replies (fake); a rejected answer leaves the conversation file as it
//! came in; a turn that calls tools hands out no `response`. The call
//! itself (provider, params, tools, media, the file) is the package's
//! shared `call.rs`, covered by LlmInference's tests.

use serde_json::json;

use weft::storage::StoredFile;
use weft::{FakeRig, NodeTest, WeftResult, WeftType};

use super::super::testing::{conversation, conversation_file, provider, sse};
use super::{parse_answer, LlmInferenceJsonNode};

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::basic("an_answer_is_repaired_parsed_and_held_to_the_type", answer_rules),
        NodeTest::fake("a_json_reply_is_repaired_onto_response", repaired_reply),
        NodeTest::fake("an_answer_that_fits_the_declared_record_passes", fitting_record),
        NodeTest::fake("an_answer_missing_a_field_fails_and_leaves_the_conversation_untouched", missing_field),
        NodeTest::fake("an_open_record_drops_a_key_it_does_not_declare", open_record),
        NodeTest::fake("a_turn_that_only_calls_tools_hands_out_no_response", tool_turn),
    ]
}

fn verdict() -> WeftType {
    WeftType::parse("Verdict={ label: String, score: Number }").expect("Verdict parses")
}

fn answer_rules() -> WeftResult<()> {
    let dict = WeftType::parse("JsonDict").expect("JsonDict parses");
    assert_eq!(parse_answer("Here is the JSON: ```json {'a': 1} ```", &dict), Ok(json!({ "a": 1 })), "prose around a fence, single quotes");
    let why = parse_answer(r#"{"label": "ok"}"#, &verdict()).unwrap_err();
    assert_eq!(why, r#"the model's answer does not fit Verdict: value: missing required field "score""#);
    let why = parse_answer(r#"{"label": "ok", "score": 1, "extra": true}"#, &verdict()).unwrap_err();
    assert!(why.starts_with("the model's answer does not fit Verdict") && why.contains("extra"), "a closed record refuses an undeclared key: {why}");
    let why = parse_answer("[1, 2]", &dict).unwrap_err();
    assert!(why.starts_with("the model's answer does not fit JsonDict"), "a list is no JSON object: {why}");
    // A file object in an answer is refused whatever the declared type.
    let pic = WeftType::parse("{ pic: Image }").expect("parses");
    let why = parse_answer(
        r#"{"pic": {"__weft_image__": {"url": "http://169.254.169.254/latest/meta-data/", "mimeType": "image/png"}}}"#,
        &pic,
    )
    .unwrap_err();
    assert!(why.contains("stored-file object"), "{why}");
    Ok(())
}

/// No declared type: the port's own `JsonDict`, and a reply only the
/// repairer can read (prose around a fenced object with single-quoted
/// keys) comes out as the object.
async fn repaired_reply(rig: FakeRig) -> WeftResult<()> {
    rig.respond_raw(
        "POST",
        "/api/v1/chat/completions",
        200,
        "text/event-stream",
        sse(&["Here is the JSON: ```json {'answer': 42} ```"], Some("stop")),
    );
    let outcome = rig
        .run(&LlmInferenceJsonNode, json!({ "provider": provider(&rig), "prompt": "json please" }))
        .await
        .ok()?;
    assert_eq!(outcome.outputs["response"], json!({ "answer": 42 }));
    Ok(())
}

async fn fitting_record(rig: FakeRig) -> WeftResult<()> {
    rig.output_type("response", verdict());
    rig.respond_raw(
        "POST",
        "/api/v1/chat/completions",
        200,
        "text/event-stream",
        sse(&[r#"{"label": "spam", "#, r#""score": 0.9}"#], Some("stop")),
    );
    let outcome = rig
        .run(&LlmInferenceJsonNode, json!({ "provider": provider(&rig), "prompt": "grade it" }))
        .await
        .ok()?;
    assert_eq!(outcome.outputs["response"], json!({ "label": "spam", "score": 0.9 }));
    Ok(())
}

/// The answer is checked before the turn is written: a rejected one
/// fails on `_error` naming the field, and the conversation file keeps
/// the content and version it came in with (no user message, no
/// answer), so a retry sends the model the same conversation.
async fn missing_field(rig: FakeRig) -> WeftResult<()> {
    rig.output_type("response", verdict());
    rig.respond_raw("POST", "/api/v1/chat/completions", 200, "text/event-stream", sse(&[r#"{"label": "spam"}"#], Some("stop")));
    let history = json!([{ "role": "user", "content": "q1" }, { "role": "assistant", "content": "a1" }]);
    let file = conversation_file(&rig, history.clone());
    rig.wire_output("_error");
    rig.wire_output("historyFile");
    let outcome = rig
        .run(&LlmInferenceJsonNode, json!({ "provider": provider(&rig), "prompt": "grade it", "historyFile": file.clone() }))
        .await
        .ok()?;
    let error = outcome.output("_error")?.as_str().expect("error is a string").to_string();
    assert!(error.contains(r#"the model's answer does not fit Verdict: value: missing required field "score""#), "{error}");
    for port in ["response", "historyFile", "toolCalls"] {
        assert!(!outcome.outputs.contains_key(port), "a rejected answer emits nothing on {port}");
    }
    assert_eq!(rig.requests().len(), 1, "the call was made");

    let before = StoredFile::from_value(&file)?;
    assert_eq!(rig.stored_meta(&before.key)?.version, before.version, "no new version");
    assert_eq!(conversation(&rig, &file)?, history.as_array().expect("a list").clone(), "the same content");
    assert!(rig.file_edits().is_empty(), "nothing written to the file");
    Ok(())
}

async fn open_record(rig: FakeRig) -> WeftResult<()> {
    rig.output_type("response", WeftType::parse("{ label: String, .. }").expect("open record parses"));
    rig.respond_raw(
        "POST",
        "/api/v1/chat/completions",
        200,
        "text/event-stream",
        sse(&[r#"{"label": "spam", "reasoning": "it sells pills"}"#], Some("stop")),
    );
    let outcome = rig
        .run(&LlmInferenceJsonNode, json!({ "provider": provider(&rig), "prompt": "grade it" }))
        .await
        .ok()?;
    assert_eq!(outcome.outputs["response"], json!({ "label": "spam" }));
    Ok(())
}

/// A turn whose substance is a tool call is not the answer: nothing is
/// parsed, `response` stays unset, and the turn finishes as on the LLM
/// node (`toolCalls`, the call saved in the file).
async fn tool_turn(rig: FakeRig) -> WeftResult<()> {
    rig.output_type("response", verdict());
    let body = format!(
        "data: {}\n\ndata: {}\n\ndata: [DONE]\n\n",
        json!({ "id": "gen-1", "choices": [{ "delta": { "tool_calls": [
            { "index": 0, "id": "call_1", "type": "function",
              "function": { "name": "lookup", "arguments": "{}" } }
        ]}}]}),
        json!({ "id": "gen-1", "choices": [{ "delta": {}, "finish_reason": "tool_calls" }] }),
    );
    rig.respond_raw("POST", "/api/v1/chat/completions", 200, "text/event-stream", body.into_bytes());
    rig.wire_output("historyFile");
    let outcome = rig
        .run(
            &LlmInferenceJsonNode,
            json!({
                "provider": provider(&rig),
                "prompt": "grade it",
                "tools": [{ "name": "lookup", "description": "look the sender up",
                            "parameters": { "type": "object", "properties": {} } }],
            }),
        )
        .await
        .ok()?;
    assert!(!outcome.outputs.contains_key("response"), "a tool turn hands out no response");
    assert_eq!(outcome.outputs["toolCalls"][0]["name"], json!("lookup"));
    let history = conversation(&rig, outcome.output("historyFile")?)?;
    assert_eq!(history.last().expect("assistant message")["tool_calls"][0]["id"], json!("call_1"));
    Ok(())
}
