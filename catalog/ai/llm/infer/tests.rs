//! LlmInference self-tests: the buffered call against canned SSE
//! replies (fake), covering the whole assemble surface (media
//! attachments, tools + toolChoice, params + system-prompt seeding,
//! the native Anthropic wire, a connection-less custom endpoint,
//! OpenRouter routing pins), and one real completion (live).

use serde_json::{json, Value};

use weft::storage::{StorageScope, StoredFile};
use weft::{FakeRig, LiveRig, NodeTest, WeftResult, WeftType};

use super::super::call::{empty_reply_message, reasoning_config};
use super::super::chat::{answer_unanswered_tool_calls, auto_cache_marks, has_cache_mark, UNANSWERED_TOOL_CALL};
use super::LlmInferenceNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::basic("reasoning_has_two_states_and_off_is_the_default", reasoning_states),
        NodeTest::basic("an_empty_reply_names_the_thinking_budget", empty_reply_wording),
        NodeTest::basic("auto_cache_marks_the_persona_and_the_last_history_turn", auto_cache_placement),
        NodeTest::basic("a_tool_call_left_unanswered_is_answered_cancelled", unanswered_tool_calls),
        NodeTest::fake("a_stopped_tool_round_sends_a_whole_conversation_and_saves_no_synthetic_answer", unanswered_on_the_wire),
        NodeTest::fake("reasoning_off_sends_none", reasoning_off_on_the_wire),
        NodeTest::fake("unwritten_params_are_off_and_send_no_max_tokens", unset_sends_nothing),
        NodeTest::fake("a_refused_reasoning_off_says_what_to_change", reasoning_refused),
        NodeTest::fake("a_server_error_mentioning_reasoning_stays_catchable", reasoning_transient),
        NodeTest::fake("auto_cache_tracks_each_request_without_marking_saved_history", auto_cache_on_the_wire),
        NodeTest::fake("a_completion_answers_text_and_keeps_nothing_unasked", completion),
        NodeTest::fake("a_wired_history_file_output_starts_a_conversation", starts_a_conversation),
        NodeTest::fake("a_history_file_grows_in_place", history_file_grows),
        NodeTest::fake("a_history_file_that_is_no_conversation_is_refused_before_the_call", history_file_refused),
        NodeTest::fake("parse_json_repairs_and_parses_the_reply", parse_json),
        NodeTest::fake("a_stream_without_finish_reason_is_truncated", truncated),
        NodeTest::fake("media_rides_the_user_message_as_image_parts", media_parts),
        NodeTest::fake("tools_and_tool_choice_ride_and_calls_surface", tool_round),
        NodeTest::fake("params_shape_the_request_and_seed_the_system_prompt", params_shape),
        NodeTest::fake("a_conversation_keeps_its_own_system_prompt", history_system_wins),
        NodeTest::fake("anthropic_speaks_its_native_wire", anthropic_wire),
        NodeTest::fake("a_custom_endpoint_runs_without_a_connection", custom_no_account),
        NodeTest::fake("openrouter_routing_pins_ride_the_provider_key", routing_pins),
        NodeTest::fake("a_refused_call_fails_the_run_when_error_is_unwired", refused_unwired),
        NodeTest::fake("a_refused_call_comes_out_on_error_when_it_is_wired", refused_wired),
        NodeTest::fake("a_provider_without_connection_fails_the_run_even_with_error_wired", mistake_wired),
        NodeTest::live("one_real_completion", "openrouter", live_completion),
    ]
}

fn reasoning_states() -> WeftResult<()> {
    assert_eq!(reasoning_config(false, Some("high")).effort.as_deref(), Some("none"), "off is off, whatever effort sits beside it");
    assert_eq!(reasoning_config(true, None).effort.as_deref(), Some("low"), "on with no effort picked is the cheap one");
    assert_eq!(reasoning_config(true, Some("high")).effort.as_deref(), Some("high"));
    Ok(())
}

fn empty_reply_wording() -> WeftResult<()> {
    let thought = minillmlib::Usage { reasoning_tokens: Some(812), ..Default::default() };
    let msg = empty_reply_message(Some(&thought));
    assert!(msg.contains("812 reasoning tokens") && msg.contains("0 text tokens"), "{msg}");
    assert!(msg.contains("maxTokens") && msg.contains("reasoning off"), "{msg}");
    let plain = empty_reply_message(None);
    assert!(plain.contains("empty response"), "{plain}");
    Ok(())
}

fn auto_cache_placement() -> WeftResult<()> {
    let message = |role: &str, text: &str| json!({ "role": role, "content": text });
    // A persona, two history turns, and this call's new user message.
    let mut stored = vec![
        message("system", "persona"),
        message("user", "q1"),
        message("assistant", "a1"),
        message("user", "q2"),
    ];
    auto_cache_marks(&mut stored, 1);
    let marks: Vec<bool> = stored.iter().map(has_cache_mark).collect();
    assert_eq!(marks, vec![true, false, true, false], "persona and the last history turn");

    // The author's own marks win: nothing is added.
    let mut own = vec![message("system", "persona"), message("user", "q1")];
    own[1]["cache_breakpoint"] = json!(true);
    auto_cache_marks(&mut own, 0);
    assert!(!has_cache_mark(&own[0]) && has_cache_mark(&own[1]));

    // A first call (persona + the new turn, no history yet) marks the
    // persona alone.
    let mut first = vec![message("system", "persona"), message("user", "q1")];
    auto_cache_marks(&mut first, 1);
    assert!(has_cache_mark(&first[0]) && !has_cache_mark(&first[1]));
    Ok(())
}

/// A run stopped between the model asking for tools and the results
/// being appended leaves calls with no answer; each is answered
/// `cancelled` right after the tool answers it did get, in call order,
/// and a conversation with every call answered is left as it is.
fn unanswered_tool_calls() -> WeftResult<()> {
    let call = |id: &str| json!({ "id": id, "name": "render", "arguments": "{}" });
    let mut stored = vec![
        json!({ "role": "user", "content": "go" }),
        json!({ "role": "assistant", "content": "", "tool_calls": [call("a"), call("b"), call("c")] }),
        json!({ "role": "tool", "content": "done", "tool_call_id": "b" }),
        json!({ "role": "user", "content": "stop that" }),
    ];
    answer_unanswered_tool_calls(&mut stored);
    let shape: Vec<(String, Option<String>)> = stored
        .iter()
        .map(|m| (m["role"].as_str().unwrap_or_default().to_string(), m["tool_call_id"].as_str().map(str::to_string)))
        .collect();
    let tool = |id: &str| ("tool".to_string(), Some(id.to_string()));
    assert_eq!(
        shape,
        vec![("user".into(), None), ("assistant".into(), None), tool("b"), tool("a"), tool("c"), ("user".into(), None)]
    );
    assert_eq!(stored[3]["content"], json!(UNANSWERED_TOOL_CALL));
    let whole = stored.clone();
    answer_unanswered_tool_calls(&mut stored);
    assert_eq!(stored, whole, "nothing left to answer");
    Ok(())
}

/// The repair reaches the provider only: the saved file keeps the call
/// unanswered, so a run sharing the file that is still mid tool call
/// can write the real answer without the call ending up with two.
async fn unanswered_on_the_wire(rig: FakeRig) -> WeftResult<()> {
    rig.output_type("response", WeftType::parse("String").expect("String parses"));
    rig.respond_raw("POST", "/api/v1/chat/completions", 200, "text/event-stream", sse(&["ok"], Some("stop")));
    let stopped = json!([
        { "role": "user", "content": "render" },
        { "role": "assistant", "content": "", "tool_calls": [{ "id": "t1", "name": "render", "arguments": "{}" }] },
    ]);
    let file = conversation_file(&rig, stopped);
    let outcome = rig
        .run(&LlmInferenceNode, json!({ "provider": provider(&rig), "prompt": "never mind", "historyFile": file }))
        .await
        .ok()?;
    let sent = rig.requests()[0].body.clone().expect("call body");
    let roles: Vec<_> = sent["messages"].as_array().expect("messages").iter().map(|m| m["role"].clone()).collect();
    assert_eq!(roles, vec![json!("user"), json!("assistant"), json!("tool"), json!("user")], "{sent}");
    let written = conversation(&rig, outcome.output("historyFile")?)?;
    let roles: Vec<_> = written.iter().map(|m| m["role"].clone()).collect();
    assert_eq!(roles, vec![json!("user"), json!("assistant"), json!("user"), json!("assistant")], "{written:?}");
    assert!(written.iter().all(|m| m["content"] != json!(UNANSWERED_TOOL_CALL)), "no synthetic answer saved: {written:?}");
    Ok(())
}

async fn reasoning_off_on_the_wire(rig: FakeRig) -> WeftResult<()> {
    rig.output_type("response", WeftType::parse("String").expect("String parses"));
    rig.respond_raw("POST", "/api/v1/chat/completions", 200, "text/event-stream", sse(&["ok"], Some("stop")));
    rig.run(
        &LlmInferenceNode,
        json!({ "provider": provider(&rig), "prompt": "hi", "params": { "reasoning": false } }),
    )
    .await
    .ok()?;
    let sent = rig.requests();
    let off = sent[0].body.as_ref().expect("body");
    assert_eq!(off["reasoning"]["effort"], json!("none"), "explicit false sends none");
    Ok(())
}

async fn unset_sends_nothing(rig: FakeRig) -> WeftResult<()> {
    rig.output_type("response", WeftType::parse("String").expect("String parses"));
    rig.respond_raw("POST", "/api/v1/chat/completions", 200, "text/event-stream", sse(&["ok"], Some("stop")));
    rig.run(&LlmInferenceNode, json!({ "provider": provider(&rig), "prompt": "hi", "params": {} }))
        .await
        .ok()?;
    let sent = rig.requests();
    let unset = sent[0].body.as_ref().expect("body");
    assert!(unset.get("max_completion_tokens").is_none(), "no maxTokens, none sent: {unset}");
    // Nothing written for `reasoning` is the same state as off: no model
    // runs at a default nobody wrote down.
    assert_eq!(unset["reasoning"]["effort"], json!("none"), "unwritten reasoning is off: {unset}");
    Ok(())
}

/// Only a client refusal over reasoning is the settings mistake: a
/// server failure whose text mentions reasoning is transient, so it
/// goes to a wired `error` like any other provider failure.
async fn reasoning_transient(rig: FakeRig) -> WeftResult<()> {
    rig.output_type("response", WeftType::parse("String").expect("String parses"));
    rig.respond_raw(
        "POST",
        "/api/v1/chat/completions",
        503,
        "application/json",
        br#"{"error":{"message":"reasoning backend overloaded, reason: capacity"}}"#.to_vec(),
    );
    rig.wire_output("error");
    let outcome = rig
        .run(
            &LlmInferenceNode,
            json!({ "provider": provider(&rig), "prompt": "hi", "params": { "reasoning": false } }),
        )
        .await
        .ok()?;
    let error = outcome.output("error")?.as_str().expect("error is a string").to_string();
    assert!(error.contains("overloaded") && !error.contains("always reasons"), "{error}");
    Ok(())
}

async fn reasoning_refused(rig: FakeRig) -> WeftResult<()> {
    rig.output_type("response", WeftType::parse("String").expect("String parses"));
    rig.respond_raw(
        "POST",
        "/api/v1/chat/completions",
        400,
        "application/json",
        br#"{"error":{"message":"reasoning cannot be disabled for this model"}}"#.to_vec(),
    );
    // A settings mistake: it fails the run even with `error` wired.
    rig.wire_output("error");
    let err = rig
        .run(
            &LlmInferenceNode,
            json!({ "provider": provider(&rig), "prompt": "hi", "params": { "reasoning": false } }),
        )
        .await
        .failure()?;
    assert!(err.contains("always reasons") && err.contains("reasoning: true"), "{err}");
    Ok(())
}

async fn auto_cache_on_the_wire(rig: FakeRig) -> WeftResult<()> {
    rig.output_type("response", WeftType::parse("String").expect("String parses"));
    rig.respond_raw("POST", "/api/v1/chat/completions", 200, "text/event-stream", sse(&["ok"], Some("stop")));
    let file = conversation_file(&rig, json!([
        { "role": "user", "content": "q1" },
        { "role": "assistant", "content": "a1" }
    ]));
    // OpenRouter passes cache markers through for Claude models only
    // (the others cache on their own), so the wire proof uses one.
    let claude = json!({ "kind": "openrouter", "model": "anthropic/claude-test", "account": rig.access("openrouter") });
    let outcome = rig
        .run(
            &LlmInferenceNode,
            json!({
                "provider": claude,
                "prompt": "q2",
                "historyFile": file,
                "params": { "systemPrompt": "persona" },
            }),
        )
        .await
        .ok()?;
    let kept = conversation(&rig, outcome.output("historyFile")?)?;
    let marks: Vec<bool> = kept.iter().map(has_cache_mark).collect();
    assert_eq!(marks, vec![false; 5], "automatic marks must not become author instructions: {kept:?}");
    let sent = rig.requests();
    let body = sent[0].body.as_ref().expect("body");
    assert!(body["messages"][0].to_string().contains("cache_control"), "persona: {body}");
    assert!(body["messages"][2].to_string().contains("cache_control"), "last previous answer: {body}");
    assert!(!body["messages"][3].to_string().contains("cache_control"), "new question: {body}");

    let second = rig.run(&LlmInferenceNode, json!({
        "provider": claude, "prompt": "q3", "historyFile": outcome.output("historyFile")?.clone(),
    })).await.ok()?;
    let sent = rig.requests();
    let body = sent[1].body.as_ref().expect("second request");
    assert!(body["messages"][0].to_string().contains("cache_control"), "persona remains cached: {body}");
    assert!(!body["messages"][2].to_string().contains("cache_control"), "old automatic mark must move: {body}");
    assert!(body["messages"][4].to_string().contains("cache_control"), "latest answer must be cached: {body}");
    let mut authored = conversation(&rig, second.output("historyFile")?)?;
    assert!(!authored.iter().any(has_cache_mark));

    authored[1]["cache_breakpoint"] = json!(true);
    let third = rig.run(&LlmInferenceNode, json!({
        "provider": claude, "prompt": "q4", "historyFile": conversation_file(&rig, Value::Array(authored)),
    })).await.ok()?;
    let marks: Vec<_> = conversation(&rig, third.output("historyFile")?)?.iter().enumerate()
        .filter(|(_, message)| has_cache_mark(message)).map(|(index, _)| index).collect();
    assert_eq!(marks, vec![1], "explicit author placement survives every call");
    Ok(())
}

fn provider(rig: &FakeRig) -> serde_json::Value {
    json!({ "kind": "openrouter", "model": "test/model", "account": rig.access("openrouter") })
}

/// A conversation as an author starts one: a project file holding it.
fn conversation_file(rig: &FakeRig, messages: Value) -> Value {
    rig.store_file_in(&StorageScope::Project, "chat.json", "application/json", serde_json::to_vec(&messages).expect("json"))
}

/// The conversation the file value `file` names holds now.
fn conversation(rig: &FakeRig, file: &Value) -> WeftResult<Vec<Value>> {
    let key = StoredFile::from_value(file)?.key;
    let written: Value = serde_json::from_slice(&rig.stored_bytes(&key)?).expect("the file holds JSON");
    Ok(written.as_array().expect("a list of messages").clone())
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

/// A conversation kept in a file: it is read from the file, sent, and
/// this call's turn is added to that same file in place (next version),
/// which comes out on `historyFile`; no second file appears, and the
/// change is recorded as a readable diff.
async fn history_file_grows(rig: FakeRig) -> WeftResult<()> {
    rig.output_type("response", WeftType::parse("String").expect("String parses"));
    rig.respond_raw("POST", "/api/v1/chat/completions", 200, "text/event-stream", sse(&["a2"], Some("stop")));
    let file = conversation_file(&rig, json!([{ "role": "user", "content": "q1" }, { "role": "assistant", "content": "a1" }]));
    let outcome = rig
        .run(&LlmInferenceNode, json!({ "provider": provider(&rig), "prompt": "q2", "historyFile": file.clone() }))
        .await
        .ok()?;
    let sent = rig.requests()[0].body.clone().expect("call body");
    let contents: Vec<_> = sent["messages"].as_array().expect("messages").iter().map(|m| m["content"].clone()).collect();
    assert_eq!(contents, vec![json!("q1"), json!("a1"), json!("q2")], "the file's conversation is sent, then the prompt");

    let before = StoredFile::from_value(&file)?;
    let after = StoredFile::from_value(outcome.output("historyFile")?)?;
    assert_eq!(after.key, before.key, "the same file, changed in place");
    assert_eq!(after.version, before.version + 1, "one write, one version");
    let written = conversation(&rig, outcome.output("historyFile")?)?;
    assert_eq!(written.len(), 4, "q1, a1, q2 and the new reply");
    assert_eq!((written[3]["role"].clone(), written[3]["content"].clone()), (json!("assistant"), json!("a2")));
    assert_eq!(after.size_bytes, rig.stored_bytes(&after.key)?.len() as u64, "the value carries the new size");
    assert_eq!(rig.stored_files(&StorageScope::Project)?.len(), 1, "one file that grows, not one per turn");
    let edits = rig.file_edits();
    assert_eq!(edits.len(), 1);
    assert!(edits[0].diff.lines().any(|l| l.starts_with('+') && l.contains("a2")), "{}", edits[0].diff);
    Ok(())
}

async fn history_file_refused(rig: FakeRig) -> WeftResult<()> {
    rig.output_type("response", WeftType::parse("String").expect("String parses"));
    let file = rig.store_file("notes.txt", "text/plain", b"hello".to_vec());
    let err = rig
        .run(&LlmInferenceNode, json!({ "provider": provider(&rig), "prompt": "hi", "historyFile": file }))
        .await
        .failure()?;
    assert!(err.contains("the file on historyFile is not a conversation") && err.contains("not JSON"), "{err}");
    let list = rig.store_file("list.json", "application/json", br#"[{"content": 3}]"#.to_vec());
    let err = rig
        .run(&LlmInferenceNode, json!({ "provider": provider(&rig), "prompt": "hi", "historyFile": list }))
        .await
        .failure()?;
    assert!(err.contains("the file on historyFile is not a conversation"), "{err}");
    assert!(rig.requests().is_empty(), "refused before the paid call");
    Ok(())
}

async fn completion(rig: FakeRig) -> WeftResult<()> {
    rig.output_type("response", WeftType::parse("String").expect("String parses"));
    rig.respond_raw(
        "POST",
        "/api/v1/chat/completions",
        200,
        "text/event-stream",
        sse(&["Hel", "lo"], Some("stop")),
    );
    let outcome = rig
        .run(
            &LlmInferenceNode,
            json!({ "provider": provider(&rig), "prompt": "say hello" }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["response"], json!("Hello"));
    assert!(!outcome.outputs.contains_key("toolCalls"), "no tools, no toolCalls pulse");
    // A one-shot call with nothing reading a conversation keeps none.
    assert!(!outcome.outputs.contains_key("historyFile"));
    assert!(rig.stored_files(&StorageScope::Execution)?.is_empty(), "no file for a one-shot call");

    let sent = rig.requests();
    assert_eq!(sent.len(), 1);
    let body = sent[0].body.as_ref().expect("call body");
    assert_eq!(body["model"], json!("test/model"));
    assert_eq!(body["messages"][0]["role"], json!("user"));
    Ok(())
}

/// With no file in and the `historyFile` output read, the call starts a
/// conversation: a new file holding its turn.
async fn starts_a_conversation(rig: FakeRig) -> WeftResult<()> {
    rig.output_type("response", WeftType::parse("String").expect("String parses"));
    rig.respond_raw("POST", "/api/v1/chat/completions", 200, "text/event-stream", sse(&["Hello"], Some("stop")));
    rig.wire_output("historyFile");
    let outcome = rig
        .run(&LlmInferenceNode, json!({ "provider": provider(&rig), "prompt": "say hello" }))
        .await
        .ok()?;
    let history = conversation(&rig, outcome.output("historyFile")?)?;
    let roles: Vec<_> = history.iter().map(|m| m["role"].clone()).collect();
    assert_eq!(roles, vec![json!("user"), json!("assistant")]);
    Ok(())
}

async fn parse_json(rig: FakeRig) -> WeftResult<()> {
    rig.output_type("response", WeftType::parse("JsonDict").expect("JsonDict parses"));
    // A reply that needs the repairer: prose around a fenced object
    // with single-quoted keys, nothing serde would accept as-is.
    rig.respond_raw(
        "POST",
        "/api/v1/chat/completions",
        200,
        "text/event-stream",
        sse(&["Here is the JSON: ```json {'answer': 42} ```"], Some("stop")),
    );
    let outcome = rig
        .run(
            &LlmInferenceNode,
            json!({ "provider": provider(&rig), "prompt": "json please", "parseJson": true }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["response"], json!({ "answer": 42 }));
    Ok(())
}

async fn truncated(rig: FakeRig) -> WeftResult<()> {
    rig.output_type("response", WeftType::parse("String").expect("String parses"));
    // A body that STOPS mid-stream: no finish reason and no `[DONE]`
    // (which the wire parser reads as a synthesized "stop"), i.e. the
    // connection dropped mid-generation.
    rig.respond_raw(
        "POST",
        "/api/v1/chat/completions",
        200,
        "text/event-stream",
        format!(
            "data: {}\n\n",
            json!({ "id": "gen-1", "choices": [{ "delta": { "content": "half an ans" } }] })
        )
        .into_bytes(),
    );
    let outcome = rig
        .run(
            &LlmInferenceNode,
            json!({ "provider": provider(&rig), "prompt": "say hello" }),
        )
        .await;
    let err = outcome.result.expect_err("no finish reason must refuse").to_string();
    assert!(err.contains("truncated"), "{err}");
    Ok(())
}

async fn media_parts(rig: FakeRig) -> WeftResult<()> {
    rig.output_type("response", WeftType::parse("String").expect("String parses"));
    rig.respond_raw(
        "POST",
        "/api/v1/chat/completions",
        200,
        "text/event-stream",
        sse(&["A cat."], Some("stop")),
    );
    let image = rig.store_file("cat.png", "image/png", b"PNGDATA".to_vec());
    rig.wire_output("historyFile");
    let outcome = rig
        .run(
            &LlmInferenceNode,
            json!({
                "provider": provider(&rig),
                "prompt": "what is on the picture?",
                "media": image,
            }),
        )
        .await
        .ok()?;

    // The SENT user message carries parts: the text, then the image as
    // a data: URL (the fake store serves no public links, so
    // externalize inlines the bytes).
    let sent = rig.requests();
    let body = sent[0].body.as_ref().expect("call body");
    let parts = body["messages"][0]["content"].as_array().expect("parts");
    assert_eq!(parts[0]["type"], json!("text"));
    assert_eq!(parts[1]["type"], json!("image_url"));
    let url = parts[1]["image_url"]["url"].as_str().expect("inline url");
    assert!(url.starts_with("data:image/png;base64,"), "not inlined: {url}");

    // The KEPT conversation holds the stored-file value in the media
    // slot, never the inlined bytes.
    let history = conversation(&rig, outcome.output("historyFile")?)?;
    let stored_slot = &history[0]["content"][1]["image_url"]["url"];
    assert!(
        stored_slot.get("__weft_image__").is_some(),
        "history must hold the stored value, got {stored_slot}"
    );
    Ok(())
}

async fn tool_round(rig: FakeRig) -> WeftResult<()> {
    rig.output_type("response", WeftType::parse("String").expect("String parses"));
    // A reply that ONLY calls a tool: no text, tool_call deltas, then
    // the tool_calls finish reason.
    let body = format!(
        "data: {}\n\ndata: {}\n\ndata: [DONE]\n\n",
        json!({ "id": "gen-1", "choices": [{ "delta": { "tool_calls": [
            { "index": 0, "id": "call_1", "type": "function",
              "function": { "name": "dog_name", "arguments": "{}" } }
        ]}}]}),
        json!({ "id": "gen-1", "choices": [{ "delta": {}, "finish_reason": "tool_calls" }] }),
    );
    rig.respond_raw("POST", "/api/v1/chat/completions", 200, "text/event-stream", body.into_bytes());
    rig.wire_output("historyFile");
    let outcome = rig
        .run(
            &LlmInferenceNode,
            json!({
                "provider": provider(&rig),
                "prompt": "what is my dog's name?",
                "tools": { "name": "dog_name", "description": "the dog's name",
                           "parameters": { "type": "object", "properties": {} } },
                "toolChoice": "required",
            }),
        )
        .await
        .ok()?;

    // The declaration and the choice rode the request.
    let sent = rig.requests();
    let body = sent[0].body.as_ref().expect("call body");
    assert_eq!(body["tools"][0]["function"]["name"], json!("dog_name"));
    assert_eq!(body["tool_choice"], json!("required"));

    // The model's call surfaced on toolCalls (a text-less reply with
    // calls has substance), and the kept conversation closes with the
    // assistant's tool-call message for the role-tool append to chain
    // onto.
    // ToolCall is weft's flat shape ({id, name, arguments}), not the
    // provider's nested `function` envelope.
    let calls = outcome.outputs["toolCalls"].as_array().expect("toolCalls").clone();
    assert_eq!(calls[0]["id"], json!("call_1"));
    assert_eq!(calls[0]["name"], json!("dog_name"));
    let history = conversation(&rig, outcome.output("historyFile")?)?;
    let last = history.last().expect("assistant message");
    assert_eq!(last["role"], json!("assistant"));
    assert_eq!(last["tool_calls"][0]["id"], json!("call_1"));
    Ok(())
}

async fn params_shape(rig: FakeRig) -> WeftResult<()> {
    rig.output_type("response", WeftType::parse("String").expect("String parses"));
    rig.respond_raw(
        "POST",
        "/api/v1/chat/completions",
        200,
        "text/event-stream",
        sse(&["ok"], Some("stop")),
    );
    rig.run(
        &LlmInferenceNode,
        json!({
            "provider": provider(&rig),
            "prompt": "hi",
            "params": {
                "systemPrompt": "Be brief.",
                "temperature": 0.25,
                "maxTokens": 64,
                "reasoning": true,
                "reasoningEffort": "low",
            },
        }),
    )
    .await
    .ok()?;

    let sent = rig.requests();
    let body = sent[0].body.as_ref().expect("call body");
    assert_eq!(body["temperature"], json!(0.25));
    // The OpenAI-wire limit key (max_completion_tokens); Anthropic's
    // native wire writes max_tokens instead, asserted in its own test.
    assert_eq!(body["max_completion_tokens"], json!(64));
    assert_eq!(body["reasoning"]["effort"], json!("low"));
    // The system prompt seeded the conversation as its first message.
    assert_eq!(body["messages"][0]["role"], json!("system"));
    assert_eq!(body["messages"][0]["content"], json!("Be brief."));
    assert_eq!(body["messages"][1]["role"], json!("user"));
    Ok(())
}

async fn history_system_wins(rig: FakeRig) -> WeftResult<()> {
    rig.output_type("response", WeftType::parse("String").expect("String parses"));
    rig.respond_raw(
        "POST",
        "/api/v1/chat/completions",
        200,
        "text/event-stream",
        sse(&["ok"], Some("stop")),
    );
    // The conversation already opens with its own system message; the
    // params' prompt must NOT override or duplicate it.
    let file = conversation_file(&rig, json!([
        { "role": "system", "content": "You are Rex." },
        { "role": "user", "content": "hi" },
        { "role": "assistant", "content": "hello" },
    ]));
    let outcome = rig.run(
        &LlmInferenceNode,
        json!({
            "provider": provider(&rig),
            "prompt": "hi again",
            "historyFile": file,
            "params": { "systemPrompt": "You are someone else." },
        }),
    )
    .await
    .ok()?;
    let kept = conversation(&rig, outcome.output("historyFile")?)?;
    assert_eq!(kept.iter().filter(|m| m["role"] == json!("system")).count(), 1, "kept as it was: {kept:?}");

    let sent = rig.requests();
    let body = sent[0].body.as_ref().expect("call body");
    let messages = body["messages"].as_array().expect("messages");
    assert_eq!(messages[0]["content"], json!("You are Rex."));
    let systems = messages.iter().filter(|m| m["role"] == json!("system")).count();
    assert_eq!(systems, 1, "exactly the conversation's own system message");
    Ok(())
}

async fn anthropic_wire(rig: FakeRig) -> WeftResult<()> {
    rig.output_type("response", WeftType::parse("String").expect("String parses"));
    // Anthropic's own stream framing: typed events, text deltas, the
    // stop reason on message_delta.
    let body = format!(
        "data: {}\n\ndata: {}\n\ndata: {}\n\n",
        json!({ "type": "content_block_delta", "delta": { "type": "text_delta", "text": "Bonjour" } }),
        json!({ "type": "message_delta", "delta": { "stop_reason": "end_turn" },
                "usage": { "output_tokens": 2 } }),
        json!({ "type": "message_stop" }),
    );
    rig.respond_raw("POST", "/v1/messages", 200, "text/event-stream", body.into_bytes());
    let outcome = rig
        .run(
            &LlmInferenceNode,
            json!({
                "provider": { "kind": "anthropic", "model": "claude-x",
                              "account": rig.access("anthropic") },
                "prompt": "greet me",
                "params": { "systemPrompt": "Answer in French.", "maxTokens": 32 },
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["response"], json!("Bonjour"));

    // The native envelope: /v1/messages, the system prompt hoisted to
    // the top-level `system` field, max_tokens present (Anthropic
    // rejects a request without it).
    let sent = rig.requests();
    assert_eq!(sent[0].path, "/v1/messages");
    let body = sent[0].body.as_ref().expect("call body");
    // `autoCache` (on by default) marks the persona, which the native
    // wire writes as a text block carrying the cache marker.
    assert_eq!(body["system"][0]["text"], json!("Answer in French."));
    assert_eq!(body["system"][0]["cache_control"]["type"], json!("ephemeral"));
    assert_eq!(body["max_tokens"], json!(32));
    assert_eq!(body["messages"][0]["role"], json!("user"));
    Ok(())
}

async fn custom_no_account(rig: FakeRig) -> WeftResult<()> {
    rig.output_type("response", WeftType::parse("String").expect("String parses"));
    rig.respond_raw(
        "POST",
        "/v1/chat/completions",
        200,
        "text/event-stream",
        sse(&["local"], Some("stop")),
    );
    // No account on a custom provider: the call rides the plain client
    // (a local or unauthenticated OpenAI-compatible server).
    let outcome = rig
        .run(
            &LlmInferenceNode,
            json!({
                "provider": { "kind": "custom", "baseUrl": "http://llm.internal/v1",
                              "model": "local-model" },
                "prompt": "hi",
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["response"], json!("local"));
    let sent = rig.requests();
    assert_eq!(sent[0].path, "/v1/chat/completions");
    assert_eq!(sent[0].body.as_ref().expect("body")["model"], json!("local-model"));
    Ok(())
}

async fn routing_pins(rig: FakeRig) -> WeftResult<()> {
    rig.output_type("response", WeftType::parse("String").expect("String parses"));
    rig.respond_raw(
        "POST",
        "/api/v1/chat/completions",
        200,
        "text/event-stream",
        sse(&["ok"], Some("stop")),
    );
    let mut provider = provider(&rig);
    provider["servingProvider"] = json!("groq");
    provider["providerFallbacks"] = json!(false);
    rig.run(&LlmInferenceNode, json!({ "provider": provider, "prompt": "hi" }))
        .await
        .ok()?;
    let sent = rig.requests();
    let body = sent[0].body.as_ref().expect("call body");
    assert_eq!(body["provider"]["order"], json!(["groq"]));
    assert_eq!(body["provider"]["allow_fallbacks"], json!(false));
    Ok(())
}

async fn live_completion(rig: LiveRig) -> WeftResult<()> {
    let outcome = rig
        .run(
            &LlmInferenceNode,
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

/// The provider refusing the call, the failure both `error` tests read.
fn refuse_the_call(rig: &FakeRig) {
    rig.output_type("response", WeftType::parse("String").expect("String parses"));
    rig.respond_raw(
        "POST",
        "/api/v1/chat/completions",
        400,
        "application/json",
        br#"{"error":{"message":"the prompt was refused"}}"#.to_vec(),
    );
}

async fn refused_unwired(rig: FakeRig) -> WeftResult<()> {
    refuse_the_call(&rig);
    let err = rig
        .run(&LlmInferenceNode, json!({ "provider": provider(&rig), "prompt": "hi" }))
        .await
        .failure()?;
    assert!(err.contains("the prompt was refused"), "{err}");
    Ok(())
}

async fn refused_wired(rig: FakeRig) -> WeftResult<()> {
    refuse_the_call(&rig);
    rig.wire_output("error");
    let outcome = rig
        .run(&LlmInferenceNode, json!({ "provider": provider(&rig), "prompt": "hi" }))
        .await
        .ok()?;
    let error = outcome.output("error")?.as_str().expect("error is a string").to_string();
    assert!(error.contains("the prompt was refused"), "{error}");
    for port in ["response", "historyFile", "toolCalls"] {
        assert!(!outcome.outputs.contains_key(port), "a caught failure emits nothing on {port}");
    }
    Ok(())
}

async fn mistake_wired(rig: FakeRig) -> WeftResult<()> {
    rig.wire_output("error");
    let no_connection = json!({ "kind": "openrouter", "model": "openai/gpt-test" });
    let err = rig
        .run(&LlmInferenceNode, json!({ "provider": no_connection, "prompt": "hi" }))
        .await
        .failure()?;
    assert!(err.starts_with("input error") && err.contains("carries no connection"), "{err}");
    assert!(rig.requests().is_empty(), "a program mistake refuses before anything is sent");
    Ok(())
}
