//! LlmInference self-tests: the buffered call against canned SSE
//! replies (fake), covering the whole assemble surface (media
//! attachments, tools + toolChoice, params + system-prompt seeding,
//! the native Anthropic wire, a connection-less custom endpoint,
//! OpenRouter routing pins), and one real completion (live).

use serde_json::json;

use weft::{FakeRig, LiveRig, NodeTest, WeftResult, WeftType};

use super::LlmInferenceNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("a_completion_answers_text_and_history", completion),
        NodeTest::fake("parse_json_repairs_and_parses_the_reply", parse_json),
        NodeTest::fake("a_stream_without_finish_reason_is_truncated", truncated),
        NodeTest::fake("media_rides_the_user_message_as_image_parts", media_parts),
        NodeTest::fake("tools_and_tool_choice_ride_and_calls_surface", tool_round),
        NodeTest::fake("params_shape_the_request_and_seed_the_system_prompt", params_shape),
        NodeTest::fake("a_wired_history_keeps_its_own_system_prompt", history_system_wins),
        NodeTest::fake("anthropic_speaks_its_native_wire", anthropic_wire),
        NodeTest::fake("a_custom_endpoint_runs_without_a_connection", custom_no_account),
        NodeTest::fake("openrouter_routing_pins_ride_the_provider_key", routing_pins),
        NodeTest::live("one_real_completion", "openrouter", live_completion),
    ]
}

fn provider(rig: &FakeRig) -> serde_json::Value {
    json!({ "kind": "openrouter", "model": "test/model", "account": rig.access("openrouter") })
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
    let history = outcome.outputs["history"].as_array().expect("history list").clone();
    assert_eq!(history.len(), 2, "user + assistant");
    assert_eq!(history[0]["role"], json!("user"));
    assert_eq!(history[1]["role"], json!("assistant"));
    assert!(!outcome.outputs.contains_key("toolCalls"), "no tools, no toolCalls pulse");

    let sent = rig.requests();
    assert_eq!(sent.len(), 1);
    let body = sent[0].body.as_ref().expect("call body");
    assert_eq!(body["model"], json!("test/model"));
    assert_eq!(body["messages"][0]["role"], json!("user"));
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

    // The EMITTED history keeps the stored-file value in the media
    // slot, never the inlined bytes.
    let history = outcome.outputs["history"].as_array().expect("history").clone();
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
    // calls has substance), and the emitted history closes with the
    // assistant's tool-call message for the role-tool append to chain
    // onto.
    // ToolCall is weft's flat shape ({id, name, arguments}), not the
    // provider's nested `function` envelope.
    let calls = outcome.outputs["toolCalls"].as_array().expect("toolCalls").clone();
    assert_eq!(calls[0]["id"], json!("call_1"));
    assert_eq!(calls[0]["name"], json!("dog_name"));
    let history = outcome.outputs["history"].as_array().expect("history").clone();
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
    // The wired history already opens with its own system message; the
    // params' prompt must NOT override or duplicate it.
    rig.run(
        &LlmInferenceNode,
        json!({
            "provider": provider(&rig),
            "prompt": "hi again",
            "history": [
                { "role": "system", "content": "You are Rex." },
                { "role": "user", "content": "hi" },
                { "role": "assistant", "content": "hello" },
            ],
            "params": { "systemPrompt": "You are someone else." },
        }),
    )
    .await
    .ok()?;

    let sent = rig.requests();
    let body = sent[0].body.as_ref().expect("call body");
    let messages = body["messages"].as_array().expect("messages");
    assert_eq!(messages[0]["content"], json!("You are Rex."));
    let systems = messages.iter().filter(|m| m["role"] == json!("system")).count();
    assert_eq!(systems, 1, "exactly the history's own system message");
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
    assert_eq!(body["system"], json!("Answer in French."));
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
