//! ElevenLabsCreateAgent self-tests: every typed input reaching the
//! create request, the JSON inputs merging under the typed ones, each
//! refusal, and one real agent made, read back and deleted.

use serde_json::{json, Value};

use weft::{FakeRig, LiveRig, NodeTest, WeftResult};

use super::ElevenLabsCreateAgentNode;

const CREATE: &str = "/v1/convai/agents/create";

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("creates_and_emits_the_agent_id", creates),
        NodeTest::fake("only_what_is_set_is_sent", only_what_is_set),
        NodeTest::fake("every_typed_input_reaches_the_request", every_input),
        NodeTest::fake("the_json_inputs_merge_under_the_typed_ones", json_inputs_merge),
        NodeTest::fake("settings_elevenlabs_would_refuse_are_refused_before_anything_is_sent", refused),
        NodeTest::live("one_real_agent_created_and_deleted", "elevenlabs", live_create),
    ]
}

/// The inputs of a minimal agent, plus `extra`.
fn inputs(rig: &FakeRig, extra: Value) -> Value {
    let mut base = json!({
        "account": rig.access("elevenlabs"),
        "name": "Booking bot",
        "prompt": "You book tables.",
    });
    for (k, v) in extra.as_object().expect("extra inputs are an object") {
        base[k] = v.clone();
    }
    base
}

/// The JSON body of the one create the node sent.
fn sent(rig: &FakeRig) -> Value {
    let requests = rig.requests();
    assert_eq!(requests.len(), 1, "one create");
    assert_eq!(requests[0].path, CREATE);
    requests[0].body.clone().expect("json payload")
}

/// The built-in tool a system tool name becomes.
fn system_tool(name: &str) -> Value {
    json!({ "type": "system", "name": name, "params": { "system_tool_type": name } })
}

/// One real agent made with the settings ElevenLabs is most likely to
/// reshape (a built-in tool, a per-call permission, a data collection
/// item and a success criterion), read back by id, then DELETED so
/// repeated runs never pile agents onto the account. Making an agent
/// costs nothing.
async fn live_create(rig: LiveRig) -> WeftResult<()> {
    let conn = rig.connect().await?;
    let voice = crate::testing::first_voice_id(&conn).await?;
    let outcome = rig
        .run(
            &ElevenLabsCreateAgentNode,
            json!({
                "account": rig.access("elevenlabs"),
                "name": "weft-node-tests",
                "prompt": "You are a test agent; say you are a test and end the call.",
                "firstMessage": "This is a weft node test.",
                "voice": voice,
                "language": "en",
                "ttsModel": "eleven_flash_v2",
                "maxDurationSeconds": 60,
                "allowOverrides": ["agent.first_message"],
                "dataCollection": { "caller_name": "The caller's name, if they gave it" },
                "evaluationCriteria": { "greeted": "The agent greeted the caller" },
            }),
        )
        .await
        .ok()?;
    let agent = outcome.output("agentId")?.as_str().expect("agent id").to_string();
    weft::with_cleanup(
        || async {
            let read = weft::access::client::get_json(
                conn.client(),
                &format!("{}/convai/agents/{agent}", crate::elevenlabs::API),
                "read the new agent back",
            )
            .await?;
            assert_eq!(read["name"].as_str(), Some("weft-node-tests"), "{read}");
            let config = &read["conversation_config"];
            assert_eq!(config["tts"]["model_id"], json!("eleven_flash_v2"), "{read}");
            assert_eq!(config["conversation"]["max_duration_seconds"], json!(60), "{read}");
            assert!(config["agent"]["prompt"]["built_in_tools"]["end_call"].is_object(), "the agent can hang up: {read}");
            let platform = &read["platform_settings"];
            assert_eq!(
                platform["overrides"]["conversation_config_override"]["agent"]["first_message"],
                json!(true),
                "{read}"
            );
            assert!(platform["data_collection"]["caller_name"].is_object(), "{read}");
            assert!(
                platform["evaluation"]["criteria"]
                    .as_array()
                    .is_some_and(|c| c.iter().any(|c| c["id"] == json!("greeted"))),
                "{read}"
            );
            Ok(())
        },
        || async { crate::testing::delete_agent(&conn, &agent).await },
    )
    .await
}

async fn creates(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", CREATE, json!({ "agent_id": "agent-new" }));
    let outcome = rig
        .run(
            &ElevenLabsCreateAgentNode,
            inputs(
                &rig,
                json!({
                    "firstMessage": "Hi, this is the booking line!",
                    "voice": "voice-1",
                    "language": "fr",
                }),
            ),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["agentId"], json!("agent-new"));

    let body = sent(&rig);
    assert_eq!(body["name"], json!("Booking bot"));
    let config = &body["conversation_config"];
    assert_eq!(config["agent"]["prompt"]["prompt"], json!("You book tables."));
    assert_eq!(config["agent"]["first_message"], json!("Hi, this is the booking line!"));
    assert_eq!(config["agent"]["language"], json!("fr"));
    assert_eq!(config["tts"]["voice_id"], json!("voice-1"));
    Ok(())
}

/// Nothing the program left unset is sent, so ElevenLabs applies its
/// own defaults; the one default weft adds is the tool that lets the
/// agent hang up.
async fn only_what_is_set(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", CREATE, json!({ "agent_id": "agent-new" }));
    rig.run(&ElevenLabsCreateAgentNode, inputs(&rig, json!({}))).await.ok()?;
    assert_eq!(
        sent(&rig),
        json!({
            "name": "Booking bot",
            "conversation_config": {
                "agent": {
                    "prompt": {
                        "prompt": "You book tables.",
                        "built_in_tools": { "end_call": system_tool("end_call") },
                    },
                },
            },
        })
    );
    Ok(())
}

async fn every_input(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", CREATE, json!({ "agent_id": "agent-new" }));
    rig.run(
        &ElevenLabsCreateAgentNode,
        inputs(
            &rig,
            json!({
                "firstMessage": "Hello!",
                "voice": "voice-1",
                "language": "de",
                "llm": "claude-sonnet-4-5",
                "temperature": 0.3,
                "maxTokens": 400,
                "reasoningEffort": "low",
                "thinkingBudget": 0,
                "timezone": "Europe/Berlin",
                "toolIds": ["tool-1", "tool-2"],
                "mcpServerIds": ["mcp-1"],
                "builtInTools": ["end_call", "voicemail_detection"],
                "knowledgeBase": [
                    { "id": "doc-1", "name": "Menu", "type": "file" },
                    { "id": "doc-2", "name": "Site", "type": "url", "usageMode": "prompt" },
                ],
                "ttsModel": "eleven_flash_v2_5",
                "stability": 0.4,
                "similarityBoost": 0.9,
                "speed": 1.1,
                "outputAudioFormat": "ulaw_8000",
                "asrKeywords": ["Weft", "Quentin"],
                "turnTimeout": 5,
                "silenceEndCallTimeout": 20,
                "maxDurationSeconds": 300,
                "dynamicVariables": { "customer_name": "Ada" },
                "dataCollection": {
                    "customer_name": "The caller's full name",
                    "party_size": { "type": "integer", "description": "How many people" },
                    "seating": { "type": "string", "description": "Where they sit", "enum": ["inside", "terrace"] },
                },
                "evaluationCriteria": { "booked": "The caller booked a table" },
                "allowOverrides": ["agent.first_message", "tts.voice_id", "agent.prompt.prompt", "procedure_ids"],
                "tags": ["booking", "fr"],
            }),
        ),
    )
    .await
    .ok()?;
    assert_eq!(
        sent(&rig),
        json!({
            "name": "Booking bot",
            "tags": ["booking", "fr"],
            "conversation_config": {
                "agent": {
                    "first_message": "Hello!",
                    "language": "de",
                    "dynamic_variables": { "dynamic_variable_placeholders": { "customer_name": "Ada" } },
                    "prompt": {
                        "prompt": "You book tables.",
                        "llm": "claude-sonnet-4-5",
                        "temperature": 0.3,
                        "max_tokens": 400,
                        "reasoning_effort": "low",
                        "thinking_budget": 0,
                        "timezone": "Europe/Berlin",
                        "tool_ids": ["tool-1", "tool-2"],
                        "mcp_server_ids": ["mcp-1"],
                        "built_in_tools": {
                            "end_call": system_tool("end_call"),
                            "voicemail_detection": system_tool("voicemail_detection"),
                        },
                        "knowledge_base": [
                            { "type": "file", "name": "Menu", "id": "doc-1" },
                            { "type": "url", "name": "Site", "id": "doc-2", "usage_mode": "prompt" },
                        ],
                    },
                },
                "tts": {
                    "voice_id": "voice-1",
                    "model_id": "eleven_flash_v2_5",
                    "stability": 0.4,
                    "similarity_boost": 0.9,
                    "speed": 1.1,
                    "agent_output_audio_format": "ulaw_8000",
                },
                "asr": { "keywords": ["Weft", "Quentin"] },
                "turn": { "turn_timeout": 5.0, "silence_end_call_timeout": 20.0 },
                "conversation": { "max_duration_seconds": 300 },
            },
            "platform_settings": {
                "data_collection": {
                    "customer_name": { "type": "string", "description": "The caller's full name" },
                    "party_size": { "type": "integer", "description": "How many people" },
                    "seating": { "type": "string", "description": "Where they sit", "enum": ["inside", "terrace"] },
                },
                "evaluation": {
                    "criteria": [
                        { "id": "booked", "name": "booked", "conversation_goal_prompt": "The caller booked a table" },
                    ],
                },
                "overrides": {
                    "conversation_config_override": {
                        "agent": { "first_message": true, "prompt": { "prompt": true } },
                        "tts": { "voice_id": true },
                    },
                    "enable_procedure_ids_from_client": true,
                },
            },
        })
    );
    Ok(())
}

/// Each JSON input reaches ElevenLabs whole, and the typed inputs go on
/// top: objects merge key by key, and the typed value wins where both
/// set the same key (a list included).
async fn json_inputs_merge(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", CREATE, json!({ "agent_id": "agent-new" }));
    let transfer = json!({
        "type": "system",
        "name": "transfer_to_number",
        "params": {
            "system_tool_type": "transfer_to_number",
            "transfers": [{ "transfer_destination": { "type": "phone", "phone_number": "+15550001111" } }],
        },
    });
    let workflow = json!({ "nodes": {}, "edges": {}, "prevent_subagent_loops": true });
    rig.run(
        &ElevenLabsCreateAgentNode,
        inputs(
            &rig,
            json!({
                "llm": "custom-llm",
                "stability": 0.6,
                "asrKeywords": ["Weft"],
                "dataCollection": { "customer_name": "The caller's name" },
                "evaluationCriteria": { "booked": "The caller booked a table" },
                "allowOverrides": ["agent.first_message"],
                "conversationConfig": {
                    "agent": {
                        "disable_first_message_interruptions": true,
                        "prompt": {
                            "prompt": "Written in the JSON, overwritten by the typed prompt.",
                            "custom_llm": { "url": "https://llm.example.com/v1" },
                            "built_in_tools": { "transfer_to_number": transfer },
                        },
                    },
                    "tts": { "stability": 0.1, "expressive_mode": false },
                    "asr": { "keywords": ["Old"] },
                    "turn": { "turn_eagerness": "eager" },
                },
                "platformSettings": {
                    "privacy": { "record_voice": false },
                    "data_collection": {
                        "customer_name": { "type": "boolean", "description": "Overwritten" },
                        "callback": { "type": "boolean", "description": "Kept" },
                    },
                    "evaluation": { "criteria": [{ "id": "old", "name": "old", "conversation_goal_prompt": "Replaced" }] },
                    "overrides": { "conversation_config_override": { "tts": { "speed": true } } },
                },
                "workflow": workflow,
            }),
        ),
    )
    .await
    .ok()?;
    let body = sent(&rig);
    let config = &body["conversation_config"];
    assert_eq!(config["agent"]["disable_first_message_interruptions"], json!(true), "a key only the JSON sets is kept");
    assert_eq!(config["agent"]["prompt"]["prompt"], json!("You book tables."), "the typed prompt wins");
    assert_eq!(config["agent"]["prompt"]["llm"], json!("custom-llm"));
    assert_eq!(
        config["agent"]["prompt"]["custom_llm"],
        json!({ "url": "https://llm.example.com/v1" }),
        "a custom model with its definition goes through"
    );
    assert_eq!(
        config["agent"]["prompt"]["built_in_tools"],
        json!({ "transfer_to_number": transfer, "end_call": system_tool("end_call") }),
        "the JSON's tool and the typed one sit side by side"
    );
    assert_eq!(config["tts"], json!({ "stability": 0.6, "expressive_mode": false }));
    assert_eq!(config["asr"]["keywords"], json!(["Weft"]), "a typed list replaces the JSON's");
    assert_eq!(config["turn"], json!({ "turn_eagerness": "eager" }));

    let platform = &body["platform_settings"];
    assert_eq!(platform["privacy"], json!({ "record_voice": false }));
    assert_eq!(
        platform["data_collection"],
        json!({
            "customer_name": { "type": "string", "description": "The caller's name" },
            "callback": { "type": "boolean", "description": "Kept" },
        })
    );
    assert_eq!(
        platform["evaluation"]["criteria"],
        json!([{ "id": "booked", "name": "booked", "conversation_goal_prompt": "The caller booked a table" }])
    );
    assert_eq!(
        platform["overrides"],
        json!({ "conversation_config_override": { "tts": { "speed": true }, "agent": { "first_message": true } } })
    );
    assert_eq!(body["workflow"], workflow);
    Ok(())
}

async fn refused(rig: FakeRig) -> WeftResult<()> {
    rig.wire_output("_error");
    // What the rules cannot see: a dict's values, and what the JSON
    // inputs set. The MCP server count and the knowledge base's closed
    // values are metadata rules, which this rig does not run.
    for (extra, says) in [
        (
            json!({ "dataCollection": { "party_size": { "type": "integer", "description": "How many", "enum": ["2"] } } }),
            "only on a `string` item",
        ),
        (json!({ "dataCollection": { "day": { "type": "date", "description": "Which day" } } }), "has type 'date'"),
        (json!({ "evaluationCriteria": { "long": "a".repeat(2001) } }), "2001 characters"),
        (json!({ "llm": "custom-llm" }), "needs that model's definition"),
        (
            json!({ "conversationConfig": { "agent": { "prompt": { "llm": "custom-llm" } } } }),
            "needs that model's definition",
        ),
        (json!({ "language": "en", "ttsModel": "eleven_flash_v2_5" }), "an English agent only on a turbo voice model"),
        (json!({ "ttsModel": "eleven_multilingual_v2" }), "not 'eleven_multilingual_v2'"),
    ] {
        let err = rig.run(&ElevenLabsCreateAgentNode, inputs(&rig, extra.clone())).await.failure()?;
        assert!(err.starts_with("input error"), "a mistake in the program fails the run even with `_error` wired: {extra}: {err}");
        assert!(err.contains(says), "{extra}: {err}");
    }
    assert!(rig.requests().is_empty(), "nothing is sent");
    Ok(())
}
