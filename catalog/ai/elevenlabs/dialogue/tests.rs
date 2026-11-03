//! ElevenLabsDialogue self-tests: the request a script makes, the
//! settings that travel only when set, the stored audio, and how a
//! refusal from ElevenLabs comes out.

use serde_json::{json, Value};

use weft::{FakeRig, LiveRig, NodeTest, WeftResult};

use crate::testing::emitted_file;

use super::ElevenLabsDialogueNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("a_script_goes_as_lines_with_their_voices", script),
        NodeTest::fake("every_setting_reaches_the_request", every_setting),
        NodeTest::fake("the_shared_speech_settings_and_the_request_id", shared_settings),
        NodeTest::fake("a_refused_dialogue_quotes_elevenlabs", refused),
        NodeTest::fake("a_refused_dialogue_comes_out_on_error_when_it_is_wired", refused_caught),
        NodeTest::live("one_real_two_line_dialogue_on_v4", "elevenlabs", live_dialogue),
    ]
}

fn inputs(rig: &FakeRig, extra: Value) -> Value {
    let mut base = json!({
        "account": rig.access("elevenlabs"),
        "lines": [
            { "text": "[curious] Who is there?", "voice": "v1" },
            { "text": "Just me.", "voice": "v2" },
        ],
    });
    for (k, v) in extra.as_object().expect("extra inputs are an object") {
        base[k] = v.clone();
    }
    base
}

fn sent_body(rig: &FakeRig) -> Value {
    rig.requests()
        .into_iter()
        .find(|r| r.path == "/v1/text-to-dialogue")
        .and_then(|r| r.body)
        .expect("the node sent the script")
}

async fn script(rig: FakeRig) -> WeftResult<()> {
    rig.respond_raw("POST", "/v1/text-to-dialogue", 200, "audio/mpeg", b"talk".to_vec());
    let outcome = rig.run(&ElevenLabsDialogueNode, inputs(&rig, json!({}))).await.ok()?;
    assert_eq!(
        sent_body(&rig),
        json!({
            "inputs": [
                { "text": "[curious] Who is there?", "voice_id": "v1" },
                { "text": "Just me.", "voice_id": "v2" },
            ],
            "model_id": "eleven_v4",
        }),
        "a setting nobody gave never travels"
    );
    let sent = rig.requests().into_iter().find(|r| r.path == "/v1/text-to-dialogue").expect("a request");
    assert_eq!(sent.query.as_deref(), Some("output_format=mp3_44100_128"));
    let (bytes, filename, mime) = emitted_file(&rig, &outcome.outputs, "file")?;
    assert_eq!(bytes, b"talk");
    assert_eq!(filename, "dialogue.mp3");
    assert_eq!(mime, "audio/mpeg");
    assert_eq!(outcome.outputs["mimeType"], json!("audio/mpeg"));
    Ok(())
}

async fn every_setting(rig: FakeRig) -> WeftResult<()> {
    rig.respond_raw("POST", "/v1/text-to-dialogue", 200, "audio/ogg", b"ogg".to_vec());
    let outcome = rig
        .run(
            &ElevenLabsDialogueNode,
            inputs(
                &rig,
                json!({
                    "model": "eleven_v3",
                    "outputFormat": "opus_48000_64",
                    "languageCode": "fr",
                    "stability": 0.3,
                    "similarity": 0.9,
                    "textNormalization": "on",
                    "previousText": "They knocked twice.",
                    "nextText": "Then the door opened.",
                    "seed": 7,
                }),
            ),
        )
        .await
        .ok()?;
    let body = sent_body(&rig);
    assert_eq!(body["model_id"], json!("eleven_v3"));
    assert_eq!(body["settings"], json!({ "stability": 0.3, "similarity": 0.9 }));
    assert_eq!(body["language_code"], json!("fr"));
    assert_eq!(body["apply_text_normalization"], json!("on"));
    assert_eq!(body["previous_text"], json!("They knocked twice."));
    assert_eq!(body["future_text"], json!("Then the door opened."));
    assert_eq!(body["seed"], json!(7));
    let (_, filename, mime) = emitted_file(&rig, &outcome.outputs, "file")?;
    assert_eq!(filename, "dialogue.ogg");
    assert_eq!(mime, "audio/ogg; codecs=opus", "the type the requested format really is");
    Ok(())
}

async fn shared_settings(rig: FakeRig) -> WeftResult<()> {
    rig.respond_with_headers(
        "POST",
        "/v1/text-to-dialogue?output_format=mp3_44100_128&enable_logging=false",
        200,
        "audio/mpeg",
        &[("request-id", "req-7")],
        b"talk".to_vec(),
    );
    let outcome = rig
        .run(
            &ElevenLabsDialogueNode,
            inputs(
                &rig,
                json!({
                    "pronunciationDictionaries": [{ "id": "dict-a" }],
                    "usePvcAsIvc": true,
                    "environment": "small_room",
                    "backgroundNoise": "keyboard",
                    "enableLogging": false,
                }),
            ),
        )
        .await
        .ok()?;
    let body = sent_body(&rig);
    assert_eq!(body["pronunciation_dictionary_locators"], json!([{ "pronunciation_dictionary_id": "dict-a" }]));
    assert_eq!(body["use_pvc_as_ivc"], json!(true));
    assert_eq!(body["audio_effects"], json!({ "environment_id": "small_room", "background_noise_id": "keyboard" }));
    assert_eq!(outcome.outputs["requestId"], json!("req-7"));
    Ok(())
}

fn refuse(rig: &FakeRig) {
    rig.respond_status(
        "POST",
        "/v1/text-to-dialogue",
        400,
        json!({ "detail": { "type": "validation_error", "code": "too_many_voices", "message": "A dialogue takes at most 10 different voices." } }),
    );
}

async fn refused(rig: FakeRig) -> WeftResult<()> {
    refuse(&rig);
    let err = rig.run(&ElevenLabsDialogueNode, inputs(&rig, json!({}))).await.failure()?;
    assert!(err.contains("400"), "{err}");
    assert!(err.contains("at most 10 different voices"), "{err}");
    assert!(!err.contains("too_many_voices"), "ElevenLabs's message, not its whole error envelope: {err}");
    Ok(())
}

async fn refused_caught(rig: FakeRig) -> WeftResult<()> {
    refuse(&rig);
    rig.wire_output("_error");
    let outcome = rig.run(&ElevenLabsDialogueNode, inputs(&rig, json!({}))).await.ok()?;
    let error = outcome.output("_error")?.as_str().expect("error is a string").to_string();
    assert!(error.contains("at most 10 different voices"), "{error}");
    assert!(!outcome.outputs.contains_key("file"), "a caught failure emits no audio");
    Ok(())
}

/// Two short lines between two stock voices on Eleven v4: 21
/// characters, billed per character at the model's rate whatever the
/// format. Nothing stays on the account.
async fn live_dialogue(rig: LiveRig) -> WeftResult<()> {
    let conn = rig.connect().await?;
    let voices = weft::access::client::get_json(
        conn.client(),
        &format!("{}/voices", crate::elevenlabs::API),
        "list the ElevenLabs voices",
    )
    .await?;
    let ids: Vec<&str> = voices["voices"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|v| v["voice_id"].as_str())
        .take(2)
        .collect();
    assert_eq!(ids.len(), 2, "the account lists two stock voices");
    let outcome = rig
        .run(
            &ElevenLabsDialogueNode,
            json!({
                "account": rig.access("elevenlabs"),
                "lines": [
                    { "text": "Who is there?", "voice": ids[0] },
                    { "text": "Just me.", "voice": ids[1] },
                ],
                "model": "eleven_v4",
                "outputFormat": "mp3_44100_128",
            }),
        )
        .await
        .ok()?;
    let size = outcome.output("sizeBytes")?.as_f64().unwrap_or(0.0);
    assert!(size > 2_000.0, "two spoken lines are real audio: {size}");
    assert!(outcome.output("requestId")?.as_str().is_some_and(|id| !id.is_empty()), "the dialogue route answers a request id");
    Ok(())
}
