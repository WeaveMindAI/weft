//! ElevenLabsSpeakFile self-tests: the request the node builds and the
//! stored-file quartet it emits, against canned audio bytes.

use serde_json::{json, Value};

use weft::{FakeRig, LiveRig, NodeTest, WeftResult};

use super::super::elevenlabs::audio_file_type;
use super::ElevenLabsSpeakFileNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::basic("every_declared_output_format_has_a_file_type", file_types),
        NodeTest::fake("speaks_and_stores_the_audio", speaks),
        NodeTest::fake("voice_settings_ride_only_when_set", settings_optional),
        NodeTest::fake("a_voice_id_can_only_ever_name_a_voice", voice_id_encoded),
        NodeTest::fake("every_elevenlabs_setting_reaches_the_request", every_setting),
        NodeTest::fake("the_request_id_comes_out_for_stitching", request_id),
        NodeTest::fake("zero_retention_rides_the_query", zero_retention),
        NodeTest::live("one_real_short_speech", "elevenlabs", live_speech),
        NodeTest::live("one_real_short_speech_on_v4", "elevenlabs", live_speech_v4),
        NodeTest::live("two_real_stitched_takes_with_a_filter", "elevenlabs", live_stitched),
    ]
}

/// A typed voice id that looks like a path stays one path segment, so
/// it cannot reach another ElevenLabs route.
async fn voice_id_encoded(rig: FakeRig) -> WeftResult<()> {
    rig.respond_raw(
        "POST",
        "/v1/text-to-speech/..%2Fuser?output_format=mp3_44100_128",
        200,
        "audio/mpeg",
        b"mp3".to_vec(),
    );
    rig.run(
        &ElevenLabsSpeakFileNode,
        json!({
            "account": rig.access("elevenlabs"),
            "text": "hi",
            "voice": "../user",
            "model": "eleven_flash_v2_5",
            "outputFormat": "mp3_44100_128",
        }),
    )
    .await
    .ok()?;
    assert_eq!(rig.requests()[0].path, "/v1/text-to-speech/..%2Fuser");
    Ok(())
}

/// Eleven v4 on the plain text to speech route, which ElevenLabs's
/// model list marks it as able to do: 9 characters at the v4 rate.
async fn live_speech_v4(rig: LiveRig) -> WeftResult<()> {
    let conn = rig.connect().await?;
    let voice = crate::testing::first_voice_id(&conn).await?;
    let outcome = rig
        .run(
            &ElevenLabsSpeakFileNode,
            json!({
                "account": rig.access("elevenlabs"),
                "text": "Hi, weft.",
                "voice": voice,
                "model": "eleven_v4",
                "outputFormat": "mp3_44100_128",
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.output("mimeType")?.as_str(), Some("audio/mpeg"));
    let size = outcome.output("sizeBytes")?.as_f64().unwrap_or(0.0);
    assert!(size > 1_000.0, "a spoken phrase is more than a kilobyte of mp3: {size}");
    Ok(())
}

/// Every format the metadata offers stores under a real extension and
/// mime, the opus family as the Ogg Opus a WhatsApp voice note wants;
/// a family the nodes never declared fails, never guessed.
fn file_types() -> WeftResult<()> {
    assert_eq!(audio_file_type("mp3_44100_128")?, ("mp3", "audio/mpeg"));
    assert_eq!(audio_file_type("pcm_16000")?, ("pcm", "audio/pcm"));
    assert_eq!(audio_file_type("ulaw_8000")?, ("ulaw", "audio/basic"));
    assert_eq!(audio_file_type("alaw_8000")?, ("alaw", "audio/PCMA"));
    assert_eq!(audio_file_type("wav_44100")?, ("wav", "audio/wav"));
    assert_eq!(audio_file_type("opus_48000_64")?, ("ogg", "audio/ogg; codecs=opus"));
    assert!(audio_file_type("flac_44100").is_err());
    Ok(())
}

/// One real short flash TTS: a stock voice speaks one sentence and the
/// stored file is genuine mp3 of a plausible size. Nothing lands on
/// the account (TTS creates no asset), so there is nothing to clean.
async fn live_speech(rig: LiveRig) -> WeftResult<()> {
    let conn = rig.connect().await?;
    let voice = crate::testing::first_voice_id(&conn).await?;
    let outcome = rig
        .run(
            &ElevenLabsSpeakFileNode,
            json!({
                "account": rig.access("elevenlabs"),
                "text": "This is a weft node test.",
                "voice": voice,
                "model": "eleven_flash_v2_5",
                "outputFormat": "mp3_44100_128",
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.output("mimeType")?.as_str(), Some("audio/mpeg"));
    let size = outcome.output("sizeBytes")?.as_f64().unwrap_or(0.0);
    assert!(size > 1_000.0, "one spoken sentence is more than a kilobyte of mp3: {size}");
    Ok(())
}

async fn speaks(rig: FakeRig) -> WeftResult<()> {
    rig.respond_raw(
        "POST",
        "/v1/text-to-speech/voice-1?output_format=mp3_44100_128",
        200,
        "audio/mpeg",
        b"mp3-bytes".to_vec(),
    );
    let outcome = rig
        .run(
            &ElevenLabsSpeakFileNode,
            json!({
                "account": rig.access("elevenlabs"),
                "text": "hello there",
                "voice": "voice-1",
                "model": "eleven_multilingual_v2",
                "outputFormat": "mp3_44100_128",
                "stability": 0.4,
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["filename"], json!("speech.mp3"));
    assert_eq!(outcome.outputs["mimeType"], json!("audio/mpeg"));
    assert_eq!(outcome.outputs["sizeBytes"], json!(9));

    let sent = &rig.requests()[0];
    assert_eq!(
        sent.body.as_ref().expect("json payload"),
        &json!({
            "text": "hello there",
            "model_id": "eleven_multilingual_v2",
            "voice_settings": { "stability": 0.4 },
        })
    );
    Ok(())
}

async fn settings_optional(rig: FakeRig) -> WeftResult<()> {
    rig.respond_raw(
        "POST",
        "/v1/text-to-speech/v2?output_format=pcm_16000",
        200,
        "audio/pcm",
        b"pcm".to_vec(),
    );
    let outcome = rig
        .run(
            &ElevenLabsSpeakFileNode,
            json!({
                "account": rig.access("elevenlabs"),
                "text": "hi",
                "voice": "v2",
                "model": "eleven_flash_v2_5",
                "outputFormat": "pcm_16000",
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["filename"], json!("speech.pcm"));
    let sent = &rig.requests()[0];
    assert_eq!(
        sent.body.as_ref().expect("json payload"),
        &json!({ "text": "hi", "model_id": "eleven_flash_v2_5" }),
        "no voice_settings key when no setting is set"
    );
    Ok(())
}

/// The inputs of a short flash speech, plus `extra`.
fn inputs(rig: &FakeRig, extra: Value) -> Value {
    let mut base = json!({
        "account": rig.access("elevenlabs"),
        "text": "hi",
        "voice": "v1",
        "model": "eleven_flash_v2_5",
        "outputFormat": "mp3_44100_128",
    });
    for (k, v) in extra.as_object().expect("extra inputs are an object") {
        base[k] = v.clone();
    }
    base
}

const SPEECH: &str = "/v1/text-to-speech/v1";

fn answers(rig: &FakeRig, query: &str) {
    rig.respond_with_headers("POST", &format!("{SPEECH}?{query}"), 200, "audio/mpeg", &[("request-id", "req-9")], b"mp3".to_vec());
}

async fn every_setting(rig: FakeRig) -> WeftResult<()> {
    answers(&rig, "output_format=wav_44100");
    rig.run(
        &ElevenLabsSpeakFileNode,
        inputs(
            &rig,
            json!({
                "outputFormat": "wav_44100",
                "stability": 0.4,
                "similarityBoost": 0.8,
                "style": 0.2,
                "speed": 1.1,
                "speakerBoost": false,
                "languageCode": "fr",
                "textNormalization": "off",
                "languageTextNormalization": true,
                "seed": 42,
                "pronunciationDictionaries": ["dict-a", { "id": "dict-b", "version": "v3" }],
                "usePvcAsIvc": true,
                "previousRequestIds": ["req-1", "req-2"],
                "nextRequestIds": ["req-4"],
                "filterPreset": "phone",
                "distance": 0.3,
                "environment": "hall",
                "sendLevel": 0.5,
                "backgroundNoise": "cafe",
                "pan": -0.5,
            }),
        ),
    )
    .await
    .ok()?;
    let sent = &rig.requests()[0];
    assert_eq!(sent.query.as_deref(), Some("output_format=wav_44100"), "logging stays on unless turned off");
    assert_eq!(
        sent.body.as_ref().expect("json payload"),
        &json!({
            "text": "hi",
            "model_id": "eleven_flash_v2_5",
            "voice_settings": {
                "stability": 0.4,
                "similarity_boost": 0.8,
                "style": 0.2,
                "speed": 1.1,
                "use_speaker_boost": false,
            },
            "apply_language_text_normalization": true,
            "language_code": "fr",
            "seed": 42,
            "apply_text_normalization": "off",
            "use_pvc_as_ivc": true,
            "pronunciation_dictionary_locators": [
                { "pronunciation_dictionary_id": "dict-a" },
                { "pronunciation_dictionary_id": "dict-b", "version_id": "v3" },
            ],
            "previous_request_ids": ["req-1", "req-2"],
            "next_request_ids": ["req-4"],
            "audio_effects": {
                "filter_preset_id": "phone",
                "distance": 0.3,
                "environment_id": "hall",
                "send_level": 0.5,
                "background_noise_id": "cafe",
                "pan": -0.5,
            },
        })
    );
    Ok(())
}

async fn request_id(rig: FakeRig) -> WeftResult<()> {
    answers(&rig, "output_format=mp3_44100_128");
    let outcome = rig.run(&ElevenLabsSpeakFileNode, inputs(&rig, json!({ "previousText": "Earlier." }))).await.ok()?;
    assert_eq!(outcome.outputs["requestId"], json!("req-9"), "the id ElevenLabs answered under");
    assert_eq!(rig.requests()[0].body.as_ref().expect("json payload")["previous_text"], json!("Earlier."));
    Ok(())
}

async fn zero_retention(rig: FakeRig) -> WeftResult<()> {
    answers(&rig, "output_format=mp3_44100_128&enable_logging=false");
    rig.run(&ElevenLabsSpeakFileNode, inputs(&rig, json!({ "enableLogging": false }))).await.ok()?;
    assert_eq!(rig.requests()[0].query.as_deref(), Some("output_format=mp3_44100_128&enable_logging=false"));
    Ok(())
}

/// Two short flash takes, the second stitched to the first by its
/// request id, both through a filter preset the account's catalog lists
/// (effects do not change what a request costs): about 40 characters at
/// the flash rate.
async fn live_stitched(rig: LiveRig) -> WeftResult<()> {
    let conn = rig.connect().await?;
    let voice = crate::testing::first_voice_id(&conn).await?;
    let catalog = weft::access::client::get_json(
        conn.client(),
        &format!("{}/audio-effects/catalog", crate::elevenlabs::API),
        "list the ElevenLabs audio effects",
    )
    .await?;
    let filter = catalog["filter_presets"][0]["filter_preset_id"].as_str().expect("the catalog lists a filter").to_string();
    let take = |text: &str, previous: Vec<String>| {
        json!({
            "account": rig.access("elevenlabs"),
            "text": text,
            "voice": voice,
            "model": "eleven_flash_v2_5",
            "outputFormat": "mp3_44100_128",
            "filterPreset": filter,
            "previousRequestIds": previous,
        })
    };
    let first = rig.run(&ElevenLabsSpeakFileNode, take("The first part.", vec![])).await.ok()?;
    let id = first.output("requestId")?.as_str().expect("ElevenLabs answers with a request id").to_string();
    let second = rig.run(&ElevenLabsSpeakFileNode, take("And the second part.", vec![id])).await.ok()?;
    let size = second.output("sizeBytes")?.as_f64().unwrap_or(0.0);
    assert!(size > 1_000.0, "the stitched take is real audio: {size}");
    Ok(())
}
