//! ElevenLabsChangeVoice self-tests: the form and the query the node
//! sends, against canned audio bytes.

use serde_json::{json, Value};

use weft::{FakeRig, LiveRig, NodeManifest, NodeTest, WeftResult, Widget};

use super::super::elevenlabs::audio_file_type;
use super::ElevenLabsChangeVoiceNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::basic("every_declared_output_format_has_a_file_type", file_types),
        NodeTest::fake("uploads_and_stores_the_revoiced_audio", revoices),
        NodeTest::fake("every_elevenlabs_setting_reaches_the_request", every_setting),
        NodeTest::fake("a_voice_id_can_only_ever_name_a_voice", voice_id_encoded),
        NodeTest::fake("raw_pcm_declared_on_an_encoded_recording_is_refused_before_anything_is_sent", refused),
        NodeTest::live("one_real_revoice_of_generated_speech", "elevenlabs", live_revoice),
    ]
}

/// Real speech-to-speech: a freshly TTS'd clip re-voiced through a
/// stock voice, with voice settings (which the form carries as a JSON
/// string) and a seed; the answer is real audio. Nothing lands on the
/// account.
async fn live_revoice(rig: LiveRig) -> WeftResult<()> {
    let conn = rig.connect().await?;
    let voice = crate::testing::first_voice_id(&conn).await?;
    let sample =
        crate::testing::speech_sample(&conn, &voice, "Testing the voice changer.").await?;
    let audio = rig.store_file("take.mp3", "audio/mpeg", sample).await?;
    let outcome = rig
        .run(
            &ElevenLabsChangeVoiceNode,
            json!({
                "account": rig.access("elevenlabs"),
                "audio": audio,
                "voice": voice,
                "model": "eleven_multilingual_sts_v2",
                "outputFormat": "mp3_44100_128",
                "removeBackgroundNoise": false,
                "stability": 0.5,
                "seed": 7,
            }),
        )
        .await
        .ok()?;
    let size = outcome.output("sizeBytes")?.as_f64().unwrap_or(0.0);
    assert!(size > 1_000.0, "the re-voiced clip is real audio: {size}");
    Ok(())
}

/// Every format the metadata offers (all 28 the route takes) stores
/// under a real extension and mime, so none of them can be paid for
/// and then fail to store.
fn file_types() -> WeftResult<()> {
    let spec = ElevenLabsChangeVoiceNode
        .manifest()
        .inputs
        .iter()
        .find(|i| i.name == "outputFormat")
        .expect("the node declares `outputFormat`");
    let Some(Widget::Select { options, .. }) = &spec.widget else {
        panic!("`outputFormat` is a select");
    };
    assert_eq!(options.len(), 28, "every output format of the speech to speech route");
    for format in options {
        audio_file_type(format)?;
    }
    Ok(())
}

/// The inputs of a re-voice of `audio`, plus `extra`.
fn inputs(rig: &FakeRig, audio: Value, extra: Value) -> Value {
    let mut base = json!({
        "account": rig.access("elevenlabs"),
        "audio": audio,
        "voice": "voice-9",
        "model": "eleven_multilingual_sts_v2",
        "outputFormat": "mp3_44100_128",
    });
    for (k, v) in extra.as_object().expect("extra inputs are an object") {
        base[k] = v.clone();
    }
    base
}

/// The multipart body of the one request the node sent (streamed, and
/// read whole by the rig).
fn form(rig: &FakeRig) -> String {
    let sent = &rig.requests()[0];
    assert!(sent.body_streamed, "the form streams the recording out of storage");
    sent.body_text.clone().expect("the rig reads a streamed body whole")
}

/// The value of the text field `name` in the multipart `form`, if it
/// has one.
fn field<'f>(form: &'f str, name: &str) -> Option<&'f str> {
    let marker = format!("name=\"{name}\"\r\n\r\n");
    let start = form.find(&marker)? + marker.len();
    form[start..].split("\r\n").next()
}

async fn revoices(rig: FakeRig) -> WeftResult<()> {
    rig.respond_raw(
        "POST",
        "/v1/speech-to-speech/voice-9?output_format=mp3_44100_128",
        200,
        "audio/mpeg",
        b"revoiced".to_vec(),
    );
    let audio = rig.store_file("take.mp3", "audio/mpeg", b"original".to_vec());
    let outcome = rig.run(&ElevenLabsChangeVoiceNode, inputs(&rig, audio, json!({}))).await.ok()?;
    assert_eq!(outcome.outputs["filename"], json!("revoiced.mp3"));
    assert_eq!(outcome.outputs["mimeType"], json!("audio/mpeg"));

    let sent = &rig.requests()[0];
    assert_eq!(sent.query.as_deref(), Some("output_format=mp3_44100_128"), "logging stays on unless turned off");
    let form = form(&rig);
    assert!(form.contains("name=\"audio\"; filename=\"take.mp3\"") && form.contains("original"), "{form}");
    assert_eq!(field(&form, "model_id"), Some("eleven_multilingual_sts_v2"));
    for unset in ["voice_settings", "seed", "remove_background_noise", "file_format"] {
        assert_eq!(field(&form, unset), None, "nothing the person did not set travels: {unset}");
    }
    Ok(())
}

async fn every_setting(rig: FakeRig) -> WeftResult<()> {
    rig.respond_raw(
        "POST",
        "/v1/speech-to-speech/voice-9?output_format=wav_16000&enable_logging=false",
        200,
        "audio/wav",
        b"RIFF".to_vec(),
    );
    let audio = rig.store_file("take.pcm", "audio/pcm", b"raw".to_vec());
    let outcome = rig
        .run(
            &ElevenLabsChangeVoiceNode,
            inputs(
                &rig,
                audio,
                json!({
                    "model": "eleven_english_sts_v2",
                    "outputFormat": "wav_16000",
                    "stability": 0.4,
                    "similarityBoost": 0.8,
                    "style": 0.2,
                    "speed": 1.1,
                    "speakerBoost": false,
                    "seed": 42,
                    "removeBackgroundNoise": true,
                    "fileFormat": "pcm_s16le_16",
                    "enableLogging": false,
                }),
            ),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["filename"], json!("revoiced.wav"));
    assert_eq!(outcome.outputs["mimeType"], json!("audio/wav"));

    assert_eq!(rig.requests()[0].query.as_deref(), Some("output_format=wav_16000&enable_logging=false"));
    let form = form(&rig);
    assert_eq!(field(&form, "model_id"), Some("eleven_english_sts_v2"));
    let settings: Value = serde_json::from_str(field(&form, "voice_settings").expect("the settings ride the form"))
        .expect("the settings are one JSON string");
    assert_eq!(
        settings,
        json!({ "stability": 0.4, "similarity_boost": 0.8, "style": 0.2, "speed": 1.1, "use_speaker_boost": false })
    );
    assert_eq!(field(&form, "seed"), Some("42"));
    assert_eq!(field(&form, "remove_background_noise"), Some("true"));
    assert_eq!(field(&form, "file_format"), Some("pcm_s16le_16"));
    Ok(())
}

/// A typed voice id that looks like a path stays one path segment, so
/// it cannot reach another ElevenLabs route.
async fn voice_id_encoded(rig: FakeRig) -> WeftResult<()> {
    rig.respond_raw(
        "POST",
        "/v1/speech-to-speech/..%2Fuser?output_format=mp3_44100_128",
        200,
        "audio/mpeg",
        b"mp3".to_vec(),
    );
    let audio = rig.store_file("take.mp3", "audio/mpeg", b"original".to_vec());
    rig.run(&ElevenLabsChangeVoiceNode, inputs(&rig, audio, json!({ "voice": "../user" }))).await.ok()?;
    assert_eq!(rig.requests()[0].path, "/v1/speech-to-speech/..%2Fuser");
    Ok(())
}

async fn refused(rig: FakeRig) -> WeftResult<()> {
    rig.wire_output("_error");
    let audio = rig.store_file("take.mp3", "audio/mpeg", b"original".to_vec());
    let err = rig
        .run(&ElevenLabsChangeVoiceNode, inputs(&rig, audio, json!({ "fileFormat": "pcm_s16le_16" })))
        .await
        .failure()?;
    assert!(err.starts_with("input error"), "a mistake in the program fails the run even with `_error` wired: {err}");
    assert!(err.contains("typed 'audio/mpeg'"), "{err}");
    assert!(rig.requests().is_empty(), "nothing is sent, so nothing is paid for");
    Ok(())
}
