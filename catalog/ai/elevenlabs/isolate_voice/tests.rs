//! ElevenLabsIsolateVoice self-tests: the form the node sends, and the
//! stored file taking the type ElevenLabs answered with.

use serde_json::{json, Value};

use weft::{FakeRig, LiveRig, NodeTest, WeftResult};

use super::{isolated_name, ElevenLabsIsolateVoiceNode};

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::basic("the_cleaned_file_is_named_for_the_answered_type", names),
        NodeTest::fake("uploads_and_stores_the_cleaned_audio", isolates),
        NodeTest::fake("the_stored_type_is_the_answers_not_the_recordings", answered_type),
        NodeTest::fake("raw_pcm_rides_the_form_when_declared", raw_pcm),
        NodeTest::fake("raw_pcm_declared_on_an_encoded_recording_is_refused_before_anything_is_sent", refused),
        NodeTest::live("one_real_isolation_of_generated_speech", "elevenlabs", live_isolate),
    ]
}

/// Real isolation of a real (freshly TTS'd) speech clip: the cleaned
/// answer is genuine audio of a plausible size, in the mp3 ElevenLabs's
/// reference declares, named for it. Nothing lands on the account.
async fn live_isolate(rig: LiveRig) -> WeftResult<()> {
    let conn = rig.connect().await?;
    let voice = crate::testing::first_voice_id(&conn).await?;
    // The isolator refuses clips under ~5 seconds, so the sample has
    // to be a few spoken sentences long.
    let sample = crate::testing::speech_sample(
        &conn,
        &voice,
        "Testing voice isolation on a clip that is long enough to process. \
         The isolator needs several seconds of speech before it accepts the \
         audio, so this sentence keeps going for a little while longer.",
    )
    .await?;
    let audio = rig.store_file("sample.mp3", "audio/mpeg", sample).await?;
    let outcome = rig
        .run(
            &ElevenLabsIsolateVoiceNode,
            json!({ "account": rig.access("elevenlabs"), "audio": audio }),
        )
        .await
        .ok()?;
    let size = outcome.output("sizeBytes")?.as_f64().unwrap_or(0.0);
    assert!(size > 1_000.0, "the cleaned clip is real audio: {size}");
    assert_eq!(outcome.output("mimeType")?.as_str(), Some("audio/mpeg"), "the type the reference declares");
    assert_eq!(outcome.output("filename")?.as_str(), Some("isolated_sample.mp3"));
    Ok(())
}

fn names() -> WeftResult<()> {
    assert_eq!(isolated_name("noisy.wav", "audio/mpeg"), "isolated_noisy.mp3", "never the recording's extension");
    assert_eq!(isolated_name("take", "audio/wav"), "isolated_take.wav");
    assert_eq!(isolated_name("a.b.ogg", "audio/ogg"), "isolated_a.b.ogg");
    assert_eq!(isolated_name(".hidden", "audio/mpeg"), "isolated_.hidden.mp3");
    assert_eq!(isolated_name("noisy.mp3", "audio/flac"), "isolated_noisy", "a type no format family names is not guessed");
    Ok(())
}

/// The inputs of an isolation of `audio`, plus `extra`.
fn inputs(rig: &FakeRig, audio: Value, extra: Value) -> Value {
    let mut base = json!({ "account": rig.access("elevenlabs"), "audio": audio });
    for (k, v) in extra.as_object().expect("extra inputs are an object") {
        base[k] = v.clone();
    }
    base
}

async fn isolates(rig: FakeRig) -> WeftResult<()> {
    rig.respond_raw("POST", "/v1/audio-isolation", 200, "audio/mpeg", b"clean".to_vec());
    let audio = rig.store_file("noisy.mp3", "audio/mpeg", b"noisy-bytes".to_vec());
    let outcome = rig.run(&ElevenLabsIsolateVoiceNode, inputs(&rig, audio, json!({}))).await.ok()?;
    assert_eq!(outcome.outputs["filename"], json!("isolated_noisy.mp3"));
    assert_eq!(outcome.outputs["mimeType"], json!("audio/mpeg"));
    let sent = &rig.requests()[0];
    assert!(sent.body_streamed, "the recording streams out of storage, never held whole");
    let form = sent.body_text.clone().expect("the rig reads a streamed body whole");
    assert!(form.contains("name=\"audio\"; filename=\"noisy.mp3\"") && form.contains("noisy-bytes"), "{form}");
    assert!(!form.contains("name=\"file_format\""), "ElevenLabs's own default needs no field: {form}");
    Ok(())
}

/// A wav goes in and ElevenLabs answers mp3: the stored file is mp3 by
/// both its type and its name.
async fn answered_type(rig: FakeRig) -> WeftResult<()> {
    rig.respond_raw("POST", "/v1/audio-isolation", 200, "audio/mpeg", b"clean".to_vec());
    let audio = rig.store_file("noisy.wav", "audio/wav", b"RIFF-noisy".to_vec());
    let outcome = rig.run(&ElevenLabsIsolateVoiceNode, inputs(&rig, audio, json!({}))).await.ok()?;
    assert_eq!(outcome.outputs["mimeType"], json!("audio/mpeg"));
    assert_eq!(outcome.outputs["filename"], json!("isolated_noisy.mp3"));
    Ok(())
}

async fn raw_pcm(rig: FakeRig) -> WeftResult<()> {
    rig.respond_raw("POST", "/v1/audio-isolation", 200, "audio/mpeg", b"clean".to_vec());
    let audio = rig.store_file("noisy.pcm", "audio/pcm", b"raw".to_vec());
    rig.run(&ElevenLabsIsolateVoiceNode, inputs(&rig, audio, json!({ "fileFormat": "pcm_s16le_16" }))).await.ok()?;
    let form = rig.requests()[0].body_text.clone().expect("the rig reads a streamed body whole");
    assert!(form.contains("name=\"file_format\"\r\n\r\npcm_s16le_16\r\n"), "{form}");
    Ok(())
}

async fn refused(rig: FakeRig) -> WeftResult<()> {
    rig.wire_output("_error");
    let audio = rig.store_file("noisy.mp3", "audio/mpeg", b"noisy-bytes".to_vec());
    let err = rig
        .run(&ElevenLabsIsolateVoiceNode, inputs(&rig, audio, json!({ "fileFormat": "pcm_s16le_16" })))
        .await
        .failure()?;
    assert!(err.starts_with("input error"), "a mistake in the program fails the run even with `_error` wired: {err}");
    assert!(err.contains("typed 'audio/mpeg'"), "{err}");
    assert!(rig.requests().is_empty(), "nothing is sent, so nothing is paid for");
    Ok(())
}
