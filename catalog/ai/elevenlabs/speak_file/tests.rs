//! ElevenLabsSpeakFile self-tests: the request the node builds and the
//! stored-file quartet it emits, against canned audio bytes.

use serde_json::json;

use weft::{FakeRig, LiveRig, NodeTest, WeftResult};

use super::super::elevenlabs::audio_file_type;
use super::ElevenLabsSpeakFileNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::basic("every_declared_output_format_has_a_file_type", file_types),
        NodeTest::fake("speaks_and_stores_the_audio", speaks),
        NodeTest::fake("voice_settings_ride_only_when_set", settings_optional),
        NodeTest::live("one_real_short_speech", "elevenlabs", live_speech),
    ]
}

/// Every format the metadata offers stores under a real extension and
/// mime, the opus family as the Ogg Opus a WhatsApp voice note wants;
/// a family the nodes never declared is refused, never guessed.
fn file_types() -> WeftResult<()> {
    assert_eq!(audio_file_type("mp3_44100_128")?, ("mp3", "audio/mpeg"));
    assert_eq!(audio_file_type("pcm_16000")?, ("pcm", "audio/pcm"));
    assert_eq!(audio_file_type("ulaw_8000")?, ("ulaw", "audio/basic"));
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
