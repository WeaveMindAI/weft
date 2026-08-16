//! ElevenLabsChangeVoice self-tests.

use serde_json::json;

use weft::{FakeRig, LiveRig, NodeTest, WeftResult};

use super::ElevenLabsChangeVoiceNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("uploads_and_stores_the_revoiced_audio", revoices),
        NodeTest::live("one_real_revoice_of_generated_speech", "elevenlabs", live_revoice),
    ]
}

/// Real speech-to-speech: a freshly TTS'd clip re-voiced through a
/// stock voice; the answer is real audio. Nothing lands on the account.
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
            }),
        )
        .await
        .ok()?;
    let size = outcome.output("sizeBytes")?.as_f64().unwrap_or(0.0);
    assert!(size > 1_000.0, "the re-voiced clip is real audio: {size}");
    Ok(())
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
    let outcome = rig
        .run(
            &ElevenLabsChangeVoiceNode,
            json!({
                "account": rig.access("elevenlabs"),
                "audio": audio,
                "voice": "voice-9",
                "model": "eleven_multilingual_sts_v2",
                "outputFormat": "mp3_44100_128",
                "removeBackgroundNoise": false,
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["filename"], json!("revoiced.mp3"));
    Ok(())
}
