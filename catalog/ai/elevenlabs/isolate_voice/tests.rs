//! ElevenLabsIsolateVoice self-tests.

use serde_json::json;

use weft::{FakeRig, LiveRig, NodeTest, WeftResult};

use super::ElevenLabsIsolateVoiceNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("uploads_and_stores_the_cleaned_audio", isolates),
        NodeTest::live("one_real_isolation_of_generated_speech", "elevenlabs", live_isolate),
    ]
}

/// Real isolation of a real (freshly TTS'd) speech clip: the cleaned
/// answer is genuine audio of a plausible size. Nothing lands on the
/// account.
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
    Ok(())
}

async fn isolates(rig: FakeRig) -> WeftResult<()> {
    rig.respond_raw("POST", "/v1/audio-isolation", 200, "audio/mpeg", b"clean".to_vec());
    let audio = rig.store_file("noisy.mp3", "audio/mpeg", b"noisy-bytes".to_vec());
    let outcome = rig
        .run(
            &ElevenLabsIsolateVoiceNode,
            json!({ "account": rig.access("elevenlabs"), "audio": audio }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["filename"], json!("isolated_noisy.mp3"));
    assert_eq!(outcome.outputs["mimeType"], json!("audio/mpeg"));
    let sent = &rig.requests()[0];
    assert!(sent.body.is_none(), "a multipart upload is not a JSON body");
    Ok(())
}
