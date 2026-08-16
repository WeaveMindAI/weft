//! ElevenLabsAlign self-tests.

use serde_json::json;

use weft::{FakeRig, LiveRig, NodeTest, WeftResult};

use super::ElevenLabsAlignNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("aligns_and_emits_timed_words", aligns),
        NodeTest::live("one_real_alignment_of_generated_speech", "elevenlabs", live_align),
    ]
}

/// Real forced alignment: TTS a known sentence, align it against its
/// own transcript, and check the words come back in time order.
/// Nothing lands on the account.
async fn live_align(rig: LiveRig) -> WeftResult<()> {
    let transcript = "the quick brown fox jumps over the lazy dog";
    let conn = rig.connect().await?;
    let voice = crate::testing::first_voice_id(&conn).await?;
    let sample = crate::testing::speech_sample(&conn, &voice, transcript).await?;
    let audio = rig.store_file("spoken.mp3", "audio/mpeg", sample).await?;
    let outcome = rig
        .run(
            &ElevenLabsAlignNode,
            json!({
                "account": rig.access("elevenlabs"),
                "audio": audio,
                "transcript": transcript,
            }),
        )
        .await
        .ok()?;
    let words = outcome.output("words")?.as_array().expect("a word list").clone();
    assert_eq!(words.len(), 9, "every transcript word gets a timing: {words:?}");
    let mut last_end = 0.0;
    for w in &words {
        let start = w["start"].as_f64().expect("start");
        let end = w["end"].as_f64().expect("end");
        assert!(start >= last_end - 0.05 && end >= start, "times run forward: {words:?}");
        last_end = end;
    }
    Ok(())
}

async fn aligns(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "POST",
        "/v1/forced-alignment",
        json!({
            "words": [
                { "text": "hello", "start": 0.0, "end": 0.4, "extra": "dropped" },
                { "text": "world", "start": 0.5, "end": 0.9 },
            ],
            "loss": 0.12,
        }),
    );
    let audio = rig.store_file("take.mp3", "audio/mpeg", b"audio".to_vec());
    let outcome = rig
        .run(
            &ElevenLabsAlignNode,
            json!({
                "account": rig.access("elevenlabs"),
                "audio": audio,
                "transcript": "hello world",
            }),
        )
        .await
        .ok()?;
    assert_eq!(
        outcome.outputs["words"],
        json!([
            { "text": "hello", "start": 0.0, "end": 0.4 },
            { "text": "world", "start": 0.5, "end": 0.9 },
        ])
    );
    assert_eq!(outcome.outputs["loss"], json!(0.12));
    Ok(())
}
