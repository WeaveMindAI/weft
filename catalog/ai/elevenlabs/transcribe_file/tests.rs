//! ElevenLabsTranscribeFile self-tests: the multipart upload and the
//! transcript it emits, against a canned answer; one real transcription
//! of a freshly spoken sentence on the live tier.

use serde_json::json;

use weft::{FakeRig, LiveRig, NodeTest, WeftResult};

use super::{transcript_output, ElevenLabsTranscribeFileNode};

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::basic("an_answer_without_text_is_refused", answer_shape),
        NodeTest::fake("uploads_the_file_and_emits_the_transcript", transcribes),
        NodeTest::live("one_real_transcription_of_generated_speech", "elevenlabs", live_transcribe),
    ]
}

fn answer_shape() -> WeftResult<()> {
    let out = transcript_output(&json!({ "text": "hello there", "language_code": "en" }))?;
    assert_eq!(out.outputs["text"], json!("hello there"));
    assert_eq!(out.outputs["language"], json!("en"));
    let no_language = transcript_output(&json!({ "text": "salut" }))?;
    assert!(!no_language.outputs.contains_key("language"), "no language reported, no port mentioned");
    let err = transcript_output(&json!({ "words": [] })).expect_err("no text is a refusal").to_string();
    assert!(err.contains("no text"), "{err}");
    Ok(())
}

async fn transcribes(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "POST",
        "/v1/speech-to-text",
        json!({ "language_code": "fr", "text": "bonjour tout le monde" }),
    );
    let audio = rig.store_file("note.ogg", "audio/ogg", b"OggS-bytes".to_vec());
    let outcome = rig
        .run(
            &ElevenLabsTranscribeFileNode,
            json!({ "account": rig.access("elevenlabs"), "audio": audio, "language": "fr" }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["text"], json!("bonjour tout le monde"));
    assert_eq!(outcome.outputs["language"], json!("fr"));
    let sent = &rig.requests()[0];
    let content_type = sent
        .headers
        .iter()
        .find(|(name, _)| name == "content-type")
        .map(|(_, value)| value.clone())
        .unwrap_or_default();
    assert!(content_type.starts_with("multipart/form-data"), "{content_type}");
    assert!(!sent.body_streamed, "the form is a buffered body the rig can read");
    let raw = sent.body_text.clone().unwrap_or_default();
    assert!(raw.contains("name=\"file\"") && raw.contains("OggS-bytes"), "the bytes ride the form");
    assert!(raw.contains("name=\"model_id\"") && raw.contains("scribe_v1"), "the default model rides");
    assert!(raw.contains("name=\"language_code\"") && raw.contains("fr"), "the language rides when set");
    Ok(())
}

/// One real transcription: a stock voice speaks a sentence through the
/// flash TTS route, the file goes through Scribe, and a word of the
/// sentence comes back. Nothing lands on the account.
async fn live_transcribe(rig: LiveRig) -> WeftResult<()> {
    let conn = rig.connect().await?;
    let voice = crate::testing::first_voice_id(&conn).await?;
    let sample = crate::testing::speech_sample(
        &conn,
        &voice,
        "The quick brown fox jumps over the lazy dog.",
    )
    .await?;
    let audio = rig.store_file("sample.mp3", "audio/mpeg", sample).await?;
    let outcome = rig
        .run(
            &ElevenLabsTranscribeFileNode,
            json!({ "account": rig.access("elevenlabs"), "audio": audio, "language": "en" }),
        )
        .await
        .ok()?;
    let text = outcome.output("text")?.as_str().unwrap_or_default().to_lowercase();
    assert!(text.contains("fox"), "the transcript carries the sentence: {text}");
    Ok(())
}
