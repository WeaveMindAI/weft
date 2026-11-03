//! ElevenLabsAlign self-tests: the timed words with their loss and the
//! timed characters in the stored alignment file, and the overall loss
//! beside it.

use serde_json::{json, Value};

use weft::storage::StoredFile;
use weft::{FakeRig, LiveRig, NodeTest, WeftResult};

use crate::testing::emitted_file;

use super::ElevenLabsAlignNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("aligns_and_stores_the_timed_words_and_characters", aligns),
        NodeTest::live("one_real_alignment_of_generated_speech", "elevenlabs", live_align),
    ]
}

/// Real forced alignment: TTS a known sentence, align it against its
/// own transcript, and check the alignment lands as a JSON file with
/// the overall loss beside it. Nothing lands on the account.
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
    let file = StoredFile::from_value(outcome.output("alignment")?)?;
    assert_eq!((file.filename.as_str(), file.mime_type.as_str()), ("alignment.json", "application/json"));
    let alignment: Value = serde_json::from_slice(&rig.stored_bytes(outcome.output("alignment")?).await?)
        .map_err(|e| weft::node_error(format!("the alignment file is not JSON: {e}")))?;
    // Every word of the transcript is timed, in order, and so is every
    // character.
    let words: Vec<&Value> = alignment["words"]
        .as_array()
        .expect("the file holds the words")
        .iter()
        .filter(|w| !w["text"].as_str().unwrap_or_default().trim().is_empty())
        .collect();
    assert_eq!(words.len(), 9, "every transcript word gets a timing: {words:?}");
    let mut last_end = 0.0;
    for w in &words {
        let (start, end) = (w["start"].as_f64().unwrap_or(-1.0), w["end"].as_f64().unwrap_or(-1.0));
        assert!(start >= last_end - 0.05 && end >= start, "times run forward: {words:?}");
        last_end = end;
    }
    assert_eq!(
        alignment["characters"].as_array().map(Vec::len),
        Some(transcript.chars().count()),
        "every character gets a timing"
    );
    assert!(outcome.output("loss")?.is_number(), "the whole transcript's loss");
    Ok(())
}

/// An alignment answer for "hi yo": the words with the spacing
/// between them as its own token, the way ElevenLabs answers, and
/// every character.
fn answers(rig: &FakeRig) {
    rig.respond(
        "POST",
        "/v1/forced-alignment",
        json!({
            "words": [
                { "text": "hi", "start": 0.0, "end": 0.4, "loss": 0.1, "extra": "dropped" },
                { "text": " ", "start": 0.4, "end": 0.5, "loss": 0.0 },
                { "text": "yo", "start": 0.5, "end": 0.9, "loss": 0.3 },
            ],
            "characters": [
                { "text": "h", "start": 0.0, "end": 0.2 },
                { "text": "i", "start": 0.2, "end": 0.4, "extra": "dropped" },
                { "text": " ", "start": 0.4, "end": 0.5 },
                { "text": "y", "start": 0.5, "end": 0.7 },
                { "text": "o", "start": 0.7, "end": 0.9 },
            ],
            "loss": 0.12,
        }),
    );
}

fn inputs(rig: &FakeRig) -> Value {
    let audio = rig.store_file("take.mp3", "audio/mpeg", b"audio".to_vec());
    json!({
        "account": rig.access("elevenlabs"),
        "audio": audio,
        "transcript": "hi yo",
    })
}

/// Nothing reads the outputs here, and the file is stored anyway: it
/// is the node's product.
async fn aligns(rig: FakeRig) -> WeftResult<()> {
    answers(&rig);
    let inputs = inputs(&rig);
    let outcome = rig.run(&ElevenLabsAlignNode, inputs).await.ok()?;
    let (bytes, filename, mime) = emitted_file(&rig, &outcome.outputs, "alignment")?;
    assert_eq!((filename.as_str(), mime.as_str()), ("alignment.json", "application/json"));
    let alignment: Value = serde_json::from_slice(&bytes).expect("the alignment is JSON");
    assert_eq!(
        alignment,
        json!({
            "words": [
                { "text": "hi", "start": 0.0, "end": 0.4, "loss": 0.1 },
                { "text": "yo", "start": 0.5, "end": 0.9, "loss": 0.3 },
            ],
            "characters": [
                { "text": "h", "start": 0.0, "end": 0.2 },
                { "text": "i", "start": 0.2, "end": 0.4 },
                { "text": " ", "start": 0.4, "end": 0.5 },
                { "text": "y", "start": 0.5, "end": 0.7 },
                { "text": "o", "start": 0.7, "end": 0.9 },
            ],
            "loss": 0.12,
        }),
        "the words alone with their loss, every character ElevenLabs timed, and the overall loss"
    );
    assert_eq!(outcome.outputs["loss"], json!(0.12));

    let sent = &rig.requests()[0];
    assert!(sent.body_streamed, "the recording streams out of storage, never held whole");
    let form = sent.body_text.clone().expect("the rig reads a streamed body whole");
    assert!(form.contains("name=\"text\"\r\n\r\nhi yo\r\n"), "{form}");
    assert!(form.contains("name=\"file\"; filename=\"take.mp3\""), "{form}");
    Ok(())
}
