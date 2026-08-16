//! ElevenLabsSpeak self-tests: the pure pieces (chunk extraction and
//! the bus metadata derivation). The full websocket loop is exercised
//! live; its protocol vocabulary is small enough that the pure tests
//! carry the shaping.

use serde_json::json;

use weft::bus::BusOptions;
use weft::{LiveRig, NodeTest, WeftResult};

use super::{bus_meta, classify_session_frame, text_of, ElevenLabsSpeakNode};

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::basic("text_extraction_reads_both_bus_shapes", texts),
        NodeTest::basic("bus_metadata_matches_the_output_format", meta),
        NodeTest::basic("session_frames_classify_by_their_vocabulary", frames),
        NodeTest::live("one_real_streamed_speech", "elevenlabs", live_stream),
    ]
}

/// One real streaming session: two text chunks on a live bus, spoken
/// through the stream-input websocket; the stored full-audio file
/// proves the chunks synthesized and the drain ran to the final
/// frame. Nothing lands on the account.
async fn live_stream(rig: LiveRig) -> WeftResult<()> {
    let conn = rig.connect().await?;
    let voice = crate::testing::first_voice_id(&conn).await?;
    let (mut text, marker) = rig.bus(BusOptions::default())?;
    text.register("llm").expect("a fresh bus accepts its first name");
    // The two producer shapes the node accepts: a plain-string delta
    // and a { text } object.
    text.send("delta", json!("Hello from a weft ")).expect("an open bus accepts a send");
    text.send("text", json!({ "text": "streaming test." }))
        .expect("an open bus accepts a send");
    text.close();

    let outcome = rig
        .run(
            &ElevenLabsSpeakNode,
            json!({
                "account": rig.access("elevenlabs"),
                "text": marker,
                "voice": voice,
                "model": "eleven_flash_v2_5",
                "outputFormat": "pcm_16000",
            }),
        )
        .await
        .ok()?;
    let size = outcome.output("sizeBytes")?.as_f64().unwrap_or(0.0);
    // pcm_16000 is 32 kB per second; one short spoken sentence is
    // comfortably past 10 kB.
    assert!(size > 10_000.0, "the streamed speech is real audio: {size}");
    Ok(())
}

fn texts() -> WeftResult<()> {
    assert_eq!(text_of(&json!("hello ")), Some("hello "));
    assert_eq!(text_of(&json!({ "text": "hi " })), Some("hi "));
    assert_eq!(text_of(&json!({ "other": 1 })), None);
    assert_eq!(text_of(&json!(42)), None);
    Ok(())
}

fn frames() -> WeftResult<()> {
    // An audio frame decodes its base64 payload ("aGk=" is "hi").
    let (audio, is_final) = classify_session_frame(r#"{"audio":"aGk=","isFinal":null}"#)?;
    assert_eq!(audio, b"hi");
    assert!(!is_final);
    // The final frame carries a null audio and ends the generation.
    let (audio, is_final) = classify_session_frame(r#"{"audio":null,"isFinal":true}"#)?;
    assert!(audio.is_empty());
    assert!(is_final);
    // A refusal surfaces the provider's own words.
    let err = classify_session_frame(r#"{"error":"quota exceeded"}"#)
        .expect_err("a refusal frame must refuse")
        .to_string();
    assert!(err.contains("quota exceeded"), "{err}");
    // Garbage is an error, never a silent skip.
    assert!(classify_session_frame("not json").is_err());
    Ok(())
}

fn meta() -> WeftResult<()> {
    assert_eq!(
        bus_meta("pcm_16000"),
        json!({ "sample_rate": 16000, "encoding": "pcm_s16le" })
    );
    assert_eq!(bus_meta("ulaw_8000"), json!({ "sample_rate": 8000, "encoding": "ulaw" }));
    assert_eq!(bus_meta("mp3_44100_128"), json!({ "encoding": "mp3_44100_128" }));
    Ok(())
}
