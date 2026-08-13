//! ElevenLabsTranscribe self-tests: the server-frame classifier (the
//! node's pure decision core), plus one real realtime session. The
//! socket session has no fake form yet, so the streaming path's free
//! coverage is the mechanism e2es.

use serde_json::json;

use weft::bus::{BusOptions, BusPayloadKind};
use weft::{LiveRig, NodeTest, WeftResult};

use super::{classify_server_frame, ServerWord};

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::live("one_real_session_over_a_tone_clip", "elevenlabs", live_session),
        NodeTest::basic("a_committed_frame_forwards_its_text", || {
            let word = classify_server_frame(
                r#"{"message_type": "committed_transcript", "text": "hello there"}"#,
            );
            assert!(matches!(word, ServerWord::Committed(t) if t == "hello there"));
            Ok(())
        }),
        NodeTest::basic("timestamped_variants_classify_by_commitment", || {
            // The committed one forwards; the final (still-revising
            // family) one is ignored.
            let committed = classify_server_frame(
                r#"{"message_type": "committed_transcript_with_timestamps", "text": "so"}"#,
            );
            assert!(matches!(committed, ServerWord::Committed(t) if t == "so"));
            let final_ts = classify_server_frame(
                r#"{"message_type": "final_transcript_with_timestamps", "text": "so"}"#,
            );
            assert!(matches!(final_ts, ServerWord::Ignore));
            Ok(())
        }),
        NodeTest::basic("bookkeeping_frames_are_ignored", || {
            for kind in [
                "session_started",
                "partial_transcript",
                "final_transcript",
                "committed_transcript_entities",
            ] {
                let word = classify_server_frame(&format!(r#"{{"message_type": "{kind}"}}"#));
                assert!(matches!(word, ServerWord::Ignore), "{kind} not ignored");
            }
            Ok(())
        }),
        NodeTest::basic("insufficient_audio_activity_is_the_benign_end", || {
            let word =
                classify_server_frame(r#"{"message_type": "insufficient_audio_activity"}"#);
            assert!(matches!(word, ServerWord::NothingLeft));
            Ok(())
        }),
        NodeTest::basic("an_unparseable_body_refuses", || {
            let word = classify_server_frame("not json at all");
            assert!(matches!(word, ServerWord::Refused(why) if why.contains("unparseable")));
            Ok(())
        }),
        NodeTest::basic("an_unknown_type_refuses_with_its_error_detail", || {
            let word = classify_server_frame(
                r#"{"message_type": "quota_exceeded", "error": "out of credits"}"#,
            );
            assert!(
                matches!(word, ServerWord::Refused(why) if why.contains("quota_exceeded")
                    && why.contains("out of credits"))
            );
            Ok(())
        }),
    ]
}

/// One real realtime session: two seconds of a generated 440 Hz tone
/// on a live audio bus, transcribed for real. A tone carries no
/// speech, so the assertion is the protocol round-trip itself (the
/// session opens, drains the audio, and answers a String transcript),
/// not any particular words; the audio never lands anywhere, so there
/// is nothing to clean up.
async fn live_session(rig: LiveRig) -> WeftResult<()> {
    const RATE: usize = 16_000;
    let (mut audio, marker) = rig.bus(BusOptions {
        payload: BusPayloadKind::Bytes,
        meta: json!({ "sample_rate": RATE, "encoding": "pcm_s16le" }),
        ..BusOptions::default()
    })?;
    audio.register("mic").expect("a fresh bus accepts its first name");
    // 2 seconds of 16-bit little-endian mono sine, in quarter-second
    // frames: the same chunked shape a microphone producer sends.
    let samples: Vec<u8> = (0..2 * RATE)
        .flat_map(|n| {
            let t = n as f64 / RATE as f64;
            let s = (t * 440.0 * 2.0 * std::f64::consts::PI).sin();
            (((s * 0.4) * i16::MAX as f64) as i16).to_le_bytes()
        })
        .collect();
    for frame in samples.chunks(RATE / 2) {
        audio
            .send_bytes("audio", frame.to_vec())
            .expect("an open bytes bus accepts a frame");
    }
    audio.close();

    let outcome = rig
        .run(
            &super::ElevenLabsTranscribeNode,
            json!({ "account": rig.access("elevenlabs"), "audio": marker }),
        )
        .await
        .ok()?;
    assert!(
        outcome.output("text")?.is_string(),
        "the session ends in a String transcript (a tone may transcribe to nothing)"
    );
    Ok(())
}
