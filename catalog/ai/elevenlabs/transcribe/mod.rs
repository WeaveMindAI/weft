//! ElevenLabsTranscribe: realtime speech-to-text over a live audio bus.
//!
//! The audio arrives on the `audio` Bus: raw 16-bit PCM frames on a
//! BYTES bus whose creator metadata declares the format (an AudioStream
//! node produces exactly this shape):
//!
//! ```json
//! { "sample_rate": 16000, "encoding": "pcm_s16le" }
//! ```
//!
//! The node opens the connection's WebSocket to the realtime Scribe
//! endpoint (`conn.socket`, so the handshake is signed and the session
//! is measured by the runtime), forwards each frame as an
//! `input_audio_chunk` (base64 exists only at this provider-protocol
//! boundary), and relays every COMMITTED transcript segment onto the
//! `transcript` output Bus as it lands. When the audio bus closes, the
//! remaining audio is flushed with a final commit, the session is
//! closed, and the full transcript is emitted on `text`.
//!
//! Segmentation is the provider's voice-activity detection
//! (`commit_strategy=vad`): committed segments flow mid-stream, which
//! is what makes the transcript LIVE rather than one blob at the end.
//! Partial (still-revising) transcripts are not forwarded; a consumer
//! of the bus sees each segment once, final.

use async_trait::async_trait;
use base64::Engine as _;
use serde_json::{json, Value};

use weft::access::socket::SocketMessage;
use weft::bus::BusOptions;
use weft::{node_error, ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};
use weft::node::NodeOutput;

#[derive(NodeManifest)]
pub struct ElevenLabsTranscribeNode;

/// The sample rates the realtime endpoint accepts as `pcm_<rate>`.
const SUPPORTED_RATES: [u64; 6] = [8_000, 16_000, 22_050, 24_000, 44_100, 48_000];

/// One transcript-affecting answer from the session, reduced to what the
/// forwarding loop acts on. Pure classification so the protocol's
/// vocabulary is testable without a socket.
enum ServerWord {
    /// A committed segment: forward it.
    Committed(String),
    /// Bookkeeping (session_started, partial transcripts): ignore.
    Ignore,
    /// "There was nothing left to transcribe" on the final flush: a
    /// clean end, not a failure.
    NothingLeft,
    /// A refusal: fail the node with the provider's own words.
    Refused(String),
}

fn classify_server_frame(payload: &str) -> ServerWord {
    let Ok(msg) = serde_json::from_str::<Value>(payload) else {
        return ServerWord::Refused(format!("unparseable session frame: {payload:.200}"));
    };
    match msg["message_type"].as_str().unwrap_or("") {
        "committed_transcript" | "committed_transcript_with_timestamps" => {
            ServerWord::Committed(msg["text"].as_str().unwrap_or("").to_string())
        }
        "session_started" | "partial_transcript" | "final_transcript"
        | "final_transcript_with_timestamps" | "committed_transcript_entities" => {
            ServerWord::Ignore
        }
        "insufficient_audio_activity" => ServerWord::NothingLeft,
        other => ServerWord::Refused(format!(
            "the session answered '{other}': {}",
            msg["error"].as_str().unwrap_or("no detail")
        )),
    }
}

/// One committed segment lands on the transcript bus and joins the
/// full transcript; an empty segment lands nowhere.
fn record_segment(bus: &weft::bus::BusHandle, full: &mut String, text: String) -> WeftResult<()> {
    if text.is_empty() {
        return Ok(());
    }
    bus.send("text", json!({ "text": text })).node_err("relay a segment")?;
    if !full.is_empty() {
        full.push(' ');
    }
    full.push_str(&text);
    Ok(())
}

#[async_trait]
impl Node for ElevenLabsTranscribeNode {
    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account = ctx.inputs.get("account")?;
        let language: Option<String> =
            ctx.inputs.opt::<String>("language")?.filter(|l| !l.trim().is_empty());
        let audio = ctx.bus_from_input("audio")?;

        // The stream's format is the bus's creator metadata: known
        // before the first frame, so the session dials to match.
        let rate = audio.meta()["sample_rate"].as_u64().ok_or_else(|| {
            node_error(
                "the audio bus declares no sample_rate in its metadata; wire an \
                 AudioStream (or a producer declaring the same contract) into 'audio'",
            )
        })?;
        if !SUPPORTED_RATES.contains(&rate) {
            return Err(node_error(format!(
                "the audio stream's sample rate ({rate} Hz) is not one the realtime \
                 endpoint accepts ({SUPPORTED_RATES:?}); resample the source"
            )));
        }
        // Read from the earliest frame still buffered: the producer may
        // have started before this node attached (its ephemeral window
        // is the allowed head start).
        let mut frames = audio.cursor_from_start();

        let conn = ctx.open(&account).await?;
        let mut url = format!(
            "wss://api.elevenlabs.io/v1/speech-to-text/realtime\
             ?model_id=scribe_v2_realtime&audio_format=pcm_{rate}&commit_strategy=vad"
        );
        if let Some(language) = &language {
            url.push_str(&format!("&language_code={}", urlencoding::encode(language)));
        }
        let mut session = conn.socket(&url).await?;

        // The live transcript bus: marker emitted before any
        // transcription (a downstream consumer reads while segments
        // land), closed on EVERY exit by the guard.
        let transcript_bus =
            ctx.open_bus("transcript", BusOptions::default(), "transcriber").await?;

        let mut full = String::new();
        // Forward audio and relay transcripts CONCURRENTLY: the session
        // speaks in both directions at once. The loop ends when the
        // audio bus closes; every server frame already queued is picked
        // up by the drain below, never abandoned.
        loop {
            tokio::select! {
                next = frames.next_bytes("audio") => match next.node_err("reading the audio stream")? {
                    Some((_, pcm)) => {
                        session
                            .send(SocketMessage::Text(
                                json!({
                                    "message_type": "input_audio_chunk",
                                    "audio_base_64":
                                        base64::engine::general_purpose::STANDARD.encode(&pcm),
                                    "sample_rate": rate,
                                })
                                .to_string(),
                            ))
                            .await?;
                    }
                    // The audio ended; the drain below takes over.
                    None => break,
                },
                frame = session.recv() => match frame? {
                    Some(msg) => match classify_server_frame(&msg.into_text()?) {
                        ServerWord::Committed(text) => {
                            record_segment(&transcript_bus, &mut full, text)?
                        }
                        ServerWord::Ignore | ServerWord::NothingLeft => {}
                        ServerWord::Refused(why) => return Err(node_error(why)),
                    },
                    None => {
                        return Err(node_error("the session closed before the audio ended"))
                    }
                },
            }
        }

        // The audio ended: commit whatever the VAD still holds, then
        // drain the session alone. The drain relays EVERY committed
        // segment (the flush may commit several) and ends only on the
        // benign nothing-left answer, the session closing, or a refusal.
        session
            .send(SocketMessage::Text(
                json!({
                    "message_type": "input_audio_chunk",
                    "audio_base_64": "",
                    "sample_rate": rate,
                    "commit": true,
                })
                .to_string(),
            ))
            .await?;
        while let Some(msg) = session.recv().await? {
            match classify_server_frame(&msg.into_text()?) {
                ServerWord::Committed(text) => record_segment(&transcript_bus, &mut full, text)?,
                ServerWord::NothingLeft => break,
                ServerWord::Ignore => {}
                ServerWord::Refused(why) => return Err(node_error(why)),
            }
        }

        session.close().await?;
        drop(transcript_bus);

        ctx.pulse_downstream(NodeOutput::new().set("text", full)).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A committed frame forwards its text.
    #[test]
    fn a_committed_frame_forwards_its_text() {
        let word = classify_server_frame(
            r#"{"message_type": "committed_transcript", "text": "hello there"}"#,
        );
        assert!(matches!(word, ServerWord::Committed(t) if t == "hello there"));
    }

    /// Both timestamped variants: the committed one forwards, the
    /// final (still-revising family) one is ignored.
    #[test]
    fn timestamped_variants_classify_by_commitment() {
        let committed = classify_server_frame(
            r#"{"message_type": "committed_transcript_with_timestamps", "text": "so"}"#,
        );
        assert!(matches!(committed, ServerWord::Committed(t) if t == "so"));
        let final_ts = classify_server_frame(
            r#"{"message_type": "final_transcript_with_timestamps", "text": "so"}"#,
        );
        assert!(matches!(final_ts, ServerWord::Ignore));
    }

    /// Every explicitly-ignored bookkeeping message type.
    #[test]
    fn bookkeeping_frames_are_ignored() {
        for kind in [
            "session_started",
            "partial_transcript",
            "final_transcript",
            "committed_transcript_entities",
        ] {
            let word = classify_server_frame(&format!(r#"{{"message_type": "{kind}"}}"#));
            assert!(matches!(word, ServerWord::Ignore), "{kind} not ignored");
        }
    }

    /// "Nothing left to transcribe" is the benign end, not a failure.
    #[test]
    fn insufficient_audio_activity_is_the_benign_end() {
        let word =
            classify_server_frame(r#"{"message_type": "insufficient_audio_activity"}"#);
        assert!(matches!(word, ServerWord::NothingLeft));
    }

    /// A body that is not JSON refuses with the payload quoted.
    #[test]
    fn an_unparseable_body_refuses() {
        let word = classify_server_frame("not json at all");
        assert!(matches!(word, ServerWord::Refused(why) if why.contains("unparseable")));
    }

    /// An unknown message_type refuses with the provider's own detail.
    #[test]
    fn an_unknown_type_refuses_with_its_error_detail() {
        let word = classify_server_frame(
            r#"{"message_type": "quota_exceeded", "error": "out of credits"}"#,
        );
        assert!(
            matches!(word, ServerWord::Refused(why) if why.contains("quota_exceeded")
                && why.contains("out of credits"))
        );
    }

}
