//! ElevenLabsSpeak: streaming text-to-speech over a live text bus.
//!
//! The text arrives on the `text` Bus: either plain-string messages
//! (an LLM stream's `delta` entries) or `{ text }` objects (a
//! transcript bus). The node opens the connection's WebSocket to the
//! stream-input endpoint (`conn.socket`, so the handshake is signed
//! and the session is measured by the runtime), forwards each text
//! chunk, and relays every answered audio chunk onto the `audio`
//! output Bus as raw bytes (base64 exists only at this
//! provider-protocol boundary). When the text bus closes, the tail is
//! flushed, the session drains, and the full audio is also stored and
//! emitted as a file.
//!
//! The audio bus is BYTES with creator metadata declaring the format
//! (`{ "sample_rate": 16000, "encoding": "pcm_s16le" }` for the
//! default pcm_16000), the same convention the transcribe node reads,
//! so speak -> telephony and transcribe <- telephony meet on one
//! frame format.

use std::time::Duration;

use async_trait::async_trait;
use base64::Engine as _;
use serde_json::{json, Value};

use weft::access::socket::SocketMessage;
use weft::bus::{BusEntryKind, BusOptions, BusPayloadKind, WirePayload};
use weft::node::NodeOutput;
use weft::storage::StorageScope;
use weft::{node_error, Access, ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

use super::elevenlabs::audio_file_type;

#[derive(NodeManifest)]
pub struct ElevenLabsSpeakNode;

/// The chunk of text one bus entry carries: a plain string message
/// (an LLM stream's delta) or an object with a `text` field (a
/// transcript bus). Anything else carries no text.
fn text_of(payload: &Value) -> Option<&str> {
    payload.as_str().or_else(|| payload["text"].as_str())
}

/// One parsed session frame: the audio bytes it carries (empty when
/// the frame carried none; the final frame's `audio` is null) and
/// whether it declared the generation complete. A refusal or an
/// unparseable frame is an error quoting the provider.
fn classify_session_frame(payload: &str) -> WeftResult<(Vec<u8>, bool)> {
    let msg: Value = serde_json::from_str(payload)
        .map_err(|e| node_error(format!("unparseable session frame: {e}")))?;
    if let Some(err) = msg["error"].as_str() {
        return Err(node_error(format!("the session refused: {err}")));
    }
    let audio = match msg["audio"].as_str() {
        Some(b64) => base64::engine::general_purpose::STANDARD
            .decode(b64)
            .node_err("decoding an audio chunk")?,
        None => Vec::new(),
    };
    Ok((audio, msg["isFinal"].as_bool() == Some(true)))
}

/// The audio bus's creator metadata for a declared output format, so
/// a consumer knows the frame format before the first chunk.
fn bus_meta(output_format: &str) -> Value {
    if let Some(rate) = output_format.strip_prefix("pcm_").and_then(|r| r.parse::<u64>().ok())
    {
        return json!({ "sample_rate": rate, "encoding": "pcm_s16le" });
    }
    if output_format == "ulaw_8000" {
        return json!({ "sample_rate": 8000, "encoding": "ulaw" });
    }
    json!({ "encoding": output_format })
}

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for ElevenLabsSpeakNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let voice: String = ctx.inputs.get("voice")?;
        let model: String = ctx.inputs.get("model")?;
        let output_format: String = ctx.inputs.get("outputFormat")?;
        // Resolved before the session opens: a format this node cannot
        // store must refuse before any paid synthesis, not after.
        let (ext, mime) = audio_file_type(&output_format)?;
        let text = ctx.bus_from_input("text")?;
        // Read from the earliest message still buffered: the producer
        // may have started before this node attached.
        let mut chunks = text.cursor_from_start();

        let conn = ctx.open(&account).await?;
        let url = format!(
            "wss://api.elevenlabs.io/v1/text-to-speech/{voice}/stream-input\
             ?model_id={}&output_format={}&auto_mode=true&inactivity_timeout=180",
            urlencoding::encode(&model),
            urlencoding::encode(&output_format),
        );
        let mut session = conn.socket(&url).await?;
        // The protocol's opener: a single space plus the settings the
        // session keeps.
        session
            .send(SocketMessage::Text(json!({ "text": " " }).to_string()))
            .await?;

        // The live audio bus: marker emitted before any synthesis (a
        // downstream consumer reads while chunks land), closed on
        // EVERY exit by the guard. Ephemeral: the frames are transient
        // by nature; the stored file below is the durable copy.
        let audio_bus = ctx
            .open_bus(
                "audio",
                BusOptions {
                    ephemeral: true,
                    payload: BusPayloadKind::Bytes,
                    meta: bus_meta(&output_format),
                    ..Default::default()
                },
                "speaker",
            )
            .await?;

        /// One session frame's audio, relayed onto the bus and into
        /// the full-copy buffer; `true` when the frame said the
        /// session is complete.
        fn relay_frame(
            payload: &str,
            bus: &weft::bus::BusHandle,
            full: &mut Vec<u8>,
        ) -> WeftResult<bool> {
            let (audio, is_final) = classify_session_frame(payload)?;
            if !audio.is_empty() {
                full.extend_from_slice(&audio);
                bus.send_bytes("audio", audio).node_err("relaying an audio chunk")?;
            }
            Ok(is_final)
        }

        let mut full: Vec<u8> = Vec::new();
        // Whether any chunk carried a pronounceable character.
        // Whitespace-only chunks are still forwarded (they space the
        // words), but a stream of nothing but whitespace synthesizes
        // no audio, and that outcome must read as "no usable text",
        // not as a provider failure.
        let mut spoke = false;
        // The provider closes an idle session (the URL asks for the
        // maximum window), and real text streams pause far longer (a
        // slow first token, a tool call, a human): tick the
        // protocol's sanctioned no-op (the same single space the
        // opener sends) so quiet stretches keep the session alive.
        // Never touches `spoke`: a keep-alive is not user text.
        let mut keepalive = tokio::time::interval_at(
            tokio::time::Instant::now() + Duration::from_secs(15),
            Duration::from_secs(15),
        );
        keepalive.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // Forward text and relay audio CONCURRENTLY: the session
        // speaks in both directions at once. The loop ends when the
        // text bus closes; the drain below picks up the queued tail.
        loop {
            tokio::select! {
                _ = keepalive.tick() => {
                    session
                        .send(SocketMessage::Text(json!({ "text": " " }).to_string()))
                        .await?;
                }
                entry = chunks.next() => match entry {
                    Some(entry) => {
                        if let BusEntryKind::Message {
                            payload: Some(WirePayload::Json(p)), ..
                        } = &entry.kind
                        {
                            if let Some(t) = text_of(p).filter(|t| !t.is_empty()) {
                                spoke |= t.chars().any(|c| !c.is_whitespace());
                                session
                                    .send(SocketMessage::Text(
                                        json!({ "text": t }).to_string(),
                                    ))
                                    .await?;
                                // Real text IS activity: re-base the
                                // idle timer so a no-op space can
                                // never land between two mid-word
                                // deltas of an active stream.
                                keepalive.reset();
                            }
                        }
                    }
                    // The text ended; the drain below takes over.
                    None => break,
                },
                frame = session.recv() => match frame? {
                    Some(msg) => {
                        // A final frame is not expected before the
                        // goodbye below; one arriving here means the
                        // session ended its generation while text was
                        // still coming, so the tail would go unspoken.
                        if relay_frame(&msg.into_text()?, &audio_bus, &mut full)? {
                            return Err(node_error(
                                "the session finished before the text ended; the tail would go \
                                 unspoken",
                            ));
                        }
                    }
                    None => {
                        return Err(node_error("the session closed before the text ended"))
                    }
                },
            }
        }

        // The text ended: close the generation (the protocol's
        // empty-text goodbye flushes whatever is buffered), then
        // drain the session's queued audio until its final frame.
        session
            .send(SocketMessage::Text(json!({ "text": "" }).to_string()))
            .await?;
        let mut finished = false;
        while let Some(msg) = session.recv().await? {
            if relay_frame(&msg.into_text()?, &audio_bus, &mut full)? {
                finished = true;
                break;
            }
        }
        // Only the final frame ends a generation cleanly; a socket
        // that closed first cut the session short, and storing the
        // partial bytes would report a truncated file as success.
        if !finished {
            return Err(node_error(
                "the session closed before it finished speaking; the audio would be truncated",
            ));
        }
        session.close().await?;
        drop(audio_bus);

        // A zero-byte file would look like a success and only fail
        // downstream, silently; name the actual cause instead.
        if !spoke {
            return Err(node_error(
                "no usable text arrived on the 'text' bus (it carries plain-string messages \
                 or { text } objects)",
            ));
        }
        if full.is_empty() {
            return Err(node_error("the session finished without producing any audio"));
        }

        let stored = ctx
            .storage(StorageScope::Execution)
            .put(full, mime, &format!("speech.{ext}"), None)
            .await?;
        let stored = weft::storage::StoredFile::from_value(&stored)?;
        ctx.pulse_downstream(NodeOutput::stored_file(stored)).await
    }
}
