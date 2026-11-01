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
//! emitted as a file. A text bus that ended failed (its producer broke
//! off) fails the node rather than speak a cut text as whole. The audio
//! bus opens on the first audio frame, so a session refused before it
//! speaks is a failure `_error` can catch.
//!
//! The session's settings ride two places, as the protocol has them:
//! the URL's query string (model, format, language, seed, ...) and the
//! opener, the first message (voice settings, the chunk schedule, the
//! pronunciation dictionaries).
//!
//! The audio bus is BYTES with creator metadata declaring the format
//! (`{ "sample_rate": 16000, "encoding": "pcm_s16le" }` for the
//! default pcm_16000), the same convention the transcribe node reads,
//! so speak -> telephony and transcribe <- telephony meet on one
//! frame format.
//!
//! When `alignment` is wired, the session is opened with
//! `sync_alignment`, the frames carry the timing of the characters
//! they speak (relative to the frame's own audio, ElevenLabs's
//! reference point), and the node places them on the whole speech by
//! the audio bytes relayed before the frame.

use std::time::Duration;

use async_trait::async_trait;
use base64::Engine as _;
use serde::Deserialize;
use serde_json::{json, Value};

use weft::access::socket::SocketMessage;
use weft::bus::{BusEntryKind, BusOptions, BusPayloadKind, WirePayload};
use weft::stream_journal::JournalPolicy;
use weft::context::PendingBus;
use weft::node::NodeOutput;
use weft::storage::{StorageScope};
use weft::{
    node_error, Access, RunContext, Node, NodeErrExt, NodeManifest, ValueBag, WeftError, WeftResult,
};

use super::elevenlabs::{audio_file_type, refuse_broken_input, WS_API};
use super::flows::set_some;
use super::speech::voice_settings;

#[derive(NodeManifest)]
pub struct ElevenLabsSpeakNode;

/// A pronunciation dictionary the session applies. The socket's
/// locator requires the version as well as the id (the batch speech
/// routes take a bare id as the latest version; this one does not).
#[derive(Deserialize)]
struct Dictionary {
    id: String,
    version: String,
}

/// Everything the session is opened with, read from the node's inputs.
struct Session {
    model: String,
    output_format: String,
    language: Option<String>,
    logging: bool,
    normalization: Option<String>,
    seed: Option<u64>,
    ssml: bool,
    auto_mode: bool,
    schedule: Vec<f64>,
    dictionaries: Vec<Dictionary>,
    voice_settings: Option<Value>,
    /// The audio's bytes per second when `alignment` is wired: what
    /// places each frame's characters on the whole speech.
    timing: Option<f64>,
}

impl Session {
    /// Read the session from the node's inputs, refusing before
    /// anything is sent what the socket would refuse or ignore.
    /// `voice_settings` is the run's [`voice_settings`], and `aligned`
    /// whether anything reads the `alignment` output.
    fn read(inputs: &ValueBag, voice_settings: Option<Value>, aligned: bool) -> WeftResult<Self> {
        let model: String = inputs.get("model")?;
        let output_format: String = inputs.get("outputFormat")?;
        let language: Option<String> = inputs.opt("languageCode")?;
        let auto_mode: bool = inputs.get("autoMode")?;
        let schedule: Vec<f64> = inputs.list("chunkLengthSchedule")?;
        if let Some(refusal) = refusal(&model, language.is_some(), auto_mode, &schedule, &output_format, aligned) {
            return Err(WeftError::Input(refusal));
        }
        Ok(Self {
            timing: if aligned { byte_rate(&output_format) } else { None },
            model,
            output_format,
            language,
            logging: inputs.get("enableLogging")?,
            normalization: inputs.opt("textNormalization")?,
            // A whole number: the widget's step holds a wired value to
            // whole numbers too.
            seed: inputs.opt("seed")?,
            ssml: inputs.get("enableSsmlParsing")?,
            auto_mode,
            schedule,
            dictionaries: inputs.list("pronunciationDictionaries")?,
            voice_settings,
        })
    }

    /// The session's address: the voice, then every setting the
    /// protocol takes in the query string. The voice id is typed text
    /// that becomes a path segment: encoded, it can only ever name a
    /// voice. The idle window is the maximum the protocol allows; the
    /// keep-alive in the loop covers longer pauses.
    fn url(&self, voice: &str) -> String {
        let mut url = format!(
            "{WS_API}/text-to-speech/{}/stream-input?model_id={}&output_format={}&inactivity_timeout=180",
            urlencoding::encode(voice),
            urlencoding::encode(&self.model),
            urlencoding::encode(&self.output_format),
        );
        let mut put = |key: &str, value: &str| url.push_str(&format!("&{key}={}", urlencoding::encode(value)));
        if self.auto_mode {
            put("auto_mode", "true");
        }
        if let Some(language) = &self.language {
            put("language_code", language.as_str());
        }
        if !self.logging {
            put("enable_logging", "false");
        }
        if let Some(normalization) = &self.normalization {
            put("apply_text_normalization", normalization.as_str());
        }
        if let Some(seed) = self.seed {
            put("seed", seed.to_string().as_str());
        }
        if self.ssml {
            put("enable_ssml_parsing", "true");
        }
        if self.timing.is_some() {
            put("sync_alignment", "true");
        }
        url
    }

    /// The protocol's opener: a single space plus the settings the
    /// session keeps, which only the first message may carry.
    fn opener(&self) -> Value {
        let mut opener = json!({ "text": " " });
        set_some(&mut opener, "voice_settings", self.voice_settings.clone());
        if !self.schedule.is_empty() {
            opener["generation_config"] = json!({ "chunk_length_schedule": self.schedule });
        }
        if !self.dictionaries.is_empty() {
            opener["pronunciation_dictionary_locators"] = Value::Array(
                self.dictionaries
                    .iter()
                    .map(|d| json!({ "pronunciation_dictionary_id": d.id, "version_id": d.version }))
                    .collect(),
            );
        }
        opener
    }
}

/// The settings the streaming socket will not take, or that would do
/// nothing there, said before anything is sent (the compiler refuses
/// the same when they are written; this is the floor for wired ones),
/// or `None` when the session is fine.
fn refusal(
    model: &str,
    language: bool,
    auto_mode: bool,
    schedule: &[f64],
    output_format: &str,
    aligned: bool,
) -> Option<String> {
    // ElevenLabs's text to speech socket takes no v3 or v4 model (those
    // stream through its text to dialogue socket). That is also why the
    // metadata lists the socket's models by hand instead of asking
    // `/v1/models` for every model that can do text to speech: that
    // list would offer the v3 and v4 models this socket refuses.
    if model.starts_with("eleven_v3") || model.starts_with("eleven_v4") {
        return Some(format!(
            "'{model}' does not stream through ElevenLabs's text to speech socket, which takes no eleven_v3 or \
             eleven_v4 model: pick a Flash or Multilingual v2 `model`, or speak v3 and v4 with ElevenLabsSpeakFile"
        ));
    }
    if language && model == "eleven_multilingual_v2" {
        return Some(
            "eleven_multilingual_v2 takes no `languageCode`: pick another `model`, or leave `languageCode` empty".into(),
        );
    }
    // In auto mode ElevenLabs decides when to generate by itself; only
    // with it off does the model wait for text to match the schedule.
    if auto_mode && !schedule.is_empty() {
        return Some(
            "`chunkLengthSchedule` is set with `autoMode` on, and in auto mode ElevenLabs decides when to generate \
             without waiting on a schedule: turn `autoMode` off, or leave `chunkLengthSchedule` empty"
                .into(),
        );
    }
    if let Some(n) = schedule.iter().find(|n| !(50.0..=500.0).contains(*n)) {
        return Some(format!(
            "`chunkLengthSchedule` holds {n}, and ElevenLabs takes each number of characters between 50 and 500"
        ));
    }
    if aligned && byte_rate(output_format).is_none() {
        return Some(format!(
            "`alignment` places each character on the whole speech by counting the audio bytes before it, and \
             '{output_format}' does not pin that down: pick a pcm, ulaw or alaw `outputFormat`, or leave \
             `alignment` unwired"
        ));
    }
    None
}

/// Bytes per second of a format whose byte count is its duration
/// exactly: 16-bit pcm, and one-byte ulaw and alaw. `None` for the
/// compressed formats and for wav, whose header shifts the count.
fn byte_rate(output_format: &str) -> Option<f64> {
    let (family, rate) = output_format.split_once('_')?;
    let rate: f64 = rate.parse().ok().filter(|r: &f64| r.is_finite() && *r > 0.0)?;
    match family {
        "pcm" => Some(rate * 2.0),
        "ulaw" | "alaw" => Some(rate),
        _ => None,
    }
}

/// The chunk of text one bus entry carries: a plain string message
/// (an LLM stream's delta) or an object with a `text` field (a
/// transcript bus). Anything else carries no text.
fn text_of(payload: &Value) -> Option<&str> {
    payload.as_str().or_else(|| payload["text"].as_str())
}

/// One spoken character: its text, and its start and length in
/// milliseconds from the start of its frame's audio.
#[derive(Debug, PartialEq)]
struct Character {
    text: String,
    start_ms: f64,
    duration_ms: f64,
}

/// One parsed session frame: the audio bytes it carries (empty when
/// the frame carried none; the final frame's `audio` is null), the
/// characters it speaks when the session was opened for timing and the
/// frame carried it, and whether it declared the generation complete.
struct Frame {
    audio: Vec<u8>,
    characters: Option<Vec<Character>>,
    is_final: bool,
}

/// Parse one session frame. `timed` reads its `alignment` too. A
/// refusal or an unparseable frame is an error quoting the provider.
fn classify_session_frame(payload: &str, timed: bool) -> WeftResult<Frame> {
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
    let characters = if timed { characters_of(&msg["alignment"])? } else { None };
    Ok(Frame { audio, characters, is_final: msg["isFinal"].as_bool() == Some(true) })
}

/// A frame's `alignment` (`chars`, `charStartTimesMs`,
/// `charDurationsMs`, three lists of one length) as characters, or
/// `None` when the frame carried none.
fn characters_of(alignment: &Value) -> WeftResult<Option<Vec<Character>>> {
    if alignment.is_null() {
        return Ok(None);
    }
    fn list<'a>(alignment: &'a Value, key: &str) -> WeftResult<&'a Vec<Value>> {
        alignment[key].as_array().node_err(format!("a frame's alignment carries no `{key}` list"))
    }
    let (chars, starts, durations) =
        (list(alignment, "chars")?, list(alignment, "charStartTimesMs")?, list(alignment, "charDurationsMs")?);
    if starts.len() != chars.len() || durations.len() != chars.len() {
        weft::node_bail!(
            "a frame's alignment lists {} characters, {} start times and {} durations",
            chars.len(),
            starts.len(),
            durations.len()
        );
    }
    chars
        .iter()
        .zip(starts)
        .zip(durations)
        .map(|((text, start), duration)| -> WeftResult<Character> {
            Ok(Character {
                text: text.as_str().node_err("a frame's alignment holds a character that is not text")?.to_string(),
                start_ms: start.as_f64().node_err("a frame's alignment holds a start time that is not a number")?,
                duration_ms: duration.as_f64().node_err("a frame's alignment holds a duration that is not a number")?,
            })
        })
        .collect::<WeftResult<Vec<_>>>()
        .map(Some)
}

/// A frame's characters placed on the whole speech, `offset` seconds
/// of audio having come before the frame: `{ text, start, end }` in
/// seconds, what the `alignment` bus carries.
fn placed(characters: &[Character], offset: f64) -> Value {
    Value::Array(
        characters
            .iter()
            .map(|c| {
                let start = offset + c.start_ms / 1000.0;
                json!({ "text": c.text, "start": start, "end": start + c.duration_ms / 1000.0 })
            })
            .collect(),
    )
}

/// One session frame, read for relaying ([`read_frame`]).
struct Relayed {
    /// Its characters placed on the whole speech, when the session was
    /// opened for timing and the frame carries any.
    alignment: Option<Value>,
    /// Its audio, already added to the full copy; `None` when it had none.
    audio: Option<Vec<u8>>,
    /// The frame said the session is complete.
    is_final: bool,
}

/// Read one session frame: its characters, placed on the whole speech
/// when `rate` (the audio's bytes per second) is given, and its audio,
/// added to the full-copy buffer.
fn read_frame(payload: &str, rate: Option<f64>, full: &mut Vec<u8>) -> WeftResult<Relayed> {
    let frame = classify_session_frame(payload, rate.is_some())?;
    let characters = frame.characters.as_ref().filter(|c| !c.is_empty());
    // The frame's times count from its own first byte, which comes after
    // every byte relayed so far.
    let alignment = rate.zip(characters).map(|(rate, characters)| placed(characters, full.len() as f64 / rate));
    let audio = (!frame.audio.is_empty()).then(|| {
        full.extend_from_slice(&frame.audio);
        frame.audio
    });
    Ok(Relayed { alignment, audio, is_final: frame.is_final })
}

/// Relay a read frame: its timing onto the `alignment` bus when that is
/// wired, its audio onto the `audio` bus, each bus opening with the first
/// piece it carries. `true` when the frame said the session is complete.
async fn relay_frame(frame: Relayed, audio_bus: &mut PendingBus<'_>, alignment_bus: Option<&mut PendingBus<'_>>) -> WeftResult<bool> {
    if let (Some(bus), Some(alignment)) = (alignment_bus, frame.alignment) {
        bus.open().await?.send("alignment", alignment).node_err("relaying the character timing")?;
    }
    if let Some(audio) = frame.audio {
        audio_bus.open().await?.send_bytes("audio", audio).node_err("relaying an audio chunk")?;
    }
    Ok(frame.is_final)
}

/// The audio bus's creator metadata for a declared output format, so
/// a consumer knows the frame format before the first chunk.
fn bus_meta(output_format: &str) -> Value {
    let (family, rate) = output_format.split_once('_').unwrap_or((output_format, ""));
    match (family, rate.parse::<u64>()) {
        ("pcm", Ok(rate)) => json!({ "sample_rate": rate, "encoding": "pcm_s16le" }),
        ("ulaw" | "alaw", Ok(rate)) => json!({ "sample_rate": rate, "encoding": family }),
        _ => json!({ "encoding": output_format }),
    }
}

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for ElevenLabsSpeakNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let voice: String = ctx.inputs.get("voice")?;
        let session = Session::read(&ctx.inputs, voice_settings(&ctx)?, ctx.is_output_wired("alignment"))?;
        // Resolved before the session opens: a format this node cannot
        // store must refuse before any paid synthesis, not after.
        let (ext, mime) = audio_file_type(&session.output_format)?;
        let text = ctx.bus_from_input("text")?;
        // Read from the earliest message still buffered: the producer
        // may have started before this node attached.
        let mut chunks = text.cursor_from_start();

        let conn = ctx.open(&account).await?;
        let mut socket = conn.socket(&session.url(&voice)).await?;
        socket.send(SocketMessage::text(session.opener().to_string())).await?;

        // The live audio bus, opened with the first audio frame (a
        // downstream consumer reads while chunks land) and ended the way
        // the body ends. Ephemeral: the frames are transient by nature;
        // the stored file below is the durable copy.
        let mut audio_bus = ctx.pending_bus(
            "audio",
            BusOptions {
                journal: JournalPolicy::ephemeral(),
                payload: BusPayloadKind::Bytes,
                meta: bus_meta(&session.output_format),
                ..Default::default()
            },
            "speaker",
        );
        // The character timing, only when something reads it, and as
        // transient as the frames it describes.
        let mut alignment_bus = session
            .timing
            .map(|_| ctx.pending_bus("alignment", BusOptions { journal: JournalPolicy::ephemeral(), ..Default::default() }, "speaker"));

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
                    socket
                        .send(SocketMessage::text(json!({ "text": " " }).to_string()))
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
                                socket
                                    .send(SocketMessage::text(
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
                frame = socket.recv() => match frame? {
                    Some(msg) => {
                        // A final frame is not expected before the
                        // goodbye below; one arriving here means the
                        // session ended its generation while text was
                        // still coming, so the tail would go unspoken.
                        let frame = read_frame(&msg.into_text()?, session.timing, &mut full)?;
                        if relay_frame(frame, &mut audio_bus, alignment_bus.as_mut()).await? {
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

        // A text that broke off is not a text to finish speaking.
        refuse_broken_input("text", text.end())?;
        // The text ended: close the generation (the protocol's
        // empty-text goodbye flushes whatever is buffered), then
        // drain the session's queued audio until its final frame.
        socket
            .send(SocketMessage::text(json!({ "text": "" }).to_string()))
            .await?;
        let mut finished = false;
        while let Some(msg) = socket.recv().await? {
            let frame = read_frame(&msg.into_text()?, session.timing, &mut full)?;
            if relay_frame(frame, &mut audio_bus, alignment_bus.as_mut()).await? {
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
        socket.close().await?;
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

        // The speech is whole (the checks above passed): readers end now, not after the file is
        // stored. A timing bus no frame carried characters for opens here,
        // so its readers read an empty stream rather than none at all.
        if let Some(alignment_bus) = &mut alignment_bus {
            alignment_bus.open().await?.close();
        }
        if let Some(audio_bus) = audio_bus.opened() {
            audio_bus.close();
        }

        let stored = ctx
            .storage(StorageScope::Run)
            .put(full, mime, &format!("speech.{ext}"), None)
            .await?;
        let stored = weft::storage::StoredFile::from_value(&stored)?;
        ctx.pulse_downstream(NodeOutput::stored_file(stored)).await
    }
}
