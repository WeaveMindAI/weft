//! ElevenLabsTranscribe: realtime speech-to-text over a live audio bus.
//!
//! The audio arrives on the `audio` Bus: raw frames on a BYTES bus whose
//! creator metadata declares the format, 16-bit PCM at one of the rates
//! the endpoint takes, or 8-bit μ-law at 8000 Hz:
//!
//! ```json
//! { "sample_rate": 16000, "encoding": "pcm_s16le" }
//! { "sample_rate": 8000, "encoding": "ulaw" }
//! ```
//!
//! The node opens the connection's WebSocket to the realtime Scribe
//! endpoint (`conn.socket`, so the handshake is signed and the session
//! is measured by the runtime) with every session setting it was given
//! in the URL, forwards each frame as an `input_audio_chunk` (base64
//! exists only at this provider-protocol boundary), and relays every
//! COMMITTED transcript segment onto the `segments` output Bus as it
//! lands, followed by what ElevenLabs annotates it with (its words and
//! language, its entities, its rewrite); the bus opens with the first
//! segment, so a session refused before it hears anything is a failure
//! `_error` can catch. When the audio bus closes, the
//! remaining audio is flushed with a final commit, the session is
//! closed, and the full transcript is stored as the JSON file emitted on
//! `transcript` (its text, and the entities and the rewritten transcript
//! when asked for), with the language beside it.
//!
//! Segmentation is the commit strategy: by default the provider's
//! voice-activity detection (`commit_strategy=vad`), so committed
//! segments flow mid-stream, which is what makes the transcript LIVE
//! rather than one blob at the end. Partial (still-revising)
//! transcripts are not forwarded; a consumer of the bus sees each
//! segment once, final. Left out on purpose: `keepalive_interval_ms`,
//! which exists for a client or proxy that closes a quiet connection,
//! and the runtime's socket closes none.

use async_trait::async_trait;
use base64::Engine as _;
use serde_json::{json, Value};

use weft::access::socket::SocketMessage;
use weft::bus::BusOptions;
use weft::context::{LogLevel, PendingBus};
use weft::node::NodeOutput;
use weft::{node_error, Access, RunContext, Node, NodeErrExt, NodeManifest, WeftError, WeftResult};

use super::elevenlabs::{detected_entity, refuse_broken_input, store_json, text_input, WS_API};

#[derive(NodeManifest)]
pub struct ElevenLabsTranscribeNode;

/// The sample rates the realtime endpoint accepts as `pcm_<rate>`.
const SUPPORTED_RATES: [u64; 6] = [8_000, 16_000, 22_050, 24_000, 44_100, 48_000];
/// The sample encoding the endpoint's `pcm_<rate>` formats mean.
const PCM_S16LE: &str = "pcm_s16le";
/// The μ-law encoding, which the endpoint takes at one rate only
/// (`ulaw_8000`).
const ULAW: &str = "ulaw";
const ULAW_RATE: u64 = 8_000;
/// ElevenLabs's limits on realtime keyterms: how many, and how long each.
const MAX_KEYTERMS: usize = 50;
const MAX_KEYTERM_CHARS: usize = 20;
/// The longest `transcript_edit` instruction ElevenLabs takes.
const MAX_EDIT_CHARS: usize = 2000;

/// Every session setting the node sends, read from its inputs.
struct Session {
    model: String,
    commit_strategy: String,
    language: Option<String>,
    secondary_languages: Vec<String>,
    vad_silence_threshold_secs: Option<f64>,
    vad_threshold: Option<f64>,
    min_speech_duration_ms: Option<u64>,
    min_silence_duration_ms: Option<u64>,
    include_timestamps: bool,
    include_language_detection: bool,
    keyterms: Vec<String>,
    no_verbatim: bool,
    entity_detection: Vec<String>,
    transcript_edit: Option<String>,
    filter_background_audio: bool,
    enable_logging: bool,
    previous_text: Option<String>,
}

impl Session {
    fn read(ctx: &RunContext) -> WeftResult<Self> {
        Ok(Self {
            model: ctx.inputs.get("model")?,
            commit_strategy: ctx.inputs.get("commitStrategy")?,
            language: text_input(ctx, "language")?,
            secondary_languages: ctx.inputs.list("secondaryLanguages")?,
            vad_silence_threshold_secs: ctx.inputs.opt("vadSilenceThresholdSecs")?,
            vad_threshold: ctx.inputs.opt("vadThreshold")?,
            // Whole numbers: the widgets' step holds a wired value to
            // whole numbers too.
            min_speech_duration_ms: ctx.inputs.opt("minSpeechDurationMs")?,
            min_silence_duration_ms: ctx.inputs.opt("minSilenceDurationMs")?,
            include_timestamps: ctx.inputs.get("includeTimestamps")?,
            include_language_detection: ctx.inputs.get("includeLanguageDetection")?,
            keyterms: ctx.inputs.list("keyterms")?,
            no_verbatim: ctx.inputs.get("noVerbatim")?,
            entity_detection: ctx.inputs.list("entityDetection")?,
            transcript_edit: text_input(ctx, "transcriptEdit")?,
            filter_background_audio: ctx.inputs.get("filterBackgroundAudio")?,
            enable_logging: ctx.inputs.get("enableLogging")?,
            previous_text: text_input(ctx, "previousText")?,
        })
    }

    /// What ElevenLabs would refuse, or what would do nothing, said before
    /// the session opens (the compiler refuses the same shapes when they
    /// are written; this is the floor for wired ones), or `None`.
    fn refusal(&self) -> Option<String> {
        let tuned = self.vad_silence_threshold_secs.is_some()
            || self.vad_threshold.is_some()
            || self.min_speech_duration_ms.is_some()
            || self.min_silence_duration_ms.is_some();
        if tuned && self.commit_strategy == "manual" {
            return Some(
                "the voice detection settings tune what commits a segment, and `commitStrategy` manual turns the \
                 voice detection off: pick `vad` or `turn_prediction`, or leave those settings empty"
                    .into(),
            );
        }
        if self.filter_background_audio && self.include_timestamps {
            return Some(
                "ElevenLabs does not combine `filterBackgroundAudio` with `includeTimestamps`: turn one of them off"
                    .into(),
            );
        }
        if let Some(edit) = &self.transcript_edit {
            if !self.entity_detection.is_empty() {
                return Some(
                    "ElevenLabs refuses a session that asks for both `transcriptEdit` and `entityDetection`: keep \
                     one of them"
                        .into(),
                );
            }
            let chars = edit.chars().count();
            if chars > MAX_EDIT_CHARS {
                return Some(format!(
                    "`transcriptEdit` is {chars} characters, and ElevenLabs takes at most {MAX_EDIT_CHARS}: shorten it"
                ));
            }
        }
        if self.keyterms.len() > MAX_KEYTERMS {
            return Some(format!(
                "`keyterms` lists {} terms, and a realtime session takes at most {MAX_KEYTERMS}: keep {MAX_KEYTERMS} \
                 or fewer",
                self.keyterms.len()
            ));
        }
        if let Some(term) = self.keyterms.iter().find(|t| t.chars().count() > MAX_KEYTERM_CHARS) {
            return Some(format!(
                "the keyterm '{term}' is {} characters, and a realtime session takes terms of at most \
                 {MAX_KEYTERM_CHARS}: shorten it",
                term.chars().count()
            ));
        }
        None
    }

    /// The session URL's query, under ElevenLabs's names: the model, the
    /// audio format and the commit strategy always, then every setting
    /// that was given and nothing left at ElevenLabs's own default. A
    /// list goes as the parameter repeated, the way ElevenLabs's own SDK
    /// sends it.
    fn query(&self, audio_format: &str) -> String {
        let mut params: Vec<(&str, String)> = vec![
            ("model_id", self.model.clone()),
            ("audio_format", audio_format.to_string()),
            ("commit_strategy", self.commit_strategy.clone()),
        ];
        if let Some(language) = &self.language {
            params.push(("language_code", language.clone()));
        }
        params.extend(self.secondary_languages.iter().map(|l| ("secondary_languages", l.clone())));
        if let Some(secs) = self.vad_silence_threshold_secs {
            params.push(("vad_silence_threshold_secs", secs.to_string()));
        }
        if let Some(threshold) = self.vad_threshold {
            params.push(("vad_threshold", threshold.to_string()));
        }
        if let Some(ms) = self.min_speech_duration_ms {
            params.push(("min_speech_duration_ms", ms.to_string()));
        }
        if let Some(ms) = self.min_silence_duration_ms {
            params.push(("min_silence_duration_ms", ms.to_string()));
        }
        for (name, on) in [
            ("include_timestamps", self.include_timestamps),
            ("include_language_detection", self.include_language_detection),
            ("no_verbatim", self.no_verbatim),
            ("filter_background_audio", self.filter_background_audio),
        ] {
            if on {
                params.push((name, "true".into()));
            }
        }
        params.extend(self.keyterms.iter().map(|t| ("keyterms", t.clone())));
        params.extend(self.entity_detection.iter().map(|e| ("entity_detection", e.clone())));
        if let Some(edit) = &self.transcript_edit {
            params.push(("transcript_edit", edit.clone()));
        }
        if !self.enable_logging {
            params.push(("enable_logging", "false".into()));
        }
        params.iter().map(|(name, value)| format!("{name}={}", urlencoding::encode(value))).collect::<Vec<_>>().join("&")
    }
}

/// The session's `audio_format`, and the sample rate every chunk
/// declares, from the audio bus's creator metadata: known before the
/// first frame, so the session dials to match. An encoding or rate the
/// endpoint does not take would transcribe to garbage, so it is refused.
fn audio_format(meta: &Value) -> WeftResult<(String, u64)> {
    let rate = meta["sample_rate"].as_u64().ok_or_else(|| {
        node_error(
            "the audio bus declares no sample_rate in its metadata; wire a producer that declares \
             { sample_rate, encoding } as its bus metadata into 'audio'",
        )
    })?;
    match meta["encoding"].as_str() {
        Some(PCM_S16LE) if SUPPORTED_RATES.contains(&rate) => Ok((format!("pcm_{rate}"), rate)),
        Some(PCM_S16LE) => Err(node_error(format!(
            "the audio stream's sample rate ({rate} Hz) is not one the realtime endpoint accepts \
             ({SUPPORTED_RATES:?}); resample the source"
        ))),
        Some(ULAW) if rate == ULAW_RATE => Ok((format!("ulaw_{ULAW_RATE}"), rate)),
        Some(ULAW) => Err(node_error(format!(
            "the audio stream is μ-law at {rate} Hz, and the realtime endpoint takes μ-law at {ULAW_RATE} Hz only; \
             resample the source"
        ))),
        Some(other) => Err(node_error(format!(
            "the audio bus declares encoding {other:?}; the realtime endpoint takes {PCM_S16LE} or {ULAW} only, \
             convert the source"
        ))),
        None => Err(node_error(format!(
            "the audio bus declares no encoding in its metadata; declare {{ sample_rate, encoding }} with \
             encoding {PCM_S16LE:?} or {ULAW:?}"
        ))),
    }
}

/// One `input_audio_chunk` frame: the audio as base64, the commit flag,
/// and the context text when this is the session's first chunk (the only
/// one ElevenLabs takes it on).
fn audio_chunk(audio: &[u8], rate: u64, previous_text: Option<&str>, commit: bool) -> String {
    let mut chunk = json!({
        "message_type": "input_audio_chunk",
        "audio_base_64": base64::engine::general_purpose::STANDARD.encode(audio),
        "commit": commit,
        "sample_rate": rate,
    });
    if let Some(text) = previous_text {
        chunk["previous_text"] = json!(text);
    }
    chunk.to_string()
}

/// One answer from the session, reduced to what the forwarding loop acts
/// on. Pure classification so the protocol's vocabulary is testable
/// without a socket.
enum ServerWord {
    /// A committed segment: forward it.
    Committed(String),
    /// The words of a committed segment, and the language ElevenLabs
    /// detected or was given for it.
    Words { text: String, words: Vec<Value>, language: Option<String> },
    /// A language ElevenLabs reported on its own.
    Language(String),
    /// The entities found in a committed segment, under weft's names,
    /// with positions within that segment.
    Entities { text: String, entities: Vec<Value> },
    /// A committed segment, rewritten as `transcript_edit` asks.
    Edited { text: String, edited: String },
    /// Bookkeeping (session_started, partial transcripts): ignore.
    Ignore,
    /// "There was nothing left to transcribe" on the final flush: a
    /// clean end, not a failure.
    NothingLeft,
    /// A warning about the session that does not end it: logged.
    Warning(String),
    /// A refusal: fail the node with the provider's own words.
    Refused(String),
}

fn classify_server_frame(payload: &str) -> ServerWord {
    let Ok(msg) = serde_json::from_str::<Value>(payload) else {
        return ServerWord::Refused(format!("unparseable session frame: {payload:.200}"));
    };
    let text = || msg["text"].as_str().unwrap_or("").to_string();
    let language = || msg["language_code"].as_str().map(str::to_string);
    match msg["message_type"].as_str().unwrap_or("") {
        "committed_transcript" => ServerWord::Committed(text()),
        "committed_transcript_with_timestamps" => ServerWord::Words {
            text: text(),
            words: msg["words"].as_array().cloned().unwrap_or_default(),
            language: language(),
        },
        "committed_transcript_entities" => {
            let found = msg["entities"].as_array().map(Vec::as_slice).unwrap_or_default();
            match found.iter().map(|e| detected_entity(e, 0)).collect::<WeftResult<Vec<_>>>() {
                Ok(entities) => ServerWord::Entities { text: text(), entities },
                Err(e) => ServerWord::Refused(e.to_string()),
            }
        }
        "edited_transcript" => match msg["edited_text"].as_str() {
            Some(edited) => ServerWord::Edited { text: text(), edited: edited.to_string() },
            None => ServerWord::Refused(format!("the session sent a rewrite with no edited_text: {payload:.200}")),
        },
        // ElevenLabs's SDK delivers the detected language on a later
        // final transcript; the segment's text came committed already.
        "final_transcript" | "final_transcript_with_timestamps" => match language() {
            Some(language) => ServerWord::Language(language),
            None => ServerWord::Ignore,
        },
        "session_started" | "partial_transcript" => ServerWord::Ignore,
        "insufficient_audio_activity" => ServerWord::NothingLeft,
        "warning" => ServerWord::Warning(msg["warning"].as_str().unwrap_or("no detail").to_string()),
        other => ServerWord::Refused(format!(
            "the session answered '{other}': {}",
            msg["error"].as_str().unwrap_or("no detail")
        )),
    }
}

/// What the session committed so far, and what ElevenLabs annotated each
/// segment with. The segments make the full transcript; an entity list or
/// a rewrite finds its segment by text, the way ElevenLabs says to match
/// them, since a rewrite can arrive after later segments.
#[derive(Default)]
struct Transcript {
    segments: Vec<Segment>,
    /// The length of the full transcript so far, in characters.
    chars: u64,
    language: Option<String>,
    /// Every entity found, positioned in the full transcript.
    entities: Vec<Value>,
}

struct Segment {
    text: String,
    /// Where the segment starts in the full transcript, in characters.
    start: u64,
    edited: Option<String>,
    entities_seen: bool,
}

impl Transcript {
    /// Add a committed segment; an empty one adds nothing (and says so).
    fn commit(&mut self, text: &str) -> bool {
        if text.is_empty() {
            return false;
        }
        // The full transcript joins the segments with one space.
        let start = if self.segments.is_empty() { 0 } else { self.chars + 1 };
        self.chars = start + text.chars().count() as u64;
        self.segments.push(Segment { text: text.to_string(), start, edited: None, entities_seen: false });
        true
    }

    fn full_text(&self) -> String {
        self.segments.iter().map(|s| s.text.as_str()).collect::<Vec<_>>().join(" ")
    }

    /// Keep the entities found in the segment `text`, positioned in the
    /// full transcript. False when no committed segment waits for them.
    fn add_entities(&mut self, text: &str, entities: &[Value]) -> bool {
        let Some(segment) = self.segments.iter_mut().find(|s| s.text == text && !s.entities_seen) else {
            return false;
        };
        segment.entities_seen = true;
        let shift = segment.start;
        for entity in entities {
            let mut placed = entity.clone();
            for key in ["startChar", "endChar"] {
                placed[key] = json!(entity[key].as_u64().unwrap_or_default() + shift);
            }
            self.entities.push(placed);
        }
        true
    }

    /// Keep the rewrite of the segment `text`. False when no committed
    /// segment waits for one.
    fn add_edit(&mut self, text: &str, edited: &str) -> bool {
        let Some(segment) = self.segments.iter_mut().find(|s| s.text == text && s.edited.is_none()) else {
            return false;
        };
        segment.edited = Some(edited.to_string());
        true
    }

    /// The whole transcript as rewritten, a segment with no rewrite kept as
    /// spoken.
    fn edited_text(&self) -> String {
        self.segments.iter().map(|s| s.edited.as_deref().unwrap_or(&s.text)).collect::<Vec<_>>().join(" ")
    }

    /// How many segments got no rewrite.
    fn unedited(&self) -> usize {
        self.segments.iter().filter(|s| s.edited.is_none()).count()
    }

    /// What the `transcript` file holds: the full text, plus the whole
    /// rewritten transcript when the session asked for rewrites, and every
    /// entity when it asked for entities, under the same names
    /// ElevenLabsTranscribeFile's transcript file uses.
    fn document(&self, session: &Session) -> Value {
        let mut document = json!({ "text": self.full_text() });
        if session.transcript_edit.is_some() {
            document["editedText"] = json!(self.edited_text());
        }
        if !session.entity_detection.is_empty() {
            document["entities"] = json!(self.entities);
        }
        document
    }
}

/// Act on one session answer: put a segment, or what ElevenLabs annotated
/// one with, on the segments bus and keep it for the end; log a warning;
/// or fail on a refusal. True when the session said nothing was left to
/// transcribe.
async fn relay(
    ctx: &RunContext,
    bus: &mut PendingBus<'_>,
    transcript: &mut Transcript,
    word: ServerWord,
) -> WeftResult<bool> {
    match word {
        ServerWord::Committed(text) => {
            if transcript.commit(&text) {
                bus.open().await?.send("text", json!({ "text": text })).node_err("relay a segment")?;
            }
        }
        ServerWord::Words { text, words, language } => {
            let mut message = json!({ "text": text, "words": words });
            if let Some(language) = language {
                message["language"] = json!(language);
                transcript.language = Some(language);
            }
            bus.open().await?.send("words", message).node_err("relay a segment's words")?;
        }
        ServerWord::Language(language) => transcript.language = Some(language),
        ServerWord::Entities { text, entities } => {
            if !transcript.add_entities(&text, &entities) && !entities.is_empty() {
                ctx.log(
                    LogLevel::Warn,
                    format!("ElevenLabs sent entities for a segment the session never committed ('{text}'); they are on `segments` but not in the transcript file's `entities`"),
                )
                .await?;
            }
            bus.open().await?.send("entities", json!({ "text": text, "entities": entities })).node_err("relay a segment's entities")?;
        }
        ServerWord::Edited { text, edited } => {
            if !transcript.add_edit(&text, &edited) && !text.is_empty() {
                ctx.log(
                    LogLevel::Warn,
                    format!("ElevenLabs sent a rewrite of a segment the session never committed ('{text}'); it is on `segments` but not in the transcript file's `editedText`"),
                )
                .await?;
            }
            bus.open().await?.send("edited", json!({ "text": text, "editedText": edited })).node_err("relay a segment's rewrite")?;
        }
        ServerWord::Ignore => {}
        ServerWord::NothingLeft => return Ok(true),
        ServerWord::Warning(what) => {
            ctx.log(LogLevel::Warn, format!("ElevenLabs warned about the session: {what}")).await?
        }
        ServerWord::Refused(why) => return Err(node_error(why)),
    }
    Ok(false)
}

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for ElevenLabsTranscribeNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let session = Session::read(&ctx)?;
        if let Some(refusal) = session.refusal() {
            return Err(WeftError::Input(refusal));
        }
        let audio = ctx.bus_from_input("audio")?;
        let (format, rate) = audio_format(audio.meta())?;
        // Read from the earliest frame still buffered: the producer may
        // have started before this node attached (its ephemeral window
        // is the allowed head start).
        let mut frames = audio.cursor_from_start();

        let conn = ctx.open(&account).await?;
        let url = format!("{WS_API}/speech-to-text/realtime?{}", session.query(&format));
        let mut socket = conn.socket(&url).await?;

        // The live segments bus, opened with the first segment (a
        // downstream consumer reads while segments land) and ended the
        // way the body ends.
        let mut segments_bus = ctx.pending_bus("segments", BusOptions { journal: ctx.journal_policy()?, ..BusOptions::default() }, "transcriber");

        let mut transcript = Transcript::default();
        // The context rides the first chunk only, whichever that is.
        let mut previous_text = session.previous_text.clone();
        let cancel = ctx.cancellation();
        // Forward audio and relay transcripts CONCURRENTLY: the session
        // speaks in both directions at once. The loop ends when the
        // audio bus closes; every server frame already queued is picked
        // up by the drain below, never abandoned.
        loop {
            tokio::select! {
                next = frames.next_bytes("audio") => match next.node_err("reading the audio stream")? {
                    Some((_, audio)) => {
                        let chunk = audio_chunk(&audio, rate, previous_text.take().as_deref(), false);
                        socket.send(SocketMessage::text(chunk)).await?;
                    }
                    // The audio ended; the drain below takes over.
                    None => break,
                },
                frame = socket.recv() => match frame? {
                    // Mid-stream, "nothing left" only means a quiet
                    // stretch: the audio goes on.
                    Some(msg) => {
                        relay(&ctx, &mut segments_bus, &mut transcript, classify_server_frame(&msg.into_text()?)).await?;
                    }
                    None => return Err(node_error("the session closed before the audio ended")),
                },
                err = cancel.cancelled_err() => return Err(err),
            }
        }

        // Audio that broke off is not a recording to transcribe whole.
        refuse_broken_input("audio", audio.end())?;
        // The audio ended: commit whatever the session still holds, then
        // drain it alone. The drain relays EVERY committed segment (the
        // flush may commit several) and what ElevenLabs annotates them
        // with, and ends only on the benign nothing-left answer, the
        // session closing, or a refusal. The session CLOSING is a clean
        // end, not a truncation: after the final commit the server
        // delivers its remaining committed segments and hangs up (the
        // nothing-left answer only comes when the flush found no speech),
        // and frames arrive in order, so every segment sent before the
        // close has already been read by this loop. Requiring a goodbye
        // frame here fails real sessions (verified live).
        let flush = audio_chunk(&[], rate, previous_text.take().as_deref(), true);
        socket.send(SocketMessage::text(flush)).await?;
        loop {
            tokio::select! {
                frame = socket.recv() => match frame? {
                    Some(msg) => {
                        let word = classify_server_frame(&msg.into_text()?);
                        if relay(&ctx, &mut segments_bus, &mut transcript, word).await? {
                            break;
                        }
                    }
                    None => break,
                },
                err = cancel.cancelled_err() => return Err(err),
            }
        }

        socket.close().await?;
        // Every segment is out: readers end now, not after the file is
        // stored. A session that heard no speech opens the bus here, so
        // its readers read an empty stream rather than none at all.
        segments_bus.open().await?.close();

        let unedited = transcript.unedited();
        if session.transcript_edit.is_some() && unedited > 0 {
            ctx.log(
                LogLevel::Warn,
                format!(
                    "ElevenLabs sent no rewrite for {unedited} of the {} segments; the transcript file's `editedText` \
                     has them as spoken",
                    transcript.segments.len()
                ),
            )
            .await?;
        }
        // A long session's transcript is more than a wire carries, so it
        // always travels as a file.
        let file = store_json(&ctx, &transcript.document(&session), "transcript.json").await?;
        let mut out = NodeOutput::new().set("transcript", file);
        if let Some(language) = transcript.language.take() {
            out = out.set("language", language);
        }
        ctx.pulse_downstream(out).await
    }
}
