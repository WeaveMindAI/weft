//! ElevenLabsTranscribeFile: batch speech-to-text for a recording that
//! already exists. POST the file (or a public link ElevenLabs fetches
//! itself) to the speech-to-text route as a multipart form carrying every
//! setting the node was given, and emit what the answer carries: the
//! transcript as one stored JSON file (its text, words, entities and
//! rewrite, or one transcript per channel when the recording was split
//! by channel), the subtitle and document exports as stored files, and
//! the small facts about the recording as values. The realtime sibling
//! (`ElevenLabsTranscribe`) is for a live audio bus. Left out on
//! purpose: `webhook`, `webhook_id` and
//! `webhook_metadata`, since the node waits for the answer itself, and
//! `cloud_storage_url`, which ElevenLabs deprecated for `source_url`.

use async_trait::async_trait;
use base64::Engine as _;
use serde_json::{json, Map, Value};

use weft::access::client::{json_call, Multipart};
use weft::context::LogLevel;
use weft::node::NodeOutput;
use weft::storage::{FileHandle, StorageScope};
use weft::{Access, RunContext, Node, NodeErrExt, NodeManifest, WeftError, WeftResult};

use super::elevenlabs::{detected_entity, store_json, text_input, API};

#[derive(NodeManifest)]
pub struct ElevenLabsTranscribeFileNode;

#[cfg(feature = "node-tests")]
mod tests;

/// ElevenLabs's limits on keyterms: how many one request takes, the
/// length every term stays under, and the characters none may hold.
const MAX_KEYTERMS: usize = 1000;
const KEYTERM_CHARS_UNDER: usize = 50;
const KEYTERM_FORBIDDEN: [char; 7] = ['<', '>', '{', '}', '[', ']', '\\'];
/// The longest `transcript_edit` instruction ElevenLabs takes.
const MAX_EDIT_CHARS: usize = 2000;
/// The most export formats one request asks for.
const MAX_FORMATS: usize = 10;
/// The export formats ElevenLabs writes, and the ones that also take a
/// line length.
const FORMATS: [&str; 6] = ["segmented_json", "docx", "pdf", "txt", "html", "srt"];
const LINE_LENGTH_FORMATS: [&str; 2] = ["srt", "txt"];

/// Every setting the node sends, read from its inputs.
struct Settings {
    model: String,
    language: Option<String>,
    diarize: bool,
    num_speakers: Option<u64>,
    diarization_threshold: Option<f64>,
    detect_speaker_roles: bool,
    use_speaker_library: bool,
    timestamps_granularity: Option<String>,
    tag_audio_events: bool,
    no_verbatim: bool,
    keyterms: Vec<String>,
    additional_formats: Vec<Value>,
    entity_detection: Vec<String>,
    entity_redaction: Vec<String>,
    entity_redaction_mode: Option<String>,
    transcript_edit: Option<String>,
    use_multi_channel: bool,
    multichannel_output_style: Option<String>,
    temperature: Option<f64>,
    seed: Option<u64>,
    file_format: Option<String>,
    enable_logging: bool,
}

impl Settings {
    fn read(ctx: &RunContext) -> WeftResult<Self> {
        Ok(Self {
            model: ctx.inputs.get("model")?,
            language: text_input(ctx, "language")?,
            diarize: ctx.inputs.get("diarize")?,
            // Whole numbers: the widgets' step holds a wired value to
            // whole numbers too.
            num_speakers: ctx.inputs.opt("numSpeakers")?,
            diarization_threshold: ctx.inputs.opt("diarizationThreshold")?,
            detect_speaker_roles: ctx.inputs.get("detectSpeakerRoles")?,
            use_speaker_library: ctx.inputs.get("useSpeakerLibrary")?,
            timestamps_granularity: ctx.inputs.opt("timestampsGranularity")?,
            tag_audio_events: ctx.inputs.get("tagAudioEvents")?,
            no_verbatim: ctx.inputs.get("noVerbatim")?,
            keyterms: ctx.inputs.list("keyterms")?,
            additional_formats: ctx.inputs.list("additionalFormats")?,
            entity_detection: ctx.inputs.list("entityDetection")?,
            entity_redaction: ctx.inputs.list("entityRedaction")?,
            entity_redaction_mode: ctx.inputs.opt("entityRedactionMode")?,
            transcript_edit: text_input(ctx, "transcriptEdit")?,
            use_multi_channel: ctx.inputs.get("useMultiChannel")?,
            multichannel_output_style: ctx.inputs.opt("multichannelOutputStyle")?,
            temperature: ctx.inputs.opt("temperature")?,
            seed: ctx.inputs.opt("seed")?,
            file_format: ctx.inputs.opt("fileFormat")?,
            enable_logging: ctx.inputs.get("enableLogging")?,
        })
    }

    /// What ElevenLabs would refuse, or what would pay for nothing, said
    /// before anything is sent (the compiler refuses the same shapes when
    /// they are written; this is the floor for wired ones), or `None` when
    /// the request can go.
    fn refusal(&self) -> Option<String> {
        let entities = !self.entity_detection.is_empty() || !self.entity_redaction.is_empty();
        let combined = self.multichannel_output_style.as_deref() == Some("combined");
        let v1 = self.model.starts_with("scribe_v1");
        if self.diarization_threshold.is_some() && !self.diarize {
            return Some(
                "`diarizationThreshold` tunes the speaker labelling, which only `diarize` turns on: turn `diarize` \
                 on, or leave `diarizationThreshold` empty"
                    .into(),
            );
        }
        if self.diarization_threshold.is_some() && self.num_speakers.is_some() {
            return Some(
                "ElevenLabs takes `diarizationThreshold` only without `numSpeakers`: keep one of them".into(),
            );
        }
        if self.detect_speaker_roles && !self.diarize {
            return Some(
                "`detectSpeakerRoles` names the speakers that `diarize` finds, and `diarize` is off: turn `diarize` \
                 on, or `detectSpeakerRoles` off"
                    .into(),
            );
        }
        if self.detect_speaker_roles && self.use_multi_channel {
            return Some(
                "ElevenLabs does not detect speaker roles with `useMultiChannel`: turn one of them off".into(),
            );
        }
        if self.use_speaker_library && !self.diarize {
            return Some(
                "`useSpeakerLibrary` matches the speakers that `diarize` finds, and `diarize` is off: turn `diarize` \
                 on, or `useSpeakerLibrary` off"
                    .into(),
            );
        }
        if self.use_multi_channel && self.diarize {
            return Some(
                "with `useMultiChannel` each channel is one speaker, and ElevenLabs needs `diarize` off: turn one \
                 of them off"
                    .into(),
            );
        }
        if self.use_multi_channel && self.num_speakers.is_some() {
            return Some(
                "with `useMultiChannel` the channels set the number of speakers, and ElevenLabs refuses \
                 `numSpeakers`: leave `numSpeakers` empty, or turn `useMultiChannel` off"
                    .into(),
            );
        }
        if self.multichannel_output_style.is_some() && !self.use_multi_channel {
            return Some(
                "`multichannelOutputStyle` shapes what `useMultiChannel` answers, and `useMultiChannel` is off: \
                 turn `useMultiChannel` on, or leave `multichannelOutputStyle` empty"
                    .into(),
            );
        }
        if combined && self.timestamps_granularity.as_deref() == Some("none") {
            return Some(
                "the `combined` style merges the channels by time, and `timestampsGranularity` is none: set it to \
                 word or character, or pick the `separate` style"
                    .into(),
            );
        }
        if combined && entities {
            return Some(
                "ElevenLabs does not detect or redact entities in the `combined` style: pick the `separate` style, \
                 or leave `entityDetection` and `entityRedaction` empty"
                    .into(),
            );
        }
        if let Some(edit) = &self.transcript_edit {
            if entities || self.use_multi_channel {
                return Some(
                    "ElevenLabs does not rewrite a transcript with entity detection, redaction or \
                     `useMultiChannel`: leave `transcriptEdit` empty, or turn those off"
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
        if !self.entity_redaction.is_empty() && self.entity_detection.is_empty() {
            return Some(
                "`entityRedaction` blanks out entities that `entityDetection` finds, and `entityDetection` is \
                 empty: set `entityDetection` to cover what you redact"
                    .into(),
            );
        }
        if self.entity_redaction_mode.is_some() && self.entity_redaction.is_empty() {
            return Some(
                "`entityRedactionMode` says how redacted entities read, and `entityRedaction` redacts nothing: set \
                 `entityRedaction`, or leave `entityRedactionMode` empty"
                    .into(),
            );
        }
        if v1 && self.no_verbatim {
            return Some(format!(
                "ElevenLabs offers `noVerbatim` on scribe_v2 and scribe_v2_medical only, and this transcription \
                 uses '{}': pick one of those, or turn `noVerbatim` off",
                self.model
            ));
        }
        if !self.keyterms.is_empty() {
            if v1 {
                return Some(format!(
                    "ElevenLabs offers `keyterms` on scribe_v2 and scribe_v2_medical only, and this transcription \
                     uses '{}': pick one of those, or leave `keyterms` empty",
                    self.model
                ));
            }
            if let Some(refusal) = keyterms_refusal(&self.keyterms) {
                return Some(refusal);
            }
        }
        export_options(&self.additional_formats).err()
    }

    /// The multipart form ElevenLabs reads, under its own field names:
    /// every setting that was given, nothing that was left at
    /// ElevenLabs's own default.
    fn form(&self, mut form: Multipart) -> WeftResult<Multipart> {
        form = form.text("model_id", &self.model);
        if let Some(language) = &self.language {
            form = form.text("language_code", language);
        }
        form = flag(form, "diarize", self.diarize);
        if let Some(n) = self.num_speakers {
            form = form.text("num_speakers", &n.to_string());
        }
        if let Some(threshold) = self.diarization_threshold {
            form = form.text("diarization_threshold", &threshold.to_string());
        }
        form = flag(form, "detect_speaker_roles", self.detect_speaker_roles);
        form = flag(form, "use_speaker_library", self.use_speaker_library);
        if let Some(granularity) = &self.timestamps_granularity {
            form = form.text("timestamps_granularity", granularity);
        }
        if !self.tag_audio_events {
            form = form.text("tag_audio_events", "false");
        }
        form = flag(form, "no_verbatim", self.no_verbatim);
        // ElevenLabs reads a list of terms as the field repeated.
        for term in &self.keyterms {
            form = form.text("keyterms", term);
        }
        // The structured fields go as JSON text, the way ElevenLabs's own
        // SDK sends them.
        if !self.additional_formats.is_empty() {
            let options = export_options(&self.additional_formats).map_err(WeftError::Input)?;
            form = form.text("additional_formats", &Value::Array(options).to_string());
        }
        if !self.entity_detection.is_empty() {
            form = form.text("entity_detection", &json!(self.entity_detection).to_string());
        }
        if !self.entity_redaction.is_empty() {
            form = form.text("entity_redaction", &json!(self.entity_redaction).to_string());
        }
        if let Some(mode) = &self.entity_redaction_mode {
            form = form.text("entity_redaction_mode", mode);
        }
        if let Some(edit) = &self.transcript_edit {
            form = form.text("transcript_edit", edit);
        }
        form = flag(form, "use_multi_channel", self.use_multi_channel);
        if let Some(style) = &self.multichannel_output_style {
            form = form.text("multichannel_output_style", style);
        }
        if let Some(temperature) = self.temperature {
            form = form.text("temperature", &temperature.to_string());
        }
        if let Some(seed) = self.seed {
            form = form.text("seed", &seed.to_string());
        }
        if let Some(format) = &self.file_format {
            form = form.text("file_format", format);
        }
        Ok(form)
    }
}

/// `form` with the field `name` set to true when `set`, and untouched
/// otherwise (ElevenLabs's default for each of these is false).
fn flag(form: Multipart, name: &str, set: bool) -> Multipart {
    if set {
        form.text(name, "true")
    } else {
        form
    }
}

/// What ElevenLabs would refuse in a list of keyterms, or `None`. The
/// five-word limit is left to ElevenLabs, which counts words after its
/// own normalisation.
fn keyterms_refusal(terms: &[String]) -> Option<String> {
    if terms.len() > MAX_KEYTERMS {
        return Some(format!(
            "`keyterms` lists {} terms, and ElevenLabs takes at most {MAX_KEYTERMS}: keep {MAX_KEYTERMS} or fewer",
            terms.len()
        ));
    }
    if let Some(term) = terms.iter().find(|t| t.chars().count() >= KEYTERM_CHARS_UNDER) {
        return Some(format!(
            "the keyterm '{term}' is {} characters, and ElevenLabs takes terms under {KEYTERM_CHARS_UNDER}: shorten it",
            term.chars().count()
        ));
    }
    if let Some(term) = terms.iter().find(|t| t.chars().any(|c| KEYTERM_FORBIDDEN.contains(&c))) {
        return Some(format!(
            "the keyterm '{term}' holds one of the characters `<>{{}}[]\\`, which ElevenLabs does not take: remove it"
        ));
    }
    None
}

/// The `additional_formats` ElevenLabs reads, from the node's list: a
/// format's name, or `{ format, ... }` with the layout options under
/// weft's names. A shape ElevenLabs would refuse is the error, naming
/// the fix.
pub fn export_options(items: &[Value]) -> Result<Vec<Value>, String> {
    if items.len() > MAX_FORMATS {
        return Err(format!(
            "`additionalFormats` lists {} formats, and ElevenLabs exports at most {MAX_FORMATS} per request: keep \
             {MAX_FORMATS} or fewer",
            items.len()
        ));
    }
    items.iter().map(export_option).collect()
}

fn export_option(item: &Value) -> Result<Value, String> {
    let known = |format: &str| -> Result<(), String> {
        if FORMATS.contains(&format) {
            Ok(())
        } else {
            Err(format!("'{format}' is not a format ElevenLabs exports to; pick one of {}", FORMATS.join(", ")))
        }
    };
    if let Some(format) = item.as_str() {
        known(format)?;
        return Ok(json!({ "format": format }));
    }
    let Some(fields) = item.as_object() else {
        return Err(format!("an `additionalFormats` entry is a format name or `{{ format, ... }}`, not {item}"));
    };
    let format = fields
        .get("format")
        .and_then(Value::as_str)
        .ok_or_else(|| format!("the `additionalFormats` entry {item} names no `format`"))?;
    known(format)?;
    let mut option = Map::new();
    option.insert("format".into(), json!(format));
    for (key, value) in fields {
        let (name, whole) = match key.as_str() {
            "format" => continue,
            "includeSpeakers" => ("include_speakers", false),
            "includeTimestamps" => ("include_timestamps", false),
            "segmentOnSilenceLongerThanS" => ("segment_on_silence_longer_than_s", false),
            "maxSegmentDurationS" => ("max_segment_duration_s", false),
            "maxSegmentChars" => ("max_segment_chars", true),
            "maxCharactersPerLine" if LINE_LENGTH_FORMATS.contains(&format) => ("max_characters_per_line", true),
            "maxCharactersPerLine" => {
                return Err(format!(
                    "ElevenLabs takes `maxCharactersPerLine` on srt and txt only, and this entry is {format}: \
                     leave it out"
                ))
            }
            other => {
                return Err(format!("`{other}` is not a layout option of an `additionalFormats` entry"));
            }
        };
        let value = if whole {
            // A wire carries `80.0` where a literal held `80`; a count of
            // characters is a whole number either way.
            match value.as_f64() {
                Some(n) if n >= 0.0 && n.fract() == 0.0 => json!(n as u64),
                _ => return Err(format!("`{key}` in an `additionalFormats` entry is a whole number, not {value}")),
            }
        } else {
            value.clone()
        };
        option.insert(name.into(), value);
    }
    Ok(Value::Object(option))
}

/// What a transcript answer carries, read out of ElevenLabs's JSON.
pub struct Answer {
    pub transcripts: Transcripts,
    pub transcription_id: Option<String>,
    pub audio_duration_secs: Option<f64>,
}

/// The transcripts of an answer.
pub enum Transcripts {
    /// The whole recording's transcript (a `combined` multichannel one
    /// included).
    One(Chunk),
    /// One per channel (`useMultiChannel` with the `separate` style).
    PerChannel(Vec<Chunk>),
}

impl Transcripts {
    /// Every transcript, in the answer's order.
    pub fn chunks(&self) -> &[Chunk] {
        match self {
            Transcripts::One(chunk) => std::slice::from_ref(chunk),
            Transcripts::PerChannel(chunks) => chunks.as_slice(),
        }
    }
}

/// One transcript of the answer: the whole recording's, or one channel's.
pub struct Chunk {
    pub text: String,
    pub language: Option<String>,
    pub language_probability: Option<f64>,
    pub channel: Option<u64>,
    /// The words as ElevenLabs gave them, when it answered a word list.
    pub words: Option<Vec<Value>>,
    /// The entities found, under weft's names, when ElevenLabs answered
    /// them (it answers none when they were redacted).
    pub entities: Option<Vec<Value>>,
    pub exports: Vec<Export>,
    /// How many of the exports ElevenLabs answered as nothing.
    pub empty_exports: usize,
    /// The rewritten transcript, or why ElevenLabs could not write it.
    pub edited: Option<Result<String, String>>,
}

/// One subtitle or document export, decoded.
pub struct Export {
    pub format: String,
    pub extension: String,
    pub content_type: String,
    pub bytes: Vec<u8>,
}

/// Read a transcript answer: one transcript, or (`transcripts`) one per
/// channel. Pure, so the provider's answer shape is testable without a
/// socket; a transcript with no `text` is a loud refusal naming the
/// answer rather than an empty transcript.
pub fn parse_answer(answer: &Value) -> WeftResult<Answer> {
    let transcripts = match answer["transcripts"].as_array() {
        Some(transcripts) => {
            Transcripts::PerChannel(transcripts.iter().map(parse_chunk).collect::<WeftResult<Vec<_>>>()?)
        }
        None => Transcripts::One(parse_chunk(answer)?),
    };
    Ok(Answer {
        transcripts,
        transcription_id: answer["transcription_id"].as_str().map(str::to_string),
        audio_duration_secs: answer["audio_duration_secs"].as_f64(),
    })
}

fn parse_chunk(chunk: &Value) -> WeftResult<Chunk> {
    let text = chunk["text"].as_str().node_err(format!(
        "ElevenLabs answered a transcript with no text: {}",
        chunk.to_string().chars().take(300).collect::<String>()
    ))?;
    let entities = match chunk["entities"].as_array() {
        Some(found) => Some(found.iter().map(|e| detected_entity(e, 0)).collect::<WeftResult<Vec<_>>>()?),
        None => None,
    };
    let mut exports = Vec::new();
    let mut empty_exports = 0;
    for export in chunk["additional_formats"].as_array().into_iter().flatten() {
        if export.is_null() {
            empty_exports += 1;
        } else {
            exports.push(parse_export(export)?);
        }
    }
    let edited = match &chunk["edited_transcript"] {
        Value::Null => None,
        edit => Some(match edit["kind"].as_str() {
            Some("transcript") => Ok(edit["text"]
                .as_str()
                .node_err("ElevenLabs answered an edited transcript with no text")?
                .to_string()),
            Some("error") => Err(format!(
                "{}: {}",
                edit["error_type"].as_str().unwrap_or("no error type given"),
                edit["message"].as_str().unwrap_or("no message")
            )),
            _ => Err(format!("ElevenLabs answered an edit this node does not know: {edit}")),
        }),
    };
    Ok(Chunk {
        text: text.to_string(),
        language: chunk["language_code"].as_str().map(str::to_string),
        language_probability: chunk["language_probability"].as_f64(),
        channel: chunk["channel_index"].as_u64(),
        words: chunk["words"].as_array().cloned(),
        entities,
        exports,
        empty_exports,
        edited,
    })
}

fn parse_export(export: &Value) -> WeftResult<Export> {
    let field = move |name: &str| {
        export[name].as_str().node_err(format!("ElevenLabs answered an exported format with no {name}: {export}"))
    };
    let content = field("content")?;
    let bytes = if export["is_base64_encoded"].as_bool() == Some(true) {
        base64::engine::general_purpose::STANDARD.decode(content).node_err("decode an exported format")?
    } else {
        content.as_bytes().to_vec()
    };
    Ok(Export {
        format: field("requested_format")?.to_string(),
        extension: field("file_extension")?.trim_start_matches('.').to_string(),
        content_type: field("content_type")?.to_string(),
        bytes,
    })
}

/// What the `transcript` file holds: every word of every transcript in
/// the answer's order (each saying its channel when the recording was
/// split), and either the one transcript's text, entities and rewrite,
/// or one entry per channel. A part the answer does not carry is left
/// out.
pub fn transcript_document(transcripts: &Transcripts) -> Value {
    let mut document = Map::new();
    let chunks = transcripts.chunks();
    if chunks.iter().any(|c| c.words.is_some()) {
        let words: Vec<&Value> = chunks.iter().filter_map(|c| c.words.as_ref()).flatten().collect();
        document.insert("words".into(), json!(words));
    }
    match transcripts {
        Transcripts::One(chunk) => {
            document.insert("text".into(), json!(chunk.text));
            if let Some(entities) = &chunk.entities {
                document.insert("entities".into(), json!(entities));
            }
            if let Some(Ok(edited)) = &chunk.edited {
                document.insert("editedText".into(), json!(edited));
            }
        }
        Transcripts::PerChannel(channels) => {
            document.insert("channels".into(), Value::Array(channels.iter().map(channel_value).collect()));
        }
    }
    Value::Object(document)
}

/// One channel's transcript, as an entry of the `transcript` file's
/// `channels`.
fn channel_value(chunk: &Chunk) -> Value {
    let mut value = json!({ "text": chunk.text });
    if let Some(channel) = chunk.channel {
        value["channel"] = json!(channel);
    }
    if let Some(language) = &chunk.language {
        value["language"] = json!(language);
    }
    if let Some(probability) = chunk.language_probability {
        value["languageProbability"] = json!(probability);
    }
    if let Some(entities) = &chunk.entities {
        value["entities"] = json!(entities);
    }
    value
}

#[async_trait]
impl Node for ElevenLabsTranscribeFileNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let audio: Option<FileHandle> = ctx.inputs.opt("audio")?;
        let source_url = text_input(&ctx, "sourceUrl")?;
        let settings = Settings::read(&ctx)?;
        // Everything ElevenLabs would refuse, refused before the recording
        // is read or anything is sent.
        let recording = match (audio, source_url) {
            (Some(file), None) => Recording::File(file),
            (None, Some(url)) => Recording::Url(url),
            (Some(_), Some(_)) => {
                return Err(WeftError::Input(
                    "both `audio` and `sourceUrl` are given: give the recording one way".into(),
                ))
            }
            (None, None) => {
                return Err(WeftError::Input("there is nothing to transcribe: give `audio` or a `sourceUrl`".into()))
            }
        };
        if let Some(refusal) = settings.refusal() {
            return Err(WeftError::Input(refusal));
        }

        let form = match recording {
            Recording::File(file) => {
                let (meta, bytes) = ctx.storage(StorageScope::Run).get_bytes(&file).await?;
                Multipart::form_data().file("file", &meta.filename, &meta.mime_type, bytes)
            }
            Recording::Url(url) => Multipart::form_data().text("source_url", &url),
        };
        let (content_type, body) = settings.form(form)?.build();
        let mut url = format!("{API}/speech-to-text");
        if !settings.enable_logging {
            url.push_str("?enable_logging=false");
        }

        let http = ctx.client(&account).await?;
        let answer = json_call(
            http.post(url).header("content-type", content_type).body(body),
            "transcribe the recording on ElevenLabs",
        )
        .await?;
        let answer = parse_answer(&answer)?;
        emit(&ctx, answer).await
    }
}

/// Where the recording to transcribe comes from.
enum Recording {
    /// A file in the run's storage, uploaded with the request.
    File(FileHandle),
    /// A public link ElevenLabs fetches the recording from itself.
    Url(String),
}

/// Emit a parsed answer on the ports: the transcript and the exports as
/// stored files, the small facts about the recording as values, and a
/// log line for what ElevenLabs could not produce.
async fn emit(ctx: &RunContext, answer: Answer) -> WeftResult<()> {
    let storage = ctx.storage(StorageScope::Run);
    let mut out = NodeOutput::new();
    if let Some(id) = &answer.transcription_id {
        out = out.set("transcriptionId", id.as_str());
    }
    if let Some(seconds) = answer.audio_duration_secs {
        out = out.set("audioDurationSecs", seconds);
    }

    let per_channel = matches!(answer.transcripts, Transcripts::PerChannel(_));
    let mut format_files = Vec::new();
    let mut empty_exports = 0;
    for (position, chunk) in answer.transcripts.chunks().iter().enumerate() {
        empty_exports += chunk.empty_exports;
        for export in &chunk.exports {
            let (filename, channel) = if per_channel {
                let channel = chunk.channel.unwrap_or(position as u64);
                (format!("transcript_channel_{channel}.{}", export.extension), chunk.channel)
            } else {
                (format!("transcript.{}", export.extension), None)
            };
            let file = storage
                .put(export.bytes.clone(), &export.content_type, &filename, None)
                .await?;
            let mut entry = json!({ "format": export.format, "file": file });
            if let Some(channel) = channel {
                entry["channel"] = json!(channel);
            }
            format_files.push(entry);
        }
    }
    if empty_exports > 0 {
        ctx.log(
            LogLevel::Warn,
            format!("ElevenLabs answered {empty_exports} of the asked `additionalFormats` with nothing"),
        )
        .await?;
    }
    if !format_files.is_empty() {
        out = out.set("formatFiles", Value::Array(format_files));
    }

    // Hours of speech are more than a wire carries, so the transcript
    // always travels as a file, like the exports.
    let file = store_json(ctx, &transcript_document(&answer.transcripts), "transcript.json").await?;
    out = out.set("transcript", file);

    // Per channel, each channel's language is in its entry of the file.
    if let Transcripts::One(chunk) = &answer.transcripts {
        if let Some(language) = &chunk.language {
            out = out.set("language", language.as_str());
        }
        if let Some(probability) = chunk.language_probability {
            out = out.set("languageProbability", probability);
        }
        if let Some(Err(why)) = &chunk.edited {
            ctx.log(
                LogLevel::Warn,
                format!(
                    "ElevenLabs could not rewrite the transcript as `transcriptEdit` asks ({why}); the transcript file \
                     has the transcript as spoken and no `editedText`"
                ),
            )
            .await?
        }
    }
    ctx.pulse_downstream(out).await
}
