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
use serde::Deserialize;
use serde_json::{json, Map, Value};

use weft::access::client::{json_call, url, Multipart, StoredBodyExt};
use weft::context::LogLevel;
use weft::node::NodeOutput;
use weft::storage::{FileHandle, StorageScope};
use weft::{Access, RunContext, Node, NodeErrExt, NodeManifest, ValueBag, WeftResult};

use super::elevenlabs::{detected_entity, API};

#[derive(NodeManifest)]
pub struct ElevenLabsTranscribeFileNode;

#[cfg(feature = "node-tests")]
mod tests;

/// The export formats that also take a line length.
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
    fn read(inputs: &ValueBag) -> WeftResult<Self> {
        Ok(Self {
            model: inputs.get("model")?,
            language: inputs.opt("language")?,
            diarize: inputs.get("diarize")?,
            // Whole numbers: the widgets' step holds a wired value to
            // whole numbers too.
            num_speakers: inputs.opt("numSpeakers")?,
            diarization_threshold: inputs.opt("diarizationThreshold")?,
            detect_speaker_roles: inputs.get("detectSpeakerRoles")?,
            use_speaker_library: inputs.get("useSpeakerLibrary")?,
            timestamps_granularity: inputs.opt("timestampsGranularity")?,
            tag_audio_events: inputs.get("tagAudioEvents")?,
            no_verbatim: inputs.get("noVerbatim")?,
            keyterms: inputs.list("keyterms")?,
            additional_formats: inputs
                .list::<ExportItem>("additionalFormats")?
                .into_iter()
                .map(ExportItem::option)
                .collect::<WeftResult<_>>()?,
            entity_detection: inputs.list("entityDetection")?,
            entity_redaction: inputs.list("entityRedaction")?,
            entity_redaction_mode: inputs.opt("entityRedactionMode")?,
            transcript_edit: inputs.opt("transcriptEdit")?,
            use_multi_channel: inputs.get("useMultiChannel")?,
            multichannel_output_style: inputs.opt("multichannelOutputStyle")?,
            temperature: inputs.opt("temperature")?,
            seed: inputs.opt("seed")?,
            file_format: inputs.opt("fileFormat")?,
            enable_logging: inputs.get("enableLogging")?,
        })
    }

    /// The multipart form ElevenLabs reads, under its own field names:
    /// every setting that was given, nothing that was left at
    /// ElevenLabs's own default.
    fn form(&self, mut form: Multipart) -> Multipart {
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
            form = form.text("additional_formats", &json!(self.additional_formats).to_string());
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
        form
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

/// One `additionalFormats` entry, as the port types it: a format's
/// name, or the format with its layout options.
#[derive(Deserialize)]
#[serde(untagged)]
pub enum ExportItem {
    Name(String),
    Options(ExportOptions),
}

/// An `additionalFormats` entry with its layout options, under weft's
/// names.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExportOptions {
    format: String,
    include_speakers: Option<bool>,
    include_timestamps: Option<bool>,
    segment_on_silence_longer_than_s: Option<f64>,
    max_segment_duration_s: Option<f64>,
    max_segment_chars: Option<u64>,
    max_characters_per_line: Option<u64>,
}

impl ExportItem {
    /// The entry as ElevenLabs reads it in `additional_formats`, under its
    /// own names, with only the options that were given.
    pub fn option(self) -> WeftResult<Value> {
        let options = match self {
            ExportItem::Name(format) => return Ok(json!({ "format": format })),
            ExportItem::Options(options) => options,
        };
        if options.max_characters_per_line.is_some() && !LINE_LENGTH_FORMATS.contains(&options.format.as_str()) {
            weft::input_bail!(
                "ElevenLabs takes `maxCharactersPerLine` on srt and txt only, and an `additionalFormats` entry is {}: \
                 leave it out",
                options.format
            );
        }
        let mut option = json!({ "format": options.format });
        for (key, value) in [
            ("include_speakers", options.include_speakers.map(Value::from)),
            ("include_timestamps", options.include_timestamps.map(Value::from)),
            ("segment_on_silence_longer_than_s", options.segment_on_silence_longer_than_s.map(Value::from)),
            ("max_segment_duration_s", options.max_segment_duration_s.map(Value::from)),
            ("max_segment_chars", options.max_segment_chars.map(Value::from)),
            ("max_characters_per_line", options.max_characters_per_line.map(Value::from)),
        ] {
            if let Some(value) = value {
                option[key] = value;
            }
        }
        Ok(option)
    }
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
        let settings = Settings::read(&ctx.inputs)?;
        // One of `audio` and `sourceUrl` arrives: the rules refuse both,
        // and a run with neither skips this node.
        let form = match ctx.inputs.opt::<FileHandle>("audio")? {
            Some(file) => Multipart::form_data().stored("file", ctx.storage(StorageScope::Run).body(&file).await?),
            None => Multipart::form_data().text("source_url", &ctx.inputs.get::<String>("sourceUrl")?),
        };
        let address = url(API, &["speech-to-text"])?.query_opt("enable_logging", (!settings.enable_logging).then_some("false"));

        let http = ctx.client(&account).await?;
        let answer = json_call(
            http.post(address.as_str()).multipart(settings.form(form)),
            "transcribe the recording on ElevenLabs",
        )
        .await?;
        let answer = parse_answer(&answer)?;
        emit(&ctx, answer).await
    }
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
    let file = storage.put_json(&transcript_document(&answer.transcripts), "transcript.json", None).await?;
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
