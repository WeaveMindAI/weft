//! ElevenLabsTranscribeFile self-tests: the multipart form with every
//! setting under ElevenLabs's names, and every port and the stored transcript file filled from canned
//! answers (one transcript, one per channel, a rewrite and a failed one);
//! one real transcription of a freshly spoken sentence on the live tier.
//! What ElevenLabs would refuse in the settings is the metadata rules',
//! which this rig does not run. The request streams the recording out
//! of storage, so the fake rig cannot read it: the form is checked as
//! `Settings` builds it.

use base64::Engine as _;
use serde_json::{json, Value};

use weft::access::client::Multipart;
use weft::storage::StoredFile;
use weft::{FakeRig, LiveRig, NodeManifest, NodeTest, ValueBag, WeftResult};

use crate::testing::emitted_file;

use super::{parse_answer, transcript_document, ElevenLabsTranscribeFileNode, ExportItem, Settings, Transcripts};

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::basic("an_answer_without_text_is_refused", answer_shape),
        NodeTest::basic("the_transcript_file_holds_only_what_the_answer_carries", document_parts),
        NodeTest::basic("export_formats_go_under_elevenlabs_names", export_formats),
        NodeTest::basic("a_bare_form_carries_only_the_model", bare_form),
        NodeTest::fake("streams_the_file_and_emits_the_transcript", transcribes),
        NodeTest::basic("every_speaker_and_entity_setting_reaches_the_form_under_elevenlabs_names", every_setting),
        NodeTest::fake("a_link_goes_instead_of_a_file", link),
        NodeTest::basic("the_channel_settings_reach_the_form", channels),
        NodeTest::basic("a_rewrite_and_a_speaker_count_reach_the_form", rewrite_setting),
        NodeTest::fake("zero_retention_rides_the_query", zero_retention),
        NodeTest::fake("the_answer_fills_every_port_and_the_transcript_file", every_port),
        NodeTest::fake("a_multichannel_answer_comes_out_per_channel", per_channel),
        NodeTest::fake("a_combined_multichannel_answer_is_one_transcript", combined),
        NodeTest::fake("a_rewritten_transcript_lands_in_the_transcript_file", rewritten),
        NodeTest::fake("a_failed_rewrite_is_logged_and_the_transcript_still_lands", rewrite_failed),
        NodeTest::live("one_real_transcription_of_generated_speech", "elevenlabs", live_transcribe),
    ]
}

const STT: &str = "/v1/speech-to-text";

/// The inputs of a transcription of `note.ogg`, plus `extra`.
fn inputs(rig: &FakeRig, extra: Value) -> Value {
    let mut base = json!({
        "account": rig.access("elevenlabs"),
        "audio": rig.store_file("note.ogg", "audio/ogg", b"OggS-bytes".to_vec()),
    });
    for (k, v) in extra.as_object().expect("extra inputs are an object") {
        base[k] = v.clone();
    }
    base
}

/// The multipart form `Settings` builds from the node's defaults plus
/// `extra`, as text.
fn form(extra: Value) -> WeftResult<String> {
    let mut values = serde_json::Map::new();
    for spec in &ElevenLabsTranscribeFileNode.manifest().inputs {
        if let Some(default) = &spec.default {
            values.insert(spec.name.clone(), default.clone());
        }
    }
    for (k, v) in extra.as_object().expect("extra inputs are an object") {
        values.insert(k.clone(), v.clone());
    }
    let settings = Settings::read(&ValueBag::inputs(values, Default::default(), Vec::new()))?;
    let (_, body) = settings.form(Multipart::form_data()).build();
    Ok(String::from_utf8(body).expect("a form of text fields is text"))
}

/// Whether the multipart `form` holds a text field `name` set to `value`.
fn has_field(form: &str, name: &str, value: &str) -> bool {
    form.contains(&format!("name=\"{name}\"\r\n\r\n{value}\r\n"))
}

fn has_name(form: &str, name: &str) -> bool {
    form.contains(&format!("name=\"{name}\""))
}

/// The `transcript` file a run emitted, parsed; loud unless it is the
/// JSON file `transcript.json`.
fn transcript(rig: &FakeRig, outputs: &serde_json::Map<String, Value>) -> WeftResult<Value> {
    let (bytes, filename, mime) = emitted_file(rig, outputs, "transcript")?;
    assert_eq!((filename.as_str(), mime.as_str()), ("transcript.json", "application/json"));
    Ok(serde_json::from_slice(&bytes).expect("the transcript is JSON"))
}

/// The bytes, stored name and type of a stored-file value.
fn stored(rig: &FakeRig, value: &Value) -> WeftResult<(Vec<u8>, String, String)> {
    let file = StoredFile::from_value(value)?;
    let meta = rig.stored_meta(&file.key)?;
    Ok((rig.stored_bytes(&file.key)?.to_vec(), meta.filename, meta.mime_type))
}

fn answer_shape() -> WeftResult<()> {
    let answer = parse_answer(&json!({ "text": "hello there", "language_code": "en" }))?;
    let Transcripts::One(chunk) = &answer.transcripts else { panic!("one transcript read as per channel") };
    assert_eq!(chunk.text, "hello there");
    assert_eq!(chunk.language.as_deref(), Some("en"));
    let no_language = parse_answer(&json!({ "text": "salut" }))?;
    let Transcripts::One(chunk) = &no_language.transcripts else { panic!("one transcript read as per channel") };
    assert!(chunk.language.is_none(), "no language reported, none made up");
    assert!(chunk.entities.is_none(), "no entities answered, none made up");
    assert!(chunk.words.is_none(), "no words answered, none made up");
    let err = parse_answer(&json!({ "words": [] })).err().expect("no text is a refusal").to_string();
    assert!(err.contains("no text"), "{err}");
    let err = parse_answer(&json!({ "transcripts": [{ "text": "a" }, { "words": [] }] }))
        .err()
        .expect("a channel with no text is a refusal")
        .to_string();
    assert!(err.contains("no text"), "{err}");
    Ok(())
}

fn document_parts() -> WeftResult<()> {
    let bare = parse_answer(&json!({ "text": "salut" }))?;
    assert_eq!(transcript_document(&bare.transcripts), json!({ "text": "salut" }), "nothing made up");
    let channels = parse_answer(&json!({ "transcripts": [{ "text": "a", "channel_index": 0 }] }))?;
    assert_eq!(
        transcript_document(&channels.transcripts),
        json!({ "channels": [{ "channel": 0, "text": "a" }] }),
        "per channel, the text is in each channel's entry"
    );
    Ok(())
}

fn export_formats() -> WeftResult<()> {
    let items: Vec<ExportItem> = serde_json::from_value(json!([
        "srt",
        {
            "format": "txt",
            "includeSpeakers": false,
            "includeTimestamps": true,
            "segmentOnSilenceLongerThanS": 0.8,
            "maxSegmentDurationS": 4.5,
            "maxSegmentChars": 84,
            "maxCharactersPerLine": 42,
        },
        { "format": "pdf", "includeTimestamps": false },
    ]))
    .expect("every entry reads as the port types it");
    let options = items.into_iter().map(ExportItem::option).collect::<WeftResult<Vec<_>>>()?;
    assert_eq!(
        options,
        vec![
            json!({ "format": "srt" }),
            json!({
                "format": "txt",
                "include_speakers": false,
                "include_timestamps": true,
                "segment_on_silence_longer_than_s": 0.8,
                "max_segment_duration_s": 4.5,
                "max_segment_chars": 84,
                "max_characters_per_line": 42,
            }),
            json!({ "format": "pdf", "include_timestamps": false }),
        ]
    );
    let html: ExportItem = serde_json::from_value(json!({ "format": "html", "maxCharactersPerLine": 40 }))
        .expect("the entry reads as the port types it");
    let err = html.option().expect_err("a line length only srt and txt take").to_string();
    assert!(err.starts_with("input error") && err.contains("srt and txt only"), "{err}");
    Ok(())
}

fn bare_form() -> WeftResult<()> {
    let raw = form(json!({}))?;
    assert!(has_field(&raw, "model_id", "scribe_v2"), "the default model rides: {raw}");
    for left in [
        "language_code",
        "diarize",
        "tag_audio_events",
        "no_verbatim",
        "use_multi_channel",
        "detect_speaker_roles",
        "use_speaker_library",
        "timestamps_granularity",
        "keyterms",
        "additional_formats",
        "entity_detection",
    ] {
        assert!(!has_name(&raw, left), "nothing nobody set rides, `{left}` included: {raw}");
    }
    Ok(())
}

async fn transcribes(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", STT, json!({ "language_code": "fr", "text": "bonjour tout le monde" }));
    let outcome = rig.run(&ElevenLabsTranscribeFileNode, inputs(&rig, json!({ "language": "fr" }))).await.ok()?;
    assert_eq!(transcript(&rig, &outcome.outputs)?, json!({ "text": "bonjour tout le monde" }));
    assert_eq!(outcome.outputs["language"], json!("fr"));
    let sent = &rig.requests()[0];
    let content_type = sent.header("content-type").unwrap_or_default().to_string();
    assert!(content_type.starts_with("multipart/form-data"), "{content_type}");
    assert!(sent.body_streamed, "the recording streams out of storage, never read whole");
    assert_eq!(sent.query, None, "logging stays on unless turned off");
    Ok(())
}

fn every_setting() -> WeftResult<()> {
    let raw = form(json!({
        "model": "scribe_v2_medical",
        "language": "eng",
        "diarize": true,
        "diarizationThreshold": 0.3,
        "detectSpeakerRoles": true,
        "useSpeakerLibrary": true,
        "timestampsGranularity": "character",
        "tagAudioEvents": false,
        "noVerbatim": true,
        "keyterms": ["Weavemind", "weft node"],
        "additionalFormats": ["srt", { "format": "txt", "includeSpeakers": false }],
        "entityDetection": ["pii", "phi"],
        "entityRedaction": ["pii"],
        "entityRedactionMode": "redacted",
        "temperature": 0.5,
        // A wired number arrives as a float.
        "seed": 42.0,
        "fileFormat": "pcm_s16le_16",
    }))?;
    for (name, value) in [
        ("model_id", "scribe_v2_medical"),
        ("language_code", "eng"),
        ("diarize", "true"),
        ("diarization_threshold", "0.3"),
        ("detect_speaker_roles", "true"),
        ("use_speaker_library", "true"),
        ("timestamps_granularity", "character"),
        ("tag_audio_events", "false"),
        ("no_verbatim", "true"),
        ("keyterms", "Weavemind"),
        ("keyterms", "weft node"),
        ("additional_formats", r#"[{"format":"srt"},{"format":"txt","include_speakers":false}]"#),
        ("entity_detection", r#"["pii","phi"]"#),
        ("entity_redaction", r#"["pii"]"#),
        ("entity_redaction_mode", "redacted"),
        ("temperature", "0.5"),
        ("seed", "42"),
        ("file_format", "pcm_s16le_16"),
    ] {
        assert!(has_field(&raw, name, value), "`{name}` = {value} rides: {raw}");
    }
    assert_eq!(raw.matches("name=\"keyterms\"").count(), 2, "one field per term: {raw}");
    Ok(())
}

async fn link(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", STT, json!({ "text": "hi" }));
    let mut given = inputs(&rig, json!({ "sourceUrl": "https://www.youtube.com/watch?v=abc" }));
    given.as_object_mut().expect("inputs are an object").remove("audio");
    let outcome = rig.run(&ElevenLabsTranscribeFileNode, given).await.ok()?;
    assert_eq!(transcript(&rig, &outcome.outputs)?, json!({ "text": "hi" }));
    assert_eq!(rig.requests().len(), 1, "ElevenLabs fetches the link itself: one request, nothing read from storage");
    Ok(())
}

fn channels() -> WeftResult<()> {
    let raw = form(json!({
        "useMultiChannel": true,
        "multichannelOutputStyle": "separate",
        "timestampsGranularity": "none",
    }))?;
    assert!(has_field(&raw, "use_multi_channel", "true"), "{raw}");
    assert!(has_field(&raw, "multichannel_output_style", "separate"), "{raw}");
    assert!(has_field(&raw, "timestamps_granularity", "none"), "{raw}");
    Ok(())
}

fn rewrite_setting() -> WeftResult<()> {
    let raw = form(json!({ "transcriptEdit": "Write all dates as YYYY-MM-DD.", "numSpeakers": 2.0 }))?;
    assert!(has_field(&raw, "transcript_edit", "Write all dates as YYYY-MM-DD."), "{raw}");
    assert!(has_field(&raw, "num_speakers", "2"), "a speaker count goes as a whole number: {raw}");
    Ok(())
}

async fn zero_retention(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", STT, json!({ "text": "hi" }));
    rig.run(&ElevenLabsTranscribeFileNode, inputs(&rig, json!({ "enableLogging": false }))).await.ok()?;
    assert_eq!(rig.requests()[0].query.as_deref(), Some("enable_logging=false"));
    Ok(())
}

const PDF: &[u8] = b"%PDF-1.4 a transcript";

/// A single-transcript answer carrying every field the ports read.
fn full_answer() -> Value {
    json!({
        "language_code": "eng",
        "language_probability": 0.98,
        "text": "Hi Jill, call me.",
        "words": [
            { "text": "Hi", "start": 0.0, "end": 0.3, "type": "word", "speaker_id": "speaker_0", "logprob": -0.1 },
            { "text": " ", "start": 0.3, "end": 0.35, "type": "spacing", "speaker_id": "speaker_0", "logprob": 0.0 },
            { "text": "Jill,", "start": 0.35, "end": 0.8, "type": "word", "speaker_id": "speaker_0", "logprob": -0.2 },
        ],
        "transcription_id": "tr-1",
        "audio_duration_secs": 2.5,
        "entities": [{ "text": "Jill", "entity_type": "name", "start_char": 3, "end_char": 7 }],
        "additional_formats": [
            {
                "requested_format": "srt",
                "file_extension": "srt",
                "content_type": "text/srt",
                "is_base64_encoded": false,
                "content": "1\n00:00:00,000 --> 00:00:02,500\nHi Jill, call me.\n",
            },
            {
                "requested_format": "pdf",
                "file_extension": ".pdf",
                "content_type": "application/pdf",
                "is_base64_encoded": true,
                "content": base64::engine::general_purpose::STANDARD.encode(PDF),
            },
        ],
    })
}

/// Nothing reads the outputs here, and the transcript file is stored
/// anyway: it is the node's product.
async fn every_port(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", STT, full_answer());
    let outcome = rig
        .run(
            &ElevenLabsTranscribeFileNode,
            inputs(&rig, json!({ "entityDetection": ["pii"], "additionalFormats": ["srt", "pdf"] })),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["language"], json!("eng"));
    assert_eq!(outcome.outputs["languageProbability"], json!(0.98));
    assert_eq!(outcome.outputs["transcriptionId"], json!("tr-1"));
    assert_eq!(outcome.outputs["audioDurationSecs"], json!(2.5));
    assert_eq!(
        transcript(&rig, &outcome.outputs)?,
        json!({
            "text": "Hi Jill, call me.",
            "words": full_answer()["words"],
            "entities": [{ "text": "Jill", "entityType": "name", "startChar": 3, "endChar": 7 }],
        }),
        "the text, the words as ElevenLabs gave them, and the entities; no channels and no rewrite"
    );

    let files = outcome.outputs["formatFiles"].as_array().expect("formatFiles is a list");
    assert_eq!(files.len(), 2);
    assert_eq!(files[0]["format"], json!("srt"));
    let (srt, name, mime) = stored(&rig, &files[0]["file"])?;
    assert_eq!((name.as_str(), mime.as_str()), ("transcript.srt", "text/srt"));
    assert!(String::from_utf8_lossy(&srt).contains("Hi Jill, call me."));
    assert_eq!(files[1]["format"], json!("pdf"));
    let (pdf, name, mime) = stored(&rig, &files[1]["file"])?;
    assert_eq!(pdf, PDF, "a base64 export is stored decoded");
    assert_eq!((name.as_str(), mime.as_str()), ("transcript.pdf", "application/pdf"), "one dot, whatever the answer wrote");
    assert!(files.iter().all(|f| f.get("channel").is_none()), "one transcript has no channel");
    Ok(())
}

async fn per_channel(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "POST",
        STT,
        json!({
            "transcripts": [
                {
                    "language_code": "eng",
                    "language_probability": 0.98,
                    "text": "Hello from channel one.",
                    "channel_index": 0,
                    "words": [{ "text": "Hello", "start": 0.0, "end": 0.5, "type": "word", "speaker_id": "speaker_0", "channel_index": 0, "logprob": -0.1 }],
                    "entities": [],
                    "additional_formats": [{ "requested_format": "txt", "file_extension": "txt", "content_type": "text/plain", "is_base64_encoded": false, "content": "Hello from channel one." }],
                },
                {
                    "language_code": "fra",
                    "language_probability": 0.97,
                    "text": "Bonjour du canal deux.",
                    "channel_index": 1,
                    "words": [{ "text": "Bonjour", "start": 0.1, "end": 0.7, "type": "word", "speaker_id": "speaker_1", "channel_index": 1, "logprob": -0.2 }],
                    "entities": [{ "text": "deux", "entity_type": "cardinal", "start_char": 17, "end_char": 21 }],
                    "additional_formats": [{ "requested_format": "txt", "file_extension": "txt", "content_type": "text/plain", "is_base64_encoded": false, "content": "Bonjour du canal deux." }],
                },
            ],
            "transcription_id": "tr-2",
            "audio_duration_secs": 12.0,
        }),
    );
    let outcome = rig
        .run(
            &ElevenLabsTranscribeFileNode,
            inputs(&rig, json!({ "useMultiChannel": true, "entityDetection": ["all"], "additionalFormats": ["txt"] })),
        )
        .await
        .ok()?;
    let document = transcript(&rig, &outcome.outputs)?;
    assert_eq!(
        document["channels"],
        json!([
            { "channel": 0, "text": "Hello from channel one.", "language": "eng", "languageProbability": 0.98, "entities": [] },
            {
                "channel": 1,
                "text": "Bonjour du canal deux.",
                "language": "fra",
                "languageProbability": 0.97,
                "entities": [{ "text": "deux", "entityType": "cardinal", "startChar": 17, "endChar": 21 }],
            },
        ])
    );
    for single in ["text", "entities"] {
        assert!(document.get(single).is_none(), "per channel, `{single}` is in each channel's entry");
    }
    for single in ["language", "languageProbability"] {
        assert!(!outcome.outputs.contains_key(single), "per channel, `{single}` is in each channel's entry");
    }
    assert_eq!(outcome.outputs["transcriptionId"], json!("tr-2"));
    assert_eq!(outcome.outputs["audioDurationSecs"], json!(12.0));

    let spoken: Vec<(&str, u64)> = document["words"]
        .as_array()
        .expect("a list")
        .iter()
        .map(|w| (w["text"].as_str().unwrap_or_default(), w["channel_index"].as_u64().unwrap_or(99)))
        .collect();
    assert_eq!(spoken, vec![("Hello", 0), ("Bonjour", 1)], "every channel's words, each saying its channel");

    let files = outcome.outputs["formatFiles"].as_array().expect("formatFiles is a list");
    assert_eq!(files.len(), 2, "one export per channel");
    for (i, entry) in files.iter().enumerate() {
        assert_eq!(entry["channel"], json!(i));
        let (_, name, _) = stored(&rig, &entry["file"])?;
        assert_eq!(name, format!("transcript_channel_{i}.txt"));
    }
    Ok(())
}

async fn combined(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "POST",
        STT,
        json!({
            "language_code": "eng",
            "language_probability": 0.98,
            "text": "Hello from channel one. Greetings from channel two.",
            "words": [
                { "text": "Hello", "start": 0.0, "end": 0.5, "type": "word", "speaker_id": "speaker_0", "channel_index": 0, "logprob": -0.1 },
                { "text": "Greetings", "start": 0.6, "end": 1.1, "type": "word", "speaker_id": "speaker_1", "channel_index": 1, "logprob": -0.1 },
            ],
        }),
    );
    let outcome = rig
        .run(
            &ElevenLabsTranscribeFileNode,
            inputs(&rig, json!({ "useMultiChannel": true, "multichannelOutputStyle": "combined" })),
        )
        .await
        .ok()?;
    let document = transcript(&rig, &outcome.outputs)?;
    assert_eq!(document["text"], json!("Hello from channel one. Greetings from channel two."));
    assert!(document.get("channels").is_none(), "combined is one transcript");
    assert_eq!(document["words"].as_array().map(Vec::len), Some(2), "both channels' words, in time order");
    Ok(())
}

async fn rewritten(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "POST",
        STT,
        json!({
            "text": "we meet on the twelfth of July",
            "edited_transcript": { "kind": "transcript", "text": "we meet on 2026-07-12" },
        }),
    );
    let outcome = rig
        .run(&ElevenLabsTranscribeFileNode, inputs(&rig, json!({ "transcriptEdit": "Write dates as YYYY-MM-DD." })))
        .await
        .ok()?;
    let document = transcript(&rig, &outcome.outputs)?;
    assert_eq!(document["editedText"], json!("we meet on 2026-07-12"));
    assert_eq!(document["text"], json!("we meet on the twelfth of July"), "the transcript stays as spoken");
    assert!(rig.logs().is_empty(), "{:?}", rig.logs());
    Ok(())
}

async fn rewrite_failed(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "POST",
        STT,
        json!({
            "text": "we meet on the twelfth of July",
            "edited_transcript": { "kind": "error", "error_type": "edit_failed", "message": "The instruction could not be applied." },
        }),
    );
    let outcome = rig
        .run(&ElevenLabsTranscribeFileNode, inputs(&rig, json!({ "transcriptEdit": "Write dates as YYYY-MM-DD." })))
        .await
        .ok()?;
    let document = transcript(&rig, &outcome.outputs)?;
    assert_eq!(document["text"], json!("we meet on the twelfth of July"), "the paid transcript still lands");
    assert!(document.get("editedText").is_none(), "no rewrite, none in the file");
    let logs = rig.logs();
    assert_eq!(logs.len(), 1, "{logs:?}");
    assert_eq!(logs[0].0, weft::context::LogLevel::Warn);
    assert!(
        logs[0].1.contains("edit_failed") && logs[0].1.contains("The instruction could not be applied."),
        "{}",
        logs[0].1
    );
    Ok(())
}

/// One real transcription on scribe_v2: a stock voice speaks a sentence
/// through the flash TTS route, the file goes through Scribe, and the
/// transcript comes back as a JSON file. Nothing lands on the account.
async fn live_transcribe(rig: LiveRig) -> WeftResult<()> {
    const SENTENCE: &str = "The quick brown fox jumps over the lazy dog.";
    let conn = rig.connect().await?;
    let voice = crate::testing::first_voice_id(&conn).await?;
    let sample = crate::testing::speech_sample(&conn, &voice, SENTENCE).await?;
    let audio = rig.store_file("sample.mp3", "audio/mpeg", sample).await?;
    let outcome = rig
        .run(
            &ElevenLabsTranscribeFileNode,
            json!({ "account": rig.access("elevenlabs"), "audio": audio, "language": "en", "model": "scribe_v2" }),
        )
        .await
        .ok()?;
    let file = StoredFile::from_value(outcome.output("transcript")?)?;
    assert_eq!((file.filename.as_str(), file.mime_type.as_str()), ("transcript.json", "application/json"));
    let transcript: Value = serde_json::from_slice(&rig.stored_bytes(outcome.output("transcript")?).await?)
        .map_err(|e| weft::node_error(format!("the transcript file is not JSON: {e}")))?;
    let text = transcript["text"].as_str().unwrap_or_default().to_lowercase();
    assert!(text.contains("fox") && text.contains("lazy dog"), "the sentence comes back: {text:?}");
    assert!(transcript["words"].as_array().is_some_and(|w| !w.is_empty()), "with its word timings");
    Ok(())
}
