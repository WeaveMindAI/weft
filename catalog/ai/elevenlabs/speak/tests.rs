//! ElevenLabsSpeak self-tests. The fake rig has no provider WebSocket,
//! so the session is tested through its parts: the URL and the opener
//! a run's inputs make, the frame vocabulary, and the read that turns
//! frames into bus messages. The live test drives the whole socket.

use base64::Engine as _;
use serde_json::{json, Value};

use weft::bus::BusOptions;
use weft::{FakeRig, LiveRig, NodeManifest, NodeTest, ValueBag, WeftResult, Widget};

use super::super::elevenlabs::audio_file_type;
use super::{
    bus_meta, byte_rate, classify_session_frame, placed, read_frame, text_of, Character,
    ElevenLabsSpeakNode, Session,
};

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::basic("text_extraction_reads_both_bus_shapes", texts),
        NodeTest::basic("bus_metadata_matches_the_output_format", meta),
        NodeTest::basic("session_frames_classify_by_their_vocabulary", frames),
        NodeTest::basic("every_declared_output_format_has_a_file_type", file_types),
        NodeTest::basic("a_bare_session_sends_only_what_it_must", bare_session),
        NodeTest::basic("every_setting_reaches_the_url_or_the_opener", every_setting),
        NodeTest::basic("a_voice_id_can_only_ever_name_a_voice", voice_id_encoded),
        NodeTest::basic("characters_land_on_the_whole_speech", characters_placed),
        NodeTest::basic("each_frames_timing_lands_on_one_clock", timing_relay),
        NodeTest::fake("accepted_settings_pass_every_refusal", accepted),
        NodeTest::live("one_real_streamed_speech", "elevenlabs", live_stream),
    ]
}

/// One real streaming session: two text chunks on a live bus, spoken
/// through the stream-input websocket with a few settings and the
/// `alignment` output wired; the stored full-audio file proves the
/// chunks synthesized and the drain ran to the final frame, and every
/// frame's timing was parsed on the way (a frame shape the node
/// misreads fails the run). Nothing lands on the account.
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

    rig.wire_output("alignment");
    let outcome = rig
        .run(
            &ElevenLabsSpeakNode,
            json!({
                "account": rig.access("elevenlabs"),
                "text": marker,
                "voice": voice,
                "model": "eleven_flash_v2_5",
                "outputFormat": "pcm_16000",
                "stability": 0.5,
                "languageCode": "en",
                "seed": 7,
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
    let frame = classify_session_frame(r#"{"audio":"aGk=","isFinal":null}"#, false)?;
    assert_eq!(frame.audio, b"hi");
    assert!(!frame.is_final);
    assert!(frame.characters.is_none());
    // The final frame carries a null audio and ends the generation.
    let frame = classify_session_frame(r#"{"audio":null,"isFinal":true}"#, false)?;
    assert!(frame.audio.is_empty());
    assert!(frame.is_final);
    // A timed session reads the characters the frame speaks.
    let frame = classify_session_frame(
        r#"{"audio":"aGk=","alignment":{"chars":["h","i"],"charStartTimesMs":[0,125],"charDurationsMs":[125,250]}}"#,
        true,
    )?;
    assert_eq!(
        frame.characters,
        Some(vec![
            Character { text: "h".into(), start_ms: 0.0, duration_ms: 125.0 },
            Character { text: "i".into(), start_ms: 125.0, duration_ms: 250.0 },
        ])
    );
    // A frame with no timing carries none, and an untimed session never
    // reads it, so a timing nobody asked for cannot fail the speech.
    assert!(classify_session_frame(r#"{"audio":"aGk=","alignment":null}"#, true)?.characters.is_none());
    assert!(classify_session_frame(r#"{"audio":"aGk=","alignment":{"chars":["h"]}}"#, false)?
        .characters
        .is_none());
    // A timing whose lists disagree is an error, never a guess.
    let err = classify_session_frame(
        r#"{"audio":"aGk=","alignment":{"chars":["h","i"],"charStartTimesMs":[0],"charDurationsMs":[125,250]}}"#,
        true,
    )
    .err()
    .expect("lists of different lengths must refuse")
    .to_string();
    assert!(err.contains("2 characters, 1 start times and 2 durations"), "{err}");
    // A refusal surfaces the provider's own words.
    let err = classify_session_frame(r#"{"error":"quota exceeded"}"#, false)
        .err()
        .expect("a refusal frame must refuse")
        .to_string();
    assert!(err.contains("quota exceeded"), "{err}");
    // Garbage is an error, never a silent skip.
    assert!(classify_session_frame("not json", false).is_err());
    Ok(())
}

fn meta() -> WeftResult<()> {
    assert_eq!(
        bus_meta("pcm_16000"),
        json!({ "sample_rate": 16000, "encoding": "pcm_s16le" })
    );
    assert_eq!(bus_meta("ulaw_8000"), json!({ "sample_rate": 8000, "encoding": "ulaw" }));
    assert_eq!(bus_meta("alaw_8000"), json!({ "sample_rate": 8000, "encoding": "alaw" }));
    assert_eq!(bus_meta("mp3_44100_128"), json!({ "encoding": "mp3_44100_128" }));
    Ok(())
}

/// Every format the metadata offers stores under a real extension and
/// mime, so none of them can be paid for and then fail to store.
fn file_types() -> WeftResult<()> {
    let spec = ElevenLabsSpeakNode
        .manifest()
        .inputs
        .iter()
        .find(|i| i.name == "outputFormat")
        .expect("the node declares `outputFormat`");
    let Some(Widget::Select { options, .. }) = &spec.widget else {
        panic!("`outputFormat` is a select");
    };
    for format in options {
        audio_file_type(format)?;
    }
    Ok(())
}

/// The inputs a run reads: the metadata's defaults, then `extra`.
fn bag(extra: Value) -> ValueBag {
    let mut values = serde_json::Map::new();
    for spec in &ElevenLabsSpeakNode.manifest().inputs {
        if let Some(default) = &spec.default {
            values.insert(spec.name.clone(), default.clone());
        }
    }
    for (k, v) in extra.as_object().expect("extra inputs are an object") {
        values.insert(k.clone(), v.clone());
    }
    ValueBag::inputs(values, Default::default(), Vec::new())
}

const SOCKET: &str = "wss://api.elevenlabs.io/v1/text-to-speech";

fn bare_session() -> WeftResult<()> {
    let session = Session::read(&bag(json!({})), None, false)?;
    assert_eq!(
        session.url("voice-1")?.to_string(),
        format!(
            "{SOCKET}/voice-1/stream-input?model_id=eleven_flash_v2_5&output_format=pcm_16000\
             &inactivity_timeout=180&auto_mode=true"
        )
    );
    assert_eq!(session.opener(), json!({ "text": " " }), "nothing the person did not set");
    assert!(session.timing.is_none(), "no timing when nothing reads `alignment`");
    Ok(())
}

fn every_setting() -> WeftResult<()> {
    let session = Session::read(
        &bag(json!({
            "model": "eleven_flash_v2",
            "outputFormat": "ulaw_8000",
            "languageCode": "en",
            "enableLogging": false,
            "textNormalization": "on",
            "seed": 42,
            "enableSsmlParsing": true,
            "autoMode": false,
            "chunkLengthSchedule": [50, 120, 160, 290],
            "pronunciationDictionaries": [{ "id": "dict-a", "version": "v3" }],
        })),
        Some(json!({ "stability": 0.4, "use_speaker_boost": false })),
        true,
    )?;
    assert_eq!(
        session.url("voice-1")?.to_string(),
        format!(
            "{SOCKET}/voice-1/stream-input?model_id=eleven_flash_v2&output_format=ulaw_8000\
             &inactivity_timeout=180&language_code=en&enable_logging=false&apply_text_normalization=on\
             &seed=42&enable_ssml_parsing=true&sync_alignment=true"
        ),
        "auto mode off is ElevenLabs's own default, so it is left out"
    );
    assert_eq!(
        session.opener(),
        json!({
            "text": " ",
            "voice_settings": { "stability": 0.4, "use_speaker_boost": false },
            "generation_config": { "chunk_length_schedule": [50.0, 120.0, 160.0, 290.0] },
            "pronunciation_dictionary_locators": [
                { "pronunciation_dictionary_id": "dict-a", "version_id": "v3" },
            ],
        })
    );
    assert_eq!(session.timing, Some(8_000.0), "ulaw is one byte a sample");
    Ok(())
}

/// A typed voice id that looks like a path stays one path segment, so
/// it cannot reach another ElevenLabs route.
fn voice_id_encoded() -> WeftResult<()> {
    let url = Session::read(&bag(json!({})), None, false)?.url("../user")?;
    assert!(url.as_str().starts_with(&format!("{SOCKET}/..%2Fuser/stream-input?")), "{url}");
    Ok(())
}

fn characters_placed() -> WeftResult<()> {
    assert_eq!(byte_rate("pcm_16000"), Some(32_000.0), "16-bit pcm is two bytes a sample");
    assert_eq!(byte_rate("ulaw_8000"), Some(8_000.0));
    assert_eq!(byte_rate("alaw_8000"), Some(8_000.0));
    assert_eq!(byte_rate("wav_16000"), None, "a wav header shifts the byte count");
    assert_eq!(byte_rate("mp3_44100_128"), None);
    assert_eq!(byte_rate("opus_48000_64"), None);
    let characters = vec![
        Character { text: "H".into(), start_ms: 0.0, duration_ms: 125.0 },
        Character { text: "i".into(), start_ms: 125.0, duration_ms: 375.0 },
    ];
    assert_eq!(
        placed(&characters, 0.5),
        json!([{ "text": "H", "start": 0.5, "end": 0.625 }, { "text": "i", "start": 0.625, "end": 1.0 }]),
        "half a second of audio came before the frame"
    );
    Ok(())
}

/// Two timed frames read for relaying: the second frame's characters
/// count from where the first frame's audio ends.
fn timing_relay() -> WeftResult<()> {
    let frame = |bytes: usize, chars: Value, starts: Value, durations: Value| {
        json!({
            "audio": base64::engine::general_purpose::STANDARD.encode(vec![0u8; bytes]),
            "alignment": { "chars": chars, "charStartTimesMs": starts, "charDurationsMs": durations },
        })
        .to_string()
    };
    // pcm_16000 is 32 000 bytes a second: the first frame is half a
    // second of audio.
    let clock = Some(32_000.0);
    let mut full = Vec::new();
    let first = read_frame(&frame(16_000, json!(["H", "i"]), json!([0, 125]), json!([125, 375])), clock, &mut full)?;
    assert!(!first.is_final);
    assert_eq!(first.audio.as_ref().map(Vec::len), Some(16_000));
    assert_eq!(first.alignment, Some(json!([{ "text": "H", "start": 0.0, "end": 0.125 }, { "text": "i", "start": 0.125, "end": 0.5 }])));
    let second = read_frame(&frame(3_200, json!(["!"]), json!([0]), json!([250])), clock, &mut full)?;
    assert_eq!(second.alignment, Some(json!([{ "text": "!", "start": 0.5, "end": 0.75 }])), "placed after every byte before it");
    let last = read_frame(r#"{"audio":null,"isFinal":true}"#, clock, &mut full)?;
    assert!(last.is_final);
    assert!(last.audio.is_none() && last.alignment.is_none(), "the final frame carries no audio and no timing");
    assert_eq!(full.len(), 19_200, "every frame's audio is in the full copy");
    // Without the clock (timing not asked for), no frame carries timing.
    let untimed = read_frame(&frame(10, json!(["x"]), json!([0]), json!([1])), None, &mut Vec::new())?;
    assert!(untimed.alignment.is_none());
    Ok(())
}

/// The inputs of a short session on a fresh text bus, plus `extra`.
fn inputs(rig: &FakeRig, extra: Value) -> WeftResult<Value> {
    let (_text, marker) = rig.seed_bus(BusOptions::default())?;
    let mut base = json!({
        "account": rig.access("elevenlabs"),
        "text": marker,
        "voice": "v1",
    });
    for (k, v) in extra.as_object().expect("extra inputs are an object") {
        base[k] = v.clone();
    }
    Ok(base)
}

/// Every setting at once, through the real manifest and the run's own
/// reading: nothing is refused, and the run goes on to open the session
/// (where the fake rig, which has no provider WebSocket, stops it).
async fn accepted(rig: FakeRig) -> WeftResult<()> {
    rig.wire_output("alignment");
    let err = rig
        .run(
            &ElevenLabsSpeakNode,
            inputs(
                &rig,
                json!({
                    "model": "eleven_flash_v2",
                    "outputFormat": "alaw_8000",
                    "stability": 0.4,
                    "similarityBoost": 0.8,
                    "style": 0.2,
                    "speed": 1.1,
                    "speakerBoost": false,
                    "languageCode": "en",
                    "textNormalization": "on",
                    "seed": 42,
                    "pronunciationDictionaries": [{ "id": "dict-a", "version": "v3" }],
                    "enableSsmlParsing": true,
                    "autoMode": false,
                    "chunkLengthSchedule": [50, 120],
                    "enableLogging": false,
                }),
            )?,
        )
        .await
        .failure()?;
    assert!(!err.starts_with("input error"), "no setting was refused: {err}");
    assert!(err.contains("WebSocket"), "the run got as far as opening the session: {err}");
    Ok(())
}
