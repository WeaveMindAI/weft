//! ElevenLabsTranscribe self-tests: the server-frame classifier and the
//! transcript it builds, with the file it stores (the node's pure
//! decision core), the session URL with every setting under
//! ElevenLabs's names, the audio formats a bus
//! can declare, plus one real realtime session. What ElevenLabs would
//! refuse in the settings is the metadata rules', which this rig does not
//! run. The socket session has no fake form yet, so the
//! streaming path's free coverage is the mechanism e2es.

use serde_json::{json, Value};

use weft::bus::{BusOptions, BusPayloadKind};
use weft::storage::StoredFile;
use weft::{LiveRig, NodeTest, WeftResult};

use super::{audio_chunk, audio_format, classify_server_frame, ServerWord, Session, Transcript};

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
        NodeTest::basic("a_timestamped_frame_carries_the_words_and_language_of_its_segment", timestamped),
        NodeTest::basic("an_entities_frame_carries_them_under_weft_names", entities_frame),
        NodeTest::basic("an_edited_frame_carries_the_rewrite_of_its_segment", edited_frame),
        NodeTest::basic("a_final_frame_only_ever_reports_a_language", final_frames),
        NodeTest::basic("bookkeeping_frames_are_ignored", || {
            for kind in ["session_started", "partial_transcript"] {
                let word = classify_server_frame(&format!(r#"{{"message_type": "{kind}", "text": ""}}"#));
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
        NodeTest::basic("a_warning_is_logged_and_the_session_goes_on", || {
            let word = classify_server_frame(r#"{"message_type": "warning", "warning": "audio is clipping"}"#);
            assert!(matches!(word, ServerWord::Warning(w) if w == "audio is clipping"));
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
        NodeTest::basic("segments_join_and_annotations_find_their_segment_by_text", annotations),
        NodeTest::basic("the_transcript_file_holds_what_the_session_asked_for", document),
        NodeTest::basic("every_session_setting_rides_the_url_under_elevenlabs_names", every_setting),
        NodeTest::basic("only_the_model_format_and_commit_ride_by_default", default_query),
        NodeTest::basic("the_bus_metadata_picks_the_audio_format", formats),
        NodeTest::basic("the_context_rides_the_chunk_it_is_given", chunks),
    ]
}

fn timestamped() -> WeftResult<()> {
    let word = classify_server_frame(
        r#"{"message_type": "committed_transcript_with_timestamps", "text": "so", "language_code": "en",
            "words": [{"text": "so", "start": 0.1, "end": 0.3, "type": "word", "logprob": -0.2}]}"#,
    );
    let ServerWord::Words { text, words, language } = word else { panic!("not read as a segment's words") };
    assert_eq!(text, "so");
    assert_eq!(words, vec![json!({ "text": "so", "start": 0.1, "end": 0.3, "type": "word", "logprob": -0.2 })]);
    assert_eq!(language.as_deref(), Some("en"));
    // Language detection alone may answer no words.
    let word = classify_server_frame(
        r#"{"message_type": "committed_transcript_with_timestamps", "text": "so", "words": null}"#,
    );
    assert!(matches!(word, ServerWord::Words { words, language: None, .. } if words.is_empty()));
    Ok(())
}

fn entities_frame() -> WeftResult<()> {
    let word = classify_server_frame(
        r#"{"message_type": "committed_transcript_entities", "text": "call Jill",
            "entities": [{"text": "Jill", "entity_type": "name", "start_char": 5, "end_char": 9}]}"#,
    );
    let ServerWord::Entities { text, entities } = word else { panic!("not read as entities") };
    assert_eq!(text, "call Jill");
    assert_eq!(entities, vec![json!({ "text": "Jill", "entityType": "name", "startChar": 5, "endChar": 9 })]);
    let word = classify_server_frame(
        r#"{"message_type": "committed_transcript_entities", "text": "call Jill", "entities": [{"text": "Jill"}]}"#,
    );
    assert!(matches!(word, ServerWord::Refused(why) if why.contains("entity_type")), "a malformed entity is loud");
    Ok(())
}

fn edited_frame() -> WeftResult<()> {
    let word = classify_server_frame(
        r#"{"message_type": "edited_transcript", "text": "on the twelfth of July", "edited_text": "on 07-12"}"#,
    );
    assert!(matches!(word, ServerWord::Edited { text, edited } if text == "on the twelfth of July" && edited == "on 07-12"));
    let word = classify_server_frame(r#"{"message_type": "edited_transcript", "text": "x"}"#);
    assert!(matches!(word, ServerWord::Refused(why) if why.contains("edited_text")));
    Ok(())
}

fn final_frames() -> WeftResult<()> {
    for kind in ["final_transcript", "final_transcript_with_timestamps"] {
        let word = classify_server_frame(&format!(r#"{{"message_type": "{kind}", "text": "so", "language_code": "fr"}}"#));
        assert!(matches!(word, ServerWord::Language(l) if l == "fr"), "{kind} reports its language");
        let word = classify_server_frame(&format!(r#"{{"message_type": "{kind}", "text": "so"}}"#));
        assert!(matches!(word, ServerWord::Ignore), "{kind} never adds a segment twice");
    }
    Ok(())
}

fn annotations() -> WeftResult<()> {
    let mut t = Transcript::default();
    assert!(t.commit("yes"));
    assert!(!t.commit(""), "an empty segment adds nothing");
    assert!(t.commit("call Jill"));
    assert!(t.commit("yes"));
    assert_eq!(t.full_text(), "yes call Jill yes");

    // Entities land positioned in the full transcript.
    let jill = json!({ "text": "Jill", "entityType": "name", "startChar": 5, "endChar": 9 });
    assert!(t.add_entities("call Jill", &[jill]));
    assert_eq!(t.entities, vec![json!({ "text": "Jill", "entityType": "name", "startChar": 9, "endChar": 13 })]);
    assert_eq!(&t.full_text()[9..13], "Jill");
    assert!(!t.add_entities("call Jill", &[]), "a segment takes its entities once");
    assert!(!t.add_entities("never said", &[]));

    // Rewrites arrive out of order and find the earliest segment with
    // their text still waiting for one.
    assert!(t.add_edit("yes", "Yes."));
    assert!(t.add_edit("yes", "Yes!"));
    assert!(!t.add_edit("yes", "Yes?"), "two segments, two rewrites");
    assert_eq!(t.edited_text(), "Yes. call Jill Yes!", "the unedited segment stays as spoken");
    assert_eq!(t.unedited(), 1);
    Ok(())
}

fn document() -> WeftResult<()> {
    let mut t = Transcript::default();
    assert!(t.commit("call Jill"));
    assert!(t.commit("on the twelfth"));
    assert_eq!(t.document(&session()), json!({ "text": "call Jill on the twelfth" }), "the text alone by default");

    let jill = json!({ "text": "Jill", "entityType": "name", "startChar": 5, "endChar": 9 });
    assert!(t.add_entities("call Jill", &[jill.clone()]));
    let entities = Session { entity_detection: vec!["pii".into()], ..session() };
    assert_eq!(t.document(&entities), json!({ "text": "call Jill on the twelfth", "entities": [jill] }));

    assert!(t.add_edit("on the twelfth", "on the 12th"));
    let rewrites = Session { transcript_edit: Some("Write dates as numbers.".into()), ..session() };
    assert_eq!(
        t.document(&rewrites),
        json!({ "text": "call Jill on the twelfth", "editedText": "call Jill on the 12th" }),
        "the text as spoken, and the rewrite with the segment that got none kept as spoken"
    );
    Ok(())
}

/// The node's defaults, as a run with nothing set reads them.
fn session() -> Session {
    Session {
        model: "scribe_v2_realtime".into(),
        commit_strategy: "vad".into(),
        language: None,
        secondary_languages: Vec::new(),
        vad_silence_threshold_secs: None,
        vad_threshold: None,
        min_speech_duration_ms: None,
        min_silence_duration_ms: None,
        include_timestamps: false,
        include_language_detection: false,
        keyterms: Vec::new(),
        no_verbatim: false,
        entity_detection: Vec::new(),
        transcript_edit: None,
        filter_background_audio: false,
        enable_logging: true,
        previous_text: None,
    }
}

fn default_query() -> WeftResult<()> {
    assert_eq!(
        session().url("pcm_16000")?.as_str(),
        "wss://api.elevenlabs.io/v1/speech-to-text/realtime?model_id=scribe_v2_realtime&audio_format=pcm_16000\
         &commit_strategy=vad"
    );
    Ok(())
}

fn every_setting() -> WeftResult<()> {
    let s = Session {
        commit_strategy: "turn_prediction".into(),
        language: Some("en".into()),
        secondary_languages: vec!["fr".into(), "de".into()],
        vad_silence_threshold_secs: Some(0.8),
        vad_threshold: Some(0.35),
        min_speech_duration_ms: Some(120),
        min_silence_duration_ms: Some(300),
        include_timestamps: true,
        include_language_detection: true,
        keyterms: vec!["Weavemind".into(), "weft node".into()],
        no_verbatim: true,
        entity_detection: vec!["pii".into(), "email_address".into()],
        enable_logging: false,
        ..session()
    };
    assert_eq!(
        s.url("ulaw_8000")?.as_str(),
        "wss://api.elevenlabs.io/v1/speech-to-text/realtime?model_id=scribe_v2_realtime&audio_format=ulaw_8000&commit_strategy=turn_prediction&language_code=en\
         &secondary_languages=fr&secondary_languages=de&vad_silence_threshold_secs=0.8&vad_threshold=0.35\
         &min_speech_duration_ms=120&min_silence_duration_ms=300&include_timestamps=true\
         &include_language_detection=true&no_verbatim=true&keyterms=Weavemind&keyterms=weft%20node\
         &entity_detection=pii&entity_detection=email_address&enable_logging=false"
    );
    let rewrite = Session {
        transcript_edit: Some("Write dates as YYYY-MM-DD & times as HH:MM".into()),
        filter_background_audio: true,
        ..session()
    };
    assert!(
        rewrite.url("pcm_16000")?.as_str().ends_with(
            "&filter_background_audio=true&transcript_edit=Write%20dates%20as%20YYYY-MM-DD%20%26%20times%20as%20HH%3AMM"
        ),
        "{}",
        rewrite.url("pcm_16000")?
    );
    Ok(())
}

fn formats() -> WeftResult<()> {
    for rate in [8_000, 16_000, 22_050, 24_000, 44_100, 48_000] {
        let (format, chunk_rate) = audio_format(&json!({ "sample_rate": rate, "encoding": "pcm_s16le" }))?;
        assert_eq!((format, chunk_rate), (format!("pcm_{rate}"), rate));
    }
    assert_eq!(audio_format(&json!({ "sample_rate": 8000, "encoding": "ulaw" }))?, ("ulaw_8000".to_string(), 8000));
    for (meta, says) in [
        (json!({ "sample_rate": 11025, "encoding": "pcm_s16le" }), "11025 Hz"),
        (json!({ "sample_rate": 16000, "encoding": "ulaw" }), "μ-law at 8000 Hz only"),
        (json!({ "sample_rate": 16000, "encoding": "f32le" }), "\"f32le\""),
        (json!({ "sample_rate": 16000 }), "no encoding"),
        (json!({ "encoding": "pcm_s16le" }), "no sample_rate"),
    ] {
        let err = audio_format(&meta).expect_err("a format the endpoint does not take").to_string();
        assert!(err.contains(says), "{meta}: {err}");
    }
    Ok(())
}

fn chunks() -> WeftResult<()> {
    let first: Value = serde_json::from_str(&audio_chunk(b"\x01\x02", 16_000, Some("Agent: how can I help?"), false))
        .expect("a chunk is JSON");
    assert_eq!(
        first,
        json!({
            "message_type": "input_audio_chunk",
            "audio_base_64": "AQI=",
            "commit": false,
            "sample_rate": 16000,
            "previous_text": "Agent: how can I help?",
        })
    );
    let flush: Value = serde_json::from_str(&audio_chunk(&[], 8_000, None, true)).expect("a chunk is JSON");
    assert_eq!(
        flush,
        json!({ "message_type": "input_audio_chunk", "audio_base_64": "", "commit": true, "sample_rate": 8000 })
    );
    Ok(())
}

/// One real realtime session: two seconds of a generated 440 Hz tone
/// on a live audio bus, transcribed for real. A tone carries no
/// speech, so the assertion is the protocol round-trip itself (the
/// session opens, drains the audio, and the transcript lands as a JSON
/// file), not any particular words; the audio never lands on the
/// account, so there is nothing to clean up.
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
    // A tone may transcribe to nothing, so the file's size says nothing.
    let file = StoredFile::from_value(outcome.output("transcript")?)?;
    assert_eq!((file.filename.as_str(), file.mime_type.as_str()), ("transcript.json", "application/json"));
    Ok(())
}
