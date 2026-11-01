//! ElevenLabsRemixVoice self-tests: the request the node builds from
//! every setting, sent on the picked voice's remix route, the previews
//! stored as the format asked for, the line ElevenLabs answered, and
//! what is refused before anything is sent.

use base64::Engine as _;
use serde_json::{json, Value};

use weft::storage::StoredFile;
use weft::{FakeRig, NodeTest, WeftResult};

use super::super::elevenlabs::audio_file_type;
use super::{refusal, ElevenLabsRemixVoiceNode};

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::basic("every_offered_output_format_has_a_file_type", formats),
        NodeTest::fake("remixes_and_stores_the_previews", remixes),
        NodeTest::fake("every_elevenlabs_setting_reaches_the_request", every_setting),
        NodeTest::fake("a_typed_voice_id_stays_one_path_segment", voice_id_encoded),
        NodeTest::fake("an_answer_without_its_line_still_hands_the_previews_on", no_line),
        NodeTest::fake("remixes_elevenlabs_would_refuse_are_refused_before_anything_is_sent", refused),
        NodeTest::basic("the_remix_requests_elevenlabs_refuses", refusals),
    ]
}

const VOICE: &str = "voice-1";
/// The remix route of [`VOICE`].
const REMIX: &str = "/v1/text-to-voice/voice-1/remix";
const DESCRIPTION: &str = "Use a higher pitch and change to a Boston accent.";
/// A preview line inside ElevenLabs's 100 to 1000 characters.
const LINE: &str = "The lighthouse keeper climbed the stairs every evening, counting each step, and lit the lamp before the fog rolled in.";

/// The inputs of a plain remix of [`VOICE`], plus `extra`.
fn inputs(rig: &FakeRig, extra: Value) -> Value {
    let mut base = json!({
        "account": rig.access("elevenlabs"),
        "voice": VOICE,
        "description": DESCRIPTION,
    });
    for (k, v) in extra.as_object().expect("extra inputs are an object") {
        base[k] = v.clone();
    }
    base
}

/// A canned preview answer on `route` under `query`: one preview per
/// clip, and the line they speak when `text` is given.
fn answers(rig: &FakeRig, route: &str, query: &str, clips: &[(&str, &[u8])], text: Option<&str>) {
    let previews: Vec<Value> = clips
        .iter()
        .map(|(id, bytes)| {
            json!({
                "generated_voice_id": id,
                "audio_base_64": base64::engine::general_purpose::STANDARD.encode(bytes),
                "media_type": "audio/mpeg",
                "duration_secs": 3.5,
                "language": "en",
            })
        })
        .collect();
    let mut answer = json!({ "previews": previews });
    if let Some(text) = text {
        answer["text"] = json!(text);
    }
    rig.respond("POST", &format!("{route}?{query}"), answer);
}

/// The stored file behind a preview's audio: its bytes, name and type,
/// and its deadline.
fn stored(rig: &FakeRig, preview: &Value) -> WeftResult<(Vec<u8>, String, String, Option<u64>)> {
    let file = StoredFile::from_value(&preview["audio"])?;
    let meta = rig.stored_meta(&file.key)?;
    Ok((rig.stored_bytes(&file.key)?.to_vec(), meta.filename, meta.mime_type, meta.keep_ttl_secs))
}

/// Every format the metadata offers stores under a real extension and
/// mime, so no pick can fail after the previews are paid for.
fn formats() -> WeftResult<()> {
    let meta: Value = serde_json::from_str(include_str!("metadata.json")).expect("the metadata parses");
    let input = meta["inputs"]
        .as_array()
        .expect("inputs")
        .iter()
        .find(|i| i["name"] == "outputFormat")
        .expect("an outputFormat input");
    let options = input["widget"]["options"].as_array().expect("the format options");
    assert_eq!(options.len(), 21, "every format ElevenLabs's remix route offers");
    for option in options {
        audio_file_type(option.as_str().expect("a format name"))?;
    }
    Ok(())
}

async fn remixes(rig: FakeRig) -> WeftResult<()> {
    answers(&rig, REMIX, "output_format=mp3_44100_128", &[("gen-1", &b"preview-audio"[..])], Some(LINE));
    let outcome = rig.run(&ElevenLabsRemixVoiceNode, inputs(&rig, json!({}))).await.ok()?;

    let previews = outcome.outputs["previews"].as_array().expect("a list");
    assert_eq!(previews.len(), 1);
    assert_eq!(previews[0]["voiceId"], json!("gen-1"));
    assert_eq!(previews[0]["durationSecs"], json!(3.5));
    assert_eq!(previews[0]["language"], json!("en"));
    let (bytes, filename, mime, deadline) = stored(&rig, &previews[0])?;
    assert_eq!(bytes, b"preview-audio".to_vec(), "the decoded clip is what is stored");
    assert_eq!(filename, "preview-1.mp3");
    assert_eq!(mime, "audio/mpeg");
    assert_eq!(deadline, None, "the previews are the run's, with no deadline: they go with the run");
    let key = StoredFile::from_value(&previews[0]["audio"])?.key;
    assert!(key.contains("/run/"), "run-scoped: the previews are the run's: {key}");
    assert_eq!(outcome.outputs["text"], json!(LINE), "the line ElevenLabs wrote");

    let sent = &rig.requests()[0];
    assert_eq!(sent.path, REMIX, "the remix route of the picked voice");
    assert_eq!(sent.query.as_deref(), Some("output_format=mp3_44100_128"), "weft's default format");
    assert_eq!(
        sent.body.as_ref().expect("json payload"),
        &json!({
            "voice_description": DESCRIPTION,
            "auto_generate_text": true,
        }),
        "no line given asks ElevenLabs to write one, and no setting nobody gave travels"
    );
    Ok(())
}

async fn every_setting(rig: FakeRig) -> WeftResult<()> {
    answers(&rig, REMIX, "output_format=opus_48000_64", &[("gen-1", &b"one"[..]), ("gen-2", &b"two"[..])], Some(LINE));
    let outcome = rig
        .run(
            &ElevenLabsRemixVoiceNode,
            inputs(
                &rig,
                json!({
                    "text": LINE,
                    "outputFormat": "opus_48000_64",
                    "guidanceScale": 3.5,
                    "loudness": -0.2,
                    "seed": 42,
                    "promptStrength": 0.7,
                }),
            ),
        )
        .await
        .ok()?;

    let sent = &rig.requests()[0];
    assert_eq!(sent.path, REMIX);
    assert_eq!(sent.query.as_deref(), Some("output_format=opus_48000_64"));
    assert_eq!(
        sent.body.as_ref().expect("json payload"),
        &json!({
            "voice_description": DESCRIPTION,
            "text": LINE,
            "guidance_scale": 3.5,
            "loudness": -0.2,
            "seed": 42,
            "prompt_strength": 0.7,
        }),
        "a given line is sent as it is and asks for no generated one, and only fields the remix route takes travel"
    );

    let previews = outcome.outputs["previews"].as_array().expect("a list");
    assert_eq!(previews.len(), 2);
    let (bytes, filename, mime, _) = stored(&rig, &previews[1])?;
    assert_eq!(bytes, b"two".to_vec());
    assert_eq!(filename, "preview-2.ogg", "each preview is named by its place, as the format asked for");
    assert_eq!(mime, "audio/ogg; codecs=opus", "stored as the format asked for, not the answer's media_type");
    Ok(())
}

/// A typed voice id that looks like a path stays one path segment, so
/// it cannot reach another ElevenLabs route.
async fn voice_id_encoded(rig: FakeRig) -> WeftResult<()> {
    answers(&rig, "/v1/text-to-voice/..%2Fuser/remix", "output_format=mp3_44100_128", &[("gen-1", &b"clip"[..])], None);
    rig.run(&ElevenLabsRemixVoiceNode, inputs(&rig, json!({ "voice": "../user" }))).await.ok()?;
    assert_eq!(rig.requests()[0].path, "/v1/text-to-voice/..%2Fuser/remix");
    Ok(())
}

async fn no_line(rig: FakeRig) -> WeftResult<()> {
    answers(&rig, REMIX, "output_format=mp3_44100_128", &[("gen-1", &b"clip"[..])], None);
    let outcome = rig.run(&ElevenLabsRemixVoiceNode, inputs(&rig, json!({ "text": LINE }))).await.ok()?;
    assert_eq!(outcome.outputs["previews"].as_array().map(Vec::len), Some(1), "the paid previews still come out");
    assert!(!outcome.outputs.contains_key("text"), "`text` closes when ElevenLabs answers no line");
    Ok(())
}

async fn refused(rig: FakeRig) -> WeftResult<()> {
    rig.wire_output("_error");
    for (extra, says) in [
        (json!({ "voice": " " }), "`voice` is empty"),
        (json!({ "description": "Low" }), "`description` is 3 characters"),
        (json!({ "description": "x".repeat(1001) }), "`description` is 1001 characters"),
        (json!({ "text": "A short line." }), "`text` is 13 characters"),
        (json!({ "text": "x".repeat(1001) }), "`text` is 1001 characters"),
    ] {
        let err = rig.run(&ElevenLabsRemixVoiceNode, inputs(&rig, extra.clone())).await.failure()?;
        assert!(err.starts_with("input error"), "a mistake in the program fails the run even with `_error` wired: {extra}: {err}");
        assert!(err.contains(says), "{extra}: {err}");
    }
    assert!(rig.requests().is_empty(), "nothing is sent, so nothing is paid for");
    Ok(())
}

fn refusals() -> WeftResult<()> {
    let fine = "x".repeat(5);
    assert!(refusal(VOICE, &"x".repeat(4), None).is_some());
    assert!(refusal(VOICE, &fine, None).is_none(), "5 characters is enough to remix");
    assert!(refusal(VOICE, &"é".repeat(5), None).is_none(), "characters count, not bytes");
    assert!(refusal(VOICE, &"x".repeat(1000), None).is_none());
    assert!(refusal(VOICE, &"x".repeat(1001), None).is_some());
    assert!(refusal(VOICE, &fine, Some("x".repeat(99).as_str())).is_some());
    assert!(refusal(VOICE, &fine, Some("x".repeat(100).as_str())).is_none());
    assert!(refusal(VOICE, &fine, Some("x".repeat(1000).as_str())).is_none());
    assert!(refusal(VOICE, &fine, Some("x".repeat(1001).as_str())).is_some());
    assert!(refusal("", &fine, None).is_some(), "an empty voice names no voice");
    assert!(refusal("  ", &fine, None).is_some(), "nor does a blank one");
    Ok(())
}
