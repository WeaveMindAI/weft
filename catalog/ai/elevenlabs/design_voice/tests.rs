//! ElevenLabsDesignVoice self-tests: the request the node builds from
//! every setting, the previews stored as the format asked for, the
//! line ElevenLabs answered, and what is refused before anything is
//! sent.

use base64::Engine as _;
use serde_json::{json, Value};

use weft::storage::StoredFile;
use weft::{FakeRig, LiveRig, NodeTest, WeftResult};

use super::super::elevenlabs::audio_file_type;
use super::{refusal, ElevenLabsDesignVoiceNode};

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::basic("every_offered_output_format_has_a_file_type", formats),
        NodeTest::fake("designs_and_stores_the_previews", designs),
        NodeTest::fake("every_elevenlabs_setting_reaches_the_request", every_setting),
        NodeTest::fake("an_answer_without_its_line_still_hands_the_previews_on", no_line),
        NodeTest::fake("designs_elevenlabs_would_refuse_are_refused_before_anything_is_sent", refused),
        NodeTest::basic("the_design_requests_elevenlabs_refuses", refusals),
        NodeTest::live("one_real_design_minted_and_deleted", "elevenlabs", live_design),
        NodeTest::live("one_real_v3_design_from_a_reference_recording", "elevenlabs", live_reference_design),
    ]
}

const DESIGN: &str = "/v1/text-to-voice/design";
const DESCRIPTION: &str = "A calm, warm narrator with a soft voice";
/// A preview line inside ElevenLabs's 100 to 1000 characters.
const LINE: &str = "The lighthouse keeper climbed the stairs every evening, counting each step, and lit the lamp before the fog rolled in.";

/// The inputs of a plain design, plus `extra`.
fn inputs(rig: &FakeRig, extra: Value) -> Value {
    let mut base = json!({
        "account": rig.access("elevenlabs"),
        "description": DESCRIPTION,
    });
    for (k, v) in extra.as_object().expect("extra inputs are an object") {
        base[k] = v.clone();
    }
    base
}

/// A canned design answer under `query`: one preview per clip, and the
/// line they speak when `text` is given.
fn answers(rig: &FakeRig, query: &str, clips: &[(&str, &[u8])], text: Option<&str>) {
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
    rig.respond("POST", &format!("{DESIGN}?{query}"), answer);
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
    assert_eq!(options.len(), 21, "every format ElevenLabs's design route offers");
    for option in options {
        audio_file_type(option.as_str().expect("a format name"))?;
    }
    Ok(())
}

async fn designs(rig: FakeRig) -> WeftResult<()> {
    answers(&rig, "output_format=mp3_44100_128", &[("gen-1", &b"preview-audio"[..])], Some(LINE));
    let outcome = rig.run(&ElevenLabsDesignVoiceNode, inputs(&rig, json!({}))).await.ok()?;

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
    assert_eq!(sent.query.as_deref(), Some("output_format=mp3_44100_128"), "weft's default format");
    assert_eq!(
        sent.body.as_ref().expect("json payload"),
        &json!({
            "voice_description": DESCRIPTION,
            "model_id": "eleven_multilingual_ttv_v2",
            "auto_generate_text": true,
        }),
        "no line given asks ElevenLabs to write one, and no setting nobody gave travels"
    );
    Ok(())
}

async fn every_setting(rig: FakeRig) -> WeftResult<()> {
    answers(&rig, "output_format=opus_48000_64", &[("gen-1", &b"one"[..]), ("gen-2", &b"two"[..])], Some(LINE));
    let reference = rig.store_file("reference.mp3", "audio/mpeg", b"reference-take".to_vec());
    let outcome = rig
        .run(
            &ElevenLabsDesignVoiceNode,
            inputs(
                &rig,
                json!({
                    "text": LINE,
                    "model": "eleven_ttv_v3",
                    "outputFormat": "opus_48000_64",
                    "guidanceScale": 3.5,
                    "shouldEnhance": true,
                    "quality": 0.4,
                    "loudness": -0.2,
                    "seed": 42,
                    "referenceAudio": reference,
                    "promptStrength": 0.7,
                }),
            ),
        )
        .await
        .ok()?;

    let sent = &rig.requests()[0];
    assert_eq!(sent.query.as_deref(), Some("output_format=opus_48000_64"));
    assert_eq!(
        sent.body.as_ref().expect("json payload"),
        &json!({
            "voice_description": DESCRIPTION,
            "model_id": "eleven_ttv_v3",
            "text": LINE,
            "guidance_scale": 3.5,
            "quality": 0.4,
            "loudness": -0.2,
            "seed": 42,
            "prompt_strength": 0.7,
            "should_enhance": true,
            "reference_audio_base64": base64::engine::general_purpose::STANDARD.encode(b"reference-take"),
        }),
        "a given line is sent as it is, and asks for no generated one"
    );

    let previews = outcome.outputs["previews"].as_array().expect("a list");
    assert_eq!(previews.len(), 2);
    let (bytes, filename, mime, _) = stored(&rig, &previews[1])?;
    assert_eq!(bytes, b"two".to_vec());
    assert_eq!(filename, "preview-2.ogg", "each preview is named by its place, as the format asked for");
    assert_eq!(mime, "audio/ogg; codecs=opus", "stored as the format asked for, not the answer's media_type");
    Ok(())
}

async fn no_line(rig: FakeRig) -> WeftResult<()> {
    answers(&rig, "output_format=mp3_44100_128", &[("gen-1", &b"clip"[..])], None);
    let outcome = rig.run(&ElevenLabsDesignVoiceNode, inputs(&rig, json!({ "text": LINE }))).await.ok()?;
    assert_eq!(outcome.outputs["previews"].as_array().map(Vec::len), Some(1), "the paid previews still come out");
    assert!(!outcome.outputs.contains_key("text"), "`text` closes when ElevenLabs answers no line");
    Ok(())
}

async fn refused(rig: FakeRig) -> WeftResult<()> {
    rig.wire_output("_error");
    let reference = rig.store_file("reference.mp3", "audio/mpeg", b"reference-take".to_vec());
    for (extra, says) in [
        (json!({ "description": "too short" }), "`description` is 9 characters"),
        (json!({ "description": "x".repeat(1001) }), "`description` is 1001 characters"),
        (json!({ "text": "A short line." }), "`text` is 13 characters"),
        (json!({ "text": "x".repeat(1001) }), "`text` is 1001 characters"),
        (json!({ "referenceAudio": reference.clone() }), "only on eleven_ttv_v3"),
        (json!({ "model": "eleven_ttv_v3", "promptStrength": 0.5 }), "no `referenceAudio` is given"),
    ] {
        let err = rig.run(&ElevenLabsDesignVoiceNode, inputs(&rig, extra.clone())).await.failure()?;
        assert!(err.starts_with("input error"), "a mistake in the program fails the run even with `_error` wired: {extra}: {err}");
        assert!(err.contains(says), "{extra}: {err}");
    }
    assert!(rig.requests().is_empty(), "nothing is sent, so nothing is paid for");
    Ok(())
}

fn refusals() -> WeftResult<()> {
    let v2 = "eleven_multilingual_ttv_v2";
    let v3 = "eleven_ttv_v3";
    let fine = "x".repeat(20);
    assert!(refusal(&"x".repeat(19), None, v2, false, false).is_some());
    assert!(refusal(&fine, None, v2, false, false).is_none(), "20 characters is enough");
    assert!(refusal(&"é".repeat(20), None, v2, false, false).is_none(), "characters count, not bytes");
    assert!(refusal(&"x".repeat(1000), None, v2, false, false).is_none());
    assert!(refusal(&"x".repeat(1001), None, v2, false, false).is_some());
    assert!(refusal(&fine, Some("x".repeat(99).as_str()), v2, false, false).is_some());
    assert!(refusal(&fine, Some("x".repeat(100).as_str()), v2, false, false).is_none());
    assert!(refusal(&fine, Some("x".repeat(1000).as_str()), v2, false, false).is_none());
    assert!(refusal(&fine, Some("x".repeat(1001).as_str()), v2, false, false).is_some());
    assert!(refusal(&fine, None, v2, true, false).is_some(), "reference audio is for eleven_ttv_v3 only");
    assert!(refusal(&fine, None, v3, true, true).is_none());
    assert!(refusal(&fine, None, v3, false, true).is_some(), "a prompt strength with nothing to weigh against");
    Ok(())
}

/// The whole design story for real: previews from a description, the
/// preferred preview minted into a voice through Create Voice, the
/// voice read back, then DELETED so repeated runs never eat the
/// account's voice slots.
async fn live_design(rig: LiveRig) -> WeftResult<()> {
    let designed = rig
        .run(
            &ElevenLabsDesignVoiceNode,
            json!({
                "account": rig.access("elevenlabs"),
                "description": "A calm, warm female narrator with a neutral accent.",
                "model": "eleven_multilingual_ttv_v2",
            }),
        )
        .await
        .ok()?;
    let previews = designed.output("previews")?.as_array().expect("a preview list").clone();
    assert!(!previews.is_empty(), "a design answers at least one preview");
    let line = designed.output("text")?.as_str().expect("the line the previews speak");
    assert!(!line.is_empty(), "ElevenLabs wrote a line for the previews");
    let first = &previews[0];
    assert!(first["audio"].is_object(), "preview audio is a stored file: {first}");
    let preview_id = first["voiceId"].as_str().expect("preview id").to_string();

    let created = rig
        .run(
            &crate::eleven_labs_create_voice::ElevenLabsCreateVoiceNode,
            json!({
                "account": rig.access("elevenlabs"),
                "name": "weft-node-tests",
                "description": "weft node test design; safe to delete",
                "previewId": preview_id,
            }),
        )
        .await
        .ok()?;
    let minted = created.output("voiceId")?.as_str().expect("voice id").to_string();
    let conn = rig.connect().await?;
    weft::with_cleanup(
        || async {
            let read = weft::access::client::get_json(
                conn.client(),
                &format!("{}/voices/{minted}", crate::elevenlabs::API),
                "read the minted voice back",
            )
            .await?;
            assert_eq!(read["name"].as_str(), Some("weft-node-tests"), "{read}");
            Ok(())
        },
        || async { crate::testing::delete_voice(&conn, &minted).await },
    )
    .await
}

/// A real eleven_ttv_v3 design from a reference recording (a stock
/// voice reading one sentence) weighed evenly against the description.
/// Previews are not voices until Create Voice mints one, so nothing
/// lands on the account.
async fn live_reference_design(rig: LiveRig) -> WeftResult<()> {
    let conn = rig.connect().await?;
    let voice = crate::testing::first_voice_id(&conn).await?;
    let sample = crate::testing::speech_sample(
        &conn,
        &voice,
        "This recording is the reference the new voice starts from, read slowly and clearly.",
    )
    .await?;
    let reference = rig.store_file("reference.mp3", "audio/mpeg", sample).await?;
    let designed = rig
        .run(
            &ElevenLabsDesignVoiceNode,
            json!({
                "account": rig.access("elevenlabs"),
                "description": "A brighter, younger version of this voice, speaking with energy.",
                "model": "eleven_ttv_v3",
                "referenceAudio": reference,
                "promptStrength": 0.5,
            }),
        )
        .await
        .ok()?;
    let previews = designed.output("previews")?.as_array().expect("a preview list").clone();
    assert!(!previews.is_empty(), "a design answers at least one preview");
    assert!(previews[0]["audio"].is_object(), "preview audio is a stored file: {}", previews[0]);
    Ok(())
}
