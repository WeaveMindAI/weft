//! ElevenLabsCloneVoice self-tests.

use serde_json::json;

use weft::{FakeRig, LiveRig, NodeTest, WeftResult};

use super::{fields, ElevenLabsCloneVoiceNode};

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("clones_and_emits_the_voice_id", clones),
        NodeTest::basic("the_text_fields_ride_the_form_only_when_set", fields_form),
        NodeTest::fake("a_refused_clone_fails_the_run_when_error_is_unwired", refused_unwired),
        NodeTest::fake("a_refused_clone_comes_out_on_error_when_it_is_wired", refused_wired),
        NodeTest::live("one_real_clone_created_and_deleted", "elevenlabs", live_clone),
    ]
}

/// One real instant clone from two freshly TTS'd samples: the minted
/// voice exists (readable by id), then is DELETED so repeated runs
/// never eat the account's voice slots.
async fn live_clone(rig: LiveRig) -> WeftResult<()> {
    let conn = rig.connect().await?;
    let voice = crate::testing::first_voice_id(&conn).await?;
    let a = crate::testing::speech_sample(&conn, &voice, "First cloning sample.").await?;
    let b = crate::testing::speech_sample(&conn, &voice, "Second cloning sample.").await?;
    let sample_a = rig.store_file("a.mp3", "audio/mpeg", a).await?;
    let sample_b = rig.store_file("b.mp3", "audio/mpeg", b).await?;
    let outcome = rig
        .run(
            &ElevenLabsCloneVoiceNode,
            json!({
                "account": rig.access("elevenlabs"),
                "name": "weft-node-tests",
                "samples": [sample_a, sample_b],
                "description": "weft node test clone; safe to delete",
                "removeBackgroundNoise": false,
            }),
        )
        .await
        .ok()?;
    let minted = outcome.output("voiceId")?.as_str().expect("voice id").to_string();
    weft::with_cleanup(
        || async {
            let verification = outcome.output("requiresVerification")?;
            assert!(verification.is_boolean(), "ElevenLabs says whether the voice needs verifying: {verification}");
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

/// The clone request the node sent.
fn clone_request(rig: &FakeRig) -> weft::SentRequest {
    rig.requests()
        .into_iter()
        .find(|r| r.method == "POST" && r.path == "/v1/voices/add")
        .expect("the node sent a multipart clone")
}

/// Whether the multipart `form` holds a text field `name` set to `value`.
fn has_field(form: &str, name: &str, value: &str) -> bool {
    form.contains(&format!("name=\"{name}\"\r\n\r\n{value}\r\n"))
}

async fn clones(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", "/v1/voices/add", json!({ "voice_id": "voice-new", "requires_verification": false }));
    let a = rig.store_file("sample_a.mp3", "audio/mpeg", b"aaa".to_vec());
    let b = rig.store_file("sample_b.mp3", "audio/mpeg", b"bbb".to_vec());
    let outcome = rig
        .run(
            &ElevenLabsCloneVoiceNode,
            json!({
                "account": rig.access("elevenlabs"),
                "name": "Narrator",
                "samples": [a, b],
                "removeBackgroundNoise": false,
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["voiceId"], json!("voice-new"));
    assert_eq!(outcome.outputs["requiresVerification"], json!(false));
    assert!(clone_request(&rig).body_streamed, "the samples stream out of storage, never held whole");
    Ok(())
}

/// The text fields ride the form only when set, and the labels go as
/// their JSON.
fn fields_form() -> WeftResult<()> {
    let (_, bare) = fields("Narrator", None, false, None).build();
    let bare = String::from_utf8_lossy(&bare).into_owned();
    assert!(has_field(&bare, "name", "Narrator"), "{bare}");
    assert!(!bare.contains("name=\"labels\""), "no labels given, none sent: {bare}");
    assert!(!bare.contains("name=\"remove_background_noise\""), "noise removal stays off unless turned on: {bare}");
    assert!(!bare.contains("name=\"description\""), "{bare}");

    let labels = [("language".to_string(), "en".to_string()), ("accent".to_string(), "en-US".to_string())].into_iter().collect();
    let (_, full) = fields("Narrator", Some("warm"), true, Some(labels)).build();
    let full = String::from_utf8_lossy(&full).into_owned();
    assert!(has_field(&full, "labels", r#"{"accent":"en-US","language":"en"}"#), "{full}");
    assert!(has_field(&full, "description", "warm"), "{full}");
    assert!(has_field(&full, "remove_background_noise", "true"), "{full}");
    Ok(())
}

/// Two stored samples and the clone call refused by elevenlabs.
fn refuse_the_clone(rig: &FakeRig) -> serde_json::Value {
    rig.respond_status("POST", "/v1/voices/add", 401, json!({ "detail": "bad key" }));
    let a = rig.store_file("sample_a.mp3", "audio/mpeg", b"aaa".to_vec());
    json!({
        "account": rig.access("elevenlabs"),
        "name": "Narrator",
        "samples": [a],
        "removeBackgroundNoise": false,
    })
}

async fn refused_unwired(rig: FakeRig) -> WeftResult<()> {
    let inputs = refuse_the_clone(&rig);
    let err = rig.run(&ElevenLabsCloneVoiceNode, inputs).await.failure()?;
    assert!(err.contains("bad key"), "{err}");
    Ok(())
}

async fn refused_wired(rig: FakeRig) -> WeftResult<()> {
    let inputs = refuse_the_clone(&rig);
    rig.wire_output("_error");
    let outcome = rig.run(&ElevenLabsCloneVoiceNode, inputs).await.ok()?;
    let error = outcome.output("_error")?.as_str().expect("error is a string").to_string();
    assert!(error.contains("bad key"), "{error}");
    assert!(!outcome.outputs.contains_key("voiceId"), "a caught failure emits no voiceId");
    Ok(())
}
