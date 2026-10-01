//! ElevenLabsCloneVoice self-tests.

use serde_json::json;

use weft::{FakeRig, LiveRig, NodeTest, WeftResult};

use super::ElevenLabsCloneVoiceNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("clones_and_emits_the_voice_id", clones),
        NodeTest::fake("no_samples_fails_the_run_even_when_error_is_wired", no_samples),
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

async fn clones(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", "/v1/voices/add", json!({ "voice_id": "voice-new" }));
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
    Ok(())
}

/// No samples is a mistake in the program, not the service's answer:
/// a wired `error` must not swallow it.
async fn no_samples(rig: FakeRig) -> WeftResult<()> {
    rig.wire_output("error");
    let err = rig
        .run(
            &ElevenLabsCloneVoiceNode,
            json!({
                "account": rig.access("elevenlabs"),
                "name": "Narrator",
                "samples": [],
                "removeBackgroundNoise": false,
            }),
        )
        .await
        .failure()?;
    assert!(err.starts_with("input error"), "{err}");
    assert!(err.contains("at least one sample"), "{err}");
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
    rig.wire_output("error");
    let outcome = rig.run(&ElevenLabsCloneVoiceNode, inputs).await.ok()?;
    let error = outcome.output("error")?.as_str().expect("error is a string").to_string();
    assert!(error.contains("bad key"), "{error}");
    assert!(!outcome.outputs.contains_key("voiceId"), "a caught failure emits no voiceId");
    Ok(())
}
