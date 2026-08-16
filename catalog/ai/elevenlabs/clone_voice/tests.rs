//! ElevenLabsCloneVoice self-tests.

use serde_json::json;

use weft::{FakeRig, LiveRig, NodeTest, WeftResult};

use super::ElevenLabsCloneVoiceNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("clones_and_emits_the_voice_id", clones),
        NodeTest::fake("no_samples_refuses", no_samples),
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

async fn no_samples(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig
        .run(
            &ElevenLabsCloneVoiceNode,
            json!({
                "account": rig.access("elevenlabs"),
                "name": "Narrator",
                "samples": [],
                "removeBackgroundNoise": false,
            }),
        )
        .await;
    let err = outcome.result.expect_err("no samples must refuse").to_string();
    assert!(err.contains("at least one sample"), "{err}");
    Ok(())
}
