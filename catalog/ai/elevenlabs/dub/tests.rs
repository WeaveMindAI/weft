//! ElevenLabsDub self-tests: the submit -> poll -> download dance
//! against canned answers.

use serde_json::json;

use weft::{FakeRig, LiveRig, NodeTest, WeftResult};

use super::ElevenLabsDubNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("submits_waits_and_stores_the_dub", dubs),
        NodeTest::fake("a_failed_dub_fails_loud", failed),
        NodeTest::live("one_real_short_dub_to_spanish", "elevenlabs", live_dub),
    ]
}

/// One real dub of a freshly TTS'd English sentence into Spanish: the
/// whole submit -> wait -> download loop against the real pipeline (a
/// short clip, but expect a couple of minutes of processing). The dub
/// project the run leaves on the account is deleted afterwards, so
/// repeated runs never pile projects up.
async fn live_dub(rig: LiveRig) -> WeftResult<()> {
    let conn = rig.connect().await?;
    let voice = crate::testing::first_voice_id(&conn).await?;
    let sample = crate::testing::speech_sample(
        &conn,
        &voice,
        "Hello! This is a very short dubbing test.",
    )
    .await?;
    let file = rig.store_file("english.mp3", "audio/mpeg", sample).await?;
    let outcome = rig
        .run(
            &ElevenLabsDubNode,
            json!({
                "account": rig.access("elevenlabs"),
                "file": file,
                "targetLang": "es",
                "sourceLang": "en",
                "numSpeakers": 1,
                "dropBackgroundAudio": false,
            }),
        )
        .await
        .ok()?;
    let dub_id = outcome.output("dubbingId")?.as_str().expect("dub id").to_string();
    weft::with_cleanup(
        || async {
            let size = outcome.output("sizeBytes")?.as_f64().unwrap_or(0.0);
            assert!(size > 1_000.0, "the dubbed clip is real audio: {size}");
            let filename =
                outcome.output("filename")?.as_str().unwrap_or_default().to_string();
            assert!(filename.starts_with("dubbed_es_"), "{filename}");
            Ok(())
        },
        || async {
            crate::testing::delete(
                &conn,
                &format!("{}/dubbing/{dub_id}", crate::elevenlabs::API),
                "delete the test dub project",
            )
            .await
        },
    )
    .await
}

async fn dubs(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", "/v1/dubbing", json!({ "dubbing_id": "dub-1" }));
    rig.respond("GET", "/v1/dubbing/dub-1", json!({ "status": "dubbed" }));
    rig.respond_raw(
        "GET",
        "/v1/dubbing/dub-1/audio/fr",
        200,
        "audio/mpeg",
        b"french".to_vec(),
    );
    let file = rig.store_file("talk.mp3", "audio/mpeg", b"english".to_vec());
    let outcome = rig
        .run(
            &ElevenLabsDubNode,
            json!({
                "account": rig.access("elevenlabs"),
                "file": file,
                "targetLang": "fr",
                "dropBackgroundAudio": false,
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["filename"], json!("dubbed_fr_talk.mp3"));
    assert_eq!(outcome.outputs["sizeBytes"], json!(6));
    Ok(())
}

async fn failed(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", "/v1/dubbing", json!({ "dubbing_id": "dub-2" }));
    rig.respond(
        "GET",
        "/v1/dubbing/dub-2",
        json!({ "status": "failed", "error": "unsupported language" }),
    );
    let file = rig.store_file("talk.mp3", "audio/mpeg", b"english".to_vec());
    let outcome = rig
        .run(
            &ElevenLabsDubNode,
            json!({
                "account": rig.access("elevenlabs"),
                "file": file,
                "targetLang": "xx",
                "dropBackgroundAudio": false,
            }),
        )
        .await;
    let err = outcome.result.expect_err("a failed dub must refuse").to_string();
    assert!(err.contains("unsupported language"), "{err}");
    Ok(())
}
