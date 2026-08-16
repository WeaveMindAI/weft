//! ElevenLabsMusic self-tests.

use serde_json::json;

use weft::{FakeRig, LiveRig, NodeTest, WeftResult};

use super::ElevenLabsMusicNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("composes_and_stores_the_track", composes),
        NodeTest::live("one_real_ten_second_track", "elevenlabs", live_track),
    ]
}

/// One real ten-second instrumental (the shortest sensible track);
/// nothing lands on the account.
async fn live_track(rig: LiveRig) -> WeftResult<()> {
    let outcome = rig
        .run(
            &ElevenLabsMusicNode,
            json!({
                "account": rig.access("elevenlabs"),
                "prompt": "a short calm solo piano phrase",
                "lengthSecs": 10,
                "model": "music_v1",
                "forceInstrumental": true,
            }),
        )
        .await
        .ok()?;
    let size = outcome.output("sizeBytes")?.as_f64().unwrap_or(0.0);
    assert!(size > 10_000.0, "ten seconds of mp3 is tens of kilobytes: {size}");
    Ok(())
}

async fn composes(rig: FakeRig) -> WeftResult<()> {
    rig.respond_raw("POST", "/v1/music", 200, "audio/mpeg", b"track".to_vec());
    let outcome = rig
        .run(
            &ElevenLabsMusicNode,
            json!({
                "account": rig.access("elevenlabs"),
                "prompt": "warm lo-fi piano",
                "lengthSecs": 30,
                "model": "music_v1",
                "forceInstrumental": true,
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["filename"], json!("music.mp3"));
    let sent = &rig.requests()[0];
    assert_eq!(
        sent.body.as_ref().expect("json payload"),
        &json!({
            "prompt": "warm lo-fi piano",
            "model_id": "music_v1",
            "music_length_ms": 30000,
            "force_instrumental": true,
        })
    );
    Ok(())
}
