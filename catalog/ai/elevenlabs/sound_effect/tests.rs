//! ElevenLabsSoundEffect self-tests.

use serde_json::json;

use weft::{FakeRig, LiveRig, NodeTest, WeftResult};

use super::ElevenLabsSoundEffectNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("generates_and_stores_the_effect", generates),
        NodeTest::live("one_real_short_effect", "elevenlabs", live_effect),
    ]
}

/// One real one-second effect (the cheapest possible generation);
/// nothing lands on the account.
async fn live_effect(rig: LiveRig) -> WeftResult<()> {
    let outcome = rig
        .run(
            &ElevenLabsSoundEffectNode,
            json!({
                "account": rig.access("elevenlabs"),
                "text": "a single soft click",
                "durationSecs": 1,
                "loop": false,
            }),
        )
        .await
        .ok()?;
    let size = outcome.output("sizeBytes")?.as_f64().unwrap_or(0.0);
    assert!(size > 500.0, "one second of mp3 is non-trivial: {size}");
    Ok(())
}

async fn generates(rig: FakeRig) -> WeftResult<()> {
    rig.respond_raw("POST", "/v1/sound-generation", 200, "audio/mpeg", b"fx".to_vec());
    let outcome = rig
        .run(
            &ElevenLabsSoundEffectNode,
            json!({
                "account": rig.access("elevenlabs"),
                "text": "rain on glass",
                "durationSecs": 4,
                "loop": true,
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["filename"], json!("sound_effect.mp3"));
    let sent = &rig.requests()[0];
    assert_eq!(
        sent.body.as_ref().expect("json payload"),
        &json!({ "text": "rain on glass", "loop": true, "duration_seconds": 4.0 })
    );
    Ok(())
}
