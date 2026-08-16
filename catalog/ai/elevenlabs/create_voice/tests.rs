//! ElevenLabsCreateVoice self-tests.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::ElevenLabsCreateVoiceNode;

pub fn tests() -> Vec<NodeTest> {
    vec![NodeTest::fake("creates_and_emits_the_voice_id", creates)]
}

async fn creates(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", "/v1/text-to-voice", json!({ "voice_id": "voice-durable" }));
    let outcome = rig
        .run(
            &ElevenLabsCreateVoiceNode,
            json!({
                "account": rig.access("elevenlabs"),
                "name": "Narrator",
                "description": "calm narrator",
                "previewId": "gen-1",
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["voiceId"], json!("voice-durable"));
    let sent = &rig.requests()[0];
    assert_eq!(
        sent.body.as_ref().expect("json payload"),
        &json!({
            "voice_name": "Narrator",
            "voice_description": "calm narrator",
            "generated_voice_id": "gen-1",
        })
    );
    Ok(())
}
