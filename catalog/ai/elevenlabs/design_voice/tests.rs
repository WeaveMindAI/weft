//! ElevenLabsDesignVoice self-tests.

use base64::Engine as _;
use serde_json::json;

use weft::{FakeRig, LiveRig, NodeTest, WeftResult, WeftType};

use super::ElevenLabsDesignVoiceNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("designs_and_stores_the_previews", designs),
        NodeTest::live("one_real_design_minted_and_deleted", "elevenlabs", live_design),
    ]
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

async fn designs(rig: FakeRig) -> WeftResult<()> {
    let b64 = base64::engine::general_purpose::STANDARD.encode(b"preview-audio");
    rig.respond(
        "POST",
        "/v1/text-to-voice/design",
        json!({ "previews": [
            {
                "generated_voice_id": "gen-1",
                "audio_base_64": b64,
                "media_type": "audio/mpeg",
                "duration_secs": 3.5,
                "language": "en",
            },
        ]}),
    );
    // The structural form of the declared VoicePreview list, so the
    // rig's internalize sees the audio media slot.
    rig.output_type(
        "previews",
        WeftType::parse(
            "List[{ voiceId: String, audio: Audio, durationSecs?: Number, language?: String }]",
        )
        .expect("the declared type parses"),
    );
    let outcome = rig
        .run(
            &ElevenLabsDesignVoiceNode,
            json!({
                "account": rig.access("elevenlabs"),
                "description": "calm narrator",
                "model": "eleven_multilingual_ttv_v2",
            }),
        )
        .await
        .ok()?;
    let previews = outcome.outputs["previews"].as_array().expect("a list");
    assert_eq!(previews.len(), 1);
    assert_eq!(previews[0]["voiceId"], json!("gen-1"));
    assert!(
        previews[0]["audio"].is_object(),
        "the preview audio is internalized to a stored file: {}",
        previews[0]["audio"]
    );

    let sent = &rig.requests()[0];
    assert_eq!(
        sent.body.as_ref().expect("json payload"),
        &json!({
            "voice_description": "calm narrator",
            "model_id": "eleven_multilingual_ttv_v2",
            "auto_generate_text": true,
        })
    );
    Ok(())
}
