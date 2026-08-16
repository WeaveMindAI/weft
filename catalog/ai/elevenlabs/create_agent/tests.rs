//! ElevenLabsCreateAgent self-tests.

use serde_json::json;

use weft::{FakeRig, LiveRig, NodeTest, WeftResult};

use super::ElevenLabsCreateAgentNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("creates_and_emits_the_agent_id", creates),
        NodeTest::live("one_real_agent_created_and_deleted", "elevenlabs", live_create),
    ]
}

/// One real agent minted (name, prompt, first message, stock voice),
/// read back by id, then DELETED so repeated runs never pile agents
/// onto the account.
async fn live_create(rig: LiveRig) -> WeftResult<()> {
    let conn = rig.connect().await?;
    let voice = crate::testing::first_voice_id(&conn).await?;
    let outcome = rig
        .run(
            &ElevenLabsCreateAgentNode,
            json!({
                "account": rig.access("elevenlabs"),
                "name": "weft-node-tests",
                "prompt": "You are a test agent; say you are a test and end the call.",
                "firstMessage": "This is a weft node test.",
                "voice": voice,
                "language": "en",
            }),
        )
        .await
        .ok()?;
    let agent = outcome.output("agentId")?.as_str().expect("agent id").to_string();
    weft::with_cleanup(
        || async {
            let read = weft::access::client::get_json(
                conn.client(),
                &format!("{}/convai/agents/{agent}", crate::elevenlabs::API),
                "read the minted agent back",
            )
            .await?;
            assert_eq!(read["name"].as_str(), Some("weft-node-tests"), "{read}");
            Ok(())
        },
        || async { crate::testing::delete_agent(&conn, &agent).await },
    )
    .await
}

async fn creates(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", "/v1/convai/agents/create", json!({ "agent_id": "agent-new" }));
    let outcome = rig
        .run(
            &ElevenLabsCreateAgentNode,
            json!({
                "account": rig.access("elevenlabs"),
                "name": "Booking bot",
                "prompt": "You book tables.",
                "firstMessage": "Hi, this is the booking line!",
                "voice": "voice-1",
                "language": "en",
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["agentId"], json!("agent-new"));

    let body = rig.requests()[0].body.clone().expect("json payload");
    assert_eq!(body["name"], json!("Booking bot"));
    let config = &body["conversation_config"];
    assert_eq!(config["agent"]["prompt"]["prompt"], json!("You book tables."));
    assert_eq!(config["agent"]["first_message"], json!("Hi, this is the booking line!"));
    assert_eq!(config["tts"]["voice_id"], json!("voice-1"));
    Ok(())
}
