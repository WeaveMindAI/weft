//! ElevenLabsAgentCall self-tests: the provider-routed dial and the
//! refused-call surface.

use serde_json::json;

use weft::{fixture_spec, FakeRig, LiveRig, NodeTest, WeftResult};

use super::ElevenLabsAgentCallNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("dials_through_the_numbers_provider", dials),
        NodeTest::fake("a_soft_refusal_fails_loud", refused),
        NodeTest::live("one_real_outbound_call", "elevenlabs", live_call)
            .with_fixture(fixture_spec(
                "ELEVENLABS_PHONE_NUMBER_ID",
                "Phone number id",
                "An imported phone number's id on the connected account (Agents > Phone Numbers > the number's id). The call goes out FROM this number.",
            ))
            .with_fixture(fixture_spec(
                "ELEVENLABS_TO_NUMBER",
                "Number to call",
                "The E.164 number the test REALLY CALLS (your own phone): it rings once with a short test message; you can decline or hang up.",
            )),
    ]
}

/// One real outbound call: a throwaway agent is minted, the fixture
/// number is REALLY DIALED (announce itself and hang up), the
/// conversation is read back through Get Conversation (no wait, so
/// the test never depends on the call being answered), and the agent
/// is deleted again. The conversation row the call leaves on the
/// account IS the proof and stays.
async fn live_call(rig: LiveRig) -> WeftResult<()> {
    let phone_number = rig.fixture("ELEVENLABS_PHONE_NUMBER_ID")?;
    let to_number = rig.fixture("ELEVENLABS_TO_NUMBER")?;
    let conn = rig.connect().await?;
    let voice = crate::testing::first_voice_id(&conn).await?;

    let created = rig
        .run(
            &crate::eleven_labs_create_agent::ElevenLabsCreateAgentNode,
            json!({
                "account": rig.access("elevenlabs"),
                "name": "weft-node-tests",
                "prompt": "You are a weft node test call for {{tester}}. Say exactly: this \
                           is an automated weft test call, goodbye. Then end the call.",
                "firstMessage": "This is an automated weft test call. Goodbye!",
                "voice": voice,
                "language": "en",
            }),
        )
        .await
        .ok()?;
    let agent = created.output("agentId")?.as_str().expect("agent id").to_string();

    weft::with_cleanup(
        || async {
            let outcome = rig
                .run(
                    &ElevenLabsAgentCallNode,
                    json!({
                        "account": rig.access("elevenlabs"),
                        "agent": agent.clone(),
                        "phoneNumber": phone_number,
                        "toNumber": to_number,
                        "dynamicVariables": { "tester": "weft" },
                    }),
                )
                .await
                .ok()?;
            let conversation = outcome
                .output("conversationId")?
                .as_str()
                .expect("conversation id")
                .to_string();

            // Read the conversation WITHOUT waiting: the row exists the
            // moment the dial goes out, whatever the callee does.
            let read = rig
                .run(
                    &crate::eleven_labs_get_conversation::ElevenLabsGetConversationNode,
                    json!({
                        "account": rig.access("elevenlabs"),
                        "conversationId": conversation,
                        "waitUntilDone": false,
                        "includeAudio": false,
                    }),
                )
                .await
                .ok()?;
            let status = read.output("status")?.as_str().unwrap_or_default().to_string();
            assert!(
                ["initiated", "in-progress", "processing", "done", "failed"]
                    .contains(&status.as_str()),
                "the conversation reads back with a real status: {status}"
            );
            Ok(())
        },
        || async { crate::testing::delete_agent(&conn, &agent).await },
    )
    .await
}

async fn dials(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "GET",
        "/v1/convai/phone-numbers/pn-1",
        json!({ "phone_number_id": "pn-1", "provider": "twilio" }),
    );
    rig.respond(
        "POST",
        "/v1/convai/twilio/outbound-call",
        json!({ "success": true, "conversation_id": "conv-1", "callSid": "CA123" }),
    );
    let outcome = rig
        .run(
            &ElevenLabsAgentCallNode,
            json!({
                "account": rig.access("elevenlabs"),
                "agent": "agent-1",
                "phoneNumber": "pn-1",
                "toNumber": "+15551234567",
                "dynamicVariables": { "customer_name": "Ada" },
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["conversationId"], json!("conv-1"));
    assert_eq!(outcome.outputs["callSid"], json!("CA123"));

    let call = rig.requests()[1].body.clone().expect("json payload");
    assert_eq!(call["agent_id"], json!("agent-1"));
    assert_eq!(call["to_number"], json!("+15551234567"));
    assert_eq!(
        call["conversation_initiation_client_data"]["dynamic_variables"]["customer_name"],
        json!("Ada")
    );
    Ok(())
}

async fn refused(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "GET",
        "/v1/convai/phone-numbers/pn-2",
        json!({ "phone_number_id": "pn-2", "provider": "sip_trunk" }),
    );
    rig.respond(
        "POST",
        "/v1/convai/sip-trunk/outbound-call",
        json!({ "success": false, "message": "agent has no voice configured" }),
    );
    let outcome = rig
        .run(
            &ElevenLabsAgentCallNode,
            json!({
                "account": rig.access("elevenlabs"),
                "agent": "agent-1",
                "phoneNumber": "pn-2",
                "toNumber": "+15550000000",
            }),
        )
        .await;
    let err = outcome.result.expect_err("a soft refusal must fail").to_string();
    assert!(err.contains("no voice configured"), "{err}");
    Ok(())
}
