//! ElevenLabsGetConversation self-tests.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::ElevenLabsGetConversationNode;

pub fn tests() -> Vec<NodeTest> {
    // Live coverage rides the agent_call round trip: its live test
    // reads the conversation it just dialed back THROUGH THIS NODE
    // (mint agent -> call -> Get Conversation -> delete agent). A
    // standalone live read cannot exist on a self-cleaning account:
    // every past conversation's agent is deleted by the tests that
    // minted it, and ElevenLabs answers 404 for a conversation whose
    // agent is gone, so there is never an old readable conversation
    // to pick up.
    vec![NodeTest::fake("reads_and_shapes_a_done_conversation", reads)]
}

async fn reads(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "GET",
        "/v1/convai/conversations/conv-1",
        json!({
            "status": "done",
            "transcript": [
                { "role": "agent", "message": "Hello!", "extra": "dropped" },
                { "role": "user", "message": "Hi." },
            ],
            "analysis": {
                "transcript_summary": "A greeting.",
                "call_successful": "success",
            },
        }),
    );
    rig.respond_raw(
        "GET",
        "/v1/convai/conversations/conv-1/audio",
        200,
        "audio/mpeg",
        b"call-audio".to_vec(),
    );
    let outcome = rig
        .run(
            &ElevenLabsGetConversationNode,
            json!({
                "account": rig.access("elevenlabs"),
                "conversationId": "conv-1",
                "waitUntilDone": true,
                "includeAudio": true,
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["status"], json!("done"));
    assert_eq!(outcome.outputs["text"], json!("agent: Hello!\nuser: Hi."));
    assert_eq!(outcome.outputs["summary"], json!("A greeting."));
    assert_eq!(outcome.outputs["successful"], json!("success"));
    assert!(outcome.outputs["audio"].is_object(), "the recording lands as a stored file");
    Ok(())
}
