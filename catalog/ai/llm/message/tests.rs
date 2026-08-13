//! ChatHistoryAppend self-tests: appending in stored form, and the
//! tool-role rules.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::ChatHistoryAppendNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("appends_a_user_message_to_the_history", appends_user),
        NodeTest::fake("media_rides_the_appended_message_as_stored_slots", appends_media),
        NodeTest::fake("a_tool_message_needs_its_call_id", tool_needs_call_id),
        NodeTest::fake("a_call_id_only_belongs_on_a_tool_message", call_id_only_on_tool),
        NodeTest::fake("refuses_a_message_with_no_substance", refuses_empty),
    ]
}

async fn appends_media(rig: FakeRig) -> WeftResult<()> {
    let image = rig.store_file("cat.png", "image/png", b"PNGDATA".to_vec());
    let outcome = rig
        .run(
            &ChatHistoryAppendNode,
            json!({ "role": "user", "text": "look", "media": image }),
        )
        .await
        .ok()?;
    // The stored form: parts, text first, then the media slot holding
    // the stored-file value VERBATIM (never bytes, never a URL).
    let out = outcome.outputs["history"].as_array().expect("history is a list").clone();
    let parts = out[0]["content"].as_array().expect("parts");
    assert_eq!(parts[0]["type"], json!("text"));
    assert_eq!(parts[0]["text"], json!("look"));
    assert_eq!(parts[1]["type"], json!("image_url"));
    assert!(
        parts[1]["image_url"]["url"].get("__weft_image__").is_some(),
        "the media slot must hold the stored value, got {}",
        parts[1]["image_url"]["url"]
    );
    Ok(())
}

async fn appends_user(rig: FakeRig) -> WeftResult<()> {
    let history = json!([{ "role": "system", "content": "be terse" }]);
    let outcome = rig
        .run(
            &ChatHistoryAppendNode,
            json!({ "history": history, "role": "user", "text": "hi" }),
        )
        .await
        .ok()?;
    let out = outcome.outputs["history"].as_array().expect("history is a list").clone();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0]["role"], json!("system"), "existing messages stand");
    assert_eq!(out[1]["role"], json!("user"));
    Ok(())
}

async fn tool_needs_call_id(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig
        .run(&ChatHistoryAppendNode, json!({ "role": "tool", "text": "result" }))
        .await;
    let err = outcome.result.expect_err("tool without id must refuse").to_string();
    assert!(err.contains("toolCallId"), "{err}");
    Ok(())
}

async fn call_id_only_on_tool(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig
        .run(
            &ChatHistoryAppendNode,
            json!({ "role": "user", "text": "hi", "toolCallId": "call_1" }),
        )
        .await;
    let err = outcome.result.expect_err("user with call id must refuse").to_string();
    assert!(err.contains("only belongs on a 'tool' message"), "{err}");
    Ok(())
}

async fn refuses_empty(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig.run(&ChatHistoryAppendNode, json!({ "role": "user" })).await;
    let err = outcome.result.expect_err("no text, no media must refuse").to_string();
    assert!(err.contains("text or media"), "{err}");
    Ok(())
}
