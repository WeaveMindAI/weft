//! ChatHistoryAppend self-tests: appending in stored form to a
//! conversation file, and the tool-role rules.

use serde_json::json;

use weft::storage::StoredFile;
use weft::{FakeRig, NodeTest, WeftResult};

use super::super::testing::{conversation, conversation_file};
use super::ChatHistoryAppendNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("appends_a_user_message_to_the_conversation_file", appends_user),
        NodeTest::fake("with_no_file_a_message_starts_a_new_conversation_file", starts_a_file),
        NodeTest::fake("media_rides_the_appended_message_as_stored_slots", appends_media),
        NodeTest::fake("a_null_in_a_media_list_is_left_out", null_media_left_out),
        NodeTest::fake("a_tool_message_needs_its_call_id", tool_needs_call_id),
        NodeTest::fake("a_call_id_only_belongs_on_a_tool_message", call_id_only_on_tool),
        NodeTest::fake("refuses_a_message_with_no_substance", refuses_empty),
        NodeTest::fake("a_cache_breakpoint_marks_the_message", cache_breakpoint),
        NodeTest::fake("a_tool_result_is_appended_in_place", appends_to_file),
        NodeTest::fake("two_appends_at_once_both_land", appends_at_once),
    ]
}

async fn cache_breakpoint(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig
        .run(
            &ChatHistoryAppendNode,
            json!({ "role": "system", "text": "persona", "cacheBreakpoint": true }),
        )
        .await
        .ok()?;
    let out = conversation(&rig, outcome.output("historyFile")?)?;
    assert_eq!(out[0]["cache_breakpoint"], json!(true));
    let plain = rig
        .run(&ChatHistoryAppendNode, json!({ "role": "user", "text": "hi" }))
        .await
        .ok()?;
    assert!(conversation(&rig, plain.output("historyFile")?)?[0].get("cache_breakpoint").is_none(), "off by default");
    Ok(())
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
    let out = conversation(&rig, outcome.output("historyFile")?)?;
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

async fn null_media_left_out(rig: FakeRig) -> WeftResult<()> {
    // A loop's gathered list: a null for each iteration that made no image.
    let first = rig.store_file("one.png", "image/png", b"ONE".to_vec());
    let second = rig.store_file("two.png", "image/png", b"TWO".to_vec());
    let outcome = rig
        .run(
            &ChatHistoryAppendNode,
            json!({ "role": "user", "text": "the screenshots", "media": [null, first, null, second] }),
        )
        .await
        .ok()?;
    let out = conversation(&rig, outcome.output("historyFile")?)?;
    let parts = out[0]["content"].as_array().expect("parts");
    let kinds: Vec<&str> = parts.iter().map(|p| p["type"].as_str().expect("a type")).collect();
    assert_eq!(kinds, vec!["text", "image_url", "image_url"], "{parts:?}");
    // Every slot null is no media at all: a message with nothing else is refused.
    let empty = rig.run(&ChatHistoryAppendNode, json!({ "role": "user", "media": [null, null] })).await;
    let err = empty.result.expect_err("only nulls is no media").to_string();
    assert!(err.contains("text or media"), "{err}");
    Ok(())
}

async fn appends_user(rig: FakeRig) -> WeftResult<()> {
    let file = conversation_file(&rig, json!([{ "role": "system", "content": "be terse" }]));
    let outcome = rig
        .run(
            &ChatHistoryAppendNode,
            json!({ "historyFile": file.clone(), "role": "user", "text": "hi" }),
        )
        .await
        .ok()?;
    let after = outcome.output("historyFile")?;
    assert_eq!(StoredFile::from_value(after)?.key, StoredFile::from_value(&file)?.key, "the same file");
    let out = conversation(&rig, after)?;
    assert_eq!(out.len(), 2);
    assert_eq!(out[0]["role"], json!("system"), "existing messages stand");
    assert_eq!(out[1]["role"], json!("user"));
    // The change is recorded for the inspector as added lines.
    let edits = rig.file_edits();
    assert_eq!(edits.len(), 1);
    assert_eq!((edits[0].from_version, edits[0].to_version), (Some(1), 2));
    assert!(
        edits[0].diff.lines().any(|line| line.starts_with('+') && line.contains("\"hi\"")),
        "the new message reads as an added line: {}",
        edits[0].diff
    );
    Ok(())
}

/// With no file in, the message starts a conversation of its own, a file
/// of the run like any output the run made.
async fn starts_a_file(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig
        .run(&ChatHistoryAppendNode, json!({ "role": "user", "text": "hi" }))
        .await
        .ok()?;
    let file = StoredFile::from_value(outcome.output("historyFile")?)?;
    let meta = rig.stored_meta(&file.key)?;
    assert_eq!(meta.keep_ttl_secs, None, "a started conversation has no deadline, it goes with its run: {meta:?}");
    assert!(file.key.contains("/run/"), "run-scoped: a started conversation is the run's: {}", file.key);
    assert_eq!(conversation(&rig, outcome.output("historyFile")?)?.len(), 1);
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

/// A tool result fed back into a conversation kept in a file: appended
/// into that same file, which goes out on `historyFile`.
async fn appends_to_file(rig: FakeRig) -> WeftResult<()> {
    let file = conversation_file(&rig, json!([
        { "role": "user", "content": "render" },
        { "role": "assistant", "content": "", "tool_calls": [{ "id": "t1", "name": "render", "arguments": "{}" }] },
    ]));
    let outcome = rig
        .run(&ChatHistoryAppendNode, json!({ "role": "tool", "text": "rendered", "toolCallId": "t1", "historyFile": file.clone() }))
        .await
        .ok()?;
    let after = outcome.output("historyFile")?;
    assert_eq!(StoredFile::from_value(after)?.key, StoredFile::from_value(&file)?.key, "the same file");
    let written = conversation(&rig, after)?;
    assert_eq!(written.len(), 3);
    assert_eq!(written[2], json!({ "role": "tool", "content": "rendered", "tool_call_id": "t1" }));
    Ok(())
}

/// Two appends to one file at the same moment (two runs, or two
/// iterations of a parallel loop): neither is lost, whichever lands
/// second is added after the first.
async fn appends_at_once(rig: FakeRig) -> WeftResult<()> {
    let file = conversation_file(&rig, json!([]));
    let (a, b) = tokio::join!(
        rig.run(&ChatHistoryAppendNode, json!({ "role": "user", "text": "one", "historyFile": file.clone() })),
        rig.run(&ChatHistoryAppendNode, json!({ "role": "user", "text": "two", "historyFile": file.clone() })),
    );
    a.ok()?;
    b.ok()?;
    let mut texts: Vec<String> = conversation(&rig, &file)?
        .iter()
        .map(|m| m["content"].as_str().unwrap_or_default().to_string())
        .collect();
    texts.sort();
    assert_eq!(texts, vec!["one", "two"], "both appends landed");
    assert_eq!(rig.file_edits().len(), 2);
    Ok(())
}
