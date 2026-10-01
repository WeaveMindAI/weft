//! Stored-form chat helpers shared by the package members (the
//! inference calls and the provider-agnostic ChatHistoryAppend).
//!
//! A `ChatHistory` value keeps media as weft stored-file values inside
//! minillmlib-shaped messages. The type is declared once, in this
//! package's root `metadata.json`. The declared fields mirror
//! minillmlib's `Message` serde exactly: a field the declaration
//! omitted would be DROPPED when a consumer round-trips the history
//! through `serde_json::from_value::<Message>`, so "unused" optional
//! fields (`name`, `cache_breakpoint`) are part of the mirror, not
//! decoration. These helpers build the stored form; a provider call
//! externalizes it at the boundary.
// SYNC: types.ChatMessage/ChatContentPart/ToolCall (metadata.json) <->
//       MiniLLMLibRS/src/message/mod.rs Message,
//       MiniLLMLibRS/src/message/content.rs ContentPart,
//       MiniLLMLibRS/src/tools/mod.rs ToolCall

use serde_json::{json, Value};

use weft::storage::{FileHandle, KeepTtl, StorageScope};
use weft::weft_type::FileKind;
use weft::{ExecutionContext, WeftError, WeftResult};

/// The content part wrapping one stored media value, keyed by the
/// marker's own kind. The slot keeps the stored-file value verbatim
/// (the stored form of a history holds references, never bytes).
pub fn media_part(media: &Value) -> WeftResult<Value> {
    let kind = media
        .as_object()
        .and_then(FileKind::from_marker_obj)
        .ok_or_else(|| {
            WeftError::Input("a media attachment is not a stored file value".to_string())
        })?;
    Ok(match kind {
        FileKind::Image => json!({ "type": "image_url", "image_url": { "url": media } }),
        FileKind::Audio => json!({ "type": "input_audio", "input_audio": { "data": media } }),
        FileKind::Video => json!({ "type": "video_url", "video_url": { "url": media } }),
        FileKind::Blob => {
            return Err(WeftError::Input(
                "a chat message carries images, audio, or video; a generic file does not \
                 fit a provider's message parts"
                    .to_string(),
            ))
        }
    })
}

/// The stored-form message for one turn: plain text content, or parts
/// (text first, then each stored media). A tool-result message (`role:
/// tool`) carries the id of the call it answers.
pub fn stored_message(
    role: &str,
    text: &str,
    media: &[Value],
    tool_call_id: Option<&str>,
) -> WeftResult<Value> {
    let content = if media.is_empty() {
        Value::String(text.to_string())
    } else {
        let mut parts = Vec::new();
        if !text.is_empty() {
            parts.push(json!({ "type": "text", "text": text }));
        }
        for item in media {
            parts.push(media_part(item)?);
        }
        Value::Array(parts)
    };
    let mut message = json!({ "role": role, "content": content });
    if let Some(id) = tool_call_id {
        message["tool_call_id"] = json!(id);
    }
    Ok(message)
}

/// Whether a stored message carries a cache mark.
pub fn has_cache_mark(message: &Value) -> bool {
    message.get("cache_breakpoint").and_then(Value::as_bool) == Some(true)
}

/// Put a cache mark on a stored message (the lib turns the mark into
/// the provider's own cache instruction, and caps how many it keeps).
pub fn mark_cache(message: &mut Value) {
    message["cache_breakpoint"] = json!(true);
}

/// The marks `LlmInference`'s `autoCache` puts on a conversation that
/// carries none of its own: the system message (the stable persona
/// every call shares) and the last message of the wired history (the
/// turns before this call's new one, which the next call shares
/// again). A conversation already carrying a mark is the author's to
/// place, and is left alone. `new_turn` is how many messages at the
/// end are this call's own (the user message just appended), so the
/// history's last message is found under them.
pub fn auto_cache_marks(stored: &mut [Value], new_turn: usize) {
    if stored.iter().any(has_cache_mark) {
        return;
    }
    if let Some(system) = stored.first_mut().filter(|m| m.get("role").and_then(Value::as_str) == Some("system")) {
        mark_cache(system);
    }
    let history_len = stored.len().saturating_sub(new_turn);
    // The last history message, when it is not the system message
    // already marked above (a history of one system message has only
    // that to mark).
    if history_len > 1 {
        mark_cache(&mut stored[history_len - 1]);
    }
}

/// What a tool call nobody answered is answered with, so the provider
/// accepts the conversation.
pub const UNANSWERED_TOOL_CALL: &str =
    "cancelled: this tool call was never answered (the run stopped before its result was written)";

/// Answer every tool call in `stored` that no `tool` message answers,
/// with [`UNANSWERED_TOOL_CALL`], placed among the tool messages right
/// after the assistant message that made the call. A run stopped between
/// the model asking for a tool and the result being appended leaves such
/// a call behind, and every provider refuses a conversation holding one,
/// so the next turn would fail for good over a moment nobody chose.
pub fn answer_unanswered_tool_calls(stored: &mut Vec<Value>) {
    let role = |m: &Value| m.get("role").and_then(Value::as_str).map(str::to_string);
    let mut i = 0;
    while i < stored.len() {
        let asked: Vec<String> = match role(&stored[i]).as_deref() {
            Some("assistant") => stored[i]
                .get("tool_calls")
                .and_then(Value::as_array)
                .map(|calls| calls.iter().filter_map(|c| c.get("id").and_then(Value::as_str).map(str::to_string)).collect())
                .unwrap_or_default(),
            _ => Vec::new(),
        };
        let mut end = i + 1;
        let mut answered = Vec::new();
        while end < stored.len() && role(&stored[end]).as_deref() == Some("tool") {
            if let Some(id) = stored[end].get("tool_call_id").and_then(Value::as_str) {
                answered.push(id.to_string());
            }
            end += 1;
        }
        for id in asked.iter().filter(|id| !answered.contains(id)) {
            stored.insert(end, json!({ "role": "tool", "content": UNANSWERED_TOOL_CALL, "tool_call_id": id }));
            end += 1;
        }
        i = end;
    }
}

/// The conversation type, as this package declares it (`ChatHistory`
/// in the root `metadata.json`): what a conversation file is held to,
/// and what the stored-form round trips read media slots by.
pub fn history_type() -> WeftResult<weft::WeftType> {
    weft::WeftType::parse("ChatHistory").ok_or_else(|| {
        WeftError::Type(
            "the package's ChatHistory type does not resolve: the type registry this process \
             installed holds no such declaration"
                .to_string(),
        )
    })
}

/// A conversation as its file holds it: a JSON list with one message
/// per line, so a turn appended reads as added lines in the run's
/// record of the edit.
pub fn conversation_bytes(messages: &[Value]) -> WeftResult<Vec<u8>> {
    let lines = messages
        .iter()
        .map(|m| serde_json::to_string(m))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| WeftError::NodeExecution(format!("serializing the conversation: {e}")))?;
    Ok(if lines.is_empty() { b"[]\n".to_vec() } else { format!("[\n{}\n]\n", lines.join(",\n")).into_bytes() })
}

/// The conversation `bytes` hold (a conversation file's content), held
/// to `history_ty` and refused when it is not one.
pub fn parse_conversation(bytes: &[u8], history_ty: &weft::WeftType) -> WeftResult<Vec<Value>> {
    let not_a_conversation = |why: String| {
        WeftError::Input(format!(
            "the file on historyFile is not a conversation: {why}. It holds a ChatHistory as \
             JSON, `[]` for a new one"
        ))
    };
    let history: Value =
        serde_json::from_slice(bytes).map_err(|e| not_a_conversation(format!("not JSON ({e})")))?;
    history_ty.validate_value(&history).map_err(not_a_conversation)?;
    match history {
        Value::Array(messages) => Ok(messages),
        _ => Err(not_a_conversation("not a list of messages".to_string())),
    }
}

/// The conversation a node was handed on `historyFile`, and that file;
/// an empty conversation and no file when nothing is wired.
pub async fn incoming_history(
    ctx: &ExecutionContext,
    history_ty: &weft::WeftType,
) -> WeftResult<(Vec<Value>, Option<FileHandle>)> {
    let Some(file) = ctx.inputs.opt::<FileHandle>("historyFile")? else {
        return Ok((Vec::new(), None));
    };
    let (_, bytes) = ctx.storage(StorageScope::Project).get_bytes(&file).await?;
    Ok((parse_conversation(&bytes, history_ty)?, Some(file)))
}

/// Add `turn` (stored-form messages) to the end of the conversation in
/// `file`, after `normalize` has put the conversation as it is NOW into
/// the shape the turn was made from. The file is edited in place
/// (`StorageHandle::edit`): a turn another run or a parallel iteration
/// appended in the meantime is kept, and this one lands after it. With
/// no file, the turn starts a new conversation file, kept past the run.
/// Returns the file's value, to go out on `historyFile`.
pub async fn append_turn(
    ctx: &ExecutionContext,
    file: Option<&FileHandle>,
    history_ty: &weft::WeftType,
    turn: &[Value],
    normalize: impl Fn(&mut Vec<Value>) -> WeftResult<()>,
) -> WeftResult<Value> {
    let Some(file) = file else {
        let mut conversation = Vec::new();
        normalize(&mut conversation)?;
        conversation.extend_from_slice(turn);
        // The conversation is the run's product, like a generated
        // image: kept past the run (default access-renewed lifetime) so
        // the file still reads once the run has ended.
        return ctx
            .storage(StorageScope::Execution)
            .put(conversation_bytes(&conversation)?, "application/json", "conversation.json", Some(KeepTtl::Default))
            .await;
    };
    ctx.storage(StorageScope::Project)
        .edit(file, |old| {
            let mut conversation = parse_conversation(old, history_ty)?;
            normalize(&mut conversation)?;
            conversation.extend_from_slice(turn);
            conversation_bytes(&conversation)
        })
        .await
}
