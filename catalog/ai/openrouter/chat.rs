//! Stored-form chat helpers shared by the package members (the
//! inference call and the provider-agnostic ChatHistoryAppend).
//!
//! A `ChatHistory` value keeps media as weft stored-file values inside
//! minillmlib-shaped messages. The type is declared once, in this
//! package's root `metadata.json`. The declared fields mirror
//! minillmlib's `Message` serde exactly: a field the declaration
//! omitted would be DROPPED when a consumer round-trips the history
//! through `serde_json::from_value::<Message>`, so "unused" optional
//! fields (`name`, `tool_call_id`, `cache_breakpoint`) are part of the
//! mirror, not decoration. These helpers build the stored form; a
//! provider call externalizes it at the boundary.
// SYNC: types.ChatMessage/ChatContentPart (../metadata.json) <->
//       MiniLLMLibRS/src/message/mod.rs Message,
//       MiniLLMLibRS/src/message/content.rs ContentPart

use serde_json::{json, Value};

use weft::weft_type::FileKind;
use weft::WeftResult;

/// The content part wrapping one stored media value, keyed by the
/// marker's own kind. The slot keeps the stored-file value verbatim
/// (the stored form of a history holds references, never bytes).
pub fn media_part(media: &Value) -> WeftResult<Value> {
    let kind = media
        .as_object()
        .and_then(FileKind::from_marker_obj)
        .ok_or_else(|| weft::node_error("a media attachment is not a stored file value"))?;
    Ok(match kind {
        FileKind::Image => json!({ "type": "image_url", "image_url": { "url": media } }),
        FileKind::Audio => json!({ "type": "input_audio", "input_audio": { "data": media } }),
        FileKind::Video => json!({ "type": "video_url", "video_url": { "url": media } }),
        FileKind::Blob => {
            return Err(weft::node_error(
                "a chat message carries images, audio, or video; a generic file does not \
                 fit a provider's message parts",
            ))
        }
    })
}

/// The `media` input's one-or-many forms, normalized: absent/null is
/// none, a list is itself, a single stored value is a one-item list.
pub fn media_items(value: Option<Value>) -> Vec<Value> {
    match value {
        None | Some(Value::Null) => Vec::new(),
        Some(Value::Array(items)) => items,
        Some(single) => vec![single],
    }
}

/// The stored-form message for one turn: plain text content, or parts
/// (text first, then each stored media).
pub fn stored_message(role: &str, text: &str, media: &[Value]) -> WeftResult<Value> {
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
    Ok(json!({ "role": role, "content": content }))
}
