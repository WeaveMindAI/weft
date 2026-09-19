//! How a stream's messages are framed on an HTTP response body. Pure:
//! one function per format, unit tested here, used by `Stream`.
//!
//!   - `raw`: the payload as-is (a text delta becomes text, a JSON
//!     object becomes its JSON), one write per message, no separator.
//!   - `ndjson`: one JSON value per line (`application/x-ndjson`), what
//!     a script reading line by line wants.
//!   - `sse`: server-sent events (`text/event-stream`), what a browser's
//!     `EventSource` and LLM-style clients read: `data: <line>` per line
//!     of the payload, a blank line after each event.

use serde_json::Value;

/// The framings `Stream { format }` accepts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    Raw,
    Ndjson,
    Sse,
}

impl Format {
    /// The node-facing spelling (`raw`, `ndjson`, `sse`).
    pub fn parse(raw: &str) -> Result<Self, String> {
        match raw {
            "raw" => Ok(Self::Raw),
            "ndjson" => Ok(Self::Ndjson),
            "sse" => Ok(Self::Sse),
            other => Err(format!("format must be raw, ndjson or sse, got '{other}'")),
        }
    }

    /// The content type the format implies, or `None` for `raw` (the
    /// payload's own shape decides it).
    pub fn content_type(self) -> Option<&'static str> {
        match self {
            Self::Raw => None,
            Self::Ndjson => Some("application/x-ndjson"),
            Self::Sse => Some("text/event-stream"),
        }
    }

    /// What the worker may write while the stream is quiet, which a
    /// reader of the format ignores: an SSE comment line (`EventSource`
    /// drops lines starting with a colon), an empty line between ndjson
    /// values (a line reader skips it). Nothing for `raw`, where every
    /// byte is payload. The write is how a caller who hung up without
    /// a word is found: a quiet feed (a watched table that never
    /// changes) would otherwise hold its run open for ever.
    pub fn keepalive(self) -> Option<&'static str> {
        match self {
            Self::Raw => None,
            Self::Ndjson => Some("\n"),
            Self::Sse => Some(": keepalive\n\n"),
        }
    }
}

/// The text a payload contributes to a framed line: a string as
/// itself (an LLM delta stays a delta), anything else as JSON.
fn payload_text(payload: &Value) -> String {
    match payload {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// One server-sent event. Every line of the payload's text becomes
/// its own `data:` line (the SSE grammar forbids a newline inside one),
/// and the event ends with a blank line. A JSON payload is one line
/// (its serialization has no raw newlines).
pub fn sse_event(payload: &Value) -> String {
    let text = payload_text(payload);
    let mut out = String::with_capacity(text.len() + 16);
    for line in text.split('\n') {
        out.push_str("data: ");
        out.push_str(line);
        out.push('\n');
    }
    out.push('\n');
    out
}

/// One ndjson line: the payload as JSON, newline-terminated. A string
/// payload is JSON-quoted (a line must be one JSON value).
pub fn ndjson_line(payload: &Value) -> String {
    let mut out = payload.to_string();
    out.push('\n');
    out
}
