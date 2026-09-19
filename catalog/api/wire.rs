//! The shared translation between port values and the caller's wire,
//! per the trigger's declared data type (`json`, `text`, `bytes`):
//!
//!   - OUT (`Reply`, `Stream`): a port value becomes an
//!     [`OutboundChunk`]. `json` carries any value; `text` wants a
//!     string; `bytes` wants a stored file (weft's one representation
//!     of bytes), whose content the node reads back.
//!   - IN (`Route`, `Socket`): an [`InboundMessage`] becomes a port
//!     value. A byte body is stored as an execution-scoped file and
//!     the stored-file value is what flows.
//!
//! Bytes never travel a wire: a picture a caller sends inside a JSON
//! body (a `data:` URL or bare base64 on a port declared as a file
//! kind) is stored on the way in and the stored-file value is what
//! flows ([`inline_files`]); a stored file inside a JSON answer goes
//! out as a link the caller can fetch ([`link_files`]).
//!
//! One place, so the four nodes agree and a fifth node an author writes
//! can reuse it.

use serde_json::Value;

use weft::caller::{InboundMessage, LiveRequest, OutboundChunk, ResponseHead};
use weft::node::NodeOutput;
use weft::signal::DataType;
use weft::storage::{FileHandle, StorageScope, StoredFile};
use weft::{ExecutionContext, WeftResult, WeftType};

/// The fixed ports both triggers emit from the caller's opening
/// request. A body key of the same name is shadowed by these (the
/// request wins), which the trigger docs say.
pub const REQUEST_PORTS: &[&str] = &["method", "path", "params", "query", "headers", "caller"];

/// The opening request the trigger woke on, out of the wake payload.
/// Both triggers read the same payload, so they read it here and a
/// payload that is not a request is refused in the same words.
pub fn opening_request(ctx: &ExecutionContext) -> WeftResult<LiveRequest> {
    ctx.wake.record().and_then(|v| {
        serde_json::from_value(v)
            .map_err(|e| weft::node_error(format!("the wake payload is not a caller request: {e}")))
    })
}

/// The fixed request ports' values, as `(port, value)` pairs, from the
/// caller's opening request. A trigger sets the ones it declares
/// (`Route` all six, `Socket` everything but `method`: an upgrade is
/// always a GET) AFTER the body ports, so a body key cannot shadow
/// them. Header names are lowercased, as HTTP reads them.
pub fn request_ports(request: &LiveRequest) -> Vec<(&'static str, Value)> {
    let headers: serde_json::Map<String, Value> = request
        .headers
        .iter()
        .map(|(k, v)| (k.to_ascii_lowercase(), Value::String(v.clone())))
        .collect();
    vec![
        ("method", Value::String(request.method.clone())),
        ("path", Value::String(request.path.clone())),
        ("params", serde_json::to_value(&request.params).expect("a string map serializes")),
        ("query", serde_json::to_value(&request.query).expect("a string map serializes")),
        ("headers", Value::Object(headers)),
        ("caller", request.caller.clone().unwrap_or(Value::Null)),
    ]
}

/// A port value as the wire wants it for `data_type`. A JSON value's
/// stored files go out as links (see [`link_files`]).
pub async fn chunk_for(
    ctx: &ExecutionContext,
    data_type: DataType,
    value: Value,
) -> WeftResult<OutboundChunk> {
    match data_type {
        DataType::Json => Ok(OutboundChunk::Json(link_files(ctx, value).await?)),
        DataType::Text => match value {
            Value::String(s) => Ok(OutboundChunk::Text(s)),
            other => weft::node_bail!(
                "this caller speaks text (the trigger's dataType), so the body must be a \
                 String; got {}",
                kind_of(&other)
            ),
        },
        DataType::Bytes => {
            // A stored-file VALUE (the marker object), never a bare key
            // string: a key is an implementation detail no port carries.
            let file = StoredFile::from_value(&value).map_err(|e| {
                weft::node_error(format!(
                    "this caller speaks bytes (the trigger's dataType), so the body must be a \
                     stored file; {e}"
                ))
            })?;
            let (_, bytes) = ctx
                .storage(StorageScope::Execution)
                .get_bytes(&FileHandle::Key(file.key))
                .await?;
            Ok(OutboundChunk::Bytes(bytes.to_vec()))
        }
    }
}

/// An inbound message as a port value. Bytes are stored (execution
/// scope, swept with the run) under `filename`, and the stored-file
/// value is what flows.
///
/// The file's kind comes from its own first bytes, not from what the
/// caller's `Content-Type` claimed. A header is a caller's word for it,
/// and a port declaring `Image` would otherwise be handed a zip that
/// said `image/png`, which is the same trust a JSON body's inline
/// picture is never given (`inline_files` sniffs and checks). The header
/// is the fallback for bytes carrying no signature weft knows, and a
/// socket has no header at all, so without the sniff every binary frame
/// would be a `Blob` and an `Image` port could never be served.
///
/// `declared` is what the port receiving this says it takes, when one
/// port takes the whole message. Bytes that are not that kind fail here,
/// naming both kinds, rather than inside whatever node reads the file.
pub async fn value_of(
    ctx: &ExecutionContext,
    message: InboundMessage,
    content_type: Option<&str>,
    filename: &str,
    declared: Option<&WeftType>,
) -> WeftResult<Value> {
    match message {
        InboundMessage::Json(v) => Ok(v),
        InboundMessage::Text(s) => Ok(Value::String(s)),
        InboundMessage::Bytes(b) => {
            if let Some(declared) = declared {
                weft::storage::check_declared_kind(declared, &b).map_err(|why| {
                    weft::node_error(format!(
                        "port '{filename}': the caller's bytes are not what the port declares: {why}"
                    ))
                })?;
            }
            let mime = weft::storage::sniff_mime(&b)
                .or(content_type)
                .unwrap_or("application/octet-stream")
                .to_string();
            ctx.storage(StorageScope::Execution).put(b, &mime, filename, None).await
        }
    }
}

/// The declared ports of a trigger typed as a file kind (`File`,
/// `Image`, `Video`, `Audio`, `Blob`, or a union of those): where a
/// caller's inline bytes are stored on the way in.
fn file_ports(ctx: &ExecutionContext) -> Vec<(String, WeftType)> {
    // A socket's `inbound` is a stream of messages (`Generator[Image]`):
    // the element type is what each message is held to.
    let mut ports: Vec<(String, WeftType)> = ctx
        .declared_outputs()
        .iter()
        .map(|(name, ty)| (name.clone(), ty.as_generator().unwrap_or(ty).clone()))
        .filter(|(_, ty)| ty.is_file_valued())
        .collect();
    ports.sort_by(|a, b| a.0.cmp(&b.0));
    ports
}

/// Turn every inline picture a JSON body put on a file-kind port into a
/// stored file, so the value that flows is the stored-file marker and
/// never the bytes. A `data:<mime>;base64,<payload>` URL keeps its
/// media type; a bare base64 string is typed by its own bytes (a PNG
/// signature says `image/png`) and `application/octet-stream` when
/// they say nothing. The bytes are held to the port's declared kind
/// (base64 of a video on an `Image` port fails loud). A value that is
/// already a stored-file marker passes; anything else on a file port
/// is refused by name. Files land at execution scope, swept with the
/// run: the program keeps what it wants at project scope.
pub async fn inline_files(ctx: &ExecutionContext, output: NodeOutput) -> WeftResult<NodeOutput> {
    let mut out = output;
    for (port, declared) in file_ports(ctx) {
        let Some(value) = out.get(&port).cloned() else { continue };
        let stored = match value {
            Value::String(text) => {
                let (mime, bytes) = decode_inline(&text).map_err(|why| {
                    weft::node_error(format!(
                        "port '{port}' is declared {declared}, so its body key carries a picture as a \
                         data: URL or base64; {why}"
                    ))
                })?;
                let mime = match mime {
                    Some(mime) => mime,
                    None => weft::storage::sniff_mime(&bytes).unwrap_or("application/octet-stream").to_string(),
                };
                weft::storage::check_declared_kind(&declared, &bytes).map_err(|why| {
                    weft::node_error(format!("port '{port}': the caller's bytes are not what the port declares: {why}"))
                })?;
                ctx.storage(StorageScope::Execution).put(bytes, &mime, &port, None).await?
            }
            Value::Object(_) => {
                StoredFile::from_value(&value).map_err(|e| {
                    weft::node_error(format!("port '{port}' is declared {declared}; {e}"))
                })?;
                value
            }
            other => weft::node_bail!(
                "port '{port}' is declared {declared}, so its body key carries a picture as a data: \
                 URL, base64 or a stored-file value; got {}",
                kind_of(&other)
            ),
        };
        out = out.set(port, stored);
    }
    Ok(out)
}

/// The bytes an inline body key carries, and the media type when the
/// key said one: a `data:` URL names it, bare base64 does not.
fn decode_inline(text: &str) -> Result<(Option<String>, Vec<u8>), String> {
    if let Some(rest) = text.strip_prefix("data:") {
        let (mime, payload) = rest
            .split_once(";base64,")
            .ok_or_else(|| "the data: URL carries no base64 payload".to_string())?;
        let bytes = weft::storage::media::base64_decode(payload)
            .map_err(|e| format!("the data: URL payload is not valid base64: {e}"))?;
        return Ok((Some(mime.to_string()), bytes));
    }
    let bytes = weft::storage::media::base64_decode(text.trim())
        .map_err(|e| format!("the string is not valid base64: {e}"))?;
    Ok((None, bytes))
}

/// Every stored file inside a JSON answer, replaced by what a caller can
/// use: `{ "url", "mimeType", "filename", "sizeBytes" }`, the url being a
/// link this install serves the caller (a browser follows it straight
/// into an `<img>`). Walks lists and objects, so a file nested in a row
/// is linked too. A file the install cannot mint a link for fails the
/// node: an answer with no way to the bytes is not an answer.
pub async fn link_files(ctx: &ExecutionContext, value: Value) -> WeftResult<Value> {
    match value {
        Value::Object(map) => {
            if let Ok(file) = StoredFile::from_value(&Value::Object(map.clone())) {
                let url = ctx
                    .storage(StorageScope::Execution)
                    .caller_link(&FileHandle::Key(file.key.clone()), None)
                    .await?
                    .ok_or_else(|| {
                        weft::node_error(format!(
                            "the answer carries the stored file {} but this install has no address a \
                             caller could fetch it from",
                            file.filename
                        ))
                    })?;
                return Ok(serde_json::json!({
                    "url": url,
                    "mimeType": file.mime_type,
                    "filename": file.filename,
                    "sizeBytes": file.size_bytes,
                }));
            }
            let mut linked = serde_json::Map::with_capacity(map.len());
            for (key, inner) in map {
                linked.insert(key, Box::pin(link_files(ctx, inner)).await?);
            }
            Ok(Value::Object(linked))
        }
        Value::Array(items) => {
            let mut linked = Vec::with_capacity(items.len());
            for item in items {
                linked.push(Box::pin(link_files(ctx, item)).await?);
            }
            Ok(Value::Array(linked))
        }
        other => Ok(other),
    }
}

/// The response head a node's `status` and `headers` inputs describe.
/// `headers` is a JSON object of string values (the port's declared
/// type); anything else is refused by name.
pub fn head_for(status: u16, headers: Option<&Value>) -> WeftResult<ResponseHead> {
    let mut head = ResponseHead::new(status);
    if let Some(headers) = headers {
        let Some(map) = headers.as_object() else {
            weft::node_bail!("headers must be an object of header name to value, got {}", kind_of(headers));
        };
        for (name, value) in map {
            let Some(value) = value.as_str() else {
                weft::node_bail!("header '{name}' must be a string, got {}", kind_of(value));
            };
            head = head.with_header(name.clone(), value.to_string());
        }
    }
    Ok(head)
}

/// Refuse a status or headers behind a Socket: a socket message has no
/// status line, so a program that set one meant a Route. `status` is
/// the node's default (200) and `headers` absent or empty when nothing
/// was set; anything else is the program's bug, said loud.
pub fn refuse_head_on_socket(node: &str, status: u16, headers: Option<&Value>) -> WeftResult<()> {
    let has_headers = headers.is_some_and(|h| !h.as_object().is_some_and(|m| m.is_empty()));
    if status != 200 || has_headers {
        weft::node_bail!(
            "{node} behind a Socket sends messages; a status ({status}) or headers have no \
             meaning on a socket. Drop them, or put the {node} behind a Route"
        );
    }
    Ok(())
}

/// A one-word description of a value's shape, for error messages.
pub fn kind_of(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "a boolean",
        Value::Number(_) => "a number",
        Value::String(_) => "a string",
        Value::Array(_) => "a list",
        Value::Object(_) => "an object",
    }
}
