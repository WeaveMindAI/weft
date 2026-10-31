//! The shared translation between port values and the caller's wire,
//! per the trigger's declared data type (`json`, `text`, `bytes`):
//!
//!   - OUT (`Reply`, `Stream`): a port value becomes an
//!     [`OutboundChunk`]. `json` carries any value; `text` wants a
//!     string; `bytes` wants a stored file (weft's one representation
//!     of bytes), whose content the node reads back.
//!   - IN (`Route`, `Socket`): an [`InboundMessage`] becomes the value
//!     the program declared it to be ([`take_message`]), or the
//!     caller's mistake in words the caller can act on. A byte body is
//!     stored as a run-scoped file and the stored-file value is what
//!     flows.
//!
//! Bytes never travel a wire: a picture a caller sends inside a JSON
//! body (a `data:` URL or bare base64 in a place declared as a file
//! kind) is stored on the way in, a file the page uploaded through a
//! slot (`weft-upload:<slot>`) is received, and the stored-file value is
//! what flows ([`take_inbound`]); a file inside a JSON answer
//! goes out as a link the caller can fetch ([`link_files`]).
//!
//! One place, so the four nodes agree and a fifth node an author writes
//! can reuse it.

use serde_json::Value;

use weft::caller::{InboundMessage, LiveRequest, OutboundChunk, ResponseHead};
use weft::signal::DataType;
use weft::storage::{FileHandle, StorageScope, StoredFile};
use weft::weft_type::FileKind;
use weft::{RunContext, WeftError, WeftResult, WeftType};

/// The opening request the trigger woke on, out of the wake payload.
/// Both triggers read the same payload, so they read it here and a
/// payload that is not a request is refused in the same words.
pub fn opening_request(ctx: &RunContext) -> WeftResult<LiveRequest> {
    ctx.wake.record().and_then(|v| {
        serde_json::from_value(v)
            .map_err(|e| weft::node_error(format!("the wake payload is not a caller request: {e}")))
    })
}

/// The fixed request ports' values, as `(port, value)` pairs, from the
/// caller's opening request, keeping only the ports the trigger's own
/// metadata declares (`trigger`): `Route` has no `params` (a program
/// reads a path capture on an output it declares under the capture's
/// name, which a capture called `params` is too), and `Socket` has no
/// `method` (an upgrade is always a GET). `fired` is always true: a port
/// a program can gate on "the trigger was called" without reading any
/// request part. Header names are lowercased, as HTTP reads them. The
/// headers that carry a credential are on `credentials`, as secrets the
/// door made of them (`LiveRequest::credentials`), never on `headers`.
pub fn request_ports(trigger: &weft::NodeMetadata, request: &LiveRequest) -> Vec<(&'static str, Value)> {
    let headers: serde_json::Map<String, Value> = request
        .headers
        .iter()
        .map(|(k, v)| (k.to_ascii_lowercase(), Value::String(v.clone())))
        .collect();
    let credentials: serde_json::Map<String, Value> =
        request.credentials.iter().map(|(k, secret)| (k.to_ascii_lowercase(), secret.to_value())).collect();
    vec![
        ("fired", Value::Bool(true)),
        ("method", Value::String(request.method.clone())),
        ("path", Value::String(request.path.clone())),
        ("params", serde_json::to_value(&request.params).expect("a string map serializes")),
        ("query", serde_json::to_value(&request.query).expect("a string map serializes")),
        ("headers", Value::Object(headers)),
        ("credentials", Value::Object(credentials)),
        ("caller", request.caller.clone().unwrap_or(Value::Null)),
    ]
    .into_iter()
    .filter(|(port, _)| trigger.outputs.iter().any(|o| o.name == *port))
    .collect()
}

/// What an answer node sends for `value`, the value of its input `port`:
/// the chunk `make` builds of it, from the value revealed by the input's
/// type (`RunContext::reveal_input`, which refuses a secret the type does
/// not declare). Where the type holds a secret, the caller receives the
/// value revealed and the run's record keeps it with its markers
/// (`OutboundChunk::Revealed`).
pub async fn revealing<F, Fut>(ctx: &RunContext, port: &str, value: Value, make: F) -> WeftResult<OutboundChunk>
where
    F: FnOnce(Value) -> Fut,
    Fut: std::future::Future<Output = WeftResult<OutboundChunk>>,
{
    if !ctx.declared_inputs().get(port).is_some_and(WeftType::holds_secret) {
        return make(ctx.reveal_input(port, value).await?).await;
    }
    let sent = ctx.reveal_input(port, value.clone()).await?;
    Ok(OutboundChunk::Revealed { sent: Box::new(make(sent).await?), kept: value })
}

/// Refuse a bus message holding a secret's marker, naming its place: a bus
/// carries no type, so there is no declared type to reveal it by (the
/// typed inputs go through `RunContext::reveal_input`), and the caller
/// would receive the marker instead of the value.
pub fn refuse_secret_in_message(port: &str, message: &Value) -> WeftResult<()> {
    match weft::secret::markers_in(message).first() {
        None => Ok(()),
        Some((at, _)) => Err(WeftError::Input(format!(
            "a message on `{port}` holds a secret {}: a bus carries no type, so nothing says to reveal \
             it, and the caller would receive its marker instead of its value. Send it through an input \
             whose type declares it (`first`, or a Reply's `body`), and it goes out revealed",
            place_name(at)
        ))),
    }
}

/// Where a JSON pointer sits, for a message: `at /token`, or `as the whole
/// value` for the empty pointer.
fn place_name(at: &str) -> String {
    if at.is_empty() { "as the whole value".to_string() } else { format!("at {at}") }
}

/// A port value as the wire wants it for `data_type`. A JSON value's
/// files go out as links (see [`link_files`]).
pub async fn chunk_for(
    ctx: &RunContext,
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
                .storage(StorageScope::Run)
                .get_bytes(&FileHandle::key(file.key))
                .await?;
            Ok(OutboundChunk::Bytes(bytes.to_vec()))
        }
    }
}

/// An inbound message (a route's body, one socket message) as the value
/// the program declared it to be, `declared`. JSON is held to the type
/// as [`take_inbound`] holds it (inline pictures stored, open records
/// trimmed); text is held to it as a String; bytes are stored (run
/// scope, swept with the run) under `name`, and the stored-file value
/// is held to it.
///
/// A byte message is stored as the type the caller's `Content-Type`
/// states ([`weft::storage::received_file`]); its first bytes fill one in
/// when none was stated (a socket frame never states one) and hold a
/// place declared `Image` to bytes that are an image, whatever was stated.
///
/// `Ok(Err(why))` is the caller's mistake (a message that does not fit
/// what the program declared), `Err` the node's own failure.
pub async fn take_message(
    ctx: &RunContext,
    message: InboundMessage,
    content_type: Option<&str>,
    name: &str,
    declared: &WeftType,
) -> WeftResult<Result<Value, String>> {
    match message {
        InboundMessage::Json(v) => take_inbound(ctx, v, declared, name).await,
        InboundMessage::Text(s) => Ok(declared
            .conform_value(Value::String(s))
            .map_err(|why| format!("{name} does not fit {declared}: {why}"))),
        InboundMessage::Bytes(b) => {
            // Bytes only ever become a stored file, so a place that holds
            // no file kind is refused before anything is written.
            if declared.admitted_file_kinds().is_empty() {
                return Ok(Err(format!("{name} is bytes, and it takes {declared}")));
            }
            let mime = match weft::storage::received_file(&b, content_type, declared) {
                Ok((mime, _)) => mime,
                Err(why) => return Ok(Err(format!("{name}: the bytes sent are not what it takes: {why}"))),
            };
            let file = ctx.storage(StorageScope::Run).put(b, &mime, name, None).await?;
            Ok(declared.conform_value(file).map_err(|why| format!("{name} does not fit {declared}: {why}")))
        }
    }
}

/// A value the caller sent, held to the type the program declared for
/// it (a route's `body`, one item of a socket's `inbound`): every
/// picture it carries in a file-typed place as a `data:` URL or bare
/// base64 is stored on the way in (run scope, swept with the run) and
/// the stored-file value takes its place, then the whole is held to
/// the type, which drops what an open record does not declare. The
/// bytes are held to the kinds their place declares (base64 of a video
/// in an `Image` or `Image | Null` field is refused), and stored under
/// the type a `data:` link states, or the one the bytes announce when
/// none was stated. A file-typed place may instead hold an upload slot
/// (`weft-upload:<slot>`) the page sent a file through
/// (`uploadFile` in `@weft/connect`): this run receives that file in the
/// scope the slot named, after the store checks it fits the place. Every
/// upload of one value is received together or none is: a refusal of any
/// of them (a file that does not fit its place, a slot that ended)
/// receives none, so every slot stays open for a corrected call.
///
/// A caller never hands over a stored-file value itself: a marker
/// names a key or a URL, and taking one on the caller's word would let
/// them point the program at a file of this project or make the worker
/// fetch an address of their choosing. Nor a secret: only a node makes
/// one, and a marker taken on the caller's word would name a secret of
/// another run. Any marker anywhere in the value is refused.
///
/// `Ok(Err(why))` is the CALLER's mistake, said in words the caller can
/// act on; `Err` is the node's own failure (storage refusing a write).
pub async fn take_inbound(
    ctx: &RunContext,
    value: Value,
    declared: &WeftType,
    name: &str,
) -> WeftResult<Result<Value, String>> {
    if weft::storage::media::holds_file_marker(&value) {
        return Ok(Err(format!(
            "{name} carries a stored-file object; send a file's bytes as a data: URL or base64 instead"
        )));
    }
    if weft::secret::Secret::claimed_in(&value) {
        return Ok(Err(format!(
            "{name} carries a `{}` object, which only weft makes; send the value itself",
            weft::secret::SECRET_MARKER_KEY
        )));
    }
    // Four steps, so a refusal leaves nothing behind: every inline
    // picture is decoded and checked first, then the whole value is held
    // to the type with a stand-in file in every file place (nothing
    // written yet, so a wrong field or a missing key refuses the call
    // with every slot still open), then every upload is received in ONE
    // claim (the broker receives them all or none, so a refused one
    // leaves the others open too), and only then are the decoded pictures
    // stored. A bad picture never follows an upload this run already
    // received.
    let mut uploads = Vec::new();
    let mut inline = Vec::new();
    let mut stand_ins = std::collections::HashMap::new();
    for weft::storage::media::TypedSlot { at, value: slot, ty: slot_ty } in
        weft::storage::media::typed_media_slots(&value, declared)
    {
        // The place inside the value, so a body with two pictures says
        // which one was wrong.
        let place = if at.is_empty() { name.to_string() } else { format!("{name}.{at}") };
        let text = match &slot {
            Value::String(text) => text,
            // Anything else in a file place (a null, a number) is held
            // to the type with the rest of the value just below.
            _ => continue,
        };
        if weft::storage::slot::is_slot(text) {
            // The broker answers the kind the bytes are, among the ones
            // the place admits; any of them stands in for it here.
            let kind = *slot_ty.admitted_file_kinds().first().expect("a file place admits a file kind");
            stand_ins.insert(slot.to_string(), stand_in(kind));
            uploads.push((place, text.clone(), slot_ty));
            continue;
        }
        let (mime, bytes) = match decode_inline(text) {
            Ok(decoded) => decoded,
            Err(why) => {
                return Ok(Err(format!(
                    "{place}: a place declared {slot_ty} takes a picture as a data: URL or base64; {why}"
                )))
            }
        };
        let mime = match weft::storage::received_file(&bytes, mime.as_deref(), &slot_ty) {
            Ok((mime, _)) => mime,
            Err(why) => {
                return Ok(Err(format!(
                    "{place}: the bytes sent for a place declared {slot_ty} are not what it takes: {why}"
                )))
            }
        };
        stand_ins.insert(slot.to_string(), stand_in(FileKind::from_mime(&mime)));
        inline.push((text.clone(), mime, bytes));
    }
    if let Err(why) = declared.validate_value(&weft::storage::media::substitute_media(&value, declared, &stand_ins)) {
        return Ok(Err(format!("{name} does not fit {declared}: {why}")));
    }
    let claims: Vec<(&str, &WeftType)> = uploads.iter().map(|(_, slot, slot_ty)| (slot.as_str(), slot_ty)).collect();
    let received = match ctx.storage(StorageScope::Run).claim_uploads(&claims).await {
        Ok(received) => received,
        // A slot that is no good (forged, ended, another project's,
        // received by another run, nothing uploaded, a file that does not
        // fit its place) is the caller's to fix, said in the store's own
        // words (they name the slot, never a key), beside every place
        // that named an upload, since none of them was received.
        Err(WeftError::Input(refused)) => {
            let places: Vec<&str> = uploads.iter().map(|(place, _, _)| place.as_str()).collect();
            return Ok(Err(format!("{}: the uploads named cannot be received, so none was: {refused}", places.join(", "))))
        }
        Err(failed) => return Err(failed),
    };
    let mut stored: std::collections::HashMap<String, Value> =
        uploads.iter().map(|(_, slot, _)| Value::String(slot.clone()).to_string()).zip(received).collect();
    for (text, mime, bytes) in inline {
        let file = ctx.storage(StorageScope::Run).put(bytes, &mime, name, None).await?;
        stored.insert(Value::String(text).to_string(), file);
    }
    let value = weft::storage::media::substitute_media(&value, declared, &stored);
    Ok(declared.conform_value(value).map_err(|why| format!("{name} does not fit {declared}: {why}")))
}

/// What stands in a file place while [`take_inbound`] holds the value to
/// its type before anything is stored: a stored-file value of `kind`
/// naming no file, which the type judges like the one that will take its
/// place.
fn stand_in(kind: FileKind) -> Value {
    WeftType::file_marker(kind, serde_json::json!({ "key": "", "mimeType": "" }))
}

/// The bytes an inline picture carries, and the media type when it said
/// one: a `data:` URL names it, bare base64 does not.
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

/// Every file inside a JSON answer, replaced by what a caller can use:
/// `{ "url", "mimeType", "filename", "sizeBytes" }`, the url being the
/// file's external link (a browser follows it straight into an `<img>`
/// or a `<video>`, which can seek). A stored file answered again soon
/// gets the same link, so a page that refreshes keeps playing; the link
/// lives as long as the value says (`linkSecs`), else fifteen minutes. A
/// url file goes out in the same shape, its url its own. Walks lists and
/// objects, so a file nested in a row is linked too. A file the install
/// cannot link fails the node: an answer with no way to the bytes is not
/// an answer.
pub async fn link_files(ctx: &RunContext, value: Value) -> WeftResult<Value> {
    match value {
        Value::Object(map) => {
            let value = Value::Object(map);
            if let Some(file) = file_value(&value) {
                let file = file?;
                let link = ctx.storage(StorageScope::Run).external_link(&file).await?;
                let (mime_type, filename, size_bytes) = match file {
                    FileHandle::Key { .. } => {
                        let stored = StoredFile::from_value(&value)?;
                        (stored.mime_type, stored.filename, stored.size_bytes)
                    }
                    FileHandle::Url { mime_type, filename, size_bytes, .. } => (mime_type, filename, size_bytes),
                };
                return Ok(serde_json::json!({
                    "url": link.url,
                    "mimeType": mime_type,
                    "filename": filename,
                    "sizeBytes": size_bytes,
                }));
            }
            let Value::Object(map) = value else { unreachable!("an object was just matched") };
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

/// The file a value is, when it is a file value (a stored file or a url
/// file): its handle, or why its marker names no file weft can reach.
/// `None` for a value that is no file value at all.
pub fn file_value(value: &Value) -> Option<WeftResult<FileHandle>> {
    weft::weft_type::FileKind::from_marker_obj(value.as_object()?)?;
    Some(FileHandle::from_value(value))
}

/// What an answer node (`Reply`, `Stream`) sends: its own `answerAs`
/// when set, else the shape the trigger's `dataType` declares. Apart
/// because what comes in and what goes out differ often: a GET has no
/// body at all and may still answer a picture as bytes. A message of one
/// of the wire's shapes, or a redirect to a file's link
/// (`answerAs: "redirect"`, which only a `Reply` sends).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Answer {
    Data(DataType),
    Redirect,
}

/// The answer the node's `answerAs` and the trigger's `dataType` ask for.
pub fn answer(ctx: &RunContext) -> WeftResult<Answer> {
    match ctx.inputs.opt::<String>("answerAs")?.as_deref() {
        Some("redirect") => Ok(Answer::Redirect),
        Some(raw) if !raw.is_empty() => DataType::parse_field(raw)
            .map(Answer::Data)
            .map_err(|_| WeftError::Input(format!("answerAs must be json, text, bytes or redirect, got '{raw}'"))),
        _ => Ok(Answer::Data(ctx.caller_data_type().unwrap_or_default())),
    }
}

/// The status a redirect answers with: `status` when it is one a redirect
/// can be, 302 when the program left the default 200, a refusal naming
/// the ones that work otherwise.
pub fn redirect_status(status: u16) -> WeftResult<u16> {
    match status {
        200 => Ok(302),
        301 | 302 | 303 | 307 | 308 => Ok(status),
        other => weft::node_bail!("a redirect answers 301, 302, 303, 307 or 308, not {other}; leave status at 200 for 302"),
    }
}

/// A stored file answered as bytes: the file's own type, and its name as
/// an inline `content-disposition` (a browser shows the picture, and
/// saving it keeps the name). Whatever the program set itself wins.
pub fn with_file_head(mut head: ResponseHead, file: &StoredFile) -> ResponseHead {
    if !head.has_content_type() {
        head = head.with_header("content-type", file.mime_type.clone());
    }
    if head.header("content-disposition").is_none() && !file.filename.is_empty() {
        head = head.with_header(
            "content-disposition",
            format!("inline; {}", weft::storage::disposition_filename(&file.filename)),
        );
    }
    head
}

/// The `headers` input of an answer node, every secret in it revealed
/// (its type, `Dict[String, String | Secret[String]]`, says where): a
/// header the caller receives carries the value, while the run's record
/// keeps the marker. `None` when nothing was set.
pub async fn headers(ctx: &RunContext) -> WeftResult<Option<Value>> {
    let Some(headers) = ctx.inputs.opt::<Value>("headers")? else { return Ok(None) };
    ctx.reveal_input("headers", headers).await.map(Some)
}

/// The response head a node's `status` and `headers` inputs describe.
/// `headers` is a JSON object of string values, its secrets revealed
/// ([`headers`]); anything else is refused by name.
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
    if status != 200 || has_headers(headers) {
        weft::node_bail!(
            "{node} behind a Socket sends messages; a status ({status}) or headers have no \
             meaning on a socket. Drop them, or put the {node} behind a Route"
        );
    }
    Ok(())
}

/// Whether a program set headers: present and not an empty object.
pub fn has_headers(headers: Option<&Value>) -> bool {
    headers.is_some_and(|h| !h.as_object().is_some_and(|m| m.is_empty()))
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
