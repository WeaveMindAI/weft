//! The translation between weft values and what a live caller sends and
//! receives, per the trigger's declared data type (`json`, `text`,
//! `bytes`), for any node that talks to a caller (`ctx.http_caller()`,
//! `ctx.ws_caller()`, `ctx.live_caller()`):
//!
//! ```text
//! caller sends  {"img": "data:image/png;base64,..."}
//!                  | InboundMessage::into_value: the picture stored as a weft
//!                  | file, an upload slot received, a forged file or secret
//!                  | marker refused, the whole held to its declared type
//!                  v
//! weft value    {"img": <file>}
//!
//! weft value    {"report": <file>, "token": <secret>}
//!                  | OutboundChunk::of_input: every file the type places
//!                  | becomes a link, every secret is revealed, one chunk per
//!                  | dataType; Format: SSE / ndjson framing and keepalive
//!                  v
//! caller gets   {"report": {"url": "https://...", ...}, "token": "abc"}
//! ```
//!
//! Bytes never travel a wire: a picture a caller sends inside a JSON body
//! is stored on the way in, and a file going out inside a JSON answer
//! goes as a link the caller fetches. Which places are files is the
//! value's DECLARED type's to say, both ways, never a marker key found in
//! raw JSON: a `JsonDict` that happens to carry one is not a file.

use std::collections::BTreeMap;

use serde_json::Value;

use super::{HttpCaller, InboundMessage, OutboundChunk, ResponseHead, WsCaller};
use crate::bus::{BusEntryKind, BusHandle, StreamEnd, WirePayload};
use crate::context::RunContext;
use crate::error::{node_error, WeftError, WeftResult};
use crate::signal::DataType;
use crate::storage::{FileHandle, StorageScope, StoredFile};
use crate::weft_type::{FileKind, WeftType};

/// What a value a caller sent became: the value, held to its declared
/// type, or the caller's own mistake in words the caller can act on
/// (answered `400` on a route, sent back on a socket, by
/// [`HttpCaller::refuse`] / [`WsCaller::refuse`]). A node's own failure
/// (storage refusing a write) is the outer `Err`.
pub type Taken<T> = Result<T, String>;

impl InboundMessage {
    /// This message as the value the program declared it to be,
    /// `declared`, under `name` (what a refusal and a stored file are
    /// called). JSON is held to the type (inline pictures stored, upload
    /// slots received, open records trimmed: see
    /// [`take_json`]); text is held to it as a String; bytes are stored
    /// (run scope, swept with the run) as the type `content_type` states,
    /// their first bytes filling one in when none was stated (a socket
    /// frame never states one) and holding a place declared `Image` to
    /// bytes that are an image, whatever was stated.
    pub async fn into_value(self, ctx: &RunContext, declared: &WeftType, name: &str, content_type: Option<&str>) -> WeftResult<Taken<Value>> {
        match self {
            InboundMessage::Json(v) => take_json(ctx, v, declared, name).await,
            InboundMessage::Text(s) => {
                Ok(declared.conform_value(Value::String(s)).map_err(|why| format!("{name} does not fit {declared}: {why}")))
            }
            InboundMessage::Bytes(b) => {
                // Bytes only ever become a stored file, so a place that
                // holds no file kind is refused before anything is written.
                if declared.admitted_file_kinds().is_empty() {
                    return Ok(Err(format!("{name} is bytes, and it takes {declared}")));
                }
                let mime = match crate::storage::received_file(&b, content_type, declared) {
                    Ok((mime, _)) => mime,
                    Err(why) => return Ok(Err(format!("{name}: the bytes sent are not what it takes: {why}"))),
                };
                let file = ctx.storage(StorageScope::Run).put(b, &mime, name, None).await?;
                Ok(declared.conform_value(file).map_err(|why| format!("{name} does not fit {declared}: {why}")))
            }
        }
    }

    /// The shape's word, for a refusal.
    fn shape(&self) -> &'static str {
        match self {
            InboundMessage::Json(_) => "json",
            InboundMessage::Text(_) => "text",
            InboundMessage::Bytes(_) => "bytes",
        }
    }
}

impl HttpCaller {
    /// The request's body as the value the program declared it to be
    /// ([`InboundMessage::into_value`]), named `name`, read in the
    /// trigger's data type. `otherwise` stands in when the connection
    /// carries no body (a fired run, whose author typed it into the fire
    /// payload). `Ok(Ok(None))` for no body at all, which is every GET:
    /// an ordinary way to call a route. A json body that is `null` is a
    /// value where the type takes `Null` (`Json`, `Signup | Null`) and no
    /// body anywhere else. A body in another shape than the trigger's
    /// data type (a fired text route sent a number) is the caller's
    /// mistake.
    pub async fn body_as(
        &self,
        ctx: &RunContext,
        declared: &WeftType,
        name: &str,
        otherwise: Option<InboundMessage>,
    ) -> WeftResult<Taken<Option<Value>>> {
        let parts = self.request_parts()?;
        let data_type = self.conn.config().data_type;
        let message = match (&parts.body, otherwise) {
            (Some(body), _) => body.clone(),
            (None, Some(fired)) => fired,
            (None, None) => return Ok(Ok(None)),
        };
        let message = match (data_type, message) {
            (DataType::Json, InboundMessage::Json(Value::Null)) if !declared.contains_null() => return Ok(Ok(None)),
            (DataType::Json, message @ InboundMessage::Json(_))
            | (DataType::Text, message @ InboundMessage::Text(_))
            | (DataType::Bytes, message @ InboundMessage::Bytes(_)) => message,
            (data_type, other) => {
                return Ok(Err(format!(
                    "this route takes a {} body, and the request sent {}",
                    data_type.as_wire_str(),
                    other.shape()
                )))
            }
        };
        let content_type = parts.request.header("content-type");
        Ok(message.into_value(ctx, declared, name, content_type).await?.map(Some))
    }

    /// Answer `400` with `why`, the request not being what the program
    /// declared ([`Taken`]): `{"error": why}` on a json route, the text
    /// on the others. Terminal.
    pub async fn refuse(&self, why: String) -> WeftResult<()> {
        self.respond_with(ResponseHead::new(400), OutboundChunk::refusal(self.conn.config().data_type, why)).await
    }

    /// Stream `bus` to the caller, one chunk per message, from the
    /// earliest message still buffered (a producer that started before
    /// this node, or already finished, loses nothing), until the bus
    /// closes; then end the response. `head` goes out with the first
    /// chunk, with the content type `format` implies unless it names one,
    /// and the format's filler for quiet stretches. `first` is one value
    /// sent ahead of the bus and framed like its messages, the value of
    /// the node's input `first.0` (its type says where its files and
    /// secrets are). A chunk that cannot be made goes through
    /// [`Self::drop_or_fail`]. A bus that ended failed is never ended as a
    /// whole answer: the response fails ([`Self::fail`]). The run is held
    /// while it streams: answering a caller cannot be picked up from the
    /// middle. Answers whether the whole bus reached the caller (`false`
    /// when it ended failed), for a node that says so on an output.
    pub async fn stream_bus(
        &self,
        ctx: &RunContext,
        bus: &BusHandle,
        head: ResponseHead,
        format: Format,
        data_type: DataType,
        first: Option<(&str, Value)>,
    ) -> WeftResult<bool> {
        let mut head = Some(head);
        if let Some(ct) = format.content_type() {
            head = head.map(|h| if h.has_content_type() { h } else { h.with_header("content-type", ct) });
        }
        // The worker writes the filler while the bus is quiet, and a
        // caller who left is found by that write instead of by the next
        // message.
        if let Some(filler) = format.keepalive() {
            head = head.map(|h| h.with_keepalive(filler));
        }
        if let Some((port, value)) = first {
            if let Some(chunk) = self.drop_or_fail(framed_input(ctx, port, value, format, data_type).await)? {
                self.write_held(&mut head, chunk).await?;
            }
        }
        let mut messages = Messages::of(ctx, bus)?;
        while let Some(payload) = messages.next().await? {
            if let Some(chunk) = self.drop_or_fail(framed_message(ctx, format, data_type, payload).await)? {
                self.write_held(&mut head, chunk).await?;
            }
        }
        if let Some(why) = messages.broke_off() {
            self.fail(&why).await?;
            return Ok(false);
        }
        // A stream that never carried a message still answers, with the
        // head alone.
        match head.take() {
            Some(h) => self.close_with(h).await?,
            None => self.close().await?,
        }
        Ok(true)
    }

    /// Write one chunk, the held head riding the first one.
    async fn write_held(&self, head: &mut Option<ResponseHead>, chunk: OutboundChunk) -> WeftResult<()> {
        match head.take() {
            Some(h) => self.write_with(h, chunk).await,
            None => self.write(chunk).await,
        }
    }
}

impl WsCaller {
    /// The next message the caller sends, as the value the program
    /// declared it to be ([`InboundMessage::into_value`]), named `name`
    /// (a byte frame states no type, so what its bytes say is the only
    /// word on their kind). `None` once the caller is gone, which ends
    /// the session.
    pub async fn recv_as(&self, ctx: &RunContext, declared: &WeftType, name: &str) -> WeftResult<Option<Taken<Value>>> {
        let Some(message) = self.recv_next().await? else { return Ok(None) };
        Ok(Some(message.into_value(ctx, declared, name, None).await?))
    }

    /// Tell the caller what was wrong with a message ([`Taken`]):
    /// `{"error": why}` on a json socket, the text on the others. The
    /// session goes on.
    pub async fn refuse(&self, why: String) -> WeftResult<()> {
        self.send(OutboundChunk::refusal(self.conn.config().data_type, why)).await
    }

    /// Stream `bus` to the caller, one socket message per bus message (a
    /// frame needs no HTTP framing), from the earliest message still
    /// buffered, until the bus closes; the socket stays open. `first` as
    /// for [`HttpCaller::stream_bus`]. A bus that ended failed closes the
    /// session as failed ([`Self::fail`]). Answers whether the whole bus
    /// reached the caller.
    pub async fn stream_bus(&self, ctx: &RunContext, bus: &BusHandle, data_type: DataType, first: Option<(&str, Value)>) -> WeftResult<bool> {
        if let Some((port, value)) = first {
            if let Some(chunk) = self.drop_or_fail(framed_input(ctx, port, value, Format::Raw, data_type).await)? {
                self.send(chunk).await?;
            }
        }
        let mut messages = Messages::of(ctx, bus)?;
        while let Some(payload) = messages.next().await? {
            if let Some(chunk) = self.drop_or_fail(framed_message(ctx, Format::Raw, data_type, payload).await)? {
                self.send(chunk).await?;
            }
        }
        match messages.broke_off() {
            Some(why) => {
                self.fail(&why).await?;
                Ok(false)
            }
            None => Ok(true),
        }
    }
}

/// The messages of a bus a caller is streamed, from the earliest still
/// buffered until it closes, read while holding the run. An observer, not
/// a participant: it never registers (a closed bus refuses a
/// registration, and closing it is the producer's act).
struct Messages<'b> {
    bus: &'b BusHandle,
    cursor: crate::bus::BusCursor,
    holding: crate::context::Holding,
}

impl<'b> Messages<'b> {
    fn of(ctx: &RunContext, bus: &'b BusHandle) -> WeftResult<Self> {
        Ok(Self { bus, cursor: bus.cursor_from_start()?, holding: ctx.hold() })
    }

    /// The next message's payload, `None` once the bus closed, finished
    /// or failed ([`Self::broke_off`] says which).
    async fn next(&mut self) -> WeftResult<Option<WirePayload>> {
        loop {
            match self.cursor.next(&mut self.holding).await {
                Ok(Some(entry)) => {
                    if let BusEntryKind::Message { payload: Some(payload), .. } = entry.kind {
                        return Ok(Some(payload));
                    }
                }
                Ok(None) => return Ok(None),
                // A read that failed because the bus ended failed is the
                // break-off, which the exchange is failed with; any other
                // is the node's.
                Err(e) => match self.bus.end() {
                    Some(StreamEnd::Failed { .. }) => return Ok(None),
                    _ => return Err(e),
                },
            }
        }
    }

    /// Why the stream did not complete, when the bus ended failed (its
    /// producer broke off): what the exchange is failed with.
    fn broke_off(&self) -> Option<String> {
        match self.bus.end() {
            Some(StreamEnd::Failed { error }) => Some(format!("the stream broke off: {error}")),
            Some(StreamEnd::Finished) | None => None,
        }
    }
}

impl OutboundChunk {
    /// The answer to a caller whose message was not what the program
    /// declared ([`Taken`]), in the trigger's data type: `{"error": why}`
    /// for json, the text otherwise.
    pub fn refusal(data_type: DataType, why: String) -> Self {
        match data_type {
            DataType::Json => OutboundChunk::Json(serde_json::json!({ "error": why })),
            DataType::Text | DataType::Bytes => OutboundChunk::Text(why),
        }
    }

    /// What a caller receives for `value`, the value of this node's input
    /// `port`, in `data_type`: `json` carries any value, every file its
    /// type places going out as a link ([`link_files`]); `text` wants a
    /// string; `bytes` wants a stored file, whose content is read back.
    /// Every secret the input's type places is revealed for the caller
    /// (a secret where it declares none is refused,
    /// `RunContext::reveal_input`), and the run's record keeps the value
    /// with its markers ([`OutboundChunk::Revealed`]).
    pub async fn of_input(ctx: &RunContext, port: &str, value: Value, data_type: DataType) -> WeftResult<Self> {
        framed_input(ctx, port, value, Format::Raw, data_type).await
    }
}

/// [`OutboundChunk::of_input`], framed per `format`.
async fn framed_input(ctx: &RunContext, port: &str, value: Value, format: Format, data_type: DataType) -> WeftResult<OutboundChunk> {
    let ty = ctx
        .declared_inputs()
        .get(port)
        .cloned()
        .ok_or_else(|| WeftError::Config(format!("this node declares no `{port}` input")))?;
    framed_revealed(ctx, value, &ty, Some(port), format, data_type).await
}

/// One bus message as the chunk to send. A bus declares no type, so the
/// message's own markers give it one ([`WirePayload::into_typed_json`]),
/// and it goes out the way an input of that type does: every file a
/// link, every secret revealed. A byte payload rides as bytes whatever
/// the format: it has no text form.
async fn framed_message(ctx: &RunContext, format: Format, data_type: DataType, payload: WirePayload) -> WeftResult<OutboundChunk> {
    let (value, ty) = match payload {
        WirePayload::Bytes(b) => return Ok(OutboundChunk::Bytes(b.to_vec())),
        json => json.into_typed_json().expect("a json payload"),
    };
    framed_revealed(ctx, value, &ty, None, format, data_type).await
}

/// `value`, of type `ty`, as the chunk to send with every secret `ty`
/// places in it revealed (by the input `port`'s declared type when it is
/// one, `RunContext::reveal_input`, else `RunContext::reveal_in`): what
/// holds a secret goes out as [`OutboundChunk::Revealed`], so the run's
/// record keeps the value with its markers.
async fn framed_revealed(ctx: &RunContext, value: Value, ty: &WeftType, port: Option<&str>, format: Format, data_type: DataType) -> WeftResult<OutboundChunk> {
    let reveal = |value: Value| async move {
        match port {
            Some(port) => ctx.reveal_input(port, value).await,
            None => ctx.reveal_in(value, ty).await,
        }
    };
    if !ty.holds_secret() {
        return framed_value(ctx, reveal(value).await?, ty, format, data_type).await;
    }
    let sent = reveal(value.clone()).await?;
    Ok(OutboundChunk::Revealed { sent: Box::new(framed_value(ctx, sent, ty, format, data_type).await?), kept: value })
}

/// `value`, of type `ty`, as the chunk `format` and `data_type` make of
/// it: framed text for `sse` / `ndjson` (its files linked), the value
/// itself per the data type for `raw`.
async fn framed_value(ctx: &RunContext, value: Value, ty: &WeftType, format: Format, data_type: DataType) -> WeftResult<OutboundChunk> {
    match format {
        Format::Sse => Ok(OutboundChunk::Text(format.frame(&link_files(ctx, &value, ty).await?))),
        Format::Ndjson => Ok(OutboundChunk::Text(format.frame(&link_files(ctx, &value, ty).await?))),
        Format::Raw => match data_type {
            DataType::Json => Ok(OutboundChunk::Json(link_files(ctx, &value, ty).await?)),
            DataType::Text => match value {
                Value::String(s) => Ok(OutboundChunk::Text(s)),
                other => Err(node_error(format!(
                    "this caller speaks text (the trigger's dataType), so the body must be a String; got {}",
                    kind_of(&other)
                ))),
            },
            DataType::Bytes => {
                // A stored-file VALUE (the marker object), never a bare
                // key string: a key is an implementation detail no port
                // carries.
                let file = StoredFile::from_value(&value).map_err(|e| {
                    node_error(format!("this caller speaks bytes (the trigger's dataType), so the body must be a stored file; {e}"))
                })?;
                let (_, bytes) = ctx.storage(StorageScope::Run).get_bytes(&FileHandle::key(file.key)).await?;
                Ok(OutboundChunk::Bytes(bytes.to_vec()))
            }
        },
    }
}

/// `value` with every file its type `ty` places in it replaced by what a
/// caller can use: `{ "url", "mimeType", "filename", "sizeBytes" }`, the
/// url being the file's external link (a browser follows it straight
/// into an `<img>` or a `<video>`, which can seek). A stored file
/// answered again soon gets the same link, so a page that refreshes
/// keeps playing; the link lives as long as the value says
/// (`linkSecs`), else fifteen minutes. A url file goes out in the same
/// shape, its url its own. A file the install cannot link fails the
/// node: an answer with no way to the bytes is not an answer.
pub async fn link_files(ctx: &RunContext, value: &Value, ty: &WeftType) -> WeftResult<Value> {
    let storage = ctx.storage(StorageScope::Run);
    crate::storage::media::map_stored_files(value, ty, |slot| {
        let storage = &storage;
        async move {
            let link = storage.external_link(&slot.handle).await?;
            let (mime_type, filename, size_bytes) = match slot.handle {
                FileHandle::Key { .. } => {
                    let stored = StoredFile::from_value(&slot.value)?;
                    (stored.mime_type, stored.filename, stored.size_bytes)
                }
                FileHandle::Url { mime_type, filename, size_bytes, .. } => (mime_type, filename, size_bytes),
            };
            Ok(serde_json::json!({ "url": link.url, "mimeType": mime_type, "filename": filename, "sizeBytes": size_bytes }))
        }
    })
    .await
}

impl ResponseHead {
    /// The head with every header of `headers` (a node's `headers` input,
    /// its secrets revealed: `RunContext::reveal_input`).
    pub fn with_headers(mut self, headers: BTreeMap<String, String>) -> Self {
        self.headers.extend(headers);
        self
    }

    /// The head of a stored file answered as bytes: the file's own type,
    /// and its name as an inline `content-disposition` (a browser shows
    /// the picture, and saving it keeps the name). Whatever the head
    /// already set wins.
    pub fn with_file(mut self, file: &StoredFile) -> Self {
        if !self.has_content_type() {
            self = self.with_header("content-type", file.mime_type.clone());
        }
        if self.header("content-disposition").is_none() && !file.filename.is_empty() {
            self = self.with_header("content-disposition", format!("inline; {}", crate::storage::disposition_filename(&file.filename)));
        }
        self
    }

    /// A redirect to `location` under `status`: one a redirect can be
    /// (301, 302, 303, 307, 308), or 302 for the plain 200 a program left
    /// as it was. Any other is refused, naming the ones that work.
    pub fn redirect(status: u16, location: impl Into<String>) -> WeftResult<Self> {
        let status = match status {
            200 => 302,
            301 | 302 | 303 | 307 | 308 => status,
            other => crate::node_bail!("a redirect answers 301, 302, 303, 307 or 308, not {other}; leave status at 200 for 302"),
        };
        Ok(Self::new(status).with_header("location", location))
    }
}

/// How a stream's messages are framed on an HTTP response body.
///
///   - `raw`: the payload as-is (a text delta becomes text, a JSON
///     object its JSON), one write per message, no separator.
///   - `ndjson`: one JSON value per line (`application/x-ndjson`), what a
///     script reading line by line wants.
///   - `sse`: server-sent events (`text/event-stream`), what a browser's
///     `EventSource` and LLM-style clients read: `data: <line>` per line
///     of the payload, a blank line after each event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Format {
    Raw,
    Ndjson,
    Sse,
}

impl Format {
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
    /// byte is payload. The write is how a caller who hung up without a
    /// word is found: a quiet feed (a watched table that never changes)
    /// would otherwise hold its run open for ever.
    pub fn keepalive(self) -> Option<&'static str> {
        match self {
            Self::Raw => None,
            Self::Ndjson => Some("\n"),
            Self::Sse => Some(": keepalive\n\n"),
        }
    }

    /// One payload framed: for `sse` every line of its text (a string as
    /// itself, so an LLM delta stays a delta; anything else as JSON) its
    /// own `data:` line (the grammar forbids a newline inside one) and a
    /// blank line after; for `ndjson` the payload as JSON (a string
    /// quoted: a line is one JSON value) and a newline; for `raw` the
    /// text alone.
    pub fn frame(self, payload: &Value) -> String {
        let text = match payload {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        };
        match self {
            Self::Raw => text,
            Self::Ndjson => format!("{payload}\n"),
            Self::Sse => {
                let mut out = String::with_capacity(text.len() + 16);
                for line in text.split('\n') {
                    out.push_str("data: ");
                    out.push_str(line);
                    out.push('\n');
                }
                out.push('\n');
                out
            }
        }
    }
}

/// A one-word description of a value's shape, for error messages.
fn kind_of(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "a boolean",
        Value::Number(_) => "a number",
        Value::String(_) => "a string",
        Value::Array(_) => "a list",
        Value::Object(_) => "an object",
    }
}

/// A value the caller sent, held to the type the program declared for
/// it (a route's body, one socket message): every picture it carries in
/// a file-typed place as a `data:` URL or bare base64 is stored on the
/// way in (run scope, swept with the run) and the stored-file value takes
/// its place, then the whole is held to the type, which drops what an
/// open record does not declare. The bytes are held to the kinds their
/// place declares (base64 of a video in an `Image | Null` field is
/// refused), and stored under the type a `data:` link states, or the one
/// the bytes announce when none was stated. A file-typed place may
/// instead hold an upload slot (`weft-upload:<slot>`) the page sent a
/// file through (`uploadFile` in `@weft/connect`): this run receives that
/// file in the scope the slot named, after the store checks it fits the
/// place. Every upload of one value is received together or none is: a
/// refusal of any (a file that does not fit its place, a slot that
/// ended) receives none, so every slot stays open for a corrected call.
///
/// A caller never hands over a stored-file value itself: a marker names
/// a key or a URL, and taking one on the caller's word would let them
/// point the program at a file of this project or make the worker fetch
/// an address of their choosing. Nor a secret: only a node makes one,
/// and a marker taken on the caller's word would name a secret of
/// another run. Any marker anywhere in the value is refused.
async fn take_json(ctx: &RunContext, value: Value, declared: &WeftType, name: &str) -> WeftResult<Taken<Value>> {
    if crate::storage::media::holds_file_marker(&value) {
        return Ok(Err(format!("{name} carries a stored-file object; send a file's bytes as a data: URL or base64 instead")));
    }
    if crate::secret::Secret::claimed_in(&value) {
        return Ok(Err(format!("{name} carries a `{}` object, which only weft makes; send the value itself", crate::secret::SECRET_MARKER_KEY)));
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
    for crate::storage::media::TypedSlot { at, value: slot, ty: slot_ty } in crate::storage::media::typed_media_slots(&value, declared) {
        // The place inside the value, so a body with two pictures says
        // which one was wrong.
        let place = if at.is_empty() { name.to_string() } else { format!("{name}.{at}") };
        let text = match &slot {
            Value::String(text) => text,
            // Anything else in a file place (a null, a number) is held to
            // the type with the rest of the value just below.
            _ => continue,
        };
        if crate::storage::slot::is_slot(text) {
            // The broker answers the kind the bytes are, among the ones
            // the place admits; any of them stands in for it here.
            let kind = *slot_ty.admitted_file_kinds().first().expect("a file place admits a file kind");
            stand_ins.insert(slot.to_string(), stand_in(kind));
            uploads.push((place, text.clone(), slot_ty));
            continue;
        }
        let (mime, bytes) = match decode_inline(text) {
            Ok(decoded) => decoded,
            Err(why) => return Ok(Err(format!("{place}: a place declared {slot_ty} takes a picture as a data: URL or base64; {why}"))),
        };
        let mime = match crate::storage::received_file(&bytes, mime.as_deref(), &slot_ty) {
            Ok((mime, _)) => mime,
            Err(why) => return Ok(Err(format!("{place}: the bytes sent for a place declared {slot_ty} are not what it takes: {why}"))),
        };
        stand_ins.insert(slot.to_string(), stand_in(FileKind::from_mime(&mime)));
        inline.push((text.clone(), mime, bytes));
    }
    if let Err(why) = declared.validate_value(&crate::storage::media::substitute_media(&value, declared, &stand_ins)) {
        return Ok(Err(format!("{name} does not fit {declared}: {why}")));
    }
    let claims: Vec<(&str, &WeftType)> = uploads.iter().map(|(_, slot, slot_ty)| (slot.as_str(), slot_ty)).collect();
    let received = match ctx.storage(StorageScope::Run).claim_uploads(&claims).await {
        Ok(received) => received,
        // A slot that is no good (forged, ended, another project's,
        // received by another run, nothing uploaded, a file that does not
        // fit its place) is the caller's to fix, said in the store's own
        // words (they name the slot, never a key), beside every place that
        // named an upload, since none of them was received.
        Err(WeftError::Input(refused)) => {
            let places: Vec<&str> = uploads.iter().map(|(place, _, _)| place.as_str()).collect();
            return Ok(Err(format!("{}: the uploads named cannot be received, so none was: {refused}", places.join(", "))));
        }
        Err(failed) => return Err(failed),
    };
    let mut stored: std::collections::HashMap<String, Value> =
        uploads.iter().map(|(_, slot, _)| Value::String(slot.clone()).to_string()).zip(received).collect();
    for (text, mime, bytes) in inline {
        let file = ctx.storage(StorageScope::Run).put(bytes, &mime, name, None).await?;
        stored.insert(Value::String(text).to_string(), file);
    }
    let value = crate::storage::media::substitute_media(&value, declared, &stored);
    Ok(declared.conform_value(value).map_err(|why| format!("{name} does not fit {declared}: {why}")))
}

/// What stands in a file place while [`take_json`] holds the value to its
/// type before anything is stored: a stored-file value of `kind` naming
/// no file, which the type judges like the one that will take its place.
fn stand_in(kind: FileKind) -> Value {
    WeftType::file_marker(kind, serde_json::json!({ "key": "", "mimeType": "" }))
}

/// The bytes an inline picture carries, and the media type when it said
/// one: a `data:` URL names it, bare base64 does not.
fn decode_inline(text: &str) -> Result<(Option<String>, Vec<u8>), String> {
    if let Some(rest) = text.strip_prefix("data:") {
        let (mime, payload) = rest.split_once(";base64,").ok_or_else(|| "the data: URL carries no base64 payload".to_string())?;
        let bytes = crate::storage::media::base64_decode(payload).map_err(|e| format!("the data: URL payload is not valid base64: {e}"))?;
        return Ok((Some(mime.to_string()), bytes));
    }
    let bytes = crate::storage::media::base64_decode(text.trim()).map_err(|e| format!("the string is not valid base64: {e}"))?;
    Ok((None, bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Each format frames a payload as its reader expects: a string
    /// stays a delta in SSE (one `data:` line per line of it), is quoted
    /// in ndjson, and is itself in raw.
    #[test]
    fn each_format_frames_a_payload_as_its_reader_reads_it() {
        assert_eq!(Format::Sse.frame(&Value::String("a\nb".into())), "data: a\ndata: b\n\n");
        assert_eq!(Format::Sse.frame(&serde_json::json!({ "k": 1 })), "data: {\"k\":1}\n\n");
        assert_eq!(Format::Ndjson.frame(&Value::String("a".into())), "\"a\"\n");
        assert_eq!(Format::Raw.frame(&Value::String("a".into())), "a");
        assert_eq!(serde_json::from_value::<Format>(Value::String("ndjson".into())).unwrap(), Format::Ndjson);
        assert!(serde_json::from_value::<Format>(Value::String("xml".into())).is_err());
    }

    #[test]
    fn a_redirect_takes_only_a_redirect_status() {
        assert_eq!(ResponseHead::redirect(200, "/x").unwrap().status, 302);
        assert_eq!(ResponseHead::redirect(308, "/x").unwrap().header("location"), Some("/x"));
        assert!(ResponseHead::redirect(404, "/x").is_err());
    }
}
