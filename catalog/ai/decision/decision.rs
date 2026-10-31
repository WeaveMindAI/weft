//! The shared body of the decision nodes (`DecisionChoice`,
//! `DecisionYesNo`, `DecisionScore`, `DecisionBox`).
//!
//! A decision model reads a STATE and answers one typed question about
//! it with probabilities, never text. Each node asks one kind of
//! question; everything else is the same for all four and lives here:
//! reading the wired `DecisionProvider` (an OpenRouter Decision Model
//! node's output), building the state from the ports the program added,
//! and the call itself.
//!
//! THE STATE is the node's added input ports, in the order the program
//! wrote them (`DecisionYesNo(ticket: String, customer: Customer,
//! photo: Image)`). Each port is one of the shapes of [`PORT_TYPES`],
//! held to exactly one of them when the program is built:
//! - a VALUE (made of JSON: text, a record, a dict, a list of those) is
//!   a field of one object, named after its port, sent as it is;
//! - a FILE, or a list of files, is read at the call, so
//!   whatever is too big for a wire reaches the model as a stored file. A
//!   text or JSON file is a field like a value, holding its content (a
//!   list of them, a list of contents). An image, audio clip or video is
//!   sent as its own item after the object, behind a label naming its
//!   port (`` `photo`: ``, `` `photos[1]`: ``): most decision models read
//!   media only as items of a top-level state array, never inside an
//!   object. Measured on four models (2026-10), a question naming `photo`
//!   reads the labelled image as well as one written in place.
//! A file inside a value (a record with an `Image` field) and a port that
//! is a value or a file (`String | Image`) fit none of them, so the
//! compiler refuses both.
//!
//! The call goes to OpenRouter's System One route, the Decisions API
//! under the `/api/v1` base (same request, same answer as
//! `/api/alpha/decisions`), which is what lets the OpenRouter meter
//! price it and weft's own key travel on it.

use base64::Engine;
use serde_json::{json, Map, Value};

use weft::storage::media::{classify_media_slot, data_url, holds_file_marker, MediaSlotContent};
use weft::storage::StorageScope;
use weft::weft_type::FileKind;
use weft::{Access, RunContext, WeftError, WeftResult, WeftType};

/// OpenRouter's API base.
pub const OPENROUTER_API: &str = "https://openrouter.ai/api/v1";

/// The Decisions API route under [`OPENROUTER_API`].
// SYNC: decision::ROUTE <-> crates/weft-providers/src/providers/openrouter.rs DECISIONS
pub const ROUTE: &str = "systemone";

/// The key of the provider object holding OpenRouter's routing block
/// (`order`, `allow_fallbacks`), built by the OpenRouter Decision Model
/// node and sent as the request's `provider` as it is.
pub const ROUTING: &str = "routing";

/// The key the one question rides under. Question keys are for the
/// caller and never reach the model, so any name does.
const ANSWER: &str = "answer";

/// The types an added port may be, each port exactly one of them (see the
/// module docs), in [`Shape`] order: a value made of JSON, one file, a
/// list of files.
// SYNC: decision::PORT_TYPES <-> catalog/ai/decision/yes_no/metadata.json, catalog/ai/decision/choice/metadata.json, catalog/ai/decision/score/metadata.json, catalog/ai/decision/box/metadata.json (custom_inputs_fit)
pub const PORT_TYPES: [&str; 3] = ["Json", "File | Null", "List[File] | Null"];

/// What an added port is, by the one of [`PORT_TYPES`] its type fits.
enum Shape {
    Value,
    File,
    Files,
}

impl Shape {
    /// The first of [`PORT_TYPES`] `ty` fits, so a type still open is a
    /// value. `None` for a type the compiler refuses on these nodes.
    fn of(ty: &WeftType) -> Option<Self> {
        let fits = |allowed: &str| WeftType::fits_within(ty, &WeftType::parse(allowed).expect("PORT_TYPES are weft types"));
        let [value, file, files] = PORT_TYPES;
        if fits(value) {
            Some(Self::Value)
        } else if fits(file) {
            Some(Self::File)
        } else {
            fits(files).then_some(Self::Files)
        }
    }
}

/// The audio formats the Decisions API takes on an `input_audio` part.
const AUDIO_FORMATS: [&str; 12] =
    ["wav", "mp3", "mpeg", "m4a", "mp4", "aac", "aiff", "aif", "flac", "ogg", "opus", "webm"];

/// The state built from the node's added ports, ready to send.
pub struct State {
    pub value: Value,
    /// How many images it carries (a Box question needs one).
    pub images: usize,
}

/// Build the state from the ports the program added to the node (the
/// compiler refuses a node with none, `custom_inputs_declared`, and a
/// port fitting none of [`PORT_TYPES`], `custom_inputs_fit`). A
/// port nothing arrived on (written `name?:`) is left out.
pub async fn state(ctx: &RunContext) -> WeftResult<State> {
    let declared = ctx.declared_inputs();
    let mut fields = Map::new();
    let mut media = Vec::new();
    let mut images = 0;
    for name in ctx.inputs.created() {
        let Some(value) = ctx.inputs.raw(name).filter(|value| !value.is_null()) else { continue };
        let ty = declared.get(name).ok_or_else(|| {
            WeftError::Type(format!("the added input `{name}` has no declared type"))
        })?;
        let content = match Shape::of(ty) {
            Some(Shape::Value) => {
                // A value typed as plain JSON that carries a stored file
                // (one wired from a record with an Image field into a
                // JsonDict) has nothing that reads the file, and its value
                // names a file of this project, which never leaves weft.
                if holds_file_marker(value) {
                    weft::node_bail!(
                        "`{name}` holds a stored file inside a value, which is never read from there: \
                         give the file a port of its own (`photo: Image`)"
                    );
                }
                Some(value.clone())
            }
            Some(Shape::File) => read_file(ctx, name, value, &mut media).await?,
            Some(Shape::Files) => {
                let Value::Array(items) = value else {
                    weft::node_bail!("`{name}` is a {ty}, and what arrived is not a list");
                };
                let mut contents = Vec::new();
                for (at, item) in items.iter().enumerate() {
                    contents.extend(read_file(ctx, &format!("{name}[{at}]"), item, &mut media).await?);
                }
                (!contents.is_empty()).then_some(Value::Array(contents))
            }
            None => weft::node_bail!(
                "`{name}` is a {ty}, and a decision node takes, on each added input, a value made of JSON, \
                 a file, or a list of files"
            ),
        };
        if let Some(content) = content {
            fields.insert(name.clone(), content);
        }
    }
    if fields.is_empty() && media.is_empty() {
        weft::node_bail!(
            "none of this node's added inputs holds anything to judge (nothing arrived on them, or only \
             empty lists): make at least one of them required (`ticket: String`, not `ticket?: String`), \
             so the node runs only when it has something to judge"
        );
    }
    let mut items = Vec::with_capacity(media.len() * 2);
    for part in media {
        images += usize::from(part.kind == FileKind::Image);
        items.push(Value::String(format!("`{}`:", part.place)));
        items.push(part.part);
    }
    Ok(State { value: layout(fields, items), images })
}

/// Ask the wired decision model `question` (a Decisions API question,
/// `type` included) about `state`, and hand back its answer, checked to
/// be of the type asked.
pub async fn ask(ctx: &RunContext, state: State, question: Value) -> WeftResult<Value> {
    let provider = ctx.inputs.nested("provider")?;
    let kind: String = provider.get("kind")?;
    if kind != "openrouter" {
        return Err(WeftError::Input(format!(
            "decision nodes speak the OpenRouter Decisions API; wire an OpenRouter Decision Model \
             (got '{kind}')"
        )));
    }
    let model: String = provider.get("model")?;
    let account: Access = provider.get("account")?;
    let asked = question["type"].as_str().unwrap_or_default().to_string();
    let mut body = json!({
        "model": model,
        "state": state.value,
        "questions": { ANSWER: question },
    });
    if let Some(routing) = provider.opt::<Value>(ROUTING)? {
        body["provider"] = routing;
    }

    let http = ctx.client(&account).await?;
    let url = format!("{OPENROUTER_API}/{ROUTE}");
    let call = weft::access::client::post_json(&http, &url, &body, "openrouter: decision");
    let cancelled = ctx.cancellation();
    let response = tokio::select! {
        // A run already stopped sends nothing.
        biased;
        err = cancelled.cancelled_err() => return Err(err),
        response = call => response?,
    };
    let answer = &response["answers"][ANSWER];
    if answer["type"].as_str() != Some(asked.as_str()) {
        weft::node_bail!(
            "openrouter: the decision model answered {} to a {asked} question",
            match answer["type"].as_str() {
                Some(other) => format!("a {other}"),
                None => "nothing".to_string(),
            }
        );
    }
    Ok(answer.clone())
}

/// A number the answer must carry.
pub fn number(answer: &Value, field: &str) -> WeftResult<f64> {
    answer[field].as_f64().ok_or_else(|| {
        weft::node_error(format!("openrouter: the decision model's answer carries no `{field}`"))
    })
}

/// The answer's `probabilities`, an object of numbers.
pub fn probabilities(answer: &Value) -> WeftResult<&Map<String, Value>> {
    let probabilities = answer["probabilities"].as_object().ok_or_else(|| {
        weft::node_error("openrouter: the decision model's answer carries no `probabilities`")
    })?;
    if let Some((key, _)) = probabilities.iter().find(|(_, p)| !p.is_number()) {
        weft::node_bail!("openrouter: the decision model answered a probability for `{key}` that is not a number");
    }
    Ok(probabilities)
}

/// One media file of the state, sent as its own item.
struct Media {
    /// Its port, and its position when the port is a list (`photos[1]`).
    place: String,
    kind: FileKind,
    part: Value,
}

/// Read the file at `place`: a text or JSON file is its content, handed
/// back for the state's object; a media file is pushed onto `media`.
async fn read_file(ctx: &RunContext, place: &str, file: &Value, media: &mut Vec<Media>) -> WeftResult<Option<Value>> {
    let slot = classify_media_slot(file).map_err(|why| WeftError::Input(format!("`{place}`: {why}")))?;
    let (kind, mime, bytes) = match slot {
        MediaSlotContent::Stored { handle, kind } => {
            let (meta, bytes) = ctx.storage(StorageScope::Run).get_bytes(&handle).await?;
            (kind, meta.mime_type, bytes)
        }
        MediaSlotContent::DataUrl { mime, bytes } => (FileKind::from_mime(&mime), mime, bytes.into()),
        MediaSlotContent::ExternalUrl(url) => weft::node_bail!(
            "`{place}` is a web address ({url}), and decision models never fetch one: store the file \
             first and wire the stored file"
        ),
    };
    if kind == FileKind::Blob {
        return file_content(place, &mime, &bytes).map(Some);
    }
    media.push(Media { place: place.to_string(), kind, part: media_part(place, kind, &mime, &bytes)? });
    Ok(None)
}

/// A text or JSON file as the value the state holds in its place: JSON
/// parsed, text as a string.
pub(crate) fn file_content(place: &str, mime: &str, bytes: &[u8]) -> WeftResult<Value> {
    let essence = mime.split(';').next().unwrap_or_default().trim().to_ascii_lowercase();
    if essence == "application/json" || essence.ends_with("+json") {
        let content: Value = serde_json::from_slice(bytes).map_err(|e| {
            weft::node_error(format!("`{place}` is a JSON file that does not parse: {e}"))
        })?;
        // A stored file named inside a file's content is read by nothing,
        // and its value names a file of this project, which never leaves
        // weft.
        if holds_file_marker(&content) {
            weft::node_bail!(
                "`{place}` is a JSON file naming a stored file inside it, which is never read from \
                 there: send that file on a port of its own type (`photo: Image`)"
            );
        }
        return Ok(content);
    }
    let text = essence.starts_with("text/")
        || essence == "application/xml"
        || essence.ends_with("+xml")
        || essence == "application/yaml";
    if !text {
        weft::node_bail!(
            "`{place}` is a {essence} file, and a decision model reads a file as text: turn it \
             into text first"
        );
    }
    String::from_utf8(bytes.to_vec())
        .map(Value::String)
        .map_err(|_| weft::node_error(format!("`{place}` is a {essence} file that is not UTF-8 text")))
}

/// One media file as the Decisions API part for it, its bytes inline
/// (the API fetches no link).
fn media_part(place: &str, kind: FileKind, mime: &str, bytes: &[u8]) -> WeftResult<Value> {
    Ok(match kind {
        FileKind::Image => json!({ "type": "image_url", "image_url": { "url": data_url(mime, bytes) } }),
        FileKind::Video => json!({ "type": "video_url", "video_url": { "url": data_url(mime, bytes) } }),
        FileKind::Audio => json!({
            "type": "input_audio",
            "input_audio": {
                "data": base64::engine::general_purpose::STANDARD.encode(bytes),
                "format": audio_format(place, mime)?,
            },
        }),
        FileKind::Blob => unreachable!("a non-media file is read as text, never sent as an item"),
    })
}

/// The `input_audio` format an audio mime type names.
pub(crate) fn audio_format(place: &str, mime: &str) -> WeftResult<&'static str> {
    let essence = mime.split(';').next().unwrap_or_default().trim().to_ascii_lowercase();
    let subtype = essence.strip_prefix("audio/").unwrap_or_default();
    let subtype = subtype.strip_prefix("x-").unwrap_or(subtype);
    let subtype = match subtype {
        "wave" | "vnd.wave" => "wav",
        other => other,
    };
    AUDIO_FORMATS.iter().copied().find(|format| *format == subtype).ok_or_else(|| {
        weft::node_error(format!(
            "`{place}` is {essence} audio, which the Decisions API does not take (it takes {})",
            AUDIO_FORMATS.join(", ")
        ))
    })
}

/// The state as sent: the fields as one object, or, when there is
/// media, an array of that object (when there is any field) followed by
/// each label and its part.
pub(crate) fn layout(fields: Map<String, Value>, media: Vec<Value>) -> Value {
    if media.is_empty() {
        return Value::Object(fields);
    }
    let mut items = Vec::with_capacity(media.len() + 1);
    if !fields.is_empty() {
        items.push(Value::Object(fields));
    }
    items.extend(media);
    Value::Array(items)
}
