//! Type-driven media traversal: find and rewrite the MEDIA SLOTS of a
//! typed JSON value.
//!
//! A media slot is any position whose DECLARED type is a stored-file
//! type (`Image`/`Video`/`Audio`/`Blob`, alone or as a union member),
//! anywhere inside lists, dicts, records, named types, and unions. The
//! canonical stored form keeps a stored-file value (the
//! `__weft_<kind>__` reference) in every media slot, so journals carry
//! small references; converting a whole typed value to and from forms
//! the outside world consumes (a link per slot, inline base64) is `StorageHandle::externalize` / `internalize`, which are
//! thin I/O loops around the PURE walks here.
//!
//! The walks are deliberately not validators: a value that does not
//! match the declared shape at some position is left untouched there
//! (`WeftType::validate_value` owns shape errors).

use std::collections::HashMap;

use serde_json::Value;

use crate::weft_type::{FileKind, WeftPrimitive, WeftType};

/// What external form a stored media slot takes: a link (cheap:
/// nothing is downloaded, the consumer fetches the bytes itself) or an inline `data:` URL (the bytes are read and embedded,
/// for consumers that only take base64).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MediaForm {
    /// A PREFERENCE, not a promise: the slot becomes the file's external
    /// link when the open internet can open it (`FileLink::public`), and
    /// inline bytes when it cannot (a local install with no tunnel and no
    /// visitor address). Consumers that only take base64 declare `Inline`.
    Url,
    /// Always inline bytes.
    Inline,
}

/// Per-kind externalize policy. The CALLER picks (it knows its
/// consumer), and real consumers mix forms: a chat provider takes
/// image URLs but only inline base64 audio. Start from
/// [`Self::urls`]/[`Self::inline`] and override fields:
/// `ExternalizePolicy { audio: MediaForm::Inline, ..ExternalizePolicy::urls() }`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExternalizePolicy {
    pub image: MediaForm,
    pub audio: MediaForm,
    pub video: MediaForm,
    pub blob: MediaForm,
}

impl ExternalizePolicy {
    /// Every kind as a link.
    pub fn urls() -> Self {
        Self {
            image: MediaForm::Url,
            audio: MediaForm::Url,
            video: MediaForm::Url,
            blob: MediaForm::Url,
        }
    }

    /// Every kind inlined as a `data:` URL.
    pub fn inline() -> Self {
        Self {
            image: MediaForm::Inline,
            audio: MediaForm::Inline,
            video: MediaForm::Inline,
            blob: MediaForm::Inline,
        }
    }

    /// The form for one concrete kind.
    pub fn form(&self, kind: FileKind) -> MediaForm {
        match kind {
            FileKind::Image => self.image,
            FileKind::Audio => self.audio,
            FileKind::Video => self.video,
            FileKind::Blob => self.blob,
        }
    }
}

/// What a media slot currently holds.
#[derive(Debug, Clone, PartialEq)]
pub enum MediaSlotContent {
    /// The canonical form: a stored-file reference (key- or
    /// url-backed marker). The marker's own sentinel says the KIND, so
    /// per-kind policies never guess from a mime.
    Stored { handle: crate::storage::FileHandle, kind: FileKind },
    /// Inline material: a `data:<mime>;base64,<payload>` URL.
    DataUrl { mime: String, bytes: Vec<u8> },
    /// External material: a plain http(s) URL string.
    ExternalUrl(String),
}

/// Classify one media slot's value. `Err` for material no media slot
/// may hold (a bare non-URL string, a number, an object that is not a
/// stored-file marker): the caller reports it loudly.
pub fn classify_media_slot(value: &Value) -> Result<MediaSlotContent, String> {
    if let Some(s) = value.as_str() {
        if let Some(rest) = s.strip_prefix("data:") {
            let (mime, payload) = rest
                .split_once(";base64,")
                .ok_or_else(|| "data: URL without a base64 payload".to_string())?;
            let bytes = base64_decode(payload)
                .map_err(|e| format!("data: URL payload is not valid base64: {e}"))?;
            return Ok(MediaSlotContent::DataUrl { mime: mime.to_string(), bytes });
        }
        if s.starts_with("http://") || s.starts_with("https://") {
            return Ok(MediaSlotContent::ExternalUrl(s.to_string()));
        }
        return Err(format!(
            "a media slot holds a bare string that is neither a data: URL nor an http(s) \
             URL: {}",
            crate::truncate_user_string(s, 128)
        ));
    }
    let kind = value
        .as_object()
        .and_then(FileKind::from_marker_obj)
        .ok_or_else(|| {
            format!(
                "a media slot holds an object that is not a stored-file value: {}",
                crate::truncate_user_string(&value.to_string(), 128)
            )
        })?;
    let handle =
        crate::storage::FileHandle::from_value(value).map_err(|e| e.to_string())?;
    Ok(MediaSlotContent::Stored { handle, kind })
}

/// Collect every media slot's VALUE in `value` per `ty`, deduped by
/// serialized form, in traversal order. The companion of
/// [`substitute_media`]: collect, resolve each to its replacement,
/// substitute.
pub fn media_slots(value: &Value, ty: &WeftType) -> Vec<Value> {
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::new();
    walk(value, ty, &mut |slot, _| {
        if seen.insert(slot.to_string()) {
            out.push(slot.clone());
        }
        None
    });
    out
}

/// [`media_slots`] with the type each slot is declared as (`Image`, or
/// the union `Image | Null` it sits in) and where the slot sits in the
/// value (`photo`, `pages[2].scan`; empty when the value itself is the
/// slot), for a caller that holds what arrives in a slot to what the
/// slot says it takes and names the place that did not. One entry per
/// distinct (value, type) pair, at its first place, in traversal order.
pub fn typed_media_slots(value: &Value, ty: &WeftType) -> Vec<TypedSlot> {
    let mut out: Vec<TypedSlot> = Vec::new();
    walk_at(value, ty, &mut Vec::new(), &mut |at, slot, slot_ty| {
        if !out.iter().any(|s| s.value == *slot && s.ty == *slot_ty) {
            out.push(TypedSlot { at: place_name(at), value: slot.clone(), ty: slot_ty.clone() });
        }
        None
    });
    out
}

/// One media slot [`typed_media_slots`] found.
#[derive(Debug, Clone, PartialEq)]
pub struct TypedSlot {
    /// Where it sits: record fields and dict keys joined by `.`, list
    /// items as `[i]`. Empty when the whole value is the slot.
    pub at: String,
    pub value: Value,
    pub ty: WeftType,
}

/// One step into a value on the way to a slot.
enum Step<'v> {
    Key(&'v str),
    Item(usize),
}

fn place_name(at: &[Step<'_>]) -> String {
    let mut out = String::new();
    for step in at {
        match step {
            Step::Key(key) => {
                if !out.is_empty() {
                    out.push('.');
                }
                out.push_str(key);
            }
            Step::Item(i) => out.push_str(&format!("[{i}]")),
        }
    }
    out
}

/// Rewrite `value` per `ty`, replacing each media slot whose value has
/// an entry in `replacements` (keyed by the slot value's serialized
/// form, as [`media_slots`] deduped it). A slot without an entry is
/// kept as-is.
pub fn substitute_media(
    value: &Value,
    ty: &WeftType,
    replacements: &HashMap<String, Value>,
) -> Value {
    walk(value, ty, &mut |slot, _| replacements.get(&slot.to_string()).cloned())
        .unwrap_or_else(|| value.clone())
}

/// One stored file a value holds where its declared type says a file
/// goes, as [`map_stored_files`] hands it over.
#[derive(Debug, Clone, PartialEq)]
pub struct StoredSlot {
    /// The file's address, to read it or link it.
    pub handle: crate::storage::FileHandle,
    /// The kind its marker names, so a per-kind choice never guesses from
    /// a mime.
    pub kind: FileKind,
    /// The stored-file value itself (the marker), for what its payload
    /// says beyond the address (name, type, size).
    pub value: Value,
}

/// `value` with every stored file its type `ty` places in it replaced by
/// what `replace` makes of it: the ONE walk of a value's files on their
/// way out of weft (a link for a caller, an inline `data:` URL for a
/// provider, a dict for a script). The type decides what is a file,
/// never a marker key found in raw JSON: a `JsonDict` that happens to
/// carry one is left whole. Material already outside weft in a file place
/// (a `data:` URL, an http URL) passes untouched; anything else there is
/// refused, as [`classify_media_slot`] words it. Each distinct file is
/// handed over once, however often it appears.
pub async fn map_stored_files<F, Fut>(value: &Value, ty: &WeftType, mut replace: F) -> crate::error::WeftResult<Value>
where
    F: FnMut(StoredSlot) -> Fut,
    Fut: std::future::Future<Output = crate::error::WeftResult<Value>>,
{
    let mut replacements = HashMap::new();
    for slot in media_slots(value, ty) {
        let (handle, kind) = match classify_media_slot(&slot).map_err(crate::error::WeftError::Input)? {
            MediaSlotContent::Stored { handle, kind } => (handle, kind),
            MediaSlotContent::DataUrl { .. } | MediaSlotContent::ExternalUrl(_) => continue,
        };
        let key = slot.to_string();
        let made = replace(StoredSlot { handle, kind, value: slot }).await?;
        replacements.insert(key, made);
    }
    Ok(substitute_media(value, ty, &replacements))
}

/// Whether a stored-file value sits anywhere in `value`. Type-free: it
/// finds markers by their sentinel.
pub fn holds_file_marker(value: &Value) -> bool {
    match value {
        Value::Object(map) => FileKind::from_marker_obj(map).is_some() || map.values().any(holds_file_marker),
        Value::Array(items) => items.iter().any(holds_file_marker),
        _ => false,
    }
}

/// The one traversal both verbs share. `f` sees every media slot's
/// value and the type the slot is declared as, and answers an optional
/// replacement; the walk answers a rebuilt value when anything changed
/// underneath (None = subtree unchanged, caller keeps the original).
fn walk(
    value: &Value,
    ty: &WeftType,
    f: &mut impl FnMut(&Value, &WeftType) -> Option<Value>,
) -> Option<Value> {
    walk_at(value, ty, &mut Vec::new(), &mut |_, slot, slot_ty| f(slot, slot_ty))
}

/// [`walk`], telling `f` where each slot sits. The steps are borrowed
/// from the value and pushed and popped as the walk descends, so a
/// caller that never reads them pays no allocation per slot.
fn walk_at<'v>(
    value: &'v Value,
    ty: &WeftType,
    at: &mut Vec<Step<'v>>,
    f: &mut impl FnMut(&[Step<'v>], &Value, &WeftType) -> Option<Value>,
) -> Option<Value> {
    match ty {
        WeftType::Primitive(p) if FileKind::from_primitive(*p).is_some() => f(at, value, ty),
        WeftType::List(inner) => {
            let items = value.as_array()?;
            rebuild_seq(items, |i, item| {
                at.push(Step::Item(i));
                let out = walk_at(item, inner, at, f);
                at.pop();
                out
            })
        }
        WeftType::Dict(v_ty) => {
            let map = value.as_object()?;
            rebuild_map(map, |k, v| {
                at.push(Step::Key(k));
                let out = walk_at(v, v_ty, at, f);
                at.pop();
                out
            })
        }
        WeftType::Record { fields, .. } => {
            let map = value.as_object()?;
            rebuild_map(map, |k, v| {
                let field = fields.iter().find(|fld| fld.name == *k)?;
                at.push(Step::Key(k));
                let out = walk_at(v, &field.ty, at, f);
                at.pop();
                out
            })
        }
        WeftType::Named { body, .. } => walk_at(value, body, at, f),
        WeftType::Union(members) => {
            // A union with a member that is ITSELF a file (`Image |
            // Null`, `Media`) makes the slot a media slot when the value
            // is a stored-file object (well formed or not: a broken one
            // reaches `f`, which says so), or a string the union can only
            // read as a file. With a `String` member a string is text,
            // whatever it looks like (a URL, a `data:` URL), and is never
            // fetched or decoded. Members that merely CONTAIN files
            // deeper (`String | List[Part]`) don't make the slot one: the
            // walk descends into them instead, never into the file
            // members.
            if members.iter().any(|m| m.is_file_valued()) {
                let takes_text = members.iter().any(takes_text);
                let is_file_object = value.as_object().and_then(FileKind::from_marker_obj).is_some();
                if is_file_object || (value.is_string() && !takes_text) {
                    return f(at, value, ty);
                }
            }
            members.iter().filter(|m| !m.is_file_valued()).find_map(|m| walk_at(value, m, at, f))
        }
        _ => None,
    }
}

/// Whether a value of `ty` may be text: `String`, through any declared
/// name and nested union (`type Text = String | Number`).
fn takes_text(ty: &WeftType) -> bool {
    match ty.structural() {
        WeftType::Primitive(WeftPrimitive::String) => true,
        WeftType::Union(members) => members.iter().any(takes_text),
        _ => false,
    }
}

/// Rebuild an array from per-item results; None when nothing changed.
fn rebuild_seq<'v>(
    items: &'v [Value],
    mut per_item: impl FnMut(usize, &'v Value) -> Option<Value>,
) -> Option<Value> {
    let results: Vec<Option<Value>> = items.iter().enumerate().map(|(i, item)| per_item(i, item)).collect();
    if results.iter().all(Option::is_none) {
        return None;
    }
    Some(Value::Array(
        items
            .iter()
            .zip(results)
            .map(|(orig, replaced)| replaced.unwrap_or_else(|| orig.clone()))
            .collect(),
    ))
}

/// Rebuild an object from per-entry results; None when nothing changed.
fn rebuild_map<'v>(
    map: &'v serde_json::Map<String, Value>,
    mut per_entry: impl FnMut(&'v String, &'v Value) -> Option<Value>,
) -> Option<Value> {
    let results: Vec<Option<Value>> = map.iter().map(|(k, v)| per_entry(k, v)).collect();
    if results.iter().all(Option::is_none) {
        return None;
    }
    Some(Value::Object(
        map.iter()
            .zip(results)
            .map(|((k, orig), replaced)| (k.clone(), replaced.unwrap_or_else(|| orig.clone())))
            .collect(),
    ))
}

/// Decode standard base64 (what a `data:` URL and a JSON body carry).
pub fn base64_decode(payload: &str) -> Result<Vec<u8>, String> {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD
        .decode(payload)
        .map_err(|e| e.to_string())
}

/// Base64-encode bytes as a `data:<mime>;base64,<payload>` URL, the
/// inline form [`StorageHandle::externalize`]'s `Inline` mode emits.
///
/// [`StorageHandle::externalize`]: crate::context::StorageHandle::externalize
pub fn data_url(mime: &str, bytes: &[u8]) -> String {
    use base64::Engine;
    format!(
        "data:{mime};base64,{}",
        base64::engine::general_purpose::STANDARD.encode(bytes)
    )
}

/// One stored file in the form an outside service can read, with what
/// a send to that service usually needs beside it. What
/// [`StorageHandle::external_file`] answers.
///
/// [`StorageHandle::external_file`]: crate::context::StorageHandle::external_file
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalFile {
    /// The file's external link when the open internet can open it, else
    /// an inline `data:` URL carrying the bytes.
    pub url: String,
    pub mime_type: String,
    pub filename: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn marker(key: &str) -> Value {
        WeftType::file_marker(
            FileKind::Image,
            json!({ "key": key, "mimeType": "image/png" }),
        )
    }

    fn history_ty() -> WeftType {
        let reg = std::sync::Arc::new(
            crate::weft_type::TypeRegistry::build(&[
                (
                    "ChatHistory".into(),
                    "List[{ role: String, content: String | List[Part] }]".into(),
                    "test".into(),
                ),
                ("Part".into(), "{ type: String, image?: Image }".into(), "test".into()),
            ])
            .unwrap(),
        );
        reg.scoped(|| WeftType::parse("ChatHistory")).unwrap()
    }

    #[test]
    fn slots_are_found_through_records_lists_unions_and_names() {
        let value = json!([
            { "role": "user", "content": "plain text" },
            { "role": "user", "content": [
                { "type": "text" },
                { "type": "image", "image": marker("a") },
                { "type": "image", "image": marker("b") },
                { "type": "image", "image": marker("a") },
            ]},
        ]);
        let slots = media_slots(&value, &history_ty());
        assert_eq!(slots, vec![marker("a"), marker("b")], "deduped, in order");
    }

    #[test]
    fn a_typed_slot_names_where_it_sits() {
        let value = json!([
            { "role": "user", "content": [
                { "type": "image", "image": marker("a") },
                { "type": "image", "image": marker("b") },
            ]},
        ]);
        let at: Vec<String> = typed_media_slots(&value, &history_ty()).into_iter().map(|s| s.at).collect();
        assert_eq!(at, vec!["[0].content[0].image", "[0].content[1].image"]);
        let whole = typed_media_slots(&marker("a"), &WeftType::parse("Image").unwrap());
        assert_eq!(whole[0].at, "", "the value itself is the slot");
    }

    #[test]
    fn substitute_replaces_only_mapped_slots_and_keeps_the_rest() {
        let value = json!([
            { "role": "user", "content": [
                { "type": "image", "image": marker("a") },
                { "type": "image", "image": marker("b") },
            ]},
        ]);
        let map =
            HashMap::from([(marker("a").to_string(), json!("https://signed.example/a"))]);
        let out = substitute_media(&value, &history_ty(), &map);
        assert_eq!(
            out[0]["content"][0]["image"],
            json!("https://signed.example/a")
        );
        assert_eq!(out[0]["content"][1]["image"], marker("b"), "unmapped slot kept");
        // Nothing outside media slots is touched.
        assert_eq!(out[0]["role"], "user");
    }

    #[test]
    fn untyped_interiors_are_invisible_to_the_walk() {
        // A JsonDict-typed position holding a marker-shaped object is
        // NOT a media slot: the walk is type-driven, never a scan.
        let value = json!({ "x": marker("a") });
        assert!(media_slots(&value, &WeftType::JsonDict).is_empty());
    }

    #[test]
    fn classification_covers_the_three_forms_and_refuses_junk() {
        assert!(matches!(
            classify_media_slot(&marker("a")),
            Ok(MediaSlotContent::Stored { kind: FileKind::Image, .. })
        ));
        assert_eq!(
            classify_media_slot(&json!("https://x.example/f.png")),
            Ok(MediaSlotContent::ExternalUrl("https://x.example/f.png".into()))
        );
        let data = classify_media_slot(&json!("data:image/png;base64,aGk=")).unwrap();
        assert_eq!(
            data,
            MediaSlotContent::DataUrl { mime: "image/png".into(), bytes: b"hi".to_vec() }
        );
        assert!(classify_media_slot(&json!("just words")).is_err());
        assert!(classify_media_slot(&json!(42)).is_err());
        assert!(classify_media_slot(&json!({ "not": "a marker" })).is_err());
    }

    #[test]
    fn data_url_round_trips_through_classification() {
        let url = data_url("image/png", b"pixels");
        match classify_media_slot(&Value::String(url)).unwrap() {
            MediaSlotContent::DataUrl { mime, bytes } => {
                assert_eq!(mime, "image/png");
                assert_eq!(bytes, b"pixels");
            }
            other => panic!("expected DataUrl, got {other:?}"),
        }
    }
}
