//! Type-driven media traversal: find and rewrite the MEDIA SLOTS of a
//! typed JSON value.
//!
//! A media slot is any position whose DECLARED type is a stored-file
//! type (`Image`/`Video`/`Audio`/`Blob`, alone or as a union member),
//! anywhere inside lists, dicts, records, named types, and unions. The
//! canonical stored form keeps a stored-file value (the
//! `__weft_<kind>__` reference) in every media slot, so journals carry
//! small references; converting a whole typed value to and from forms
//! the outside world consumes (a presigned URL per slot, inline
//! base64) is `StorageHandle::externalize` / `internalize`, which are
//! thin I/O loops around the PURE walks here.
//!
//! The walks are deliberately not validators: a value that does not
//! match the declared shape at some position is left untouched there
//! (`WeftType::validate_value` owns shape errors).

use std::collections::HashMap;

use serde_json::Value;

use crate::weft_type::{FileKind, WeftType};

/// What external form a stored media slot takes: a presigned URL
/// (cheap: nothing is downloaded, the consumer fetches the bytes
/// itself) or an inline `data:` URL (the bytes are read and embedded,
/// for consumers that only take base64).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MediaForm {
    /// A PREFERENCE, not a promise: the slot becomes a public link when
    /// an internet-reachable address is configured to serve one, and
    /// falls back to inline bytes when none is (private store, no
    /// public relay). Consumers that only take base64 declare `Inline`.
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
    /// Every kind as a presigned URL.
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
    walk(value, ty, &mut |slot| {
        if seen.insert(slot.to_string()) {
            out.push(slot.clone());
        }
        None
    });
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
    walk(value, ty, &mut |slot| replacements.get(&slot.to_string()).cloned())
        .unwrap_or_else(|| value.clone())
}

/// The one traversal both verbs share. `f` sees every media slot's
/// value and answers an optional replacement; the walk answers a
/// rebuilt value when anything changed underneath (None = subtree
/// unchanged, caller keeps the original).
fn walk(
    value: &Value,
    ty: &WeftType,
    f: &mut impl FnMut(&Value) -> Option<Value>,
) -> Option<Value> {
    match ty {
        WeftType::Primitive(p) if FileKind::from_primitive(*p).is_some() => f(value),
        WeftType::List(inner) => {
            let items = value.as_array()?;
            rebuild_seq(items, |item| walk(item, inner, f))
        }
        WeftType::Dict(_, v_ty) => {
            let map = value.as_object()?;
            rebuild_map(map, |_, v| walk(v, v_ty, f))
        }
        WeftType::Record(fields) => {
            let map = value.as_object()?;
            rebuild_map(map, |k, v| {
                let field = fields.iter().find(|fld| fld.name == *k)?;
                walk(v, &field.ty, f)
            })
        }
        WeftType::Named { body, .. } => walk(value, body, f),
        WeftType::Union(members) => {
            // A union slot is a media slot exactly when a member is
            // ITSELF a media type (`Image | Null`, `Media`) and the
            // value holds media material. Members that merely CONTAIN
            // media deeper (`String | List[Part]`) don't make the slot
            // one: a plain string there is text, even when it looks
            // like a URL; the walk descends instead.
            if members.iter().any(|m| m.is_file_valued()) && classify_media_slot(value).is_ok() {
                return f(value);
            }
            members.iter().find_map(|m| walk(value, m, f))
        }
        _ => None,
    }
}

/// Rebuild an array from per-item results; None when nothing changed.
fn rebuild_seq(
    items: &[Value],
    mut per_item: impl FnMut(&Value) -> Option<Value>,
) -> Option<Value> {
    let results: Vec<Option<Value>> = items.iter().map(&mut per_item).collect();
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
fn rebuild_map(
    map: &serde_json::Map<String, Value>,
    mut per_entry: impl FnMut(&String, &Value) -> Option<Value>,
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

fn base64_decode(payload: &str) -> Result<Vec<u8>, String> {
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
