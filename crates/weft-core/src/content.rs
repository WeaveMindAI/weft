//! The text of a file a program names (`@file`, a text-typed `@asset`),
//! carried by name instead of inline.
//!
//! Inline, a 2 MB SQL script read by `@file` sat in every copy of the
//! program: each trigger's cuts, each definition hash, each stored setup,
//! each program a worker keeps. So a built program holds a small marker
//! in its place, naming the content by the sha256 of its bytes and the
//! type it is cast to, with the path it was read from for whoever reads
//! the marker: `{"__weft_content__": {"sha256": "<hex>", "type":
//! "String", "path": "sql/import.sql"}}`. The
//! bytes are one of the tenant's assets (`asset/<sha256>`, read by every
//! worker of the tenant): a version's file is stored there by the version
//! itself, and the build stores any other text it read (a URL, a stored
//! file, a file of the machine). One content is stored once however many
//! programs and versions name it, and a changed file is a new hash, so a
//! new definition hash, exactly as the inline text made it.
//!
//! A worker puts every value back when it reads a program
//! ([`resolve_program`]): it reads each content once, casts it to its
//! type, and writes the value where the marker was, so everything that
//! runs a node (its inputs, a group's literal, an `@instance_filled`
//! fallback, a key path into a JSON value) reads the value as it was
//! written and never meets a marker.
//!
//! Only the build makes one (`weft_compiler`'s compile, the build's
//! asset step): the compiler refuses a marker written in a program, which
//! would name content its author never stored.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::project::ProjectDefinition;
use crate::weft_type::WeftType;

/// The value of each content a program names, by [`ContentRef::id`]: what
/// [`resolve_program`] puts back.
pub type ContentValues = BTreeMap<(String, String), Arc<Value>>;

/// The on-wire sentinel key that tags a [`ContentRef`].
// SYNC: CONTENT_MARKER_KEY <-> packages/weft-graph/src/protocol.ts CONTENT_MARKER_KEY
pub const CONTENT_MARKER_KEY: &str = "__weft_content__";

/// A value a program carries by name: the content's sha256 and the type
/// its text is cast to, and where it was read from (a path from the
/// project root, a path on the machine, a URL, a stored file's key), which
/// names it to a person. Two refs to one content cast to two types are two
/// values.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContentRef {
    sha256: String,
    #[serde(rename = "type")]
    ty: WeftType,
    path: String,
}

impl ContentRef {
    /// The ref to `text`, read from `path`, cast to `ty`, with the value
    /// it stands for: the declared type's own cast of the text, so a cast
    /// that fails is the same error it always was, before anything is
    /// named. A file whose value holds a marker is refused: a worker would
    /// read it as content the tenant stored.
    pub fn of_text(text: &str, ty: &WeftType, path: &str) -> Result<(ContentRef, Value), String> {
        let value = ty.cast_text(text)?;
        if Self::claimed_in(&value) {
            return Err(format!(
                "the file holds `{CONTENT_MARKER_KEY}`, which only a build writes, for the content of a file it read; \
                 a file's own JSON cannot use that key: rename it in the file, or read the file as String"
            ));
        }
        Ok((ContentRef { sha256: crate::project::hash::sha256_hex(text.as_bytes()), ty: ty.clone(), path: path.to_string() }, value))
    }

    /// The sha256 of the content's bytes: the asset it is stored as.
    pub fn sha256(&self) -> &str {
        &self.sha256
    }

    pub fn ty(&self) -> &WeftType {
        &self.ty
    }

    /// Where the content was read from.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// The content's key in the tenant's assets, tenant-less
    /// (`asset/<sha256>`).
    pub fn asset_key(&self) -> String {
        crate::storage::key::scope_key(&crate::storage::key::KeyScope::Asset, &self.sha256)
            .expect("a content ref is only read or made with a content hash")
    }

    /// What names this value among others: the content and the type
    /// (one content read from two paths is one value).
    pub fn id(&self) -> (String, String) {
        (self.sha256.clone(), self.ty.wire_string())
    }

    /// The value `bytes` stand for, once they are checked to be this
    /// content: what a worker reads back from the store is held to the
    /// hash it was asked by, then cast like the build cast it.
    pub fn value_of(&self, bytes: &[u8]) -> Result<Value, String> {
        let got = crate::project::hash::sha256_hex(bytes);
        if got != self.sha256 {
            return Err(format!("the stored content {} came back as {got}; the store holds other bytes under that name", self.sha256));
        }
        let text = std::str::from_utf8(bytes).map_err(|e| format!("the stored content {} is not UTF-8 text: {e}", self.sha256))?;
        self.ty.cast_text(text).map_err(|e| format!("the stored content {} no longer reads as {}: {e}", self.sha256, self.ty))
    }

    /// The marker.
    pub fn to_value(&self) -> Value {
        serde_json::json!({ CONTENT_MARKER_KEY: self })
    }

    /// The ref `value` is the marker of: an object holding the sentinel
    /// key alone, whose payload is a content hash and a type and nothing
    /// else. `None` for anything else, a malformed marker included.
    // SYNC: ContentRef::from_value <-> packages/weft-graph/src/protocol.ts contentMarker
    pub fn from_value(value: &Value) -> Option<Self> {
        let object = value.as_object().filter(|object| object.len() == 1)?;
        let content = ContentRef::deserialize(object.get(CONTENT_MARKER_KEY)?).ok()?;
        crate::storage::is_content_hash(&content.sha256).then_some(content)
    }

    /// Whether anything in `value`, at any depth, claims to be a content
    /// marker (an object carrying the sentinel key, well-formed or not):
    /// what a value written in a program never holds.
    pub fn claimed_in(value: &Value) -> bool {
        match value {
            Value::Object(fields) => fields.contains_key(CONTENT_MARKER_KEY) || fields.values().any(Self::claimed_in),
            Value::Array(items) => items.iter().any(Self::claimed_in),
            _ => false,
        }
    }
}

fn each_marker(value: &Value, visit: &mut impl FnMut(ContentRef)) {
    if let Some(content) = ContentRef::from_value(value) {
        return visit(content);
    }
    match value {
        Value::Array(items) => items.iter().for_each(|v| each_marker(v, visit)),
        Value::Object(fields) => fields.values().for_each(|v| each_marker(v, visit)),
        _ => {}
    }
}

/// Every content `project` names, once each.
pub fn refs_in_program(project: &ProjectDefinition) -> Vec<ContentRef> {
    let mut seen = BTreeSet::new();
    let mut refs = Vec::new();
    for value in project.written_values() {
        each_marker(value, &mut |content| {
            if seen.insert(content.id()) {
                refs.push(content);
            }
        });
    }
    refs
}

/// Every content `value` names, at any depth.
pub fn refs_in_value(value: &Value) -> Vec<ContentRef> {
    let mut refs = Vec::new();
    each_marker(value, &mut |content| refs.push(content));
    refs
}

/// Put back, in place, the value of every content `project` names:
/// `values` answers each ref ([`ContentRef::id`]) with the value it
/// stands for. Every ref `values` does not answer is named in the error:
/// the caller read every ref [`refs_in_program`] gave it.
pub fn resolve_program(project: &mut ProjectDefinition, values: &ContentValues) -> Result<(), String> {
    fn put_back(value: &mut Value, values: &ContentValues, missing: &mut BTreeMap<(String, String), ContentRef>) {
        if let Some(content) = ContentRef::from_value(value) {
            match values.get(&content.id()) {
                Some(resolved) => *value = (**resolved).clone(),
                None => {
                    missing.insert(content.id(), content);
                }
            }
            return;
        }
        match value {
            Value::Array(items) => items.iter_mut().for_each(|v| put_back(v, values, missing)),
            Value::Object(fields) => fields.values_mut().for_each(|v| put_back(v, values, missing)),
            _ => {}
        }
    }
    let mut missing = BTreeMap::new();
    for value in project.written_values_mut() {
        put_back(value, values, &mut missing);
    }
    if missing.is_empty() {
        return Ok(());
    }
    let named: Vec<String> = missing.values().map(|content| format!("'{}' ({}, {})", content.path, content.ty, content.sha256)).collect();
    Err(format!("the program names contents that were not read: {}", named.join(", ")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn string() -> WeftType {
        WeftType::Primitive(crate::weft_type::WeftPrimitive::String)
    }

    /// The marker's wire shape: one sentinel key, a content hash and the
    /// type's wire spelling, read back to the same ref.
    #[test]
    fn a_ref_round_trips_through_its_marker() {
        let (content, value) = ContentRef::of_text("SELECT 1;", &string(), "sql/one.sql").unwrap();
        assert_eq!(value, json!("SELECT 1;"));
        let marker = content.to_value();
        assert_eq!(
            marker,
            json!({ "__weft_content__": { "sha256": crate::project::hash::sha256_hex(b"SELECT 1;"), "type": "String", "path": "sql/one.sql" } })
        );
        assert_eq!(ContentRef::from_value(&marker), Some(content));
    }

    /// Anything else is no marker: another key beside it, a field too
    /// many, a hash that is no sha256.
    #[test]
    fn only_a_well_formed_marker_reads_as_one() {
        let hash = crate::project::hash::sha256_hex(b"x");
        assert!(ContentRef::from_value(&json!({ "__weft_content__": { "sha256": hash, "type": "String", "path": "x" } })).is_some());
        assert!(ContentRef::from_value(&json!({ "__weft_content__": { "sha256": hash, "type": "String", "path": "x" }, "other": 1 })).is_none());
        assert!(ContentRef::from_value(&json!({ "__weft_content__": { "sha256": hash, "type": "String", "path": "x", "size": 1 } })).is_none());
        assert!(ContentRef::from_value(&json!({ "__weft_content__": { "sha256": "../x", "type": "String", "path": "x" } })).is_none());
        assert!(ContentRef::claimed_in(&json!([{ "a": { "__weft_content__": 1 } }])));
        assert!(!ContentRef::claimed_in(&json!({ "a": "__weft_content__" })));
    }

    /// The bytes a worker reads back resolve to the cast value of each
    /// type, and bytes that are not the content are refused.
    #[test]
    fn stored_bytes_resolve_to_the_cast_value_or_are_refused() {
        let number = WeftType::Primitive(crate::weft_type::WeftPrimitive::Number);
        let (content, value) = ContentRef::of_text(" 42\n", &number, "n.txt").unwrap();
        assert_eq!(content.value_of(b" 42\n").unwrap(), value);
        assert_eq!(value, json!(42.0));
        let dict = WeftType::parse("JsonDict").unwrap();
        let (content, value) = ContentRef::of_text(r#"{"a": [1, 2]}"#, &dict, "a.json").unwrap();
        assert_eq!(content.value_of(br#"{"a": [1, 2]}"#).unwrap(), value);
        assert!(content.value_of(b"{}").unwrap_err().contains("other bytes"));
        assert!(ContentRef::of_text("not a number", &number, "n.txt").is_err());
    }

    /// A file whose JSON holds the marker's key is refused when it is
    /// cast, wherever the key sits: a worker would read it as stored
    /// content.
    #[test]
    fn a_file_holding_a_marker_is_refused() {
        let dict = WeftType::parse("JsonDict").unwrap();
        let hash = crate::project::hash::sha256_hex(b"x");
        let forged = format!(r#"{{"a": [{{"__weft_content__": {{"sha256": "{hash}", "type": "String", "path": "x"}}}}]}}"#);
        assert!(ContentRef::of_text(&forged, &dict, "forged.json").unwrap_err().contains("__weft_content__"));
        assert!(ContentRef::of_text(&forged, &string(), "forged.txt").is_ok(), "as text it is only text");
    }

    /// Every content a program names that was not read is named at once.
    #[test]
    fn every_content_not_read_is_named() {
        let (one, _) = ContentRef::of_text("1", &string(), "one.txt").unwrap();
        let (two, _) = ContentRef::of_text("2", &string(), "two.txt").unwrap();
        let mut project: ProjectDefinition = serde_json::from_value(json!({
            "id": uuid::Uuid::nil(), "name": "p", "description": null,
            "nodes": [{ "id": "q", "nodeType": "Text", "label": null, "config": { "b": [two.to_value()] }, "position": { "x": 0.0, "y": 0.0 },
                        "inputs": [], "outputs": [], "features": {}, "scope": [], "groupBoundary": null, "requiresInfra": false, "images": [] }],
            "edges": [], "groups": [],
            "createdAt": "1970-01-01T00:00:00Z", "updatedAt": "1970-01-01T00:00:00Z"
        }))
        .unwrap();
        project.nodes[0].port_literals.insert("a".into(), one.to_value());
        let e = resolve_program(&mut project, &ContentValues::new()).unwrap_err();
        assert!(e.contains("'one.txt'") && e.contains("'two.txt'"), "{e}");
    }

    /// One content cast to two types is two refs.
    #[test]
    fn one_content_cast_two_ways_is_two_refs() {
        let (as_text, _) = ContentRef::of_text("1", &string(), "one.txt").unwrap();
        let (as_number, _) = ContentRef::of_text("1", &WeftType::Primitive(crate::weft_type::WeftPrimitive::Number), "one.txt").unwrap();
        assert_eq!(as_text.sha256(), as_number.sha256());
        assert_ne!(as_text.id(), as_number.id());
    }
}
