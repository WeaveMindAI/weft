//! Form submission with a rendered schema. The consumer (the browser
//! extension) reads `schema` to render a form; submission
//! flows back through the dispatcher's task-callback URL.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::Signal;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Form {
    /// Routes the form to the right UI panel ("human-trigger" vs
    /// "human-query"). Hardcoded by the node author; not pulled
    /// from config.
    pub form_type: String,
    pub schema: FormSchema,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Consumer label for token-scoped enumeration. Consumers (the
    /// browser extension) fetch signals tagged with the
    /// matching string (e.g. `"human_in_the_loop"`). `None` = not
    /// listed in any consumer surface.
    ///
    /// `serde(skip)` so the value lives only at the top of
    /// `SignalSpec` (lifted there by `signal::to_spec` via the
    /// `Signal::consumer_kind` trait method). Carrying it twice on
    /// the wire was a redundancy.
    #[serde(skip)]
    pub consumer_kind: Option<String>,
}

// SYNC: FormSchema/FormField wire shape <->
//       extension-browser/src/lib/api.ts (FormSchema/FormField)
// (catalog/human/form_helpers.rs constructs these structs directly, so
// the compiler keeps it in step; only the TS restatement can drift.)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FormSchema {
    /// Just the fields: the form's `title`/`description` live on
    /// [`Form`] itself, the single home (a second copy here was a
    /// duplicate nobody read).
    pub fields: Vec<FormField>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FormField {
    pub field_type: String,
    pub key: String,
    pub label: String,
    /// Render hint copied from the spec (component name + flags),
    /// typed so a field can never ship without one (a form field with
    /// no render is undrawable and refused at build time). The
    /// browser extension reads `render.component` to pick the UI
    /// primitive.
    pub render: crate::node::FormFieldRender,
    /// Pre-fill value for fields that need an upstream input port
    /// value (display, display_image, editable_*, *_input). None
    /// for purely interactive fields.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<Value>,
    /// Per-field config from the source (options, labels, etc).
    #[serde(default)]
    pub config: serde_json::Map<String, Value>,
}

impl FormSchema {
    /// The schema a CONSUMER reads (the browser extension, anything on
    /// the signal-token door). A stored file among the field values is
    /// weft's internal reference and never leaves: a key-backed one
    /// becomes `{ mimeType, sizeBytes, filename }`, which a consumer
    /// turns into a picture by asking the files door for a fresh link
    /// each time it renders (so a form answered a month later still
    /// shows its image, and an expired file says so); a URL-backed
    /// one is its own link, `{ url, mimeType, sizeBytes, filename }`.
    /// A field value can be anything an upstream port produced (a
    /// `display` field takes any type), so the rule walks into lists and
    /// objects: a file marker nested three levels down is projected like
    /// a top-level one. A value carrying no file marker at all is data,
    /// and passes through untouched.
    ///
    /// Errors when a value IS a file marker that does not read as a
    /// file. That form is broken, and publishing its raw marker would
    /// hand the consumer the storage key this method exists to keep in.
    ///
    /// A file nested inside a value keeps its facts and loses its key
    /// like any other, but the files door answers per FIELD, so only a
    /// field that IS a file can be fetched. A nested one shows its name
    /// and size and no picture.
    pub fn for_consumer(&self) -> Result<FormSchema, String> {
        let mut fields = Vec::with_capacity(self.fields.len());
        for field in &self.fields {
            let value = match &field.value {
                Some(v) => Some(consumer_file_value(v).map_err(|e| {
                    format!("form field '{}' carries an unreadable file: {e}", field.key)
                })?),
                None => None,
            };
            fields.push(FormField { value, ..field.clone() });
        }
        Ok(FormSchema { fields })
    }

    /// The stored file a field carries, for the files door. `Ok(None)`
    /// when the field is unknown, holds nothing, holds something that
    /// is not a file, or holds a URL-backed file (which needs no door).
    /// `Err` when it holds a stored-file marker that does not read.
    pub fn stored_file(&self, field_key: &str) -> Result<Option<crate::storage::StoredFile>, String> {
        let Some(field) = self.fields.iter().find(|f| f.key == field_key) else {
            return Ok(None);
        };
        let Some(value) = field.value.as_ref() else {
            return Ok(None);
        };
        if !is_file_marker(value) {
            return Ok(None);
        }
        match crate::storage::FileHandle::from_value(value).map_err(|e| e.to_string())? {
            crate::storage::FileHandle::Url { .. } => Ok(None),
            crate::storage::FileHandle::Key(_) => crate::storage::StoredFile::from_value(value)
                .map(Some)
                .map_err(|e| format!("form field '{field_key}' carries an unreadable file: {e}")),
        }
    }
}

/// Does this value carry one of the four file markers
/// (`__weft_image__`/video/audio/blob)? The one test for "this is a
/// file value", so plain data (a string that happens to look like a
/// storage key, a number, a form's own object) is never mistaken for
/// one. Note `FileHandle::from_value` deliberately also accepts a bare
/// key string for the read path; that is not a file VALUE and must not
/// be treated as one here.
fn is_file_marker(value: &Value) -> bool {
    value
        .as_object()
        .is_some_and(|obj| crate::weft_type::FileKind::from_marker_obj(obj).is_some())
}

/// [`FormSchema::for_consumer`]'s rule for one value, applied to every
/// file marker anywhere inside it.
fn consumer_file_value(value: &Value) -> Result<Value, String> {
    if is_file_marker(value) {
        return match crate::storage::FileHandle::from_value(value).map_err(|e| e.to_string())? {
            // SYNC: the four consumer file keys <-> crates/weft-dispatcher/src/api/signal.rs (SignalFileLink), extension-browser/src/lib/api.ts (TaskFileLink, isStoredFileValue)
            crate::storage::FileHandle::Url { url, mime_type, filename, size_bytes } => {
                Ok(serde_json::json!({
                    "url": url,
                    "mimeType": mime_type,
                    "sizeBytes": size_bytes,
                    "filename": filename,
                }))
            }
            crate::storage::FileHandle::Key(_) => {
                let file = crate::storage::StoredFile::from_value(value).map_err(|e| e.to_string())?;
                Ok(serde_json::json!({
                    "mimeType": file.mime_type,
                    "sizeBytes": file.size_bytes,
                    "filename": file.filename,
                }))
            }
        };
    }
    match value {
        Value::Array(items) => items.iter().map(consumer_file_value).collect::<Result<Vec<_>, _>>().map(Value::Array),
        Value::Object(obj) => {
            let mut out = serde_json::Map::with_capacity(obj.len());
            for (k, v) in obj {
                out.insert(k.clone(), consumer_file_value(v)?);
            }
            Ok(Value::Object(out))
        }
        _ => Ok(value.clone()),
    }
}

impl Signal for Form {
    const TAG: &'static str = "form";

    fn validate(&self) -> Result<(), String> {
        if self.form_type.trim().is_empty() {
            return Err("form.form_type must not be empty".into());
        }
        Ok(())
    }

    fn consumer_kind(&self) -> Option<&str> {
        self.consumer_kind.as_deref()
    }

    fn stored_file(&self, field: &str) -> Result<Option<crate::storage::StoredFile>, String> {
        self.schema.stored_file(field)
    }
}

crate::register_signal_kind!(Form);

#[cfg(test)]
mod wire_tests {
    use super::*;

    fn field() -> FormField {
        FormField {
            field_type: "text_input".into(),
            key: "answer".into(),
            label: "Answer".into(),
            render: crate::node::FormFieldRender {
                component: "text".into(),
                source: None,
                multiple: false,
                prefilled: false,
            },
            value: None,
            config: serde_json::Map::new(),
        }
    }

    /// The wire is what the browser extension's TS restatement reads
    /// and what registered signals persist, so the EXACT JSON is the
    /// contract, keys and absences included.
    #[test]
    fn form_wire_shape_with_title_and_description() {
        let form = Form {
            form_type: "human-query".into(),
            schema: FormSchema { fields: vec![field()] },
            title: Some("Ask".into()),
            description: Some("Why".into()),
            consumer_kind: Some("human_in_the_loop".into()),
        };
        let json = serde_json::to_value(&form).expect("serialize");
        assert_eq!(
            json,
            serde_json::json!({
                "form_type": "human-query",
                "schema": { "fields": [{
                    "fieldType": "text_input",
                    "key": "answer",
                    "label": "Answer",
                    "render": { "component": "text" },
                    "config": {}
                }] },
                "title": "Ask",
                "description": "Why"
            })
        );
        let back: Form = serde_json::from_value(json).expect("round-trip");
        assert_eq!(back.title.as_deref(), Some("Ask"));
        assert_eq!(back.schema.fields.len(), 1);
        // `consumer_kind` is serde(skip): it never rides the wire.
        assert_eq!(back.consumer_kind, None);
    }

    #[test]
    fn form_wire_shape_without_optionals() {
        let form = Form {
            form_type: "human-trigger".into(),
            schema: FormSchema { fields: vec![] },
            title: None,
            description: None,
            consumer_kind: None,
        };
        let json = serde_json::to_value(&form).expect("serialize");
        // Absent, never null: the TS `title?`/`description?` fields
        // depend on it.
        assert_eq!(
            json,
            serde_json::json!({
                "form_type": "human-trigger",
                "schema": { "fields": [] }
            })
        );
        let back: Form = serde_json::from_value(json).expect("round-trip");
        assert_eq!(back.title, None);
        assert_eq!(back.description, None);
    }
}

#[cfg(test)]
mod consumer_tests {
    use super::*;
    use serde_json::json;

    fn field(key: &str, value: Value) -> FormField {
        FormField {
            field_type: "display_image".into(),
            key: key.into(),
            label: String::new(),
            render: crate::node::FormFieldRender {
                component: "image".into(),
                source: None,
                multiple: false,
                prefilled: false,
            },
            value: Some(value),
            config: Default::default(),
        }
    }

    #[test]
    fn a_stored_file_leaves_the_form_as_its_facts_and_never_its_key() {
        let stored = json!({ "__weft_image__": { "key": "t/project/p/cat", "mimeType": "image/png", "sizeBytes": 7, "filename": "cat.png" } });
        let remote = json!({ "__weft_image__": { "url": "https://x/cat.png", "mimeType": "image/png", "sizeBytes": 9, "filename": "cat.png" } });
        let schema = FormSchema { fields: vec![field("pic", stored), field("web", remote), field("note", json!("plain"))] };
        let shown = schema.for_consumer().expect("every value reads");
        assert_eq!(shown.fields[0].value, Some(json!({ "mimeType": "image/png", "sizeBytes": 7, "filename": "cat.png" })));
        assert_eq!(shown.fields[1].value, Some(json!({ "url": "https://x/cat.png", "mimeType": "image/png", "sizeBytes": 9, "filename": "cat.png" })));
        assert_eq!(shown.fields[2].value, Some(json!("plain")));
        assert_eq!(
            schema.stored_file("pic").unwrap().map(|f| f.key),
            Some("t/project/p/cat".to_string())
        );
        assert_eq!(schema.stored_file("web").unwrap(), None, "a URL-backed file needs no door");
        assert_eq!(schema.stored_file("note").unwrap(), None);
        assert_eq!(schema.stored_file("nope").unwrap(), None);
    }

    /// A `display` field takes whatever an upstream port produced, so a
    /// file can sit inside a list or an object. The key has to be
    /// stripped there too, or the consumer payload carries it.
    #[test]
    fn a_file_nested_in_a_value_is_projected_like_a_top_level_one() {
        let stored = json!({ "__weft_image__": { "key": "t/project/p/cat", "mimeType": "image/png", "sizeBytes": 7, "filename": "cat.png" } });
        let schema = FormSchema {
            fields: vec![field("gallery", json!({ "best": [stored.clone()], "count": 1 }))],
        };
        let shown = schema.for_consumer().expect("every value reads");
        assert_eq!(
            shown.fields[0].value,
            Some(json!({
                "best": [{ "mimeType": "image/png", "sizeBytes": 7, "filename": "cat.png" }],
                "count": 1
            }))
        );
        assert!(!shown.fields[0].value.as_ref().unwrap().to_string().contains("t/project/p/cat"));
        // A nested file is not the field's own file: the door answers
        // for a field that IS one, not for something inside it.
        assert_eq!(schema.stored_file("gallery").unwrap(), None);
    }

    /// A marker that does not read as a file used to pass through
    /// verbatim, which published the storage key it carries.
    #[test]
    fn a_broken_file_marker_fails_instead_of_passing_its_key_through() {
        let broken = json!({ "__weft_image__": { "key": "t/project/p/cat" } });
        let schema = FormSchema { fields: vec![field("pic", broken)] };
        let err = schema.for_consumer().expect_err("an unreadable file is loud");
        assert!(err.contains("pic"), "{err}");
        assert!(!err.contains("t/project/p/cat"), "the failure must not print the key: {err}");
        assert!(schema.stored_file("pic").is_err());
        // A bare string is data, not a file value: it has no marker.
        let text = FormSchema { fields: vec![field("note", json!("project/p/cat"))] };
        assert_eq!(text.for_consumer().unwrap().fields[0].value, Some(json!("project/p/cat")));
        assert_eq!(text.stored_file("note").unwrap(), None);
    }
}
