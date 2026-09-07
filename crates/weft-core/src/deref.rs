//! Reading a key off a wire: `w.seconds = x.profile.wpm`.
//!
//! The path lives on the [`Edge`]. At compile time every segment is
//! checked against the record type at that level ([`walk_path`]); at
//! run time the value is projected right before it lands on the target
//! port ([`project_value`]), so a value with several consumers is
//! projected once per wire and each wire is independent. A `?` key
//! that is absent (or `null`) closes the pulse on that wire; the target
//! port's own required/optional rule then decides what the closure
//! means. A required key that is absent is a value that does not match
//! its declared type, and that is a failure, never a quiet null.

use serde_json::Value;

use crate::project::{Edge, ProjectDefinition};
use crate::weft_type::WeftType;

/// Why a path cannot be read off a type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathError {
    /// The type at segment `at` is not a record, so it has no keys to
    /// read. `JsonDict` and scalars land here.
    NotARecord { at: usize, ty: String },
    /// The record at segment `at` has no field `key`.
    NoSuchKey { at: usize, key: String, available: Vec<String> },
}

impl std::fmt::Display for PathError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PathError::NotARecord { ty, .. } => write!(
                f,
                "'{ty}' has no keys to read; declare the type on the source port or Cast first"
            ),
            PathError::NoSuchKey { key, available, .. } => write!(
                f,
                "no key '{key}' here. Available: {}",
                if available.is_empty() { "none".to_string() } else { available.join(", ") }
            ),
        }
    }
}

/// The type a wire carries after reading `path` off a value of `ty`:
/// each segment must name a field of the record at that level
/// (nominal aliases peel), and the result is the last field's type.
/// An empty path is the type itself.
pub fn walk_path(ty: &WeftType, path: &[String]) -> Result<WeftType, PathError> {
    let mut current = ty.clone();
    for (at, key) in path.iter().enumerate() {
        let WeftType::Record(fields) = current.structural() else {
            return Err(PathError::NotARecord { at, ty: current.to_string() });
        };
        let Some(field) = fields.iter().find(|f| &f.name == key) else {
            return Err(PathError::NoSuchKey {
                at,
                key: key.clone(),
                available: fields.iter().map(|f| f.name.clone()).collect(),
            });
        };
        current = field.ty.clone();
    }
    Ok(current)
}

/// What a projection produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Projection {
    /// The value at the end of the path.
    Value(Value),
    /// An optional key along the path was absent (or `null`): the wire
    /// closes. `key` is the one that was missing.
    Closed { key: String },
}

/// Read `path` off `value`, whose declared type is `ty`. The type says
/// which keys may be absent: an absent `?` key closes the wire, an
/// absent required key is a value that broke its declared contract and
/// is an error naming the key.
pub fn project_value(value: &Value, ty: &WeftType, path: &[String]) -> Result<Projection, String> {
    let mut current_value = value;
    let mut current_ty = ty.clone();
    for key in path {
        let WeftType::Record(fields) = current_ty.structural() else {
            return Err(format!("'{current_ty}' has no keys to read (path {})", path.join(".")));
        };
        let Some(field) = fields.iter().find(|f| &f.name == key) else {
            return Err(format!("no key '{key}' in '{current_ty}' (path {})", path.join(".")));
        };
        // Only optional fields use null to mean absence. A required
        // nullable field carries null as an ordinary value.
        let next = current_value.as_object().and_then(|o| o.get(key))
            .filter(|v| !field.optional || !v.is_null());
        match next {
            Some(v) => {
                current_value = v;
                current_ty = field.ty.clone();
            }
            None if field.optional => return Ok(Projection::Closed { key: key.clone() }),
            None => {
                return Err(format!(
                    "the value carries no '{key}', which its type '{current_ty}' requires \
                     (path {})",
                    path.join(".")
                ))
            }
        }
    }
    Ok(Projection::Value(current_value.clone()))
}

/// The declared type of the value a wire delivers: the source port's
/// type with the edge's path read off it. `None` when the source port
/// does not exist; the error when the path does not fit the type.
pub fn wire_source_type(project: &ProjectDefinition, edge: &Edge) -> Option<Result<WeftType, PathError>> {
    let port = project
        .nodes
        .iter()
        .find(|n| n.id == edge.source)?
        .outputs
        .iter()
        .find(|p| Some(p.name.as_str()) == edge.source_handle.as_deref())?;
    Some(walk_path(&port.port_type, &edge.path))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn ty(s: &str) -> WeftType {
        WeftType::parse(s).expect("type parses")
    }

    fn path(keys: &[&str]) -> Vec<String> {
        keys.iter().map(|k| k.to_string()).collect()
    }

    #[test]
    fn a_path_walks_nested_records_to_the_last_field_type() {
        let t = ty("{ profile: { wpm: Number, name?: String }, id: String }");
        assert_eq!(walk_path(&t, &path(&["profile", "wpm"])).unwrap(), ty("Number"));
        assert_eq!(walk_path(&t, &path(&["profile"])).unwrap(), ty("{ wpm: Number, name?: String }"));
        assert_eq!(walk_path(&t, &[]).unwrap(), t);
    }

    #[test]
    fn a_missing_key_names_the_level_and_what_is_there() {
        let t = ty("{ profile: { wpm: Number }, id: String }");
        assert_eq!(
            walk_path(&t, &path(&["profile", "speed"])),
            Err(PathError::NoSuchKey { at: 1, key: "speed".into(), available: vec!["wpm".into()] })
        );
    }

    #[test]
    fn a_dict_or_a_scalar_has_no_keys_to_read() {
        assert_eq!(
            walk_path(&ty("JsonDict"), &path(&["x"])),
            Err(PathError::NotARecord { at: 0, ty: "JsonDict".into() })
        );
        assert_eq!(
            walk_path(&ty("{ n: Number }"), &path(&["n", "x"])),
            Err(PathError::NotARecord { at: 1, ty: "Number".into() })
        );
        assert!(PathError::NotARecord { at: 0, ty: "JsonDict".into() }
            .to_string()
            .contains("declare the type on the source port or Cast first"));
    }

    #[test]
    fn a_nominal_alias_peels_at_every_level() {
        let t = WeftType::parse("Profile={ wpm: Number }").expect("self-contained named type");
        assert_eq!(walk_path(&t, &path(&["wpm"])).unwrap(), ty("Number"));
    }

    #[test]
    fn projection_reads_the_value_at_the_end_of_the_path() {
        let t = ty("{ profile: { wpm: Number } }");
        let v = json!({ "profile": { "wpm": 42 } });
        assert_eq!(project_value(&v, &t, &path(&["profile", "wpm"])).unwrap(), Projection::Value(json!(42)));
        assert_eq!(project_value(&v, &t, &[]).unwrap(), Projection::Value(v.clone()));
    }

    #[test]
    fn an_absent_optional_key_closes_and_names_itself() {
        let t = ty("{ profile: { wpm?: Number } }");
        for v in [json!({ "profile": {} }), json!({ "profile": { "wpm": null } })] {
            assert_eq!(
                project_value(&v, &t, &path(&["profile", "wpm"])).unwrap(),
                Projection::Closed { key: "wpm".into() }
            );
        }
        // Absent higher up: the first absent optional key closes.
        let t = ty("{ profile?: { wpm: Number } }");
        assert_eq!(
            project_value(&json!({}), &t, &path(&["profile", "wpm"])).unwrap(),
            Projection::Closed { key: "profile".into() }
        );
    }

    #[test]
    fn a_present_required_nullable_key_delivers_null() {
        let t = ty("{ profile: { wpm: Number | Null } }");
        let v = json!({ "profile": { "wpm": null } });
        assert_eq!(
            project_value(&v, &t, &path(&["profile", "wpm"])).unwrap(),
            Projection::Value(Value::Null)
        );
        assert!(project_value(&json!({ "profile": {} }), &t, &path(&["profile", "wpm"])).is_err());
    }

    #[test]
    fn an_absent_required_key_is_an_error_not_a_null() {
        let t = ty("{ profile: { wpm: Number } }");
        let err = project_value(&json!({ "profile": {} }), &t, &path(&["profile", "wpm"])).unwrap_err();
        assert!(err.contains("no 'wpm'") && err.contains("requires"), "{err}");
    }
}
