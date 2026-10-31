//! `Secret[T]`: a value that flows through a run like any other while the
//! run's record holds only a marker naming it.
//!
//! A node makes one explicitly (`ctx.secret(value)`) and gets the marker
//! back (`{"__weft_secret__": {"secretId": "<id>"}}`), which is what flows
//! on its wires and what every event of the run's record holds. So
//! everything that shows a run's values later (the inspector, `weft
//! events`, the values route, search) shows the marker and never the
//! value, with no reader of the record knowing secrets exist. The value
//! itself is kept sealed beside the run's record, written with the run's
//! own writes, and lives exactly as long as that record. A node that
//! needs it asks for it explicitly (`ctx.reveal(&secret)`): the run's
//! worker holds what the run made or revealed in memory, and reads any
//! other back through the broker, which opens the sealed row for a run of
//! the same project.
//!
//! A program never writes a secret: the compiler refuses a marker written
//! as a value, and a Cast into a `Secret[T]`, so only a node makes one.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The on-wire sentinel key that tags a [`Secret`].
// SYNC: SECRET_MARKER_KEY <-> packages/weft-graph/src/protocol.ts SECRET_MARKER_KEY
pub const SECRET_MARKER_KEY: &str = "__weft_secret__";

/// What names one secret: minted by the run that makes it, and the key
/// its sealed value is kept under. Read only in the hyphenated form it is
/// written in, so a marker is one spelling everywhere it is recognized.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
#[serde(transparent)]
pub struct SecretId(uuid::Uuid);

impl<'de> Deserialize<'de> for SecretId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        let id = uuid::Uuid::try_parse(&text).map_err(serde::de::Error::custom)?;
        if id.hyphenated().to_string() != text.to_ascii_lowercase() {
            return Err(serde::de::Error::custom(format!("a secret id is a hyphenated uuid, got '{text}'")));
        }
        Ok(Self(id))
    }
}

impl SecretId {
    /// A fresh id, random: nothing about it says which run made it or
    /// when.
    #[cfg(feature = "runtime")]
    pub fn mint() -> Self {
        Self(uuid::Uuid::new_v4())
    }

    pub fn from_uuid(id: uuid::Uuid) -> Self {
        Self(id)
    }

    pub fn as_uuid(&self) -> uuid::Uuid {
        self.0
    }
}

impl std::fmt::Display for SecretId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// A secret as a value carries it: the marker naming it, never the value.
/// Serializes as the marker, and reads only a well-formed one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Secret {
    id: SecretId,
}

/// The marker's payload.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Payload {
    secret_id: SecretId,
}

impl Secret {
    pub fn new(id: SecretId) -> Self {
        Self { id }
    }

    pub fn id(&self) -> SecretId {
        self.id
    }

    /// The marker.
    pub fn to_value(&self) -> Value {
        serde_json::json!({ SECRET_MARKER_KEY: { "secretId": self.id } })
    }

    /// The secret `value` is the marker of: an object holding the sentinel
    /// key alone, whose payload names a secret id and nothing else. `None`
    /// for anything else, a malformed marker included.
    // SYNC: Secret::from_value <-> packages/weft-graph/src/protocol.ts isSecretMarker
    pub fn from_value(value: &Value) -> Option<Self> {
        let object = value.as_object().filter(|object| object.len() == 1)?;
        let payload = object.get(SECRET_MARKER_KEY)?;
        let Payload { secret_id } = Payload::deserialize(payload).ok()?;
        Some(Self { id: secret_id })
    }

    /// Whether anything in `value`, at any depth, claims to be a secret
    /// (an object carrying the sentinel key, well-formed or not): what a
    /// value from outside a run (a program's literal, what a caller sent)
    /// never holds, since only a node makes a secret.
    pub fn claimed_in(value: &Value) -> bool {
        match value {
            Value::Object(fields) => fields.contains_key(SECRET_MARKER_KEY) || fields.values().any(Self::claimed_in),
            Value::Array(items) => items.iter().any(Self::claimed_in),
            _ => false,
        }
    }
}

impl From<Secret> for Value {
    fn from(secret: Secret) -> Self {
        secret.to_value()
    }
}

/// Every secret's marker in `value`, at any depth, as a JSON pointer to it
/// and the secret it names: what a node sends a caller, whose secrets
/// the caller receives revealed while the run's record keeps the markers.
pub fn markers_in(value: &Value) -> Vec<(String, Secret)> {
    fn walk(value: &Value, at: &mut String, out: &mut Vec<(String, Secret)>) {
        if let Some(secret) = Secret::from_value(value) {
            out.push((at.clone(), secret));
            return;
        }
        let step = |at: &mut String, key: &str, inner: &Value, out: &mut Vec<(String, Secret)>| {
            let len = at.len();
            push_pointer_key(at, key);
            walk(inner, at, out);
            at.truncate(len);
        };
        match value {
            Value::Array(items) => items.iter().enumerate().for_each(|(i, item)| step(at, &i.to_string(), item, out)),
            Value::Object(fields) => fields.iter().for_each(|(key, item)| step(at, key, item, out)),
            _ => {}
        }
    }
    let mut out = Vec::new();
    walk(value, &mut String::new(), &mut out);
    out
}

/// `key` as the next step of the JSON pointer `at` (RFC 6901: `~` and `/`
/// escaped), the form [`markers_in`] and `WeftType::secret_places` name a
/// place by and `Value::pointer_mut` reads.
pub(crate) fn push_pointer_key(at: &mut String, key: &str) {
    at.push('/');
    at.push_str(&key.replace('~', "~0").replace('/', "~1"));
}

impl Serialize for Secret {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.to_value().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Secret {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = Value::deserialize(deserializer)?;
        Self::from_value(&value).ok_or_else(|| {
            serde::de::Error::custom(format!(
                "not a secret: a secret is the marker {{\"{SECRET_MARKER_KEY}\": {{\"secretId\": \"<id>\"}}}} a node made with ctx.secret"
            ))
        })
    }
}

/// What a node is told when a value it is handed is not the secret its
/// port's type says: made by `ctx.secret` and nowhere else.
pub const MADE_BY_A_NODE: &str =
    "a Secret is made by a node at run time (ctx.secret), so a program cannot write one: wire it from a node's Secret output";

#[cfg(test)]
mod tests {
    use super::*;

    /// The marker is the whole wire form: it reads back as the same
    /// secret, through serde and through `from_value`.
    #[test]
    fn a_secret_round_trips_as_its_marker() {
        let secret = Secret::new(SecretId::mint());
        let value = serde_json::to_value(secret).unwrap();
        assert_eq!(value, secret.to_value());
        assert_eq!(value[SECRET_MARKER_KEY]["secretId"], serde_json::json!(secret.id().to_string()));
        assert_eq!(serde_json::from_value::<Secret>(value.clone()).unwrap(), secret);
        assert_eq!(Secret::from_value(&value), Some(secret));
    }

    /// Only the exact marker reads as a secret: a payload with anything
    /// else in it, an id that is no id, or the sentinel beside other keys
    /// is refused, so a plain dict can never pass for one by accident.
    #[test]
    fn anything_but_the_exact_marker_is_no_secret() {
        let id = SecretId::mint();
        for value in [
            serde_json::json!({ SECRET_MARKER_KEY: { "secretId": id, "more": 1 } }),
            serde_json::json!({ SECRET_MARKER_KEY: { "secretId": "not an id" } }),
            serde_json::json!({ SECRET_MARKER_KEY: { "secretId": id.as_uuid().simple().to_string() } }),
            serde_json::json!({ SECRET_MARKER_KEY: { "secretId": id }, "beside": 1 }),
            serde_json::json!({ SECRET_MARKER_KEY: id }),
            serde_json::json!("plain text"),
        ] {
            assert_eq!(Secret::from_value(&value), None, "{value}");
            assert!(serde_json::from_value::<Secret>(value).is_err());
        }
    }

    /// Every marker is found where it sits, a key needing escape included,
    /// and a malformed one is no marker.
    #[test]
    fn markers_are_found_at_any_depth() {
        let (a, b) = (Secret::new(SecretId::mint()), Secret::new(SecretId::mint()));
        let value = serde_json::json!({ "x/y": [1, a], "b": b, "bad": { SECRET_MARKER_KEY: "x" } });
        let mut found = markers_in(&value);
        found.sort_by(|l, r| l.0.cmp(&r.0));
        assert_eq!(found, vec![("/b".to_string(), b), ("/x~1y/1".to_string(), a)]);
        assert_eq!(markers_in(&a.to_value()), vec![(String::new(), a)]);
    }

    /// A claim at any depth counts, malformed or not: what a value from
    /// outside a run is refused on.
    #[test]
    fn a_claim_is_found_at_any_depth() {
        assert!(Secret::claimed_in(&serde_json::json!({ "a": [{ SECRET_MARKER_KEY: "x" }] })));
        assert!(Secret::claimed_in(&Secret::new(SecretId::mint()).to_value()));
        assert!(!Secret::claimed_in(&serde_json::json!({ "a": [{ "secretId": "x" }] })));
    }
}
