//! The `Access` wire value: the ONE reference an access node emits and
//! action nodes consume.
//!
//! An ACCESS NODE's `access` widget stores the small `{id, identity}`
//! handle of a connection; the runtime builds the full marker (service
//! stamped from the widget) when it assembles the node's input bag, and
//! the marker flows downstream like any value. Like the stored-file
//! values, the marker is a single-key wrapper object
//! (`{"__weft_access__": {...}}`) so a plain user dict can never
//! collide with it and the value self-describes on any edge.
//!
//! The reference is not a secret: everything secret lives in the store,
//! behind the tenant wall, and a worker resolves the reference per
//! firing. Whose credential it resolves to (the user's own, or one the
//! runtime supplies) is a fact of the STORED connection, never of this
//! value; node code cannot tell and never needs to.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{WeftError, WeftResult};

/// The on-wire sentinel key that tags an [`Access`] value.
// SYNC: ACCESS_MARKER_KEY <-> packages/weft-graph/src/protocol.ts ACCESS_MARKER_KEY
pub const ACCESS_MARKER_KEY: &str = "__weft_access__";

/// Authorized ability to call a third party: a reference to a stored
/// connection. Carries the connection row's id, the service name
/// (stamped from the access widget's compiler-resolved `service`), a
/// display identity, and (stamped per CONSUMER when the bag is built)
/// the permissions that consumer's input declared it needs, so run-time
/// resolution can hold a verified connection to them.
#[derive(Clone, PartialEq)]
pub struct Access {
    inner: AccessInner,
}

// The marker payload. camelCase field names on the wire, like the
// stored-file payloads.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AccessInner {
    #[serde(rename = "accessId")]
    access_id: String,
    service: String,
    /// Display identity ("Quentin @ Acme"); cosmetic only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    identity: Option<String>,
    /// The permissions the CONSUMING input declared it needs
    /// (`requiresScopes`), stamped onto the marker when the consumer's
    /// bag is built. Rides to resolution as the drift backstop: a
    /// VERIFIED connection short of one of these is refused there.
    #[serde(
        default,
        rename = "requiresPermissions",
        skip_serializing_if = "Vec::is_empty"
    )]
    requires_permissions: Vec<String>,
    /// The stored values the CONSUMING input declared it needs
    /// (`requiresValues`), stamped the same way and refused the same
    /// way at resolution.
    #[serde(default, rename = "requiresValues", skip_serializing_if = "Vec::is_empty")]
    requires_values: Vec<String>,
}

impl Access {
    /// A reference to the stored connection `access_id` at `service`.
    pub fn new(
        access_id: impl Into<String>,
        service: impl Into<String>,
        identity: Option<String>,
    ) -> Self {
        Self {
            inner: AccessInner {
                access_id: access_id.into(),
                service: service.into(),
                identity,
                requires_permissions: Vec::new(),
                requires_values: Vec::new(),
            },
        }
    }

    /// Stamp the consuming input's required permissions onto the
    /// marker (done by the runtime when the consumer's bag is built,
    /// never by node code).
    pub fn with_required_permissions(mut self, required: Vec<String>) -> Self {
        self.inner.requires_permissions = required;
        self
    }

    /// The permissions the consuming input declared it needs.
    pub fn required_permissions(&self) -> &[String] {
        &self.inner.requires_permissions
    }

    /// Stamp the consuming input's required stored values onto the
    /// marker (runtime only, like the permissions above).
    pub fn with_required_values(mut self, required: Vec<String>) -> Self {
        self.inner.requires_values = required;
        self
    }

    /// The stored values the consuming input declared it needs.
    pub fn required_values(&self) -> &[String] {
        &self.inner.requires_values
    }

    /// The stored connection's id.
    pub fn access_id(&self) -> &str {
        &self.inner.access_id
    }

    pub fn service(&self) -> &str {
        &self.inner.service
    }

    /// Display identity, if the connection recorded one.
    pub fn identity(&self) -> Option<&str> {
        self.inner.identity.as_deref()
    }

    /// The wire marker: `{"__weft_access__": {...}}`.
    pub fn to_value(&self) -> Value {
        serde_json::json!({
            ACCESS_MARKER_KEY: serde_json::to_value(&self.inner)
                .expect("access payload always serializes"),
        })
    }

    /// Parse a wire value back. Loud on anything that is not an access
    /// marker (the usual cause: the input is wired to something other
    /// than an access node's access output).
    pub fn from_value(value: &Value) -> WeftResult<Self> {
        let payload = value
            .as_object()
            .and_then(|o| o.get(ACCESS_MARKER_KEY))
            .ok_or_else(|| {
                WeftError::Input(
                    "not an access value: wire this input to an access node's output".into(),
                )
            })?;
        let inner: AccessInner = serde_json::from_value(payload.clone())
            .map_err(|e| WeftError::Input(format!("malformed access value: {e}")))?;
        Ok(Self { inner })
    }
}

impl std::fmt::Debug for Access {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Access")
            .field("service", &self.inner.service)
            .field("accessId", &self.inner.access_id)
            .field("identity", &self.inner.identity)
            .finish()
    }
}

impl From<Access> for Value {
    fn from(access: Access) -> Value {
        access.to_value()
    }
}

impl Serialize for Access {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.to_value().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Access {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = Value::deserialize(deserializer)?;
        Self::from_value(&value).map_err(|e| serde::de::Error::custom(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn access_marker_round_trips() {
        let access =
            Access::new("11111111-2222-3333-4444-555555555555", "slack", Some("Q @ Acme".into()));
        let v = access.to_value();
        assert_eq!(v[ACCESS_MARKER_KEY]["accessId"], "11111111-2222-3333-4444-555555555555");
        assert_eq!(v[ACCESS_MARKER_KEY]["service"], "slack");
        let back = Access::from_value(&v).unwrap();
        assert_eq!(back, access);
        assert_eq!(back.identity(), Some("Q @ Acme"));
        assert_eq!(back.access_id(), "11111111-2222-3333-4444-555555555555");
        assert_eq!(back.service(), "slack");
    }

    #[test]
    fn required_permissions_ride_the_marker() {
        let access = Access::new("id-1", "google", None)
            .with_required_permissions(vec!["drive.readonly".into()]);
        let v = access.to_value();
        assert_eq!(v[ACCESS_MARKER_KEY]["requiresPermissions"][0], "drive.readonly");
        let back = Access::from_value(&v).unwrap();
        assert_eq!(back.required_permissions(), ["drive.readonly".to_string()]);

        // Empty stays off the wire.
        let bare = Access::new("id-1", "google", None);
        assert!(bare.to_value()[ACCESS_MARKER_KEY].get("requiresPermissions").is_none());
    }

    #[test]
    fn a_non_marker_value_fails_loud() {
        let e = Access::from_value(&serde_json::json!({"accessId": "x"})).unwrap_err();
        assert!(e.to_string().contains("wire this input to an access node"), "{e}");
    }
}
