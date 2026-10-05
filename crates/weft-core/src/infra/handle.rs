//! The `Infra` wire value: what an infra node emits to let other nodes
//! reach one of its endpoints.
//!
//! An infra node decides what it shares: it emits a handle on an output
//! port it declares (`ctx.endpoint("api")?.infra_handle()`), and any node
//! wired to that port reaches the same endpoint through
//! `ctx.endpoint_of(&handle)`. The handle names the endpoint, never its
//! address: an address belongs to one deployment of the infra, and the
//! handle has to keep working across a redeploy that moves it.
//!
//! Like the access and stored-file values, the marker is a single-key
//! wrapper object (`{"__weft_infra__": {...}}`) so a plain user dict can
//! never collide with it and the value self-describes on any edge.
//!
//! The handle is not a credential. It resolves only inside the project
//! of the run holding it, only to an infra place that project's program
//! declares, and only to the copy of the run's own instance.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{WeftError, WeftResult};
use crate::instance::InstanceId;

/// The on-wire sentinel key that tags an [`InfraHandle`] value.
// SYNC: INFRA_MARKER_KEY <-> packages/weft-graph/src/protocol.ts INFRA_MARKER_KEY
pub const INFRA_MARKER_KEY: &str = "__weft_infra__";

/// One endpoint of one copy of an infra node, by name.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct InfraHandle {
    inner: InfraHandleInner,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct InfraHandleInner {
    /// The infra node's place in the program, spelled the way infra
    /// places are spelled (`bridge`, or `one.bridge` inside the file the
    /// site `one` includes): the key its infra row is stored under.
    place: String,
    /// The endpoint's name, as the node's `InfraSpec.endpoints` declares it.
    endpoint: String,
    /// The instance whose copy this is, for a node that exists once per
    /// instance; absent for a node with one shared copy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    instance: Option<InstanceId>,
}

impl InfraHandle {
    pub fn new(place: impl Into<String>, endpoint: impl Into<String>, instance: Option<InstanceId>) -> Self {
        Self { inner: InfraHandleInner { place: place.into(), endpoint: endpoint.into(), instance } }
    }

    /// The infra node's place in the program.
    pub fn place(&self) -> &str {
        &self.inner.place
    }

    /// The endpoint's name.
    pub fn endpoint(&self) -> &str {
        &self.inner.endpoint
    }

    /// The instance whose copy this is, or `None` for a shared copy.
    pub fn instance(&self) -> Option<&InstanceId> {
        self.inner.instance.as_ref()
    }

    /// The wire marker: `{"__weft_infra__": {...}}`.
    pub fn to_value(&self) -> Value {
        serde_json::json!({
            INFRA_MARKER_KEY: serde_json::to_value(&self.inner).expect("infra handle always serializes"),
        })
    }

    /// Parse a wire value back. Loud on anything that is not an infra
    /// marker (the usual cause: the input is wired to something other
    /// than an infra node's handle output).
    pub fn from_value(value: &Value) -> WeftResult<Self> {
        let payload = value.as_object().and_then(|o| o.get(INFRA_MARKER_KEY)).ok_or_else(|| {
            WeftError::Input("not an infra handle: wire this input to an infra node's handle output".into())
        })?;
        let inner: InfraHandleInner = serde_json::from_value(payload.clone())
            .map_err(|e| WeftError::Input(format!("malformed infra handle: {e}")))?;
        Ok(Self { inner })
    }
}

impl std::fmt::Display for InfraHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "endpoint '{}' of '{}'", self.inner.endpoint, self.inner.place)?;
        if let Some(instance) = &self.inner.instance {
            write!(f, " for instance '{}'", instance.as_str())?;
        }
        Ok(())
    }
}

impl From<InfraHandle> for Value {
    fn from(handle: InfraHandle) -> Value {
        handle.to_value()
    }
}

impl From<&InfraHandle> for Value {
    fn from(handle: &InfraHandle) -> Value {
        handle.to_value()
    }
}

impl Serialize for InfraHandle {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.to_value().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for InfraHandle {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = Value::deserialize(deserializer)?;
        Self::from_value(&value).map_err(|e| serde::de::Error::custom(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_shared_copys_handle_carries_no_instance_on_the_wire() {
        let handle = InfraHandle::new("one.bridge", "api", None);
        assert_eq!(
            handle.to_value(),
            json!({ "__weft_infra__": { "place": "one.bridge", "endpoint": "api" } })
        );
        assert_eq!(InfraHandle::from_value(&handle.to_value()).unwrap(), handle);
    }

    #[test]
    fn an_instance_copys_handle_names_its_instance() {
        let ada = InstanceId::new("ada").unwrap();
        let handle = InfraHandle::new("bridge", "api", Some(ada.clone()));
        let wire = handle.to_value();
        assert_eq!(wire, json!({ "__weft_infra__": { "place": "bridge", "endpoint": "api", "instance": "ada" } }));
        let back: InfraHandle = serde_json::from_value(wire).unwrap();
        assert_eq!(back.instance(), Some(&ada));
        assert_eq!((back.place(), back.endpoint()), ("bridge", "api"));
    }

    #[test]
    fn a_plain_url_or_a_bad_instance_is_refused() {
        let e = InfraHandle::from_value(&json!("http://bridge:8090")).unwrap_err();
        assert!(e.to_string().contains("wire this input to an infra node"), "{e}");
        let e = InfraHandle::from_value(&json!({ "__weft_infra__": { "place": "b", "endpoint": "api", "instance": "../x" } }))
            .unwrap_err();
        assert!(e.to_string().contains("malformed infra handle"), "{e}");
        let e = InfraHandle::from_value(&json!({ "__weft_infra__": { "place": "b", "endpoint": "api", "url": "x" } }))
            .unwrap_err();
        assert!(e.to_string().contains("malformed infra handle"), "{e}");
    }
}
