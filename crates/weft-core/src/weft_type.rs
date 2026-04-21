// NOTE: this file is a stub. The full type system implementation lives
// in crates-v1/weft-core/src/weft_type.rs (~1500 lines) and will be
// ported in phase A2 once the scaffold compiles end-to-end. Keeping
// a minimal shape here so downstream types (PortDef) can reference
// WeftType without a circular dep on the port.

use serde::{Deserialize, Serialize};

/// Placeholder. In A2, port from crates-v1/weft-core/src/weft_type.rs
/// (WeftPrimitive, unions, type vars, runtime checks, string
/// serialization).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WeftType(pub String);

impl WeftType {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}
