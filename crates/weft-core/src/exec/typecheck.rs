//! Runtime type check. The compiler's static pass catches most
//! mismatches; this helper runs at preprocess/postprocess time to
//! catch cases where values diverge from declared port types at
//! runtime (e.g., an LLM producing unexpected JSON).

use serde_json::Value;

use crate::weft_type::WeftType;

/// Check a value against a declared port type. Unresolved types
/// (TypeVar, MustOverride) always pass because the compiler has
/// already verified they're resolved before dispatch.
pub fn runtime_type_check(port_type: &WeftType, value: &Value) -> bool {
    if port_type.is_unresolved() {
        return true;
    }
    let inferred = WeftType::infer(value);
    WeftType::is_compatible(&inferred, port_type)
}
