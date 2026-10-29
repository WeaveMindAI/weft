//! Shared by `Switch` and `Dispatch`: when two values count as the same.
//! One comparison, so a value one node matches the other matches too.

use serde_json::Value;

/// Are `a` and `b` the same value? JSON equality separates `5` from
/// `5.0` (integer vs float representation), which an author never
/// means: numbers compare by VALUE, everything else structurally. Two
/// integers compare exactly, never through f64: above 2^53 an f64
/// cannot tell adjacent integers apart, and message-platform ids are
/// exactly that large.
pub fn same(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => match (x.as_i64(), y.as_i64()) {
            (Some(i), Some(j)) => i == j,
            _ => match (x.as_u64(), y.as_u64()) {
                (Some(i), Some(j)) => i == j,
                _ => x.as_f64() == y.as_f64(),
            },
        },
        _ => a == b,
    }
}
