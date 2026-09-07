//! What the two timer nodes (`Wait`, `WaitUntil`) share: the output a
//! parked branch emits once its timer fired. The timer each builds is
//! its own; what comes out after is the same.

use serde_json::Value;

use weft::node::NodeOutput;
use weft::signal::Timer;
use weft::WeftResult;

/// What the node emits once the timer fired: the pass-through value
/// (only when one arrived) and the wake time the listener stamped.
pub fn output_after(value: Option<Value>, wake: &Value) -> WeftResult<NodeOutput> {
    let mut out = NodeOutput::new().set("wokeAt", Timer::woke_at(wake)?);
    if let Some(v) = value {
        out = out.set("value", v);
    }
    Ok(out)
}
