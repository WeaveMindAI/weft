//! Range: produce a list of numbers `[from, from+step, ..., to)`.
//! Designed to drive `Loop(over: ["values"])` for count-based loops.
//! Negative step is allowed (descending range). A `step` of zero is
//! rejected loudly (would produce an infinite loop and is almost
//! certainly a config bug). A range whose `from` already passes `to`
//! in the direction of `step` produces an empty list. Non-integer
//! steps are supported but accumulate floating-point error per
//! iteration; for deterministic counts, prefer integer steps or
//! compute the iteration count up front.

use async_trait::async_trait;
use serde_json::{Number, Value};

use weft_core::{ExecutionContext, Node, NodeManifest, WeftResult, WeftError};
use weft_core::node::NodeOutput;

#[derive(NodeManifest)]
pub struct RangeNode;

#[async_trait]
impl Node for RangeNode {
    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let from: f64 = ctx.ports.get_or("from", 0.0)?;
        let to: f64 = ctx.ports.get("to")?;
        let step: f64 = ctx.ports.get_or("step", 1.0)?;

        // Non-finite bounds (NaN / Infinity) silently produce nonsense:
        // NaN comparisons always evaluate false (empty list), Infinity
        // bounds run until f64 saturation or OOM. Reject loudly so the
        // user sees the config bug rather than an empty / hung output.
        if !from.is_finite() || !to.is_finite() || !step.is_finite() {
            return Err(WeftError::NodeExecution(format!(
                "Range: from/to/step must all be finite (got from={from}, to={to}, step={step})"
            )));
        }
        if step == 0.0 {
            return Err(WeftError::NodeExecution("Range: step cannot be zero".to_string()));
        }

        // `[from, from+step, ..., to)`: half-open, negative step walks
        // down; a `from` already past `to` yields an empty list.
        let mut values: Vec<Value> = Vec::new();
        let mut cur = from;
        while if step > 0.0 { cur < to } else { cur > to } {
            // The finite guard above is the only case `from_f64` rejects.
            values.push(Value::Number(Number::from_f64(cur).expect("Range: guarded is_finite")));
            cur += step;
        }
        ctx.pulse_downstream(NodeOutput::new().set("values", Value::Array(values))).await
    }
}
