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
    async fn execute(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let from: f64 = ctx.input.get_optional("from")?.unwrap_or(0.0);
        let to: f64 = ctx.input.get("to")?;
        let step: f64 = ctx.input.get_optional("step")?.unwrap_or(1.0);

        let values = compute_range(from, to, step).map_err(WeftError::NodeExecution)?;
        let values: Vec<Value> = values.into_iter().map(num).collect();
        ctx.pulse_downstream(NodeOutput::with("values", Value::Array(values))).await
    }
}

/// Pure range arithmetic: `[from, from+step, ..., to)`. Extracted so
/// the count/boundary logic is unit-testable without an
/// `ExecutionContext`. Errors (as a message string) on non-finite
/// bounds or a zero step; returns an empty vec when `from` already
/// passes `to` in the direction of `step`.
fn compute_range(from: f64, to: f64, step: f64) -> Result<Vec<f64>, String> {
    // Non-finite bounds (NaN / Infinity) silently produce nonsense:
    // NaN comparisons always evaluate false (empty list), Infinity
    // bounds run until f64 saturation or OOM. Reject loudly so the
    // user sees the config bug rather than an empty / hung output.
    if !from.is_finite() || !to.is_finite() || !step.is_finite() {
        return Err(format!(
            "Range: from/to/step must all be finite (got from={from}, to={to}, step={step})"
        ));
    }
    if step == 0.0 {
        return Err("Range: step cannot be zero".to_string());
    }

    let mut values = Vec::new();
    let mut cur = from;
    if step > 0.0 {
        while cur < to {
            values.push(cur);
            cur += step;
        }
    } else {
        while cur > to {
            values.push(cur);
            cur += step;
        }
    }
    Ok(values)
}

fn num(v: f64) -> Value {
    // Callers guard with `is_finite` before reaching here; NaN/inf is
    // the only case `from_f64` rejects, so this is unreachable.
    Value::Number(Number::from_f64(v).expect("Range::num: caller guards is_finite"))
}

#[cfg(test)]
mod tests {
    use super::compute_range;

    #[test]
    fn ascending_default_step() {
        assert_eq!(compute_range(0.0, 5.0, 1.0).unwrap(), vec![0.0, 1.0, 2.0, 3.0, 4.0]);
    }

    #[test]
    fn descending_negative_step() {
        assert_eq!(compute_range(3.0, 0.0, -1.0).unwrap(), vec![3.0, 2.0, 1.0]);
    }

    #[test]
    fn empty_when_from_passes_to() {
        assert!(compute_range(5.0, 0.0, 1.0).unwrap().is_empty(), "ascending step, from > to");
        assert!(compute_range(0.0, 5.0, -1.0).unwrap().is_empty(), "descending step, from < to");
        assert!(compute_range(2.0, 2.0, 1.0).unwrap().is_empty(), "from == to is half-open");
    }

    #[test]
    fn non_unit_step() {
        assert_eq!(compute_range(0.0, 10.0, 2.0).unwrap(), vec![0.0, 2.0, 4.0, 6.0, 8.0]);
    }

    #[test]
    fn zero_step_is_rejected() {
        assert!(compute_range(0.0, 5.0, 0.0).is_err());
    }

    #[test]
    fn non_finite_is_rejected() {
        assert!(compute_range(f64::NAN, 5.0, 1.0).is_err());
        assert!(compute_range(0.0, f64::INFINITY, 1.0).is_err());
        assert!(compute_range(0.0, 5.0, f64::NAN).is_err());
    }
}
