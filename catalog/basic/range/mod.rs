//! Range: yield the numbers `[from, from+step, ..., to)` one at a
//! time, as a stream. Designed to drive `Loop(over: ["values"])` for
//! count-based loops: the loop pulls one number per iteration, so
//! `Range(to: 10_000_000)` never materializes ten million numbers in
//! memory. Negative step is allowed (descending range). A `step` of
//! zero is rejected loudly (would produce an infinite stream and is
//! almost certainly a config bug). A range whose `from` already passes
//! `to` in the direction of `step` yields nothing. Non-integer steps
//! are supported but accumulate floating-point error per iteration;
//! for deterministic counts, prefer integer steps.

use async_trait::async_trait;
use serde_json::{Number, Value};

use weft::{ExecutionContext, Node, NodeManifest, WeftResult};
use weft::node::NodeOutput;

#[derive(NodeManifest)]
pub struct RangeNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for RangeNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        // `from`/`step` declare metadata defaults, so the bag always
        // holds values; required reads keep each default in ONE place.
        let from: f64 = ctx.inputs.get("from")?;
        let to: f64 = ctx.inputs.get("to")?;
        let step: f64 = ctx.inputs.get("step")?;

        // Non-finite bounds (NaN / Infinity) silently produce nonsense:
        // NaN comparisons always evaluate false (empty stream), Infinity
        // bounds run until f64 saturation. Reject loudly so the user
        // sees the config bug rather than an empty / never-ending output.
        if !from.is_finite() || !to.is_finite() || !step.is_finite() {
            weft::node_bail!(
                "Range: from/to/step must all be finite (got from={from}, to={to}, step={step})"
            );
        }
        if step == 0.0 {
            weft::node_bail!("Range: step cannot be zero");
        }

        // `[from, from+step, ..., to)`: half-open, negative step walks
        // down; a `from` already past `to` yields nothing. Each number
        // is one yield that WAITS for its pull, so the range runs in
        // lock-step with its consumer and never buffers ahead.
        let mut cur = from;
        while if step > 0.0 { cur < to } else { cur > to } {
            // The finite guard above is the only case `from_f64` rejects.
            let value = Value::Number(Number::from_f64(cur).expect("Range: guarded is_finite"));
            ctx.yield_downstream(NodeOutput::new().set("values", value)).await?;
            cur += step;
        }
        Ok(())
    }
}
