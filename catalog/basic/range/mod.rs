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
//!
//! A run can pause while the range waits for its next pull. The state
//! keeps how many numbers went out, so the resumed body walks back to
//! the number it was offering (with the same additions, so a
//! non-integer step lands on the very same float) and carries on from
//! there instead of starting over.

use async_trait::async_trait;
use serde_json::{Number, Value};

use weft::{RunContext, Node, NodeManifest, WeftResult};
use weft::node::NodeOutput;

#[derive(NodeManifest)]
pub struct RangeNode;

/// Where the walk stands: how many numbers it yielded. Only ever
/// advanced after a yield returned, so a pause inside a yield saves the
/// count of the numbers before it, and the resumed body offers that same
/// number again (which sends nothing new: the one on record stands).
#[derive(Default, serde::Serialize, serde::Deserialize)]
struct Walk {
    yielded: u64,
}

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for RangeNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
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
        let mut state = ctx.keep(Walk::default()).await?;
        let mut cur = from;
        for _ in 0..state.yielded {
            cur += step;
        }
        while if step > 0.0 { cur < to } else { cur > to } {
            // The finite guard above is the only case `from_f64` rejects.
            let value = Value::Number(Number::from_f64(cur).expect("Range: guarded is_finite"));
            ctx.yield_downstream(NodeOutput::new().set("values", value), &mut state).await?;
            state.yielded += 1;
            cur += step;
        }
        Ok(())
    }
}
