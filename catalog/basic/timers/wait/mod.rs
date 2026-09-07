//! Wait: park this branch for a number of seconds, then carry on.
//!
//! The pause is a timer signal the run suspends on (`ctx.await_signal`),
//! so a waiting run holds no worker: ten thousand branches sleeping for
//! an hour are ten thousand journal rows. When the listener fires the
//! timer, a worker resumes the firing here and it emits.

use async_trait::async_trait;
use serde_json::Value;

use weft::signal::{Timer, TimerSpec};
use weft::{ExecutionContext, Node, NodeManifest, WeftResult};

use super::timers::output_after;

#[derive(NodeManifest)]
pub struct WaitNode;

#[cfg(feature = "node-tests")]
mod tests;

/// The timer a wait of `seconds` parks on. Pure, so the refusal of a
/// zero or negative wait is testable without a rig. How FAR ahead a
/// timer can point is the signal's own rule (`Timer::validate`), which
/// runs before the signal is registered.
pub fn timer_for(seconds: f64) -> WeftResult<Timer> {
    if !(seconds > 0.0) || !seconds.is_finite() {
        weft::node_bail!("`seconds` must be more than zero; got {seconds}");
    }
    let timer =
        Timer { spec: TimerSpec::After { duration_ms: (seconds * 1000.0).ceil() as u64 } };
    if let Err(e) = weft::signal::Signal::validate(&timer) {
        weft::node_bail!("`seconds` is {seconds}, which no timer can hold: {e}");
    }
    Ok(timer)
}

#[async_trait]
impl Node for WaitNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let seconds: f64 = ctx.inputs.get("seconds")?;
        let value: Option<Value> = ctx.inputs.raw("value").cloned();
        let wake = ctx.await_signal(timer_for(seconds)?).await?;
        ctx.pulse_downstream(output_after(value, &wake)?).await
    }
}
