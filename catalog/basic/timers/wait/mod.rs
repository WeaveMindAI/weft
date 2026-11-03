//! Wait: park this branch for a number of seconds, then carry on.
//!
//! The pause is a timer signal the run suspends on (`state.wait`), so a
//! waiting run holds no worker: ten thousand branches sleeping for an
//! hour are ten thousand journal rows. When the listener fires the
//! timer, a worker runs the body again, the wait hands the wake over,
//! and it emits.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use weft::signal::{Timer, TimerSpec};
use weft::{RunContext, Node, NodeManifest, WeftResult};

use super::timers::output_after;

#[derive(NodeManifest)]
pub struct WaitNode;

/// The timer's wake, once it fired.
#[derive(Default, Serialize, Deserialize)]
struct Parked {
    wake: Option<Value>,
}

#[cfg(feature = "node-tests")]
mod tests;

/// The timer a wait of `seconds` parks on, to the millisecond above it.
/// A wait of zero or less never gets here (the metadata refuses it, on a
/// wired value too), and how FAR ahead a timer can point is the signal's
/// own rule, checked where the run parks.
pub fn timer_for(seconds: f64) -> Timer {
    Timer { spec: TimerSpec::After { duration_ms: (seconds * 1000.0).ceil() as u64 } }
}

#[async_trait]
impl Node for WaitNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let seconds: f64 = ctx.inputs.get("seconds")?;
        let value: Option<Value> = ctx.inputs.raw("value").cloned();
        let mut state = ctx.keep(Parked::default()).await?;
        let wake = state.wait(|s| &mut s.wake, timer_for(seconds)).await?;
        ctx.pulse_downstream(output_after(value, &wake)?).await
    }
}
