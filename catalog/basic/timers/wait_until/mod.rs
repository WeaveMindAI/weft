//! WaitUntil: park this branch until a date and time, then carry on.
//!
//! The same parked timer as `Wait` (`state.wait` on a timer signal, no
//! worker held while waiting), aimed at a wall-clock moment instead of
//! a duration: a follow-up message planned for tomorrow at nine is this
//! node with `when` wired from wherever the plan came from.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use weft::signal::{Timer, TimerSpec};
use weft::{RunContext, Node, NodeManifest, WeftResult};

use super::timers::output_after;

#[derive(NodeManifest)]
pub struct WaitUntilNode;

/// The timer's wake, once it fired.
#[derive(Default, Serialize, Deserialize)]
struct Parked {
    wake: Option<Value>,
}

#[cfg(feature = "node-tests")]
mod tests;

/// The timer a wait until `when` parks on. The `datetime` widget holds
/// `when` to an RFC 3339 date and time, on a wired value too; the
/// signal's own validation is what refuses a moment already past, at the
/// point the run parks.
pub fn timer_for(when: DateTime<Utc>) -> Timer {
    Timer { spec: TimerSpec::At { when } }
}

#[async_trait]
impl Node for WaitUntilNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let when: DateTime<Utc> = ctx.inputs.get("when")?;
        let value: Option<Value> = ctx.inputs.raw("value").cloned();
        // "Already past" is the signal's rule, checked where the run
        // parks, so it cannot be checked here: a resumed firing runs
        // this body again with the moment now behind it, and a
        // node-side check would fail every completed wait. The signal's
        // own refusal names the moment, which is what `when` holds.
        let mut state = ctx.keep(Parked::default()).await?;
        let wake = state.wait(|s| &mut s.wake, timer_for(when)).await?;
        ctx.pulse_downstream(output_after(value, &wake)?).await
    }
}
