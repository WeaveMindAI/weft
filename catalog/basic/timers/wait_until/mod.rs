//! WaitUntil: park this branch until a date and time, then carry on.
//!
//! The same parked timer as `Wait` (`ctx.await_signal` on a timer
//! signal, no worker held while waiting), aimed at a wall-clock
//! moment instead of a duration: a follow-up message planned for
//! tomorrow at nine is this node with `when` wired from wherever the
//! plan came from.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;

use weft::signal::{Timer, TimerSpec};
use weft::{ExecutionContext, Node, NodeManifest, WeftResult};

use super::timers::output_after;

#[derive(NodeManifest)]
pub struct WaitUntilNode;

#[cfg(feature = "node-tests")]
mod tests;

/// The timer a wait until `when` parks on. The text has to be a full
/// ISO-8601 date and time with a zone; the signal's own validation is
/// what refuses a moment already past, at the point the run parks.
pub fn timer_for(when: &str) -> WeftResult<Timer> {
    let when: DateTime<Utc> = DateTime::parse_from_rfc3339(when.trim())
        .map(|t| t.with_timezone(&Utc))
        .map_err(|e| {
            weft::error::node_error(format!(
                "`when` is not an ISO-8601 date and time ({e}); write it like \
                 `2026-09-03T09:00:00Z` or `2026-09-03T11:00:00+02:00`: got '{when}'"
            ))
        })?;
    Ok(Timer { spec: TimerSpec::At { when } })
}

#[async_trait]
impl Node for WaitUntilNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let when: String = ctx.inputs.get("when")?;
        let value: Option<Value> = ctx.inputs.raw("value").cloned();
        // "Already past" is the signal's rule, checked where the run
        // parks, so it cannot be checked here: a resumed firing replays
        // this body with the moment now behind it, and a node-side
        // check would fail every completed wait. The signal's own
        // refusal names the moment, which is what `when` holds.
        let wake = ctx.await_signal(timer_for(&when)?).await?;
        ctx.pulse_downstream(output_after(value, &wake)?).await
    }
}
