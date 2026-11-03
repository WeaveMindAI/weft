//! Cron: fires a run on a schedule.
//!
//!   - `setup_trigger`: register a Timer signal for the configured
//!     cron expression.
//!
//!   - `run`: the timer listener delivers `{scheduledTime,
//!     actualTime}` as the wake payload, fanned onto the outputs.

use async_trait::async_trait;

use weft::signal::{Timer, TimerSpec};
use weft::{RunContext, Node, NodeManifest, WeftResult};

#[cfg(feature = "node-tests")]
mod tests;

#[derive(NodeManifest)]
pub struct CronNode;

#[async_trait]
impl Node for CronNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn setup_trigger(&self, ctx: RunContext) -> WeftResult<()> {
        // `cron` is required (no schedule is right for everybody) and
        // `timezone` declares a default, so the bag always holds both.
        let expression: String = ctx.inputs.get("cron")?;
        let timezone: String = ctx.inputs.get("timezone")?;
        let spec = TimerSpec::Cron { expression, timezone };
        // Registers the signal; setup emits nothing downstream.
        ctx.register_signal(Timer { spec }).await
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        // The trigger declares both fields (`firesWith`), so a delivery
        // missing one is refused before this body runs.
        ctx.pulse_downstream(ctx.fan_declared(&ctx.wake.record()?)).await
    }
}

