//! Cron: fires an execution on a schedule.
//!
//!   - `setup_trigger`: register a Timer signal for the configured
//!     cron expression.
//!
//!   - `run`: the timer listener delivers `{scheduledTime,
//!     actualTime}` as the wake payload. Forward them to outputs.

use async_trait::async_trait;
use serde_json::Value;

use weft::node::NodeOutput;
use weft::signal::{Timer, TimerSpec};
use weft::{ExecutionContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct CronNode;

#[async_trait]
impl Node for CronNode {
    async fn setup_trigger(&self, ctx: ExecutionContext) -> WeftResult<()> {
        // `cron` declares a metadata default, so the bag always holds
        // an expression.
        let expression: String = ctx.inputs.get("cron")?;
        let spec = TimerSpec::Cron { expression };
        // Registers the signal; setup emits nothing downstream.
        ctx.register_signal(Timer { spec }).await
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        // Required reads fail loud on a missing field (a broken listener
        // delivery) rather than substitute `now()`, which would silently
        // mask it as an on-time fire.
        let scheduled: Value = ctx.wake.get("scheduledTime")?;
        let actual: Value = ctx.wake.get("actualTime")?;
        ctx.pulse_downstream(NodeOutput::new()
            .set("scheduledTime", scheduled)
            .set("actualTime", actual)).await
    }
}

