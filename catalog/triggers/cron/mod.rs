//! Cron: fires an execution on a schedule.
//!
//!   - `setup_trigger`: resolve the timer spec from config
//!     (`cron` / `after_ms` / `at`) and register a Timer signal.
//!
//!   - `run`: the timer listener delivers `{scheduledTime,
//!     actualTime}` as the wake payload. Forward them to outputs.

use async_trait::async_trait;
use serde_json::Value;

use weft_core::node::NodeOutput;
use weft_core::signal::{Timer, TimerSpec};
use weft_core::{ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct CronNode;

#[async_trait]
impl Node for CronNode {
    async fn setup_trigger(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let spec = if let Some(expression) = ctx.config.opt::<String>("cron")? {
            TimerSpec::Cron { expression }
        } else if let Some(duration_ms) = ctx.config.opt::<u64>("after_ms")? {
            TimerSpec::After { duration_ms }
        } else if let Some(s) = ctx.config.opt::<String>("at")? {
            let when = chrono::DateTime::parse_from_rfc3339(&s)
                .node_err("config.at not RFC-3339")?
                .with_timezone(&chrono::Utc);
            TimerSpec::At { when }
        } else {
            weft_core::node_bail!("Cron requires one of config.cron / config.after_ms / config.at");
        };
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

