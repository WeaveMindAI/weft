//! Cron: fires an execution on a schedule. Two phases:
//!
//!   - `Phase::TriggerSetup`: resolve the timer spec from config
//!     (`cron` / `after_ms` / `at`) and register a Timer signal.
//!
//!   - `Phase::Fire`: the timer listener delivers `{scheduledTime,
//!     actualTime}` as the wake payload. Forward them to outputs.

use async_trait::async_trait;

use weft_core::context::Phase;
use weft_core::error::WeftError;
use weft_core::node::NodeOutput;
use weft_core::signal::{Timer, TimerSpec};
use weft_core::{ExecutionContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct CronNode;

#[async_trait]
impl Node for CronNode {
    async fn execute(&self, ctx: ExecutionContext) -> WeftResult<()> {
        match ctx.phase {
            Phase::TriggerSetup => {
                let spec = cron_spec_from_config(&ctx)?;
                ctx.register_signal(Timer { spec }).await?;
                // Setup registers the signal; it emits nothing downstream.
                Ok(())
            }
            Phase::Fire => {
                // The timer listener delivers `{scheduledTime,
                // actualTime}` as the wake payload. Missing either
                // means the listener contract broke; fail loud rather
                // than substitute `now()`, which would silently mask
                // a broken delivery as an on-time fire.
                let payload = ctx.wake_payload().ok_or_else(|| {
                    WeftError::NodeExecution(
                        "Cron Fire: timer listener delivered no wake payload".into(),
                    )
                })?;
                let scheduled = payload.get("scheduledTime").cloned().ok_or_else(|| {
                    WeftError::NodeExecution(
                        "Cron Fire: timer payload missing `scheduledTime`".into(),
                    )
                })?;
                let actual = payload.get("actualTime").cloned().ok_or_else(|| {
                    WeftError::NodeExecution(
                        "Cron Fire: timer payload missing `actualTime`".into(),
                    )
                })?;
                ctx.pulse_downstream(NodeOutput::empty()
                    .set("scheduledTime", scheduled)
                    .set("actualTime", actual)).await
            }
            Phase::InfraSetup => Ok(()),
        }
    }
}

fn cron_spec_from_config(ctx: &ExecutionContext) -> WeftResult<TimerSpec> {
    let cfg = &ctx.config.values;
    if let Some(expr) = cfg.get("cron").and_then(|v| v.as_str()) {
        return Ok(TimerSpec::Cron {
            expression: expr.to_string(),
        });
    }
    if let Some(ms) = cfg.get("after_ms").and_then(|v| v.as_u64()) {
        return Ok(TimerSpec::After { duration_ms: ms });
    }
    if let Some(s) = cfg.get("at").and_then(|v| v.as_str()) {
        let when = chrono::DateTime::parse_from_rfc3339(s)
            .map_err(|e| WeftError::Config(format!("config.at not RFC-3339: {e}")))?
            .with_timezone(&chrono::Utc);
        return Ok(TimerSpec::At { when });
    }
    Err(WeftError::Config(
        "Cron requires one of config.cron / config.after_ms / config.at".into(),
    ))
}
