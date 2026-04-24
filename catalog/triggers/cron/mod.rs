//! Cron: fires an execution on a schedule. Two phases:
//!
//!   - `Phase::TriggerSetup`: resolve the timer spec from config
//!     (`cron` / `after_ms` / `at`) and register with the listener.
//!
//!   - `Phase::Fire`: the listener's timer tick seeded `__seed__`
//!     with `{scheduledTime, actualTime}`. Forward them to outputs.

use async_trait::async_trait;
use serde_json::Value;

use weft_core::context::Phase;
use weft_core::node::NodeOutput;
use weft_core::primitive::{TimerSpec, WakeSignalKind, WakeSignalSpec};
use weft_core::{ExecutionContext, Node, NodeMetadata, WeftResult};

pub struct CronNode;

const METADATA_JSON: &str = include_str!("metadata.json");

#[async_trait]
impl Node for CronNode {
    fn node_type(&self) -> &'static str {
        "Cron"
    }

    fn metadata(&self) -> NodeMetadata {
        serde_json::from_str(METADATA_JSON).expect("Cron metadata.json must be valid")
    }

    async fn execute(&self, ctx: ExecutionContext) -> WeftResult<NodeOutput> {
        match ctx.phase {
            Phase::TriggerSetup => {
                let spec = cron_spec_from_config(&ctx)?;
                ctx.register_signal(WakeSignalSpec {
                    kind: WakeSignalKind::Timer { spec },
                    is_resume: false,
                })
                .await?;
                Ok(NodeOutput::empty())
            }
            Phase::Fire => {
                let payload = ctx
                    .input
                    .values
                    .get("__seed__")
                    .cloned()
                    .unwrap_or(Value::Null);
                let now = chrono::Utc::now().to_rfc3339();
                let scheduled = payload
                    .get("scheduledTime")
                    .cloned()
                    .unwrap_or_else(|| Value::String(now.clone()));
                let actual = payload
                    .get("actualTime")
                    .cloned()
                    .unwrap_or(Value::String(now));
                Ok(NodeOutput::empty()
                    .set("scheduledTime", scheduled)
                    .set("actualTime", actual))
            }
            Phase::InfraSetup => Ok(NodeOutput::empty()),
        }
    }
}

fn cron_spec_from_config(ctx: &ExecutionContext) -> WeftResult<TimerSpec> {
    use weft_core::error::WeftError;
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
