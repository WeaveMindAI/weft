//! Timer handler. Each registration spawns one tokio task that
//! sleeps to the next fire, enqueues a `FireSignal` task via the
//! broker, and loops if the spec is recurring.

use std::str::FromStr;
use std::sync::Arc;

use anyhow::Result;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde_json::Value;
use tokio::task::JoinHandle;
use tokio::time::{sleep_until, Duration, Instant};
use weft_core::primitive::{SignalAuth, SignalRouting, SignalSpec, SignalSurface};
use weft_core::signal::{Signal, Timer, TimerSpec};

use crate::config::ListenerConfig;
use crate::fire_sink::FireSignalSink;
use crate::protocol::{ProcessOutcome, ProcessTarget};
use crate::registry::RegisteredSignal;

use super::KindHandler;

pub struct TimerHandler;

impl KindHandler for TimerHandler {
    fn tag(&self) -> &'static str {
        Timer::TAG
    }

    fn compute_routing(
        &self,
        _token: &str,
        _spec: &SignalSpec,
        _secret_cache: &Arc<DashMap<String, String>>,
    ) -> Result<SignalRouting> {
        Ok(SignalRouting {
            surface: SignalSurface::Internal,
            auth: SignalAuth::None,
            auth_config: Value::Null,
        })
    }

    /// Pin the fire time at register time for `After` schedules so a
    /// listener restart doesn't reset the clock. `At` is already
    /// wall-clock-absolute in the spec, and `Cron` recomputes the
    /// next tick from "now" on every iteration (each fire is its
    /// own deadline), so neither needs persisted state.
    fn compute_initial_state(&self, spec: &SignalSpec) -> Result<Value> {
        let timer: Timer = serde_json::from_value(spec.config.clone())
            .map_err(|e| anyhow::anyhow!("malformed timer spec: {e}"))?;
        if let TimerSpec::After { duration_ms } = timer.spec {
            let fire_at = (unix_now_ms() + duration_ms) / 1000;
            return Ok(serde_json::json!({ "next_fire_at_unix": fire_at as i64 }));
        }
        Ok(Value::Object(serde_json::Map::new()))
    }

    fn spawn_task(
        &self,
        token: &str,
        spec: &SignalSpec,
        kind_state: &Value,
        sink: FireSignalSink,
        _config: Arc<ListenerConfig>,
    ) -> Result<Option<JoinHandle<()>>> {
        let timer: Timer = serde_json::from_value(spec.config.clone())
            .map_err(|e| anyhow::anyhow!("malformed timer spec: {e}"))?;
        let pinned_after = kind_state
            .get("next_fire_at_unix")
            .and_then(|v| v.as_i64());
        Ok(Some(spawn_loop(
            token.to_string(),
            timer.spec,
            pinned_after,
            sink,
        )))
    }

    fn process_entry(
        &self,
        _sig: &RegisteredSignal,
        payload: Value,
    ) -> ProcessOutcome {
        ProcessOutcome {
            value: payload,
            target: ProcessTarget::Drop {
                reason: Some("timer is internal-fire only".into()),
            },
        }
    }

    fn render(&self, _token: &str, _sig: &RegisteredSignal) -> Result<Option<Value>> {
        Ok(None)
    }
}

fn spawn_loop(
    token: String,
    spec: TimerSpec,
    pinned_after_unix: Option<i64>,
    sink: FireSignalSink,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        // For After: use the pinned absolute time from kind_state if
        // present (set at register time, survives listener restarts).
        // Only consumed on the first iteration; After is one-shot so
        // there is no "next" after that. For At / Cron the pinned
        // value is ignored.
        let mut pinned = pinned_after_unix;
        loop {
            let Some(next) = next_fire(&spec, pinned.take()) else {
                tracing::warn!(
                    target: "weft_listener::timer",
                    %token,
                    "timer spec has no next fire; task exiting"
                );
                return;
            };
            sleep_until(next).await;

            let now_iso = Utc::now().to_rfc3339();
            let payload = serde_json::json!({
                "scheduledTime": now_iso,
                "actualTime": now_iso,
            });
            if let Err(e) = sink.fire(&token, payload).await {
                tracing::warn!(
                    target: "weft_listener::timer",
                    %token, error = %e,
                    "fire enqueue failed; will retry on next tick if recurring"
                );
            }

            if matches!(spec, TimerSpec::After { .. } | TimerSpec::At { .. }) {
                return;
            }
        }
    })
}

fn next_fire(spec: &TimerSpec, pinned_after_unix: Option<i64>) -> Option<Instant> {
    match spec {
        TimerSpec::After { duration_ms: _ } => {
            // `After` is one-shot and must be pinned at register
            // time so a listener restart preserves the deadline. If
            // the pin is missing, the register flow is broken; fail
            // loudly instead of silently restarting the clock.
            let target_unix = pinned_after_unix
                .expect("After timer must have pinned next_fire_at_unix in kind_state");
            let now_unix = (unix_now_ms() / 1000) as i64;
            let delta = (target_unix - now_unix).max(0) as u64;
            Some(Instant::now() + Duration::from_secs(delta))
        }
        TimerSpec::At { when } => {
            let now = Utc::now();
            let delta = *when - now;
            let ms = delta.num_milliseconds();
            if ms <= 0 {
                None
            } else {
                Some(Instant::now() + Duration::from_millis(ms as u64))
            }
        }
        TimerSpec::Cron { expression } => {
            // The expression is already validated at register time
            // (Signal::validate ran in register_signal task), so
            // from_str cannot fail here. `expect` surfaces the
            // invariant violation if the validator and the parser
            // ever drift apart.
            let schedule = cron::Schedule::from_str(expression)
                .expect("cron expression validated at register time");
            let now: DateTime<Utc> = Utc::now();
            let next_dt = schedule.upcoming(Utc).next()?;
            let delta = next_dt - now;
            let ms = delta.num_milliseconds().max(0) as u64;
            Some(Instant::now() + Duration::from_millis(ms))
        }
    }
}

fn unix_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

inventory::submit!(&TimerHandler as &dyn KindHandler);
