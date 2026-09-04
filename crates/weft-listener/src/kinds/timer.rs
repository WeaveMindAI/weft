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

use crate::protocol::{ProcessOutcome, ProcessTarget};
use crate::registry::RegisteredSignal;

use async_trait::async_trait;

use super::{KindHandler, SpawnCtx};

pub struct TimerHandler;

#[async_trait]
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
    // `_prior` is deliberately ignored: reactivate IS a fresh schedule
    // (an After-timer restarts its countdown from the activation).
    fn compute_initial_state(&self, spec: &SignalSpec, _prior: Option<&Value>) -> Result<Value> {
        let timer: Timer = serde_json::from_value(spec.config.clone())
            .map_err(|e| anyhow::anyhow!("malformed timer spec: {e}"))?;
        if let TimerSpec::After { duration_ms } = timer.spec {
            // Pin in MILLISECONDS (no /1000 truncation): a sub-second
            // `After` (duration_ms < 1000, which validation allows)
            // must not floor to 0 and fire immediately. At/Cron are
            // already ms-precise; After matches.
            let fire_at_ms = unix_now_ms() + duration_ms;
            return Ok(serde_json::json!({ "next_fire_at_unix_ms": fire_at_ms as i64 }));
        }
        Ok(Value::Object(serde_json::Map::new()))
    }

    async fn spawn_task(
        &self,
        spec: &SignalSpec,
        kind_state: &Value,
        ctx: SpawnCtx,
    ) -> Result<Option<JoinHandle<()>>> {
        let timer: Timer = serde_json::from_value(spec.config.clone())
            .map_err(|e| anyhow::anyhow!("malformed timer spec: {e}"))?;
        let pinned_after = kind_state
            .get("next_fire_at_unix_ms")
            .and_then(|v| v.as_i64());
        Ok(Some(spawn_loop(timer.spec, pinned_after, ctx.fire)))
    }

    fn process_entry(
        &self,
        _sig: &RegisteredSignal,
        payload: Value,
    ) -> ProcessOutcome {
        // A timer tick (raised internally by the tick loop, delivered
        // through the FireSignal broker task) routes to the entry
        // trigger.
        ProcessOutcome {
            value: payload,
            target: ProcessTarget::Entry,
        }
    }

    fn render(&self, _token: &str, _sig: &RegisteredSignal) -> Result<Option<Value>> {
        Ok(None)
    }
}

fn spawn_loop(
    spec: TimerSpec,
    pinned_after_unix: Option<i64>,
    fire: crate::event_context::FireContext,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        // For After: use the pinned absolute time from kind_state if
        // present (set at register time, survives listener restarts).
        // Only consumed on the first iteration; After is one-shot so
        // there is no "next" after that. For At / Cron the pinned
        // value is ignored.
        let mut pinned = pinned_after_unix;
        loop {
            let Some((next, deadline)) = next_fire(&spec, pinned.take()) else {
                tracing::warn!(
                    target: "weft_listener::timer",
                    token = %fire.token(),
                    "timer spec has no next fire; task exiting"
                );
                return;
            };
            sleep_until(next).await;

            // scheduledTime = the intended deadline; actualTime = when
            // we actually woke. They differ when the wakeup is late
            // (the whole point of exposing both, per the cron node's
            // metadata).
            let payload = serde_json::json!({
                "scheduledTime": deadline.to_rfc3339(),
                "actualTime": Utc::now().to_rfc3339(),
            });
            // A timer tick has no replay cursor; the delivery outcome
            // is already logged by the fire path.
            let _ = fire.fire(payload, "timer").await;

            if matches!(spec, TimerSpec::After { .. } | TimerSpec::At { .. }) {
                return;
            }
        }
    })
}

/// The next fire as both the monotonic `Instant` to sleep on AND the
/// intended wall-clock deadline (reported as `scheduledTime` so a
/// late wakeup is distinguishable from an on-time one).
fn next_fire(spec: &TimerSpec, pinned_after_unix_ms: Option<i64>) -> Option<(Instant, DateTime<Utc>)> {
    match spec {
        TimerSpec::After { duration_ms: _ } => {
            // `After` is one-shot and must be pinned (in ms) at
            // register time so a listener restart preserves the
            // deadline. If the pin is missing, the register flow is
            // broken; fail loudly instead of silently restarting the
            // clock. Millisecond-precise throughout (no second
            // truncation): a sub-second After fires after its real
            // duration, not immediately.
            let target_ms = pinned_after_unix_ms
                .expect("After timer must have pinned next_fire_at_unix_ms in kind_state");
            let delta_ms = (target_ms - unix_now_ms() as i64).max(0) as u64;
            let deadline = DateTime::from_timestamp_millis(target_ms)?;
            Some((Instant::now() + Duration::from_millis(delta_ms), deadline))
        }
        TimerSpec::At { when } => {
            let now = Utc::now();
            let delta = *when - now;
            let ms = delta.num_milliseconds();
            if ms <= 0 {
                None
            } else {
                Some((Instant::now() + Duration::from_millis(ms as u64), *when))
            }
        }
        TimerSpec::Cron { expression, timezone } => {
            // The expression is supposed to be validated at register
            // time (Signal::validate inside register_signal). If the
            // validator and the `cron` parser drift apart on a minor
            // version bump, a panic here would crash the timer task
            // and silently take down every other timer the same task
            // owned. Log + return None instead: the signal stays
            // registered but doesn't fire until the underlying bug
            // is fixed.
            let schedule = match cron::Schedule::from_str(expression) {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!(
                        target: "weft_listener::timer",
                        expression,
                        error = %e,
                        "cron parser rejected a previously-validated expression; \
                         skipping this timer (parser/validator drift)"
                    );
                    return None;
                }
            };
            let zone = match weft_core::signal::timer::parse_timezone(timezone) {
                Ok(z) => z,
                Err(e) => {
                    tracing::error!(target: "weft_listener::timer", timezone, error = %e,
                        "cron zone rejected after validation; skipping this timer");
                    return None;
                }
            };
            // The next moment is found on the zone's wall clock
            // (`WallClock`: one answer inside the hour the clocks
            // change, where the bare zone would skip a day), so a
            // schedule at nine stays at nine all year, then carried as
            // UTC like every other deadline.
            let now: DateTime<Utc> = Utc::now();
            let next_dt = schedule
                .upcoming(weft_core::signal::timer::WallClock(zone))
                .next()?
                .with_timezone(&Utc);
            let delta = next_dt - now;
            let ms = delta.num_milliseconds().max(0) as u64;
            Some((Instant::now() + Duration::from_millis(ms), next_dt))
        }
    }
}

fn unix_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock is past UNIX_EPOCH")
        .as_millis() as u64
}

inventory::submit!(&TimerHandler as &dyn KindHandler);
