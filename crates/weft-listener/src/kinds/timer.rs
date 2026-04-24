//! Timer signals (Cron / After / At). Each registration spawns one
//! tokio task that sleeps to the next fire, relays, and loops if
//! the spec is recurring.

use std::str::FromStr;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use tokio::task::JoinHandle;
use tokio::time::{sleep_until, Duration, Instant};
use weft_core::primitive::TimerSpec;

use crate::relay::FireRelayer;

/// Spawn the firing task for a TimerSpec. The task lives until
/// aborted (via the TaskGuard held in the registry entry).
pub fn spawn(token: String, spec: TimerSpec, relay: Arc<FireRelayer>) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let Some(next) = next_fire(&spec) else {
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
            relay.fire(token.clone(), payload).await;

            if matches!(spec, TimerSpec::After { .. } | TimerSpec::At { .. }) {
                return;
            }
        }
    })
}

fn next_fire(spec: &TimerSpec) -> Option<Instant> {
    match spec {
        TimerSpec::After { duration_ms } => {
            Some(Instant::now() + Duration::from_millis(*duration_ms))
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
            let schedule = cron::Schedule::from_str(expression).ok()?;
            let now: DateTime<Utc> = Utc::now();
            let next_dt = schedule.upcoming(Utc).next()?;
            let delta = next_dt - now;
            let ms = delta.num_milliseconds().max(0) as u64;
            Some(Instant::now() + Duration::from_millis(ms))
        }
    }
}
