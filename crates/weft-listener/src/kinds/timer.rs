//! Timer handler. Each registration spawns one tokio task that
//! sleeps to the next fire, enqueues a `FireSignal` task via the
//! broker, and loops if the spec is recurring.

use std::str::FromStr;

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde_json::Value;
use tokio::task::JoinHandle;
use tokio::time::{sleep_until, Duration, Instant};
use weft_core::primitive::{SignalAuth, SignalRouting, SignalSpec, SignalSurface};
use weft_core::signal::{Signal, Timer, TimerSpec};

use weft_core::signal::listener_protocol::{ProcessOutcome, ProcessTarget};
use crate::registry::RegisteredSignal;

use async_trait::async_trait;

use super::{KindHandler, LiveCtx, SpawnCtx};
use weft_core::live::{LiveFeed, LiveItem};

pub struct TimerHandler;

/// The schedule in one line, as the person who wrote it would say it.
fn schedule_line(spec: &TimerSpec) -> String {
    match spec {
        TimerSpec::After { duration_ms } => {
            format!("once, {} after activation", human_duration(*duration_ms))
        }
        TimerSpec::At { when } => format!("once, at {}", when.to_rfc3339()),
        TimerSpec::Cron { expression, timezone } => format!("{expression} ({timezone})"),
    }
}

/// Milliseconds as a person reads them. Whole units only: a schedule
/// written as `30m` should read back as `30m`, not `0h 30m 0s`.
fn human_duration(ms: u64) -> String {
    const SECOND: u64 = 1000;
    const MINUTE: u64 = 60 * SECOND;
    const HOUR: u64 = 60 * MINUTE;
    const DAY: u64 = 24 * HOUR;
    for (unit, name) in [(DAY, "d"), (HOUR, "h"), (MINUTE, "m"), (SECOND, "s")] {
        if ms >= unit && ms.is_multiple_of(unit) {
            return format!("{}{name}", ms / unit);
        }
    }
    format!("{ms}ms")
}

#[async_trait]
impl KindHandler for TimerHandler {
    fn tag(&self) -> &'static str {
        Timer::TAG
    }

    fn compute_routing(&self, _spec: &SignalSpec) -> Result<SignalRouting> {
        Ok(SignalRouting {
            surface: SignalSurface::Internal,
            auth: SignalAuth::None,
            auth_config: Value::Null,
        })
    }

    /// A timer is the one kind a person can wake by hand, because what
    /// it waits for is the clock and "now" is a true answer for it.
    ///
    /// Both stamps are now, and they differ from a real tick in exactly
    /// the way they should: a tick reports the deadline it was AIMED at
    /// as `scheduledTime` and when it actually woke as `actualTime`, so
    /// a late tick shows the gap. A hand wake was aimed at this moment,
    /// so the two agree. The node reads the same two fields either way
    /// and never has to know which happened.
    fn wake_by_hand(&self) -> Option<Value> {
        let now = Utc::now().to_rfc3339();
        Some(serde_json::json!({ "scheduledTime": now, "actualTime": now }))
    }

    /// Pin the fire time for `After` schedules so a listener restart
    /// doesn't reset the clock. It counts from when the wait was ASKED
    /// for, not from when this pod heard of it: the trip from the node
    /// through the dispatcher to here is not part of the wait. `At` is already
    /// wall-clock-absolute in the spec, and `Cron` recomputes the
    /// next tick from "now" on every iteration (each fire is its
    /// own deadline), so neither needs persisted state.
    // `_prior` is deliberately ignored: reactivate IS a fresh schedule
    // (an After-timer restarts its countdown from the activation).
    fn compute_initial_state(&self, spec: &SignalSpec, _prior: Option<&Value>, asked_at_unix_ms: i64) -> Result<Value> {
        let timer: Timer = serde_json::from_value(spec.config.clone())
            .map_err(|e| anyhow::anyhow!("malformed timer spec: {e}"))?;
        if let TimerSpec::After { duration_ms } = timer.spec {
            // Pin in MILLISECONDS (no /1000 truncation): a sub-second
            // `After` (duration_ms < 1000, which validation allows)
            // must not floor to 0 and fire immediately. At/Cron are
            // already ms-precise; After matches.
            return Ok(serde_json::json!({ "next_fire_at_unix_ms": after_deadline_ms(asked_at_unix_ms, duration_ms) }));
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
        // Checked HERE, where a failure is this registration's error and
        // becomes a 400 the person sees, rather than inside the loop.
        // An `After` with no pinned deadline used to panic in the
        // spawned task, and a panic in a detached task is caught by the
        // runtime and goes nowhere: the signal stayed in the registry,
        // counted towards this pod's load, showed as registered, and
        // never fired. Loud is only loud where somebody is listening.
        if matches!(timer.spec, TimerSpec::After { .. }) && pinned_after.is_none() {
            anyhow::bail!(
                "an `after` timer must carry the moment it was aimed at, so a listener restart \
                 keeps the same deadline instead of starting the wait again; this one has none"
            );
        }
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

    /// A timer has no address to show, so it shows its schedule:
    /// somebody looking at the node wants to know when it goes off.
    fn live(&self, ctx: &LiveCtx<'_>) -> LiveFeed {
        let timer = match super::config_for_display::<Timer>(ctx.sig, "Fires") {
            Ok(timer) => timer,
            Err(feed) => return feed,
        };
        LiveFeed::new(vec![LiveItem::text("Fires", schedule_line(&timer.spec))])
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
        // The signal's row is written right around now, so an unknown
        // token only means "not committed yet" within a short window of
        // this moment (see `redeliver`).
        let armed = Instant::now();
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
            deliver(&fire, payload, armed).await;

            if matches!(spec, TimerSpec::After { .. } | TimerSpec::At { .. }) {
                return;
            }
        }
    })
}

/// How long a tick waits before it is offered again after a failed
/// delivery, doubling from the first to the longest.
const REDELIVER_FIRST: Duration = Duration::from_millis(250);
const REDELIVER_LONGEST: Duration = Duration::from_secs(30);

/// How long after arming an unknown token still reads as "the row is
/// not committed yet". The row is written right around arming, so past
/// this window the signal is gone.
fn unknown_token_grace() -> Duration {
    weft_core::time_scale::scaled(Duration::from_secs(120))
}

/// Offer one tick until it lands. A tick is the whole of what a timer
/// has to say and nothing re-offers it later (a feed kind keeps a
/// cursor and offers an item again on its next poll; a timer has no
/// next poll for an `After`), so a delivery that fails would leave the
/// run waiting on it for ever. The failure that happens in practice is
/// the tick beating its own registration: this pod is armed before the
/// dispatcher has written the signal's row, a wait counted from when
/// the node asked can already be due by then, and the broker refuses a
/// token it does not know yet. The broker answers the same for a
/// signal that is gone, so an unknown token is retried only within
/// [`unknown_token_grace`] of arming; past it the tick is dropped with
/// a warning. Every other failure is logged by the fire path and
/// retried with backoff.
async fn deliver(fire: &crate::event_context::FireContext, payload: Value, armed: Instant) {
    let mut wait = REDELIVER_FIRST;
    loop {
        let outcome = fire.fire(payload.clone(), "timer").await;
        match redeliver(outcome, armed.elapsed()) {
            Redeliver::Done => return,
            Redeliver::Again => {
                tokio::time::sleep(wait).await;
                wait = (wait * 2).min(REDELIVER_LONGEST);
            }
            Redeliver::Gone => {
                tracing::warn!(
                    target: "weft_listener::timer",
                    token = %fire.token(),
                    "the broker does not know this timer's signal token long after it was \
                     armed; the signal is gone, dropping the tick"
                );
                return;
            }
        }
    }
}

/// What a tick does after one delivery attempt.
#[derive(Debug, PartialEq, Eq)]
enum Redeliver {
    /// Delivered, filtered or fenced: nothing more to offer.
    Done,
    /// Not delivered and may still land: offer it again.
    Again,
    /// The signal is gone: stop.
    Gone,
}

fn redeliver(outcome: crate::event_context::FireOutcome, since_armed: Duration) -> Redeliver {
    use crate::event_context::FireOutcome;
    match outcome {
        FireOutcome::Fired | FireOutcome::Filtered | FireOutcome::Fenced => Redeliver::Done,
        FireOutcome::EnqueueFailed => Redeliver::Again,
        FireOutcome::UnknownSignal if since_armed < unknown_token_grace() => Redeliver::Again,
        FireOutcome::UnknownSignal => Redeliver::Gone,
    }
}

/// The next fire as both the monotonic `Instant` to sleep on AND the
/// intended wall-clock deadline (reported as `scheduledTime` so a
/// late wakeup is distinguishable from an on-time one).
fn next_fire(spec: &TimerSpec, pinned_after_unix_ms: Option<i64>) -> Option<(Instant, DateTime<Utc>)> {
    match spec {
        TimerSpec::After { duration_ms: _ } => {
            // `After` is one-shot and must be pinned (in ms) at
            // register time so a listener restart preserves the
            // deadline. `spawn_task` refuses a registration without the
            // pin, which is where the person can be told; by here the
            // only honest thing left is to not fire, since firing on a
            // restarted clock would be a deadline nobody asked for.
            // Millisecond-precise throughout (no second truncation): a
            // sub-second After fires after its real duration, not
            // immediately.
            let target_ms = pinned_after_unix_ms?;
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
            // version bump, a panic here would go NOWHERE: each timer
            // runs in its own detached task, so the runtime swallows it
            // and the signal sits registered, counted, and silent. Log
            // and return None instead, which at least leaves a line
            // saying which expression stopped working.
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

/// The moment an `After` timer fires: `duration_ms` after it was asked
/// for. Validation already refused a duration the epoch cannot hold.
fn after_deadline_ms(asked_at_unix_ms: i64, duration_ms: u64) -> i64 {
    asked_at_unix_ms.saturating_add(i64::try_from(duration_ms).unwrap_or(i64::MAX))
}

fn unix_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock is past UNIX_EPOCH")
        .as_millis() as u64
}

inventory::submit!(&TimerHandler as &dyn KindHandler);

#[cfg(test)]
mod schedule_tests {
    use super::*;

    /// A wait counts from when it was asked for: the time the
    /// registration spent reaching this pod is already spent.
    #[test]
    fn an_after_timer_counts_from_when_it_was_asked_for() {
        let spec = weft_core::signal::to_spec(Timer { spec: TimerSpec::After { duration_ms: 3_000 } });
        let asked = 1_700_000_000_000;
        let state = TimerHandler.compute_initial_state(&spec, None, asked).unwrap();
        assert_eq!(state["next_fire_at_unix_ms"], asked + 3_000);
    }

    #[test]
    fn a_duration_reads_back_in_the_unit_it_was_written_in() {
        // Somebody who wrote `30m` should read `30m`, not `0h 30m 0s`.
        assert_eq!(human_duration(30 * 60 * 1000), "30m");
        assert_eq!(human_duration(90 * 60 * 1000), "90m", "not a whole hour, so minutes");
        assert_eq!(human_duration(2 * 60 * 60 * 1000), "2h");
        assert_eq!(human_duration(24 * 60 * 60 * 1000), "1d");
        assert_eq!(human_duration(5000), "5s");
    }

    #[test]
    fn anything_that_is_not_a_whole_unit_stays_in_milliseconds() {
        // Rounding here would tell somebody their timer fires at a time
        // it does not.
        assert_eq!(human_duration(1500), "1500ms");
        assert_eq!(human_duration(500), "500ms");
        assert_eq!(human_duration(0), "0ms");
    }

    #[test]
    fn every_schedule_says_when_it_goes_off() {
        assert_eq!(
            schedule_line(&TimerSpec::After { duration_ms: 30 * 60 * 1000 }),
            "once, 30m after activation"
        );
        let line = schedule_line(&TimerSpec::Cron {
            expression: "0 9 * * 1-5".into(),
            timezone: "Europe/Paris".into(),
        });
        assert_eq!(line, "0 9 * * 1-5 (Europe/Paris)");
        let when = chrono::DateTime::parse_from_rfc3339("2026-09-19T08:00:00Z")
            .expect("a fixed instant")
            .with_timezone(&chrono::Utc);
        assert!(schedule_line(&TimerSpec::At { when }).contains("2026-09-19T08:00:00"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event_context::FireOutcome;

    #[test]
    fn a_delivered_filtered_or_fenced_tick_is_done() {
        for outcome in [FireOutcome::Fired, FireOutcome::Filtered, FireOutcome::Fenced] {
            assert_eq!(redeliver(outcome, Duration::ZERO), Redeliver::Done);
        }
    }

    #[test]
    fn a_failed_enqueue_is_retried_however_long_ago_it_armed() {
        assert_eq!(redeliver(FireOutcome::EnqueueFailed, Duration::ZERO), Redeliver::Again);
        assert_eq!(
            redeliver(FireOutcome::EnqueueFailed, unknown_token_grace() * 10),
            Redeliver::Again
        );
    }

    #[test]
    fn an_unknown_token_is_retried_only_within_the_grace_after_arming() {
        let grace = unknown_token_grace();
        assert_eq!(redeliver(FireOutcome::UnknownSignal, Duration::ZERO), Redeliver::Again);
        assert_eq!(
            redeliver(FireOutcome::UnknownSignal, grace - Duration::from_millis(1)),
            Redeliver::Again
        );
        assert_eq!(redeliver(FireOutcome::UnknownSignal, grace), Redeliver::Gone);
    }
}
