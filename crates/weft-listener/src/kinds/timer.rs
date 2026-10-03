//! Timer handler. A timer waits for the clock, so it needs nothing
//! running between fires: its next moment is on the signal row
//! (`next_fire_at_unix_ms`), handed to the platform's alarm as a wake,
//! and each wake fires once and sets the next (so a cron is "each wake
//! sets the next").

use std::str::FromStr;

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde_json::Value;
use weft_core::primitive::{SignalAuth, SignalRouting, SignalSpec, SignalSurface};
use weft_core::signal::{Signal, Timer, TimerSpec};

use weft_core::signal::listener_protocol::{ProcessOutcome, ProcessTarget};
use crate::registry::RegisteredSignal;

use async_trait::async_trait;

use super::{BetweenFires, KindHandler, LiveCtx, SpawnCtx, WakeFrom, Woken};
use weft_core::live::{LiveFeed, LiveItem};

pub struct TimerHandler;

/// The state key holding the next moment the timer fires. Absent: the
/// timer has nothing left to fire (a one-shot that fired, or an `at`
/// already past when it was armed).
const NEXT: &str = "next_fire_at_unix_ms";

fn next_of(state: &Value) -> Option<i64> {
    state.get(NEXT).and_then(Value::as_i64)
}

fn state_at(next: Option<i64>) -> Value {
    match next {
        Some(at) => serde_json::json!({ NEXT: at }),
        None => Value::Object(serde_json::Map::new()),
    }
}

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

    fn between_fires(&self, _spec: &SignalSpec, _kind_state: &Value) -> Result<BetweenFires> {
        Ok(BetweenFires::Wakes)
    }

    /// The timer's first moment, pinned on the row so a restart never
    /// moves it. An `after` counts from when the wait was ASKED for, not
    /// from when the listener heard of it: the trip from the node
    /// through the dispatcher is not part of the wait. An `at` is its
    /// own moment, or nothing when that moment had passed by then. A
    /// cron's first is its next occurrence after the asking.
    // `_prior` is deliberately ignored: reactivate IS a fresh schedule
    // (an after-timer restarts its countdown from the activation).
    fn compute_initial_state(&self, spec: &SignalSpec, _prior: Option<&Value>, asked_at_unix_ms: i64, _is_resume: bool) -> Result<Value> {
        let timer: Timer = serde_json::from_value(spec.config.clone())
            .map_err(|e| anyhow::anyhow!("malformed timer spec: {e}"))?;
        let next = match &timer.spec {
            // Milliseconds throughout (no second truncation): a
            // sub-second `after` (which validation allows) must not
            // floor to 0 and fire at once.
            TimerSpec::After { duration_ms } => Some(after_deadline_ms(asked_at_unix_ms, *duration_ms)),
            TimerSpec::At { when } => Some(when.timestamp_millis()).filter(|at| *at > asked_at_unix_ms),
            TimerSpec::Cron { .. } => next_cron_after(&timer.spec, asked_at_unix_ms)?,
        };
        Ok(state_at(next))
    }

    fn next_wake(&self, _spec: &SignalSpec, state: &Value, _from: WakeFrom, _now_ms: i64) -> Result<Option<i64>> {
        Ok(next_of(state))
    }

    /// Fire the moment on the row once it is due, at least once.
    ///
    /// The tick is enqueued BEFORE the moment is claimed, under a key
    /// naming the moment (`tick:{due}`), and a failed enqueue or claim
    /// fails the wake so the alarm delivers it again. So a process that
    /// dies anywhere in here leaves the row unclaimed and the retried wake
    /// fires it; a retry landing while the first enqueue is still queued
    /// collapses onto it. Claiming first would lose the tick to any death
    /// between the claim and the enqueue, and a one-shot never fires
    /// again. The claim still has one winner, so of two copies woken for
    /// one moment only one moves the row on.
    ///
    /// A wake aimed at an earlier moment than the row names (one already
    /// served) does nothing; its next wake is the one the row names.
    async fn on_wake(&self, spec: &SignalSpec, woken: Woken, ctx: SpawnCtx) -> Result<Option<Value>> {
        let timer: Timer = serde_json::from_value(spec.config.clone())
            .map_err(|e| anyhow::anyhow!("malformed timer spec: {e}"))?;
        let Some(due) = next_of(&woken.state) else { return Ok(Some(woken.state)) };
        if due > woken.now_ms.max(woken.aimed_at_ms) {
            return Ok(Some(woken.state));
        }
        // A cron's next is its next occurrence after both the moment it
        // just served and now, so a wake that came late does not fire
        // every occurrence it slept through. A one-shot has no next.
        let next = match &timer.spec {
            TimerSpec::Cron { .. } => next_cron_after(&timer.spec, due.max(woken.now_ms))?,
            TimerSpec::After { .. } | TimerSpec::At { .. } => None,
        };
        let after = state_at(next);
        // scheduledTime = the moment it was aimed at; actualTime = when it
        // actually woke. They differ when the wake is late (the whole
        // point of exposing both, per the cron node's metadata).
        let scheduled = DateTime::from_timestamp_millis(due)
            .ok_or_else(|| anyhow::anyhow!("timer moment {due} is outside the calendar"))?;
        let payload = serde_json::json!({
            "scheduledTime": scheduled.to_rfc3339(),
            "actualTime": Utc::now().to_rfc3339(),
        });
        use crate::event_context::FireOutcome;
        match ctx.fire.fire_as(payload, "timer", &format!("tick:{due}")).await {
            FireOutcome::Fired | FireOutcome::Filtered => {}
            // The row was read a moment ago, so the broker not knowing the
            // token means the signal went in between: nothing to fire.
            FireOutcome::UnknownSignal | FireOutcome::NotHeld => return Ok(Some(woken.state)),
            FireOutcome::EnqueueFailed => anyhow::bail!(
                "the tick for {scheduled} could not be enqueued (see the warning before this line); \
                 the alarm delivers this wake again"
            ),
        }
        // Losing the claim means another copy claimed this moment; its
        // tick and ours share one key, and it set the next wake.
        let claimed = ctx.fire.claim_kind_state(after.clone(), woken.seq).await?;
        Ok(claimed.then_some(after))
    }

    fn process_entry(
        &self,
        _sig: &RegisteredSignal,
        payload: Value,
    ) -> ProcessOutcome {
        // A timer tick (raised by a wake, delivered through the
        // FireSignal broker task) routes to the entry trigger.
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

/// A cron's next occurrence strictly after `after_ms`, on the zone's
/// wall clock (`WallClock`: one answer inside the hour the clocks change,
/// where the bare zone would skip a day), so a schedule at nine stays at
/// nine all year. `None` for a schedule with no next occurrence. The
/// expression and zone were validated at registration; a parser that
/// disagrees with the validator is an error naming the expression.
fn next_cron_after(spec: &TimerSpec, after_ms: i64) -> Result<Option<i64>> {
    let TimerSpec::Cron { expression, timezone } = spec else { return Ok(None) };
    let schedule = cron::Schedule::from_str(expression)
        .map_err(|e| anyhow::anyhow!("cron parser rejected '{expression}' after validation accepted it: {e}"))?;
    let zone = weft_core::signal::timer::parse_timezone(timezone)
        .map_err(|e| anyhow::anyhow!("cron zone '{timezone}' rejected after validation: {e}"))?;
    let after = DateTime::from_timestamp_millis(after_ms)
        .ok_or_else(|| anyhow::anyhow!("moment {after_ms} is outside the calendar"))?
        .with_timezone(&weft_core::signal::timer::WallClock(zone));
    Ok(schedule.after(&after).next().map(|next| next.with_timezone(&Utc).timestamp_millis()))
}

/// The moment an `After` timer fires: `duration_ms` after it was asked
/// for. Validation already refused a duration the epoch cannot hold.
fn after_deadline_ms(asked_at_unix_ms: i64, duration_ms: u64) -> i64 {
    asked_at_unix_ms.saturating_add(i64::try_from(duration_ms).unwrap_or(i64::MAX))
}

inventory::submit!(&TimerHandler as &dyn KindHandler);

#[cfg(test)]
mod schedule_tests {
    use super::*;

    /// A wait counts from when it was asked for: the time the
    /// registration spent reaching this process is already spent.
    #[test]
    fn an_after_timer_counts_from_when_it_was_asked_for() {
        let spec = weft_core::signal::to_spec(Timer { spec: TimerSpec::After { duration_ms: 3_000 } });
        let asked = 1_700_000_000_000;
        let state = TimerHandler.compute_initial_state(&spec, None, asked, false).unwrap();
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

    fn spec(timer: TimerSpec) -> SignalSpec {
        weft_core::signal::to_spec(Timer { spec: timer })
    }

    /// Every schedule's first moment is pinned on the row, and the wake
    /// is that moment: the same answer however many times it is asked,
    /// so arming twice sets one wake.
    #[test]
    fn the_first_moment_is_pinned_and_is_the_wake() {
        let asked = 1_700_000_000_000;
        let after = spec(TimerSpec::After { duration_ms: 3_000 });
        let state = TimerHandler.compute_initial_state(&after, None, asked, false).unwrap();
        assert_eq!(next_of(&state), Some(asked + 3_000));
        for from in [WakeFrom::Armed, WakeFrom::Woken { aimed_at_ms: asked }] {
            assert_eq!(TimerHandler.next_wake(&after, &state, from, asked + 99).unwrap(), Some(asked + 3_000));
        }

        let when = DateTime::from_timestamp_millis(asked + 60_000).unwrap();
        let at = spec(TimerSpec::At { when });
        assert_eq!(next_of(&TimerHandler.compute_initial_state(&at, None, asked, false).unwrap()), Some(asked + 60_000));
        let past = TimerHandler.compute_initial_state(&at, None, asked + 120_000, false).unwrap();
        assert_eq!(next_of(&past), None, "an `at` already past when armed never fires");
        assert_eq!(TimerHandler.next_wake(&at, &past, WakeFrom::Armed, asked).unwrap(), None);
    }

    /// A cron's next occurrence follows the one it served, skipping any
    /// it slept through rather than firing them all.
    #[test]
    fn a_cron_moves_to_its_next_occurrence_after_now() {
        let every_minute = TimerSpec::Cron { expression: "0 * * * * *".into(), timezone: "UTC".into() };
        let t0 = DateTime::parse_from_rfc3339("2026-09-19T08:00:00Z").unwrap().timestamp_millis();
        assert_eq!(next_cron_after(&every_minute, t0).unwrap(), Some(t0 + 60_000));
        assert_eq!(next_cron_after(&every_minute, t0 + 1).unwrap(), Some(t0 + 60_000));
        // Woken ten minutes late: the next is after now, not the missed ones.
        assert_eq!(next_cron_after(&every_minute, t0 + 600_500).unwrap(), Some(t0 + 660_000));
    }
}
