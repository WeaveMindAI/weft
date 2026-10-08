//! The wake-and-drain loop every role's background work runs.
//!
//! The loops that use it (the dispatcher's task picker, its delivery of
//! executions, the lifecycle claimer, the reapers; the supervisor's and
//! the broker's sweeps) all have the same shape: sleep until a write they
//! care about is announced on the process's [`PgSignalWatch`] (see
//! [`crate::pg_signal`]), then run their drain body until it reports the
//! queue empty. A safety tick drains anyway every so often, for the
//! notification that was lost, and for the rows whose readiness no write
//! announces (a lease that lapsed, a delay that ran out). A loop that only
//! notices silence has no channel at all and runs on its safety tick.
//!
//! A role runs its loops one of two ways, by where it is placed:
//!
//! - in a local install's one process, [`run_forever`] per loop, sleeping
//!   on the process's one `LISTEN` connection;
//! - as a service that scales to zero, [`drain_due`] once per tick: the
//!   role is called (by the process that wrote what a loop waits on,
//!   naming the loops it concerns, or by its own next alarm), drains those
//!   loops and the ones due, and answers when the next look is due. A
//!   process at zero hears no notification, so this is the only way its
//!   loops run there. A tick may land on any instance of the role, so
//!   when each loop is due lives in the database (the `role_loop_due`
//!   table, [`GROUP`]), shared by them all. A loop with nothing left to
//!   watch is not looked at again for [`IDLE_LOOK`], so an install with
//!   nothing going on wakes nothing, and its database can sleep.
//!
//! The subsystem provides its drain body, which channels wake it, and its
//! safety interval; the coalescing and the timing live here.
//!
//! [`PgSignalWatch`]: crate::pg_signal::PgSignalWatch

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Context, Result};
use sqlx::PgPool;
use tokio::time::Instant;

use crate::pg_signal::Subscription;

/// The usual safety net for missed notifications while a process that
/// listens is up (on a local install, and a cloud role that writes), and
/// how soon a failed drain is tried again anywhere. Notifications are
/// best-effort by Postgres design (a reconnecting listener can lose
/// some, though it says so with a recheck), so this only catches what
/// slipped past; 30s of delay on a lost one is acceptable, and a tighter
/// tick would hammer the DB for nothing.
pub const SAFETY_POLL_INTERVAL: Duration = Duration::from_secs(30);

/// How long a loop of a role that scales to zero sleeps once it has
/// nothing left to watch ([`DrainStep::Done`]): until a write wakes it, or
/// this long at the latest. Every look wakes the role and its database, so
/// it is long; all it catches is a wake lost between a write and its ring
/// (the writer's process died right after its commit), and the
/// housekeeping that may run late (expiries, retention).
pub const IDLE_LOOK: Duration = Duration::from_secs(6 * 3600);

/// How soon a loop looks again when a sibling holds its install-wide lock.
/// The sibling is draining right now, but it may have read before the
/// write that woke this loop, so the wake is kept instead of dropped; a
/// lock is held for one drain, so a short pause finds it free.
pub const LOCK_HELD_RETRY: Duration = Duration::from_secs(2);

/// What a subsystem's drain body returns after one iteration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrainStep {
    /// More work likely remains; the runner re-invokes the body
    /// without waiting. Use when an iteration hit a row-limit or
    /// otherwise expects siblings behind it.
    More,
    /// Nothing is left that a look before the next write would find:
    /// the runner sleeps until a wake (on a local install, at most its
    /// safety interval; scaled to zero, [`IDLE_LOOK`]).
    Done,
    /// Something is left that will need a look without anything
    /// announcing it (a row whose writer has not committed, a delay that
    /// has not run out, a lease that may lapse because its holder died).
    /// The runner looks again after this long, or on the next wake if
    /// that comes first.
    RetryIn(Duration),
}

/// Which notifications wake a loop: those on `channel` whose payload
/// `concerns` says are its business.
#[derive(Clone, Copy)]
pub struct WakeOn {
    pub channel: &'static str,
    pub concerns: fn(&str) -> bool,
}

impl WakeOn {
    /// Every notification on `channel`.
    pub const fn any(channel: &'static str) -> Self {
        Self { channel, concerns: |_| true }
    }

    /// Whether a notification on `channel` saying `payload` wakes this.
    pub fn hears(&self, channel: &str, payload: &str) -> bool {
        self.channel == channel && (self.concerns)(payload)
    }
}

/// One background loop of a role: what wakes it, how often it looks
/// anyway, and its drain body.
#[derive(Clone)]
pub struct DrainLoop {
    /// For logs, the key its install-wide lock (when it takes one) is
    /// named by, and its row in `role_loop_due` ([`GROUP`]).
    pub name: &'static str,
    pub wake_on: &'static [WakeOn],
    pub safety: Duration,
    pub drain: std::sync::Arc<dyn Fn() -> futures::future::BoxFuture<'static, Result<DrainStep>> + Send + Sync>,
}

impl DrainLoop {
    pub fn new<F, Fut>(name: &'static str, wake_on: &'static [WakeOn], safety: Duration, drain: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<DrainStep>> + Send + 'static,
    {
        Self { name, wake_on, safety, drain: std::sync::Arc::new(move || Box::pin(drain())) }
    }

    /// Run this loop for the life of the process (see [`run`]).
    pub async fn run_forever(&self, signals: Subscription) {
        let drain = self.drain.clone();
        run(signals, self.wake_on, self.safety, self.name, move || drain()).await
    }
}

/// The `role_loop_due` table: when each loop of a role that scales to zero
/// next wants a look. Cloud Run sends a role's tick to any of its
/// instances, so this lives here, shared by all of them, and an instance
/// that never saw a loop's work still knows when it is due.
pub static GROUP: crate::SchemaGroup = crate::SchemaGroup {
    name: "role_loop_due",
    tables: &["role_loop_due"],
    ddl: &[r#"CREATE TABLE IF NOT EXISTS role_loop_due (
            -- The role (`CoreRole::as_str`) and one of its loops
            -- (`DrainLoop::name`).
            role TEXT NOT NULL,
            loop_name TEXT NOT NULL,
            -- When the loop next wants a look, in unix milliseconds on the
            -- database's clock, so instances whose own clocks disagree
            -- still agree on it.
            due_ms BIGINT NOT NULL,
            -- When this row was last written, on the same clock: how a
            -- pass tells whether a sibling booked a look since it read.
            written_ms BIGINT NOT NULL,
            PRIMARY KEY (role, loop_name)
        )"#],
    seed: &[],
};

/// The database's clock, in unix milliseconds. Every due time is read and
/// written on it, never on a process's own clock.
pub const DB_NOW_MS: &str = "(extract(epoch from clock_timestamp()) * 1000)::bigint";

/// What a pass does with one loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Plan {
    /// Drain it now.
    Run,
    /// Leave it until this moment (unix ms, the database's clock).
    SleepUntil(i64),
    /// The due times could not be read: leave it, and look again at its
    /// safety interval.
    Unknown,
}

/// What a pass that read the due times at `read_at` (`None` when the read
/// failed) does with a loop booked for `due_ms` (`None` with no row). A
/// woken loop always runs; one with no row is due now.
fn plan(woken: bool, read_at: Option<i64>, due_ms: Option<i64>) -> Plan {
    if woken {
        return Plan::Run;
    }
    match (read_at, due_ms) {
        (None, _) => Plan::Unknown,
        (Some(now), Some(at)) if at > now => Plan::SleepUntil(at),
        (Some(_), _) => Plan::Run,
    }
}

/// `at_ms` on the database's clock as an instant of this process, given
/// `clock`: a reading of the database's clock and the instant it was
/// taken at.
fn instant_of(clock: (i64, Instant), at_ms: i64) -> Instant {
    clock.1 + Duration::from_millis(at_ms.saturating_sub(clock.0).max(0).unsigned_abs())
}

/// The database's clock now, and when each of `role`'s loops that has a
/// row is next due, in one query.
/// What a pass reads of `role`'s due times: the database's clock, when
/// each loop is due, and the loops nobody has booked for two idle looks.
struct ReadDue {
    now: i64,
    due: HashMap<String, i64>,
    unbooked: Vec<String>,
}

/// One row of [`read_due`]'s query: the clock, then a loop's name, when
/// it is due, and whether nobody has booked it for two idle looks (all
/// three `None` when the role has no row).
type DueRow = (i64, Option<String>, Option<i64>, Option<bool>);

async fn read_due(pool: &PgPool, role: &str) -> Result<ReadDue> {
    let rows: Vec<DueRow> = sqlx::query_as(&format!(
        "SELECT c.now_ms, d.loop_name, d.due_ms, d.written_ms < c.now_ms - $2 \
         FROM (SELECT {DB_NOW_MS} AS now_ms) c \
         LEFT JOIN role_loop_due d ON d.role = $1"
    ))
    .bind(role)
    .bind(i64::try_from(2 * IDLE_LOOK.as_millis()).unwrap_or(i64::MAX))
    .fetch_all(pool)
    .await?;
    let now = rows.first().map(|r| r.0).context("the clock query returned no row")?;
    let unbooked = rows.iter().filter(|r| r.3 == Some(true)).filter_map(|r| r.1.clone()).collect();
    let due = rows.into_iter().filter_map(|(_, name, at, _)| Some((name?, at?))).collect();
    Ok(ReadDue { now, due, unbooked })
}

/// Book `role`'s loop `name` for a look `after` from now on the database's
/// clock, and answer when it is now due and the clock at the write. When
/// a sibling wrote the row since this pass read it at `read_at` (or the
/// read failed), the sooner of the two looks stands, so a wake the sibling
/// booked is never pushed back. A row written in the very millisecond of
/// the read counts as a sibling's: keeping the sooner look costs one extra
/// pass at most, overwriting it could cost a wake.
async fn book(pool: &PgPool, role: &str, name: &str, after: Duration, read_at: Option<i64>) -> Result<(i64, i64)> {
    let after_ms = i64::try_from(after.as_millis()).context("a look further out than the clock reaches")?;
    let row: (i64, i64) = sqlx::query_as(&format!(
        "INSERT INTO role_loop_due AS d (role, loop_name, due_ms, written_ms) \
         SELECT $1, $2, c.now_ms + $3, c.now_ms FROM (SELECT {DB_NOW_MS} AS now_ms) c \
         ON CONFLICT (role, loop_name) DO UPDATE SET \
             due_ms = CASE WHEN d.written_ms < $4::bigint THEN excluded.due_ms \
                           ELSE LEAST(d.due_ms, excluded.due_ms) END, \
             written_ms = excluded.written_ms \
         RETURNING due_ms, written_ms"
    ))
    .bind(role)
    .bind(name)
    .bind(after_ms)
    .bind(read_at)
    .fetch_one(pool)
    .await?;
    Ok(row)
}

/// How long one pass of [`drain_due`] keeps draining, shared out evenly
/// between the loops it runs: a loop with more left once its share is
/// spent books itself due now for the next tick. Every loop the pass runs
/// drains at least once, so a loop that always has more (a steady stream
/// of tasks) never keeps the loops after it waiting, nor holds the tick
/// open until the platform cuts it.
pub const TICK_SLICE: Duration = Duration::from_secs(60);

/// One pass of a role that scales to zero: drain the loops named in
/// `woken` (the writes they wait on happened) and the ones whose next
/// look is due, each until it has nothing left (or asks to look again
/// later, or spent its share of [`TICK_SLICE`]), book each one's next
/// look in `role_loop_due`, and answer how
/// soon any loop of `role` next wants a look. The others stay asleep, so
/// a pass run because a journal event was written does not also run the
/// hourly sweeps. A loop with nothing left sleeps [`IDLE_LOOK`]; one that
/// asked to look again does so then; a drain that fails is logged and
/// looked at again at its safety interval. When the due times cannot be
/// read or written, that is logged too, and the loops it leaves unknown
/// are looked at again at their safety interval.
pub async fn drain_due(pool: &PgPool, role: &str, loops: &[DrainLoop], woken: &[String]) -> Duration {
    drain_pass(pool, role, loops, woken, TICK_SLICE).await
}

/// [`drain_due`] with the pass's draining time as `slice`.
async fn drain_pass(pool: &PgPool, role: &str, loops: &[DrainLoop], woken: &[String], slice: Duration) -> Duration {
    let (read_at, due, unbooked) = match read_due(pool, role).await {
        Ok(read) => (Some(read.now), read.due, read.unbooked),
        Err(e) => {
            tracing::warn!(
                target: "weft_task_store::drain", role, error = %format!("{e:#}"),
                "could not read when the loops are due; running only the woken ones, the rest look again at their safety interval"
            );
            (None, HashMap::new(), Vec::new())
        }
    };
    // A loop renamed or dropped by a later weft leaves its row behind,
    // forgotten once nobody has booked it for two idle looks: until then
    // it may be a loop of another weft still taking ticks beside this one
    // (a deploy rolling out), which books every loop it runs at least
    // once per idle look.
    let gone: Vec<&String> = unbooked.iter().filter(|name| !loops.iter().any(|l| l.name == name.as_str())).collect();
    if !gone.is_empty() {
        if let Err(e) = sqlx::query("DELETE FROM role_loop_due WHERE role = $1 AND loop_name = ANY($2)")
            .bind(role)
            .bind(&gone)
            .execute(pool)
            .await
        {
            tracing::warn!(target: "weft_task_store::drain", role, error = %format!("{e:#}"), "could not forget the due times of loops this role no longer has; the next pass tries again");
        }
    }
    let mut clock = read_at.map(|now| (now, Instant::now()));
    let mut next: Option<Instant> = None;
    let plans: Vec<Plan> = loops.iter().map(|l| plan(woken.iter().any(|w| w == l.name), read_at, due.get(l.name).copied())).collect();
    let share = slice / u32::try_from(plans.iter().filter(|p| **p == Plan::Run).count().max(1)).unwrap_or(u32::MAX);
    for (l, plan) in loops.iter().zip(plans) {
        let look = match plan {
            Plan::SleepUntil(at) => instant_of(clock.expect("a planned sleep comes from a read clock"), at),
            Plan::Unknown => Instant::now() + l.safety,
            Plan::Run => {
                let started = Instant::now();
                let after = loop {
                    match (l.drain)().await {
                        Ok(DrainStep::More) if started.elapsed() >= share => break Duration::ZERO,
                        Ok(DrainStep::More) => continue,
                        Ok(DrainStep::Done) => break IDLE_LOOK,
                        Ok(DrainStep::RetryIn(after)) => break after,
                        Err(e) => {
                            tracing::warn!(target: "weft_task_store::drain", subsystem = l.name, error = %e, "drain failed; will retry at its next look");
                            break l.safety;
                        }
                    }
                };
                match book(pool, role, l.name, after, read_at).await {
                    Ok((due_ms, now)) => {
                        let at = (now, Instant::now());
                        clock = Some(at);
                        instant_of(at, due_ms)
                    }
                    Err(e) => {
                        tracing::warn!(
                            target: "weft_task_store::drain", role, subsystem = l.name, error = %format!("{e:#}"),
                            "could not book the loop's next look; looking again at its safety interval"
                        );
                        Instant::now() + after.min(l.safety)
                    }
                }
            }
        };
        next = Some(next.map_or(look, |n| n.min(look)));
    }
    next.map(|n| n.saturating_duration_since(Instant::now())).unwrap_or(IDLE_LOOK)
}

/// Drive the drain loop forever: drain once at start (rows that landed
/// before the loop subscribed), then after every wake, every `RetryIn`,
/// and every `safety` interval. Returns only when the process's signal
/// watch stops, since nothing could wake the loop again; the caller's
/// supervisor crashes the process on that.
///
/// `signals` must be subscribed before the call, and `target` is the
/// tracing target, so subsystems log under their own module name.
pub async fn run<F, Fut>(
    mut signals: Subscription,
    wake_on: &[WakeOn],
    safety: Duration,
    target: &'static str,
    mut drain: F,
) where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<DrainStep>>,
{
    let mut next_look = Instant::now();
    loop {
        // A loop that listens to no channel runs on its interval alone: a
        // recheck (the watch fell behind, or reconnected) concerns only the
        // loops that wait on a channel, and waking a timed sweep on one
        // would run it back to back while notifications pour in.
        if wake_on.is_empty() {
            tokio::time::sleep_until(next_look).await;
        } else if Instant::now() < next_look {
            let woken = signals
                .woken_before(next_look, |channel, payload| wake_on.iter().any(|w| w.hears(channel, payload)))
                .await;
            if let Err(e) = woken {
                tracing::error!(target: "weft_task_store::drain", subsystem = target, error = %e, "cannot be woken any more");
                return;
            }
        }
        // Everything heard up to here is covered by the drain that
        // follows, so a burst of notifications costs one drain, not one
        // per notification.
        if let Err(e) = signals.clear() {
            tracing::error!(target: "weft_task_store::drain", subsystem = target, error = %e, "cannot be woken any more");
            return;
        }
        next_look = Instant::now() + safety;
        // Drain until the body reports it has nothing left. This is what
        // makes a burst of more rows than one batch finish in one wake.
        loop {
            match drain().await {
                Ok(DrainStep::More) => continue,
                Ok(DrainStep::Done) => break,
                Ok(DrainStep::RetryIn(after)) => {
                    next_look = next_look.min(Instant::now() + after);
                    break;
                }
                Err(e) => {
                    tracing::warn!(
                        target: "weft_task_store::drain",
                        subsystem = target,
                        error = %e,
                        "drain failed; will retry on next wake"
                    );
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use crate::pg_signal::Heard;
    use tokio::sync::broadcast;

    const CHANNEL: &str = "chan";
    const WAKE: &[WakeOn] = &[WakeOn { channel: CHANNEL, concerns: |p| p == "mine" }];

    /// A drain body that records when it ran and answers from a script
    /// (then `Done` once the script is spent).
    fn scripted(
        runs: Arc<Mutex<Vec<Duration>>>,
        script: Vec<DrainStep>,
    ) -> impl FnMut() -> std::future::Ready<Result<DrainStep>> {
        let started = Instant::now();
        let script = Arc::new(Mutex::new(std::collections::VecDeque::from(script)));
        move || {
            runs.lock().unwrap().push(started.elapsed());
            std::future::ready(Ok(script.lock().unwrap().pop_front().unwrap_or(DrainStep::Done)))
        }
    }

    fn signal(payload: &str) -> Heard {
        Heard::Signal { channel: CHANNEL, payload: payload.into() }
    }

    #[tokio::test(start_paused = true)]
    async fn it_drains_at_start_and_until_the_body_is_done() {
        let (_tx, rx) = broadcast::channel(16);
        let runs = Arc::new(Mutex::new(Vec::new()));
        let body = scripted(runs.clone(), vec![DrainStep::More, DrainStep::More]);
        tokio::spawn(run(rx.into(), WAKE, Duration::from_secs(30), "test", body));
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(runs.lock().unwrap().len(), 3, "More, More, Done");
    }

    #[tokio::test(start_paused = true)]
    async fn a_wake_that_concerns_it_drains_and_one_that_does_not_is_ignored() {
        let (tx, rx) = broadcast::channel(16);
        let runs = Arc::new(Mutex::new(Vec::new()));
        tokio::spawn(run(rx.into(), WAKE, Duration::from_secs(30), "test", scripted(runs.clone(), vec![])));
        tokio::time::sleep(Duration::from_secs(1)).await;
        tx.send(signal("theirs")).unwrap();
        tx.send(Heard::Signal { channel: "other", payload: "mine".into() }).unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(runs.lock().unwrap().len(), 1, "only the start drain");
        tx.send(signal("mine")).unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(runs.lock().unwrap().len(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn a_recheck_drains() {
        let (tx, rx) = broadcast::channel(16);
        let runs = Arc::new(Mutex::new(Vec::new()));
        tokio::spawn(run(rx.into(), WAKE, Duration::from_secs(30), "test", scripted(runs.clone(), vec![])));
        tokio::time::sleep(Duration::from_secs(1)).await;
        tx.send(Heard::Recheck).unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(runs.lock().unwrap().len(), 2);
    }

    /// A loop that listens to no channel is never woken early, not even
    /// by a recheck: it runs on its interval alone.
    #[tokio::test(start_paused = true)]
    async fn a_timed_loop_ignores_rechecks() {
        let (tx, rx) = broadcast::channel(16);
        let runs = Arc::new(Mutex::new(Vec::new()));
        tokio::spawn(run(rx.into(), &[], Duration::from_secs(30), "test", scripted(runs.clone(), vec![])));
        tokio::time::sleep(Duration::from_secs(1)).await;
        for _ in 0..3 {
            tx.send(Heard::Recheck).unwrap();
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(runs.lock().unwrap().len(), 1, "only the start drain");
        tokio::time::sleep(Duration::from_secs(30)).await;
        assert_eq!(runs.lock().unwrap().len(), 2, "then its interval");
    }

    #[tokio::test(start_paused = true)]
    async fn a_burst_of_wakes_costs_one_drain() {
        let (tx, rx) = broadcast::channel(64);
        let runs = Arc::new(Mutex::new(Vec::new()));
        tokio::spawn(run(rx.into(), WAKE, Duration::from_secs(30), "test", scripted(runs.clone(), vec![])));
        tokio::time::sleep(Duration::from_secs(1)).await;
        for _ in 0..20 {
            tx.send(signal("mine")).unwrap();
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(runs.lock().unwrap().len(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn with_no_signal_the_safety_tick_drains() {
        let (_tx, rx) = broadcast::channel(16);
        let runs = Arc::new(Mutex::new(Vec::new()));
        tokio::spawn(run(rx.into(), WAKE, Duration::from_secs(30), "test", scripted(runs.clone(), vec![])));
        tokio::time::sleep(Duration::from_secs(61)).await;
        let runs = runs.lock().unwrap().clone();
        assert_eq!(runs, vec![Duration::ZERO, Duration::from_secs(30), Duration::from_secs(60)]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_retry_looks_again_soon_and_only_while_asked() {
        let (_tx, rx) = broadcast::channel(16);
        let runs = Arc::new(Mutex::new(Vec::new()));
        let body = scripted(runs.clone(), vec![DrainStep::RetryIn(Duration::from_millis(100)), DrainStep::Done]);
        tokio::spawn(run(rx.into(), WAKE, Duration::from_secs(30), "test", body));
        tokio::time::sleep(Duration::from_secs(1)).await;
        let runs = runs.lock().unwrap().clone();
        assert_eq!(runs, vec![Duration::ZERO, Duration::from_millis(100)]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_stopped_watch_ends_the_loop() {
        let (tx, rx) = broadcast::channel::<Heard>(16);
        let runs = Arc::new(Mutex::new(Vec::new()));
        let handle = tokio::spawn(run(rx.into(), WAKE, Duration::from_secs(30), "test", scripted(runs.clone(), vec![])));
        tokio::time::sleep(Duration::from_secs(1)).await;
        drop(tx);
        handle.await.unwrap();
    }
}

#[cfg(test)]
mod plan_tests {
    use super::*;

    #[test]
    fn a_woken_loop_always_runs() {
        assert_eq!(plan(true, Some(100), Some(1_000)), Plan::Run);
        assert_eq!(plan(true, None, None), Plan::Run, "even when the due times could not be read");
    }

    #[test]
    fn a_loop_with_no_row_or_a_row_due_runs_and_one_booked_later_sleeps() {
        assert_eq!(plan(false, Some(100), None), Plan::Run, "no row: due now");
        assert_eq!(plan(false, Some(100), Some(100)), Plan::Run);
        assert_eq!(plan(false, Some(100), Some(50)), Plan::Run);
        assert_eq!(plan(false, Some(100), Some(101)), Plan::SleepUntil(101));
    }

    #[test]
    fn an_unread_due_time_is_unknown() {
        assert_eq!(plan(false, None, Some(1)), Plan::Unknown);
        assert_eq!(plan(false, None, None), Plan::Unknown);
    }

    #[test]
    fn a_due_time_converts_to_an_instant_on_the_reading_it_came_with() {
        let taken = Instant::now();
        assert_eq!(instant_of((1_000, taken), 3_500), taken + Duration::from_millis(2_500));
        assert_eq!(instant_of((1_000, taken), 400), taken, "one already past is due at the reading");
    }
}

#[cfg(all(test, feature = "db-tests"))]
mod drain_due_tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    const NONE: &[WakeOn] = &[];
    const ROLE: &str = "dispatcher";

    async fn schema(pool: &PgPool) {
        crate::apply_groups(pool, &[&GROUP]).await.expect("role_loop_due schema");
    }

    fn counting(name: &'static str, safety: u64) -> (DrainLoop, Arc<AtomicU32>) {
        let runs = Arc::new(AtomicU32::new(0));
        let counter = runs.clone();
        let l = DrainLoop::new(name, NONE, Duration::from_secs(safety), move || {
            counter.fetch_add(1, Ordering::SeqCst);
            async { Ok(DrainStep::Done) }
        });
        (l, runs)
    }

    fn approx(d: Duration, secs: u64) -> bool {
        d <= Duration::from_secs(secs) && d + Duration::from_secs(2) > Duration::from_secs(secs)
    }

    async fn due_ms(pool: &PgPool, name: &str) -> i64 {
        sqlx::query_scalar("SELECT due_ms FROM role_loop_due WHERE role = $1 AND loop_name = $2")
            .bind(ROLE)
            .bind(name)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    #[sqlx::test]
    async fn a_fresh_pass_drains_every_loop_and_answers_the_soonest_look(pool: PgPool) {
        schema(&pool).await;
        let a_runs = Arc::new(AtomicU32::new(0));
        let runs = a_runs.clone();
        let a = DrainLoop::new("a", NONE, Duration::from_secs(60), move || {
            let n = runs.fetch_add(1, Ordering::SeqCst);
            async move { Ok(if n < 2 { DrainStep::More } else { DrainStep::Done }) }
        });
        let b = DrainLoop::new("b", NONE, Duration::from_secs(30), || async { Ok(DrainStep::RetryIn(Duration::from_secs(5))) });
        let c = DrainLoop::new("c", NONE, Duration::from_secs(90), || async { anyhow::bail!("boom") });
        let next = drain_due(&pool, ROLE, &[a, b, c], &[]).await;
        assert_eq!(a_runs.load(Ordering::SeqCst), 3, "More, More, Done");
        assert!(approx(next, 5), "the RetryIn is the soonest: {next:?}");
    }

    /// Two loops that always have more both drain within one pass, each
    /// for its share of it, and both book themselves due now.
    #[sqlx::test]
    async fn loops_that_always_have_more_share_the_pass(pool: PgPool) {
        schema(&pool).await;
        let busy = |name: &'static str| {
            let runs = Arc::new(AtomicU32::new(0));
            let counter = runs.clone();
            let l = DrainLoop::new(name, NONE, Duration::from_secs(60), move || {
                counter.fetch_add(1, Ordering::SeqCst);
                async {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    Ok(DrainStep::More)
                }
            });
            (l, runs)
        };
        let ((first, first_runs), (second, second_runs)) = (busy("first"), busy("second"));
        sqlx::query("INSERT INTO role_loop_due (role, loop_name, due_ms, written_ms) VALUES ($1, 'renamed', 0, 0)").bind(ROLE).execute(&pool).await.unwrap();
        let next = drain_pass(&pool, ROLE, &[first, second], &[], Duration::from_millis(200)).await;
        let left: i64 = sqlx::query_scalar("SELECT count(*) FROM role_loop_due WHERE loop_name = 'renamed'").fetch_one(&pool).await.unwrap();
        assert_eq!(left, 0, "a loop this role no longer has is forgotten");
        book(&pool, ROLE, "another_wefts", Duration::from_secs(60), None).await.unwrap();
        let (fresh, _) = busy("fresh");
        drain_pass(&pool, ROLE, &[fresh], &[], Duration::from_millis(50)).await;
        let kept: i64 = sqlx::query_scalar("SELECT count(*) FROM role_loop_due WHERE loop_name = 'another_wefts'").fetch_one(&pool).await.unwrap();
        assert_eq!(kept, 1, "a loop another weft booked lately is left to it");
        assert!(first_runs.load(Ordering::SeqCst) > 1 && second_runs.load(Ordering::SeqCst) > 1, "both drained");
        assert!(next < Duration::from_secs(1), "and both are due again at once: {next:?}");
        let now: i64 = sqlx::query_scalar(&format!("SELECT {DB_NOW_MS}")).fetch_one(&pool).await.unwrap();
        assert!(due_ms(&pool, "first").await <= now && due_ms(&pool, "second").await <= now);
    }

    /// A loop with nothing left to watch is not looked at again until a
    /// write wakes it or the idle look comes, so a quiet install wakes
    /// nothing; one that failed is looked at again at its safety.
    #[sqlx::test]
    async fn a_quiet_pass_sleeps_the_idle_look_and_a_failed_one_its_safety(pool: PgPool) {
        schema(&pool).await;
        let (quiet, _) = counting("quiet", 30);
        assert!(approx(drain_due(&pool, ROLE, &[quiet], &[]).await, IDLE_LOOK.as_secs()));
        let failing = DrainLoop::new("failing", NONE, Duration::from_secs(90), || async { anyhow::bail!("boom") });
        assert!(approx(drain_due(&pool, ROLE, &[failing], &[]).await, 90));
    }

    /// The due times are the database's, not a process's: a pass started
    /// fresh (any instance the tick lands on) sees what an earlier pass
    /// booked, and a pass for one woken loop leaves the others asleep
    /// until they are due, so a write never sets off every sweep.
    #[sqlx::test]
    async fn a_woken_pass_runs_the_woken_loop_and_leaves_the_rest_until_due(pool: PgPool) {
        schema(&pool).await;
        let (hourly, hourly_runs) = counting("hourly", 3600);
        let picker_runs = Arc::new(AtomicU32::new(0));
        let counter = picker_runs.clone();
        let picker = DrainLoop::new("picker", NONE, Duration::from_secs(30), move || {
            counter.fetch_add(1, Ordering::SeqCst);
            async { Ok(DrainStep::RetryIn(Duration::from_secs(30))) }
        });
        let loops = [hourly, picker];
        drain_due(&pool, ROLE, &loops, &[]).await;
        let next = drain_due(&pool, ROLE, &loops, &["picker".to_string()]).await;
        assert_eq!(hourly_runs.load(Ordering::SeqCst), 1, "not due again until the idle look");
        assert_eq!(picker_runs.load(Ordering::SeqCst), 2, "woken");
        assert!(approx(next, 30), "{next:?}");
        drain_due(&pool, ROLE, &loops, &[]).await;
        assert_eq!(picker_runs.load(Ordering::SeqCst), 2, "nothing woken, nothing due");
        let (other_role, other_runs) = counting("hourly", 3600);
        drain_due(&pool, "broker", &[other_role], &[]).await;
        assert_eq!(other_runs.load(Ordering::SeqCst), 1, "another role's loop of the same name has its own row");
    }

    /// A pass that drained a loop replaces its booking when nobody wrote
    /// the row since the pass read it, and keeps a sooner look a sibling
    /// booked in between, so the sibling's wake is never pushed back.
    #[sqlx::test]
    async fn a_booking_keeps_a_sooner_look_a_sibling_wrote_since_the_read(pool: PgPool) {
        schema(&pool).await;
        let soon = Duration::from_secs(10);
        let later = Duration::from_secs(3600);

        // Nobody wrote since the read: the new booking stands, even later.
        book(&pool, ROLE, "x", soon, None).await.unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;
        let read_at = read_due(&pool, ROLE).await.unwrap().now;
        let (due, now) = book(&pool, ROLE, "x", later, Some(read_at)).await.unwrap();
        assert_eq!(due, now + 3_600_000);
        assert_eq!(due_ms(&pool, "x").await, due);

        // A sibling booked a sooner look after this pass read: it stands.
        let read_at = read_due(&pool, ROLE).await.unwrap().now;
        tokio::time::sleep(Duration::from_millis(5)).await;
        let (sibling_due, _) = book(&pool, ROLE, "x", soon, None).await.unwrap();
        let (due, _) = book(&pool, ROLE, "x", later, Some(read_at)).await.unwrap();
        assert_eq!(due, sibling_due, "the sibling's sooner look is kept");
    }
}
