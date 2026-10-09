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
//!   on the process's one `LISTEN` connection, and looking anyway at its
//!   safety interval ([`Looks::AtSafety`]);
//! - as a service that scales to zero, [`drain_due`] once per tick: the
//!   role is called (by the process that wrote what a loop waits on,
//!   naming the loops it concerns, or by its own next alarm), drains those
//!   loops and the ones due, and answers when the next look is due. A
//!   process at zero hears no notification, so this is the only way its
//!   loops run there. A tick may land on any instance of the role, so
//!   when each loop is due lives in the database (the `role_loop_due`
//!   table, [`GROUP`]), shared by them all. A process of the role that is
//!   up and hears the database also runs its loops for as long as it
//!   lives, looking on their safety interval only while it has work
//!   ([`Looks::WhileWorking`]): its timed looks are the ticks'.
//!
//! Scaled to zero, nothing is looked at "just in case": a loop with
//! nothing left sleeps until a write wakes it, and only a look a loop
//! asked for ([`DrainStep::RetryIn`]) books a wake. A loop no write wakes
//! (a sweep for what nobody announces) runs again once its safety
//! interval has passed, on whichever pass the role makes next, and books
//! no wake of its own. So an install with nothing going on wakes nothing,
//! and its database sleeps.
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

use crate::pg_signal::{PgSignalWatch, Subscription};

/// The usual safety net for missed notifications while a local install's
/// process is up, and how soon a failed drain is tried again anywhere. Notifications are
/// best-effort by Postgres design (a reconnecting listener can lose
/// some, though it says so with a recheck), so this only catches what
/// slipped past; 30s of delay on a lost one is acceptable, and a tighter
/// tick would hammer the DB for nothing.
pub const SAFETY_POLL_INTERVAL: Duration = Duration::from_secs(30);

/// How long a loop's row in `role_loop_due` stays once no pass of this
/// weft runs that loop: a loop renamed or dropped by a later weft leaves
/// its row behind, and until this long has passed it may be a loop of
/// another weft still taking ticks beside this one (a deploy rolling out).
const FORGET_AFTER: Duration = Duration::from_secs(24 * 3600);

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
    /// safety interval; scaled to zero, until woken, see the module docs).
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
    pub async fn run_forever(&self, signals: Subscription, looks: Looks) {
        let drain = self.drain.clone();
        run(signals, self.wake_on, self.safety, looks, self.name, move || drain()).await
    }

    /// Whether a write wakes this loop, or only time does (a sweep for
    /// what nobody announces).
    pub fn woken_by_writes(&self) -> bool {
        !self.wake_on.is_empty()
    }
}

/// How a loop run for the life of its process ([`run`]) looks when
/// nothing wakes it.
#[derive(Clone)]
pub enum Looks {
    /// At its safety interval: a machine's process, up for good.
    AtSafety,
    /// At its safety interval only while the process has work
    /// ([`PgSignalWatch::worked_since`]): a process of a role that scales
    /// to zero, left idle, queries nothing, and its timed looks are the
    /// role's ticks' ([`drain_due`]). Each drain holds the watch
    /// ([`PgSignalWatch::looking`]), so it listens while the drain runs.
    WhileWorking(std::sync::Arc<PgSignalWatch>),
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
            -- still agree on it. Empty while it has nothing to look at
            -- until a write wakes it (or, for a loop no write wakes, until
            -- its safety interval has passed since `written_ms`, on the
            -- role's next pass).
            due_ms BIGINT,
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
    /// Leave it until a write wakes it (or, for a loop no write wakes,
    /// until a later pass finds its safety interval passed).
    Asleep,
    /// The due times could not be read: leave it, and look again at its
    /// safety interval.
    Unknown,
}

/// A loop's row in `role_loop_due`, as a pass reads it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Booked {
    due_ms: Option<i64>,
    written_ms: i64,
}

/// What a pass that read the due times at `read_at` (`None` when the read
/// failed) does with a loop booked as `booked` (`None` with no row). A
/// woken loop always runs; one with no row is due now. `sweeps_every` is
/// the safety interval of a loop no write wakes, which runs again once it
/// has passed since the loop was last booked; `None` for a loop a write
/// wakes, which sleeps until then.
fn plan(woken: bool, read_at: Option<i64>, booked: Option<Booked>, sweeps_every: Option<Duration>) -> Plan {
    if woken {
        return Plan::Run;
    }
    let Some(now) = read_at else { return Plan::Unknown };
    match booked {
        None => Plan::Run,
        Some(Booked { due_ms: Some(at), .. }) if at > now => Plan::SleepUntil(at),
        Some(Booked { due_ms: Some(_), .. }) => Plan::Run,
        Some(Booked { due_ms: None, written_ms }) => match sweeps_every {
            Some(every) if written_ms.saturating_add(millis(every)) <= now => Plan::Run,
            _ => Plan::Asleep,
        },
    }
}

/// `d` in whole milliseconds, saturating.
fn millis(d: Duration) -> i64 {
    i64::try_from(d.as_millis()).unwrap_or(i64::MAX)
}

/// `at_ms` on the database's clock as an instant of this process, given
/// `clock`: a reading of the database's clock and the instant it was
/// taken at.
fn instant_of(clock: (i64, Instant), at_ms: i64) -> Instant {
    clock.1 + Duration::from_millis(at_ms.saturating_sub(clock.0).max(0).unsigned_abs())
}

/// What a pass reads of `role`'s due times: the database's clock, each
/// loop's row, and the loops nobody has booked for [`FORGET_AFTER`].
struct ReadDue {
    now: i64,
    booked: HashMap<String, Booked>,
    unbooked: Vec<String>,
}

/// One row of [`read_due`]'s query: the clock, then a loop's name, when
/// it is due, when it was written, and whether nobody has booked it for
/// [`FORGET_AFTER`] (all four `None` when the role has no row).
type DueRow = (i64, Option<String>, Option<i64>, Option<i64>, Option<bool>);

async fn read_due(pool: &PgPool, role: &str) -> Result<ReadDue> {
    let rows: Vec<DueRow> = sqlx::query_as(&format!(
        "SELECT c.now_ms, d.loop_name, d.due_ms, d.written_ms, d.written_ms < c.now_ms - $2 \
         FROM (SELECT {DB_NOW_MS} AS now_ms) c \
         LEFT JOIN role_loop_due d ON d.role = $1"
    ))
    .bind(role)
    .bind(millis(FORGET_AFTER))
    .fetch_all(pool)
    .await?;
    let now = rows.first().map(|r| r.0).context("the clock query returned no row")?;
    let unbooked = rows.iter().filter(|r| r.4 == Some(true)).filter_map(|r| r.1.clone()).collect();
    let booked = rows
        .into_iter()
        .filter_map(|(_, name, due_ms, written_ms, _)| Some((name?, Booked { due_ms, written_ms: written_ms? })))
        .collect();
    Ok(ReadDue { now, booked, unbooked })
}

/// Book `role`'s loop `name` for a look `after` from now on the database's
/// clock (`None`: asleep until woken), and answer when it is now due and
/// the clock at the write. When a sibling wrote the row since this pass
/// read it at `read_at` (or the read failed), the sooner of the two looks
/// stands, so a wake the sibling booked is never pushed back (an empty
/// due is the latest of all). A row written in the very millisecond of the
/// read counts as a sibling's: keeping the sooner look costs one extra
/// pass at most, overwriting it could cost a wake.
async fn book(pool: &PgPool, role: &str, name: &str, after: Option<Duration>, read_at: Option<i64>) -> Result<(Option<i64>, i64)> {
    let after_ms = after.map(|after| i64::try_from(after.as_millis()).context("a look further out than the clock reaches")).transpose()?;
    let row: (Option<i64>, i64) = sqlx::query_as(&format!(
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
/// look in `role_loop_due`, and answer how soon any loop of `role` next
/// wants a look, or `None` when none does. The others stay asleep, so a
/// pass run because a journal event was written does not also run the
/// hourly sweeps. A loop with nothing left sleeps until woken (see the
/// module docs); one that asked to look again does so then; a drain that
/// fails is logged and, when a write would wake it, looked at again at
/// its safety interval (a sweep that fails runs again on a later pass
/// like one that did not). When the due times cannot be read or written,
/// that is logged too, and the loops it leaves unknown are looked at again
/// at their safety interval.
pub async fn drain_due(pool: &PgPool, role: &str, loops: &[DrainLoop], woken: &[String]) -> Option<Duration> {
    drain_pass(pool, role, loops, woken, TICK_SLICE).await
}

/// [`drain_due`] with the pass's draining time as `slice`.
async fn drain_pass(pool: &PgPool, role: &str, loops: &[DrainLoop], woken: &[String], slice: Duration) -> Option<Duration> {
    let (read_at, booked, unbooked) = match read_due(pool, role).await {
        Ok(read) => (Some(read.now), read.booked, read.unbooked),
        Err(e) => {
            tracing::warn!(
                target: "weft_task_store::drain", role, error = %format!("{e:#}"),
                "could not read when the loops are due; running only the woken ones, the rest look again at their safety interval"
            );
            (None, HashMap::new(), Vec::new())
        }
    };
    // A loop renamed or dropped by a later weft leaves its row behind,
    // forgotten once nobody has booked it for `FORGET_AFTER`.
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
    let plans: Vec<Plan> = loops
        .iter()
        .map(|l| {
            let sweeps_every = (!l.woken_by_writes()).then_some(l.safety);
            plan(woken.iter().any(|w| w == l.name), read_at, booked.get(l.name).copied(), sweeps_every)
        })
        .collect();
    let share = slice / u32::try_from(plans.iter().filter(|p| **p == Plan::Run).count().max(1)).unwrap_or(u32::MAX);
    for (l, plan) in loops.iter().zip(plans) {
        let look = match plan {
            Plan::SleepUntil(at) => Some(instant_of(clock.expect("a planned sleep comes from a read clock"), at)),
            Plan::Asleep => None,
            Plan::Unknown => Some(Instant::now() + l.safety),
            Plan::Run => {
                let started = Instant::now();
                let after = loop {
                    match (l.drain)().await {
                        Ok(DrainStep::More) if started.elapsed() >= share => break Some(Duration::ZERO),
                        Ok(DrainStep::More) => continue,
                        Ok(DrainStep::Done) => break None,
                        Ok(DrainStep::RetryIn(after)) => break Some(after),
                        Err(e) => {
                            tracing::warn!(target: "weft_task_store::drain", subsystem = l.name, error = %e, "drain failed; will retry at its next look");
                            break l.woken_by_writes().then_some(l.safety);
                        }
                    }
                };
                match book(pool, role, l.name, after, read_at).await {
                    Ok((due_ms, now)) => {
                        let at = (now, Instant::now());
                        clock = Some(at);
                        due_ms.map(|due_ms| instant_of(at, due_ms))
                    }
                    Err(e) => {
                        tracing::warn!(
                            target: "weft_task_store::drain", role, subsystem = l.name, error = %format!("{e:#}"),
                            "could not book the loop's next look; looking again at its safety interval"
                        );
                        Some(Instant::now() + after.map_or(l.safety, |after| after.min(l.safety)))
                    }
                }
            }
        };
        if let Some(look) = look {
            next = Some(next.map_or(look, |n| n.min(look)));
        }
    }
    next.map(|n| n.saturating_duration_since(Instant::now()))
}

/// Drive the drain loop forever: drain once at start (rows that landed
/// before the loop subscribed), then after every wake, every `RetryIn`,
/// and every `safety` interval (only when the process had work since the
/// last look, for [`Looks::WhileWorking`]). Returns only when the
/// process's signal watch stops, since nothing could wake the loop again;
/// the caller's supervisor crashes the process on that.
///
/// `signals` must be subscribed before the call, and `target` is the
/// tracing target, so subsystems log under their own module name.
pub async fn run<F, Fut>(
    mut signals: Subscription,
    wake_on: &[WakeOn],
    safety: Duration,
    looks: Looks,
    target: &'static str,
    mut drain: F,
) where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<DrainStep>>,
{
    // A look the drain asked for (the first one: at once), and the next
    // look on the safety interval.
    let mut asked = Some(Instant::now());
    let mut sweep = Instant::now() + safety;
    let mut last_look = Instant::now();
    loop {
        let deadline = asked.map_or(sweep, |at| at.min(sweep));
        // A loop that listens to no channel runs on its interval alone: a
        // recheck (the watch fell behind, or reconnected) concerns only the
        // loops that wait on a channel, and waking a timed sweep on one
        // would run it back to back while notifications pour in.
        let woken = if Instant::now() >= deadline {
            false
        } else if wake_on.is_empty() {
            tokio::time::sleep_until(deadline).await;
            false
        } else {
            match signals.woken_before(deadline, |channel, payload| wake_on.iter().any(|w| w.hears(channel, payload))).await {
                Ok(woken) => woken,
                Err(e) => {
                    tracing::error!(target: "weft_task_store::drain", subsystem = target, error = %e, "cannot be woken any more");
                    return;
                }
            }
        };
        let now = Instant::now();
        // The safety look alone, in a process that scales to zero: skipped
        // while the process has had no work, so an idle one queries nothing.
        if !woken && asked.is_none_or(|at| at > now) {
            if let Looks::WhileWorking(watch) = &looks {
                if !watch.worked_since(last_look) {
                    sweep = now + safety;
                    continue;
                }
            }
        }
        // Everything heard up to here is covered by the drain that
        // follows, so a burst of notifications costs one drain, not one
        // per notification.
        if let Err(e) = signals.clear() {
            tracing::error!(target: "weft_task_store::drain", subsystem = target, error = %e, "cannot be woken any more");
            return;
        }
        last_look = now;
        sweep = now + safety;
        asked = None;
        let looking = match &looks {
            Looks::AtSafety => None,
            Looks::WhileWorking(watch) => Some(watch.looking().await),
        };
        // Drain until the body reports it has nothing left. This is what
        // makes a burst of more rows than one batch finish in one wake.
        loop {
            match drain().await {
                Ok(DrainStep::More) => continue,
                Ok(DrainStep::Done) => break,
                Ok(DrainStep::RetryIn(after)) => {
                    asked = Some(Instant::now() + after);
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
        drop(looking);
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
        tokio::spawn(run(rx.into(), WAKE, Duration::from_secs(30), Looks::AtSafety, "test", body));
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(runs.lock().unwrap().len(), 3, "More, More, Done");
    }

    #[tokio::test(start_paused = true)]
    async fn a_wake_that_concerns_it_drains_and_one_that_does_not_is_ignored() {
        let (tx, rx) = broadcast::channel(16);
        let runs = Arc::new(Mutex::new(Vec::new()));
        tokio::spawn(run(rx.into(), WAKE, Duration::from_secs(30), Looks::AtSafety, "test", scripted(runs.clone(), vec![])));
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
        tokio::spawn(run(rx.into(), WAKE, Duration::from_secs(30), Looks::AtSafety, "test", scripted(runs.clone(), vec![])));
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
        tokio::spawn(run(rx.into(), &[], Duration::from_secs(30), Looks::AtSafety, "test", scripted(runs.clone(), vec![])));
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
        tokio::spawn(run(rx.into(), WAKE, Duration::from_secs(30), Looks::AtSafety, "test", scripted(runs.clone(), vec![])));
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
        tokio::spawn(run(rx.into(), WAKE, Duration::from_secs(30), Looks::AtSafety, "test", scripted(runs.clone(), vec![])));
        tokio::time::sleep(Duration::from_secs(61)).await;
        let runs = runs.lock().unwrap().clone();
        assert_eq!(runs, vec![Duration::ZERO, Duration::from_secs(30), Duration::from_secs(60)]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_retry_looks_again_soon_and_only_while_asked() {
        let (_tx, rx) = broadcast::channel(16);
        let runs = Arc::new(Mutex::new(Vec::new()));
        let body = scripted(runs.clone(), vec![DrainStep::RetryIn(Duration::from_millis(100)), DrainStep::Done]);
        tokio::spawn(run(rx.into(), WAKE, Duration::from_secs(30), Looks::AtSafety, "test", body));
        tokio::time::sleep(Duration::from_secs(1)).await;
        let runs = runs.lock().unwrap().clone();
        assert_eq!(runs, vec![Duration::ZERO, Duration::from_millis(100)]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_stopped_watch_ends_the_loop() {
        let (tx, rx) = broadcast::channel::<Heard>(16);
        let runs = Arc::new(Mutex::new(Vec::new()));
        let handle = tokio::spawn(run(rx.into(), WAKE, Duration::from_secs(30), Looks::AtSafety, "test", scripted(runs.clone(), vec![])));
        tokio::time::sleep(Duration::from_secs(1)).await;
        drop(tx);
        handle.await.unwrap();
    }
}

#[cfg(test)]
mod plan_tests {
    use super::*;

    fn due(at: i64) -> Option<Booked> {
        Some(Booked { due_ms: Some(at), written_ms: 0 })
    }

    fn asleep(written_ms: i64) -> Option<Booked> {
        Some(Booked { due_ms: None, written_ms })
    }

    const SWEEP: Option<Duration> = Some(Duration::from_millis(50));

    #[test]
    fn a_woken_loop_always_runs() {
        assert_eq!(plan(true, Some(100), due(1_000), None), Plan::Run);
        assert_eq!(plan(true, Some(100), asleep(100), None), Plan::Run);
        assert_eq!(plan(true, None, None, None), Plan::Run, "even when the due times could not be read");
    }

    #[test]
    fn a_loop_with_no_row_or_a_row_due_runs_and_one_booked_later_sleeps() {
        assert_eq!(plan(false, Some(100), None, None), Plan::Run, "no row: due now");
        assert_eq!(plan(false, Some(100), due(100), None), Plan::Run);
        assert_eq!(plan(false, Some(100), due(50), None), Plan::Run);
        assert_eq!(plan(false, Some(100), due(101), None), Plan::SleepUntil(101));
    }

    /// A loop with nothing left sleeps until a write wakes it, however
    /// long ago it was booked; a sweep no write wakes runs again on the
    /// first pass after its interval, and never sooner.
    #[test]
    fn an_asleep_loop_waits_for_a_write_and_a_sweep_for_its_interval() {
        assert_eq!(plan(false, Some(1_000_000), asleep(0), None), Plan::Asleep);
        assert_eq!(plan(false, Some(100), asleep(60), SWEEP), Plan::Asleep);
        assert_eq!(plan(false, Some(110), asleep(60), SWEEP), Plan::Run);
        assert_eq!(plan(false, Some(100), due(150), SWEEP), Plan::SleepUntil(150), "a look it asked for stands");
    }

    #[test]
    fn an_unread_due_time_is_unknown() {
        assert_eq!(plan(false, None, due(1), None), Plan::Unknown);
        assert_eq!(plan(false, None, None, SWEEP), Plan::Unknown);
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
    const WRITES: &[WakeOn] = &[WakeOn::any("chan")];
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

    async fn due_ms(pool: &PgPool, name: &str) -> Option<i64> {
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
        assert!(approx(next.expect("a look is due"), 5), "the RetryIn is the soonest: {next:?}");
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
        book(&pool, ROLE, "another_wefts", Some(Duration::from_secs(60)), None).await.unwrap();
        let (fresh, _) = busy("fresh");
        drain_pass(&pool, ROLE, &[fresh], &[], Duration::from_millis(50)).await;
        let kept: i64 = sqlx::query_scalar("SELECT count(*) FROM role_loop_due WHERE loop_name = 'another_wefts'").fetch_one(&pool).await.unwrap();
        assert_eq!(kept, 1, "a loop another weft booked lately is left to it");
        assert!(first_runs.load(Ordering::SeqCst) > 1 && second_runs.load(Ordering::SeqCst) > 1, "both drained");
        assert!(next.expect("a look is due") < Duration::from_secs(1), "and both are due again at once: {next:?}");
        let now: i64 = sqlx::query_scalar(&format!("SELECT {DB_NOW_MS}")).fetch_one(&pool).await.unwrap();
        assert!(due_ms(&pool, "first").await.unwrap() <= now && due_ms(&pool, "second").await.unwrap() <= now);
    }

    /// A quiet install books no wake at all: a loop with nothing left
    /// sleeps until a write wakes it, and a sweep (which no write wakes)
    /// waits for a pass that happens anyway. Only a loop a write wakes that
    /// failed is looked at again, at its safety interval.
    #[sqlx::test]
    async fn a_quiet_pass_books_no_wake_and_a_failed_one_its_safety(pool: PgPool) {
        schema(&pool).await;
        let quiet = DrainLoop::new("quiet", WRITES, Duration::from_secs(30), || async { Ok(DrainStep::Done) });
        let (sweep, _) = counting("sweep", 30);
        assert_eq!(drain_due(&pool, ROLE, &[quiet.clone(), sweep.clone()], &[]).await, None);
        assert_eq!(due_ms(&pool, "quiet").await, None, "asleep until woken");
        assert_eq!(drain_due(&pool, ROLE, &[quiet, sweep], &[]).await, None, "and a pass right after runs nothing");
        let failing_sweep = DrainLoop::new("failing_sweep", NONE, Duration::from_secs(90), || async { anyhow::bail!("boom") });
        assert_eq!(drain_due(&pool, ROLE, &[failing_sweep], &[]).await, None, "a sweep that failed runs on a later pass");
        let failing = DrainLoop::new("failing", WRITES, Duration::from_secs(90), || async { anyhow::bail!("boom") });
        assert!(approx(drain_due(&pool, ROLE, &[failing], &[]).await.expect("a look is due"), 90));
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
        assert_eq!(hourly_runs.load(Ordering::SeqCst), 1, "not due again until its interval");
        assert_eq!(picker_runs.load(Ordering::SeqCst), 2, "woken");
        assert!(approx(next.expect("a look is due"), 30), "{next:?}");
        drain_due(&pool, ROLE, &loops, &[]).await;
        assert_eq!(picker_runs.load(Ordering::SeqCst), 2, "nothing woken, nothing due");
        let (other_role, other_runs) = counting("hourly", 3600);
        drain_due(&pool, "broker", &[other_role], &[]).await;
        assert_eq!(other_runs.load(Ordering::SeqCst), 1, "another role's loop of the same name has its own row");
    }

    /// In a process that scales to zero, a loop run for the life of the
    /// process drains at start and when a write wakes it, and on its
    /// safety interval only once the process had work since: a process
    /// left idle queries nothing.
    #[sqlx::test]
    async fn a_loop_looks_on_its_interval_only_while_its_process_works(pool: PgPool) {
        let watch = PgSignalWatch::start(&pool.connect_options(), &["chan"], crate::pg_signal::Listening::WhileBusy).await.unwrap();
        let runs = Arc::new(AtomicU32::new(0));
        let counter = runs.clone();
        let l = DrainLoop::new("woken", WRITES, Duration::from_millis(200), move || {
            counter.fetch_add(1, Ordering::SeqCst);
            async { Ok(DrainStep::Done) }
        });
        let subscription = watch.subscribe();
        let looks = Looks::WhileWorking(watch.clone());
        tokio::spawn(async move { l.run_forever(subscription, looks).await });
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(runs.load(Ordering::SeqCst), 1, "the start drain, and no safety look since");
        sqlx::query("SELECT pg_notify('chan', '')").execute(&pool).await.unwrap();
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(runs.load(Ordering::SeqCst), 2, "a write wakes it");
        drop(watch.busy().await);
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(runs.load(Ordering::SeqCst), 3, "work since: the safety look runs");
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(runs.load(Ordering::SeqCst), 3, "and none after it while nothing works");
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
        book(&pool, ROLE, "x", Some(soon), None).await.unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;
        let read_at = read_due(&pool, ROLE).await.unwrap().now;
        let (due, now) = book(&pool, ROLE, "x", Some(later), Some(read_at)).await.unwrap();
        assert_eq!(due, Some(now + 3_600_000));
        assert_eq!(due_ms(&pool, "x").await, due);

        // A sibling booked a sooner look after this pass read: it stands,
        // even over a pass that found nothing left.
        let read_at = read_due(&pool, ROLE).await.unwrap().now;
        tokio::time::sleep(Duration::from_millis(5)).await;
        let (sibling_due, _) = book(&pool, ROLE, "x", Some(soon), None).await.unwrap();
        let (asleep, _) = book(&pool, ROLE, "x", None, Some(read_at)).await.unwrap();
        assert_eq!(asleep, sibling_due, "the sibling's look outlasts an empty booking");
        let (due, _) = book(&pool, ROLE, "x", Some(later), Some(read_at)).await.unwrap();
        assert_eq!(due, sibling_due, "the sibling's sooner look is kept");
    }
}
