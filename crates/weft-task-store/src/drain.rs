//! The wake-and-drain loop every role's background work runs.
//!
//! The loops that use it (the dispatcher's task picker, its delivery of
//! runs, the lifecycle claimer, the reapers; the broker's expiry of
//! stored files) all have the same shape: sleep until a write they care
//! about is announced on the process's [`PgSignalWatch`] (see
//! [`crate::pg_signal`]), then run their drain body until it reports the
//! queue empty, or that it needs another look at a given moment (a lease
//! that may lapse, a delay that runs out).
//!
//! A role runs its loops one of two ways, by where it is placed:
//!
//! - in a local install's one process, [`run_forever`](DrainLoop::run_forever)
//!   per loop, sleeping on the process's one `LISTEN` connection, and
//!   looking anyway at its safety interval ([`Looks::AtSafety`]), for the
//!   notification that was lost.
//! - as a service that scales to zero, nothing is looked at "just in
//!   case". Each loop is either rung (the process that wrote what it waits
//!   on calls the role's tick naming it) or booked: when a pass leaves a
//!   loop with a look to come, the moment goes into the `role_loop_due`
//!   table ([`GROUP`]), shared by every instance of the role, with one
//!   wake on the platform's alarm that calls the role's tick then
//!   ([`Booking`]). A booking that moves sets the new wake before it
//!   commits and cancels the old one after, so a crash leaves at most one
//!   stray wake, never a booking without one. A tick runs the loops it was
//!   rung for, the loop its wake was booked for, and any loop already due
//!   ([`drain_due`]). An instance that is up and hears the database also
//!   runs its loops for as long as it lives, woken by what it hears
//!   ([`Looks::WhileWorking`]); it keeps no timer and books nothing, since
//!   the tick rung for the same write books what is left.
//!   A loop nothing rings and nothing books would never run again there,
//!   so a role refuses to start with one ([`check_woken`]).
//!
//! A loop whose drain fails books its next look a little later, a few
//! times ([`RETRY_PAUSES`]); past the last it books nothing, keeps no look
//! already past (which would run it again on every pass of its role), and
//! is written down with its error (`crate::unanswered`, for `weft
//! status`): it runs again when a write that concerns it rings it, or once
//! as a process of its role starts ([`Pass::Start`]), and its next drain
//! that goes through clears what was written. A process starts only when
//! something real woke the role, so that try keeps no database awake; it
//! is what brings back a loop that failed on a fresh cloud install while
//! Google was still applying the install's permissions. The try keeps the
//! loop's count of failures: a loop that spent its retries books none when
//! it fails again, and is written down as before, while one written down
//! only for a wake that could not be set has spent none, and fails into
//! them like any loop, each failing retry writing its error over the one
//! shown. A next look whose
//! wake cannot be set is tried a few times in the pass, then written down
//! the same way, the booking left as it was. Either way the pass answers
//! its tick as done. Only a pass that could not write this down for some
//! loop (the database did not answer it) fails its tick, naming those
//! loops ([`PassFailed`]), so the platform's queue calls it again for as
//! long as its retry window allows. When that tick was a writer's ring,
//! the writer writes the failure down for the loops it names
//! ([`pass_failed`]), through the same database. While that database does
//! not answer, the writer cannot write it down either, and only logs:
//! what carries those loops then is the ring's own retry (the alarm calls
//! the tick again with them), and the first of those calls the database
//! answers runs them, each one that fails then written down by its own
//! booking. A pass that could not even read when its loops are due names
//! every loop its call names, or every loop of the role for a call naming
//! none, so a short blip the writer does get through writes all of those
//! down, each until its next drain that goes through.
//!
//! The subsystem provides its drain body, which channels wake it, and its
//! safety interval; the coalescing, the booking and the timing live here.
//!
//! [`PgSignalWatch`]: crate::pg_signal::PgSignalWatch

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tokio::time::Instant;
use weft_platform_traits::roles::TICK_PATH;
use weft_platform_traits::{Alarm, CoreRole, Wake, WakeQueue};

use crate::db::{Client as _, Pool};
use crate::pg_signal::{Heard, PgSignalWatch, Subscription};
use crate::unanswered::{self, Callee};

/// How often a local install's process looks at a loop anyway, for the
/// notification that was lost. Notifications are best-effort by Postgres
/// design (a reconnecting listener can lose some, though it says so with a
/// recheck), so this only catches what slipped past; 30s of delay on a
/// lost one is acceptable, and a tighter tick would hammer the DB for
/// nothing. In real time, like every loop's safety interval
/// ([`DrainLoop::new`] scales it).
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

/// How many times a booking tries to set its wake before it gives up and
/// writes the failure down.
const SET_TRIES: u32 = 3;

/// How long a loop whose drain failed waits before each of its retries,
/// booked like any look, by how many of its drains in a row failed before
/// this one. Past the last it books nothing and is written down.
const RETRY_PAUSES: [Duration; 3] = [Duration::from_secs(1), Duration::from_secs(5), Duration::from_secs(20)];

/// What a subsystem's drain body returns after one iteration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrainStep {
    /// More work likely remains; the runner re-invokes the body
    /// without waiting. Use when an iteration hit a row-limit or
    /// otherwise expects siblings behind it.
    More,
    /// Nothing is left that a look before the next write would find:
    /// the runner sleeps until a write wakes it (on a local install, at
    /// most its safety interval).
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

/// One background loop of a role: what wakes it, how often a local
/// install looks anyway, and its drain body.
#[derive(Clone)]
pub struct DrainLoop {
    /// For logs, the key its install-wide lock (when it takes one) is
    /// named by, and its row in `role_loop_due` ([`GROUP`]).
    pub name: &'static str,
    /// What the loop does, in a person's words, finishing "weft's
    /// dispatcher could not ...": what `weft status` says when it gave up.
    pub label: &'static str,
    pub wake_on: &'static [WakeOn],
    /// The other loops of the role that book this one's looks
    /// ([`Booking::book_sooner`]), for a loop no write wakes.
    pub booked_by: &'static [&'static str],
    /// How often a local install's process looks anyway, at this install's
    /// pace (see [`DrainLoop::new`]).
    pub safety: Duration,
    pub drain: Arc<dyn Fn() -> futures::future::BoxFuture<'static, Result<DrainStep>> + Send + Sync>,
}

impl DrainLoop {
    /// `safety` is given in real time, and the loop runs it at this
    /// install's pace (`weft_core::time_scale::scaled`): a loop's safety
    /// interval is always the runtime's own housekeeping, so it is scaled
    /// here, once, for every loop.
    pub fn new<F, Fut>(name: &'static str, label: &'static str, wake_on: &'static [WakeOn], safety: Duration, drain: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<DrainStep>> + Send + 'static,
    {
        let safety = weft_core::time_scale::scaled(safety);
        Self { name, label, wake_on, booked_by: &[], safety, drain: Arc::new(move || Box::pin(drain())) }
    }

    /// This loop's looks are booked by `loops`, other loops of its role.
    pub fn booked_by(mut self, loops: &'static [&'static str]) -> Self {
        self.booked_by = loops;
        self
    }

    /// Run this loop for the life of the process (see [`run`]).
    pub async fn run_forever(&self, signals: Subscription, looks: Looks) {
        let drain = self.drain.clone();
        run(signals, self.wake_on, self.safety, looks, self.name, move || drain()).await
    }
}

/// Refuse to run `role`'s `loops` as a service that scales to zero when
/// one of them would never run again there: one no write wakes and no
/// other loop books, or booked by a loop the role does not have.
pub fn check_woken(role: CoreRole, loops: &[DrainLoop]) -> Result<()> {
    for l in loops {
        anyhow::ensure!(
            !l.wake_on.is_empty() || !l.booked_by.is_empty(),
            "the {role}'s loop '{}' is woken by no write and booked by no other loop, so scaled to zero it would never run again",
            l.name
        );
        for by in l.booked_by {
            anyhow::ensure!(
                loops.iter().any(|other| other.name == *by),
                "the {role}'s loop '{}' says '{by}' books it, which is no loop of the {role}",
                l.name
            );
        }
    }
    Ok(())
}

/// How a loop run for the life of its process ([`run`]) looks when
/// nothing wakes it.
#[derive(Clone)]
pub enum Looks {
    /// At its safety interval: a machine's process, up for good.
    AtSafety,
    /// Only when a write wakes it: a process of a role that scales to
    /// zero, whose looks are the role's ticks' (see [`run`]). Each drain
    /// holds the watch ([`PgSignalWatch::busy`]), so it listens while the
    /// drain runs.
    WhileWorking(Arc<PgSignalWatch>),
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
            -- until a write wakes it.
            due_ms BIGINT,
            -- When this row was last written, on the same clock: how a
            -- pass tells whether a sibling booked a look since it read.
            written_ms BIGINT NOT NULL,
            -- How many bookings this row has had: the generation of the
            -- wake set for `due_ms`, so a wake set again never reuses the
            -- name of one delivered or cancelled.
            seq BIGINT NOT NULL DEFAULT 0,
            -- How many times a tick the wake of booking `seq` arrived
            -- early at set it again (`BookedLook::set_again`): the wake
            -- of this booking still waiting is the one under that count,
            -- which a booking that moves cancels.
            set_again INT NOT NULL DEFAULT 0,
            -- How many of the loop's drains in a row failed: which of its
            -- retries comes next (`RETRY_PAUSES`).
            failures INT NOT NULL DEFAULT 0,
            -- Whether a failure of the loop is written down for `weft
            -- status` (`unanswered_call`): its next drain that goes
            -- through clears it.
            failure_written BOOLEAN NOT NULL DEFAULT FALSE,
            PRIMARY KEY (role, loop_name)
        )"#],
    seed: &[],
    data_steps: &[],
};

/// The database's clock, in unix milliseconds. Every due time is read and
/// written on it, never on a process's own clock.
pub const DB_NOW_MS: &str = "(extract(epoch from clock_timestamp()) * 1000)::bigint";

/// The body of a role's tick wake ([`weft_platform_traits::WakeCall`]'s
/// `body`): what the tick runs besides the loops already due. A tick with
/// no body (a ring, a role's first pass) runs what its query names.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TickBody {
    /// Loops to run whether or not they are due: a ring that could not be
    /// delivered at once, carried by the alarm instead.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub loops: Vec<String>,
    /// The booking this wake was set for: its loop runs when the booking
    /// is still the current one, however the clocks disagree.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub booked: Option<BookedLook>,
    /// Set on a ring carried by the alarm: the tick that takes it answers
    /// the ring, so `weft status` stops saying the role cannot be reached.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub after_failed_ring: bool,
}

impl TickBody {
    /// The booking (its `seq`) of loop `name` this wake was set for.
    fn booking_of(&self, name: &str) -> Option<i64> {
        self.booked.as_ref().filter(|b| b.loop_name == name).map(|b| b.seq)
    }
}

/// One booking of a loop's look ([`Booking`]), as its wake carries it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BookedLook {
    pub loop_name: String,
    pub seq: i64,
    /// How many times a tick this booking's wake arrived early at set it
    /// again for its moment: each is a booking of its own, named apart,
    /// since the task being answered still holds the name of the one
    /// before.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub set_again: u32,
}

fn is_zero(n: &u32) -> bool {
    *n == 0
}

impl BookedLook {
    /// The wake that calls `role`'s tick for this booking at `at_ms`.
    pub fn wake(&self, role: CoreRole, at_ms: i64) -> Wake {
        let name = &self.loop_name;
        Wake {
            key: weft_platform_traits::alarm::again_key(&format!("loop:{role}:{name}"), self.set_again),
            at_unix_ms: at_ms,
            generation: self.seq,
            queue: WakeQueue::Internal,
            role,
            path: TICK_PATH.to_string(),
            body: serde_json::to_value(TickBody { booked: Some(self.clone()), ..TickBody::default() }).expect("a tick body serializes"),
        }
    }

    /// The same booking, set again by a tick its wake arrived early at.
    pub fn set_again(&self) -> Self {
        Self { set_again: self.set_again + 1, ..self.clone() }
    }
}

/// The wake that calls `role`'s tick for its loop `name` booked at `at_ms`
/// as the row's `seq`th booking.
pub fn loop_wake(role: CoreRole, name: &str, at_ms: i64, seq: i64) -> Wake {
    BookedLook { loop_name: name.to_string(), seq, set_again: 0 }.wake(role, at_ms)
}

/// Ask the alarm to call `role`'s tick in about a second for `loops`, for
/// a writer whose own ring could not get through: the platform's queue
/// tries it again for as long as its window allows.
pub async fn ring_later(alarm: &dyn Alarm, role: CoreRole, loops: Vec<String>) -> Result<()> {
    let body = TickBody { loops, booked: None, after_failed_ring: true };
    alarm
        .set(Wake {
            key: format!("ring:{role}"),
            at_unix_ms: now_ms() + 1000,
            // A name of its own every time, whichever process sets it:
            // two whose rings failed in the same millisecond each carry
            // their own loops.
            generation: uuid::Uuid::new_v4().as_u64_pair().0 as i64,
            queue: WakeQueue::Internal,
            role,
            path: TICK_PATH.to_string(),
            body: serde_json::to_value(body).expect("a tick body serializes"),
        })
        .await
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock past UNIX_EPOCH")
        .as_millis() as i64
}

/// `d` in whole milliseconds, saturating.
fn millis(d: Duration) -> i64 {
    i64::try_from(d.as_millis()).unwrap_or(i64::MAX)
}

/// The sooner of two looks, an empty one being the latest of all.
fn sooner(a: Option<i64>, b: Option<i64>) -> Option<i64> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    }
}

/// What a pass did with a loop, as its booking records it.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Step {
    /// It drained, and wants its next look this long from now (`None`:
    /// when a write wakes it).
    Drained(Option<Duration>),
    /// Its drain failed with `error`: it retries shortly, or, past its
    /// retries, is written down and waits for a write. `spent`: the
    /// booking (its `seq`) whose wake ran this pass, whose look is used up
    /// whatever the clocks say.
    Failed { error: String, spent: Option<i64> },
    /// Someone else books it (another loop, another role) for this
    /// moment: its failures stay as they are, unless that wake cannot be
    /// set.
    Booked(Option<i64>),
}

/// Whose look stands when a row is booked again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Rule {
    /// The new look stands, sooner or later: the booker knows.
    Exactly,
    /// Never later than what is booked: a look asked for without having
    /// read the row.
    Sooner,
    /// The new look stands unless a sibling booked the loop since this
    /// pass read its row with this seq (0 when there was no row, the seq
    /// a new row starts at); then the sooner of the two does, so the
    /// sibling's wake is never pushed back. Every booking that sets a
    /// wake moves the seq, so a changed seq is exactly a sibling's
    /// booking, whatever the clocks read.
    SinceRead(i64),
}

/// Booking the looks of one role's loops: the `role_loop_due` rows and
/// the alarm's wakes that go with them.
#[derive(Clone)]
pub struct Booking {
    pool: Pool,
    role: CoreRole,
    alarm: Arc<dyn Alarm>,
}

impl Booking {
    pub fn new(pool: Pool, role: CoreRole, alarm: Arc<dyn Alarm>) -> Self {
        Self { pool, role, alarm }
    }

    /// Book `name`'s next look at `at_ms` on the database's clock (`None`:
    /// none), whatever was booked before: for a booker that computed the
    /// one right moment (the broker booking the supervisor's next pass).
    pub async fn book(&self, name: &str, at_ms: Option<i64>) -> Result<()> {
        self.rebook(name, None, Step::Booked(at_ms), Rule::Exactly).await
    }

    /// Book `name`'s next look at `at_ms` on the database's clock unless
    /// one at or before it is booked already: for a loop that makes work
    /// another loop must look at by then. Reads the row first, so a look
    /// already booked in time costs no write.
    pub async fn book_sooner(&self, name: &str, at_ms: i64) -> Result<()> {
        let booked: Option<Option<i64>> = self
            .pool
            .scalar_opt("SELECT due_ms FROM role_loop_due WHERE role = $1 AND loop_name = $2", &[&self.role.as_str(), &name])
            .await?;
        if booked.flatten().is_some_and(|due| due <= at_ms) {
            return Ok(());
        }
        self.rebook(name, None, Step::Booked(Some(at_ms)), Rule::Sooner).await
    }

    /// [`Self::book_sooner`], `after` from now on the database's clock.
    pub async fn book_sooner_after(&self, name: &str, after: Duration) -> Result<()> {
        let now: i64 = self.pool.scalar(&format!("SELECT {DB_NOW_MS}"), &[]).await?;
        self.book_sooner(name, now.saturating_add(millis(after))).await
    }

    /// Book `name` (labelled `label`) again after `step`, as `rule` says:
    /// set the new look's wake, commit the row, cancel the old wake, in
    /// that order, so a crash between leaves a stray wake (one extra pass)
    /// and never a booking without one: a wake that cannot be set leaves
    /// the booking as it was.
    ///
    /// A failed drain books its next retry ([`RETRY_PAUSES`]); past the
    /// last it books nothing and drops a look already past. A failed drain
    /// past its retries, or a wake that could not be set, is written down
    /// with its error in the same transaction; the loop's next drain that
    /// goes through clears what was written, and a retry that fails before
    /// then writes its own error over it. An error only when the
    /// database could not be written: nothing was written down then.
    async fn rebook(&self, name: &str, label: Option<&str>, step: Step, rule: Rule) -> Result<()> {
        let role = self.role.as_str();
        let mut conn = self.pool.get().await?;
        let tx = conn.transaction().await?;
        tx.execute(
            &format!(
                "INSERT INTO role_loop_due (role, loop_name, due_ms, written_ms) VALUES ($1, $2, NULL, {DB_NOW_MS}) \
                 ON CONFLICT (role, loop_name) DO NOTHING"
            ),
            &[&role, &name],
        )
        .await?;
        let (due, seq, set_again, failures, failure_written, now): (Option<i64>, i64, i32, i32, bool, i64) = tx
            .one(
                &format!(
                    "SELECT due_ms, seq, set_again, failures, failure_written, {DB_NOW_MS} FROM role_loop_due \
                     WHERE role = $1 AND loop_name = $2 FOR UPDATE"
                ),
                &[&role, &name],
            )
            .await?;
        let sibling_wrote = matches!(rule, Rule::SinceRead(read_seq) if read_seq != seq);
        // What a failed drain keeps of the booking: a look still to come
        // that is not the one whose wake ran this pass, or one a sibling
        // booked since this pass read. A look already past, or spent, was
        // the one that just failed; kept, it would run the loop again on
        // every later pass of its role.
        let kept = match &step {
            Step::Failed { spent, .. } if !sibling_wrote => due.filter(|at| *at > now && *spent != Some(seq)),
            _ => due,
        };
        let by_rule = |wanted: Option<i64>| match rule {
            Rule::Exactly => wanted,
            Rule::SinceRead(_) if !sibling_wrote => wanted,
            Rule::SinceRead(_) | Rule::Sooner => sooner(due, wanted),
        };
        let retry = RETRY_PAUSES.get(usize::try_from(failures).unwrap_or(usize::MAX));
        let mut next = match &step {
            Step::Failed { .. } => sooner(kept, retry.map(|pause| now.saturating_add(millis(*pause)))),
            Step::Drained(after) => by_rule(after.map(|after| now.saturating_add(millis(after)))),
            Step::Booked(at) => by_rule(*at),
        };
        let mut next_seq = seq;
        let mut unset: Option<String> = None;
        if next != due {
            if let Some(at) = next {
                if let Err(e) = set_with_tries(&*self.alarm, loop_wake(self.role, name, at, seq + 1)).await {
                    // No wake, no booking: the row keeps the booking whose
                    // wake is set, so a later booking sets one again.
                    unset = Some(format!("its next look could not be set: {e:#}"));
                    next = kept;
                } else {
                    next_seq = seq + 1;
                }
            }
        }
        let error = match (&step, &unset) {
            (Step::Failed { error, .. }, Some(unset)) => Some(format!("{error}; and {unset}")),
            // Past its last retry.
            (Step::Failed { error, .. }, None) if retry.is_none() => Some(error.clone()),
            (_, unset) => unset.clone(),
        };
        let failures_now = match &step {
            Step::Failed { .. } => failures.saturating_add(1),
            Step::Drained(_) => 0,
            Step::Booked(_) => failures,
        };
        let cleared = matches!(step, Step::Drained(_)) && error.is_none() && failure_written;
        let callee = Callee::Loop { role, name };
        let mut alerts = unanswered::Alerts::default();
        if let Some(error) = &error {
            tracing::error!(target: "weft_task_store::drain", role, subsystem = name, %error, failures = failures_now, "a loop's pass failed and is written down; it runs again when a write that concerns it rings it");
            alerts = unanswered::gave_up(&tx, callee, label, error).await?;
        } else if cleared {
            unanswered::answered(&tx, callee).await?;
        } else if let (Step::Failed { error, .. }, true) = (&step, failure_written) {
            // Retrying, with a failure written down before (a wake that
            // could not be set): what `weft status` shows is what fails now.
            alerts = unanswered::failed(&tx, callee, label, error).await?;
        }
        tx.execute(
            &format!(
                "UPDATE role_loop_due SET due_ms = $3, seq = $4, set_again = $5, failures = $6, failure_written = $7, written_ms = {DB_NOW_MS} \
                 WHERE role = $1 AND loop_name = $2"
            ),
            &[
                &role,
                &name,
                &next,
                &next_seq,
                &if next_seq == seq { set_again } else { 0 },
                &failures_now,
                &(error.is_some() || (failure_written && !cleared)),
            ],
        )
        .await?;
        tx.commit().await?;
        alerts.log();
        if next != due {
            if let Some(old) = due {
                let old_wake = BookedLook { loop_name: name.to_string(), seq, set_again: u32::try_from(set_again).unwrap_or(0) }.wake(self.role, old);
                if let Err(e) = self.alarm.cancel(&old_wake).await {
                    tracing::warn!(
                        target: "weft_task_store::drain", role, subsystem = name, error = %format!("{e:#}"),
                        "could not cancel a loop's old wake; it goes off once and finds nothing to do"
                    );
                }
            }
        }
        Ok(())
    }
}

/// The tick `booked`'s wake arrived early at set it again (its
/// [`BookedLook::set_again`] counts that set): the row of its loop says
/// so while that booking is still the current one, so a booking that moves
/// cancels the wake still waiting rather than the one already delivered.
pub async fn set_again_recorded(pool: &Pool, role: CoreRole, booked: &BookedLook) -> Result<()> {
    pool.execute(
        "UPDATE role_loop_due SET set_again = GREATEST(set_again, $4) WHERE role = $1 AND loop_name = $2 AND seq = $3",
        &[&role.as_str(), &booked.loop_name, &booked.seq, &i32::try_from(booked.set_again).unwrap_or(i32::MAX)],
    )
    .await?;
    Ok(())
}

/// A pass that could not write down how some of its loops went (the
/// database did not answer it): those loops, and why. A tick answers it as
/// its failure's body, so a writer whose ring the tick was writes it down
/// for exactly those loops ([`pass_failed`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PassFailed {
    /// The loops whose failure the pass could not write down.
    pub loops: Vec<String>,
    pub error: String,
}

impl std::fmt::Display for PassFailed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} (loops: {})", self.error, self.loops.join(", "))
    }
}

impl std::error::Error for PassFailed {}

/// A pass of `role` could not write down how its loops went (`failed`:
/// it answered a writer's ring with that, the database not answering it):
/// the writer writes it down for the loops `failed` names, each loop's
/// next drain that goes through clearing it. Only the flag that says so
/// changes on a loop's row, its booking kept; a loop with no row gets one
/// due now, which is what having no row means ([`plan`]), so writing the
/// failure down never puts a due loop to sleep.
pub async fn pass_failed(pool: &Pool, role: CoreRole, failed: &PassFailed) -> Result<()> {
    let error = format!("its pass could not write down how it went: {}", failed.error);
    let mut conn = pool.get().await?;
    let tx = conn.transaction().await?;
    let mut alerts = unanswered::Alerts::default();
    for name in &failed.loops {
        tx.execute(
            &format!(
                "INSERT INTO role_loop_due (role, loop_name, due_ms, written_ms, failure_written) VALUES ($1, $2, {DB_NOW_MS}, {DB_NOW_MS}, TRUE) \
                 ON CONFLICT (role, loop_name) DO UPDATE SET failure_written = TRUE"
            ),
            &[&role.as_str(), name],
        )
        .await?;
        alerts = alerts.and(unanswered::failed(&tx, Callee::Loop { role: role.as_str(), name }, None, &error).await?);
    }
    tx.commit().await?;
    alerts.log();
    Ok(())
}

/// Set `wake`, trying [`SET_TRIES`] times with a short pause between.
async fn set_with_tries(alarm: &dyn Alarm, wake: Wake) -> Result<()> {
    let mut pause = Duration::from_millis(200);
    let mut tries = 1;
    loop {
        match alarm.set(wake.clone()).await {
            Ok(()) => return Ok(()),
            Err(e) if tries >= SET_TRIES => return Err(e),
            Err(e) => {
                tracing::warn!(target: "weft_task_store::drain", key = %wake.key, error = %format!("{e:#}"), "could not set a loop's wake; trying again");
                tokio::time::sleep(pause).await;
                pause *= 4;
                tries += 1;
            }
        }
    }
}

/// Which pass of its process a pass of a role that scales to zero is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pass {
    /// The one its process runs as it starts, for what came due while no
    /// process of the role was up: it also gives each loop that gave up
    /// ([`Booked::gave_up`]) one more try. Only this pass does, once per
    /// process, and the try keeps the loop's count of failures, so a loop
    /// past its retries whose try fails is not tried again before the
    /// next start or the next write that rings it.
    Start,
    /// A tick's: the loops its call names, and the ones due.
    Tick,
}

/// What a pass does with one loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Plan {
    /// Drain it now.
    Run,
    /// Leave it: its own wake or a write calls it when it is wanted.
    Leave,
}

/// A loop's row in `role_loop_due`, as a pass reads it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Booked {
    due_ms: Option<i64>,
    seq: i64,
    /// Written down with no look booked: past its last retry, or with a
    /// look its wake could not be set for and none kept. Nothing but a
    /// write that rings it, or a process of its role starting
    /// ([`Pass::Start`]), runs it again.
    gave_up: bool,
}

/// What `pass`, which read the due times at `now`, does with a loop booked
/// as `booked` (`None` with no row). A woken loop always runs, and so does
/// the loop whose current booking this pass's wake was set for
/// (`current_seq`); one with no row is due now, one due by now runs, one
/// that gave up runs on a process's start, and every other one is left.
fn plan(woken: bool, current_seq: Option<i64>, now: i64, booked: Option<Booked>, pass: Pass) -> Plan {
    if woken {
        return Plan::Run;
    }
    match booked {
        None => Plan::Run,
        Some(b) if current_seq == Some(b.seq) && b.due_ms.is_some() => Plan::Run,
        Some(Booked { due_ms: Some(at), .. }) if at <= now => Plan::Run,
        Some(Booked { gave_up: true, .. }) if pass == Pass::Start => Plan::Run,
        Some(_) => Plan::Leave,
    }
}

/// What a pass reads of `role`'s due times: the database's clock, each
/// loop's row, and the loops nobody has booked for [`FORGET_AFTER`].
struct ReadDue {
    now: i64,
    booked: HashMap<String, Booked>,
    unbooked: Vec<String>,
}

/// One row of [`read_due`]'s query: the clock, then a loop's name, when
/// it is due, its seq, whether nobody has booked it for [`FORGET_AFTER`],
/// and whether it gave up ([`Booked::gave_up`]); all five `None` when the
/// role has no row.
type DueRow = (i64, Option<String>, Option<i64>, Option<i64>, Option<bool>, Option<bool>);

async fn read_due(pool: &Pool, role: &str) -> Result<ReadDue> {
    let rows: Vec<DueRow> = pool
        .all(
            &format!(
                "SELECT c.now_ms, d.loop_name, d.due_ms, d.seq, d.written_ms < c.now_ms - $2::bigint, d.failure_written AND d.due_ms IS NULL \
                 FROM (SELECT {DB_NOW_MS} AS now_ms) c \
                 LEFT JOIN role_loop_due d ON d.role = $1"
            ),
            &[&role, &millis(FORGET_AFTER)],
        )
        .await?;
    let now = rows.first().map(|r| r.0).context("the clock query returned no row")?;
    let mut booked = HashMap::new();
    let mut unbooked = Vec::new();
    for (_, name, due_ms, seq, stale, gave_up) in rows {
        let (Some(name), Some(seq)) = (name, seq) else { continue };
        if stale == Some(true) {
            unbooked.push(name.clone());
        }
        booked.insert(name, Booked { due_ms, seq, gave_up: gave_up == Some(true) });
    }
    Ok(ReadDue { now, booked, unbooked })
}

/// One pass of a role that scales to zero, for a tick that says `call`:
/// drain the loops it names, the loop its booking is still current for,
/// and every loop already due, each until it has nothing left (or asks to
/// look again later, or spent its share of `slice`), and book each one's
/// next look. The others stay as they are, each with its own wake, so a
/// pass run because a journal event was written does not also run every
/// other loop. A process's first pass ([`Pass::Start`]) also runs each
/// loop that gave up, once.
///
/// `slice` is how long the pass keeps draining, shared out evenly between
/// the loops it runs: a loop with more left once its share is spent books
/// itself due now for the next tick. Every loop the pass runs drains at
/// least once, so a loop that always has more (a steady stream of tasks)
/// never keeps the loops after it waiting, nor holds the tick open until
/// the platform cuts it. `listen` is the process's watch, held while a
/// loop drains (its writes are what other roles wait on), and only once a
/// loop runs: a wake whose booking moved since reads the due times and
/// nothing more.
///
/// A loop that failed, or whose next look could not be set, is booked or
/// written down as [`Booking::rebook`] says, and the pass goes on. An
/// error only when the database could not be read or written, so nothing
/// was written down for the loops it names, never none: every loop `call`
/// names when the due times could not be read (none ran; every loop of
/// the role when `call` names none), else the loops whose rebook failed.
/// The tick answers it as a failure: the platform's queue calls a wake's
/// tick again, and a writer whose ring it was hands it to the alarm and
/// writes it down, when the database answers it (see the module docs).
pub async fn drain_due(
    booking: &Booking,
    loops: &[DrainLoop],
    call: &TickBody,
    pass: Pass,
    slice: Duration,
    listen: Option<&PgSignalWatch>,
) -> std::result::Result<(), PassFailed> {
    let role = booking.role.as_str();
    let read = read_due(&booking.pool, role).await.map_err(|e| {
        let named: Vec<String> =
            loops.iter().filter(|l| call.loops.iter().any(|w| w == l.name) || call.booking_of(l.name).is_some()).map(|l| l.name.to_string()).collect();
        PassFailed {
            // None ran: a call that names no loop (a role's first pass)
            // names them all, so a failed pass always says which loops it
            // is about.
            loops: if named.is_empty() { loops.iter().map(|l| l.name.to_string()).collect() } else { named },
            error: format!("could not read when the loops are due: {e:#}"),
        }
    })?;
    forget(booking, loops, &read.unbooked).await;
    let plans: Vec<Plan> = loops
        .iter()
        .map(|l| {
            let woken = call.loops.iter().any(|w| w == l.name);
            let booked = read.booked.get(l.name).copied();
            let plan = plan(woken, call.booking_of(l.name), read.now, booked, pass);
            if plan == Plan::Run && pass == Pass::Start && booked.is_some_and(|b| b.gave_up) {
                tracing::info!(target: "weft_task_store::drain", role, subsystem = l.name, "a loop that gave up gets one more try as its role's process starts");
            }
            plan
        })
        .collect();
    let running = plans.iter().filter(|p| **p == Plan::Run).count();
    if running == 0 {
        return Ok(());
    }
    let _busy = match listen {
        Some(watch) => Some(watch.busy().await),
        None => None,
    };
    let share = slice / u32::try_from(running).unwrap_or(u32::MAX);
    let mut failed: Vec<(&str, String)> = Vec::new();
    for (l, plan) in loops.iter().zip(plans) {
        if plan == Plan::Leave {
            continue;
        }
        let started = Instant::now();
        let step = loop {
            match (l.drain)().await {
                Ok(DrainStep::More) if started.elapsed() >= share => break Step::Drained(Some(Duration::ZERO)),
                Ok(DrainStep::More) => continue,
                Ok(DrainStep::Done) => break Step::Drained(None),
                Ok(DrainStep::RetryIn(after)) => break Step::Drained(Some(after)),
                Err(e) => {
                    tracing::warn!(target: "weft_task_store::drain", subsystem = l.name, error = %format!("{e:#}"), "drain failed");
                    break Step::Failed { error: format!("{e:#}"), spent: call.booking_of(l.name) };
                }
            }
        };
        let rule = Rule::SinceRead(read.booked.get(l.name).map_or(0, |b| b.seq));
        if let Err(e) = booking.rebook(l.name, Some(l.label), step, rule).await {
            failed.push((l.name, format!("{}: {e:#}", l.name)));
        }
    }
    if failed.is_empty() {
        return Ok(());
    }
    let errors: Vec<&str> = failed.iter().map(|(_, error)| error.as_str()).collect();
    Err(PassFailed {
        error: errors.join("; "),
        loops: failed.into_iter().map(|(name, _)| name.to_string()).collect(),
    })
}

/// Forget the rows of loops this role no longer has and nobody has booked
/// for [`FORGET_AFTER`] (a loop renamed or dropped by a later weft), and
/// any failure of theirs still written down. A wake one of them still had
/// finds no loop of its name, and runs nothing.
async fn forget(booking: &Booking, loops: &[DrainLoop], unbooked: &[String]) {
    let gone: Vec<&str> = unbooked.iter().map(String::as_str).filter(|name| !loops.iter().any(|l| l.name == *name)).collect();
    if gone.is_empty() {
        return;
    }
    let forgotten = booking
        .pool
        .execute(
            &format!(
                "WITH gone AS (DELETE FROM role_loop_due WHERE role = $1 AND loop_name = ANY($2) RETURNING loop_name) \
                 DELETE FROM unanswered_call WHERE callee IN (SELECT {} FROM gone)",
                unanswered::loop_key_sql("$1", "gone.loop_name")
            ),
            &[&booking.role.as_str(), &gone],
        )
        .await;
    if let Err(e) = forgotten {
        tracing::warn!(target: "weft_task_store::drain", role = booking.role.as_str(), error = %format!("{e:#}"), "could not forget the due times of loops this role no longer has; the next pass tries again");
    }
}

/// Whether `heard` wakes a loop listening as `wake_on`. Only a write it
/// waits on, or a recheck (notifications may have been lost); a watch
/// back from a quiet spell ([`Heard::Resumed`]) missed nothing a loop
/// waits on, since the process that wrote rang the loop's role itself.
fn wakes_loop(heard: &Heard, wake_on: &[WakeOn]) -> bool {
    match heard {
        Heard::Signal { channel, payload } => wake_on.iter().any(|w| w.hears(channel, payload)),
        Heard::Recheck => true,
        Heard::Resumed | Heard::Lost => false,
    }
}

/// Drive the drain loop forever: after every wake, and, by `looks`, once
/// at start and on every `safety` interval and `RetryIn` (a local
/// install's process), or only when woken (a process of a role that scales
/// to zero). Returns only when the process's signal watch stops, since
/// nothing could wake the loop again; the caller's supervisor crashes the
/// process on that.
///
/// Scaled to zero, a drain here books nothing: every write that wakes it
/// also rings the role's tick for the same loop (the writer's waker rings
/// by the same rules the loop wakes by), and that tick's pass drains after
/// the write and books the loop's next look from what it finds. One owner
/// for every booking, and a drain on the run's path writes nothing more.
///
/// `signals` must be subscribed before the call, and `target` is the
/// tracing field, so subsystems log under their own loop name.
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
    // A look the drain asked for: on a local install the first one is at
    // once (rows that landed before the loop subscribed), since nothing
    // else catches them there; scaled to zero, the role's ticks do.
    let mut asked = match &looks {
        Looks::AtSafety => Some(Instant::now()),
        Looks::WhileWorking(_) => None,
    };
    let mut sweep = Instant::now() + safety;
    // The first safety look after the start drain comes at this loop's own
    // place in its interval ([`phase`]), and each one after it an interval
    // after the drain before: every loop of a process starts in the same
    // instant, and without it their looks would all take a connection in
    // the same millisecond, once an interval, for as long as it runs.
    let mut first_look = Some(phase(target, safety));
    loop {
        let waited = match &looks {
            Looks::AtSafety => {
                let deadline = asked.map_or(sweep, |at| at.min(sweep));
                if Instant::now() >= deadline {
                    Ok(())
                } else if wake_on.is_empty() {
                    // A loop that listens to no channel runs on its
                    // interval alone: waking a timed sweep on a recheck
                    // would run it back to back while notifications pour in.
                    tokio::time::sleep_until(deadline).await;
                    Ok(())
                } else {
                    woken_before(&mut signals, deadline, wake_on).await
                }
            }
            Looks::WhileWorking(_) => woken(&mut signals, wake_on).await,
        };
        if let Err(e) = waited {
            tracing::error!(target: "weft_task_store::drain", subsystem = target, error = %e, "cannot be woken any more");
            return;
        }
        // Everything heard up to here is covered by the drain that
        // follows, so a burst of notifications costs one drain, not one
        // per notification.
        if let Err(e) = signals.clear() {
            tracing::error!(target: "weft_task_store::drain", subsystem = target, error = %e, "cannot be woken any more");
            return;
        }
        sweep = Instant::now() + first_look.take().unwrap_or(safety);
        asked = None;
        let busy = match &looks {
            Looks::AtSafety => None,
            Looks::WhileWorking(watch) => Some(watch.busy().await),
        };
        // Drain until the body reports it has nothing left. This is what
        // makes a burst of more rows than one batch finish in one wake.
        loop {
            match drain().await {
                Ok(DrainStep::More) => continue,
                Ok(DrainStep::Done) => break,
                Ok(DrainStep::RetryIn(after)) => {
                    if let Looks::AtSafety = looks {
                        asked = Some(Instant::now() + after);
                    }
                    break;
                }
                Err(e) => {
                    tracing::warn!(target: "weft_task_store::drain", subsystem = target, error = %format!("{e:#}"), "drain failed; will retry on next wake");
                    break;
                }
            }
        }
        drop(busy);
    }
}

/// Where in its `safety` interval the loop `name` takes its first look on
/// a machine's process ([`run`]): a fixed point from the loop's name (its
/// hash, `crate::locks::fnv1a_64`, the same on every build and every run),
/// so the loops of a process look one after another across the interval
/// instead of all at once.
fn phase(name: &str, safety: Duration) -> Duration {
    let hash = crate::locks::fnv1a_64(name.bytes());
    // Whole milliseconds: tokio's timer fires on its millisecond ticks,
    // so a finer place would only be rounded on the way.
    let span = u64::try_from(safety.as_millis()).unwrap_or(u64::MAX).max(1);
    Duration::from_millis(hash % span)
}

/// Wait until `deadline` at most for a notification that wakes a loop
/// listening as `wake_on` ([`wakes_loop`]).
async fn woken_before(signals: &mut Subscription, deadline: Instant, wake_on: &[WakeOn]) -> Result<()> {
    match tokio::time::timeout_at(deadline, woken(signals, wake_on)).await {
        Err(_) => Ok(()),
        Ok(woken) => woken,
    }
}

/// Wait, however long it takes, for a notification that wakes a loop
/// listening as `wake_on` ([`wakes_loop`]).
async fn woken(signals: &mut Subscription, wake_on: &[WakeOn]) -> Result<()> {
    loop {
        if wakes_loop(&signals.next().await?, wake_on) {
            return Ok(());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use tokio::sync::broadcast;

    const CHANNEL: &str = "chan";
    const WAKE: &[WakeOn] = &[WakeOn { channel: CHANNEL, concerns: |p| p == "mine" }];
    const LOOP: &str = "test";

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
        tokio::spawn(run(rx.into(), WAKE, Duration::from_secs(30), Looks::AtSafety, LOOP, body));
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(runs.lock().unwrap().len(), 3, "More, More, Done");
    }

    /// Loops started together on a machine's process take their first
    /// safety look each at its own place in the interval, the same on
    /// every run, then an interval after each drain.
    #[tokio::test(start_paused = true)]
    async fn loops_started_together_look_each_at_its_own_place_in_the_interval() {
        let safety = Duration::from_secs(60);
        let names = ["delivery", "run_ends", "lifecycle_claimer", "dispatcher_picker", "retention", "lapses"];
        let mut runs = Vec::new();
        let mut keep = Vec::new();
        for name in names {
            let (tx, rx) = broadcast::channel(16);
            let r = Arc::new(Mutex::new(Vec::new()));
            tokio::spawn(run(rx.into(), WAKE, safety, Looks::AtSafety, name, scripted(r.clone(), vec![])));
            runs.push(r);
            keep.push(tx);
        }
        tokio::time::sleep(safety * 2 + Duration::from_millis(1)).await;
        let mut firsts = Vec::new();
        for (name, r) in names.iter().zip(&runs) {
            let r = r.lock().unwrap().clone();
            let first = phase(name, safety);
            assert!(first < safety);
            assert_eq!(r[0], Duration::ZERO, "{name}: the start drain at once");
            assert_eq!(r[1], first, "{name}: its first look at its place");
            assert_eq!(r[2], first + safety, "{name}: then an interval after it");
            firsts.push(first);
        }
        assert_eq!(phase("delivery", safety), phase("delivery", safety), "the same place every time");
        firsts.sort();
        firsts.dedup_by(|a, b| a.abs_diff(*b) < Duration::from_secs(1));
        assert_eq!(firsts.len(), names.len(), "no two within a second of each other: {firsts:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn a_wake_that_concerns_it_drains_and_one_that_does_not_is_ignored() {
        let (tx, rx) = broadcast::channel(16);
        let runs = Arc::new(Mutex::new(Vec::new()));
        tokio::spawn(run(rx.into(), WAKE, Duration::from_secs(30), Looks::AtSafety, LOOP, scripted(runs.clone(), vec![])));
        tokio::time::sleep(Duration::from_secs(1)).await;
        tx.send(signal("theirs")).unwrap();
        tx.send(Heard::Signal { channel: "other", payload: "mine".into() }).unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(runs.lock().unwrap().len(), 1, "only the start drain");
        tx.send(signal("mine")).unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(runs.lock().unwrap().len(), 2);
    }

    /// A recheck (notifications may have been lost) drains; a watch back
    /// from a quiet spell does not: the process that wrote rang the role.
    #[tokio::test(start_paused = true)]
    async fn a_recheck_drains_and_a_resume_does_not() {
        let (tx, rx) = broadcast::channel(16);
        let runs = Arc::new(Mutex::new(Vec::new()));
        tokio::spawn(run(rx.into(), WAKE, Duration::from_secs(30), Looks::AtSafety, LOOP, scripted(runs.clone(), vec![])));
        tokio::time::sleep(Duration::from_secs(1)).await;
        tx.send(Heard::Resumed).unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(runs.lock().unwrap().len(), 1, "a resume is not a reason to look");
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
        tokio::spawn(run(rx.into(), &[], Duration::from_secs(30), Looks::AtSafety, LOOP, scripted(runs.clone(), vec![])));
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
        tokio::spawn(run(rx.into(), WAKE, Duration::from_secs(30), Looks::AtSafety, LOOP, scripted(runs.clone(), vec![])));
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
        tokio::spawn(run(rx.into(), WAKE, Duration::from_secs(30), Looks::AtSafety, LOOP, scripted(runs.clone(), vec![])));
        tokio::time::sleep(Duration::from_secs(61)).await;
        let runs = runs.lock().unwrap().clone();
        let first = phase(LOOP, Duration::from_secs(30));
        assert_eq!(runs, vec![Duration::ZERO, first, first + Duration::from_secs(30)]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_retry_looks_again_soon_and_only_while_asked() {
        let (_tx, rx) = broadcast::channel(16);
        let runs = Arc::new(Mutex::new(Vec::new()));
        let body = scripted(runs.clone(), vec![DrainStep::RetryIn(Duration::from_millis(100)), DrainStep::Done]);
        tokio::spawn(run(rx.into(), WAKE, Duration::from_secs(30), Looks::AtSafety, LOOP, body));
        tokio::time::sleep(Duration::from_secs(1)).await;
        let runs = runs.lock().unwrap().clone();
        assert_eq!(runs, vec![Duration::ZERO, Duration::from_millis(100)]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_stopped_watch_ends_the_loop() {
        let (tx, rx) = broadcast::channel::<Heard>(16);
        let runs = Arc::new(Mutex::new(Vec::new()));
        let handle = tokio::spawn(run(rx.into(), WAKE, Duration::from_secs(30), Looks::AtSafety, LOOP, scripted(runs.clone(), vec![])));
        tokio::time::sleep(Duration::from_secs(1)).await;
        drop(tx);
        handle.await.unwrap();
    }

    fn drain_loop(name: &'static str, wake_on: &'static [WakeOn]) -> DrainLoop {
        DrainLoop::new(name, "test things", wake_on, Duration::from_secs(30), || async { Ok(DrainStep::Done) })
    }

    /// A role scaled to zero refuses to start with a loop nothing would
    /// ever run again: woken by no write, booked by no loop it has.
    #[test]
    fn a_loop_nothing_wakes_or_books_is_refused() {
        let rung = drain_loop("rung", WAKE);
        let booked = drain_loop("booked", &[]).booked_by(&["rung"]);
        check_woken(CoreRole::Dispatcher, &[rung.clone(), booked]).expect("every loop is rung or booked");
        let err = check_woken(CoreRole::Dispatcher, &[rung.clone(), drain_loop("sweep", &[])]).unwrap_err();
        assert!(err.to_string().contains("'sweep'"), "{err}");
        let err = check_woken(CoreRole::Dispatcher, &[rung, drain_loop("orphan", &[]).booked_by(&["gone"])]).unwrap_err();
        assert!(err.to_string().contains("'gone'"), "{err}");
    }
}

#[cfg(test)]
mod plan_tests {
    use super::*;

    fn due(at: i64, seq: i64) -> Option<Booked> {
        Some(Booked { due_ms: Some(at), seq, gave_up: false })
    }

    fn asleep(seq: i64) -> Option<Booked> {
        Some(Booked { due_ms: None, seq, gave_up: false })
    }

    fn gave_up(seq: i64) -> Option<Booked> {
        Some(Booked { due_ms: None, seq, gave_up: true })
    }

    #[test]
    fn a_woken_loop_always_runs() {
        assert_eq!(plan(true, None, 100, due(1_000, 1), Pass::Tick), Plan::Run);
        assert_eq!(plan(true, None, 100, asleep(1), Pass::Tick), Plan::Run);
        assert_eq!(plan(true, None, 100, gave_up(1), Pass::Tick), Plan::Run, "a ring runs a loop that gave up");
    }

    #[test]
    fn a_loop_with_no_row_or_a_row_due_runs_and_one_booked_later_is_left() {
        assert_eq!(plan(false, None, 100, None, Pass::Tick), Plan::Run, "no row: due now");
        assert_eq!(plan(false, None, 100, due(100, 1), Pass::Tick), Plan::Run);
        assert_eq!(plan(false, None, 100, due(50, 1), Pass::Tick), Plan::Run);
        assert_eq!(plan(false, None, 100, due(101, 1), Pass::Tick), Plan::Leave, "its own wake comes then");
        assert_eq!(plan(false, None, 100, due(101, 1), Pass::Start), Plan::Leave, "a start runs nothing booked later");
    }

    /// The wake of the current booking runs its loop even when the
    /// database's clock says a hair too early; a stale wake (its booking
    /// moved since) runs nothing.
    #[test]
    fn the_current_bookings_wake_runs_its_loop_and_a_stale_one_does_not() {
        assert_eq!(plan(false, Some(4), 100, due(150, 4), Pass::Tick), Plan::Run);
        assert_eq!(plan(false, Some(3), 100, due(150, 4), Pass::Tick), Plan::Leave);
        assert_eq!(plan(false, Some(4), 100, asleep(4), Pass::Tick), Plan::Leave, "the booking was cleared since");
    }

    /// A loop with nothing left waits for a write however long ago it was
    /// booked, a process's start included: nothing runs "just in case".
    #[test]
    fn an_asleep_loop_waits_for_a_write() {
        assert_eq!(plan(false, None, 1_000_000, asleep(0), Pass::Tick), Plan::Leave);
        assert_eq!(plan(false, None, 1_000_000, asleep(0), Pass::Start), Plan::Leave, "it never gave up");
    }

    /// A loop that gave up gets its one more try from the pass its process
    /// starts with, and from no other: a tick, the start's try that failed
    /// included, leaves it for a write to ring.
    #[test]
    fn a_loop_that_gave_up_runs_on_a_start_and_on_no_tick() {
        assert_eq!(plan(false, None, 100, gave_up(3), Pass::Start), Plan::Run);
        assert_eq!(plan(false, None, 100, gave_up(3), Pass::Tick), Plan::Leave);
        assert_eq!(plan(false, Some(3), 100, gave_up(3), Pass::Tick), Plan::Leave, "nor does a wake of the booking it gave up on");
    }

    #[test]
    fn sooner_reads_an_empty_look_as_the_latest() {
        assert_eq!(sooner(None, Some(5)), Some(5));
        assert_eq!(sooner(Some(3), Some(5)), Some(3));
        assert_eq!(sooner(None, None), None);
    }

    /// A tick body with nothing in it is the empty one, and an old or
    /// foreign body (a push from another service) reads as one too.
    #[test]
    fn a_tick_body_round_trips_and_defaults() {
        let body = TickBody { loops: vec!["a".into()], booked: Some(BookedLook { loop_name: "b".into(), seq: 2, set_again: 1 }), after_failed_ring: true };
        assert_eq!(serde_json::from_value::<TickBody>(serde_json::to_value(&body).unwrap()).unwrap(), body);
        assert_eq!(serde_json::to_value(TickBody::default()).unwrap(), serde_json::json!({}));
        assert_eq!(serde_json::from_value::<TickBody>(serde_json::json!({ "message": {} })).unwrap(), TickBody::default());
    }
}

// These tests need a real Postgres. A module named `db_tests` is how
// `.config/nextest.toml` finds database tests inside a library: the
// default profile leaves them out, the `db` profile runs them.
#[cfg(test)]
mod db_tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Mutex;
    use weft_platform_traits::FakeAlarm;

    const WRITES: &[WakeOn] = &[WakeOn::any("chan")];
    const ROLE: CoreRole = CoreRole::Dispatcher;

    async fn schema(pool: &Pool) {
        crate::apply_groups(pool, &[&GROUP, &crate::unanswered::GROUP]).await.expect("role_loop_due schema");
        crate::unanswered::judged_run_columns(pool).await.expect("the run columns");
    }

    fn booking(pool: &Pool) -> (Booking, Arc<FakeAlarm>) {
        let alarm = Arc::new(FakeAlarm::new());
        (Booking::new(pool.clone(), ROLE, alarm.clone()), alarm)
    }

    fn counting(name: &'static str, step: DrainStep) -> (DrainLoop, Arc<AtomicU32>) {
        let runs = Arc::new(AtomicU32::new(0));
        let counter = runs.clone();
        let l = DrainLoop::new(name, "count", WRITES, Duration::from_secs(30), move || {
            counter.fetch_add(1, Ordering::SeqCst);
            async move { Ok(step) }
        });
        (l, runs)
    }

    /// A tick's pass, with a minute to drain and no watch.
    async fn pass(booking: &Booking, loops: &[DrainLoop], call: &TickBody) -> std::result::Result<(), PassFailed> {
        drain_due(booking, loops, call, Pass::Tick, Duration::from_secs(60), None).await
    }

    /// The pass a process of the role starts with.
    async fn start(booking: &Booking, loops: &[DrainLoop]) -> std::result::Result<(), PassFailed> {
        drain_due(booking, loops, &TickBody::default(), Pass::Start, Duration::from_secs(60), None).await
    }

    async fn written(pool: &Pool) -> Vec<(String, Option<String>, String)> {
        pool.all("SELECT callee, label, error FROM unanswered_call ORDER BY callee", &[]).await.unwrap()
    }

    async fn row(pool: &Pool, name: &str) -> (Option<i64>, i64, i32) {
        pool.one("SELECT due_ms, seq, failures FROM role_loop_due WHERE role = $1 AND loop_name = $2", &[&ROLE.as_str(), &name])
            .await
            .unwrap()
    }

    async fn db_now(pool: &Pool) -> i64 {
        pool.scalar(&format!("SELECT {DB_NOW_MS}"), &[]).await.unwrap()
    }

    fn named(names: &[&str]) -> TickBody {
        TickBody { loops: names.iter().map(|n| n.to_string()).collect(), ..TickBody::default() }
    }

    /// A fresh pass drains every loop; one that asks for a look books it
    /// with one wake for exactly that loop, and one with nothing left
    /// books nothing.
    #[weft_db_macros::test]
    async fn a_fresh_pass_drains_every_loop_and_books_only_the_looks_asked_for(pool: Pool) {
        schema(&pool).await;
        let (booking, alarm) = booking(&pool);
        let a_runs = Arc::new(AtomicU32::new(0));
        let runs = a_runs.clone();
        let a = DrainLoop::new("a", "a things", WRITES, Duration::from_secs(60), move || {
            let n = runs.fetch_add(1, Ordering::SeqCst);
            async move { Ok(if n < 2 { DrainStep::More } else { DrainStep::Done }) }
        });
        let (b, _) = counting("b", DrainStep::RetryIn(Duration::from_secs(5)));
        let before = db_now(&pool).await;
        pass(&booking, &[a, b], &TickBody::default()).await.unwrap();
        assert_eq!(a_runs.load(Ordering::SeqCst), 3, "More, More, Done");
        assert_eq!(row(&pool, "a").await.0, None, "nothing left: asleep until woken");
        let (due, seq, _) = row(&pool, "b").await;
        let due = due.expect("b asked for a look");
        assert!(due >= before + 5_000 && due < before + 7_000, "{due} vs {before}");
        let wakes = alarm.wakes();
        assert_eq!(wakes, vec![loop_wake(ROLE, "b", due, seq)], "one wake, for b alone");
        assert_eq!(wakes[0].queue, WakeQueue::Internal);
    }

    /// Moving a booking sets the new wake and cancels the old one; clearing
    /// it cancels it; a booking that does not move touches no wake.
    #[weft_db_macros::test]
    async fn a_booking_that_moves_replaces_its_wake_and_one_cleared_cancels_it(pool: Pool) {
        schema(&pool).await;
        let (booking, alarm) = booking(&pool);
        let now = db_now(&pool).await;
        booking.book("x", Some(now + 60_000)).await.unwrap();
        booking.book("x", Some(now + 60_000)).await.unwrap();
        assert_eq!(alarm.wakes().len(), 1, "the same moment again is no new wake");
        booking.book("x", Some(now + 3_600_000)).await.unwrap();
        let first = loop_wake(ROLE, "x", now + 60_000, 1);
        let second = loop_wake(ROLE, "x", now + 3_600_000, 2);
        assert_eq!(alarm.wakes(), vec![first.clone(), second.clone()]);
        assert_eq!(alarm.cancelled(), vec![first.clone()], "moved later: the old wake is cancelled");
        assert_ne!(first.name(), second.name());
        booking.book("x", None).await.unwrap();
        assert_eq!(alarm.cancelled(), vec![first, second], "cleared: its wake too");
        assert!(alarm.pending().is_empty());
        assert_eq!(row(&pool, "x").await, (None, 2, 0));
    }

    /// Booking a sooner look never pushes one already booked back, and a
    /// look already booked in time costs no write.
    #[weft_db_macros::test]
    async fn a_sooner_look_never_pushes_a_booking_back(pool: Pool) {
        schema(&pool).await;
        let (booking, alarm) = booking(&pool);
        let now = db_now(&pool).await;
        booking.book_sooner("lapses", now + 60_000).await.unwrap();
        booking.book_sooner("lapses", now + 120_000).await.unwrap();
        assert_eq!(row(&pool, "lapses").await.0, Some(now + 60_000));
        assert_eq!(alarm.wakes().len(), 1, "a later look leaves the booking as it is");
        booking.book_sooner("lapses", now + 30_000).await.unwrap();
        assert_eq!(row(&pool, "lapses").await.0, Some(now + 30_000));
        assert_eq!(alarm.pending(), vec![loop_wake(ROLE, "lapses", now + 30_000, 2)]);
    }

    /// The wake of a booking still current runs its loop even before the
    /// database's clock reaches it; a wake whose booking moved since runs
    /// nothing; a woken loop runs whatever its booking.
    #[weft_db_macros::test]
    async fn a_tick_runs_its_current_booking_and_its_woken_loops_only(pool: Pool) {
        schema(&pool).await;
        let (booking, _) = booking(&pool);
        let (x, x_runs) = counting("x", DrainStep::Done);
        let (y, y_runs) = counting("y", DrainStep::Done);
        let loops = [x, y];
        pass(&booking, &loops, &TickBody::default()).await.unwrap();
        assert_eq!((x_runs.load(Ordering::SeqCst), y_runs.load(Ordering::SeqCst)), (1, 1), "no rows: both due");
        let now = db_now(&pool).await;
        booking.book("x", Some(now + 600_000)).await.unwrap();
        let stale = TickBody { booked: Some(BookedLook { loop_name: "x".into(), seq: 0, set_again: 0 }), ..TickBody::default() };
        pass(&booking, &loops, &stale).await.unwrap();
        assert_eq!(x_runs.load(Ordering::SeqCst), 1, "a stale wake runs nothing");
        let current = TickBody { booked: Some(BookedLook { loop_name: "x".into(), seq: 1, set_again: 0 }), ..TickBody::default() };
        pass(&booking, &loops, &current).await.unwrap();
        assert_eq!(x_runs.load(Ordering::SeqCst), 2, "the current booking's wake runs it");
        assert_eq!(y_runs.load(Ordering::SeqCst), 1, "and nothing else");
        pass(&booking, &loops, &named(&["y"])).await.unwrap();
        assert_eq!((x_runs.load(Ordering::SeqCst), y_runs.load(Ordering::SeqCst)), (2, 2));
    }

    /// The tick body of the wake booking `seq` of `name` set.
    fn booked(name: &str, seq: i64) -> TickBody {
        TickBody { booked: Some(BookedLook { loop_name: name.into(), seq, set_again: 0 }), ..TickBody::default() }
    }

    /// A drain that fails books its retries, a little later each time,
    /// and its pass goes through; past the last retry it books nothing,
    /// keeps no look already spent, and is written down for `weft status`.
    /// From then on only a ring runs it, each failure rewriting the error,
    /// and its next success clears what was written.
    #[weft_db_macros::test]
    async fn a_failing_loop_retries_then_stops_written_down_until_a_ring_drains_it(pool: Pool) {
        schema(&pool).await;
        let (booking, alarm) = booking(&pool);
        let error: Arc<Mutex<Option<&'static str>>> = Arc::new(Mutex::new(Some("the database is down")));
        let failing = error.clone();
        let runs = Arc::new(AtomicU32::new(0));
        let counter = runs.clone();
        let l = DrainLoop::new("flaky", "hand queued runs to workers", WRITES, Duration::from_secs(30), move || {
            counter.fetch_add(1, Ordering::SeqCst);
            let error = *failing.lock().unwrap();
            async move {
                match error {
                    Some(error) => anyhow::bail!(error),
                    None => Ok(DrainStep::Done),
                }
            }
        });
        let loops = [l];
        let mut call = named(&["flaky"]);
        for (n, pause) in RETRY_PAUSES.iter().enumerate() {
            let before = db_now(&pool).await;
            pass(&booking, &loops, &call).await.expect("a retry booked is a pass that went through");
            let (due, seq, failures) = row(&pool, "flaky").await;
            let due = due.expect("a retry is booked");
            assert!(due >= before + millis(*pause) && due < before + millis(*pause) + 2_000, "retry {n}: {due} vs {before}");
            assert_eq!((seq, failures), (n as i64 + 1, n as i32 + 1));
            assert!(written(&pool).await.is_empty(), "nothing written down while it retries");
            call = booked("flaky", seq);
        }
        assert_eq!(alarm.wakes().len(), RETRY_PAUSES.len());
        pass(&booking, &loops, &call).await.expect("written down, the pass went through");
        assert_eq!(row(&pool, "flaky").await, (None, RETRY_PAUSES.len() as i64, RETRY_PAUSES.len() as i32 + 1), "no look left");
        let label = Some("hand queued runs to workers".to_string());
        assert_eq!(written(&pool).await, vec![("loop:dispatcher:flaky".into(), label.clone(), "the database is down".into())]);
        assert_eq!(alarm.wakes().len(), RETRY_PAUSES.len(), "no wake past the last retry");

        let ran = runs.load(Ordering::SeqCst);
        pass(&booking, &loops, &TickBody::default()).await.unwrap();
        assert_eq!(runs.load(Ordering::SeqCst), ran, "another pass of its role leaves it alone");
        *error.lock().unwrap() = Some("permission denied");
        pass(&booking, &loops, &named(&["flaky"])).await.unwrap();
        assert_eq!(row(&pool, "flaky").await.0, None, "a ring that fails books nothing either");
        assert_eq!(written(&pool).await, vec![("loop:dispatcher:flaky".into(), label, "permission denied".into())], "the latest error");
        *error.lock().unwrap() = None;
        pass(&booking, &loops, &named(&["flaky"])).await.unwrap();
        assert_eq!(row(&pool, "flaky").await, (None, RETRY_PAUSES.len() as i64, 0));
        assert!(written(&pool).await.is_empty(), "its success clears it");
    }

    /// A loop that gave up gets one more try from the pass a process of
    /// its role starts with: failing, it books no retry (its count of
    /// failures kept) and is written down again, and no tick tries it
    /// after; the next start tries it again, and going through, it clears
    /// what was written. A loop that never gave up is left by a start.
    #[weft_db_macros::test]
    async fn a_loop_that_gave_up_is_tried_once_per_start(pool: Pool) {
        schema(&pool).await;
        let (booking, alarm) = booking(&pool);
        let error: Arc<Mutex<Option<&'static str>>> = Arc::new(Mutex::new(Some("Permission 'cloudtasks.tasks.create' denied")));
        let failing = error.clone();
        let runs = Arc::new(AtomicU32::new(0));
        let counter = runs.clone();
        let l = DrainLoop::new("expiry", "expire stored files", WRITES, Duration::from_secs(30), move || {
            counter.fetch_add(1, Ordering::SeqCst);
            let error = *failing.lock().unwrap();
            async move {
                match error {
                    Some(error) => anyhow::bail!(error),
                    None => Ok(DrainStep::Done),
                }
            }
        });
        let (asleep, asleep_runs) = counting("asleep", DrainStep::Done);
        let loops = [l, asleep];
        // Fails through its retries and gives up; `asleep` drains once and
        // sleeps until a write wakes it.
        let mut call = named(&["expiry", "asleep"]);
        for _ in 0..=RETRY_PAUSES.len() {
            pass(&booking, &loops, &call).await.unwrap();
            call = booked("expiry", row(&pool, "expiry").await.1);
        }
        let gave_up = (None, RETRY_PAUSES.len() as i64, RETRY_PAUSES.len() as i32 + 1);
        assert_eq!(row(&pool, "expiry").await, gave_up);
        assert_eq!(written(&pool).await.len(), 1);
        let (ran, wakes) = (runs.load(Ordering::SeqCst), alarm.wakes().len());

        *error.lock().unwrap() = Some("Permission 'cloudtasks.tasks.create' still denied");
        start(&booking, &loops).await.unwrap();
        assert_eq!(runs.load(Ordering::SeqCst), ran + 1, "a start tries it once");
        assert_eq!(asleep_runs.load(Ordering::SeqCst), 1, "and leaves a loop that never gave up");
        let (due, seq, failures) = row(&pool, "expiry").await;
        assert_eq!((due, seq, failures), (None, gave_up.1, gave_up.2 + 1), "failing, it books no retry and keeps counting");
        assert_eq!(alarm.wakes().len(), wakes, "no wake set for it");
        let again: Vec<String> = written(&pool).await.into_iter().map(|w| w.2).collect();
        assert_eq!(again, vec!["Permission 'cloudtasks.tasks.create' still denied".to_string()], "written down again, with the start's error");
        pass(&booking, &loops, &TickBody::default()).await.unwrap();
        assert_eq!(runs.load(Ordering::SeqCst), ran + 1, "no tick tries it after");

        *error.lock().unwrap() = None;
        start(&booking, &loops).await.unwrap();
        assert_eq!(runs.load(Ordering::SeqCst), ran + 2, "the next start tries it again");
        assert_eq!(row(&pool, "expiry").await, (None, gave_up.1, 0), "going through, it is back to normal");
        assert!(written(&pool).await.is_empty(), "and what was written is cleared");
        start(&booking, &loops).await.unwrap();
        assert_eq!(runs.load(Ordering::SeqCst), ran + 2, "a loop back to normal is left by a start");
    }

    /// A loop written down only because its wake could not be set has
    /// spent no retry: the start's try that fails books its first, and
    /// what `weft status` shows is the error that try failed with.
    #[weft_db_macros::test]
    async fn a_start_try_of_a_loop_whose_wake_failed_retries_showing_its_error(pool: Pool) {
        schema(&pool).await;
        let (booking, alarm) = booking(&pool);
        alarm.fail_sets(true);
        let (x, _) = counting("x", DrainStep::RetryIn(Duration::from_secs(5)));
        pass(&booking, &[x], &TickBody::default()).await.expect("written down");
        assert_eq!(row(&pool, "x").await, (None, 0, 0), "nothing booked, no failure counted");
        assert!(written(&pool).await[0].2.contains("could not be set"));
        alarm.fail_sets(false);
        let failing = DrainLoop::new("x", "count", WRITES, Duration::from_secs(30), || async { anyhow::bail!("still denied") });
        let before = db_now(&pool).await;
        start(&booking, &[failing]).await.unwrap();
        let (due, seq, failures) = row(&pool, "x").await;
        assert!(due.is_some_and(|due| due >= before + millis(RETRY_PAUSES[0])), "its first retry is booked: {due:?}");
        assert_eq!((seq, failures), (1, 1));
        assert_eq!(written(&pool).await.into_iter().map(|w| w.2).collect::<Vec<_>>(), vec!["still denied".to_string()]);
    }

    /// A booking whose wake cannot be set leaves the booking as it was and
    /// is written down, and the call goes through: a later booking sets the
    /// wake, and the loop's next drain clears what was written.
    #[weft_db_macros::test]
    async fn a_wake_that_cannot_be_set_leaves_no_booking_without_one(pool: Pool) {
        schema(&pool).await;
        let (booking, alarm) = booking(&pool);
        alarm.fail_sets(true);
        let now = db_now(&pool).await;
        booking.book("x", Some(now + 60_000)).await.expect("written down");
        assert_eq!(row(&pool, "x").await, (None, 0, 0));
        assert_eq!(written(&pool).await.into_iter().map(|w| w.0).collect::<Vec<_>>(), vec!["loop:dispatcher:x".to_string()]);
        alarm.fail_sets(false);
        booking.book_sooner("x", now + 60_000).await.unwrap();
        assert_eq!(row(&pool, "x").await, (Some(now + 60_000), 1, 0));
        assert_eq!(alarm.pending(), vec![loop_wake(ROLE, "x", now + 60_000, 1)]);
        assert_eq!(written(&pool).await.len(), 1, "a booking that goes through is not a drain");
        let (x, _) = counting("x", DrainStep::Done);
        pass(&booking, &[x], &named(&["x"])).await.unwrap();
        assert!(written(&pool).await.is_empty());
    }

    /// A wake set again by a tick it arrived early at is the one a booking
    /// that moves cancels, once the loop's row knows of it; a wake of a
    /// booking no longer current changes nothing.
    #[weft_db_macros::test]
    async fn a_booking_that_moves_cancels_its_wake_as_set_again(pool: Pool) {
        schema(&pool).await;
        let (booking, alarm) = booking(&pool);
        let now = db_now(&pool).await;
        booking.book("x", Some(now + 3_600_000)).await.unwrap();
        let again = BookedLook { loop_name: "x".into(), seq: 1, set_again: 2 };
        set_again_recorded(&pool, ROLE, &again).await.unwrap();
        set_again_recorded(&pool, ROLE, &BookedLook { set_again: 1, ..again.clone() }).await.unwrap();
        set_again_recorded(&pool, ROLE, &BookedLook { loop_name: "x".into(), seq: 0, set_again: 9 }).await.unwrap();
        booking.book("x", Some(now + 60_000)).await.unwrap();
        assert_eq!(alarm.cancelled(), vec![again.wake(ROLE, now + 3_600_000)]);
        booking.book("x", Some(now + 120_000)).await.unwrap();
        assert_eq!(alarm.cancelled()[1], loop_wake(ROLE, "x", now + 60_000, 2), "a new booking starts from its own wake");
    }

    /// A pass that failed and could not write that down is written down by
    /// the writer whose ring it answered, without changing when any loop
    /// is due: a loop with no row stays due now, one booked keeps its
    /// look. The loop's next drain that goes through clears it.
    #[weft_db_macros::test]
    async fn a_failed_pass_written_down_by_its_ringer_is_cleared_by_the_next_drain(pool: Pool) {
        schema(&pool).await;
        let (booking, _) = booking(&pool);
        let later = db_now(&pool).await + 3_600_000;
        booking.book("y", Some(later)).await.unwrap();
        let failed = PassFailed { loops: vec!["x".to_string(), "y".to_string()], error: "the database is down".to_string() };
        pass_failed(&pool, ROLE, &failed).await.unwrap();
        let why = "its pass could not write down how it went: the database is down".to_string();
        assert_eq!(
            written(&pool).await.into_iter().map(|w| (w.0, w.2)).collect::<Vec<_>>(),
            vec![("loop:dispatcher:x".to_string(), why.clone()), ("loop:dispatcher:y".to_string(), why.clone())]
        );
        assert_eq!(row(&pool, "y").await.0, Some(later), "a booked look is kept");
        let (x, x_runs) = counting("x", DrainStep::Done);
        let (y, y_runs) = counting("y", DrainStep::Done);
        pass(&booking, &[x, y], &TickBody::default()).await.unwrap();
        assert_eq!((x_runs.load(Ordering::SeqCst), y_runs.load(Ordering::SeqCst)), (1, 0), "x was due and still is; y waits for its look");
        assert_eq!(written(&pool).await.into_iter().map(|w| w.0).collect::<Vec<_>>(), vec!["loop:dispatcher:y".to_string()]);
    }

    /// A pass whose database stops answering partway names only the loops
    /// it could not write down: one that drained and was booked before is
    /// not among them.
    #[weft_db_macros::test]
    async fn a_failed_pass_names_only_the_loops_it_could_not_write_down(pool: Pool) {
        schema(&pool).await;
        let (booking, _) = booking(&pool);
        let (a, _) = counting("a", DrainStep::Done);
        let breaks = pool.clone();
        let b = DrainLoop::new("b", "break the table", WRITES, Duration::from_secs(30), move || {
            let pool = breaks.clone();
            async move {
                pool.batch_execute("ALTER TABLE role_loop_due RENAME TO role_loop_gone").await?;
                Ok(DrainStep::Done)
            }
        });
        let failed = pass(&booking, &[a, b], &named(&["a", "b"])).await.unwrap_err();
        assert_eq!(failed.loops, vec!["b".to_string()]);
        assert!(failed.error.contains("b: "), "{}", failed.error);
    }

    /// A pass that cannot read when the loops are due runs nothing and
    /// fails, so the queue calls the tick again, naming the loops its call
    /// names, or every loop when it names none.
    #[weft_db_macros::test]
    async fn a_pass_that_cannot_read_the_due_times_fails(pool: Pool) {
        schema(&pool).await;
        let (booking, _) = booking(&pool);
        pool.batch_execute("DROP TABLE role_loop_due").await.unwrap();
        let (x, runs) = counting("x", DrainStep::Done);
        let (y, _) = counting("y", DrainStep::Done);
        let loops = [x, y];
        assert_eq!(pass(&booking, &loops, &named(&["x"])).await.unwrap_err().loops, vec!["x".to_string()]);
        assert_eq!(pass(&booking, &loops, &TickBody::default()).await.unwrap_err().loops, vec!["x".to_string(), "y".to_string()]);
        assert_eq!(runs.load(Ordering::SeqCst), 0);
    }

    /// A loop drains with its process's watch held, and a pass that runs
    /// no loop (a wake whose booking moved since) does not take it.
    #[weft_db_macros::test]
    async fn a_pass_holds_the_watch_only_while_a_loop_runs(pool: Pool) {
        schema(&pool).await;
        let (booking, _) = booking(&pool);
        let watch = PgSignalWatch::start(pool.config(), &["chan"], crate::pg_signal::Listening::WhileBusy).await.unwrap();
        let held = Arc::new(Mutex::new(Vec::new()));
        let (seen, looking) = (held.clone(), watch.clone());
        let l = DrainLoop::new("x", "test things", WRITES, Duration::from_secs(30), move || {
            seen.lock().unwrap().push(looking.holds());
            async { Ok(DrainStep::Done) }
        });
        let loops = [l];
        let now = db_now(&pool).await;
        booking.book("x", Some(now + 600_000)).await.unwrap();
        let stale = TickBody { booked: Some(BookedLook { loop_name: "x".into(), seq: 0, set_again: 0 }), ..TickBody::default() };
        drain_due(&booking, &loops, &stale, Pass::Tick, Duration::from_secs(60), Some(&watch)).await.unwrap();
        assert!(held.lock().unwrap().is_empty() && !watch.holds(), "nothing ran, nothing held");
        drain_due(&booking, &loops, &named(&["x"]), Pass::Tick, Duration::from_secs(60), Some(&watch)).await.unwrap();
        assert_eq!(*held.lock().unwrap(), vec![true]);
        assert!(!watch.holds(), "let go once the pass ended");
    }

    /// A pass that drained a loop replaces its booking when nobody booked
    /// the loop since the pass read its row, and keeps a sooner look a
    /// sibling booked in between, so the sibling's wake is never pushed
    /// back. Read and write land in the same millisecond here (no pause
    /// between them): the seq decides, never the clock.
    #[weft_db_macros::test]
    async fn a_booking_keeps_a_sooner_look_a_sibling_wrote_since_the_read(pool: Pool) {
        schema(&pool).await;
        let (booking, _) = booking(&pool);
        let soon = Duration::from_secs(10);
        let later = Duration::from_secs(3600);
        let read_seq = |read: ReadDue| read.booked.get("x").map_or(0, |b| b.seq);

        booking.rebook("x", None, Step::Drained(Some(soon)), Rule::Exactly).await.unwrap();
        let read = read_due(&pool, ROLE.as_str()).await.unwrap();
        let read_at = read.now;
        booking.rebook("x", None, Step::Drained(Some(later)), Rule::SinceRead(read_seq(read))).await.unwrap();
        let due = row(&pool, "x").await.0.unwrap();
        assert!(due >= read_at + 3_600_000, "nobody booked since the read: the later look stands");

        let seen = read_seq(read_due(&pool, ROLE.as_str()).await.unwrap());
        booking.rebook("x", None, Step::Drained(Some(soon)), Rule::Exactly).await.unwrap();
        let sibling_due = row(&pool, "x").await.0;
        booking.rebook("x", None, Step::Drained(None), Rule::SinceRead(seen)).await.unwrap();
        assert_eq!(row(&pool, "x").await.0, sibling_due, "the sibling's look outlasts an empty booking");
        booking.rebook("x", None, Step::Drained(Some(later)), Rule::SinceRead(seen)).await.unwrap();
        assert_eq!(row(&pool, "x").await.0, sibling_due, "the sibling's sooner look is kept");
    }

    /// Scaled to zero, a loop run for the life of its process drains only
    /// when a write wakes it (no start drain, no timed look), and keeps no
    /// timer for the look its drain asks for: the tick books it.
    #[weft_db_macros::test]
    async fn a_working_process_never_looks_on_a_timer(pool: Pool) {
        schema(&pool).await;
        let watch = PgSignalWatch::start(pool.config(), &["chan"], crate::pg_signal::Listening::WhileBusy).await.unwrap();
        let runs = Arc::new(AtomicU32::new(0));
        let counter = runs.clone();
        let l = DrainLoop::new("woken", "test things", WRITES, Duration::from_millis(200), move || {
            counter.fetch_add(1, Ordering::SeqCst);
            async { Ok(DrainStep::RetryIn(Duration::from_millis(100))) }
        });
        let subscription = watch.subscribe();
        let looks = Looks::WhileWorking(watch.clone());
        tokio::spawn(async move { l.run_forever(subscription, looks).await });
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(runs.load(Ordering::SeqCst), 0, "no start drain, no safety look");
        pool.execute("SELECT pg_notify('chan', '')", &[]).await.unwrap();
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(runs.load(Ordering::SeqCst), 1, "a write wakes it");
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(runs.load(Ordering::SeqCst), 1, "and nothing after it, not even the look it asked for");
        let rows: i64 = pool.scalar("SELECT count(*) FROM role_loop_due", &[]).await.unwrap();
        assert_eq!(rows, 0, "and it books nothing: the tick rung for the same write does");
    }

    /// A row of a loop this role no longer has is forgotten once nobody
    /// booked it for a day; one booked lately is left to the other weft
    /// that may still run it.
    #[weft_db_macros::test]
    async fn a_loop_this_role_no_longer_has_is_forgotten(pool: Pool) {
        schema(&pool).await;
        let (booking, _) = booking(&pool);
        pool.execute("INSERT INTO role_loop_due (role, loop_name, due_ms, written_ms, seq) VALUES ($1, 'renamed', 5, 0, 3)", &[&ROLE.as_str()])
            .await
            .unwrap();
        let now = db_now(&pool).await;
        booking.book("another_wefts", Some(now + 60_000)).await.unwrap();
        for name in ["renamed", "another_wefts"] {
            crate::unanswered::failed(&pool, Callee::Loop { role: ROLE.as_str(), name }, None, "it failed").await.unwrap().log();
        }
        let (fresh, _) = counting("fresh", DrainStep::Done);
        pass(&booking, &[fresh], &TickBody::default()).await.unwrap();
        let names: Vec<String> = pool.scalars("SELECT loop_name FROM role_loop_due ORDER BY loop_name", &[]).await.unwrap();
        assert_eq!(names, vec!["another_wefts".to_string(), "fresh".to_string()]);
        let written: Vec<String> = written(&pool).await.into_iter().map(|w| w.0).collect();
        assert_eq!(written, vec!["loop:dispatcher:another_wefts".to_string()], "its failure written down goes with it");
    }
}
