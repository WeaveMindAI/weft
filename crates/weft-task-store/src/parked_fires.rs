//! A trigger's queue of events waiting to become runs (`parked_fire`): the
//! events a parked trigger keeps, the ones that could not become a run yet
//! (what they read was not ready, the worker was full or out of reach),
//! and the answers waiting for a run that cannot take them now. The
//! install's door and a worker's door both park through [`park`]; the
//! dispatcher drains each trigger's queue head by head once it can take
//! them ([`take_head`]), and a run's owner takes the answers handed to it
//! ([`hand_answer_in`], [`answers_for`]).

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The channel an event parked for a trigger is announced on, with its
/// project's id: the dispatcher's drain wakes on it, and so does a worker
/// holding for the answers of a run it drives.
// SYNC: PARKED_FIRE_CHANNEL <-> 'weft_parked_fire' in parked_fire_notify (GROUP below)
pub const PARKED_FIRE_CHANNEL: &str = "weft_parked_fire";

/// The events waiting for their trigger, one row each, first in first out
/// per trigger (`seq`): the events a parked trigger keeps, the ones that
/// could not become a run yet, and the answers waiting for a run that
/// cannot take them now (it is parked behind a parked trigger, or another
/// worker drives it). A drain takes only each token's head row, under
/// `FOR UPDATE SKIP LOCKED`, so two drains never hand one event over twice.
pub static GROUP: crate::SchemaGroup = crate::SchemaGroup {
    name: "parked_fire",
    tables: &["parked_fire"],
    ddl: &[
        r#"CREATE TABLE IF NOT EXISTS parked_fire (
            -- The trigger's signal token, or the token a waiting run's
            -- answer resolves.
            token TEXT NOT NULL,
            seq BIGINT GENERATED ALWAYS AS IDENTITY,
            -- The event's identity: the run it starts is
            -- `weft_core::door_fire::run_of_fire(fire_id)`, so an event
            -- handed over twice is born once.
            fire_id UUID NOT NULL UNIQUE,
            -- What the trigger wakes with: every event waits here already
            -- processed by its kind (`/process`).
            payload JSONB NOT NULL,
            -- Who sent it, for a trigger somebody outside calls.
            caller TEXT,
            -- How many times it failed to become a run, and the moment it
            -- may be tried again (`park_backoff_secs`).
            attempts INTEGER NOT NULL DEFAULT 0,
            not_before BIGINT NOT NULL,
            -- Set while it waits on what its instance provides: the
            -- refusal, naming each field. Not retried on a timer: the
            -- instance's next change of values routes it again.
            instance_gap JSONB,
            -- An answer to a waiting run, rather than an event of an entry.
            is_resume BOOLEAN NOT NULL,
            -- Set on an answer already taken off its wait (the wait's
            -- signal is gone) for a run a worker drives: the run it is
            -- for. Its worker takes it; if the worker lets go of the run
            -- first, the letting go writes it into the run's record
            -- (`weft_journal::record::resolve_handed_in`).
            execution_id UUID,
            PRIMARY KEY (token, seq)
        )"#,
        r#"CREATE INDEX IF NOT EXISTS parked_fire_handed ON parked_fire (execution_id) WHERE execution_id IS NOT NULL"#,
        // Wake the drain when an event is parked, so a trigger that is
        // already back takes it at once instead of on the drain's next look,
        // and a run's worker when an answer is handed to it. Through the
        // outbox (`crate::announce`): these are writes on a run's path, and
        // their writers poke the flusher once they commit.
        // SYNC: 'weft_parked_fire' <-> PARKED_FIRE_CHANNEL
        r#"CREATE OR REPLACE FUNCTION parked_fire_notify() RETURNS trigger AS $$
            BEGIN
                PERFORM weft_announce('weft_parked_fire', COALESCE(
                    (SELECT s.project_id FROM signal s WHERE s.token = NEW.token),
                    (SELECT r.project_id FROM run r WHERE r.execution_id = NEW.execution_id))::text);
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql"#,
        r#"DROP TRIGGER IF EXISTS parked_fire_notify_on_insert ON parked_fire"#,
        r#"CREATE TRIGGER parked_fire_notify_on_insert
            AFTER INSERT ON parked_fire
            FOR EACH ROW
            EXECUTE FUNCTION parked_fire_notify()"#,
        // A signal going takes the events waiting for it along, whoever
        // removes it (a wipe, an answer, a project going): nothing could
        // hand them over any more. An answer already handed to its run
        // stays: its run's worker takes it.
        r#"CREATE OR REPLACE FUNCTION parked_fire_drop_with_signal() RETURNS trigger AS $$
            BEGIN
                DELETE FROM parked_fire WHERE token = OLD.token AND execution_id IS NULL;
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql"#,
        r#"DROP TRIGGER IF EXISTS parked_fire_drop_on_signal_delete ON signal"#,
        r#"CREATE TRIGGER parked_fire_drop_on_signal_delete
            AFTER DELETE ON signal
            FOR EACH ROW
            EXECUTE FUNCTION parked_fire_drop_with_signal()"#,
    ],
    seed: &[],
};

/// An event as it waits in its trigger's queue: what [`park`] writes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Waiting {
    /// The event's identity: the run it starts is
    /// `weft_core::door_fire::run_of_fire(fire_id)`, so an event handed
    /// over again (a drain retried after a crash, a park that raced its
    /// own start) is born once.
    pub fire_id: uuid::Uuid,
    /// What the trigger wakes with, already processed by its kind.
    pub payload: Value,
    /// Who sent it, for a trigger somebody outside calls
    /// (`weft_core::door_fire::DoorFire::caller`): what its per-caller
    /// limit counts when it is handed over.
    pub caller: Option<String>,
    /// How many times it failed to become a run. Zero for an event parked
    /// by its trigger's standing. Drives the backoff ([`park_backoff_secs`]).
    pub attempts: u32,
    /// Not handed over before this instant (unix seconds).
    pub not_before: i64,
    /// Set when it waits on what its instance provides: the refusal,
    /// naming each field. Not retried on a timer: the instance's next
    /// change of values routes it again. Shown per trigger by `weft status`
    /// and `ctx.instances().list()`.
    pub instance_gap: Option<String>,
}

/// Seconds an event waits before its `attempts`-th retry: 1, 2, 4, ...
/// doubling, capped at five minutes. The first failure retries almost at
/// once (a transient read); an event that keeps failing settles at the cap
/// and the dispatcher's parked-fire sweep picks it up when due.
pub fn park_backoff_secs(attempts: u32) -> i64 {
    const CAP_SECS: i64 = 300;
    if attempts == 0 {
        return 0;
    }
    1i64.checked_shl(attempts - 1).unwrap_or(CAP_SECS).min(CAP_SECS)
}

/// How many events one entry's queue holds at most. Entry events
/// accumulate while a trigger is parked; an unbounded queue lets an
/// external caller who knows a public path grow it without limit, so a
/// flood is refused loudly (`QueueFull`). A run's wait holds one answer.
pub const MAX_PARKED_ENTRY_FIRES: i64 = 1000;

/// An event that could not become a run now, as it waits: its
/// `attempts`-th failure, retried after its backoff.
pub fn waiting(fire_id: uuid::Uuid, payload: Value, caller: Option<String>, attempts: u32, instance_gap: Option<String>) -> Waiting {
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_secs() as i64);
    Waiting { fire_id, payload, caller, attempts, not_before: now + park_backoff_secs(attempts), instance_gap }
}

/// THE one park. Every park site (an answer whose run cannot take it now, a
/// worker's door, the install's fire path when the worker is out of reach)
/// goes through here, under the trigger's signal row lock, so the guards
/// hold together:
///   - a waiting run's answer is kept only while none waits for it already
///     (one answer resolves one wait); an entry's events while its queue is
///     under [`MAX_PARKED_ENTRY_FIRES`];
///   - an event a held connection picked up (`held_by`) is kept only while
///     its holder still holds the signal; once kept it is the trigger's;
///   - an event already waiting (its `fire_id`) is not kept twice.
pub async fn park(pool: &sqlx::PgPool, token: &str, waiting: &Waiting, held_by: Option<&str>) -> anyhow::Result<ParkAppend> {
    let mut tx = pool.begin().await?;
    let signal: Option<(bool, bool)> = sqlx::query_as(
        "SELECT is_resume, ($2::text IS NULL OR (holds AND held_by = $2)) FROM signal WHERE token = $1 FOR UPDATE",
    )
    .bind(token)
    .bind(held_by)
    .fetch_optional(&mut *tx)
    .await?;
    let Some((is_resume, held)) = signal else { return Ok(ParkAppend::Refused(ParkRefusal::RowGone)) };
    if !held {
        return Ok(ParkAppend::Refused(ParkRefusal::NotHeld));
    }
    let (queued, already): (i64, bool) = sqlx::query_as(
        "SELECT count(*), COALESCE(bool_or(fire_id = $2), FALSE) FROM parked_fire WHERE token = $1",
    )
    .bind(token)
    .bind(waiting.fire_id)
    .fetch_one(&mut *tx)
    .await?;
    if already {
        return Ok(ParkAppend::Refused(ParkRefusal::AlreadyQueued));
    }
    if is_resume && queued > 0 {
        return Ok(ParkAppend::Refused(ParkRefusal::ResumeAlreadyAnswered));
    }
    if !is_resume && queued >= MAX_PARKED_ENTRY_FIRES {
        return Ok(ParkAppend::Refused(ParkRefusal::QueueFull));
    }
    let inserted = sqlx::query(
        "INSERT INTO parked_fire (token, fire_id, payload, caller, attempts, not_before, instance_gap, is_resume) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT (fire_id) DO NOTHING",
    )
    .bind(token)
    .bind(waiting.fire_id)
    .bind(&waiting.payload)
    .bind(&waiting.caller)
    .bind(waiting.attempts as i32)
    .bind(waiting.not_before)
    .bind(waiting.instance_gap.as_ref().map(sqlx::types::Json))
    .bind(is_resume)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    crate::announce::committed(pool);
    // The id is unique across tokens: an event parked under another token
    // already (it cannot be) would read as queued.
    Ok(if inserted.rows_affected() == 0 { ParkAppend::Refused(ParkRefusal::AlreadyQueued) } else { ParkAppend::Parked })
}

/// Outcome of [`park`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParkAppend {
    Parked,
    Refused(ParkRefusal),
}

/// Why [`park`] refused, one per guard plus the signal being gone. Each is a
/// different fact for the caller: a retry that finds its event already
/// waiting has lost nothing; a cap has refused a NEW event, which is a loss
/// the caller must say out loud; a vanished signal means the trigger or
/// the wait is gone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParkRefusal {
    /// The event (its `fire_id`) is already waiting.
    AlreadyQueued,
    /// A run's wait whose one answer is already waiting.
    ResumeAlreadyAnswered,
    /// An entry's queue at [`MAX_PARKED_ENTRY_FIRES`].
    QueueFull,
    /// No signal for this token.
    RowGone,
    /// The holder it was picked up under (a held connection's) no longer
    /// holds the signal: another holder serves its events.
    NotHeld,
}

/// The head of one trigger's queue, as a drain took it.
#[derive(Debug, Clone, PartialEq)]
pub struct Head {
    pub token: String,
    pub seq: i64,
    pub waiting: Waiting,
    pub is_resume: bool,
}

/// Take the head of `token`'s queue on the caller's transaction, locked
/// for it (`FOR UPDATE SKIP LOCKED`: a head another drain holds is
/// skipped, and the row behind it is no head while it waits). `None` when
/// the queue is empty, its head is locked, or its head is not due at `now`
/// (backing off, or waiting on its instance): one trigger's events never
/// overtake each other.
pub async fn take_head(tx: &mut sqlx::PgConnection, token: &str, now: i64) -> anyhow::Result<Option<Head>> {
    let row: Option<HeadRow> = sqlx::query_as(
        "SELECT token, seq, fire_id, payload, caller, attempts, not_before, instance_gap, is_resume FROM parked_fire p \
         WHERE p.token = $1 AND p.seq = (SELECT min(q.seq) FROM parked_fire q WHERE q.token = $1) \
           AND p.not_before <= $2 AND p.instance_gap IS NULL \
         FOR UPDATE SKIP LOCKED",
    )
    .bind(token)
    .bind(now)
    .fetch_optional(&mut *tx)
    .await?;
    Ok(row.map(HeadRow::into_head))
}

/// The heads of `token`'s queue a person or an instance's change routes
/// again, whatever their backoff: the same as [`take_head`] but due now.
pub async fn take_head_now(tx: &mut sqlx::PgConnection, token: &str) -> anyhow::Result<Option<Head>> {
    let row: Option<HeadRow> = sqlx::query_as(
        "SELECT token, seq, fire_id, payload, caller, attempts, not_before, instance_gap, is_resume FROM parked_fire p \
         WHERE p.token = $1 AND p.seq = (SELECT min(q.seq) FROM parked_fire q WHERE q.token = $1) \
         FOR UPDATE SKIP LOCKED",
    )
    .bind(token)
    .fetch_optional(&mut *tx)
    .await?;
    Ok(row.map(HeadRow::into_head))
}

/// A head handed over: it goes.
pub async fn remove_in(tx: &mut sqlx::PgConnection, head: &Head) -> anyhow::Result<()> {
    sqlx::query("DELETE FROM parked_fire WHERE token = $1 AND seq = $2")
        .bind(&head.token)
        .bind(head.seq)
        .execute(&mut *tx)
        .await?;
    Ok(())
}

/// A head that could not be handed over: it stays the head, tried again at
/// `not_before` after `attempts` failures, or, waiting on its instance
/// (`instance_gap`), on the instance's next change.
pub async fn restamp_in(tx: &mut sqlx::PgConnection, head: &Head, attempts: u32, not_before: i64, instance_gap: Option<&str>) -> anyhow::Result<()> {
    sqlx::query("UPDATE parked_fire SET attempts = $3, not_before = $4, instance_gap = $5 WHERE token = $1 AND seq = $2")
        .bind(&head.token)
        .bind(head.seq)
        .bind(attempts as i32)
        .bind(not_before)
        .bind(instance_gap.map(sqlx::types::Json))
        .execute(&mut *tx)
        .await?;
    Ok(())
}

/// Hand `value`, the answer to the wait `token` of run `execution_id`,
/// which a worker drives, to that worker, on the caller's transaction:
/// nobody but a run's worker writes the record of a run it drives. The
/// wait's signal is consumed by the caller in the same transaction, so it
/// is answered once.
pub async fn hand_answer_in(tx: &mut sqlx::PgConnection, token: &str, execution_id: weft_core::ExecutionId, value: &Value) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO parked_fire (token, fire_id, payload, attempts, not_before, is_resume, execution_id) \
         VALUES ($1, $2, $3, 0, 0, TRUE, $4)",
    )
    .bind(token)
    .bind(uuid::Uuid::new_v4())
    .bind(value)
    .bind(execution_id)
    .execute(&mut *tx)
    .await?;
    Ok(())
}

/// The answers handed to run `execution_id` ([`hand_answer_in`]), oldest
/// first: what the worker driving the run takes. They go once the run's
/// record holds them (`weft_record_batch` deletes the answers a batch
/// resolves).
pub async fn answers_for<'e, E: sqlx::PgExecutor<'e>>(executor: E, execution_id: weft_core::ExecutionId) -> anyhow::Result<Vec<(String, Value)>> {
    Ok(sqlx::query_as("SELECT token, payload FROM parked_fire WHERE execution_id = $1 ORDER BY seq")
        .bind(execution_id)
        .fetch_all(executor)
        .await?)
}

#[derive(sqlx::FromRow)]
struct HeadRow {
    token: String,
    seq: i64,
    fire_id: uuid::Uuid,
    payload: Value,
    caller: Option<String>,
    attempts: i32,
    not_before: i64,
    instance_gap: Option<sqlx::types::Json<String>>,
    is_resume: bool,
}

impl HeadRow {
    fn into_head(self) -> Head {
        Head {
            token: self.token,
            seq: self.seq,
            waiting: Waiting {
                fire_id: self.fire_id,
                payload: self.payload,
                caller: self.caller,
                attempts: self.attempts.max(0) as u32,
                not_before: self.not_before,
                instance_gap: self.instance_gap.map(|gap| gap.0),
            },
            is_resume: self.is_resume,
        }
    }
}

#[cfg(test)]
mod park_backoff_tests {
    use super::*;

    /// 1, 2, 4, ... doubling from the first failed route, capped at five
    /// minutes, and never overflowing on an absurd count.
    #[test]
    fn backoff_doubles_from_one_second_and_caps() {
        assert_eq!(park_backoff_secs(0), 0);
        assert_eq!(park_backoff_secs(1), 1);
        assert_eq!(park_backoff_secs(2), 2);
        assert_eq!(park_backoff_secs(5), 16);
        assert_eq!(park_backoff_secs(9), 256);
        assert_eq!(park_backoff_secs(10), 300);
        assert_eq!(park_backoff_secs(40), 300);
        assert_eq!(park_backoff_secs(u32::MAX), 300);
    }

    /// An event parked now waits its backoff: none for one that never
    /// failed.
    #[test]
    fn an_event_waits_its_backoff() {
        let fresh = waiting(uuid::Uuid::nil(), Value::Null, None, 0, None);
        let failed = waiting(uuid::Uuid::nil(), Value::Null, None, 3, None);
        assert_eq!(failed.not_before - fresh.not_before, 4);
    }
}
