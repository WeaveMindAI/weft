//! Which worker processes are alive (`worker_lease`), the image each runs,
//! what each drives right now per trigger (`worker_lease.in_flight`), and what each counted
//! at its door this minute (`door_count`), all written once a second by
//! every worker that has work under way, through the broker
//! (`/v1/door/tick`). A worker with nothing under way stops ticking after
//! one last tick that says so, and its lease lapses: it owns no run then,
//! and it ticks again before it takes one. A worker whose runs only wait on
//! something outside them stops ticking too, after a last tick that vouches
//! for it until the earliest of those waits gives up ([`lease_until`]), so
//! its lease holds that long. A worker's live lease is half of what says a
//! run is being worked on ([`crate::in_flight_sql!`]), and what it drives
//! is what a drain waits for.

/// How long a worker's lease (`worker_lease`) holds without a tick, at
/// this install's pace: a few missed ticks, so a busy second never reads
/// as a dead worker. A run its lapsed lease owned is lost.
pub const WORKER_LEASE_SECS: i64 = 15;

/// Until when (unix seconds) a tick at `now` keeps a worker's lease: one
/// lease length (`lease_secs`, [`WORKER_LEASE_SECS`] at this install's
/// pace), or, for the last tick of a worker whose runs only wait on
/// something outside them, until `vouch_until` (the earliest moment one of
/// those waits gives up, and a lease length). A vouch is held to the
/// longest a wait can hold (`weft_core::run_settings::MAX_HOLD_SECS`), and
/// one shorter than a lease length changes nothing.
pub fn lease_until(now: i64, lease_secs: i64, vouch_until: Option<i64>) -> i64 {
    let renewed = now.saturating_add(lease_secs);
    let longest = renewed.saturating_add(i64::from(weft_core::run_settings::MAX_HOLD_SECS));
    vouch_until.map_or(renewed, |vouch| vouch.clamp(renewed, longest))
}

/// The one rule for a worker being alive: its lease (`worker_lease` under
/// the alias `$lease`) is ahead of the database's clock. Every reader of a
/// worker's own liveness (the copies a door counts with, a run seed the
/// broker takes, what a drain counts) spells it through here; whether a
/// run is driven is [`crate::in_flight_sql!`]'s.
#[macro_export]
macro_rules! worker_alive {
    ($lease:literal) => {
        concat!($lease, ".leased_until_unix >= EXTRACT(EPOCH FROM NOW())::BIGINT")
    };
}

/// The channel a worker's lease coming alive is announced on: its row
/// made, or renewed after it had lapsed (a worker that went idle taking
/// work again). Once per stretch of a worker's work, never per tick: the
/// dispatcher's `lapses` loop wakes on it, so a run born at that worker's
/// door is noticed if the worker dies under it, though no dispatcher loop
/// saw the run begin. (The dispatcher's delivery hears a lease coming
/// alive only for a project with queued runs whose workers could not be
/// reached, on a channel of its own, `worker_lease.alive_since` saying
/// which runs it takes again.) It is also sent when a worker's tick shortens a lease it had
/// vouched for (the look at lapsed leases was booked for the longer one);
/// and by the broker when a worker bore a run before its lease was ever
/// written, which it learns from the record's answer
/// (`weft_journal::record::Recorded::unwatched`, `weft_broker::notices`).
/// A `NOTIFY` of its own rather than the announcement
/// outbox: the tick does not poke the outbox's flusher, and a notification
/// this rare costs the commit order nothing.
// SYNC: LEASE_ALIVE_CHANNEL <-> 'weft_lease_alive' in worker_lease_alive_notify and weft_door_tick (GROUP)
pub const LEASE_ALIVE_CHANNEL: &str = "weft_lease_alive";

/// How long a lease row is kept once it lapsed: long enough that every
/// run its process owned was let go of first.
pub const LEASES_KEPT_SECS: i64 = 3600;

/// How long the counts of a minute are kept after it began: `weft status`
/// reads the refusals of this minute and the one before.
pub const COUNTS_KEPT_SECS: i64 = 180;

/// A worker's memory note: its memory is nearly full (`weft_engine::busy`),
/// since the reading it crossed the line at. Kept on its own lease row
/// while it lasts and taken off once the memory has room again: every
/// tick that writes the row carries it (`weft_door_tick`), and a crossing
/// tells it at once to a row already there ([`keep_memory_state`]). So a
/// note on a worker that went away says it went away full: the lost-run
/// sweep copies it onto each run it takes from that worker
/// (`run.lost_memory_full_at`), and the worker that claims the run reads
/// it with its claim (`weft_journal::record::Claimed`) and says the worker
/// ran out of memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MemoryNote {
    /// How many bytes it used, and of how many, when it crossed.
    pub used_bytes: u64,
    pub limit_bytes: u64,
    /// When it crossed (unix seconds, on the worker's own clock).
    pub at_unix: u64,
}

/// Keep the memory state of worker `replica` of `project` on its lease
/// row, when it has one: `full` while its memory is nearly full, `None`
/// (the note taken off) once it has room again. A worker whose row is not
/// written yet, or was dropped while it slept ([`LEASES_KEPT_SECS`]),
/// keeps it nowhere here: the tick that writes its row carries it.
pub async fn keep_memory_state(pool: &crate::db::Pool, replica: &str, project: uuid::Uuid, full: Option<&MemoryNote>) -> anyhow::Result<()> {
    use crate::db::Client as _;
    let (at, used, limit) = memory_columns(full)?;
    pool.execute(
        "UPDATE worker_lease SET memory_full_at = $3, memory_used = $4, memory_limit = $5 WHERE replica = $1 AND project_id = $2",
        &[&replica, &project, &at, &used, &limit],
    )
    .await?;
    Ok(())
}

/// A memory note's three lease columns (`memory_full_at`, `memory_used`,
/// `memory_limit`), all NULL for none.
pub fn memory_columns(full: Option<&MemoryNote>) -> anyhow::Result<(Option<i64>, Option<i64>, Option<i64>)> {
    let bigint = |n: u64| i64::try_from(n).map_err(|_| anyhow::anyhow!("a memory note holds {n}, past what a BIGINT stores"));
    Ok(match full {
        Some(note) => (Some(bigint(note.at_unix)?), Some(bigint(note.used_bytes)?), Some(bigint(note.limit_bytes)?)),
        None => (None, None, None),
    })
}

/// The memory note a claim reads (`weft_journal::record::claim`), from
/// the run's copy of the lease row of the worker it was lost by
/// (`run.lost_memory_full_at`): its columns, all set or all NULL.
pub fn memory_note_of(at: Option<i64>, used: Option<i64>, limit: Option<i64>) -> anyhow::Result<Option<MemoryNote>> {
    let unsigned = |n: i64| u64::try_from(n).map_err(|_| anyhow::anyhow!("a stored memory note holds {n}"));
    match (at, used, limit) {
        (Some(at), Some(used), Some(limit)) => Ok(Some(MemoryNote { at_unix: unsigned(at)?, used_bytes: unsigned(used)?, limit_bytes: unsigned(limit)? })),
        (None, None, None) => Ok(None),
        partly => anyhow::bail!("a stored memory note is only partly there: {partly:?}"),
    }
}

/// How many rows of each table one [`prune`] deletes at most.
const PRUNE_BATCH: i64 = 1000;

/// Drop counts no limit reads any more and leases long lapsed, a bounded
/// batch of each, skipping rows a tick holds right now. Both tables are
/// written by the door tick, so this runs beside the dispatcher's look at
/// lapsed leases, never on the tick.
pub async fn prune(pool: &crate::db::Pool, now: i64) -> anyhow::Result<()> {
    use crate::db::Client as _;
    pool.execute(
        "DELETE FROM door_count WHERE ctid IN \
         (SELECT ctid FROM door_count WHERE window_start < $1 LIMIT $2 FOR UPDATE SKIP LOCKED)",
        &[&(now - COUNTS_KEPT_SECS), &PRUNE_BATCH],
    )
    .await?;
    pool.execute(
        "DELETE FROM worker_lease WHERE replica IN \
         (SELECT replica FROM worker_lease WHERE leased_until_unix < $1 LIMIT $2 FOR UPDATE SKIP LOCKED)",
        &[&(now - LEASES_KEPT_SECS), &PRUNE_BATCH],
    )
    .await?;
    Ok(())
}

/// Count one fire or call of trigger `token` of `project` the install
/// dropped itself, before any worker's door saw it (its trigger has no
/// armed program), with `reason`: under `counter` (the dispatcher copy
/// that dropped it), beside what the workers' doors count, so `weft
/// status` reads both the same way. One statement; a drop is the path
/// that pays for it.
pub async fn count_drop(pool: &crate::db::Pool, project: uuid::Uuid, token: &str, counter: &str, reason: &str, now: i64) -> anyhow::Result<()> {
    use crate::db::Client as _;
    let (window_start, _) = weft_core::signal::limits::window(now);
    pool.execute(
        "INSERT INTO door_count (key, window_start, replica, project_id, hits, reason) VALUES ($1, $2, $3, $4, 1, $5) \
         ON CONFLICT (key, window_start, replica) DO UPDATE SET hits = door_count.hits + 1, reason = EXCLUDED.reason \
         WHERE door_count.project_id = EXCLUDED.project_id",
        &[&weft_core::signal::limits::dropped_key(token), &window_start, &counter, &project, &weft_core::signal::limits::drop_reason(reason)],
    )
    .await?;
    Ok(())
}

pub static GROUP: crate::SchemaGroup = crate::SchemaGroup {
    name: "worker_door",
    tables: &["worker_lease", "door_count"],
    ddl: &[
        // One row per worker process: alive while `leased_until_unix` is
        // ahead. Renewed by every tick; a process that stopped ticking is
        // gone. Rows of processes long gone are dropped by the sweep.
        r#"CREATE TABLE IF NOT EXISTS worker_lease (
            replica TEXT PRIMARY KEY,
            project_id UUID NOT NULL,
            tenant_id TEXT NOT NULL,
            leased_until_unix BIGINT NOT NULL,
            -- The image it runs (the program's binary hash); NULL for a
            -- process that runs no program (a node test's).
            binary_hash TEXT,
            -- How many runs it drives right now, per trigger token
            -- (`{"<token>": n}`): what a drain counts as work in flight.
            in_flight JSONB NOT NULL DEFAULT '{}'::jsonb,
            -- When its lease last came alive (made, or renewed after it
            -- had lapsed), on the database's clock: a queued run its
            -- project's workers could not be reached for is tried again
            -- once a worker came alive after its tries stopped
            -- (`weft_dispatcher::delivery`).
            alive_since BIGINT NOT NULL DEFAULT 0,
            -- Its memory note while its memory is nearly full: when it
            -- crossed the line (unix seconds, on its own clock), and how
            -- many bytes it used of how many then (`weft_engine::busy`).
            -- NULL while it has room. The lost-run sweep copies it onto
            -- each run it takes from the worker once it went away
            -- (`run.lost_memory_full_at`), to say the worker ran out of
            -- memory.
            memory_full_at BIGINT,
            memory_used BIGINT,
            memory_limit BIGINT
        )"#,
        r#"CREATE INDEX IF NOT EXISTS idx_worker_lease_project ON worker_lease(project_id, leased_until_unix)"#,
        // SYNC: 'weft_lease_alive' <-> LEASE_ALIVE_CHANNEL
        r#"CREATE OR REPLACE FUNCTION worker_lease_alive_notify() RETURNS trigger AS $$
            BEGIN
                PERFORM pg_notify('weft_lease_alive', NEW.replica);
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql"#,
        // Only a row really inserted: the tick's `ON CONFLICT` update fires
        // the update trigger instead.
        r#"DROP TRIGGER IF EXISTS worker_lease_alive_on_insert ON worker_lease"#,
        r#"CREATE TRIGGER worker_lease_alive_on_insert
            AFTER INSERT ON worker_lease
            FOR EACH ROW
            EXECUTE FUNCTION worker_lease_alive_notify()"#,
        // A lapsed lease renewed: the worker went quiet and works again.
        // SYNC: the WHEN <-> weft_dispatcher's journal group (worker_lease_workers_back_on_renewal)
        r#"DROP TRIGGER IF EXISTS worker_lease_alive_on_renewal ON worker_lease"#,
        r#"CREATE TRIGGER worker_lease_alive_on_renewal
            AFTER UPDATE OF leased_until_unix ON worker_lease
            FOR EACH ROW
            WHEN (OLD.leased_until_unix < EXTRACT(EPOCH FROM NOW())::BIGINT)
            EXECUTE FUNCTION worker_lease_alive_notify()"#,
        // SYNC: door_count keys <-> crates/weft-core/src/signal/limits.rs (ran_key, failed_key, dropped_key)
        // What one worker counted at its door in one minute, per key
        // (`c:<token>:<caller>`, `e:<token>`, `refused:<token>:<limit>`,
        // `ran:<token>`, `failed:<token>`, `dropped:<token>`):
        // its own total so far, replaced by each tick, under its project
        // (a copy hears only its own project's counts). UNLOGGED: a crash
        // loses a minute's counts, which only ever lets a few extra calls
        // through.
        r#"CREATE UNLOGGED TABLE IF NOT EXISTS door_count (
            key TEXT NOT NULL,
            window_start BIGINT NOT NULL,
            replica TEXT NOT NULL,
            project_id UUID NOT NULL,
            hits BIGINT NOT NULL,
            -- The last reason the worker gave for the count, for a key
            -- that keeps one (a dropped fire's); NULL for every other.
            reason TEXT,
            PRIMARY KEY (key, window_start, replica)
        )"#,
        r#"CREATE INDEX IF NOT EXISTS idx_door_count_window ON door_count(window_start)"#,
        // One tick of a worker: its lease renewed with what it drives
        // now and its memory note (NULLs while it has room), its counts of the minute stored (each with its reason, for
        // a key that keeps one), and what the project's
        // other copies counted (keys are `<what>:<token>:...`; every key
        // but the refused-token ones, `t:-:<address>`, which each copy
        // bounds alone), with how many copies are alive; and, on a tick
        // that shortens a lease the worker vouched for, the look at lapsed
        // leases rung.
        // One statement, one round trip.
        // SYNC: the answer's shape <-> weft_broker_client::protocol::DoorTick
concat!(        r#"CREATE OR REPLACE FUNCTION weft_door_tick(
                p_replica TEXT, p_project UUID, p_tenant TEXT, p_binary_hash TEXT, p_leased_until BIGINT,
                p_in_flight JSONB, p_window BIGINT, p_keys TEXT[], p_hits BIGINT[], p_reasons TEXT[], p_shortens BOOLEAN,
                p_memory_full_at BIGINT, p_memory_used BIGINT, p_memory_limit BIGINT
            ) RETURNS JSONB AS $$
            BEGIN
                INSERT INTO worker_lease (replica, project_id, tenant_id, binary_hash, leased_until_unix, in_flight, alive_since,
                                          memory_full_at, memory_used, memory_limit)
                    VALUES (p_replica, p_project, p_tenant, p_binary_hash, p_leased_until, p_in_flight,
                            EXTRACT(EPOCH FROM NOW())::BIGINT, p_memory_full_at, p_memory_used, p_memory_limit)
                    ON CONFLICT (replica) DO UPDATE
                        SET leased_until_unix = EXCLUDED.leased_until_unix, in_flight = EXCLUDED.in_flight,
                            memory_full_at = EXCLUDED.memory_full_at, memory_used = EXCLUDED.memory_used,
                            memory_limit = EXCLUDED.memory_limit,
                            alive_since = CASE WHEN "#, worker_alive!("worker_lease"), r#"
                                               THEN worker_lease.alive_since ELSE EXCLUDED.alive_since END
                    WHERE worker_lease.project_id = EXCLUDED.project_id;
                INSERT INTO door_count (key, window_start, replica, project_id, hits, reason)
                    SELECT k, p_window, p_replica, p_project, h, r FROM unnest(p_keys, p_hits, p_reasons) AS t(k, h, r)
                    ON CONFLICT (key, window_start, replica) DO UPDATE SET hits = EXCLUDED.hits, reason = EXCLUDED.reason
                    WHERE door_count.project_id = EXCLUDED.project_id;
                -- This tick's lease ends before the one the worker vouched
                -- for last: the look at lapsed leases, booked for the
                -- longer one, is rung again.
                -- SYNC: 'weft_lease_alive' <-> LEASE_ALIVE_CHANNEL
                IF p_shortens THEN
                    PERFORM pg_notify('weft_lease_alive', p_replica);
                END IF;
                RETURN jsonb_build_object(
                    'others', COALESCE((
                        SELECT jsonb_agg(jsonb_build_object('key', o.key, 'hits', o.hits))
                        FROM (SELECT key, SUM(hits)::bigint AS hits FROM door_count
                              WHERE window_start = p_window AND replica <> p_replica
                                AND project_id = p_project
                                -- SYNC: '-' <-> crates/weft-engine/src/door/limits.rs (invalid_tokens_key)
                                AND split_part(key, ':', 2) <> '-' 
                              GROUP BY key) o), '[]'::jsonb),
                    'copies', (SELECT COUNT(*) FROM worker_lease l
                               WHERE l.project_id = p_project
                                 AND "#, worker_alive!("l"), r#"));
            END;
            $$ LANGUAGE plpgsql"#),
    ],
    seed: &[],
    data_steps: &[],
};

#[cfg(test)]
mod tests {
    use super::*;

    /// A tick keeps the lease one length; a vouch keeps it until then, held
    /// to the longest a wait can hold, and never shorter than a tick.
    #[test]
    fn a_vouch_keeps_the_lease_until_then_within_the_longest_hold() {
        let longest = i64::from(weft_core::run_settings::MAX_HOLD_SECS);
        assert_eq!(lease_until(1_000, 15, None), 1_015);
        assert_eq!(lease_until(1_000, 15, Some(5_000)), 5_000);
        assert_eq!(lease_until(1_000, 15, Some(1_001)), 1_015, "a vouch shorter than a lease changes nothing");
        assert_eq!(lease_until(1_000, 15, Some(i64::MAX)), 1_015 + longest, "held to the longest hold");
    }
}
