//! Which worker processes are alive (`worker_lease`), the image each runs,
//! what each drives right now per trigger (`worker_lease.in_flight`), and what each counted
//! at its door this minute (`door_count`), all written once a second by
//! every worker through the broker (`/v1/door/tick`). A worker's live
//! lease is half of what says a run is being worked on
//! ([`crate::in_flight_sql!`]), and what it drives is what a drain waits
//! for.

/// How long a worker's lease (`worker_lease`) holds without a tick, at
/// this install's pace: a few missed ticks, so a busy second never reads
/// as a dead worker. A run its lapsed lease owned is lost.
pub const WORKER_LEASE_SECS: i64 = 15;

/// The one rule for a worker being alive: its lease (`worker_lease` under
/// the alias `$lease`) is ahead of the database's clock. Every reader
/// (whether a run is driven, the copies a door counts with, a run seed the
/// broker takes) spells it through here.
#[macro_export]
macro_rules! worker_alive {
    ($lease:literal) => {
        concat!($lease, ".leased_until_unix >= EXTRACT(EPOCH FROM NOW())::BIGINT")
    };
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
            in_flight JSONB NOT NULL DEFAULT '{}'::jsonb
        )"#,
        r#"CREATE INDEX IF NOT EXISTS idx_worker_lease_project ON worker_lease(project_id, leased_until_unix)"#,
        // SYNC: door_count keys <-> crates/weft-core/src/signal/limits.rs (ran_key, failed_key)
        // What one worker counted at its door in one minute, per key
        // (`c:<token>:<caller>`, `e:<token>`, `refused:<token>:<limit>`,
        // `ran:<token>`, `failed:<token>`):
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
            PRIMARY KEY (key, window_start, replica)
        )"#,
        r#"CREATE INDEX IF NOT EXISTS idx_door_count_window ON door_count(window_start)"#,
        // One tick of a worker: its lease renewed with what it drives
        // now, its counts of the minute stored, and what the project's
        // other copies counted for the routes it serves (keys are
        // `<what>:<token>:...`), with how many copies are alive. One
        // statement, one round trip.
        // SYNC: the answer's shape <-> weft_broker_client::protocol::DoorTick
concat!(        r#"CREATE OR REPLACE FUNCTION weft_door_tick(
                p_replica TEXT, p_project UUID, p_tenant TEXT, p_binary_hash TEXT, p_leased_until BIGINT,
                p_in_flight JSONB, p_window BIGINT, p_keys TEXT[], p_hits BIGINT[], p_tokens TEXT[]
            ) RETURNS JSONB AS $$
            BEGIN
                INSERT INTO worker_lease (replica, project_id, tenant_id, binary_hash, leased_until_unix, in_flight)
                    VALUES (p_replica, p_project, p_tenant, p_binary_hash, p_leased_until, p_in_flight)
                    ON CONFLICT (replica) DO UPDATE
                        SET leased_until_unix = EXCLUDED.leased_until_unix, in_flight = EXCLUDED.in_flight
                    WHERE worker_lease.project_id = EXCLUDED.project_id;
                INSERT INTO door_count (key, window_start, replica, project_id, hits)
                    SELECT k, p_window, p_replica, p_project, h FROM unnest(p_keys, p_hits) AS t(k, h)
                    ON CONFLICT (key, window_start, replica) DO UPDATE SET hits = EXCLUDED.hits
                    WHERE door_count.project_id = EXCLUDED.project_id;
                RETURN jsonb_build_object(
                    'others', COALESCE((
                        SELECT jsonb_agg(jsonb_build_object('key', o.key, 'hits', o.hits))
                        FROM (SELECT key, SUM(hits)::bigint AS hits FROM door_count
                              WHERE window_start = p_window AND replica <> p_replica
                                AND project_id = p_project
                                AND split_part(key, ':', 2) = ANY(p_tokens)
                              GROUP BY key) o), '[]'::jsonb),
                    'copies', (SELECT COUNT(*) FROM worker_lease l
                               WHERE l.project_id = p_project
                                 AND "#, worker_alive!("l"), r#"));
            END;
            $$ LANGUAGE plpgsql"#),
    ],
    seed: &[],
};
