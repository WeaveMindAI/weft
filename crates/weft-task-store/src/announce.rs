//! Announcing the writes a run makes, without making every commit wait on
//! every other.
//!
//! A transaction that calls `pg_notify` takes one lock shared by the whole
//! database while it commits, and holds it until its commit is on disk,
//! because Postgres hands notifications out in commit order. Every write
//! that announces itself therefore commits one after another, whatever
//! rows they touch. A run writes several (its birth, its journal rows, its
//! task's end), so under load the database spent most of its time with
//! writes queued on that one lock.
//!
//! So the writes a run makes do not notify. Their triggers put what they
//! would have announced in [`TABLE`] (`weft_announce(channel, payload)`),
//! a row like any other that commits with the write or not at all, and
//! takes no shared lock. After the write commits, its writer pokes this
//! process's flusher ([`committed`]), which takes every row waiting there
//! and sends them all in ONE transaction: the shared lock is taken once
//! per batch, not once per write. Under load the flusher runs back to back
//! and each batch carries every write that committed meanwhile; with
//! nothing to do it sends nothing.
//!
//! Listeners are unchanged: they hear the same channels with the same
//! payloads, and read their rows when woken. A writer that forgets to poke
//! only delays its announcement until the next poke of any writer on the
//! database, since a flush takes every row waiting; a process that dies
//! between its commit and its flush leaves its rows for the next one the
//! same way, and a process's flusher sends what it finds waiting as it
//! starts. While a release rolls out, a process still on the one before
//! writes through the same triggers and never pokes: what it announces
//! goes out with the next write of a process on the new one, or, on a
//! quiet database, at the waiter's own next look.
//!
//! The rare writes (a connection changed, an infra copy's status, a domain)
//! still notify from their own trigger: they commit seldom, so the lock
//! costs them nothing, and they keep needing no poke.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use sqlx::postgres::PgPool;

/// Where the writes a run makes leave what they announce.
pub const TABLE: &str = "weft_announcement";

/// How many announcements one flush sends at most; a fuller outbox is
/// flushed again at once.
const BATCH: i32 = 10_000;

pub static GROUP: crate::SchemaGroup = crate::SchemaGroup {
    name: "announcement",
    tables: &["weft_announcement"],
    ddl: &[
        // UNLOGGED: an announcement only says when to look at rows that
        // are durable on their own, so a database crash losing the ones
        // not sent yet costs a waiter its next safety look, never data.
        r#"CREATE UNLOGGED TABLE IF NOT EXISTS weft_announcement (
            channel TEXT NOT NULL,
            payload TEXT NOT NULL
        )"#,
        // What a trigger on a run's path calls instead of `pg_notify`.
        r#"CREATE OR REPLACE FUNCTION weft_announce(p_channel TEXT, p_payload TEXT) RETURNS VOID AS $$
            INSERT INTO weft_announcement (channel, payload) VALUES (p_channel, p_payload)
            $$ LANGUAGE sql"#,
        // The flush ([`flush`]): take up to `p_limit` waiting rows (those
        // another flusher is sending right now are left to it), send each
        // distinct one, all in the caller's one transaction, and answer
        // how many rows were taken.
        r#"CREATE OR REPLACE FUNCTION weft_flush_announcements(p_limit INTEGER) RETURNS INTEGER AS $$
            DECLARE
                v_channels TEXT[];
                v_payloads TEXT[];
            BEGIN
                WITH taken AS (
                    DELETE FROM weft_announcement a
                    WHERE a.ctid IN (SELECT ctid FROM weft_announcement FOR UPDATE SKIP LOCKED LIMIT p_limit)
                    RETURNING channel, payload)
                SELECT array_agg(channel), array_agg(payload) INTO v_channels, v_payloads FROM taken;
                IF v_channels IS NULL THEN
                    RETURN 0;
                END IF;
                PERFORM pg_notify(d.channel, d.payload)
                    FROM (SELECT DISTINCT u.channel, u.payload FROM unnest(v_channels, v_payloads) AS u(channel, payload)) d;
                RETURN array_length(v_channels, 1);
            END;
            $$ LANGUAGE plpgsql"#,
    ],
    seed: &[],
};

/// Tell this process's flusher for `pool`'s database that a write which
/// may have left announcements just committed.
pub fn committed(pool: &PgPool) {
    flusher(pool).wanted.notify_one();
}

/// One database's flusher in this process, and the pool it sends through:
/// the last one a write poked with that is still open, so a pool closing
/// (a test's, at its end) never leaves the database without one.
struct Flusher {
    wanted: tokio::sync::Notify,
    pool: Mutex<PgPool>,
}

/// The flushers of this process, by database ([`database_key`]). An entry
/// leaves when its flusher stops, so the next poke starts another.
type Flushers = Mutex<HashMap<String, Arc<Flusher>>>;

fn flushers() -> &'static Flushers {
    static FLUSHERS: OnceLock<Flushers> = OnceLock::new();
    FLUSHERS.get_or_init(Mutex::default)
}

/// Where `pool` connects: one flusher per database, whatever the number of
/// pools on it.
fn database_key(pool: &PgPool) -> String {
    let options = pool.connect_options();
    format!("{}:{}/{}", options.get_host(), options.get_port(), options.get_database().unwrap_or(""))
}

/// The flusher of `pool`'s database, started when there is none.
fn flusher(pool: &PgPool) -> Arc<Flusher> {
    let key = database_key(pool);
    let mut flushers = flushers().lock().expect("announcement flushers");
    if let Some(flusher) = flushers.get(&key) {
        let mut sends_through = flusher.pool.lock().expect("announcement flusher pool");
        if sends_through.is_closed() {
            *sends_through = pool.clone();
        }
        return flusher.clone();
    }
    let flusher = Arc::new(Flusher { wanted: tokio::sync::Notify::new(), pool: Mutex::new(pool.clone()) });
    tokio::spawn(flush_when_wanted(key.clone(), flusher.clone()));
    flushers.insert(key, flusher.clone());
    flusher
}

/// Takes a flusher out of [`flushers`] if its task ends (the runtime it
/// ran on went away), so a later poke on the same database starts a new
/// one instead of waking nobody.
struct Stopped {
    key: String,
    flusher: Arc<Flusher>,
}

impl Drop for Stopped {
    fn drop(&mut self) {
        let mut flushers = flushers().lock().expect("announcement flushers");
        if flushers.get(&self.key).is_some_and(|f| Arc::ptr_eq(f, &self.flusher)) {
            flushers.remove(&self.key);
        }
    }
}

/// Flush at once (what a process that died between its commit and its
/// flush left waiting goes out with the first write after it), then each
/// time a write asks.
async fn flush_when_wanted(key: String, flusher: Arc<Flusher>) {
    let _stopped = Stopped { key, flusher: flusher.clone() };
    loop {
        let pool = flusher.pool.lock().expect("announcement flusher pool").clone();
        flush_waiting(&pool).await;
        flusher.wanted.notified().await;
    }
}

/// How long a flush that failed waits before it is tried again, at first
/// and at most.
const RETRY_FIRST: Duration = Duration::from_millis(500);
const RETRY_LONGEST: Duration = Duration::from_secs(30);

/// Send everything waiting, in batches of [`BATCH`]. A flush that fails is
/// tried again until it goes through: the rows stay in the outbox
/// meanwhile, and the writes that left them wait on what they announce.
async fn flush_waiting(pool: &PgPool) {
    let mut retry = RETRY_FIRST;
    loop {
        match flush(pool).await {
            Ok(sent) if sent >= BATCH as usize => retry = RETRY_FIRST,
            Ok(_) => return,
            Err(_) if pool.is_closed() => return,
            Err(e) => {
                tracing::warn!(
                    target: "weft_task_store::announce",
                    error = %e, retry_in_ms = retry.as_millis() as u64,
                    "could not send the waiting announcements; trying again"
                );
                tokio::time::sleep(retry).await;
                retry = (retry * 2).min(RETRY_LONGEST);
            }
        }
    }
}

/// Send what waits in the outbox, in one transaction
/// (`weft_flush_announcements`): how many rows it took.
async fn flush(pool: &PgPool) -> Result<usize, sqlx::Error> {
    let taken: i32 = sqlx::query_scalar("SELECT weft_flush_announcements($1)").bind(BATCH).fetch_one(pool).await?;
    Ok(taken.max(0) as usize)
}
