//! Rate limits and flood protection for the entries: how often one
//! caller may call, how often one entry may start runs (its callers and
//! the events it picks up itself together), how many runs one entry may
//! have going, and an address that keeps guessing tokens.
//!
//! Every count lives in Postgres, so every dispatcher replica sees the
//! same number and there is no service of its own to run. The per-minute
//! counts are buckets in an UNLOGGED table (a crash may lose the current
//! minute's counts, which only ever lets a few extra calls through);
//! the at-once count is one row per run an entry started, released when
//! the run ends.
//!
//! What an entry allows is `weft_core::signal::EntryLimits`, declared by
//! its node and resolved against the language defaults; what the whole
//! install allows (the trusted proxy hops, the token-guessing bound) is
//! [`EdgeConfig`], install config.

use std::net::IpAddr;

use anyhow::{Context, Result};
use sqlx::PgPool;

/// The length of one counting window, in seconds.
pub const WINDOW_SECS: i64 = 60;

/// How long a slot taken for a run that has not started yet holds before
/// it no longer counts: a fire's birth follows its admission at once, so
/// one still unborn this long later never will be.
pub const UNBORN_FIRE_SLOT_SECS: i64 = 600;

pub static GROUP: weft_task_store::SchemaGroup = weft_task_store::SchemaGroup {
    name: "entry_rate",
    tables: &["entry_rate", "entry_slot"],
    ddl: &[
        // One counter per (key, minute). UNLOGGED: a crash loses the
        // current minute's counts, which only lets a few extra calls
        // through, and every hit skips the write-ahead log.
        r#"CREATE UNLOGGED TABLE IF NOT EXISTS entry_rate (
            key TEXT NOT NULL,
            window_start BIGINT NOT NULL,
            hits INTEGER NOT NULL,
            PRIMARY KEY (key, window_start)
        )"#,
        // One row per run a public entry started, while it counts
        // toward the entry's at-once limit: taken at admission (keyed by
        // the execution the run will have), dropped when the run ends. A slot
        // whose run never started stops counting at `unborn_until`, and
        // one whose run ended stops counting at once (`slot_stopped_counting`).
        r#"CREATE TABLE IF NOT EXISTS entry_slot (
            execution_id TEXT PRIMARY KEY,
            signal_token TEXT NOT NULL,
            unborn_until BIGINT NOT NULL
        )"#,
        r#"CREATE INDEX IF NOT EXISTS idx_entry_slot_token ON entry_slot(signal_token)"#,
    ],
    seed: &[],
};

/// What the whole install allows at its public edge: the install config's.
pub use weft_platform_traits::config::EdgeConfig;

/// The caller's address: the `X-Forwarded-For` entries followed by the
/// peer the dispatcher saw, read `trusted_hops` from the right. Every
/// trusted proxy appends the address it received from, so the entries a
/// caller sent itself sit further left and are never read. A request
/// that passed fewer proxies than that (straight to the dispatcher's own
/// port) has a shorter list, and its leftmost entry is the caller.
pub fn caller_address(forwarded_for: Option<&str>, peer: IpAddr, trusted_hops: usize) -> IpAddr {
    let mut chain: Vec<IpAddr> = forwarded_for
        .unwrap_or("")
        .split(',')
        .filter_map(|entry| entry.trim().parse::<IpAddr>().ok())
        .collect();
    chain.push(peer);
    let index = chain.len().saturating_sub(1).saturating_sub(trusted_hops);
    chain[index]
}

/// A refused call: when the caller may try again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Refused {
    pub retry_after_secs: u64,
    pub reason: Limited,
}

/// Which limit refused a call. Named so the answer, the log and the
/// project's status all say the same thing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Limited {
    PerCaller,
    PerEntry,
    AtOnce,
    InvalidTokens,
}

impl Limited {
    pub fn describe(self) -> &'static str {
        match self {
            Limited::PerCaller => "this caller's calls per minute",
            Limited::PerEntry => "this entry's calls per minute",
            Limited::AtOnce => "this entry's runs at once",
            Limited::InvalidTokens => "refused tokens from this address",
        }
    }

    fn slug(self) -> &'static str {
        match self {
            Limited::PerCaller => "per_caller",
            Limited::PerEntry => "per_entry",
            Limited::AtOnce => "at_once",
            Limited::InvalidTokens => "invalid_tokens",
        }
    }
}

/// The minute `now` falls in, and the seconds left in it.
fn window(now: i64) -> (i64, u64) {
    let start = now - now.rem_euclid(WINDOW_SECS);
    (start, (start + WINDOW_SECS - now).max(1) as u64)
}

/// Count one hit on `key` in the current minute: its hits so far, this
/// one included, and the seconds left in the minute. The increment and
/// the read are one statement, so two replicas counting at once each see
/// the other's hit.
async fn count<'e>(db: impl sqlx::PgExecutor<'e>, key: &str, now: i64) -> Result<(i64, u64)> {
    let (start, left) = window(now);
    let (hits,): (i32,) = sqlx::query_as(
        "INSERT INTO entry_rate (key, window_start, hits) VALUES ($1, $2, 1) \
         ON CONFLICT (key, window_start) DO UPDATE SET hits = entry_rate.hits + 1 \
         RETURNING hits",
    )
    .bind(key)
    .bind(start)
    .fetch_one(db)
    .await
    .context("count a public call")?;
    Ok((hits as i64, left))
}

/// [`count`], and whether the hit is within `limit`.
async fn hit<'e>(db: impl sqlx::PgExecutor<'e>, key: &str, limit: u32, reason: Limited, now: i64) -> Result<Result<(), Refused>> {
    let (hits, left) = count(db, key, now).await?;
    Ok(if hits > limit as i64 { Err(Refused { retry_after_secs: left, reason }) } else { Ok(()) })
}

/// The per-minute limits of one call to the entry `token` by `caller`
/// (an identity or an address, already spelled as the caller's key).
/// Checked per caller first, so one caller at its limit never spends the
/// entry's shared allowance.
pub async fn admit_call(
    pool: &PgPool,
    token: &str,
    caller: &str,
    limits: &weft_core::signal::ResolvedLimits,
    now: i64,
) -> Result<Result<(), Refused>> {
    if let Some(limit) = limits.per_caller_per_minute {
        if let Err(refused) = hit(pool, &format!("c:{token}:{caller}"), limit, Limited::PerCaller, now).await? {
            return Ok(Err(refused));
        }
    }
    per_entry(pool, token, limits, now).await
}

/// The entry's own per-minute limit: one count per run it asks for,
/// shared by its callers and the events it picks up itself.
async fn per_entry<'e>(
    db: impl sqlx::PgExecutor<'e>,
    token: &str,
    limits: &weft_core::signal::ResolvedLimits,
    now: i64,
) -> Result<Result<(), Refused>> {
    match limits.per_minute {
        Some(limit) => hit(db, &format!("e:{token}"), limit, Limited::PerEntry, now).await,
        None => Ok(Ok(())),
    }
}

/// The per-minute limit of one fire nobody called: an event the entry
/// picked up itself (a schedule tick, a feed item, a provider's push).
/// There is no caller to count, so only the entry's own count applies;
/// a refusal is counted for `weft status` here, and the fire is the
/// caller's to drop (there is nobody to answer 429 to, and holding it
/// for later would only move the flood to the next minute). Its
/// at-once limit is taken when its run is born (`route_entry`).
///
/// `fire` is the fire's own identity when the same fire can come back
/// (a retried `fire_signal` task): its first decision is recorded in
/// `entry_rate` beside the counts (`f:` key, `hits` 1 admitted, 0
/// refused) and a retry reads it back instead of counting again. The
/// record lives as long as the counts it was decided against (the
/// sweep drops both after two minutes). `None` for a fire that never
/// comes back as itself (a provider's redelivery is a new push).
pub async fn admit_fire(
    pool: &PgPool,
    token: &str,
    fire: Option<&str>,
    limits: &weft_core::signal::ResolvedLimits,
    now: i64,
) -> Result<Result<(), Refused>> {
    let Some(fire) = fire else {
        let admitted = per_entry(pool, token, limits, now).await?;
        if let Err(refused) = admitted {
            note_refusal(pool, token, refused.reason, now).await?;
        }
        return Ok(admitted);
    };
    let key = format!("f:{token}:{fire}");
    let (start, left) = window(now);
    let mut tx = pool.begin().await.context("begin fire admission")?;
    // Serialized per fire, so two copies of one retried fire decide once.
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(&key)
        .execute(&mut *tx)
        .await
        .context("lock the fire's admission")?;
    let decided: Option<(i32,)> = sqlx::query_as("SELECT hits FROM entry_rate WHERE key = $1 LIMIT 1")
        .bind(&key)
        .fetch_optional(&mut *tx)
        .await
        .context("read the fire's admission")?;
    let admitted = match decided {
        Some((1,)) => Ok(()),
        Some(_) => Err(Refused { retry_after_secs: left, reason: Limited::PerEntry }),
        None => {
            let admitted = per_entry(&mut *tx, token, limits, now).await?;
            if let Err(refused) = admitted {
                count(&mut *tx, &format!("refused:{token}:{}", refused.reason.slug()), now).await?;
            }
            sqlx::query("INSERT INTO entry_rate (key, window_start, hits) VALUES ($1, $2, $3)")
                .bind(&key)
                .bind(start)
                .bind(i32::from(admitted.is_ok()))
                .execute(&mut *tx)
                .await
                .context("record the fire's admission")?;
            admitted
        }
    };
    tx.commit().await.context("commit fire admission")?;
    Ok(admitted)
}

/// Whether `address` is blocked for presenting too many refused tokens
/// this minute. Read-only: [`note_invalid_token`] counts.
pub async fn token_guessing_blocked(pool: &PgPool, edge: &EdgeConfig, address: IpAddr, now: i64) -> Result<Option<Refused>> {
    let Some(limit) = edge.invalid_tokens_per_minute else { return Ok(None) };
    let (start, left) = window(now);
    let hits: Option<(i32,)> = sqlx::query_as("SELECT hits FROM entry_rate WHERE key = $1 AND window_start = $2")
        .bind(format!("bad:{address}"))
        .bind(start)
        .fetch_optional(pool)
        .await
        .context("read refused tokens")?;
    Ok(hits
        .filter(|(h,)| *h as i64 >= limit as i64)
        .map(|_| Refused { retry_after_secs: left, reason: Limited::InvalidTokens }))
}

/// Count one refused token from `address`.
pub async fn note_invalid_token(pool: &PgPool, edge: &EdgeConfig, address: IpAddr, now: i64) -> Result<()> {
    if edge.invalid_tokens_per_minute.is_none() {
        return Ok(());
    }
    count(pool, &format!("bad:{address}"), now).await?;
    Ok(())
}

/// Count one refusal against the entry, for `weft status`.
pub async fn note_refusal(pool: &PgPool, token: &str, reason: Limited, now: i64) -> Result<()> {
    count(pool, &format!("refused:{token}:{}", reason.slug()), now).await?;
    Ok(())
}

/// Refusals of the entry `token` in this minute and the one before, by
/// the limit that refused them.
pub async fn recent_refusals(pool: &PgPool, token: &str, now: i64) -> Result<Vec<(Limited, u64)>> {
    let (start, _) = window(now);
    let mut out = Vec::new();
    for reason in [Limited::PerCaller, Limited::PerEntry, Limited::AtOnce] {
        let (hits,): (Option<i64>,) = sqlx::query_as(
            "SELECT SUM(hits)::BIGINT FROM entry_rate WHERE key = $1 AND window_start >= $2",
        )
        .bind(format!("refused:{token}:{}", reason.slug()))
        .bind(start - WINDOW_SECS)
        .fetch_one(pool)
        .await
        .context("read refusals")?;
        if let Some(hits) = hits.filter(|h| *h > 0) {
            out.push((reason, hits as u64));
        }
    }
    Ok(out)
}

/// The condition, on an `entry_slot s`, under which a slot no longer
/// counts toward its entry's at-once limit, with `now` bound at `now_param`:
/// its run never started (or was an unrecorded run, forgotten with its
/// row) and its hold passed, or its run ended, whether or not the bridge
/// freed the slot yet: a terminal event is in the journal, or an
/// unrecorded run whose costs kept its row is stamped ended.
fn slot_stopped_counting(now_param: &str) -> String {
    format!(
        "((s.unborn_until < {now_param} \
           AND NOT EXISTS (SELECT 1 FROM execution ec WHERE ec.execution_id = s.execution_id)) \
          OR EXISTS (SELECT 1 FROM execution ec WHERE ec.execution_id = s.execution_id \
                     AND ec.ended_at_unix IS NOT NULL) \
          OR EXISTS (SELECT 1 FROM exec_event e WHERE e.execution_id = s.execution_id \
                     AND e.kind IN {terminal}))",
        terminal = weft_journal::EXECUTION_TERMINAL_KINDS_SQL,
    )
}

/// Take one of `token`'s at-once slots for the run `execution_id` will be, or
/// say the entry is full. A slot already held by `execution_id` (a retried
/// admission of the same run) is kept, never counted twice.
///
/// Serialized per entry by a transaction-scoped advisory lock, so two
/// replicas admitting the last free slot at once cannot both take it.
/// Slots that stopped counting are dropped first: a handshake nobody
/// followed, or a run that ended.
pub async fn take_slot(
    pool: &PgPool,
    token: &str,
    execution_id: &str,
    max: u32,
    unborn_until: i64,
    now: i64,
) -> Result<Result<(), Refused>> {
    let mut tx = pool.begin().await.context("begin slot take")?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended('entry_slot:' || $1, 0))")
        .bind(token)
        .execute(&mut *tx)
        .await
        .context("lock the entry's slots")?;
    sqlx::query(&format!("DELETE FROM entry_slot s WHERE s.signal_token = $1 AND {}", slot_stopped_counting("$2")))
    .bind(token)
    .bind(now)
    .execute(&mut *tx)
    .await
    .context("drop the entry's abandoned slots")?;
    let held: Option<(String,)> = sqlx::query_as("SELECT execution_id FROM entry_slot WHERE execution_id = $1")
        .bind(execution_id)
        .fetch_optional(&mut *tx)
        .await?;
    if held.is_none() {
        let (taken,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM entry_slot WHERE signal_token = $1")
            .bind(token)
            .fetch_one(&mut *tx)
            .await?;
        if taken >= max as i64 {
            tx.rollback().await?;
            return Ok(Err(Refused { retry_after_secs: 5, reason: Limited::AtOnce }));
        }
        sqlx::query("INSERT INTO entry_slot (execution_id, signal_token, unborn_until) VALUES ($1, $2, $3)")
        .bind(execution_id)
        .bind(token)
        .bind(unborn_until)
        .execute(&mut *tx)
        .await
        .context("take the slot")?;
    }
    tx.commit().await.context("commit slot take")?;
    Ok(Ok(()))
}

/// Whether the entry is already full, without taking anything: the
/// quick answer at the edge for a fire, whose slot is taken when its run
/// is born.
pub async fn at_once_full(pool: &PgPool, token: &str, max: u32, now: i64) -> Result<Option<Refused>> {
    let (taken,): (i64,) = sqlx::query_as(&format!(
        "SELECT COUNT(*) FROM entry_slot s WHERE s.signal_token = $1 AND NOT {}",
        slot_stopped_counting("$2")
    ))
    .bind(token)
    .bind(now)
    .fetch_one(pool)
    .await
    .context("count the entry's runs")?;
    Ok((taken >= max as i64).then_some(Refused { retry_after_secs: 5, reason: Limited::AtOnce }))
}

/// The run `execution_id` ended: its slot, if it held one, is free.
pub async fn release_slot(pool: &PgPool, execution_id: &str) -> Result<()> {
    sqlx::query("DELETE FROM entry_slot WHERE execution_id = $1")
        .bind(execution_id)
        .execute(pool)
        .await
        .context("release an entry slot")?;
    Ok(())
}

/// Drop what no longer counts: minutes before the last two, slots of runs
/// that never started, and slots of runs that ended (the bridge frees
/// those as each run ends; this catches one whose cleanup stopped before
/// it got there). The reaper runs this.
pub async fn sweep(pool: &PgPool, now: i64) -> Result<()> {
    let (start, _) = window(now);
    sqlx::query("DELETE FROM entry_rate WHERE window_start < $1")
        .bind(start - WINDOW_SECS)
        .execute(pool)
        .await
        .context("drop old rate windows")?;
    sqlx::query(&format!("DELETE FROM entry_slot s WHERE {}", slot_stopped_counting("$1")))
    .bind(now)
    .execute(pool)
    .await
    .context("drop abandoned entry slots")?;
    Ok(())
}

/// The `429` a refused call gets: which limit, and when to come back.
pub fn too_many(refused: Refused) -> axum::response::Response {
    use axum::response::IntoResponse;
    (
        axum::http::StatusCode::TOO_MANY_REQUESTS,
        [(axum::http::header::RETRY_AFTER, refused.retry_after_secs.to_string())],
        format!("too many calls: {} is reached; try again in {}s", refused.reason.describe(), refused.retry_after_secs),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    #[test]
    fn the_caller_is_read_trusted_hops_from_the_right() {
        // Through the tunnel and the front door on kind: the caller, the
        // tunnel's process, then the peer the dispatcher saw (the proxy).
        assert_eq!(caller_address(Some("203.0.113.9, 10.244.0.7"), ip("10.244.0.9"), 2), ip("203.0.113.9"));
        // A forged entry sent by the caller sits further left and is
        // never read.
        assert_eq!(caller_address(Some("1.1.1.1, 203.0.113.9, 10.244.0.7"), ip("10.244.0.9"), 2), ip("203.0.113.9"));
        // Straight to the dispatcher's own port: the peer is the caller.
        assert_eq!(caller_address(None, ip("127.0.0.1"), 2), ip("127.0.0.1"));
        // Garbage entries are skipped, never read as an address.
        assert_eq!(caller_address(Some("not-an-ip, 203.0.113.9"), ip("10.0.0.1"), 1), ip("203.0.113.9"));
        assert_eq!(caller_address(Some("203.0.113.9"), ip("10.0.0.1"), 0), ip("10.0.0.1"));
    }

    #[test]
    fn the_window_is_the_minute_and_what_is_left_of_it() {
        assert_eq!(window(120), (120, 60));
        assert_eq!(window(179), (120, 1));
        assert_eq!(window(150), (120, 30));
    }
}
