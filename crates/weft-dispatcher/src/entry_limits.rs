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
        // one whose run ended stops counting at once (`weft_slot_stopped_counting`).
        r#"CREATE TABLE IF NOT EXISTS entry_slot (
            execution_id TEXT PRIMARY KEY,
            signal_token TEXT NOT NULL,
            unborn_until BIGINT NOT NULL
        )"#,
        r#"CREATE INDEX IF NOT EXISTS idx_entry_slot_token ON entry_slot(signal_token)"#,
        // Count one hit on a key in a minute, and answer its hits so far,
        // this one included. The increment and the read are one statement,
        // so two replicas counting at once each see the other's hit.
        r#"CREATE OR REPLACE FUNCTION weft_rate_hit(p_key TEXT, p_window_start BIGINT) RETURNS INTEGER AS $$
            INSERT INTO entry_rate (key, window_start, hits) VALUES (p_key, p_window_start, 1)
            ON CONFLICT (key, window_start) DO UPDATE SET hits = entry_rate.hits + 1
            RETURNING hits
            $$ LANGUAGE sql"#,
        // Whether a slot no longer counts toward its entry's at-once limit
        // at `p_now`: its run never started (or was an unrecorded run,
        // forgotten with its row) and its hold passed, or its run ended,
        // whether or not the bridge freed the slot yet: a terminal event is
        // in the journal, or an unrecorded run whose costs kept its row is
        // stamped ended.
        concat!(
            r#"CREATE OR REPLACE FUNCTION weft_slot_stopped_counting(p_execution_id TEXT, p_unborn_until BIGINT, p_now BIGINT)
            RETURNS BOOLEAN AS $$
            SELECT (p_unborn_until < p_now
                    AND NOT EXISTS (SELECT 1 FROM execution ec WHERE ec.execution_id = p_execution_id))
                OR EXISTS (SELECT 1 FROM execution ec WHERE ec.execution_id = p_execution_id
                           AND ec.ended_at_unix IS NOT NULL)
                OR EXISTS (SELECT 1 FROM exec_event e WHERE e.execution_id = p_execution_id
                           AND e.kind IN "#,
            weft_journal::execution_terminal_kinds_sql!(),
            r#")
            $$ LANGUAGE sql STABLE"#
        ),
        // One call's (or one run's) admission at the entry's limits, in the
        // caller's transaction: `NULL` when admitted, else which limit
        // refused it and when to try again. In order: an address blocked
        // for guessing tokens (read only), then each per-minute count (the
        // caller's before the entry's, so a caller at its limit never spends
        // the entry's shared allowance), then the at-once limit. A slot is
        // taken for `slot.execution_id` when one is named (kept, never
        // counted twice, when that execution already holds it), else the
        // entry is only checked for room. Taking is serialized per entry by
        // a transaction-scoped lock, and the count runs after it, so it sees
        // every slot a sibling took before letting go; two replicas
        // admitting the last free slot at once cannot both take it. Every
        // refusal but a blocked address is counted for `weft status`.
        // SYNC: p's fields <-> Admission (below)
        r#"CREATE OR REPLACE FUNCTION weft_admit(p JSONB) RETURNS JSONB AS $$
            DECLARE
                v_window BIGINT := (p->>'window_start')::bigint;
                v_later INTEGER := (p->>'retry_after_secs')::integer;
                v_now BIGINT := (p->>'now')::bigint;
                v_slot JSONB := p->'slot';
                v_count JSONB;
                v_hits INTEGER;
                v_refused JSONB;
                v_room BOOLEAN;
            BEGIN
                IF jsonb_typeof(p->'blocked') = 'object' THEN
                    SELECT r.hits INTO v_hits FROM entry_rate r
                        WHERE r.key = p->'blocked'->>'key' AND r.window_start = v_window;
                    IF v_hits >= (p->'blocked'->>'limit')::integer THEN
                        RETURN jsonb_build_object('reason', 'invalid_tokens', 'retry_after_secs', v_later);
                    END IF;
                END IF;
                FOR v_count IN SELECT c FROM jsonb_array_elements(p->'counts') WITH ORDINALITY AS e(c, n) ORDER BY n LOOP
                    IF weft_rate_hit(v_count->>'key', v_window) > (v_count->>'limit')::integer THEN
                        v_refused := jsonb_build_object('reason', v_count->>'reason', 'retry_after_secs', v_later);
                        EXIT;
                    END IF;
                END LOOP;
                IF v_refused IS NULL AND jsonb_typeof(v_slot) = 'object' THEN
                    IF v_slot->>'execution_id' IS NULL THEN
                        SELECT COUNT(*) < (v_slot->>'max')::bigint INTO v_room FROM entry_slot s
                            WHERE s.signal_token = v_slot->>'token'
                              AND NOT weft_slot_stopped_counting(s.execution_id, s.unborn_until, v_now);
                    ELSE
                        PERFORM pg_advisory_xact_lock(hashtextextended('entry_slot:' || (v_slot->>'token'), 0));
                        SELECT EXISTS (SELECT 1 FROM entry_slot s WHERE s.execution_id = v_slot->>'execution_id'
                                       AND NOT weft_slot_stopped_counting(s.execution_id, s.unborn_until, v_now))
                            OR (SELECT COUNT(*) FROM entry_slot s
                                WHERE s.signal_token = v_slot->>'token' AND s.execution_id <> v_slot->>'execution_id'
                                  AND NOT weft_slot_stopped_counting(s.execution_id, s.unborn_until, v_now))
                               < (v_slot->>'max')::bigint
                            INTO v_room;
                        IF v_room THEN
                            INSERT INTO entry_slot (execution_id, signal_token, unborn_until)
                                VALUES (v_slot->>'execution_id', v_slot->>'token', (v_slot->>'unborn_until')::bigint)
                                ON CONFLICT (execution_id) DO UPDATE
                                    SET unborn_until = GREATEST(entry_slot.unborn_until, EXCLUDED.unborn_until);
                        END IF;
                    END IF;
                    IF NOT v_room THEN
                        v_refused := jsonb_build_object('reason', 'at_once', 'retry_after_secs', 5);
                    END IF;
                END IF;
                IF v_refused IS NOT NULL THEN
                    PERFORM weft_rate_hit((p->>'refusals_key') || (v_refused->>'reason'), v_window);
                END IF;
                RETURN v_refused;
            END;
            $$ LANGUAGE plpgsql"#,
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

    // SYNC: the slugs <-> weft_admit's reasons ('at_once', 'invalid_tokens')
    fn slug(self) -> &'static str {
        match self {
            Limited::PerCaller => "per_caller",
            Limited::PerEntry => "per_entry",
            Limited::AtOnce => "at_once",
            Limited::InvalidTokens => "invalid_tokens",
        }
    }

    fn from_slug(slug: &str) -> Option<Self> {
        [Limited::PerCaller, Limited::PerEntry, Limited::AtOnce, Limited::InvalidTokens].into_iter().find(|l| l.slug() == slug)
    }
}

/// The minute `now` falls in, and the seconds left in it.
fn window(now: i64) -> (i64, u64) {
    let start = now - now.rem_euclid(WINDOW_SECS);
    (start, (start + WINDOW_SECS - now).max(1) as u64)
}

/// Count one hit on `key` in the current minute (`weft_rate_hit`): its
/// hits so far, this one included, and the seconds left in the minute.
async fn count<'e>(db: impl sqlx::PgExecutor<'e>, key: &str, now: i64) -> Result<(i64, u64)> {
    let (start, left) = window(now);
    let hits: i32 = sqlx::query_scalar("SELECT weft_rate_hit($1, $2)")
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

/// One admission at an entry's limits, as `weft_admit` takes it: what
/// is checked, counted and taken, in order. Built by [`Admission::call`]
/// for a caller's call and [`Admission::fire`] for a run an event
/// started, and handed to the database whole: on its own ([`admit`]), or
/// with the run's birth in the same call (`Journal::start_execution`).
// SYNC: Admission's fields <-> weft_admit (GROUP above)
#[derive(Debug, Clone, serde::Serialize)]
pub struct Admission {
    now: i64,
    window_start: i64,
    retry_after_secs: u64,
    /// The address checked for guessing tokens, when the install bounds it.
    blocked: Option<Bound>,
    /// The per-minute counts, in the order they are checked.
    counts: Vec<Count>,
    /// The at-once limit, when the entry has one.
    slot: Option<Slot>,
    /// Where a refusal is counted: the key, less the limit's slug.
    refusals_key: String,
}

#[derive(Debug, Clone, serde::Serialize)]
struct Bound {
    key: String,
    limit: u32,
}

#[derive(Debug, Clone, serde::Serialize)]
struct Count {
    key: String,
    limit: u32,
    reason: &'static str,
}

#[derive(Debug, Clone, serde::Serialize)]
struct Slot {
    token: String,
    max: u32,
    /// The run the slot is taken for; `None` only checks there is room.
    execution_id: Option<String>,
    unborn_until: i64,
}

/// What a call's slot is for: the run born with it (and how long its slot
/// holds if the run never starts), or nothing, when only room is checked.
pub type TakeFor<'a> = Option<(&'a str, i64)>;

impl Admission {
    fn at(token: &str, now: i64) -> Self {
        let (window_start, retry_after_secs) = window(now);
        Self {
            now,
            window_start,
            retry_after_secs,
            blocked: None,
            counts: Vec::new(),
            slot: None,
            refusals_key: format!("refused:{token}:"),
        }
    }

    fn with_slot(mut self, token: &str, limits: &weft_core::signal::ResolvedLimits, take_for: TakeFor<'_>) -> Self {
        self.slot = limits.at_once.map(|max| Slot {
            token: token.to_string(),
            max,
            execution_id: take_for.map(|(execution_id, _)| execution_id.to_string()),
            unborn_until: take_for.map(|(_, until)| until).unwrap_or(self.now),
        });
        self
    }

    /// A call to the entry `token` by `caller` (an identity or an address,
    /// already spelled as the caller's key): the caller's count and the
    /// entry's, then its room, taking a slot for `take_for`'s run when one
    /// is named. `address` is checked for guessing tokens first, for a
    /// door whose token guard leaves that check to the call's admission
    /// (`api::guard_token_doors`).
    pub fn call(
        edge: &EdgeConfig,
        address: Option<IpAddr>,
        token: &str,
        caller: &str,
        limits: &weft_core::signal::ResolvedLimits,
        take_for: TakeFor<'_>,
        now: i64,
    ) -> Self {
        let mut admission = Self::at(token, now);
        admission.blocked = address
            .zip(edge.invalid_tokens_per_minute)
            .map(|(address, limit)| Bound { key: invalid_tokens_key(address), limit });
        if let Some(limit) = limits.per_caller_per_minute {
            admission.counts.push(Count { key: format!("c:{token}:{caller}"), limit, reason: Limited::PerCaller.slug() });
        }
        if let Some(limit) = limits.per_minute {
            admission.counts.push(Count { key: format!("e:{token}"), limit, reason: Limited::PerEntry.slug() });
        }
        admission.with_slot(token, limits, take_for)
    }

    /// The at-once limit of the run `execution_id` an event started at the
    /// entry `token` (its per-minute count was taken when the event came
    /// in, [`admit_fire`]), whose slot holds [`UNBORN_FIRE_SLOT_SECS`] if
    /// the run is never born.
    pub fn fire(token: &str, limits: &weft_core::signal::ResolvedLimits, execution_id: &str, now: i64) -> Self {
        Self::at(token, now).with_slot(token, limits, Some((execution_id, now + UNBORN_FIRE_SLOT_SECS)))
    }

    /// Whether there is nothing to check, count or take.
    pub fn is_empty(&self) -> bool {
        self.blocked.is_none() && self.counts.is_empty() && self.slot.is_none()
    }
}

/// `weft_admit`'s answer: `None` admitted, else the refusal.
#[derive(Debug, serde::Deserialize)]
pub struct Answer {
    reason: String,
    retry_after_secs: u64,
}

impl Answer {
    pub fn refused(&self) -> Result<Refused> {
        let reason = Limited::from_slug(&self.reason)
            .ok_or_else(|| anyhow::anyhow!("the database refused a call for a limit weft does not know: '{}'", self.reason))?;
        Ok(Refused { retry_after_secs: self.retry_after_secs, reason })
    }
}

/// Admit `admission` on its own: one round trip, nothing born with it.
pub async fn admit(pool: &PgPool, admission: &Admission) -> Result<Result<(), Refused>> {
    if admission.is_empty() {
        return Ok(Ok(()));
    }
    let answer: Option<serde_json::Value> = sqlx::query_scalar("SELECT weft_admit($1)")
        .bind(serde_json::to_value(admission)?)
        .fetch_one(pool)
        .await
        .context("admit a public call")?;
    match answer {
        None => Ok(Ok(())),
        Some(answer) => Ok(Err(serde_json::from_value::<Answer>(answer).context("read the admission's answer")?.refused()?)),
    }
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

/// The count of refused tokens from `address`.
fn invalid_tokens_key(address: IpAddr) -> String {
    format!("bad:{address}")
}

/// Whether `address` is blocked for presenting too many refused tokens
/// this minute. Read-only: [`note_invalid_token`] counts.
pub async fn token_guessing_blocked(pool: &PgPool, edge: &EdgeConfig, address: IpAddr, now: i64) -> Result<Option<Refused>> {
    let Some(limit) = edge.invalid_tokens_per_minute else { return Ok(None) };
    let (start, left) = window(now);
    let hits: Option<(i32,)> = sqlx::query_as("SELECT hits FROM entry_rate WHERE key = $1 AND window_start = $2")
        .bind(invalid_tokens_key(address))
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
    count(pool, &invalid_tokens_key(address), now).await?;
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
    sqlx::query("DELETE FROM entry_slot s WHERE weft_slot_stopped_counting(s.execution_id, s.unborn_until, $1)")
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
