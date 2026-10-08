//! The install's own flood guard: an address that keeps guessing tokens at
//! the dispatcher's own token doors (a form's, an instance's) is refused
//! for the rest of the minute. A trigger's limits (per caller, per minute,
//! at once) are counted at its worker's door for every way work arrives
//! (`weft_engine::door::limits`); this reads their refusals for
//! `weft status`.
//!
//! The counts live in an UNLOGGED table in Postgres, so every dispatcher
//! replica sees the same number (a crash may lose the current minute's
//! counts, which only ever lets a few extra calls through). What the whole
//! install allows (the trusted proxy hops, the token-guessing bound) is
//! [`EdgeConfig`], install config.

use std::net::IpAddr;

use anyhow::{Context, Result};
use sqlx::PgPool;

pub use weft_core::signal::limits::{Limited, Refused, WINDOW_SECS};
use weft_core::signal::limits::window;

pub static GROUP: weft_task_store::SchemaGroup = weft_task_store::SchemaGroup {
    name: "entry_rate",
    tables: &["entry_rate"],
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
        // Count one hit on a key in a minute, and answer its hits so far,
        // this one included. The increment and the read are one statement,
        // so two replicas counting at once each see the other's hit.
        r#"CREATE OR REPLACE FUNCTION weft_rate_hit(p_key TEXT, p_window_start BIGINT) RETURNS INTEGER AS $$
            INSERT INTO entry_rate (key, window_start, hits) VALUES (p_key, p_window_start, 1)
            ON CONFLICT (key, window_start) DO UPDATE SET hits = entry_rate.hits + 1
            RETURNING hits
            $$ LANGUAGE sql"#,
    ],
    seed: &[],
};

/// What the whole install allows at its public edge: the install config's.
pub use weft_platform_traits::config::EdgeConfig;


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

/// Count one refused token from `address`, when the install bounds them.
pub async fn note_invalid_token(pool: &PgPool, edge: &EdgeConfig, address: IpAddr, now: i64) -> Result<()> {
    if edge.invalid_tokens_per_minute.is_some() {
        count(pool, &invalid_tokens_key(address), now).await?;
    }
    Ok(())
}

/// Refusals of the entry `token` in this minute and the one before, by
/// the limit that refused them, as the project's workers' doors counted
/// them (`door_count`, each copy's total, summed). Read for `project`
/// only: a worker writes the keys it is handed, and another project's
/// worker naming this entry's token counts for nothing.
pub async fn recent_refusals(pool: &PgPool, project: uuid::Uuid, token: &str, now: i64) -> Result<Vec<(Limited, u64)>> {
    let (start, _) = window(now);
    let mut out = Vec::new();
    for reason in Limited::ALL {
        let (hits,): (Option<i64>,) =
            sqlx::query_as("SELECT SUM(hits)::BIGINT FROM door_count WHERE key = $1 AND window_start >= $2 AND project_id = $3")
                .bind(reason.refusal_key(token))
                .bind(start - WINDOW_SECS)
                .bind(project)
                .fetch_one(pool)
                .await
                .context("read refusals")?;
        if let Some(hits) = hits.filter(|h| *h > 0) {
            out.push((reason, hits as u64));
        }
    }
    Ok(out)
}

/// How many runs each of the project's triggers started, and how many of
/// those failed, in this minute and the one before, by trigger token, as
/// the doors that started them counted (`door_count`). An unrecorded run
/// leaves no row of its own when it goes well, so this is where it is
/// seen.
pub async fn recent_runs(pool: &PgPool, project: uuid::Uuid, now: i64) -> Result<std::collections::HashMap<String, (u64, u64)>> {
    let (start, _) = window(now);
    let rows: Vec<(String, i64)> = sqlx::query_as(
        "SELECT key, SUM(hits)::BIGINT FROM door_count \
         WHERE project_id = $1 AND window_start >= $2 AND (key LIKE 'ran:%' OR key LIKE 'failed:%') \
         GROUP BY key",
    )
    .bind(project)
    .bind(start - WINDOW_SECS)
    .fetch_all(pool)
    .await
    .context("read run counts")?;
    let mut runs: std::collections::HashMap<String, (u64, u64)> = std::collections::HashMap::new();
    for (key, hits) in rows {
        let hits = hits.max(0) as u64;
        match key.split_once(':') {
            Some(("ran", token)) => runs.entry(token.to_string()).or_default().0 += hits,
            Some(("failed", token)) => runs.entry(token.to_string()).or_default().1 += hits,
            _ => {}
        }
    }
    Ok(runs)
}

/// Drop the minutes that no longer count: every one before the last two.
/// The reaper runs this.
pub async fn sweep(pool: &PgPool, now: i64) -> Result<()> {
    let (start, _) = window(now);
    sqlx::query("DELETE FROM entry_rate WHERE window_start < $1")
        .bind(start - WINDOW_SECS)
        .execute(pool)
        .await
        .context("drop old rate windows")?;
    Ok(())
}

/// The `429` a refused call gets: which limit, and when to come back.
pub fn too_many(refused: Refused) -> axum::response::Response {
    use axum::response::IntoResponse;
    (
        axum::http::StatusCode::TOO_MANY_REQUESTS,
        [(axum::http::header::RETRY_AFTER, refused.retry_after_secs.to_string())],
        refused.answer(),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_window_is_the_minute_and_what_is_left_of_it() {
        assert_eq!(window(120), (120, 60));
        assert_eq!(window(179), (120, 1));
        assert_eq!(window(150), (120, 30));
        assert_eq!(Limited::from_slug(Limited::AtOnce.slug()), Some(Limited::AtOnce));
    }
}
