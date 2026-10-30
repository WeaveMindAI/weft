//! The `alarm` table: wakes a local install has set and not yet delivered.
//!
//! A cloud keeps wakes in its own queue (Cloud Tasks); a local install has
//! nothing like it, so its alarm keeps them here, where a restart finds
//! them again. A wake is claimed before it is delivered (its time pushed
//! past a short lease) and deleted once the receiver took it, so a
//! delivery cut short by a crash comes due again by itself. At least once,
//! as every alarm is: the receiver recomputes from its own state whether
//! anything is due.

use anyhow::Result;
use crate::db::{Client as _, FromRow, JsonText, Pool, Row};

pub static GROUP: crate::SchemaGroup = crate::SchemaGroup {
    name: "alarm",
    tables: &["alarm"],
    ddl: &[
        r#"CREATE TABLE IF NOT EXISTS alarm (
            -- The wake's name (`Wake::name`): the same key, time and
            -- generation is the same wake, set once however often it is
            -- asked for, and deleted by name when it is cancelled.
            name TEXT PRIMARY KEY,
            key TEXT NOT NULL,
            -- When it is due (unix milliseconds). A claimed wake has this
            -- pushed past its lease, so a delivery that never finishes
            -- comes due again.
            at_unix_ms BIGINT NOT NULL,
            -- The moment the wake was set for, what the receiver is told.
            set_for_unix_ms BIGINT NOT NULL,
            role TEXT NOT NULL,
            path TEXT NOT NULL,
            -- As its JSON text (`db::JsonText`): it carries the signal's
            -- state, which quotes what an outside service answered.
            body TEXT NOT NULL,
            -- Deliveries claimed, whatever became of them: how long the
            -- next try waits.
            attempts INTEGER NOT NULL DEFAULT 0,
            -- Deliveries the receiver answered and failed
            -- (`weft_platform_traits::alarm::answered_by_receiver`): what
            -- the receiver is told, so one it never got spends none of
            -- its tries.
            answered INTEGER NOT NULL DEFAULT 0
        )"#,
        r#"CREATE INDEX IF NOT EXISTS idx_alarm_due ON alarm(at_unix_ms)"#,
    ],
    seed: &[],
    data_steps: &[],
};

/// One wake as stored.
#[derive(Debug, Clone, PartialEq)]
pub struct AlarmRow {
    pub name: String,
    pub key: String,
    pub set_for_unix_ms: i64,
    pub role: String,
    pub path: String,
    pub body: serde_json::Value,
    pub attempts: i32,
    pub answered: i32,
}

impl FromRow for AlarmRow {
    fn from_row(row: Row) -> crate::db::Result<Self> {
        Ok(Self {
            name: row.try_get("name")?,
            key: row.try_get("key")?,
            set_for_unix_ms: row.try_get("set_for_unix_ms")?,
            role: row.try_get("role")?,
            path: row.try_get("path")?,
            body: row.try_get::<_, JsonText<serde_json::Value>>("body")?.0,
            attempts: row.try_get("attempts")?,
            answered: row.try_get("answered")?,
        })
    }
}

/// Store a wake. Setting one already stored does nothing.
pub async fn set(
    pool: &Pool,
    name: &str,
    key: &str,
    at_unix_ms: i64,
    role: &str,
    path: &str,
    body: &serde_json::Value,
) -> Result<()> {
    pool.execute(
        "INSERT INTO alarm (name, key, at_unix_ms, set_for_unix_ms, role, path, body) \
         VALUES ($1, $2, $3, $3, $4, $5, $6) ON CONFLICT (name) DO NOTHING",
        &[&name, &key, &at_unix_ms, &role, &path, &JsonText(body)],
    )
    .await?;
    Ok(())
}

/// Claim up to `limit` wakes due at `now_ms`: each one's time moves to
/// `now_ms + lease_ms`, so it is nobody else's while it is delivered.
pub async fn claim_due(pool: &Pool, now_ms: i64, lease_ms: i64, limit: i64) -> Result<Vec<AlarmRow>> {
    let rows = pool
        .all(
            "UPDATE alarm SET at_unix_ms = $1::bigint + $2::bigint, attempts = attempts + 1 \
             WHERE name IN ( \
                 SELECT name FROM alarm WHERE at_unix_ms <= $1 \
                 ORDER BY at_unix_ms LIMIT $3 FOR UPDATE SKIP LOCKED) \
             RETURNING name, key, set_for_unix_ms, role, path, body, attempts, answered",
            &[&now_ms, &lease_ms, &limit],
        )
        .await?;
    Ok(rows)
}

/// A delivered wake is done.
pub async fn delete(pool: &Pool, name: &str) -> Result<()> {
    pool.execute("DELETE FROM alarm WHERE name = $1", &[&name]).await?;
    Ok(())
}

/// A delivery failed: try again at `at_unix_ms`. `answered`: the receiver
/// answered it, so it counts as one of its tries.
pub async fn retry_at(pool: &Pool, name: &str, at_unix_ms: i64, answered: bool) -> Result<()> {
    pool.execute(
        "UPDATE alarm SET at_unix_ms = $2, answered = answered + CASE WHEN $3 THEN 1 ELSE 0 END WHERE name = $1",
        &[&name, &at_unix_ms, &answered],
    )
    .await?;
    Ok(())
}

/// When the next wake comes due, if any is stored.
pub async fn next_due(pool: &Pool) -> Result<Option<i64>> {
    Ok(pool.scalar::<Option<i64>>("SELECT MIN(at_unix_ms) FROM alarm", &[]).await?)
}
