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
use sqlx::postgres::PgPool;

pub static GROUP: crate::SchemaGroup = crate::SchemaGroup {
    name: "alarm",
    tables: &["alarm"],
    ddl: &[
        r#"CREATE TABLE IF NOT EXISTS alarm (
            -- The wake's name (`Wake::name`): the same key and time is the
            -- same wake, set once however often it is asked for.
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
            body JSONB NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0
        )"#,
        r#"CREATE INDEX IF NOT EXISTS idx_alarm_due ON alarm(at_unix_ms)"#,
    ],
    seed: &[],
};

/// One wake as stored.
#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct AlarmRow {
    pub name: String,
    pub key: String,
    pub set_for_unix_ms: i64,
    pub role: String,
    pub path: String,
    pub body: serde_json::Value,
    pub attempts: i32,
}

/// Store a wake. Setting one already stored does nothing.
pub async fn set(
    pool: &PgPool,
    name: &str,
    key: &str,
    at_unix_ms: i64,
    role: &str,
    path: &str,
    body: &serde_json::Value,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO alarm (name, key, at_unix_ms, set_for_unix_ms, role, path, body) \
         VALUES ($1, $2, $3, $3, $4, $5, $6) ON CONFLICT (name) DO NOTHING",
    )
    .bind(name)
    .bind(key)
    .bind(at_unix_ms)
    .bind(role)
    .bind(path)
    .bind(body)
    .execute(pool)
    .await?;
    Ok(())
}

/// Claim up to `limit` wakes due at `now_ms`: each one's time moves to
/// `now_ms + lease_ms`, so it is nobody else's while it is delivered.
pub async fn claim_due(pool: &PgPool, now_ms: i64, lease_ms: i64, limit: i64) -> Result<Vec<AlarmRow>> {
    let rows = sqlx::query_as::<_, AlarmRow>(
        "UPDATE alarm SET at_unix_ms = $1 + $2, attempts = attempts + 1 \
         WHERE name IN ( \
             SELECT name FROM alarm WHERE at_unix_ms <= $1 \
             ORDER BY at_unix_ms LIMIT $3 FOR UPDATE SKIP LOCKED) \
         RETURNING name, key, set_for_unix_ms, role, path, body, attempts",
    )
    .bind(now_ms)
    .bind(lease_ms)
    .bind(limit)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// A delivered wake is done.
pub async fn delete(pool: &PgPool, name: &str) -> Result<()> {
    sqlx::query("DELETE FROM alarm WHERE name = $1").bind(name).execute(pool).await?;
    Ok(())
}

/// A delivery failed: try again at `at_unix_ms`.
pub async fn retry_at(pool: &PgPool, name: &str, at_unix_ms: i64) -> Result<()> {
    sqlx::query("UPDATE alarm SET at_unix_ms = $2 WHERE name = $1").bind(name).bind(at_unix_ms).execute(pool).await?;
    Ok(())
}

/// When the next wake comes due, if any is stored.
pub async fn next_due(pool: &PgPool) -> Result<Option<i64>> {
    Ok(sqlx::query_scalar::<_, Option<i64>>("SELECT MIN(at_unix_ms) FROM alarm").fetch_one(pool).await?)
}
