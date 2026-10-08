//! The `unanswered_call` table: weft's own calls that keep failing.
//!
//! When weft cannot reach one of its own pieces (a role it rings awake, a
//! project's workers it hands a run to), it tries again on its own, and the
//! work behind the call waits meanwhile: an infra start nobody picks up, a
//! run nobody claims. The process that tries is not the one a person asks,
//! so each failure is written here, and `weft status` shows the ones still
//! failing, with the error, for whoever is waiting to see why.
//!
//! A row is written on every failed try. A role's ring is one call tried
//! until it goes through, which then drops the row. A run's hand-off is a
//! new call per run, and dropping the row on every one that goes through
//! would cost every run a write, so a row is read as current only while
//! its last failure is [`RECENT_MS`] old at most: a call still failing is
//! tried again well within that, and one that recovered stops being shown
//! once it has passed.

use anyhow::Result;
use sqlx::postgres::PgPool;

use crate::drain::DB_NOW_MS;

pub static GROUP: crate::SchemaGroup = crate::SchemaGroup {
    name: "unanswered_call",
    tables: &["unanswered_call"],
    ddl: &[r#"CREATE TABLE IF NOT EXISTS unanswered_call (
            -- What does not answer (`Callee::key`).
            callee TEXT PRIMARY KEY,
            -- The last try's error, as the caller logged it.
            error TEXT NOT NULL,
            -- The first failure of this stretch and the last, in unix
            -- milliseconds on the database's clock.
            since_ms BIGINT NOT NULL,
            last_ms BIGINT NOT NULL
        )"#],
    seed: &[],
};

/// How old a row's last failure may be and still be read as failing now. A
/// role's ring is tried again a minute after a failure at most, and a run's
/// hand-off at the delivery's next look, which is sooner.
pub const RECENT_MS: i64 = 120_000;

/// What weft could not reach.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Callee<'a> {
    /// A role it rings awake, by `CoreRole::as_str`.
    Role(&'a str),
    /// A project's workers, a run being handed to them.
    Workers(uuid::Uuid),
}

impl Callee<'_> {
    fn key(self) -> String {
        match self {
            Callee::Role(role) => format!("role:{role}"),
            Callee::Workers(project) => format!("workers:{project}"),
        }
    }
}

/// A call still failing, as read back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Failing {
    /// The role's name, or `None` for the project's workers.
    pub role: Option<String>,
    pub error: String,
    pub since_ms: i64,
    pub last_ms: i64,
    /// The database's clock when this was read, which the times above are
    /// on: how long ago reads right whatever clock the reader has.
    pub as_of_ms: i64,
}

/// A try at `callee` failed with `error`.
pub async fn failed(pool: &PgPool, callee: Callee<'_>, error: &str) -> Result<()> {
    sqlx::query(&format!(
        "INSERT INTO unanswered_call (callee, error, since_ms, last_ms) VALUES ($1, $2, {DB_NOW_MS}, {DB_NOW_MS}) \
         ON CONFLICT (callee) DO UPDATE SET error = EXCLUDED.error, last_ms = EXCLUDED.last_ms, \
             since_ms = CASE WHEN unanswered_call.last_ms < EXCLUDED.last_ms - {RECENT_MS} \
                 THEN EXCLUDED.since_ms ELSE unanswered_call.since_ms END"
    ))
    .bind(callee.key())
    .bind(error)
    .execute(pool)
    .await?;
    Ok(())
}

/// A try at `callee` went through after this caller's failed.
pub async fn answered(pool: &PgPool, callee: Callee<'_>) -> Result<()> {
    sqlx::query("DELETE FROM unanswered_call WHERE callee = $1").bind(callee.key()).execute(pool).await?;
    Ok(())
}

/// Every role still failing, and `project`'s workers if they are: what
/// the project's work waits on.
pub async fn failing_for(pool: &PgPool, project: uuid::Uuid) -> Result<Vec<Failing>> {
    let rows: Vec<(String, String, i64, i64, i64)> = sqlx::query_as(&format!(
        "SELECT callee, error, since_ms, last_ms, {DB_NOW_MS} FROM unanswered_call \
         WHERE (callee LIKE 'role:%' OR callee = $1) AND last_ms >= {DB_NOW_MS} - {RECENT_MS} \
         ORDER BY callee"
    ))
    .bind(Callee::Workers(project).key())
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(callee, error, since_ms, last_ms, as_of_ms)| Failing {
            role: callee.strip_prefix("role:").map(str::to_string),
            error,
            since_ms,
            last_ms,
            as_of_ms,
        })
        .collect())
}
