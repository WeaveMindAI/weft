//! Tenant listener lease management.
//!
//! `tenant_listener` records the per-tenant listener Deployment +
//! its admin URL/token. Multi-Pod dispatchers re-attach to a
//! sibling Pod's listener via this row; restarts adopt orphan rows
//! whose owner has gone away.
//!
//! Lifecycle: `ListenerPool::with_listener` upserts the row inside
//! a transactional advisory-lock for state transitions. Every Pod
//! periodically calls `renew_tenant_listener` to keep the row's
//! ownership lease alive. On hard death the lease expires after
//! `LEASE_DURATION_SECS` and another Pod can adopt the row.
//!
//! Note: this row-ownership lease is distinct from the per-operation
//! advisory locks in `ListenerPool::with_listener`. The row lease
//! says "Pod X currently drives state transitions for this tenant";
//! the operation locks say "someone is currently using the listener,
//! reaper back off."

use anyhow::Result;
use sqlx::postgres::PgPool;

/// How long a row-ownership lease is valid before it must be renewed.
pub const LEASE_DURATION_SECS: i64 = 30;

/// Soft renewal interval. Owners renew this often to stay ahead of
/// expiry. Set well below `LEASE_DURATION_SECS`.
pub const LEASE_RENEW_INTERVAL_SECS: u64 = 10;

pub async fn migrate(pool: &PgPool) -> Result<()> {
    let stmts = [
        r#"CREATE TABLE IF NOT EXISTS tenant_listener (
            tenant_id TEXT PRIMARY KEY,
            owner_pod_id TEXT NOT NULL,
            leased_until_unix BIGINT NOT NULL,
            namespace TEXT NOT NULL,
            admin_url TEXT NOT NULL,
            state TEXT NOT NULL DEFAULT 'starting'
        )"#,
        r#"CREATE INDEX IF NOT EXISTS idx_tenant_listener_owner ON tenant_listener(owner_pod_id)"#,
    ];
    for sql in stmts {
        sqlx::query(sql).execute(pool).await?;
    }
    Ok(())
}

pub fn is_lease_live(leased_until_unix: i64) -> bool {
    leased_until_unix >= now_unix()
}

pub async fn renew_tenant_listener(
    pool: &PgPool,
    tenant_id: &str,
    pod_id: &str,
) -> Result<bool> {
    let leased_until = now_unix() + LEASE_DURATION_SECS;
    let res = sqlx::query(
        "UPDATE tenant_listener \
         SET leased_until_unix = $1 \
         WHERE tenant_id = $2 AND owner_pod_id = $3",
    )
    .bind(leased_until)
    .bind(tenant_id)
    .bind(pod_id)
    .execute(pool)
    .await?;
    Ok(res.rows_affected() > 0)
}

/// One row-state snapshot for the listener reaper. The reaper walks
/// every row, takes an EXCLUSIVE per-tenant operation lock to fence
/// off in-flight `with_listener` calls, then decides whether to kill.
#[derive(Debug, Clone)]
pub struct ListenerRowSnapshot {
    pub tenant_id: String,
    pub namespace: String,
    pub state: String,
}

pub async fn list_tenant_listener_rows(pool: &PgPool) -> Result<Vec<ListenerRowSnapshot>> {
    let rows: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT tenant_id, namespace, state FROM tenant_listener",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(tenant_id, namespace, state)| ListenerRowSnapshot {
            tenant_id,
            namespace,
            state,
        })
        .collect())
}

pub fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}
