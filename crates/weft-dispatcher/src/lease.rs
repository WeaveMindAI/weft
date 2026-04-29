//! Lease management for HA dispatcher routing.
//!
//! Two kinds of leases coexist:
//!
//! - `slot_lease`: per-color execution ownership. The Pod that holds
//!   the worker's WebSocket renews this lease. Other Pods read it
//!   to forward inbound wakes to the right Pod.
//! - `tenant_listener`: per-tenant listener ownership + handle
//!   metadata (URL + tokens). The Pod that spawned the listener
//!   renews this lease. Persisting the handle here lets dispatcher
//!   restarts re-attach to existing listener Pods.
//!
//! Lease lifecycle: a Pod calls `claim_*` to take ownership (atomic
//! INSERT ... ON CONFLICT (...) DO UPDATE WHERE expired). It calls
//! `renew_*` periodically while it still owns the lease. On
//! graceful shutdown it calls `release_*`. On hard death the lease
//! expires after `LEASE_DURATION` and another Pod can claim it.

use anyhow::Result;
use sqlx::postgres::PgPool;

use weft_core::Color;

/// How long a lease is valid before it must be renewed.
pub const LEASE_DURATION_SECS: i64 = 30;

/// Soft renewal interval. Owners renew this often to stay ahead of
/// expiry. Set well below `LEASE_DURATION_SECS`.
pub const LEASE_RENEW_INTERVAL_SECS: u64 = 10;

/// Outcome of a `claim` call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClaimOutcome {
    /// We took the lease (fresh or expired before us). Caller is
    /// now the owner.
    AcquiredFresh,
    /// We re-took an expired lease. Same as `AcquiredFresh` from
    /// the caller's perspective; differs only in journaling.
    AcquiredAfterExpiry { previous_owner: String },
    /// Lease is held by a live owner. Caller is NOT the owner.
    /// The caller must forward inbound work to `current_owner`.
    HeldByOther { current_owner: String },
}

pub async fn migrate(pool: &PgPool) -> Result<()> {
    let stmts = [
        r#"CREATE TABLE IF NOT EXISTS slot_lease (
            color TEXT PRIMARY KEY,
            owner_pod_id TEXT NOT NULL,
            leased_until_unix BIGINT NOT NULL,
            last_renewed_unix BIGINT NOT NULL
        )"#,
        r#"CREATE INDEX IF NOT EXISTS idx_slot_lease_owner ON slot_lease(owner_pod_id)"#,
        r#"CREATE TABLE IF NOT EXISTS tenant_listener (
            tenant_id TEXT PRIMARY KEY,
            owner_pod_id TEXT NOT NULL,
            leased_until_unix BIGINT NOT NULL,
            last_renewed_unix BIGINT NOT NULL,
            namespace TEXT NOT NULL,
            deploy_name TEXT NOT NULL,
            admin_url TEXT NOT NULL,
            public_base_url TEXT NOT NULL,
            admin_token TEXT NOT NULL,
            relay_token TEXT NOT NULL
        )"#,
        r#"CREATE INDEX IF NOT EXISTS idx_tenant_listener_owner ON tenant_listener(owner_pod_id)"#,
    ];
    for sql in stmts {
        sqlx::query(sql).execute(pool).await?;
    }
    Ok(())
}

/// Try to claim ownership of `color`'s slot for `pod_id`. Atomic.
pub async fn claim_slot(
    pool: &PgPool,
    color: Color,
    pod_id: &str,
) -> Result<ClaimOutcome> {
    let now = now_unix();
    let leased_until = now + LEASE_DURATION_SECS;
    // Atomic claim: insert if no row, OR replace the row's owner if
    // the existing lease has expired (`leased_until_unix < now`).
    // Otherwise leave the row alone (the old owner keeps it).
    let row: Option<(String, i64)> = sqlx::query_as(
        r#"
        INSERT INTO slot_lease (color, owner_pod_id, leased_until_unix, last_renewed_unix)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (color) DO UPDATE
            SET owner_pod_id = CASE
                    WHEN slot_lease.leased_until_unix < $4
                      THEN EXCLUDED.owner_pod_id
                    ELSE slot_lease.owner_pod_id
                END,
                leased_until_unix = CASE
                    WHEN slot_lease.leased_until_unix < $4
                      THEN EXCLUDED.leased_until_unix
                    ELSE slot_lease.leased_until_unix
                END,
                last_renewed_unix = CASE
                    WHEN slot_lease.leased_until_unix < $4
                      THEN EXCLUDED.last_renewed_unix
                    ELSE slot_lease.last_renewed_unix
                END
        RETURNING owner_pod_id, leased_until_unix
        "#,
    )
    .bind(color.to_string())
    .bind(pod_id)
    .bind(leased_until)
    .bind(now)
    .fetch_optional(pool)
    .await?;
    let (winner, _) = row.ok_or_else(|| anyhow::anyhow!("claim_slot returned no row"))?;
    if winner == pod_id {
        Ok(ClaimOutcome::AcquiredFresh)
    } else {
        Ok(ClaimOutcome::HeldByOther { current_owner: winner })
    }
}

/// Renew the slot lease. Returns true if we still own it; false if
/// some other Pod has stolen the lease (e.g. our renew was late).
pub async fn renew_slot(
    pool: &PgPool,
    color: Color,
    pod_id: &str,
) -> Result<bool> {
    let now = now_unix();
    let leased_until = now + LEASE_DURATION_SECS;
    let res = sqlx::query(
        "UPDATE slot_lease SET leased_until_unix = $1, last_renewed_unix = $2 \
         WHERE color = $3 AND owner_pod_id = $4",
    )
    .bind(leased_until)
    .bind(now)
    .bind(color.to_string())
    .bind(pod_id)
    .execute(pool)
    .await?;
    Ok(res.rows_affected() > 0)
}

/// Release the slot lease (graceful shutdown). Idempotent.
pub async fn release_slot(
    pool: &PgPool,
    color: Color,
    pod_id: &str,
) -> Result<()> {
    sqlx::query(
        "DELETE FROM slot_lease WHERE color = $1 AND owner_pod_id = $2",
    )
    .bind(color.to_string())
    .bind(pod_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// Look up the current owner of a slot, ignoring expiry. Returns
/// `(owner, leased_until)`. Use `is_lease_live` to gate forwarding.
pub async fn lookup_slot(pool: &PgPool, color: Color) -> Result<Option<(String, i64)>> {
    let row: Option<(String, i64)> = sqlx::query_as(
        "SELECT owner_pod_id, leased_until_unix FROM slot_lease WHERE color = $1",
    )
    .bind(color.to_string())
    .fetch_optional(pool)
    .await?;
    Ok(row)
}

pub fn is_lease_live(leased_until_unix: i64) -> bool {
    leased_until_unix >= now_unix()
}

/// Persisted listener handle plus lease metadata.
#[derive(Debug, Clone)]
pub struct PersistedListener {
    pub tenant_id: String,
    pub owner_pod_id: String,
    pub leased_until_unix: i64,
    pub namespace: String,
    pub deploy_name: String,
    pub admin_url: String,
    pub public_base_url: String,
    pub admin_token: String,
    pub relay_token: String,
}

#[allow(clippy::too_many_arguments)]
pub async fn upsert_tenant_listener(
    pool: &PgPool,
    tenant_id: &str,
    pod_id: &str,
    namespace: &str,
    deploy_name: &str,
    admin_url: &str,
    public_base_url: &str,
    admin_token: &str,
    relay_token: &str,
) -> Result<()> {
    let now = now_unix();
    let leased_until = now + LEASE_DURATION_SECS;
    sqlx::query(
        r#"
        INSERT INTO tenant_listener (
            tenant_id, owner_pod_id, leased_until_unix, last_renewed_unix,
            namespace, deploy_name, admin_url, public_base_url,
            admin_token, relay_token
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        ON CONFLICT (tenant_id) DO UPDATE SET
            owner_pod_id = EXCLUDED.owner_pod_id,
            leased_until_unix = EXCLUDED.leased_until_unix,
            last_renewed_unix = EXCLUDED.last_renewed_unix,
            namespace = EXCLUDED.namespace,
            deploy_name = EXCLUDED.deploy_name,
            admin_url = EXCLUDED.admin_url,
            public_base_url = EXCLUDED.public_base_url,
            admin_token = EXCLUDED.admin_token,
            relay_token = EXCLUDED.relay_token
        "#,
    )
    .bind(tenant_id)
    .bind(pod_id)
    .bind(leased_until)
    .bind(now)
    .bind(namespace)
    .bind(deploy_name)
    .bind(admin_url)
    .bind(public_base_url)
    .bind(admin_token)
    .bind(relay_token)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn renew_tenant_listener(
    pool: &PgPool,
    tenant_id: &str,
    pod_id: &str,
) -> Result<bool> {
    let now = now_unix();
    let leased_until = now + LEASE_DURATION_SECS;
    let res = sqlx::query(
        "UPDATE tenant_listener \
         SET leased_until_unix = $1, last_renewed_unix = $2 \
         WHERE tenant_id = $3 AND owner_pod_id = $4",
    )
    .bind(leased_until)
    .bind(now)
    .bind(tenant_id)
    .bind(pod_id)
    .execute(pool)
    .await?;
    Ok(res.rows_affected() > 0)
}

pub async fn release_tenant_listener(
    pool: &PgPool,
    tenant_id: &str,
    pod_id: &str,
) -> Result<()> {
    sqlx::query(
        "DELETE FROM tenant_listener WHERE tenant_id = $1 AND owner_pod_id = $2",
    )
    .bind(tenant_id)
    .bind(pod_id)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn delete_tenant_listener(pool: &PgPool, tenant_id: &str) -> Result<()> {
    sqlx::query("DELETE FROM tenant_listener WHERE tenant_id = $1")
        .bind(tenant_id)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn lookup_tenant_listener(
    pool: &PgPool,
    tenant_id: &str,
) -> Result<Option<PersistedListener>> {
    let row: Option<(String, String, i64, String, String, String, String, String, String)> =
        sqlx::query_as(
            "SELECT owner_pod_id, namespace, leased_until_unix, deploy_name, admin_url, \
                    public_base_url, admin_token, relay_token, tenant_id \
             FROM tenant_listener WHERE tenant_id = $1",
        )
        .bind(tenant_id)
        .fetch_optional(pool)
        .await?;
    Ok(row.map(
        |(owner_pod_id, namespace, leased_until_unix, deploy_name, admin_url,
          public_base_url, admin_token, relay_token, tenant_id_back)| PersistedListener {
            tenant_id: tenant_id_back,
            owner_pod_id,
            leased_until_unix,
            namespace,
            deploy_name,
            admin_url,
            public_base_url,
            admin_token,
            relay_token,
        },
    ))
}

pub async fn list_tenant_listeners(pool: &PgPool) -> Result<Vec<PersistedListener>> {
    let rows: Vec<(String, String, i64, String, String, String, String, String, String)> =
        sqlx::query_as(
            "SELECT owner_pod_id, namespace, leased_until_unix, deploy_name, admin_url, \
                    public_base_url, admin_token, relay_token, tenant_id \
             FROM tenant_listener",
        )
        .fetch_all(pool)
        .await?;
    Ok(rows
        .into_iter()
        .map(
            |(owner_pod_id, namespace, leased_until_unix, deploy_name, admin_url,
              public_base_url, admin_token, relay_token, tenant_id)| PersistedListener {
                tenant_id,
                owner_pod_id,
                leased_until_unix,
                namespace,
                deploy_name,
                admin_url,
                public_base_url,
                admin_token,
                relay_token,
            },
        )
        .collect())
}

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}
