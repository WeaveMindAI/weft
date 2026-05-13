//! Per-project infra registry, backed by Postgres `infra_pod` rows.
//!
//! Each row records a sidecar the dispatcher has provisioned for a
//! `(project_id, node_id)` pair. Lifecycle:
//!   - `Running`: k8s Deployment at replicas=1, sidecar reachable.
//!   - `Stopped`: Deployment scaled to 0, Service / PVC / Ingress
//!     kept so `start` can bring it back with state intact.
//!
//! Workers query this table directly via `ctx.sidecar_endpoint()`
//! (read-only, no task round-trip). Provision / scale / terminate
//! all go through dispatcher tasks.

use anyhow::Result;
use serde::Serialize;
use sqlx::postgres::PgPool;
use sqlx::Row;

use crate::backend::InfraHandle;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum InfraStatus {
    Running,
    Stopped,
}

impl InfraStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Stopped => "stopped",
        }
    }

    fn parse(s: &str) -> Option<Self> {
        match s {
            "running" => Some(Self::Running),
            "stopped" => Some(Self::Stopped),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct InfraEntry {
    pub handle: InfraHandle,
    pub status: InfraStatus,
}

pub async fn migrate(pool: &PgPool) -> Result<()> {
    let stmts = [
        // running_image_hash: sidecar source-hash the image was built
        // from. Set by `infra/start` (and later `infra/upgrade`)
        // BEFORE provision so the provision_sidecar task can pull it
        // for the docker image tag, then read back by drift-detection
        // to compare against what the CLI's current source would
        // hash to.
        r#"CREATE TABLE IF NOT EXISTS infra_pod (
            project_id TEXT NOT NULL,
            node_id TEXT NOT NULL,
            instance_id TEXT NOT NULL,
            endpoint_url TEXT,
            namespace TEXT NOT NULL,
            status TEXT NOT NULL,
            created_at_unix BIGINT NOT NULL,
            running_image_hash TEXT,
            PRIMARY KEY (project_id, node_id)
        )"#,
        r#"CREATE INDEX IF NOT EXISTS idx_infra_pod_project ON infra_pod(project_id)"#,
    ];
    for sql in stmts {
        sqlx::query(sql).execute(pool).await?;
    }
    Ok(())
}

/// Pre-set the desired sidecar image-hash for a node before
/// provision runs. The hash becomes the docker image tag suffix
/// (`weft-sidecar-<name>:<hash>`) when provision_sidecar fires.
/// Creates a row with status=stopped and a placeholder handle if
/// none exists yet, otherwise updates the existing row's hash.
pub async fn set_pending_image_hash(
    pool: &PgPool,
    project_id: &str,
    node_id: &str,
    namespace: &str,
    image_hash: &str,
) -> Result<()> {
    let now = unix_now();
    sqlx::query(
        "INSERT INTO infra_pod \
         (project_id, node_id, instance_id, endpoint_url, namespace, status, created_at_unix, running_image_hash) \
         VALUES ($1, $2, '', NULL, $3, $4, $5, $6) \
         ON CONFLICT (project_id, node_id) DO UPDATE \
         SET running_image_hash = EXCLUDED.running_image_hash",
    )
    .bind(project_id)
    .bind(node_id)
    .bind(namespace)
    .bind(InfraStatus::Stopped.as_str())
    .bind(now)
    .bind(image_hash)
    .execute(pool)
    .await?;
    Ok(())
}

/// Read the desired sidecar image-hash for (project, node).
/// Returns None when no infra_pod row exists or when the row was
/// never tagged with a hash (legacy rows from before this column
/// was added).
pub async fn pending_image_hash(
    pool: &PgPool,
    project_id: &str,
    node_id: &str,
) -> Result<Option<String>> {
    let row: Option<(Option<String>,)> = sqlx::query_as(
        "SELECT running_image_hash FROM infra_pod WHERE project_id = $1 AND node_id = $2",
    )
    .bind(project_id)
    .bind(node_id)
    .fetch_optional(pool)
    .await?;
    Ok(row.and_then(|(h,)| h))
}

pub async fn insert_running(
    pool: &PgPool,
    project_id: &str,
    node_id: &str,
    handle: &InfraHandle,
) -> Result<()> {
    upsert_with_status(pool, project_id, node_id, handle, InfraStatus::Running).await
}

pub async fn upsert_with_status(
    pool: &PgPool,
    project_id: &str,
    node_id: &str,
    handle: &InfraHandle,
    status: InfraStatus,
) -> Result<()> {
    let now = unix_now();
    sqlx::query(
        "INSERT INTO infra_pod \
         (project_id, node_id, instance_id, endpoint_url, namespace, status, created_at_unix) \
         VALUES ($1, $2, $3, $4, $5, $6, $7) \
         ON CONFLICT (project_id, node_id) DO UPDATE \
         SET instance_id = EXCLUDED.instance_id, \
             endpoint_url = EXCLUDED.endpoint_url, \
             namespace = EXCLUDED.namespace, \
             status = EXCLUDED.status",
    )
    .bind(project_id)
    .bind(node_id)
    .bind(&handle.id)
    .bind(handle.endpoint_url.as_deref())
    .bind(&handle.namespace)
    .bind(status.as_str())
    .bind(now)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn set_status(
    pool: &PgPool,
    project_id: &str,
    node_id: &str,
    status: InfraStatus,
) -> Result<()> {
    sqlx::query(
        "UPDATE infra_pod SET status = $1 WHERE project_id = $2 AND node_id = $3",
    )
    .bind(status.as_str())
    .bind(project_id)
    .bind(node_id)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn get(
    pool: &PgPool,
    project_id: &str,
    node_id: &str,
) -> Result<Option<InfraEntry>> {
    let row = sqlx::query(
        "SELECT instance_id, endpoint_url, namespace, status \
         FROM infra_pod WHERE project_id = $1 AND node_id = $2",
    )
    .bind(project_id)
    .bind(node_id)
    .fetch_optional(pool)
    .await?;
    Ok(row.and_then(parse_row))
}

/// Returns the handle only when the sidecar is `Running`. Callers
/// looking up an endpoint to call get None for Stopped sidecars so
/// they fail loudly instead of dialing a dead DNS name.
pub async fn handle_if_running(
    pool: &PgPool,
    project_id: &str,
    node_id: &str,
) -> Result<Option<InfraHandle>> {
    let entry = get(pool, project_id, node_id).await?;
    Ok(entry.and_then(|e| match e.status {
        InfraStatus::Running => Some(e.handle),
        InfraStatus::Stopped => None,
    }))
}

pub async fn remove(
    pool: &PgPool,
    project_id: &str,
    node_id: &str,
) -> Result<Option<InfraEntry>> {
    let row = sqlx::query(
        "DELETE FROM infra_pod WHERE project_id = $1 AND node_id = $2 \
         RETURNING instance_id, endpoint_url, namespace, status",
    )
    .bind(project_id)
    .bind(node_id)
    .fetch_optional(pool)
    .await?;
    Ok(row.and_then(parse_row))
}

pub async fn list_for_project(
    pool: &PgPool,
    project_id: &str,
) -> Result<Vec<(String, InfraEntry)>> {
    let rows = sqlx::query(
        "SELECT node_id, instance_id, endpoint_url, namespace, status \
         FROM infra_pod WHERE project_id = $1",
    )
    .bind(project_id)
    .fetch_all(pool)
    .await?;
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let node_id: String = row.try_get("node_id")?;
        if let Some(entry) = parse_row(row) {
            out.push((node_id, entry));
        }
    }
    Ok(out)
}

pub async fn remove_project(
    pool: &PgPool,
    project_id: &str,
) -> Result<Vec<(String, InfraEntry)>> {
    let rows = sqlx::query(
        "DELETE FROM infra_pod WHERE project_id = $1 \
         RETURNING node_id, instance_id, endpoint_url, namespace, status",
    )
    .bind(project_id)
    .fetch_all(pool)
    .await?;
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let node_id: String = row.try_get("node_id")?;
        if let Some(entry) = parse_row(row) {
            out.push((node_id, entry));
        }
    }
    Ok(out)
}

fn parse_row(row: sqlx::postgres::PgRow) -> Option<InfraEntry> {
    let instance_id: String = row.try_get("instance_id").ok()?;
    let endpoint_url: Option<String> = row.try_get("endpoint_url").ok().flatten();
    let namespace: String = row.try_get("namespace").ok()?;
    let status_str: String = row.try_get("status").ok()?;
    let status = InfraStatus::parse(&status_str)?;
    Some(InfraEntry {
        handle: InfraHandle {
            id: instance_id,
            endpoint_url,
            namespace,
        },
        status,
    })
}

fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}
