//! Per-project infra registry, backed by Postgres `infra_node` rows.
//!
//! One row per `(project_id, infra_node_id)` pair tracking the
//! desired-vs-applied state of one infra node. The supervisor pod
//! writes status transitions as it executes a claimed
//! `infra_lifecycle_command`, and writes runtime events (Flaky /
//! Recovered) and may flip status as part of that execution.
//!
//! Workers / listeners read `endpoints_json` via the broker's
//! `/v1/infra/endpoint_url` endpoint (NOT via this module).

use anyhow::Result;
use serde_json::Value;
use sqlx::postgres::PgPool;
use sqlx::Row;
use std::collections::BTreeMap;

// `InfraNodeStatus` and `FailureStage` are the wire contract for the
// `status` and `failure_stage` columns of `infra_node`. They live in
// `weft-broker-client::protocol` so the supervisor + dispatcher share
// one source of truth. Re-export here so this module stays the
// canonical home for infra_node glue.
pub use weft_broker_client::protocol::{FailureStage, InfraNodeStatus, UnitRuntime};

/// One row in `infra_node`.
#[derive(Debug, Clone)]
pub struct InfraNodeRow {
    pub project_id: String,
    pub node_id: String,
    /// Stable per-apply id (Deployment name etc). Empty string when
    /// status is `Failed` and the apply never produced one.
    pub instance_id: String,
    /// Project namespace (`wft-project-<tenant>-<project>`).
    pub namespace: String,
    pub status: InfraNodeStatus,
    pub failure_stage: Option<FailureStage>,
    pub failure_message: Option<String>,
    /// Hash of the resolved manifest set this row was last applied
    /// against. Set by the apply task on success; used by the next
    /// apply attempt to decide skip-vs-roll.
    pub applied_spec_hash: Option<String>,
    pub applied_at_unix: Option<i64>,
    /// Endpoint name → cluster-internal URL. `BTreeMap`: callers
    /// resolve endpoints by name, and a deterministic order keeps any
    /// "first" semantics stable.
    pub endpoints: BTreeMap<String, String>,
    /// PVC names to KEEP on terminate. Carried from
    /// `InfraSpec.lifecycle.on_terminate.preserve_pvcs` at apply
    /// time so the supervisor can honor it at terminate time
    /// (terminate has no access to the spec; the worker that
    /// applied the spec is long gone).
    pub preserve_pvcs: Vec<String>,
    /// Per-unit runtime (status + resolved health windows +
    /// stop_behavior), keyed by unit name. The `status` column above
    /// is a rollup over these. Stamped at apply from the spec's units;
    /// the authoritative unit roster the supervisor operates on.
    pub units: BTreeMap<String, UnitRuntime>,
}

pub async fn migrate(pool: &PgPool) -> Result<()> {
    let stmts = [
        r#"CREATE TABLE IF NOT EXISTS infra_node (
            project_id          TEXT NOT NULL,
            node_id             TEXT NOT NULL,
            instance_id         TEXT NOT NULL DEFAULT '',
            namespace           TEXT NOT NULL,
            status              TEXT NOT NULL,
            failure_stage       TEXT,
            failure_message     TEXT,
            applied_spec_hash   TEXT,
            applied_at_unix     BIGINT,
            endpoints_json      JSONB NOT NULL DEFAULT '{}'::jsonb,
            -- PVC names to preserve on terminate. JSON array;
            -- empty means "delete all matching PVCs."
            preserve_pvcs_json  JSONB NOT NULL DEFAULT '[]'::jsonb,
            -- Per-unit runtime (status + resolved health windows +
            -- stop_behavior) keyed by unit name. The `status` column
            -- is a rollup over these. Stamped at apply.
            units_json          JSONB NOT NULL DEFAULT '{}'::jsonb,
            PRIMARY KEY (project_id, node_id)
        )"#,
        r#"CREATE INDEX IF NOT EXISTS idx_infra_node_project   ON infra_node(project_id)"#,
        r#"CREATE INDEX IF NOT EXISTS idx_infra_node_namespace ON infra_node(namespace)"#,
    ];
    for sql in stmts {
        sqlx::query(sql).execute(pool).await?;
    }
    Ok(())
}

/// Upsert a row in `infra_node`. Used by the apply task's status
/// transitions and by the supervisor's lifecycle commands. The
/// caller supplies the full target state; this function overwrites
/// all columns (idempotent UPSERT on the (project, node) key).
pub async fn upsert(pool: &PgPool, row: &InfraNodeRow) -> Result<()> {
    let endpoints_json = serde_json::to_value(&row.endpoints)?;
    let preserve_pvcs_json = serde_json::to_value(&row.preserve_pvcs)?;
    let units_json = serde_json::to_value(&row.units)?;
    sqlx::query(
        "INSERT INTO infra_node \
         (project_id, node_id, instance_id, namespace, status, \
          failure_stage, failure_message, applied_spec_hash, \
          applied_at_unix, endpoints_json, preserve_pvcs_json, units_json) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) \
         ON CONFLICT (project_id, node_id) DO UPDATE SET \
            instance_id        = EXCLUDED.instance_id, \
            namespace          = EXCLUDED.namespace, \
            status             = EXCLUDED.status, \
            failure_stage      = EXCLUDED.failure_stage, \
            failure_message    = EXCLUDED.failure_message, \
            applied_spec_hash  = EXCLUDED.applied_spec_hash, \
            applied_at_unix    = EXCLUDED.applied_at_unix, \
            endpoints_json     = EXCLUDED.endpoints_json, \
            preserve_pvcs_json = EXCLUDED.preserve_pvcs_json, \
            units_json         = EXCLUDED.units_json",
    )
    .bind(&row.project_id)
    .bind(&row.node_id)
    .bind(&row.instance_id)
    .bind(&row.namespace)
    .bind(row.status.as_str())
    .bind(row.failure_stage.map(|f| f.as_str()))
    .bind(row.failure_message.as_deref())
    .bind(row.applied_spec_hash.as_deref())
    .bind(row.applied_at_unix)
    .bind(endpoints_json)
    .bind(preserve_pvcs_json)
    .bind(units_json)
    .execute(pool)
    .await?;
    Ok(())
}

/// Update just the status column. Idempotent: identical writes
/// produce no observable effect. Used by the supervisor and the
/// stop/terminate API handlers for transient states like Stopping.
pub async fn set_status(
    pool: &PgPool,
    project_id: &str,
    node_id: &str,
    status: InfraNodeStatus,
) -> Result<()> {
    sqlx::query("UPDATE infra_node SET status = $1 WHERE project_id = $2 AND node_id = $3")
        .bind(status.as_str())
        .bind(project_id)
        .bind(node_id)
        .execute(pool)
        .await?;
    Ok(())
}

/// Read one (project, node) row. Returns None when the row doesn't
/// exist (no infra was ever applied for this node).
pub async fn get(pool: &PgPool, project_id: &str, node_id: &str) -> Result<Option<InfraNodeRow>> {
    let row = sqlx::query(
        "SELECT project_id, node_id, instance_id, namespace, status, \
                failure_stage, failure_message, applied_spec_hash, \
                applied_at_unix, endpoints_json, preserve_pvcs_json, units_json \
         FROM infra_node WHERE project_id = $1 AND node_id = $2",
    )
    .bind(project_id)
    .bind(node_id)
    .fetch_optional(pool)
    .await?;
    match row {
        None => Ok(None),
        Some(r) => Ok(Some(parse_row(r)?)),
    }
}

/// List every row for a project. Drives the project status response.
pub async fn list_for_project(
    pool: &PgPool,
    project_id: &str,
) -> Result<Vec<InfraNodeRow>> {
    let rows = sqlx::query(
        "SELECT project_id, node_id, instance_id, namespace, status, \
                failure_stage, failure_message, applied_spec_hash, \
                applied_at_unix, endpoints_json, preserve_pvcs_json, units_json \
         FROM infra_node WHERE project_id = $1",
    )
    .bind(project_id)
    .fetch_all(pool)
    .await?;
    rows.into_iter().map(parse_row).collect()
}

/// Whether ANY infra_node row exists for the project (regardless of
/// status). The "live infra state exists" fact worker placement keys
/// on: a project whose infra was never provisioned (or fully
/// terminated) has no rows, so its worker (including the InfraSetup
/// provisioning execution) runs in the shared pool; the first apply
/// writes a row and subsequent workers land in the project namespace.
pub async fn any_for_project(pool: &PgPool, project_id: &str) -> Result<bool> {
    let (exists,): (bool,) =
        sqlx::query_as("SELECT EXISTS(SELECT 1 FROM infra_node WHERE project_id = $1)")
            .bind(project_id)
            .fetch_one(pool)
            .await?;
    Ok(exists)
}

/// Delete a row by (project, node). Idempotent. Called by the
/// supervisor after a successful terminate.
pub async fn remove(pool: &PgPool, project_id: &str, node_id: &str) -> Result<bool> {
    let res = sqlx::query("DELETE FROM infra_node WHERE project_id = $1 AND node_id = $2")
        .bind(project_id)
        .bind(node_id)
        .execute(pool)
        .await?;
    Ok(res.rows_affected() > 0)
}

/// Drop every row for a project. Called on `weft rm` after the
/// project's terminate has completed.
pub async fn remove_project(pool: &PgPool, project_id: &str) -> Result<u64> {
    let res = sqlx::query("DELETE FROM infra_node WHERE project_id = $1")
        .bind(project_id)
        .execute(pool)
        .await?;
    Ok(res.rows_affected())
}

/// Look up the cluster-internal URL for an endpoint of a Running
/// infra node. None if not running or endpoint name not declared.
/// Used by the broker's `infra/endpoint_url` handler.
pub async fn endpoint_url(
    pool: &PgPool,
    project_id: &str,
    node_id: &str,
    endpoint_name: &str,
) -> Result<Option<String>> {
    let row = sqlx::query(
        "SELECT endpoints_json FROM infra_node \
         WHERE project_id = $1 AND node_id = $2 AND status = 'running'",
    )
    .bind(project_id)
    .bind(node_id)
    .fetch_optional(pool)
    .await?;
    let Some(r) = row else {
        return Ok(None);
    };
    // Decode fails loud: a corrupt / schema-drifted `endpoints_json`
    // must surface as an error, not masquerade as "endpoint not
    // available" (which would send the worker chasing a phantom
    // missing-endpoint). A missing endpoint NAME inside a valid
    // object is the legitimate `None` (not declared).
    let v: Value = r.try_get("endpoints_json")?;
    Ok(v.as_object()
        .and_then(|m| m.get(endpoint_name))
        .and_then(|val| val.as_str().map(|s| s.to_string())))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_round_trips() {
        for s in [
            InfraNodeStatus::Provisioning,
            InfraNodeStatus::Running,
            InfraNodeStatus::Stopped,
            InfraNodeStatus::Flaky,
            InfraNodeStatus::Failed,
            InfraNodeStatus::Stopping,
            InfraNodeStatus::Terminating,
        ] {
            assert_eq!(InfraNodeStatus::parse(s.as_str()), Some(s));
        }
    }

    #[test]
    fn status_parse_unknown_returns_none() {
        assert_eq!(InfraNodeStatus::parse("garbage"), None);
        assert_eq!(InfraNodeStatus::parse(""), None);
        // Casing matters; status strings are lowercase on the wire.
        assert_eq!(InfraNodeStatus::parse("Running"), None);
    }

    #[test]
    fn status_as_str_is_lowercase_snake() {
        // The supervisor + apply executor write these directly into
        // the row; the parse() round-trip above already proves them,
        // but pin exact wire bytes too so a casual rename doesn't
        // silently break the SSE protocol.
        assert_eq!(InfraNodeStatus::Provisioning.as_str(), "provisioning");
        assert_eq!(InfraNodeStatus::Running.as_str(), "running");
        assert_eq!(InfraNodeStatus::Stopped.as_str(), "stopped");
        assert_eq!(InfraNodeStatus::Flaky.as_str(), "flaky");
        assert_eq!(InfraNodeStatus::Failed.as_str(), "failed");
        assert_eq!(InfraNodeStatus::Stopping.as_str(), "stopping");
        assert_eq!(InfraNodeStatus::Terminating.as_str(), "terminating");
    }

    #[test]
    fn failure_stage_as_str() {
        assert_eq!(FailureStage::Provision.as_str(), "provision");
        assert_eq!(FailureStage::Apply.as_str(), "apply");
        assert_eq!(FailureStage::Execute.as_str(), "execute");
        assert_eq!(FailureStage::ApplyLifecycle.as_str(), "apply_lifecycle");
    }
}

/// Decode one `infra_node` row. Every NOT-NULL column is required;
/// every nullable column is `Option<T>` and surfaces as such. A
/// decode failure on ANY column is schema drift (or a wrong-typed
/// JSON column) and propagates as `Err` rather than being coerced
/// to None / empty (which would let downstream observe a half-valid
/// row).
fn parse_row(row: sqlx::postgres::PgRow) -> anyhow::Result<InfraNodeRow> {
    let project_id: String = row.try_get("project_id")?;
    let node_id: String = row.try_get("node_id")?;
    let instance_id: String = row.try_get("instance_id")?;
    let namespace: String = row.try_get("namespace")?;
    let status_str: String = row.try_get("status")?;
    let status = InfraNodeStatus::parse(&status_str).ok_or_else(|| {
        anyhow::anyhow!(
            "infra_node.status='{status_str}' for project={project_id} node={node_id} \
             is not a known InfraNodeStatus"
        )
    })?;
    let failure_stage_str: Option<String> = row.try_get("failure_stage")?;
    let failure_stage = match failure_stage_str.as_deref() {
        None => None,
        Some(s) => Some(FailureStage::parse(s).ok_or_else(|| {
            anyhow::anyhow!(
                "infra_node.failure_stage='{s}' for project={project_id} node={node_id} \
                 is not a known FailureStage"
            )
        })?),
    };
    let failure_message: Option<String> = row.try_get("failure_message")?;
    let applied_spec_hash: Option<String> = row.try_get("applied_spec_hash")?;
    let applied_at_unix: Option<i64> = row.try_get("applied_at_unix")?;
    let endpoints_json: Value = row.try_get("endpoints_json")?;
    let endpoints: BTreeMap<String, String> = serde_json::from_value(endpoints_json)
        .map_err(|e| {
            anyhow::anyhow!(
                "infra_node.endpoints_json for project={project_id} node={node_id} \
                 is not a string-to-string map: {e}"
            )
        })?;
    let preserve_pvcs_json: Value = row.try_get("preserve_pvcs_json")?;
    let preserve_pvcs: Vec<String> = serde_json::from_value(preserve_pvcs_json)
        .map_err(|e| {
            anyhow::anyhow!(
                "infra_node.preserve_pvcs_json for project={project_id} node={node_id} \
                 is not a Vec<String>: {e}"
            )
        })?;
    let units_json: Value = row.try_get("units_json")?;
    let units: BTreeMap<String, UnitRuntime> = serde_json::from_value(units_json)
        .map_err(|e| {
            anyhow::anyhow!(
                "infra_node.units_json for project={project_id} node={node_id} \
                 is not a unit-name-to-UnitRuntime map: {e}"
            )
        })?;
    Ok(InfraNodeRow {
        project_id,
        node_id,
        instance_id,
        namespace,
        status,
        failure_stage,
        failure_message,
        applied_spec_hash,
        applied_at_unix,
        endpoints,
        preserve_pvcs,
        units,
    })
}
