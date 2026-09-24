//! Worker-facing task, worker-pod and infra surfaces.
//!
//! The task and worker-pod shapes mirror the free functions in `tasks`
//! and `worker_pod`; `InfraReader` reads the dispatcher-written
//! `infra_node` table. Two implementations per trait:
//!   - `Postgres*` (this crate): direct DB. Used by the dispatcher and
//!     by the broker (after its scope check).
//!   - `Broker*` (in `weft-broker-client`): HTTP through the broker.
//!     Used by workers and listeners.
//!
//! The engine takes `TaskStoreClient`, `WorkerPodClient` and
//! `InfraReader`; the listener takes only `TaskStoreClient`.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;
use sqlx::postgres::PgPool;
use sqlx::Row;
use uuid::Uuid;

use crate::pg_signal::PgSignalWatch;
use crate::worker_pod::WorkerStanding;
use crate::tasks::{
    ClaimFilter, DedupOutcome, NewTask, Task, TaskOutcome,
};

#[async_trait]
pub trait TaskStoreClient: Send + Sync {
    async fn enqueue_dedup(&self, spec: NewTask) -> Result<DedupOutcome>;

    /// Wait for the task to finish, or for `timeout` to pass, and hand
    /// back its outcome either way. Ends the moment the task does (see
    /// [`crate::terminal`]), never on a polling tick.
    async fn wait_for_terminal(&self, task_id: Uuid, timeout: Duration) -> Result<TaskOutcome>;

    /// Picker primitive: claim one pending or stale-claimed row that
    /// matches the filter, and when there is none, hold for up to
    /// `wait` for one to become claimable (see
    /// [`crate::tasks::TASK_READY_CHANNEL`]). `None` once `wait` passed
    /// with nothing to claim; a zero `wait` answers at once. Used by
    /// both pickers.
    async fn claim_one(&self, pod_id: &str, filter: ClaimFilter, wait: Duration) -> Result<Option<Task>>;

    async fn heartbeat(&self, task_id: Uuid, pod_id: &str) -> Result<bool>;

    /// Surrender a claim back to `pending` (no claimant), guarded on
    /// `claimed_by = pod_id` so a row already re-claimed elsewhere is
    /// never clobbered. Returns true when the requeue landed. See
    /// `tasks::requeue`.
    async fn requeue(&self, task_id: Uuid, pod_id: &str) -> Result<bool>;

    async fn complete(&self, task_id: Uuid, pod_id: &str, result: Value) -> Result<()>;

    async fn fail(&self, task_id: Uuid, pod_id: &str, error: String) -> Result<()>;
}

#[async_trait]
pub trait WorkerPodClient: Send + Sync {
    async fn register_alive(
        &self,
        pod_name: &str,
        project_id: Uuid,
    ) -> Result<()>;

    /// Heartbeat + self-reported memory pressure ([0,1]) in one call.
    /// The worker reads its own cgroup pressure each tick and reports it
    /// so the dispatcher places / scales workers by real memory load.
    /// Answers the pod's standing off its own row (`None`: the row is no
    /// longer alive, the pod shuts down). See `worker_pod::heartbeat`.
    async fn heartbeat(&self, pod_name: &str, mem_pressure: f64) -> Result<Option<WorkerStanding>>;

    async fn mark_done(&self, pod_name: &str) -> Result<()>;

    /// Guarded idle self-exit: flip `alive -> done` IFF no
    /// pending/claimed worker task for the pod's own project (read
    /// from its row, not a parameter). Returns true if this pod won
    /// the flip. See `worker_pod::mark_done_if_idle`.
    async fn mark_done_if_idle(&self, pod_name: &str) -> Result<bool>;
}

// ---------- Postgres impls ----------

pub struct PostgresTaskStoreClient {
    pool: PgPool,
    /// The process's one `LISTEN` connection, which every wait here
    /// sleeps on.
    signals: Arc<PgSignalWatch>,
}

impl PostgresTaskStoreClient {
    /// `signals` must listen on the channels this client's waits sleep
    /// on, [`crate::tasks::TASK_READY_CHANNEL`] and
    /// [`crate::terminal::TERMINAL_CHANNEL`].
    pub fn new(pool: PgPool, signals: Arc<PgSignalWatch>) -> Result<Self> {
        signals.require(crate::tasks::TASK_READY_CHANNEL)?;
        signals.require(crate::terminal::TERMINAL_CHANNEL)?;
        Ok(Self { pool, signals })
    }
}

#[async_trait]
impl TaskStoreClient for PostgresTaskStoreClient {
    async fn enqueue_dedup(&self, spec: NewTask) -> Result<DedupOutcome> {
        crate::tasks::enqueue_dedup(&self.pool, spec).await
    }

    async fn wait_for_terminal(&self, task_id: Uuid, timeout: Duration) -> Result<TaskOutcome> {
        crate::terminal::wait_for_terminal(&self.pool, &self.signals, task_id, timeout).await
    }

    async fn claim_one(&self, pod_id: &str, filter: ClaimFilter, wait: Duration) -> Result<Option<Task>> {
        let deadline = tokio::time::Instant::now() + wait;
        // Subscribed before the first claim, so a task that lands
        // between an empty claim and the wait still wakes it.
        let mut signals = self.signals.subscribe();
        let ready = filter.ready_payload();
        loop {
            if let Some(task) = crate::tasks::claim_one(&self.pool, pod_id, &filter).await? {
                return Ok(Some(task));
            }
            if !signals.woken_before(deadline, |c, p| c == crate::tasks::TASK_READY_CHANNEL && p == ready).await? {
                return Ok(None);
            }
        }
    }

    async fn heartbeat(&self, task_id: Uuid, pod_id: &str) -> Result<bool> {
        crate::tasks::heartbeat(&self.pool, task_id, pod_id).await
    }

    async fn requeue(&self, task_id: Uuid, pod_id: &str) -> Result<bool> {
        crate::tasks::requeue(&self.pool, task_id, pod_id).await
    }

    async fn complete(&self, task_id: Uuid, pod_id: &str, result: Value) -> Result<()> {
        crate::tasks::complete(&self.pool, task_id, pod_id, result).await
    }

    async fn fail(&self, task_id: Uuid, pod_id: &str, error: String) -> Result<()> {
        crate::tasks::fail(&self.pool, task_id, pod_id, error).await
    }
}

pub struct PostgresWorkerPodClient {
    pool: PgPool,
}

impl PostgresWorkerPodClient {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl WorkerPodClient for PostgresWorkerPodClient {
    async fn register_alive(
        &self,
        pod_name: &str,
        project_id: Uuid,
    ) -> Result<()> {
        crate::worker_pod::register_alive(
            &self.pool,
            pod_name,
            project_id,
            crate::worker_pod::AliveTransition::FromSpawning,
        )
        .await
    }

    async fn heartbeat(&self, pod_name: &str, mem_pressure: f64) -> Result<Option<WorkerStanding>> {
        crate::worker_pod::heartbeat(&self.pool, pod_name, mem_pressure).await
    }

    async fn mark_done(&self, pod_name: &str) -> Result<()> {
        crate::worker_pod::mark_done(&self.pool, pod_name).await
    }

    async fn mark_done_if_idle(&self, pod_name: &str) -> Result<bool> {
        crate::worker_pod::mark_done_if_idle(&self.pool, pod_name).await
    }
}

/// Read surface for `infra_node`, the table the dispatcher writes as it
/// provisions infrastructure.
#[async_trait]
pub trait InfraReader: Send + Sync {
    /// Where one declared endpoint of an infra node answers. `None` when
    /// the node is not Running or declares no endpoint by that name.
    /// Backs `ctx.endpoint(name)` in node code.
    async fn endpoint_address(
        &self,
        project_id: Uuid,
        node_id: &str,
        endpoint_name: &str,
    ) -> Result<Option<weft_core::infra::EndpointAddress>>;
}

pub struct PostgresInfraReader {
    pool: PgPool,
    /// The front door's base URL, which a `TenantPublic` endpoint's
    /// stored path hangs off; `None` on an install that has none, and
    /// then no endpoint has a public URL.
    front_door: Option<String>,
}

impl PostgresInfraReader {
    pub fn new(pool: PgPool, front_door: Option<String>) -> Self {
        Self { pool, front_door }
    }
}

#[async_trait]
impl InfraReader for PostgresInfraReader {
    async fn endpoint_address(
        &self,
        project_id: Uuid,
        node_id: &str,
        endpoint_name: &str,
    ) -> Result<Option<weft_core::infra::EndpointAddress>> {
        let row = sqlx::query(
            "SELECT endpoints_json, public_paths_json FROM infra_node \
             WHERE project_id = $1 AND node_id = $2 AND status = 'running'",
        )
        .bind(project_id)
        .bind(node_id)
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        // A corrupt column fails loud rather than reading as "endpoint
        // not available", which would send the node chasing an endpoint
        // that is really there. Only a name the object does not hold is
        // the legitimate `None`.
        let endpoints: Value = row.try_get("endpoints_json")?;
        let Some(url) = entry_in(&endpoints, "endpoints_json", endpoint_name)? else {
            return Ok(None);
        };
        let public_paths: Value = row.try_get("public_paths_json")?;
        let public_url = match (&self.front_door, entry_in(&public_paths, "public_paths_json", endpoint_name)?) {
            (Some(base), Some(path)) => Some(weft_core::infra::tenant_public_url(base, &path)),
            _ => None,
        };
        Ok(Some(weft_core::infra::EndpointAddress { url, public_url }))
    }
}

/// One endpoint's string out of an `infra_node` name-to-string column
/// (`column` names it in the error).
fn entry_in(map: &Value, column: &str, endpoint_name: &str) -> Result<Option<String>> {
    let Some(map) = map.as_object() else {
        anyhow::bail!("infra_node.{column} is not an object: {map}");
    };
    match map.get(endpoint_name) {
        None => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        Some(other) => {
            anyhow::bail!("infra_node.{column} entry '{endpoint_name}' is not a string: {other}")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::entry_in;
    use serde_json::json;

    #[test]
    fn a_declared_endpoint_gives_its_url() {
        let endpoints = json!({ "http": "http://pg.ns.svc:5432" });
        assert_eq!(
            entry_in(&endpoints, "endpoints_json", "http").unwrap().as_deref(),
            Some("http://pg.ns.svc:5432"),
        );
    }

    #[test]
    fn an_undeclared_endpoint_is_none() {
        assert_eq!(entry_in(&json!({}), "endpoints_json", "http").unwrap(), None);
    }

    #[test]
    fn a_corrupt_endpoints_value_is_an_error() {
        assert!(entry_in(&json!(["http"]), "endpoints_json", "http").is_err());
        assert!(entry_in(&json!({ "http": 5432 }), "endpoints_json", "http").is_err());
    }
}
