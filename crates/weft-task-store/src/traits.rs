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

use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;
use sqlx::postgres::PgPool;
use sqlx::Row;
use uuid::Uuid;

use crate::worker_pod::WorkerStanding;
use crate::tasks::{
    ClaimFilter, DedupOutcome, NewTask, Task, TaskOutcome,
};

#[async_trait]
pub trait TaskStoreClient: Send + Sync {
    async fn enqueue_dedup(&self, spec: NewTask) -> Result<DedupOutcome>;

    async fn wait_for_terminal(
        &self,
        task_id: Uuid,
        timeout: Duration,
        poll_interval: Duration,
    ) -> Result<TaskOutcome>;

    /// Picker primitive: claim one pending or stale-claimed row that
    /// matches the filter. Used by both pickers.
    async fn claim_one(&self, pod_id: &str, filter: ClaimFilter) -> Result<Option<Task>>;

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
        project_id: &str,
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
}

impl PostgresTaskStoreClient {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl TaskStoreClient for PostgresTaskStoreClient {
    async fn enqueue_dedup(&self, spec: NewTask) -> Result<DedupOutcome> {
        crate::tasks::enqueue_dedup(&self.pool, spec).await
    }

    async fn wait_for_terminal(
        &self,
        task_id: Uuid,
        timeout: Duration,
        poll_interval: Duration,
    ) -> Result<TaskOutcome> {
        crate::tasks::wait_for_terminal(&self.pool, task_id, timeout, poll_interval).await
    }

    async fn claim_one(&self, pod_id: &str, filter: ClaimFilter) -> Result<Option<Task>> {
        crate::tasks::claim_one(&self.pool, pod_id, filter).await
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
        project_id: &str,
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
    /// The cluster-internal URL of one declared endpoint of an infra
    /// node. `None` when the node is not Running or declares no endpoint
    /// by that name. Backs `ctx.endpoint(name)` in node code.
    async fn endpoint_url(
        &self,
        project_id: &str,
        node_id: &str,
        endpoint_name: &str,
    ) -> Result<Option<String>>;
}

pub struct PostgresInfraReader {
    pool: PgPool,
}

impl PostgresInfraReader {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl InfraReader for PostgresInfraReader {
    async fn endpoint_url(
        &self,
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
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        // A corrupt `endpoints_json` fails loud rather than reading as
        // "endpoint not available", which would send the node chasing an
        // endpoint that is really there. Only a name the object does not
        // hold is the legitimate `None`.
        let endpoints: Value = row.try_get("endpoints_json")?;
        endpoint_in(&endpoints, endpoint_name)
    }
}

/// One endpoint's URL out of an `endpoints_json` object.
fn endpoint_in(endpoints: &Value, endpoint_name: &str) -> Result<Option<String>> {
    let Some(map) = endpoints.as_object() else {
        anyhow::bail!("infra_node.endpoints_json is not an object: {endpoints}");
    };
    match map.get(endpoint_name) {
        None => Ok(None),
        Some(Value::String(url)) => Ok(Some(url.clone())),
        Some(other) => {
            anyhow::bail!("infra_node endpoint '{endpoint_name}' is not a URL string: {other}")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::endpoint_in;
    use serde_json::json;

    #[test]
    fn a_declared_endpoint_gives_its_url() {
        let endpoints = json!({ "http": "http://pg.ns.svc:5432" });
        assert_eq!(
            endpoint_in(&endpoints, "http").unwrap().as_deref(),
            Some("http://pg.ns.svc:5432"),
        );
    }

    #[test]
    fn an_undeclared_endpoint_is_none() {
        assert_eq!(endpoint_in(&json!({}), "http").unwrap(), None);
    }

    #[test]
    fn a_corrupt_endpoints_value_is_an_error() {
        assert!(endpoint_in(&json!(["http"]), "http").is_err());
        assert!(endpoint_in(&json!({ "http": 5432 }), "http").is_err());
    }
}
