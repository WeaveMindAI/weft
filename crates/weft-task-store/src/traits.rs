//! Worker-facing task and worker-pod surfaces.
//!
//! The shapes mirror the existing free functions in `tasks` and
//! `worker_pod`. Two implementations per trait:
//!   - `Postgres*Client` (this crate): direct DB. Used by the
//!     dispatcher and by the broker (after its scope check).
//!   - `Broker*Client` (in `weft-broker-client`): HTTP through the
//!     broker. Used by workers and listeners.
//!
//! `InfraReader` lives in the dedicated `weft-infra` crate (its
//! table is dispatcher-owned, not part of the task / worker_pod
//! surface).
//!
//! The engine takes both `TaskStoreClient` and `WorkerPodClient`;
//! the listener takes only `TaskStoreClient`.

use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;
use sqlx::postgres::PgPool;
use uuid::Uuid;

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

    async fn heartbeat(&self, pod_name: &str) -> Result<bool>;

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
        crate::worker_pod::register_alive(&self.pool, pod_name, project_id).await
    }

    async fn heartbeat(&self, pod_name: &str) -> Result<bool> {
        crate::worker_pod::heartbeat(&self.pool, pod_name).await
    }

    async fn mark_done(&self, pod_name: &str) -> Result<()> {
        crate::worker_pod::mark_done(&self.pool, pod_name).await
    }

    async fn mark_done_if_idle(&self, pod_name: &str) -> Result<bool> {
        crate::worker_pod::mark_done_if_idle(&self.pool, pod_name).await
    }
}
