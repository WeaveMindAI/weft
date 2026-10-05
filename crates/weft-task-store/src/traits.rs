//! Worker-facing task and infra surfaces.
//!
//! The task shape mirrors the free functions in `tasks`; `InfraReader`
//! reads the dispatcher-written
//! `infra_node` table. Two implementations per trait:
//!   - `Postgres*` (this crate): direct DB. Used by the dispatcher and
//!     by the broker (after its scope check).
//!   - `Broker*` (in `weft-broker-client`): HTTP through the broker.
//!     Used by workers and listeners.
//! `InfraReader` is the exception: its only implementation is the
//! broker client, because the worker names a run and the broker
//! decides whose copy that run may reach. The broker reads the row
//! through `PostgresInfraReader::endpoint_address` after that check.
//!
//! The engine takes `TaskStoreClient` and `InfraReader`; the listener
//! takes only `TaskStoreClient`.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;
use sqlx::postgres::PgPool;
use sqlx::Row;
use uuid::Uuid;

use crate::pg_signal::PgSignalWatch;
use crate::tasks::{CancelAsked, ClaimedExecution, DedupOutcome, NewTask, Task, TaskOutcome};

#[async_trait]
pub trait TaskStoreClient: Send + Sync {
    async fn enqueue_dedup(&self, spec: NewTask) -> Result<DedupOutcome>;

    /// Wait for the task to finish, or for `timeout` to pass, and hand
    /// back its outcome either way. Ends the moment the task does (see
    /// [`crate::terminal`]), never on a polling tick.
    async fn wait_for_terminal(&self, task_id: Uuid, timeout: Duration) -> Result<TaskOutcome>;

    /// Claim the execute or resume task of the execution this worker was
    /// handed, with the execution's journal (see
    /// [`crate::tasks::claim_execution`]). `None` when there is nothing
    /// here to claim.
    async fn claim_execution(&self, replica: &str, project_id: Uuid, execution_id: &str) -> Result<Option<ClaimedExecution>>;

    async fn heartbeat(&self, task_id: Uuid, replica: &str) -> Result<bool>;

    /// Surrender a claim back to `pending` (no claimant), guarded on
    /// `claimed_by = replica` so a row already re-claimed elsewhere is
    /// never clobbered. Returns true when the requeue landed. See
    /// `tasks::requeue`.
    async fn requeue(&self, task_id: Uuid, replica: &str) -> Result<bool>;

    async fn complete(&self, task_id: Uuid, replica: &str, result: Value) -> Result<()>;

    async fn fail(&self, task_id: Uuid, replica: &str, error: String) -> Result<()>;

    /// The cancels asked for any of `execution_ids` of `project_id` (the
    /// executions the asking worker drives), each taken as it is
    /// answered; when there are none, hold for up to `wait` for one. An
    /// empty answer once `wait` passed.
    async fn wait_cancels(&self, project_id: Uuid, execution_ids: Vec<String>, wait: Duration) -> Result<Vec<CancelAsked>>;
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

    /// Claim one pending or stale-claimed dispatcher task, `None` when
    /// none is claimable (the dispatcher's picker is woken when one
    /// becomes so, see [`crate::executor::DISPATCHER_READY`]). Only the
    /// dispatcher claims these, from the database, so this is no part of
    /// [`TaskStoreClient`], which a worker reaches through the broker.
    pub async fn claim_dispatcher_task(&self, replica: &str) -> Result<Option<Task>> {
        crate::tasks::claim_one(&self.pool, replica).await
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

    async fn claim_execution(&self, replica: &str, project_id: Uuid, execution_id: &str) -> Result<Option<ClaimedExecution>> {
        crate::tasks::claim_execution(&self.pool, replica, project_id, execution_id).await
    }

    async fn heartbeat(&self, task_id: Uuid, replica: &str) -> Result<bool> {
        crate::tasks::heartbeat(&self.pool, task_id, replica).await
    }

    async fn requeue(&self, task_id: Uuid, replica: &str) -> Result<bool> {
        crate::tasks::requeue(&self.pool, task_id, replica).await
    }

    async fn complete(&self, task_id: Uuid, replica: &str, result: Value) -> Result<()> {
        crate::tasks::complete(&self.pool, task_id, replica, result).await
    }

    async fn fail(&self, task_id: Uuid, replica: &str, error: String) -> Result<()> {
        crate::tasks::fail(&self.pool, task_id, replica, error).await
    }

    async fn wait_cancels(&self, project_id: Uuid, execution_ids: Vec<String>, wait: Duration) -> Result<Vec<CancelAsked>> {
        let deadline = tokio::time::Instant::now() + wait;
        // Subscribed before the first look, so a cancel that lands
        // between an empty look and the wait still wakes it: a cancel
        // task is a worker task of the project, announced on its ready
        // payload like any other.
        let mut signals = self.signals.subscribe();
        let ready = crate::tasks::ready_payload(crate::tasks::TaskTarget::Worker, Some(project_id));
        loop {
            let taken = crate::tasks::take_cancels(&self.pool, project_id, &execution_ids).await?;
            if !taken.is_empty() {
                return Ok(taken);
            }
            if !signals.woken_before(deadline, |c, p| c == crate::tasks::TASK_READY_CHANNEL && p == ready).await? {
                return Ok(Vec::new());
            }
        }
    }
}

/// Read surface for `infra_node`, the table the dispatcher writes as it
/// provisions infrastructure.
#[async_trait]
pub trait InfraReader: Send + Sync {
    /// Where the endpoint `infra` names answers, for the run
    /// `execution_id`. `None` when the node is not Running or declares no
    /// endpoint by that name. Backs `ctx.endpoint(name)` and
    /// `ctx.endpoint_of(&handle)` in node code. The project is the
    /// broker's to resolve from the run, and a handle naming an instance
    /// other than the run's is refused there, so a run can never reach
    /// another project's or another instance's copy. `run_instance` is
    /// the run's instance as the worker read it from the run's journal:
    /// the broker reads it again off the run's row and goes by that; the
    /// worker keys what it keeps by it (`weft_engine`'s `held`).
    async fn endpoint_address(
        &self,
        execution_id: weft_core::ExecutionId,
        run_instance: Option<&weft_core::instance::InstanceId>,
        infra: &weft_core::infra::InfraHandle,
    ) -> Result<Option<weft_core::infra::EndpointAddress>>;
}

pub struct PostgresInfraReader {
    pool: PgPool,
    /// The install's public base URL, which a public endpoint's stored
    /// path hangs off; `None` on an install that has none, and then no
    /// endpoint has a public URL.
    front_door: Option<String>,
}

impl PostgresInfraReader {
    pub fn new(pool: PgPool, front_door: Option<String>) -> Self {
        Self { pool, front_door }
    }
}

impl PostgresInfraReader {
    /// [`InfraReader::endpoint_address`] once the broker has resolved
    /// the run: `instance` is which copy (`None` for a shared node).
    pub async fn endpoint_address(
        &self,
        project_id: Uuid,
        node_id: &str,
        instance: Option<&weft_core::instance::InstanceId>,
        endpoint_name: &str,
    ) -> Result<Option<weft_core::infra::EndpointAddress>> {
        let row = sqlx::query(
            "SELECT endpoints_json, public_paths_json FROM infra_node \
             WHERE project_id = $1 AND node_id = $2 AND instance_id IS NOT DISTINCT FROM $3 \
               AND status = 'running'",
        )
        .bind(project_id)
        .bind(node_id)
        .bind(instance.map(|m| m.as_str()))
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
            (Some(base), Some(path)) => Some(weft_core::infra::public_url(base, &path)),
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
