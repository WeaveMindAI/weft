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
//! through `PostgresInfraReader::endpoint` after that check.
//!
//! The engine takes `TaskStoreClient` and `InfraReader`; the listener
//! takes only `TaskStoreClient`.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;
use uuid::Uuid;

use crate::db::{Client as _, JsonText, Pool};
use crate::pg_signal::PgSignalWatch;
use crate::tasks::{DedupOutcome, NewTask, TaskOutcome};

#[async_trait]
pub trait TaskStoreClient: Send + Sync {
    async fn enqueue_dedup(&self, spec: NewTask) -> Result<DedupOutcome>;

    /// Wait for the task to finish, or for `timeout` to pass, and hand
    /// back its outcome either way. Ends the moment the task does (see
    /// [`crate::terminal`]), never on a polling tick.
    async fn wait_for_terminal(&self, task_id: Uuid, timeout: Duration) -> Result<TaskOutcome>;
}

// ---------- Postgres impls ----------

pub struct PostgresTaskStoreClient {
    pool: Pool,
    /// The process's one `LISTEN` connection, which every wait here
    /// sleeps on.
    signals: Arc<PgSignalWatch>,
}

impl PostgresTaskStoreClient {
    /// `signals` must listen on the channels this client's waits sleep
    /// on, [`crate::tasks::TASK_READY_CHANNEL`] and
    /// [`crate::terminal::TERMINAL_CHANNEL`].
    pub fn new(pool: Pool, signals: Arc<PgSignalWatch>) -> Result<Self> {
        signals.require(crate::tasks::TASK_READY_CHANNEL)?;
        signals.require(crate::terminal::TERMINAL_CHANNEL)?;
        Ok(Self { pool, signals })
    }

    /// Claim one pending or stale-claimed dispatcher task, or say when the
    /// earliest claim out lapses when none is claimable (the dispatcher's
    /// picker is woken when one becomes so, see
    /// [`crate::executor::DISPATCHER_READY`]). Only the dispatcher claims
    /// these, from the database, so this is no part of
    /// [`TaskStoreClient`], which a worker reaches through the broker.
    pub async fn claim_dispatcher_task(&self, replica: &str) -> Result<crate::tasks::Claim> {
        crate::tasks::claim_next(&self.pool, replica).await
    }

    pub fn pool(&self) -> &Pool {
        &self.pool
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
}

/// Read surface for `infra_node`, the table the dispatcher writes as it
/// provisions infrastructure.
#[async_trait]
pub trait InfraReader: Send + Sync {
    /// Where the endpoint `infra` names answers, for the run
    /// `run_id`. `None` when the node is not Running or declares no
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
        run_id: weft_core::RunId,
        run_instance: Option<&weft_core::instance::InstanceId>,
        infra: &weft_core::infra::InfraHandle,
    ) -> Result<Option<weft_core::infra::EndpointAddress>>;

    /// What the copy of the infra node at `place` (spelled) saved for its
    /// baked outputs (`weft_core::infra::bake`), port to value, for the
    /// run `run_id` that runs it: the shared copy (`copy` `None`) or
    /// the run's own instance's. Empty when it saved nothing. The broker
    /// checks the copy against the run, like an endpoint's.
    async fn baked_outputs(
        &self,
        run_id: weft_core::RunId,
        run_instance: Option<&weft_core::instance::InstanceId>,
        place: &str,
        copy: Option<&weft_core::instance::InstanceId>,
    ) -> Result<std::collections::BTreeMap<String, serde_json::Value>>;
}

pub struct PostgresInfraReader {
    pool: Pool,
}

impl PostgresInfraReader {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }
}

/// One endpoint of a running infra node's copy, as stored: its address,
/// and, for a public endpoint, the path it hangs off the install's
/// internet address at (the caller joins the two, with
/// `weft_core::infra::public_url`, against the address as it is now).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredEndpoint {
    pub url: String,
    pub public_path: Option<String>,
}

impl PostgresInfraReader {
    /// [`InfraReader::baked_outputs`] once the broker has resolved the
    /// run: empty when the copy saved nothing or has no row.
    pub async fn baked_outputs(
        &self,
        project_id: Uuid,
        node_id: &str,
        instance: Option<&weft_core::instance::InstanceId>,
    ) -> Result<std::collections::BTreeMap<String, serde_json::Value>> {
        let saved: Option<JsonText<std::collections::BTreeMap<String, serde_json::Value>>> = self
            .pool
            .scalar_opt(
                "SELECT baked_json FROM infra_node WHERE project_id = $1 AND node_id = $2 AND instance_id IS NOT DISTINCT FROM $3",
                &[&project_id, &node_id, &instance.map(|i| i.as_str())],
            )
            .await?;
        Ok(saved.map(|saved| saved.0).unwrap_or_default())
    }

    /// The stored half of [`InfraReader::endpoint_address`], once the
    /// broker has resolved the run: `instance` is which copy (`None` for
    /// a shared node).
    pub async fn endpoint(
        &self,
        project_id: Uuid,
        node_id: &str,
        instance: Option<&weft_core::instance::InstanceId>,
        endpoint_name: &str,
    ) -> Result<Option<StoredEndpoint>> {
        let row = self
            .pool
            .opt::<crate::db::Row>(
                "SELECT endpoints_json, public_paths_json FROM infra_node \
                 WHERE project_id = $1 AND node_id = $2 AND instance_id IS NOT DISTINCT FROM $3 \
                   AND status = 'running'",
                &[&project_id, &node_id, &instance.map(|m| m.as_str())],
            )
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
        let public_path = entry_in(&public_paths, "public_paths_json", endpoint_name)?;
        Ok(Some(StoredEndpoint { url, public_path }))
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
