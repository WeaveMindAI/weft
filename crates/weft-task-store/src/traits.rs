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
//! through `PostgresInfraReader::reach` after that check.
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
    /// `run_id`. `None` when the copy does not answer now (it is not
    /// running or flaky, [`InfraNodeStatus::answers`](weft_core::infra::InfraNodeStatus::answers)) or declares no
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

/// One endpoint of a running infra node's copy, as stored: its address
/// (or the endpoint of another node it lives inside, and the path under
/// it), and, for a public endpoint, the path it hangs off the install's
/// internet address at (the caller joins the two, with
/// `weft_core::infra::public_url`, against the address as it is now).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredEndpoint {
    pub record: weft_core::infra::EndpointRecord,
    pub public_path: Option<String>,
}

/// Where an endpoint answers now ([`PostgresInfraReader::reach`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reached {
    /// Its address, and its front-door path when it is public.
    At { url: String, public_path: Option<String> },
    /// It lives inside another node's endpoint that cannot be reached
    /// now, or may not be: why, for a person.
    Unreachable(String),
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
    /// a shared node). `None` when the copy does not answer now
    /// ([`InfraNodeStatus::answers`](weft_core::infra::InfraNodeStatus::answers)) or declares no such endpoint.
    pub async fn endpoint(
        &self,
        project_id: Uuid,
        node_id: &str,
        instance: Option<&weft_core::instance::InstanceId>,
        endpoint_name: &str,
    ) -> Result<Option<StoredEndpoint>> {
        match self.copy(project_id, node_id, instance).await? {
            Some((status, row)) if status.answers() => stored_endpoint(&row, endpoint_name),
            _ => Ok(None),
        }
    }

    /// The status of the copy (`instance`) of the infra node `node_id`
    /// and its row, to read its endpoints from ([`stored_endpoint`]);
    /// `None` when it has no row. The status is read whatever it is
    /// (the rule is [`InfraNodeStatus::answers`](weft_core::infra::InfraNodeStatus::answers), in Rust, never a second spelling of it in
    /// SQL), so a lookup can say why a copy does not answer.
    async fn copy(
        &self,
        project_id: Uuid,
        node_id: &str,
        instance: Option<&weft_core::instance::InstanceId>,
    ) -> Result<Option<(weft_core::infra::InfraNodeStatus, crate::db::Row)>> {
        let row = self
            .pool
            .opt::<crate::db::Row>(
                "SELECT status, endpoints_json, public_paths_json FROM infra_node \
                 WHERE project_id = $1 AND node_id = $2 AND instance_id IS NOT DISTINCT FROM $3",
                &[&project_id, &node_id, &instance.map(|m| m.as_str())],
            )
            .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let status: String = row.try_get("status")?;
        let status = weft_core::infra::InfraNodeStatus::parse(&status).ok_or_else(|| {
            anyhow::anyhow!("infra_node.status='{status}' for project={project_id} node={node_id} is not a status this weft knows")
        })?;
        Ok(Some((status, row)))
    }

    /// Where the endpoint `endpoint_name` of the copy (`instance`) of the
    /// infra node `node_id` answers now: [`Self::endpoint`], read through
    /// [`Self::address_of`]. `None`: the copy does not answer now or
    /// declares no such endpoint.
    pub async fn reach(
        &self,
        project_id: Uuid,
        node_id: &str,
        instance: Option<&weft_core::instance::InstanceId>,
        endpoint_name: &str,
        allowed: impl FnOnce(&weft_core::infra::InfraHandle) -> Result<(), String>,
    ) -> Result<Option<Reached>> {
        let Some(StoredEndpoint { record, public_path }) = self.endpoint(project_id, node_id, instance, endpoint_name).await? else {
            return Ok(None);
        };
        Ok(Some(match self.address_of(project_id, record, allowed).await? {
            Ok(url) => Reached::At { url, public_path },
            Err(why) => Reached::Unreachable(why),
        }))
    }

    /// Where `record`, an endpoint of a copy of an infra node of
    /// `project_id`, answers now: its address, or for one that lives
    /// inside another node's endpoint, where that one answers at this
    /// moment (its copy's row read again, one hop,
    /// `EndpointRecord::address`), never an address kept from an earlier
    /// apply, so a node made again on another port moves every endpoint
    /// inside it along. The one lookup every caller goes through: the
    /// broker for a run's `ctx.endpoint`, the dispatcher for a copy's
    /// `/live` and actions.
    ///
    /// `allowed` is the caller's scope rule: it vets the inner endpoint
    /// before it is read, with the rules any handle the caller holds is
    /// held to (its project's program declares it, its copy is the
    /// caller's instance's), and its refusal is the answer. `Err` within
    /// says why the endpoint cannot be reached, for a person.
    pub async fn address_of(
        &self,
        project_id: Uuid,
        record: weft_core::infra::EndpointRecord,
        allowed: impl FnOnce(&weft_core::infra::InfraHandle) -> Result<(), String>,
    ) -> Result<std::result::Result<String, String>> {
        let Some(within) = record.within().cloned() else {
            return Ok(record.address(None));
        };
        if let Err(why) = allowed(&within) {
            return Ok(Err(why));
        }
        let inner = match self.copy(project_id, within.place(), within.instance()).await? {
            Some((status, row)) if status.answers() => match stored_endpoint(&row, within.endpoint())? {
                Some(inner) => Some(inner.record),
                None => return Ok(Err(format!("this endpoint runs inside {within}, which that node does not declare"))),
            },
            // No copy, or one that does not answer: `address` says so and
            // names `weft infra start`.
            _ => None,
        };
        Ok(record.address(inner))
    }
}

/// The endpoint `endpoint_name` of a copy's row ([`PostgresInfraReader`]'s
/// `copy`): `None` when it declares none by that name. A corrupt column
/// fails loud rather than reading as "endpoint not available", which
/// would send the node chasing an endpoint that is really there.
fn stored_endpoint(row: &crate::db::Row, endpoint_name: &str) -> Result<Option<StoredEndpoint>> {
    let endpoints = row.try_get::<_, JsonText<Value>>("endpoints_json")?.0;
    let Some(record) = entry_in(&endpoints, "endpoints_json", endpoint_name)? else {
        return Ok(None);
    };
    let public_paths = row.try_get::<_, JsonText<Value>>("public_paths_json")?.0;
    let public_path = entry_in(&public_paths, "public_paths_json", endpoint_name)?;
    Ok(Some(StoredEndpoint { record, public_path }))
}

/// One endpoint's entry out of an `infra_node` name-to-entry column
/// (`column` names it in the error): a string, or for `endpoints_json` an
/// `EndpointRecord`.
fn entry_in<T: serde::de::DeserializeOwned>(map: &Value, column: &str, endpoint_name: &str) -> Result<Option<T>> {
    let Some(map) = map.as_object() else {
        anyhow::bail!("infra_node.{column} is not an object: {map}");
    };
    match map.get(endpoint_name) {
        None => Ok(None),
        Some(entry) => serde_json::from_value(entry.clone())
            .map(Some)
            .map_err(|e| anyhow::anyhow!("infra_node.{column} entry '{endpoint_name}' does not read ({e}): {entry}")),
    }
}

#[cfg(test)]
mod tests {
    use super::entry_in;
    use serde_json::json;

    use weft_core::infra::EndpointRecord;

    #[test]
    fn a_declared_endpoint_gives_its_url() {
        let endpoints = json!({ "http": "http://pg.ns.svc:5432" });
        assert_eq!(
            entry_in::<EndpointRecord>(&endpoints, "endpoints_json", "http").unwrap(),
            Some(EndpointRecord::Url("http://pg.ns.svc:5432".into())),
        );
        let within = json!({ "api": { "within": { "__weft_infra__": { "place": "box", "endpoint": "api" } }, "path": "/apps/claude" } });
        let record = entry_in::<EndpointRecord>(&within, "endpoints_json", "api").unwrap().unwrap();
        assert_eq!(record.within().map(|h| h.place()), Some("box"));
    }

    #[test]
    fn an_undeclared_endpoint_is_none() {
        assert_eq!(entry_in::<EndpointRecord>(&json!({}), "endpoints_json", "http").unwrap(), None);
    }

    #[test]
    fn a_corrupt_endpoints_value_is_an_error() {
        assert!(entry_in::<EndpointRecord>(&json!(["http"]), "endpoints_json", "http").is_err());
        assert!(entry_in::<EndpointRecord>(&json!({ "http": 5432 }), "endpoints_json", "http").is_err());
        assert!(entry_in::<String>(&json!({ "http": 5432 }), "public_paths_json", "http").is_err());
    }
}
