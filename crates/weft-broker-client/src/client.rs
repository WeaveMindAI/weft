//! HTTP-backed implementations of the trait surfaces defined in
//! `weft-journal::traits` and `weft-task-store::traits`. Drop-in
//! replacements for the Postgres clients on the user-pod side
//! (worker, listener, infra).

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;

use weft_core::Color;
use weft_infra::InfraReader;
use weft_journal::{ExecEvent, JournalClient};
use weft_task_store::tasks::{
    ClaimFilter, DedupOutcome, NewTask, Task, TaskOutcome,
};
use weft_task_store::{TaskStoreClient, WorkerPodClient};

use crate::protocol::*;
use crate::token::TokenSource;

#[derive(Clone)]
struct HttpCore {
    /// Default-timeout client for short-call endpoints. Long-poll
    /// callers (`wait_for_terminal`) build a per-call client via
    /// `with_timeout` instead so the HTTP deadline doesn't fire
    /// before the broker's own polling deadline.
    client: reqwest::Client,
    base_url: String,
    token: TokenSource,
}

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

impl HttpCore {
    pub fn new(base_url: String, token: TokenSource) -> Self {
        let client = reqwest::Client::builder()
            .timeout(DEFAULT_TIMEOUT)
            .build()
            .expect("reqwest client builds");
        Self {
            client,
            base_url,
            token,
        }
    }

    async fn post<Req: Serialize, Res: for<'de> serde::Deserialize<'de>>(
        &self,
        path: &str,
        body: &Req,
    ) -> Result<Res> {
        self.post_with(path, body, &self.client).await
    }

    /// Long-poll variant: caller supplies the broker-side timeout
    /// budget; the HTTP client gets `budget + buffer` so reqwest
    /// can't abort before the broker's own deadline fires.
    async fn post_with_timeout<Req: Serialize, Res: for<'de> serde::Deserialize<'de>>(
        &self,
        path: &str,
        body: &Req,
        budget: Duration,
    ) -> Result<Res> {
        let http_budget = budget + Duration::from_secs(5);
        let client = reqwest::Client::builder()
            .timeout(http_budget)
            .build()
            .expect("reqwest client builds");
        self.post_with(path, body, &client).await
    }

    /// Single POST core: build the request (url join, bearer token,
    /// JSON body), send, hand back the raw response. Status
    /// interpretation lives in the thin wrappers (`post_with` =
    /// 2xx-or-error, `post_or_404` = 404 is "no row", `post_or_raced`
    /// = 410 is `WriteOutcome::Raced`) so the build/send body exists
    /// exactly once.
    async fn post_raw<Req: Serialize>(
        &self,
        path: &str,
        body: &Req,
        client: &reqwest::Client,
    ) -> Result<reqwest::Response> {
        let url = format!("{}{}", self.base_url.trim_end_matches('/'), path);
        let bearer = self.token.read().await.context("read SA token")?;
        client
            .post(&url)
            .bearer_auth(bearer)
            .json(body)
            .send()
            .await
            .with_context(|| format!("POST {path}"))
    }

    /// Shared 2xx gate + JSON parse for the status-interpreting
    /// wrappers.
    async fn parse_success<Res: for<'de> serde::Deserialize<'de>>(
        resp: reqwest::Response,
        path: &str,
    ) -> Result<Res> {
        if !resp.status().is_success() {
            let code = resp.status();
            let txt = resp.text().await.unwrap_or_default();
            anyhow::bail!("broker {path} returned {code}: {txt}");
        }
        resp.json().await.with_context(|| format!("parse {path}"))
    }

    async fn post_with<Req: Serialize, Res: for<'de> serde::Deserialize<'de>>(
        &self,
        path: &str,
        body: &Req,
        client: &reqwest::Client,
    ) -> Result<Res> {
        let resp = self.post_raw(path, body, client).await?;
        Self::parse_success(resp, path).await
    }

    /// Variant of `post` for content-addressed reads where 404 means
    /// "no row exists for this key" (a real "not found", NOT a race).
    /// Returns `Ok(None)` on 404 and `Ok(Some(_))` on 2xx; any other
    /// non-2xx is an error. Distinct from `post_or_raced` because a
    /// content-addressed read CANNOT race: the row either exists
    /// under the requested key or it doesn't.
    pub async fn post_or_404<Req: Serialize, Res: for<'de> serde::Deserialize<'de>>(
        &self,
        path: &str,
        body: &Req,
    ) -> Result<Option<Res>> {
        let resp = self.post_raw(path, body, &self.client).await?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        Ok(Some(Self::parse_success(resp, path).await?))
    }

    /// Variant of `post` that converts HTTP 410 Gone into
    /// `WriteOutcome::Raced`. Used by lifecycle write paths where
    /// the broker returns 410 when the row was already-completed
    /// or reclaimed by a sibling pod (lease takeover, remove_node
    /// cascade). Lets callers distinguish "this raced, no-op
    /// gracefully" from "real failure, bubble up."
    pub async fn post_or_raced<Req: Serialize, Res: for<'de> serde::Deserialize<'de>>(
        &self,
        path: &str,
        body: &Req,
    ) -> Result<WriteOutcome<Res>> {
        let resp = self.post_raw(path, body, &self.client).await?;
        if resp.status() == reqwest::StatusCode::GONE {
            return Ok(WriteOutcome::Raced);
        }
        Ok(WriteOutcome::Applied(Self::parse_success(resp, path).await?))
    }
}

/// Outcome of a lifecycle-bound broker write. `Applied(_)` means
/// the write landed and the caller can rely on its effect.
/// `Raced` means the broker returned 410 Gone: the row was
/// already completed, the lease was reassigned to a sibling pod,
/// or the underlying object (infra_node) was removed mid-flight.
/// Callers should treat `Raced` as "someone else handled it" and
/// move on (log + return, don't bubble as error).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteOutcome<T> {
    Applied(T),
    Raced,
}

impl<T> WriteOutcome<T> {
    pub fn is_raced(&self) -> bool {
        matches!(self, WriteOutcome::Raced)
    }
}

// ---------- Journal ----------

pub struct BrokerJournalClient {
    http: HttpCore,
}

impl BrokerJournalClient {
    pub fn new(base_url: String, token: TokenSource) -> Arc<Self> {
        Arc::new(Self {
            http: HttpCore::new(base_url, token),
        })
    }
}

#[async_trait]
impl JournalClient for BrokerJournalClient {
    async fn record_event(
        &self,
        event: &ExecEvent,
        pod_name: Option<&str>,
    ) -> Result<()> {
        // The broker journal path is worker-only: every write is
        // fenced by the writer's pod. A `None` here is a contract
        // violation (only the dispatcher's in-process writer is
        // pod-less, and it never goes through the broker), so fail
        // loud rather than send a pod-less write the broker rejects.
        let pod_name = pod_name.ok_or_else(|| {
            anyhow::anyhow!("broker journal write requires a pod_name (worker-only path)")
        })?;
        let req = JournalRecordRequest {
            event: event.clone(),
            pod_name: pod_name.to_string(),
        };
        let _: JournalRecordResponse = self.http.post("/v1/journal/record", &req).await?;
        Ok(())
    }

    async fn events_for_color(&self, color: Color) -> Result<Vec<ExecEvent>> {
        let req = JournalFetchRequest {
            color: color.to_string(),
        };
        let resp: JournalFetchResponse = self.http.post("/v1/journal/fetch", &req).await?;
        Ok(resp.events)
    }

    async fn has_terminal_event(&self, color: Color) -> Result<bool> {
        let req = JournalHasTerminalRequest {
            color: color.to_string(),
        };
        let resp: JournalHasTerminalResponse =
            self.http.post("/v1/journal/has_terminal", &req).await?;
        Ok(resp.terminal)
    }
}

// ---------- TaskStore ----------

pub struct BrokerTaskStoreClient {
    http: HttpCore,
}

impl BrokerTaskStoreClient {
    pub fn new(base_url: String, token: TokenSource) -> Arc<Self> {
        Arc::new(Self {
            http: HttpCore::new(base_url, token),
        })
    }
}

#[async_trait]
impl TaskStoreClient for BrokerTaskStoreClient {
    async fn enqueue_dedup(&self, spec: NewTask) -> Result<DedupOutcome> {
        let req = TaskEnqueueDedupRequest { spec };
        let resp: TaskEnqueueDedupResponse =
            self.http.post("/v1/task/enqueue_dedup", &req).await?;
        Ok(if resp.inserted {
            DedupOutcome::Inserted(resp.id)
        } else {
            DedupOutcome::AlreadyLive(resp.id)
        })
    }

    async fn wait_for_terminal(
        &self,
        task_id: Uuid,
        timeout: Duration,
        poll_interval: Duration,
    ) -> Result<TaskOutcome> {
        let req = TaskWaitTerminalRequest::new(task_id, timeout, poll_interval);
        // The broker-side wait can run for the full `timeout` budget,
        // so the HTTP layer must not abort sooner. Per-call client
        // gets `timeout + 5s` of grace.
        let resp: TaskWaitTerminalResponse = self
            .http
            .post_with_timeout("/v1/task/wait_terminal", &req, timeout)
            .await?;
        Ok(resp.into_outcome())
    }

    async fn claim_one(&self, pod_id: &str, filter: ClaimFilter) -> Result<Option<Task>> {
        let req = TaskClaimOneRequest {
            pod_id: pod_id.to_string(),
            filter,
        };
        let resp: TaskClaimOneResponse = self.http.post("/v1/task/claim_one", &req).await?;
        Ok(resp.task)
    }

    async fn heartbeat(&self, task_id: Uuid, pod_id: &str) -> Result<bool> {
        let req = TaskHeartbeatRequest {
            task_id,
            pod_id: pod_id.to_string(),
        };
        let resp: TaskHeartbeatResponse = self.http.post("/v1/task/heartbeat", &req).await?;
        Ok(resp.renewed)
    }

    async fn complete(&self, task_id: Uuid, pod_id: &str, result: Value) -> Result<()> {
        let req = TaskCompleteRequest {
            task_id,
            pod_id: pod_id.to_string(),
            result,
        };
        let _: TaskCompleteResponse = self.http.post("/v1/task/complete", &req).await?;
        Ok(())
    }

    async fn fail(&self, task_id: Uuid, pod_id: &str, error: String) -> Result<()> {
        let req = TaskFailRequest {
            task_id,
            pod_id: pod_id.to_string(),
            error,
        };
        let _: TaskFailResponse = self.http.post("/v1/task/fail", &req).await?;
        Ok(())
    }
}

// ---------- WorkerPod ----------

pub struct BrokerWorkerPodClient {
    http: HttpCore,
}

impl BrokerWorkerPodClient {
    pub fn new(base_url: String, token: TokenSource) -> Arc<Self> {
        Arc::new(Self {
            http: HttpCore::new(base_url, token),
        })
    }
}

#[async_trait]
impl WorkerPodClient for BrokerWorkerPodClient {
    async fn register_alive(
        &self,
        pod_name: &str,
        project_id: &str,
    ) -> Result<()> {
        let req = WorkerPodRegisterAliveRequest {
            pod_name: pod_name.to_string(),
            project_id: project_id.to_string(),
        };
        let _: WorkerPodRegisterAliveResponse = self
            .http
            .post("/v1/worker_pod/register_alive", &req)
            .await?;
        Ok(())
    }

    async fn heartbeat(&self, pod_name: &str) -> Result<bool> {
        let req = WorkerPodHeartbeatRequest {
            pod_name: pod_name.to_string(),
        };
        let resp: WorkerPodHeartbeatResponse =
            self.http.post("/v1/worker_pod/heartbeat", &req).await?;
        Ok(resp.renewed)
    }

    async fn mark_done(&self, pod_name: &str) -> Result<()> {
        let req = WorkerPodMarkDoneRequest {
            pod_name: pod_name.to_string(),
        };
        let _: WorkerPodMarkDoneResponse =
            self.http.post("/v1/worker_pod/mark_done", &req).await?;
        Ok(())
    }

    async fn mark_done_if_idle(&self, pod_name: &str) -> Result<bool> {
        let req = WorkerPodMarkDoneIfIdleRequest {
            pod_name: pod_name.to_string(),
        };
        let resp: WorkerPodMarkDoneIfIdleResponse =
            self.http.post("/v1/worker_pod/mark_done_if_idle", &req).await?;
        Ok(resp.exited)
    }
}

// ---------- Signals (listener-only rehydrate) ----------

pub struct BrokerSignalClient {
    http: HttpCore,
}

impl BrokerSignalClient {
    pub fn new(base_url: String, token: TokenSource) -> Arc<Self> {
        Arc::new(Self {
            http: HttpCore::new(base_url, token),
        })
    }

    pub async fn list_for_tenant(
        &self,
        tenant_id: &str,
    ) -> Result<Vec<SignalRowWire>> {
        let req = SignalListForTenantRequest {
            tenant_id: tenant_id.to_string(),
        };
        let resp: SignalListForTenantResponse =
            self.http.post("/v1/signal/list_for_tenant", &req).await?;
        Ok(resp.rows)
    }
}

// ---------- Infra ----------

pub struct BrokerInfraClient {
    http: HttpCore,
}

impl BrokerInfraClient {
    pub fn new(base_url: String, token: TokenSource) -> Arc<Self> {
        Arc::new(Self {
            http: HttpCore::new(base_url, token),
        })
    }
}

#[async_trait]
impl InfraReader for BrokerInfraClient {
    async fn endpoint_url(
        &self,
        project_id: &str,
        node_id: &str,
        endpoint_name: &str,
    ) -> Result<Option<String>> {
        let req = InfraEndpointUrlRequest {
            project_id: project_id.to_string(),
            node_id: node_id.to_string(),
            endpoint_name: endpoint_name.to_string(),
        };
        let resp: InfraEndpointUrlResponse =
            self.http.post("/v1/infra/endpoint_url", &req).await?;
        Ok(resp.endpoint_url)
    }
}

// ---------- Project (worker fetches own definition) ----------

/// Worker-side client for `/v1/project/fetch_definition`. Used at
/// execution claim time to pull the runtime `ProjectDefinition`
/// keyed by `(project_id, expected_hash)`. The lookup is content-
/// addressed against the append-only `project_definition` history
/// table: the row either exists for the requested hash (200), or it
/// does not (404). There is no "raced" case for this endpoint
/// because the read does not contend with any writer.
pub struct BrokerProjectClient {
    http: HttpCore,
}

impl BrokerProjectClient {
    pub fn new(base_url: String, token: TokenSource) -> Arc<Self> {
        Arc::new(Self {
            http: HttpCore::new(base_url, token),
        })
    }

    /// Fetch the project's `ProjectDefinition` JSON keyed by hash.
    /// Returns `Some(resp)` on 200, `None` on 404 (no row for the
    /// requested key, a real "not found"), `Err` on every other
    /// failure (5xx, IO, parse). Callers turn `None` into a loud
    /// error: the dispatcher should never enqueue a task with a
    /// hash that has no history row, so a 404 here is a real bug
    /// upstream, not a recoverable race.
    pub async fn fetch_definition(
        &self,
        project_id: &str,
        expected_hash: &str,
    ) -> Result<Option<ProjectFetchDefinitionResponse>> {
        let req = ProjectFetchDefinitionRequest {
            project_id: project_id.to_string(),
            expected_hash: expected_hash.to_string(),
        };
        self.http
            .post_or_404("/v1/project/fetch_definition", &req)
            .await
    }
}

// ---------- Supervisor ----------

/// Broker client for the infra-supervisor. Talks to the
/// `/v1/supervisor/*` endpoints under the InfraSupervisor role.
pub struct BrokerSupervisorClient {
    http: HttpCore,
}

impl BrokerSupervisorClient {
    pub fn new(base_url: String, token: TokenSource) -> Arc<Self> {
        Arc::new(Self {
            http: HttpCore::new(base_url, token),
        })
    }

    pub async fn projects_for_tenant(
        &self,
        tenant_id: &str,
    ) -> Result<Vec<SupervisorProject>> {
        let req = SupervisorProjectsForTenantRequest {
            tenant_id: tenant_id.to_string(),
        };
        let resp: SupervisorProjectsForTenantResponse = self
            .http
            .post("/v1/supervisor/projects_for_tenant", &req)
            .await?;
        Ok(resp.projects)
    }

    pub async fn infra_nodes(&self, project_id: &str) -> Result<Vec<SupervisorInfraNode>> {
        let req = SupervisorInfraNodesRequest {
            project_id: project_id.to_string(),
        };
        let resp: SupervisorInfraNodesResponse =
            self.http.post("/v1/supervisor/infra_nodes", &req).await?;
        Ok(resp.nodes)
    }

    pub async fn health_protocols(
        &self,
        project_id: &str,
    ) -> Result<Option<serde_json::Value>> {
        let req = SupervisorHealthProtocolsRequest {
            project_id: project_id.to_string(),
        };
        let resp: SupervisorHealthProtocolsResponse = self
            .http
            .post("/v1/supervisor/health_protocols", &req)
            .await?;
        Ok(resp.protocols)
    }

    pub async fn claim_command(
        &self,
        tenant_id: &str,
        claimer_pod: &str,
    ) -> Result<Option<SupervisorCommandRow>> {
        let req = SupervisorClaimCommandRequest {
            tenant_id: tenant_id.to_string(),
            claimer_pod: claimer_pod.to_string(),
        };
        let resp: SupervisorClaimCommandResponse =
            self.http.post("/v1/supervisor/claim_command", &req).await?;
        Ok(resp.command)
    }

    pub async fn event_record(
        &self,
        project_id: &str,
        node_id: Option<&str>,
        event: crate::protocol::InfraEvent,
    ) -> Result<i64> {
        let (kind, payload) = event.into_record();
        let req = SupervisorEventRecordRequest {
            project_id: project_id.to_string(),
            node_id: node_id.map(|s| s.to_string()),
            kind,
            payload,
        };
        let resp: SupervisorEventRecordResponse =
            self.http.post("/v1/supervisor/event_record", &req).await?;
        Ok(resp.id)
    }

    pub async fn set_status(
        &self,
        command_id: Option<i64>,
        project_id: &str,
        node_id: &str,
        unit: Option<&str>,
        status: crate::protocol::InfraNodeStatus,
        failure_stage: Option<crate::protocol::FailureStage>,
        failure_message: Option<&str>,
    ) -> Result<WriteOutcome<SupervisorSetStatusResponse>> {
        let req = SupervisorSetStatusRequest {
            command_id,
            project_id: project_id.to_string(),
            node_id: node_id.to_string(),
            unit: unit.map(|s| s.to_string()),
            status,
            failure_stage,
            failure_message: failure_message.map(|s| s.to_string()),
        };
        self.http
            .post_or_raced::<_, SupervisorSetStatusResponse>("/v1/supervisor/set_status", &req)
            .await
    }

    pub async fn remove_node(&self, project_id: &str, node_id: &str) -> Result<bool> {
        let req = SupervisorRemoveNodeRequest {
            project_id: project_id.to_string(),
            node_id: node_id.to_string(),
        };
        let resp: SupervisorRemoveNodeResponse =
            self.http.post("/v1/supervisor/remove_node", &req).await?;
        Ok(resp.removed)
    }

    pub async fn command_complete(
        &self,
        command_id: i64,
        error: Option<&str>,
    ) -> Result<WriteOutcome<SupervisorCommandCompleteResponse>> {
        let req = SupervisorCommandCompleteRequest {
            command_id,
            error: error.map(|s| s.to_string()),
        };
        self.http
            .post_or_raced::<_, SupervisorCommandCompleteResponse>(
                "/v1/supervisor/command_complete",
                &req,
            )
            .await
    }

    pub async fn running_count(&self, project_id: &str) -> Result<i64> {
        let req = SupervisorRunningCountRequest {
            project_id: project_id.to_string(),
        };
        let resp: SupervisorRunningCountResponse =
            self.http.post("/v1/supervisor/running_count", &req).await?;
        Ok(resp.running_count)
    }

    /// True if a user infra action (any uncompleted
    /// infra_lifecycle_command: apply / stop / terminate) is in flight
    /// for the project. The health loop stands down for the project
    /// while this holds, so it never fights the action.
    pub async fn infra_command_in_flight(&self, project_id: &str) -> Result<bool> {
        let req = SupervisorInfraCommandInFlightRequest {
            project_id: project_id.to_string(),
        };
        let resp: SupervisorInfraCommandInFlightResponse = self
            .http
            .post("/v1/supervisor/infra_command_in_flight", &req)
            .await?;
        Ok(resp.in_flight)
    }

    pub async fn trigger_deps(
        &self,
        project_id: &str,
    ) -> Result<Vec<SupervisorTriggerDep>> {
        let req = SupervisorTriggerDepsRequest {
            project_id: project_id.to_string(),
        };
        let resp: SupervisorTriggerDepsResponse =
            self.http.post("/v1/supervisor/trigger_deps", &req).await?;
        Ok(resp.deps)
    }

    pub async fn set_applied(
        &self,
        command_id: i64,
        project_id: &str,
        node_id: &str,
        instance_id: &str,
        applied_spec_hash: &str,
        endpoints: std::collections::BTreeMap<String, String>,
        namespace: &str,
        preserve_pvcs: Vec<String>,
        units: std::collections::BTreeMap<String, crate::protocol::UnitRuntime>,
    ) -> Result<WriteOutcome<SupervisorSetAppliedResponse>> {
        let req = SupervisorSetAppliedRequest {
            command_id,
            project_id: project_id.to_string(),
            node_id: node_id.to_string(),
            instance_id: instance_id.to_string(),
            applied_spec_hash: applied_spec_hash.to_string(),
            endpoints,
            namespace: namespace.to_string(),
            preserve_pvcs,
            units,
        };
        self.http
            .post_or_raced::<_, SupervisorSetAppliedResponse>("/v1/supervisor/set_applied", &req)
            .await
    }

    /// Pre-apply commitment: writes the infra_node row at
    /// Provisioning status with the locked-in instance_id +
    /// namespace + preserve_pvcs. A subsequent apply failure
    /// leaves a visible row the user can Terminate (delete_by_label
    /// keyed on instance_id + preserve_pvcs). Apply success flips
    /// to Running via `set_applied`.
    pub async fn set_provisioning(
        &self,
        command_id: i64,
        project_id: &str,
        node_id: &str,
        instance_id: &str,
        namespace: &str,
        preserve_pvcs: Vec<String>,
        units: std::collections::BTreeMap<String, crate::protocol::UnitRuntime>,
    ) -> Result<WriteOutcome<SupervisorSetProvisioningResponse>> {
        let req = SupervisorSetProvisioningRequest {
            command_id,
            project_id: project_id.to_string(),
            node_id: node_id.to_string(),
            instance_id: instance_id.to_string(),
            namespace: namespace.to_string(),
            preserve_pvcs,
            units,
        };
        self.http
            .post_or_raced::<_, SupervisorSetProvisioningResponse>(
                "/v1/supervisor/set_provisioning",
                &req,
            )
            .await
    }

    pub async fn project_image_tags(
        &self,
        project_id: &str,
        node_id: &str,
    ) -> Result<std::collections::HashMap<String, String>> {
        let req = SupervisorProjectImageTagsRequest {
            project_id: project_id.to_string(),
            node_id: node_id.to_string(),
        };
        let resp: SupervisorProjectImageTagsResponse = self
            .http
            .post("/v1/supervisor/project_image_tags", &req)
            .await?;
        Ok(resp.tags)
    }

    /// Enqueue a dispatcher-targeted lifecycle command
    /// (`Deactivate(...)` | `Reactivate`). Used by HealthProtocol
    /// action dispatch in the supervisor. Verb-and-payload are
    /// typed via `LifecycleSpec`, so the supervisor cannot
    /// accidentally enqueue a supervisor-owned verb.
    pub async fn enqueue_lifecycle(
        &self,
        project_id: &str,
        spec: crate::protocol::LifecycleSpec,
    ) -> Result<i64> {
        let req = SupervisorEnqueueLifecycleRequest {
            project_id: project_id.to_string(),
            spec,
        };
        let resp: SupervisorEnqueueLifecycleResponse = self
            .http
            .post("/v1/supervisor/enqueue_lifecycle", &req)
            .await?;
        Ok(resp.command_id)
    }
}

/// Worker-callable broker client. Used by the engine to hand off an
/// InfraSpec to the supervisor and wait for it to settle. The worker
/// never reads prior applied state, never compiles, never decides
/// skip/fresh/replace; the supervisor owns the whole apply pipeline.
pub struct BrokerInfraStateClient {
    http: HttpCore,
}

impl BrokerInfraStateClient {
    pub fn new(base_url: String, token: TokenSource) -> Arc<Self> {
        Arc::new(Self {
            http: HttpCore::new(base_url, token),
        })
    }

    pub async fn enqueue_apply(
        &self,
        project_id: &str,
        node_id: &str,
        spec_json: serde_json::Value,
    ) -> Result<i64> {
        let req = InfraEnqueueApplyRequest {
            project_id: project_id.to_string(),
            node_id: node_id.to_string(),
            spec_json,
        };
        let resp: InfraEnqueueApplyResponse =
            self.http.post("/v1/infra/enqueue_apply", &req).await?;
        Ok(resp.command_id)
    }

    pub async fn wait_apply(
        &self,
        project_id: &str,
        command_id: i64,
    ) -> Result<InfraWaitApplyResponse> {
        let req = InfraWaitApplyRequest {
            project_id: project_id.to_string(),
            command_id,
        };
        let resp: InfraWaitApplyResponse =
            self.http.post("/v1/infra/wait_apply", &req).await?;
        Ok(resp)
    }
}
