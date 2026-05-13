//! HTTP-backed implementations of the trait surfaces defined in
//! `weft-journal::traits` and `weft-task-store::traits`. Drop-in
//! replacements for the Postgres clients on the user-pod side
//! (worker, listener, sidecar).

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

    async fn post_with<Req: Serialize, Res: for<'de> serde::Deserialize<'de>>(
        &self,
        path: &str,
        body: &Req,
        client: &reqwest::Client,
    ) -> Result<Res> {
        let url = format!("{}{}", self.base_url.trim_end_matches('/'), path);
        let bearer = self.token.read().await.context("read SA token")?;
        let resp = client
            .post(&url)
            .bearer_auth(bearer)
            .json(body)
            .send()
            .await
            .with_context(|| format!("POST {path}"))?;
        if !resp.status().is_success() {
            let code = resp.status();
            let txt = resp.text().await.unwrap_or_default();
            anyhow::bail!("broker {path} returned {code}: {txt}");
        }
        let parsed: Res = resp.json().await.with_context(|| format!("parse {path}"))?;
        Ok(parsed)
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
        let req = JournalRecordRequest {
            event: event.clone(),
            pod_name: pod_name.map(str::to_string),
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
        let req = TaskEnqueueDedupRequest {
            spec: NewTaskWire::from_new_task(&spec),
        };
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
            filter: ClaimFilterWire::from_filter(&filter),
        };
        let resp: TaskClaimOneResponse = self.http.post("/v1/task/claim_one", &req).await?;
        Ok(resp.task.map(TaskWire::into_task))
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
    async fn sidecar_endpoint(&self, project_id: &str, node_id: &str) -> Result<Option<String>> {
        let req = InfraSidecarEndpointRequest {
            project_id: project_id.to_string(),
            node_id: node_id.to_string(),
        };
        let resp: InfraSidecarEndpointResponse =
            self.http.post("/v1/infra/sidecar_endpoint", &req).await?;
        Ok(resp.endpoint_url)
    }
}
