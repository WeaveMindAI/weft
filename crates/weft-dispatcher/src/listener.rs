//! Per-tenant listener lifecycle. The listener is a long-lived
//! kind-aware processor: one Pod (or subprocess in dev) per tenant,
//! shared by every project belonging to that tenant.
//!
//! ## Two lock layers
//!
//! Every operation on a listener (register, unregister, fire,
//! display, action) goes through `ListenerPool::with_listener`,
//! which guards the operation with two distinct PG advisory-lock
//! mechanisms:
//!
//! 1. **Operation lock (SHARED)**: held for the duration of the
//!    caller's work. Multiple operations on the same tenant can
//!    coexist (SHARED). The reaper takes the same key in EXCLUSIVE
//!    mode, so as long as any operation holds SHARED, the reaper
//!    backs off. PG releases the lock automatically when the
//!    holding session disconnects, so a Pod crash mid-operation
//!    cannot leak a permanent block.
//!
//! 2. **State-transition lock (XACT)**: held for the brief window
//!    around row reads/writes that drive the four-state machine
//!    (Stopped → Starting → Alive → Stopping → Stopped). Released
//!    on transaction commit, never spans slow kubectl I/O.
//!
//! ## Why two locks
//!
//! The XACT lock alone can't keep the reaper out: the operation
//! does work AFTER the row write (POSTs the listener admin API),
//! and we'd have to hold the transaction open across the HTTP call
//! to a foreign service. Bad. The SHARED lock fences the whole
//! operation; the XACT lock only fences row mutations.
//!
//! ## Reaper side
//!
//! The listener reaper takes the operation key in EXCLUSIVE mode
//! via `pg_try_advisory_lock`: succeeds iff zero operations are
//! currently in flight. If it succeeds, it then runs the row's
//! Alive → Stopping → Stopped transitions and the kubectl delete
//! while still holding EXCLUSIVE. The signal table doubles as the
//! "is the listener semantically needed" check.
//!
//! ## Row-ownership lease
//!
//! `tenant_listener.owner_pod_id` + `leased_until_unix` is a
//! separate concern: which dispatcher Pod is currently authoritative
//! for state transitions on this row. A dead Pod's row gets adopted
//! by the next `with_listener` call on a sibling Pod (CleanupAbandoned
//! branch).

use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use dashmap::DashMap;
use serde_json::Value;
use sqlx::postgres::PgPool;
use sqlx::Postgres;
use tokio::process::{Child, Command};
use tokio::sync::Mutex;
use weft_core::primitive::SignalSpec;

use crate::tenant::TenantId;

/// Handle to a running listener. Just the URL: there is no bearer
/// auth between dispatcher and listener. The trust boundary is the
/// network (NetworkPolicy in k8s, loopback-only listen address in
/// subprocess dev), not a shared secret.
#[derive(Debug, Clone)]
pub struct ListenerHandle {
    pub admin_url: String,
}

#[async_trait]
pub trait ListenerBackend: Send + Sync {
    async fn spawn(&self, tenant: &TenantId, namespace: &str) -> Result<ListenerHandle>;
    async fn stop(&self, tenant: &TenantId, namespace: &str) -> Result<()>;
}

// =============================================================
// Backends
// =============================================================

/// Local-development backend: forks the `weft-listener` binary as a
/// child process. Pre-allocates an ephemeral port so the dispatcher
/// knows the URL before exec.
pub struct SubprocessListenerBackend {
    binary_path: PathBuf,
    children: Arc<DashMap<String, Arc<Mutex<Child>>>>,
}

impl SubprocessListenerBackend {
    pub fn new(binary_path: PathBuf) -> Self {
        Self {
            binary_path,
            children: Arc::new(DashMap::new()),
        }
    }
}

#[async_trait]
impl ListenerBackend for SubprocessListenerBackend {
    async fn spawn(&self, tenant: &TenantId, _namespace: &str) -> Result<ListenerHandle> {
        let port = pick_free_port()?;
        // Listener binds to `127.0.0.1` only: that's the auth boundary
        // in subprocess dev. Anyone with shell on the dev machine
        // already has dispatcher-equivalent access.
        let admin_url = format!("http://127.0.0.1:{port}");
        let broker_url = std::env::var("WEFT_BROKER_URL")
            .context("WEFT_BROKER_URL must be set for subprocess listener")?;
        let token_path = std::env::var("WEFT_BROKER_TOKEN_PATH")
            .context("WEFT_BROKER_TOKEN_PATH must be set for subprocess listener (point at a file with a valid SA token)")?;

        let mut cmd = Command::new(&self.binary_path);
        cmd.env("WEFT_LISTENER_TENANT_ID", tenant.as_str())
            .env("WEFT_LISTENER_PORT", port.to_string())
            .env("WEFT_BROKER_URL", broker_url)
            .env("WEFT_BROKER_TOKEN_PATH", token_path)
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .kill_on_drop(true);
        let child = cmd
            .spawn()
            .with_context(|| format!("spawn listener for tenant {tenant}"))?;
        self.children
            .insert(tenant.to_string(), Arc::new(Mutex::new(child)));

        wait_for_health(&admin_url).await?;
        Ok(ListenerHandle { admin_url })
    }

    async fn stop(&self, tenant: &TenantId, _namespace: &str) -> Result<()> {
        if let Some((_, child)) = self.children.remove(tenant.as_str()) {
            let mut c = child.lock().await;
            // `kill` failing means the child is already dead or the
            // OS refused; either way the subprocess is no longer
            // ours to manage. Log so a stuck process doesn't go
            // silent.
            if let Err(e) = c.kill().await {
                tracing::warn!(
                    target: "weft_dispatcher::listener",
                    tenant = tenant.as_str(),
                    error = %e,
                    "child kill failed (likely already exited)"
                );
            }
        }
        Ok(())
    }
}

/// k8s backend: applies a Deployment + Service in the tenant's
/// namespace and resolves the admin URL via cluster DNS. Listener
/// has no public surface; only the dispatcher reaches it.
pub struct K8sListenerBackend {
    listener_image: String,
    broker_url: String,
    kube: Arc<dyn weft_platform_traits::KubeClient>,
}

impl K8sListenerBackend {
    pub fn new(
        listener_image: String,
        broker_url: String,
        kube: Arc<dyn weft_platform_traits::KubeClient>,
    ) -> Self {
        Self {
            listener_image,
            broker_url,
            kube,
        }
    }
}

#[async_trait]
impl ListenerBackend for K8sListenerBackend {
    async fn spawn(&self, tenant: &TenantId, namespace: &str) -> Result<ListenerHandle> {
        let deploy_name = deploy_name_for_tenant(tenant.as_str());
        let admin_url = format!("http://{deploy_name}.{namespace}.svc.cluster.local:8080");

        let manifest = render_listener_manifest(
            &deploy_name,
            namespace,
            &self.listener_image,
            tenant.as_str(),
            &self.broker_url,
        );
        self.kube.apply_yaml(&manifest).await?;
        self.kube
            .wait_rollout_status(namespace, &deploy_name, 120)
            .await?;
        wait_for_health(&admin_url).await?;
        Ok(ListenerHandle { admin_url })
    }

    async fn stop(&self, tenant: &TenantId, namespace: &str) -> Result<()> {
        let deploy_name = deploy_name_for_tenant(tenant.as_str());
        // Service first (instant delete), then Deployment with
        // foreground cascade so ReplicaSet + Pods finish terminating
        // before we return; eliminates the "old Pod still holds the
        // Service Endpoint" race when a fresh spawn lands milliseconds
        // later. Routes through the shared `KubeClient` trait so the
        // reaper's `backend.stop failed during reap` branch can
        // actually observe failures (and tests can fake them).
        self.kube
            .delete_named(
                namespace,
                "service",
                &deploy_name,
                weft_platform_traits::DeleteOpts::wait(),
            )
            .await?;
        self.kube
            .delete_named(
                namespace,
                "deployment",
                &deploy_name,
                weft_platform_traits::DeleteOpts::wait_cascade(),
            )
            .await?;
        Ok(())
    }
}

fn deploy_name_for_tenant(tenant_id: &str) -> String {
    format!("listener-{}", short_id(tenant_id))
}

fn short_id(project_id: &str) -> String {
    project_id
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .take(8)
        .collect::<String>()
        .to_lowercase()
}

fn render_listener_manifest(
    name: &str,
    namespace: &str,
    image: &str,
    tenant_id: &str,
    broker_url: &str,
) -> String {
    // The pod-level isolation comes from the per-namespace
    // NetworkPolicies that ship with the namespace itself
    // (`listener-policy` selects `weft.dev/role=listener`). Here we
    // only render the Deployment + Service.
    //
    // Auth: the broker validates the projected SA token mounted at
    // /var/run/weft/sa/token. The audience claim is `weft-broker`.
    // The pod runs as `weft-listener-sa`, and the broker maps that
    // SA name to the listener role.
    format!(
        r#"apiVersion: apps/v1
kind: Deployment
metadata:
  name: {name}
  namespace: {namespace}
  labels:
    app: {name}
    weft.dev/role: listener
    weft.dev/tenant: "{tenant_id}"
spec:
  replicas: 1
  selector:
    matchLabels:
      app: {name}
  template:
    metadata:
      labels:
        app: {name}
        weft.dev/role: listener
        weft.dev/tenant: "{tenant_id}"
    spec:
      serviceAccountName: weft-listener-sa
      automountServiceAccountToken: false
      containers:
        - name: listener
          image: {image}
          imagePullPolicy: IfNotPresent
          ports:
            - containerPort: 8080
          env:
            - name: WEFT_LISTENER_TENANT_ID
              value: "{tenant_id}"
            - name: WEFT_LISTENER_PORT
              value: "8080"
            - name: WEFT_BROKER_URL
              value: "{broker_url}"
            - name: WEFT_BROKER_TOKEN_PATH
              value: "/var/run/weft/sa/token"
          volumeMounts:
            - name: weft-sa-token
              mountPath: /var/run/weft/sa
              readOnly: true
          readinessProbe:
            httpGet:
              path: /health
              port: 8080
            initialDelaySeconds: 1
            periodSeconds: 2
      volumes:
        - name: weft-sa-token
          projected:
            sources:
              - serviceAccountToken:
                  audience: weft-broker
                  expirationSeconds: 3600
                  path: token
---
apiVersion: v1
kind: Service
metadata:
  name: {name}
  namespace: {namespace}
spec:
  selector:
    app: {name}
  ports:
    - port: 8080
      targetPort: 8080
"#,
    )
}

fn pick_free_port() -> Result<u16> {
    let s = std::net::TcpListener::bind("127.0.0.1:0").context("bind ephemeral port")?;
    let port = s.local_addr()?.port();
    drop(s);
    Ok(port)
}

async fn wait_for_health(admin_url: &str) -> Result<()> {
    let client = reqwest::Client::new();
    let health = format!("{}/health", admin_url.trim_end_matches('/'));
    for _ in 0..50 {
        if client.get(&health).send().await.is_ok() {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    anyhow::bail!("listener at {admin_url} did not become healthy in time")
}

// =============================================================
// Listener admin client (HTTP helpers)
// =============================================================

/// Bail with `<route> returned <status>: <body>` if the response
/// wasn't 2xx; otherwise pass it through. The body decode in the
/// error path surfaces decode failures via
/// `unwrap_or_else(|e| format!("<body read failed: {e}>"))` so a
/// truncated TLS / network reset mid-response shows up in the
/// error message instead of an empty body.
async fn bail_unless_ok(resp: reqwest::Response, route: &str) -> Result<reqwest::Response> {
    if resp.status().is_success() {
        return Ok(resp);
    }
    let status = resp.status();
    // Body read is secondary detail; the primary error (status +
    // route) is already named. Surface a body-read failure as
    // part of the error message so a truncated TLS / network reset
    // mid-response is legible instead of looking like an empty body.
    let body = resp
        .text()
        .await
        .unwrap_or_else(|e| format!("<body read failed: {e}>"));
    anyhow::bail!("listener {route} returned {status}: {body}")
}

pub async fn register_signal(
    handle: &ListenerHandle,
    token: &str,
    spec: &SignalSpec,
    node_id: &str,
    is_resume: bool,
    color: Option<&str>,
) -> Result<(weft_core::primitive::SignalRouting, Value)> {
    let client = reqwest::Client::new();
    let url = format!("{}/register", handle.admin_url.trim_end_matches('/'));
    let resp = client
        .post(&url)
        .json(&serde_json::json!({
            "token": token,
            "spec": spec,
            "node_id": node_id,
            "is_resume": is_resume,
            "color": color,
        }))
        .send()
        .await?;
    let resp = bail_unless_ok(resp, "/register").await?;
    let body: weft_listener::protocol::RegisterResponse = resp.json().await?;
    Ok((body.routing, body.kind_state))
}

pub async fn display_signal(handle: &ListenerHandle, token: &str) -> Result<Value> {
    let client = reqwest::Client::new();
    let url = format!("{}/display", handle.admin_url.trim_end_matches('/'));
    let resp = client
        .post(&url)
        .json(&serde_json::json!({ "token": token }))
        .send()
        .await?;
    let resp = bail_unless_ok(resp, "/display").await?;
    let body: weft_listener::protocol::DisplayResponse = resp.json().await?;
    Ok(body.display)
}

pub async fn action_signal(
    handle: &ListenerHandle,
    token: &str,
    action_kind: &str,
    payload: &Value,
) -> Result<weft_listener::protocol::ActionResponse> {
    let client = reqwest::Client::new();
    let url = format!("{}/action", handle.admin_url.trim_end_matches('/'));
    let resp = client
        .post(&url)
        .json(&serde_json::json!({
            "token": token,
            "kind": action_kind,
            "payload": payload,
        }))
        .send()
        .await?;
    let resp = bail_unless_ok(resp, "/action").await?;
    let body: weft_listener::protocol::ActionResponse = resp.json().await?;
    Ok(body)
}

pub async fn process_signal(
    handle: &ListenerHandle,
    token: &str,
    payload: &Value,
) -> Result<weft_listener::protocol::ProcessOutcome> {
    let client = reqwest::Client::new();
    let url = format!("{}/process", handle.admin_url.trim_end_matches('/'));
    let resp = client
        .post(&url)
        .json(&serde_json::json!({ "token": token, "payload": payload }))
        .send()
        .await?;
    let resp = bail_unless_ok(resp, "/process").await?;
    Ok(resp.json::<weft_listener::protocol::ProcessOutcome>().await?)
}

pub async fn render_signal(handle: &ListenerHandle, token: &str) -> Result<Value> {
    let client = reqwest::Client::new();
    let url = format!("{}/render", handle.admin_url.trim_end_matches('/'));
    let resp = client
        .post(&url)
        .json(&serde_json::json!({ "token": token }))
        .send()
        .await?;
    let resp = bail_unless_ok(resp, "/render").await?;
    Ok(resp.json::<Value>().await?)
}

/// Tell the listener to reconcile its in-memory registry with the
/// durable signal table. Used by activate after TriggerSetup so
/// resume signals that survived a deactivate-park come back from
/// DB before the gate flips to Active. Idempotent: signals already
/// in the registry are not touched.
pub async fn rehydrate(handle: &ListenerHandle) -> Result<()> {
    let client = reqwest::Client::new();
    let url = format!("{}/rehydrate", handle.admin_url.trim_end_matches('/'));
    let resp = client.post(&url).send().await?;
    bail_unless_ok(resp, "/rehydrate").await?;
    Ok(())
}

pub async fn unregister_signal(handle: &ListenerHandle, token: &str) -> Result<()> {
    let client = reqwest::Client::new();
    let url = format!("{}/unregister", handle.admin_url.trim_end_matches('/'));
    let resp = client
        .post(&url)
        .json(&serde_json::json!({ "token": token }))
        .send()
        .await?;
    bail_unless_ok(resp, "/unregister").await?;
    Ok(())
}

// =============================================================
// Lifecycle row state machine
// =============================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ListenerState {
    Starting,
    Alive,
    Stopping,
}

impl ListenerState {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Alive => "alive",
            Self::Stopping => "stopping",
        }
    }
    fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "starting" => Self::Starting,
            "alive" => Self::Alive,
            "stopping" => Self::Stopping,
            _ => return None,
        })
    }
}

const TRANSITION_POLL_MS: u64 = 200;
const TRANSITION_WAIT_MAX_SECS: u64 = 180;

// =============================================================
// Advisory lock keys
// =============================================================

/// Domain constants for these locks live in `crate::lease`.
/// `LISTENER_OP_DOMAIN`: SHARED during `with_listener`, EXCLUSIVE
/// during the reaper sweep. `LISTENER_ROW_DOMAIN`: held only
/// across row reads/writes; never spans kubectl I/O.
fn op_lock_key(tenant_id: &str) -> i64 {
    crate::lease::advisory_key(crate::lease::LISTENER_OP_DOMAIN, tenant_id)
}

fn row_lock_key(tenant_id: &str) -> i64 {
    crate::lease::advisory_key(crate::lease::LISTENER_ROW_DOMAIN, tenant_id)
}

/// Begin a transaction and immediately take the per-tenant advisory
/// row-state lock. Caller commits or drops; lock is released at
/// commit/rollback. Used by every state-machine edit on
/// `tenant_listener` so concurrent dispatchers serialize per-tenant.
async fn begin_with_lock(
    pool: &PgPool,
    lock_key: i64,
) -> Result<sqlx::Transaction<'_, Postgres>> {
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(lock_key)
        .execute(&mut *tx)
        .await?;
    Ok(tx)
}

// =============================================================
// ListenerPool
// =============================================================

/// Per-tenant listener orchestrator.
///
/// The pool itself is stateless (just plumbing). All state lives in
/// the `tenant_listener` row + per-call PG advisory locks. There is
/// no in-memory cache: every operation reads the row to compute the
/// fresh handle, ensuring the dispatcher never hands out a URL that
/// no longer points at a live listener.
#[derive(Default, Clone)]
pub struct ListenerPool;

impl ListenerPool {
    pub fn new() -> Self {
        Self
    }

    /// Run `work(handle)` against the tenant's listener. Spawns the
    /// listener if it isn't running. `ensure_listener_alive` arms the
    /// `op_in_flight_until_unix` sentinel atomically with the row
    /// write (INSERT or Alive renew), so the reaper sees this op
    /// while `work` runs.
    ///
    /// Crash safety: the sentinel is a TTL hint, not a held lock.
    /// If the dispatcher pod dies, the sentinel expires
    /// (`SENTINEL_TTL_SECS`) and the reaper proceeds.
    /// No connection state to clean up.
    pub async fn with_listener<R, F, Fut>(
        &self,
        tenant: &TenantId,
        namespace: &str,
        backend: &dyn ListenerBackend,
        pg_pool: &PgPool,
        pod_id: &str,
        work: F,
    ) -> Result<R>
    where
        F: FnOnce(ListenerHandle) -> Fut,
        Fut: std::future::Future<Output = Result<R>>,
    {
        // `ensure_listener_alive` arms the op-sentinel as part of
        // its row write (INSERT or UPDATE), so by the time it
        // returns Use, the reaper sees the sentinel and backs
        // off. The sentinel is a TTL hint, not a lock: we don't
        // clear on exit (a sibling op may have re-armed it
        // between our work and a clear, and clearing would
        // expose its listener to reap). The sentinel expires
        // naturally `SENTINEL_TTL_SECS` after the
        // last arm.
        let handle = ensure_listener_alive(tenant, namespace, backend, pg_pool, pod_id).await?;
        work(handle).await
    }

    /// Best-effort listener call with no spawn. Returns `Ok(None)`
    /// if the listener isn't currently alive (work is skipped),
    /// `Ok(Some(R))` if it ran. Used by cleanup paths that want to
    /// drop registrations IF the listener happens to be running,
    /// and by inspector paths that want to read live state without
    /// bringing the listener up.
    pub async fn with_listener_if_alive<R, F, Fut>(
        &self,
        tenant: &TenantId,
        pg_pool: &PgPool,
        work: F,
    ) -> Result<Option<R>>
    where
        F: FnOnce(ListenerHandle) -> Fut,
        Fut: std::future::Future<Output = Result<R>>,
    {
        // Pre-arm the sentinel ONLY if a row already exists (this
        // path doesn't spawn). `arm_op_sentinel` returns
        // `Ok(None)` if there's no row to update; benign here.
        // Real DB errors still propagate. No clear at exit: see
        // `with_listener` comment on the TTL hint shape.
        arm_op_sentinel(pg_pool, tenant.as_str()).await?;
        let work_result = async {
            match read_alive_row(tenant, pg_pool).await? {
                Some(handle) => Ok(Some(work(handle).await?)),
                None => Ok(None),
            }
        }
        .await;
        work_result
    }

    /// True iff the tenant currently has an Alive listener row.
    /// Used by status endpoints to render "listener: running" vs
    /// "listener: stopped". Does not take the OP lock.
    pub async fn is_alive(&self, tenant: &TenantId, pg_pool: &PgPool) -> Result<bool> {
        Ok(read_alive_row(tenant, pg_pool).await?.is_some())
    }

    /// Best-effort bulk unregister: group `signals` by tenant, then
    /// for each tenant whose listener is currently alive, POST
    /// `/unregister` for every token. Listeners that are reaped get
    /// skipped (the registry will rehydrate fresh next time the
    /// listener spawns; the durable signal table is the source of
    /// truth, and the caller is expected to drop the rows itself).
    pub async fn unregister_many_if_alive(
        &self,
        pg_pool: &PgPool,
        signals: &[crate::journal::SignalRegistration],
    ) {
        let mut by_tenant: std::collections::HashMap<String, Vec<String>> = Default::default();
        for sig in signals {
            by_tenant
                .entry(sig.tenant_id.clone())
                .or_default()
                .push(sig.token.clone());
        }
        for (tenant_id, tokens) in by_tenant {
            let tenant = TenantId(tenant_id.clone());
            let tokens_for_log = tokens.clone();
            if let Err(e) = self
                .with_listener_if_alive(&tenant, pg_pool, |handle| async move {
                    for token in &tokens {
                        if let Err(e) = unregister_signal(&handle, token).await {
                            tracing::warn!(
                                target: "weft_dispatcher::listener",
                                token,
                                error = %e,
                                "unregister_signal failed (sweep); listener pod may carry stale state"
                            );
                        }
                    }
                    Ok::<_, anyhow::Error>(())
                })
                .await
            {
                tracing::warn!(
                    target: "weft_dispatcher::listener",
                    tenant = %tenant_id,
                    tokens = ?tokens_for_log,
                    error = %e,
                    "with_listener_if_alive failed during sweep; tokens remain registered"
                );
            }
        }
    }

    /// Reaper hook: try to kill an idle listener.
    ///
    /// Returns Ok(true) if the kill happened (or the row was already
    /// gone), Ok(false) if the listener is still in use. The caller
    /// (reaper sweep loop) typically iterates this across every row.
    ///
    /// Algorithm:
    /// 1. Try to acquire the OP lock in EXCLUSIVE mode (non-blocking).
    ///    Fails if any `with_listener` is in flight: back off.
    /// 2. With EXCLUSIVE held, check the signal table: if the tenant
    ///    has any signals, the listener is still semantically needed.
    ///    Back off.
    /// 3. Otherwise, run the row state transition (Alive → Stopping
    ///    → row deleted) under the row-state XACT lock, kubectl delete
    ///    the listener, and release EXCLUSIVE.
    pub async fn try_reap_if_idle(
        &self,
        tenant: &TenantId,
        namespace: &str,
        backend: &dyn ListenerBackend,
        pg_pool: &PgPool,
        pod_id: &str,
    ) -> Result<bool> {
        // Decide under the per-tenant xact-scoped advisory lock.
        // If an op holds the lock or the sentinel is live, back off.
        match try_reap_decision(pg_pool, tenant.as_str()).await? {
            ReapDecision::Busy => return Ok(false),
            ReapDecision::Idle => {}
        }
        run_reap(tenant, namespace, backend, pg_pool, pod_id).await
    }
}

/// Reap body. `try_reap_decision` already established (under one
/// xact + advisory lock) that this tenant has no live signals and
/// no live op sentinel. The lock has since released, but the row
/// delete below is the synchronization barrier: any op landing
/// after this point sees a missing row in `decide_under_lock` and
/// re-spawns fresh.
async fn run_reap(
    tenant: &TenantId,
    namespace: &str,
    backend: &dyn ListenerBackend,
    pg_pool: &PgPool,
    pod_id: &str,
) -> Result<bool> {
    // Claim Stopping under the row-state lock.
    let claimed = claim_stopping(tenant, pg_pool, pod_id).await?;
    if !claimed {
        // Row was already gone or in another transition.
        return Ok(true);
    }

    // Slow kubectl delete. `backend.stop` failures are logged but
    // don't block the row delete: leaving a tenant entry pointing
    // at a half-stopped pod is worse than leaving an orphaned pod
    // that the next supervisor sweep catches.
    if let Err(e) = backend.stop(tenant, namespace).await {
        tracing::warn!(
            target: "weft_dispatcher::listener",
            tenant = tenant.as_str(),
            namespace,
            error = %e,
            "backend.stop failed during reap; will proceed to delete_row"
        );
    }
    delete_row(tenant, pg_pool).await?;
    Ok(true)
}

// =============================================================
// Per-operation sentinel + xact lock
// =============================================================
//
// Ops (`with_listener`, `with_listener_if_alive`) and the reaper
// coordinate via the same per-tenant xact-scoped advisory key
// (`lease::LISTENER_OP_DOMAIN`). Ops arm a sentinel under the lock,
// then run their work (possibly slow, involving broker HTTP).
// The reaper takes the same lock NON-BLOCKING; if it can't, an
// op's arm is still in flight. If it can, it then reads the
// sentinel: a live `op_in_flight_until_unix` means an op is
// running (or recently was). The reaper backs off until the
// sentinel expires AND the lock is free.
//
// xact-scoped means no session-leak back into the pool: the lock
// auto-releases on COMMIT/ROLLBACK. No fire-and-forget Drop
// unlocks anywhere.

/// Refresh the op-in-flight sentinel for `tenant_id` under a
/// short xact-scoped advisory lock. Returns `Some(deadline)` if
/// the row exists (sentinel armed), `None` if no row exists
/// (caller should not assume the sentinel was armed). Most ops
/// arm via `ensure_listener_alive` (which writes the row +
/// sentinel atomically); this helper is for explicit refresh
/// during long ops (e.g. ActivateKeepAlive heartbeat).
async fn arm_op_sentinel(pg_pool: &PgPool, tenant_id: &str) -> Result<Option<i64>> {
    let key = op_lock_key(tenant_id);
    let until = crate::lease::now_unix() + crate::lease::SENTINEL_TTL_SECS;
    let mut tx = pg_pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(key)
        .execute(&mut *tx)
        .await?;
    let res = sqlx::query(
        "UPDATE tenant_listener SET op_in_flight_until_unix = $1 WHERE tenant_id = $2",
    )
    .bind(until)
    .bind(tenant_id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    if res.rows_affected() == 0 {
        Ok(None)
    } else {
        Ok(Some(until))
    }
}

// No explicit clear: the sentinel is a TTL-based hint, not a
// lock. Clearing on op-exit would clobber a sibling op that
// arrived between our arm and our clear (its sentinel would
// vanish, the reaper would see idle, and kill the listener
// mid-sibling-op). The TTL is short enough (SENTINEL_TTL_SECS
// = 30s) that natural expiry is fast; the reaper checks the
// deadline against NOW() rather than presence of a value.

/// Outcome of the reaper's "can I reap this tenant?" check under
/// the xact-scoped lock.
enum ReapDecision {
    /// Sentinel is in the future, or an op holds the lock; back
    /// off and retry on the next sweep.
    Busy,
    /// No ops in flight; safe to delete the tenant_listener row
    /// and scale down the listener pod.
    Idle,
}

/// Decide whether a tenant's listener is idle enough to reap.
/// Three checks, all under one xact-scoped advisory lock so a
/// new op can't slip in between them:
///
///   1. `pg_try_advisory_xact_lock` on the tenant key. If held by
///      a mid-tx op, return `Busy`.
///   2. `signal` rows for the tenant. If any exist, return `Busy`:
///      registered triggers are the primary "this tenant needs a
///      listener" signal.
///   3. `tenant_listener.op_in_flight_until_unix > NOW()`. If the
///      sentinel is live, return `Busy`: an op is in flight that
///      isn't holding the lock right now but will be back.
///
/// Returning `Idle` decides on a snapshot under this lock; an op
/// that lands AFTER this tx commits but BEFORE `claim_stopping`
/// acquires the row-lock would otherwise slip in. The second
/// barrier is in `claim_stopping`: it re-reads
/// `op_in_flight_until_unix` under its own row-lock'd tx and
/// bails if an op armed the sentinel in the gap. Two-phase commit
/// without long-held locks across kubectl.
async fn try_reap_decision(pg_pool: &PgPool, tenant_id: &str) -> Result<ReapDecision> {
    let key = op_lock_key(tenant_id);
    let mut tx = pg_pool.begin().await?;
    let got: bool = sqlx::query_scalar("SELECT pg_try_advisory_xact_lock($1)")
        .bind(key)
        .fetch_one(&mut *tx)
        .await?;
    if !got {
        tx.commit().await?;
        return Ok(ReapDecision::Busy);
    }
    let signal_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM signal WHERE tenant_id = $1",
    )
    .bind(tenant_id)
    .fetch_one(&mut *tx)
    .await?;
    if signal_count > 0 {
        tx.commit().await?;
        return Ok(ReapDecision::Busy);
    }
    let sentinel: Option<i64> = sqlx::query_scalar(
        "SELECT op_in_flight_until_unix FROM tenant_listener WHERE tenant_id = $1",
    )
    .bind(tenant_id)
    .fetch_optional(&mut *tx)
    .await?
    .flatten();
    let now = crate::lease::now_unix();
    let live_sentinel = sentinel.map_or(false, |u| u > now);
    tx.commit().await?;
    if live_sentinel {
        return Ok(ReapDecision::Busy);
    }
    Ok(ReapDecision::Idle)
}

// =============================================================
// Row state machine (transactional advisory-lock helpers)
// =============================================================

/// Drive the row to Alive, spawning if needed. Returns the live
/// handle. Caller must already be holding the OP-lock (SHARED) so
/// the reaper can't slip in mid-spawn.
async fn ensure_listener_alive(
    tenant: &TenantId,
    namespace: &str,
    backend: &dyn ListenerBackend,
    pg_pool: &PgPool,
    pod_id: &str,
) -> Result<ListenerHandle> {
    let started_waiting = std::time::Instant::now();
    loop {
        let decision = decide_under_lock(tenant, namespace, pg_pool, pod_id).await?;
        match decision {
            EnsureDecision::Use(handle) => return Ok(handle),
            EnsureDecision::Wait => {
                if started_waiting.elapsed().as_secs() > TRANSITION_WAIT_MAX_SECS {
                    anyhow::bail!(
                        "tenant_listener transition for {tenant} did not finish within \
                         {TRANSITION_WAIT_MAX_SECS}s; the owning Pod likely crashed mid-\
                         transition (lease will expire and the next call will recover)"
                    );
                }
                tokio::time::sleep(std::time::Duration::from_millis(TRANSITION_POLL_MS)).await;
                continue;
            }
            EnsureDecision::Spawn => {
                // We've inserted a Starting row + own the slot. If
                // backend.spawn fails, drop the row and any K8s state
                // it may have left behind so the next ensure starts
                // clean. Without this rollback the row stays Starting
                // forever (we'd renew our own lease, never expire).
                match backend.spawn(tenant, namespace).await {
                    Ok(handle) => {
                        commit_alive(tenant, namespace, pg_pool, pod_id, &handle).await?;
                        return Ok(handle);
                    }
                    Err(spawn_err) => {
                        // Cleanup on spawn failure. Both calls log
                        // on failure but the original spawn error is
                        // what we surface to the caller.
                        if let Err(e) = backend.stop(tenant, namespace).await {
                            tracing::warn!(
                                target: "weft_dispatcher::listener",
                                tenant = tenant.as_str(),
                                namespace,
                                error = %e,
                                "backend.stop failed during spawn rollback"
                            );
                        }
                        if let Err(e) = delete_row(tenant, pg_pool).await {
                            tracing::warn!(
                                target: "weft_dispatcher::listener",
                                tenant = tenant.as_str(),
                                error = %e,
                                "delete_row failed during spawn rollback"
                            );
                        }
                        return Err(spawn_err);
                    }
                }
            }
            EnsureDecision::CleanupAbandoned { abandoned_namespace } => {
                if let Err(e) = backend.stop(tenant, &abandoned_namespace).await {
                    tracing::warn!(
                        target: "weft_dispatcher::listener",
                        tenant = tenant.as_str(),
                        namespace = abandoned_namespace.as_str(),
                        error = %e,
                        "backend.stop failed for abandoned listener; will retry on next ensure"
                    );
                }
                delete_row(tenant, pg_pool).await?;
                continue;
            }
        }
    }
}

enum EnsureDecision {
    Use(ListenerHandle),
    Wait,
    Spawn,
    CleanupAbandoned { abandoned_namespace: String },
}

async fn decide_under_lock(
    tenant: &TenantId,
    namespace: &str,
    pg_pool: &PgPool,
    pod_id: &str,
) -> Result<EnsureDecision> {
    let mut tx = begin_with_lock(pg_pool, row_lock_key(tenant.as_str())).await?;

    let row: Option<(String, String, i64, String)> = sqlx::query_as(
        "SELECT admin_url, namespace, leased_until_unix, state \
         FROM tenant_listener WHERE tenant_id = $1",
    )
    .bind(tenant.as_str())
    .fetch_optional(&mut *tx)
    .await?;

    let Some((admin_url, row_namespace, leased_until, state_str)) = row else {
        // No row: claim Spawn. `admin_url` gets filled in by
        // `commit_alive` once the listener Pod is up. Arm the
        // op-sentinel from the moment of row birth so the reaper
        // can't snipe the row between this INSERT and the
        // caller's later work.
        let op_until = crate::lease::now_unix()
            + crate::lease::SENTINEL_TTL_SECS;
        sqlx::query(
            "INSERT INTO tenant_listener \
             (tenant_id, owner_pod_id, leased_until_unix, namespace, \
              admin_url, state, op_in_flight_until_unix) \
             VALUES ($1, $2, $3, $4, '', $5, $6)",
        )
        .bind(tenant.as_str())
        .bind(pod_id)
        .bind(crate::lease::now_unix() + crate::lease::LEASE_DURATION_SECS)
        .bind(namespace)
        .bind(ListenerState::Starting.as_str())
        .bind(op_until)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        return Ok(EnsureDecision::Spawn);
    };

    let lease_live = crate::lease::is_lease_live(leased_until);
    let state = ListenerState::parse(&state_str)
        .ok_or_else(|| anyhow::anyhow!("unknown tenant_listener.state '{state_str}'"))?;

    // Lease lapse on a non-alive row means the prior owner died
    // mid-transition: reap and respawn. Lease lapse on an Alive row
    // means the prior owner died but the listener Pod itself is
    // (presumably) still healthy (its Deployment is independent of
    // the dispatcher's lifecycle). Take ownership and reuse; if the
    // listener really is dead we'll discover it via the next HTTP
    // round-trip and the operation surfaces a clean error.
    if !lease_live && !matches!(state, ListenerState::Alive) {
        sqlx::query(
            "UPDATE tenant_listener \
             SET owner_pod_id = $2, leased_until_unix = $3, state = $4 \
             WHERE tenant_id = $1",
        )
        .bind(tenant.as_str())
        .bind(pod_id)
        .bind(crate::lease::now_unix() + crate::lease::LEASE_DURATION_SECS)
        .bind(ListenerState::Stopping.as_str())
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        return Ok(EnsureDecision::CleanupAbandoned {
            abandoned_namespace: row_namespace,
        });
    }

    match state {
        ListenerState::Alive => {
            // Renew the lease AND arm the op-sentinel on every
            // successful Use. The lease keeps any sibling dispatcher
            // pod from concluding the owner died; the sentinel keeps
            // the reaper from killing the listener pod while our
            // caller is mid-work.
            let op_until = crate::lease::now_unix()
                + crate::lease::SENTINEL_TTL_SECS;
            sqlx::query(
                "UPDATE tenant_listener \
                 SET owner_pod_id = $2, leased_until_unix = $3, \
                     op_in_flight_until_unix = $4 \
                 WHERE tenant_id = $1",
            )
            .bind(tenant.as_str())
            .bind(pod_id)
            .bind(crate::lease::now_unix() + crate::lease::LEASE_DURATION_SECS)
            .bind(op_until)
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;
            Ok(EnsureDecision::Use(ListenerHandle { admin_url }))
        }
        ListenerState::Starting | ListenerState::Stopping => {
            tx.commit().await?;
            Ok(EnsureDecision::Wait)
        }
    }
}

async fn commit_alive(
    tenant: &TenantId,
    namespace: &str,
    pg_pool: &PgPool,
    pod_id: &str,
    handle: &ListenerHandle,
) -> Result<()> {
    let mut tx = begin_with_lock(pg_pool, row_lock_key(tenant.as_str())).await?;
    sqlx::query(
        "UPDATE tenant_listener \
         SET admin_url = $2, namespace = $3, owner_pod_id = $4, \
             leased_until_unix = $5, state = $6 \
         WHERE tenant_id = $1",
    )
    .bind(tenant.as_str())
    .bind(&handle.admin_url)
    .bind(namespace)
    .bind(pod_id)
    .bind(crate::lease::now_unix() + crate::lease::LEASE_DURATION_SECS)
    .bind(ListenerState::Alive.as_str())
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

async fn delete_row(tenant: &TenantId, pg_pool: &PgPool) -> Result<()> {
    let mut tx = begin_with_lock(pg_pool, row_lock_key(tenant.as_str())).await?;
    sqlx::query("DELETE FROM tenant_listener WHERE tenant_id = $1")
        .bind(tenant.as_str())
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

/// Flip Alive → Stopping under the row-state lock. Returns true on
/// success, false if the row is gone or already mid-transition.
async fn claim_stopping(
    tenant: &TenantId,
    pg_pool: &PgPool,
    pod_id: &str,
) -> Result<bool> {
    let mut tx = begin_with_lock(pg_pool, row_lock_key(tenant.as_str())).await?;
    // Read state AND the sentinel under the row-lock'd tx. If
    // an op armed the sentinel between `try_reap_decision`'s
    // commit and this acquisition, we see it here and bail. The
    // reap stays correct without holding any cross-step lock.
    let row: Option<(String, Option<i64>)> = sqlx::query_as(
        "SELECT state, op_in_flight_until_unix \
         FROM tenant_listener WHERE tenant_id = $1",
    )
    .bind(tenant.as_str())
    .fetch_optional(&mut *tx)
    .await?;
    let Some((state_str, sentinel)) = row else {
        tx.commit().await?;
        return Ok(false);
    };
    let Some(state) = ListenerState::parse(&state_str) else {
        tx.commit().await?;
        anyhow::bail!("unknown tenant_listener.state '{state_str}'");
    };
    if state != ListenerState::Alive {
        tx.commit().await?;
        return Ok(false);
    }
    let now = crate::lease::now_unix();
    if sentinel.map_or(false, |u| u > now) {
        // An op armed the sentinel between the reaper's
        // try_reap_decision commit and this claim. Back off:
        // the op is mid-flight, will finish, and the next sweep
        // sees the row again.
        tx.commit().await?;
        return Ok(false);
    }
    sqlx::query(
        "UPDATE tenant_listener \
         SET owner_pod_id = $2, leased_until_unix = $3, state = $4 \
         WHERE tenant_id = $1",
    )
    .bind(tenant.as_str())
    .bind(pod_id)
    .bind(crate::lease::now_unix() + crate::lease::LEASE_DURATION_SECS)
    .bind(ListenerState::Stopping.as_str())
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(true)
}

/// Read the row iff state=Alive. Returns None for any other state
/// (Starting, Stopping, no row). Used by `with_listener_if_alive`
/// after the OP lock is held, so the row state is stable for the
/// duration of the read.
async fn read_alive_row(tenant: &TenantId, pg_pool: &PgPool) -> Result<Option<ListenerHandle>> {
    let lock_key = row_lock_key(tenant.as_str());
    let mut tx = pg_pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(lock_key)
        .execute(&mut *tx)
        .await?;
    let row: Option<(String, String)> = sqlx::query_as(
        "SELECT admin_url, state FROM tenant_listener WHERE tenant_id = $1",
    )
    .bind(tenant.as_str())
    .fetch_optional(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(row.and_then(|(admin_url, state)| {
        if state == ListenerState::Alive.as_str() {
            Some(ListenerHandle { admin_url })
        } else {
            None
        }
    }))
}

// =============================================================
// Activate-handler keep-alive lease
// =============================================================

/// Keep the listener alive for an activate / TriggerSetup window
/// that may span many seconds (spawning trigger-setup worker,
/// waiting for first ctx.register_signal). Arms the per-tenant
/// op-sentinel up-front and re-arms it on a background heartbeat
/// until the guard is dropped.
///
/// Without this, the reaper can find a window where:
/// - status=Active is set,
/// - no signal rows exist yet (first register_signal hasn't completed),
/// - no in-flight register_signal task exists yet,
/// and reap the freshly-spawned listener.
///
/// The heartbeat task is owned by the inner `TtlHeartbeat`; Drop
/// aborts it. After Drop the sentinel expires by TTL: the row's
/// `op_in_flight_until_unix` becomes stale `SENTINEL_TTL_SECS`
/// after the last heartbeat, allowing the reaper to proceed.
#[must_use = "the keep-alive's TtlHeartbeat aborts on Drop; \
              hold the binding for the duration of the activate window"]
pub struct ActivateKeepAlive(#[allow(dead_code)] crate::lease::TtlHeartbeat);

impl ActivateKeepAlive {
    pub async fn acquire(pg_pool: &PgPool, tenant: &TenantId) -> Result<Self> {
        // First arm is best-effort: the listener row may not yet
        // exist (this is called from activate BEFORE TriggerSetup
        // spawns the listener). `arm_op_sentinel` returns
        // Ok(None) in that case; later, register_signal's spawn
        // path arms the sentinel atomically with the INSERT.
        // The heartbeat keeps refreshing.
        arm_op_sentinel(pg_pool, tenant.as_str()).await?;
        let pool = pg_pool.clone();
        let t = tenant.clone();
        let hb = crate::lease::TtlHeartbeat::spawn(
            "ActivateKeepAlive",
            crate::lease::heartbeat_interval(),
            move || {
                let pool = pool.clone();
                let t = t.clone();
                async move {
                    arm_op_sentinel(&pool, t.as_str()).await?;
                    Ok(())
                }
            },
        );
        Ok(Self(hb))
    }
}
