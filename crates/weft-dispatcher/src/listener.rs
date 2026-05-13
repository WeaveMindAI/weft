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
use sqlx::pool::PoolConnection;
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
            let _ = c.kill().await;
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
}

impl K8sListenerBackend {
    pub fn new(listener_image: String, broker_url: String) -> Self {
        Self { listener_image, broker_url }
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
        kubectl_apply_manifest(&manifest).await?;
        kubectl_rollout_status(namespace, &deploy_name).await?;
        wait_for_health(&admin_url).await?;
        Ok(ListenerHandle { admin_url })
    }

    async fn stop(&self, tenant: &TenantId, namespace: &str) -> Result<()> {
        let deploy_name = deploy_name_for_tenant(tenant.as_str());
        // Service first: bounded `--wait=true`. Then Deployment with
        // foreground cascade so ReplicaSet + Pods finish terminating
        // before we return; eliminates the "old Pod still holds the
        // Service Endpoint" race when a fresh spawn lands milliseconds
        // later.
        let _ = Command::new("kubectl")
            .args([
                "-n", namespace, "delete", "service", &deploy_name,
                "--ignore-not-found", "--wait=true",
            ])
            .status()
            .await;
        let _ = Command::new("kubectl")
            .args([
                "-n", namespace, "delete", "deployment", &deploy_name,
                "--ignore-not-found", "--wait=true", "--cascade=foreground",
            ])
            .status()
            .await;
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

async fn kubectl_apply_manifest(manifest: &str) -> Result<()> {
    use tokio::io::AsyncWriteExt;
    let mut child = Command::new("kubectl")
        .args(["apply", "-f", "-"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .context("spawn kubectl apply")?;
    if let Some(stdin) = child.stdin.as_mut() {
        stdin.write_all(manifest.as_bytes()).await?;
    }
    let output = child.wait_with_output().await?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("kubectl apply failed: {stderr}");
    }
    Ok(())
}

async fn kubectl_rollout_status(namespace: &str, deployment: &str) -> Result<()> {
    let status = Command::new("kubectl")
        .args([
            "-n", namespace,
            "rollout", "status", &format!("deployment/{deployment}"),
            "--timeout=120s",
        ])
        .status()
        .await?;
    if !status.success() {
        anyhow::bail!("{deployment} did not reach Ready within 120s");
    }
    Ok(())
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
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("listener /register returned {}: {}", status, body);
    }
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
    if !resp.status().is_success() {
        anyhow::bail!(
            "listener /display returned {}: {}",
            resp.status(),
            resp.text().await.unwrap_or_default()
        );
    }
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
    if !resp.status().is_success() {
        anyhow::bail!(
            "listener /action returned {}: {}",
            resp.status(),
            resp.text().await.unwrap_or_default()
        );
    }
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
    if !resp.status().is_success() {
        anyhow::bail!(
            "listener /process returned {}: {}",
            resp.status(),
            resp.text().await.unwrap_or_default()
        );
    }
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
    if !resp.status().is_success() {
        anyhow::bail!(
            "listener /render returned {}: {}",
            resp.status(),
            resp.text().await.unwrap_or_default()
        );
    }
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
    if !resp.status().is_success() {
        anyhow::bail!(
            "listener /rehydrate returned {}: {}",
            resp.status(),
            resp.text().await.unwrap_or_default()
        );
    }
    Ok(())
}

pub async fn unregister_signal(handle: &ListenerHandle, token: &str) -> Result<()> {
    let client = reqwest::Client::new();
    let url = format!("{}/unregister", handle.admin_url.trim_end_matches('/'));
    let _ = client
        .post(&url)
        .json(&serde_json::json!({ "token": token }))
        .send()
        .await;
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

/// Hash domain separator for the per-tenant operation lock. SHARED
/// during `with_listener`, EXCLUSIVE during the reaper sweep.
const OP_LOCK_DOMAIN: &str = "tenant_listener_op";
/// Hash domain separator for the per-tenant transactional state-row
/// mutex. Held only across row reads/writes; never spans kubectl I/O.
const ROW_LOCK_DOMAIN: &str = "tenant_listener_row";

fn op_lock_key(tenant_id: &str) -> i64 {
    hash_advisory_key(OP_LOCK_DOMAIN, tenant_id)
}

fn row_lock_key(tenant_id: &str) -> i64 {
    hash_advisory_key(ROW_LOCK_DOMAIN, tenant_id)
}

fn hash_advisory_key(domain: &str, tenant_id: &str) -> i64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    domain.hash(&mut h);
    tenant_id.hash(&mut h);
    h.finish() as i64
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
    /// listener if it isn't running. Holds an OP-lock in SHARED mode
    /// for the duration of `work`, fencing the reaper out.
    ///
    /// Crash safety: if the dispatcher Pod dies while `work` is
    /// running, PG releases the SHARED lock automatically when the
    /// connection drops. The reaper can then proceed.
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
        let guard = ListenerOpGuard::acquire(pg_pool, tenant.as_str()).await?;
        let work_result = async {
            let handle = ensure_listener_alive(tenant, namespace, backend, pg_pool, pod_id).await?;
            work(handle).await
        }
        .await;
        guard.release().await;
        work_result
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
        let guard = ListenerOpGuard::acquire(pg_pool, tenant.as_str()).await?;
        let work_result = async {
            match read_alive_row(tenant, pg_pool).await? {
                Some(handle) => Ok(Some(work(handle).await?)),
                None => Ok(None),
            }
        }
        .await;
        guard.release().await;
        work_result
    }

    /// True iff the tenant currently has an Alive listener row.
    /// Used by status endpoints to render "listener: running" vs
    /// "listener: stopped". Does not take the OP lock.
    pub async fn is_alive(&self, tenant: &TenantId, pg_pool: &PgPool) -> bool {
        read_alive_row(tenant, pg_pool).await.ok().flatten().is_some()
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
            let tenant = TenantId(tenant_id);
            let _ = self
                .with_listener_if_alive(&tenant, pg_pool, |handle| async move {
                    for token in &tokens {
                        let _ = unregister_signal(&handle, token).await;
                    }
                    Ok::<_, anyhow::Error>(())
                })
                .await;
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
        let Some(guard) = ListenerReapGuard::try_acquire(pg_pool, tenant.as_str()).await? else {
            // EXCLUSIVE wasn't free; some operation is in flight.
            return Ok(false);
        };
        let outcome = run_reap(tenant, namespace, backend, pg_pool, pod_id, &guard).await;
        guard.release().await;
        outcome
    }
}

/// Body of `try_reap_if_idle` factored out so the EXCLUSIVE guard's
/// release is centralized at the caller. `_guard` argument is only
/// here to make the borrow lifetime explicit.
async fn run_reap(
    tenant: &TenantId,
    namespace: &str,
    backend: &dyn ListenerBackend,
    pg_pool: &PgPool,
    pod_id: &str,
    _guard: &ListenerReapGuard,
) -> Result<bool> {
    // EXCLUSIVE held. Re-check semantic need: any signal rows for
    // this tenant => still needed.
    let signal_count: (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM signal WHERE tenant_id = $1")
            .bind(tenant.as_str())
            .fetch_one(pg_pool)
            .await?;
    if signal_count.0 > 0 {
        return Ok(false);
    }

    // Claim Stopping under the row-state lock.
    let claimed = claim_stopping(tenant, pg_pool, pod_id).await?;
    if !claimed {
        // Row was already gone or in another transition.
        return Ok(true);
    }

    // Slow kubectl delete with the EXCLUSIVE OP-lock still held
    // (operations can't slip in mid-kill).
    let _ = backend.stop(tenant, namespace).await;
    delete_row(tenant, pg_pool).await?;
    Ok(true)
}

// =============================================================
// Per-operation SHARED lock guard
// =============================================================

/// Holds a SHARED advisory lock on the tenant's OP key on a
/// dedicated PG connection. Lifetime semantics:
///
/// - `acquire`: checkout a connection, take the SHARED lock on it.
/// - `release` (async): explicit unlock + return the connection to
///   the pool clean. Always called by `with_listener` /
///   `with_listener_if_alive` on the happy path.
/// - `Drop`: synchronous fallback. Spawns a fire-and-forget task to
///   run the unlock, so a panic / early-return doesn't leak the
///   lock on the pooled connection (sqlx's PgPool does NOT issue
///   `DISCARD ALL` between checkouts by default, so the lock would
///   otherwise survive the connection's reuse).
#[must_use = "call `release().await` to free the advisory lock; \
              Drop spawns a fire-and-forget unlock that may not \
              run if the runtime is tearing down, leaking the lock \
              onto the pooled connection"]
struct ListenerOpGuard {
    conn: Option<PoolConnection<Postgres>>,
    key: i64,
}

impl ListenerOpGuard {
    async fn acquire(pg_pool: &PgPool, tenant_id: &str) -> Result<Self> {
        let mut conn = pg_pool.acquire().await?;
        let key = op_lock_key(tenant_id);
        sqlx::query("SELECT pg_advisory_lock_shared($1)")
            .bind(key)
            .execute(&mut *conn)
            .await?;
        Ok(Self { conn: Some(conn), key })
    }

    async fn release(mut self) {
        if let Some(mut conn) = self.conn.take() {
            let _ = sqlx::query("SELECT pg_advisory_unlock_shared($1)")
                .bind(self.key)
                .execute(&mut *conn)
                .await;
        }
    }
}

impl Drop for ListenerOpGuard {
    fn drop(&mut self) {
        // If `release` was called (Some(conn) drained on the way),
        // there's nothing to do here. Otherwise spawn a fire-and-
        // forget unlock so the SHARED lock doesn't survive on a
        // pooled connection that may be re-used for unrelated work.
        if let Some(mut conn) = self.conn.take() {
            let key = self.key;
            tokio::spawn(async move {
                let _ = sqlx::query("SELECT pg_advisory_unlock_shared($1)")
                    .bind(key)
                    .execute(&mut *conn)
                    .await;
            });
        }
    }
}

/// Holds an EXCLUSIVE advisory lock on the tenant's OP key. Used by
/// the reaper to fence out concurrent `with_listener` calls. Same
/// connection-leak protection as `ListenerOpGuard`.
#[must_use = "call `release().await` to free the advisory lock; \
              Drop spawns a fire-and-forget unlock that may not \
              run if the runtime is tearing down, leaking the lock \
              onto the pooled connection"]
struct ListenerReapGuard {
    conn: Option<PoolConnection<Postgres>>,
    key: i64,
}

impl ListenerReapGuard {
    /// Try to grab the EXCLUSIVE lock without blocking. Returns None
    /// if any operation is currently holding SHARED on the same key.
    async fn try_acquire(pg_pool: &PgPool, tenant_id: &str) -> Result<Option<Self>> {
        let mut conn = pg_pool.acquire().await?;
        let key = op_lock_key(tenant_id);
        let acquired: (bool,) = sqlx::query_as("SELECT pg_try_advisory_lock($1)")
            .bind(key)
            .fetch_one(&mut *conn)
            .await?;
        if !acquired.0 {
            return Ok(None);
        }
        Ok(Some(Self { conn: Some(conn), key }))
    }

    async fn release(mut self) {
        if let Some(mut conn) = self.conn.take() {
            let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
                .bind(self.key)
                .execute(&mut *conn)
                .await;
        }
    }
}

impl Drop for ListenerReapGuard {
    fn drop(&mut self) {
        if let Some(mut conn) = self.conn.take() {
            let key = self.key;
            tokio::spawn(async move {
                let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
                    .bind(key)
                    .execute(&mut *conn)
                    .await;
            });
        }
    }
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
                    Err(e) => {
                        let _ = backend.stop(tenant, namespace).await;
                        let _ = delete_row(tenant, pg_pool).await;
                        return Err(e);
                    }
                }
            }
            EnsureDecision::CleanupAbandoned { abandoned_namespace } => {
                let _ = backend.stop(tenant, &abandoned_namespace).await;
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
        // `commit_alive` once the listener Pod is up.
        sqlx::query(
            "INSERT INTO tenant_listener \
             (tenant_id, owner_pod_id, leased_until_unix, namespace, \
              admin_url, state) \
             VALUES ($1, $2, $3, $4, '', $5)",
        )
        .bind(tenant.as_str())
        .bind(pod_id)
        .bind(crate::lease::now_unix() + crate::lease::LEASE_DURATION_SECS)
        .bind(namespace)
        .bind(ListenerState::Starting.as_str())
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
            // Renew the lease on every successful Use, with our pod
            // as owner. This way ANY dispatcher pod that touches the
            // listener keeps it alive, not just the original spawner.
            sqlx::query(
                "UPDATE tenant_listener \
                 SET owner_pod_id = $2, leased_until_unix = $3 \
                 WHERE tenant_id = $1",
            )
            .bind(tenant.as_str())
            .bind(pod_id)
            .bind(crate::lease::now_unix() + crate::lease::LEASE_DURATION_SECS)
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
    let row: Option<(String,)> = sqlx::query_as(
        "SELECT state FROM tenant_listener WHERE tenant_id = $1",
    )
    .bind(tenant.as_str())
    .fetch_optional(&mut *tx)
    .await?;
    let Some((state_str,)) = row else {
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

/// Hold a SHARED OP-lock on the tenant for the duration of an
/// activate / TriggerSetup window. This keeps the listener from
/// being reaped between the moment the activate handler spawns its
/// trigger-setup worker and the moment that worker writes its first
/// signal row.
///
/// Without this, the reaper can find a window where:
/// - status=Active is set
/// - no signal rows exist yet (first register_signal hasn't completed)
/// - no in-flight register_signal task exists yet (worker hasn't
///   reached its first ctx.register_signal call)
/// and reap the freshly-spawned listener.
#[must_use = "call `release().await` to free the keep-alive lease; \
              Drop falls back to a fire-and-forget unlock that may \
              not run on shutdown, leaking the lock onto the \
              pooled connection"]
pub struct ActivateKeepAlive {
    inner: ListenerOpGuard,
}

impl ActivateKeepAlive {
    pub async fn acquire(pg_pool: &PgPool, tenant: &TenantId) -> Result<Self> {
        let guard = ListenerOpGuard::acquire(pg_pool, tenant.as_str()).await?;
        Ok(Self { inner: guard })
    }

    pub async fn release(self) {
        self.inner.release().await;
    }
}
