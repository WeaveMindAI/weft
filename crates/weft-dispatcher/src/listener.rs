//! Pooled listener placement. A listener is a long-lived, TRUSTED,
//! tenant-agnostic processor: each pod holds signals belonging to MANY
//! tenants (placement is per-signal, not per-tenant). The dispatcher
//! places each signal on a non-saturated listener pod (or spawns one),
//! records the holder in `signal.listener_pod`, and resolves a fire by
//! looking that holder up.
//!
//! ## State (all in Postgres; pod-local RAM is only optimization)
//!
//! - `listener_pod(pod_name PK, admin_url, namespace, owner_pod_id,
//!   leased_until_unix)`: the registry of live listener pods. The
//!   pooled analog of the old per-tenant `tenant_listener` row, keyed
//!   by POD. `owner_pod_id` + `leased_until_unix` say which dispatcher
//!   pod is authoritative for this listener's lifecycle; a sibling
//!   dispatcher adopts an expired lease.
//! - `signal.listener_pod`: which listener holds each signal's live
//!   registry entry. NULL = not placed yet, or the holder died and it
//!   awaits re-placement.
//!
//! ## Placement (load-based, Branch 2)
//!
//! `place_signal` reads each live pod's `GET /load`, picks the
//! least-loaded NON-saturated pod, and registers the signal there
//! (setting `signal.listener_pod`). If every pod is saturated (or none
//! exist), it spawns a fresh pod and places there. A pod that reports
//! `saturated` also 503s `/register`, so a placement race against a
//! stale load read fails loudly instead of overloading the pod, and
//! the caller retries placement.
//!
//! ## Resolution + reap
//!
//! Fire / display / action resolve `token -> signal.listener_pod ->
//! admin_url`. The reaper reaps a listener pod that holds ZERO signals
//! (per-pod idle, replacing the old per-tenant idle check). A pod
//! holding live held-connection loops is never reaped under load; its
//! signals are re-placed elsewhere first on an intentional scale-down.

use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use dashmap::DashMap;
use serde_json::Value;
use sqlx::postgres::PgPool;
use tokio::process::{Child, Command};
use tokio::sync::Mutex;
use uuid::Uuid;
use weft_core::primitive::SignalSpec;

/// Handle to a running listener. Just the URL: there is no bearer
/// auth between dispatcher and listener. The trust boundary is the
/// network (NetworkPolicy in k8s, loopback-only listen address in
/// subprocess dev), not a shared secret.
#[derive(Debug, Clone)]
pub struct ListenerHandle {
    pub admin_url: String,
}

/// A live listener pod: its name (placement key) + admin URL.
#[derive(Debug, Clone)]
pub struct ListenerPod {
    pub pod_name: String,
    pub admin_url: String,
}

impl ListenerPod {
    fn handle(&self) -> ListenerHandle {
        ListenerHandle {
            admin_url: self.admin_url.clone(),
        }
    }
}

#[async_trait]
pub trait ListenerBackend: Send + Sync {
    /// Spawn a fresh listener pod named `pod_name` in `namespace`.
    /// The pod is tenant-agnostic; its identity is its own name.
    async fn spawn(&self, pod_name: &str, namespace: &str) -> Result<ListenerHandle>;
    async fn stop(&self, pod_name: &str, namespace: &str) -> Result<()>;
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
    async fn spawn(&self, pod_name: &str, _namespace: &str) -> Result<ListenerHandle> {
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
        cmd.env("WEFT_POD_NAME", pod_name)
            .env("WEFT_LISTENER_PORT", port.to_string())
            .env("WEFT_BROKER_URL", broker_url)
            .env("WEFT_BROKER_TOKEN_PATH", token_path)
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .kill_on_drop(true);
        let child = cmd
            .spawn()
            .with_context(|| format!("spawn listener pod {pod_name}"))?;
        self.children
            .insert(pod_name.to_string(), Arc::new(Mutex::new(child)));

        wait_for_health(&admin_url).await?;
        Ok(ListenerHandle { admin_url })
    }

    async fn stop(&self, pod_name: &str, _namespace: &str) -> Result<()> {
        if let Some((_, child)) = self.children.remove(pod_name) {
            let mut c = child.lock().await;
            // `kill` failing means the child is already dead or the
            // OS refused; either way the subprocess is no longer
            // ours to manage. Log so a stuck process doesn't go
            // silent.
            if let Err(e) = c.kill().await {
                tracing::warn!(
                    target: "weft_dispatcher::listener",
                    pod = pod_name,
                    error = %e,
                    "child kill failed (likely already exited)"
                );
            }
        }
        Ok(())
    }
}

/// k8s backend: applies a Deployment + Service in the pooled-tier
/// namespace and resolves the admin URL via cluster DNS. Listener has
/// no public surface; only the dispatcher reaches it.
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
    async fn spawn(&self, pod_name: &str, namespace: &str) -> Result<ListenerHandle> {
        let admin_url = format!("http://{pod_name}.{namespace}.svc.cluster.local:8080");
        let manifest =
            render_listener_manifest(pod_name, namespace, &self.listener_image, &self.broker_url);
        self.kube.apply_yaml(&manifest).await?;
        self.kube
            .wait_rollout_status(namespace, pod_name, 120)
            .await?;
        wait_for_health(&admin_url).await?;
        Ok(ListenerHandle { admin_url })
    }

    async fn stop(&self, pod_name: &str, namespace: &str) -> Result<()> {
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
                pod_name,
                weft_platform_traits::DeleteOpts::wait(),
            )
            .await?;
        self.kube
            .delete_named(
                namespace,
                "deployment",
                pod_name,
                weft_platform_traits::DeleteOpts::wait_cascade(),
            )
            .await?;
        Ok(())
    }
}

/// Mint a fresh listener pod name. Pooled listeners are not tied to a
/// tenant, so the name is just a unique k8s-safe id.
fn mint_pod_name() -> String {
    format!("listener-{}", Uuid::new_v4().simple())
}

fn render_listener_manifest(name: &str, namespace: &str, image: &str, broker_url: &str) -> String {
    // The pod-level isolation comes from the `pooled-listener`
    // NetworkPolicy in the control-plane namespace (in
    // deploy/k8s/system-namespace.yaml; selects `weft.dev/role=listener`
    // there). Here we only render the Deployment + Service.
    //
    // Auth: the broker validates the projected SA token mounted at
    // /var/run/weft/sa/token. The audience claim is `weft-broker`.
    // The pod runs as `weft-listener-sa`, which the broker maps to the
    // Listener role (a TRUSTED control-plane role: a pooled listener
    // may fire held events for any tenant, validated per-fire against
    // the signal's real tenant). `WEFT_POD_NAME` (the literal Deployment
    // name, injected as a plain env value below, NOT a downward-API
    // fieldRef, see the env block) is the placement key: the pod
    // rehydrates `signal WHERE listener_pod = this name` on restart.
    format!(
        r#"apiVersion: apps/v1
kind: Deployment
metadata:
  name: {name}
  namespace: {namespace}
  labels:
    app: {name}
    weft.dev/role: listener
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
            # The listener's placement identity. This MUST be the
            # Deployment name (`{name}`), the same string the dispatcher
            # mints, writes to `signal.listener_pod`, and uses as the
            # Service DNS host. We deliberately do NOT use
            # `fieldRef: metadata.name` here: that resolves to the
            # auto-generated POD name (`{name}-<rs-hash>-<rand>`), which
            # placement never writes, so a restarted listener would
            # rehydrate `WHERE listener_pod = <pod-name>` and find ZERO
            # signals, silently dropping every Timer/SSE it held. The
            # literal Deployment name keeps placement, resolution, and
            # rehydrate on one consistent key.
            - name: WEFT_POD_NAME
              value: "{name}"
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
    let body = resp
        .text()
        .await
        .unwrap_or_else(|e| format!("<body read failed: {e}>"));
    anyhow::bail!("listener {route} returned {status}: {body}")
}

pub async fn register_signal(
    handle: &ListenerHandle,
    token: &str,
    tenant_id: &str,
    spec: &SignalSpec,
    node_id: &str,
    is_resume: bool,
    color: Option<&str>,
    placement_generation: i64,
) -> Result<(weft_core::primitive::SignalRouting, Value)> {
    let client = reqwest::Client::new();
    let url = format!("{}/register", handle.admin_url.trim_end_matches('/'));
    // Use the typed wire struct so a new required field (e.g. tenant_id)
    // is a compile error here, not a runtime deserialize failure on the
    // listener.
    let req = weft_listener::protocol::RegisterRequest {
        token: token.to_string(),
        tenant_id: tenant_id.to_string(),
        spec: spec.clone(),
        node_id: node_id.to_string(),
        is_resume,
        color: color.map(str::to_string),
        placement_generation,
    };
    let resp = client.post(&url).json(&req).send().await?;
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

/// Tell a listener pod to reconcile its in-memory registry with the
/// durable signal table (the signals placed on it). Idempotent.
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

async fn load_report(handle: &ListenerHandle) -> Result<weft_listener::protocol::LoadReport> {
    let client = reqwest::Client::new();
    let url = format!("{}/load", handle.admin_url.trim_end_matches('/'));
    let resp = client.get(&url).send().await?;
    let resp = bail_unless_ok(resp, "/load").await?;
    Ok(resp.json::<weft_listener::protocol::LoadReport>().await?)
}

// =============================================================
// Pod registry schema
// =============================================================

pub async fn migrate(pool: &PgPool) -> Result<()> {
    // The registry of live listener pods. Keyed by pod (the placement
    // target), NOT tenant: a pooled listener holds many tenants'
    // signals. `owner_pod_id` + `leased_until_unix` say which
    // dispatcher pod is authoritative for this listener's lifecycle.
    // `grace_until_unix` is the spawn grace: until it passes, the idle
    // reaper leaves the pod alone even with zero signals placed, so a
    // freshly-spawned pod is not torn down in the window before its
    // first placement row is written.
    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS listener_pod (
            pod_name          TEXT PRIMARY KEY,
            admin_url         TEXT NOT NULL,
            namespace         TEXT NOT NULL,
            owner_pod_id      TEXT NOT NULL,
            leased_until_unix BIGINT NOT NULL,
            grace_until_unix  BIGINT NOT NULL
        )"#,
    )
    .execute(pool)
    .await
    .context("create listener_pod table")?;
    Ok(())
}

// =============================================================
// ListenerPool: load-based placement
// =============================================================

/// Where pooled listener pods run + how to spawn them. The placement
/// namespace is the pooled tier's shared namespace (the dispatcher's
/// own namespace by default); a listener serves many tenants so it does
/// not live in any one tenant's namespace.
#[derive(Clone)]
pub struct ListenerPool {
    /// Namespace the pooled listeners run in.
    namespace: String,
}

impl ListenerPool {
    pub fn new(namespace: String) -> Self {
        Self { namespace }
    }

    /// Resolve the listener pod currently holding `token`'s signal, if
    /// any. Reads `signal.listener_pod -> listener_pod.admin_url`.
    /// Returns `None` when the signal is unplaced or its holder is gone.
    pub async fn resolve_signal(
        &self,
        token: &str,
        pg_pool: &PgPool,
    ) -> Result<Option<ListenerHandle>> {
        let row: Option<(Option<String>,)> =
            sqlx::query_as("SELECT listener_pod FROM signal WHERE token = $1")
                .bind(token)
                .fetch_optional(pg_pool)
                .await?;
        let Some((Some(pod_name),)) = row else {
            return Ok(None);
        };
        Ok(self.pod_handle(&pod_name, pg_pool).await?)
    }

    /// The admin handle for a named live pod, or None if its registry
    /// row is gone (the pod was reaped). Public alias for callers that
    /// hold a pod name directly (e.g. register rollback).
    pub async fn resolve_pod(
        &self,
        pod_name: &str,
        pg_pool: &PgPool,
    ) -> Result<Option<ListenerHandle>> {
        self.pod_handle(pod_name, pg_pool).await
    }

    /// The admin handle for a named live pod, or None if its registry
    /// row is gone (the pod was reaped).
    async fn pod_handle(&self, pod_name: &str, pg_pool: &PgPool) -> Result<Option<ListenerHandle>> {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT admin_url FROM listener_pod WHERE pod_name = $1")
                .bind(pod_name)
                .fetch_optional(pg_pool)
                .await?;
        Ok(row.map(|(admin_url,)| ListenerHandle { admin_url }))
    }

    /// Resolve the live holder of `token`, RE-PLACING the signal from
    /// its durable row if no holder is live (the prior holder was
    /// reaped while the signal sat idle, e.g. a parked webhook trigger).
    /// This is the fire-path equivalent of the old respawn-on-fire: a
    /// fire must always find a listener, never silently drop. Fails loud
    /// if the signal row is gone (a fire for a token with no durable
    /// signal is a real inconsistency, not something to swallow).
    pub async fn ensure_placed_handle(
        &self,
        token: &str,
        backend: &dyn ListenerBackend,
        pg_pool: &PgPool,
        pod_id: &str,
    ) -> Result<ListenerHandle> {
        if let Some(handle) = self.resolve_signal(token, pg_pool).await? {
            return Ok(handle);
        }
        // No live holder: re-place under the per-token lock. The lock
        // serializes this against a concurrent drain / fire re-place of
        // the same token, and we DOUBLE-CHECK resolve under it: a sibling
        // re-placement that finished while we waited for the lock means
        // the signal is already live and we just reuse its holder (no
        // second placement, no generation churn).
        let key = crate::lease::advisory_key(crate::lease::SIGNAL_PLACEMENT_DOMAIN, token);
        let placed = crate::lease::with_advisory_lock_blocking(pg_pool, key, || async {
            if let Some(handle) = self.resolve_signal(token, pg_pool).await? {
                return Ok(handle);
            }
            let (_pod_name, _generation, handle) = self
                .replace_onto_new_pod(token, backend, pg_pool, pod_id, None)
                .await
                .with_context(|| {
                    format!(
                        "fire for token '{token}' could not place a listener \
                         (re-registering from its durable signal row)"
                    )
                })?;
            Ok(handle)
        })
        .await?;
        Ok(placed)
    }

    /// Re-place `token`'s signal (read from its durable `signal` row) onto
    /// a freshly-picked pod (optionally excluding `exclude`, the pod being
    /// drained): reserve the next generation, register on the new pod,
    /// AND write the holder (`set_placement`) as one unit, returning the
    /// chosen pod name + generation + handle.
    ///
    /// MUST be called while holding the per-token placement lock
    /// (`SIGNAL_PLACEMENT_DOMAIN`, scope = token). That lock is what makes
    /// the sequence safe: it serializes concurrent re-placements of the
    /// same token so the holder column always ends up pointing at the pod
    /// registered under the HIGHEST reserved generation. Without it, two
    /// re-placements could interleave reserve / register / set_placement
    /// and leave the holder under a LOWER generation than a still-live
    /// pod, which the broker's stale-fire fence (row gen == max live
    /// holder gen) depends on never happening. The single re-place path
    /// shared by the fire re-placement (`ensure_placed_handle`) and the
    /// scale-down drain.
    async fn replace_onto_new_pod(
        &self,
        token: &str,
        backend: &dyn ListenerBackend,
        pg_pool: &PgPool,
        pod_id: &str,
        exclude: Option<&str>,
    ) -> Result<(String, i64, ListenerHandle)> {
        let row: Option<(String, String, String, bool, Option<String>)> = sqlx::query_as(
            "SELECT tenant_id, node_id, spec_json, is_resume, color \
             FROM signal WHERE token = $1",
        )
        .bind(token)
        .fetch_optional(pg_pool)
        .await?;
        let Some((tenant_id, node_id, spec_json, is_resume, color)) = row else {
            anyhow::bail!(
                "token '{token}' has no durable signal row; \
                 cannot place a listener for a signal that does not exist"
            );
        };
        let spec: SignalSpec = serde_json::from_str(&spec_json)
            .with_context(|| format!("parse spec_json for re-placing signal {token}"))?;
        // The next generation (current + 1), a PURE READ. It is committed
        // only by `set_placement` at the end of this method; if register
        // fails before then, the row's generation is untouched so the
        // still-live old holder keeps firing correctly. Safe to read-then-
        // write because we hold the per-token placement lock.
        let generation = next_generation(pg_pool, token).await?;
        let token_owned = token.to_string();
        let (pod_name, handle) = self
            .place_signal_excluding(backend, pg_pool, pod_id, exclude, move |handle| {
                let spec = spec.clone();
                let node_id = node_id.clone();
                let tenant_id = tenant_id.clone();
                let color = color.clone();
                async move {
                    register_signal(
                        &handle,
                        &token_owned,
                        &tenant_id,
                        &spec,
                        &node_id,
                        is_resume,
                        color.as_deref(),
                        generation,
                    )
                    .await?;
                    Ok(handle)
                }
            })
            .await?;
        // Write the holder + generation together, under the lock, so the
        // row reflects this (highest) generation's pod.
        set_placement(pg_pool, token, &pod_name, generation).await?;
        Ok((pod_name, generation, handle))
    }

    /// Place a signal on a listener: pick the least-loaded non-saturated
    /// pod (or spawn a fresh one), run `register` against it, and on
    /// success record `signal.listener_pod = pod`. The placement write
    /// is the caller's responsibility AFTER its own `signal_insert`
    /// (the signal row must exist before we point a holder at it); this
    /// returns the chosen pod so the caller stamps it.
    ///
    /// `register` runs the actual `/register` POST (and any follow-up
    /// the caller needs) against the chosen pod's handle. If it fails,
    /// the spawn (if we spawned) is left for the reaper (an empty pod is
    /// reaped on the next idle sweep); we do not leak a placement row
    /// because we never wrote one.
    pub async fn place_signal<R, F, Fut>(
        &self,
        backend: &dyn ListenerBackend,
        pg_pool: &PgPool,
        pod_id: &str,
        register: F,
    ) -> Result<(String, R)>
    where
        F: FnOnce(ListenerHandle) -> Fut,
        Fut: std::future::Future<Output = Result<R>>,
    {
        self.place_signal_excluding(backend, pg_pool, pod_id, None, register)
            .await
    }

    /// Like `place_signal`, but never places onto `exclude` (a pod being
    /// drained on scale-down). `exclude = None` is the normal path. The
    /// exclusion keeps a draining pod's own signals from being re-picked
    /// back onto it; it is a placement filter, not a separate code path.
    pub async fn place_signal_excluding<R, F, Fut>(
        &self,
        backend: &dyn ListenerBackend,
        pg_pool: &PgPool,
        pod_id: &str,
        exclude: Option<&str>,
        register: F,
    ) -> Result<(String, R)>
    where
        F: FnOnce(ListenerHandle) -> Fut,
        Fut: std::future::Future<Output = Result<R>>,
    {
        let pod = self.pick_or_spawn(backend, pg_pool, pod_id, exclude).await?;
        // The chosen pod 503s `/register` if it saturated between our
        // load read and now; that surfaces as an error here and the
        // task framework retries placement (a fresh pick, possibly a
        // spawn). No silent overload.
        let r = register(pod.handle()).await?;
        Ok((pod.pod_name, r))
    }

    /// Pick the least-loaded non-saturated live pod, or spawn a fresh
    /// one when all are saturated / none exist. Serialized cluster-wide
    /// against a cold-start thundering herd (a burst of concurrent
    /// placements each spawning its own listener) by a TRANSACTION-scoped
    /// Postgres advisory lock, TRY-locked so a loser never blocks:
    ///   - The winner takes the lock, picks-or-spawns (spawn waits for
    ///     the new pod's health THEN inserts its registry row, so the
    ///     pod is live before the lock releases), then the lock drops
    ///     with the transaction.
    ///   - A loser fails the try-lock (`with_advisory_lock` returns
    ///     `None`), waits briefly, and retries `pick_or_spawn` from the
    ///     top, by which point the winner's pod is live and gets reused.
    /// At most one spawn is ever in flight. The lock lives in Postgres,
    /// so this holds across N dispatcher replicas; being transaction-
    /// scoped it is released the instant the transaction ends, INCLUDING
    /// a panic unwind (sqlx's `Transaction::drop` rolls back), so a panic
    /// mid-spawn cannot orphan the lock on a recycled pooled connection.
    async fn pick_or_spawn(
        &self,
        backend: &dyn ListenerBackend,
        pg_pool: &PgPool,
        pod_id: &str,
        exclude: Option<&str>,
    ) -> Result<ListenerPod> {
        let key = crate::lease::advisory_key(crate::lease::LISTENER_POOL_DOMAIN, "placement");
        loop {
            // Fast path: an existing live non-saturated pod needs no
            // lock at all (the common steady-state case).
            if let Some(pod) = self.pick_live(pg_pool, exclude).await? {
                return Ok(pod);
            }
            // No live pod: contend for the right to spawn one. Under the
            // lock, re-check (another winner may have spawned between our
            // pick and our lock), then pick-or-spawn.
            let outcome = crate::lease::with_advisory_lock(pg_pool, key, || async {
                if let Some(pod) = self.pick_live(pg_pool, exclude).await? {
                    return Ok(pod);
                }
                self.spawn_pod(backend, pg_pool, pod_id).await
            })
            .await?;
            match outcome {
                Some(pod) => return Ok(pod),
                // A sibling holds the spawn lock; back off and retry the
                // pick, by which point its pod should be live.
                None => {
                    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                    continue;
                }
            }
        }
    }

    /// Pick the least-loaded non-saturated live pod, or `None` if there
    /// is none (so the caller spawns one under the placement lock).
    /// `exclude` (a pod being drained) is never chosen.
    async fn pick_live(
        &self,
        pg_pool: &PgPool,
        exclude: Option<&str>,
    ) -> Result<Option<ListenerPod>> {
        let pods = self.live_pods(pg_pool).await?;
        // Read each pod's load; keep the non-saturated ones, pick the
        // least loaded. A pod that fails to answer /load is treated as
        // unavailable for placement (it may be mid-restart); it is not
        // chosen, and the reaper / lease expiry handles a truly dead
        // pod. We never place onto a pod we cannot confirm has room.
        let mut best: Option<(u32, ListenerPod)> = None;
        for pod in pods {
            if Some(pod.pod_name.as_str()) == exclude {
                continue;
            }
            match load_report(&pod.handle()).await {
                Ok(load) if !load.saturated => {
                    let signals = load.signals;
                    if best.as_ref().map_or(true, |(b, _)| signals < *b) {
                        best = Some((signals, pod));
                    }
                }
                Ok(_) => {} // saturated: skip
                Err(e) => {
                    tracing::warn!(
                        target: "weft_dispatcher::listener",
                        pod = %pod.pod_name,
                        error = %e,
                        "listener /load failed; not a placement candidate this round"
                    );
                }
            }
        }
        Ok(best.map(|(_, pod)| pod))
    }

    /// Spawn a fresh listener pod and register it. The lease is armed
    /// at insert so a sibling dispatcher does not adopt it immediately.
    async fn spawn_pod(
        &self,
        backend: &dyn ListenerBackend,
        pg_pool: &PgPool,
        pod_id: &str,
    ) -> Result<ListenerPod> {
        let pod_name = mint_pod_name();
        let handle = backend.spawn(&pod_name, &self.namespace).await?;
        let now = crate::lease::now_unix();
        sqlx::query(
            "INSERT INTO listener_pod \
             (pod_name, admin_url, namespace, owner_pod_id, leased_until_unix, grace_until_unix) \
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(&pod_name)
        .bind(&handle.admin_url)
        .bind(&self.namespace)
        .bind(pod_id)
        .bind(now + crate::lease::LEASE_DURATION_SECS)
        .bind(now + crate::lease::SPAWN_GRACE_SECS)
        .execute(pg_pool)
        .await
        .context("insert listener_pod row")?;
        Ok(ListenerPod {
            pod_name,
            admin_url: handle.admin_url,
        })
    }

    /// All listener pods whose lease is live.
    async fn live_pods(&self, pg_pool: &PgPool) -> Result<Vec<ListenerPod>> {
        let now = crate::lease::now_unix();
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT pod_name, admin_url FROM listener_pod WHERE leased_until_unix >= $1",
        )
        .bind(now)
        .fetch_all(pg_pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|(pod_name, admin_url)| ListenerPod { pod_name, admin_url })
            .collect())
    }

    /// Live pods that are also PAST their spawn grace (established pool
    /// members). The scale-down planner reads these so a freshly-spawned
    /// pod is never a consolidation candidate while still warming up.
    async fn established_pods(&self, pg_pool: &PgPool) -> Result<Vec<ListenerPod>> {
        let now = crate::lease::now_unix();
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT pod_name, admin_url FROM listener_pod \
             WHERE leased_until_unix >= $1 AND grace_until_unix < $1",
        )
        .bind(now)
        .fetch_all(pg_pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|(pod_name, admin_url)| ListenerPod { pod_name, admin_url })
            .collect())
    }

    /// Tell every pod holding any of `project_id`'s signals to
    /// reconcile its registry (used by activate, after TriggerSetup, so
    /// resume signals that survived a deactivate-park come back). A
    /// signal whose holder is gone (NULL `listener_pod`) is re-placed by
    /// the next register/fire; here we only nudge live holders.
    pub async fn rehydrate_project(
        &self,
        project_id: &str,
        pg_pool: &PgPool,
    ) -> Result<()> {
        let pods: Vec<(String,)> = sqlx::query_as(
            "SELECT DISTINCT listener_pod FROM signal \
             WHERE project_id = $1 AND listener_pod IS NOT NULL",
        )
        .bind(project_id)
        .fetch_all(pg_pool)
        .await?;
        for (pod_name,) in pods {
            if let Some(handle) = self.pod_handle(&pod_name, pg_pool).await? {
                rehydrate(&handle).await?;
            }
        }
        Ok(())
    }

    /// Best-effort bulk unregister: for each token, resolve its holder
    /// and POST `/unregister` (drop the in-RAM registry entry only).
    /// Does NOT touch `signal.listener_pod` or delete the `signal` row;
    /// what the caller does with the durable rows is the caller's choice:
    ///   - delete/cancel callers (delete_signals, project-delete, cancel)
    ///     delete the `signal` rows themselves, which removes the
    ///     placement with them;
    ///   - the hibernate/park caller deliberately KEEPS the rows (the DB
    ///     is canonical; reactivate re-rehydrates them), clearing only the
    ///     in-RAM registry.
    /// Either way this function's job is solely the in-RAM unregister.
    /// Holders already reaped are skipped (their registry is gone; the
    /// durable signal row is the source of truth).
    pub async fn unregister_many(
        &self,
        pg_pool: &PgPool,
        signals: &[crate::journal::SignalRegistration],
    ) {
        for sig in signals {
            let handle = match self.resolve_signal(&sig.token, pg_pool).await {
                Ok(h) => h,
                Err(e) => {
                    tracing::warn!(
                        target: "weft_dispatcher::listener",
                        token = %sig.token,
                        error = %e,
                        "resolve_signal failed during unregister sweep; token may remain registered"
                    );
                    continue;
                }
            };
            if let Some(handle) = handle {
                if let Err(e) = unregister_signal(&handle, &sig.token).await {
                    tracing::warn!(
                        target: "weft_dispatcher::listener",
                        token = %sig.token,
                        error = %e,
                        "unregister_signal failed (sweep); listener pod may carry stale state"
                    );
                }
            }
        }
    }

    /// True iff `project_id` has at least one signal with a live holder.
    /// Replaces the old per-tenant `is_alive`; used by status endpoints
    /// to render "listener: running" for a project.
    pub async fn project_has_live_listener(
        &self,
        project_id: &str,
        pg_pool: &PgPool,
    ) -> Result<bool> {
        let row: Option<(i64,)> = sqlx::query_as(
            "SELECT COUNT(*) FROM signal s \
             JOIN listener_pod lp ON lp.pod_name = s.listener_pod \
             WHERE s.project_id = $1",
        )
        .bind(project_id)
        .fetch_optional(pg_pool)
        .await?;
        Ok(row.map_or(false, |(n,)| n > 0))
    }

    /// Renew the lease on every listener pod this dispatcher owns. The
    /// main loop calls this on a heartbeat so a live pod is not adopted
    /// by a sibling dispatcher.
    pub async fn renew_owned(&self, pg_pool: &PgPool, pod_id: &str) -> Result<()> {
        sqlx::query(
            "UPDATE listener_pod SET leased_until_unix = $1 WHERE owner_pod_id = $2",
        )
        .bind(crate::lease::now_unix() + crate::lease::LEASE_DURATION_SECS)
        .bind(pod_id)
        .execute(pg_pool)
        .await?;
        Ok(())
    }

    /// Reaper hook: reap every listener pod that holds ZERO signals.
    /// Per-pod idle reap (replaces the old per-tenant idle check). A pod
    /// holding even one signal is kept (something still needs it).
    /// Adopt-on-expiry: a pod whose owning dispatcher died (lease
    /// lapsed) is adopted before being reaped, so a sibling cleans it.
    pub async fn reap_idle(
        &self,
        backend: &dyn ListenerBackend,
        pg_pool: &PgPool,
        pod_id: &str,
    ) -> Result<()> {
        // Candidate pods: those with no signals placed on them, AND
        // past their spawn grace (a freshly-spawned pod inside its grace
        // has zero placements only because its first placement has not
        // landed yet; reaping it there is the mid-setup race). The LEFT
        // JOIN + IS NULL finds pods absent from the signal placement set.
        // Restrict to pods we own OR whose lease lapsed (adopt then
        // reap), so two dispatchers do not both reap one.
        let now = crate::lease::now_unix();
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT lp.pod_name, lp.namespace \
             FROM listener_pod lp \
             LEFT JOIN signal s ON s.listener_pod = lp.pod_name \
             WHERE s.token IS NULL \
               AND lp.grace_until_unix < $2 \
               AND (lp.owner_pod_id = $1 OR lp.leased_until_unix < $2)",
        )
        .bind(pod_id)
        .bind(now)
        .fetch_all(pg_pool)
        .await?;
        for (pod_name, namespace) in rows {
            // Claim the pod (take ownership + a short lease) so a
            // sibling does not also reap it. The UPDATE ... RETURNING
            // re-checks BOTH the idle condition and the grace under the
            // row lock, so a placement (or a re-spawn that re-armed the
            // grace) between the scan and the claim aborts the reap.
            let claimed: Option<(String,)> = sqlx::query_as(
                "UPDATE listener_pod SET owner_pod_id = $1, leased_until_unix = $2 \
                 WHERE pod_name = $3 \
                   AND grace_until_unix < $4 \
                   AND NOT EXISTS (SELECT 1 FROM signal WHERE listener_pod = $3) \
                 RETURNING pod_name",
            )
            .bind(pod_id)
            .bind(now + crate::lease::LEASE_DURATION_SECS)
            .bind(&pod_name)
            .bind(now)
            .fetch_optional(pg_pool)
            .await?;
            if claimed.is_none() {
                // A signal got placed on it (or it re-armed its grace)
                // between the scan and the claim, or a sibling claimed
                // it. Leave it.
                continue;
            }
            // Tear down the pod, then delete its registry row. A
            // backend.stop failure is logged but does not block the row
            // delete: a dangling pod with no registry row is caught by
            // a later k8s sweep, whereas a registry row pointing at a
            // half-dead pod would mis-route placement.
            if let Err(e) = backend.stop(&pod_name, &namespace).await {
                tracing::warn!(
                    target: "weft_dispatcher::listener",
                    pod = %pod_name,
                    namespace = %namespace,
                    error = %e,
                    "backend.stop failed during listener reap; deleting registry row anyway"
                );
            }
            sqlx::query("DELETE FROM listener_pod WHERE pod_name = $1")
                .bind(&pod_name)
                .execute(pg_pool)
                .await?;
        }
        Ok(())
    }

    /// Read every ESTABLISHED live pod's memory pressure via `GET /load`,
    /// for the scale-down planner. Pods still in their spawn grace are
    /// excluded: a fresh pod is not yet a stable pool member and must not
    /// be a consolidation candidate (draining it would re-place nothing
    /// useful and it could not be reaped until its grace passed). A pod
    /// that fails to answer is omitted (mid-restart / unreachable; the
    /// lease reaper handles a truly dead one).
    async fn pod_loads(&self, pg_pool: &PgPool) -> Result<Vec<weft_platform_traits::PoolPodLoad>> {
        let mut out = Vec::new();
        for pod in self.established_pods(pg_pool).await? {
            match load_report(&pod.handle()).await {
                Ok(load) => out.push(weft_platform_traits::PoolPodLoad {
                    pod_name: pod.pod_name,
                    mem_pressure: load.mem_pressure,
                }),
                Err(e) => tracing::warn!(
                    target: "weft_dispatcher::listener",
                    pod = %pod.pod_name,
                    error = %e,
                    "listener /load failed; excluded from scale-down planning this cycle"
                ),
            }
        }
        Ok(out)
    }

    /// Scale-down: drain AT MOST ONE pod per call (one per cycle so we
    /// never thrash). Reads each live pod's memory pressure, asks the
    /// shared `plan_memory_scaledown` whether the pool has excess
    /// capacity, and if so re-places the chosen pod's signals onto the
    /// OTHER pods (never back onto the drain target), then lets the empty
    /// pod be reaped. A held connection is never dropped: each signal is
    /// re-registered on its new holder BEFORE its
    /// old registration is removed.
    ///
    /// Re-placement reuses the normal placement path (`place_signal_
    /// excluding`), so a survivor that saturated mid-drain 503s and the
    /// signal lands elsewhere or spawns a pod, exactly like a first
    /// placement. If re-placement of any signal fails, we STOP draining
    /// this pod (leave it live, holding its remaining signals) rather
    /// than half-drain it: a partially-drained pod still serves its
    /// signals, and the next cycle retries from a clean read.
    pub async fn drain_one(
        &self,
        backend: &dyn ListenerBackend,
        pg_pool: &PgPool,
        pod_id: &str,
    ) -> Result<()> {
        // Serialize consolidation cluster-wide: two dispatchers planning
        // off near-identical loads would otherwise pick the same target
        // (or two targets that drain onto each other). Skip this cycle if
        // a sibling already holds the lock; the next sweep retries.
        crate::lease::with_scaledown_lock(pg_pool, "listener", || {
            self.drain_one_locked(backend, pg_pool, pod_id)
        })
        .await
        .map(|_| ())
    }

    /// The body of `drain_one`, run under the scale-down lock.
    async fn drain_one_locked(
        &self,
        backend: &dyn ListenerBackend,
        pg_pool: &PgPool,
        pod_id: &str,
    ) -> Result<()> {
        let loads = self.pod_loads(pg_pool).await?;
        let Some(target) = weft_platform_traits::plan_memory_scaledown(
            &loads,
            weft_platform_traits::SATURATION_MEM_FRACTION,
        ) else {
            return Ok(());
        };
        let target = target.as_str();
        // The signals currently placed on the drain target. Re-place each
        // onto another pod, then unregister it from the target.
        let signals: Vec<(String,)> =
            sqlx::query_as("SELECT token FROM signal WHERE listener_pod = $1")
                .bind(target)
                .fetch_all(pg_pool)
                .await?;
        let old_handle = self.pod_handle(target, pg_pool).await?;
        for (token,) in &signals {
            // Re-place onto a fresh pod (excluding the drain target),
            // UNDER the per-token lock so a concurrent fire-path re-place
            // of the same token cannot interleave and leave the holder
            // under a lower generation than a still-live pod. The lock
            // covers reserve -> register -> set_placement as one unit
            // (inside `replace_onto_new_pod`). The bumped generation
            // fences the OLD pod's lingering fire (old generation), so it
            // is rejected by the broker even before we unregister it.
            let key = crate::lease::advisory_key(
                crate::lease::SIGNAL_PLACEMENT_DOMAIN,
                token,
            );
            let placed = crate::lease::with_advisory_lock_blocking(pg_pool, key, || async {
                self.replace_onto_new_pod(token, backend, pg_pool, pod_id, Some(target))
                    .await
            })
            .await;
            if let Err(e) = placed {
                tracing::warn!(
                    target: "weft_dispatcher::listener",
                    token = %token,
                    drain_target = %target,
                    error = %e,
                    "re-place during drain failed; leaving pod live with remaining signals, retry next cycle"
                );
                return Ok(());
            }
            // The signal is live on its new holder (set_placement ran
            // under the lock). Remove the old registration. A failure here
            // only leaves a stale in-RAM entry on a pod we are about to
            // tear down, so it is logged, not fatal.
            if let Some(handle) = &old_handle {
                if let Err(e) = unregister_signal(handle, token).await {
                    tracing::warn!(
                        target: "weft_dispatcher::listener",
                        token = %token,
                        drain_target = %target,
                        error = %e,
                        "unregister from drain target failed; pod is being torn down anyway"
                    );
                }
            }
        }
        // The target now holds zero signals (all re-placed). Reap it via
        // the shared idle path so the claim-then-teardown logic is not
        // duplicated.
        self.reap_idle(backend, pg_pool, pod_id).await
    }
}

/// Clear the placement holder on a set of signal tokens (set
/// `signal.listener_pod = NULL`). Used after a bulk unregister so a
/// later fire re-places the signal instead of routing to a stale pod.
pub async fn clear_placement(pg_pool: &PgPool, tokens: &[String]) -> Result<()> {
    if tokens.is_empty() {
        return Ok(());
    }
    sqlx::query("UPDATE signal SET listener_pod = NULL WHERE token = ANY($1)")
        .bind(tokens)
        .execute(pg_pool)
        .await?;
    Ok(())
}

/// Generation for the first-ever placement of a token that has no
/// signal row yet (a brand-new entry token, or a per-suspension resume
/// token: both insert fresh). Used as the reserve result when the row
/// does not exist; `set_placement` then stamps it onto the row the
/// caller is about to insert.
const FIRST_PLACEMENT_GENERATION: i64 = 1;

/// The generation the NEXT placement of `token` will hold the signal
/// under: the row's current generation + 1, or `FIRST_PLACEMENT_GENERATION`
/// if no row exists yet (a brand-new token whose `signal_insert` runs
/// after this). A PURE READ: it does NOT mutate the row.
///
/// Mutating eagerly would be a bug. The generation is committed ONLY by
/// the final holder write (`set_placement`, or `signal_insert` on the
/// fresh path), so a re-placement that FAILS before that write leaves the
/// row's generation untouched and the still-live old holder keeps firing
/// under a generation the row still matches (no spurious fence, no lost
/// fire). Safe as a read-then-write because both re-placement callers
/// hold the per-token placement lock (`SIGNAL_PLACEMENT_DOMAIN`) across
/// read -> register -> write, serializing concurrent re-placements of one
/// token; and the fresh-register path has a unique, dedup'd token with no
/// concurrent placer. Called BEFORE register so the register call carries
/// the generation the pod stamps on its fires.
pub async fn next_generation(pg_pool: &PgPool, token: &str) -> Result<i64> {
    let row: Option<(i64,)> =
        sqlx::query_as("SELECT placement_generation FROM signal WHERE token = $1")
            .bind(token)
            .fetch_optional(pg_pool)
            .await?;
    Ok(row.map(|(g,)| g + 1).unwrap_or(FIRST_PLACEMENT_GENERATION))
}

/// Record the chosen holder + its generation for a re-placed signal in
/// ONE write, so the row's `listener_pod` and `placement_generation` are
/// never observed out of step (the fire path reads both together; the
/// broker's stale-fire fence depends on them agreeing). This is the FIRST
/// and ONLY mutation of the generation for a re-placement (the read in
/// `next_generation` does not write), so if the re-placement fails before
/// this call the row is untouched. Called under the per-token lock, so
/// the `generation` passed (computed by `next_generation` under the same
/// lock) is still the current+1 at write time.
pub async fn set_placement(
    pg_pool: &PgPool,
    token: &str,
    pod_name: &str,
    generation: i64,
) -> Result<()> {
    sqlx::query(
        "UPDATE signal SET listener_pod = $1, placement_generation = $2 WHERE token = $3",
    )
    .bind(pod_name)
    .bind(generation)
    .bind(token)
    .execute(pg_pool)
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The listener's placement identity (`WEFT_POD_NAME`) MUST be the
    /// literal Deployment name the dispatcher minted, the same string it
    /// writes to `signal.listener_pod` and uses as the Service DNS host.
    /// It must NOT be `fieldRef: metadata.name`, which resolves to the
    /// auto-generated POD name and would make a restarted listener
    /// rehydrate zero signals (placement is keyed by Deployment name).
    /// This invariant is invisible to fakes (k8s is faked at layers
    /// 1-3), so pin it here.
    #[test]
    fn listener_manifest_pod_name_is_deployment_name_not_fieldref() {
        let yaml = render_listener_manifest(
            "listener-abc123",
            "weft-system",
            "weft-listener:local",
            "http://broker:9090",
        );
        // The env var carries the literal Deployment name.
        assert!(
            yaml.contains("name: WEFT_POD_NAME"),
            "WEFT_POD_NAME env missing:\n{yaml}"
        );
        assert!(
            yaml.contains("value: \"listener-abc123\""),
            "WEFT_POD_NAME must be the literal Deployment name:\n{yaml}"
        );
        // The Deployment/Service name, the DNS host, and the placement
        // key are all this one string; a fieldRef pod-name would break
        // rehydrate. Guard against a regression re-introducing it.
        assert!(
            !yaml.contains("fieldPath: metadata.name"),
            "WEFT_POD_NAME must not be a fieldRef pod-name:\n{yaml}"
        );
    }

    // Scale-down headroom math is the shared, memory-based
    // `weft_platform_traits::plan_memory_scaledown`, tested next to its
    // definition (mem_pressure.rs). The listener's drain_one just feeds
    // it per-pod memory pressure read from each pod's /load.

    /// The Service selector must match the pod template's `app` label so
    /// the admin URL (Service DNS) actually routes to the pod. A
    /// mismatch makes every listener unreachable, an e2e-only failure.
    #[test]
    fn listener_manifest_service_selector_matches_pod() {
        let yaml = render_listener_manifest("listener-x", "weft-system", "img", "url");
        // Both the Deployment selector and the Service selector key on
        // `app: <name>`, which the pod template carries.
        assert!(yaml.contains("app: listener-x"));
        assert!(yaml.contains("weft.dev/role: listener"));
    }
}
