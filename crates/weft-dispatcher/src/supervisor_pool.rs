//! Pooled infra-supervisor placement. A supervisor is a long-lived,
//! TRUSTED, tenant-agnostic processor: each pod reconciles the
//! infrastructure of MANY projects (ownership is per-project, not
//! per-tenant). The dispatcher keeps at least one supervisor alive,
//! spawns a second when the pool saturates, and reaps / drains pods
//! when load drops. This is the supervisor twin of `listener.rs`.
//!
//! ## State (all in Postgres; pod-local RAM is only optimization)
//!
//! - `supervisor_pod(pod_name PK, admin_url, namespace, owner_pod_id,
//!   leased_until_unix)`: the registry of live supervisor pods. The
//!   EXACT analog of `listener_pod`, keyed by POD. `owner_pod_id` +
//!   `leased_until_unix` say which dispatcher pod is authoritative for
//!   this supervisor's lifecycle; a sibling dispatcher adopts an
//!   expired lease.
//! - `infra_owner(project_id PK, supervisor_pod, namespace, tenant_id,
//!   leased_until_unix)`: the EXCLUSIVE ownership lease. Exactly one
//!   supervisor owns a project's infra at a time, so two supervisors
//!   never run kubectl against the same project (which would corrupt
//!   it). The grain is the PROJECT (not the namespace) because a
//!   project's namespace is a pure function of (tenant, project), so
//!   the two are 1:1, and the supervisor's whole API is project-scoped.
//!
//! ## Why exclusive (unlike the listener's sticky-soft placement)
//!
//! A listener's work-item (a signal) is non-corrupting if briefly held
//! by two pods (wasteful, not wrong), so its placement is sticky-soft.
//! A supervisor's work-item (a project's infra) is STATEFUL: two
//! supervisors running apply/stop on the same infra race and corrupt
//! it. So ownership is an exclusive lease, claimed before any reconcile
//! and renewed on a heartbeat; a dead supervisor's leases expire and a
//! live, non-saturated supervisor adopts them.
//!
//! ## Load
//!
//! A supervisor's load is its self-reported MEMORY pressure (the same
//! metric the listener uses), written to the `supervisor_pod` row on each
//! ownership tick. Placement reads it from the row (no HTTP `/load`
//! surface on the supervisor; it stays a pure claim-loop). A supervisor
//! is saturated at `SATURATION_MEM_FRACTION`; the dispatcher places a new
//! project on the least-pressured non-saturated pod or spawns one.

use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use dashmap::DashMap;
use sqlx::postgres::PgPool;
use tokio::process::{Child, Command};
use tokio::sync::Mutex;
use uuid::Uuid;

/// Handle to a running supervisor: just its admin URL. Like the
/// listener, there is no bearer auth dispatcher->supervisor; the trust
/// boundary is the network (NetworkPolicy in k8s, loopback in dev). The
/// dispatcher does not currently call the supervisor over this URL (the
/// supervisor pulls work from the broker), but the spawn health-wait
/// uses it and it is kept for symmetry + future direct calls.
#[derive(Debug, Clone)]
pub struct SupervisorHandle {
    pub admin_url: String,
}

/// A live supervisor pod: its name (placement key) + admin URL.
#[derive(Debug, Clone)]
pub struct SupervisorPod {
    pub pod_name: String,
    pub admin_url: String,
}

#[async_trait]
pub trait SupervisorBackend: Send + Sync {
    /// Spawn a fresh supervisor pod named `pod_name` in `namespace`.
    /// The pod is tenant-agnostic; its identity is its own name.
    async fn spawn(&self, pod_name: &str, namespace: &str) -> Result<SupervisorHandle>;
    async fn stop(&self, pod_name: &str, namespace: &str) -> Result<()>;
}

// =============================================================
// Backends
// =============================================================

/// Local-development backend: forks the `weft-infra-supervisor` binary
/// as a child process. The supervisor has no HTTP surface, so unlike the
/// listener there is no port to pre-allocate; the admin URL is a stable
/// loopback placeholder (the dispatcher never dials it locally).
pub struct SubprocessSupervisorBackend {
    binary_path: PathBuf,
    children: Arc<DashMap<String, Arc<Mutex<Child>>>>,
}

impl SubprocessSupervisorBackend {
    pub fn new(binary_path: PathBuf) -> Self {
        Self {
            binary_path,
            children: Arc::new(DashMap::new()),
        }
    }
}

#[async_trait]
impl SupervisorBackend for SubprocessSupervisorBackend {
    async fn spawn(&self, pod_name: &str, _namespace: &str) -> Result<SupervisorHandle> {
        let broker_url = std::env::var("WEFT_BROKER_URL")
            .context("WEFT_BROKER_URL must be set for subprocess supervisor")?;
        let token_path = std::env::var("WEFT_BROKER_TOKEN_PATH").context(
            "WEFT_BROKER_TOKEN_PATH must be set for subprocess supervisor (point at a file with a valid SA token)",
        )?;
        let mut cmd = Command::new(&self.binary_path);
        cmd.env("WEFT_POD_NAME", pod_name)
            .env("WEFT_BROKER_URL", broker_url)
            .env("WEFT_BROKER_TOKEN_PATH", token_path)
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .kill_on_drop(true);
        let child = cmd
            .spawn()
            .with_context(|| format!("spawn supervisor pod {pod_name}"))?;
        self.children
            .insert(pod_name.to_string(), Arc::new(Mutex::new(child)));
        // The supervisor has no /health endpoint to poll (it is a pure
        // claim-loop). The child being spawned is the liveness signal;
        // the broker lease + reaper handle a pod that never comes up.
        Ok(SupervisorHandle {
            admin_url: format!("subprocess://{pod_name}"),
        })
    }

    async fn stop(&self, pod_name: &str, _namespace: &str) -> Result<()> {
        if let Some((_, child)) = self.children.remove(pod_name) {
            let mut c = child.lock().await;
            if let Err(e) = c.kill().await {
                tracing::warn!(
                    target: "weft_dispatcher::supervisor_pool",
                    pod = pod_name,
                    error = %e,
                    "child kill failed (likely already exited)"
                );
            }
        }
        Ok(())
    }
}

/// k8s backend: applies a Deployment in the control-plane namespace and
/// resolves the admin URL via cluster DNS. The supervisor exposes no
/// Service (it pulls work from the broker), so there is no Service to
/// render; the admin URL is the pod-name-based DNS kept for symmetry.
pub struct K8sSupervisorBackend {
    supervisor_image: String,
    broker_url: String,
    kube: Arc<dyn weft_platform_traits::KubeClient>,
}

impl K8sSupervisorBackend {
    pub fn new(
        supervisor_image: String,
        broker_url: String,
        kube: Arc<dyn weft_platform_traits::KubeClient>,
    ) -> Self {
        Self {
            supervisor_image,
            broker_url,
            kube,
        }
    }
}

#[async_trait]
impl SupervisorBackend for K8sSupervisorBackend {
    async fn spawn(&self, pod_name: &str, namespace: &str) -> Result<SupervisorHandle> {
        let admin_url = format!("http://{pod_name}.{namespace}.svc.cluster.local:8080");
        let manifest = render_supervisor_manifest(
            pod_name,
            namespace,
            &self.supervisor_image,
            &self.broker_url,
        );
        self.kube.apply_yaml(&manifest).await?;
        self.kube
            .wait_rollout_status(namespace, pod_name, 120)
            .await?;
        Ok(SupervisorHandle { admin_url })
    }

    async fn stop(&self, pod_name: &str, namespace: &str) -> Result<()> {
        // The supervisor has only a Deployment (no Service). Foreground
        // cascade so the ReplicaSet + Pod finish terminating before we
        // return, so a freshly-spawned replacement never collides with a
        // still-draining old pod on the broker's command claims.
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

/// Mint a fresh supervisor pod name. Pooled supervisors are not tied to
/// a tenant, so the name is just a unique k8s-safe id.
fn mint_pod_name() -> String {
    format!("weft-infra-supervisor-{}", Uuid::new_v4().simple())
}

// SYNC: supervisor WEFT_POD_NAME (= Deployment {name}, the infra_owner lease key) <-> crates/weft-broker-client/src/protocol.rs (Supervisor*Request.pod_name), crates/weft-infra-supervisor/src/lib.rs (SupervisorState.pod_name), crates/weft-broker-client/src/lifecycle_command.rs (owns_project_predicate)
fn render_supervisor_manifest(
    name: &str,
    namespace: &str,
    image: &str,
    broker_url: &str,
) -> String {
    // The supervisor's pod-level isolation comes from the control-plane
    // namespace NetworkPolicies (it talks only to the broker + the
    // kube-apiserver). Here we render just the Deployment; the
    // supervisor has no Service (it pulls work, it is never dialed).
    //
    // Auth, TWO tokens (unlike the listener, which has only the broker
    // one because it never touches the kube-apiserver):
    //   1. Broker auth: the projected SA token mounted at
    //      /var/run/weft/sa/token (audience `weft-broker`) authenticates
    //      claim/ownership/state calls to the broker.
    //   2. kube-apiserver auth: the supervisor reconciles infra by
    //      shelling out to `kubectl` (apply / scale / delete in tenant
    //      namespaces), so it NEEDS the DEFAULT kube-API service-account
    //      token auto-mounted at the standard path. Hence
    //      `automountServiceAccountToken: true` (the same posture as the
    //      dispatcher, which also runs kubectl); the listener disables it
    //      because it has no kubectl path. Without it, kubectl finds no
    //      in-cluster config and falls back to localhost:8080 (refused).
    // The pod runs as `weft-infra-supervisor-sa`, which the broker maps
    // to the InfraSupervisor role AND which holds the per-tenant-namespace
    // RoleBindings to `weft-infra-supervisor-clusterrole` that kubectl
    // authenticates against. `WEFT_POD_NAME` (the literal Deployment name)
    // is the pod's claim identity in the broker. `weft.dev/role:
    // infra-supervisor` is the selector the reaper + pool replica-state
    // reads key on.
    format!(
        r#"apiVersion: apps/v1
kind: Deployment
metadata:
  name: {name}
  namespace: {namespace}
  labels:
    app: {name}
    weft.dev/role: infra-supervisor
spec:
  replicas: 1
  selector:
    matchLabels:
      app: {name}
  template:
    metadata:
      labels:
        app: {name}
        weft.dev/role: infra-supervisor
    spec:
      serviceAccountName: weft-infra-supervisor-sa
      automountServiceAccountToken: true
      containers:
        - name: supervisor
          image: {image}
          imagePullPolicy: IfNotPresent
          env:
            # The supervisor's claim identity in the broker. MUST be the
            # literal Deployment name (`{name}`), the same string the
            # dispatcher mints, writes to `supervisor_pod`, and the
            # supervisor reports as its `WEFT_POD_NAME` when claiming
            # projects/commands. A `fieldRef: metadata.name` would
            # resolve to the auto-generated POD name, splitting the claim
            # identity from the placement key.
            - name: WEFT_POD_NAME
              value: "{name}"
            - name: WEFT_BROKER_URL
              value: "{broker_url}"
            - name: WEFT_BROKER_TOKEN_PATH
              value: "/var/run/weft/sa/token"
          volumeMounts:
            - name: weft-sa-token
              mountPath: /var/run/weft/sa
              readOnly: true
      volumes:
        - name: weft-sa-token
          projected:
            sources:
              - serviceAccountToken:
                  audience: weft-broker
                  expirationSeconds: 3600
                  path: token
"#,
    )
}

// =============================================================
// Pod registry schema
// =============================================================

pub async fn migrate(pool: &PgPool) -> Result<()> {
    // Registry of live supervisor pods (the placement target). Keyed by
    // pod, NOT tenant. Exact analog of `listener_pod`.
    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS supervisor_pod (
            pod_name          TEXT PRIMARY KEY,
            admin_url         TEXT NOT NULL,
            namespace         TEXT NOT NULL,
            owner_pod_id      TEXT NOT NULL,
            leased_until_unix BIGINT NOT NULL,
            -- The supervisor's last self-reported memory pressure
            -- (usage/limit, [0,1]), written on each ownership tick. The
            -- dispatcher's placement + scale-down read this (the same
            -- load metric the listener uses), so a supervisor sheds /
            -- attracts work by real pressure, not a project count. 0
            -- until the pod reports (fresh-spawned row).
            mem_pressure      DOUBLE PRECISION NOT NULL DEFAULT 0,
            -- Spawn grace: until it passes, the idle reaper leaves the
            -- pod alone even owning zero projects, so a freshly-spawned
            -- supervisor is not torn down in the window before its claim
            -- loop adopts its first project. Exact analog of
            -- `listener_pod.grace_until_unix`.
            grace_until_unix  BIGINT NOT NULL,
            -- True while this pod is being scaled DOWN: its leases have
            -- been released for re-adoption and it must claim nothing
            -- more (else its own ownership loop re-grabs what the drain
            -- just released, defeating consolidation). The broker's
            -- claim CTE excludes a draining pod; the reaper clears it
            -- with the row.
            draining          BOOLEAN NOT NULL DEFAULT FALSE
        )"#,
    )
    .execute(pool)
    .await
    .context("create supervisor_pod table")?;
    // The EXCLUSIVE ownership lease. One row per project whose infra is
    // currently owned by a supervisor. Keyed by project (1:1 with its
    // namespace), carrying the namespace + tenant the kubectl path needs.
    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS infra_owner (
            project_id        TEXT PRIMARY KEY,
            supervisor_pod    TEXT NOT NULL,
            namespace         TEXT NOT NULL,
            tenant_id         TEXT NOT NULL,
            leased_until_unix BIGINT NOT NULL
        )"#,
    )
    .execute(pool)
    .await
    .context("create infra_owner table")?;
    sqlx::query(
        r#"CREATE INDEX IF NOT EXISTS idx_infra_owner_pod
             ON infra_owner(supervisor_pod)"#,
    )
    .execute(pool)
    .await
    .context("create idx_infra_owner_pod")?;
    Ok(())
}

// =============================================================
// SupervisorPool: load-based placement with exclusive ownership
// =============================================================

/// Where pooled supervisor pods run + how to spawn them. The placement
/// namespace is the control-plane namespace (a supervisor serves many
/// tenants, so it does not live in any one tenant's namespace).
#[derive(Clone)]
pub struct SupervisorPool {
    namespace: String,
}

impl SupervisorPool {
    pub fn new(namespace: String) -> Self {
        Self { namespace }
    }

    /// Ensure at least one supervisor pod is live, spawning one if the
    /// pool is empty. Called by the infra-sync path so a project that
    /// just declared infra has a supervisor to claim it. Idempotent:
    /// a no-op when any live pod already exists. Returns the chosen /
    /// spawned pod.
    pub async fn ensure_at_least_one(
        &self,
        backend: &dyn SupervisorBackend,
        pg_pool: &PgPool,
        pod_id: &str,
    ) -> Result<SupervisorPod> {
        self.pick_or_spawn(backend, pg_pool, pod_id, None).await
    }

    /// Pick the least-loaded non-saturated live supervisor, or spawn a
    /// fresh one when all are saturated / none exist. Serialized
    /// cluster-wide against a cold-start thundering herd by a
    /// TRANSACTION-scoped Postgres advisory lock (TRY-locked + retry so a
    /// loser never blocks), exactly like the listener pool. Being
    /// transaction-scoped, the lock releases on transaction end including
    /// a panic unwind, so a panic mid-spawn cannot orphan it on a
    /// recycled pooled connection. `exclude` (a pod being drained) is
    /// never chosen.
    async fn pick_or_spawn(
        &self,
        backend: &dyn SupervisorBackend,
        pg_pool: &PgPool,
        pod_id: &str,
        exclude: Option<&str>,
    ) -> Result<SupervisorPod> {
        let key = crate::lease::advisory_key(crate::lease::SUPERVISOR_COORD_DOMAIN, "placement");
        loop {
            if let Some(pod) = self.pick_live(pg_pool, exclude).await? {
                return Ok(pod);
            }
            let outcome = crate::lease::with_advisory_lock(pg_pool, key, || async {
                if let Some(pod) = self.pick_live(pg_pool, exclude).await? {
                    return Ok(pod);
                }
                self.spawn_pod(backend, pg_pool, pod_id).await
            })
            .await?;
            match outcome {
                Some(pod) => return Ok(pod),
                None => {
                    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                    continue;
                }
            }
        }
    }

    /// Pick the least-pressured non-saturated live supervisor, or `None`.
    /// Load is the pod's self-reported `mem_pressure` (the SAME metric
    /// the listener uses), read from the `supervisor_pod` row, NOT a
    /// project count. A pod at/above `SATURATION_MEM_FRACTION` is skipped.
    /// `exclude` (a pod being drained) is skipped.
    async fn pick_live(
        &self,
        pg_pool: &PgPool,
        exclude: Option<&str>,
    ) -> Result<Option<SupervisorPod>> {
        let now = crate::lease::now_unix();
        // Least-pressured first. The supervisor reports its pressure on
        // each ownership tick; a freshly-spawned pod is at 0 until its
        // first report, so it is preferred (correctly: it is empty).
        let rows: Vec<(String, String, f64)> = sqlx::query_as(
            "SELECT pod_name, admin_url, mem_pressure \
             FROM supervisor_pod \
             WHERE leased_until_unix >= $1 AND NOT draining \
             ORDER BY mem_pressure ASC",
        )
        .bind(now)
        .fetch_all(pg_pool)
        .await?;
        for (pod_name, admin_url, pressure) in rows {
            if Some(pod_name.as_str()) == exclude {
                continue;
            }
            if weft_platform_traits::is_saturated(
                pressure,
                weft_platform_traits::SATURATION_MEM_FRACTION,
            ) {
                continue; // saturated
            }
            return Ok(Some(SupervisorPod { pod_name, admin_url }));
        }
        Ok(None)
    }

    /// Spawn a fresh supervisor pod and register it. The lease is armed
    /// at insert so a sibling dispatcher does not adopt it immediately.
    async fn spawn_pod(
        &self,
        backend: &dyn SupervisorBackend,
        pg_pool: &PgPool,
        pod_id: &str,
    ) -> Result<SupervisorPod> {
        let pod_name = mint_pod_name();
        let handle = backend.spawn(&pod_name, &self.namespace).await?;
        let now = crate::lease::now_unix();
        sqlx::query(
            "INSERT INTO supervisor_pod \
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
        .context("insert supervisor_pod row")?;
        Ok(SupervisorPod {
            pod_name,
            admin_url: handle.admin_url,
        })
    }

    /// Renew the lease on every supervisor pod this dispatcher owns. The
    /// main loop calls this on a heartbeat so a live pod is not adopted
    /// by a sibling dispatcher.
    pub async fn renew_owned(&self, pg_pool: &PgPool, pod_id: &str) -> Result<()> {
        sqlx::query("UPDATE supervisor_pod SET leased_until_unix = $1 WHERE owner_pod_id = $2")
            .bind(crate::lease::now_unix() + crate::lease::LEASE_DURATION_SECS)
            .bind(pod_id)
            .execute(pg_pool)
            .await?;
        Ok(())
    }

    /// Reaper hook: reap every supervisor pod that owns ZERO projects.
    /// Per-pod idle reap (the supervisor twin of the listener's). A pod
    /// owning even one project is kept (it is reconciling that infra);
    /// when no infra exists globally, every supervisor owns nothing and
    /// the pool drains to ZERO, with `ensure_at_least_one` on the next
    /// sync covering the cold start (the same shape as the listener pool,
    /// which also keeps no idle pods). Adopt-on-expiry: a pod whose
    /// owning dispatcher died (lease lapsed) is adopted before being
    /// reaped, so a sibling cleans it.
    pub async fn reap_idle(
        &self,
        backend: &dyn SupervisorBackend,
        pg_pool: &PgPool,
        pod_id: &str,
    ) -> Result<()> {
        let now = crate::lease::now_unix();
        // Idle candidate pods: those owning no projects AND past their
        // spawn grace (a freshly-spawned supervisor owns zero projects
        // only because its claim loop has not run yet; reaping it there
        // strands the work that was just enqueued for it). A draining
        // pod is always eligible regardless of grace (the drain
        // explicitly wants it gone). Restrict to pods we own OR whose
        // lease lapsed (adopt then reap).
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT sp.pod_name, sp.namespace \
             FROM supervisor_pod sp \
             LEFT JOIN infra_owner io ON io.supervisor_pod = sp.pod_name \
             WHERE io.project_id IS NULL \
               AND (sp.draining OR sp.grace_until_unix < $2) \
               AND (sp.owner_pod_id = $1 OR sp.leased_until_unix < $2)",
        )
        .bind(pod_id)
        .bind(now)
        .fetch_all(pg_pool)
        .await?;
        for (pod_name, namespace) in rows {
            // Claim the pod (take ownership + a short lease) gated on it
            // still owning zero projects AND still past-grace-or-draining,
            // so a sibling does not also reap it and a project claimed (or
            // a re-spawn that re-armed the grace) between scan and claim
            // aborts it.
            let claimed: Option<(String,)> = sqlx::query_as(
                "UPDATE supervisor_pod SET owner_pod_id = $1, leased_until_unix = $2 \
                 WHERE pod_name = $3 \
                   AND (draining OR grace_until_unix < $4) \
                   AND NOT EXISTS (SELECT 1 FROM infra_owner WHERE supervisor_pod = $3) \
                 RETURNING pod_name",
            )
            .bind(pod_id)
            .bind(now + crate::lease::LEASE_DURATION_SECS)
            .bind(&pod_name)
            .bind(now)
            .fetch_optional(pg_pool)
            .await?;
            if claimed.is_none() {
                continue;
            }
            if let Err(e) = backend.stop(&pod_name, &namespace).await {
                tracing::warn!(
                    target: "weft_dispatcher::supervisor_pool",
                    pod = %pod_name,
                    namespace = %namespace,
                    error = %e,
                    "backend.stop failed during supervisor reap; deleting registry row anyway"
                );
            }
            sqlx::query("DELETE FROM supervisor_pod WHERE pod_name = $1")
                .bind(&pod_name)
                .execute(pg_pool)
                .await?;
        }
        Ok(())
    }

    /// Scale-DOWN: drain AT MOST ONE supervisor per call. Reads each
    /// pod's memory pressure, asks the shared `plan_memory_scaledown`
    /// whether the pool has excess capacity, and if so RELEASES the
    /// chosen pod's project leases so other live, non-saturated
    /// supervisors adopt them, then lets the emptied pod be reaped.
    ///
    /// Releasing a lease (not handing it to a specific pod) is correct
    /// because the supervisor claim path is pull-based: a released
    /// project becomes claimable and the next non-saturated supervisor's
    /// claim loop adopts it. We do NOT kubectl anything here; we only
    /// move ownership rows. The drained pod stops reconciling a project
    /// the instant its lease row is gone (its own claim loop only acts on
    /// projects it owns).
    pub async fn drain_one(
        &self,
        backend: &dyn SupervisorBackend,
        pg_pool: &PgPool,
        pod_id: &str,
    ) -> Result<()> {
        // Serialize consolidation cluster-wide (see the listener twin):
        // two dispatchers must not drain the same supervisor or two onto
        // each other. Skip this cycle if a sibling holds the lock.
        crate::lease::with_scaledown_lock(pg_pool, "supervisor", || {
            self.drain_one_locked(backend, pg_pool, pod_id)
        })
        .await
        .map(|_| ())
    }

    /// The body of `drain_one`, run under the scale-down lock.
    async fn drain_one_locked(
        &self,
        backend: &dyn SupervisorBackend,
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
        // Mark the target draining BEFORE releasing its leases. The
        // target supervisor is still running its own ownership loop; if
        // we released first, that loop would re-claim the just-released
        // projects (its memory is unchanged) and consolidation would
        // never converge. The broker's claim CTE excludes a draining
        // pod, so once this flag is set the target claims nothing more.
        sqlx::query("UPDATE supervisor_pod SET draining = TRUE WHERE pod_name = $1")
            .bind(&target)
            .execute(pg_pool)
            .await?;
        // Release every project this pod owns so the survivors' claim
        // loops adopt them. A released project sits unowned until a
        // non-draining supervisor claims it (sub-second on an active
        // claim loop); its infra keeps running untouched in the meantime
        // (release moves only an ownership row, never kubectl).
        let released = sqlx::query("DELETE FROM infra_owner WHERE supervisor_pod = $1")
            .bind(&target)
            .execute(pg_pool)
            .await?
            .rows_affected();
        tracing::info!(
            target: "weft_dispatcher::supervisor_pool",
            drain_target = %target,
            released_projects = released,
            "supervisor scale-down: released project leases for re-adoption, reaping pod"
        );
        // The target now owns zero projects; reap it via the shared idle
        // path.
        self.reap_idle(backend, pg_pool, pod_id).await
    }

    /// Per-pod memory pressure for every live supervisor pod (the same
    /// metric the listener uses), feeding the shared scale-down planner.
    /// Reads the `supervisor_pod.mem_pressure` the supervisor reports on
    /// each ownership tick, no HTTP.
    async fn pod_loads(&self, pg_pool: &PgPool) -> Result<Vec<weft_platform_traits::PoolPodLoad>> {
        let now = crate::lease::now_unix();
        // Established pods only: a pod still in its spawn grace is not yet
        // a stable pool member and must not be a consolidation candidate.
        // Draining pods are already leaving and are excluded too.
        let rows: Vec<(String, f64)> = sqlx::query_as(
            "SELECT pod_name, mem_pressure FROM supervisor_pod \
             WHERE leased_until_unix >= $1 AND grace_until_unix < $1 AND NOT draining",
        )
        .bind(now)
        .fetch_all(pg_pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|(pod_name, mem_pressure)| weft_platform_traits::PoolPodLoad {
                pod_name,
                mem_pressure,
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Scale-down headroom math is the shared, memory-based
    // `weft_platform_traits::plan_memory_scaledown`, tested next to its
    // definition (mem_pressure.rs). The supervisor's drain_one just feeds
    // it per-pod memory pressure read from `supervisor_pod.mem_pressure`.

    /// The manifest's claim identity MUST be the literal Deployment name,
    /// not a fieldRef pod-name: the supervisor reports `WEFT_POD_NAME` as
    /// its claim id, which must equal the `supervisor_pod` placement key.
    #[test]
    fn supervisor_manifest_pod_name_is_deployment_name_not_fieldref() {
        let yaml = render_supervisor_manifest(
            "weft-infra-supervisor-abc",
            "weft-system",
            "weft-infra-supervisor:local",
            "http://broker:9090",
        );
        assert!(yaml.contains("name: WEFT_POD_NAME"));
        assert!(yaml.contains("value: \"weft-infra-supervisor-abc\""));
        assert!(!yaml.contains("fieldPath: metadata.name"));
        assert!(yaml.contains("weft.dev/role: infra-supervisor"));
    }

    /// The supervisor shells out to `kubectl`, so it MUST get the default
    /// kube-API service-account token auto-mounted; without it kubectl
    /// finds no in-cluster config and dials localhost:8080 (refused).
    /// This differs from the listener (which disables the mount because it
    /// never touches the kube-apiserver). Pin it so a copy-paste from the
    /// listener manifest can't silently reintroduce the bug.
    #[test]
    fn supervisor_manifest_automounts_kube_api_token_for_kubectl() {
        let yaml = render_supervisor_manifest(
            "weft-infra-supervisor-abc",
            "weft-system",
            "weft-infra-supervisor:local",
            "http://broker:9090",
        );
        assert!(
            yaml.contains("automountServiceAccountToken: true"),
            "supervisor needs the default kube-API token for kubectl"
        );
        assert!(
            !yaml.contains("automountServiceAccountToken: false"),
            "must not disable the kube-API token mount"
        );
        // The broker-audience token is still mounted alongside it.
        assert!(yaml.contains("audience: weft-broker"));
    }
}
