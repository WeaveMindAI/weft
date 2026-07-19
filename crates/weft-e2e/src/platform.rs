//! The platform layer: a host-side, test-only window into what the SYSTEM does
//! underneath a running program (which worker served an execution, whether an
//! idle resource was reaped), plus the levers to drive platform behavior
//! deterministically (fake "time passed", fake a crash).
//!
//! ## Why this reaches behind the public API
//!
//! The program layer (the rest of this crate) asserts through the dispatcher's
//! public HTTP API, exactly as the outside world does. But platform facts
//! (worker-pod identity per execution, lease ownership, reap state) are NOT on
//! that surface, by design: exposing them would add privileged endpoints to the
//! shipped system, i.e. attack surface, for a need only tests have. So the
//! platform layer instead reaches BEHIND the API, the way an operator with
//! cluster credentials would: it reads the cluster's Postgres directly and
//! drives pods with `kubectl`. This is host-side TEST code only. It is compiled
//! solely under the `e2e` feature and lives in `crates/weft-e2e`, which is never
//! compiled into any shipped image, so NONE of this exists in the shipped
//! system.
//!
//! ## Faking time = database backdating, never a clock hook
//!
//! Lifecycle rules (idle-reap) are gated by wall-clock thresholds. To make a
//! resource look idle WITHOUT waiting real minutes, we shift the timestamp
//! column the reaper reads so its existing wall-clock comparison fires
//! naturally. There is no clock-freeze endpoint anywhere (not even a
//! compiled-out one), so nothing privileged ships in any form. Each backdate
//! helper imports the reaper's OWN threshold constant, so "old enough" tracks
//! the source of truth and can never rot against a hard-coded number here.
//!
//! ## Faking a crash = kill a pod from the host
//!
//! A worker crash (or a local dispatcher update: off then on) is faked with
//! `kubectl delete pod` from the host. No shipped endpoint; the test process
//! has cluster credentials a developer machine already has.

use anyhow::{Context, Result};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::PgPool;
use std::time::Duration;
use uuid::Uuid;

use crate::client::poll_until;

/// The local port the rig forwards Postgres onto. Fixed (not random) so a
/// leaked port-forward from a previous crashed run is visibly the same port,
/// and so this stays simple; the rig runs single-threaded against one cluster.
const PG_LOCAL_PORT: u16 = 55432;

/// Postgres credentials for the LOCAL kind cluster. These are the
/// well-known local-dev secret baked into `deploy/k8s/postgres.yaml`
/// (`postgres://weft:weft-local-dev@weft-postgres.weft-db:5432/weft`); they are
/// not secret in any meaningful sense (local kind only) and exist only so the
/// platform layer can reach behind the API. NOT used by any shipped code.
/// SYNC: PG_USER/PG_PASSWORD/PG_DBNAME <-> deploy/k8s/postgres.yaml (WEFT_DATABASE_URL secret)
const PG_USER: &str = "weft";
const PG_PASSWORD: &str = "weft-local-dev";
const PG_DBNAME: &str = "weft";

/// Host-side handle to the cluster's platform state. Holds a Postgres pool
/// (reached through a `kubectl port-forward` child this struct owns) plus the
/// shell-out levers. One per test process, like [`crate::client::Dispatcher`].
pub struct Platform {
    pool: PgPool,
    /// The `kubectl port-forward` child. Owned so Drop tears it down with the
    /// test process; a leaked forward would hold `PG_LOCAL_PORT` and break the
    /// next run loudly (bind failure) rather than silently connect to a stale
    /// tunnel.
    _port_forward: tokio::process::Child,
}

impl Platform {
    /// Bring up a Postgres connection to the local cluster: spawn the
    /// port-forward, wait for it, connect. Call once per test process.
    pub async fn connect() -> Result<Self> {
        let port_forward = tokio::process::Command::new("kubectl")
            .args([
                "port-forward",
                "-n",
                "weft-db",
                "svc/weft-postgres",
                &format!("{PG_LOCAL_PORT}:5432"),
            ])
            .kill_on_drop(true)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .context(
                "spawn `kubectl port-forward svc/weft-postgres`; is the cluster up \
                 (run setup.sh) and kubectl on PATH?",
            )?;

        let opts = PgConnectOptions::new()
            .host("127.0.0.1")
            .port(PG_LOCAL_PORT)
            .username(PG_USER)
            .password(PG_PASSWORD)
            .database(PG_DBNAME);

        // The port-forward needs a moment to bind. Poll the connection until it
        // succeeds rather than sleep a guessed interval; a connect failure here
        // means the forward never came up, which we surface loudly.
        let pool = poll_until(
            "postgres reachable via port-forward",
            Duration::from_secs(30),
            Duration::from_millis(500),
            || {
                let opts = opts.clone();
                async move {
                    match PgPoolOptions::new()
                        .max_connections(4)
                        .connect_with(opts)
                        .await
                    {
                        Ok(pool) => Ok(Some(pool)),
                        Err(_) => Ok(None),
                    }
                }
            },
        )
        .await
        .context("connect to cluster Postgres through the port-forward")?;

        Ok(Self {
            pool,
            _port_forward: port_forward,
        })
    }

    /// STARTUP-ONLY blanket sweep of pooled-pod CLONES left behind by
    /// EARLIER runs (a failed run preserves its clone for inspection, so
    /// the next run reaps the straggler at `ensure::up`). Matches by the
    /// `e2e` name marker, so it deletes EVERY e2e clone in the cluster.
    /// Do NOT call this on a test's success path: it would delete a
    /// concurrent test's in-flight clone if the suite ever runs tests in
    /// parallel. A passing test cleans up its OWN clone by exact name via
    /// [`Self::sweep_clone`] instead.
    ///
    /// The extra listener / supervisor pods a scale-down test stood up
    /// with [`Self::add_second_listener`] / [`Self::add_second_supervisor`].
    /// A failed run leaves its clone up (the rig preserves failure state
    /// for inspection), so the NEXT run sweeps the stragglers at startup
    /// to keep the cluster from accumulating idle pods across runs.
    ///
    /// Only e2e CLONES are touched (their names carry the `e2e` marker the
    /// clone helpers mint); a real pooled pod the dispatcher spawned is
    /// never matched. The registry row is deleted too so placement no
    /// longer sees a ghost. Deployment + Service + row, by exact name.
    pub async fn sweep_e2e_clones(&self) -> Result<()> {
        // Names of every clone we have a registry row for (listener +
        // supervisor). The row carries the namespace for the kubectl
        // delete; deleting by exact name (not a label guess) keeps the
        // sweep to e2e artifacts only.
        let listeners: Vec<(String, String)> = sqlx::query_as(
            "SELECT pod_name, namespace FROM listener_pod WHERE pod_name LIKE '%e2e%'",
        )
        .fetch_all(&self.pool)
        .await
        .context("query leftover e2e listener clones")?;
        let supervisors: Vec<(String, String)> = sqlx::query_as(
            "SELECT pod_name, namespace FROM supervisor_pod WHERE pod_name LIKE '%e2e%'",
        )
        .fetch_all(&self.pool)
        .await
        .context("query leftover e2e supervisor clones")?;

        for (pod_name, namespace) in listeners.iter().chain(supervisors.iter()) {
            // A clone is a Deployment (+ a Service for listeners). Delete
            // both by name, ignoring NotFound (a half-cleaned clone may
            // have only one). The registry row goes last so a failed
            // kubectl delete still leaves the row for a retry next run.
            for kind in ["deployment", "service"] {
                self.kubectl_delete_ignore_missing(kind, namespace, pod_name)
                    .await?;
            }
        }
        // Worker clones (`add_second_worker` mints `wp-e2e-clone-*`) are bare
        // Pods, not Deployments, and a no-infra project's teardown does NOT
        // reap shared-pool workers, so a crashed multi_worker test would leave
        // the clone Pod + its `alive` registry row behind forever, starving a
        // fresh fixture. Sweep them here too: delete the Pod by name, then the
        // row (row last so a failed kubectl delete retries next run).
        let workers: Vec<(String, String)> = sqlx::query_as(
            "SELECT pod_name, namespace FROM worker_pod WHERE pod_name LIKE '%e2e%'",
        )
        .fetch_all(&self.pool)
        .await
        .context("query leftover e2e worker clones")?;
        for (pod_name, namespace) in &workers {
            self.kubectl_delete_ignore_missing("pod", namespace, pod_name)
                .await?;
        }
        sqlx::query("DELETE FROM listener_pod WHERE pod_name LIKE '%e2e%'")
            .execute(&self.pool)
            .await
            .context("delete leftover e2e listener clone rows")?;
        sqlx::query("DELETE FROM supervisor_pod WHERE pod_name LIKE '%e2e%'")
            .execute(&self.pool)
            .await
            .context("delete leftover e2e supervisor clone rows")?;
        sqlx::query("DELETE FROM worker_pod WHERE pod_name LIKE '%e2e%'")
            .execute(&self.pool)
            .await
            .context("delete leftover e2e worker clone rows")?;
        Ok(())
    }

    /// Remove ONE clone this test created, by EXACT name: its Deployment +
    /// Service + its registry row (listener_pod or supervisor_pod,
    /// whichever holds it). This is the success-path cleanup a passing
    /// clone-test calls, scoped to its own artifact so it can never touch a
    /// concurrent test's clone (unlike the blanket startup
    /// [`Self::sweep_e2e_clones`]). The namespace comes from the registry
    /// row; if no row matches (already reaped by the dispatcher, e.g. a
    /// scale-down test whose clone was consolidated away), it is a no-op.
    pub async fn sweep_clone(&self, pod_name: &str) -> Result<()> {
        // The clone is a listener OR a supervisor; find its namespace from
        // whichever registry row exists.
        // A clone lives in exactly one of the two registries; LIMIT 1 makes
        // that explicit so `fetch_optional` can't trip on a surprise 2nd row.
        let namespace: Option<(String,)> = sqlx::query_as(
            "SELECT namespace FROM listener_pod WHERE pod_name = $1 \
             UNION ALL \
             SELECT namespace FROM supervisor_pod WHERE pod_name = $1 \
             LIMIT 1",
        )
        .bind(pod_name)
        .fetch_optional(&self.pool)
        .await
        .context("look up clone namespace for sweep_clone")?;
        if let Some((namespace,)) = namespace {
            for kind in ["deployment", "service"] {
                self.kubectl_delete_ignore_missing(kind, &namespace, pod_name)
                    .await?;
            }
        }
        // Delete the registry row(s) last (a failed kubectl delete leaves
        // the row for the startup sweep to retry). Both tables, by exact
        // name: harmless if one matches nothing.
        sqlx::query("DELETE FROM listener_pod WHERE pod_name = $1")
            .bind(pod_name)
            .execute(&self.pool)
            .await
            .context("delete clone listener_pod row")?;
        sqlx::query("DELETE FROM supervisor_pod WHERE pod_name = $1")
            .bind(pod_name)
            .execute(&self.pool)
            .await
            .context("delete clone supervisor_pod row")?;
        Ok(())
    }

    /// `kubectl delete <kind> <name> -n <ns> --ignore-not-found`. A
    /// missing resource is success (the clone's Deployment or Service may
    /// already be gone); any other failure is surfaced.
    async fn kubectl_delete_ignore_missing(
        &self,
        kind: &str,
        namespace: &str,
        name: &str,
    ) -> Result<()> {
        let out = tokio::process::Command::new("kubectl")
            .args(["delete", kind, name, "-n", namespace, "--ignore-not-found"])
            .output()
            .await
            .with_context(|| format!("kubectl delete {kind} {name}"))?;
        anyhow::ensure!(
            out.status.success(),
            "kubectl delete {kind} {name} -n {namespace} failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        Ok(())
    }

    // ---- Read surface: what the platform did underneath an execution ----
    //
    // NOTE on worker identity: a worker pod's name is DETERMINISTIC from its
    // spawn-pod task id (`wp-<project>-<task8>`), so when a crashed worker is
    // respawned the dispatcher RETRIES the same spawn-pod task and the fresh
    // pod gets the SAME name. "Distinct pod names per color" is therefore NOT a
    // resume signal (it stays 1 across a real resume). The honest crash-resume
    // signals are: the worker_pod row reached `dead` (the original process is
    // gone) AND the spawn-pod task's `attempts > 1` (a fresh process was
    // spawned) AND the execution still completed. We assert those, not names.

    /// The `worker_pod` rows for a project, newest first. Lets a test see pod
    /// status / heartbeat directly (e.g. to confirm a killed pod went `dead`).
    pub async fn worker_pods_for_project(&self, project_id: &Uuid) -> Result<Vec<WorkerPodRow>> {
        let rows: Vec<WorkerPodRow> = sqlx::query_as(
            "SELECT pod_name, project_id, namespace, status, last_heartbeat_unix, \
                    terminal_at_unix, draining, binary_hash \
             FROM worker_pod WHERE project_id = $1 ORDER BY created_at_unix DESC",
        )
        .bind(project_id.to_string())
        .fetch_all(&self.pool)
        .await
        .context("query worker_pod rows for project")?;
        Ok(rows)
    }

    /// Clone a REAL running worker pod of the project into a SECOND worker,
    /// exactly like `add_second_listener` clones a listener: kubectl-read the
    /// live pod manifest, strip runtime fields, whole-name rename, insert the
    /// `spawning` registry row (the worker's boot-time register_alive requires
    /// it), apply. The clone boots the same project binary, self-identifies
    /// via the downward-API `WEFT_POD_NAME`, registers alive, and CLAIMS from
    /// the same queue: a genuine multi-worker fleet without memory saturation
    /// (which kind cannot produce; its pods run without memory limits, so
    /// pressure always reads 0). Waits until the clone's row is `alive`.
    /// Cleanup is the NORMAL worker lifecycle (idle-exit / reconcile /
    /// project teardown); no special sweep needed.
    pub async fn add_second_worker(&self, project_id: &Uuid) -> Result<String> {
        let rows = self.worker_pods_for_project(project_id).await?;
        let src = rows
            .iter()
            .find(|p| p.status == "alive")
            .context("no alive worker pod to clone; run something on the project first")?;
        let (owner,): (String,) =
            sqlx::query_as("SELECT owner_dispatcher FROM worker_pod WHERE pod_name = $1")
                .bind(&src.pod_name)
                .fetch_one(&self.pool)
                .await
                .context("read source worker's owner")?;
        let new_name = format!("wp-e2e-clone-{}", &Uuid::new_v4().simple().to_string()[..12]);
        // Registry row FIRST; same image hash as the source so the claim
        // gate admits the clone for the project's current work.
        weft_task_store::worker_pod::insert_spawning(
            &self.pool,
            &new_name,
            &src.project_id,
            &src.namespace,
            &owner,
            &src.binary_hash,
        )
        .await
        .context("insert clone's spawning row")?;
        let manifest = self
            .clone_manifest("pod", &src.namespace, &src.pod_name, &new_name)
            .await?;
        self.kubectl_apply(&manifest).await?;
        crate::client::poll_until(
            &format!("worker clone '{new_name}' to register alive"),
            std::time::Duration::from_secs(120),
            std::time::Duration::from_millis(500),
            || async {
                let rows = self.worker_pods_for_project(project_id).await?;
                Ok(rows
                    .iter()
                    .any(|p| p.pod_name == new_name && p.status == "alive")
                    .then_some(()))
            },
        )
        .await?;
        Ok(new_name)
    }

    /// Toggle a worker pod's REAL `draining` flag. Draining is the system's
    /// own no-new-admissions mechanism (scale-down + reconcile use it); the
    /// rig flips it directly as a STEERING lever: drain every pod except the
    /// target, fire, and the new execution can only land on the target. That
    /// makes "this execution runs on THAT pod" deterministic without memory
    /// pressure. Clearing also resets `drained_at_unix` so a later genuine
    /// drain measures from its own start.
    pub async fn set_worker_draining(&self, pod_name: &str, draining: bool) -> Result<()> {
        sqlx::query(
            "UPDATE worker_pod SET draining = $2, \
             drained_at_unix = CASE WHEN $2 THEN drained_at_unix ELSE NULL END \
             WHERE pod_name = $1",
        )
        .bind(pod_name)
        .bind(draining)
        .execute(&self.pool)
        .await
        .context("toggle worker draining")?;
        Ok(())
    }

    /// The pod that currently OWNS an execution's color (stamped by the
    /// claim trigger), or None while unclaimed. Lets a multi-worker e2e
    /// assert WHICH worker picked a run up.
    pub async fn execution_owner(&self, color: &Uuid) -> Result<Option<String>> {
        let row: Option<(Option<String>,)> =
            sqlx::query_as("SELECT owner_pod_name FROM execution_color WHERE color = $1")
                .bind(color.to_string())
                .fetch_optional(&self.pool)
                .await
                .context("read execution owner")?;
        Ok(row.and_then(|(p,)| p))
    }

    /// The project's OWN k8s namespace as recorded on the `project` row,
    /// empty until infra is provisioned (and re-emptied on teardown). A
    /// no-infra project keeps this empty forever (its worker lives in the
    /// shared namespace). Lets an e2e assert "no per-project namespace was
    /// created" without a kubectl probe.
    pub async fn project_namespace(&self, project_id: &Uuid) -> Result<String> {
        let ns: Option<String> =
            sqlx::query_scalar("SELECT project_namespace FROM project WHERE id = $1")
                .bind(project_id)
                .fetch_optional(&self.pool)
                .await
                .context("query project_namespace")?;
        Ok(ns.unwrap_or_default())
    }

    /// How many times the project's `spawn_pod` task ran. A respawn after a
    /// worker death retries the SAME spawn-pod task, so `> 1` means a fresh
    /// worker process was started for this project (a crash-then-respawn).
    pub async fn spawn_attempts(&self, project_id: &Uuid) -> Result<i32> {
        let attempts: Option<i32> = sqlx::query_scalar(
            "SELECT MAX(attempts) FROM task \
             WHERE project_id = $1 AND kind = 'spawn_pod'",
        )
        .bind(project_id.to_string())
        .fetch_one(&self.pool)
        .await
        .context("query spawn_pod attempts")?;
        Ok(attempts.unwrap_or(0))
    }

    /// How many IN-FLIGHT (pending) runtime-file uploads exist under a color's
    /// exec scope. The runtime-file key is `<tenant>/exec/<color>/<id>`, and a
    /// begin reserves a 'pending' row that only flips 'active' at complete, so
    /// this counts uploads that were started but never finished. An interrupted
    /// upload that cleaned up correctly leaves ZERO: the abort deleted the row
    /// (and freed its quota reservation). Tenant-agnostic (matches any tenant
    /// prefix), so it works for the OSS `local` tenant.
    pub async fn runtime_pending_uploads_for_color(&self, color: &Uuid) -> Result<i64> {
        let pattern = format!("%/exec/{color}/%");
        let n: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM runtime_file WHERE key LIKE $1 AND status = 'pending'",
        )
        .bind(pattern)
        .fetch_one(&self.pool)
        .await
        .context("count pending runtime uploads for color")?;
        Ok(n)
    }

    /// A tenant's CHARGED runtime-storage bytes: an ACTIVE file counts by its
    /// size, an in-flight (pending) upload by its reserved bytes. This is the
    /// exact number the broker's quota check enforces, so a test can assert an
    /// interrupted upload freed its reservation (usage back to the pre-upload
    /// value) rather than leaving phantom bytes charged. The tenant is the first
    /// key segment; the OSS tenant is `local`.
    pub async fn runtime_charged_bytes(&self, tenant: &str) -> Result<i64> {
        let bytes: Option<i64> = sqlx::query_scalar(
            "SELECT COALESCE(SUM(CASE WHEN status = 'active' THEN size_bytes ELSE reserved_bytes END), 0)::BIGINT \
             FROM runtime_file WHERE tenant_id = $1",
        )
        .bind(tenant)
        .fetch_one(&self.pool)
        .await
        .context("sum tenant charged runtime bytes")?;
        Ok(bytes.unwrap_or(0))
    }

    /// The tenant a color's runtime-file rows belong to (the first key
    /// segment), or `None` if the color stored nothing. Lets a test read the
    /// charged-bytes for the exact tenant a run used without hardcoding
    /// `local`.
    pub async fn runtime_tenant_for_color(&self, color: &Uuid) -> Result<Option<String>> {
        let pattern = format!("%/exec/{color}/%");
        let tenant: Option<String> = sqlx::query_scalar(
            "SELECT tenant_id FROM runtime_file WHERE key LIKE $1 LIMIT 1",
        )
        .bind(pattern)
        .fetch_optional(&self.pool)
        .await
        .context("read tenant for color's runtime files")?;
        Ok(tenant)
    }

    // NOTE: there is deliberately no `assert_resumed_to_a_new_instance` here.
    // The obvious fingerprints (a `dead` worker_pod row, a spawn-pod
    // `attempts > 1`, a changed pod name) are all TIMING-FLAKY: a respawn reuses
    // the worker's deterministic name, the dead row is GC'd within ~seconds, and
    // depending on timing the respawn is either the same spawn-task retried
    // (attempts bumps) or a fresh spawn-task (attempts stays 1). None survives
    // as a stable post-hoc signal. The resume guarantee is instead asserted by
    // INFERENCE in the test: kill the only live worker while parked (before the
    // resume input is sent, so it provably had not finished), then assert the
    // execution completed correctly. A dead worker cannot finish a job, so a
    // fresh one must have. The reads above (`worker_pods_for_project`,
    // `spawn_attempts`) remain for tests that want to OBSERVE (not gate on)
    // worker lifecycle.

    // ---- Crash fake: kill a pod from the host (no shipped endpoint) ----

    /// Kill every `alive`/`spawning` worker pod for `project_id`, faking a
    /// worker crash. Targets each pod by its EXACT name + namespace read from
    /// `worker_pod` (not a label guess). Returns the names of the pods it
    /// ACTUALLY killed (a pod that was present and got deleted).
    ///
    /// The hard part is honesty about "pod not found". Between the row read and
    /// the delete a worker can idle-exit on its own, and THAT "gone" is fine
    /// (the goal, a dead pod, is met) and must not count as a kill nor fail the
    /// call. But a "gone" caused by a DEFECT (the row's `namespace` column
    /// drifted, so we look in the wrong namespace) MUST fail loud, and kubectl
    /// cannot tell the two apart: `delete` (and `get`) report the byte-identical
    /// `pods "<name>" not found` whether the pod genuinely exited or the
    /// namespace was wrong, and `delete` can even exit 0 when the apiserver is
    /// unreachable. So we do NOT infer absence from the delete alone. Instead we
    /// establish ground truth FIRST with a positive existence check, and treat a
    /// NotFound at EITHER the check or the delete (the worker can self-exit in
    /// the tiny window between them) as the same benign race:
    ///   - pod present and delete succeeds -> a real kill, count it.
    ///   - pod absent (or delete returns NotFound) BUT its namespace exists ->
    ///     the benign idle-exit race; skip without counting (the worker beat us).
    ///   - pod/delete absent AND its namespace is missing -> a drift defect; bail.
    ///   - any other kubectl error (RBAC, apiserver down) -> bail.
    /// An empty return means nothing live was actually killed, which a resume
    /// test treats as a setup miss: it asserts the return is non-empty so it
    /// exercises the abrupt-crash path on purpose, not an already-idle worker.
    pub async fn kill_workers(&self, project_id: &Uuid) -> Result<Vec<String>> {
        let live: Vec<(String, String)> = sqlx::query_as(
            "SELECT pod_name, namespace FROM worker_pod \
             WHERE project_id = $1 AND status IN ('spawning', 'alive')",
        )
        .bind(project_id.to_string())
        .fetch_all(&self.pool)
        .await
        .context("query live worker pods to kill")?;

        let mut killed = Vec::new();
        for (pod_name, namespace) in live {
            // The pod can self-exit at two moments we must treat as the benign
            // idle-exit race (goal "pod dead" already met): before our existence
            // check, OR in the window between that check and the delete. Both
            // surface as a NotFound and both route to `handle_absent`, which
            // tells the race apart from a real namespace-drift defect. Only the
            // delete actually killing the pod counts toward `killed`.
            if !self.resource_exists("pod", Some(&namespace), &pod_name).await? {
                self.handle_absent(&pod_name, &namespace).await?;
                continue;
            }
            let out = tokio::process::Command::new("kubectl")
                .args([
                    "delete",
                    "pod",
                    &pod_name,
                    "-n",
                    &namespace,
                    "--force",
                    "--grace-period=0",
                ])
                .output()
                .await
                .with_context(|| format!("kubectl delete pod {pod_name} -n {namespace}"))?;
            if out.status.success() {
                killed.push(pod_name);
                continue;
            }
            // Delete failed. If the pod self-exited between the check and now
            // (NotFound), that is the same benign race: route to handle_absent
            // (skip, or bail on real drift). Any other failure (apiserver
            // hiccup, RBAC) is real: fail loud rather than under-count the kill.
            let stderr = String::from_utf8_lossy(&out.stderr);
            anyhow::ensure!(
                is_notfound(&stderr),
                "kubectl delete pod {pod_name} -n {namespace} failed: {stderr}"
            );
            self.handle_absent(&pod_name, &namespace).await?;
        }
        Ok(killed)
    }

    /// Resolve a worker pod that turned out to be absent (NotFound at the
    /// existence check OR at the delete). Disambiguates the two reasons a pod can
    /// be gone: a valid namespace means the worker simply idle-exited before we
    /// killed it (the benign race, `Ok(())`, do not count it as a kill); a
    /// MISSING namespace means the `worker_pod` row's namespace column drifted (a
    /// real defect that kubectl cannot distinguish from the race on its own, so
    /// we check it explicitly and bail).
    async fn handle_absent(&self, pod_name: &str, namespace: &str) -> Result<()> {
        anyhow::ensure!(
            self.resource_exists("namespace", None, namespace).await?,
            "worker_pod row for {pod_name} names namespace {namespace}, which does not \
             exist: the namespace column has drifted (a real defect, not the idle-exit race)"
        );
        Ok(())
    }

    /// Does a kubectl resource exist? `Ok(true)` if `kubectl get` finds it,
    /// `Ok(false)` if the apiserver answers NotFound, `Err` on any other failure
    /// (RBAC, apiserver unreachable). This is the single place that draws the
    /// line between "genuinely absent" (a real apiserver NotFound answer) and "I
    /// could not find out" (an error), so callers never have to guess from a
    /// command's exit code. `namespace` is `None` for cluster-scoped resources.
    async fn resource_exists(
        &self,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<bool> {
        let mut args = vec!["get", kind, name, "-o", "name"];
        if let Some(ns) = namespace {
            args.extend(["-n", ns]);
        }
        let out = tokio::process::Command::new("kubectl")
            .args(&args)
            .output()
            .await
            .with_context(|| format!("kubectl get {kind} {name}"))?;
        if out.status.success() {
            return Ok(true);
        }
        // Non-zero: only a genuine NotFound from the apiserver means "absent".
        // Anything else (could not reach the apiserver, forbidden) is "I could
        // not find out" and must fail loud, never be read as absent.
        let stderr = String::from_utf8_lossy(&out.stderr);
        anyhow::ensure!(
            is_notfound(&stderr),
            "kubectl get {kind} {name} failed (not a NotFound): {stderr}"
        );
        Ok(false)
    }

    /// Restart the dispatcher, faking a local update (off then on). Rolls the
    /// StatefulSet so a new pod replaces the old; in-flight executions on worker
    /// pods are untouched (workers coordinate through Postgres, not a live
    /// dispatcher connection), and parked work resumes once the new dispatcher
    /// is up. Waits for the rollout to complete so a follow-on call sees a live
    /// dispatcher.
    pub async fn restart_dispatcher(&self) -> Result<()> {
        let restart = tokio::process::Command::new("kubectl")
            .args([
                "rollout",
                "restart",
                "statefulset/weft-dispatcher",
                "-n",
                "weft-system",
            ])
            .status()
            .await
            .context("kubectl rollout restart statefulset/weft-dispatcher")?;
        anyhow::ensure!(restart.success(), "dispatcher rollout restart failed");
        let wait = tokio::process::Command::new("kubectl")
            .args([
                "rollout",
                "status",
                "statefulset/weft-dispatcher",
                "-n",
                "weft-system",
            ])
            .status()
            .await
            .context("kubectl rollout status statefulset/weft-dispatcher")?;
        anyhow::ensure!(wait.success(), "dispatcher rollout did not complete");
        Ok(())
    }

    // ---- Time fake: backdate so the existing wall-clock reaper fires ----

    /// Make every `alive` worker pod for `project_id` look heartbeat-stale, so
    /// the worker-pod reaper marks it dead on its next sweep. Backdates
    /// `last_heartbeat_unix` past the reaper's OWN staleness threshold
    /// (`HEARTBEAT_STALE_SECS`) plus slack. This fakes "the worker stopped
    /// heartbeating" (a crash the pod-kill didn't catch, or a hang) without
    /// waiting the real interval.
    pub async fn make_worker_pods_stale(&self, project_id: &Uuid) -> Result<u64> {
        let cutoff = now_unix() - weft_task_store::worker_pod::HEARTBEAT_STALE_SECS - SLACK_SECS;
        let res = sqlx::query(
            "UPDATE worker_pod SET last_heartbeat_unix = $1 \
             WHERE project_id = $2 AND status = 'alive'",
        )
        .bind(cutoff)
        .bind(project_id.to_string())
        .execute(&self.pool)
        .await
        .context("backdate worker_pod heartbeats")?;
        Ok(res.rows_affected())
    }

    // ---- Listener pool: inspect placement + force a scale-down move ----

    /// Live listener pods (name + namespace) from the `listener_pod`
    /// registry, lease not expired. A pooled listener holds many signals;
    /// this is the set the dispatcher places onto and consolidates.
    pub async fn live_listener_pods(&self) -> Result<Vec<ListenerPodRow>> {
        let rows: Vec<ListenerPodRow> = sqlx::query_as(
            "SELECT pod_name, namespace FROM listener_pod \
             WHERE leased_until_unix >= $1 ORDER BY pod_name",
        )
        .bind(now_unix())
        .fetch_all(&self.pool)
        .await
        .context("query live listener_pod rows")?;
        Ok(rows)
    }

    /// The `owner_pod_id` (the dispatcher pod that manages this listener)
    /// for a listener pod. A clone inherits this so it is drained/reaped
    /// by the real dispatcher rather than stranded under a synthetic owner.
    async fn listener_pod_owner(&self, pod_name: &str) -> Result<String> {
        let (owner,): (String,) =
            sqlx::query_as("SELECT owner_pod_id FROM listener_pod WHERE pod_name = $1")
                .bind(pod_name)
                .fetch_one(&self.pool)
                .await
                .with_context(|| format!("read owner_pod_id of listener {pod_name}"))?;
        Ok(owner)
    }

    /// The `owner_pod_id` for a supervisor pod (the supervisor twin of
    /// [`Self::listener_pod_owner`]).
    async fn supervisor_pod_owner(&self, pod_name: &str) -> Result<String> {
        let (owner,): (String,) =
            sqlx::query_as("SELECT owner_pod_id FROM supervisor_pod WHERE pod_name = $1")
                .bind(pod_name)
                .fetch_one(&self.pool)
                .await
                .with_context(|| format!("read owner_pod_id of supervisor {pod_name}"))?;
        Ok(owner)
    }

    /// The signal token registered for a project's node, read straight from
    /// the registry. Unlike the consumer-facing `GET /signal-token/{tk}/signals`
    /// enumeration (which only lists `is_resume=TRUE` consumer signals: forms
    /// and human-in-the-loop resumes a person submits to), this finds ANY
    /// registered signal including ENTRY triggers (`is_resume=FALSE`: SSE,
    /// webhook, cron, fired by the outside world or a timer, never browsed by a
    /// consumer). An overlap / placement test works at the operator layer and
    /// needs the entry trigger's token, so it reads it here rather than through
    /// the consumer API that deliberately hides entry triggers. `None` if the
    /// node has no registered signal yet.
    pub async fn signal_token_for_node(
        &self,
        project_id: &Uuid,
        node_id: &str,
    ) -> Result<Option<String>> {
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT token FROM signal WHERE project_id = $1 AND node_id = $2",
        )
        .bind(project_id.to_string())
        .bind(node_id)
        .fetch_optional(&self.pool)
        .await
        .with_context(|| format!("query signal token for node '{node_id}'"))?;
        Ok(row.map(|(t,)| t))
    }

    /// The pod currently holding a signal + its placement generation, by
    /// token. `None` if the signal row is gone. Used to OBSERVE a move:
    /// after a drain, the holder changes and the generation bumps.
    pub async fn signal_placement(&self, token: &str) -> Result<Option<SignalPlacement>> {
        let row: Option<(Option<String>, i64)> = sqlx::query_as(
            "SELECT listener_pod, placement_generation FROM signal WHERE token = $1",
        )
        .bind(token)
        .fetch_optional(&self.pool)
        .await
        .context("query signal placement")?;
        Ok(row.map(|(listener_pod, generation)| SignalPlacement {
            listener_pod,
            generation,
        }))
    }

    /// Every signal token currently placed on `pod`, with its placement
    /// generation. Lets a test pick a signal to move off a pod (e.g. to
    /// load a freshly-cloned listener so it is not idle-reaped before the
    /// scenario runs).
    pub async fn signal_tokens_on_pod(&self, pod: &str) -> Result<Vec<(String, i64)>> {
        let rows: Vec<(String, i64)> = sqlx::query_as(
            "SELECT token, placement_generation FROM signal \
             WHERE listener_pod = $1 ORDER BY token",
        )
        .bind(pod)
        .fetch_all(&self.pool)
        .await
        .context("query signal tokens on pod")?;
        Ok(rows)
    }

    /// Point a signal's placement at `pod` under `generation`: the same
    /// END-STATE columns (`listener_pod` + `placement_generation`) the
    /// dispatcher writes when it moves a signal. Production computes the
    /// generation as current + 1 (`next_generation`, a pure read) and
    /// writes the holder + generation together (`set_placement`); the rig
    /// passes the next value directly, landing the identical row state.
    /// Used to construct a deterministic move (combined with
    /// `rehydrate_listener` so the target pod registers the signal through
    /// its OWN production code), since the dispatcher's load-driven drain
    /// cannot be steered from a test.
    /// SYNC: signal placement columns <-> crates/weft-dispatcher/src/listener.rs (set_placement / next_generation)
    pub async fn set_signal_placement(
        &self,
        token: &str,
        pod: &str,
        generation: i64,
    ) -> Result<()> {
        let res = sqlx::query(
            "UPDATE signal SET listener_pod = $1, placement_generation = $2 WHERE token = $3",
        )
        .bind(pod)
        .bind(generation)
        .bind(token)
        .execute(&self.pool)
        .await
        .context("set signal placement")?;
        anyhow::ensure!(
            res.rows_affected() == 1,
            "set_signal_placement updated {} rows for token {token} (expected 1; bad token?)",
            res.rows_affected()
        );
        Ok(())
    }

    /// POST a listener pod's REAL `/rehydrate` endpoint, making it
    /// reconcile its in-RAM registry with the durable `signal` table: it
    /// registers every signal `WHERE listener_pod = <this pod>` through
    /// its own production register code. Reached via a short-lived
    /// `kubectl port-forward` to the pod's Service (listeners have no
    /// public ingress), the same operator-style access the platform layer
    /// uses for Postgres. Lets a test make a chosen pod take over a
    /// signal WITHOUT the test hand-building a register request: the
    /// listener's own code reads the durable row.
    pub async fn rehydrate_listener(&self, pod: &str, namespace: &str) -> Result<()> {
        let local_port = pick_free_local_port()?;
        let mut forward = tokio::process::Command::new("kubectl")
            .args([
                "port-forward",
                "-n",
                namespace,
                &format!("svc/{pod}"),
                &format!("{local_port}:8080"),
            ])
            .kill_on_drop(true)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .with_context(|| format!("port-forward svc/{pod} for /rehydrate"))?;
        let url = format!("http://127.0.0.1:{local_port}/rehydrate");
        // The forward needs a moment to bind; poll the POST until it lands
        // (or the deadline), then tear the forward down.
        let result = poll_until(
            &format!("listener {pod} /rehydrate to succeed"),
            Duration::from_secs(20),
            Duration::from_millis(500),
            || {
                let url = url.clone();
                async move {
                    match reqwest::Client::new().post(&url).send().await {
                        Ok(resp) if resp.status().is_success() => Ok(Some(())),
                        // Forward not bound yet / transient: retry.
                        Ok(_) | Err(_) => Ok(None),
                    }
                }
            },
        )
        .await;
        let _ = forward.kill().await;
        result
    }

    /// Add a SECOND real listener pod by cloning the running one, creating
    /// the 2-pod precondition a scale-down drain needs. Placement normally
    /// spawns a second listener only under real memory saturation, which a
    /// test cannot force; an operator with cluster credentials can instead
    /// stand up another pod. This clones the live listener's Deployment +
    /// Service under a fresh name (so it runs the SAME image), inserts its
    /// `listener_pod` registry row (so the dispatcher's pool sees it), and
    /// waits for it to become ready. Returns the new pod name.
    ///
    /// The clone reuses the cluster's OWN running manifest (`kubectl get`
    /// then re-apply with a new name), so it never duplicates the
    /// dispatcher's manifest template here, it can't drift from it.
    pub async fn add_second_listener(&self) -> Result<String> {
        let existing = self.live_listener_pods().await?;
        let src = existing.first().context(
            "no live listener pod to clone; activate a triggered project first so the \
             dispatcher has spawned one",
        )?;
        let namespace = src.namespace.clone();
        let new_name = format!("listener-e2e-clone-{}", Uuid::new_v4().simple());

        // Clone the Deployment + Service: get the running manifest, rewrite
        // every occurrence of the source name to the new name, re-apply.
        // The source name appears as the metadata name, the Deployment +
        // Service selectors, and the pod label; a whole-name substitution
        // re-points all of them consistently.
        for kind in ["deployment", "service"] {
            let cloned = self
                .clone_manifest(kind, &namespace, &src.pod_name, &new_name)
                .await?;
            self.kubectl_apply(&cloned).await?;
        }

        // Register the clone in the pool so placement + drain see it. The
        // admin URL mirrors the dispatcher's cluster-DNS convention
        // (`http://<name>.<ns>.svc.cluster.local:8080`).
        // SYNC: listener admin URL <-> crates/weft-dispatcher/src/listener.rs (K8sListenerBackend::spawn)
        //
        // The clone's `owner_pod_id` is the SAME dispatcher that owns the
        // source pod, NOT a synthetic id: a pod is only drained/reaped by
        // the dispatcher that owns it (or after its lease lapses), so a
        // synthetic owner with a long lease would be un-reapable and the
        // scale-down could never consolidate the clone away. Inheriting
        // the real owner makes the clone a first-class pool member the
        // dispatcher renews + reaps like one it spawned itself.
        let owner = self.listener_pod_owner(&src.pod_name).await?;
        let admin_url = format!("http://{new_name}.{namespace}.svc.cluster.local:8080");
        // `grace_until_unix` is set to a FUTURE window (the spawn grace),
        // exactly as the dispatcher does for a pod it spawns itself. This
        // is load-bearing: the dispatcher's idle reaper deletes any
        // listener pod that is past its grace AND holds zero signals
        // (`ListenerPool::reap_idle`). A freshly-cloned pod holds zero
        // signals until the test wires it up (places a signal / rehydrates
        // it), which takes a moment (a kubectl rollout, a port-forwarded
        // /rehydrate). With grace in the PAST the idle reaper (~10s) races
        // that setup and deletes the clone mid-test, surfacing as "rollout
        // did not complete" or an overlap that silently never fires. The
        // grace window protects the clone through setup, just like a real
        // spawn; it does NOT block the scale-down tests, which reap via
        // the explicit `drain_one` path (grace-independent) once the clone
        // is a stable, signal-holding pool member.
        sqlx::query(
            "INSERT INTO listener_pod \
             (pod_name, admin_url, namespace, owner_pod_id, leased_until_unix, grace_until_unix) \
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(&new_name)
        .bind(&admin_url)
        .bind(&namespace)
        .bind(&owner)
        .bind(now_unix() + POOL_CLONE_LEASE_SECS)
        .bind(now_unix() + POOL_CLONE_GRACE_SECS)
        .execute(&self.pool)
        .await
        .context("insert cloned listener_pod registry row")?;

        // Wait for the clone's rollout so a follow-on drain re-places onto
        // a pod that can actually answer /register + /load.
        let wait = tokio::process::Command::new("kubectl")
            .args(["rollout", "status", &format!("deployment/{new_name}"), "-n", &namespace])
            .status()
            .await
            .context("kubectl rollout status for cloned listener")?;
        anyhow::ensure!(wait.success(), "cloned listener rollout did not complete");
        Ok(new_name)
    }

    /// Expire a cloned listener's spawn grace (set `grace_until_unix` to
    /// the past) so the dispatcher treats it as an ESTABLISHED pool member:
    /// eligible for the scale-down planner's consolidation. A
    /// scale-down test calls this AFTER it has wired the clone up (placed a
    /// signal on it) so the clone survived setup under the grace window but
    /// is now a genuine consolidation candidate. The move-overlap test does
    /// NOT call this: it wants the clone to stay a fresh, non-drained
    /// holder for the duration of the fire.
    pub async fn expire_listener_grace(&self, pod: &str) -> Result<()> {
        sqlx::query("UPDATE listener_pod SET grace_until_unix = $2 WHERE pod_name = $1")
            .bind(pod)
            .bind(now_unix() - SLACK_SECS)
            .execute(&self.pool)
            .await
            .context("expire cloned listener grace")?;
        Ok(())
    }

    // ---- Supervisor pool: inspect ownership + force a scale-down move ----

    /// Live supervisor pods (name + namespace) from the `supervisor_pod`
    /// registry, lease not expired. A pooled supervisor reconciles the
    /// infra of MANY projects; this is the set the dispatcher places onto
    /// and consolidates (the supervisor twin of `live_listener_pods`).
    pub async fn live_supervisor_pods(&self) -> Result<Vec<SupervisorPodRow>> {
        let rows: Vec<SupervisorPodRow> = sqlx::query_as(
            "SELECT pod_name, namespace FROM supervisor_pod \
             WHERE leased_until_unix >= $1 ORDER BY pod_name",
        )
        .bind(now_unix())
        .fetch_all(&self.pool)
        .await
        .context("query live supervisor_pod rows")?;
        Ok(rows)
    }

    /// Which supervisor pod currently OWNS a project's infra (the
    /// exclusive `infra_owner` lease), or `None` if unowned. A project is
    /// reconciled by exactly its owner; a test reads this before/after a
    /// move to confirm ownership changed hands to exactly one new pod.
    pub async fn infra_owner_of(&self, project_id: &Uuid) -> Result<Option<String>> {
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT supervisor_pod FROM infra_owner \
             WHERE project_id = $1 AND leased_until_unix >= $2",
        )
        .bind(project_id.to_string())
        .bind(now_unix())
        .fetch_optional(&self.pool)
        .await
        .context("query infra_owner")?;
        Ok(row.map(|(pod,)| pod))
    }

    /// Count of LIVE `infra_owner` leases for a project: 0 (unowned) or 1
    /// (owned). The `project_id` PRIMARY KEY makes >1 physically
    /// impossible, so this is NOT the single-actor safety check (a real
    /// double-actor bug surfaces as the owner VALUE flipping, which
    /// `infra_owner_of` + an allowed-set assertion catch). It is used to
    /// distinguish owned-vs-unowned: e.g. after a fresh pod joins, the
    /// project must still be owned (count 1), proving the newcomer did not
    /// drop or steal the existing lease.
    pub async fn infra_owner_count(&self, project_id: &Uuid) -> Result<i64> {
        let n: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM infra_owner \
             WHERE project_id = $1 AND leased_until_unix >= $2",
        )
        .bind(project_id.to_string())
        .bind(now_unix())
        .fetch_one(&self.pool)
        .await
        .context("count infra_owner leases")?;
        Ok(n)
    }

    /// How many projects a supervisor pod currently owns. Used to confirm
    /// a scale-down drain emptied the chosen pod (owns 0) before it is
    /// reaped, and that the survivor adopted the released projects.
    pub async fn projects_owned_by_supervisor(&self, pod: &str) -> Result<i64> {
        let n: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM infra_owner \
             WHERE supervisor_pod = $1 AND leased_until_unix >= $2",
        )
        .bind(pod)
        .bind(now_unix())
        .fetch_one(&self.pool)
        .await
        .context("count projects owned by supervisor")?;
        Ok(n)
    }

    /// Operator lever: move a project's exclusive `infra_owner` lease onto
    /// `pod` (the supervisor twin of `set_signal_placement`). Writes the
    /// SAME columns the broker's claim CTE writes (supervisor_pod +
    /// renewed lease), so the post-move state is one the production code
    /// genuinely produces. Used to set up a consolidation where the
    /// drained pod actually OWNS the project, so the survivor's adopt
    /// (migration) path is exercised rather than an empty pod just being
    /// reaped.
    pub async fn place_infra_owner_on(&self, project_id: &Uuid, pod: &str) -> Result<()> {
        let n = sqlx::query(
            "UPDATE infra_owner \
             SET supervisor_pod = $1, leased_until_unix = $2 \
             WHERE project_id = $3",
        )
        .bind(pod)
        .bind(now_unix() + POOL_CLONE_LEASE_SECS)
        .bind(project_id.to_string())
        .execute(&self.pool)
        .await
        .context("move infra_owner lease onto pod")?
        .rows_affected();
        anyhow::ensure!(n == 1, "expected exactly one infra_owner row to move, moved {n}");
        Ok(())
    }

    /// Add a SECOND real supervisor pod by cloning the running one, the
    /// supervisor twin of `add_second_listener`. Placement spawns a
    /// second supervisor only under real memory saturation (which a test
    /// cannot force); an operator with cluster credentials stands one up
    /// instead. The supervisor has only a Deployment (no Service: it
    /// PULLS work from the broker, it is never dialed), so this clones
    /// just the Deployment under a fresh name, inserts its
    /// `supervisor_pod` registry row, and waits for readiness. Returns
    /// the new pod name.
    ///
    /// The clone reuses the cluster's OWN running Deployment (`kubectl
    /// get` then re-apply with a new name), so it runs the same image
    /// with the same SA-token projection (its broker claim identity is
    /// `WEFT_POD_NAME`, which the manifest pins to the Deployment name),
    /// and can never drift from the dispatcher's manifest template.
    pub async fn add_second_supervisor(&self) -> Result<String> {
        let existing = self.live_supervisor_pods().await?;
        let src = existing.first().context(
            "no live supervisor pod to clone; provision a project's infra first so the \
             dispatcher has spawned one",
        )?;
        let namespace = src.namespace.clone();
        // Keep the `weft-infra-supervisor-` prefix (the reaper + pool
        // queries do not key on it, but it keeps the clone recognizable),
        // and use a SHORT random suffix: the full name is a k8s label
        // value (selector + pod label), capped at 63 bytes, and the
        // prefix alone is 22 chars, so a full 32-char uuid would overflow.
        let suffix = Uuid::new_v4().simple().to_string();
        let new_name = format!("weft-infra-supervisor-e2e-{}", &suffix[..8]);

        // Clone the Deployment only (no Service). A whole-name
        // substitution re-points the metadata name, the Deployment
        // selector, the pod label, and WEFT_POD_NAME together.
        let cloned = self
            .clone_manifest("deployment", &namespace, &src.pod_name, &new_name)
            .await?;
        self.kubectl_apply(&cloned).await?;

        // Register the clone in the pool so placement + drain + the
        // broker's ownership claim see it. The admin URL mirrors the
        // dispatcher's cluster-DNS convention (the supervisor is never
        // dialed, but the row requires the column). `owner_pod_id`
        // inherits the source pod's real owner so the clone is
        // drained/reaped by the dispatcher (see add_second_listener).
        // SYNC: supervisor admin URL <-> crates/weft-dispatcher/src/supervisor_pool.rs (K8sSupervisorBackend::spawn)
        let owner = self.supervisor_pod_owner(&src.pod_name).await?;
        let admin_url = format!("http://{new_name}.{namespace}.svc.cluster.local:8080");
        // `grace_until_unix` is a FUTURE window (the spawn grace), exactly
        // as for the cloned listener and for a pod the dispatcher spawns
        // itself. The supervisor idle reaper (`SupervisorPool::reap_idle`)
        // deletes any pod owning ZERO projects that is past its grace, and
        // a freshly-cloned supervisor owns zero projects until the test
        // calls `place_infra_owner_on`, so a past grace lets the reaper
        // race that setup and delete the clone mid-test (the same defect
        // the listener clone had). The grace protects the clone through
        // setup; the scale-down tests make it drain-eligible afterward via
        // `expire_supervisor_grace`. `draining` defaults to FALSE (the
        // dispatcher's scale-down sets it).
        sqlx::query(
            "INSERT INTO supervisor_pod \
             (pod_name, admin_url, namespace, owner_pod_id, leased_until_unix, \
              mem_pressure, grace_until_unix) \
             VALUES ($1, $2, $3, $4, $5, 0, $6)",
        )
        .bind(&new_name)
        .bind(&admin_url)
        .bind(&namespace)
        .bind(&owner)
        .bind(now_unix() + POOL_CLONE_LEASE_SECS)
        .bind(now_unix() + POOL_CLONE_GRACE_SECS)
        .execute(&self.pool)
        .await
        .context("insert cloned supervisor_pod registry row")?;

        // Wait for the clone's rollout so it can actually claim + own
        // projects (its claim loop needs a live pod + valid SA token).
        let wait = tokio::process::Command::new("kubectl")
            .args(["rollout", "status", &format!("deployment/{new_name}"), "-n", &namespace])
            .status()
            .await
            .context("kubectl rollout status for cloned supervisor")?;
        anyhow::ensure!(wait.success(), "cloned supervisor rollout did not complete");
        Ok(new_name)
    }

    /// Expire a cloned supervisor's spawn grace (the supervisor twin of
    /// `expire_listener_grace`): set `grace_until_unix` to the past so the
    /// dispatcher treats it as an ESTABLISHED pool member, eligible for the
    /// scale-down planner's consolidation (whose candidate query requires
    /// `grace_until_unix < now`). A scale-down test calls this AFTER it has
    /// wired the clone up (given it a project to own via
    /// `place_infra_owner_on`), so the clone survived setup under the grace
    /// window but is now a genuine consolidation candidate.
    pub async fn expire_supervisor_grace(&self, pod: &str) -> Result<()> {
        sqlx::query("UPDATE supervisor_pod SET grace_until_unix = $2 WHERE pod_name = $1")
            .bind(pod)
            .bind(now_unix() - SLACK_SECS)
            .execute(&self.pool)
            .await
            .context("expire cloned supervisor grace")?;
        Ok(())
    }

    /// `kubectl get <kind> <name> -n <ns> -o json`, parsed. JSON (not
    /// YAML) so the manifest can be sanitized + renamed by structural
    /// edits on a real document, never line-by-line text surgery (which
    /// cannot safely handle embedded block scalars like the
    /// `last-applied-configuration` annotation, a multi-line JSON string).
    async fn kubectl_get_json(
        &self,
        kind: &str,
        namespace: &str,
        name: &str,
    ) -> Result<serde_json::Value> {
        let out = tokio::process::Command::new("kubectl")
            .args(["get", kind, name, "-n", namespace, "-o", "json"])
            .output()
            .await
            .with_context(|| format!("kubectl get {kind} {name} -o json"))?;
        anyhow::ensure!(
            out.status.success(),
            "kubectl get {kind} {name} -o json failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        serde_json::from_slice(&out.stdout)
            .with_context(|| format!("parse `kubectl get {kind} {name} -o json` output"))
    }

    /// Build a clean clone manifest from a live resource: fetch it as
    /// JSON, strip every cluster-managed + status field, and rename it to
    /// `new_name` (which re-points the metadata name, the selector, and
    /// the pod label together). Returns the JSON string to `kubectl
    /// apply`. Used for both pooled-pod clones (listener + supervisor).
    async fn clone_manifest(
        &self,
        kind: &str,
        namespace: &str,
        src_name: &str,
        new_name: &str,
    ) -> Result<String> {
        // Wait (bounded) for the source resource to EXIST before reading it. A
        // listener pod's Deployment + Service can be transiently absent while the
        // pool churns (a scale-down deletes-then-recreates, or a just-spawned
        // source's Service hasn't landed yet), so a single-shot `get` races that
        // window and fails NotFound. Poll for existence: removes the race without
        // masking a genuinely missing resource (still fails loud past the deadline).
        let mut doc = crate::client::poll_until(
            &format!("source {kind} '{src_name}' exists to clone"),
            std::time::Duration::from_secs(30),
            std::time::Duration::from_millis(500),
            || async { Ok(self.kubectl_get_json(kind, namespace, src_name).await.ok()) },
        )
        .await?;
        sanitize_and_rename(&mut doc, src_name, new_name);
        serde_json::to_string(&doc).context("serialize cloned manifest")
    }

    /// `kubectl apply -f -` with `manifest` on stdin.
    async fn kubectl_apply(&self, manifest: &str) -> Result<()> {
        use tokio::io::AsyncWriteExt;
        let mut child = tokio::process::Command::new("kubectl")
            .args(["apply", "-f", "-"])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .context("spawn kubectl apply -f -")?;
        child
            .stdin
            .take()
            .context("kubectl apply stdin not piped")?
            .write_all(manifest.as_bytes())
            .await
            .context("write manifest to kubectl apply stdin")?;
        let out = child
            .wait_with_output()
            .await
            .context("wait for kubectl apply")?;
        anyhow::ensure!(
            out.status.success(),
            "kubectl apply failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        Ok(())
    }
}

/// Is a kubectl stderr a genuine "the apiserver answered NotFound"? kubectl
/// emits the structured `(NotFound)` token for every real not-found (verified
/// against absent-pod, wrong-namespace, and absent-namespace responses), so we
/// match that token alone. A looser `"not found"` substring would also catch
/// unrelated messages (a transient "could not find the requested resource", a
/// CRD error), risking a real failure being misread as a benign absence, which
/// is exactly the masking this whole existence-check design exists to prevent.
/// The single source of truth for the NotFound judgment, shared by the
/// existence check and the post-delete classification so they cannot diverge.
fn is_notfound(stderr: &str) -> bool {
    stderr.contains("(NotFound)")
}

/// A `worker_pod` row, as observed from the host. The rig's own projection of
/// the table (status + terminal_at, which production does not carry as struct
/// fields); distinct from `weft_task_store::worker_pod::WorkerPodRow` on purpose,
/// kept grep-linked rather than merged.
/// SYNC: WorkerPodRow <-> crates/weft-task-store/src/worker_pod.rs (WorkerPodRow + CREATE TABLE worker_pod)
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct WorkerPodRow {
    pub pod_name: String,
    pub project_id: String,
    pub namespace: String,
    pub status: String,
    pub last_heartbeat_unix: i64,
    pub terminal_at_unix: Option<i64>,
    /// The system's no-new-admissions flag (scale-down / reconcile drains).
    pub draining: bool,
    /// The image the pod was baked from (what the claim gate matches).
    pub binary_hash: String,
}

/// Backdate slack: push a timestamp this much PAST a reaper's threshold so the
/// reaper's `>=` comparison is unambiguously satisfied despite clock skew
/// between the host and the dispatcher.
const SLACK_SECS: i64 = 5;

/// Lease window for a test-cloned pooled-pod registry row (listener or
/// supervisor). Long enough that the dispatcher's reaper does not
/// adopt/reap the clone out from under a test mid-scenario, short enough
/// that a leaked clone (a crashed test) expires and gets cleaned on the
/// next sweep.
const POOL_CLONE_LEASE_SECS: i64 = 600;

/// Spawn-grace window for a test-cloned listener pod. Until it passes,
/// the dispatcher's idle reaper leaves the clone alone even while it
/// holds zero signals, so the clone survives the test's setup (rollout +
/// /rehydrate + fire) instead of being deleted mid-scenario by the ~10s
/// idle reaper. This is the SAME protection the dispatcher gives a pod it
/// spawns itself; without it both the move-overlap and scale-down tests
/// race the reaper. Generous because the setup (a kubectl rollout, a
/// port-forwarded rehydrate) can take a while under cluster load; the
/// clone is still reaped after the test via the explicit scale-down
/// drain (grace-independent) or `sweep_e2e_clones` on the next run.
const POOL_CLONE_GRACE_SECS: i64 = 300;

/// A live `listener_pod` registry row, as observed from the host.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ListenerPodRow {
    pub pod_name: String,
    pub namespace: String,
}

/// A live `supervisor_pod` registry row, as observed from the host. The
/// supervisor twin of [`ListenerPodRow`]; same shape (the clone path and
/// scale-down reads need only name + namespace).
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct SupervisorPodRow {
    pub pod_name: String,
    pub namespace: String,
}

/// Which pod holds a signal + its placement generation. After a scale-down
/// move the holder changes and the generation bumps; a test reads this
/// before/after a drain to confirm the move actually happened.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignalPlacement {
    /// The holding listener pod, or `None` if unplaced.
    pub listener_pod: Option<String>,
    pub generation: i64,
}

/// Sanitize a fetched resource document (parsed from `kubectl get -o
/// json`) so it re-applies cleanly under a new name, by STRUCTURAL edits
/// on the JSON, not text surgery:
///   - drop the whole `status` block (server-computed);
///   - drop server-assigned `metadata` fields (`resourceVersion`, `uid`,
///     `creationTimestamp`, `generation`, `managedFields`) and the
///     `annotations` (kubectl's `last-applied-configuration` + the
///     deployment-revision annotation, all cluster bookkeeping);
///   - drop the Service's live `spec.clusterIP` / `clusterIPs` so a fresh
///     Service gets its own;
///   - then rename: every occurrence of `old_name` becomes `new_name`
///     throughout the document, which re-points the metadata name, the
///     Deployment + Service selectors, the pod label, and the
///     `WEFT_POD_NAME` env together, keeping the clone internally
///     consistent.
fn sanitize_and_rename(doc: &mut serde_json::Value, old_name: &str, new_name: &str) {
    use serde_json::Value;
    doc.as_object_mut().map(|root| root.remove("status"));
    if let Some(meta) = doc.get_mut("metadata").and_then(Value::as_object_mut) {
        for k in [
            "resourceVersion",
            "uid",
            "creationTimestamp",
            "generation",
            "managedFields",
            "annotations",
        ] {
            meta.remove(k);
        }
    }
    if let Some(spec) = doc.get_mut("spec").and_then(Value::as_object_mut) {
        spec.remove("clusterIP");
        spec.remove("clusterIPs");
        // Pod-kind clones: a live pod's manifest carries its scheduling
        // outcome; a re-apply must let the scheduler place the clone.
        spec.remove("nodeName");
    }
    rename_in_place(doc, old_name, new_name);
}

/// Replace every string occurrence of `old` with `new` anywhere in a JSON
/// document (object values, array elements, nested). The resource's name
/// appears as the metadata name, selectors, the pod label, and the
/// `WEFT_POD_NAME` env value; one whole-document substitution re-points
/// them all consistently.
fn rename_in_place(v: &mut serde_json::Value, old: &str, new: &str) {
    use serde_json::Value;
    match v {
        Value::String(s) => {
            if s.contains(old) {
                *s = s.replace(old, new);
            }
        }
        Value::Array(a) => a.iter_mut().for_each(|e| rename_in_place(e, old, new)),
        Value::Object(o) => o.values_mut().for_each(|e| rename_in_place(e, old, new)),
        _ => {}
    }
}

/// Host wall clock as unix seconds. The dispatcher's reapers compare against
/// their own `now_unix()`; the host and cluster share real time on a local
/// kind setup, so a host-computed cutoff is valid for the dispatcher's check.
fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock is past UNIX_EPOCH")
        .as_secs() as i64
}

/// Grab a free local TCP port for a short-lived `kubectl port-forward`.
/// Binds to port 0 (the OS picks a free one), reads it, drops the
/// listener; a tiny TOCTOU window exists before the forward re-binds, but
/// the rig runs single-threaded against one cluster so contention is nil.
fn pick_free_local_port() -> Result<u16> {
    let listener =
        std::net::TcpListener::bind("127.0.0.1:0").context("bind ephemeral local port")?;
    let port = listener.local_addr().context("read ephemeral port")?.port();
    Ok(port)
}
