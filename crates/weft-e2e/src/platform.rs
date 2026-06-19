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
//! compiled into any shipped image, so NONE of this exists in a real
//! deployment, local or cloud.
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
            "SELECT pod_name, project_id, status, last_heartbeat_unix, terminal_at_unix \
             FROM worker_pod WHERE project_id = $1 ORDER BY created_at_unix DESC",
        )
        .bind(project_id.to_string())
        .fetch_all(&self.pool)
        .await
        .context("query worker_pod rows for project")?;
        Ok(rows)
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
    pub status: String,
    pub last_heartbeat_unix: i64,
    pub terminal_at_unix: Option<i64>,
}

/// Backdate slack: push a timestamp this much PAST a reaper's threshold so the
/// reaper's `>=` comparison is unambiguously satisfied despite clock skew
/// between the host and the dispatcher.
const SLACK_SECS: i64 = 5;

/// Host wall clock as unix seconds. The dispatcher's reapers compare against
/// their own `now_unix()`; the host and cluster share real time on a local
/// kind setup, so a host-computed cutoff is valid for the dispatcher's check.
fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock is past UNIX_EPOCH")
        .as_secs() as i64
}
