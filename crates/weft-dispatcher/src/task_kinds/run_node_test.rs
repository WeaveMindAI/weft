//! `run_node_test` task: run ONE node self-test in a short-lived
//! `weft-test-` pod and complete the task with the runner's JSON
//! output.
//!
//! The pod runs the per-package node-test image (built from
//! `weft-compiler`'s test crate; the enqueuer supplies the image ref)
//! in the shared worker pool with a WORKER's identity, end to end:
//! same namespace, same service account and projected broker token,
//! same `weft.dev/role: worker` network envelope, a `worker_pod` row
//! with `role = 'node-test'` (kept beating by this executor, so the
//! broker resolves the pod to its tenant and the reaper sees it
//! alive, while worker capacity / reconciliation / scale-down never
//! count it), and, for a LIVE run, a real journaled execution
//! (`ExecutionStarted` minted from the task id, `ExecutionCompleted`
//! on cleanup). That identity is what makes a live test's connection
//! resolution take the exact production path, including a credential
//! source that answers a relay instead of a raw key.
//!
//! Task semantics: a FAILING test is a SUCCESSFUL task (the report
//! says `passed: false`); the executor errors only when the run
//! itself could not happen (image missing, pod unschedulable, no
//! parseable report).
//!
//! Idempotency under re-claim (a surrendered lease requeues the task;
//! a lapsed one is rescued by `claim_one`): the pod name and the
//! color both derive from the task id. A harvested report is
//! persisted on the TASK row (`tasks::store_result_partial`) BEFORE
//! any cleanup; the task row outlives claims (only the hourly
//! retention sweep deletes terminal rows), so a re-claim first reads
//! it back and returns the recorded report with no pod work.
//! Otherwise it finds the prior pod by name: terminal => harvest its
//! report (never re-run: a live test spends money), running => keep
//! waiting. Spawning happens ONLY on the task's first claim
//! (`task.attempts == 1`): a re-claim that finds no pod cannot know
//! whether the prior claim's pod ran before disappearing, so it fails
//! loudly instead of ever re-spending. The `worker_pod` /
//! `execution_color` inserts are ON CONFLICT no-ops, and the reaper's
//! orphaned node-test sweep only touches the pod once the task has
//! been terminal past a full claim duration, so an in-flight claim
//! always wins over the sweep.

use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use weft_platform_traits::DeleteOpts;
use weft_task_store::executor::TaskExecutor;
use weft_task_store::tasks::Task;

use crate::backend::k8s_worker::short_project_id;
use crate::state::DispatcherState;

/// Wire tag of this kind on the task table. String-keyed (the
/// `TaskKind` enum holds only the dispatcher's own core kinds; extra
/// kinds register by string).
pub const RUN_NODE_TEST_KIND: &str = "run_node_test";

/// How often the executor re-reads the pod's phase (and beats the
/// pod's `worker_pod` heartbeat) while the test runs. No overall
/// deadline on a RUNNING test: it may legitimately run long (the
/// claim heartbeat keeps the task leased).
const POD_POLL: Duration = Duration::from_secs(2);

/// How many polls a freshly-applied pod may stay invisible before the
/// executor gives up (an apply that silently landed nowhere). This is
/// an internal service-to-service wait, not a user-controlled one, so
/// a deadline is correct here.
const NEVER_SEEN_POLL_LIMIT: u32 = 90;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunNodeTestPayload {
    pub project_id: String,
    pub tenant: String,
    /// The per-package node-test image to run (content-addressed;
    /// bare tag when loaded onto the node, registry-qualified when
    /// pulled). Minted by the enqueuer, spawned verbatim.
    pub image_ref: String,
    /// Run the binary's `list` instead of one test: the pod prints the
    /// package's whole test listing. A list run takes no node, test,
    /// or connection (nothing executes, nothing spends).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub list: bool,
    /// Node type under test. Required for a run, absent for `list`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<String>,
    /// Declared test name. Required for a run, absent for `list`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub test: Option<String>,
    /// Live tier: the connection (grant) id resolving the test's
    /// declared service. Absent for basic/fake runs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub live_connection: Option<String>,
    /// Live-test fixture variables, forwarded verbatim into the test
    /// pod's env (`WEFT_NODE_TEST_*` only; a test reads them through
    /// `LiveRig::fixture`). Values a test cannot self-provision, like
    /// a chat id the tester's bot may message.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub fixtures: std::collections::BTreeMap<String, String>,
}

/// The validated runner invocation a payload resolves to: the argv,
/// plus the `(node, test)` a RUN carries (None for a `list`). Holding
/// the resolved names here is what lets the executor label the journal
/// without re-unwrapping the payload's Options: the invariant "a run
/// has node+test" is proven ONCE, at validation, not papered over at
/// each use.
#[cfg_attr(test, derive(Debug, PartialEq))]
struct RunnerInvocation {
    args: Vec<String>,
    entry: Option<(String, String)>,
}

impl RunNodeTestPayload {
    /// The runner invocation this payload asks for, validated: a `list`
    /// takes no node/test/connection, a run needs both node and test.
    /// `color` is the task's minted execution identity, passed only on
    /// live runs.
    fn runner_invocation(&self, color: Option<&str>) -> anyhow::Result<RunnerInvocation> {
        if self.list {
            anyhow::ensure!(
                self.node.is_none() && self.test.is_none() && self.live_connection.is_none(),
                "run_node_test: a list run takes no node, test, or connection"
            );
            return Ok(RunnerInvocation { args: vec!["list".to_string()], entry: None });
        }
        let (Some(node), Some(test)) = (&self.node, &self.test) else {
            anyhow::bail!("run_node_test: a run needs both node and test");
        };
        let mut args = vec![
            "run".to_string(),
            "--node".into(),
            node.clone(),
            "--test".into(),
            test.clone(),
        ];
        if let Some(conn) = &self.live_connection {
            args.push("--live-connection".into());
            args.push(conn.clone());
        }
        if let Some(color) = color {
            args.push("--color".into());
            args.push(color.to_string());
        }
        Ok(RunnerInvocation { args, entry: Some((node.clone(), test.clone())) })
    }
}

pub struct RunNodeTestExecutor;

#[async_trait]
impl TaskExecutor<DispatcherState> for RunNodeTestExecutor {
    async fn execute(&self, state: &DispatcherState, task: &Task) -> Result<Value> {
        let payload: RunNodeTestPayload = serde_json::from_value(task.payload.clone())?;
        // Every payload string that lands inside the YAML manifest /
        // argv is gated by the charset its field actually needs:
        // `image_ref` alone may carry registry syntax (`:` `/` `@`);
        // node types, test names, grant uuids, and project ids are
        // plain identifiers. `tenant` alone goes through
        // `SafeLabel::new` at render time instead (tenant ids are not
        // charset-bounded by us).
        require_image_ref_safe(&payload.image_ref)?;
        require_identifier_safe("project_id", &payload.project_id)?;
        if let Some(node) = &payload.node {
            require_identifier_safe("node", node)?;
        }
        if let Some(test) = &payload.test {
            require_identifier_safe("test", test)?;
        }
        if let Some(conn) = &payload.live_connection {
            require_identifier_safe("live_connection", conn)?;
        }
        for (name, value) in &payload.fixtures {
            require_fixture_safe(name, value)?;
        }

        let task_short = &task.id.simple().to_string()[..8];
        let pod_name = format!(
            "weft-test-{}-{}",
            short_project_id(&payload.project_id),
            task_short
        );
        let namespace = crate::project_namespace::SHARED_WORKER_NAMESPACE;

        // The ONE color derivation: on a live run the color IS the
        // task id (one task = one run = one attribution anchor), so
        // every claim of this task, first or re-claim, lands on the
        // same color. Basic/fake runs carry none.
        let color: Option<String> =
            payload.live_connection.as_ref().map(|_| task.id.to_string());
        // Validated BEFORE anything is minted: a malformed payload
        // (a run missing its node/test, a list carrying them) must
        // fail before a color is journaled or a pod exists. The
        // resolved run entry rides along, so the journal label below
        // has no Option to unwrap.
        let RunnerInvocation { args, entry } = payload.runner_invocation(color.as_deref())?;

        // Re-claim fast path: a prior claim already harvested the
        // report and persisted it on the TASK row BEFORE its cleanup
        // (the run's outcome must never die with a task lease: a live
        // test spent money). On a hit, close the color (deduped, so a
        // prior claim's own close is a no-op here) and return the
        // recorded report. NO pod work: a leftover pod from a claim
        // that died mid-cleanup is the orphaned node-test sweep's job
        // once this task goes terminal, and a waiting pod delete here
        // would race that claim's cleanup and hold a picker slot for
        // a run whose answer is already in hand.
        if let Some(report) = weft_task_store::tasks::stored_result(&state.pg_pool, task.id).await?
        {
            close_color(state, &pod_name, color.as_deref()).await;
            return Ok(report);
        }

        // A LIVE run gets a REAL execution identity: the color IS the
        // task id, so a re-claim lands on the same color (one task =
        // one run = one attribution anchor). Journaled as a genuine
        // `ExecutionStarted` (event + `execution_color` seed in one
        // transaction, deduped so a re-claim is a no-op), stamped
        // `node_test: true`: cost attribution, broker scoping, and
        // the journal bridge's terminal cleanup treat the color like
        // any execution, while the project-lifecycle machinery
        // (cancel/wipe sweeps, drain counting, the executions list)
        // reads only `kind = 'execution'` colors and so never touches
        // it: THIS task owns the color's whole lifecycle, and the
        // `ExecutionCompleted` the cleanup writes pairs with the
        // start. A vanished project fails the write loudly (no pod,
        // no spend).
        if let Some(color_str) = &color {
            let color: weft_core::Color = task.id;
            // A color is minted only on a live RUN, and a run always
            // carries its entry (proven by `runner_invocation` above),
            // so this expect is a real invariant, not a papered-over
            // Option.
            let (node, test) = entry.as_ref().expect("a live run resolves its node+test entry");
            state
                .journal
                .record_event_dedup(
                    &weft_journal::ExecEvent::ExecutionStarted {
                        color,
                        project_id: payload.project_id.clone(),
                        entry_node: format!("node-test:{node}::{test}"),
                        phase: weft_core::context::Phase::Fire,
                        // A node test executes the task payload's
                        // content-addressed image, never a project
                        // definition; `None` makes a resume against
                        // this color fail loudly as an unknown hash.
                        definition_hash: None,
                        node_test: true,
                        at_unix: crate::lease::now_unix() as u64,
                    },
                    &format!("node_test_started:{color}"),
                )
                .await?;
            // The test pod never claims a task, so the ownership-
            // follows-claim trigger can't stamp it; appoint the pod
            // we're about to spawn as the color's driver so its
            // journal writes and storage calls pass the broker's
            // owner gate.
            if let Err(e) = weft_task_store::worker_pod::bind_color_owner(
                &state.pg_pool,
                color_str,
                &pod_name,
            )
            .await
            {
                // The start is already journaled: pair it with a
                // terminal event before bailing.
                cleanup(state, namespace, &pod_name, Some(color_str)).await;
                return Err(e);
            }
        }

        // Re-claim path: a prior claim's pod answers for us. A
        // terminal pod is harvested (never re-run: live tests spend
        // money); a running one is waited on. Spawning is reserved
        // for the task's FIRST claim: a re-claim that finds no pod
        // cannot prove the prior claim never ran it (the pod could
        // have run and been deleted mid-cleanup), and the fast path
        // above already covers every recorded outcome, so the only
        // honest answer is a loud failure that says so. Spending
        // twice on one task is never acceptable.
        // Any bail from here on funnels through `cleanup` first: the
        // journaled start above must always meet a terminal event, and
        // whatever identity rows / pod exist by then are harvested.
        let existing = match state.kube.pod_phase(namespace, &pod_name).await {
            Ok(p) => p,
            Err(e) => {
                cleanup(state, namespace, &pod_name, color.as_deref()).await;
                return Err(e.context("probe the node-test pod before spawning"));
            }
        };
        if existing.is_none() && task.attempts > 1 {
            cleanup(state, namespace, &pod_name, color.as_deref()).await;
            anyhow::bail!(
                "a prior claim of this test task left no pod and no recorded \
                 report; the test may or may not have run (and spent money), so \
                 it will not be re-run automatically. Re-run the test"
            );
        }

        // The pod's broker identity: a worker_pod row keyed by the pod
        // name, kept beating by THIS executor (the test binary is not
        // a claim-loop worker). role='node-test' keeps the row out of
        // every worker-capacity / reconciliation / scale-down query
        // while the broker still resolves it. binary_hash is NULL: the
        // column's contract is "worker binary hash", which a test pod
        // has none of (its image travels on the task payload), and
        // NULL never compares equal to a real hash. Both calls are
        // idempotent, so a lease-loss re-claim just re-runs them.
        let identity: Result<()> = async {
            weft_task_store::worker_pod::insert_spawning(
                &state.pg_pool,
                &pod_name,
                &payload.project_id,
                namespace,
                state.pod_id.as_str(),
                None,
                "node-test",
                Some(task.id),
            )
            .await?;
            weft_task_store::worker_pod::register_alive(
                &state.pg_pool,
                &pod_name,
                &payload.project_id,
                weft_task_store::worker_pod::AliveTransition::FromSpawningOrAlive,
            )
            .await
        }
        .await;
        if let Err(e) = identity {
            cleanup(state, namespace, &pod_name, color.as_deref()).await;
            return Err(e);
        }

        if existing.is_none() {
            let spawned: Result<()> = async {
                crate::shared_worker_namespace::ensure(
                    &*state.kube,
                    &crate::shared_worker_namespace::SharedWorkerNamespaceArgs {
                        pod_cidr: &state.cluster_pod_cidr,
                        service_cidr: &state.cluster_service_cidr,
                    },
                )
                .await
                .map_err(|e| anyhow::anyhow!("ensure shared worker namespace: {e}"))?;

                let manifest = render_test_pod_manifest(
                    &pod_name,
                    namespace,
                    &payload,
                    &args,
                    &state.broker_url,
                    state.sandbox.runtime_class(namespace).as_deref(),
                    state.workers.pull_secret().as_deref(),
                );
                state.kube.apply_yaml(&manifest).await
            }
            .await;
            if let Err(e) = spawned {
                cleanup(state, namespace, &pod_name, color.as_deref()).await;
                return Err(e);
            }
        }

        // Wait for the pod to finish. Pull failures surface
        // immediately (same early-bail the worker spawn does); a pod
        // that VANISHES after being seen (deleted out-of-band) is a
        // loud error, not an infinite wait. A phase read that ERRORS
        // (apiserver blip) is retried, never treated as absence, and
        // fails loudly once it stays broken past the same limit.
        let mut seen = existing.is_some();
        let mut never_seen_polls: u32 = 0;
        let mut phase_failures: u32 = 0;
        loop {
            // The waiting-reason read shares the phase read's failure
            // streak below: a persistently unreadable pod escalates
            // through the same ladder either way, and one successful
            // phase read resets it.
            match state.kube.pod_waiting_reason(namespace, &pod_name).await {
                Ok(Some(reason)) if matches!(reason.as_str(), "ImagePullBackOff" | "ErrImagePull") => {
                    cleanup(state, namespace, &pod_name, color.as_deref()).await;
                    anyhow::bail!(
                        "node-test pod {pod_name}: image {} could not be pulled \
                         (ImagePullBackOff); build and push/load the test image first, \
                         and check the registry pull credential if one is configured",
                        payload.image_ref
                    );
                }
                Ok(_) => {}
                Err(e) => {
                    phase_failures += 1;
                    if phase_failures > NEVER_SEEN_POLL_LIMIT {
                        cleanup(state, namespace, &pod_name, color.as_deref()).await;
                        return Err(e.context(format!(
                            "could not read node-test pod {pod_name}'s waiting reason for \
                             {NEVER_SEEN_POLL_LIMIT} consecutive polls; check cluster \
                             access and re-run the test"
                        )));
                    }
                    tracing::warn!(
                        target: "weft_dispatcher::run_node_test",
                        pod = %pod_name, error = %e, streak = phase_failures,
                        "node-test pod waiting-reason read failed; retrying next poll"
                    );
                    tokio::time::sleep(POD_POLL).await;
                    continue;
                }
            }
            let phase = match state.kube.pod_phase(namespace, &pod_name).await {
                Ok(p) => {
                    phase_failures = 0;
                    p
                }
                Err(e) => {
                    phase_failures += 1;
                    if phase_failures > NEVER_SEEN_POLL_LIMIT {
                        cleanup(state, namespace, &pod_name, color.as_deref()).await;
                        return Err(e.context(format!(
                            "could not read node-test pod {pod_name}'s phase for \
                             {NEVER_SEEN_POLL_LIMIT} consecutive polls; check cluster \
                             access and re-run the test"
                        )));
                    }
                    tracing::warn!(
                        target: "weft_dispatcher::run_node_test",
                        pod = %pod_name, error = %e, streak = phase_failures,
                        "node-test pod phase read failed; retrying next poll"
                    );
                    tokio::time::sleep(POD_POLL).await;
                    continue;
                }
            };
            match phase.as_deref() {
                Some("Succeeded") | Some("Failed") => break,
                Some(_) => seen = true,
                None if seen => {
                    cleanup(state, namespace, &pod_name, color.as_deref()).await;
                    anyhow::bail!(
                        "node-test pod {pod_name} vanished before finishing (deleted \
                         out-of-band?); re-run the test"
                    );
                }
                None => {
                    never_seen_polls += 1;
                    if never_seen_polls > NEVER_SEEN_POLL_LIMIT {
                        cleanup(state, namespace, &pod_name, color.as_deref()).await;
                        anyhow::bail!(
                            "node-test pod {pod_name} never became visible after apply; \
                             check the cluster and re-run the test"
                        );
                    }
                }
            }
            // Keep the pod's identity row fresh so the reaper never
            // mistakes a long test for a dead worker. `Ok(false)`
            // means the row went non-alive under us (another actor is
            // cleaning this pod up): loud, never swallowed.
            match weft_task_store::worker_pod::heartbeat(&state.pg_pool, &pod_name, 0.0).await {
                Ok(true) => {}
                Ok(false) => tracing::warn!(
                    target: "weft_dispatcher::run_node_test",
                    pod = %pod_name,
                    "the test pod's identity row is no longer alive; another actor \
                     (reaper or a concurrent cleanup) is retiring it"
                ),
                Err(e) => tracing::warn!(
                    target: "weft_dispatcher::run_node_test",
                    pod = %pod_name, error = %e,
                    "test-pod heartbeat write failed; retrying next poll"
                ),
            }
            tokio::time::sleep(POD_POLL).await;
        }

        // Harvest the report. The pod is terminal, so re-reading its
        // logs is free: a transient read failure retries on the poll
        // cadence with the same escalation ladder as the phase read,
        // never losing the run to one apiserver blip.
        let mut log_failures: u32 = 0;
        let logs = loop {
            match state.kube.pod_logs(namespace, &pod_name, "node-test").await {
                Ok(logs) => break logs,
                Err(e) => {
                    log_failures += 1;
                    if log_failures > NEVER_SEEN_POLL_LIMIT {
                        cleanup(state, namespace, &pod_name, color.as_deref()).await;
                        return Err(e.context(format!(
                            "could not read node-test pod {pod_name}'s logs for \
                             {NEVER_SEEN_POLL_LIMIT} consecutive polls; check cluster \
                             access and re-run the test"
                        )));
                    }
                    tracing::warn!(
                        target: "weft_dispatcher::run_node_test",
                        pod = %pod_name, error = %e, streak = log_failures,
                        "node-test pod log read failed; retrying next poll"
                    );
                    tokio::time::sleep(POD_POLL).await;
                }
            }
        };
        // The runner's stdout protocol (one JSON document on the last
        // line prefixed with the report sentinel) has one definition
        // (`weft_core::node_test::report_line`), shared with every
        // reader.
        let report: Option<Value> = weft_core::node_test::report_line(&logs)
            .and_then(|l| serde_json::from_str(l).ok());
        let Some(report) = report else {
            cleanup(state, namespace, &pod_name, color.as_deref()).await;
            anyhow::bail!(
                "node-test pod {pod_name} produced no JSON report; log tail: {}",
                logs.lines().rev().take(5).collect::<Vec<_>>().join(" | ")
            );
        };
        // Persist the report on the TASK row BEFORE any cleanup: once
        // the pod and its logs are gone, that row is the only copy,
        // and a re-claim returns it through the fast path at the top.
        // On a failed write the pod is deliberately KEPT (no cleanup):
        // a claim that still holds the task re-harvests the logs, and
        // once the task is terminal past the grace window the reaper's
        // orphaned node-test sweep retires the pod through the row's
        // owner_task_id.
        if let Err(e) = weft_task_store::tasks::store_result_partial(
            &state.pg_pool,
            task.id,
            state.pod_id.as_str(),
            &report,
        )
        .await
        {
            return Err(e.context(format!(
                "could not persist node-test pod {pod_name}'s report on its task row; \
                 the pod is kept so a claim of the task can re-harvest its logs"
            )));
        }
        cleanup(state, namespace, &pod_name, color.as_deref()).await;
        Ok(report)
    }
}

/// Remove the pod, retire its `worker_pod` row, and on a live run
/// close the color: a bare `execution_color` row with no terminal
/// journal event would read as a forever-non-terminal execution (the
/// wipe / cancel sweeps would chase it forever), so the color gets a
/// terminal event instead of a delete, which keeps the run's cost
/// trail addressable by project. Every exit path of the executor
/// funnels through here so nothing lingers that nobody watches; a
/// lease-loss drop mid-await is covered by the next re-claim running
/// the same exit paths. Failures here never abort the caller's own
/// outcome, but each is logged loudly with the pod name: a swallowed
/// delete failure would otherwise leave the pod for the reaper's
/// orphaned node-test sweep with no trace of why.
async fn cleanup(state: &DispatcherState, namespace: &str, pod_name: &str, color: Option<&str>) {
    // A WAITING delete: `mark_done` fences the pod out of the journal
    // (the pod-alive trigger), so the row must only flip once the
    // container is confirmed gone; a fire-and-forget delete could
    // fence a still-terminating pod's final broker write. The pod is
    // short-lived and this executor is not a sweep loop, so blocking
    // here costs nothing.
    if let Err(e) = state
        .kube
        .delete_named(namespace, "pod", pod_name, DeleteOpts::wait())
        .await
    {
        tracing::error!(
            target: "weft_dispatcher::run_node_test",
            pod = %pod_name, error = %e,
            "node-test pod delete failed during cleanup; the reaper's orphaned \
             node-test sweep will reap it once its task is terminal"
        );
    }
    if let Err(e) = weft_task_store::worker_pod::mark_done(&state.pg_pool, pod_name).await {
        tracing::error!(
            target: "weft_dispatcher::run_node_test",
            pod = %pod_name, error = %e,
            "node-test pod row mark_done failed during cleanup; the reaper's orphaned \
             node-test sweep will retire it once its task is terminal"
        );
    }
    close_color(state, pod_name, color).await;
}

/// Write the color's terminal `ExecutionCompleted` (deduped, so any
/// number of writers agree). Split from [`cleanup`] because the
/// re-claim fast path needs ONLY this: the pod work is the orphaned
/// node-test sweep's job once the task is terminal, and a fast path
/// blocking on a waiting pod delete would hold a picker slot for a
/// run whose answer is already in hand.
async fn close_color(state: &DispatcherState, pod_name: &str, color: Option<&str>) {
    if let Some(color) = color.and_then(|c| c.parse::<weft_core::Color>().ok()) {
        if let Err(e) = state
            .journal
            .record_event_dedup(
                &weft_journal::ExecEvent::ExecutionCompleted {
                    color,
                    outputs: serde_json::json!({ "nodeTest": true }),
                    at_unix: crate::lease::now_unix() as u64,
                },
                &format!("node_test_color_close:{color}"),
            )
            .await
        {
            tracing::error!(
                target: "weft_dispatcher::run_node_test",
                pod = %pod_name, color = %color, error = %e,
                "node-test color close failed; the color stays open until a \
                 re-claim re-runs it"
            );
        }
    }
}

/// Refuse an image ref that could break out of the rendered YAML /
/// argv: alphanumerics plus registry syntax (`_-./:@`), first byte
/// alphanumeric so it can never pose as a runner flag in the argv.
fn require_image_ref_safe(value: &str) -> Result<()> {
    let ok = !value.is_empty()
        && value.len() <= 200
        && value.as_bytes()[0].is_ascii_alphanumeric()
        && value.bytes().all(|b| {
            b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.' | b':' | b'/' | b'@')
        });
    if !ok {
        anyhow::bail!("run_node_test: image_ref contains characters not allowed in a pod spec");
    }
    Ok(())
}

/// Refuse a plain identifier (node type, test name, grant uuid,
/// project id) that could break out of the rendered YAML / argv:
/// alphanumerics plus `_-.` only, first byte alphanumeric so it can
/// never pose as a runner flag in the argv.
/// A fixture pair as pod-spec env: the name must be a
/// `WEFT_NODE_TEST_*` env identifier (the reserved fixture namespace,
/// so a payload can never override the pod's own WEFT_* wiring), the
/// value printable single-line text (it lands inside a quoted YAML
/// scalar; quotes/backslashes are escaped at render).
fn require_fixture_safe(name: &str, value: &str) -> Result<()> {
    let name_ok = name.starts_with("WEFT_NODE_TEST_")
        && name.len() <= 200
        && name.bytes().all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_');
    if !name_ok {
        anyhow::bail!(
            "run_node_test: fixture '{name}' is not a WEFT_NODE_TEST_* env identifier \
             (ascii uppercase/digits/underscores)"
        );
    }
    if value.is_empty() || value.len() > 4096 || value.bytes().any(|b| b.is_ascii_control()) {
        anyhow::bail!(
            "run_node_test: fixture '{name}' value must be non-empty single-line text \
             (no control characters, at most 4096 bytes)"
        );
    }
    Ok(())
}

fn require_identifier_safe(what: &str, value: &str) -> Result<()> {
    let ok = !value.is_empty()
        && value.len() <= 200
        && value.as_bytes()[0].is_ascii_alphanumeric()
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'));
    if !ok {
        anyhow::bail!("run_node_test: {what} contains characters not allowed in a pod spec");
    }
    Ok(())
}

/// The test pod: the worker pod's identity envelope (namespace, SA
/// token projection, `weft.dev/role: worker` label so the shared
/// namespace's network policies grant it the same broker + egress
/// reach) minus the worker-only machinery (no connection server, no
/// headless-service DNS), plus the runner's argv. Runs to completion,
/// never restarts.
fn render_test_pod_manifest(
    pod_name: &str,
    namespace: &str,
    payload: &RunNodeTestPayload,
    args: &[String],
    broker_url: &str,
    runtime_class: Option<&str>,
    pull_secret: Option<&str>,
) -> String {
    let project_label = crate::project_namespace::SafeLabel::new(&payload.project_id, 63);
    let tenant_label = crate::project_namespace::SafeLabel::new(&payload.tenant, 63);
    let runtime_class_line = match runtime_class {
        Some(rc) => format!("  runtimeClassName: {rc}\n"),
        None => String::new(),
    };
    // Same conditional line the worker manifest emits: a registry that
    // needs a pull credential needs it on the test pod too.
    let image_pull_secrets_line = match pull_secret {
        Some(secret) => format!("  imagePullSecrets:\n    - name: {secret}\n"),
        None => String::new(),
    };
    let args_yaml: String = args
        .iter()
        .map(|a| format!("        - \"{a}\"\n"))
        .collect();
    // Validated by `require_fixture_safe` (single-line printable text)
    // before render; quotes and backslashes still need escaping to sit
    // inside a double-quoted YAML scalar.
    let fixtures_yaml: String = payload
        .fixtures
        .iter()
        .map(|(name, value)| {
            let escaped = value.replace('\\', "\\\\").replace('"', "\\\"");
            format!("        - name: {name}\n          value: \"{escaped}\"\n")
        })
        .collect();
    format!(
        r#"apiVersion: v1
kind: Pod
metadata:
  name: {pod_name}
  namespace: {namespace}
  labels:
    weft.dev/role: worker
    weft.dev/kind: node-test
    weft.dev/tenant: "{tenant_label}"
    weft.dev/project: "{project_label}"
spec:
{runtime_class_line}{image_pull_secrets_line}  serviceAccountName: weft-worker-sa
  automountServiceAccountToken: false
  restartPolicy: Never
  containers:
    - name: node-test
      image: {image}
      imagePullPolicy: IfNotPresent
      args:
{args_yaml}      env:
        - name: WEFT_BROKER_URL
          value: "{broker_url}"
        - name: WEFT_BROKER_TOKEN_PATH
          value: "/var/run/weft/sa/token"
        - name: WEFT_POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: WEFT_TENANT_ID
          value: "{tenant_label}"
        - name: WEFT_PROJECT_ID
          value: "{project_label}"
{fixtures_yaml}      volumeMounts:
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
        image = payload.image_ref,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn payload() -> RunNodeTestPayload {
        RunNodeTestPayload {
            project_id: "p1".into(),
            tenant: "t1".into(),
            image_ref: "weft-node-tests:abc".into(),
            list: false,
            node: Some("SlackSendMessage".into()),
            test: Some("posts_a_message".into()),
            live_connection: Some("11111111-2222-3333-4444-555555555555".into()),
            fixtures: Default::default(),
        }
    }

    /// The two payload modes validate their own shape: a run needs
    /// node + test, a list refuses them (and a connection).
    #[test]
    fn runner_args_shape_is_validated() {
        let p = payload();
        let run = p.runner_invocation(None).unwrap();
        assert_eq!(
            run.args,
            vec![
                "run",
                "--node",
                "SlackSendMessage",
                "--test",
                "posts_a_message",
                "--live-connection",
                "11111111-2222-3333-4444-555555555555",
            ]
        );
        assert_eq!(
            run.entry,
            Some(("SlackSendMessage".to_string(), "posts_a_message".to_string())),
            "a run resolves its journal entry alongside the argv"
        );
        let mut run_missing = payload();
        run_missing.test = None;
        assert!(run_missing.runner_invocation(None).is_err());

        let mut list = payload();
        list.list = true;
        assert!(
            list.runner_invocation(None).is_err(),
            "list with node/test/connection refuses"
        );
        list.node = None;
        list.test = None;
        list.live_connection = None;
        let list_inv = list.runner_invocation(None).unwrap();
        assert_eq!(list_inv.args, vec!["list"]);
        assert_eq!(list_inv.entry, None, "a list has no journal entry");
    }

    /// Fixture pairs land as pod env, escaped for the quoted YAML
    /// scalar; names outside the reserved namespace and multi-line
    /// values are refused before render.
    #[test]
    fn fixtures_render_as_env_and_are_gated() {
        let mut p = payload();
        p.fixtures.insert("WEFT_NODE_TEST_TELEGRAM_CHAT_ID".into(), "123456".into());
        p.fixtures.insert("WEFT_NODE_TEST_QUOTED".into(), "a\"b\\c".into());
        let args = p.runner_invocation(None).unwrap().args;
        let pod = render_test_pod_manifest(
            "weft-test-p1-abc",
            "wft-shared-workers",
            &p,
            &args,
            "http://broker:9090",
            None,
            None,
        );
        assert!(pod.contains("- name: WEFT_NODE_TEST_TELEGRAM_CHAT_ID\n          value: \"123456\""));
        assert!(pod.contains("value: \"a\\\"b\\\\c\""), "quoted-scalar escaping");

        assert!(require_fixture_safe("WEFT_NODE_TEST_CHAT_ID", "123").is_ok());
        assert!(
            require_fixture_safe("WEFT_BROKER_URL", "x").is_err(),
            "only the reserved fixture namespace may ride: the pod's own wiring is not overridable"
        );
        assert!(require_fixture_safe("WEFT_NODE_TEST_lower", "x").is_err());
        assert!(require_fixture_safe("WEFT_NODE_TEST_X", "two\nlines").is_err());
        assert!(require_fixture_safe("WEFT_NODE_TEST_X", "").is_err());
    }

    /// The rendered pod rides the worker network envelope (role
    /// label, SA token) and carries the runner argv incl. the color.
    #[test]
    fn manifest_carries_worker_envelope_and_argv() {
        let p = payload();
        let args = p.runner_invocation(Some("c0ffee")).unwrap().args;
        let pod = render_test_pod_manifest(
            "weft-test-p1-abc",
            "wft-shared-workers",
            &p,
            &args,
            "http://broker:9090",
            None,
            Some("weft-regcred"),
        );
        assert!(pod.contains("weft.dev/role: worker"), "worker network envelope");
        assert!(pod.contains("weft.dev/kind: node-test"), "identifiable as a test pod");
        assert!(pod.contains("serviceAccountToken"), "broker token projection");
        assert!(pod.contains("restartPolicy: Never"), "run to completion");
        assert!(pod.contains("- \"--node\"") && pod.contains("- \"SlackSendMessage\""));
        assert!(pod.contains("- \"--live-connection\""));
        assert!(pod.contains("- \"--color\"") && pod.contains("- \"c0ffee\""));
        assert!(!pod.contains("subdomain:"), "no worker DNS machinery");
        assert!(!pod.contains("containerPort"), "no connection server");
        assert!(
            pod.contains("imagePullSecrets:\n    - name: weft-regcred"),
            "registry pull credential rides the test pod"
        );

        let no_secret = render_test_pod_manifest(
            "weft-test-p1-abc",
            "wft-shared-workers",
            &p,
            &args,
            "http://broker:9090",
            None,
            None,
        );
        assert!(!no_secret.contains("imagePullSecrets"), "no secret configured, no line");
    }

    /// Payload strings are charset-gated before they reach YAML, each
    /// field by the charset it actually needs.
    #[test]
    fn manifest_unsafe_strings_are_refused() {
        assert!(require_identifier_safe("node", "SlackSendMessage").is_ok());
        assert!(require_image_ref_safe("reg.example/weft-node-tests:ab12").is_ok());
        assert!(require_identifier_safe("test", "has space").is_err());
        assert!(require_identifier_safe("test", "quote\"break").is_err());
        assert!(require_identifier_safe("test", "").is_err());
        assert!(require_identifier_safe("test", "line\nbreak").is_err());
        assert!(
            require_identifier_safe("node", "--color").is_err(),
            "a leading dash would pose as a runner flag"
        );
        assert!(
            require_identifier_safe("node", "reg.example/x:y").is_err(),
            "registry syntax belongs to image refs only"
        );
        assert!(require_image_ref_safe("--rm").is_err(), "flag-shaped image ref");
    }
}
