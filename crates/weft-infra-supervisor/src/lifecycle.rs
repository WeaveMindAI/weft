//! Lifecycle loop. Claims `infra_lifecycle_command` rows for this
//! tenant and executes them via kubectl. Three verbs:
//!
//! - **apply**: compile the InfraSpec (weft-core), resolve local
//!   image tags, kubectl-apply, wait for readiness, write the
//!   `infra_node` row via `set_applied`. Fresh applies mint a new
//!   instance_id; Replace reuses the prior one (PVCs reattach by
//!   name) and sweeps workload-shaped resources before applying.
//!   (Upstream `Image::Upstream` references pass through verbatim;
//!   mutable tags like `:latest` are NOT resolved to digests, so a
//!   tag rolling underneath produces no spec-hash change. See the
//!   authoring docs' "upstream image" limitation.)
//! - **stop**: scale each unit's workloads to 0 per the unit's
//!   `on_stop` (ScaleToZero), or leave it running (NoOp); preserve
//!   PVCs.
//! - **terminate**: delete-by-label sweep including PVCs; remove the
//!   `infra_node` row.

use std::time::Duration;

use anyhow::{anyhow, Result};
use uuid::Uuid;

use weft_core::infra::{self, CompileContext, InfraSpec};

use crate::SupervisorState;

/// Resolve the spec's units into the per-unit runtime map stamped on
/// the infra_node row. Windows + stop_behavior always come from the
/// (current) spec. STATUS is per-unit: a unit in `reconciled` gets
/// `status`; a unit NOT in `reconciled` (i.e. left up, frozen) keeps
/// its `prior` status. This is what lets apply touch only the down
/// units while up units stay Running at their old version.
///
/// IMAGE REFS mirror that split, because they are the reclamation
/// keep-set's only window into what running units actually use (the
/// project row's tag map only holds the CURRENT refs, so a frozen
/// unit's older image would otherwise be reclaimable while it runs):
/// a reconciled unit records the refs its containers resolve to in
/// `image_tags` (the refs this apply puts in the cluster); a frozen
/// unit carries its `prior` refs forward unchanged. With
/// `transitioning` (the PROVISIONING pre-commit stamp), a reconciled
/// unit records prior ∪ current: its old pods may still exist during
/// the sweep half of the apply, so both generations must stay
/// referenced; the post-readiness (`set_applied`) stamp drops the
/// prior generation, closing the window.
///
/// Also under `transitioning` only: a unit in `prior` but DROPPED from
/// the spec is carried forward verbatim. Its workload is reaped later
/// in the same apply, but a cancel or kubectl failure BEFORE the reap
/// leaves the row `Failed` with those pods still running - carried,
/// they keep their refs in the keep-set and the honest roster shows a
/// unit that may still exist in the cluster. The post-readiness stamp
/// rebuilds from the CURRENT spec only, so a completed apply drops
/// them (by then the reap has taken their workloads down).
///
/// Units in the spec but absent from `prior` are new -> they're always
/// in `reconciled` (the caller computes that), so they get `status`.
fn resolve_units(
    spec: &InfraSpec,
    node_id: &str,
    prior: &std::collections::BTreeMap<String, weft_broker_client::protocol::UnitRuntime>,
    reconciled: &std::collections::HashSet<String>,
    status: weft_broker_client::protocol::InfraNodeStatus,
    image_tags: &std::collections::BTreeMap<String, String>,
    transitioning: bool,
) -> Result<std::collections::BTreeMap<String, weft_broker_client::protocol::UnitRuntime>> {
    use crate::health_engine::{FLAKY_AFTER, RECOVERY_AFTER};
    let mut out = std::collections::BTreeMap::new();
    for u in &spec.units {
        let (unit_status, image_refs) = if reconciled.contains(&u.name) {
            let mut refs = infra::unit_image_refs(u, node_id, image_tags)?;
            if transitioning {
                // The sweep half of this apply may not have taken the
                // unit's old pods down yet: keep both generations
                // referenced until the post-readiness stamp.
                if let Some(p) = prior.get(&u.name) {
                    refs.extend(p.image_refs.iter().cloned());
                }
            }
            (status, refs)
        } else {
            // Left up / frozen: keep its current status (Running or
            // Flaky) AND the refs its last apply recorded (they can be
            // older than the project's current tag map). A frozen unit
            // is by construction in `prior` (`units_to_reconcile`
            // reconciles every unit that is not), so its absence is a
            // caller bug; empty refs here would silently drop a
            // running unit's image from the keep-set, so fail instead.
            let p = prior.get(&u.name).ok_or_else(|| {
                anyhow!(
                    "unit '{}' of node '{node_id}' is neither reconciled nor in the prior roster",
                    u.name
                )
            })?;
            (p.status, p.image_refs.clone())
        };
        out.insert(
            u.name.clone(),
            weft_broker_client::protocol::UnitRuntime {
                status: unit_status,
                stop_behavior: u.on_stop,
                flaky_after_seconds: u
                    .health
                    .flaky_after_seconds
                    .unwrap_or(FLAKY_AFTER.as_secs() as u32),
                recovery_after_seconds: u
                    .health
                    .recovery_after_seconds
                    .unwrap_or(RECOVERY_AFTER.as_secs() as u32),
                image_refs,
            },
        );
    }
    if transitioning {
        // Units dropped from the spec: see the doc block. Verbatim
        // (status included): at stamp time their pods may still be up,
        // so their prior entry is still the truth about them.
        for (name, runtime) in prior {
            if !out.contains_key(name) {
                out.insert(name.clone(), runtime.clone());
            }
        }
    }
    Ok(out)
}

/// Set of declared unit names to reconcile (apply) this pass: a unit
/// is reconciled unless it is currently UP (Running/Flaky) in `prior`.
/// Up units are left frozen at their current version (something
/// downstream depends on them running). New units (not in prior) are
/// reconciled. The apply path only touches reconciled units' manifests.
fn units_to_reconcile(
    spec: &InfraSpec,
    prior: &std::collections::BTreeMap<String, weft_broker_client::protocol::UnitRuntime>,
) -> std::collections::HashSet<String> {
    spec.units
        .iter()
        .filter(|u| {
            prior
                .get(&u.name)
                .map(|p| !p.status.expects_running_replicas())
                // Not in prior = new unit = reconcile it.
                .unwrap_or(true)
        })
        .map(|u| u.name.clone())
        .collect()
}

/// Pull the `weft.dev/unit` label from a compiled manifest, if any.
/// Workload manifests (Deployment/StatefulSet/etc) carry it; shared
/// resources (Service, NetworkPolicy, ConfigMap, Secret, PVC) don't.
fn manifest_unit(manifest: &serde_json::Value) -> Option<&str> {
    manifest
        .get("metadata")?
        .get("labels")?
        .get("weft.dev/unit")?
        .as_str()
}

const READINESS_POLL_INTERVAL: Duration = Duration::from_millis(500);
/// How often the unbounded readiness wait logs a "still waiting" breadcrumb, so
/// a stuck workload is legible without a hard-fail deadline killing a slow but
/// legitimate warmup.
const READINESS_BREADCRUMB_INTERVAL: Duration = Duration::from_secs(30);

/// Selector for weft-managed infra workloads. Used as the
/// `-l weft.dev/role=infra` filter on every `list_replica_state`
/// call inside the supervisor.
pub(crate) const INFRA_SELECTOR: &str = "weft.dev/role=infra";

/// How often the executing supervisor polls the command's
/// `cancel_requested` flag while inside a wait loop (readiness /
/// drain). Between discrete kubectl steps the check is per-step.
const CANCEL_POLL_INTERVAL: Duration = Duration::from_secs(2);

/// Marker error: the command was HALTED because the user requested
/// cancellation. `tick` maps it to a `cancelled` outcome (never a
/// failure). Cancel = halt, not rollback: kubectl is not
/// transactional, so per-node partial state is left visible (the
/// apply error path stamps `Failed("cancelled ...")` on the node so
/// the user terminates/retries per-node from where it stopped).
#[derive(Debug)]
pub(crate) struct CancelledByUser {
    /// Where execution halted, for the outcome message.
    pub at: &'static str,
}

impl std::fmt::Display for CancelledByUser {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "cancelled by user ({})", self.at)
    }
}

impl std::error::Error for CancelledByUser {}

/// Bail with `CancelledByUser` if the command's cancel flag is set.
/// `at` names the step for the outcome message.
async fn check_cancel(
    state: &SupervisorState,
    command_id: i64,
    at: &'static str,
) -> Result<()> {
    if state.broker.command_cancel_requested(command_id).await? {
        return Err(anyhow::Error::new(CancelledByUser { at }));
    }
    Ok(())
}

/// Block until every Deployment / StatefulSet with the matching
/// instance label reports Ready, or the deadline passes. The
/// supervisor uses this to gate the post-apply `set_applied` write
/// so downstream `endpoint_url` queries return live URLs.
///
/// Reads workloads via `state.kube.list_replica_state` (which the
/// in-cluster impl scopes to `weft.dev/role=infra`) and filters
/// further by `weft.dev/instance`. Time is driven by `state.clock`
/// so tests can advance deterministically.
async fn wait_for_readiness(
    state: &SupervisorState,
    command_id: i64,
    namespace: &str,
    instance_id: &str,
) -> Result<()> {
    let instance_selector = format!("{INFRA_SELECTOR},weft.dev/instance={instance_id}");
    // A user apply is a user-controlled operation: a slow-warmup workload (a
    // model server pulling weights) can legitimately take a long time, so this
    // wait is NOT capped by a fixed hard-fail deadline (matching the sibling
    // drain wait, which the same theme made unbounded + cancellable). The user
    // interrupts a workload that will never come up via cancel (polled at
    // CANCEL_POLL_INTERVAL); a periodic breadcrumb makes a stuck readiness
    // legible in the logs rather than a silent hang.
    let mut next_cancel_check = state.clock.now();
    let mut next_breadcrumb = state.clock.now() + READINESS_BREADCRUMB_INTERVAL;
    loop {
        if state.clock.now() >= next_cancel_check {
            check_cancel(state, command_id, "waiting for workload readiness").await?;
            next_cancel_check = state.clock.now() + CANCEL_POLL_INTERVAL;
        }
        // Filter by instance at the apiserver. No more in-Rust
        // filter pass.
        let workloads = state
            .kube
            .list_replica_state(namespace, &instance_selector)
            .await?;
        // No workloads under this instance label is legitimate for
        // specs that emit only Service / ConfigMap (no Deployment).
        // Treat as ready; nothing to wait on. A workload deliberately
        // set to `replicas: 0` is also ready by definition
        // (ready >= desired = 0); previously we rejected those with
        // a `desired <= 0` clause that blocked any zero-replica spec.
        let all_ready = !workloads.iter().any(|w| w.ready < w.desired);
        if all_ready {
            return Ok(());
        }
        if state.clock.now() >= next_breadcrumb {
            let (ready, desired): (i64, i64) = workloads
                .iter()
                .fold((0, 0), |(r, d), w| (r + w.ready, d + w.desired));
            tracing::info!(
                target: "weft_infra_supervisor::lifecycle",
                instance = %instance_id,
                ready, desired,
                "still waiting for infra workloads to become Ready (cancel the apply to stop waiting)"
            );
            next_breadcrumb = state.clock.now() + READINESS_BREADCRUMB_INTERVAL;
        }
        state.clock.sleep(READINESS_POLL_INTERVAL).await;
    }
}

/// Drain the project's running executions before a stop/terminate,
/// via THE shared drain loop (`weft_platform_traits::drain_until_zero`,
/// the same mechanism the dispatcher's worker-replacement drain uses).
/// The cap comes from the command row (the user picked it with the
/// wait choice); on timeout the op proceeds with a loud warning. The
/// user's infra-cancel rides inside the count closure so a cancel
/// landing mid-drain aborts promptly.
async fn wait_for_drain(
    state: &SupervisorState,
    command_id: i64,
    drain_timeout_secs: u64,
    project_id: &str,
) -> Result<()> {
    let outcome = weft_platform_traits::drain_until_zero(
        state.clock.as_ref(),
        Duration::from_secs(drain_timeout_secs),
        "infra lifecycle op",
        || async {
            check_cancel(state, command_id, "waiting for running executions to drain").await?;
            state.broker.running_count(project_id).await
        },
    )
    .await?;
    if let weft_platform_traits::DrainOutcome::TimedOut { still_running } = outcome {
        tracing::warn!(
            project_id,
            still_running,
            drain_timeout_secs,
            "running_policy=wait drain timeout; proceeding with lifecycle op"
        );
    }
    Ok(())
}

pub async fn run_loop(state: SupervisorState) -> Result<()> {
    loop {
        match tick(&state).await {
            Ok(true) => {
                // Got work; loop immediately to drain any queue.
                continue;
            }
            Ok(false) => {}
            Err(e) => {
                tracing::warn!(error = %e, "lifecycle tick failed");
            }
        }
        state.clock.sleep(state.poll_interval).await;
    }
}

/// Returns true when work was done.
///
/// Exposed for integration tests. The real `run_loop` calls this
/// in a hot loop; tests call it once per scenario step.
pub async fn tick(state: &SupervisorState) -> Result<bool> {
    let Some(cmd) = state
        .broker
        .claim_command(&state.pod_name)
        .await?
    else {
        return Ok(false);
    };
    tracing::info!(
        command_id = cmd.id,
        project_id = %cmd.project_id,
        node_id = ?cmd.node_id,
        verb = %cmd.verb,
        "lifecycle command claimed"
    );
    let result = execute(state, &cmd).await;
    // A user-honored cancel is its own outcome, never a failure.
    let cancelled = result
        .as_ref()
        .err()
        .map(|e| e.downcast_ref::<CancelledByUser>().is_some())
        .unwrap_or(false);
    let error = result.as_ref().err().map(|e| e.to_string());
    // `command_complete` is Gone if the row was already completed
    // (remove_node cascade cancelled it) and Displaced if this pod no
    // longer owns the project (drain / lease takeover moved it
    // mid-command). In the displaced case the command stays
    // UNCOMPLETED on purpose, so the new owner re-runs and finishes it
    // (the user never re-acts). Either way, log + move on; never
    // propagate as a failure of this tick.
    match state
        .broker
        .command_complete(&state.pod_name, cmd.id, error.as_deref(), cancelled)
        .await?
    {
        weft_broker_client::WriteOutcome::Applied(_) => {}
        weft_broker_client::WriteOutcome::Displaced => tracing::info!(
            command_id = cmd.id,
            "command_complete displaced (project ownership moved); the command is left for the new owner"
        ),
        weft_broker_client::WriteOutcome::Gone => tracing::info!(
            command_id = cmd.id,
            "command_complete: the command was already completed; no-op"
        ),
    }
    if let Err(e) = result {
        if cancelled {
            tracing::info!(
                command_id = cmd.id,
                halt = %e,
                "lifecycle command halted by user cancel; marked cancelled"
            );
        } else {
            tracing::warn!(
                command_id = cmd.id,
                error = %e,
                "lifecycle command failed; marked complete with error"
            );
        }
    }
    Ok(true)
}

async fn execute(
    state: &SupervisorState,
    cmd: &weft_broker_client::protocol::SupervisorCommandRow,
) -> Result<()> {
    use weft_broker_client::protocol::{InfraLifecycleVerb, RunningPolicy};
    // Apply is the only verb that doesn't operate on existing
    // infra_node rows; it creates / updates one. Route early.
    if cmd.verb == InfraLifecycleVerb::Apply {
        return execute_apply(state, cmd).await;
    }

    // Honor running_policy. `wait`: poll the broker's running-count
    // endpoint until 0 (or timeout). `cancel`: skip; the dispatcher
    // already ran cancel_running_non_suspended when it issued the
    // command, so any colors still alive are draining naturally.
    if cmd.running_policy == Some(RunningPolicy::Wait) {
        wait_for_drain(state, cmd.id, cmd.drain_timeout_secs, &cmd.project_id).await?;
    }
    let nodes = state.broker.infra_nodes(&cmd.project_id).await?;
    let targets: Vec<&weft_broker_client::protocol::SupervisorInfraNode> = match &cmd.node_id {
        Some(node_id) => nodes.iter().filter(|n| n.node_id == *node_id).collect(),
        None => nodes.iter().collect(),
    };
    if targets.is_empty() {
        // No matching rows is a soft no-op, not a failure. Happens
        // when the user clicks Stop / Terminate multiple times in
        // quick succession: the first command already deleted (or
        // cleared) the `infra_node` row(s), so follow-up commands
        // have nothing left to act on. Mark complete cleanly so the
        // CLI gets a 200 and the action bar doesn't display an
        // unhelpful error.
        tracing::info!(
            command_id = cmd.id,
            project_id = %cmd.project_id,
            node_id = ?cmd.node_id,
            verb = %cmd.verb,
            "lifecycle command: no matching infra_node rows; completing as no-op"
        );
        return Ok(());
    }
    // The project namespace isn't on the command row; fetch via the
    // projects this pod owns. The command was only claimable because
    // this pod owns the project (the broker's claim ownership predicate),
    // so it is guaranteed present in the owned set.
    let projects = state.broker.owned_projects(&state.pod_name).await?;
    let project = projects
        .iter()
        .find(|p| p.project_id == cmd.project_id)
        .ok_or_else(|| anyhow!("project not in supervisor's owned set"))?;
    let namespace = project.project_namespace.clone();

    match cmd.verb {
        InfraLifecycleVerb::Stop => {
            // The `stopping` transient is flipped per-unit INSIDE the
            // cancel-guarded work loop below (right before each unit's scale),
            // NOT in an upfront flip-all loop: a cancel that lands before a unit
            // is reached must leave it in its prior RESTING status, never stuck
            // in the transient `stopping`.
            //
            // List workloads ONCE; iterate targets against the single snapshot.
            // Re-listing per target would be N kube round-trips when the snapshot
            // already covers the whole namespace.
            let workloads = state.kube.list_replica_state(&namespace, INFRA_SELECTOR).await?;
            for n in &targets {
                // Interruptible between nodes: already-stopped units
                // stay stopped (halt, not rollback); the rest keep
                // their prior status.
                check_cancel(state, cmd.id, "stopping infra nodes").await?;
                // Per-unit stop: scale a unit's workloads to 0 only if
                // its `stop_behavior` is ScaleToZero. A NoOp unit (a
                // license server, a slow-warmup model) survives stop and
                // is only removed by terminate. We then mark each
                // stopped unit `Stopped`; NoOp units keep their status,
                // so the node rollup reflects "partly running".
                let mut any_stopped = false;
                for w in workloads.iter().filter(|w| {
                    w.labels.get("weft.dev/instance").map(|s| s.as_str())
                        == Some(n.instance_id.as_str())
                }) {
                    let Some(unit) = w.labels.get("weft.dev/unit") else {
                        continue;
                    };
                    // A workload whose unit is NOT in the row's roster
                    // is an orphan: a unit dropped from the spec whose
                    // reap never landed (the apply meant it gone), or
                    // foreign debris. Scaling it to zero regardless of
                    // `force` finishes that intent; there is no
                    // stop_behavior to honor because the row does not
                    // know the unit. Never STAMP it: the broker fences
                    // per-unit stamps on roster membership, so the
                    // write would come back Raced and read below as
                    // "ownership moved", ending the stop early. The
                    // honest record for an orphan is its absence from
                    // the roster.
                    let Some(runtime) = n.units.get(unit) else {
                        tracing::warn!(
                            project_id = %cmd.project_id,
                            node_id = %n.node_id,
                            unit = %unit,
                            workload = %w.name,
                            "stopping an orphan workload with no roster entry; scaling down without stamping the row"
                        );
                        state
                            .kube
                            .scale_workload(&namespace, w.kind, &w.name, 0)
                            .await?;
                        continue;
                    };
                    // `force` takes every unit down regardless of
                    // on_stop. Otherwise honor the unit's stop_behavior.
                    let scale_to_zero = cmd.force
                        || runtime.stop_behavior == weft_core::StopBehavior::ScaleToZero;
                    if !scale_to_zero {
                        continue;
                    }
                    // Flip THIS unit to the `stopping` transient right before its
                    // scale (not upfront for the whole set): a cancel that landed
                    // earlier left the not-yet-reached units in their resting
                    // status. UI hint only (the terminal per-unit `stopped` write
                    // below is what matters), so a broker failure here is logged,
                    // not fatal.
                    if let Err(e) = state
                        .broker
                        .set_status(
                            &state.pod_name,
                            Some(cmd.id),
                            &cmd.project_id,
                            &n.node_id,
                            Some(unit),
                            weft_broker_client::protocol::InfraNodeStatus::Stopping,
                            None,
                            None,
                        )
                        .await
                    {
                        tracing::warn!(
                            project_id = %cmd.project_id,
                            node_id = %n.node_id,
                            unit = %unit,
                            error = %e,
                            "set_status(stopping) failed; continuing with scale-down"
                        );
                    }
                    state
                        .kube
                        .scale_workload(&namespace, w.kind, &w.name, 0)
                        .await?;
                    let outcome = state
                        .broker
                        .set_status(
                            &state.pod_name,
                            Some(cmd.id),
                            &cmd.project_id,
                            &n.node_id,
                            Some(unit),
                            weft_broker_client::protocol::InfraNodeStatus::Stopped,
                            None,
                            None,
                        )
                        .await?;
                    match outcome {
                        weft_broker_client::WriteOutcome::Applied(_) => any_stopped = true,
                        weft_broker_client::WriteOutcome::Displaced => {
                            // Project ownership moved: this pod must not
                            // keep scaling the remaining nodes down. The
                            // new owner re-runs the (idempotent) stop, and
                            // command_complete is displaced for the same
                            // reason, so `tick` leaves the command for it.
                            // Same exit as the terminate path.
                            tracing::info!(
                                project_id = %cmd.project_id,
                                node_id = %n.node_id,
                                unit = %unit,
                                "set_status(stopped) displaced; aborting stop for the new owner to re-run"
                            );
                            return Ok(());
                        }
                        weft_broker_client::WriteOutcome::Gone => {
                            // This node's row is gone (removed mid-stop)
                            // while the project is still ours. It cannot
                            // be "the unit left the roster": only an
                            // apply rewrites the roster and this pod's
                            // per-project work loop runs one command at
                            // a time. Nothing left to record for the
                            // node; on to the next one, and the command
                            // completes normally. Units of it that did
                            // stop keep their event (`any_stopped`
                            // stays what they earned).
                            tracing::info!(
                                project_id = %cmd.project_id,
                                node_id = %n.node_id,
                                unit = %unit,
                                "set_status(stopped): row gone; skipping this node's remaining units"
                            );
                            break;
                        }
                    }
                }
                // One Stopped event per node that actually stopped a
                // unit (the event rail is node-scoped; the per-unit
                // detail lives in the row's units map).
                if any_stopped {
                    state
                        .broker
                        .event_record(
                            &cmd.project_id,
                            Some(&n.node_id),
                            weft_broker_client::protocol::InfraEvent::Stopped,
                        )
                        .await?;
                }
            }
        }
        InfraLifecycleVerb::Terminate => {
            for n in &targets {
                // Interruptible between nodes: already-terminated
                // nodes are gone; remaining nodes keep their rows
                // (visible partial state the user acts on per-node).
                //
                // The `terminating` transient is flipped HERE, per node, right
                // before this node's kubectl delete, NOT in an upfront flip-all
                // loop. That way a cancel that lands before a node is reached
                // leaves it in its prior RESTING status, never stuck in the
                // transient `terminating` (which blocks re-apply reuse and shows
                // a permanent spinner). Halt, not rollback: nodes already deleted
                // stay gone; the rest keep their status.
                //
                // The stamp is REQUIRED before the delete, never best-effort:
                // it is the durable record that this instance's resources are
                // being torn down. Were the delete to run without it and
                // `remove_node` then fail (same broker, so the two fail
                // together), the row would keep saying Running with the old
                // applied hash while nothing is deployed, and every later
                // apply would full-skip on the hash match, unable to repair
                // it. Stamped, the row says Terminating, which is the one
                // status the next apply refuses to reuse: it mints a fresh
                // instance and finishes this delete first. Displaced means
                // ownership moved: the new owner re-runs the terminate.
                // Gone means the row vanished under us (a project removal
                // in flight): the delete below still runs, since the
                // instance's resources are exactly what this verb takes
                // down and the label selector needs no row.
                check_cancel(state, cmd.id, "terminating infra nodes").await?;
                match state
                    .broker
                    .set_status(
                        &state.pod_name,
                        Some(cmd.id),
                        &cmd.project_id,
                        &n.node_id,
                        None,
                        weft_broker_client::protocol::InfraNodeStatus::Terminating,
                        None,
                        None,
                    )
                    .await?
                {
                    weft_broker_client::WriteOutcome::Applied(_) => {}
                    weft_broker_client::WriteOutcome::Displaced => {
                        tracing::info!(
                            project_id = %cmd.project_id,
                            node_id = %n.node_id,
                            "set_status(terminating) displaced (project ownership moved); aborting terminate for re-run"
                        );
                        return Ok(());
                    }
                    weft_broker_client::WriteOutcome::Gone => tracing::info!(
                        project_id = %cmd.project_id,
                        node_id = %n.node_id,
                        "set_status(terminating): row gone; deleting the instance's resources anyway"
                    ),
                }
                let selector = format!("weft.dev/instance={}", n.instance_id);
                // The list of PVCs to preserve was carried on the
                // `infra_node` row at apply time (from
                // `InfraSpec.lifecycle.on_terminate.preserve_pvcs`).
                // The supervisor doesn't have the spec at terminate
                // time, but it has the row.
                state
                    .kube
                    .delete_by_label(&namespace, &selector, &n.preserve_pvcs)
                    .await?;
                // remove_node is ownership-gated only: a row that is
                // already gone is `Applied { removed: false }`, and the
                // event below still records that this instance was
                // taken down.
                if !state
                    .broker
                    .remove_node(&state.pod_name, &cmd.project_id, &n.node_id)
                    .await?
                    .is_applied()
                {
                    // Lost ownership mid-Terminate (drain / lease
                    // takeover). Abort: leave the command uncompleted so
                    // the new owner re-runs the (idempotent) terminate.
                    // command_complete will also be displaced for the
                    // same reason, so `tick` won't mark it done.
                    tracing::info!(
                        project_id = %cmd.project_id,
                        node_id = %n.node_id,
                        "remove_node displaced (project ownership moved); aborting terminate for re-run"
                    );
                    return Ok(());
                }
                state
                    .broker
                    .event_record(
                        &cmd.project_id,
                        Some(&n.node_id),
                        weft_broker_client::protocol::InfraEvent::Terminated,
                    )
                    .await?;
            }
        }
        InfraLifecycleVerb::Apply => {
            // Apply is routed at the top of the function before this
            // match; exhaustive matching (no catch-all) makes a new
            // verb a compile error rather than a silent fallthrough.
            unreachable!("Apply is routed before the verb match");
        }
        InfraLifecycleVerb::Deactivate | InfraLifecycleVerb::Reactivate => {
            // The supervisor's `claim_command` filters these out
            // (they're dispatcher-claimable); if one ever lands
            // here it's a routing bug at the broker, fail loud.
            return Err(anyhow!(
                "supervisor claimed dispatcher-only verb '{}'; broker filter must match",
                cmd.verb
            ));
        }
    }
    Ok(())
}

async fn execute_apply(
    state: &SupervisorState,
    cmd: &weft_broker_client::protocol::SupervisorCommandRow,
) -> Result<()> {
    let node_id = cmd
        .node_id
        .as_deref()
        .ok_or_else(|| anyhow!("apply command missing node_id"))?;
    let spec_value = cmd
        .spec_json
        .as_ref()
        .ok_or_else(|| anyhow!("apply command missing spec_json"))?;
    let spec: InfraSpec = serde_json::from_value(spec_value.clone())
        .map_err(|e| anyhow!("deserialize spec_json: {e}"))?;

    // Resolve project namespace + tenant. Both are needed for the
    // compile context. The pooled supervisor has no tenant of its own,
    // so the tenant comes from the project. The command was only
    // claimable because this pod owns the project, so it is in the owned
    // set.
    let project = state
        .broker
        .owned_projects(&state.pod_name)
        .await?
        .into_iter()
        .find(|p| p.project_id == cmd.project_id)
        .ok_or_else(|| anyhow!("project not in supervisor's owned set"))?;
    let namespace = project.project_namespace;
    let project_tenant = project.tenant_id;

    // Per-(project, node) image tag map, used to resolve
    // `Image::Local { name }` references at compile time. Converted
    // to `BTreeMap` so the downstream `hash_spec` walk is
    // deterministic (HashMap iteration order would randomize the
    // hash).
    let image_tags_unsorted = state
        .broker
        .project_image_tags(&cmd.project_id, node_id)
        .await?;
    let image_tags: std::collections::BTreeMap<String, String> =
        image_tags_unsorted.into_iter().collect();

    // Read the prior infra_node row. Drives skip / fresh / replace.
    let prior = state
        .broker
        .infra_nodes(&cmd.project_id)
        .await?
        .into_iter()
        .find(|n| n.node_id == node_id);

    // Mint or reuse instance_id BEFORE the compile so the hash we
    // compute is the same one we'll write on success.
    //
    // Reuse the prior instance_id whenever a usable row exists.
    // `instance_id` is the base name the compiler stamps into every
    // emitted resource (Deployment, Service, PVC); reusing it lets
    // PVCs reattach by name on the next apply. Mint fresh only when
    // there's nothing to reattach to:
    //   - no row at all (first apply for this node), or
    //   - status=terminating (the supervisor is actively deleting
    //     this instance's resources; reusing the id would race the
    //     delete and produce a half-zombie set).
    // Every other status (running, stopped, flaky, failed,
    // provisioning, stopping) means the PVC is still bound and we
    // want to attach to it again.
    let (mode, instance_id) = match prior.as_ref() {
        Some(p) if p.status.permits_instance_id_reuse() => {
            (ApplyMode::ReplaceOrSkip, p.instance_id.clone())
        }
        _ => (
            ApplyMode::Fresh,
            mint_instance_id(&cmd.project_id, node_id),
        ),
    };

    let compile_ctx = CompileContext {
        tenant_id: &project_tenant,
        project_id: &cmd.project_id,
        node_id,
        instance_id: &instance_id,
        namespace: &namespace,
        local_image_tags: &image_tags,
    };

    // Hash the typed spec FIRST (with image_tags mixed in) so the
    // skip-vs-replace decision is stable across compile.rs changes.
    // Then compile to manifests for kubectl apply.
    let applied_spec_hash = infra::hash_spec(&spec, &image_tags)
        .map_err(|e| anyhow!("hash_spec: {e}"))?;
    let manifests = infra::compile(&spec, &compile_ctx)
        .map_err(|e| anyhow!("compile: {e}"))?;

    // Per-unit apply. Reconcile only the units that are DOWN (or new);
    // leave UP units (Running/Flaky) frozen at their current version,
    // because something downstream depends on them running. Up units
    // are taken down only by an explicit force-stop, never by apply.
    let prior_units: std::collections::BTreeMap<String, weft_broker_client::protocol::UnitRuntime> =
        prior.as_ref().map(|p| p.units.clone()).unwrap_or_default();
    let reconcile = units_to_reconcile(&spec, &prior_units);

    // Full skip: every declared unit is already up AND the hash
    // matches. Cluster state is already what we want; no kubectl. The
    // row keeps its instance_id, hash, endpoints. (`reconcile` empty
    // means every unit is up; hash match means the up units are at the
    // current spec, so there's genuinely nothing to do.)
    let hash_matches = prior
        .as_ref()
        .and_then(|p| p.applied_spec_hash.as_deref())
        == Some(applied_spec_hash.as_str());
    if matches!(mode, ApplyMode::ReplaceOrSkip) && reconcile.is_empty() && hash_matches {
        // Re-fire `started` so the dispatcher's SSE bus wakes any
        // subscribers waiting on this command. Nothing else changed.
        state
            .broker
            .event_record(
                &cmd.project_id,
                Some(node_id),
                weft_broker_client::protocol::InfraEvent::Started(
                    weft_broker_client::protocol::StartedPayload {
                        instance_id: instance_id.clone(),
                        mode: weft_broker_client::protocol::StartMode::Skip,
                    },
                ),
            )
            .await?;
        return Ok(());
    }

    // A Fresh apply over an existing row means the row is `Terminating`
    // (the only status that refuses instance-id reuse): a terminate
    // stamped it and then failed or died before its delete landed, so
    // the PRIOR instance's workloads can still be running. Every sweep
    // in the apply selects by the NEW instance id and would leave them
    // up, while the post-readiness stamp rebuilds the roster from the
    // current spec and drops their image refs from the keep-set (an
    // image reclaim would then delete what those pods run). Finish the
    // terminate first, delete the prior instance by its own label with
    // the PVC list its row recorded, and do it BEFORE the provisioning
    // stamp below overwrites the row's instance_id: after that stamp
    // the prior id lives nowhere durable, so a pod death between the
    // stamp and this delete would strand the old instance forever
    // (the retry reuses the new id). The row is already a visible,
    // terminable `Terminating` row, so the "row before kubectl" rule
    // the stamp exists for is already met, and a failure here leaves
    // it exactly as it was for the next apply to finish. What the
    // provisioning stamp ALSO provides is the ownership fence before
    // the first kubectl call (a pod that lost the project's lease
    // must not touch its namespace), so the same fence is taken here
    // by re-stamping the row's own `Terminating` through the
    // command-gated write: it changes nothing on the row; Displaced
    // means the lease moved (leave the apply for the owner), and Gone
    // means the row or the command vanished under a running apply,
    // which nothing here can act on, so it fails loud.
    if let (ApplyMode::Fresh, Some(p)) = (&mode, prior.as_ref()) {
        match state
            .broker
            .set_status(
                &state.pod_name,
                Some(cmd.id),
                &cmd.project_id,
                node_id,
                None,
                weft_broker_client::protocol::InfraNodeStatus::Terminating,
                None,
                None,
            )
            .await?
        {
            weft_broker_client::WriteOutcome::Applied(_) => {}
            weft_broker_client::WriteOutcome::Displaced => {
                tracing::info!(
                    project_id = %cmd.project_id,
                    node_id = %node_id,
                    "ownership fence displaced before finishing the prior terminate; leaving apply for the new owner"
                );
                return Ok(());
            }
            weft_broker_client::WriteOutcome::Gone => {
                return Err(anyhow!(
                    "infra_node row for node '{node_id}' (or its apply command) vanished before \
                     the prior instance could be finished; re-run the apply"
                ));
            }
        }
        state
            .kube
            .delete_by_label(
                &namespace,
                &format!("weft.dev/instance={}", p.instance_id),
                &p.preserve_pvcs,
            )
            .await?;
    }

    // Pre-apply commitment: write the infra_node row before any
    // kubectl call so a partial-apply failure leaves a visible row the
    // user can Terminate. Reconciled units go Provisioning; up units
    // keep their (Running/Flaky) status. The units map also carries
    // the (possibly removed) prior units' absence: it's rebuilt from
    // the CURRENT spec, so a unit dropped from the spec disappears
    // from the row here (its workloads are reaped below).
    // `transitioning = true`: the reconciled units' PRIOR image refs
    // stay recorded until the post-readiness stamp, because their old
    // pods can still exist while the sweep half of this apply runs.
    let provision_outcome = state
        .broker
        .set_provisioning(
            &state.pod_name,
            cmd.id,
            &cmd.project_id,
            node_id,
            &instance_id,
            &namespace,
            spec.lifecycle.on_terminate.preserve_pvcs.clone(),
            resolve_units(
                &spec,
                node_id,
                &prior_units,
                &reconcile,
                weft_broker_client::protocol::InfraNodeStatus::Provisioning,
                &image_tags,
                true,
            )?,
        )
        .await?;
    if !provision_outcome.is_applied() {
        // Displaced: project ownership moved before we committed the
        // Provisioning row; the new owner re-runs the apply from
        // scratch and the command stays uncompleted. Gone: the command
        // is already completed (a node removal cancelled it), so there
        // is nothing left to apply for; `tick`'s completion is a no-op.
        tracing::info!(
            project_id = %cmd.project_id,
            node_id = %node_id,
            outcome = ?provision_outcome,
            "set_provisioning not applied; leaving the apply"
        );
        return Ok(());
    }

    // Determine the spec's current unit set (for orphan reap) and the
    // manifests to apply (reconciled units' workloads + all shared
    // resources; up units' workload manifests are skipped so a frozen
    // unit never receives a changed spec).
    let spec_units: std::collections::HashSet<String> =
        spec.units.iter().map(|u| u.name.clone()).collect();
    let start_mode = if matches!(mode, ApplyMode::ReplaceOrSkip) {
        weft_broker_client::protocol::StartMode::Replace
    } else {
        weft_broker_client::protocol::StartMode::Fresh
    };

    let apply_result: Result<std::collections::BTreeMap<String, String>> = async {
        // Interruptible between the phases below (sweep / apply /
        // readiness). A cancel mid-apply bails through the error path,
        // which stamps the node `Failed("cancelled by user (...)")`:
        // the honest resting state for a half-applied node (kubectl is
        // not transactional; the user terminates or retries from
        // there), while `tick` records the COMMAND outcome as
        // `cancelled`, not failed.
        check_cancel(state, cmd.id, "before sweeping stale workloads").await?;
        // Orphan reap (unit-level): delete workloads for units that
        // are in the cluster (prior row) but no longer declared in the
        // spec. The node-level reap (deleted node) is the dispatcher's
        // job; this is the unit-granularity analog.
        for unit in prior_units.keys() {
            if !spec_units.contains(unit) {
                state
                    .kube
                    .delete_by_label(
                        &namespace,
                        &format!("weft.dev/instance={instance_id},weft.dev/unit={unit}"),
                        &[],
                    )
                    .await?;
            }
        }
        // Sweep the reconciled units' workloads before re-applying so a
        // spec change touching immutable fields (StatefulSet selectors)
        // succeeds. Only reconciled (down) units are swept; up units
        // are never touched. PVCs/ConfigMaps/Secrets are kept.
        for unit in &reconcile {
            state
                .kube
                .delete_by_label(
                    &namespace,
                    &format!("weft.dev/instance={instance_id},weft.dev/unit={unit}"),
                    &spec.lifecycle.on_terminate.preserve_pvcs,
                )
                .await?;
        }
        check_cancel(state, cmd.id, "before applying manifests").await?;
        // Apply reconciled units' manifests + shared resources (no unit
        // label). Skip up units' workload manifests entirely.
        for manifest in &manifests {
            match manifest_unit(manifest) {
                Some(unit) if !reconcile.contains(unit) => continue, // frozen up unit
                _ => state.kube.apply(manifest).await?,
            }
        }
        wait_for_readiness(state, cmd.id, &namespace, &instance_id).await?;
        compute_endpoints(&spec, &instance_id, &namespace)
    }
    .await;
    let endpoints = match apply_result {
        Ok(eps) => eps,
        Err(e) => {
            let msg = e.to_string();
            // Best-effort row-status hint. The PRIMARY error record
            // is `infra_lifecycle_command.outcome=failed` (written by
            // the supervisor's command_complete wrapper after we
            // bubble); the action bar reads from there. This write
            // additionally stamps `infra_node.status=Failed +
            // failure_message` so the node-level UI sees the error
            // too. If it itself fails, the primary record still
            // carries the cause; log + bubble the apply error.
            if let Err(status_err) = state
                .broker
                .set_status(
                    &state.pod_name,
                    Some(cmd.id),
                    &cmd.project_id,
                    node_id,
                    None, // apply failure fails the whole node, all units
                    weft_broker_client::protocol::InfraNodeStatus::Failed,
                    Some(weft_broker_client::protocol::FailureStage::Apply),
                    Some(&msg),
                )
                .await
            {
                tracing::warn!(
                    project_id = %cmd.project_id,
                    node_id = %node_id,
                    error = %status_err,
                    "failed to write Failed status after apply error; bubbling apply error"
                );
            }
            return Err(e);
        }
    };

    let outcome = state
        .broker
        .set_applied(
            &state.pod_name,
            cmd.id,
            &cmd.project_id,
            node_id,
            &instance_id,
            &applied_spec_hash,
            endpoints,
            &namespace,
            spec.lifecycle.on_terminate.preserve_pvcs.clone(),
            // `transitioning = false`: readiness waited, the reconciled
            // units' old pods are gone, so their PRIOR image refs leave
            // the row here (the keep-set window closes with this stamp).
            resolve_units(
                &spec,
                node_id,
                &prior_units,
                &reconcile,
                weft_broker_client::protocol::InfraNodeStatus::Running,
                &image_tags,
                false,
            )?,
        )
        .await?;
    if !outcome.is_applied() {
        // Displaced: project ownership moved mid-apply (drain / lease
        // takeover); don't fire the Started event and don't complete
        // the command; the new owner re-runs the (idempotent) apply
        // and finishes it. Gone: the command was completed under us (a
        // node removal cancelled it); nothing to record.
        tracing::info!(
            project_id = %cmd.project_id,
            node_id = %node_id,
            outcome = ?outcome,
            "set_applied not applied; leaving the apply"
        );
        return Ok(());
    }
    state
        .broker
        .event_record(
            &cmd.project_id,
            Some(node_id),
            weft_broker_client::protocol::InfraEvent::Started(
                weft_broker_client::protocol::StartedPayload {
                    instance_id: instance_id.clone(),
                    mode: start_mode,
                },
            ),
        )
        .await?;
    Ok(())
}

enum ApplyMode {
    /// No usable prior state. Mint a new instance id and apply
    /// from scratch.
    Fresh,
    /// Prior was Running. Either skip (if hash matches) or replace
    /// (sweep workload-shaped resources, re-apply, PVCs reattach).
    /// The choice is made after compile, when we have the new hash
    /// to compare against the stored one.
    ReplaceOrSkip,
}

fn mint_instance_id(project_id: &str, node_id: &str) -> String {
    // K8s names: lowercase alphanum + `-`, max 63. The instance id
    // ends up as a Deployment / Service / PVC name; leave room for
    // suffixes like `-data` or `-api`.
    let pid = sanitize(project_id).chars().take(8).collect::<String>();
    let nid = sanitize(node_id).chars().take(20).collect::<String>();
    let suffix = Uuid::new_v4().simple().to_string();
    // 10 hex chars = 40 bits of entropy. 6 was a birthday-risk
    // ceiling for high-frequency apply cycles on hot tenants; 10
    // fits comfortably under the k8s 63-char label limit even
    // alongside the truncated project + node prefixes.
    let short_suffix: String = suffix.chars().take(10).collect();
    format!("wn-{pid}-{nid}-{short_suffix}")
}

fn sanitize(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut last_dash = false;
    for c in s.chars() {
        let lc = c.to_ascii_lowercase();
        if lc.is_ascii_alphanumeric() {
            out.push(lc);
            last_dash = false;
        } else if !last_dash {
            out.push('-');
            last_dash = true;
        }
    }
    while out.starts_with('-') {
        out.remove(0);
    }
    while out.ends_with('-') {
        out.pop();
    }
    out
}

fn compute_endpoints(
    spec: &InfraSpec,
    instance_id: &str,
    namespace: &str,
) -> Result<std::collections::BTreeMap<String, String>> {
    use weft_core::infra::Protocol;
    let mut out = std::collections::BTreeMap::new();
    for ep in &spec.endpoints {
        // Spec validation (`weft-core::infra::compile::validate_endpoint`)
        // already rejected endpoints whose (unit, container, port)
        // chain doesn't resolve, so reaching `None` here means the
        // applied spec_json diverges from the spec we compiled (a
        // hand-edited row, or a validation gap). Bubble the error so
        // it fails THIS apply (Failed status), NOT the whole tenant
        // supervisor: a panic here would unwind the lifecycle loop
        // task and take down health monitoring for every project
        // under the tenant.
        let port = spec
            .units
            .iter()
            .find(|u| u.name == ep.unit)
            .and_then(|u| u.containers.iter().find(|c| c.name == ep.container))
            .and_then(|c| c.ports.iter().find(|p| p.name == ep.port))
            .ok_or_else(|| {
                anyhow!(
                    "compute_endpoints: endpoint '{}' references unit/container/port \
                     '{}/{}/{}' that doesn't exist; applied spec_json diverges from the \
                     compiled spec",
                    ep.name, ep.unit, ep.container, ep.port,
                )
            })?;
        let scheme = if matches!(port.protocol, Protocol::Udp) {
            "udp"
        } else {
            "http"
        };
        let url = format!(
            "{scheme}://{instance_id}-{name}.{namespace}.svc.cluster.local:{p}",
            scheme = scheme,
            instance_id = instance_id,
            name = ep.name,
            namespace = namespace,
            p = port.port,
        );
        out.insert(ep.name.clone(), url);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_strips_non_alphanum() {
        assert_eq!(sanitize("Foo_Bar-123"), "foo-bar-123");
        assert_eq!(sanitize("a/b/c"), "a-b-c");
        assert_eq!(sanitize("--leading--"), "leading");
    }

    #[test]
    fn mint_instance_id_format() {
        let id = mint_instance_id("Project-Id-Long-12345", "node_one");
        assert!(id.starts_with("wn-"));
        assert!(id.len() <= 50);
        assert!(id.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-'));
    }

    #[test]
    fn compute_endpoints_resolves_url() {
        use weft_core::infra::*;
        let spec = InfraSpec {
            units: vec![Unit {
                name: "u".into(),
                kind: UnitKind::Deployment,
                containers: vec![Container {
                    ports: vec![ContainerPort {
                        name: "http".into(),
                        port: 8080,
                        protocol: Protocol::Tcp,
                    }],
                    ..Container::new("c", Image::Upstream { reference: "x:1".into() })
                }],
                ..Default::default()
            }],
            endpoints: vec![Endpoint {
                name: "api".into(),
                unit: "u".into(),
                container: "c".into(),
                port: "http".into(),
                expose: Expose::ClusterInternal,
            }],
            ..Default::default()
        };
        let map = compute_endpoints(&spec, "inst1", "wft-project-x-y").unwrap();
        assert_eq!(
            map.get("api").unwrap(),
            "http://inst1-api.wft-project-x-y.svc.cluster.local:8080"
        );
    }

    /// The image-ref bookkeeping that lets `GET /images/referenced` keep
    /// what running units actually use while the project's tag map has
    /// moved on. Three rules, one test: a FROZEN up unit carries its
    /// prior refs forward untouched (the whole point: its image is older
    /// than the map); the PROVISIONING stamp unions a reconciled unit's
    /// prior refs with its new ones (old pods can still exist during the
    /// sweep half of the apply); the post-readiness stamp drops the
    /// prior generation (the window closes). Local names resolve through
    /// the tag map, upstream literals pass through.
    #[test]
    fn resolve_units_records_the_refs_running_units_use() {
        use std::collections::{BTreeMap, HashSet};
        use weft_broker_client::protocol::{InfraNodeStatus, UnitRuntime};
        use weft_core::infra::*;

        fn runtime(status: InfraNodeStatus, refs: &[&str]) -> UnitRuntime {
            UnitRuntime {
                status,
                stop_behavior: weft_core::StopBehavior::ScaleToZero,
                flaky_after_seconds: 1,
                recovery_after_seconds: 1,
                image_refs: refs.iter().map(|s| s.to_string()).collect(),
            }
        }

        let spec = InfraSpec {
            units: vec![
                Unit {
                    name: "frozen".into(),
                    containers: vec![Container::new(
                        "c",
                        Image::Local { name: "bridge".into() },
                    )],
                    ..Default::default()
                },
                Unit {
                    name: "replaced".into(),
                    containers: vec![
                        Container::new("c", Image::Local { name: "bridge".into() }),
                        Container::new(
                            "sidecar",
                            Image::Upstream { reference: "busybox:1".into() },
                        ),
                    ],
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        // Both units ran weft-infra-bridge:old; "replaced" is down, so it
        // reconciles onto the map's current bridge ref. A third unit,
        // "gone", was dropped from the spec entirely (its workload is
        // the orphan the apply will reap).
        let mut prior = BTreeMap::new();
        prior.insert("frozen".into(), runtime(InfraNodeStatus::Running, &["weft-infra-bridge:old"]));
        prior.insert(
            "replaced".into(),
            runtime(InfraNodeStatus::Stopped, &["weft-infra-bridge:old"]),
        );
        prior.insert(
            "gone".into(),
            runtime(InfraNodeStatus::Running, &["weft-infra-gone:0ld"]),
        );
        let reconciled: HashSet<String> = ["replaced".to_string()].into_iter().collect();
        let tags: BTreeMap<String, String> =
            [("bridge".to_string(), "weft-infra-bridge:new".to_string())]
                .into_iter()
                .collect();

        let provisioning = resolve_units(
            &spec,
            "n1",
            &prior,
            &reconciled,
            InfraNodeStatus::Provisioning,
            &tags,
            true,
        )
        .unwrap();
        // Frozen: prior refs carried forward, current map ignored.
        assert_eq!(
            provisioning["frozen"].image_refs,
            ["weft-infra-bridge:old".to_string()].into_iter().collect()
        );
        // Reconciling: BOTH generations stay referenced while the sweep
        // may still have old pods up (plus the upstream literal).
        assert_eq!(
            provisioning["replaced"].image_refs,
            [
                "weft-infra-bridge:old".to_string(),
                "weft-infra-bridge:new".to_string(),
                "busybox:1".to_string(),
            ]
            .into_iter()
            .collect()
        );
        // Dropped from the spec: carried VERBATIM at the provisioning
        // stamp (its pods may still run until the reap; the keep-set
        // must keep seeing its ref).
        assert_eq!(provisioning["gone"], *prior.get("gone").unwrap());

        let applied = resolve_units(
            &spec,
            "n1",
            &prior,
            &reconciled,
            InfraNodeStatus::Running,
            &tags,
            false,
        )
        .unwrap();
        // Readiness waited: the prior generation leaves the row here.
        assert_eq!(
            applied["replaced"].image_refs,
            ["weft-infra-bridge:new".to_string(), "busybox:1".to_string()]
                .into_iter()
                .collect()
        );
        assert_eq!(
            applied["frozen"].image_refs,
            ["weft-infra-bridge:old".to_string()].into_iter().collect()
        );
        // The dropped unit's reap completed with the apply: the
        // post-readiness row is rebuilt from the CURRENT spec only, so
        // it disappears here (and with it, its ref leaves the keep-set).
        assert!(!applied.contains_key("gone"));
    }
}
