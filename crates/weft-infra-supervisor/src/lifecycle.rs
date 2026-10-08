//! Lifecycle loop. Claims the `infra_lifecycle_command` rows of the
//! projects this supervisor owns and executes them through the
//! platform's `InfraHost`. Three verbs:
//!
//! - **apply**: resolve the InfraSpec (weft-core) for the copy, apply the
//!   units that are down (or new) through the host, wait for them to be
//!   ready, write the `infra_node` row via `set_applied`. The copy's
//!   id is derived from (project, node, instance),
//!   so every apply
//!   of it, the first after a terminate included, finds the copy's disks
//!   again. (Upstream `Image::Upstream` references pass
//!   through verbatim; mutable tags like `:latest` are NOT resolved to
//!   digests, so a tag rolling underneath changes nothing. See the
//!   authoring docs' "upstream image" limitation.)
//! - **stop**: stop each unit per its `on_stop` (Stop), or leave it
//!   running (KeepRunning); disks are kept.
//! - **terminate**: remove everything the copy runs and owns, keeping
//!   the disks the spec listed; remove the `infra_node` row.

use std::time::Duration;

use anyhow::{anyhow, Result};
use uuid::Uuid;

use weft_broker_client::protocol::SupervisorClaim;
use weft_core::infra::{self, InfraSpec, NodeRef, ResolvedNode, TerminateDisks};
use weft_platform_traits::UnitRunState;

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
/// `image_tags`; a frozen unit carries its `prior` refs forward
/// unchanged. With `transitioning` (the PROVISIONING pre-commit stamp),
/// a reconciled unit records prior ∪ current: its old copy may still
/// run while the apply replaces it, so both generations must stay
/// referenced; the post-readiness (`set_applied`) stamp drops the prior
/// generation, closing the window.
///
/// Also under `transitioning` only: a unit in `prior` but DROPPED from
/// the spec is carried forward verbatim. It is removed later in the same
/// apply, but a cancel or host failure BEFORE the removal leaves the row
/// `Failed` with that unit still running: carried, it keeps its refs in
/// the keep-set and the honest roster shows a unit that may still exist.
/// The post-readiness stamp rebuilds from the CURRENT spec only, so a
/// completed apply drops it (by then it is removed).
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
                // The apply may not have replaced the unit's old copy
                // yet: keep both generations referenced until the
                // post-readiness stamp.
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
            // caller bug; empty refs here would silently drop a running
            // unit's image from the keep-set, so fail instead.
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
                flaky_after_seconds: u.health.flaky_after_seconds.unwrap_or(FLAKY_AFTER.as_secs() as u32),
                recovery_after_seconds: u.health.recovery_after_seconds.unwrap_or(RECOVERY_AFTER.as_secs() as u32),
                image_refs,
            },
        );
    }
    if transitioning {
        // Units dropped from the spec: see the doc block. Verbatim
        // (status included): at stamp time they may still run, so their
        // prior entry is still the truth about them.
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
/// reconciled.
fn units_to_reconcile(
    spec: &InfraSpec,
    prior: &std::collections::BTreeMap<String, weft_broker_client::protocol::UnitRuntime>,
) -> std::collections::HashSet<String> {
    spec.units
        .iter()
        .filter(|u| {
            prior
                .get(&u.name)
                .map(|p| !p.status.expects_running_units())
                // Not in prior = new unit = reconcile it.
                .unwrap_or(true)
        })
        .map(|u| u.name.clone())
        .collect()
}

const READINESS_POLL_INTERVAL: Duration = Duration::from_millis(500);
/// How often the unbounded readiness wait logs a "still waiting" breadcrumb, so
/// a stuck unit is legible without a hard-fail deadline killing a slow but
/// legitimate warmup.
const READINESS_BREADCRUMB_INTERVAL: Duration = Duration::from_secs(30);

/// How often the executing supervisor polls the command's
/// `cancel_requested` flag while inside the readiness wait. Between
/// discrete host steps the check is per-step.
const CANCEL_POLL_INTERVAL: Duration = Duration::from_secs(2);

/// Marker error: the command was HALTED because the user requested
/// cancellation. `tick` maps it to a `cancelled` outcome (never a
/// failure). Cancel = halt, not rollback: the host is not
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

/// How long the lifecycle loop waits after a failed claim before it
/// asks again, so a broker that is down is not asked in a tight loop.
const CLAIM_ERROR_BACKOFF: Duration = Duration::from_secs(1);

/// Claim and run commands for as long as the process lives. Commands that
/// touch different copies run side by side; the ones that touch a copy in
/// common run in the order they were issued (the broker hands out a command
/// only once every older one touching one of its copies has ended,
/// `lifecycle_writes::next_command`): an apply waiting minutes on a slow
/// database's readiness holds up only the copies it touches.
///
/// With nothing waiting, the claim itself sleeps: the broker holds it
/// until a command is issued (or the hold ends). Two things in this supervisor
/// end the hold early and ask again: a command finishing, since the
/// command it was blocking may already be waiting; and the ownership loop
/// reporting a change (`changes`). A project this supervisor took on may
/// have had its command issued while nobody owned it; a project it lost has
/// a new owner, which runs its command again from the start, so the command
/// running here is stopped rather than left issuing host calls for a
/// project that is no longer this supervisor's. When the broker answers that a
/// command waits on a project nobody owns, the ownership loop is asked
/// to tick now.
pub async fn run_loop(
    state: SupervisorState,
    changes: tokio::sync::mpsc::UnboundedReceiver<crate::ownership::OwnershipChange>,
) -> Result<()> {
    work(state, changes, Until::Forever).await
}

/// One pass of [`run_loop`] for a supervisor that scales to zero: claim and
/// run, side by side the same way, every command claimable now and every
/// one that becomes claimable while any runs (issued meanwhile, or freed
/// by one ending), and return once none is running and none is left to
/// claim. A claim that fails with nothing running fails the pass.
pub async fn drain(
    state: SupervisorState,
    changes: tokio::sync::mpsc::UnboundedReceiver<crate::ownership::OwnershipChange>,
) -> Result<()> {
    work(state, changes, Until::Idle).await
}

/// How long [`work`] goes on.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Until {
    Forever,
    Idle,
}

async fn work(
    state: SupervisorState,
    mut changes: tokio::sync::mpsc::UnboundedReceiver<crate::ownership::OwnershipChange>,
    until: Until,
) -> Result<()> {
    let mut running: tokio::task::JoinSet<()> = tokio::task::JoinSet::new();
    // The command each running task runs (its project, its id), and how to
    // stop it.
    let mut busy: std::collections::HashMap<tokio::task::Id, (Uuid, i64, tokio::task::AbortHandle)> =
        std::collections::HashMap::new();
    loop {
        // While anything runs, the claim sleeps on the broker until a
        // command is issued, in a pass too: a command issued for another
        // copy mid-pass starts beside the running ones rather than after
        // them. A pass with nothing running asks once and ends on nothing.
        let hold = match (until, busy.is_empty()) {
            (Until::Idle, true) => Duration::ZERO,
            _ => weft_broker_client::protocol::MAX_HOLD,
        };
        let busy_commands: Vec<i64> = busy.values().map(|(_, command, _)| *command).collect();
        // Ownership first: a claim can only hand out a project this supervisor
        // took back AFTER the loss that is already queued here, so the
        // loss is applied before that claim spawns anything, and never
        // stops the command the new claim started. Dropping a claim that
        // lost the race is free: claiming marks nothing on the broker.
        tokio::select! {
            biased;
            change = changes.recv() => {
                let change = change.ok_or_else(|| {
                    anyhow!("the ownership loop is gone; the lifecycle loop cannot follow what this supervisor owns")
                })?;
                busy.retain(|_, (project_id, _, task)| {
                    if !change.lost.contains(project_id) {
                        return true;
                    }
                    tracing::info!(
                        %project_id,
                        "this supervisor no longer owns the project; stopping its running command, the new owner runs it again"
                    );
                    task.abort();
                    false
                });
            }
            claimed = state.broker.claim_command(&state.replica, &busy_commands, hold) => match claimed {
                Ok(SupervisorClaim::Command(cmd)) => {
                    let (project_id, command_id) = (cmd.project_id, cmd.id);
                    let task_state = state.clone();
                    let task = running.spawn(async move {
                        if let Err(e) = run_command(&task_state, cmd).await {
                            tracing::warn!(error = %e, "lifecycle command could not be recorded");
                        }
                    });
                    busy.insert(task.id(), (project_id, command_id, task));
                }
                // A pass with nothing running is done: the project nobody
                // owns is taken on by the ownership tick it asked for, and
                // its command runs in the next pass.
                Ok(SupervisorClaim::UnownedWork) => {
                    state.ownership_wanted.notify_one();
                    if until == Until::Idle && busy.is_empty() {
                        return Ok(());
                    }
                }
                Ok(SupervisorClaim::Nothing) => {
                    if until == Until::Idle && busy.is_empty() {
                        return Ok(());
                    }
                }
                // A pass with nothing running fails, so the tick that ran it
                // fails and is run again; one with commands running lets
                // them finish and asks again.
                Err(e) if until == Until::Idle && busy.is_empty() => return Err(e.context("claim a lifecycle command")),
                Err(e) => {
                    tracing::warn!(error = %e, "lifecycle claim failed");
                    state.clock.sleep(CLAIM_ERROR_BACKOFF).await;
                }
            },
            Some(done) = running.join_next_with_id() => {
                match done {
                    Ok((id, ())) => {
                        busy.remove(&id);
                    }
                    // Stopped above, its busy entry already gone.
                    Err(e) if e.is_cancelled() => {}
                    // A command that panicked leaves its project's state
                    // unknown to this supervisor: exit, so the process restarts clean.
                    Err(e) => return Err(anyhow!("a lifecycle command panicked: {e}")),
                }
            }
        }
    }
}

/// Claim one command, holding up to `wait` for one to be issued, and
/// run it. Returns true when work was done.
///
/// Exposed for integration tests, which step the loop one command at a
/// time with no wait; the real [`run_loop`] claims and runs side by side.
pub async fn tick(state: &SupervisorState, wait: Duration) -> Result<bool> {
    let SupervisorClaim::Command(cmd) = state
        .broker
        .claim_command(&state.replica, &[], wait)
        .await?
    else {
        return Ok(false);
    };
    run_command(state, cmd).await?;
    Ok(true)
}

/// Run one claimed command and record how it ended.
async fn run_command(
    state: &SupervisorState,
    cmd: weft_broker_client::protocol::SupervisorCommandRow,
) -> Result<()> {
    tracing::info!(
        command_id = cmd.id,
        project_id = %cmd.project_id,
        node_id = ?cmd.node_id,
        verb = %cmd.verb,
        "lifecycle command claimed"
    );
    // Shared until the command is recorded, so the ownership loop's sweep
    // never deletes a copy this command is building (`ProjectLocks`).
    let _project = state.project_locks.share(cmd.project_id).await;
    let result = execute(state, &cmd).await;
    // A user-honored cancel is its own outcome, never a failure.
    let cancelled = result
        .as_ref()
        .err()
        .map(|e| e.downcast_ref::<CancelledByUser>().is_some())
        .unwrap_or(false);
    let error = result.as_ref().err().map(|e| e.to_string());
    // `command_complete` is Gone if the row was already completed
    // (remove_node cascade cancelled it) and Displaced if this supervisor no
    // longer owns the project (drain / lease takeover moved it
    // mid-command). In the displaced case the command stays
    // UNCOMPLETED on purpose, so the new owner re-runs and finishes it
    // (the user never re-acts). Either way, log + move on; neither is
    // a failure of this command's run.
    match state
        .broker
        .command_complete(&state.replica, cmd.id, error.as_deref(), cancelled)
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
    Ok(())
}

/// Which copy a row of `project` names, as the host knows it.
fn node_ref(project: &weft_broker_client::protocol::SupervisorProject, node_id: &str, copy_id: &str) -> NodeRef {
    NodeRef {
        tenant: project.tenant_id.clone(),
        project: project.project_id,
        node: node_id.to_string(),
        copy_id: copy_id.to_string(),
    }
}

/// The copies `cmd` names that the host still holds but no row does: what
/// is left of a copy an earlier terminate took down keeping its listed
/// disks. A copy's id is derived from its project, node and instance, so
/// the ones `cmd.copies` admits are recognized by id. A copy with a row
/// is never one of these: the command reaches it through its row.
async fn rowless_copies(
    state: &SupervisorState,
    cmd: &weft_broker_client::protocol::SupervisorCommandRow,
    rows: &[weft_broker_client::protocol::SupervisorInfraNode],
) -> Result<Vec<NodeRef>> {
    use weft_core::instance::Copies;
    let with_row: std::collections::HashSet<&str> = rows.iter().map(|n| n.copy_id.as_str()).collect();
    let named = |copy: &NodeRef| match &cmd.copies {
        Copies::Every => true,
        Copies::Shared => copy.copy_id == NodeRef::copy_id(cmd.project_id, &copy.node, None),
        Copies::Instance(instance) => {
            copy.copy_id == NodeRef::copy_id(cmd.project_id, &copy.node, Some(instance))
        }
    };
    Ok(state
        .host
        .copies()
        .await?
        .into_iter()
        .filter(|c| c.project == cmd.project_id)
        .filter(|c| cmd.node_id.as_ref().is_none_or(|node| *node == c.node))
        .filter(|c| !with_row.contains(c.copy_id.as_str()))
        .filter(named)
        .collect())
}

/// The project a claimed command belongs to. The command was only
/// claimable because this supervisor owns the project (the broker's
/// claim ownership predicate), so it is in the owned set.
async fn owned_project(
    state: &SupervisorState,
    project_id: Uuid,
) -> Result<weft_broker_client::protocol::SupervisorProject> {
    state
        .broker
        .owned_projects(&state.replica)
        .await?
        .into_iter()
        .find(|p| p.project_id == project_id)
        .ok_or_else(|| anyhow!("project not in the supervisor's owned set"))
}

/// Block until every unit in `units` of the copy `node` is ready. A unit
/// the host reports failed fails the apply with the host's words. The
/// supervisor uses this to gate the post-apply `set_applied` write so
/// downstream `endpoint_url` queries return live addresses.
async fn wait_for_readiness(
    state: &SupervisorState,
    command_id: i64,
    project_id: Uuid,
    node_id: &str,
    instance: Option<&weft_core::instance::InstanceId>,
    node: &NodeRef,
    units: &std::collections::HashSet<String>,
) -> Result<()> {
    // A user apply is a user-controlled operation: a slow-warmup unit (a
    // model server pulling weights) can legitimately take a long time, so
    // this wait is NOT capped by a fixed hard-fail deadline. The user
    // interrupts a unit that will never come up via cancel (polled at
    // CANCEL_POLL_INTERVAL); a periodic breadcrumb makes a stuck readiness
    // legible in the logs rather than a silent hang. Unlike the wait for
    // running work a stop does first (the dispatcher's, capped by the
    // person's deadline, `weft_dispatcher::drain`), there is nothing to
    // cancel instead of waiting: the unit either comes up or the user
    // stops it.
    let mut next_cancel_check = state.clock.now();
    let mut next_breadcrumb = state.clock.now() + READINESS_BREADCRUMB_INTERVAL;
    // What the row says it waits on, rewritten only when it changes.
    let mut recorded = String::new();
    loop {
        if state.clock.now() >= next_cancel_check {
            check_cancel(state, command_id, "waiting for the units to be ready").await?;
            next_cancel_check = state.clock.now() + CANCEL_POLL_INTERVAL;
        }
        let seen = state.host.observe(&node.tenant, node.project).await?;
        let mut waiting = Vec::new();
        for unit in units {
            match seen.iter().find(|o| o.copy_id == node.copy_id && &o.unit == unit).map(|o| &o.state) {
                Some(UnitRunState::Ready) => {}
                Some(UnitRunState::Failed { why }) => {
                    return Err(anyhow!("unit '{unit}' could not start: {why}"));
                }
                Some(UnitRunState::NotReady { why }) => waiting.push(format!("{unit}: {why}")),
                Some(UnitRunState::Starting { step }) => waiting.push(format!("{unit}: {step}")),
                Some(UnitRunState::Stopped) => waiting.push(format!("{unit}: stopped")),
                None => waiting.push(format!("{unit}: not reported by its host yet")),
            }
        }
        if waiting.is_empty() {
            return Ok(());
        }
        let now_waiting = waiting.join("; ");
        if now_waiting != recorded
            && record_waiting(state, command_id, project_id, node_id, instance, &node.copy_id, &now_waiting).await
        {
            recorded = now_waiting;
        }
        if state.clock.now() >= next_breadcrumb {
            tracing::info!(
                target: "weft_infra_supervisor::lifecycle",
                copy_id = %node.copy_id,
                waiting = %waiting.join(", "),
                "still waiting for infra units to be ready (cancel the apply to stop waiting)"
            );
            next_breadcrumb = state.clock.now() + READINESS_BREADCRUMB_INTERVAL;
        }
        state.clock.sleep(READINESS_POLL_INTERVAL).await;
    }
}

/// Record on the copy's row what the command `command_id` waits on, for
/// `weft status`; true once the broker answered. A copy the command no
/// longer reaches, or a project another supervisor took, is the command's
/// next fenced write to end: the record is only what a person reads, so
/// a failure is logged and the work goes on.
async fn record_waiting(
    state: &SupervisorState,
    command_id: i64,
    project_id: Uuid,
    node_id: &str,
    instance: Option<&weft_core::instance::InstanceId>,
    copy_id: &str,
    waiting: &str,
) -> bool {
    match state.broker.set_waiting(&state.replica, command_id, project_id, node_id, instance, waiting).await {
        Ok(_) => true,
        Err(e) => {
            tracing::warn!(
                target: "weft_infra_supervisor::lifecycle",
                %copy_id, error = %format!("{e:#}"),
                "could not record what the command waits on"
            );
            false
        }
    }
}

async fn execute(
    state: &SupervisorState,
    cmd: &weft_broker_client::protocol::SupervisorCommandRow,
) -> Result<()> {
    use weft_broker_client::protocol::InfraLifecycleVerb;
    // Apply is the only verb that doesn't operate on existing
    // infra_node rows; it creates / updates one. Route early.
    if cmd.verb == InfraLifecycleVerb::Apply {
        return execute_apply(state, cmd).await;
    }
    // What runs on these copies was settled before the command reached
    // a supervisor: cancelled by the dispatcher as it issued it, or waited
    // for by the dispatcher's claimer (`weft_dispatcher::drain`).
    // What a terminate does with the disks its nodes keep is the
    // command's answer (a person's terminate keeps them, an instance's
    // wipe deletes them), read before anything is touched so a row without it
    // fails the command instead of guessing.
    let disks = match cmd.verb {
        InfraLifecycleVerb::Terminate => Some(cmd.terminate_work().map_err(|e| anyhow!(e))?.disks),
        _ => None,
    };
    let nodes = state.broker.infra_nodes(cmd.project_id).await?;
    let targets: Vec<&weft_broker_client::protocol::SupervisorInfraNode> = match &cmd.node_id {
        Some(node_id) => nodes
            .iter()
            .filter(|n| n.node_id == *node_id && cmd.copies.admits(n.instance.as_ref()))
            .collect(),
        None => nodes.iter().filter(|n| cmd.copies.admits(n.instance.as_ref())).collect(),
    };
    // A terminate that deletes every disk also reaches the copies with no
    // row left: ones an earlier terminate took down keeping their listed
    // disks, which only the host still holds.
    let rowless = match disks {
        Some(TerminateDisks::DeleteAll) => rowless_copies(state, cmd, &nodes).await?,
        _ => Vec::new(),
    };
    if targets.is_empty() && rowless.is_empty() {
        // No matching rows is a soft no-op, not a failure. Happens when
        // the user clicks Stop / Terminate several times in quick
        // succession: the first command already deleted (or cleared) the
        // `infra_node` row(s), so follow-up commands have nothing left to
        // act on.
        tracing::info!(
            command_id = cmd.id,
            project_id = %cmd.project_id,
            node_id = ?cmd.node_id,
            verb = %cmd.verb,
            "lifecycle command: no matching infra_node rows; completing as no-op"
        );
        return Ok(());
    }
    let project = owned_project(state, cmd.project_id).await?;

    match cmd.verb {
        InfraLifecycleVerb::Stop => {
            let seen = state.host.observe(&project.tenant_id, project.project_id).await?;
            for n in &targets {
                // Interruptible between nodes: already-stopped units
                // stay stopped (halt, not rollback); the rest keep their
                // prior status.
                check_cancel(state, cmd.id, "stopping infra nodes").await?;
                let copy = node_ref(&project, &n.node_id, &n.copy_id);
                // A unit the host runs for this copy that the row's roster
                // does not carry is an orphan: a unit dropped from the
                // spec whose removal never landed. Stopping it regardless
                // of `force` finishes that intent. Never STAMPED: the
                // broker fences per-unit stamps on roster membership, and
                // the honest record for an orphan is its absence.
                for orphan in seen.iter().filter(|o| o.copy_id == n.copy_id && !n.units.contains_key(&o.unit)) {
                    tracing::warn!(
                        project_id = %cmd.project_id,
                        node_id = %n.node_id,
                        unit = %orphan.unit,
                        "stopping an orphan unit with no roster entry; not stamping the row"
                    );
                    state.host.stop_unit(&copy, &orphan.unit).await?;
                }
                // Per-unit stop: a unit is stopped only if its
                // `stop_behavior` is Stop (or `force`). A KeepRunning unit
                // (a license server, a slow-warmup model) survives stop
                // and is only removed by terminate; it keeps its status,
                // so the node rollup reflects "partly running".
                let mut any_stopped = false;
                for (unit, runtime) in &n.units {
                    if !(cmd.force || runtime.stop_behavior == weft_core::StopBehavior::Stop) {
                        continue;
                    }
                    // Flip THIS unit to the `stopping` transient right
                    // before its stop (not upfront for the whole set): a
                    // cancel that landed earlier left the not-yet-reached
                    // units in their resting status. UI hint only, so a
                    // broker failure here is logged, not fatal.
                    if let Err(e) = state
                        .broker
                        .set_status(
                            &state.replica,
                            Some(cmd.id),
                            cmd.project_id,
                            &n.node_id,
                            n.instance.as_ref(),
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
                            "set_status(stopping) failed; continuing with the stop"
                        );
                    }
                    // The host's stop can take minutes (a machine
                    // powering off): say what the stop is on.
                    let waiting = format!("{unit}: its host is taking it down");
                    record_waiting(state, cmd.id, cmd.project_id, &n.node_id, n.instance.as_ref(), &n.copy_id, &waiting)
                        .await;
                    state.host.stop_unit(&copy, unit).await?;
                    let outcome = state
                        .broker
                        .set_status(
                            &state.replica,
                            Some(cmd.id),
                            cmd.project_id,
                            &n.node_id,
                            n.instance.as_ref(),
                            Some(unit),
                            weft_broker_client::protocol::InfraNodeStatus::Stopped,
                            None,
                            None,
                        )
                        .await?;
                    match outcome {
                        weft_broker_client::WriteOutcome::Applied(_) => any_stopped = true,
                        weft_broker_client::WriteOutcome::Displaced => {
                            // Project ownership moved: this supervisor must
                            // not keep stopping the remaining nodes. The new
                            // owner re-runs the (idempotent) stop, and
                            // command_complete is displaced for the same
                            // reason, so the command is left for it.
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
                            // while the project is still ours. Nothing left
                            // to record for the node; on to the next one.
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
                // One Stopped event per node that actually stopped a unit
                // (the event rail is node-scoped; the per-unit detail
                // lives in the row's units map).
                if any_stopped {
                    state
                        .broker
                        .event_record(
                            cmd.project_id,
                            Some(&n.node_id),
                            n.instance.as_ref(),
                            weft_broker_client::protocol::InfraEvent::Stopped,
                        )
                        .await?;
                }
            }
        }
        InfraLifecycleVerb::Terminate => {
            for n in &targets {
                // Interruptible between nodes: already-terminated nodes
                // are gone; remaining nodes keep their rows (visible
                // partial state the user acts on per node).
                //
                // The `terminating` transient is flipped HERE, per node,
                // right before this node's removal, so a cancel that lands
                // before a node is reached leaves it in its prior RESTING
                // status.
                //
                // The stamp is REQUIRED before the removal, never
                // best-effort: it is the durable record that this copy is
                // being torn down. Were the removal to run without it and
                // `remove_node` then fail, the row would keep saying Running
                // with the old applied hash while nothing runs, and every
                // later apply would skip on the hash match, unable to repair
                // it. Stamped, the row says Terminating, which is the one
                // status the next apply refuses to work on in place: it
                // finishes this removal first and starts the copy fresh. Displaced means
                // ownership moved: the new owner re-runs the terminate. Gone
                // means the row vanished under us (a project removal in
                // flight): the removal below still runs.
                check_cancel(state, cmd.id, "terminating infra nodes").await?;
                match state
                    .broker
                    .set_status(
                        &state.replica,
                        Some(cmd.id),
                        cmd.project_id,
                        &n.node_id,
                        n.instance.as_ref(),
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
                        "set_status(terminating): row gone; removing the copy anyway"
                    ),
                }
                // The disks the node lists were carried on the row at
                // apply time (from `InfraSpec.keep_on_terminate`): the
                // supervisor has no spec at terminate time, but it has the
                // row. Whether they stay is the command's answer.
                let keep = disks.expect("a terminate read its disks above").kept(&n.keep_disks);
                let removing = "its host is removing it";
                record_waiting(state, cmd.id, cmd.project_id, &n.node_id, n.instance.as_ref(), &n.copy_id, removing).await;
                state.host.terminate(&node_ref(&project, &n.node_id, &n.copy_id), keep).await?;
                if !state
                    .broker
                    .remove_node(&state.replica, cmd.project_id, &n.node_id, n.instance.as_ref(), cmd.id)
                    .await?
                    .is_applied()
                {
                    // Lost ownership mid-Terminate. Abort: leave the
                    // command uncompleted so the new owner re-runs the
                    // (idempotent) terminate.
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
                        cmd.project_id,
                        Some(&n.node_id),
                        n.instance.as_ref(),
                        weft_broker_client::protocol::InfraEvent::Terminated,
                    )
                    .await?;
            }
            for copy in &rowless {
                check_cancel(state, cmd.id, "deleting the disks of infra copies already down").await?;
                state.host.terminate(copy, &[]).await?;
            }
        }
        InfraLifecycleVerb::Apply => {
            // Apply is routed at the top of the function before this
            // match; exhaustive matching (no catch-all) makes a new verb
            // a compile error rather than a silent fallthrough.
            unreachable!("Apply is routed before the verb match");
        }
        InfraLifecycleVerb::Deactivate | InfraLifecycleVerb::Reactivate | InfraLifecycleVerb::Upgrade => {
            // The supervisor's `claim_command` filters these out (they're
            // dispatcher-claimable); if one ever lands here it's a routing
            // bug at the broker, fail loud.
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
    let node_id = cmd.node_id.as_deref().ok_or_else(|| anyhow!("apply command missing node_id"))?;
    // An apply builds exactly one copy: the shared one, or one
    // instance's.
    let instance = match &cmd.copies {
        weft_core::instance::Copies::Shared => None,
        weft_core::instance::Copies::Instance(i) => Some(i),
        weft_core::instance::Copies::Every => {
            return Err(anyhow!("apply command names every copy; an apply builds exactly one"))
        }
    };
    let spec_value = cmd.spec_json.as_ref().ok_or_else(|| anyhow!("apply command missing spec_json"))?;
    let spec: InfraSpec = serde_json::from_value(spec_value.clone()).map_err(|e| anyhow!("deserialize spec_json: {e}"))?;
    let project = owned_project(state, cmd.project_id).await?;

    // Per-(project, node) image tag map, used to resolve
    // `Image::Local { name }` references.
    let image_tags: std::collections::BTreeMap<String, String> =
        state.broker.project_image_tags(cmd.project_id, node_id).await?.into_iter().collect();

    // Read the prior infra_node row. Drives skip / fresh / replace.
    let prior = state
        .broker
        .infra_nodes(cmd.project_id)
        .await?
        .into_iter()
        .find(|n| n.node_id == node_id && n.instance.as_ref() == instance);

    // The copy's id is derived from what it is a copy of, so it is the
    // same on every apply, a Fresh one after a terminate included: that
    // is how a disk kept through the terminate is found again. What the
    // prior row decides is only whether to work in place or to finish a
    // terminate that did not complete first.
    let copy_id = NodeRef::copy_id(cmd.project_id, node_id, instance);
    let mode = match prior.as_ref() {
        Some(p) if p.status.applies_in_place() => ApplyMode::ReplaceOrSkip,
        _ => ApplyMode::Fresh,
    };
    let copy = node_ref(&project, node_id, &copy_id);

    // Resolve, then refuse what this host cannot run (a GPU it lacks), at
    // the earliest point: before any row is written.
    let resolved = infra::resolve(&spec, &copy, &image_tags).map_err(|e| anyhow!("{e}"))?;
    state.host.check(&resolved).map_err(|why| anyhow!("{why}"))?;
    // What the host runs differently from what was asked (a GPU kind it
    // cannot choose): stamped on the row with the apply, where every
    // status read shows it to the person who started the node.
    let notes = state.host.notes(&resolved);
    let applied_spec_hash = resolved.hash();

    // Per-unit apply. Reconcile only the units that are DOWN (or new);
    // leave UP units (Running/Flaky) frozen at their current version,
    // because something downstream depends on them running. Up units are
    // taken down only by an explicit force-stop, never by apply.
    let prior_units: std::collections::BTreeMap<String, weft_broker_client::protocol::UnitRuntime> =
        prior.as_ref().map(|p| p.units.clone()).unwrap_or_default();
    let reconcile = units_to_reconcile(&spec, &prior_units);

    // Full skip: every declared unit is already up and the hash matches.
    // The host already runs what we want; no host call. The row keeps
    // its copy id, hash, endpoints.
    let hash_matches = prior.as_ref().and_then(|p| p.applied_spec_hash.as_deref()) == Some(applied_spec_hash.as_str());
    if matches!(mode, ApplyMode::ReplaceOrSkip) && reconcile.is_empty() && hash_matches {
        // Re-fire `started` so the dispatcher's SSE bus wakes any
        // subscribers waiting on this command. Nothing else changed.
        state
            .broker
            .event_record(
                cmd.project_id,
                Some(node_id),
                instance,
                weft_broker_client::protocol::InfraEvent::Started(weft_broker_client::protocol::StartedPayload {
                    copy_id: copy_id.clone(),
                    mode: weft_broker_client::protocol::StartMode::Skip,
                }),
            )
            .await?;
        return Ok(());
    }

    // A Fresh apply over an existing row means the row is `Terminating`
    // (the only status an apply does not work on in place): a terminate
    // stamped it and then failed or died before its removal landed, so
    // the PRIOR copy can still be running. Finish the terminate first,
    // with the keep list the row carries, and do it BEFORE the
    // provisioning stamp below overwrites that list: after that stamp
    // the prior one lives nowhere durable. The copy has the same copy
    // id either way, so the kept disks are the ones this apply adopts.
    // The ownership fence the provisioning stamp provides (a supervisor
    // that lost the project's lease must not touch its infra) is taken
    // here by re-stamping the row's own `Terminating` through the
    // command-gated write: Displaced means the lease moved (leave the
    // apply for the owner), and Gone means the row or the command
    // vanished under a running apply, which fails loud.
    if let (ApplyMode::Fresh, Some(p)) = (&mode, prior.as_ref()) {
        match state
            .broker
            .set_status(
                &state.replica,
                Some(cmd.id),
                cmd.project_id,
                node_id,
                instance,
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
                     the prior copy could be finished; re-run the apply"
                ));
            }
        }
        state.host.terminate(&node_ref(&project, node_id, &p.copy_id), &p.keep_disks).await?;
    }

    // Pre-apply commitment: write the infra_node row before any host call
    // so a partial-apply failure leaves a visible row the user can
    // Terminate. Reconciled units go Provisioning; up units keep their
    // (Running/Flaky) status.
    let provision_outcome = state
        .broker
        .set_provisioning(
            &state.replica,
            cmd.id,
            cmd.project_id,
            node_id,
            instance,
            &copy_id,
            spec.keep_on_terminate.clone(),
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
        // Provisioning row; the new owner re-runs the apply from scratch.
        // Gone: the command is already completed (a node removal
        // cancelled it), so there is nothing left to apply for.
        tracing::info!(
            project_id = %cmd.project_id,
            node_id = %node_id,
            outcome = ?provision_outcome,
            "set_provisioning not applied; leaving the apply"
        );
        return Ok(());
    }

    let start_mode = if matches!(mode, ApplyMode::ReplaceOrSkip) {
        weft_broker_client::protocol::StartMode::Replace
    } else {
        weft_broker_client::protocol::StartMode::Fresh
    };
    let apply_result: Result<weft_broker_client::protocol::AppliedEndpoints> = async {
        // Interruptible between the steps below. A cancel mid-apply bails
        // through the error path, which stamps the node
        // `Failed("cancelled by user (...)")`: the honest resting state for
        // a half-applied node (the host is not transactional; the user
        // terminates or retries from there), while the command outcome is
        // recorded as `cancelled`, not failed.
        check_cancel(state, cmd.id, "before removing units the spec dropped").await?;
        // Units the prior roster carries and the spec no longer declares.
        for unit in prior_units.keys().filter(|u| resolved.unit(u).is_none()) {
            state.host.remove_unit(&copy, unit).await?;
        }
        check_cancel(state, cmd.id, "before applying units").await?;
        for unit in &reconcile {
            state.host.apply_unit(&resolved, unit).await?;
        }
        wait_for_readiness(state, cmd.id, cmd.project_id, node_id, instance, &copy, &reconcile).await?;
        endpoint_addresses(state, &resolved).await
    }
    .await;
    let addresses = match apply_result {
        Ok(addresses) => addresses,
        Err(e) => {
            let msg = e.to_string();
            // Best-effort row-status hint. The PRIMARY error record is
            // `infra_lifecycle_command.outcome=failed` (written after we
            // bubble); the action bar reads from there. This write
            // additionally stamps `infra_node.status=Failed +
            // failure_message` so the node-level UI sees the error too.
            if let Err(status_err) = state
                .broker
                .set_status(
                    &state.replica,
                    Some(cmd.id),
                    cmd.project_id,
                    node_id,
                    instance,
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
            &state.replica,
            cmd.id,
            cmd.project_id,
            node_id,
            instance,
            &copy_id,
            &applied_spec_hash,
            addresses,
            spec.keep_on_terminate.clone(),
            notes,
            // `transitioning = false`: readiness waited, the reconciled
            // units' old copies are replaced, so their PRIOR image refs
            // leave the row here (the keep-set window closes with this
            // stamp).
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
        // Displaced: project ownership moved mid-apply; don't fire the
        // Started event and don't complete the command; the new owner
        // re-runs the (idempotent) apply. Gone: the command was completed
        // under us; nothing to record.
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
            cmd.project_id,
            Some(node_id),
            instance,
            weft_broker_client::protocol::InfraEvent::Started(weft_broker_client::protocol::StartedPayload {
                copy_id: copy_id.clone(),
                mode: start_mode,
            }),
        )
        .await?;
    Ok(())
}

enum ApplyMode {
    /// No usable prior state: apply every unit from scratch (disks kept
    /// through a terminate are adopted under the copy's same names).
    Fresh,
    /// A usable prior row. Either skip (if the hash matches and every
    /// unit is up) or replace the down units. The choice is made after
    /// resolving, when we have the new hash to compare against the stored
    /// one.
    ReplaceOrSkip,
}

/// Where each declared endpoint answers, as the host gives it once the
/// units run: the address the project's workers use, the front-door path
/// of a `Public` one, and the install-network address of a `SameNetwork`
/// one.
async fn endpoint_addresses(
    state: &SupervisorState,
    resolved: &ResolvedNode,
) -> Result<weft_broker_client::protocol::AppliedEndpoints> {
    let mut out = weft_broker_client::protocol::AppliedEndpoints::default();
    for ep in &resolved.spec.endpoints {
        let at = state.host.endpoint(resolved, &ep.name).await?;
        out.urls.insert(ep.name.clone(), at.url);
        out.install_urls.insert(ep.name.clone(), at.install_url);
        match &ep.expose {
            infra::Expose::Project => {}
            infra::Expose::Public { path } => {
                out.public_paths
                    .insert(ep.name.clone(), infra::public_path(resolved.node.project, &resolved.node.copy_id, path));
            }
            infra::Expose::SameNetwork => {
                let door = at.same_network.ok_or_else(|| {
                    anyhow!("endpoint '{}' is open to the install's network, but the host gave it no address there", ep.name)
                })?;
                out.doors.insert(ep.name.clone(), door);
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The image-ref bookkeeping that lets the image keep-set keep
    /// what running units actually use while the project's tag map has
    /// moved on. Three rules, one test: a FROZEN up unit carries its
    /// prior refs forward untouched; the PROVISIONING stamp unions a
    /// reconciled unit's prior refs with its new ones (its old copy may
    /// still run while the apply replaces it); the post-readiness stamp
    /// drops the prior generation. Local names resolve through the tag
    /// map, upstream literals pass through.
    #[test]
    fn resolve_units_records_the_refs_running_units_use() {
        use std::collections::{BTreeMap, HashSet};
        use weft_broker_client::protocol::{InfraNodeStatus, UnitRuntime};
        use weft_core::infra::*;

        fn runtime(status: InfraNodeStatus, refs: &[&str]) -> UnitRuntime {
            UnitRuntime {
                status,
                stop_behavior: weft_core::StopBehavior::Stop,
                flaky_after_seconds: 1,
                recovery_after_seconds: 1,
                image_refs: refs.iter().map(|s| s.to_string()).collect(),
            }
        }

        let spec = InfraSpec {
            units: vec![
                Unit {
                    name: "frozen".into(),
                    containers: vec![Container::new("c", Image::Local { name: "bridge".into() })],
                    ..Default::default()
                },
                Unit {
                    name: "replaced".into(),
                    containers: vec![
                        Container::new("c", Image::Local { name: "bridge".into() }),
                        Container::new("sidecar", Image::Upstream { reference: "busybox:1".into() }),
                    ],
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let mut prior = BTreeMap::new();
        prior.insert("frozen".into(), runtime(InfraNodeStatus::Running, &["weft-infra-bridge:old"]));
        prior.insert("replaced".into(), runtime(InfraNodeStatus::Stopped, &["weft-infra-bridge:old"]));
        prior.insert("gone".into(), runtime(InfraNodeStatus::Running, &["weft-infra-gone:0ld"]));
        let reconciled: HashSet<String> = ["replaced".to_string()].into_iter().collect();
        let tags: BTreeMap<String, String> =
            [("bridge".to_string(), "weft-infra-bridge:new".to_string())].into_iter().collect();

        let provisioning =
            resolve_units(&spec, "n1", &prior, &reconciled, InfraNodeStatus::Provisioning, &tags, true).unwrap();
        assert_eq!(provisioning["frozen"].image_refs, ["weft-infra-bridge:old".to_string()].into_iter().collect());
        assert_eq!(
            provisioning["replaced"].image_refs,
            ["weft-infra-bridge:old".to_string(), "weft-infra-bridge:new".to_string(), "busybox:1".to_string()]
                .into_iter()
                .collect()
        );
        assert_eq!(provisioning["gone"], *prior.get("gone").unwrap());

        let applied = resolve_units(&spec, "n1", &prior, &reconciled, InfraNodeStatus::Running, &tags, false).unwrap();
        assert_eq!(
            applied["replaced"].image_refs,
            ["weft-infra-bridge:new".to_string(), "busybox:1".to_string()].into_iter().collect()
        );
        assert_eq!(applied["frozen"].image_refs, ["weft-infra-bridge:old".to_string()].into_iter().collect());
        assert!(!applied.contains_key("gone"));
    }
}
