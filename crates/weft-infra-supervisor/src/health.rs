//! Health loop. Polls the tenant's project namespaces for replica
//! readiness, evaluates the project's HealthProtocols, and emits
//! `infra_event` rows via the broker when a node's status changes.
//!
//! Two concerns:
//!   1. Per-node windowed health-state transitions (flaky after N
//!      seconds below threshold, recovered after N seconds above).
//!      Drives `infra_event` flaky / recovered + `infra_node.status`.
//!   2. HealthProtocols evaluation: ordered (condition → action)
//!      rules. First match fires; while a project's protocol is in
//!      flight, subsequent matches queue (next tick re-checks once
//!      the current one settles).

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use anyhow::Result;
use weft_core::truncate_user_string;

use crate::health_engine::{
    evaluate_node_health, evaluate_protocols, all_units_healthy, NodeHealthState,
    NodeDecision, NodeEdgeEvent, NodeObservation, ProtocolEvalInputs,
};
use crate::protocol::{self, HealthProtocol, ProtocolAction};
use crate::SupervisorState;

/// Exponential-backoff state for a (project, protocol) whose action
/// keeps failing. Without this, an action that fails fast (broker
/// rejects immediately) re-fires every poll interval (~5s) forever
/// while infra stays broken, flooding the lifecycle-command table.
#[derive(Clone)]
struct BackoffState {
    /// Consecutive failures since the last success. Drives the delay.
    consecutive_failures: u32,
    /// Earliest instant the protocol may re-fire. Until then, a match
    /// is skipped.
    next_retry_at: Instant,
}

/// Backoff delay for the Nth consecutive failure: 5s, 10s, 20s, 40s,
/// doubling, capped at 5 min. The action still self-heals (it keeps
/// retrying), just not in a tight loop while broken.
fn backoff_delay(consecutive_failures: u32) -> Duration {
    const BASE_SECS: u64 = 5;
    const CAP_SECS: u64 = 300;
    // failures>=1 here; shift caps at a large exponent to avoid overflow.
    let shift = consecutive_failures.saturating_sub(1).min(20);
    let secs = (BASE_SECS.saturating_mul(1u64 << shift)).min(CAP_SECS);
    Duration::from_secs(secs)
}

#[derive(Default)]
pub struct HealthRegistry {
    /// Per-(project, node, unit) state tracking. Records when the
    /// unit was last seen Ready vs Not-Ready so we can apply windowed
    /// transitions (flaky_after, recovery_after). Health is per-unit.
    state: HashMap<(String, String, String), NodeHealthState>,
    /// Per-project "currently in flight" protocol names. While a
    /// protocol is in flight, the supervisor doesn't re-fire any
    /// protocol for that project (avoids action storms when health
    /// flaps mid-action).
    in_flight: HashSet<String>,
    /// Per-project "already fired" protocol names since the most
    /// recent recovery. Re-armed when every condition in the project
    /// becomes false (i.e. the project is fully healthy).
    fired: HashMap<String, HashSet<String>>,
    /// Per-(project, protocol) exponential backoff after a failed
    /// action. A matched protocol whose entry's `next_retry_at` is in
    /// the future is skipped this tick. Cleared on success.
    backoff: HashMap<(String, String), BackoffState>,
}

impl HealthRegistry {
    /// True if a protocol action is currently in flight for the
    /// project. Exposed for introspection (e.g. asserting a hung
    /// action's timeout freed the slot).
    pub fn is_in_flight(&self, project_id: &str) -> bool {
        self.in_flight.contains(project_id)
    }

    /// True if `protocol_name` is latched in the project's fired set
    /// (a successful action suppresses re-fire until re-arm). Exposed
    /// for introspection (e.g. asserting a failed action un-latched).
    pub fn is_fired(&self, project_id: &str, protocol_name: &str) -> bool {
        self.fired
            .get(project_id)
            .is_some_and(|names| names.contains(protocol_name))
    }

    /// Consecutive-failure count for a (project, protocol)'s backoff,
    /// 0 if none. Exposed for introspection (e.g. asserting a failed
    /// action armed backoff and a success cleared it).
    pub fn backoff_failures(&self, project_id: &str, protocol_name: &str) -> u32 {
        self.backoff
            .get(&(project_id.to_string(), protocol_name.to_string()))
            .map(|b| b.consecutive_failures)
            .unwrap_or(0)
    }
}

pub async fn run_loop(state: SupervisorState) -> Result<()> {
    loop {
        if let Err(e) = tick(&state).await {
            tracing::warn!(error = %e, "health tick failed");
        }
        state.clock.sleep(state.poll_interval).await;
    }
}

/// One iteration of the health loop. Exposed (rather than only
/// running inside `run_loop`) so integration tests can step the
/// loop one tick at a time.
pub async fn tick(state: &SupervisorState) -> Result<()> {
    let projects = state.broker.owned_projects(&state.pod_name).await?;
    for project in &projects {
        if let Err(e) = tick_project(state, project).await {
            tracing::warn!(
                project_id = %project.project_id,
                error = %e,
                "health tick (project) failed"
            );
        }
    }

    // Sweep ALL per-project registry maps for projects that no longer
    // exist (deleted between ticks). A deleted project is never
    // iterated again, so its entries would leak forever without this.
    // `tick` is the only place that sees the full live set. (The
    // per-node `state` map is also pruned inside tick_project when a
    // node leaves running, but a deleted project's nodes never get
    // iterated, so it must be swept here too.)
    {
        let live: std::collections::HashSet<&str> =
            projects.iter().map(|p| p.project_id.as_str()).collect();
        let mut registry = state.health.lock().await;
        registry.state.retain(|(proj, _, _), _| live.contains(proj.as_str()));
        registry.in_flight.retain(|proj| live.contains(proj.as_str()));
        registry.fired.retain(|proj, _| live.contains(proj.as_str()));
        registry.backoff.retain(|(proj, _), _| live.contains(proj.as_str()));
    }
    Ok(())
}

async fn tick_project(
    state: &SupervisorState,
    project: &weft_broker_client::protocol::SupervisorProject,
) -> Result<()> {
    // Stand down while a user infra action is running: any uncompleted
    // infra_lifecycle_command (apply / stop / terminate) for this
    // project means the lifecycle handler owns its nodes' status
    // right now. Health observes (replicas dropping to 0 during a stop,
    // etc.) but must NOT write, or its autonomous reconcile would race
    // and clobber the user action's transition. Re-arms automatically
    // on the next tick once the command completes (status has settled).
    if state.broker.infra_command_in_flight(&project.project_id).await? {
        // Clear in-memory health state so the post-action tick starts
        // clean (no stale flaky-window arithmetic from before the
        // action), matching the per-node skip below.
        state.health.lock().await.state.retain(|(p, _, _), _| p != &project.project_id);
        return Ok(());
    }

    let workloads = state
        .kube
        .list_replica_state(&project.project_namespace, crate::lifecycle::INFRA_SELECTOR)
        .await?;
    let nodes = state.broker.infra_nodes(&project.project_id).await?;

    // Group workloads by `(weft.dev/node, weft.dev/unit)`. Health is
    // PER-UNIT: one infra node deploys N units (workloads), each with
    // independent health, so a flaky sidecar doesn't drag a healthy
    // primary into "node flaky" (and can be remediated on its own).
    let mut by_unit: HashMap<(String, String), (i64, i64)> = HashMap::new();
    for w in &workloads {
        let (Some(node_id), Some(unit)) =
            (w.labels.get("weft.dev/node"), w.labels.get("weft.dev/unit"))
        else {
            continue;
        };
        let entry = by_unit
            .entry((node_id.clone(), unit.clone()))
            .or_insert((0, 0));
        entry.0 += w.desired;
        entry.1 += w.ready;
    }

    // Per-(node, unit) ready ratio + ready replicas. Used by
    // HealthCondition evaluation below. ONLY populated for nodes the
    // user expects to be up right now ({running, flaky}). A
    // provisioning / stopped / stopping / terminating / failed node
    // contributes NOTHING (the AutoRecover default protocol would
    // otherwise fire on every mid-first-provision project). The unit
    // roster comes from the node's persisted `units` map (a 0-replica
    // or Service-only unit shows no workload but is still expected).
    let mut ready_ratio: HashMap<(String, String), f32> = HashMap::new();
    let mut ready_replicas: HashMap<(String, String), u32> = HashMap::new();
    for n in &nodes {
        if !n.status.expects_running_replicas() {
            continue;
        }
        for unit in n.units.keys() {
            let key = (n.node_id.clone(), unit.clone());
            let (desired, ready) = by_unit.get(&key).copied().unwrap_or((0, 0));
            let ratio = if desired > 0 {
                (ready as f32 / desired as f32).clamp(0.0, 1.0)
            } else {
                // desired=0: scaling toward 0, apply hasn't produced
                // replicas yet. Treat as ready (no "almost zero is
                // broken" math).
                1.0
            };
            ready_ratio.insert(key.clone(), ratio);
            ready_replicas.insert(key, ready.max(0) as u32);
        }
    }

    // Windowed flaky/recovered transitions per node. These drive
    // the dispatcher-visible status badge regardless of whether any
    // HealthProtocol fires.
    //
    // ONLY evaluate health for nodes whose status implies "should be
    // running right now". Skip the rest:
    //   - `stopped` / `stopping`: user intentionally scaled to 0;
    //     0/0 ready is the desired state, not flaky.
    //   - `terminating`: resources being deleted; transient.
    //   - `provisioning`: apply is in progress; the apply executor
    //     is the source of truth until it writes Running.
    //   - `failed`: apply errored; the failure stage carries the
    //     diagnosis, health-flaky would just clobber it.
    // Also clear the in-memory health state for skipped nodes so a
    // post-restart cycle starts clean (no stale last_ready_at /
    // last_not_ready_at from before the Stop biasing the next
    // flaky-window arithmetic).
    //
    // The actual state-machine math lives in `health_engine`; this
    // loop is just the I/O harness around it.
    // Per-tick decisions. Each carries:
    //   - the node_id,
    //   - the latched decision (desired_status + edge event),
    //   - the row's observed status (so the I/O layer below can
    //     reconcile any drift in EITHER direction in one place).
    // Per-tick per-unit decisions. Each carries node_id + unit + the
    // latched decision + the unit's observed status (so the I/O layer
    // reconciles drift in either direction in one place).
    let mut decisions: Vec<(
        String,
        String,
        NodeDecision,
        weft_broker_client::protocol::InfraNodeStatus,
    )> = Vec::new();
    {
        let mut registry = state.health.lock().await;
        for n in &nodes {
            for (unit, unit_rt) in &n.units {
                let key = (project.project_id.clone(), n.node_id.clone(), unit.clone());
                // A unit's status implies whether it should be running.
                // Skip non-running ones (clearing window state) just
                // like the node-level skip did, but per-unit.
                if !unit_rt.status.expects_running_replicas() {
                    registry.state.remove(&key);
                    continue;
                }
                let (desired, ready) = by_unit
                    .get(&(n.node_id.clone(), unit.clone()))
                    .copied()
                    .unwrap_or((0, 0));
                let observation = NodeObservation {
                    desired: desired.max(0) as u32,
                    ready: ready.max(0) as u32,
                };
                let prior = registry.state.get(&key).cloned().unwrap_or_default();
                let decision = evaluate_node_health(
                    prior,
                    observation,
                    state.clock.now(),
                    Duration::from_secs(unit_rt.flaky_after_seconds as u64),
                    Duration::from_secs(unit_rt.recovery_after_seconds as u64),
                );
                registry.state.insert(key, decision.next.clone());
                decisions.push((n.node_id.clone(), unit.clone(), decision, unit_rt.status));
            }
        }

        // If every unit is healthy again, re-arm the per-project fired
        // set (protocols can fire again on the next degradation) AND
        // drop the project's backoff entries (a recovered project
        // starts fresh; otherwise stale backoff would delay the first
        // action of the next degradation episode).
        if all_units_healthy(&ready_ratio) {
            registry.fired.remove(&project.project_id);
            registry.backoff.retain(|(proj, _), _| proj != &project.project_id);
        }
    }

    // Dispatch outside the lock so sibling per-project ticks
    // don't block on slow broker calls.
    //
    // Two orthogonal things happen per node:
    //   1. EDGE: if `decision.event` is `Some`, publish an
    //      `infra_event` row. Edges only fire on Flaky / Recovered
    //      transitions; no event means "same state as last tick."
    //   2. STATUS: if the row's observed status drifted from the
    //      latch's desired status, write set_status. This handles
    //      BOTH directions of drift:
    //        - sibling `set_applied` flipped Flaky→Running while
    //          we still latch Flaky → re-write Flaky;
    //        - external write to Flaky while we latch Running →
    //          re-write Running.
    //      The latch is the single source of truth for the row's
    //      status; the broker write is a reconcile, not a
    //      consequence of the edge.
    for (node_id, unit, decision, observed_status) in decisions {
        if let Some(edge) = decision.event {
            let infra_event = match edge {
                NodeEdgeEvent::BecameFlaky { desired, ready } => {
                    weft_broker_client::protocol::InfraEvent::Flaky(
                        weft_broker_client::protocol::FlakyPayload {
                            desired: desired as i64,
                            ready: ready as i64,
                            reason: Some(format!("unit '{unit}'")),
                        },
                    )
                }
                NodeEdgeEvent::Recovered => {
                    weft_broker_client::protocol::InfraEvent::Recovered
                }
            };
            state
                .broker
                .event_record(&project.project_id, Some(&node_id), infra_event)
                .await?;
        }
        if observed_status != decision.desired_status {
            // Autonomous per-unit reconcile: no lifecycle command in
            // flight. `command_id = None` skips the broker's
            // command-ownership check; tenant scope still applies. The
            // broker sets this unit's status then recomputes the node
            // rollup. A Raced means the row was removed (or a command
            // appeared, fenced); drop the write silently.
            let outcome = state
                .broker
                .set_status(
                    &state.pod_name,
                    None,
                    &project.project_id,
                    &node_id,
                    Some(&unit),
                    decision.desired_status,
                    None,
                    None,
                )
                .await?;
            if outcome.is_raced() {
                tracing::debug!(
                    project_id = %project.project_id,
                    node_id = %node_id,
                    unit = %unit,
                    "health-reconcile set_status raced (row removed); skipping"
                );
            }
        }
    }

    // HealthProtocol evaluation. The pure decision (which protocol
    // fires, if any) lives in `health_engine::evaluate_protocols`.
    // This block just gathers the inputs, calls the pure fn, then
    // dispatches the matched action.
    let protocols_value = state.broker.health_protocols(&project.project_id).await?;
    let protocols: protocol::HealthProtocols = match protocols_value {
        Some(v) => match serde_json::from_value(v.clone()) {
            Ok(p) => p,
            Err(e) => {
                // The user's protocol config is broken. Emit a
                // `protocol_config_error` event so the action bar
                // shows it, then SKIP this tick's evaluation. The
                // previous shape fell back to default_protocols,
                // which silently overrode user intent (an auto-
                // recover protocol could fire on a project the
                // user explicitly configured for park-only). Skip
                // is safer: the next tick re-reads the column and
                // recovers automatically once the user fixes it.
                // serde error strings can balloon on deeply nested
                // protocol shapes (each tried untagged-enum branch
                // shows up in the message). Bound it before shipping
                // so a verbose error doesn't blow the 7800-byte
                // Postgres NOTIFY cap and cause sibling-pod dropout.
                state
                    .broker
                    .event_record(
                        &project.project_id,
                        None,
                        weft_broker_client::protocol::InfraEvent::ProtocolConfigError(
                            weft_broker_client::protocol::ProtocolConfigErrorPayload {
                                error: truncate_user_string(&e.to_string(), 4096),
                            },
                        ),
                    )
                    .await?;
                tracing::warn!(
                    project_id = %project.project_id,
                    error = %e,
                    "health_protocols_json malformed; skipping protocol eval until fixed"
                );
                return Ok(());
            }
        },
        None => protocol::default_protocols(),
    };
    let inputs = ProtocolEvalInputs {
        ready_ratio,
        ready_replicas,
        project_status: project.status,
        deactivated_by_health: project.deactivated_by_health,
    };

    // Snapshot the per-project in_flight + fired sets under the
    // lock, evaluate purely, then re-take the lock to mutate. This
    // is safe because tick_project is the only writer for its own
    // project_id; concurrent ticks are scoped to other projects.
    let (in_flight_snap, fired_snap) = {
        let registry = state.health.lock().await;
        (
            registry.in_flight.contains(&project.project_id),
            registry
                .fired
                .get(&project.project_id)
                .cloned()
                .unwrap_or_default(),
        )
    };

    let Some(matched) = evaluate_protocols(&protocols, &fired_snap, in_flight_snap, &inputs) else {
        return Ok(());
    };
    let matched_name = matched.protocol.name.clone();
    let matched_proto = matched.protocol.clone();

    // Backoff gate + claim, in ONE critical section so the check and
    // the in_flight/fired insert can't interleave with another tick
    // (the gate-then-insert window must be atomic). If this protocol's
    // action recently failed, skip until its backoff window elapses:
    // the protocol stays un-latched from `fired` (so it WILL retry),
    // just not on every poll tick.
    let backoff_key = (project.project_id.clone(), matched_name.clone());
    {
        let mut registry = state.health.lock().await;
        if let Some(b) = registry.backoff.get(&backoff_key) {
            if state.clock.now() < b.next_retry_at {
                return Ok(());
            }
        }
        registry.in_flight.insert(project.project_id.clone());
        registry
            .fired
            .entry(project.project_id.clone())
            .or_default()
            .insert(matched_name.clone());
    }
    tracing::info!(
        project_id = %project.project_id,
        protocol = %matched_name,
        "HealthProtocol matched; firing action"
    );
    // Bound the action by the protocol's timeout (always set:
    // `timeout_seconds` is non-optional with a safe default, so
    // "unbounded" can't be expressed). A hung broker/kube call would
    // otherwise pin `in_flight` forever (the remove below only runs
    // after the await), wedging this project's health monitoring.
    // The timeout maps a wedged action to a loud failure so the slot
    // frees and the protocol un-latches (below).
    let secs = matched_proto.timeout_seconds;
    let dur = std::time::Duration::from_secs(secs as u64);
    let result =
        match tokio::time::timeout(dur, run_action(state, project, &matched_proto)).await {
            Ok(r) => r,
            Err(_elapsed) => Err(anyhow::anyhow!(
                "HealthProtocol action timed out after {secs}s (broker/kube call hung)"
            )),
        };
    let now = state.clock.now();
    {
        let mut registry = state.health.lock().await;
        registry.in_flight.remove(&project.project_id);
        if result.is_err() {
            // On failure (incl. timeout), un-latch the protocol from
            // `fired` so it retries. `fired` suppresses re-firing a
            // SUCCESSFUL action until the project re-arms (goes fully
            // healthy); a failed action never took effect, so leaving
            // it latched would wedge the project (e.g. an AutoRecover
            // that fails would never retry, and "all nodes healthy"
            // can never become true because recovery is what makes it
            // true). The in_flight guard already prevents storms
            // DURING the run, so only success latches.
            if let Some(names) = registry.fired.get_mut(&project.project_id) {
                names.remove(&matched_name);
                if names.is_empty() {
                    registry.fired.remove(&project.project_id);
                }
            }
            // Bump exponential backoff so the un-latched protocol
            // doesn't re-fire every poll tick (~5s) while infra stays
            // broken. Grows the retry delay 5s→10s→...→cap 300s.
            let entry = registry.backoff.entry(backoff_key.clone()).or_insert(BackoffState {
                consecutive_failures: 0,
                next_retry_at: now,
            });
            entry.consecutive_failures = entry.consecutive_failures.saturating_add(1);
            entry.next_retry_at = now + backoff_delay(entry.consecutive_failures);
        } else {
            // Success: clear any backoff so the next degradation
            // fires immediately.
            registry.backoff.remove(&backoff_key);
        }
    }
    if let Err(e) = result {
        tracing::warn!(
            project_id = %project.project_id,
            protocol = %matched_name,
            error = %e,
            "HealthProtocol action failed; un-latched from fired set to retry next tick"
        );
    }
    Ok(())
}

/// What `plan_action` decided. The I/O wrapper turns each variant
/// into the corresponding broker/kube call. The split lets us unit-
/// test the decision (which verb, which payload, which label
/// selector) without needing fake broker + kube clients.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ActionPlan {
    /// Emit a `notify` infra_event for UI / observability (the
    /// target of `ProtocolAction::Notify`). NOT used for trigger-state
    /// changes; those go through `EnqueueLifecycle`. Purely a
    /// "flag a channel"-style notification, no lifecycle effect.
    Notify {
        payload: weft_broker_client::protocol::NotifyPayload,
    },
    /// Enqueue a dispatcher-targeted lifecycle command. The
    /// dispatcher's claim loop picks it up and runs the
    /// deactivate/activate flow (the supervisor has no signal-table
    /// access of its own). Verb-and-payload coherence is checked
    /// at compile time by the typed `LifecycleSpec`.
    EnqueueLifecycle {
        spec: weft_broker_client::protocol::LifecycleSpec,
    },
    /// Scale a specific (instance, unit) workload to `replicas`.
    /// `instance_id` is resolved from the broker's `infra_nodes`
    /// list ahead of dispatch.
    Scale {
        instance_id: String,
        unit: String,
        replicas: u32,
    },
    /// Delete every Pod matching this (instance, unit). Kubernetes
    /// restarts them via the Deployment/StatefulSet controller. The
    /// Deployment / Service / ConfigMap / PVCs are NOT touched :
    /// this is the "kick the process" hammer, not a teardown.
    BouncePods { instance_id: String, unit: String },
    /// The action references a node id that doesn't exist in the
    /// project's `infra_nodes`. Logged via tracing; otherwise no-op.
    NodeMissing { node_id: String },
}

/// Pure: given the matched protocol + the broker's current
/// `infra_nodes` snapshot, decide what side effect to perform. No
/// I/O.
pub(crate) fn plan_action(
    proto: &HealthProtocol,
    infra_nodes: &[weft_broker_client::protocol::SupervisorInfraNode],
) -> ActionPlan {
    match &proto.action {
        ProtocolAction::Notify { channel } => ActionPlan::Notify {
            payload: weft_broker_client::protocol::NotifyPayload {
                protocol: proto.name.clone(),
                channel: channel.clone(),
            },
        },
        // Trigger-state actions: the supervisor enqueues a
        // dispatcher-targeted lifecycle command. The dispatcher's
        // claim loop runs `deactivate_project_with_mode` /
        // `activate_inner`. Side-channel `event_record(notify,
        // action=...)` is gone; lifecycle commands are the channel.
        ProtocolAction::ParkTriggers => ActionPlan::EnqueueLifecycle {
            spec: weft_broker_client::protocol::LifecycleSpec::Deactivate(
                weft_broker_client::protocol::DeactivateSpec {
                    mode: weft_broker_client::protocol::DeactivationMode::Park,
                    grace_minutes: 15,
                    running_policy: weft_broker_client::protocol::RunningPolicy::Wait,
                    // Autonomous park: the server default cap.
                    drain_timeout_secs: None,
                },
            ),
        },
        ProtocolAction::HibernateTriggers { grace_minutes } => ActionPlan::EnqueueLifecycle {
            spec: weft_broker_client::protocol::LifecycleSpec::Deactivate(
                weft_broker_client::protocol::DeactivateSpec {
                    mode: weft_broker_client::protocol::DeactivationMode::Hibernate,
                    grace_minutes: *grace_minutes,
                    running_policy: weft_broker_client::protocol::RunningPolicy::Wait,
                    drain_timeout_secs: None,
                },
            ),
        },
        ProtocolAction::WipeTriggers => ActionPlan::EnqueueLifecycle {
            spec: weft_broker_client::protocol::LifecycleSpec::Deactivate(
                weft_broker_client::protocol::DeactivateSpec {
                    mode: weft_broker_client::protocol::DeactivationMode::Wipe,
                    grace_minutes: 0,
                    running_policy: weft_broker_client::protocol::RunningPolicy::Cancel,
                    drain_timeout_secs: None,
                },
            ),
        },
        ProtocolAction::AutoRecover => ActionPlan::EnqueueLifecycle {
            spec: weft_broker_client::protocol::LifecycleSpec::Reactivate,
        },
        ProtocolAction::Scale {
            node_id,
            unit,
            replicas,
        } => match infra_nodes.iter().find(|n| &n.node_id == node_id) {
            Some(n) => ActionPlan::Scale {
                instance_id: n.instance_id.clone(),
                unit: unit.clone(),
                replicas: *replicas,
            },
            None => ActionPlan::NodeMissing {
                node_id: node_id.clone(),
            },
        },
        ProtocolAction::BouncePods { node_id, unit } => {
            match infra_nodes.iter().find(|n| &n.node_id == node_id) {
                Some(n) => ActionPlan::BouncePods {
                    instance_id: n.instance_id.clone(),
                    unit: unit.clone(),
                },
                None => ActionPlan::NodeMissing {
                    node_id: node_id.clone(),
                },
            }
        }
    }
}

async fn run_action(
    state: &SupervisorState,
    project: &weft_broker_client::protocol::SupervisorProject,
    proto: &HealthProtocol,
) -> Result<()> {
    // For Scale / BouncePods we need the current infra_nodes list to
    // resolve node_id → instance_id. EnqueueLifecycle / Notify don't
    // need it; pay the broker round-trip up front to keep the
    // planner pure regardless.
    let nodes = state.broker.infra_nodes(&project.project_id).await?;
    let plan = plan_action(proto, &nodes);
    match plan {
        ActionPlan::Notify { payload } => {
            state
                .broker
                .event_record(
                    &project.project_id,
                    None,
                    weft_broker_client::protocol::InfraEvent::Notify(payload),
                )
                .await?;
        }
        ActionPlan::EnqueueLifecycle { spec } => {
            state.broker.enqueue_lifecycle(&project.project_id, spec).await?;
        }
        ActionPlan::Scale {
            instance_id,
            unit,
            replicas,
        } => {
            // Filter at the apiserver: instance + unit. No in-Rust
            // filter pass.
            let selector = format!(
                "{},weft.dev/instance={instance_id},weft.dev/unit={unit}",
                crate::lifecycle::INFRA_SELECTOR
            );
            let workloads = state
                .kube
                .list_replica_state(&project.project_namespace, &selector)
                .await?;
            for w in workloads.iter() {
                state
                    .kube
                    .scale_workload(&project.project_namespace, w.kind, &w.name, replicas)
                    .await?;
            }
        }
        ActionPlan::BouncePods { instance_id, unit } => {
            // Pods-only delete: the Deployment / Service /
            // ConfigMap / PVC / Secret all survive. The Deployment
            // controller respawns Pods with the same spec.
            let selector =
                format!("weft.dev/instance={instance_id},weft.dev/unit={unit}");
            state
                .kube
                .delete_pods(&project.project_namespace, &selector)
                .await?;
        }
        ActionPlan::NodeMissing { node_id } => {
            tracing::warn!(
                project_id = %project.project_id,
                protocol = %proto.name,
                missing_node = %node_id,
                "HealthProtocol action references node that's not in infra_nodes; skipping",
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{HealthCondition, HealthProtocol, ProtocolAction};
    use weft_broker_client::protocol::SupervisorInfraNode;

    fn proto(action: ProtocolAction) -> HealthProtocol {
        HealthProtocol {
            name: "p".to_string(),
            when: HealthCondition::NodeReadyRatioBelow {
                node_id: "*".into(),
                unit: "*".into(),
                ratio: 1.0,
            },
            action,
            timeout_seconds: 1800,
        }
    }

    fn node(node_id: &str, instance_id: &str) -> SupervisorInfraNode {
        SupervisorInfraNode {
            node_id: node_id.to_string(),
            instance_id: instance_id.to_string(),
            status: weft_broker_client::protocol::InfraNodeStatus::Running,
            applied_spec_hash: None,
            endpoints: Default::default(),
            preserve_pvcs: Vec::new(),
            units: Default::default(),
        }
    }

    #[test]
    fn plan_notify_returns_notify_with_channel() {
        let p = proto(ProtocolAction::Notify {
            channel: "ops".into(),
        });
        match plan_action(&p, &[]) {
            ActionPlan::Notify { payload } => {
                assert_eq!(payload.channel, "ops");
                assert_eq!(payload.protocol, "p");
            }
            other => panic!("wrong plan: {other:?}"),
        }
    }

    #[test]
    fn plan_park_triggers_enqueues_deactivate_park() {
        use weft_broker_client::protocol::{
            DeactivationMode, LifecycleSpec, RunningPolicy,
        };
        let p = proto(ProtocolAction::ParkTriggers);
        match plan_action(&p, &[]) {
            ActionPlan::EnqueueLifecycle {
                spec: LifecycleSpec::Deactivate(d),
            } => {
                assert_eq!(d.mode, DeactivationMode::Park);
                assert_eq!(d.running_policy, RunningPolicy::Wait);
            }
            other => panic!("wrong plan: {other:?}"),
        }
    }

    #[test]
    fn plan_hibernate_triggers_carries_grace() {
        use weft_broker_client::protocol::{
            DeactivationMode, LifecycleSpec, RunningPolicy,
        };
        let p = proto(ProtocolAction::HibernateTriggers { grace_minutes: 42 });
        match plan_action(&p, &[]) {
            ActionPlan::EnqueueLifecycle {
                spec: LifecycleSpec::Deactivate(d),
            } => {
                assert_eq!(d.mode, DeactivationMode::Hibernate);
                assert_eq!(d.grace_minutes, 42);
                assert_eq!(d.running_policy, RunningPolicy::Wait);
            }
            other => panic!("wrong plan: {other:?}"),
        }
    }

    #[test]
    fn plan_wipe_triggers_enqueues_deactivate_wipe_cancel() {
        use weft_broker_client::protocol::{
            DeactivationMode, LifecycleSpec, RunningPolicy,
        };
        let p = proto(ProtocolAction::WipeTriggers);
        match plan_action(&p, &[]) {
            ActionPlan::EnqueueLifecycle {
                spec: LifecycleSpec::Deactivate(d),
            } => {
                assert_eq!(d.mode, DeactivationMode::Wipe);
                assert_eq!(d.running_policy, RunningPolicy::Cancel);
            }
            other => panic!("wrong plan: {other:?}"),
        }
    }

    #[test]
    fn plan_auto_recover_enqueues_reactivate() {
        use weft_broker_client::protocol::LifecycleSpec;
        let p = proto(ProtocolAction::AutoRecover);
        match plan_action(&p, &[]) {
            ActionPlan::EnqueueLifecycle {
                spec: LifecycleSpec::Reactivate,
            } => {}
            other => panic!("wrong plan: {other:?}"),
        }
    }

    #[test]
    fn plan_scale_resolves_instance_id() {
        let p = proto(ProtocolAction::Scale {
            node_id: "n1".into(),
            unit: "main".into(),
            replicas: 3,
        });
        let nodes = vec![node("n1", "inst-abc")];
        let result = plan_action(&p, &nodes);
        assert_eq!(
            result,
            ActionPlan::Scale {
                instance_id: "inst-abc".into(),
                unit: "main".into(),
                replicas: 3,
            }
        );
    }

    #[test]
    fn plan_scale_node_missing_when_no_match() {
        let p = proto(ProtocolAction::Scale {
            node_id: "ghost".into(),
            unit: "main".into(),
            replicas: 3,
        });
        let nodes = vec![node("n1", "inst-abc")];
        let result = plan_action(&p, &nodes);
        assert_eq!(
            result,
            ActionPlan::NodeMissing {
                node_id: "ghost".into()
            }
        );
    }

    #[test]
    fn plan_bounce_pods_resolves_instance_id() {
        let p = proto(ProtocolAction::BouncePods {
            node_id: "n1".into(),
            unit: "main".into(),
        });
        let nodes = vec![node("n1", "inst-abc")];
        let result = plan_action(&p, &nodes);
        assert_eq!(
            result,
            ActionPlan::BouncePods {
                instance_id: "inst-abc".into(),
                unit: "main".into()
            }
        );
    }

    #[test]
    fn plan_bounce_pods_node_missing_when_no_match() {
        let p = proto(ProtocolAction::BouncePods {
            node_id: "ghost".into(),
            unit: "main".into(),
        });
        let result = plan_action(&p, &[]);
        assert_eq!(
            result,
            ActionPlan::NodeMissing {
                node_id: "ghost".into()
            }
        );
    }

    #[test]
    fn default_protocols_two_stage_park_then_recover() {
        let p = protocol::default_protocols();
        assert_eq!(p.protocols.len(), 2);
        assert!(matches!(p.protocols[0].action, ProtocolAction::ParkTriggers));
        assert!(matches!(p.protocols[1].action, ProtocolAction::AutoRecover));
    }
}
