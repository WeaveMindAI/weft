//! Health loop. Asks the host how every unit of the owned projects is
//! doing, evaluates each project's HealthProtocols, and emits
//! `infra_event` rows via the broker when a node's status changes.
//!
//! Two concerns:
//!   1. Per-node windowed health-state transitions (flaky after N
//!      seconds below threshold, recovered after N seconds above).
//!      Drives `infra_event` flaky / recovered + `infra_node.status`.
//!   2. HealthProtocols evaluation: ordered (condition → action)
//!      rules. First match fires; while a project's protocol is in
//!      flight, subsequent matches queue (next look re-checks once
//!      the current one settles).
//!
//! When it looks: every `health_interval`, every owned project (the
//! flaky and recovery windows are about TIME passing with nothing
//! changing, so a look on a clock is what they need). What this
//! supervisor owns is followed as the ownership loop changes it: a lost
//! project's health state is dropped the moment the loss is known (a look
//! already running finishes; its status write is fenced by ownership),
//! and a claim runs a tick right away.

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use anyhow::Result;
use weft_core::truncate_user_string;

use crate::health_engine::{
    all_units_healthy, evaluate_protocols, observe_unit, rearm, record_fire, NodeDecision, NodeEdgeEvent, NodeHealthState,
    NodeObservation, ProtocolEvalInputs,
};
use crate::protocol::{self, HealthProtocol, ProtocolAction, UnitView};
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
    /// Per-(project, copy_id, unit) state tracking. Records when the
    /// unit was last seen Ready vs Not-Ready so we can apply windowed
    /// transitions (flaky_after, recovery_after). Health is per-unit of
    /// one deployed copy: a node that exists once per instance
    /// has one copy on the host per instance, each with its own
    /// health.
    state: HashMap<(uuid::Uuid, String, String), NodeHealthState>,
    /// Per-project "currently in flight" protocol names. While a
    /// protocol is in flight, the supervisor doesn't re-fire any
    /// protocol for that project (avoids action storms when health
    /// flaps mid-action).
    in_flight: HashSet<uuid::Uuid>,
    /// Per-project protocols already fired, each with the copies its
    /// action aimed at (empty for a notification, which aims at no
    /// copy). A protocol fires again only for a copy it has not acted
    /// on. Re-armed copy by copy (`health_engine::rearm`).
    fired: HashMap<uuid::Uuid, HashMap<String, std::collections::BTreeSet<weft_broker_client::protocol::InfraCopy>>>,
    /// Per-(project, protocol) exponential backoff after a failed
    /// action. A matched protocol whose entry's `next_retry_at` is in
    /// the future is skipped this tick. Cleared on success.
    backoff: HashMap<(uuid::Uuid, String), BackoffState>,
}

impl HealthRegistry {
    /// Drop every entry of a project `keep` refuses (deleted, or no
    /// longer this supervisor's): a project this loop stops looking at would
    /// otherwise keep its entries forever, and a reclaimed one would
    /// start from stale windows.
    fn keep_only(&mut self, keep: impl Fn(uuid::Uuid) -> bool) {
        self.state.retain(|(project, _, _), _| keep(*project));
        self.in_flight.retain(|project| keep(*project));
        self.fired.retain(|project, _| keep(*project));
        self.backoff.retain(|(project, _), _| keep(*project));
    }

    /// True if a protocol action is currently in flight for the
    /// project. Exposed for introspection (e.g. asserting a hung
    /// action's timeout freed the slot).
    pub fn is_in_flight(&self, project_id: uuid::Uuid) -> bool {
        self.in_flight.contains(&project_id)
    }

    /// True if `protocol_name` is latched in the project's fired set
    /// (a successful action suppresses re-fire until re-arm). Exposed
    /// for introspection (e.g. asserting a failed action un-latched).
    pub fn is_fired(&self, project_id: uuid::Uuid, protocol_name: &str) -> bool {
        self.fired
            .get(&project_id)
            .is_some_and(|fired| fired.contains_key(protocol_name))
    }

    /// Consecutive-failure count for a (project, protocol)'s backoff,
    /// 0 if none. Exposed for introspection (e.g. asserting a failed
    /// action armed backoff and a success cleared it).
    pub fn backoff_failures(&self, project_id: uuid::Uuid, protocol_name: &str) -> u32 {
        self.backoff
            .get(&(project_id, protocol_name.to_string()))
            .map(|b| b.consecutive_failures)
            .unwrap_or(0)
    }
}

pub async fn run_loop(
    state: SupervisorState,
    mut changes: tokio::sync::mpsc::UnboundedReceiver<crate::ownership::OwnershipChange>,
) -> Result<()> {
    loop {
        if let Err(e) = tick(&state).await {
            tracing::warn!(error = %e, "health tick failed");
        }
        let next_tick = state.clock.sleep(state.health_interval);
        tokio::pin!(next_tick);
        loop {
            tokio::select! {
                biased;
                change = changes.recv() => {
                    let change = change.ok_or_else(|| {
                        anyhow::anyhow!("the ownership loop is gone; the health loop cannot follow what this supervisor owns")
                    })?;
                    if let Err(e) = on_ownership_change(&state, &change).await {
                        tracing::warn!(error = %e, "health tick after a claim failed");
                    }
                }
                _ = &mut next_tick => break,
            }
        }
    }
}

/// Follow one ownership change: a lost project's health state is dropped
/// at once; a claim runs a tick now rather than at the next interval.
/// Exposed so integration tests can step it.
pub async fn on_ownership_change(state: &SupervisorState, change: &crate::ownership::OwnershipChange) -> Result<()> {
    if !change.lost.is_empty() {
        state.health.lock().await.keep_only(|project| !change.lost.contains(&project));
    }
    if !change.claimed.is_empty() {
        tick(state).await?;
    }
    Ok(())
}

/// One tick of the health loop: evaluate every owned project. Exposed
/// (rather than only running inside `run_loop`) so integration tests can
/// step the loop one tick at a time.
pub async fn tick(state: &SupervisorState) -> Result<()> {
    let projects = state.broker.owned_projects(&state.replica).await?;
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
    {
        let live: std::collections::HashSet<uuid::Uuid> =
            projects.iter().map(|p| p.project_id).collect();
        state.health.lock().await.keep_only(|project| live.contains(&project));
    }
    Ok(())
}

async fn tick_project(
    state: &SupervisorState,
    project: &weft_broker_client::protocol::SupervisorProject,
) -> Result<()> {
    // Stand down, copy by copy, while a user infra action runs on it:
    // an uncompleted supervisor command (apply / stop / terminate) that
    // reaches a copy means the lifecycle handler owns that copy's status
    // right now. Health must NOT look at it (units going down
    // during a stop are the action, not a fault), or its autonomous
    // reconcile would race and clobber the action's transition. Every
    // other copy keeps its health. A stood-down copy's latches are
    // cleared, so the look after the command starts from the row it
    // left (`NodeHealthState::seeded_from`).
    let commands = state.broker.infra_commands_in_flight(project.project_id).await?;
    let nodes = state.broker.infra_nodes(project.project_id).await?;

    // How the host sees every unit of the project, by `(copy_id,
    // unit)`. Health is PER-UNIT: one infra node runs N units, each with
    // independent health, so a flaky sidecar doesn't drag a healthy
    // primary into "node flaky" (and can be remediated on its own). The
    // copy, not the node, is the key: a node that exists once
    // per instance runs one copy for each, and one
    // instance's broken copy says nothing about another's.
    let seen: HashMap<(String, String), weft_platform_traits::UnitRunState> = state
        .host
        .observe(&project.tenant_id, project.project_id)
        .await?
        .into_iter()
        .map(|o| ((o.copy_id, o.unit), o.state))
        .collect();

    // Per unit of each copy: its windowed health (flaky/recovered
    // transitions, which drive the dispatcher-visible status badge
    // whether or not any HealthProtocol fires) and the view the
    // protocols read.
    //
    // ONLY units whose status implies "should be running right now"
    // ({running, flaky}). Skip the rest:
    //   - `stopped` / `stopping`: user intentionally scaled to 0;
    //     0/0 ready is the desired state, not flaky.
    //   - `terminating`: resources being deleted; transient.
    //   - `provisioning`: apply is in progress; the apply executor
    //     is the source of truth until it writes Running.
    //   - `failed`: apply errored; the failure stage carries the
    //     diagnosis, health-flaky would just clobber it.
    // Also skipped: a copy a command in flight reaches (above). Clear
    // the in-memory health state for skipped units so a post-restart
    // cycle starts clean (no stale last_ready_at / last_not_ready_at
    // from before the Stop biasing the next flaky-window arithmetic).
    //
    // A unit the host does not report yet is UNKNOWN: no decision, no
    // view, so no protocol reads it as broken (a copy just applied can
    // reach this look before the host reports it). The unit roster
    // comes from the row's `units` map.
    //
    // The state-machine math lives in `health_engine`; this loop is
    // just the I/O harness around it. Each decision carries the unit's
    // observed row status, so the I/O layer below reconciles drift in
    // either direction in one place.
    let mut decisions: Vec<(
        String,
        Option<weft_core::instance::InstanceId>,
        String,
        NodeDecision,
        weft_broker_client::protocol::InfraNodeStatus,
        String,
    )> = Vec::new();
    let mut units: Vec<UnitView> = Vec::new();
    {
        let mut registry = state.health.lock().await;
        for n in &nodes {
            let stood_down = commands.iter().any(|c| c.reaches(&n.node_id, n.instance.as_ref()));
            for (unit, unit_rt) in &n.units {
                let key = (project.project_id, n.copy_id.clone(), unit.clone());
                if stood_down || !unit_rt.status.expects_running_units() {
                    registry.state.remove(&key);
                    continue;
                }
                let reading = seen.get(&(n.copy_id.clone(), unit.clone()));
                let observed = reading.map(|s| NodeObservation { ready: *s == weft_platform_traits::UnitRunState::Ready });
                // No latch yet (first look by this supervisor, or the copy's
                // latches were cleared by a lifecycle command): start
                // from what the row says, see `seeded_from`.
                let now = state.clock.now();
                let flaky_after = Duration::from_secs(unit_rt.flaky_after_seconds as u64);
                let prior = registry.state.get(&key).cloned().unwrap_or_else(|| {
                    NodeHealthState::seeded_from(
                        unit_rt.status,
                        n.applied_at_unix,
                        flaky_after,
                        now,
                        state.clock.now_unix(),
                    )
                });
                let Some(decision) = observe_unit(
                    prior,
                    observed,
                    now,
                    flaky_after,
                    Duration::from_secs(unit_rt.recovery_after_seconds as u64),
                ) else {
                    continue;
                };
                units.push(UnitView {
                    node_id: n.node_id.clone(),
                    instance: n.instance.clone(),
                    unit: unit.clone(),
                    ready: observed.is_some_and(|o| o.ready),
                    flaky: decision.next.declared_flaky,
                });
                registry.state.insert(key, decision.next.clone());
                // Why it is not ready, in the host's words, for the
                // flaky event.
                let why = match reading {
                    Some(weft_platform_traits::UnitRunState::NotReady { why })
                    | Some(weft_platform_traits::UnitRunState::Failed { why }) => why.clone(),
                    Some(weft_platform_traits::UnitRunState::Starting { step }) => format!("still starting ({step})"),
                    Some(weft_platform_traits::UnitRunState::Stopped) => "stopped outside weft".into(),
                    Some(weft_platform_traits::UnitRunState::Ready) => "ready".into(),
                    None => "the host no longer reports it".into(),
                };
                decisions.push((n.node_id.clone(), n.instance.clone(), unit.clone(), decision, unit_rt.status, why));
            }
        }

        // If every unit is healthy again, drop the project's backoff
        // entries (a recovered project starts fresh; otherwise stale
        // backoff would delay the first action of the next degradation
        // episode). What the protocols fired re-arms copy by copy once
        // the protocols are read, below.
        if all_units_healthy(&units) {
            registry.backoff.retain(|(proj, _), _| *proj != project.project_id);
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
    for (node_id, instance, unit, decision, observed_status, why) in decisions {
        if let Some(edge) = decision.event {
            let infra_event = match edge {
                NodeEdgeEvent::BecameFlaky => weft_broker_client::protocol::InfraEvent::Flaky(
                    weft_broker_client::protocol::FlakyPayload { reason: format!("unit '{unit}' is not ready: {why}") },
                ),
                NodeEdgeEvent::Recovered => {
                    weft_broker_client::protocol::InfraEvent::Recovered
                }
            };
            state
                .broker
                .event_record(project.project_id, Some(&node_id), instance.as_ref(), infra_event)
                .await?;
        }
        if observed_status != decision.desired_status {
            // Autonomous per-unit reconcile: no lifecycle command in
            // flight. `command_id = None` skips the broker's
            // command-ownership check; tenant scope still applies. The
            // broker sets this unit's status then recomputes the node
            // rollup. A stale outcome means the row was removed, the
            // unit left the roster (an apply dropped it after this tick
            // read the roster; the broker fences per-unit stamps on
            // membership) or a command appeared (fenced); drop the
            // write silently.
            let outcome = state
                .broker
                .set_status(
                    &state.replica,
                    None,
                    project.project_id,
                    &node_id,
                    instance.as_ref(),
                    Some(&unit),
                    decision.desired_status,
                    None,
                    None,
                )
                .await?;
            if !outcome.is_applied() {
                tracing::debug!(
                    project_id = %project.project_id,
                    node_id = %node_id,
                    unit = %unit,
                    ?outcome,
                    "health-reconcile set_status not applied (row removed, unit left the roster, or a command is in flight); skipping"
                );
            }
        }
    }

    // HealthProtocol evaluation. The pure decision (which protocol
    // fires, if any) lives in `health_engine::evaluate_protocols`.
    // This block just gathers the inputs, calls the pure fn, then
    // dispatches the matched action.
    let protocols_value = state.broker.health_protocols(project.project_id).await?;
    let protocols: protocol::HealthProtocols = match protocols_value {
        Some(v) => match serde_json::from_value::<protocol::HealthProtocols>(v.clone()) {
            Ok(p) => p,
            Err(e) => {
                // The user's protocol config is broken (unreadable). Emit a
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
                // Postgres NOTIFY cap and cause a sibling listener to drop out.
                state
                    .broker
                    .event_record(
                        project.project_id,
                        None,
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
        units,
        project_status: project.status,
        health_parked: project.health_parked,
    };

    // Re-arm what the protocols fired, copy by copy, then snapshot the
    // per-project in_flight + fired sets under the lock, evaluate
    // purely, then re-take the lock to mutate. This is safe because
    // tick_project is the only writer for its own project_id;
    // concurrent ticks are scoped to other projects.
    let (in_flight_snap, fired_snap) = {
        let mut registry = state.health.lock().await;
        if let Some(fired) = registry.fired.get_mut(&project.project_id) {
            rearm(fired, &protocols, &inputs);
            if fired.is_empty() {
                registry.fired.remove(&project.project_id);
            }
        }
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
    let scope = matched.scope;

    // Backoff gate + claim, in ONE critical section so the check and
    // the in_flight/fired insert can't interleave with another tick
    // (the gate-then-insert window must be atomic). If this protocol's
    // action recently failed, skip until its backoff window elapses:
    // the protocol stays un-latched from `fired` (so it WILL retry),
    // just not on every poll tick.
    let backoff_key = (project.project_id, matched_name.clone());
    let acted_before = {
        let mut registry = state.health.lock().await;
        if let Some(b) = registry.backoff.get(&backoff_key) {
            if state.clock.now() < b.next_retry_at {
                return Ok(());
            }
        }
        registry.in_flight.insert(project.project_id);
        let fired = registry.fired.entry(project.project_id).or_default();
        let acted_before = fired.get(&matched_name).cloned();
        record_fire(fired, &matched_proto, &scope);
        acted_before
    };
    tracing::info!(
        project_id = %project.project_id,
        protocol = %matched_name,
        "HealthProtocol matched; firing action"
    );
    // Bound the action by the protocol's timeout (always set:
    // `timeout_seconds` is non-optional with a safe default, so
    // "unbounded" can't be expressed). A hung broker/host call would
    // otherwise pin `in_flight` forever (the remove below only runs
    // after the await), wedging this project's health monitoring.
    // The timeout maps a wedged action to a loud failure so the slot
    // frees and the protocol un-latches (below).
    let secs = matched_proto.timeout_seconds;
    let dur = std::time::Duration::from_secs(secs as u64);
    let result =
        match tokio::time::timeout(dur, run_action(state, project, &matched_proto, &scope)).await {
            Ok(r) => r,
            Err(_elapsed) => Err(anyhow::anyhow!(
                "HealthProtocol action timed out after {secs}s (broker/host call hung)"
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
            // Back to what it had acted on before this attempt.
            if let Some(fired) = registry.fired.get_mut(&project.project_id) {
                match acted_before {
                    Some(acted) => {
                        fired.insert(matched_name.clone(), acted);
                    }
                    None => {
                        fired.remove(&matched_name);
                    }
                }
                if fired.is_empty() {
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
/// into the corresponding broker/host call. The split lets us unit-test
/// the decision (which verb, which payload, which copies) without
/// needing fake broker + host clients.
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
    /// Restart `unit` in the copies of the node owned by whoever owns a
    /// broken copy (the protocol's scope). Each copy's id is
    /// resolved from the broker's `infra_nodes` list ahead of dispatch.
    RestartUnit { copies: Vec<HostCopy>, unit: String },
    /// The action references a node with no copy at all in the
    /// project's `infra_nodes`. Logged via tracing; otherwise no-op.
    NodeMissing { node_id: String },
    /// The node has copies, but none belongs to the owners of the broken
    /// copies (`owners`: `None` for the shared copy): the action names a
    /// node on the other side (a shared node for a broken instance's copy,
    /// or the reverse). Logged via tracing; otherwise no-op.
    NoCopyForOwners {
        node_id: String,
        owners: Vec<Option<weft_core::instance::InstanceId>>,
    },
}

/// One copy of a node and the id the host runs it under.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HostCopy {
    pub copy: weft_broker_client::protocol::InfraCopy,
    pub copy_id: String,
}

/// Pure: given the matched protocol, the broken copies it aims at (its
/// match's scope), and the broker's current `infra_nodes` snapshot,
/// decide what side effect to perform. No I/O.
pub(crate) fn plan_action(
    proto: &HealthProtocol,
    scope: &std::collections::BTreeSet<weft_broker_client::protocol::InfraCopy>,
    infra_nodes: &[weft_broker_client::protocol::SupervisorInfraNode],
) -> ActionPlan {
    use weft_broker_client::protocol::{
        DeactivateSpec, DeactivationMode, LifecycleSpec, RestoreReaders, RunningPolicy, TakeDownReach, TakeDownReaders,
    };
    // A take-down of the triggers reading the broken copies. The
    // dispatcher's claim loop runs it; lifecycle commands are the
    // channel (the supervisor has no signal-table access).
    // A condition naming no infra says nothing about which copies are
    // broken: the take-down reaches the whole project.
    let reach = if crate::protocol::names_infra(&proto.when) {
        TakeDownReach::ReadersOf { broken: scope.iter().cloned().collect() }
    } else {
        TakeDownReach::Project
    };
    let take_down = |spec: DeactivateSpec| ActionPlan::EnqueueLifecycle {
        spec: LifecycleSpec::Deactivate(TakeDownReaders { spec, reach: reach.clone() }),
    };
    match &proto.action {
        ProtocolAction::Notify { channel } => ActionPlan::Notify {
            payload: weft_broker_client::protocol::NotifyPayload {
                protocol: proto.name.clone(),
                channel: channel.clone(),
            },
        },
        ProtocolAction::ParkTriggers => take_down(DeactivateSpec {
            mode: DeactivationMode::Park,
            grace_minutes: 15,
            running_policy: RunningPolicy::Wait,
            // Autonomous park: the server default cap.
            drain_timeout_secs: None,
        }),
        ProtocolAction::HibernateTriggers { grace_minutes } => take_down(DeactivateSpec {
            mode: DeactivationMode::Hibernate,
            grace_minutes: *grace_minutes,
            running_policy: RunningPolicy::Wait,
            drain_timeout_secs: None,
        }),
        ProtocolAction::WipeTriggers => take_down(DeactivateSpec {
            mode: DeactivationMode::Wipe,
            grace_minutes: 0,
            running_policy: RunningPolicy::Cancel,
            drain_timeout_secs: None,
        }),
        ProtocolAction::AutoRecover => ActionPlan::EnqueueLifecycle {
            spec: LifecycleSpec::Reactivate(RestoreReaders { still_broken: scope.iter().cloned().collect() }),
        },
        ProtocolAction::RestartUnit { node_id, unit } => {
            copies_of(infra_nodes, node_id, scope, |copies| ActionPlan::RestartUnit { copies, unit: unit.clone() })
        }
    }
}

/// The plan `act` makes over each copy of `node_id`
/// owned by the owner of one of the broken copies in `scope` (the shared
/// copy for a broken shared one, ada's for a broken copy of ada's); when
/// there is none, why: no copy of the node at all, or none of those
/// owners'.
fn copies_of(
    infra_nodes: &[weft_broker_client::protocol::SupervisorInfraNode],
    node_id: &str,
    scope: &std::collections::BTreeSet<weft_broker_client::protocol::InfraCopy>,
    act: impl FnOnce(Vec<HostCopy>) -> ActionPlan,
) -> ActionPlan {
    let copies: Vec<_> = infra_nodes.iter().filter(|n| n.node_id == node_id).collect();
    if copies.is_empty() {
        return ActionPlan::NodeMissing { node_id: node_id.to_string() };
    }
    let targets: Vec<HostCopy> = copies
        .iter()
        .filter(|n| scope.iter().any(|broken| broken.instance == n.instance))
        .map(|n| HostCopy {
            copy: weft_broker_client::protocol::InfraCopy { node_id: n.node_id.clone(), instance: n.instance.clone() },
            copy_id: n.copy_id.clone(),
        })
        .collect();
    if targets.is_empty() {
        let owners: std::collections::BTreeSet<_> = scope.iter().map(|broken| broken.instance.clone()).collect();
        return ActionPlan::NoCopyForOwners { node_id: node_id.to_string(), owners: owners.into_iter().collect() };
    }
    act(targets)
}

async fn run_action(
    state: &SupervisorState,
    project: &weft_broker_client::protocol::SupervisorProject,
    proto: &HealthProtocol,
    scope: &std::collections::BTreeSet<weft_broker_client::protocol::InfraCopy>,
) -> Result<()> {
    // For RestartUnit we need the current infra_nodes list to resolve
    // node_id → copy_id. EnqueueLifecycle / Notify don't
    // need it; pay the broker round-trip up front to keep the
    // planner pure regardless.
    let nodes = state.broker.infra_nodes(project.project_id).await?;
    let plan = plan_action(proto, scope, &nodes);
    match plan {
        ActionPlan::Notify { payload } => {
            state
                .broker
                .event_record(
                    project.project_id,
                    None,
                    None,
                    weft_broker_client::protocol::InfraEvent::Notify(payload),
                )
                .await?;
        }
        ActionPlan::EnqueueLifecycle { spec } => {
            state.broker.enqueue_lifecycle(project.project_id, spec).await?;
        }
        ActionPlan::RestartUnit { copies, unit } => {
            // Every copy is attempted; any left unrestarted fails the
            // action, which releases the protocol's latch so it retries
            // next tick (a restart is idempotent).
            let mut failed = Vec::new();
            for HostCopy { copy, copy_id } in copies {
                let node = weft_core::infra::NodeRef {
                    tenant: project.tenant_id.clone(),
                    project: project.project_id,
                    node: copy.node_id.clone(),
                    copy_id,
                };
                if let Err(e) = state.host.restart_unit(&node, &unit).await {
                    failed.push(format!("{}: {e:#}", copy.node_id));
                }
            }
            if !failed.is_empty() {
                anyhow::bail!(
                    "protocol '{}' restarted unit '{unit}' in only some copies; not done: {}",
                    proto.name,
                    failed.join("; ")
                );
            }
        }
        ActionPlan::NodeMissing { node_id } => {
            tracing::warn!(
                project_id = %project.project_id,
                protocol = %proto.name,
                missing_node = %node_id,
                "HealthProtocol action references node that's not in infra_nodes; skipping",
            );
        }
        ActionPlan::NoCopyForOwners { node_id, owners } => {
            let owners: Vec<String> =
                owners.iter().map(|o| o.as_ref().map_or_else(|| "shared".to_string(), |m| m.to_string())).collect();
            tracing::warn!(
                project_id = %project.project_id,
                protocol = %proto.name,
                node = %node_id,
                owners = %owners.join(", "),
                "HealthProtocol action: no copy of {node_id} belongs to the broken owners; skipping",
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{HealthCondition, HealthProtocol, ProtocolAction};
    use weft_broker_client::protocol::{InfraCopy, SupervisorInfraNode};

    fn proto(action: ProtocolAction) -> HealthProtocol {
        HealthProtocol {
            name: "p".to_string(),
            when: HealthCondition::NodeNotReady { node_id: "*".into(), unit: "*".into() },
            action,
            timeout_seconds: 1800,
        }
    }

    fn node(node_id: &str, copy_id: &str) -> SupervisorInfraNode {
        SupervisorInfraNode {
            node_id: node_id.to_string(),
            copy_id: copy_id.to_string(),
            status: weft_broker_client::protocol::InfraNodeStatus::Running,
            applied_spec_hash: None,
            applied_at_unix: None,
            addresses: Default::default(),
            keep_disks: Vec::new(),
            units: Default::default(),
            instance: None,
        }
    }

    #[test]
    fn plan_notify_returns_notify_with_channel() {
        let p = proto(ProtocolAction::Notify {
            channel: "ops".into(),
        });
        match plan_action(&p, &no_scope(), &[]) {
            ActionPlan::Notify { payload } => {
                assert_eq!(payload.channel, "ops");
                assert_eq!(payload.protocol, "p");
            }
            other => panic!("wrong plan: {other:?}"),
        }
    }

    fn no_scope() -> std::collections::BTreeSet<InfraCopy> {
        Default::default()
    }

    /// The park carries what is broken, so the dispatcher parks only
    /// the triggers that read it: here ada's copy of `svc`.
    #[test]
    fn plan_park_triggers_enqueues_deactivate_park_aimed_at_the_broken_copies() {
        use weft_broker_client::protocol::{
            DeactivationMode, LifecycleSpec, RunningPolicy,
        };
        let p = proto(ProtocolAction::ParkTriggers);
        let ada_svc = InfraCopy { node_id: "svc".into(), instance: Some(weft_core::instance::InstanceId::new("ada").unwrap()) };
        match plan_action(&p, &std::collections::BTreeSet::from([ada_svc.clone()]), &[]) {
            ActionPlan::EnqueueLifecycle {
                spec: LifecycleSpec::Deactivate(d),
            } => {
                assert_eq!(d.spec.mode, DeactivationMode::Park);
                assert_eq!(d.spec.running_policy, RunningPolicy::Wait);
                assert_eq!(d.reach, weft_broker_client::protocol::TakeDownReach::ReadersOf { broken: vec![ada_svc] });
            }
            other => panic!("wrong plan: {other:?}"),
        }
    }

    /// A take-down whose condition names no infra reaches the whole
    /// project, as it did before copies had owners.
    #[test]
    fn plan_park_on_a_condition_naming_no_infra_reaches_the_project() {
        use weft_broker_client::protocol::{LifecycleSpec, ProjectStatus, TakeDownReach};
        let p = HealthProtocol {
            when: HealthCondition::ProjectStatusEq { status: ProjectStatus::Active },
            ..proto(ProtocolAction::ParkTriggers)
        };
        match plan_action(&p, &no_scope(), &[]) {
            ActionPlan::EnqueueLifecycle { spec: LifecycleSpec::Deactivate(d) } => {
                assert_eq!(d.reach, TakeDownReach::Project)
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
        match plan_action(&p, &no_scope(), &[]) {
            ActionPlan::EnqueueLifecycle {
                spec: LifecycleSpec::Deactivate(d),
            } => {
                assert_eq!(d.spec.mode, DeactivationMode::Hibernate);
                assert_eq!(d.spec.grace_minutes, 42);
                assert_eq!(d.spec.running_policy, RunningPolicy::Wait);
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
        match plan_action(&p, &no_scope(), &[]) {
            ActionPlan::EnqueueLifecycle {
                spec: LifecycleSpec::Deactivate(d),
            } => {
                assert_eq!(d.spec.mode, DeactivationMode::Wipe);
                assert_eq!(d.spec.running_policy, RunningPolicy::Cancel);
            }
            other => panic!("wrong plan: {other:?}"),
        }
    }

    #[test]
    fn plan_auto_recover_enqueues_reactivate() {
        use weft_broker_client::protocol::LifecycleSpec;
        let p = proto(ProtocolAction::AutoRecover);
        let shared = InfraCopy { node_id: "svc".into(), instance: None };
        match plan_action(&p, &std::collections::BTreeSet::from([shared.clone()]), &[]) {
            ActionPlan::EnqueueLifecycle {
                spec: LifecycleSpec::Reactivate(restore),
            } => assert_eq!(restore.still_broken, vec![shared]),
            other => panic!("wrong plan: {other:?}"),
        }
    }

    fn shared_copy(node_id: &str, copy_id: &str) -> HostCopy {
        HostCopy { copy: InfraCopy { node_id: node_id.into(), instance: None }, copy_id: copy_id.into() }
    }

    fn shared_broken(node_id: &str) -> std::collections::BTreeSet<InfraCopy> {
        std::collections::BTreeSet::from([InfraCopy { node_id: node_id.into(), instance: None }])
    }

    /// A restart reaches the copies owned by whoever owns a broken copy:
    /// ada's broken `n1` restarts ada's `n1` only, never the shared one
    /// or bob's; a broken shared `db` restarts the shared `n1`.
    #[test]
    fn plan_restart_reaches_only_the_broken_owners_copy() {
        let p = proto(ProtocolAction::RestartUnit { node_id: "n1".into(), unit: "main".into() });
        let ada_id = weft_core::instance::InstanceId::new("ada").unwrap();
        let mut ada = node("n1", "inst-ada");
        ada.instance = Some(ada_id.clone());
        let mut bob = node("n1", "inst-bob");
        bob.instance = Some(weft_core::instance::InstanceId::new("bob").unwrap());
        let nodes = vec![node("n1", "inst-shared"), ada, bob, node("n2", "inst-other")];
        let ada_broken = std::collections::BTreeSet::from([InfraCopy { node_id: "n1".into(), instance: Some(ada_id.clone()) }]);
        assert_eq!(
            plan_action(&p, &ada_broken, &nodes),
            ActionPlan::RestartUnit {
                copies: vec![HostCopy {
                    copy: InfraCopy { node_id: "n1".into(), instance: Some(ada_id) },
                    copy_id: "inst-ada".into(),
                }],
                unit: "main".into(),
            }
        );
        assert_eq!(
            plan_action(&p, &shared_broken("db"), &nodes),
            ActionPlan::RestartUnit { copies: vec![shared_copy("n1", "inst-shared")], unit: "main".into() }
        );
    }

    #[test]
    fn plan_names_the_owners_when_the_node_is_on_the_other_side() {
        let p = proto(ProtocolAction::RestartUnit { node_id: "n1".into(), unit: "main".into() });
        let ada = weft_core::instance::InstanceId::new("ada").unwrap();
        let broken = std::collections::BTreeSet::from([InfraCopy { node_id: "db".into(), instance: Some(ada.clone()) }]);
        assert_eq!(
            plan_action(&p, &broken, &[node("n1", "inst-shared")]),
            ActionPlan::NoCopyForOwners { node_id: "n1".into(), owners: vec![Some(ada)] }
        );
    }

    #[test]
    fn plan_restart_node_missing_when_no_match() {
        let p = proto(ProtocolAction::RestartUnit { node_id: "ghost".into(), unit: "main".into() });
        assert_eq!(
            plan_action(&p, &shared_broken("ghost"), &[node("n1", "inst-abc")]),
            ActionPlan::NodeMissing { node_id: "ghost".into() }
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
