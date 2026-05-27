//! Pure state machine driving the supervisor's flaky/recovered
//! detection and HealthProtocol matching.
//!
//! Split off from `health.rs` so the windowed transitions can be
//! tested deterministically without spinning a real supervisor +
//! broker + kube + tokio runtime. The shape:
//!
//!   inputs           pure fn                outputs
//!   ─────────  ───────────────────────  ───────────────
//!   prior NodeHealthState  +
//!   observed replicas      ──> evaluate_node_health  ──> NodeDecision
//!   "now" instant
//!
//!   prior in_flight set    +
//!   prior fired set        +
//!   ratios + replicas      ──> evaluate_protocols    ──> Option<ProtocolMatch>
//!   protocol list
//!
//! Tests poke values straight into these functions, no fakes
//! required. The I/O glue in `health.rs` is then a thin caller that
//! collects k8s state, calls these, dispatches the resulting events.

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use crate::protocol::{evaluate_condition, HealthProtocol, HealthProtocols};

/// Default windows. Public so callers can override per-test or
/// per-deployment without forking the engine.
pub const FLAKY_AFTER: Duration = Duration::from_secs(30);
pub const RECOVERY_AFTER: Duration = Duration::from_secs(30);

/// In-memory tracking of one (project, node) pair's health window.
/// Plain data; the engine reads and writes it via copy + return,
/// the caller owns the map of `(project, node) -> state`.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct NodeHealthState {
    /// Last observation in which this node was Ready (desired > 0
    /// AND ready >= desired). None until we've ever seen it Ready.
    pub last_ready_at: Option<Instant>,
    /// Last observation in which this node was NOT Ready. None
    /// until we've ever seen it Not-Ready.
    pub last_not_ready_at: Option<Instant>,
    /// Latched: true after we declared flaky, false after we
    /// declared recovered. Flips via the windowed transitions.
    pub declared_flaky: bool,
}

/// What `evaluate_node_health` decided this tick.
///
/// Three pieces:
///   - `next`: the new tracker state.
///   - `desired_status`: what the row SHOULD say, derived from the
///     tracker. The I/O layer compares this to the observed row
///     and writes only when they differ. Sibling writes (e.g.
///     `set_applied` flipping the row to Running while we think
///     it's Flaky) drift on a single tick; this carries the
///     "what's correct" answer so reconciliation falls out for
///     free in both directions.
///   - `event`: the event-of-record for this tick. `Some` only on
///     edges (`Flaky` / `Recovered`); `None` means "no edge, just
///     reconcile if the row drifted."
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeDecision {
    pub next: NodeHealthState,
    pub desired_status: weft_broker_client::protocol::InfraNodeStatus,
    pub event: Option<NodeEdgeEvent>,
}

/// Edge events the supervisor publishes to `infra_event` on
/// transitions. Distinct from `NodeDecision.desired_status` which
/// the row-status reconciliation reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeEdgeEvent {
    BecameFlaky { desired: u32, ready: u32 },
    Recovered,
}

/// What the caller observed for one node this tick.
#[derive(Debug, Clone, Copy)]
pub struct NodeObservation {
    pub desired: u32,
    pub ready: u32,
}

impl NodeObservation {
    pub fn is_ready(self) -> bool {
        self.desired > 0 && self.ready >= self.desired
    }
}

/// Pure: given prior state + this tick's observation + the current
/// instant, compute the next state and whether anything happened.
///
/// Caller invariants:
///   - `prior` MUST be the previous tick's `next` (don't pass empty
///     state if you've seen this node before, that's the bug we
///     fixed in the stop→start dance).
///   - `now` MUST be monotonic with the `prior.last_*` instants.
pub fn evaluate_node_health(
    prior: NodeHealthState,
    observation: NodeObservation,
    now: Instant,
    flaky_after: Duration,
    recovery_after: Duration,
) -> NodeDecision {
    use weft_broker_client::protocol::InfraNodeStatus;
    let mut next = prior.clone();
    let mut event: Option<NodeEdgeEvent> = None;

    if observation.is_ready() {
        next.last_ready_at = Some(now);
        if next.declared_flaky {
            // Recovery window: ready continuously since the LAST
            // time we saw non-ready. If that gap is bigger than the
            // recovery window, flip the flag and record the edge.
            let elapsed_since_not_ready = prior
                .last_not_ready_at
                .map(|t| now.duration_since(t))
                .unwrap_or(Duration::MAX);
            if elapsed_since_not_ready >= recovery_after {
                next.declared_flaky = false;
                event = Some(NodeEdgeEvent::Recovered);
            }
        }
    } else {
        next.last_not_ready_at = Some(now);
        if !next.declared_flaky {
            // Flaky window: not-ready continuously since the LAST
            // time we saw ready. `last_ready_at.is_none()` means we
            // haven't seen this node Ready yet (fresh provision).
            // Do NOT call it flaky in that case : the apply path
            // owns the status until first readiness.
            if let Some(t) = prior.last_ready_at {
                if now.duration_since(t) >= flaky_after {
                    next.declared_flaky = true;
                    event = Some(NodeEdgeEvent::BecameFlaky {
                        desired: observation.desired,
                        ready: observation.ready,
                    });
                }
            }
        }
    }

    // Desired status derived from the LATCH, not from the
    // instantaneous observation. The latch only flips on a window
    // expiry; a single bad replica reading on a tick doesn't drag
    // the row to Flaky, and a single good reading doesn't drag
    // it back to Running. This is what the windows are FOR.
    let desired_status = if next.declared_flaky {
        InfraNodeStatus::Flaky
    } else {
        InfraNodeStatus::Running
    };

    NodeDecision { next, desired_status, event }
}

/// Per-node ready ratios + replica counts the caller computed for
/// the project's nodes. Used by `evaluate_protocols`.
#[derive(Debug, Clone)]
pub struct ProtocolEvalInputs {
    /// Keyed by `(node_id, unit)`. Health is per-unit.
    pub ready_ratio: HashMap<(String, String), f32>,
    pub ready_replicas: HashMap<(String, String), u32>,
    pub project_status: weft_broker_client::protocol::ProjectStatus,
    /// Did the health loop perform the current deactivation? Gates
    /// `HealthCondition::DeactivatedByHealth` (auto-recover).
    pub deactivated_by_health: bool,
}

impl Default for ProtocolEvalInputs {
    fn default() -> Self {
        Self {
            ready_ratio: HashMap::new(),
            ready_replicas: HashMap::new(),
            project_status: weft_broker_client::protocol::ProjectStatus::Registered,
            deactivated_by_health: false,
        }
    }
}

/// What `evaluate_protocols` decided. None = no protocol fires this
/// tick. Some = the named protocol matched and the caller should
/// dispatch its action.
#[derive(Debug, Clone)]
pub struct ProtocolMatch<'a> {
    pub protocol: &'a HealthProtocol,
}

/// Pure: given the protocol list, the set of already-fired protocol
/// names, the in-flight flag, and the inputs computed from this
/// tick, return the FIRST protocol that matches.
///
/// Skips:
///   - if the project has any protocol in-flight (returns None);
///   - protocols already in `fired_set` (queued for next tick);
///   - protocols whose `when` condition is false.
pub fn evaluate_protocols<'a>(
    protocols: &'a HealthProtocols,
    fired_set: &HashSet<String>,
    in_flight: bool,
    inputs: &ProtocolEvalInputs,
) -> Option<ProtocolMatch<'a>> {
    if in_flight {
        return None;
    }
    for proto in &protocols.protocols {
        if fired_set.contains(&proto.name) {
            continue;
        }
        let ctx = crate::protocol::ConditionContext {
            ready_ratio: &inputs.ready_ratio,
            ready_replicas: &inputs.ready_replicas,
            project_status: inputs.project_status,
            deactivated_by_health: inputs.deactivated_by_health,
        };
        if !evaluate_condition(&proto.when, &ctx) {
            continue;
        }
        return Some(ProtocolMatch { protocol: proto });
    }
    None
}

/// Pure: are all expected-running units in the project healthy now?
///
/// "Healthy" = `ready_ratio` >= 1.0 for every (node, unit) in the
/// map. The map only contains units the user expects up right now
/// (running / flaky); a stopped/provisioning unit isn't in it and so
/// doesn't block re-arm.
///
/// When this returns true, the caller should clear the per-project
/// `fired_set` so protocols can re-fire on the next degradation.
pub fn all_units_healthy(ratios: &HashMap<(String, String), f32>) -> bool {
    ratios.values().all(|r| *r >= 1.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{HealthCondition, ProtocolAction};
    use weft_broker_client::protocol::InfraNodeStatus as Status;

    fn t0() -> Instant {
        Instant::now()
    }

    fn obs(desired: u32, ready: u32) -> NodeObservation {
        NodeObservation { desired, ready }
    }

    // ---------- evaluate_node_health: NoChange paths ----------

    #[test]
    fn fresh_node_first_observation_ready_no_change() {
        let now = t0();
        let result = evaluate_node_health(
            NodeHealthState::default(),
            obs(1, 1),
            now,
            FLAKY_AFTER,
            RECOVERY_AFTER,
        );
        assert!(result.event.is_none());
        assert_eq!(result.desired_status, Status::Running);
        assert_eq!(result.next.last_ready_at, Some(now));
        assert_eq!(result.next.last_not_ready_at, None);
        assert!(!result.next.declared_flaky);
    }

    #[test]
    fn fresh_node_first_observation_not_ready_no_flaky_yet() {
        // The provisioning false-alarm scenario: a node we've never
        // seen Ready shouldn't be declared flaky just because some
        // other supervisor process saw a previous deployment Ready.
        let now = t0();
        let result = evaluate_node_health(
            NodeHealthState::default(),
            obs(1, 0),
            now,
            FLAKY_AFTER,
            RECOVERY_AFTER,
        );
        assert!(result.event.is_none());
        assert_eq!(result.desired_status, Status::Running);
        assert!(!result.next.declared_flaky);
        assert_eq!(result.next.last_not_ready_at, Some(now));
    }

    #[test]
    fn ready_then_briefly_not_ready_no_flaky() {
        // Ready at t0; not ready at t0 + 5s. Flaky window 30s.
        let t = t0();
        let r1 = evaluate_node_health(
            NodeHealthState::default(),
            obs(1, 1),
            t,
            FLAKY_AFTER,
            RECOVERY_AFTER,
        );
        assert!(r1.event.is_none());
        let r2 = evaluate_node_health(
            r1.next,
            obs(1, 0),
            t + Duration::from_secs(5),
            FLAKY_AFTER,
            RECOVERY_AFTER,
        );
        assert!(r2.event.is_none());
        assert_eq!(r2.desired_status, Status::Running);
    }

    // ---------- BecameFlaky edge ----------

    #[test]
    fn not_ready_past_flaky_window_becomes_flaky() {
        let t = t0();
        let state = NodeHealthState {
            last_ready_at: Some(t),
            ..Default::default()
        };
        let result = evaluate_node_health(
            state,
            obs(1, 0),
            t + Duration::from_secs(31),
            FLAKY_AFTER,
            RECOVERY_AFTER,
        );
        assert_eq!(
            result.event,
            Some(NodeEdgeEvent::BecameFlaky { desired: 1, ready: 0 })
        );
        assert!(result.next.declared_flaky);
        assert_eq!(result.desired_status, Status::Flaky);
    }

    #[test]
    fn already_flaky_does_not_re_fire_event_but_desired_stays_flaky() {
        let t = t0();
        let state = NodeHealthState {
            last_ready_at: Some(t),
            last_not_ready_at: Some(t + Duration::from_secs(31)),
            declared_flaky: true,
        };
        let result = evaluate_node_health(
            state,
            obs(1, 0),
            t + Duration::from_secs(41),
            FLAKY_AFTER,
            RECOVERY_AFTER,
        );
        // No edge event : we're already past the transition.
        assert!(result.event.is_none());
        // But desired_status is Flaky (latch is still set). This is
        // what drives the I/O-layer reconciliation: even on
        // NoChange ticks, the row should stay (or return to) Flaky.
        assert_eq!(result.desired_status, Status::Flaky);
    }

    #[test]
    fn flaky_with_partial_replicas_carries_counts() {
        let t = t0();
        let state = NodeHealthState {
            last_ready_at: Some(t),
            ..Default::default()
        };
        let result = evaluate_node_health(
            state,
            obs(3, 1),
            t + Duration::from_secs(35),
            FLAKY_AFTER,
            RECOVERY_AFTER,
        );
        assert_eq!(
            result.event,
            Some(NodeEdgeEvent::BecameFlaky { desired: 3, ready: 1 })
        );
    }

    // ---------- Recovered edge ----------

    #[test]
    fn flaky_then_ready_past_recovery_window_recovers() {
        let t = t0();
        let state = NodeHealthState {
            last_ready_at: Some(t),
            last_not_ready_at: Some(t + Duration::from_secs(10)),
            declared_flaky: true,
        };
        let result = evaluate_node_health(
            state,
            obs(1, 1),
            t + Duration::from_secs(50),
            FLAKY_AFTER,
            RECOVERY_AFTER,
        );
        assert_eq!(result.event, Some(NodeEdgeEvent::Recovered));
        assert!(!result.next.declared_flaky);
        assert_eq!(result.desired_status, Status::Running);
    }

    #[test]
    fn flaky_then_briefly_ready_does_not_recover() {
        let t = t0();
        let state = NodeHealthState {
            last_ready_at: Some(t),
            last_not_ready_at: Some(t + Duration::from_secs(30)),
            declared_flaky: true,
        };
        let result = evaluate_node_health(
            state,
            obs(1, 1),
            t + Duration::from_secs(35),
            FLAKY_AFTER,
            RECOVERY_AFTER,
        );
        assert!(result.event.is_none());
        assert!(result.next.declared_flaky);
        // Desired status stays Flaky: row should NOT flip back
        // until the full recovery window elapses.
        assert_eq!(result.desired_status, Status::Flaky);
    }

    #[test]
    fn recovery_window_zero_means_first_ready_recovers() {
        let t = t0();
        let state = NodeHealthState {
            last_ready_at: Some(t),
            last_not_ready_at: Some(t + Duration::from_secs(50)),
            declared_flaky: true,
        };
        let result = evaluate_node_health(
            state,
            obs(1, 1),
            t + Duration::from_secs(51),
            FLAKY_AFTER,
            Duration::from_secs(0),
        );
        assert_eq!(result.event, Some(NodeEdgeEvent::Recovered));
    }

    // ---------- full lifecycle ----------

    #[test]
    fn full_lifecycle_ready_flaky_recovered() {
        let t = t0();
        let mut s = NodeHealthState::default();

        // 1) ready at t0
        let r1 = evaluate_node_health(s, obs(1, 1), t, FLAKY_AFTER, RECOVERY_AFTER);
        assert!(r1.event.is_none());
        s = r1.next;
        // 2) not ready at t+5s (no flip)
        let r2 = evaluate_node_health(s, obs(1, 0), t + Duration::from_secs(5), FLAKY_AFTER, RECOVERY_AFTER);
        assert!(r2.event.is_none());
        s = r2.next;
        // 3) still not ready at t+40s (flaky edge fires)
        let r3 = evaluate_node_health(s, obs(1, 0), t + Duration::from_secs(40), FLAKY_AFTER, RECOVERY_AFTER);
        assert!(matches!(r3.event, Some(NodeEdgeEvent::BecameFlaky { .. })));
        s = r3.next;
        // 4) ready at t+45s (no flip, recovery window not elapsed)
        let r4 = evaluate_node_health(s, obs(1, 1), t + Duration::from_secs(45), FLAKY_AFTER, RECOVERY_AFTER);
        assert!(r4.event.is_none());
        s = r4.next;
        // 5) ready at t+80s (recovered edge fires)
        let r5 = evaluate_node_health(s, obs(1, 1), t + Duration::from_secs(80), FLAKY_AFTER, RECOVERY_AFTER);
        assert_eq!(r5.event, Some(NodeEdgeEvent::Recovered));
    }

    // ---------- reconciliation property ----------

    #[test]
    fn desired_status_follows_latch_not_observation() {
        // Already-flaky state with an instantaneous ready
        // observation INSIDE the recovery window: the latch stays
        // set, so desired_status must stay Flaky. The I/O layer
        // relies on this to reconcile a row that an external
        // writer flipped to Running.
        let t = t0();
        let state = NodeHealthState {
            last_ready_at: Some(t),
            last_not_ready_at: Some(t + Duration::from_secs(40)),
            declared_flaky: true,
        };
        let result = evaluate_node_health(
            state,
            obs(1, 1),
            t + Duration::from_secs(41), // 1s into recovery window
            FLAKY_AFTER,
            RECOVERY_AFTER,
        );
        assert!(result.event.is_none());
        assert!(result.next.declared_flaky);
        assert_eq!(result.desired_status, Status::Flaky);
    }

    #[test]
    fn observation_is_ready_zero_desired() {
        // 0 replicas desired is the Stopped state. The pure
        // function returns is_ready=false (no replicas means
        // nothing observable). The caller is responsible for
        // skipping evaluation when status != running/flaky; the
        // pure function doesn't second-guess that contract.
        assert!(!obs(0, 0).is_ready());
    }

    #[test]
    fn observation_is_ready_exact_match() {
        assert!(obs(3, 3).is_ready());
    }

    #[test]
    fn observation_is_ready_excess() {
        // Ready > desired (transient during rolling restart). Still
        // counts as ready.
        assert!(obs(2, 3).is_ready());
    }

    // ---------- evaluate_protocols ----------

    fn proto(name: &str, when: HealthCondition, action: ProtocolAction) -> HealthProtocol {
        HealthProtocol {
            name: name.to_string(),
            when,
            action,
            timeout_seconds: 1800,
        }
    }

    fn flaky_when() -> HealthCondition {
        HealthCondition::NodeReadyRatioBelow {
            node_id: "*".into(),
            unit: "*".into(),
            ratio: 1.0,
        }
    }

    fn inputs_with_ratio(node: &str, ratio: f32) -> ProtocolEvalInputs {
        let mut r = HashMap::new();
        r.insert((node.to_string(), node.to_string()), ratio);
        ProtocolEvalInputs {
            ready_ratio: r,
            ready_replicas: HashMap::new(),
            project_status: weft_broker_client::protocol::ProjectStatus::Active,
            deactivated_by_health: false,
        }
    }

    #[test]
    fn protocols_match_first_in_list_order() {
        let p = HealthProtocols {
            protocols: vec![
                proto("first", flaky_when(), ProtocolAction::AutoRecover),
                proto(
                    "second",
                    flaky_when(),
                    ProtocolAction::Notify {
                        channel: "ops".into(),
                    },
                ),
            ],
        };
        let inputs = inputs_with_ratio("n1", 0.0);
        let m = evaluate_protocols(&p, &HashSet::new(), false, &inputs);
        assert_eq!(m.expect("match").protocol.name, "first");
    }

    #[test]
    fn protocols_skip_already_fired() {
        let p = HealthProtocols {
            protocols: vec![
                proto("first", flaky_when(), ProtocolAction::AutoRecover),
                proto(
                    "second",
                    flaky_when(),
                    ProtocolAction::Notify {
                        channel: "ops".into(),
                    },
                ),
            ],
        };
        let mut fired = HashSet::new();
        fired.insert("first".to_string());
        let inputs = inputs_with_ratio("n1", 0.0);
        let m = evaluate_protocols(&p, &fired, false, &inputs);
        assert_eq!(m.expect("match").protocol.name, "second");
    }

    #[test]
    fn protocols_no_match_when_in_flight() {
        let p = HealthProtocols {
            protocols: vec![proto("first", flaky_when(), ProtocolAction::AutoRecover)],
        };
        let inputs = inputs_with_ratio("n1", 0.0);
        let m = evaluate_protocols(&p, &HashSet::new(), true, &inputs);
        assert!(m.is_none());
    }

    #[test]
    fn protocols_no_match_when_condition_false() {
        let p = HealthProtocols {
            protocols: vec![proto("first", flaky_when(), ProtocolAction::AutoRecover)],
        };
        // ratio=1.0 → condition NodeReadyRatioBelow(1.0) is false.
        let inputs = inputs_with_ratio("n1", 1.0);
        let m = evaluate_protocols(&p, &HashSet::new(), false, &inputs);
        assert!(m.is_none());
    }

    #[test]
    fn protocols_empty_list_no_match() {
        let p = HealthProtocols {
            protocols: vec![],
        };
        let m = evaluate_protocols(&p, &HashSet::new(), false, &ProtocolEvalInputs::default());
        assert!(m.is_none());
    }

    // ---------- all_units_healthy ----------

    fn unit_key(node: &str, unit: &str) -> (String, String) {
        (node.to_string(), unit.to_string())
    }

    #[test]
    fn all_healthy_when_every_ratio_1() {
        let mut r = HashMap::new();
        r.insert(unit_key("a", "a"), 1.0);
        r.insert(unit_key("a", "b"), 1.0);
        assert!(all_units_healthy(&r));
    }

    #[test]
    fn not_healthy_when_one_below() {
        let mut r = HashMap::new();
        r.insert(unit_key("a", "a"), 1.0);
        r.insert(unit_key("a", "b"), 0.5);
        assert!(!all_units_healthy(&r));
    }

    #[test]
    fn empty_map_is_healthy() {
        // No expected-running units (every unit stopped/provisioning)
        // means nothing keeps protocols "armed". The map only holds
        // units the user expects up right now.
        let r = HashMap::new();
        assert!(all_units_healthy(&r));
    }
}
