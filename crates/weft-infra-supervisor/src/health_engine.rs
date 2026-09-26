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
//!   prior fired scopes     +
//!   seen units             ──> evaluate_protocols    ──> Option<ProtocolMatch>
//!   protocol list
//!
//! Tests poke values straight into these functions, no fakes
//! required. The I/O glue in `health.rs` is then a thin caller that
//! collects k8s state, calls these, dispatches the resulting events.

use std::collections::{BTreeSet, HashMap};
use std::time::{Duration, Instant};

use weft_broker_client::protocol::InfraCopy;

use crate::protocol::{
    broken_copies, evaluate_condition, names_infra, still_broken_copies, ConditionContext, HealthProtocol, HealthProtocols,
    ProtocolAction, UnitView,
};

/// Default windows. Public so callers can override per-test or
/// per-instance without forking the engine.
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

impl NodeHealthState {
    /// A fresh latch for a unit whose row already says `status`, so the
    /// first look after the latch is gone (a command on the copy, a
    /// restart, the project moving to another pod: the latch lives in
    /// this pod's memory only) argues from the durable row instead of
    /// against it.
    ///
    /// A unit the row calls `Flaky` starts declared flaky with its
    /// not-ready edge at `now`, so it recovers only through the full
    /// recovery window; an empty latch would instead read as "never
    /// seen ready, nothing declared", derive `Running`, and rewrite a
    /// frozen, still-broken unit to `Running` for as long as it never
    /// becomes ready.
    ///
    /// A `watched` unit (one the loop's watch can see ready, see
    /// `UnitRuntime::watched`) the row calls `Running` whose copy was
    /// applied at least `flaky_after` ago (`applied_at_unix`, wall clock)
    /// is KNOWN: its apply finished and its workload has had its whole
    /// window to show up, so it starts as seen ready at `now`. A workload
    /// missing from the watch then reads as zero ready and turns flaky
    /// after the window, instead of staying unknown (and counting as
    /// healthy) for ever. One applied more recently starts empty: its
    /// workload may not have reached the watch yet. So does a unit the
    /// watch never shows ready (a Job, a DaemonSet, zero replicas): it
    /// would otherwise read as vanished and park healthy readers. The
    /// caller never evaluates a unit outside {Running, Flaky}.
    pub fn seeded_from(
        status: weft_broker_client::protocol::InfraNodeStatus,
        watched: bool,
        applied_at_unix: Option<i64>,
        flaky_after: Duration,
        now: Instant,
        now_unix: i64,
    ) -> Self {
        use weft_broker_client::protocol::InfraNodeStatus;
        let flaky = status == InfraNodeStatus::Flaky;
        let settled = status == InfraNodeStatus::Running
            && watched
            && applied_at_unix.is_some_and(|at| now_unix.saturating_sub(at) >= flaky_after.as_secs() as i64);
        Self {
            last_ready_at: settled.then_some(now),
            last_not_ready_at: flaky.then_some(now),
            declared_flaky: flaky,
        }
    }
}

impl NodeHealthState {
    /// Whether this latch has ever observed the unit's workload (or was
    /// seeded from a row that says it is broken).
    pub fn has_seen(&self) -> bool {
        self.last_ready_at.is_some() || self.last_not_ready_at.is_some()
    }
}

/// Pure: one unit's decision from what this look saw of its workload
/// (`None`: the workload is not in the watched set). A workload never
/// seen is UNKNOWN (`None` back, the latch untouched): a copy just
/// applied whose workload has not reached the watch yet is neither ready
/// nor broken. One seen before and now absent reads as zero ready.
/// A workload present at zero replicas is UNKNOWN too only when weft
/// asked for that zero (`zero_intended`, from
/// `UnitRuntime::zero_replicas_intended`: a protocol's `Scale` to 0, or
/// a spec at 0): nothing is meant to serve, so it is neither ready nor
/// broken. Any other zero (scaled down outside weft) is not ready and
/// runs the flaky window.
pub fn observe_unit(
    prior: NodeHealthState,
    workload: Option<NodeObservation>,
    zero_intended: bool,
    now: Instant,
    flaky_after: Duration,
    recovery_after: Duration,
) -> Option<NodeDecision> {
    let observation = match workload {
        Some(observation) if observation.desired == 0 && zero_intended => return None,
        Some(observation) => observation,
        None if prior.has_seen() => NodeObservation { desired: 0, ready: 0 },
        None => return None,
    };
    Some(evaluate_node_health(prior, observation, now, flaky_after, recovery_after))
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

/// What the protocols are evaluated on: every seen unit of every copy
/// expected to run now, and the project's listening.
#[derive(Debug, Clone)]
pub struct ProtocolEvalInputs {
    pub units: Vec<UnitView>,
    pub project_status: weft_broker_client::protocol::ProjectStatus,
    /// Is something the health loop took down still down? Gates
    /// `HealthCondition::HealthParked` (auto-recover).
    pub health_parked: bool,
}

impl ProtocolEvalInputs {
    fn context(&self) -> ConditionContext<'_> {
        ConditionContext { units: &self.units, project_status: self.project_status, health_parked: self.health_parked }
    }
}

/// What `evaluate_protocols` decided. None = no protocol fires this
/// tick. Some = the named protocol matched and the caller should
/// dispatch its action.
#[derive(Debug, Clone)]
pub struct ProtocolMatch<'a> {
    pub protocol: &'a HealthProtocol,
    /// Where the action lands, see [`protocol_scope`]: the copies it
    /// acts on, the copies still broken for an auto-recover, empty for a
    /// notification or a take-down of the whole project.
    pub scope: BTreeSet<InfraCopy>,
}

/// Pure: where `proto` holds this look. `None` when it does not hold
/// at all.
///
/// - An action on broken copies: the copies its condition finds broken
///   ([`broken_copies`], `None` rather than an empty set: such an action
///   with no copy has nothing to aim at); when the condition names no
///   infra, [`ProtocolAction::unowned_scope`] (the named node's shared
///   copy for a scale or a bounce, the empty set, the whole project, for
///   a take-down).
/// - An auto-recover: the copies still broken ([`still_broken_copies`]),
///   when the condition holds with those set aside. Empty when nothing
///   is, and every reader the loop took comes back.
/// - A notification: the empty set.
pub fn protocol_scope(proto: &HealthProtocol, ctx: &ConditionContext<'_>) -> Option<BTreeSet<InfraCopy>> {
    if matches!(proto.action, ProtocolAction::AutoRecover) {
        let broken = still_broken_copies(&proto.when, ctx);
        let rest: Vec<UnitView> = ctx.units.iter().filter(|u| !broken.contains(&u.copy())).cloned().collect();
        return evaluate_condition(&proto.when, &ConditionContext { units: &rest, ..ctx.clone() }).then_some(broken);
    }
    if !evaluate_condition(&proto.when, ctx) {
        return None;
    }
    if !proto.action.aims_at_broken_copies() {
        return Some(BTreeSet::new());
    }
    if !names_infra(&proto.when) {
        return Some(proto.action.unowned_scope());
    }
    let broken = broken_copies(&proto.when, ctx);
    (!broken.is_empty()).then_some(broken)
}

/// Pure: what a protocol that already fired (`acted`, see
/// [`record_fire`]) still has to do where it holds now (`scope`), or
/// `None` when it has done it all.
///
/// - An auto-recover fires again once a copy that was broken when it
///   last fired, or broke since ([`rearm`]), is broken no longer, with
///   the copies still broken now.
/// - An action on copies acts only on the copies it has not acted on:
///   a second copy breaking after the first is scaled, bounced or taken
///   down alone, never the first one again.
/// - An action on no copy (a notification, a take-down of the whole
///   project) fired once for the episode.
fn still_to_do(proto: &HealthProtocol, acted: &BTreeSet<InfraCopy>, scope: BTreeSet<InfraCopy>) -> Option<BTreeSet<InfraCopy>> {
    if matches!(proto.action, ProtocolAction::AutoRecover) {
        return (!acted.is_subset(&scope)).then_some(scope);
    }
    let fresh: BTreeSet<InfraCopy> = scope.difference(acted).cloned().collect();
    (!fresh.is_empty()).then_some(fresh)
}

/// Pure: given the protocol list, what each already-fired protocol
/// acted on and still holds for (`fired`: name to the copies its action
/// aimed at, see [`rearm`]), the in-flight flag, and the inputs computed
/// from this tick, return the FIRST protocol that has something to do,
/// aimed at only that ([`still_to_do`]).
///
/// Skips:
///   - if the project has any protocol in-flight (returns None);
///   - protocols that do not hold ([`protocol_scope`]);
///   - a protocol already fired with nothing left to do.
pub fn evaluate_protocols<'a>(
    protocols: &'a HealthProtocols,
    fired: &HashMap<String, BTreeSet<InfraCopy>>,
    in_flight: bool,
    inputs: &ProtocolEvalInputs,
) -> Option<ProtocolMatch<'a>> {
    if in_flight {
        return None;
    }
    let ctx = inputs.context();
    for proto in &protocols.protocols {
        let Some(scope) = protocol_scope(proto, &ctx) else {
            continue;
        };
        let scope = match fired.get(&proto.name) {
            None => scope,
            Some(acted) => match still_to_do(proto, acted, scope) {
                Some(scope) => scope,
                None => continue,
            },
        };
        return Some(ProtocolMatch { protocol: proto, scope });
    }
    None
}

/// Pure: note that `proto` fired on `scope`. An action on copies adds
/// them to what it acted on; an auto-recover remembers the copies still
/// broken when it fired, so it fires again when one of them heals.
pub fn record_fire(fired: &mut HashMap<String, BTreeSet<InfraCopy>>, proto: &HealthProtocol, scope: &BTreeSet<InfraCopy>) {
    let acted = fired.entry(proto.name.clone()).or_default();
    if matches!(proto.action, ProtocolAction::AutoRecover) {
        *acted = scope.clone();
    } else {
        acted.extend(scope.iter().cloned());
    }
}

/// Pure: is every seen unit of the project healthy now: fully ready,
/// and none declared flaky?
pub fn all_units_healthy(units: &[UnitView]) -> bool {
    units.iter().all(|u| u.ready_ratio >= 1.0 && !u.flaky)
}

/// Pure: re-arm what the protocols fired, copy by copy.
///
/// - An action on copies keeps, of those, only the ones it still holds
///   for ([`protocol_scope`]): a copy that healed is armed again, so its
///   next episode fires, while every other copy stays as it was.
/// - An auto-recover is forgotten once it no longer holds (nothing the
///   loop took is still down), and otherwise also counts every copy
///   broken now, so a copy that breaks after it fired is restored when
///   it heals.
/// - A notification re-arms once every seen unit of the project is
///   healthy; a take-down of the whole project once it no longer holds.
/// - A protocol no longer configured is forgotten.
pub fn rearm(fired: &mut HashMap<String, BTreeSet<InfraCopy>>, protocols: &HealthProtocols, inputs: &ProtocolEvalInputs) {
    let ctx = inputs.context();
    let healthy = all_units_healthy(&inputs.units);
    fired.retain(|name, acted| {
        let Some(proto) = protocols.protocols.iter().find(|p| p.name == *name) else {
            return false;
        };
        let holds = protocol_scope(proto, &ctx);
        match &proto.action {
            ProtocolAction::AutoRecover => match holds {
                Some(broken) => {
                    acted.extend(broken);
                    true
                }
                None => false,
            },
            ProtocolAction::Notify { .. } => !healthy,
            _ if acted.is_empty() => holds.is_some(),
            _ => {
                let holds = holds.unwrap_or_default();
                acted.retain(|copy| holds.contains(copy));
                !acted.is_empty()
            }
        }
    });
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

    // ---------- latch seeding from the row ----------

    /// A latch seeded from a `Flaky` row keeps saying Flaky while the
    /// unit stays unready (no `Running` rewrite), and recovers only
    /// after the recovery window of continuous readiness; a latch
    /// seeded from `Running` is the empty latch.
    #[test]
    fn latch_seeded_from_row_status_argues_from_the_row() {
        let now = t0();
        let seeded = NodeHealthState::seeded_from(Status::Flaky, true, None, FLAKY_AFTER, now, 0);
        let still_down =
            evaluate_node_health(seeded.clone(), obs(1, 0), now, FLAKY_AFTER, RECOVERY_AFTER);
        assert_eq!(still_down.desired_status, Status::Flaky);
        assert_eq!(still_down.event, None);
        // Ready right away: not yet recovered (the window has not run).
        let ready_now =
            evaluate_node_health(seeded.clone(), obs(1, 1), now, FLAKY_AFTER, RECOVERY_AFTER);
        assert_eq!(ready_now.desired_status, Status::Flaky);
        // Ready past the window since the seeded not-ready edge: recovered.
        let ready_later = evaluate_node_health(
            seeded,
            obs(1, 1),
            now + RECOVERY_AFTER,
            FLAKY_AFTER,
            RECOVERY_AFTER,
        );
        assert_eq!(ready_later.desired_status, Status::Running);
        assert_eq!(ready_later.event, Some(NodeEdgeEvent::Recovered));
        assert_eq!(
            NodeHealthState::seeded_from(Status::Running, true, Some(100), FLAKY_AFTER, now, 100 + 29),
            NodeHealthState::default(),
            "applied inside its window: its workload may not be watched yet"
        );
    }

    /// A Running copy applied longer ago than its flaky window is known
    /// from the row alone: after the latch is lost (a command, a restart,
    /// a move to another pod) a missing workload still reads as zero
    /// ready and turns flaky after the window, instead of staying
    /// unknown for ever.
    #[test]
    fn a_settled_running_copy_is_known_without_the_latch() {
        let now = t0();
        let seeded = NodeHealthState::seeded_from(Status::Running, true, Some(100), FLAKY_AFTER, now, 100 + 30);
        assert!(seeded.has_seen());
        let gone = observe_unit(seeded, None, false, now, FLAKY_AFTER, RECOVERY_AFTER).expect("known, not unknown");
        assert_eq!(gone.desired_status, Status::Running, "inside the window");
        let later = observe_unit(gone.next, None, false, now + FLAKY_AFTER, FLAKY_AFTER, RECOVERY_AFTER).expect("known");
        assert_eq!(later.event, Some(NodeEdgeEvent::BecameFlaky { desired: 0, ready: 0 }));
        // Never applied: nothing says a workload should exist.
        assert!(!NodeHealthState::seeded_from(Status::Running, true, None, FLAKY_AFTER, now, 1_000).has_seen());
        // A unit the watch never shows ready (a Job, a DaemonSet, zero
        // replicas) is never seeded as seen: it would read as vanished.
        let unwatched = NodeHealthState::seeded_from(Status::Running, false, Some(100), FLAKY_AFTER, now, 100 + 30);
        assert!(!unwatched.has_seen());
        assert_eq!(observe_unit(unwatched, None, false, now + FLAKY_AFTER, FLAKY_AFTER, RECOVERY_AFTER), None);
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

    fn unit(scaled_to: Option<u32>) -> weft_broker_client::protocol::UnitRuntime {
        weft_broker_client::protocol::UnitRuntime {
            status: Status::Running,
            stop_behavior: weft_core::StopBehavior::ScaleToZero,
            flaky_after_seconds: 30,
            recovery_after_seconds: 30,
            image_refs: Default::default(),
            watched: true,
            scaled_to,
        }
    }

    /// A zero a protocol's `Scale` asked for is neither ready nor broken,
    /// however long it lasts.
    #[test]
    fn a_protocol_scale_to_zero_stays_unknown() {
        let t = t0();
        let seen = observe_unit(NodeHealthState::default(), Some(obs(1, 1)), false, t, FLAKY_AFTER, RECOVERY_AFTER)
            .expect("seen");
        let later = t + Duration::from_secs(3600);
        let intended = unit(Some(0)).zero_replicas_intended();
        assert!(intended);
        assert_eq!(observe_unit(seen.next, Some(obs(0, 0)), intended, later, FLAKY_AFTER, RECOVERY_AFTER), None);
    }

    /// A workload scaled to zero outside weft, while the row expects
    /// replicas, is not ready and turns flaky after the window.
    #[test]
    fn an_outside_scale_to_zero_turns_flaky() {
        let t = t0();
        let seen = observe_unit(NodeHealthState::default(), Some(obs(1, 1)), false, t, FLAKY_AFTER, RECOVERY_AFTER)
            .expect("seen");
        let intended = unit(None).zero_replicas_intended();
        assert!(!intended, "a watched unit with no protocol scale expects replicas");
        let zero = observe_unit(seen.next, Some(obs(0, 0)), intended, t + FLAKY_AFTER, FLAKY_AFTER, RECOVERY_AFTER)
            .expect("an unasked zero is known");
        assert_eq!(zero.event, Some(NodeEdgeEvent::BecameFlaky { desired: 0, ready: 0 }));
        assert_eq!(zero.desired_status, Status::Flaky);
    }

    /// After a protocol scaled a unit to zero, a `Scale` back up records
    /// the new replicas: the look evaluates the workload again, ready
    /// once its replicas are up, flaky if they never come.
    #[test]
    fn a_scale_back_up_resumes_evaluation() {
        let t = t0();
        let seen = observe_unit(NodeHealthState::default(), Some(obs(1, 1)), false, t, FLAKY_AFTER, RECOVERY_AFTER)
            .expect("seen");
        assert_eq!(
            observe_unit(seen.next.clone(), Some(obs(0, 0)), unit(Some(0)).zero_replicas_intended(), t + FLAKY_AFTER, FLAKY_AFTER, RECOVERY_AFTER),
            None
        );
        let up = unit(Some(2)).zero_replicas_intended();
        assert!(!up);
        let ready = observe_unit(seen.next.clone(), Some(obs(2, 2)), up, t + FLAKY_AFTER * 2, FLAKY_AFTER, RECOVERY_AFTER)
            .expect("known again");
        assert_eq!(ready.desired_status, Status::Running);
        let stuck = observe_unit(seen.next, Some(obs(2, 0)), up, t + FLAKY_AFTER * 2, FLAKY_AFTER, RECOVERY_AFTER)
            .expect("known again");
        assert_eq!(stuck.event, Some(NodeEdgeEvent::BecameFlaky { desired: 2, ready: 0 }));
    }

    /// A row stamped before `scaled_to` existed (and before `watched`)
    /// says nothing about intent: a zero there is broken, as it always
    /// was, and turns flaky after the window.
    #[test]
    fn a_legacy_row_zero_turns_flaky() {
        let t = t0();
        let mut legacy = unit(None);
        legacy.watched = false;
        let intended = legacy.zero_replicas_intended();
        assert!(!intended);
        let seen = observe_unit(NodeHealthState::default(), Some(obs(1, 1)), false, t, FLAKY_AFTER, RECOVERY_AFTER)
            .expect("seen");
        let zero = observe_unit(seen.next, Some(obs(0, 0)), intended, t + FLAKY_AFTER, FLAKY_AFTER, RECOVERY_AFTER)
            .expect("known");
        assert_eq!(zero.desired_status, Status::Flaky);
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

    // ---------- observe_unit: unknown vs vanished ----------

    /// A workload the loop has never seen is unknown: no decision, the
    /// latch untouched, whatever the windows. Once seen, its absence
    /// reads as zero ready and runs the flaky window like any not-ready.
    #[test]
    fn an_unseen_workload_is_unknown_and_a_vanished_one_is_not_ready() {
        let t = t0();
        assert_eq!(
            observe_unit(NodeHealthState::default(), None, false, t + Duration::from_secs(600), FLAKY_AFTER, RECOVERY_AFTER),
            None
        );
        let seen = observe_unit(NodeHealthState::default(), Some(obs(1, 1)), false, t, FLAKY_AFTER, RECOVERY_AFTER)
            .expect("seen");
        let gone = observe_unit(seen.next, None, false, t + Duration::from_secs(31), FLAKY_AFTER, RECOVERY_AFTER)
            .expect("vanished is known");
        assert_eq!(gone.event, Some(NodeEdgeEvent::BecameFlaky { desired: 0, ready: 0 }));
        // A row seeded Flaky has been seen broken: its absence keeps it so.
        let seeded = NodeHealthState::seeded_from(Status::Flaky, true, None, FLAKY_AFTER, t, 0);
        let still = observe_unit(seeded, None, false, t, FLAKY_AFTER, RECOVERY_AFTER).expect("seeded is known");
        assert_eq!(still.desired_status, Status::Flaky);
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

    fn view(node: &str, member: Option<&str>, ratio: f32, flaky: bool) -> UnitView {
        UnitView {
            node_id: node.into(),
            member: member.map(|m| weft_core::member::MemberId::new(m).unwrap()),
            unit: node.into(),
            ready_ratio: ratio,
            ready: (ratio >= 1.0) as u32,
            flaky,
        }
    }

    fn inputs(units: Vec<UnitView>) -> ProtocolEvalInputs {
        ProtocolEvalInputs {
            units,
            project_status: weft_broker_client::protocol::ProjectStatus::Active,
            health_parked: false,
        }
    }

    fn inputs_with_ratio(node: &str, ratio: f32) -> ProtocolEvalInputs {
        inputs(vec![view(node, None, ratio, false)])
    }

    fn fired(name: &str, scope: BTreeSet<InfraCopy>) -> HashMap<String, BTreeSet<InfraCopy>> {
        HashMap::from([(name.to_string(), scope)])
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
        let m = evaluate_protocols(&p, &HashMap::new(), false, &inputs);
        assert_eq!(m.expect("match").protocol.name, "first");
    }

    #[test]
    fn protocols_skip_already_fired() {
        let p = HealthProtocols {
            protocols: vec![
                proto("first", flaky_when(), ProtocolAction::ParkTriggers),
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
        let n1 = BTreeSet::from([InfraCopy { node_id: "n1".into(), member: None }]);
        let m = evaluate_protocols(&p, &fired("first", n1), false, &inputs);
        assert_eq!(m.expect("match").protocol.name, "second");
    }

    #[test]
    fn protocols_no_match_when_in_flight() {
        let p = HealthProtocols {
            protocols: vec![proto("first", flaky_when(), ProtocolAction::AutoRecover)],
        };
        let inputs = inputs_with_ratio("n1", 0.0);
        let m = evaluate_protocols(&p, &HashMap::new(), true, &inputs);
        assert!(m.is_none());
    }

    #[test]
    fn protocols_no_match_when_condition_false() {
        let p = HealthProtocols {
            protocols: vec![proto("first", flaky_when(), ProtocolAction::AutoRecover)],
        };
        // ratio=1.0 → condition NodeReadyRatioBelow(1.0) is false.
        let inputs = inputs_with_ratio("n1", 1.0);
        let m = evaluate_protocols(&p, &HashMap::new(), false, &inputs);
        assert!(m.is_none());
    }

    #[test]
    fn protocols_empty_list_no_match() {
        let p = HealthProtocols {
            protocols: vec![],
        };
        let m = evaluate_protocols(&p, &HashMap::new(), false, &inputs(Vec::new()));
        assert!(m.is_none());
    }

    /// The default park fires on a unit's latch, never on one reading:
    /// a copy at zero ready that is not declared flaky parks nothing; a
    /// declared-flaky one parks, aimed at exactly its copy.
    #[test]
    fn the_default_park_waits_for_the_latch_and_aims_at_the_broken_copy() {
        let p = crate::protocol::default_protocols();
        let one_bad_reading = inputs(vec![view("svc", Some("ada"), 0.0, false), view("db", None, 1.0, false)]);
        assert!(evaluate_protocols(&p, &HashMap::new(), false, &one_bad_reading).is_none());

        let latched = inputs(vec![view("svc", Some("ada"), 0.0, true), view("db", None, 1.0, false)]);
        let m = evaluate_protocols(&p, &HashMap::new(), false, &latched).expect("park");
        assert_eq!(m.protocol.name, "park-while-infra-broken");
        let ada_svc = InfraCopy { node_id: "svc".into(), member: Some(weft_core::member::MemberId::new("ada").unwrap()) };
        assert_eq!(m.scope, BTreeSet::from([ada_svc.clone()]));

        // Fired on ada's copy: the same breakage does not fire again, a
        // second copy breaking does, aimed at that copy alone.
        let acted = fired("park-while-infra-broken", BTreeSet::from([ada_svc.clone()]));
        assert!(evaluate_protocols(&p, &acted, false, &latched).is_none());
        let both = inputs(vec![view("svc", Some("ada"), 0.0, true), view("db", None, 0.0, true)]);
        let m = evaluate_protocols(&p, &acted, false, &both).expect("the new breakage parks");
        assert_eq!(m.scope, BTreeSet::from([InfraCopy { node_id: "db".into(), member: None }]));
    }

    /// A scale or a bounce fires once per copy: ada's broken copy is
    /// bounced, and when bob's breaks after it, only bob's is.
    #[test]
    fn a_protocol_acts_once_per_copy() {
        let p = HealthProtocols {
            protocols: vec![proto(
                "bounce",
                HealthCondition::NodeFlaky { node_id: "svc".into(), unit: "*".into() },
                ProtocolAction::BouncePods { node_id: "svc".into(), unit: "svc".into() },
            )],
        };
        let ada = InfraCopy { node_id: "svc".into(), member: Some(weft_core::member::MemberId::new("ada").unwrap()) };
        let bob = InfraCopy { node_id: "svc".into(), member: Some(weft_core::member::MemberId::new("bob").unwrap()) };
        let mut acted = HashMap::new();
        let first = inputs(vec![view("svc", Some("ada"), 0.0, true), view("svc", Some("bob"), 1.0, false)]);
        let m = evaluate_protocols(&p, &acted, false, &first).expect("ada's copy breaks");
        assert_eq!(m.scope, BTreeSet::from([ada.clone()]));
        record_fire(&mut acted, m.protocol, &m.scope);
        let then = inputs(vec![view("svc", Some("ada"), 0.0, true), view("svc", Some("bob"), 0.0, true)]);
        rearm(&mut acted, &p, &then);
        let m = evaluate_protocols(&p, &acted, false, &then).expect("bob's copy breaks after");
        assert_eq!(m.scope, BTreeSet::from([bob]), "ada's copy is not bounced again");
        record_fire(&mut acted, m.protocol, &m.scope);
        assert!(evaluate_protocols(&p, &acted, false, &then).is_none());
    }

    /// A scale on a broken shared copy aims at that shared copy.
    #[test]
    fn a_scale_on_a_broken_shared_copy_aims_at_it() {
        let p = HealthProtocols {
            protocols: vec![proto(
                "scale",
                flaky_when(),
                ProtocolAction::Scale { node_id: "svc".into(), unit: "svc".into(), replicas: 2 },
            )],
        };
        let m = evaluate_protocols(&p, &HashMap::new(), false, &inputs(vec![view("svc", None, 0.5, false)]))
            .expect("the shared copy is below its ratio");
        assert_eq!(m.scope, BTreeSet::from([InfraCopy { node_id: "svc".into(), member: None }]));
    }

    /// A protocol whose condition names no infra acts where it did
    /// before copies had owners: a scale or a bounce on the named node's
    /// shared copy, a take-down on the whole project, each once while it
    /// holds, and again after it stopped holding.
    #[test]
    fn a_protocol_with_no_unit_condition_acts_as_before() {
        use weft_broker_client::protocol::ProjectStatus;
        let active = HealthCondition::ProjectStatusEq { status: ProjectStatus::Active };
        let bounce = HealthProtocols {
            protocols: vec![proto(
                "bounce",
                active.clone(),
                ProtocolAction::BouncePods { node_id: "svc".into(), unit: "svc".into() },
            )],
        };
        let m = evaluate_protocols(&bounce, &HashMap::new(), false, &inputs(Vec::new())).expect("fires");
        assert_eq!(m.scope, BTreeSet::from([InfraCopy { node_id: "svc".into(), member: None }]));

        let park = HealthProtocols { protocols: vec![proto("park", active, ProtocolAction::ParkTriggers)] };
        let mut acted = HashMap::new();
        let m = evaluate_protocols(&park, &acted, false, &inputs(Vec::new())).expect("fires on the whole project");
        assert!(m.scope.is_empty());
        record_fire(&mut acted, m.protocol, &m.scope);
        rearm(&mut acted, &park, &inputs(Vec::new()));
        assert!(evaluate_protocols(&park, &acted, false, &inputs(Vec::new())).is_none(), "once while it holds");
        let inactive = ProtocolEvalInputs { project_status: ProjectStatus::Inactive, ..inputs(Vec::new()) };
        rearm(&mut acted, &park, &inactive);
        assert!(acted.is_empty(), "re-armed once it stopped holding");
    }

    /// Auto-recover reads each copy on its own and names the copies
    /// still broken: while ada's copy is flaky it fires with that copy
    /// (everything else comes back), and only when the health loop parked
    /// something.
    #[test]
    fn the_default_recover_names_the_copies_still_broken() {
        let p = crate::protocol::default_protocols();
        let ada_svc = InfraCopy { node_id: "svc".into(), member: Some(weft_core::member::MemberId::new("ada").unwrap()) };
        let healthy_parked = ProtocolEvalInputs { health_parked: true, ..inputs(vec![view("svc", None, 1.0, false)]) };
        let m = evaluate_protocols(&p, &HashMap::new(), false, &healthy_parked).expect("recover");
        assert_eq!(m.protocol.name, "auto-recover-when-infra-healthy");
        assert!(m.scope.is_empty(), "nothing is still broken");

        let parked_svc = fired("park-while-infra-broken", BTreeSet::from([ada_svc.clone()]));
        let one_still_flaky = ProtocolEvalInputs {
            health_parked: true,
            ..inputs(vec![view("svc", None, 1.0, false), view("svc", Some("ada"), 0.0, true)])
        };
        let m = evaluate_protocols(&p, &parked_svc, false, &one_still_flaky).expect("the rest recovers");
        assert_eq!(m.protocol.name, "auto-recover-when-infra-healthy");
        assert_eq!(m.scope, BTreeSet::from([ada_svc]));

        let nothing_parked = inputs(vec![view("svc", None, 1.0, false)]);
        assert!(evaluate_protocols(&p, &HashMap::new(), false, &nothing_parked).is_none());
    }

    /// A shared-only project whose triggers read only copies the loop
    /// cannot see: nothing is known broken, so a recovery fires with an
    /// empty still-broken set and every reader the loop took comes back.
    #[test]
    fn a_recovery_with_no_seen_copy_restores_everything() {
        let p = crate::protocol::default_protocols();
        let parked = ProtocolEvalInputs { health_parked: true, ..inputs(Vec::new()) };
        let m = evaluate_protocols(&p, &HashMap::new(), false, &parked).expect("recover");
        assert_eq!(m.protocol.name, "auto-recover-when-infra-healthy");
        assert!(m.scope.is_empty());
    }

    /// Re-arming is per copy: a take-down forgets a copy once it healed
    /// and keeps the one still broken; a recovery fires again when a copy
    /// broken at its fire, or broken since, heals, and is forgotten once
    /// nothing it took is down; a notification waits for the whole
    /// project.
    #[test]
    fn rearm_is_copy_by_copy() {
        let p = crate::protocol::default_protocols();
        let ada = InfraCopy { node_id: "svc".into(), member: Some(weft_core::member::MemberId::new("ada").unwrap()) };
        let bob = InfraCopy { node_id: "svc".into(), member: Some(weft_core::member::MemberId::new("bob").unwrap()) };
        let recover = &p.protocols[1];
        let mut acted = HashMap::from([
            ("park-while-infra-broken".to_string(), BTreeSet::from([ada.clone(), bob.clone()])),
            ("gone-from-the-config".to_string(), BTreeSet::new()),
        ]);
        // The recovery fired while ada's copy was still broken.
        record_fire(&mut acted, recover, &BTreeSet::from([ada.clone()]));
        // Ada healed, bob is still flaky, and something is still parked.
        let now = ProtocolEvalInputs {
            health_parked: true,
            ..inputs(vec![view("svc", Some("ada"), 1.0, false), view("svc", Some("bob"), 0.0, true)])
        };
        rearm(&mut acted, &p, &now);
        assert_eq!(acted.get("park-while-infra-broken"), Some(&BTreeSet::from([bob.clone()])));
        assert!(!acted.contains_key("gone-from-the-config"));
        // Bob broke after the recovery fired: counted, so his healing fires it.
        assert_eq!(acted.get("auto-recover-when-infra-healthy"), Some(&BTreeSet::from([ada.clone(), bob.clone()])));
        let m = evaluate_protocols(&p, &acted, false, &now).expect("ada healed since it fired");
        assert_eq!(m.scope, BTreeSet::from([bob.clone()]));
        record_fire(&mut acted, m.protocol, &m.scope);
        assert!(evaluate_protocols(&p, &acted, false, &now).is_none(), "nothing healed since");

        // Nothing is parked any more: the recovery is forgotten.
        let done = inputs(vec![view("svc", Some("ada"), 1.0, false), view("svc", Some("bob"), 1.0, false)]);
        rearm(&mut acted, &p, &done);
        assert!(!acted.contains_key("auto-recover-when-infra-healthy"));

        let notify = HealthProtocols { protocols: vec![proto("tell", flaky_when(), ProtocolAction::Notify { channel: "ops".into() })] };
        let mut told = fired("tell", BTreeSet::new());
        rearm(&mut told, &notify, &inputs(vec![view("svc", None, 0.5, false)]));
        assert!(told.contains_key("tell"), "not healthy yet");
        rearm(&mut told, &notify, &inputs(vec![view("svc", None, 1.0, false)]));
        assert!(told.is_empty());
    }

    // ---------- all_units_healthy ----------

    #[test]
    fn healthy_means_every_unit_ready_and_none_flaky() {
        assert!(all_units_healthy(&[view("a", None, 1.0, false), view("b", None, 1.0, false)]));
        assert!(!all_units_healthy(&[view("a", None, 1.0, false), view("b", None, 0.5, false)]));
        assert!(!all_units_healthy(&[view("a", None, 1.0, true)]), "ready inside the recovery window is not healthy");
        // No expected-running unit keeps nothing armed.
        assert!(all_units_healthy(&[]));
    }
}
