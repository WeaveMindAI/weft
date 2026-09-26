//! HealthProtocols data shape. The user configures this per-project;
//! the supervisor's health loop reads via the broker and evaluates
//! the rules on every tick.
//!
//! Mirror of the design doc section 7.5 (HealthCondition AST plus
//! ProtocolAction enum). Stored in `project.health_protocols_json`
//! as raw JSON; we deserialize on demand.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use weft_broker_client::protocol::InfraCopy;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HealthProtocols {
    /// Ordered: first match wins; while one is in-flight, others
    /// queue.
    #[serde(default)]
    pub protocols: Vec<HealthProtocol>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthProtocol {
    pub name: String,
    pub when: HealthCondition,
    pub action: ProtocolAction,
    // Action timeout. Non-optional with a safe default so "unbounded"
    // can't be expressed by accident: an omitted field inherits the
    // ceiling rather than disabling the bound (which would reopen the
    // action-hang wedge). snake_case to match the rest of the
    // protocol wire shape (the condition/action enums are
    // `rename_all = "snake_case"`); a camelCase rename here was a
    // silent footgun for the same reason.
    #[serde(default = "default_action_timeout_seconds")]
    pub timeout_seconds: u32,
}

/// Default action timeout (30 min). A hung broker/kube call inside a
/// HealthProtocol action is bounded by this; the action fails loud
/// and the slot frees. There is no "unbounded" option by design.
fn default_action_timeout_seconds() -> u32 {
    1800
}

/// Default unit selector for conditions: scan every unit. Lets a
/// config omit `unit` and keep node-wide semantics.
fn wildcard() -> String {
    "*".into()
}

/// A condition on units reads only the units whose workload the health
/// loop has seen: a unit whose workload has not appeared yet is unknown,
/// and no condition on units is true of it (it is neither ready nor
/// broken). One that was seen and then vanished reads as zero ready.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum HealthCondition {
    /// Some matched unit is declared flaky: not ready for its
    /// `flaky_after` window after having been ready, and not yet ready
    /// again for its `recovery_after` window. The same latch that sets
    /// the node's `flaky` status, so one bad reading never makes it true.
    NodeFlaky {
        node_id: String,
        #[serde(default = "wildcard")]
        unit: String,
    },
    NodeReadyRatioBelow {
        node_id: String,
        /// Unit selector. `"*"` (default) scans every unit of the
        /// matched node(s). Health is per-unit, so a condition can
        /// target one unit to match the unit-aware actions.
        #[serde(default = "wildcard")]
        unit: String,
        ratio: f32,
    },
    NodeReadyReplicas {
        node_id: String,
        #[serde(default = "wildcard")]
        unit: String,
        op: CompareOp,
        value: u32,
    },
    /// Match against the project's current lifecycle status. Used
    /// to express "this protocol applies only when the project is
    /// parked / only when it's active." A two-stage AutoRecover is
    /// expressible as
    ///   - `All([infra_broken,  ProjectStatusEq=Active])`  → park,
    ///   - `All([infra_healthy, ProjectStatusEq=Inactive])` → reactivate.
    ProjectStatusEq {
        status: weft_broker_client::protocol::ProjectStatus,
    },
    /// True iff some activation the health loop took down is still
    /// down. The default auto-recover protocol pairs this with "no infra
    /// broken" so it reactivates ONLY what it took down itself: a
    /// person's stop / deactivate clears the mark on what it takes down,
    /// so auto-recover leaves that alone.
    HealthParked,
    /// All sub-conditions must hold. Struct variant (`conds: [...]`)
    /// instead of tuple variant because internally-tagged enums
    /// can't serialize a tuple-newtype-with-Vec via serde_json.
    All { conds: Vec<HealthCondition> },
    /// Any sub-condition holds. Same struct-variant shape as `All`.
    Any { conds: Vec<HealthCondition> },
    /// Negation. Struct variant (not tuple) so it round-trips
    /// cleanly under `#[serde(tag = "kind")]`: internally-tagged
    /// enums only support struct + unit variants. The single
    /// `cond` field carries the negated sub-condition.
    ///
    /// `Vec<HealthCondition>` (not `Box<HealthCondition>`) avoids
    /// a known serde trait-monomorphization blowup on
    /// `Box<RecursiveEnum>`. The contract is one element; the
    /// evaluator takes `.first()` and treats empty as `false`
    /// (defensive against malformed configs).
    Not { cond: Vec<HealthCondition> },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompareOp {
    Eq,
    Ne,
    Lt,
    Lte,
    Gt,
    Gte,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProtocolAction {
    /// Park the live triggers that read an infra copy the protocol's
    /// condition found broken (see [`broken_copies`]); the same for the
    /// two below. A trigger that reads only healthy infra keeps running.
    ParkTriggers,
    HibernateTriggers { grace_minutes: u32 },
    WipeTriggers,
    /// Bring back what the health loop took down, copy by copy: the
    /// protocol's condition is read on each copy's own units, the copies
    /// it fails for are still broken (see [`still_broken_copies`]), and
    /// every trigger that reads none of them comes back. One copy still
    /// broken holds back only its own readers.
    AutoRecover,
    Notify { channel: String },
    /// Scale `unit` of the copies of `node_id` owned by whoever owns a
    /// copy the condition finds broken: the shared copy for a broken
    /// shared one, ada's for a broken copy of ada's.
    Scale {
        node_id: String,
        unit: String,
        replicas: u32,
    },
    /// Delete the Pods of `unit` in the same copies `Scale` reaches.
    BouncePods {
        node_id: String,
        unit: String,
    },
}

impl ProtocolAction {
    /// Whether this action acts on the copies its condition finds broken
    /// ([`broken_copies`]): it takes their readers down, or scales or
    /// bounces their owners' copies. When its condition names no infra
    /// ([`names_infra`]), see [`ProtocolAction::unowned_scope`].
    pub fn aims_at_broken_copies(&self) -> bool {
        matches!(
            self,
            Self::ParkTriggers
                | Self::HibernateTriggers { .. }
                | Self::WipeTriggers
                | Self::Scale { .. }
                | Self::BouncePods { .. }
        )
    }

    /// Where an action on broken copies lands when its condition names
    /// no infra, so nothing says which copies are broken: a scale or a
    /// bounce on the named node's shared copy, a take-down on the whole
    /// project (the empty set).
    pub fn unowned_scope(&self) -> BTreeSet<InfraCopy> {
        match self {
            Self::Scale { node_id, .. } | Self::BouncePods { node_id, .. } => {
                BTreeSet::from([InfraCopy { node_id: node_id.clone(), member: None }])
            }
            _ => BTreeSet::new(),
        }
    }
}

/// Whether `cond` holds a condition on units in a positive place (not
/// under a `not`): the leaves [`broken_copies`] reads. A protocol acting
/// on broken copies whose condition names none acts where it did before
/// copies had owners: a take-down on the whole project, a scale or a
/// bounce on the named node's shared copy.
pub(crate) fn names_infra(cond: &HealthCondition) -> bool {
    match cond {
        HealthCondition::NodeFlaky { .. }
        | HealthCondition::NodeReadyRatioBelow { .. }
        | HealthCondition::NodeReadyReplicas { .. } => true,
        HealthCondition::All { conds } | HealthCondition::Any { conds } => conds.iter().any(names_infra),
        HealthCondition::Not { .. } | HealthCondition::ProjectStatusEq { .. } | HealthCondition::HealthParked => false,
    }
}

/// Default protocol set if the project hasn't configured anything.
///
/// Two-stage AutoRecover. The expressiveness comes from the condition
/// language (`ProjectStatusEq`, `HealthParked`); the supervisor's handler
/// is dumb.
///
/// 1. **Park what reads broken infra.** While the project listens AND
///    some unit is declared flaky (not ready for its whole flaky window
///    after having been ready), park the triggers that read the broken
///    copies. New fires queue against parked signals; running executions
///    drain. Triggers reading only healthy infra keep running.
///
/// 2. **Reactivate on recovery.** While something the health loop
///    parked is still down, reactivate what it parked that reads only
///    copies with no unit flaky any more. Read copy by copy: one copy
///    still flaky keeps only its own readers parked.
///
/// Users can override the whole thing with their own
/// `HealthProtocols` in `project.health_protocols_json`.
pub fn default_protocols() -> HealthProtocols {
    use weft_broker_client::protocol::ProjectStatus;
    // Any unit of any node latched flaky. Per-unit: one broken unit is
    // enough to consider its copy broken.
    let any_unit_flaky = HealthCondition::NodeFlaky { node_id: "*".into(), unit: "*".into() };
    HealthProtocols {
        protocols: vec![
            HealthProtocol {
                name: "park-while-infra-broken".into(),
                when: HealthCondition::All {
                    conds: vec![
                        any_unit_flaky.clone(),
                        HealthCondition::ProjectStatusEq {
                            status: ProjectStatus::Active,
                        },
                    ],
                },
                action: ProtocolAction::ParkTriggers,
                timeout_seconds: 1800,
            },
            HealthProtocol {
                name: "auto-recover-when-infra-healthy".into(),
                when: HealthCondition::All {
                    conds: vec![
                        HealthCondition::Not {
                            cond: vec![any_unit_flaky],
                        },
                        // ONLY reactivate what the health loop itself
                        // parked. A user stop / deactivate clears the
                        // mark, so this protocol never overrides the
                        // user's explicit deactivation.
                        HealthCondition::HealthParked,
                    ],
                },
                action: ProtocolAction::AutoRecover,
                timeout_seconds: 1800,
            },
        ],
    }
}

/// One unit of one deployed copy, as this look saw it. Only units whose
/// workload the loop has seen are here (see [`HealthCondition`]), and
/// only units expected to run now (running or flaky).
#[derive(Debug, Clone, PartialEq)]
pub struct UnitView {
    pub node_id: String,
    /// Whose copy: `None` for the shared one.
    pub member: Option<weft_core::member::MemberId>,
    pub unit: String,
    /// Ready over desired, clamped to [0, 1]; 1.0 when none is desired.
    pub ready_ratio: f32,
    pub ready: u32,
    /// The unit's latched health (`evaluate_node_health`).
    pub flaky: bool,
}

impl UnitView {
    pub(crate) fn copy(&self) -> InfraCopy {
        InfraCopy { node_id: self.node_id.clone(), member: self.member.clone() }
    }

    /// Whether this unit satisfies the unit condition `cond`; `None`
    /// when `cond` is not a condition on units.
    fn satisfies(&self, cond: &HealthCondition) -> Option<bool> {
        let (node_id, unit) = match cond {
            HealthCondition::NodeFlaky { node_id, unit }
            | HealthCondition::NodeReadyRatioBelow { node_id, unit, .. }
            | HealthCondition::NodeReadyReplicas { node_id, unit, .. } => (node_id, unit),
            _ => return None,
        };
        if !selector_matches(node_id, unit, &self.node_id, &self.unit) {
            return Some(false);
        }
        Some(match cond {
            HealthCondition::NodeFlaky { .. } => self.flaky,
            HealthCondition::NodeReadyRatioBelow { ratio, .. } => self.ready_ratio < *ratio,
            HealthCondition::NodeReadyReplicas { op, value, .. } => op.holds(self.ready, *value),
            _ => unreachable!("matched a condition on units above"),
        })
    }
}

impl CompareOp {
    fn holds(self, n: u32, value: u32) -> bool {
        match self {
            CompareOp::Eq => n == value,
            CompareOp::Ne => n != value,
            CompareOp::Lt => n < value,
            CompareOp::Lte => n <= value,
            CompareOp::Gt => n > value,
            CompareOp::Gte => n >= value,
        }
    }
}

/// Everything `evaluate_condition` needs from one tick. Grouped
/// into a struct so adding new condition kinds doesn't blow up the
/// arg list at every call site.
#[derive(Debug, Clone)]
pub struct ConditionContext<'a> {
    /// Every seen unit of every copy expected to run now.
    pub units: &'a [UnitView],
    pub project_status: weft_broker_client::protocol::ProjectStatus,
    /// Is something the health loop took down still down? Feeds
    /// `HealthCondition::HealthParked`.
    pub health_parked: bool,
}

/// True if a `(node_id, unit)` selector pair (each possibly `"*"`)
/// matches an actual `(node, unit)` key.
fn selector_matches(sel_node: &str, sel_unit: &str, node: &str, unit: &str) -> bool {
    (sel_node == "*" || sel_node == node) && (sel_unit == "*" || sel_unit == unit)
}

pub fn evaluate_condition(cond: &HealthCondition, ctx: &ConditionContext<'_>) -> bool {
    match cond {
        // A condition on units holds when some seen unit satisfies it;
        // over no seen unit it is false (an unknown unit is neither
        // ready nor broken).
        HealthCondition::NodeFlaky { .. }
        | HealthCondition::NodeReadyRatioBelow { .. }
        | HealthCondition::NodeReadyReplicas { .. } => {
            ctx.units.iter().any(|u| u.satisfies(cond) == Some(true))
        }
        HealthCondition::ProjectStatusEq { status } => ctx.project_status == *status,
        HealthCondition::HealthParked => ctx.health_parked,
        HealthCondition::All { conds } => conds.iter().all(|c| evaluate_condition(c, ctx)),
        HealthCondition::Any { conds } => conds.iter().any(|c| evaluate_condition(c, ctx)),
        HealthCondition::Not { cond } => match cond.first() {
            Some(c) => !evaluate_condition(c, ctx),
            // Defensive: a malformed config with empty `Not.cond=[]`
            // shouldn't panic; "nothing to negate" evaluates false.
            None => false,
        },
    }
}

/// The copies `cond` does NOT hold for when read on each copy's own
/// units: what an auto-recover keeps the readers of down. A condition on
/// units reads only that copy's; a condition on the project (its status,
/// whether the health loop parked something) reads the same for every
/// copy. Only seen copies are candidates: a copy whose workload is
/// unknown is neither broken nor recovered, so it holds back nobody.
pub fn still_broken_copies(cond: &HealthCondition, ctx: &ConditionContext<'_>) -> BTreeSet<InfraCopy> {
    let copies: BTreeSet<InfraCopy> = ctx.units.iter().map(UnitView::copy).collect();
    copies
        .into_iter()
        .filter(|copy| {
            let own: Vec<UnitView> = ctx.units.iter().filter(|u| u.copy() == *copy).cloned().collect();
            !evaluate_condition(cond, &ConditionContext { units: &own, ..ctx.clone() })
        })
        .collect()
}

/// The copies `cond` finds broken: every copy with a unit that satisfies
/// one of its conditions on units in a positive place (not under a
/// `not`). What a take-down action aims at: the triggers reading these.
pub fn broken_copies(cond: &HealthCondition, ctx: &ConditionContext<'_>) -> BTreeSet<InfraCopy> {
    match cond {
        HealthCondition::NodeFlaky { .. }
        | HealthCondition::NodeReadyRatioBelow { .. }
        | HealthCondition::NodeReadyReplicas { .. } => ctx
            .units
            .iter()
            .filter(|u| u.satisfies(cond) == Some(true))
            .map(UnitView::copy)
            .collect(),
        HealthCondition::All { conds } | HealthCondition::Any { conds } => {
            conds.iter().flat_map(|c| broken_copies(c, ctx)).collect()
        }
        HealthCondition::Not { .. } | HealthCondition::ProjectStatusEq { .. } | HealthCondition::HealthParked => {
            BTreeSet::new()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use weft_broker_client::protocol::ProjectStatus;
    use weft_core::member::MemberId;

    /// One seen unit of the shared copy of `node` (unit named after it).
    fn unit(node: &str, ratio: f32, ready: u32, flaky: bool) -> UnitView {
        UnitView { node_id: node.into(), member: None, unit: node.into(), ready_ratio: ratio, ready, flaky }
    }

    fn ctx(units: &[UnitView]) -> ConditionContext<'_> {
        ConditionContext { units, project_status: ProjectStatus::Active, health_parked: false }
    }

    fn ev(cond: &HealthCondition, units: &[UnitView]) -> bool {
        evaluate_condition(cond, &ctx(units))
    }

    fn ratio_below(node: &str, ratio: f32) -> HealthCondition {
        HealthCondition::NodeReadyRatioBelow { node_id: node.into(), unit: "*".into(), ratio }
    }

    fn replicas(node: &str, op: CompareOp, value: u32) -> HealthCondition {
        HealthCondition::NodeReadyReplicas { node_id: node.into(), unit: "*".into(), op, value }
    }

    fn flaky(node: &str) -> HealthCondition {
        HealthCondition::NodeFlaky { node_id: node.into(), unit: "*".into() }
    }

    // ---------- evaluate_condition: NodeReadyRatioBelow ----------

    #[test]
    fn ratio_below_strict_for_named_node() {
        assert!(ev(&ratio_below("n1", 1.0), &[unit("n1", 0.5, 1, false)]));
        assert!(!ev(&ratio_below("n1", 1.0), &[unit("n1", 1.0, 1, false)]), "at the threshold is not below");
    }

    #[test]
    fn ratio_wildcard_scans_all_nodes() {
        assert!(ev(&ratio_below("*", 1.0), &[unit("a", 1.0, 1, false), unit("b", 0.3, 0, false)]));
        assert!(!ev(&ratio_below("*", 1.0), &[unit("a", 1.0, 1, false), unit("b", 1.0, 1, false)]));
    }

    // ---------- evaluate_condition: NodeReadyReplicas ----------

    #[test]
    fn replicas_compare_ops() {
        let m = [unit("n", 1.0, 3, false)];
        assert!(ev(&replicas("n", CompareOp::Eq, 3), &m));
        assert!(!ev(&replicas("n", CompareOp::Eq, 2), &m));
        assert!(ev(&replicas("n", CompareOp::Ne, 2), &m));
        assert!(!ev(&replicas("n", CompareOp::Ne, 3), &m));
        assert!(ev(&replicas("n", CompareOp::Lt, 5), &m));
        assert!(!ev(&replicas("n", CompareOp::Lt, 3), &m));
        assert!(ev(&replicas("n", CompareOp::Lte, 3), &m));
        assert!(ev(&replicas("n", CompareOp::Gt, 1), &m));
        assert!(!ev(&replicas("n", CompareOp::Gt, 3), &m));
        assert!(ev(&replicas("n", CompareOp::Gte, 3), &m));
    }

    /// A unit whose workload has not been seen is not in the look at all,
    /// and no condition on units is true of it: not "zero ready", not
    /// "below ratio", not flaky, named or wildcard. This is what keeps a
    /// copy that was just applied, before its workload reached the
    /// watch, from reading as broken.
    #[test]
    fn an_unseen_unit_satisfies_no_condition_on_units() {
        for cond in [
            replicas("ghost", CompareOp::Eq, 0),
            replicas("*", CompareOp::Eq, 0),
            ratio_below("ghost", 1.0),
            ratio_below("*", 1.0),
            flaky("ghost"),
            flaky("*"),
        ] {
            assert!(!ev(&cond, &[]), "{cond:?} must be false over no seen unit");
            assert!(broken_copies(&cond, &ctx(&[])).is_empty());
        }
        // The default park reads nothing broken either.
        assert!(!ev(&default_protocols().protocols[0].when, &[]));
    }

    // ---------- evaluate_condition: NodeFlaky ----------

    /// `node_flaky` reads the latch, never the instantaneous count: a
    /// unit at zero ready that is not (yet) declared flaky does not
    /// satisfy it, and one declared flaky does even while momentarily
    /// ready inside its recovery window.
    #[test]
    fn flaky_reads_the_latch_not_the_reading() {
        assert!(!ev(&flaky("*"), &[unit("n", 0.0, 0, false)]));
        assert!(ev(&flaky("*"), &[unit("n", 1.0, 1, true)]));
        assert!(!ev(&flaky("other"), &[unit("n", 0.0, 0, true)]));
    }

    // ---------- broken_copies ----------

    /// A take-down aims at the copies whose units make a positive
    /// condition on units true: here ada's svc copy, not bob's healthy
    /// one, not the shared db; nothing under a `not`.
    #[test]
    fn broken_copies_name_whose_copy_is_broken() {
        let ada = MemberId::new("ada").unwrap();
        let units = [
            UnitView { member: Some(ada.clone()), ..unit("svc", 0.0, 0, true) },
            UnitView { member: Some(MemberId::new("bob").unwrap()), ..unit("svc", 1.0, 1, false) },
            unit("db", 1.0, 1, false),
        ];
        let park = &default_protocols().protocols[0].when;
        assert!(ev(park, &units));
        assert_eq!(
            broken_copies(park, &ctx(&units)),
            BTreeSet::from([InfraCopy { node_id: "svc".into(), member: Some(ada) }])
        );
        let negated = HealthCondition::Not { cond: vec![replicas("*", CompareOp::Eq, 1)] };
        assert!(broken_copies(&negated, &ctx(&units)).is_empty());
    }

    /// The default recovery reads each copy on its own units: bob's
    /// copy is still flaky, while ada's healed copy and the healthy shared
    /// one are not; with nothing parked every copy fails it.
    #[test]
    fn still_broken_copies_are_read_copy_by_copy() {
        let bob = MemberId::new("bob").unwrap();
        let units = [
            UnitView { member: Some(MemberId::new("ada").unwrap()), ..unit("svc", 1.0, 1, false) },
            UnitView { member: Some(bob.clone()), ..unit("svc", 0.0, 0, true) },
            unit("db", 1.0, 1, false),
        ];
        let recover = &default_protocols().protocols[1].when;
        let parked = ConditionContext { health_parked: true, ..ctx(&units) };
        assert!(!evaluate_condition(recover, &parked), "read on the whole project, bob's copy holds it back");
        assert_eq!(
            still_broken_copies(recover, &parked),
            BTreeSet::from([InfraCopy { node_id: "svc".into(), member: Some(bob) }])
        );
        assert_eq!(still_broken_copies(recover, &ctx(&units)).len(), 3);
    }

    // ---------- combinators ----------

    fn always_true() -> HealthCondition {
        HealthCondition::ProjectStatusEq { status: ProjectStatus::Active }
    }
    fn always_false() -> HealthCondition {
        HealthCondition::HealthParked
    }

    #[test]
    fn all_combinator_logic() {
        assert!(ev(&HealthCondition::All { conds: vec![always_true(), always_true()] }, &[]));
        assert!(!ev(&HealthCondition::All { conds: vec![always_true(), always_false()] }, &[]));
        // Empty All is vacuously true.
        assert!(ev(&HealthCondition::All { conds: vec![] }, &[]));
    }

    #[test]
    fn any_combinator_logic() {
        assert!(ev(&HealthCondition::Any { conds: vec![always_false(), always_true()] }, &[]));
        assert!(!ev(&HealthCondition::Any { conds: vec![always_false(), always_false()] }, &[]));
        // Empty Any is vacuously false.
        assert!(!ev(&HealthCondition::Any { conds: vec![] }, &[]));
    }

    #[test]
    fn not_combinator_inverts() {
        assert!(ev(&HealthCondition::Not { cond: vec![always_false()] }, &[]));
        assert!(!ev(&HealthCondition::Not { cond: vec![always_true()] }, &[]));
    }

    #[test]
    fn not_combinator_empty_is_false() {
        // Defensive: malformed `Not { cond: [] }` evaluates false.
        assert!(!ev(&HealthCondition::Not { cond: vec![] }, &[]));
    }

    #[test]
    fn combinators_nest_arbitrarily() {
        // NOT (a OR (b AND c))  where a=false, b=true, c=false
        // → NOT (false OR (true AND false)) = NOT false = true
        let cond = HealthCondition::Not {
            cond: vec![HealthCondition::Any {
                conds: vec![
                    always_false(),
                    HealthCondition::All {
                        conds: vec![always_true(), always_false()],
                    },
                ],
            }],
        };
        assert!(ev(&cond, &[]));
    }

    #[test]
    fn not_round_trips_via_json_string() {
        // Wire-shape test: a hand-authored `Not` survives the
        // serialize → deserialize path the broker uses.
        let original = HealthCondition::Not {
            cond: vec![HealthCondition::NodeReadyReplicas {
                node_id: "n1".into(),
                unit: "*".into(),
                op: CompareOp::Eq,
                value: 0,
            }],
        };
        let s = serde_json::to_string(&original).expect("serialize");
        let back: HealthCondition = serde_json::from_str(&s).expect("deserialize");
        // Equality via re-serialize: HealthCondition doesn't impl
        // PartialEq because some nested types don't.
        let s2 = serde_json::to_string(&back).expect("re-serialize");
        assert_eq!(s, s2);
    }

    // ---------- default_protocols ----------

    #[test]
    fn default_protocols_two_stage_auto_recover() {
        let p = default_protocols();
        assert_eq!(p.protocols.len(), 2);
        assert_eq!(p.protocols[0].name, "park-while-infra-broken");
        assert!(matches!(p.protocols[0].action, ProtocolAction::ParkTriggers));
        assert_eq!(p.protocols[1].name, "auto-recover-when-infra-healthy");
        assert!(matches!(p.protocols[1].action, ProtocolAction::AutoRecover));
    }

    #[test]
    fn default_protocols_round_trip_via_serde() {
        // The dispatcher stores protocols as JSON; make sure the
        // default set survives a round trip. We go through strings
        // rather than `serde_json::Value` to avoid the deep
        // monomorphization chain triggered by `Box<HealthCondition>`
        // round-tripping through `to_value`/`from_value` (serde issue
        // #2522: Box<T> in a recursive enum hits the trait recursion
        // limit during type-check, even when bumped).
        let p = default_protocols();
        let json = serde_json::to_string(&p).expect("serialize");
        let back: HealthProtocols = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back.protocols.len(), 2);
        assert_eq!(back.protocols[0].name, "park-while-infra-broken");
    }

    #[test]
    fn timeout_seconds_deserializes_from_snake_case() {
        // Wire-contract pin: the protocol JSON is snake_case
        // throughout (condition/action `kind`s are snake_case), so
        // `timeout_seconds` MUST deserialize from the snake_case key.
        // A camelCase rename here silently fell back to the default,
        // and worse, an Option default was unbounded, reopening the
        // action-hang wedge. This locks the snake_case key.
        let json = r#"{
            "name": "p",
            "when": { "kind": "node_ready_replicas", "node_id": "n", "op": "eq", "value": 0 },
            "action": { "kind": "bounce_pods", "node_id": "n", "unit": "u" },
            "timeout_seconds": 42
        }"#;
        let proto: HealthProtocol = serde_json::from_str(json).expect("deserialize");
        assert_eq!(proto.timeout_seconds, 42, "must parse from the snake_case key");
    }

    #[test]
    fn omitted_timeout_inherits_safe_default_never_unbounded() {
        // The wedge this guards: an author who OMITS timeout_seconds
        // must inherit the 30-min ceiling, NOT an unbounded action.
        // "Unbounded" is not expressible (the field is non-optional).
        let json = r#"{
            "name": "p",
            "when": { "kind": "node_ready_replicas", "node_id": "n", "op": "eq", "value": 0 },
            "action": { "kind": "bounce_pods", "node_id": "n", "unit": "u" }
        }"#;
        let proto: HealthProtocol = serde_json::from_str(json).expect("deserialize");
        assert_eq!(
            proto.timeout_seconds, 1800,
            "omitted timeout must inherit the safe default, never be unbounded"
        );
    }
}
