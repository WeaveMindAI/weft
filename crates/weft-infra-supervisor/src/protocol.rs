//! HealthProtocols data shape. The user configures this per-project;
//! the supervisor's health loop reads via the broker and evaluates
//! the rules on every tick.
//!
//! Mirror of the design doc section 7.5 (HealthCondition AST plus
//! ProtocolAction enum). Stored in `project.health_protocols_json`
//! as raw JSON; we deserialize on demand.

use serde::{Deserialize, Serialize};

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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum HealthCondition {
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
    ParkTriggers,
    HibernateTriggers { grace_minutes: u32 },
    WipeTriggers,
    AutoRecover,
    Notify { channel: String },
    Scale {
        node_id: String,
        unit: String,
        replicas: u32,
    },
    BouncePods {
        node_id: String,
        unit: String,
    },
}

/// Default protocol set if the project hasn't configured anything.
///
/// Two-stage AutoRecover. The expressiveness comes from the
/// `HealthCondition::ProjectStatusEq` clause that lets the
/// protocol's preconditions look at lifecycle state. The condition
/// language is the user-visible interface; the supervisor's
/// handler is dumb.
///
/// 1. **Park on infra degradation.** While Active AND some infra
///    node has zero ready replicas (for the flaky window), park
///    triggers. New fires queue against parked signals; running
///    executions either drain or get stuck on `endpoint_url` :
///    that's the design's intent (no execution proceeds against
///    broken infra).
///
/// 2. **Reactivate on infra recovery.** While Inactive AND every
///    infra node has at least one ready replica, reactivate.
///    Triggers come back online, parked fires drain.
///
/// Users can override the whole thing with their own
/// `HealthProtocols` in `project.health_protocols_json`.
pub fn default_protocols() -> HealthProtocols {
    use weft_broker_client::protocol::ProjectStatus;
    // Any unit of any node with zero ready replicas. Per-unit: one
    // broken unit is enough to consider the infra degraded.
    let any_unit_zero_ready = HealthCondition::NodeReadyReplicas {
        node_id: "*".into(),
        unit: "*".into(),
        op: CompareOp::Eq,
        value: 0,
    };
    HealthProtocols {
        protocols: vec![
            HealthProtocol {
                name: "park-while-infra-broken".into(),
                when: HealthCondition::All {
                    conds: vec![
                        any_unit_zero_ready.clone(),
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
                            cond: vec![any_unit_zero_ready],
                        },
                        HealthCondition::ProjectStatusEq {
                            status: ProjectStatus::Inactive,
                        },
                    ],
                },
                action: ProtocolAction::AutoRecover,
                timeout_seconds: 1800,
            },
        ],
    }
}

/// Everything `evaluate_condition` needs from one tick. Grouped
/// into a struct so adding new condition kinds doesn't blow up the
/// arg list at every call site.
#[derive(Debug, Clone)]
pub struct ConditionContext<'a> {
    /// Keyed by `(node_id, unit)`. Health is per-unit.
    pub ready_ratio: &'a std::collections::HashMap<(String, String), f32>,
    pub ready_replicas: &'a std::collections::HashMap<(String, String), u32>,
    pub project_status: weft_broker_client::protocol::ProjectStatus,
}

/// True if a `(node_id, unit)` selector pair (each possibly `"*"`)
/// matches an actual `(node, unit)` key.
fn selector_matches(sel_node: &str, sel_unit: &str, node: &str, unit: &str) -> bool {
    (sel_node == "*" || sel_node == node) && (sel_unit == "*" || sel_unit == unit)
}

pub fn evaluate_condition(cond: &HealthCondition, ctx: &ConditionContext<'_>) -> bool {
    match cond {
        HealthCondition::NodeReadyRatioBelow { node_id, unit, ratio } => {
            // Any matched (node, unit) below the ratio fires. An exact
            // selector with no matching key defaults to ready (1.0):
            // a not-yet-observed unit isn't "broken".
            let matched: Vec<f32> = ctx
                .ready_ratio
                .iter()
                .filter(|((n, u), _)| selector_matches(node_id, unit, n, u))
                .map(|(_, r)| *r)
                .collect();
            if matched.is_empty() {
                // A named node with no observed matching unit defaults
                // to ready (1.0): named-but-not-yet-observed isn't
                // "below ratio". A `*` node wildcard over zero units
                // stays false. (Mirrors the old node-level default.)
                node_id != "*" && 1.0 < *ratio
            } else {
                matched.iter().any(|r| *r < *ratio)
            }
        }
        HealthCondition::NodeReadyReplicas { node_id, unit, op, value } => {
            let cmp = |n: u32| match op {
                CompareOp::Eq => n == *value,
                CompareOp::Ne => n != *value,
                CompareOp::Lt => n < *value,
                CompareOp::Lte => n <= *value,
                CompareOp::Gt => n > *value,
                CompareOp::Gte => n >= *value,
            };
            let matched: Vec<u32> = ctx
                .ready_replicas
                .iter()
                .filter(|((n, u), _)| selector_matches(node_id, unit, n, u))
                .map(|(_, r)| *r)
                .collect();
            if matched.is_empty() {
                // A NAMED node with no observed matching unit is
                // treated as 0 ready (named-and-absent = down; matches
                // the old node-level `unwrap_or(0)`). A pure `*` node
                // wildcard over zero units stays false (nothing to
                // match), like the old `values().any()`.
                node_id != "*" && cmp(0)
            } else {
                matched.iter().copied().any(cmp)
            }
        }
        HealthCondition::ProjectStatusEq { status } => ctx.project_status == *status,
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// Test helper: takes node-keyed maps (one unit per node, unit
    /// name = node id) and wraps them into the per-(node,unit)
    /// `ConditionContext` so the existing single-node tests stay
    /// readable. Health is per-unit, but a one-unit-per-node fixture
    /// exercises the same logic.
    fn ev(
        cond: &HealthCondition,
        ready_ratio: &HashMap<String, f32>,
        ready_replicas: &HashMap<String, u32>,
    ) -> bool {
        let rr: HashMap<(String, String), f32> = ready_ratio
            .iter()
            .map(|(n, v)| ((n.clone(), n.clone()), *v))
            .collect();
        let rp: HashMap<(String, String), u32> = ready_replicas
            .iter()
            .map(|(n, v)| ((n.clone(), n.clone()), *v))
            .collect();
        evaluate_condition(
            cond,
            &ConditionContext {
                ready_ratio: &rr,
                ready_replicas: &rp,
                project_status: weft_broker_client::protocol::ProjectStatus::Active,
            },
        )
    }


    // ---------- evaluate_condition: NodeReadyRatioBelow ----------

    #[test]
    fn ratio_below_strict_for_named_node() {
        let mut r = HashMap::new();
        r.insert("n1".to_string(), 0.5);
        let c = HealthCondition::NodeReadyRatioBelow {
            node_id: "n1".into(),
            unit: "*".into(),
            ratio: 1.0,
        };
        assert!(ev(&c, &r, &HashMap::new()));
    }

    #[test]
    fn ratio_at_threshold_does_not_trigger() {
        let mut r = HashMap::new();
        r.insert("n1".to_string(), 1.0);
        let c = HealthCondition::NodeReadyRatioBelow {
            node_id: "n1".into(),
            unit: "*".into(),
            ratio: 1.0,
        };
        assert!(!ev(&c, &r, &HashMap::new()));
    }

    #[test]
    fn ratio_missing_node_defaults_to_1_safe() {
        // A node we have no data on shouldn't trigger flaky detection.
        let c = HealthCondition::NodeReadyRatioBelow {
            node_id: "ghost".into(),
            unit: "*".into(),
            ratio: 1.0,
        };
        assert!(!ev(&c, &HashMap::new(), &HashMap::new()));
    }

    #[test]
    fn ratio_wildcard_scans_all_nodes() {
        let mut r = HashMap::new();
        r.insert("a".to_string(), 1.0);
        r.insert("b".to_string(), 0.3);
        let c = HealthCondition::NodeReadyRatioBelow {
            node_id: "*".into(),
            unit: "*".into(),
            ratio: 1.0,
        };
        assert!(ev(&c, &r, &HashMap::new()));
    }

    #[test]
    fn ratio_wildcard_all_healthy_does_not_trigger() {
        let mut r = HashMap::new();
        r.insert("a".to_string(), 1.0);
        r.insert("b".to_string(), 1.0);
        let c = HealthCondition::NodeReadyRatioBelow {
            node_id: "*".into(),
            unit: "*".into(),
            ratio: 1.0,
        };
        assert!(!ev(&c, &r, &HashMap::new()));
    }

    #[test]
    fn ratio_wildcard_empty_map_does_not_trigger() {
        // No data at all: nothing to flag.
        let c = HealthCondition::NodeReadyRatioBelow {
            node_id: "*".into(),
            unit: "*".into(),
            ratio: 1.0,
        };
        assert!(!ev(&c, &HashMap::new(), &HashMap::new()));
    }

    // ---------- evaluate_condition: NodeReadyReplicas ----------

    fn reps(node: &str, n: u32) -> HashMap<String, u32> {
        let mut m = HashMap::new();
        m.insert(node.to_string(), n);
        m
    }

    #[test]
    fn replicas_eq() {
        let m = reps("n1", 3);
        assert!(ev(
            &HealthCondition::NodeReadyReplicas {
                node_id: "n1".into(),
                unit: "*".into(),
                op: CompareOp::Eq,
                value: 3,
            },
            &HashMap::new(),
            &m,
        ));
        assert!(!ev(
            &HealthCondition::NodeReadyReplicas {
                node_id: "n1".into(),
                unit: "*".into(),
                op: CompareOp::Eq,
                value: 2,
            },
            &HashMap::new(),
            &m,
        ));
    }

    #[test]
    fn replicas_ne_lt_lte_gt_gte() {
        let m = reps("n", 3);
        let case = |op: CompareOp, v: u32| {
            ev(
                &HealthCondition::NodeReadyReplicas {
                    node_id: "n".into(),
                    unit: "*".into(),
                    op,
                    value: v,
                },
                &HashMap::new(),
                &m,
            )
        };
        assert!(case(CompareOp::Ne, 2));
        assert!(!case(CompareOp::Ne, 3));
        assert!(case(CompareOp::Lt, 5));
        assert!(!case(CompareOp::Lt, 3));
        assert!(case(CompareOp::Lte, 3));
        assert!(case(CompareOp::Gt, 1));
        assert!(!case(CompareOp::Gt, 3));
        assert!(case(CompareOp::Gte, 3));
    }

    #[test]
    fn replicas_missing_defaults_to_zero() {
        // Missing → defaults to 0 in the implementation; verify the
        // contract so tests catch a future drift.
        assert!(ev(
            &HealthCondition::NodeReadyReplicas {
                node_id: "ghost".into(),
                unit: "*".into(),
                op: CompareOp::Eq,
                value: 0,
            },
            &HashMap::new(),
            &HashMap::new(),
        ));
    }

    // ---------- combinators ----------

    fn always_true() -> HealthCondition {
        HealthCondition::NodeReadyReplicas {
            node_id: "x".into(),
            unit: "*".into(),
            op: CompareOp::Eq,
            value: 0,
        }
    }
    fn always_false() -> HealthCondition {
        HealthCondition::NodeReadyReplicas {
            node_id: "x".into(),
            unit: "*".into(),
            op: CompareOp::Ne,
            value: 0,
        }
    }

    #[test]
    fn all_combinator_logic() {
        assert!(ev(
            &HealthCondition::All { conds: vec![always_true(), always_true()] },
            &HashMap::new(),
            &HashMap::new(),
        ));
        assert!(!ev(
            &HealthCondition::All { conds: vec![always_true(), always_false()] },
            &HashMap::new(),
            &HashMap::new(),
        ));
        // Empty All is vacuously true.
        assert!(ev(
            &HealthCondition::All { conds: vec![] },
            &HashMap::new(),
            &HashMap::new(),
        ));
    }

    #[test]
    fn any_combinator_logic() {
        assert!(ev(
            &HealthCondition::Any { conds: vec![always_false(), always_true()] },
            &HashMap::new(),
            &HashMap::new(),
        ));
        assert!(!ev(
            &HealthCondition::Any { conds: vec![always_false(), always_false()] },
            &HashMap::new(),
            &HashMap::new(),
        ));
        // Empty Any is vacuously false.
        assert!(!ev(
            &HealthCondition::Any { conds: vec![] },
            &HashMap::new(),
            &HashMap::new(),
        ));
    }

    #[test]
    fn not_combinator_inverts() {
        assert!(ev(
            &HealthCondition::Not { cond: vec![always_false()] },
            &HashMap::new(),
            &HashMap::new(),
        ));
        assert!(!ev(
            &HealthCondition::Not { cond: vec![always_true()] },
            &HashMap::new(),
            &HashMap::new(),
        ));
    }

    #[test]
    fn not_combinator_empty_is_false() {
        // Defensive: malformed `Not { cond: [] }` evaluates false.
        assert!(!ev(
            &HealthCondition::Not { cond: vec![] },
            &HashMap::new(),
            &HashMap::new(),
        ));
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
        assert!(ev(&cond, &HashMap::new(), &HashMap::new()));
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
