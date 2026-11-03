//! Skip decision: given a ReadyGroup (every wired port has an
//! arrival), decide whether to run the body or short-circuit to a
//! Skipped lifecycle event, and say WHY.
//!
//! Most skips are about CLOSURES (the structural "nothing's coming"
//! marker), never about user-emitted null values:
//!
//!   - Any REQUIRED port arrived as a closure -> skip. Required means
//!     the body declared "I cannot run without this", and the engine
//!     has proof nothing will ever arrive.
//!   - Every port of a `@require_one_of` group arrived as a closure.
//!   - A join (`features.join`) none of whose branches can deliver
//!     (each arrived closed, or has neither a wire nor a value), so
//!     there is nothing to pass on.
//!
//! An OPTIONAL port takes absence and failure alike: optional means "I
//! run without it", whatever the reason it is missing. So a node whose
//! every input closed still runs, unless it is required to wait on one
//! of them; when that is its meaning, the gate says so. The compiler
//! refuses a node whose wired inputs are all optional and whose gate is
//! not wired (when it runs would be undecided), unless something else
//! decides it: a `@require_one_of`, being a join, or being fed by a
//! group port that is there whenever the group runs (a gated group's
//! port, a loop's item or index). The compiler's own nodes (group
//! boundaries, whose ports are all optional) and triggers are not asked.
//!
//! A user-emitted `null` on a required port is NOT a skip. Null is
//! data the body has to interpret; the body runs.
//!
//! The ONE exception, and the only place a VALUE decides: the
//! `_should_flow` port. It is the author saying whether this node runs
//! at all, so `false` on it skips the node the same way a closure
//! does. Nothing else reads a value here.
//!
//! `Generator[T]` ports are exempt from every closure rule: a closure
//! on a generator port is the stream's END, and an end with no items
//! before it is the EMPTY STREAM, a value the body acts on (the
//! documented pull contract answers `Ok(None)` immediately). Skipping
//! the consumer there would make "zero items" behave differently from
//! "one item", and the body's post-loop code would never run.

use std::collections::HashSet;

use crate::project::NodeDefinition;
use crate::pulse::{Failure, Pulse};

/// Why a firing did not run. Carried on the Skipped lifecycle event so
/// the inspector can tell a DECISION (the author's `_should_flow` said
/// no) from a CONSEQUENCE (an input it needed never arrived).
// SYNC: SkipReason <-> packages/weft-graph/src/protocol.ts SkipReason
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SkipReason {
    /// `_should_flow` arrived `false`: this node was told not to run.
    DidNotFlow,
    /// `_should_flow` closed: whatever decides whether this node runs
    /// never said yes. `failure` when it never said yes because it
    /// broke (the closure carried the node that failed and its error).
    FlowClosed {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        failure: Option<Failure>,
    },
    /// `_should_not_flow` got a value: this node runs when the thing it
    /// watches did NOT happen, and it did.
    DidFlow,
    /// `_should_not_flow` closed because the node it watches did not
    /// FINISH (it failed, or the run was cancelled under it), not
    /// because the thing did not happen. A database that errored is not
    /// an empty table, so the branch written for "nothing there" stays
    /// off and the failure is what the run reports. `failure` is the
    /// closure's own: the node that broke (or was cancelled) and why.
    WatchedNodeFailed { failure: Failure },
    /// A required input arrived closed, so nothing can drive the body.
    /// `failure` when it closed because its producer broke.
    RequiredInputClosed {
        port: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        failure: Option<Failure>,
    },
    /// A join (`features.join`) whose every branch closed: none of them
    /// delivered, so there is nothing to pass on. `failure` when one of
    /// them closed because its producer broke.
    NothingJoined {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        failure: Option<Failure>,
    },
    /// A group with no gate whose every wire coming in closed with
    /// nothing on it: nothing reached it, so it does not run. A scope's
    /// reason only; the nodes inside it carry [`Self::ScopeSkipped`].
    /// `failure` when one of those wires closed because its producer broke.
    EveryInputClosed {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        failure: Option<Failure>,
    },
    /// Every port of a `@require_one_of` group arrived closed.
    /// `failure` when one of them closed because its producer broke.
    OneOfGroupClosed {
        ports: Vec<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        failure: Option<Failure>,
    },
    /// The scope this node lives in (a group, a call site, an included
    /// body or a loop) did not run: its gate said no, every input of a
    /// group closed with nothing on it, or a loop's list never came.
    /// Every node inside a scope that did not run carries this, however
    /// deep, naming the outermost one that did not.
    ScopeSkipped { scope: String },
    /// A person skipped what the node was waiting for (a form's skip):
    /// it ended there, with nothing to send on.
    WaitSkipped,
}

impl SkipReason {
    /// The failure this skip passes on, when it is one. A node that
    /// skipped because something before it BROKE did not decline: its
    /// own closures go out with that error, so a `_should_not_flow`
    /// further down still reads a failure and not an absence, however
    /// many skips sit between it and the node that failed. A decision
    /// (`_should_flow` said no, `_should_not_flow` saw a value) passes
    /// nothing on. A scope member's `ScopeSkipped` emits no closures of
    /// its own (the scope's teardown closed its exits, with the scope's
    /// failure), so it carries none.
    pub fn inherited_failure(&self) -> Option<&Failure> {
        match self {
            Self::DidNotFlow | Self::DidFlow | Self::ScopeSkipped { .. } | Self::WaitSkipped => None,
            Self::WatchedNodeFailed { failure } => Some(failure),
            Self::FlowClosed { failure }
            | Self::RequiredInputClosed { failure, .. }
            | Self::NothingJoined { failure }
            | Self::EveryInputClosed { failure }
            | Self::OneOfGroupClosed { failure, .. } => failure.as_ref(),
        }
    }
}

/// The tail a reason's text grows when the closure it names carried a
/// failure. One spelling for every reason.
fn after_failure(failure: &Option<Failure>) -> String {
    match failure {
        Some(failure) => format!(" because {failure}"),
        None => String::new(),
    }
}

// SYNC: Display for SkipReason <-> packages/weft-graph/src/webview/lib/utils/status.ts skipReasonText
impl std::fmt::Display for SkipReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DidNotFlow => write!(f, "its `_should_flow` said no"),
            Self::FlowClosed { failure } => {
                write!(f, "nothing ever answered its `_should_flow`{}", after_failure(failure))
            }
            Self::DidFlow => write!(f, "its `_should_not_flow` saw a value"),
            Self::WatchedNodeFailed { failure } => write!(
                f,
                "what its `_should_not_flow` watches did not finish ({failure}), which is \
                 not the absence this node runs on"
            ),
            Self::RequiredInputClosed { port, failure } => {
                write!(f, "the required input '{port}' closed{}", after_failure(failure))
            }
            Self::NothingJoined { failure } => {
                write!(f, "none of the branches it joins delivered{}", after_failure(failure))
            }
            Self::EveryInputClosed { failure } => {
                write!(f, "every wire into it closed with nothing on it{}", after_failure(failure))
            }
            Self::OneOfGroupClosed { ports, failure } => {
                write!(f, "every input of the group ({}) closed{}", ports.join(", "), after_failure(failure))
            }
            Self::ScopeSkipped { scope } => {
                write!(f, "the scope '{scope}' it lives in did not run")
            }
            Self::WaitSkipped => write!(f, "a person skipped what it was waiting for"),
        }
    }
}

/// The port every node carries, deciding whether it runs at all.
/// Unwired it is the literal `true`; wired, a `false` value or a
/// closure skips the node.
// SYNC: SHOULD_FLOW_PORT <-> packages/weft-graph/src/protocol.ts SHOULD_FLOW_PORT,
// docs/src/language/syntax.md (reserved keys),
// packages/weft-syntax/weft.tmLanguage.json (reserved-key rule; see its README)
pub const SHOULD_FLOW_PORT: &str = "_should_flow";

/// The same gate, read the other way round: this node runs when the
/// thing wired here did NOT happen.
///
/// Why the language needs a second spelling at all. Acting on a value
/// that arrived is free: wire it into `_should_flow`. Acting on one
/// that did NOT is the shape a required input cannot express: a node
/// SKIPS when a required input closes, so it is never alive to notice
/// the absence, and a node taking the value as optional runs either way
/// without saying which happened. A node that decides can announce its
/// own "no" on a second port, which covers a decision; it cannot cover
/// data that simply never came (a key missing from a request body).
/// This is the one port that fires ON a closure, which is what makes
/// "run exactly when this did not come" writable.
///
/// A node has ONE gate: wiring both spellings is refused by the
/// compiler, not silently combined.
// SYNC: SHOULD_NOT_FLOW_PORT <-> packages/weft-graph/src/protocol.ts SHOULD_NOT_FLOW_PORT,
// docs/src/language/syntax.md (reserved keys),
// packages/weft-syntax/weft.tmLanguage.json (reserved-key rule; see its README)
pub const SHOULD_NOT_FLOW_PORT: &str = "_should_not_flow";

/// Is this port name the language's gate, either way round? Everything
/// that treats the gate as "not the node's own data" (the input bag,
/// the readiness walk, the editor's port list) asks this rather than
/// comparing one name, so the inverted spelling is never mistaken for a
/// port the node declared.
pub fn is_gate_port(name: &str) -> bool {
    name == SHOULD_FLOW_PORT || name == SHOULD_NOT_FLOW_PORT
}

/// Rule 0 of the skip decision: did `_should_flow` say no? The only
/// rule that reads a VALUE, because this port IS the author's decision
/// rather than data: a `false` means "do not run this", written on a
/// wire or straight into the braces. A closure means whatever computes
/// the permission never spoke, which is a different fact and gets its
/// own reason.
///
/// Split out of `check_should_skip` because it applies to EVERY node,
/// including entry nodes with no incoming edges (which the closure
/// rules never see: with nothing wired there is nothing to close, but
/// a `_should_flow: false` literal still turns the node off).
pub fn check_flow_permission(node: &NodeDefinition, group_pulses: &[&Pulse]) -> Option<SkipReason> {
    flow_verdict(group_pulses, &node.port_literals)
}

/// The gate's verdict from what reached it (`group_pulses`, the pulses at
/// one firing or one scope's copy) and what is written on it
/// (`literals`): `Some` with why it says no, `None` for go. THE one
/// reading of a gate, for a node's own and for a group's alike (a group
/// is gated by the wires that land on it and the literal in its braces).
pub fn flow_verdict(group_pulses: &[&Pulse], literals: &std::collections::BTreeMap<String, serde_json::Value>) -> Option<SkipReason> {
    // The plain gate first. A node carries one or the other, never both
    // (the compiler refuses that), so whichever one speaks decides.
    let plain = match super::ready::resolve_port_value(group_pulses, SHOULD_FLOW_PORT) {
        Some(pulse) if pulse.closed => {
            Some(Some(SkipReason::FlowClosed { failure: pulse.failure.clone() }))
        }
        Some(pulse) if *pulse.value == serde_json::Value::Bool(false) => {
            Some(Some(SkipReason::DidNotFlow))
        }
        // A value arrived and it is not `false`: this node flows.
        Some(_) => Some(None),
        // Nothing on the wire: a literal in the braces still decides.
        None => match literals.get(SHOULD_FLOW_PORT) {
            Some(serde_json::Value::Bool(false)) => Some(Some(SkipReason::DidNotFlow)),
            Some(_) => Some(None),
            None => None,
        },
    };
    if let Some(verdict) = plain {
        return verdict;
    }

    // The inverted gate, every answer the other way round. A CLOSURE is
    // the yes here: the thing never happened, which is exactly what this
    // node was written to act on. This is the only place in the language
    // where a closure starts a node instead of stopping one.
    //
    // Except a closure that carries an error. The watched node did not
    // decline to emit; it broke, and "broke" is not "nothing there". A
    // query that errored would otherwise render as an empty result (an
    // API route answering 404 over a database that is down). The wire
    // already tells the two apart (`failure`), so the gate does.
    match super::ready::resolve_port_value(group_pulses, SHOULD_NOT_FLOW_PORT) {
        Some(pulse) if pulse.closed => pulse
            .failure
            .as_ref()
            .map(|failure| SkipReason::WatchedNodeFailed { failure: failure.clone() }),
        Some(pulse) if *pulse.value == serde_json::Value::Bool(false) => None,
        // Something real arrived, so the absence this node waits for did
        // not happen.
        Some(_) => Some(SkipReason::DidFlow),
        None => match literals.get(SHOULD_NOT_FLOW_PORT) {
            Some(serde_json::Value::Bool(false)) => None,
            // `_should_not_flow: true` written into the braces is the
            // literal way to say "never run this", the mirror of
            // `_should_flow: false`.
            Some(_) => Some(SkipReason::DidFlow),
            None => None,
        },
    }
}

#[allow(clippy::too_many_arguments)]
/// Whether `node` skips this firing, and why. `filled` holds the ports
/// that have a value without a wire delivering it (`ready::filled_ports`:
/// its literals and the values a carved run hands in).
pub fn check_should_skip(
    node: &NodeDefinition,
    group_pulses: &[&Pulse],
    required: &HashSet<&str>,
    wired: &HashSet<&str>,
    filled: &HashSet<&str>,
) -> Option<SkipReason> {
    // Rule 0: `_should_flow` said no. The port is optional, so neither
    // of its skip reasons reaches rule 1.
    if let Some(reason) = check_flow_permission(node, group_pulses) {
        return Some(reason);
    }

    // Rule 1: any wired required port that arrived as a closure -> skip.
    // `required` is built by `ready::required_inputs`, which counts an
    // input that is not `optional` as required exactly where it is wired.
    // (A value a carved run hands in for a wired port replaces that
    // port's closure among `group_pulses` already, and a literal only
    // fills an unwired port, so `filled` needs no check here.)
    // Generator ports are exempt: their closure is the empty stream,
    // not "nothing is coming" (module doc).
    for port_name in required {
        if !wired.contains(port_name) || super::ready::is_generator_input(node, port_name) {
            continue;
        }
        if let Some(failure) = port_closure(group_pulses, port_name) {
            return Some(SkipReason::RequiredInputClosed {
                port: (*port_name).to_string(),
                failure: failure.cloned(),
            });
        }
    }

    // A join runs on the first of its branches that delivers. When none
    // can (each closed, or has no wire and no value to deliver), there is
    // nothing to run, and the skip closes its output, carrying the
    // failure of a branch that broke so a `_should_not_flow` further on
    // reads a failure rather than an absence. A branch in `filled` (a
    // literal, or a value a carved run hands in) always delivers.
    if node.features.join {
        let branches: Vec<&str> = node
            .inputs
            .iter()
            .map(|p| p.name.as_str())
            .filter(|p| !is_gate_port(p))
            .collect();
        let nothing = !branches.is_empty()
            && branches.iter().all(|p| {
                !filled.contains(p) && (!wired.contains(p) || port_arrived_closed(group_pulses, p))
            });
        if nothing {
            let failure = first_failure(group_pulses, branches.into_iter());
            return Some(SkipReason::NothingJoined { failure });
        }
    }

    // oneOfRequired groups: skip when EVERY port in a group is closed.
    // A blank text where a blank is no value (`ready::blank_is_absent`)
    // is not given either: a literal one is never in `filled`, and a
    // wired one counts as closed.
    for group in &node.features.one_of_required {
        if group.is_empty() {
            continue;
        }
        let all_closed = group.iter().all(|port_name| {
            if filled.contains(port_name.as_str()) {
                return false;
            }
            if !wired.contains(port_name.as_str()) {
                return true;
            }
            // A generator port's closure is the empty stream, a live
            // value; it keeps its oneOf group satisfied.
            if super::ready::is_generator_input(node, port_name) {
                return false;
            }
            port_arrived_closed(group_pulses, port_name) || arrived_blank(node, group_pulses, port_name)
        });
        if all_closed {
            let failure = first_failure(group_pulses, group.iter().map(String::as_str));
            return Some(SkipReason::OneOfGroupClosed { ports: group.clone(), failure });
        }
    }

    None
}

/// Whether what arrived on `port_name` is a blank text the input counts
/// as no value ([`super::ready::blank_is_absent`]).
fn arrived_blank(node: &NodeDefinition, group_pulses: &[&Pulse], port_name: &str) -> bool {
    super::ready::resolve_port_value(group_pulses, port_name).is_some_and(|pulse| {
        node.inputs.iter().find(|p| p.name == port_name).is_some_and(|input| super::ready::blank_is_absent(input, &pulse.value))
    })
}

fn port_arrived_closed(group_pulses: &[&Pulse], port_name: &str) -> bool {
    port_closure(group_pulses, port_name).is_some()
}

/// `Some` when the port arrived closed, holding the error the closure
/// carried when its producer failed rather than declined.
fn port_closure<'a>(group_pulses: &[&'a Pulse], port_name: &str) -> Option<Option<&'a Failure>> {
    super::ready::resolve_port_value(group_pulses, port_name)
        .filter(|p| p.closed)
        .map(|p| p.failure.as_ref())
}

/// Among `ports` (in the node's own order), the failure of the first
/// one whose closure carried one. The one failure a skip over several
/// closed ports passes on.
fn first_failure<'a>(group_pulses: &[&Pulse], ports: impl Iterator<Item = &'a str>) -> Option<Failure> {
    ports
        .filter_map(|port| port_closure(group_pulses, port).flatten())
        .next()
        .cloned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pulse::Pulse;
    use serde_json::json;

    fn node_one_required() -> crate::project::NodeDefinition {
        serde_json::from_value(json!({
            "id": "n",
            "nodeType": "X",
            "label": null,
            "config": null,
            "position": { "x": 0.0, "y": 0.0 },
            "inputs": [{ "name": "a", "portType": "Number", "required": true }],
            "outputs": [],
            "features": {},
            "scope": [],
            "groupBoundary": null,
            "requiresInfra": false,
            "images": []
        }))
        .expect("node json")
    }

    fn node_all_optional() -> crate::project::NodeDefinition {
        serde_json::from_value(json!({
            "id": "n",
            "nodeType": "X",
            "label": null,
            "config": null,
            "position": { "x": 0.0, "y": 0.0 },
            "inputs": [
                { "name": "a", "portType": "Number", "required": false },
                { "name": "b", "portType": "Number", "required": false }
            ],
            "outputs": [],
            "features": {},
            "scope": [],
            "groupBoundary": null,
            "requiresInfra": false,
            "images": []
        }))
        .expect("node json")
    }

    /// A node with a data input plus the language's own guard port, the
    /// shape enrich gives every catalog node.
    fn node_with_guard() -> crate::project::NodeDefinition {
        serde_json::from_value(json!({
            "id": "n",
            "nodeType": "X",
            "label": null,
            "config": null,
            "position": { "x": 0.0, "y": 0.0 },
            "inputs": [
                { "name": "a", "portType": "Number", "required": true },
                { "name": SHOULD_FLOW_PORT, "portType": "T", "required": false }
            ],
            "outputs": [],
            "features": {},
            "scope": [],
            "groupBoundary": null,
            "requiresInfra": false,
            "images": []
        }))
        .expect("node json")
    }

    /// The guard is the one port whose VALUE decides: `false` skips the
    /// node even though every input it needs arrived.
    #[test]
    fn a_false_guard_skips_a_node_whose_inputs_all_arrived() {
        let node = node_with_guard();
        let pulses = vec![data_pulse("a", json!(7)), data_pulse(SHOULD_FLOW_PORT, json!(false))];
        let required: HashSet<&str> = ["a"].into_iter().collect();
        let wired: HashSet<&str> = ["a", SHOULD_FLOW_PORT].into_iter().collect();
        assert_eq!(
            check_should_skip(&node, &view(&pulses), &required, &wired, &HashSet::new()),
            Some(SkipReason::DidNotFlow),
        );
    }

    /// A guard that never spoke is a different fact from one that said
    /// no, and the journal has to be able to tell them apart.
    #[test]
    fn a_closed_guard_skips_with_its_own_reason() {
        let node = node_with_guard();
        let pulses = vec![data_pulse("a", json!(7)), closure_pulse(SHOULD_FLOW_PORT)];
        let required: HashSet<&str> = ["a"].into_iter().collect();
        let wired: HashSet<&str> = ["a", SHOULD_FLOW_PORT].into_iter().collect();
        assert_eq!(
            check_should_skip(&node, &view(&pulses), &required, &wired, &HashSet::new()),
            Some(SkipReason::FlowClosed { failure: None }),
        );
    }

    /// Any other value is permission: only `false` says no, so a node
    /// guarded by something that emits a string still runs.
    #[test]
    fn a_non_false_guard_lets_the_node_run() {
        let node = node_with_guard();
        for value in [json!(true), json!("ready"), json!(0), json!(null)] {
            let pulses =
                vec![data_pulse("a", json!(7)), data_pulse(SHOULD_FLOW_PORT, value.clone())];
            let required: HashSet<&str> = ["a"].into_iter().collect();
            let wired: HashSet<&str> = ["a", SHOULD_FLOW_PORT].into_iter().collect();
            assert_eq!(
                check_should_skip(&node, &view(&pulses), &required, &wired, &HashSet::new()),
                None,
                "a guard carrying {value} is not a refusal",
            );
        }
    }

    /// `_should_flow: false` written straight into the braces turns a
    /// node off with no wire at all.
    #[test]
    fn a_false_guard_literal_skips_the_node() {
        let mut node = node_with_guard();
        node.port_literals.insert(SHOULD_FLOW_PORT.to_string(), json!(false));
        let pulses = vec![data_pulse("a", json!(7))];
        let required: HashSet<&str> = ["a"].into_iter().collect();
        let wired: HashSet<&str> = ["a"].into_iter().collect();
        assert_eq!(
            check_should_skip(&node, &view(&pulses), &required, &wired, &HashSet::new()),
            Some(SkipReason::DidNotFlow),
        );
    }

    /// With the gate open, a node whose every input is optional runs
    /// even when every one of them closed: optional means "I run
    /// without it". The gate is where the author said when it runs.
    #[test]
    fn an_open_guard_runs_a_node_whose_optional_inputs_all_closed() {
        let mut node = node_all_optional();
        node.inputs.push(
            serde_json::from_value(json!({
                "name": SHOULD_FLOW_PORT, "portType": "T", "required": false
            }))
            .expect("guard port"),
        );
        let pulses = vec![
            closure_pulse("a"),
            closure_pulse("b"),
            data_pulse(SHOULD_FLOW_PORT, json!(true)),
        ];
        let wired: HashSet<&str> = ["a", "b", SHOULD_FLOW_PORT].into_iter().collect();
        assert_eq!(
            check_should_skip(&node, &view(&pulses), &HashSet::new(), &wired, &HashSet::new()),
            None,
        );
    }

    /// An optional input takes a closure that carries a failure the same
    /// as one that carries none: it runs without the value either way.
    #[test]
    fn an_optional_input_takes_a_failed_closure_like_an_absence() {
        let node = node_all_optional();
        let failure = Failure { node: "up".into(), error: "the database is down".into() };
        let failed = Pulse::closure_with_failure(uuid::Uuid::new_v4(), uuid::Uuid::nil(), Vec::new(), "n", "a", Some(failure));
        let pulses = vec![failed, data_pulse("b", json!(1))];
        let wired: HashSet<&str> = ["a", "b"].into_iter().collect();
        assert_eq!(
            check_should_skip(&node, &view(&pulses), &HashSet::new(), &wired, &HashSet::new()),
            None,
        );
    }

    /// A required input that closed with a failure still skips the node,
    /// and the skip passes the failure on.
    #[test]
    fn a_required_input_closed_by_a_failure_skips_and_passes_it_on() {
        let node = node_one_required();
        let failure = Failure { node: "up".into(), error: "the database is down".into() };
        let failed = Pulse::closure_with_failure(uuid::Uuid::new_v4(), uuid::Uuid::nil(), Vec::new(), "n", "a", Some(failure.clone()));
        let pulses = vec![failed];
        let required: HashSet<&str> = ["a"].into_iter().collect();
        let wired: HashSet<&str> = ["a"].into_iter().collect();
        let reason = check_should_skip(&node, &view(&pulses), &required, &wired, &HashSet::new())
            .expect("a closed required input skips");
        assert_eq!(reason.inherited_failure(), Some(&failure));
    }

    fn join_of(branches: &[&str]) -> crate::project::NodeDefinition {
        let mut node = node_all_optional();
        node.inputs = branches
            .iter()
            .map(|name| serde_json::from_value(json!({ "name": name, "portType": "T", "required": false })).expect("port"))
            .collect();
        node.features.join = true;
        node
    }

    /// A join runs on the first branch that delivered: one value among
    /// closed branches is enough.
    #[test]
    fn a_join_with_one_branch_delivered_runs() {
        let node = join_of(&["a", "b"]);
        let pulses = vec![closure_pulse("a"), data_pulse("b", json!(1))];
        let wired: HashSet<&str> = ["a", "b"].into_iter().collect();
        assert_eq!(check_should_skip(&node, &view(&pulses), &HashSet::new(), &wired, &HashSet::new()), None);
    }

    /// Every branch closed: nothing joined, and a branch that broke hands
    /// its failure on.
    #[test]
    fn a_join_whose_every_branch_closed_skips_passing_a_failure_on() {
        let node = join_of(&["a", "b"]);
        let failure = Failure { node: "up".into(), error: "the database is down".into() };
        let pulses = vec![
            closure_pulse("a"),
            Pulse::closure_with_failure(uuid::Uuid::new_v4(), uuid::Uuid::nil(), Vec::new(), "n", "b", Some(failure.clone())),
        ];
        let wired: HashSet<&str> = ["a", "b"].into_iter().collect();
        assert_eq!(
            check_should_skip(&node, &view(&pulses), &HashSet::new(), &wired, &HashSet::new()),
            Some(SkipReason::NothingJoined { failure: Some(failure) }),
        );
        // A branch filled by a literal always delivers.
        let pulses = vec![closure_pulse("a")];
        let wired: HashSet<&str> = ["a"].into_iter().collect();
        let literal: HashSet<&str> = ["b"].into_iter().collect();
        assert_eq!(check_should_skip(&node, &view(&pulses), &HashSet::new(), &wired, &literal), None);
        // A branch with neither a wire nor a value delivers nothing
        // either: the broken one's failure still passes on.
        let failure_on_a = Failure { node: "up".into(), error: "the query failed".into() };
        let pulses = vec![Pulse::closure_with_failure(
            uuid::Uuid::new_v4(), uuid::Uuid::nil(), Vec::new(), "n", "a", Some(failure_on_a.clone()),
        )];
        assert_eq!(
            check_should_skip(&node, &view(&pulses), &HashSet::new(), &wired, &HashSet::new()),
            Some(SkipReason::NothingJoined { failure: Some(failure_on_a) }),
        );
    }

    /// A blank text on an optional String input of the node's own is not
    /// given: a group whose every other member closed skips on it, and an
    /// input that keeps blanks counts it given.
    #[test]
    fn a_blank_text_is_not_given_to_a_one_of_group() {
        let node = |keeps: bool| {
            let mut node = node_all_optional();
            node.inputs = ["text", "file"]
                .iter()
                .map(|name| {
                    serde_json::from_value(json!({ "name": name, "portType": "String", "required": false, "optional": true, "fromSpec": true, "keepsBlank": keeps && *name == "text" }))
                        .expect("port")
                })
                .collect();
            node.features.one_of_required = vec![vec!["text".into(), "file".into()]];
            node
        };
        let wired: HashSet<&str> = ["text", "file"].into_iter().collect();
        let pulses = vec![data_pulse("text", json!("  ")), closure_pulse("file")];
        assert!(matches!(
            check_should_skip(&node(false), &view(&pulses), &HashSet::new(), &wired, &HashSet::new()),
            Some(SkipReason::OneOfGroupClosed { .. })
        ));
        assert_eq!(check_should_skip(&node(true), &view(&pulses), &HashSet::new(), &wired, &HashSet::new()), None);
        let pulses = vec![data_pulse("text", json!("hi")), closure_pulse("file")];
        assert_eq!(check_should_skip(&node(false), &view(&pulses), &HashSet::new(), &wired, &HashSet::new()), None);
    }

    fn data_pulse(port: &str, value: serde_json::Value) -> Pulse {
        Pulse::new(uuid::Uuid::new_v4(), uuid::Uuid::nil(), Vec::new(), "n", port, std::sync::Arc::new(value))
    }

    fn closure_pulse(port: &str) -> Pulse {
        Pulse::closure(uuid::Uuid::new_v4(), uuid::Uuid::nil(), Vec::new(), "n", port)
    }

    /// The pulses as a firing sees them.
    fn view(pulses: &[Pulse]) -> Vec<&Pulse> {
        pulses.iter().collect()
    }

    fn node_generator_required() -> crate::project::NodeDefinition {
        serde_json::from_value(json!({
            "id": "n",
            "nodeType": "X",
            "label": null,
            "config": null,
            "position": { "x": 0.0, "y": 0.0 },
            "inputs": [{ "name": "rows", "portType": "Generator[Number]", "required": true }],
            "outputs": [],
            "features": {},
            "scope": [],
            "groupBoundary": null,
            "requiresInfra": false,
            "images": []
        }))
        .expect("node json")
    }

    #[test]
    fn empty_stream_closure_on_a_generator_port_does_not_skip() {
        // A closure on a Generator port is the EMPTY STREAM, a value
        // the body consumes (`next()` answers Ok(None) right away),
        // never a "nothing is coming" skip signal.
        let node = node_generator_required();
        let pulses = vec![closure_pulse("rows")];
        let required: HashSet<&str> = ["rows"].into_iter().collect();
        let wired: HashSet<&str> = ["rows"].into_iter().collect();
        let filled = HashSet::new();
        assert!(
            check_should_skip(&node, &view(&pulses), &required, &wired, &filled).is_none(),
            "an empty stream must RUN its consumer, not skip it"
        );
    }

    #[test]
    fn user_null_on_required_does_not_skip() {
        let node = node_one_required();
        let pulses = vec![data_pulse("a", json!(null))];
        let required: HashSet<&str> = ["a"].into_iter().collect();
        let wired: HashSet<&str> = ["a"].into_iter().collect();
        let filled = HashSet::new();
        assert!(
            check_should_skip(&node, &view(&pulses), &required, &wired, &filled).is_none(),
            "user-emitted null is data; required port + null must NOT skip"
        );
    }

    #[test]
    fn closure_on_required_skips() {
        let node = node_one_required();
        let pulses = vec![closure_pulse("a")];
        let required: HashSet<&str> = ["a"].into_iter().collect();
        let wired: HashSet<&str> = ["a"].into_iter().collect();
        let filled = HashSet::new();
        assert!(
            check_should_skip(&node, &view(&pulses), &required, &wired, &filled).is_some(),
            "closure on a required port must skip"
        );
    }

    /// Every optional input closed is not a reason to skip: the node
    /// runs without them, scope boundaries and plain nodes alike (when
    /// such a node runs is its gate's to say, which the compiler
    /// demands).
    #[test]
    fn all_optional_all_closed_runs() {
        let pulses = vec![closure_pulse("a"), closure_pulse("b")];
        let wired: HashSet<&str> = ["a", "b"].into_iter().collect();
        let mut boundary = node_all_optional();
        boundary.group_boundary = Some(crate::project::GroupBoundary {
            group_id: "g".into(),
            role: crate::project::GroupBoundaryRole::In,
        });
        for node in [node_all_optional(), boundary] {
            assert_eq!(
                check_should_skip(&node, &view(&pulses), &HashSet::new(), &wired, &HashSet::new()),
                None,
                "{:?}",
                node.group_boundary
            );
        }
    }

    #[test]
    fn all_optional_one_value_fires() {
        let node = node_all_optional();
        let pulses = vec![data_pulse("a", json!(7)), closure_pulse("b")];
        let required = HashSet::new();
        let wired: HashSet<&str> = ["a", "b"].into_iter().collect();
        let filled = HashSet::new();
        assert!(
            check_should_skip(&node, &view(&pulses), &required, &wired, &filled).is_none(),
            "all-optional + one value + one closed must NOT skip"
        );
    }

    #[test]
    fn all_optional_closed_wire_plus_a_filled_port_fires() {
        // `a` wired and closed, `b` config-filled: optional `a` runs
        // without its value.
        let node = node_all_optional();
        let pulses = vec![closure_pulse("a")];
        let required = HashSet::new();
        let wired: HashSet<&str> = ["a"].into_iter().collect();
        let filled: HashSet<&str> = ["b"].into_iter().collect();
        assert!(
            check_should_skip(&node, &view(&pulses), &required, &wired, &filled).is_none(),
            "a closed optional input does not skip the node"
        );
    }

    #[test]
    fn all_optional_one_user_null_fires() {
        let node = node_all_optional();
        let pulses = vec![data_pulse("a", json!(null)), closure_pulse("b")];
        let required = HashSet::new();
        let wired: HashSet<&str> = ["a", "b"].into_iter().collect();
        let filled = HashSet::new();
        assert!(
            check_should_skip(&node, &view(&pulses), &required, &wired, &filled).is_none(),
            "user-emitted null is data; one null + one closed must NOT skip"
        );
    }
}
