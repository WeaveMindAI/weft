//! What a stuck run was waiting for. The drive loop declares a
//! run stuck when pulses are still pending, nothing is running,
//! and nothing is parked on a signal: no firing can ever complete. That
//! is a graph-shape bug, and the person reading the terminal row needs
//! the shape, not the verdict. A bare "run stuck" sent one
//! reader to the dispatcher's HTTP API with curl to find out which node
//! it was; this report names every firing that holds a pulse, the
//! ports that arrived, and the wired ports still empty, every scope
//! that never decided whether to run, and every node a run kicked that
//! never started, so the fix (an unwired input, a producer that never
//! fires, a gate nothing feeds) reads off the message.

use std::fmt;

use crate::frames::{FiringLocation, LoopFrames};
use crate::primitive::KickTable;
use crate::project::scopes::ScopeCopy;
use crate::project::{ProgramIndex, ProjectDefinition};
use crate::pulse::PulseTable;
use crate::run::scope::ScopeTable;
use crate::RunId;

/// One firing that can never complete: the pulses it holds and the
/// wired ports it is still waiting on, at one exact frame stack.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StuckFiring {
    pub node_id: String,
    pub frames: LoopFrames,
    /// Input ports holding a pending pulse, in port order.
    pub holding: Vec<String>,
    /// Wired input ports with no pulse at this frame stack, in edge
    /// order. Empty when every wired port arrived and the node still
    /// did not fire, which points at the node itself rather than its
    /// inputs.
    pub missing: Vec<String>,
}

/// What a stuck run was left waiting on: every firing holding pulses,
/// in node order; every copy of a scope that never decided whether to
/// run though every copy around it opened, in order; every node kicked
/// that never started (its scope never opened), in order.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StuckReport {
    pub firings: Vec<StuckFiring>,
    pub waiting_scopes: Vec<ScopeCopy>,
    pub unstarted: Vec<FiringLocation>,
}

impl StuckReport {
    /// Nothing is left waiting.
    pub fn is_empty(&self) -> bool {
        self.firings.is_empty() && self.waiting_scopes.is_empty() && self.unstarted.is_empty()
    }
}

/// Walk the run's tables and name each (node, frames) with a pending
/// pulse, with what it holds and what it lacks, each scope copy still
/// waiting, and each kick never dispatched. Pure: the same tables give
/// the same report.
pub fn stuck_report(
    project: &ProjectDefinition,
    program_idx: &ProgramIndex,
    pulses: &PulseTable,
    scopes: &ScopeTable,
    kicked: &KickTable,
) -> StuckReport {
    let mut firings = Vec::new();
    for node in &project.nodes {
        let Some(node_pulses) = pulses.get(&node.id) else {
            continue;
        };
        // One entry per exact firing point: pulses only meet when their
        // run and frames agree, so that is the unit that is stuck.
        // Kept in first-seen order (the table's own), which is what a
        // reader following the run expects.
        let mut groups: Vec<((RunId, LoopFrames), Vec<String>)> = Vec::new();
        for p in node_pulses.iter().filter(|p| p.status.is_pending()) {
            let key = (p.run_id, p.frames.clone());
            let holding = match groups.iter_mut().find(|(k, _)| *k == key) {
                Some((_, holding)) => holding,
                None => {
                    groups.push((key, Vec::new()));
                    &mut groups.last_mut().expect("just pushed").1
                }
            };
            if !holding.contains(&p.target_port) {
                holding.push(p.target_port.clone());
            }
        }
        for ((_, frames), holding) in groups {
            let wired: Vec<String> = program_idx
                .get_incoming(project, &node.id, &frames)
                .iter()
                .map(|e| e.target_handle.clone().unwrap_or_else(|| "default".to_string()))
                .fold(Vec::new(), |mut acc, port| {
                    if !acc.contains(&port) {
                        acc.push(port);
                    }
                    acc
                });
            let missing = wired.iter().filter(|w| !holding.contains(w)).cloned().collect();
            firings.push(StuckFiring { node_id: node.id.clone(), frames, holding, missing });
        }
    }
    let waiting_scopes = crate::run::scope::waiting_copies(program_idx, scopes);
    // In place order already: the table keeps them sorted.
    let unstarted: Vec<FiringLocation> = kicked.undispatched().cloned().collect();
    StuckReport { firings, waiting_scopes, unstarted }
}

/// `#3` for the fourth iteration of one loop, `#3.0` one level deeper,
/// `#@auth` inside the include `auth` uses, nothing at the root: the
/// same suffix `weft logs` prints.
fn frames_suffix(frames: &LoopFrames) -> String {
    if frames.is_empty() {
        return String::new();
    }
    format!("#{}", crate::frames::frames_text(frames))
}

impl fmt::Display for StuckReport {
    /// The terminal row's text. One clause per firing, so a reader
    /// holds the whole shape in one line:
    /// `run stuck: 2 firings hold pulses and can never fire:
    /// theirs has value, still waiting on go; leaf has value, every
    /// wired input arrived`, then the scopes that never decided and the
    /// nodes that never started.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("run stuck")?;
        let n = self.firings.len();
        if n > 0 {
            write!(f, ": {n} firing{} pulses and can never fire", if n == 1 { " holds" } else { "s hold" })?;
            for (i, firing) in self.firings.iter().enumerate() {
                write!(f, "{} ", if i == 0 { ":" } else { ";" })?;
                write!(f, "{}{} has {}", firing.node_id, frames_suffix(&firing.frames), firing.holding.join(", "))?;
                if firing.missing.is_empty() {
                    write!(f, ", every wired input arrived")?;
                } else {
                    write!(f, ", still waiting on {}", firing.missing.join(", "))?;
                }
            }
        }
        if !self.waiting_scopes.is_empty() {
            let named: Vec<String> = self.waiting_scopes.iter().map(|copy| format!("{}{}", copy.scope, frames_suffix(&copy.frames))).collect();
            write!(
                f,
                "{} {} never decided whether to run (nothing reached {} gate or inputs)",
                if n > 0 { ";" } else { ":" },
                named.join(", "),
                if named.len() == 1 { "its" } else { "their" },
            )?;
        }
        if !self.unstarted.is_empty() {
            let named: Vec<String> = self.unstarted.iter().map(|at| format!("{}{}", at.node_id, frames_suffix(&at.frames))).collect();
            write!(
                f,
                "{} {} never started, waiting on {} scope to open",
                if n > 0 || !self.waiting_scopes.is_empty() { ";" } else { ":" },
                named.join(", "),
                if named.len() == 1 { "its" } else { "their" },
            )?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::frames::Frame;
    use super::*;
    use crate::pulse::Pulse;
    use serde_json::json;

    fn node(id: &str, inputs: &[&str]) -> serde_json::Value {
        let ins: Vec<serde_json::Value> = inputs
            .iter()
            .map(|n| json!({ "name": n, "portType": "String", "required": true }))
            .collect();
        json!({
            "id": id, "nodeType": "Sink", "label": null,
            "config": null, "position": { "x": 0.0, "y": 0.0 },
            "inputs": ins,
            "outputs": [{ "name": "value", "portType": "String", "required": false }],
            "features": {}, "scope": [], "groupBoundary": null,
            "requiresInfra": false, "images": []
        })
    }

    fn project(nodes: Vec<serde_json::Value>, edges: &[(&str, &str, &str)]) -> ProjectDefinition {
        let edges: Vec<serde_json::Value> = edges
            .iter()
            .enumerate()
            .map(|(i, (s, t, port))| {
                json!({ "id": format!("e{i}"), "source": s, "target": t,
                        "sourceHandle": "value", "targetHandle": port })
            })
            .collect();
        serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(),
            "nodes": nodes,
            "edges": edges,
            "createdAt": "2026-01-01T00:00:00Z",
            "updatedAt": "2026-01-01T00:00:00Z"
        }))
        .expect("project")
    }

    fn pulse(run_id: RunId, frames: LoopFrames, node: &str, port: &str) -> Pulse {
        Pulse::new(uuid::Uuid::new_v4(), run_id, frames, node, port, std::sync::Arc::new(json!("x")))
    }

    #[test]
    fn names_the_ports_held_and_the_wired_ports_still_empty() {
        let p = project(
            vec![node("src", &[]), node("theirs", &["value", "go"])],
            &[("src", "theirs", "value"), ("src", "theirs", "go")],
        );
        let idx = ProgramIndex::build(&p);
        let run_id = uuid::Uuid::new_v4();
        let mut pulses = PulseTable::new();
        pulses.insert("theirs".into(), vec![pulse(run_id, vec![], "theirs", "value")]);
        let report = stuck_report(&p, &idx, &pulses, &Default::default(), &Default::default());
        assert_eq!(
            report.firings,
            vec![StuckFiring {
                node_id: "theirs".into(),
                frames: vec![],
                holding: vec!["value".into()],
                missing: vec!["go".into()],
            }]
        );
        assert_eq!(
            report.to_string(),
            "run stuck: 1 firing holds pulses and can never fire: theirs has value, still waiting on go"
        );
    }

    #[test]
    fn one_entry_per_frame_stack_and_the_iteration_in_the_text() {
        let p = project(
            vec![node("src", &[]), node("join", &["a", "b"])],
            &[("src", "join", "a"), ("src", "join", "b")],
        );
        let idx = ProgramIndex::build(&p);
        let run_id = uuid::Uuid::new_v4();
        let f2 = vec![Frame::Loop { index: 2 }];
        let f5 = vec![Frame::Loop { index: 5 }];
        let mut pulses = PulseTable::new();
        pulses.insert(
            "join".into(),
            vec![
                pulse(run_id, f2.clone(), "join", "a"),
                pulse(run_id, f5.clone(), "join", "a"),
                pulse(run_id, f5.clone(), "join", "b"),
            ],
        );
        let report = stuck_report(&p, &idx, &pulses, &Default::default(), &Default::default());
        assert_eq!(report.firings.len(), 2);
        let text = report.to_string();
        assert!(text.contains("join#2 has a, still waiting on b"), "{text}");
        assert!(text.contains("join#5 has a, b, every wired input arrived"), "{text}");
        assert!(text.starts_with("run stuck: 2 firings hold pulses"), "{text}");
    }

    #[test]
    fn absorbed_pulses_are_not_stuck() {
        let p = project(vec![node("src", &[]), node("sink", &["value"])], &[("src", "sink", "value")]);
        let idx = ProgramIndex::build(&p);
        let run_id = uuid::Uuid::new_v4();
        let mut pulses = PulseTable::new();
        pulses.insert("sink".into(), vec![pulse(run_id, vec![], "sink", "value")]);
        pulses.absorb("sink", |_| true);
        assert!(stuck_report(&p, &idx, &pulses, &Default::default(), &Default::default()).firings.is_empty());
    }
}
