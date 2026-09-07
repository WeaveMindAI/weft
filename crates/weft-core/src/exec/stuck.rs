//! What a stuck execution was waiting for. The drive loop declares an
//! execution stuck when pulses are still pending, nothing is running,
//! and nothing is parked on a signal: no firing can ever complete. That
//! is a graph-shape bug, and the person reading the terminal row needs
//! the shape, not the verdict. A bare "execution stuck" sent one
//! reader to the dispatcher's HTTP API with curl to find out which node
//! it was; this report names every firing that holds a pulse, the
//! ports that arrived, and the wired ports still empty, so the fix
//! (an unwired input, a producer that never fires, a trigger's program
//! leaking into another) reads off the message.

use std::fmt;

use crate::frames::LoopFrames;
use crate::project::{EdgeIndex, ProjectDefinition};
use crate::pulse::PulseTable;
use crate::Color;

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

/// Every firing a stuck execution left holding pulses, in node order.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StuckReport {
    pub firings: Vec<StuckFiring>,
}

/// Walk the pulse table and name each (node, frames) with a pending
/// pulse, with what it holds and what it lacks. Pure: the same table
/// gives the same report.
pub fn stuck_report(
    project: &ProjectDefinition,
    edge_idx: &EdgeIndex,
    pulses: &PulseTable,
) -> StuckReport {
    let mut firings = Vec::new();
    for node in &project.nodes {
        let Some(node_pulses) = pulses.get(&node.id) else {
            continue;
        };
        let wired: Vec<String> = edge_idx
            .get_incoming(project, &node.id)
            .iter()
            .map(|e| e.target_handle.clone().unwrap_or_else(|| "default".to_string()))
            .fold(Vec::new(), |mut acc, port| {
                if !acc.contains(&port) {
                    acc.push(port);
                }
                acc
            });
        // One entry per exact firing point: pulses only meet when their
        // color and frames agree, so that is the unit that is stuck.
        // Kept in first-seen order (the table's own), which is what a
        // reader following the run expects.
        let mut groups: Vec<((Color, LoopFrames), Vec<String>)> = Vec::new();
        for p in node_pulses.iter().filter(|p| p.status.is_pending()) {
            let key = (p.color, p.frames.clone());
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
            let missing = wired.iter().filter(|w| !holding.contains(w)).cloned().collect();
            firings.push(StuckFiring { node_id: node.id.clone(), frames, holding, missing });
        }
    }
    StuckReport { firings }
}

/// `#3` for the fourth iteration of one loop, `#3.0` one level deeper,
/// nothing at the root: the same suffix `weft logs` prints.
fn frames_suffix(frames: &LoopFrames) -> String {
    if frames.is_empty() {
        return String::new();
    }
    let path: Vec<String> = frames.iter().map(|f| f.index.to_string()).collect();
    format!("#{}", path.join("."))
}

impl fmt::Display for StuckReport {
    /// The terminal row's text. One clause per firing, so a reader
    /// holds the whole shape in one line:
    /// `execution stuck: 2 firings hold pulses and can never fire:
    /// theirs has value, still waiting on go; leaf has value, every
    /// wired input arrived`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let n = self.firings.len();
        write!(
            f,
            "execution stuck: {n} firing{} pulses and can never fire",
            if n == 1 { " holds" } else { "s hold" }
        )?;
        for (i, firing) in self.firings.iter().enumerate() {
            write!(f, "{} ", if i == 0 { ":" } else { ";" })?;
            write!(f, "{}{} has {}", firing.node_id, frames_suffix(&firing.frames), firing.holding.join(", "))?;
            if firing.missing.is_empty() {
                write!(f, ", every wired input arrived")?;
            } else {
                write!(f, ", still waiting on {}", firing.missing.join(", "))?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frames::LoopIteration;
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

    fn pulse(color: Color, frames: LoopFrames, node: &str, port: &str) -> Pulse {
        Pulse::new(color, frames, node, port, json!("x"))
    }

    #[test]
    fn names_the_ports_held_and_the_wired_ports_still_empty() {
        let p = project(
            vec![node("src", &[]), node("theirs", &["value", "go"])],
            &[("src", "theirs", "value"), ("src", "theirs", "go")],
        );
        let idx = EdgeIndex::build(&p);
        let color = uuid::Uuid::new_v4();
        let mut pulses = PulseTable::new();
        pulses.insert("theirs".into(), vec![pulse(color, vec![], "theirs", "value")]);
        let report = stuck_report(&p, &idx, &pulses);
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
            "execution stuck: 1 firing holds pulses and can never fire: theirs has value, still waiting on go"
        );
    }

    #[test]
    fn one_entry_per_frame_stack_and_the_iteration_in_the_text() {
        let p = project(
            vec![node("src", &[]), node("step", &["a", "b"])],
            &[("src", "step", "a"), ("src", "step", "b")],
        );
        let idx = EdgeIndex::build(&p);
        let color = uuid::Uuid::new_v4();
        let f2 = vec![LoopIteration { index: 2 }];
        let f5 = vec![LoopIteration { index: 5 }];
        let mut pulses = PulseTable::new();
        pulses.insert(
            "step".into(),
            vec![
                pulse(color, f2.clone(), "step", "a"),
                pulse(color, f5.clone(), "step", "a"),
                pulse(color, f5.clone(), "step", "b"),
            ],
        );
        let report = stuck_report(&p, &idx, &pulses);
        assert_eq!(report.firings.len(), 2);
        let text = report.to_string();
        assert!(text.contains("step#2 has a, still waiting on b"), "{text}");
        assert!(text.contains("step#5 has a, b, every wired input arrived"), "{text}");
        assert!(text.starts_with("execution stuck: 2 firings hold pulses"), "{text}");
    }

    #[test]
    fn absorbed_pulses_are_not_stuck() {
        let p = project(vec![node("src", &[]), node("sink", &["value"])], &[("src", "sink", "value")]);
        let idx = EdgeIndex::build(&p);
        let color = uuid::Uuid::new_v4();
        let mut absorbed = pulse(color, vec![], "sink", "value");
        absorbed.status = crate::pulse::PulseStatus::Absorbed;
        let mut pulses = PulseTable::new();
        pulses.insert("sink".into(), vec![absorbed]);
        assert!(stuck_report(&p, &idx, &pulses).firings.is_empty());
    }
}
