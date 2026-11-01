//! Where one node of a run stands (`weft_core::program::NodeState`): what
//! a program reads through `GetNodeState` and a person through
//! `weft node-state`, one answer for both.
//!
//! The status is folded from the run's record, so it is readable for as
//! long as the run is kept, whoever drives it and whether anybody does. A
//! node waiting on a signal also shows the raw last answer its wait saw,
//! which only the listener can read off the wait (the answer is the kind's
//! own state), so that one part is asked of it.

use std::collections::HashMap;

use axum::http::StatusCode;

use weft_core::frames::{loop_indices, FiringLocation, Located, LoopFrames};
use weft_core::primitive::{AwaitedEntryKind, LoopInstanceKey, LoopTerminationReason, RunSnapshot};
use weft_core::program::{NodeState, NodeStatus};
use weft_core::project::graph::ProjectGraph;
use weft_core::project::places::{enclosing_loops, every_place, members_in, named, scopes_around, state_place, Named, StatePlace};
use weft_core::project::scopes::ScopeCopy;
use weft_core::project::{loop_in_id, loop_out_id, ProgramIndex, ProjectDefinition};
use weft_core::run::scope::ScopeState;
use weft_core::run::{firing_at, NodeFiringStatus};
use weft_core::signal::listener_protocol::LastAnswer;
use weft_core::RunId;

use crate::state::DispatcherState;

type ReadError = (StatusCode, String);

/// Where the node `spelled` (written the way `--from` takes it) stands in
/// run `run_id` of `project_id`. A run of another project is no run here.
pub async fn read(state: &DispatcherState, project_id: uuid::Uuid, run_id: RunId, spelled: &str) -> Result<NodeState, ReadError> {
    let internal = |what: &'static str| move |e: anyhow::Error| (StatusCode::INTERNAL_SERVER_ERROR, format!("{what}: {e:#}"));
    let owner = state.runs.run_owner(run_id).await.map_err(internal("the run's row"))?
        .filter(|owner| owner.project_id == project_id)
        .ok_or_else(|| (StatusCode::NOT_FOUND, format!("there is no run {run_id} in this project")))?;
    // The name is read against the whole program the run was started on
    // (a run of part of it runs a cut, which leaves out what it reuses),
    // so it is found by the name it had in that run.
    let program = crate::projection::registered_program(state, run_id, &owner).await.map_err(internal("the run's program"))?
        .or_why(run_id)
        .map_err(|why| (StatusCode::CONFLICT, why))?;
    let (cut, snapshot) = crate::projection::run_snapshot(state, run_id).await.map_err(internal("the run's record"))?;
    let place = place_in_run(&program, &cut, spelled).map_err(|why| (StatusCode::BAD_REQUEST, why))?;
    if !in_run(&cut, &snapshot, &place) {
        return Err((StatusCode::BAD_REQUEST, format!("run {run_id} runs only part of its program, and '{spelled}' is not in that part")));
    }
    let seen = seen_at(&program, &cut, &snapshot, &place);
    let live = match &seen.waiting_on {
        Some(token) => wait_answer(state, token).await?,
        None => None,
    };
    Ok(NodeState { status: seen.status, live, error: seen.error })
}

/// The place `spelled` names (read against the whole `program`) whose
/// state the run of `cut` can tell ([`state_place`]), and a node inside
/// loops `cut` holds only part of the body of: that run never launched
/// them, it ran inside iteration 0 of each, so the node fired once there.
pub(crate) fn place_in_run(program: &ProjectDefinition, cut: &ProjectDefinition, spelled: &str) -> Result<StatePlace, String> {
    state_place(program, spelled).or_else(|why| {
        let Ok(Named::Node(place)) = named(program, spelled) else { return Err(why) };
        let index = ProgramIndex::build(cut);
        let loops = enclosing_loops(&ProjectGraph::new(cut), &place);
        let entered = !loops.is_empty() && loops.iter().all(|around| index.scopes().get(&around.id).is_some_and(|scope| scope.entered));
        if entered && every_place(cut).contains(&place) { Ok(StatePlace::Node(place)) } else { Err(why) }
    })
}

/// Whether `place` is part of the run whose program is `cut` (the part of
/// its registered program it runs) and whose record is `snapshot`: a place
/// its cut holds, or one its seed handed it. A loop is part of it when its
/// `LoopIn` is: a cut takes a loop whole.
pub(crate) fn in_run(cut: &ProjectDefinition, snapshot: &RunSnapshot, place: &StatePlace) -> bool {
    let at = match place {
        StatePlace::Node(place) => place.clone(),
        StatePlace::Loop(place) => Located::new(loop_in_id(&place.id), place.path.clone()),
    };
    snapshot.inherited_origins.contains_key(&at) || weft_core::project::places::every_place(cut).contains(&at)
}

/// The loop iterations each wait the node at `place` registered sits in,
/// by the wait's token, outermost first, read off the run's record
/// (`snapshot`): what tells two iterations waiting at one node apart.
pub(crate) fn wait_iterations(snapshot: &RunSnapshot, place: &Located) -> HashMap<String, Vec<u32>> {
    snapshot
        .awaited_sequences
        .iter()
        .filter(|(at, _)| Located::at(at.node_id.clone(), &at.frames) == *place)
        .flat_map(|(at, entries)| {
            entries.iter().filter_map(move |entry| match &entry.kind {
                AwaitedEntryKind::Await { token, .. } => Some((token.clone(), loop_indices(&at.frames))),
                _ => None,
            })
        })
        .collect()
}

/// What a run's record says of one place: its status, the wait it is on
/// while it waits, and its error when it failed.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Seen {
    pub status: NodeStatus,
    pub waiting_on: Option<String>,
    pub error: Option<String>,
}

impl Seen {
    fn of(status: NodeStatus) -> Self {
        Self { status, waiting_on: None, error: None }
    }
}

/// [`Seen`] for `place` in `snapshot`, the record of a run of `cut` (a
/// cut of `project`, or `project` itself): pure, so every status is
/// pinned by a test without a database.
pub(crate) fn seen_at(project: &ProjectDefinition, cut: &ProjectDefinition, snapshot: &RunSnapshot, place: &StatePlace) -> Seen {
    let index = ProgramIndex::build(cut);
    match place {
        StatePlace::Node(place) => node_seen(project, snapshot, place, index.frames_at(cut, place), |scope| {
            ScopeCopy::new(scope.id.clone(), index.scopes().frames_at(Some(&scope.id), &scope.path))
        }),
        StatePlace::Loop(place) => {
            let entry = Located::new(loop_in_id(&place.id), place.path.clone());
            loop_seen(project, snapshot, place, index.frames_at(cut, &entry))
        }
    }
}

/// What the record says of the node at `place`, which fires at `frames`;
/// `copy_of` is the copy of a scope around it the run would decide.
fn node_seen(project: &ProjectDefinition, snapshot: &RunSnapshot, place: &Located, frames: LoopFrames, copy_of: impl Fn(&Located) -> ScopeCopy) -> Seen {
    // A node outside every loop fires once in a run; a seeded run holds the
    // firing it took over under its own id too.
    let Some(firing) = firing_at(&snapshot.firings, &place.id, snapshot.run_id, &frames) else {
        // Not fired yet: it never will when a scope around it (a group, a
        // site, the included file's body) decided not to run, before the
        // skip reached the node itself.
        let graph = ProjectGraph::new(project);
        let skipped = scopes_around(&graph, place).into_iter().any(|scope| {
            snapshot.scopes.get(&copy_of(&scope)).is_some_and(|record| matches!(record.state, ScopeState::Skipped { .. }))
        });
        return Seen::of(if skipped { NodeStatus::Skipped } else { NodeStatus::Pending });
    };
    let pending_wait = || {
        snapshot.awaited_sequences.get(&FiringLocation::new(place.id.clone(), frames.clone()))?.iter().rev().find_map(|entry| match &entry.kind {
            AwaitedEntryKind::Await { token, ended: None } => Some(token.clone()),
            _ => None,
        })
    };
    match firing.status() {
        // A wait the worker holds leaves its firing running: the wait on
        // record with no ending says it waits all the same.
        NodeFiringStatus::Running => match pending_wait() {
            Some(token) => Seen { status: NodeStatus::Waiting, waiting_on: Some(token), error: None },
            None => Seen::of(NodeStatus::Running),
        },
        NodeFiringStatus::WaitingForInput => {
            Seen { status: NodeStatus::Waiting, waiting_on: firing.callback_id.clone().or_else(pending_wait), error: None }
        }
        NodeFiringStatus::Completed => Seen::of(NodeStatus::Completed),
        NodeFiringStatus::Failed => Seen { status: NodeStatus::Failed, waiting_on: None, error: firing.error.clone() },
        NodeFiringStatus::Skipped => Seen::of(NodeStatus::Skipped),
        NodeFiringStatus::Cancelled => Seen::of(NodeStatus::Cancelled),
    }
}

/// A loop is read whole: not yet started, going, or ended the way its
/// instance ended. A loop that failed reads the error of the firing that
/// failed first among its boundaries and the nodes inside it, by the
/// second its failure was recorded at (the journal's precision); failures
/// in the same second are taken boundaries first, then in the order of
/// the loop's members.
fn loop_seen(project: &ProjectDefinition, snapshot: &RunSnapshot, place: &Located, parent_frames: LoopFrames) -> Seen {
    let key = LoopInstanceKey { group_id: place.id.clone(), parent_frames: parent_frames.clone(), run_id: snapshot.run_id };
    let Some(instance) = snapshot.loop_runtime.get(&key) else { return Seen::of(NodeStatus::Pending) };
    match &instance.terminated {
        None => Seen::of(NodeStatus::Running),
        Some(LoopTerminationReason::Cancelled) => Seen::of(NodeStatus::Cancelled),
        Some(LoopTerminationReason::Failed) => {
            let graph = ProjectGraph::new(project);
            let inside = |frames: &LoopFrames| frames.len() > parent_frames.len() && frames.starts_with(&parent_frames);
            let boundaries = [loop_in_id(&place.id), loop_out_id(&place.id)];
            let members = members_in(&graph, &place.id, &place.path);
            let error = boundaries
                .iter()
                .chain(members.iter().map(|member| &member.id))
                .flat_map(|id| snapshot.firings.get(id).into_iter().flat_map(|records| records.values()))
                .filter(|f| f.status() == NodeFiringStatus::Failed && (f.frames == parent_frames || inside(&f.frames)))
                .min_by_key(|f| f.completed_at)
                .and_then(|f| f.error.clone());
            Seen { status: NodeStatus::Failed, waiting_on: None, error }
        }
        Some(LoopTerminationReason::OverExhausted | LoopTerminationReason::DoneVoted | LoopTerminationReason::MaxItersReached) => {
            Seen::of(NodeStatus::Completed)
        }
    }
}

/// What the wait `token` kept of the last answer it saw, off the listener
/// holding it; `None` when it saw none yet, keeps none, or is gone by now
/// (answered between the fold and this read).
async fn wait_answer(state: &DispatcherState, token: &str) -> Result<Option<LastAnswer>, ReadError> {
    let live = state
        .listener
        .live(token, None)
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, format!("the wait's last answer, from the listener: {e:#}")))?;
    Ok(live.and_then(|live| live.answer))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::{json, Value};
    use weft_core::primitive::SignalSpec;
    use weft_journal::RunEvent;

    use super::*;

    const RUN: RunId = RunId::from_u128(7);

    /// `a` and `b` at the top, and `c` inside the loop `g`.
    fn program() -> ProjectDefinition {
        let node = |id: &str, scope: &[&str], boundary: Value| json!({
            "id": id, "nodeType": "T", "label": null, "config": {},
            "position": {"x": 0, "y": 0}, "inputs": [], "outputs": [],
            "features": {}, "scope": scope, "groupBoundary": boundary, "requiresInfra": false
        });
        serde_json::from_value(json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "nodes": [
                node("a", &[], Value::Null), node("b", &[], Value::Null),
                node(&loop_in_id("g"), &[], json!({"groupId": "g", "role": "In"})),
                node("c", &["g"], Value::Null),
                node(&loop_out_id("g"), &[], json!({"groupId": "g", "role": "Out"}))
            ],
            "edges": [],
            "groups": [{"id": "g", "kind": "loop", "loopConfig": {}, "nodeIds": ["c"]}]
        }))
        .unwrap()
    }

    /// What the record of `RUN` says of `spelled` once `events` are folded.
    fn seen(events: &[RunEvent], spelled: &str) -> Seen {
        let project = program();
        let snapshot = weft_journal::fold_to_snapshot(RUN, Arc::new(project.clone()), events);
        seen_at(&project, &project, &snapshot, &state_place(&project, spelled).unwrap())
    }

    fn started(node: &str) -> RunEvent {
        RunEvent::NodeStarted { run_id: RUN, node_id: node.into(), frames: vec![], at_unix: 1 }
    }

    fn registered(node: &str, token: &str) -> RunEvent {
        RunEvent::SuspensionRegistered {
            run_id: RUN,
            node_id: node.into(),
            frames: vec![],
            token: token.into(),
            spec: SignalSpec::of_kind("poll_endpoint", json!({ "url": "https://x/status" })),
            call_index: 0,
            at_unix: 2,
        }
    }

    #[test]
    fn a_node_reads_pending_until_it_fires_then_its_firing() {
        assert_eq!(seen(&[], "a").status, NodeStatus::Pending);
        assert_eq!(seen(&[started("a")], "a").status, NodeStatus::Running);
        assert_eq!(seen(&[started("a")], "b").status, NodeStatus::Pending, "another node's firing is not its own");
        let done = RunEvent::NodeCompleted { run_id: RUN, node_id: "a".into(), frames: vec![], at_unix: 3 };
        assert_eq!(seen(&[started("a"), done], "a"), Seen::of(NodeStatus::Completed));
        let failed = RunEvent::NodeFailed { run_id: RUN, node_id: "a".into(), frames: vec![], error: "boom".into(), gave_up_wait: false, at_unix: 3 };
        assert_eq!(seen(&[started("a"), failed], "a"), Seen { status: NodeStatus::Failed, waiting_on: None, error: Some("boom".into()) });
        let skipped = RunEvent::NodeSkipped {
            run_id: RUN,
            node_id: "a".into(),
            frames: vec![],
            reason: weft_core::run::skip::SkipReason::DidNotFlow,
            at_unix: 3,
        };
        assert_eq!(seen(&[started("a"), skipped], "a").status, NodeStatus::Skipped);
        let cancelled = RunEvent::NodeCancelled { run_id: RUN, node_id: "a".into(), frames: vec![], reason: "stop".into(), at_unix: 3 };
        assert_eq!(seen(&[started("a"), cancelled], "a").status, NodeStatus::Cancelled);
    }

    /// A node waits whether its worker holds the wait (its firing still
    /// running) or the run paused on it, and it is the wait with no ending
    /// yet that it waits on.
    #[test]
    fn a_node_with_a_wait_on_record_and_no_ending_waits() {
        let held = seen(&[started("a"), registered("a", "t1")], "a");
        assert_eq!((held.status, held.waiting_on.as_deref()), (NodeStatus::Waiting, Some("t1")));
        let suspended = RunEvent::NodeSuspended { run_id: RUN, node_id: "a".into(), frames: vec![], token: "t1".into(), at_unix: 3 };
        let paused = seen(&[started("a"), registered("a", "t1"), suspended], "a");
        assert_eq!((paused.status, paused.waiting_on.as_deref()), (NodeStatus::Waiting, Some("t1")));
    }

    /// Each wait a node registered is read off the record with the loop
    /// iterations it sits in; another node's waits are not its own.
    #[test]
    fn a_nodes_waits_are_read_with_their_iterations() {
        let in_iteration = |node: &str, index: u32, token: &str| RunEvent::SuspensionRegistered {
            run_id: RUN,
            node_id: node.into(),
            frames: vec![weft_core::frames::Frame::Loop { index }],
            token: token.into(),
            spec: SignalSpec::of_kind("poll_endpoint", json!({ "url": "https://x/status" })),
            call_index: 0,
            at_unix: 2,
        };
        let project = program();
        let snapshot = weft_journal::fold_to_snapshot(
            RUN,
            Arc::new(project.clone()),
            &[in_iteration("c", 0, "t0"), in_iteration("c", 1, "t1"), registered("a", "ta")],
        );
        let iterations = wait_iterations(&snapshot, &Located::top("c"));
        assert_eq!(iterations, HashMap::from([("t0".to_string(), vec![0]), ("t1".to_string(), vec![1])]));
    }

    /// A name is read against the whole registered program, and a place
    /// is part of a run of a cut of it when the cut holds it (a loop by its
    /// `LoopIn`, the cut takes it whole) or its seed handed it over.
    #[test]
    fn a_place_is_in_a_run_when_its_cut_holds_it_or_its_seed_handed_it() {
        let registered = program();
        let mut cut = program();
        cut.nodes.retain(|node| node.id != "b");
        let mut snapshot = weft_journal::fold_to_snapshot(RUN, Arc::new(cut.clone()), &[]);
        let place = |spelled: &str| state_place(&registered, spelled).unwrap();
        assert!(in_run(&cut, &snapshot, &place("a")));
        assert!(in_run(&cut, &snapshot, &place("g")), "a loop, by its LoopIn");
        assert!(!in_run(&cut, &snapshot, &place("b")), "the cut left it out");
        snapshot.inherited_origins.insert(Located::top("b"), RunId::from_u128(9));
        assert!(in_run(&cut, &snapshot, &place("b")), "the seed handed it over");
    }

    /// A node inside a group that did not run reads skipped from the
    /// group's decision, before (or without) any firing of its own.
    #[test]
    fn a_node_in_a_scope_that_did_not_run_reads_skipped() {
        let mut project = program();
        project.nodes.push(serde_json::from_value(json!({
            "id": "inside", "nodeType": "T", "label": null, "config": {},
            "position": {"x": 0, "y": 0}, "inputs": [], "outputs": [],
            "features": {}, "scope": ["off"], "requiresInfra": false
        })).unwrap());
        project.groups.push(serde_json::from_value(json!({
            "id": "off", "kind": "group", "nodeIds": ["inside"],
            "portLiterals": { "_should_flow": false }
        })).unwrap());
        let started = RunEvent::RunStarted {
            run_id: RUN, project_id: project.id, entry_node: "a".into(), phase: weft_core::context::Phase::Fire,
            definition_hash: None, binary_hash: None, cut_from: None, source_version: None,
            run_kind: weft_core::run::RunKind::Project, seed: None, instance: None, stand_in: None,
            fired_trigger: None, settings: Default::default(), instance_values: Default::default(),
            picks: Default::default(), at_unix: 0, given: Default::default(),
        };
        let snapshot = weft_journal::fold_to_snapshot(RUN, Arc::new(project.clone()), &[started]);
        let place = state_place(&project, "inside").unwrap();
        assert_eq!(seen_at(&project, &project, &snapshot, &place).status, NodeStatus::Skipped);
        assert!(state_place(&project, "off").unwrap_err().contains("is a group"));
        let refusal = state_place(&project, "c").unwrap_err();
        assert!(refusal.contains("inside loop 'g'") && refusal.contains("read the loop 'g'"), "{refusal}");
    }

    /// A run of part of a loop's body (its cut holds neither of the loop's
    /// ends) never launched the loop: a node of that part fired once,
    /// inside iteration 0, and reads like a node outside every loop. In a
    /// run that holds the loop, it is refused as ever.
    #[test]
    fn a_node_of_a_run_of_part_of_its_loops_body_reads_its_one_firing() {
        let registered = program();
        let mut cut = program();
        cut.nodes.retain(|node| node.id == "c");
        let inside = vec![weft_core::frames::Frame::Loop { index: 0 }];
        let events = [
            RunEvent::NodeStarted { run_id: RUN, node_id: "c".into(), frames: inside.clone(), at_unix: 1 },
            RunEvent::NodeCompleted { run_id: RUN, node_id: "c".into(), frames: inside, at_unix: 2 },
        ];
        let snapshot = weft_journal::fold_to_snapshot(RUN, Arc::new(cut.clone()), &events);
        let place = place_in_run(&registered, &cut, "c").expect("a node of the part the run ran");
        assert!(in_run(&cut, &snapshot, &place));
        assert_eq!(seen_at(&registered, &cut, &snapshot, &place).status, NodeStatus::Completed);
        let refusal = place_in_run(&registered, &registered, "c").unwrap_err();
        assert!(refusal.contains("read the loop 'g'"), "{refusal}");
    }

    /// A loop reads whole: pending before it starts, running while it goes,
    /// and ended the way its instance ended, a failure with the first error
    /// recorded inside it.
    #[test]
    fn a_loop_reads_the_way_its_instance_stands() {
        use weft_core::run::loop_runtime::{LoopConfig, LoopItemSource};
        let project = program();
        let place = state_place(&project, "g").unwrap();
        let mut snapshot = weft_journal::fold_to_snapshot(RUN, Arc::new(project.clone()), &[]);
        assert_eq!(seen_at(&project, &project, &snapshot, &place).status, NodeStatus::Pending);
        let key = LoopInstanceKey { group_id: "g".into(), parent_frames: vec![], run_id: RUN };
        let config = LoopConfig { parallel: false, over: vec![], carry: vec![], max_iters: None, trim_on_mismatch: false };
        snapshot.loop_runtime.ensure(key.clone(), config, LoopItemSource::DoneDriven, None, vec![]);
        assert_eq!(seen_at(&project, &project, &snapshot, &place).status, NodeStatus::Running);

        let mut failed = snapshot.clone();
        failed.loop_runtime.terminate(&key, LoopTerminationReason::Failed).unwrap();
        let inside = vec![weft_core::frames::Frame::Loop { index: 0 }];
        let events = [
            RunEvent::NodeStarted { run_id: RUN, node_id: "c".into(), frames: inside.clone(), at_unix: 1 },
            RunEvent::NodeFailed { run_id: RUN, node_id: "c".into(), frames: inside, error: "bad item".into(), gave_up_wait: false, at_unix: 2 },
        ];
        failed.firings = weft_journal::fold_to_snapshot(RUN, Arc::new(project.clone()), &events).firings;
        assert_eq!(seen_at(&project, &project, &failed, &place), Seen { status: NodeStatus::Failed, waiting_on: None, error: Some("bad item".into()) });

        snapshot.loop_runtime.terminate(&key, LoopTerminationReason::DoneVoted).unwrap();
        assert_eq!(seen_at(&project, &project, &snapshot, &place).status, NodeStatus::Completed);
    }
}
