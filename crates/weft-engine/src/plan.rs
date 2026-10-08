//! What a run needs that depends on its program and not on the call, worked
//! out once and shared by every run it serves.
//!
//! Two layers, both kept by the worker ([`Plans`]):
//!
//! - [`ProgramTables`], per program definition and selection: what a drive
//!   reads on every turn (the wires of the part of the program the run may
//!   dispatch, the nodes by id, their ports, which are group boundaries and
//!   which read a stream, the infra the program declares). Every run reads
//!   them, born here or carried on from its record.
//! - [`RunPlan`], per trigger, per instance its runs are for, and per
//!   version of what its runs read (the trigger as armed, the run facts):
//!   the run a fire of the trigger starts
//!   (its selection, the roots it kicks, what infra it reads baked), whether
//!   what it reads lets it start (its infra up, what its instance provides,
//!   the install's picks), its birth, its caller's connection settings and
//!   its limits. A run born from a plan starts from the plan's roots: no
//!   record is folded to start it.
//!
//! A plan is keyed by the versions of the held copies it was made from
//! (`crate::door::broker`): a change heard drops the copy, the next call
//! reads a new one, and its version makes that call build a new plan.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use serde_json::Value;

use weft_broker_client::protocol::ArmedEntry;
use weft_core::frames::{FiringLocation, Located};
use weft_core::project::selection::RecordedSelection;
use weft_core::project::{DeclaredInfra, ProgramIndex, NodeDefinition};
use weft_core::pulse::PulseTable;
use weft_core::run_spec::KickPlan;
use weft_core::weft_type::WeftType;
use weft_core::{ExecutionId, ProjectDefinition};
use weft_journal::ExecEvent;

use crate::door::{DoorBroker, HeldTrigger};

/// A node's ports as a drive hands them to its firings.
pub(crate) struct NodePorts {
    pub(crate) inputs: Arc<HashMap<String, WeftType>>,
    pub(crate) outputs: Arc<HashMap<String, WeftType>>,
}

/// See the module doc.
pub(crate) struct ProgramTables {
    pub(crate) project: Arc<ProjectDefinition>,
    /// The part of the program the run may dispatch, `None` for the whole.
    pub(crate) selection: Option<RecordedSelection>,
    pub(crate) index: ProgramIndex,
    pub(crate) dispatchable: Option<HashSet<Located>>,
    ports: Vec<NodePorts>,
    /// The group boundaries (a group's In and Out, a loop's): runtime
    /// machinery whose whole state is on record.
    pub(crate) boundaries: HashSet<String>,
    pub(crate) declared_infra: Arc<DeclaredInfra>,
}

impl ProgramTables {
    pub(crate) fn new(project: Arc<ProjectDefinition>, selection: Option<RecordedSelection>) -> Self {
        let index = match &selection {
            Some(selection) => ProgramIndex::selected(&project, selection.selection().as_ref().clone()),
            None => ProgramIndex::build(&project),
        };
        let dispatchable = selection.as_ref().map(|selection| selection.selection().dispatchable_nodes());
        let ports = project
            .nodes
            .iter()
            .map(|node| NodePorts {
                inputs: Arc::new(node.inputs.iter().map(|p| (p.name.clone(), p.port_type.clone())).collect()),
                outputs: Arc::new(node.outputs.iter().map(|p| (p.name.clone(), p.port_type.clone())).collect()),
            })
            .collect::<Vec<_>>();
        let boundaries = project.nodes.iter().filter(|n| n.group_boundary.is_some()).map(|n| n.id.clone()).collect();
        let declared_infra = Arc::new(DeclaredInfra::of(&project));
        Self { project, selection, index, dispatchable, ports, boundaries, declared_infra }
    }

    /// The node whose id is `id`.
    pub(crate) fn node(&self, id: &str) -> Option<&NodeDefinition> {
        self.index.node(&self.project, id)
    }

    /// The ports of the node whose id is `id`.
    pub(crate) fn ports(&self, id: &str) -> Option<&NodePorts> {
        self.index.position(id).map(|at| &self.ports[at])
    }

    /// The nodes ready to fire on `pulses`, in the program's order: what
    /// `weft_core::exec::find_ready_nodes` answers, asking only the nodes
    /// some pending pulse waits at (a node with none is never ready)
    /// instead of every node of the program.
    pub(crate) fn ready(&self, pulses: &PulseTable) -> Vec<(String, weft_core::exec::ReadyGroup)> {
        let mut waited_at: Vec<usize> = pulses
            .iter()
            .filter(|(_, bucket)| bucket.iter().any(|pulse| pulse.status.is_pending()))
            .filter_map(|(id, _)| self.index.position(id))
            .collect();
        waited_at.sort_unstable();
        weft_core::exec::ready::find_ready_among(
            &self.project,
            waited_at.into_iter().map(|at| &self.project.nodes[at]),
            pulses,
            &self.index,
            self.dispatchable.as_ref(),
        )
        .into_iter()
        .map(|(node, group)| (node.id.clone(), group))
        .collect()
    }
}

/// Why what a run reads keeps it from starting now: each closes on its own
/// (an infra comes up, an instance gives its values, a connection is
/// picked).
#[derive(Debug, Clone)]
pub(crate) enum RunGate {
    /// Infra it reads is not running.
    InfraDown(Vec<weft_core::infra::run_gate::MissingCopy>),
    /// What its instance provides does not fit it.
    InstanceValues(weft_core::run_spec::Refusal),
    /// The install's picks for its connections do not fit it.
    Picks(weft_core::run_spec::Refusal),
}

/// Why a plan's run cannot start.
#[derive(Debug, Clone)]
pub(crate) enum Unready {
    /// The fire itself is wrong (the trigger is not one, the run needs an
    /// instance it lacks): it never becomes a run.
    Refused(String),
    /// It waits on [`RunGate`].
    Waits(RunGate),
}

/// What a plan's runs start with besides their call: the values their
/// instance provides and the install's picks, as the gate read them.
#[derive(Debug, Clone, Default)]
pub(crate) struct Ready {
    pub(crate) instance_values: weft_core::instance::InstanceValues,
    pub(crate) picks: weft_core::picks::Picks,
}

/// See the module doc.
pub(crate) struct RunPlan {
    pub(crate) trigger: Arc<HeldTrigger>,
    pub(crate) entry: Arc<ArmedEntry>,
    /// The instance its runs are for: the one the call named, else the one
    /// whose copy of the trigger fired.
    pub(crate) instance: Option<weft_core::instance::InstanceId>,
    /// How long its ended runs are kept: the trigger's, else the project's.
    pub(crate) keep_for: weft_core::run_settings::KeepFor,
    /// The run it starts, or why it cannot start now.
    pub(crate) start: Result<Startable, Unready>,
}

/// The run a plan starts.
pub(crate) struct Startable {
    pub(crate) program: Arc<ProgramTables>,
    /// The roots its run kicks, the firing one's payload left for the call.
    kicks: Vec<KickPlan>,
    /// The infra places it reads only baked outputs of, with their saved
    /// values: they do not run, and the run is born with their values.
    baked: weft_core::infra::bake::Saved,
    /// What it starts with besides its call.
    pub(crate) ready: Ready,
}

/// A run's first state, as a plan starts it.
pub(crate) struct Opening {
    pub(crate) kicked: HashMap<FiringLocation, weft_core::primitive::KickedNode>,
    pub(crate) pulses: PulseTable,
}

impl RunPlan {
    fn build(
        program_of: impl FnOnce(RecordedSelection) -> Arc<ProgramTables>,
        project: &Arc<ProjectDefinition>,
        trigger: &Arc<HeldTrigger>,
        entry: &Arc<ArmedEntry>,
        instance: Option<&weft_core::instance::InstanceId>,
        facts: &weft_broker_client::protocol::DoorRunFacts,
    ) -> Self {
        let saved = weft_core::infra::bake::saved_for_run(project, &facts.infra, instance);
        // The firing kick's payload is the call's: the carve does not read it.
        let start = weft_core::run_spec::compute_trigger_fire(project, &trigger.node_id, &Value::Null, entry.port_snapshot.as_ref(), &saved)
            .map_err(Unready::Refused)
            .and_then(|fire| {
                let selection = RecordedSelection::new(fire.subgraph);
                weft_core::run_spec::refuse_instanceless(project, selection.selection(), instance)
                    .map_err(|refusal| Unready::Refused(refusal.to_string()))?;
                let ready = run_facts_gate(project, selection.selection(), instance, facts).map_err(Unready::Waits)?;
                Ok(Startable { program: program_of(selection), kicks: fire.kicks, baked: fire.baked, ready })
            });
        Self {
            trigger: trigger.clone(),
            entry: entry.clone(),
            instance: instance.cloned(),
            keep_for: entry.spec.settings.kept_for(project.defaults.keep_for()),
            start,
        }
    }
}

impl Startable {
    /// The kicks of a run whose call brought `payload`.
    fn kicks_with(&self, payload: &Value) -> Vec<KickPlan> {
        self.kicks
            .iter()
            .map(|kick| KickPlan { payload: if kick.firing { Some(payload.clone()) } else { kick.payload.clone() }, ..kick.clone() })
            .collect()
    }

    /// The birth of run `execution_id` of `plan`, whose call brought
    /// `payload`: its `ExecutionStarted`, its kicks and the values it reads
    /// baked, in the order its record keeps them.
    pub(crate) fn birth(&self, plan: &RunPlan, execution_id: ExecutionId, payload: &Value, at_unix: u64) -> Vec<ExecEvent> {
        let program = &self.program;
        let ready = &self.ready;
        let kicks = self.kicks_with(payload);
        let (start, kicks) = weft_journal::birth::birth_events(weft_journal::birth::Birth {
            execution_id,
            project_id: program.project.id,
            phase: weft_core::context::Phase::Fire,
            entry_node: &plan.trigger.node_id,
            kicks: &kicks,
            definition_hash: &plan.entry.definition_hash,
            binary_hash: &plan.entry.binary_hash,
            selection: program.selection.as_ref(),
            seed: None,
            source_version: Some(&plan.entry.source_version),
            instance: plan.instance.as_ref().map(|instance| weft_journal::birth::RunFor { instance, values: &ready.instance_values }),
            picks: &ready.picks,
            fired_trigger: Some(&plan.trigger.node_id),
            stand_in: None,
            settings: plan.entry.spec.settings,
            at_unix,
        });
        let baked = weft_journal::birth::baked_rows(&program.project, execution_id, &self.baked, at_unix);
        std::iter::once(start).chain(kicks).chain(baked).collect()
    }

    /// The first state of run `execution_id`, whose call brought `payload`:
    /// its roots kicked, and the values it reads baked already on their
    /// wires. Built from the plan, never from a fold of its birth. A baked
    /// value that does not fit its wires is the program's shape broken.
    pub(crate) fn opening(&self, execution_id: ExecutionId, payload: &Value) -> Result<Opening, String> {
        let kicked = self
            .kicks_with(payload)
            .into_iter()
            .map(|kick| {
                (
                    FiringLocation::new(kick.node, kick.frames),
                    weft_core::primitive::KickedNode {
                        firing: kick.firing,
                        payload: kick.payload,
                        port_snapshot: kick.port_snapshot,
                        dispatched: false,
                        scope_skipped: None,
                    },
                )
            })
            .collect();
        let mut pulses = PulseTable::new();
        if !self.baked.is_empty() {
            let program = &self.program;
            let rows = weft_journal::birth::baked_rows(&program.project, execution_id, &self.baked, 0);
            weft_journal::fold::provided_pulses(&program.project, &program.index, execution_id, &rows, &mut pulses)?;
        }
        Ok(Opening { kicked, pulses })
    }
}

/// The run gate over `facts`: what a run of `selection` reads is up, what
/// its instance provides fits it, and the install's picks fit it. Answers
/// the values and picks it is born with.
fn run_facts_gate(
    project: &ProjectDefinition,
    selection: &weft_core::project::selection::RunSelection,
    instance: Option<&weft_core::instance::InstanceId>,
    facts: &weft_broker_client::protocol::DoorRunFacts,
) -> Result<Ready, RunGate> {
    let within: HashSet<String> = selection.nodes.iter().map(|place| weft_core::project::address_of(project, &place.id, &place.path)).collect();
    let wanted = weft_core::infra::run_gate::copies_read(project, Some(&within), instance);
    let missing = weft_core::infra::run_gate::missing_copies(&wanted, &facts.infra);
    if !missing.is_empty() {
        return Err(RunGate::InfraDown(missing));
    }
    let instance_values = match (instance, &facts.instance_values) {
        (Some(instance), Some(stored)) => {
            weft_core::run_spec::instance_run_values(project, selection, instance, stored).map_err(RunGate::InstanceValues)?
        }
        _ => Default::default(),
    };
    let picks = if weft_core::picks::picked_places(project).is_empty() {
        Default::default()
    } else {
        weft_core::picks::run_picks(project, selection, &facts.picks).map_err(RunGate::Picks)?
    };
    Ok(Ready { instance_values, picks })
}

/// A map of at most `cap` entries that forgets the oldest-inserted one
/// first. Not a true LRU: what a worker keeps here is read in bursts of one
/// program, one trigger, so insertion order tracks recency closely enough,
/// and a read stays a plain map lookup.
struct Bounded<K, V> {
    map: HashMap<K, V>,
    order: std::collections::VecDeque<K>,
    cap: usize,
}

impl<K: std::hash::Hash + Eq + Clone, V: Clone> Bounded<K, V> {
    fn new(cap: usize) -> Self {
        Self { map: HashMap::new(), order: std::collections::VecDeque::new(), cap }
    }

    fn get(&self, key: &K) -> Option<V> {
        self.map.get(key).cloned()
    }

    fn insert(&mut self, key: K, value: V) {
        if self.map.insert(key.clone(), value).is_none() {
            self.order.push_back(key);
            while self.order.len() > self.cap {
                if let Some(evicted) = self.order.pop_front() {
                    self.map.remove(&evicted);
                }
            }
        }
    }
}

/// Which program a table set is for: its definition, and its selection's
/// digest (`None` for the whole program).
type ProgramKey = (String, Option<String>);

/// Which plan: the trigger's token, and the versions of the held triggers
/// and run facts it was made from.
type PlanKey = (String, Option<weft_core::instance::InstanceId>, u64, u64);

/// What a worker keeps of its programs and plans (see the module doc).
pub(crate) struct Plans {
    project_id: uuid::Uuid,
    projects: Mutex<Bounded<String, Arc<ProjectDefinition>>>,
    programs: Mutex<Bounded<ProgramKey, Arc<ProgramTables>>>,
    plans: Mutex<Bounded<PlanKey, Arc<RunPlan>>>,
}

impl Plans {
    pub(crate) fn new(project_id: uuid::Uuid) -> Self {
        Self {
            project_id,
            projects: Mutex::new(Bounded::new(8)),
            programs: Mutex::new(Bounded::new(64)),
            plans: Mutex::new(Bounded::new(256)),
        }
    }

    /// The program whose definition hash is `definition_hash`: kept, else
    /// fetched from the broker, which reads it from the project's
    /// append-only history, so a run carried on after the project was
    /// built again still runs the shape it started on.
    pub(crate) async fn project(&self, clients: &crate::context::EngineClients, definition_hash: &str) -> anyhow::Result<Arc<ProjectDefinition>> {
        if let Some(project) = self.projects.lock().expect("project cache").get(&definition_hash.to_string()) {
            return Ok(project);
        }
        let project = clients.project.fetch_definition(self.project_id, definition_hash).await?.ok_or_else(|| {
            anyhow::anyhow!(
                "no row in project_definition for project {} hash {definition_hash}: a run was started for a program that was never recorded",
                self.project_id
            )
        })?;
        let project = Arc::new(project);
        self.projects.lock().expect("project cache").insert(definition_hash.to_string(), project.clone());
        Ok(project)
    }

    /// The tables of `project` (whose definition hash is `definition_hash`)
    /// for `selection`.
    pub(crate) fn program(&self, project: &Arc<ProjectDefinition>, definition_hash: &str, selection: Option<RecordedSelection>) -> Arc<ProgramTables> {
        let key = (definition_hash.to_string(), selection.as_ref().map(|s| s.digest().to_string()));
        if let Some(tables) = self.programs.lock().expect("program cache").get(&key) {
            return tables;
        }
        let tables = Arc::new(ProgramTables::new(project.clone(), selection));
        self.programs.lock().expect("program cache").insert(key, tables.clone());
        tables
    }

    /// The plan of `trigger` (armed as `entry`, from the held triggers of
    /// version `triggers_version`) for runs for `instance`. A plan whose run waits on what the held
    /// run facts say is made again from the broker's answer now: a change
    /// committed a moment ago may not have been heard yet.
    pub(crate) async fn plan(
        &self,
        clients: &crate::context::EngineClients,
        broker: &dyn DoorBroker,
        trigger: &Arc<HeldTrigger>,
        entry: &Arc<ArmedEntry>,
        instance: Option<&weft_core::instance::InstanceId>,
        triggers_version: u64,
    ) -> anyhow::Result<Arc<RunPlan>> {
        let facts = broker.run_facts(instance).await?;
        let plan = self.plan_over(clients, trigger, entry, instance, triggers_version, &facts).await?;
        if !matches!(plan.start, Err(Unready::Waits(_))) {
            return Ok(plan);
        }
        let fresh = broker.fresh_run_facts(instance).await?;
        self.plan_over(clients, trigger, entry, instance, triggers_version, &fresh).await
    }

    async fn plan_over(
        &self,
        clients: &crate::context::EngineClients,
        trigger: &Arc<HeldTrigger>,
        entry: &Arc<ArmedEntry>,
        instance: Option<&weft_core::instance::InstanceId>,
        triggers_version: u64,
        facts: &crate::door::RunFacts,
    ) -> anyhow::Result<Arc<RunPlan>> {
        let key = (trigger.token.clone(), instance.cloned(), triggers_version, facts.version);
        if let Some(plan) = self.plans.lock().expect("plan cache").get(&key) {
            return Ok(plan);
        }
        let project = self.project(clients, &entry.definition_hash).await?;
        let plan = Arc::new(RunPlan::build(
            |selection| self.program(&project, &entry.definition_hash, Some(selection)),
            &project,
            trigger,
            entry,
            instance,
            &facts.facts,
        ));
        self.plans.lock().expect("plan cache").insert(key, plan.clone());
        Ok(plan)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use serde_json::json;

    /// A plan of `trigger` in `project`, armed as a live timer of this
    /// worker's program, over `facts`: what a door's first call builds.
    pub(crate) fn plan_of(project: &Arc<ProjectDefinition>, trigger: &str, facts: &weft_broker_client::protocol::DoorRunFacts) -> RunPlan {
        plan_for(project, trigger, None, facts)
    }

    /// [`plan_of`], its runs for `instance`.
    pub(crate) fn plan_for(
        project: &Arc<ProjectDefinition>,
        trigger: &str,
        instance: Option<&weft_core::instance::InstanceId>,
        facts: &weft_broker_client::protocol::DoorRunFacts,
    ) -> RunPlan {
        let spec: weft_core::primitive::SignalSpec = serde_json::from_value(json!({ "kind": "timer", "config": {} })).unwrap();
        let entry = Arc::new(ArmedEntry {
            spec,
            auth_kind: "none".into(),
            auth_config: None,
            port_snapshot: None,
            definition_hash: "def-1".into(),
            binary_hash: "bin-1".into(),
            source_version: "v1".into(),
            standing: weft_core::arrival::Standing {
                status: weft_core::projects::ProjectStatus::Active,
                accepting_fires: true,
                fires_deadline_unix: None,
            },
            instance: None,
        });
        let held = Arc::new(HeldTrigger::new("t1".into(), trigger.into(), crate::door::HeldEntry::Armed(entry.clone()), None));
        RunPlan::build(|selection| Arc::new(ProgramTables::new(project.clone(), Some(selection))), project, &held, &entry, instance, facts)
    }

    /// A trigger feeding two nodes, one behind the other.
    pub(crate) fn chain() -> Arc<ProjectDefinition> {
        Arc::new(
            serde_json::from_value(json!({
                "id": uuid::Uuid::from_u128(9),
                "nodes": [
                    { "id": "trig", "nodeType": "Trig", "position": { "x": 0.0, "y": 0.0 },
                      "outputs": [{ "name": "value", "portType": "String", "required": false }],
                      "features": { "isTrigger": true } },
                    { "id": "a", "nodeType": "Step", "position": { "x": 1.0, "y": 0.0 },
                      "inputs": [{ "name": "value", "portType": "String", "required": true }],
                      "outputs": [{ "name": "value", "portType": "String", "required": false }] },
                    { "id": "b", "nodeType": "Step", "position": { "x": 2.0, "y": 0.0 },
                      "inputs": [{ "name": "value", "portType": "String", "required": true }],
                      "outputs": [{ "name": "value", "portType": "String", "required": false }] }
                ],
                "edges": [
                    { "id": "e1", "source": "trig", "target": "a", "sourceHandle": "value", "targetHandle": "value" },
                    { "id": "e2", "source": "a", "target": "b", "sourceHandle": "value", "targetHandle": "value" }
                ]
            }))
            .unwrap(),
        )
    }

    /// A run started from a plan holds exactly what the fold of its own
    /// birth holds: the roots kicked (the firing one with the call's
    /// payload), the wires as they stand, the same part of the program.
    #[test]
    fn a_run_from_a_plan_starts_where_the_fold_of_its_birth_does() {
        let project = chain();
        let plan = plan_of(&project, "trig", &Default::default());
        let startable = plan.start.as_ref().map_err(|_| "unready").expect("a plan that starts");
        let execution_id = ExecutionId::from_u128(4);
        let payload = json!({ "tick": 1 });
        let opening = startable.opening(execution_id, &payload).unwrap();
        let birth = startable.birth(&plan, execution_id, &payload, 7);
        let folded = weft_journal::fold_to_snapshot(execution_id, project.clone(), &birth);
        assert!(folded.corruptions.is_empty(), "{:?}", folded.corruptions);
        let kicks = |kicked: &HashMap<FiringLocation, weft_core::primitive::KickedNode>| {
            let mut kicks: Vec<(String, String)> =
                kicked.iter().map(|(at, kick)| (format!("{at:?}"), serde_json::to_string(kick).unwrap())).collect();
            kicks.sort();
            kicks
        };
        assert_eq!(kicks(&opening.kicked), kicks(&folded.kicked));
        let pending = |pulses: &PulseTable| -> Vec<(String, String)> {
            pulses.values().flatten().filter(|p| p.status.is_pending()).map(|p| (p.target_node.clone(), p.target_port.clone())).collect()
        };
        assert_eq!(pending(&opening.pulses), pending(&folded.pulses));
        assert_eq!(folded.selection.as_ref(), startable.program.selection.as_ref().map(|s| s.selection().as_ref()));
    }

    /// The ready nodes asked only where a pending pulse waits are the ones
    /// a scan of every node finds, in the same order, whatever pulses are
    /// pending, absorbed or closed where (random tables over a program with
    /// a join, a chain and a node reading two inputs).
    #[test]
    fn readiness_asked_where_pulses_wait_matches_a_full_scan() {
        let project: Arc<ProjectDefinition> = Arc::new(
            serde_json::from_value(json!({
                "id": uuid::Uuid::from_u128(10),
                "nodes": [
                    { "id": "s1", "nodeType": "S", "position": { "x": 0.0, "y": 0.0 },
                      "outputs": [{ "name": "v", "portType": "String", "required": false }] },
                    { "id": "s2", "nodeType": "S", "position": { "x": 0.0, "y": 1.0 },
                      "outputs": [{ "name": "v", "portType": "String", "required": false }] },
                    { "id": "join", "nodeType": "J", "position": { "x": 1.0, "y": 0.0 },
                      "inputs": [{ "name": "a", "portType": "String", "required": true },
                                 { "name": "b", "portType": "String", "required": false }],
                      "outputs": [{ "name": "v", "portType": "String", "required": false }] },
                    { "id": "tail", "nodeType": "T", "position": { "x": 2.0, "y": 0.0 },
                      "inputs": [{ "name": "a", "portType": "String", "required": true }] }
                ],
                "edges": [
                    { "id": "e1", "source": "s1", "target": "join", "sourceHandle": "v", "targetHandle": "a" },
                    { "id": "e2", "source": "s2", "target": "join", "sourceHandle": "v", "targetHandle": "b" },
                    { "id": "e3", "source": "join", "target": "tail", "sourceHandle": "v", "targetHandle": "a" }
                ]
            }))
            .unwrap(),
        );
        let program = ProgramTables::new(project.clone(), None);
        let execution_id = ExecutionId::from_u128(5);
        let targets = [("join", "a"), ("join", "b"), ("tail", "a")];
        // A small linear congruential generator: the same tables every run.
        let mut seed: u64 = 0x5eed;
        let mut next = move |bound: u64| {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            (seed >> 33) % bound
        };
        for _ in 0..500 {
            let mut pulses = PulseTable::new();
            for _ in 0..next(6) {
                let (node, port) = targets[next(targets.len() as u64) as usize];
                let id = uuid::Uuid::from_u128(u128::from(next(u64::MAX)));
                let mut pulse = if next(4) == 0 {
                    weft_core::pulse::Pulse::closure(id, execution_id, Vec::new(), node, port)
                } else {
                    weft_core::pulse::Pulse::new(id, execution_id, Vec::new(), node, port, Arc::new(json!("x")))
                };
                if next(3) == 0 {
                    pulse.absorb();
                }
                pulses.entry(node.to_string()).or_default().push(pulse);
            }
            let asked: Vec<(String, Vec<uuid::Uuid>)> = program.ready(&pulses).into_iter().map(|(id, group)| (id, group.pulse_ids)).collect();
            let scanned: Vec<(String, Vec<uuid::Uuid>)> = weft_core::exec::find_ready_nodes(&project, &pulses, &program.index, None)
                .into_iter()
                .map(|(id, group)| (id, group.pulse_ids))
                .collect();
            assert_eq!(asked, scanned);
        }
    }

    /// A plan of a node that is no trigger never starts a run, and says why.
    #[test]
    fn a_plan_of_no_trigger_refuses() {
        let plan = plan_of(&chain(), "a", &Default::default());
        assert!(matches!(plan.start, Err(Unready::Refused(_))));
    }

    /// A run that reaches a step per instance is planned for the instance
    /// its call names: with none it is refused, naming the step, and with
    /// one its plan (and the birth of its runs) is for that instance.
    #[test]
    fn a_plan_is_for_the_instance_its_call_names() {
        let mut project = (*chain()).clone();
        project.nodes[1].per_instance = Some(weft_core::instance::PerInstance::Marked);
        let project = Arc::new(project);
        let refused = plan_of(&project, "trig", &Default::default());
        assert!(matches!(&refused.start, Err(Unready::Refused(why)) if why.contains("once per instance")), "{:?}", refused.start.as_ref().err());
        let ada = weft_core::instance::InstanceId::new("ada").unwrap();
        let plan = plan_for(&project, "trig", Some(&ada), &Default::default());
        assert_eq!(plan.instance, Some(ada));
        assert!(!matches!(plan.start, Err(Unready::Refused(_))), "{:?}", plan.start.as_ref().err());
    }
}
