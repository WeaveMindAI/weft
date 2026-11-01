//! What a run needs that depends on its program and not on the call, worked
//! out once and shared by every run it serves.
//!
//! Two layers, both kept by the worker ([`Plans`]):
//!
//! - [`ProgramTables`], per program definition: what a drive reads on
//!   every turn (the wires, the nodes by id, their ports, which are a
//!   loop's boundaries and which read a stream, the infra the program
//!   declares). Every run reads them, born here or carried on from its
//!   record.
//! - [`RunPlan`], per trigger, per instance its runs are for, and per
//!   version of what its runs read (the trigger as armed, the run facts):
//!   the run a fire of the trigger starts
//!   (its cut of the program, the roots it kicks, what infra it reads
//!   baked), whether
//!   what it reads lets it start (its infra up, what its instance provides,
//!   the install's picks), its birth, its caller's connection settings and
//!   its limits. A run born from a plan starts from the plan's roots: no
//!   record is folded to start it.
//!
//! A plan is keyed by the versions of the held copies it was made from
//! (`crate::door::broker`): a change heard drops the copy, the next call
//! reads a new one, and its version makes that call build a new plan.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use serde_json::Value;

use weft_broker_client::protocol::ArmedEntry;
use weft_core::content_cache::{json_bytes, ContentCache, LoadFailed};
use weft_core::frames::FiringLocation;
use weft_core::project::{DeclaredInfra, ProgramIndex, NodeDefinition};
use weft_core::pulse::PulseTable;
use weft_core::run_spec::KickPlan;
use weft_core::weft_type::WeftType;
use weft_core::{RunId, ProjectDefinition};
use weft_journal::record::RunsProgram;
use weft_journal::RunEvent;

use crate::door::{DoorBroker, HeldTrigger};

/// A node's ports as a drive hands them to its firings.
pub(crate) struct NodePorts {
    pub(crate) inputs: Arc<HashMap<String, WeftType>>,
    pub(crate) outputs: Arc<HashMap<String, WeftType>>,
}

/// A node's ports at every place it runs: its own, and those of each
/// place whose types differ (a node inside an included file a site
/// resolved its own way, `NodeDefinition::place_types`).
struct PlacedPorts {
    own: NodePorts,
    places: Vec<(Vec<String>, NodePorts)>,
}

/// See the module doc.
pub(crate) struct ProgramTables {
    pub(crate) project: Arc<ProjectDefinition>,
    pub(crate) index: ProgramIndex,
    ports: Vec<PlacedPorts>,
    /// A loop's boundaries (its `LoopIn` and `LoopOut`): runtime
    /// machinery whose whole state is on record.
    pub(crate) boundaries: HashSet<String>,
    pub(crate) declared_infra: Arc<DeclaredInfra>,
}

impl ProgramTables {
    pub(crate) fn new(project: Arc<ProjectDefinition>) -> Self {
        let index = ProgramIndex::build(&project);
        let ports = project
            .nodes
            .iter()
            .map(|node| {
                // The node's own inputs: the gates are the language's
                // (the bag drops them too), so a node asking what it
                // declares (a List with no items, a script binding its
                // ports by name) never sees them.
                let ports_at = |types: weft_core::project::TypesAt<'_>| NodePorts {
                    inputs: Arc::new(
                        node.inputs
                            .iter()
                            .filter(|p| !weft_core::run::skip::is_gate_port(&p.name))
                            .filter_map(|p| Some((p.name.clone(), types.input(&p.name)?.clone())))
                            .collect(),
                    ),
                    outputs: Arc::new(
                        node.outputs.iter().filter_map(|p| Some((p.name.clone(), types.output(&p.name)?.clone()))).collect(),
                    ),
                };
                PlacedPorts {
                    own: ports_at(node.types_at::<&str>(&[])),
                    places: node
                        .place_types
                        .iter()
                        .map(|place| (place.place.clone(), ports_at(node.types_at(&place.place))))
                        .collect(),
                }
            })
            .collect::<Vec<_>>();
        let boundaries = project.nodes.iter().filter(|n| n.group_boundary.is_some()).map(|n| n.id.clone()).collect();
        let declared_infra = Arc::new(DeclaredInfra::of(&project));
        Self { project, index, ports, boundaries, declared_infra }
    }

    /// The node whose id is `id`.
    pub(crate) fn node(&self, id: &str) -> Option<&NodeDefinition> {
        self.index.node(&self.project, id)
    }

    /// The ports of the node whose id is `id`, as they are where it
    /// fires at `frames` (the include call path on them picks the place).
    pub(crate) fn ports(&self, id: &str, frames: &weft_core::frames::LoopFrames) -> Option<&NodePorts> {
        let at = self.index.position(id)?;
        let placed = &self.ports[at];
        // The same reading `NodeDefinition::types_at` makes: the entry of
        // the place this firing runs at, else the node's own.
        let place = self.project.nodes[at].place_of(&weft_core::frames::call_path(frames));
        Some(place.map_or(&placed.own, |index| &placed.places[index].1))
    }

    /// The nodes ready to fire on `pulses`, in the program's order: what
    /// `weft_core::run::find_ready_nodes` answers, asking only the nodes
    /// some pending pulse waits at (a node with none is never ready)
    /// instead of every node of the program.
    pub(crate) fn ready(&self, pulses: &PulseTable, given: &weft_core::project::Given) -> Vec<(String, weft_core::run::ReadyGroup)> {
        let mut waited_at: Vec<usize> = pulses
            .iter()
            .filter(|(_, bucket)| bucket.iter().any(|pulse| pulse.status.is_pending()))
            .filter_map(|(id, _)| self.index.position(id))
            .collect();
        waited_at.sort_unstable();
        weft_core::run::ready::find_ready_among(
            &self.project,
            waited_at.into_iter().map(|at| &self.project.nodes[at]),
            pulses,
            &self.index,
            given,
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
    /// Whether the worker keeps it for the fires after this one: not when
    /// it is refused for a content its cut names that is gone
    /// (`crate::contents::Gone`), since `weft resync` stores the content
    /// again under the same name, and may leave everything else a plan is
    /// keyed by as it was. The next fire reads the content again.
    keep: bool,
}

/// The run a plan starts.
pub(crate) struct Startable {
    /// Its cut of the program (`weft_core::run_spec::compute_trigger_fire`).
    pub(crate) program: Arc<ProgramTables>,
    /// The cut as its runs' births name it and the record keeps it: a
    /// run's first write carries the cut ([`RunsProgram::kept`]) unless its
    /// lane already put it on record (`crate::journal_writer`).
    pub(crate) runs: RunsProgram,
    /// The roots its run kicks, the firing one's payload left for the call.
    kicks: Vec<KickPlan>,
    /// What its runs are given at the places its cut says: the saved
    /// values of the infra places it reads only baked, standing in.
    pub(crate) given: Arc<weft_core::project::Given>,
    /// The infra places it reads only baked outputs of, with their saved
    /// values: they do not run, and each run's record says why.
    baked: weft_core::infra::bake::Saved,
    /// What it starts with besides its call.
    pub(crate) ready: Ready,
}

impl RunPlan {
    /// What a plan weighs in a worker's [`Plans`]: itself, and the cut it
    /// keeps written out for its runs' births ([`RunsProgram`]). Its cut's
    /// tables are weighed where they are kept, with the programs.
    fn weight(&self) -> usize {
        std::mem::size_of::<Self>() + self.start.as_ref().map_or(0, |start| start.runs.json_bytes())
    }

    /// The plan of a fire of `trigger` in `project`, its cut's tables made
    /// by `program_of` (handed the cut and its definition hash): only the
    /// contents the cut names are read, so a fire never waits on, or fails
    /// for, a file only other triggers' runs read. A content the cut names
    /// that is gone refuses the plan; one whose read failed is the error
    /// this answers, and no plan is made.
    async fn build<Fut>(
        program_of: impl FnOnce(ProjectDefinition, String) -> Fut,
        project: &Arc<ProjectDefinition>,
        trigger: &Arc<HeldTrigger>,
        entry: &Arc<ArmedEntry>,
        instance: Option<&weft_core::instance::InstanceId>,
        facts: &weft_broker_client::protocol::DoorRunFacts,
    ) -> anyhow::Result<Self>
    where
        Fut: std::future::Future<Output = Result<Arc<ProgramTables>, LoadFailed>>,
    {
        let saved = weft_core::infra::bake::saved_for_run(project, &facts.infra, instance);
        let mut keep = true;
        let start = async {
            // The firing kick's payload is the call's: the cut does not read it.
            let fire =
                weft_core::run_spec::compute_trigger_fire(project, &trigger.node_id, &Value::Null, entry.port_snapshot.as_ref(), &saved)
                    .map_err(Unready::Refused)?;
            weft_core::run_spec::refuse_instanceless(&fire.program, instance).map_err(|refusal| Unready::Refused(refusal.to_string()))?;
            let ready = run_facts_gate(&fire.program, instance, facts).map_err(Unready::Waits)?;
            let runs = RunsProgram::of(&entry.definition_hash, project.id, &fire.program)
                .map_err(|e| Unready::Refused(format!("the cut a fire of '{}' runs does not serialize: {e:#}", trigger.node_id)))?;
            let program = program_of(fire.program, runs.definition_hash().to_string()).await.map_err(|e| {
                let cut = format!("the cut a fire of '{}' runs", trigger.node_id);
                if e.downcast_ref::<crate::contents::Gone>().is_some() {
                    keep = false;
                    Stopped::Unready(Unready::Refused(format!("{cut}: {e}")))
                } else {
                    Stopped::Unread(anyhow::Error::new(e).context(cut))
                }
            })?;
            Ok::<_, Stopped>(Startable { program, runs, kicks: fire.kicks, given: Arc::new(fire.given), baked: fire.baked, ready })
        }
        .await;
        let start = match start {
            Ok(startable) => Ok(startable),
            Err(Stopped::Unready(unready)) => Err(unready),
            Err(Stopped::Unread(e)) => return Err(e),
        };
        Ok(Self {
            trigger: trigger.clone(),
            entry: entry.clone(),
            instance: instance.cloned(),
            keep_for: entry.spec.settings.kept_for(project.defaults.keep_for()),
            start,
            keep,
        })
    }
}

/// Why [`RunPlan::build`] stopped short of a run that starts: what the
/// plan says ([`Unready`]), or a read that failed, which makes no plan.
enum Stopped {
    Unready(Unready),
    Unread(anyhow::Error),
}

impl From<Unready> for Stopped {
    fn from(unready: Unready) -> Self {
        Self::Unready(unready)
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

    /// The birth of run `run_id` of `plan`, whose call brought
    /// `payload`: its `RunStarted`, its kicks and the values it reads
    /// baked, in the order its record keeps them.
    pub(crate) fn birth(&self, plan: &RunPlan, run_id: RunId, payload: &Value, at_unix: u64) -> Vec<RunEvent> {
        let program = &self.program;
        let ready = &self.ready;
        let kicks = self.kicks_with(payload);
        let (start, kicks) = weft_journal::birth::birth_events(weft_journal::birth::Birth {
            run_id,
            project_id: program.project.id,
            phase: weft_core::context::Phase::Fire,
            entry_node: &plan.trigger.node_id,
            kicks: &kicks,
            program: &self.runs,
            given: &self.given,
            binary_hash: &plan.entry.binary_hash,
            seed: None,
            source_version: Some(&plan.entry.source_version),
            instance: plan.instance.as_ref().map(|instance| weft_journal::birth::RunFor { instance, values: &ready.instance_values }),
            picks: &ready.picks,
            fired_trigger: Some(&plan.trigger.node_id),
            stand_in: None,
            settings: plan.entry.spec.settings,
            at_unix,
        });
        let baked = weft_journal::birth::baked_rows(run_id, &program.project, &program.index, &self.baked, at_unix);
        std::iter::once(start).chain(kicks).chain(baked).collect()
    }

    /// The first state of a run whose call brought `payload`: its roots
    /// kicked, and nothing on its wires. Built from the plan, never from a
    /// fold of its birth; what its cut gives (the baked values its
    /// stand-ins send) and what its scopes decide at once go out on the
    /// drive's first pass over its scopes (`weft_core::run::scope::settle_table`),
    /// as a fold of its birth sends and decides them.
    pub(crate) fn opening(&self, payload: &Value) -> weft_core::primitive::KickTable {
        let mut kicked = weft_core::primitive::KickTable::new();
        for kick in self.kicks_with(payload) {
            kicked.kick(FiringLocation::new(kick.node, kick.frames), || {
                weft_core::primitive::KickedNode::new(kick.firing, kick.payload, kick.port_snapshot, None)
            });
        }
        kicked
    }
}

/// The run gate over `facts`: what a run of `program` (its cut) reads is
/// up, what its instance provides fits it, and the install's picks fit
/// it. Answers the values and picks it is born with.
fn run_facts_gate(
    program: &ProjectDefinition,
    instance: Option<&weft_core::instance::InstanceId>,
    facts: &weft_broker_client::protocol::DoorRunFacts,
) -> Result<Ready, RunGate> {
    let wanted = weft_core::infra::run_gate::copies_read(program, None, instance);
    let missing = weft_core::infra::run_gate::missing_copies(&wanted, &facts.infra);
    if !missing.is_empty() {
        return Err(RunGate::InfraDown(missing));
    }
    let instance_values = match (instance, &facts.instance_values) {
        (Some(instance), Some(stored)) => {
            weft_core::run_spec::instance_run_values(program, instance, stored).map_err(RunGate::InstanceValues)?
        }
        _ => Default::default(),
    };
    let picks = if weft_core::picks::picked_places(program).is_empty() {
        Default::default()
    } else {
        weft_core::picks::run_picks(program, &facts.picks).map_err(RunGate::Picks)?
    };
    Ok(Ready { instance_values, picks })
}

/// How many bytes of JSON what one of a worker's caches keeps may weigh
/// together, whatever one entry weighs: a count of programs bounds nothing
/// when one is a few kilobytes and another tens of megabytes. Each of
/// [`Plans`]' three is held to it: the programs parsed and the tables made
/// of them, both weighed by their program's JSON
/// (`weft_core::content_cache::json_bytes`), and the plans by the cut they
/// keep written out ([`RunPlan::weight`]). A parsed program holds about
/// three times its JSON, so the three hold about 110 MiB at most (less
/// when a table and its program share one parse), a tenth of the 1Gi a
/// copy gets unless its project sets another (`weft workers set --memory`).
/// A program heavier than this on its own is kept anyway, alone in its
/// cache (`ContentCache::weighed`), so a cache holds at most this or that
/// one program, whichever weighs more.
pub(crate) const KEPT_JSON_BYTES: usize = 16 * 1024 * 1024;

/// Which program a table set is for: its definition hash.
type ProgramKey = String;

/// Which plan: the trigger's token, and the versions of the held triggers
/// and run facts it was made from.
type PlanKey = (String, Option<weft_core::instance::InstanceId>, u64, u64);

/// What a worker keeps of its programs and plans (see the module doc).
pub(crate) struct Plans {
    project_id: uuid::Uuid,
    /// What this worker's build left out because it did not compile
    /// (`weft_core::NodeCatalog::left_out`): a fire whose cut uses one of
    /// those node types is refused before it starts.
    left_out: &'static weft_core::builds::LeftOut,
    /// Programs as stored: what is cut and hashed. Each content one names
    /// (`weft_core::content`) is a marker here, put back only in its
    /// tables (`programs`).
    projects: ContentCache<String, ProjectDefinition>,
    programs: ContentCache<ProgramKey, ProgramTables>,
    plans: ContentCache<PlanKey, RunPlan>,
    contents: crate::contents::Contents,
}

impl Plans {
    /// `tenant` is the project's, whose contents its programs name.
    pub(crate) fn new(project_id: uuid::Uuid, tenant: String, left_out: &'static weft_core::builds::LeftOut) -> Self {
        Self {
            project_id,
            left_out,
            contents: crate::contents::Contents::new(tenant),
            projects: ContentCache::weighed(KEPT_JSON_BYTES, json_bytes),
            programs: ContentCache::weighed(KEPT_JSON_BYTES, |tables| json_bytes(&*tables.project)),
            plans: ContentCache::weighed(KEPT_JSON_BYTES, RunPlan::weight),
        }
    }

    /// The program whose definition hash is `definition_hash`: kept, else
    /// fetched from the broker (once, however many runs ask at the same
    /// moment), which reads it from the project's append-only history, so
    /// a run carried on after the project was built again still runs the
    /// shape it started on. One the history does not hold is
    /// [`crate::contents::Gone`].
    pub(crate) async fn project(&self, clients: &crate::context::EngineClients, definition_hash: &str) -> Result<Arc<ProjectDefinition>, LoadFailed> {
        self.projects
            .get_or_load(definition_hash.to_string(), || async {
                clients.project.fetch_definition(self.project_id, definition_hash).await?.ok_or_else(|| {
                    anyhow::Error::new(crate::contents::Gone(format!(
                        "no row in project_definition for project {} hash {definition_hash}: a run was started for a program that was never recorded",
                        self.project_id
                    )))
                })
            })
            .await
    }

    /// `project` as this worker can run it: what its build left out added
    /// to the program's broken parts (`weft_core::builds::LeftOut`), so the
    /// fire's cut refuses a node it has no code for, naming the package.
    fn runnable(&self, project: Arc<ProjectDefinition>) -> Arc<ProjectDefinition> {
        if self.left_out.is_empty() {
            return project;
        }
        let mut runnable = (*project).clone();
        runnable.broken.extend(weft_core::builds::LeftOut::broken_parts(&self.left_out.packages, &project, None));
        Arc::new(runnable)
    }

    /// The tables of `project`, whose definition hash is
    /// `definition_hash`: kept, else made once however many runs ask at
    /// the same moment ([`Self::tables`]). A failure says whether a content
    /// it names is gone (`crate::contents::Gone`).
    pub(crate) async fn program(
        &self,
        clients: &crate::context::EngineClients,
        project: &ProjectDefinition,
        definition_hash: &str,
    ) -> Result<Arc<ProgramTables>, LoadFailed> {
        self.programs.get_or_load(definition_hash.to_string(), || self.tables(clients, project, definition_hash)).await
    }

    /// The tables of `project` (whose definition hash is
    /// `definition_hash`), made of it with every content it names put back
    /// (`crate::contents`), read through the worker's storage unless kept.
    async fn tables(
        &self,
        clients: &crate::context::EngineClients,
        project: &ProjectDefinition,
        definition_hash: &str,
    ) -> anyhow::Result<ProgramTables> {
        let values = self.contents.of(clients.storage.as_ref(), project, definition_hash).await?;
        Ok(ProgramTables::new(Arc::new(crate::contents::runnable(project, &values)?)))
    }

    /// The tables of `cut` (whose definition hash is `definition_hash`),
    /// kept like a program fetched by its hash: a run born on it and one
    /// carried on from its record share them. Only the contents the cut
    /// names are read; a failure says whether one is gone
    /// (`crate::contents::Gone`).
    async fn cut(&self, clients: &crate::context::EngineClients, cut: ProjectDefinition, definition_hash: &str) -> Result<Arc<ProgramTables>, LoadFailed> {
        self.programs
            .get_or_load(definition_hash.to_string(), || async {
                let cut = Arc::new(cut);
                self.projects.put(definition_hash.to_string(), cut.clone());
                self.tables(clients, &cut, definition_hash).await
            })
            .await
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
        if let Some(plan) = self.plans.get(&key) {
            return Ok(plan);
        }
        let project = self.runnable(self.project(clients, &entry.definition_hash).await?);
        let plan = Arc::new(
            RunPlan::build(
                |cut, definition_hash| async move { self.cut(clients, cut, &definition_hash).await },
                &project,
                trigger,
                entry,
                instance,
                &facts.facts,
            )
            .await?,
        );
        if plan.keep {
            self.plans.put(key, plan.clone());
        }
        Ok(plan)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use serde_json::json;

    /// A plan of `trigger` in `project`, armed as a live timer of this
    /// worker's program, over `facts`: what a door's first call builds.
    pub(crate) async fn plan_of(project: &Arc<ProjectDefinition>, trigger: &str, facts: &weft_broker_client::protocol::DoorRunFacts) -> RunPlan {
        plan_for(project, trigger, None, facts).await
    }

    /// [`plan_of`], its runs for `instance`.
    pub(crate) async fn plan_for(
        project: &Arc<ProjectDefinition>,
        trigger: &str,
        instance: Option<&weft_core::instance::InstanceId>,
        facts: &weft_broker_client::protocol::DoorRunFacts,
    ) -> RunPlan {
        let (held, entry) = armed(trigger);
        RunPlan::build(|cut, _| async move { Ok(Arc::new(ProgramTables::new(Arc::new(cut)))) }, project, &held, &entry, instance, facts)
            .await
            .expect("a plan whose tables are made in memory reads nothing")
    }

    /// A fake storage, and engine clients reading contents through it.
    fn storage_clients() -> (Arc<crate::storage::FakeWorkerStorage>, crate::context::EngineClients) {
        use crate::run_driver::engine_test_rig::{clients, MemJournal};
        let storage = crate::storage::FakeWorkerStorage::new();
        let clients = crate::context::EngineClients { storage: storage.clone(), ..clients(Arc::new(MemJournal::default())) };
        (storage, clients)
    }

    /// What `plans` answers for a fire of `trigger` ([`armed`]), over no
    /// run facts.
    async fn planned(plans: &Plans, clients: &crate::context::EngineClients, trigger: &str) -> anyhow::Result<Arc<RunPlan>> {
        let (held, entry) = armed(trigger);
        plans.plan_over(clients, &held, &entry, None, 1, &crate::door::RunFacts::of(Default::default())).await
    }

    /// `trigger` armed as a live timer of the program whose definition hash
    /// is `def-1`.
    fn armed(trigger: &str) -> (Arc<HeldTrigger>, Arc<ArmedEntry>) {
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
        let held = Arc::new(HeldTrigger::new(format!("token-{trigger}"), trigger.into(), crate::door::HeldEntry::Armed(entry.clone()), None));
        (held, entry)
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
                    { "id": "a", "nodeType": "Pass", "position": { "x": 1.0, "y": 0.0 },
                      "inputs": [{ "name": "value", "portType": "String", "required": true }],
                      "outputs": [{ "name": "value", "portType": "String", "required": false }] },
                    { "id": "b", "nodeType": "Pass", "position": { "x": 2.0, "y": 0.0 },
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

    /// A trigger feeding `a` inside group `g` (through its input `x`), an
    /// infra node `db` whose baked `url` feeds `a` too (through `g.u`), and
    /// `b` past the group reading `a` (through `g.y`), and a literal on
    /// `g`'s input `k` that `a` reads (so the cut keeps `g`: it sends `k` as
    /// it opens). `g` has no gate: it opens on the first value that reaches
    /// it from outside.
    fn baked_group() -> Arc<ProjectDefinition> {
        let string = |name: &str, required: bool| json!({ "name": name, "portType": "String", "required": required });
        Arc::new(
            serde_json::from_value(json!({
                "id": uuid::Uuid::from_u128(11),
                "nodes": [
                    { "id": "trig", "nodeType": "Trig", "position": { "x": 0.0, "y": 0.0 },
                      "outputs": [string("value", false)], "features": { "isTrigger": true } },
                    { "id": "db", "nodeType": "Db", "position": { "x": 0.0, "y": 1.0 },
                      "outputs": [string("url", false)], "requiresInfra": true, "bakedOutputs": ["url"] },
                    { "id": "a", "nodeType": "Pass", "position": { "x": 1.0, "y": 0.0 }, "scope": ["g"],
                      "inputs": [string("value", true), string("url", true), string("k", true)], "outputs": [string("value", false)] },
                    { "id": "b", "nodeType": "Pass", "position": { "x": 2.0, "y": 0.0 },
                      "inputs": [string("value", true)], "outputs": [string("value", false)] }
                ],
                "edges": [
                    { "id": "e1", "source": "trig", "target": "a", "sourceHandle": "value", "targetHandle": "value",
                      "via": [{ "group": "g", "port": "x", "side": "in" }] },
                    { "id": "e2", "source": "db", "target": "a", "sourceHandle": "url", "targetHandle": "url",
                      "via": [{ "group": "g", "port": "u", "side": "in" }] },
                    { "id": "e3", "source": "a", "target": "b", "sourceHandle": "value", "targetHandle": "value",
                      "via": [{ "group": "g", "port": "y", "side": "out" }] },
                    { "id": "e4", "source": "g", "target": "a", "sourceHandle": "k", "targetHandle": "k",
                      "via": [{ "group": "g", "port": "k", "side": "in" }] }
                ],
                "groups": [{ "id": "g", "kind": "group", "nodeIds": ["a"], "portLiterals": { "k": "lit" },
                             "inPorts": [string("x", false), string("u", false), string("k", false)], "outPorts": [string("y", false)] }]
            }))
            .unwrap(),
        )
    }

    /// A plan weighs the cut it keeps written out, byte for byte, and one
    /// that cannot start keeps none.
    #[tokio::test]
    async fn a_plan_weighs_the_cut_it_keeps() {
        let project = chain();
        let plan = plan_of(&project, "trig", &Default::default()).await;
        let Ok(start) = &plan.start else { panic!("a chain's fire starts") };
        let cut = serde_json::to_string(&*start.program.project).unwrap();
        assert_eq!(plan.weight(), std::mem::size_of::<RunPlan>() + cut.len());
        assert_eq!(json_bytes(&*start.program.project), cut.len());
        let refused = plan_of(&project, "a", &Default::default()).await;
        assert!(refused.start.is_err());
        assert_eq!(refused.weight(), std::mem::size_of::<RunPlan>());
    }

    /// A run started from a plan, once its drive's first pass over its
    /// scopes ran, holds exactly what the fold of its own birth holds: the
    /// roots kicked (the firing one with the call's payload), the baked
    /// value its stand-in sent, the group that value opened, the same
    /// part of the program.
    #[tokio::test]
    async fn a_run_from_a_plan_starts_where_the_fold_of_its_birth_does() {
        let project = baked_group();
        let facts = weft_broker_client::protocol::DoorRunFacts {
            infra: vec![weft_core::infra::run_gate::InfraCopyUp {
                node_id: "db".into(),
                instance: None,
                running: true,
                baked: [("url".to_string(), json!("postgres://db"))].into(),
            }],
            ..Default::default()
        };
        let plan = plan_of(&project, "trig", &facts).await;
        let startable = plan.start.as_ref().map_err(|_| "unready").expect("a plan that starts");
        let program = &startable.program;
        let run_id = RunId::from_u128(4);
        let payload = json!({ "tick": 1 });
        let birth = startable.birth(&plan, run_id, &payload, 7);
        let mut kicked = startable.opening(&payload);
        let mut pulses = PulseTable::new();
        let mut scopes = weft_core::run::scope::ScopeTable::default();
        weft_core::run::scope::settle_table(
            &program.project, &program.index, &startable.given, weft_core::context::Phase::Fire, run_id, 7, &mut pulses, &mut scopes, &mut kicked,
        );
        let folded = weft_journal::fold_to_snapshot(run_id, program.project.clone(), &birth);
        assert!(folded.corruptions.is_empty(), "{:?}", folded.corruptions);
        let kicks = |kicked: &weft_core::primitive::KickTable| {
            let mut kicks: Vec<(String, String)> =
                kicked.iter().map(|(at, kick)| (format!("{at:?}"), serde_json::to_string(kick).unwrap())).collect();
            kicks.sort();
            kicks
        };
        assert_eq!(kicks(&kicked), kicks(&folded.kicked));
        // On their wires and taken, each said apart.
        let wires = |pulses: &PulseTable| -> Vec<String> {
            let mut wires: Vec<String> = pulses.values().flatten().map(|pulse| format!("{pulse:?}"))
                .chain(pulses.all_absorbed().map(|pulse| format!("absorbed {pulse:?}")))
                .collect();
            wires.sort();
            wires
        };
        assert_eq!(wires(&pulses), wires(&folded.pulses));
        let decided = |scopes: &weft_core::run::scope::ScopeTable| -> Vec<String> {
            scopes.iter().map(|(copy, record)| format!("{copy:?} {:?}", record.state)).collect()
        };
        assert_eq!(decided(&scopes), decided(&folded.scopes));
        // What the two agree on is the run's real start: the baked url is
        // on its way to `a`, and it opened `g`.
        let url = pulses.get("a").into_iter().flatten().find(|pulse| pulse.target_port == "url").expect("the baked url reached a");
        assert_eq!(*url.value, json!("postgres://db"));
        assert_eq!(
            scopes.get(&weft_core::project::scopes::ScopeCopy::new("g", vec![])).map(|record| record.state.clone()),
            Some(weft_core::run::scope::ScopeState::Open),
        );
        let k = pulses.get("a").into_iter().flatten().find(|pulse| pulse.target_port == "k").expect("g sent its literal as it opened");
        assert_eq!(*k.value, json!("lit"));
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
        let program = ProgramTables::new(project.clone());
        let run_id = RunId::from_u128(5);
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
                let pulse = if next(4) == 0 {
                    weft_core::pulse::Pulse::closure(id, run_id, Vec::new(), node, port)
                } else {
                    weft_core::pulse::Pulse::new(id, run_id, Vec::new(), node, port, Arc::new(json!("x")))
                };
                pulses.entry(node.to_string()).or_default().push(pulse);
                if next(3) == 0 {
                    pulses.absorb(node, |pulse| pulse.id == id);
                }
            }
            let asked: Vec<(String, Vec<uuid::Uuid>)> = program.ready(&pulses, &Default::default()).into_iter().map(|(id, group)| (id, group.pulse_ids)).collect();
            let scanned: Vec<(String, Vec<uuid::Uuid>)> = weft_core::run::find_ready_nodes(&project, &pulses, &program.index, &Default::default())
                .into_iter()
                .map(|(id, group)| (id, group.pulse_ids))
                .collect();
            assert_eq!(asked, scanned);
        }
    }

    /// The inputs a firing is told its node declares are the node's own:
    /// the gates are the language's, so a List with no item ports sees
    /// none, as it does in the node-test rig.
    #[test]
    fn a_nodes_declared_inputs_leave_out_the_gates() {
        let project: Arc<ProjectDefinition> = Arc::new(
            serde_json::from_value(json!({
                "id": uuid::Uuid::from_u128(11),
                "nodes": [
                    { "id": "n", "nodeType": "N", "position": { "x": 0.0, "y": 0.0 },
                      "inputs": [
                        { "name": "item", "portType": "String", "required": true },
                        { "name": weft_core::run::skip::SHOULD_FLOW_PORT, "portType": "T__should_flow", "required": false },
                        { "name": weft_core::run::skip::SHOULD_NOT_FLOW_PORT, "portType": "T__should_not_flow", "required": false }
                      ] }
                ],
                "edges": []
            }))
            .unwrap(),
        );
        let program = ProgramTables::new(project);
        let ports = program.ports("n", &Vec::new()).expect("the node's ports");
        assert_eq!(ports.inputs.keys().collect::<Vec<_>>(), vec!["item"]);
    }

    /// A node an included file shares between sites hands each firing
    /// the types of the place its frames name, and its own elsewhere.
    #[test]
    fn a_firing_gets_the_ports_of_its_place() {
        let project: Arc<ProjectDefinition> = Arc::new(
            serde_json::from_value(json!({
                "id": uuid::Uuid::from_u128(12),
                "nodes": [
                    { "id": "n", "nodeType": "N", "position": { "x": 0.0, "y": 0.0 },
                      "inputs": [{ "name": "file", "portType": "Image", "required": true }],
                      "outputs": [{ "name": "file", "portType": "Image", "required": true }],
                      "placeTypes": [
                        { "place": ["img"], "inputs": { "file": "Image" }, "outputs": { "file": "Image" } },
                        { "place": ["vid"], "inputs": { "file": "Video" }, "outputs": { "file": "Video" } }
                      ] }
                ],
                "edges": []
            }))
            .unwrap(),
        );
        let program = ProgramTables::new(project);
        let at = |site: &str| {
            let frames = vec![weft_core::frames::Frame::Call { site: site.into() }];
            program.ports("n", &frames).expect("the node's ports").outputs["file"].clone()
        };
        assert_eq!(at("img"), WeftType::parse("Image").unwrap());
        assert_eq!(at("vid"), WeftType::parse("Video").unwrap());
        assert_eq!(program.ports("n", &Vec::new()).unwrap().outputs["file"], WeftType::parse("Image").unwrap());
    }

    /// A worker whose build left a package out refuses, before it starts,
    /// a fire whose cut uses one of its node types, naming the package and
    /// its first error; a worker built whole plans the same fire.
    #[tokio::test]
    async fn a_fire_reaching_a_left_out_package_is_refused_up_front() {
        let left_out: &'static weft_core::builds::LeftOut = Box::leak(Box::new(weft_core::builds::LeftOut {
            packages: vec![weft_core::builds::LeftOutPackage {
                package: "relay".into(),
                node_types: vec!["Pass".into()],
                error: "error[E0425]: cannot find value `x`".into(),
            }],
        }));
        let whole = Plans::new(uuid::Uuid::from_u128(9), "t1".into(), Box::leak(Box::default()));
        assert!(!matches!(plan_of(&whole.runnable(chain()), "trig", &Default::default()).await.start, Err(Unready::Refused(_))));
        let missing = Plans::new(uuid::Uuid::from_u128(9), "t1".into(), left_out);
        let refused = plan_of(&missing.runnable(chain()), "trig", &Default::default()).await;
        assert!(
            matches!(&refused.start, Err(Unready::Refused(why)) if why.contains("package 'relay' didn't compile: error[E0425]")),
            "{:?}",
            refused.start.as_ref().err()
        );
    }

    /// The tables a run runs have every content their program names put
    /// back (`crate::contents`), while the program kept for cutting and
    /// hashing keeps its marker; two versions naming one content read it
    /// once.
    #[tokio::test]
    async fn a_programs_tables_read_the_contents_it_names_once() {
        let (storage, clients) = storage_clients();
        let sql = "SELECT 1;\n".repeat(10_000);
        storage.hold_content(sql.as_bytes());
        let (query, _) =
            weft_core::content::ContentRef::of_text(&sql, &WeftType::Primitive(weft_core::WeftPrimitive::String), "sql/q.sql").unwrap();
        let version = |label: &str| {
            let mut program = (*chain()).clone();
            program.nodes[1].label = Some(label.into());
            program.nodes[1].port_literals.insert("query".into(), query.to_value());
            program
        };
        let plans = Plans::new(uuid::Uuid::from_u128(9), "t1".into(), Box::leak(Box::default()));
        for (hash, program) in [("v1", version("one")), ("v2", version("two"))] {
            let tables = plans.program(&clients, &program, hash).await.unwrap();
            assert_eq!(tables.project.nodes[1].port_literals["query"], json!(sql), "the run reads the content");
            assert_eq!(program.nodes[1].port_literals["query"], query.to_value(), "the program keeps its marker");
        }
        assert_eq!(storage.content_reads().len(), 1, "one content, read once: {:?}", storage.content_reads());
    }

    /// A fire reads only the contents its own cut names: a file only
    /// another trigger's runs read is never read for it, and its being
    /// gone refuses only that other trigger's fires, naming the file and
    /// the way back. That refusal is not kept: the next fire reads again.
    #[tokio::test]
    async fn a_fire_reads_only_the_contents_its_cut_names() {
        let (storage, clients) = storage_clients();
        let (gone, _) =
            weft_core::content::ContentRef::of_text("SELECT 2;", &WeftType::Primitive(weft_core::WeftPrimitive::String), "sql/gone.sql").unwrap();
        let string = |name: &str| json!({ "name": name, "portType": "String", "required": false });
        let mut project: ProjectDefinition = serde_json::from_value(json!({
            "id": uuid::Uuid::from_u128(9),
            "nodes": [
                { "id": "ta", "nodeType": "Trig", "position": { "x": 0.0, "y": 0.0 },
                  "outputs": [string("value")], "features": { "isTrigger": true } },
                { "id": "a", "nodeType": "Pass", "position": { "x": 1.0, "y": 0.0 }, "inputs": [string("value")] },
                { "id": "tb", "nodeType": "Trig", "position": { "x": 0.0, "y": 1.0 },
                  "outputs": [string("value")], "features": { "isTrigger": true } },
                { "id": "b", "nodeType": "Query", "position": { "x": 1.0, "y": 1.0 }, "inputs": [string("value"), string("query")] }
            ],
            "edges": [
                { "id": "e1", "source": "ta", "target": "a", "sourceHandle": "value", "targetHandle": "value" },
                { "id": "e2", "source": "tb", "target": "b", "sourceHandle": "value", "targetHandle": "value" }
            ]
        }))
        .unwrap();
        project.nodes[3].port_literals.insert("query".into(), gone.to_value());
        let plans = Plans::new(uuid::Uuid::from_u128(9), "t1".into(), Box::leak(Box::default()));
        plans.projects.put("def-1".into(), Arc::new(project));
        let a = planned(&plans, &clients, "ta").await.unwrap();
        assert!(a.start.is_ok(), "{:?}", a.start.as_ref().err());
        assert!(storage.content_reads().is_empty(), "a's cut names no content: {:?}", storage.content_reads());
        for read in 1..=2 {
            let b = planned(&plans, &clients, "tb").await.unwrap();
            let Err(Unready::Refused(why)) = &b.start else { panic!("b's cut names a gone file: {:?}", b.start.as_ref().err()) };
            assert!(why.contains("reads 'sql/gone.sql' (String)") && why.contains("weft resync"), "{why}");
            assert_eq!(storage.content_reads().len(), read, "a refusal for a gone file is not kept");
        }
        assert!(planned(&plans, &clients, "ta").await.unwrap().start.is_ok(), "b's file being gone leaves a's fires alone");
    }

    /// A fire whose cut names a content the store could not be asked for
    /// is not refused: no plan is made, the error comes back (the door
    /// answers it as "try again in a moment"), and the next fire once the
    /// store answers again plans.
    #[tokio::test]
    async fn a_fire_whose_contents_could_not_be_read_is_tried_again() {
        let (storage, clients) = storage_clients();
        let sql = "SELECT 1;";
        storage.hold_content(sql.as_bytes());
        let (query, _) =
            weft_core::content::ContentRef::of_text(sql, &WeftType::Primitive(weft_core::WeftPrimitive::String), "sql/q.sql").unwrap();
        let mut project = (*chain()).clone();
        project.nodes[1].port_literals.insert("query".into(), query.to_value());
        let plans = Plans::new(uuid::Uuid::from_u128(9), "t1".into(), Box::leak(Box::default()));
        plans.projects.put("def-1".into(), Arc::new(project));
        storage.contents_down(true);
        let Err(e) = planned(&plans, &clients, "trig").await else { panic!("an outage makes no plan, refused or not") };
        assert!(format!("{e:#}").contains("reads 'sql/q.sql' (String)") && !format!("{e:#}").contains("weft resync"), "{e:#}");
        storage.contents_down(false);
        let plan = planned(&plans, &clients, "trig").await.unwrap();
        let Ok(start) = &plan.start else { panic!("the store answers again: {:?}", plan.start.as_ref().err()) };
        assert_eq!(start.program.project.nodes[1].port_literals["query"], json!(sql));
    }

    /// A plan of a node that is no trigger never starts a run, and says why.
    #[tokio::test]
    async fn a_plan_of_no_trigger_refuses() {
        let plan = plan_of(&chain(), "a", &Default::default()).await;
        assert!(matches!(plan.start, Err(Unready::Refused(_))));
    }

    /// A run that reaches a node per instance is planned for the instance
    /// its call names: with none it is refused, naming the node, and with
    /// one its plan (and the birth of its runs) is for that instance.
    #[tokio::test]
    async fn a_plan_is_for_the_instance_its_call_names() {
        let mut project = (*chain()).clone();
        project.nodes[1].per_instance = Some(weft_core::instance::PerInstance::Marked);
        let project = Arc::new(project);
        let refused = plan_of(&project, "trig", &Default::default()).await;
        assert!(matches!(&refused.start, Err(Unready::Refused(why)) if why.contains("once per instance")), "{:?}", refused.start.as_ref().err());
        let ada = weft_core::instance::InstanceId::new("ada").unwrap();
        let plan = plan_for(&project, "trig", Some(&ada), &Default::default()).await;
        assert_eq!(plan.instance, Some(ada));
        assert!(!matches!(plan.start, Err(Unready::Refused(_))), "{:?}", plan.start.as_ref().err());
    }
}
