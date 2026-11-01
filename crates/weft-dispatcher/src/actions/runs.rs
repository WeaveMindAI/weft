//! Starting a run, the one way a run starts from the CLI, the editor or a
//! program: it reads what the run starts with from the version it runs
//! (`crate::build::assets::run_spec`), resolves the spec
//! (`weft_core::run_spec`), decides what a seeded run inherits
//! (`weft_core::seeding`), journals the birth rows through
//! `project::start_queued_run_with` exactly as every other start does, and
//! records the run under its version, moving its working copy's head
//! there. A plain `weft run` is this action with no seed and no scope.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use axum::http::StatusCode;
use serde_json::Value;
use weft_core::frames::Located;
use weft_core::run_spec::{resolve_seeded, resolve_spec, Refusal, RunSpec};
use weft_core::seeding::{inheritable_nodes, seed_plan, SeedOutcome};
use weft_core::versions::{RunStarted, RunStarting, StartRun, StartingRun, VersionRunRequest};
use weft_core::RunId;

use super::{internal, with_tree_lock, Actor, Refused};
use crate::api::project::{coherent_definition, require_action};
use crate::state::DispatcherState;
use crate::versions::{resolve_seed, SeedChoice};

/// This area's actions ([`super::names`]).
pub const NAMES: &[(&str, super::Reach)] = &[
    ("weft.runs.start", super::Reach::Everyone),
];

/// What the resolver refused, as the 422 body: JSON, so the CLI prints
/// every line and the editor makes a field per missing input.
fn refused(refusal: &Refusal) -> Refused {
    crate::api::project::refusal_error(refusal)
}

/// Every settled run of a project: ended, or parked on a wait. Asked
/// once per project, never per run.
async fn settled_run_ids(
    state: &DispatcherState,
    project: uuid::Uuid,
) -> Result<std::collections::HashSet<RunId>, Refused> {
    state.runs.settled_run_ids_for_project(project).await.map_err(|e| internal("settled runs", e))
}

/// The seed run folded: the complete results it holds, by place (its
/// own, and those it reused from earlier runs, each from the run that made
/// it), and the places that waited in it with the fingerprint of their
/// code there.
struct SeededFrom {
    run_id: RunId,
    outcomes: BTreeMap<Located, SeedOutcome>,
    answered: BTreeMap<Located, weft_core::project::hash::SourceHash>,
}

/// The registered program a run was run against (the one its cut was cut
/// from, or its own: `DispatcherState::registered_identity`) and that
/// program's identity: what a seed holds the run's results against.
async fn registered_of(
    state: &DispatcherState,
    project: uuid::Uuid,
    run_id: RunId,
    snapshot: &weft_core::primitive::RunSnapshot,
) -> Result<(Arc<weft_core::ProjectDefinition>, weft_core::project::hash::ProgramIdentity), Refused> {
    let (Some(definition_hash), Some(binary_hash)) = (snapshot.definition_hash.as_deref(), snapshot.binary_hash.as_deref()) else {
        return Err(internal("seed identity", format!("run {run_id} has no production code identity")));
    };
    let identity = state.registered_identity(run_id, project, definition_hash, snapshot.cut_from.as_deref(), binary_hash).await
        .map_err(|e| internal("seed identity", e))?;
    let hash = &identity.definition_hash;
    let registered = state.program(project, hash).await.map_err(|e| internal("seed program", e))?
        .ok_or_else(|| internal("seed program", format!("run {run_id} ran against program {hash}, which this project no longer records")))?;
    Ok((registered, identity))
}

/// Fold `run_id`, a settled run of `project`, for a seed (see
/// [`SeededFrom`]).
async fn fold_seed(state: &DispatcherState, project: uuid::Uuid, run_id: RunId) -> Result<SeededFrom, Refused> {
    let sources = crate::projection::reconstruct_run(state, run_id).await.map_err(|e| internal("seed sources", e))?;
    let fold = &sources[&run_id];
    if fold.project().id != project {
        return Err((StatusCode::NOT_FOUND, "seed is outside this project".into()));
    }
    let snapshot = fold.snapshot();
    let outputs = fold.output_wires().map_err(|e| internal("seed outputs", e))?;
    let live_nodes: BTreeSet<Located> = outputs.iter().filter(|wire| weft_core::weft_type::WeftType::contains_bus_handle(&wire.value))
        .map(|wire| wire.place()).collect();
    // Per run a result comes from: its slices over the program it was run
    // against, and its output history.
    let mut slices: BTreeMap<RunId, BTreeMap<Located, weft_core::project::hash::SourceHash>> = BTreeMap::new();
    let mut histories: BTreeMap<RunId, Vec<weft_core::run_spec::OutputWire>> = BTreeMap::new();
    let mut outcomes = BTreeMap::new();
    for place in inheritable_nodes(fold.project(), snapshot) {
        let origin = snapshot.inherited_origins.get(&place).copied().unwrap_or(run_id);
        let source = sources.get(&origin).ok_or_else(|| internal("seed origin", format!("missing run {origin}")))?;
        let (original, graph) = (source.snapshot(), source.project());
        if let std::collections::btree_map::Entry::Vacant(slice) = slices.entry(origin) {
            let (registered, identity) = registered_of(state, project, origin, original).await?;
            slice.insert(identity.slice_hashes(&registered).map_err(|e| internal("seed identity", e))?);
            histories.insert(origin, source.output_wires().map_err(|e| internal("seed outputs", e))?);
        }
        let declaration = graph.nodes.iter().find(|n| n.id == place.id).ok_or_else(|| internal("seed node", &place.id))?;
        let slice_hash = slices[&origin].get(&place).ok_or_else(|| internal("seed identity", format!("run {origin} has no slice for {place}")))?.clone();
        outcomes.insert(place.clone(), SeedOutcome {
            origin,
            slice_hash,
            handed: original.given.inputs.get(&place).cloned().unwrap_or_default(),
            gates: weft_core::seeding::gates_handed_around(&original.given, graph, &place),
            emitted_live_handle: live_nodes.contains(&place),
            fire: original.kicked.iter().find(|(location, kick)| Located::at(&location.node_id, &location.frames) == place && kick.firing)
                .map(|(_, kick)| kick.payload.clone().unwrap_or(Value::Null)),
            outputs: weft_core::seeding::sent_ports(declaration, &place, &histories[&origin]),
        });
    }
    // The places that waited here and were answered, each with its code's
    // fingerprint: what a run that runs one again replays.
    let mut answered = BTreeMap::new();
    let waited: BTreeSet<Located> = snapshot.awaited_sequences.iter()
        .filter(|(_, entries)| !weft_core::primitive::answered_steps(entries).is_empty())
        .map(|(at, _)| Located::at(&at.node_id, &at.frames))
        .collect();
    if !waited.is_empty() {
        let (registered, identity) = registered_of(state, project, run_id, snapshot).await?;
        for place in waited {
            answered.insert(place.clone(), identity.node_hash(&registered, &place.id).map_err(|e| internal("seed answers", e))?);
        }
    }
    Ok(SeededFrom { run_id, outcomes, answered })
}

/// Refuse a seed that would hand this run an output naming a stored file
/// the file has moved on from: a reused node's saved result says what the
/// node made, and a file edited since (by a later node, by another run)
/// or gone no longer holds it. Only what stands in for itself here
/// (`stand_ins`) hands this run anything; a place reused and read by
/// nothing that runs hands it nothing. Every such output is named at
/// once, so one refusal says everything to hand in or re-run. Each file
/// is one look at the store, said first ([`RunStarting::CheckingFiles`]).
async fn refuse_moved_files(
    state: &DispatcherState,
    project: &weft_core::ProjectDefinition,
    stand_ins: &BTreeMap<Located, weft_core::project::StandIn>,
    starting: &Starting<'_>,
) -> Result<(), Refused> {
    let mut handed = Vec::new();
    for (place, stand_in) in stand_ins {
        for (port, given) in &stand_in.ports {
            let weft_core::project::GivenPort::Value { value } = given else { continue };
            handed.extend(weft_core::storage::stored_files_within(value).into_iter().map(|file| (place, port, file)));
        }
    }
    if handed.is_empty() {
        return Ok(());
    }
    starting.at(RunStarting::CheckingFiles { files: handed.len() }).await;
    let mut refusals = Vec::new();
    for (place, port, file) in handed {
        let now = match crate::storage::file_meta(state, &file.key).await {
            Ok(meta) => weft_core::seeding::FileNow::At(meta.version),
            Err(e) if e.downcast_ref::<crate::storage::StorageNotFound>().is_some() => weft_core::seeding::FileNow::Gone,
            Err(e) => return Err(internal("read a reused output's file", e)),
        };
        let node = weft_core::project::address_of(project, &place.id, &place.path);
        refusals.extend(weft_core::seeding::moved_file_refusal(&node, port, &file, now));
    }
    if refusals.is_empty() {
        return Ok(());
    }
    Err((StatusCode::CONFLICT, refusals.join("\n")))
}

/// Where a run being started says it is ([`RunStarting`]): on its task,
/// for the person following it, or nowhere for a start nobody follows (a
/// program's, which waits on its own call).
pub(crate) enum Starting<'a> {
    Task { pool: &'a weft_task_store::db::Pool, task: uuid::Uuid },
    Unfollowed,
}

impl Starting<'_> {
    /// Say the start is at `stage`. A line for the person following, so a
    /// write the database did not take is logged and the start goes on:
    /// it is never a reason to refuse the run.
    async fn at(&self, stage: RunStarting) {
        let Starting::Task { pool, task } = self else { return };
        let said = match serde_json::to_value(&stage) {
            Ok(stage) => weft_task_store::tasks::set_progress(pool, *task, &stage).await,
            Err(e) => Err(e.into()),
        };
        if let Err(e) = said {
            tracing::warn!(target: "weft_dispatcher::runs", %task, error = %format!("{e:#}"), "where a run's start is could not be said");
        }
    }
}

/// `POST /projects/{id}/versions/runs`: hand a person's run to its task
/// ([`crate::task_kinds::start_run`]), under the id the ask carries, and
/// answer that task to follow. What a run can be refused at once (the
/// project is not the asker's, or it is the asking program's own) is
/// refused here; the task holds every other check, and reads them again.
/// The same ask sent again joins its task.
pub async fn start(state: &DispatcherState, actor: &Actor, id: uuid::Uuid, ask: &StartRun) -> Result<StartingRun, Refused> {
    actor.may_act_on(state, id).await?;
    refuse_own_run(actor, id)?;
    let task = crate::task_kinds::start_run::task(actor.tenant().as_str(), id, ask)?;
    match weft_task_store::tasks::enqueue_as(&state.pg_pool, ask.task, task)
        .await
        .map_err(|e| crate::api::unavailable_or_internal("hand the run to its task", e))?
    {
        weft_task_store::tasks::Enqueued::Inserted | weft_task_store::tasks::Enqueued::Joined => Ok(StartingRun { task: ask.task }),
        weft_task_store::tasks::Enqueued::Refused(why) => Err((StatusCode::CONFLICT, why)),
    }
}

/// A program starting a run of its own project starts one more run each
/// time it runs, and nothing stops the chain: refused by name, the way a
/// loop that never ends is impossible to write. Asked by the run itself
/// and by the build for one ([`super::build::build`]), so the refusal
/// comes before anything is stamped or built.
pub(crate) fn refuse_own_run(actor: &Actor, project: uuid::Uuid) -> Result<(), Refused> {
    if !actor.is_own_project(project) {
        return Ok(());
    }
    Err((
        StatusCode::FORBIDDEN,
        "a program cannot start a run of its own project: every run of it would start the next, with \
         nothing to end the chain. Start the run from another project, or reach the part you want with \
         a trigger"
            .into(),
    ))
}

/// Start a run that records itself in the tree, from a working copy, of
/// a version a build registered (see the module doc), as `run_id`, saying
/// where it is on the way when somebody follows it (`starting`). The same
/// start asked again under its id (an ask, a program's call, a task's
/// claim rescued: each names the run before it starts) answers the run
/// it started ([`started_before`]) instead of starting another.
pub(crate) async fn run(
    state: &DispatcherState,
    actor: &Actor,
    id: uuid::Uuid,
    body: &VersionRunRequest,
    run_id: RunId,
    starting: &Starting<'_>,
) -> Result<RunStarted, Refused> {
    actor.may_act_on(state, id).await?;
    refuse_own_run(actor, id)?;
    if let Some(started) = started_before(state, id, run_id, body.working_copy).await? {
        return Ok(started);
    }
    crate::maintenance::refuse_while_on(&state.pg_pool).await?;
    require_action(state, id, &["run"]).await?;
    if (!body.seed_until.is_empty() || !body.seed_before.is_empty()) && !body.seed && body.seed_run.is_none() {
        return Err((StatusCode::BAD_REQUEST, "`--seed-until` and `--seed-before` need `--seed`".into()));
    }
    let (program, project) = coherent_definition(state, id).await?;
    let definition_hash = program.definition_hash.clone();
    if definition_hash != body.definition_hash {
        return Err((
            StatusCode::CONFLICT,
            format!(
                "the project's definition moved to {definition_hash} since this run was prepared \
                 (it registered {}); run again",
                body.definition_hash
            ),
        ));
    }
    if program.binary_hash != body.binary_hash {
        return Err((StatusCode::CONFLICT, "the project's implementation changed since this run was prepared; run again".into()));
    }
    // The version is read now for what the run starts with, and held again
    // in the locked breath that records the run: a prune between the two
    // would leave the run under a version that is gone.
    let version = state
        .versions
        .version(id, &body.version)
        .await
        .map_err(|e| internal("version", e))?
        .ok_or((StatusCode::NOT_FOUND, format!("no version {} in this project; build again", body.version)))?;
    let copy = super::versions::copy_of(state, id, body.working_copy).await?;
    // Head for the SEED choice. The head the run moves is read again
    // inside the lock, where it cannot be stale.
    let head = copy.head.clone();
    let seed_run: Option<RunId> = match (body.seed, body.seed_run) {
        // Any settled run of the project: one a trigger started, one that
        // failed, one of any version.
        (_, Some(run_id)) => {
            if !settled_run_ids(state, id).await?.contains(&run_id) {
                return Err((
                    StatusCode::CONFLICT,
                    format!("run {run_id} is not a settled run of this project (it is still going, or it is not this project's): `weft stop {run_id}` or wait, then run again"),
                ));
            }
            Some(run_id)
        }
        (true, None) => {
            let versions = state.versions.versions(id).await.map_err(|e| internal("versions", e))?;
            let runs = state.versions.runs(id).await.map_err(|e| internal("runs", e))?;
            let settled = settled_run_ids(state, id).await?;
            match resolve_seed(&head, &versions, &runs, |c| settled.contains(&c)) {
                SeedChoice::Run(run_id) => Some(run_id),
                SeedChoice::Nothing => None,
                SeedChoice::HeadRunning(run_id) => {
                    return Err((
                        StatusCode::CONFLICT,
                        format!("head {run_id} is still running; `weft stop {run_id}` or wait, then run again"),
                    ));
                }
            }
        }
        _ => None,
    };

    // What the run starts with, read from the version it runs, every
    // `@file` and `@asset` in it resolved from that version's files.
    let reach = crate::build::assets::Reach::of(state, id, &version.manifest, Some(&copy)).await?;
    let spec = crate::build::assets::run_spec(&reach, &body.shape).await?.unwrap_or_else(|| RunSpec::whole("run"));
    // A run reads its infra's baked outputs as the dispatcher holds its
    // copies; an infra node it reads nothing but saved values of does not
    // run.
    let saved = crate::api::project::saved_for_run(state, id, &project, spec.instance.as_ref()).await?;
    let authored = resolve_spec(&spec, &project, &saved).map_err(|r| refused(&r))?;
    let bakes = if spec.fire.is_some() {
        // An instance's trigger was baked with that instance's values: the
        // fire replays the instance's bake.
        state.bakes.trigger_bakes(id, spec.instance.as_ref()).await.map_err(|error| internal("read trigger bakes", error))?
    } else { crate::store::Bakes::default() };
    // A fired CALLER trigger (a Route) gets a stand-in caller: there is
    // no socket coming, so the run serves the body the author typed and
    // the program's answer goes to the journal. Without it a route's
    // program is unrunnable offline, because the trigger and every
    // Reply behind it ask for a caller that is not there.
    //
    // The trigger's signal spec comes from its BAKE, the same capture
    // the port snapshot below comes from, so the dispatcher needs no
    // knowledge of what a Route is: whether this trigger speaks to a
    // caller at all is the signal kind's own declaration
    // (`weft_core::signal::caller_protocol`).
    let mut stand_in: Option<weft_core::primitive::SignalSpec> = None;
    // The trigger `--fire` names, spelled: the run is that trigger's.
    let mut fired_trigger: Option<String> = None;
    // What the fired trigger's setup captured on its ports, which its
    // firing kick replays.
    let mut fired_ports: Option<Value> = None;
    // How the run is kept: a fire starts the way its trigger says (the
    // spec its bake captured), with what the run asks for by hand over it.
    let mut kept_by_trigger = weft_core::run_settings::RunSettings::default();
    for kick in authored.kicks.iter().filter(|kick| kick.firing) {
        // A bake captures a trigger under its place (`one.door` for the
        // `door` an include site `one` reaches), so the kick is spelled
        // through its call frames before the lookup.
        let address = crate::api::project::kick_place(&project, kick);
        fired_trigger = Some(address.clone());
        // This program's bake no longer reads (another program's, which
        // the fire does not replay, is read past): what it captured is
        // gone, and a setup of the trigger makes it again. `weft bake` does
        // that whether the trigger is on or off, and changes nothing that
        // listens.
        if let Some(unreadable) = bakes.unreadable_for(&program) {
            return Err((
                StatusCode::CONFLICT,
                format!("{}; `weft bake` prepares trigger '{address}' again and replaces it", unreadable.error.trim_end_matches('.')),
            ));
        }
        weft_core::run_spec::validate_fire_bake(&address, &program, &bakes.read.iter().map(|bake| bake.summary()).collect::<Vec<_>>())
            .map_err(|refusal| refused(&refusal))?;
        let capture = bakes.read.iter().find(|bake| bake.program == program)
            .and_then(|bake| bake.captured.get(&address))
            .expect("validated bake contains this trigger for this program");
        fired_ports = Some(capture.ports.clone());
        kept_by_trigger = capture.spec.settings;
        if weft_core::signal::caller_protocol(&capture.spec.kind).is_some() {
            let payload = spec
                .fire
                .as_ref()
                .map(|(_, payload)| payload.clone())
                .unwrap_or(serde_json::Value::Null);
            let refusal = |why: String| refused(&weft_core::run_spec::Refusal::error(why));
            let request = stand_in_caller(&capture.spec, &payload, &address).map_err(refusal)?;
            // Only a header typed by hand can be one the gate reads, so a
            // request with none reads no connection.
            let gate = match request.headers.is_empty() {
                true => Vec::new(),
                false => gate_headers(state, id, &capture.spec, spec.instance.as_ref(), &address).await?,
            };
            refuse_typed_credential(&request, &gate, &address).map_err(refusal)?;
            stand_in = Some(capture.spec.clone());
        }
    }

    // The infra gate (`require_run_infra`), against the program this run
    // was cut to before any seed narrows it: a node a seed stands in for
    // still calls the infra it was wired to. `require_action` above
    // deliberately does not enforce the whole-graph fact (it reports it,
    // for the bar's unaimed button), the way it leaves the trigger facts
    // to `require_trigger_infra`.
    crate::api::project::require_run_infra(state, id, &authored.program, spec.instance.as_ref()).await?;
    // What the nodes this run reaches need from the machine.
    crate::needs::refuse_unmet(state, id, &authored.program).await?;
    let settings = spec.settings.over(kept_by_trigger).map_err(|why| refused(&weft_core::run_spec::Refusal::error(why)))?;

    // What the run reuses, and what it runs.
    let mut starting_parameters = spec.clone();
    let (mut resolved, seed) = match seed_run {
        Some(seed_run) => {
            starting.at(RunStarting::ReadingSeed { seed: seed_run }).await;
            let from = fold_seed(state, id, seed_run).await?;
            let planned = seed_plan(&project, &program, &authored, &spec, &from.outcomes, &from.answered,
                &body.seed_until, &body.seed_before)
                .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;
            refuse_moved_files(state, &project, &planned.stand_ins, starting).await?;
            starting_parameters = weft_core::seeding::starting_parameters(&project, &spec, &authored.walk, &planned, &from.outcomes)
                .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;
            let mut resolved = resolve_seeded(&spec, &project, &saved, Some(&planned)).map_err(|r| refused(&r))?;
            resolved.warnings.extend(planned.warnings.iter().cloned());
            let seed = weft_journal::Seed { parent: from.run_id, origins: planned.origins, answers: planned.answers };
            (resolved, Some(seed))
        }
        None => (authored, None),
    };
    for kick in resolved.kicks.iter_mut().filter(|kick| kick.firing) {
        kick.port_snapshot = fired_ports.clone();
    }
    // Against the program as it will run: a seed's history may have fed a
    // crossing the authored cut left open. When it did not, say which run
    // was read and why its results could not stand in, or `--seed` reads
    // as ignored.
    weft_core::run_spec::refuse_unrunnable(&project, &resolved, &spec).map_err(|mut refusal| {
        if let Some(seed) = &seed {
            refusal.errors.push(format!(
                "--seed read run {}: a saved result stands in only for a node that run completed and whose \
                 code has not changed since.",
                seed.parent
            ));
            refusal.errors.extend(resolved.warnings.iter().filter(|w| w.starts_with("cannot seed")).cloned());
        }
        refused(&refusal)
    })?;
    // An instance's run reaching a `@instance_filled` field runs on what that
    // instance provides there: read and checked now, refused naming every
    // field left unfilled or filled wrong, rather than failing at the
    // node mid-run. The run carries what was read.
    let instance_values = match &spec.instance {
        Some(instance) => {
            crate::api::project::instance_values_for_run(state, id, &resolved.program, instance, None).await?
        }
        None => Default::default(),
    };
    // The program's own connections, as this install picked them: a
    // required one nobody picked is refused now, naming the command.
    let picks = crate::api::project::picks_for_run(state, id, &resolved.program).await?;
    let run_program = crate::api::project::runs_program(&program, &resolved.program)?;
    let stale = resolved.runs.clone();
    let kicks = resolved.kicks.clone();
    let now = crate::lease::now_unix() as u64;
    let birth_rows = weft_journal::birth::baked_rows(run_id, &resolved.baked, now);
    // An empty `entry_node` is the journal's marker for a row that no
    // longer decodes, and `weft runs` prints `?` for it. A run with
    // nothing kicked (a seeded run of a node fed by what it reuses, a run
    // fed by `--emit` alone) is named after a place it runs instead: this
    // label is display only (nothing routes on it), and `stale` is a
    // sorted set, so this is the alphabetically first place the run runs,
    // chosen because it is stable, not because it is first in any graph
    // order. A seeded run that reuses everything it reaches runs nothing;
    // it says so.
    // Named the way the program reads them (`one.strip` for a node of
    // an included file under the site `one`).
    let spell = |place: &Located| weft_core::project::address_of(&*project, &place.id, &place.path);
    // Among the kicks, the first ordinary root in the program's own
    // order (a trigger that does not fire is kicked too, and it is a
    // poor name for a run started by hand).
    let order = |node: &str| project.nodes.iter().position(|n| n.id == node).unwrap_or(usize::MAX);
    let is_trigger = |node: &str| project.nodes.iter().any(|n| n.id == node && n.features.is_trigger);
    let entry_node = kicks.iter()
        .min_by_key(|k| (!k.firing, is_trigger(&k.node), k.frames.len(), order(&k.node)))
        .map(|k| crate::api::project::kick_place(&project, k))
        .or_else(|| stale.iter().next().map(spell))
        .unwrap_or_else(|| "everything inherited".into());
    // Recording the run and starting it happen under the project's
    // transition lock, together.
    //
    // The tree row goes first: once `start_queued_run_with`
    // returns, a real run is queued and burning worker capacity, so a
    // failure after that point would hand the caller no run at all
    // while the run proceeded, unreachable from `weft tree` and with
    // nothing to hand `weft clean`.
    //
    // And the pair is locked because `prune` holds the same lock while
    // it decides what to delete. Between the row and the journal a run
    // is a row the journal has never heard of, which is exactly what
    // prune reads as "not running": without the lock prune could
    // delete the version out from under a run that was about to start,
    // leaving it executing under a version that no longer exists.
    //
    // The section is deliberately tiny (two writes and a task enqueue,
    // no storage and no network), because the lock is a transaction and
    // so holds a pooled connection while it runs. Everything expensive
    // this handler does, the seed fold above and the response below,
    // stays outside it.
    let stale_vec: Vec<String> = stale.iter().map(spell).collect::<BTreeSet<_>>().into_iter().collect();
    starting.at(RunStarting::WaitingForLock).await;
    let recorded = with_tree_lock(state, id, || async {
        // Head is read again in here. The one read outside chose the
        // seed, which is this run's own business; the version's PARENT
        // has to be where head is at the moment the row is written, or
        // a session that checkpointed in between is cut out of the
        // lineage.
        let copy = state
            .working_copies
            .get(copy.id)
            .await
            .map_err(|e| internal("working copy", e))?
            .ok_or((StatusCode::NOT_FOUND, "the working copy this run starts from is gone".to_string()))?;
        if state.versions.version(id, &version.id).await.map_err(|e| internal("version", e))?.is_none() {
            return Err((StatusCode::CONFLICT, format!("version {} was pruned while this run was being prepared; build and run again", version.id)));
        }
        crate::api::project::start_queued_run_with(
            state,
            &project.defaults,
            weft_journal::birth::Birth {
                run_id,
                project_id: id,
                phase: weft_core::context::Phase::Fire,
                entry_node: &entry_node,
                kicks: &kicks,
                program: &run_program,
                given: &resolved.given,
                binary_hash: &program.binary_hash,
                seed: seed.clone(),
                source_version: Some(&version.id),
                instance: spec.instance.as_ref().map(|instance| weft_journal::birth::RunFor { instance, values: &instance_values }),
                picks: &picks,
                fired_trigger: fired_trigger.as_deref(),
                stand_in: stand_in.as_ref(),
                settings,
                at_unix: now,
            },
            &birth_rows,
            crate::api::project::QueueAs {
                stale: &stale_vec,
                spec: Some(&starting_parameters),
                example: body.shape.example.as_deref(),
                ..Default::default()
            },
        )
        .await?;
        // Head moves LAST, and a lost race is reported, never refused.
        let to = weft_core::working_copy::Head { head_version: Some(version.id.clone()), head_run: Some(run_id) };
        let moved = state
            .working_copies
            .move_head(copy.id, &copy.head, &to, crate::lease::now_unix() as u64)
            .await
            .map_err(|e| internal("set head", e))?;
        Ok(moved)
    })
    .await;
    let moved = match recorded {
        Ok(moved) => moved,
        // Another claim of this same start recorded it first (a task's
        // claim rescued while the first claimant still ran): that run is
        // this start's, and is answered as it recorded itself.
        Err(refused) => return started_before(state, id, run_id, body.working_copy).await?.ok_or(refused),
    };

    // A head that would not move is reported, never refused.
    //
    // By this point the run is journaled, queued and recorded
    // under its version: it is running. Answering 409 would tell the
    // person nothing started while a real run burned worker capacity
    // under a run they were never given, with no way to stop it. So
    // the run stands and the answer says what happened.
    //
    // Every tree write holds this project's lock, and this compare-and-set
    // is inside it. What is left is a `weft clean` of the run head pointed
    // at (it clears the head's run under no lock), or a `weft rm` landing
    // between the two.
    let mut warnings = resolved.warnings.clone();
    if !moved {
        warnings.push(head_not_moved(&version.id, run_id));
    }

    super::versions::announce(state, id, copy.id).await;
    let ran: Vec<String> = stale_vec.clone();
    let inherited: Vec<String> = seed.as_ref().map(|seed| seed.origins.keys().map(spell).collect::<BTreeSet<_>>().into_iter().collect()).unwrap_or_default();
    Ok(RunStarted {
        run_id,
        version: version.id,
        seed: seed.map(|s| s.parent),
        inherited,
        ran,
        warnings,
    })
}

/// The request a stand-in caller serves when a run is started by hand
/// through a trigger that answers a caller, standing in for the socket
/// nobody opened: read out of the fire's payload, refused at the command
/// when it is no request or the trigger is one nobody can stand in for
/// (and when it carries a credential, [`refuse_typed_credential`]). The
/// run's birth carries the trigger's spec
/// (`RunStarted::stand_in`), and the worker that drives it serves this
/// request from its firing kick.
///
/// The payload is what the trigger's `firesWith` declares and the run
/// spec already held it to, so the request is read straight out of it.
/// Fields this type has no place for are left alone rather than
/// refused: they are the trigger's, declared by it, checked against its
/// own contract, and the node reads them off its wake.
///
/// Every fired caller trigger gets a stand-in, whether or not the
/// payload carries anything to say. The difference matters more than it
/// looks: NO stand-in means the trigger itself fails, asking to be
/// triggered through a Route, which is the node the author is already
/// looking at.
fn stand_in_caller(
    spec: &weft_core::primitive::SignalSpec,
    payload: &serde_json::Value,
    address: &str,
) -> Result<weft_core::caller::LiveRequest, String> {
    // Refused HERE, at the command, rather than only in the worker that
    // would serve it: the engine keeps its own floor, but by then the
    // person has already waited for a run to start just to be
    // told the thing they asked for cannot happen.
    if weft_core::signal::caller_protocol(&spec.kind) == Some(weft_core::signal::Protocol::Websocket) {
        return Err(format!(
            "--fire {address}: a Socket cannot be fired. Its shape is a conversation over \
             time, and there is nothing honest to invent for the caller's next message. \
             Point a real client at it (`weft activate` prints the URL)"
        ));
    }
    // The payload travels WHOLE. The request is read out of it, and
    // whatever else the trigger declared (a `body`, for the triggers
    // that can carry one) stays where the author put it and reaches the
    // node as its wake. Nothing is lifted out on the way: a body cut out
    // here and served back through the ordinary request call would mean
    // this tier holding a caller trigger's field name to make the
    // impersonation work.
    let request: weft_core::caller::LiveRequest = serde_json::from_value(payload.clone()).map_err(|e| {
        format!(
            "--fire {address}: this trigger answers a caller, so its payload is the \
             request to serve: {e}"
        )
    })?;
    Ok(request)
}

/// Refuse a fired `request` (fired as `address`) carrying a credential:
/// any request's, or one of `gate`, the headers the route's gate reads
/// ([`gate_headers`]). A real call makes those secrets at the door, so a
/// run started with one typed by hand would not behave as one.
fn refuse_typed_credential(request: &weft_core::caller::LiveRequest, gate: &[String], address: &str) -> Result<(), String> {
    match request.credential_typed_by_hand(gate) {
        Some(header) => Err(format!("--fire {address}: {}", weft_core::caller::fired_credential(header))),
        None => Ok(()),
    }
}

/// The headers the gate of the caller trigger `spec` (fired as `address`,
/// for `instance` of project `project`) reads a caller's credential from:
/// none on an open route, else what its connection's recipe names.
async fn gate_headers(
    state: &DispatcherState,
    project: uuid::Uuid,
    spec: &weft_core::primitive::SignalSpec,
    instance: Option<&weft_core::instance::InstanceId>,
    address: &str,
) -> Result<Vec<String>, Refused> {
    let refusal = |why: String| refused(&weft_core::run_spec::Refusal::error(format!("--fire {address}: {why}")));
    let (_, config) = weft_core::signal::live_connection(spec).map_err(refusal)?;
    let weft_core::signal::PublicEntryAuth::Connection { access_id, service } = &config.auth else {
        return Ok(Vec::new());
    };
    let access_id: uuid::Uuid =
        access_id.parse().map_err(|_| refusal(format!("the route's gate names '{access_id}', which is no connection id")))?;
    let tenant = crate::api::infra::tenant_of(state, project).await?;
    let scope = instance.map(|instance| weft_core::instance::InstanceScope { project_id: project, instance: instance.clone() });
    weft_access_store::caller_credential_headers(
        &state.pg_pool,
        &tenant,
        weft_access_store::GrantUser::of(scope.as_ref()),
        access_id,
        service,
    )
    .await
    .map_err(|e| refusal(format!("the connection gating this route cannot be read: {e:#}")))
}



// ----- a program's calls on runs -----------------------------------------

/// The run started under `run_id` before (the same start asked again
/// after its answer was lost), read back off what the run recorded: its
/// row in the version tree, written with it, and its birth. `None` when
/// no run has that id. A run of that id in another project refuses: the
/// id was not this start's. One that started and whose answer cannot be
/// read again (its version was pruned since) refuses loudly, naming it.
/// The warnings the first answer carried are not kept; whether head (of
/// `working_copy`, the copy the start moves) points at the run is read
/// again and said when it does not, and the tree announced (the first
/// start may have stopped before either), while that copy is still there.
async fn started_before(
    state: &DispatcherState,
    project: uuid::Uuid,
    run_id: RunId,
    working_copy: weft_core::working_copy::WorkingCopyRef,
) -> Result<Option<RunStarted>, Refused> {
    let Some(owner) = state.runs.run_owner(run_id).await.map_err(|e| internal("the run started under this id", e))? else {
        return Ok(None);
    };
    if owner.project_id != project {
        return Err((StatusCode::CONFLICT, format!("run id {run_id} is taken; ask again under a new id")));
    }
    let Some(recorded) = state.versions.run(run_id).await.map_err(|e| internal("the run started under this id", e))? else {
        return Err((
            StatusCode::CONFLICT,
            format!(
                "run {run_id} was started by this same request already, and the version it ran was pruned since, so what \
                 it started with cannot be read again; `weft runs` lists it"
            ),
        ));
    };
    let birth = crate::projection::run_birth(state, run_id)
        .await
        .map_err(|e| internal("the birth of the run started under this id", e))?
        .map_err(|e| internal("the birth of the run started under this id", e))?
        // The row and the birth are written in one transaction.
        .ok_or_else(|| internal("the run started under this id", format!("run {run_id} is in the version tree with no record")))?;
    let reused: Vec<Located> = match birth.event {
        weft_journal::RunEvent::RunStarted { seed: Some(seed), .. } => seed.origins.into_keys().collect(),
        _ => Vec::new(),
    };
    // Only what it reused needs the program it ran, to spell those places.
    let inherited = match reused.is_empty() {
        true => Vec::new(),
        false => {
            let program = state
                .program(project, &recorded.definition_hash)
                .await
                .map_err(|e| internal("the program the run ran", e))?
                .ok_or_else(|| internal("the program the run ran", format!("program {} is no longer recorded", recorded.definition_hash)))?;
            let spelled: BTreeSet<String> =
                reused.iter().map(|place| weft_core::project::address_of(&*program, &place.id, &place.path)).collect();
            spelled.into_iter().collect()
        }
    };
    let mut started = started_as_recorded(recorded, inherited);
    // The copy is read only for the warning and the announcement: one
    // forgotten since is no reason to refuse the run's answer.
    let copy = match working_copy.working_copy {
        Some(named) => state.working_copies.get(named).await.map_err(|e| internal("working copy", e))?.filter(|copy| copy.project_id == project),
        None => Some(super::versions::copy_of(state, project, working_copy).await?),
    };
    if let Some(copy) = copy {
        if copy.head.head_run != Some(run_id) {
            let short = &run_id.to_string()[..8];
            started.warnings.push(format!(
                "this run was started by this same request before, and its first answer was lost; head is not on it \
                 now, because it moved on since or because the first start stopped before moving it. `weft branch \
                 {short}` moves head here, if that is where you want it."
            ));
        }
        super::versions::announce(state, project, copy.id).await;
    }
    Ok(Some(started))
}

/// What a run's first answer says when head was not moved to it: why that
/// happens, and the way through.
fn head_not_moved(version: &str, run_id: RunId) -> String {
    let short = &run_id.to_string()[..8];
    format!(
        "this run started and is recorded under version {version} (`weft tree` shows it), but head \
         could not be moved to it: head changed under this call, which happens when the run head pointed \
         at was cleaned, or when the project was removed while this was being prepared. If the project is \
         gone, this run outlives it and `weft clean {short}` is what removes it. Otherwise `weft branch \
         {short}` moves head here."
    )
}

/// What a run's start answered, as the run recorded it: its row in the
/// version tree (`recorded`) and the places it reused (`inherited`,
/// spelled and sorted).
fn started_as_recorded(recorded: crate::versions::RunRow, inherited: Vec<String>) -> RunStarted {
    RunStarted {
        run_id: recorded.run_id,
        version: recorded.version_id,
        seed: recorded.seed_run_id,
        inherited,
        ran: recorded.stale,
        warnings: Vec::new(),
    }
}

/// Carry out one run call of `actor` on project `project_id`: a start
/// here, every other call on runs in [`super::run_admin`].
pub(crate) async fn program_call(
    state: &DispatcherState,
    actor: &Actor,
    project_id: uuid::Uuid,
    call: &weft_core::program::runs::RunCall,
    stop_self: weft_core::StopSelf,
) -> Result<weft_core::program::ProgramCallOutcome, Refused> {
    use weft_core::program::runs::RunCall;
    match call {
        // The run is named after the call, so the call issued again (its
        // answer lost before the run journaled it) finds the run it
        // started instead of starting another.
        RunCall::Start { request } => {
            let Actor::Program { call, .. } = actor else {
                return Err(internal("a program's call", "a run call reached the program calls with no program asking"));
            };
            super::answered(&run(state, actor, project_id, request, *call, &Starting::Unfollowed).await?)
        }
        other => super::run_admin::program_call(state, actor, project_id, other, stop_self).await,
    }
}

#[cfg(test)]
mod started_before_tests {
    use super::*;

    /// A start asked again is answered with the run its first ask
    /// started, as that run recorded it: its id, version, seed, what it
    /// reused and what it runs.
    #[test]
    fn a_start_asked_again_answers_the_run_as_it_recorded() {
        let run_id = RunId::from_u128(4);
        let recorded = crate::versions::RunRow {
            run_id,
            project_id: uuid::Uuid::from_u128(1),
            version_id: "v2".into(),
            seed_run_id: Some(RunId::from_u128(3)),
            stale: vec!["a".into(), "one.b".into()],
            spec: None,
            definition_hash: "d".into(),
            example: None,
            created_at: 7,
        };
        let started = started_as_recorded(recorded, vec!["start".into()]);
        assert_eq!(
            started,
            RunStarted {
                run_id,
                version: "v2".into(),
                seed: Some(RunId::from_u128(3)),
                inherited: vec!["start".into()],
                ran: vec!["a".into(), "one.b".into()],
                warnings: Vec::new(),
            }
        );
    }
}

#[cfg(test)]
mod fire_snapshot_tests {
    use super::*;

    fn plan(node: &str, firing: bool) -> crate::api::project::Kick {
        crate::api::project::Kick { node: node.to_string(), frames: Vec::new(), firing, payload: None, port_snapshot: None }
    }

    /// A fired trigger replays the ports it registered at activation,
    /// the same snapshot a real event fire replays. Without this a
    /// trigger whose port is wired from another node failed on that
    /// port under `weft run --fire`, while the identical trigger fired
    /// by a real event ran fine.
    #[test]
    fn the_conversion_carries_what_the_resolver_decided() {
        // The snapshot rule itself lives in `weft_core::run_spec`
        // (`a_registered_port_lets_the_fire_resolve`), which is where
        // the fact is known. This only pins that the conversion to the
        // journal's kick loses nothing on the way.
        let mut fired = plan("ask", true);
        fired.port_snapshot = Some(serde_json::json!({ "endpointUrl": "http://bridge:8090" }));
        let kicks = [fired, plan("bridge", false)];
        let ask = kicks.iter().find(|k| k.node == "ask").expect("the trigger is kicked");
        assert_eq!(ask.port_snapshot, Some(serde_json::json!({ "endpointUrl": "http://bridge:8090" })));
        let bridge = kicks.iter().find(|k| k.node == "bridge").expect("kicked");
        assert_eq!(bridge.port_snapshot, None);
    }

    /// A trigger with no wired input (a timer, the common case) fires
    /// by hand with no activation and nothing to refuse. Testing one
    /// must never require activating the project, since that would
    /// start the real schedule firing on its own.
    #[test]
    fn a_trigger_with_no_wired_input_fires_by_hand_unactivated() {
        let project: weft_core::ProjectDefinition = serde_json::from_value(serde_json::json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "nodes": [{
                "id": "tick", "nodeType": "Cron", "label": null,
                "config": {}, "position": {"x": 0, "y": 0},
                "inputs": [{"name": "cron", "portType": "String", "required": true}],
                "outputs": [{"name": "scheduledTime", "portType": "String", "required": true}],
                "features": {"isTrigger": true}, "scope": [], "groupBoundary": null,
                "requiresInfra": false,
                "portLiterals": {"cron": "0 0 * * * *"},
            }],
            "edges": [],
        }))
        .expect("program");
        let spec = RunSpec {
            fire: Some(("tick".into(), serde_json::json!({ "scheduledTime": "t" }))),
            ..RunSpec::whole("fire")
        };
        let resolved = resolve_spec(&spec, &project, &Default::default()).expect("the walk needs no external facts");
        assert!(resolved.kicks.iter().any(|k| k.node == "tick" && k.firing));
    }

}

#[cfg(test)]
mod stand_in_caller_tests {
    use super::{refuse_typed_credential, stand_in_caller};
    use serde_json::json;

    fn route_spec() -> weft_core::primitive::SignalSpec {
        serde_json::from_value(json!({ "kind": "route", "config": {} }))
            .expect("a minimal signal spec")
    }

    /// A GET has no body, so the natural spelling of firing one has no
    /// `body` key. That must still attach a caller: without one the
    /// Route node itself fails, telling the author to trigger it
    /// through a Route, which is the node they are already firing.
    #[test]
    fn a_get_with_no_body_still_gets_a_caller() {
        let request = stand_in_caller(
            &route_spec(),
            &json!({ "method": "GET", "path": "cards" }),
            "list.door",
        )
        .expect("a request with no body is a request");
        assert_eq!(request.method, "GET");
    }

    /// A socket is a conversation over time and there is nothing
    /// honest to invent for the caller's next message, so firing one is
    /// refused. At the command, not after a run has started:
    /// the worker keeps its own floor, but waiting for a run just to be
    /// told no is a worse way to hear it.
    #[test]
    fn firing_a_socket_is_refused_at_the_command() {
        let socket: weft_core::primitive::SignalSpec =
            serde_json::from_value(json!({ "kind": "socket", "config": {} }))
                .expect("a minimal signal spec");
        let why = stand_in_caller(&socket, &json!({ "path": "chat" }), "chat.door")
            .expect_err("a socket cannot be fired");
        assert!(why.contains("Socket cannot be fired"), "{why}");
        // And it says what to do instead, which is the whole point of
        // refusing early.
        assert!(why.contains("weft activate"), "{why}");
    }

    /// The request is READ from the payload, never carved out of it.
    ///
    /// A `body` the author typed is the trigger's own declared field:
    /// it stays in the payload, reaches the node as its wake, and the
    /// node reads it from there. Cutting it out here and feeding it
    /// back through the ordinary request call would mean this tier
    /// holding a caller trigger's field name.
    #[test]
    fn the_payload_is_read_not_carved_up() {
        let payload = json!({ "method": "POST", "path": "hello", "body": { "name": "ada" } });
        let request = stand_in_caller(&route_spec(), &payload, "hello").expect("a post with a body");
        assert_eq!(request.path, "hello");
        assert_eq!(request.method, "POST");
    }

    /// A payload that is not a request at all is refused naming the
    /// trigger, rather than starting a run that fails inside the node.
    #[test]
    fn a_payload_that_is_not_a_request_is_refused_by_name() {
        let why = stand_in_caller(&route_spec(), &json!({ "method": 7 }), "list.door")
            .expect_err("a method that is not a string is not a request");
        assert!(why.contains("list.door") && why.contains("request to serve"), "{why}");
    }

    /// A credential header typed into a fired request is refused at the
    /// command: the program reads a caller's credentials as secrets,
    /// which only a real call makes, so the run would not behave as one.
    #[test]
    fn a_fired_request_carrying_a_credential_is_refused() {
        let payload = json!({ "method": "GET", "path": "me", "headers": [["Authorization", "Bearer x"]] });
        let request = stand_in_caller(&route_spec(), &payload, "me.door").expect("a request");
        let why = refuse_typed_credential(&request, &[], "me.door").expect_err("a credential typed by hand");
        assert!(why.contains("me.door") && why.contains("`Authorization`") && why.contains("credentials"), "{why}");
    }

    /// A header the route's gate reads its caller's credential from is
    /// refused the same way, and is a plain header on a route no gate
    /// reads it on.
    #[test]
    fn a_fired_request_carrying_the_gates_header_is_refused() {
        let payload = json!({ "method": "GET", "path": "me", "headers": [["X-Api-Key", "key-9"]] });
        let request = stand_in_caller(&route_spec(), &payload, "me.door").expect("a request");
        let why = refuse_typed_credential(&request, &["X-Api-Key".to_string(), "authorization".to_string()], "me.door")
            .expect_err("the gate's header typed by hand");
        assert!(why.contains("`X-Api-Key`") && why.contains("Call the route for real"), "{why}");
        assert_eq!(refuse_typed_credential(&request, &[], "me.door"), Ok(()), "no gate reads it");
    }
}
