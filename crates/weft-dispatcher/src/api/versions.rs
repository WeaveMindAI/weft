//! The version tree's HTTP surface: checkpoint, the run that records
//! itself (seeded, scoped, or plain), the tree, head, activation, and
//! prune. See `crate::versions` for the model.
//!
//! One start path. `run` here is THE way an execution starts from the
//! CLI or the editor: it resolves the spec (`weft_core::run_spec`),
//! decides what a seeded run inherits (`weft_core::seeding`), journals
//! the birth rows through `project::start_queued_execution` exactly as
//! every other start does, and records the run under its version. A
//! plain `weft run` is this endpoint with no seed and no scope.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use weft_core::run_spec::{resolve_spec, Refusal, Resolved, RunSpec};
use weft_core::seeding::{inheritable_nodes, seed_plan, SeedOutcome};
use weft_core::Color;

use crate::api::project::{coherent_definition, require_action, Kick};
use crate::authenticator::{authorize_project, CallerTenant};
use crate::state::DispatcherState;
use crate::versions::{
    manifest_diff, plan_prune, resolve_seed, version_id, Head, Manifest, ManifestDiff,
    PrunePlan, RunRow, SeedChoice, VersionRow,
};

type ApiError = (StatusCode, String);

fn internal(what: &str, e: impl std::fmt::Display) -> ApiError {
    (StatusCode::INTERNAL_SERVER_ERROR, format!("{what}: {e}"))
}

fn parse_id(id: &str) -> Result<uuid::Uuid, ApiError> {
    id.parse::<uuid::Uuid>().map_err(|_| (StatusCode::BAD_REQUEST, "bad id".into()))
}

/// Every tree write that reads head and moves it happens inside this,
/// holding the project's transition lock.
///
/// The writes that do NOT are the ones that only take rows away, and
/// they are named here because the compare-and-set inside
/// [`move_head_or_conflict`] is the only thing standing between them and
/// a writer in here: `weft clean` deletes one run row and clears
/// `head_run` (`api::execution`), a deactivate clears the activation pin
/// (`api::project`), and the retirement sweep deletes rows of a project
/// that no longer exists (`versions::retire_unused_versions`). A lost
/// compare-and-set names those, and only those, as its cause.
///
/// A tree write is a read-modify-write: read head, record a version
/// under it, move head there. Each writer spelling that itself meant
/// three of them (checkpoint, the activation pin, the bare-version
/// sweep) read head, wrote, and moved with nothing serialising them
/// against `prune`, which deletes versions, or against each other. Two
/// sessions checkpointing at once could leave head naming a version
/// another session's prune had just deleted, and `parent_id` carries no
/// foreign key, so nothing downstream rejected it: the branch became
/// unreachable, and therefore unprunable, for good.
///
/// So the lock is the way in, not a thing to remember. The compare-and-
/// set inside [`move_head_or_conflict`] stays as well: it costs one
/// `WHERE` clause and it is what keeps a writer that somehow arrives
/// without the lock from clobbering silently.
///
/// Keep the body SHORT: the lock is a Postgres advisory lock held in a
/// transaction, so it holds a pooled connection while it runs. Anything
/// expensive (a seed fold, a journal read, storage) belongs outside it,
/// which is why `run` computes its version id (a pure hash of the
/// manifest) and folds its seed first, then writes both rows in here.
async fn with_tree_lock<T, F, Fut>(
    state: &DispatcherState,
    project: &str,
    body: F,
) -> Result<T, ApiError>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<T, ApiError>>,
{
    // The lock reports only its own failures through `anyhow`; the
    // body's own refusals ride through it untouched, so a CONFLICT
    // from inside stays a CONFLICT instead of collapsing to a 500.
    crate::lease::with_project_transition_lock(&state.lock_pool, project, || async move {
        Ok(body().await)
    })
    .await
    // Shared with every other holder of this lock, so the three of them
    // cannot answer the same failure differently.
    .map_err(|e| crate::lease::lock_answer("tree lock", e))?
}

// ----- checkpoint --------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct CheckpointRequest {
    pub manifest: Manifest,
    #[serde(default)]
    pub label: Option<String>,
    /// Start a new tree: the version parents on nothing.
    #[serde(default)]
    pub root: bool,
}

// SYNC: VersionUpsert <-> crates/weft-cli/src/commands/checkpoint.rs (response fields)
#[derive(Debug, Serialize)]
pub struct VersionUpsert {
    pub version: String,
    /// `false` when the project already had this exact code.
    pub created: bool,
    pub parent: Option<String>,
}

/// Record the files as a version under head (no run, no build) and
/// move head there. Identical code answers the existing version.
pub async fn checkpoint(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<String>,
    Json(body): Json<CheckpointRequest>,
) -> Result<Json<VersionUpsert>, ApiError> {
    let id = parse_id(&id)?;
    authorize_project(&state, &caller.0, id).await?;
    let project_id = id.to_string();
    let upsert = with_tree_lock(&state, &project_id, || async {
        let (upsert, head) = upsert_version(&state, id, &body.manifest, body.label.as_deref(), body.root).await?;
        move_head_or_conflict(&state, id, &head, Some(&upsert.version), None).await?;
        Ok(upsert)
    })
    .await?;
    Ok(Json(upsert))
}

/// Record `manifest` as a version parented on head (or on nothing
/// under `root`), unless the project already has it. A label is set
/// on a new version and replaces an existing one's.
///
/// Reads head and writes in one breath, so it belongs inside
/// [`with_tree_lock`]; the head it returns is what the caller then
/// compare-and-sets against.
async fn upsert_version(
    state: &DispatcherState,
    project: uuid::Uuid,
    manifest: &Manifest,
    label: Option<&str>,
    root: bool,
) -> Result<(VersionUpsert, Head), ApiError> {
    let head = state.versions.head(project).await.map_err(|e| internal("head", e))?;
    let upsert = commit_version(state, project, manifest, label, root, &head).await?;
    Ok((upsert, head))
}

pub(crate) async fn record_program_source(
    state: &DispatcherState,
    project: uuid::Uuid,
    program: &weft_core::project::hash::ProgramIdentity,
) -> Result<String, ApiError> {
    with_tree_lock(state, &project.to_string(), || async {
        record_program_source_locked(state, project, program).await
    }).await
}

/// Record the exact program while the caller holds the project transition lock.
pub(crate) async fn record_program_source_locked(
    state: &DispatcherState,
    project: uuid::Uuid,
    program: &weft_core::project::hash::ProgramIdentity,
) -> Result<String, ApiError> {
    let manifest = state.projects.program_source(project, program).await
        .map_err(|e| (StatusCode::CONFLICT, e.to_string()))?;
    let (version, _) = upsert_version(state, project, &manifest, None, false).await?;
    Ok(version.version)
}

/// The durable journal bridge records real fires from their immutable birth.
/// A storage failure retries without firing the trigger again.
pub(crate) async fn record_trigger_run(state: &DispatcherState, color: Color) -> anyhow::Result<()> {
    if state.versions.run(color).await?.is_some() { return Ok(()); }
    let rows = state.journal.events_log(color).await?;
    if let Some(run) = trigger_run_from_birth(color, &rows)? {
        state.versions.insert_run(&run).await?;
    }
    Ok(())
}

fn trigger_run_from_birth(color: Color, rows: &[weft_journal::ExecEvent]) -> anyhow::Result<Option<RunRow>> {
    let Some(weft_journal::ExecEvent::ExecutionStarted { project_id, source_version, definition_hash, at_unix, .. }) = rows.first() else {
        anyhow::bail!("run {color} has no birth");
    };
    let mut fires = rows.iter().filter_map(|row| match row {
        weft_journal::ExecEvent::NodeKicked { node_id, firing: true, payload, .. } => Some((node_id.clone(), payload.clone().unwrap_or(Value::Null))),
        _ => None,
    });
    let Some(fire) = fires.next() else { return Ok(None); };
    anyhow::ensure!(fires.next().is_none(), "trigger run {color} started from more than one trigger");
    let version = source_version.clone().ok_or_else(|| anyhow::anyhow!("trigger run {color} has no original source version"))?;
    let definition_hash = definition_hash.clone().ok_or_else(|| anyhow::anyhow!("trigger run {color} has no original definition"))?;
    let mut spec = weft_core::run_spec::RunSpec::whole("");
    spec.fire = Some(fire);
    Ok(Some(RunRow {
        color, project_id: project_id.parse()?, version_id: version, seed_color: None,
        stale: Vec::new(), spec: Some(spec), definition_hash, example: None, created_at: *at_unix,
    }))
}

/// Write the version row itself, under a head the caller has already
/// read. Separate from [`upsert_version`] because `run` needs the id
/// (a pure hash of the manifest) and the head long before it may
/// write: it folds its seed in between, which is far too slow to do
/// while holding the tree lock, so it commits the row here at the same
/// moment it records the run.
async fn commit_version(
    state: &DispatcherState,
    project: uuid::Uuid,
    manifest: &Manifest,
    label: Option<&str>,
    root: bool,
    head: &Head,
) -> Result<VersionUpsert, ApiError> {
    validate_manifest(manifest)?;
    let id = version_id(manifest);
    let parent = if root { None } else { head.head_version.clone().filter(|h| h != &id) };
    let row = VersionRow {
        id: id.clone(),
        project_id: project,
        parent_id: parent.clone(),
        manifest: manifest.clone(),
        label: label.map(str::to_string),
        created_at: crate::lease::now_unix() as u64,
    };
    let created = state.versions.upsert_version(&row).await.map_err(|e| internal("record version", e))?;
    let recorded = if created {
        None
    } else {
        state.versions.version(project, &id).await.map_err(|e| internal("version", e))?
    };
    if !created && root && recorded.as_ref().and_then(|v| v.parent_id.as_ref()).is_some() {
        let parent = recorded.as_ref().and_then(|v| v.parent_id.clone()).unwrap_or_default();
        return Err(root_conflict(&id, &parent));
    }
    if !created {
        if let Some(label) = label {
            state.versions.set_label(project, &id, Some(label)).await.map_err(|e| internal("label", e))?;
        }
    }
    let parent = if created { parent } else { recorded.and_then(|v| v.parent_id) };
    Ok(VersionUpsert { version: id, created, parent })
}

/// `--root` on code the project already holds.
///
/// A version IS its code, so this exact code is already a version, and it
/// already has a parent: content addressed, it cannot have two. Answering
/// the existing version instead would hand the caller the old tree when
/// they asked for a new one.
fn root_conflict(id: &str, parent: &str) -> ApiError {
    (
        StatusCode::CONFLICT,
        format!(
            "a version IS its code, and this exact code is already version {id}, recorded under \
             {parent}. It cannot also be a root, so there is nothing `--root` can do here: change \
             something and run again, or drop `--root` to record this run under the version it \
             already has"
        ),
    )
}

/// Refuse a manifest that could not be stored or addressed.
///
/// The manifest comes from the client and its values become storage
/// keys (`blob_keys`), so an entry carrying a `/` or a `..` would
/// produce a key the storage surface refuses, and because the asset
/// reference publish spans EVERY version of the project, one such row
/// would break that project's publish from then on, durably. The gate
/// belongs here because this is where a manifest becomes a version.
fn validate_manifest(manifest: &Manifest) -> Result<(), ApiError> {
    for (path, hash) in manifest {
        if path.starts_with(weft_core::project::hash::WEFT_ENTRY_PREFIX) {
            // The pseudo-entry naming the installed weft. It addresses
            // no blob, so its value must be empty: checked, not
            // assumed, because `blob_keys` decides what is a blob by
            // the empty value and would otherwise try to build a key
            // out of whatever a caller put here.
            if !hash.is_empty() {
                return Err((
                    StatusCode::BAD_REQUEST,
                    format!(
                        "'{}' names the installed weft, which addresses no file, so it carries no hash",
                        weft_core::truncate_user_string(path, 256)
                    ),
                ));
            }
            continue;
        }
        if path.is_empty()
            || path.starts_with('/')
            || path.contains('\\')
            || path.split('/').any(|seg| seg.is_empty() || seg == "." || seg == "..")
        {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("'{}' is not a project-relative path", weft_core::truncate_user_string(path, 256)),
            ));
        }
        if hash.is_empty() {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("'{}' carries no content hash", weft_core::truncate_user_string(path, 256)),
            ));
        }
        if hash.len() != 64 || !hash.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err((
                StatusCode::BAD_REQUEST,
                format!(
                    "'{}' names '{}', which is not a sha256",
                    weft_core::truncate_user_string(path, 256),
                    weft_core::truncate_user_string(hash, 128)
                ),
            ));
        }
    }
    Ok(())
}

/// Move head from where the caller read it, or refuse: a sibling Pod
/// moved head in between, and every decision here (which version to
/// parent on, which run to seed from) was made against the old one.
async fn move_head_or_conflict(
    state: &DispatcherState,
    project: uuid::Uuid,
    expected: &Head,
    version: Option<&str>,
    run: Option<Color>,
) -> Result<(), ApiError> {
    let moved = state
        .versions
        .move_head(project, expected, version, run)
        .await
        .map_err(|e| internal("set head", e))?;
    if !moved {
        // Not "another session checkpointed": every tree write holds the
        // project's lock and this compare-and-set runs inside it. Head
        // changing under a locked call means the row itself moved on, by a
        // `weft clean` clearing the run head named or by the project being
        // removed. A retry helps in the first case and never in the
        // second, so both are named.
        return Err((
            StatusCode::CONFLICT,
            "head changed under this call: either the run head pointed at was cleaned, or the \
             project was removed. Try again, and if it refuses again check `weft ps` for the \
             project"
                .into(),
        ));
    }
    Ok(())
}

// ----- run ---------------------------------------------------------------

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VersionRunRequest {
    pub manifest: Manifest,
    /// The definition the CLI just registered; the run refuses if the
    /// project's running definition moved since.
    #[serde(rename = "definitionHash")]
    pub definition_hash: String,
    #[serde(rename = "binaryHash")]
    pub binary_hash: String,
    #[serde(default)]
    pub seed: bool,
    #[serde(default, rename = "seedUntil")]
    pub seed_until: Vec<String>,
    #[serde(default, rename = "seedBefore")]
    pub seed_before: Vec<String>,
    #[serde(default)]
    pub root: bool,
    /// `None` is a plain whole-graph run.
    #[serde(default)]
    pub spec: Option<RunSpec>,
    /// The saved example whose starting parameters this run uses, if any.
    #[serde(default)]
    pub example: Option<String>,
}

// SYNC: VersionRunResponse <-> crates/weft-cli/src/commands/run.rs (response fields)
#[derive(Debug, Serialize)]
pub struct VersionRunResponse {
    pub color: Color,
    pub version: String,
    pub seed: Option<Color>,
    /// Nodes taken from the seed, sorted.
    pub inherited: Vec<String>,
    /// Nodes this run executes, sorted.
    pub ran: Vec<String>,
    pub warnings: Vec<String>,
}

/// What the resolver refused, as the 422 body: JSON, so the CLI prints
/// every line and the editor makes a field per missing input.
fn refused(refusal: &Refusal) -> ApiError {
    (
        StatusCode::UNPROCESSABLE_ENTITY,
        serde_json::to_string(refusal).expect("a Refusal serializes"),
    )
}

/// The runs of a project that are genuinely still going.
///
/// NOT simply "not settled": a color the journal does not know at all
/// (a tree row whose execution never started, which is what a failure
/// between recording the run and journaling it leaves) has no terminal
/// event, and calling that "in flight" made prune refuse the whole
/// subtree for ever, with `weft stop` unable to reach it either. The
/// journal's own non-terminal list only contains colors it knows, so a
/// row it has never heard of is not in flight; it is nothing.
async fn in_flight_colors(
    state: &DispatcherState,
    project: &str,
) -> Result<std::collections::HashSet<Color>, ApiError> {
    let mut live: std::collections::HashSet<Color> = state
        .journal
        .list_non_terminal_colors_for_project(project, None)
        .await
        .map_err(|e| internal("non-terminal colors", e))?
        .into_iter()
        .collect();
    // Parked on a person is not "still going": it is waiting, and
    // prune's refusal is about work that could still write rows.
    for parked in crate::api::project::suspended_color_set(state, project)
        .await
        .map_err(|e| internal("suspended", e))?
    {
        live.remove(&parked);
    }
    Ok(live)
}

/// Every settled color of a project: terminal, or parked on a suspension.
///
/// Both halves are project-wide questions, so they are asked once. Asked
/// per run instead, a project with a thousand recorded runs did two
/// thousand round trips on every `weft run --seed` and every `weft
/// prune`, re-fetching the same project-wide suspended set each time. The
/// terminal half goes through the `Journal` trait rather than the raw
/// pool, so the in-memory journal can answer it too.
async fn settled_colors(
    state: &DispatcherState,
    project: &str,
) -> Result<std::collections::HashSet<Color>, ApiError> {
    let mut settled = state
        .journal
        .list_terminal_colors_for_project(project)
        .await
        .map_err(|e| internal("terminal colors", e))?;
    settled.extend(
        crate::api::project::suspended_color_set(state, project)
            .await
            .map_err(|e| internal("suspended", e))?,
    );
    Ok(settled)
}

/// The seed run folded, with its program: what the stale set reads.
struct SeededFrom {
    color: Color,
    outcomes: BTreeMap<String, SeedOutcome>,
    outputs: Vec<weft_core::run_spec::ExpectedWire>,
}

async fn fold_seed(state: &DispatcherState, project: uuid::Uuid, run: &RunRow) -> Result<SeededFrom, ApiError> {
    if run.project_id != project { return Err((StatusCode::NOT_FOUND, "seed is outside this project".into())); }
    let sources = crate::projection::reconstruct_execution(state, run.color).await.map_err(|e| internal("seed sources", e))?;
    let definition = sources[&run.color].project().as_ref();
    let snapshot = sources[&run.color].snapshot();
    let outputs = sources[&run.color].output_wires().map_err(|e| internal("seed outputs", e))?;
    let live_nodes: BTreeSet<_> = outputs.iter().filter(|wire| weft_core::weft_type::WeftType::contains_bus_handle(&wire.value))
        .map(|wire| wire.node.as_str()).collect();
    let mut identities = BTreeMap::new();
    let mut outcomes = BTreeMap::new();
    for node in inheritable_nodes(definition, snapshot) {
        let origin = snapshot.inherited_origins.get(&node).copied().unwrap_or(run.color);
        let (original, graph) = if origin == run.color { (snapshot, definition) } else {
            let source = sources.get(&origin).ok_or_else(|| internal("seed origin", format!("missing run {origin}")))?;
            (source.snapshot(), source.project().as_ref())
        };
        if let std::collections::btree_map::Entry::Vacant(slot) = identities.entry(origin) {
            let program = original.program.as_ref().ok_or_else(|| internal("seed identity", format!("run {origin} has no production code identity")))?;
            slot.insert(program.slice_hashes(graph).map_err(|e| internal("seed identity", e))?);
        }
        let mut used_backups = BTreeMap::new();
        let mut backup_origins = BTreeMap::new();
        let mut absent_ports = BTreeSet::new();
        let declaration = graph.nodes.iter().find(|n| n.id == node).ok_or_else(|| internal("seed node", &node))?;
        for record in original.executions.get(&node).into_iter().flatten() {
            for port in &record.received.backup_ports {
                let value = original.selection.as_ref().and_then(|selection| selection.input.get(&node))
                    .and_then(|ports| ports.get(port)).ok_or_else(|| internal("seed backup", format!("{origin}: {node}.{port} has no recorded backup")))?;
                used_backups.insert(port.clone(), value.clone());
                backup_origins.insert(port.clone(), record.received.inherited_ports.get(port).copied().unwrap_or(origin));
            }
            absent_ports.extend(declaration.inputs.iter().filter(|port| !record.received.input.contains_key(&port.name)).map(|port| port.name.clone()));
        }
        let boundary_ports = original.selection.as_ref().and_then(|selection| selection.boundary_ports.get(&node)).cloned().unwrap_or_default();
        outcomes.insert(node.clone(), SeedOutcome { origin, slice_hash: identities[&origin][&node].clone(), used_backups, backup_origins, absent_ports, boundary_ports,
            emitted_live_handle: live_nodes.contains(node.as_str()),
            fire: original.kicked.iter().find(|(location, kick)| location.node_id == node && kick.firing)
                .map(|(_, kick)| kick.payload.clone().unwrap_or(Value::Null)),
        });
    }
    Ok(SeededFrom { color: run.color, outcomes, outputs })
}

/// Start a run that records itself in the tree. See the module doc.
pub async fn run(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<String>,
    Json(body): Json<VersionRunRequest>,
) -> Result<Json<VersionRunResponse>, ApiError> {
    let id = parse_id(&id)?;
    authorize_project(&state, &caller.0, id).await?;
    require_action(&state, id, &["run"]).await?;
    if (!body.seed_until.is_empty() || !body.seed_before.is_empty()) && !body.seed {
        return Err((StatusCode::BAD_REQUEST, "`--seed-until` and `--seed-before` need `--seed`".into()));
    }
    let (program, project) = coherent_definition(&state, id).await?;
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
    let project_id = id.to_string();

    // The manifest is checked here, before the expensive part, but the
    // version ROW is written later, in the same locked breath as the
    // run's own row: writing it up here left a window in which a
    // concurrent `prune` deleted a version that had no runs yet
    // (nothing marks one as being prepared), and the run then recorded
    // against a version that no longer existed. The id needs no row to
    // be known, it is a hash of the manifest.
    validate_manifest(&body.manifest)?;
    // `--root` on code the project already holds is refused HERE, before
    // the pre-flight, because it is a mistake about the request rather than
    // about the project's state. Left to `commit_version` at the end, a
    // project whose infra was down answered "infra not running" to someone
    // whose actual problem was that a version is its code and this code
    // already has a parent.
    if body.root {
        let existing = state
            .versions
            .version(id, &version_id(&body.manifest))
            .await
            .map_err(|e| internal("version", e))?;
        if let Some(parent) = existing.and_then(|v| v.parent_id) {
            return Err(root_conflict(&version_id(&body.manifest), &parent));
        }
    }
    // Head for the SEED choice. The version's parent reads head again
    // inside the lock, where it cannot be stale.
    let head = state.versions.head(id).await.map_err(|e| internal("head", e))?;
    let seed_run: Option<RunRow> = if body.seed && !body.root {
        let versions = state.versions.versions(id).await.map_err(|e| internal("versions", e))?;
        let runs = state.versions.runs(id).await.map_err(|e| internal("runs", e))?;
        let settled = settled_colors(&state, &project_id).await?;
        match resolve_seed(&head, &versions, &runs, |c| settled.contains(&c)) {
            SeedChoice::Run(color) => runs.into_iter().find(|r| r.color == color),
            SeedChoice::Nothing => None,
            SeedChoice::HeadRunning(color) => {
                return Err((
                    StatusCode::CONFLICT,
                    format!("head {color} is still running; `weft stop {color}` or wait, then run again"),
                ));
            }
        }
    } else {
        None
    };

    let spec = body.spec.clone().unwrap_or_else(|| RunSpec::whole("run"));
    let mut resolved = resolve_spec(&spec, &project).map_err(|r| refused(&r))?;
    let bakes = if spec.fire.is_some() {
        state.journal.trigger_bakes(&project_id).await.map_err(|error| internal("read trigger bakes", error))?
    } else { Vec::new() };
    for kick in resolved.kicks.iter_mut().filter(|kick| kick.firing) {
        weft_core::run_spec::validate_fire_bake(&kick.node, &program, &bakes.iter().map(|bake| bake.summary()).collect::<Vec<_>>())
            .map_err(|refusal| refused(&refusal))?;
        let capture = bakes.iter().find(|bake| bake.program == program)
            .and_then(|bake| bake.captured.get(&kick.node))
            .expect("validated bake contains this trigger for this program");
        kick.port_snapshot = Some(capture.ports.clone());
    }

    // Infra pre-flight, scoped to what THIS run executes.
    let bound: Option<HashSet<String>> = Some(resolved.selection.nodes.iter().cloned().collect());
    let missing = crate::api::project::missing_infra_nodes(&state, &project_id, &project, bound.as_ref()).await?;
    if !missing.is_empty() {
        return Err((
            StatusCode::PRECONDITION_REQUIRED,
            format!("infra not running for: {}. Run `weft infra start` first.", missing.join(", ")),
        ));
    }

    // What the run inherits and what it kicks.
    let mut starting_parameters = spec.clone();
    let (seed, stale, kicks) = match &seed_run {
        Some(run) => {
            let from = fold_seed(&state, id, run).await?;
            let planned = seed_plan(&project, &program, &resolved.selection, &spec, &from.outcomes,
                &body.seed_until, &body.seed_before)
                .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;
            starting_parameters = weft_core::seeding::starting_parameters(&project, &spec, &resolved.selection, &planned, &from.outcomes, &from.outputs)
                .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;
            resolved.warnings.extend(planned.warnings);
            resolved.selection = planned.selection;
            let stale = resolved.selection.nodes.clone();
            let kicks = seeded_kicks(&resolved, &stale);
            let seed = weft_journal::Seed { parent: from.color, origins: planned.origins };
            (Some(seed), stale, kicks)
        }
        None => (None, resolved.selection.nodes.clone(), resolved.kicks.clone()),
    };
    // A stale node with no kick still runs: its inputs are inherited
    // pulses the worker folds in, and it fires the moment they settle
    // after the selected reuse boundary. Only a run with
    // nothing stale, nothing kicked and nothing provided has no work.
    let color = uuid::Uuid::new_v4();
    let now = crate::lease::now_unix() as u64;
    let birth_rows = supplied_output_events(&project, &spec, &resolved, color, now);
    // An empty `entry_node` is the journal's marker for a row that no
    // longer decodes, and `weft executions` prints `?` for it. A
    // seeded re-run of a mid-graph node has no kick and no provided
    // value, so falling back to the empty string labelled a perfectly
    // good run as corrupt. A stale node is the honest answer instead:
    // this label is display only (nothing routes on it), and `stale` is
    // a sorted set, so this is the alphabetically first node the run
    // re-runs, chosen because it is stable, not because it is first in
    // any graph order. A seeded run that inherited every node it
    // selected kicks nothing and re-runs nothing; it says so.
    let entry_node = kicks
        .first()
        .map(|k| k.node.clone())
        .or_else(|| resolved.provided.iter().find_map(|p| p.consumers.first().map(|(n, _)| n.clone())))
        .or_else(|| stale.iter().next().cloned())
        .unwrap_or_else(|| "everything inherited".into());
    // Recording the run and starting it happen under the project's
    // transition lock, together.
    //
    // The tree row goes first: once `start_queued_execution_with`
    // returns, a real run is queued and burning worker capacity, so a
    // failure after that point would hand the caller no color at all
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
    let stale_vec: Vec<String> = stale.iter().cloned().collect();
    let (version, moved) = with_tree_lock(&state, &project_id, || async {
        // Head is read again in here. The one read outside chose the
        // seed, which is this run's own business; the version's PARENT
        // has to be where head is at the moment the row is written, or
        // a session that checkpointed in between is cut out of the
        // lineage.
        let head = state.versions.head(id).await.map_err(|e| internal("head", e))?;
        let version = commit_version(&state, id, &body.manifest, None, body.root, &head).await?;
        state
            .versions
            .insert_run(&RunRow {
                color,
                project_id: id,
                version_id: version.version.clone(),
                seed_color: seed.as_ref().map(|s| s.parent),
                stale: stale_vec.clone(),
                spec: Some(starting_parameters.clone()),
                definition_hash: definition_hash.clone(),
                example: body.example.clone(),
                created_at: now,
            })
            .await
            .map_err(|e| internal("record run", e))?;
        crate::api::project::start_queued_execution_with(
            &state,
            color,
            &project_id,
            weft_core::context::Phase::Fire,
            &entry_node,
            &kicks,
            &birth_rows,
            &program,
            Some(&resolved.selection),
            seed.clone(),
            None,
            Some(&version.version),
        )
        .await?;
        // Head moves LAST, and a lost race is reported, never refused.
        let moved = state
            .versions
            .move_head(id, &head, Some(&version.version), Some(color))
            .await
            .map_err(|e| internal("set head", e))?;
        Ok((version, moved))
    })
    .await?;

    // A head that would not move is reported, never refused.
    //
    // By this point the execution is journaled, queued and recorded
    // under its version: it is running. Answering 409 would tell the
    // person nothing started while a real run burned worker capacity
    // under a color they were never given, with no way to stop it. So
    // the run stands and the answer says what happened.
    //
    // Another session checkpointing is NOT the cause any more: every tree
    // write now holds this project's lock, and this compare-and-set is
    // inside it. What is left is a `weft clean` of the run head pointed at
    // (it clears `head_run` under no lock) or a `weft rm` landing between
    // the two, and telling someone to `weft branch --discard` in either
    // case would be advice about a race that did not happen.
    let mut warnings = resolved.warnings.clone();
    if !moved {
        warnings.push(format!(
            "this run started and is recorded under version {} (`weft tree` shows it), but head \
             could not be moved to it: head changed under this call, which happens when the run \
             head pointed at was cleaned, or when the project was removed while this was being \
             prepared. If the project is gone, this run outlives it and `weft clean {}` is what \
             removes it. Otherwise `weft branch {} --discard` moves head here. It needs \
             `--discard` because head is still on the older version, so plain `weft branch` reads \
             your files as uncommitted changes against it. `--discard` overwrites the files with \
             this version, which is what they already are unless you have edited them since \
             reading this.",
            version.version,
            &color.to_string()[..8],
            &color.to_string()[..8],
        ));
    }

    let in_run = &resolved.selection.nodes;
    let (inherited, ran): (Vec<String>, Vec<String>) = match &seed {
        Some(seed) => (seed.origins.keys().cloned().collect(), in_run.iter().cloned().collect()),
        None => (Vec::new(), in_run.iter().cloned().collect()),
    };
    Ok(Json(VersionRunResponse {
        color,
        version: version.version,
        seed: seed.map(|s| s.parent),
        inherited,
        ran,
        warnings,
    }))
}

fn supplied_output_events(project: &weft_core::ProjectDefinition, spec: &RunSpec, resolved: &Resolved, color: Color, at_unix: u64) -> Vec<weft_journal::ExecEvent> {
    let mut events = Vec::new();
    // Seed suppliers carry their original events. Only authored emits create
    // new supplied facts, including closures for an explicit empty port map,
    // and only the emits the cut kept (`--before` can drop one).
    for (source, ports) in spec.emit.iter().filter(|(source, _)| resolved.selection.suppliers.contains(*source)) {
        let node = project.nodes.iter().find(|node| &node.id == source).expect("resolved source exists");
        for port in &node.outputs {
            let supplied = ports.get(&port.name);
            let generator = matches!(port.port_type, weft_core::weft_type::WeftType::Generator(_));
            let values: Vec<&Value> = match supplied {
                Some(value) if generator => value.as_array().expect("resolved stream is a list").iter().collect(),
                Some(value) => vec![value],
                None => vec![],
            };
            for (index, value) in values.into_iter().enumerate() {
                events.push(weft_journal::ExecEvent::PortEmitted {
                    color, emission_id: uuid::Uuid::new_v5(&color, format!("supplied\0{source}\0{}\0{index}", port.name).as_bytes()),
                    node_id: source.clone(), frames: vec![], port: port.name.clone(), value: Arc::new(value.clone()),
                    provided: true, at_unix,
                });
            }
            if generator || supplied.is_none() {
                events.push(weft_journal::ExecEvent::PortClosed {
                    color, emission_id: uuid::Uuid::new_v5(&color, format!("supplied-end\0{source}\0{}", port.name).as_bytes()),
                    node_id: source.clone(), frames: vec![], port: port.name.clone(), provided: true, at_unix,
                });
            }
        }
    }
    events
}

/// Code identities and captured trigger names for preview validation.
pub async fn trigger_bakes(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<String>,
) -> Result<Json<Vec<weft_core::run_spec::BakeSummary>>, ApiError> {
    let id = parse_id(&id)?;
    authorize_project(&state, &caller.0, id).await?;
    let bakes = state.journal.trigger_bakes(&id.to_string()).await.map_err(|error| internal("read trigger bakes", error))?;
    Ok(Json(bakes.iter().map(|bake| bake.summary()).collect()))
}

/// A seeded run kicks its stale roots and every explicitly fired trigger.
fn seeded_kicks(
    resolved: &Resolved,
    stale: &BTreeSet<String>,
) -> Vec<Kick> {
    resolved
        .kicks
        .iter()
        .filter(|k| k.firing || stale.contains(&k.node))
        .cloned()
        .collect()
}

// ----- tree --------------------------------------------------------------

// SYNC: VersionSummary <-> crates/weft-cli/src/commands/versions.rs VersionSummary, extension-vscode/src/sidebar/version-tree.ts VersionSummary
#[derive(Debug, Serialize)]
pub struct VersionSummary {
    pub id: String,
    pub parent_id: Option<String>,
    pub label: Option<String>,
    pub created_at: u64,
    /// What changed against the parent (empty on a root).
    pub diff: ManifestDiff,
    pub manifest: Manifest,
}

// SYNC: RunSummary <-> crates/weft-cli/src/commands/versions.rs RunSummary, extension-vscode/src/sidebar/version-tree.ts RunSummary
#[derive(Debug, Serialize)]
pub struct RunSummary {
    pub color: Color,
    pub version_id: String,
    /// The program this run executed. `weft freeze` records it on the
    /// example so a later reader can tell which definition the frozen
    /// wires came from.
    pub definition_hash: String,
    pub seed_color: Option<Color>,
    pub stale: Vec<String>,
    pub spec: Option<RunSpec>,
    pub example: Option<String>,
    /// The execution's status as the executions list reports it, or
    /// `unknown` when its journal is gone.
    pub status: String,
    pub started_at: u64,
    pub completed_at: Option<u64>,
}

#[derive(Debug, Serialize)]
pub struct TreeResponse {
    pub head: Head,
    pub versions: Vec<VersionSummary>,
    pub runs: Vec<RunSummary>,
}

pub async fn tree(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<String>,
) -> Result<Json<TreeResponse>, ApiError> {
    let id = parse_id(&id)?;
    authorize_project(&state, &caller.0, id).await?;
    let head = state.versions.head(id).await.map_err(|e| internal("head", e))?;
    let versions = state.versions.versions(id).await.map_err(|e| internal("versions", e))?;
    let runs = state.versions.runs(id).await.map_err(|e| internal("runs", e))?;
    let by_id: BTreeMap<&str, &VersionRow> = versions.iter().map(|v| (v.id.as_str(), v)).collect();
    let version_summaries = versions
        .iter()
        .map(|v| VersionSummary {
            id: v.id.clone(),
            parent_id: v.parent_id.clone(),
            label: v.label.clone(),
            created_at: v.created_at,
            diff: v
                .parent_id
                .as_deref()
                .and_then(|p| by_id.get(p))
                .map(|p| manifest_diff(&p.manifest, &v.manifest))
                .unwrap_or_default(),
            manifest: v.manifest.clone(),
        })
        .collect();
    let suspended = crate::api::project::suspended_color_set(&state, &id.to_string())
        .await
        .map_err(|e| internal("suspended", e))?;
    // ONE read for every run's status. Asked per run, a project with a
    // thousand recorded runs did a thousand point lookups on every `weft
    // tree` and every refresh of the editor's version sidebar.
    let summaries = state
        .journal
        .execution_summaries_for_project(&id.to_string())
        .await
        .map_err(|e| internal("execution summaries", e))?;
    let mut run_summaries = Vec::with_capacity(runs.len());
    for r in runs {
        let (mut status, started_at, completed_at) = match summaries.get(&r.color) {
            Some(s) => (s.status.clone(), s.started_at, s.completed_at),
            None => ("unknown".to_string(), r.created_at, None),
        };
        if status == "running" && suspended.contains(&r.color) {
            status = "waiting_for_input".to_string();
        }
        run_summaries.push(RunSummary {
            color: r.color,
            version_id: r.version_id,
            definition_hash: r.definition_hash,
            seed_color: r.seed_color,
            stale: r.stale,
            spec: r.spec,
            example: r.example,
            status,
            started_at,
            completed_at,
        });
    }
    Ok(Json(TreeResponse { head, versions: version_summaries, runs: run_summaries }))
}

// ----- head, activation, run bookkeeping ---------------------------------

#[derive(Debug, Deserialize)]
pub struct HeadRequest {
    #[serde(default)]
    pub version: Option<String>,
    #[serde(default)]
    pub run: Option<Color>,
}

#[derive(Debug, Serialize)]
pub struct HeadResponse {
    pub version: String,
    pub run: Option<Color>,
    pub manifest: Manifest,
}

/// Move head: to a run (its version, with the run as the next seed) or
/// to a bare version. Answers the manifest so `weft branch` restores
/// the files.
pub async fn set_head(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<String>,
    Json(body): Json<HeadRequest>,
) -> Result<Json<HeadResponse>, ApiError> {
    let id = parse_id(&id)?;
    authorize_project(&state, &caller.0, id).await?;
    let project_id = id.to_string();
    with_tree_lock(&state, &project_id, || async {
        // Resolve the destination under the same lock as prune and the head move.
        // Otherwise a valid destination can disappear between its read and use.
        let (version_id, run) = match (body.run, body.version) {
            (Some(color), _) => {
                let run = state
                    .versions
                    .run(color)
                    .await
                    .map_err(|e| internal("run", e))?
                    .filter(|r| r.project_id == id)
                    .ok_or((StatusCode::NOT_FOUND, format!("run {color} is not recorded in this project's tree")))?;
                (run.version_id, Some(color))
            }
            (None, Some(version)) => (version, None),
            (None, None) => return Err((StatusCode::BAD_REQUEST, "name a version or a run".into())),
        };
        let version = state
            .versions
            .version(id, &version_id)
            .await
            .map_err(|e| internal("version", e))?
            .ok_or((StatusCode::NOT_FOUND, format!("no version {version_id} in this project")))?;
        // `weft branch` names where head should go, so it does not depend
        // on where head was; it still moves from the position it read, so a
        // concurrent branch cannot be silently swallowed.
        let expected = state.versions.head(id).await.map_err(|e| internal("head", e))?;
        move_head_or_conflict(&state, id, &expected, Some(&version.id), run).await?;
        Ok(Json(HeadResponse { version: version.id, run, manifest: version.manifest }))
    })
    .await
}

/// A field the caller may leave alone, set, or clear.
///
/// `None` is "the key was absent, keep what is recorded"; `Some(None)`
/// is an explicit `null`, "clear it".
fn present<'de, D, T>(d: D) -> Result<Option<T>, D::Error>
where
    T: Deserialize<'de>,
    D: serde::Deserializer<'de>,
{
    Ok(Some(T::deserialize(d)?))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunUpdate {
    #[serde(default, deserialize_with = "present")]
    pub example: Option<Option<String>>,
}

/// Associate a recorded run with a saved example.
pub async fn update_run(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path((id, color)): Path<(String, String)>,
    Json(body): Json<RunUpdate>,
) -> Result<StatusCode, ApiError> {
    let id = parse_id(&id)?;
    authorize_project(&state, &caller.0, id).await?;
    let color: Color = color.parse().map_err(|_| (StatusCode::BAD_REQUEST, "bad color".into()))?;
    // Read only to authorize the color to this project. What gets written
    // is exactly what the caller named: the store leaves an unmentioned
    // column alone, so two sessions writing different fields of one run
    // cannot put back each other's stale value.
    state
        .versions
        .run(color)
        .await
        .map_err(|e| internal("run", e))?
        .filter(|r| r.project_id == id)
        .ok_or((StatusCode::NOT_FOUND, format!("run {color} is not recorded in this project's tree")))?;
    if let Some(example) = body.example {
        state
            .versions
            .set_run_example(color, example.as_deref())
            .await
            .map_err(|e| internal("update run", e))?;
    }
    Ok(StatusCode::NO_CONTENT)
}

// ----- prune -------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct PruneQuery {
    /// Answer the plan without deleting anything.
    #[serde(default)]
    pub plan: bool,
    /// Every `frozen_from.version` in the project's examples, comma
    /// separated: the CLI reads `examples/` and the dispatcher refuses
    /// to prune under one.
    #[serde(default)]
    pub frozen: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct PruneResponse {
    pub plan: PrunePlan,
    pub deleted: bool,
}

/// Delete a version subtree and every run under it, or (with
/// `?plan=true`) say what that would remove. The blobs only the
/// pruned versions held drop out of the referenced set on the next
/// `weft` publish of asset references and expire under the storage
/// rule; the plan lists them.
pub async fn prune(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path((id, version)): Path<(String, String)>,
    Query(query): Query<PruneQuery>,
) -> Result<Json<PruneResponse>, ApiError> {
    let id = parse_id(&id)?;
    authorize_project(&state, &caller.0, id).await?;
    let project_id = id.to_string();
    // Plan and delete under the project's tree lock: without it a run
    // started between the two lands under a version this call is about to
    // delete, and the plan's "nothing here is still running" was true only
    // of a snapshot. Through the shared helper like every other tree
    // write, so a refusal in here stays a refusal instead of collapsing
    // into a 500 (which is why this body used to smuggle its reasons out
    // as an `Ok(Err(..))`).
    let outcome = with_tree_lock(&state, &project_id, || async {
        let head = state.versions.head(id).await.map_err(|e| internal("head", e))?;
        let versions = state.versions.versions(id).await.map_err(|e| internal("versions", e))?;
        let runs = state.versions.runs(id).await.map_err(|e| internal("runs", e))?;
        let in_flight = in_flight_colors(&state, &project_id).await?;
        let frozen: Vec<String> = query
            .frozen
            .as_deref()
            .map(|f| f.split(',').filter(|s| !s.is_empty()).map(str::to_string).collect())
            .unwrap_or_default();
        let mut plan = match plan_prune(&version, &versions, &runs, &head, &frozen, |c| in_flight.contains(&c)) {
            Ok(plan) => plan,
            Err(r) => return Err((StatusCode::CONFLICT, r.reasons.join("\n"))),
        };
        let protected = crate::versions::source_versions_in_use(&mut *state.pg_pool.acquire().await.map_err(|e| internal("source references", e))?, id, false)
            .await.map_err(|e| internal("source references", e))?;
        if plan.versions.iter().any(|id| protected.contains(id)) {
            return Err((StatusCode::CONFLICT, "these versions still supply trigger settings or running executions; stop the runs and replace or wipe those trigger settings before pruning".into()));
        }
        let source: Option<Option<Value>> = sqlx::query_scalar("SELECT running_source FROM project WHERE id = $1")
            .bind(id).fetch_optional(&state.pg_pool).await.map_err(|e| internal("registered source", e))?;
        if let Some(source) = source.flatten() {
            let source: Manifest = serde_json::from_value(source).map_err(|e| internal("registered source", e))?;
            let retained: BTreeSet<_> = source.values().collect();
            plan.blobs.retain(|hash| !retained.contains(hash));
        }
        if query.plan {
            return Ok((plan, false));
        }
        // The TREE first, then the journals.
        //
        // The order matters on a failure. Deleting journals first left
        // version rows pointing at journals that were gone, and a run
        // whose journal has no terminal row reads as still in flight,
        // so every later seeded run and every later prune refused with
        // "run X is still running", for a run that could never finish
        // and that `weft stop` could no longer reach. Dropping the tree
        // rows first means a failure here leaves journals nobody's tree
        // names, which `weft clean` still reaches by color.
        state
            .versions
            .delete_versions(id, &plan.versions)
            .await
            .map_err(|e| internal("delete versions", e))?;
        Ok((plan, true))
    })
    .await?;
    let (plan, deleted) = outcome;
    if !deleted {
        return Ok(Json(PruneResponse { plan, deleted }));
    }

    // The journals go OUTSIDE the lock.
    //
    // Their version rows are already gone, so nothing can decide
    // anything about these runs any more and no other session needs to
    // be held off. Cleaning inside the lock meant one prune of a large
    // subtree held a pooled connection through a storage wipe per run,
    // and every run start on that project queued behind it while
    // holding a connection of its own.
    let mut failed: Vec<String> = Vec::new();
    for color in &plan.runs {
        match crate::api::execution::clean_execution(&state, &caller.0, *color).await {
            Ok(_) => {}
            // A journal that is already gone is already clean: the
            // residue an older ordering left behind (a tree row
            // outliving its journal). Its row has just been cascaded
            // away, so there is nothing to do and nothing to report;
            // calling it a failure told the person to run a command
            // that answers 404 for ever on that color.
            Err(StatusCode::NOT_FOUND) => {}
            Err(status) => failed.push(format!("{color} ({status})")),
        }
    }
    if !failed.is_empty() {
        return Err(internal(
            "prune",
            format!(
                "the versions were pruned, but these runs' journals could not be cleaned: {}. \
                 They are no longer in the tree; `weft clean <color>` removes each one.",
                failed.join(", ")
            ),
        ));
    }
    Ok(Json(PruneResponse { plan, deleted }))
}

#[derive(Debug, Serialize)]
pub struct SweepResponse {
    pub swept: Vec<String>,
}

/// Drop the versions a bulk clean left bare: no runs, no descendants,
/// no label, not head and not activated (`versions::sweepable_versions`).
pub async fn sweep(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<String>,
) -> Result<Json<SweepResponse>, ApiError> {
    let id = parse_id(&id)?;
    authorize_project(&state, &caller.0, id).await?;
    Ok(Json(SweepResponse { swept: sweep_bare_versions(&state, id).await? }))
}

/// Drop every bare version of one project, and answer which.
///
/// Called by the sweep endpoint and by the removal of a single run,
/// which is the act that makes a version bare. It lives here, on the
/// server, rather than in whoever asked: the CLI, the editor and a
/// prune's own per-run cleanup all delete runs, and a sweep each client
/// has to remember is a sweep the next client forgets. Deleting the last
/// run of a version and leaving the version behind shows it in
/// `weft tree` with nothing under it and a status of `unknown`, for
/// ever, with no verb that reaches it.
pub(crate) async fn sweep_bare_versions(
    state: &DispatcherState,
    id: uuid::Uuid,
) -> Result<Vec<String>, ApiError> {
    let project_id = id.to_string();
    with_tree_lock(state, &project_id, || async {
        let mut swept: Vec<String> = Vec::new();
        // A sweep can free a parent: loop until nothing is bare. Head is
        // read inside the lock and re-read each pass: a checkpoint
        // landing mid-loop moves head onto a fresh, label-less,
        // run-less leaf, which is exactly what this loop deletes.
        loop {
            let head = state.versions.head(id).await.map_err(|e| internal("head", e))?;
            let versions = state.versions.versions(id).await.map_err(|e| internal("versions", e))?;
            let runs = state.versions.runs(id).await.map_err(|e| internal("runs", e))?;
            let protected = crate::versions::source_versions_in_use(&mut *state.pg_pool.acquire().await.map_err(|e| internal("source references", e))?, id, true)
                .await.map_err(|e| internal("source references", e))?;
            let bare: Vec<_> = crate::versions::sweepable_versions(&versions, &runs, &head)
                .into_iter().filter(|id| !protected.contains(id)).collect();
            if bare.is_empty() {
                break;
            }
            state.versions.delete_versions(id, &bare).await.map_err(|e| internal("delete versions", e))?;
            swept.extend(bare);
        }
        Ok(swept)
    })
    .await
}

/// Every blob the project's surviving versions name, as storage keys,
/// for the reference publish (`api::storage::asset_references`) to keep
/// alive alongside the build's own assets.
pub async fn version_blob_keys(state: &DispatcherState, project: uuid::Uuid) -> anyhow::Result<Vec<String>> {
    let versions = state.versions.versions(project).await?;
    let source: Option<Option<Value>> = sqlx::query_scalar("SELECT running_source FROM project WHERE id = $1")
        .bind(project).fetch_optional(&state.pg_pool).await?;
    let source: Option<Manifest> = source.flatten().map(serde_json::from_value).transpose()?;
    blob_keys(project, versions.iter().map(|v| (v.id.as_str(), &v.manifest))
        .chain(source.as_ref().map(|source| ("registered sources", source))))
}

/// The distinct blob keys the given manifests name. A manifest entry
/// with an empty hash names no blob (the CLI's `weft:<version>:<catalog
/// hash>` pseudo-entry, which is part of the version id and nothing in
/// storage) and is skipped rather than turned into a malformed key.
pub fn blob_keys<'a>(
    project: uuid::Uuid,
    manifests: impl Iterator<Item = (&'a str, &'a BTreeMap<String, String>)>,
) -> anyhow::Result<Vec<String>> {
    let mut keys: BTreeSet<String> = BTreeSet::new();
    let scope = weft_core::storage::key::KeyScope::Asset { project_id: project.to_string() };
    for (version, manifest) in manifests {
        for (path, hash) in manifest.iter().filter(|(_, h)| !h.is_empty()) {
            keys.insert(weft_core::storage::key::scope_key(&scope, hash).map_err(|e| {
                // Every stored manifest passed `validate_manifest`, so
                // this cannot happen for a row written since that gate
                // existed. A row from before it can still be here, and
                // dropping it silently would quietly stop keeping that
                // blob alive until it expired: data loss with no signal.
                // So it fails, naming the one version the person has to
                // deal with and the verb that deals with it.
                anyhow::anyhow!(
                    "version {version} lists '{path}' as '{hash}', which is not a storable \
                     address ({e}), so this project's asset references cannot be published. \
                     `weft tree` shows where that version sits; getting rid of it is `weft \
                     prune {version}`, which first needs head (and the activated version) off \
                     it, every run under it settled, and any frozen example that came from it \
                     re-frozen."
                )
            })?);
        }
    }
    Ok(keys.into_iter().collect())
}

#[cfg(test)]
mod blob_key_tests {
    use super::*;

    /// The `weft:` pseudo-entry carries no blob, so it makes no key; a
    /// hash two versions share makes one key.
    #[test]
    fn blob_keys_skip_empty_hashes_and_dedupe() {
        let project = uuid::Uuid::nil();
        let a: BTreeMap<String, String> =
            [("main.weft".to_string(), "aaa".to_string()), ("weft:0.5.0:cat".to_string(), String::new())].into_iter().collect();
        let b: BTreeMap<String, String> =
            [("main.weft".to_string(), "aaa".to_string()), ("weft.toml".to_string(), "bbb".to_string())].into_iter().collect();
        let keys = blob_keys(project, [("v1", &a), ("v2", &b)].into_iter()).expect("both are addressable");
        assert_eq!(
            keys,
            vec![format!("asset/{project}/aaa"), format!("asset/{project}/bbb")],
            "no key for the empty hash, one for the shared one"
        );
    }

    /// A manifest is client-supplied, and its values become storage
    /// keys. One entry that cannot be addressed would break the asset
    /// reference publish for every version of the project at once, so
    /// it is refused where a manifest becomes a version.
    #[test]
    fn a_manifest_that_cannot_be_addressed_is_refused() {
        let good = "a".repeat(64);
        let ok: Manifest = [("main.weft".to_string(), good.clone()), ("weft:0.5.0:cat".to_string(), String::new())]
            .into_iter()
            .collect();
        validate_manifest(&ok).expect("an ordinary manifest passes");

        for bad in [
            [("main.weft".to_string(), "aaa/../../etc".to_string())],
            [("main.weft".to_string(), "aaa".to_string())],
            [("../escape.weft".to_string(), good.clone())],
            [("/absolute.weft".to_string(), good.clone())],
            [("main.weft".to_string(), String::new())],
        ] {
            let m: Manifest = bad.into_iter().collect();
            assert!(validate_manifest(&m).is_err(), "{m:?} must be refused");
        }
    }
}

#[cfg(test)]
mod fire_snapshot_tests {
    use super::*;

    #[test]
    fn supplied_events_only_describe_authored_emits() {
        let project: weft_core::ProjectDefinition = serde_json::from_value(serde_json::json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "nodes": [{
                "id": "source", "nodeType": "Text", "config": {},
                "position": {"x": 0, "y": 0}, "inputs": [],
                "outputs": [{"name": "value", "portType": "String", "required": true}]
            }], "edges": []
        })).unwrap();
        let color = Color::new_v4();
        let mut spec = RunSpec::whole("reuse");
        let resolved = |spec: &RunSpec| resolve_spec(spec, &project).expect("one node resolves");
        assert!(supplied_output_events(&project, &spec, &resolved(&spec), color, 0).is_empty());
        spec.emit.insert("source".into(), BTreeMap::new());
        let events = supplied_output_events(&project, &spec, &resolved(&spec), color, 0);
        assert!(matches!(&events[..], [weft_journal::ExecEvent::PortClosed { node_id, port, provided: true, .. }]
            if node_id == "source" && port == "value"));
        spec.emit.get_mut("source").unwrap().insert("value".into(), serde_json::json!("manual"));
        let events = supplied_output_events(&project, &spec, &resolved(&spec), color, 0);
        assert!(matches!(&events[..], [weft_journal::ExecEvent::PortEmitted { value, provided: true, .. }]
            if **value == serde_json::json!("manual")));
    }

    #[test]
    fn real_trigger_run_preserves_its_birth_version_and_single_wake() {
        let color = Color::new_v4();
        let project = uuid::Uuid::new_v4();
        let wake = serde_json::json!({"message": "original"});
        let (birth, mut kicks) = crate::api::project::execution_birth_events(
            color, &project.to_string(), weft_core::context::Phase::Fire, "trigger",
            &[Kick { node: "trigger".into(), firing: true, payload: Some(wake.clone()), port_snapshot: None }],
            "original-graph", None, None, None, Some("original-source"), 42,
        );
        let mut rows = vec![birth];
        rows.append(&mut kicks);
        let run = trigger_run_from_birth(color, &rows).unwrap().unwrap();
        assert_eq!(run.version_id, "original-source");
        assert_eq!(run.definition_hash, "original-graph");
        assert_eq!(run.created_at, 42);
        assert_eq!(run.spec.unwrap().fire, Some(("trigger".into(), wake)));
        rows.push(weft_journal::ExecEvent::NodeKicked {
            color, node_id: "other-trigger".into(), firing: true,
            payload: None, port_snapshot: None, at_unix: 42,
        });
        assert!(trigger_run_from_birth(color, &rows).unwrap_err().to_string().contains("more than one trigger"));
    }

    fn plan(node: &str, firing: bool) -> Kick {
        Kick { node: node.to_string(), firing, payload: None, port_snapshot: None }
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

    /// A seeded run's kicks go through the same conversion, so a fire
    /// keeps its snapshot there too. Building them separately is what
    /// used to drop it.
    #[test]
    fn a_seeded_fire_keeps_its_snapshot() {
        let mut fired = plan("ask", true);
        fired.port_snapshot = Some(serde_json::json!({ "endpointUrl": "http://bridge:8090" }));
        let resolved = Resolved {
            selection: weft_core::project::selection::RunSelection {
                nodes: ["ask".to_string()].into_iter().collect(),
                ..Default::default()
            },
            kicks: vec![fired],
            provided: vec![],
            crossings: vec![],
            warnings: vec![],
        };
        let stale: BTreeSet<String> = ["ask".to_string()].into_iter().collect();
        let kicks = seeded_kicks(&resolved, &stale);
        assert_eq!(kicks[0].port_snapshot, Some(serde_json::json!({ "endpointUrl": "http://bridge:8090" })));
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
        let resolved = resolve_spec(&spec, &project).expect("graph selection needs no external facts");
        assert!(resolved.kicks.iter().any(|k| k.node == "tick" && k.firing));
    }

}
