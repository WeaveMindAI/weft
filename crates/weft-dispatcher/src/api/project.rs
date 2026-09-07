//! Project lifecycle HTTP handlers. `POST /projects` registers a
//! project; `POST /projects/{id}/run` kicks off a fresh execution.
//! `weft run` on the CLI calls these.

use std::collections::HashSet;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use weft_core::project::{downstream_closure, infra_ids, run_subgraph, subgraph_roots, trigger_ids, EdgeIndex};
use weft_core::ProjectDefinition;

use crate::authenticator::{authorize_project, CallerTenant};
use crate::events::DispatcherEvent;
use crate::state::DispatcherState;

/// One root node that needs to be "kicked into life" at the start of
/// an execution: a node with no wired inputs in the active subgraph
/// (a firing trigger, a manual-run entry, an InfraSetup root). The
/// dispatcher journals one `ExecEvent::NodeKicked` per `Kick`; the
/// worker's fold turns these into the `kicked` set the scheduler
/// dispatches at frames=[].
///
/// `payload` is the wake event's data (HTTP body, SSE message JSON,
/// form submission, timer info) ONLY for the firing trigger. Every
/// other kicked root has `payload: None`.
#[derive(Debug, Clone)]
pub struct Kick {
    pub node_id: String,
    /// This kick is the firing trigger of the execution. Explicit (see
    /// `ExecEvent::NodeKicked::firing` for why payload presence can't
    /// carry it).
    pub firing: bool,
    pub payload: Option<Value>,
    /// The firing trigger's setup-time port snapshot (from its signal
    /// row). `None` for every other kicked root.
    pub port_snapshot: Option<Value>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ProjectSummary {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub description: String,
    pub status: String,
}

impl From<crate::project_store::StoredProjectSummary> for ProjectSummary {
    fn from(p: crate::project_store::StoredProjectSummary) -> Self {
        Self {
            id: p.id.to_string(),
            name: p.name,
            description: p.description,
            status: p.status.as_str().to_string(),
        }
    }
}

/// `GET /images/referenced`: every image the system still needs, across
/// ALL tenants, as one object with two lists. `workerHashes` is the union
/// of each project's `running_binary_hash` (what a fresh spawn would run),
/// the `binary_hash` of every non-terminal worker pod (a pod draining
/// in-flight work may still run an image its project no longer points
/// at), and the `binary_hash` stamped on every pending/claimed task (a
/// task can outlive both the project pointer and its pod between a
/// resync and the cold-start sweep that fails superseded tasks).
/// `infraRefs` is the union of every project's complete infra image-tag
/// map (`infra_image_tags_json`, written atomically with the running-hash
/// trio on every infra sync) AND the image refs recorded on every
/// `infra_node` unit (the infra mirror of the draining-pod rule: an UP
/// unit is deliberately left frozen at its old image across syncs until
/// it is force-stopped, so its recorded ref can be older than the
/// project's current map and must stay referenced while the unit runs).
/// Both lists hold BARE refs (`weft-worker:<hash>` suffixes and
/// `weft-infra-<name>:<hash>` full refs; the CLI's tag policy never
/// writes a registry prefix into either). THE authority image
/// reclamation deletes against: `weft clean --images` keeps exactly this
/// set and reclaims everything else, so under-reporting here deletes an
/// image something still runs.
///
/// Control-plane only: the set spans every tenant, so no single tenant may
/// read it (a public build-activity oracle otherwise).
// SYNC: response shape ({"workerHashes": [bare worker-hash strings],
//       "infraRefs": [bare weft-infra-<name>:<hash> refs]}) <->
//       crates/weft-cli/src/images.rs referenced_images
#[derive(Debug, serde::Serialize)]
pub struct ReferencedImages {
    #[serde(rename = "workerHashes")]
    pub worker_hashes: Vec<String>,
    #[serde(rename = "infraRefs")]
    pub infra_refs: Vec<String>,
}

pub async fn referenced_images(
    State(state): State<DispatcherState>,
    _ops: crate::authenticator::ControlPlaneCaller,
) -> Result<Json<ReferencedImages>, (StatusCode, String)> {
    referenced_images_query(&state.pg_pool)
        .await
        .map(Json)
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("referenced images: {e}"),
            )
        })
}

/// The queries behind `GET /images/referenced` (see `referenced_images`
/// for the contract). Split out so the layer-3 db tests exercise the exact
/// SQL the endpoint serves.
pub async fn referenced_images_query(
    pool: &sqlx::PgPool,
) -> anyhow::Result<ReferencedImages> {
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT DISTINCT running_binary_hash FROM project \
         WHERE running_binary_hash IS NOT NULL AND running_binary_hash <> '' \
         UNION \
         SELECT DISTINCT binary_hash FROM worker_pod \
         WHERE terminal_at_unix IS NULL AND binary_hash <> '' \
         UNION \
         SELECT DISTINCT binary_hash FROM task \
         WHERE status IN ('pending', 'claimed') \
           AND binary_hash IS NOT NULL AND binary_hash <> ''",
    )
    .fetch_all(pool)
    .await?;
    // One row per project, each the COMPLETE per-node tag map written by
    // its last committed infra sync. Rows are small and few (one per
    // project), so decoding in Rust beats a JSON-path query nobody can
    // read. The canonical decode names the project in its error, so one
    // corrupt row among many is diagnosable, and a row that fails it is a
    // loud error, never a skipped keep-set entry (skipping it would
    // condemn images the supervisor may still apply). Blank refs are
    // skipped, mirroring the `<> ''` convention of the worker-hash arms
    // (a blank matches no image; keeping it would only launder a
    // buggy-writer blank into the set).
    let tag_rows: Vec<(uuid::Uuid, serde_json::Value)> =
        sqlx::query_as("SELECT id, infra_image_tags_json FROM project")
            .fetch_all(pool)
            .await?;
    let mut infra_refs = std::collections::BTreeSet::new();
    for (project_id, tags) in tag_rows {
        let by_node = weft_broker_client::protocol::decode_infra_image_tags(
            tags,
            &format!("project={project_id}"),
        )?;
        for node_tags in by_node.values() {
            for tag in node_tags.values() {
                if !tag.is_empty() {
                    infra_refs.insert(tag.clone());
                }
            }
        }
    }
    // The refs live units actually run, recorded per unit on the
    // infra_node row at every apply (see `UnitRuntime::image_refs`).
    // Covers what the project-row map cannot: an UP unit frozen at an
    // older image across one or more syncs keeps running that ref until
    // it is force-stopped, so its old ref must stay referenced. Units
    // recorded before the field existed (or frozen since before it did)
    // contribute nothing until their next apply stamps them; the map's
    // current refs already cover everything reconciled from now on.
    // All rows, all units, regardless of status: over-keeping a ref is
    // bounded (rows die with the node on terminate) and the safe
    // direction for a set reclamation deletes against.
    let unit_rows: Vec<(String, String, serde_json::Value)> =
        sqlx::query_as("SELECT project_id, node_id, units_json FROM infra_node")
            .fetch_all(pool)
            .await?;
    for (project_id, node_id, units) in unit_rows {
        let by_unit =
            weft_broker_client::protocol::decode_units_json(units, &project_id, &node_id)?;
        for runtime in by_unit.into_values() {
            for image_ref in runtime.image_refs {
                if !image_ref.is_empty() {
                    infra_refs.insert(image_ref);
                }
            }
        }
    }
    Ok(ReferencedImages {
        worker_hashes: rows.into_iter().map(|(h,)| h).collect(),
        infra_refs: infra_refs.into_iter().collect(),
    })
}

pub async fn list(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
) -> Result<Json<Vec<ProjectSummary>>, (StatusCode, String)> {
    let items = state
        .projects
        .list(caller.0.as_str())
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("list projects: {e}")))?;
    Ok(Json(items.into_iter().map(ProjectSummary::from).collect()))
}

/// Payload accepted by `POST /projects`. The client (CLI) sends an
/// already compiled + enriched `ProjectDefinition`; the dispatcher
/// stores it and provisions namespaces. The dispatcher does NO
/// node-aware work: it has no access to the project's `nodes/` (those
/// live on the user's machine), so compilation and enrichment, which
/// need the catalog, happen entirely client-side. The dispatcher is a
/// dumb store of the compiled artifact.
#[derive(Debug, Deserialize)]
pub struct RegisterRequest {
    pub id: uuid::Uuid,
    pub name: String,
    /// Free-text project description (metadata only). The CLI has no notion of
    /// one today, so it's optional and defaults to empty; a future CLI flag can
    /// set it without a wire change.
    #[serde(default)]
    pub description: String,
    pub definition: ProjectDefinition,
    /// Binary hash of the worker image. CLI computes from engine
    /// workspace + node-type set + node impls + weft.toml; dispatcher
    /// persists on the project row (used as worker docker tag
    /// suffix). Flips on engine / node-impl / type-set changes, NOT
    /// on per-node config or topology edits. Optional in tests;
    /// production paths always set it.
    #[serde(default, rename = "binaryHash")]
    pub binary_hash: Option<String>,
    /// Definition hash. CLI computes from the canonical
    /// `ProjectDefinition` (topology + configs); dispatcher persists
    /// as the runtime-shape identity. Used as the broker fetch key
    /// (worker reads it back at execution claim time so the engine
    /// is guaranteed to run on the version the user clicked Run
    /// against). Optional in tests; production paths always set it.
    #[serde(default, rename = "definitionHash")]
    pub definition_hash: Option<String>,
    /// Infra hash. CLI computes from infra-closure source +
    /// workspace; dispatcher persists for the upgrade drift signal.
    /// Optional in tests; production paths set it whenever they set
    /// `binary_hash` so all three signals stay in sync.
    #[serde(default, rename = "infraHash")]
    pub infra_hash: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct RegisterError {
    pub error: String,
}

pub fn register_internal_error(msg: String) -> (StatusCode, Json<RegisterError>) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(RegisterError { error: msg }),
    )
}

pub fn registered_summary(summary: crate::project_store::StoredProjectSummary) -> Json<ProjectSummary> {
    Json(ProjectSummary::from(summary))
}

/// `POST /projects/register` (the standalone / CLI door). The CLI does all
/// node-aware work (compile + enrich + build the worker image against the
/// local `nodes/`) and hands over the finished `ProjectDefinition` + the three
/// hashes it computed locally; the dispatcher stores the artifact + the source
/// and advances the running hashes. No build task is enqueued here: the CLI
/// already produced the image (loaded into the local cluster).
///
/// Register creates NO namespace. Storage is a shared pooled pod in the
/// control-plane namespace (placed lazily on first write, walled by the tenant
/// prefix in the key); workers/infra get a PROJECT namespace only when the
/// project declares infra (see `api::infra`). A project is created under the
/// CALLER's tenant (the authoritative owner); a re-register under a different
/// tenant is rejected inside `register_with_hashes`.
///
/// The row + history row + running-hash pointers commit in ONE transaction;
/// `has_infra` is derived from the definition inside `register_with_hashes`.
pub async fn register(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Json(req): Json<RegisterRequest>,
) -> Result<Json<ProjectSummary>, (StatusCode, Json<RegisterError>)> {
    let mut project = req.definition;
    project.id = req.id;
    let name = req.name;
    let tenant = caller.0.clone();

    let summary = state
        .projects
        .register_with_hashes(
            project,
            &name,
            &req.description,
            tenant.as_str(),
            req.binary_hash.as_deref(),
            req.definition_hash.as_deref(),
            req.infra_hash.as_deref(),
            // Registration precedes /infra/sync, which is what writes the infra
            // image tags; nothing to persist here.
            None,
        )
        .await
        .map_err(|e| register_internal_error(format!("register_with_hashes: {e}")))?;

    // This register path stores NO source of its own: the project lives as a
    // folder on the user's disk.

    state
        .events
        .publish(DispatcherEvent::ProjectRegistered {
            project_id: summary.id.to_string(),
            name: weft_core::truncate_user_string(&summary.name, 4096),
        })
        .await;
    Ok(registered_summary(summary))
}

pub async fn get(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<String>,
) -> Result<Json<ProjectSummary>, StatusCode> {
    let id = id.parse::<uuid::Uuid>().map_err(|_| StatusCode::BAD_REQUEST)?;
    authorize_project(&state, &caller.0, id)
        .await
        .map_err(|(s, _)| s)?;
    let summary = state
        .projects
        .get(id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;
    Ok(Json(ProjectSummary::from(summary)))
}

#[derive(Debug, Default, Deserialize)]
pub struct RemoveQuery {
    /// `weft rm --force` sets this. When true, the dispatcher skips
    /// the supervisor terminate-wait window and proceeds straight to
    /// namespace deletion. Use when the supervisor is wedged and the
    /// user wants the project gone NOW.
    #[serde(default)]
    pub force: bool,
}

pub async fn remove(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<String>,
    axum::extract::Query(query): axum::extract::Query<RemoveQuery>,
) -> Result<StatusCode, StatusError> {
    let id = id
        .parse::<uuid::Uuid>()
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    // Marked gate, not `authorize_project`: rm is a delete, so the CLI
    // retries it idempotently and needs the `x-weft-not-found` marker
    // to treat "already gone" as the desired end state instead of an
    // error (a headerless 404 must still bubble as version skew).
    authorize_project_marked(&state, &caller.0, id).await?;
    // Deactivate first: cancels any in-flight executions, unregisters
    // every wake signal (entry + resume) from the tenant's listener,
    // drops entry tokens. If deactivate fails on a DB / listener
    // error we must abort: removing the project row while signals
    // remain in the listener leaves dangling registrations that
    // outlive their owning project.
    deactivate_project(&state, id).await?;
    // The project's OWNING tenant is its `project.tenant_id`, the same source that
    // keyed everything of the project's that lives outside the project row: its
    // stored data, and the connections its nodes published. `tenant_router` is a
    // request-routing lookup (the default returns `local` for every project), NOT
    // the resource owner, so using it here would key those by the wrong tenant and
    // find nothing.
    //
    // Resolved ONCE, here, before anything starts deleting: every step below needs
    // it, and a lookup repeated after a step has already run could come back empty
    // and leave that step silently skipped.
    let tenant = state
        .projects
        .tenant_for(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("tenant_for: {e:#}")))?
        // The row vanished mid-remove (a concurrent rm won): that IS
        // rm's desired end state, so it gets the marker too.
        .ok_or(StatusError::NotMyProject)?;
    // Tear down infra: issues a supervisor terminate command, waits
    // up to 120s for completion (unless --force), then drops all
    // infra_* rows and deletes the project namespace. MUST succeed:
    // if any of the DB cascade writes fail, the project row stays
    // (so a retry replays cleanly). Step 2 (supervisor wait) and
    // step 4 (namespace delete) inside `delete_project` are still
    // log-and-continue for the cluster-unreachable case; only DB
    // writes are fail-loud.
    crate::api::infra::delete_project(&state, id, &tenant, query.force).await?;
    // Reclaim the project's instance-specific stored data through the ONE hook.
    // The default frees the project's `project/`-scoped runtime files; a reclaimer
    // that also stores project content elsewhere (a versioned project-editing
    // history) extends it. `shared/`-scoped runtime files are the owner's and are
    // deliberately NOT touched (they outlive the project). MUST run before the
    // project row is dropped: a row-cascade would otherwise erase the bookkeeping
    // this reclaim reads, stranding bytes. A failure aborts the rm (a retry
    // replays cleanly).
    state
        .project_reclaimer
        .reclaim(&state, tenant.as_str(), id)
        .await
        .map_err(|e| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                // `{e:#}` prints the full anyhow cause chain, so the real reason
                // (a SQL error, a ledger inconsistency, a missing tree) reaches the
                // caller instead of just the top wrapper.
                format!("could not reclaim the project's stored data: {e:#}; retry `weft rm`"),
            )
        })?;
    let removed = state
        .projects
        .remove(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("remove: {e}")))?;
    if removed {
        Ok(StatusCode::NO_CONTENT)
    } else {
        // A concurrent rm dropped the row after our gate: still rm's
        // desired end state, so it gets the marker.
        Err(StatusError::NotMyProject)
    }
}

#[derive(Debug, Deserialize)]
// A field this struct no longer has (an old client's `entry_node`) is a
// loud 422, never a silently different run than the caller asked for.
#[serde(deny_unknown_fields)]
pub struct RunRequest {
    /// Run only what these nodes need: their upstream closure (a scope
    /// brings its whole body), which is both the set of roots kicked
    /// and the boundary the run is held to. Several targets run the
    /// union of their closures. Empty means the whole project, which
    /// is the ordinary run. Any node can be a target; an unknown name
    /// is refused.
    #[serde(default)]
    pub targets: Vec<String>,
    /// Initial payload for the entry node's first pulse.
    #[serde(default)]
    pub payload: Value,
}

#[derive(Debug, Serialize)]
pub struct RunResponse {
    pub color: String,
}

/// The project's CURRENT definition as one coherent (hash, shape)
/// pair: read `running_definition_hash` once, then fetch the
/// definition recorded under THAT hash. Every execution-starting path
/// MUST use this instead of pairing a live `project_json` read with a
/// separate hash read: a concurrent re-register between two separate
/// reads would journal hash B while the kick set was computed from
/// shape A, and the worker (which fetches the definition BY the
/// journaled hash) would fold kicks that may not exist in the shape
/// it actually runs.
pub(crate) async fn coherent_definition(
    state: &DispatcherState,
    id: uuid::Uuid,
) -> Result<(String, ProjectDefinition), (StatusCode, String)> {
    // Verb auto-build: when a builder is present, make the project's latest saved
    // source runnable BEFORE reading the running definition,
    // so clicking run/infra on a not-yet-built (or edited-since-built) project builds
    // it first instead of 412-ing. A cheap no-op when already current; when a REAL
    // build starts, the gated wrapper flips the project's `building` transition
    // (single-flight, heartbeat, cancellable via /cancel-build). When no builder is
    // configured (the CLI already built + registered), this is skipped and the
    // existing "register first" precondition below still stands.
    crate::transition::ensure_built_gated(state, id).await?;
    let hash = state
        .projects
        .running_definition_hash(id)
        .await
        .map_err(|e| {
            (StatusCode::INTERNAL_SERVER_ERROR, format!("running_definition_hash: {e}"))
        })?
        .ok_or_else(|| {
            (
                StatusCode::PRECONDITION_FAILED,
                format!(
                    "project {id} has no definition_hash; register the project before \
                     starting an execution"
                ),
            )
        })?;
    let json = state
        .projects
        .definition_for_hash(id, &hash)
        .await
        .map_err(|e| {
            (StatusCode::INTERNAL_SERVER_ERROR, format!("definition_for_hash: {e}"))
        })?
        .ok_or_else(|| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!(
                    "project {id} has no recorded definition for hash {hash}; the \
                     definition history must cover the running hash"
                ),
            )
        })?;
    let project: ProjectDefinition = serde_json::from_str(&json).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("parse recorded definition {hash}: {e}"),
        )
    })?;
    Ok((hash, project))
}

/// Start a fresh execution for a registered project.
///
/// Manual-run semantics: kick every root of the graph (a node no wire
/// feeds, outside any scope; a scope's own roots are the scope
/// launcher's, fired when the scope starts) and let pulses run
/// whatever they reach. `body.targets` narrows the run to the named
/// nodes' upstream closure: only its roots are kicked, and the run is
/// bounded to it.
pub async fn run(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<String>,
    Json(body): Json<RunRequest>,
) -> Result<Json<RunResponse>, (StatusCode, String)> {
    let id = id.parse::<uuid::Uuid>().map_err(|_| (StatusCode::BAD_REQUEST, "bad id".into()))?;
    authorize_project(&state, &caller.0, id).await?;
    // Enforce against the reconciliation BEFORE the auto-build: a
    // stale tab firing `run` into a transitional project (activating /
    // building / infra mid-flip) is rejected here, and never triggers
    // a build as a side effect of a rejected verb.
    require_action(&state, id, &["run"]).await?;
    let (definition_hash, project) = coherent_definition(&state, id).await?;
    let project_id = id.to_string();

    // The whole graph, or what the named targets need.
    for target in &body.targets {
        // Caller-supplied strings, bounded before they echo back.
        let shown = weft_core::truncate_user_string(target, 256);
        if !project.nodes.iter().any(|n| &n.id == target) {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("no node '{shown}' in this project"),
            ));
        }
    }
    let subgraph = if body.targets.is_empty() {
        RunSubgraph::whole(&project)
    } else {
        RunSubgraph::of(&project, &body.targets)
    };

    // Pre-flight, scoped to what THIS run executes: every
    // `requires_infra` node inside the subgraph must be Running (the
    // node body's `ctx.endpoint(...)` deep in execute() would fail
    // with a confusing "endpoint not available" otherwise). Infra
    // outside the subgraph never runs here, so it does not gate: a
    // maintenance branch with no infra fires whether or not the rest
    // of the project's infra is up.
    // Only an AIMED run gates a subset of the infra: an untargeted run
    // gates the whole graph, because pulses will flow wherever the
    // wiring takes them.
    let bound = if body.targets.is_empty() { None } else { Some(&subgraph.nodes) };
    let missing = missing_infra_nodes(&state, &project_id, &project, bound).await?;
    if !missing.is_empty() {
        return Err((
            StatusCode::PRECONDITION_REQUIRED,
            format!(
                "infra not running for: {}. Run `weft infra start` first.",
                missing.join(", ")
            ),
        ));
    }

    let kicks = subgraph.root_kicks(&project, &body.payload, None);
    if kicks.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "computed subgraph has no roots; graph may be cyclic or malformed".into(),
        ));
    }

    let color = uuid::Uuid::new_v4();

    // ExecutionStarted carries a single `entry_node`. When the
    // subgraph has many roots, the first kick's node wins for display
    // purposes; the NodeKicked events below carry the full root set.
    let entry_node_for_journal = kicks[0].node_id.clone();

    // Event-sourced log: ExecutionStarted + one NodeKicked per root.
    // The worker's journal fold turns these into the kicked set the
    // scheduler dispatches at frames=[]. The enqueue targets the worker
    // pool; the cold-start trigger spawns a Pod for this project if none
    // is alive, and the worker's claim loop folds the journal and runs.
    //
    // `definition_hash` is snapshotted on ExecutionStarted AND on
    // the enqueued task payload (same value, from the same
    // `coherent_definition` pair the kicks were computed from). The
    // journal field is the load-bearing one: a resume task enqueued
    // LATER (after the user has edited and re-registered) reads the
    // hash from the journal's ExecutionStarted, NOT from the project
    // row, so a suspended execution is always resumed against the
    // shape it was started on.
    start_queued_execution(
        &state,
        color,
        &id.to_string(),
        weft_core::context::Phase::Fire,
        &entry_node_for_journal,
        &kicks,
        &definition_hash,
        // An AIMED run journals its subgraph as the boundary: the
        // targets' upstream closure, closed over scopes. The engine
        // holds the run to it and absorbs, silently, a pulse into
        // anything outside, so a root shared with another branch (a
        // database feeding the whole file) never drags that branch in.
        // "Start only from what these nodes need" means exactly that
        // set. An untargeted run journals None: the user asked for the
        // whole file, and pulses go wherever the wiring takes them.
        bound,
    )
    .await?;

    // The journal bridge publishes the recorded start with the same identity
    // replay uses. A second direct notification would be an indistinguishable
    // duplicate with no journal identity, so there is only one delivery path.

    Ok(Json(RunResponse { color: color.to_string() }))
}

/// The subgraph one execution runs: the upstream closure of its
/// targets with triggers as terminators, closed over scopes (a node
/// inside a group brings the whole group: everything inside a scope
/// runs when the scope starts), next to the edge index and trigger
/// set every consumer of it needs. Built once per request; manual runs
/// and trigger fires both go through it, so "what runs" and "what gets
/// kicked" can never disagree.
// SYNC: RunSubgraph::of <-> packages/weft-graph/src/webview/lib/run-targets.ts runTargetFacts
struct RunSubgraph {
    edge_idx: EdgeIndex,
    triggers: HashSet<String>,
    /// Every node the execution dispatches; journaled as its scope.
    nodes: HashSet<String>,
}

impl RunSubgraph {
    /// The edge index and trigger set alone, for a caller (the fire
    /// path) that has to pick its targets before it can aim.
    fn graph_base(project: &ProjectDefinition) -> (EdgeIndex, HashSet<String>) {
        (EdgeIndex::build(project), trigger_ids(project).into_iter().collect())
    }

    fn aimed(
        project: &ProjectDefinition,
        edge_idx: EdgeIndex,
        triggers: HashSet<String>,
        targets: &[String],
    ) -> Self {
        let nodes = run_subgraph(project, &edge_idx, targets, &triggers);
        Self { edge_idx, triggers, nodes }
    }

    fn of(project: &ProjectDefinition, targets: &[String]) -> Self {
        let (edge_idx, triggers) = Self::graph_base(project);
        Self::aimed(project, edge_idx, triggers, targets)
    }

    /// The whole graph: an untargeted manual run.
    fn whole(project: &ProjectDefinition) -> Self {
        let (edge_idx, triggers) = Self::graph_base(project);
        let nodes = project.nodes.iter().map(|n| n.id.clone()).collect();
        Self { edge_idx, triggers, nodes }
    }

    /// One `Kick` per root of the subgraph: a node with no in-subgraph
    /// parent that sits outside every scope (a scope's own roots are
    /// fired by the scope launcher when the scope starts, never here),
    /// plus every trigger (always a root: it was a terminator, and a
    /// trigger inside a group is still kicked by its fire).
    ///
    /// `firing = Some((trigger, snapshot))` is fire time: only that
    /// trigger carries the payload (its wake body) and the snapshot.
    ///
    /// `firing = None` is a manual run: there is no firing trigger, so
    /// every trigger in the subgraph is kicked payload-less, which the
    /// engine turns into "close all its output ports" so trigger-fed
    /// branches prune via the skip cascade. A non-null `payload` lands
    /// on every non-trigger root (per-target mocks); plain manual runs
    /// pass `Value::Null` ("kick alive").
    fn root_kicks(
        &self,
        project: &ProjectDefinition,
        payload: &Value,
        firing: Option<(&str, Option<&Value>)>,
    ) -> Vec<Kick> {
        subgraph_roots(project, &self.edge_idx, &self.nodes, &self.triggers)
            .into_iter()
            .map(|id| match firing {
                Some((firing_id, snapshot)) if id == firing_id => Kick {
                    node_id: id,
                    firing: true,
                    payload: Some(payload.clone()),
                    port_snapshot: snapshot.cloned(),
                },
                Some(_) => Kick { node_id: id, firing: false, payload: None, port_snapshot: None },
                None => Kick {
                    payload: if payload.is_null() || self.triggers.contains(&id) {
                        None
                    } else {
                        Some(payload.clone())
                    },
                    node_id: id,
                    firing: false,
                    port_snapshot: None,
                },
            })
            .collect()
    }
}

/// For each `requires_infra` node in the project, list the
/// `is_trigger` nodes that have it in their upstream closure.
///
/// Re-export from weft-core. The function lives there because both
/// the dispatcher (per-node safety check) and the broker
/// (supervisor_trigger_deps) need it; one definition, no clones.
pub use weft_core::project::compute_trigger_deps;

/// Kicks for an InfraSetup-phase sub-execution: the roots of the run
/// subgraph seeded by every `requires_infra` node, NOT the infra nodes
/// themselves. Without this, "text → compute_url → provision_infra"
/// graphs would skip the text/compute_url path, and the infra node's
/// `provision()` body wouldn't see those upstream values as input. An
/// infra node inside a group is reached through its scope, so the kick
/// is the group's In boundary (see `weft_core::project::run_subgraph`).
///
/// Returns an empty vec if the project has no infra nodes (the
/// caller short-circuits : no InfraSetup execution needed).
pub fn compute_infra_setup_kicks(project: &ProjectDefinition) -> Vec<Kick> {
    setup_kicks(project, infra_ids(project))
}

/// Kicks for a TriggerSetup-phase sub-execution: the roots of the run
/// subgraph seeded by every trigger. Triggers call `ctx.register_signal`
/// under this phase; infra nodes return their `/outputs`; regular
/// upstream nodes do their normal work.
///
/// Returns an empty vec if the project has no triggers (activate is
/// a no-op in that case).
pub fn compute_trigger_setup_kicks(project: &ProjectDefinition) -> Vec<Kick> {
    setup_kicks(project, trigger_ids(project))
}

/// A setup phase kicks the roots of `seeds`' run subgraph, payload-less
/// and with no terminators (a trigger's setup needs its inputs). The
/// engine bounds the same phase with the same subgraph.
fn setup_kicks(project: &ProjectDefinition, seeds: Vec<String>) -> Vec<Kick> {
    if seeds.is_empty() {
        return Vec::new();
    }
    let edge_idx = EdgeIndex::build(project);
    let in_subgraph = run_subgraph(project, &edge_idx, &seeds, &HashSet::new());
    subgraph_roots(project, &edge_idx, &in_subgraph, &HashSet::new())
        .into_iter()
        .map(|id| Kick {
            node_id: id,
            firing: false,
            payload: None,
            port_snapshot: None,
        })
        .collect()
}

/// Non-terminal InfraSetup colors for the project. The journaled
/// non-terminal color IS the durable "infra sync in flight" state:
/// cancellable via the per-color cancel, crash-recovered by the
/// orphaned-task reaper, visible to every dispatcher Pod. Sync rejects
/// while any exists (two concurrent syncs would race the provisioning
/// subworkflow); the infra-cancel verb interrupts them.
pub(crate) async fn non_terminal_infra_setup_colors(
    state: &DispatcherState,
    project_id: &str,
) -> anyhow::Result<Vec<weft_core::Color>> {
    use sqlx::Row;
    let rows = sqlx::query(
        "SELECT ec.color FROM execution_color ec \
         WHERE ec.project_id = $1 AND ec.phase = 'infra_setup' \
           AND NOT EXISTS ( \
             SELECT 1 FROM exec_event e \
             WHERE e.color = ec.color \
               AND e.kind IN ('execution_completed', 'execution_failed', 'execution_cancelled') \
           )",
    )
    .bind(project_id)
    .fetch_all(&state.pg_pool)
    .await?;
    let mut out = Vec::new();
    for row in rows {
        let color_str: String = row.try_get("color")?;
        match color_str.parse::<weft_core::Color>() {
            Ok(c) => out.push(c),
            Err(e) => {
                tracing::warn!(
                    target: "weft_dispatcher::api::project",
                    project_id, %color_str, error = %e,
                    "skipping infra_setup color with bad uuid"
                );
            }
        }
    }
    Ok(out)
}

/// Whether an InfraSetup provisioning execution is in flight.
pub(crate) async fn infra_setup_in_flight(
    state: &DispatcherState,
    project_id: &str,
) -> anyhow::Result<bool> {
    Ok(!non_terminal_infra_setup_colors(state, project_id).await?.is_empty())
}

/// A started InfraSetup sub-execution: the color to await plus the
/// event subscription opened BEFORE the enqueue (so the worker can't
/// beat the waiter to the terminal event).
pub struct InfraSetupRun {
    color: weft_core::Color,
    events: tokio::sync::broadcast::Receiver<crate::events::LiveEvent>,
    project_id: String,
}

/// Start an execution: journal `ExecutionStarted` + one `NodeKicked` per kick
/// AND enqueue the `execute` task, all in ONE transaction
/// (`Journal::start_execution`). The reads (tenant, task spec) happen first;
/// a failure anywhere rolls the whole birth back, so a journaled execution
/// with no task row (a "ghost" nothing would ever run or reclaim, which would
/// wedge a later drain) is impossible by construction. Every start path
/// (`run`, trigger setup, infra setup) goes through here.
async fn start_queued_execution(
    state: &DispatcherState,
    color: weft_core::Color,
    project_id: &str,
    phase: weft_core::context::Phase,
    entry_node: &str,
    kicks: &[Kick],
    definition_hash: &str,
    // A manual run's computed subgraph, journaled so the engine skips
    // everything outside it and a resume rebuilds the same boundary.
    // `None` for the setup phases (they compute their scope engine-side)
    // and for anything that runs the whole graph.
    subgraph: Option<&HashSet<String>>,
) -> Result<(), (StatusCode, String)> {
    let now = crate::lease::now_unix() as u64;
    let tenant = state
        .tenant_router
        .tenant_for_project(project_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let task = crate::task_kinds::execute::execution_task_spec(
        &state.pg_pool,
        weft_task_store::TaskKind::Execute,
        project_id,
        color,
        definition_hash,
        Some(tenant.as_str()),
        None,
        None,
    )
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("execute task spec: {e}")))?;
    let (start, kick_events) =
        execution_birth_events(color, project_id, phase, entry_node, kicks, definition_hash, subgraph, now);
    state
        .journal
        .start_execution(&start, &kick_events, task)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("start execution: {e}")))?;
    Ok(())
}

/// The two event shapes that give an execution its identity: the one
/// `ExecutionStarted` and one `NodeKicked` per root. Every start path
/// (manual run, setup phases, entry-trigger fire, live-trigger fire)
/// builds them here, so a field added to either event has one home and
/// the journaled subgraph is always written the same way (sorted, so the
/// row is deterministic). The COMMIT differs per path (one transaction
/// here, dedup-keyed writes in route_entry, an admission transaction for
/// a live fire); the events do not.
pub(crate) fn execution_birth_events(
    color: weft_core::Color,
    project_id: &str,
    phase: weft_core::context::Phase,
    entry_node: &str,
    kicks: &[Kick],
    definition_hash: &str,
    subgraph: Option<&HashSet<String>>,
    at_unix: u64,
) -> (weft_journal::ExecEvent, Vec<weft_journal::ExecEvent>) {
    let start = weft_journal::ExecEvent::ExecutionStarted {
        color,
        project_id: project_id.to_string(),
        entry_node: entry_node.to_string(),
        phase,
        definition_hash: Some(definition_hash.to_string()),
        node_test: false,
        subgraph: subgraph.map(|s| {
            let mut v: Vec<String> = s.iter().cloned().collect();
            v.sort();
            v
        }),
        at_unix,
    };
    let kick_events = kicks
        .iter()
        .map(|kick| weft_journal::ExecEvent::NodeKicked {
            color,
            node_id: kick.node_id.clone(),
            firing: kick.firing,
            payload: kick.payload.clone(),
            port_snapshot: kick.port_snapshot.clone(),
            at_unix,
        })
        .collect();
    (start, kick_events)
}

/// The `requires_infra` nodes whose infra is NOT currently Running. Empty means
/// every infra node is up. The ONE place this pre-flight is computed: `run`,
/// `activate`, and `reactivate` all consult it (a Stopped/Failed/Flaky/missing node
/// is not-running), then each formats its own precondition message, so there is one
/// definition of "is the infra up" and no drift between the entry points.
///
/// `within` narrows the check to the named nodes: a targeted run only touches
/// its own upstream subgraph, so infra outside it has no bearing on that run
/// and must not gate it. `None` checks the whole project (activate does).
async fn missing_infra_nodes(
    state: &DispatcherState,
    project_id: &str,
    project: &ProjectDefinition,
    within: Option<&HashSet<String>>,
) -> Result<Vec<String>, (StatusCode, String)> {
    let mut missing: Vec<String> = Vec::new();
    for node in project
        .nodes
        .iter()
        .filter(|n| n.requires_infra)
        .filter(|n| within.is_none_or(|set| set.contains(&n.id)))
    {
        let row = crate::infra_node::get(&state.pg_pool, project_id, &node.id)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("infra_node: {e}")))?;
        let running = row
            .map(|r| r.status == crate::infra_node::InfraNodeStatus::Running)
            .unwrap_or(false);
        if !running {
            missing.push(node.id.clone());
        }
    }
    Ok(missing)
}

/// START the InfraSetup sub-execution for every `requires_infra` node
/// in the project: journal `ExecutionStarted` + the upstream-closure
/// root kicks (so programmatic-infra patterns, text → compute →
/// infra, flow values into the infra body), then enqueue the execute
/// task. Returns `None` when the project has no infra nodes (nothing
/// to provision). Split from `await_infra_setup` so the caller (sync)
/// can perform the start under the short per-project transition lock
/// (serializing the flip) and do the unbounded wait OUTSIDE it.
pub async fn start_infra_setup(
    state: &DispatcherState,
    project_id_uuid: uuid::Uuid,
) -> Result<Option<InfraSetupRun>, (StatusCode, String)> {
    // One coherent (hash, shape) pair: the kicks below and the
    // journaled/enqueued hash must come from the SAME definition (see
    // `coherent_definition`).
    let (definition_hash, project) = coherent_definition(state, project_id_uuid).await?;
    let kicks = compute_infra_setup_kicks(&project);
    if kicks.is_empty() {
        return Ok(None);
    }
    let project_id = project_id_uuid.to_string();
    let color = uuid::Uuid::new_v4();

    // Subscribe BEFORE journaling+enqueueing so the worker can't beat us to the
    // completion event.
    let events = state.events.subscribe_project(&project_id).await;

    start_queued_execution(
        state,
        color,
        &project_id,
        weft_core::context::Phase::InfraSetup,
        &kicks[0].node_id,
        &kicks,
        &definition_hash,
        None,
    )
    .await?;
    Ok(Some(InfraSetupRun { color, events, project_id }))
}

/// Wait for a started InfraSetup sub-execution to settle.
pub async fn await_infra_setup(
    state: &DispatcherState,
    run: InfraSetupRun,
) -> Result<(), (StatusCode, String)> {
    let InfraSetupRun { color, mut events, project_id } = run;
    // Wait for completion. NO deadline: the InfraSetup execution runs
    // user-authored upstream nodes (text -> compute -> infra), and
    // user code may legitimately be slow; a hard cap would refuse
    // legitimate provisioning. Instead, a periodic breadcrumb keeps
    // the stuck-state legible in the dispatcher logs, and the user
    // can always cancel the execution (`weft stop`) to unblock.
    let started = std::time::Instant::now();
    let mut breadcrumb = tokio::time::interval(std::time::Duration::from_secs(30));
    breadcrumb.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    breadcrumb.tick().await; // the first tick fires immediately; skip it
    loop {
        tokio::select! {
            _ = breadcrumb.tick() => {
                tracing::info!(
                    target: "weft_dispatcher::infra_setup",
                    project_id = %project_id,
                    color = %color,
                    elapsed_secs = started.elapsed().as_secs(),
                    "infra setup still running; waiting on the InfraSetup execution \
                     (cancel it with `weft stop` to unblock)"
                );
            }
            res = events.recv() => {
                match res.map(|record| record.event) {
                    Ok(crate::events::DispatcherEvent::ExecutionCompleted { color: c, .. })
                        if c == color => return Ok(()),
                    Ok(crate::events::DispatcherEvent::ExecutionFailed { color: c, error, .. })
                        if c == color => {
                        return Err((
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("infra setup failed: {error}"),
                        ));
                    }
                    Ok(crate::events::DispatcherEvent::ExecutionCancelled { color: c, reason, .. })
                        if c == color => {
                        // The user's infra-cancel (or a per-color stop)
                        // interrupted the provisioning execution. 409,
                        // not 500: the state is exactly what the user
                        // asked for; per-node partial state stays
                        // visible for per-node terminate/retry.
                        return Err((
                            StatusCode::CONFLICT,
                            format!("infra setup cancelled: {reason}"),
                        ));
                    }
                    Ok(_) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        // The bus dropped a batch that may have held this
                        // color's terminal event. The journal is
                        // authoritative: re-query it rather than wait
                        // blind (which would spuriously time out even
                        // though infra setup already finished).
                        match crate::api::execution::terminal_outcome(&state.pg_pool, color).await {
                            Ok(Some(crate::api::execution::TerminalOutcome::Completed)) => {
                                return Ok(())
                            }
                            Ok(Some(crate::api::execution::TerminalOutcome::Cancelled)) => {
                                return Err((
                                    StatusCode::CONFLICT,
                                    "infra setup cancelled".into(),
                                ))
                            }
                            Ok(Some(_)) => {
                                return Err((
                                    StatusCode::INTERNAL_SERVER_ERROR,
                                    "infra setup failed".into(),
                                ))
                            }
                            Ok(None) => {} // still in flight; keep waiting
                            Err(e) => {
                                return Err((
                                    StatusCode::INTERNAL_SERVER_ERROR,
                                    format!("infra setup terminal lookup: {e}"),
                                ))
                            }
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        return Err((
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "event bus closed during infra setup".into(),
                        ));
                    }
                }
            }
        }
    }
}

/// What one trigger fire runs: the roots to kick and the node set they
/// were computed from, the fire's PROGRAM. The set is journaled on
/// `ExecutionStarted` as the execution's subgraph, so the engine holds
/// the run to it and absorbs, silently, a pulse into anything outside.
/// Both come out of one `RunSubgraph`, so "what runs" and "what gets
/// kicked" cannot disagree.
///
/// Why the boundary matters for a fire: emission is scope-blind, so a
/// node shared by two programs in one file (a database, a provider)
/// pushes a pulse into the OTHER program's consumers too. Unbounded,
/// those consumers hold a partial input set forever and the run ends
/// Stuck after all its real work completed. Bounded, they never appear.
pub struct TriggerFire {
    pub kicks: Vec<Kick>,
    pub subgraph: HashSet<String>,
}

/// Kicks for a trigger fire.
///
/// Rule: from the FIRING trigger, walk downstream: everything it
/// reaches is the fire's. Then walk back up from all of that for what
/// it needs, treating every trigger node as a terminator. Triggers
/// themselves are included as kicks: the firing trigger carries the
/// payload and its setup-time port snapshot; any other trigger in the
/// subgraph is kicked payload-less, which the engine turns into "close
/// all its
/// output ports" (the skip cascade prunes its exclusive branches).
///
/// Why terminators: at fire time a trigger's outputs are the payload,
/// not a function of its inputs (its ports replay the setup-time
/// snapshot). Nodes that exist only to produce inputs for triggers
/// must not re-run every time the trigger fires. If a node also feeds
/// non-trigger paths that reach a targeted output, it re-runs via
/// those paths.
///
/// Why start from the fired trigger: an output with no path from it
/// (a sibling branch fed by another trigger or by static sources
/// alone) is someone else's work; this fire must not re-run it.
///
/// Returns `None` if the fired trigger reaches no output; the caller
/// treats that as "nothing to run."
pub fn compute_trigger_fire(
    project: &ProjectDefinition,
    firing_node_id: &str,
    payload: &Value,
    port_snapshot: Option<&Value>,
) -> Option<TriggerFire> {
    // All trigger nodes register signals during TriggerSetup; that set
    // is what fires route to.
    let (edge_idx, triggers) = RunSubgraph::graph_base(project);
    if !triggers.contains(firing_node_id) {
        return None;
    }

    // Targets = everything the FIRED trigger reaches downstream (the
    // trigger itself included, so a trigger with nothing behind it
    // still fires and runs alone).
    let reachable = downstream_closure(project, &edge_idx, firing_node_id);
    let targets: Vec<String> = project
        .nodes
        .iter()
        .filter(|n| reachable.contains(&n.id))
        .map(|n| n.id.clone())
        .collect();

    // Upstream closure from all of that, stopping at triggers (include
    // the trigger but do not walk through its incoming edges). Only the
    // firing trigger's kick carries the wake payload and the snapshot.
    let subgraph = RunSubgraph::aimed(project, edge_idx, triggers, &targets);
    let kicks = subgraph.root_kicks(project, payload, Some((firing_node_id, port_snapshot)));
    Some(TriggerFire { kicks, subgraph: subgraph.nodes })
}

#[derive(Debug, Serialize)]
pub struct ActivateResponse {
    pub urls: Vec<ActivationUrl>,
}

#[derive(Debug, Serialize)]
pub struct ActivationUrl {
    pub node_id: String,
    pub url: String,
}

/// Activate a project. Preconditions: every `requires_infra: true`
/// node has been provisioned via `weft infra up`. Steps:
/// 1. Tear down any previous listener + tracked signals.
/// 2. Spawn a fresh listener.
/// 3. Run the TriggerSetup sub-execution. Trigger nodes register
///    themselves via `ctx.register_signal`; the worker calls the
///    dispatcher's register endpoint, which enqueues a
///    `register_signal` task that POSTs the listener and writes the
///    `signal` row.
/// 4. Mark project Active, publish TriggerUrlChanged events, return
///    the listener-minted URLs.
#[derive(Debug, Serialize)]
pub struct ProjectStatusResponse {
    pub id: String,
    pub name: String,
    /// Raw status enum: "registered" | "activating" | "active" |
    /// "deactivating" | "inactive". Mirrors `project.status`.
    /// SYNC: ProjectStatus <-> crates/weft-broker-client/src/protocol.rs ProjectStatus, packages/weft-graph/src/protocol.ts projectStatus
    pub status: String,
    /// The build transition axis, orthogonal to `status`: "none" |
    /// "building" | "cancelling_build". While not "none", the only
    /// offered action is cancel_build.
    /// SYNC: ProjectTransition <-> crates/weft-dispatcher/src/project_store.rs ProjectTransition, packages/weft-graph/src/protocol.ts ProjectTransition, packages/weft-graph/src/status.ts VALID_TRANSITIONS
    pub transition: String,
    /// User-facing mode label derived from the lifecycle axes:
    /// "registered" | "active" | "deactivating" | "wipe" |
    /// "hibernate" | "park". The action bar reads this verbatim.
    /// The accepting/visible booleans the gate keys on are NOT
    /// exposed: the user-facing mode label is the only thing
    /// clients need; the booleans are an internal projection.
    pub mode: String,
    /// Unix-second deadline after which `accepting_fires=true`
    /// flips to refusal (hibernate's grace window). `None` outside
    /// hibernate. Surfaced so the action bar can render a countdown.
    pub fires_deadline_unix: Option<i64>,
    /// Count of running, non-suspended executions right now.
    /// Drives the deactivating-state UI: progress towards drain.
    pub running_count: usize,
    pub listener_running: bool,
    /// True when the project has any trigger node (`features.is_trigger`).
    /// Clients show the listening/activation status indicator ONLY when this
    /// is true: a project with no trigger has nothing to listen with, so no
    /// indicator (not even an off one). `listener_running` then says whether
    /// it is currently listening (live) vs registered-but-not-listening (off).
    pub has_triggers: bool,
    pub infra: Vec<ProjectInfraEntry>,
    pub executions: ProjectExecutionsSummary,
    /// True when project has any infra-typed nodes in its source.
    /// Used by clients to decide whether to even show the
    /// Start/Stop/Upgrade infra controls.
    pub has_infra: bool,
    /// True when live `infra_node` rows exist whose node is NOT in the
    /// current source (the user deleted the node while it was
    /// deployed). Never gates run/activate (the no-infra graph runs in
    /// the shared pool, unlinked); clients OR it into their
    /// infra-slot-visibility check so the controls for live infra
    /// never vanish (the never-lose-track guarantee).
    pub orphaned_infra: bool,
    /// Aggregate infra state across the project's infra nodes, one of nine values:
    /// "none" (no infra nodes defined), "running" (all up), "provisioning",
    /// "stopping", "terminating" (transitional), "stopped" (all down), "partial"
    /// (mixed), "flaky", "failed". Clients map these to a status glyph.
    /// SYNC: infra_rollup values <-> packages/weft-graph/src/status.ts (infra_rollup consumer)
    pub infra_rollup: String,
    /// Desired vs running source/infra-hash drift. Either bit is
    /// only meaningful when the caller passed the corresponding
    /// `desired_*_hash` query param.
    pub drift: ProjectDrift,
    /// Verbs the dispatcher will currently accept. Driven by the
    /// state machine: project status, infra state, drift bits, etc.
    /// Clients render the action bar from this list directly; no
    /// client-side state machine.
    pub available_actions: Vec<String>,
    /// Counts of preserved state, for the reactivate-time prompt.
    pub preservation: PreservationCounts,
}

#[derive(Debug, Default, Serialize)]
pub struct PreservationCounts {
    /// Total fires queued across every signal in the project. Sum of
    /// `jsonb_array_length(parked_fires)`. Entry triggers append one
    /// element per fire; resume signals at most one. Drives the
    /// "execute parked / drop parked / wipe" choice on reactivate.
    pub parked: usize,
    /// Resume signals whose `parked_fires` queue is empty: registered
    /// but no submission yet. Stay across the inactive window so the
    /// corresponding suspended execution can resume later. Entries
    /// have no equivalent state; an entry with an empty queue is just
    /// "registered, idle" and isn't preserved per se.
    pub suspended: usize,
}

#[derive(Debug, Default, Serialize)]
pub struct ProjectDrift {
    /// "Infra is stale relative to source." Drives the Upgrade
    /// button. Computed from desired_infra_hash != running_infra_hash.
    pub infra_drift: bool,
    /// "Worker BINARY needs rebuilding." Computed from
    /// desired_binary_hash != running_binary_hash. Flips on engine /
    /// node-impl / type-set / weft.toml edits; the dialog before
    /// running asks the user about killing running executions when
    /// this drift is non-zero.
    pub binary_drift: bool,
    /// "Project SHAPE has changed." Computed from
    /// desired_definition_hash != running_definition_hash. A pure
    /// config / topology edit flips this without flipping
    /// `binary_drift`; the next execution picks up the new
    /// definition via the worker's broker fetch.
    pub definition_drift: bool,
}

#[derive(Debug, Serialize)]
pub struct ProjectInfraEntry {
    pub node_id: String,
    /// Infra node type (e.g. "whatsapp_bridge"). Sourced from the
    /// project definition so the extension can decorate the node
    /// without re-parsing the source.
    pub node_type: String,
    pub status: String,
    pub endpoint_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "failureStage")]
    pub failure_stage: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "failureMessage")]
    pub failure_message: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ProjectExecutionsSummary {
    pub total: usize,
    pub last_completed_at: Option<u64>,
    pub last_color: Option<String>,
    pub last_status: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
pub struct StatusQuery {
    /// Binary hash the CLI computed for the current build inputs.
    /// Compared against `project.running_binary_hash` to surface
    /// "the worker image needs rebuilding" drift.
    #[serde(default, rename = "desiredBinaryHash")]
    pub desired_binary_hash: Option<String>,
    /// Definition hash the CLI computed for the current canonical
    /// `ProjectDefinition`. Compared against
    /// `project.running_definition_hash` to surface "the project
    /// shape has changed" drift (a config / topology edit that
    /// hasn't been resynced into the running project).
    #[serde(default, rename = "desiredDefinitionHash")]
    pub desired_definition_hash: Option<String>,
    /// Infra hash the CLI computed for the current infra closure.
    /// Compared against `project.running_infra_hash` for the upgrade
    /// drift signal.
    #[serde(default, rename = "desiredInfraHash")]
    pub desired_infra_hash: Option<String>,
}

/// Aggregate view for `weft status`. Returns registration,
/// listener state, per-node infra state, a rollup of recent
/// executions, drift signals (when desired hashes are passed in
/// query params), and the list of currently-valid action verbs.
/// One response, no stitching required by the CLI.
/// Error envelope for project-scoped handlers whose 404 must be
/// machine-readable (`status`, `remove`). Most arms are a plain
/// `(StatusCode, String)` (via `From`, so existing `.map_err`
/// closures are unchanged); the one special case is `NotMyProject`,
/// which renders a 404 carrying the `x-weft-not-found: project`
/// marker header, so a client can tell "no project I may see under
/// this id" apart from a bare routing 404 (e.g. a version-skewed
/// dispatcher missing the route), which must bubble as an error.
/// `remove` needs it so the CLI's `delete_idempotent` can treat
/// "already gone" as rm's desired end state on a retry.
///
/// `NotMyProject` covers BOTH "no such project" AND "exists but owned by
/// another tenant", with the SAME response. Collapsing them is the
/// no-existence-leak property: an authenticated caller probing another
/// tenant's project id cannot distinguish "exists but not yours" from
/// "does not exist" (both get 404 + the marker).
pub enum StatusError {
    /// No project the caller may run under this id: either no row, or a row
    /// owned by another tenant. Marker header set; the two are
    /// indistinguishable on the wire (no existence leak).
    NotMyProject,
    /// Any other failure: status + body, no marker.
    Other(StatusCode, String),
}

impl From<(StatusCode, String)> for StatusError {
    fn from((code, msg): (StatusCode, String)) -> Self {
        StatusError::Other(code, msg)
    }
}

impl IntoResponse for StatusError {
    fn into_response(self) -> Response {
        match self {
            StatusError::NotMyProject => (
                StatusCode::NOT_FOUND,
                [("x-weft-not-found", "project")],
                "project not found",
            )
                .into_response(),
            StatusError::Other(code, msg) => (code, msg).into_response(),
        }
    }
}

/// Ownership gate for marked handlers: `authorize_project` (the ONE
/// ownership lookup, existence-leak collapse included) with its 404
/// upgraded to `NotMyProject` so the response carries the
/// `x-weft-not-found: project` marker instead of a headerless 404 the
/// client cannot tell apart from a version-skew routing 404.
async fn authorize_project_marked(
    state: &DispatcherState,
    caller: &crate::tenant::TenantId,
    id: uuid::Uuid,
) -> Result<(), StatusError> {
    authorize_project(state, caller, id).await.map_err(|(status, msg)| {
        if status == StatusCode::NOT_FOUND {
            StatusError::NotMyProject
        } else {
            (status, msg).into()
        }
    })
}

pub async fn status(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id_str): Path<String>,
    axum::extract::Query(query): axum::extract::Query<StatusQuery>,
) -> Result<Json<ProjectStatusResponse>, StatusError> {
    let id = id_str
        .parse::<uuid::Uuid>()
        .map_err(|_| (StatusCode::BAD_REQUEST, "bad id".to_string()))?;
    // The `x-weft-not-found: project` marker lets a client tell "no
    // project I may see under this id" apart from a version-skew
    // routing 404 it must bubble. We must NOT call
    // `authorize_project` first: it returns a headerless 404, which would
    // make the marker unreachable and a brand-new `weft run` could never
    // pass the gate. So resolve ownership through the marked gate.
    authorize_project_marked(&state, &caller.0, id).await?;
    let summary = state
        .projects
        .get(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("get project: {e}")))?
        .ok_or(StatusError::NotMyProject)?;
    let project = state
        .projects
        .project(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("project: {e}")))?
        // A registered project missing its definition is a real
        // inconsistency, NOT "no such project": Other (no marker), so
        // the CLI bubbles it loudly instead of skipping the gate.
        .ok_or((StatusCode::NOT_FOUND, "project definition missing".to_string()))?;
    let project_id = id.to_string();
    let listener_running = state
        .listeners
        .project_has_live_listener(&project_id, &state.pg_pool)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("listener status: {e}")))?;

    let snapshot = gather_action_snapshot(&state, id, &project).await?;
    // Per-node list for the UI: SOURCE nodes only. A live row whose
    // node isn't in the current source (an orphan) is deliberately not
    // rendered per-node (the graph shows only source nodes); it is
    // surfaced project-level via `orphaned_infra` + the rollup, which
    // count it so the infra controls never vanish while live infra
    // exists.
    let mut infra = Vec::new();
    for row in &snapshot.infra_rows {
        let Some(node_type) = project
            .nodes
            .iter()
            .find(|n| n.id == row.node_id)
            .map(|n| n.node_type.clone())
        else {
            continue;
        };
        infra.push(ProjectInfraEntry {
            node_id: row.node_id.clone(),
            node_type,
            status: row.status.as_str().to_string(),
            // Coarse UI hint: first endpoint by name (BTreeMap = stable).
            endpoint_url: row.endpoints.values().next().cloned(),
            failure_stage: row.failure_stage.map(|f| f.as_str().to_string()),
            failure_message: row.failure_message.clone(),
        });
    }
    let has_infra = snapshot.has_infra;
    let has_triggers = snapshot.has_triggers;
    let infra_rollup = snapshot.infra_rollup.clone();

    // Project-filtered, newest first: `total` is the true count of the project's
    // executions (from SQL, not capped at a fetched window) and the first row is
    // the latest for the `last_*` fields.
    let execs = state
        .journal
        .list_executions(
            caller.0.as_str(),
            &crate::journal::ExecutionQuery {
                limit: 1,
                offset: 0,
                project_id: Some(project_id.clone()),
                started_after: None,
                started_before: None,
                phase: None,
            },
        )
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("journal: {e}")))?;
    let last = execs.executions.first();
    let executions = ProjectExecutionsSummary {
        total: execs.total as usize,
        last_completed_at: last.and_then(|l| l.completed_at),
        last_color: last.map(|l| l.color.to_string()),
        last_status: last.map(|l| l.status.clone()),
    };

    let binary_hash = state
        .projects
        .running_binary_hash(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("running_binary_hash: {e}")))?;
    let definition_hash = state
        .projects
        .running_definition_hash(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("running_definition_hash: {e}")))?;
    let infra_hash = state
        .projects
        .running_infra_hash(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("running_infra_hash: {e}")))?;
    let drift = compute_drift(
        &query,
        binary_hash.as_deref(),
        definition_hash.as_deref(),
        infra_hash.as_deref(),
    );
    let available_actions = compute_available_actions(&ActionInputs {
        lifecycle: &snapshot.lifecycle,
        transition: snapshot.transition,
        has_triggers,
        has_infra,
        orphaned_infra: snapshot.orphaned_infra,
        infra_rollup: &infra_rollup,
        infra_busy: snapshot.infra_busy,
        drift: &drift,
        preservation: &snapshot.preservation,
        running_count: snapshot.running_count,
    });

    Ok(Json(ProjectStatusResponse {
        id: project_id,
        name: summary.name,
        status: snapshot.lifecycle.status.as_str().to_string(),
        transition: snapshot.transition.as_str().to_string(),
        mode: snapshot.lifecycle.mode_label().to_string(),
        fires_deadline_unix: snapshot.lifecycle.fires_deadline_unix,
        running_count: snapshot.running_count,
        listener_running,
        has_triggers,
        infra,
        executions,
        has_infra,
        orphaned_infra: snapshot.orphaned_infra,
        infra_rollup,
        drift: ProjectDrift {
            infra_drift: drift.infra_drift,
            binary_drift: drift.binary_drift,
            definition_drift: drift.definition_drift,
        },
        available_actions,
        preservation: snapshot.preservation,
    }))
}

/// Every reconciliation input gathered from live state, shared by the
/// status handler (renders the list) and `require_action` (enforces
/// against the same list). One producer so the two can never disagree.
pub(crate) struct ActionSnapshot {
    pub lifecycle: crate::project_store::ProjectLifecycle,
    pub transition: crate::project_store::ProjectTransition,
    pub has_triggers: bool,
    pub has_infra: bool,
    pub orphaned_infra: bool,
    pub infra_rollup: String,
    pub infra_busy: bool,
    pub infra_rows: Vec<crate::infra_node::InfraNodeRow>,
    pub preservation: PreservationCounts,
    pub running_count: usize,
}

pub(crate) async fn gather_action_snapshot(
    state: &DispatcherState,
    id: uuid::Uuid,
    project: &ProjectDefinition,
) -> Result<ActionSnapshot, (StatusCode, String)> {
    let project_id = id.to_string();
    let lifecycle = state
        .projects
        .lifecycle(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("lifecycle: {e}")))?
        .ok_or((StatusCode::NOT_FOUND, "project not found".into()))?;
    let transition = state
        .projects
        .transition(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("transition: {e}")))?
        .ok_or((StatusCode::NOT_FOUND, "project not found".into()))?;
    let infra_rows = crate::infra_node::list_for_project(&state.pg_pool, &project_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("infra_node: {e}")))?;

    let source_infra: HashSet<&str> = project
        .nodes
        .iter()
        .filter(|n| n.requires_infra)
        .map(|n| n.id.as_str())
        .collect();
    let has_infra = !source_infra.is_empty();
    let has_triggers = project.nodes.iter().any(|n| n.features.is_trigger);
    // Orphans: live rows whose node the user deleted from source.
    // They count into the rollup (below) and raise the project-level
    // signal, so a FULLY-orphaned live infra set never collapses the
    // rollup to `none` and never makes the infra controls vanish
    // (Model 1's never-lose-track guarantee).
    let orphan_count = infra_rows
        .iter()
        .filter(|r| !source_infra.contains(r.node_id.as_str()))
        .count();
    let orphaned_infra = orphan_count > 0;

    // Aggregate infra state across SOURCE nodes + orphan rows:
    //   - none:    no source infra node and no live row.
    //   - running: every counted slot is Running.
    //   - stopped: every counted slot is Stopped or absent.
    //   - partial: mixed (some running, some not, no failures).
    //   - failed:  at least one Failed.
    //   - flaky:   at least one Flaky and the rest Running.
    // Transient states take precedence (terminating > stopping >
    // provisioning) so the action bar shows the in-flight state and
    // offers only its cancel.
    let total = source_infra.len() + orphan_count;
    let infra_rollup = if total == 0 {
        "none".to_string()
    } else {
        use crate::infra_node::InfraNodeStatus;
        let mut running = 0usize;
        let mut stopped = 0usize;
        let mut absent = 0usize; // source node never provisioned OR terminated
        let mut failed = 0usize;
        let mut flaky = 0usize;
        let mut stopping = 0usize;
        let mut terminating = 0usize;
        let mut provisioning = 0usize;
        // One slot per source node (absent counted) + one per orphan
        // row (an orphan always HAS a row, by definition).
        let mut count_status = |status: InfraNodeStatus| match status {
            InfraNodeStatus::Running => running += 1,
            InfraNodeStatus::Failed => failed += 1,
            InfraNodeStatus::Flaky => flaky += 1,
            InfraNodeStatus::Stopping => stopping += 1,
            InfraNodeStatus::Terminating => terminating += 1,
            InfraNodeStatus::Provisioning => provisioning += 1,
            InfraNodeStatus::Stopped => stopped += 1,
        };
        for n in project.nodes.iter().filter(|n| n.requires_infra) {
            match infra_rows.iter().find(|r| r.node_id == n.id) {
                Some(r) => count_status(r.status),
                // No row: never provisioned OR terminated (terminate
                // removes the row). Nothing lives in the namespace for
                // this node.
                None => absent += 1,
            }
        }
        for r in infra_rows
            .iter()
            .filter(|r| !source_infra.contains(r.node_id.as_str()))
        {
            count_status(r.status);
        }
        if terminating > 0 {
            "terminating".to_string()
        } else if stopping > 0 {
            "stopping".to_string()
        } else if provisioning > 0 {
            "provisioning".to_string()
        } else if failed > 0 {
            "failed".to_string()
        } else if flaky > 0 && running + flaky == total {
            "flaky".to_string()
        } else if running == total {
            "running".to_string()
        } else if absent == total {
            "none".to_string()
        } else if stopped + absent == total {
            // Some stopped + some never-provisioned. Pragmatic bucket
            // as `stopped`: the user can re-start, and Terminate still
            // makes sense for the actually-stopped subset.
            "stopped".to_string()
        } else {
            "partial".to_string()
        }
    };

    // Infra-op-in-flight fact the rollup can't always see (a claimed
    // stop mid-drain; a provisioning execution before any row flips).
    let infra_busy = crate::infra_lifecycle_command::any_in_flight(&state.pg_pool, &project_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("commands in flight: {e}")))?
        || infra_setup_in_flight(state, &project_id)
            .await
            .map_err(|e| {
                (StatusCode::INTERNAL_SERVER_ERROR, format!("infra_setup_in_flight: {e}"))
            })?;

    let preservation = preservation_counts(state, &project_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("preservation_counts: {e}")))?;
    let running_now = running_count(state, &project_id, None)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("running_count: {e}")))?;
    Ok(ActionSnapshot {
        lifecycle,
        transition,
        has_triggers,
        has_infra,
        orphaned_infra,
        infra_rollup,
        infra_busy,
        infra_rows,
        preservation,
        running_count: running_now,
    })
}

/// Enforce a verb against the SAME reconciliation the status handler
/// renders: the greyed-out button prevents the common case, this
/// rejection is the race safety net (a stale tab whose bar hasn't
/// refreshed). `verbs` is the acceptable set for the handler (some
/// verbs are aliases of one endpoint, e.g. activate serves activate /
/// reactivate / resume_active). Drift bits are client-side facts, so
/// enforcement treats them as set (drift-gated verbs are never
/// spuriously rejected here; their own handlers validate further).
pub(crate) async fn require_action(
    state: &DispatcherState,
    id: uuid::Uuid,
    verbs: &[&str],
) -> Result<(), (StatusCode, String)> {
    let project = state
        .projects
        .project(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("project: {e}")))?
        .ok_or((StatusCode::NOT_FOUND, "project not found".into()))?;
    let snapshot = gather_action_snapshot(state, id, &project).await?;
    let allowed = compute_available_actions(&ActionInputs {
        lifecycle: &snapshot.lifecycle,
        transition: snapshot.transition,
        // Client-side fact, treated as satisfied (same posture as the
        // drift bits below): when the build happens at verb time the user
        // clicks activate on the SAVED source's triggers before a build
        // has re-registered them, so the REGISTERED definition's
        // has_triggers may lag. `activate_inner` re-checks honestly
        // against the built definition and 412s a genuinely
        // trigger-less project after its build.
        has_triggers: true,
        has_infra: snapshot.has_infra,
        orphaned_infra: snapshot.orphaned_infra,
        infra_rollup: &snapshot.infra_rollup,
        infra_busy: snapshot.infra_busy,
        drift: &DriftBits { infra_drift: true, binary_drift: true, definition_drift: true },
        preservation: &snapshot.preservation,
        running_count: snapshot.running_count,
    });
    if verbs.iter().any(|v| allowed.iter().any(|a| a == v)) {
        return Ok(());
    }
    Err((
        StatusCode::CONFLICT,
        format!(
            "'{}' is not available right now: project is {} (transition {}, infra {}); \
             allowed actions: [{}]",
            verbs[0],
            snapshot.lifecycle.status.as_str(),
            snapshot.transition.as_str(),
            snapshot.infra_rollup,
            allowed.join(", "),
        ),
    ))
}

/// Count parked vs purely-suspended resume signals for a project.
/// Drives the reactivate-time prompt: caller decides whether to
/// ask the user "execute parked / keep suspended only / wipe all"
/// based on whether either count is non-zero.
async fn preservation_counts(
    state: &DispatcherState,
    project_id: &str,
) -> anyhow::Result<PreservationCounts> {
    let (parked, suspended): (i64, i64) = sqlx::query_as(
        "SELECT \
            COALESCE(SUM(jsonb_array_length(parked_fires)), 0)::bigint AS parked, \
            COUNT(*) FILTER (WHERE is_resume = TRUE \
                              AND jsonb_array_length(parked_fires) = 0) AS suspended \
         FROM signal WHERE project_id = $1",
    )
    .bind(project_id)
    .fetch_one(&state.pg_pool)
    .await?;
    Ok(PreservationCounts {
        parked: parked as usize,
        suspended: suspended as usize,
    })
}

/// Pure drift comparison: desired (CLI's view) vs running (DB view).
/// Both sides compute the SAME hash function for each signal, so the
/// comparison is a string equality check.
fn compute_drift(
    query: &StatusQuery,
    running_binary_hash: Option<&str>,
    running_definition_hash: Option<&str>,
    running_infra_hash: Option<&str>,
) -> DriftBits {
    // Each drift bit is only meaningful when both sides have a
    // hash. No running hash means the project was never built /
    // activated; the action bar shouldn't surface drift then.
    let binary_drift = match (query.desired_binary_hash.as_deref(), running_binary_hash) {
        (Some(want), Some(have)) => want != have,
        _ => false,
    };
    let definition_drift = match (
        query.desired_definition_hash.as_deref(),
        running_definition_hash,
    ) {
        (Some(want), Some(have)) => want != have,
        _ => false,
    };
    let infra_drift = match (query.desired_infra_hash.as_deref(), running_infra_hash) {
        (Some(want), Some(have)) => want != have,
        _ => false,
    };
    DriftBits {
        infra_drift,
        binary_drift,
        definition_drift,
    }
}

#[derive(Default, Clone, Copy)]
pub(crate) struct DriftBits {
    pub infra_drift: bool,
    pub binary_drift: bool,
    pub definition_drift: bool,
}

/// Every input the reconciliation reads, gathered in one place so the
/// status handler (UI list) and verb enforcement (`require_action`)
/// feed the SAME pure function from the SAME facts. `drift` is the one
/// input only a client can supply (the desired hashes live on the
/// client); enforcement passes all-true so drift-gated verbs are never
/// spuriously rejected by the dispatcher.
pub(crate) struct ActionInputs<'a> {
    pub lifecycle: &'a crate::project_store::ProjectLifecycle,
    pub transition: crate::project_store::ProjectTransition,
    /// Source declares any trigger node (frontend-parse fact, derived
    /// here from the stored definition).
    pub has_triggers: bool,
    /// Source declares any infra node. This is the SOURCE fact only;
    /// orphaned live infra does not count (Model 1).
    pub has_infra: bool,
    /// Live `infra_node` rows exist whose node is NOT in the current
    /// source (the user deleted the node while it was deployed). Does
    /// NOT gate run/activate; DOES keep the infra controls offered so
    /// the user never loses track of live infra.
    pub orphaned_infra: bool,
    pub infra_rollup: &'a str,
    /// An infra operation is in flight: an uncompleted lifecycle
    /// command (apply / stop / terminate) or a running InfraSetup
    /// provisioning execution. Transitional per the master rule even
    /// BEFORE the supervisor flips any node status (the window where
    /// the rollup alone would still read as stable), so a drain in
    /// progress is never starved by new runs.
    pub infra_busy: bool,
    pub drift: &'a DriftBits,
    pub preservation: &'a PreservationCounts,
    pub running_count: usize,
}

/// THE reconciliation table (`docs/project-lifecycle-state-model.md`
/// §8) as a pure function: verbs the dispatcher will currently accept,
/// computed from the concern-lifecycles + source facts, never stored.
/// Clients render the action bar from this list directly AND the verb
/// handlers enforce against the same list, so the button state and the
/// enforcement cannot disagree.
///
/// The master rule collapses the table: in ANY transitional state
/// (building / cancelling_build / activating / deactivating / infra
/// provisioning / stopping / terminating) the only offered action is
/// the matching CANCEL. Stable states then enumerate:
///
///   - run           : allowed iff the source's infra (if any) is
///                     running. Never gated on a build (run builds as
///                     step 0) and never gated on an orphan (a
///                     no-infra graph runs in the shared pool,
///                     unlinked). Allowed while Active (manual run
///                     alongside live triggers; the bar may not show
///                     a button, the backend permits it).
///   - activate /
///     reactivate    : Registered/Inactive, source has triggers,
///                     infra (if any) running. Reactivate variant
///                     when preserved state exists.
///   - deactivate    : Active. NOT gated on has_triggers: deleting
///                     the last trigger from source while active must
///                     keep Deactivate offered (trigger divergence).
///   - resync        : Active + definition drift. Deactivate-then-
///                     reactivate under the hood; re-picks placement
///                     from the CURRENT source. Never touches
///                     orphaned infra.
///   - cancel_*      : the transitional rows.
///   - infra_*       : offered whenever live-or-declared infra exists
///                     (`has_infra || orphaned_infra`), per rollup.
///                     start/upgrade additionally need the SOURCE to
///                     declare infra (there is no spec to provision an
///                     orphan from); stop/terminate work on the live
///                     rows themselves, so they stay offered for a
///                     pure orphan (the never-lose-track guarantee).
fn compute_available_actions(inputs: &ActionInputs<'_>) -> Vec<String> {
    use crate::project_store::ProjectStatus;

    // Master rule, build axis: a build in flight offers only its
    // cancel (idempotent while already cancelling).
    if inputs.transition.is_building() {
        return vec!["cancel_build".to_string()];
    }

    // Master rule, trigger axis.
    match inputs.lifecycle.status {
        ProjectStatus::Activating => {
            return vec!["cancel_activate".to_string()];
        }
        ProjectStatus::Deactivating => {
            // Mid-deactivate: give up on the wait (cancel_running,
            // the drain finishes immediately) or change your mind
            // (resume_active rolls forward into Active).
            let mut out = Vec::new();
            if inputs.running_count > 0 {
                out.push("cancel_running".to_string());
            }
            out.push("resume_active".to_string());
            return out;
        }
        ProjectStatus::Registered | ProjectStatus::Inactive | ProjectStatus::Active => {}
    }

    // Master rule, infra axis: either the derived rollup reports a
    // transitional state, or an infra operation is in flight that the
    // rollup can't see yet (a claimed stop still draining, a
    // provisioning execution before any node row flips). Only
    // meaningful when infra state exists at all.
    let infra_live = inputs.has_infra || inputs.orphaned_infra;
    if infra_live
        && (inputs.infra_busy
            || matches!(inputs.infra_rollup, "provisioning" | "stopping" | "terminating"))
    {
        return vec!["infra_cancel".to_string()];
    }

    // Stable states.
    let mut out = Vec::new();
    let infra_ready = !inputs.has_infra || inputs.infra_rollup == "running";

    // `run` is offered whenever it is a legal NEXT STEP, NOT gated on
    // whether a worker image is already built (a click auto-builds on
    // demand). The one gate is genuine live-state: any infra the
    // SOURCE declares must be running (a run would fail fetching infra
    // outputs; the user starts infra first, its own verb).
    if infra_ready {
        out.push("run".to_string());
    }

    match inputs.lifecycle.status {
        ProjectStatus::Active => {
            out.push("deactivate".to_string());
            // Resync if the project's RUNTIME shape drifted (config /
            // topology edit). Binary drift alone does NOT light
            // resync: a binary-only change rebuilds the image but
            // doesn't need the user to re-register triggers; the next
            // run picks up the new image.
            if inputs.drift.definition_drift {
                out.push("resync".to_string());
            }
        }
        ProjectStatus::Registered | ProjectStatus::Inactive => {
            if inputs.has_triggers && infra_ready {
                let has_preserved =
                    inputs.preservation.parked + inputs.preservation.suspended > 0;
                if has_preserved && inputs.lifecycle.status == ProjectStatus::Inactive {
                    out.push("reactivate".to_string());
                } else {
                    out.push("activate".to_string());
                }
            }
        }
        ProjectStatus::Activating | ProjectStatus::Deactivating => {
            unreachable!("transitional statuses returned above")
        }
    }

    // Infra controls, per rollup. `partial` (some units up, some down)
    // is a valid steady state, not a transient: Start brings the down
    // units up, Stop takes the up units down, Terminate kills
    // everything; all three are meaningful at once.
    if infra_live {
        let can_provision = inputs.has_infra; // needs a source spec
        match inputs.infra_rollup {
            "running" => {
                out.push("infra_stop".to_string());
                out.push("infra_terminate".to_string());
                if can_provision && inputs.drift.infra_drift {
                    out.push("infra_upgrade".to_string());
                }
            }
            "stopped" => {
                if can_provision {
                    out.push("infra_start".to_string());
                }
                out.push("infra_terminate".to_string());
            }
            "none" => {
                if can_provision {
                    out.push("infra_start".to_string());
                }
            }
            "partial" | "failed" | "flaky" => {
                if can_provision {
                    out.push("infra_start".to_string());
                }
                out.push("infra_stop".to_string());
                out.push("infra_terminate".to_string());
                if can_provision && inputs.drift.infra_drift {
                    out.push("infra_upgrade".to_string());
                }
            }
            // provisioning/stopping/terminating returned above.
            _ => {}
        }
    }

    out
}

/// Body for `POST /projects/{id}/activate`. Optional `binaryHash`
/// refreshes the running image-tag for the next worker spawn.
///
/// `reactivateChoice` matters only when there's preserved state
/// from a prior deactivate (status=Inactive AND any signal row
/// exists for the project). Three choices, applied as 1-line
/// pre-flight against the existing rows; the rest of the activate
/// path is identical regardless of choice.
///
///   - `execute_parked_keep_suspended` (default): no pre-flight.
///     The drain step at the end of activate replays every element
///     of every signal's `parked_fires` queue through
///     `dispatch_listener_outcome` (same chain a live fire takes).
///     Suspended rows whose queue is empty stay waiting.
///   - `keep_suspended_only`: clear `parked_fires` on every row
///     before draining, so the drain finds nothing to replay.
///     Suspended-but-not-yet-fired stay waiting.
///   - `wipe_all`: drop every signal row + cancel every color
///     before TriggerSetup runs. Equivalent to having deactivated
///     with `wipe`; the project starts entirely fresh.
///
/// If the project has no preserved state (status=Registered, or
/// signals were already wiped), the choice is irrelevant and the
/// activate is a fresh boot.
#[derive(Debug, Default, Deserialize)]
pub struct ActivateRequest {
    #[serde(default, rename = "binaryHash")]
    pub binary_hash: Option<String>,
    #[serde(default, rename = "definitionHash")]
    pub definition_hash: Option<String>,
    #[serde(default, rename = "infraHash")]
    pub infra_hash: Option<String>,
    #[serde(default, rename = "reactivateChoice")]
    pub reactivate_choice: Option<String>,
}

pub async fn activate(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id_str): Path<String>,
    body: Option<Json<ActivateRequest>>,
) -> Result<Json<ActivateResponse>, (StatusCode, String)> {
    let id = id_str
        .parse::<uuid::Uuid>()
        .map_err(|_| (StatusCode::BAD_REQUEST, "bad id".into()))?;
    authorize_project(&state, &caller.0, id).await?;
    // Enforce against the reconciliation BEFORE the auto-build (same
    // discipline as `run`): this one endpoint serves three table verbs
    // (activate from Registered/Inactive, reactivate when preserved
    // state exists, resume_active while Deactivating), so any of the
    // three being offered admits the call. Everything else (already
    // Active, transitional, infra not ready) rejects here and never
    // triggers a build as a side effect of a rejected verb.
    require_action(&state, id, &["activate", "reactivate", "resume_active"]).await?;
    // Verb auto-build: make the latest saved source runnable before activating, so
    // activate can be clicked on a not-yet-built project. No-op when there is no
    // builder (the source was already built + registered).
    // Gated: a real build flips the `building` transition (single-flight,
    // cancellable); a concurrent build rejects with 409.
    crate::transition::ensure_built_gated(&state, id).await?;
    let (binary_hash, definition_hash, infra_hash, reactivate_choice) = match body {
        Some(Json(b)) => (
            b.binary_hash,
            b.definition_hash,
            b.infra_hash,
            b.reactivate_choice,
        ),
        None => (None, None, None, None),
    };
    activate_inner(
        &state,
        id,
        binary_hash,
        definition_hash,
        infra_hash,
        reactivate_choice,
    )
    .await
}

/// How `activate_inner` must roll back a failed Activating-window.
/// The deciding factor is whether `run_trigger_setup` STARTED: before
/// it, no signal state was touched by the activate (prior suspended /
/// entry signals are untouched), so rollback is just "un-stick the
/// status". Once trigger-setup starts, signals may be half-registered
/// and must be wiped.
enum ActivateRollback {
    /// CAS Activating→Inactive only. Keep existing signals (a failure
    /// before trigger-setup never touched them; wiping would nuke the
    /// project's prior suspended/parked work on a transient error).
    UnstickOnly,
    /// Full cleanup: CAS Activating→Inactive, cancel the setup run the
    /// row recorded, sweep superseded setup runs, drop all signal rows.
    /// Used once trigger-setup started (signals are in flux).
    WipeSignals,
}

struct ActivateWindowError {
    status: StatusCode,
    msg: String,
    rollback: ActivateRollback,
}

/// The Activating-window of `activate_inner`: every step that runs
/// while the project status is `Activating`, ending with the CAS to
/// `Active`. Factored out so `activate_inner` has ONE rollback site
/// for the whole window (a failure anywhere here must un-stick the
/// project from Activating; the caller does that once on Err).
///
/// The error carries the rollback MODE (`ActivateRollback`): failures
/// before `run_trigger_setup` un-stick only (keep signals), failures
/// from trigger-setup onward wipe (signals are in flux).
///
/// No keep-alive lease is involved: in the pooled model a listener pod is
/// reaped only when it holds ZERO signals, and placement happens per-signal
/// during register_signal, so there is no pre-spawned empty pod to keep alive
/// across the window (the first placed signal keeps its holder alive by being
/// on it). Single-flight against a concurrent activate is the exclusive
/// Activating transition (`try_begin_activating`), not a lock.
#[allow(clippy::too_many_arguments)]
async fn activate_trigger_setup_window(
    state: &DispatcherState,
    id: uuid::Uuid,
    project_id: &str,
    choice: &str,
    project: &ProjectDefinition,
    definition_hash: &str,
) -> Result<(), ActivateWindowError> {
    // Failures up to (not including) run_trigger_setup haven't touched
    // signal state, so they only need the status un-stuck.
    let unstick = |(status, msg): (StatusCode, String)| ActivateWindowError {
        status,
        msg,
        rollback: ActivateRollback::UnstickOnly,
    };

    // No activate keep-alive: in the pooled model a listener pod is
    // reaped only when it holds ZERO signals, and placement happens
    // per-signal during register_signal, so there is no pre-spawned
    // empty pod for the reaper to snipe mid-activate. The first placed
    // signal keeps its holder alive by being on it.

    // Apply the reactivate choice's destructive effect now that we
    // hold the exclusive Activating transition (validated by caller).
    apply_reactivate_choice(state, project_id, choice).await.map_err(unstick)?;

    // Sweep any prior TriggerSetup colors that leaked from a failed
    // previous activate. Safe because we won `try_begin_activating`:
    // no sibling activate is in flight, so every non-terminal
    // trigger_setup color here is a leftover from a dead prior
    // activate, never a concurrent one.
    sweep_superseded_trigger_setup_colors(state, project_id)
        .await
        .map_err(|e| {
            unstick((StatusCode::INTERNAL_SERVER_ERROR, format!("sweep superseded ts colors: {e}")))
        })?;

    // Run TriggerSetup. From here on signals are in flux, so a failure
    // wipes (the rollback cancels the setup run the row recorded). Its
    // register_signal tasks UPSERT entry rows on (project_id,
    // node_id), so existing rows from before a deactivate get
    // spec/mount/auth refreshed in place; the token survives, so
    // parked_fires still attached drain cleanly later.
    let kicks = compute_trigger_setup_kicks(project);
    if !kicks.is_empty() {
        run_trigger_setup(state, id, kicks, definition_hash).await.map_err(
            |(status, msg)| ActivateWindowError {
                status,
                msg,
                rollback: ActivateRollback::WipeSignals,
            },
        )?;
    }

    // From here trigger-setup succeeded but signals are registered, so
    // any failure still wipes (setup's own color is already terminal;
    // the rollback sweeps remaining leftovers).
    let wipe = |(status, msg): (StatusCode, String)| ActivateWindowError {
        status,
        msg,
        rollback: ActivateRollback::WipeSignals,
    };

    // Drop orphan entry rows: nodes that previously had triggers but
    // no longer do (user edited source while deactivated).
    drop_orphan_entry_rows(state, project_id, project)
        .await
        .map_err(|e| wipe((StatusCode::INTERNAL_SERVER_ERROR, format!("drop_orphan_entry_rows: {e}"))))?;

    // Reconcile the registries of every listener holding this project's
    // signals with the durable signal table (resume signals belong to
    // suspended executions whose workers are gone). /rehydrate is
    // idempotent. A signal whose holder was reaped (NULL placement) is
    // re-placed by the next fire via `ensure_placed_handle`.
    state
        .listeners
        .rehydrate_project(project_id, &state.pg_pool)
        .await
        .map_err(|e| wipe((StatusCode::INTERNAL_SERVER_ERROR, format!("listener rehydrate: {e}"))))?;

    // Flip Activating → Active. CAS guards against a concurrent
    // cancel_activate that flipped us to Inactive: in that case the
    // cancel already wiped our signals, so we surrender with no
    // further rollback (UnstickOnly: status is already Inactive, the
    // inner CAS no-ops).
    // The setup run this activation recorded is terminal by now
    // (`run_trigger_setup` returned on its terminal), so the color the
    // flip hands back is only bookkeeping being cleared.
    let cas_ok = state
        .projects
        .end_activating(id, &crate::project_store::ProjectLifecycle::active())
        .await
        .map_err(|e| wipe((StatusCode::INTERNAL_SERVER_ERROR, format!("end_activating: {e}"))))?
        .is_some();
    if !cas_ok {
        return Err(ActivateWindowError {
            status: StatusCode::CONFLICT,
            msg: "activate raced with cancel_activate; project is now Inactive".into(),
            rollback: ActivateRollback::UnstickOnly,
        });
    }

    // Window done: project is Active. Its signals are placed; their
    // holders stay alive on signal-row presence alone.
    Ok(())
}

/// In-process callable for `activate`. Used by `/infra/sync`'s
/// auto-reactivate path. Same body as the axum handler minus the
/// extractor plumbing.
pub async fn activate_inner(
    state: &DispatcherState,
    id: uuid::Uuid,
    binary_hash: Option<String>,
    definition_hash: Option<String>,
    infra_hash: Option<String>,
    reactivate_choice: Option<String>,
) -> Result<Json<ActivateResponse>, (StatusCode, String)> {
    state
        .projects
        .set_running_hashes(
            id,
            binary_hash.as_deref(),
            definition_hash.as_deref(),
            infra_hash.as_deref(),
            // Activation advances the running hashes but does not rebuild infra
            // images, so it leaves the stored infra tag map untouched.
            None,
        )
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("set_running_hashes: {e}")))?;

    // One coherent (hash, shape) pair for everything below: the
    // trigger checks here AND the kicks the activate window computes
    // must come from the definition recorded under the hash that
    // trigger-setup will journal (see `coherent_definition`).
    let (running_definition_hash, project) = coherent_definition(state, id).await?;
    let project_id = id.to_string();

    // Activate is the trigger-setup verb. A project without any
    // trigger nodes has nothing to register, so flipping its status
    // to Active is meaningless and creates an absorbing state: the
    // next sync would see was_active=true and re-call activate
    // forever. Refuse loudly so the CLI / extension knows the
    // verb doesn't apply.
    let has_triggers = project.nodes.iter().any(|n| n.features.is_trigger);
    if !has_triggers {
        return Err((
            StatusCode::PRECONDITION_FAILED,
            "project has no trigger nodes; nothing to activate. \
             Use `weft run` to fire an execution directly."
                .into(),
        ));
    }

    // Pre-flight: every requires_infra node must be Running. A
    // Stopped / Failed / Flaky node is just as bad as a missing one
    // from TriggerSetup's POV (the worker will try `endpoint_url`
    // and the broker will return None).
    let missing = missing_infra_nodes(state, &project_id, &project, None).await?;
    if !missing.is_empty() {
        return Err((
            StatusCode::PRECONDITION_REQUIRED,
            format!(
                "infra not running for: {}. Run `weft infra start` first.",
                missing.join(", ")
            ),
        ));
    }

    // A binary-hash change must kill the stale-image worker before
    // the activate's TriggerSetup exec runs, otherwise it's
    // dispatched against a worker that doesn't know about the new
    // trigger nodes. Idempotent (kill-by-binary-hash + respawn), so
    // it's safe before the single-flight CAS: two concurrent
    // activates both calling it is harmless, and keeping it before
    // the CAS means its failure leaves the project in its original
    // status rather than stranded in Activating. MUST propagate.
    // Wait policy: activate never silently kills running work; stale
    // workers drain (no new admissions, in-flight finishes) up to the
    // default cap before being replaced.
    reconcile_worker(
        state,
        &project_id,
        crate::infra_lifecycle_command::RunningPolicy::Wait,
        weft_broker_client::protocol::DEFAULT_DRAIN_TIMEOUT_SECS,
    )
    .await?;

    // Validate (don't yet apply) the reactivate choice. Validation is
    // a pure rejection and belongs in the read-only pre-flight; the
    // choice's DESTRUCTIVE effect (clearing parked / wiping signals)
    // is applied AFTER the single-flight CAS below, so a losing
    // concurrent activate that 409s never wipes the winner's state.
    let choice = reactivate_choice.as_deref().unwrap_or("execute_parked_keep_suspended");
    if !matches!(choice, "execute_parked_keep_suspended" | "keep_suspended_only" | "wipe_all") {
        return Err((
            StatusCode::BAD_REQUEST,
            format!(
                "unknown reactivate_choice '{choice}'; must be one of: \
                 execute_parked_keep_suspended, keep_suspended_only, wipe_all"
            ),
        ));
    }

    // Single-flight gate: atomically claim the Activating transition.
    // Activating is the one mutual-exclusion state in the lifecycle;
    // while a project is activating (registering trigger signals),
    // no second activation may start. `try_begin_activating` flips
    // the full activating() lifecycle IFF the project isn't already
    // Activating, and reports whether THIS call won. A losing caller
    // (a concurrent activate from another dispatcher Pod, a double-
    // click, CLI + extension racing) bails here with 409 BEFORE
    // the orphan sweep. This is
    // what makes the sweep below correct: having won the exclusive
    // transition, any non-terminal trigger_setup color we then see
    // is genuinely an orphan from a dead prior activate, never a
    // sibling activate's in-flight color.
    let won = state
        .projects
        .try_begin_activating(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("try_begin_activating: {e}")))?;
    if !won {
        return Err((
            StatusCode::CONFLICT,
            "project is already activating or building; wait for it to finish or cancel it"
                .into(),
        ));
    }
    // Won the transition: broadcast it (other tabs' action bars flip
    // to "Activating… (cancel)" without a verb round-trip) and keep
    // the heartbeat fresh for the whole in-process window so the
    // stuck-transition reaper only ever repairs a DEAD driver's row.
    // The guard drops when this function returns, on every path.
    crate::transition::publish_transition_changed(state, id).await;
    let _activation_heartbeat =
        crate::transition::TransitionHeartbeat::spawn(state.projects.clone(), id);

    // Everything from here to the Active CAS happens while the
    // project is Activating. ANY failure in this window must
    // un-stick the project (a stranded Activating locks out all
    // future activates). Rather
    // than hand-roll a rollback at each `?` (the footgun that
    // stranded the project before), the whole window is ONE fallible
    // block with ONE rollback site below. A future step added here
    // can't forget the un-stick.
    let setup = activate_trigger_setup_window(
        state, id, &project_id, choice, &project,
        &running_definition_hash,
    )
    .await;
    if let Err(ActivateWindowError { status, msg, rollback }) = setup {
        // Single rollback site for the whole window. The mode says
        // how much to undo: a pre-trigger-setup failure only un-sticks
        // the status (signals were never touched, so wiping would
        // destroy the project's prior suspended/parked work); a
        // trigger-setup-or-later failure wipes (signals are in flux).
        // Both are idempotent and safe if a concurrent cancel/success
        // already moved us out of Activating (the inner CAS no-ops).
        let rb = match rollback {
            ActivateRollback::UnstickOnly => match unstick_activating(state, id).await {
                // An un-stick-only rollback runs before setup started, so
                // the row can hold no setup color. One coming back means a
                // step after `run_trigger_setup` was mapped to the wrong
                // rollback: say so, and still never leave the run live.
                Ok(Some(Some(color))) => {
                    tracing::error!(
                        target: "weft_dispatcher::activate",
                        project_id = %id, %color,
                        "unstick-only rollback found a recorded setup run; cancelling it \
                         (a post-setup failure must roll back with WipeSignals)"
                    );
                    crate::api::execution::cancel_color(
                        state,
                        color,
                        &weft_core::exec::CancelCause::Runtime {
                            detail: "the activation failed and rolled back its setup run".into(),
                        },
                    )
                    .await
                    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("cancel_color: {e}")))
                }
                Ok(_) => Ok(()),
                Err(e) => Err(e),
            },
            ActivateRollback::WipeSignals => {
                // The activation failed on its own; the setup run it
                // spawned is cancelled by that failure, not by anyone's
                // decision.
                wipe_activating_state(
                    state, id, &project_id,
                    &weft_core::exec::CancelCause::Runtime {
                        detail: "the activation failed and rolled back its setup run".into(),
                    },
                )
                .await
            }
        };
        if let Err((rb_status, rb_msg)) = rb {
            tracing::error!(
                target: "weft_dispatcher::activate",
                project_id = %id, rb_status = %rb_status, rb_error = %rb_msg,
                "activate rollback failed; the stuck-transition reaper will repair \
                 the row once the heartbeat goes stale"
            );
        }
        // Broadcast whatever state the rollback landed (inactive on
        // success, or the still-stuck state the reaper will repair).
        crate::transition::publish_transition_changed(state, id).await;
        return Err((status, msg));
    }
    // Past here the project is Active (the window's final step CASed
    // it). A failure now is a 500 but the project is correctly
    // Active, NOT stranded, so no rollback.

    // Drain every queued fire that survived the inactive window.
    // Single loop, kind-agnostic: dispatch_listener_outcome routes
    // Resume vs Entry vs Drop based on what the listener returns,
    // exactly like a live fire. Runs after the Active CAS so the
    // gate relays instead of re-queueing.
    drain_parked_fires(state, &project_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("drain_parked_fires: {e}")))?;

    let urls = collect_listener_urls(state, &project_id).await.map_err(|e| {
        (StatusCode::INTERNAL_SERVER_ERROR, format!("collect_listener_urls: {e}"))
    })?;
    for url in &urls {
        state
            .events
            .publish(DispatcherEvent::TriggerUrlChanged {
                project_id: project_id.clone(),
                // User strings on a NOTIFY-path event: node ids are
                // user-authored and the url embeds the user's mount
                // path. Bound at construction.
                node_id: weft_core::truncate_user_string(&url.node_id, 4096),
                url: weft_core::truncate_user_string(&url.url, 4096),
            })
            .await;
    }
    state
        .events
        .publish(DispatcherEvent::ProjectActivated { project_id: project_id.clone() })
        .await;
    Ok(Json(ActivateResponse { urls }))
}

/// One drain pass: every signal row in the project with at least
/// one queued fire gets replayed through `dispatch_listener_outcome`.
/// The listener's `/process` returns the right `ProcessTarget`
/// (Resume for is_resume rows, Entry for entry rows, Drop if
/// obsolete) so the drain doesn't need to know what kind it's
/// draining.
///
/// Per row (see `drain_one_token`):
///   1. Atomically claim: UPDATE drain_claimed_at = now,
///      drain_claimed_by = <fresh nonce> WHERE drain_claimed_at IS
///      NULL. If 0 rows updated, another pass raced us and won; skip.
///   2. Pop-then-dispatch loop: read head (`parked_fires -> 0`),
///      dispatch, then pop it (`parked_fires - 0::int`) FENCED on our
///      claim nonce (`WHERE drain_claimed_by = <ours>`). One element
///      commits at a time, so a mid-loop failure leaves the unsent
///      remainder intact in FIFO order for the next activate. Appends
///      from concurrent fires land at the tail, so index 0 is stable
///      across the dispatch window. If a stale-claim sweep handed the
///      row to a sibling pod mid-drain, our fenced pop matches 0 rows
///      and we abort: the element we just dispatched dedups at the
///      task table (`ParkedFire.id` -> `enqueue_dedup`), and the new
///      owner re-drives from the same head.
///   3. Release the row claim FENCED on our nonce (so we never clear a
///      sibling's claim that took over), regardless of outcome.
///
/// A pod crash between steps 1 and 3 leaves the claim set;
/// [`release_stale_drain_claims`] (run by this pass's pre-step and by
/// the reaper's parked-fire sweep) releases claims older than the
/// threshold so the row becomes drainable again.
async fn drain_parked_fires(
    state: &DispatcherState,
    project_id: &str,
) -> anyhow::Result<()> {
    use sqlx::Row;

    // Pre-pass: release stale claims. A crashed pod could have left
    // drain_claimed_at set; the shared release clears any claim older
    // than the threshold (globally: a claim that old is dead whichever
    // pass notices it) so this pass can claim the rows itself.
    release_stale_drain_claims(&state.pg_pool).await?;

    // Bound on the snapshot-loop below. A fire whose
    // `lookup_signal_routing` saw status=Activating just before the
    // CAS to Active commits will append to parked_fires AFTER our
    // first snapshot. We rerun the snapshot until it returns empty
    // so those fires drain in the same activate pass. Cap at 3
    // iterations so a stuck token (something appending faster than
    // we can dispatch) cannot livelock the activate handler; an
    // operator-visible failure beats an infinite loop.
    const MAX_DRAIN_PASSES: u32 = 3;
    for pass in 0..MAX_DRAIN_PASSES {
        let rows = sqlx::query(
            "SELECT token FROM signal \
             WHERE project_id = $1 \
               AND jsonb_array_length(parked_fires) > 0 \
               AND drain_claimed_at_unix IS NULL \
             ORDER BY created_at ASC",
        )
        .bind(project_id)
        .fetch_all(&state.pg_pool)
        .await?;

        if rows.is_empty() {
            return Ok(());
        }
        tracing::debug!(
            target: "weft_dispatcher::activate",
            project_id, pass, count = rows.len(),
            "drain_parked_fires pass"
        );
        for row in rows {
            let token: String = row.try_get("token")?;
            drain_one_token(state, project_id, &token).await?;
        }
    }
    // Final check: any leftover queued fires get one warn line so an
    // operator can investigate. They drain on the next activate.
    let leftover: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM signal WHERE project_id = $1 \
           AND jsonb_array_length(parked_fires) > 0",
    )
    .bind(project_id)
    .fetch_one(&state.pg_pool)
    .await?;
    if leftover.0 > 0 {
        tracing::warn!(
            target: "weft_dispatcher::activate",
            project_id, leftover = leftover.0,
            "drain_parked_fires exceeded MAX_DRAIN_PASSES; \
             leftover fires will drain on next activate"
        );
    }
    Ok(())
}

/// The reaper's half of the parked-fire retry: every unclaimed signal row
/// of an Active project whose queue head is due gets one drain pass. A
/// fire that failed to route re-parked itself with a backoff stamp
/// (`ParkedFire::not_before_unix`); nothing else drains an Active
/// project's queue (activate drains once, at activation), so without this
/// sweep a re-parked fire would wait for the next activate, which may
/// never come. Idempotent across pods: `drain_one_token` claims the row.
pub(crate) async fn drain_due_parked_fires(state: &DispatcherState) -> anyhow::Result<()> {
    // Stale drain claims first: a pod that died mid-drain leaves its
    // claim set, and this sweep is the only thing that re-drives an
    // Active project's queue, so a stale claim would starve that
    // token's retries until the next activate. The same release the
    // activate pre-pass runs; a mistaken release is safe because every
    // pop and re-stamp is fenced on the claim nonce.
    release_stale_drain_claims(&state.pg_pool).await?;
    for (token, project_id) in due_parked_tokens(&state.pg_pool, crate::lease::now_unix()).await? {
        if let Err(e) = drain_one_token(state, &project_id, &token).await {
            // One token's dispatch failure must not stop the others;
            // the failed head was re-stamped with a longer backoff by
            // the drain itself, so this token comes back when due.
            tracing::warn!(
                target: "weft_dispatcher::reaper",
                project_id = %project_id,
                token = %token,
                error = %e,
                "parked-fire sweep: dispatch failed; the head's backoff was lengthened"
            );
        }
    }
    Ok(())
}

/// The sweep's selection: every signal row of an ACTIVE project whose
/// queue holds at least one element, is unclaimed, and whose HEAD is due
/// (`not_before_unix` in the past; an element from before the field
/// existed reads as due now). Pool-level on purpose so the db-tests can
/// pin the predicate against the real statement. Head-only on purpose:
/// the queue is FIFO, so a backing-off head blocks its token's tail (a
/// later fire overtaking it would reorder one trigger's events) and the
/// sweep simply comes back for the token once the head is due.
pub async fn due_parked_tokens(
    pool: &sqlx::PgPool,
    now: i64,
) -> anyhow::Result<Vec<(String, String)>> {
    Ok(sqlx::query_as::<_, (String, String)>(
        "SELECT s.token, s.project_id FROM signal s \
         JOIN project p ON p.id::text = s.project_id \
         WHERE p.status = 'active' \
           AND jsonb_array_length(s.parked_fires) > 0 \
           AND s.drain_claimed_at_unix IS NULL \
           AND COALESCE((s.parked_fires -> 0 ->> 'not_before_unix')::bigint, 0) <= $1",
    )
    .bind(now)
    .fetch_all(pool)
    .await?)
}

/// How old a drain claim must be before any owner is considered dead.
/// A claim is held for one pop-dispatch pass, so five minutes is far
/// beyond any live drain; a takeover younger than this would race a
/// healthy drain for nothing (the fence keeps it safe, not pointless).
const DRAIN_CLAIM_STALE_SECS: i64 = 300;

/// Release every drain claim older than [`DRAIN_CLAIM_STALE_SECS`],
/// clearing both claim columns. Run by the activate pre-pass (so an
/// activation's own drain can claim rows a crashed pod left held) and
/// by the reaper's parked-fire sweep (so a stale claim cannot starve an
/// Active project's retries). Global on purpose: a claim that old is
/// dead whichever pass notices it, and the fenced pops make a mistaken
/// release safe (the old owner aborts on its next fenced write, the new
/// owner re-drives, and dispatched elements dedup at the task table).
pub async fn release_stale_drain_claims(pool: &sqlx::PgPool) -> anyhow::Result<()> {
    sqlx::query(
        "UPDATE signal SET drain_claimed_at_unix = NULL, drain_claimed_by = NULL \
         WHERE drain_claimed_at_unix IS NOT NULL \
           AND drain_claimed_at_unix < $1",
    )
    .bind(crate::lease::now_unix() - DRAIN_CLAIM_STALE_SECS)
    .execute(pool)
    .await?;
    Ok(())
}

/// Drain one signal row's queue. Internal helper: caller has already
/// established the row has fires and is unclaimed. Idempotent under
/// retry: if the row's queue becomes empty mid-loop (e.g. another
/// drain raced; or our pop sequence finished), we exit cleanly.
pub(crate) async fn drain_one_token(
    state: &DispatcherState,
    project_id: &str,
    token: &str,
) -> anyhow::Result<()> {
    use sqlx::Row;

    // Atomic claim with a per-claim owner nonce. If another pass beat
    // us, bail. The nonce fences every subsequent pop + the release:
    // if a stale-claim sweep on a sibling pod takes the row over
    // mid-drain, our fenced pop matches 0 rows and we abort, rather
    // than popping an element the new owner already dispatched.
    let owner = uuid::Uuid::new_v4().to_string();
    let claim = sqlx::query(
        "UPDATE signal SET drain_claimed_at_unix = $2, drain_claimed_by = $3 \
         WHERE token = $1 AND drain_claimed_at_unix IS NULL",
    )
    .bind(token)
    .bind(crate::lease::now_unix())
    .bind(&owner)
    .execute(&state.pg_pool)
    .await?;
    if claim.rows_affected() == 0 {
        return Ok(());
    }

    // The signal's own tenant (frozen on the row at register), used to stamp the
    // dispatched fire's tasks/spawns, so the fire path never re-derives it.
    let tenant: String =
        sqlx::query_scalar("SELECT tenant_id FROM signal WHERE token = $1")
            .bind(token)
            .fetch_one(&state.pg_pool)
            .await?;

    // Pop-then-dispatch loop. Head-stable invariant: appends from
    // concurrent fires land at the tail of the array, so `index 0`
    // is always the next element to dispatch even under concurrent
    // /signal/{token} writes. Crash between dispatch-success and
    // pop is safe: each queue element carries its own per-fire UUID
    // (`ParkedFire.id`), passed to `dispatch_listener_outcome` as
    // the dedup nonce. On retry, the same element produces the same
    // dedup key, so the RouteEntry task collapses at the task table.
    // Distinct queued fires have distinct UUIDs and never collapse.
    let outcome = loop {
        let head_row = sqlx::query(
            "SELECT parked_fires -> 0 AS head \
             FROM signal WHERE token = $1",
        )
        .bind(token)
        .fetch_optional(&state.pg_pool)
        .await?;
        let Some(head_row) = head_row else {
            // Row vanished mid-drain (CASCADE delete?). Nothing to do.
            break Ok(());
        };
        let head: Option<Value> = head_row.try_get("head")?;
        // `parked_fires -> 0` returns SQL NULL (decoded as None) for
        // an empty array; an actual queued element decodes to
        // `Some(Value::Object(_))`. Anything else is a schema bug;
        // we fail rather than guess.
        let Some(head) = head else {
            break Ok(());
        };
        // Typed deserialize against the shared ParkedFire schema so
        // the writer (apply_lifecycle_gate) and the reader stay in
        // lockstep; a typo on either side becomes a compile error.
        let fire: crate::api::signal::ParkedFire =
            serde_json::from_value(head).map_err(|e| {
                anyhow::anyhow!("malformed parked_fires element for token {token}: {e}")
            })?;
        // A re-parked fire in its backoff window blocks its token's
        // queue: the queue is FIFO, and letting a later fire overtake it
        // would reorder one trigger's events. The reaper's parked-fire
        // sweep re-drives this token once the head is due.
        let now = crate::lease::now_unix();
        if fire.not_before_unix > now {
            tracing::debug!(
                target: "weft_dispatcher::activate",
                project_id, token, fire_id = %fire.id, attempts = fire.attempts,
                due_in_secs = fire.not_before_unix - now,
                "head of the parked queue is backing off; leaving the token for the sweep"
            );
            break Ok(());
        }
        // Keep the id and the attempt count: the dispatch moves
        // `fire.payload`, and both are needed afterward (the id to
        // remove or re-stamp this exact element by id, the count to
        // lengthen the backoff when the dispatch failed).
        let fire_id = fire.id.clone();
        let attempts_before = fire.attempts;

        match crate::api::signal::dispatch_listener_outcome(
            state,
            token,
            project_id,
            &tenant,
            fire.payload,
            Some(crate::api::signal::ParkedRef { id: &fire_id, attempts: attempts_before }),
        )
        .await
        {
            Ok(_) => {
                // Remove the element we just dispatched BY ID (not by
                // array index), FENCED on our claim nonce. By-id removal
                // commutes with a concurrent success-path removal of a
                // different fire, so a sibling removing the head out from
                // under us can't make us delete the wrong element (the
                // old index-0 pop assumed a head-stable array, which the
                // route_entry success-path removal breaks). If our claim
                // was taken over (0 rows), abort: the dispatched element
                // dedups at the task table via its fire id, and the new
                // owner re-drives.
                let removed = crate::api::signal::remove_parked_fire(
                    &state.pg_pool,
                    token,
                    &fire_id,
                    Some(&owner),
                )
                .await?;
                if removed == 0 {
                    break Ok(());
                }
            }
            Err((status, msg)) => {
                // The dispatch failed without popping the element, so
                // the fire is still the queue's head. Re-stamp it IN
                // PLACE with a longer backoff (a pop-and-re-append would
                // send it to the back of the queue, reordering one
                // trigger's events), and leave the remainder behind it.
                // The retry is whoever drains next: this activate's next
                // pass re-selects the token but stops at the future
                // stamp, and the reaper's parked-fire sweep comes back
                // once the head is due. Without the re-stamp that sweep
                // would retry a persistently failing dispatch every 5s
                // forever.
                let attempts = attempts_before + 1;
                let backoff = crate::api::signal::park_backoff_secs(attempts);
                let restamped = crate::api::signal::restamp_parked_fire(
                    &state.pg_pool,
                    token,
                    &fire_id,
                    attempts,
                    crate::lease::now_unix() + backoff,
                    Some(&owner),
                )
                .await?;
                if restamped == 0 {
                    // Our claim was taken over mid-drain: nothing of
                    // ours committed (the dispatch failed), so there is
                    // nothing to finish; the new owner re-reads the same
                    // head and retries.
                    break Ok(());
                }
                tracing::warn!(
                    target: "weft_dispatcher::activate",
                    project_id, token, fire_id = %fire_id, %status,
                    attempts, retry_in_secs = backoff,
                    error = %msg,
                    "drain: dispatch failed; head re-stamped, retries when due"
                );
                break Err(anyhow::anyhow!("dispatch failed: {msg}"));
            }
        }
    };

    // Release the claim regardless of outcome, FENCED on our nonce: if
    // a sibling already took the row over via a stale-claim sweep, we
    // must not clear ITS claim. A no-op release (0 rows) is fine.
    sqlx::query(
        "UPDATE signal SET drain_claimed_at_unix = NULL, drain_claimed_by = NULL \
         WHERE token = $1 AND drain_claimed_by = $2",
    )
    .bind(token)
    .bind(&owner)
    .execute(&state.pg_pool)
    .await?;

    // Surface the dispatch error to the activate caller so the
    // operator sees the failure. Subsequent rows in the snapshot
    // still won't be drained this pass; the next activate gets them.
    outcome
}

/// Cancel every non-terminal TriggerSetup run of `project_id` that
/// nobody is driving any more, every one attempted before any error
/// is reported. The current activation's own run is never among them:
/// its color is on the project row (`activating_ts_color`), and the
/// caller cancels it first with the cause it knows. Everything found
/// here is a leftover of an older activation whose driver died before
/// it could cancel its run, and the one truth the sweep can vouch for
/// about such a run is that a newer activation superseded it. A row
/// this table cannot parse is schema drift: the journal read fails
/// loud rather than skipping a run the sweep exists to catch.
async fn sweep_superseded_trigger_setup_colors(
    state: &DispatcherState,
    project_id: &str,
) -> anyhow::Result<()> {
    let colors = state
        .journal
        .list_non_terminal_colors_for_project(
            project_id,
            Some(weft_core::context::Phase::TriggerSetup),
        )
        .await?;
    if colors.is_empty() {
        return Ok(());
    }
    tracing::info!(
        target: "weft_dispatcher::activate",
        project_id, ?colors,
        "cancelling trigger_setup runs left by older activations"
    );
    let superseded = weft_core::exec::CancelCause::Runtime {
        detail: "a newer activation superseded this trigger setup run".into(),
    };
    let targets: Vec<(weft_core::Color, &weft_core::exec::CancelCause)> =
        colors.iter().map(|c| (*c, &superseded)).collect();
    crate::api::execution::cancel_colors(state, &targets).await?;
    Ok(())
}

/// Sweep entry-trigger rows whose node no longer exists in the
/// project source. Called after TriggerSetup so any node the user
/// removed-while-parked has its leftover signal row dropped.
/// Resume rows (per-suspension) skip this: the corresponding
/// suspended execution is the source of truth for them.
async fn drop_orphan_entry_rows(
    state: &DispatcherState,
    project_id: &str,
    project: &ProjectDefinition,
) -> anyhow::Result<()> {
    let live_node_ids: std::collections::HashSet<&str> =
        project.nodes.iter().map(|n| n.id.as_str()).collect();
    let signals = state.journal.signal_list_for_project(project_id).await?;
    let orphans: Vec<crate::journal::SignalRegistration> = signals
        .into_iter()
        .filter(|s| !s.is_resume && !live_node_ids.contains(s.node_id.as_str()))
        .collect();
    crate::api::signal::delete_signals(state, &orphans)
        .await
        .map_err(|(_, msg)| anyhow::anyhow!("delete_signals: {msg}"))?;
    Ok(())
}

/// Apply an activate `reactivate_choice`'s destructive effect on the
/// project's parked/suspended signal state. Run AFTER the
/// single-flight CAS so a losing concurrent activate can't wipe the
/// winner's state. `choice` must already be validated.
///   - `execute_parked_keep_suspended`: no-op (the drain at the end
///     of activate replays every parked fire).
///   - `keep_suspended_only`: clear parked fires, keep suspensions.
///   - `wipe_all`: drop every signal row.
async fn apply_reactivate_choice(
    state: &DispatcherState,
    project_id: &str,
    choice: &str,
) -> Result<(), (StatusCode, String)> {
    match choice {
        "execute_parked_keep_suspended" => Ok(()),
        "keep_suspended_only" => {
            sqlx::query(
                "UPDATE signal SET parked_fires = '[]'::jsonb \
                 WHERE project_id = $1 AND jsonb_array_length(parked_fires) > 0",
            )
            .bind(project_id)
            .execute(&state.pg_pool)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("clear parked: {e}")))?;
            Ok(())
        }
        "wipe_all" => wipe_project_signals(state, project_id).await,
        _ => unreachable!("reactivate_choice validated by caller"),
    }
}

/// Un-stick a project from Activating: CAS Activating → Inactive
/// (wiped lifecycle). Returns whether THIS call won the transition.
/// A loss means a concurrent activate-success (→Active) or another
/// cancel already moved us out of Activating, in which case the
/// caller must NOT proceed to wipe signals (it would nuke a
/// freshly-active project's triggers, or double-wipe a cancel's).
///
/// This is the minimal rollback: it touches NO signal state, so it's
/// the correct undo for a failure BEFORE trigger-setup ran (the
/// project returns to Inactive with its prior suspended/parked
/// signals intact; the next activate retries cleanly).
///
/// Returns the setup color the activation had recorded when the CAS
/// wins (`Some(None)` if setup never started), `None` when it lost.
async fn unstick_activating(
    state: &DispatcherState,
    id: uuid::Uuid,
) -> Result<Option<Option<weft_core::Color>>, (StatusCode, String)> {
    state
        .projects
        .end_activating(id, &crate::project_store::ProjectLifecycle::wiped())
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("end_activating: {e}")))
}

/// Cancel-activate / TriggerSetup-failure cleanup: un-stick the
/// status (via `unstick_activating`) AND wipe signal state (cancel TS
/// color + sweep orphan TS colors + drop all signal rows). Used once
/// trigger-setup has started, so signals are in flux and must go.
/// Idempotent: a partial run leaves the next call with less work, and
/// if the un-stick CAS loses (a concurrent activate-success/cancel
/// already left Activating) it returns early WITHOUT wiping.
///
/// The current activation's setup run comes off the project row, where
/// the activation recorded it before starting the run, and it comes out
/// of the un-stick itself (one atomic step, so a driver racing this
/// cannot slip a run in between). `cause` names why THAT run is being
/// cancelled, and the three callers each carry a different truth: the
/// failed activate's rollback (runtime), a person's cancel-activate
/// (user), the reaper's stuck-activation wipe (the activation's driver
/// died). The journal repeats it verbatim, so it must be the caller's
/// truth, never a guess made here. Setup runs left by OLDER activations
/// are not this activation's story; the sweep journals them as
/// superseded.
pub(crate) async fn wipe_activating_state(
    state: &DispatcherState,
    id: uuid::Uuid,
    project_id: &str,
    cause: &weft_core::exec::CancelCause,
) -> Result<(), (StatusCode, String)> {
    // Un-stick FIRST, and take the setup color out of the same step. If
    // we lose the CAS, activate already finished cleanly (or a sibling
    // cancel did the wipe); do not touch signals.
    let Some(ts_color) = unstick_activating(state, id).await? else {
        return Ok(());
    };
    if let Some(c) = ts_color {
        crate::api::execution::cancel_color(state, c, cause)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("cancel_color: {e}")))?;
    }
    // Everything else still running setup belongs to an older, dead
    // activation.
    sweep_superseded_trigger_setup_colors(state, project_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("sweep ts: {e}")))?;
    // Drop every signal row + unregister from the listener via the
    // shared helper (DB-first-listener-second ordering).
    crate::api::signal::delete_signals_for_project(state, project_id).await?;
    Ok(())
}

/// "wipe_all" pre-flight: cancel every color in the project + drop
/// every signal row. Used by activate's wipe_all reactivate-choice
/// AND by deactivate's wipe mode. Cancellation reaches into
/// in-flight workers; row drop unregisters from the listener.
async fn wipe_project_signals(
    state: &DispatcherState,
    project_id: &str,
) -> Result<(), (StatusCode, String)> {
    let colors = state
        .journal
        .list_non_terminal_colors_for_project(project_id, None)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("list colors: {e}")))?;
    // Every run is attempted before any error is reported: one failing
    // cancel must not leave the runs after it live with their wakes
    // registered on a project being wiped.
    let user = weft_core::exec::CancelCause::User;
    let targets: Vec<(weft_core::Color, &weft_core::exec::CancelCause)> =
        colors.iter().map(|c| (*c, &user)).collect();
    crate::api::execution::cancel_colors(state, &targets)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("cancel: {e}")))?;
    crate::api::signal::delete_signals_for_project(state, project_id).await
}

/// Body for `POST /projects/{id}/deactivate`.
///
/// `mode` controls what survives the inactive window:
///   - `wipe`:      every signal row + cancel every color
///                  (suspended ones too). Fresh slate on reactivate.
///   - `hibernate`: signal rows stay; gate parks fires for the
///                  grace window, then refuses; the project is
///                  hidden from consumer enumeration the entire
///                  time.
///   - `park`:      signal rows stay visible; gate parks fires
///                  indefinitely (no deadline).
///
/// `runningPolicy` controls how the deactivate interacts with
/// in-flight executions:
///   - `cancel`: cancel running (non-suspended) executions
///               immediately, then flip to inactive. Synchronous.
///   - `wait`:   set status=deactivating; new fires already park,
///               but running executions drain naturally. The
///               journal-bridge CASes status to inactive once the
///               last one terminates. The user can re-deactivate
///               with runningPolicy=cancel to give up on the wait,
///               or activate to roll back into Active.
// `DeactivationMode` is the wire contract for the `mode` field on a
// `DeactivateSpec`. It lives in `weft-broker-client::protocol` so
// the supervisor + dispatcher share one source of truth. Re-export
// here so this module stays the canonical home for deactivation glue.
pub use weft_broker_client::protocol::DeactivationMode;

/// Run the trigger-deactivation side effects embedded in Sync /
/// Stop / Terminate. Single place for the validate + execute
/// pair; previously duplicated as `TriggerDeactivation::execute`
/// in `api/infra.rs`, but the spec shape is the same as the wire
/// `DeactivateSpec`, so the executor lives next to the rest of
/// the deactivation machinery. The validation rule itself lives
/// on `DeactivateSpec::validate` (broker, dispatcher, supervisor
/// share one validator); this site adds the `triggerDeactivation:`
/// prefix so clients see which field tripped the check.
pub async fn execute_trigger_deactivation(
    state: &DispatcherState,
    id: uuid::Uuid,
    spec: &weft_broker_client::protocol::DeactivateSpec,
) -> Result<(), (StatusCode, String)> {
    spec.validate()
        .map_err(|m| (StatusCode::BAD_REQUEST, format!("triggerDeactivation: {m}")))?;
    let existed = deactivate_project_with_mode(
        state,
        id,
        spec.mode,
        spec.grace_minutes,
        spec.running_policy,
        spec.drain_timeout_secs
            .unwrap_or(weft_broker_client::protocol::DEFAULT_DRAIN_TIMEOUT_SECS),
        false, // user-initiated (stop / upgrade / terminate)
    )
    .await?;
    if !existed {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            "triggerDeactivation: project disappeared mid-deactivate".into(),
        ));
    }
    Ok(())
}

pub async fn deactivate(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id_str): Path<String>,
    Json(spec): Json<weft_broker_client::protocol::DeactivateSpec>,
) -> Result<StatusCode, StatusError> {
    let id = id_str
        .parse::<uuid::Uuid>()
        .map_err(|_| (StatusCode::BAD_REQUEST, "bad id".to_string()))?;
    // Marked gate: deactivate is part of teardown flows (`weft rm
    // --journal` quiesces through it), so its "no such project" must
    // be tellable apart from a routing 404, same as `remove`/`status`.
    authorize_project_marked(&state, &caller.0, id).await?;
    spec.validate()
        .map_err(|m| (StatusCode::BAD_REQUEST, m.to_string()))?;
    let existed = deactivate_project_with_mode(
        &state,
        id,
        spec.mode,
        spec.grace_minutes,
        spec.running_policy,
        spec.drain_timeout_secs
            .unwrap_or(weft_broker_client::protocol::DEFAULT_DRAIN_TIMEOUT_SECS),
        false, // user-initiated (the standalone Deactivate verb)
    )
    .await?;
    if !existed {
        // Vanished between the gate and the deactivate (a concurrent
        // rm): same "no project I may see" answer, marker included.
        return Err(StatusError::NotMyProject);
    }
    Ok(StatusCode::NO_CONTENT)
}

/// Body for `POST /projects/{id}/resync`: the activate fields plus
/// the trigger-deactivation choice (mode + runningPolicy + drain cap,
/// the SAME picker as the standalone Deactivate). Required when the
/// project is Active (412 otherwise); ignored when it isn't (a resync
/// of an inactive project is just an activate).
#[derive(Debug, Default, Deserialize)]
pub struct ResyncRequest {
    #[serde(flatten)]
    pub activate: ActivateRequest,
    #[serde(default, rename = "triggerDeactivation")]
    pub trigger_deactivation: Option<weft_broker_client::protocol::DeactivateSpec>,
}

/// `POST /projects/{id}/resync`. Deactivate-then-activate against
/// (optionally) fresh source / infra hashes, bringing the deployed
/// trigger/worker shape in line with the current source. The
/// deactivation uses the USER'S spec (never a hardcoded wipe): with
/// `runningPolicy = wait` the handler drains through THE shared drain
/// loop up to the spec's cap, cancels the stragglers, lands the
/// deactivation, then reactivates. Refuses with 412 if the project
/// has infra nodes and infra isn't running (the deactivate step still
/// runs; the user is told to start infra before reclicking activate).
pub async fn resync(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id_str): Path<String>,
    body: Option<Json<ResyncRequest>>,
) -> Result<Json<ActivateResponse>, (StatusCode, String)> {
    use crate::project_store::ProjectStatus;
    let id = id_str
        .parse::<uuid::Uuid>()
        .map_err(|_| (StatusCode::BAD_REQUEST, "bad id".into()))?;
    authorize_project(&state, &caller.0, id).await?;
    let body = body.map(|Json(b)| b).unwrap_or_default();
    let project = state
        .projects
        .project(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("project: {e}")))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "project not found".into()))?;
    let project_id = id.to_string();

    // 1. Deactivate, ONLY when the project is actually Active, with
    //    the user's spec (the shared picker asked both questions:
    //    what happens to incoming signals, and what happens to
    //    running executions). An inactive project has nothing to
    //    deactivate; activate's own UPSERT + orphan-row sweep
    //    reconcile its signals.
    let lifecycle = state
        .projects
        .lifecycle(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("lifecycle: {e}")))?
        .ok_or((StatusCode::NOT_FOUND, "project not found".into()))?;
    if lifecycle.status == ProjectStatus::Active {
        let Some(spec) = body.trigger_deactivation.as_ref() else {
            return Err((
                StatusCode::PRECONDITION_REQUIRED,
                "project is active; triggerDeactivation { mode, runningPolicy, \
                 graceMinutes?, drainTimeoutSecs? } required so the user can choose \
                 how triggers come down (same picker as the standalone Deactivate)"
                    .into(),
            ));
        };
        execute_trigger_deactivation(&state, id, spec).await?;
        if spec.running_policy == crate::infra_lifecycle_command::RunningPolicy::Wait {
            // Wait for the drain through THE shared loop, capped at
            // the spec's cap; stragglers past it are cancelled with
            // the ONE cancel helper, and the ONE landing CAS flips
            // the row before the reactivate.
            let cap = spec
                .drain_timeout_secs
                .unwrap_or(weft_broker_client::protocol::DEFAULT_DRAIN_TIMEOUT_SECS);
            let clock = weft_platform_traits::SystemClock;
            let outcome = weft_platform_traits::drain_until_zero(
                &clock,
                std::time::Duration::from_secs(cap),
                "resync",
                || async {
                    running_count(&state, &project_id, None)
                        .await
                        .map(|n| n as i64)
                        .map_err(|e| {
                            (StatusCode::INTERNAL_SERVER_ERROR, format!("running_count: {e}"))
                        })
                },
            )
            .await?;
            if let weft_platform_traits::DrainOutcome::TimedOut { still_running } = outcome {
                tracing::warn!(
                    target: "weft_dispatcher::api::project",
                    project_id = %project_id,
                    still_running,
                    drain_timeout_secs = cap,
                    "resync drain cap reached; cancelling remaining executions"
                );
                cancel_running_non_suspended(&state, &project_id, None).await?;
            }
            crate::journal_bridge::try_finish_drain(&state, &project_id, None)
                .await
                .map_err(|e| {
                    (StatusCode::INTERNAL_SERVER_ERROR, format!("try_finish_drain: {e}"))
                })?;
        }
    }

    // 2. Reactivate precondition: every requires_infra node must be
    //    running. If not, leave the project deactivated and surface
    //    a clear error. The user starts infra and clicks Activate.
    let missing = missing_infra_nodes(&state, &project_id, &project, None).await?;
    if !missing.is_empty() {
        return Err((
            StatusCode::PRECONDITION_REQUIRED,
            format!(
                "deactivated; cannot reactivate (infra not running for: {}). \
                 Run `weft infra start`, then `weft activate`.",
                missing.join(", ")
            ),
        ));
    }

    // 3. Reactivate. Reuses the activate handler so hash persistence
    //    + atomic-cleanup-on-failure semantics are identical. The caller was
    //    already authorized against this project at the top of resync; activate
    //    re-checks the same gate (cheap, and keeps activate self-contained).
    activate(State(state), caller, Path(id_str), Some(Json(body.activate))).await
}

/// Reconcile the project's live worker pod with what current state
/// says it should be, on two INDEPENDENT staleness axes:
///
///   - IMAGE: its baked-in binary_hash no longer matches the project's
///     current `running_binary_hash` (a node-impl edit or engine bump;
///     the worker binary embeds the engine + node implementations at
///     compile time). Pure definition edits (config / topology) don't
///     trip this: the worker re-fetches the definition per claim.
///   - PLACEMENT: it runs in a different namespace than the placement
///     resolver's answer (`resolve_worker_placement`): the source
///     gained/lost infra, or the project namespace was created / torn
///     down. There is NO "move": a worker's namespace is fixed at
///     spawn, so reconciliation is kill-then-respawn.
///
/// The kill is GATED on the doomed pods' running work.
/// `RunningPolicy::Wait` marks them DRAINING (the existing worker
/// mechanism: no NEW executions are admitted to them; anything new
/// spawns a fresh CORRECT pod via the resolver; in-flight work
/// finishes and the pod idle-exits) and waits through THE shared
/// drain loop (`weft_platform_traits::drain_until_zero`, the same
/// mechanism the supervisor's stop/terminate drain uses) up to the
/// caller's `drain_timeout_secs` cap, then kills the stragglers with
/// a loud warning. `RunningPolicy::Cancel` cancels the running
/// non-suspended executions and kills immediately. Never a silent
/// kill. Suspended executions survive either way (they hold no
/// worker; their resume respawns wherever placement then says).
/// Because Wait can sit for minutes, callers MUST NOT invoke this
/// while holding the per-project advisory lock.
///
/// Kill ordering: `mark_dead` FIRST so the journal-fencing trigger
/// rejects any late write from the doomed pod, kubectl delete second.
/// Idempotent: a no-op when both axes match, or when no pod is alive.
pub async fn reconcile_worker(
    state: &DispatcherState,
    project_id: &str,
    running_policy: crate::infra_lifecycle_command::RunningPolicy,
    drain_timeout_secs: u64,
) -> Result<(), (StatusCode, String)> {
    let internal = |e: anyhow::Error| {
        (StatusCode::INTERNAL_SERVER_ERROR, format!("reconcile_worker: {e:#}"))
    };
    let project_uuid: uuid::Uuid = match project_id.parse() {
        Ok(u) => u,
        Err(_) => return Ok(()),
    };
    // ALL alive pods: a project under parallel load runs N workers,
    // and any subset can be stale. No live worker: nothing to compare
    // against; the next spawn goes through the resolver and lands
    // right by construction. No fallback on DB errors: a failure here
    // must propagate or the check would kill a healthy pod (false
    // positive) or skip a stale one (false negative).
    let alive =
        weft_task_store::worker_pod::alive_pods_for_project_full(&state.pg_pool, project_id)
            .await
            .map_err(internal)?;
    if alive.is_empty() {
        return Ok(());
    }
    // With live workers, a missing binary hash is a broken ordering
    // invariant (sync writes the hash before any task that spawns).
    let want_hash = state
        .projects
        .running_binary_hash(project_uuid)
        .await
        .map_err(internal)?
        .ok_or_else(|| {
            internal(anyhow::anyhow!(
                "project {project_id} has a live worker but no running_binary_hash; \
                 sync ordering invariant broken"
            ))
        })?;
    let placement = crate::placement::resolve_worker_placement(state, project_id)
        .await
        .map_err(internal)?
        .ok_or_else(|| {
            internal(anyhow::anyhow!(
                "project {project_id} vanished mid-reconcile; cannot place worker"
            ))
        })?;
    // The doomed set: every pod stale on either axis. Fresh pods stay
    // untouched (their executions keep running throughout).
    let doomed: Vec<(String, String, String)> = alive
        .into_iter()
        .filter(|(_, ns, hash)| *hash != want_hash || *ns != placement.namespace)
        .collect();
    if doomed.is_empty() {
        return Ok(());
    }
    let doomed_names: Vec<String> = doomed.iter().map(|(p, _, _)| p.clone()).collect();
    tracing::info!(
        target: "weft_dispatcher::api::project",
        project_id,
        doomed = ?doomed_names,
        want_hash = %want_hash,
        to_namespace = %placement.namespace,
        policy = running_policy.as_str(),
        "worker reconciliation: pods stale (image and/or namespace)"
    );

    // Gate the kill on the doomed pods' running work per the policy.
    use crate::infra_lifecycle_command::RunningPolicy;
    match running_policy {
        RunningPolicy::Wait => {
            // DRAIN via the existing worker-draining mechanism (the
            // same one worker scale-down uses): a draining pod stops
            // being chosen for NEW work (`pick_admittable_for_project`
            // and cold_start both skip it; anything new spawns a fresh
            // CORRECT pod via the resolver), finishes its in-flight
            // executions, and idle-exits. We wait for the doomed pods
            // to empty out through THE shared drain loop, capped at
            // the caller's `drain_timeout_secs`; stragglers past the
            // cap are killed (their executions die with them, loudly).
            for name in &doomed_names {
                weft_task_store::worker_pod::set_draining(&state.pg_pool, name)
                    .await
                    .map_err(internal)?;
            }
            let clock = weft_platform_traits::SystemClock;
            let outcome = weft_platform_traits::drain_until_zero(
                &clock,
                std::time::Duration::from_secs(drain_timeout_secs),
                "worker replacement",
                || async {
                    weft_task_store::worker_pod::count_alive_named(
                        &state.pg_pool,
                        &doomed_names,
                    )
                    .await
                    .map_err(internal)
                },
            )
            .await?;
            if let weft_platform_traits::DrainOutcome::TimedOut { still_running } = outcome {
                tracing::warn!(
                    target: "weft_dispatcher::api::project",
                    project_id,
                    stragglers = still_running,
                    drain_timeout_secs,
                    "runningPolicy=wait drain cap reached; killing the remaining stale \
                     workers (their in-flight executions die with them)"
                );
            }
        }
        RunningPolicy::Cancel => {
            // Scoped to the DOOMED pods' own executions: a mixed fleet
            // (fresh pods next to stale ones) keeps its healthy work.
            cancel_running_non_suspended(state, project_id, Some(&doomed_names)).await?;
        }
    }

    // Kill whatever of the doomed set is still alive (all of it under
    // Cancel; only the past-cap stragglers under Wait). mark_dead
    // FIRST so the journal-fencing trigger blocks any late write from
    // a doomed worker; kubectl delete second. The `spawn_pod` task
    // executor is intentionally narrow ("spawn a pod when none is
    // alive") and never kills, so the kill lives here with the
    // decision.
    for (pod_name, namespace, _) in &doomed {
        weft_task_store::worker_pod::mark_dead(&state.pg_pool, pod_name)
            .await
            .map_err(internal)?;
        state
            .workers
            .kill_pod(pod_name.clone(), namespace.clone())
            .await
            .map_err(|e| {
                internal(anyhow::anyhow!(
                    "kill_pod {pod_name} failed (stale worker would survive spawn): {e}"
                ))
            })?;
    }

    // Enqueue ONE SpawnPod task for a fresh worker in the resolver's
    // namespace (parallel load re-scales via cold_start on demand).
    // Dedup key matches cold_start's so a concurrent sweep collapses
    // on us.
    let dedup = format!("{project_id}:spawn");
    let payload = serde_json::json!({
        "project_id": project_id,
        "tenant": placement.tenant.as_str(),
        "namespace": placement.namespace,
        "owner_dispatcher": state.pod_id.as_str(),
    });
    weft_task_store::tasks::enqueue_dedup(
        &state.pg_pool,
        weft_task_store::tasks::NewTask {
            kind: weft_task_store::TaskKind::SpawnPod.into(),
            target: weft_task_store::tasks::TaskTarget::Dispatcher,
            project_id: Some(project_id.to_string()),
            dedup_key: Some(dedup),
            color: None,
            tenant_id: Some(placement.tenant.as_str().to_string()),
            target_pod_name: None,
            binary_hash: None,
            payload,
        },
    )
    .await
    .map_err(internal)?;

    // Wait for a correct replacement to register itself alive. Bounded
    // so a wedged image build doesn't block the verb indefinitely.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    let mut ready = false;
    while std::time::Instant::now() < deadline {
        let now_alive = weft_task_store::worker_pod::alive_pods_for_project_full(
            &state.pg_pool,
            project_id,
        )
        .await
        .map_err(internal)?;
        if now_alive.iter().any(|(p, ns, h)| {
            !doomed_names.contains(p) && *h == want_hash && *ns == placement.namespace
        }) {
            ready = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    if !ready {
        tracing::warn!(
            target: "weft_dispatcher::api::project",
            project_id,
            "replacement worker did not come up within 60s; \
             cold_start will retry as soon as a worker task lands"
        );
    }
    Ok(())
}

/// Shared deactivation logic. Used both by the explicit
/// `/deactivate` endpoint and auto-called from `infra stop` /
/// `infra terminate` / project removal: a stopped infra leaves
/// the project's triggers pointing at a dead endpoint, so we
/// always wipe in those flows. Returns true if the project existed.
///
/// Always wipes (preservationMode=wipe, runningPolicy=cancel).
/// User-initiated deactivates use the parameterized variant via
/// the API handler.
pub async fn deactivate_project(
    state: &DispatcherState,
    id: uuid::Uuid,
) -> Result<bool, (StatusCode, String)> {
    deactivate_project_with_mode(
        state,
        id,
        DeactivationMode::Wipe,
        0,
        crate::infra_lifecycle_command::RunningPolicy::Cancel,
        // Cancel never drains; the cap is inert.
        weft_broker_client::protocol::DEFAULT_DRAIN_TIMEOUT_SECS,
        false, // project teardown (delete / rm), not a health park
    )
    .await
}

/// Mode-aware deactivation.
///
/// `mode` ∈ {wipe, hibernate, park}: which lifecycle target the
/// project lands in. The target's accepting/visible/deadline axes
/// are written to the row before we look at running executions, so
/// new fires arriving during the deactivation already obey the
/// target gate behavior.
///
/// `running_policy` ∈ {cancel, wait}:
///   - `cancel`: cancel running (non-suspended) executions
///               immediately, then flip status straight to Inactive.
///   - `wait`:   leave running executions to drain. Status is set
///               to Deactivating; the journal-bridge CASes it to
///               Inactive once `running_count = 0`: see
///               `journal_bridge::terminal_cleanup`. Hibernate /
///               park + wait leave suspended executions alone.
///
/// `wait` is only ever paired with hibernate / park. Wipe is always
/// paired with cancel (the only producers are the supervisor's
/// WipeTriggers action and the internal `deactivate_project`, both
/// hardcoding cancel), and `DeactivateSpec::validate` rejects
/// wipe+wait at every boundary, so that combination never reaches
/// here.
pub async fn deactivate_project_with_mode(
    state: &DispatcherState,
    id: uuid::Uuid,
    mode: DeactivationMode,
    grace_minutes: u32,
    running_policy: crate::infra_lifecycle_command::RunningPolicy,
    drain_timeout_secs: u64,
    by_health: bool,
) -> Result<bool, (StatusCode, String)> {
    use crate::project_store::{ProjectLifecycle, ProjectStatus, ProjectTransition};

    let project_id = id.to_string();

    // Reject BEFORE any destructive step (the signal wipe below runs
    // ahead of the status write): a project mid-activation is
    // cancelled via cancel-activate, not deactivated over; a project
    // mid-build finishes or is build-cancelled first. The guarded
    // `set_lifecycle_guarded` at the end re-checks atomically, so a
    // verb racing past this pre-check still cannot land the write.
    let current = state
        .projects
        .lifecycle(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("lifecycle: {e}")))?;
    let Some(current) = current else {
        return Ok(false);
    };
    if current.status == ProjectStatus::Activating {
        return Err((
            StatusCode::CONFLICT,
            "cannot deactivate: project is activating; cancel the activation first".into(),
        ));
    }
    let transition = state
        .projects
        .transition(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("transition: {e}")))?
        .unwrap_or(ProjectTransition::None);
    if transition.is_building() {
        return Err((
            StatusCode::CONFLICT,
            format!(
                "cannot deactivate: project is {}; cancel the build first",
                transition.as_str()
            ),
        ));
    }

    // Resolve the target lifecycle (the axes the gate must be
    // showing on completion). For wait mode we wrap it in
    // `deactivating_to(target)` so status=Deactivating but the
    // gate axes are already the target's. `by_health` stamps WHO
    // deactivated: true only on the health loop's autonomous park,
    // so its auto-recover can later reactivate ONLY its own park (a
    // user deactivate sets false and is never auto-reactivated).
    let target = ProjectLifecycle {
        deactivated_by_health: by_health,
        ..match mode {
            DeactivationMode::Wipe => ProjectLifecycle::wiped(),
            DeactivationMode::Hibernate => {
                let deadline = crate::lease::now_unix() + (grace_minutes as i64) * 60;
                ProjectLifecycle::hibernating(deadline)
            }
            DeactivationMode::Park => ProjectLifecycle::parked(),
        }
    };

    // For wipe (always paired with cancel), do the cancel + drop
    // FIRST while the listener is still alive on the row; THEN
    // flip the lifecycle to wiped. The reaper would otherwise
    // observe accepting=false and start killing the listener
    // mid-cleanup.
    if mode == DeactivationMode::Wipe {
        wipe_project_signals(state, &project_id).await?;
    } else {
        // hibernate / park: KEEP the DB signal rows (the parking
        // gate needs them to recognise incoming fires) but
        // unregister EVERY signal from the listener's in-RAM
        // registry, entries and resumes alike. The DB is the
        // canonical source; reactivate calls listener /rehydrate
        // after TriggerSetup, which re-inserts every signal that's
        // not already in the registry. Symmetric: deactivate
        // clears the cache, reactivate restores it from DB.
        let signals = state
            .journal
            .signal_list_for_project(&project_id)
            .await
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("signal_list_for_project: {e}"),
                )
            })?;
        if !signals.is_empty() {
            state
                .listeners
                .unregister_many(&state.pg_pool, &signals)
                .await;
        }
    }

    // For preservation modes (hibernate / park), cancel running
    // non-suspended executions immediately when the user picked
    // `cancel`. Suspended executions stay alive across the
    // deactivate. With `wait`, we set status=Deactivating instead
    // and let the drain-watcher flip status to Inactive once the
    // running set empties. Wipe never reaches the wait branch:
    // upstream rejects (mode=wipe, running_policy=wait).
    use crate::infra_lifecycle_command::RunningPolicy;
    let lifecycle_to_set = if running_policy == RunningPolicy::Wait {
        // The user's drain cap ("wait at most N, then proceed", same
        // semantics as the infra drains): past the deadline the
        // stuck-transition reaper cancels the remaining executions
        // (the SAME cancel every path uses) and the drain-watcher
        // lands the row (the SAME landing CAS).
        ProjectLifecycle {
            drain_deadline_unix: Some(
                crate::lease::now_unix() + drain_timeout_secs as i64,
            ),
            ..ProjectLifecycle::deactivating_to(target)
        }
    } else if mode != DeactivationMode::Wipe {
        cancel_running_non_suspended(state, &project_id, None).await?;
        target
    } else {
        // wipe + cancel: rows + executions already gone above.
        target
    };

    // Guarded write: refused while the project is Activating (cancel
    // the activation instead; deactivating over an in-flight
    // trigger-setup would strand its color) or while a build is in
    // flight (cancel the build first). Re-deactivating a Deactivating
    // project (give-up-on-wait with runningPolicy=cancel) stays legal.
    let existed = match state
        .projects
        .set_lifecycle_guarded(id, &lifecycle_to_set)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("set_lifecycle_guarded: {e}")))?
    {
        crate::project_store::LifecycleWrite::Applied => true,
        crate::project_store::LifecycleWrite::NotFound => false,
        crate::project_store::LifecycleWrite::Rejected { status, transition } => {
            let blocker = if transition.is_building() {
                transition.as_str()
            } else {
                status.as_str()
            };
            return Err((
                StatusCode::CONFLICT,
                format!("cannot deactivate: project is {blocker}; cancel it first"),
            ));
        }
    };
    if existed {
        state
            .events
            .publish(DispatcherEvent::ProjectDeactivated {
                project_id: project_id.clone(),
            })
            .await;
        // Wait mode: if there's actually nothing running, fast-path
        // the CAS to Inactive so the user doesn't see a transient
        // Deactivating that's already done.
        let running_now = running_count(state, &project_id, None)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("running_count: {e}")))?;
        if running_policy == RunningPolicy::Wait
            && lifecycle_to_set.status == ProjectStatus::Deactivating
            && running_now == 0
        {
            // Fast-path CAS: if this returns false (raced something
            // else) or errors, the bridge's drain-watcher will
            // perform the same transition on its next tick.
            // Either way no caller action; log on error.
            match state
                .projects
                .cas_status(id, ProjectStatus::Deactivating, ProjectStatus::Inactive)
                .await
            {
                Ok(true) => {
                    crate::transition::publish_transition_changed(state, id).await;
                }
                Ok(false) => {}
                Err(e) => {
                    tracing::warn!(
                        target: "weft_dispatcher::api::project",
                        project_id = %project_id,
                        error = %e,
                        "fast-path cas_status(deactivating -> inactive) failed; \
                         drain-watcher will retry"
                    );
                }
            }
        }
    }
    Ok(existed)
}

/// `POST /projects/{id}/cancel-running`. Force the drain to finish
/// when the project is in `deactivating`: cancel every running,
/// non-suspended execution. The lifecycle target the original
/// deactivate already wrote stays in place; the journal-bridge
/// drain-watcher flips status to Inactive once the running set
/// empties. No-op when the project isn't in `deactivating` (the
/// drain watcher only fires from that state).
pub async fn cancel_running(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id_str): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    let id = id_str
        .parse::<uuid::Uuid>()
        .map_err(|_| (StatusCode::BAD_REQUEST, "bad id".into()))?;
    authorize_project(&state, &caller.0, id).await?;
    let project_id = id_str;
    cancel_running_non_suspended(&state, &project_id, None).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `POST /projects/{id}/cancel-build`. Cancel the in-flight build
/// transition: CAS `transition` building → cancelling_build (the
/// durable, cross-Pod cancel signal the driving pod's build gate
/// polls), then best-effort interrupt the builder job locally so the
/// wait shortens when the cancel lands on the driving pod itself.
/// Cancel reconciles, never asserts: the response is 202 and the
/// displayed state is whatever the backend reports next (the build
/// may still complete if it beat the cancel).
///
/// 412 when no build is in flight (stale tab; the client refetches
/// `/status` and reconciles).
pub async fn cancel_build(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id_str): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    let id = id_str
        .parse::<uuid::Uuid>()
        .map_err(|_| (StatusCode::BAD_REQUEST, "bad id".into()))?;
    authorize_project(&state, &caller.0, id).await?;
    let flipped = state
        .projects
        .request_cancel_build(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("request_cancel_build: {e}")))?;
    if !flipped {
        return Err((
            StatusCode::PRECONDITION_FAILED,
            "no build in flight to cancel".into(),
        ));
    }
    crate::transition::publish_transition_changed(&state, id).await;
    if let Some(builder) = &state.ensure_built {
        // Local shortcut only; a failure here is not a failed cancel
        // (the driving pod's gate poll picks the durable signal up).
        if let Err(e) = builder.cancel_build(id).await {
            tracing::warn!(
                target: "weft_dispatcher::api::project",
                project_id = %id, error = %e,
                "local builder interrupt failed; the driving pod's gate poll \
                 will pick up the cancel"
            );
        }
    }
    Ok(StatusCode::ACCEPTED)
}

/// Cancel an in-flight `activate` (status=Activating). CAS-flips status
/// Activating → Inactive, cancels the setup run the row recorded,
/// sweeps setup runs left by older activations, and wipes every signal
/// row registered so far.
///
/// 412 if status isn't Activating: the user (or stale UI) clicked
/// cancel against an already-active or already-inactive project.
pub async fn cancel_activate(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id_str): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    use crate::project_store::ProjectStatus;
    let id = id_str
        .parse::<uuid::Uuid>()
        .map_err(|_| (StatusCode::BAD_REQUEST, "bad id".into()))?;
    authorize_project(&state, &caller.0, id).await?;
    let project_id = id_str;
    let lifecycle = state
        .projects
        .lifecycle(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("lifecycle: {e}")))?
        .ok_or((StatusCode::NOT_FOUND, "project not found".into()))?;
    if lifecycle.status != ProjectStatus::Activating {
        return Err((
            StatusCode::PRECONDITION_FAILED,
            format!(
                "cancel-activate requires status=activating, got {}",
                lifecycle.status.as_str()
            ),
        ));
    }
    // The in-flight setup run's color comes off the project row inside
    // the helper. The cause is the person's: they pressed Cancel.
    wipe_activating_state(&state, id, &project_id, &weft_core::exec::CancelCause::User).await?;
    state
        .events
        .publish(DispatcherEvent::ProjectDeactivated { project_id })
        .await;
    Ok(StatusCode::NO_CONTENT)
}

/// Cancel every running execution that isn't currently suspended.
/// Used by deactivate-with-runningPolicy=cancel (non-wipe) and by
/// the user-initiated "force cancel during deactivating" path.
/// Settled (completed/failed/cancelled) executions skip; suspended
/// ones (color appears as is_resume in the signal table) skip.
/// `owned_by`: when set, only cancel executions whose color is OWNED by
/// one of the named pods. A multi-worker project's fleet is routinely
/// MIXED (fresh pods next to doomed ones mid-replacement), and a
/// pod-scoped cancel (reconcile's Cancel policy) must not kill work
/// running fine on the fresh pods. `None` = the whole project (a
/// deactivation, a resync, the drain give-up verb).
pub(crate) async fn cancel_running_non_suspended(
    state: &DispatcherState,
    project_id: &str,
    owned_by: Option<&[String]>,
) -> Result<(), (StatusCode, String)> {
    let suspended_colors = suspended_color_set(state, project_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("suspended_color_set: {e}")))?;
    let owned: Option<std::collections::HashSet<weft_core::Color>> = match owned_by {
        None => None,
        Some(pods) => {
            let rows: Vec<(String,)> = sqlx::query_as(
                "SELECT color FROM execution_color \
                 WHERE project_id = $1 AND owner_pod_name = ANY($2)",
            )
            .bind(project_id)
            .bind(pods)
            .fetch_all(&state.pg_pool)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("owned colors: {e}")))?;
            Some(rows.into_iter().filter_map(|(c,)| c.parse().ok()).collect())
        }
    };
    let colors = state
        .journal
        .list_non_terminal_colors_for_project(project_id, None)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("list colors: {e}")))?;
    // A pod-scoped sweep is the runtime replacing a sick pod; a
    // whole-project sweep is a verb a person invoked.
    let cause = match owned_by {
        Some(_) => weft_core::exec::CancelCause::Runtime {
            detail: "the worker pod hosting this run was replaced by health recovery".into(),
        },
        None => weft_core::exec::CancelCause::User,
    };
    let targets: Vec<(weft_core::Color, &weft_core::exec::CancelCause)> = colors
        .iter()
        .filter(|color| !suspended_colors.contains(color))
        // Un-owned colors (a task not yet claimed) are also skipped on a
        // pod-scoped sweep: their hash-stamped task can only ever be
        // claimed by a correct-image pod, so they are not the doomed
        // pods' work.
        .filter(|color| owned.as_ref().is_none_or(|o| o.contains(color)))
        .map(|c| (*c, &cause))
        .collect();
    // Every run is attempted before any error is reported, so one
    // failing cancel never leaves the runs after it live.
    crate::api::execution::cancel_colors(state, &targets)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("cancel: {e}")))?;
    Ok(())
}

/// Set of colors holding at least one resume signal: the canonical
/// "this execution is suspended" record (the engine doesn't journal
/// a terminal event for stalls).
pub(crate) async fn suspended_color_set(
    state: &DispatcherState,
    project_id: &str,
) -> anyhow::Result<std::collections::HashSet<weft_core::Color>> {
    let signals = state.journal.signal_list_for_project(project_id).await?;
    Ok(signals
        .into_iter()
        .filter(|s| s.is_resume)
        .filter_map(|s| s.color)
        .collect())
}

/// Count how many non-settled non-suspended executions a project
/// has right now, PLUS in-flight task rows that are about to become
/// one. `0` means deactivate-with-wait can flip status to Inactive
/// immediately.
///
/// The task rows matter for the lifecycle CAS: a `route_entry` task
/// is a fire that passed the gate but has not journaled
/// `ExecutionStarted` yet, and a pending `resume` task belongs to a
/// color the suspended-set still excludes. Counting only the
/// journal would let the CAS flip a project Inactive while such a
/// fire is mid-route. Colors are unioned (a journaled color with a
/// live execute task counts once); colorless `route_entry` rows add
/// one each (each will mint a distinct color).
///
/// `exclude_task`: discount one still-claimed task row; see
/// `journal_bridge::try_finish_drain`.
pub(crate) async fn running_count(
    state: &DispatcherState,
    project_id: &str,
    exclude_task: Option<uuid::Uuid>,
) -> anyhow::Result<usize> {
    let suspended_colors = suspended_color_set(state, project_id).await?;
    let colors = state
        .journal
        .list_non_terminal_colors_for_project(project_id, None)
        .await?;
    // Colors the journal already records as finished. A task row must
    // NEVER resurrect one of these: a completed/failed/cancelled
    // execution is not "running" even if a stray `pending`/`claimed`
    // task for its color lingers (an orphaned task is a separate
    // concern, not a live execution).
    let terminal_colors = state
        .journal
        .list_terminal_colors_for_project(project_id)
        .await?;
    let mut running: std::collections::HashSet<weft_core::Color> = colors
        .into_iter()
        .filter(|c| !suspended_colors.contains(c))
        .collect();
    let task_rows: Vec<(uuid::Uuid, Option<String>)> = sqlx::query_as(
        "SELECT id, color FROM task \
         WHERE project_id = $1 \
           AND kind IN ('route_entry', 'execute', 'resume') \
           AND status IN ('pending', 'claimed')",
    )
    .bind(project_id)
    .fetch_all(&state.pg_pool)
    .await?;
    let mut colorless = 0usize;
    for (task_id, color) in task_rows {
        if Some(task_id) == exclude_task {
            continue;
        }
        match color {
            Some(c) => {
                let parsed: weft_core::Color = c
                    .parse()
                    .map_err(|e| anyhow::anyhow!("corrupt task.color '{c}': {e}"))?;
                // Skip a task whose color is journal-terminal (finished)
                // or suspended: neither is a running execution.
                if terminal_colors.contains(&parsed) || suspended_colors.contains(&parsed) {
                    continue;
                }
                running.insert(parsed);
            }
            None => colorless += 1,
        }
    }
    Ok(running.len() + colorless)
}

/// Spawn a worker for the TriggerSetup sub-execution and block
/// until it settles. The run's color is recorded on the project row
/// before it starts, so a rollback (or a cancel-activate, or the
/// reaper) cancels just THIS execution and never touches
/// suspended/running work from prior cycles.
async fn run_trigger_setup(
    state: &DispatcherState,
    project_id_uuid: uuid::Uuid,
    kicks: Vec<Kick>,
    // The hash of the SAME definition the caller computed `kicks`
    // from (one `coherent_definition` pair). Reading the project
    // row's hash here instead would race a concurrent re-register:
    // kicks from shape A journaled under hash B.
    definition_hash: &str,
) -> Result<(), (StatusCode, String)> {
    let project_id = project_id_uuid.to_string();
    let color = uuid::Uuid::new_v4();

    // Subscribe BEFORE journaling+enqueueing so the worker can't beat us to
    // the completion event.
    let mut events = state.events.subscribe_project(&project_id).await;

    // Record the color on the project row BEFORE the run exists, so an
    // activation that dies between the two leaves nothing unaccounted
    // for (a recorded color with no run is cancelled as a no-op, and
    // the re-check after the start below covers the run that arrives
    // later). The write is guarded on the status: a refusal means
    // cancel-activate or the reaper already ended this activation
    // under us, and the run must not start.
    let recorded = state
        .projects
        .set_activating_ts_color(project_id_uuid, color)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("record ts color: {e}")))?;
    if !recorded {
        return Err((
            StatusCode::CONFLICT,
            "the activation was cancelled before trigger setup started".into(),
        ));
    }

    // The birth is one transaction, so a start failure leaves no journal
    // rows and no task; the color the row recorded above then names a
    // run that never existed, which the rollback's cancel finds ownerless
    // and skips.
    start_queued_execution(
        state,
        color,
        &project_id,
        weft_core::context::Phase::TriggerSetup,
        &kicks[0].node_id,
        &kicks,
        definition_hash,
        None,
    )
    .await?;

    // The birth has no status guard of its own, so re-check AFTER it:
    // if the activation ended between the record above and the start
    // (cancel-activate or the reaper un-stuck the row and found only a
    // color with no run to cancel), the run now exists and is ours to
    // end. Whichever side saw the run last cancels it; both cancels are
    // idempotent, so a run never survives its activation.
    let still_ours = state
        .projects
        .lifecycle(project_id_uuid)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("lifecycle: {e}")))?
        .is_some_and(|l| {
            l.status == crate::project_store::ProjectStatus::Activating
                && l.activating_ts_color == Some(color)
        });
    if !still_ours {
        crate::api::execution::cancel_color(
            state,
            color,
            &weft_core::exec::CancelCause::Runtime {
                detail: "the activation ended before this trigger setup run started".into(),
            },
        )
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("cancel_color: {e}")))?;
        return Err((
            StatusCode::CONFLICT,
            "the activation was cancelled as trigger setup started".into(),
        ));
    }

    // No backend-imposed deadline. Trigger setup spans worker pod
    // spawn + image pull + fold + run + bridge wakeup; on a cold
    // cluster the legitimate path is just slow, and a hard timeout
    // would surface that as a 504 even though the work is still in
    // flight. The CLI / extension is the right layer to choose a
    // client-side patience budget.
    loop {
        match events.recv().await.map(|record| record.event) {
            Ok(crate::events::DispatcherEvent::ExecutionCompleted { color: c, .. })
                if c == color =>
            {
                return Ok(());
            }
            Ok(crate::events::DispatcherEvent::ExecutionFailed { color: c, error, .. })
                if c == color =>
            {
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("trigger setup failed: {error}"),
                ));
            }
            Ok(crate::events::DispatcherEvent::ExecutionCancelled { color: c, reason, .. })
                if c == color =>
            {
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("trigger setup cancelled: {reason}"),
                ));
            }
            Ok(_) => continue,
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                // Dropped batch may have held this color's terminal.
                // The journal is authoritative: re-query rather than
                // fail the trigger-setup on a transient lag.
                match crate::api::execution::terminal_outcome(&state.pg_pool, color).await {
                    Ok(Some(crate::api::execution::TerminalOutcome::Completed)) => return Ok(()),
                    Ok(Some(crate::api::execution::TerminalOutcome::Failed)) => {
                        return Err((
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "trigger setup failed".into(),
                        ))
                    }
                    Ok(Some(crate::api::execution::TerminalOutcome::Cancelled)) => {
                        return Err((
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "trigger setup cancelled".into(),
                        ))
                    }
                    Ok(None) => continue, // still in flight; keep waiting
                    Err(e) => {
                        return Err((
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("trigger setup terminal lookup: {e}"),
                        ))
                    }
                }
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "trigger setup event stream closed".into(),
                ));
            }
        }
    }
}

/// After a trigger-setup sub-exec, collect every persisted signal
/// for the project that has a user-facing URL. These become the
/// `urls` in ActivateResponse.
async fn collect_listener_urls(
    state: &DispatcherState,
    project_id: &str,
) -> anyhow::Result<Vec<ActivationUrl>> {
    let signals = state.journal.signal_list_for_project(project_id).await?;
    let mut out = Vec::new();
    for meta in signals {
        if meta.is_resume {
            continue;
        }
        if let Some(url) = meta.public_url(state.external_base_url()) {
            out.push(ActivationUrl {
                node_id: meta.node_id.clone(),
                url,
            });
        }
    }
    Ok(out)
}


// `crate::lease::now_unix` is the canonical wall-clock reader.

#[cfg(test)]
mod trigger_kick_tests {
    use super::*;
    use weft_core::project::{GroupBoundary, GroupBoundaryRole};

    /// Build a minimal ProjectDefinition from a JSON spec. Tests
    /// only care about node id, trigger status, the scope a node sits
    /// in, and edges; everything else is defaulted. The third element
    /// is the node's scope chain (empty at the top level).
    fn project(nodes: &[(&str, bool, &[&str])], edges: &[(&str, &str)]) -> ProjectDefinition {
        let mut n_json = Vec::new();
        for (id, is_trigger, scope) in nodes {
            n_json.push(serde_json::json!({
                "id": id,
                "nodeType": "T",
                "label": null,
                "config": {},
                "position": { "x": 0, "y": 0 },
                "scope": scope,
                "features": {
                    "isTrigger": is_trigger,
                },
            }));
        }
        let e_json: Vec<Value> = edges
            .iter()
            .map(|(s, t)| {
                serde_json::json!({
                    "id": format!("e_{}_{}", s, t),
                    "source": s,
                    "sourcePort": "out",
                    "target": t,
                    "targetPort": "in"
                })
            })
            .collect();
        let body = serde_json::json!({
            "id": uuid::Uuid::new_v4(),
            "nodes": n_json,
            "edges": e_json,
            "groups": []
        });
        serde_json::from_value(body).expect("valid test project")
    }

    fn ids(kicks: &[Kick]) -> Vec<String> {
        let mut v: Vec<String> = kicks.iter().map(|k| k.node_id.clone()).collect();
        v.sort();
        v
    }

    fn sorted(set: &HashSet<String>) -> Vec<String> {
        let mut v: Vec<String> = set.iter().cloned().collect();
        v.sort();
        v
    }

    fn fire(p: &ProjectDefinition, node: &str, payload: Value) -> TriggerFire {
        compute_trigger_fire(p, node, &payload, None).expect("the fire reaches an output")
    }

    #[test]
    fn trigger_only_upstream_node_is_skipped() {
        // A ──► TriggerX ──► B ──► Out
        // A feeds only the trigger; nothing past the trigger reaches Out
        // through A, so A must not run at fire time.
        let p = project(
            &[
                ("a", false, &[]),
                ("trigger_x", true, &[]),
                ("b", false, &[]),
                ("out", false, &[]),
            ],
            &[("a", "trigger_x"), ("trigger_x", "b"), ("b", "out")],
        );
        let TriggerFire { kicks, subgraph } = fire(&p, "trigger_x", Value::String("payload".into()));
        assert_eq!(
            ids(&kicks),
            vec!["trigger_x".to_string()],
            "only the firing trigger should be a kick"
        );
        assert_eq!(kicks[0].payload, Some(Value::String("payload".into())));
        assert_eq!(
            sorted(&subgraph),
            vec!["b".to_string(), "out".to_string(), "trigger_x".to_string()],
            "A feeds only the trigger, so it is outside the fire's subgraph"
        );
    }

    #[test]
    fn node_shared_with_non_trigger_path_runs() {
        //        ┌──► TriggerX ──► C ──► Out
        //  A ────┤
        //        └──► B ───────────────► Out
        // A feeds both the trigger AND B. B → Out is a non-trigger
        // path. A must run at fire time (via the B path).
        let p = project(
            &[
                ("a", false, &[]),
                ("trigger_x", true, &[]),
                ("b", false, &[]),
                ("c", false, &[]),
                ("out", false, &[]),
            ],
            &[
                ("a", "trigger_x"),
                ("a", "b"),
                ("trigger_x", "c"),
                ("c", "out"),
                ("b", "out"),
            ],
        );
        let TriggerFire { kicks, subgraph } = fire(&p, "trigger_x", Value::String("payload".into()));
        assert_eq!(
            ids(&kicks),
            vec!["a".to_string(), "trigger_x".to_string()],
            "A must run via its non-trigger path; trigger carries payload"
        );
        assert_eq!(sorted(&subgraph), vec!["a", "b", "c", "out", "trigger_x"]);
        for k in &kicks {
            if k.node_id == "trigger_x" {
                assert_eq!(k.payload, Some(Value::String("payload".into())));
            } else {
                assert_eq!(k.payload, None);
            }
        }
    }

    #[test]
    fn non_firing_triggers_in_subgraph_get_no_payload() {
        // TriggerX ──► Out ◄── TriggerY
        // Firing TriggerX: TriggerY still gets kicked (without payload)
        // because it's reachable upstream from Out.
        let p = project(
            &[
                ("trigger_x", true, &[]),
                ("trigger_y", true, &[]),
                ("out", false, &[]),
            ],
            &[("trigger_x", "out"), ("trigger_y", "out")],
        );
        let TriggerFire { kicks, subgraph } = fire(&p, "trigger_x", Value::String("fire".into()));
        assert_eq!(ids(&kicks), vec!["trigger_x".to_string(), "trigger_y".to_string()]);
        assert_eq!(sorted(&subgraph), vec!["out", "trigger_x", "trigger_y"]);
        for k in &kicks {
            if k.node_id == "trigger_x" {
                assert_eq!(k.payload, Some(Value::String("fire".into())));
            } else {
                assert_eq!(k.payload, None);
            }
        }
    }

    #[test]
    fn everything_the_trigger_reaches_runs() {
        // TriggerX ──► DeadEnd: nothing declares itself a deliverable
        // any more; whatever the fire reaches runs.
        let p = project(
            &[("trigger_x", true, &[]), ("dead_end", false, &[])],
            &[("trigger_x", "dead_end")],
        );
        let TriggerFire { kicks, subgraph } = fire(&p, "trigger_x", Value::Null);
        assert_eq!(ids(&kicks), vec!["trigger_x".to_string()]);
        assert_eq!(sorted(&subgraph), vec!["dead_end", "trigger_x"]);
    }

    #[test]
    fn a_manual_run_kicks_no_root_inside_a_scope() {
        // g__in (nothing feeds it) ──► g.a ──► g__out ──► out
        // g.seed (no wire feeds it, inside g)
        // top (no wire feeds it, outside every scope)
        // The untargeted run kicks the top-level roots, the group's In
        // boundary among them; the body's seed is the scope launcher's
        // to start, so it is never a kick. An aimed run at a member
        // brings the whole scope and kicks the same boundary.
        let mut p = project(
            &[
                ("top", false, &[]),
                ("g__in", false, &[]),
                ("g.a", false, &["g"]),
                ("g.seed", false, &["g"]),
                ("g__out", false, &[]),
                ("out", false, &[]),
            ],
            &[("g__in", "g.a"), ("g.a", "g__out"), ("g__out", "out")],
        );
        // The boundaries carry their role, the way the compiler
        // flattens them: that is what closes a member over its scope.
        for (id, role) in [("g__in", GroupBoundaryRole::In), ("g__out", GroupBoundaryRole::Out)] {
            let node = p.nodes.iter_mut().find(|n| n.id == id).expect(id);
            node.group_boundary = Some(GroupBoundary { group_id: "g".into(), role });
        }
        let kicks = RunSubgraph::whole(&p).root_kicks(&p, &Value::Null, None);
        assert_eq!(ids(&kicks), vec!["g__in".to_string(), "top".to_string()]);

        let aimed = RunSubgraph::of(&p, &["g.a".to_string()]);
        assert_eq!(sorted(&aimed.nodes), vec!["g.a", "g.seed", "g__in", "g__out"], "a member brings its scope");
        let kicks = aimed.root_kicks(&p, &Value::Null, None);
        assert_eq!(ids(&kicks), vec!["g__in".to_string()]);
    }

    /// A trigger sitting in the middle of a group fires there: it is
    /// the run's one kick, the group comes whole around it (the sibling
    /// its output feeds, and the sibling's other input's source), and
    /// nothing outside the group is kicked to reach it.
    #[test]
    fn a_trigger_in_the_middle_of_a_group_starts_the_run_from_itself() {
        // cfg ──► g__in ──► g.a ──► g.join ──► g__out ──► out
        //                   g.trig ─────────┘
        let mut p = project(
            &[
                ("cfg", false, &[]),
                ("g__in", false, &[]),
                ("g.a", false, &["g"]),
                ("g.trig", true, &["g"]),
                ("g.join", false, &["g"]),
                ("g__out", false, &[]),
                ("out", false, &[]),
            ],
            &[
                ("cfg", "g__in"),
                ("g__in", "g.a"),
                ("g.a", "g.join"),
                ("g.trig", "g.join"),
                ("g.join", "g__out"),
                ("g__out", "out"),
            ],
        );
        for (id, role) in [("g__in", GroupBoundaryRole::In), ("g__out", GroupBoundaryRole::Out)] {
            let node = p.nodes.iter_mut().find(|n| n.id == id).expect(id);
            node.group_boundary = Some(GroupBoundary { group_id: "g".into(), role });
        }
        let TriggerFire { kicks, subgraph } = fire(&p, "g.trig", serde_json::json!({ "hi": 1 }));
        assert_eq!(
            sorted(&subgraph),
            vec!["cfg", "g.a", "g.join", "g.trig", "g__in", "g__out", "out"],
            "the run is the trigger's program: its group whole, and what feeds the group"
        );
        let firing: Vec<&str> = kicks.iter().filter(|k| k.firing).map(|k| k.node_id.as_str()).collect();
        assert_eq!(firing, vec!["g.trig"], "the trigger is the one firing kick");
        assert_eq!(
            ids(&kicks),
            vec!["cfg".to_string(), "g.trig".to_string()],
            "the group's input source is kicked so g.join's other side arrives; the group's \
             members start when the group does"
        );
    }

    #[test]
    fn a_scope_the_fire_touches_comes_whole() {
        // TriggerX ──► g__in ──► g.a ──► g__out ──► Out
        //              g.seed (no wire feeds it; the scope launcher's)
        // The fire reaches the group's In boundary; the whole body is
        // in the program then, the unwired seed included, and the seed
        // is never a kick of the run (its scope kicks it when it starts).
        let p = project(
            &[
                ("trigger_x", true, &[]),
                ("g__in", false, &[]),
                ("g.a", false, &["g"]),
                ("g.seed", false, &["g"]),
                ("g__out", false, &[]),
                ("out", false, &[]),
            ],
            &[("trigger_x", "g__in"), ("g__in", "g.a"), ("g.a", "g__out"), ("g__out", "out")],
        );
        let TriggerFire { kicks, subgraph } = fire(&p, "trigger_x", Value::Null);
        assert_eq!(ids(&kicks), vec!["trigger_x".to_string()], "the body's seed is not the run's root");
        assert_eq!(sorted(&subgraph), vec!["g.a", "g.seed", "g__in", "g__out", "out", "trigger_x"]);
    }

    #[test]
    fn setup_phases_reach_a_trigger_or_infra_node_inside_a_group() {
        // cfg ──► g__in ──► g.db (infra, unwired inside the body)
        //                   g.trig (trigger, unwired inside the body)
        // Both setup phases seed through the scope: the kick is the
        // group's In boundary (and the Out boundary, which nothing
        // feeds), and the source of the group's input runs first.
        let mut p = project(
            &[
                ("cfg", false, &[]),
                ("g__in", false, &[]),
                ("g.db", false, &["g"]),
                ("g.trig", true, &["g"]),
                ("g__out", false, &[]),
            ],
            &[("cfg", "g__in")],
        );
        for (id, role) in [("g__in", GroupBoundaryRole::In), ("g__out", GroupBoundaryRole::Out)] {
            let node = p.nodes.iter_mut().find(|n| n.id == id).expect(id);
            node.group_boundary = Some(GroupBoundary { group_id: "g".into(), role });
        }
        p.nodes.iter_mut().find(|n| n.id == "g.db").unwrap().requires_infra = true;
        assert_eq!(ids(&compute_infra_setup_kicks(&p)), vec!["cfg".to_string(), "g__out".to_string()]);
        assert_eq!(ids(&compute_trigger_setup_kicks(&p)), vec!["cfg".to_string(), "g__out".to_string()]);
    }

    #[test]
    fn firing_non_trigger_returns_empty() {
        // Defensive: caller must never pass a non-trigger id. We
        // return empty rather than silently fabricating kicks.
        let p = project(
            &[("a", false, &[]), ("out", false, &[])],
            &[("a", "out")],
        );
        assert!(compute_trigger_fire(&p, "a", &Value::Null, None).is_none());
    }

    /// Two programs in one file sharing an upstream node (a database, a
    /// provider). Firing one trigger must leave the other program's
    /// consumers OUTSIDE the subgraph: the shared node still runs (it
    /// feeds the fired program) and still emits down every wire, so the
    /// engine needs the boundary to skip the other program's nodes
    /// instead of parking their partial input sets forever. This is the
    /// shape that ended every fire of a two-trigger project as Stuck.
    #[test]
    fn a_shared_upstream_node_does_not_pull_the_other_program_in() {
        //  Shared ──┬──► TriggerX ──► Bx ──► OutX
        //           └──► TriggerY ──► By ──► OutY
        // (Shared also wires straight into Bx and By.)
        let p = project(
            &[
                ("shared", false, &[]),
                ("trigger_x", true, &[]),
                ("trigger_y", true, &[]),
                ("bx", false, &[]),
                ("by", false, &[]),
                ("out_x", false, &[]),
                ("out_y", false, &[]),
            ],
            &[
                ("shared", "bx"),
                ("shared", "by"),
                ("trigger_x", "bx"),
                ("trigger_y", "by"),
                ("bx", "out_x"),
                ("by", "out_y"),
            ],
        );
        let TriggerFire { kicks, subgraph } = fire(&p, "trigger_x", Value::String("msg".into()));
        assert_eq!(ids(&kicks), vec!["shared".to_string(), "trigger_x".to_string()]);
        assert_eq!(
            sorted(&subgraph),
            vec!["bx", "out_x", "shared", "trigger_x"],
            "the other program's trigger and consumers stay outside the fire"
        );
    }
}

#[cfg(test)]
mod infra_kick_and_dep_tests {
    use super::*;

    /// (id, is_trigger, requires_infra). Shared with `run_subgraph_tests`.
    pub(super) fn project(nodes: &[(&str, bool, bool)], edges: &[(&str, &str)]) -> ProjectDefinition {
        let n_json: Vec<serde_json::Value> = nodes
            .iter()
            .map(|(id, is_trigger, requires_infra)| {
                serde_json::json!({
                    "id": id,
                    "nodeType": "T",
                    "label": null,
                    "config": {},
                    "position": { "x": 0, "y": 0 },
                    "features": { "isTrigger": is_trigger },
                    "requiresInfra": requires_infra,
                })
            })
            .collect();
        let e_json: Vec<serde_json::Value> = edges
            .iter()
            .map(|(s, t)| {
                serde_json::json!({
                    "id": format!("e_{}_{}", s, t),
                    "source": s,
                    "sourcePort": "out",
                    "target": t,
                    "targetPort": "in",
                })
            })
            .collect();
        let body = serde_json::json!({
            "id": uuid::Uuid::new_v4(),
            "nodes": n_json,
            "edges": e_json,
            "groups": []
        });
        serde_json::from_value(body).expect("valid test project")
    }

    fn kick_ids(kicks: &[Kick]) -> Vec<String> {
        let mut v: Vec<String> = kicks.iter().map(|k| k.node_id.clone()).collect();
        v.sort();
        v
    }

    #[test]
    fn infra_kicks_are_upstream_roots_not_infra_nodes() {
        // text → compute → infra
        let p = project(
            &[("text", false, false), ("compute", false, false), ("infra", false, true)],
            &[("text", "compute"), ("compute", "infra")],
        );
        let kicks = compute_infra_setup_kicks(&p);
        // The kick is the upstream root (text), NOT the infra node.
        assert_eq!(kick_ids(&kicks), vec!["text".to_string()]);
    }

    #[test]
    fn infra_kicks_skip_unreachable_branches() {
        // unrelated standalone node + a real text → infra chain.
        let p = project(
            &[
                ("standalone", false, false),
                ("text", false, false),
                ("infra", false, true),
            ],
            &[("text", "infra")],
        );
        let kicks = compute_infra_setup_kicks(&p);
        assert_eq!(kick_ids(&kicks), vec!["text".to_string()]);
    }

    #[test]
    fn infra_node_with_no_upstream_kicks_itself() {
        // A parameterless infra node (no upstream edges) IS its own
        // root: has to kick something to fire.
        let p = project(&[("infra", false, true)], &[]);
        let kicks = compute_infra_setup_kicks(&p);
        assert_eq!(kick_ids(&kicks), vec!["infra".to_string()]);
    }

    #[test]
    fn infra_kicks_empty_when_no_infra_nodes() {
        let p = project(&[("a", false, false), ("b", false, false)], &[("a", "b")]);
        assert!(compute_infra_setup_kicks(&p).is_empty());
    }

    #[test]
    fn infra_kicks_handle_multiple_infra_nodes_with_shared_root() {
        // text → infraA ; text → infraB
        let p = project(
            &[("text", false, false), ("infraA", false, true), ("infraB", false, true)],
            &[("text", "infraA"), ("text", "infraB")],
        );
        let kicks = compute_infra_setup_kicks(&p);
        // text is the only root reaching both.
        assert_eq!(kick_ids(&kicks), vec!["text".to_string()]);
    }

    #[test]
    fn trigger_deps_direct_chain() {
        // infra → trigger
        let p = project(
            &[("infra", false, true), ("trigger", true, false)],
            &[("infra", "trigger")],
        );
        let deps = compute_trigger_deps(&p);
        assert_eq!(deps, vec![("infra".to_string(), "trigger".to_string())]);
    }

    #[test]
    fn trigger_deps_indirect_chain() {
        // infra → middle → trigger
        let p = project(
            &[
                ("infra", false, true),
                ("middle", false, false),
                ("trigger", true, false),
            ],
            &[("infra", "middle"), ("middle", "trigger")],
        );
        let deps = compute_trigger_deps(&p);
        assert_eq!(deps, vec![("infra".to_string(), "trigger".to_string())]);
    }

    #[test]
    fn trigger_deps_skip_unrelated_triggers() {
        // infraA → triggerA ; infraB and triggerB are not connected.
        let p = project(
            &[
                ("infraA", false, true),
                ("infraB", false, true),
                ("triggerA", true, false),
                ("triggerB", true, false),
            ],
            &[("infraA", "triggerA")],
        );
        let deps = compute_trigger_deps(&p);
        assert_eq!(deps, vec![("infraA".to_string(), "triggerA".to_string())]);
    }

    #[test]
    fn trigger_deps_one_infra_two_triggers() {
        // infra → t1 ; infra → t2
        let p = project(
            &[("infra", false, true), ("t1", true, false), ("t2", true, false)],
            &[("infra", "t1"), ("infra", "t2")],
        );
        let deps = compute_trigger_deps(&p);
        // Sorted by (infra, trigger).
        assert_eq!(
            deps,
            vec![
                ("infra".to_string(), "t1".to_string()),
                ("infra".to_string(), "t2".to_string()),
            ]
        );
    }

    #[test]
    fn trigger_deps_empty_when_no_triggers() {
        let p = project(
            &[("infra", false, true), ("downstream", false, false)],
            &[("infra", "downstream")],
        );
        assert!(compute_trigger_deps(&p).is_empty());
    }

    #[test]
    fn trigger_deps_empty_when_no_infra() {
        let p = project(
            &[("trigger", true, false), ("upstream", false, false)],
            &[("upstream", "trigger")],
        );
        assert!(compute_trigger_deps(&p).is_empty());
    }

    #[test]
    fn trigger_deps_skip_when_trigger_does_not_reach_infra() {
        // text → trigger (no infra in the path)
        let p = project(
            &[
                ("text", false, false),
                ("trigger", true, false),
                ("infra", false, true),
            ],
            &[("text", "trigger"), ("infra", "text")],
        );
        // text → trigger; infra → text; so trigger's upstream
        // includes both text AND infra. Deps should include infra.
        let deps = compute_trigger_deps(&p);
        assert_eq!(deps, vec![("infra".to_string(), "trigger".to_string())]);
    }
}

#[cfg(test)]
mod run_subgraph_tests {
    use super::infra_kick_and_dep_tests::project;
    use super::*;

    /// Diamond: src feeds two branches, only one is aimed at. The
    /// other branch's exclusive nodes stay out of the subgraph, so
    /// they are neither infra-gated nor dispatched.
    #[test]
    fn aiming_at_one_output_excludes_the_other_branch() {
        let p = project(
            &[
                ("src", false, false),
                ("a", false, false),
                ("out1", false, false),
                ("b", false, false),
                ("out2", false, false),
            ],
            &[("src", "a"), ("a", "out1"), ("src", "b"), ("b", "out2")],
        );
        let sub = RunSubgraph::of(&p, &["out1".to_string()]);
        let mut nodes: Vec<&str> = sub.nodes.iter().map(|s| s.as_str()).collect();
        nodes.sort();
        assert_eq!(nodes, vec!["a", "out1", "src"]);
    }

    /// A trigger in the walk is a terminator: included (it gets kicked
    /// payload-less so its branch prunes), but its own upstream is not.
    #[test]
    fn a_trigger_terminates_the_upstream_walk_and_kicks_bare() {
        let p = project(
            &[
                ("setup", false, false),
                ("trig", true, false),
                ("mid", false, false),
                ("out", false, false),
            ],
            &[("setup", "trig"), ("trig", "mid"), ("mid", "out")],
        );
        let sub = RunSubgraph::of(&p, &["out".to_string()]);
        assert!(sub.nodes.contains("trig"));
        assert!(!sub.nodes.contains("setup"), "the trigger's own upstream stays out");

        let kicks = sub.root_kicks(&p, &serde_json::json!({ "mock": 1 }), None);
        let trig = kicks.iter().find(|k| k.node_id == "trig").expect("trigger is a root");
        assert!(trig.payload.is_none(), "a manual run kicks triggers payload-less");
        assert!(!trig.firing);
    }

    /// Fire time: only the firing trigger carries the wake payload
    /// and the snapshot; every other root is a bare kick.
    #[test]
    fn only_the_firing_trigger_carries_the_payload() {
        let p = project(
            &[
                ("trig", true, false),
                ("other", true, false),
                ("out", false, false),
            ],
            &[("trig", "out"), ("other", "out")],
        );
        let sub = RunSubgraph::of(&p, &["out".to_string()]);
        let payload = serde_json::json!({ "msg": "hi" });
        let snapshot = serde_json::json!({ "port": "v" });
        let kicks = sub.root_kicks(&p, &payload, Some(("trig", Some(&snapshot))));
        let trig = kicks.iter().find(|k| k.node_id == "trig").unwrap();
        assert!(trig.firing);
        assert_eq!(trig.payload.as_ref(), Some(&payload));
        assert_eq!(trig.port_snapshot.as_ref(), Some(&snapshot));
        let other = kicks.iter().find(|k| k.node_id == "other").unwrap();
        assert!(!other.firing);
        assert!(other.payload.is_none() && other.port_snapshot.is_none());
    }
}

#[cfg(test)]
mod available_actions_tests {
    //! Layer-1 tests for the reconciliation table
    //! (`docs/project-lifecycle-state-model.md` §8): one case per
    //! table row, pure inputs -> expected verb set.

    use super::*;
    use crate::project_store::{ProjectLifecycle, ProjectStatus, ProjectTransition};

    struct Case {
        lifecycle: ProjectLifecycle,
        transition: ProjectTransition,
        has_triggers: bool,
        has_infra: bool,
        orphaned_infra: bool,
        infra_rollup: &'static str,
        infra_busy: bool,
        drift: DriftBits,
        preservation: PreservationCounts,
        running_count: usize,
    }

    impl Default for Case {
        fn default() -> Self {
            Self {
                lifecycle: ProjectLifecycle::wiped(),
                transition: ProjectTransition::None,
                has_triggers: true,
                has_infra: false,
                orphaned_infra: false,
                infra_rollup: "none",
                infra_busy: false,
                drift: DriftBits::default(),
                preservation: PreservationCounts::default(),
                running_count: 0,
            }
        }
    }

    fn actions(case: &Case) -> Vec<String> {
        compute_available_actions(&ActionInputs {
            lifecycle: &case.lifecycle,
            transition: case.transition,
            has_triggers: case.has_triggers,
            has_infra: case.has_infra,
            orphaned_infra: case.orphaned_infra,
            infra_rollup: case.infra_rollup,
            infra_busy: case.infra_busy,
            drift: &case.drift,
            preservation: &case.preservation,
            running_count: case.running_count,
        })
    }

    fn assert_actions(case: &Case, expected: &[&str]) {
        assert_eq!(actions(case), expected.to_vec());
    }

    // ---- master rule: any transitional state offers only its cancel ----

    #[test]
    fn building_offers_only_cancel_build() {
        assert_actions(
            &Case { transition: ProjectTransition::Building, ..Case::default() },
            &["cancel_build"],
        );
        // Idempotent while already cancelling.
        assert_actions(
            &Case { transition: ProjectTransition::CancellingBuild, ..Case::default() },
            &["cancel_build"],
        );
    }

    #[test]
    fn activating_offers_only_cancel_activate() {
        assert_actions(
            &Case { lifecycle: ProjectLifecycle::activating(), ..Case::default() },
            &["cancel_activate"],
        );
    }

    #[test]
    fn deactivating_offers_cancel_running_and_resume() {
        let deactivating = ProjectLifecycle::deactivating_to(ProjectLifecycle::parked());
        assert_actions(
            &Case { lifecycle: deactivating.clone(), running_count: 2, ..Case::default() },
            &["cancel_running", "resume_active"],
        );
        // Nothing left to cancel once the running set empties.
        assert_actions(
            &Case { lifecycle: deactivating, running_count: 0, ..Case::default() },
            &["resume_active"],
        );
    }

    #[test]
    fn infra_transitional_offers_only_infra_cancel() {
        for rollup in ["provisioning", "stopping", "terminating"] {
            assert_actions(
                &Case { has_infra: true, infra_rollup: rollup, ..Case::default() },
                &["infra_cancel"],
            );
        }
    }

    #[test]
    fn infra_op_in_flight_is_transitional_even_with_a_stable_rollup() {
        // A claimed stop still draining (or an InfraSetup execution
        // before any node row flips) leaves the rollup reading
        // "running"; the in-flight fact must still collapse the row
        // to cancel-only, or new runs would starve the drain.
        assert_actions(
            &Case {
                has_infra: true,
                infra_rollup: "running",
                infra_busy: true,
                ..Case::default()
            },
            &["infra_cancel"],
        );
    }

    // ---- stable rows ----

    #[test]
    fn inactive_no_infra_offers_run_and_activate() {
        assert_actions(&Case::default(), &["run", "activate"]);
    }

    #[test]
    fn no_triggers_hides_activate_but_run_stands() {
        assert_actions(&Case { has_triggers: false, ..Case::default() }, &["run"]);
    }

    #[test]
    fn inactive_infra_resting_gates_run_and_activate_on_infra() {
        // Source declares infra, nothing provisioned: run/activate
        // are rejected until infra is running; start is the way up.
        assert_actions(
            &Case { has_infra: true, infra_rollup: "none", ..Case::default() },
            &["infra_start"],
        );
    }

    #[test]
    fn inactive_infra_running_offers_everything_stable() {
        assert_actions(
            &Case { has_infra: true, infra_rollup: "running", ..Case::default() },
            &["run", "activate", "infra_stop", "infra_terminate"],
        );
        // Infra drift additionally lights upgrade.
        assert_actions(
            &Case {
                has_infra: true,
                infra_rollup: "running",
                drift: DriftBits { infra_drift: true, ..Default::default() },
                ..Case::default()
            },
            &["run", "activate", "infra_stop", "infra_terminate", "infra_upgrade"],
        );
    }

    #[test]
    fn inactive_infra_degraded_offers_repair_verbs_only() {
        for rollup in ["failed", "flaky", "partial"] {
            assert_actions(
                &Case { has_infra: true, infra_rollup: rollup, ..Case::default() },
                &["infra_start", "infra_stop", "infra_terminate"],
            );
        }
    }

    #[test]
    fn inactive_infra_stopped_offers_start_and_terminate() {
        assert_actions(
            &Case { has_infra: true, infra_rollup: "stopped", ..Case::default() },
            &["infra_start", "infra_terminate"],
        );
    }

    #[test]
    fn active_offers_run_and_deactivate_and_drift_lights_resync() {
        let active = ProjectLifecycle::active();
        assert_actions(
            &Case { lifecycle: active.clone(), ..Case::default() },
            &["run", "deactivate"],
        );
        assert_actions(
            &Case {
                lifecycle: active,
                drift: DriftBits { definition_drift: true, ..Default::default() },
                ..Case::default()
            },
            &["run", "deactivate", "resync"],
        );
    }

    #[test]
    fn active_with_last_trigger_deleted_keeps_deactivate() {
        // Trigger divergence: the source no longer has triggers but the
        // backend is still active; deactivate must stay offered so the
        // user can bring the live trigger down.
        assert_actions(
            &Case { lifecycle: ProjectLifecycle::active(), has_triggers: false, ..Case::default() },
            &["run", "deactivate"],
        );
    }

    #[test]
    fn active_infra_running_keeps_stop_and_terminate() {
        // Resolved cell Q2: stop/terminate while active auto-deactivate
        // first (one click), so both stay offered.
        assert_actions(
            &Case {
                lifecycle: ProjectLifecycle::active(),
                has_infra: true,
                infra_rollup: "running",
                ..Case::default()
            },
            &["run", "deactivate", "infra_stop", "infra_terminate"],
        );
    }

    #[test]
    fn preserved_state_flips_activate_to_reactivate_only_when_inactive() {
        let preserved = PreservationCounts { parked: 2, suspended: 1 };
        assert_actions(
            &Case {
                preservation: PreservationCounts { ..preserved },
                ..Case::default()
            },
            &["run", "reactivate"],
        );
        // Registered (never activated) always offers plain activate.
        let mut registered = ProjectLifecycle::wiped();
        registered.status = ProjectStatus::Registered;
        assert_actions(
            &Case {
                lifecycle: registered,
                preservation: PreservationCounts { parked: 2, suspended: 1 },
                ..Case::default()
            },
            &["run", "activate"],
        );
    }

    // ---- Model 1: orphaned live infra ----

    #[test]
    fn orphan_never_gates_run_and_keeps_stop_terminate_visible() {
        // Source has NO infra (the user deleted the node) but live
        // rows still run: run/activate are free (the plain graph runs
        // in the shared pool, unlinked) AND the infra controls stay so
        // the user never loses track of the orphan. No start/upgrade:
        // there is no source spec to provision from.
        assert_actions(
            &Case { orphaned_infra: true, infra_rollup: "running", ..Case::default() },
            &["run", "activate", "infra_stop", "infra_terminate"],
        );
    }

    #[test]
    fn stopped_orphan_still_offers_terminate() {
        assert_actions(
            &Case { orphaned_infra: true, infra_rollup: "stopped", ..Case::default() },
            &["run", "activate", "infra_terminate"],
        );
    }

    #[test]
    fn orphan_mid_terminate_offers_infra_cancel() {
        assert_actions(
            &Case { orphaned_infra: true, infra_rollup: "terminating", ..Case::default() },
            &["infra_cancel"],
        );
    }
}

#[cfg(test)]
mod status_query_wire_shape_tests {
    use super::*;

    /// `StatusQuery`'s wire shape is the contract with the CLI's URL
    /// builder (and any future direct caller from the extension).
    /// A typo in one of the three `rename = "desiredXyzHash"`
    /// attributes silently sets the field to `None`, the drift
    /// comparison bypasses, and no compile error fires. Pin the
    /// camelCase wire shape here so a rename is loud.
    ///
    /// Tested via the JSON serde shape because Query at runtime
    /// flows through `serde_urlencoded` (transitive dep of axum,
    /// not directly in the dispatcher's dev-dep set). The `rename`
    /// attribute applies to BOTH formats, so a JSON round-trip is
    /// enough to catch a typo in the rename string.
    #[test]
    fn status_query_round_trips_camelcase_keys() {
        let json = serde_json::json!({
            "desiredBinaryHash": "abc",
            "desiredDefinitionHash": "def",
            "desiredInfraHash": "ghi",
        });
        let q: StatusQuery = serde_json::from_value(json)
            .expect("camelCase keys must deserialize");
        assert_eq!(q.desired_binary_hash.as_deref(), Some("abc"));
        assert_eq!(q.desired_definition_hash.as_deref(), Some("def"));
        assert_eq!(q.desired_infra_hash.as_deref(), Some("ghi"));
    }

    /// Snake_case used to be accepted via `alias`. The Round 4
    /// alias-drop removed that compatibility; pin it as not-aliased
    /// so a future regression that re-adds the alias is loud.
    #[test]
    fn status_query_ignores_snake_case_keys() {
        let json = serde_json::json!({
            "desired_binary_hash": "abc",
        });
        let q: StatusQuery = serde_json::from_value(json)
            .expect("deserialize never fails on optional-only fields");
        // The snake_case key is unknown; the camelCase field stays
        // None. If anyone re-adds `alias = "desired_binary_hash"`,
        // this flips to Some("abc") and the test fails.
        assert_eq!(q.desired_binary_hash, None);
    }
}
