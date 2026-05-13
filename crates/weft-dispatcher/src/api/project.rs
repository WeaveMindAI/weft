//! Project lifecycle HTTP handlers. `POST /projects` registers a
//! project; `POST /projects/{id}/run` kicks off a fresh execution.
//! `weft run` on the CLI calls these.

use std::collections::HashSet;

use axum::{extract::{Path, State}, http::StatusCode, Json};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use weft_compiler::{Diagnostic, Severity};
use weft_core::project::EdgeIndex;
use weft_core::ProjectDefinition;
use weft_catalog::stdlib_catalog;

use weft_core::primitive::RootSeed;
use crate::events::DispatcherEvent;
use crate::state::DispatcherState;

#[derive(Debug, Serialize, Deserialize)]
pub struct ProjectSummary {
    pub id: String,
    pub name: String,
    pub status: String,
}

pub async fn list(State(state): State<DispatcherState>) -> Json<Vec<ProjectSummary>> {
    let items = state.projects.list().await;
    Json(items.into_iter().map(|p| ProjectSummary {
        id: p.id.to_string(),
        name: p.name,
        status: p.status.as_str().to_string(),
    }).collect())
}

/// Payload accepted by `POST /projects`. The client (CLI, VS Code
/// extension) sends the raw `main.weft` source and a stable project
/// id; the dispatcher compiles, enriches, and registers the result.
///
/// Keeping compilation inside the dispatcher means every client calls
/// one endpoint to register, not two (parse + register). Matches the
/// `cargo build` shape: hand the tool a source tree, it does the work.
#[derive(Debug, Deserialize)]
pub struct RegisterRequest {
    pub id: uuid::Uuid,
    pub name: String,
    pub source: String,
    /// Absolute path to the project root. Unused today; reserved
    /// for multi-file imports once the compiler resolves them.
    #[serde(default)]
    pub root: Option<String>,
    /// Source hash of the worker image. CLI computes from project
    /// source + workspace; dispatcher persists on the project row
    /// (used as worker docker tag suffix AND as the resync drift
    /// signal). Optional in tests; production paths always set it.
    #[serde(default, rename = "sourceHash", alias = "source_hash")]
    pub source_hash: Option<String>,
    /// Infra hash. CLI computes from infra-closure source +
    /// workspace; dispatcher persists for the upgrade drift signal.
    /// Optional in tests; production paths set it whenever they set
    /// `source_hash` so both signals stay in sync.
    #[serde(default, rename = "infraHash", alias = "infra_hash")]
    pub infra_hash: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct RegisterError {
    pub error: String,
    pub diagnostics: Vec<Diagnostic>,
}

/// Compile the supplied source and register the resulting project.
/// Returns 400 with structured diagnostics on compile or enrich
/// failure so the editor can highlight the offending lines.
pub async fn register(
    State(state): State<DispatcherState>,
    Json(req): Json<RegisterRequest>,
) -> Result<Json<ProjectSummary>, (StatusCode, Json<RegisterError>)> {
    let mut project = match weft_compiler::weft_compiler::compile(&req.source, req.id) {
        Ok(p) => p,
        Err(errors) => {
            let diagnostics = errors
                .into_iter()
                .map(|e| Diagnostic {
                    line: e.line,
                    column: 0,
                    severity: Severity::Error,
                    message: e.message,
                    code: Some("parse".into()),
                })
                .collect();
            return Err((
                StatusCode::BAD_REQUEST,
                Json(RegisterError {
                    error: "compile".into(),
                    diagnostics,
                }),
            ));
        }
    };
    project.name = req.name;

    let catalog = stdlib_catalog().map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(RegisterError {
                error: format!("catalog: {e}"),
                diagnostics: Vec::new(),
            }),
        )
    })?;
    if let Err(e) = weft_compiler::enrich::enrich(&mut project, &catalog) {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(RegisterError {
                error: "enrich".into(),
                diagnostics: vec![Diagnostic {
                    line: 0,
                    column: 0,
                    severity: Severity::Error,
                    message: format!("{e}"),
                    code: Some("enrich".into()),
                }],
            }),
        ));
    }

    let tenant = state.tenant_router.tenant_for_project(&project.id.to_string());
    let namespace = state.namespace_mapper.namespace_for(&tenant);
    if let Err(e) = crate::tenant_namespace::ensure_tenant_namespace(
        &namespace,
        tenant.as_str(),
        crate::tenant_namespace::ClusterCidrs {
            pod_cidr: &state.cluster_pod_cidr,
            service_cidr: &state.cluster_service_cidr,
            ingress_namespace: &state.cluster_ingress_namespace,
        },
    )
    .await
    {
        // Best-effort; tolerate failures during local dev where
        // kubectl isn't available. The static `wm-local` manifest
        // covers the local case.
        tracing::warn!(
            target: "weft_dispatcher::register",
            tenant = %tenant,
            namespace = %namespace,
            error = %e,
            "ensure_tenant_namespace failed; continuing"
        );
    }
    let summary = state
        .projects
        .register(project, tenant.as_str())
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(RegisterError {
                    error: format!("register: {e}"),
                    diagnostics: Vec::new(),
                }),
            )
        })?;
    if let Some(hash) = req.source_hash.as_deref() {
        state
            .projects
            .set_running_source_hash(summary.id, hash)
            .await;
    }
    if let Some(hash) = req.infra_hash.as_deref() {
        state
            .projects
            .set_running_infra_hash(summary.id, hash)
            .await;
    }
    state
        .events
        .publish(DispatcherEvent::ProjectRegistered {
            project_id: summary.id.to_string(),
            name: summary.name.clone(),
        })
        .await;
    Ok(Json(ProjectSummary {
        id: summary.id.to_string(),
        name: summary.name,
        status: summary.status.as_str().to_string(),
    }))
}

pub async fn get(
    State(state): State<DispatcherState>,
    Path(id): Path<String>,
) -> Result<Json<ProjectSummary>, StatusCode> {
    let id = id.parse::<uuid::Uuid>().map_err(|_| StatusCode::BAD_REQUEST)?;
    let summary = state.projects.get(id).await.ok_or(StatusCode::NOT_FOUND)?;
    Ok(Json(ProjectSummary {
        id: summary.id.to_string(),
        name: summary.name,
        status: summary.status.as_str().to_string(),
    }))
}

pub async fn remove(
    State(state): State<DispatcherState>,
    Path(id): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    let id = id
        .parse::<uuid::Uuid>()
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    // Deactivate first: cancels any in-flight executions, unregisters
    // every wake signal (entry + resume) from the tenant's listener,
    // drops entry tokens. If deactivate fails on a DB / listener
    // error we must abort: removing the project row while signals
    // remain in the listener leaves dangling registrations that
    // outlive their owning project.
    deactivate_project(&state, id).await?;
    if state.projects.remove(id).await {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err((StatusCode::NOT_FOUND, "project not found".into()))
    }
}

#[derive(Debug, Deserialize)]
pub struct RunRequest {
    /// Optional override: which node to start from. Defaults to the
    /// first entry-primitive-bearing node in the project. If none,
    /// falls back to the first top-level node.
    #[serde(default)]
    pub entry_node: Option<String>,
    /// Initial payload for the entry node's first pulse.
    #[serde(default)]
    pub payload: Value,
}

#[derive(Debug, Serialize)]
pub struct RunResponse {
    pub color: String,
}

/// Start a fresh execution for a registered project.
///
/// Manual-run semantics (see docs/v2-design.md 3.0): collect every
/// node with `is_output: true`, compute the union of their upstream
/// subgraphs, find the roots, seed each root with a null-valued pulse.
/// If `body.entry_node` is set, that node's roots are used as a
/// single-entry override instead (used for debugging a specific
/// subgraph).
pub async fn run(
    State(state): State<DispatcherState>,
    Path(id): Path<String>,
    Json(body): Json<RunRequest>,
) -> Result<Json<RunResponse>, (StatusCode, String)> {
    let id = id.parse::<uuid::Uuid>().map_err(|_| (StatusCode::BAD_REQUEST, "bad id".into()))?;
    let project = state
        .projects
        .project(id)
        .await
        .ok_or_else(|| (StatusCode::NOT_FOUND, "project definition missing".into()))?;

    // Pick targets: explicit override -> upstream of that one node.
    // Otherwise every `is_output` node in the project.
    let targets: Vec<String> = match &body.entry_node {
        Some(n) => vec![n.clone()],
        None => project
            .nodes
            .iter()
            .filter(|n| n.is_output())
            .map(|n| n.id.clone())
            .collect(),
    };
    if targets.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "project has no outputs; add a Debug node or mark one with is_output: true".into(),
        ));
    }

    let seeds = compute_root_seeds(&project, &targets, &body.payload);
    if seeds.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "computed subgraph has no roots; graph may be cyclic or malformed".into(),
        ));
    }

    let color = uuid::Uuid::new_v4();

    // ExecutionStarted carries a single `entry_node`. When the
    // subgraph has many roots the first seed's node wins; the
    // PulseSeeded events below carry the full root set anyway.
    let entry_node_for_journal = seeds[0].node_id.clone();

    // Event-sourced log: execution started + one PulseSeeded event
    // per root. Replay rebuilds the initial pulse table from these.
    let now = unix_now();
    let _ = state
        .journal
        .record_event(&weft_journal::ExecEvent::ExecutionStarted {
            color,
            project_id: id.to_string(),
            entry_node: entry_node_for_journal.clone(),
            phase: weft_core::context::Phase::Fire,
            at_unix: now,
        })
        .await;
    // Mint a pulse_id per seed. Both the journaled `PulseSeeded`
    // event and the `RootSeed` shipped to the worker carry the
    // same UUID, so a fresh worker's fold reconstructs the seed
    // pulse with the same identity the live worker used.
    let core_seeds: Vec<weft_core::primitive::RootSeed> = seeds
        .into_iter()
        .map(|s| weft_core::primitive::RootSeed {
            node_id: s.node_id,
            pulse_id: uuid::Uuid::new_v4().to_string(),
            value: s.value,
        })
        .collect();
    for seed in &core_seeds {
        let _ = state
            .journal
            .record_event(&weft_journal::ExecEvent::PulseSeeded {
                color,
                pulse_id: seed.pulse_id.clone(),
                node_id: seed.node_id.clone(),
                port: "__seed__".to_string(),
                lane: Vec::new(),
                value: seed.value.clone(),
                at_unix: now,
            })
            .await;
    }
    // The journal already has ExecutionStarted + PulseSeeded above.
    // Enqueue an `execute` task targeted at the worker pool; the
    // cold-start trigger spawns a Pod for this project if none is
    // alive, and the worker's claim loop folds the journal and runs.
    let tenant = state.tenant_router.tenant_for_project(&id.to_string());
    crate::task_kinds::execute::enqueue_execute(
        &state.pg_pool,
        &id.to_string(),
        color,
        Some(tenant.as_str()),
    )
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("enqueue execute: {e}")))?;

    state
        .events
        .publish(DispatcherEvent::ExecutionStarted {
            color,
            entry_node: entry_node_for_journal,
            project_id: id.to_string(),
        })
        .await;

    Ok(Json(RunResponse { color: color.to_string() }))
}

/// Compute the set of seed pulses for a manual run. Walks upstream
/// from each target, collects the nodes in the subgraph, picks the
/// ones with no incoming edge inside the subgraph as roots, and
/// returns one seed per root.
///
/// The `payload` is attached to every seed as-is. Per-trigger mocks
/// will attach different payloads per root once the mock file format
/// lands; the seed shape already supports it.
fn compute_root_seeds(
    project: &ProjectDefinition,
    targets: &[String],
    payload: &Value,
) -> Vec<RootSeed> {
    let edge_idx = EdgeIndex::build(project);
    let in_subgraph = upstream_closure(project, &edge_idx, targets);
    roots_of(project, &edge_idx, &in_subgraph)
        .into_iter()
        .map(|id| RootSeed {
            node_id: id,
            pulse_id: uuid::Uuid::new_v4().to_string(),
            value: payload.clone(),
        })
        .collect()
}

/// Seeds for a TriggerSetup-phase sub-execution.
///
/// Target set = every trigger node. Walk upstream (no terminators);
/// every node in the closure runs. Triggers call `ctx.register_signal`
/// under this phase; infra nodes return their `/outputs`; regular
/// upstream nodes do their normal work.
///
/// Returns an empty vec if the project has no triggers (activate is
/// a no-op in that case).
pub fn compute_trigger_setup_seeds(project: &ProjectDefinition) -> Vec<RootSeed> {
    let edge_idx = EdgeIndex::build(project);

    // A node counts as a trigger iff its metadata sets
    // `features.is_trigger`. Trigger nodes run `Phase::TriggerSetup`
    // at activation: their body builds a `SignalSpec` and calls
    // `ctx.register_signal`.
    let triggers: Vec<String> = project
        .nodes
        .iter()
        .filter(|n| n.features.is_trigger)
        .map(|n| n.id.clone())
        .collect();
    if triggers.is_empty() {
        return Vec::new();
    }

    let in_subgraph = upstream_closure(project, &edge_idx, &triggers);
    roots_of(project, &edge_idx, &in_subgraph)
        .into_iter()
        .map(|id| RootSeed {
            node_id: id,
            pulse_id: uuid::Uuid::new_v4().to_string(),
            value: Value::Null,
        })
        .collect()
}

/// Spawn a worker to run the InfraSetup sub-execution for the
/// given nodes and wait for it to complete. Each node's
/// `execute()` runs with `Phase::InfraSetup` and calls
/// `ctx.provision_sidecar(spec)` to get its sidecar applied.
/// Mirrors `run_trigger_setup` but seeds the infra subgraph.
pub async fn run_infra_setup(
    state: &DispatcherState,
    project_id_uuid: uuid::Uuid,
    node_ids: Vec<String>,
) -> Result<(), (StatusCode, String)> {
    if node_ids.is_empty() {
        return Ok(());
    }
    let project_id = project_id_uuid.to_string();
    let color = uuid::Uuid::new_v4();
    let now = unix_now();

    let seeds: Vec<RootSeed> = node_ids
        .into_iter()
        .map(|node_id| RootSeed {
            node_id,
            pulse_id: uuid::Uuid::new_v4().to_string(),
            value: Value::Null,
        })
        .collect();

    state
        .journal
        .record_event(&weft_journal::ExecEvent::ExecutionStarted {
            color,
            project_id: project_id.clone(),
            entry_node: seeds[0].node_id.clone(),
            phase: weft_core::context::Phase::InfraSetup,
            at_unix: now,
        })
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("journal: {e}")))?;
    for seed in &seeds {
        let _ = state
            .journal
            .record_event(&weft_journal::ExecEvent::PulseSeeded {
                color,
                pulse_id: seed.pulse_id.clone(),
                node_id: seed.node_id.clone(),
                port: "__seed__".to_string(),
                lane: Vec::new(),
                value: seed.value.clone(),
                at_unix: now,
            })
            .await;
    }

    let mut events = state.events.subscribe_project(&project_id).await;

    let tenant = state.tenant_router.tenant_for_project(&project_id);
    crate::task_kinds::execute::enqueue_execute(
        &state.pg_pool,
        &project_id,
        color,
        Some(tenant.as_str()),
    )
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("enqueue execute: {e}")))?;

    // Wait for completion. The broadcast channel returns Err on
    // lag or close; treat both as transient and keep waiting (the
    // deadline backstops runaway work).
    let deadline = tokio::time::sleep(std::time::Duration::from_secs(300));
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            res = events.recv() => {
                match res {
                    Ok(crate::events::DispatcherEvent::ExecutionCompleted { color: c, .. })
                        if c == color => return Ok(()),
                    Ok(crate::events::DispatcherEvent::ExecutionFailed { color: c, error, .. })
                        if c == color => {
                        return Err((
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("infra setup failed: {error}"),
                        ));
                    }
                    Ok(_) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        return Err((
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "event bus closed during infra setup".into(),
                        ));
                    }
                }
            }
            _ = &mut deadline => {
                return Err((
                    StatusCode::GATEWAY_TIMEOUT,
                    "infra setup timed out after 300s".into(),
                ));
            }
        }
    }
}

/// Seeds for a trigger fire.
///
/// Rule: walk upstream from every output node, but treat trigger
/// nodes as terminators. A node ends up in the fire-time subgraph
/// iff it is reachable upstream from some output WITHOUT passing
/// through a trigger. Triggers themselves are included as seeds
/// (the firing trigger carries the payload; other triggers reachable
/// in the subgraph carry null).
///
/// Why terminators: at fire time the firing trigger's outputs are
/// the payload, not a function of its inputs. Nodes that exist only
/// to produce inputs for triggers (e.g. setup-time config) must not
/// re-run every time the trigger fires. If a node also feeds non-
/// trigger paths that reach the output, it re-runs via those paths.
///
/// Returns an empty vec if no output is reachable via a non-trigger
/// path; the caller treats that as "nothing to run."
pub fn compute_trigger_seeds(
    project: &ProjectDefinition,
    firing_node_id: &str,
    payload: &Value,
) -> Vec<RootSeed> {
    let edge_idx = EdgeIndex::build(project);

    // Set of trigger nodes (`features.is_trigger`). All trigger
    // nodes register signals during TriggerSetup; this is the set
    // we route fires to.
    let triggers: HashSet<String> = project
        .nodes
        .iter()
        .filter(|n| n.features.is_trigger)
        .map(|n| n.id.clone())
        .collect();
    if !triggers.contains(firing_node_id) {
        return Vec::new();
    }

    // Targets = every output node in the project.
    let targets: Vec<String> = project
        .nodes
        .iter()
        .filter(|n| n.is_output())
        .map(|n| n.id.clone())
        .collect();
    if targets.is_empty() {
        return Vec::new();
    }

    // Upstream closure from outputs, stopping at triggers (include
    // the trigger but do not walk through its incoming edges).
    let in_subgraph = upstream_closure_stop_at(project, &edge_idx, &targets, &triggers);

    // If the firing trigger isn't in the subgraph, nothing its
    // payload would feed is reachable upstream from an output;
    // nothing to run.
    if !in_subgraph.contains(firing_node_id) {
        return Vec::new();
    }

    // Roots of the subgraph. Triggers are always roots (they were
    // terminators); nodes in the subgraph with no in-subgraph
    // parent are roots too. A firing trigger carries the payload;
    // every other root carries null.
    roots_of_with_forced(project, &edge_idx, &in_subgraph, &triggers)
        .into_iter()
        .map(|id| {
            let value = if id == firing_node_id {
                payload.clone()
            } else {
                Value::Null
            };
            RootSeed {
                node_id: id,
                pulse_id: uuid::Uuid::new_v4().to_string(),
                value,
            }
        })
        .collect()
}

/// BFS upstream from `targets` through incoming edges, returning
/// every reachable node id.
fn upstream_closure(
    project: &ProjectDefinition,
    edge_idx: &EdgeIndex,
    targets: &[String],
) -> HashSet<String> {
    upstream_closure_stop_at(project, edge_idx, targets, &HashSet::new())
}

/// BFS upstream from `targets`, but do not walk through any node in
/// `stop_at`. Stopped nodes are still included in the returned set
/// (so they can be seeded as roots), but their incoming edges are
/// not followed. Used by the fire-time subgraph so triggers act as
/// terminators.
fn upstream_closure_stop_at(
    project: &ProjectDefinition,
    edge_idx: &EdgeIndex,
    targets: &[String],
    stop_at: &HashSet<String>,
) -> HashSet<String> {
    let mut seen: HashSet<String> = HashSet::new();
    let mut frontier: Vec<String> = targets.to_vec();
    while let Some(node_id) = frontier.pop() {
        if !seen.insert(node_id.clone()) {
            continue;
        }
        if stop_at.contains(&node_id) {
            continue;
        }
        for edge in edge_idx.get_incoming(project, &node_id) {
            if !seen.contains(&edge.source) {
                frontier.push(edge.source.clone());
            }
        }
    }
    seen
}

/// Nodes of `in_subgraph` whose incoming edges all come from
/// outside the subgraph. These are the pulse-seed points for a
/// fresh run.
fn roots_of(
    project: &ProjectDefinition,
    edge_idx: &EdgeIndex,
    in_subgraph: &HashSet<String>,
) -> Vec<String> {
    roots_of_with_forced(project, edge_idx, in_subgraph, &HashSet::new())
}

/// Like `roots_of`, but nodes in `force_roots` are always treated
/// as roots regardless of their in-subgraph parents. Used at fire
/// time so triggers (which are terminators, not computed from their
/// inputs at fire time) always end up as seed roots.
fn roots_of_with_forced(
    project: &ProjectDefinition,
    edge_idx: &EdgeIndex,
    in_subgraph: &HashSet<String>,
    force_roots: &HashSet<String>,
) -> Vec<String> {
    let mut roots = Vec::new();
    for node_id in in_subgraph {
        if force_roots.contains(node_id) {
            roots.push(node_id.clone());
            continue;
        }
        let has_in_subgraph_parent = edge_idx
            .get_incoming(project, node_id)
            .iter()
            .any(|e| in_subgraph.contains(&e.source));
        if !has_in_subgraph_parent {
            roots.push(node_id.clone());
        }
    }
    roots
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
    /// Raw status enum: "registered" | "active" | "deactivating"
    /// | "inactive". Mirrors `project.status`.
    pub status: String,
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
    pub infra: Vec<ProjectInfraEntry>,
    pub executions: ProjectExecutionsSummary,
    /// True when project has any infra-typed nodes in its source.
    /// Used by clients to decide whether to even show the
    /// Start/Stop/Upgrade infra controls.
    pub has_infra: bool,
    /// Aggregate infra state across the project's infra nodes:
    /// "none" (no infra nodes defined), "running" (all up),
    /// "stopped" (all down), "partial" (mixed).
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
    /// "Worker source is stale relative to running deployment."
    /// Drives the Resync button. Computed from desired_source_hash
    /// != running_source_hash.
    pub source_drift: bool,
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
    /// Source hash the CLI computed for the current project source.
    /// Compared against `project.running_source_hash` for the resync
    /// drift signal.
    #[serde(default, rename = "desiredSourceHash", alias = "desired_source_hash")]
    pub desired_source_hash: Option<String>,
    /// Infra hash the CLI computed for the current infra closure.
    /// Compared against `project.running_infra_hash` for the upgrade
    /// drift signal.
    #[serde(default, rename = "desiredInfraHash", alias = "desired_infra_hash")]
    pub desired_infra_hash: Option<String>,
}

/// Aggregate view for `weft status`. Returns registration,
/// listener state, per-node infra state, a rollup of recent
/// executions, drift signals (when desired hashes are passed in
/// query params), and the list of currently-valid action verbs.
/// One response, no stitching required by the CLI.
pub async fn status(
    State(state): State<DispatcherState>,
    Path(id_str): Path<String>,
    axum::extract::Query(query): axum::extract::Query<StatusQuery>,
) -> Result<Json<ProjectStatusResponse>, (StatusCode, String)> {
    let id = id_str
        .parse::<uuid::Uuid>()
        .map_err(|_| (StatusCode::BAD_REQUEST, "bad id".into()))?;
    let summary = state
        .projects
        .get(id)
        .await
        .ok_or((StatusCode::NOT_FOUND, "project not found".into()))?;
    let project = state
        .projects
        .project(id)
        .await
        .ok_or((StatusCode::NOT_FOUND, "project definition missing".into()))?;
    let project_id = id.to_string();
    let tenant = state.tenant_router.tenant_for_project(&project_id);
    let listener_running = state.listeners.is_alive(&tenant, &state.pg_pool).await;

    let infra_entries = crate::infra::list_for_project(&state.pg_pool, &project_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("infra_pod: {e}")))?;
    let mut infra = Vec::new();
    for (node_id, entry) in &infra_entries {
        // Skip rows whose node_id no longer exists in the project
        // source (post-resync drift). An empty node_type would mask
        // the divergence; filtering surfaces the cleanup work to the
        // operator via the row count alone.
        let Some(node_type) = project
            .nodes
            .iter()
            .find(|n| n.id == *node_id)
            .map(|n| n.node_type.clone())
        else {
            tracing::warn!(
                target: "weft_dispatcher::status",
                project_id, node_id,
                "infra_pod row references node not in project source; skipping"
            );
            continue;
        };
        infra.push(ProjectInfraEntry {
            node_id: node_id.clone(),
            node_type,
            status: format!("{:?}", entry.status).to_lowercase(),
            endpoint_url: entry.handle.endpoint_url.clone(),
        });
    }

    // Aggregate state across infra nodes:
    // - none:    project has 0 requires_infra nodes (or 0 rows).
    // - running: every requires_infra node has a Running row.
    // - stopped: every requires_infra node has a Stopped row.
    // - partial: mixed (some Running, some not).
    let infra_node_count = project.nodes.iter().filter(|n| n.requires_infra).count();
    let has_infra = infra_node_count > 0;
    let infra_rollup = if !has_infra {
        "none".to_string()
    } else {
        let mut running = 0usize;
        let mut stopped = 0usize;
        for n in project.nodes.iter().filter(|n| n.requires_infra) {
            match infra_entries.iter().find(|(id, _)| id == &n.id) {
                Some((_, e)) if e.status == crate::infra::InfraStatus::Running => running += 1,
                Some(_) => stopped += 1,
                None => stopped += 1,
            }
        }
        if running == infra_node_count {
            "running".to_string()
        } else if stopped == infra_node_count {
            "stopped".to_string()
        } else {
            "partial".to_string()
        }
    };

    let execs = state
        .journal
        .list_executions(500)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("journal: {e}")))?;
    let project_execs: Vec<&crate::journal::ExecutionSummary> = execs
        .iter()
        .filter(|e| e.project_id == project_id)
        .collect();
    let total = project_execs.len();
    let last = project_execs.first();
    let executions = ProjectExecutionsSummary {
        total,
        last_completed_at: last.and_then(|l| l.completed_at),
        last_color: last.map(|l| l.color.to_string()),
        last_status: last.map(|l| l.status.clone()),
    };

    let drift = compute_drift(
        &query,
        state.projects.running_source_hash(id).await.as_deref(),
        state.projects.running_infra_hash(id).await.as_deref(),
    );
    let lifecycle = state.projects.lifecycle(id).await;
    let preservation = preservation_counts(&state, &project_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("preservation_counts: {e}")))?;
    let running_now = running_count(&state, &project_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("running_count: {e}")))?;
    let available_actions = compute_available_actions(
        &lifecycle,
        has_infra,
        &infra_rollup,
        &drift,
        &preservation,
        running_now,
    );

    Ok(Json(ProjectStatusResponse {
        id: project_id,
        name: summary.name,
        status: lifecycle.status.as_str().to_string(),
        mode: lifecycle.mode_label().to_string(),
        fires_deadline_unix: lifecycle.fires_deadline_unix,
        running_count: running_now,
        listener_running,
        infra,
        executions,
        has_infra,
        infra_rollup,
        drift: ProjectDrift {
            infra_drift: drift.infra_drift,
            source_drift: drift.source_drift,
        },
        available_actions,
        preservation,
    }))
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
    running_source_hash: Option<&str>,
    running_infra_hash: Option<&str>,
) -> DriftBits {
    // Source drift: only meaningful when both sides have a hash.
    // No running hash means the project was never built/activated;
    // the action bar shouldn't surface resync drift then.
    let source_drift = match (query.desired_source_hash.as_deref(), running_source_hash) {
        (Some(want), Some(have)) => want != have,
        _ => false,
    };
    let infra_drift = match (query.desired_infra_hash.as_deref(), running_infra_hash) {
        (Some(want), Some(have)) => want != have,
        _ => false,
    };
    DriftBits { infra_drift, source_drift }
}

#[derive(Default, Clone, Copy)]
struct DriftBits {
    infra_drift: bool,
    source_drift: bool,
}

/// Build the action list from the project's full lifecycle. Each
/// action lights iff its preconditions are met. The CLI and the
/// VS Code extension render this list directly: there is no
/// client-side state machine; the dispatcher is the source of
/// truth.
///
/// Action vocabulary:
///   - run                  : trigger a manual fire (always
///                            available on a registered project).
///   - activate             : flip to status=Active.
///   - deactivate           : open the deactivate dialog (mode +
///                            running policy choice).
///   - cancel_running       : during status=Deactivating, kill
///                            running executions to unblock the
///                            drain.
///   - resume_active        : during status=Deactivating, abort the
///                            deactivation and roll back to Active.
///   - reactivate           : during Inactive with preserved
///                            state, open the reactivate-choice
///                            dialog.
///   - infra_start          : project has stopped infra.
///   - infra_stop           : project has running infra.
///   - infra_terminate      : drop infra PVCs.
///   - infra_upgrade        : running infra hash drifted from
///                            desired source.
///   - resync               : trigger-setup hash drifted while
///                            active; re-register triggers.
fn compute_available_actions(
    lifecycle: &crate::project_store::ProjectLifecycle,
    has_infra: bool,
    infra_rollup: &str,
    drift: &DriftBits,
    preservation: &PreservationCounts,
    running_count: usize,
) -> Vec<String> {
    use crate::project_store::ProjectStatus;
    let mut out = Vec::new();
    let infra_running = infra_rollup == "running";

    // `run` is available in every state EXCEPT Activating (the
    // worker is dedicated to TriggerSetup and a manual run would
    // race the lifecycle CAS) AND when the project has infra nodes
    // whose sidecars aren't running (the run would fail at sidecar
    // /outputs anyway; surface as "not available" rather than let
    // it through with a confusing error).
    if lifecycle.status != ProjectStatus::Activating
        && (!has_infra || infra_running)
    {
        out.push("run".to_string());
    }

    match lifecycle.status {
        ProjectStatus::Active => {
            out.push("deactivate".to_string());
            if drift.source_drift {
                out.push("resync".to_string());
            }
        }
        ProjectStatus::Activating => {
            // Mid-activate: the only legal action is cancel.
            // Deactivate/run/resync are gated until status reaches
            // Active (TriggerSetup completes) or Inactive (cancel
            // completes).
            out.push("cancel_activate".to_string());
        }
        ProjectStatus::Deactivating => {
            // Mid-deactivate: the user can either give up on the
            // wait (cancel_running, drain finishes immediately) or
            // change their mind (resume_active rolls back into
            // Active). Activate also stays in the verb list as an
            // alias for resume_active so existing UIs keep working.
            if running_count > 0 {
                out.push("cancel_running".to_string());
            }
            out.push("resume_active".to_string());
        }
        ProjectStatus::Registered | ProjectStatus::Inactive => {
            if !has_infra || infra_running {
                let has_preserved =
                    preservation.parked + preservation.suspended > 0;
                if has_preserved
                    && lifecycle.status == ProjectStatus::Inactive
                {
                    out.push("reactivate".to_string());
                } else {
                    out.push("activate".to_string());
                }
            }
        }
    }

    // Infra controls share the worker pool indirectly (some infra
    // ops re-run TriggerSetup on success). Hide them during
    // Activating so the user can't kick off conflicting infra
    // changes mid-activate.
    if has_infra && lifecycle.status != ProjectStatus::Activating {
        if infra_running {
            out.push("infra_stop".to_string());
            out.push("infra_terminate".to_string());
            if drift.infra_drift {
                out.push("infra_upgrade".to_string());
            }
        } else {
            out.push("infra_start".to_string());
            if infra_rollup == "stopped" || infra_rollup == "partial" {
                out.push("infra_terminate".to_string());
            }
        }
    }

    out
}

/// Body for `POST /projects/{id}/activate`. Optional `sourceHash`
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
    #[serde(default, rename = "sourceHash", alias = "source_hash")]
    pub source_hash: Option<String>,
    #[serde(default, rename = "infraHash", alias = "infra_hash")]
    pub infra_hash: Option<String>,
    #[serde(default, rename = "reactivateChoice", alias = "reactivate_choice")]
    pub reactivate_choice: Option<String>,
}

pub async fn activate(
    State(state): State<DispatcherState>,
    Path(id_str): Path<String>,
    body: Option<Json<ActivateRequest>>,
) -> Result<Json<ActivateResponse>, (StatusCode, String)> {
    let id = id_str
        .parse::<uuid::Uuid>()
        .map_err(|_| (StatusCode::BAD_REQUEST, "bad id".into()))?;
    let (source_hash, infra_hash, reactivate_choice) = match body {
        Some(Json(b)) => (b.source_hash, b.infra_hash, b.reactivate_choice),
        None => (None, None, None),
    };
    if let Some(hash) = source_hash.as_deref() {
        state.projects.set_running_source_hash(id, hash).await;
    }
    if let Some(hash) = infra_hash.as_deref() {
        state.projects.set_running_infra_hash(id, hash).await;
    }

    let project = state
        .projects
        .project(id)
        .await
        .ok_or_else(|| (StatusCode::NOT_FOUND, "project not found".into()))?;
    let project_id = id.to_string();

    // Pre-flight: every requires_infra node must be provisioned
    // AND in the Running state. A stopped sidecar is just as bad
    // as a missing one from the trigger-setup subgraph's point of
    // view (the worker will try to query /outputs and fail).
    let mut missing: Vec<String> = Vec::new();
    for node in project.nodes.iter().filter(|n| n.requires_infra) {
        let live = crate::infra::handle_if_running(&state.pg_pool, &project_id, &node.id)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("infra_pod: {e}")))?;
        if live.is_none() {
            missing.push(node.id.clone());
        }
    }
    if !missing.is_empty() {
        return Err((
            StatusCode::PRECONDITION_REQUIRED,
            format!(
                "infra not running for: {}. Run `weft infra start` first.",
                missing.join(", ")
            ),
        ));
    }

    // Reactivate-choice pre-flight. The default
    // (execute_parked_keep_suspended) is a no-op here: the drain
    // pass at the end of activate already replays every queued fire
    // through dispatch_listener_outcome.
    let choice = reactivate_choice.as_deref().unwrap_or("execute_parked_keep_suspended");
    match choice {
        "execute_parked_keep_suspended" => {}
        "keep_suspended_only" => {
            sqlx::query(
                "UPDATE signal SET parked_fires = '[]'::jsonb \
                 WHERE project_id = $1 AND jsonb_array_length(parked_fires) > 0",
            )
            .bind(&project_id)
            .execute(&state.pg_pool)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("clear parked: {e}")))?;
        }
        "wipe_all" => {
            wipe_project_signals(&state, &project_id).await?;
        }
        other => {
            return Err((
                StatusCode::BAD_REQUEST,
                format!(
                    "unknown reactivate_choice '{other}'; must be one of: \
                     execute_parked_keep_suspended, keep_suspended_only, wipe_all"
                ),
            ));
        }
    }

    // Hold a per-tenant keep-alive lease (SHARED OP-lock) for the
    // entire activate window. This fences the listener reaper out
    // for every step: TriggerSetup spawn, register_signal task
    // execution, drain pass. Without this lease the reaper can
    // observe a freshly-spawned listener with zero signals + zero
    // in-flight tasks (the activate-spawned worker may not have
    // reached its first ctx.register_signal yet) and kill it,
    // leaving the next register POST on a dead Service.
    let tenant = state.tenant_router.tenant_for_project(&project_id);
    let namespace = state.namespace_mapper.namespace_for(&tenant);
    let keep_alive =
        crate::listener::ActivateKeepAlive::acquire(&state.pg_pool, &tenant)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("keep-alive: {e}")))?;

    // Sweep any prior TriggerSetup colors that leaked from a
    // failed previous activate (rollback's cancel_color may have
    // failed; the per-tenant keep_alive lock guarantees no
    // concurrent in-flight TriggerSetup in this tenant, so any
    // non-terminal trigger_setup color we see here is orphaned).
    sweep_orphan_trigger_setup_colors(&state, &project_id)
        .await
        .map_err(|e| {
            (StatusCode::INTERNAL_SERVER_ERROR, format!("sweep orphan ts colors: {e}"))
        })?;

    // Flip status=Activating up-front. The gate parks fires while
    // we set up triggers (the listener may not yet have every
    // signal registered, so relaying could 404 / race). The drain
    // at the end replays every parked payload through the
    // post-CAS Active gate.
    //
    // Use full set_lifecycle (not CAS): we accept any prior status
    // (Inactive / Registered / Active for re-activate from active).
    state
        .projects
        .set_lifecycle(id, &crate::project_store::ProjectLifecycle::activating())
        .await;

    // Run TriggerSetup. Its register_signal tasks UPSERT entry
    // rows on (project_id, node_id), so existing rows from before
    // a deactivate get spec_json/mount_path/auth refreshed in
    // place; the token survives, so any parked_fires still
    // attached to it drain cleanly in the loop below.
    let seeds = compute_trigger_setup_seeds(&project);
    if !seeds.is_empty() {
        match run_trigger_setup(&state, id, seeds).await {
            Ok(()) => {}
            Err((status, msg, ts_color)) => {
                // Atomic rollback: same body as cancel-activate.
                // If cleanup itself fails the original failure
                // still dominates the response; we log loudly.
                if let Err((rb_status, rb_msg)) =
                    wipe_activating_state(&state, id, &project_id, ts_color).await
                {
                    tracing::error!(
                        target: "weft_dispatcher::activate",
                        project_id = %id,
                        rb_status = %rb_status,
                        rb_error = %rb_msg,
                        "activate rollback: wipe_activating_state failed; manual cleanup needed"
                    );
                }
                keep_alive.release().await;
                return Err((status, msg));
            }
        }
    }

    // Drop orphan entry rows: nodes that previously had triggers
    // but no longer do (user edited the project source while
    // deactivated). TriggerSetup just upserted every still-existing
    // entry; anything left over is dead.
    drop_orphan_entry_rows(&state, &project_id, &project)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("drop_orphan_entry_rows: {e}")))?;

    // Reconcile the listener's in-RAM registry with the durable
    // signal table. TriggerSetup just re-registered entry triggers
    // via its worker, but resume signals belong to suspended
    // executions whose workers are long gone (nothing replays
    // `ctx.await_signal`). Without this step, post-park resume
    // fires would 404 at the listener. The listener's /rehydrate is
    // idempotent: entries already in its registry stay untouched,
    // so this is safe to call even when the listener was never
    // reaped.
    state
        .listeners
        .with_listener(
            &tenant,
            &namespace,
            state.listener_backend.as_ref(),
            &state.pg_pool,
            state.pod_id.as_str(),
            |handle| async move { crate::listener::rehydrate(&handle).await },
        )
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("listener rehydrate: {e}")))?;

    // Flip Activating → Active. CAS guards against a concurrent
    // cancel_activate that flipped us to Inactive between the
    // TriggerSetup completion and this point: in that case we
    // surrender: the cancel already wiped our signals, so
    // proceeding with the drain would replay payloads against a
    // half-torn-down state.
    let active = crate::project_store::ProjectLifecycle::active();
    let cas_ok = state
        .projects
        .cas_lifecycle(id, crate::project_store::ProjectStatus::Activating, &active)
        .await;
    if !cas_ok {
        keep_alive.release().await;
        return Err((
            StatusCode::CONFLICT,
            "activate raced with cancel_activate; project is now Inactive".into(),
        ));
    }

    // Drain every queued fire that survived the inactive window.
    // Single loop, kind-agnostic: dispatch_listener_outcome routes
    // Resume vs Entry vs Drop based on what the listener returns,
    // exactly like a live fire. Runs after the Active CAS so the
    // gate relays instead of re-queueing.
    drain_parked_fires(&state, &project_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("drain_parked_fires: {e}")))?;

    // Activate window done. Release the keep-alive lease; the
    // listener now stays alive purely on signal-row presence (the
    // reaper's only kill condition is "zero signals AND no in-flight
    // operation").
    keep_alive.release().await;

    let urls = collect_listener_urls(&state, &project_id).await.map_err(|e| {
        (StatusCode::INTERNAL_SERVER_ERROR, format!("collect_listener_urls: {e}"))
    })?;
    for url in &urls {
        state
            .events
            .publish(DispatcherEvent::TriggerUrlChanged {
                project_id: project_id.clone(),
                node_id: url.node_id.clone(),
                url: url.url.clone(),
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
/// Per row:
///   1. Atomically claim: UPDATE drain_claimed_at = now WHERE
///      drain_claimed_at IS NULL. If 0 rows updated, another pass
///      raced us and won; skip.
///   2. Pop-then-dispatch loop: read head (`parked_fires -> 0`),
///      dispatch, then pop it (`parked_fires - 0`). One element
///      commits at a time, so a mid-loop failure leaves the unsent
///      remainder intact in FIFO order for the next activate to
///      retry. Appends from concurrent fires land at the tail; index
///      0 stays stable across the pop-then-dispatch window, so the
///      read-then-pop is race-free without a separate per-element
///      claim.
///   3. Release the row claim regardless of outcome.
///
/// A pod crash between steps 1 and 3 leaves drain_claimed_at set;
/// the next activate's pre-pass releases stale claims older than
/// the threshold so the row becomes drainable again.
async fn drain_parked_fires(
    state: &DispatcherState,
    project_id: &str,
) -> anyhow::Result<()> {
    use sqlx::Row;

    // Pre-pass: release stale claims. A crashed pod could have left
    // drain_claimed_at set; older than DRAIN_CLAIM_STALE_SECS means
    // any owner is definitely dead.
    const DRAIN_CLAIM_STALE_SECS: i64 = 300;
    // Bound on the snapshot-loop below. A fire whose
    // `lookup_signal_routing` saw status=Activating just before the
    // CAS to Active commits will append to parked_fires AFTER our
    // first snapshot. We rerun the snapshot until it returns empty
    // so those fires drain in the same activate pass. Cap at 3
    // iterations so a stuck token (something appending faster than
    // we can dispatch) cannot livelock the activate handler; an
    // operator-visible failure beats an infinite loop.
    const MAX_DRAIN_PASSES: u32 = 3;
    let now = unix_now() as i64;
    sqlx::query(
        "UPDATE signal SET drain_claimed_at_unix = NULL \
         WHERE project_id = $1 \
           AND drain_claimed_at_unix IS NOT NULL \
           AND drain_claimed_at_unix < $2",
    )
    .bind(project_id)
    .bind(now - DRAIN_CLAIM_STALE_SECS)
    .execute(&state.pg_pool)
    .await?;

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

/// Drain one signal row's queue. Internal helper: caller has already
/// established the row has fires and is unclaimed. Idempotent under
/// retry: if the row's queue becomes empty mid-loop (e.g. another
/// drain raced; or our pop sequence finished), we exit cleanly.
async fn drain_one_token(
    state: &DispatcherState,
    project_id: &str,
    token: &str,
) -> anyhow::Result<()> {
    use sqlx::Row;

    // Atomic claim. If another pass beat us, bail.
    let claim = sqlx::query(
        "UPDATE signal SET drain_claimed_at_unix = $2 \
         WHERE token = $1 AND drain_claimed_at_unix IS NULL",
    )
    .bind(token)
    .bind(unix_now() as i64)
    .execute(&state.pg_pool)
    .await?;
    if claim.rows_affected() == 0 {
        return Ok(());
    }

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

        match crate::api::signal::dispatch_listener_outcome(
            state,
            token,
            project_id,
            fire.payload,
            Some(&fire.id),
        )
        .await
        {
            Ok(_) => {
                // Pop the head. `- 0::int` is the unambiguous form
                // of `jsonb_array - integer_index`; plain `- 0`
                // would be ambiguous with `jsonb_object - text_key`
                // under some inference paths.
                sqlx::query(
                    "UPDATE signal SET parked_fires = parked_fires - 0::int \
                     WHERE token = $1",
                )
                .bind(token)
                .execute(&state.pg_pool)
                .await?;
            }
            Err((status, msg)) => {
                // Leave the unsent remainder in place (FIFO order
                // preserved) and break. Next activate retries.
                tracing::warn!(
                    target: "weft_dispatcher::activate",
                    project_id, token, %status, error = %msg,
                    "drain_parked_fires: dispatch failed; leaving \
                     remainder queued for retry"
                );
                break Err(anyhow::anyhow!("dispatch failed: {msg}"));
            }
        }
    };

    // Release the claim regardless of outcome.
    sqlx::query(
        "UPDATE signal SET drain_claimed_at_unix = NULL \
         WHERE token = $1",
    )
    .bind(token)
    .execute(&state.pg_pool)
    .await?;

    // Surface the dispatch error to the activate caller so the
    // operator sees the failure. Subsequent rows in the snapshot
    // still won't be drained this pass; the next activate gets them.
    outcome
}

/// Sweep entry-trigger rows whose node no longer exists in the
/// project source. Called after TriggerSetup so any node the user
/// removed-while-parked has its leftover signal row dropped.
/// Resume rows (per-suspension) skip this: the corresponding
/// suspended execution is the source of truth for them.
/// Cancel every non-terminal TriggerSetup color for `project_id`.
/// Called at the top of activate so a previous activate's leaked
/// trigger-setup color (cancel_color failed during rollback) gets
/// cleaned up before we spawn a new one. The per-tenant
/// activate-keepalive lock guarantees no in-flight TriggerSetup
/// runs concurrently in the same tenant, so anything we find here
/// is by definition orphaned.
async fn sweep_orphan_trigger_setup_colors(
    state: &DispatcherState,
    project_id: &str,
) -> anyhow::Result<()> {
    use sqlx::Row;
    let rows = sqlx::query(
        "SELECT ec.color FROM execution_color ec \
         WHERE ec.project_id = $1 AND ec.phase = 'trigger_setup' \
           AND NOT EXISTS ( \
             SELECT 1 FROM exec_event e \
             WHERE e.color = ec.color \
               AND e.kind IN ('execution_completed', 'execution_failed', 'execution_cancelled') \
           )",
    )
    .bind(project_id)
    .fetch_all(&state.pg_pool)
    .await?;
    if rows.is_empty() {
        return Ok(());
    }
    for row in rows {
        let color_str: String = row.try_get("color")?;
        let color: weft_core::Color = match color_str.parse() {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(
                    target: "weft_dispatcher::activate",
                    project_id, %color_str, error = %e,
                    "skipping orphan TS color with bad uuid"
                );
                continue;
            }
        };
        tracing::info!(
            target: "weft_dispatcher::activate",
            project_id, %color,
            "cancelling orphan trigger_setup color from prior activate"
        );
        crate::api::execution::cancel_color(state, color).await?;
    }
    Ok(())
}

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

/// Cancel-activate / TriggerSetup-failure cleanup. Runs the
/// canonical wipe (cancel TS color + drop all signal rows) and
/// then CAS Activating → Inactive (wiped). Idempotent: a partial
/// run leaves the next call with less work.
///
/// `ts_color` is the TriggerSetup color the in-flight activate
/// spawned (Some when called from the rollback path inside
/// `run_trigger_setup`'s error branch). When called from
/// cancel_activate the helper looks up the orphan TS color via
/// the same mechanism `sweep_orphan_trigger_setup_colors` uses.
async fn wipe_activating_state(
    state: &DispatcherState,
    id: uuid::Uuid,
    project_id: &str,
    ts_color: Option<weft_core::Color>,
) -> Result<(), (StatusCode, String)> {
    // CAS Activating → Inactive FIRST. This races a concurrent
    // activate-success that's about to CAS Activating → Active:
    // exactly one of the two CASes lands. If we lose, activate
    // already finished cleanly and we must NOT wipe signals
    // (we'd nuke a freshly-active project's triggers). If we win,
    // status is Inactive and no future activate-CAS can touch it
    // until the user re-activates.
    let won = state
        .projects
        .cas_lifecycle(
            id,
            crate::project_store::ProjectStatus::Activating,
            &crate::project_store::ProjectLifecycle::wiped(),
        )
        .await;
    if !won {
        // Activate raced us and won. Nothing to clean up.
        return Ok(());
    }
    if let Some(c) = ts_color {
        crate::api::execution::cancel_color(state, c)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("cancel_color: {e}")))?;
    }
    // Sweep any other in-flight TS colors (orphans from prior
    // attempts, plus the in-flight one if `ts_color` was None).
    sweep_orphan_trigger_setup_colors(state, project_id)
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
        .list_non_terminal_colors_for_project(project_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("list colors: {e}")))?;
    for color in colors {
        crate::api::execution::cancel_color(state, color)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("cancel_color: {e}")))?;
    }
    crate::api::signal::delete_signals_for_project(state, project_id).await
}

/// Body for `POST /projects/{id}/deactivate`.
///
/// `preservationMode` controls what survives the inactive window:
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
#[derive(Debug, Default, Deserialize)]
pub struct DeactivateRequest {
    #[serde(default, rename = "preservationMode", alias = "preservation_mode")]
    pub preservation_mode: Option<String>,
    /// Grace window in minutes; only meaningful when
    /// preservationMode = "hibernate". Defaults to 15.
    #[serde(default, rename = "graceMinutes", alias = "grace_minutes")]
    pub grace_minutes: Option<u32>,
    /// `cancel` | `wait`. Default: `wait`.
    #[serde(default, rename = "runningPolicy", alias = "running_policy")]
    pub running_policy: Option<String>,
}

const DEFAULT_HIBERNATE_GRACE_MINUTES: u32 = 15;

pub async fn deactivate(
    State(state): State<DispatcherState>,
    Path(id_str): Path<String>,
    body: Option<Json<DeactivateRequest>>,
) -> Result<StatusCode, (StatusCode, String)> {
    let id = id_str
        .parse::<uuid::Uuid>()
        .map_err(|_| (StatusCode::BAD_REQUEST, "bad id".into()))?;
    let body = body.map(|Json(b)| b).unwrap_or_default();
    let mode = body.preservation_mode.as_deref().unwrap_or("wipe");
    if !["wipe", "hibernate", "park"].contains(&mode) {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("invalid preservationMode '{mode}'; must be wipe|hibernate|park"),
        ));
    }
    let running_policy = body.running_policy.as_deref().unwrap_or("wait");
    if !["wait", "cancel"].contains(&running_policy) {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("invalid runningPolicy '{running_policy}'; must be wait|cancel"),
        ));
    }
    // wipe + wait is meaningless: wipe means "drop everything
    // including suspended". Waiting for executions to drain before
    // dropping them contradicts the intent. Force-pair them.
    if mode == "wipe" && running_policy == "wait" {
        return Err((
            StatusCode::BAD_REQUEST,
            "wipe requires runningPolicy=cancel; waiting before wiping is contradictory".into(),
        ));
    }
    let grace_minutes = body
        .grace_minutes
        .unwrap_or(DEFAULT_HIBERNATE_GRACE_MINUTES);
    let existed = deactivate_project_with_mode(
        &state,
        id,
        mode,
        grace_minutes,
        running_policy,
    )
    .await?;
    if !existed {
        return Err((StatusCode::NOT_FOUND, "project not found".into()));
    }
    Ok(StatusCode::NO_CONTENT)
}

/// `POST /projects/{id}/resync`. Atomic deactivate-then-activate
/// against (optionally) fresh source / infra hashes. Refuses with
/// 412 if project has infra nodes and infra isn't running (the
/// deactivate step still runs; the user is told to start infra
/// before reclicking activate).
pub async fn resync(
    State(state): State<DispatcherState>,
    Path(id_str): Path<String>,
    body: Option<Json<ActivateRequest>>,
) -> Result<Json<ActivateResponse>, (StatusCode, String)> {
    let id = id_str
        .parse::<uuid::Uuid>()
        .map_err(|_| (StatusCode::BAD_REQUEST, "bad id".into()))?;
    let project = state
        .projects
        .project(id)
        .await
        .ok_or_else(|| (StatusCode::NOT_FOUND, "project not found".into()))?;
    let project_id = id.to_string();

    // 1. Deactivate (always): drop all signals + cancel in-flight
    //    + flip to inactive. Idempotent if not active.
    deactivate_project(&state, id).await?;

    // 2. Reactivate precondition: every requires_infra node must be
    //    running. If not, leave the project deactivated and surface
    //    a clear error. The user starts infra and clicks Activate.
    let mut missing: Vec<String> = Vec::new();
    for node in project.nodes.iter().filter(|n| n.requires_infra) {
        let live = crate::infra::handle_if_running(&state.pg_pool, &project_id, &node.id)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("infra_pod: {e}")))?;
        if live.is_none() {
            missing.push(node.id.clone());
        }
    }
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
    //    + atomic-cleanup-on-failure semantics are identical.
    activate(State(state), Path(id_str), body).await
}

/// Shared deactivation logic. Used both by the explicit
/// `/deactivate` endpoint and auto-called from `infra stop` /
/// `infra terminate` / project removal: a stopped sidecar leaves
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
    deactivate_project_with_mode(state, id, "wipe", 0, "cancel").await
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
///               `journal_bridge::terminal_cleanup`. Wipe with
///               `wait` does NOT cancel suspended executions (they
///               only get cancelled when status finally flips to
///               Inactive, but with wipe-target the row is gone by
///               then; in practice wipe + wait still cancels
///               suspended at the moment the drain completes
///               because nothing keeps them alive). Hibernate /
///               park + wait leave suspended alone forever.
pub async fn deactivate_project_with_mode(
    state: &DispatcherState,
    id: uuid::Uuid,
    mode: &str,
    grace_minutes: u32,
    running_policy: &str,
) -> Result<bool, (StatusCode, String)> {
    use crate::project_store::{ProjectLifecycle, ProjectStatus};

    let project_id = id.to_string();

    // Resolve the target lifecycle (the axes the gate must be
    // showing on completion). For wait mode we wrap it in
    // `deactivating_to(target)` so status=Deactivating but the
    // gate axes are already the target's.
    let target = match mode {
        "wipe" => ProjectLifecycle::wiped(),
        "hibernate" => {
            let deadline = unix_now() as i64 + (grace_minutes as i64) * 60;
            ProjectLifecycle::hibernating(deadline)
        }
        "park" => ProjectLifecycle::parked(),
        _ => unreachable!("validated upstream"),
    };

    // For wipe (always paired with cancel), do the cancel + drop
    // FIRST while the listener is still alive on the row; THEN
    // flip the lifecycle to wiped. The reaper would otherwise
    // observe accepting=false and start killing the listener
    // mid-cleanup.
    if mode == "wipe" {
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
                .unregister_many_if_alive(&state.pg_pool, &signals)
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
    let lifecycle_to_set = if running_policy == "wait" {
        ProjectLifecycle::deactivating_to(target)
    } else if mode != "wipe" {
        cancel_running_non_suspended(state, &project_id).await?;
        target
    } else {
        // wipe + cancel: rows + executions already gone above.
        target
    };

    let existed = state
        .projects
        .set_lifecycle(id, &lifecycle_to_set)
        .await;
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
        let running_now = running_count(state, &project_id)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("running_count: {e}")))?;
        if running_policy == "wait"
            && lifecycle_to_set.status == ProjectStatus::Deactivating
            && running_now == 0
        {
            let _ = state
                .projects
                .cas_status(id, ProjectStatus::Deactivating, ProjectStatus::Inactive)
                .await;
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
    Path(id_str): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    let _id = id_str
        .parse::<uuid::Uuid>()
        .map_err(|_| (StatusCode::BAD_REQUEST, "bad id".into()))?;
    let project_id = id_str;
    cancel_running_non_suspended(&state, &project_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Cancel an in-flight `activate` (status=Activating). Wipes every
/// signal row registered so far, cancels the TriggerSetup color via
/// the orphan sweep, and CAS-flips status Activating → Inactive.
///
/// 412 if status isn't Activating: the user (or stale UI) clicked
/// cancel against an already-active or already-inactive project.
pub async fn cancel_activate(
    State(state): State<DispatcherState>,
    Path(id_str): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    use crate::project_store::ProjectStatus;
    let id = id_str
        .parse::<uuid::Uuid>()
        .map_err(|_| (StatusCode::BAD_REQUEST, "bad id".into()))?;
    let project_id = id_str;
    let lifecycle = state.projects.lifecycle(id).await;
    if lifecycle.status != ProjectStatus::Activating {
        return Err((
            StatusCode::PRECONDITION_FAILED,
            format!(
                "cancel-activate requires status=activating, got {}",
                lifecycle.status.as_str()
            ),
        ));
    }
    // ts_color = None tells the helper to discover the in-flight TS
    // color via sweep_orphan_trigger_setup_colors (the running
    // activate's color shows up there because it has no terminal
    // event yet).
    wipe_activating_state(&state, id, &project_id, None).await?;
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
async fn cancel_running_non_suspended(
    state: &DispatcherState,
    project_id: &str,
) -> Result<(), (StatusCode, String)> {
    let suspended_colors = suspended_color_set(state, project_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("suspended_color_set: {e}")))?;
    let colors = state
        .journal
        .list_non_terminal_colors_for_project(project_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("list colors: {e}")))?;
    for color in colors {
        if suspended_colors.contains(&color) {
            continue;
        }
        crate::api::execution::cancel_color(state, color)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("cancel_color: {e}")))?;
    }
    Ok(())
}

/// Set of colors holding at least one resume signal: the canonical
/// "this execution is suspended" record (the engine doesn't journal
/// a terminal event for stalls).
async fn suspended_color_set(
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
/// has right now. `0` means deactivate-with-wait can flip status to
/// Inactive immediately.
pub(crate) async fn running_count(
    state: &DispatcherState,
    project_id: &str,
) -> anyhow::Result<usize> {
    let suspended_colors = suspended_color_set(state, project_id).await?;
    let colors = state
        .journal
        .list_non_terminal_colors_for_project(project_id)
        .await?;
    Ok(colors
        .into_iter()
        .filter(|c| !suspended_colors.contains(c))
        .count())
}

/// Spawn a worker for the TriggerSetup sub-execution and block
/// until it settles. On error returns the trigger-setup color so
/// the caller can scope cleanup to it (cancel just THIS execution,
/// don't touch suspended/running work from prior cycles).
async fn run_trigger_setup(
    state: &DispatcherState,
    project_id_uuid: uuid::Uuid,
    seeds: Vec<RootSeed>,
) -> Result<(), (StatusCode, String, Option<weft_core::Color>)> {
    let project_id = project_id_uuid.to_string();
    let color = uuid::Uuid::new_v4();
    let now = unix_now();

    state
        .journal
        .record_event(&weft_journal::ExecEvent::ExecutionStarted {
            color,
            project_id: project_id.clone(),
            entry_node: seeds[0].node_id.clone(),
            phase: weft_core::context::Phase::TriggerSetup,
            at_unix: now,
        })
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("journal: {e}"), Some(color)))?;
    for seed in &seeds {
        let _ = state
            .journal
            .record_event(&weft_journal::ExecEvent::PulseSeeded {
                color,
                pulse_id: seed.pulse_id.clone(),
                node_id: seed.node_id.clone(),
                port: "__seed__".to_string(),
                lane: Vec::new(),
                value: seed.value.clone(),
                at_unix: now,
            })
            .await;
    }

    // Subscribe BEFORE enqueueing so the worker can't beat us to
    // the completion event.
    let mut events = state.events.subscribe_project(&project_id).await;

    let tenant = state.tenant_router.tenant_for_project(&project_id);
    crate::task_kinds::execute::enqueue_execute(
        &state.pg_pool,
        &project_id,
        color,
        Some(tenant.as_str()),
    )
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("enqueue execute: {e}"), Some(color)))?;

    // No server-side deadline. Trigger setup spans worker pod
    // spawn + image pull + fold + run + bridge wakeup; on a cold
    // cluster the legitimate path is just slow, and a hard timeout
    // would surface that as a 504 even though the work is still in
    // flight. The CLI / extension is the right layer to choose a
    // client-side patience budget.
    loop {
        match events.recv().await {
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
                    Some(color),
                ));
            }
            Ok(crate::events::DispatcherEvent::ExecutionCancelled { color: c, reason, .. })
                if c == color =>
            {
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("trigger setup cancelled: {reason}"),
                    Some(color),
                ));
            }
            Ok(_) => continue,
            Err(_) => {
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "trigger setup event stream closed".into(),
                    Some(color),
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
        if let Some(url) = meta.public_url(&state.public_base_url) {
            out.push(ActivationUrl {
                node_id: meta.node_id.clone(),
                url,
            });
        }
    }
    Ok(out)
}


fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod trigger_seed_tests {
    use super::*;

    /// Build a minimal ProjectDefinition from a JSON spec. Tests
    /// only care about node id, trigger status, output status, and
    /// edges; everything else is defaulted.
    fn project(nodes: &[(&str, bool, bool)], edges: &[(&str, &str)]) -> ProjectDefinition {
        let mut n_json = Vec::new();
        for (id, is_trigger, is_output) in nodes {
            n_json.push(serde_json::json!({
                "id": id,
                "nodeType": "T",
                "label": null,
                "config": {},
                "position": { "x": 0, "y": 0 },
                "features": {
                    "isOutputDefault": is_output,
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
            "name": "t",
            "description": null,
            "nodes": n_json,
            "edges": e_json,
            "groups": []
        });
        serde_json::from_value(body).expect("valid test project")
    }

    fn ids(seeds: &[RootSeed]) -> Vec<String> {
        let mut v: Vec<String> = seeds.iter().map(|s| s.node_id.clone()).collect();
        v.sort();
        v
    }

    #[test]
    fn trigger_only_upstream_node_is_skipped() {
        // A ──► TriggerX ──► B ──► Out
        // A feeds only the trigger; nothing past the trigger reaches Out
        // through A, so A must not run at fire time.
        let p = project(
            &[
                ("a", false, false),
                ("trigger_x", true, false),
                ("b", false, false),
                ("out", false, true),
            ],
            &[("a", "trigger_x"), ("trigger_x", "b"), ("b", "out")],
        );
        let seeds = compute_trigger_seeds(&p, "trigger_x", &Value::String("payload".into()));
        assert_eq!(
            ids(&seeds),
            vec!["trigger_x".to_string()],
            "only the firing trigger should be a seed"
        );
        assert_eq!(seeds[0].value, Value::String("payload".into()));
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
                ("a", false, false),
                ("trigger_x", true, false),
                ("b", false, false),
                ("c", false, false),
                ("out", false, true),
            ],
            &[
                ("a", "trigger_x"),
                ("a", "b"),
                ("trigger_x", "c"),
                ("c", "out"),
                ("b", "out"),
            ],
        );
        let seeds = compute_trigger_seeds(&p, "trigger_x", &Value::String("payload".into()));
        assert_eq!(
            ids(&seeds),
            vec!["a".to_string(), "trigger_x".to_string()],
            "A must run via its non-trigger path; trigger carries payload"
        );
        for s in &seeds {
            if s.node_id == "trigger_x" {
                assert_eq!(s.value, Value::String("payload".into()));
            } else {
                assert_eq!(s.value, Value::Null);
            }
        }
    }

    #[test]
    fn non_firing_triggers_in_subgraph_get_null() {
        // TriggerX ──► Out ◄── TriggerY
        // Firing TriggerX: TriggerY still gets seeded (with null)
        // because it's reachable upstream from Out.
        let p = project(
            &[
                ("trigger_x", true, false),
                ("trigger_y", true, false),
                ("out", false, true),
            ],
            &[("trigger_x", "out"), ("trigger_y", "out")],
        );
        let seeds = compute_trigger_seeds(&p, "trigger_x", &Value::String("fire".into()));
        let sorted = ids(&seeds);
        assert_eq!(sorted, vec!["trigger_x".to_string(), "trigger_y".to_string()]);
        for s in &seeds {
            if s.node_id == "trigger_x" {
                assert_eq!(s.value, Value::String("fire".into()));
            } else {
                assert_eq!(s.value, Value::Null);
            }
        }
    }

    #[test]
    fn no_output_downstream_returns_empty() {
        // TriggerX with no reachable output = nothing to run.
        let p = project(
            &[("trigger_x", true, false), ("dead_end", false, false)],
            &[("trigger_x", "dead_end")],
        );
        let seeds = compute_trigger_seeds(&p, "trigger_x", &Value::Null);
        assert!(seeds.is_empty());
    }

    #[test]
    fn firing_non_trigger_returns_empty() {
        // Defensive: caller must never pass a non-trigger id. We
        // return empty rather than silently fabricating seeds.
        let p = project(
            &[("a", false, false), ("out", false, true)],
            &[("a", "out")],
        );
        let seeds = compute_trigger_seeds(&p, "a", &Value::Null);
        assert!(seeds.is_empty());
    }
}
