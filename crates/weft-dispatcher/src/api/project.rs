//! Project lifecycle HTTP handlers. `POST /projects` registers a
//! project; `POST /projects/{id}/run` kicks off a fresh execution.
//! `weft run` on the CLI calls these.

use std::collections::HashSet;

use axum::{extract::{Path, State}, http::StatusCode, Json};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use weft_core::project::EdgeIndex;
use weft_core::ProjectDefinition;

use weft_core::primitive::RootSeed;
use crate::events::DispatcherEvent;
use crate::state::DispatcherState;

#[derive(Debug, Serialize, Deserialize)]
pub struct ProjectSummary {
    pub id: String,
    pub name: String,
    pub status: String,
}

pub async fn list(
    State(state): State<DispatcherState>,
) -> Result<Json<Vec<ProjectSummary>>, (StatusCode, String)> {
    let items = state
        .projects
        .list()
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("list projects: {e}")))?;
    Ok(Json(
        items
            .into_iter()
            .map(|p| ProjectSummary {
                id: p.id.to_string(),
                name: p.name,
                status: p.status.as_str().to_string(),
            })
            .collect(),
    ))
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
    pub definition: ProjectDefinition,
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
}

fn register_internal_error(msg: String) -> (StatusCode, Json<RegisterError>) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(RegisterError { error: msg }),
    )
}

/// Register an already compiled + enriched project. The CLI does all
/// node-aware work (compile + enrich against the local `nodes/`) and
/// hands over the finished `ProjectDefinition`; the dispatcher only
/// stores it and provisions the namespace bundle.
pub async fn register(
    State(state): State<DispatcherState>,
    Json(req): Json<RegisterRequest>,
) -> Result<Json<ProjectSummary>, (StatusCode, Json<RegisterError>)> {
    let mut project = req.definition;
    project.id = req.id;
    project.name = req.name;

    let project_id_str = project.id.to_string();
    let tenant = state.tenant_router.tenant_for_project(&project_id_str);
    let tenant_namespace = state.namespace_mapper.namespace_for(&tenant);
    // The tenant namespace is a hard prerequisite, not best-effort:
    // it holds `weft-listener-sa` / `weft-infra-supervisor-sa` (which
    // the project-namespace RoleBindings below reference as subjects)
    // plus the broker's TokenReview registry row. If it fails to land,
    // fail register loudly rather than create a project namespace with
    // RoleBindings pointing at non-existent SAs and a tenant with no
    // registry row. Same policy as the project-namespace ensure below.
    crate::tenant_namespace::ensure_tenant_namespace(
        &state.pg_pool,
        &*state.kube,
        &tenant_namespace,
        tenant.as_str(),
        crate::tenant_namespace::ClusterCidrs {
            pod_cidr: &state.cluster_pod_cidr,
            service_cidr: &state.cluster_service_cidr,
            ingress_namespace: &state.cluster_ingress_namespace,
        },
    )
    .await
    .map_err(|e| register_internal_error(format!("ensure_tenant_namespace ({tenant_namespace}): {e}")))?;
    // Note: we no longer spawn the per-tenant supervisor Deployment
    // at register time. The supervisor is lazy: it gets applied the
    // first time the project's sync handler needs to enqueue an
    // Apply command. Projects that never use infra never spawn one.
    // The supervisor reaper kills idle supervisor Deployments when
    // a tenant has no live infra_node rows and no in-flight commands.
    // Per-project namespace bundle: namespace + worker/infra SAs +
    // NetworkPolicies + RoleBindings to the supervisor/listener
    // ClusterRoles.
    let project_namespace =
        crate::project_namespace::name_for(tenant.as_str(), &project_id_str);
    let project_namespace_args = crate::project_namespace::ProjectNamespaceArgs {
        project_id: &project_id_str,
        tenant_id: tenant.as_str(),
        namespace: &project_namespace,
        pod_cidr: &state.cluster_pod_cidr,
        service_cidr: &state.cluster_service_cidr,
        ingress_namespace: &state.cluster_ingress_namespace,
        tenant_namespace: &tenant_namespace,
    };
    // Namespace MUST land before we write the project row: every
    // downstream step (worker spawn, infra apply, listener attach)
    // assumes the namespace + its RBAC bundle exist. If kubectl
    // refuses, fail register loudly rather than insert a row that
    // points at a namespace nothing else can create.
    crate::project_namespace::ensure(&state.pg_pool, &*state.kube, &project_namespace_args)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(RegisterError {
                    error: format!(
                        "ensure_project_namespace {}: {e}",
                        project_namespace
                    ),
                }),
            )
        })?;
    let summary = state
        .projects
        .register(project, tenant.as_str(), &project_namespace)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(RegisterError {
                    error: format!("register: {e}"),
                }),
            )
        })?;
    if let Some(hash) = req.source_hash.as_deref() {
        state
            .projects
            .set_running_source_hash(summary.id, hash)
            .await
            .map_err(|e| register_internal_error(format!("set_running_source_hash: {e}")))?;
    }
    if let Some(hash) = req.infra_hash.as_deref() {
        state
            .projects
            .set_running_infra_hash(summary.id, hash)
            .await
            .map_err(|e| register_internal_error(format!("set_running_infra_hash: {e}")))?;
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
    let summary = state
        .projects
        .get(id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;
    Ok(Json(ProjectSummary {
        id: summary.id.to_string(),
        name: summary.name,
        status: summary.status.as_str().to_string(),
    }))
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
    Path(id): Path<String>,
    axum::extract::Query(query): axum::extract::Query<RemoveQuery>,
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
    // Tear down infra: issues a supervisor terminate command, waits
    // up to 120s for completion (unless --force), then drops all
    // infra_* rows and deletes the project namespace. MUST succeed:
    // if any of the DB cascade writes fail, the project row stays
    // (so a retry replays cleanly). Step 2 (supervisor wait) and
    // step 4 (namespace delete) inside `delete_project` are still
    // log-and-continue for the cluster-unreachable case; only DB
    // writes are fail-loud.
    crate::api::infra::delete_project(&state, id, query.force).await?;
    let removed = state
        .projects
        .remove(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("remove: {e}")))?;
    if removed {
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
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("project: {e}")))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "project definition missing".into()))?;
    let project_id = id.to_string();

    // Pre-flight: every `requires_infra` node must be Running. The
    // node body's `ctx.endpoint(...)` deep in execute() would
    // fail with a confusing "endpoint not available" otherwise.
    // Match the activate / reactivate pre-flight semantics so the
    // user sees the same actionable error from any entry point.
    let mut missing: Vec<String> = Vec::new();
    for node in project.nodes.iter().filter(|n| n.requires_infra) {
        let row = crate::infra_node::get(&state.pg_pool, &project_id, &node.id)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("infra_node: {e}")))?;
        let running = row
            .map(|r| r.status == crate::infra_node::InfraNodeStatus::Running)
            .unwrap_or(false);
        if !running {
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
    let now = crate::lease::now_unix() as u64;
    state
        .journal
        .record_event(&weft_journal::ExecEvent::ExecutionStarted {
            color,
            project_id: id.to_string(),
            entry_node: entry_node_for_journal.clone(),
            phase: weft_core::context::Phase::Fire,
            at_unix: now,
        })
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("journal: {e}")))?;
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
        state
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
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("journal: {e}")))?;
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

/// For each `requires_infra` node in the project, list the
/// `is_trigger` nodes that have it in their upstream closure.
///
/// Re-export from weft-core. The function lives there because both
/// the dispatcher (per-node safety check) and the broker
/// (supervisor_trigger_deps) need it; one definition, no clones.
pub use weft_core::project::compute_trigger_deps;

/// Mirror of [`compute_trigger_setup_seeds`] for `Phase::InfraSetup`.
///
/// Seeds are the roots of the upstream closure of every
/// `requires_infra` node : NOT the infra nodes themselves. Without
/// this, "text → compute_url → provision_infra" graphs would skip the
/// text/compute_url path, and the infra node's `provision()` body
/// wouldn't see those upstream values as input.
///
/// Returns an empty vec if the project has no infra nodes (the
/// caller short-circuits : no InfraSetup execution needed).
pub fn compute_infra_setup_seeds(project: &ProjectDefinition) -> Vec<RootSeed> {
    let edge_idx = EdgeIndex::build(project);
    let infra: Vec<String> = project
        .nodes
        .iter()
        .filter(|n| n.requires_infra)
        .map(|n| n.id.clone())
        .collect();
    if infra.is_empty() {
        return Vec::new();
    }
    let in_subgraph = upstream_closure(project, &edge_idx, &infra);
    roots_of(project, &edge_idx, &in_subgraph)
        .into_iter()
        .map(|id| RootSeed {
            node_id: id,
            pulse_id: uuid::Uuid::new_v4().to_string(),
            value: Value::Null,
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

/// Spawn a worker to run the InfraSetup sub-execution for every
/// `requires_infra` node in the project and wait for it to complete.
/// Seeds the upstream-closure roots so programmatic-infra patterns
/// (text → compute → infra) flow values into the infra body.
pub async fn run_infra_setup(
    state: &DispatcherState,
    project_id_uuid: uuid::Uuid,
) -> Result<(), (StatusCode, String)> {
    let project = state
        .projects
        .project(project_id_uuid)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("project: {e}")))?
        .ok_or((StatusCode::NOT_FOUND, "project not found".into()))?;
    let seeds = compute_infra_setup_seeds(&project);
    if seeds.is_empty() {
        return Ok(());
    }
    let project_id = project_id_uuid.to_string();
    let color = uuid::Uuid::new_v4();
    let now = crate::lease::now_unix() as u64;

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
        state
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
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("journal: {e}")))?;
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
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("get project: {e}")))?
        .ok_or((StatusCode::NOT_FOUND, "project not found".into()))?;
    let project = state
        .projects
        .project(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("project: {e}")))?
        .ok_or((StatusCode::NOT_FOUND, "project definition missing".into()))?;
    let project_id = id.to_string();
    let tenant = state.tenant_router.tenant_for_project(&project_id);
    let listener_running = state
        .listeners
        .is_alive(&tenant, &state.pg_pool)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("listener is_alive: {e}")))?;

    let infra_rows = crate::infra_node::list_for_project(&state.pg_pool, &project_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("infra_node: {e}")))?;
    let mut infra = Vec::new();
    for row in &infra_rows {
        let Some(node_type) = project
            .nodes
            .iter()
            .find(|n| n.id == row.node_id)
            .map(|n| n.node_type.clone())
        else {
            tracing::warn!(
                target: "weft_dispatcher::status",
                project_id, node_id = %row.node_id,
                "infra_node row references node not in project source; skipping"
            );
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

    // Aggregate state across infra nodes:
    //   - none:    project has 0 requires_infra nodes.
    //   - running: every requires_infra node has a Running row.
    //   - stopped: every requires_infra node is Stopped (or absent).
    //   - partial: mixed (some Running, some not, no failures).
    //   - failed:  at least one row has status=Failed.
    //   - flaky:   at least one row has status=Flaky and the rest are Running.
    let infra_node_count = project.nodes.iter().filter(|n| n.requires_infra).count();
    let has_infra = infra_node_count > 0;
    let infra_rollup = if !has_infra {
        "none".to_string()
    } else {
        use crate::infra_node::InfraNodeStatus;
        let mut running = 0usize;
        let mut stopped = 0usize;
        let mut absent = 0usize; // never provisioned OR terminated
        let mut failed = 0usize;
        let mut flaky = 0usize;
        let mut stopping = 0usize;
        let mut terminating = 0usize;
        let mut provisioning = 0usize;
        for n in project.nodes.iter().filter(|n| n.requires_infra) {
            match infra_rows.iter().find(|r| r.node_id == n.id) {
                Some(r) => match r.status {
                    InfraNodeStatus::Running => running += 1,
                    InfraNodeStatus::Failed => failed += 1,
                    InfraNodeStatus::Flaky => flaky += 1,
                    InfraNodeStatus::Stopping => stopping += 1,
                    InfraNodeStatus::Terminating => terminating += 1,
                    InfraNodeStatus::Provisioning => provisioning += 1,
                    InfraNodeStatus::Stopped => stopped += 1,
                },
                // No `infra_node` row means the node was either
                // never provisioned OR was terminated (terminate
                // removes the row). Either way the project namespace
                // holds nothing for this node; Terminate as an
                // action is meaningless. Treat as `none`.
                None => absent += 1,
            }
        }
        // Transient states take precedence: if anything is mid-flip
        // we report that so the action bar shows a spinner and no
        // user-actionable verbs. `terminating` beats `stopping`
        // (terminate is the more aggressive verb).
        if terminating > 0 {
            "terminating".to_string()
        } else if stopping > 0 {
            "stopping".to_string()
        } else if provisioning > 0 {
            "provisioning".to_string()
        } else if failed > 0 {
            "failed".to_string()
        } else if flaky > 0 && running + flaky == infra_node_count {
            "flaky".to_string()
        } else if running == infra_node_count {
            "running".to_string()
        } else if absent == infra_node_count {
            // All nodes have no row → terminated (or fresh).
            "none".to_string()
        } else if stopped + absent == infra_node_count {
            // Some stopped + some never-provisioned. Pragmatic
            // bucket as `stopped` because the user can re-start
            // and Terminate still makes sense for the actually-
            // stopped subset.
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

    let source_hash = state
        .projects
        .running_source_hash(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("running_source_hash: {e}")))?;
    let infra_hash = state
        .projects
        .running_infra_hash(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("running_infra_hash: {e}")))?;
    let drift = compute_drift(&query, source_hash.as_deref(), infra_hash.as_deref());
    let lifecycle = state
        .projects
        .lifecycle(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("lifecycle: {e}")))?
        .ok_or((StatusCode::NOT_FOUND, "project not found".into()))?;
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
    // whose infra isn't running (the run would fail at infra
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
            // Active).
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

    // Infra controls. Hide during Activating so the user can't
    // race the trigger-setup subworkflow. Rollup states:
    //   running:     stop, terminate, (upgrade if drift)
    //   stopped:     start, terminate
    //   none:        start                       (no infra applied yet)
    //   partial /
    //   failed /
    //   flaky:       start, stop, terminate, (upgrade if drift)
    //   stopping /
    //   terminating: (no actions; the supervisor is in flight)
    //
    // `partial` (per-unit lifecycle: some units up, some down) is a
    // valid steady state, not a transient. From it the user can push
    // in any direction: Start brings the down units up, Stop takes the
    // up units down (respecting NoOp), Terminate kills everything. It
    // is NOT the all-or-nothing "stopped" set plus a restart; it's the
    // union of start and stop because both are meaningful at once.
    if has_infra && lifecycle.status != ProjectStatus::Activating {
        match infra_rollup {
            "running" => {
                out.push("infra_stop".to_string());
                out.push("infra_terminate".to_string());
                if drift.infra_drift {
                    out.push("infra_upgrade".to_string());
                }
            }
            "stopped" => {
                // The user can either bring the nodes back up (Start
                // scales replicas back, PVC intact) OR escalate to
                // Terminate to delete the PVC and start fresh.
                out.push("infra_start".to_string());
                out.push("infra_terminate".to_string());
            }
            "none" => {
                out.push("infra_start".to_string());
            }
            "partial" | "failed" | "flaky" => {
                out.push("infra_start".to_string());
                out.push("infra_stop".to_string());
                out.push("infra_terminate".to_string());
                if drift.infra_drift {
                    out.push("infra_upgrade".to_string());
                }
            }
            "stopping" | "terminating" => {
                // Supervisor is processing a lifecycle command. No
                // user-actionable verbs; the action bar renders a
                // spinner from the rollup alone.
            }
            _ => {}
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
    activate_inner(&state, id, source_hash, infra_hash, reactivate_choice).await
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
    /// Full cleanup: cancel the TS color, sweep orphan TS colors, drop
    /// all signal rows, CAS Activating→Inactive. Used once trigger-
    /// setup started (signals are in flux).
    WipeSignals { ts_color: Option<weft_core::Color> },
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
/// The keep-alive lease is acquired and held for the whole window,
/// then dropped here on success (the listener thereafter stays alive
/// on signal-row presence). On failure it drops when this fn returns.
#[allow(clippy::too_many_arguments)]
async fn activate_trigger_setup_window(
    state: &DispatcherState,
    id: uuid::Uuid,
    project_id: &str,
    tenant: &crate::tenant::TenantId,
    namespace: &str,
    choice: &str,
    project: &ProjectDefinition,
) -> Result<(), ActivateWindowError> {
    // Failures up to (not including) run_trigger_setup haven't touched
    // signal state, so they only need the status un-stuck.
    let unstick = |(status, msg): (StatusCode, String)| ActivateWindowError {
        status,
        msg,
        rollback: ActivateRollback::UnstickOnly,
    };

    let keep_alive = crate::listener::ActivateKeepAlive::acquire(&state.pg_pool, tenant)
        .await
        .map_err(|e| unstick((StatusCode::INTERNAL_SERVER_ERROR, format!("keep-alive: {e}"))))?;

    // Apply the reactivate choice's destructive effect now that we
    // hold the exclusive Activating transition (validated by caller).
    apply_reactivate_choice(state, project_id, choice).await.map_err(unstick)?;

    // Sweep any prior TriggerSetup colors that leaked from a failed
    // previous activate. Safe because we won `try_begin_activating`:
    // no sibling activate is in flight, so every non-terminal
    // trigger_setup color here is an orphan from a dead prior
    // activate, never a concurrent one.
    sweep_orphan_trigger_setup_colors(state, project_id)
        .await
        .map_err(|e| {
            unstick((StatusCode::INTERNAL_SERVER_ERROR, format!("sweep orphan ts colors: {e}")))
        })?;

    // Run TriggerSetup. From here on signals are in flux, so a failure
    // wipes (carrying the TS color so the rollback cancels it). Its
    // register_signal tasks UPSERT entry rows on (project_id,
    // node_id), so existing rows from before a deactivate get
    // spec/mount/auth refreshed in place; the token survives, so
    // parked_fires still attached drain cleanly later.
    let seeds = compute_trigger_setup_seeds(project);
    if !seeds.is_empty() {
        run_trigger_setup(state, id, seeds).await.map_err(|(status, msg, ts_color)| {
            ActivateWindowError { status, msg, rollback: ActivateRollback::WipeSignals { ts_color } }
        })?;
    }

    // From here trigger-setup succeeded but signals are registered, so
    // any failure still wipes (ts_color is None: setup's own color is
    // already terminal, the rollback sweeps remaining orphans).
    let wipe = |(status, msg): (StatusCode, String)| ActivateWindowError {
        status,
        msg,
        rollback: ActivateRollback::WipeSignals { ts_color: None },
    };

    // Drop orphan entry rows: nodes that previously had triggers but
    // no longer do (user edited source while deactivated).
    drop_orphan_entry_rows(state, project_id, project)
        .await
        .map_err(|e| wipe((StatusCode::INTERNAL_SERVER_ERROR, format!("drop_orphan_entry_rows: {e}"))))?;

    // Reconcile the listener's in-RAM registry with the durable
    // signal table (resume signals belong to suspended executions
    // whose workers are gone). /rehydrate is idempotent.
    state
        .listeners
        .with_listener(
            tenant,
            namespace,
            state.listener_backend.as_ref(),
            &state.pg_pool,
            state.pod_id.as_str(),
            |handle| async move { crate::listener::rehydrate(&handle).await },
        )
        .await
        .map_err(|e| wipe((StatusCode::INTERNAL_SERVER_ERROR, format!("listener rehydrate: {e}"))))?;

    // Flip Activating → Active. CAS guards against a concurrent
    // cancel_activate that flipped us to Inactive: in that case the
    // cancel already wiped our signals, so we surrender with no
    // further rollback (UnstickOnly: status is already Inactive, the
    // inner CAS no-ops).
    let cas_ok = state
        .projects
        .cas_lifecycle(
            id,
            crate::project_store::ProjectStatus::Activating,
            &crate::project_store::ProjectLifecycle::active(),
        )
        .await
        .map_err(|e| wipe((StatusCode::INTERNAL_SERVER_ERROR, format!("cas_lifecycle: {e}"))))?;
    if !cas_ok {
        return Err(ActivateWindowError {
            status: StatusCode::CONFLICT,
            msg: "activate raced with cancel_activate; project is now Inactive".into(),
            rollback: ActivateRollback::UnstickOnly,
        });
    }

    // Window done: project is Active. Release the lease; the listener
    // now stays alive on signal-row presence alone.
    drop(keep_alive);
    Ok(())
}

/// In-process callable for `activate`. Used by `/infra/sync`'s
/// auto-reactivate path. Same body as the axum handler minus the
/// extractor plumbing.
pub async fn activate_inner(
    state: &DispatcherState,
    id: uuid::Uuid,
    source_hash: Option<String>,
    infra_hash: Option<String>,
    reactivate_choice: Option<String>,
) -> Result<Json<ActivateResponse>, (StatusCode, String)> {
    if let Some(hash) = source_hash.as_deref() {
        state
            .projects
            .set_running_source_hash(id, hash)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("set_running_source_hash: {e}")))?;
    }
    if let Some(hash) = infra_hash.as_deref() {
        state
            .projects
            .set_running_infra_hash(id, hash)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("set_running_infra_hash: {e}")))?;
    }

    let project = state
        .projects
        .project(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("project: {e}")))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "project not found".into()))?;
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
    let mut missing: Vec<String> = Vec::new();
    for node in project.nodes.iter().filter(|n| n.requires_infra) {
        let row = crate::infra_node::get(&state.pg_pool, &project_id, &node.id)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("infra_node: {e}")))?;
        let running = row
            .map(|r| r.status == crate::infra_node::InfraNodeStatus::Running)
            .unwrap_or(false);
        if !running {
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

    // A source-hash change must kill the stale-image worker before
    // the activate's TriggerSetup exec runs, otherwise it's
    // dispatched against a worker that doesn't know about the new
    // trigger nodes. Idempotent (kill-by-source-hash + respawn), so
    // it's safe before the single-flight CAS: two concurrent
    // activates both calling it is harmless, and keeping it before
    // the CAS means its failure leaves the project in its original
    // status rather than stranded in Activating. MUST propagate.
    replace_stale_worker_if_needed(state, &project_id).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("replace_stale_worker_if_needed: {e}"),
        )
    })?;

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
    // touching the keep-alive sentinel or the orphan sweep. This is
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
            "project is already activating; wait for it to finish or `weft deactivate` to cancel"
                .into(),
        ));
    }

    // Tenant/namespace for the trigger-setup window (which holds the
    // keep-alive lease internally) and the post-activate URL publish.
    let tenant = state.tenant_router.tenant_for_project(&project_id);
    let namespace = state.namespace_mapper.namespace_for(&tenant);

    // Everything from here to the Active CAS happens while the
    // project is Activating. ANY failure in this window must
    // un-stick the project (a stranded Activating locks out all
    // future activates) and release the keep-alive lease. Rather
    // than hand-roll a rollback at each `?` (the footgun that
    // stranded the project before), the whole window is ONE fallible
    // block with ONE rollback site below. A future step added here
    // can't forget the un-stick.
    let setup = activate_trigger_setup_window(
        &state, id, &project_id, &tenant, &namespace, choice, &project,
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
            ActivateRollback::UnstickOnly => unstick_activating(&state, id).await.map(|_| ()),
            ActivateRollback::WipeSignals { ts_color } => {
                wipe_activating_state(&state, id, &project_id, ts_color).await
            }
        };
        if let Err((rb_status, rb_msg)) = rb {
            tracing::error!(
                target: "weft_dispatcher::activate",
                project_id = %id, rb_status = %rb_status, rb_error = %rb_msg,
                "activate rollback failed; project may be stuck Activating, manual cleanup needed"
            );
        }
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
    drain_parked_fires(&state, &project_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("drain_parked_fires: {e}")))?;

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
/// A pod crash between steps 1 and 3 leaves the claim set; the next
/// activate's pre-pass releases stale claims older than the threshold
/// so the row becomes drainable again.
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
    let now = crate::lease::now_unix();
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
                // Pop the head, FENCED on our claim nonce. `- 0::int`
                // is the unambiguous form of `jsonb_array - integer_index`
                // (plain `- 0` is ambiguous with `jsonb_object - text_key`
                // under some inference paths). If `drain_claimed_by` is
                // no longer ours, a sibling took over after a stale-claim
                // sweep: 0 rows affected -> abort without popping (the
                // element we just dispatched dedups at the task table via
                // ParkedFire.id, and the new owner re-drives from head).
                let popped = sqlx::query(
                    "UPDATE signal SET parked_fires = parked_fires - 0::int \
                     WHERE token = $1 AND drain_claimed_by = $2",
                )
                .bind(token)
                .bind(&owner)
                .execute(&state.pg_pool)
                .await?;
                if popped.rows_affected() == 0 {
                    break Ok(());
                }
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
async fn unstick_activating(
    state: &DispatcherState,
    id: uuid::Uuid,
) -> Result<bool, (StatusCode, String)> {
    state
        .projects
        .cas_lifecycle(
            id,
            crate::project_store::ProjectStatus::Activating,
            &crate::project_store::ProjectLifecycle::wiped(),
        )
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("cas_lifecycle: {e}")))
}

/// Cancel-activate / TriggerSetup-failure cleanup: un-stick the
/// status (via `unstick_activating`) AND wipe signal state (cancel TS
/// color + sweep orphan TS colors + drop all signal rows). Used once
/// trigger-setup has started, so signals are in flux and must go.
/// Idempotent: a partial run leaves the next call with less work, and
/// if the un-stick CAS loses (a concurrent activate-success/cancel
/// already left Activating) it returns early WITHOUT wiping.
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
    // Un-stick FIRST. If we lose the CAS, activate already finished
    // cleanly (or a sibling cancel did the wipe); do not touch signals.
    if !unstick_activating(state, id).await? {
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
    Path(id_str): Path<String>,
    Json(spec): Json<weft_broker_client::protocol::DeactivateSpec>,
) -> Result<StatusCode, (StatusCode, String)> {
    let id = id_str
        .parse::<uuid::Uuid>()
        .map_err(|_| (StatusCode::BAD_REQUEST, "bad id".into()))?;
    spec.validate()
        .map_err(|m| (StatusCode::BAD_REQUEST, m.to_string()))?;
    let existed = deactivate_project_with_mode(
        &state,
        id,
        spec.mode,
        spec.grace_minutes,
        spec.running_policy,
        false, // user-initiated (the standalone Deactivate verb)
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
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("project: {e}")))?
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
        let row = crate::infra_node::get(&state.pg_pool, &project_id, &node.id)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("infra_node: {e}")))?;
        let running = row
            .map(|r| r.status == crate::infra_node::InfraNodeStatus::Running)
            .unwrap_or(false);
        if !running {
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
/// `infra terminate` / project removal: a stopped infra leaves
/// the project's triggers pointing at a dead endpoint, so we
/// always wipe in those flows. Returns true if the project existed.
///
/// Always wipes (preservationMode=wipe, runningPolicy=cancel).
/// User-initiated deactivates use the parameterized variant via
/// the API handler.
/// Replace the project's alive worker pod when its baked-in
/// source_hash no longer matches the project's current
/// `running_source_hash`. The worker binary embeds the project
/// definition at compile time (codegen bakes `project.json` into the
/// binary), so a worker spawned before the user added a node will
/// never see that node.
///
/// Order matters here. If we killed the old pod first and then waited
/// for cold_start to spawn a replacement, the next enqueued worker
/// task (the InfraSetup `execute`) could race the doomed pod's
/// in-flight task picker: the pod is marked dead in the DB but still
/// alive in k8s during its terminationGracePeriod, and the picker
/// happily claims tasks until its own heartbeat detects the dead row.
/// Any journal write then trips the fencing trigger and the task
/// fails. We avoid the race by spawning the replacement FIRST, then
/// killing the stale pod once the new one is alive.
///
/// Idempotent: a no-op when hashes match or no pod is alive. Safe to
/// call from every path that updates `running_source_hash`.
pub async fn replace_stale_worker_if_needed(
    state: &DispatcherState,
    project_id: &str,
) -> anyhow::Result<()> {
    let project_uuid: uuid::Uuid = match project_id.parse() {
        Ok(u) => u,
        Err(_) => return Ok(()),
    };
    // No fallback: a DB error here must propagate or the stale-pod
    // check would treat the failure as "no expected hash" and
    // either kill a healthy pod (false positive) or skip a stale
    // pod (false negative). `Ok(None)` is fine ("never set"); the
    // hash comparison treats it as empty.
    // No live worker: nothing to compare against. Skip without
    // consulting the source hash: a None hash with no live worker
    // is a legitimate "project just registered, no sync run yet"
    // state (sync writes the hash before any task that would
    // spawn). With a live worker, we need a hash to decide whether
    // to kill it; fail loud if it's missing.
    let Some((stale_pod_name, stale_namespace, have_hash)) =
        weft_task_store::worker_pod::alive_pod_for_project_full(&state.pg_pool, project_id)
            .await?
    else {
        return Ok(());
    };
    let want_hash = state
        .projects
        .running_source_hash(project_uuid)
        .await?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "replace_stale_worker: project {project_id} has a live worker but no \
                 running_source_hash; sync ordering invariant broken"
            )
        })?;
    if have_hash == want_hash {
        return Ok(());
    }
    tracing::info!(
        target: "weft_dispatcher::api::project",
        project_id,
        stale_pod = %stale_pod_name,
        have_hash = %have_hash,
        want_hash = %want_hash,
        "replacing stale-image worker pod after source_hash change"
    );

    // Step 1: kill the stale pod ourselves. The `spawn_pod` task
    // executor is intentionally narrow: "spawn a pod when none is
    // alive". It will not kill a mismatched pod for us. The kill
    // belongs to the caller (this function) that decided the stale
    // pod must go. mark_dead first so the fencing trigger blocks
    // any late journal write from the doomed worker; kubectl delete
    // second so the pod is actually removed.
    weft_task_store::worker_pod::mark_dead(&state.pg_pool, &stale_pod_name).await?;
    state
        .workers
        .kill_pod(stale_pod_name.clone(), stale_namespace)
        .await
        .map_err(|e| {
            anyhow::anyhow!(
                "kill_pod {stale_pod_name} failed (stale worker would survive spawn): {e}"
            )
        })?;

    // Step 2: enqueue a SpawnPod task for a fresh worker. Dedup key
    // matches cold_start's so a concurrent sweep collapses on us.
    let tenant = state.tenant_router.tenant_for_project(project_id);
    let namespace = state
        .projects
        .project_namespace(project_id)
        .await?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "project_namespace({project_id}) returned None; cannot spawn worker"
            )
        })?;
    let dedup = format!("{project_id}:spawn");
    let payload = serde_json::json!({
        "project_id": project_id,
        "tenant": tenant.as_str(),
        "namespace": namespace,
        "owner_dispatcher": state.pod_id.as_str(),
    });
    weft_task_store::tasks::enqueue_dedup(
        &state.pg_pool,
        weft_task_store::tasks::NewTask {
            kind: weft_task_store::TaskKind::SpawnPod,
            target: weft_task_store::tasks::TaskTarget::Dispatcher,
            project_id: Some(project_id.to_string()),
            dedup_key: Some(dedup),
            color: None,
            tenant_id: Some(tenant.to_string()),
            target_pod_name: None,
            payload,
        },
    )
    .await?;

    // Step 3: wait for the new pod to register itself alive. Bounded
    // so a wedged image build doesn't block the sync indefinitely.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    let mut ready = false;
    while std::time::Instant::now() < deadline {
        if let Some((p, _, h)) = weft_task_store::worker_pod::alive_pod_for_project_full(
            &state.pg_pool,
            project_id,
        )
        .await?
        {
            if p != stale_pod_name && h == want_hash {
                ready = true;
                break;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    if !ready {
        tracing::warn!(
            target: "weft_dispatcher::api::project",
            project_id,
            stale_pod = %stale_pod_name,
            "replacement worker did not come up within 60s; \
             cold_start will retry as soon as a worker task lands"
        );
    }
    Ok(())
}

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
    by_health: bool,
) -> Result<bool, (StatusCode, String)> {
    use crate::project_store::{ProjectLifecycle, ProjectStatus};

    let project_id = id.to_string();

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
    use crate::infra_lifecycle_command::RunningPolicy;
    let lifecycle_to_set = if running_policy == RunningPolicy::Wait {
        ProjectLifecycle::deactivating_to(target)
    } else if mode != DeactivationMode::Wipe {
        cancel_running_non_suspended(state, &project_id).await?;
        target
    } else {
        // wipe + cancel: rows + executions already gone above.
        target
    };

    let existed = state
        .projects
        .set_lifecycle(id, &lifecycle_to_set)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("set_lifecycle: {e}")))?;
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
        if running_policy == RunningPolicy::Wait
            && lifecycle_to_set.status == ProjectStatus::Deactivating
            && running_now == 0
        {
            // Fast-path CAS: if this returns false (raced something
            // else) or errors, the bridge's drain-watcher will
            // perform the same transition on its next tick.
            // Either way no caller action; log on error.
            if let Err(e) = state
                .projects
                .cas_status(id, ProjectStatus::Deactivating, ProjectStatus::Inactive)
                .await
            {
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
    let now = crate::lease::now_unix() as u64;

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
        state
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
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("journal: {e}"), Some(color)))?;
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
                            Some(color),
                        ))
                    }
                    Ok(Some(crate::api::execution::TerminalOutcome::Cancelled)) => {
                        return Err((
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "trigger setup cancelled".into(),
                            Some(color),
                        ))
                    }
                    Ok(None) => continue, // still in flight; keep waiting
                    Err(e) => {
                        return Err((
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("trigger setup terminal lookup: {e}"),
                            Some(color),
                        ))
                    }
                }
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
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


// `crate::lease::now_unix` is the canonical wall-clock reader.

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

#[cfg(test)]
mod infra_seed_and_dep_tests {
    use super::*;

    /// (id, is_trigger, requires_infra)
    fn project(nodes: &[(&str, bool, bool)], edges: &[(&str, &str)]) -> ProjectDefinition {
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
            "name": "t",
            "description": null,
            "nodes": n_json,
            "edges": e_json,
            "groups": []
        });
        serde_json::from_value(body).expect("valid test project")
    }

    fn seed_ids(seeds: &[RootSeed]) -> Vec<String> {
        let mut v: Vec<String> = seeds.iter().map(|s| s.node_id.clone()).collect();
        v.sort();
        v
    }

    #[test]
    fn infra_seeds_are_upstream_roots_not_infra_nodes() {
        // text → compute → infra
        let p = project(
            &[("text", false, false), ("compute", false, false), ("infra", false, true)],
            &[("text", "compute"), ("compute", "infra")],
        );
        let seeds = compute_infra_setup_seeds(&p);
        // The seed is the upstream root (text), NOT the infra node.
        assert_eq!(seed_ids(&seeds), vec!["text".to_string()]);
    }

    #[test]
    fn infra_seeds_skip_unreachable_branches() {
        // unrelated standalone node + a real text → infra chain.
        let p = project(
            &[
                ("standalone", false, false),
                ("text", false, false),
                ("infra", false, true),
            ],
            &[("text", "infra")],
        );
        let seeds = compute_infra_setup_seeds(&p);
        assert_eq!(seed_ids(&seeds), vec!["text".to_string()]);
    }

    #[test]
    fn infra_node_with_no_upstream_seeds_itself() {
        // A parameterless infra node (no upstream edges) IS its own
        // root : has to seed something to fire.
        let p = project(&[("infra", false, true)], &[]);
        let seeds = compute_infra_setup_seeds(&p);
        assert_eq!(seed_ids(&seeds), vec!["infra".to_string()]);
    }

    #[test]
    fn infra_seeds_empty_when_no_infra_nodes() {
        let p = project(&[("a", false, false), ("b", false, false)], &[("a", "b")]);
        assert!(compute_infra_setup_seeds(&p).is_empty());
    }

    #[test]
    fn infra_seeds_handle_multiple_infra_nodes_with_shared_root() {
        // text → infraA ; text → infraB
        let p = project(
            &[("text", false, false), ("infraA", false, true), ("infraB", false, true)],
            &[("text", "infraA"), ("text", "infraB")],
        );
        let seeds = compute_infra_setup_seeds(&p);
        // text is the only root reaching both.
        assert_eq!(seed_ids(&seeds), vec!["text".to_string()]);
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
