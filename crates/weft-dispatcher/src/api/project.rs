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

use crate::backend::WakeContext;
use weft_core::primitive::RootSeed;
use crate::events::DispatcherEvent;
use crate::project_store::ProjectStatus;
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

    let summary = state.projects.register(project).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(RegisterError {
                error: format!("register: {e}"),
                diagnostics: Vec::new(),
            }),
        )
    })?;
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
) -> Result<StatusCode, StatusCode> {
    let id = id.parse::<uuid::Uuid>().map_err(|_| StatusCode::BAD_REQUEST)?;
    // Deactivate first: cancels any in-flight executions, unregisters
    // every wake signal (entry + resume) from the tenant's listener,
    // drops entry tokens. Best-effort: if the project wasn't active
    // there's nothing to deactivate and we proceed.
    let _ = deactivate_project(&state, id).await;
    if state.projects.remove(id).await {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(StatusCode::NOT_FOUND)
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
    let summary = state
        .projects
        .get(id)
        .await
        .ok_or_else(|| (StatusCode::NOT_FOUND, "project not found".into()))?;
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

    // Record the run against the first seed's node for journal
    // compatibility; the journal's single `entry_node` field is a
    // vestige of single-entry mode. Subgraph rooted at many nodes
    // means there's no single entry, pick the first seed's node to
    // keep the start row valid.
    let entry_node_for_journal = seeds[0].node_id.clone();

    // Event-sourced log: execution started + one PulseSeeded event
    // per root. Replay rebuilds the initial pulse table from these.
    let now = unix_now();
    let _ = state
        .journal
        .record_event(&crate::journal::ExecEvent::ExecutionStarted {
            color,
            project_id: id.to_string(),
            entry_node: entry_node_for_journal.clone(),
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
            .record_event(&crate::journal::ExecEvent::PulseSeeded {
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
    state
        .slots
        .with_slot(color, move |slot| {
            Box::pin(async move {
                let queued = match slot {
                    crate::slots::Slot::Idle { queued, .. }
                    | crate::slots::Slot::Starting { queued, .. }
                    | crate::slots::Slot::WaitingReconnect { queued, .. } => queued,
                    crate::slots::Slot::Live { .. }
                    | crate::slots::Slot::StalledGrace { .. } => {
                        // Live or stalled shouldn't happen for a
                        // fresh run (we minted a brand-new color).
                        // Defensive: overwrite with Idle.
                        *slot = crate::slots::Slot::Idle {
                            queued: std::collections::VecDeque::new(),
                        };
                        if let crate::slots::Slot::Idle { queued, .. } = slot {
                            queued
                        } else {
                            unreachable!()
                        }
                    }
                };
                queued.push_back(crate::slots::QueuedWake::Start(
                    weft_core::primitive::WakeMessage::Fresh {
                        seeds: core_seeds,
                        phase: weft_core::context::Phase::Fire,
                    },
                ));
            })
        })
        .await;

    let wake = WakeContext::resolve(&state, id.to_string(), color);
    let _ = &summary;
    let worker = state
        .workers
        .spawn_worker(wake)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("spawn: {e}")))?;
    let _ = state
        .journal
        .record_event(&crate::journal::ExecEvent::WorkerSpawned {
            color,
            at_unix: unix_now(),
        })
        .await;

    // Promote Idle → Starting so subsequent wakes queue instead of
    // spawning a second worker.
    state
        .slots
        .with_slot(color, move |slot| {
            Box::pin(async move {
                if let crate::slots::Slot::Idle { queued } = std::mem::replace(
                    slot,
                    crate::slots::Slot::Idle {
                        queued: std::collections::VecDeque::new(),
                    },
                ) {
                    *slot = crate::slots::Slot::Starting { queued, worker: Some(worker) };
                }
            })
        })
        .await;

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
/// The `payload` is attached to every seed as-is for now. Per-trigger
/// mocks will attach different payloads per root; that lands with
/// the mock file format.
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

    // A node counts as a trigger for setup-phase purposes if it
    // either declares static entry_signals in its metadata (legacy
    // path: ApiPost declaring Webhook) OR its features mark it as
    // a trigger (runtime-registered path: WhatsAppReceive, future
    // SSE triggers). Both are expected to call ctx.register_signal
    // during TriggerSetup phase; the metadata flag is just a hint
    // so we know to run them in that phase.
    let triggers: Vec<String> = project
        .nodes
        .iter()
        .filter(|n| !n.entry_signals.is_empty() || n.features.is_trigger)
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

/// Seeds for an InfraSetup-phase sub-execution.
///
/// Target set = every node flagged `requires_infra: true`. Infra
/// nodes are the only thing that runs in this phase; we don't walk
/// upstream because `provision` is self-contained (the node's config
/// carries everything the sidecar spec needs).
pub fn compute_infra_setup_seeds(project: &ProjectDefinition) -> Vec<RootSeed> {
    project
        .nodes
        .iter()
        .filter(|n| n.requires_infra)
        .map(|n| RootSeed {
            node_id: n.id.clone(),
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
        .record_event(&crate::journal::ExecEvent::ExecutionStarted {
            color,
            project_id: project_id.clone(),
            entry_node: seeds[0].node_id.clone(),
            at_unix: now,
        })
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("journal: {e}")))?;
    for seed in &seeds {
        let _ = state
            .journal
            .record_event(&crate::journal::ExecEvent::PulseSeeded {
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

    state
        .slots
        .with_slot(color, {
            let seeds = seeds.clone();
            move |slot| {
                Box::pin(async move {
                    let queued = match slot {
                        crate::slots::Slot::Idle { queued, .. }
                        | crate::slots::Slot::Starting { queued, .. }
                        | crate::slots::Slot::WaitingReconnect { queued, .. } => queued,
                        crate::slots::Slot::Live { .. }
                        | crate::slots::Slot::StalledGrace { .. } => {
                            *slot = crate::slots::Slot::Idle {
                                queued: std::collections::VecDeque::new(),
                            };
                            let crate::slots::Slot::Idle { queued, .. } = slot else {
                                unreachable!()
                            };
                            queued
                        }
                    };
                    queued.push_back(crate::slots::QueuedWake::Start(
                        weft_core::primitive::WakeMessage::Fresh {
                            seeds,
                            phase: weft_core::context::Phase::InfraSetup,
                        },
                    ));
                })
            }
        })
        .await;

    let wake = WakeContext::resolve(state, project_id.clone(), color);
    let worker = state
        .workers
        .spawn_worker(wake)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("spawn: {e}")))?;
    let _ = state
        .journal
        .record_event(&crate::journal::ExecEvent::WorkerSpawned {
            color,
            at_unix: unix_now(),
        })
        .await;
    state
        .slots
        .with_slot(color, move |slot| {
            Box::pin(async move {
                if let crate::slots::Slot::Starting { worker: w, .. } = slot {
                    *w = Some(worker);
                }
            })
        })
        .await;

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

    // Set of trigger nodes. A node counts as a trigger if either
    // its metadata pre-declares entry_signals (legacy path:
    // ApiPost / Cron resolved at enrich time) OR its features
    // mark it as a trigger (runtime-registered path:
    // WhatsAppReceive via ctx.register_signal). Both end up in
    // signal_tracker, so both need to be recognized here as
    // fire targets.
    let triggers: HashSet<String> = project
        .nodes
        .iter()
        .filter(|n| !n.entry_signals.is_empty() || n.features.is_trigger)
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
    pub kind: String,
    pub url: String,
}

/// Activate a project. Preconditions: every `requires_infra: true`
/// node has been provisioned via `weft infra up`. Steps:
/// 1. Tear down any previous listener + tracked signals.
/// 2. Spawn a fresh listener.
/// 3. Run the TriggerSetup sub-execution. Trigger nodes register
///    themselves via `ctx.register_signal`; dispatcher's WS handler
///    forwards each registration to the listener and fills
///    `signal_tracker`.
/// 4. Mark project Active, publish TriggerUrlChanged events, return
///    the listener-minted URLs.
#[derive(Debug, Serialize)]
pub struct ProjectStatusResponse {
    pub id: String,
    pub name: String,
    pub status: String,
    pub listener_running: bool,
    pub infra: Vec<ProjectInfraEntry>,
    pub executions: ProjectExecutionsSummary,
}

#[derive(Debug, Serialize)]
pub struct ProjectInfraEntry {
    pub node_id: String,
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

/// Aggregate view for `weft status`. Returns registration,
/// listener state, per-node infra state, and a rollup of recent
/// executions in one response so the CLI doesn't need to
/// stitch three separate calls.
pub async fn status(
    State(state): State<DispatcherState>,
    Path(id_str): Path<String>,
) -> Result<Json<ProjectStatusResponse>, (StatusCode, String)> {
    let id = id_str
        .parse::<uuid::Uuid>()
        .map_err(|_| (StatusCode::BAD_REQUEST, "bad id".into()))?;
    let summary = state
        .projects
        .get(id)
        .await
        .ok_or((StatusCode::NOT_FOUND, "project not found".into()))?;
    let project_id = id.to_string();
    let tenant = state.tenant_router.tenant_for_project(&project_id);
    let listener_running = state.listeners.get(tenant.as_str()).is_some();

    let infra_entries = state.infra_registry.list_for_project(&project_id);
    let mut infra = Vec::new();
    for (node_id, entry) in infra_entries {
        infra.push(ProjectInfraEntry {
            node_id,
            status: format!("{:?}", entry.status).to_lowercase(),
            endpoint_url: entry.handle.endpoint_url.clone(),
        });
    }

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

    Ok(Json(ProjectStatusResponse {
        id: project_id,
        name: summary.name,
        status: summary.status.as_str().to_string(),
        listener_running,
        infra,
        executions,
    }))
}

pub async fn activate(
    State(state): State<DispatcherState>,
    Path(id_str): Path<String>,
) -> Result<Json<ActivateResponse>, (StatusCode, String)> {
    let id = id_str
        .parse::<uuid::Uuid>()
        .map_err(|_| (StatusCode::BAD_REQUEST, "bad id".into()))?;
    let project = state
        .projects
        .project(id)
        .await
        .ok_or_else(|| (StatusCode::NOT_FOUND, "project not found".into()))?;
    let summary = state
        .projects
        .get(id)
        .await
        .ok_or_else(|| (StatusCode::NOT_FOUND, "project not found".into()))?;
    let project_id = id.to_string();

    // Pre-flight: every requires_infra node must be provisioned
    // AND in the Running state. A stopped sidecar is just as bad
    // as a missing one from the trigger-setup subgraph's point of
    // view (the worker will try to query /outputs and fail).
    let missing: Vec<String> = project
        .nodes
        .iter()
        .filter(|n| n.requires_infra)
        .filter(|n| state.infra_registry.handle_if_running(&project_id, &n.id).is_none())
        .map(|n| n.id.clone())
        .collect();
    if !missing.is_empty() {
        return Err((
            StatusCode::PRECONDITION_REQUIRED,
            format!(
                "infra not running for: {}. Run `weft infra start` first.",
                missing.join(", ")
            ),
        ));
    }

    // Re-activation: cancel every still-running execution of this
    // project. The user may have changed the source between
    // activations; leaving in-flight runs around would let an old
    // worker handle a fire that was registered against the new
    // code. Then drop signals + tokens.
    let summaries = state
        .journal
        .list_executions(500)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("list execs: {e}")))?;
    for s in summaries {
        if s.project_id != project_id {
            continue;
        }
        let settled = matches!(
            s.status.to_ascii_lowercase().as_str(),
            "completed" | "failed" | "cancelled",
        );
        if settled {
            continue;
        }
        let _ = crate::api::execution::cancel_color(&state, s.color).await;
    }

    let _ = state
        .journal
        .signal_remove_for_project(&project_id)
        .await;
    state
        .journal
        .drop_entry_tokens(&project_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("drop tokens: {e}")))?;

    let tenant = state.tenant_router.tenant_for_project(&project_id);
    let namespace = state.namespace_mapper.namespace_for(&tenant);
    let dispatcher_url = state.config.cluster_dispatcher_url();
    let deploy_name = crate::listener::deploy_name_for_tenant(tenant.as_str());
    state
        .listeners
        .ensure(
            &tenant,
            &namespace,
            &dispatcher_url,
            state.listener_backend.as_ref(),
            &state.pg_pool,
            &deploy_name,
            state.pod_id.as_str(),
        )
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("spawn listener: {e}")))?;

    // Compute the trigger-setup subgraph. If the project has no
    // triggers there's nothing to register and we're done.
    let _ = &summary;
    let seeds = compute_trigger_setup_seeds(&project);
    if !seeds.is_empty() {
        run_trigger_setup(&state, id, seeds).await?;
    }

    // Collect the URLs that landed in signal_tracker during the
    // sub-exec. The signal_tracker carries only routing metadata,
    // not URLs; the listener minted them. We need a small lookup
    // pass: for each tracked signal in this project, ask the
    // listener for the URL.
    let urls = collect_listener_urls(&state, &project_id).await;

    state.projects.set_status(id, ProjectStatus::Active).await;
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

pub async fn deactivate(
    State(state): State<DispatcherState>,
    Path(id_str): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    let id = id_str
        .parse::<uuid::Uuid>()
        .map_err(|_| (StatusCode::BAD_REQUEST, "bad id".into()))?;
    let existed = deactivate_project(&state, id).await?;
    if !existed {
        return Err((StatusCode::NOT_FOUND, "project not found".into()));
    }
    Ok(StatusCode::NO_CONTENT)
}

/// Shared deactivation logic. Used both by the explicit
/// `/deactivate` endpoint and auto-called from `infra stop` /
/// `infra terminate`, because a stopped or terminated sidecar
/// leaves the project's listener-registered triggers pointing at
/// a dead endpoint. Returns true if the project existed, false if
/// we silently no-op'd (useful for the auto-call path where a
/// project might not have been activated in the first place).
pub async fn deactivate_project(
    state: &DispatcherState,
    id: uuid::Uuid,
) -> Result<bool, (StatusCode, String)> {
    let project_id = id.to_string();

    // Cancel every still-running execution for this project before
    // tearing down trigger registrations / listener. Without this,
    // workers keep chewing on a dead project (no listener, no
    // tokens) until they finish their current node, which can be
    // minutes for things like agent loops or long sidecar calls.
    let summaries = state
        .journal
        .list_executions(500)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("list execs: {e}")))?;
    for s in summaries {
        if s.project_id != project_id {
            continue;
        }
        // Anything not yet settled (no completed/failed terminal
        // event) is fair game. `terminal_for_color` reports an
        // empty status for runs we haven't seen finish.
        let settled = matches!(
            s.status.to_ascii_lowercase().as_str(),
            "completed" | "failed" | "cancelled",
        );
        if settled {
            continue;
        }
        // Best-effort: a single failed cancel shouldn't block the
        // whole deactivate.
        let _ = crate::api::execution::cancel_color(state, s.color).await;
    }

    state
        .journal
        .drop_entry_tokens(&project_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("drop tokens: {e}")))?;
    // Unregister this project's signals from the tenant's listener.
    // The listener self-destructs (via /listener/empty) once its
    // own registry hits zero. Other projects of the same tenant
    // keep using it.
    let tenant = state.tenant_router.tenant_for_project(&project_id);
    let signals = state
        .journal
        .signal_list_for_project(&project_id)
        .await
        .unwrap_or_default();
    if let Some(handle) = state.listeners.get(tenant.as_str()) {
        for meta in &signals {
            let _ = crate::listener::unregister_signal(&handle, &meta.token).await;
        }
    }
    let _ = state
        .journal
        .signal_remove_for_project(&project_id)
        .await;
    let existed = state.projects.set_status(id, ProjectStatus::Inactive).await;
    if existed {
        state
            .events
            .publish(DispatcherEvent::ProjectDeactivated { project_id })
            .await;
    }
    Ok(existed)
}

/// Spawn a worker to run the TriggerSetup sub-execution and wait
/// for it to complete. Blocks the activate handler until every
/// trigger node has had a chance to call `ctx.register_signal`.
async fn run_trigger_setup(
    state: &DispatcherState,
    project_id_uuid: uuid::Uuid,
    seeds: Vec<RootSeed>,
) -> Result<(), (StatusCode, String)> {
    let project_id = project_id_uuid.to_string();
    let color = uuid::Uuid::new_v4();
    let now = unix_now();

    // Journal the sub-exec.
    state
        .journal
        .record_event(&crate::journal::ExecEvent::ExecutionStarted {
            color,
            project_id: project_id.clone(),
            entry_node: seeds[0].node_id.clone(),
            at_unix: now,
        })
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("journal: {e}")))?;
    for seed in &seeds {
        let _ = state
            .journal
            .record_event(&crate::journal::ExecEvent::PulseSeeded {
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

    // Subscribe to the project's event bus BEFORE spawning the
    // worker so we don't race on the completion signal.
    let mut events = state.events.subscribe_project(&project_id).await;

    // Queue the Fresh wake with Phase::TriggerSetup.
    state
        .slots
        .with_slot(color, {
            let seeds = seeds.clone();
            move |slot| {
                Box::pin(async move {
                    let queued = match slot {
                        crate::slots::Slot::Idle { queued, .. }
                        | crate::slots::Slot::Starting { queued, .. }
                        | crate::slots::Slot::WaitingReconnect { queued, .. } => queued,
                        crate::slots::Slot::Live { .. }
                        | crate::slots::Slot::StalledGrace { .. } => {
                            *slot = crate::slots::Slot::Idle {
                                queued: std::collections::VecDeque::new(),
                            };
                            let crate::slots::Slot::Idle { queued, .. } = slot else {
                                unreachable!()
                            };
                            queued
                        }
                    };
                    queued.push_back(crate::slots::QueuedWake::Start(
                        weft_core::primitive::WakeMessage::Fresh {
                            seeds,
                            phase: weft_core::context::Phase::TriggerSetup,
                        },
                    ));
                })
            }
        })
        .await;

    let wake = WakeContext::resolve(state, project_id.clone(), color);
    let worker = state
        .workers
        .spawn_worker(wake)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("spawn: {e}")))?;
    let _ = state
        .journal
        .record_event(&crate::journal::ExecEvent::WorkerSpawned {
            color,
            at_unix: unix_now(),
        })
        .await;
    state
        .slots
        .with_slot(color, move |slot| {
            Box::pin(async move {
                if let crate::slots::Slot::Starting { worker: w, .. } = slot {
                    *w = Some(worker);
                }
            })
        })
        .await;

    // Wait for completion or failure. 30s timeout is generous;
    // trigger setup should be near-instant (nodes just call
    // register_signal and return).
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err((
                StatusCode::GATEWAY_TIMEOUT,
                format!(
                    "trigger setup for {color} timed out after 30s without an ExecutionCompleted or ExecutionFailed event"
                ),
            ));
        }
        match tokio::time::timeout(remaining, events.recv()).await {
            Ok(Ok(crate::events::DispatcherEvent::ExecutionCompleted { color: c, .. })) if c == color => {
                return Ok(());
            }
            Ok(Ok(crate::events::DispatcherEvent::ExecutionFailed { color: c, error, .. })) if c == color => {
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("trigger setup failed: {error}"),
                ));
            }
            Ok(Ok(_)) => continue,
            Ok(Err(_)) | Err(_) => {
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
async fn collect_listener_urls(state: &DispatcherState, project_id: &str) -> Vec<ActivationUrl> {
    let mut out = Vec::new();
    let signals = state
        .journal
        .signal_list_for_project(project_id)
        .await
        .unwrap_or_default();
    for meta in signals {
        if meta.is_resume {
            continue;
        }
        if let Some(url) = meta.user_url.clone() {
            out.push(ActivationUrl {
                node_id: meta.node_id.clone(),
                kind: meta.kind.clone(),
                url,
            });
        }
    }
    out
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
            let entry_signals = if *is_trigger {
                serde_json::json!([{
                    "kind": { "kind": "webhook", "path": "", "auth": { "kind": "none" } },
                    "is_resume": false
                }])
            } else {
                serde_json::json!([])
            };
            n_json.push(serde_json::json!({
                "id": id,
                "nodeType": "T",
                "label": null,
                "config": {},
                "position": { "x": 0, "y": 0 },
                "features": { "isOutputDefault": is_output },
                "entrySignals": entry_signals,
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
    fn probe_entry_signals_roundtrip() {
        let p = project(&[("t", true, false)], &[]);
        assert_eq!(
            p.nodes[0].entry_signals.len(),
            1,
            "entrySignals should parse into a single WakeSignalSpec"
        );
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
