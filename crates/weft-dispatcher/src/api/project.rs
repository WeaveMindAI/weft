//! Project lifecycle HTTP handlers. `POST /projects` registers a
//! project; `POST /projects/{id}/run` kicks off a fresh execution.
//! `weft run` on the CLI calls these.

use std::collections::HashSet;

use axum::{extract::{Path, State}, http::StatusCode, Json};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use weft_compiler::{Diagnostic, Severity};
use weft_core::primitive::WakeSignalKind;
use weft_core::project::EdgeIndex;
use weft_core::ProjectDefinition;
use weft_catalog::stdlib_catalog;

use crate::backend::WakeContext;
use weft_core::primitive::RootSeed;
use crate::events::DispatcherEvent;
use crate::journal::EntryKind;
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
    /// Absolute path to the project root. Unused today; reserved for
    /// multi-file imports once the compiler resolves them.
    #[serde(default)]
    pub root: Option<String>,
    /// Absolute path to the compiled project binary. Supplied by
    /// `weft run` / `weft build` after the local compile step. The
    /// dispatcher spawns this binary per wake; if it doesn't exist
    /// at spawn time, the wake fails with a clear error.
    #[serde(default)]
    pub binary_path: Option<String>,
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

    let binary_path = req.binary_path.clone().map(std::path::PathBuf::from);
    let summary = state.projects.register(project, binary_path).await.map_err(|e| {
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
    for seed in &seeds {
        let _ = state
            .journal
            .record_event(&crate::journal::ExecEvent::PulseSeeded {
                color,
                node_id: seed.node_id.clone(),
                port: "__seed__".to_string(),
                lane: Vec::new(),
                value: seed.value.clone(),
                at_unix: now,
            })
            .await;
    }

    // Enqueue the Manual wake in the slot so it lands in the Start
    // message the moment the worker connects. Seeds are cloned into
    // the core type so the slot state is self-contained.
    let core_seeds: Vec<weft_core::primitive::RootSeed> = seeds
        .into_iter()
        .map(|s| weft_core::primitive::RootSeed { node_id: s.node_id, value: s.value })
        .collect();
    state
        .slots
        .with_slot(color, move |slot| {
            Box::pin(async move {
                let queued = match slot {
                    crate::slots::Slot::Idle { queued, .. }
                    | crate::slots::Slot::Starting { queued, .. }
                    | crate::slots::Slot::WaitingReconnect { queued, .. } => queued,
                    crate::slots::Slot::Live { .. } => {
                        // Live shouldn't happen for a fresh run
                        // (we minted a brand-new color). Defensive:
                        // overwrite with Idle + queued.
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
                    weft_core::primitive::WakeMessage::Fresh { seeds: core_seeds },
                ));
            })
        })
        .await;

    let wake = WakeContext { project_id: id.to_string(), color };
    let worker = state
        .workers
        .spawn_worker(&summary.binary_path, wake)
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
        .map(|id| RootSeed { node_id: id, value: payload.clone() })
        .collect()
}

/// Seeds for a trigger fire. Steps:
///   1. Walk downstream from `firing_node_id` to collect every node
///      it can reach through outgoing edges.
///   2. Intersect with output nodes (`is_output: true`).
///   3. Compute the upstream subgraph that feeds those outputs.
///   4. Every root of that subgraph is a seed. The firing trigger
///      gets `payload`; every other root (including other triggers
///      reachable in the subgraph) gets `null`.
///
/// Returns an empty vec if the firing trigger doesn't reach any
/// output node; the caller treats that as "nothing to run."
pub fn compute_trigger_seeds(
    project: &ProjectDefinition,
    firing_node_id: &str,
    payload: &Value,
) -> Vec<RootSeed> {
    let edge_idx = EdgeIndex::build(project);

    // 1. Downstream closure from firing trigger.
    let mut downstream: HashSet<String> = HashSet::new();
    let mut frontier: Vec<String> = vec![firing_node_id.to_string()];
    while let Some(node_id) = frontier.pop() {
        if !downstream.insert(node_id.clone()) {
            continue;
        }
        for edge in edge_idx.get_outgoing(project, &node_id) {
            if !downstream.contains(&edge.target) {
                frontier.push(edge.target.clone());
            }
        }
    }

    // 2. Output nodes inside the downstream set.
    let targets: Vec<String> = project
        .nodes
        .iter()
        .filter(|n| downstream.contains(&n.id) && n.is_output())
        .map(|n| n.id.clone())
        .collect();
    if targets.is_empty() {
        return Vec::new();
    }

    // 3. Upstream closure from those outputs.
    let in_subgraph = upstream_closure(project, &edge_idx, &targets);

    // 4. Roots of the subgraph. The firing trigger gets the payload;
    //    every other root gets null.
    roots_of(project, &edge_idx, &in_subgraph)
        .into_iter()
        .map(|id| {
            let value = if id == firing_node_id {
                payload.clone()
            } else {
                Value::Null
            };
            RootSeed { node_id: id, value }
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
    let mut seen: HashSet<String> = HashSet::new();
    let mut frontier: Vec<String> = targets.to_vec();
    while let Some(node_id) = frontier.pop() {
        if !seen.insert(node_id.clone()) {
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
    let mut roots = Vec::new();
    for node_id in in_subgraph {
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

/// Activate a project. For each node that declares an entry
/// primitive, mint an entry token and return the user-facing URL.
/// Webhook tokens under `/w/{token}/{path}`, cron tokens are
/// registered for scheduled firing (future), manual ones just get
/// surfaced for completeness.
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

    // Drop stale tokens before minting fresh ones. The scheduler's
    // prior timers are replaced atomically at the end of this
    // handler via `replace_project`, so we don't wipe them yet.
    let project_id = id.to_string();
    state
        .journal
        .drop_entry_tokens(&project_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("drop tokens: {e}")))?;

    let base = format!("http://localhost:{}", state.config.http_port);
    let mut urls = Vec::new();
    let mut timer_registrations: Vec<crate::scheduler::TimerRegistration> = Vec::new();

    for node in &project.nodes {
        for spec in &node.entry_signals {
            // Entry-use specs must be is_resume=false; validated here
            // as a runtime guard (compiler will enforce this earlier
            // in Slice 1+).
            if spec.is_resume {
                tracing::warn!(
                    target: "weft_dispatcher::activate",
                    node = %node.id,
                    "entry signal with is_resume=true; skipping (invalid)"
                );
                continue;
            }
            let (kind_str, path_hint, auth_json, url) = match &spec.kind {
                WakeSignalKind::Webhook { path, auth } => {
                    let auth_json = serde_json::to_value(auth).ok();
                    let token = state
                        .journal
                        .mint_entry_token(
                            &project_id,
                            &node.id,
                            EntryKind::Webhook,
                            Some(path.as_str()),
                            auth_json.clone(),
                        )
                        .await
                        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("mint: {e}")))?;
                    let url = if path.is_empty() {
                        format!("{base}/w/{token}")
                    } else {
                        format!("{base}/w/{token}/{path}")
                    };
                    ("webhook", Some(path.clone()), auth_json, url)
                }
                WakeSignalKind::Timer { spec: timer } => {
                    // `timer` is already fully resolved from the
                    // node's config by enrich. Mint the tracking
                    // token and collect a registration; we hand the
                    // whole batch to the scheduler atomically at
                    // the end so concurrent activates cannot leave
                    // orphan timers.
                    let serialized = serde_json::to_value(timer).ok();
                    let token = state
                        .journal
                        .mint_entry_token(
                            &project_id,
                            &node.id,
                            EntryKind::Cron,
                            None,
                            serialized,
                        )
                        .await
                        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("mint: {e}")))?;
                    timer_registrations.push(crate::scheduler::TimerRegistration {
                        node_id: node.id.clone(),
                        entry_token: token.clone(),
                        spec: timer.clone(),
                        binary_path: summary.binary_path.clone(),
                    });
                    let url = format!("timer (token {token})");
                    ("timer", None, None, url)
                }
                WakeSignalKind::Form { form_type, .. } => {
                    // Form entry (HumanTrigger style). Minted as a
                    // webhook-flavored token for now; Slice 3 will
                    // route form submissions properly.
                    let token = state
                        .journal
                        .mint_entry_token(
                            &project_id,
                            &node.id,
                            EntryKind::Webhook,
                            Some(form_type.as_str()),
                            None,
                        )
                        .await
                        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("mint: {e}")))?;
                    let url = format!("{base}/f/{token}");
                    ("form", Some(form_type.clone()), None, url)
                }
                WakeSignalKind::Socket { .. } => {
                    // Phase B.
                    continue;
                }
            };

            let _ = (path_hint, auth_json);
            urls.push(ActivationUrl {
                node_id: node.id.clone(),
                kind: kind_str.to_string(),
                url,
            });
        }
    }

    // Atomic swap: abort any prior timer tasks for this project
    // and spawn the new set in one critical section.
    state
        .scheduler
        .replace_project(project_id.clone(), timer_registrations, state.clone());

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
    let project_id = id.to_string();
    state
        .journal
        .drop_entry_tokens(&project_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("drop tokens: {e}")))?;
    state.scheduler.cancel_project(&project_id);
    if !state.projects.set_status(id, ProjectStatus::Inactive).await {
        return Err((StatusCode::NOT_FOUND, "project not found".into()));
    }
    state
        .events
        .publish(DispatcherEvent::ProjectDeactivated { project_id })
        .await;
    Ok(StatusCode::NO_CONTENT)
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
