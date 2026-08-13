//! Node self-test runs: enqueue a `run_node_test` task (a short-lived
//! test pod, see `task_kinds::run_node_test`) and poll its outcome.
//!
//! Two verbs, both project-scoped and tenant-authenticated:
//!   POST /projects/{id}/node-tests/run      -> { taskId }
//!   GET  /projects/{id}/node-tests/runs/{task} -> { status, report?, error? }
//!
//! The caller supplies the per-package test image ref (it built or
//! ensured the image; the dispatcher spawns verbatim). Basic/fake
//! tiers normally run without any of this (the test binary runs where
//! the caller is); this surface exists for LIVE tests, whose
//! credential path only exists next to the broker.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::authenticator::{authorize_project, CallerTenant};
use crate::state::DispatcherState;
use crate::task_kinds::run_node_test::{RunNodeTestPayload, RUN_NODE_TEST_KIND};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunNodeTestRequest {
    pub image_ref: String,
    pub node: String,
    pub test: String,
    #[serde(default)]
    pub live_connection: Option<String>,
    /// Live-test fixture variables (`WEFT_NODE_TEST_*`), forwarded
    /// into the test pod's env.
    #[serde(default)]
    pub fixtures: std::collections::BTreeMap<String, String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RunNodeTestResponse {
    pub task_id: String,
}

pub async fn run(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<String>,
    Json(body): Json<RunNodeTestRequest>,
) -> Result<Json<RunNodeTestResponse>, (StatusCode, String)> {
    let id = id.parse::<uuid::Uuid>().map_err(|_| (StatusCode::BAD_REQUEST, "bad id".into()))?;
    authorize_project(&state, &caller.0, id).await?;

    let payload = RunNodeTestPayload {
        project_id: id.to_string(),
        tenant: caller.0 .0.clone(),
        image_ref: body.image_ref,
        list: false,
        node: Some(body.node),
        test: Some(body.test),
        live_connection: body.live_connection,
        fixtures: body.fixtures,
    };
    let task_id = weft_task_store::tasks::enqueue(
        &state.pg_pool,
        weft_task_store::NewTask {
            kind: RUN_NODE_TEST_KIND.to_string(),
            target: weft_task_store::TaskTarget::Dispatcher,
            project_id: Some(id.to_string()),
            dedup_key: None,
            color: None,
            tenant_id: Some(caller.0 .0.clone()),
            target_pod_name: None,
            binary_hash: None,
            payload: serde_json::to_value(&payload)
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?,
        },
    )
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("enqueue node test: {e}")))?;

    Ok(Json(RunNodeTestResponse { task_id: task_id.to_string() }))
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeTestRunStatus {
    /// `pending` / `claimed` / `complete` / `failed`.
    pub status: weft_task_store::tasks::TaskStatus,
    /// The runner's JSON report, present once the task completed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub report: Option<serde_json::Value>,
    /// The task's error, present when the RUN ITSELF failed (a failing
    /// test is a completed task whose report says `passed: false`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

pub async fn status(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path((id, task_id)): Path<(String, String)>,
) -> Result<Json<NodeTestRunStatus>, (StatusCode, String)> {
    let id = id.parse::<uuid::Uuid>().map_err(|_| (StatusCode::BAD_REQUEST, "bad id".into()))?;
    authorize_project(&state, &caller.0, id).await?;
    let task_id = task_id
        .parse::<uuid::Uuid>()
        .map_err(|_| (StatusCode::BAD_REQUEST, "bad task id".into()))?;

    let outcome =
        weft_task_store::tasks::peek_for_project(&state.pg_pool, task_id, &id.to_string())
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
            .ok_or_else(|| {
                (
                    StatusCode::NOT_FOUND,
                    "no such node-test run for this project (terminal runs are kept about \
                     an hour)"
                        .to_string(),
                )
            })?;

    Ok(Json(NodeTestRunStatus {
        status: outcome.status,
        report: outcome.result,
        error: outcome.error,
    }))
}
