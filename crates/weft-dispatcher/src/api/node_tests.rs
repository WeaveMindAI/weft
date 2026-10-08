//! Node self-test runs: enqueue a `run_node_test` task (a call to the
//! project's test server, see `task_kinds::run_node_test`) and poll its
//! outcome.
//!
//! Two verbs, both project-scoped and tenant-authenticated:
//!   POST /projects/{id}/node-tests/run      -> { taskId }
//!   GET  /projects/{id}/node-tests/runs/{task} -> { status, report?, error? }
//!
//! The caller supplies the per-package test image ref (it built or
//! ensured the image; the dispatcher runs it verbatim). Basic/fake
//! tiers normally run without any of this (the test binary runs where
//! the caller is); this surface exists for LIVE tests, whose
//! credential path only exists next to the broker.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use weft_core::node_test::{NodeTestRunStatus, RunNodeTestRequest, RunNodeTestResponse};

use crate::authenticator::{authorize_project, CallerTenant};
use crate::state::DispatcherState;
use crate::task_kinds::run_node_test::{RunNodeTestPayload, RUN_NODE_TEST_KIND};

pub async fn run(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
    Json(body): Json<RunNodeTestRequest>,
) -> Result<Json<RunNodeTestResponse>, (StatusCode, String)> {
    authorize_project(&state, &caller.0, id).await?;

    let payload = RunNodeTestPayload {
        project_id: id,
        tenant: caller.0 .0.clone(),
        image_ref: body.image_ref,
        node: body.node,
        test: body.test,
        live_connection: body.live_connection,
        fixtures: body.fixtures,
    };
    let task_id = weft_task_store::tasks::enqueue(
        &state.pg_pool,
        weft_task_store::NewTask {
            kind: RUN_NODE_TEST_KIND.to_string(),
            project_id: Some(id),
            dedup_key: None,
            execution_id: None,
            tenant_id: caller.0 .0.clone(),
            payload: serde_json::to_value(&payload)
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?,
        },
    )
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("enqueue node test: {e}")))?;

    Ok(Json(RunNodeTestResponse { task_id: task_id.to_string() }))
}

pub async fn status(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path((id, task_id)): Path<(uuid::Uuid, String)>,
    Query(hold): Query<super::HoldQuery>,
) -> Result<Json<NodeTestRunStatus>, (StatusCode, String)> {
    authorize_project(&state, &caller.0, id).await?;
    let task_id = task_id
        .parse::<uuid::Uuid>()
        .map_err(|_| (StatusCode::BAD_REQUEST, "bad task id".into()))?;

    // Every read is scoped to this project. A client waiting on a run
    // holds here until it finishes, or the hold runs out, instead of
    // asking on a timer; a zero hold is one read.
    let outcome = weft_task_store::terminal::wait_for_terminal_in_project(
        &state.pg_pool,
        &state.signals,
        task_id,
        id,
        hold.hold(),
    )
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

    // The report is the runner's `TestReport`; one that does not read as
    // one is a runner and dispatcher out of step, said as such.
    let report = outcome
        .result
        .map(serde_json::from_value)
        .transpose()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("the node-test run's report does not read as a test report: {e}")))?;
    Ok(Json(NodeTestRunStatus { status: outcome.status, report, error: outcome.error }))
}
