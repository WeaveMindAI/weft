//! A worker's records and the runs it drives: its writer lanes' batches
//! (`weft_journal::frame`, written in one statement on the broker's own
//! record pool), a run's record read back, a queued run claimed, the
//! answers and cancels waiting for the runs it drives. Every one is about
//! the asking worker's own project and its own runs, from its verified
//! identity: a batch's continuing runs are fenced on the calling replica
//! in the write itself, and a birth must name the worker's project.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Json, Router};

use weft_broker_client::protocol::{
    RecordOfRequest, RecordOfResponse, RunAnswer, RunAnswersRequest, RunAnswersResponse, RunCancel, RunCancelsRequest, RunCancelsResponse,
    RunClaimRequest, RunClaimResponse, RunGiveUpRequest, RunGiveUpResponse, JOURNAL_RECORD_BODY_LIMIT, JOURNAL_RECORD_PATH,
};
use weft_journal::frame::BatchAnswer;
use weft_journal::record::Fate;

use crate::auth::{AuthedCaller, CallerIdentity};
use crate::door::worker;
use crate::handlers::unavailable_or_internal;
use crate::state::BrokerState;

type Resp<T> = Result<Json<T>, (StatusCode, String)>;

pub fn routes() -> Router<Arc<BrokerState>> {
    Router::new()
        .route(JOURNAL_RECORD_PATH, post(record_batch).layer(axum::extract::DefaultBodyLimit::max(JOURNAL_RECORD_BODY_LIMIT)))
        .route("/v1/journal/record_of", post(record_of))
        .route("/v1/run/claim", post(run_claim))
        .route("/v1/run/answers", post(run_answers))
        .route("/v1/run/cancels", post(run_cancels))
        .route("/v1/run/give_up", post(run_give_up))
}

/// `POST /v1/journal/record`: one batch of the calling worker's writer
/// lane, written in one statement (`weft_journal::record::record_batch`),
/// on the record pool so a burst of records never starves the broker's
/// other work. Once it commits, the dispatcher is told what changed
/// ([`crate::notices`]).
async fn record_batch(State(state): State<Arc<BrokerState>>, AuthedCaller(caller): AuthedCaller, body: Bytes) -> Resp<BatchAnswer> {
    let (tenant, project, replica) = worker(&caller)?;
    let batch = weft_journal::frame::decode(&body).map_err(|e| (StatusCode::BAD_REQUEST, format!("a batch of records does not read: {e:#}")))?;
    if let Some(stray) = batch.head.runs.iter().filter_map(|run| run.born.as_ref()).find(|born| born.project_id != project) {
        tracing::warn!(target: "weft_broker::scope", caller_project = %project, born_in = %stray.project_id, "broker refused a birth in another project");
        return Err((StatusCode::FORBIDDEN, "a worker bears runs of its own project only".into()));
    }
    // How many runs a batch carries, for a measurement of the write path
    // (the bench reads it with `weft_broker::batch=debug`).
    tracing::debug!(target: "weft_broker::batch", runs = batch.head.runs.len(), bytes = body.len(), "a batch of records");
    let lane = format!("{replica}/{}", batch.head.lane);
    let mut conn = state.record_pool.acquire().await.map_err(|e| unavailable_or_internal(e.into()))?;
    let recorded = weft_journal::record::record_batch(&mut conn, replica, tenant, &lane, &batch)
        .await
        .map_err(|e| unavailable_or_internal(e.context("write a batch of records")))?;
    drop(conn);
    let accepted = |at: usize| recorded[at].fate == Fate::Accepted;
    state.notices.tell(
        recorded.iter().filter(|run| run.fate == Fate::Accepted).filter_map(|run| run.project_id.map(|project| (project, run.execution_id))),
        recorded.iter().filter(|run| run.tell_end).map(|run| run.execution_id),
    );
    // A run that ended having stored files queued its sweep through the
    // announcement outbox, which goes out once poked.
    if batch.head.runs.iter().enumerate().any(|(at, run)| accepted(at) && run.wrote_files && run.written.ended.is_some()) {
        weft_task_store::announce::committed(&state.pool);
    }
    Ok(Json(BatchAnswer { fates: recorded.into_iter().map(|run| run.fate).collect() }))
}

/// The calling worker's run `execution_id`, as its scope says: refused when
/// it is another project's.
async fn own_run(state: &BrokerState, caller: &CallerIdentity, execution_id: weft_core::ExecutionId) -> Result<(), (StatusCode, String)> {
    crate::scope::require_execution_id_scope(&state.scope_cache, &state.pool, caller, execution_id).await?;
    Ok(())
}

/// `POST /v1/journal/record_of`: see [`RecordOfRequest`].
async fn record_of(State(state): State<Arc<BrokerState>>, AuthedCaller(caller): AuthedCaller, Json(req): Json<RecordOfRequest>) -> Resp<RecordOfResponse> {
    worker(&caller)?;
    own_run(&state, &caller, req.execution_id).await?;
    let mut conn = state.pool.acquire().await.map_err(|e| unavailable_or_internal(e.into()))?;
    let record = weft_journal::record::read_record(&mut conn, req.execution_id, None)
        .await
        .map_err(|e| unavailable_or_internal(e.context("read a run's record")))?;
    Ok(Json(RecordOfResponse { record }))
}

/// `POST /v1/run/claim`: see [`RunClaimRequest`]. The claim is the
/// project's own: a run of another project is not queued for this worker.
async fn run_claim(State(state): State<Arc<BrokerState>>, AuthedCaller(caller): AuthedCaller, Json(req): Json<RunClaimRequest>) -> Resp<RunClaimResponse> {
    let (_, project, replica) = worker(&caller)?;
    let mut conn = state.pool.acquire().await.map_err(|e| unavailable_or_internal(e.into()))?;
    let claimed = weft_journal::record::claim(&mut conn, req.execution_id, project, replica)
        .await
        .map_err(|e| unavailable_or_internal(e.context("claim a run")))?;
    Ok(Json(RunClaimResponse { claimed }))
}

/// `POST /v1/run/answers`: see [`RunAnswersRequest`]. Only the worker
/// driving the run takes its answers; the hold wakes when an answer is
/// parked for the run's project.
async fn run_answers(State(state): State<Arc<BrokerState>>, AuthedCaller(caller): AuthedCaller, Json(req): Json<RunAnswersRequest>) -> Resp<RunAnswersResponse> {
    let (_, project, _) = worker(&caller)?;
    crate::handlers::require_worker_drives(&state, &caller, req.execution_id).await?;
    let deadline = tokio::time::Instant::now() + Duration::from_millis(req.wait_ms).min(weft_task_store::pg_signal::MAX_HOLD);
    let project = project.to_string();
    // Subscribed before the first read, so an answer parked between an
    // empty read and the wait still ends the wait.
    let mut heard = state.signals.subscribe();
    loop {
        let mut answers = weft_task_store::parked_fires::answers_for(&state.pool, req.execution_id)
            .await
            .map_err(|e| unavailable_or_internal(e.context("read a run's answers")))?;
        answers.retain(|(token, _)| !req.taken.contains(token));
        let parked = |channel: &str, payload: &str| channel == weft_task_store::parked_fires::PARKED_FIRE_CHANNEL && payload == project;
        if !answers.is_empty()
            || !heard.woken_before(deadline, parked).await.map_err(|e| unavailable_or_internal(e.context("wait for a run's answers")))?
        {
            return Ok(Json(RunAnswersResponse { answers: answers.into_iter().map(|(token, value)| RunAnswer { token, value }).collect() }));
        }
    }
}

/// `POST /v1/run/give_up`: see [`RunGiveUpRequest`]. Its ending is a
/// record like any batch's, so the dispatcher hears of it the same way.
async fn run_give_up(State(state): State<Arc<BrokerState>>, AuthedCaller(caller): AuthedCaller, Json(req): Json<RunGiveUpRequest>) -> Resp<RunGiveUpResponse> {
    let (_, _, replica) = worker(&caller)?;
    own_run(&state, &caller, req.execution_id).await?;
    let mut tx = state.record_pool.begin().await.map_err(|e| unavailable_or_internal(e.into()))?;
    let gave_up = weft_journal::record::give_up_in(&mut tx, req.execution_id, replica, &req.error)
        .await
        .map_err(|e| unavailable_or_internal(e.context("end a run its worker gave up")))?;
    let weft_journal::record::GaveUp::Ended(seq) = gave_up else {
        return Err((StatusCode::CONFLICT, format!("execution {} is not driven by this worker", req.execution_id)));
    };
    tx.commit().await.map_err(|e| unavailable_or_internal(e.into()))?;
    // Its row and its ending were announced with the write, through the
    // outbox.
    weft_task_store::announce::committed(&state.pool);
    Ok(Json(RunGiveUpResponse { ended_at_seq: seq }))
}

/// `POST /v1/run/cancels`: see [`RunCancelsRequest`].
async fn run_cancels(State(state): State<Arc<BrokerState>>, AuthedCaller(caller): AuthedCaller, Json(_): Json<RunCancelsRequest>) -> Resp<RunCancelsResponse> {
    let (_, project, replica) = worker(&caller)?;
    let rows: Vec<(weft_core::ExecutionId, sqlx::types::Json<weft_core::exec::CancelCause>)> = sqlx::query_as(
        "SELECT execution_id, cancel_requested FROM run \
         WHERE owner = $1 AND state = 'running' AND project_id = $2 AND cancel_requested IS NOT NULL",
    )
    .bind(replica)
    .bind(project)
    .fetch_all(&state.pool)
    .await
    .map_err(|e| unavailable_or_internal(anyhow::Error::from(e).context("read the cancels of a worker's runs")))?;
    Ok(Json(RunCancelsResponse { cancels: rows.into_iter().map(|(execution_id, cause)| RunCancel { execution_id, cause: cause.0 }).collect() }))
}
