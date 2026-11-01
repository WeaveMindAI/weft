//! A worker's records and the runs it drives: its writer lanes' batches
//! (`weft_journal::frame`, written in one statement on the broker's own
//! record pool), a run's record read back, a queued run claimed, the
//! answers and cancels waiting for the runs it drives. Every one is about
//! the asking worker's own project and its own runs, from its verified
//! identity: a batch's continuing runs are fenced on the calling replica
//! in the write itself, and a birth must name the worker's project.

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse as _, Response};
use axum::routing::post;
use axum::Router;

use weft_broker_client::protocol::{
    RecordOfRequest, RecordOfResponse, RecordSecretRequest, RecordSecretResponse, RunAnswer, RunAnswerOf, RunAnswersRequest, RunAnswersResponse, RunCancel, RunClaimRequest,
    RunClaimResponse, RunGiveUpRequest, RunGiveUpResponse, RunRecheckedRequest, RunRecheckedResponse,
    JOURNAL_RECORD_BODY_LIMIT,
    BATCH_FATES, BATCH_FATES_HEADER, JOURNAL_RECORD_PATH,
};
use weft_journal::frame::BatchAnswer;
use weft_journal::record::Fate;

use weft_task_store::db::{self, Client as _, Pool};

use crate::auth::{AuthedCaller, CallerIdentity};
use crate::door::worker;
use crate::handlers::unavailable_or_internal;
use crate::state::BrokerState;
use weft_core::wire_shape::Json;

type Resp<T> = Result<Json<T>, (StatusCode, String)>;

pub fn routes() -> Router<Arc<BrokerState>> {
    Router::new()
        .route(JOURNAL_RECORD_PATH, post(record_batch).layer(axum::extract::DefaultBodyLimit::max(JOURNAL_RECORD_BODY_LIMIT)))
        .route("/v1/journal/record_of", post(record_of))
        .route("/v1/journal/secret", post(record_secret))
        .route("/v1/run/claim", post(run_claim))
        .route("/v1/run/answers", post(run_answers))
        .route("/v1/run/rechecked", post(run_rechecked))
        .route(PREVIOUS_RELEASE_CANCELS_PATH, post(previous_release_run_cancels))
        .route("/v1/run/give_up", post(run_give_up))
}

/// `POST /v1/journal/record` ([`JOURNAL_RECORD_PATH`]): see
/// [`write_batch`]. A worker of the previous release, which still sends
/// batches while an upgrade rolls out (its program keeps running until it
/// is built again), says nothing of the fates it reads
/// ([`BATCH_FATES_HEADER`]) and is answered as it reads them
/// ([`previous_release_answer`]).
async fn record_batch(State(state): State<Arc<BrokerState>>, AuthedCaller(caller): AuthedCaller, headers: HeaderMap, body: Bytes) -> Resp<BatchAnswer> {
    let answer = write_batch(&state, &caller, &body).await?;
    let reads_every_fate = headers.get(BATCH_FATES_HEADER).is_some_and(|fates| fates.as_bytes() == BATCH_FATES.as_bytes());
    Ok(Json(if reads_every_fate { answer } else { previous_release_answer(answer) }))
}

/// `answer` as the previous release's worker reads it: a birth for a
/// project being removed answered as refused, the one fate it reads for
/// it (an answer holding a fate it does not know fails its whole batch,
/// the runs accepted included). The release after this one drops it.
pub fn previous_release_answer(answer: BatchAnswer) -> BatchAnswer {
    BatchAnswer {
        fates: answer
            .fates
            .into_iter()
            .map(|fate| match fate {
                Fate::ProjectRemoving => Fate::Refused,
                fate => fate,
            })
            .collect(),
    }
}

/// One batch of the calling worker's writer lane, written in one
/// statement (`weft_journal::record::record_batch`), on the record pool so
/// a burst of records never starves the broker's other work. Once it
/// commits, the dispatcher is told what changed ([`crate::notices`]).
async fn write_batch(state: &BrokerState, caller: &CallerIdentity, body: &Bytes) -> Result<BatchAnswer, (StatusCode, String)> {
    let (tenant, project, replica) = worker(caller)?;
    let batch = weft_journal::frame::decode(body).map_err(|e| {
        // A head of another shape is the call's body of another shape
        // (`crate::wire`); a frame that is cut short is not.
        let shape = e.chain().filter_map(|cause| cause.downcast_ref::<serde_json::Error>()).find(|e| e.classify() == serde_json::error::Category::Data);
        match shape {
            Some(shape) => state.unreadable_shapes.refuse(crate::wire::Sender::of_caller(caller), JOURNAL_RECORD_PATH, shape),
            None => (StatusCode::BAD_REQUEST, format!("a batch of records does not read: {e:#}")),
        }
    })?;
    if let Some(stray) = batch.head.runs.iter().filter_map(|run| run.born.as_ref()).find(|born| born.project_id != project) {
        tracing::warn!(target: "weft_broker::scope", caller_project = %project, born_in = %stray.project_id, "broker refused a birth in another project");
        return Err((StatusCode::FORBIDDEN, "a worker bears runs of its own project only".into()));
    }
    // A cut a birth runs is kept under its hash, and every later reader
    // runs what that hash names: it is of this project, is what it says it
    // is, and is the cut a birth in this batch runs.
    for program in &batch.head.programs {
        if program.project_id != project {
            tracing::warn!(target: "weft_broker::scope", caller_project = %project, stored_for = %program.project_id, "broker refused a program of another project");
            return Err((StatusCode::FORBIDDEN, "a worker stores programs of its own project only".into()));
        }
        program.check().map_err(|e| (StatusCode::BAD_REQUEST, format!("a batch of records carries a program that is not what it says: {e:#}")))?;
        let named = batch.head.runs.iter().filter_map(|run| run.born.as_ref())
            .any(|born| born.cut_from.is_some() && born.definition_hash.as_deref() == Some(program.definition_hash.as_str()));
        if !named {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("a batch of records carries program {}, which no birth in it runs as a cut", program.definition_hash),
            ));
        }
    }
    // How many runs a batch carries, for a measurement of the write path
    // (the bench reads it with `weft_broker::batch=debug`).
    tracing::debug!(target: "weft_broker::batch", runs = batch.head.runs.len(), bytes = body.len(), "a batch of records");
    let lane = format!("{replica}/{}", batch.head.lane);
    let conn = state.record_pool.get().await.map_err(|e| unavailable_or_internal(e.into()))?;
    // A secret a run made is sealed here, before it is bound: its value
    // never reaches the database in the clear.
    let recorded = weft_journal::record::record_batch(&conn, replica, tenant, &lane, &batch, &weft_access_store::seal_json)
        .await
        .map_err(|e| unavailable_or_internal(e.context("write a batch of records")))?;
    drop(conn);
    state.notices.tell(
        recorded.iter().filter(|run| run.fate == Fate::Accepted).filter_map(|run| run.project_id.map(|project| (project, run.run_id))),
        recorded.iter().filter(|run| run.tell_end).map(|run| run.run_id),
        recorded.iter().filter_map(|run| run.keep_until),
        recorded.iter().any(|run| run.unwatched).then_some(replica),
    );
    Ok(BatchAnswer { fates: recorded.into_iter().map(|run| run.fate).collect() })
}

/// The calling worker's run `run_id`, as its scope says: refused when
/// it is another project's.
async fn own_run(state: &BrokerState, caller: &CallerIdentity, run_id: weft_core::RunId) -> Result<(), (StatusCode, String)> {
    crate::scope::require_run_id_scope(&state.scope_cache, &state.pool, caller, run_id).await?;
    Ok(())
}

/// `POST /v1/journal/record_of`: see [`RecordOfRequest`]. A page is a few
/// megabytes at most (`weft_journal::record::PAGE_BYTES`), written on the
/// blocking pool, never on a thread serving calls.
async fn record_of(State(state): State<Arc<BrokerState>>, AuthedCaller(caller): AuthedCaller, Json(req): Json<RecordOfRequest>) -> Result<Response, (StatusCode, String)> {
    worker(&caller)?;
    own_run(&state, &caller, req.run_id).await?;
    let record = weft_journal::record::read_page(&state.pool, req.run_id, req.after)
        .await
        .map_err(|e| unavailable_or_internal(e.context("read a run's record")))?;
    let body = weft_journal::on_blocking_pool(move || serde_json::to_vec(&RecordOfResponse { record }))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("write the answer: {e}")))?;
    Ok(([(axum::http::header::CONTENT_TYPE, "application/json")], body).into_response())
}

/// `POST /v1/journal/secret`: see [`RecordSecretRequest`]. Any run of the
/// calling worker's project reveals a secret any run of it made (a run
/// picked up again, a seeded run, a marker a program kept and reads back
/// later): the project is the scope, read off the caller.
async fn record_secret(State(state): State<Arc<BrokerState>>, AuthedCaller(caller): AuthedCaller, Json(req): Json<RecordSecretRequest>) -> Result<Json<RecordSecretResponse>, (StatusCode, String)> {
    let (_, project, _) = worker(&caller)?;
    let value = revealed(&state.pool, project, req.secret_id).await.map_err(|e| unavailable_or_internal(e.context("reveal a secret")))?;
    match value {
        Some(value) => Ok(Json(RecordSecretResponse { value })),
        None => Err((StatusCode::NOT_FOUND, format!("no run of this project keeps secret {}", req.secret_id))),
    }
}

/// The value of secret `secret_id`, of a run of `project`, opened from its
/// sealed copy (`run_secret`); `None` when no run of the project keeps it.
pub async fn revealed(pool: &Pool, project: uuid::Uuid, secret_id: weft_core::secret::SecretId) -> anyhow::Result<Option<serde_json::Value>> {
    let sealed: Option<String> = pool
        .scalar_opt("SELECT sealed FROM run_secret WHERE secret_id = $1 AND project_id = $2", &[&secret_id.as_uuid(), &project])
        .await?;
    sealed.map(|sealed| weft_access_store::open_json(&sealed)).transpose()
}

/// `POST /v1/run/claim`: see [`RunClaimRequest`]. The claim is the
/// project's own: a run of another project is not queued for this worker.
async fn run_claim(State(state): State<Arc<BrokerState>>, AuthedCaller(caller): AuthedCaller, Json(req): Json<RunClaimRequest>) -> Resp<RunClaimResponse> {
    let (_, project, replica) = worker(&caller)?;
    let conn = state.pool.get().await.map_err(|e| unavailable_or_internal(e.into()))?;
    let claimed = weft_journal::record::claim(&conn, req.run_id, project, replica)
        .await
        .map_err(|e| unavailable_or_internal(e.context("claim a run")))?;
    Ok(Json(RunClaimResponse { claimed }))
}

/// `POST /v1/run/answers`: see [`RunAnswersRequest`]. Only the worker
/// driving the run takes its answers, read once: the worker asks when its
/// run starts waiting, and again when the line tells it one was handed to
/// the run (`weft_broker_client::line::PARKED_FIRE_CHANNEL`).
async fn run_answers(State(state): State<Arc<BrokerState>>, AuthedCaller(caller): AuthedCaller, Json(req): Json<RunAnswersRequest>) -> Resp<RunAnswersResponse> {
    worker(&caller)?;
    crate::handlers::require_worker_drives(&state, &caller, req.run_id).await?;
    let mut answers = weft_task_store::parked_fires::answers_for(&state.pool, req.run_id)
        .await
        .map_err(|e| unavailable_or_internal(e.context("read a run's answers")))?;
    answers.retain(|(token, _)| !req.taken.contains(token));
    Ok(Json(RunAnswersResponse { answers: answers.into_iter().map(|(token, answer)| RunAnswer { token, answer }).collect() }))
}

/// `POST /v1/run/give_up`: see [`RunGiveUpRequest`]. Its ending is a
/// record like any batch's, so the dispatcher hears of it the same way.
async fn run_give_up(State(state): State<Arc<BrokerState>>, AuthedCaller(caller): AuthedCaller, Json(req): Json<RunGiveUpRequest>) -> Resp<RunGiveUpResponse> {
    let (_, _, replica) = worker(&caller)?;
    own_run(&state, &caller, req.run_id).await?;
    let mut conn = state.record_pool.get().await.map_err(|e| unavailable_or_internal(e.into()))?;
    let tx = conn.transaction().await.map_err(|e| unavailable_or_internal(e.into()))?;
    let gave_up = weft_journal::record::give_up_in(&tx, req.run_id, replica, &req.error)
        .await
        .map_err(|e| unavailable_or_internal(e.context("end a run its worker gave up")))?;
    let weft_journal::record::GaveUp::Ended(seq) = gave_up else {
        return Err((StatusCode::CONFLICT, format!("run {} is not driven by this worker", req.run_id)));
    };
    tx.commit().await.map_err(|e| unavailable_or_internal(e.into()))?;
    // Its row and its ending were announced with the write, through the
    // outbox.
    weft_task_store::announce::committed(&state.pool);
    Ok(Json(RunGiveUpResponse { ended_at_seq: seq }))
}

/// `POST /v1/run/rechecked`: see [`RunRecheckedRequest`] and [`rechecked`].
async fn run_rechecked(State(state): State<Arc<BrokerState>>, AuthedCaller(caller): AuthedCaller, Json(_): Json<RunRecheckedRequest>) -> Resp<RunRecheckedResponse> {
    let (_, project, replica) = worker(&caller)?;
    let rechecked = rechecked(&state.pool, replica, project)
        .await
        .map_err(|e| unavailable_or_internal(e.context("read the cancels and answers of a worker's runs")))?;
    Ok(Json(rechecked))
}

/// The cancels and the answers of every run `replica` drives in `project`,
/// answers oldest first, in one statement. The runs are the worker's own
/// by the read itself (`owner`), so no run is checked one by one.
pub async fn rechecked(pool: &Pool, replica: &str, project: uuid::Uuid) -> anyhow::Result<RunRecheckedResponse> {
    type Row = (weft_core::RunId, Option<db::JsonText<weft_core::run::CancelCause>>, Option<String>, Option<db::JsonText<serde_json::Value>>, bool);
    let sql = format!(
        "WITH mine AS ( \
             SELECT run_id, cancel_requested FROM run \
             WHERE owner = $1 AND project_id = $2 AND state = 'running' \
         ) \
         SELECT m.run_id, m.cancel_requested, NULL::text AS token, NULL::text AS payload, FALSE AS skipped, 0::bigint AS seq \
             FROM mine m WHERE m.cancel_requested IS NOT NULL \
         UNION ALL \
         SELECT a.run_id, NULL, a.token, a.payload, a.skipped, a.seq \
             FROM ({answers}) a \
         ORDER BY seq",
        answers = weft_task_store::parked_fires::handed_answers_sql("SELECT run_id FROM mine"),
    );
    let rows: Vec<Row> = pool.all(&sql, &[&replica, &project]).await?;
    let mut rechecked = RunRecheckedResponse::default();
    for (run_id, cause, token, payload, skipped) in rows {
        match (cause, token, payload) {
            (Some(cause), None, None) => rechecked.cancels.push(RunCancel { run_id, cause: cause.0 }),
            (None, Some(token), Some(payload)) => rechecked.answers.push(RunAnswerOf {
                run_id,
                token,
                answer: weft_task_store::parked_fires::stored_answer(payload.0, skipped),
            }),
            (cause, token, payload) => {
                anyhow::bail!("a recheck row of {run_id} is neither a cancel nor an answer: {cause:?} {token:?} {payload:?}")
            }
        }
    }
    Ok(rechecked)
}

/// The route a worker of the previous release reads its runs' cancels
/// from once its line came back, which it still calls while an upgrade
/// rolls out (its program keeps running until it is built again). The
/// release after this one drops it, with [`previous_release_run_cancels`],
/// [`previous_release_cancels`] and [`PreviousReleaseCancels`].
pub const PREVIOUS_RELEASE_CANCELS_PATH: &str = "/v1/run/cancels";

/// What [`PREVIOUS_RELEASE_CANCELS_PATH`] answers, in the shape the
/// previous release's worker reads: the cancels only.
#[derive(Debug, serde::Serialize)]
pub struct PreviousReleaseCancels {
    pub cancels: Vec<RunCancel>,
}

/// `POST /v1/run/cancels`: see [`PREVIOUS_RELEASE_CANCELS_PATH`]. Its
/// request was an empty object, so the body is not read.
async fn previous_release_run_cancels(State(state): State<Arc<BrokerState>>, AuthedCaller(caller): AuthedCaller) -> Resp<PreviousReleaseCancels> {
    let (_, project, replica) = worker(&caller)?;
    let cancels = previous_release_cancels(&state.pool, replica, project)
        .await
        .map_err(|e| unavailable_or_internal(e.context("read the cancels of a worker's runs")))?;
    Ok(Json(cancels))
}

/// The cancels waiting for every run `replica` drives in `project`, as the
/// previous release answered them.
pub async fn previous_release_cancels(pool: &Pool, replica: &str, project: uuid::Uuid) -> anyhow::Result<PreviousReleaseCancels> {
    let rows: Vec<(weft_core::RunId, db::JsonText<weft_core::run::CancelCause>)> = pool
        .all(
            "SELECT run_id, cancel_requested FROM run \
             WHERE owner = $1 AND project_id = $2 AND state = 'running' AND cancel_requested IS NOT NULL",
            &[&replica, &project],
        )
        .await?;
    Ok(PreviousReleaseCancels { cancels: rows.into_iter().map(|(run_id, cause)| RunCancel { run_id, cause: cause.0 }).collect() })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A worker of the previous release reads every fate it is answered
    /// with: a birth for a project being removed reaches it as refused,
    /// and every other fate as it is.
    #[test]
    fn the_previous_release_reads_its_answer() {
        let answer = BatchAnswer {
            fates: vec![Fate::Accepted, Fate::AlreadyApplied, Fate::BornElsewhere, Fate::ProjectRemoving, Fate::Refused],
        };
        let old = previous_release_answer(answer);
        assert_eq!(old.fates, [Fate::Accepted, Fate::AlreadyApplied, Fate::BornElsewhere, Fate::Refused, Fate::Refused]);
        let wire = serde_json::to_string(&old).unwrap();
        assert_eq!(wire, r#"{"fates":["accepted","already_applied","born_elsewhere","refused","refused"]}"#);
    }
}
