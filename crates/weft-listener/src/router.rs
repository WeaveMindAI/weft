//! HTTP router for the listener. Internal: the process that serves it
//! checks every caller's platform identity before a request reaches
//! here, and only weft's own roles (the dispatcher, the platform's alarm)
//! call it.
//!
//!   POST /prepare      compute a new signal's row (routing, kind
//!                      state, consumer payload); starts nothing
//!   POST /start        bring up a signal whose row was just committed
//!                      (its first wake; its held connection, where this
//!                      process holds)
//!   POST /unregister   drop a removed signal's held connection and
//!                      what it arranged outside
//!   POST /process      run kind-specific logic for one fire,
//!                      return a `ProcessOutcome` (value + target)
//!                      for the dispatcher to journal on
//!   POST /match_push   which of the offered signals a verified
//!                      provider push feeds, and with what payload
//!   POST /wake_by_hand what one signal wakes with when a person
//!                      wakes it instead of waiting, or nothing when
//!                      its kind cannot be woken that way
//!   POST /live         what one signal shows
//!   POST /rehydrate    reconcile with the durable signal table
//!   POST /wake         the alarm calling back for a `Wakes` signal
//!   GET  /signals      debug: list the connections this process holds
//!   GET  /health       liveness probe
//!
//! Every endpoint that names a signal reads it through `registry::held`:
//! from its durable row, or from this process's registry for a connection
//! it holds.

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde_json::Value;

use crate::kinds;
use crate::ListenerState;
use weft_core::signal::listener_protocol::{
    LiveRequest, LiveResponse, MatchPushRequest, MatchPushResponse, ProcessOutcome, ProcessRequest,
    PrepareRequest, PrepareResponse, StartRequest, UnregisterRequest, WakeByHandRequest, WakeByHandResponse,
};

pub fn router(state: ListenerState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/prepare", post(prepare))
        .route("/start", post(start))
        .route("/unregister", post(unregister))
        .route("/process", post(process))
        .route("/match_push", post(match_push))
        .route("/wake_by_hand", post(wake_by_hand))
        .route("/live", post(live))
        .route("/signals", get(list_signals))
        .route("/rehydrate", post(rehydrate_handler))
        // SYNC: the wake route <-> kinds::WAKE_PATH
        .route(kinds::WAKE_PATH, post(wake))
        .with_state(state)
}

fn internal(e: anyhow::Error) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}"))
}

/// Reconcile one project's signals with the durable signal table.
/// Idempotent. Called by the dispatcher's activate flow once the
/// activation's rows are written, so what those signals need between
/// fires (a wake, a held connection) exists before the gate flips to
/// Active. Rows named in `skip` (the ones the activation is about to
/// delete) are never brought up. Every other row that can come up does;
/// any that could not fail the call, named with their reasons (and are
/// retried meanwhile, see `registry::hold`).
async fn rehydrate_handler(
    State(state): State<ListenerState>,
    Json(req): Json<weft_core::signal::listener_protocol::RehydrateRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    let failed = crate::registry::rehydrate(&state, Some(req.project), &req.skip).await.map_err(internal)?;
    if !failed.is_empty() {
        return Err(internal(anyhow::anyhow!("{} signal(s) could not come up: {}", failed.len(), failed.join("; "))));
    }
    Ok(StatusCode::NO_CONTENT)
}

async fn health() -> StatusCode {
    StatusCode::NO_CONTENT
}

async fn prepare(
    State(state): State<ListenerState>,
    Json(req): Json<PrepareRequest>,
) -> Result<Json<PrepareResponse>, (StatusCode, String)> {
    let weft_core::signal::listener_protocol::PrepareSource { prior_kind_state, asked_at_unix_ms } = req.source;
    let for_instance = req.for_instance;
    let prepared = kinds::prepare_signal(
        &state,
        kinds::SignalIdentity {
            token: req.token,
            tenant_id: req.tenant_id,
            node_id: req.node_id,
            is_resume: req.is_resume,
            execution_id: req.execution_id,
            spec: req.spec,
        },
        for_instance,
        prior_kind_state.as_ref(),
        asked_at_unix_ms,
    )
    .await
    // `{e:#}` keeps the whole cause chain: a refusal's reason must reach
    // the user, not just the outermost context line.
    .map_err(|e| (StatusCode::BAD_REQUEST, format!("{e:#}")))?;
    Ok(Json(PrepareResponse {
        routing: prepared.routing,
        kind_state: prepared.kind_state,
        rendered: prepared.rendered.unwrap_or(Value::Null),
        holds: prepared.holds,
    }))
}

/// Bring up a signal the dispatcher just registered or put back (see
/// `StartMode`). Its row is committed before this is called, so no row is
/// a 404, and a kind refusing to come up (a connection missing a required
/// value, an unservable topic) is a 400 carrying its reason; a put-back
/// that refuses is also marked down and retried here.
async fn start(
    State(state): State<ListenerState>,
    Json(req): Json<StartRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    let row = state
        .signals
        .get_held(&req.token)
        .await
        .map_err(internal)?
        .ok_or((StatusCode::NOT_FOUND, format!("no signal is held under token {}", req.token)))?;
    // A fresh one's refusal is this registration's.
    crate::registry::start(&state, row, req.mode).await.map_err(|e| (StatusCode::BAD_REQUEST, format!("{e:#}")))?;
    Ok(StatusCode::NO_CONTENT)
}

async fn live(
    State(state): State<ListenerState>,
    Json(req): Json<LiveRequest>,
) -> Result<Json<LiveResponse>, (StatusCode, String)> {
    let sig = crate::registry::held(&state, &req.token)
        .await
        .map_err(internal)?
        .ok_or((StatusCode::NOT_FOUND, format!("unknown token: {}", req.token)))?;
    let mut live = kinds::compute_live(&kinds::LiveCtx { sig: &sig, address: req.address.as_deref() });
    if let Some(reason) = state.registry.down_reason(&req.token) {
        live.items.insert(0, weft_core::live::LiveItem::text("State", format!("down, retrying: {reason}")));
    }
    kinds::read_only_display(&sig.spec.kind, &live).map_err(|why| (StatusCode::INTERNAL_SERVER_ERROR, why))?;
    Ok(Json(LiveResponse { live }))
}

async fn unregister(
    State(state): State<ListenerState>,
    Json(req): Json<UnregisterRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    // The teardown is detached unless the token is reused right after
    // (`UnregisterRequest::reused`): the answer must not otherwise wait on
    // a provider round trip.
    // A row no listener can ever read is 422, which a caller reusing the
    // token may pass; anything trying again can fix is not.
    kinds::unregister(&state, req).await.map_err(|e| {
        let status = match &e {
            kinds::UnregisterError::Unreadable(_) => StatusCode::UNPROCESSABLE_ENTITY,
            kinds::UnregisterError::UnknownKind(_) => StatusCode::SERVICE_UNAVAILABLE,
            kinds::UnregisterError::TeardownFailed(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (status, e.to_string())
    })?;
    Ok(StatusCode::NO_CONTENT)
}

async fn process(
    State(state): State<ListenerState>,
    Json(req): Json<ProcessRequest>,
) -> Result<Json<ProcessOutcome>, (StatusCode, String)> {
    let outcome = kinds::process(&state, &req.token, req.payload).await.map_err(internal)?;
    Ok(Json(outcome))
}

/// Which of the offered signals a verified provider push feeds.
///
/// The dispatcher has already done its half: taken the push at the
/// public door, had the broker verify it, and narrowed the candidates to
/// the signals hanging off the connections the broker named. This is the
/// other half, and it is here because it reads a kind's own settings.
async fn match_push(
    State(state): State<ListenerState>,
    Json(req): Json<MatchPushRequest>,
) -> Result<Json<MatchPushResponse>, (StatusCode, String)> {
    let matched = kinds::match_push(&state, &req.push, &req.tokens).await.map_err(internal)?;
    Ok(Json(MatchPushResponse { matched }))
}

/// What a signal wakes with when a person wakes it by hand.
///
/// The asking tier owns "may this be woken and by whom"; the payload is
/// the KIND's, and minting one anywhere else would mean a second tier
/// holding a kind's wake shape.
async fn wake_by_hand(
    State(state): State<ListenerState>,
    Json(req): Json<WakeByHandRequest>,
) -> Result<Json<WakeByHandResponse>, (StatusCode, String)> {
    let payload = kinds::wake_by_hand(&state, &req.token)
        .await
        .map_err(|e| (StatusCode::NOT_FOUND, format!("{e:#}")))?;
    Ok(Json(WakeByHandResponse { payload }))
}

/// A `Wakes` signal's wake, delivered by the platform's alarm. A failure
/// answers 500, which every alarm takes as "try again"; the kind's claim
/// keeps a retried wake from acting twice. The moment it was aimed at
/// rides in the body (`WakeBody::due_at_ms`), not the delivery time.
///
/// A body that can never be read as a wake (not JSON, the wrong shape)
/// is dropped here, on every platform the same way: logged as an error
/// with the body and the reason, and answered 200 with
/// `{"dropped": true, "reason": ...}`. Only a success stops a retrying
/// queue (Cloud Tasks retries every non-2xx until the task expires), so
/// a 4xx would redeliver the same unreadable body forever. The body is
/// read by hand for that reason: axum's `Json` extractor would answer
/// the 4xx itself.
async fn wake(State(state): State<ListenerState>, body: axum::body::Bytes) -> Result<Response, (StatusCode, String)> {
    let call: weft_platform_traits::WakeCall<kinds::WakeBody> = match serde_json::from_slice(&body) {
        Ok(call) => call,
        Err(e) => {
            let reason = format!("the wake body is not a wake call ({{at_unix_ms, body: {{token, due_at_ms}}}}): {e}");
            tracing::error!(
                target: "weft_listener::wake",
                body = %String::from_utf8_lossy(&body),
                %reason,
                "dropped a wake that can never be read; the alarm that sent it is wrong"
            );
            return Ok((StatusCode::OK, Json(serde_json::json!({ "dropped": true, "reason": reason }))).into_response());
        }
    };
    kinds::wake(&state, call.body).await.map_err(internal)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

async fn list_signals(State(state): State<ListenerState>) -> Json<Value> {
    let rows: Vec<Value> = state
        .registry
        .list()
        .into_iter()
        .map(|(token, sig)| serde_json::json!({ "token": token, "node_id": sig.node_id, "kind": &sig.spec.kind }))
        .collect();
    Json(Value::Array(rows))
}
