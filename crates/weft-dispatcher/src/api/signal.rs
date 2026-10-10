//! Signal-related dispatcher routes. Every endpoint here either relays a
//! fire to the listener or reads/writes the durable signal table.

use std::sync::Arc;

use anyhow::Context;
use axum::{
    extract::{Path, RawQuery, State},
    http::{HeaderMap, StatusCode},
    response::Response,
    Json,
};
use serde_json::Value;
use sqlx::Row;

use weft_core::signal::listener_protocol::ProcessTarget;

use crate::authenticator::{authorize_project, CallerTenant};
use crate::state::DispatcherState;

pub use weft_task_store::parked_fires::{park, park_backoff_secs, ParkAppend, ParkRefusal, Waiting};

/// What became of an entry's event handed to the worker's door.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Landed {
    /// It is dealt with: a run started (or had already), or it was dropped
    /// (and why is logged).
    Done,
    /// It waits in its trigger's queue: the trigger is parked, its runs at
    /// once are all going, what it reads is not ready, or its worker was
    /// out of reach. `instance_gap` when it waits on what its instance
    /// provides.
    Waits { reason: String, instance_gap: bool },
    /// A caller past the trigger's per-caller limit, and when to come back.
    Refused { reason: String, retry_after_secs: u64 },
}

/// Every instance trigger's events waiting on a value its instance has not
/// given (`parked_fire.instance_gap`) in `project_id`: how many, and the
/// reason the one parked last gave.
pub async fn instance_waits(
    pool: &sqlx::PgPool,
    project_id: uuid::Uuid,
) -> anyhow::Result<std::collections::BTreeMap<weft_core::activation::ActivationKey, weft_core::program::WaitingFires>> {
    let rows: Vec<(String, Option<String>, i64, String)> = sqlx::query_as(
        "SELECT s.activation_trigger, s.instance_id, count(*)::bigint, \
                (array_agg(p.instance_gap #>> '{}' ORDER BY p.seq DESC))[1] \
         FROM parked_fire p JOIN signal s ON s.token = p.token \
         WHERE s.project_id = $1 AND s.activation_trigger IS NOT NULL AND p.instance_gap IS NOT NULL \
         GROUP BY s.activation_trigger, s.instance_id",
    )
    .bind(project_id)
    .fetch_all(pool)
    .await?;
    rows.into_iter()
        .map(|(trigger, instance, fires, reason)| {
            let instance = instance
                .map(weft_core::instance::InstanceId::new)
                .transpose()
                .map_err(|e| anyhow::anyhow!("signal.instance_id for trigger {trigger}: {e}"))?;
            let key = weft_core::activation::ActivationKey::new(trigger, weft_core::instance::Owner::from_instance(instance));
            Ok((key, weft_core::program::WaitingFires { fires: fires as u32, reason }))
        })
        .collect()
}

/// The signals of `instance` in `project_id` whose queue holds an event
/// waiting on that instance's values, and whose activation is Active: what
/// a change of the instance's values routes again.
pub async fn instance_gap_tokens(
    pool: &sqlx::PgPool,
    project_id: uuid::Uuid,
    instance: &weft_core::instance::InstanceId,
) -> anyhow::Result<Vec<String>> {
    Ok(sqlx::query_scalar(&format!(
        "SELECT s.token FROM signal s {} \
         WHERE s.project_id = $1 AND s.instance_id = $2 \
           AND COALESCE(a.status, 'active') = 'active' \
           AND EXISTS (SELECT 1 FROM parked_fire p WHERE p.token = s.token AND p.instance_gap IS NOT NULL)",
        weft_broker_client::protocol::SIGNAL_ACTIVATION_JOIN,
    ))
    .bind(project_id)
    .bind(instance.as_str())
    .fetch_all(pool)
    .await?)
}

/// `POST /signal/{token}`. Dispatcher entry point for every
/// stateless signal fire (webhook, form submission, extension's
/// resume completion): routed by token through [`take_event`].
pub async fn fire_signal(
    State(state): State<DispatcherState>,
    caller: crate::api::CallerAddress,
    Path(token): Path<String>,
    body: Option<Json<Value>>,
) -> axum::response::Response {
    let payload = body.map(|Json(v)| v).unwrap_or(Value::Null);
    fire_signal_inner(&state, &token, &caller, payload).await
}

/// `POST /signal/{token}/skip`. A person declines what a waiting run
/// asked: the waiting step ends skipped, every output of it closed, and
/// what reads them skips in turn. Sibling firings of the same execution
/// keep going. Only a waiting run's question can be skipped; an entry
/// has nothing to decline. The answer is weft's own
/// (`WaitAnswer::Skipped`), never a value the signal's kind processes,
/// so no kind can mistake it for an answer.
///
/// Auth: signal token alone (knowing it = permission to skip).
/// Same auth model as fire: a consumer that can answer the form
/// can also refuse to answer it.
pub async fn skip_signal(
    State(state): State<DispatcherState>,
    Path(token): Path<String>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let routing = match lookup_signal_routing(&state, &token).await {
        Ok(r) => r,
        Err(e) => return e.into_response(),
    };
    if routing.surface_kind == "internal" {
        return (StatusCode::NOT_FOUND, "internal signal kind has no public surface").into_response();
    }
    if !routing.is_resume {
        return (StatusCode::CONFLICT, "only a question a waiting run asked can be skipped; this signal starts runs").into_response();
    }
    answer_run(&state, &token, &routing, weft_core::primitive::WaitAnswer::Skipped).await.into_response()
}

async fn fire_signal_inner(
    state: &DispatcherState,
    token: &str,
    caller: &crate::api::CallerAddress,
    payload: Value,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let routing = match lookup_signal_routing(state, token).await {
        Ok(r) => r,
        Err(e) => return e.into_response(),
    };
    fire_checked(state, token, &routing, payload, caller).await.into_response()
}

async fn fire_checked(
    state: &DispatcherState,
    token: &str,
    routing: &FireGateInfo,
    payload: Value,
    caller: &crate::api::CallerAddress,
) -> Result<StatusCode, GateRefusal> {
    // Internal-surface signals (Timer, SSE) have no public path;
    // they fire from inside the listener via the FireSignal broker
    // task. External callers that somehow guess the token still hit
    // our public handler; refuse loudly instead of silently
    // swallowing.
    if routing.surface_kind == "internal" {
        return Err((StatusCode::NOT_FOUND, "internal signal kind has no public surface".to_string()).into());
    }
    // A resume answers a run that is already waiting; an entry gated by
    // a connection starts one only after the caller is checked, which
    // this door cannot do.
    if !routing.is_resume {
        refuse_gated_entry(&routing.auth_kind, "its /connect address")?;
    }
    // The caller is counted against the entry's per-caller limit at the
    // worker's door.
    take_event(state, token, routing, payload, Some(caller.key())).await
}

/// Refuse to start a run through an entry gated by a connection (its
/// `auth_kind` is not `none`) from a door that does not check the caller:
/// such an entry is a live route, gated at `/connect/...`, the address
/// `at` names.
fn refuse_gated_entry(auth_kind: &str, at: &str) -> Result<(), (StatusCode, String)> {
    if auth_kind == "none" {
        return Ok(());
    }
    Err((
        StatusCode::UNAUTHORIZED,
        format!("this entry is gated by a connection (auth '{auth_kind}'); call it at {at}"),
    ))
}

/// Fire one registered signal through [`take_event`], looked up by
/// token. What the public events receiver calls per matched
/// subscription, so a provider push takes exactly the path every other
/// external fire does.
pub(crate) async fn fire_registered_signal(
    state: &DispatcherState,
    token: &str,
    payload: Value,
) -> Result<StatusCode, (StatusCode, String)> {
    let routing = lookup_signal_routing(state, token).await?;
    // A provider's push has no caller to count: only the entry's own
    // per-minute limit applies (at the worker's door), and a fire past it
    // is dropped.
    take_event(state, token, &routing, payload, None).await.map_err(Into::into)
}

/// Why a fire was not taken, as its sender is answered: the status and
/// the message, and when to try again for a caller past a limit.
#[derive(Debug)]
pub(crate) struct GateRefusal {
    pub status: StatusCode,
    pub message: String,
    pub retry_after_secs: Option<u64>,
}

impl From<(StatusCode, String)> for GateRefusal {
    fn from((status, message): (StatusCode, String)) -> Self {
        Self { status, message, retry_after_secs: None }
    }
}

impl From<GateRefusal> for (StatusCode, String) {
    fn from(refused: GateRefusal) -> Self {
        (refused.status, refused.message)
    }
}

impl axum::response::IntoResponse for GateRefusal {
    fn into_response(self) -> axum::response::Response {
        match self.retry_after_secs {
            Some(secs) => (self.status, [(axum::http::header::RETRY_AFTER, secs.to_string())], self.message).into_response(),
            None => (self.status, self.message).into_response(),
        }
    }
}

/// One chokepoint for every event that reaches a signal through the
/// install (a call at its doors, a provider's push, an answer to a waiting
/// run): the listener's `/process` says what the event is (the dispatcher
/// stays kind-unaware). An entry's event goes to the worker's door
/// ([`fire_entry`]), which applies the trigger's standing and limits the
/// way it does for a caller; an answer to a waiting run goes to
/// [`answer_run`], which applies the standing here (an answer has no door).
pub(crate) async fn take_event(
    state: &DispatcherState,
    token: &str,
    routing: &FireGateInfo,
    payload: Value,
    caller: Option<String>,
) -> Result<StatusCode, GateRefusal> {
    // The listener answers for any signal whose row exists, whether it
    // has it in memory or not (it loads the row on a miss).
    let outcome = state
        .listener
        .process(token, &payload)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("listener dispatch: {e:#}")))?;
    match outcome.target {
        ProcessTarget::Resume { .. } => Ok(answer_run(state, token, routing, weft_core::primitive::WaitAnswer::Given { value: outcome.value }).await?),
        ProcessTarget::Entry => {
            let fire = weft_core::door_fire::DoorFire {
                token: token.to_string(),
                fire_id: uuid::Uuid::new_v4(),
                payload: outcome.value,
                caller,
                // A fire the install hands over is the trigger's: one a
                // holder picked up was held to its holder when it was kept
                // (`/v1/door/park_fire`), and none other names one.
                held_by: None,
                attempts: 0,
            };
            match fire_entry(state, routing, fire)
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("hand the event to its worker: {e:#}")))?
            {
                Landed::Done | Landed::Waits { .. } => Ok(StatusCode::OK),
                Landed::Refused { reason, retry_after_secs } => Err(GateRefusal {
                    status: StatusCode::TOO_MANY_REQUESTS,
                    message: format!("too many calls: {reason}; try again in {retry_after_secs}s"),
                    retry_after_secs: Some(retry_after_secs),
                }),
            }
        }
        ProcessTarget::Drop { reason } => {
            tracing::debug!(target: "weft_dispatcher::signal", %token, ?reason, "listener dropped fire");
            Ok(StatusCode::OK)
        }
    }
}

/// THE one way an answer reaches a waiting run (entrance 2), whatever
/// brought it (a call at `/signal/{token}`, an answer a listener picked up,
/// `weft wake`, a parked answer drained): `value` is already the kind's.
/// Its run's trigger decides, by the rule `weft_core::arrival` states:
///
/// - **Live**: it reaches its run now (`Journal::answer`).
/// - **Wait**: it waits in the wait's queue (one answer per wait), and the
///   parked-fire drain hands it over once the trigger is back.
/// - **Refused**: 410 Gone; the trigger takes no work any more.
pub(crate) async fn answer_run(
    state: &DispatcherState,
    token: &str,
    routing: &FireGateInfo,
    answer: weft_core::primitive::WaitAnswer,
) -> Result<StatusCode, (StatusCode, String)> {
    match routing.standing().arrival(crate::lease::now_unix()) {
        weft_core::arrival::Arrival::Live => {
            let answered = state.journal.answer(token, &answer).await.map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("answer: {e:#}")))?;
            match answered {
                crate::journal::Answered::Reached { consumed } => {
                    // The row is gone now, so the listener only learns of
                    // it from here.
                    state.listener.unregister_many(&[consumed]).await;
                    Ok(StatusCode::OK)
                }
                crate::journal::Answered::RunEnded { consumed } => {
                    state.listener.unregister_many(&[consumed]).await;
                    Err((StatusCode::GONE, "the run this answers has ended".into()))
                }
                crate::journal::Answered::Gone => {
                    Err((StatusCode::CONFLICT, "suspension already answered; duplicate submission ignored".into()))
                }
            }
        }
        // An answer for a waiting run whose trigger takes no work any more:
        // there is no run to hand it to now, or later.
        weft_core::arrival::Arrival::Refused => Err((StatusCode::GONE, "This no longer takes answers.".into())),
        weft_core::arrival::Arrival::Wait => {
            let waiting = weft_task_store::parked_fires::waiting_answer(uuid::Uuid::new_v4(), answer);
            // The shared park names its refusal; never swallow one under a
            // 200.
            match park(&state.pg_pool, token, &waiting, None).await.map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("park: {e}")))? {
                ParkAppend::Parked => Ok(StatusCode::OK),
                ParkAppend::Refused(ParkRefusal::ResumeAlreadyAnswered) => {
                    Err((StatusCode::CONFLICT, "suspension already answered; duplicate submission ignored".into()))
                }
                ParkAppend::Refused(ParkRefusal::RowGone) => {
                    Err((StatusCode::CONFLICT, "suspension already answered; duplicate submission ignored".into()))
                }
                refused @ ParkAppend::Refused(ParkRefusal::QueueFull | ParkRefusal::AlreadyQueued | ParkRefusal::NotHeld) => Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("park: an answer with a fresh id, in no holder's name, was refused: {refused:?}"),
                )),
            }
        }
    }
}

// `crate::lease::now_unix` is the canonical wall-clock reader.

/// Fire-time projection: just the three lifecycle fields the gate
/// actually reads (status + accepting + deadline) plus the project
/// id needed for tenant routing. `fires_visible_to_consumers` is
/// not on the fire path: it gates consumer enumeration in
/// `visible_signals`, never decides whether to park / refuse / pass.
pub(crate) struct FireGateInfo {
    pub project_id: uuid::Uuid,
    /// The signal's own owning tenant, frozen on the row at register time. The
    /// fire path stamps tasks/spawns with THIS, not a re-derivation through the
    /// tenant router, so the answer comes from the same source that authorized
    /// the register.
    pub tenant_id: String,
    /// The governing activation's status (`Active` when none governs it).
    pub status: crate::activation_store::ProjectStatus,
    pub accepting_fires: bool,
    pub fires_deadline_unix: Option<i64>,
    pub surface_kind: String,
    /// `signal.auth_kind`: `none` for an open entry.
    pub auth_kind: String,
    /// A resume token answers one waiting run; an entry starts new ones.
    pub is_resume: bool,
    /// The binary of the program its trigger is armed on (an entry's),
    /// read off the same row: where its events are handed.
    pub binary_hash: Option<String>,
}

impl FireGateInfo {
    /// How the governing activation stands, for the arrival rule.
    pub(crate) fn standing(&self) -> weft_core::arrival::Standing {
        weft_core::arrival::Standing {
            status: self.status,
            accepting_fires: self.accepting_fires,
            fires_deadline_unix: self.fires_deadline_unix,
        }
    }
}

/// The gate columns of a signal, read through the activation that
/// governs it (`signal.activation_trigger` + `signal.instance_id`, the one
/// join `SIGNAL_ACTIVATION_JOIN`). No governing activation reads as
/// live and accepting.
fn gate_select() -> String {
    format!(
        "SELECT s.token, s.project_id, s.tenant_id, s.surface_kind, s.auth_kind, \
                s.is_resume, s.program_json->>'binary_hash' AS binary_hash, \
                COALESCE(a.status, 'active') AS status, \
                COALESCE(a.accepting_fires, TRUE) AS accepting_fires, \
                a.fires_deadline_unix \
         FROM signal s {} \
         WHERE s.token = $1",
        weft_broker_client::protocol::SIGNAL_ACTIVATION_JOIN,
    )
}

fn gate_info(row: &sqlx::postgres::PgRow) -> Result<FireGateInfo, (StatusCode, String)> {
    let get_err = |e: sqlx::Error| (StatusCode::INTERNAL_SERVER_ERROR, format!("row: {e}"));
    let status_str: String = row.try_get("status").map_err(get_err)?;
    Ok(FireGateInfo {
        project_id: row.try_get("project_id").map_err(get_err)?,
        tenant_id: row.try_get("tenant_id").map_err(get_err)?,
        status: crate::activation_store::ProjectStatus::parse(&status_str)
            .ok_or_else(|| (StatusCode::INTERNAL_SERVER_ERROR, format!("unknown activation status '{status_str}'")))?,
        accepting_fires: row.try_get("accepting_fires").map_err(get_err)?,
        fires_deadline_unix: row.try_get("fires_deadline_unix").map_err(get_err)?,
        surface_kind: row.try_get("surface_kind").map_err(get_err)?,
        auth_kind: row.try_get("auth_kind").map_err(get_err)?,
        is_resume: row.try_get("is_resume").map_err(get_err)?,
        binary_hash: row.try_get("binary_hash").map_err(get_err)?,
    })
}

pub(crate) async fn lookup_signal_routing(
    state: &DispatcherState,
    token: &str,
) -> Result<FireGateInfo, (StatusCode, String)> {
    let row = sqlx::query(&gate_select())
        .bind(token)
        .fetch_optional(&state.pg_pool)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("signal lookup: {e}")))?
        .ok_or((StatusCode::NOT_FOUND, "unknown signal token".into()))?;
    gate_info(&row)
}


/// Hand an entry's event to the worker's door of the program its trigger
/// is armed on (`crate::worker_fire`). A worker out of reach is no loss:
/// the fire waits in its trigger's queue, and the drain hands it over
/// again.
pub(crate) async fn fire_entry(state: &DispatcherState, routing: &FireGateInfo, fire: weft_core::door_fire::DoorFire) -> anyhow::Result<Landed> {
    use weft_core::door_fire::Fired;
    let project_id = routing.project_id;
    let Some(binary_hash) = &routing.binary_hash else {
        tracing::warn!(target: "weft_dispatcher::signal", token = %fire.token, "fire dropped: its trigger has no armed program; activate it again");
        return Ok(Landed::Done);
    };
    let reason = match crate::worker_fire::fire(state, &routing.tenant_id, project_id, binary_hash, &fire).await {
        Ok(Fired::Started { .. } | Fired::AlreadyBorn) => return Ok(Landed::Done),
        // The install hands over no fire under a holder's name, so a worker
        // has nobody to refuse as no longer holding.
        Ok(Fired::NotHeld) => anyhow::bail!("the worker answered that a fire's holder no longer holds the signal, for a fire handed over in no holder's name"),
        Ok(Fired::Dropped { reason }) => {
            tracing::info!(target: "weft_dispatcher::signal", token = %fire.token, %project_id, "fire dropped: {reason}");
            return Ok(Landed::Done);
        }
        Ok(Fired::Refused { reason, retry_after_secs }) => return Ok(Landed::Refused { reason, retry_after_secs }),
        // The worker put it in the queue itself.
        Ok(Fired::Parked { reason, instance_gap }) => return Ok(Landed::Waits { reason, instance_gap }),
        Err(e) => format!("its worker could not take it: {e:#}"),
    };
    let parked = weft_task_store::parked_fires::waiting(fire.fire_id, fire.payload, fire.caller, fire.attempts + 1, None);
    match park(&state.pg_pool, &fire.token, &parked, None).await? {
        ParkAppend::Parked | ParkAppend::Refused(ParkRefusal::AlreadyQueued) => {
            tracing::warn!(target: "weft_dispatcher::signal", token = %fire.token, %project_id, "a fire waits in its trigger's queue: {reason}");
            Ok(Landed::Waits { reason, instance_gap: false })
        }
        ParkAppend::Refused(refusal) => anyhow::bail!("{reason}, and it could not be queued ({refusal:?})"),
    }
}

// ---------- Signal-deletion helpers ----------
//
// Two helpers, one ordering rule: "delete the durable row first,
// then best-effort unregister from the listener's in-RAM cache."
// The DB is canonical. A crash between the two steps leaves a
// stale listener registry entry that fails-loud (the dispatcher's
// fire-time lookup 404s when no row exists) instead of an orphan
// DB row that the listener would later re-register on rehydrate
// (which would resurrect a signal the caller just deleted).
//
// Every site that needs to delete signals goes through one of
// these helpers so the ordering invariant has a single home.

/// Delete a specific set of signals: DB rows first, then listener
/// unregister. Use when the caller already has the
/// `SignalRegistration` values in hand (lookup or filter). The DB
/// delete is one atomic SQL statement so a mid-loop failure can't
/// leave the system half-deleted.
pub(crate) async fn delete_signals(
    state: &DispatcherState,
    signals: &[crate::journal::SignalRegistration],
) -> Result<(), (StatusCode, String)> {
    if signals.is_empty() {
        return Ok(());
    }
    let tokens: Vec<String> = signals.iter().map(|s| s.token.clone()).collect();
    let deleted = state
        .journal
        .signal_remove_many(&tokens)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("signal_remove_many: {e}")))?;
    if !deleted.is_empty() {
        state.listener.unregister_many(&deleted).await;
    }
    Ok(())
}

/// `DELETE /signal/{token}`. Hard-cancel the underlying execution
/// for a resume signal: every signal attached to the same execution
/// goes down (so canceling one HumanQuery in a 5-parallel set
/// drops the other 4 too), the worker receives Cancel, and
/// NodeCancelled + ExecutionFailed get journaled. The journal
/// preserves everything for log-review.
///
/// For an entry-trigger signal (is_resume=false, e.g. a webhook),
/// there's no execution to cancel: the signal row gets dropped
/// and the listener registration unregistered.
///
/// Auth: requires `Authorization: Bearer <signal_token>` AND the
/// token's scope must be ≥ project (kinds + tags both empty,
/// project covered). Tag-scoped tokens cannot cancel because
/// cancellation reaches into sibling signals the token can't see.
/// Tag-scoped tokens can still skip via POST /signal/{token}/skip.
pub async fn cancel_signal(
    State(state): State<DispatcherState>,
    headers: HeaderMap,
    Path(signal_token): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    let scoping_token = bearer_token(&headers)?;
    let scope = require_scoped_signal_token(&state, &scoping_token).await?;

    let row = state
        .journal
        .signal_get(&signal_token)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("signal_get: {e}")))?
        .ok_or((StatusCode::NOT_FOUND, "unknown signal token".into()))?;

    // Tenant wall FIRST, as a 404: a signal token owned by ANOTHER tenant must be
    // indistinguishable from a nonexistent one (both "unknown signal token" / 404),
    // so a caller holding one valid token of their own cannot probe which tokens
    // exist on other accounts (a cross-tenant existence oracle). Only AFTER the
    // signal is confirmed same-tenant do we distinguish the intra-tenant
    // scope-too-narrow case as a 403 (which reveals nothing across the wall).
    if !scope.same_tenant(&row) {
        return Err((StatusCode::NOT_FOUND, "unknown signal token".into()));
    }
    if !scope.can_cancel_within_tenant(&row) {
        return Err((
            StatusCode::FORBIDDEN,
            "cancel requires a token whose scope is at least the whole project (no kind/tag restrictions)".into(),
        ));
    }

    if let Some(execution_id) = row.execution_id {
        // The one cancel strips the run's wake signals and ends it, or asks
        // the worker driving it to (`cancel_execution_id`).
        crate::api::execution::cancel_execution_id(&state, execution_id, &weft_core::exec::CancelCause::User)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("cancel: {e}")))?;
    } else {
        // Entry-trigger signal: no execution to cancel. Single
        // signal deletion via the shared helper that owns the
        // DB-first-listener-second ordering.
        delete_signals(&state, std::slice::from_ref(&row)).await?;
    }
    Ok(StatusCode::NO_CONTENT)
}

/// `GET /signal-token/signals` (signal token in `Authorization: Bearer`).
/// `GET /signal-token/signals/{signal token}/files/{field}`: the link a
/// consumer fetches a form's stored file through, made at the moment it
/// asks. The consumer payload carries a stored file as its facts only
/// (`FormSchema::for_consumer`), so a form answered a month after it
/// parked still shows its image: the link is never stored, it is minted
/// on every read and lives an hour. A file that is gone answers 410 in
/// the consumer's own terms (the store's own message names the storage
/// key, which is exactly what this door keeps in); a store that cannot
/// be reached answers 502, because "your file is gone" is a claim a
/// timeout does not support.
///
/// Scoped exactly like the listing: the bearer is the api token, the
/// signal must be one that token sees (same tenant, allowed projects and
/// tags, project visibility), the field is one the signal's kind names a
/// file for (`Signal::stored_file`, asked through the kind inventory so
/// this door knows no kind), and the file must belong to the signal (its
/// project, its execution, or the tenant's shared space). Anything else
/// is 404, so a token learns nothing about signals or files it cannot see.
pub async fn signal_file_for_token(
    State(state): State<DispatcherState>,
    headers: HeaderMap,
    Path((signal_token, field)): Path<(String, String)>,
) -> Result<Json<SignalFileLink>, (StatusCode, String)> {
    let api_token = bearer_token(&headers)?;
    let scope = require_scoped_signal_token(&state, &api_token).await?;
    let not_found = || (StatusCode::NOT_FOUND, "no such signal file".to_string());
    let visible = scope
        .visible_signals(&state)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("filter: {e}")))?;
    // The same row set the listing serves. A row is listed only when
    // its kind rendered a consumer payload (a form does; a socket or a
    // poll returns nothing to show), so a row with none is not a
    // consumer's to read files from either.
    let sig = visible
        .into_iter()
        .find(|s| s.token == signal_token && s.consumer_payload.is_some())
        .ok_or_else(not_found)?;
    let spec: weft_core::primitive::SignalSpec = serde_json::from_str(&sig.spec_json)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("corrupt signal spec: {e}")))?;
    // A field that holds no file is a 404; a field whose file cannot be
    // read is the signal being broken, and says so.
    let file = weft_core::signal::stored_file(&spec, &field)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?
        .ok_or_else(not_found)?;
    let parsed = weft_core::storage::key::parse_key(&file.key).map_err(|_| not_found())?;
    if !file_belongs_to_signal(&parsed, &sig) {
        return Err(not_found());
    }
    // The store's own message names the storage key, and this door
    // exists so a consumer never sees one. Say what happened in the
    // consumer's terms and keep the store's text for the operator log.
    // The consumer holding this token is the one who will fetch the
    // bytes (the browser extension showing a form), so the link comes
    // back on the address they reached us at.
    let base = crate::storage::LinkBase::for_request(&headers).map_err(|error| (StatusCode::BAD_REQUEST, error))?;
    let link = crate::storage::download_link(&state, &base, &file.key, Some(SIGNAL_FILE_LINK_TTL_SECS))
        .await
        .map_err(|e| {
            tracing::warn!(
                target: "weft_dispatcher::api::signal",
                key = %file.key,
                error = format!("{e:#}"),
                "a signal's file could not be linked"
            );
            // Only a store that says the file is gone means it is gone.
            // A timeout or a refused credential is the store being
            // unreachable, and telling the consumer their file is
            // permanently lost (and to re-run the workflow, which costs
            // real calls) would be a claim this never established.
            if e.downcast_ref::<crate::storage::StorageNotFound>().is_some() {
                (
                    StatusCode::GONE,
                    format!(
                        "the file behind '{field}' is no longer available: it expired or was \
                         deleted. Run the workflow again to make it afresh."
                    ),
                )
            } else {
                (
                    StatusCode::BAD_GATEWAY,
                    format!("the file behind '{field}' could not be reached just now; try again"),
                )
            }
        })?;
    // The facts come from the signal's own value, not from the link:
    // they describe the file the form is showing, and the mime (which
    // the store's answer does not carry) has to come from there anyway,
    // so taking all four from one source keeps them consistent.
    Ok(Json(SignalFileLink {
        url: link.url,
        mime_type: file.mime_type,
        size_bytes: file.size_bytes,
        filename: file.filename,
    }))
}

/// What the files door answers: a link that lives an hour, plus the
/// facts a consumer needs to render the file without fetching it.
// SYNC: SignalFileLink <-> extension-browser/src/lib/api.ts TaskFileLink, crates/weft-core/src/signal/form.rs consumer_file_value (the URL-backed arm publishes the same four keys)
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SignalFileLink {
    pub url: String,
    pub mime_type: String,
    pub size_bytes: u64,
    pub filename: String,
}

/// How long a link the files door hands out lives: the same hour a
/// node's own inputs get, long enough to look at, short enough that a
/// leaked link is soon worthless. A consumer asks again when it renders.
const SIGNAL_FILE_LINK_TTL_SECS: u64 = 3600;

/// May a signal's consumer be handed this file: the file sits in the
/// signal's own tenant, and in the signal's project (a project file),
/// its execution (a file the run made), the tenant's shared space, or the
/// tenant's assets. A file of another project or run is not the form's to show.
fn file_belongs_to_signal(
    parsed: &weft_core::storage::key::ParsedKey,
    sig: &crate::journal::SignalRegistration,
) -> bool {
    use weft_core::storage::key::KeyScope;
    if parsed.tenant != sig.tenant_id {
        return false;
    }
    match &parsed.scope {
        KeyScope::Project { project_id } => *project_id == sig.project_id.to_string(),
        // The tenant's own content (the tenant wall above): an asset is named
        // by its sha256, so naming one already takes holding its bytes.
        KeyScope::Asset => true,
        // An instance's file shows on a form of that instance's run alone.
        KeyScope::Instance { project_id, instance } => {
            *project_id == sig.project_id.to_string() && sig.instance.as_ref().is_some_and(|m| m.as_str() == instance)
        }
        KeyScope::Exec { execution_id } => sig.execution_id.is_some_and(|c| c.to_string() == *execution_id),
        KeyScope::Shared { .. } => true,
    }
}

/// Scoped enumeration. Filters by
/// the signal token's allowed_projects, allowed_tags AND
/// by project visibility (`fires_visible_to_consumers = TRUE`):
/// active and parked projects show up; hibernate-mode projects do
/// not. Wiped projects have no rows. The dispatcher never asks the
/// listener for the signal token: the SQL pre-filter is the only
/// scope check.
///
/// Returns the cached `consumer_payload` from each row. Computed
/// once at register time by the listener's `/prepare` and
/// stored on the row, so this endpoint is a pure SQL read with
/// no listener round-trip; park-mode projects can serve
/// `/signal-token/.../signals` even with the listener process reaped.
pub async fn list_signals_for_token(
    State(state): State<DispatcherState>,
    headers: HeaderMap,
) -> Result<Json<Value>, (StatusCode, String)> {
    let signal_token = bearer_token(&headers)?;
    let scope = require_scoped_signal_token(&state, &signal_token).await?;
    let visible = scope
        .visible_signals(&state)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("filter: {e}")))?;
    let mut out: Vec<Value> = Vec::with_capacity(visible.len());
    for sig in visible {
        if let Some(mut payload) = sig.consumer_payload {
            // Stamp `isResume` onto the enumerated payload so the consumer can
            // split the list into TRIGGERS (entry signals, always shown while
            // registered, fireable repeatedly to start a run) and RESUME tasks
            // (one-shot replies to a paused execution). It's a signal-row
            // property, not part of the listener's render, so it is added here
            // where the row and its rendered payload meet.
            // SYNC: consumer payload keys <->
            //       extension-browser/src/lib/api.ts (PendingTask),
            //       crates/weft-listener/src/kinds/form.rs (FormHandler::render)
            if let Some(obj) = payload.as_object_mut() {
                obj.insert("isResume".into(), Value::Bool(sig.is_resume));
            }
            out.push(payload);
        }
    }
    Ok(Json(Value::Array(out)))
}

/// `GET /signal-token/health` (Bearer-authenticated). Liveness + auth probe.
pub async fn signal_token_health(
    State(state): State<DispatcherState>,
    headers: HeaderMap,
) -> Result<Json<Value>, (StatusCode, String)> {
    let signal_token = bearer_token(&headers)?;
    require_scoped_signal_token(&state, &signal_token).await?;
    Ok(Json(serde_json::json!({ "ok": true })))
}

/// `DELETE /signal-token/signals` (signal token in `Authorization: Bearer`).
/// Bulk clear-all: cancel
/// every execution this token has visibility over. Distinct executions
/// cancel once each (cancel_execution_id drops every sibling signal of
/// the same execution), so a 5-parallel HumanQuery set under one
/// execution costs one cancel call.
///
/// Auth: token's scope must be ≥ project (kinds + tags empty).
/// Tag-scoped or kind-scoped tokens get 403: clear-all reaches
/// into sibling signals they can't see; same rationale as cancel.
///
/// Returns counts: { execution_ids_cancelled, entry_signals_dropped }.
pub async fn clear_all_signals(
    State(state): State<DispatcherState>,
    headers: HeaderMap,
) -> Result<Json<Value>, (StatusCode, String)> {
    let signal_token = bearer_token(&headers)?;
    let scope = require_scoped_signal_token(&state, &signal_token).await?;
    if scope.row.instance.is_some() {
        return Err((
            StatusCode::FORBIDDEN,
            "an instance token answers its instance's waits one by one; clearing everything is the author's".into(),
        ));
    }
    if !scope.row.allowed_tags.is_empty() {
        return Err((
            StatusCode::FORBIDDEN,
            "clear-all requires a token whose scope is at least the whole project (no tag restriction)".into(),
        ));
    }

    let visible = scope
        .visible_signals(&state)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("filter: {e}")))?;

    // Distinct executions only. cancel_execution_id drops every sibling
    // signal of the same execution, so cancelling once per execution is
    // both correct AND avoids duplicate work on parallel HumanQueries.
    let mut execution_ids: std::collections::BTreeSet<weft_core::ExecutionId> = Default::default();
    let mut entry_signals: Vec<crate::journal::SignalRegistration> = Vec::new();
    for s in visible {
        match s.execution_id {
            Some(c) => {
                execution_ids.insert(c);
            }
            None => entry_signals.push(s),
        }
    }

    for execution_id in &execution_ids {
        cancel_execution_id_logged(&state, *execution_id).await;
    }
    // Entry-trigger signals have no execution to cancel; delete via the
    // shared helper (DB-first-listener-second ordering).
    delete_signals(&state, &entry_signals).await?;

    Ok(Json(serde_json::json!({
        "execution_ids_cancelled": execution_ids.len(),
        "entry_signals_dropped": entry_signals.len(),
    })))
}

/// Best-effort `cancel_execution_id` wrapper for the admin sweep path:
/// failures are logged and skipped so one bad execution doesn't block
/// the whole clear-all. (The handler exists for the admin "drop
/// everything" verb where best-effort is the contract.)
async fn cancel_execution_id_logged(state: &DispatcherState, execution_id: weft_core::ExecutionId) {
    if let Err(e) =
        crate::api::execution::cancel_execution_id(state, execution_id, &weft_core::exec::CancelCause::User).await
    {
        tracing::warn!(
            target: "weft_dispatcher::signal",
            %execution_id, error = %e,
            "clear_all_signals: cancel_execution_id failed; skipping"
        );
    }
}

/// Pull the bearer token from `Authorization: Bearer <token>`.
fn bearer_token(headers: &HeaderMap) -> Result<String, (StatusCode, String)> {
    let raw = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .ok_or((
            StatusCode::UNAUTHORIZED,
            "missing 'Authorization: Bearer ...' header".into(),
        ))?;
    Ok(raw.to_string())
}

/// Resolve and load a signal token from its PRESENTED value, returning a
/// typed scope helper. The DB stores only the sha256 of the value
/// (show-once), so the lookup hashes first: a fixed-width digest match,
/// never a raw-secret equality. 401 if the token doesn't exist.
async fn require_scoped_signal_token(
    state: &DispatcherState,
    token: &str,
) -> Result<TokenScope, (StatusCode, String)> {
    let hash = weft_core::signal_token::token_hash(token);
    let row = state
        .journal
        .get_signal_token(&hash)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("signal_token: {e}")))?
        .ok_or((StatusCode::UNAUTHORIZED, "unknown signal token".into()))?;
    if row.expired(crate::lease::now_unix() as u64) {
        return Err((StatusCode::UNAUTHORIZED, "this token has expired; ask for a new one".into()));
    }
    // An operator key is never taken on the outside doors: it would be
    // an admin key living in a frontend. Answered like an unknown token.
    if row.kind != weft_core::signal_token::TokenKind::Caller {
        return Err((StatusCode::UNAUTHORIZED, "unknown signal token".into()));
    }
    Ok(TokenScope { row })
}

/// The signal token a request presents, loaded. The doors that are not
/// about signals (a node's display) apply their own scope rules to the
/// row, so they take it from here rather than re-reading the header.
pub(crate) async fn token_from_bearer(
    state: &DispatcherState,
    headers: &HeaderMap,
) -> Result<crate::journal::SignalToken, (StatusCode, String)> {
    let presented = bearer_token(headers)?;
    Ok(require_scoped_signal_token(state, &presented).await?.row)
}

struct TokenScope {
    row: crate::journal::SignalToken,
}

impl TokenScope {
    /// The tenant wall: is this signal owned by the same tenant as the scoping
    /// token? Kept SEPARATE from the scope check so a cross-tenant signal is
    /// answered as "not found" (indistinguishable from a nonexistent token),
    /// never as a distinct "forbidden" that would leak the token's existence on
    /// another account.
    fn same_tenant(&self, sig: &crate::journal::SignalRegistration) -> bool {
        sig.tenant_id == self.row.tenant_id
    }

    /// True if the token may CANCEL this signal, GIVEN it is already known to be
    /// same-tenant (call `same_tenant` first). Cancel reaches into sibling
    /// signals of the same execution (different tags), so the token must have full
    /// project-level view, not a sub-project slice.
    ///
    /// Rule (within the tenant): covered project AND no tag restriction. Empty
    /// allowed_tags = "I see everything in the projects I'm allowed in." A
    /// tag-scoped token can only skip its own visible signals, never cancel.
    fn can_cancel_within_tenant(&self, sig: &crate::journal::SignalRegistration) -> bool {
        if !self.row.allowed_tags.is_empty() {
            return false;
        }
        // An instance token reaches its own instance's rows alone.
        if let Some(instance) = &self.row.instance {
            if sig.instance.as_ref() != Some(instance) {
                return false;
            }
        }
        self.row.covers_project(&sig.project_id)
    }

    /// Run the SQL filter to enumerate every signal this token sees.
    /// Filters by token scope (tenant / projects / tags) AND by the
    /// visibility of the activation governing each signal: only those
    /// with `fires_visible_to_consumers = TRUE` (or none governing them)
    /// show up. That covers active and parked activations (consumers can
    /// browse + submit; submissions still park at /signal/{token}).
    /// Hibernating ones are hidden during the entire inactive window
    /// because hibernate sets `fires_visible_to_consumers = FALSE`.
    /// Wiped ones have no rows at all.
    async fn visible_signals(
        &self,
        state: &DispatcherState,
    ) -> anyhow::Result<Vec<crate::journal::SignalRegistration>> {
        signals_visible_to(
            &state.pg_pool,
            &self.row.tenant_id,
            &self.row.allowed_projects,
            &self.row.allowed_tags,
            self.row.instance.as_ref(),
        )
        .await
    }
}

/// Every signal a consumer token scoped to `tenant`, `projects` (empty:
/// all of the tenant's), `tags` (empty: any) and `instance` (an instance
/// token: that instance's rows only, its runs' waits and its own
/// per-instance triggers) may see: rows of projects showing their fires to
/// consumers, resume rows only while unanswered. Entry rows first, then
/// by age.
pub async fn signals_visible_to(
    pool: &sqlx::PgPool,
    tenant: &str,
    projects: &[uuid::Uuid],
    tags: &[String],
    instance: Option<&weft_core::instance::InstanceId>,
) -> anyhow::Result<Vec<crate::journal::SignalRegistration>> {
    // One decoder for a signal row, shared with the journal
    // (`row_to_signal`): the SELECT differs (a join and the consumer
    // filters), the columns and the decoding must not.
    let rows = sqlx::query_as::<_, crate::journal::postgres::SignalRow>(&format!(
        concat!("SELECT ", crate::journal::postgres::signal_columns!("s."), " \
         FROM signal s {join} \
         WHERE COALESCE(a.fires_visible_to_consumers, TRUE) = TRUE \
           AND s.tenant_id = $1 \
           AND ($2::uuid[] = '{{}}'::uuid[] OR s.project_id = ANY($2)) \
           AND ($3::text[] = '{{}}'::text[] OR s.tags && $3) \
           AND ($4::text IS NULL OR s.instance_id = $4) \
           AND ( \
             s.is_resume = FALSE \
             OR NOT EXISTS (SELECT 1 FROM parked_fire p WHERE p.token = s.token) \
           ) \
         ORDER BY s.is_resume ASC, s.created_at ASC"),
        join = weft_broker_client::protocol::SIGNAL_ACTIVATION_JOIN,
    ))
    .bind(tenant)
    .bind(projects)
    .bind(tags)
    .bind(instance.map(|m| m.as_str()))
    .fetch_all(pool)
    .await
    .context("signals_visible_to (the consumer listing): read a signal row")?;
    rows.into_iter().map(crate::journal::postgres::row_to_signal).collect()
}

// ---------- PublicEntry catch-all + inspector display/action -----------

/// `POST /<mount_path>` catch-all. External clients hit this for
/// any signal whose `surface_kind = 'public_entry'` (Webhook,
/// ApiPost, future public-form). Splits the tenant off the called
/// path, MATCHES it against that tenant's registered patterns (a
/// pattern like `cards/{id}` is not a string to compare), then reads
/// the matched row and hands the event to [`take_event`]. Two calls it
/// refuses rather than serves, both because the answer lives at
/// `/connect`: a pattern that captured part of the path, and a row gated
/// by a connection. So what fires here is always a bare, open address.
/// Anything that matches nothing 404s.
pub async fn fire_public_entry(
    State(state): State<DispatcherState>,
    headers: HeaderMap,
    caller: crate::api::CallerAddress,
    Path(mount_path): Path<String>,
    body: Option<Json<Value>>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    // A bare fire runs for nobody: naming an instance here would be dropped
    // on the floor, so it is refused, pointing at the door that honours it.
    for named in [weft_core::instance::INSTANCE_HEADER, weft_core::instance::INSTANCE_TOKEN_HEADER] {
        if headers.contains_key(named) {
            return (
                StatusCode::BAD_REQUEST,
                format!(
                    "a bare fire runs for no instance, so {named} is not taken here; call the route at \
                     /connect/{mount_path} to start a run for an instance"
                ),
            )
                .into_response();
        }
    }
    let (token, routing, payload) = match public_entry_target(&state, &mount_path, body).await {
        Ok(found) => found,
        Err(e) => return e.into_response(),
    };
    // A bare-path fire is always an entry (a resume token has no mount);
    // its caller is counted at the worker's door.
    take_event(&state, &token, &routing, payload, Some(caller.key())).await.into_response()
}

/// Which open entry a bare-path fire reaches, with its gate info and
/// payload, or the refusal (the same vague words whatever went wrong).
async fn public_entry_target(
    state: &DispatcherState,
    mount_path: &str,
    body: Option<Json<Value>>,
) -> Result<(String, FireGateInfo, Value), (StatusCode, String)> {
    // Normalize: catch-all gives us a path without the leading
    // slash. Convert to `/foo` form (or `/` for empty) to match
    // the row.
    let normalized = if mount_path.is_empty() {
        "/".to_string()
    } else {
        format!("/{}", mount_path)
    };
    // Deliberately vague, and the same words whatever went wrong: this
    // door faces the open internet, so telling a stranger the
    // difference between "no such address" and "that address exists but
    // is not taking fires" tells them what this account runs.
    let refuse = || {
        (
            StatusCode::NOT_FOUND,
            "Project is not accepting requests. Please contact the project administrator."
                .to_string(),
        )
    };
    // The stored address is `/<tenant>/<project>/<pattern>`, and a pattern is not
    // a string to compare: `cards/{id}` has to be MATCHED against
    // `cards/7`. This used to be an equality lookup, so a registered
    // address holding a capture could never be reached through here at
    // all, whatever was called.
    let (mount, called) = weft_core::route::SharedMount::split(&normalized).ok_or_else(refuse)?;
    // Matched against the held routes, and against the rows themselves
    // before refusing: a route activated a moment ago may not have been
    // heard yet.
    let rows = match state.held.routes.held(&mount.tenant.to_string()).map(|held| rows_of(&held, mount.project)) {
        Some(held) if resolve_route(&held, mount, "POST", called).is_ok() => held,
        _ => rows_of(&fresh_tenant_routes(state, mount.tenant).await?, mount.project),
    };
    let (matched, params) = resolve_route(&rows, mount, "POST", called).map_err(|_| refuse())?;
    // An address with a capture in it is a live route's shape, and a
    // live route is served (and gated, and answered) at `/connect`.
    // Reached here it would fire the program with the capture thrown
    // away, so the program would run without the part of the address
    // that said which thing it was about. Say so instead.
    if !params.is_empty() {
        return Err((
            StatusCode::NOT_FOUND,
            format!(
                "this address captures part of the path, which is a live route: call it at \
                 /connect{normalized}"
            ),
        ));
    }
    let token = matched.token.clone();
    let routing = lookup_signal_routing(state, &token).await.map_err(|e| {
        if e.0 == StatusCode::NOT_FOUND { refuse() } else { e }
    })?;

    // The bare-path fire is open or nothing: a connection-gated entry is
    // a live route, served (and gated) at `/connect/...`; naming that here
    // beats a silent drop at the listener.
    refuse_gated_entry(&routing.auth_kind, &format!("/connect{normalized}"))?;

    let payload = body.map(|Json(v)| v).unwrap_or(Value::Null);
    Ok((token, routing, payload))
}

/// The rows of `project` among a tenant's held routes.
fn rows_of(routes: &[HeldRoute], project: uuid::Uuid) -> Vec<RouteRow> {
    routes.iter().filter(|route| route.project_id == project).map(|route| route.row.clone()).collect()
}

/// One public-entry row of a project, as the route matcher sees it.
#[derive(Debug, Clone)]
pub(crate) struct RouteRow {
    pub mount_path: String,
    pub mount_methods: Vec<String>,
    pub token: String,
}

/// The route a call resolved to: its row and the path's captures.
pub(crate) type ResolvedRoute<'r> = (&'r RouteRow, std::collections::BTreeMap<String, String>);

/// Pick the row serving `method` on `path` among one project's public
/// entries (`rows`, all under `mount`), or the HTTP answer when none does:
/// `404` for an unknown path, `405` naming the allowed methods for a known
/// path called with the wrong verb. Pure over the rows; a stored pattern
/// that no longer parses is skipped, loud in the log (its own register
/// validated it, so this is corruption).
pub(crate) fn resolve_route<'r>(
    rows: &'r [RouteRow],
    mount: weft_core::route::SharedMount<'_>,
    method: &str,
    path: &str,
) -> Result<ResolvedRoute<'r>, (StatusCode, String)> {
    let candidates = rows.iter().filter_map(|row| {
        let pattern = mount.pattern_of(&row.mount_path);
        match weft_core::route::RoutePattern::parse(&pattern) {
            Ok(pattern) => Some((
                weft_core::route::RouteKey { pattern, methods: row.mount_methods.clone() },
                row,
            )),
            Err(e) => {
                tracing::error!(
                    target: "weft_dispatcher::signal",
                    token = %row.token, mount_path = %row.mount_path, error = %e,
                    "a stored route pattern no longer parses; the route is unreachable"
                );
                None
            }
        }
    });
    match weft_core::route::find_route(candidates, method, path) {
        weft_core::route::RouteMatch::Found { route, params } => Ok((route, params)),
        weft_core::route::RouteMatch::WrongMethod { allowed } => Err((
            StatusCode::METHOD_NOT_ALLOWED,
            format!("{method} is not served at this path; allowed: {}", allowed.join(", ")),
        )),
        weft_core::route::RouteMatch::NotFound => {
            Err((StatusCode::NOT_FOUND, "no live endpoint at this path".into()))
        }
    }
}

// ----- Live callers at a shared address -----------------------------

/// Set by the install's door on a request that came for one project's API
/// domain, naming that project: the call sits at the domain's root, so the
/// worker is told no prefix. The door drops any copy a caller sent; one
/// that reaches the relay some other way must name the project the path
/// names, or nothing matches.
// SYNC: API_PROJECT_HEADER <-> crates/weft-dispatcher/src/door.rs (route)
pub const API_PROJECT_HEADER: &str = "x-weft-api-project";

/// `ANY /connect/{*path}`: a live call at an address the install shares
/// between its projects, `/connect/<tenant>/<project id>/<path>`. The
/// project's routes (pattern + method) pick the program serving the route,
/// and the call is passed on to that project's workers as the caller sent
/// it (`live_relay`), whose door runs every check. A path no route serves
/// is answered here, so a stray call wakes no worker.
pub async fn connect_live(
    State(state): State<DispatcherState>,
    address: crate::api::CallerAddress,
    Path(called_path): Path<String>,
    RawQuery(raw_query): RawQuery,
    request: axum::extract::Request,
) -> Result<Response, (StatusCode, String)> {
    let no_endpoint = || (StatusCode::NOT_FOUND, "no live endpoint at this path".to_string());
    let (mount, path) = weft_core::route::SharedMount::split(&called_path).ok_or_else(no_endpoint)?;
    let by_domain = match request.headers().get(API_PROJECT_HEADER) {
        None => false,
        Some(v) => {
            let named = v
                .to_str()
                .ok()
                .and_then(|v| v.parse::<uuid::Uuid>().ok())
                .ok_or((StatusCode::BAD_REQUEST, format!("{API_PROJECT_HEADER} is not a project id")))?;
            if named != mount.project {
                return Err(no_endpoint());
            }
            true
        }
    };
    let binary_hash = relayed_route(&state, mount, request.method().as_str(), path).await?;
    // The path as the caller sent it, still percent-encoded: the decoded
    // capture would turn an escaped `?`, `/` or `#` into a real one on the
    // way to the worker. A project's API domain serves its routes at its
    // root; everything else sits under its tenant and project.
    let shared = format!("/connect{}", mount.prefix());
    let raw_path = request
        .uri()
        .path()
        .strip_prefix(&shared)
        .ok_or_else(|| (StatusCode::INTERNAL_SERVER_ERROR, format!("a live call reached the relay at {}, outside {shared}", request.uri().path())))?;
    let raw_path = if raw_path.is_empty() { "/".to_string() } else { raw_path.to_string() };
    let prefix = if by_domain { String::new() } else { shared };
    Ok(crate::live_relay::to_project(&state, mount.project, &binary_hash, address.0, &prefix, &raw_path, raw_query.as_deref().unwrap_or(""), request).await)
}

/// One public entry of a tenant as held in memory (`crate::held`): where
/// it is mounted, whose it is, and the program it is armed for.
pub(crate) struct HeldRoute {
    project_id: uuid::Uuid,
    row: RouteRow,
    binary_hash: String,
}

/// The program serving `method` on `path` among the live routes of the
/// project `mount` names. Matched against the routes this dispatcher
/// holds, and against the rows themselves before refusing: a route armed a
/// moment ago may not have been heard yet.
async fn relayed_route(
    state: &DispatcherState,
    mount: weft_core::route::SharedMount<'_>,
    method: &str,
    path: &str,
) -> Result<String, (StatusCode, String)> {
    if let Some(held) = state.held.routes.held(&mount.tenant.to_string()) {
        if let Ok(found) = match_relayed(&held, mount, method, path) {
            return Ok(found);
        }
    }
    match_relayed(&fresh_tenant_routes(state, mount.tenant).await?, mount, method, path)
}

fn match_relayed(
    routes: &[HeldRoute],
    mount: weft_core::route::SharedMount<'_>,
    method: &str,
    path: &str,
) -> Result<String, (StatusCode, String)> {
    let candidates: Vec<&HeldRoute> = routes.iter().filter(|route| route.project_id == mount.project).collect();
    let rows: Vec<RouteRow> = candidates.iter().map(|route| route.row.clone()).collect();
    let (matched, _) = resolve_route(&rows, mount, method, path)?;
    let route = candidates
        .iter()
        .find(|route| route.row.token == matched.token)
        .expect("the matched route is one of the rows it was matched among");
    Ok(route.binary_hash.clone())
}

/// Every public entry of `tenant` as the rows say now, kept for the next
/// call (`crate::held`).
async fn fresh_tenant_routes(state: &DispatcherState, tenant: &str) -> Result<Arc<Vec<HeldRoute>>, (StatusCode, String)> {
    state.held.routes.load_fresh(tenant.to_string(), || read_tenant_routes(&state.pg_pool, tenant)).await
}

async fn read_tenant_routes(pool: &sqlx::PgPool, tenant: &str) -> Result<Vec<HeldRoute>, (StatusCode, String)> {
    // token, mount path, methods, project, the program's binary (an entry
    // is armed with its program as it is captured)
    type MountRow = (String, String, Vec<String>, uuid::Uuid, String);
    let rows: Vec<MountRow> = sqlx::query_as(
        "SELECT s.token, s.mount_path, s.mount_methods, s.project_id, s.program_json->>'binary_hash' \
         FROM signal s \
         WHERE s.tenant_id = $1 AND s.surface_kind = 'public_entry' AND s.mount_path IS NOT NULL",
    )
    .bind(tenant)
    .fetch_all(pool)
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("route lookup: {e}")))?;
    Ok(rows
        .into_iter()
        .map(|(token, mount_path, mount_methods, project_id, binary_hash)| HeldRoute {
            project_id,
            row: RouteRow { token, mount_path, mount_methods },
            binary_hash,
        })
        .collect())
}

/// Project-token proxy: what a trigger node is showing. `{node}` is
/// the trigger's place, spelled the way a person writes it (`door`,
/// `one.door` for the `door` inside the file the site `one` includes),
/// which is the key its entry row is stored under. Resolves that row
/// → token → the listener's `/live`. Every way of having nothing to
/// show (no signal row, a listener that does not know the token) is
/// one 404: the caller is the graph's trigger panel, which draws 404
/// as "not running, activate it", the way an infra node draws its
/// unprovisioned state. The rest is a failure it shows verbatim.
pub async fn live_signal(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path((id, node)): Path<(uuid::Uuid, String)>,
) -> Result<Json<weft_core::live::LiveFeed>, (StatusCode, String)> {
    authorize_project(&state, &caller.0, id).await?;
    Ok(Json(read_signal_live(&state, id, &node, None).await?))
}

/// What a trigger node is showing right now, off the listener holding
/// its signal. The caller has already been authorized for the project
/// (the editor by its project token, an outside client by its signal
/// token), so this is the one implementation both doors share, down to
/// the 404 they both give for a trigger nothing is holding.
/// `node` is the trigger's place as the caller spelled it, which is
/// both the row's key and the only name the refusals use: somebody who
/// asked for `test.whatsapp` is never told about `Test.whatsapp`, a
/// name that exists nowhere in their source.
pub(crate) async fn read_signal_live(
    state: &DispatcherState,
    id: uuid::Uuid,
    node: &str,
    instance: Option<&weft_core::instance::InstanceId>,
) -> Result<weft_core::live::LiveFeed, (StatusCode, String)> {
    // The entry registered at this place for this copy, read once: it carries the
    // token the listener is asked by, and the address the caller is
    // shown. A resume row of the same node is another registration
    // entirely (a display is what a TRIGGER shows), and the journal
    // never answers with one here.
    let entry = state
        .journal
        .signal_entry_at(id, node, instance)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("signal row: {e}")))?
        .ok_or((StatusCode::NOT_FOUND, format!("no signal for node '{node}'")))?;
    let not_listening = || {
        (
            StatusCode::NOT_FOUND,
            format!("the listener does not hold the trigger '{node}'; activate the project to register it"),
        )
    };
    // Where a caller reaches this signal. The row is the authority (it
    // holds the tenant-namespaced mount path) and `public_url` is the
    // one place that knows a held-connection kind is served under
    // `/connect/`; the listener holds only the route pattern its kind
    // computed, so the finished address is sent to it rather than
    // assembled there from three things it cannot see.
    //
    // The CONFIGURED base, not the reading request's own host: this
    // address is handed on to a third party (whoever will call the
    // trigger), and the reader's host says nothing about what that
    // party reaches.
    let address = entry.public_url(state.external_base_url());
    // The listener's own text names the route already.
    state
        .listener
        .live(&entry.token, address.as_deref())
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, format!("the trigger's display: {e}")))?
        .ok_or_else(not_listening)
}

#[cfg(test)]
mod route_lookup_tests {
    use super::*;

    fn row(mount_path: &str, methods: &[&str], token: &str) -> RouteRow {
        RouteRow {
            mount_path: mount_path.into(),
            mount_methods: methods.iter().map(|m| m.to_string()).collect(),
            token: token.into(),
        }
    }

    const P: uuid::Uuid = uuid::Uuid::from_u128(7);

    fn mount() -> weft_core::route::SharedMount<'static> {
        weft_core::route::SharedMount::new("alice", P)
    }

    /// A row of project `P` of tenant `alice`, at `rest` under it.
    fn at(rest: &str, methods: &[&str], token: &str) -> RouteRow {
        row(&mount().mount_path(rest), methods, token)
    }

    #[test]
    fn a_literal_route_beats_a_capture_and_params_come_back() {
        let rows = vec![at("users/{id}", &[], "by-id"), at("users/me", &[], "me")];
        let (hit, params) = resolve_route(&rows, mount(), "GET", "users/me").unwrap();
        assert_eq!(hit.token, "me");
        assert!(params.is_empty());
        let (hit, params) = resolve_route(&rows, mount(), "GET", "users/42").unwrap();
        assert_eq!(hit.token, "by-id");
        assert_eq!(params.get("id").map(String::as_str), Some("42"));
    }

    #[test]
    fn a_known_path_with_the_wrong_verb_is_405_naming_the_allowed_ones() {
        let rows = vec![at("items", &["POST", "PUT"], "w"), at("items", &["DELETE"], "d")];
        let err = resolve_route(&rows, mount(), "GET", "items").unwrap_err();
        assert_eq!(err.0, StatusCode::METHOD_NOT_ALLOWED);
        assert!(err.1.contains("DELETE, POST, PUT"), "{}", err.1);
    }

    #[test]
    fn an_unknown_path_is_404_and_the_root_route_serves_the_empty_path() {
        let rows = vec![at("", &[], "root")];
        assert_eq!(resolve_route(&rows, mount(), "GET", "nothing").unwrap_err().0, StatusCode::NOT_FOUND);
        assert_eq!(resolve_route(&rows, mount(), "GET", "").unwrap().0.token, "root");
    }

    #[test]
    fn a_corrupt_stored_pattern_is_skipped_not_fatal() {
        let rows = vec![at("bad/{", &[], "bad"), at("good", &[], "good")];
        assert_eq!(resolve_route(&rows, mount(), "GET", "good").unwrap().0.token, "good");
    }

    /// Two projects serving the same path each answer at their own
    /// address: the project the path names is the only one matched.
    #[test]
    fn a_call_reaches_only_the_project_its_address_names() {
        let other = uuid::Uuid::from_u128(8);
        let held = |project: uuid::Uuid, token: &str, binary: &str| HeldRoute {
            project_id: project,
            row: row(&weft_core::route::SharedMount::new("alice", project).mount_path("chat"), &[], token),
            binary_hash: binary.into(),
        };
        let routes = vec![held(P, "mine", "bin-p"), held(other, "theirs", "bin-o")];
        assert_eq!(match_relayed(&routes, mount(), "GET", "chat").unwrap(), "bin-p");
        let theirs = weft_core::route::SharedMount::new("alice", other);
        assert_eq!(match_relayed(&routes, theirs, "GET", "chat").unwrap(), "bin-o");
        let nobody = weft_core::route::SharedMount::new("alice", uuid::Uuid::from_u128(9));
        assert_eq!(match_relayed(&routes, nobody, "GET", "chat").unwrap_err().0, StatusCode::NOT_FOUND);
    }
}

#[cfg(test)]
mod public_url_tests {
    use crate::journal::SignalRegistration;

    fn fresh(surface: &str, mount: Option<&str>) -> SignalRegistration {
        SignalRegistration {
            instance: None,
            activation_trigger: None,
            source_version: None,
            setup_execution_id: None,
            program: None,
            token: "tok-1".into(),
            tenant_id: "t".into(),
            project_id: uuid::Uuid::from_u128(0x100),
            execution_id: None,
            node_id: "n".into(),
            is_resume: false,
            // A real row always names its kind, and the address depends
            // on it: a held connection answers under `/connect/`, a
            // plain public fire at the bare path. `form` is the plain
            // one; the live case has its own test below.
            spec_json: r#"{"kind":"form"}"#.into(),
            access_id: None,
            consumer_kind: None,
            tags: vec![],
            port_snapshot: None,
            consumer_payload: None,
            surface_kind: surface.into(),
            mount_path: mount.map(String::from),
            mount_methods: Vec::new(),
            auth_kind: "none".into(),
            auth_config: None,
            kind_state: serde_json::Value::Object(Default::default()),
            kind_state_seq: 0,
            holds: false,
        }
    }

    #[test]
    fn public_entry_root_normalizes() {
        let s = fresh("public_entry", Some("/"));
        assert_eq!(
            s.public_url("http://127.0.0.1:14111"),
            Some("http://127.0.0.1:14111/".into())
        );
    }

    /// A row whose spec cannot be read gets NO address rather than the
    /// bare-path guess. The two look equally real and only one of them
    /// works, so handing out the wrong one sends somebody to debug a
    /// route that was answering all along somewhere else.
    #[test]
    fn an_unreadable_spec_yields_no_address_rather_than_a_guess() {
        let mut s = fresh("public_entry", Some("/chat"));
        s.spec_json = "{".into();
        assert_eq!(s.public_url("http://127.0.0.1:14111"), None);
    }

    #[test]
    fn public_entry_with_path() {
        let s = fresh("public_entry", Some("/webhooks/stripe"));
        assert_eq!(
            s.public_url("http://127.0.0.1:14111"),
            Some("http://127.0.0.1:14111/webhooks/stripe".into())
        );
    }

    #[test]
    fn live_connection_url_carries_connect_prefix() {
        // A live-connection kind (route) is reachable ONLY via
        // /connect/...; the displayed URL must carry that prefix, unlike a
        // plain public fire.
        let mut s = fresh("public_entry", Some("/alice/chat"));
        s.spec_json = r#"{"kind":"route","config":{},"consumer_kind":null}"#.into();
        assert_eq!(
            s.public_url("http://127.0.0.1:14111"),
            Some("http://127.0.0.1:14111/connect/alice/chat".into())
        );
    }

    #[test]
    fn public_entry_strips_trailing_slash_on_base() {
        let s = fresh("public_entry", Some("/foo"));
        assert_eq!(
            s.public_url("http://127.0.0.1:14111/"),
            Some("http://127.0.0.1:14111/foo".into())
        );
    }

    #[test]
    fn task_callback_uses_token() {
        let s = fresh("task_callback", None);
        assert_eq!(
            s.public_url("http://127.0.0.1:14111"),
            Some("http://127.0.0.1:14111/signal/tok-1".into())
        );
    }

    #[test]
    fn unknown_surface_returns_none() {
        let s = fresh("future_kind", None);
        assert!(s.public_url("http://127.0.0.1:14111").is_none());
    }
}



/// Layer-1 tests for the `can_cancel` authorization gate (the C1 cross-tenant
/// fix). The cancel path reaches sibling signals of the same execution, so the gate
/// must enforce: SAME TENANT (the outer wall, always) + covered project + no tag
/// restriction. A pure function over `(SignalToken, SignalRegistration)`, so no
/// DB is needed. Without these, the tenant predicate could be reverted and the
/// suite would still pass (the enumeration test only exercises token CRUD).
#[cfg(test)]
mod can_cancel_tests {
    use super::TokenScope;
    use crate::journal::{SignalRegistration, SignalToken};

    fn token(tenant: &str, projects: Vec<uuid::Uuid>, tags: Vec<String>) -> TokenScope {
        TokenScope {
            row: SignalToken {
                id: uuid::Uuid::nil(),
                kind: weft_core::signal_token::TokenKind::Caller,
                token_hash: "hash".into(),
                recognizer: "wft-test-…".into(),
                tenant_id: tenant.into(),
                name: None,
                allowed_projects: projects,
                allowed_tags: tags,
                allowed_displays: vec![],
                all_displays: false,
                created_at: 0,
                instance: None,
                expires_at: None,
            },
        }
    }

    fn signal(tenant: &str, project: uuid::Uuid) -> SignalRegistration {
        SignalRegistration {
            instance: None,
            activation_trigger: None,
            source_version: None,
            setup_execution_id: None,
            program: None,
            token: "s".into(),
            tenant_id: tenant.into(),
            project_id: project,
            execution_id: None,
            node_id: "n".into(),
            is_resume: false,
            spec_json: "{}".into(),
            access_id: None,
            consumer_kind: None,
            tags: vec![],
            port_snapshot: None,
            consumer_payload: None,
            surface_kind: "public_entry".into(),
            mount_path: None,
            mount_methods: Vec::new(),
            auth_kind: "none".into(),
            auth_config: None,
            kind_state: serde_json::Value::Object(Default::default()),
            kind_state_seq: 0,
            holds: false,
        }
    }

    #[test]
    fn cross_tenant_is_not_same_tenant_even_with_wildcard_scope() {
        // A tenant-A token is NOT same-tenant with a tenant-B signal, regardless of
        // how wide its project/tag scope is. The handler maps `!same_tenant` to a
        // 404 (identical to a nonexistent token), so a cross-tenant token is not a
        // distinguishable "forbidden" that would leak its existence on another
        // account.
        let a = token("tenant-a", vec![], vec![]);
        assert!(
            !a.same_tenant(&signal("tenant-b", uuid::Uuid::from_u128(1))),
            "cross-tenant signal must not be same-tenant (handler returns 404)"
        );
    }

    #[test]
    fn same_tenant_wildcard_token_can_cancel() {
        let a = token("tenant-a", vec![], vec![]);
        let sig = signal("tenant-a", uuid::Uuid::from_u128(1));
        assert!(a.same_tenant(&sig));
        assert!(a.can_cancel_within_tenant(&sig), "same-tenant wildcard token may cancel");
    }

    #[test]
    fn project_scoped_token_is_not_same_tenant_across_tenants() {
        // A tenant-A token scoped to a project id STILL is not same-tenant with that
        // same id under tenant B: the tenant wall is a 404 checked before project
        // scope, so a matching project id can never let a token reach across.
        let pid = uuid::Uuid::from_u128(1);
        let a = token("tenant-a", vec![pid], vec![]);
        assert!(
            !a.same_tenant(&signal("tenant-b", pid)),
            "tenant wall (404) wins over a matching project id"
        );
        let own = signal("tenant-a", pid);
        assert!(a.same_tenant(&own));
        assert!(a.can_cancel_within_tenant(&own), "same tenant + covered project may cancel");
    }

    #[test]
    fn an_instance_token_cancels_its_own_instances_rows_only() {
        let pid = uuid::Uuid::from_u128(1);
        let ada = weft_core::instance::InstanceId::new("ada").unwrap();
        let mut a = token("tenant-a", vec![pid], vec![]);
        a.row.instance = Some(ada.clone());
        let mut own = signal("tenant-a", pid);
        own.instance = Some(ada);
        assert!(a.can_cancel_within_tenant(&own));
        let mut other = signal("tenant-a", pid);
        other.instance = Some(weft_core::instance::InstanceId::new("bob").unwrap());
        assert!(!a.can_cancel_within_tenant(&other));
        assert!(!a.can_cancel_within_tenant(&signal("tenant-a", pid)), "a shared row is not the instance's");
    }

    #[test]
    fn project_scoped_token_cannot_cancel_uncovered_project() {
        let covered = uuid::Uuid::from_u128(1);
        let other = uuid::Uuid::from_u128(2);
        let a = token("tenant-a", vec![covered], vec![]);
        let sig = signal("tenant-a", other);
        assert!(a.same_tenant(&sig), "same tenant, so it is a 403 (scope), not a 404");
        assert!(
            !a.can_cancel_within_tenant(&sig),
            "a project-scoped token can't cancel outside its projects (403)"
        );
    }

    #[test]
    fn tag_scoped_token_can_never_cancel() {
        // A tag restriction means the token sees a sub-project SLICE, so it must not
        // cancel (cancel reaches sibling signals of other tags in the same execution).
        let a = token("tenant-a", vec![], vec!["support".into()]);
        let sig = signal("tenant-a", uuid::Uuid::from_u128(1));
        assert!(a.same_tenant(&sig));
        assert!(
            !a.can_cancel_within_tenant(&sig),
            "a tag-scoped token can never cancel (403)"
        );
    }
}

#[cfg(test)]
mod signal_file_scope_tests {
    use super::file_belongs_to_signal;
    use crate::journal::SignalRegistration;
    use weft_core::storage::key::parse_key;

    fn signal(execution_id: Option<&str>) -> SignalRegistration {
        SignalRegistration {
            instance: None,
            activation_trigger: None,
            source_version: None,
            setup_execution_id: None,
            program: None,
            token: "tok-1".into(),
            tenant_id: "t".into(),
            project_id: uuid::Uuid::from_u128(0x100),
            execution_id: execution_id.map(|c| c.parse().expect("a uuid")),
            node_id: "n".into(),
            is_resume: execution_id.is_some(),
            spec_json: "{}".into(),
            access_id: None,
            consumer_kind: None,
            tags: vec![],
            port_snapshot: None,
            consumer_payload: None,
            surface_kind: "task_callback".into(),
            mount_path: None,
            mount_methods: Vec::new(),
            auth_kind: "none".into(),
            auth_config: None,
            kind_state: serde_json::Value::Object(Default::default()),
            kind_state_seq: 0,
            holds: false,
        }
    }

    /// A form may show its own project's files, its own run's files,
    /// and the tenant's shared files; nothing of another tenant,
    /// project or run.
    #[test]
    fn a_file_is_the_forms_to_show_only_inside_its_own_walls() {
        let execution_id = "11111111-1111-1111-1111-111111111111";
        let sig = signal(Some(execution_id));
        let p = sig.project_id;
        let ok = |key: &str| file_belongs_to_signal(&parse_key(key).expect(key), &sig);
        assert!(ok(&format!("t/project/{p}/cat")));
        assert!(ok("t/asset/0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"));
        assert!(!ok("other/asset/0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"), "another tenant's asset");
        assert!(ok(&format!("t/exec/{execution_id}/cat")));
        assert!(ok("t/shared/pool/cat"));
        assert!(!ok(&format!("other/project/{p}/cat")), "another tenant");
        assert!(!ok("t/project/q/cat"), "another project");
        assert!(!ok("t/exec/22222222-2222-2222-2222-222222222222/cat"), "another run");
        let entry = signal(None);
        assert!(!file_belongs_to_signal(&parse_key(&format!("t/exec/{execution_id}/cat")).unwrap(), &entry), "an entry signal has no run");
    }
}

#[cfg(test)]
mod gated_entry_tests {
    use super::*;

    #[test]
    fn only_an_open_entry_fires_without_a_caller_check() {
        assert!(refuse_gated_entry("none", "/connect/local/x").is_ok());
        let (status, message) = refuse_gated_entry("connection", "/connect/local/x").unwrap_err();
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert!(message.contains("call it at /connect/local/x"), "{message}");
    }
}
