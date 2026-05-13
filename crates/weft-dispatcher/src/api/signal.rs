//! Signal-related dispatcher routes. Every endpoint here either
//! relays a fire through the tenant's listener (via `with_listener`)
//! or reads/writes the durable signal table.
//!
//! Diagnostic surface:
//!   - `GET /listener/inspect`: per-tenant `signal` row counts
//!     compared against each listener's in-process registry, to
//!     surface drift between the dispatcher's view and the listener's.

use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    Json,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::Row;

use weft_listener::protocol::ProcessTarget;

use crate::state::DispatcherState;

/// One element of `signal.parked_fires`. Single source of truth for
/// the queue element shape: the park path serializes one of these
/// onto the array; the drain loop deserializes it back. A typo on
/// either side becomes a compile error.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ParkedFire {
    /// Per-fire UUID stamped at park time. The drain pass uses this
    /// as the task-table dedup nonce so a crash between
    /// `dispatch_listener_outcome`'s task-insert and the head-pop
    /// collapses the next drain's retry to the same task. Distinct
    /// queued fires have distinct ids and never collapse.
    pub id: String,
    pub payload: Value,
    pub received_at_unix: i64,
}

/// Diagnostic: per-tenant `signal` row count alongside the
/// listener's own registry contents. Drift between them means the
/// cleanup pipeline went wrong somewhere; an operator can compare
/// and decide whether to nuke a stale listener Deployment.
pub async fn listener_inspect(
    State(state): State<DispatcherState>,
) -> Result<Json<Vec<Value>>, (StatusCode, String)> {
    let rows = sqlx::query(
        "SELECT tenant_id, admin_url FROM tenant_listener",
    )
    .fetch_all(&state.pg_pool)
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("tenant_listener: {e}")))?;
    let http = reqwest::Client::new();
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let tenant_id: String = row
            .try_get("tenant_id")
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("row: {e}")))?;
        let admin_url: String = row
            .try_get("admin_url")
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("row: {e}")))?;
        let journal_count = state
            .journal
            .signal_count_for_tenant(&tenant_id)
            .await
            .unwrap_or(0);
        let listener_registry: Option<Value> = match http
            .get(format!("{}/signals", admin_url.trim_end_matches('/')))
            .send()
            .await
        {
            Ok(r) if r.status().is_success() => r.json::<Value>().await.ok(),
            _ => None,
        };
        out.push(serde_json::json!({
            "tenant_id": tenant_id,
            "listener_url": admin_url,
            "journal_signal_count": journal_count,
            "listener_registry": listener_registry,
        }));
    }
    Ok(Json(out))
}

/// `POST /signal/{token}`. Dispatcher entry point for every
/// stateless signal fire (webhook, form submission, extension's
/// resume completion). Architecture-4: dispatcher routes by token,
/// runs the lifecycle gate (live / park / refuse), relays through
/// the tenant's listener `/process`, then journals based on the
/// returned action.
pub async fn fire_signal(
    State(state): State<DispatcherState>,
    Path(token): Path<String>,
    body: Option<Json<Value>>,
) -> Result<StatusCode, (StatusCode, String)> {
    let payload = body.map(|Json(v)| v).unwrap_or(Value::Null);
    fire_signal_inner(&state, &token, payload).await
}

/// `POST /signal/{token}/skip`. Resume the suspended lane with a
/// null payload. Sibling lanes of the same color keep going; the
/// skipped lane wakes, downstream null-propagation decides what
/// happens (most nodes auto-skip on null inputs).
///
/// Auth: signal token alone (knowing it = permission to skip).
/// Same auth model as fire: a consumer that can answer the form
/// can also refuse to answer it.
///
/// Implementation: thin wrapper around fire_signal with body=null.
/// No special engine path; the engine sees a normal
/// SuspensionResolved with value=null and unwinds via existing
/// null-propagation rules.
pub async fn skip_signal(
    State(state): State<DispatcherState>,
    Path(token): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    fire_signal_inner(&state, &token, Value::Null).await
}

async fn fire_signal_inner(
    state: &DispatcherState,
    token: &str,
    payload: Value,
) -> Result<StatusCode, (StatusCode, String)> {
    let routing = lookup_signal_routing(state, token).await?;
    // Internal-surface signals (Timer, SSE) have no public path;
    // they fire from inside the listener via the FireSignal broker
    // task. External callers that somehow guess the token still hit
    // our public handler; refuse loudly instead of silently
    // swallowing.
    if routing.surface_kind == "internal" {
        return Err((
            StatusCode::NOT_FOUND,
            "internal signal kind has no public surface".into(),
        ));
    }
    apply_lifecycle_gate(state, token, &routing, payload).await
}

/// One chokepoint for every external fire. Reads the project's
/// lifecycle (status + accepting/visible/deadline) and decides:
///
/// - **Live**: dispatch to listener for immediate processing.
/// - **Park**: append `payload` to `signal.parked_fires`. Drained on
///   reactivate by `drain_parked_fires`, which calls the exact same
///   `dispatch_listener_outcome` a live fire would. Entry signals
///   append on every fire; resume signals append iff the queue is
///   empty (first submission answers the suspension, later ones are
///   dropped as duplicates).
/// - **Refuse**: 410 Gone. Used when the project is wiped, the
///   hibernate deadline has expired, or status is fully Inactive
///   with `accepting_fires=false`.
///
/// The function is signal-kind agnostic past the resume vs entry
/// queue-cap rule: park / refuse / dispatch work uniformly across
/// webhook, form, resume tokens. Stateful kinds (timer, sse) bypass
/// entirely via internal fire paths.
async fn apply_lifecycle_gate(
    state: &DispatcherState,
    token: &str,
    routing: &FireGateInfo,
    payload: Value,
) -> Result<StatusCode, (StatusCode, String)> {
    use crate::project_store::ProjectStatus;

    // Active project: live fire. Ship straight to the listener.
    if routing.status == ProjectStatus::Active {
        return dispatch_listener_outcome(state, token, &routing.project_id, payload, None).await;
    }

    // Past the deadline (hibernate-style grace expired): refuse,
    // even if accepting_fires is still true on the row. We could
    // also lazily flip accepting_fires=false when this triggers,
    // but the gate is the cheapest place to evaluate the deadline
    // and avoids a write per fire.
    if let Some(deadline) = routing.fires_deadline_unix {
        if (unix_now() as i64) > deadline {
            return Err((
                StatusCode::GONE,
                "Project is not accepting requests. Please contact the project administrator.".into(),
            ));
        }
    }

    // Not Active but still accepting fires. Append to the queue.
    // Cases:
    //   - Activating: TriggerSetup is mid-flight; the listener may
    //     not have every signal registered yet. The drain at the
    //     end of activate replays everything queued here.
    //   - Inactive in park / hibernate-in-grace mode.
    //   - Deactivating toward park / hibernate.
    // Reactivate's drain replays each element through
    // dispatch_listener_outcome in FIFO order.
    if routing.accepting_fires {
        // `id` distinguishes this queued fire from any other fire on
        // the same token, even when bodies are identical. The drain
        // uses it as the task-table dedup nonce so a crash between
        // task-insert and head-pop collapses the retry back to one
        // task (same id, same dedup_key) while two genuinely
        // distinct fires (different ids) produce two executions.
        let entry = ParkedFire {
            id: uuid::Uuid::new_v4().to_string(),
            payload,
            received_at_unix: unix_now() as i64,
        };
        let entry_json = serde_json::to_value(&entry).map_err(|e| {
            (StatusCode::INTERNAL_SERVER_ERROR, format!("park serialize: {e}"))
        })?;
        // Entry signals: unconditional append.
        // Resume signals: append iff queue is empty. Single atomic
        // UPDATE so there's no read-then-write race. A resume signal
        // with a non-empty queue matches zero rows; we surface that
        // as 409 Conflict so the consumer knows the suspension was
        // already answered, not silently dropped under a 200.
        // `jsonb_array_length` returns 0 for '[]'.
        let updated = sqlx::query(
            "UPDATE signal \
             SET parked_fires = parked_fires || $1::jsonb \
             WHERE token = $2 \
               AND ($3::bool = FALSE OR jsonb_array_length(parked_fires) = 0)",
        )
        .bind(&entry_json)
        .bind(token)
        .bind(routing.is_resume)
        .execute(&state.pg_pool)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("park: {e}")))?;
        if updated.rows_affected() == 0 && routing.is_resume {
            return Err((
                StatusCode::CONFLICT,
                "suspension already answered; duplicate submission ignored".into(),
            ));
        }
        return Ok(StatusCode::OK);
    }

    // Wiped or hibernate-post-grace fully off: refuse.
    Err((
        StatusCode::GONE,
        "Project is not accepting requests. Please contact the project administrator.".into(),
    ))
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Fire-time projection: just the three lifecycle fields the gate
/// actually reads (status + accepting + deadline) plus the project
/// id needed for tenant routing. `fires_visible_to_consumers` is
/// not on the fire path: it gates consumer enumeration in
/// `visible_signals`, never decides whether to park / refuse / pass.
pub(crate) struct FireGateInfo {
    pub project_id: String,
    pub status: crate::project_store::ProjectStatus,
    pub accepting_fires: bool,
    pub fires_deadline_unix: Option<i64>,
    pub surface_kind: String,
    /// Drives the park-queue cap: entry signals append on every
    /// fire (each event is distinct); resume signals append iff the
    /// queue is empty, because a single form submission answers a
    /// single suspension and any later submission on the same token
    /// is a duplicate.
    pub is_resume: bool,
}

pub(crate) async fn lookup_signal_routing(
    state: &DispatcherState,
    token: &str,
) -> Result<FireGateInfo, (StatusCode, String)> {
    let row = sqlx::query(
        "SELECT s.project_id, s.surface_kind, s.is_resume, \
                COALESCE(p.status, 'inactive') AS status, \
                COALESCE(p.accepting_fires, FALSE) AS accepting_fires, \
                p.fires_deadline_unix \
         FROM signal s \
         LEFT JOIN project p ON p.id::text = s.project_id \
         WHERE s.token = $1",
    )
    .bind(token)
    .fetch_optional(&state.pg_pool)
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("signal lookup: {e}")))?
    .ok_or((StatusCode::NOT_FOUND, "unknown signal token".into()))?;
    let project_id: String = row
        .try_get("project_id")
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("row: {e}")))?;
    let surface_kind: String = row
        .try_get("surface_kind")
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("row: {e}")))?;
    let is_resume: bool = row
        .try_get("is_resume")
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("row: {e}")))?;
    let status_str: String = row
        .try_get("status")
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("row: {e}")))?;
    let accepting: bool = row
        .try_get("accepting_fires")
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("row: {e}")))?;
    let deadline: Option<i64> = row.try_get("fires_deadline_unix").ok();
    Ok(FireGateInfo {
        project_id,
        status: crate::project_store::project_status_from_str(&status_str),
        accepting_fires: accepting,
        fires_deadline_unix: deadline,
        surface_kind,
        is_resume,
    })
}

/// The post-park-gate processor. Relays the payload to the
/// listener's `/process`, then dispatches based on the returned
/// `ProcessTarget`. This is THE shared "what does the dispatcher
/// do with a fire" function: every path (external fire that
/// passed the gate, drain replay of a parked payload, internal
/// stateful callback) converges here. The dispatcher stays
/// kind-unaware: the listener owns the resume-vs-entry decision
/// (it stored is_resume + color at register time).
///
/// Runs the entire flow inside `with_listener` so the listener
/// stays alive for both the `/process` POST AND the journal +
/// task-enqueue work that follows.
/// `dedup_nonce`: identifies one specific fire so a mid-flight crash
/// between task-insert and the caller's commit can be safely retried
/// without producing a duplicate execution. The drain pass supplies
/// the per-fire UUID stamped at park time; live fires pass `None`
/// (no retry path that could double-insert).
pub(crate) async fn dispatch_listener_outcome(
    state: &DispatcherState,
    token: &str,
    project_id: &str,
    payload: Value,
    dedup_nonce: Option<&str>,
) -> Result<StatusCode, (StatusCode, String)> {
    let tenant = state.tenant_router.tenant_for_project(project_id);
    let namespace = state.namespace_mapper.namespace_for(&tenant);
    let token_owned = token.to_string();
    let project_owned = project_id.to_string();
    let tenant_str = tenant.as_str().to_string();
    let dedup_nonce_owned = dedup_nonce.map(|s| s.to_string());
    state
        .listeners
        .with_listener(
            &tenant,
            &namespace,
            state.listener_backend.as_ref(),
            &state.pg_pool,
            state.pod_id.as_str(),
            |handle| async move {
                let outcome = crate::listener::process_signal(&handle, &token_owned, &payload)
                    .await?;
                match outcome.target {
                    ProcessTarget::Resume { color, .. } => {
                        let color: weft_core::Color = color
                            .parse()
                            .map_err(|e| anyhow::anyhow!("bad color from listener: {e}"))?;
                        // Order: journal SuspensionResolved, enqueue
                        // the resume task, THEN drop the suspension
                        // row. The DELETE is the only non-idempotent
                        // step, so we run it last: a crash earlier
                        // leaves the row in place, the next drain
                        // re-pops the same parked element, and the
                        // earlier steps collapse on their dedup keys:
                        //   - journal write: `record_event_dedup`
                        //     keyed on `suspension_resolved:{token}`,
                        //     so a duplicate is rejected at the
                        //     journal layer.
                        //   - resume task: `enqueue_resume` uses
                        //     dedup_key `{color}:{TaskKind::Resume}`,
                        //     so a re-call collapses to the same
                        //     task row.
                        // Without this ordering, a crash between
                        // DELETE and enqueue would leave the
                        // suspension token gone and the worker
                        // waiting forever.
                        let now = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_secs())
                            .unwrap_or(0);
                        state
                            .journal
                            .record_event_dedup(
                                &weft_journal::ExecEvent::SuspensionResolved {
                                    color,
                                    token: token_owned.clone(),
                                    value: outcome.value,
                                    at_unix: now,
                                },
                                &format!("suspension_resolved:{token_owned}"),
                            )
                            .await?;
                        crate::task_kinds::execute::enqueue_resume(
                            &state.pg_pool,
                            &project_owned,
                            color,
                            Some(&tenant_str),
                        )
                        .await?;
                        state.journal.consume_suspension(&token_owned).await?;
                        Ok(StatusCode::OK)
                    }
                    ProcessTarget::Entry { .. } => {
                        let task_payload = serde_json::json!({
                            "token": token_owned,
                            "payload": outcome.value,
                            "tenant_id": tenant_str,
                        });
                        // With a nonce (drain pop, FireSignal task
                        // retry): enqueue_dedup so a retry collapses
                        // on the per-fire dedup key.
                        // Without a nonce (live fire): plain enqueue
                        // because there is no retry path that could
                        // re-insert.
                        let new_task = |dedup_key: Option<String>| {
                            weft_task_store::tasks::NewTask {
                                kind: weft_task_store::TaskKind::RouteEntry,
                                target: weft_task_store::tasks::TaskTarget::Dispatcher,
                                project_id: None,
                                dedup_key,
                                color: None,
                                tenant_id: Some(tenant_str.clone()),
                                target_pod_name: None,
                                payload: task_payload,
                            }
                        };
                        match dedup_nonce_owned.as_deref() {
                            Some(nonce) => {
                                let key = format!("entry:{token_owned}:{nonce}");
                                weft_task_store::tasks::enqueue_dedup(
                                    &state.pg_pool,
                                    new_task(Some(key)),
                                )
                                .await?;
                            }
                            None => {
                                weft_task_store::tasks::enqueue(
                                    &state.pg_pool,
                                    new_task(None),
                                )
                                .await?;
                            }
                        }
                        Ok(StatusCode::OK)
                    }
                    ProcessTarget::Drop { reason } => {
                        tracing::debug!(
                            target: "weft_dispatcher::signal",
                            token = %token_owned,
                            reason = ?reason,
                            "listener dropped fire"
                        );
                        Ok(StatusCode::OK)
                    }
                }
            },
        )
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("listener dispatch: {e}"),
            )
        })
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
        state
            .listeners
            .unregister_many_if_alive(&state.pg_pool, &deleted)
            .await;
    }
    Ok(())
}

/// Delete every signal row belonging to `project_id` and unregister
/// each from the listener. Single SQL roundtrip for the DB delete
/// (the journal returns the deleted rows for the unregister step).
pub(crate) async fn delete_signals_for_project(
    state: &DispatcherState,
    project_id: &str,
) -> Result<(), (StatusCode, String)> {
    let deleted = state
        .journal
        .signal_remove_for_project(project_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("signal_remove_for_project: {e}")))?;
    if !deleted.is_empty() {
        state
            .listeners
            .unregister_many_if_alive(&state.pg_pool, &deleted)
            .await;
    }
    Ok(())
}

/// `DELETE /signal/{token}`. Hard-cancel the underlying execution
/// for a resume signal: every signal attached to the same color
/// goes down (so canceling one HumanQuery in a 5-parallel set
/// drops the other 4 too), the worker receives Cancel, and
/// NodeCancelled + ExecutionFailed get journaled. The journal
/// preserves everything for log-review.
///
/// For an entry-trigger signal (is_resume=false, e.g. a webhook),
/// there's no execution to cancel: the signal row gets dropped
/// and the listener registration unregistered.
///
/// Auth: requires `Authorization: Bearer <api_token>` AND the
/// token's scope must be ≥ project (kinds + tags both empty,
/// project covered). Tag-scoped tokens cannot cancel because
/// cancellation reaches into sibling signals the token can't see.
/// Tag-scoped tokens can still skip via POST /signal/{token}/skip.
pub async fn cancel_signal(
    State(state): State<DispatcherState>,
    headers: HeaderMap,
    Path(signal_token): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    let api_token = bearer_token(&headers)?;
    let scope = require_scoped_api_token(&state, &api_token).await?;

    let row = state
        .journal
        .signal_get(&signal_token)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("signal_get: {e}")))?
        .ok_or((StatusCode::NOT_FOUND, "unknown signal token".into()))?;

    if !scope.can_cancel(&row) {
        return Err((
            StatusCode::FORBIDDEN,
            "cancel requires a token whose scope is at least the whole project (no kind/tag restrictions)".into(),
        ));
    }

    if let Some(color) = row.color {
        // cancel_color strips wake signals for the color and
        // enqueues a cancel_execution task. The worker fires the
        // per-color Notify, journals NodeCancelled per non-terminal
        // node + ExecutionFailed, and the journal bridge publishes
        // each event onto the project SSE bus.
        crate::api::execution::cancel_color(&state, color)
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

/// `GET /api-token/{token}/signals`. Scoped enumeration. Filters by
/// the api_token's allowed_kinds, allowed_projects, allowed_tags AND
/// by project visibility (`fires_visible_to_consumers = TRUE`):
/// active and parked projects show up; hibernate-mode projects do
/// not. Wiped projects have no rows. The dispatcher never asks the
/// listener for the api_token: the SQL pre-filter is the only
/// scope check.
///
/// Returns the cached `consumer_payload` from each row. Computed
/// once at register time on the listener `/render` endpoint and
/// stored on the row, so this endpoint is a pure SQL read with
/// no listener round-trip; park-mode projects can serve
/// `/api-token/.../signals` even with the listener pod reaped.
pub async fn list_signals_for_token(
    State(state): State<DispatcherState>,
    Path(api_token): Path<String>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let scope = require_scoped_api_token(&state, &api_token).await?;
    let visible = scope
        .visible_signals(&state)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("filter: {e}")))?;
    let mut out: Vec<Value> = Vec::with_capacity(visible.len());
    for sig in visible {
        if let Some(payload) = sig.consumer_payload {
            out.push(payload);
        }
    }
    Ok(Json(Value::Array(out)))
}

/// `GET /api-token/{token}/health`. Liveness + auth probe.
pub async fn api_token_health(
    State(state): State<DispatcherState>,
    Path(api_token): Path<String>,
) -> Result<Json<Value>, (StatusCode, String)> {
    require_scoped_api_token(&state, &api_token).await?;
    Ok(Json(serde_json::json!({ "ok": true })))
}

/// `DELETE /api-token/{api_token}/signals`. Bulk clear-all: cancel
/// every execution this token has visibility over. Distinct colors
/// cancel once each (cancel_color drops every sibling signal of
/// the same color), so a 5-parallel HumanQuery set under one
/// execution costs one cancel call.
///
/// Auth: token's scope must be ≥ project (kinds + tags empty).
/// Tag-scoped or kind-scoped tokens get 403: clear-all reaches
/// into sibling signals they can't see; same rationale as cancel.
///
/// Returns counts: { colors_cancelled, entry_signals_dropped }.
pub async fn clear_all_signals(
    State(state): State<DispatcherState>,
    Path(api_token): Path<String>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let scope = require_scoped_api_token(&state, &api_token).await?;
    if !scope.row.allowed_kinds.is_empty() || !scope.row.allowed_tags.is_empty() {
        return Err((
            StatusCode::FORBIDDEN,
            "clear-all requires a token whose scope is at least the whole project (no kind/tag restrictions)".into(),
        ));
    }

    let visible = scope
        .visible_signals(&state)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("filter: {e}")))?;

    // Distinct colors only. cancel_color drops every sibling
    // signal of the same color, so cancelling once per color is
    // both correct AND avoids duplicate work on parallel HumanQueries.
    let mut colors: std::collections::BTreeSet<weft_core::Color> = Default::default();
    let mut entry_signals: Vec<crate::journal::SignalRegistration> = Vec::new();
    for s in visible {
        match s.color {
            Some(c) => {
                colors.insert(c);
            }
            None => entry_signals.push(s),
        }
    }

    for color in &colors {
        cancel_color_logged(&state, *color).await;
    }
    // Entry-trigger signals have no color to cancel; delete via the
    // shared helper (DB-first-listener-second ordering).
    delete_signals(&state, &entry_signals).await?;

    Ok(Json(serde_json::json!({
        "colors_cancelled": colors.len(),
        "entry_signals_dropped": entry_signals.len(),
    })))
}

/// Best-effort `cancel_color` wrapper for the admin sweep path:
/// failures are logged and skipped so one bad color doesn't block
/// the whole clear-all. (The handler exists for the admin "drop
/// everything" verb where best-effort is the contract.)
async fn cancel_color_logged(state: &DispatcherState, color: weft_core::Color) {
    if let Err(e) = crate::api::execution::cancel_color(state, color).await {
        tracing::warn!(
            target: "weft_dispatcher::signal",
            %color, error = %e,
            "clear_all_signals: cancel_color failed; skipping"
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

/// Resolve and load an api_token by string, returning a typed scope
/// helper. 401 if the token doesn't exist.
async fn require_scoped_api_token(
    state: &DispatcherState,
    token: &str,
) -> Result<TokenScope, (StatusCode, String)> {
    let row = state
        .journal
        .get_api_token(token)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("api_token: {e}")))?
        .ok_or((StatusCode::UNAUTHORIZED, "unknown api token".into()))?;
    Ok(TokenScope { row })
}

struct TokenScope {
    row: crate::journal::ApiToken,
}

impl TokenScope {
    /// True if the token may CANCEL this signal. Cancel reaches
    /// into sibling signals of the same color (different tags,
    /// different consumer kinds), so the token must have full
    /// project-level view, not a sub-project slice.
    ///
    /// Rule: project covered AND no kind/tag restrictions. Empty
    /// allowed_kinds + empty allowed_tags = "I see everything in
    /// the projects I'm allowed in." Anything narrower can only
    /// skip its own visible signals.
    fn can_cancel(&self, sig: &crate::journal::SignalRegistration) -> bool {
        if !self.row.allowed_kinds.is_empty() {
            return false;
        }
        if !self.row.allowed_tags.is_empty() {
            return false;
        }
        if !self.row.allowed_projects.is_empty() {
            let Ok(want) = sig.project_id.parse::<uuid::Uuid>() else {
                return false;
            };
            if !self.row.allowed_projects.contains(&want) {
                return false;
            }
        }
        true
    }

    /// Run the SQL filter to enumerate every signal this token sees.
    /// Filters by token scope (kinds / projects / tags) AND by
    /// project visibility: only projects with
    /// `fires_visible_to_consumers = TRUE` show up. That covers
    /// both active projects and parked projects (consumers can
    /// browse + submit; submissions still park at /signal/{token}).
    /// Hibernate projects are hidden during the entire inactive
    /// window because hibernate sets `fires_visible_to_consumers
    /// = FALSE`. Wiped projects have no rows at all.
    async fn visible_signals(
        &self,
        state: &DispatcherState,
    ) -> anyhow::Result<Vec<crate::journal::SignalRegistration>> {
        use sqlx::Row;
        let rows = sqlx::query(
            "SELECT s.token, s.tenant_id, s.project_id, s.color, s.node_id, s.is_resume, \
                    s.spec_json, s.consumer_kind, s.tags, \
                    s.consumer_payload, \
                    s.surface_kind, s.mount_path, s.auth_kind, s.auth_config, \
                    s.kind_state \
             FROM signal s \
             LEFT JOIN project p ON p.id::text = s.project_id \
             WHERE s.is_resume = TRUE AND jsonb_array_length(s.parked_fires) = 0 \
               AND COALESCE(p.fires_visible_to_consumers, FALSE) = TRUE \
               AND ($1::text[] = '{}'::text[] OR s.consumer_kind = ANY($1)) \
               AND ($2::uuid[] = '{}'::uuid[] OR s.project_id::uuid = ANY($2)) \
               AND ($3::text[] = '{}'::text[] OR s.tags && $3) \
             ORDER BY s.created_at ASC",
        )
        .bind(&self.row.allowed_kinds)
        .bind(&self.row.allowed_projects)
        .bind(&self.row.allowed_tags)
        .fetch_all(&state.pg_pool)
        .await?;
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            let color_str: Option<String> = r.try_get("color")?;
            let color = color_str.and_then(|s| s.parse::<weft_core::Color>().ok());
            let payload_str: Option<String> = r.try_get("consumer_payload")?;
            let consumer_payload =
                payload_str.and_then(|s| serde_json::from_str(&s).ok());
            out.push(crate::journal::SignalRegistration {
                token: r.try_get("token")?,
                tenant_id: r.try_get("tenant_id")?,
                project_id: r.try_get("project_id")?,
                color,
                node_id: r.try_get("node_id")?,
                is_resume: r.try_get("is_resume")?,
                spec_json: r.try_get("spec_json")?,
                consumer_kind: r.try_get("consumer_kind")?,
                tags: r.try_get("tags")?,
                consumer_payload,
                surface_kind: r.try_get("surface_kind")?,
                mount_path: r.try_get("mount_path")?,
                auth_kind: r.try_get("auth_kind")?,
                auth_config: r.try_get("auth_config")?,
                kind_state: r.try_get("kind_state")?,
            });
        }
        Ok(out)
    }
}

// ---------- PublicEntry catch-all + inspector display/action -----------

/// `POST /<mount_path>` catch-all. External clients hit this for
/// any signal whose `surface_kind = 'public_entry'` (Webhook,
/// ApiPost, future public-form). Looks up the row by `mount_path`,
/// applies the auth gate (api_key check, future schemes), park
/// gate, then `dispatch_listener_outcome`. Path components that
/// don't match a registered mount_path 404.
pub async fn fire_public_entry(
    State(state): State<DispatcherState>,
    headers: HeaderMap,
    Path(mount_path): Path<String>,
    body: Option<Json<Value>>,
) -> Result<StatusCode, (StatusCode, String)> {
    // Normalize: catch-all gives us a path without the leading
    // slash. Convert to `/foo` form (or `/` for empty) to match
    // the row.
    let normalized = if mount_path.is_empty() {
        "/".to_string()
    } else {
        format!("/{}", mount_path)
    };
    let row = sqlx::query(
        "SELECT s.token, s.project_id, s.auth_kind, s.auth_config, \
                COALESCE(p.status, 'inactive') AS status, \
                COALESCE(p.accepting_fires, FALSE) AS accepting_fires, \
                p.fires_deadline_unix \
         FROM signal s \
         LEFT JOIN project p ON p.id::text = s.project_id \
         WHERE s.mount_path = $1",
    )
    .bind(&normalized)
    .fetch_optional(&state.pg_pool)
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("mount lookup: {e}")))?
    .ok_or((
        StatusCode::NOT_FOUND,
        "Project is not accepting requests. Please contact the project administrator.".into(),
    ))?;

    let token: String = row
        .try_get("token")
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("row: {e}")))?;
    let project_id: String = row
        .try_get("project_id")
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("row: {e}")))?;
    let status_str: String = row
        .try_get("status")
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("row: {e}")))?;
    let accepting: bool = row
        .try_get("accepting_fires")
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("row: {e}")))?;
    let deadline: Option<i64> = row.try_get("fires_deadline_unix").ok();
    let auth_kind: String = row
        .try_get("auth_kind")
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("row: {e}")))?;
    let auth_config: Option<Value> = row
        .try_get("auth_config")
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("row: {e}")))?;

    apply_auth_gate(&auth_kind, auth_config.as_ref(), &headers)?;

    let payload = body.map(|Json(v)| v).unwrap_or(Value::Null);

    let routing = FireGateInfo {
        project_id,
        status: crate::project_store::project_status_from_str(&status_str),
        accepting_fires: accepting,
        fires_deadline_unix: deadline,
        // Catch-all is matched on mount_path; only public_entry rows
        // get one, so this branch is always public_entry. Resume
        // tokens use surface_kind='task_callback' and hit a
        // different route (lookup-by-token), never this one.
        surface_kind: "public_entry".to_string(),
        is_resume: false,
    };
    apply_lifecycle_gate(&state, &token, &routing, payload).await
}

/// Apply the configured auth gate to the request. Returns Ok(())
/// on pass, Err with appropriate status on fail. Generic in
/// `auth_kind`; new schemes add a branch here.
fn apply_auth_gate(
    auth_kind: &str,
    auth_config: Option<&Value>,
    headers: &HeaderMap,
) -> Result<(), (StatusCode, String)> {
    match auth_kind {
        "none" => Ok(()),
        "api_key" => {
            let cfg = auth_config.ok_or((
                StatusCode::INTERNAL_SERVER_ERROR,
                "api_key auth has no config".into(),
            ))?;
            let header_name = cfg
                .get("header_name")
                .and_then(|v| v.as_str())
                .unwrap_or("X-Api-Key");
            let value_hash = cfg
                .get("value_hash")
                .and_then(|v| v.as_str())
                .ok_or((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "api_key auth missing value_hash".into(),
                ))?;
            let supplied = headers
                .get(header_name)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            if supplied.is_empty() {
                return Err((
                    StatusCode::UNAUTHORIZED,
                    format!("missing {header_name} header"),
                ));
            }
            let supplied_hash = sha256_hex(supplied);
            if !ct_eq(&supplied_hash, value_hash) {
                return Err((StatusCode::UNAUTHORIZED, "bad api key".into()));
            }
            Ok(())
        }
        other => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("unknown auth_kind: {other}"),
        )),
    }
}

fn sha256_hex(s: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(s.as_bytes());
    let bytes = h.finalize();
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes.iter() {
        use std::fmt::Write;
        let _ = write!(&mut out, "{:02x}", b);
    }
    out
}

/// Constant-time string equality. Avoids leaking key length / a
/// timing oracle on the value_hash comparison.
fn ct_eq(a: &str, b: &str) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for (x, y) in a.bytes().zip(b.bytes()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Inspector proxy: read the listener's per-signal display info.
/// Resolves (project_id, node_id) → signal row → token →
/// listener `/display` call. Returns 503 if the listener happens
/// to be reaped (caller is the inspector UI; we don't spin the
/// listener up just to render its display).
pub async fn display_signal(
    State(state): State<DispatcherState>,
    _headers: HeaderMap,
    Path((project_id, node_id)): Path<(String, String)>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let token = lookup_signal_token_for_node(&state, &project_id, &node_id).await?;
    let tenant = state.tenant_router.tenant_for_project(&project_id);
    let display = state
        .listeners
        .with_listener_if_alive(&tenant, &state.pg_pool, |handle| async move {
            crate::listener::display_signal(&handle, &token).await
        })
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, format!("listener /display: {e}")))?
        .ok_or((StatusCode::SERVICE_UNAVAILABLE, "tenant listener not running".into()))?;
    Ok(Json(display))
}

#[derive(Debug, Deserialize)]
pub struct ActionBody {
    pub kind: String,
    #[serde(default)]
    pub payload: Value,
}

/// Inspector proxy: invoke a kind-specific `/action` on the
/// listener. Returns 503 if the listener is currently reaped:
/// actions are user-initiated and only meaningful while the
/// listener is alive (mid-cycle invariants exist on the Pod's
/// in-memory state).
pub async fn action_signal(
    State(state): State<DispatcherState>,
    _headers: HeaderMap,
    Path((project_id, node_id)): Path<(String, String)>,
    Json(body): Json<ActionBody>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let token = lookup_signal_token_for_node(&state, &project_id, &node_id).await?;
    let tenant = state.tenant_router.tenant_for_project(&project_id);
    let token_for_call = token.clone();
    let resp = state
        .listeners
        .with_listener_if_alive(&tenant, &state.pg_pool, |handle| async move {
            crate::listener::action_signal(&handle, &token_for_call, &body.kind, &body.payload).await
        })
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, format!("listener /action: {e}")))?
        .ok_or((StatusCode::SERVICE_UNAVAILABLE, "tenant listener not running".into()))?;
    if let Some(routing) = &resp.routing {
        let auth_config = if routing.auth_config.is_null() {
            None
        } else {
            Some(routing.auth_config.clone())
        };
        sqlx::query(
            "UPDATE signal SET auth_kind = $1, auth_config = $2 WHERE token = $3",
        )
        .bind(routing.auth.kind_tag())
        .bind(auth_config.as_ref())
        .bind(&token)
        .execute(&state.pg_pool)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("update auth: {e}")))?;
    }
    Ok(Json(resp.result))
}

async fn lookup_signal_token_for_node(
    state: &DispatcherState,
    project_id: &str,
    node_id: &str,
) -> Result<String, (StatusCode, String)> {
    // Pick the most recent signal for this (project, node). For
    // entry triggers there's only one; for resumes there could be
    // many across lanes but inspector use is per-trigger today, so
    // first match is fine.
    let row = sqlx::query("SELECT token FROM signal WHERE project_id = $1 AND node_id = $2 ORDER BY created_at DESC LIMIT 1")
        .bind(project_id)
        .bind(node_id)
        .fetch_optional(&state.pg_pool)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("token lookup: {e}")))?
        .ok_or((StatusCode::NOT_FOUND, format!("no signal for node {node_id}")))?;
    row.try_get("token")
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("row: {e}")))
}

/// Project-management auth: caller must present an api_token whose
/// scope includes this project. Distinct from per-signal fire auth
/// (the api_key gate); inspector actions are project-administrative.
#[cfg(test)]
mod auth_gate_tests {
    use super::*;
    use axum::http::HeaderMap;
    use serde_json::json;

    #[test]
    fn auth_none_passes_with_no_header() {
        let headers = HeaderMap::new();
        assert!(apply_auth_gate("none", None, &headers).is_ok());
    }

    #[test]
    fn auth_api_key_rejects_missing_header() {
        let cfg = json!({
            "header_name": "X-Api-Key",
            "value_hash": sha256_hex("secret"),
        });
        let headers = HeaderMap::new();
        let err = apply_auth_gate("api_key", Some(&cfg), &headers).expect_err("missing");
        assert_eq!(err.0, StatusCode::UNAUTHORIZED);
        assert!(err.1.contains("missing"));
    }

    #[test]
    fn auth_api_key_rejects_wrong_value() {
        let cfg = json!({
            "header_name": "X-Api-Key",
            "value_hash": sha256_hex("secret"),
        });
        let mut headers = HeaderMap::new();
        headers.insert("X-Api-Key", "wrong".parse().unwrap());
        let err = apply_auth_gate("api_key", Some(&cfg), &headers).expect_err("wrong");
        assert_eq!(err.0, StatusCode::UNAUTHORIZED);
    }

    #[test]
    fn auth_api_key_accepts_correct_value() {
        let cfg = json!({
            "header_name": "X-Api-Key",
            "value_hash": sha256_hex("secret"),
        });
        let mut headers = HeaderMap::new();
        headers.insert("X-Api-Key", "secret".parse().unwrap());
        assert!(apply_auth_gate("api_key", Some(&cfg), &headers).is_ok());
    }

    #[test]
    fn auth_api_key_uses_configured_header_name() {
        let cfg = json!({
            "header_name": "Authorization-Token",
            "value_hash": sha256_hex("xyz"),
        });
        let mut headers = HeaderMap::new();
        headers.insert("Authorization-Token", "xyz".parse().unwrap());
        assert!(apply_auth_gate("api_key", Some(&cfg), &headers).is_ok());
        // Default X-Api-Key shouldn't match.
        let mut headers2 = HeaderMap::new();
        headers2.insert("X-Api-Key", "xyz".parse().unwrap());
        assert!(apply_auth_gate("api_key", Some(&cfg), &headers2).is_err());
    }

    #[test]
    fn unknown_auth_kind_errors() {
        let headers = HeaderMap::new();
        let err = apply_auth_gate("hmac", None, &headers).expect_err("unknown");
        assert_eq!(err.0, StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn ct_eq_handles_lengths() {
        assert!(ct_eq("abc", "abc"));
        assert!(!ct_eq("abc", "abd"));
        assert!(!ct_eq("abc", "abcd"));
        assert!(!ct_eq("", "x"));
        assert!(ct_eq("", ""));
    }
}

#[cfg(test)]
mod public_url_tests {
    use crate::journal::SignalRegistration;

    fn fresh(surface: &str, mount: Option<&str>) -> SignalRegistration {
        SignalRegistration {
            token: "tok-1".into(),
            tenant_id: "t".into(),
            project_id: "p".into(),
            color: None,
            node_id: "n".into(),
            is_resume: false,
            spec_json: "{}".into(),
            consumer_kind: None,
            tags: vec![],
            consumer_payload: None,
            surface_kind: surface.into(),
            mount_path: mount.map(String::from),
            auth_kind: "none".into(),
            auth_config: None,
            kind_state: serde_json::Value::Object(Default::default()),
        }
    }

    #[test]
    fn public_entry_root_normalizes() {
        let s = fresh("public_entry", Some("/"));
        assert_eq!(
            s.public_url("http://localhost:9999"),
            Some("http://localhost:9999/".into())
        );
    }

    #[test]
    fn public_entry_with_path() {
        let s = fresh("public_entry", Some("/webhooks/stripe"));
        assert_eq!(
            s.public_url("http://localhost:9999"),
            Some("http://localhost:9999/webhooks/stripe".into())
        );
    }

    #[test]
    fn public_entry_strips_trailing_slash_on_base() {
        let s = fresh("public_entry", Some("/foo"));
        assert_eq!(
            s.public_url("http://localhost:9999/"),
            Some("http://localhost:9999/foo".into())
        );
    }

    #[test]
    fn task_callback_uses_token() {
        let s = fresh("task_callback", None);
        assert_eq!(
            s.public_url("http://localhost:9999"),
            Some("http://localhost:9999/signal/tok-1".into())
        );
    }

    #[test]
    fn unknown_surface_returns_none() {
        let s = fresh("future_kind", None);
        assert!(s.public_url("http://localhost:9999").is_none());
    }
}


