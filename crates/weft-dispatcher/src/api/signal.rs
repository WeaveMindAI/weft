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
    response::Response,
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

/// THE one append onto `signal.parked_fires`. Every park site (the
/// lifecycle gate on external fires, the route_entry executor's
/// authoritative re-check) goes through here so the queue-element
/// shape and the append guards live in one place. Guards, both in
/// one atomic UPDATE (no read-then-write race):
///   - resume cap: resume signals append iff the queue is empty
///     (one submission answers one suspension; later ones are
///     duplicates). Entry signals append on every fire.
///   - id dedup: an element with the same `ParkedFire.id` already
///     queued matches zero rows, so a retry of the same park (task
///     re-run after a crash) collapses instead of double-queueing.
/// Returns the number of rows updated (0 = guard refused or the
/// signal row is gone; the caller decides what that means).
pub(crate) async fn append_parked_fire(
    pool: &sqlx::PgPool,
    token: &str,
    entry: &ParkedFire,
) -> anyhow::Result<u64> {
    let entry_json = serde_json::to_value(entry)?;
    // `@>` containment on `[{"id": ...}]` matches any element
    // carrying that id, regardless of its other fields.
    let dedup_probe = serde_json::json!([{ "id": entry.id }]);
    // `is_resume` is read from the TARGETED ROW (not a caller arg) so a
    // caller that could not first fetch the signal row (a transient read
    // error before re-parking) can still park correctly: a resume signal
    // caps `parked_fires` at one element, an entry signal does not.
    let updated = sqlx::query(
        "UPDATE signal \
         SET parked_fires = parked_fires || $1::jsonb \
         WHERE token = $2 \
           AND (is_resume = FALSE OR jsonb_array_length(parked_fires) = 0) \
           AND NOT (parked_fires @> $3::jsonb)",
    )
    .bind(&entry_json)
    .bind(token)
    .bind(&dedup_probe)
    .execute(pool)
    .await?;
    Ok(updated.rows_affected())
}

/// THE one removal from `parked_fires` (mirror of `append_parked_fire`).
/// Removes the element whose `id` equals `fire_id` BY ID, not by array
/// index, so concurrent removals of different fires commute and a drain
/// pop can never delete the wrong element after a sibling removed the
/// head out from under it (an index-based pop assumed a head-stable
/// array, which a success-path removal breaks). `fence` is the drain's
/// claim nonce: when `Some`, the removal only applies while we still own
/// the drain claim (a sibling takeover yields 0 rows, preserving the
/// drain's abort-on-takeover semantics); the success path passes `None`.
/// Returns rows affected (0 = row gone, or not our claim).
pub(crate) async fn remove_parked_fire(
    pool: &sqlx::PgPool,
    token: &str,
    fire_id: &str,
    fence: Option<&str>,
) -> anyhow::Result<u64> {
    let updated = sqlx::query(
        "UPDATE signal \
         SET parked_fires = COALESCE( \
             (SELECT jsonb_agg(elem ORDER BY ord) \
              FROM jsonb_array_elements(parked_fires) WITH ORDINALITY AS t(elem, ord) \
              WHERE elem ->> 'id' <> $2), '[]'::jsonb) \
         WHERE token = $1 \
           AND ($3::text IS NULL OR drain_claimed_by = $3)",
    )
    .bind(token)
    .bind(fire_id)
    .bind(fence)
    .execute(pool)
    .await?;
    Ok(updated.rows_affected())
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
        // Drift-detection endpoint: the whole point is to surface
        // mismatches between journal and listener registry. A
        // silenced DB error or silent JSON decode failure here
        // would defeat that. Propagate / report the failure shape.
        let journal_count = state
            .journal
            .signal_count_for_tenant(&tenant_id)
            .await
            .map_err(|e| {
                (StatusCode::INTERNAL_SERVER_ERROR, format!("signal_count_for_tenant: {e}"))
            })?;
        let listener_registry: Value = match http
            .get(format!("{}/signals", admin_url.trim_end_matches('/')))
            .send()
            .await
        {
            Ok(r) if r.status().is_success() => match r.json::<Value>().await {
                Ok(v) => v,
                Err(e) => serde_json::json!({ "decode_error": e.to_string() }),
            },
            Ok(r) => serde_json::json!({
                "http_error": r.status().as_u16(),
            }),
            Err(e) => serde_json::json!({ "network_error": e.to_string() }),
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

/// `POST /signal/{token}/skip`. Resume the suspended firing with a
/// null payload. Sibling firings of the same color keep going; the
/// skipped firing wakes, downstream null-propagation decides what
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
        if (crate::lease::now_unix()) > deadline {
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
            received_at_unix: crate::lease::now_unix(),
        };
        // A resume signal with a non-empty queue matches zero rows
        // in the shared append; we surface that as 409 Conflict so
        // the consumer knows the suspension was already answered,
        // not silently dropped under a 200.
        let updated = append_parked_fire(&state.pg_pool, token, &entry)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("park: {e}")))?;
        if updated == 0 && routing.is_resume {
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

// `crate::lease::now_unix` is the canonical wall-clock reader.

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
    let deadline: Option<i64> = row
        .try_get("fires_deadline_unix")
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("row: {e}")))?;
    Ok(FireGateInfo {
        project_id,
        status: crate::project_store::project_status_from_str(&status_str)
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("decode status: {e}")))?,
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
                        let now = crate::lease::now_unix() as u64;
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
                        // CRITICAL: pull definition_hash from the
                        // journal's ExecutionStarted event for THIS
                        // color, NOT from the project row's current
                        // hash. If the user edited and re-registered
                        // between when this color suspended and now,
                        // the project row holds the NEW hash but the
                        // suspended execution must resume on the
                        // SAME shape it was started with (the
                        // journal state is bound to that shape).
                        // Falling back to the row's hash would run
                        // the fold on the OLD state then execute
                        // against the NEW topology, which is
                        // undefined behavior.
                        let definition_hash = match state
                            .journal
                            .execution_definition_hash(color)
                            .await?
                        {
                            crate::journal::ColorLookup::Found(h) => h,
                            crate::journal::ColorLookup::NotFound => anyhow::bail!(
                                "no ExecutionStarted event for color {color}; \
                                 cannot determine the definition_hash to \
                                 resume against"
                            ),
                            crate::journal::ColorLookup::Corrupt => anyhow::bail!(
                                "journal row for color {color} is corrupt; \
                                 see dispatcher logs"
                            ),
                        };
                        crate::task_kinds::execute::enqueue_resume(
                            &state.pg_pool,
                            &project_owned,
                            color,
                            &definition_hash,
                            Some(&tenant_str),
                        )
                        .await?;
                        state.journal.consume_suspension(&token_owned).await?;
                        Ok(StatusCode::OK)
                    }
                    ProcessTarget::Entry { .. } => {
                        // Every fire gets a STABLE fire id: a drain pop
                        // already carries one (the ParkedFire id, passed
                        // as the dedup nonce); a live fire mints a fresh
                        // one. It is the RouteEntry dedup key, the
                        // execution color seed, and the ParkedFire id if
                        // the fire is later re-parked, so one fire can
                        // never spawn two executions across a park /
                        // drain / lease-rescue interleaving. ALWAYS
                        // enqueue_dedup (live fires too): the re-park path
                        // IS a re-insert path, so a non-deduped live task
                        // and its re-parked twin would otherwise both run.
                        let fire_id = dedup_nonce_owned
                            .clone()
                            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
                        let task_payload = serde_json::json!({
                            "token": token_owned,
                            "fire_id": fire_id,
                            "payload": outcome.value,
                            "tenant_id": tenant_str,
                        });
                        let key = format!("entry:{token_owned}:{fire_id}");
                        weft_task_store::tasks::enqueue_dedup(
                            &state.pg_pool,
                            weft_task_store::tasks::NewTask {
                                kind: weft_task_store::TaskKind::RouteEntry,
                                target: weft_task_store::tasks::TaskTarget::Dispatcher,
                                // Stamped so `running_count` can see
                                // routed-but-unjournaled fires: the
                                // deactivate fast-path CAS and the
                                // drain-watcher must not flip a project
                                // Inactive while one of these is in flight.
                                project_id: Some(project_owned.clone()),
                                dedup_key: Some(key),
                                color: None,
                                tenant_id: Some(tenant_str.clone()),
                                target_pod_name: None,
                                payload: task_payload,
                            },
                        )
                        .await?;
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
            // Color parse fails loud: a corrupt `color` would silently
            // reclassify a resume signal as an entry signal, and
            // `clear_all_signals` would then DELETE it instead of
            // cancelling its execution. (consumer_payload stays
            // best-effort: it's display-only enumeration data.)
            let color_str: Option<String> = r.try_get("color")?;
            let color = match color_str {
                Some(s) => Some(
                    s.parse::<weft_core::Color>()
                        .map_err(|e| anyhow::anyhow!("corrupt signal.color '{s}': {e}"))?,
                ),
                None => None,
            };
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
    let deadline: Option<i64> = row
        .try_get("fires_deadline_unix")
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("row: {e}")))?;
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
        status: crate::project_store::project_status_from_str(&status_str)
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("decode status: {e}")))?,
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

/// How the dispatcher points a live caller at the worker, decided purely
/// from the protocol. The two protocol-specific edges of the otherwise
/// shared live-connection machinery:
///   - HTTP: a `307` redirect to the gateway URL (the client follows it
///     invisibly, preserving method + body; one call from the caller's
///     code).
///   - WebSocket: a `200` with the gateway WS URL + token in the body
///     (WS cannot be redirected; the client reads the URL then opens the
///     real WebSocket to it).
/// Pure: maps (protocol, gateway_url) to the response form. Unit tested
/// without a router or socket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum HandshakeResponse {
    /// `307 Temporary Redirect` with this `Location`.
    Redirect { location: String },
    /// `200 OK` with this JSON body (`{ "url": ..., "protocol": "websocket" }`).
    ReturnUrl { url: String },
}

/// Decide the caller-pointing response. `gateway_url` already carries the
/// routing token (the dispatcher built it after minting the token), so
/// this step only chooses the HTTP shape per protocol.
pub(crate) fn handshake_response(
    protocol: weft_core::signal::Protocol,
    gateway_url: String,
) -> HandshakeResponse {
    match protocol {
        weft_core::signal::Protocol::Http => HandshakeResponse::Redirect {
            location: gateway_url,
        },
        weft_core::signal::Protocol::Websocket => HandshakeResponse::ReturnUrl { url: gateway_url },
    }
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

// ----- Live caller connection handshake ------------------------------

/// How long the handshake waits for a freshly-spawned worker pod to come
/// alive before giving the caller a "retry shortly" error. Worker spawn +
/// register is normally a few seconds.
const LIVE_SPAWN_WAIT: std::time::Duration = std::time::Duration::from_secs(30);
const LIVE_SPAWN_POLL: std::time::Duration = std::time::Duration::from_millis(500);
/// How long the handshake waits for the chosen pod's per-pod cluster DNS
/// record to become resolvable before failing the connection. A pod can be
/// DB-alive and Ready a beat before its `<pod>.weft-workers.<ns>.svc` record
/// has propagated (EndpointSlice -> CoreDNS); handing the caller a URL the
/// gateway cannot yet resolve produces a 503 "no healthy upstream". We gate
/// on the SAME resolver the gateway uses (cluster CoreDNS), so once the
/// dispatcher resolves it, the gateway's next attempt does too. This is an
/// internal service-to-service wait the caller cannot influence, so a bounded
/// deadline is correct (a hang here is a cluster bug, not user-controlled).
const LIVE_DNS_WAIT: std::time::Duration = std::time::Duration::from_secs(10);
const LIVE_DNS_POLL: std::time::Duration = std::time::Duration::from_millis(100);
/// Routing-token lifetime. Generous: it only needs to survive the caller
/// following the redirect / opening the socket, but a slow client (mobile,
/// cold DNS) should not race it. The connection, once attached, is not
/// re-validated against the token's expiry.
const LIVE_TOKEN_TTL_SECS: i64 = 120;

/// `GET|POST /connect/{*path}`: the live caller connection control
/// handshake. Authenticates the caller, ensures a worker pod is up and
/// routable, starts a fresh execution pinned to that pod, mints a signed
/// routing token, and points the caller at the gateway URL for the pod
/// (HTTP: a `307` redirect; WebSocket: a `200` with the URL in the body).
/// The dispatcher is NEVER in the byte path; the caller's actual traffic
/// flows caller -> gateway -> worker.
pub async fn connect_live(
    State(state): State<DispatcherState>,
    headers: HeaderMap,
    Path(mount_path): Path<String>,
) -> Result<Response, (StatusCode, String)> {
    if state.caller_token_secret.is_empty() || state.gateway_base_url.is_empty() {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "live caller connections are not provisioned on this dispatcher \
             (WEFT_CALLER_TOKEN_SECRET / WEFT_GATEWAY_BASE_URL unset)"
                .into(),
        ));
    }
    let normalized = if mount_path.is_empty() {
        "/".to_string()
    } else {
        format!("/{}", mount_path)
    };

    // Resolve the live_connection signal row (kind + config + the trigger
    // node + project), with auth fields for the gate.
    let row = sqlx::query(
        "SELECT s.project_id, s.node_id, s.spec_json, s.auth_kind, s.auth_config, \
                COALESCE(p.status, 'inactive') AS status \
         FROM signal s \
         LEFT JOIN project p ON p.id::text = s.project_id \
         WHERE s.mount_path = $1",
    )
    .bind(&normalized)
    .fetch_optional(&state.pg_pool)
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("mount lookup: {e}")))?
    .ok_or((StatusCode::NOT_FOUND, "no live endpoint at this path".into()))?;

    let project_id: String = row.try_get("project_id").map_err(row_err)?;
    let node_id: String = row.try_get("node_id").map_err(row_err)?;
    let spec_json: String = row.try_get("spec_json").map_err(row_err)?;
    let status_str: String = row.try_get("status").map_err(row_err)?;
    let auth_kind: String = row.try_get("auth_kind").map_err(row_err)?;
    let auth_config: Option<Value> = row.try_get("auth_config").map_err(row_err)?;

    // The signal spec carries the kind tag + the live-caller config. The
    // protocol is the kind itself (ApiEndpoint -> Http, LiveSocket -> Ws),
    // recovered from the tag; a non-live-caller tag at this route is a bug.
    let spec: weft_core::primitive::SignalSpec = serde_json::from_str(&spec_json)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("spec parse: {e}")))?;
    let protocol = weft_core::signal::protocol_for_tag(&spec.kind).ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            format!("endpoint at '{normalized}' is not a live connection ({})", spec.kind),
        )
    })?;
    // Validate the config body parses (fail loud on a malformed row); the
    // body itself travels to the worker verbatim in `spec.config`.
    serde_json::from_value::<weft_core::signal::LiveConnectionConfig>(spec.config.clone())
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("live config parse: {e}")))?;

    // Auth gate (reuse the shared gate; same as fire_public_entry).
    apply_auth_gate(&auth_kind, auth_config.as_ref(), &headers)?;

    // Project must be Active to accept a live connection.
    let project_uuid: uuid::Uuid = project_id
        .parse()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("bad project id: {e}")))?;
    if crate::project_store::project_status_from_str(&status_str)
        .map(|s| s != crate::project_store::ProjectStatus::Active)
        .unwrap_or(true)
    {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "project is not active; cannot accept a live connection".into(),
        ));
    }

    // Ensure at least one worker pod is up for the project (spawn + wait if
    // none); this does NOT admit a slot, it only guarantees a routable pod
    // exists. Admission happens atomically below as the execute-task insert.
    let tenant = state.tenant_router.tenant_for_project(&project_id);
    ensure_live_worker(&state, &project_id, tenant.as_str()).await?;

    // Prepare + ATOMICALLY admit the execution. `prepare_live_execution`
    // journals ExecutionStarted/kicks and then calls `admit_live_execution`,
    // which (in one transaction, under a per-project lock) picks the least-
    // loaded under-cap pod and INSERTS the pinned execute task on it: admission
    // IS the task insert, so there is no separate slot counter to drift and no
    // admit/insert window. It returns the chosen pod, or signals "all full" so
    // we spawn another and retry.
    let color = uuid::Uuid::new_v4();
    let pod = match prepare_live_execution(
        &state,
        &project_id,
        project_uuid,
        &node_id,
        &spec,
        tenant.as_str(),
        color,
    )
    .await
    {
        Ok(pod) => pod,
        Err(e) => {
            cleanup_failed_live_setup(&state, color).await;
            return Err(e);
        }
    };

    // Do not hand the caller a URL the gateway cannot route yet: the pod is
    // admitted and DB-alive, but its per-pod DNS record may not have
    // propagated. Wait until the record resolves (through the same cluster
    // resolver the gateway uses); on failure clean up exactly like a prepare
    // failure, since ExecutionStarted is already journaled and the task is
    // already admitted.
    if let Err(e) = wait_for_pod_dns(&pod.pod_name, &pod.namespace).await {
        cleanup_failed_live_setup(&state, color).await;
        return Err(e);
    }

    // Mint the signed routing token (pins to the chosen pod) and build the
    // per-pod gateway URL. The pod subdomain is `<pod>.<ns>` prepended to
    // the gateway host.
    let token = weft_core::caller_token::mint(
        &state.caller_token_secret,
        color,
        &project_id,
        &pod.pod_name,
        crate::lease::now_unix() + LIVE_TOKEN_TTL_SECS,
    );
    let url = build_pod_gateway_url(
        &state.gateway_base_url,
        &pod.pod_name,
        &pod.namespace,
        &mount_path,
        &token,
    );

    // Point the caller at the worker per protocol.
    Ok(match handshake_response(protocol, url) {
        HandshakeResponse::Redirect { location } => Response::builder()
            .status(StatusCode::TEMPORARY_REDIRECT)
            .header(axum::http::header::LOCATION, location)
            .body(axum::body::Body::empty())
            .expect("redirect response builds"),
        HandshakeResponse::ReturnUrl { url } => {
            let body = serde_json::json!({ "url": url, "protocol": "websocket" });
            Response::builder()
                .status(StatusCode::OK)
                .header(axum::http::header::CONTENT_TYPE, "application/json")
                .body(axum::body::Body::from(body.to_string()))
                .expect("json response builds")
        }
    })
}

/// Resolve the project definition, journal `ExecutionStarted` + the trigger
/// kicks, then ATOMICALLY admit the execution by inserting the pinned execute
/// task on the least-loaded under-cap pod (`admit_live_execution`). Returns
/// the chosen pod. If every pod is at the cap, spawns another and retries the
/// admit (bounded). The caller cleans up on `Err` (delete the pending task +
/// record a terminal); since admission IS the task insert, there is no slot
/// counter to release.
async fn prepare_live_execution(
    state: &DispatcherState,
    project_id: &str,
    project_uuid: uuid::Uuid,
    node_id: &str,
    spec: &weft_core::primitive::SignalSpec,
    tenant: &str,
    color: uuid::Uuid,
) -> Result<weft_task_store::tasks::AdmittedPod, (StatusCode, String)> {
    let definition_hash = state
        .projects
        .running_definition_hash(project_uuid)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("hash lookup: {e}")))?
        .ok_or((StatusCode::INTERNAL_SERVER_ERROR, "project has no definition_hash".into()))?;
    let project_json = state
        .projects
        .definition_for_hash(project_uuid, &definition_hash)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("def lookup: {e}")))?
        .ok_or((StatusCode::INTERNAL_SERVER_ERROR, "no definition for hash".into()))?;
    let project_def: weft_core::ProjectDefinition = serde_json::from_str(&project_json)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("def parse: {e}")))?;

    let kicks = crate::api::project::compute_trigger_kicks(&project_def, node_id, &Value::Null);
    if kicks.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("live trigger '{node_id}' has nothing downstream to run"),
        ));
    }

    let now = crate::lease::now_unix() as u64;
    state
        .journal
        .record_event(&weft_journal::ExecEvent::ExecutionStarted {
            color,
            project_id: project_id.to_string(),
            entry_node: node_id.to_string(),
            phase: weft_core::context::Phase::Fire,
            definition_hash: definition_hash.clone(),
            at_unix: now,
        })
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("journal start: {e}")))?;
    for kick in &kicks {
        state
            .journal
            .record_event(&weft_journal::ExecEvent::NodeKicked {
                color,
                node_id: kick.node_id.clone(),
                payload: kick.payload.clone(),
                at_unix: now,
            })
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("journal kick: {e}")))?;
    }
    let spec_json = serde_json::to_value(spec)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("spec serialize: {e}")))?;

    // Atomic admit-by-insert; if all pods are at the cap, spawn another and
    // retry (bounded). The first attempt usually wins (ensure_live_worker
    // already guaranteed a pod exists).
    let deadline = std::time::Instant::now() + LIVE_SPAWN_WAIT;
    loop {
        if let Some(pod) = crate::task_kinds::execute::admit_live_execution(
            &state.pg_pool,
            project_id,
            color,
            &definition_hash,
            Some(tenant),
            spec_json.clone(),
            LIVE_CAP,
        )
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("admit live exec: {e}")))?
        {
            return Ok(pod);
        }
        // Every pod is at the cap: spawn another and retry.
        spawn_worker_pod(state, project_id, tenant).await?;
        if std::time::Instant::now() >= deadline {
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                "all worker pods are at the live-connection cap and none freed up; retry shortly"
                    .into(),
            ));
        }
        tokio::time::sleep(LIVE_SPAWN_POLL).await;
    }
}

fn row_err(e: sqlx::Error) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, format!("row: {e}"))
}

/// Build the per-pod gateway URL: `<pod>` as the leftmost host label (a
/// single-label `*.` wildcard listener matches it; pod names are DNS-clean
/// with single hyphens only), and the `<namespace>` as the FIRST PATH
/// SEGMENT (NOT the host: the project namespace contains `--` and
/// Envoy/Gateway-API reject host labels with consecutive hyphens). The
/// gateway's Lua rewrite reads pod from the host + namespace from the path
/// and forwards to the pod's internal DNS with the namespace segment
/// stripped. The signed token rides the `wct` query param. `gateway_base`
/// is `<scheme>://<host>[:port]`.
// SYNC: pod-in-host + ns-in-first-path-segment <-> deploy/k8s/gateway.yaml (Lua host/path rewrite)
pub(crate) fn build_pod_gateway_url(
    gateway_base: &str,
    pod_name: &str,
    namespace: &str,
    mount_path: &str,
    token: &str,
) -> String {
    // Split scheme from host so we can inject the pod label in front of
    // the host authority.
    let (scheme, host_port) = match gateway_base.split_once("://") {
        Some((s, rest)) => (s, rest),
        None => ("https", gateway_base),
    };
    let host_port = host_port.trim_end_matches('/');
    let path = mount_path.trim_start_matches('/');
    format!("{scheme}://{pod_name}.{host_port}/{namespace}/{path}?wct={token}")
}

/// Per-pod live-connection cap: how many held connections one pod multiplexes
/// before the dispatcher fans out to (or spawns) another. A pod's load is the
/// count of in-flight live-execute tasks pinned to it (the task row IS the
/// slot). `0` would mean no cap; this default is a sane multiplexing ceiling.
const LIVE_CAP: i32 = 256;

/// Ensure at least one worker pod is alive/spawning for the project, spawning
/// one and waiting (bounded) if none exist. Does NOT admit a connection; it
/// only guarantees a routable pod exists so the atomic admit
/// (`admit_live_execution`) has a candidate. Admission itself (which pod, cap
/// enforcement) is the task insert, done by the caller.
async fn ensure_live_worker(
    state: &DispatcherState,
    project_id: &str,
    tenant: &str,
) -> Result<(), (StatusCode, String)> {
    if weft_task_store::worker_pod::has_live_for_project(&state.pg_pool, project_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("pod check: {e}")))?
    {
        return Ok(());
    }
    // None alive: spawn one and wait for it.
    spawn_worker_pod(state, project_id, tenant).await?;
    let deadline = std::time::Instant::now() + LIVE_SPAWN_WAIT;
    loop {
        tokio::time::sleep(LIVE_SPAWN_POLL).await;
        if weft_task_store::worker_pod::has_live_for_project(&state.pg_pool, project_id)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("pod check: {e}")))?
        {
            return Ok(());
        }
        if std::time::Instant::now() >= deadline {
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                "no worker pod became available for the live connection; retry shortly".into(),
            ));
        }
    }
}

/// Enqueue a `spawn_pod` task for the project (deduped on `{project}:spawn`).
/// Enqueued DIRECTLY rather than via `cold_start::spawn`, whose scan only
/// fires for projects with a pending WORKER TASK: on the live path the first
/// execute task is only inserted at admission (after a pod exists), so relying
/// on cold_start would deadlock (no pod -> no task -> no pod). The dedup key
/// collapses concurrent callers (and a later cold_start tick) onto one spawn.
async fn spawn_worker_pod(
    state: &DispatcherState,
    project_id: &str,
    tenant: &str,
) -> Result<(), (StatusCode, String)> {
    let namespace = state
        .projects
        .project_namespace(project_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("namespace lookup: {e}")))?
        .ok_or((
            StatusCode::SERVICE_UNAVAILABLE,
            "project namespace not found; project may be unregistered".into(),
        ))?;
    weft_task_store::tasks::enqueue_dedup(
        &state.pg_pool,
        weft_task_store::tasks::NewTask {
            kind: weft_task_store::TaskKind::SpawnPod,
            target: weft_task_store::tasks::TaskTarget::Dispatcher,
            project_id: Some(project_id.to_string()),
            dedup_key: Some(format!("{project_id}:spawn")),
            color: None,
            tenant_id: Some(tenant.to_string()),
            target_pod_name: None,
            payload: serde_json::to_value(weft_task_store::SpawnPodPayload {
                project_id: project_id.to_string(),
                tenant: tenant.to_string(),
                namespace,
                owner_dispatcher: state.pod_id.as_str().to_string(),
            })
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("spawn payload: {e}")))?,
        },
    )
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("enqueue spawn_pod: {e}")))?;
    Ok(())
}

/// Clean up after a live-connection setup that failed AFTER
/// `prepare_live_execution` journaled `ExecutionStarted` and admitted the
/// pinned execute task. Delete the still-pending execute task (the task row IS
/// the slot, so deleting it frees the slot) and learn who, if anyone, will run
/// this color. Then journal the cancel terminal ONLY if no worker will run it,
/// routed through the canonical guarded writer (`journal_cancel_terminals`
/// skips if a terminal already exists). If a worker CLAIMED the task in the
/// commit-but-Err race, it owns the run AND its own terminal, so we record
/// nothing (recording a cancel would stack a second, contradictory terminal on
/// a color the worker is finishing). Shared by every post-admission failure
/// path in `connect_live` (prepare error, DNS wait timeout).
async fn cleanup_failed_live_setup(state: &DispatcherState, color: uuid::Uuid) {
    use weft_task_store::tasks::SetupFailureOutcome;
    match weft_task_store::tasks::delete_pending_live_execution(&state.pg_pool, &color.to_string())
        .await
    {
        Ok(SetupFailureOutcome::NoWorkerWillRun) => {
            if let Err(ce) = crate::api::execution::journal_cancel_terminals(
                state,
                color,
                "live-connection setup failed; no worker run was created",
            )
            .await
            {
                tracing::warn!(
                    target: "weft_dispatcher::signal",
                    color = %color, error = %ce,
                    "failed to journal cancel terminal for a failed live setup"
                );
            }
        }
        // A worker owns the run and its own terminal: nothing to do.
        Ok(SetupFailureOutcome::WorkerOwnsIt) => {}
        Err(de) => tracing::warn!(
            target: "weft_dispatcher::signal",
            color = %color, error = %de,
            "failed to clean up a failed live setup; the orphan sweep reconciles if a \
             task was left on a dead pod"
        ),
    }
}

/// Per-pod connection FQDN the gateway dynamic-resolves a live caller to:
/// `<pod>.weft-workers.<ns>.svc.cluster.local:<port>`. Built from the SAME
/// pieces the gateway's host-rewrite Lua composes (headless Service name +
/// worker connection port), so the dispatcher resolves exactly what the
/// gateway will. SYNC with the Lua in `deploy/k8s/gateway.yaml`.
fn pod_connection_fqdn(pod_name: &str, namespace: &str) -> String {
    format!(
        "{pod}.{svc}.{ns}.svc.cluster.local:{port}",
        pod = pod_name,
        svc = crate::backend::k8s_worker::worker_headless_service_name(),
        ns = namespace,
        port = crate::backend::k8s_worker::WORKER_CONNECTION_PORT,
    )
}

/// Block until the chosen pod's per-pod cluster DNS record resolves, so we
/// never hand the caller a URL the gateway cannot route yet. A pod can be
/// DB-alive a beat before its A-record has propagated (EndpointSlice ->
/// CoreDNS); if the caller connects in that gap the gateway resolver gets
/// NXDOMAIN and (absent the short failure-refresh on the BackendTrafficPolicy)
/// negative-caches it into a multi-second 503. We resolve through the cluster
/// resolver (CoreDNS, the same one the gateway uses), so a success here means
/// the record is live for the gateway too. Bounded by `LIVE_DNS_WAIT`: this
/// is an internal wait the caller cannot influence, so a hang is a cluster
/// bug, not legitimate long-running user work.
async fn wait_for_pod_dns(pod_name: &str, namespace: &str) -> Result<(), (StatusCode, String)> {
    let host = pod_connection_fqdn(pod_name, namespace);
    let deadline = std::time::Instant::now() + LIVE_DNS_WAIT;
    loop {
        // A successful lookup yielding at least one address means the record
        // is live; zero addresses or NXDOMAIN means it has not propagated yet.
        if let Ok(mut addrs) = tokio::net::lookup_host(&host).await {
            if addrs.next().is_some() {
                return Ok(());
            }
        }
        if std::time::Instant::now() >= deadline {
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                format!(
                    "worker pod DNS '{host}' did not become resolvable within \
                     {}s; the pod is up but its cluster DNS record has not \
                     propagated. Retry the connection shortly.",
                    LIVE_DNS_WAIT.as_secs()
                ),
            ));
        }
        tokio::time::sleep(LIVE_DNS_POLL).await;
    }
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
    // many across firings but inspector use is per-trigger today, so
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



#[cfg(test)]
mod handshake_tests {
    use super::*;
    use weft_core::signal::Protocol;

    #[test]
    fn http_points_via_redirect() {
        let r = handshake_response(Protocol::Http, "https://gw/chat?wct=t".into());
        assert_eq!(
            r,
            HandshakeResponse::Redirect { location: "https://gw/chat?wct=t".into() }
        );
    }

    #[test]
    fn websocket_points_via_return_url() {
        let r = handshake_response(Protocol::Websocket, "wss://gw/chat?wct=t".into());
        assert_eq!(
            r,
            HandshakeResponse::ReturnUrl { url: "wss://gw/chat?wct=t".into() }
        );
    }
}

#[cfg(test)]
mod connect_url_tests {
    use super::build_pod_gateway_url;

    #[test]
    fn builds_per_pod_subdomain_url_with_token() {
        let url = build_pod_gateway_url(
            "http://127-0-0-1.nip.io:9097",
            "wp-abc",
            "wm-project-t--p",
            "chat",
            "v1.aaa.bbb",
        );
        assert_eq!(
            url,
            "http://wp-abc.127-0-0-1.nip.io:9097/wm-project-t--p/chat?wct=v1.aaa.bbb"
        );
    }

    #[test]
    fn https_and_empty_path() {
        let url = build_pod_gateway_url(
            "https://live.example.com",
            "wp-1",
            "ns1",
            "",
            "tok",
        );
        assert_eq!(url, "https://wp-1.live.example.com/ns1/?wct=tok");
    }

    #[test]
    fn defaults_scheme_when_missing() {
        let url = build_pod_gateway_url("live.example.com", "p", "n", "x", "t");
        assert_eq!(url, "https://p.live.example.com/n/x?wct=t");
    }
}
