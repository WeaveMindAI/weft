//! Signal-related dispatcher routes. Every endpoint here either relays a
//! fire to the listener or reads/writes the durable signal table.

use anyhow::Context;
use axum::{
    extract::{Path, RawQuery, State},
    http::{HeaderMap, Method, StatusCode},
    response::Response,
    Json,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::Row;

use weft_core::signal::listener_protocol::ProcessTarget;

use crate::authenticator::{authorize_project, CallerTenant};
use crate::state::DispatcherState;

/// One element of `signal.parked_fires`. Single source of truth for
/// the queue element shape: the park path serializes one of these
/// onto the array; the drain loop deserializes it back. A typo on
/// either side becomes a compile error.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParkedFire {
    /// Per-fire UUID stamped at park time. The drain pass uses this
    /// as the task-table dedup nonce so a crash between
    /// `dispatch_listener_outcome`'s task-insert and the head-pop
    /// collapses the next drain's retry to the same task. Distinct
    /// queued fires have distinct ids and never collapse.
    pub id: String,
    pub payload: Value,
    pub received_at_unix: i64,
    /// How many times the dispatcher has tried and failed to move this
    /// fire along: a dispatch that could not place it, or a route that
    /// failed (a transient read, the project not Active at route time).
    /// Zero for a fire parked by the lifecycle gate. Drives the backoff
    /// below; an element written before the field existed reads as
    /// zero.
    #[serde(default)]
    pub attempts: u32,
    /// Not drained before this instant. A re-parked fire waits
    /// `park_backoff_secs(attempts)` before the next try, so a fire that
    /// keeps failing (a missing definition, a project row gone) retries
    /// every few minutes instead of spinning against Postgres and the
    /// logs. Zero (the default) means due now.
    #[serde(default)]
    pub not_before_unix: i64,
    /// Set when the fire parked because its instance has not given (or gave
    /// an invalid) value it needs: the refusal, naming each field. Such a
    /// fire is not retried on a timer (the reaper's sweep passes it by,
    /// whatever `not_before_unix` says); the instance's next change of
    /// values routes it again (`instance_values::change`), and so does
    /// activating its trigger. Shown per trigger by `weft status` and
    /// `ctx.instances().list()`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance_gap: Option<String>,
}

/// A parked fire's identity as a drain hands it to the fire path: the
/// stable fire id (the route task's dedup nonce and execution seed) and how
/// many routes have already failed, so the next re-park backs off
/// further.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ParkedRef<'a> {
    pub id: &'a str,
    pub attempts: u32,
}

/// Seconds a fire waits before its `attempts`-th retry: 1, 2, 4, ...
/// doubling, capped at five minutes. The first failure retries almost at
/// once (a transient read); a fire that keeps failing settles at the cap
/// and the reaper's parked-fire sweep picks it up when due.
pub(crate) fn park_backoff_secs(attempts: u32) -> i64 {
    const CAP_SECS: i64 = 300;
    if attempts == 0 {
        return 0;
    }
    1i64.checked_shl(attempts - 1).unwrap_or(CAP_SECS).min(CAP_SECS)
}

/// Every instance trigger's fires waiting on a value its instance has not
/// given (`ParkedFire::instance_gap`) in `project_id`: how many, and the
/// reason the one parked last gave.
pub async fn instance_waits(
    pool: &sqlx::PgPool,
    project_id: uuid::Uuid,
) -> anyhow::Result<std::collections::BTreeMap<weft_core::activation::ActivationKey, weft_core::program::WaitingFires>> {
    let rows: Vec<(String, Option<String>, i64, String)> = sqlx::query_as(
        "SELECT s.activation_trigger, s.instance_id, count(*)::bigint, \
                (array_agg(t.elem ->> 'instance_gap' ORDER BY t.ord DESC))[1] \
         FROM signal s, jsonb_array_elements(s.parked_fires) WITH ORDINALITY AS t(elem, ord) \
         WHERE s.project_id = $1 AND s.activation_trigger IS NOT NULL AND t.elem ? 'instance_gap' \
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

/// The signal rows of `instance` in `project_id` whose queue holds a fire
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
           AND EXISTS (SELECT 1 FROM jsonb_array_elements(s.parked_fires) e WHERE e ? 'instance_gap')",
        weft_broker_client::protocol::SIGNAL_ACTIVATION_JOIN,
    ))
    .bind(project_id)
    .bind(instance.as_str())
    .fetch_all(pool)
    .await?)
}

/// Ceiling on an ENTRY signal's `parked_fires` queue. Entry fires accumulate while
/// a project is parked (inactive); an unbounded queue lets an external caller who
/// knows a public mount path grow one row without limit. Cap it so a flood is
/// refused loudly (the append returns 0 rows) rather than growing the row until a
/// write fails. Resume signals are already capped at one element. Sized generously
/// so a legitimately busy parked project isn't cut off.
const MAX_PARKED_ENTRY_FIRES: i64 = 1000;

/// THE one append onto `signal.parked_fires`. Every park site (the
/// lifecycle gate on external fires, the route_entry executor's
/// authoritative re-check) goes through here so the queue-element
/// shape and the append guards live in one place. Guards, all in
/// one atomic UPDATE (no read-then-write race):
///   - resume cap: resume signals append iff the queue is empty
///     (one submission answers one suspension; later ones are
///     duplicates). Entry signals append while under the entry cap.
///   - entry cap: an entry signal appends only while its queue is
///     below `MAX_PARKED_ENTRY_FIRES`, so a flood can't grow the row
///     without bound.
///   - id dedup: an element with the same `ParkedFire.id` already
///     queued matches zero rows, so a retry of the same park (task
///     re-run after a crash) collapses instead of double-queueing.
/// Returns what happened; a refusal names its cause, read back from the
/// row in a second query, so no caller has to guess from "0 rows" (the
/// guards above are four different facts and each one means something
/// different to the caller). The read is not in the UPDATE's
/// transaction, so the row can move between them (a drain pops the
/// queue, a sibling appends); the classification is made from the state
/// actually observed, and when that state no longer explains a refusal
/// (the queue drained under us) the append is simply tried again.
pub async fn append_parked_fire(
    pool: &sqlx::PgPool,
    token: &str,
    entry: &ParkedFire,
) -> anyhow::Result<ParkAppend> {
    let entry_json = serde_json::to_value(entry)?;
    // `@>` containment on `[{"id": ...}]` matches any element
    // carrying that id, regardless of its other fields.
    let dedup_probe = serde_json::json!([{ "id": entry.id }]);
    // Bounded so two writers racing the row forever cannot spin here;
    // hitting the bound is a loud error, not a silent drop.
    const CONTENDED_ATTEMPTS: usize = 3;
    for _ in 0..CONTENDED_ATTEMPTS {
        // `is_resume` is read from the TARGETED ROW (not a caller arg) so a
        // caller that could not first fetch the signal row (a transient read
        // error before re-parking) can still park correctly: a resume signal
        // caps `parked_fires` at one element, an entry signal caps at
        // `MAX_PARKED_ENTRY_FIRES`.
        let updated = sqlx::query(
            "UPDATE signal \
             SET parked_fires = parked_fires || $1::jsonb \
             WHERE token = $2 \
               AND (is_resume = FALSE OR jsonb_array_length(parked_fires) = 0) \
               AND (is_resume = TRUE OR jsonb_array_length(parked_fires) < $4) \
               AND NOT (parked_fires @> $3::jsonb)",
        )
        .bind(&entry_json)
        .bind(token)
        .bind(&dedup_probe)
        .bind(MAX_PARKED_ENTRY_FIRES)
        .execute(pool)
        .await?;
        if updated.rows_affected() > 0 {
            return Ok(ParkAppend::Parked);
        }
        // Refused: read the row once and classify from what it holds NOW,
        // in the order that matters to a caller (an element already
        // carrying this id is the retry case and never a loss; the caps
        // are). `jsonb_array_length` is INT4, cast to BIGINT so the
        // i64 decode matches (a mismatch here only shows at runtime,
        // against a real Postgres).
        let row: Option<(bool, i64, bool)> = sqlx::query_as(
            "SELECT is_resume, jsonb_array_length(parked_fires)::bigint, \
                    parked_fires @> $2::jsonb \
             FROM signal WHERE token = $1",
        )
        .bind(token)
        .bind(&dedup_probe)
        .fetch_optional(pool)
        .await?;
        let refusal = match row {
            None => ParkRefusal::RowGone,
            Some((_, _, true)) => ParkRefusal::AlreadyQueued,
            Some((true, len, _)) if len > 0 => ParkRefusal::ResumeAlreadyAnswered,
            Some((false, len, _)) if len >= MAX_PARKED_ENTRY_FIRES => ParkRefusal::QueueFull,
            // The row no longer refuses this append: the queue moved
            // between the UPDATE and the read. Try again.
            Some(_) => continue,
        };
        return Ok(ParkAppend::Refused(refusal));
    }
    anyhow::bail!(
        "could not park fire {} on signal {}: the parked queue kept changing under the \
         append for {CONTENDED_ATTEMPTS} attempts",
        entry.id,
        token
    )
}

/// Outcome of [`append_parked_fire`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParkAppend {
    Parked,
    Refused(ParkRefusal),
}

/// Why [`append_parked_fire`] refused, one per guard plus the row being
/// gone. Each is a different fact for the caller: a retry that finds its
/// element already queued has lost nothing; a cap has refused a NEW fire,
/// which is a loss the caller must say out loud; a vanished row means the
/// project was wiped under the fire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParkRefusal {
    /// An element with this `ParkedFire.id` is already queued.
    AlreadyQueued,
    /// A resume signal whose one submission is already queued.
    ResumeAlreadyAnswered,
    /// An entry signal at `MAX_PARKED_ENTRY_FIRES`.
    QueueFull,
    /// No signal row for this token.
    RowGone,
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

/// THE one re-stamp of a `parked_fires` element (completes the set with
/// [`append_parked_fire`] / [`remove_parked_fire`]): bump its attempt
/// count and push its due time out to `not_before_unix`, IN PLACE. The
/// caller is a drain whose dispatch of this element failed without
/// popping it: the fire is still the queue's head, and re-appending it
/// at the tail would reorder one trigger's events, so the failed
/// element keeps its position and only its retry clock moves. `fence`
/// is the drain's claim nonce, same semantics as
/// [`remove_parked_fire`]'s. Returns rows affected (0 = row gone, no
/// such element, or not our claim; the containment guard means an id
/// that is not queued writes nothing at all).
pub async fn restamp_parked_fire(
    pool: &sqlx::PgPool,
    token: &str,
    fire_id: &str,
    attempts: u32,
    not_before_unix: i64,
    fence: Option<&str>,
) -> anyhow::Result<u64> {
    // Same containment probe shape as the append's id-dedup guard: an
    // array element carrying this id, regardless of its other fields.
    let probe = serde_json::json!([{ "id": fire_id }]);
    let updated = sqlx::query(
        "UPDATE signal \
         SET parked_fires = COALESCE( \
             (SELECT jsonb_agg( \
                 CASE WHEN elem ->> 'id' = $2 THEN \
                     elem || jsonb_build_object('attempts', $3, 'not_before_unix', $4) \
                 ELSE elem END \
                 ORDER BY ord) \
              FROM jsonb_array_elements(parked_fires) WITH ORDINALITY AS t(elem, ord)), \
             '[]'::jsonb) \
         WHERE token = $1 \
           AND parked_fires @> $6::jsonb \
           AND ($5::text IS NULL OR drain_claimed_by = $5)",
    )
    .bind(token)
    .bind(fire_id)
    .bind(attempts as i64)
    .bind(not_before_unix)
    .bind(fence)
    .bind(&probe)
    .execute(pool)
    .await?;
    Ok(updated.rows_affected())
}

/// `POST /signal/{token}`. Dispatcher entry point for every
/// stateless signal fire (webhook, form submission, extension's
/// resume completion). Architecture-4: dispatcher routes by token,
/// runs the lifecycle gate (live / park / refuse), relays through
/// the `/process` of the listener process holding the signal, then journals based on the
/// returned action.
pub async fn fire_signal(
    State(state): State<DispatcherState>,
    caller: crate::api::CallerAddress,
    Path(token): Path<String>,
    body: Option<Json<Value>>,
) -> axum::response::Response {
    let payload = body.map(|Json(v)| v).unwrap_or(Value::Null);
    fire_signal_inner(&state, &token, &caller, payload).await
}

/// `POST /signal/{token}/skip`. Resume the suspended firing with a
/// null payload. Sibling firings of the same execution keep going; the
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
    caller: crate::api::CallerAddress,
    Path(token): Path<String>,
) -> axum::response::Response {
    fire_signal_inner(&state, &token, &caller, Value::Null).await
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
    if !routing.is_resume {
        if let Err(refused) =
            check_entry_limits(state, token, routing.project_id, &routing.limits, &caller.key(), None).await
        {
            return refused;
        }
    }
    fire_checked(state, token, &routing, payload).await.into_response()
}

async fn fire_checked(
    state: &DispatcherState,
    token: &str,
    routing: &FireGateInfo,
    payload: Value,
) -> Result<StatusCode, (StatusCode, String)> {
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
    // A resume answers a run that is already waiting; an entry gated by
    // a connection starts one only after the caller is checked, which
    // this door cannot do.
    if !routing.is_resume {
        refuse_gated_entry(&routing.auth_kind, "its /connect address")?;
    }
    apply_lifecycle_gate(state, token, routing, payload).await
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

/// Fire one registered signal through the shared lifecycle gate:
/// look up its routing by token, then park / refuse / dispatch. What
/// the public events receiver calls per matched subscription, so a
/// provider push passes exactly the gate every other external fire
/// does.
pub(crate) async fn fire_registered_signal(
    state: &DispatcherState,
    token: &str,
    payload: Value,
) -> Result<StatusCode, (StatusCode, String)> {
    let routing = lookup_signal_routing(state, token).await?;
    // A provider's push has no caller to count: only the entry's own
    // per-minute limit applies, and a fire past it is dropped.
    if !routing.is_resume {
        let admitted = crate::entry_limits::admit_fire(&state.pg_pool, token, None, &routing.limits, crate::lease::now_unix())
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("rate limit: {e:#}")))?;
        if let Err(refused) = admitted {
            return Err((StatusCode::TOO_MANY_REQUESTS, format!("fire dropped: {} is reached", refused.reason.describe())));
        }
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
    use crate::activation_store::ProjectStatus;

    // The governing activation is live: live fire. A signal no
    // activation governs (a wait of a run started by hand) reads as live
    // too: it has no activate/drain moment ever, so a fire parked here
    // would strand the suspended run forever.
    if matches!(
        routing.status,
        ProjectStatus::Active | ProjectStatus::Registered
    ) {
        return dispatch_listener_outcome(
            state,
            token,
            routing.project_id,
            &routing.tenant_id,
            payload,
            None,
        )
        .await;
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
            attempts: 0,
            not_before_unix: 0,
            instance_gap: None,
        };
        // The shared append names its refusal; never swallow one under a
        // 200. A fresh-UUID id can't hit the dedup guard on a live fire, so
        // that arm is a contract violation if it ever fires.
        match append_parked_fire(&state.pg_pool, token, &entry)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("park: {e}")))?
        {
            ParkAppend::Parked => {}
            ParkAppend::Refused(ParkRefusal::ResumeAlreadyAnswered) => {
                return Err((
                    StatusCode::CONFLICT,
                    "suspension already answered; duplicate submission ignored".into(),
                ));
            }
            ParkAppend::Refused(ParkRefusal::QueueFull) => {
                return Err((
                    StatusCode::TOO_MANY_REQUESTS,
                    "this entry has too many pending fires queued; wait for the project \
                     to process them (it is currently parked) or retry later"
                        .into(),
                ));
            }
            ParkAppend::Refused(ParkRefusal::RowGone) => {
                return Err((
                    StatusCode::GONE,
                    "this signal is no longer registered; the fire was not accepted".into(),
                ));
            }
            ParkAppend::Refused(ParkRefusal::AlreadyQueued) => {
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("park: a fresh fire id {} is already queued; dispatcher contract broken", entry.id),
                ));
            }
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
    /// What outside callers may do with this entry, off the stored spec
    /// its node registered (`SignalSpec::limits`), resolved against the
    /// language defaults.
    pub limits: weft_core::signal::ResolvedLimits,
}

/// The gate columns of a signal, read through the activation that
/// governs it (`signal.activation_trigger` + `signal.instance_id`, the one
/// join `SIGNAL_ACTIVATION_JOIN`). No governing activation reads as
/// live and accepting.
fn gate_select() -> String {
    format!(
        "SELECT s.token, s.project_id, s.tenant_id, s.surface_kind, s.auth_kind, \
                s.is_resume, s.spec_json, \
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
    let spec_json: String = row.try_get("spec_json").map_err(get_err)?;
    let spec: weft_core::primitive::SignalSpec = serde_json::from_str(&spec_json)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("signal spec: {e}")))?;
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
        limits: spec.limits.resolve(),
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

/// The lifecycle of the activation governing `signal` (its entry
/// trigger's, or the trigger's that fired its run), or a live one when
/// none governs it. What route-time re-checks read.
pub(crate) async fn signal_gate(
    state: &DispatcherState,
    signal: &crate::journal::SignalRegistration,
) -> anyhow::Result<crate::activation_store::ActivationLifecycle> {
    let Some(trigger) = &signal.activation_trigger else {
        return Ok(crate::activation_store::ActivationLifecycle::active());
    };
    let key = weft_core::activation::ActivationKey::new(trigger.clone(), weft_core::instance::Owner::from_instance(signal.instance.clone()));
    Ok(state
        .activations
        .list(signal.project_id)
        .await?
        .into_iter()
        .find(|a| a.key == key)
        .map(|a| a.lifecycle)
        .unwrap_or_else(crate::activation_store::ActivationLifecycle::active))
}

/// The per-minute and at-once limits of one outside call to the entry
/// `token`, checked before anything is started, so a refused call costs
/// nothing. `caller` is who is calling, spelled as its key (the verified
/// identity, or the address). `take_slot_for` is the execution of the run
/// this call will start and how long its slot holds unborn, when the
/// slot is taken now (a live handshake); a fire takes its slot when its
/// run is born, so here it only checks the entry is not already full.
/// A resume token answers one run already going and is not an entry:
/// its callers never come here.
pub(crate) async fn check_entry_limits(
    state: &DispatcherState,
    token: &str,
    project_id: uuid::Uuid,
    limits: &weft_core::signal::ResolvedLimits,
    caller: &str,
    take_slot_for: Option<(&str, i64)>,
) -> Result<(), axum::response::Response> {
    let now = crate::lease::now_unix();
    let internal = |e: anyhow::Error| {
        use axum::response::IntoResponse;
        (StatusCode::INTERNAL_SERVER_ERROR, format!("rate limit: {e:#}")).into_response()
    };
    let mut refused = crate::entry_limits::admit_call(&state.pg_pool, token, caller, limits, now)
        .await
        .map_err(internal)?
        .err();
    if refused.is_none() {
        if let Some(max) = limits.at_once {
            refused = match take_slot_for {
                Some((execution_id, unborn_until)) => {
                    crate::entry_limits::take_slot(&state.pg_pool, token, execution_id, max, unborn_until, now)
                        .await
                        .map_err(internal)?
                        .err()
                }
                None => crate::entry_limits::at_once_full(&state.pg_pool, token, max, now)
                    .await
                    .map_err(internal)?,
            };
        }
    }
    let Some(refused) = refused else { return Ok(()) };
    tracing::info!(
        target: "weft_dispatcher::signal",
        token = %token, project_id = %project_id,
        "public call refused: {} is reached", refused.reason.describe()
    );
    crate::entry_limits::note_refusal(&state.pg_pool, token, refused.reason, now)
        .await
        .map_err(internal)?;
    Err(crate::entry_limits::too_many(refused))
}


#[cfg(test)]
mod park_backoff_tests {
    use super::*;

    /// 1, 2, 4, ... doubling from the first failed route, capped at five
    /// minutes, and never overflowing on an absurd count.
    #[test]
    fn backoff_doubles_from_one_second_and_caps() {
        assert_eq!(park_backoff_secs(0), 0);
        assert_eq!(park_backoff_secs(1), 1);
        assert_eq!(park_backoff_secs(2), 2);
        assert_eq!(park_backoff_secs(5), 16);
        assert_eq!(park_backoff_secs(9), 256);
        assert_eq!(park_backoff_secs(10), 300);
        assert_eq!(park_backoff_secs(40), 300);
        assert_eq!(park_backoff_secs(u32::MAX), 300);
    }

    /// An element written before the backoff fields existed still
    /// decodes: it reads as never retried and due now, so a queue
    /// parked by an older dispatcher drains as before.
    #[test]
    fn a_parked_element_without_backoff_fields_reads_as_due_now() {
        let old = serde_json::json!({
            "id": "f1", "payload": {"x": 1}, "received_at_unix": 7
        });
        let fire: ParkedFire = serde_json::from_value(old).expect("old element decodes");
        assert_eq!(fire.attempts, 0);
        assert_eq!(fire.not_before_unix, 0);
    }
}

/// The post-park-gate processor. Relays the payload to the
/// listener's `/process`, then dispatches based on the returned
/// `ProcessTarget`. This is THE shared "what does the dispatcher
/// do with a fire" function: every path (external fire that
/// passed the gate, drain replay of a parked payload, internal
/// stateful callback) converges here. The dispatcher stays
/// kind-unaware: the listener owns the resume-vs-entry decision
/// (it stored is_resume + execution at register time).
///
/// `parked`: identifies one specific fire so a mid-flight crash between
/// task-insert and the caller's commit can be safely retried without
/// producing a duplicate execution. The drain pass supplies the per-fire
/// UUID stamped at park time; live fires pass `None` (no retry path that
/// could double-insert).
pub(crate) async fn dispatch_listener_outcome(
    state: &DispatcherState,
    token: &str,
    project_id: uuid::Uuid,
    tenant: &str,
    payload: Value,
    // `Some` when a drain pops a parked element: its id is the fire's
    // stable identity and its attempt count carries into the route
    // task. `None` for a live fire, which mints a fresh id.
    parked: Option<ParkedRef<'_>>,
) -> Result<StatusCode, (StatusCode, String)> {
    let token_owned = token.to_string();
    let tenant_str = tenant.to_string();
    let parked_id = parked.map(|p| p.id.to_string());
    let attempts = parked.map(|p| p.attempts).unwrap_or(0);
    let result: Result<StatusCode, anyhow::Error> = async {
        // The listener answers for any signal whose row exists, whether
        // it has it in memory or not (it loads the row on a miss), so a
        // parked webhook firing long after the listener last saw it still
        // finds it.
        let outcome = state.listener.process(&token_owned, &payload).await?;
        {
                match outcome.target {
                    ProcessTarget::Resume { execution_id, .. } => {
                        let execution_id: weft_core::ExecutionId = execution_id
                            .parse()
                            .map_err(|e| anyhow::anyhow!("bad execution from listener: {e}"))?;
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
                        //     dedup_key `{execution_id}:{TaskKind::Resume}`,
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
                                    execution_id,
                                    token: token_owned.clone(),
                                    value: outcome.value,
                                    at_unix: now,
                                },
                                &format!("suspension_resolved:{token_owned}"),
                            )
                            .await?;
                        // CRITICAL: pull definition_hash from the
                        // journal's ExecutionStarted event for THIS
                        // execution, NOT from the project row's current
                        // hash. If the user edited and re-registered
                        // between when this execution suspended and now,
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
                            .execution_definition_hash(execution_id)
                            .await?
                        {
                            crate::journal::ExecutionIdLookup::Found(h) => h,
                            crate::journal::ExecutionIdLookup::NotFound => anyhow::bail!(
                                "no ExecutionStarted event for execution {execution_id}; \
                                 cannot determine the definition_hash to \
                                 resume against"
                            ),
                            crate::journal::ExecutionIdLookup::Corrupt => anyhow::bail!(
                                "journal row for execution {execution_id} is corrupt; \
                                 see dispatcher logs"
                            ),
                        };
                        crate::task_kinds::execute::enqueue_resume(
                            &state.pg_pool,
                            project_id,
                            execution_id,
                            &definition_hash,
                            &tenant_str,
                        )
                        .await?;
                        // The row is gone now, so the listener only learns
                        // of it from the row we just deleted.
                        if let Some(consumed) = state.journal.consume_suspension(&token_owned).await? {
                            state.listener.unregister_many(&[consumed]).await;
                        }
                        Ok(StatusCode::OK)
                    }
                    ProcessTarget::Entry => {
                        // Every fire gets a STABLE fire id: a drain pop
                        // already carries one (the ParkedFire id, passed
                        // as the dedup nonce); a live fire mints a fresh
                        // one. It is the RouteEntry dedup key, the
                        // execution seed, and the ParkedFire id if
                        // the fire is later re-parked, so one fire can
                        // never spawn two executions across a park /
                        // drain / lease-rescue interleaving. ALWAYS
                        // enqueue_dedup (live fires too): the re-park path
                        // IS a re-insert path, so a non-deduped live task
                        // and its re-parked twin would otherwise both run.
                        let fire_id = parked_id
                            .clone()
                            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
                        let task_payload = serde_json::to_value(
                            crate::task_kinds::route_entry::RouteEntryPayload {
                                token: token_owned.clone(),
                                fire_id: fire_id.clone(),
                                payload: outcome.value,
                                tenant_id: tenant_str.clone(),
                                attempts,
                            },
                        )?;
                        let key = format!("entry:{token_owned}:{fire_id}");
                        weft_task_store::tasks::enqueue_dedup(
                            &state.pg_pool,
                            weft_task_store::tasks::NewTask {
                                kind: weft_task_store::TaskKind::RouteEntry.into(),
                                target: weft_task_store::tasks::TaskTarget::Dispatcher,
                                // Stamped so `running_count` can see
                                // routed-but-unjournaled fires: the
                                // deactivate fast-path CAS and the
                                // drain-watcher must not flip a project
                                // Inactive while one of these is in flight.
                                project_id: Some(project_id),
                                dedup_key: Some(key),
                                execution_id: None,
                                tenant_id: tenant_str.clone(),
                                target_replica: None,
                                binary_hash: None,
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
        }
    }
    .await;
    result.map_err(|e| {
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
        // cancel_execution_id, in one transaction, strips the execution's wake
        // signals, journals NodeCancelled per non-terminal node plus
        // ExecutionCancelled, and queues the cancel task for the process
        // driving it (which flips the run's CancellationFlag); the
        // journal bridge publishes each row onto the project SSE bus.
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
             OR jsonb_array_length(s.parked_fires) = 0 \
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
/// the matched row and lets it through the park gate into
/// `dispatch_listener_outcome`. Two calls it refuses rather than
/// serves, both because the answer lives at `/connect`: a pattern
/// that captured part of the path, and a row gated by a connection.
/// So what fires here is always a bare, open address. Anything that
/// matches nothing 404s.
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
    // A bare-path fire is always an entry (a resume token has no mount).
    if let Err(refused) =
        check_entry_limits(&state, &token, routing.project_id, &routing.limits, &caller.key(), None).await
    {
        return refused;
    }
    apply_lifecycle_gate(&state, &token, &routing, payload).await.into_response()
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
    // The stored address is `/<tenant>/<pattern>`, and a pattern is not
    // a string to compare: `cards/{id}` has to be MATCHED against
    // `cards/7`. This used to be an equality lookup, so a registered
    // address holding a capture could never be reached through here at
    // all, whatever was called.
    let (tenant, called) = split_tenant(&normalized).map_err(|_| refuse())?;
    let rows: Vec<RouteRow> = sqlx::query_as::<_, (String, Vec<String>, String)>(
        "SELECT s.mount_path, s.mount_methods, s.token \
         FROM signal s \
         WHERE s.tenant_id = $1 AND s.surface_kind = 'public_entry' \
           AND s.mount_path IS NOT NULL",
    )
    .bind(tenant)
    .fetch_all(&state.pg_pool)
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("mount lookup: {e}")))?
    .into_iter()
    .map(|(mount_path, mount_methods, token)| RouteRow { mount_path, mount_methods, token })
    .collect();
    let (matched, params) = resolve_route(&rows, tenant, "POST", called).map_err(|_| refuse())?;
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


/// The auth gate of a live route: who may open a connection on it.
/// `none` admits everyone; `connection` asks the broker to check the
/// caller against the connection the route names (the broker holds the
/// connection's `verify` recipe and its material; the dispatcher never
/// sees a secret). Answers the identity the check established, which
/// rides the request as its `caller`; a refusal is the broker's flat
/// `401`, and anything else that goes wrong is a `500` naming it.
/// Generic in `auth_kind`: a new scheme is a new arm here and a new
/// `SignalAuth` variant, never a node name.
async fn caller_gate(
    state: &DispatcherState,
    auth_kind: &str,
    auth_config: Option<&Value>,
    tenant: &str,
    // Whose route it is (its gate's connection is that instance's own).
    for_instance: Option<weft_core::instance::InstanceScope>,
    call: &CallerRequestParts<'_>,
) -> Result<Option<Value>, (StatusCode, String)> {
    match auth_kind {
        "none" => Ok(None),
        "connection" => {
            let cfg = auth_config
                .ok_or((StatusCode::INTERNAL_SERVER_ERROR, "connection auth has no config".into()))?;
            let field = |name: &str| -> Result<String, (StatusCode, String)> {
                cfg.get(name).and_then(Value::as_str).map(str::to_string).ok_or((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("connection auth config has no '{name}'"),
                ))
            };
            let verify = weft_broker_client::protocol::CallerVerifyRequest {
                tenant: tenant.to_string(),
                for_instance,
                access_id: field("access_id")?,
                service: field("service")?,
                method: call.method.to_string(),
                path: call.path.to_string(),
                headers: call.headers.clone(),
                query: call.query.clone(),
                body_b64: {
                    use base64::Engine as _;
                    base64::engine::general_purpose::STANDARD.encode(call.body)
                },
            };
            let verdict: weft_broker_client::protocol::CallerVerified =
                crate::broker_admin::forward_json(state, "/v1/caller/verify", &verify)
                    .await
                    .map_err(|(status, msg)| {
                        // The broker's 401 IS the answer (flat, the reason in
                        // its log); anything else is our problem, not the
                        // caller's.
                        if status == StatusCode::UNAUTHORIZED {
                            (StatusCode::UNAUTHORIZED, "refused".to_string())
                        } else {
                            (StatusCode::INTERNAL_SERVER_ERROR, format!("caller verify: {msg}"))
                        }
                    })?;
            Ok(Some(verdict.identity))
        }
        other => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("unknown auth_kind: {other}"),
        )),
    }
}

/// Who a call through a live route is for. An instance token
/// ([`weft_core::instance::INSTANCE_TOKEN_HEADER`]) names its instance on any
/// route of its one project; otherwise the Weft-Instance header names one,
/// honoured only on a `gated` route. A token and a header that disagree
/// are refused rather than one silently winning.
async fn door_instance(
    state: &DispatcherState,
    headers: &std::collections::BTreeMap<String, String>,
    gated: bool,
    project_id: uuid::Uuid,
) -> Result<Option<weft_core::instance::InstanceId>, (StatusCode, String)> {
    let named = weft_core::instance::instance_from_header(headers, gated).map_err(|why| (StatusCode::BAD_REQUEST, why));
    let presented = headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(weft_core::instance::INSTANCE_TOKEN_HEADER))
        .map(|(_, value)| value.trim().to_string());
    let Some(presented) = presented else { return named };
    let token = require_scoped_signal_token(state, &presented).await?.row;
    let Some((token_project, instance)) = token.instance_scope() else {
        return Err((
            StatusCode::UNAUTHORIZED,
            format!("{} holds a token that is not an instance token", weft_core::instance::INSTANCE_TOKEN_HEADER),
        ));
    };
    if token_project != project_id {
        return Err((StatusCode::UNAUTHORIZED, "this instance token is for another project".into()));
    }
    // The header is checked only against the token here: a token is its
    // own proof, so the open-route rule does not apply to it.
    let header_instance = weft_core::instance::instance_from_header(headers, true).map_err(|why| (StatusCode::BAD_REQUEST, why))?;
    if header_instance.as_ref().is_some_and(|m| m != instance) {
        return Err((
            StatusCode::BAD_REQUEST,
            format!(
                "the {} header names instance '{}', and the instance token is instance '{instance}'s",
                weft_core::instance::INSTANCE_HEADER,
                header_instance.expect("checked above"),
            ),
        ));
    }
    Ok(Some(instance.clone()))
}

/// The parts of a caller's opening request the gate hands the broker.
struct CallerRequestParts<'a> {
    method: &'a str,
    path: &'a str,
    headers: &'a std::collections::BTreeMap<String, String>,
    query: &'a std::collections::BTreeMap<String, String>,
    body: &'a [u8],
}

/// One public-entry row of the tenant, as the route matcher sees it.
#[derive(Debug)]
pub(crate) struct RouteRow {
    pub mount_path: String,
    pub mount_methods: Vec<String>,
    pub token: String,
}

/// The route a call resolved to: its row and the path's captures.
pub(crate) type ResolvedRoute<'r> = (&'r RouteRow, std::collections::BTreeMap<String, String>);

/// Pick the row serving `method` on `path` among the tenant's public
/// entries, or the HTTP answer when none does: `404` for an unknown
/// path, `405` naming the allowed methods for a known path called with
/// the wrong verb. Pure over the rows (the SQL only narrows to the
/// tenant); a stored pattern that no longer parses is skipped, loud in
/// the log (its own register validated it, so this is corruption).
pub(crate) fn resolve_route<'r>(
    rows: &'r [RouteRow],
    tenant: &str,
    method: &str,
    path: &str,
) -> Result<ResolvedRoute<'r>, (StatusCode, String)> {
    let candidates = rows.iter().filter_map(|row| {
        let pattern = crate::task_kinds::register_signal::pattern_of_mount_path(&row.mount_path, tenant);
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

/// Split the called `/connect/{*path}` into the tenant segment and the
/// path under the project (no leading slash). The tenant is always the
/// first segment; a call with none is not a route anyone registered.
pub(crate) fn split_tenant(called: &str) -> Result<(&str, &str), (StatusCode, String)> {
    let called = called.trim_start_matches('/');
    let (tenant, rest) = match called.split_once('/') {
        Some((t, r)) => (t, r),
        None => (called, ""),
    };
    if tenant.is_empty() {
        return Err((StatusCode::NOT_FOUND, "no live endpoint at this path".into()));
    }
    Ok((tenant, rest))
}

// ----- Live caller connection handshake ------------------------------

/// Routing-token lifetime. Generous: it only needs to survive the caller
/// following the redirect / opening the socket, but a slow client (mobile,
/// cold DNS) should not race it. The connection, once attached, is not
/// re-validated against the token's expiry. 120 seconds in real time, at
/// this install's pace (`weft_core::time_scale`).
fn live_token_ttl_secs() -> i64 {
    weft_core::time_scale::scaled_secs(120)
}

/// `ANY /connect/{*path}`: the live caller connection control handshake.
/// Matches the call against the tenant's routes (pattern + method),
/// checks the caller against the route's auth, mints a signed routing
/// token (carrying the execution the birth will use and the program armed
/// now), and points the caller at the install's live door for the project
/// (HTTP: a `307` redirect; WebSocket: a `200` with the URL in the body).
/// The live door (`crate::live_door`) forwards the caller to one of the
/// project's workers. Nothing is admitted and no execution is born here:
/// the execution is born when the caller actually arrives at a worker
/// (`birth_on_arrival`), on that worker, so a caller who never follows
/// the redirect leaves nothing behind.
/// Set by the install's front door on a request that came for one
/// project's API domain: the handshake then matches only that project's
/// routes. The front door drops any copy a caller sent; one that reaches
/// the handshake some other way can only narrow what matches.
// SYNC: API_PROJECT_HEADER <-> crates/weft-runtime/src/front_door.rs (route)
pub const API_PROJECT_HEADER: &str = "x-weft-api-project";

pub async fn connect_live(
    State(state): State<DispatcherState>,
    address: crate::api::CallerAddress,
    method: Method,
    headers: HeaderMap,
    Path(called_path): Path<String>,
    RawQuery(raw_query): RawQuery,
    body: axum::body::Body,
) -> Result<Response, (StatusCode, String)> {
    if state.caller_token_secret.is_empty() {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "live caller connections are not provisioned on this dispatcher \
             (WEFT_CALLER_TOKEN_SECRET unset)"
                .into(),
        ));
    }
    let (tenant_segment, path) = split_tenant(&called_path)?;
    let (tenant_segment, path) = (tenant_segment.to_string(), path.to_string());
    let method_name = method.as_str().to_string();

    // The tenant's public entries, matched in Rust: a route is a pattern
    // (`chat/{room}`), never an equality key.
    let only_project = match headers.get(API_PROJECT_HEADER) {
        None => None,
        Some(v) => Some(
            v.to_str()
                .ok()
                .and_then(|v| v.parse::<uuid::Uuid>().ok())
                .ok_or((StatusCode::BAD_REQUEST, format!("{API_PROJECT_HEADER} is not a project id")))?,
        ),
    };
    let rows = sqlx::query(
        "SELECT s.token, s.mount_path, s.mount_methods \
         FROM signal s \
         WHERE s.tenant_id = $1 AND s.surface_kind = 'public_entry' AND s.mount_path IS NOT NULL \
           AND ($2::uuid IS NULL OR s.project_id = $2)",
    )
    .bind(&tenant_segment)
    .bind(only_project)
    .fetch_all(&state.pg_pool)
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("route lookup: {e}")))?
    .into_iter()
    .map(|r| {
        Ok(RouteRow {
            token: r.try_get("token").map_err(row_err)?,
            mount_path: r.try_get("mount_path").map_err(row_err)?,
            mount_methods: r.try_get("mount_methods").map_err(row_err)?,
        })
    })
    .collect::<Result<Vec<_>, (StatusCode, String)>>()?;
    let (matched, params) = resolve_route(&rows, &tenant_segment, &method_name, &path)?;
    let token = matched.token.clone();

    let route = armed_route(&state, &token).await?;
    let ArmedRoute { project_id, node_id, protocol, live_config, auth_kind, auth_config, program, .. } = &route;

    // What the caller sent, as the gate sees it. The body is read here
    // ONLY when the gate needs it (a signing scheme covers the bytes); the
    // 307 makes the caller resend it to the worker, which reads it there
    // in every case.
    let header_map: std::collections::BTreeMap<String, String> = headers
        .iter()
        .filter_map(|(k, v)| Some((k.as_str().to_string(), v.to_str().ok()?.to_string())))
        .collect();
    let query = weft_core::route::parse_query(raw_query.as_deref().unwrap_or(""));
    let body_bytes = if auth_kind == "connection" {
        let limit = live_config.max_inbound_bytes as usize;
        axum::body::to_bytes(body, limit)
            .await
            .map_err(|_| (StatusCode::PAYLOAD_TOO_LARGE, format!("request body exceeds {limit} bytes")))?
    } else {
        axum::body::Bytes::new()
    };
    let caller = caller_gate(
        &state,
        auth_kind,
        auth_config.as_ref(),
        &tenant_segment,
        route.instance.clone().map(|instance| weft_core::instance::InstanceScope { project_id: *project_id, instance }),
        &CallerRequestParts {
            method: &method_name,
            path: &path,
            headers: &header_map,
            query: &query,
            body: &body_bytes,
        },
    )
    .await?;
    // Who the run is for: an instance token, or the Weft-Instance header
    // behind the gate that just passed (an open route refuses it).
    let instance = door_instance(&state, &header_map, auth_kind != "none", *project_id).await?;

    // What the gate approved, so the worker can hold the caller to it.
    // Only when something was actually checked: an open route approves
    // nobody, so there is nothing to be held to and no reason to stop a
    // caller re-using their own redirect.
    let approved = (auth_kind != "none").then(|| {
        weft_core::caller_token::RequestFingerprint::of(
            &method_name,
            &path,
            raw_query.as_deref().unwrap_or(""),
            &body_bytes,
        )
    });

    // Project must be Active to accept a live connection.
    route.require_active()?;

    // The entry's limits, before anything is started. The caller is who
    // the gate established when the route has auth, else the instance the
    // run is for, else the address. The slot is taken now, for the execution
    // this call's run will carry, and holds unborn for the ticket's life.
    let execution_id = uuid::Uuid::new_v4();
    let caller_key = match (&caller, &instance) {
        (Some(identity), _) => format!("id:{identity}"),
        (None, Some(instance)) => format!("instance:{instance}"),
        (None, None) => address.key(),
    };
    let issued_at = crate::lease::now_unix();
    let expires_at = issued_at + live_token_ttl_secs();
    let limits = route.spec.limits.resolve();
    if let Err(refused) = check_entry_limits(
        &state,
        &token,
        *project_id,
        &limits,
        &caller_key,
        Some((&execution_id.to_string(), expires_at)),
    )
    .await
    {
        return Ok(refused);
    }

    // Mint the signed routing token: the execution the birth will carry, the
    // program armed now (the live door forwards to its workers), and what
    // the birth needs that the arriving request cannot supply (the route,
    // the gate's verdict, the path captures). Then build the live URL on
    // the door the caller came through.
    let routing = weft_core::caller_token::mint(
        &state.caller_token_secret,
        &weft_core::caller_token::CallerTokenClaims {
            execution_id,
            project_id: *project_id,
            binary_hash: program.binary_hash.clone(),
            signal: token.clone(),
            path: path.clone(),
            params,
            caller,
            approved,
            instance,
            // The same instant the slot's hold was computed from, so the
            // ticket's life and the slot's cannot drift apart.
            exp: expires_at,
        },
    );
    let url = crate::live_relay::live_url(
        &live_door(&headers, &state.public_base_url),
        *project_id,
        &called_path,
        raw_query.as_deref().unwrap_or(""),
        &routing,
    );
    tracing::info!(
        target: "weft_dispatcher::signal",
        execution_id = %execution_id, node = %node_id,
        "live handshake: caller pointed at the live door; the execution is born on arrival"
    );

    // Point the caller at the worker per protocol.
    Ok(match handshake_response(*protocol, url) {
        // The body is for a HUMAN holding curl. Every client follows the
        // `Location` header on its own, but a person who called without
        // `-L` sees a blank answer and reads it as a failure, so the one
        // line that costs nothing says what happened and what to add.
        HandshakeResponse::Redirect { location } => Response::builder()
            .status(StatusCode::TEMPORARY_REDIRECT)
            .header(axum::http::header::LOCATION, location)
            .header(axum::http::header::CONTENT_TYPE, "text/plain; charset=utf-8")
            .body(axum::body::Body::from(
                "weft: this run is answered by a worker, named in the Location header.\n\
                 Your client should follow it; curl needs -L.\n",
            ))
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

/// A public entry as its signal row arms it: the trigger, its spec, the
/// gate's settings, and the program identity the arrival births under.
/// Read at the handshake (to gate and to point the caller) and again at
/// the arrival (to give birth), by the signal token the routing token
/// carries between the two.
pub(crate) struct ArmedRoute {
    pub project_id: uuid::Uuid,
    pub node_id: String,
    pub spec: weft_core::primitive::SignalSpec,
    pub protocol: weft_core::signal::Protocol,
    pub live_config: weft_core::signal::LiveConnectionConfig,
    pub auth_kind: String,
    pub auth_config: Option<Value>,
    pub port_snapshot: Option<Value>,
    pub program: weft_core::project::hash::ProgramIdentity,
    pub source_version: String,
    /// The status of the activation governing the route at the read.
    pub status: String,
    /// Whose route it is: the instance whose trigger registered it, `None`
    /// for a shared one. Its gate's connection is that instance's.
    pub instance: Option<weft_core::instance::InstanceId>,
}

impl ArmedRoute {
    /// A live connection is accepted only while the route's activation is
    /// Active.
    pub(crate) fn require_active(&self) -> Result<(), (StatusCode, String)> {
        if crate::project_store::project_status_from_str(&self.status)
            .map(|s| s != crate::project_store::ProjectStatus::Active)
            .unwrap_or(true)
        {
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                "project is not active; cannot accept a live connection".into(),
            ));
        }
        Ok(())
    }
}

/// The armed route behind a signal token, or the HTTP answer when the
/// row is missing or half-armed.
pub(crate) async fn armed_route(state: &DispatcherState, token: &str) -> Result<ArmedRoute, (StatusCode, String)> {
    // The status is the governing activation's (the route's own
    // trigger, for its owner); a project row gone reads as inactive.
    let row = sqlx::query(&format!(
        "SELECT s.project_id, s.node_id, s.spec_json, s.auth_kind, s.auth_config, \
                s.port_snapshot, s.program_json, s.source_version, s.instance_id, \
                CASE WHEN p.id IS NULL THEN 'inactive' ELSE COALESCE(a.status, 'active') END AS status \
         FROM signal s \
         LEFT JOIN project p ON p.id = s.project_id \
         {} \
         WHERE s.token = $1",
        weft_broker_client::protocol::SIGNAL_ACTIVATION_JOIN,
    ))
    .bind(token)
    .fetch_optional(&state.pg_pool)
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("route lookup: {e}")))?
    .ok_or((StatusCode::NOT_FOUND, "no live endpoint at this path".into()))?;

    let project_id: uuid::Uuid = row.try_get("project_id").map_err(row_err)?;
    let node_id: String = row.try_get("node_id").map_err(row_err)?;
    let spec_json: String = row.try_get("spec_json").map_err(row_err)?;
    let status: String = row.try_get("status").map_err(row_err)?;
    let auth_kind: String = row.try_get("auth_kind").map_err(row_err)?;
    let auth_config: Option<Value> = row.try_get("auth_config").map_err(row_err)?;
    let port_snapshot: Option<Value> = row.try_get("port_snapshot").map_err(row_err)?;
    let program_json: Option<Value> = row.try_get("program_json").map_err(row_err)?;
    let source_version: Option<String> = row.try_get("source_version").map_err(row_err)?;
    let instance: Option<String> = row.try_get("instance_id").map_err(row_err)?;
    let instance = instance
        .map(weft_core::instance::InstanceId::new)
        .transpose()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("corrupt signal.instance_id: {e}")))?;
    let source_version = source_version.ok_or_else(|| (StatusCode::PRECONDITION_REQUIRED, format!("trigger '{node_id}' has no original source version; activate it again")))?;
    let program: weft_core::project::hash::ProgramIdentity = serde_json::from_value(program_json
        .ok_or_else(|| (StatusCode::PRECONDITION_REQUIRED, format!("trigger '{node_id}' has no armed code identity; activate it again")))?)
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, format!("armed program identity: {error}")))?;

    // The signal spec carries the kind tag + its config. The kind itself
    // says whether it serves a caller on the line, the protocol they
    // speak, and the connection's settings (`Signal::CALLER`); a kind
    // that serves none is not a live route. The config travels to the
    // worker verbatim in `spec.config`.
    let spec: weft_core::primitive::SignalSpec = serde_json::from_str(&spec_json)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("spec parse: {e}")))?;
    if weft_core::signal::caller_protocol(&spec.kind).is_none() {
        return Err((StatusCode::BAD_REQUEST, format!("endpoint of '{node_id}' is not a live connection ({})", spec.kind)));
    }
    let (protocol, live_config) = weft_core::signal::live_connection(&spec)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("live config: {e}")))?;
    Ok(ArmedRoute {
        project_id, node_id, spec, protocol, live_config, auth_kind, auth_config,
        port_snapshot, program, source_version, status, instance,
    })
}

/// Give birth to the execution a live caller arrived for, on the worker
/// replica their connection reached: resolve the program, compute the
/// fire from the caller's request, and ATOMICALLY admit the execute task
/// pinned to that replica and journal `ExecutionStarted` + the trigger
/// kicks in one transaction (`Journal::start_live_execution`). A retry of
/// the same arrival (the caller's client resent) finds the execution
/// already admitted and answers the replica it sits on. A failure
/// anywhere leaves NOTHING journaled or queued. Returns the replica the
/// execution runs on.
pub(crate) async fn birth_on_arrival(
    state: &DispatcherState,
    route: &ArmedRoute,
    request: &weft_core::caller::LiveRequest,
    tenant: &str,
    execution_id: uuid::Uuid,
    replica: &str,
    instance: Option<&weft_core::instance::InstanceId>,
) -> Result<String, (StatusCode, String)> {
    let project_id = route.project_id;
    let definition_hash = &route.program.definition_hash;
    let project_json = state
        .projects
        .definition_for_hash(project_id, definition_hash)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("def lookup: {e}")))?
        .ok_or((StatusCode::INTERNAL_SERVER_ERROR, "no definition for hash".into()))?;
    let project_def: weft_core::ProjectDefinition = serde_json::from_str(&project_json)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("def parse: {e}")))?;

    // The caller's request IS the trigger's wake payload: the trigger node
    // reads it off `ctx.wake` and fans it onto its ports.
    let payload = serde_json::to_value(request)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("request serialize: {e}")))?;
    let crate::api::project::TriggerFire { kicks, subgraph } =
        crate::api::project::compute_trigger_fire(&project_def, &route.node_id, &payload, route.port_snapshot.as_ref())
            .map_err(|error| (StatusCode::BAD_REQUEST, error))?;
    // A run reaching something per instance runs for one, finds what that
    // instance provides filled and valid, and finds that instance's infra up:
    // refused here, to the caller standing at the door, rather than
    // mid-run. The values read are the run's.
    let instance_values = crate::api::project::refuse_instance_gaps(state, project_id, &project_def, &subgraph, instance)
        .await
        .map_err(<(StatusCode, String)>::from)?;
    // The program's own connections, as this install picked them.
    let picks = crate::api::project::picks_for_run(state, project_id, &project_def, &subgraph).await?;

    let now = crate::lease::now_unix() as u64;
    // The fire's computed subgraph rides on ExecutionStarted: the
    // boundary the engine holds the run to (see `TriggerFire`).
    let (start, kick_events) = crate::api::project::execution_birth_events(crate::api::project::Birth {
        execution_id,
        project_id,
        phase: weft_core::context::Phase::Fire,
        entry_node: &route.node_id,
        kicks: &kicks,
        program: &route.program,
        subgraph: Some(&subgraph),
        seed: None,
        source_version: Some(&route.source_version),
        instance: instance.map(|instance| crate::api::project::RunFor { instance, values: &instance_values }),
        picks: &picks,
        fired_trigger: Some(&route.node_id),
        run_kind: route.live_config.run_kind(),
        // Driven inside the caller's own request (`validate_spec` refuses
        // a live signal registered `long`).
        run_class: weft_core::run_class::RunClass::Short,
        at_unix: now,
    });
    // An unrecorded run's birth never reaches the journal: its rows ride
    // the execute task, and the worker holds the run's journal in memory.
    let unrecorded_birth: Option<Vec<weft_journal::ExecEvent>> = (!route.live_config.run_kind().journaled())
        .then(|| std::iter::once(start.clone()).chain(kick_events.iter().cloned()).collect());
    let live_start = weft_task_store::kinds::LiveConnectionStart {
        spec: route.spec.clone(),
        request: request.clone(),
        // A real caller is standing at the worker, so it waits for their
        // socket. Only a fired run serves its own body.
        fired: None,
    };
    // The execute task, pinned to the replica the caller stands at, which
    // drives it inside the caller's own request. `live_connection` carries
    // the trigger's full signal spec (so the worker recovers the protocol
    // + connection knobs and expects a caller) and the caller's request
    // (so the connection carries it).
    let task = crate::task_kinds::execute::execution_task_spec(crate::task_kinds::execute::ExecutionTask {
        kind: weft_task_store::TaskKind::Execute,
        project_id,
        execution_id,
        definition_hash,
        binary_hash: &route.program.binary_hash,
        tenant_id: tenant,
        run_class: weft_core::run_class::RunClass::Short,
        pinned_to: Some(replica.to_string()),
        live_connection: Some(live_start),
        unrecorded_birth: unrecorded_birth.as_deref(),
    })
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("live task spec: {e}")))?;

    use weft_task_store::tasks::LiveAdmitOutcome;
    match state
        .journal
        .start_live_execution(&start, &kick_events, task)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("admit live exec: {e}")))?
    {
        LiveAdmitOutcome::Admitted => Ok(replica.to_string()),
        LiveAdmitOutcome::AlreadyAdmitted { replica } => Ok(replica),
    }
}

fn row_err(e: sqlx::Error) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, format!("row: {e}"))
}

/// Which door the caller should be sent back through for the live hop.
/// A request that passed a door carries `X-Forwarded-Proto` (a cloud
/// machine's front door, which terminated TLS; the machine's pass to a
/// dispatcher of its own; a local install's tunnel) and is answered at
/// the address the caller used, so a caller on the internet is never sent
/// to a loopback address and a local one never to the tunnel. A request
/// that reached the dispatcher's own port directly (tooling on the
/// machine, a frontend on the private network) passed no door, and is
/// sent to the install's configured base, which is one.
fn live_door(headers: &HeaderMap, configured: &str) -> String {
    let through_door = headers.contains_key("x-forwarded-proto");
    match weft_core::net::request_base_url(headers).filter(|_| through_door) {
        Some(base) => base,
        None => configured.trim_end_matches('/').to_string(),
    }
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

    #[test]
    fn the_tenant_is_the_first_segment() {
        assert_eq!(split_tenant("alice/chat/room7").unwrap(), ("alice", "chat/room7"));
        assert_eq!(split_tenant("alice").unwrap(), ("alice", ""));
        assert_eq!(split_tenant("/alice/").unwrap(), ("alice", ""));
        assert_eq!(split_tenant("").unwrap_err().0, StatusCode::NOT_FOUND);
    }

    #[test]
    fn a_literal_route_beats_a_capture_and_params_come_back() {
        let rows = vec![
            row("/alice/users/{id}", &[], "by-id"),
            row("/alice/users/me", &[], "me"),
        ];
        let (hit, params) = resolve_route(&rows, "alice", "GET", "users/me").unwrap();
        assert_eq!(hit.token, "me");
        assert!(params.is_empty());
        let (hit, params) = resolve_route(&rows, "alice", "GET", "users/42").unwrap();
        assert_eq!(hit.token, "by-id");
        assert_eq!(params.get("id").map(String::as_str), Some("42"));
    }

    #[test]
    fn a_known_path_with_the_wrong_verb_is_405_naming_the_allowed_ones() {
        let rows = vec![row("/alice/items", &["POST", "PUT"], "w"), row("/alice/items", &["DELETE"], "d")];
        let err = resolve_route(&rows, "alice", "GET", "items").unwrap_err();
        assert_eq!(err.0, StatusCode::METHOD_NOT_ALLOWED);
        assert!(err.1.contains("DELETE, POST, PUT"), "{}", err.1);
    }

    #[test]
    fn an_unknown_path_is_404_and_the_root_route_serves_the_empty_path() {
        let rows = vec![row("/alice", &[], "root")];
        assert_eq!(resolve_route(&rows, "alice", "GET", "nothing").unwrap_err().0, StatusCode::NOT_FOUND);
        assert_eq!(resolve_route(&rows, "alice", "GET", "").unwrap().0.token, "root");
    }

    #[test]
    fn a_corrupt_stored_pattern_is_skipped_not_fatal() {
        let rows = vec![row("/alice/bad/{", &[], "bad"), row("/alice/good", &[], "good")];
        assert_eq!(resolve_route(&rows, "alice", "GET", "good").unwrap().0.token, "good");
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
    use super::live_door;

    /// Through a door, the caller comes back the way it came (the tunnel
    /// host, the cloud's hostname); straight to the dispatcher's own
    /// port, it is sent to the install's configured door.
    #[test]
    fn the_live_hop_goes_back_through_the_door_the_caller_used() {
        let mut h = axum::http::HeaderMap::new();
        h.insert("host", "abc.trycloudflare.com".parse().unwrap());
        h.insert("x-forwarded-proto", "https".parse().unwrap());
        assert_eq!(live_door(&h, "http://127.0.0.1:14112/"), "https://abc.trycloudflare.com");
        let mut direct = axum::http::HeaderMap::new();
        direct.insert("host", "127.0.0.1:14111".parse().unwrap());
        assert_eq!(live_door(&direct, "http://127.0.0.1:14112/"), "http://127.0.0.1:14112");
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
