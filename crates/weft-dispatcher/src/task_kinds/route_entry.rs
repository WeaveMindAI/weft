//! `route_entry` task: a dispatcher Pod loads the project,
//! computes trigger kicks, journals ExecutionStarted + NodeKicked
//! events, and enqueues an execute task. Used by the listener
//! when an entry-trigger fire arrives.
//!
//! Idempotency rests on a STABLE per-fire id (`RouteEntryPayload.
//! fire_id`), minted once at the live-fire enqueue or reused from the
//! ParkedFire id on a drain pop. The RouteEntry task dedup key is
//! `entry:{token}:{fire_id}`, the execution color is `v5(fire_id)`, and
//! every journal event (ExecutionStarted / NodeKicked / ExecutionFailed)
//! is dedup-keyed on the fire id too. So any number of tasks carrying
//! the same fire (a live task and its re-parked-then-drained twin)
//! converge on ONE color whose events are single-write, never forking
//! the execution. A pre-journal failure re-parks the fire (it is not
//! lost); a post-journal failure journals a terminal ExecutionFailed (so
//! the color does not haunt running_count).

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use weft_task_store::executor::TaskExecutor;
use weft_task_store::tasks::Task;

use crate::state::DispatcherState;

/// Namespace UUID used to derive deterministic execution colors
/// from task ids. Generated once via `Uuid::new_v4` and frozen.
const COLOR_NAMESPACE: Uuid = Uuid::from_u128(0x9c4a_e6a4_0b3f_4e8e_a0f1_1d3d_9b2c_5a47);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouteEntryPayload {
    /// Token of the signal that fired. Used to look up
    /// `(project_id, node_id)` in the `signal` table at execute
    /// time. The listener doesn't pass project_id directly because
    /// the dispatcher's project store has the up-to-date copy.
    pub token: String,
    /// Stable identity of THIS fire, minted once when the fire is
    /// enqueued (live) or popped from the parked queue (drain). It is
    /// the ParkedFire id when re-parked, the execution color seed
    /// (`v5(fire_id)`), AND the RouteEntry task dedup nonce, so one fire
    /// can never spawn two executions across a park / drain / lease-
    /// rescue interleaving (every path converges on one color whose
    /// events are dedup-keyed). NOT the task id (a re-parked fire is
    /// re-enqueued under a NEW task, but keeps the same fire id).
    pub fire_id: String,
    /// Payload the trigger fire carried.
    pub payload: Value,
    /// Tenant id, propagated to the spawned execute task for
    /// listener-side resolution of tenant-scoped resources.
    pub tenant_id: String,
}

/// Outcome of a route_entry run. `Routed` carries the execution
/// color; `Reparked` means the authoritative lifecycle re-check saw
/// a non-Active project and the fire went back onto
/// `signal.parked_fires` instead of becoming journal state.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum RouteEntryResult {
    Routed { color: String },
    Reparked,
}

pub struct RouteEntryExecutor;

#[async_trait]
impl TaskExecutor<DispatcherState> for RouteEntryExecutor {
    async fn execute(&self, state: &DispatcherState, task: &Task) -> Result<Value> {
        let payload: RouteEntryPayload = serde_json::from_value(task.payload.clone())?;

        // EVERY fallible step BEFORE the ExecutionStarted journal write is
        // a fire-loss point: the fire is not yet journal state, and a
        // failed dispatcher task is terminal (never retried), so a bare
        // error here would silently drop the fire (and a wait-mode drain
        // could complete over the loss). So the whole pre-journal section
        // runs in a fallible block and, on ANY error (transient read,
        // project not Active, missing hash), the fire is RE-PARKED via
        // the shared `park_fire` helper instead of lost. The fire's
        // identity is `payload.fire_id` (stable across re-enqueued tasks),
        // so a retry collapses to one queued element. `is_resume` is read
        // from the signal row itself inside `append_parked_fire`, so even
        // a `signal_get`/`lifecycle` read error can still park (the token
        // alone suffices).
        let pre = pre_journal_route(state, &payload).await;
        let (signal, color, candidate_hash) = match pre {
            Ok(v) => v,
            Err(e) => return park_fire(state, task, &payload, &e.to_string()).await,
        };
        let now = crate::lease::now_unix() as u64;
        state
            .journal
            .record_event_dedup(
                &weft_journal::ExecEvent::ExecutionStarted {
                    color,
                    project_id: signal.project_id.clone(),
                    entry_node: signal.node_id.clone(),
                    phase: weft_core::context::Phase::Fire,
                    definition_hash: candidate_hash.clone(),
                    at_unix: now,
                },
                &format!("route_entry:{}:start", payload.fire_id),
            )
            .await?;
        // Everything below runs AFTER ExecutionStarted has committed, so
        // the color now EXISTS and `running_count` counts it. A bubbled
        // error here would leave a color that is journaled-started but
        // never kicked / never enqueued: it never runs, never terminates,
        // and blocks every wait-mode deactivate drain forever (a "ghost"
        // running execution). So run the post-start work in a fallible
        // block and, on ANY error, journal a terminal `ExecutionFailed`
        // (which releases the color from `running_count`) before
        // propagating. A task retry replays the same dedup-keyed events;
        // ExecutionFailed is keyed too so the terminal is single-write.
        let outcome = route_after_started(state, &payload, &signal, color, now).await;
        if let Err(e) = &outcome {
            // Journal the terminal ExecutionFailed so the started-but-
            // never-run color does not haunt `running_count`. SKIP if a
            // terminal already exists for the color: a cancel arriving during
            // the route window writes `ExecutionCancelled` via the guarded
            // writer, and stacking `ExecutionFailed` on top would be a second,
            // contradictory terminal (the dedup key only collapses a duplicate
            // ExecutionFailed, not a cancel). The keyed write stays as the
            // single-write guard for the retry case. If THIS write fails (one
            // transient error deep), the color is genuinely stranded: surface
            // it loud with the recovery verb, then propagate.
            let already_terminal = crate::api::execution::has_terminal_event(&state.pg_pool, color)
                .await
                .unwrap_or(false);
            if already_terminal {
                tracing::info!(
                    target: "weft_dispatcher::route_entry",
                    %color,
                    "route_entry post-start failed but a terminal already exists; \
                     skipping ExecutionFailed (no contradictory second terminal)"
                );
            } else if let Err(je) = state
                .journal
                .record_event_dedup(
                    &weft_journal::ExecEvent::ExecutionFailed {
                        color,
                        error: format!("route_entry: {e}"),
                        at_unix: now,
                    },
                    &format!("route_entry:{}:failed", payload.fire_id),
                )
                .await
            {
                tracing::error!(
                    target: "weft_dispatcher::route_entry",
                    %color,
                    route_error = %e,
                    journal_error = %je,
                    "route_entry post-start failed AND the terminal ExecutionFailed write \
                     failed; color {color} is stranded as a running execution and will block \
                     wait-mode drains. Recovery: `weft stop {color}`"
                );
                return Err(je.context(format!("route_entry post-start error: {e}")));
            }
        }
        outcome?;

        // Invariant: a parked element for this fire id must not coexist
        // with a started color v5(fire_id). A fire can be parked (a prior
        // attempt hit a non-Active window) and then succeed on a later
        // attempt; without removing the parked element, the next activate
        // would drain it and re-run the (now journaled) fire. Remove it
        // by id via the shared helper (unfenced: this is the success
        // path, not a drain claim). By-id removal commutes with a
        // concurrent drain pop, so neither deletes the wrong element.
        if let Err(e) =
            crate::api::signal::remove_parked_fire(&state.pg_pool, &payload.token, &payload.fire_id, None)
                .await
        {
            tracing::warn!(
                target: "weft_dispatcher::route_entry",
                token = %payload.token,
                fire_id = %payload.fire_id,
                error = %e,
                "routed fire, but could not remove its parked element (if one existed); a stale \
                 re-run could occur on the next activate (the fire-id-keyed journal events make \
                 it a harmless no-op replay)"
            );
        }

        // SSE for ExecutionStarted is emitted by the journal bridge
        // on its next poll. We don't publish inline because a retry
        // of this task would double-emit; the bridge keys off the
        // event log itself, which the dedup key keeps single-write.

        Ok(serde_json::to_value(RouteEntryResult::Routed {
            color: color.to_string(),
        })?)
    }
}

/// The pre-`ExecutionStarted` half of route_entry: resolve the signal,
/// re-check the lifecycle gate (a sibling Pod may have finished a
/// deactivation since the HTTP gate), and snapshot the definition hash.
/// Returns `(signal, color, candidate_hash)` on the happy path. ANY
/// error (transient read, project not Active, missing hash) is returned
/// to the caller, which RE-PARKS the fire rather than losing it. The
/// definition_hash is snapshotted onto ExecutionStarted so a resume of
/// this color reads THIS hash from the journal, not the project row's
/// current hash (which may change if the user re-registers mid-flight).
async fn pre_journal_route(
    state: &DispatcherState,
    payload: &RouteEntryPayload,
) -> Result<(crate::journal::SignalRegistration, Uuid, String)> {
    let signal = state
        .journal
        .signal_get(&payload.token)
        .await?
        .ok_or_else(|| anyhow::anyhow!("signal {} not found", payload.token))?;
    let project_uuid: Uuid = signal.project_id.parse()?;

    let lifecycle = state
        .projects
        .lifecycle(project_uuid)
        .await?
        .ok_or_else(|| anyhow::anyhow!("project {} not found; cannot route fire", signal.project_id))?;
    if lifecycle.status != crate::project_store::ProjectStatus::Active {
        anyhow::bail!(
            "project {} is {} (not Active) at route time",
            signal.project_id,
            lifecycle.status
        );
    }

    // Derive color from the FIRE id (not the task id): a fire re-parked
    // then re-dispatched runs under a new task but must converge on ONE
    // color, else a park / drain / lease-rescue interleaving spawns two
    // executions for one fire. Events are dedup-keyed, so a retry of any
    // task carrying this fire id replays the same ExecutionStarted /
    // NodeKicked rows.
    let fire_uuid: Uuid = payload
        .fire_id
        .parse()
        .map_err(|e| anyhow::anyhow!("route_entry: invalid fire_id {}: {e}", payload.fire_id))?;
    let color = Uuid::new_v5(&COLOR_NAMESPACE, fire_uuid.as_bytes());
    let candidate_hash = state
        .projects
        .running_definition_hash(project_uuid)
        .await?
        .ok_or_else(|| anyhow::anyhow!("project {} has no definition_hash", signal.project_id))?;
    Ok((signal, color, candidate_hash))
}

/// Re-park a fire whose pre-journal routing failed (or which arrived at
/// a non-Active project), so it survives to the next activate instead of
/// being lost when the task goes terminal. Idempotent on retry (task id
/// is the fire identity). Then re-drive the drain CAS, since this task
/// may have been the last in-flight item keeping `running_count` above
/// zero and nothing was journaled to re-trigger the watcher. A drain-
/// recheck error must NOT fail the task (the fire is already safely
/// parked); log it and let a later poll re-drive.
async fn park_fire(
    state: &DispatcherState,
    task: &Task,
    payload: &RouteEntryPayload,
    reason: &str,
) -> Result<Value> {
    // ParkedFire.id is the stable FIRE id (not the task id): a re-parked
    // fire is later drained under a NEW task but keeps this id, so the
    // re-dispatch dedups against any in-flight task carrying the same
    // fire id, and a park / drain / lease-rescue interleaving converges
    // on one execution.
    let entry = crate::api::signal::ParkedFire {
        id: payload.fire_id.clone(),
        payload: payload.payload.clone(),
        received_at_unix: crate::lease::now_unix(),
    };
    let appended =
        crate::api::signal::append_parked_fire(&state.pg_pool, &payload.token, &entry).await?;
    if appended == 0 {
        // Either the signal row is gone (project wiped: the fire drops,
        // matching the gate's refusal) or this fire id is already queued
        // (idempotent retry). Don't claim "re-parked" in the drop case.
        tracing::warn!(
            target: "weft_dispatcher::route_entry",
            token = %payload.token,
            fire_id = %payload.fire_id,
            reason = %reason,
            "re-park matched 0 rows: signal row gone (fire dropped) or fire already queued"
        );
    } else {
        tracing::info!(
            target: "weft_dispatcher::route_entry",
            token = %payload.token,
            fire_id = %payload.fire_id,
            reason = %reason,
            "fire re-parked (not routed)"
        );
    }
    // Re-drive the drain CAS unconditionally: this task may have been the
    // last in-flight item keeping `running_count` above zero, and nothing
    // was journaled to re-trigger the watcher. The project_id is on the
    // TASK row (always stamped for route_entry tasks), so no extra read.
    // The CAS is event-driven (no background poll re-drives it), so this
    // call is the only thing that finishes a drain whose last item was a
    // re-parked fire. A failure here must NOT fail the task (the fire is
    // already safely parked); log it loud.
    if let Some(project_id) = task.project_id.as_deref() {
        if let Err(e) =
            crate::journal_bridge::try_finish_drain(state, project_id, Some(task.id)).await
        {
            tracing::error!(
                target: "weft_dispatcher::route_entry",
                project_id = %project_id,
                error = %e,
                "fire re-parked, but the post-park drain re-check failed; the project may linger \
                 in deactivating until another terminal event re-drives the CAS"
            );
        }
        // If the project is currently ACTIVE (the re-park was a transient
        // read error, not a deactivation), nothing else will drain this
        // fire until the next activate, which may be far off. Drive the
        // drain for this token now so the fire re-dispatches promptly.
        // This only ENQUEUES a fresh route_entry task (dedup-keyed on the
        // same fire id, so it can't double-run); it does not re-enter this
        // executor. Best-effort: a failure leaves the fire parked for the
        // next activate.
        if appended > 0 {
            let active = match project_id.parse::<Uuid>() {
                Ok(uuid) => matches!(
                    state.projects.lifecycle(uuid).await,
                    Ok(Some(lc)) if lc.status == crate::project_store::ProjectStatus::Active
                ),
                Err(_) => false,
            };
            if active {
                // This task STILL holds the `entry:{token}:{fire_id}`
                // dedup slot. If we re-drained now, the drain's
                // enqueue_dedup would collapse onto US (the live task that
                // just decided NOT to route this fire), and the drain
                // would then pop the parked element believing it was
                // dispatched: the fire would be silently deleted. So
                // release our dedup slot FIRST, so the drain inserts a
                // fresh task carrying this same fire id (which converges
                // on one execution via the fire-id-keyed journal events).
                if let Err(e) = sqlx::query("UPDATE task SET dedup_key = NULL WHERE id = $1")
                    .bind(task.id)
                    .execute(&state.pg_pool)
                    .await
                {
                    tracing::warn!(
                        target: "weft_dispatcher::route_entry",
                        error = %e,
                        "could not release dedup slot before re-drain; fire stays parked for \
                         the next activate"
                    );
                } else if let Err(e) =
                    crate::api::project::drain_one_token(state, project_id, &payload.token).await
                {
                    tracing::warn!(
                        target: "weft_dispatcher::route_entry",
                        project_id = %project_id,
                        error = %e,
                        "re-parked fire: immediate re-drain on the active project failed; \
                         it will drain on the next activate"
                    );
                }
            }
        }
    }
    Ok(serde_json::to_value(RouteEntryResult::Reparked)?)
}

/// The post-`ExecutionStarted` half of route_entry: read back the
/// committed hash, fetch the definition, compute + journal kicks, and
/// enqueue the execute task. Split out so the caller can journal a
/// terminal `ExecutionFailed` on ANY error here (the color is already
/// started, so a bare error would strand it as a ghost running
/// execution).
async fn route_after_started(
    state: &DispatcherState,
    payload: &RouteEntryPayload,
    signal: &crate::journal::SignalRegistration,
    color: Uuid,
    now: u64,
) -> Result<()> {
    let project_uuid: Uuid = signal.project_id.parse()?;
    // The journal is the single source of truth for this color's hash: a
    // RETRY of this task re-reads the project row, which may have
    // advanced (user re-registered). The dedup write kept attempt 1's
    // row, so read the committed value back and derive EVERYTHING
    // downstream (kick set, execute payload) from it, so every attempt
    // converges on one shape.
    let definition_hash = match state.journal.execution_definition_hash(color).await? {
        crate::journal::ColorLookup::Found(h) => h,
        crate::journal::ColorLookup::NotFound => anyhow::bail!(
            "color {color} has no ExecutionStarted after the dedup write; journal contract broken"
        ),
        crate::journal::ColorLookup::Corrupt => anyhow::bail!(
            "journal row for color {color} is corrupt; see dispatcher logs"
        ),
    };
    let project_json = state
        .projects
        .definition_for_hash(project_uuid, &definition_hash)
        .await?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "project {} has no recorded definition for hash {definition_hash}; \
                 the definition history must cover every journaled hash",
                signal.project_id
            )
        })?;
    let project_def: weft_core::ProjectDefinition = serde_json::from_str(&project_json)?;

    let kicks = crate::api::project::compute_trigger_kicks(
        &project_def,
        &signal.node_id,
        &payload.payload,
        signal.port_snapshot.as_ref(),
    );
    if kicks.is_empty() {
        anyhow::bail!(
            "trigger '{}' has no output downstream; nothing to run",
            signal.node_id
        );
    }
    for kick in &kicks {
        state
            .journal
            .record_event_dedup(
                &weft_journal::ExecEvent::NodeKicked {
                    color,
                    node_id: kick.node_id.clone(),
                    firing: kick.firing,
                    payload: kick.payload.clone(),
                    port_snapshot: kick.port_snapshot.clone(),
                    at_unix: now,
                },
                &format!("route_entry:{}:kick:{}", payload.fire_id, kick.node_id),
            )
            .await?;
    }

    // Enqueue an `execute` task targeted at the worker pool. The
    // cold-start trigger spawns a Pod for this project if none is alive;
    // the worker's claim loop folds the journal and runs. Same hash on
    // the task payload as on ExecutionStarted.
    crate::task_kinds::execute::enqueue_execute(
        &state.pg_pool,
        &signal.project_id,
        color,
        &definition_hash,
        Some(&payload.tenant_id),
    )
    .await?;

    // Entry triggers are persistent: registered once at TriggerSetup,
    // fire many times until deactivate. The signal row stays. Single-use
    // resume signals are deleted in the resume path, not here.
    Ok(())
}
