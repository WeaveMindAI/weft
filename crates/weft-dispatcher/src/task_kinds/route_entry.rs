//! `route_entry` task: a dispatcher Pod loads the project,
//! computes trigger kicks, journals ExecutionStarted + NodeKicked
//! events, and enqueues an execute task. Used by the listener
//! when an entry-trigger fire arrives.
//!
//! Idempotency rests on a STABLE per-fire id (`RouteEntryPayload.
//! fire_id`), minted once at the live-fire enqueue or reused from the
//! ParkedFire id on a drain pop. The RouteEntry task dedup key is
//! `entry:{token}:{fire_id}` and the execution color is `v5(fire_id)`.
//! Birth, starting inputs, and worker admission commit together. A rescued
//! routing task finds the existing run and never reads mutable settings to
//! rebuild it. Before birth, a failure parks the event for later routing.

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
    /// Routes of this fire that already failed and re-parked it. Zero
    /// for a live fire; a drain copies the parked element's count in.
    /// The next re-park stamps `attempts + 1` and the matching backoff.
    #[serde(default)]
    pub attempts: u32,
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
    /// The signal row is gone (the project was wiped under the fire), so
    /// there is nowhere to park it: the fire is dropped, matching the
    /// gate's refusal of a fire for a project that no longer exists.
    Dropped { reason: String },
    /// A re-run found the color already terminal (cancelled during the
    /// route window, or run to its end by an earlier attempt): nothing
    /// left to finish.
    AlreadySettled { color: String },
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
        // The color is a pure function of the fire id, so it is known
        // before any read. Ask the journal FIRST whether this color was
        // already born: a start write whose commit landed but whose ack
        // was lost re-parked the fire, and this task is its drained twin;
        // or a Pod died between the start write and the kicks and this
        // is the lease rescue. In both cases the fire IS journal state
        // and the only correct thing is to finish it (kicks, execute
        // task), whatever the project's lifecycle says now: a born color
        // counts in `running_count`, and a wait-mode deactivate waits on
        // it, so re-parking it would leave a ghost that blocks the drain
        // forever. Everything after the start write already runs under
        // that rule (an error journals a terminal); this makes a re-run
        // join it instead of re-entering the pre-journal gate.
        let color = color_for_fire(&payload)?;
        let born = match state.journal.execution_definition_hash(color).await {
            Ok(crate::journal::ColorLookup::Found(hash)) => Some(hash),
            Ok(crate::journal::ColorLookup::NotFound) => None,
            Ok(crate::journal::ColorLookup::Corrupt) => anyhow::bail!(
                "journal row for color {color} is corrupt; see dispatcher logs"
            ),
            Err(e) => return park_fire(state, task, &payload, &format!("journal read: {e}")).await,
        };
        if born.is_some() {
            forget_parked_twin(state, &payload).await;
            refinish_drain(state, task).await;
            let terminal = state.journal.events_log(color).await?.iter().any(|event| event.is_execution_terminal());
            return Ok(serde_json::to_value(if terminal {
                RouteEntryResult::AlreadySettled { color: color.to_string() }
            } else { RouteEntryResult::Routed { color: color.to_string() } })?);
        }
        {
            let routed = match pre_journal_route(state, &payload).await {
                Ok(v) => v,
                Err(e) => return park_fire(state, task, &payload, &e.to_string()).await,
            };
            let RoutedFire { signal, program, fire } = routed;
            let candidate_hash = program.definition_hash.clone();
            // A trigger that reaches no output has nothing to run: not a
            // failure, not a park (a park would drain it back into this
            // same no-op on every activate), just nothing. Clear any parked
            // twin of this fire, re-drive the drain CAS (this task may have
            // been the last in-flight item holding `running_count` up, and
            // nothing was journaled to re-trigger the watcher), and answer.
            let now = crate::lease::now_unix() as u64;
            // The fire's computed subgraph rides on ExecutionStarted: the
            // engine holds the run (resumes included) to this set. Without
            // it a node shared with another program in the same file pushes
            // pulses into that program's consumers, which park forever and
            // end the run Stuck.
            let Some(source_version) = signal.source_version.as_deref() else {
                return park_fire(state, task, &payload, &format!("trigger '{}' has no original source version; activate it again", signal.node_id)).await;
            };
            let (start, kick_events) = crate::api::project::execution_birth_events(
                color,
                &signal.project_id,
                weft_core::context::Phase::Fire,
                &signal.node_id,
                &fire.kicks,
                &candidate_hash,
                Some(&program),
                Some(&fire.subgraph),
                None,
                Some(source_version),
                now,
            );
            // The start write is the LAST fire-loss point: until it commits
            // the fire is not journal state, so a transient error here
            // re-parks like every step before it. A write that committed
            // but failed to acknowledge re-parks too, and the drained twin
            // finds the color born (above) and finishes it.
            let execution_task = crate::task_kinds::execute::execution_task_spec(
                weft_task_store::TaskKind::Execute, &signal.project_id, color,
                &candidate_hash, &program.binary_hash, Some(&payload.tenant_id), None, None,
            )?;
            if let Err(e) = state
                .journal
                .start_execution(&start, &kick_events, execution_task, None)
                .await
            {
                return park_fire(state, task, &payload, &format!("ExecutionStarted write: {e}")).await;
            }
        };

        forget_parked_twin(state, &payload).await;

        // SSE for ExecutionStarted is emitted by the journal bridge
        // on its next poll. We don't publish inline because a retry
        // of this task would double-emit; the bridge keys off the
        // event log itself, which the dedup key keeps single-write.

        Ok(serde_json::to_value(RouteEntryResult::Routed {
            color: color.to_string(),
        })?)
    }
}

/// Re-drive the drain CAS for this task's project, excluding the task
/// itself: it journals nothing and is about to complete, but its row is
/// still `claimed` at check time. Called by every route_entry outcome
/// that ends WITHOUT a journal event (a re-park, a nothing-to-run), because
/// such a task may have been the last in-flight item keeping
/// `running_count` above zero and nothing else re-triggers the watcher
/// promptly (the reaper's periodic sweep would, but only after its
/// interval). A failure here must NOT fail the task (the fire's fate is
/// already settled); log it loud.
async fn refinish_drain(state: &DispatcherState, task: &Task) {
    let Some(project_id) = task.project_id.as_deref() else { return };
    if let Err(e) = crate::journal_bridge::try_finish_drain(state, project_id, Some(task.id)).await {
        tracing::error!(
            target: "weft_dispatcher::route_entry",
            project_id = %project_id,
            error = %e,
            "route_entry ended without a journal event, and the drain re-check failed; the \
             project may linger in deactivating until the reaper's next sweep re-drives the CAS"
        );
    }
}

/// Remove the parked element for this fire id, if one exists.
///
/// Invariant: a parked element for a fire id must not coexist with a
/// routed outcome for it. A fire can be parked (a prior attempt hit a
/// non-Active window) and then succeed on a later attempt; without
/// removing the parked element, the next activate would drain it and
/// re-run the (now journaled, or now known to be a no-op) fire. Removal
/// is by id via the shared helper (unfenced: this is the success path,
/// not a drain claim), and by-id removal commutes with a concurrent
/// drain pop, so neither deletes the wrong element. A failure here is
/// logged, not raised: the fire is already routed, and the fire-id-keyed
/// journal events make a stale re-run a harmless no-op replay.
async fn forget_parked_twin(state: &DispatcherState, payload: &RouteEntryPayload) {
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
             re-run could occur on the next activate"
        );
    }
}

/// The pre-`ExecutionStarted` half of route_entry: resolve the signal,
/// re-check the lifecycle gate (a sibling Pod may have finished a
/// deactivation since the HTTP gate), and snapshot the definition hash.
/// Returns the [`RoutedFire`] on the happy path, its `fire` computed
/// from the candidate hash's definition (`None` when the trigger reaches
/// no output). ANY error (transient read, project not Active, missing
/// hash or definition) is returned to the caller, which RE-PARKS the
/// fire rather than losing it: a missing definition is a contract
/// violation, but parking keeps the fire alive for the operator to
/// route once the history is repaired, where a bare failure would drop
/// it. The definition_hash is snapshotted onto
/// ExecutionStarted so a resume of this color reads THIS hash from the
/// journal, not the project row's current hash (which may change if the
/// user re-registers mid-flight); the fire is computed here, before the
/// journal write, because ExecutionStarted carries its subgraph.
async fn pre_journal_route(
    state: &DispatcherState,
    payload: &RouteEntryPayload,
) -> Result<RoutedFire> {
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

    let program = signal.program.clone()
        .ok_or_else(|| anyhow::anyhow!("trigger '{}' has no armed code identity; activate it again", signal.node_id))?;
    let project_def = definition_for(state, project_uuid, &signal.project_id, &program.definition_hash).await?;
    let fire = crate::api::project::compute_trigger_fire(
        &project_def,
        &signal.node_id,
        &payload.payload,
        signal.port_snapshot.as_ref(),
    ).map_err(anyhow::Error::msg)?;
    Ok(RoutedFire { signal, program, fire })
}

/// The routing decision for one fire, everything the journal write and
/// the post-start work need: the signal that fired, the definition hash
/// the fire was computed against, and its executable selection.
struct RoutedFire {
    signal: crate::journal::SignalRegistration,
    program: weft_core::project::hash::ProgramIdentity,
    fire: crate::api::project::TriggerFire,
}

/// The execution color for a fire: `v5(fire_id)`, derived from the FIRE
/// id (not the task id), so a fire re-parked then re-dispatched under a
/// new task, or the same task re-run after a lease rescue, converges on
/// ONE color. Every journal event for the color is dedup-keyed on the
/// same fire id, so any number of tasks carrying it replay the same rows.
fn color_for_fire(payload: &RouteEntryPayload) -> Result<Uuid> {
    let fire_uuid: Uuid = payload
        .fire_id
        .parse()
        .map_err(|e| anyhow::anyhow!("route_entry: invalid fire_id {}: {e}", payload.fire_id))?;
    Ok(Uuid::new_v5(&COLOR_NAMESPACE, fire_uuid.as_bytes()))
}

/// The project definition recorded for `hash`, parsed. The definition
/// history must cover every hash a fire or a journal row can name, so
/// a miss is a contract violation; what the caller does with it (park
/// before the journal write, fail the color after) is the caller's.
async fn definition_for(
    state: &DispatcherState,
    project_uuid: Uuid,
    project_id: &str,
    hash: &str,
) -> Result<weft_core::ProjectDefinition> {
    let project_json = state
        .projects
        .definition_for_hash(project_uuid, hash)
        .await?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "project {project_id} has no recorded definition for hash {hash}; \
                 the definition history must cover every journaled hash"
            )
        })?;
    Ok(serde_json::from_str(&project_json)?)
}

/// Re-park a fire whose pre-journal routing failed (or which arrived at
/// a non-Active project), so it survives instead of being lost when the
/// task goes terminal: the reaper's parked-fire sweep retries it after
/// its backoff, or the next activate drains it. Idempotent on retry (task id
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
    let attempts = payload.attempts + 1;
    let now = crate::lease::now_unix();
    let entry = crate::api::signal::ParkedFire {
        id: payload.fire_id.clone(),
        payload: payload.payload.clone(),
        received_at_unix: now,
        attempts,
        not_before_unix: now + crate::api::signal::park_backoff_secs(attempts),
    };
    use crate::api::signal::{ParkAppend, ParkRefusal};
    match crate::api::signal::append_parked_fire(&state.pg_pool, &payload.token, &entry).await? {
        ParkAppend::Parked => {
            tracing::info!(
                target: "weft_dispatcher::route_entry",
                token = %payload.token,
                fire_id = %payload.fire_id,
                reason = %reason,
                "fire re-parked (not routed)"
            );
        }
        // A lease-rescue re-run of a task that parked this fire before its
        // Pod died: the element is there with its backoff stamp, nothing
        // is lost, and the reaper's parked-fire sweep retries it when due.
        ParkAppend::Refused(ParkRefusal::AlreadyQueued) => {
            tracing::info!(
                target: "weft_dispatcher::route_entry",
                token = %payload.token,
                fire_id = %payload.fire_id,
                reason = %reason,
                "fire already parked under this id (re-run); it retries when its backoff is due"
            );
        }
        // The project was wiped under the fire: nowhere to park it, so it
        // drops, matching the gate's refusal of a fire for a project that
        // no longer exists.
        ParkAppend::Refused(ParkRefusal::RowGone) => {
            tracing::warn!(
                target: "weft_dispatcher::route_entry",
                token = %payload.token,
                fire_id = %payload.fire_id,
                reason = %reason,
                "signal row gone; fire dropped"
            );
            refinish_drain(state, task).await;
            return Ok(serde_json::to_value(RouteEntryResult::Dropped {
                reason: format!("signal row gone: {reason}"),
            })?);
        }
        // A NEW fire refused by the cap can neither be routed nor kept:
        // that is a loss, and it fails the task out loud rather than
        // returning a "re-parked" that is not true.
        ParkAppend::Refused(ParkRefusal::QueueFull) => {
            refinish_drain(state, task).await;
            anyhow::bail!(
                "fire {} for signal {} could not be routed ({reason}) and could not be parked: \
                 the entry's parked queue is full; the fire is lost. Activate the project to \
                 drain the queue.",
                payload.fire_id,
                payload.token
            );
        }
        ParkAppend::Refused(ParkRefusal::ResumeAlreadyAnswered) => {
            refinish_drain(state, task).await;
            anyhow::bail!(
                "fire {} routed through route_entry targets resume signal {}; entry fires only; \
                 dispatcher contract broken",
                payload.fire_id,
                payload.token
            );
        }
    }
    refinish_drain(state, task).await;
    // No immediate re-drain: the element carries its backoff stamp, and
    // the reaper's parked-fire sweep (`drain_due_parked_fires`) re-drives
    // the token once it is due. A persistent failure therefore retries
    // every few minutes at most, instead of park / drain / enqueue / fail
    // spinning against Postgres and the logs. This task's dedup slot
    // frees when it returns, so the sweep's enqueue lands on a fresh task.
    Ok(serde_json::to_value(RouteEntryResult::Reparked)?)
}
