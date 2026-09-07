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
//! the same fire (a live task and its re-parked-then-drained twin, or
//! the same task re-run after a lease rescue because the Pod that
//! claimed it died mid-way) converge on ONE color whose events are
//! single-write, never forking the execution. A task that RETURNS an
//! error is terminal and never re-run; a task whose claim lapses is.
//! So a pre-journal failure re-parks the fire (it is not lost) and a
//! post-journal failure journals a terminal ExecutionFailed (so the
//! color does not haunt running_count).

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
/// `signal.parked_fires` instead of becoming journal state;
/// `NothingToRun` means the fired trigger reaches no output node, so
/// there is no execution to start (a no-op, never an error: the
/// trigger is wired, it just has nothing downstream that asks for a
/// result).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum RouteEntryResult {
    Routed { color: String },
    Reparked,
    NothingToRun { node_id: String },
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
        let (signal, candidate_hash, kick_events) = if let Some(committed_hash) = born {
            // Already started. `None` signal: route_after_started reads
            // it itself, so a failure there is a post-start failure
            // (journals a terminal below), never a re-park of a born
            // color. `None` kicks: rebuilt from the committed definition,
            // the same one attempt 1 used.
            (None, committed_hash, None)
        } else {
            let routed = match pre_journal_route(state, &payload).await {
                Ok(v) => v,
                Err(e) => return park_fire(state, task, &payload, &e.to_string()).await,
            };
            let RoutedFire { signal, candidate_hash, fire } = routed;
            // A trigger that reaches no output has nothing to run: not a
            // failure, not a park (a park would drain it back into this
            // same no-op on every activate), just nothing. Clear any parked
            // twin of this fire, re-drive the drain CAS (this task may have
            // been the last in-flight item holding `running_count` up, and
            // nothing was journaled to re-trigger the watcher), and answer.
            let Some(fire) = fire else {
                tracing::info!(
                    target: "weft_dispatcher::route_entry",
                    node_id = %signal.node_id,
                    fire_id = %payload.fire_id,
                    "trigger fired but reaches no output node; nothing to run"
                );
                forget_parked_twin(state, &payload).await;
                refinish_drain(state, task).await;
                return Ok(serde_json::to_value(RouteEntryResult::NothingToRun {
                    node_id: signal.node_id.clone(),
                })?);
            };
            let now = crate::lease::now_unix() as u64;
            // The fire's computed subgraph rides on ExecutionStarted: the
            // engine holds the run (resumes included) to this set. Without
            // it a node shared with another program in the same file pushes
            // pulses into that program's consumers, which park forever and
            // end the run Stuck.
            let (start, kick_events) = crate::api::project::execution_birth_events(
                color,
                &signal.project_id,
                weft_core::context::Phase::Fire,
                &signal.node_id,
                &fire.kicks,
                &candidate_hash,
                Some(&fire.subgraph),
                now,
            );
            // The start write is the LAST fire-loss point: until it commits
            // the fire is not journal state, so a transient error here
            // re-parks like every step before it. A write that committed
            // but failed to acknowledge re-parks too, and the drained twin
            // finds the color born (above) and finishes it.
            if let Err(e) = state
                .journal
                .record_event_dedup(&start, &format!("route_entry:{}:start", payload.fire_id))
                .await
            {
                return park_fire(state, task, &payload, &format!("ExecutionStarted write: {e}")).await;
            }
            (Some(signal), candidate_hash, Some(kick_events))
        };
        let now = crate::lease::now_unix() as u64;
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
        let outcome =
            route_after_started(state, &payload, signal, color, now, &candidate_hash, kick_events).await;
        // A born color whose journal already held a terminal (cancelled
        // in the route window, or a re-run after it ran to its end): the
        // check lives inside the guarded block so a transient failure of
        // the check itself ends as a journaled terminal, never as a born
        // color with no kicks.
        if let Ok(AfterStart::AlreadySettled) = &outcome {
            tracing::info!(
                target: "weft_dispatcher::route_entry",
                %color,
                fire_id = %payload.fire_id,
                "color already has a terminal; nothing to finish"
            );
            forget_parked_twin(state, &payload).await;
            refinish_drain(state, task).await;
            return Ok(serde_json::to_value(RouteEntryResult::AlreadySettled {
                color: color.to_string(),
            })?);
        }
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

    let candidate_hash = state
        .projects
        .running_definition_hash(project_uuid)
        .await?
        .ok_or_else(|| anyhow::anyhow!("project {} has no definition_hash", signal.project_id))?;
    let project_def = definition_for(state, project_uuid, &signal.project_id, &candidate_hash).await?;
    let fire = crate::api::project::compute_trigger_fire(
        &project_def,
        &signal.node_id,
        &payload.payload,
        signal.port_snapshot.as_ref(),
    );
    Ok(RoutedFire { signal, candidate_hash, fire })
}

/// The routing decision for one fire, everything the journal write and
/// the post-start work need: the signal that fired, the definition hash
/// the fire was computed against, and the fire itself (`None`: the
/// trigger reaches no output).
struct RoutedFire {
    signal: crate::journal::SignalRegistration,
    candidate_hash: String,
    fire: Option<crate::api::project::TriggerFire>,
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

/// The post-`ExecutionStarted` half of route_entry: read back the
/// committed hash, fetch the definition, compute + journal kicks, and
/// enqueue the execute task. Split out so the caller can journal a
/// terminal `ExecutionFailed` on ANY error here (the color is already
/// started, so a bare error would strand it as a ghost running
/// execution).
async fn route_after_started(
    state: &DispatcherState,
    payload: &RouteEntryPayload,
    // `None` on a re-run of a born color: read here, so that a missing
    // or unreadable signal is a post-start failure (a terminal for the
    // color), not a fire-loss point.
    signal: Option<crate::journal::SignalRegistration>,
    color: Uuid,
    now: u64,
    candidate_hash: &str,
    kick_events: Option<Vec<weft_journal::ExecEvent>>,
) -> Result<AfterStart> {
    // A terminal already on the color: cancelled during the route window,
    // or a re-run of a task whose color ran to its end (the execute dedup
    // key frees once the first task completes). Kicking and enqueuing it
    // would run a finished execution again; the failure path in
    // `execute` guards on the same fact. The window between this check
    // and the enqueue is closed on the worker side: `run_one_execution`
    // refuses to drive a color whose journal already holds a terminal.
    if crate::api::execution::has_terminal_event(&state.pg_pool, color).await? {
        return Ok(AfterStart::AlreadySettled);
    }
    let signal = match signal {
        Some(s) => s,
        None => state
            .journal
            .signal_get(&payload.token)
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "signal {} not found for born color {color}; journal contract broken",
                    payload.token
                )
            })?,
    };
    let project_uuid: Uuid = signal.project_id.parse()?;
    // The journal is the single source of truth for this color's hash: a
    // re-run of this task (a lease rescue) re-reads the project row,
    // which may have advanced (user re-registered). The dedup write kept
    // attempt 1's row, so read the committed value back and derive
    // EVERYTHING downstream (kick set, execute payload) from it, so every
    // attempt converges on one shape. On the first attempt the committed
    // hash IS the candidate and the kicks built before the journal write
    // are used as they are; a re-run of a born color (no kicks in hand),
    // or one that saw a moved hash, rebuilds from the committed one.
    let definition_hash = match state.journal.execution_definition_hash(color).await? {
        crate::journal::ColorLookup::Found(h) => h,
        crate::journal::ColorLookup::NotFound => anyhow::bail!(
            "color {color} has no ExecutionStarted after the dedup write; journal contract broken"
        ),
        crate::journal::ColorLookup::Corrupt => anyhow::bail!(
            "journal row for color {color} is corrupt; see dispatcher logs"
        ),
    };
    let kick_events = match kick_events {
        Some(events) if definition_hash == candidate_hash => events,
        _ => {
            let project_def =
                definition_for(state, project_uuid, &signal.project_id, &definition_hash).await?;
            // The committed definition is the one attempt 1 computed a fire
            // from, so it reaches an output by construction; `None` here
            // means the journal and the definition history disagree.
            let fire = crate::api::project::compute_trigger_fire(
                &project_def,
                &signal.node_id,
                &payload.payload,
                signal.port_snapshot.as_ref(),
            )
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "trigger '{}' reaches no output under the committed definition {definition_hash}, \
                     yet an ExecutionStarted was journaled for it; journal contract broken",
                    signal.node_id
                )
            })?;
            crate::api::project::execution_birth_events(
                color,
                &signal.project_id,
                weft_core::context::Phase::Fire,
                &signal.node_id,
                &fire.kicks,
                &definition_hash,
                Some(&fire.subgraph),
                now,
            )
            .1
        }
    };
    for kick in &kick_events {
        let weft_journal::ExecEvent::NodeKicked { node_id, .. } = kick else {
            unreachable!("execution_birth_events yields NodeKicked events only");
        };
        state
            .journal
            .record_event_dedup(kick, &format!("route_entry:{}:kick:{node_id}", payload.fire_id))
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
    Ok(AfterStart::Routed)
}

/// What the post-start half found: the color was kicked and its execute
/// task enqueued, or the color was already terminal and nothing was done.
enum AfterStart {
    Routed,
    AlreadySettled,
}
