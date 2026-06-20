//! Bridge between the journal's `exec_event` table and the
//! dispatcher's `EventBus` SSE fanout.
//!
//! Polls `exec_event` on a tick, picks up newly-inserted rows,
//! converts each `ExecEvent` into the matching `DispatcherEvent`,
//! publishes to `EventBus` so SSE consumers (CLI follow, VS Code
//! execution view) see the live event.
//!
//! Why a converter instead of broadcasting `ExecEvent` directly:
//! the SSE wire format is `DispatcherEvent` and several consumers
//! depend on it. The journal is the durable shape; DispatcherEvent
//! is the user-facing shape. They're allowed to diverge.

use std::time::Duration;

use sqlx::Row;

use weft_journal::ExecEvent;

use crate::events::DispatcherEvent;
use crate::state::DispatcherState;

const POLL_INTERVAL: Duration = Duration::from_millis(100);
/// Single-row table key. We only ever have one cursor for the
/// whole dispatcher fleet; rows are keyed by this constant so any
/// Pod's UPDATE targets the same row.
const CURSOR_KEY: &str = "journal_bridge";

/// Persistent cursor table. One row per cursor key. The bridge
/// reads `last_id` on boot and writes it after every successful
/// drain so a Pod restart resumes where the cluster left off.
pub async fn migrate(pool: &sqlx::PgPool) -> anyhow::Result<()> {
    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS dispatcher_cursor (
            key TEXT PRIMARY KEY,
            last_id BIGINT NOT NULL
        )"#,
    )
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO dispatcher_cursor (key, last_id) VALUES ($1, 0) \
         ON CONFLICT (key) DO NOTHING",
    )
    .bind(CURSOR_KEY)
    .execute(pool)
    .await?;
    Ok(())
}

#[derive(Default)]
struct Cursor {
    last_id: i64,
    /// Stall legibility: the inserting xid that last blocked the cursor
    /// (the gap-safety guard stopping at an unsettled row), and how many
    /// consecutive ticks it has blocked. The xmin guard correctly waits
    /// for a long-open Postgres transaction to commit/abort, but that
    /// can freeze the whole fleet's event publishing for the duration;
    /// a breadcrumb after enough ticks names the culprit so the stall is
    /// not invisible. Reset when the cursor advances.
    blocked_on_xid: Option<i64>,
    blocked_ticks: u32,
}

/// Long-running task. Spawn one per dispatcher Pod.
pub async fn run(state: DispatcherState) {
    let mut cursor = Cursor::default();

    if let Err(e) = bootstrap(&state.pg_pool, &mut cursor).await {
        tracing::warn!(target: "weft_dispatcher::journal_bridge", error = %e, "bootstrap failed");
    }

    loop {
        if let Err(e) = drain_new_rows(&state, &mut cursor).await {
            tracing::warn!(
                target: "weft_dispatcher::journal_bridge",
                error = %e,
                "drain failed; will retry"
            );
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// True iff the transaction that inserted a row (`xmin`, 32-bit) is
/// strictly below the snapshot's xmin horizon (xid8 as i64), i.e.
/// every transaction old enough to have allocated a lower exec_event
/// id has finished. The row's xmin wraps at 2^32 while the horizon
/// carries the epoch, so the comparison is done modulo 2^32 with a
/// signed wraparound distance (valid because Postgres keeps live
/// xids within 2^31 of the current horizon).
fn xid_settled(inserted_xid: i64, horizon_xid: i64) -> bool {
    let row = inserted_xid as u32;
    let horizon = horizon_xid as u32;
    (horizon.wrapping_sub(row) as i32) > 0
}

async fn bootstrap(pool: &sqlx::PgPool, cursor: &mut Cursor) -> anyhow::Result<()> {
    // Read persisted cursor. The migration seeds it to 0 on first
    // run; subsequent runs pick up where the cluster left off so a
    // dispatcher restart doesn't strand `Deactivating` projects on
    // unprocessed terminal events.
    let row: Option<(i64,)> =
        sqlx::query_as("SELECT last_id FROM dispatcher_cursor WHERE key = $1")
            .bind(CURSOR_KEY)
            .fetch_optional(pool)
            .await?;
    cursor.last_id = row.map(|(v,)| v).unwrap_or(0);
    Ok(())
}

async fn drain_new_rows(
    state: &DispatcherState,
    cursor: &mut Cursor,
) -> anyhow::Result<()> {
    // Gap-safety: `id` is BIGSERIAL, allocated at INSERT time, but
    // transactions can COMMIT out of id order. A plain `id > cursor`
    // poll that advances past the max id seen would permanently skip
    // a lower-id row whose transaction commits after this poll. The
    // guard: alongside each row, fetch the row's inserting xid
    // (`xmin`) and the snapshot's xmin horizon
    // (`pg_snapshot_xmin(pg_current_snapshot())`). A row whose
    // inserting transaction is still at/above the horizon may have
    // in-flight SIBLINGS holding lower ids, so we stop processing
    // there and re-poll the same window next tick; the cursor only
    // ever advances over rows whose entire lower-id neighborhood is
    // settled.
    let rows = sqlx::query(
        "SELECT id, color, payload_json, \
                xmin::text::bigint AS inserted_xid, \
                pg_snapshot_xmin(pg_current_snapshot())::text::bigint AS horizon_xid \
         FROM exec_event \
         WHERE id > $1 ORDER BY id ASC LIMIT 1000",
    )
    .bind(cursor.last_id)
    .fetch_all(&state.pg_pool)
    .await?;
    // Per-row processing returns Ok on both happy path AND
    // intentional skips (malformed payload, color/project parse
    // miss). Cursor advances on Ok. A hard error (DB write inside
    // `terminal_cleanup`, journal read, publish) bails via `?` and
    // leaves the cursor at the last successful row, so the next
    // tick retries.
    let cursor_start_id = cursor.last_id;
    let mut max_id_processed = cursor.last_id;
    for row in rows {
        let id: i64 = row.try_get("id")?;
        let inserted_xid: i64 = row.try_get("inserted_xid")?;
        let horizon_xid: i64 = row.try_get("horizon_xid")?;
        if !xid_settled(inserted_xid, horizon_xid) {
            // Re-polled next tick once every older transaction has
            // committed or aborted (the horizon moves past it). Track
            // the blocking xid so a prolonged stall (a long-open
            // transaction holding the horizon back, freezing fleet-wide
            // event publishing) becomes legible instead of silent.
            const STALL_BREADCRUMB_TICKS: u32 = 30;
            if cursor.blocked_on_xid == Some(inserted_xid) {
                cursor.blocked_ticks += 1;
            } else {
                cursor.blocked_on_xid = Some(inserted_xid);
                cursor.blocked_ticks = 1;
            }
            if cursor.blocked_ticks % STALL_BREADCRUMB_TICKS == 0 {
                tracing::warn!(
                    target: "weft_dispatcher::journal_bridge",
                    blocking_xid = inserted_xid,
                    horizon_xid,
                    ticks = cursor.blocked_ticks,
                    after_id = cursor.last_id,
                    "journal-bridge cursor held back: an uncommitted transaction (xid above the \
                     snapshot horizon) is blocking event publishing for the whole fleet; this \
                     self-clears when that transaction commits or aborts. A long-open Postgres \
                     transaction is the usual cause."
                );
            }
            break;
        }
        process_one_row(state, &row, id).await?;
        cursor.last_id = id;
        max_id_processed = id;
    }
    // Cursor advanced (or there was nothing to block on): clear the
    // stall tracker so the next genuine stall starts a fresh count.
    if max_id_processed > cursor_start_id {
        cursor.blocked_on_xid = None;
        cursor.blocked_ticks = 0;
    }
    if max_id_processed > 0 {
        sqlx::query(
            "UPDATE dispatcher_cursor SET last_id = $1 \
             WHERE key = $2 AND last_id < $1",
        )
        .bind(max_id_processed)
        .bind(CURSOR_KEY)
        .execute(&state.pg_pool)
        .await?;
    }
    Ok(())
}

/// When an execution reaches a terminal state, strip every wake-
/// signal registration tied to it so the listener can exit when
/// its tenant's registry hits zero. Then, if the project this
/// color belongs to is currently `Deactivating`, check whether
/// the running set is now empty: if so, CAS the project's status
/// to `Inactive` (the deactivate-with-runningPolicy=wait drain has
/// finished).
/// Side effects for one exec_event row. Soft skips (malformed
/// payload, unparseable color, missing execution project) return
/// Ok so the caller advances the cursor past them. Hard errors
/// (DB writes inside terminal_cleanup, publish) propagate via `?`
/// so the cursor stays put and the next tick retries.
async fn process_one_row(
    state: &DispatcherState,
    row: &sqlx::postgres::PgRow,
    id: i64,
) -> anyhow::Result<()> {
    let payload: String = row.try_get("payload_json")?;
    let color_str: String = row.try_get("color")?;
    let event: ExecEvent = match serde_json::from_str(&payload) {
        Ok(e) => e,
        Err(e) => {
            tracing::warn!(
                target: "weft_dispatcher::journal_bridge",
                %id, error = %e,
                "could not parse journal payload; skipping"
            );
            return Ok(());
        }
    };
    let Ok(color) = color_str.parse() else {
        return Ok(());
    };
    // NotFound and Corrupt both soft-skip: the cursor must advance
    // past a permanently-poisoned row (the decode site already
    // logged loud) rather than stall the fleet.
    let project_id = match state.journal.execution_project(color).await?.found() {
        Some(p) => p,
        None => return Ok(()),
    };
    // Terminal events drive signal-row cleanup + the
    // deactivate-drain CAS. Idempotent across pods: only the
    // first pod observing the terminal row removes the signal
    // entries; sibling pods see an empty result and skip.
    match &event {
        ExecEvent::ExecutionCompleted { .. }
        | ExecEvent::ExecutionFailed { .. }
        | ExecEvent::ExecutionCancelled { .. } => {
            terminal_cleanup(state, color).await?;
            // Storage terminate sweep: queue the un-kept exec-file
            // sweep DURABLY (workers stall-then-die, so worker-side
            // cleanup is only an eager optimization; this row is the
            // guarantee). The storage_sweep reaper drains the queue.
            let tenant = state.tenant_router.tenant_for_project(&project_id);
            crate::storage_box::enqueue_sweep(
                &state.pg_pool,
                tenant.as_str(),
                &color.to_string(),
            )
            .await?;
        }
        // A suspension is the running -> suspended edge of the drain
        // condition: `running_count` excludes suspended colors (the
        // signal row exists before this event is journaled), so a
        // wait-mode deactivate whose last running execution SUSPENDS
        // must re-check here or it stays Deactivating forever.
        // `try_finish_drain` is idempotent, so the extra trigger is
        // free when nothing is draining.
        ExecEvent::SuspensionRegistered { .. } => {
            try_finish_drain(state, &project_id, None).await?;
        }
        _ => {}
    }
    // One ExecEvent can project to MULTIPLE DispatcherEvents: a
    // PulseEmitted carrying a bus marker yields both BusParticipant
    // edges (source-node + target-node) in addition to the pulse
    // notification itself.
    for de in to_dispatcher_events(&event, project_id) {
        // Local-only: every dispatcher pod runs this same bridge,
        // so every pod's own subscribers get the event from its
        // own poll. NOTIFY would cause double-delivery.
        state.events.publish_local(de).await;
    }
    Ok(())
}

async fn terminal_cleanup(state: &DispatcherState, color: weft_core::Color) -> anyhow::Result<()> {
    let removed = state.journal.signal_remove_for_color(color).await?;
    let project_id = removed.first().map(|m| m.project_id.clone());
    state
        .listeners
        .unregister_many(&state.pg_pool, &removed)
        .await;

    // If signal_remove_for_color found nothing (entry trigger or
    // already-cleaned execution), still try to find the project
    // via execution_project so the drain-watcher fires.
    let project_id = match project_id {
        Some(p) => Some(p),
        None => state.journal.execution_project(color).await?.found(),
    };
    if let Some(project_id) = project_id {
        try_finish_drain(state, &project_id, None).await?;
    }
    Ok(())
}

/// Drain-watcher CAS. If the project is `Deactivating` AND no
/// running non-suspended executions remain, flip status to
/// `Inactive`. Idempotent: a stale view loses the CAS and the
/// next terminal event re-checks. Activate concurrently flipping
/// status back to `Active` also wins the CAS, so the deactivate
/// rolls back cleanly.
///
/// `exclude_task`: a still-claimed task row to discount from the
/// running count. The route_entry executor's re-park branch passes
/// its own task id (the task journals nothing and is about to
/// complete, but its row is still `claimed` at check time); every
/// other caller passes `None`.
pub(crate) async fn try_finish_drain(
    state: &DispatcherState,
    project_id: &str,
    exclude_task: Option<uuid::Uuid>,
) -> anyhow::Result<()> {
    use crate::project_store::ProjectStatus;
    let id = match uuid::Uuid::parse_str(project_id) {
        Ok(id) => id,
        Err(_) => return Ok(()),
    };
    let Some(lifecycle) = state.projects.lifecycle(id).await? else {
        return Ok(());
    };
    if lifecycle.status != ProjectStatus::Deactivating {
        return Ok(());
    }
    let running = crate::api::project::running_count(state, project_id, exclude_task).await?;
    if running > 0 {
        return Ok(());
    }
    let flipped = state
        .projects
        .cas_status(id, ProjectStatus::Deactivating, ProjectStatus::Inactive)
        .await?;
    if flipped {
        tracing::info!(
            target: "weft_dispatcher::journal_bridge",
            project_id,
            "drain finished: deactivating -> inactive"
        );
    }
    Ok(())
}

pub(crate) fn to_dispatcher_events(ev: &ExecEvent, project_id: String) -> Vec<DispatcherEvent> {
    match ev {
        ExecEvent::ExecutionStarted { color, entry_node, .. } => {
            vec![DispatcherEvent::ExecutionStarted {
                color: *color,
                entry_node: entry_node.clone(),
                project_id,
            }]
        }
        ExecEvent::NodeStarted { color, node_id, frames, input, closed_ports, .. } => {
            vec![DispatcherEvent::NodeStarted {
                color: *color,
                node: node_id.clone(),
                frames: frames.clone(),
                input: input.clone(),
                closed_ports: closed_ports.clone(),
                project_id,
            }]
        }
        ExecEvent::NodeCompleted { color, node_id, frames, output, .. } => {
            vec![DispatcherEvent::NodeCompleted {
                color: *color,
                node: node_id.clone(),
                frames: frames.clone(),
                output: output.clone(),
                project_id,
            }]
        }
        ExecEvent::NodeFailed { color, node_id, frames, error, .. } => {
            vec![DispatcherEvent::NodeFailed {
                color: *color,
                node: node_id.clone(),
                frames: frames.clone(),
                error: error.clone(),
                project_id,
            }]
        }
        ExecEvent::NodeSkipped { color, node_id, frames, closed_ports, .. } => {
            vec![DispatcherEvent::NodeSkipped {
                color: *color,
                node: node_id.clone(),
                frames: frames.clone(),
                closed_ports: closed_ports.clone(),
                project_id,
            }]
        }
        ExecEvent::PortTypeMismatch { color, node_id, frames, port, expected, actual, .. } => {
            vec![DispatcherEvent::PortTypeMismatch {
                color: *color,
                node: node_id.clone(),
                frames: frames.clone(),
                port: port.clone(),
                expected: expected.clone(),
                actual: actual.clone(),
                project_id,
            }]
        }
        ExecEvent::NodeSuspended { color, node_id, frames, token, .. } => {
            vec![DispatcherEvent::NodeSuspended {
                color: *color,
                node: node_id.clone(),
                frames: frames.clone(),
                token: token.clone(),
                project_id,
            }]
        }
        ExecEvent::NodeResumed { color, node_id, frames, token, value, .. } => {
            vec![DispatcherEvent::NodeResumed {
                color: *color,
                node: node_id.clone(),
                frames: frames.clone(),
                token: token.clone(),
                value: value.clone(),
                project_id,
            }]
        }
        ExecEvent::NodeCancelled { color, node_id, frames, reason, .. } => {
            vec![DispatcherEvent::NodeCancelled {
                color: *color,
                node: node_id.clone(),
                frames: frames.clone(),
                reason: reason.clone(),
                project_id,
            }]
        }
        ExecEvent::ExecutionCompleted { color, outputs, .. } => {
            vec![DispatcherEvent::ExecutionCompleted {
                color: *color,
                outputs: outputs.clone(),
                project_id,
            }]
        }
        ExecEvent::ExecutionFailed { color, error, .. } => {
            // No truncation: journal_bridge fans out via
            // `publish_local` (no NOTIFY hop); the full error is
            // what the operator wants for debugging. Truncation
            // belongs at NOTIFY producer sites (api/project.rs,
            // infra_event_bridge.rs), not here.
            vec![DispatcherEvent::ExecutionFailed {
                color: *color,
                error: error.clone(),
                project_id,
            }]
        }
        ExecEvent::ExecutionCancelled { color, reason, .. } => {
            vec![DispatcherEvent::ExecutionCancelled {
                color: *color,
                reason: reason.clone(),
                project_id,
            }]
        }
        ExecEvent::CostReported { color, service, amount_usd, .. } => {
            vec![DispatcherEvent::CostReported {
                color: *color,
                project_id,
                service: service.clone(),
                amount_usd: *amount_usd,
            }]
        }
        // Bus events: surfaced so the inspector renders a live IRC-style
        // log per bus. The webview groups by `bus_id` and renders ONE
        // panel per bus on every node listed as a `BusParticipant`.
        ExecEvent::BusJoined { color, bus_id, offset, name, at_unix } => {
            vec![DispatcherEvent::BusJoined {
                color: *color,
                project_id,
                bus_id: bus_id.clone(),
                offset: *offset,
                name: name.clone(),
                at_unix: *at_unix,
            }]
        }
        ExecEvent::BusLeft { color, bus_id, offset, name, at_unix } => {
            vec![DispatcherEvent::BusLeft {
                color: *color,
                project_id,
                bus_id: bus_id.clone(),
                offset: *offset,
                name: name.clone(),
                at_unix: *at_unix,
            }]
        }
        ExecEvent::BusMessage {
            color, bus_id, offset, from, msg_kind, payload,
            payload_byte_size, payload_sha256_prefix, at_unix,
        } => {
            vec![DispatcherEvent::BusMessage {
                color: *color,
                project_id,
                bus_id: bus_id.clone(),
                offset: *offset,
                from: from.clone(),
                msg_kind: msg_kind.clone(),
                payload: payload.clone(),
                payload_byte_size: *payload_byte_size,
                payload_sha256_prefix: *payload_sha256_prefix,
                at_unix: *at_unix,
            }]
        }
        ExecEvent::BusClosed { color, bus_id, offset, at_unix } => {
            vec![DispatcherEvent::BusClosed {
                color: *color,
                project_id,
                bus_id: bus_id.clone(),
                offset: *offset,
                at_unix: *at_unix,
            }]
        }
        // Bus participation is derived from PulseEmitted: a pulse
        // whose value carries a bus marker means BOTH the producer
        // node AND the consumer node touched that bus, so we emit a
        // BusParticipant edge for each end. The webview unions these
        // into a per-bus participant set; each participant gets the
        // inspector's IRC panel for that bus.
        //
        // Counter-perspective: a pulse routed through a passthrough /
        // group boundary will also stamp the boundary node as a
        // participant. That's intentional: a Group's inspector
        // SHOULD show the bus conversation flowing through it. The
        // alternative ("only stamp leaf nodes") would hide the
        // signal at the group level for no real benefit.
        ExecEvent::PulseEmitted {
            color, source_node, target_node, value, closed, ..
        } => sniff_bus_participants(*color, &project_id, source_node, target_node, value, *closed),
        ExecEvent::LoopInstantiated {
            color, group_id, parent_frames, iter_count, parallel, ..
        } => vec![DispatcherEvent::LoopInstantiated {
            color: *color, project_id,
            group_id: group_id.clone(),
            parent_frames: parent_frames.clone(),
            iter_count: *iter_count,
            parallel: *parallel,
        }],
        ExecEvent::LoopIterationLaunched {
            color, group_id, parent_frames, index, body_emissions, ..
        } => {
            // The body pulses ride INSIDE this row (atomic marker+pulses),
            // so the bus-participant sniff must run over them here, just
            // as it does for standalone PulseEmitted rows; otherwise a bus
            // marker pulsed into a loop body never registers its consumer
            // as a participant (the agent-swarm pattern).
            let mut out = vec![DispatcherEvent::LoopIterationLaunched {
                color: *color, project_id: project_id.clone(),
                group_id: group_id.clone(),
                parent_frames: parent_frames.clone(),
                index: *index,
            }];
            for e in body_emissions {
                out.extend(sniff_bus_participants(
                    *color, &project_id, &e.source_node, &e.target_node, &e.value, e.closed,
                ));
            }
            out
        }
        ExecEvent::LoopOutFired {
            color, group_id, parent_frames, index, done_vote, ..
        } => vec![DispatcherEvent::LoopOutFired {
            color: *color, project_id,
            group_id: group_id.clone(),
            parent_frames: parent_frames.clone(),
            index: *index,
            done_vote: *done_vote,
        }],
        ExecEvent::LoopTerminated {
            color, group_id, parent_frames, reason, outward_emissions, ..
        } => {
            // Outward pulses ride INSIDE this row; sniff them for bus
            // markers too (a loop exporting a bus handle outward through a
            // gather/carry port must register its downstream consumer as a
            // participant).
            let mut out = vec![DispatcherEvent::LoopTerminated {
                color: *color, project_id: project_id.clone(),
                group_id: group_id.clone(),
                parent_frames: parent_frames.clone(),
                reason: *reason,
            }];
            for e in outward_emissions {
                out.extend(sniff_bus_participants(
                    *color, &project_id, &e.source_node, &e.target_node, &e.value, e.closed,
                ));
            }
            out
        }
        // Caller events: surfaced 1:1 so the inspector replays the live
        // caller exchange (connected / inbound / outbound / errored /
        // disconnected) the same way it replays a bus. Payloads carry the
        // journaled-vs-ephemeral `JournaledPayload` so high-volume streams stay
        // metadata-only.
        ExecEvent::CallerConnected { color, offset, protocol, at_unix } => {
            vec![DispatcherEvent::CallerConnected {
                color: *color, project_id, offset: *offset,
                protocol: protocol.clone(), at_unix: *at_unix,
            }]
        }
        ExecEvent::CallerInbound {
            color, offset, payload, payload_byte_size, payload_sha256_prefix, at_unix,
        } => {
            vec![DispatcherEvent::CallerInbound {
                color: *color, project_id, offset: *offset,
                payload: payload.clone(),
                payload_byte_size: *payload_byte_size,
                payload_sha256_prefix: *payload_sha256_prefix,
                at_unix: *at_unix,
            }]
        }
        ExecEvent::CallerOutbound {
            color, offset, payload, payload_byte_size, payload_sha256_prefix, terminal, at_unix,
        } => {
            vec![DispatcherEvent::CallerOutbound {
                color: *color, project_id, offset: *offset,
                payload: payload.clone(),
                payload_byte_size: *payload_byte_size,
                payload_sha256_prefix: *payload_sha256_prefix,
                terminal: *terminal,
                at_unix: *at_unix,
            }]
        }
        ExecEvent::CallerErrored { color, offset, message, at_unix } => {
            vec![DispatcherEvent::CallerErrored {
                color: *color, project_id, offset: *offset,
                message: message.clone(), at_unix: *at_unix,
            }]
        }
        ExecEvent::CallerDisconnected { color, offset, reason, at_unix } => {
            vec![DispatcherEvent::CallerDisconnected {
                color: *color, project_id, offset: *offset,
                reason: reason.clone(), at_unix: *at_unix,
            }]
        }
        // SuspensionRegistered / SuspensionResolved / LogLine /
        // RunOutput / NodeKicked: not surfaced through DispatcherEvent.
        // SSE consumers don't need them for live UI; they read the
        // journal directly when they want full detail.
        _ => Vec::new(),
    }
}

/// Derive `BusParticipant` events from one emitted pulse if its value
/// carries a bus marker. Shared by the standalone `PulseEmitted` arm and
/// the loop arms whose pulses ride INSIDE the marker row
/// (`LoopIterationLaunched.body_emissions`,
/// `LoopTerminated.outward_emissions`), so the rule stays "any journaled
/// pulse, however it rides, derives participants".
fn sniff_bus_participants(
    color: uuid::Uuid,
    project_id: &str,
    source_node: &str,
    target_node: &str,
    value: &serde_json::Value,
    closed: bool,
) -> Vec<DispatcherEvent> {
    // Closure pulses are structural markers (`value: Null`) and can never
    // carry a bus marker. Bail before the sniff so a future change that
    // puts non-null payloads on closures can't synthesise spurious edges.
    if closed {
        return Vec::new();
    }
    let Some(bus_id) = weft_core::weft_type::WeftType::bus_marker_id(value) else {
        return Vec::new();
    };
    let bus_id = bus_id.to_string();
    // A `None` mode means the marker carries an id but no recognised mode
    // (malformed: every `BusHandle::marker()` always sets both). Log loud
    // and skip rather than defaulting to journaled, which would mislabel
    // the inspector and hide the corruption.
    let Some(mode) = weft_core::weft_type::WeftType::bus_marker_mode(value) else {
        tracing::warn!(
            target: "weft_dispatcher::journal_bridge",
            bus_id, %color, marker = %value,
            "skip BusParticipant: marker has id but no recognised mode"
        );
        return Vec::new();
    };
    let ephemeral = mode == weft_core::bus::BusMode::Ephemeral;
    let mut out = vec![DispatcherEvent::BusParticipant {
        color,
        project_id: project_id.to_string(),
        bus_id: bus_id.clone(),
        node_id: source_node.to_string(),
        ephemeral,
    }];
    if target_node != source_node {
        out.push(DispatcherEvent::BusParticipant {
            color,
            project_id: project_id.to_string(),
            bus_id,
            node_id: target_node.to_string(),
            ephemeral,
        });
    }
    out
}
