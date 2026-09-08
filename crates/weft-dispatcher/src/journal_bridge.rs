//! Bridge between the journal's `exec_event` table and the
//! dispatcher's `EventBus` SSE fanout.
//!
//! Polls `exec_event` on a tick, picks up newly-inserted rows, folds
//! each row into its execution's live projection
//! (`crate::projection::ExecutionProjector`, one per open color on
//! this pod), and publishes the `DispatcherEvent`s it paints to
//! `EventBus` so SSE consumers (CLI follow, VS Code execution view)
//! see the live event.
//!
//! Why a projection instead of broadcasting `ExecEvent` directly:
//! the SSE wire format is `DispatcherEvent` and several consumers
//! depend on it, and the journal rows no longer carry what the
//! screen shows (a firing's input, its output, a group boundary
//! running). The journal is the durable shape; DispatcherEvent is the
//! user-facing shape, derived by folding the rows over the program.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use sqlx::Row;

use weft_journal::ExecEvent;

use crate::projection::{execution_program, ExecutionProjector, ProgramLookup};
use crate::state::DispatcherState;

const POLL_INTERVAL: Duration = Duration::from_millis(100);
/// A projection that has seen no row for this long is dropped: a run
/// parked on a form for days would otherwise hold its whole fold in
/// this pod's RAM. The next row for the color rebuilds it from the
/// journal, the same path a pod that boots mid-run takes.
const PROJECTION_IDLE_TTL: Duration = Duration::from_secs(10 * 60);
/// Single-row table key. We only ever have one cursor for the
/// whole dispatcher fleet; rows are keyed by this constant so any
/// Pod's UPDATE targets the same row.
const CURSOR_KEY: &str = "journal_bridge";

/// Persistent cursor table. One row per cursor key. The bridge
/// reads `last_id` on boot and writes it after every successful
/// drain so a Pod restart resumes where the cluster left off. The
/// seed row's key literal is `CURSOR_KEY` (static DDL cannot bind).
pub static GROUP: weft_task_store::SchemaGroup = weft_task_store::SchemaGroup {
    name: "dispatcher_cursor",
    tables: &["dispatcher_cursor"],
    ddl: &[
        r#"CREATE TABLE IF NOT EXISTS dispatcher_cursor (
            key TEXT PRIMARY KEY,
            last_id BIGINT NOT NULL
        )"#,
    ],
    seed: &[
        "INSERT INTO dispatcher_cursor (key, last_id) VALUES ('journal_bridge', 0) \
         ON CONFLICT (key) DO NOTHING",
    ],
};

#[derive(Default)]
struct Cursor {
    last_id: i64,
    /// One live projection per execution this pod has seen rows for
    /// recently and that has not reached its terminal. A color first
    /// seen mid-flight (this pod booted after the run started, or the
    /// projection was evicted idle) rebuilds its projection from the
    /// rows before the cursor. RAM only: any pod rebuilds any of them
    /// from Postgres.
    projectors: HashMap<weft_core::Color, LiveProjection>,
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

struct LiveProjection {
    projector: ExecutionProjector,
    last_row_at: Instant,
}

impl Cursor {
    /// Drop the projections that have folded nothing for a while. One
    /// that does not fold at all (a run with no program to read) holds
    /// no RAM worth freeing, and reopening it would republish its
    /// corruption, so it stays until its terminal.
    fn evict_idle_projections(&mut self, now: Instant) {
        self.projectors.retain(|_, live| {
            !live.projector.folds() || now.duration_since(live.last_row_at) < PROJECTION_IDLE_TTL
        });
    }
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
        cursor.evict_idle_projections(Instant::now());
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
            if cursor.blocked_ticks.is_multiple_of(STALL_BREADCRUMB_TICKS) {
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
        process_one_row(state, cursor, &row, id).await?;
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
    cursor: &mut Cursor,
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
    // A missing row soft-skips: the cursor must advance past a row for
    // a wiped execution rather than stall the fleet. Read ONCE here;
    // the terminal arm below needs the tenant off the same row.
    let Some(owner) = state.journal.execution_owner(color).await? else {
        return Ok(());
    };
    let project_id = owner.project_id.clone();
    // Terminal events drive signal-row cleanup + the
    // deactivate-drain CAS. Idempotent across pods: only the
    // first pod observing the terminal row removes the signal
    // entries; sibling pods see an empty result and skip.
    match &event {
        e if e.is_execution_terminal() => {
            terminal_cleanup(state, color).await?;
            // Storage terminate sweep: queue the un-kept exec-file
            // sweep DURABLY (workers stall-then-die, so worker-side
            // cleanup is only an eager optimization; this row is the
            // guarantee). The storage_sweep reaper drains the queue.
            //
            // The tenant comes from the execution's OWN journal row
            // (`execution_color.tenant_id`, frozen at start), NOT the mutable
            // project store: it is the tenant the run actually keyed its storage
            // under, and it survives project deletion.
            crate::storage::enqueue_sweep(
                &state.pg_pool,
                &owner.tenant,
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
    // One ExecEvent can project to MULTIPLE DispatcherEvents: a row
    // that fires a group boundary paints the boundary's start and end,
    // and a value carrying a bus marker yields BusParticipant edges.
    let projected = if let Some(live) = cursor.projectors.get_mut(&color) {
        live.last_row_at = Instant::now();
        live.projector.project(&event)
    } else if !matches!(event, ExecEvent::ExecutionStarted { .. })
        && !event.is_execution_terminal()
        && ExecutionProjector::paints_without_program(&event)
    {
        // A row that paints the same with or without the program (a
        // cost record landing after the terminal, a tag, a log line)
        // on a color this pod holds no projection for is painted as it
        // is: opening a projection would refold the whole log for
        // nothing. Two exceptions open one: the birth row (the run's
        // node rows will need it) and a terminal (its completion
        // carries the run's outputs, which only a fold knows).
        ExecutionProjector::new(color, ProgramLookup::NoProgram, project_id.clone()).project(&event)
    } else {
        let live = open_projection(state, cursor, color, project_id.clone(), id, &event).await?;
        live.projector.project(&event)
    };
    for de in crate::events::IdentifiedEvent::recorded(id, ()).project(|()| projected) {
        // Local-only: every dispatcher pod runs this same bridge,
        // so every pod's own subscribers get the event from its
        // own poll. NOTIFY would cause double-delivery.
        state.events.publish_local(de).await;
    }
    // A terminal ends the projection: nothing more lands for the color.
    if event.is_execution_terminal() {
        cursor.projectors.remove(&color);
    }
    Ok(())
}

/// Open the execution's live projection on this pod: on its birth row,
/// or rebuilt from every earlier row when the pod first sees the color
/// mid-flight. Only the database itself is a hard error (the cursor
/// stays put and the next tick retries). Anything permanent about ONE
/// color (its program cannot be found, an earlier row no longer
/// decodes) degrades that color's projection to the program-free
/// painting and publishes the corruption, so the cursor (fleet-wide)
/// moves on.
async fn open_projection<'c>(
    state: &DispatcherState,
    cursor: &'c mut Cursor,
    color: weft_core::Color,
    project_id: String,
    row_id: i64,
    event: &ExecEvent,
) -> anyhow::Result<&'c mut LiveProjection> {
    let program = match execution_program(state, color).await? {
        ProgramLookup::Unreadable(reason) => {
            publish_unreadable(state, color, &project_id, reason).await;
            ProgramLookup::Unreadable(String::new())
        }
        found => found,
    };
    let projector = if matches!(event, ExecEvent::ExecutionStarted { .. }) {
        ExecutionProjector::new(color, program, project_id)
    } else {
        match rows_before(&state.pg_pool, color, row_id).await? {
            CatchUp::Rows(earlier) => {
                let mut projector = ExecutionProjector::new(color, program, project_id);
                for row in &earlier {
                    projector.project(row);
                }
                projector
            }
            CatchUp::Undecodable { row_id, reason } => {
                publish_unreadable(
                    state,
                    color,
                    &project_id,
                    format!("row {row_id} of this execution no longer decodes: {reason}"),
                )
                .await;
                ExecutionProjector::new(color, ProgramLookup::Unreadable(reason), project_id)
            }
        }
    };
    // The caller found no projection for the color; a stale one here
    // would mean two folds of one run on this pod, which nothing does.
    let replaced = cursor.projectors.insert(color, LiveProjection { projector, last_row_at: Instant::now() });
    assert!(replaced.is_none(), "a projection for color {color} was already open on this pod");
    Ok(cursor.projectors.get_mut(&color).expect("inserted just above"))
}

/// A color whose journal cannot be read back: logged, and published
/// as the same corruption event the replay read publishes, so the
/// screen shows the run as unreadable and names `weft clean`.
async fn publish_unreadable(
    state: &DispatcherState,
    color: weft_core::Color,
    project_id: &str,
    reason: String,
) {
    tracing::error!(
        target: "weft_dispatcher::journal_bridge",
        %color, %reason,
        "this execution's journal cannot be read back; its live projection paints only what \
         needs no program. `weft clean` removes the execution."
    );
    let corruption = crate::events::IdentifiedEvent::transient(
        crate::events::DispatcherEvent::JournalCorruption {
            color,
            project_id: project_id.to_string(),
            site: weft_core::primitive::CorruptionSite::UndecodableRow,
            reason,
        },
    );
    state.events.publish_local(corruption).await;
}

/// What a projection opened mid-flight replays to catch up.
enum CatchUp {
    /// Every earlier row of the color, in journal order.
    Rows(Vec<ExecEvent>),
    /// A row that no longer decodes: a projection built over a partial
    /// log would paint a run that never existed, so the caller paints
    /// the color without a program instead.
    Undecodable { row_id: i64, reason: String },
}

/// Every row of `color` below `before_id`, in journal order. `Err`
/// only for the database itself (the cursor stays put and retries).
async fn rows_before(
    pool: &sqlx::PgPool,
    color: weft_core::Color,
    before_id: i64,
) -> anyhow::Result<CatchUp> {
    let rows: Vec<(i64, String)> = sqlx::query_as(
        "SELECT id, payload_json FROM exec_event WHERE color = $1 AND id < $2 ORDER BY id ASC",
    )
    .bind(color.to_string())
    .bind(before_id)
    .fetch_all(pool)
    .await?;
    let mut out = Vec::with_capacity(rows.len());
    for (row_id, payload) in rows {
        match weft_journal::decode_event(color, &payload) {
            Ok(event) => out.push(event),
            Err(reason) => return Ok(CatchUp::Undecodable { row_id, reason }),
        }
    }
    Ok(CatchUp::Rows(out))
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
    // via the execution's own row so the drain-watcher fires.
    let project_id = match project_id {
        Some(p) => Some(p),
        None => state.journal.execution_owner(color).await?.map(|o| o.project_id),
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
        // Broadcast the landing so both frontends reconcile without a
        // verb (the backend-owns-state rule needs the exit of a
        // transitional state to be observable, not just its entry).
        crate::transition::publish_transition_changed(state, id).await;
    }
    Ok(())
}
