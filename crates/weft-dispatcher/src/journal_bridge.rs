//! Bridge between the journal's `exec_event` table and the
//! dispatcher's `EventBus` SSE fanout.
//!
//! Wakes on every journal row announced (`EXEC_EVENT_CHANNEL`), picks
//! up newly-committed rows, folds
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

use crate::pg_wake::{self, DrainStep, WakeOn};
use crate::projection::{execution_program, ExecutionProjector, ProgramLookup};
use crate::settled::{Position, SettledReader};
use crate::state::DispatcherState;

/// Rows read per look; a full batch means more may wait behind it.
const BATCH: i64 = 1000;
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
/// reads `(last_xid, last_id)` on boot and writes it after every successful
/// drain so a Pod restart resumes where the cluster left off. The
/// seed row's key literal is `CURSOR_KEY` (static DDL cannot bind).
pub static GROUP: weft_task_store::SchemaGroup = weft_task_store::SchemaGroup {
    name: "dispatcher_cursor",
    tables: &["dispatcher_cursor"],
    ddl: &[
        r#"CREATE TABLE IF NOT EXISTS dispatcher_cursor (
            key TEXT PRIMARY KEY,
            last_id BIGINT NOT NULL,
            -- With `last_id`, the last row passed, in the order a settled
            -- read goes (`crate::settled`). A cursor starts at xid 0,
            -- below every writer: the rows a database held before
            -- `writer_xid` existed were all given 0, so they keep their
            -- `last_id` order behind the cursor, and any row written
            -- since sorts after them.
            last_xid XID8 NOT NULL DEFAULT '0'::xid8
        )"#,
    ],
    seed: &[
        "INSERT INTO dispatcher_cursor (key, last_id) VALUES ('journal_bridge', 0) \
         ON CONFLICT (key) DO NOTHING",
    ],
};

struct Cursor {
    /// The last row passed (`crate::settled`).
    last: Position,
    /// One live projection per execution this pod has seen rows for
    /// recently and that has not reached its terminal. A color first
    /// seen mid-flight (this pod booted after the run started, or the
    /// projection was evicted idle) rebuilds its projection from the
    /// rows before the cursor. RAM only: any pod rebuilds any of them
    /// from Postgres.
    projectors: HashMap<weft_core::Color, LiveProjection>,
    /// The settled read the cursor advances by (`crate::settled`).
    reader: SettledReader,
}

struct LiveProjection {
    projector: ExecutionProjector,
    last_row_at: Instant,
}

impl Cursor {
    fn new() -> Self {
        Self { last: Position::default(), projectors: HashMap::new(), reader: SettledReader::new("journal_bridge") }
    }

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

const ON_EXEC_EVENT: &[WakeOn] = &[WakeOn::any(weft_journal::EXEC_EVENT_CHANNEL)];

/// Long-running task. Spawn one per dispatcher Pod. Sleeps until a
/// journal row is announced on `EXEC_EVENT_CHANNEL`, then publishes
/// everything new; the safety tick also drops idle projections.
pub async fn run(state: DispatcherState) {
    let cursor = tokio::sync::Mutex::new(Cursor::new());

    if let Err(e) = bootstrap(&state.pg_pool, &mut *cursor.lock().await).await {
        tracing::warn!(target: "weft_dispatcher::journal_bridge", error = %e, "bootstrap failed");
    }

    pg_wake::run(
        state.signals.subscribe(),
        ON_EXEC_EVENT,
        pg_wake::SAFETY_POLL_INTERVAL,
        "weft_dispatcher::journal_bridge",
        || async {
            let mut cursor = cursor.lock().await;
            let step = drain_new_rows(&state, &mut cursor).await;
            cursor.evict_idle_projections(Instant::now());
            step
        },
    )
    .await;
}

async fn bootstrap(pool: &sqlx::PgPool, cursor: &mut Cursor) -> anyhow::Result<()> {
    // Read persisted cursor. The migration seeds it to 0 on first
    // run; subsequent runs pick up where the cluster left off so a
    // dispatcher restart doesn't strand `Deactivating` projects on
    // unprocessed terminal events.
    let row: Option<(i64, i64)> = sqlx::query_as(
        "SELECT last_xid::text::bigint, last_id FROM dispatcher_cursor WHERE key = $1",
    )
    .bind(CURSOR_KEY)
    .fetch_optional(pool)
    .await?;
    cursor.last = row.map(|(xid, id)| Position { xid, id }).unwrap_or_default();
    Ok(())
}

/// Publish every settled row past the cursor, in id order (one batch).
/// The step says whether a full batch left more behind, or a row still
/// being written held the cursor back (see `crate::settled`).
async fn drain_new_rows(
    state: &DispatcherState,
    cursor: &mut Cursor,
) -> anyhow::Result<DrainStep> {
    // Rows commit out of id order, so the cursor goes in writer order
    // and stops at the horizon (`crate::settled`).
    let batch = {
        let mut conn = state.pg_pool.acquire().await?;
        cursor
            .reader
            .read(&mut conn, "exec_event", "color, payload_json", cursor.last, BATCH)
            .await?
    };
    // Per-row processing returns Ok on both happy path AND
    // intentional skips (malformed payload, color/project parse
    // miss). Cursor advances on Ok. A hard error (DB write inside
    // `terminal_cleanup`, journal read, publish) bails via `?` and
    // leaves the cursor at the last successful row, so the next
    // tick retries.
    let cursor_start = cursor.last;
    for row in batch.rows {
        let at = Position::of(&row)?;
        process_one_row(state, cursor, &row, at.id).await?;
        cursor.last = at;
    }
    if cursor.last > cursor_start {
        sqlx::query(
            "UPDATE dispatcher_cursor SET last_xid = $1::text::xid8, last_id = $2 \
             WHERE key = $3 AND (last_xid, last_id) < ($1::text::xid8, $2)",
        )
        .bind(cursor.last.xid.to_string())
        .bind(cursor.last.id)
        .bind(CURSOR_KEY)
        .execute(&state.pg_pool)
        .await?;
    }
    Ok(batch.next)
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
    let project_id = owner.project_id;
    // Terminal events drive signal-row cleanup + the
    // deactivate-drain CAS. Idempotent across pods: only the
    // first pod observing the terminal row removes the signal
    // entries; sibling pods see an empty result and skip.
    match &event {
        ExecEvent::ExecutionStarted { phase: weft_core::context::Phase::Fire, node_test: false, .. } => {
            crate::api::versions::record_trigger_run(state, color).await?;
        }
        e if e.is_execution_terminal() => {
            // Publication precedes the completion notification. The durable
            // cursor retries this after a dispatcher dies, even if the request
            // that started the bake no longer exists.
            if state.journal.is_trigger_setup_pending(color).await? {
                // Storage failures retry. A permanently unreadable capture must
                // release its ownership without replacing the last good bake.
                let (rows, bad) = state.journal.events_log_lossy(color).await?;
                let capture = match bad.into_iter().next() {
                    Some(reason) => Err(anyhow::Error::msg(reason)),
                    None => crate::journal::TriggerBake::from_events(
                        &rows.into_iter().map(|row| row.event).collect::<Vec<_>>(),
                    ),
                };
                match capture {
                    Ok(bake) => state.journal.finish_trigger_setup(color, bake.as_ref()).await?,
                    Err(error) => {
                        state.journal.finish_trigger_setup(color, None).await?;
                        publish_unreadable(state, color, project_id,
                            weft_core::primitive::CorruptionSite::UndecodableRow,
                            format!("cannot save trigger bake: {error:#}; the last good bake is preserved. Run `weft bake` again to refresh it."),
                        ).await;
                    }
                }
            }
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
            try_finish_drain(state, project_id, None).await?;
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
        ExecutionProjector::new(color, ProgramLookup::NoProgram, project_id).project(&event)
    } else {
        let live = open_projection(state, cursor, color, project_id, id, &event).await?;
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
    project_id: uuid::Uuid,
    row_id: i64,
    event: &ExecEvent,
) -> anyhow::Result<&'c mut LiveProjection> {
    // Whatever stops this run from painting whole, say it once, here,
    // as the projection opens. The projector then carries a reason-less
    // copy of the same state: the corruption is published, and the
    // projection must not republish it on every later row.
    let found = execution_program(state, color).await?;
    let program = match found.unpaintable() {
        Some((site, reason)) => {
            let reason = reason.to_string();
            publish_unreadable(state, color, project_id, site, reason).await;
            found.without_reason()
        }
        None => found,
    };
    let projector = if matches!(event, ExecEvent::ExecutionStarted { .. }) {
        let inheritance = inheritance_or_unreadable(state, color, project_id, std::slice::from_ref(event), &program).await;
        ExecutionProjector::new(color, program, project_id).with_inheritance(inheritance)
    } else {
        match rows_before(&state.pg_pool, color, row_id).await? {
            CatchUp::Rows(earlier) => {
                let inheritance = inheritance_or_unreadable(state, color, project_id, &earlier, &program).await;
                let mut projector = ExecutionProjector::new(color, program, project_id).with_inheritance(inheritance);
                for row in &earlier {
                    projector.project(row);
                }
                projector
            }
            CatchUp::Undecodable { row_id, reason } => {
                publish_unreadable(
                    state,
                    color,
                    project_id,
                    weft_core::primitive::CorruptionSite::UndecodableRow,
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

/// The seed chain a run inherits, or an empty one with the reason
/// published as the run's corruption (a cleaned seed leaves the run's
/// own rows paintable and its inherited nodes empty on the screen).
async fn inheritance_or_unreadable(
    state: &DispatcherState,
    color: weft_core::Color,
    project_id: uuid::Uuid,
    rows: &[ExecEvent],
    program: &ProgramLookup,
) -> weft_journal::SeedChain {
    match crate::projection::execution_inheritance(state, rows, program).await {
        Ok(chain) => chain,
        Err(reason) => {
            publish_unreadable(
                state,
                color,
                project_id,
                weft_core::primitive::CorruptionSite::UndecodableRow,
                reason,
            )
            .await;
            weft_journal::SeedChain::default()
        }
    }
}

/// A color whose journal cannot be read back: logged, and published
/// as the same corruption event the replay read publishes, so the
/// screen shows the run as unreadable and names `weft clean`.
async fn publish_unreadable(
    state: &DispatcherState,
    color: weft_core::Color,
    project_id: uuid::Uuid,
    site: weft_core::primitive::CorruptionSite,
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
            project_id,
            site,
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
    let project_id = removed.first().map(|m| m.project_id);
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
        try_finish_drain(state, project_id, None).await?;
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
    project_id: uuid::Uuid,
    exclude_task: Option<uuid::Uuid>,
) -> anyhow::Result<()> {
    use crate::project_store::ProjectStatus;
    let Some(lifecycle) = state.projects.lifecycle(project_id).await? else {
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
        .cas_status(project_id, ProjectStatus::Deactivating, ProjectStatus::Inactive)
        .await?;
    if flipped {
        tracing::info!(
            target: "weft_dispatcher::journal_bridge",
            %project_id,
            "drain finished: deactivating -> inactive"
        );
        // Broadcast the landing so both frontends reconcile without a
        // verb (the backend-owns-state rule needs the exit of a
        // transitional state to be observable, not just its entry).
        crate::transition::publish_transition_changed(state, project_id).await;
    }
    Ok(())
}
