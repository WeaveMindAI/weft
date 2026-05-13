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
    let rows = sqlx::query(
        "SELECT id, color, payload_json FROM exec_event \
         WHERE id > $1 ORDER BY id ASC LIMIT 1000",
    )
    .bind(cursor.last_id)
    .fetch_all(&state.pg_pool)
    .await?;
    let mut max_id_processed = cursor.last_id;
    for row in rows {
        let id: i64 = row.try_get("id")?;
        let payload: String = row.try_get("payload_json")?;
        let color_str: String = row.try_get("color")?;
        cursor.last_id = id;
        max_id_processed = id;
        let event: ExecEvent = match serde_json::from_str(&payload) {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!(
                    target: "weft_dispatcher::journal_bridge",
                    %id, error = %e,
                    "could not parse journal payload; skipping"
                );
                continue;
            }
        };
        let Ok(color) = color_str.parse() else { continue };
        let project_id = match state.journal.execution_project(color).await {
            Ok(Some(p)) => p,
            _ => continue,
        };
        // Terminal events drive signal-row cleanup + the
        // deactivate-drain CAS. Idempotent across Pods: only the
        // first Pod observing the terminal row removes the signal
        // entries; sibling Pods see an empty result and skip.
        match &event {
            ExecEvent::ExecutionCompleted { .. }
            | ExecEvent::ExecutionFailed { .. }
            | ExecEvent::ExecutionCancelled { .. } => {
                terminal_cleanup(state, color).await?;
            }
            _ => {}
        }
        if let Some(de) = to_dispatcher_event(&event, project_id) {
            // Local-only: every dispatcher pod runs this same bridge,
            // so every pod's own subscribers get the event from its
            // own poll. NOTIFY would cause double-delivery.
            state.events.publish_local(de).await;
        }
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
async fn terminal_cleanup(state: &DispatcherState, color: weft_core::Color) -> anyhow::Result<()> {
    let removed = state.journal.signal_remove_for_color(color).await?;
    let project_id = removed.first().map(|m| m.project_id.clone());
    state
        .listeners
        .unregister_many_if_alive(&state.pg_pool, &removed)
        .await;

    // If signal_remove_for_color found nothing (entry trigger or
    // already-cleaned execution), still try to find the project
    // via execution_project so the drain-watcher fires.
    let project_id = match project_id {
        Some(p) => Some(p),
        None => state.journal.execution_project(color).await?,
    };
    if let Some(project_id) = project_id {
        try_finish_drain(state, &project_id).await?;
    }
    Ok(())
}

/// Drain-watcher CAS. If the project is `Deactivating` AND no
/// running non-suspended executions remain, flip status to
/// `Inactive`. Idempotent: a stale view loses the CAS and the
/// next terminal event re-checks. Activate concurrently flipping
/// status back to `Active` also wins the CAS, so the deactivate
/// rolls back cleanly.
async fn try_finish_drain(state: &DispatcherState, project_id: &str) -> anyhow::Result<()> {
    use crate::project_store::ProjectStatus;
    let id = match uuid::Uuid::parse_str(project_id) {
        Ok(id) => id,
        Err(_) => return Ok(()),
    };
    let lifecycle = state.projects.lifecycle(id).await;
    if lifecycle.status != ProjectStatus::Deactivating {
        return Ok(());
    }
    let running = crate::api::project::running_count(state, project_id).await?;
    if running > 0 {
        return Ok(());
    }
    let flipped = state
        .projects
        .cas_status(id, ProjectStatus::Deactivating, ProjectStatus::Inactive)
        .await;
    if flipped {
        tracing::info!(
            target: "weft_dispatcher::journal_bridge",
            project_id,
            "drain finished: deactivating -> inactive"
        );
    }
    Ok(())
}

fn to_dispatcher_event(ev: &ExecEvent, project_id: String) -> Option<DispatcherEvent> {
    match ev {
        ExecEvent::ExecutionStarted { color, entry_node, .. } => {
            Some(DispatcherEvent::ExecutionStarted {
                color: *color,
                entry_node: entry_node.clone(),
                project_id,
            })
        }
        ExecEvent::NodeStarted { color, node_id, lane, input, .. } => {
            Some(DispatcherEvent::NodeStarted {
                color: *color,
                node: node_id.clone(),
                lane: serde_json::to_string(lane).unwrap_or_default(),
                input: input.clone(),
                project_id,
            })
        }
        ExecEvent::NodeCompleted { color, node_id, lane, output, .. } => {
            Some(DispatcherEvent::NodeCompleted {
                color: *color,
                node: node_id.clone(),
                lane: serde_json::to_string(lane).unwrap_or_default(),
                output: output.clone(),
                project_id,
            })
        }
        ExecEvent::NodeFailed { color, node_id, lane, error, .. } => {
            Some(DispatcherEvent::NodeFailed {
                color: *color,
                node: node_id.clone(),
                lane: serde_json::to_string(lane).unwrap_or_default(),
                error: error.clone(),
                project_id,
            })
        }
        ExecEvent::NodeSkipped { color, node_id, lane, .. } => {
            Some(DispatcherEvent::NodeSkipped {
                color: *color,
                node: node_id.clone(),
                lane: serde_json::to_string(lane).unwrap_or_default(),
                project_id,
            })
        }
        ExecEvent::NodeSuspended { color, node_id, lane, token, .. } => {
            Some(DispatcherEvent::NodeSuspended {
                color: *color,
                node: node_id.clone(),
                lane: serde_json::to_string(lane).unwrap_or_default(),
                token: token.clone(),
                project_id,
            })
        }
        ExecEvent::NodeResumed { color, node_id, lane, token, value, .. } => {
            Some(DispatcherEvent::NodeResumed {
                color: *color,
                node: node_id.clone(),
                lane: serde_json::to_string(lane).unwrap_or_default(),
                token: token.clone(),
                value: value.clone(),
                project_id,
            })
        }
        ExecEvent::NodeCancelled { color, node_id, lane, reason, .. } => {
            Some(DispatcherEvent::NodeCancelled {
                color: *color,
                node: node_id.clone(),
                lane: serde_json::to_string(lane).unwrap_or_default(),
                reason: reason.clone(),
                project_id,
            })
        }
        ExecEvent::ExecutionCompleted { color, outputs, .. } => {
            Some(DispatcherEvent::ExecutionCompleted {
                color: *color,
                outputs: outputs.clone(),
                project_id,
            })
        }
        ExecEvent::ExecutionFailed { color, error, .. } => {
            Some(DispatcherEvent::ExecutionFailed {
                color: *color,
                error: error.clone(),
                project_id,
            })
        }
        ExecEvent::ExecutionCancelled { color, reason, .. } => {
            Some(DispatcherEvent::ExecutionCancelled {
                color: *color,
                reason: reason.clone(),
                project_id,
            })
        }
        ExecEvent::CostReported { color, service, amount_usd, .. } => {
            Some(DispatcherEvent::CostReported {
                color: *color,
                project_id,
                service: service.clone(),
                amount_usd: *amount_usd,
            })
        }
        // Pulse* / SuspensionRegistered / SuspensionResolved /
        // LogLine / RunOutput / PulseSeeded: not currently surfaced
        // through DispatcherEvent. SSE consumers don't need them
        // for live UI; they read the journal directly when they
        // want full detail.
        _ => None,
    }
}
