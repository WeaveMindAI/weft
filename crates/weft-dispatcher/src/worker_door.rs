//! What the dispatcher keeps of the workers' doors: which worker processes
//! are alive and what each counted this minute (the tables are
//! `weft_task_store::worker_door`'s).
//!
//! A running run's owner is a worker process, and the worker's lease is
//! what says it is still being driven. A run whose owner's lease ran out is
//! lost, and [`sweep_lost_runs`] lets go of it.

use anyhow::{Context, Result};
use sqlx::PgPool;


/// How long the counts of a minute are kept after it began: `weft status`
/// reads the refusals of this minute and the one before.
const COUNTS_KEPT_SECS: i64 = 180;

/// How long a lapsed lease row is kept: long enough that every run its
/// process owned was swept first.
const LEASES_KEPT_SECS: i64 = 3600;

/// Drop counts no limit reads any more, and leases long lapsed.
pub async fn sweep(pool: &PgPool, now: i64) -> Result<()> {
    sqlx::query("DELETE FROM door_count WHERE window_start < $1")
        .bind(now - COUNTS_KEPT_SECS)
        .execute(pool)
        .await
        .context("drop old door counts")?;
    sqlx::query("DELETE FROM worker_lease WHERE leased_until_unix < $1")
        .bind(now - LEASES_KEPT_SECS)
        .execute(pool)
        .await
        .context("drop old worker leases")?;
    Ok(())
}


/// How far past its lease a worker's runs are left alone before they are
/// read as lost: a worker that missed a few ticks under load is not gone.
fn lost_margin_secs() -> i64 {
    weft_core::time_scale::scaled_secs(weft_task_store::worker_door::WORKER_LEASE_SECS)
}

/// How many lost runs one sweep lets go of; a full one sweeps again at its
/// next look.
const LOST_BATCH: i64 = 500;

/// Let go of the runs whose owner went away (its lease ran out more than
/// [`lost_margin_secs`] ago): a durable run is queued again, a fast run
/// ends (`crate::journal::Journal::let_go_of_lost`). Each run is taken
/// under its own row lock, so a batch its old owner sends meanwhile and
/// this sweep never write the run at once, and the epoch this raises
/// refuses whichever of that owner's batches comes after.
pub async fn sweep_lost_runs(state: &crate::state::DispatcherState, now: i64) -> Result<()> {
    let lapsed_before = now - lost_margin_secs();
    let lost: Vec<(weft_core::ExecutionId, String)> = sqlx::query_as(
        "SELECT r.execution_id, r.keeping FROM run r \
         WHERE r.state = 'running' \
           AND NOT EXISTS (SELECT 1 FROM worker_lease l WHERE l.replica = r.owner AND l.leased_until_unix >= $1) \
         ORDER BY r.execution_id LIMIT $2",
    )
    .bind(lapsed_before)
    .bind(LOST_BATCH)
    .fetch_all(&state.pg_pool)
    .await
    .context("find runs whose worker went away")?;
    for (execution_id, keeping) in lost {
        // Only a fast run's ending folds its per-node cancels.
        let program = match keeping == weft_core::run_settings::Keeping::Durable.as_str() {
            true => Ok(None),
            false => crate::api::execution::program_for_cancel(state, execution_id).await,
        };
        let let_go = match program {
            Ok(program) => state.journal.let_go_of_lost(execution_id, lapsed_before, program.as_deref()).await,
            Err(e) => Err(e),
        };
        match let_go {
            Ok(crate::journal::Lost::NotLost) => {}
            Ok(lost) => tracing::warn!(target: "weft_dispatcher::worker_door", %execution_id, ?lost, "a run whose worker went away was let go of"),
            Err(e) => tracing::warn!(
                target: "weft_dispatcher::worker_door",
                %execution_id, error = %format!("{e:#}"),
                "could not let go of a run whose worker went away; the next sweep tries again"
            ),
        }
    }
    Ok(())
}
