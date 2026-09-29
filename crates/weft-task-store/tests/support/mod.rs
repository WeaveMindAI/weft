//! The schema every database test in this crate stands up: the crate's own
//! task and worker-process groups, plus a stand-in for the journal table the
//! ownership trigger writes.

use std::sync::Arc;

use sqlx::PgPool;
use weft_task_store::pg_signal::PgSignalWatch;
use weft_task_store::tasks;

/// Every channel this crate's writes notify on. (Each test file
/// compiles this module; the ones that never wait leave these unused.)
#[allow(dead_code)]
pub const CHANNELS: &[&str] = &[tasks::TASK_READY_CHANNEL, weft_task_store::terminal::TERMINAL_CHANNEL];

/// A signal watch on the test database, listening on [`CHANNELS`].
#[allow(dead_code)]
pub async fn signals(pool: &PgPool) -> Arc<PgSignalWatch> {
    PgSignalWatch::start(pool, CHANNELS).await.expect("start the signal watch")
}

/// Apply the task and worker-process schema to a fresh test database.
pub async fn setup(pool: &PgPool) {
    weft_task_store::apply_groups(pool, &[&tasks::GROUP])
        .await
        .expect("tasks schema");
    // `tasks::GROUP` creates a trigger ON `task` that stamps
    // `execution.owner_instance` on claim (ownership-follows-claim).
    // Same situation as exec_event: the table is the dispatcher journal's,
    // created before task-store migrate in production. Stand up a minimal
    // `execution` so the trigger has a row to update; seed a row per
    // execution the tests claim so the stamp is observable. SYNC: keep columns
    // in step with crates/weft-dispatcher/src/journal/postgres.rs
    // `execution`.
    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS execution (
            execution_id TEXT PRIMARY KEY,
            project_id UUID NOT NULL,
            tenant_id TEXT NOT NULL,
            started_at_unix BIGINT NOT NULL,
            phase TEXT NOT NULL,
            owner_instance TEXT
        )"#,
    )
    .execute(pool)
    .await
    .expect("execution stub");
}
