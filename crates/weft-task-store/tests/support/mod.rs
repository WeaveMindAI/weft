//! The schema every database test in this crate stands up: the crate's own
//! task and worker-pod groups, plus stand-ins for the two journal tables
//! the worker-pod triggers attach to.

use std::sync::Arc;

use sqlx::PgPool;
use weft_task_store::pg_signal::PgSignalWatch;
use weft_task_store::tasks;
use weft_task_store::worker_pod;

/// Every channel this crate's writes notify on. (Each test file
/// compiles this module; the ones that never wait leave these unused.)
#[allow(dead_code)]
pub const CHANNELS: &[&str] = &[
    tasks::TASK_READY_CHANNEL,
    weft_task_store::terminal::TERMINAL_CHANNEL,
    worker_pod::WORKER_POD_CHANNEL,
];

/// A signal watch on the test database, listening on [`CHANNELS`].
#[allow(dead_code)]
pub async fn signals(pool: &PgPool) -> Arc<PgSignalWatch> {
    PgSignalWatch::start(pool, CHANNELS).await.expect("start the signal watch")
}

/// Apply the task and worker-pod schema to a fresh test database.
pub async fn setup(pool: &PgPool) {
    weft_task_store::apply_groups(pool, &[&tasks::GROUP])
        .await
        .expect("tasks schema");
    // `worker_pod::GROUP` creates a fencing trigger ON `exec_event`, a
    // table owned by the dispatcher's journal layer (created at journal
    // connect, which runs before the task-store groups in production). These
    // task-store tests never touch the journal, so we stand up a minimal
    // `exec_event` table (matching the dispatcher's columns) purely so the
    // trigger DDL has a table to attach to. SYNC: keep these columns in step
    // with crates/weft-dispatcher/src/journal/postgres.rs `exec_event`.
    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS exec_event (
            id BIGSERIAL PRIMARY KEY,
            color TEXT NOT NULL,
            kind TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            created_at BIGINT NOT NULL,
            pod_name TEXT,
            dedup_key TEXT,
            writer_xid XID8 NOT NULL DEFAULT pg_current_xact_id()
        )"#,
    )
    .execute(pool)
    .await
    .expect("exec_event stub");
    // `worker_pod::GROUP` also creates a trigger ON `task` that stamps
    // `execution_color.owner_pod_name` on claim (ownership-follows-claim).
    // Same situation as exec_event: the table is the dispatcher journal's,
    // created before task-store migrate in production. Stand up a minimal
    // `execution_color` so the trigger has a row to update; seed a row per
    // color the tests claim so the stamp is observable. SYNC: keep columns
    // in step with crates/weft-dispatcher/src/journal/postgres.rs
    // `execution_color`.
    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS execution_color (
            color TEXT PRIMARY KEY,
            project_id UUID NOT NULL,
            tenant_id TEXT NOT NULL,
            started_at_unix BIGINT NOT NULL,
            phase TEXT NOT NULL,
            owner_pod_name TEXT
        )"#,
    )
    .execute(pool)
    .await
    .expect("execution_color stub");
    weft_task_store::apply_groups(pool, &[&worker_pod::GROUP])
        .await
        .expect("worker_pod schema");
}
