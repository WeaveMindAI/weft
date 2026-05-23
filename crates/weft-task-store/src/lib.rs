//! Postgres-backed task queue + worker-pod registry shared by the
//! dispatcher and the engine. Both sides go through the same SQL
//! helpers; the dispatcher owns the schema via `migrate()`.
//!
//! Modules:
//!   - `tasks`: the `task` table (enqueue, claim, heartbeat,
//!     complete, fail, sweep).
//!   - `worker_pod`: the `worker_pod` table + journal-fencing trigger
//!     (register, heartbeat, mark_done, list_stale).
//!   - `executor`: `TaskExecutor` and `WorkerTaskKind` traits, plus
//!     the dispatcher and worker picker loops.

pub mod executor;
pub mod kinds;
pub mod tasks;
pub mod traits;
pub mod worker_pod;

/// Run both `task` and `worker_pod` migrations. The fencing trigger
/// on `exec_event` requires `exec_event` to exist first, so the
/// dispatcher's journal migrate must run BEFORE this.
pub async fn migrate(pool: &sqlx::postgres::PgPool) -> anyhow::Result<()> {
    tasks::migrate(pool).await?;
    worker_pod::migrate(pool).await?;
    Ok(())
}

pub use executor::{
    run_dispatcher_picker, run_worker_picker, TaskExecutor, TaskRegistry, TaskRegistryBuilder,
    WorkerTaskKind, WorkerTaskRegistry, WorkerTaskRegistryBuilder,
};
pub use kinds::{
    CancelExecutionPayload, ExecutionPayload,
    FireSignalPayload, RecordCostPayload, RecordLogPayload, SpawnPodPayload, TaskKind,
};
pub use tasks::{
    claim_one, complete, enqueue, enqueue_dedup, fail, heartbeat, sweep_terminal,
    wait_for_terminal, ClaimFilter, DedupOutcome, NewTask, Task, TaskOutcome, TaskStatus,
    TaskTarget, CLAIM_DURATION_SECS, CLAIM_HEARTBEAT_INTERVAL_SECS, TERMINAL_RETENTION_SECS,
};
pub use traits::{
    PostgresTaskStoreClient, PostgresWorkerPodClient, TaskStoreClient, WorkerPodClient,
};
pub use worker_pod::{
    alive_pod_for_project, alive_pod_for_project_full, delete_row, has_live_for_project,
    insert_spawning, list_stale, list_terminal, mark_dead, mark_done, mark_done_if_idle,
    register_alive, WorkerPodRow, HEARTBEAT_INTERVAL_SECS, HEARTBEAT_STALE_SECS,
};
