//! Postgres-backed task queue + worker-pod registry shared by the
//! dispatcher and the engine. Both sides go through the same SQL
//! helpers; the boot applies the schema via `schema_guard::apply_groups`
//! over each module's `GROUP`.
//!
//! Modules:
//!   - `tasks`: the `task` table (enqueue, claim, heartbeat,
//!     complete, fail, sweep).
//!   - `worker_pod`: the `worker_pod` table + journal-fencing trigger
//!     (register, heartbeat, mark_done, list_stale). Its trigger on
//!     `exec_event` requires `exec_event` to exist first, so the
//!     journal schema must be applied BEFORE this group.
//!   - `executor`: `TaskExecutor` and `WorkerTaskKind` traits, plus
//!     the dispatcher and worker picker loops.
//!   - `schema_guard`: the schema runner every boot routes its
//!     `SchemaGroup`s through. It builds a new database from the canonical
//!     `CREATE TABLE` text and carries an existing one forward with the
//!     group's migration files.

pub mod executor;
pub mod kinds;
pub mod schema_guard;
pub mod tasks;
pub mod traits;
pub mod worker_pod;

pub use schema_guard::{apply_groups, Migration, SchemaGroup};

pub use executor::{
    run_dispatcher_picker, run_worker_picker, TaskExecutor, TaskRegistry, TaskRegistryBuilder,
    WorkerTaskKind, WorkerTaskRegistry, WorkerTaskRegistryBuilder,
};
pub use kinds::{
    CancelExecutionPayload, ExecutionPayload, FireSignalPayload, RecordCostPayload,
    RecordLogPayload, SpawnPodPayload, TaskKind, UpdateSignalKindStatePayload,
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
    alive_pod_for_project_full, delete_row, has_live_for_project,
    insert_spawning, list_orphaned_node_test, list_stale, list_stale_spawning, list_terminal,
    mark_dead, mark_done, mark_done_if_idle, register_alive, AliveTransition, PodStatus,
    WorkerPodRow, HEARTBEAT_INTERVAL_SECS, HEARTBEAT_STALE_SECS, SPAWN_BOOT_DEADLINE_SECS,
};
