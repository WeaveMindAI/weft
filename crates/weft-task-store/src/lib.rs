//! Postgres-backed task queue shared by the dispatcher, the broker and the
//! engine. Both sides go through the same SQL
//! helpers; the boot applies the schema via `schema_guard::apply_groups`
//! over each module's `GROUP`.
//!
//! Modules:
//!   - `db`: connecting to the database; every pool comes from here.
//!   - `alarm`: the wakes a local install has set and not yet
//!     delivered.
//!   - `tasks`: the `task` table (enqueue, claim, heartbeat,
//!     complete, fail, sweep), and the trigger that makes an execution's
//!     owner follow its task's claim.
//!   - `executor`: `TaskExecutor` and `WorkerTaskKind` traits, the
//!     dispatcher's picker loop, and the worker's run of one claimed
//!     task.
//!   - `drain`: the wake-and-drain loop every role's background work
//!     runs, on the machine or, scaled to zero, once per tick.
//!   - `pg_signal`: the process's one Postgres `LISTEN` connection,
//!     which every wait on a row sleeps on (`terminal` is the task
//!     waiter built on it).
//!   - `schema_guard`: the schema runner every boot routes its
//!     `SchemaGroup`s through. It builds a new database from the canonical
//!     `CREATE TABLE` text and carries an existing one forward with the
//!     group's migration files.

pub mod alarm;
pub mod db;
pub mod drain;
pub mod executor;
pub mod kinds;
pub mod locks;
pub mod pg_signal;
pub mod schema_guard;
pub mod tasks;
pub mod terminal;
pub mod traits;

pub use schema_guard::{apply_groups, Migration, SchemaGroup};

pub use executor::{
    dispatcher_picker_loop, run_claimed_worker_task, TaskEnd, TaskExecutor, TaskRegistry, TaskRegistryBuilder,
};
pub use kinds::{
    CancelExecutionPayload, ExecutionPayload, FireSignalPayload, RecordCostPayload,
    RecordLogPayload, StopTaggedPayload, TaskKind,
};
pub use tasks::{
    bind_execution_id_owner, claim_one, complete, enqueue, enqueue_dedup, fail, heartbeat, sweep_terminal, take_deliveries,
    CancelAsked, ClaimFilter, DedupOutcome, Delivery, NewTask, Task, TaskOutcome, TaskStatus,
    TaskTarget, claim_duration_secs, claim_heartbeat_interval, TERMINAL_RETENTION_SECS,
};
pub use traits::{InfraReader, PostgresInfraReader, PostgresTaskStoreClient, TaskStoreClient};
