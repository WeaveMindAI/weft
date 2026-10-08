//! Postgres-backed task queue shared by the dispatcher, the broker and the
//! engine. Both sides go through the same SQL
//! helpers; the boot applies the schema via `schema_guard::apply_groups`
//! over each module's `GROUP`.
//!
//! Modules:
//!   - `db`: connecting to the database; every pool comes from here.
//!   - `alarm`: the wakes a local install has set and not yet
//!     delivered.
//!   - `tasks`: the `task` table, the dispatcher's work queue
//!     (enqueue, claim, heartbeat, complete, fail, sweep).
//!   - `executor`: the `TaskExecutor` trait and the dispatcher's picker
//!     loop.
//!   - `runs`: the one rule for a run being worked on, and how a cancel
//!     reaches the worker driving a run.
//!   - `drain`: the wake-and-drain loop every role's background work
//!     runs, on the machine or, scaled to zero, once per tick.
//!   - `pg_signal`: the process's one Postgres `LISTEN` connection,
//!     which every wait on a row sleeps on (`terminal` is the task
//!     waiter built on it).
//!   - `announce`: how a write announces itself without every commit
//!     waiting on every other (an outbox, flushed in batches).
//!   - `unanswered`: weft's own calls that keep failing, kept for
//!     whoever waits on the work behind them to see why.
//!   - `held_copy`: a process's copy of rows read on every request,
//!     dropped the moment a notification says they changed.
//!   - `schema_guard`: the schema runner every boot routes its
//!     `SchemaGroup`s through. It builds a new database from the canonical
//!     `CREATE TABLE` text and carries an existing one forward with the
//!     group's migration files.

pub mod alarm;
pub mod announce;
pub mod db;
pub mod drain;
pub mod executor;
pub mod held_copy;
pub mod infra_copies;
pub mod kinds;
pub mod locks;
pub mod parked_fires;
pub mod pg_signal;
pub mod runs;
pub mod schema_guard;
pub mod tasks;
pub mod terminal;
pub mod traits;
pub mod unanswered;
pub mod worker_door;

pub use schema_guard::{apply_groups, Migration, SchemaGroup};

pub use executor::{dispatcher_picker_loop, TaskExecutor, TaskRegistry, TaskRegistryBuilder};
pub use kinds::{FireSignalPayload, StopTaggedPayload, TaskKind, WithdrawSignalPayload};
pub use tasks::{
    claim_duration_secs, claim_heartbeat_interval, claim_one, complete, enqueue, enqueue_dedup, fail, heartbeat, sweep_terminal,
    DedupOutcome, NewTask, Task, TaskOutcome, TaskStatus, TERMINAL_RETENTION_SECS,
};
pub use traits::{InfraReader, PostgresInfraReader, PostgresTaskStoreClient, TaskStoreClient};
