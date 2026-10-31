//! Postgres-backed task queue shared by the dispatcher, the broker and the
//! engine. Both sides go through the same SQL
//! helpers; the boot applies the schema via `schema_guard::apply_schema`
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
//!   - `install_address`: the address the install hands out in its
//!     links, its domain once that domain's certificate is issued.
//!   - `schema_group`: what a module declares about its tables.
//!   - `schema_guard`: the schema runner every boot routes its
//!     `SchemaGroup`s through. It builds a new database from the canonical
//!     `CREATE TABLE` text and carries an existing one forward with the
//!     group's migration files.
//!   - `documents`: the same for the JSON a column holds: the rows an
//!     older weft wrote, carried to the shape the code reads today.
//!
//! `schema_guard` and `documents` are behind the `migrate` feature: only a
//! role that boots a database applies the schema, and a worker, which
//! links this crate, must not carry the migration history (see
//! `schema_group`).

// What `#[weft_db_macros::test]` expands to names this crate by its path,
// which this crate's own tests reach this way.
#[cfg(test)]
extern crate self as weft_task_store;

pub mod alarm;
pub mod announce;
pub mod budget;
pub mod db;
#[cfg(feature = "migrate")]
pub mod documents;
pub mod drain;
pub mod executor;
pub mod held_copy;
pub mod infra_command;
pub mod infra_copies;
pub mod install_address;
pub mod kinds;
pub mod locks;
pub mod parked_fires;
pub mod pg_signal;
pub mod runs;
pub mod schema_group;
#[cfg(feature = "migrate")]
pub mod schema_guard;
pub mod tasks;
pub mod terminal;
pub mod throttle;
pub mod traits;
pub mod unanswered;
pub mod worker_door;

#[cfg(feature = "migrate")]
pub use documents::{apply_documents, Document};

pub use schema_group::{DataStep, SchemaGroup, StepCode, StepWork, Until};
#[cfg(feature = "migrate")]
pub use schema_guard::{apply_groups, apply_schema, Migration, Part};

pub use executor::{dispatcher_picker_loop, TaskExecutor, TaskRegistry};
pub use kinds::{FireSignalPayload, RegisterSignalPayload, RemovalWaitPayload, ResyncPayload, RunNodeTestPayload, StopTaggedPayload, TakeDownWaitPayload, TaskKind, WithdrawSignalPayload};
pub use tasks::{
    claim_duration_secs, claim_heartbeat_interval, claim_next, complete, enqueue, enqueue_dedup, fail, heartbeat, Claim,
    DedupOutcome, NewTask, Task, TaskOutcome, TaskStatus, TERMINAL_RETENTION_SECS,
};
pub use traits::{InfraReader, PostgresInfraReader, PostgresTaskStoreClient, StoredEndpoint, TaskStoreClient};
