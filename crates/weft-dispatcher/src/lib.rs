//! The weft dispatcher role.
//!
//! Owns:
//! - Event routing (webhook URLs, form URLs, cron, infra events).
//! - Handing executions to the project's workers (`delivery`, through
//!   the platform's `Runner`).
//! - Infrastructure orchestration: the `infra_lifecycle_command` table the
//!   project's supervisor claims, and a bridge that fans supervisor-emitted
//!   `infra_event` rows out over SSE.
//! - Journal (Postgres-backed; `weft-journal` crate).
//! - Cost aggregation.
//!
//! Does NOT execute user node code. Workers run the user's compiled
//! binary; node trait impls live inside that binary. Does NOT do runtime
//! health probing of infra; that's the supervisor's job.

pub mod activation_store;
pub mod api;
pub mod app;
pub mod authenticator;
pub mod build;
pub mod delivery;
pub mod domains;
pub mod door;
pub mod frontends;
pub mod held;
pub mod holders;
pub mod entry_limits;
pub mod display_feeds;
pub mod events;
pub mod infra_event;
pub mod infra_event_bridge;
pub mod infra_lifecycle_command;
pub mod infra_door;
pub mod infra_node;
pub mod infra_owner;
pub mod journal;
pub mod journal_bridge;
pub mod lease;
pub mod lifecycle_claimer;
pub mod listener;
pub mod live_relay;
pub mod proxy;
pub mod instance_values;
pub mod install_picks;
pub mod projection;
pub mod project_store;
pub mod reaper;
pub mod reclaim;
pub mod settled;
pub mod state;
pub mod broker_admin;
pub mod role_client;
pub mod storage;
pub mod take_down;
pub mod task_kinds;
pub mod tenant;
pub mod transition;
pub mod versions;

/// Dispatcher-side aliases over the shared task-store surface.
/// Executors `impl TaskExecutor<DispatcherState>` directly using the
/// trait from `weft_task_store::executor`.
pub mod task_executor {
    use crate::state::DispatcherState;

    pub type TaskRegistry = weft_task_store::executor::TaskRegistry<DispatcherState>;
    pub type TaskRegistryBuilder =
        weft_task_store::executor::TaskRegistryBuilder<DispatcherState>;
}

pub use events::{DispatcherEvent, EventBus};
pub use project_store::{
    PostgresProjectStore, ProjectStatus as StoreStatus, ProjectStore, ProjectStoreOps,
};
#[cfg(any(test, feature = "test-helpers"))]
pub use project_store::FakeProjectStore;
pub use state::DispatcherState;
