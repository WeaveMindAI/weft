//! The weft dispatcher daemon.
//!
//! Owns:
//! - Event routing (webhook URLs, form URLs, cron, infra events).
//! - Worker lifecycle (via pluggable `WorkerBackend`).
//! - Infrastructure orchestration (via pluggable `InfraBackend`).
//! - Journal (Postgres-backed; `weft-journal` crate).
//! - Ops dashboard (HTTP + SSE).
//! - Cost aggregation.
//!
//! Does NOT execute user node code. Workers run the user's compiled
//! binary; node trait impls live inside that binary.

pub mod api;
pub mod backend;
pub mod cold_start;
pub mod events;
pub mod infra;
pub mod journal;
pub mod journal_bridge;
pub mod lease;
pub mod listener;
pub mod project_store;
pub mod reaper;
pub mod state;
pub mod task_kinds;
pub mod tenant;
pub mod tenant_namespace;

/// Dispatcher-side aliases over the shared task-store surface.
/// Executors `impl TaskExecutor<DispatcherState>` directly using the
/// trait from `weft_task_store::executor`.
pub mod task_executor {
    use crate::state::DispatcherState;

    pub use weft_task_store::executor::run_dispatcher_picker as run_picker_loop;

    pub type TaskRegistry = weft_task_store::executor::TaskRegistry<DispatcherState>;
    pub type TaskRegistryBuilder =
        weft_task_store::executor::TaskRegistryBuilder<DispatcherState>;
}

pub use events::{DispatcherEvent, EventBus};
pub use project_store::{
    PostgresProjectStore, ProjectStatus as StoreStatus, ProjectStore, ProjectStoreOps,
};
#[cfg(any(test, feature = "test-helpers"))]
pub use project_store::MockProjectStore;
pub use state::DispatcherState;
