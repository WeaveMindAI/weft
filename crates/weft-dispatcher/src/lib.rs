//! The weft dispatcher role.
//!
//! Owns:
//! - Event routing: the install's public door (`door`), and every event
//!   that comes through it (a call at a webhook or form URL, a provider's
//!   push), which the listener says what it is and the dispatcher hands to
//!   its entry's worker door or its waiting run (`api::signal`). A timer
//!   (a cron) is not routed here: the platform's alarm wakes the listener,
//!   which hands its fire over itself (`weft_listener::fire_sink`).
//! - Handing runs to the project's workers (`delivery`, through
//!   the platform's `Runner`).
//! - Infrastructure orchestration: the `infra_lifecycle_command` table the
//!   project's supervisor claims, and a bridge that fans supervisor-emitted
//!   `infra_event` rows out over SSE.
//! - The run records (`run`, `run_log`; the `weft-journal` crate): what
//!   the dispatcher writes into runs nobody drives, their live view, what a
//!   run's ending leaves to do, search, retention.
//! - Cost aggregation.
//!
//! Does NOT execute user node code. Workers run the user's compiled
//! binary; node trait impls live inside that binary. Does NOT do runtime
//! health probing of infra; that's the supervisor's job.

pub mod actions;
pub mod activation_store;
pub mod api;
pub mod app;
pub mod authenticator;
pub mod build;
pub mod delivery;
pub mod domains;
pub mod drain;
pub mod infra_reach;
pub mod door;
pub mod frontends;
pub mod held;
pub mod holders;
pub mod entry_limits;
pub mod files_door;
pub mod folder_sync;
pub mod display_feeds;
pub mod events;
pub mod infra_event;
pub mod infra_event_bridge;
pub mod infra_lifecycle_command;
pub mod infra_door;
pub mod infra_node;
pub mod infra_owner;
pub mod lapses;
pub mod lease;
pub mod lifecycle_claimer;
pub mod listener;
pub mod live_relay;
pub mod live_view;
pub mod maintenance;
pub mod needs;
pub mod proxy;
pub mod instance_values;
pub mod install_picks;
pub mod node_state;
pub mod painting;
pub mod projection;
pub mod project_store;
pub mod front;
pub mod parked_drain;
pub mod project_ports;
pub mod reaper;
pub mod run_ends;
pub mod reclaim;
pub mod retention;
pub mod search_index;
pub mod state;
pub mod store;
pub mod broker_admin;
pub mod role_client;
pub mod storage;
pub mod take_down;
pub mod task_kinds;
pub mod tenant;
pub mod transition;
pub mod versions;
pub mod working_copies;
pub mod worker_door;
pub mod worker_fire;

/// Dispatcher-side aliases over the shared task-store surface.
/// Executors `impl TaskExecutor<DispatcherState>` directly using the
/// trait from `weft_task_store::executor`.
pub mod task_executor {
    use crate::state::DispatcherState;

    pub type TaskRegistry = weft_task_store::executor::TaskRegistry<DispatcherState>;
}

pub use events::{DispatcherEvent, EventBus};
pub use project_store::{
    PostgresProjectStore, ProjectStatus as StoreStatus, ProjectStore, ProjectStoreOps,
};
pub use state::DispatcherState;
