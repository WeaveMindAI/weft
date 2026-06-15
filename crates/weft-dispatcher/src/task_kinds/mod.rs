//! Concrete task-kind executors. Each module here defines one
//! `TaskExecutor` impl plus the typed payload struct producers
//! serialize into the task's JSON payload.
//!
//! The kind name lives in this module's `KIND` constant so producers
//! enqueue with the same string the registry uses to look up the
//! executor.

pub mod ensure_storage_box;
pub mod execute;
pub mod fire_signal;
pub mod record_cost;
pub mod record_log;
pub mod register_signal;
pub mod route_entry;
pub mod spawn_pod;

// Only the executor unit structs are re-exported because main.rs
// instantiates them when wiring the registry. Concrete payload /
// result types are imported directly by their producers (e.g.
// `cold_start.rs` imports `SpawnPodPayload` from the submodule).
pub use ensure_storage_box::EnsureStorageBoxExecutor;
pub use fire_signal::FireSignalExecutor;
pub use record_cost::RecordCostExecutor;
pub use record_log::RecordLogExecutor;
pub use register_signal::RegisterSignalExecutor;
pub use route_entry::RouteEntryExecutor;
pub use spawn_pod::SpawnPodExecutor;
