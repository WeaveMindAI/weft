//! The weft dispatcher daemon.
//!
//! Owns:
//! - Event routing (webhook URLs, form URLs, cron, infra events).
//! - Worker lifecycle (via pluggable `WorkerBackend`).
//! - Infrastructure orchestration (via pluggable `InfraBackend`).
//! - Journal (via restate).
//! - Ops dashboard (HTTP + SSE).
//! - Cost aggregation.
//!
//! Does NOT execute user node code. Workers run the user's compiled
//! binary; node trait impls live inside that binary.

pub mod api;
pub mod backend;
pub mod config;
pub mod journal;
pub mod project_store;
pub mod state;

pub use config::DispatcherConfig;
pub use project_store::{ProjectStore, ProjectStatus as StoreStatus};
pub use state::DispatcherState;
