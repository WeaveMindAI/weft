//! Weft execution engine, linked into each compiled project binary.
//! Connects to the broker, folds the journal, drives the pulse loop,
//! writes journal events through the broker. All control-plane
//! round-trips (`await_signal`, `register_signal`,
//! `provision_sidecar`) flow through the dispatcher's task queue
//! (also via the broker).

pub mod context;
pub(crate) mod loop_driver;
pub mod run_pod;

pub use context::EngineClients;
pub use run_pod::run_pod;
