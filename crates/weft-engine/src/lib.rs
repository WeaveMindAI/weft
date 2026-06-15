//! Weft execution engine, linked into each compiled project binary.
//! Connects to the broker, folds the journal, drives one execution
//! to completion, writes journal events through the broker. All
//! control-plane round-trips (`await_signal`, `register_signal`,
//! `ApplyInfra`) flow through the dispatcher's task queue (also via
//! the broker).

pub(crate) mod context;
pub(crate) mod execution_driver;
pub(crate) mod loop_runtime;
pub mod run_pod;
pub mod storage;

pub use context::EngineClients;
pub use run_pod::run_pod;
pub use storage::{WorkerStorage, WorkerStorageOps};

/// Wall-clock seconds since the UNIX epoch, for `at_unix` event
/// timestamps (observational metadata, not control-flow deadlines:
/// those use the injected `Clock`). One definition for the crate.
pub(crate) fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock past UNIX_EPOCH")
        .as_secs()
}
