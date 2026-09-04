//! Weft execution engine, linked into each compiled project binary.
//! Connects to the broker, folds the journal, drives one execution
//! to completion, writes journal events through the broker. Its
//! control-plane round-trips (`await_signal`, `register_signal`)
//! flow through the dispatcher's task queue (also via the broker).

pub(crate) mod caller_conn;
pub(crate) mod context;
pub(crate) mod execution_driver;
pub(crate) mod loop_runtime;
pub(crate) mod metering;
pub(crate) mod socket;
pub(crate) mod stream_runtime;
pub(crate) mod wait_tracker;
pub mod run_pod;
pub mod storage;
// The node-test rig + runner. Feature-gated so ONLY the emitted
// per-package test crate compiles them; a worker binary carries no
// test machinery.
#[cfg(feature = "node-tests")]
pub mod test_rig;
#[cfg(feature = "node-tests")]
pub mod test_runner;

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

/// The same clock in milliseconds, for a log line: two nodes that
/// log in the same second still read back in the order they wrote.
pub(crate) fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock past UNIX_EPOCH")
        .as_millis() as u64
}
