//! Weft execution engine, linked into each compiled project binary. The
//! binary serves HTTP (`worker`): weft calls it per execution, and it folds
//! the journal, drives the execution to its end, and writes journal events
//! through the broker. Its control-plane round-trips (`await_signal`,
//! `register_signal`) flow through the dispatcher's task queue (also via
//! the broker).

pub(crate) mod caller_conn;
pub(crate) mod busy;
pub(crate) mod context;
pub mod door;
pub(crate) mod execution_driver;
pub(crate) mod fired_caller;
pub(crate) mod held;
pub(crate) mod held_waits;
pub(crate) mod journal_writer;
pub(crate) mod metering;
pub(crate) mod plan;
pub mod profile;
pub(crate) mod record_first;
pub(crate) mod socket;
pub(crate) mod stream_runtime;
pub(crate) mod wait_tracker;
pub mod worker;
pub mod storage;
#[cfg(test)]
pub(crate) mod test_record;
// The node-test rig + runner. Feature-gated so ONLY the emitted
// per-package test crate compiles them; a worker binary carries no
// test machinery.
#[cfg(feature = "node-tests")]
pub mod test_rig;
#[cfg(feature = "node-tests")]
pub mod test_runner;

pub use context::EngineClients;
pub use context::ProcessSettings;
pub use journal_writer::WriterSettings;
pub use weft_platform_traits::identity::mint_replica_id;
pub use worker::{identity_from_env, serve, WorkerConfig, WeftCredential};
pub use storage::{WorkerStorage, WorkerStorageOps};
/// The worker binary's global allocator, which the generated `main`
/// declares (`weft_compiler::codegen`): it hands freed memory back to the
/// system, so the memory a worker reads as used (`busy`) is what it uses.
pub use tikv_jemallocator::Jemalloc;

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
