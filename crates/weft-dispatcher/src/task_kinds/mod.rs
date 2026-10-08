//! Concrete task-kind executors. Each module here defines one
//! `TaskExecutor` impl plus the typed payload struct producers
//! serialize into the task's JSON payload.
//!
//! The kind name lives in this module's `KIND` constant so producers
//! enqueue with the same string the registry uses to look up the
//! executor.

pub mod fire_signal;
pub mod program_call;
pub mod register_signal;
pub mod run_node_test;
pub mod stop_tagged;
pub mod withdraw_signal;

// Only the executor unit structs are re-exported because main.rs
// instantiates them when wiring the registry. Concrete payload /
// result types are imported directly by their producers.
pub use fire_signal::FireSignalExecutor;
pub use program_call::ProgramCallExecutor;
pub use register_signal::RegisterSignalExecutor;
pub use run_node_test::RunNodeTestExecutor;
pub use stop_tagged::StopTaggedExecutor;
pub use withdraw_signal::WithdrawSignalExecutor;
