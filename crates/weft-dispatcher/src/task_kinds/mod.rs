//! Concrete task-kind executors. Each module here defines one
//! `TaskExecutor` impl for one `weft_task_store::TaskKind`.
//!
//! The kinds and their payloads live in `weft_task_store::kinds`, so a
//! producer in another crate (the listener's `FireSignal`, a broker
//! caller) enqueues the same `TaskKind` and payload type the executor
//! here reads; `crate::app::task_registry` maps every kind to its
//! executor.

pub mod activate;
pub mod cancel_call;
pub mod fire_signal;
pub mod maintenance_lift;
pub mod program_call;
pub mod register_signal;
pub mod removal_wait;
pub mod resync;
pub mod run_node_test;
pub mod start_run;
pub mod stop_tagged;
pub mod take_down_wait;
pub mod withdraw_signal;

// Only the executor unit structs are re-exported, for
// `crate::app::task_registry` to wire into the registry.
pub use activate::ActivateExecutor;
pub use cancel_call::CancelCallExecutor;
pub use fire_signal::FireSignalExecutor;
pub use maintenance_lift::MaintenanceLiftExecutor;
pub use program_call::ProgramCallExecutor;
pub use register_signal::RegisterSignalExecutor;
pub use removal_wait::RemovalWaitExecutor;
pub use resync::ResyncExecutor;
pub use run_node_test::RunNodeTestExecutor;
pub use start_run::StartRunExecutor;
pub use stop_tagged::StopTaggedExecutor;
pub use take_down_wait::TakeDownWaitExecutor;
pub use withdraw_signal::WithdrawSignalExecutor;
