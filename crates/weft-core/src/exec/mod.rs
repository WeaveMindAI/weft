//! Pure execution algorithms. Stateless functions operating on
//! `PulseTable` + `NodeExecutionTable` + project metadata. No IO, no
//! journal, no HTTP. The runtime crate (inside user binaries) calls
//! these; the journal fold calls the same ones to rebuild an
//! execution from its rows, so a resumed worker holds what the live
//! one held.

pub mod boundary;
pub mod cancel;
pub mod completion;
pub mod emission;
pub mod execution;
#[cfg(feature = "runtime")]
pub mod loop_runtime;
pub mod postprocess;
pub mod ready;
pub mod skip;
pub mod stuck;

pub use cancel::CancelCause;
pub use completion::check_completion;
pub use emission::PulseEmission;
pub use execution::{
    latest_firing, latest_firing_mut, next_firing_ordinal, NodeExecution, NodeExecutionStatus,
    NodeExecutionTable, PortWarning,
};
pub use postprocess::{close_unmentioned_downstream, postprocess_output, OutputBag};
pub use ready::{find_ready_nodes, InputBag, ReadyGroup};
pub use skip::check_should_skip;
pub use stuck::{stuck_report, StuckFiring, StuckReport};
