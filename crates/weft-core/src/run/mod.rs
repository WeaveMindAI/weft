//! Pure run algorithms. Stateless functions operating on
//! `PulseTable` + `NodeFiringTable` + project metadata. No IO, no
//! journal, no HTTP. The runtime crate (inside user binaries) calls
//! these; the journal fold calls the same ones to rebuild a
//! run from its rows, so a resumed worker holds what the live
//! one held.

pub mod cancel;
pub mod completion;
pub mod emission;
pub mod firing;
#[cfg(feature = "runtime")]
pub mod loop_runtime;
pub mod postprocess;
pub mod ready;
pub mod run_kind;
pub mod scope;
pub mod skip;
pub mod stuck;

pub use cancel::CancelCause;
pub use completion::check_completion;
pub use emission::PulseEmission;
pub use firing::{
    firing_at, firing_at_mut, set_firing_status, NodeFiring, NodeFiringStatus,
    NodeFirings, NodeFiringTable,
};
pub use postprocess::{close_unmentioned_downstream, postprocess_output, OutputBag};
pub use ready::{find_ready_nodes, InputBag, ReadyGroup};
pub use run_kind::RunKind;
pub use skip::check_should_skip;
pub use stuck::{stuck_report, StuckFiring, StuckReport};
