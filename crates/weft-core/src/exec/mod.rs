//! Pure execution algorithms. Stateless functions operating on
//! `PulseTable` + `NodeExecutionTable` + project metadata. No IO, no
//! journal, no HTTP. The runtime crate (inside user binaries) calls
//! these; the dispatcher also calls them for simulation/dry-run.

pub mod execution;
pub mod emission;
pub mod ready;
pub mod postprocess;
pub mod skip;
pub mod completion;
pub mod typecheck;

pub use execution::{NodeExecution, NodeExecutionStatus, NodeExecutionTable, PortWarning};
pub use emission::PulseEmission;
pub use ready::{find_ready_nodes, ReadyGroup};
pub use postprocess::{close_unmentioned_downstream, postprocess_output};
pub use skip::check_should_skip;
pub use completion::check_completion;
