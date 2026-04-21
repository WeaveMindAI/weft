//! Pure execution algorithms. Stateless functions operating on
//! `PulseTable` + `NodeExecutionTable` + project metadata. No IO, no
//! journal, no HTTP. The runtime crate (inside user binaries) calls
//! these; the dispatcher also calls them for simulation/dry-run.
//!
//! Ported from `crates-v1/weft-core/src/executor_core.rs` but split
//! across focused modules to avoid the 1700-line monolith.

pub mod execution;
pub mod preprocess;
pub mod ready;
pub mod postprocess;
pub mod skip;
pub mod completion;
pub mod typecheck;

pub use execution::{NodeExecution, NodeExecutionStatus, NodeExecutionTable};
pub use preprocess::preprocess_input;
pub use ready::{find_ready_nodes, ReadyGroup};
pub use postprocess::postprocess_output;
pub use skip::check_should_skip;
pub use completion::check_completion;
