//! The weft runner. Library form: exports the pulse loop driver so
//! integration tests and embedded use cases can drive it
//! in-process. The binary (`src/main.rs`) wraps this as a CLI.

pub mod context;
pub mod loop_driver;

pub use context::RunnerHandle;
pub use loop_driver::{run_loop, EntryMode, LoopOutcome};
