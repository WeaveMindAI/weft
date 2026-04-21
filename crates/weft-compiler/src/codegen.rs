//! Codegen. Given a validated+enriched graph plus the set of referenced
//! nodes (stdlib + user + vendor), emit rust source files that:
//!
//! 1. Instantiate the graph as a static structure.
//! 2. Declare each referenced node module.
//! 3. Link everything into a binary with a `main` that runs the weft
//!    pulse loop.
//! 4. Emit the necessary restate client wiring.
//!
//! The emitted output is a standard cargo crate rooted in
//! `.weft/target/build/` that cargo then compiles into the final
//! binary.
//!
//! Phase A2: implement emission. The template pattern is
//! well-established in v1; replicate the shape.

use std::path::Path;

use crate::error::CompileResult;

pub fn emit(_project_root: &Path, _target_root: &Path) -> CompileResult<()> {
    // Phase A2 target.
    Ok(())
}
