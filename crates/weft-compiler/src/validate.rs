//! Graph validation. Runs after enrichment to enforce v2 invariants:
//!
//! - Callback isolation (body subgraph has exactly one entry edge and
//!   one exit edge; no leaks to the outer graph).
//! - Entry-point detection (find nodes declaring entry primitives in
//!   their metadata).
//! - Required-port coverage (every required port has either an
//!   incoming edge or a config default).
//! - Type compatibility (edges connect compatible weft types).
//! - Expand/gather discipline (if/when the opt-in primitives land,
//!   default becomes "type error if List[T] meets T without explicit
//!   keyword" per ROADMAP).

use crate::error::CompileResult;
use weft_core::ProjectDefinition;

pub fn validate(_project: &ProjectDefinition) -> CompileResult<()> {
    // Phase A2 target.
    Ok(())
}
