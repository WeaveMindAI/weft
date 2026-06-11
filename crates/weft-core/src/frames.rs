//! Frame stack for pulse keying. Each pulse carries a `LoopFrames`
//! stack that identifies which iteration of which (nested) loop the
//! firing belongs to. Empty stack = a firing at the root (not inside
//! any loop). Used by `ready::find_ready_nodes` to match pulses across
//! required ports at the same frame stack.

use serde::{Deserialize, Serialize};

/// One iteration of one nested loop level the firing is inside.
// SYNC: LoopIteration <-> extension-vscode/src/shared/protocol.ts LoopIteration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LoopIteration {
    pub index: u32,
}

/// Stack of nested-loop iterations the firing is inside. Empty = the
/// root (not inside any loop). `[3]` = fourth iteration of one loop.
/// `[3, 0]` = first iteration of an inner loop, inside the fourth
/// iteration of an outer loop.
pub type LoopFrames = Vec<LoopIteration>;
