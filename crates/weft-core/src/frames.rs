//! Frame stack for pulse keying. Each pulse carries a `LoopFrames`
//! stack that identifies which iteration of which (nested) loop the
//! firing belongs to. Empty stack = a firing at the root (not inside
//! any loop). Used by `ready::find_ready_nodes` to match pulses across
//! required ports at the same frame stack.

use serde::{Deserialize, Serialize};

/// One iteration of one nested loop level the firing is inside.
// SYNC: LoopIteration <-> packages/weft-graph/src/protocol.ts LoopIteration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LoopIteration {
    pub index: u32,
}

/// Stack of nested-loop iterations the firing is inside. Empty = the
/// root (not inside any loop). `[3]` = fourth iteration of one loop.
/// `[3, 0]` = first iteration of an inner loop, inside the fourth
/// iteration of an outer loop.
pub type LoopFrames = Vec<LoopIteration>;

/// One node EXECUTION: a node id plus the loop frames it fires under
/// (a node inside a loop runs as distinct executions, one per frame).
/// The key for per-execution state: the engine's stuck-check lanes
/// (see `liveness`) and the persisted `awaited_sequences` on
/// `NodeRunState`. Lives here, not in `liveness`, because it is a
/// pure identity the parse-only build needs too.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FiringLocation {
    pub node_id: String,
    pub frames: LoopFrames,
}

impl FiringLocation {
    pub fn new(node_id: impl Into<String>, frames: LoopFrames) -> Self {
        Self { node_id: node_id.into(), frames }
    }
}
