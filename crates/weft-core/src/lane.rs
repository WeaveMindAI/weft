use serde::{Deserialize, Serialize};

/// One level of sub-dimension within a color. Expansions and loop
/// iterations extend the lane vector; gathers shorten it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LaneFrame {
    pub count: u32,
    pub index: u32,
}

/// The path through nested expansions/loops for a pulse. Empty = the
/// root dimension. `[3]` = fourth element of one expansion.
/// `[3, 0]` = first iteration/element inside lane `[3]`.
pub type Lane = Vec<LaneFrame>;
