//! MiniMaxCodeAccess: the ONE place a MiniMax Code login is picked; the marker passes
//! on `access` to the MiniMaxCode nodes, which hand its values to the agent. The
//! service recipe lives once, in metadata.json.

weft::access_node!(MiniMaxCodeAccessNode);
