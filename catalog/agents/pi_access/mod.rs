//! PiAccess: the ONE place a Pi login is picked; the marker passes
//! on `access` to the Pi nodes, which hand its values to the agent. The
//! service recipe lives once, in metadata.json.

weft::access_node!(PiAccessNode);
