//! KiloCodeAccess: the ONE place a Kilo Code login is picked; the marker passes
//! on `access` to the KiloCode nodes, which hand its values to the agent. The
//! service recipe lives once, in metadata.json.

weft::access_node!(KiloCodeAccessNode);
