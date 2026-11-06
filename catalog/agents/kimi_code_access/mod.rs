//! KimiCodeAccess: the ONE place a Kimi Code login is picked; the marker passes
//! on `access` to the KimiCode nodes, which hand its values to the agent. The
//! service recipe lives once, in metadata.json.

weft::access_node!(KimiCodeAccessNode);
