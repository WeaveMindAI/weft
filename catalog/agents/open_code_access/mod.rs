//! OpenCodeAccess: the ONE place a OpenCode login is picked; the marker passes
//! on `access` to the OpenCode nodes, which hand its values to the agent. The
//! service recipe lives once, in metadata.json.

weft::access_node!(OpenCodeAccessNode);
