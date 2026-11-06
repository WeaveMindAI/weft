//! GoogleAntigravityAccess: the ONE place a Google Antigravity login is picked; the marker passes
//! on `access` to the GoogleAntigravity nodes, which hand its values to the agent. The
//! service recipe lives once, in metadata.json.

weft::access_node!(GoogleAntigravityAccessNode);
