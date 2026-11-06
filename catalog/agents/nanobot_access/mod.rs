//! NanobotAccess: the ONE place a nanobot login is picked; the marker passes
//! on `access` to the Nanobot nodes, which hand its values to the agent. The
//! service recipe lives once, in metadata.json.

weft::access_node!(NanobotAccessNode);
