//! OpenClawAccess: the ONE place a OpenClaw login is picked; the marker passes
//! on `access` to the OpenClaw nodes, which hand its values to the agent. The
//! service recipe lives once, in metadata.json.

weft::access_node!(OpenClawAccessNode);
