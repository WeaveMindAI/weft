//! KiroCliAccess: the ONE place a Kiro CLI login is picked; the marker passes
//! on `access` to the KiroCli nodes, which hand its values to the agent. The
//! service recipe lives once, in metadata.json.

weft::access_node!(KiroCliAccessNode);
