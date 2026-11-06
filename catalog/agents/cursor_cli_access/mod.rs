//! CursorCliAccess: the ONE place a Cursor CLI login is picked; the marker passes
//! on `access` to the CursorCli nodes, which hand its values to the agent. The
//! service recipe lives once, in metadata.json.

weft::access_node!(CursorCliAccessNode);
