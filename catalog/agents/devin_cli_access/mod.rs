//! DevinCliAccess: the ONE place a Devin CLI login is picked; the marker passes
//! on `access` to the DevinCli nodes, which hand its values to the agent. The
//! service recipe lives once, in metadata.json.

weft::access_node!(DevinCliAccessNode);
