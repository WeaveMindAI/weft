//! JunieCliAccess: the ONE place a Junie CLI login is picked; the marker passes
//! on `access` to the JunieCli nodes, which hand its values to the agent. The
//! service recipe lives once, in metadata.json.

weft::access_node!(JunieCliAccessNode);
