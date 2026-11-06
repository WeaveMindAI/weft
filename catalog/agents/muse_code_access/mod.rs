//! MuseCodeAccess: the ONE place a Muse Code login is picked; the marker passes
//! on `access` to the MuseCode nodes, which hand its values to the agent. The
//! service recipe lives once, in metadata.json.

weft::access_node!(MuseCodeAccessNode);
