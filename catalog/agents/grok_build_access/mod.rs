//! GrokBuildAccess: the ONE place a Grok Build login is picked; the marker passes
//! on `access` to the GrokBuild nodes, which hand its values to the agent. The
//! service recipe lives once, in metadata.json.

weft::access_node!(GrokBuildAccessNode);
