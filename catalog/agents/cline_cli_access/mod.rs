//! ClineCliAccess: the ONE place a Cline CLI login is picked; the marker passes
//! on `access` to the ClineCli nodes, which hand its values to the agent. The
//! service recipe lives once, in metadata.json.

weft::access_node!(ClineCliAccessNode);
