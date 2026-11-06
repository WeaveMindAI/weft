//! GooseAccess: the ONE place a goose login is picked; the marker passes
//! on `access` to the Goose nodes, which hand its values to the agent. The
//! service recipe lives once, in metadata.json.

weft::access_node!(GooseAccessNode);
