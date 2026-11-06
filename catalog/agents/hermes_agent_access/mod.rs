//! HermesAgentAccess: the ONE place a Hermes Agent login is picked; the marker passes
//! on `access` to the HermesAgent nodes, which hand its values to the agent. The
//! service recipe lives once, in metadata.json.

weft::access_node!(HermesAgentAccessNode);
