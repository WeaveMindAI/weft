//! DeepSeekHarnessAccess: the ONE place a DeepSeek Harness login is picked; the marker passes
//! on `access` to the DeepSeekHarness nodes, which hand its values to the agent. The
//! service recipe lives once, in metadata.json.

weft::access_node!(DeepSeekHarnessAccessNode);
