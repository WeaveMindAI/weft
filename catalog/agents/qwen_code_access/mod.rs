//! QwenCodeAccess: the ONE place a Qwen Code login is picked; the marker passes
//! on `access` to the QwenCode nodes, which hand its values to the agent. The
//! service recipe lives once, in metadata.json.

weft::access_node!(QwenCodeAccessNode);
