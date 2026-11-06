//! ClaudeCodeAccess: the ONE place a Claude Code login is picked, a
//! subscription token or an API key; the marker passes on `access` to the
//! ClaudeCode nodes, which hand its values to the agent. The service recipe
//! lives once, in metadata.json.

weft::access_node!(ClaudeCodeAccessNode);
