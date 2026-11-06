//! MistralVibeAccess: the ONE place a Mistral Vibe login is picked; the marker passes
//! on `access` to the MistralVibe nodes, which hand its values to the agent. The
//! service recipe lives once, in metadata.json.

weft::access_node!(MistralVibeAccessNode);
