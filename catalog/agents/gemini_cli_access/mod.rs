//! GeminiCliAccess: the ONE place a Gemini CLI login is picked; the marker passes
//! on `access` to the GeminiCli nodes, which hand its values to the agent. The
//! service recipe lives once, in metadata.json.

weft::access_node!(GeminiCliAccessNode);
