//! CopilotCliAccess: the ONE place a GitHub Copilot CLI login is picked; the marker passes
//! on `access` to the CopilotCli nodes, which hand its values to the agent. The
//! service recipe lives once, in metadata.json.

weft::access_node!(CopilotCliAccessNode);
