//! CodeBuddyCodeAccess: the ONE place a CodeBuddy Code login is picked; the marker passes
//! on `access` to the CodeBuddyCode nodes, which hand its values to the agent. The
//! service recipe lives once, in metadata.json.

weft::access_node!(CodeBuddyCodeAccessNode);
