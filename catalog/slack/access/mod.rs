//! SlackAccess: emits the connected workspace's access value. Pure
//! pass-through of the sealed connect handle (plus the node's ticked
//! scopes, threaded by the runtime); the OAuth install/refresh all
//! happen store-side.

weft::access_node!(SlackAccessNode);
