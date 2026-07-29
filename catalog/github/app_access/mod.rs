//! GitHubAppAccess: emits the connected GitHub App installation's
//! access value. Pure pass-through of the sealed connect handle; the
//! JWT mint + exchange happen store-side at resolution time.

weft::access_node!(GitHubAppAccessNode);
