//! ExaAccess: emits the connected Exa access value. Pure pass-through
//! of the sealed connect handle; the x-api-key header is applied by
//! the authenticated client at call time.

weft::access_node!(ExaAccessNode);
