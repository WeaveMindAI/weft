//! KeenableAccess: emits the connected Keenable access value. Pure
//! pass-through of the sealed connect handle; the X-API-Key header is
//! applied by the authenticated client at call time.

weft::access_node!(KeenableAccessNode);
