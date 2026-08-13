//! MistralAccess: emits the connected Mistral access value. Pure
//! pass-through of the sealed connect handle; the bearer header is
//! applied by the authenticated client at call time.

weft::access_node!(MistralAccessNode);
