//! JwtAuth: emits the stored issuer as an access value for a trigger's
//! `auth` input. Pure pass-through of the connect handle; the issuer
//! and its key address live on the connection and the gateway verifies
//! callers' tokens against them through the broker (the recipe's
//! `verify` block says how).

weft::access_node!(JwtAuthNode);
