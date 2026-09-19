//! ApiKeyAuth: emits the stored key set as an access value for a
//! trigger's `auth` input. Pure pass-through of the connect handle; the
//! keys live on the connection and the gateway checks callers against
//! them through the broker (the recipe's `verify` block says how).

weft::access_node!(ApiKeyAuthNode);
