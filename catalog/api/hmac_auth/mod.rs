//! HmacAuth: emits the stored signing secret as an access value for a
//! trigger's `auth` input. Pure pass-through of the connect handle; the
//! secret lives on the connection and the gateway recomputes callers'
//! signatures against it through the broker (the recipe's `verify`
//! block spells the scheme: sha256 over `<timestamp>.<body>`, hex, in
//! `X-Signature`, the unix timestamp in `X-Timestamp`).

weft::access_node!(HmacAuthNode);
