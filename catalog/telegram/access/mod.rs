//! TelegramAccess: emits the connected bot's access value. Pure
//! pass-through of the sealed connect handle; the `/bot<token>/` path
//! prefix is applied by the authenticated client at call time.

weft::access_node!(TelegramAccessNode);
