//! EmailAccess: emits the connected mailbox's access value. Pure
//! pass-through of the sealed connect handle; the trigger and action
//! nodes read the servers and credentials off it at use time.

weft::access_node!(EmailAccessNode);
