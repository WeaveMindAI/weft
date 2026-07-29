//! GoogleAccess: emits the connected Google account's access value.
//! One sign-in for everything Google (Drive, Sheets, Calendar); what a
//! connection can DO is the permissions it was granted. Pure
//! pass-through of the sealed connect handle; token refresh (rotating
//! refresh tokens included) happens store-side.

weft::access_node!(GoogleAccessNode);
