//! Auth policy shared by every public-entry signal kind (any kind the
//! dispatcher exposes on a public URL: the live-caller routes today). Not
//! tied to one kind, so it lives in its own module rather than on a single
//! signal struct.

use serde::{Deserialize, Serialize};

/// How the dispatcher gates a public-entry connection.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PublicEntryAuth {
    /// Anyone with the URL can connect.
    #[default]
    None,
    /// The caller is verified against a stored connection: the
    /// connection's service recipe declares HOW (its `verify` block:
    /// a set of API keys, a JWT issuer, an HMAC secret) and the
    /// connection holds the material. The dispatcher asks the broker,
    /// which holds the connection, and admits the run only on a yes.
    Connection {
        /// The connection to verify against.
        access_id: String,
        /// The service the connection belongs to (the auth access
        /// node's recipe name), so the lookup refuses a connection of
        /// another service instead of reading its fields blind.
        service: String,
    },
}
