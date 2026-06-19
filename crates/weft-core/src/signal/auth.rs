//! Auth policy shared by every public-entry signal kind (any kind the
//! dispatcher exposes on a public URL: live-caller endpoints today). Not
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
    /// The listener mints a plaintext key at register and stores its sha256
    /// on the signal row; `/display` returns the plaintext until the
    /// listener pod restarts.
    OptionalApiKey,
}
