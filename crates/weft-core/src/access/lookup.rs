//! The editor's `remote_select` lookups as they travel the wire: the
//! editor or `weft options` posts a request to the dispatcher, the
//! dispatcher forwards it (tenant-wrapped) to the broker, and a page of
//! id/label pairs comes back. ONE definition per shape so the hops
//! cannot drift.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::member::MemberScope;
use crate::node::Lookup;

/// One page of lookup options, as the editor renders them.
// SYNC: LookupPage <-> packages/weft-connect/src/core/wire.ts LookupPage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LookupPage {
    pub items: Vec<LookupItem>,
    pub next_cursor: Option<String>,
}

/// One option a lookup offers: the id stored, the label shown.
// SYNC: LookupItem <-> packages/weft-connect/src/core/wire.ts LookupItem
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LookupItem {
    pub id: String,
    pub label: String,
}

/// One `remote_select` lookup request.
// SYNC: LookupRequest <-> packages/weft-graph/src/webview/lib/components/project/editor-connect.ts list
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LookupRequest {
    /// The connection to sign with; `None` for a `public` lookup (no
    /// connection is involved at all, and no `service` either).
    #[serde(default)]
    pub access_id: Option<uuid::Uuid>,
    /// The member this lookup is for (their own door); `None`: the
    /// author. Set by the dispatcher alone, never taken from the caller.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub for_member: Option<MemberScope>,
    /// The service `access_id` is a connection of; `None` with no
    /// connection.
    #[serde(default)]
    pub service: Option<String>,
    /// The widget's declarative lookup, verbatim from the node's
    /// (compiler-resolved) metadata.
    pub lookup: Lookup,
    #[serde(default)]
    pub query: String,
    /// Picked parent values for drill-down (`depends_on`).
    #[serde(default)]
    pub parents: BTreeMap<String, String>,
    #[serde(default)]
    pub cursor: Option<String>,
}

/// A `granted` resource-source read: which connection, and which
/// stored capture's id/label pairs to read.
// SYNC: GrantedQuery <-> packages/weft-graph/src/webview/lib/components/project/editor-connect.ts granted
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrantedQuery {
    pub access_id: uuid::Uuid,
    pub service: String,
    /// The member this read is for (their own door); `None`: the author.
    /// Set by the dispatcher alone, never taken from the caller.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub for_member: Option<MemberScope>,
    /// The stored value (captured at connect) holding the JSON array.
    pub from: String,
    /// Dotted path to one item's label (same vocabulary as [`Lookup`]).
    pub label: String,
    /// Dotted path to one item's id.
    pub value: String,
}
