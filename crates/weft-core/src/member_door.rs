//! What the member door answers about a member's values: the fields a
//! program asks a member to fill, and what a change of them did. ONE
//! definition for the dispatcher that answers and the CLI that reads.

use serde::{Deserialize, Serialize};

/// One field the member fills, at one place of the program.
// SYNC: MemberField <-> packages/weft-connect/src/core/wire.ts MemberField
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemberField {
    /// The step, spelled the way the program reads it: with `field`, the
    /// key the member's value is stored under.
    pub step: String,
    pub field: String,
    /// The step's node type and label, for titling it on a page.
    #[serde(rename = "nodeType")]
    pub node_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    /// The input as its node declares it: its type, its widget (what to
    /// draw), its label, placeholder and default.
    pub input: crate::project::InputDefinition,
    /// What a member who gives nothing gets (`@member_filled(<value>)`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fallback: Option<serde_json::Value>,
    /// Whether a run needs the member's value here: with no fallback, a
    /// run would be refused without it (a required input with no
    /// default, or a rule of the node about this field).
    pub needed: bool,
    /// For a connection field, the service's recipe: what drives the
    /// connect page (doors, fields, permissions).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spec: Option<crate::AccessSpec>,
    /// For a `remote_select` field, the connection wired to it for this
    /// member and whose it is; `none` for any other field.
    pub connection: FieldConnection,
    /// What the member gave, if anything (a connection reads as its
    /// `{id, identity}` handle).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<serde_json::Value>,
}

/// Whose connection signs a member's `remote_select` field. A list
/// lookup runs server-side, so any connection signs it; a chooser
/// (`picker`) or the connection's granted resources (`granted`) hand
/// the connection to the member's own browser, so they only run on the
/// member's OWN connection.
// SYNC: FieldConnection <-> packages/weft-connect/src/core/wire.ts FieldConnection
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FieldConnection {
    /// Nothing is wired (or the field is no `remote_select`).
    None,
    /// The member's own connection.
    Own,
    /// The author's connection, written in the source and shared.
    Shared,
}

/// What a change of a member's values did beyond storing: the member's
/// triggers it set up again, spelled the way the program reads them.
// SYNC: ValuesChanged <-> packages/weft-connect/src/core/wire.ts ValuesChanged
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValuesChanged {
    pub rearmed: Vec<String>,
}
