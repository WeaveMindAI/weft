//! What the instance door answers about an instance's values: the fields
//! a program asks each instance to fill, and what a change of them did. ONE
//! definition for the dispatcher that answers and the CLI that reads.

use serde::{Deserialize, Serialize};

/// One field an instance fills, at one place of the program.
// SYNC: InstanceField <-> packages/weft-connect/src/core/wire.ts InstanceField
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstanceField {
    /// The step, spelled the way the program reads it: with `field`, the
    /// key the instance's value is stored under.
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
    /// What an instance that gives nothing gets (`@instance_filled(<value>)`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fallback: Option<serde_json::Value>,
    /// Whether a run needs the instance's value here: with no fallback, a
    /// run would be refused without it (a required input with no
    /// default, or a rule of the node about this field).
    pub needed: bool,
    /// For a connection field, the service's recipe: what drives the
    /// connect page (doors, fields, permissions).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spec: Option<crate::AccessSpec>,
    /// For a `remote_select` field, the connection wired to it for this
    /// instance and whose it is; `none` for any other field.
    pub connection: FieldConnection,
    /// What the instance gave, if anything (a connection reads as its
    /// `{id, identity}` handle).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<serde_json::Value>,
}

/// Whose connection signs an instance's `remote_select` field. A list
/// lookup runs server-side, so any connection signs it; a chooser
/// (`picker`) or the connection's granted resources (`granted`) hand
/// the connection to the browser of whoever acts for the instance, so
/// they only run on the instance's OWN connection.
// SYNC: FieldConnection <-> packages/weft-connect/src/core/wire.ts FieldConnection
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FieldConnection {
    /// Nothing is wired (or the field is no `remote_select`).
    None,
    /// The instance's own connection.
    Own,
    /// The author's connection, written in the source and shared.
    Shared,
}

/// `PUT /instance/values`: give values for fields, and clear others, all
/// at once.
// SYNC: ValuesRequest <-> packages/weft-connect/src/core/wire.ts ValuesRequest
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ValuesRequest {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub set: Vec<crate::run_spec::InstanceValueInput>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub clear: Vec<crate::run_spec::InstanceFieldRef>,
}

/// What a change of an instance's values did beyond storing: the instance's
/// triggers it set up again, spelled the way the program reads them.
// SYNC: ValuesChanged <-> packages/weft-connect/src/core/wire.ts ValuesChanged
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValuesChanged {
    pub rearmed: Vec<String>,
}
