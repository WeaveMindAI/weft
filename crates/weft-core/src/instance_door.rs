//! What the doors that fill a program's fields from outside its source
//! answer: the instance door (the fields a program asks each instance to
//! fill, and what a change of them did) and the program door (the
//! connections the program itself uses on the install, its picks, managed
//! with a connections key). ONE definition for the dispatcher that answers
//! and the CLI and the connect library that read.

use serde::{Deserialize, Serialize};

/// One field a door fills, at one place of the program: a field an
/// instance fills at the instance door, or a connection the program picks
/// on the install at the program door (there `value` is the picked
/// connection's handle, `needed` whether a run is refused without one, and
/// `fallback` is never set and `connection` is `none`).
// SYNC: FilledField <-> packages/weft-connect/src/core/wire.ts FilledField
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilledField {
    /// The node, spelled the way the program reads it: with `field`, the
    /// key the instance's value is stored under.
    pub node: String,
    pub field: String,
    /// The node type and label, for titling it on a page.
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

/// What a change of the fields a door fills did beyond storing: the
/// triggers it set up again, spelled the way the program reads them, and
/// the connections it forgot because the change left nothing on the
/// install using them (only a change at the program door forgets any: the
/// connection a pick moved off, or the one a clear left unused).
// SYNC: ValuesChanged <-> packages/weft-connect/src/core/wire.ts ValuesChanged
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValuesChanged {
    pub rearmed: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub forgotten: Vec<uuid::Uuid>,
}

/// `POST /program/connections/direct`: a pasted connect for one connection
/// field of the program, stored as one of the program owner's connections
/// and picked there. The connect is the editor's (`ConnectDirect`), with the
/// field it is for; the door takes the service's recipe from the program
/// (the sent `spec` must name the field's service) and stamps the project.
// SYNC: ProgramConnectDirect <-> packages/weft-connect/src/core/transport.ts ProgramConnectDirect
// Built on `access::wire`, which only a runtime build has.
#[cfg(feature = "runtime")]
#[derive(Debug, Serialize, Deserialize)]
pub struct ProgramConnectDirect {
    /// The node, spelled the way the program reads it, and its field.
    pub node: String,
    pub field: String,
    #[serde(flatten)]
    pub connect: crate::access::wire::ConnectDirect,
}

/// The answer to `POST /program/connections/direct`: the connection made
/// (what the connect page shows, as at every other connect) and what
/// picking it did.
// SYNC: ProgramConnected <-> packages/weft-connect/src/core/wire.ts ProgramConnected
// Built on `access::wire`, which only a runtime build has.
#[cfg(feature = "runtime")]
#[derive(Debug, Serialize, Deserialize)]
pub struct ProgramConnected {
    pub grant: crate::access::wire::GrantSummary,
    #[serde(flatten)]
    pub changed: ValuesChanged,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The program door's shapes travel in camelCase: a connect is the
    /// editor's connect with the field beside it, and the answers carry
    /// what was set up again and what was forgotten.
    #[test]
    fn the_program_door_shapes_round_trip() {
        let request = serde_json::json!({
            "node": "one.vapi",
            "field": "account",
            "spec": { "service": "vapi", "acquisition": { "kind": "static", "ways": [{ "fields": [{ "name": "key" }] }] } },
            "door": "own",
            "values": { "key": "sk-1" },
            "label": null,
            "permissions": [],
            "paste": false,
            "projectId": null,
            "sharedApp": null
        });
        let parsed: ProgramConnectDirect = serde_json::from_value(request).unwrap();
        assert_eq!((parsed.node.as_str(), parsed.field.as_str()), ("one.vapi", "account"));
        assert_eq!(parsed.connect.values.get("key").map(String::as_str), Some("sk-1"));
        let wire = serde_json::to_value(&parsed).unwrap();
        assert_eq!(wire["node"], "one.vapi");
        assert_eq!(wire["values"]["key"], "sk-1");
        assert!(wire.get("projectId").is_some(), "the connect keeps its own camelCase names");

        let gone = uuid::Uuid::from_u128(3);
        let changed = ValuesChanged { rearmed: vec!["call".into()], forgotten: vec![gone] };
        let wire = serde_json::to_value(&changed).unwrap();
        assert_eq!(wire, serde_json::json!({ "rearmed": ["call"], "forgotten": [gone] }));
        assert_eq!(serde_json::from_value::<ValuesChanged>(wire).unwrap(), changed);
        let nothing_forgotten = ValuesChanged { rearmed: vec!["call".into()], ..Default::default() };
        let wire = serde_json::to_value(&nothing_forgotten).unwrap();
        assert_eq!(wire, serde_json::json!({ "rearmed": ["call"] }), "an empty forgotten is left out");
        assert_eq!(serde_json::from_value::<ValuesChanged>(wire).unwrap(), nothing_forgotten);

        let grant: crate::access::wire::GrantSummary = serde_json::from_value(serde_json::json!({
            "id": uuid::Uuid::from_u128(1),
            "service": "vapi",
            "projectId": null,
            "identity": null,
            "label": "vapi",
            "scopes": [],
            "permissionsVerified": false,
            "owner": "author",
            "door": "own",
            "expiresAt": null
        }))
        .unwrap();
        let wire = serde_json::to_value(ProgramConnected { grant, changed }).unwrap();
        assert_eq!(wire["grant"]["service"], "vapi");
        assert_eq!(wire["rearmed"], serde_json::json!(["call"]));
        assert_eq!(wire["forgotten"], serde_json::json!([gone]));
        assert_eq!(serde_json::from_value::<ProgramConnected>(wire).unwrap().changed.forgotten, vec![gone]);
    }
}
