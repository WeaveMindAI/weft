//! Instances: separate running copies of part of a program.
//!
//! One graph, written once by its author, can run as many separate copies
//! of its per-instance part, each under its own id, each with its own
//! connections, its own copy of a container, the triggers listening on
//! it, its own values and runs. An instance can be one per person, one per
//! session, or several per person: which is the program's own business.
//! An instance is an opaque id the AUTHOR chooses (`"user-42"`); weft
//! keeps no registry of them. An instance exists as long as something is
//! attached to it, and the same id always resolves to the same things.
//!
//! An instance is always read together with its project:
//! `(project, instance)` is the identity, so two projects that both say
//! `"user-42"` share nothing. Every table stores the two as separate
//! columns and every storage key as two separate path segments, never one
//! joined string.

use serde::{Deserialize, Serialize};

/// The header a trusted backend names the instance with, on a route gated
/// by a connection credential (never honoured on an open route, where
/// anybody could claim any instance).
// SYNC: INSTANCE_HEADER <-> packages/weft-connect/src/core/transport.ts INSTANCE_HEADER
pub const INSTANCE_HEADER: &str = "Weft-Instance";

/// The header a browser or extension presents its instance token in, on
/// a route call. The token IS the identity: it names its instance and its
/// one project, so no other header can override it.
// SYNC: INSTANCE_TOKEN_HEADER <-> packages/weft-connect/src/core/transport.ts INSTANCE_TOKEN_HEADER
pub const INSTANCE_TOKEN_HEADER: &str = "Weft-Instance-Token";

/// Which instance a call through a route is for, from its
/// [`INSTANCE_HEADER`]. The header is honoured only on a route gated by a
/// connection (`gated`): the caller then proved they hold the route's
/// credential, which makes them the author's own backend. On an open route
/// anybody could name any instance, so the header is refused there rather
/// than ignored, and the caller hears why. `headers` as received (any
/// case).
pub fn instance_from_header(
    headers: &std::collections::BTreeMap<String, String>,
    gated: bool,
) -> Result<Option<InstanceId>, String> {
    let Some((_, value)) = headers.iter().find(|(name, _)| name.eq_ignore_ascii_case(INSTANCE_HEADER)) else {
        return Ok(None);
    };
    if !gated {
        return Err(format!(
            "the {INSTANCE_HEADER} header is honoured only on a route gated by a connection: on an open \
             route anybody could claim any instance. Gate the route, or give the browser an instance \
             token instead"
        ));
    }
    InstanceId::new(value.trim()).map(Some).map_err(|e| format!("{INSTANCE_HEADER}: {e}"))
}

/// The body directive that marks a node as existing once per instance
/// (`@per_instance`, on its own line inside the node's braces).
pub const PER_INSTANCE_DIRECTIVE: &str = "per_instance";

/// One instance of a program: an id its author chose. Validated on the
/// way in, with the grammar of a storage key segment, so it can stand as a
/// path segment, a label value or a column without escaping and can never
/// forge a level (`..`, `/`).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct InstanceId(String);

impl InstanceId {
    /// The longest id accepted, the storage segment limit.
    pub const MAX_LEN: usize = 128;

    // SYNC: InstanceId::new <-> packages/weft-graph/src/run-spec.ts INSTANCE_ID_PATTERN, crates/weft-core/src/storage/key.rs valid_segment
    pub fn new(id: impl Into<String>) -> Result<Self, String> {
        let id = id.into();
        if crate::storage::key::valid_segment(&id) {
            Ok(Self(id))
        } else {
            Err(format!(
                "'{id}' is not a valid instance id: use 1 to {} letters, digits, '-', '_' or '.' \
                 (and not '.' or '..' alone)",
                Self::MAX_LEN
            ))
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for InstanceId {
    type Error = String;
    fn try_from(id: String) -> Result<Self, String> {
        Self::new(id)
    }
}

impl From<InstanceId> for String {
    fn from(id: InstanceId) -> String {
        id.0
    }
}

impl std::fmt::Display for InstanceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::str::FromStr for InstanceId {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        Self::new(s)
    }
}

/// Whose a per-project resource is: the program's own shared one, or one
/// instance's. The key dimension every per-instance resource carries (an
/// infra copy, a trigger activation, a connection, a pick, a stored file),
/// so every table spells it the same way: a nullable `instance_id` column,
/// NULL for [`Owner::Shared`].
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default)]
#[serde(tag = "kind", content = "instance", rename_all = "snake_case")]
pub enum Owner {
    #[default]
    Shared,
    Instance(InstanceId),
}

impl Owner {
    /// The owner a nullable `instance_id` column holds.
    pub fn from_instance(instance: Option<InstanceId>) -> Self {
        match instance {
            Some(instance) => Owner::Instance(instance),
            None => Owner::Shared,
        }
    }

    /// The value for a nullable `instance_id` column.
    pub fn instance(&self) -> Option<&InstanceId> {
        match self {
            Owner::Shared => None,
            Owner::Instance(instance) => Some(instance),
        }
    }

    /// The owner as a person reads it in a message: `shared`, or
    /// `instance 'user-42'`.
    pub fn describe(&self) -> String {
        match self {
            Owner::Shared => "shared".into(),
            Owner::Instance(instance) => format!("instance '{instance}'"),
        }
    }
}

/// Which copies of the program's infra an infra verb acts on: the shared
/// ones (`weft infra stop`), one instance's (`--instance`, or a program's
/// `ctx.infra(..).instance(..)`), or every copy there is (the project
/// being removed).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(tag = "kind", content = "instance", rename_all = "snake_case")]
pub enum Copies {
    #[default]
    Shared,
    Instance(InstanceId),
    Every,
}

impl Copies {
    /// The copies an optional `--instance` names: that instance's, or the
    /// shared ones.
    pub fn of(instance: Option<InstanceId>) -> Self {
        match instance {
            Some(instance) => Copies::Instance(instance),
            None => Copies::Shared,
        }
    }

    /// Whether the copy owned by `instance` (`None` = the shared copy) is
    /// among these.
    // SYNC: Copies::admits <-> crates/weft-broker-client/src/lifecycle_command.rs (command_reaches_copy)
    pub fn admits(&self, instance: Option<&InstanceId>) -> bool {
        match self {
            Copies::Shared => instance.is_none(),
            Copies::Instance(m) => instance == Some(m),
            Copies::Every => true,
        }
    }

    /// The two columns a stored command keeps them in: `instance_id`
    /// and `every_copy`.
    pub fn columns(&self) -> (Option<&str>, bool) {
        match self {
            Copies::Shared => (None, false),
            Copies::Instance(m) => (Some(m.as_str()), false),
            Copies::Every => (None, true),
        }
    }

    /// Back from the two stored columns.
    pub fn from_columns(instance: Option<String>, every: bool) -> Result<Self, String> {
        match (instance, every) {
            (None, false) => Ok(Copies::Shared),
            (Some(m), false) => Ok(Copies::Instance(InstanceId::new(m)?)),
            (None, true) => Ok(Copies::Every),
            (Some(_), true) => Err("a command for every copy names no instance".into()),
        }
    }
}

/// One instance of one project: which instance a use of a connection is
/// for, where a request carries it (an instance's own lookup, an
/// instance's trigger reading through its connection). `None` wherever it
/// is carried is the author, or a run for no instance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstanceScope {
    pub project_id: uuid::Uuid,
    pub instance: InstanceId,
}

impl InstanceScope {
    /// Back from a row's two columns (`project_id`, `instance_id`): the
    /// instance's scope, or `None` for a row that is no instance's.
    pub fn from_columns(project_id: uuid::Uuid, instance: Option<String>) -> Result<Option<Self>, String> {
        instance.map(|m| Ok(InstanceScope { project_id, instance: InstanceId::new(m)? })).transpose()
    }
}

/// Why a node exists once per instance. The two starting points the
/// author writes: `Marked` is an infra node marked `@per_instance` (one
/// container per instance), `Filled` a node with a field written
/// `@instance_filled` (each instance provides its value). `Derived` is
/// everything the compiler found reading a per-instance value downstream
/// of one, which runs in that instance's runs.
// SYNC: PerInstance <-> packages/weft-graph/src/protocol.ts PerInstance
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PerInstance {
    Marked,
    Filled,
    Derived,
}

/// The marker a field's value is written as when each instance provides
/// it: `@instance_filled`, or `@instance_filled(<value>)` with the value
/// used for an instance that gave none.
pub const INSTANCE_FILLED_MARKER: &str = "instance_filled";

/// The key a `@instance_filled` literal is lowered to: the one JSON shape
/// the compiled definition carries it in (`{"__weft_instance_filled__":
/// {}}`, or `{..: {"fallback": <value>}}`). A structured value rather
/// than a string, so no string a program writes can ever read as one;
/// the parser refuses the key inside a written object for the same
/// reason.
// SYNC: INSTANCE_FILLED_KEY <-> packages/weft-graph/src/protocol.ts INSTANCE_FILLED_KEY
pub const INSTANCE_FILLED_KEY: &str = "__weft_instance_filled__";

/// A field written `@instance_filled`, as it sits in the compiled literals.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct InstanceFilled<'a> {
    /// The value for an instance that gave none
    /// (`@instance_filled(<value>)`); `None`: such an instance leaves the
    /// field unwritten, and the node's default, optionality or required
    /// refusal decides.
    pub fallback: Option<&'a serde_json::Value>,
}

/// The literal `@instance_filled` lowers to.
// SYNC: instance_filled_literal <-> packages/weft-graph/src/protocol.ts instanceFilledValue
pub fn instance_filled_literal(fallback: Option<serde_json::Value>) -> serde_json::Value {
    let mut inner = serde_json::Map::new();
    if let Some(fallback) = fallback {
        inner.insert("fallback".into(), fallback);
    }
    serde_json::json!({ INSTANCE_FILLED_KEY: inner })
}

/// `value` read as a `@instance_filled` literal, `None` for any other value.
// SYNC: as_instance_filled <-> packages/weft-graph/src/protocol.ts instanceFilled
pub fn as_instance_filled(value: &serde_json::Value) -> Option<InstanceFilled<'_>> {
    let serde_json::Value::Object(map) = value else { return None };
    if map.len() != 1 {
        return None;
    }
    let inner = map.get(INSTANCE_FILLED_KEY)?.as_object()?;
    Some(InstanceFilled { fallback: inner.get("fallback") })
}

/// Every field of `node` written `@instance_filled`, with its marker.
pub fn instance_filled_fields(node: &crate::project::NodeDefinition) -> impl Iterator<Item = (&str, InstanceFilled<'_>)> {
    node.port_literals.iter().filter_map(|(field, value)| Some((field.as_str(), as_instance_filled(value)?)))
}

/// One instance's values: field -> value, for the step at one place.
pub type PlaceValues = std::collections::BTreeMap<String, serde_json::Value>;

/// An instance's values for a run: place (`read`, `one.read`) -> its fields.
/// Read once when the run is born and carried on it, so every firing of
/// the run sees the values it was born with.
pub type InstanceValues = std::collections::BTreeMap<String, PlaceValues>;

/// A change to one instance's values: fields given a value, and fields
/// cleared (which then fall back as if never given). What a store call
/// asks for, and what the setup it re-arms runs with before the change is
/// stored.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ValueChanges {
    pub set: InstanceValues,
    /// `(place, field)` pairs.
    pub cleared: std::collections::BTreeSet<(String, String)>,
}

impl ValueChanges {
    /// `values` with this change made.
    pub fn apply(&self, values: &mut InstanceValues) {
        for (place, field) in &self.cleared {
            if let Some(fields) = values.get_mut(place) {
                fields.remove(field);
                if fields.is_empty() {
                    values.remove(place);
                }
            }
        }
        for (place, fields) in &self.set {
            values.entry(place.clone()).or_default().extend(fields.clone());
        }
    }

    /// Every place the change touches.
    pub fn places(&self) -> std::collections::BTreeSet<&str> {
        self.set.keys().map(String::as_str).chain(self.cleared.iter().map(|(place, _)| place.as_str())).collect()
    }

    pub fn is_empty(&self) -> bool {
        self.set.is_empty() && self.cleared.is_empty()
    }
}

/// Put the instance's values into `literals` (the constants delivered to
/// one firing of `node` at one place): each of the node's
/// `@instance_filled` fields takes the instance's value, else its
/// fallback, else is removed, so the node's default, optionality or
/// required refusal applies exactly as if the field had not been written.
/// Only the node's own instance-filled
/// fields are touched: a value on any other port, whatever its shape, is
/// left alone.
pub fn fill_literals(
    node: &crate::project::NodeDefinition,
    literals: &mut serde_json::Map<String, serde_json::Value>,
    values: Option<&PlaceValues>,
) {
    for (field, filled) in instance_filled_fields(node) {
        match values.and_then(|v| v.get(field)).or(filled.fallback) {
            Some(value) => {
                literals.insert(field.to_string(), value.clone());
            }
            None => {
                literals.remove(field);
            }
        }
    }
}

/// A value an instance gives for `field` of `node`, held to what the field
/// takes, exactly as a value written in the source is: a connection or
/// resource handle to its shape, anything else to the port's type (cast
/// where the cast is unambiguous, a `"18"` for a Number) and to its
/// widget's domain. Answers the value as it is kept.
pub fn check_instance_value(
    node: &crate::project::NodeDefinition,
    field: &str,
    value: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    if !instance_filled_fields(node).any(|(filled, _)| filled == field) {
        return Err(format!("'{field}' of '{}' is not filled by each instance (it is not `@instance_filled`)", node.id));
    }
    let Some(input) = node.inputs.iter().find(|i| i.name == field) else {
        return Err(format!("'{}' has no input '{field}'", node.id));
    };
    if let Some(widget) = &input.widget {
        if value.is_object()
            && matches!(widget, crate::node::Widget::Access { .. } | crate::node::Widget::RemoteSelect { .. })
        {
            widget.check_handle_shape(value)?;
            return Ok(value.clone());
        }
    }
    let typed = if input.port_type.is_unresolved()
        || crate::weft_type::WeftType::is_compatible(&crate::weft_type::WeftType::infer(value), &input.port_type)
    {
        value.clone()
    } else {
        input.port_type.cast_value(value).map_err(|why| format!("'{field}' takes {}: {why}", input.port_type))?
    };
    if let Some(widget) = &input.widget {
        widget.check_value(&typed).map_err(|why| format!("'{field}': {why}"))?;
    }
    Ok(typed)
}

/// The instance-filled fields of `original` that `filled` (the node as
/// one instance's run at `place` sees it, [`filled_node`]) leaves with
/// nothing where the node needs a value: required, no default, not
/// optional. One sentence each, naming the instance and the place.
pub fn unfilled_gaps(
    original: &crate::project::NodeDefinition,
    filled: &crate::project::NodeDefinition,
    place: &str,
    instance: &InstanceId,
) -> Vec<String> {
    instance_filled_fields(original)
        .filter(|(field, _)| !filled.port_literals.contains_key(*field))
        .filter(|(field, _)| {
            original
                .inputs
                .iter()
                .find(|i| i.name == *field)
                .is_some_and(|i| i.required && i.default.is_none() && !original.optional_ports.contains(*field))
        })
        .map(|(field, _)| format!("instance '{instance}' has not filled '{place}.{field}'"))
        .collect()
}

/// Every error rule of `original` (`instance_rules`) that fires on
/// `filled`, the node with an instance's values in (`crate::rules`), one
/// sentence each naming the instance and the place. A field still
/// `@instance_filled` in `filled` (an instance that has not filled it yet,
/// when its values are checked as they are stored) reads as there with a
/// value not known yet, exactly as at compile time. A rule about a
/// `@instance_filled` field the instance left empty (the node's "connect
/// one on the node", say) is said the way whoever fills the instance can
/// act on: that it has not been filled.
pub fn rule_gaps(
    cx: &crate::rules::RuleContext,
    original: &crate::project::NodeDefinition,
    filled: &crate::project::NodeDefinition,
    place: &str,
    instance: &InstanceId,
) -> Vec<String> {
    let Some(rules) = &original.instance_rules else { return Vec::new() };
    rules
        .rules
        .iter()
        .filter(|rule| rule.then.severity == crate::node::RuleSeverity::Error)
        .filter(|rule| crate::rules::fires(rule, filled, cx, &rules.custom_outputs))
        .map(|rule| match rule.then.field.as_deref() {
            Some(field)
                if !filled.port_literals.contains_key(field)
                    && instance_filled_fields(original).any(|(filled_field, _)| filled_field == field) =>
            {
                format!("instance '{instance}' has not filled '{place}.{field}'")
            }
            _ => format!("instance '{instance}' at '{place}': {}", crate::rules::message(rule, filled, cx, &rules.custom_outputs)),
        })
        .collect()
}

/// Whether a run needs the instance's own value for `field` of `node`: left
/// empty (no fallback), either the input is required with no default, or
/// one of the node's error rules about that field fires. The same two
/// checks a run's birth makes ([`unfilled_gaps`], [`rule_gaps`]), so a
/// settings page for an instance marks exactly the fields a run would
/// refuse without.
pub fn instance_field_needed(
    cx: &crate::rules::RuleContext,
    node: &crate::project::NodeDefinition,
    field: &str,
) -> bool {
    let empty = filled_node(node, None);
    if empty.port_literals.contains_key(field) {
        return false;
    }
    let required = node
        .inputs
        .iter()
        .find(|i| i.name == field)
        .is_some_and(|i| i.required && i.default.is_none() && !node.optional_ports.contains(field));
    required
        || node.instance_rules.as_ref().is_some_and(|rules| {
            rules.rules.iter().any(|rule| {
                rule.then.severity == crate::node::RuleSeverity::Error
                    && rule.then.field.as_deref() == Some(field)
                    && crate::rules::fires(rule, &empty, cx, &rules.custom_outputs)
            })
        })
}

/// `node` as one instance's run at one place sees it: its
/// `@instance_filled` literals replaced by [`fill_literals`]. What an
/// instance's value is checked
/// against (the node's rules, its port types, its required inputs) is
/// this node, so the check and the run read the same thing.
pub fn filled_node(node: &crate::project::NodeDefinition, values: Option<&PlaceValues>) -> crate::project::NodeDefinition {
    let mut filled = node.clone();
    let mut literals: serde_json::Map<String, serde_json::Value> = std::mem::take(&mut filled.port_literals).into_iter().collect();
    fill_literals(node, &mut literals, values);
    filled.port_literals = literals.into_iter().collect();
    filled
}

/// The connection one instance's run signs a `remote_select` field's lookup
/// with: the field's widget names an access input of the same node, and
/// the wire into that input leads (through any group or included file's
/// ports) to an access node. Its connection field is either picked on
/// the install (the author's connection, shared: `picks` give it) or
/// `@instance_filled`, then `values` (the instance's) give it. Nothing wired
/// there is [`LookupConnection::Unwired`]: the lookup then only works on
/// a public source. A wired connection the instance has not filled yet (or
/// no connection picked at the access node) is
/// [`LookupConnection::NotConnected`], with why: the field listing shows
/// the field as unconnected, the lookup door refuses with that reason.
/// `Err` is only a program that cannot look anything up. The same walk the editor does over its canvas, over the
/// compiled program, so an instance's dropdown lists what the step's run
/// would reach.
pub fn lookup_connection(
    project: &crate::project::ProjectDefinition,
    at: &crate::frames::Located,
    field: &str,
    values: &InstanceValues,
    picks: &crate::picks::Picks,
) -> Result<LookupConnection, String> {
    let node = project.nodes.iter().find(|n| n.id == at.id).ok_or_else(|| format!("no node '{}'", at.id))?;
    let Some(crate::node::Widget::RemoteSelect { access, .. }) =
        node.inputs.iter().find(|i| i.name == field).and_then(|i| i.widget.as_ref())
    else {
        return Err(format!("'{field}' of '{}' has no list to look up", at.id));
    };
    let (mut place, mut port) = (at.clone(), access.clone());
    // Upstream along the wire into `port`, through every boundary on
    // the way, to the node that makes the connection.
    let source = loop {
        let Some(edge) = project.edges.iter().find(|e| e.target == place.id && e.target_handle.as_deref() == Some(port.as_str())) else {
            return Ok(LookupConnection::Unwired);
        };
        let Some(from) = crate::project::selection::source_place(project, &place, edge) else {
            return Ok(LookupConnection::Unwired);
        };
        let from_node = project.nodes.iter().find(|n| n.id == from.id).ok_or_else(|| format!("no node '{}'", from.id))?;
        if from_node.group_boundary.is_some() {
            port = edge.source_handle.clone().unwrap_or_default();
            place = from;
            continue;
        }
        break (from, from_node);
    };
    let (source_place, source_node) = source;
    let Some(input) = source_node.inputs.iter().find(|i| matches!(i.widget, Some(crate::node::Widget::Access { .. }))) else {
        return Err(format!("'{field}' of '{}' is wired from '{}', which makes no connection", at.id, source_node.id));
    };
    let spelled = crate::project::address_of(project, &source_place.id, &source_place.path);
    let (handle, whose) = match source_node.port_literals.get(&input.name) {
        Some(literal) if as_instance_filled(literal).is_some() => {
            match values.get(&spelled).and_then(|fields| fields.get(&input.name)) {
                Some(value) => (value.clone(), crate::instance_door::FieldConnection::Own),
                None => {
                    return Ok(LookupConnection::NotConnected(format!(
                        "connect your account at '{spelled}' first: this list is read through it"
                    )))
                }
            }
        }
        Some(literal) if crate::picks::is_install_picked(literal) => {
            match picks.get(&spelled).and_then(|fields| fields.get(&input.name)) {
                Some(handle) => (handle.clone(), crate::instance_door::FieldConnection::Shared),
                None => {
                    return Ok(LookupConnection::NotConnected(format!(
                        "'{spelled}' has no connection picked on this install, so this list has nothing to read through"
                    )))
                }
            }
        }
        Some(_) => return Err(format!("'{spelled}' holds a connection written in the source; see {}", crate::picks::PICKS_DOC)),
        None => {
            return Ok(LookupConnection::NotConnected(format!(
                "'{spelled}' has no connection picked, so this list has nothing to read through"
            )))
        }
    };
    let id = handle
        .get("id")
        .and_then(serde_json::Value::as_str)
        .and_then(|id| id.parse::<uuid::Uuid>().ok())
        .ok_or_else(|| format!("the connection at '{spelled}' is not a connection handle"))?;
    let service = match &input.widget {
        Some(crate::node::Widget::Access { service: Some(service), .. }) => service.clone(),
        _ => return Err(format!("'{spelled}' names no service")),
    };
    Ok(LookupConnection::Wired(WiredConnection { id, service, whose }))
}

/// What [`lookup_connection`] found on the wire into a `remote_select`
/// field's access input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LookupConnection {
    /// Nothing is wired: the lookup only works on a public source.
    Unwired,
    /// A connection is wired but there is none to sign with yet (the
    /// instance has no account connected, or the access node has no
    /// connection picked), and why, spelled for whoever fills the
    /// instance.
    NotConnected(String),
    /// The connection the lookup is signed with.
    Wired(WiredConnection),
}

impl LookupConnection {
    /// Whose connection the instance's field listing shows: none until one
    /// is there to sign with.
    pub fn whose(&self) -> crate::instance_door::FieldConnection {
        match self {
            LookupConnection::Wired(wired) => wired.whose,
            LookupConnection::Unwired | LookupConnection::NotConnected(_) => crate::instance_door::FieldConnection::None,
        }
    }

    /// The connection a lookup signs with (`None` when nothing is wired),
    /// or why a wired one is not there yet.
    pub fn signing(self) -> Result<Option<WiredConnection>, String> {
        match self {
            LookupConnection::Unwired => Ok(None),
            LookupConnection::NotConnected(why) => Err(why),
            LookupConnection::Wired(wired) => Ok(Some(wired)),
        }
    }
}

/// The connection an instance's `remote_select` lookup is signed with,
/// and whose it is: the instance's own, or the author's shared one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WiredConnection {
    pub id: uuid::Uuid,
    pub service: String,
    /// [`FieldConnection::Own`] or [`FieldConnection::Shared`], never
    /// `None` (no connection is no `WiredConnection`).
    ///
    /// [`FieldConnection::Own`]: crate::instance_door::FieldConnection::Own
    /// [`FieldConnection::Shared`]: crate::instance_door::FieldConnection::Shared
    pub whose: crate::instance_door::FieldConnection,
}

/// `node` with only the instance-filled fields `values` give a value for
/// replaced: every other `@instance_filled` stays one. How an instance's
/// values are checked while its fields are still being filled in, one form
/// at a time: a rule about a field not reached yet has no answer.
pub fn partly_filled_node(node: &crate::project::NodeDefinition, values: &PlaceValues) -> crate::project::NodeDefinition {
    let mut filled = node.clone();
    for (field, value) in values {
        if filled.port_literals.get(field).is_some_and(|literal| as_instance_filled(literal).is_some()) {
            filled.port_literals.insert(field.clone(), value.clone());
        }
    }
    filled
}

/// Whose copy of a node a run reaches: the run's instance at a node that
/// exists once per instance (`per_instance` set), the shared copy
/// (`None`) at any other, whichever instance the run is for.
pub fn copy_owner(per_instance: Option<PerInstance>, instance: Option<&InstanceId>) -> Option<&InstanceId> {
    per_instance.and(instance)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_instance_header_is_honoured_only_behind_a_gate() {
        let with = |value: &str| std::collections::BTreeMap::from([("weft-instance".to_string(), value.to_string())]);
        assert_eq!(instance_from_header(&with("ada"), true).unwrap(), Some(InstanceId::new("ada").unwrap()));
        assert!(instance_from_header(&with("ada"), false).unwrap_err().contains("gated by a connection"));
        assert!(instance_from_header(&with("a/b"), true).is_err());
        assert_eq!(instance_from_header(&Default::default(), false).unwrap(), None);
    }

    #[test]
    fn a_run_reaches_its_instances_copy_only_at_a_per_instance_node() {
        let ada = InstanceId::new("ada").unwrap();
        assert_eq!(copy_owner(Some(PerInstance::Marked), Some(&ada)), Some(&ada));
        assert_eq!(copy_owner(Some(PerInstance::Derived), Some(&ada)), Some(&ada));
        assert_eq!(copy_owner(None, Some(&ada)), None);
        assert_eq!(copy_owner(Some(PerInstance::Marked), None), None);
    }

    #[test]
    fn an_instance_id_is_a_key_segment() {
        assert!(InstanceId::new("user-42").is_ok());
        assert!(InstanceId::new("a.b_c-D9").is_ok());
        for bad in ["", ".", "..", "a/b", "a b", "é", &"x".repeat(129)] {
            assert!(InstanceId::new(bad).is_err(), "{bad:?} must be refused");
        }
    }

    #[test]
    fn an_instance_id_is_refused_on_the_wire_too() {
        assert!(serde_json::from_str::<InstanceId>("\"../x\"").is_err());
        let id: InstanceId = serde_json::from_str("\"user-42\"").unwrap();
        assert_eq!(serde_json::to_string(&id).unwrap(), "\"user-42\"");
    }

    #[test]
    fn owner_round_trips() {
        for owner in [Owner::Shared, Owner::Instance(InstanceId::new("m").unwrap())] {
            let wire = serde_json::to_value(&owner).unwrap();
            assert_eq!(serde_json::from_value::<Owner>(wire).unwrap(), owner);
        }
        assert_eq!(serde_json::to_value(Owner::Shared).unwrap(), serde_json::json!({"kind": "shared"}));
        assert_eq!(
            serde_json::to_value(Owner::Instance(InstanceId::new("m").unwrap())).unwrap(),
            serde_json::json!({"kind": "instance", "instance": "m"})
        );
    }

    #[test]
    fn copies_round_trip_their_columns() {
        let m = InstanceId::new("m").unwrap();
        for copies in [Copies::Shared, Copies::Instance(m.clone()), Copies::Every] {
            let (instance, every) = copies.columns();
            assert_eq!(Copies::from_columns(instance.map(str::to_string), every).unwrap(), copies);
        }
        assert!(Copies::from_columns(Some("m".into()), true).is_err());
        assert!(Copies::Shared.admits(None) && !Copies::Shared.admits(Some(&m)));
        assert!(Copies::Instance(m.clone()).admits(Some(&m)) && !Copies::Instance(m.clone()).admits(None));
        assert!(Copies::Every.admits(None) && Copies::Every.admits(Some(&m)));
    }

    #[test]
    fn per_instance_wire() {
        assert_eq!(serde_json::to_value(PerInstance::Marked).unwrap(), "marked");
        assert_eq!(serde_json::to_value(PerInstance::Derived).unwrap(), "derived");
    }
}
