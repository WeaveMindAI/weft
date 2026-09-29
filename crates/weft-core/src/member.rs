//! Members: the end users of a program.
//!
//! One graph, written once by its author, can serve many people who each
//! bring their own accounts: a member's own connection, a member's own
//! copy of a container, the triggers listening on it. A member is an
//! opaque id the AUTHOR chooses (`"user-42"`); weft keeps no registry of
//! them. A member exists as long as something is attached to it, and the
//! same id always resolves to the same things.
//!
//! A member is always read together with its project: `(project, member)`
//! is the identity, so two projects that both say `"user-42"` share
//! nothing. Every table stores the two as separate columns and every
//! storage key as two separate path segments, never one joined string.

use serde::{Deserialize, Serialize};

/// The header a trusted backend names the member with, on a route gated
/// by a connection credential (never honoured on an open route, where
/// anybody could claim to be anybody).
// SYNC: MEMBER_HEADER <-> packages/weft-connect/src/core/transport.ts MEMBER_HEADER
pub const MEMBER_HEADER: &str = "Weft-Member";

/// The header a browser or extension presents its member token in, on a
/// route call. The token IS the identity: it names its member and its one
/// project, so no other header can override it.
// SYNC: MEMBER_TOKEN_HEADER <-> packages/weft-connect/src/core/transport.ts MEMBER_TOKEN_HEADER
pub const MEMBER_TOKEN_HEADER: &str = "Weft-Member-Token";

/// Who a call through a route is for, from its [`MEMBER_HEADER`]. The
/// header is honoured only on a route gated by a connection (`gated`):
/// the caller then proved they hold the route's credential, which makes
/// them the author's own backend. On an open route anybody could name any
/// member, so the header is refused there rather than ignored, and the
/// caller hears why. `headers` as received (any case).
pub fn member_from_header(
    headers: &std::collections::BTreeMap<String, String>,
    gated: bool,
) -> Result<Option<MemberId>, String> {
    let Some((_, value)) = headers.iter().find(|(name, _)| name.eq_ignore_ascii_case(MEMBER_HEADER)) else {
        return Ok(None);
    };
    if !gated {
        return Err(format!(
            "the {MEMBER_HEADER} header is honoured only on a route gated by a connection: on an open \
             route anybody could claim to be any member. Gate the route, or give the browser a member \
             token instead"
        ));
    }
    MemberId::new(value.trim()).map(Some).map_err(|e| format!("{MEMBER_HEADER}: {e}"))
}

/// The body directive that marks a node as existing once per member
/// (`@per_member`, on its own line inside the node's braces).
pub const PER_MEMBER_DIRECTIVE: &str = "per_member";

/// One member of a program: an id its author chose. Validated on the way
/// in, with the grammar of a storage key segment, so it can stand as a
/// path segment, a label value or a column without escaping and can
/// never forge a level (`..`, `/`).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct MemberId(String);

impl MemberId {
    /// The longest id accepted, the storage segment limit.
    pub const MAX_LEN: usize = 128;

    // SYNC: MemberId::new <-> packages/weft-graph/src/run-spec.ts MEMBER_ID_PATTERN
    pub fn new(id: impl Into<String>) -> Result<Self, String> {
        let id = id.into();
        if crate::storage::key::valid_segment(&id) {
            Ok(Self(id))
        } else {
            Err(format!(
                "'{id}' is not a valid member id: use 1 to {} letters, digits, '-', '_' or '.' \
                 (and not '.' or '..' alone)",
                Self::MAX_LEN
            ))
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for MemberId {
    type Error = String;
    fn try_from(id: String) -> Result<Self, String> {
        Self::new(id)
    }
}

impl From<MemberId> for String {
    fn from(id: MemberId) -> String {
        id.0
    }
}

impl std::fmt::Display for MemberId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::str::FromStr for MemberId {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        Self::new(s)
    }
}

/// Whose a per-project resource is: the program's own shared one, or one
/// member's. The key dimension every per-member resource carries (an infra
/// copy, a trigger activation, a connection, a pick, a stored file), so
/// every table spells it the same way: a nullable `member_id` column,
/// NULL for [`Owner::Shared`].
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default)]
#[serde(tag = "kind", content = "member", rename_all = "snake_case")]
pub enum Owner {
    #[default]
    Shared,
    Member(MemberId),
}

impl Owner {
    /// The owner a nullable `member_id` column holds.
    pub fn from_member(member: Option<MemberId>) -> Self {
        match member {
            Some(member) => Owner::Member(member),
            None => Owner::Shared,
        }
    }

    /// The value for a nullable `member_id` column.
    pub fn member(&self) -> Option<&MemberId> {
        match self {
            Owner::Shared => None,
            Owner::Member(member) => Some(member),
        }
    }

    /// The owner as a person reads it in a message: `shared`, or
    /// `member 'user-42'`.
    pub fn describe(&self) -> String {
        match self {
            Owner::Shared => "shared".into(),
            Owner::Member(member) => format!("member '{member}'"),
        }
    }
}

/// Which copies of the program's infra an infra verb acts on: the shared
/// ones (`weft infra stop`), one member's (`--member`, or a program's
/// `ctx.infra(..).member(..)`), or every copy there is (the project being
/// removed).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(tag = "kind", content = "member", rename_all = "snake_case")]
pub enum Copies {
    #[default]
    Shared,
    Member(MemberId),
    Every,
}

impl Copies {
    /// The copies an optional `--member` names: that member's, or the
    /// shared ones.
    pub fn of(member: Option<MemberId>) -> Self {
        match member {
            Some(member) => Copies::Member(member),
            None => Copies::Shared,
        }
    }

    /// Whether the copy owned by `member` (`None` = the shared copy) is
    /// among these.
    // SYNC: Copies::admits <-> crates/weft-broker-client/src/lifecycle_command.rs (command_reaches_copy)
    pub fn admits(&self, member: Option<&MemberId>) -> bool {
        match self {
            Copies::Shared => member.is_none(),
            Copies::Member(m) => member == Some(m),
            Copies::Every => true,
        }
    }

    /// The two columns a stored command keeps them in: `member_id`
    /// and `every_copy`.
    pub fn columns(&self) -> (Option<&str>, bool) {
        match self {
            Copies::Shared => (None, false),
            Copies::Member(m) => (Some(m.as_str()), false),
            Copies::Every => (None, true),
        }
    }

    /// Back from the two stored columns.
    pub fn from_columns(member: Option<String>, every: bool) -> Result<Self, String> {
        match (member, every) {
            (None, false) => Ok(Copies::Shared),
            (Some(m), false) => Ok(Copies::Member(MemberId::new(m)?)),
            (None, true) => Ok(Copies::Every),
            (Some(_), true) => Err("a command for every copy names no member".into()),
        }
    }
}

/// One member of one project: whom a use of a connection is for, where a
/// request carries it (a member's own lookup, a member's trigger reading
/// through their connection). `None` wherever it is carried is the
/// author, or a run for nobody.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemberScope {
    pub project_id: uuid::Uuid,
    pub member: MemberId,
}

impl MemberScope {
    /// Back from a row's two columns (`project_id`, `member_id`): the
    /// member's scope, or `None` for a row that is nobody's.
    pub fn from_columns(project_id: uuid::Uuid, member: Option<String>) -> Result<Option<Self>, String> {
        member.map(|m| Ok(MemberScope { project_id, member: MemberId::new(m)? })).transpose()
    }
}

/// Why a node exists once per member. The two starting points the author
/// writes: `Marked` is an infra node marked `@per_member` (one container
/// per member), `Filled` a node with a field written `@member_filled`
/// (each member provides its value). `Derived` is everything the compiler
/// found reading a per-member value downstream of one, which runs in that
/// member's runs.
// SYNC: PerMember <-> packages/weft-graph/src/protocol.ts PerMember
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PerMember {
    Marked,
    Filled,
    Derived,
}

/// The marker a field's value is written as when each member provides
/// it: `@member_filled`, or `@member_filled(<value>)` with the value used
/// for a member who gave none.
pub const MEMBER_FILLED_MARKER: &str = "member_filled";

/// The key a `@member_filled` literal is lowered to: the one JSON shape
/// the compiled definition carries it in (`{"__weft_member_filled__":
/// {}}`, or `{..: {"fallback": <value>}}`). A structured value rather
/// than a string, so no string a program writes can ever read as one;
/// the parser refuses the key inside a written object for the same
/// reason.
// SYNC: MEMBER_FILLED_KEY <-> packages/weft-graph/src/protocol.ts MEMBER_FILLED_KEY
pub const MEMBER_FILLED_KEY: &str = "__weft_member_filled__";

/// A field written `@member_filled`, as it sits in the compiled literals.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MemberFilled<'a> {
    /// The value for a member who gave none (`@member_filled(<value>)`);
    /// `None`: such a member leaves the field unwritten, and the node's
    /// default, optionality or required refusal decides.
    pub fallback: Option<&'a serde_json::Value>,
}

/// The literal `@member_filled` lowers to.
// SYNC: member_filled_literal <-> packages/weft-graph/src/protocol.ts memberFilledValue
pub fn member_filled_literal(fallback: Option<serde_json::Value>) -> serde_json::Value {
    let mut inner = serde_json::Map::new();
    if let Some(fallback) = fallback {
        inner.insert("fallback".into(), fallback);
    }
    serde_json::json!({ MEMBER_FILLED_KEY: inner })
}

/// `value` read as a `@member_filled` literal, `None` for any other value.
// SYNC: as_member_filled <-> packages/weft-graph/src/protocol.ts memberFilled
pub fn as_member_filled(value: &serde_json::Value) -> Option<MemberFilled<'_>> {
    let serde_json::Value::Object(map) = value else { return None };
    if map.len() != 1 {
        return None;
    }
    let inner = map.get(MEMBER_FILLED_KEY)?.as_object()?;
    Some(MemberFilled { fallback: inner.get("fallback") })
}

/// Every field of `node` written `@member_filled`, with its marker.
pub fn member_filled_fields(node: &crate::project::NodeDefinition) -> impl Iterator<Item = (&str, MemberFilled<'_>)> {
    node.port_literals.iter().filter_map(|(field, value)| Some((field.as_str(), as_member_filled(value)?)))
}

/// One member's values: field -> value, for the step at one place.
pub type PlaceValues = std::collections::BTreeMap<String, serde_json::Value>;

/// A member's values for a run: place (`read`, `one.read`) -> its fields.
/// Read once when the run is born and carried on it, so every firing of
/// the run sees the values it was born with.
pub type MemberValues = std::collections::BTreeMap<String, PlaceValues>;

/// A change to one member's values: fields given a value, and fields
/// cleared (which then fall back as if never given). What a store call
/// asks for, and what the setup it re-arms runs with before the change is
/// stored.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ValueChanges {
    pub set: MemberValues,
    /// `(place, field)` pairs.
    pub cleared: std::collections::BTreeSet<(String, String)>,
}

impl ValueChanges {
    /// `values` with this change made.
    pub fn apply(&self, values: &mut MemberValues) {
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

/// Put the member's values into `literals` (the constants delivered to
/// one firing of `node` at one place): each of the node's `@member_filled`
/// fields takes the member's value, else its fallback, else is removed, so
/// the node's default, optionality or required refusal applies exactly as
/// if the field had not been written. Only the node's own member-filled
/// fields are touched: a value on any other port, whatever its shape, is
/// left alone.
pub fn fill_literals(
    node: &crate::project::NodeDefinition,
    literals: &mut serde_json::Map<String, serde_json::Value>,
    values: Option<&PlaceValues>,
) {
    for (field, filled) in member_filled_fields(node) {
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

/// A value a member gives for `field` of `node`, held to what the field
/// takes, exactly as a value written in the source is: a connection or
/// resource handle to its shape, anything else to the port's type (cast
/// where the cast is unambiguous, a `"18"` for a Number) and to its
/// widget's domain. Answers the value as it is kept.
pub fn check_member_value(
    node: &crate::project::NodeDefinition,
    field: &str,
    value: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    if !member_filled_fields(node).any(|(filled, _)| filled == field) {
        return Err(format!("'{field}' of '{}' is not filled by each member (it is not `@member_filled`)", node.id));
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

/// The member-filled fields of `original` that `filled` (the node as one
/// member's run at `place` sees it, [`filled_node`]) leaves with nothing
/// where the node needs a value: required, no default, not optional.
/// One sentence each, naming the member and the place.
pub fn unfilled_gaps(
    original: &crate::project::NodeDefinition,
    filled: &crate::project::NodeDefinition,
    place: &str,
    member: &MemberId,
) -> Vec<String> {
    member_filled_fields(original)
        .filter(|(field, _)| !filled.port_literals.contains_key(*field))
        .filter(|(field, _)| {
            original
                .inputs
                .iter()
                .find(|i| i.name == *field)
                .is_some_and(|i| i.required && i.default.is_none() && !original.optional_ports.contains(*field))
        })
        .map(|(field, _)| format!("member '{member}' has not filled '{place}.{field}'"))
        .collect()
}

/// Every error rule of `original` (`member_rules`) that fires on `filled`,
/// the node with a member's values in (`crate::rules`), one sentence each
/// naming the member and the place. A field still `@member_filled` in
/// `filled` (a member who has not filled it yet, when their values are
/// checked as they are stored) reads as there with a value not known yet,
/// exactly as at compile time. A rule about a `@member_filled` field the
/// member left empty (the node's "connect one on the node", say) is said
/// the way a member can act on: that they have not filled it.
pub fn rule_gaps(
    project: &crate::project::ProjectDefinition,
    original: &crate::project::NodeDefinition,
    filled: &crate::project::NodeDefinition,
    place: &str,
    member: &MemberId,
) -> Vec<String> {
    let Some(rules) = &original.member_rules else { return Vec::new() };
    rules
        .rules
        .iter()
        .filter(|rule| rule.then.severity == crate::node::RuleSeverity::Error)
        .filter(|rule| crate::rules::fires(rule, filled, project, &rules.custom_outputs))
        .map(|rule| match rule.then.field.as_deref() {
            Some(field)
                if !filled.port_literals.contains_key(field)
                    && member_filled_fields(original).any(|(filled_field, _)| filled_field == field) =>
            {
                format!("member '{member}' has not filled '{place}.{field}'")
            }
            _ => format!("member '{member}' at '{place}': {}", crate::rules::message(rule, filled, &rules.custom_outputs)),
        })
        .collect()
}

/// Whether a run needs the member's own value for `field` of `node`: left
/// empty (no fallback), either the input is required with no default, or
/// one of the node's error rules about that field fires. The same two
/// checks a run's birth makes ([`unfilled_gaps`], [`rule_gaps`]), so a
/// member's page marks exactly the fields a run would refuse without.
pub fn member_field_needed(
    project: &crate::project::ProjectDefinition,
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
        || node.member_rules.as_ref().is_some_and(|rules| {
            rules.rules.iter().any(|rule| {
                rule.then.severity == crate::node::RuleSeverity::Error
                    && rule.then.field.as_deref() == Some(field)
                    && crate::rules::fires(rule, &empty, project, &rules.custom_outputs)
            })
        })
}

/// `node` as one member's run at one place sees it: its `@member_filled`
/// literals replaced by [`fill_literals`]. What a member's value is checked
/// against (the node's rules, its port types, its required inputs) is
/// this node, so the check and the run read the same thing.
pub fn filled_node(node: &crate::project::NodeDefinition, values: Option<&PlaceValues>) -> crate::project::NodeDefinition {
    let mut filled = node.clone();
    let mut literals: serde_json::Map<String, serde_json::Value> = std::mem::take(&mut filled.port_literals).into_iter().collect();
    fill_literals(node, &mut literals, values);
    filled.port_literals = literals.into_iter().collect();
    filled
}

/// The connection one member's run signs a `remote_select` field's lookup
/// with: the field's widget names an access input of the same node, and
/// the wire into that input leads (through any group or included file's
/// ports) to an access node. Its connection field is either picked on
/// the install (the author's connection, shared: `picks` give it) or
/// `@member_filled`, then `values` (the member's) give it. Nothing wired
/// there is [`LookupConnection::Unwired`]: the lookup then only works on
/// a public source. A wired connection the member has not filled yet (or
/// no connection picked at the access node) is
/// [`LookupConnection::NotConnected`], with why: the field listing shows
/// the field as unconnected, the lookup door refuses with that reason.
/// `Err` is only a program that cannot look anything up. The same walk the editor does over its canvas, over the
/// compiled program, so a member's dropdown lists what the step's run
/// would reach.
pub fn lookup_connection(
    project: &crate::project::ProjectDefinition,
    at: &crate::frames::Located,
    field: &str,
    values: &MemberValues,
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
        Some(literal) if as_member_filled(literal).is_some() => {
            match values.get(&spelled).and_then(|fields| fields.get(&input.name)) {
                Some(value) => (value.clone(), crate::member_door::FieldConnection::Own),
                None => {
                    return Ok(LookupConnection::NotConnected(format!(
                        "connect your account at '{spelled}' first: this list is read through it"
                    )))
                }
            }
        }
        Some(literal) if crate::picks::is_install_picked(literal) => {
            match picks.get(&spelled).and_then(|fields| fields.get(&input.name)) {
                Some(handle) => (handle.clone(), crate::member_door::FieldConnection::Shared),
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
    /// member has not connected their account, or the access node has no
    /// connection picked), and why, spelled for the member.
    NotConnected(String),
    /// The connection the lookup is signed with.
    Wired(WiredConnection),
}

impl LookupConnection {
    /// Whose connection the member's field listing shows: none until one
    /// is there to sign with.
    pub fn whose(&self) -> crate::member_door::FieldConnection {
        match self {
            LookupConnection::Wired(wired) => wired.whose,
            LookupConnection::Unwired | LookupConnection::NotConnected(_) => crate::member_door::FieldConnection::None,
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

/// The connection a member's `remote_select` lookup is signed with, and
/// whose it is: the member's own, or the author's shared one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WiredConnection {
    pub id: uuid::Uuid,
    pub service: String,
    /// [`FieldConnection::Own`] or [`FieldConnection::Shared`], never
    /// `None` (no connection is no `WiredConnection`).
    ///
    /// [`FieldConnection::Own`]: crate::member_door::FieldConnection::Own
    /// [`FieldConnection::Shared`]: crate::member_door::FieldConnection::Shared
    pub whose: crate::member_door::FieldConnection,
}

/// `node` with only the member-filled fields `values` give a value for
/// replaced: every other `@member_filled` stays one. How a member's values
/// are checked while they are still filling their fields in, one form at
/// a time: a rule about a field they have not reached yet has no answer.
pub fn partly_filled_node(node: &crate::project::NodeDefinition, values: &PlaceValues) -> crate::project::NodeDefinition {
    let mut filled = node.clone();
    for (field, value) in values {
        if filled.port_literals.get(field).is_some_and(|literal| as_member_filled(literal).is_some()) {
            filled.port_literals.insert(field.clone(), value.clone());
        }
    }
    filled
}

/// Whose copy of a node a run reaches: the run's member at a node that
/// exists once per member (`per_member` set), the shared copy (`None`)
/// at any other, whoever the run is for.
pub fn copy_owner(per_member: Option<PerMember>, member: Option<&MemberId>) -> Option<&MemberId> {
    per_member.and(member)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_member_header_is_honoured_only_behind_a_gate() {
        let with = |value: &str| std::collections::BTreeMap::from([("weft-member".to_string(), value.to_string())]);
        assert_eq!(member_from_header(&with("ada"), true).unwrap(), Some(MemberId::new("ada").unwrap()));
        assert!(member_from_header(&with("ada"), false).unwrap_err().contains("gated by a connection"));
        assert!(member_from_header(&with("a/b"), true).is_err());
        assert_eq!(member_from_header(&Default::default(), false).unwrap(), None);
    }

    #[test]
    fn a_run_reaches_its_members_copy_only_at_a_per_member_node() {
        let ada = MemberId::new("ada").unwrap();
        assert_eq!(copy_owner(Some(PerMember::Marked), Some(&ada)), Some(&ada));
        assert_eq!(copy_owner(Some(PerMember::Derived), Some(&ada)), Some(&ada));
        assert_eq!(copy_owner(None, Some(&ada)), None);
        assert_eq!(copy_owner(Some(PerMember::Marked), None), None);
    }

    #[test]
    fn a_member_id_is_a_key_segment() {
        assert!(MemberId::new("user-42").is_ok());
        assert!(MemberId::new("a.b_c-D9").is_ok());
        for bad in ["", ".", "..", "a/b", "a b", "é", &"x".repeat(129)] {
            assert!(MemberId::new(bad).is_err(), "{bad:?} must be refused");
        }
    }

    #[test]
    fn a_member_id_is_refused_on_the_wire_too() {
        assert!(serde_json::from_str::<MemberId>("\"../x\"").is_err());
        let id: MemberId = serde_json::from_str("\"user-42\"").unwrap();
        assert_eq!(serde_json::to_string(&id).unwrap(), "\"user-42\"");
    }

    #[test]
    fn owner_round_trips() {
        for owner in [Owner::Shared, Owner::Member(MemberId::new("m").unwrap())] {
            let wire = serde_json::to_value(&owner).unwrap();
            assert_eq!(serde_json::from_value::<Owner>(wire).unwrap(), owner);
        }
        assert_eq!(serde_json::to_value(Owner::Shared).unwrap(), serde_json::json!({"kind": "shared"}));
        assert_eq!(
            serde_json::to_value(Owner::Member(MemberId::new("m").unwrap())).unwrap(),
            serde_json::json!({"kind": "member", "member": "m"})
        );
    }

    #[test]
    fn copies_round_trip_their_columns() {
        let m = MemberId::new("m").unwrap();
        for copies in [Copies::Shared, Copies::Member(m.clone()), Copies::Every] {
            let (member, every) = copies.columns();
            assert_eq!(Copies::from_columns(member.map(str::to_string), every).unwrap(), copies);
        }
        assert!(Copies::from_columns(Some("m".into()), true).is_err());
        assert!(Copies::Shared.admits(None) && !Copies::Shared.admits(Some(&m)));
        assert!(Copies::Member(m.clone()).admits(Some(&m)) && !Copies::Member(m.clone()).admits(None));
        assert!(Copies::Every.admits(None) && Copies::Every.admits(Some(&m)));
    }

    #[test]
    fn per_member_wire() {
        assert_eq!(serde_json::to_value(PerMember::Marked).unwrap(), "marked");
        assert_eq!(serde_json::to_value(PerMember::Derived).unwrap(), "derived");
    }
}
