//! A run selects graph paths, supplies backup inputs, and can stand in for outputs.
//! Resolution is pure. Trigger preparation and seed lookup are caller-owned I/O.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::marker::PhantomData;

use serde::de::{Error, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::frames::{Located, LoopFrames};
use crate::project::selection::{source_place, RunSelection, SelectionBounds};
use crate::project::ProjectDefinition;
use crate::weft_type::WeftType;
use crate::Color;

pub type PortValues = BTreeMap<String, BTreeMap<String, Value>>;
/// A group's simulated outputs: the group id and one value per output port.
pub type GroupOutputs = (String, BTreeMap<String, Value>);

// SYNC: BakeSummary <-> packages/weft-graph/src/run-spec.ts BakeSummary
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BakeSummary {
    pub program: crate::project::hash::ProgramIdentity,
    pub captured: Vec<String>,
    pub color: Color,
    pub at_unix: u64,
}

/// Both preview and execution require a capture from the exact requested code.
pub fn validate_fire_bake(node: &str, program: &crate::project::hash::ProgramIdentity, bakes: &[BakeSummary]) -> Result<(), Refusal> {
    if bakes.iter().any(|bake| bake.program == *program && bake.captured.iter().any(|id| id == node)) {
        return Ok(());
    }
    // A bake is per code, so ANY edit to the graph leaves the last one
    // behind. That is the cause every single time somebody reads this,
    // so it leads; the hashes are for the rare case where it is not.
    let older = bakes.iter().filter(|bake| bake.captured.iter().any(|id| id == node))
        .max_by_key(|bake| bake.at_unix)
        .map(|bake| format!(" (the last one was made for graph {} and binary {})", bake.program.definition_hash, bake.program.binary_hash))
        .unwrap_or_default();
    Err(Refusal::error(format!(
        "the code changed since trigger '{node}' was last prepared, so its bake is stale{older}. Prepare it again: `weft bake` does it without listening, `weft activate` does it and listens"
    )))
}

/// The example file, run request, and editor value share one contract.
// SYNC: RunSpec <-> packages/weft-graph/src/run-spec.ts RunSpec
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunSpec {
    pub name: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty", deserialize_with = "deserialize_port_values")]
    pub from: PortValues,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty", deserialize_with = "deserialize_port_values")]
    pub emit: PortValues,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub target: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub before: Vec<String>,
    /// Starts whose feeders run too: for each port of the start not
    /// handed a value, the node that feeds it (and nothing above it).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub feed: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none", deserialize_with = "deserialize_group")]
    pub group: Option<GroupOutputs>,
    #[serde(default, skip_serializing_if = "Option::is_none", deserialize_with = "deserialize_unique")]
    pub fire: Option<(String, Value)>,
    #[serde(default, skip_serializing_if = "Vec::is_empty", deserialize_with = "deserialize_unique")]
    pub answers: Vec<Answer>,
    #[serde(default, skip_serializing_if = "Vec::is_empty", deserialize_with = "deserialize_unique")]
    pub caller: Vec<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub frozen_from: Option<FrozenFrom>,
    #[serde(default, skip_serializing_if = "Option::is_none", deserialize_with = "deserialize_unique")]
    pub expected: Option<Expected>,
}

fn deserialize_unique<'de, D: serde::Deserializer<'de>, T: serde::de::DeserializeOwned>(deserializer: D) -> Result<T, D::Error> {
    let UniqueValue(value) = UniqueValue::deserialize(deserializer)?;
    serde_json::from_value(value).map_err(D::Error::custom)
}

struct UniqueMap<T>(BTreeMap<String, T>);

impl<'de, T: Deserialize<'de>> Deserialize<'de> for UniqueMap<T> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct MapVisitor<T>(PhantomData<T>);
        impl<'de, T: Deserialize<'de>> Visitor<'de> for MapVisitor<T> {
            type Value = UniqueMap<T>;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("an object with unique keys")
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut entries = BTreeMap::new();
                while let Some(key) = map.next_key::<String>()? {
                    if entries.contains_key(&key) {
                        return Err(A::Error::custom(format!("duplicate key '{key}'")));
                    }
                    entries.insert(key, map.next_value()?);
                }
                Ok(UniqueMap(entries))
            }
        }
        deserializer.deserialize_map(MapVisitor(PhantomData))
    }
}

fn deserialize_port_values<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<PortValues, D::Error> {
    Ok(UniqueMap::<UniqueMap<UniqueValue>>::deserialize(deserializer)?.0.into_iter()
        .map(|(node, ports)| (node, ports.0.into_iter().map(|(port, value)| (port, value.0)).collect())).collect())
}

fn deserialize_group<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<Option<GroupOutputs>, D::Error> {
    let group = Option::<(String, UniqueMap<UniqueValue>)>::deserialize(deserializer)?;
    Ok(group.map(|(id, ports)| (id, ports.0.into_iter().map(|(port, value)| (port, value.0)).collect())))
}

/// Parse one node's supplied ports without losing duplicate keys.
pub fn parse_port_object(text: &str) -> Result<BTreeMap<String, Value>, serde_json::Error> {
    serde_json::from_str::<UniqueMap<UniqueValue>>(text)
        .map(|ports| ports.0.into_iter().map(|(port, value)| (port, value.0)).collect())
}

/// JSON values retain arbitrary nesting, but never silently overwrite a key.
struct UniqueValue(Value);

impl<'de> Deserialize<'de> for UniqueValue {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ValueVisitor;
        impl<'de> Visitor<'de> for ValueVisitor {
            type Value = UniqueValue;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a JSON value with unique object keys")
            }

            fn visit_unit<E: Error>(self) -> Result<Self::Value, E> { Ok(UniqueValue(Value::Null)) }
            fn visit_none<E: Error>(self) -> Result<Self::Value, E> { self.visit_unit() }
            fn visit_bool<E: Error>(self, value: bool) -> Result<Self::Value, E> { Ok(UniqueValue(value.into())) }
            fn visit_i64<E: Error>(self, value: i64) -> Result<Self::Value, E> { Ok(UniqueValue(value.into())) }
            fn visit_u64<E: Error>(self, value: u64) -> Result<Self::Value, E> { Ok(UniqueValue(value.into())) }
            fn visit_f64<E: Error>(self, value: f64) -> Result<Self::Value, E> {
                serde_json::Number::from_f64(value).map(|number| UniqueValue(Value::Number(number)))
                    .ok_or_else(|| E::custom("JSON numbers must be finite"))
            }
            fn visit_str<E: Error>(self, value: &str) -> Result<Self::Value, E> { Ok(UniqueValue(value.into())) }
            fn visit_string<E: Error>(self, value: String) -> Result<Self::Value, E> { Ok(UniqueValue(value.into())) }

            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                let mut values = Vec::new();
                while let Some(UniqueValue(value)) = seq.next_element()? { values.push(value); }
                Ok(UniqueValue(Value::Array(values)))
            }

            fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<Self::Value, A::Error> {
                let values = UniqueMap::<UniqueValue>::deserialize(serde::de::value::MapAccessDeserializer::new(map))?;
                Ok(UniqueValue(Value::Object(values.0.into_iter().map(|(key, value)| (key, value.0)).collect())))
            }
        }
        deserializer.deserialize_any(ValueVisitor)
    }
}

/// A person's answer to a human step, as recorded at freeze time:
/// `question` is what the node showed the person, so whoever answers on
/// a new run reads old question, old answer, new question. The runtime
/// never replays answers; Tangle plays the person.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Answer {
    pub node: String,
    #[serde(default)]
    pub frames: LoopFrames,
    pub payload: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub question: Option<Value>,
}

/// Where a frozen example's `expected` came from.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrozenFrom {
    pub version: String,
    pub color: Color,
    pub definition_hash: String,
}

// SYNC: Expected <-> packages/weft-graph/src/run-spec.ts Expected
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Expected {
    /// Node declarations in the captured program, including nodes with no output.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub nodes: Vec<String>,
    /// Nodes to emphasize in review. All output evidence remains in wires.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub focus: Vec<String>,
    pub wires: Vec<ExpectedWire>,
}

/// One ordered output item or closure at a node and loop position, as
/// an example file shows it: `node` is the address a person types
/// (`one.strip`, through the site for a node in an included file) and
/// `frames` holds the loop positions only. `ExpectedWire::spell` makes
/// one from the journal's `OutputWire`.
// SYNC: ExpectedWire <-> packages/weft-graph/src/run-spec.ts ExpectedWire
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExpectedWire {
    pub node: String,
    pub port: String,
    #[serde(default)]
    pub frames: LoopFrames,
    #[serde(default)]
    pub ordinal: u64,
    #[serde(default)]
    pub closed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub value: Value,
}

/// One ordered output item or closure as the journal holds it: the
/// node's compiled id under its full frame stack (call and loop
/// frames). What seeding reads; `ExpectedWire::spell` is what a person
/// reads.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct OutputWire {
    pub node: String,
    pub frames: LoopFrames,
    pub port: String,
    pub ordinal: u64,
    pub closed: bool,
    pub error: Option<String>,
    pub value: Value,
}

impl OutputWire {
    /// The node at the call this output belongs to.
    pub fn place(&self) -> Located {
        Located::at(&self.node, &self.frames)
    }
}

impl ExpectedWire {
    /// The wire as an example file spells it: the node's address
    /// through its sites, the loop positions kept, the call frames
    /// folded into the address.
    pub fn spell(project: &ProjectDefinition, wire: &OutputWire) -> Self {
        let place = wire.place();
        Self {
            node: crate::project::address_of(project, &place.id, &place.path),
            port: wire.port.clone(),
            frames: wire.frames.iter().filter(|frame| frame.loop_index().is_some()).cloned().collect(),
            ordinal: wire.ordinal,
            closed: wire.closed,
            error: wire.error.clone(),
            value: wire.value.clone(),
        }
    }
}

impl RunSpec {
    /// Explicit starting values addressed to runtime entry nodes.
    /// The backups keyed by the place that reads them: a start spelled
    /// from the top (`triage.up`, or a group or a call site) resolves
    /// to its node (a group's or site's In boundary) under the calls
    /// the spelling walked.
    pub fn starting_inputs(&self, project: &ProjectDefinition) -> BTreeMap<Located, BTreeMap<String, Value>> {
        self.from.iter().chain(self.group.iter().map(|(id, ports)| (id, ports)))
            .map(|(spelled, ports)| (start_place(project, spelled), ports.clone())).collect()
    }

    /// A whole-graph run: what a plain `weft run` sends.
    pub fn whole(name: impl Into<String>) -> Self {
        Self { name: name.into(), ..Self::default() }
    }

    /// Whether this spec is a frozen example (`expected` filled).
    pub fn is_frozen(&self) -> bool {
        self.expected.is_some()
    }

    /// The spec without what freezing added, for a run that replays it.
    pub fn without_expected(&self) -> Self {
        Self { frozen_from: None, expected: None, ..self.clone() }
    }
}

/// Where a start spelled `spelled` sits: the node, or a group's In door.
fn start_place(project: &ProjectDefinition, spelled: &str) -> Located {
    let (id, path) = crate::project::resolve_address(project, spelled);
    let id = if project.groups.iter().any(|group| group.id == id) {
        crate::project::boundary_in_id(&id)
    } else { id };
    Located::new(id, path)
}

/// One root the dispatcher kicks.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KickPlan {
    pub node: String,
    /// The frames the kick fires under: one call frame per site on the
    /// node's call path, for a root inside an included file reached
    /// through a site (`--from triage.up`); empty at the top.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub frames: crate::frames::LoopFrames,
    /// The trigger fired by hand: its payload is the event.
    pub firing: bool,
    pub payload: Option<Value>,
    /// What this trigger registered at activation, for a firing kick:
    /// a fired trigger REPLAYS its setup-time ports instead of reading
    /// them off the wire. It rides on the kick because the resolver
    /// already had to know it (the same fact is what let those ports
    /// pass as supplied); a caller that re-looked-it-up would be a
    /// second source of one truth, and the two did diverge: the seeded
    /// path used to build kicks without it and refire a wired trigger
    /// that the resolver had just accepted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port_snapshot: Option<Value>,
}

impl KickPlan {
    /// The shared root intent for both listener fires and authored runs.
    pub fn for_selection(project: &ProjectDefinition, selection: &RunSelection, fire: Option<(&Located, &Value)>, port_snapshot: Option<&Value>) -> Vec<Self> {
        let mut roots: BTreeSet<_> = selection.roots(project).into_iter().collect();
        roots.extend(selection.nodes.iter().filter(|place| project.nodes.iter().any(|node| node.id == place.id && node.features.is_trigger)).cloned());
        // A root is kicked at its place: under the call frames of the
        // sites that reach it, none at the top.
        roots.into_iter().map(|place| {
            let event = fire.filter(|(at, _)| *at == &place);
            Self {
                frames: place.frames(),
                node: place.id,
                firing: event.is_some(), payload: event.map(|(_, value)| value.clone()),
                port_snapshot: event.and(port_snapshot).cloned(),
            }
        }).collect()
    }
}

/// A value handed to a wire, as the emission its source would have
/// made: journaled as `PortEmitted { node: source, port: source_port,
/// provided: true }` and fanned out over the source port's wires.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProvidedEmission {
    pub source_node: String,
    pub source_port: String,
    /// The frames the value is emitted under: the call the source is
    /// spelled through (`--emit triage.up=...`), empty at the top.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub frames: crate::frames::LoopFrames,
    pub value: Value,
    /// The `(node, port)` pairs inside the scope the value reaches.
    pub consumers: Vec<(String, String)>,
}

/// What the runtime will execute and what the caller should explain.
// SYNC: Resolved <-> packages/weft-graph/src/run-spec.ts Resolved
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Resolved {
    pub selection: RunSelection,
    pub kicks: Vec<KickPlan>,
    pub provided: Vec<ProvidedEmission>,
    pub crossings: Vec<CrossingPort>,
    pub warnings: Vec<String>,
}

/// A selected input whose wire's source does not execute in this run.
// SYNC: CrossingPort <-> packages/weft-graph/src/run-spec.ts CrossingPort
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CrossingPort {
    /// The receiving node, spelled the way the program reads it
    /// (`one.strip` for a node of an included file under the site
    /// `one`), the same key a `from` entry uses.
    pub node: String,
    pub port: String,
    pub source_node: String,
    pub source_port: String,
    /// Whether something in the run needs this input: `needed_by` is set.
    pub required: bool,
    /// The required input this one ends up feeding, spelled like `node`:
    /// the node's own input, or one inside the group, loop or included
    /// file it enters.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub needed_by: Option<String>,
    /// Where to hand the missing value: the first start of the run the
    /// value would pass, spelled like `node`. A group run inside an
    /// include gets its enclosing doors in the run too, so the crossing
    /// itself can be an outer door the person never named.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hand_at: Option<StartPort>,
    pub supplied: bool,
}

/// One input port of a run's start.
// SYNC: StartPort <-> packages/weft-graph/src/run-spec.ts StartPort
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StartPort {
    pub node: String,
    pub port: String,
}

// SYNC: Refusal <-> packages/weft-graph/src/run-spec.ts Refusal
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Refusal {
    pub errors: Vec<String>,
}

impl Refusal {
    pub fn error(message: impl Into<String>) -> Self {
        Self { errors: vec![message.into()] }
    }

    pub fn is_empty(&self) -> bool {
        self.errors.is_empty()
    }
}

impl fmt::Display for Refusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.errors.join("\n"))
    }
}

impl std::error::Error for Refusal {}

/// Resolve only immutable graph/spec facts. Bake validation attaches the
/// captured trigger ports to the returned firing kick before execution birth.
pub fn resolve_spec(spec: &RunSpec, project: &ProjectDefinition) -> Result<Resolved, Refusal> {
    let mut selection = RunSelection::carve(project, &SelectionBounds {
        from: spec.from.keys().cloned().collect(), emit: spec.emit.keys().cloned().collect(),
        target: spec.target.clone(), before: spec.before.clone(), group: spec.group.as_ref().map(|(id, _)| id.clone()),
        fire: spec.fire.as_ref().map(|(node, _)| node.clone()),
        // A handed value wins over a feeder, so a handed port is not fed.
        feed: spec.feed.iter().map(|start| {
            let handed = spec.from.get(start)
                .or_else(|| spec.group.as_ref().filter(|(group, _)| group == start).map(|(_, ports)| ports))
                .map(|ports| ports.keys().cloned().collect()).unwrap_or_default();
            (start.clone(), handed)
        }).collect(),
    }).map_err(Refusal::error)?;
    let starting_inputs = spec.starting_inputs(project);
    selection.input = starting_inputs.clone();
    // Emits are spelled from the top too (`triage.up`); the rows they
    // become carry the source's id and its call frames.
    let emits: BTreeMap<Located, BTreeMap<String, Value>> = spec.emit.iter()
        .map(|(spelled, ports)| { let (id, path) = crate::project::resolve_address(project, spelled); (Located::new(id, path), ports.clone()) }).collect();
    let mut refusal = Refusal::default();
    let mut warnings = Vec::new();
    for (is_input, supplied) in [(true, &starting_inputs), (false, &emits)] {
        for (place, ports) in supplied {
            let spelled = crate::project::address_of(project, &place.id, &place.path);
            let Some(node) = project.nodes.iter().find(|n| n.id == place.id) else {
                refusal.errors.push(format!("unknown node '{spelled}'"));
                continue;
            };
            if is_input && !ports.is_empty() && !selection.nodes.contains(place) {
                refusal.errors.push(format!("supplied start '{spelled}' is outside the selected run"));
            }
            if is_input && node.features.is_trigger {
                refusal.errors.push(format!("trigger '{spelled}' cannot be a from start; use --fire with a prepared trigger or --emit to supply its outputs"));
            }
            for (port, value) in ports {
                let declared = if is_input {
                    node.inputs.iter().find(|p| &p.name == port).map(|p| &p.port_type)
                } else {
                    node.outputs.iter().find(|p| &p.name == port).map(|p| &p.port_type)
                };
                match declared {
                    None if is_input => {
                        warnings.push(format!("ignored supplied input '{spelled}.{port}': this input port does not exist in the current program"));
                        selection.input.get_mut(place).expect("starting input map exists").remove(port);
                    }
                    None => refusal.errors.push(format!("unknown output port '{spelled}.{port}'")),
                    // The language's flow gate accepts any value: false stops,
                    // other values permit flow. Its generic type is intentional.
                    Some(_) if is_input && crate::exec::skip::is_gate_port(port) => {},
                    Some(ty) => if let Err(error) = validate_supplied_value(ty, value) {
                        refusal.errors.push(format!("{spelled}.{port}: {error}"));
                    },
                }
            }
        }
    }
    let fire = spec.fire.as_ref().map(|(spelled, value)| {
        let (id, path) = crate::project::resolve_address(project, spelled);
        (Located::new(id, path), value)
    });
    if let Some((fire, payload)) = &fire {
        if !selection.nodes.contains(fire) {
            refusal.errors.push(format!("fired trigger '{}' is outside the selected run", crate::project::address_of(project, &fire.id, &fire.path)));
        }
        // What the trigger wakes with is declared (`firesWith`), so a
        // payload that does not match is refused HERE, before anything
        // is built or started, and the refusal prints the shape it
        // wanted. The engine holds every firing to the same contract;
        // this is the same check, early, where the author is still
        // looking at the command they typed.
        if let Some(node) = project.nodes.iter().find(|n| n.id == fire.id) {
            if let Err(why) = crate::node::check_fire_payload(&node.fires_with, Some(payload)) {
                refusal.errors.push(format!(
                    "--fire {}: {why}",
                    crate::project::address_of(project, &fire.id, &fire.path)
                ));
            }
        }
    }
    if !refusal.is_empty() { return Err(refusal); }
    if selection.nodes.is_empty() && selection.suppliers.is_empty() {
        return Err(Refusal::error("this selection is empty; choose starts and endpoints on a connected path"));
    }
    let crossings = crossings_of(project, &selection);
    let kicks = KickPlan::for_selection(project, &selection, fire.as_ref().map(|(at, value)| (at, *value)), None);
    let mut provided = Vec::new();
    for (place, ports) in &emits {
        for (port, value) in ports {
            provided.push(ProvidedEmission {
                source_node: place.id.clone(), source_port: port.clone(), frames: place.frames(), value: value.clone(),
                consumers: project.edges.iter().filter(|edge| edge.source == place.id
                    && edge.source_handle.as_deref().unwrap_or("default") == port
                    && selection.has_edge(project, place, edge, true))
                    .map(|edge| (edge.target.clone(), edge.target_handle.as_deref().unwrap_or("default").into())).collect(),
            });
        }
    }
    if !spec.from.is_empty() && spec.target.is_empty() && spec.before.is_empty() {
        warnings.push("No target or before endpoint: the run continues to the end of its downstream graph.".into());
    }
    // A trigger in the run either fires (the one named by `--fire`) or
    // does nothing: it never reads its wires and closes its outputs. Say
    // per trigger what that costs this run, because from the graph alone
    // it looks like any other node. A fire also carved every wire into
    // the fired trigger out of the run (it replays the inputs its bake
    // captured), and a producer whose only consumer was that wire is
    // not in the run at all, so the wire is named whether or not its
    // producer survived the carve.
    let fired = fire.as_ref().map(|(at, _)| at);
    let spell = |place: &Located| crate::project::address_of(project, &place.id, &place.path);
    let triggers: Vec<Located> = selection.nodes.iter()
        .filter(|place| project.nodes.iter().any(|node| node.id == place.id && node.features.is_trigger)).cloned().collect();
    for trigger in &triggers {
        if fired == Some(trigger) {
            let undelivered: Vec<String> = project.edges.iter()
                .filter(|edge| edge.target == trigger.id && !selection.has_edge(project, trigger, edge, false))
                .map(|edge| format!("{}.{}", edge.source, edge.source_handle.as_deref().unwrap_or("default")))
                .collect();
            if !undelivered.is_empty() {
                warnings.push(format!(
                    "{} not delivered to trigger '{}': a fired trigger reads the inputs its bake captured; change them with weft bake.",
                    undelivered.join(", "), spell(trigger)
                ));
            }
            continue;
        }
        // A group's boundaries spell as the group: a set names it once.
        let downstream: Vec<String> = RunSelection::downstream(project, std::slice::from_ref(trigger)).into_iter()
            .filter(|place| place != trigger && selection.nodes.contains(place))
            .map(|place| spell(&place))
            .collect::<BTreeSet<_>>().into_iter().collect();
        let why = match fired {
            Some(fired) => format!("only '{}' fires in this run", spell(fired)),
            None => "a run started by hand fires no trigger".to_string(),
        };
        let cost = if downstream.is_empty() { String::new() } else {
            format!(", and {} skip unless something else feeds them", downstream.join(", "))
        };
        warnings.push(format!(
            "trigger '{}' does not fire: {why}, so it closes its outputs{cost}. `--fire {}=<payload>` replays a wake.",
            spell(trigger), spell(trigger)
        ));
    }
    // An unfed input something needs is a refusal (`refuse_unfed`), and
    // that check waits for the selection's final shape: a seeded run's
    // history can feed it. What nothing needs only closes, and says so.
    for crossing in crossings.iter().filter(|c| !c.supplied && !c.required) {
        warnings.push(format!("{}.{} gets nothing in this run ({}.{} is outside it), so it closes; nothing that runs goes without it.",
            crossing.node, crossing.port, crossing.source_node, crossing.source_port));
    }
    Ok(Resolved { selection, kicks, provided, crossings, warnings })
}

/// The inputs of the run whose wire comes from a place that does not
/// run, each saying whether something in the run needs it (followed
/// through boundaries to the node that consumes it, see
/// [`crate::project::selection::required_consumer`]) and whether this
/// run feeds it anyway (a starting value, or a supplier standing in for
/// the source).
pub fn crossings_of(project: &ProjectDefinition, selection: &RunSelection) -> Vec<CrossingPort> {
    selection.nodes.iter().flat_map(|place| {
        project.edges.iter().filter(move |edge| edge.target == place.id)
            .filter_map(move |edge| source_place(project, place, edge).map(|source| (edge, source)))
            .filter(move |(edge, source)| selection.has_edge(project, place, edge, false) && !selection.nodes.contains(source))
            .map(move |(edge, source)| {
                let port = edge.target_handle.as_deref().unwrap_or("default");
                let is_trigger = |at: &Located| project.nodes.iter().any(|n| n.id == at.id && n.features.is_trigger);
                // A value matters where the run executes, never at a
                // trigger (a fired one reads its bake, the others close
                // without reading), and not past a start handed it.
                let counts = |at: &Located, port: &str| selection.nodes.contains(at) && !is_trigger(at)
                    && !selection.input.get(at).is_some_and(|ports| ports.contains_key(port));
                let need = crate::project::selection::required_consumer(
                    project, place, port, &counts, &|at| selection.input.contains_key(at),
                    &|at, port| gets_value(project, selection, at, port),
                );
                let spell = |at: &Located| crate::project::address_of(project, &at.id, &at.path);
                let needed_by = need.as_ref().map(|need| format!("{}.{}", spell(&need.needer.0), need.needer.1));
                let hand_at = need.as_ref().and_then(|need| need.start.as_ref())
                    .map(|(at, port)| StartPort { node: spell(at), port: port.clone() });
                CrossingPort {
                    node: crate::project::address_of(project, &place.id, &place.path), port: port.into(),
                    source_node: crate::project::address_of(project, &source.id, &source.path),
                    source_port: edge.source_handle.as_deref().unwrap_or("default").into(),
                    required: needed_by.is_some(),
                    needed_by,
                    hand_at,
                    supplied: selection.input.get(place).is_some_and(|ports| ports.contains_key(port))
                        || selection.suppliers.contains(&source),
                }
            })
    }).collect()
}

/// Whether an input of the node at `at` gets a value in this run other
/// than over a crossing: handed at a start, written in the source, a
/// default, or a wire from something the run executes or supplies. Read
/// generously (a wire from a place that runs counts even if that place
/// could skip), so a refusal built on it is never wrong.
fn gets_value(project: &ProjectDefinition, selection: &RunSelection, at: &Located, port: &str) -> bool {
    let Some(node) = project.nodes.iter().find(|n| n.id == at.id) else { return false };
    selection.input.get(at).is_some_and(|ports| ports.contains_key(port))
        || node.port_literals.get(port).is_some_and(|value| crate::exec::ready::literal_is_data(node, port, value))
        || node.inputs.iter().any(|input| input.name == port && input.default.is_some())
        || selection.has_supplier(project, at, port)
}

/// Refuse a run in which an input something needs gets nothing: its
/// wire's source does not run, no starting value stands in, and an input
/// its node cannot run without (on the node itself, or inside the group
/// it enters) would close. Checked against the selection as it will run,
/// after a seed has had its say, since history can feed a crossing. Each
/// is named where the person can act on it.
pub fn refuse_unfed(project: &ProjectDefinition, selection: &RunSelection, spec: &RunSpec) -> Result<(), Refusal> {
    let flag = start_flag(project, spec);
    let errors: Vec<String> = crossings_of(project, selection).into_iter()
        .filter(|c| !c.supplied)
        .filter_map(|c| c.needed_by.clone().map(|needed_by| (c, needed_by)))
        .map(|(c, needed_by)| {
            // Named where the person can act: the start the value would
            // pass, else the input the wire lands on.
            let (node, port) = match &c.hand_at {
                Some(at) => (at.node.clone(), at.port.clone()),
                None => (c.node.clone(), c.port.clone()),
            };
            let inside = if needed_by == format!("{node}.{port}") { String::new() }
                else { format!(", and {needed_by} cannot run without it") };
            let (src, src_port) = (&c.source_node, &c.source_port);
            let fix = match &c.hand_at {
                Some(_) => format!(
                    "Hand it a value at this start ({} {node}='{{\"{port}\": ...}}'), run what feeds it \
                     with --feed {node}, or start further up so {src} runs too.",
                    flag(&node),
                ),
                None => format!(
                    "Start at it with a value (--from {node}='{{\"{port}\": ...}}'), or start further \
                     up so {src} runs too."
                ),
            };
            format!("{node}.{port} gets nothing in this run: {src}.{src_port} is outside it{inside}. {fix}")
        })
        .collect();
    if errors.is_empty() { Ok(()) } else { Err(Refusal { errors }) }
}

/// Everything that makes a run pointless before it starts, checked
/// against the selection as it will run: an input something needs that
/// gets nothing ([`refuse_unfed`]), and a start behind a gate that cannot
/// open in this run (its own `_should_flow`, or a group's around it,
/// whose value only comes from triggers the run does not fire or from
/// outside the run), which would report "completed" with the start
/// skipped.
pub fn refuse_unrunnable(project: &ProjectDefinition, selection: &RunSelection, spec: &RunSpec) -> Result<(), Refusal> {
    let mut refusal = refuse_unfed(project, selection, spec).err().unwrap_or_default();
    let fired = spec.fire.as_ref().map(|(spelled, _)| {
        let (id, path) = crate::project::resolve_address(project, spelled);
        Located::new(id, path)
    });
    let spell = |at: &Located| crate::project::address_of(project, &at.id, &at.path);
    let flag = start_flag(project, spec);
    for start in selection.input.keys() {
        for shut in selection.shut_gates(project, start, fired.as_ref()) {
            let named = spell(start);
            let why = if shut.triggers.is_empty() {
                "its gate's value comes from outside this run".to_string()
            } else {
                let names: Vec<String> = shut.triggers.iter().map(spell).collect();
                format!("its gate only comes from {}, which this run does not fire", names.join(", "))
            };
            let fire = shut.triggers.first().map(|t| format!(", --fire {}='<event>'", spell(t))).unwrap_or_default();
            refusal.errors.push(if shut.door == *start {
                format!(
                    "{named} would skip: {why}. Hand it a gate value ({} {named}='{{\"_should_flow\": true}}'){fire}, \
                     or start further up.",
                    flag(&named),
                )
            } else {
                // A group around the start: only a start at that group can
                // be handed its gate, named with the flag this start used
                // (`--group` runs a group alone, `--from` from there on).
                let group = spell(&shut.door);
                format!(
                    "{named} would skip: the group {group} around it is shut, as {why}. Start at the group \
                     instead, with a gate value ({} {group}='{{\"_should_flow\": true}}'){fire}, or start further up.",
                    flag(&named),
                )
            });
        }
    }
    if refusal.is_empty() { Ok(()) } else { Err(refusal) }
}

/// The flag a value for the start spelled `node` is handed with: the
/// `--group` start's own, else `--from`.
fn start_flag(project: &ProjectDefinition, spec: &RunSpec) -> impl Fn(&str) -> &'static str {
    let group = spec.group.as_ref().map(|(spelled, _)| {
        let at = start_place(project, spelled);
        crate::project::address_of(project, &at.id, &at.path)
    });
    move |node| if group.as_deref() == Some(node) { "--group" } else { "--from" }
}

/// A supplied generator is an ordered list of items followed by a clean end.
pub fn validate_supplied_value(ty: &WeftType, value: &Value) -> Result<(), String> {
    if let WeftType::Generator(item) = ty {
        let items = value.as_array().ok_or_else(|| "a supplied generator must be a list of items".to_string())?;
        for (index, value) in items.iter().enumerate() {
            item.validate_value(value).map_err(|error| format!("item {index}: {error}"))?;
        }
        Ok(())
    } else {
        ty.validate_value(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn fire_bake_requires_full_identity_and_an_actual_capture() {
        let program = crate::project::hash::ProgramIdentity {
            definition_hash: "graph".into(), binary_hash: "binary".into(),
            implementations: BTreeMap::from([("T".into(), "implementation".into())]),
        };
        let mut bake = BakeSummary { program: program.clone(), captured: vec!["trigger".into()], color: Color::nil(), at_unix: 1 };
        assert!(validate_fire_bake("trigger", &program, &[bake.clone()]).is_ok());
        assert!(validate_fire_bake("trigger", &program, &[]).unwrap_err().to_string().contains("weft bake"));
        bake.captured.clear();
        assert!(validate_fire_bake("trigger", &program, &[bake.clone()]).is_err());
        bake.captured.push("trigger".into());
        for changed in ["graph", "binary", "implementation"] {
            let mut other = program.clone();
            match changed {
                "graph" => other.definition_hash.push('2'),
                "binary" => other.binary_hash.push('2'),
                _ => { other.implementations.insert("T".into(), "changed".into()); }
            }
            let refusal = validate_fire_bake("trigger", &other, &[bake.clone()]).unwrap_err().to_string();
            // The cause leads (every reader of this got here by editing
            // the graph); the hashes stay, for the rare case where it
            // was something else.
            assert!(refusal.contains("the code changed since"), "{refusal}");
            assert!(refusal.contains("weft bake"), "it names the fix: {refusal}");
            assert!(refusal.contains("made for graph"), "{refusal}");
        }
        let roundtrip: BakeSummary = serde_json::from_value(serde_json::to_value(&bake).unwrap()).unwrap();
        assert_eq!(roundtrip.program, program);
    }

    fn program() -> ProjectDefinition {
        serde_json::from_value(json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "nodes": (["a", "b", "c"].iter().map(|id| json!({
                "id": id, "nodeType": "T", "label": null, "config": {},
                "position": {"x": 0, "y": 0},
                "inputs": [{"name": "in", "portType": "String", "required": true}],
                "outputs": [{"name": "out", "portType": "String", "required": true}],
                "features": {}, "requiresInfra": false
            })).collect::<Vec<_>>()),
            "edges": ([("a", "b"), ("b", "c")].iter().map(|(a, b)| json!({
                "id": format!("{a}->{b}"), "source": a, "target": b,
                "sourceHandle": "out", "targetHandle": "in"
            })).collect::<Vec<_>>())
        })).unwrap()
    }

    #[test]
    fn supplied_flow_gate_accepts_values_without_resolving_its_generic_type() {
        let mut project = program();
        project.nodes[0].inputs.push(serde_json::from_value(json!({
            "name": "_should_flow", "portType": "T__should_flow", "required": false
        })).unwrap());
        for value in [json!(false), json!(true), json!("go")] {
            let spec = RunSpec { from: BTreeMap::from([("a".into(),
                BTreeMap::from([("_should_flow".into(), value.clone())]))]), ..RunSpec::whole("x") };
            let resolved = resolve_spec(&spec, &project).unwrap();
            assert_eq!(resolved.selection.input[&Located::top("a")]["_should_flow"], value);
        }
    }

    #[test]
    fn an_unfed_required_crossing_is_refused_and_a_backup_feeds_it() {
        let mut spec = RunSpec { from: BTreeMap::from([("b".into(), BTreeMap::new())]), ..RunSpec::whole("x") };
        let resolved = resolve_spec(&spec, &program()).unwrap();
        assert!(!resolved.selection.nodes.contains(&Located::top("a")));
        assert_eq!(resolved.crossings.len(), 1);
        assert!(!resolved.crossings[0].supplied);
        assert_eq!(resolved.crossings[0].needed_by.as_deref(), Some("b.in"));
        let refusal = refuse_unfed(&program(), &resolved.selection, &spec).unwrap_err().to_string();
        assert!(refusal.contains("b.in gets nothing in this run: a.out is outside it"), "{refusal}");
        spec.from.insert("b".into(), BTreeMap::from([("in".into(), json!("backup"))]));
        let resolved = resolve_spec(&spec, &program()).unwrap();
        assert!(resolved.crossings[0].supplied);
        refuse_unfed(&program(), &resolved.selection, &spec).expect("the backup feeds it");
        assert!(!resolved.selection.nodes.contains(&Located::top("a")));
        assert!(resolved.provided.is_empty(), "input backups are not source emissions");
    }

    #[test]
    fn an_unfed_optional_crossing_only_warns() {
        let mut project = program();
        project.nodes[1].inputs[0].port.required = false;
        let spec = RunSpec { from: BTreeMap::from([("b".into(), BTreeMap::new())]), ..RunSpec::whole("x") };
        let resolved = resolve_spec(&spec, &project).unwrap();
        assert!(resolved.crossings[0].needed_by.is_none());
        refuse_unfed(&project, &resolved.selection, &spec).expect("nothing needs it");
        assert!(resolved.warnings.iter().any(|w| w.contains("b.in gets nothing in this run")), "{:?}", resolved.warnings);
    }

    /// One node of a hand-built program: `(id, scope, boundary, inputs,
    /// outputs, trigger)`, each input `(name, required)`.
    fn node(id: &str, scope: &[&str], boundary: Option<(&str, &str)>, inputs: &[(&str, bool)], outputs: &[&str], trigger: bool) -> Value {
        let mut n = json!({
            "id": id, "nodeType": if boundary.is_some() { "Passthrough" } else { "T" }, "label": null,
            "config": {}, "position": {"x": 0, "y": 0}, "scope": scope,
            "inputs": inputs.iter().map(|(name, required)| json!({"name": name, "portType": "String", "required": required})).collect::<Vec<_>>(),
            "outputs": outputs.iter().map(|name| json!({"name": name, "portType": "String", "required": true})).collect::<Vec<_>>(),
            "features": {"isTrigger": trigger}, "requiresInfra": false
        });
        if let Some((group, role)) = boundary { n["groupBoundary"] = json!({"groupId": group, "role": role}); }
        n
    }

    fn wire(from: &str, out: &str, to: &str, input: &str) -> Value {
        json!({"id": format!("{from}.{out}->{to}.{input}"), "source": from, "target": to, "sourceHandle": out, "targetHandle": input})
    }

    fn group(id: &str, parent: Option<&str>, children: &[&str]) -> Value {
        json!({"id": id, "kind": "group", "label": null, "inPorts": [], "outPorts": [],
               "parentGroupId": parent, "childGroupIds": children, "nodeIds": []})
    }

    /// A trigger never reads its wires in a run it did not fire (it
    /// closes its outputs), and a fired one reads its bake, so what feeds
    /// a trigger is never an input the run is missing.
    #[test]
    fn a_triggers_input_is_never_missing_from_a_run() {
        let project: ProjectDefinition = serde_json::from_value(json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "nodes": [
                node("bridge", &[], None, &[], &["url"], false),
                node("hear", &[], None, &[("url", true)], &["text"], true),
                node("after", &[], None, &[("text", false)], &[], false),
            ],
            "edges": [wire("bridge", "url", "hear", "url"), wire("hear", "text", "after", "text")],
        })).unwrap();
        let spec = RunSpec { target: vec!["after".into()], ..RunSpec::whole("x") };
        let resolved = resolve_spec(&spec, &project).unwrap();
        assert!(resolved.selection.nodes.contains(&Located::top("hear")), "the walk stops at the trigger, taking it along");
        refuse_unfed(&project, &resolved.selection, &spec).expect("the trigger does not read its input in this run");
    }

    /// `--group` on a group inside another: the run takes the outer
    /// door along (the inner group runs inside it), so the wire from
    /// outside reaches the outer door. What the person hands the inner
    /// group feeds it, and a refusal names the inner group's port, the
    /// door they started at, never the outer one.
    #[test]
    fn a_nested_group_run_is_fed_and_named_at_its_own_door() {
        let project: ProjectDefinition = serde_json::from_value(json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "nodes": [
                node("src", &[], None, &[], &["out"], false),
                node("o__in", &[], Some(("o", "In")), &[("x", false)], &["x"], false),
                node("o.i__in", &["o"], Some(("o.i", "In")), &[("x", false)], &["x"], false),
                node("o.i.need", &["o", "o.i"], None, &[("in", true)], &[], false),
                node("o.i__out", &["o"], Some(("o.i", "Out")), &[], &[], false),
                node("o__out", &[], Some(("o", "Out")), &[], &[], false),
            ],
            "edges": [
                wire("src", "out", "o__in", "x"),
                wire("o__in", "x", "o.i__in", "x"),
                wire("o.i__in", "x", "o.i.need", "in"),
            ],
            "groups": [group("o", None, &["o.i"]), group("o.i", Some("o"), &[])],
        })).unwrap();
        let spec = RunSpec { group: Some(("o.i".into(), BTreeMap::new())), ..RunSpec::whole("x") };
        let resolved = resolve_spec(&spec, &project).unwrap();
        let refusal = refuse_unfed(&project, &resolved.selection, &spec).unwrap_err().to_string();
        assert!(refusal.contains("o.i.x gets nothing in this run"), "named at the door started at: {refusal}");
        assert!(!refusal.contains("o.x "), "{refusal}");

        let spec = RunSpec { group: Some(("o.i".into(), BTreeMap::from([("x".into(), json!("hi"))]))), ..RunSpec::whole("x") };
        let resolved = resolve_spec(&spec, &project).unwrap();
        refuse_unfed(&project, &resolved.selection, &spec).expect("the value handed at the inner door feeds it");
    }

    /// A `@require_one_of` member is needed when every other member of
    /// its set gets nothing: the node would skip without it. Handing the
    /// other member a value makes the crossing merely optional again.
    #[test]
    fn the_last_member_of_a_one_of_set_is_needed() {
        let mut needs = node("n", &[], None, &[("p", false), ("q", false)], &[], false);
        needs["features"]["oneOfRequired"] = json!([["p", "q"]]);
        let project: ProjectDefinition = serde_json::from_value(json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "nodes": [node("src", &[], None, &[], &["out"], false), needs],
            "edges": [wire("src", "out", "n", "p")],
        })).unwrap();
        let spec = RunSpec { from: BTreeMap::from([("n".into(), BTreeMap::new())]), ..RunSpec::whole("x") };
        let resolved = resolve_spec(&spec, &project).unwrap();
        let refusal = refuse_unfed(&project, &resolved.selection, &spec).unwrap_err().to_string();
        assert!(refusal.contains("n.p gets nothing in this run") && refusal.contains("--from n="), "{refusal}");

        let spec = RunSpec { from: BTreeMap::from([("n".into(), BTreeMap::from([("q".into(), json!("hi"))]))]), ..RunSpec::whole("x") };
        let resolved = resolve_spec(&spec, &project).unwrap();
        refuse_unfed(&project, &resolved.selection, &spec).expect("q is handed, so p may close");
    }

    /// A refusal names the flag the value is handed with: `--group` for
    /// the group start. Two needers behind one port make one refusal.
    #[test]
    fn a_refusal_names_the_start_flag() {
        let project: ProjectDefinition = serde_json::from_value(json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "nodes": [
                node("src", &[], None, &[], &["out"], false),
                node("g__in", &[], Some(("g", "In")), &[("a", false)], &["a"], false),
                node("g.one", &["g"], None, &[("in", true)], &[], false),
                node("g.two", &["g"], None, &[("in", true)], &[], false),
                node("g__out", &[], Some(("g", "Out")), &[], &[], false),
            ],
            "edges": [
                wire("src", "out", "g__in", "a"),
                wire("g__in", "a", "g.one", "in"),
                wire("g__in", "a", "g.two", "in"),
            ],
            "groups": [group("g", None, &[])],
        })).unwrap();
        let spec = RunSpec { group: Some(("g".into(), BTreeMap::new())), ..RunSpec::whole("x") };
        let resolved = resolve_spec(&spec, &project).unwrap();
        let refusal = refuse_unfed(&project, &resolved.selection, &spec).unwrap_err();
        assert_eq!(refusal.errors.len(), 1, "{refusal}");
        assert!(refusal.errors[0].contains("--group g='{\"a\": ...}'"), "{refusal}");
    }

    /// `--group g` runs the group's members alone. A port of it wired
    /// from outside gets nothing unless it is handed a value, and it is
    /// refused when, inside, it reaches an input a node needs.
    #[test]
    fn a_group_run_refuses_a_port_a_node_inside_needs() {
        let project: ProjectDefinition = serde_json::from_value(json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "nodes": [
                {"id": "src", "nodeType": "T", "label": null, "config": {}, "position": {"x": 0, "y": 0},
                 "inputs": [], "outputs": [{"name": "out", "portType": "String", "required": true}],
                 "features": {}, "requiresInfra": false},
                {"id": "g__in", "nodeType": "Passthrough", "label": null, "config": {}, "position": {"x": 0, "y": 0},
                 "groupBoundary": {"groupId": "g", "role": "In"},
                 "inputs": [{"name": "a", "portType": "String", "required": false},
                            {"name": "b", "portType": "String", "required": false}],
                 "outputs": [{"name": "a", "portType": "String", "required": true},
                             {"name": "b", "portType": "String", "required": true}],
                 "features": {}, "requiresInfra": false},
                {"id": "g.need", "nodeType": "T", "label": null, "config": {}, "position": {"x": 0, "y": 0},
                 "scope": ["g"],
                 "inputs": [{"name": "in", "portType": "String", "required": true}], "outputs": [],
                 "features": {}, "requiresInfra": false},
                {"id": "g.maybe", "nodeType": "T", "label": null, "config": {}, "position": {"x": 0, "y": 0},
                 "scope": ["g"],
                 "inputs": [{"name": "in", "portType": "String", "required": false}], "outputs": [],
                 "features": {}, "requiresInfra": false},
                {"id": "g__out", "nodeType": "Passthrough", "label": null, "config": {}, "position": {"x": 0, "y": 0},
                 "groupBoundary": {"groupId": "g", "role": "Out"},
                 "inputs": [], "outputs": [], "features": {}, "requiresInfra": false}
            ],
            "edges": [
                {"id": "src->g.a", "source": "src", "target": "g__in", "sourceHandle": "out", "targetHandle": "a"},
                {"id": "src->g.b", "source": "src", "target": "g__in", "sourceHandle": "out", "targetHandle": "b"},
                {"id": "g.a->need", "source": "g__in", "target": "g.need", "sourceHandle": "a", "targetHandle": "in"},
                {"id": "g.b->maybe", "source": "g__in", "target": "g.maybe", "sourceHandle": "b", "targetHandle": "in"}
            ],
            "groups": [{"id": "g", "kind": "group", "label": null, "inPorts": [], "outPorts": [],
                        "parentGroupId": null, "childGroupIds": [], "nodeIds": ["g.need", "g.maybe"]}]
        })).unwrap();
        let spec = RunSpec { group: Some(("g".into(), BTreeMap::new())), ..RunSpec::whole("x") };
        let resolved = resolve_spec(&spec, &project).unwrap();
        let refusal = refuse_unfed(&project, &resolved.selection, &spec).unwrap_err();
        assert_eq!(refusal.errors.len(), 1, "only the port a required input needs: {refusal}");
        assert!(refusal.errors[0].contains(".a gets nothing") && refusal.errors[0].contains("g.need.in"), "{refusal}");

        let spec = RunSpec { group: Some(("g".into(), BTreeMap::from([("a".into(), json!("hi"))]))), ..RunSpec::whole("x") };
        let resolved = resolve_spec(&spec, &project).unwrap();
        refuse_unfed(&project, &resolved.selection, &spec).expect("the handed value feeds it");
    }

    #[test]
    fn simulation_excludes_body_and_allows_unused_declared_output() {
        let spec = RunSpec { emit: BTreeMap::from([("c".into(), BTreeMap::from([("out".into(), json!("x"))]))]),
            ..RunSpec::whole("x") };
        let resolved = resolve_spec(&spec, &program()).unwrap();
        assert!(resolved.selection.nodes.is_empty());
        assert_eq!(resolved.provided.len(), 1);
    }

    #[test]
    fn disconnected_or_empty_cuts_are_refused_before_execution() {
        let spec = RunSpec { from: [("c".into(), BTreeMap::new())].into(), before: vec!["b".into()], ..RunSpec::whole("empty") };
        assert!(resolve_spec(&spec, &program()).unwrap_err().to_string().contains("selection is empty"));
        let spec = RunSpec { emit: [("c".into(), [("out".into(), json!("x"))].into())].into(), before: vec!["b".into()], ..RunSpec::whole("empty") };
        assert!(resolve_spec(&spec, &program()).unwrap_err().to_string().contains("selection is empty"));
    }

    #[test]
    fn flat_wire_shape_retains_frozen_evidence_and_rejects_old_fields() {
        let spec = RunSpec { from: BTreeMap::from([("b".into(), BTreeMap::new())]), before: vec!["c".into()],
            caller: vec![json!({"text": "hello"})],
            answers: vec![Answer { node: "b".into(), frames: vec![], payload: json!("yes"), question: Some(json!("Continue?")) }],
            ..RunSpec::whole("x") };
        assert_eq!(serde_json::from_value::<RunSpec>(serde_json::to_value(&spec).unwrap()).unwrap(), spec);
        for field in ["scope", "kicks", "provided", "emitted", "input", "bogus"] {
            assert!(serde_json::from_value::<RunSpec>(json!({"name": "x", field: {}})).is_err());
        }
    }

    #[test]
    fn finite_stream_is_a_list_of_items_not_a_runtime_handle() {
        let string = WeftType::Primitive(crate::weft_type::WeftPrimitive::String);
        let ty = WeftType::Generator(Box::new(string.clone()));
        assert!(validate_supplied_value(&ty, &json!([])).is_ok());
        assert!(validate_supplied_value(&ty, &json!(["a", "b"])).is_ok());
        assert!(validate_supplied_value(&ty, &json!(["a", 2])).is_err());
        assert!(validate_supplied_value(&ty, &json!("a")).is_err());
        let nested = WeftType::Generator(Box::new(WeftType::List(Box::new(string))));
        assert!(validate_supplied_value(&nested, &json!([["a"], ["b"]])).is_ok());
    }

    #[test]
    fn duplicate_nodes_and_ports_are_refused_before_values_are_lost() {
        for text in [
            r#"{"name":"x","from":{"a":{"in":"x","in":"y"}}}"#,
            r#"{"name":"x","from":{"a":{},"a":{}}}"#,
            r#"{"name":"x","emit":{"a":{"out":"x","out":"y"}}}"#,
            r#"{"name":"x","emit":{"a":{},"a":{}}}"#,
            r#"{"name":"x","from":{"a":{"in":{"nested":[{"key":1,"key":2}]}}}}"#,
            r#"{"name":"x","emit":{"a":{"out":[{"key":1,"key":2}]}}}"#,
            r#"{"name":"x","group":["batch",{"items":1,"items":2}]}"#,
            r#"{"name":"x","group":["batch",{"items":[{"key":1,"key":2}]}]}"#,
            r#"{"name":"x","fire":["tick",{"key":1,"key":2}]}"#,
            r#"{"name":"x","caller":[{"key":1,"key":2}]}"#,
            r#"{"name":"x","answers":[{"value":{"key":1,"key":2}}]}"#,
            r#"{"name":"x","expected":{"output":{"key":1,"key":2}}}"#,
        ] {
            assert!(serde_json::from_str::<RunSpec>(text).unwrap_err().to_string().contains("duplicate key"));
        }
        assert!(parse_port_object(r#"{"x":1,"x":2}"#).is_err());
        assert!(parse_port_object(r#"{"x":[{"nested":1,"nested":2}]}"#).is_err());
        let value = json!({"x": [null, true, false, -3, 18446744073709551615_u64, 1.5, "hello", {"nested": []}]});
        assert_eq!(serde_json::to_value(parse_port_object(&value.to_string()).unwrap()).unwrap(), value);
    }

    #[test]
    fn every_carving_endpoint_must_exist() {
        for spec in [
            RunSpec { from: BTreeMap::from([("missing".into(), BTreeMap::new())]), ..RunSpec::whole("x") },
            RunSpec { target: vec!["missing".into()], ..RunSpec::whole("x") },
            RunSpec { before: vec!["missing".into()], ..RunSpec::whole("x") },
            RunSpec { group: Some(("missing".into(), BTreeMap::new())), ..RunSpec::whole("x") },
            RunSpec { fire: Some(("missing".into(), Value::Null)), ..RunSpec::whole("x") },
            RunSpec { emit: BTreeMap::from([("missing".into(), BTreeMap::new())]), ..RunSpec::whole("x") },
        ] {
            assert!(resolve_spec(&spec, &program()).unwrap_err().to_string().contains("missing"));
        }
    }

    #[test]
    fn removed_input_ports_warn_without_dropping_the_start_or_widening_the_run() {
        let spec = RunSpec { from: BTreeMap::from([("b".into(), BTreeMap::from([
            ("missing".into(), json!("old value")), ("in".into(), json!("backup"))
        ]))]), ..RunSpec::whole("x") };
        let resolved = resolve_spec(&spec, &program()).unwrap();
        assert_eq!(resolved.selection.nodes, BTreeSet::from([Located::top("b"), Located::top("c")]));
        assert_eq!(resolved.selection.input[&Located::top("b")], BTreeMap::from([("in".into(), json!("backup"))]));
        assert!(resolved.warnings.iter().any(|warning| warning.contains("ignored supplied input 'b.missing'")));
        assert!(spec.from["b"].contains_key("missing"), "resolution does not rewrite the saved example");
    }

    #[test]
    fn emitted_node_cannot_also_be_an_explicit_start() {
        let spec = RunSpec { from: BTreeMap::from([("b".into(), BTreeMap::new())]),
            emit: BTreeMap::from([("b".into(), BTreeMap::from([("out".into(), json!("x"))]))]),
            ..RunSpec::whole("x") };
        assert!(resolve_spec(&spec, &program()).unwrap_err().to_string().contains("cannot both run"));
    }

    #[test]
    fn trigger_backups_are_refused_even_when_not_fired() {
        let mut project = program();
        project.nodes[0].features.is_trigger = true;
        let spec = RunSpec { from: BTreeMap::from([("a".into(), BTreeMap::from([("in".into(), json!("x"))]))]),
            ..RunSpec::whole("x") };
        assert!(resolve_spec(&spec, &project).unwrap_err().to_string().contains("cannot be a from start"));
    }

    #[test]
    fn fire_requires_trigger_and_cannot_be_excluded_by_end_bound() {
        let mut project = program();
        let spec = RunSpec { fire: Some(("a".into(), json!({}))), ..RunSpec::whole("x") };
        assert!(resolve_spec(&spec, &project).unwrap_err().to_string().contains("not a trigger"));
        project.nodes[0].features.is_trigger = true;
        let spec = RunSpec { before: vec!["a".into()], ..spec };
        assert!(resolve_spec(&spec, &project).unwrap_err().to_string().contains("outside"));
    }

    #[test]
    fn a_fire_names_the_wire_it_drops_even_when_its_producer_left_the_run() {
        // a -> b -> c with b the trigger: a fire on b carves a out
        // entirely (its only consumer was the trigger's wire), and the
        // warning still names a.out, because the user who wired it is
        // the one who has to learn the bake wins.
        let mut project = program();
        project.nodes[1].features.is_trigger = true;
        let spec = RunSpec { fire: Some(("b".into(), json!({}))), ..RunSpec::whole("x") };
        let resolved = resolve_spec(&spec, &project).unwrap();
        assert!(!resolved.selection.nodes.contains(&Located::top("a")), "{:?}", resolved.selection.nodes);
        assert!(resolved.warnings.iter().any(|w| w.contains("a.out not delivered to trigger 'b'") && w.contains("weft bake")),
            "{:?}", resolved.warnings);
    }

    #[test]
    fn a_run_started_by_hand_says_which_trigger_stays_quiet_and_who_skips_for_it() {
        let mut project = program();
        project.nodes[1].features.is_trigger = true;
        let resolved = resolve_spec(&RunSpec::whole("x"), &project).unwrap();
        let warning = resolved.warnings.iter().find(|w| w.starts_with("trigger 'b' does not fire")).expect("a warning for the quiet trigger");
        assert!(warning.contains("fires no trigger") && warning.contains("c skip") && warning.contains("--fire b="), "{warning}");
        // With another trigger fired, the quiet one is explained by that fire.
        project.nodes[0].features.is_trigger = true;
        let spec = RunSpec { fire: Some(("a".into(), json!({}))), ..RunSpec::whole("x") };
        let resolved = resolve_spec(&spec, &project).unwrap();
        assert!(resolved.warnings.iter().any(|w| w.starts_with("trigger 'b' does not fire: only 'a' fires")), "{:?}", resolved.warnings);
    }

    #[test]
    fn inclusive_and_exclusive_cuts_preserve_the_authored_path() {
        let spec = RunSpec { from: BTreeMap::from([("b".into(), BTreeMap::new())]), target: vec!["c".into()], ..RunSpec::whole("x") };
        assert_eq!(resolve_spec(&spec, &program()).unwrap().selection.nodes, BTreeSet::from([Located::top("b"), Located::top("c")]));
        let spec = RunSpec { before: vec!["c".into()], ..spec };
        assert_eq!(resolve_spec(&spec, &program()).unwrap().selection.nodes, BTreeSet::from([Located::top("b")]));
        assert!(resolve_spec(&spec, &program()).unwrap().warnings.iter().all(|w| !w.contains("continues to the end")));
    }
}
