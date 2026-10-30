//! A run is bounded by starts and ends, hands values to its starts, and can
//! stand in for nodes' outputs: it runs the cut of the program its bounds walk
//! (`crate::project::cut`). Resolution is pure. Trigger preparation and seed
//! lookup are caller-owned I/O.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::marker::PhantomData;

use serde::de::{Error, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::frames::{Located, LoopFrames};
use crate::stored::Op;
use crate::primitive::AwaitEnd;
use crate::project::cut::{self, SelectionBounds};
use crate::project::graph::{GraphView, ProjectGraph};
use crate::project::{Given, GivenOrigin, GivenPort, ProjectDefinition, StandIn};
use crate::pulse::Failure;
use crate::weft_type::WeftType;
use crate::RunId;

pub mod judge;

/// The chosen origin of each reused result, independent of which nodes
/// execute in the child. Readers reconstruct each origin under its own
/// birth context and import history without replaying its scheduling.
// SYNC: Seed <-> packages/weft-graph/src/protocol.ts Seed
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Seed {
    pub parent: RunId,
    /// Per place (a node under the calls that reach it): the run whose
    /// result it keeps.
    pub origins: BTreeMap<Located, RunId>,
    /// The places this run runs again that replay the answers `parent`
    /// gave them (`crate::primitive::answered_steps`): their code is
    /// what it was there, so they carry on where they stopped instead of
    /// asking again.
    #[serde(default, skip_serializing_if = "std::collections::BTreeSet::is_empty")]
    pub answers: std::collections::BTreeSet<Located>,
}

pub type PortValues = BTreeMap<String, BTreeMap<String, Value>>;
/// A group's simulated outputs: the group id and one value per output port.
pub type GroupOutputs = (String, BTreeMap<String, Value>);

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BakeSummary {
    pub program: crate::project::hash::ProgramIdentity,
    pub captured: Vec<String>,
    pub run_id: RunId,
    pub at_unix: u64,
}

/// Both preview and run require a capture from the exact requested code.
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
        "the code changed since trigger '{node}' was last prepared, so its bake is stale{older}. Prepare it again: `weft bake` does it and changes nothing that listens; if the trigger is off, `weft activate` does it and turns it on; if it is on, `weft resync` brings it onto the new build and prepares it there"
    )))
}

/// The example file, run request, and editor value share one contract.
// SYNC: RunSpec <-> packages/weft-graph/src/run-spec.ts RunSpec
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
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
    /// Which instance the run is for: one separate running copy of part of
    /// the program. Required when the run reaches a node that exists once
    /// per instance ([`refuse_instanceless`]), unused otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance: Option<crate::instance::InstanceId>,
    /// How the run asks to be kept (`weft run --durable --fast --recorded
    /// --unrecorded`): what it names goes over the fired trigger's own
    /// settings, or over the defaults for a run that fires nothing
    /// (`crate::run_settings::SettingsChoice`).
    #[serde(default, skip_serializing_if = "crate::run_settings::SettingsChoice::is_empty")]
    pub settings: crate::run_settings::SettingsChoice,
}

/// The steps a stored [`RunSpec`] takes from the shape an older weft
/// wrote, wherever it is kept: a saved spec file (`examples/<name>.json`)
/// and a run's row (`run.spec`); `crate::stored` says how they run. A file
/// says its version under [`SPEC_FILE_VERSION_KEY`], and one without it is
/// version 1: every file written before files carried a version, in either
/// spelling.
pub const RUN_SPEC_UPGRADES: &[&[Op]] = &[&[
    // Version 2: the keys a spec once spelled snake_case.
    Op::RenameKey { at: "", from: "frozen_from", to: "frozenFrom" },
    Op::RenameKey { at: "frozenFrom", from: "execution_id", to: "runId" },
    Op::RenameKey { at: "frozenFrom", from: "definition_hash", to: "definitionHash" },
    Op::RenameKey { at: "answers.*", from: "gave_up", to: "gaveUp" },
    Op::RenameKey { at: "settings", from: "keep_for", to: "keepFor" },
    Op::RenameKey { at: "settings", from: "hold_secs", to: "holdSecs" },
]];

/// The key a saved spec file holds its version under ([`RUN_SPEC_UPGRADES`]).
pub const SPEC_FILE_VERSION_KEY: &str = "specVersion";

impl RunSpec {
    /// A run spec as weft stores it: a saved spec file and a run's row.
    pub const STORED: crate::stored::Kind<RunSpec> = crate::stored::Kind::new("run spec", RUN_SPEC_UPGRADES);

    /// A saved spec file's text as a spec (the `name` is the caller's to
    /// set from the file name), carried to today's shape in memory when an
    /// older weft wrote it. Reading never rewrites the file: it takes
    /// today's shape the next time weft writes it anyway ([`Self::file_text`]).
    /// A file written by a newer weft is refused, naming both versions.
    pub fn parse_file(text: &str) -> Result<RunSpec, String> {
        // Through `UniqueValue`, so a key written twice is still refused.
        let UniqueValue(mut value) = serde_json::from_str(text).map_err(|e| e.to_string())?;
        let version = match value.as_object_mut().and_then(|file| file.remove(SPEC_FILE_VERSION_KEY)) {
            None => crate::stored::unversioned(),
            Some(v) => v
                .as_i64()
                .and_then(|n| crate::stored::Version::try_from(n).ok())
                .filter(|n| *n >= 1)
                .ok_or_else(|| format!("`{SPEC_FILE_VERSION_KEY}` is a whole number from 1 up, not {v}"))?,
        };
        Self::STORED.decode(value, version).map_err(|e| e.to_string())
    }

    /// The text a saved spec file holds for this spec: the spec, with the
    /// version its shape is at, then a newline.
    pub fn file_text(&self) -> Result<String, String> {
        #[derive(Serialize)]
        struct File<'a> {
            #[serde(rename = "specVersion")]
            version: crate::stored::Version,
            #[serde(flatten)]
            spec: &'a RunSpec,
        }
        let mut text = serde_json::to_string_pretty(&File { version: Self::STORED.version(), spec: self })
            .map_err(|e| e.to_string())?;
        text.push('\n');
        Ok(text)
    }
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

/// How one wait of a run ended, as recorded at freeze time: the
/// person's answer, their skip, or the run giving the wait up.
/// `question` is what the node showed the person, so whoever answers on
/// a new run reads old question, old ending, new question. The runtime
/// never replays answers; Tangle plays the person. The file spells a
/// given answer `"payload": <value>`, a skip `"skipped": true`, and a
/// wait given up `"gaveUp": "<the error>"`, exactly one of the three
/// ([`AnswerFile`]).
// SYNC: Answer <-> packages/weft-graph/src/run-spec.ts Answer
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "AnswerFile", into = "AnswerFile")]
pub struct Answer {
    pub node: String,
    pub frames: LoopFrames,
    /// How the wait ended: end it the same way on a new run.
    pub ended: AwaitEnd,
    pub question: Option<Value>,
}

/// [`Answer`] as the file holds it, both ways. `payload` is the field
/// every frozen example has had since the first; `skipped` came with
/// skips and `gaveUp` with waits given up, and exactly one of the
/// three is present. `deny_unknown_fields` and the defaults live here,
/// because with `try_from` serde never reads attributes on [`Answer`]
/// itself.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AnswerFile {
    node: String,
    #[serde(default)]
    frames: LoopFrames,
    /// `Some(Null)` is a given null; `None` is no payload at all.
    #[serde(default, deserialize_with = "present", skip_serializing_if = "Option::is_none")]
    payload: Option<Value>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    skipped: bool,
    /// The error the waiting node failed with when the run gave the wait up.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    gave_up: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    question: Option<Value>,
}

/// A field that is there, `null` included.
fn present<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<Option<Value>, D::Error> {
    Value::deserialize(deserializer).map(Some)
}

impl TryFrom<AnswerFile> for Answer {
    type Error = String;

    fn try_from(f: AnswerFile) -> Result<Self, String> {
        let ended = match (f.payload, f.skipped, f.gave_up) {
            (Some(value), false, None) => AwaitEnd::Answered { value },
            (None, true, None) => AwaitEnd::Skipped,
            (None, false, Some(error)) => AwaitEnd::GaveUp { error },
            (None, false, None) => return Err(format!(
                "the answer to '{}' says nothing about how its wait ended; give the value the person answered (\"payload\"), \"skipped\": true if they skipped it, or \"gaveUp\" with the error if the run gave it up",
                f.node
            )),
            (payload, skipped, gave_up) => {
                let said: Vec<&str> = [(payload.is_some(), "a payload"), (skipped, "\"skipped\": true"), (gave_up.is_some(), "\"gaveUp\"")]
                    .into_iter().filter(|(present, _)| *present).map(|(_, name)| name).collect();
                return Err(format!(
                    "the answer to '{}' says its wait ended more than one way ({}); a wait ends once, so keep only one",
                    f.node, said.join(" and ")
                ));
            }
        };
        Ok(Self { node: f.node, frames: f.frames, ended, question: f.question })
    }
}

impl From<Answer> for AnswerFile {
    fn from(a: Answer) -> Self {
        let (payload, skipped, gave_up) = match a.ended {
            AwaitEnd::Answered { value } => (Some(value), false, None),
            AwaitEnd::Skipped => (None, true, None),
            AwaitEnd::GaveUp { error } => (None, false, Some(error)),
        };
        Self { node: a.node, frames: a.frames, payload, skipped, gave_up, question: a.question }
    }
}

/// Where a frozen example's `expected` came from.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FrozenFrom {
    pub version: String,
    pub run_id: RunId,
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
    /// A closure left by a node that broke: which node and why.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<Failure>,
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
    /// A closure left by a node that broke: which node and why.
    pub failure: Option<Failure>,
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
    pub fn spell(project: &impl crate::project::graph::GraphView, wire: &OutputWire) -> Self {
        let place = wire.place();
        Self {
            node: crate::project::address_of(project, &place.id, &place.path),
            port: wire.port.clone(),
            frames: wire.frames.iter().filter(|frame| frame.loop_index().is_some()).cloned().collect(),
            ordinal: wire.ordinal,
            closed: wire.closed,
            failure: wire.failure.clone(),
            value: wire.value.clone(),
        }
    }
}

impl RunSpec {
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

    /// Whether the spec bounds the run at all. A spec with no bound runs
    /// the program as written.
    pub fn is_bounded(&self) -> bool {
        !self.from.is_empty() || !self.emit.is_empty() || !self.target.is_empty() || !self.before.is_empty()
            || !self.feed.is_empty() || self.group.is_some() || self.fire.is_some()
    }

    /// The bounds the spec puts on the run, spelled as written.
    pub fn bounds(&self) -> SelectionBounds {
        SelectionBounds {
            from: self.from.keys().cloned().collect(),
            emit: self.emit.keys().cloned().collect(),
            target: self.target.clone(),
            before: self.before.clone(),
            group: self.group.as_ref().map(|(id, _)| id.clone()),
            fire: self.fire.as_ref().map(|(node, _)| node.clone()),
            feed: self.feed.clone(),
            handed: self.from.iter().chain(self.group.iter().map(|(group, ports)| (group, ports)))
                .map(|(start, ports)| (start.clone(), ports.keys().cloned().collect()))
                .collect(),
            baked: BTreeSet::new(),
        }
    }

    /// The values the spec hands its starts, by the start's place (a node,
    /// or a group whose inputs and gate they are): `from` and `group`
    /// together. A start that names nothing is refused
    /// (`crate::project::places::named`).
    pub fn handed(&self, project: &ProjectDefinition) -> Result<BTreeMap<Located, BTreeMap<String, Value>>, String> {
        self.from.iter().chain(self.group.iter().map(|(id, ports)| (id, ports)))
            .map(|(spelled, ports)| Ok((crate::project::places::named(project, spelled)?.into_place(), ports.clone())))
            .collect()
    }
}

/// One root a run kicks at its start.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KickPlan {
    pub node: String,
    /// The frames the kick fires under: one call frame per site on the
    /// node's call path, for a trigger inside an included file reached
    /// through a site; empty at the top.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub frames: crate::frames::LoopFrames,
    /// The trigger fired: its payload is the event.
    pub firing: bool,
    pub payload: Option<Value>,
    /// What this trigger registered at activation, for a firing kick:
    /// a fired trigger REPLAYS its setup-time ports instead of reading
    /// them off the wire. Whoever holds the capture sets it: a live fire
    /// passes its armed entry's to [`compute_trigger_fire`]; a fire started
    /// by hand gets its bake's from the dispatcher, once, after the run is
    /// resolved (seeded or not), since resolving reads no bake.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port_snapshot: Option<Value>,
}

impl KickPlan {
    /// What a run of `program` kicks at its start: the nodes outside every
    /// scope that no wire feeds (each scope kicks its own when it opens,
    /// `crate::run::scope`), and every trigger no wire feeds at every place
    /// it is at, the one `fire` names with its event and snapshot (every
    /// other one is kicked payload-less, and closes its outputs; a cut
    /// keeps a wire into a trigger only for its setup, where the trigger
    /// fires on what reaches it). A node that stands in for itself is
    /// never kicked: it sends on its own.
    pub fn for_program(program: &ProjectDefinition, fire: Option<(&Located, &Value)>, port_snapshot: Option<&Value>) -> Vec<Self> {
        let index = crate::project::ProgramIndex::build(program);
        let mut roots: BTreeSet<Located> = crate::run::scope::starting_members(program, &index, None, &Vec::new())
            .into_iter().map(Located::top).collect();
        roots.extend(crate::project::trigger_places(program).into_iter()
            .filter(|place| index.admits(&place.id, &place.frames()) && !index.stands_in(&place.id, &place.frames()))
            .filter(|place| index.get_incoming(program, &place.id, &place.frames()).is_empty()));
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

/// What one trigger fire runs: its cut of the program (every fire of one
/// trigger cuts the same, from the trigger down to everything it reaches
/// and back up for what that needs, other triggers firing with nothing),
/// the roots that cut kicks, and the infra it reads baked.
///
/// Why a fire is a cut: emission is scope-blind, so a node shared by two
/// programs in one file (a database, a provider) pushes a pulse into the
/// OTHER program's consumers too. In the whole program those consumers
/// hold a partial input set forever and the run ends Stuck after all its
/// real work completed. In the cut, they are not there.
#[derive(Debug)]
pub struct TriggerFire {
    pub kicks: Vec<KickPlan>,
    pub program: ProjectDefinition,
    /// What the fire is given at the places `program` says: the baked
    /// infra's saved values, standing in.
    pub given: Given,
    /// The infra places the fire reads only baked outputs of, with their
    /// saved values (`crate::infra::bake::covered`): they stand in for
    /// themselves in `program`.
    pub baked: crate::infra::bake::Saved,
}

/// The cut a fire of `firing_node_id` (an address: `door`, or `one.door`
/// for the `door` inside the file the site `one` includes) runs.
///
/// Why the walk stops at every other trigger: at fire time a trigger's
/// outputs are its payload, not a function of its inputs (its ports
/// replay the setup-time snapshot), so nodes that exist only to produce
/// inputs for triggers must not re-run every time a trigger fires. A node
/// that also feeds what the fire reaches re-runs for that.
///
/// `saved` is what the infra places the run may read have saved
/// (`crate::infra::bake::saved_for_run`): one the fire reads only baked
/// outputs of does not run.
pub fn compute_trigger_fire(
    project: &ProjectDefinition,
    firing_node_id: &str,
    payload: &Value,
    port_snapshot: Option<&Value>,
    saved: &crate::infra::bake::Saved,
) -> Result<TriggerFire, String> {
    let cut = fire_cut(project, firing_node_id, saved)?;
    let kicks = KickPlan::for_program(&cut.program, Some((&cut.fired, payload)), port_snapshot);
    Ok(TriggerFire { kicks, program: cut.program, given: cut.given, baked: cut.baked })
}

/// The cut a fire of the trigger at `trigger` runs, `saved` standing in for
/// the infra outputs it reads baked: the one cut function every reader of
/// a trigger's cut goes through ([`compute_trigger_fire`], what a fire
/// runs; [`trigger_cut`], what its registration records), so the two
/// never disagree. Refused when the cut needs a part of the program that
/// does not compile (`cut::cut`).
struct FireCut {
    fired: Located,
    program: ProjectDefinition,
    given: Given,
    baked: BTreeMap<Located, BTreeMap<String, Value>>,
}

fn fire_cut(project: &ProjectDefinition, trigger: &str, saved: &crate::infra::bake::Saved) -> Result<FireCut, String> {
    let g = ProjectGraph::new(project);
    let bounds = cut::resolve(&g, &SelectionBounds { fire: Some(trigger.into()), ..Default::default() })?;
    let fired = bounds.fired.clone().ok_or_else(|| format!("'{trigger}' is not a trigger"))?;
    let (walked, baked) = walk_reading_baked(&g, bounds, saved, &BTreeSet::from([fired.clone()]));
    let mut given = Given { inputs: BTreeMap::new(), outputs: baked_stand_ins(&baked) };
    let program = cut::cut(project, &walked, &mut given).map_err(|why| format!("a fire of '{trigger}' cannot run: {why}"))?;
    Ok(FireCut { fired, program, given, baked })
}

/// A trigger's cut as its registration records it: what tells whether it
/// is behind the source, and what infra it depends on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TriggerCut {
    /// The hash over the two programs the trigger runs: the cut a fire
    /// runs, every infra place it reads only through baked outputs
    /// standing in (the way a fire runs it once they are saved), and the
    /// cut its setup runs, everything in front of it (which no fire
    /// runs: the walk stops at the fired trigger). A function of the
    /// source alone. A trigger that is on whose fingerprint differs from
    /// the source's is behind it, so an edit to what feeds it reads as
    /// behind too, and a resync, which sets it up again, runs that edit.
    pub fingerprint: String,
    /// The infra places (spelled the way the program writes them) the
    /// trigger depends on, sorted: every infra place its fires run, and
    /// every one its registration listens through (the address its setup
    /// reads off the infra wired into it). Taking one of them down takes
    /// the trigger down with it. An infra place its fires read only
    /// through baked outputs is not among them (a fire does not run it),
    /// and neither is one that only fed a gate its setup read.
    pub depends_on: Vec<String>,
}

/// The cut a registration records for the trigger at `trigger` (an
/// address: `door`, or `one.door`): the same walk [`compute_trigger_fire`]
/// runs, with every bakeable infra place's baked outputs taken as saved,
/// so it reads the source alone, never what an install has saved.
pub fn trigger_cut(project: &ProjectDefinition, trigger: &str) -> Result<TriggerCut, String> {
    let bakeable: crate::infra::bake::Saved = crate::project::infra_places(project)
        .into_iter()
        .filter_map(|place| {
            let node = project.nodes.iter().find(|n| n.id == place.id)?;
            (!node.baked_outputs.is_empty())
                .then(|| (place, node.baked_outputs.iter().map(|port| (port.clone(), Value::Null)).collect()))
        })
        .collect();
    // The places read baked stand in, as a fire's do: the cut says so, and
    // they are no infra the trigger's fires run.
    let FireCut { fired, program, .. } = fire_cut(project, trigger, &bakeable)?;
    let (setup, _) = setup_cut(project, std::slice::from_ref(&fired))?;
    let hash = |cut: &ProjectDefinition| {
        crate::project::hash::compute_definition_hash(cut).map_err(|e| format!("hash the cut of '{trigger}': {e:#}"))
    };
    let fingerprint = crate::project::hash::sha256_hex(format!("fire {}\nsetup {}", hash(&program)?, hash(&setup)?).as_bytes());
    let g = ProjectGraph::new(project);
    // What its registration holds: the address it reads off the infra
    // feeding its inputs, along the wires. A wire into a gate only decides
    // whether a node of the setup runs, read once: stopping what fed it
    // later breaks nothing, so the walk never crosses one.
    let is_infra = |place: &Located| project.nodes.iter().any(|n| n.id == place.id && n.requires_infra);
    let mut fed_by: BTreeSet<Located> = BTreeSet::new();
    let mut pending = vec![fired];
    while let Some(place) = pending.pop() {
        if !fed_by.insert(place.clone()) {
            continue;
        }
        pending.extend(
            crate::project::places::incoming(&g, &place)
                .filter(|(edge, _)| edge.target_handle.as_deref() != Some(crate::run::skip::SHOULD_FLOW_PORT))
                .map(|(_, source)| source),
        );
    }
    let listens_through = fed_by.into_iter().filter(|place| is_infra(place)).map(|place| crate::project::address_of(project, &place.id, &place.path));
    let depends_on: BTreeSet<String> = crate::project::infra_reached(&program).into_iter().chain(listens_through).collect();
    Ok(TriggerCut { fingerprint, depends_on: depends_on.into_iter().collect() })
}

/// Every trigger place's [`TriggerCut`] by address, or why its cut cannot
/// be walked (a trigger inside a half-selected loop): what `weft status`
/// sends as the source's cuts (`StatusQuery::desired_trigger_cuts`) and
/// the dispatcher compares the registered program's with.
pub fn trigger_cuts(project: &ProjectDefinition) -> BTreeMap<String, Result<TriggerCut, String>> {
    crate::project::trigger_places(project)
        .iter()
        .map(|place| {
            let address = crate::project::address_of(project, &place.id, &place.path);
            let cut = trigger_cut(project, &address);
            (address, cut)
        })
        .collect()
}

/// Each trigger's fingerprint by address ([`trigger_cuts`]), the ones whose
/// cut can be walked: what the source's cuts are compared by, on the
/// client that sends them (`StatusQuery::desired_trigger_cuts`) and on the
/// dispatcher that reads the registered program's. A trigger left out is
/// one whose cut cannot be walked; its activation says why.
pub fn trigger_fingerprints(project: &ProjectDefinition) -> BTreeMap<String, String> {
    trigger_cuts(project)
        .into_iter()
        .filter_map(|(trigger, cut)| cut.ok().map(|cut| (trigger, cut.fingerprint)))
        .collect()
}

/// What a setup phase of `targets` runs (an activation's triggers, an
/// infra start's nodes): its cut of the program, everything the targets
/// need and nothing else, and the roots that cut kicks, payload-less.
/// Refuses targets inside a loop (a loop runs whole).
pub fn setup_cut(project: &ProjectDefinition, targets: &[Located]) -> Result<(ProjectDefinition, Vec<KickPlan>), String> {
    // With no target the walk would keep the whole program.
    if targets.is_empty() {
        return Err("a setup phase was asked for with nothing to set up".into());
    }
    let walked = cut::walk_setup(&ProjectGraph::new(project), targets)?;
    let program = cut::cut(project, &walked, &mut Given::default()).map_err(|why| {
        let spelled: Vec<String> = targets.iter().map(|place| crate::project::address_of(project, &place.id, &place.path)).collect();
        format!("setting up {} cannot run: {why}", spelled.join(", "))
    })?;
    let kicks = KickPlan::for_program(&program, None, None);
    Ok((program, kicks))
}

/// Walk `bounds`, then once more with the infra places the walk reaches
/// and reads only baked, saved outputs of (`crate::infra::bake::covered`;
/// a place whose id is in `named` always runs) standing in for
/// themselves: what only fed them leaves the run. Answers the walk and
/// the places it reads baked.
fn walk_reading_baked(
    g: &ProjectGraph,
    mut bounds: cut::Bounds,
    saved: &crate::infra::bake::Saved,
    named: &BTreeSet<Located>,
) -> (cut::Walk, crate::infra::bake::Saved) {
    let first = cut::walk_bounds(g, &bounds);
    let covered = crate::infra::bake::covered(g, saved, &first.kept(), named);
    if covered.is_empty() {
        return (first, covered);
    }
    bounds.given.extend(covered.keys().cloned());
    let walked = cut::walk_bounds(g, &bounds);
    let baked = covered.into_iter().filter(|(place, _)| walked.stand_ins.contains(place)).collect();
    (walked, baked)
}

/// The stand-ins baked infra places are: each sends its saved values.
fn baked_stand_ins(baked: &crate::infra::bake::Saved) -> BTreeMap<Located, StandIn> {
    baked.iter().map(|(place, values)| {
        let ports = values.iter().map(|(port, value)| (port.clone(), GivenPort::Value { value: value.clone() })).collect();
        (place.clone(), StandIn { ports, origin: GivenOrigin::Baked })
    }).collect()
}

/// What a resolved spec runs and what the caller should explain.
#[derive(Debug, Clone)]
pub struct Resolved {
    /// The program the run runs: the spec's cut of the program (for a spec
    /// with no bound, every place of it, less the wires nothing carries).
    pub program: ProjectDefinition,
    /// What the run is given at the places `program` says
    /// ([`crate::project::Given`]): its birth carries it.
    pub given: Given,
    /// What the walk kept (every place, for the program as written).
    pub walk: cut::Walk,
    /// Every place `program` runs (`crate::project::places::running_places`).
    pub runs: BTreeSet<Located>,
    pub kicks: Vec<KickPlan>,
    pub crossings: Vec<CrossingPort>,
    pub warnings: Vec<String>,
    /// The infra places the run reads only baked outputs of, with their
    /// saved values (`crate::infra::bake::covered`): they stand in for
    /// themselves in `program`.
    pub baked: crate::infra::bake::Saved,
}

/// A selected input whose wire's source carries nothing in this run: it
/// does not run, or it is a trigger the run does not fire.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CrossingPort {
    /// The receiving node, spelled the way the program reads it
    /// (`one.strip` for a node of an included file under the site
    /// `one`), the same key a `from` entry uses.
    pub node: String,
    pub port: String,
    pub source_node: String,
    pub source_port: String,
    /// The source is a trigger the run does not fire: it runs, and closes
    /// its outputs.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub unfired_trigger: bool,
    /// Whether something in the run needs this input: `needed_by` is set.
    pub required: bool,
    /// The required input this one ends up feeding, spelled like `node`:
    /// the node's own input, or one inside the loop it enters.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub needed_by: Option<String>,
    /// Where to hand the missing value: the start the value would pass
    /// (a node, or the group whose input it is), spelled like `node`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hand_at: Option<StartPort>,
    pub supplied: bool,
}

/// One input port of a run's start.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StartPort {
    pub node: String,
    pub port: String,
}

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

/// What a seeded run reuses (`crate::seeding::seed_plan`, the runtime
/// planner that reads it off the old run): the run each reused place's
/// result comes from, which of them stand in for themselves in the cut,
/// the values the old run handed its starts that this one keeps, and the
/// places it runs again that replay the answers their waits were given
/// there.
#[derive(Debug, Default)]
pub struct SeedPlan {
    pub origins: BTreeMap<Located, RunId>,
    pub stand_ins: BTreeMap<Located, StandIn>,
    pub handed: BTreeMap<Located, BTreeMap<String, Value>>,
    pub answers: BTreeSet<Located>,
    pub warnings: Vec<String>,
}

impl SeedPlan {
    /// The places it reuses.
    pub fn reused(&self) -> BTreeSet<Located> {
        self.origins.keys().cloned().collect()
    }
}

/// The members of the group start at `start` that a run walking `runs`
/// runs, when a seed reusing `origins` reuses every one of them (the start
/// is then reused whole and does not run); `None` otherwise. The one
/// reading both the seed planner's announcement and the resolver's skip
/// use.
pub fn reused_group_start(
    g: &ProjectGraph,
    start: &Located,
    runs: &BTreeSet<Located>,
    origins: &BTreeMap<Located, RunId>,
) -> Option<Vec<Located>> {
    let members: Vec<Located> = crate::project::places::members_in(g, &start.id, &start.path).into_iter().filter(|member| runs.contains(member)).collect();
    (!members.is_empty() && members.iter().all(|member| origins.contains_key(member))).then_some(members)
}

/// Resolve a spec: walk its bounds, cut the program, and give the cut what
/// the spec hands in (`from` and `group` values at the starts, `emit`
/// outputs as stand-ins) and the baked infra it reads. Pure: trigger
/// preparation (a fired trigger's snapshot) and seed lookup are the
/// caller's.
///
/// `saved` is what the infra places the run may read have saved
/// (`crate::infra::bake::saved_for_run`): one the run reads only baked
/// outputs of does not run, unless the spec names it.
pub fn resolve_spec(spec: &RunSpec, project: &ProjectDefinition, saved: &crate::infra::bake::Saved) -> Result<Resolved, Refusal> {
    resolve_seeded(spec, project, saved, None)
}

/// [`resolve_spec`], with what a seed reuses (`crate::seeding::seed_plan`,
/// planned over the spec resolved without it): the places it reuses do
/// not run, the ones among them that send into what runs stand in with
/// their saved results, and the starts that run again keep what the old
/// run handed them.
pub fn resolve_seeded(spec: &RunSpec, project: &ProjectDefinition, saved: &crate::infra::bake::Saved, seed: Option<&SeedPlan>) -> Result<Resolved, Refusal> {
    let g = ProjectGraph::new(project);
    let bounds = cut::resolve(&g, &spec.bounds()).map_err(Refusal::error)?;
    let named: BTreeSet<Located> = spec.from.keys().chain(spec.emit.keys()).chain(&spec.target).chain(&spec.before)
        .chain(spec.feed.iter()).chain(spec.group.iter().map(|(group, _)| group)).chain(spec.fire.iter().map(|(node, _)| node))
        .map(|spelled| crate::project::places::named(&g, spelled).map(crate::project::places::Named::into_place))
        .collect::<Result<_, String>>().map_err(Refusal::error)?;
    let fired = bounds.fired.clone();
    let (mut walked, baked) = walk_reading_baked(&g, bounds, saved, &named);
    // The bounds name nothing to run. A seed reusing all of what they name
    // leaves nothing to run either, which is a run that reuses everything.
    let walked_nothing = walked.runs.is_empty() && walked.stand_ins.is_empty();
    // What the bounds run before the seed takes what it reuses out.
    let authored_runs = walked.runs.clone();
    if let Some(seed) = seed {
        let reused = seed.reused();
        walked.runs.retain(|place| !reused.contains(place));
        walked.starts.retain(|place| !reused.contains(place));
        walked.stand_ins.extend(seed.stand_ins.keys().cloned());
    }
    let bounded = spec.is_bounded() || !baked.is_empty() || seed.is_some();

    let mut refusal = Refusal::default();
    let mut warnings = Vec::new();
    let handed = spec.handed(project).map_err(Refusal::error)?;
    // What the seed carries over for a start that runs again was handed
    // to that start's code as it was then, so it is held to the program as
    // it is now, like a value handed today: a port gone is said, a value
    // its port no longer takes is refused.
    let mut inputs: BTreeMap<Located, BTreeMap<String, Value>> = BTreeMap::new();
    for (place, ports) in seed.map(|seed| &seed.handed).into_iter().flatten() {
        let spelled = crate::project::address_of(project, &place.id, &place.path);
        let Some(node) = g.node(&place.id) else { continue };
        // Each port's type at this place (a node inside an included file
        // has the types its site gave it).
        let types = node.types_at(&place.path);
        for (port, value) in ports {
            match types.input(port) {
                None => warnings.push(format!("dropped '{spelled}.{port}' the seed run was handed: this input port does not exist in the current program")),
                Some(port_type) => match validate_supplied_value(port_type, value) {
                    Err(error) if !crate::run::skip::is_gate_port(port) => refusal.errors.push(format!(
                        "{spelled}.{port}, as the seed run was handed it: {error}. Hand it a value that fits (--from {spelled}=...)"
                    )),
                    _ => { inputs.entry(place.clone()).or_default().insert(port.clone(), value.clone()); }
                },
            }
        }
    }
    for (place, ports) in &handed {
        let spelled = crate::project::address_of(project, &place.id, &place.path);
        if let Some(node) = g.node(&place.id) {
            if node.features.is_trigger {
                refusal.errors.push(format!("trigger '{spelled}' cannot be a from start; use --fire with a prepared trigger or --emit to supply its outputs"));
                continue;
            }
            // A start the seed reuses kept the result it made with these
            // values (`crate::seeding::seed_plan` says so): it does not run.
            if seed.is_some_and(|seed| seed.origins.contains_key(place)) {
                continue;
            }
            if !ports.is_empty() && !walked.runs.contains(place) && !walked.stand_ins.contains(place) {
                refusal.errors.push(format!("supplied start '{spelled}' is outside the selected run"));
            }
            let types = node.types_at(&place.path);
            for (port, value) in ports {
                match types.input(port) {
                    None => warnings.push(format!("ignored supplied input '{spelled}.{port}': this input port does not exist in the current program")),
                    Some(port_type) => {
                        // The language's gate takes any value: false stops,
                        // any other value lets it flow.
                        if !crate::run::skip::is_gate_port(port) {
                            if let Err(error) = validate_supplied_value(port_type, value) {
                                refusal.errors.push(format!("{spelled}.{port}: {error}"));
                                continue;
                            }
                        }
                        inputs.entry(place.clone()).or_default().insert(port.clone(), value.clone());
                    }
                }
            }
        } else if let Some(group) = g.group(&place.id) {
            let spelled = crate::project::group_address(project, &place.id, &place.path);
            let members = crate::project::places::members_in(&g, &place.id, &place.path);
            // A group start the seed reuses whole kept what it made with
            // these values (`crate::seeding::seed_plan` says so): it does
            // not run. One the walk does not reach is no start of this run.
            if seed.is_some_and(|seed| reused_group_start(&g, place, &authored_runs, &seed.origins).is_some()) {
                continue;
            }
            if !members.iter().any(|member| walked.runs.contains(member) || walked.stand_ins.contains(member)) {
                refusal.errors.push(format!("start '{spelled}' is outside the selected run"));
                continue;
            }
            // A loop takes its ports, its gate among them, on its `LoopIn`.
            let entry = g.is_loop(&place.id).then(|| Located::new(crate::project::loop_in_id(&place.id), place.path.clone()));
            for (port, value) in ports {
                if let Some(entry) = &entry {
                    let declared = g.node(&entry.id).and_then(|node| node.types_at(&entry.path).input(port));
                    match declared {
                        None => warnings.push(format!("ignored supplied input '{spelled}.{port}': this input port does not exist in the current program")),
                        Some(port_type) => match validate_supplied_value(port_type, value) {
                            Err(error) if !crate::run::skip::is_gate_port(port) => refusal.errors.push(format!("{spelled}.{port}: {error}")),
                            _ => { inputs.entry(entry.clone()).or_default().insert(port.clone(), value.clone()); }
                        },
                    }
                    continue;
                }
                if crate::run::skip::is_gate_port(port) {
                    inputs.entry(place.clone()).or_default().insert(port.clone(), value.clone());
                    continue;
                }
                // The port's type at this place (a group inside an included
                // file has the types its site gave it).
                let Some(declared) = group.types_at(&place.path).input(port) else {
                    warnings.push(format!("ignored supplied input '{spelled}.{port}': this input port does not exist in the current program"));
                    continue;
                };
                if let Err(error) = validate_supplied_value(declared, value) {
                    refusal.errors.push(format!("{spelled}.{port}: {error}"));
                    continue;
                }
                let readers = group_readers(&g, place, port);
                if readers.is_empty() {
                    warnings.push(format!("ignored supplied input '{spelled}.{port}': nothing inside '{spelled}' reads it"));
                }
                for (reader, reader_port, path) in readers {
                    let delivered = if path.is_empty() {
                        Some(value.clone())
                    } else {
                        match crate::deref::project_value(&std::sync::Arc::new(value.clone()), declared, &path) {
                            Ok(crate::deref::Projection::Value(value)) => Some(value),
                            Ok(crate::deref::Projection::Closed { .. }) => None,
                            Err(error) => {
                                refusal.errors.push(format!("{spelled}.{port}: {error}"));
                                None
                            }
                        }
                    };
                    if let Some(value) = delivered {
                        inputs.entry(reader).or_default().insert(reader_port, value);
                    }
                }
            }
        }
    }
    let mut outputs = baked_stand_ins(&baked);
    outputs.extend(seed.into_iter().flat_map(|seed| seed.stand_ins.clone()));
    for (spelled, ports) in &spec.emit {
        let place = match crate::project::places::named(&g, spelled) {
            Ok(crate::project::places::Named::Node(place)) => place,
            Ok(crate::project::places::Named::Group(_)) => {
                refusal.errors.push(format!("cannot supply outputs of group '{spelled}'; use --from {spelled} with its input payload"));
                continue;
            }
            Err(why) => {
                refusal.errors.push(why);
                continue;
            }
        };
        let Some(node) = g.node(&place.id) else { continue };
        let mut given = BTreeMap::new();
        let types = node.types_at(&place.path);
        for (port, value) in ports {
            match types.output(port) {
                None => refusal.errors.push(format!("unknown output port '{spelled}.{port}'")),
                Some(declared) => match validate_supplied_value(declared, value) {
                    Ok(()) => { given.insert(port.clone(), GivenPort::Value { value: value.clone() }); }
                    Err(error) => refusal.errors.push(format!("{spelled}.{port}: {error}")),
                },
            }
        }
        outputs.insert(place, StandIn { ports: given, origin: GivenOrigin::Emitted });
    }
    let fire = fired.zip(spec.fire.as_ref().map(|(_, value)| value));
    if let Some((fire, payload)) = &fire {
        if !walked.runs.contains(fire) {
            refusal.errors.push(format!("fired trigger '{}' is outside the selected run", crate::project::address_of(project, &fire.id, &fire.path)));
        }
        // What the trigger wakes with is declared (`firesWith`), so a
        // payload that does not match is refused HERE, before anything
        // is built or started, and the refusal prints the shape it
        // wanted. The engine holds every firing to the same contract;
        // this is the same check, early, where the author is still
        // looking at the command they typed.
        if let Some(node) = g.node(&fire.id) {
            if let Err(why) = crate::node::check_fire_payload(&node.fires_with, Some(payload)) {
                refusal.errors.push(format!("--fire {}: {why}", crate::project::address_of(project, &fire.id, &fire.path)));
            }
        }
    }
    if !refusal.is_empty() { return Err(refusal); }
    if walked_nothing {
        return Err(Refusal::error("nothing runs between these starts and endpoints; choose them on a connected path"));
    }
    let mut given = Given { inputs, outputs };
    let crossings = if bounded { crossings_of(project, &walked, &given) } else { Vec::new() };
    // Every run runs its cut, an unbounded one too: what keeps every
    // place still leaves out the wires nothing carries in it (onto a gate
    // from a trigger it does not fire, into a trigger), so a gate fed only
    // by a trigger reads its default however the run was bounded.
    let program = cut::cut(project, &walked, &mut given).map_err(Refusal::error)?;
    let kicks = KickPlan::for_program(&program, fire.as_ref().map(|(at, value)| (at, *value)), None);
    if !spec.from.is_empty() && spec.target.is_empty() && spec.before.is_empty() {
        warnings.push("No target or before endpoint: the run continues to the end of its downstream graph.".into());
    }
    warnings.extend(trigger_warnings(project, &program, fire.as_ref().map(|(at, _)| at)));
    // An unfed input something needs is a refusal (`refuse_unfed`), and
    // that check waits for the cut's final shape: a seeded run's history
    // can feed it. What nothing needs only closes, and says so.
    for crossing in crossings.iter().filter(|c| !c.supplied && !c.required) {
        let why = if crossing.unfired_trigger { "is a trigger this run does not fire" } else { "is outside it" };
        warnings.push(format!("{}.{} gets nothing in this run ({}.{} {why}), so it closes; nothing that runs goes without it.",
            crossing.node, crossing.port, crossing.source_node, crossing.source_port));
    }
    let runs = crate::project::places::running_places(&program);
    Ok(Resolved { program, given, walk: walked, runs, kicks, crossings, warnings, baked })
}

/// The members of the group at `group` that read its input `port`, each
/// with the port it reads it on and the key path the value passes after
/// the group's port.
fn group_readers(g: &ProjectGraph, group: &Located, port: &str) -> Vec<(Located, String, Vec<String>)> {
    let mut readers = Vec::new();
    for member in crate::project::places::members_in(g, &group.id, &group.path) {
        for (edge, _) in crate::project::places::incoming(g, &member) {
            let Some(k) = edge.via.iter().position(|c| c.side == crate::project::CrossingSide::In && c.group == group.id && c.port == port) else { continue };
            readers.push((member.clone(), edge.target_port().to_string(), edge.path_after(k)));
        }
    }
    readers
}

/// Say per trigger in the run that does not fire what that costs: it
/// never reads its wires and closes its outputs, which from the graph
/// alone looks like any other node. The fired one says which of its
/// wires it does not read (it replays the inputs its bake captured).
fn trigger_warnings(project: &ProjectDefinition, program: &ProjectDefinition, fired: Option<&Located>) -> Vec<String> {
    let spell = |place: &Located| crate::project::address_of(project, &place.id, &place.path);
    let g = ProjectGraph::new(project);
    let running = crate::project::places::running_places(program);
    let mut warnings = Vec::new();
    for trigger in crate::project::trigger_places(program).into_iter().filter(|place| running.contains(place)) {
        if fired == Some(&trigger) {
            let undelivered: Vec<String> = crate::project::places::incoming(&g, &trigger)
                .map(|(edge, source)| format!("{}.{}", spell(&source), edge.source_port()))
                .collect();
            if !undelivered.is_empty() {
                warnings.push(format!(
                    "{} not delivered to trigger '{}': a fired trigger reads the inputs its bake captured; change them with weft bake.",
                    undelivered.join(", "), spell(&trigger)
                ));
            }
            continue;
        }
        let downstream: Vec<String> = crate::project::places::outgoing(&g, &trigger)
            .filter(|(_, target)| running.contains(target))
            .map(|(_, target)| spell(&target))
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
            spell(&trigger), spell(&trigger)
        ));
    }
    warnings
}

/// The inputs of the run whose wire's source carries nothing in it (the
/// source does not run, or it is a trigger the run does not fire), each
/// saying whether something in the run needs it (through a loop's
/// `LoopIn` to the member that reads it, see
/// [`crate::project::places::required_consumer`]) and whether the run
/// gives it a value anyway (handed at a start, or a stand-in sending it).
pub fn crossings_of(project: &ProjectDefinition, walked: &cut::Walk, given: &Given) -> Vec<CrossingPort> {
    let g = ProjectGraph::new(project);
    let kept = walked.kept();
    let spell = |at: &Located| crate::project::address_of(project, &at.id, &at.path);
    let handed = |at: &Located, port: &str| given.inputs.get(at).is_some_and(|ports| ports.contains_key(port));
    let unfired = |at: &Located| g.is_trigger(&at.id) && walked.fired.as_ref() != Some(at) && !walked.stand_ins.contains(at);
    let mut out = Vec::new();
    for place in &walked.runs {
        if g.is_trigger(&place.id) { continue; }
        for (edge, source) in crate::project::places::incoming(&g, place) {
            if g.group(&source.id).is_some() { continue; }
            if crate::run::skip::is_gate_port(edge.target_port()) { continue; }
            let from_trigger = kept.contains(&source) && unfired(&source);
            if kept.contains(&source) && !from_trigger { continue; }
            let port = edge.target_port();
            // A value matters where the run executes, never at a trigger (a
            // fired one reads its bake, the others close without reading),
            // and not past a start handed it.
            let counts = |at: &Located, port: &str| walked.runs.contains(at) && !g.is_trigger(&at.id) && !handed(at, port);
            let need = crate::project::places::required_consumer(
                project, place, port, &counts, &|at| walked.starts.contains(at),
                &|at, port| gets_value(project, &g, walked, given, at, port),
            );
            let needed_by = need.as_ref().map(|need| format!("{}.{}", spell(&need.needer.0), need.needer.1));
            let hand_at = enters_started(&g, edge, place, walked)
                .or_else(|| need.as_ref().and_then(|need| need.start.as_ref())
                    .map(|(at, port)| StartPort { node: spell(at), port: port.clone() }));
            out.push(CrossingPort {
                node: spell(place),
                port: port.into(),
                source_node: spell(&source),
                source_port: edge.source_port().into(),
                unfired_trigger: from_trigger,
                required: needed_by.is_some(),
                needed_by,
                hand_at,
                supplied: handed(place, port),
            });
        }
    }
    out
}

/// The started group `edge` enters on its way to `at`, and the port it
/// enters on (`cut::started_group_entered`), spelled where a person hands
/// the value the wire would carry.
fn enters_started(g: &ProjectGraph, edge: &crate::project::Edge, at: &Located, walked: &cut::Walk) -> Option<StartPort> {
    let (group, port) = cut::started_group_entered(g, edge, at, &walked.group_starts)?;
    Some(StartPort { node: crate::project::group_address(g.project(), &group.id, &group.path), port })
}

/// Whether an input of the node at `at` gets a value in this run other
/// than over a crossing: handed at a start, written in the source, a
/// default, or a wire from something the run runs or stands in for. Read
/// generously (a wire from a place that runs counts even if that place
/// could skip), so a refusal built on it is never wrong.
fn gets_value(project: &ProjectDefinition, g: &ProjectGraph, walked: &cut::Walk, given: &Given, at: &Located, port: &str) -> bool {
    let Some(node) = project.nodes.iter().find(|n| n.id == at.id) else { return false };
    given.inputs.get(at).is_some_and(|ports| ports.contains_key(port))
        || node.port_literals.get(port).is_some_and(|value| crate::run::ready::literal_is_data(node, port, value))
        || node.inputs.iter().any(|input| input.name == port && input.default.is_some())
        || crate::project::places::incoming(g, at).any(|(edge, source)| edge.target_port() == port
            && (walked.kept().contains(&source) || g.group(&source.id).is_some()))
}

/// One input something in a run needs that gets nothing ([`unfed`]): the
/// sentence that names it, and the trigger it only comes from when that is
/// why (a trigger the run does not fire), spelled the way the program
/// reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unfed {
    pub message: String,
    pub trigger: Option<String>,
}

/// Refuse a run in which an input something needs gets nothing
/// ([`unfed`]).
pub fn refuse_unfed(project: &ProjectDefinition, resolved: &Resolved, spec: &RunSpec) -> Result<(), Refusal> {
    let errors: Vec<String> = unfed(project, resolved, spec).into_iter().map(|unfed| unfed.message).collect();
    if errors.is_empty() { Ok(()) } else { Err(Refusal { errors }) }
}

/// Every input in a run that something needs and that gets nothing: its
/// wire's source does not run (or is a trigger the run does not fire), no
/// value is handed in its place, and an input its node cannot run without
/// would close. Checked against the run as it will run, after a seed has
/// had its say, since history can feed it. Each is named where the person
/// can act on it, once: one port of a start wired to several inputs inside
/// is one value to hand.
pub fn unfed(project: &ProjectDefinition, resolved: &Resolved, spec: &RunSpec) -> Vec<Unfed> {
    let flag = start_flag(project, spec);
    let mut named = BTreeSet::new();
    resolved.crossings.iter()
        .filter(|c| !c.supplied)
        .filter_map(|c| c.needed_by.clone().map(|needed_by| (c, needed_by)))
        // Named where the person can act: the start the value would pass,
        // else the input the wire lands on.
        .map(|(c, needed_by)| {
            let (node, port) = match &c.hand_at {
                Some(at) => (at.node.clone(), at.port.clone()),
                None => (c.node.clone(), c.port.clone()),
            };
            (c, needed_by, node, port)
        })
        .filter(|(c, _, node, port)| named.insert((node.clone(), port.clone(), c.source_node.clone(), c.source_port.clone())))
        .map(|(c, needed_by, node, port)| {
            let inside = if needed_by == format!("{node}.{port}") { String::new() }
                else { format!(", and {needed_by} cannot run without it") };
            let (src, src_port) = (&c.source_node, &c.source_port);
            if c.unfired_trigger {
                let message = format!(
                    "{node}.{port} gets nothing in this run: it only comes from the trigger {src}, which this run does \
                     not fire{inside}. Fire it (--fire {src}='<event>'), or hand the value at a start ({} {node}='{{\"{port}\": ...}}').",
                    flag(&node),
                );
                return Unfed { message, trigger: Some(src.clone()) };
            }
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
            Unfed { message: format!("{node}.{port} gets nothing in this run: {src}.{src_port} is outside it{inside}. {fix}"), trigger: None }
        })
        .collect()
}

/// Everything that makes a run pointless before it starts, checked
/// against the run as it will run: an input something needs that gets
/// nothing ([`refuse_unfed`]), and a run that reaches something per
/// instance without saying which instance it is for
/// ([`refuse_instanceless`]).
pub fn refuse_unrunnable(project: &ProjectDefinition, resolved: &Resolved, spec: &RunSpec) -> Result<(), Refusal> {
    let mut refusal = refuse_unfed(project, resolved, spec).err().unwrap_or_default();
    if let Err(instanceless) = refuse_instanceless(&resolved.program, spec.instance.as_ref()) {
        refusal.errors.extend(instanceless.errors);
    }
    if refusal.is_empty() { Ok(()) } else { Err(refusal) }
}

/// The places `program` runs that exist once per instance (marked or
/// reached from a mark), spelled the way the program reads them, sorted.
/// A place that stands in for its node never runs, so it needs no
/// instance.
pub fn per_instance_places(program: &ProjectDefinition) -> Vec<String> {
    crate::project::places::running_places(program).iter()
        .filter(|place| crate::project::is_per_instance(program, &place.id))
        .map(|place| crate::project::address_of(program, &place.id, &place.path))
        .collect::<BTreeSet<_>>().into_iter().collect()
}

/// Refuse a run that reaches something per instance without saying which
/// instance it is for: nothing could pick that instance's connection, copy
/// or storage. An instance given to a run that reaches nothing per instance
/// is fine (the id is simply unused), and so is a run with no instance that
/// reaches nothing per instance: a cron on the shared database beside a
/// per-instance bridge runs for no instance.
pub fn refuse_instanceless(program: &ProjectDefinition, instance: Option<&crate::instance::InstanceId>) -> Result<(), Refusal> {
    if instance.is_some() {
        return Ok(());
    }
    let places = per_instance_places(program);
    if places.is_empty() {
        return Ok(());
    }
    Err(Refusal::error(format!(
        "{} {} once per instance, so this run needs to know which instance it is for. Name one with \
         --instance <id>; a trigger firing through an instance's copy, an instance token, or the Weft-Instance \
         header on a gated route name it for you",
        crate::comma_list::spell_list(&places),
        if places.len() == 1 { "exists" } else { "exist" },
    )))
}

/// What a run of `program` for `instance` starts with: the instance's
/// `stored` values for the places of the run that are `@instance_filled`,
/// each held to its field ([`crate::instance::check_instance_value`]), or the
/// refusal naming every gap ([`crate::instance::unfilled_gaps`],
/// [`crate::instance::rule_gaps`]): a field the
/// run needs and the instance never filled, a value the node's rules refuse
/// with it swapped in. The answer is what the run carries and every
/// firing reads, so a value changed after the run is born reaches the
/// next run, never this one.
pub fn instance_run_values(
    project: &ProjectDefinition,
    instance: &crate::instance::InstanceId,
    stored: &crate::instance::InstanceValues,
) -> Result<crate::instance::InstanceValues, Refusal> {
    let mut values = crate::instance::InstanceValues::new();
    let mut refusal = Refusal { errors: Vec::new() };
    let mut seen = BTreeSet::new();
    let rules = crate::rules::RuleContext::new(project);
    // Only what runs reads its instance's values: a stand-in never does.
    for place in &crate::project::places::running_places(project) {
        let Some(node) = project.nodes.iter().find(|n| n.id == place.id) else { continue };
        if crate::instance::instance_filled_fields(node).next().is_none() {
            continue;
        }
        let spelled = crate::project::address_of(project, &place.id, &place.path);
        if !seen.insert(spelled.clone()) {
            continue;
        }
        let mut kept = crate::instance::PlaceValues::new();
        let mut mistyped = Vec::new();
        for (field, value) in stored.get(&spelled).into_iter().flatten() {
            // A value for a field the program no longer fills (the marker
            // was removed, the node changed) stays stored and unused.
            if !crate::instance::instance_filled_fields(node).any(|(filled, _)| filled == field) {
                continue;
            }
            match crate::instance::check_instance_value(node, field, value) {
                Ok(typed) => {
                    kept.insert(field.clone(), typed);
                }
                Err(why) => {
                    refusal.errors.push(format!("instance '{instance}' at '{spelled}': {why}"));
                    mistyped.push(field.clone());
                }
            }
        }
        let mut filled = crate::instance::filled_node(node, Some(&kept));
        // A field whose stored value failed its check was already refused
        // above: it stays `@instance_filled` (there, value not known yet),
        // so it is never also said as unfilled or tripping a rule.
        for field in mistyped {
            filled.port_literals.insert(field.clone(), node.port_literals[&field].clone());
        }
        // A required field left empty can also trip a rule about it; the
        // refusal says it once.
        for gap in crate::instance::unfilled_gaps(node, &filled, &spelled, instance)
            .into_iter()
            .chain(crate::instance::rule_gaps(&rules, node, &filled, &spelled, instance))
        {
            if !refusal.errors.contains(&gap) {
                refusal.errors.push(gap);
            }
        }
        if !kept.is_empty() {
            values.insert(spelled, kept);
        }
    }
    if refusal.is_empty() { Ok(values) } else { Err(refusal) }
}

/// One value asked to be stored for an instance (by its token or by the
/// program): `value`
/// for `field` of the node at `node` (its place, spelled).
// SYNC: InstanceValueInput <-> packages/weft-connect/src/core/wire.ts InstanceValueInput
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InstanceValueInput {
    pub node: String,
    pub field: String,
    pub value: serde_json::Value,
}

/// One instance-filled field, named: `field` of the node at `node`.
// SYNC: InstanceFieldRef <-> packages/weft-connect/src/core/wire.ts InstanceFieldRef
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct InstanceFieldRef {
    pub node: String,
    pub field: String,
}

/// A [`InstanceValueInput`] the program accepts, as it is kept: the value
/// cast to its field, and for a connection field the connection it names
/// with the service the field connects to.
#[derive(Debug, Clone, PartialEq)]
pub struct CheckedInstanceValue {
    pub node: String,
    pub field: String,
    pub value: serde_json::Value,
    pub connection: Option<(uuid::Uuid, String)>,
}

/// Hold values asked to be stored for an instance, before anything is
/// stored: each must be for an `@instance_filled` field at one of the
/// program's places, of the field's type, and the node's rules must not
/// refuse it with the instance's values (`stored`, then these) swapped in. A
/// field the instance has not reached yet reads as not known yet, so a form
/// filled one field at a time is never refused for the fields after it;
/// a run is refused for those (`instance_run_values`). All or nothing: the
/// refusal names every value refused.
pub fn check_instance_values(
    project: &ProjectDefinition,
    instance: &crate::instance::InstanceId,
    stored: &crate::instance::InstanceValues,
    inputs: &[InstanceValueInput],
) -> Result<Vec<CheckedInstanceValue>, Refusal> {
    let places: BTreeMap<String, &crate::project::NodeDefinition> =
        crate::project::instance_filled_places(project).into_iter().map(|filled| (filled.spelled, filled.node)).collect();
    let mut refusal = Refusal { errors: Vec::new() };
    let mut checked = Vec::new();
    let mut after = stored.clone();
    for input in inputs {
        let Some(node) = places.get(&input.node) else {
            refusal.errors.push(format!("'{}' is no node with a field each instance fills", input.node));
            continue;
        };
        let value = match crate::instance::check_instance_value(node, &input.field, &input.value) {
            Ok(value) => value,
            Err(why) => {
                refusal.errors.push(format!("'{}.{}': {why}", input.node, input.field));
                continue;
            }
        };
        let is_connection = node
            .inputs
            .iter()
            .any(|i| i.name == input.field && matches!(i.widget, Some(crate::node::Widget::Access { .. })));
        let connection = if is_connection {
            let Some(service) = node.instance_service.as_ref().map(|spec| spec.service.clone()) else {
                refusal.errors.push(format!("'{}.{}' is a connection field with no service recipe", input.node, input.field));
                continue;
            };
            match value.get("id").and_then(serde_json::Value::as_str).and_then(|id| id.parse::<uuid::Uuid>().ok()) {
                Some(id) => Some((id, service)),
                None => {
                    refusal.errors.push(format!(
                        "'{}.{}' takes one of the instance's connections, as {{\"id\": \"<connection id>\"}}",
                        input.node, input.field
                    ));
                    continue;
                }
            }
        } else {
            None
        };
        after.entry(input.node.clone()).or_default().insert(input.field.clone(), value.clone());
        checked.push(CheckedInstanceValue { node: input.node.clone(), field: input.field.clone(), value, connection });
    }
    let touched: BTreeSet<&str> = checked.iter().map(|c| c.node.as_str()).collect();
    let rules = crate::rules::RuleContext::new(project);
    for at in touched {
        let node = places[at];
        let partly = crate::instance::partly_filled_node(node, &after[at]);
        refusal.errors.extend(crate::instance::rule_gaps(&rules, node, &partly, at, instance));
    }
    if refusal.is_empty() { Ok(checked) } else { Err(refusal) }
}

/// The flag a value for the start spelled `node` is handed with: the
/// `--group` start's own, else `--from`.
fn start_flag(project: &ProjectDefinition, spec: &RunSpec) -> impl Fn(&str) -> &'static str {
    // Read the one way every name is read, and spelled back the way a
    // start is named (a site's body as the site).
    let group = spec.group.as_ref().and_then(|(spelled, _)| crate::project::places::named(project, spelled).ok())
        .map(|named| crate::project::group_address(project, &named.place().id, &named.place().path));
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

    /// A given answer is written `payload` (a null one included), a skip
    /// `"skipped": true` and a wait given up `"gaveUp": "<error>"`, each
    /// with nothing else; each reads back as itself.
    #[test]
    fn a_wait_ending_is_a_payload_a_skip_or_a_give_up_on_disk() {
        let cases = [
            (AwaitEnd::Answered { value: json!({ "answer": "yes" }) }, json!({ "node": "review", "frames": [], "payload": { "answer": "yes" } })),
            (AwaitEnd::Answered { value: Value::Null }, json!({ "node": "review", "frames": [], "payload": null })),
            (AwaitEnd::Skipped, json!({ "node": "review", "frames": [], "skipped": true })),
            (AwaitEnd::GaveUp { error: "node 'review' gave up its wait: quiet".into() }, json!({ "node": "review", "frames": [], "gaveUp": "node 'review' gave up its wait: quiet" })),
        ];
        for (ended, file) in cases {
            let answer = Answer { node: "review".into(), frames: vec![], ended, question: None };
            assert_eq!(serde_json::to_value(&answer).unwrap(), file);
            assert_eq!(serde_json::from_value::<Answer>(file).unwrap(), answer);
        }
    }

    /// A saved spec with keys an older weft spelled snake_case reads as
    /// today's spec, which writes in today's shape and reads back as
    /// itself; anything else wrong is still refused.
    #[test]
    fn a_spec_file_an_older_weft_wrote_is_carried_to_todays_shape() {
        let old = r#"{"name":"x","answers":[{"node":"review","gave_up":"quiet"}],"settings":{"hold_secs":5},
                      "frozen_from":{"version":"v","execution_id":"00000000-0000-0000-0000-000000000007","definition_hash":"h"}}"#;
        let read = RunSpec::parse_file(old).unwrap();
        let today = r#"{"name":"x","answers":[{"node":"review","gaveUp":"quiet"}],"settings":{"holdSecs":5},
                        "frozenFrom":{"version":"v","runId":"00000000-0000-0000-0000-000000000007","definitionHash":"h"}}"#;
        assert_eq!(read, RunSpec::parse_file(today).unwrap());
        let written = read.file_text().unwrap();
        assert!(written.starts_with("{\n  \"specVersion\": 2,"), "{written}");
        assert_eq!(RunSpec::parse_file(&written).unwrap(), read, "a current file is read as it is");

        let newer = RunSpec::parse_file(r#"{"specVersion":99,"name":"x"}"#).unwrap_err();
        assert!(newer.contains("run spec was written by a newer weft: it is at version 99"), "{newer}");
        let other = RunSpec::parse_file(r#"{"name":"x","bogus":1}"#).unwrap_err();
        assert!(other.contains("unknown field `bogus`"), "{other}");
        let dup = RunSpec::parse_file(r#"{"name":"x","from":{"a":{"v":1,"v":2}}}"#).unwrap_err();
        assert!(dup.contains("duplicate key 'v'"), "duplicate keys are still caught: {dup}");
        let top_dup = RunSpec::parse_file(r#"{"name":"x","name":"y"}"#).unwrap_err();
        assert!(top_dup.contains("duplicate key 'name'"), "{top_dup}");
    }

    /// A file saying two endings at once, or none, is refused by name;
    /// unknown fields still are.
    #[test]
    fn a_wait_ending_is_exactly_one_of_payload_skipped_or_gave_up() {
        for (two, named) in [
            (json!({ "node": "review", "payload": null, "skipped": true }), "a payload and \"skipped\": true"),
            (json!({ "node": "review", "payload": 1, "gaveUp": "quiet" }), "a payload and \"gaveUp\""),
            (json!({ "node": "review", "skipped": true, "gaveUp": "quiet" }), "\"skipped\": true and \"gaveUp\""),
        ] {
            let error = serde_json::from_value::<Answer>(two).unwrap_err().to_string();
            assert!(error.contains("ended more than one way") && error.contains(named), "{error}");
        }
        for neither in [json!({ "node": "review" }), json!({ "node": "review", "skipped": false })] {
            let error = serde_json::from_value::<Answer>(neither).unwrap_err();
            assert!(error.to_string().contains("says nothing about how its wait ended"), "{error}");
        }
        assert!(serde_json::from_value::<Answer>(json!({ "node": "review", "payload": 1, "stray": 1 })).is_err(), "unknown fields still refused");
    }

    /// An answer frozen before skips and give-ups existed (`node`,
    /// `frames`, `payload`, `question`) reads as the given answer it was.
    #[test]
    fn an_answer_frozen_with_only_a_payload_still_reads() {
        let old: Answer = serde_json::from_str(r#"{"node":"review","frames":[],"payload":"yes","question":"Continue?"}"#).unwrap();
        assert_eq!(old, Answer { node: "review".into(), frames: vec![], ended: AwaitEnd::Answered { value: json!("yes") }, question: Some(json!("Continue?")) });
    }

    #[test]
    fn fire_bake_requires_full_identity_and_an_actual_capture() {
        let program = crate::project::hash::ProgramIdentity {
            definition_hash: "graph".into(), binary_hash: "binary".into(),
            implementations: BTreeMap::from([("T".into(), "implementation".into())]),
        };
        let mut bake = BakeSummary { program: program.clone(), captured: vec!["trigger".into()], run_id: RunId::nil(), at_unix: 1 };
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
            let resolved = resolve_spec(&spec, &project, &Default::default()).unwrap();
            assert_eq!(resolved.given.inputs[&Located::top("a")]["_should_flow"], value);
        }
    }

    #[test]
    fn an_unfed_required_crossing_is_refused_and_a_handed_value_feeds_it() {
        let mut spec = RunSpec { from: BTreeMap::from([("b".into(), BTreeMap::new())]), ..RunSpec::whole("x") };
        let resolved = resolve_spec(&spec, &program(), &Default::default()).unwrap();
        assert!(!resolved.runs.contains(&Located::top("a")));
        assert_eq!(resolved.crossings.len(), 1);
        assert!(!resolved.crossings[0].supplied);
        assert_eq!(resolved.crossings[0].needed_by.as_deref(), Some("b.in"));
        let refusal = refuse_unfed(&program(), &resolved, &spec).unwrap_err().to_string();
        assert!(refusal.contains("b.in gets nothing in this run: a.out is outside it"), "{refusal}");
        spec.from.insert("b".into(), BTreeMap::from([("in".into(), json!("handed"))]));
        let resolved = resolve_spec(&spec, &program(), &Default::default()).unwrap();
        assert!(resolved.crossings[0].supplied);
        refuse_unfed(&program(), &resolved, &spec).expect("the handed value feeds it");
        assert!(!resolved.runs.contains(&Located::top("a")));
        assert!(resolved.given.outputs.is_empty(), "a handed input is no stand-in");
        assert_eq!(resolved.given.inputs[&Located::top("b")]["in"], json!("handed"));
    }

    #[test]
    fn an_unfed_optional_crossing_only_warns() {
        let mut project = program();
        project.nodes[1].inputs[0].port.required = false;
        project.nodes[1].inputs[0].optional = true;
        let spec = RunSpec { from: BTreeMap::from([("b".into(), BTreeMap::new())]), ..RunSpec::whole("x") };
        let resolved = resolve_spec(&spec, &project, &Default::default()).unwrap();
        assert!(resolved.crossings[0].needed_by.is_none());
        refuse_unfed(&project, &resolved, &spec).expect("nothing needs it");
        assert!(resolved.warnings.iter().any(|w| w.contains("b.in gets nothing in this run")), "{:?}", resolved.warnings);
    }

    /// An input the metadata calls optional is required once wired, so a
    /// run cutting its wire needs it handed a value like a required one.
    #[test]
    fn an_unfed_crossing_into_an_optional_wired_input_is_needed() {
        let mut project = program();
        project.nodes[1].inputs[0].port.required = false;
        let spec = RunSpec { from: BTreeMap::from([("b".into(), BTreeMap::new())]), ..RunSpec::whole("x") };
        let resolved = resolve_spec(&spec, &project, &Default::default()).unwrap();
        assert_eq!(resolved.crossings[0].needed_by.as_deref(), Some("b.in"));
    }

    /// One node of a hand-built program: `(id, scope, inputs, outputs,
    /// trigger)`, each input `(name, required)`. An input that is not
    /// required is `optional` (it runs without a value even when its wire
    /// closes), as a `?` makes it.
    fn node(id: &str, scope: &[&str], inputs: &[(&str, bool)], outputs: &[&str], trigger: bool) -> Value {
        json!({
            "id": id, "nodeType": "T", "label": null,
            "config": {}, "position": {"x": 0, "y": 0}, "scope": scope,
            "inputs": inputs.iter().map(|(name, required)| json!({"name": name, "portType": "String", "required": required, "optional": !required})).collect::<Vec<_>>(),
            "outputs": outputs.iter().map(|name| json!({"name": name, "portType": "String", "required": true})).collect::<Vec<_>>(),
            "features": {"isTrigger": trigger}, "requiresInfra": false
        })
    }

    fn wire(from: &str, out: &str, to: &str, input: &str) -> Value {
        through(from, out, &[], to, input)
    }

    /// A wire through the group ports `via`, each `(group, port)`, every
    /// one entered.
    fn through(from: &str, out: &str, via: &[(&str, &str)], to: &str, input: &str) -> Value {
        json!({"id": format!("{from}.{out}->{to}.{input}"), "source": from, "target": to, "sourceHandle": out, "targetHandle": input,
               "via": via.iter().map(|(group, port)| json!({"group": group, "port": port, "side": "in"})).collect::<Vec<_>>()})
    }

    /// A group with the input ports `ins` and the member nodes `members`.
    fn group(id: &str, parent: Option<&str>, children: &[&str], ins: &[&str], members: &[&str]) -> Value {
        json!({"id": id, "kind": "group", "label": null,
               "inPorts": ins.iter().map(|name| json!({"name": name, "portType": "String", "required": false})).collect::<Vec<_>>(),
               "outPorts": [], "parentGroupId": parent, "childGroupIds": children, "nodeIds": members})
    }

    /// A trigger never reads its wires in a run it did not fire (it
    /// closes its outputs), and a fired one reads its bake, so what feeds
    /// a trigger is never an input the run is missing.
    #[test]
    fn a_triggers_input_is_never_missing_from_a_run() {
        let project: ProjectDefinition = serde_json::from_value(json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "nodes": [
                node("bridge", &[], &[], &["url"], false),
                node("hear", &[], &[("url", true)], &["text"], true),
                node("after", &[], &[("text", false)], &[], false),
            ],
            "edges": [wire("bridge", "url", "hear", "url"), wire("hear", "text", "after", "text")],
        })).unwrap();
        let spec = RunSpec { target: vec!["after".into()], ..RunSpec::whole("x") };
        let resolved = resolve_spec(&spec, &project, &Default::default()).unwrap();
        assert!(resolved.runs.contains(&Located::top("hear")), "the walk stops at the trigger, taking it along");
        refuse_unfed(&project, &resolved, &spec).expect("the trigger does not read its input in this run");
    }

    /// The program `trigger_cut` is tested on: `hear` listens at what
    /// `queue` serves and stores what it hears through `db`, reading
    /// `cache` only by its baked `addr`; `tick` writes through `other`.
    fn two_triggers() -> ProjectDefinition {
        let infra = |id: &str, outputs: &[&str], baked: &[&str]| {
            let mut n = node(id, &[], &[], outputs, false);
            n["requiresInfra"] = json!(true);
            n["bakedOutputs"] = json!(baked);
            n
        };
        serde_json::from_value(json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "nodes": [
                infra("queue", &["url"], &[]),
                infra("db", &["conn"], &[]),
                infra("cache", &["addr"], &["addr"]),
                infra("other", &["conn"], &[]),
                node("hear", &[], &[("url", true)], &["text"], true),
                node("store", &[], &[("text", true), ("conn", true), ("addr", true)], &[], false),
                node("tick", &[], &[], &["at"], true),
                node("write", &[], &[("at", true), ("conn", true)], &[], false),
            ],
            "edges": [
                wire("queue", "url", "hear", "url"),
                wire("hear", "text", "store", "text"),
                wire("db", "conn", "store", "conn"),
                wire("cache", "addr", "store", "addr"),
                wire("tick", "at", "write", "at"),
                wire("other", "conn", "write", "conn"),
            ],
        }))
        .unwrap()
    }

    /// A trigger depends on the infra its fires run and the infra its
    /// setup reads, never on infra its fires read only baked, and never
    /// on another trigger's.
    #[test]
    fn a_trigger_depends_on_the_infra_its_cut_runs_and_its_setup_reads() {
        let project = two_triggers();
        assert_eq!(trigger_cut(&project, "hear").unwrap().depends_on, vec!["db".to_string(), "queue".to_string()]);
        assert_eq!(trigger_cut(&project, "tick").unwrap().depends_on, vec!["other".to_string()]);
        assert!(trigger_cut(&project, "store").unwrap_err().contains("not a trigger"));
        let all = trigger_cuts(&project);
        assert_eq!(all.keys().cloned().collect::<Vec<_>>(), vec!["hear".to_string(), "tick".to_string()]);
    }

    /// Infra wired into the trigger's own gate decides whether its setup
    /// runs, read once: it is no dependency of the trigger.
    #[test]
    fn infra_feeding_the_triggers_own_gate_is_no_dependency() {
        let mut project = two_triggers();
        let mut flag = node("flag", &[], &[], &["ok"], false);
        flag["requiresInfra"] = json!(true);
        project.nodes.push(serde_json::from_value(flag).unwrap());
        project.edges.push(serde_json::from_value(wire("flag", "ok", "hear", "_should_flow")).unwrap());
        assert_eq!(trigger_cut(&project, "hear").unwrap().depends_on, vec!["db".to_string(), "queue".to_string()]);
    }

    /// A trigger's fingerprint moves with what its fires run, and an edit
    /// to another trigger's fires leaves it where it was.
    #[test]
    fn a_triggers_fingerprint_moves_with_its_own_cut_only() {
        let before = two_triggers();
        let fingerprint = |project: &ProjectDefinition, trigger: &str| trigger_cut(project, trigger).unwrap().fingerprint;
        let mut edited = before.clone();
        edited.nodes.iter_mut().find(|n| n.id == "write").unwrap().config = serde_json::from_value(json!({ "mode": "append" })).unwrap();
        assert_eq!(fingerprint(&before, "hear"), fingerprint(&edited, "hear"), "an edit outside its cut");
        assert_ne!(fingerprint(&before, "tick"), fingerprint(&edited, "tick"), "an edit inside its cut");
        assert_eq!(fingerprint(&before, "hear"), fingerprint(&before.clone(), "hear"), "a function of the source alone");
    }

    /// What runs in front of a trigger runs at its setup, never at a
    /// fire (a schema made before the trigger listens, say): editing it
    /// moves that trigger's fingerprint, so the trigger reads as behind
    /// and a resync sets it up again. Another trigger's stays put.
    #[test]
    fn a_triggers_fingerprint_moves_with_what_its_setup_runs() {
        let mut before = two_triggers();
        before.nodes.push(serde_json::from_value(node("schema", &[], &[], &["rows"], false)).unwrap());
        before.edges.push(serde_json::from_value(wire("schema", "rows", "hear", "_should_flow")).unwrap());
        let fingerprint = |project: &ProjectDefinition, trigger: &str| trigger_cut(project, trigger).unwrap().fingerprint;
        let mut edited = before.clone();
        edited.nodes.iter_mut().find(|n| n.id == "schema").unwrap().config =
            serde_json::from_value(json!({ "query": "create table if not exists t (id int)" })).unwrap();
        assert_ne!(fingerprint(&before, "hear"), fingerprint(&edited, "hear"), "an edit in front of the trigger");
        assert_eq!(fingerprint(&before, "tick"), fingerprint(&edited, "tick"), "an edit in front of another trigger");
        let fire = |project: &ProjectDefinition| fire_cut(project, "hear", &Default::default()).unwrap().program;
        assert!(fire(&edited).nodes.iter().all(|n| n.id != "schema"), "a fire never runs it");
    }

    /// `--group` on a group inside another: the inner group runs inside
    /// the outer one, and the wire from outside passes both. What the
    /// person hands the inner group feeds it, and a refusal names the
    /// inner group's port, the one they started at, never the outer one.
    #[test]
    fn a_nested_group_run_is_fed_and_named_at_its_own_port() {
        let project: ProjectDefinition = serde_json::from_value(json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "nodes": [
                node("src", &[], &[], &["out"], false),
                node("o.i.need", &["o", "o.i"], &[("in", true)], &[], false),
            ],
            "edges": [
                through("src", "out", &[("o", "x"), ("o.i", "x")], "o.i.need", "in"),
            ],
            "groups": [group("o", None, &["o.i"], &["x"], &[]), group("o.i", Some("o"), &[], &["x"], &["o.i.need"])],
        })).unwrap();
        let spec = RunSpec { group: Some(("o.i".into(), BTreeMap::new())), ..RunSpec::whole("x") };
        let resolved = resolve_spec(&spec, &project, &Default::default()).unwrap();
        let refusal = refuse_unfed(&project, &resolved, &spec).unwrap_err().to_string();
        assert!(refusal.contains("o.i.x gets nothing in this run"), "named at the port started at: {refusal}");
        assert!(!refusal.contains("o.x "), "{refusal}");

        let spec = RunSpec { group: Some(("o.i".into(), BTreeMap::from([("x".into(), json!("hi"))]))), ..RunSpec::whole("x") };
        let resolved = resolve_spec(&spec, &project, &Default::default()).unwrap();
        refuse_unfed(&project, &resolved, &spec).expect("the value handed at the inner group feeds it");
    }

    /// A `@require_one_of` member is needed when every other member of
    /// its set gets nothing: the node would skip without it. Handing the
    /// other member a value makes the crossing merely optional again.
    #[test]
    fn the_last_member_of_a_one_of_set_is_needed() {
        let mut needs = node("n", &[], &[("p", false), ("q", false)], &[], false);
        needs["features"]["oneOfRequired"] = json!([["p", "q"]]);
        let project: ProjectDefinition = serde_json::from_value(json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "nodes": [node("src", &[], &[], &["out"], false), needs],
            "edges": [wire("src", "out", "n", "p")],
        })).unwrap();
        let spec = RunSpec { from: BTreeMap::from([("n".into(), BTreeMap::new())]), ..RunSpec::whole("x") };
        let resolved = resolve_spec(&spec, &project, &Default::default()).unwrap();
        let refusal = refuse_unfed(&project, &resolved, &spec).unwrap_err().to_string();
        assert!(refusal.contains("n.p gets nothing in this run") && refusal.contains("--from n="), "{refusal}");

        let spec = RunSpec { from: BTreeMap::from([("n".into(), BTreeMap::from([("q".into(), json!("hi"))]))]), ..RunSpec::whole("x") };
        let resolved = resolve_spec(&spec, &project, &Default::default()).unwrap();
        refuse_unfed(&project, &resolved, &spec).expect("q is handed, so p may close");
    }

    /// A refusal names the flag the value is handed with: `--group` for
    /// the group start. Two needers behind one port make one refusal.
    #[test]
    fn a_refusal_names_the_start_flag() {
        let project: ProjectDefinition = serde_json::from_value(json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "nodes": [
                node("src", &[], &[], &["out"], false),
                node("g.one", &["g"], &[("in", true)], &[], false),
                node("g.two", &["g"], &[("in", true)], &[], false),
            ],
            "edges": [
                through("src", "out", &[("g", "a")], "g.one", "in"),
                through("src", "out", &[("g", "a")], "g.two", "in"),
            ],
            "groups": [group("g", None, &[], &["a"], &["g.one", "g.two"])],
        })).unwrap();
        let spec = RunSpec { group: Some(("g".into(), BTreeMap::new())), ..RunSpec::whole("x") };
        let resolved = resolve_spec(&spec, &project, &Default::default()).unwrap();
        let refusal = refuse_unfed(&project, &resolved, &spec).unwrap_err();
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
                node("src", &[], &[], &["out"], false),
                node("g.need", &["g"], &[("in", true)], &[], false),
                node("g.maybe", &["g"], &[("in", false)], &[], false),
            ],
            "edges": [
                through("src", "out", &[("g", "a")], "g.need", "in"),
                through("src", "out", &[("g", "b")], "g.maybe", "in"),
            ],
            "groups": [group("g", None, &[], &["a", "b"], &["g.need", "g.maybe"])],
        })).unwrap();
        let spec = RunSpec { group: Some(("g".into(), BTreeMap::new())), ..RunSpec::whole("x") };
        let resolved = resolve_spec(&spec, &project, &Default::default()).unwrap();
        let refusal = refuse_unfed(&project, &resolved, &spec).unwrap_err();
        assert_eq!(refusal.errors.len(), 1, "only the port a required input needs: {refusal}");
        assert!(refusal.errors[0].contains(".a gets nothing") && refusal.errors[0].contains("g.need.in"), "{refusal}");

        let spec = RunSpec { group: Some(("g".into(), BTreeMap::from([("a".into(), json!("hi"))]))), ..RunSpec::whole("x") };
        let resolved = resolve_spec(&spec, &project, &Default::default()).unwrap();
        refuse_unfed(&project, &resolved, &spec).expect("the handed value feeds it");
    }

    /// A group nothing in the cut waits on (no gate, no member starting with
    /// it, no port of its own sending) is dissolved: its members sit in the
    /// scope around it and the wires stop naming it, under the program's own
    /// ids. The same bounds cut the same program; other bounds another.
    #[test]
    fn a_group_nothing_waits_on_is_dissolved_under_its_own_ids() {
        let project: ProjectDefinition = serde_json::from_value(json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "nodes": [node("src", &[], &[], &["out"], false), node("g.need", &["g"], &[("in", true)], &["out"], false), node("after", &[], &[("in", true)], &[], false)],
            "edges": [through("src", "out", &[("g", "a")], "g.need", "in"), wire("g.need", "out", "after", "in")],
            "groups": [group("g", None, &[], &["a"], &["g.need"])],
        })).unwrap();
        let aimed = |target: &str| resolve_spec(&RunSpec { target: vec![target.into()], ..RunSpec::whole("x") }, &project, &Default::default()).unwrap().program;
        let cut = aimed("g.need");
        assert!(cut.groups.is_empty(), "{:?}", cut.groups);
        let need = cut.nodes.iter().find(|node| node.id == "g.need").expect("the program's own id");
        assert!(need.scope.is_empty());
        assert!(cut.edges.iter().all(|edge| edge.via.is_empty()), "{:?}", cut.edges);
        let hash = |program: &ProjectDefinition| crate::project::hash::compute_definition_hash(program).unwrap();
        assert_eq!(hash(&cut), hash(&aimed("g.need")), "the same bounds, the same cut");
        assert_ne!(hash(&cut), hash(&aimed("after")), "other bounds, another cut");
    }

    /// A wire `out` (a node outside group `g`) reads straight off `g`'s own
    /// port: `g.x` holds a literal and passes it out as `g.y`.
    fn sending_group() -> ProjectDefinition {
        let mut g = group("g", None, &[], &["x"], &[]);
        g["outPorts"] = json!([{"name": "y", "portType": "String", "required": false}]);
        g["portLiterals"] = json!({"x": "hi"});
        serde_json::from_value(json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "nodes": [node("out", &[], &[("in", true)], &[], false)],
            "edges": [{"id": "g.x->out.in", "source": "g", "sourceHandle": "x", "target": "out", "targetHandle": "in",
                       "via": [{"group": "g", "port": "x", "side": "in"}, {"group": "g", "port": "y", "side": "out"}]}],
            "groups": [g],
        })).unwrap()
    }

    /// The table a run of `resolved` holds after its first pass.
    fn first_pass(resolved: &Resolved) -> (crate::pulse::PulseTable, crate::run::scope::ScopeTable, std::collections::HashMap<Located, ()>) {
        let index = crate::project::ProgramIndex::build(&resolved.program);
        let (mut pulses, mut scopes, mut kicked) = (Default::default(), Default::default(), crate::primitive::KickTable::new());
        crate::run::scope::settle_table(&resolved.program, &index, &resolved.given, crate::context::Phase::Fire,
            uuid::Uuid::nil(), 1, &mut pulses, &mut scopes, &mut kicked);
        let kicked = kicked.keys().map(|at| (Located::at(&at.node_id, &at.frames), ())).collect();
        (pulses, scopes, kicked)
    }

    /// A group sending from its own port to a node outside it comes along
    /// to send: a run aimed at that node keeps it, and the node gets what
    /// it sends (it would otherwise wait for good on a wire nobody sends).
    #[test]
    fn a_group_sending_to_a_reader_outside_it_is_kept_to_send() {
        let project = sending_group();
        let spec = RunSpec { target: vec!["out".into()], ..RunSpec::whole("x") };
        let resolved = resolve_spec(&spec, &project, &Default::default()).unwrap();
        assert!(resolved.program.groups.iter().any(|group| group.id == "g"), "{:?}", resolved.program.groups);
        let (pulses, _, _) = first_pass(&resolved);
        assert_eq!(*pulses["out"][0].value, json!("hi"));
    }

    /// A `--from` node below a `--from` group start does not cut the group
    /// off: the run starts at the one furthest up, so the group runs.
    #[test]
    fn a_node_start_below_a_group_start_leaves_the_group_running() {
        let mut g = group("G", None, &[], &[], &["G.m"]);
        g["outPorts"] = json!([{"name": "o", "portType": "String", "required": false}]);
        let project: ProjectDefinition = serde_json::from_value(json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "nodes": [node("G.m", &["G"], &[], &["out"], false), node("N", &[], &[("in", true)], &["out"], false), node("T", &[], &[("in", true)], &[], false)],
            "edges": [
                {"id": "m->N", "source": "G.m", "sourceHandle": "out", "target": "N", "targetHandle": "in", "via": [{"group": "G", "port": "o", "side": "out"}]},
                wire("N", "out", "T", "in"),
            ],
            "groups": [g],
        })).unwrap();
        let spec = RunSpec {
            from: BTreeMap::from([("G".into(), BTreeMap::new()), ("N".into(), BTreeMap::new())]),
            target: vec!["T".into()],
            ..RunSpec::whole("x")
        };
        let resolved = resolve_spec(&spec, &project, &Default::default()).unwrap();
        for place in ["G.m", "N", "T"] {
            assert!(resolved.runs.contains(&Located::top(place)), "{place} runs: {:?}", resolved.runs);
        }
    }

    /// A group whose gate only a trigger feeds runs on the gate's default
    /// in a run that does not fire that trigger, bounded or not: the gate's
    /// wire carries nothing in it, so the cut leaves it out, and the
    /// trigger joins no run that reaches it only through that gate.
    #[test]
    fn a_gate_only_a_quiet_trigger_feeds_keeps_its_default() {
        let mut g = group("g", None, &[], &[], &["g.m"]);
        g["outPorts"] = json!([{"name": "y", "portType": "String", "required": false}]);
        let project: ProjectDefinition = serde_json::from_value(json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "nodes": [node("trig", &[], &[], &["fired"], true), node("g.m", &["g"], &[], &["out"], false), node("out", &[], &[("in", true)], &[], false)],
            "edges": [
                wire("trig", "fired", "g", "_should_flow"),
                {"id": "m->out", "source": "g.m", "sourceHandle": "out", "target": "out", "targetHandle": "in", "via": [{"group": "g", "port": "y", "side": "out"}]},
            ],
            "groups": [g],
        })).unwrap();
        for spec in [RunSpec::whole("x"), RunSpec { target: vec!["out".into()], ..RunSpec::whole("x") }] {
            let resolved = resolve_spec(&spec, &project, &Default::default()).unwrap();
            assert!(!resolved.program.edges.iter().any(|edge| edge.source == "trig"), "the quiet gate wire is left out");
            // `g.m` starts: kicked with the run when the cut dissolved `g`,
            // or by `g` as it opened.
            let (_, scopes, kicked) = first_pass(&resolved);
            assert!(scopes.iter().all(|(_, record)| record.state == crate::run::scope::ScopeState::Open), "nothing skips");
            assert!(kicked.contains_key(&Located::top("g.m")) || resolved.kicks.iter().any(|kick| kick.node == "g.m"), "the group runs on its gate's default");
        }
        let aimed = resolve_spec(&RunSpec { target: vec!["out".into()], ..RunSpec::whole("x") }, &project, &Default::default()).unwrap();
        assert!(!aimed.runs.contains(&Located::top("trig")), "reached only through the gate, the trigger stays out");
    }

    /// Two runs that differ only by the values they hand run one cut: the
    /// program says where values go, and each run carries its own.
    #[test]
    fn runs_differing_only_by_their_values_share_one_cut() {
        let handing = |value: Value| RunSpec {
            from: BTreeMap::from([("b".into(), BTreeMap::from([("in".into(), value)]))]),
            emit: BTreeMap::from([("a".into(), BTreeMap::from([("out".into(), json!("stand-in"))]))]),
            ..RunSpec::whole("x")
        };
        let one = resolve_spec(&handing(json!("one")), &program(), &Default::default()).unwrap();
        let two = resolve_spec(&handing(json!("two")), &program(), &Default::default()).unwrap();
        let hash = |resolved: &Resolved| crate::project::hash::compute_definition_hash(&resolved.program).unwrap();
        assert_eq!(hash(&one), hash(&two), "one cut");
        assert_eq!(one.program.given, one.given.places());
        assert_eq!(one.given.inputs[&Located::top("b")]["in"], json!("one"));
        assert_eq!(two.given.inputs[&Located::top("b")]["in"], json!("two"));
        let json = serde_json::to_string(&one.program).unwrap();
        assert!(!json.contains("\"one\"") && !json.contains("stand-in"), "no value in the program: {json}");
    }

    /// A gate handed at a group start is the gate's: the cut keeps the
    /// group (its gate still decides) and no wire onto that gate, even
    /// from a source the run runs.
    #[test]
    fn a_gate_handed_at_a_group_start_takes_no_wire() {
        let project: ProjectDefinition = serde_json::from_value(json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "nodes": [
                node("src", &[], &[], &["out"], false),
                node("g.need", &["g"], &[("in", false)], &[], false),
            ],
            "edges": [
                wire("src", "out", "g", "_should_flow"),
                through("src", "out", &[("g", "a")], "g.need", "in"),
            ],
            "groups": [group("g", None, &[], &["a"], &["g.need"])],
        })).unwrap();
        let handed = BTreeMap::from([("a".into(), json!("hi")), ("_should_flow".into(), json!(false))]);
        let spec = RunSpec { from: BTreeMap::from([("src".into(), BTreeMap::new())]), target: vec!["g.need".into()], ..RunSpec::whole("x") };
        let wired = resolve_spec(&spec, &project, &Default::default()).unwrap();
        assert!(wired.program.edges.iter().any(|e| e.target == "g"), "without a handed gate the wire decides");
        let spec = RunSpec { from: BTreeMap::from([("g".into(), handed), ("src".into(), BTreeMap::new())]), ..RunSpec::whole("x") };
        let resolved = resolve_spec(&spec, &project, &Default::default()).unwrap();
        assert!(resolved.program.nodes.iter().any(|n| n.id == "src"), "the gate's source runs");
        assert!(resolved.program.groups.iter().any(|g| g.id == "g"), "the handed gate keeps the group");
        assert!(!resolved.program.edges.iter().any(|e| e.target == "g"), "{:?}", resolved.program.edges);
        assert_eq!(resolved.given.inputs[&Located::top("g")]["_should_flow"], json!(false));
    }

    #[test]
    fn simulation_excludes_body_and_allows_unused_declared_output() {
        let spec = RunSpec { emit: BTreeMap::from([("c".into(), BTreeMap::from([("out".into(), json!("x"))]))]),
            ..RunSpec::whole("x") };
        let resolved = resolve_spec(&spec, &program(), &Default::default()).unwrap();
        assert!(resolved.runs.is_empty());
        let stand_in = &resolved.given.outputs[&Located::top("c")];
        assert_eq!(stand_in.ports["out"], crate::project::GivenPort::Value { value: json!("x") });
        assert_eq!(stand_in.origin, crate::project::GivenOrigin::Emitted);
    }

    #[test]
    fn disconnected_or_empty_cuts_are_refused_before_run() {
        let spec = RunSpec { from: [("c".into(), BTreeMap::new())].into(), before: vec!["b".into()], ..RunSpec::whole("empty") };
        assert!(resolve_spec(&spec, &program(), &Default::default()).unwrap_err().to_string().contains("nothing runs between these starts and endpoints"));
        let spec = RunSpec { emit: [("c".into(), [("out".into(), json!("x"))].into())].into(), before: vec!["b".into()], ..RunSpec::whole("empty") };
        assert!(resolve_spec(&spec, &program(), &Default::default()).unwrap_err().to_string().contains("nothing runs between these starts and endpoints"));
    }

    #[test]
    fn flat_wire_shape_retains_frozen_evidence_and_rejects_old_fields() {
        let spec = RunSpec { from: BTreeMap::from([("b".into(), BTreeMap::new())]), before: vec!["c".into()],
            caller: vec![json!({"text": "hello"})],
            answers: vec![Answer { node: "b".into(), frames: vec![], ended: AwaitEnd::Answered { value: json!("yes") }, question: Some(json!("Continue?")) }],
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
            assert!(resolve_spec(&spec, &program(), &Default::default()).unwrap_err().to_string().contains("missing"));
        }
    }

    #[test]
    fn removed_input_ports_warn_without_dropping_the_start_or_widening_the_run() {
        let spec = RunSpec { from: BTreeMap::from([("b".into(), BTreeMap::from([
            ("missing".into(), json!("old value")), ("in".into(), json!("handed"))
        ]))]), ..RunSpec::whole("x") };
        let resolved = resolve_spec(&spec, &program(), &Default::default()).unwrap();
        assert_eq!(resolved.runs, BTreeSet::from([Located::top("b"), Located::top("c")]));
        assert_eq!(resolved.given.inputs[&Located::top("b")], BTreeMap::from([("in".into(), json!("handed"))]));
        assert!(resolved.warnings.iter().any(|warning| warning.contains("ignored supplied input 'b.missing'")));
        assert!(spec.from["b"].contains_key("missing"), "resolution does not rewrite the saved example");
    }

    #[test]
    fn emitted_node_cannot_also_be_an_explicit_start() {
        let spec = RunSpec { from: BTreeMap::from([("b".into(), BTreeMap::new())]),
            emit: BTreeMap::from([("b".into(), BTreeMap::from([("out".into(), json!("x"))]))]),
            ..RunSpec::whole("x") };
        assert!(resolve_spec(&spec, &program(), &Default::default()).unwrap_err().to_string().contains("cannot both run"));
    }

    #[test]
    fn values_handed_to_a_trigger_are_refused_even_when_not_fired() {
        let mut project = program();
        project.nodes[0].features.is_trigger = true;
        let spec = RunSpec { from: BTreeMap::from([("a".into(), BTreeMap::from([("in".into(), json!("x"))]))]),
            ..RunSpec::whole("x") };
        assert!(resolve_spec(&spec, &project, &Default::default()).unwrap_err().to_string().contains("cannot be a from start"));
    }

    #[test]
    fn fire_requires_trigger_and_cannot_be_excluded_by_end_bound() {
        let mut project = program();
        let spec = RunSpec { fire: Some(("a".into(), json!({}))), ..RunSpec::whole("x") };
        assert!(resolve_spec(&spec, &project, &Default::default()).unwrap_err().to_string().contains("not a trigger"));
        project.nodes[0].features.is_trigger = true;
        let spec = RunSpec { before: vec!["a".into()], ..spec };
        assert!(resolve_spec(&spec, &project, &Default::default()).unwrap_err().to_string().contains("outside"));
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
        let resolved = resolve_spec(&spec, &project, &Default::default()).unwrap();
        assert!(!resolved.runs.contains(&Located::top("a")), "{:?}", resolved.runs);
        assert!(resolved.warnings.iter().any(|w| w.contains("a.out not delivered to trigger 'b'") && w.contains("weft bake")),
            "{:?}", resolved.warnings);
    }

    #[test]
    fn a_run_started_by_hand_says_which_trigger_stays_quiet_and_who_skips_for_it() {
        let mut project = program();
        project.nodes[1].features.is_trigger = true;
        let resolved = resolve_spec(&RunSpec::whole("x"), &project, &Default::default()).unwrap();
        let warning = resolved.warnings.iter().find(|w| w.starts_with("trigger 'b' does not fire")).expect("a warning for the quiet trigger");
        assert!(warning.contains("fires no trigger") && warning.contains("c skip") && warning.contains("--fire b="), "{warning}");
        // With another trigger fired, the quiet one is explained by that fire.
        project.nodes[0].features.is_trigger = true;
        let spec = RunSpec { fire: Some(("a".into(), json!({}))), ..RunSpec::whole("x") };
        let resolved = resolve_spec(&spec, &project, &Default::default()).unwrap();
        assert!(resolved.warnings.iter().any(|w| w.starts_with("trigger 'b' does not fire: only 'a' fires")), "{:?}", resolved.warnings);
    }

    #[test]
    fn inclusive_and_exclusive_cuts_preserve_the_authored_path() {
        let spec = RunSpec { from: BTreeMap::from([("b".into(), BTreeMap::new())]), target: vec!["c".into()], ..RunSpec::whole("x") };
        assert_eq!(resolve_spec(&spec, &program(), &Default::default()).unwrap().runs, BTreeSet::from([Located::top("b"), Located::top("c")]));
        let spec = RunSpec { before: vec!["c".into()], ..spec };
        assert_eq!(resolve_spec(&spec, &program(), &Default::default()).unwrap().runs, BTreeSet::from([Located::top("b")]));
        assert!(resolve_spec(&spec, &program(), &Default::default()).unwrap().warnings.iter().all(|w| !w.contains("continues to the end")));
    }

    /// `b` fills `in` from each instance (no fallback, required), and its
    /// rules refuse the value "bad".
    fn filled_program() -> ProjectDefinition {
        let mut project = program();
        project.edges.retain(|e| e.target != "b");
        let b = project.nodes.iter_mut().find(|n| n.id == "b").unwrap();
        b.port_literals.insert("in".into(), crate::instance::instance_filled_literal(None));
        b.instance_rules = Some(serde_json::from_value(json!({ "rules": [{
            "when": { "kind": "config_equals", "field": "in", "equals": "bad" },
            "then": { "message": "'{id}' refuses {field}", "field": "in" }
        }]})).unwrap());
        project
    }

    #[test]
    fn an_instances_run_carries_its_checked_values() {
        let project = filled_program();
        let ada = crate::instance::InstanceId::new("ada").unwrap();
        let stored = |value: serde_json::Value| crate::instance::InstanceValues::from([("b".into(), [("in".to_string(), value)].into())]);

        let values = instance_run_values(&project, &ada, &stored(json!("hello"))).unwrap();
        assert_eq!(values["b"]["in"], json!("hello"));

        let unfilled = instance_run_values(&project, &ada, &Default::default()).unwrap_err().to_string();
        assert!(unfilled.contains("instance 'ada' has not filled 'b.in'"), "{unfilled}");

        let refused = instance_run_values(&project, &ada, &stored(json!("bad"))).unwrap_err().to_string();
        assert!(refused.contains("instance 'ada' at 'b': 'b' refuses in"), "{refused}");

        let mistyped = instance_run_values(&project, &ada, &stored(json!({ "x": 1 }))).unwrap_err();
        // Refused once, for its type, never also as "has not filled".
        assert_eq!(
            mistyped.errors,
            vec!["instance 'ada' at 'b': 'in' takes String: no cast from Dict[String, Number] into String".to_string()]
        );

        // A run that does not reach the filled node needs nothing from the instance.
        let (just_a, _) = setup_cut(&project, &[Located::top("a")]).unwrap();
        assert!(instance_run_values(&just_a, &ada, &Default::default()).unwrap().is_empty());
    }

    /// A rule about a field the instance left empty (a connection's "pick
    /// one on the node", say) is said as the instance's gap, once, even
    /// when the field is required too.
    #[test]
    fn a_rule_on_an_empty_instance_field_names_what_the_instance_did_not_fill() {
        let mut project = filled_program();
        let b = project.nodes.iter_mut().find(|n| n.id == "b").unwrap();
        b.instance_rules = Some(serde_json::from_value(json!({ "rules": [{
            "when": { "kind": "not", "of": { "kind": "config_nonempty", "field": "in" } },
            "then": { "message": "'{id}' has nothing picked; pick one on the node", "field": "in" }
        }]})).unwrap());
        let ada = crate::instance::InstanceId::new("ada").unwrap();
        let refusal = instance_run_values(&project, &ada, &Default::default()).unwrap_err();
        assert_eq!(refusal.errors, vec!["instance 'ada' has not filled 'b.in'".to_string()]);
        project.nodes[1].inputs[0].required = false;
        let refusal = instance_run_values(&project, &ada, &Default::default()).unwrap_err();
        assert_eq!(refusal.errors, vec!["instance 'ada' has not filled 'b.in'".to_string()]);
        // The instance's settings page marks the field as needed for the same reason.
        assert!(crate::instance::instance_field_needed(&crate::rules::RuleContext::new(&project), &project.nodes[1], "in"));
        project.nodes[1].port_literals.insert("in".into(), crate::instance::instance_filled_literal(Some(json!("x"))));
        assert!(!crate::instance::instance_field_needed(&crate::rules::RuleContext::new(&project), &project.nodes[1], "in"));
    }

    #[test]
    fn a_fallback_or_a_default_stands_in_for_an_instance_that_gave_nothing() {
        let ada = crate::instance::InstanceId::new("ada").unwrap();
        let mut with_fallback = filled_program();
        with_fallback.nodes[1].port_literals.insert("in".into(), crate::instance::instance_filled_literal(Some(json!("dflt"))));
        assert!(instance_run_values(&with_fallback, &ada, &Default::default()).is_ok());
        let mut bad_fallback = filled_program();
        bad_fallback.nodes[1].port_literals.insert("in".into(), crate::instance::instance_filled_literal(Some(json!("bad"))));
        assert!(instance_run_values(&bad_fallback, &ada, &Default::default()).is_err());
        let mut defaulted = filled_program();
        defaulted.nodes[1].inputs[0].default = Some(json!("x"));
        assert!(instance_run_values(&defaulted, &ada, &Default::default()).is_ok());
    }

    #[test]
    fn values_are_held_to_the_program_before_they_are_stored() {
        let project = filled_program();
        let ada = crate::instance::InstanceId::new("ada").unwrap();
        let ask = |node: &str, value: serde_json::Value| InstanceValueInput { node: node.into(), field: "in".into(), value };
        let ok = check_instance_values(&project, &ada, &Default::default(), &[ask("b", json!("hello"))]).unwrap();
        assert_eq!(ok, vec![CheckedInstanceValue { node: "b".into(), field: "in".into(), value: json!("hello"), connection: None }]);
        let refused = check_instance_values(&project, &ada, &Default::default(), &[ask("b", json!("bad")), ask("a", json!("x"))])
            .unwrap_err()
            .to_string();
        assert!(refused.contains("'b' refuses in"), "{refused}");
        assert!(refused.contains("'a' is no node with a field each instance fills"), "{refused}");
        let unknown_field = InstanceValueInput { node: "b".into(), field: "other".into(), value: json!(1) };
        assert!(check_instance_values(&project, &ada, &Default::default(), &[unknown_field]).is_err());
    }
}
