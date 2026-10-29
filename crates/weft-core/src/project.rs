//! Graph-level project types. Describes a weft program as a graph:
//! nodes (uses of a node type), edges (connections between port
//! refs). Port and field shapes live on the node TYPE (NodeMetadata),
//! not on the node. `NodeFeatures` is preserved on each node.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::frames::{Located, LoopFrames};
use crate::node::Accepts;
use crate::weft_type::WeftType;

/// The canonical form of a program and the digests over it.
pub mod hash;
pub mod graph;
pub mod scopes;
pub mod places;
pub mod cut;
pub mod coverage;

/// The steps a stored [`ProjectDefinition`] takes from the shape an older
/// weft wrote; `crate::stored` says how they run. A stored program keeps
/// the hash it was written under through every step
/// ([`hash::compute_definition_hash`] says why).
pub const PROJECT_DEFINITION_UPGRADES: &[&[crate::stored::Op]] = &[&[
    // Version 2: the service recipes a node carries moved with
    // `AccessSpec`'s own version 2.
    crate::stored::Op::Within { at: "nodes.*.publishedService", ops: crate::access::spec::ACCESS_SPEC_V2 },
    crate::stored::Op::Within { at: "nodes.*.instanceService", ops: crate::access::spec::ACCESS_SPEC_V2 },
]];

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectDefinition {
    pub id: Uuid,
    pub nodes: Vec<NodeDefinition>,
    pub edges: Vec<Edge>,
    /// The groups, loops, call sites and included bodies, as scopes. A
    /// group is no node: its members sit in `nodes` with the group in
    /// their `scope`, the wires run straight from producers to the
    /// members that read them (each naming the group ports it passes,
    /// [`Edge::via`]), and the engine drives the group as a scope
    /// (`crate::run::scope`). A loop keeps its two boundary nodes, the
    /// machinery its iterations run through.
    #[serde(default)]
    pub groups: Vec<GroupDefinition>,
    #[serde(rename = "createdAt", default = "Utc::now")]
    pub created_at: DateTime<Utc>,
    #[serde(rename = "updatedAt", default = "Utc::now")]
    pub updated_at: DateTime<Utc>,
    /// What the project's `weft.toml` says for every run and trigger that
    /// does not say otherwise (`[runs]`, `[triggers]`).
    #[serde(default, skip_serializing_if = "ProjectDefaults::is_default")]
    pub defaults: ProjectDefaults,
    /// Where a run of this cut is given what it does not compute: the
    /// input ports handed a value per place, and the places whose node
    /// stands in for itself instead of running (an `--emit`, a seed's saved
    /// result, an infra node's baked values). Only WHERE: the values are
    /// the run's ([`Given`], on its birth), so two runs that differ only by
    /// their values run one cut. Empty in a program as written; only
    /// [`cut::cut`] fills it. Keyed by place, because a body is shared: a
    /// node of an included file can be handed a value under one call and
    /// run as written under another.
    #[serde(default, skip_serializing_if = "GivenPlaces::is_empty")]
    pub given: GivenPlaces,
    /// The parts of the program that do not compile: an included file that
    /// does not parse, a node whose type did not load, a wire between two
    /// ports that cannot meet. The program around them still compiles, and
    /// everything that does not need them runs; a cut that keeps one is
    /// refused ([`cut::cut`]), naming what is wrong. Empty in a program
    /// that compiles whole, and in every cut.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub broken: Vec<Broken>,
    /// The wires that leave a metadata port ([`MetadataPort`]), kept only
    /// so the editor can say where the value written on an input came
    /// from. Enrich has already written
    /// each one's value into the input it points at, so none of them
    /// runs, and nothing that runs or checks the program's wires ever
    /// reads this list: a metadata wire may point backwards without
    /// making a cycle. Each stops where the value lands (a node's input,
    /// or a group's port seen from outside), so the wires that carry it
    /// on from there are ordinary ones in `edges`.
    // SYNC: metadata_wires <-> packages/weft-graph/src/protocol.ts ProjectDefinition.metadataWires
    #[serde(default, rename = "metadataWires", skip_serializing_if = "Vec::is_empty")]
    pub metadata_wires: Vec<Edge>,
}

impl ProjectDefinition {
    /// A program as weft keeps it (`project_definition.project_json`, a
    /// project's current one in `project.project_json`).
    pub const STORED: crate::stored::Kind<ProjectDefinition> = crate::stored::Kind::new("program", PROJECT_DEFINITION_UPGRADES);

    /// Whether a broken part may hide nodes ([`Broken::hides`]), so a
    /// trigger or an infra copy this program does not declare is not
    /// proven gone: every decision that takes something down for being
    /// absent from the program (an orphan's reap, an orphan entry row,
    /// a gone copy's disks) asks this first, and leaves it alone.
    pub fn hides_nodes(&self) -> bool {
        self.broken.iter().any(|part| part.hides)
    }
}

/// One part of a program that does not compile
/// ([`ProjectDefinition::broken`]).
// SYNC: Broken <-> packages/weft-graph/src/protocol.ts Broken
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Broken {
    /// The nodes and groups the problem is about, by id. A cut that keeps
    /// every one of them needs the broken part: one id for a node or a
    /// group, the two ends for a wire (a cut that keeps only one end does
    /// not carry the wire).
    pub parts: Vec<String>,
    /// What is wrong, as the compiler says it, with the file and line.
    pub why: String,
    /// The part may stand for nodes the program cannot see: an included
    /// file that does not compile (its placeholder holds none of its
    /// nodes), a node whose type did not load (nothing says whether it is
    /// a trigger or infra). While a program has one, what is missing from
    /// it may only be hidden ([`ProjectDefinition::hides_nodes`]).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub hides: bool,
}

impl Broken {
    /// Whether a cut whose nodes and groups are `kept` needs this part.
    pub fn needed_by(&self, kept: &std::collections::BTreeSet<&str>) -> bool {
        !self.parts.is_empty() && self.parts.iter().all(|part| kept.contains(part.as_str()))
    }
}

/// See [`ProjectDefinition::given`].
// SYNC: GivenPlaces <-> packages/weft-graph/src/protocol.ts GivenPlaces
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GivenPlaces {
    /// Per place, the input ports handed a value. The wire into such a
    /// port (if any) carries nothing at that place: the handed value is
    /// the port's.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub inputs: std::collections::BTreeMap<Located, std::collections::BTreeSet<String>>,
    /// The places whose node sends what the run gives it instead of
    /// running.
    #[serde(default, skip_serializing_if = "std::collections::BTreeSet::is_empty")]
    pub outputs: std::collections::BTreeSet<Located>,
}

impl GivenPlaces {
    pub fn is_empty(&self) -> bool {
        self.inputs.is_empty() && self.outputs.is_empty()
    }
}

/// What a run is given: the values handed to its starts' inputs, and what
/// each stand-in sends, at the places its program says
/// ([`ProjectDefinition::given`]). Carried by the run itself (its birth),
/// never by the program it runs.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Given {
    /// Per place and input port, the value handed to it.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub inputs: std::collections::BTreeMap<Located, std::collections::BTreeMap<String, Value>>,
    /// Per place, what the node there sends instead of running.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub outputs: std::collections::BTreeMap<Located, StandIn>,
}

impl Given {
    pub fn is_empty(&self) -> bool {
        self.inputs.is_empty() && self.outputs.is_empty()
    }

    /// Where these values go: the places a program running them must
    /// say it is given ([`ProjectDefinition::given`]).
    pub fn places(&self) -> GivenPlaces {
        GivenPlaces {
            inputs: self.inputs.iter().map(|(place, ports)| (place.clone(), ports.keys().cloned().collect())).collect(),
            outputs: self.outputs.keys().cloned().collect(),
        }
    }

    /// These values, held to `program`: they go exactly where it says a run
    /// of it is given something. `Err` naming the first place that differs,
    /// which only a corrupt record or a program and values that were never
    /// resolved together produce.
    pub fn check_against(&self, program: &ProjectDefinition) -> Result<(), String> {
        let places = self.places();
        if places == program.given {
            return Ok(());
        }
        let missing_input = program.given.inputs.iter().find(|(place, ports)| places.inputs.get(*place) != Some(*ports));
        let extra_input = places.inputs.keys().find(|place| !program.given.inputs.contains_key(*place));
        let missing_output = program.given.outputs.iter().find(|place| !places.outputs.contains(*place));
        let extra_output = places.outputs.iter().find(|place| !program.given.outputs.contains(*place));
        Err(match (missing_input, extra_input, missing_output, extra_output) {
            (Some((place, _)), _, _, _) | (_, Some(place), _, _) => format!("the values handed to '{place}' are not the ports its program is handed"),
            (_, _, Some(place), _) => format!("'{place}' stands in for itself in its program, and the run gives it nothing to send"),
            (_, _, _, Some(place)) => format!("the run gives '{place}' something to send, and its program runs it"),
            (None, None, None, None) => "the run's given values do not match its program".to_string(),
        })
    }
}

/// A node that does not run: what it sends, port by port, once the scope
/// around it opens (`crate::run::scope`), and where that came from.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StandIn {
    pub ports: std::collections::BTreeMap<String, GivenPort>,
    pub origin: GivenOrigin,
}

/// One output of a [`StandIn`]: a value (a stream's items as a list), or
/// a closure, which keeps the failure it carried when its node broke.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum GivenPort {
    Value { value: Value },
    Closed {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        failure: Option<crate::pulse::Failure>,
    },
}

/// Where a [`StandIn`]'s outputs came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum GivenOrigin {
    /// Handed in by a person (`--emit`).
    Emitted,
    /// An infra node's saved values: it was applied, and a run only reads
    /// what it saved.
    Baked,
    /// The result the run named here left (`--seed`).
    Inherited { run: crate::RunId },
}

/// What a project says once, in `weft.toml`, for all its runs and
/// triggers: how long an ended run is kept (`[runs] keep_for`), and the
/// limits a trigger has when its own inputs leave them unset
/// (`[triggers] calls_per_minute_per_caller`, `calls_per_minute`,
/// `calls_at_once`). Carried in the program's definition, so the runs and
/// the triggers of a version read the defaults that version was built
/// with.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProjectDefaults {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keep_for: Option<crate::run_settings::KeepFor>,
    #[serde(default, skip_serializing_if = "crate::signal::EntryLimits::is_default")]
    pub triggers: crate::signal::EntryLimits,
}

impl ProjectDefaults {
    pub fn is_default(&self) -> bool {
        *self == Self::default()
    }

    /// How long the project keeps an ended run when what started it says
    /// nothing: its `[runs] keep_for`, else weft's own.
    pub fn keep_for(&self) -> crate::run_settings::KeepFor {
        self.keep_for.unwrap_or(crate::run_settings::KeepFor::WEFT_DEFAULT)
    }
}

/// What kind of grouping construct this is: the editor picks a renderer
/// from it, and the engine drives each kind as a scope (a loop through
/// its LoopIn / LoopOut nodes, every other kind with no node at all).
///
/// The loop's config (parallel / over / carry / maxIters /
/// trimOnMismatch) rides INSIDE the `Loop` variant, so "kind ==
/// Loop" and "has a loop config" cannot drift apart (the invalid
/// states "Loop without config" / "Group with config" are
/// unrepresentable). The enum is internally tagged on `kind` and
/// flattened into `GroupDefinition`, so the wire shape stays
/// `{"kind": "group"}` / `{"kind": "loop", "loopConfig": {...}}`.
// SYNC: GroupKind <-> packages/weft-graph/src/protocol.ts GroupDefinition (kind + loopConfig + body)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum GroupKind {
    Group,
    Loop {
        #[serde(rename = "loopConfig")]
        loop_config: serde_json::Value,
    },
    /// One use of an included file: `alias = @include("x.weft")`. The
    /// site has no members of its own: the shared `body` runs under the
    /// caller's frames plus the site's call frame, which the wires
    /// crossing the site push and pop ([`Crossing`]).
    Call {
        body: String,
        /// The places under this site the program runs, each relative to
        /// the site (the node, and the sites below this one on its path);
        /// every place when absent. Only a cut sets it, and only on a site
        /// at the top of the call tree: a body is shared, so two sites
        /// running different parts of it is said here, not by its nodes.
        // SYNC: GroupKind::Call.runs <-> packages/weft-graph/src/protocol.ts GroupDefinition.runs
        #[serde(default, skip_serializing_if = "Option::is_none")]
        runs: Option<std::collections::BTreeSet<Located>>,
    },
    /// An included file, compiled once and shared by every site that
    /// calls it. Its members fire under the caller's frames plus the
    /// site's call frame, so ten uses are one body and ten frames.
    Body,
}

impl GroupKind {
    /// The word a diagnostic calls this container.
    pub fn noun(&self) -> &'static str {
        match self {
            GroupKind::Group => "group",
            GroupKind::Loop { .. } => "loop",
            GroupKind::Call { .. } => "include",
            GroupKind::Body => "included file",
        }
    }
}

/// The node types of a loop's two boundaries, the only boundaries the
/// compiler mints: a loop's iterations run through them. One place for
/// the names, so the engine, the journal fold and the compiler cannot
/// drift on them.
pub mod boundary_types {
    pub const LOOP_IN: &str = "LoopIn";
    pub const LOOP_OUT: &str = "LoopOut";

    /// Any boundary type at all.
    pub fn is_boundary(node_type: &str) -> bool {
        matches!(node_type, LOOP_IN | LOOP_OUT)
    }
}

/// The node type of an `@include` the compiler does not open up: the
/// editor's opaque block, and in a build the placeholder an included file
/// that does not compile leaves at its site ([`own_types`]).
// SYNC: INCLUDED_GROUP_TYPE <-> packages/weft-graph/src/webview/lib/types/index.ts INCLUDE_NODE_TYPE
pub const INCLUDED_GROUP_TYPE: &str = "IncludedGroup";

/// The type names weft gives its own nodes and scopes: the language's
/// `Group` and `Loop`, a loop's two boundaries, and the opaque include.
/// The one list of names the language reserves: no catalog may declare
/// one (`weft_catalog` refuses it, naming the file), no program may name
/// a node or group one (`weft_compiler::weft_compiler::is_reserved_local`),
/// and the editor cannot add a plain node of one, so nothing a project
/// wrote is ever mistaken for the compiler's.
// SYNC: own_types::ALL <-> packages/weft-syntax/weft.tmLanguage.json,
// packages/weft-syntax/highlight-weft.js,
// crates/weft-compiler/tests/integration/highlighting_vocabulary.rs (the word-list alarm)
pub mod own_types {
    pub const GROUP: &str = "Group";
    pub const LOOP: &str = "Loop";

    /// Every one of them.
    pub const ALL: &[&str] = &[GROUP, LOOP, super::boundary_types::LOOP_IN, super::boundary_types::LOOP_OUT, super::INCLUDED_GROUP_TYPE];

    pub fn is_own(node_type: &str) -> bool {
        ALL.contains(&node_type)
    }
}

// SYNC: GroupDefinition <-> packages/weft-graph/src/protocol.ts GroupDefinition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupDefinition {
    pub id: String,
    /// Whether this is a `Group` or a `Loop` (with its loop config).
    /// Required: the compiler always sets it explicitly (see
    /// `weft_compiler.rs::collect_group_definitions`). A snapshot
    /// that omits this field is corrupt, not legitimately legacy.
    #[serde(flatten)]
    pub kind: GroupKind,
    /// Optional user-facing label. Defaults to id if missing.
    pub label: Option<String>,
    /// External input ports (the ports outside the group connects to),
    /// each as full an input as a node's: the editor draws a value written
    /// on one with the widget enrich derives from its type.
    #[serde(rename = "inPorts", default)]
    pub in_ports: Vec<InputDefinition>,
    /// External output ports.
    #[serde(rename = "outPorts", default)]
    pub out_ports: Vec<PortDefinition>,
    /// The `*name: Type` ports of the header, each with the value the body
    /// writes ([`MetadataPort`]). An include's call site carries its body's.
    // SYNC: GroupDefinition.metadata_ports <-> packages/weft-graph/src/protocol.ts GroupDefinition.metadataPorts
    #[serde(rename = "metadataPorts", default, skip_serializing_if = "Vec::is_empty")]
    pub metadata_ports: Vec<MetadataPort>,
    /// Parent group id for nested groups. None for top-level groups.
    #[serde(rename = "parentGroupId", default)]
    pub parent_group_id: Option<String>,
    /// Ids of child groups (first-level only; nested groups carry
    /// their own entry with `parent_group_id` set).
    #[serde(rename = "childGroupIds", default)]
    pub child_group_ids: Vec<String>,
    /// Ids of member nodes (only direct children, not grandchildren).
    /// A loop's own `LoopIn`/`LoopOut` are not among them: they sit in
    /// the scope around the loop.
    #[serde(rename = "nodeIds", default)]
    pub node_ids: Vec<String>,
    /// True for the anonymous top-level group of an included `.weft` file
    /// (no `name =`). The editor labels it from the filename, validates it
    /// as a component (no project-level output requirement), and draws it
    /// open until a person closes it.
    #[serde(default)]
    pub anonymous: bool,
    #[serde(default)]
    pub span: Option<Span>,
    #[serde(default, rename = "headerSpan")]
    pub header_span: Option<Span>,
    /// The file the spans live in when the group came from a full-mode
    /// `@include` (same rule as `NodeDefinition::source_file`).
    #[serde(default, rename = "sourceFile", skip_serializing_if = "Option::is_none")]
    pub source_file: Option<String>,
    /// The group's description: the plain `# ...` comment that is the first
    /// body line of the group body (text without the `# `).
    #[serde(default)]
    pub description: Option<String>,
    /// Literals written on this container's interface ports: `g.x = "hi"`
    /// from outside, or `_should_flow: false` in the braces. A wired value
    /// is an ordinary edge and never appears here. What a member reads off
    /// a filled port reaches it over the wire whose source is the group
    /// itself ([`Edge::via`]), sent when the group opens. A loop's sit on
    /// its `LoopIn` instead, which delivers them like any node.
    // SYNC: port_literals <-> packages/weft-graph/src/protocol.ts GroupDefinition.portLiterals
    #[serde(default, rename = "portLiterals")]
    pub port_literals: std::collections::BTreeMap<String, Value>,
    /// File-backed `port_literals` entries, keyed by port: the same
    /// record a node keeps for a `@file` / `@asset` field
    /// ([`NodeDefinition::file_refs`]).
    // SYNC: file_refs <-> packages/weft-graph/src/protocol.ts GroupDefinition.fileRefs
    #[serde(default, rename = "fileRefs", skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub file_refs: std::collections::BTreeMap<String, FileRef>,
    /// Where each `port_literals` entry was written, and in which form
    /// (a braces field or a `g.x = ...` statement), so an edit rewrites
    /// the value where it already lives.
    // SYNC: port_literal_spans <-> packages/weft-graph/src/protocol.ts GroupDefinition.portLiteralSpans
    #[serde(default, rename = "portLiteralSpans")]
    pub port_literal_spans: std::collections::BTreeMap<String, ConfigFieldSpan>,
    /// The same record a node keeps ([`NodeDefinition::port_literal_types`]).
    #[serde(default, rename = "portLiteralTypes", skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub port_literal_types: std::collections::BTreeMap<String, WeftType>,
    /// `[T: Media]` written after `Group`: bounds on the type variables
    /// its ports use, each with what it resolved to once enrich has run
    /// (a loop's sit on its `LoopIn` and `LoopOut` instead).
    // SYNC: GroupDefinition.type_params <-> packages/weft-graph/src/protocol.ts GroupDefinition.typeParams
    #[serde(default, rename = "typeParams", skip_serializing_if = "Vec::is_empty")]
    pub type_params: Vec<crate::weft_type::TypeParam>,
    /// The port types this group has at each place it runs, when they
    /// differ from one place to another: a group inside an included file
    /// (the file's own body among them) whose type variables a site
    /// resolves its own way. The same record a node keeps
    /// ([`NodeDefinition::place_types`]), its inputs being the group's
    /// in ports and its outputs its out ports; [`Self::types_at`] is the
    /// one reading.
    // SYNC: GroupDefinition.place_types <-> packages/weft-graph/src/protocol.ts GroupDefinition.placeTypes
    #[serde(default, rename = "placeTypes", skip_serializing_if = "Vec::is_empty")]
    pub place_types: Vec<PlaceTypes>,
}

impl GroupDefinition {
    /// The group's port types at the place `path` reaches (the call path
    /// its members run under; a site's own, the path above it).
    pub fn types_at<S: AsRef<str>>(&self, path: &[S]) -> TypesAt<'_> {
        let place = self.place_types.iter().find(|p| {
            p.place.len() == path.len() && p.place.iter().zip(path).all(|(a, b)| a == b.as_ref())
        });
        TypesAt { own: Own::Group(self), place }
    }
}

/// Graph-level instance of a node.
///
/// Two kinds of fields:
/// - Authored: id, node_type, label, config, position, scope,
///   group_boundary. Written by the user or the AI.
/// - Enriched: inputs, outputs. Populated by the compiler's enrich
///   pass, which looks up the node type's metadata, resolves TypeVars
///   against connected edges, and materializes dynamic ports.
///
/// Before enrichment, `inputs` and `outputs` are empty. After, they
/// contain the concrete per-instance port shapes the scheduler uses.
/// Source byte range. 1-indexed lines, 0-indexed columns, end-exclusive.
/// Populated by the parser; used by tooling (VS Code extension, AI
/// streaming edits) to perform surgical text edits without re-serializing
/// the whole file. Missing (None) when the struct wasn't produced by the
/// parser (e.g. hand-constructed in tests).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Span {
    pub start_line: usize,
    pub start_column: usize,
    pub end_line: usize,
    pub end_column: usize,
}

impl Span {
    pub fn single_line(line: usize, start_column: usize, end_column: usize) -> Self {
        Self { start_line: line, start_column, end_line: line, end_column }
    }
}

/// Where a config field's value was written in source. The editor needs this
/// to rewrite a field in place: an inline field (`n = Type { k: v }`) is
/// rewritten as `k: v`, a connection-line field (`n.k = v`) keeps its
/// `n.k = ` prefix.
// SYNC: ConfigOrigin <-> packages/weft-graph/src/protocol.ts ConfigFieldSpan.origin
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ConfigOrigin {
    Inline,
    Connection,
}

/// Source range of one config field plus how it was written. The editor edits
/// a single field surgically using `span`, and uses `origin` to reconstruct
/// the correct line prefix.
// SYNC: ConfigFieldSpan <-> packages/weft-graph/src/protocol.ts ConfigFieldSpan
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigFieldSpan {
    pub span: Span,
    pub origin: ConfigOrigin,
    /// The file the span's coordinates live in, when a full-mode
    /// `@include` spliced it out of another file. None = the compiled
    /// source. Carried on the SPAN (not derived from the owning node)
    /// because a group holds spans from two files at once: a loop's
    /// knobs from the included file, its interface-port fills from the
    /// file that wrote `alias.port = value`.
    #[serde(default, rename = "sourceFile", skip_serializing_if = "Option::is_none")]
    pub source_file: Option<String>,
}

impl ConfigFieldSpan {
    pub fn inline(span: Span) -> Self {
        Self { span, origin: ConfigOrigin::Inline, source_file: None }
    }
    pub fn connection(span: Span) -> Self {
        Self { span, origin: ConfigOrigin::Connection, source_file: None }
    }
}

/// True for config keys owned by the compiler/editor rather than the
/// node: `_`-reserved per-instance keys (`_label`, `_tags`) and the
/// `parentId` boundary pointer merged in at flatten time. These
/// co-reside in `NodeDefinition.config` but are never node input data:
/// the input bag skips them so node bodies only ever see their own
/// inputs.
pub fn is_internal_config_key(key: &str) -> bool {
    key.starts_with('_') || key == "parentId"
}

// SYNC: NodeDefinition (the editor-visible subset; backend-only fields like
// `images` and `publishedService` stay here) <-> packages/weft-graph/src/protocol.ts NodeDefinition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeDefinition {
    pub id: String,
    #[serde(rename = "nodeType")]
    pub node_type: String,
    pub label: Option<String>,
    #[serde(default = "default_config")]
    pub config: Value,
    pub position: Position,
    /// Group nesting, outermost first. Empty for top-level.
    #[serde(default)]
    pub scope: Vec<String>,
    /// For a loop's `LoopIn` / `LoopOut`, which loop and which side.
    #[serde(default, rename = "groupBoundary")]
    pub group_boundary: Option<GroupBoundary>,
    /// The file this node was written in, when it is NOT the compiled
    /// source: a full-mode `@include` splices another file's nodes in
    /// with their own spans, and diagnostics on them must name that
    /// file. None = the compiled source itself.
    #[serde(default, rename = "sourceFile", skip_serializing_if = "Option::is_none")]
    pub source_file: Option<String>,
    /// Enriched inputs. Empty before compile.
    #[serde(default)]
    pub inputs: Vec<InputDefinition>,
    /// Enriched output ports. Empty before compile.
    #[serde(default)]
    pub outputs: Vec<PortDefinition>,
    /// Enriched node-level features (one_of_required, etc). Mirrored
    /// from NodeMetadata at compile time so the scheduler doesn't
    /// need a registry lookup per node.
    #[serde(default)]
    pub features: crate::node::NodeFeatures,
    /// The bounds on this node's type variables: its node type's
    /// `typeParams` and the ones the program wrote on it
    /// (`ExecPython[T: Media](...)`, or on the group a boundary halves,
    /// which both its boundaries carry). Each carries what its variable
    /// resolved to once enrich has run; validate holds the two together.
    // SYNC: type_params <-> packages/weft-graph/src/protocol.ts NodeDefinition.typeParams
    #[serde(default, rename = "typeParams", skip_serializing_if = "Vec::is_empty")]
    pub type_params: Vec<crate::weft_type::TypeParam>,
    /// The port types this node has at each place it runs, when they
    /// differ from one place to another: a node inside an included file
    /// whose type variables a site resolves its own way (`blur` included
    /// once for an Image, once for a Video). One entry per place; empty
    /// when every place agrees, the node's own port types being the types
    /// at all of them. [`Self::types_at`] is the one reading.
    // SYNC: NodeDefinition.place_types <-> packages/weft-graph/src/protocol.ts NodeDefinition.placeTypes
    #[serde(default, rename = "placeTypes", skip_serializing_if = "Vec::is_empty")]
    pub place_types: Vec<PlaceTypes>,
    /// `true` if this node implements `Node::provision_infra` and the
    /// dispatcher must run InfraSetup before activate. Mirrored from
    /// NodeMetadata.requires_infra at enrich time.
    #[serde(default, rename = "requiresInfra")]
    pub requires_infra: bool,
    /// Whether this node exists once per instance of the program, and why:
    /// `Marked` when the source says `@per_instance` in its braces (only an
    /// infra node may, see `NodeMetadata::per_instance_eligible`), `Filled`
    /// when one of its fields is written `@instance_filled`, `Derived` when
    /// it reads a value that comes from a per-instance node, found by the
    /// compiler along the wires. `None` for a node every instance shares. Written by the
    /// lowering (the mark) and the compiler's propagation (the rest), and
    /// only ever READ downstream: the runtime and the editor never
    /// recompute it.
    // SYNC: per_instance <-> packages/weft-graph/src/protocol.ts NodeDefinition.perInstance
    #[serde(default, rename = "perInstance", skip_serializing_if = "Option::is_none")]
    pub per_instance: Option<crate::instance::PerInstance>,
    /// Image source dirs built for this node. Mirrored from
    /// NodeMetadata.images at enrich time. A version build walks this to
    /// know which Dockerfiles to build (`weft-dispatcher::build`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub images: Vec<String>,
    /// What this TRIGGER wakes with: field name to weft type, mirrored
    /// from NodeMetadata.fires_with at enrich time. Carried on the
    /// definition so the engine can hold a firing to it without a
    /// catalog lookup, and so `weft run --fire` can print the shape it
    /// wanted from the compiled program alone. Empty on every node that
    /// is not a trigger, and on a trigger that declares nothing.
    #[serde(default, rename = "firesWith", skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub fires_with: std::collections::BTreeMap<String, String>,
    /// The outputs this INFRA node makes once, when its infra is applied,
    /// and a fire reads saved (`crate::infra::bake`): mirrored from the
    /// outputs its metadata marks `baked` at enrich time. Empty on every
    /// other node.
    #[serde(default, rename = "bakedOutputs", skip_serializing_if = "std::collections::BTreeSet::is_empty")]
    pub baked_outputs: std::collections::BTreeSet<String>,
    /// The recipe for the service this node publishes a connection to
    /// (`ctx.publish_access`), resolved from the catalog at enrich
    /// time from the node metadata's `publishes` name. Carried on the
    /// definition rather than looked up at run time: a worker only
    /// ships the node types its project uses, so the service's own
    /// access node is usually absent from it, while the compiler sees
    /// the whole catalog.
    #[serde(default, rename = "publishedService", skip_serializing_if = "Option::is_none")]
    pub published_service: Option<crate::access::spec::AccessSpec>,
    /// The recipe of the service this access node's connection field
    /// connects to, carried when that field is `@instance_filled`: an instance's
    /// connection is made through the instance door (a browser holding its
    /// token, never the editor), and that door has only the program to read
    /// the recipe from. It drives the instance's connect page exactly as the catalog
    /// entry drives the editor's.
    #[serde(default, rename = "instanceService", skip_serializing_if = "Option::is_none")]
    pub instance_service: Option<crate::access::spec::AccessSpec>,
    /// The node type's validation rules, carried when a field of this
    /// node is `@instance_filled`: an instance's value is checked against them
    /// with the value swapped in (`weft_core::rules`), where no catalog
    /// is at hand. Resolved at enrich time from the catalog.
    #[serde(default, rename = "instanceRules", skip_serializing_if = "Option::is_none")]
    pub instance_rules: Option<InstanceRules>,
    /// Full source range of the node declaration (including config
    /// block if present). Set by the parser.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub span: Option<Span>,
    /// Source range of the node's header (`id = NodeType`), the part
    /// before the `{` config block. Used when adding a config field
    /// to a bare node.
    #[serde(default, rename = "headerSpan", skip_serializing_if = "Option::is_none")]
    pub header_span: Option<Span>,
    /// Per-config-field source ranges, keyed by field name. Each range
    /// covers the `key: value` pair including trailing comma. Used to
    /// surgically edit one field without re-serializing the whole
    /// config block.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty", rename = "configSpans")]
    pub config_spans: std::collections::BTreeMap<String, ConfigFieldSpan>,
    /// Every constant written for an INPUT PORT, keyed by input name:
    /// the value behind `n.x = 5` (the statement form) or `M { x: 5 }`
    /// (the braces form), a `@file`/`@asset` marker included. Moved out
    /// of `config` by the enrich normalization the moment the full input
    /// list is known, so a constant has ONE home whichever spelling wrote
    /// it: the engine delivers it as a pulse on the port. `config` keeps
    /// only what is not a port (the `_`-reserved keys, a loop's knobs).
    // SYNC: port_literals <-> packages/weft-graph/src/protocol.ts NodeDefinition.portLiterals
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty", rename = "portLiterals")]
    pub port_literals: std::collections::BTreeMap<String, Value>,
    /// Config keys the source marked `?` (`answer?: draft.text`), which
    /// makes the input port that key CREATES optional: a closure on it
    /// no longer skips the node. Only meaningful on a node type that
    /// accepts created ports; enrich rejects the marker anywhere else,
    /// where it would silently mean nothing.
    #[serde(default, skip_serializing_if = "std::collections::BTreeSet::is_empty", rename = "optionalPorts")]
    pub optional_ports: std::collections::BTreeSet<String>,
    /// Source ranges + written form for `port_literals` entries, keyed
    /// by input name (the twin of `config_spans` for the other home).
    /// The `origin` is the form the value is WRITTEN in (`Connection` =
    /// statement form, `Inline` = braces form), which is what the
    /// editor's form-toggle rewrites.
    // SYNC: port_literal_spans <-> packages/weft-graph/src/protocol.ts NodeDefinition.portLiteralSpans
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty", rename = "portLiteralSpans")]
    pub port_literal_spans: std::collections::BTreeMap<String, ConfigFieldSpan>,
    /// The type of a `port_literals` entry when the program states it
    /// rather than the value showing it: a value a metadata wire wrote is
    /// of its metadata port's type (an `LlmTool`, never the record its
    /// JSON reads as), so a type variable the port carries binds to that
    /// type. Every other literal's type is read off the value.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty", rename = "portLiteralTypes")]
    pub port_literal_types: std::collections::BTreeMap<String, WeftType>,
    /// The metadata ports this node carries: its node type's, or on an
    /// included file's opaque node, the file's group's ([`MetadataPort`]).
    /// Filled by enrich from the catalog.
    // SYNC: NodeDefinition.metadata_ports <-> packages/weft-graph/src/protocol.ts NodeDefinition.metadataPorts
    #[serde(default, skip_serializing_if = "Vec::is_empty", rename = "metadataPorts")]
    pub metadata_ports: Vec<MetadataPort>,
    /// File-backed config fields, keyed by field name. Present when a field
    /// value came from `@file("path", Type)`. `config` holds the resolved
    /// value; this records the source reference so the editor renders the
    /// field as file-backed and routes edits to the referenced file instead
    /// of rewriting the `@file(...)` token in the source.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty", rename = "fileRefs")]
    pub file_refs: std::collections::BTreeMap<String, FileRef>,
    /// Set on an opaque `@include` interface node: the path of the included
    /// `.weft` file. The editor renders such a node as an expandable group
    /// that navigates into the file. Only present in interface-parse output.
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "includePath")]
    pub include_path: Option<String>,
    /// Set on an opaque `@include` interface node: what the file behind it
    /// holds, reached through its own includes too. The body is not in the
    /// graph in interface mode, so this is the only way the editor can tell
    /// that a project's only trigger (or only infra node) lives inside an
    /// include. Absent in full-mode output, where the real nodes are there
    /// to be counted.
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "includeContents")]
    pub include_contents: Option<IncludedContents>,
}

/// What an opaque `@include` node stands for: the roles its file plays,
/// and every file reached to find out. Carried on the include node rather
/// than folded into `requires_infra` / `features.is_trigger`, because
/// those two are per-node identities that drive real work (an infra node
/// gets a provisioned slot and a live row, a trigger gets a mount URL),
/// and the alias is neither of those things. It only contains them.
///
/// SYNC: IncludedContents <-> packages/weft-graph/src/protocol.ts IncludedContents
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct IncludedContents {
    /// Some node inside requires infrastructure, so the project needs its
    /// infra up before it can run even though no visible node says so.
    #[serde(default, rename = "requiresInfra", skip_serializing_if = "std::ops::Not::not")]
    pub requires_infra: bool,
    /// Some node inside is a trigger, so the project can be activated.
    #[serde(default, rename = "hasTrigger", skip_serializing_if = "std::ops::Not::not")]
    pub has_trigger: bool,
    /// Every `.weft` file reached through this include, nested ones
    /// included, each relative to the PROJECT ROOT. Root-relative and not
    /// as-written, because an `@include` path is relative to the file that
    /// wrote it: a nested one resolved against the top file's directory
    /// would point at nothing. The editor watches these so editing a
    /// deeply included file re-parses the graph that depends on it.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub files: Vec<String>,
    /// The types of the SHARED nodes found inside (a `@per_instance` node
    /// is its instances' to run, so it is left out), before the catalog was
    /// consulted. The parser has no catalog, so it records the types and
    /// enrich settles the two booleans above from them.
    #[serde(default, rename = "nodeTypes", skip_serializing_if = "Vec::is_empty")]
    pub node_types: Vec<String>,
}

/// Which directive wrote a file reference, and therefore its edit contract.
/// SYNC: FileMarker <-> packages/weft-graph/src/protocol.ts FileRef.marker
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FileMarker {
    /// `@file`: BIDIRECTIONAL. The referenced file's content is the field's
    /// value, and editing the field writes the new value back to the file.
    File,
    /// `@asset`: PULL-ONLY. The field's value comes from the referenced
    /// file/URL/stored file; nothing ever writes back to it. A file-typed
    /// value defers to the build's asset resolution; a text-typed value is
    /// read at parse like `@file`, but renders read-only.
    Asset,
}

impl FileMarker {
    /// The source-level directive that writes this marker, for error
    /// messages (`@file` / `@asset`).
    pub fn directive(&self) -> &'static str {
        match self {
            FileMarker::File => "@file",
            FileMarker::Asset => "@asset",
        }
    }
}

/// A `@file("path", Type)` / `@asset("path", Type)` reference attached to a
/// config field: where the value comes from, the type it carries, and which
/// directive (edit contract) declared it. A disk `path` is relative to the
/// project root (the compiled file's directory) whichever file wrote the
/// marker: a ref from an included file is respelled under the root by the
/// compiler, so the editor, the asset sync and the build all resolve it
/// against the same anchor. Serializes as
/// `{ "path": "...", "type": "String", "marker": "file" }`. Lives in
/// weft-core because it flows on the parse wire to the editor.
/// SYNC: FileRef <-> packages/weft-graph/src/protocol.ts FileRef
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FileRef {
    pub path: String,
    #[serde(rename = "type")]
    pub ty: WeftType,
    pub marker: FileMarker,
}

impl FileRef {
    /// What a build resolves this ref BY: the path AND the declared type.
    /// The type picks the marker kind and is what the bytes are held
    /// to, so one path declared `Audio` in one place and `Image` in
    /// another is two refs, checked and resolved separately (keyed by
    /// path alone, the second would ride the first's check and value).
    pub fn resolution_key(&self) -> String {
        // Scoped aliases may have the same display name but different bodies.
        // The portable spelling preserves the complete declared contract.
        format!("{}\u{0}{}", self.path, self.ty.wire_string())
    }
}

fn default_config() -> Value {
    Value::Object(Default::default())
}

impl NodeDefinition {
    /// The part of the program this node is, as a finding about it names
    /// it ([`Broken::parts`]): its own id, or for a loop's boundary node its
    /// loop's, since a cut keeps a loop whole or not at all and the
    /// boundary is the loop's own business.
    pub fn part(&self) -> &str {
        self.group_boundary.as_ref().map(|b| b.group_id.as_str()).unwrap_or(&self.id)
    }

    /// This node's port types at the place `path` reaches (an include
    /// call path, outermost site first; empty at the top).
    pub fn types_at<S: AsRef<str>>(&self, path: &[S]) -> TypesAt<'_> {
        TypesAt { own: Own::Node(self), place: self.place_of(path).map(|index| &self.place_types[index]) }
    }

    /// Which [`Self::place_types`] entry is the place `path` reaches,
    /// `None` when the node's own types are the types there.
    pub fn place_of<S: AsRef<str>>(&self, path: &[S]) -> Option<usize> {
        self.place_types.iter().position(|p| {
            p.place.len() == path.len() && p.place.iter().zip(path).all(|(a, b)| a == b.as_ref())
        })
    }

    /// The node's header span, or a default (zero) span when it has none
    /// (synthetic nodes carry no header). The single home for "point a
    /// diagnostic at this node's header line", so every diagnostic site uses
    /// the same fallback rather than repeating `header_span.unwrap_or_default()`.
    pub fn header_span_or_default(&self) -> Span {
        self.header_span.unwrap_or_default()
    }

    /// The constant written for `key`, wherever it lives right now: a
    /// port's constant sits in `port_literals` once enrich has homed it,
    /// and in `config` before that (or when the key is not a port at
    /// all, a loop's knob say). One lookup, so a reader that runs before
    /// and after the normalization reads the same value.
    pub fn written_value(&self, key: &str) -> Option<&Value> {
        self.port_literals.get(key).or_else(|| self.config.get(key))
    }

    /// The source entry of `written_value`'s home, same two-home rule.
    pub fn written_span(&self, key: &str) -> Option<&ConfigFieldSpan> {
        self.port_literal_spans.get(key).or_else(|| self.config_spans.get(key))
    }

    /// Tags from `_tags` config. Used for token-scoped enumeration
    /// (a token with `allowed_tags` only sees signals tagged with
    /// at least one of those tags). Validated at parse time:
    /// each tag matches `[A-Za-z0-9_-]{1,64}`.
    pub fn tags(&self) -> Vec<String> {
        self.config
            .get(crate::tag::TAGS_CONFIG_KEY)
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|t| t.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GroupBoundaryRole {
    In,
    Out,
}

/// THE derivation of a loop's `LoopIn` node id. The compiler mints
/// exactly this shape; the engine and the journal fold re-derive it.
/// One definition so the convention cannot drift.
// SYNC: loop_in_id, loop_out_id <-> packages/weft-graph/src/webview/host-bridge.ts LOOP_IN, LOOP_OUT
pub fn loop_in_id(loop_id: &str) -> String {
    format!("{loop_id}{LOOP_IN_SUFFIX}")
}

/// What [`loop_in_id`] puts after a loop's id.
const LOOP_IN_SUFFIX: &str = "__in";

/// THE derivation of a loop's `LoopOut` node id; see [`loop_in_id`].
pub fn loop_out_id(loop_id: &str) -> String {
    format!("{loop_id}{LOOP_OUT_SUFFIX}")
}

/// What [`loop_out_id`] puts after a loop's id.
const LOOP_OUT_SUFFIX: &str = "__out";

/// The loop a boundary's id or spelling names (`each__in`, `each__out`
/// gives `each`), when it has a boundary's shape: whether a loop of that
/// name exists is the caller's to check against what it holds.
pub fn boundary_loop(spelled: &str) -> Option<&str> {
    spelled.strip_suffix(LOOP_IN_SUFFIX).or_else(|| spelled.strip_suffix(LOOP_OUT_SUFFIX))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupBoundary {
    #[serde(rename = "groupId")]
    pub group_id: String,
    pub role: GroupBoundaryRole,
}

/// One INPUT on a NODE INSTANCE, enriched: TypeVars resolved, derived
/// inputs materialized, accepted drivers resolved, and the editor surface
/// (widget/default/label/placeholder) stamped from the metadata so the
/// editor never re-derives any of it. The instance twin of the
/// metadata's `InputSpec`; outputs use the slim [`PortDefinition`].
// SYNC: InputDefinition <-> packages/weft-graph/src/protocol.ts InputDefinition, packages/weft-connect/src/core/wire.ts InputDefinition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InputDefinition {
    /// The wire-port half (name, type, requiredness, declared spelling,
    /// carry flag), flattened onto the wire so the JSON shape is the
    /// same as the TS `InputDefinition extends PortDefinition`. The
    /// `Deref` impls below let readers keep writing `input.name`.
    #[serde(flatten)]
    pub port: PortDefinition,
    /// Which drivers this input takes (resolved: the compiler-read fixed
    /// rule, else the metadata's list, else the type's own answer).
    // SYNC: InputDefinition.accepts <-> packages/weft-graph/src/protocol.ts InputDefinition.accepts, packages/weft-connect/src/core/wire.ts InputDefinition.accepts
    #[serde(default = "Accepts::both")]
    pub accepts: Accepts,
    /// The node runs without this input even when it is wired and the
    /// wire closed (nothing arrived, or its source failed): the program
    /// wrote `x?: T` (or a key creating the port, `x?:`), or the compiler
    /// made the port to pass an absence on (a group boundary, a gate).
    /// An input that is neither `required` nor `optional` is the default
    /// for one the node's metadata calls optional: unwired the node runs
    /// without it, wired it is required. [`Self::is_required`] is the one
    /// reading.
    // SYNC: InputDefinition.optional <-> packages/weft-graph/src/protocol.ts InputDefinition.optional, packages/weft-connect/src/core/wire.ts InputDefinition.optional
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub optional: bool,
    /// A container's input that holds nothing: a group, an included file,
    /// or a loop on a port it neither iterates nor carries hands what
    /// reaches it straight to the nodes inside that read it. It is never
    /// required or optional itself (a `?` on it is refused); whether it
    /// must be connected is what those nodes need. Only a container's
    /// inputs (a group's, an include's opaque node's) set it.
    // SYNC: InputDefinition.passes_through <-> packages/weft-connect/src/core/wire.ts InputDefinition.passesThrough
    #[serde(default, rename = "passesThrough", skip_serializing_if = "std::ops::Not::not")]
    pub passes_through: bool,
    /// The input's effective editor widget (declared, else derived from
    /// the RESOLVED instance type after TypeVar substitution). Always
    /// present after enrich.
    // SYNC: InputDefinition.widget <-> packages/weft-graph/src/protocol.ts InputDefinition.widget, packages/weft-connect/src/core/wire.ts InputDefinition.widget
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub widget: Option<crate::node::Widget>,
    /// The input's declared default value, if any (mirrored from the
    /// metadata so the runtime and the editor read it off the node).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<Value>,
    /// Editor label override (mirrored from the metadata).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    /// Editor placeholder (mirrored from the metadata).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub placeholder: Option<String>,
    /// True when this input comes from the node TYPE's own spec (its
    /// metadata: `code` on ExecPython, `title` on a form node), false
    /// when it was added on this INSTANCE (a custom header port, a
    /// form-derived port, a carry ghost). The runtime uses it to hand
    /// node bodies their instance data ([`ValueBag::custom`]) without
    /// each node hardcoding its own setting names.
    // SYNC: InputDefinition.from_spec <-> packages/weft-graph/src/protocol.ts InputDefinition.fromSpec, packages/weft-connect/src/core/wire.ts InputDefinition.fromSpec
    #[serde(default, rename = "fromSpec", skip_serializing_if = "std::ops::Not::not")]
    pub from_spec: bool,
    /// The permissions THIS consumer needs on the wired connection
    /// (mirrored from the metadata's `requiresScopes`; only ever set on
    /// an Access-typed input). The runtime stamps them onto the access
    /// marker when this node's bag is built, so resolution can hold a
    /// verified connection to them; the editor reads them for its live
    /// shortfall check.
    // SYNC: InputDefinition.requires_scopes <-> packages/weft-graph/src/protocol.ts InputDefinition.requiresScopes, packages/weft-connect/src/core/wire.ts InputDefinition.requiresScopes
    #[serde(
        default,
        rename = "requiresScopes",
        skip_serializing_if = "Option::is_none"
    )]
    pub requires_scopes: Option<Vec<String>>,
    /// The stored VALUES this input needs on the wired connection (see
    /// [`crate::node::InputSpec::requires_values`]). Same three check
    /// points as the permissions above; the editor reads them for its
    /// live check.
    // SYNC: InputDefinition.requires_values <-> packages/weft-graph/src/protocol.ts InputDefinition.requiresValues, packages/weft-connect/src/core/wire.ts InputDefinition.requiresValues
    #[serde(
        default,
        rename = "requiresValues",
        skip_serializing_if = "Option::is_none"
    )]
    pub requires_values: Option<Vec<String>>,
}

/// A node's port types at one place it runs ([`NodeDefinition::place_types`]):
/// the call path that reaches it (each include site's id, outermost
/// first), and every port's type there.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PlaceTypes {
    pub place: Vec<String>,
    pub inputs: std::collections::BTreeMap<String, WeftType>,
    pub outputs: std::collections::BTreeMap<String, WeftType>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub type_params: Vec<crate::weft_type::TypeParam>,
}

/// One node's port types at one place: the node's own, or its
/// [`PlaceTypes`] entry for that place.
pub struct TypesAt<'a> {
    own: Own<'a>,
    place: Option<&'a PlaceTypes>,
}

/// Whose own types a [`TypesAt`] falls back on.
enum Own<'a> {
    Node(&'a NodeDefinition),
    Group(&'a GroupDefinition),
}

impl<'a> TypesAt<'a> {
    /// The type of input `port` here.
    pub fn input(&self, port: &str) -> Option<&'a WeftType> {
        match (self.place, &self.own) {
            (Some(place), _) => place.inputs.get(port),
            (None, Own::Node(node)) => node.inputs.iter().find(|p| p.name == port).map(|p| &p.port_type),
            (None, Own::Group(group)) => group.in_ports.iter().find(|p| p.name == port).map(|p| &p.port_type),
        }
    }

    /// The type of output `port` here.
    pub fn output(&self, port: &str) -> Option<&'a WeftType> {
        match (self.place, &self.own) {
            (Some(place), _) => place.outputs.get(port),
            (None, Own::Node(node)) => node.outputs.iter().find(|p| p.name == port).map(|p| &p.port_type),
            (None, Own::Group(group)) => group.out_ports.iter().find(|p| p.name == port).map(|p| &p.port_type),
        }
    }

    /// The type parameters here, each with what it resolved to.
    pub fn type_params(&self) -> &'a [crate::weft_type::TypeParam] {
        match (self.place, &self.own) {
            (Some(place), _) => &place.type_params,
            (None, Own::Node(node)) => &node.type_params,
            (None, Own::Group(group)) => &group.type_params,
        }
    }
}

impl PortDefinition {
    /// Whether the header that declares this port, on `side`, wrote no
    /// type for it (`(x)`), or one that did not parse: its type is then
    /// its own variable ([`WeftType::port_var`]), taken from what is wired.
    pub fn is_untyped(&self, side: crate::weft_type::PortSide) -> bool {
        self.port_type == WeftType::port_var(side, &self.name)
    }

    /// Whether this port carries a stream (`Generator[T]`). THE one
    /// reading of a port's type for that question; the node-level
    /// readers in `run::ready` are built on it.
    pub fn is_generator(&self) -> bool {
        self.port_type.as_generator().is_some()
    }
}

impl InputDefinition {
    /// Whether a closed value on this input skips the node, given
    /// whether a wire feeds it where the node fires: always for a
    /// required input, never for an `optional` one, and otherwise only
    /// when wired.
    pub fn is_required(&self, wired: bool) -> bool {
        self.required || (wired && !self.optional)
    }

    /// An input for a pure WIRE port (a side of a loop's boundary, a
    /// source-declared custom port): drivers from the type, no editor
    /// surface beyond what enrich later stamps. Not `optional`: a port
    /// that runs without a closed wire says so with [`Self::with_optional`].
    pub fn from_wire_port(port: PortDefinition) -> Self {
        Self {
            accepts: Accepts::for_type(&port.port_type),
            port,
            optional: false,
            passes_through: false,
            widget: None,
            default: None,
            label: None,
            placeholder: None,
            from_spec: false,
            requires_scopes: None,
            requires_values: None,
        }
    }

}

impl InputDefinition {
    /// This input, marked to run without a value even when wired and the
    /// wire closed when `optional` (see [`Self::optional`]).
    pub fn with_optional(mut self, optional: bool) -> Self {
        self.optional = optional;
        self
    }

    /// This input, marked as a container's port that hands its value on
    /// when `passes_through` (see [`Self::passes_through`]).
    pub fn with_passes_through(mut self, passes_through: bool) -> Self {
        self.passes_through = passes_through;
        self
    }
}

impl std::ops::Deref for InputDefinition {
    type Target = PortDefinition;
    fn deref(&self) -> &PortDefinition {
        &self.port
    }
}

impl std::ops::DerefMut for InputDefinition {
    fn deref_mut(&mut self) -> &mut PortDefinition {
        &mut self.port
    }
}

/// A pure WIRE port on a node's OUTPUT side or a group/loop
/// interface: a named, typed dock for edges. Inputs are the richer
/// [`InputDefinition`].
// SYNC: PortDefinition <-> packages/weft-graph/src/protocol.ts PortDefinition, packages/weft-connect/src/core/wire.ts PortDefinition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortDefinition {
    pub name: String,
    #[serde(rename = "portType")]
    pub port_type: WeftType,
    /// Whether the node waits for a value here (a closed pulse on a
    /// required input skips it). Meaningful on inputs only: an output
    /// carries no optionality and is always `true` here (a firing that
    /// emits nothing on it closes it, whatever this says).
    pub required: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// True for the auto-synthesized side of a loop carry port (mirrored
    /// on the loop group's interface). The editor renders it as a
    /// non-editable ghost of the carry output of the same name.
    #[serde(default, rename = "synthesizedFromCarry", skip_serializing_if = "std::ops::Not::not")]
    pub synthesized_from_carry: bool,
    /// The VERBATIM type annotation the SOURCE header declares for this
    /// port (never a re-print of the parsed type, so a registry alias or
    /// even an unparseable typo round-trips as the author typed it);
    /// None when the header does not declare it (a catalog port, a
    /// config-derived one, a synthesized one). The editor rewrites the
    /// header from this, never from `port_type`: the rendered type may
    /// be an inference-resolved instantiation of a generic, which must
    /// not get frozen into source as if the author wrote it.
    // SYNC: PortDefinition.declared_type <-> packages/weft-graph/src/protocol.ts PortDefinition.declaredType, packages/weft-connect/src/core/wire.ts PortDefinition.declaredType
    #[serde(default, rename = "declaredType", skip_serializing_if = "Option::is_none")]
    pub declared_type: Option<String>,
}

/// A port whose value is written when the program is written, never
/// computed by a run: a tool's description on the group that performs
/// the tool. It sits on the output side of a node or a group, and a wire
/// leaving it is a metadata wire: the compiler writes the value into the
/// input the wire points at, exactly as if it had been typed there, and
/// the wire never runs ([`ProjectDefinition::metadata_wires`]).
///
/// One type everywhere: a node type declares its own in `metadata.json`
/// (`metadataPorts`, the value written there), a group or a loop in its
/// header (`-> (*tool: LlmTool)`, the value written in its body with
/// `self.tool = { ... }`), and a node instance carries its type's.
// SYNC: MetadataPort <-> packages/weft-connect/src/core/wire.ts MetadataPort
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MetadataPort {
    pub name: String,
    #[serde(rename = "type")]
    pub port_type: WeftType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// The value. A node type always writes it; a group's is `None` until
    /// enrich works out what its body writes (a literal, or other metadata
    /// ports gathered, [`Self::written`]), and one with nothing written is
    /// an error naming the line to add. An included file's opaque node (the
    /// editor's view of an `@include`) takes the file's from the file
    /// compiled whole.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<Value>,
    /// The value as a group's body writes it when part of it comes from
    /// other metadata ports (`self.tools = [...screen.tools, pause.tool]`),
    /// each reference already scoped to the id it names. Enrich works the
    /// value out from it. `None` on a value written out in full, and on a
    /// node type's.
    // SYNC: MetadataPort.written <-> packages/weft-connect/src/core/wire.ts MetadataPort.written
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub written: Option<MetadataExpr>,
    /// Where a group's body writes the value (`self.tool = ...`), in the
    /// group's file, so an edit rewrites it in place. `None` on a node's.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub span: Option<Span>,
    /// The type as a group's header spells it (the same contract as
    /// [`PortDefinition::declared_type`]), which a header rewrite writes
    /// back. `None` on a node's.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub declared_type: Option<String>,
}

/// A metadata port's value written from other metadata ports
/// ([`MetadataPort::written`]): JSON in which a name stands for the value
/// of the metadata port it names, and `...name` inside a list stands for
/// that list's items.
// SYNC: MetadataExpr <-> packages/weft-connect/src/core/wire.ts MetadataExpr
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum MetadataExpr {
    /// A JSON value written out in full.
    Literal { value: Value },
    /// The value of another node's or group's metadata port, with keys
    /// read off it (`act.tools.anthropic` is port `tools` of `act`, read
    /// at `anthropic`). `source` is the scoped id.
    Port {
        source: String,
        port: String,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        path: Vec<String>,
    },
    List { items: Vec<MetadataItem> },
    Object { fields: Vec<MetadataField> },
}

/// One item of a [`MetadataExpr::List`]: a value, or with `spread` the
/// items of a list (`...screen.tools`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MetadataItem {
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub spread: bool,
    pub value: MetadataExpr,
}

/// One key of a [`MetadataExpr::Object`], in written order.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MetadataField {
    pub key: String,
    pub value: MetadataExpr,
}

impl MetadataExpr {
    /// Every metadata port the expression names, in written order.
    pub fn ports(&self) -> Vec<(&str, &str, &[String])> {
        let mut out = Vec::new();
        self.collect_ports(&mut out);
        out
    }

    fn collect_ports<'e>(&'e self, out: &mut Vec<(&'e str, &'e str, &'e [String])>) {
        match self {
            MetadataExpr::Literal { .. } => {}
            MetadataExpr::Port { source, port, path } => out.push((source, port, path)),
            MetadataExpr::List { items } => items.iter().for_each(|item| item.value.collect_ports(out)),
            MetadataExpr::Object { fields } => fields.iter().for_each(|field| field.value.collect_ports(out)),
        }
    }

    /// Every source id the expression names, rewritten by `scope`.
    pub fn rescope(&mut self, scope: &mut impl FnMut(&str) -> String) {
        match self {
            MetadataExpr::Literal { .. } => {}
            MetadataExpr::Port { source, .. } => *source = scope(source),
            MetadataExpr::List { items } => items.iter_mut().for_each(|item| item.value.rescope(scope)),
            MetadataExpr::Object { fields } => fields.iter_mut().for_each(|field| field.value.rescope(scope)),
        }
    }

    /// The value, each named port's value read by `port`; `Err` names
    /// what does not fit (a `...` on a value that is no list).
    pub fn evaluate<E>(&self, port: &mut impl FnMut(&str, &str, &[String]) -> Result<Value, E>, spread_not_a_list: impl Fn(&MetadataExpr, &Value) -> E + Copy) -> Result<Value, E> {
        Ok(match self {
            MetadataExpr::Literal { value } => value.clone(),
            MetadataExpr::Port { source, port: name, path } => port(source, name, path)?,
            MetadataExpr::List { items } => {
                let mut out = Vec::new();
                for item in items {
                    let value = item.value.evaluate(port, spread_not_a_list)?;
                    match (item.spread, value) {
                        (true, Value::Array(values)) => out.extend(values),
                        (true, other) => return Err(spread_not_a_list(&item.value, &other)),
                        (false, value) => out.push(value),
                    }
                }
                Value::Array(out)
            }
            MetadataExpr::Object { fields } => {
                let mut out = serde_json::Map::new();
                for field in fields {
                    out.insert(field.key.clone(), field.value.evaluate(port, spread_not_a_list)?);
                }
                Value::Object(out)
            }
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    pub x: f64,
    pub y: f64,
}

/// One wire, end to end: from the node that produces a value to the node
/// that reads it, however many group ports it passes on the way (each
/// named in [`Self::via`]). Either end can also be a group itself:
///
/// - a wire whose TARGET is a group, on `_should_flow` or
///   `_should_not_flow`, is that group's gate;
/// - a wire whose SOURCE is a group starts at one of its ports
///   (`via[0]` is that group's own crossing): the literal written on an
///   input port, or nothing at all (an input nobody feeds, an output no
///   member writes), sent when the group opens;
/// - a wire whose TARGET is a group on any other port ends there (the
///   last crossing is that group's): a port nobody on the other side
///   reads. It carries nothing at run time; it is there because a
///   person wrote it, so the editor draws it and the checks see it.
// SYNC: Edge <-> packages/weft-graph/src/protocol.ts Edge
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Edge {
    pub id: String,
    pub source: String,
    pub target: String,
    #[serde(rename = "sourceHandle")]
    pub source_handle: Option<String>,
    #[serde(rename = "targetHandle")]
    pub target_handle: Option<String>,
    /// The keys read off the source value before it lands, in order
    /// (`w.seconds = x.profile.wpm` carries `["wpm"]` off port
    /// `profile`). Empty for a plain wire. Each key names a field of
    /// the record type at that level (`deref::walk_path` checks it at
    /// compile time), and the projection happens right before delivery,
    /// once per wire (`deref::project_value`): five wires off one port
    /// are five independent projections. On a wire through group ports
    /// it is every leg's keys in order (each leg's own are on its
    /// [`Crossing`]).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub path: Vec<String>,
    /// The group ports the wire passes, from its source to its target.
    /// Entering a call site pushes the site's call frame and leaving one
    /// pops it (`ProgramIndex::frames_across`); a group's
    /// ports change nothing on the way, and say which groups the value
    /// enters (it can open them) and leaves (they close it when they do
    /// not run). Empty for a wire between two nodes of one scope.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub via: Vec<Crossing>,
    /// Source range of the last leg's connection line (`target.port =
    /// source.port`, or `target.port = self.x`). Used by tooling to
    /// remove or rewrite the edge.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub span: Option<Span>,
    /// The file the span lives in when the edge came from a full-mode
    /// `@include` (same rule as `NodeDefinition::source_file`).
    #[serde(default, rename = "sourceFile", skip_serializing_if = "Option::is_none")]
    pub source_file: Option<String>,
}

/// One group port a wire passes: the group, the port, and which way. The
/// leg of the wire that ENDS at this port (the line a person wrote,
/// `g.x = a.out` for the leg into `g.x`) keeps its own key path and
/// source range here, so the editor can draw and edit each leg.
// SYNC: Crossing <-> packages/weft-graph/src/protocol.ts Crossing
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Crossing {
    pub group: String,
    pub port: String,
    pub side: CrossingSide,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub path: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub span: Option<Span>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_file: Option<String>,
}

/// Into a group through one of its inputs, or out of it through one of
/// its outputs.
// SYNC: CrossingSide <-> packages/weft-graph/src/protocol.ts CrossingSide
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CrossingSide {
    In,
    Out,
}

impl Edge {
    /// The input port the wire lands on.
    pub fn target_port(&self) -> &str {
        self.target_handle.as_deref().unwrap_or("default")
    }

    /// The output port the wire leaves from.
    pub fn source_port(&self) -> &str {
        self.source_handle.as_deref().unwrap_or("default")
    }

    /// The keys the wire reads off the value after crossing `via[k]`: the
    /// legs past that port.
    pub fn path_after(&self, k: usize) -> Vec<String> {
        let before: usize = self.via.iter().take(k + 1).map(|crossing| crossing.path.len()).sum();
        self.path.get(before..).map(<[String]>::to_vec).unwrap_or_default()
    }

    /// Whether the wire starts at a group's port rather than at a node:
    /// its first crossing is its source's own.
    pub fn from_group_port(&self) -> bool {
        self.via.first().is_some_and(|crossing| crossing.group == self.source)
    }

    /// Whether the wire stops at a group's port rather than at a node:
    /// its last crossing is its target's own.
    pub fn to_group_port(&self) -> bool {
        self.via.last().is_some_and(|crossing| crossing.group == self.target)
    }

    /// Whether the wire is a group's gate: it lands on a group's
    /// `_should_flow` or `_should_not_flow`.
    pub fn is_group_gate(&self, project: &ProjectDefinition) -> bool {
        crate::run::skip::is_gate_port(self.target_port()) && !self.to_group_port()
            && project.groups.iter().any(|group| group.id == self.target)
    }

    /// The call sites the wire crosses, in the order it crosses them:
    /// entering one pushes its call frame onto what the value carries,
    /// leaving one pops it.
    pub fn site_crossings<'a>(&'a self, g: &'a impl graph::GraphView) -> impl DoubleEndedIterator<Item = (CrossingSide, &'a str)> + 'a {
        self.via.iter()
            .filter(move |crossing| g.is_site(&crossing.group))
            .map(|crossing| (crossing.side, crossing.group.as_str()))
    }

    /// Carry the call stack `stack` across `crossings` (this wire's
    /// [`Self::site_crossings`]), walking the wire `direction`: downstream
    /// an entered site pushes and a left one pops, upstream the other way
    /// round, each in the order the walk meets them. A pop takes only the
    /// site on top (`site_of` reads a stack entry's site, `None` for one
    /// that is no call); `false` when another is there: the wire is
    /// another call's. A wire that enters a site and leaves it again
    /// (through a body that only passes a value on) leaves the stack as it
    /// found it.
    pub fn carry_calls<'s, T>(
        crossings: impl DoubleEndedIterator<Item = (CrossingSide, &'s str)>,
        direction: places::Direction,
        stack: &mut Vec<T>,
        site_of: impl Fn(&T) -> Option<&str>,
        frame: impl Fn(&str) -> T,
    ) -> bool {
        let steps: Vec<(CrossingSide, &str)> = match direction {
            places::Direction::Downstream => crossings.collect(),
            places::Direction::Upstream => crossings.rev().map(|(side, site)| (match side {
                CrossingSide::In => CrossingSide::Out,
                CrossingSide::Out => CrossingSide::In,
            }, site)).collect(),
        };
        for (side, site) in steps {
            match side {
                CrossingSide::In => stack.push(frame(site)),
                CrossingSide::Out => {
                    if stack.last().and_then(&site_of) != Some(site) { return false; }
                    stack.pop();
                }
            }
        }
        true
    }
}

/// What a run reads of its program on every turn, worked out once per
/// program: its wires by node, each node by id, the inputs each node
/// reads as a stream, its scopes ([`scopes::ScopeIndex`]), the call
/// frames each wire pushes and pops, and the shape each trigger fires
/// with. Wires answer for a PLACE (a node under the call frames it fires
/// at): a body's member is wired once per site that calls it, and the
/// frames pick which of those wires is there.
#[derive(Clone)]
pub struct ProgramIndex {
    /// The wires that carry something, by source and by target: a wire
    /// that stops at a group port nobody reads carries nothing.
    outgoing: std::collections::HashMap<String, Vec<usize>>,
    incoming: std::collections::HashMap<String, Vec<usize>>,
    /// Per wire that crosses a call site, by wire id: the sites it
    /// crosses, in order ([`Edge::site_crossings`]).
    calls: std::collections::HashMap<String, Vec<(CrossingSide, String)>>,
    /// Each node's position in `project.nodes`, by id.
    nodes: std::collections::HashMap<String, usize>,
    /// Per node, by position: the inputs it reads as a stream
    /// ([`crate::run::ready::generator_inputs`]).
    stream_inputs: Vec<Vec<String>>,
    /// The positions of the nodes that read a stream.
    stream_consumers: Vec<usize>,
    scopes: scopes::ScopeIndex,
    /// The places a cut runs under each site at the top of the call tree
    /// that runs only some of its body ([`GroupKind::Call`]'s `runs`).
    runs: std::collections::HashMap<String, std::collections::BTreeSet<Located>>,
    /// The places that stand in for their node, and the input ports
    /// handed a value per place ([`ProjectDefinition::given`]): no wire
    /// lands on either.
    stand_ins: std::collections::HashSet<Located>,
    handed: std::collections::HashMap<Located, std::collections::BTreeSet<String>>,
    /// Per node, by position: the shape its `firesWith` declares
    /// ([`crate::node::fire_payload_type`]), or why it is no shape.
    fire_payload: Vec<Result<Option<crate::weft_type::WeftType>, String>>,
}

impl ProgramIndex {
    pub fn build(project: &ProjectDefinition) -> Self {
        let mut outgoing: std::collections::HashMap<String, Vec<usize>> = std::collections::HashMap::new();
        let mut incoming: std::collections::HashMap<String, Vec<usize>> = std::collections::HashMap::new();
        let mut calls = std::collections::HashMap::new();
        for (i, edge) in project.edges.iter().enumerate() {
            if edge.to_group_port() {
                continue;
            }
            outgoing.entry(edge.source.clone()).or_default().push(i);
            incoming.entry(edge.target.clone()).or_default().push(i);
            let steps: Vec<(CrossingSide, String)> = edge.site_crossings(project).map(|(side, site)| (side, site.to_string())).collect();
            if !steps.is_empty() {
                calls.insert(edge.id.clone(), steps);
            }
        }
        let nodes = project.nodes.iter().enumerate().map(|(at, node)| (node.id.clone(), at)).collect();
        let stream_inputs: Vec<Vec<String>> = project
            .nodes
            .iter()
            .map(|node| crate::run::ready::generator_inputs(node).into_iter().map(str::to_string).collect())
            .collect();
        let stream_consumers = stream_inputs.iter().enumerate().filter(|(_, ports)| !ports.is_empty()).map(|(at, _)| at).collect();
        let runs = project.groups.iter().filter_map(|group| match &group.kind {
            GroupKind::Call { runs: Some(runs), .. } => Some((group.id.clone(), runs.clone())),
            _ => None,
        }).collect();
        let fire_payload = project.nodes.iter().map(|node| crate::node::fire_payload_type(&node.fires_with)).collect();
        let stand_ins = project.given.outputs.iter().cloned().collect();
        let handed = project.given.inputs.iter().map(|(place, ports)| (place.clone(), ports.clone())).collect();
        Self {
            outgoing, incoming, calls, nodes, stream_inputs, stream_consumers,
            scopes: scopes::ScopeIndex::build(project), runs, stand_ins, handed, fire_payload,
        }
    }

    /// The program's scopes.
    pub fn scopes(&self) -> &scopes::ScopeIndex {
        &self.scopes
    }

    /// Whether the node `node_id` exists at `frames`: every node does,
    /// except under a site a cut runs only part of the body of
    /// ([`GroupKind::Call`]'s `runs`), where only the places it lists do.
    pub fn admits(&self, node_id: &str, frames: &LoopFrames) -> bool {
        let path = crate::frames::call_path(frames);
        let Some((site, below)) = path.split_first() else { return true };
        self.runs.get(*site).is_none_or(|runs| {
            runs.contains(&Located::new(node_id, below.iter().map(|s| s.to_string()).collect()))
        })
    }

    /// Whether the node at `frames` stands in for itself rather than run
    /// ([`ProjectDefinition::given`]).
    pub fn stands_in(&self, node_id: &str, frames: &LoopFrames) -> bool {
        !self.stand_ins.is_empty() && self.stand_ins.contains(&Located::at(node_id, frames))
    }

    /// The input ports of the node at `frames` handed a value.
    pub fn handed_ports(&self, node_id: &str, frames: &LoopFrames) -> Option<&std::collections::BTreeSet<String>> {
        if self.handed.is_empty() {
            return None;
        }
        self.handed.get(&Located::at(node_id, frames))
    }

    /// Whether `port` of the node at `frames` was handed a value.
    pub fn is_handed(&self, node_id: &str, frames: &LoopFrames, port: &str) -> bool {
        !self.handed.is_empty() && self.handed.get(&Located::at(node_id, frames)).is_some_and(|ports| ports.contains(port))
    }

    /// Whether a wire landing on `port` of `target` at `frames` delivers:
    /// the program runs that place (no wire lands on a node that stands in
    /// for itself, nor on a port handed a value). A group's gate always
    /// takes its wire.
    fn lands(&self, target: &str, port: &str, frames: &LoopFrames) -> bool {
        if !self.nodes.contains_key(target) {
            return true;
        }
        self.admits(target, frames) && !self.stands_in(target, frames) && !self.is_handed(target, frames, port)
    }

    /// The position of the node `id` in `project.nodes`.
    pub fn position(&self, id: &str) -> Option<usize> {
        self.nodes.get(id).copied()
    }

    /// The node whose id is `id`.
    pub fn node<'a>(&self, project: &'a ProjectDefinition, id: &str) -> Option<&'a NodeDefinition> {
        self.nodes.get(id).map(|&at| &project.nodes[at])
    }

    /// The inputs of the node `id` that read a stream; none for a node
    /// that reads none, or one the program does not have.
    pub fn stream_inputs(&self, id: &str) -> &[String] {
        self.nodes.get(id).map_or(&[], |&at| &self.stream_inputs[at])
    }

    /// The nodes that read a stream, with the inputs each reads one on.
    pub fn stream_consumers<'a>(&'a self, project: &'a ProjectDefinition) -> impl Iterator<Item = (&'a NodeDefinition, &'a [String])> {
        self.stream_consumers.iter().map(move |&at| (&project.nodes[at], self.stream_inputs[at].as_slice()))
    }

    /// The shape the trigger `id` fires with: `None` when it declares
    /// none (or the program has no such node), `Err` naming why its
    /// declaration is no shape.
    pub fn fire_payload(&self, id: &str) -> Result<Option<&crate::weft_type::WeftType>, &str> {
        match self.nodes.get(id).map(|&at| &self.fire_payload[at]) {
            None | Some(Ok(None)) => Ok(None),
            Some(Ok(Some(shape))) => Ok(Some(shape)),
            Some(Err(why)) => Err(why),
        }
    }

    /// The frames a value carries across `edge`, given the frames its
    /// source sent it at, or `None` when the wire is not taken from
    /// there. Every wire carries the source's frames unchanged, except
    /// one that crosses a call site: leaving a site pops its call frame,
    /// and only the site the top frame names takes the wire (a body's
    /// member is wired out to every site that calls it; the frame says
    /// whose result this is); entering one pushes it (the shared body
    /// fires one frame deeper). A wire whose far end the program does not
    /// run at the frames it lands at is not taken either. THE one place
    /// frames change on a wire, shared by values and closures, so the
    /// live engine and the journal fold agree. A wire is not taken onto a
    /// place that stands in for its node, nor onto a port handed a value
    /// there ([`ProjectDefinition::given`]).
    pub fn frames_across(&self, edge: &Edge, frames: &LoopFrames) -> Option<LoopFrames> {
        let out = self.carry_frames(edge, frames, places::Direction::Downstream)?;
        self.lands(&edge.target, edge.target_port(), &out).then_some(out)
    }

    /// `frames` carried across the call sites `edge` crosses, walking it
    /// `direction` ([`Edge::carry_calls`]); `None` when the wire is
    /// another call's.
    fn carry_frames(&self, edge: &Edge, frames: &LoopFrames, direction: places::Direction) -> Option<LoopFrames> {
        let mut out = frames.clone();
        let Some(steps) = self.calls.get(&edge.id) else { return Some(out) };
        Edge::carry_calls(
            steps.iter().map(|(side, site)| (*side, site.as_str())),
            direction,
            &mut out,
            |frame| match frame { crate::frames::Frame::Call { site } => Some(site.as_str()), _ => None },
            |site| crate::frames::Frame::Call { site: site.to_string() },
        ).then_some(out)
    }

    /// The wires out of `source` (a node, or a group sending from one of
    /// its ports) taken from `frames`.
    pub fn get_outgoing<'a>(&self, project: &'a ProjectDefinition, source: &str, frames: &LoopFrames) -> Vec<&'a Edge> {
        self.outgoing.get(source)
            .map(|indices| indices.iter().map(|&i| &project.edges[i])
                .filter(|edge| self.frames_across(edge, frames).is_some()).collect())
            .unwrap_or_default()
    }

    /// The wires into `target` (a node, or a group's gate) that land at
    /// `frames`: a wire entering call sites lands only under those calls,
    /// and comes from a source the program runs.
    pub fn get_incoming<'a>(&self, project: &'a ProjectDefinition, target: &str, frames: &LoopFrames) -> Vec<&'a Edge> {
        self.incoming.get(target)
            .map(|indices| indices.iter().map(|&i| &project.edges[i])
                .filter(|edge| self.source_frames(edge, frames).is_some()).collect())
            .unwrap_or_default()
    }

    /// The frames the source of `edge` sends at, for the wire to land at
    /// `frames`; `None` when it cannot land there. The inverse of
    /// [`Self::frames_across`].
    pub fn source_frames(&self, edge: &Edge, frames: &LoopFrames) -> Option<LoopFrames> {
        if !self.lands(&edge.target, edge.target_port(), frames) {
            return None;
        }
        let out = self.carry_frames(edge, frames, places::Direction::Upstream)?;
        let sends = self.nodes.contains_key(&edge.source);
        (!sends || self.admits(&edge.source, &out)).then_some(out)
    }
}

/// Whether this project declares ANY infrastructure: true iff at least
/// one node has `requires_infra`: what a project's status reports and its
/// infra verbs are offered on. Pure walk over the node list; the one copy
/// of this predicate so the dispatcher and any other consumer can't drift
/// on what "has infra" means.
pub fn has_infra(project: &ProjectDefinition) -> bool {
    project.nodes.iter().any(|n| n.requires_infra)
}

#[cfg(test)]
mod address_tests {
    use super::*;

    #[test]
    fn an_empty_spelling_is_a_miss_and_never_a_panic() {
        // This takes whatever a person or a URL hands it (`--from`, a
        // path segment), and it used to pop an empty call path. What it
        // answers is the empty name, which no node answers to, so every
        // caller reads it as the miss it is.
        let project = ProjectDefinition {
            id: Uuid::nil(),
            nodes: vec![],
            edges: vec![],
            groups: vec![],
            created_at: Utc::now(),
            updated_at: Utc::now(),
            defaults: Default::default(),
            given: Default::default(),
            broken: Vec::new(),
            metadata_wires: Vec::new(),
        };
        let (node, path) = resolve_address(&project, "");
        assert_eq!(node, "");
        assert!(path.is_empty());
        assert!(!project.nodes.iter().any(|n| n.id == node), "no node answers to it");
    }

    /// A spelling with an empty segment is no address, and it comes back
    /// as the miss it is rather than as the nearest thing it resembles:
    /// `auth.` used to resolve to the site `auth` itself.
    #[test]
    fn an_empty_segment_is_a_miss_not_the_nearest_address() {
        let p = program();
        for malformed in ["auth.", ".check", "auth..check", "."] {
            assert_eq!(resolve_address(&p, malformed), (malformed.to_string(), vec![]), "{malformed}");
        }
    }

    fn program() -> ProjectDefinition {
        serde_json::from_value(serde_json::json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "nodes": [],
            "edges": [],
            "groups": [
                {"id": "auth", "kind": "call", "body": "Auth", "nodeIds": []},
                {"id": "Auth", "kind": "body", "nodeIds": ["Auth.check"]},
                {"id": "Auth.billing", "kind": "group", "nodeIds": [], "parentGroupId": "Auth"},
                {"id": "Auth.billing.inner", "kind": "call", "body": "Inner", "nodeIds": [], "parentGroupId": "Auth.billing"},
                {"id": "Auth.each", "kind": "loop", "loopConfig": {}, "nodeIds": [], "parentGroupId": "Auth"},
                {"id": "Inner", "kind": "body", "nodeIds": ["Inner.deep"]}
            ]
        })).unwrap()
    }

    /// With no program at hand, an id reads as the file's own name
    /// and the rest; a name somebody wrote stays as it is.
    #[test]
    fn a_compiled_id_reads_to_a_person_without_its_path() {
        assert_eq!(plain_id("@src:lib:setup.store"), "setup.store");
        assert_eq!(plain_id("@src:setup"), "setup");
        assert_eq!(plain_id("@src:cards.rows.read"), "cards.rows.read");
        assert_eq!(plain_id("outer.inner"), "outer.inner");
        assert_eq!(plain_id("plain"), "plain");
    }

    #[test]
    fn an_address_walks_the_call_sites_the_way_the_source_reads() {
        let p = program();
        assert_eq!(resolve_address(&p, "plain"), ("plain".into(), vec![]));
        assert_eq!(resolve_address(&p, "auth.check"), ("Auth.check".into(), vec!["auth".into()]));
        assert_eq!(resolve_address(&p, "auth"), ("auth".into(), vec![]));
        assert_eq!(resolve_address(&p, "auth.billing.inner"), ("Auth.billing.inner".into(), vec!["auth".into()]));
        assert_eq!(
            resolve_address(&p, "auth.billing.inner.deep"),
            ("Inner.deep".into(), vec!["auth".into(), "Auth.billing.inner".into()])
        );
        // A body's own id addresses every call of it.
        assert_eq!(resolve_address(&p, "Auth.check"), ("Auth.check".into(), vec![]));
        // A loop's boundary reads as its loop; a body as its site.
        assert_eq!(address_of(&p, "Auth.each__in", &["auth".into()]), "auth.each");
        assert_eq!(address_of(&p, "Auth.each__out", &["auth".into()]), "auth.each");
        assert_eq!(group_address(&p, "Auth", &["auth".into()]), "auth");
        assert_eq!(group_address(&p, "Auth.billing", &["auth".into()]), "auth.billing");
        assert_eq!(group_address(&p, "Inner", &["auth".into(), "Auth.billing.inner".into()]), "auth.billing.inner");
        // And back again.
        for spelled in ["plain", "auth.check", "auth.billing.inner.deep", "auth.billing.inner"] {
            let (id, path) = resolve_address(&p, spelled);
            assert_eq!(address_of(&p, &id, &path), spelled);
        }
    }
}

/// The way a person writes the group `group_id` running under
/// `call_path`: a body is the site that calls it (`one` for the file
/// `one` includes), any other group its own address.
///
/// Takes the program indexed ([`graph::ProjectGraph`]) or bare, like
/// [`address_of`].
pub fn group_address(g: &impl graph::GraphView, group_id: &str, call_path: &[String]) -> String {
    match call_path.split_last() {
        Some((site, above)) if places::is_body(g, group_id) => address_of(g, site, above),
        _ => address_of(g, group_id, call_path),
    }
}

/// A compiled id as a person reads it, when no program is at hand to
/// spell its address through a call site ([`address_of`] is the answer
/// when one is). An id inside an included file carries the file's
/// path (`@src:lib:setup.store`), unspellable on purpose; a person
/// reads it as the file's own name and the rest (`setup.store`). Any
/// other id is already a name somebody wrote and comes back as it is.
///
/// Every message the runtime writes for a person goes through this or
/// through `address_of`: the id itself is internal, and it leaks the
/// moment it is printed raw.
pub fn plain_id(id: &str) -> String {
    let Some(rest) = id.strip_prefix('@') else { return id.to_string() };
    // The path stops at the first `.`: the file `@src:lib:setup`, then
    // the node `.store` (or the nested group `.rows`).
    let (path, tail) = rest.split_once('.').unwrap_or((rest, ""));
    let file = path.rsplit(':').next().unwrap_or(path);
    if tail.is_empty() { file.to_string() } else { format!("{file}.{tail}") }
}

/// The inverse of [`resolve_address`]: the way a person writes the node
/// `id` running under `call_path`. `Auth.check` under `["auth"]` is
/// `auth.check`; `Inner.deep` under `["auth", "Auth.billing.inner"]` is
/// `auth.billing.inner.deep`. A site id after the first, and the node's
/// id, are scoped under the body they sit in, so that body's prefix
/// comes off each. With no call path the id is its own address.
///
/// A loop's `LoopIn` and `LoopOut` are the compiler's, nobody wrote
/// them, so they read as the loop itself (`each`, `one.counting`). What
/// happened at the boundary (launched, gathered, skipped) is the
/// record's kind, not its name. A body reads as the site that called it
/// (`one`, through [`group_address`]).
///
/// The editor spells the same thing for the nodes and groups it shows
/// (a trigger's display poll is keyed by this spelling), so the two
/// are kept the same function; the loop arm exists only here, because
/// the editor never addresses a loop's boundary.
///
/// Takes the program bare for one spelling, or indexed
/// ([`graph::ProjectGraph`]) by a caller spelling many (every row of a
/// run's record), so each lookup is constant time.
// SYNC: address_of <-> packages/weft-graph/src/run-spec.ts addressOf
pub fn address_of(g: &impl graph::GraphView, id: &str, call_path: &[String]) -> String {
    // A loop's boundary, read off its id rather than spelled for every
    // loop: every firing asks this.
    if let Some(group) = boundary_loop(id).and_then(|group| g.group(group)) {
        return group_address(g, &group.id, call_path);
    }
    let Some((first, rest)) = call_path.split_first() else { return id.to_string() };
    // The body a site or node sits in is the prefix its scope chain
    // starts with, which is the id's first segment; what is left is its
    // local name. A loop's boundary never gets here (the arm above spells
    // it as its loop), so every id has a segment to strip or is bare.
    let local = |scoped: &str| -> String {
        match scoped.split_once('.') {
            Some((_, local)) => local.to_string(),
            None => scoped.to_string(),
        }
    };
    let mut parts = vec![first.clone()];
    parts.extend(rest.iter().map(|s| local(s)));
    parts.push(local(id));
    parts.join(".")
}

/// [`address_of`] for an id that came from outside the program (a row of
/// a run's record): `None` when the program has no node or group of that
/// id, or a site on `call_path` is not one of its call sites.
/// [`address_of`] trusts its id, since every other caller took it from the
/// program itself, and spells an unknown one as plausible text; a record
/// read against a program it does not belong to must not read that way.
pub fn placed_address(g: &impl graph::GraphView, id: &str, call_path: &[String]) -> Option<String> {
    let known = g.node(id).is_some() || g.group(id).is_some();
    let sites = call_path.iter().all(|site| g.is_site(site));
    (known && sites).then(|| address_of(g, id, call_path))
}

/// A node named the way a person reads the program, through the call
/// sites: `auth.check` is the node `check` of the file the site `auth`
/// includes, and `auth.billing.inner.deep` walks two sites. The answer
/// is the node's id in the compiled definition (`Auth.check`) and the
/// call path that use of it runs under (`["auth"]`, or `["auth",
/// "Auth.billing.inner"]`), which is what its journal rows carry as
/// call frames. A spelling that crosses no site is the id itself with
/// an empty path, so an ordinary node keeps its ordinary address.
///
/// The inverse of [`address_of`]. A miss is never an error here: the
/// spelling comes back as given with an empty path, and it then names
/// no node, which is what every caller checks for. A spelling that is
/// no address at all (empty, or with an empty segment like `one.`)
/// is a miss the same way, rather than being read as the nearest
/// thing it resembles.
pub fn resolve_address(project: &ProjectDefinition, spelled: &str) -> (String, Vec<String>) {
    let segments: Vec<&str> = spelled.split('.').collect();
    if segments.iter().any(|segment| segment.is_empty()) {
        return (spelled.to_string(), Vec::new());
    }
    let mut call_path = Vec::new();
    let mut scope_prefix = String::new();
    let mut pos = 0;
    'outer: while pos < segments.len() {
        for k in 1..=(segments.len() - pos) {
            let candidate = format!("{scope_prefix}{}", segments[pos..pos + k].join("."));
            let body = project.groups.iter().find(|g| g.id == candidate).and_then(|g| match &g.kind {
                GroupKind::Call { body, .. } => Some(body.clone()),
                _ => None,
            });
            if let Some(body) = body {
                call_path.push(candidate);
                scope_prefix = format!("{body}.");
                pos += k;
                continue 'outer;
            }
        }
        break;
    }
    let rest = segments[pos..].join(".");
    if rest.is_empty() {
        // The spelling ends on a site: the address is the site itself,
        // which runs in its caller's frames, under the calls above it.
        // Nothing left and no site matched cannot happen: an empty
        // spelling was refused above as an empty segment.
        let Some(site) = call_path.pop() else {
            return (spelled.to_string(), Vec::new());
        };
        return (site, call_path);
    }
    (format!("{scope_prefix}{rest}"), call_path)
}

/// The ids of the project's trigger nodes (`features.is_trigger`).
pub fn trigger_ids(project: &ProjectDefinition) -> Vec<String> {
    project.nodes.iter().filter(|n| n.features.is_trigger).map(|n| n.id.clone()).collect()
}

/// The ids of the project's infra nodes (`requires_infra`).
pub fn infra_ids(project: &ProjectDefinition) -> Vec<String> {
    project.nodes.iter().filter(|n| n.requires_infra).map(|n| n.id.clone()).collect()
}

/// Every place a trigger is at: a trigger inside an included file is
/// one per site that reaches it.
pub fn trigger_places(project: &ProjectDefinition) -> Vec<Located> {
    let ids = trigger_ids(project);
    places::every_place(project).into_iter().filter(|place| ids.contains(&place.id)).collect()
}

/// Every place an infra node is at (see `trigger_places`).
pub fn infra_places(project: &ProjectDefinition) -> Vec<Located> {
    let ids = infra_ids(project);
    places::every_place(project).into_iter().filter(|place| ids.contains(&place.id)).collect()
}

/// The infra places a program runs, spelled (`db`, `one.db`): its infra
/// places, less the ones it is given instead of running (a stand-in: an
/// infra node read only through its baked outputs, a seed's saved result).
/// For a run's cut, the infra the run reaches.
pub fn infra_reached(program: &ProjectDefinition) -> std::collections::BTreeSet<String> {
    infra_places(program)
        .into_iter()
        .filter(|place| !program.given.outputs.contains(place))
        .map(|place| address_of(program, &place.id, &place.path))
        .collect()
}

/// Whether the node `id` exists once per instance (`@per_instance`): every
/// copy of it (its infra, its trigger's activation) is an instance's, and
/// none is shared. Asked of a place by its `id`, of a spelled place
/// after `resolve_address`.
pub fn is_per_instance(project: &ProjectDefinition, id: &str) -> bool {
    project.nodes.iter().any(|n| n.id == id && n.per_instance.is_some())
}

/// A node's validation rules, for checking an instance's values on it.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct InstanceRules {
    /// Every rule of the node type (the declared ones, and the ones the
    /// language adds, like "no connection picked"), at every level: a
    /// run for an instance is a run, so runtime-level rules hold too.
    pub rules: Vec<crate::node::ValidationRule>,
    /// The output ports the source added beyond the type's own, which
    /// `custom_outputs_declared` and `{customOutputs}` read.
    #[serde(default, rename = "customOutputs", skip_serializing_if = "Vec::is_empty")]
    pub custom_outputs: Vec<String>,
}

/// One place of a node with a `@instance_filled` field
/// ([`instance_filled_places`]).
#[derive(Debug, Clone)]
pub struct FilledPlace<'a> {
    /// The place spelled (`one.form`): what an instance's values are
    /// keyed by, and what a person names it by.
    pub spelled: String,
    pub place: Located,
    pub node: &'a NodeDefinition,
}

/// Every place of a node with a `@instance_filled` field: what the
/// instance door lists, and what an instance's values are keyed by. A node
/// inside a file included twice is at two places, and an instance fills
/// each on its own.
pub fn instance_filled_places(project: &ProjectDefinition) -> Vec<FilledPlace<'_>> {
    let g = graph::ProjectGraph::new(project);
    places::every_place_in(&g)
        .into_iter()
        .filter_map(|place| {
            let node = project.nodes.iter().find(|n| n.id == place.id)?;
            crate::instance::instance_filled_fields(node).next()?;
            Some(FilledPlace { spelled: address_of(&g, &place.id, &place.path), place, node })
        })
        .collect()
}

/// Every infra PLACEMENT the program declares, each spelled the way a
/// person writes its place (`db`, or `one.db` inside the file the site
/// `one` includes): the key its `infra_node` row is stored under. THE
/// set every reader of those rows checks the rows against (what is
/// missing, what is an orphan, what counts in the rollup), so they
/// cannot disagree on what the program declares. Each spelling is kept
/// with its place, so a reader that goes on to the node behind it has it
/// at hand rather than reading the spelling back.
pub fn infra_place_spellings(project: &ProjectDefinition) -> std::collections::BTreeMap<String, Located> {
    infra_places(project)
        .into_iter()
        .map(|place| (address_of(project, &place.id, &place.path), place))
        .collect()
}

/// Which copies of its infra the program declares: each place (spelled
/// as [`infra_place_spellings`] spells it) and whether it has one copy
/// per instance or one shared copy. A copy it does not declare is gone
/// from the program for good: the node was removed, or it changed sides
/// (a node no longer per instance leaves its instances' copies behind, a
/// node now per instance leaves its shared one).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DeclaredInfra {
    /// Place -> whether it is per instance.
    places: std::collections::BTreeMap<String, bool>,
    /// The program hides nodes behind a part that does not compile
    /// ([`ProjectDefinition::hides_nodes`]), so a copy it does not
    /// declare may only be hidden.
    hides: bool,
}

impl DeclaredInfra {
    pub fn of(project: &ProjectDefinition) -> Self {
        let places = infra_place_spellings(project)
            .into_iter()
            .map(|(spelled, place)| (spelled, is_per_instance(project, &place.id)))
            .collect();
        Self { places, hides: project.hides_nodes() }
    }

    /// Whether the infra place `spelled` has one copy per instance
    /// (`Some(true)`) or one shared copy (`Some(false)`); `None` when the
    /// program declares no infra place by that name.
    pub fn per_instance(&self, spelled: &str) -> Option<bool> {
        self.places.get(spelled).copied()
    }

    /// Whether the program has a copy of `spelled` of this kind: an
    /// instance's (`instance_copy`) or the shared one.
    pub fn declares(&self, spelled: &str, instance_copy: bool) -> bool {
        self.per_instance(spelled) == Some(instance_copy)
    }

    /// Whether the copy `spelled` of this kind is gone from the program
    /// for good, so its units and disks may go: it does not declare it,
    /// and no part that does not compile could be hiding it.
    pub fn gone(&self, spelled: &str, instance_copy: bool) -> bool {
        !self.hides && !self.declares(spelled, instance_copy)
    }

    /// [`Self::gone`] for the copy a host names.
    pub fn gone_copy(&self, copy: &crate::infra::NodeRef) -> bool {
        self.gone(&copy.node, copy.is_instance_copy())
    }
}

/// Everything `seeds` depend on, by following wires backward, seeds
/// included, over the whole program by id (no places, so a body's member
/// is one id however many sites call it): each wire into a node, and every
/// group around a node (its id is in the answer: the group decides whether
/// the node runs), with the wires onto that group's gate, for a loop its
/// `LoopIn`, and for an included body every call site of it. A group a wire starts from (a
/// literal on its port) is in the answer the same way.
pub fn upstream_closure(g: &impl graph::GraphView, seeds: &[String]) -> std::collections::HashSet<String> {
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut frontier: Vec<String> = seeds.to_vec();
    while let Some(id) = frontier.pop() {
        if !seen.insert(id.clone()) {
            continue;
        }
        for edge in g.edges_into(&id).filter(|edge| !edge.to_group_port()) {
            frontier.push(edge.source.clone());
        }
        match g.node(&id) {
            Some(node) => frontier.extend(node.scope.iter().cloned()),
            None => {
                let group = g.group(&id);
                frontier.extend(group.and_then(|group| group.parent_group_id.clone()));
                // A loop runs its members once per iteration its `LoopIn`
                // starts: what feeds that (the list, the gate) decides them.
                if group.is_some_and(|group| matches!(group.kind, GroupKind::Loop { .. })) {
                    frontier.push(loop_in_id(&id));
                }
                frontier.extend(g.project().groups.iter()
                    .filter(|site| matches!(&site.kind, GroupKind::Call { body, .. } if *body == id))
                    .map(|site| site.id.clone()));
            }
        }
    }
    seen
}

#[cfg(test)]
mod trigger_dependency_tests {
    use super::*;
    use crate::NodeFeatures;

    fn node(id: &str, is_trigger: bool, requires_infra: bool) -> NodeDefinition {
        NodeDefinition {
            id: id.into(),
            node_type: "Any".into(),
            type_params: Vec::new(),
            place_types: Vec::new(),
            label: None,
            config: Value::Object(Default::default()),
            position: Position { x: 0.0, y: 0.0 },
            scope: vec![],
            group_boundary: None,
            inputs: vec![],
            outputs: vec![],
            features: NodeFeatures { is_trigger, ..Default::default() },
            requires_infra,
            per_instance: None,
            images: vec![],
            published_service: None,
            instance_service: None,
            instance_rules: None,
            span: None,
            header_span: None,
            config_spans: Default::default(),
            optional_ports: Default::default(),
            port_literals: Default::default(),
            port_literal_spans: Default::default(),
            port_literal_types: Default::default(),
            metadata_ports: Vec::new(),
            file_refs: Default::default(),
            include_path: None,
            include_contents: None,
            fires_with: Default::default(), baked_outputs: Default::default(),
            source_file: None,
        }
    }

    fn wire(source: &str, target: &str) -> Edge {
        Edge {
            id: format!("e-{source}-{target}"),
            source: source.to_string(),
            target: target.to_string(),
            source_handle: None,
            target_handle: None,
            path: Vec::new(),
            via: Vec::new(),
            span: None,
            source_file: None,
        }
    }

    fn project(nodes: Vec<NodeDefinition>, edges: Vec<Edge>) -> ProjectDefinition {
        ProjectDefinition {
            id: Uuid::nil(),
            nodes,
            edges,
            groups: vec![],
            created_at: Utc::now(),
            updated_at: Utc::now(),
            defaults: Default::default(),
            given: Default::default(),
            broken: Vec::new(),
            metadata_wires: Vec::new(),
        }
    }

    /// A wire through the group ports `via`, each `(group, port, side)`.
    fn through(source: &str, sp: &str, via: &[(&str, &str, &str)], target: &str, tp: &str) -> Value {
        serde_json::json!({
            "id": format!("{source}.{sp}->{target}.{tp}"), "source": source, "target": target,
            "sourceHandle": sp, "targetHandle": tp,
            "via": via.iter().map(|(group, port, side)| serde_json::json!({ "group": group, "port": port, "side": side })).collect::<Vec<_>>(),
        })
    }

    /// What the trigger at `trigger` depends on (`crate::run_spec::trigger_cut`).
    fn deps(p: &ProjectDefinition, trigger: &str) -> Vec<String> {
        crate::run_spec::trigger_cut(p, trigger).expect("its cut walks").depends_on
    }

    #[test]
    fn an_infra_node_feeding_a_trigger_counts() {
        // bridge -> receive(trigger) -> reply
        let p = project(
            vec![
                node("bridge", false, true),
                node("receive", true, false),
                node("reply", false, false),
            ],
            vec![wire("bridge", "receive"), wire("receive", "reply")],
        );
        assert_eq!(deps(&p, "receive"), vec!["bridge".to_string()]);
    }

    #[test]
    fn an_infra_node_the_trigger_reaches_through_another_node_counts() {
        // bridge -> conf -> receive(trigger): the dependency is just as
        // real one hop further out.
        let p = project(
            vec![
                node("bridge", false, true),
                node("conf", false, false),
                node("receive", true, false),
            ],
            vec![wire("bridge", "conf"), wire("conf", "receive")],
        );
        assert_eq!(deps(&p, "receive"), vec!["bridge".to_string()]);
    }

    /// The database a fire writes to is in its cut: taking it down takes
    /// the trigger down with it, since its fires cannot run without it.
    #[test]
    fn an_infra_node_downstream_of_a_trigger_counts() {
        let p = project(
            vec![node("receive", true, false), node("db", false, true)],
            vec![wire("receive", "db")],
        );
        assert_eq!(deps(&p, "receive"), vec!["db".to_string()]);
    }

    #[test]
    fn an_infra_node_on_another_branch_stays_out() {
        let p = project(
            vec![
                node("receive", true, false),
                node("reply", false, false),
                node("svc", false, true),
                node("svc_out", false, false),
            ],
            vec![wire("receive", "reply"), wire("svc", "svc_out")],
        );
        assert!(deps(&p, "receive").is_empty());
    }

    #[test]
    fn a_project_with_no_trigger_has_no_cut() {
        let p = project(vec![node("svc", false, true)], vec![]);
        assert!(crate::run_spec::trigger_cuts(&p).is_empty());
    }

    /// A group's port is where the walk's port-precision earns its
    /// keep: the trigger sits inside the group and reads the group's `x`,
    /// so infra feeding the group's `y` never reaches it, and stopping it
    /// leaves the trigger alone.
    #[test]
    fn infra_feeding_a_group_port_the_trigger_does_not_read_stays_out() {
        let node = |id: &str, scope: &[&str], infra: bool| serde_json::json!({
            "id": id, "nodeType": "T", "label": null, "config": {},
            "position": { "x": 0, "y": 0 }, "inputs": [], "outputs": [],
            "features": { "isTrigger": id == "receive" }, "scope": scope,
            "groupBoundary": null, "requiresInfra": infra
        });
        let p: ProjectDefinition = serde_json::from_value(serde_json::json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "nodes": [
                node("bridge", &[], true),
                node("svc", &[], true),
                node("receive", &["g"], false),
                node("other", &["g"], false)
            ],
            "edges": [
                through("bridge", "out", &[("g", "x", "in")], "receive", "in"),
                through("svc", "out", &[("g", "y", "in")], "other", "in")
            ],
            "groups": [{ "id": "g", "kind": "group", "nodeIds": ["receive", "other"] }]
        })).expect("the fixture deserializes");
        assert_eq!(deps(&p, "receive"), vec!["bridge".to_string()]);
    }

    /// Infra deciding whether the trigger's group flows is read by the
    /// arming run once, and the armed trigger never reads it again, so
    /// stopping it later breaks nothing: it is no dependency.
    #[test]
    fn gate_infra_is_no_dependency() {
        let node = |id: &str, scope: &[&str], infra: bool| serde_json::json!({
            "id": id, "nodeType": "T", "label": null, "config": {},
            "position": { "x": 0, "y": 0 }, "inputs": [], "outputs": [],
            "features": { "isTrigger": id == "receive" }, "scope": scope,
            "groupBoundary": null, "requiresInfra": infra
        });
        let p: ProjectDefinition = serde_json::from_value(serde_json::json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "nodes": [
                node("bridge", &[], true),
                node("gate", &[], true),
                node("receive", &["g"], false)
            ],
            "edges": [
                through("bridge", "out", &[("g", "x", "in")], "receive", "in"),
                through("gate", "out", &[], "g", "_should_flow")
            ],
            "groups": [{ "id": "g", "kind": "group", "nodeIds": ["receive"] }]
        })).expect("the fixture deserializes");
        assert_eq!(deps(&p, "receive"), vec!["bridge".to_string()]);
    }


    #[test]
    fn a_cycle_upstream_of_a_trigger_terminates() {
        let p = project(
            vec![
                node("receive", true, false),
                node("db", false, true),
                node("loopy", false, false),
            ],
            vec![wire("db", "loopy"), wire("loopy", "db"), wire("db", "receive")],
        );
        assert_eq!(deps(&p, "receive"), vec!["db".to_string()]);
    }

    /// The shape an include compiles to: one body (`Bridge`) holding an
    /// infra node feeding a trigger, called from the sites `one` and
    /// `two`; a top-level infra node wired into `one`'s port alone, and
    /// `two`'s port fed by nobody (it sends its closure when it opens).
    fn program_with_two_sites() -> ProjectDefinition {
        let node = |id: &str, scope: &[&str], is_trigger: bool, requires_infra: bool| {
            serde_json::json!({
                "id": id, "nodeType": "T", "label": null, "config": {}, "position": {"x": 0, "y": 0},
                "inputs": [], "outputs": [], "features": {"isTrigger": is_trigger},
                "scope": scope, "groupBoundary": null, "requiresInfra": requires_infra
            })
        };
        serde_json::from_value(serde_json::json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "nodes": [
                node("top_db", &[], false, true),
                node("Bridge.svc", &["Bridge"], false, true),
                node("Bridge.door", &["Bridge"], true, false),
            ],
            "edges": [
                through("top_db", "out", &[("one", "seed", "in"), ("Bridge", "seed", "in")], "Bridge.door", "seed"),
                through("two", "seed", &[("two", "seed", "in"), ("Bridge", "seed", "in")], "Bridge.door", "seed"),
                through("Bridge.svc", "out", &[], "Bridge.door", "address"),
            ],
            "groups": [
                {"id": "one", "kind": "call", "body": "Bridge", "nodeIds": []},
                {"id": "two", "kind": "call", "body": "Bridge", "nodeIds": []},
                {"id": "Bridge", "kind": "body", "nodeIds": ["Bridge.svc", "Bridge.door"]}
            ]
        })).expect("the fixture deserializes")
    }

    /// Answered per PLACE, spelled: the trigger under `one` depends on
    /// the placement under `one` and on the top-level node wired into
    /// `one`'s door; the trigger under `two` depends on its own placement
    /// alone, because the top-level node feeds only `one`.
    #[test]
    fn a_file_included_twice_depends_per_call() {
        let p = program_with_two_sites();
        assert_eq!(deps(&p, "one.door"), vec!["one.svc".to_string(), "top_db".to_string()]);
        assert_eq!(deps(&p, "two.door"), vec!["two.svc".to_string()]);
    }
}

#[cfg(test)]
mod project_wire_tests {
    use super::*;

    /// Layer-2 wire-shape: `ProjectDefinition` is stored as JSON and crosses the
    /// CLI->dispatcher boundary. It must (a) NOT serialize a `name`/`description`
    /// key (those were dropped; identity is the manifest, descriptions per-group)
    /// and (b) still deserialize OLD json that carries those keys, so existing
    /// stored project_json rows keep loading (no `deny_unknown_fields`).
    #[test]
    fn project_definition_wire_shape() {
        let p = ProjectDefinition {
            id: Uuid::nil(),
            nodes: vec![],
            edges: vec![],
            groups: vec![],
            created_at: Utc::now(),
            updated_at: Utc::now(),
            defaults: Default::default(),
            given: Default::default(),
            broken: Vec::new(),
            metadata_wires: Vec::new(),
        };
        let v = serde_json::to_value(&p).unwrap();
        assert!(v.get("name").is_none(), "name must not serialize: {v}");
        assert!(v.get("description").is_none(), "description must not serialize: {v}");

        // Old JSON with the dropped keys still loads (unknown keys ignored).
        let old = serde_json::json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "name": "legacy",
            "description": "legacy desc",
            "nodes": [], "edges": [], "groups": []
        });
        let back: ProjectDefinition = serde_json::from_value(old).expect("old json must still deserialize");
        assert!(back.nodes.is_empty());
    }

    /// Pin the camelCase wire keys of the NESTED structs (node, port, edge,
    /// group) so a `#[serde(rename)]` drift that would break the TS contract
    /// fails here, not at the customer. Round-trips a populated project.
    #[test]
    fn project_definition_nested_wire_keys() {
        let input = InputDefinition {
            port: PortDefinition {
                name: "inp".into(),
                port_type: WeftType::primitive(crate::weft_type::WeftPrimitive::String),
                required: true,
                description: None,
                synthesized_from_carry: false,
                declared_type: None,
            },
            accepts: Accepts::wire_only(),
            optional: false,
            passes_through: false,
            widget: None,
            default: None,
            label: None,
            placeholder: None,
            from_spec: false,
            requires_scopes: None,
            requires_values: None,
        };
        let node = NodeDefinition {
            id: "g.n".into(),
            node_type: "Llm".into(),
            type_params: Vec::new(),
            place_types: Vec::new(),
            label: None,
            config: default_config(),
            position: Position { x: 0.0, y: 0.0 },
            scope: vec!["g".into()],
            group_boundary: None,
            inputs: vec![input.clone()],
            outputs: vec![],
            features: Default::default(),
            requires_infra: false, per_instance: None,
            images: vec![],
            fires_with: Default::default(), baked_outputs: Default::default(),
            published_service: None,
            instance_service: None,
            instance_rules: None,
            span: Some(Span::single_line(1, 0, 5)),
            header_span: Some(Span::single_line(1, 0, 3)),
            config_spans: Default::default(),
            optional_ports: Default::default(),
            port_literals: Default::default(),
            port_literal_spans: Default::default(),
            port_literal_types: Default::default(),
            metadata_ports: Vec::new(),
            file_refs: Default::default(),
            include_path: None,
            include_contents: None,
            source_file: None,
        };
        let group = GroupDefinition {
            id: "g".into(),
            kind: GroupKind::Group,
            label: None,
            in_ports: vec![input.clone()],
            out_ports: vec![],
            parent_group_id: None,
            child_group_ids: vec![],
            node_ids: vec!["g.n".into()],
            anonymous: false,
            span: None,
            header_span: None,
            source_file: None,
            description: None,
            port_literals: [("_should_flow".to_string(), Value::Bool(false))].into(),
            file_refs: Default::default(),
            port_literal_spans: Default::default(),
            port_literal_types: [("x".to_string(), WeftType::primitive(crate::weft_type::WeftPrimitive::String))].into(),
            metadata_ports: vec![MetadataPort {
                name: "tool".into(),
                port_type: WeftType::primitive(crate::weft_type::WeftPrimitive::String),
                description: None,
                value: Some(Value::String("x".into())),
                written: None,
                span: Some(Span::single_line(2, 0, 9)),
                declared_type: Some("String".into()),
            }],
            type_params: Vec::new(),
            place_types: Vec::new(),
        };
        let edge = Edge {
            id: "e1".into(),
            source: "g.n".into(),
            source_handle: Some("out".into()),
            target: "m".into(),
            target_handle: Some("in".into()),
            path: Vec::new(),
            via: vec![Crossing { group: "g".into(), port: "y".into(), side: CrossingSide::Out, path: vec!["k".into()], span: None, source_file: None }],
            span: None,
            source_file: None,
        };
        let p = ProjectDefinition {
            id: Uuid::nil(),
            nodes: vec![node],
            edges: vec![edge.clone()],
            groups: vec![group],
            created_at: Utc::now(),
            updated_at: Utc::now(),
            defaults: Default::default(),
            given: Default::default(),
            broken: Vec::new(),
            metadata_wires: vec![edge],
        };
        let v = serde_json::to_value(&p).unwrap();
        // A metadata port, a metadata wire and a literal's stated type, in
        // the editor's spelling.
        assert_eq!(
            v["groups"][0]["metadataPorts"],
            serde_json::json!([{ "name": "tool", "type": "String", "value": "x", "span": { "startLine": 2, "startColumn": 0, "endLine": 2, "endColumn": 9 }, "declaredType": "String" }]),
            "metadataPorts: {v}"
        );
        assert_eq!(v["metadataWires"][0]["id"], "e1", "metadataWires: {v}");
        assert_eq!(v["groups"][0]["portLiteralTypes"]["x"], "String", "portLiteralTypes: {v}");
        assert!(v["nodes"][0].get("metadataPorts").is_none() && v["nodes"][0].get("portLiteralTypes").is_none(), "empty ones are left off: {v}");
        assert_eq!(v["groups"][0]["portLiterals"]["_should_flow"], false, "portLiterals key: {v}");
        // The renamed keys the TS side depends on:
        assert!(v["nodes"][0].get("nodeType").is_some(), "nodeType key: {v}");
        assert!(v["nodes"][0]["inputs"][0].get("portType").is_some(), "portType key: {v}");
        // An instance input's resolved surface: `accepts` always
        // serializes (the list form), the optional members are OMITTED
        // when absent (never `null`, which the TS optional types don't
        // model).
        assert_eq!(v["nodes"][0]["inputs"][0]["accepts"], serde_json::json!(["wire"]), "accepts list: {v}");
        for absent in ["widget", "default", "label", "placeholder", "description", "declaredType"] {
            assert!(
                v["nodes"][0]["inputs"][0].get(absent).is_none(),
                "unset `{absent}` must be omitted: {v}"
            );
        }
        assert!(v["groups"][0].get("inPorts").is_some(), "inPorts key: {v}");
        assert!(v["groups"][0].get("nodeIds").is_some(), "nodeIds key: {v}");
        assert!(v.get("createdAt").is_some(), "createdAt key: {v}");
        // A wire's crossings: camelCase, each side spelled, an empty
        // `given` left off.
        assert_eq!(v["edges"][0]["via"], serde_json::json!([{ "group": "g", "port": "y", "side": "out", "path": ["k"] }]), "via: {v}");
        assert!(v.get("given").is_none(), "an empty given is left off: {v}");
        // headerSpan: the editor reads it to underline a node's header line. Both
        // the key (`headerSpan`) and the nested `Span` fields are camelCase, the
        // one wire convention this whole module follows; pin them so neither
        // drifts back to snake_case.
        let hs = &v["nodes"][0]["headerSpan"];
        assert!(hs.is_object(), "headerSpan key (camelCase): {v}");
        assert!(hs.get("startLine").is_some() && hs.get("startColumn").is_some(), "span start bounds (camelCase): {hs}");
        assert!(hs.get("endLine").is_some() && hs.get("endColumn").is_some(), "span end bounds (camelCase): {hs}");
        assert!(hs.get("end_col").is_none() && hs.get("start_col").is_none(), "no leftover snake_case span keys: {hs}");
        // Full round-trip survives.
        let back: ProjectDefinition = serde_json::from_value(v).expect("round-trip");
        assert_eq!(back.nodes[0].node_type, "Llm");
        assert_eq!(back.groups[0].in_ports.len(), 1);
        assert_eq!(back.nodes[0].header_span, Some(Span::single_line(1, 0, 3)), "headerSpan round-trips");
        assert_eq!(back.metadata_wires.len(), 1);
        assert_eq!(back.groups[0].metadata_ports[0].value, Some(Value::String("x".into())));
    }

    fn node_with_infra(id: &str, requires_infra: bool) -> NodeDefinition {
        NodeDefinition {
            id: id.into(),
            node_type: "Any".into(),
            type_params: Vec::new(),
            place_types: Vec::new(),
            label: None,
            config: default_config(),
            position: Position { x: 0.0, y: 0.0 },
            scope: vec![],
            group_boundary: None,
            inputs: vec![],
            outputs: vec![],
            features: Default::default(),
            requires_infra,
            per_instance: None,
            images: vec![],
            fires_with: Default::default(), baked_outputs: Default::default(),
            published_service: None,
            instance_service: None,
            instance_rules: None,
            span: None,
            header_span: None,
            config_spans: Default::default(),
            optional_ports: Default::default(),
            port_literals: Default::default(),
            port_literal_spans: Default::default(),
            port_literal_types: Default::default(),
            metadata_ports: Vec::new(),
            file_refs: Default::default(),
            include_path: None,
            include_contents: None,
            source_file: None,
        }
    }

    fn project_with_nodes(nodes: Vec<NodeDefinition>) -> ProjectDefinition {
        ProjectDefinition {
            id: Uuid::nil(),
            nodes,
            edges: vec![],
            groups: vec![],
            created_at: Utc::now(),
            updated_at: Utc::now(),
            defaults: Default::default(),
            given: Default::default(),
            broken: Vec::new(),
            metadata_wires: Vec::new(),
        }
    }

    #[test]
    fn has_infra_is_any_node_requires_infra() {
        assert!(!has_infra(&project_with_nodes(vec![])), "empty project has no infra");

        let no_infra = project_with_nodes(vec![
            node_with_infra("a", false),
            node_with_infra("b", false),
        ]);
        assert!(!has_infra(&no_infra), "no node requires infra");

        let with_infra = project_with_nodes(vec![
            node_with_infra("a", false),
            node_with_infra("c", true),
        ]);
        assert!(has_infra(&with_infra), "one infra node flips it true");
    }

    #[test]
    fn declared_infra_names_each_place_and_its_side() {
        let mut per_instance = node_with_infra("mine", true);
        per_instance.per_instance = Some(crate::instance::PerInstance::Marked);
        let project = project_with_nodes(vec![node_with_infra("db", true), per_instance, node_with_infra("plain", false)]);
        let declared = DeclaredInfra::of(&project);
        assert!(declared.declares("db", false));
        assert!(!declared.declares("db", true), "a shared node has no instance copies");
        assert!(declared.declares("mine", true));
        assert!(!declared.declares("mine", false), "a per-instance node has no shared copy");
        assert!(!declared.declares("plain", false), "a node without infra has no copy");
        assert!(!declared.declares("gone", false));

        let alice = crate::instance::InstanceId::new("alice").unwrap();
        let copy = |node: &str, of: Option<&crate::instance::InstanceId>| crate::infra::NodeRef {
            tenant: "t".into(),
            project: project.id,
            node: node.into(),
            copy_id: crate::infra::NodeRef::copy_id(project.id, node, of),
        };
        assert!(!declared.gone_copy(&copy("db", None)));
        assert!(declared.gone_copy(&copy("db", Some(&alice))));
        assert!(!declared.gone_copy(&copy("mine", Some(&alice))));
        assert!(declared.gone_copy(&copy("mine", None)));
        assert!(declared.gone("gone", false));
    }

    /// A program with a part that may hide nodes proves nothing gone: a
    /// copy it does not declare may sit inside the included file that
    /// does not compile. A broken part that hides nothing proves as much
    /// as a whole program.
    #[test]
    fn a_program_hiding_nodes_proves_no_copy_gone() {
        let mut project = project_with_nodes(vec![node_with_infra("db", true)]);
        project.broken = vec![Broken { parts: vec!["db".into()], why: "a wire".into(), hides: false }];
        assert!(DeclaredInfra::of(&project).gone("other", false), "a wire hides no node");
        project.broken.push(Broken { parts: vec!["lib".into()], why: "lib.weft does not parse".into(), hides: true });
        let declared = DeclaredInfra::of(&project);
        assert!(project.hides_nodes());
        assert!(!declared.gone("other", false), "it may be inside lib.weft");
        assert!(declared.declares("db", false), "what it declares, it still declares");
    }
}

#[cfg(test)]
mod setup_cut_tests {
    use super::*;
    use crate::run_spec::{compute_trigger_fire, setup_cut, KickPlan};

    /// A wire: its source, the group ports it passes as `(group, side)`,
    /// its target.
    type Wire<'a> = (&'a str, &'a [(&'a str, &'a str)], &'a str);

    /// A program from its nodes (id, is_trigger, requires_infra, scope
    /// chain), its groups (each with its parent), and its wires. Every
    /// port is `v`: these tests are about scopes and wires, not about which
    /// port feeds which.
    fn project(nodes: &[(&str, bool, bool, &[&str])], groups: &[(&str, Option<&str>)], edges: &[Wire]) -> ProjectDefinition {
        let n_json: Vec<serde_json::Value> = nodes
            .iter()
            .map(|(id, is_trigger, requires_infra, scope)| serde_json::json!({
                "id": id, "nodeType": "T", "label": null, "config": {},
                "position": { "x": 0, "y": 0 }, "scope": scope, "groupBoundary": null,
                "inputs": [{ "name": "v", "portType": "String", "required": false }],
                "outputs": [{ "name": "v", "portType": "String", "required": false }],
                "features": { "isTrigger": is_trigger }, "requiresInfra": requires_infra,
            }))
            .collect();
        let e_json: Vec<serde_json::Value> = edges
            .iter()
            .map(|(s, via, t)| serde_json::json!({
                "id": format!("e_{s}_{t}"), "source": s, "sourceHandle": "v", "target": t, "targetHandle": "v",
                "via": via.iter().map(|(group, side)| serde_json::json!({ "group": group, "port": "v", "side": side })).collect::<Vec<_>>(),
            }))
            .collect();
        let g_json: Vec<serde_json::Value> = groups
            .iter()
            .map(|(id, parent)| serde_json::json!({
                "id": id, "kind": "group", "parentGroupId": parent,
                "inPorts": [{ "name": "v", "portType": "String", "required": false }],
                "outPorts": [{ "name": "v", "portType": "String", "required": false }],
                "nodeIds": nodes.iter().filter(|(_, _, _, scope)| scope.last() == Some(id)).map(|(n, ..)| *n).collect::<Vec<_>>(),
            }))
            .collect();
        serde_json::from_value(serde_json::json!({
            "id": Uuid::nil(), "nodes": n_json, "edges": e_json, "groups": g_json
        }))
        .expect("valid test project")
    }

    fn runs(program: &ProjectDefinition) -> Vec<String> {
        places::running_places(program).into_iter().map(|place| place.to_string()).collect()
    }

    fn kicked(kicks: &[KickPlan]) -> Vec<(String, bool)> {
        kicks.iter().map(|kick| (kick.node.clone(), kick.firing)).collect()
    }

    fn places(v: &[&str]) -> Vec<Located> {
        v.iter().map(|s| Located::top(*s)).collect()
    }

    /// cfg ──► g.a, and g.seed has no input. Setting up the seed runs the
    /// seed alone: what feeds the group's other members is no input of
    /// it. The group stays in the cut (the seed starts when it opens) and,
    /// nothing reaching it in the cut, it opens as the run starts: no root
    /// is kicked from the outside.
    #[test]
    fn an_unwired_group_member_needs_its_scope_but_not_the_other_input_branches() {
        let p = project(
            &[("cfg", false, false, &[]), ("g.a", false, false, &["g"]), ("g.seed", false, false, &["g"]), ("other", false, false, &[])],
            &[("g", None)],
            &[("cfg", &[("g", "in")], "g.a")],
        );
        let (cut, kicks) = setup_cut(&p, &places(&["g.seed"])).unwrap();
        assert_eq!(runs(&cut), vec!["g.seed".to_string()]);
        assert!(cut.groups.iter().any(|group| group.id == "g"), "the scope the seed starts in stays");
        assert!(cut.edges.is_empty(), "{:?}", cut.edges);
        assert!(kicks.is_empty(), "the group kicks its seed when it opens: {kicks:?}");
    }

    /// root ──► h.x ──► g.a: aiming at g.a walks the wire back out of g,
    /// out of h and up to root, which is the one root.
    #[test]
    fn the_walk_follows_wires_through_scopes() {
        let p = project(
            &[("root", false, false, &[]), ("h.x", false, false, &["h"]), ("g.a", false, false, &["g"])],
            &[("h", None), ("g", None)],
            &[("root", &[("h", "in")], "h.x"), ("h.x", &[("h", "out"), ("g", "in")], "g.a")],
        );
        let (cut, kicks) = setup_cut(&p, &places(&["g.a"])).unwrap();
        assert_eq!(runs(&cut), vec!["g.a".to_string(), "h.x".to_string(), "root".to_string()]);
        assert_eq!(kicked(&kicks), vec![("root".to_string(), false)]);
    }

    /// A fire runs the trigger and what it reaches, and never what feeds
    /// the trigger: its ports replay what its setup captured.
    #[test]
    fn a_fired_trigger_runs_and_what_feeds_it_does_not() {
        let p = project(
            &[("setup", false, false, &[]), ("trig", true, false, &[]), ("out", false, false, &[])],
            &[],
            &[("setup", &[], "trig"), ("trig", &[], "out")],
        );
        let fire = compute_trigger_fire(&p, "trig", &serde_json::Value::Null, None, &Default::default()).unwrap();
        assert_eq!(runs(&fire.program), vec!["out".to_string(), "trig".to_string()]);
        assert_eq!(kicked(&fire.kicks), vec![("trig".to_string(), true)]);
        assert!(fire.program.edges.iter().all(|edge| edge.target != "trig"), "no wire into the fired trigger");
    }

    /// An infra node with no input inside a group is set up through its
    /// group: the group opens as the run starts and kicks it.
    #[test]
    fn an_unwired_infra_node_inside_a_group_is_started_by_its_scope() {
        let p = project(&[("g.db", false, true, &["g"])], &[("g", None)], &[]);
        let (cut, kicks) = setup_cut(&p, &infra_places(&p)).unwrap();
        assert_eq!(runs(&cut), vec!["g.db".to_string()]);
        assert!(cut.groups.iter().any(|group| group.id == "g"));
        assert!(kicks.is_empty(), "{kicks:?}");
    }

    #[test]
    fn a_setup_phase_runs_its_seeds_and_what_feeds_them_only() {
        // text ──► compute ──► infra ──► trigger ──► reply
        // cfg ──► infra_b
        let p = project(
            &[
                ("text", false, false, &[]),
                ("compute", false, false, &[]),
                ("infra", false, true, &[]),
                ("trigger", true, false, &[]),
                ("reply", false, false, &[]),
                ("cfg", false, false, &[]),
                ("infra_b", false, true, &[]),
            ],
            &[],
            &[("text", &[], "compute"), ("compute", &[], "infra"), ("infra", &[], "trigger"), ("trigger", &[], "reply"), ("cfg", &[], "infra_b")],
        );
        let (infra, _) = setup_cut(&p, &infra_places(&p)).unwrap();
        assert_eq!(runs(&infra), ["cfg", "compute", "infra", "infra_b", "text"].map(String::from));
        let (triggers, kicks) = setup_cut(&p, &trigger_places(&p)).unwrap();
        assert_eq!(runs(&triggers), ["compute", "infra", "text", "trigger"].map(String::from));
        // A trigger's setup reads its inputs: the wire into it stays, and
        // it fires on what reaches it rather than being kicked.
        assert!(triggers.edges.iter().any(|edge| edge.target == "trigger"));
        assert_eq!(kicked(&kicks), vec![("text".to_string(), false)]);
        let none = project(&[("a", false, false, &[]), ("b", false, false, &[])], &[], &[("a", &[], "b")]);
        assert!(setup_cut(&none, &infra_places(&none)).is_err(), "nothing to set up is never the whole program");
    }

    /// A broken part stops only the cuts that need it: a node every one
    /// of whose places the cut drops, and a wire whose other end it drops,
    /// are not its business. The refusal names every part it needs.
    #[test]
    fn a_cut_is_refused_only_when_it_needs_a_broken_part() {
        // a ──► b      g.c (inside g)      trig ──► out
        let mut p = project(
            &[("a", false, false, &[]), ("b", false, false, &[]), ("g.c", false, false, &["g"]), ("trig", true, false, &[]), ("out", false, false, &[])],
            &[("g", None)],
            &[("a", &[], "b"), ("trig", &[], "out")],
        );
        p.broken = vec![
            Broken { parts: vec!["a".into(), "b".into()], why: "src/main.weft:3:1 a.v cannot feed b.v".into(), hides: false },
            Broken { parts: vec!["g".into()], why: "src/main.weft:5:1 the group g is broken\nsecond line".into(), hides: false },
        ];
        assert!(setup_cut(&p, &places(&["a"])).is_ok(), "a alone does not carry the wire");
        let refused = setup_cut(&p, &places(&["b"])).unwrap_err();
        assert!(refused.contains("a.v cannot feed b.v"), "{refused}");
        assert!(!refused.contains("the group g"), "{refused}");
        let refused = setup_cut(&p, &places(&["g.c"])).unwrap_err();
        assert!(refused.contains("the group g is broken\n    second line"), "a reason keeps its lines under it: {refused}");
        // Fed from outside, g.c needs nothing of g as a scope, so the cut
        // dissolves g; its member and the wire through its port still run,
        // and so g's broken part is still needed.
        let mut fed = project(
            &[("f", false, false, &[]), ("g.c", false, false, &["g"])],
            &[("g", None)],
            &[("f", &[("g", "in")], "g.c")],
        );
        let (dissolved, _) = setup_cut(&fed, &places(&["g.c"])).unwrap();
        assert!(dissolved.groups.is_empty(), "the cut keeps no scope for g: {:?}", dissolved.groups);
        fed.broken = vec![Broken { parts: vec!["g".into()], why: "a stream through g's port".into(), hides: false }];
        assert!(setup_cut(&fed, &places(&["g.c"])).unwrap_err().contains("a stream through g's port"));
        let fire = compute_trigger_fire(&p, "trig", &serde_json::Value::Null, None, &Default::default()).unwrap();
        assert!(fire.program.broken.is_empty(), "a cut carries nothing broken");
        p.broken.push(Broken { parts: vec!["out".into()], why: "out did not load".into(), hides: true });
        let refused = compute_trigger_fire(&p, "trig", &serde_json::Value::Null, None, &Default::default()).unwrap_err();
        assert!(refused.starts_with("a fire of 'trig' cannot run") && refused.contains("out did not load"), "{refused}");
    }

    /// A trigger is kicked at the top wherever it sits; the copies around
    /// it decide when its kick goes.
    #[test]
    fn a_fired_trigger_inside_a_group_is_kicked_from_the_top() {
        let p = project(&[("g.trig", true, false, &["g"])], &[("g", None)], &[]);
        let fire = compute_trigger_fire(&p, "g.trig", &serde_json::Value::Null, None, &Default::default()).unwrap();
        assert_eq!(kicked(&fire.kicks), vec![("g.trig".to_string(), true)]);
    }
}
