//! Graph-level project types. Describes a weft program as a graph:
//! nodes (instances of a node type), edges (connections between port
//! refs). Port and field shapes live on the node TYPE (NodeMetadata),
//! not on the instance. `NodeFeatures` is preserved on each node.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::frames::{Located, LoopFrames};
use crate::node::Accepts;
use crate::weft_type::WeftType;

/// The canonical form of a program and the digests over it.
pub mod hash;
pub mod selection;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectDefinition {
    pub id: Uuid,
    pub nodes: Vec<NodeDefinition>,
    pub edges: Vec<Edge>,
    /// Group structure preserved by the parser. The flattened node
    /// list in `nodes` contains the In/Out boundary Passthroughs +
    /// child nodes for each group; this field carries the pre-
    /// flatten tree so tooling (the VS Code graph view, AI editors)
    /// can render groups as structured units without re-deriving
    /// them from the flat layout.
    #[serde(default)]
    pub groups: Vec<GroupDefinition>,
    #[serde(rename = "createdAt", default = "Utc::now")]
    pub created_at: DateTime<Utc>,
    #[serde(rename = "updatedAt", default = "Utc::now")]
    pub updated_at: DateTime<Utc>,
}

/// What kind of grouping construct this is. The visual editor uses
/// this to pick a renderer; the runtime / flatten step uses the
/// underlying boundary node type (Passthrough vs LoopIn/LoopOut).
///
/// The loop's config (parallel / over / carry / max_iters /
/// trim_on_mismatch) rides INSIDE the `Loop` variant, so "kind ==
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
    /// site has no members of its own; its two boundaries hand the
    /// caller's values to the shared `body` under a call frame and
    /// take the body's results back out.
    Call { body: String },
    /// An included file, compiled once and shared by every site that
    /// calls it. Its members fire under the caller's frames plus the
    /// site's call frame, so ten uses are one body and ten frames.
    Body,
}

/// The boundary node types the compiler mints. A group's two halves
/// are `Passthrough`; a loop's are `LoopIn` / `LoopOut`; a call site's
/// are `CallIn` / `CallOut`; a shared body's are `IncludeIn` /
/// `IncludeOut`. One place for the names, so the engine, the journal
/// fold and the compiler cannot drift on them.
pub mod boundary_types {
    pub const PASSTHROUGH: &str = "Passthrough";
    pub const LOOP_IN: &str = "LoopIn";
    pub const LOOP_OUT: &str = "LoopOut";
    pub const CALL_IN: &str = "CallIn";
    pub const CALL_OUT: &str = "CallOut";
    pub const INCLUDE_IN: &str = "IncludeIn";
    pub const INCLUDE_OUT: &str = "IncludeOut";

    /// A boundary that forwards its ports one for one and fires in the
    /// boundary pass (never dispatched as a node): every kind but a
    /// loop's, whose halves the engine drives itself.
    pub fn is_forwarding(node_type: &str) -> bool {
        matches!(node_type, PASSTHROUGH | CALL_IN | CALL_OUT | INCLUDE_IN | INCLUDE_OUT)
    }

    /// A boundary whose ports are selected one by one when a run is
    /// cut (`RunSelection`): a group's, a call site's and a body's. A
    /// loop is indivisible and goes in whole.
    ///
    /// The same set as [`is_forwarding`], and that is not a
    /// coincidence: a boundary forwards its ports one for one exactly
    /// when its ports can be taken one at a time. Written as one list
    /// because two lists of one closed set drift the first time a
    /// boundary kind is added and only one is edited. The two names
    /// stay because the callers are asking different questions.
    pub fn is_port_selected(node_type: &str) -> bool {
        is_forwarding(node_type)
    }

    /// An In boundary that opens a new frame for its scope: a loop's
    /// (an iteration frame per launch) and a body's (the call frame
    /// its site pushed). The scope's members fire one frame deeper
    /// than the boundary's caller.
    pub fn opens_frame(node_type: &str) -> bool {
        matches!(node_type, LOOP_IN | INCLUDE_IN)
    }

    /// Every boundary type the engine ships, for whatever has to name
    /// them all (the worker's implementation map).
    pub const ALL: &[&str] = &[PASSTHROUGH, LOOP_IN, LOOP_OUT, CALL_IN, CALL_OUT, INCLUDE_IN, INCLUDE_OUT];

    /// Any boundary type at all.
    pub fn is_boundary(node_type: &str) -> bool {
        is_forwarding(node_type) || matches!(node_type, LOOP_IN | LOOP_OUT)
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
    /// External input ports (the ports outside the group connects to).
    #[serde(rename = "inPorts", default)]
    pub in_ports: Vec<PortDefinition>,
    /// External output ports.
    #[serde(rename = "outPorts", default)]
    pub out_ports: Vec<PortDefinition>,
    /// Parent group id for nested groups. None for top-level groups.
    #[serde(rename = "parentGroupId", default)]
    pub parent_group_id: Option<String>,
    /// Ids of child groups (first-level only; nested groups carry
    /// their own entry with `parent_group_id` set).
    #[serde(rename = "childGroupIds", default)]
    pub child_group_ids: Vec<String>,
    /// Ids of member nodes (only direct children, not grandchildren).
    /// Does NOT include the In/Out boundary nodes (`Passthrough` for
    /// groups, `LoopIn`/`LoopOut` for loops).
    #[serde(rename = "nodeIds", default)]
    pub node_ids: Vec<String>,
    /// True for the anonymous top-level group of an included `.weft` file
    /// (no `name =`). The editor labels it from the filename and validates it
    /// as a component (no project-level output requirement).
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
    /// is an ordinary edge and never appears here.
    // SYNC: port_literals <-> packages/weft-graph/src/protocol.ts GroupDefinition.portLiterals
    #[serde(default, rename = "portLiterals")]
    pub port_literals: std::collections::BTreeMap<String, Value>,
    /// Where each `port_literals` entry was written, and in which form
    /// (a braces field or a `g.x = ...` statement), so an edit rewrites
    /// the value where it already lives.
    // SYNC: port_literal_spans <-> packages/weft-graph/src/protocol.ts GroupDefinition.portLiteralSpans
    #[serde(default, rename = "portLiteralSpans")]
    pub port_literal_spans: std::collections::BTreeMap<String, ConfigFieldSpan>,
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
    /// because a boundary node holds spans from two files at once: its
    /// loop knobs from the included file, its interface-port fills from
    /// the file that wrote `alias.port = value`.
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
    /// If this is a Passthrough at a group boundary, which group and
    /// side.
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
    /// `true` if this node implements `Node::provision_infra` and the
    /// dispatcher must run InfraSetup before activate. Mirrored from
    /// NodeMetadata.requires_infra at enrich time.
    #[serde(default, rename = "requiresInfra")]
    pub requires_infra: bool,
    /// Image source dirs the CLI builds for this node. Mirrored from
    /// NodeMetadata.images at enrich time. The CLI walks this to know
    /// which Dockerfiles to build before sending imageHashes to
    /// the dispatcher.
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
    /// The recipe for the service this node publishes a connection to
    /// (`ctx.publish_access`), resolved from the catalog at enrich
    /// time from the node metadata's `publishes` name. Carried on the
    /// definition rather than looked up at run time: a worker only
    /// ships the node types its project uses, so the service's own
    /// access node is usually absent from it, while the compiler sees
    /// the whole catalog.
    #[serde(default, rename = "publishedService", skip_serializing_if = "Option::is_none")]
    pub published_service: Option<crate::access::spec::AccessSpec>,
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
    // SYNC: optional_ports <-> packages/weft-graph/src/protocol.ts NodeDefinition.optionalPorts
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
    /// The node types found inside, before the catalog was consulted.
    /// The parser has no catalog, so it records the types and enrich
    /// settles the two booleans above from them.
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

/// THE derivation of a group's IN-boundary node id. The compiler
/// mints exactly this shape when it flattens a group (or a loop);
/// the engine and the journal fold re-derive it. One definition so
/// the convention cannot drift.
// SYNC: boundary_in_id, boundary_out_id <-> packages/weft-graph/src/webview/host-bridge.ts BOUNDARY_IN, BOUNDARY_OUT
pub fn boundary_in_id(group_id: &str) -> String {
    format!("{group_id}__in")
}

/// THE derivation of a group's OUT-boundary node id; see
/// [`boundary_in_id`].
pub fn boundary_out_id(group_id: &str) -> String {
    format!("{group_id}__out")
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
// SYNC: InputDefinition <-> packages/weft-graph/src/protocol.ts InputDefinition
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
    // SYNC: InputDefinition.accepts <-> packages/weft-graph/src/protocol.ts InputDefinition.accepts
    #[serde(default = "Accepts::both")]
    pub accepts: Accepts,
    /// The input's effective editor widget (declared, else derived from
    /// the RESOLVED instance type after TypeVar substitution). Always
    /// present after enrich.
    // SYNC: InputDefinition.widget <-> packages/weft-graph/src/protocol.ts InputDefinition.widget
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub widget: Option<crate::node::Widget>,
    /// The input's declared default value, if any (mirrored from the
    /// metadata so the runtime and the editor read it off the instance).
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
    // SYNC: InputDefinition.from_spec <-> packages/weft-graph/src/protocol.ts InputDefinition.fromSpec
    #[serde(default, rename = "fromSpec", skip_serializing_if = "std::ops::Not::not")]
    pub from_spec: bool,
    /// The permissions THIS consumer needs on the wired connection
    /// (mirrored from the metadata's `requiresScopes`; only ever set on
    /// an Access-typed input). The runtime stamps them onto the access
    /// marker when this node's bag is built, so resolution can hold a
    /// verified connection to them; the editor reads them for its live
    /// shortfall check.
    // SYNC: InputDefinition.requires_scopes <-> packages/weft-graph/src/protocol.ts InputDefinition.requiresScopes
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
    // SYNC: InputDefinition.requires_values <-> packages/weft-graph/src/protocol.ts InputDefinition.requiresValues
    #[serde(
        default,
        rename = "requiresValues",
        skip_serializing_if = "Option::is_none"
    )]
    pub requires_values: Option<Vec<String>>,
}

impl PortDefinition {
    /// Whether this port carries a stream (`Generator[T]`). THE one
    /// reading of a port's type for that question; the node-level
    /// readers in `exec::ready` are built on it.
    pub fn is_generator(&self) -> bool {
        self.port_type.as_generator().is_some()
    }
}

impl InputDefinition {
    /// An input for a pure WIRE port (a boundary passthrough side, a
    /// source-declared custom port): drivers from the type, no editor
    /// surface beyond what enrich later stamps.
    pub fn from_wire_port(port: PortDefinition) -> Self {
        Self {
            accepts: Accepts::for_type(&port.port_type),
            port,
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

/// A pure WIRE port on a node instance's OUTPUT side or a group/loop
/// interface: a named, typed dock for edges. Inputs are the richer
/// [`InputDefinition`].
// SYNC: PortDefinition <-> packages/weft-graph/src/protocol.ts PortDefinition
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
    // SYNC: PortDefinition.declared_type <-> packages/weft-graph/src/protocol.ts PortDefinition.declaredType
    #[serde(default, rename = "declaredType", skip_serializing_if = "Option::is_none")]
    pub declared_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    pub x: f64,
    pub y: f64,
}

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
    /// are five independent projections.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub path: Vec<String>,
    /// Source range of the connection line (`target.port = source.port`).
    /// Used by tooling to remove or rewrite the edge.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub span: Option<Span>,
    /// The file the span lives in when the edge came from a full-mode
    /// `@include` (same rule as `NodeDefinition::source_file`).
    #[serde(default, rename = "sourceFile", skip_serializing_if = "Option::is_none")]
    pub source_file: Option<String>,
}

/// Pre-indexed edge lookups. Build once per compiled project, use
/// many times during execution. Under a run selection the lookups
/// answer for a PLACE (a node under the call frames it fires at): a
/// wire between two nodes of an included file is in the run under one
/// call and not under another, so the frames pick which wires exist.
pub struct EdgeIndex {
    outgoing: std::collections::HashMap<String, Vec<usize>>,
    incoming: std::collections::HashMap<String, Vec<usize>>,
    selection: Option<selection::RunSelection>,
}

impl EdgeIndex {
    pub fn build(project: &ProjectDefinition) -> Self {
        let mut outgoing: std::collections::HashMap<String, Vec<usize>> = std::collections::HashMap::new();
        let mut incoming: std::collections::HashMap<String, Vec<usize>> = std::collections::HashMap::new();
        for (i, edge) in project.edges.iter().enumerate() {
            outgoing.entry(edge.source.clone()).or_default().push(i);
            incoming.entry(edge.target.clone()).or_default().push(i);
        }
        Self { outgoing, incoming, selection: None }
    }

    pub fn selected(project: &ProjectDefinition, selection: selection::RunSelection) -> Self {
        let mut index = Self::build(project);
        index.selection = Some(selection);
        index
    }

    pub fn selection(&self) -> Option<&selection::RunSelection> {
        self.selection.as_ref()
    }

    pub fn includes_port(&self, node: &NodeDefinition, frames: &LoopFrames, port: &str) -> bool {
        self.selection.as_ref().is_none_or(|selection| selection.includes_port(&Located::at(&node.id, frames), node, port))
    }

    /// Whether the node at `frames` is in the run (every node is, in a
    /// whole run).
    pub fn admits(&self, node_id: &str, frames: &LoopFrames) -> bool {
        self.selection.as_ref().is_none_or(|selection| selection.nodes.contains(&Located::at(node_id, frames)))
    }

    /// The wires out of `node_id` that exist at `frames`.
    pub fn get_outgoing<'a>(&self, project: &'a ProjectDefinition, node_id: &str, frames: &LoopFrames) -> Vec<&'a Edge> {
        let at = Located::at(node_id, frames);
        self.outgoing.get(node_id)
            .map(|indices| indices.iter().map(|&i| &project.edges[i])
                .filter(|edge| self.selection.as_ref().is_none_or(|s| s.has_edge(project, &at, edge, true))).collect())
            .unwrap_or_default()
    }

    /// The wires into `node_id` that exist at `frames`.
    pub fn get_incoming<'a>(&self, project: &'a ProjectDefinition, node_id: &str, frames: &LoopFrames) -> Vec<&'a Edge> {
        let at = Located::at(node_id, frames);
        self.incoming.get(node_id)
            .map(|indices| indices.iter().map(|&i| &project.edges[i])
                .filter(|edge| self.selection.as_ref().is_none_or(|s| s.has_edge(project, &at, edge, false))).collect())
            .unwrap_or_default()
    }
}

/// Whether this project declares ANY infrastructure: true iff at least
/// one node has `requires_infra`. The single project-level fact that
/// decides namespace placement: an infra project gets its own k8s
/// namespace (its worker must sit next to its infra pods), a no-infra
/// project's worker runs in the shared worker namespace. Pure walk over
/// the node list; the one copy of this predicate so the dispatcher and
/// any other consumer can't drift on what "has infra" means.
pub fn has_infra(project: &ProjectDefinition) -> bool {
    project.nodes.iter().any(|n| n.requires_infra)
}

/// What an ARMED trigger keeps depending on: for every infra PLACE,
/// every trigger place whose data path runs through it. Returns
/// `(infra, trigger)` pairs, each spelled the way a person writes the
/// place (`svc`, or `one.svc` inside the file the site `one` includes),
/// sorted by `(infra, trigger)`.
///
/// Per place, because that is what runs: a file included twice holds
/// two infra instances and two registered triggers, and the trigger
/// under `one` reads its address off the instance under `one` alone.
/// The spelling is the key every infra row and signal row is stored
/// under, so a reader compares this against those directly.
///
/// This is the STAY-RUNNING question, and it is not the same as
/// [`infra_triggers_depend_on`], which answers the BEFORE-ARMING one.
/// A registered trigger holds an address it read off the node feeding
/// it, so stopping that node breaks a signal that is live right now.
/// Infra that only fed the GATE deciding whether the trigger's group
/// flows was read once, during activation, and the armed registration
/// does not touch it again: stopping it later breaks nothing, and
/// refusing that stop would trap the user into deactivating first.
/// Hence the plain data walk here (`selection::upstream_by_wires`),
/// and the run-shaped one there.
///
/// Read by:
///   - the dispatcher's per-node infra stop/terminate guard;
///   - the broker's `supervisor_trigger_deps` endpoint, so the
///     supervisor decides the same way when infra goes flaky.
///
/// Pure walk over `ProjectDefinition`; no I/O.
pub fn compute_trigger_deps(project: &ProjectDefinition) -> Vec<(String, String)> {
    let is_infra = |place: &Located| project.nodes.iter().any(|n| n.id == place.id && n.requires_infra);
    let mut out: Vec<(String, String)> = Vec::new();
    for trigger in trigger_places(project) {
        let spelled_trigger = address_of(project, &trigger.id, &trigger.path);
        for place in selection::upstream_by_wires(project, std::slice::from_ref(&trigger)) {
            if is_infra(&place) {
                out.push((address_of(project, &place.id, &place.path), spelled_trigger.clone()));
            }
        }
    }
    out.sort();
    out
}

/// The scope (group or loop) `node` lives directly in, or `None` at
/// the top level. A boundary node lives in the scope that holds its
/// container (its `scope` is the container's parent chain), so a
/// nested group's In boundary is a member of the enclosing body.
pub fn direct_scope_of(node: &NodeDefinition) -> Option<&str> {
    node.scope.last().map(String::as_str)
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
        // A boundary reads as its group; a body's boundary as its site.
        assert_eq!(address_of(&p, "Auth__in", &["auth".into()]), "auth");
        assert_eq!(address_of(&p, "Auth__out", &["auth".into()]), "auth");
        assert_eq!(address_of(&p, "Auth.billing__in", &["auth".into()]), "auth.billing");
        assert_eq!(address_of(&p, "Inner__out", &["auth".into(), "Auth.billing.inner".into()]), "auth.billing.inner");
        assert_eq!(group_address(&p, "Auth", &["auth".into()]), "auth");
        assert_eq!(group_address(&p, "Auth.billing", &["auth".into()]), "auth.billing");
        // The internal spelling still resolves, for the machinery.
        assert_eq!(resolve_address(&p, "auth.__in"), ("Auth__in".into(), vec!["auth".into()]));
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
pub fn group_address(project: &ProjectDefinition, group_id: &str, call_path: &[String]) -> String {
    match call_path.split_last() {
        Some((site, above)) if selection::is_body(project, group_id) => address_of(project, site, above),
        _ => address_of(project, group_id, call_path),
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
/// A group's In and Out boundaries are the compiler's, nobody wrote
/// them, so they read as the group itself (`gate`, `one.counting`); a
/// body's boundaries read as the site that called it (`one`). What
/// happened at the boundary (entered, left, skipped) is the record's
/// kind, not its name.
///
/// The editor spells the same thing for the nodes and groups it shows
/// (a trigger's display poll is keyed by this spelling), so the two
/// are kept the same function; the boundary arms exist only here,
/// because the editor never addresses a boundary.
// SYNC: address_of <-> packages/weft-graph/src/run-spec.ts addressOf
pub fn address_of(project: &ProjectDefinition, id: &str, call_path: &[String]) -> String {
    if let Some(group) = project.groups.iter().find(|g| id == boundary_in_id(&g.id) || id == boundary_out_id(&g.id)) {
        return group_address(project, &group.id, call_path);
    }
    let Some((first, rest)) = call_path.split_first() else { return id.to_string() };
    // The body a site or node sits in is the prefix its scope chain
    // starts with, which is the id's first segment; what is left is its
    // local name. A boundary never gets here (the arm above spells it
    // as its group), so every id has a segment to strip or is bare.
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
                GroupKind::Call { body } => Some(body.clone()),
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
        // which fires in its caller's frames, under the calls above it.
        // Nothing left and no site matched cannot happen: an empty
        // spelling was refused above as an empty segment.
        let Some(site) = call_path.pop() else {
            return (spelled.to_string(), Vec::new());
        };
        return (site, call_path);
    }
    if rest.starts_with("__") {
        // A body's own boundary: `one.__in` is the body's In under `one`.
        return (format!("{}{rest}", scope_prefix.trim_end_matches('.')), call_path);
    }
    (format!("{scope_prefix}{rest}"), call_path)
}

/// Every node inside scope `group_id`, however deep: its direct members
/// (nested containers' boundaries among them) and everything inside
/// those. What a gated scope takes down with it.
pub fn scope_members<'a>(project: &'a ProjectDefinition, group_id: &str) -> Vec<&'a NodeDefinition> {
    project
        .nodes
        .iter()
        .filter(|n| n.scope.iter().any(|g| g == group_id))
        .collect()
}

/// The nodes a scope launcher kicks when scope `group_id` starts: its
/// DIRECT members that no wire feeds (a nested container's In boundary
/// counts as a member; its Out boundary too, so a body that wires
/// nothing to `self.out` still closes its outputs). Triggers are never
/// among them: a trigger is kicked by its fire, or payload-less by a
/// manual run, never by the START of the scope it sits in (the REFUSAL
/// of that scope does reach it: see `tear_down_scope`, which skips
/// every member). Everything else inside the scope is reached by pulses
/// once these run. The ONE definition of "what starts with a scope",
/// read by the group launcher and the loop launcher alike.
pub fn scope_body_roots(
    project: &ProjectDefinition,
    edge_idx: &EdgeIndex,
    group_id: &str,
    frames: &LoopFrames,
) -> Vec<String> {
    project
        .nodes
        .iter()
        .filter(|n| direct_scope_of(n) == Some(group_id))
        .filter(|n| edge_idx.admits(&n.id, frames))
        .filter(|n| !n.features.is_trigger)
        .filter(|n| {
            let at = Located::at(&n.id, frames);
            !edge_idx.get_incoming(project, &n.id, frames).iter().any(|edge|
                edge_idx.selection().is_none_or(|s| s.fed_by(project, &at, edge)))
        })
        .map(|n| n.id.clone())
        .collect()
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
    selection::every_place(project).into_iter().filter(|place| ids.contains(&place.id)).collect()
}

/// Every place an infra node is at (see `trigger_places`).
pub fn infra_places(project: &ProjectDefinition) -> Vec<Located> {
    let ids = infra_ids(project);
    selection::every_place(project).into_iter().filter(|place| ids.contains(&place.id)).collect()
}

/// Every infra INSTANCE the program declares, each spelled the way a
/// person writes its place (`db`, or `one.db` inside the file the site
/// `one` includes): the key its `infra_node` row is stored under. THE
/// set every reader of those rows checks the rows against (what is
/// missing, what is an orphan, what counts in the rollup), so they
/// cannot disagree on what the program declares.
pub fn infra_place_spellings(project: &ProjectDefinition) -> std::collections::BTreeSet<String> {
    infra_places(project)
        .iter()
        .map(|place| address_of(project, &place.id, &place.path))
        .collect()
}


/// Every node `seeds` depend on by following wires backward, seeds
/// included, over the whole program (no run selection, so the frames
/// are the root's). The scope of a setup phase (everything the
/// triggers, or the infra nodes, need) and of an untargeted manual run.
pub fn upstream_closure(
    project: &ProjectDefinition,
    edge_idx: &EdgeIndex,
    seeds: &[String],
) -> std::collections::HashSet<String> {
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut frontier: Vec<String> = seeds.to_vec();
    while let Some(id) = frontier.pop() {
        if !seen.insert(id.clone()) {
            continue;
        }
        for edge in edge_idx.get_incoming(project, &id, &Vec::new()) {
            if !seen.contains(&edge.source) {
                frontier.push(edge.source.clone());
            }
        }
    }
    seen
}

/// The infra a trigger DEPENDS ON, each PLACE named once, spelled the
/// way a person writes it (`svc`, `one.svc`): the key its infra row is
/// stored under, so a reader compares this against the rows directly.
///
/// Infra and triggers are independent lifetimes, joined by one rule: a
/// trigger needs the infra feeding it to be RUNNING. A WhatsApp receive
/// node takes the bridge's address off the bridge node feeding it, so
/// arming it before the bridge is up would register a signal against an
/// address that does not answer. Infra DOWNSTREAM of a trigger belongs
/// to the run a fire starts and is gated there instead, so the project
/// arms either way. Nothing here ever starts infra: that is the user's
/// own verb.
///
/// What must be RUNNING before this project's triggers can be armed.
///
/// Arming them is a run: the TriggerSetup pass evaluates everything the
/// registration needs, including the gate deciding whether a trigger's
/// group flows at all. So this walks the way that run walks
/// (`RunSelection::dependencies`, which is `RunSelection::setup` minus
/// its validations), and the answer covers every infra node that run
/// touches: the bridge whose address a trigger reads, and the node
/// feeding a gate the setup has to evaluate.
///
/// Reading ports and call frames is what keeps it honest in the other
/// direction: infra wired into a group port no trigger consumes is not
/// in it, and does not hold an activation back.
///
/// The STAY-RUNNING question is [`compute_trigger_deps`], and it is a
/// different set on purpose: once a trigger is armed, the gate's infra
/// has already done its job. Changing one of these two to match the
/// other is how the system starts contradicting itself; they answer
/// different questions about different moments.
///
/// Empty when the project has no trigger.
pub fn infra_triggers_depend_on(
    project: &ProjectDefinition,
) -> std::collections::BTreeSet<String> {
    let reached = selection::RunSelection::dependencies(project, &trigger_places(project));
    reached
        .nodes
        .iter()
        .filter(|place| project.nodes.iter().any(|n| n.id == place.id && n.requires_infra))
        .map(|place| address_of(project, &place.id, &place.path))
        .collect()
}

#[cfg(test)]
mod infra_triggers_depend_on_tests {
    use super::*;
    use crate::NodeFeatures;

    fn node(id: &str, is_trigger: bool, requires_infra: bool) -> NodeDefinition {
        NodeDefinition {
            id: id.into(),
            node_type: "Any".into(),
            label: None,
            config: Value::Object(Default::default()),
            position: Position { x: 0.0, y: 0.0 },
            scope: vec![],
            group_boundary: None,
            inputs: vec![],
            outputs: vec![],
            features: NodeFeatures { is_trigger, ..Default::default() },
            requires_infra,
            images: vec![],
            published_service: None,
            span: None,
            header_span: None,
            config_spans: Default::default(),
            optional_ports: Default::default(),
            port_literals: Default::default(),
            port_literal_spans: Default::default(),
            file_refs: Default::default(),
            include_path: None,
            include_contents: None,
            fires_with: Default::default(),
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
        }
    }

    fn ids(set: std::collections::BTreeSet<String>) -> Vec<String> {
        set.into_iter().collect()
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
        assert_eq!(ids(infra_triggers_depend_on(&p)), vec!["bridge".to_string()]);
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
        assert_eq!(ids(infra_triggers_depend_on(&p)), vec!["bridge".to_string()]);
    }

    #[test]
    fn an_infra_node_downstream_of_a_trigger_stays_out() {
        // The database the fired run writes to belongs to the run, not
        // to arming the trigger; the run is what waits on it.
        let p = project(
            vec![node("receive", true, false), node("db", false, true)],
            vec![wire("receive", "db")],
        );
        assert!(infra_triggers_depend_on(&p).is_empty());
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
        assert!(infra_triggers_depend_on(&p).is_empty());
    }

    #[test]
    fn a_project_with_no_trigger_reaches_nothing() {
        let p = project(vec![node("svc", false, true)], vec![]);
        assert!(infra_triggers_depend_on(&p).is_empty());
    }

    /// A group boundary is where the walk's port-precision earns its
    /// keep: the trigger sits inside the group and reads the In's `x`,
    /// so infra feeding the In's `y` never reaches it and must not hold
    /// the activation back. A cruder walk (every wire into the
    /// boundary) would name `svc` here and grey out Activate over infra
    /// the triggers never touch.
    #[test]
    fn infra_feeding_a_group_port_the_trigger_does_not_read_stays_out() {
        let node = |id: &str, scope: &[&str], boundary: Value, infra: bool| serde_json::json!({
            "id": id, "nodeType": "T", "label": null, "config": {},
            "position": { "x": 0, "y": 0 }, "inputs": [], "outputs": [],
            "features": { "isTrigger": id == "receive" }, "scope": scope,
            "groupBoundary": boundary, "requiresInfra": infra
        });
        let edge = |source: &str, sp: &str, target: &str, tp: &str| serde_json::json!({
            "id": format!("{source}.{sp}->{target}.{tp}"), "source": source, "target": target,
            "sourceHandle": sp, "targetHandle": tp
        });
        let p: ProjectDefinition = serde_json::from_value(serde_json::json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "nodes": [
                node("bridge", &[], Value::Null, true),
                node("svc", &[], Value::Null, true),
                node("g__in", &[], serde_json::json!({ "groupId": "g", "role": "In" }), false),
                node("receive", &["g"], Value::Null, false),
                node("other", &["g"], Value::Null, false)
            ],
            "edges": [
                edge("bridge", "out", "g__in", "x"),
                edge("svc", "out", "g__in", "y"),
                edge("g__in", "x", "receive", "in"),
                edge("g__in", "y", "other", "in")
            ],
            "groups": [{ "id": "g", "kind": "group", "nodeIds": ["receive", "other"] }]
        })).expect("the fixture deserializes");
        assert_eq!(ids(infra_triggers_depend_on(&p)), vec!["bridge".to_string()]);
        // The stay-running walk is port-precise at the boundary too.
        assert_eq!(compute_trigger_deps(&p), vec![("bridge".to_string(), "receive".to_string())]);
    }

    /// The two questions part company at the GATE: infra deciding
    /// whether the trigger's group flows is evaluated by the arming run
    /// and holds the activation back, but the armed trigger never reads
    /// it again, so stopping it later breaks nothing and the stay-running
    /// walk leaves it out.
    #[test]
    fn gate_infra_holds_the_activation_back_and_nothing_after() {
        let node = |id: &str, scope: &[&str], boundary: Value, infra: bool| serde_json::json!({
            "id": id, "nodeType": "T", "label": null, "config": {},
            "position": { "x": 0, "y": 0 }, "inputs": [], "outputs": [],
            "features": { "isTrigger": id == "receive" }, "scope": scope,
            "groupBoundary": boundary, "requiresInfra": infra
        });
        let edge = |source: &str, sp: &str, target: &str, tp: &str| serde_json::json!({
            "id": format!("{source}.{sp}->{target}.{tp}"), "source": source, "target": target,
            "sourceHandle": sp, "targetHandle": tp
        });
        let p: ProjectDefinition = serde_json::from_value(serde_json::json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "nodes": [
                node("bridge", &[], Value::Null, true),
                node("gate", &[], Value::Null, true),
                node("g__in", &[], serde_json::json!({ "groupId": "g", "role": "In" }), false),
                node("receive", &["g"], Value::Null, false),
                node("g__out", &[], serde_json::json!({ "groupId": "g", "role": "Out" }), false)
            ],
            "edges": [
                edge("bridge", "out", "g__in", "x"),
                edge("gate", "out", "g__in", "_should_flow"),
                edge("g__in", "x", "receive", "in")
            ],
            "groups": [{ "id": "g", "kind": "group", "nodeIds": ["receive"] }]
        })).expect("the fixture deserializes");
        assert_eq!(ids(infra_triggers_depend_on(&p)), vec!["bridge".to_string(), "gate".to_string()]);
        assert_eq!(compute_trigger_deps(&p), vec![("bridge".to_string(), "receive".to_string())]);
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
        assert_eq!(ids(infra_triggers_depend_on(&p)), vec!["db".to_string()]);
    }

    /// The shape an include compiles to: one body (`Bridge`) holding an
    /// infra node feeding a trigger, called from the sites `one` and
    /// `two`; a top-level infra node wired into `one`'s door alone.
    fn program_with_two_sites() -> ProjectDefinition {
        let node = |id: &str, scope: &[&str], boundary: Value, is_trigger: bool, requires_infra: bool| {
            serde_json::json!({
                "id": id, "nodeType": "T", "label": null, "config": {}, "position": {"x": 0, "y": 0},
                "inputs": [], "outputs": [], "features": {"isTrigger": is_trigger},
                "scope": scope, "groupBoundary": boundary, "requiresInfra": requires_infra
            })
        };
        let edge = |source: &str, sp: &str, target: &str, tp: &str| serde_json::json!({
            "id": format!("{source}.{sp}->{target}.{tp}"), "source": source, "target": target,
            "sourceHandle": sp, "targetHandle": tp
        });
        serde_json::from_value(serde_json::json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "nodes": [
                node("top_db", &[], Value::Null, false, true),
                node("one__in", &[], serde_json::json!({"groupId": "one", "role": "In"}), false, false),
                node("one__out", &[], serde_json::json!({"groupId": "one", "role": "Out"}), false, false),
                node("two__in", &[], serde_json::json!({"groupId": "two", "role": "In"}), false, false),
                node("two__out", &[], serde_json::json!({"groupId": "two", "role": "Out"}), false, false),
                node("Bridge__in", &[], serde_json::json!({"groupId": "Bridge", "role": "In"}), false, false),
                node("Bridge.svc", &["Bridge"], Value::Null, false, true),
                node("Bridge.door", &["Bridge"], Value::Null, true, false),
                node("Bridge__out", &[], serde_json::json!({"groupId": "Bridge", "role": "Out"}), false, false),
            ],
            "edges": [
                edge("top_db", "out", "one__in", "seed"),
                edge("one__in", "seed", "Bridge__in", "seed"),
                edge("two__in", "seed", "Bridge__in", "seed"),
                edge("Bridge__in", "seed", "Bridge.door", "seed"),
                edge("Bridge.svc", "out", "Bridge.door", "address"),
            ],
            "groups": [
                {"id": "one", "kind": "call", "body": "Bridge", "nodeIds": []},
                {"id": "two", "kind": "call", "body": "Bridge", "nodeIds": []},
                {"id": "Bridge", "kind": "body", "nodeIds": ["Bridge.svc", "Bridge.door"]}
            ]
        })).expect("the fixture deserializes")
    }

    /// Both questions are answered per PLACE, spelled: the trigger under
    /// `one` depends on the instance under `one` and on the top-level
    /// node wired into `one`'s door; the trigger under `two` depends on
    /// its own instance alone, because the top-level node feeds only
    /// `one`.
    #[test]
    fn a_file_included_twice_depends_per_call() {
        let p = program_with_two_sites();
        assert_eq!(
            compute_trigger_deps(&p),
            vec![
                ("one.svc".to_string(), "one.door".to_string()),
                ("top_db".to_string(), "one.door".to_string()),
                ("two.svc".to_string(), "two.door".to_string()),
            ]
        );
        assert_eq!(
            ids(infra_triggers_depend_on(&p)),
            vec!["one.svc".to_string(), "top_db".to_string(), "two.svc".to_string()]
        );
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
            label: None,
            config: default_config(),
            position: Position { x: 0.0, y: 0.0 },
            scope: vec!["g".into()],
            group_boundary: None,
            inputs: vec![input.clone()],
            outputs: vec![],
            features: Default::default(),
            requires_infra: false,
            images: vec![],
            fires_with: Default::default(),
            published_service: None,
            span: Some(Span::single_line(1, 0, 5)),
            header_span: Some(Span::single_line(1, 0, 3)),
            config_spans: Default::default(),
            optional_ports: Default::default(),
            port_literals: Default::default(),
            port_literal_spans: Default::default(),
            file_refs: Default::default(),
            include_path: None,
            include_contents: None,
            source_file: None,
        };
        let group = GroupDefinition {
            id: "g".into(),
            kind: GroupKind::Group,
            label: None,
            in_ports: vec![input.port.clone()],
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
            port_literal_spans: Default::default(),
        };
        let edge = Edge {
            id: "e1".into(),
            source: "g.n".into(),
            source_handle: Some("out".into()),
            target: "g.m".into(),
            target_handle: Some("in".into()),
            path: Vec::new(),
            span: None,
            source_file: None,
        };
        let p = ProjectDefinition {
            id: Uuid::nil(),
            nodes: vec![node],
            edges: vec![edge],
            groups: vec![group],
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        let v = serde_json::to_value(&p).unwrap();
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
    }

    fn node_with_infra(id: &str, requires_infra: bool) -> NodeDefinition {
        NodeDefinition {
            id: id.into(),
            node_type: "Any".into(),
            label: None,
            config: default_config(),
            position: Position { x: 0.0, y: 0.0 },
            scope: vec![],
            group_boundary: None,
            inputs: vec![],
            outputs: vec![],
            features: Default::default(),
            requires_infra,
            images: vec![],
            fires_with: Default::default(),
            published_service: None,
            span: None,
            header_span: None,
            config_spans: Default::default(),
            optional_ports: Default::default(),
            port_literals: Default::default(),
            port_literal_spans: Default::default(),
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
}

#[cfg(test)]
mod selection_setup_tests {
    use super::*;
    use super::selection::{RunSelection, SelectionBounds};

    /// (id, is_trigger, requires_infra, scope chain) plus the wires. A
    /// `{g}__in` / `{g}__out` id is a boundary of scope `g`, the way
    /// the compiler flattens one.
    fn project(nodes: &[(&str, bool, bool, &[&str])], edges: &[(&str, &str)]) -> ProjectDefinition {
        let n_json: Vec<serde_json::Value> = nodes
            .iter()
            .map(|(id, is_trigger, requires_infra, scope)| {
                let boundary = id.rsplit_once("__").map(|(group, role)| {
                    serde_json::json!({ "groupId": group, "role": if role == "in" { "In" } else { "Out" } })
                });
                serde_json::json!({
                    "id": id,
                    "nodeType": if boundary.is_some() { "Passthrough" } else { "T" },
                    "label": null,
                    "config": {},
                    "position": { "x": 0, "y": 0 },
                    "scope": scope,
                    "groupBoundary": boundary,
                    "features": { "isTrigger": is_trigger },
                    "requiresInfra": requires_infra,
                })
            })
            .collect();
        let e_json: Vec<serde_json::Value> = edges
            .iter()
            // `sourceHandle` / `targetHandle` are the names `Edge`
            // reads; the older `sourcePort` spelling was dropped
            // silently (no `deny_unknown_fields`) and left every wire
            // default -> default. One port name on both ends, because a
            // boundary's port keeps its name across it (a group's `v`
            // enters `g__in.v` and leaves `g__in.v`), and these tests are
            // about scopes and wires, not about which port feeds which.
            .map(|(s, t)| serde_json::json!({
                "id": format!("e_{s}_{t}"), "source": s, "sourceHandle": "v", "target": t, "targetHandle": "v",
            }))
            .collect();
        let groups: Vec<_> = nodes.iter().filter_map(|(id, _, _, _)| id.strip_suffix("__in"))
            .map(|id| serde_json::json!({"id": id, "kind":"group"})).collect();
        serde_json::from_value(serde_json::json!({
            "id": Uuid::nil(), "nodes": n_json, "edges": e_json, "groups": groups
        }))
        .expect("valid test project")
    }

    fn sorted(set: impl IntoIterator<Item = Located>) -> Vec<String> {
        let mut v: Vec<String> = set.into_iter().map(|place| place.to_string()).collect();
        v.sort();
        v
    }

    fn strs(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    fn places(v: &[&str]) -> Vec<Located> {
        v.iter().map(|s| Located::top(*s)).collect()
    }

    #[test]
    fn an_unwired_group_member_needs_its_gate_but_not_other_input_branches() {
        // cfg ──► g__in ──► g.a ──► g__out
        //         g.seed (unwired body root)
        // Aiming at the unwired seed brings the scope, and the scope's
        // In boundary is fed by `cfg`, which has to run first.
        let p = project(
            &[
                ("cfg", false, false, &[]),
                ("g__in", false, false, &[]),
                ("g.a", false, false, &["g"]),
                ("g.seed", false, false, &["g"]),
                ("g__out", false, false, &[]),
                ("other", false, false, &[]),
            ],
            &[("cfg", "g__in"), ("g__in", "g.a"), ("g.a", "g__out")],
        );
        let selection = RunSelection::setup(&p, &places(&["g.seed"])).unwrap();
        assert_eq!(sorted(selection.nodes.clone()), strs(&["g.seed", "g__in"]));
        assert_eq!(sorted(selection.roots(&p)), strs(&["g.seed", "g__in"]));
    }

    #[test]
    fn the_walk_alternates_wires_and_scopes_until_nothing_new_arrives() {
        // h.x (in scope h) ──► g__in ──► g.a ; aiming at g.a brings g,
        // then g's input source h.x, then h whole, then h's own input.
        let p = project(
            &[
                ("root", false, false, &[]),
                ("h__in", false, false, &[]),
                ("h.x", false, false, &["h"]),
                ("h__out", false, false, &[]),
                ("g__in", false, false, &[]),
                ("g.a", false, false, &["g"]),
                ("g__out", false, false, &[]),
            ],
            &[("root", "h__in"), ("h__in", "h.x"), ("h.x", "h__out"), ("h__out", "g__in"), ("g__in", "g.a"), ("g.a", "g__out")],
        );
        let selection = RunSelection::setup(&p, &places(&["g.a"])).unwrap();
        assert_eq!(sorted(selection.nodes.clone()), strs(&["g.a", "g__in", "h.x", "h__in", "h__out", "root"]));
        assert_eq!(sorted(selection.roots(&p)), strs(&["root"]));
    }

    #[test]
    fn a_trigger_is_included_and_not_walked_through() {
        let p = project(
            &[("setup", false, false, &[]), ("trig", true, false, &[]), ("out", false, false, &[])],
            &[("setup", "trig"), ("trig", "out")],
        );
        let selection = RunSelection::carve(&p, &SelectionBounds { fire: Some("trig".into()), ..Default::default() }).unwrap();
        assert_eq!(sorted(selection.nodes.clone()), strs(&["out", "trig"]));
        assert_eq!(sorted(selection.roots(&p)), strs(&["trig"]));
    }

    #[test]
    fn an_unwired_infra_node_inside_a_group_is_reached_through_its_scope() {
        // The setup phase seeds every infra node; one that sits unwired
        // inside a group is started by the group, so the kick is the
        // group's In boundary, not nothing.
        let p = project(
            &[
                ("g__in", false, false, &[]),
                ("g.db", false, true, &["g"]),
                ("g__out", false, false, &[]),
            ],
            &[],
        );
        let selection = RunSelection::setup(&p, &infra_places(&p)).unwrap();
        assert_eq!(sorted(selection.nodes.clone()), strs(&["g.db", "g__in"]));
        assert_eq!(sorted(selection.roots(&p)), strs(&["g.db", "g__in"]));
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
            &[("text", "compute"), ("compute", "infra"), ("infra", "trigger"), ("trigger", "reply"), ("cfg", "infra_b")],
        );
        let infra = RunSelection::setup(&p, &infra_places(&p)).unwrap();
        assert_eq!(sorted(infra.nodes), strs(&["cfg", "compute", "infra", "infra_b", "text"]));
        let triggers = RunSelection::setup(&p, &trigger_places(&p)).unwrap();
        assert_eq!(sorted(triggers.nodes), strs(&["compute", "infra", "text", "trigger"]));
        let none = project(&[("a", false, false, &[]), ("b", false, false, &[])], &[("a", "b")]);
        assert!(RunSelection::setup(&none, &infra_places(&none)).unwrap().nodes.is_empty());
    }

    #[test]
    fn a_forced_root_is_kicked_wherever_it_sits() {
        let p = project(
            &[("g__in", false, false, &[]), ("g.trig", true, false, &["g"]), ("g__out", false, false, &[])],
            &[],
        );
        let selection = RunSelection::carve(&p, &SelectionBounds { fire: Some("g.trig".into()), ..Default::default() }).unwrap();
        assert_eq!(sorted(selection.roots(&p)), strs(&["g.trig", "g__in"]));
    }
}
