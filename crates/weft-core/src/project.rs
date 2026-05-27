//! Graph-level project types. Describes a weft program as a graph:
//! nodes (instances of a node type), edges (connections between port
//! refs). Port and field shapes live on the node TYPE (NodeMetadata),
//! not on the instance. `NodeFeatures` is preserved on each node.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::weft_type::WeftType;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectDefinition {
    pub id: Uuid,
    pub name: String,
    pub description: Option<String>,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupDefinition {
    pub id: String,
    /// Optional user-facing label. Defaults to id if missing.
    pub label: Option<String>,
    /// External input ports (the ports outside the group connects to).
    #[serde(rename = "inPorts", default)]
    pub in_ports: Vec<PortDefinition>,
    /// External output ports.
    #[serde(rename = "outPorts", default)]
    pub out_ports: Vec<PortDefinition>,
    /// @require_one_of groups declared on the interface.
    #[serde(rename = "oneOfRequired", default)]
    pub one_of_required: Vec<Vec<String>>,
    /// Parent group id for nested groups. None for top-level groups.
    #[serde(rename = "parentGroupId", default)]
    pub parent_group_id: Option<String>,
    /// Ids of child groups (first-level only; nested groups carry
    /// their own entry with `parent_group_id` set).
    #[serde(rename = "childGroupIds", default)]
    pub child_group_ids: Vec<String>,
    /// Ids of member nodes (only direct children, not grandchildren).
    /// Does NOT include the In/Out boundary Passthroughs.
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Span {
    pub start_line: usize,
    pub start_col: usize,
    pub end_line: usize,
    pub end_col: usize,
}

impl Span {
    pub fn single_line(line: usize, start_col: usize, end_col: usize) -> Self {
        Self { start_line: line, start_col, end_line: line, end_col }
    }
}

/// Where a config field's value was written in source. The editor needs this
/// to rewrite a field in place: an inline field (`n = Type { k: v }`) is
/// rewritten as `k: v`, a connection-line field (`n.k = v`) keeps its
/// `n.k = ` prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ConfigOrigin {
    Inline,
    Connection,
}

/// Source range of one config field plus how it was written. The editor edits
/// a single field surgically using `span`, and uses `origin` to reconstruct
/// the correct line prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigFieldSpan {
    pub span: Span,
    pub origin: ConfigOrigin,
}

impl ConfigFieldSpan {
    pub fn inline(span: Span) -> Self {
        Self { span, origin: ConfigOrigin::Inline }
    }
    pub fn connection(span: Span) -> Self {
        Self { span, origin: ConfigOrigin::Connection }
    }
}

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
    /// Enriched input ports. Empty before compile.
    #[serde(default)]
    pub inputs: Vec<PortDefinition>,
    /// Enriched output ports. Empty before compile.
    #[serde(default)]
    pub outputs: Vec<PortDefinition>,
    /// Enriched node-level features (one_of_required, etc). Mirrored
    /// from NodeMetadata at compile time so the scheduler doesn't
    /// need a registry lookup per node.
    #[serde(default)]
    pub features: crate::node::NodeFeatures,
    /// `true` if this node implements `Node::provision` and the
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
    /// Full source range of the node declaration (including config
    /// block if present). Set by the parser.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub span: Option<Span>,
    /// Source range of the node's header (`id = NodeType`), the part
    /// before the `{` config block. Used when adding a config field
    /// to a bare node.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub header_span: Option<Span>,
    /// Per-config-field source ranges, keyed by field name. Each range
    /// covers the `key: value` pair including trailing comma. Used to
    /// surgically edit one field without re-serializing the whole
    /// config block.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty", rename = "configSpans")]
    pub config_spans: std::collections::BTreeMap<String, ConfigFieldSpan>,
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
}

/// A `@file("path", Type)` reference attached to a config field: where the
/// value was read from and the type it was cast to. Serializes as
/// `{ "path": "...", "type": "String" }`. Lives in weft-core because it
/// flows on the parse wire to the editor.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FileRef {
    pub path: String,
    #[serde(rename = "type")]
    pub ty: WeftType,
}

fn default_config() -> Value {
    Value::Object(Default::default())
}

impl NodeDefinition {
    /// Resolved `_is_output` for this node instance. Reads
    /// `config._is_output` (explicit author override) if set, else
    /// falls back to `features.is_output_default` (node-type default).
    ///
    /// Load-bearing: the dispatcher collects every `is_output()` node
    /// when computing the run subgraph (see docs/v2-design.md section
    /// 3.0). Flipping this bit changes what the runtime considers a
    /// "production target" of a run.
    pub fn is_output(&self) -> bool {
        if let Some(v) = self.config.get("_is_output").and_then(|v| v.as_bool()) {
            return v;
        }
        self.features.is_output_default
    }

    /// Tags from `_tags` config. Used for token-scoped enumeration
    /// (a token with `allowed_tags` only sees signals tagged with
    /// at least one of those tags). Validated at parse time:
    /// each tag matches `[A-Za-z0-9_-]{1,64}`.
    pub fn tags(&self) -> Vec<String> {
        self.config
            .get("_tags")
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupBoundary {
    #[serde(rename = "groupId")]
    pub group_id: String,
    pub role: GroupBoundaryRole,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum LaneMode {
    #[default]
    Single,
    Expand,
    Gather,
}

/// Port shape on a NODE INSTANCE. This is the enriched version:
/// TypeVars resolved, derived ports materialized, configurable
/// defaults applied. Nodes declare their ports via NodeMetadata; the
/// compiler resolves them and stores enriched port lists on the
/// corresponding NodeDefinition at compile time. Runtime executes
/// against these enriched ports.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortDefinition {
    pub name: String,
    #[serde(rename = "portType")]
    pub port_type: WeftType,
    pub required: bool,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default, rename = "laneMode")]
    pub lane_mode: LaneMode,
    /// Number of List[] levels to expand/gather. Default 1.
    #[serde(default = "default_lane_depth", rename = "laneDepth")]
    pub lane_depth: u32,
    /// Whether this port can be filled by a same-named config field on
    /// the node instead of a wired edge.
    #[serde(default = "default_configurable")]
    pub configurable: bool,
    /// True when the user explicitly typed this port in the .weft
    /// source (inline `NodeType(x: T) -> (y: U)` syntax). False
    /// when the type came from the catalog metadata. Used by
    /// validate to suppress the implicit-expand / implicit-gather
    /// warnings on edges where BOTH ends are user-typed: the user
    /// wrote the depth difference deliberately, no surprise to
    /// warn about. Catalog-typed ports keep the warning so a user
    /// wiring two pre-typed ports sees the inferred lane mechanic.
    #[serde(default, rename = "userTyped", skip_serializing_if = "is_false")]
    pub user_typed: bool,
}

fn default_lane_depth() -> u32 { 1 }
fn default_configurable() -> bool { true }
fn is_false(b: &bool) -> bool { !*b }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    pub x: f64,
    pub y: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Edge {
    pub id: String,
    pub source: String,
    pub target: String,
    #[serde(rename = "sourceHandle")]
    pub source_handle: Option<String>,
    #[serde(rename = "targetHandle")]
    pub target_handle: Option<String>,
    /// Source range of the connection line (`target.port = source.port`).
    /// Used by tooling to remove or rewrite the edge.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub span: Option<Span>,
}

/// Pre-indexed edge lookups. Build once per compiled project, use
/// many times during execution.
pub struct EdgeIndex {
    outgoing: std::collections::HashMap<String, Vec<usize>>,
    incoming: std::collections::HashMap<String, Vec<usize>>,
}

impl EdgeIndex {
    pub fn build(project: &ProjectDefinition) -> Self {
        let mut outgoing: std::collections::HashMap<String, Vec<usize>> = std::collections::HashMap::new();
        let mut incoming: std::collections::HashMap<String, Vec<usize>> = std::collections::HashMap::new();
        for (i, edge) in project.edges.iter().enumerate() {
            outgoing.entry(edge.source.clone()).or_default().push(i);
            incoming.entry(edge.target.clone()).or_default().push(i);
        }
        Self { outgoing, incoming }
    }

    pub fn get_outgoing<'a>(&self, project: &'a ProjectDefinition, node_id: &str) -> Vec<&'a Edge> {
        self.outgoing.get(node_id)
            .map(|indices| indices.iter().map(|&i| &project.edges[i]).collect())
            .unwrap_or_default()
    }

    pub fn get_incoming<'a>(&self, project: &'a ProjectDefinition, node_id: &str) -> Vec<&'a Edge> {
        self.incoming.get(node_id)
            .map(|indices| indices.iter().map(|&i| &project.edges[i]).collect())
            .unwrap_or_default()
    }
}

/// For every `requires_infra` node, find every trigger whose
/// upstream-closure includes it. Returns `(infra_node_id,
/// trigger_node_id)` pairs sorted by `(infra, trigger)`.
///
/// Used by:
///   - dispatcher's per-node stop/terminate safety check (refuse if a
///     trigger depends on the targeted infra node);
///   - broker's `supervisor_trigger_deps` endpoint so the supervisor
///     can make the same decision when reacting to flaky/recovered
///     events.
///
/// Pure walk over `ProjectDefinition`; no I/O. The same function
/// hosted by `weft-core` is the only copy : neither side maintains
/// a mirror.
pub fn compute_trigger_deps(project: &ProjectDefinition) -> Vec<(String, String)> {
    let edge_idx = EdgeIndex::build(project);
    let triggers: Vec<&str> = project
        .nodes
        .iter()
        .filter(|n| n.features.is_trigger)
        .map(|n| n.id.as_str())
        .collect();
    let mut out: Vec<(String, String)> = Vec::new();
    for infra in project.nodes.iter().filter(|n| n.requires_infra) {
        for trigger in &triggers {
            let mut visited: std::collections::HashSet<String> =
                std::collections::HashSet::new();
            let mut frontier: Vec<String> = vec![(*trigger).to_string()];
            let mut reached = false;
            while let Some(id) = frontier.pop() {
                if !visited.insert(id.clone()) {
                    continue;
                }
                if id == infra.id {
                    reached = true;
                    break;
                }
                for e in edge_idx.get_incoming(project, &id) {
                    if !visited.contains(&e.source) {
                        frontier.push(e.source.clone());
                    }
                }
            }
            if reached {
                out.push((infra.id.clone(), (*trigger).to_string()));
            }
        }
    }
    out.sort();
    out
}
