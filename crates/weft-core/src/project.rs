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
// SYNC: GroupKind <-> extension-vscode/src/shared/protocol.ts GroupDefinition (kind + loopConfig)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum GroupKind {
    Group,
    Loop {
        #[serde(rename = "loopConfig")]
        loop_config: serde_json::Value,
    },
}

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
    #[serde(default, rename = "headerSpan", skip_serializing_if = "Option::is_none")]
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
    /// The node's header span, or a default (zero) span when it has none
    /// (synthetic nodes carry no header). The single home for "point a
    /// diagnostic at this node's header line", so every diagnostic site uses
    /// the same fallback rather than repeating `header_span.unwrap_or_default()`.
    pub fn header_span_or_default(&self) -> Span {
        self.header_span.unwrap_or_default()
    }

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
    /// Whether this port can be filled by a same-named config field on
    /// the node instead of a wired edge.
    #[serde(default = "default_configurable")]
    pub configurable: bool,
    /// True for the auto-synthesized INPUT half of a loop carry port.
    /// The editor uses this to render it as a non-editable ghost mirroring
    /// the carry output of the same name. Never set on a user-declared port.
    #[serde(default, rename = "synthesizedFromCarry", skip_serializing_if = "std::ops::Not::not")]
    pub synthesized_from_carry: bool,
}

fn default_configurable() -> bool { true }

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
        let port = PortDefinition {
            name: "inp".into(),
            port_type: WeftType::primitive(crate::weft_type::WeftPrimitive::String),
            required: true,
            description: None,
            configurable: false,
            synthesized_from_carry: false,
        };
        let node = NodeDefinition {
            id: "g.n".into(),
            node_type: "Llm".into(),
            label: None,
            config: default_config(),
            position: Position { x: 0.0, y: 0.0 },
            scope: vec!["g".into()],
            group_boundary: None,
            inputs: vec![port.clone()],
            outputs: vec![],
            features: Default::default(),
            requires_infra: false,
            images: vec![],
            span: Some(Span::single_line(1, 0, 5)),
            header_span: Some(Span::single_line(1, 0, 3)),
            config_spans: Default::default(),
            file_refs: Default::default(),
            include_path: None,
        };
        let group = GroupDefinition {
            id: "g".into(),
            kind: GroupKind::Group,
            label: None,
            in_ports: vec![port],
            out_ports: vec![],
            one_of_required: vec![],
            parent_group_id: None,
            child_group_ids: vec![],
            node_ids: vec!["g.n".into()],
            anonymous: false,
            span: None,
            header_span: None,
        };
        let edge = Edge {
            id: "e1".into(),
            source: "g.n".into(),
            source_handle: Some("out".into()),
            target: "g.m".into(),
            target_handle: Some("in".into()),
            span: None,
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
        // The renamed keys the TS side depends on:
        assert!(v["nodes"][0].get("nodeType").is_some(), "nodeType key: {v}");
        assert!(v["nodes"][0]["inputs"][0].get("portType").is_some(), "portType key: {v}");
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
            span: None,
            header_span: None,
            config_spans: Default::default(),
            file_refs: Default::default(),
            include_path: None,
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
