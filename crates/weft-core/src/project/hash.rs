//! The canonical form of a program, and the digests read off it.
//!
//! A `ProjectDefinition` carries more than the runtime graph: source
//! spans, canvas positions, the path a `@file` came from, catalog
//! prose mirrored onto ports. None of that changes what runs, so every
//! digest of the program first strips it (at known structural levels,
//! never inside `config`, where a user value could legitimately use
//! the same key names) and sorts every object's keys. Two digests come
//! off that form:
//!
//! - **`compute_definition_hash`**: the whole program. Identifies the
//!   definition row the worker fetches at claim time and lights the
//!   resync signal.
//! - **`slice_hash`**: one node plus everything upstream of it. Two
//!   programs agree on a node's slice exactly when nothing that feeds
//!   the node changed, which is what lets a seeded run reuse the
//!   node's earlier outcome. The compiler's infra hash folds the same
//!   slice of the infra closure into its own digest.
//!
//! This module lives in core, unfeatured, because three readers need
//! the same canonical form and none may drift: the compiler (build
//! hashes), the dispatcher (which nodes a seeded run may inherit), and
//! the browser parse build (the live-preview definition hash).

use std::collections::{BTreeMap, HashSet};
use std::fmt::Write;

use sha2::{Digest, Sha256};
use serde::{Deserialize, Serialize};

use super::ProjectDefinition;
use crate::frames::Located;

/// A hex-encoded SHA-256 digest. 64 chars.
pub type SourceHash = String;

/// Immutable graph and production-code identity captured with a run.
/// Definition storage remains keyed by its graph hash alone.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProgramIdentity {
    pub definition_hash: SourceHash,
    pub binary_hash: SourceHash,
    pub implementations: BTreeMap<String, SourceHash>,
}

impl ProgramIdentity {
    /// The program a signal was registered by (`signal.program_json`) or a
    /// trigger activated on (`trigger_activation.activation_program`).
    pub const STORED: crate::stored::Kind<ProgramIdentity> = crate::stored::Kind::new("program identity", &[]);

    /// One fixed-size name for this identity, for anything that keys on
    /// it (a table's primary key, a map). The identity itself carries one
    /// hash per compiled node type, so a full-catalog worker makes it
    /// kilobytes long: too big for a Postgres btree entry, and the wrong
    /// thing to compare rows by anyway.
    pub fn digest(&self) -> SourceHash {
        let mut hasher = Sha256::new();
        hasher.update(b"weft-program-identity-v1\n");
        hasher.update(self.definition_hash.as_bytes());
        hasher.update(b"\n");
        hasher.update(self.binary_hash.as_bytes());
        hasher.update(b"\n");
        for (node_type, implementation) in &self.implementations {
            hasher.update(node_type.as_bytes());
            hasher.update(b"=");
            hasher.update(implementation.as_bytes());
            hasher.update(b"\n");
        }
        hex(&hasher.finalize())
    }

    /// Each result depends on its actual data paths and the scopes around
    /// them (their gates decide whether it runs). Whole loops share one
    /// dependency identity and cannot reuse a partial body. One digest per
    /// PLACE: a node of an included file has one per call that reaches it,
    /// since what feeds it there is that call's chain.
    pub fn slice_hashes(&self, project: &ProjectDefinition) -> anyhow::Result<BTreeMap<Located, SourceHash>> {
        anyhow::ensure!(self.definition_hash == compute_definition_hash(project)?, "program identity does not match its graph");
        let graph = super::graph::ProjectGraph::new(project);
        super::places::every_place_in(&graph).into_iter().map(|place| {
            let walked = super::cut::walk_bounds(&graph, &super::cut::Bounds::setup(std::slice::from_ref(&place)));
            let mut ids: HashSet<String> = walked.runs.iter().map(|p| p.id.clone()).collect();
            ids.extend(walked.runs.iter().flat_map(|p| super::places::scopes_around(&graph, p)).map(|scope| scope.id));
            let mut hasher = Sha256::new();
            hasher.update(b"weft-production-slice-v3\n");
            hash_definition_slice(&mut hasher, project, &ids)?;
            // The places themselves: two slices of one id under different
            // calls hold the same nodes and differ in the chain of sites.
            for place in &walked.runs {
                hasher.update(place.to_string().as_bytes());
                hasher.update(b"\n");
            }
            let mut types: Vec<&str> = project.nodes.iter().filter(|node| ids.contains(&node.id)).map(|node| node.node_type.as_str()).collect();
            types.sort_unstable();
            for node_type in types {
                let implementation = self.implementations.get(node_type)
                    .ok_or_else(|| anyhow::anyhow!("program identity is missing implementation '{node_type}'"))?;
                hasher.update(node_type.as_bytes());
                hasher.update(b"=");
                hasher.update(implementation.as_bytes());
                hasher.update(b"\n");
            }
            Ok((place, hex(&hasher.finalize())))
        }).collect()
    }
}

impl ProgramIdentity {
    /// The fingerprint of `node_id`'s own code: its definition in
    /// `project` (its settings, its ports, what is written in its braces)
    /// and the implementation of its type. Two programs agree on it
    /// exactly when the node itself is unchanged, whatever feeds it: what
    /// lets a seeded run that runs a node again replay the answers its
    /// waits were given (`crate::primitive::answered_steps`).
    pub fn node_hash(&self, project: &ProjectDefinition, node_id: &str) -> anyhow::Result<SourceHash> {
        let node = project.nodes.iter().find(|node| node.id == node_id)
            .ok_or_else(|| anyhow::anyhow!("the program has no node '{node_id}'"))?;
        let implementation = self.implementations.get(&node.node_type)
            .ok_or_else(|| anyhow::anyhow!("program identity is missing implementation '{}'", node.node_type))?;
        let mut hasher = Sha256::new();
        hasher.update(b"weft-node-code-v1\n");
        hash_definition_slice(&mut hasher, project, &HashSet::from([node_id.to_string()]))?;
        hasher.update(node.node_type.as_bytes());
        hasher.update(b"=");
        hasher.update(implementation.as_bytes());
        Ok(hex(&hasher.finalize()))
    }
}

/// Hash the runtime project shape: the canonical
/// `ProjectDefinition` (topology + configs + edges + infra flags),
/// serialized deterministically. Flips on every user edit to
/// `main.weft` that affects the runtime graph; does NOT flip on
/// pure-comment / pure-formatting edits or canvas drags, because
/// source spans and layout positions are stripped before hashing.
///
/// Used as the resync drift signal AND as the identity key the
/// worker fetches the definition with at run claim time
/// (`(project_id, definition_hash)`).
///
/// PURE over the definition (no filesystem access): the browser WASM
/// build calls it for live-preview diagnostics, and the dispatcher
/// calls it after compile.
///
/// Non-semantic fields are stripped before hashing, at their known
/// structural levels (never inside `config`, where a user value
/// could legitimately use the same key names):
/// - top level: `createdAt` / `updatedAt` (stamped `Utc::now()` on
///   every compile; hashing them would flip the hash per build).
/// - per node: `span` / `headerSpan` / `configSpans` / `portLiteralSpans`
///   (source text coordinates: comments or formatting shift them without
///   changing the runtime graph) and `position` (canvas layout from a drag).
/// - per node: `fileRefs` (records that a config field came from
///   `@file("path", Type)`; the RESOLVED value already lives in `config`,
///   which IS hashed, so the path here is editor-routing metadata: renaming
///   the file with identical content must not flip the hash; in a built
///   program the value is the content named by its hash
///   (`crate::content`), whose `path` is stripped for the same reason) and
///   `includePath` (an interface-parse-only pointer to an `@include`d file;
///   the file's PATH is non-semantic, its expanded topology is what runs)
///   and `sourceFile` (the absolute path a diagnostic anchors to; the same
///   project compiled from two directories must hash the same).
/// - per edge, and per crossing of its `via`: `span` / `sourceFile`. Per
///   group: `span` / `headerSpan` / `portLiteralSpans` / `sourceFile` /
///   `fileRefs` / `description` (the authored `# ...` comment: prose, not
///   shape).
/// - per port (any of `inputs` / `outputs` / `inPorts` / `outPorts`):
///   `declaredType` (header spelling) and the catalog prose an input
///   mirrors from its metadata: `description`, `label`, `placeholder`.
///   None of it is runtime shape.
///
/// `publishedService` IS hashed, and it is the one enriched field that
/// carries ANOTHER node's metadata (the recipe a publishing node hands
/// connections out against). It has to be: the worker reads the recipe
/// from the definition, and no other hash covers a node the project's
/// graph does not reference. The cost is that editing that recipe, even
/// cosmetically, flips this hash for every project that publishes it.
///
/// The hash is a program's identity, computed ONCE, when the program is
/// made, and never again for a program already stored. When an upgrade
/// respells a stored program's JSON (`crate::project::PROJECT_DEFINITION_UPGRADES`),
/// the program keeps the hash it was written under, and so does every
/// row naming it (`run.definition_hash`, `project.running_definition_hash`,
/// an example's `frozenFrom.definitionHash`): it is still the same
/// program. Hashing the upgraded JSON again can give another hash, so
/// nothing that reads a stored program back checks it against this
/// function: a program is read BY its hash, which is the check.
/// `weft_journal::record::StoredProgram::check` runs only on a program a
/// worker has just made and sends to be kept, and re-hashes it only when
/// it is written at today's version: one made by a worker an older weft
/// built is kept under the hash it claims, at its own version, by the
/// same rule.
pub fn compute_definition_hash(project: &ProjectDefinition) -> anyhow::Result<SourceHash> {
    // Every field named, so a field added to the program does not compile
    // here until it is said whether it is hashed.
    //
    // `id` is the project's DB identity, NOT part of the runtime shape. It is
    // already the OTHER half of the `(project_id, definition_hash)` identity key,
    // so hashing it here too is redundant. Worse, it makes the hash context-
    // dependent: the browser WASM parse computes the live-preview hash with the
    // NIL uuid (the id is not a parse input), while the build/dispatcher computes
    // the stored hash with the real project id. If `id` were hashed, those two
    // would NEVER agree and the "out of sync / resync" light would be stuck on.
    // What does not compile (`broken`) follows from the nodes and wires
    // (a placeholder, a port type), and its wording carries line numbers, so
    // moving a broken line would otherwise read as a new program. A metadata
    // wire never runs: its value is already written into the input it
    // points at (`portLiterals`, hashed with the node), and the wire itself
    // is only drawn.
    let ProjectDefinition { id: _, created_at: _, updated_at: _, broken: _, metadata_wires: _, nodes, edges, groups, defaults, given } =
        project;
    // The canonical JSON of what is left (every strip made, every object's
    // keys sorted: `defaults`, `edges`, `given`, `groups`, `nodes`, each
    // written when serde writes it), streamed into the hasher an element
    // at a time: a program's whole JSON is never held, only one node's.
    let mut out = Hashing(Sha256::new());
    out.0.update(b"weft-definition-v1\n");
    let mut fields = CanonicalObject::open(&mut out);
    if !defaults.is_default() {
        fields.field("defaults")?;
        write_canonical(fields.out, serde_json::to_value(defaults)?)?;
    }
    fields.field("edges")?;
    write_canonical_list(fields.out, edges, canonical_edge)?;
    if !given.is_empty() {
        fields.field("given")?;
        write_canonical(fields.out, serde_json::to_value(given)?)?;
    }
    fields.field("groups")?;
    write_canonical_list(fields.out, groups, canonical_group)?;
    fields.field("nodes")?;
    write_canonical_list(fields.out, nodes, canonical_node)?;
    fields.close();
    Ok(hex(&out.0.finalize()))
}

/// A hasher written to as a byte sink.
struct Hashing(Sha256);

impl std::io::Write for Hashing {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.update(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// A canonical JSON object written field by field, in the order its keys
/// sort, as serde_json writes one compact.
struct CanonicalObject<'w> {
    out: &'w mut Hashing,
    first: bool,
}

impl<'w> CanonicalObject<'w> {
    fn open(out: &'w mut Hashing) -> Self {
        out.0.update(b"{");
        Self { out, first: true }
    }

    /// The next field's key; its value is written next.
    fn field(&mut self, key: &str) -> anyhow::Result<()> {
        if !self.first {
            self.out.0.update(b",");
        }
        self.first = false;
        serde_json::to_writer(&mut *self.out, key)?;
        self.out.0.update(b":");
        Ok(())
    }

    fn close(self) {
        self.out.0.update(b"}");
    }
}

/// `value` in its canonical form ([`canonicalize_key_order`]), written.
fn write_canonical(out: &mut Hashing, mut value: serde_json::Value) -> anyhow::Result<()> {
    canonicalize_key_order(&mut value);
    serde_json::to_writer(out, &value)?;
    Ok(())
}

/// A list, each element serialized on its own, made canonical by
/// `strip`, and written.
fn write_canonical_list<T: Serialize>(
    out: &mut Hashing,
    items: &[T],
    strip: fn(&mut serde_json::Value),
) -> anyhow::Result<()> {
    out.0.update(b"[");
    for (at, item) in items.iter().enumerate() {
        if at > 0 {
            out.0.update(b",");
        }
        let mut value = serde_json::to_value(item)?;
        strip(&mut value);
        write_canonical(out, value)?;
    }
    out.0.update(b"]");
    Ok(())
}

/// Fold `closure`'s slice of the definition into `hasher`: the
/// closure's nodes and groups (ids in `closure`) and the wires between
/// them, in the definition hash's canonical form (same non-semantic
/// strips, see [`compute_definition_hash`]), sorted by id so source order
/// and unrelated graph edits cannot move the digest. Production slice
/// identity and the compiler's infra hash are both this fold under their
/// own prefix.
pub fn hash_definition_slice(
    hasher: &mut Sha256,
    project: &ProjectDefinition,
    closure: &HashSet<String>,
) -> anyhow::Result<()> {
    let sorted = |mut values: Vec<serde_json::Value>| {
        values.sort_by(|a, b| a["id"].as_str().cmp(&b["id"].as_str()));
        values
    };
    let nodes: Vec<serde_json::Value> = project
        .nodes
        .iter()
        .filter(|n| closure.contains(&n.id))
        .map(serde_json::to_value)
        .collect::<Result<_, _>>()
        .map_err(|e| anyhow::anyhow!("serialize slice node: {e}"))?;
    let groups: Vec<serde_json::Value> = project
        .groups
        .iter()
        .filter(|g| closure.contains(&g.id))
        .map(serde_json::to_value)
        .collect::<Result<_, _>>()
        .map_err(|e| anyhow::anyhow!("serialize slice group: {e}"))?;
    let edges: Vec<serde_json::Value> = project
        .edges
        .iter()
        .filter(|e| closure.contains(&e.source) && closure.contains(&e.target))
        .map(serde_json::to_value)
        .collect::<Result<_, _>>()
        .map_err(|e| anyhow::anyhow!("serialize slice edge: {e}"))?;
    let canonical = |mut values: Vec<serde_json::Value>, strip: fn(&mut serde_json::Value)| {
        values.iter_mut().for_each(strip);
        sorted(values)
    };
    let mut slice = serde_json::json!({
        "nodes": canonical(nodes, canonical_node),
        "groups": canonical(groups, canonical_group),
        "edges": canonical(edges, canonical_edge),
    });
    canonicalize_key_order(&mut slice);
    hasher.update(b"definition-slice:");
    hasher.update(
        &serde_json::to_vec(&slice).map_err(|e| anyhow::anyhow!("serialize slice: {e}"))?,
    );
    hasher.update(b"\n");
    Ok(())
}

/// The non-semantic keys stripped before hashing, shared by
/// [`compute_definition_hash`] and [`hash_definition_slice`] so the two
/// digests can never disagree on what "semantic" means.
const NODE_HASH_STRIPS: &[&str] = &[
    "span",
    "headerSpan",
    "configSpans",
    "portLiteralSpans",
    "position",
    "fileRefs",
    "includePath",
    "sourceFile",
    // What a metadata port holds reaches a run only through the inputs
    // its wires write into, which are hashed there.
    "metadataPorts",
];
const EDGE_HASH_STRIPS: &[&str] = &["span", "sourceFile"];
// `description` is the group's authored `# ...` comment: prose, never
// the runtime shape, so rewording it must not light a resync. `fileRefs`
// is a node's own strip, for a literal on a group's port.
// `metadataPorts` is a node's own strip.
const GROUP_HASH_STRIPS: &[&str] = &["span", "headerSpan", "portLiteralSpans", "sourceFile", "description", "fileRefs", "metadataPorts"];

/// A node's serialized value stripped of what is not its runtime shape:
/// [`NODE_HASH_STRIPS`], each port's presentation, and the path of every
/// content its values name.
fn canonical_node(node: &mut serde_json::Value) {
    strip_element(node, NODE_HASH_STRIPS);
    strip_ports(node);
    strip_content_paths(node);
}

/// A group's, as [`canonical_node`] with [`GROUP_HASH_STRIPS`].
fn canonical_group(group: &mut serde_json::Value) {
    strip_element(group, GROUP_HASH_STRIPS);
    strip_ports(group);
    strip_content_paths(group);
}

/// A wire's: [`EDGE_HASH_STRIPS`], and each of its crossings' (a
/// crossing's `span` and `sourceFile`, the same strips as the wire's
/// own): formatting the source must not move a digest.
fn canonical_edge(edge: &mut serde_json::Value) {
    strip_element(edge, EDGE_HASH_STRIPS);
    if let Some(serde_json::Value::Array(via)) = edge.get_mut("via") {
        via.iter_mut().for_each(|crossing| strip_element(crossing, EDGE_HASH_STRIPS));
    }
}

/// Remove the per-port PRESENTATION members from every port list of
/// every element. `declaredType` records how the source HEADER spells a
/// port (the editor's round-trip anchor); `description`, `label` and
/// `placeholder` are catalog prose mirrored onto the instance. None is
/// the runtime shape, so the same runtime graph written with or without
/// a redundant header line, or against a catalog whose wording changed,
/// must hash identically (the editor's header healing, or a reworded
/// label, would otherwise flip the hash and light a resync for a
/// cosmetic change).
const PORT_HASH_STRIPS: &[&str] = &["declaredType", "description", "label", "placeholder"];
fn strip_ports(item: &mut serde_json::Value) {
    for list in ["inputs", "outputs", "inPorts", "outPorts"] {
        if let Some(serde_json::Value::Array(ports)) = item.get_mut(list) {
            ports.iter_mut().for_each(|port| strip_element(port, PORT_HASH_STRIPS));
        }
    }
}

/// The path a content a build named (`crate::content`) was read from is
/// where it came from, like a `fileRefs` record: renaming a file with
/// the same content names the same content, and must not flip the hash.
fn strip_content_paths(value: &mut serde_json::Value) {
    if crate::content::ContentRef::from_value(value).is_some() {
        if let Some(payload) = value.get_mut(crate::content::CONTENT_MARKER_KEY).and_then(serde_json::Value::as_object_mut) {
            payload.remove("path");
        }
        return;
    }
    match value {
        serde_json::Value::Object(fields) => fields.values_mut().for_each(strip_content_paths),
        serde_json::Value::Array(items) => items.iter_mut().for_each(strip_content_paths),
        _ => {}
    }
}

/// Remove `keys` from one element's object. Top level only: deliberately
/// does NOT recurse into `config`.
fn strip_element(item: &mut serde_json::Value, keys: &[&str]) {
    if let Some(obj) = item.as_object_mut() {
        for key in keys {
            obj.remove(*key);
        }
    }
}

/// Rebuild every object in `value`, recursively, with its keys sorted
/// by name. Hashed JSON MUST pass through this before serializing:
/// serde_json's map is a BTreeMap (already sorted) by default but an
/// insertion-ordered map under its `preserve_order` feature, and that
/// feature flips with the build (`minillmlib` enables it, feature
/// unification spreads it to any build that includes `weft-providers`,
/// while a build without it, like the standalone CLI's, leaves it off).
/// Without this normalization the same project would hash differently
/// depending on which binary computed it, and the "out of sync /
/// resync" light would be stuck on between them.
///
/// Also the canonical form of any JSON compared for equality across
/// builds (a kick payload against the seed's, a version manifest).
pub fn canonicalize_key_order(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(obj) => {
            let mut entries: Vec<(String, serde_json::Value)> = std::mem::take(obj).into_iter().collect();
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            for (_, v) in entries.iter_mut() {
                canonicalize_key_order(v);
            }
            *obj = entries.into_iter().collect();
        }
        serde_json::Value::Array(items) => {
            for item in items {
                canonicalize_key_order(item);
            }
        }
        _ => {}
    }
}

/// `value` serialized in its canonical form: keys sorted at every
/// depth, no whitespace. Two values are the same fact exactly when
/// their canonical bytes agree, whichever build produced them.
pub fn canonical_json(value: &serde_json::Value) -> String {
    let mut v = value.clone();
    canonicalize_key_order(&mut v);
    serde_json::to_string(&v).expect("a serde_json::Value always serializes")
}

/// A manifest: every covered file of a project, `path -> sha256`, the
/// standard library under `nodes/base_catalog/` included like any other.
pub type Manifest = BTreeMap<String, String>;

/// The id of the version a manifest (`path -> sha256`) describes: the
/// sha256 of its canonical JSON. THE one derivation, read by the CLI
/// (to know whether the files on disk are a version the tree holds)
/// and by the dispatcher (which never trusts a sent id).
pub fn manifest_version_id(manifest: &BTreeMap<String, String>) -> SourceHash {
    let value = serde_json::to_value(manifest).expect("a BTreeMap<String, String> serializes");
    sha256_hex(canonical_json(&value).as_bytes())
}

/// SHA-256 of `bytes`, hex-encoded.
pub fn sha256_hex(bytes: &[u8]) -> SourceHash {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex(&hasher.finalize())
}

pub fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        // write! into the reused buffer, no per-byte String allocation.
        let _ = write!(s, "{b:02x}");
    }
    s
}

#[cfg(test)]
mod slice_tests {
    use super::*;
    use crate::project::{ConfigFieldSpan, Span};

    #[test]
    fn a_program_identity_digest_is_fixed_size_and_moves_with_any_part() {
        let identity = ProgramIdentity {
            definition_hash: "graph".into(), binary_hash: "worker".into(),
            implementations: (0..400).map(|i| (format!("Node{i}"), format!("{i:064x}"))).collect(),
        };
        let digest = identity.digest();
        assert_eq!(digest.len(), 64, "kilobytes of implementations key as one 64-char digest");
        assert_eq!(identity.clone().digest(), digest);
        let mut changed = identity.clone();
        changed.implementations.insert("Node7".into(), "edited".into());
        assert_ne!(changed.digest(), digest);
        let mut changed = identity.clone();
        changed.binary_hash = "other-worker".into();
        assert_ne!(changed.digest(), digest);
        let mut changed = identity;
        changed.definition_hash = "other-graph".into();
        assert_ne!(changed.digest(), digest);
    }

    #[test]
    fn production_slices_follow_code_dependencies_and_ignore_order() {
        let mut graph = project("a", "b", "x", 0.0);
        for node in &mut graph.nodes { node.node_type = node.id.clone(); }
        let identity = ProgramIdentity {
            definition_hash: compute_definition_hash(&graph).unwrap(), binary_hash: "worker".into(),
            implementations: [("a".into(), "a1".into()), ("b".into(), "b1".into()), ("x".into(), "x1".into())].into_iter().collect(),
        };
        let original = identity.slice_hashes(&graph).unwrap();
        let mut changed = identity.clone();
        changed.implementations.insert("b".into(), "b2".into());
        let hashes = changed.slice_hashes(&graph).unwrap();
        assert_eq!(hashes[&Located::top("a")], original[&Located::top("a")]);
        assert_eq!(hashes[&Located::top("x")], original[&Located::top("x")]);
        assert_ne!(hashes[&Located::top("b")], original[&Located::top("b")]);
        changed.implementations.insert("a".into(), "a2".into());
        assert_ne!(changed.slice_hashes(&graph).unwrap()[&Located::top("b")], hashes[&Located::top("b")]);
        graph.nodes.reverse();
        changed.definition_hash = compute_definition_hash(&graph).unwrap();
        let reversed = changed.slice_hashes(&graph).unwrap();
        graph.nodes.reverse();
        changed.definition_hash = compute_definition_hash(&graph).unwrap();
        assert_eq!(changed.slice_hashes(&graph).unwrap(), reversed);
        changed.implementations.remove("a");
        assert!(changed.slice_hashes(&graph).unwrap_err().to_string().contains("missing implementation"));
    }

    /// Three-node project: `a -> b`, plus an unrelated `x`. The caller
    /// varies one node's config to probe what moves a slice digest.
    fn project(a_cfg: &str, b_cfg: &str, x_cfg: &str, b_x: f64) -> ProjectDefinition {
        project_wired(a_cfg, b_cfg, x_cfg, b_x, &[("a", "b")])
    }

    fn project_wired(
        a_cfg: &str,
        b_cfg: &str,
        x_cfg: &str,
        b_x: f64,
        wires: &[(&str, &str)],
    ) -> ProjectDefinition {
        let node = |id: &str, cfg: &str, infra: bool, x: f64| {
            serde_json::json!({
                "id": id, "nodeType": "T", "label": null,
                "config": {"v": cfg},
                "position": {"x": x, "y": 0.0},
                "inputs": [], "outputs": [], "features": {},
                "scope": [], "groupBoundary": null,
                "requiresInfra": infra, "images": [],
            })
        };
        let edges: Vec<serde_json::Value> = wires
            .iter()
            .map(|(s, t)| {
                serde_json::json!({
                    "id": format!("{s}.out->{t}.in"), "source": s, "target": t,
                    "sourceHandle": "out", "targetHandle": "in",
                })
            })
            .collect();
        serde_json::from_value(serde_json::json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "nodes": [
                node("a", a_cfg, false, 0.0),
                node("b", b_cfg, true, b_x),
                node("x", x_cfg, false, 0.0),
            ],
            "edges": edges,
            "groups": [],
            "createdAt": "2026-01-01T00:00:00Z",
            "updatedAt": "2026-01-01T00:00:00Z",
        }))
        .expect("test ProjectDefinition")
    }

    fn slice_of(p: &ProjectDefinition, node: &str) -> String {
        ProgramIdentity {
            definition_hash: compute_definition_hash(p).unwrap(), binary_hash: "worker".into(),
            implementations: p.nodes.iter().map(|node| (node.node_type.clone(), "code".into())).collect(),
        }.slice_hashes(p).unwrap()[&Located::top(node)].clone()
    }

    #[test]
    fn a_config_edit_outside_the_slice_does_not_move_the_hash() {
        assert_eq!(
            slice_of(&project("a1", "b1", "x1", 0.0), "b"),
            slice_of(&project("a1", "b1", "CHANGED", 0.0), "b"),
        );
    }

    #[test]
    fn an_edit_anywhere_in_the_slice_moves_the_hash() {
        let base = slice_of(&project("a1", "b1", "x1", 0.0), "b");
        assert_ne!(base, slice_of(&project("CHANGED", "b1", "x1", 0.0), "b"));
        assert_ne!(base, slice_of(&project("a1", "CHANGED", "x1", 0.0), "b"));
    }

    #[test]
    fn a_canvas_drag_does_not_move_the_hash() {
        assert_eq!(
            slice_of(&project("a1", "b1", "x1", 0.0), "b"),
            slice_of(&project("a1", "b1", "x1", 500.0), "b"),
        );
    }

    /// The whole point of a slice: editing `b` leaves `a`'s slice where
    /// it was, so a seeded run keeps `a` and re-runs `b`.
    #[test]
    fn a_downstream_edit_does_not_move_an_upstream_slice() {
        assert_eq!(
            slice_of(&project("a1", "b1", "x1", 0.0), "a"),
            slice_of(&project("a1", "CHANGED", "x1", 0.0), "a"),
        );
    }

    /// Rewiring `x` into `b` changes what `b` reads, so `b`'s slice
    /// moves even though no config did.
    #[test]
    fn an_incoming_rewire_moves_the_target_slice() {
        let before = project_wired("a1", "b1", "x1", 0.0, &[("a", "b")]);
        let after = project_wired("a1", "b1", "x1", 0.0, &[("a", "b"), ("x", "b")]);
        assert_ne!(slice_of(&before, "b"), slice_of(&after, "b"));
        assert_eq!(slice_of(&before, "a"), slice_of(&after, "a"));
    }

    /// A literal's SPAN is a source-text coordinate: a comment added
    /// above the node shifts it without changing the graph.
    #[test]
    fn a_shifted_port_literal_span_does_not_move_the_hash() {
        let mut with_span = project("a1", "b1", "x1", 0.0);
        with_span.nodes[1].port_literals.insert("v".into(), serde_json::json!("lit"));
        let mut shifted = with_span.clone();
        let span_at = |line| ConfigFieldSpan::inline(Span {
            start_line: line,
            start_column: 3,
            end_line: line,
            end_column: 9,
        });
        with_span.nodes[1].port_literal_spans.insert("v".into(), span_at(2));
        shifted.nodes[1].port_literal_spans.insert("v".into(), span_at(7));
        assert_eq!(slice_of(&with_span, "b"), slice_of(&shifted, "b"));
        assert_eq!(
            compute_definition_hash(&with_span).unwrap(),
            compute_definition_hash(&shifted).unwrap(),
        );
    }

    /// A metadata wire and the metadata ports are only drawn: what a run
    /// reads of them is the literal the wire wrote into its input, which
    /// moves the hash like any written value.
    #[test]
    fn a_metadata_wire_moves_the_hash_only_through_what_it_wrote() {
        let base = project("a1", "b1", "x1", 0.0);
        let mut drawn = base.clone();
        drawn.metadata_wires = base.edges.clone();
        drawn.nodes[0].metadata_ports.push(crate::project::MetadataPort {
            name: "tool".into(),
            port_type: crate::weft_type::WeftType::primitive(crate::weft_type::WeftPrimitive::String),
            description: None,
            value: Some(serde_json::json!("about a")),
            written: None,
            span: None,
            declared_type: None,
        });
        assert_eq!(compute_definition_hash(&base).unwrap(), compute_definition_hash(&drawn).unwrap());
        assert_eq!(slice_of(&base, "b"), slice_of(&drawn, "b"));
        let mut written = drawn.clone();
        written.nodes[1].port_literals.insert("tool".into(), serde_json::json!("about a"));
        assert_ne!(compute_definition_hash(&drawn).unwrap(), compute_definition_hash(&written).unwrap());
        assert_ne!(slice_of(&drawn, "b"), slice_of(&written, "b"));
    }

    /// The definition hash as it was computed before it was streamed: the
    /// whole program as one JSON value, stripped and sorted, then written
    /// out. The streamed one must give the same digest, or every stored
    /// program and running hash would read as another program.
    fn whole_value_hash(project: &ProjectDefinition) -> String {
        let mut value = serde_json::to_value(project).unwrap();
        let obj = value.as_object_mut().unwrap();
        for key in ["id", "createdAt", "updatedAt", "broken", "metadataWires"] {
            obj.remove(key);
        }
        for (list, strip) in [("nodes", canonical_node as fn(&mut serde_json::Value)), ("edges", canonical_edge), ("groups", canonical_group)] {
            if let Some(serde_json::Value::Array(items)) = obj.get_mut(list) {
                items.iter_mut().for_each(strip);
            }
        }
        canonicalize_key_order(&mut value);
        let mut hasher = Sha256::new();
        hasher.update(b"weft-definition-v1\n");
        hasher.update(serde_json::to_vec(&value).unwrap());
        hex(&hasher.finalize())
    }

    /// The streamed hash is the whole-value one, with every top-level
    /// field present or left out, every list empty or not.
    #[test]
    fn the_streamed_definition_hash_is_the_whole_value_one() {
        let mut full = project_wired("a1", "b1", "x1", 0.0, &[("a", "b"), ("b", "x")]);
        assert_eq!(compute_definition_hash(&full).unwrap(), whole_value_hash(&full));
        full.nodes[0].inputs.push(serde_json::from_value(serde_json::json!({
            "name": "in", "portType": "String", "required": false, "description": "prose", "label": "In", "declaredType": "String"
        })).unwrap());
        full.nodes[0].port_literals.insert("in".into(), serde_json::json!({ "z": [1, { "b": 2, "a": 1 }], "a": "\"quoted\"" }));
        full.groups = serde_json::from_value(serde_json::json!([{ "id": "g", "kind": "body", "nodeIds": [], "description": "a group" }])).unwrap();
        full.given.outputs.insert(crate::frames::Located::top("x"));
        full.defaults.keep_for = Some(crate::run_settings::KeepFor::WEFT_DEFAULT);
        full.metadata_wires = full.edges.clone();
        assert_eq!(compute_definition_hash(&full).unwrap(), whole_value_hash(&full));
        let mut empty = full.clone();
        empty.nodes.clear();
        empty.edges.clear();
        empty.groups.clear();
        assert_eq!(compute_definition_hash(&empty).unwrap(), whole_value_hash(&empty));
    }

    /// A value named by its content hashes by the content and its type:
    /// the same content read from another path is the same program,
    /// another content another one.
    #[test]
    fn a_named_content_hashes_by_what_it_names_not_where_it_was_read() {
        let string = crate::weft_type::WeftType::primitive(crate::weft_type::WeftPrimitive::String);
        let with = |text: &str, path: &str| {
            let mut program = project("a1", "b1", "x1", 0.0);
            let (content, _) = crate::content::ContentRef::of_text(text, &string, path).unwrap();
            program.nodes[1].port_literals.insert("query".into(), serde_json::json!([content.to_value()]));
            program
        };
        let hash = |program: &ProjectDefinition| compute_definition_hash(program).unwrap();
        assert_eq!(hash(&with("SELECT 1;", "sql/a.sql")), hash(&with("SELECT 1;", "sql/renamed.sql")));
        assert_ne!(hash(&with("SELECT 1;", "sql/a.sql")), hash(&with("SELECT 2;", "sql/a.sql")));
        assert_eq!(slice_of(&with("SELECT 1;", "sql/a.sql"), "b"), slice_of(&with("SELECT 1;", "sql/renamed.sql"), "b"));
    }

    #[test]
    fn canonical_json_ignores_key_insertion_order() {
        let mut a = serde_json::json!({});
        a["zeta"] = serde_json::json!({ "y": 1, "x": [{ "b": 2, "a": 3 }] });
        a["alpha"] = serde_json::json!(true);
        let mut b = serde_json::json!({});
        b["alpha"] = serde_json::json!(true);
        b["zeta"] = serde_json::json!({ "x": [{ "a": 3, "b": 2 }], "y": 1 });
        assert_eq!(canonical_json(&a), canonical_json(&b));
        assert_ne!(canonical_json(&a), canonical_json(&serde_json::json!({ "alpha": false })));
    }
}
