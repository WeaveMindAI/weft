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

/// Immutable graph and production-code identity captured with an execution.
/// Definition storage remains keyed by its graph hash alone.
// SYNC: ProgramIdentity <-> packages/weft-graph/src/run-spec.ts ProgramIdentity
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProgramIdentity {
    pub definition_hash: SourceHash,
    pub binary_hash: SourceHash,
    pub implementations: BTreeMap<String, SourceHash>,
}

impl ProgramIdentity {
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

    /// Each result depends on its actual data paths and enclosing controls.
    /// Whole loops share one dependency identity and cannot reuse a partial body.
    /// One digest per PLACE: a node of an included file has one per call
    /// that reaches it, since what feeds it there is that call's chain.
    pub fn slice_hashes(&self, project: &ProjectDefinition) -> anyhow::Result<BTreeMap<Located, SourceHash>> {
        anyhow::ensure!(self.definition_hash == compute_definition_hash(project)?, "program identity does not match its graph");
        super::selection::every_place(project).into_iter().map(|place| {
            let selection = super::selection::RunSelection::dependencies(project, std::slice::from_ref(&place));
            let ids: HashSet<String> = selection.nodes.iter().map(|p| p.id.clone()).collect();
            let mut slice = project.clone();
            slice.nodes.retain(|node| ids.contains(&node.id));
            slice.nodes.sort_by(|a, b| a.id.cmp(&b.id));
            slice.edges.retain(|edge| selection.edges.iter().any(|wire| wire.id == edge.id));
            for boundary in slice.nodes.iter_mut().filter(|node| super::boundary_types::is_port_selected(&node.node_type)) {
                // The ports read at any place the boundary is at in this slice.
                let ports: std::collections::BTreeSet<&String> = selection.boundary_ports.iter()
                    .filter(|(at, _)| at.id == boundary.id).flat_map(|(_, ports)| ports).collect();
                let relevant = |port: &str| ports.iter().any(|p| p.as_str() == port);
                boundary.inputs.retain(|port| relevant(&port.name));
                boundary.outputs.retain(|port| relevant(&port.name));
                boundary.port_literals.retain(|port, _| relevant(port));
            }
            let mut hasher = Sha256::new();
            hasher.update(b"weft-production-slice-v2\n");
            hash_definition_slice(&mut hasher, &slice, &ids)?;
            // The places themselves: two slices of one id under different
            // calls hold the same nodes and differ in the chain of sites.
            for place in &selection.nodes {
                hasher.update(place.to_string().as_bytes());
                hasher.update(b"\n");
            }
            for member in &slice.nodes {
                let implementation = self.implementations.get(&member.node_type)
                    .ok_or_else(|| anyhow::anyhow!("program identity is missing implementation '{}'", member.node_type))?;
                hasher.update(member.node_type.as_bytes());
                hasher.update(b"=");
                hasher.update(implementation.as_bytes());
                hasher.update(b"\n");
            }
            Ok((place, hex(&hasher.finalize())))
        }).collect()
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
/// worker fetches the definition with at execution claim time
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
///   the file with identical content must not flip the hash) and
///   `includePath` (an interface-parse-only pointer to an `@include`d file;
///   the file's PATH is non-semantic, its expanded topology is what runs)
///   and `sourceFile` (the absolute path a diagnostic anchors to; the same
///   project compiled from two directories must hash the same).
/// - per edge: `span` / `sourceFile`. Per group: `span` / `headerSpan` /
///   `portLiteralSpans` / `sourceFile` / `description` (the authored
///   `# ...` comment: prose, not shape).
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
pub fn compute_definition_hash(project: &ProjectDefinition) -> anyhow::Result<SourceHash> {
    let mut hasher = Sha256::new();
    hasher.update(b"weft-definition-v1\n");
    let mut value = serde_json::to_value(project)
        .map_err(|e| anyhow::anyhow!("serialize ProjectDefinition: {e}"))?;
    // Field names are the camelCase `#[serde(rename)]` wire names. A
    // ProjectDefinition ALWAYS serializes to a JSON object; if it somehow didn't,
    // silently skipping the strips would hash the un-stripped value (timestamps
    // included) and produce a wrong, unstable hash. Fail loud instead.
    let obj = value
        .as_object_mut()
        .ok_or_else(|| anyhow::anyhow!("ProjectDefinition did not serialize to a JSON object"))?;
    // `id` is the project's DB identity, NOT part of the runtime shape. It is
    // already the OTHER half of the `(project_id, definition_hash)` identity key,
    // so hashing it here too is redundant. Worse, it makes the hash context-
    // dependent: the browser WASM parse computes the live-preview hash with the
    // NIL uuid (the id is not a parse input), while the build/dispatcher computes
    // the stored hash with the real project id. If `id` were hashed, those two
    // would NEVER agree and the "out of sync / resync" light would be stuck on.
    for key in ["id", "createdAt", "updatedAt"] {
        obj.remove(key);
    }
    strip_keys(obj.get_mut("nodes"), NODE_HASH_STRIPS);
    strip_keys(obj.get_mut("edges"), EDGE_HASH_STRIPS);
    strip_keys(obj.get_mut("groups"), GROUP_HASH_STRIPS);
    strip_port_presentation(obj.get_mut("nodes"));
    strip_port_presentation(obj.get_mut("groups"));
    canonicalize_key_order(&mut value);
    let json = serde_json::to_vec(&value)
        .map_err(|e| anyhow::anyhow!("re-serialize ProjectDefinition: {e}"))?;
    hasher.update(&json);
    Ok(hex(&hasher.finalize()))
}


/// Fold `closure`'s slice of the definition into `hasher`: the
/// closure's nodes and the edges between them, in the definition
/// hash's canonical form (same non-semantic strips, see
/// [`compute_definition_hash`]), sorted by id so source order and
/// unrelated graph edits cannot move the digest. Production slice identity and the
/// compiler's infra hash are both this fold under their own prefix.
pub fn hash_definition_slice(
    hasher: &mut Sha256,
    project: &ProjectDefinition,
    closure: &HashSet<String>,
) -> anyhow::Result<()> {
    let mut nodes: Vec<serde_json::Value> = project
        .nodes
        .iter()
        .filter(|n| closure.contains(&n.id))
        .map(serde_json::to_value)
        .collect::<Result<_, _>>()
        .map_err(|e| anyhow::anyhow!("serialize slice node: {e}"))?;
    nodes.sort_by(|a, b| a["id"].as_str().cmp(&b["id"].as_str()));
    let mut edges: Vec<serde_json::Value> = project
        .edges
        .iter()
        .filter(|e| closure.contains(&e.source) && closure.contains(&e.target))
        .map(serde_json::to_value)
        .collect::<Result<_, _>>()
        .map_err(|e| anyhow::anyhow!("serialize slice edge: {e}"))?;
    edges.sort_by(|a, b| a["id"].as_str().cmp(&b["id"].as_str()));
    let mut slice = serde_json::json!({ "nodes": nodes, "edges": edges });
    strip_keys(slice.get_mut("nodes"), NODE_HASH_STRIPS);
    strip_keys(slice.get_mut("edges"), EDGE_HASH_STRIPS);
    strip_port_presentation(slice.get_mut("nodes"));
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
];
const EDGE_HASH_STRIPS: &[&str] = &["span", "sourceFile"];
// `description` is the group's authored `# ...` comment: prose, never
// the runtime shape, so rewording it must not light a resync.
const GROUP_HASH_STRIPS: &[&str] = &["span", "headerSpan", "portLiteralSpans", "sourceFile", "description"];

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
fn strip_port_presentation(array: Option<&mut serde_json::Value>) {
    let Some(serde_json::Value::Array(items)) = array else { return };
    for item in items {
        for list in ["inputs", "outputs", "inPorts", "outPorts"] {
            if let Some(serde_json::Value::Array(ports)) = item.get_mut(list) {
                for port in ports {
                    if let Some(obj) = port.as_object_mut() {
                        for key in PORT_HASH_STRIPS {
                            obj.remove(*key);
                        }
                    }
                }
            }
        }
    }
}

/// Remove `keys` from every object in a JSON array. Top level of
/// each element only: deliberately does NOT recurse into `config`.
fn strip_keys(array: Option<&mut serde_json::Value>, keys: &[&str]) {
    let Some(serde_json::Value::Array(items)) = array else { return };
    for item in items {
        if let Some(obj) = item.as_object_mut() {
            for key in keys {
                obj.remove(*key);
            }
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

/// A manifest: every covered file of a project, `path -> sha256`, plus
/// one pseudo-entry naming the installed weft.
pub type Manifest = BTreeMap<String, String>;

/// The pseudo-entry prefix a manifest carries for the installed weft
/// (`weft:<version>:<catalog hash>`), whose value is empty because it
/// names no blob. Lives here, next to the derivation that reads it, so
/// the CLI that writes a manifest and the dispatcher that validates one
/// agree on which entry is not a file.
pub const WEFT_ENTRY_PREFIX: &str = "weft:";

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
