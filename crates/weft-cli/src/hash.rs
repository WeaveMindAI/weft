//! Source-hash functions used by drift detection + image tagging.
//!
//! Three project-level hashes split the user-visible drift signals
//! cleanly along their reason for changing:
//!
//! - **`compute_binary_hash`**: hashes everything that affects the
//!   WORKER BINARY's bytes. Drives the worker docker image tag.
//!   Inputs:
//!     - `weft.toml` `[build]` section choices (base image, custom
//!       Dockerfile template) plus the package name (the binary's
//!       crate name).
//!     - the SET of referenced node types (the codegen-emitted
//!       static dispatch table references each by name; adding /
//!       removing a node TYPE changes the binary, but changing a
//!       node's config or rewiring edges does NOT).
//!     - the REFERENCED nodes' package roots (mod.rs, deps.toml,
//!       shared package files). A change to a node implementation
//!       (re)compiles the worker.
//!     - worker build environment: `crates/`, `Cargo.toml`,
//!       `Cargo.lock`, `rust-toolchain.toml`, and the builder-base
//!       Dockerfile. Any engine, toolchain or build-image change
//!       invalidates every project's worker image.
//!
//!   NOT hashed: per-node config values, edges, node ids, positions.
//!   Those live in the `ProjectDefinition` the worker fetches from
//!   the broker at execution claim time; they no longer reach the
//!   binary via `include_str!`.
//!
//! - **`compute_definition_hash`**: hashes the runtime project
//!   shape: the canonical `ProjectDefinition` (topology, configs,
//!   edges, infra flags). Drives the resync prompt AND identifies
//!   the definition row in the broker's project store. A config-only
//!   edit flips this hash without flipping `binary_hash`, so the
//!   worker image stays cache-hit and only the project row updates.
//!
//! - **`compute_infra_hash`**: hashes everything that affects the
//!   running infrastructure. Drives the upgrade prompt. Scoped to
//!   the infra closure (every `requires_infra` node + every node
//!   upstream of one). Inputs:
//!     - graph definition: `main.weft`, `weft.toml`.
//!     - per closure-node: full source dir (host-side `mod.rs`,
//!       `metadata.json`, `deps.toml`, shared package files,
//!       `images/` dir if present).
//!     - worker build environment (same list as the binary hash).
//!       The engine runs InfraSetup; engine or toolchain changes can
//!       change the running infra's behavior.
//!
//! Plus one per-image hash kept as docker-tag plumbing only:
//!
//! - **`compute_image_hash`**: per-image source dir hash. Used
//!   verbatim as the docker image tag suffix
//!   (`weft-infra-<name>:<short>`) so a stale image source
//!   produces a fresh image. NOT a drift signal anymore: drift is
//!   the project-level `infra_hash` exclusively.
//!
//! Implementation note: SHA-256 hex-encoded. Hash inputs are ordered
//! deterministically (sorted file walks, sorted node lists), so the
//! same source produces the same digest regardless of OS file-listing
//! order. No mtime, no environment-dependent state: the binary that
//! runs the engine has no fingerprint of its own; engine identity is
//! captured by hashing `crates/` directly.

use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use sha2::{Digest, Sha256};

use weft_catalog::FsCatalog;
use weft_compiler::project::Project;
use weft_core::project::ProjectDefinition;

use crate::walk::walk_dir;

/// Public type for a hex-encoded SHA-256 digest. 64 chars.
pub type SourceHash = String;

/// Dockerfile (relative to the weft root) that builds the shared
/// worker builder-base image. Part of the worker build environment:
/// editing it changes the image every worker compiles inside, so it
/// participates in the binary / infra / builder-base hashes.
pub const BUILDER_BASE_DOCKERFILE: &str = "deploy/docker/worker-builder-base.Dockerfile";

/// The workspace source inputs every weft-built artifact depends on:
/// the engine crates, the workspace manifests, and the pinned
/// toolchain. One list, as `(label, path)` pairs (the label is the
/// machine-independent string folded into digests), consumed by the
/// three project-level hash functions here AND the image-stamp
/// hasher in `images.rs`, so the input sets can't drift apart.
pub fn workspace_source_inputs(weft_root: &Path) -> Vec<(String, PathBuf)> {
    ["crates", "Cargo.toml", "Cargo.lock", "rust-toolchain.toml"]
        .iter()
        .map(|rel| (rel.to_string(), weft_root.join(rel)))
        .collect()
}

/// Fold the full worker build environment into `hasher`: the
/// workspace source inputs plus the builder-base Dockerfile (the
/// image every worker compiles inside). Shared by the binary, infra
/// and builder-base hashes: a toolchain bump or a builder-base edit
/// must flip all three, otherwise `image_present` short-circuits and
/// a stale worker keeps running forever.
fn hash_worker_build_env(hasher: &mut Sha256, weft_root: &Path) -> Result<()> {
    for (label, path) in workspace_source_inputs(weft_root) {
        hash_path(hasher, &label, &path)?;
    }
    hash_path(
        hasher,
        BUILDER_BASE_DOCKERFILE,
        &weft_root.join(BUILDER_BASE_DOCKERFILE),
    )?;
    Ok(())
}

/// Hash everything that compiles into the worker binary. Used as
/// the worker docker tag suffix; the dispatcher selects the spawn
/// image by this hash.
///
/// Scoped to exactly the codegen-emitted-static surface: weft.toml
/// (build config + crate name), the SET of referenced node TYPES
/// (the static dispatch table in `registry.rs` references each by
/// name), each referenced node's package root (mod.rs, deps.toml,
/// shared files), and the weft workspace (the engine the binary
/// links as path deps). NOT hashed: per-node config, edges, ids,
/// positions: those land in the `ProjectDefinition` the worker
/// fetches at runtime via the broker.
///
/// An unreferenced node can't change the worker binary, so it does
/// not flip this hash. The same node walked through `weft_catalog`'s
/// `NODE_TREE_EXCLUDE` policy that `stage_build_context` uses, so
/// what the binary sees and what we hash agree byte-for-byte.
pub fn compute_binary_hash(
    definition: &ProjectDefinition,
    project: &weft_compiler::project::Project,
    weft_root: &Path,
    catalog: &FsCatalog,
) -> Result<SourceHash> {
    let project_root = project.root.as_path();
    let mut hasher = Sha256::new();
    hasher.update(b"weft-binary-v1\n");

    // weft.toml carries build choices (base image, custom template)
    // AND the crate name (which becomes the binary name). Both
    // affect the docker image bytes. Project configs live in
    // main.weft, not weft.toml, so this stays binary-scoped.
    hash_path(&mut hasher, "weft.toml", &project_root.join("weft.toml"))?;

    // A custom Dockerfile template's CONTENT shapes the image too;
    // weft.toml only carries its path, so an edit to the template
    // file itself would otherwise never trigger a rebuild. The caller
    // already loaded the project, so read the manifest off it (no
    // redundant re-read of weft.toml from disk). A set-but-MISSING
    // template path is a config error: fail loudly here rather than
    // hashing the absence (which would let the build proceed to a
    // docker failure later with a less obvious cause).
    if let Some(rel) = &project.manifest.build.worker.dockerfile_template {
        let template_path = project_root.join(rel);
        if !template_path.exists() {
            anyhow::bail!(
                "weft.toml sets [build.worker] dockerfile_template = {:?} but that file does \
                 not exist (resolved to {}); fix the path or remove the setting",
                rel,
                template_path.display()
            );
        }
        hash_path(&mut hasher, "dockerfile_template", &template_path)?;
    }

    // SET of referenced node TYPES: the dispatch table in registry.rs
    // is generated from this. The ORDER doesn't matter (we sort), and
    // the per-node config values live in main.weft (hashed by the
    // definition hash, not here).
    let referenced = weft_compiler::codegen::collect_node_types(definition);
    hasher.update(b"node_types:\n");
    for nt in &referenced {
        hasher.update(b"  ");
        hasher.update(nt.as_bytes());
        hasher.update(b"\n");
    }

    // Each referenced node's package root: mod.rs + deps.toml +
    // shared files. A node implementation edit (re)compiles the
    // binary, so it flips this hash.
    hash_package_roots(
        &mut hasher,
        &catalog.package_roots_for(&referenced),
        &[project_root, weft_root],
    )?;

    // Worker build environment: workspace source the binary links
    // against + the builder-base image it compiles inside.
    hash_worker_build_env(&mut hasher, weft_root)?;

    Ok(hex(&hasher.finalize()))
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
/// Non-semantic fields are stripped before hashing, at their known
/// structural levels (never inside `config`, where a user value
/// could legitimately use the same key names):
/// - top level: `createdAt` / `updatedAt` (stamped `Utc::now()` on
///   every compile; hashing them would flip the hash per build).
/// - per node: `span` / `headerSpan` / `configSpans` (source text
///   coordinates: comments or formatting shift them without changing
///   the runtime graph) and `position` (canvas layout from a drag).
/// - per edge: `span`. Per group: `span` / `headerSpan`.
pub fn compute_definition_hash(project: &ProjectDefinition) -> Result<SourceHash> {
    let mut hasher = Sha256::new();
    hasher.update(b"weft-definition-v1\n");
    let mut value = serde_json::to_value(project)
        .map_err(|e| anyhow::anyhow!("serialize ProjectDefinition: {e}"))?;
    // Field names are the camelCase `#[serde(rename)]` wire names.
    if let Some(obj) = value.as_object_mut() {
        obj.remove("createdAt");
        obj.remove("updatedAt");
        strip_keys(obj.get_mut("nodes"), &["span", "headerSpan", "configSpans", "position"]);
        strip_keys(obj.get_mut("edges"), &["span"]);
        strip_keys(obj.get_mut("groups"), &["span", "headerSpan"]);
    }
    let json = serde_json::to_vec(&value)
        .map_err(|e| anyhow::anyhow!("re-serialize ProjectDefinition: {e}"))?;
    hasher.update(&json);
    Ok(hex(&hasher.finalize()))
}

/// Remove `keys` from every object in a JSON array. Top level of
/// each element only: deliberately does NOT recurse into `config`.
fn strip_keys(array: Option<&mut serde_json::Value>, keys: &[&str]) {
    let Some(serde_json::Value::Array(items)) = array else { return };
    for item in items {
        if let Some(obj) = item.as_object_mut() {
            for k in keys {
                obj.remove(*k);
            }
        }
    }
}

/// Hash a sorted, deduped set of package roots, each prefixed by its
/// path RELATIVE to one of `bases` (the project root or the weft
/// root) so a node moving between packages changes the digest while
/// moving the whole checkout to another directory does not. Shared
/// by the source hash (referenced roots) and the infra hash (closure
/// roots): both fold "the node trees that matter" into a digest the
/// same way, over the same node-tree walk policy (`walk_dir`).
fn hash_package_roots(hasher: &mut Sha256, roots: &[PathBuf], bases: &[&Path]) -> Result<()> {
    let mut sorted: Vec<&PathBuf> = roots.iter().collect();
    sorted.sort();
    sorted.dedup();
    for root in sorted {
        let rel = bases
            .iter()
            .find_map(|b| root.strip_prefix(b).ok())
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "package root {} is outside the project and weft roots; \
                     hashing its absolute path would make the digest machine-local",
                    root.display()
                )
            })?
            .to_string_lossy()
            .into_owned();
        hasher.update(b"package:");
        hasher.update(rel.as_bytes());
        hasher.update(b"\n");
        hash_path(hasher, &rel, root)?;
    }
    Ok(())
}

/// Hash everything that affects the running infrastructure.
/// Scoped to the infra closure: every node where `requires_infra` is
/// true plus every node upstream of one (the chain that produces the
/// inputs the infra node consumes during InfraSetup).
pub fn compute_infra_hash(
    project: &ProjectDefinition,
    project_root: &Path,
    weft_root: &Path,
    catalog: &FsCatalog,
) -> Result<SourceHash> {
    let mut hasher = Sha256::new();
    hasher.update(b"weft-infra-v1\n");

    // Graph definition: which nodes are infra, what config they have,
    // what edges they receive. A user edit to main.weft can flip a
    // node into / out of the infra closure or rewire an upstream.
    hash_path(&mut hasher, "main.weft", &project_root.join("main.weft"))?;
    hash_path(&mut hasher, "weft.toml", &project_root.join("weft.toml"))?;

    let closure = closure_with_upstream(project, |n| n.requires_infra);

    // Hash each package root that owns a closure node (mod.rs /
    // metadata.json / deps.toml of every node in the package, plus
    // shared `.rs` and `package.toml`). Same folding the source hash
    // uses; multiple closure nodes from one package collapse to one
    // root via the helper's dedup.
    let closure_roots: Vec<PathBuf> = closure
        .iter()
        .filter_map(|id| project.nodes.iter().find(|n| n.id == *id))
        .filter_map(|n| catalog.package_of(&n.node_type))
        .map(|pkg| pkg.root.clone())
        .collect();
    hash_package_roots(&mut hasher, &closure_roots, &[project_root, weft_root])?;

    // Worker build environment: engine + core compile into the
    // worker binary that runs InfraSetup, inside the builder base.
    hash_worker_build_env(&mut hasher, weft_root)?;

    Ok(hex(&hasher.finalize()))
}

/// Hash a single image's source: Dockerfile + every file in the
/// image source dir, scoped by node type. Used verbatim as the
/// infra docker image tag suffix.
pub fn compute_image_hash(
    node_type: &str,
    image_source_dir: &Path,
) -> Result<SourceHash> {
    let mut hasher = Sha256::new();
    hasher.update(b"weft-image-v1\n");
    hasher.update(node_type.as_bytes());
    hasher.update(b"\n");
    hash_path(&mut hasher, "image-source", image_source_dir)?;
    Ok(hex(&hasher.finalize()))
}

/// Hash the inputs that shape the pre-built worker builder base image:
/// the engine workspace + rust-toolchain pin + the base Dockerfile
/// itself. An engine bump or toolchain change flips this hash and
/// triggers a fresh base image; per-project worker images then FROM
/// the new tag. Scoped to engine-affecting inputs only, NOT project
/// or catalog inputs (those don't change the base). The input set is
/// exactly `hash_worker_build_env`, shared with the binary / infra
/// hashes so a base-affecting edit flips all three together.
pub fn compute_builder_base_hash(weft_root: &Path) -> Result<SourceHash> {
    let mut hasher = Sha256::new();
    hasher.update(b"weft-builder-base-v1\n");
    hash_worker_build_env(&mut hasher, weft_root)?;
    Ok(hex(&hasher.finalize()))
}

/// Load + enrich a project to a `ProjectDefinition` AND return the
/// catalog it was enriched against, without running cargo / docker.
/// Returns both because every caller (drift hashes, infra build) needs
/// the same catalog the definition was built from; returning it here is
/// one discovery per command instead of each caller re-walking `nodes/`.
pub fn load_enriched_project(project: &Project) -> Result<(ProjectDefinition, FsCatalog)> {
    load_enriched_project_with_diagnostics(project)
        .map_err(|e| anyhow::anyhow!("{e}"))
}

/// Same as `load_enriched_project` but on compile failure the Err
/// carries the structured `Vec<Diagnostic>` so callers that surface
/// errors to the editor (the CLI's run path emitting structured
/// progress events) can render them one-per-line rather than as a
/// single flattened string.
pub fn load_enriched_project_with_diagnostics(
    project: &Project,
) -> std::result::Result<(ProjectDefinition, FsCatalog), CompileLoadError> {
    use weft_compiler::build::build_project_catalog;
    use weft_compiler::compile_enriched_with_diagnostics;
    let source = project
        .read_main_weft()
        .map_err(|e| CompileLoadError::Read(format!("read main.weft: {e}")))?;
    let catalog = build_project_catalog(&project.root)
        .map_err(|e| CompileLoadError::Read(format!("catalog: {e}")))?;
    let definition = compile_enriched_with_diagnostics(&source, project.id(), Some(&project.root), &catalog)
        .map_err(CompileLoadError::Diagnostics)?;
    Ok((definition, catalog))
}

/// Error envelope for the diagnostic-bearing loader. `Read` covers
/// I/O failures (source + catalog discovery), `Diagnostics` covers
/// compile failures with their structured per-error list.
#[derive(Debug)]
pub enum CompileLoadError {
    Read(String),
    Diagnostics(Vec<weft_compiler::Diagnostic>),
}

impl std::fmt::Display for CompileLoadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CompileLoadError::Read(msg) => write!(f, "{msg}"),
            // Same rendering as the compiler's own abort path.
            CompileLoadError::Diagnostics(diags) => {
                write!(f, "{}", weft_compiler::render_diagnostics(diags))
            }
        }
    }
}

impl std::error::Error for CompileLoadError {}

// ---- internals ----

/// Path-into-hasher: file → label + content; dir → label + recursive
/// sorted walk. Skips target/, node_modules/, .git/, .weft/. `label`
/// is the machine-independent name folded into the digest in place
/// of the (possibly absolute) on-disk path: hashing absolute paths
/// would flip every hash when the checkout moves directories.
/// Public so the image-stamp hasher in `images.rs` can share the
/// exact same framing rules (no
/// two-different-hash-functions-for-the-same-job drift).
pub(crate) fn hash_path(hasher: &mut Sha256, label: &str, path: &Path) -> Result<()> {
    if !path.exists() {
        // Hash the absence so a future appearance invalidates.
        hasher.update(b"missing:");
        hasher.update(label.as_bytes());
        hasher.update(b"\n");
        return Ok(());
    }
    if path.is_file() {
        hash_file(hasher, label, path)
    } else if path.is_dir() {
        hash_dir(hasher, label, path)
    } else {
        Ok(())
    }
}

fn hash_file(hasher: &mut Sha256, label: &str, path: &Path) -> Result<()> {
    hasher.update(b"file:");
    hasher.update(label.as_bytes());
    hasher.update(b"\n");
    let bytes = std::fs::read(path)
        .with_context(|| format!("read {} for hashing", path.display()))?;
    hasher.update(&bytes);
    hasher.update(b"\n");
    Ok(())
}

fn hash_dir(hasher: &mut Sha256, label: &str, dir: &Path) -> Result<()> {
    hasher.update(b"dir:");
    hasher.update(label.as_bytes());
    hasher.update(b"\n");
    let mut entries = walk_dir(dir)?;
    entries.sort();
    for entry in entries {
        let rel = entry
            .strip_prefix(dir)
            .unwrap_or(&entry)
            .to_string_lossy()
            .into_owned();
        hasher.update(b"path:");
        hasher.update(rel.as_bytes());
        hasher.update(b"\n");
        if entry.is_file() {
            let bytes = std::fs::read(&entry)
                .with_context(|| format!("read {} for hashing", entry.display()))?;
            hasher.update(&bytes);
            hasher.update(b"\n");
        }
    }
    Ok(())
}


/// Closure: every node matching `seed` plus every node upstream
/// (transitively) via incoming edges. Used by `compute_infra_hash`
/// to scope the hash input to "what affects the infra subgraph."
fn closure_with_upstream(
    project: &ProjectDefinition,
    seed: impl Fn(&weft_core::project::NodeDefinition) -> bool,
) -> HashSet<String> {
    let by_id: BTreeMap<&str, &weft_core::project::NodeDefinition> = project
        .nodes
        .iter()
        .map(|n| (n.id.as_str(), n))
        .collect();

    let mut frontier: Vec<String> = project
        .nodes
        .iter()
        .filter(|n| seed(n))
        .map(|n| n.id.clone())
        .collect();
    let mut closure: HashSet<String> = HashSet::new();
    while let Some(id) = frontier.pop() {
        if !closure.insert(id.clone()) {
            continue;
        }
        for edge in &project.edges {
            if edge.target == id && by_id.contains_key(edge.source.as_str()) {
                if !closure.contains(&edge.source) {
                    frontier.push(edge.source.clone());
                }
            }
        }
    }
    closure
}

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{:02x}", b));
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds a `ProjectDefinition` via JSON so the test isn't
    /// coupled to every field of every internal struct (config_spans,
    /// file_refs, etc). Stamps both timestamps to `ts` and one node
    /// with a config the caller can vary.
    fn project_at(ts: &str, config_value: &str) -> ProjectDefinition {
        serde_json::from_value(serde_json::json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "nodes": [{
                "id": "n",
                "nodeType": "Text",
                "label": null,
                "config": {"value": config_value},
                "position": {"x": 0.0, "y": 0.0},
                "inputs": [],
                "outputs": [],
                "features": {},
                "scope": [],
                "groupBoundary": null,
                "requiresInfra": false,
                "images": [],
            }],
            "edges": [],
            "groups": [],
            "createdAt": ts,
            "updatedAt": ts,
        }))
        .expect("test ProjectDefinition")
    }

    /// Regression: `created_at` / `updated_at` are stamped at
    /// compile time with `Utc::now()`. If `compute_definition_hash`
    /// hashed them verbatim, every CLI invocation would produce a
    /// different hash for the same source, the resync drift signal
    /// would always light, and the worker's per-hash definition
    /// cache would miss on every execution. The hash must depend
    /// only on the semantic shape.
    #[test]
    fn definition_hash_is_stable_across_timestamps() {
        let h1 = compute_definition_hash(&project_at("2024-01-01T00:00:00Z", "hi")).unwrap();
        let h2 = compute_definition_hash(&project_at("2099-12-31T23:59:59Z", "hi")).unwrap();
        assert_eq!(h1, h2, "different timestamps must hash identically");
    }

    /// Regression: source spans and canvas positions are
    /// non-semantic. Adding a comment line to main.weft shifts every
    /// node's `span`; dragging a node changes `position`. If either
    /// fed the hash, the resync drift signal would light on edits
    /// that don't change the runtime graph.
    #[test]
    fn definition_hash_ignores_spans_and_position() {
        let plain = project_at("2024-01-01T00:00:00Z", "hi");
        let mut shifted = serde_json::to_value(&plain).unwrap();
        let node = &mut shifted["nodes"][0];
        node["position"] = serde_json::json!({"x": 250.0, "y": -40.0});
        node["span"] = serde_json::json!({
            "startLine": 7, "startColumn": 1, "endLine": 9, "endColumn": 2
        });
        node["headerSpan"] = serde_json::json!({
            "startLine": 7, "startColumn": 1, "endLine": 7, "endColumn": 20
        });
        let shifted: ProjectDefinition = serde_json::from_value(shifted).unwrap();
        let h1 = compute_definition_hash(&plain).unwrap();
        let h2 = compute_definition_hash(&shifted).unwrap();
        assert_eq!(h1, h2, "span/position-only differences must hash identically");
    }

    /// Counterpoint to the above: the hash MUST change when the
    /// runtime shape changes (config edit, edge add, etc). Without
    /// this, definition_drift would never light and resyncs would
    /// never trigger.
    #[test]
    fn definition_hash_flips_on_config_edit() {
        let a = compute_definition_hash(&project_at("2024-01-01T00:00:00Z", "hi")).unwrap();
        let b = compute_definition_hash(&project_at("2024-01-01T00:00:00Z", "bye")).unwrap();
        assert_ne!(a, b, "config edit must flip the hash");
    }
}
