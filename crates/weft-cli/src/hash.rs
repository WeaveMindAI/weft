//! Source-hash functions used by drift detection + image tagging.
//!
//! Two project-level hashes drive the user-visible drift signals:
//!
//! - **`compute_source_hash`**: hashes everything that affects the
//!   worker binary's compilation. Drives the resync prompt AND the
//!   worker docker image tag. Inputs:
//!     - project source: `main.weft`, `weft.toml`, `nodes/` recursive.
//!     - stdlib catalog: every catalog package, including their
//!       sidecar source dirs (it's cheaper to walk the lot than to
//!       carve out sidecar/ subtrees; cargo's build cache makes a
//!       false-positive flip a no-op rebuild).
//!     - weft workspace: `crates/`, `Cargo.toml`, `Cargo.lock`. Any
//!       engine change invalidates every project's worker image.
//!
//! - **`compute_infra_hash`**: hashes everything that affects the
//!   running infrastructure. Drives the upgrade prompt. Scoped to
//!   the infra closure (every `requires_infra` node + every node
//!   upstream of one). Inputs:
//!     - graph definition: `main.weft`, `weft.toml`.
//!     - per closure-node: full source dir (host-side `mod.rs`,
//!       `metadata.json`, `deps.toml`, shared package files,
//!       `sidecar/` dir if present).
//!     - weft workspace: `crates/`, `Cargo.toml`, `Cargo.lock`. The
//!       engine runs InfraSetup; engine changes can change the
//!       running infra's behavior.
//!
//! Plus one per-sidecar hash kept as docker-tag plumbing only:
//!
//! - **`compute_sidecar_hash`**: per-sidecar source dir hash. Used
//!   verbatim as the docker image tag suffix
//!   (`weft-sidecar-<name>:<short>`) so a stale sidecar source
//!   produces a fresh image. NOT a drift signal anymore: drift is
//!   the project-level `infra_hash` exclusively.
//!
//! Implementation note: SHA-256 hex-encoded. Hash inputs are ordered
//! deterministically (sorted file walks, sorted node lists), so the
//! same source produces the same digest regardless of OS file-listing
//! order. No mtime, no environment-dependent state: the binary that
//! runs the engine has no fingerprint of its own; engine identity is
//! captured by hashing `crates/` directly.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use sha2::{Digest, Sha256};

use weft_catalog::FsCatalog;
use weft_compiler::project::Project;
use weft_core::project::ProjectDefinition;

use crate::walk::walk_dir;

/// Public type for a hex-encoded SHA-256 digest. 64 chars.
pub type SourceHash = String;

/// Hash everything that compiles into the worker binary. Used as
/// both the worker docker tag suffix AND the resync drift signal.
pub fn compute_source_hash(project_root: &Path, weft_root: &Path) -> Result<SourceHash> {
    let mut hasher = Sha256::new();
    hasher.update(b"weft-source-v1\n");

    // Project-local source.
    hash_path(&mut hasher, &project_root.join("main.weft"))?;
    hash_path(&mut hasher, &project_root.join("weft.toml"))?;
    let nodes_dir = project_root.join("nodes");
    if nodes_dir.is_dir() {
        hash_path(&mut hasher, &nodes_dir)?;
    }

    // Workspace surface that lands in the worker.
    hash_path(&mut hasher, &weft_root.join("crates"))?;
    hash_path(&mut hasher, &weft_root.join("catalog"))?;
    hash_path(&mut hasher, &weft_root.join("Cargo.toml"))?;
    hash_path(&mut hasher, &weft_root.join("Cargo.lock"))?;

    Ok(hex(&hasher.finalize()))
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
    hash_path(&mut hasher, &project_root.join("main.weft"))?;
    hash_path(&mut hasher, &project_root.join("weft.toml"))?;

    let closure = closure_with_upstream(project, |n| n.requires_infra);

    // Hash each package root that owns a closure node. Walking the
    // package root catches mod.rs / metadata.json / deps.toml of
    // every node in the package PLUS package-level shared `.rs`
    // files and `package.toml`. Multiple closure nodes from the
    // same package collapse to a single hash of that package root.
    let mut closure_packages: BTreeSet<PathBuf> = BTreeSet::new();
    for node_id in &closure {
        let Some(node) = project.nodes.iter().find(|n| n.id == *node_id) else { continue };
        let Some(pkg) = catalog.package_of(&node.node_type) else { continue };
        closure_packages.insert(pkg.root.clone());
    }
    for pkg_root in &closure_packages {
        hasher.update(b"package:");
        hasher.update(pkg_root.to_string_lossy().as_bytes());
        hasher.update(b"\n");
        hash_path(&mut hasher, pkg_root)?;
    }

    // Workspace: engine + core compile into the worker binary that
    // runs InfraSetup. Engine changes can change provision behavior.
    hash_path(&mut hasher, &weft_root.join("crates"))?;
    hash_path(&mut hasher, &weft_root.join("Cargo.toml"))?;
    hash_path(&mut hasher, &weft_root.join("Cargo.lock"))?;

    Ok(hex(&hasher.finalize()))
}

/// Hash a single sidecar's source: Dockerfile + every file in the
/// sidecar source dir, scoped by node type. Used verbatim as the
/// sidecar docker image tag suffix.
pub fn compute_sidecar_hash(
    node_type: &str,
    sidecar_source_dir: &Path,
) -> Result<SourceHash> {
    let mut hasher = Sha256::new();
    hasher.update(b"weft-sidecar-v1\n");
    hasher.update(node_type.as_bytes());
    hasher.update(b"\n");
    hash_path(&mut hasher, sidecar_source_dir)?;
    Ok(hex(&hasher.finalize()))
}

/// Convenience: load + enrich a project to a `ProjectDefinition`
/// without running cargo / docker. Drift detection and the sidecar
/// build paths both need an enriched project to walk the closure.
pub fn load_enriched_project(project: &Project) -> Result<ProjectDefinition> {
    use weft_compiler::build::build_project_catalog;
    use weft_compiler::compile_source;
    use weft_compiler::enrich::enrich;
    let source = project
        .read_main_weft()
        .map_err(|e| anyhow::anyhow!("read main.weft: {e}"))?;
    let mut definition = compile_source(&source, project.id()).map_err(|errors| {
        let msg = errors
            .iter()
            .map(|e| format!("{}: {}", e.line, e.message))
            .collect::<Vec<_>>()
            .join("\n");
        anyhow::anyhow!("compile: {msg}")
    })?;
    definition.name = project.manifest.package.name.clone();
    let catalog = build_project_catalog(&project.root)
        .map_err(|e| anyhow::anyhow!("catalog: {e}"))?;
    enrich(&mut definition, &catalog)
        .map_err(|e| anyhow::anyhow!("enrich: {e}"))?;
    Ok(definition)
}

// ---- internals ----

/// Path-into-hasher: file → name + content; dir → recursive sorted.
/// Skips target/, node_modules/, .git/, .weft/. Public so the
/// image-stamp hasher in `images.rs` can share the exact same
/// framing rules (no two-different-hash-functions-for-the-same-job
/// drift).
pub(crate) fn hash_path(hasher: &mut Sha256, path: &Path) -> Result<()> {
    if !path.exists() {
        // Hash the absent path so a future appearance invalidates.
        hasher.update(b"missing:");
        hasher.update(path.to_string_lossy().as_bytes());
        hasher.update(b"\n");
        return Ok(());
    }
    if path.is_file() {
        hash_file(hasher, path)
    } else if path.is_dir() {
        hash_dir(hasher, path)
    } else {
        Ok(())
    }
}

fn hash_file(hasher: &mut Sha256, path: &Path) -> Result<()> {
    let name = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("");
    hasher.update(b"file:");
    hasher.update(name.as_bytes());
    hasher.update(b"\n");
    let bytes = std::fs::read(path)
        .with_context(|| format!("read {} for hashing", path.display()))?;
    hasher.update(&bytes);
    hasher.update(b"\n");
    Ok(())
}

fn hash_dir(hasher: &mut Sha256, dir: &Path) -> Result<()> {
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
