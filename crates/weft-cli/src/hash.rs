//! Source-hash functions used by drift detection + image tagging.
//!
//! Two project-level hashes drive the user-visible drift signals:
//!
//! - **`compute_source_hash`**: hashes everything that affects the
//!   worker binary's compilation. Drives the resync prompt AND the
//!   worker docker image tag. Inputs:
//!     - graph: `main.weft`, `weft.toml`.
//!     - the REFERENCED nodes' package roots (the exact set the build
//!       stages + codegen shims). The stdlib reaches the worker only
//!       through its clone under `nodes/`, captured here via its roots;
//!       an unreferenced node can't change the worker, so it doesn't
//!       flip this hash.
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
//!       `images/` dir if present).
//!     - weft workspace: `crates/`, `Cargo.toml`, `Cargo.lock`. The
//!       engine runs InfraSetup; engine changes can change the
//!       running infra's behavior.
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

/// Hash everything that compiles into the worker binary. Used as
/// both the worker docker tag suffix AND the resync drift signal.
///
/// Scoped to exactly what the build compiles in: the referenced
/// nodes' package roots (the same set `stage_build_context` copies and
/// codegen emits shims for), NOT all of `nodes/`. An unreferenced node
/// can't change the worker binary, so it must not flip this hash; and
/// hashing the same package roots the build stages, through the same
/// node-tree walk policy, is what makes "hash captures the build
/// inputs" true by construction. The weft `catalog/` is deliberately
/// absent: it's only a seed source for `weft new` / `weft catalog
/// update`, never a build input (the stdlib reaches the worker only
/// through its clone under `nodes/`, captured here via its roots).
pub fn compute_source_hash(
    project: &ProjectDefinition,
    project_root: &Path,
    weft_root: &Path,
    catalog: &FsCatalog,
) -> Result<SourceHash> {
    let mut hasher = Sha256::new();
    hasher.update(b"weft-source-v1\n");

    hash_path(&mut hasher, &project_root.join("main.weft"))?;
    hash_path(&mut hasher, &project_root.join("weft.toml"))?;

    let referenced = weft_compiler::codegen::collect_node_types(project);
    hash_package_roots(&mut hasher, &catalog.package_roots_for(&referenced))?;

    // Workspace surface that lands in the worker (the language runtime
    // the generated crate links as path deps).
    hash_path(&mut hasher, &weft_root.join("crates"))?;
    hash_path(&mut hasher, &weft_root.join("Cargo.toml"))?;
    hash_path(&mut hasher, &weft_root.join("Cargo.lock"))?;

    Ok(hex(&hasher.finalize()))
}

/// Hash a sorted, deduped set of package roots, each prefixed by its
/// path so a node moving between packages changes the digest. Shared
/// by the source hash (referenced roots) and the infra hash (closure
/// roots): both fold "the node trees that matter" into a digest the
/// same way, over the same node-tree walk policy (`walk_dir`).
fn hash_package_roots(hasher: &mut Sha256, roots: &[PathBuf]) -> Result<()> {
    let mut sorted: Vec<&PathBuf> = roots.iter().collect();
    sorted.sort();
    sorted.dedup();
    for root in sorted {
        hasher.update(b"package:");
        hasher.update(root.to_string_lossy().as_bytes());
        hasher.update(b"\n");
        hash_path(hasher, root)?;
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
    hash_path(&mut hasher, &project_root.join("main.weft"))?;
    hash_path(&mut hasher, &project_root.join("weft.toml"))?;

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
    hash_package_roots(&mut hasher, &closure_roots)?;

    // Workspace: engine + core compile into the worker binary that
    // runs InfraSetup. Engine changes can change provision behavior.
    hash_path(&mut hasher, &weft_root.join("crates"))?;
    hash_path(&mut hasher, &weft_root.join("Cargo.toml"))?;
    hash_path(&mut hasher, &weft_root.join("Cargo.lock"))?;

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
    hash_path(&mut hasher, image_source_dir)?;
    Ok(hex(&hasher.finalize()))
}

/// Load + enrich a project to a `ProjectDefinition` AND return the
/// catalog it was enriched against, without running cargo / docker.
/// Returns both because every caller (drift hashes, infra build) needs
/// the same catalog the definition was built from; returning it here is
/// one discovery per command instead of each caller re-walking `nodes/`.
pub fn load_enriched_project(project: &Project) -> Result<(ProjectDefinition, FsCatalog)> {
    use weft_compiler::build::build_project_catalog;
    use weft_compiler::compile_enriched;
    let source = project
        .read_main_weft()
        .map_err(|e| anyhow::anyhow!("read main.weft: {e}"))?;
    let catalog = build_project_catalog(&project.root)
        .map_err(|e| anyhow::anyhow!("catalog: {e}"))?;
    let definition = compile_enriched(&source, project.id(), Some(&project.root), &catalog)
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    // The parsed definition carries no name (it's a manifest property); the
    // manifest file itself is hashed via the project-dir walk, so identity is
    // covered without piggybacking it onto the graph.
    Ok((definition, catalog))
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
