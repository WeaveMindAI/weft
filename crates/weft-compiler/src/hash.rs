//! Source-hash functions used by drift detection + image tagging.
//!
//! These live in `weft-compiler` (not the CLI) so that any build site shares one
//! hashing + planning brain: whoever compiles a project computes the
//! authoritative hashes here before staging the build context. One definition, no
//! two-hashers-drift hazard.
//!
//! Three project-level hashes split the user-visible drift signals
//! cleanly along their reason for changing, plus one pure content hash
//! of the uploaded source set (`compute_source_hash`) used as the
//! create-time storage key + build-dedup key BEFORE any compile:
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
//!     - worker build environment: the worker crate closure
//!       (`codegen::worker_workspace_crates`: the workspace crates a
//!       worker actually links), `Cargo.toml`, `Cargo.lock`,
//!       `rust-toolchain.toml`, and the builder-base Dockerfile. Any
//!       engine, toolchain or build-image change invalidates every
//!       project's worker image; edits to non-worker crates (CLI,
//!       dispatcher, tests) do not.
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
//!   PURE over the definition (no filesystem), so the browser (WASM)
//!   can compute it for live-preview diagnostics; it is the one hash
//!   not gated behind the `build` feature.
//!
//! - **`compute_infra_hash`**: hashes everything that affects the
//!   running infrastructure. Drives the upgrade prompt. Scoped to
//!   the infra closure (every `requires_infra` node + every node
//!   upstream of one). Inputs:
//!     - graph definition: the closure's slice of the canonical
//!       `ProjectDefinition` (its nodes' configs + the edges among
//!       them, same non-semantic strips as the definition hash), plus
//!       `weft.toml`. An edit outside the closure does not move it.
//!     - per closure-node: full source dir (host-side `mod.rs`,
//!       `metadata.json`, `deps.toml`, shared package files,
//!       `images/` dir if present).
//!     - worker build environment (same list as the binary hash).
//!       The engine runs InfraSetup; engine or toolchain changes can
//!       change the running infra's behavior.
//!
//! Two per-PACKAGE hashes cover node-test staleness rather than
//! project drift, documented on their own functions:
//! `compute_node_test_hash` (is a package's cached test
//! binary/image current) and `compute_node_test_outcome_hash` (can a
//! recorded live-tier pass still be trusted).
//!
//! Plus two hashes that are docker-tag plumbing only:
//!
//! - **`compute_image_hash`**: per-image source dir hash. Used as the
//!   docker image tag suffix with the FULL content hash
//!   (`weft-infra-<name>:<content_hash>`, assembled by
//!   `image_set::infra_image_tag`, matching the worker tag's full-hash
//!   form) so a stale image source produces a fresh image. NOT a drift
//!   signal anymore: drift is the project-level `infra_hash` exclusively.
//!
//! - **`compute_builder_base_hash`**: global (not per project) hash of
//!   the worker build environment, tagging the shared
//!   `weft-builder-base:<hash>` image. Shares its inputs with the
//!   binary and infra hashes, so an edit to the engine or the
//!   toolchain moves all three together.
//!
//! Implementation note: SHA-256 hex-encoded. Hash inputs are ordered
//! deterministically (sorted file walks, sorted node lists), so the
//! same source produces the same digest regardless of OS file-listing
//! order. No mtime, no environment-dependent state: the binary that
//! runs the engine has no fingerprint of its own; engine identity is
//! captured by hashing the worker crate closure's sources directly.

use sha2::{Digest, Sha256};

use weft_core::project::ProjectDefinition;

/// Public type for a hex-encoded SHA-256 digest. 64 chars.
pub type SourceHash = String;

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
/// PURE over the definition (no filesystem access), so it is NOT gated
/// behind the `build` feature: the browser WASM build calls it for
/// live-preview diagnostics, and the dispatcher calls it after compile.
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
///   the file's PATH is non-semantic, its expanded topology is what runs).
/// - per edge: `span`. Per group: `span` / `headerSpan` /
///   `portLiteralSpans`.
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
    obj.remove("id");
    obj.remove("createdAt");
    obj.remove("updatedAt");
    strip_keys(obj.get_mut("nodes"), NODE_HASH_STRIPS);
    strip_keys(obj.get_mut("edges"), EDGE_HASH_STRIPS);
    strip_keys(obj.get_mut("groups"), GROUP_HASH_STRIPS);
    let json = serde_json::to_vec(&value)
        .map_err(|e| anyhow::anyhow!("re-serialize ProjectDefinition: {e}"))?;
    hasher.update(&json);
    Ok(hex(&hasher.finalize()))
}

/// Hash the uploaded source FILE SET, deterministically, with no
/// compile and no workspace. PURE over `(path, content)` pairs, so the
/// dispatcher can compute it the moment a `/projects/create` body
/// arrives, BEFORE any build. It is the create-time storage key for a
/// project version's files (`project_file.source_hash`) and the build
/// dedup key. A workspace/toolchain bump is NOT visible here (it isn't
/// in the user's files), so the builder-base tag carries that axis: a
/// build is correct on a content-hit because the build recompiles
/// against whatever workspace the builder-base image baked.
///
/// Files are sorted by path so a reordered upload hashes identically;
/// each entry folds in `path` then `content`, framed like
/// [`hash_path`] so reading a file's bytes off disk vs out of a row
/// produces the same digest.
pub fn compute_source_hash(files: &[(String, String)]) -> SourceHash {
    let mut sorted: Vec<&(String, String)> = files.iter().collect();
    sorted.sort_by(|a, b| a.0.cmp(&b.0));
    let mut hasher = Sha256::new();
    hasher.update(b"weft-source-v1\n");
    for (path, content) in sorted {
        hasher.update(b"file:");
        hasher.update(path.as_bytes());
        hasher.update(b"\n");
        hasher.update(content.as_bytes());
        hasher.update(b"\n");
    }
    hex(&hasher.finalize())
}

/// The non-semantic keys stripped before hashing, shared by
/// [`compute_definition_hash`] and the infra slice hasher so the two
/// digests can never disagree on what "semantic" means.
const NODE_HASH_STRIPS: &[&str] =
    &["span", "headerSpan", "configSpans", "portLiteralSpans", "position", "fileRefs", "includePath"];
const EDGE_HASH_STRIPS: &[&str] = &["span"];
const GROUP_HASH_STRIPS: &[&str] = &["span", "headerSpan", "portLiteralSpans"];

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

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        // write! into the reused buffer, no per-byte String allocation.
        let _ = write!(s, "{b:02x}");
    }
    s
}

// ---- filesystem-bound hashes (the build path) ----
//
// Everything below needs the on-disk workspace + the catalog, so it is
// gated behind `build` exactly like codegen / worker_image. The browser
// parse build (no `build` feature) never compiles a worker, so it never
// needs these.

#[cfg(feature = "build")]
pub use fs_hashes::*;

#[cfg(feature = "build")]
mod fs_hashes {
    use std::collections::{BTreeMap, HashSet};
    use std::path::{Path, PathBuf};

    use anyhow::{Context, Result};
    use sha2::{Digest, Sha256};

    use weft_catalog::{is_node_tree_excluded, FsCatalog};
    use weft_core::project::ProjectDefinition;

    use super::{hex, strip_keys, SourceHash};
    use crate::project::Project;

    /// Dockerfile (relative to the weft root) that builds the shared
    /// worker builder-base image. Part of the worker build environment:
    /// editing it changes the image every worker compiles inside, so it
    /// participates in the binary / infra / builder-base hashes.
    pub const BUILDER_BASE_DOCKERFILE: &str = "deploy/docker/worker-builder-base.Dockerfile";

    /// Fold the worker build environment into `hasher`: the worker crate
    /// closure (`codegen::worker_workspace_crates`, the ONLY workspace
    /// crates a worker build links), the workspace manifests + toolchain
    /// pin, and the builder-base Dockerfile (the image every worker
    /// compiles inside). Shared by the binary, infra and builder-base
    /// hashes: a toolchain bump or a builder-base edit must flip all
    /// three, otherwise `image_present` short-circuits and a stale worker
    /// keeps running forever. Scoping to the closure (not all of
    /// `crates/`) is what keeps an edit to the CLI / dispatcher / tests
    /// from spuriously rebuilding the base + every worker image.
    fn hash_worker_build_env(hasher: &mut Sha256, weft_root: &Path) -> Result<()> {
        let closure = crate::codegen::worker_workspace_crates(weft_root)
            .map_err(|e| anyhow::anyhow!("worker crate closure: {e}"))?;
        for name in &closure {
            // Same enumerator as the stager (`stage_worker_workspace`):
            // hashed bytes and staged bytes must stay equal, and the
            // crate-root `tests/` dir never reaches a worker build.
            let entries =
                crate::build::worker_crate_entries(&weft_root.join("crates").join(name))
                    .map_err(|e| anyhow::anyhow!("enumerate crate {name}: {e}"))?;
            for (entry_name, entry_path) in entries {
                hash_path(hasher, &format!("crates/{name}/{entry_name}"), &entry_path)?;
            }
        }
        for rel in ["Cargo.toml", "Cargo.lock", "rust-toolchain.toml"] {
            hash_path(hasher, rel, &weft_root.join(rel))?;
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
        project: &Project,
        weft_root: &Path,
        catalog: &FsCatalog,
    ) -> Result<SourceHash> {
        let project_root = project.root.as_path();
        let mut hasher = Sha256::new();
        hasher.update(b"weft-binary-v1\n");
        hash_image_recipe(&mut hasher, project)?;

        // SET of referenced node TYPES: the dispatch table in registry.rs
        // is generated from this. The ORDER doesn't matter (we sort), and
        // the per-node config values live in main.weft (hashed by the
        // definition hash, not here).
        let referenced = crate::codegen::collect_node_types(definition);
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

        // The baked type registry: `write_main_rs` bakes EVERY
        // package's nominal type declarations into the binary (not
        // just the referenced packages'), so a type edit anywhere in
        // the catalog changes the compiled bytes and must flip this
        // digest, or a cached image would silently serve the previous
        // registry.
        hash_type_registry(&mut hasher, catalog);

        // Worker build environment: workspace source the binary links
        // against + the builder-base image it compiles inside.
        hash_worker_build_env(&mut hasher, weft_root)?;

        Ok(hex(&hasher.finalize()))
    }

    /// Fold the catalog's full nominal type registry into the digest
    /// (sorted, so map order can never flip it). Shared by the binary
    /// hash and the node-test hash: both bake this registry into their
    /// generated `main.rs`.
    fn hash_type_registry(hasher: &mut Sha256, catalog: &FsCatalog) {
        let mut entries = catalog.type_registry().nominal_entries();
        entries.sort();
        hasher.update(b"type_registry:\n");
        for (name, body) in entries {
            hasher.update(b"  ");
            hasher.update(name.as_bytes());
            hasher.update(b"=");
            hasher.update(body.as_bytes());
            hasher.update(b"\n");
        }
    }

    /// Hash the image RECIPE a staged build renders through
    /// `worker_image::emit`: `weft.toml`'s build choices (base image,
    /// custom template path, crate name) and, when a custom template
    /// is set, the template file's own content. Shared by the binary
    /// hash and the node-test hash so every image-naming digest covers
    /// the same recipe inputs; a base-image or template edit flips
    /// both instead of silently serving a stale cached image.
    fn hash_image_recipe(hasher: &mut Sha256, project: &Project) -> Result<()> {
        let project_root = project.root.as_path();
        // Project configs live in main.weft, not weft.toml, so this
        // stays image-scoped.
        hash_path(hasher, "weft.toml", &project_root.join("weft.toml"))?;

        // A custom Dockerfile template's CONTENT shapes the image too;
        // weft.toml only carries its path, so an edit to the template
        // file itself would otherwise never trigger a rebuild. A
        // set-but-MISSING template path is a config error: fail loudly
        // here rather than hashing the absence (which would let the
        // build proceed to a docker failure later with a less obvious
        // cause).
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
            hash_path(hasher, "dockerfile_template", &template_path)?;
        }
        Ok(())
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

    /// Content hash of everything that can change a package's node-test
    /// OUTCOME: the package's own sources plus the catalog's nominal
    /// type registry (the test binary bakes EVERY package's type
    /// declarations and resolves this package's port types through
    /// them, so a sibling's type edit changes this package's results).
    /// Excludes the image recipe and the worker build environment on
    /// purpose: those rebuild the test image without changing what a
    /// test does, and live tests spend real provider money.
    pub fn compute_node_test_outcome_hash(
        package_root: &Path,
        bases: &[&Path],
        catalog: &FsCatalog,
    ) -> Result<SourceHash> {
        let mut hasher = Sha256::new();
        hasher.update(b"weft-node-test-outcome-v1\n");
        hash_package_roots(&mut hasher, std::slice::from_ref(&package_root.to_path_buf()), bases)?;
        hash_type_registry(&mut hasher, catalog);
        Ok(hex(&hasher.finalize()))
    }

    /// Content hash of ONE package's node-test artifact: the image
    /// recipe (weft.toml build choices + custom template content, the
    /// same inputs the binary hash covers), the package's own sources
    /// (which include every member's `tests.rs`; the package walk
    /// hashes all files), and the worker build environment the test
    /// binary compiles against (engine closure, manifests, toolchain,
    /// builder-base Dockerfile). This is the staleness rule for a
    /// per-package test binary/image: change the base image, a node, a
    /// test, or the engine and the hash flips; change nothing and a
    /// cached artifact is current.
    pub fn compute_node_test_hash(
        package_root: &Path,
        project: &Project,
        weft_root: &Path,
        catalog: &FsCatalog,
    ) -> Result<SourceHash> {
        let mut hasher = Sha256::new();
        hasher.update(b"weft-node-test-v1\n");
        hash_image_recipe(&mut hasher, project)?;
        hash_package_roots(
            &mut hasher,
            std::slice::from_ref(&package_root.to_path_buf()),
            &[project.root.as_path(), weft_root],
        )?;
        // The test crate's main.rs bakes the FULL catalog type
        // registry (see the binary hash's identical step): a type
        // edit in a SIBLING package changes this package's test
        // binary, so it must flip this digest too.
        hash_type_registry(&mut hasher, catalog);
        hash_worker_build_env(&mut hasher, weft_root)?;
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
        hasher.update(b"weft-infra-v2\n");

        // Graph definition, scoped to the infra closure: which nodes are
        // infra, what config they carry, and the wires among them. An edit
        // can flip a node into / out of the closure or rewire an upstream,
        // and the closure slice sees all of that. NOT the raw main.weft:
        // hashing the whole file lit the Upgrade button for every edit to
        // the rest of the graph (an LLM prompt tweak has no bearing on the
        // running bridge pod). Same canonical form as the definition hash:
        // spans / positions / file-ref paths stripped, nodes and edges
        // sorted, so a comment or a canvas drag cannot flip it either.
        let closure = closure_with_upstream(project, |n| n.requires_infra);
        hash_definition_slice(&mut hasher, project, &closure)?;
        hash_path(&mut hasher, "weft.toml", &project_root.join("weft.toml"))?;

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
    pub fn compute_image_hash(node_type: &str, image_source_dir: &Path) -> Result<SourceHash> {
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

    /// Recursive directory walk that returns every regular file under
    /// `root`, skipping the shared node-tree exclude set
    /// (`weft_catalog::NODE_TREE_EXCLUDE`). Symlinked directories are
    /// followed (a cycle fails loudly via the descent chain), so a
    /// linked shared catalog is hashed at every path it appears at,
    /// matching what the stage copy materializes.
    /// This is the hash side of the one node-tree walk policy: it must
    /// see exactly the bytes the build stages, or a missed/extra file
    /// silently de/over-syncs the worker-image hash. Order is not stable;
    /// callers that need deterministic order sort the returned vec.
    pub fn walk_dir(root: &Path) -> Result<Vec<PathBuf>> {
        walk_dir_skipping(root, &[])
    }

    /// [`walk_dir`], with whole top-level directories of `root` left
    /// out. For an input whose consumer reads only part of a directory
    /// (the system images compile a crate's binaries, so its `tests/`
    /// has no bearing on what comes out).
    pub fn walk_dir_skipping(root: &Path, skip_top_level: &[&str]) -> Result<Vec<PathBuf>> {
        let mut out = Vec::new();
        let mut chain = Vec::new();
        walk_into(root, skip_top_level, &mut out, &mut chain)?;
        Ok(out)
    }

    // Symlinks are followed (a symlinked `nodes/base_catalog` has to
    // hash as the bytes it resolves to, the same view the staging copy
    // and the pack take); the descent chain turns a symlink cycle into
    // a loud error instead of an infinite walk.
    fn walk_into(
        dir: &Path,
        skip_top_level: &[&str],
        out: &mut Vec<PathBuf>,
        chain: &mut Vec<PathBuf>,
    ) -> Result<()> {
        let canon = weft_catalog::guard_node_tree_cycle(dir, chain)
            .with_context(|| format!("walk {}", dir.display()))?;
        chain.push(canon);
        let at_top = chain.len() == 1;
        let result = (|| {
            for entry in std::fs::read_dir(dir)
                .with_context(|| format!("read_dir {}", dir.display()))?
            {
                let entry = entry?;
                let name = entry.file_name().to_string_lossy().into_owned();
                if is_node_tree_excluded(&name)
                    || (at_top && skip_top_level.contains(&name.as_str()))
                {
                    continue;
                }
                let path = entry.path();
                match weft_catalog::node_tree_entry_kind(&path)
                    .with_context(|| format!("stat {}", path.display()))?
                {
                    weft_catalog::NodeTreeEntryKind::Dir => {
                        walk_into(&path, skip_top_level, out, chain)?
                    }
                    weft_catalog::NodeTreeEntryKind::File => out.push(path),
                }
            }
            Ok(())
        })();
        chain.pop();
        result
    }

    /// [`hash_path`], with whole top-level directories of `path` left out.
    ///
    /// For an input whose consumer reads only part of a directory. The system
    /// images compile a crate's binaries, so its `tests/` has no bearing on
    /// what comes out; hashing it anyway would rebuild four images every time
    /// somebody edits a test.
    pub fn hash_path_skipping(
        hasher: &mut Sha256,
        label: &str,
        path: &Path,
        skip: &[&str],
    ) -> Result<()> {
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
            hash_dir(hasher, label, path, skip)
        } else {
            Ok(())
        }
    }

    /// Path-into-hasher: file → label + content; dir → label + recursive
    /// sorted walk. Skips target/, node_modules/, .git/, .weft/. `label`
    /// is the machine-independent name folded into the digest in place
    /// of the (possibly absolute) on-disk path: hashing absolute paths
    /// would flip every hash when the checkout moves directories.
    /// Public so the image-stamp hasher in the CLI's `images.rs` can
    /// share the exact same framing rules (no
    /// two-different-hash-functions-for-the-same-job drift).
    pub fn hash_path(hasher: &mut Sha256, label: &str, path: &Path) -> Result<()> {
        hash_path_skipping(hasher, label, path, &[])
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

    fn hash_dir(hasher: &mut Sha256, label: &str, dir: &Path, skip: &[&str]) -> Result<()> {
        hasher.update(b"dir:");
        hasher.update(label.as_bytes());
        hasher.update(b"\n");
        let mut entries = walk_dir_skipping(dir, skip)?;
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
            // `walk_dir` yields files only (dirs recursed, symlinks resolved), so
            // read unconditionally; a non-file here fails loud via `read`.
            let bytes = std::fs::read(&entry)
                .with_context(|| format!("read {} for hashing", entry.display()))?;
            hasher.update(&bytes);
            hasher.update(b"\n");
        }
        Ok(())
    }

    /// Fold the closure's slice of the definition into the hasher: the
    /// closure's nodes and the edges between them, in the definition
    /// hash's canonical form (same non-semantic strips, see
    /// [`compute_definition_hash`]), sorted by id so source order and
    /// unrelated graph edits cannot move the digest.
    fn hash_definition_slice(
        hasher: &mut Sha256,
        project: &ProjectDefinition,
        closure: &HashSet<String>,
    ) -> Result<()> {
        let mut nodes: Vec<serde_json::Value> = project
            .nodes
            .iter()
            .filter(|n| closure.contains(&n.id))
            .map(serde_json::to_value)
            .collect::<Result<_, _>>()
            .map_err(|e| anyhow::anyhow!("serialize infra-closure node: {e}"))?;
        nodes.sort_by(|a, b| a["id"].as_str().cmp(&b["id"].as_str()));
        let mut edges: Vec<serde_json::Value> = project
            .edges
            .iter()
            .filter(|e| closure.contains(&e.source) && closure.contains(&e.target))
            .map(serde_json::to_value)
            .collect::<Result<_, _>>()
            .map_err(|e| anyhow::anyhow!("serialize infra-closure edge: {e}"))?;
        edges.sort_by(|a, b| a["id"].as_str().cmp(&b["id"].as_str()));
        let mut slice = serde_json::json!({ "nodes": nodes, "edges": edges });
        strip_keys(slice.get_mut("nodes"), super::NODE_HASH_STRIPS);
        strip_keys(slice.get_mut("edges"), super::EDGE_HASH_STRIPS);
        hasher.update(b"definition-slice:");
        hasher.update(
            &serde_json::to_vec(&slice)
                .map_err(|e| anyhow::anyhow!("serialize infra-closure slice: {e}"))?,
        );
        hasher.update(b"\n");
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
                if edge.target == id && by_id.contains_key(edge.source.as_str())
                    && !closure.contains(&edge.source) {
                        frontier.push(edge.source.clone());
                    }
            }
        }
        closure
    }

    #[cfg(test)]
    mod infra_slice_tests {
        use super::*;

        /// Three-node project: `a -> b` where `b` is infra, plus an
        /// unrelated `x`. Closure = {a, b}. The caller varies one
        /// node's config to probe what moves the slice digest.
        fn project(a_cfg: &str, b_cfg: &str, x_cfg: &str, b_x: f64) -> ProjectDefinition {
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
            serde_json::from_value(serde_json::json!({
                "id": "00000000-0000-0000-0000-000000000000",
                "nodes": [
                    node("a", a_cfg, false, 0.0),
                    node("b", b_cfg, true, b_x),
                    node("x", x_cfg, false, 0.0),
                ],
                "edges": [{
                    "id": "a.out->b.in", "source": "a", "target": "b",
                    "sourceHandle": "out", "targetHandle": "in",
                }],
                "groups": [],
                "createdAt": "2026-01-01T00:00:00Z",
                "updatedAt": "2026-01-01T00:00:00Z",
            }))
            .expect("test ProjectDefinition")
        }

        fn slice_digest(p: &ProjectDefinition) -> String {
            let closure = closure_with_upstream(p, |n| n.requires_infra);
            let mut hasher = Sha256::new();
            hash_definition_slice(&mut hasher, p, &closure).unwrap();
            hex(&hasher.finalize())
        }

        #[test]
        fn a_config_edit_outside_the_closure_does_not_move_the_hash() {
            assert_eq!(
                slice_digest(&project("a1", "b1", "x1", 0.0)),
                slice_digest(&project("a1", "b1", "CHANGED", 0.0)),
            );
        }

        #[test]
        fn a_closure_config_edit_moves_the_hash() {
            let base = slice_digest(&project("a1", "b1", "x1", 0.0));
            assert_ne!(base, slice_digest(&project("CHANGED", "b1", "x1", 0.0)));
            assert_ne!(base, slice_digest(&project("a1", "CHANGED", "x1", 0.0)));
        }

        #[test]
        fn a_canvas_drag_does_not_move_the_hash() {
            assert_eq!(
                slice_digest(&project("a1", "b1", "x1", 0.0)),
                slice_digest(&project("a1", "b1", "x1", 500.0)),
            );
        }

        /// A literal's SPAN is a source-text coordinate: a comment
        /// added above the node shifts it without changing the graph,
        /// and the Upgrade button must not light up for that.
        #[test]
        fn a_shifted_port_literal_span_does_not_move_the_hash() {
            let mut with_span = project("a1", "b1", "x1", 0.0);
            with_span.nodes[1].port_literals.insert("v".into(), serde_json::json!("lit"));
            let mut shifted = with_span.clone();
            use weft_core::project::{ConfigFieldSpan, Span};
            let span_at = |line| ConfigFieldSpan::inline(Span {
                start_line: line,
                start_column: 3,
                end_line: line,
                end_column: 9,
            });
            with_span.nodes[1].port_literal_spans.insert("v".into(), span_at(2));
            shifted.nodes[1].port_literal_spans.insert("v".into(), span_at(7));
            assert_eq!(slice_digest(&with_span), slice_digest(&shifted));
            assert_eq!(
                super::super::compute_definition_hash(&with_span).unwrap(),
                super::super::compute_definition_hash(&shifted).unwrap(),
            );
        }
    }

    /// Load + enrich a project to a `ProjectDefinition` AND return the
    /// catalog it was enriched against, without running cargo / docker.
    /// Returns both because every caller (drift hashes, infra build) needs
    /// the same catalog the definition was built from; returning it here is
    /// one discovery per command instead of each caller re-walking `nodes/`.
    pub fn load_enriched_project(project: &Project) -> Result<(ProjectDefinition, FsCatalog)> {
        load_enriched_project_with_diagnostics(project).map_err(|e| anyhow::anyhow!("{e}"))
    }

    /// Same as `load_enriched_project` but on compile failure the Err
    /// carries the structured `Vec<Diagnostic>` so callers that surface
    /// errors to the editor (the CLI's run path emitting structured
    /// progress events) can render them one-per-line rather than as a
    /// single flattened string.
    pub fn load_enriched_project_with_diagnostics(
        project: &Project,
    ) -> std::result::Result<(ProjectDefinition, FsCatalog), CompileLoadError> {
        use crate::build::build_project_catalog;
        use crate::compile_enriched_with_diagnostics;
        let source = project
            .read_main_weft()
            .map_err(|e| CompileLoadError::Read(format!("read main.weft: {e}")))?;
        let catalog = build_project_catalog(&project.root)
            .map_err(|e| CompileLoadError::Read(format!("catalog: {e}")))?;
        let fs = crate::CompileFs::disk(&project.root);
        let definition = compile_enriched_with_diagnostics(&source, project.id(), fs, &catalog)
            .map_err(CompileLoadError::Diagnostics)?;
        Ok((definition, catalog))
    }

    /// Error envelope for the diagnostic-bearing loader. `Read` covers
    /// I/O failures (source + catalog discovery), `Diagnostics` covers
    /// compile failures with their structured per-error list.
    #[derive(Debug)]
    pub enum CompileLoadError {
        Read(String),
        Diagnostics(Vec<crate::Diagnostic>),
    }

    impl std::fmt::Display for CompileLoadError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                CompileLoadError::Read(msg) => write!(f, "{msg}"),
                // Same rendering as the compiler's own abort path.
                CompileLoadError::Diagnostics(diags) => {
                    write!(f, "{}", crate::render_diagnostics(diags))
                }
            }
        }
    }

    impl std::error::Error for CompileLoadError {}
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

    /// Regression: the project `id` is DB identity, NOT runtime shape. The
    /// browser WASM parse computes the live-preview hash with the NIL uuid (the
    /// id is not a parse input), while the build/dispatcher computes the stored
    /// hash with the project's real id. If `id` fed the hash, those two would
    /// NEVER agree and the "out of sync / resync" light would be permanently
    /// stuck on for every activated project. The id is already the OTHER half of
    /// the `(project_id, definition_hash)` identity key, so it is redundant here.
    #[test]
    fn definition_hash_ignores_project_id() {
        let plain = project_at("2024-01-01T00:00:00Z", "hi");
        let mut other = serde_json::to_value(&plain).unwrap();
        other["id"] = serde_json::json!("11111111-2222-3333-4444-555555555555");
        let other: ProjectDefinition = serde_json::from_value(other).unwrap();
        let h1 = compute_definition_hash(&plain).unwrap();
        let h2 = compute_definition_hash(&other).unwrap();
        assert_eq!(h1, h2, "a different project id must hash identically");
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

    /// Regression: a `@file` field's `fileRefs` path and an `@include`
    /// node's `includePath` are non-semantic (the RESOLVED value lives in
    /// `config`, which IS hashed; the path is editor-routing / navigation
    /// metadata). Renaming the referenced file WITHOUT changing the resolved
    /// value must not flip the definition hash, or every file rename would
    /// light the resync drift signal spuriously.
    #[test]
    fn definition_hash_ignores_file_ref_and_include_paths() {
        let plain = project_at("2024-01-01T00:00:00Z", "hi");
        let mut renamed = serde_json::to_value(&plain).unwrap();
        let node = &mut renamed["nodes"][0];
        // Same resolved config value ("hi"), only the source-reference paths differ.
        node["fileRefs"] =
            serde_json::json!({ "value": { "path": "renamed.txt", "type": "String", "marker": "file" } });
        node["includePath"] = serde_json::json!("some/other/path.weft");
        let renamed: ProjectDefinition = serde_json::from_value(renamed).unwrap();
        let h1 = compute_definition_hash(&plain).unwrap();
        let h2 = compute_definition_hash(&renamed).unwrap();
        assert_eq!(h1, h2, "file-ref / include path-only differences must hash identically");
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

    /// The content source hash is pure over the file set and order-
    /// independent: the dispatcher computes it on `/projects/create`
    /// before any compile, and must derive the SAME key from the rows it
    /// fetches at build time regardless of row order.
    #[test]
    fn source_hash_is_order_independent() {
        let a = compute_source_hash(&[
            ("main.weft".into(), "graph {}".into()),
            ("nodes/x/mod.rs".into(), "fn x() {}".into()),
        ]);
        let b = compute_source_hash(&[
            ("nodes/x/mod.rs".into(), "fn x() {}".into()),
            ("main.weft".into(), "graph {}".into()),
        ]);
        assert_eq!(a, b, "file order must not change the source hash");
    }

    /// Source hash flips on any content or path change, so a build
    /// dedup keyed by it can't serve a stale image for edited source.
    #[test]
    fn source_hash_flips_on_content_and_path() {
        let base = compute_source_hash(&[("main.weft".into(), "graph {}".into())]);
        let edited = compute_source_hash(&[("main.weft".into(), "graph { a }".into())]);
        let renamed = compute_source_hash(&[("other.weft".into(), "graph {}".into())]);
        assert_ne!(base, edited, "content edit must flip the source hash");
        assert_ne!(base, renamed, "path change must flip the source hash");
    }
}
