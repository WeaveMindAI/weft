//! Source-hash functions used by drift detection + image tagging.
//!
//! These live in `weft-compiler` (not the CLI) so that any build site shares one
//! hashing + planning brain: whoever compiles a project computes the
//! authoritative hashes here before staging the build context. One definition, no
//! two-hashers-drift hazard.
//!
//! Three project-level hashes split the user-visible drift signals
//! cleanly along their reason for changing:
//!
//! - **`compute_binary_hash`**: hashes everything that affects the
//!   WORKER BINARY's bytes. Drives the worker docker image tag.
//!   Inputs:
//!     - `weft.toml` `[build.worker]` choices (base image, custom
//!       Dockerfile template). Nothing else in weft.toml reaches the
//!       binary: every worker compiles as the crate `weft-worker`.
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


/// The canonical form of a program and the pure digests over it live in
/// core, so the dispatcher and the browser parse build read the same
/// bytes this crate's build hashes fold: the definition hash, the
/// per-node slice hash, and the hex encoding.
pub use weft_core::project::hash::{compute_definition_hash, hex, SourceHash};

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
    use std::path::{Path, PathBuf};

    use anyhow::{Context, Result};
    use sha2::{Digest, Sha256};

    use weft_catalog::{is_node_tree_excluded, FsCatalog};
    use weft_core::project::ProjectDefinition;

    use super::{hex, SourceHash};
    use weft_core::project::hash::hash_definition_slice;
    use weft_core::project::{infra_ids, upstream_closure, EdgeIndex};
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
        // The stdlib packages' dependency declarations. The builder base
        // compiles the stock worker, whose dependency tree (and the
        // feature unification of every shared crate in it) is the fixed
        // set plus what these files add; a change here makes the baked
        // rlibs and the host compile cache dead weight, a node body edit
        // does not (that only moves one package's content slot).
        let stdlib = weft_catalog::stdlib_root().map_err(|e| anyhow::anyhow!("{e}"))?;
        for path in walk_dir(&stdlib)?.into_iter().filter(|p| p.file_name().is_some_and(|n| n == "deps.toml")) {
            let rel = path.strip_prefix(&stdlib).map_err(|_| anyhow::anyhow!("{} is outside the stdlib", path.display()))?;
            hash_path(hasher, &format!("catalog/{}", rel.display()), &path)?;
        }
        Ok(())
    }

    /// Hash everything that compiles into the worker binary. Used as
    /// the worker docker tag suffix; the dispatcher selects the spawn
    /// image by this hash.
    ///
    /// Scoped to exactly the codegen-emitted-static surface: weft.toml's
    /// `[build.worker]` section, the SET of referenced node TYPES
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
        node_set: crate::codegen::NodeSet,
    ) -> Result<SourceHash> {
        let project_root = project.root.as_path();
        let mut hasher = Sha256::new();
        hasher.update(b"weft-binary-v2\n");
        hash_image_recipe(&mut hasher, project)?;

        // SET of referenced node TYPES: the dispatch table in registry.rs
        // is generated from this. The ORDER doesn't matter (we sort), and
        // the per-node config values live in main.weft (hashed by the
        // definition hash, not here).
        // The choice itself is hashed too, so the tag says what the
        // image was built AS: `weft status` can tell a full build from
        // a referenced one instead of guessing from the type list.
        let referenced = crate::codegen::node_types_for(definition, catalog, node_set);
        hasher.update(b"node_set:");
        hasher.update(node_set.hash_marker().as_bytes());
        hasher.update(b"\n");
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

    /// Production implementation identity per node type. Package helpers and
    /// dependencies are shared by its members; unrelated packages stay independent.
    pub fn implementation_hashes(
        definition: &ProjectDefinition,
        project: &Project,
        weft_root: &Path,
        catalog: &FsCatalog,
        node_set: crate::codegen::NodeSet,
    ) -> Result<std::collections::BTreeMap<String, SourceHash>> {
        let mut shared = Sha256::new();
        shared.update(b"weft-implementation-v1\n");
        hash_worker_build_env(&mut shared, weft_root)?;
        hash_image_recipe(&mut shared, project)?;
        hash_type_registry(&mut shared, catalog);
        // Fingerprints describe the compiled worker, whose type set can be
        // wider than this graph. Built-in boundaries (a group's, a loop's,
        // a call site's and a body's halves) always ship in the engine.
        let mut types = crate::codegen::node_types_for(definition, catalog, node_set);
        types.extend(crate::weft_compiler::RESERVED_TYPE_KEYWORDS.iter().map(|t| (*t).to_string()));
        types.extend(weft_core::project::boundary_types::ALL.iter().map(|t| (*t).to_string()));
        let mut packages: std::collections::BTreeMap<Vec<PathBuf>, SourceHash> = std::collections::BTreeMap::new();
        let mut result = std::collections::BTreeMap::new();
        for node_type in types {
            let roots = catalog.package_roots_for(&[node_type.clone()].into_iter().collect());
            let hash = if let Some(hash) = packages.get(&roots) {
                hash.clone()
            } else {
                let mut hasher = shared.clone();
                hash_package_roots(&mut hasher, &roots, &[&project.root, weft_root])?;
                let hash = hex(&hasher.finalize());
                packages.insert(roots, hash.clone());
                hash
            };
            result.insert(node_type, hash);
        }
        Ok(result)
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
    /// custom template path) and, when a custom template
    /// is set, the template file's own content. Shared by the binary
    /// hash and the node-test hash so every image-naming digest covers
    /// the same recipe inputs; a base-image or template edit flips
    /// both instead of silently serving a stale cached image.
    fn hash_image_recipe(hasher: &mut Sha256, project: &Project) -> Result<()> {
        let project_root = project.root.as_path();
        // Names, IDs and dispatcher addresses do not change executable code.
        hasher.update(b"worker_build:\n");
        hasher.update(serde_json::to_vec(&project.manifest.build.worker)?);

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
        let mut labeled = std::collections::BTreeMap::new();
        for root in roots {
            let label = bases
                .iter()
                .enumerate()
                .find_map(|(index, base)| root.strip_prefix(base).ok().map(|path| format!("{index}/{}", path.to_string_lossy())))
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "package root {} is outside the project and weft roots; \
                         hashing its absolute path would make the digest machine-local",
                        root.display()
                    )
                })?;
            labeled.insert(label, root);
        }
        // Sort by portable labels, not absolute paths whose order can change
        // when a project and the weft checkout move independently.
        for (label, root) in labeled {
            hasher.update(b"package:");
            hasher.update(label.as_bytes());
            hasher.update(b"\n");
            hash_path(hasher, &label, root)?;
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
        let closure = upstream_closure(project, &EdgeIndex::build(project), &infra_ids(project));
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

    /// Name of the BuildKit cache every worker `cargo build` on this host
    /// compiles into (`weft_worker_cache_key` in the rendered Dockerfile).
    /// One cache per (build environment, builder): the same inputs as
    /// the builder base, plus `builder` (the prebuilt base, or the raw
    /// image a from-scratch builder stage installs its toolchain on: C
    /// artifacts compiled against one distro's headers must never be
    /// linked by another). Inside the cache, cargo tells the generated
    /// package crates apart by their content-addressed directory
    /// (`codegen::write_package_crates`) and everything else by
    /// fingerprint.
    pub fn compute_worker_cache_key(weft_root: &Path, builder: &str) -> Result<String> {
        let mut hasher = Sha256::new();
        hasher.update(b"weft-worker-cache-v1\n");
        hasher.update(b"builder:");
        hasher.update(builder.as_bytes());
        hasher.update(b"\n");
        hash_worker_build_env(&mut hasher, weft_root)?;
        Ok(hex(&hasher.finalize()).chars().take(16).collect())
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
            .map_err(|e| CompileLoadError::Read(format!("read {}: {e}", project.main_weft().display())))?;
        let catalog = build_project_catalog(&project.root)
            .map_err(|e| CompileLoadError::Read(format!("catalog: {e}")))?;
        let src_dir = project.src_dir();
        let fs = crate::CompileFs::disk(&project.root).anchored_at(Some(&src_dir));
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

    #[cfg(test)]
    mod package_identity_tests {
        use super::*;

        #[test]
        fn implementation_identity_matches_the_compiled_type_set() {
            let dir = tempfile::tempdir().unwrap();
            std::fs::write(dir.path().join("weft.toml"), "[package]\nname = 'test'\nid = '00000000-0000-0000-0000-000000000001'\n").unwrap();
            let project = Project::load(dir.path()).unwrap();
            let catalog = FsCatalog::discover(&weft_catalog::stdlib_root().unwrap()).unwrap();
            let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap().parent().unwrap();
            let empty: ProjectDefinition = serde_json::from_value(serde_json::json!({"id":project.id(),"nodes":[],"edges":[]})).unwrap();
            let mut grouped = empty.clone();
            grouped.nodes.push(serde_json::from_value(serde_json::json!({
                "id":"g__in", "nodeType":"Passthrough", "inputs":[], "outputs":[], "position":{"x":0,"y":0},
                "groupBoundary":{"groupId":"g","role":"In"}
            })).unwrap());
            for set in [crate::codegen::NodeSet::Full, crate::codegen::NodeSet::Referenced] {
                let before = implementation_hashes(&empty, &project, root, &catalog, set).unwrap();
                let after = implementation_hashes(&grouped, &project, root, &catalog, set).unwrap();
                assert_eq!(before, after, "adding a built-in boundary does not change the worker's implementation map");
                assert!(before.contains_key("Passthrough") && before.contains_key("IncludeIn") && before.contains_key("CallOut"));
                if matches!(set, crate::codegen::NodeSet::Full) { assert!(before.contains_key("Text")); }
            }
        }

        #[test]
        fn worker_cache_key_is_stable_per_builder() {
            let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap().parent().unwrap();
            let prebuilt = compute_worker_cache_key(root, "builder-base").unwrap();
            assert_eq!(prebuilt.len(), 16);
            assert_eq!(compute_worker_cache_key(root, "builder-base").unwrap(), prebuilt);
            assert_ne!(
                compute_worker_cache_key(root, "alpine:3.19").unwrap(),
                prebuilt,
                "a from-scratch builder on another distro gets its own cache"
            );
        }

        #[test]
        fn repeated_full_builds_and_config_edits_keep_the_same_image_identity() {
            let dir = tempfile::tempdir().unwrap();
            std::fs::write(dir.path().join("weft.toml"), "[package]\nname = 'test'\nid = '00000000-0000-0000-0000-000000000001'\n").unwrap();
            let project = Project::load(dir.path()).unwrap();
            let catalog = FsCatalog::discover(&weft_catalog::stdlib_root().unwrap()).unwrap();
            let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap().parent().unwrap();
            let mut graph: ProjectDefinition = serde_json::from_value(serde_json::json!({
                "id":project.id(), "nodes":[{"id":"text", "nodeType":"Text", "config":{"value":"before"}, "position":{"x":0,"y":0}}], "edges":[]
            })).unwrap();
            let hash = |graph: &ProjectDefinition| compute_binary_hash(graph, &project, root, &catalog, crate::codegen::NodeSet::Full).unwrap();
            let initial = hash(&graph);
            assert_eq!(hash(&graph), initial);
            graph.nodes[0].config = serde_json::json!({"value":"after"});
            assert_eq!(hash(&graph), initial, "config belongs to the execution definition");
            graph.nodes[0].node_type = "Format".into();
            assert_eq!(hash(&graph), initial, "all catalog implementations were already compiled");
        }

        fn digest(roots: &[PathBuf], bases: &[&Path]) -> String {
            let mut hash = Sha256::new();
            hash_package_roots(&mut hash, roots, bases).unwrap();
            hex(&hash.finalize())
        }

        #[test]
        fn moving_checkouts_does_not_change_package_order_or_identity() {
            let temp = tempfile::tempdir().unwrap();
            let mut hashes = Vec::new();
            for (project_name, weft_name) in [("a-project", "z-weft"), ("z-project", "a-weft")] {
                let project = temp.path().join(project_name);
                let weft = temp.path().join(weft_name);
                let roots = [project.join("nodes/local"), weft.join("catalog/shared")];
                for (root, text) in roots.iter().zip(["local body", "shared helper"]) {
                    std::fs::create_dir_all(root).unwrap();
                    std::fs::write(root.join("mod.rs"), text).unwrap();
                }
                let hash = digest(&roots, &[&project, &weft]);
                assert_eq!(hash, digest(&[roots[1].clone(), roots[0].clone(), roots[0].clone()], &[&project, &weft]));
                hashes.push(hash);
            }
            assert_eq!(hashes[0], hashes[1]);
        }

        #[test]
        fn package_helpers_and_dependencies_invalidate_only_their_package() {
            let temp = tempfile::tempdir().unwrap();
            let a = temp.path().join("nodes/a");
            let b = temp.path().join("nodes/b");
            for root in [&a, &b] {
                std::fs::create_dir_all(root).unwrap();
                std::fs::write(root.join("mod.rs"), "node body").unwrap();
            }
            let hash = |root: &PathBuf| digest(std::slice::from_ref(root), &[temp.path()]);
            let unchanged = hash(&b);
            let mut previous = hash(&a);
            for file in ["helper.rs", "deps.toml"] {
                std::fs::write(a.join(file), "new dependency").unwrap();
                let next = hash(&a);
                assert_ne!(previous, next);
                assert_eq!(hash(&b), unchanged);
                previous = next;
            }
            std::fs::create_dir_all(a.join("target")).unwrap();
            std::fs::write(a.join("target/output"), "build artifact").unwrap();
            assert_eq!(previous, hash(&a));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use weft_core::project::ProjectDefinition;

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
                "inputs": [{ "name": "a", "portType": "String", "required": true }],
                "outputs": [{ "name": "out", "portType": "String", "required": true }],
                "features": {},
                "scope": [],
                "groupBoundary": null,
                "requiresInfra": false,
                "images": [],
            }],
            "edges": [{
                "id": "e",
                "source": "n",
                "target": "n",
                "sourceHandle": null,
                "targetHandle": null,
            }],
            "groups": [{
                "id": "g",
                "kind": "group",
                "label": null,
                "inPorts": [{ "name": "gi", "portType": "String", "required": true }],
                "outPorts": [{ "name": "go", "portType": "String", "required": true }],
            }],
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
    /// Also pins every other non-runtime member the hash strips: diagnostic
    /// anchors, the header's declared spelling, and port / group prose.
    #[test]
    fn definition_hash_ignores_non_runtime_fields() {
        let plain = project_at("2024-01-01T00:00:00Z", "hi");
        let mut renamed = serde_json::to_value(&plain).unwrap();
        let node = &mut renamed["nodes"][0];
        // Same resolved config value ("hi"), only the source-reference paths differ.
        node["fileRefs"] =
            serde_json::json!({ "value": { "path": "renamed.txt", "type": "String", "marker": "file" } });
        node["includePath"] = serde_json::json!("some/other/path.weft");
        // The same project compiled from another directory stamps different
        // absolute diagnostic anchors on nodes, edges AND groups; the
        // runtime graph is unchanged.
        node["sourceFile"] = serde_json::json!("/somewhere/else/main.weft");
        // A header restating a port (the editor's healing later drops
        // it) differs only in `declaredType`; the runtime shape is
        // identical, so the hash must not move.
        node["inputs"][0]["declaredType"] = serde_json::json!("String");
        // Prose is not shape: a catalog reworded a port's description,
        // an author reworded a group's `# ...` comment.
        node["inputs"][0]["description"] = serde_json::json!("catalog prose changed");
        node["inputs"][0]["label"] = serde_json::json!("Prompt text");
        node["inputs"][0]["placeholder"] = serde_json::json!("Type here");
        // Every port list the strip walks: node outputs, group interface.
        node["outputs"][0]["declaredType"] = serde_json::json!("String");
        node["outputs"][0]["description"] = serde_json::json!("reworded");
        renamed["groups"][0]["inPorts"][0]["description"] = serde_json::json!("reworded");
        renamed["groups"][0]["outPorts"][0]["description"] = serde_json::json!("reworded");
        renamed["edges"][0]["sourceFile"] = serde_json::json!("/somewhere/else/main.weft");
        renamed["groups"][0]["sourceFile"] = serde_json::json!("/somewhere/else/main.weft");
        renamed["groups"][0]["description"] = serde_json::json!("reworded comment");
        let renamed: ProjectDefinition = serde_json::from_value(renamed).unwrap();
        let h1 = compute_definition_hash(&plain).unwrap();
        let h2 = compute_definition_hash(&renamed).unwrap();
        assert_eq!(h1, h2, "file-ref / include path-only differences must hash identically");
    }

    /// The hash serializes through `canonicalize_key_order`, so the map
    /// implementation behind `serde_json::Value` (sorted by default,
    /// insertion-ordered under `preserve_order`, which feature
    /// unification flips between builds) cannot move the digest. The
    /// property is checked directly: two structurally equal values
    /// built in different key orders serialize identically after
    /// canonicalization, at every nesting depth.
    #[test]
    fn canonicalized_serialization_ignores_key_insertion_order() {
        let mut a = serde_json::json!({});
        a["zeta"] = serde_json::json!({ "y": 1, "x": [{ "b": 2, "a": 3 }] });
        a["alpha"] = serde_json::json!(true);
        let mut b = serde_json::json!({});
        b["alpha"] = serde_json::json!(true);
        b["zeta"] = serde_json::json!({ "x": [{ "a": 3, "b": 2 }], "y": 1 });
        weft_core::project::hash::canonicalize_key_order(&mut a);
        weft_core::project::hash::canonicalize_key_order(&mut b);
        assert_eq!(
            serde_json::to_string(&a).unwrap(),
            serde_json::to_string(&b).unwrap(),
            "canonicalized serialization must not depend on key insertion order"
        );
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
