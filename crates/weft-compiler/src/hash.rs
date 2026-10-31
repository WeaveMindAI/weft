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
//!   WORKER BINARY's bytes. Names the worker (its image ref, an image on
//!   the cloud and a program on a local install).
//!   Inputs:
//!     - for a worker image only, `weft.toml` `[build.worker]` choices
//!       (base image, custom Dockerfile template). Nothing else in
//!       weft.toml reaches the binary: every worker compiles as the
//!       crate `weft-worker`.
//!     - the SET of referenced node types (the codegen-emitted
//!       static dispatch table references each by name; adding /
//!       removing a node TYPE changes the binary, but changing a
//!       node's config or rewiring edges does NOT).
//!     - the REFERENCED nodes' packages (mod.rs, deps.toml, shared
//!       package files), each by its package NAME and content, never
//!       by where it sits on disk: a project whose `nodes/base_catalog`
//!       copy matches this weft's standard library names the same
//!       worker as every other such project
//!       ([`compute_stock_binary_hash`]). A change to a node
//!       implementation (re)compiles the worker.
//!     - worker build environment: the worker crate closure
//!       (`codegen::worker_workspace_crates`: the workspace crates a
//!       worker actually links, less what a crate leaves out of workers,
//!       like the database's migration history:
//!       `build::worker_crate_entries`), `Cargo.toml`, `Cargo.lock`,
//!       `rust-toolchain.toml`, and for a worker image the builder-base
//!       Dockerfile and its scripts. Any engine or toolchain change
//!       invalidates every project's worker, a builder-base change every
//!       worker image; edits to non-worker crates (CLI, dispatcher,
//!       tests) do not.
//!
//!   NOT hashed: per-node config values, edges, node ids, positions.
//!   Those live in the `ProjectDefinition` the worker fetches from
//!   the broker at run claim time; they no longer reach the
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
//!   the infra closure (every `requiresInfra` node + every node
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
//! `compute_node_test_hash` (is a package's compiled test server
//! current) and `compute_node_test_outcome_hash` (can a recorded
//! live-tier pass still be trusted).
//!
//! Plus the key of the workspace slice host builds share
//! (`compute_worker_workspace_key`), and two hashes that are docker-tag
//! plumbing only:
//!
//! - **`compute_image_hash`**: per-image source dir hash. Used as the
//!   infra image tag suffix with the FULL content hash
//!   (`weft-infra-<name>:<content_hash>`, assembled by
//!   `image_set::infra_image_tag`, matching the worker tag's full-hash
//!   form) so a stale image source produces a fresh image. NOT a drift
//!   signal anymore: drift is the project-level `infra_hash` exclusively.
//!
//! - **`compute_builder_base_hash`**: global (not per project) hash of
//!   the worker build environment, tagging the shared
//!   `weft-builder-base:<hash>` image the cloud's worker images compile
//!   in. Shares its inputs with the binary and infra hashes, so an edit
//!   to the engine or the toolchain moves all three together.
//!
//! A file enters every digest as its path and its fingerprint (the sha256
//! of its bytes), never its bytes: a build from a stored version takes the
//! fingerprints the version already names ([`Fingerprints::of_version`]),
//! so its image names come from the version itself and the files are not
//! read a second time; everything else reads them off the disk.
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
    use weft_core::project::{graph::ProjectGraph, infra_ids, upstream_closure};
    use crate::project::Project;

    /// Where a file's fingerprint (the sha256 of its bytes) comes from: what
    /// a stored version names for a file it holds under its root, else the
    /// disk. The version's are checked already: every file a build lays out
    /// is hashed against them (the dispatcher's `build::source`).
    #[derive(Debug, Clone, Default)]
    pub struct Fingerprints {
        version: Option<(PathBuf, weft_core::project::hash::Manifest)>,
    }

    /// Every fingerprint read off the disk.
    pub static DISK: Fingerprints = Fingerprints { version: None };

    impl Fingerprints {
        /// The fingerprints of the version laid out at `root`, by `manifest`.
        pub fn of_version(root: PathBuf, manifest: weft_core::project::hash::Manifest) -> Self {
            Self { version: Some((root, manifest)) }
        }

        /// The fingerprint of the file at `path`.
        pub fn of(&self, path: &Path) -> Result<String> {
            if let Some((root, manifest)) = &self.version {
                let named = path.strip_prefix(root).ok().and_then(|rel| rel.to_str()).and_then(|rel| manifest.get(&rel.replace('\\', "/")));
                if let Some(hash) = named {
                    return Ok(hash.clone());
                }
            }
            let mut file = std::fs::File::open(path).with_context(|| format!("read {} for hashing", path.display()))?;
            let mut hasher = Sha256::new();
            let mut chunk = vec![0u8; 64 * 1024];
            loop {
                let read = std::io::Read::read(&mut file, &mut chunk).with_context(|| format!("read {} for hashing", path.display()))?;
                if read == 0 {
                    break;
                }
                hasher.update(&chunk[..read]);
            }
            Ok(hex(&hasher.finalize()))
        }
    }

    /// Dockerfile (relative to the weft root) that builds the shared
    /// worker builder-base image. Part of the worker build environment:
    /// editing it changes the image every worker compiles inside, so it
    /// participates in the binary / infra / builder-base hashes.
    pub const BUILDER_BASE_DOCKERFILE: &str = "deploy/docker/worker-builder-base.Dockerfile";

    /// The script that Dockerfile runs to split the compiled artifacts
    /// into its two image layers. Hashed with the Dockerfile for the
    /// same reason: it decides what the baked image contains, so an
    /// edit here must mint a new base tag rather than leave every host
    /// holding an image built by the old rule.
    pub const BUILDER_BASE_SPLIT_SCRIPT: &str = "deploy/docker/worker-builder-base-split.sh";

    /// The compile-cache sweep (`worker_image::CACHE_GC_SCRIPT`, compiled
    /// in from this file). Hashed with the split script for the same
    /// reason: the builder base runs it before baking its cache, so it
    /// decides what the image holds.
    // SYNC: CACHE_GC_SCRIPT_PATH <-> the include_str! of worker_image::CACHE_GC_SCRIPT
    pub const CACHE_GC_SCRIPT_PATH: &str = "crates/weft-compiler/src/weft-cache-gc.sh";

    /// Fold the weft workspace a worker links into `hasher`: the worker
    /// crate closure (`codegen::worker_workspace_crates`, the ONLY workspace
    /// crates a worker build links) and the workspace manifest and lock,
    /// which is exactly what `build::stage_worker_workspace` writes (its
    /// empty workspace-hack is the same for every weft). Scoping to the
    /// closure (not all of `crates/`) is what keeps an edit to the CLI /
    /// dispatcher / tests from rebuilding every worker.
    fn hash_worker_workspace(hasher: &mut Sha256, weft_root: &Path) -> Result<()> {
        let closure = crate::codegen::worker_workspace_crates(weft_root)
            .map_err(|e| anyhow::anyhow!("worker crate closure: {e}"))?;
        for name in &closure {
            // Same enumerator as the stager (`stage_worker_workspace`):
            // hashed bytes and staged bytes must stay equal, and neither the
            // crate-root `tests/` dir nor what a crate lists as left out of
            // workers (weft-task-store's `migrations/`) reaches a worker
            // build.
            let entries =
                crate::build::worker_crate_entries(&weft_root.join("crates").join(name))
                    .map_err(|e| anyhow::anyhow!("enumerate crate {name}: {e}"))?;
            for (entry_name, entry_path) in entries {
                hash_path(hasher, &format!("crates/{name}/{entry_name}"), &entry_path, &DISK)?;
            }
        }
        for rel in ["Cargo.toml", "Cargo.lock"] {
            hash_path(hasher, rel, &weft_root.join(rel), &DISK)?;
        }
        Ok(())
    }

    /// Fold the worker build environment into `hasher`: the workspace a
    /// worker links ([`hash_worker_workspace`]) and the toolchain pin, and
    /// with `in_image` (a worker image) the builder base it compiles inside (its
    /// Dockerfile, its split script and the cache sweep it runs before
    /// baking). A toolchain bump or a builder-base edit must flip every
    /// digest an image is named by, otherwise a present image short-circuits
    /// its build and a stale worker keeps running forever; a worker
    /// compiled on the machine reads none of the builder base, so an edit
    /// there rebuilds no program.
    fn hash_worker_build_env(hasher: &mut Sha256, weft_root: &Path, in_image: bool) -> Result<()> {
        hash_worker_workspace(hasher, weft_root)?;
        hash_path(hasher, "rust-toolchain.toml", &weft_root.join("rust-toolchain.toml"), &DISK)?;
        if in_image {
            for rel in [BUILDER_BASE_DOCKERFILE, BUILDER_BASE_SPLIT_SCRIPT, CACHE_GC_SCRIPT_PATH] {
                hash_path(hasher, rel, &weft_root.join(rel), &DISK)?;
            }
        }
        Ok(())
    }

    /// The key naming the worker-linked slice of the weft workspace a host
    /// build shares with every other on the machine
    /// (`build::stage_worker_workspace`, under `worker-crates/weft-<key>`):
    /// a digest of exactly what the slice holds, so a weft whose worker
    /// crates did not change shares the slice of the one before it.
    pub fn compute_worker_workspace_key(weft_root: &Path) -> Result<String> {
        let mut hasher = Sha256::new();
        // Bump this marker whenever the code that writes the slice changes
        // (`build::stage_worker_workspace`, `build::write_scoped_workspace_manifest`,
        // `build::stage_empty_workspace_hack`): the key reads only the crates
        // and files the slice copies, and `build::write_once` never rewrites a
        // slice already there, so without the bump every machine keeps the
        // slice the old code wrote.
        hasher.update(b"weft-worker-workspace-v2\n");
        hash_worker_workspace(&mut hasher, weft_root)?;
        Ok(hex(&hasher.finalize()).chars().take(16).collect())
    }

    /// The installation's stdlib packages' dependency declarations. Only
    /// weft's own shared images read them: the builder base compiles the
    /// stock worker, whose dependency tree (and the feature unification
    /// of every shared crate in it) is the fixed set plus what these files
    /// add, so a change here makes the baked rlibs and the host compile
    /// cache dead weight. A user project's hashes never read them: its
    /// own `nodes/base_catalog/` deps.toml files are hashed with the rest
    /// of each referenced package (`hash_packages`).
    fn hash_installed_stdlib_deps(hasher: &mut Sha256) -> Result<()> {
        let stdlib = weft_catalog::stdlib_root().map_err(|e| anyhow::anyhow!("{e}"))?;
        for path in walk_dir(&stdlib)?.into_iter().filter(|p| p.file_name().is_some_and(|n| n == "deps.toml")) {
            let rel = path.strip_prefix(&stdlib).map_err(|_| anyhow::anyhow!("{} is outside the stdlib", path.display()))?;
            hash_path(hasher, &format!("catalog/{}", rel.display()), &path, &DISK)?;
        }
        Ok(())
    }

    /// Hash everything that compiles into the worker binary: the worker's
    /// image ref names it by this, and the dispatcher selects the worker a
    /// run starts on by it.
    ///
    /// Scoped to exactly the codegen-emitted-static surface: the SET of
    /// referenced node TYPES (the static dispatch table in `registry.rs`
    /// references each by name), each referenced node's package root
    /// (mod.rs, deps.toml, shared files), the weft workspace (the engine
    /// the binary links as path deps) and the toolchain, and for a worker
    /// packaged as an image what the image is built with besides
    /// (weft.toml's `[build.worker]`, the builder base). NOT hashed:
    /// per-node config, edges, ids, positions: those land in the
    /// `ProjectDefinition` the worker fetches at runtime via the broker.
    ///
    /// An unreferenced node can't change the worker binary, so it does
    /// not flip this hash. The same node walked through `weft_catalog`'s
    /// `is_left_out_of_package_build` policy the build stages with, so
    /// what the binary sees and what we hash agree byte-for-byte.
    pub fn compute_binary_hash(
        definition: &ProjectDefinition,
        project: &Project,
        weft_root: &Path,
        catalog: &FsCatalog,
        node_set: weft_core::builds::NodeSet,
        packaging: &weft_core::builds::WorkerPackaging,
        fingerprints: &Fingerprints,
    ) -> Result<SourceHash> {
        let referenced = crate::codegen::node_types_for(definition, catalog, node_set);
        binary_hash_of(&project.manifest.build.worker, &project.root, &referenced, weft_root, catalog, node_set, packaging, fingerprints)
    }

    /// The binary hash of the stock worker: what a project with no nodes of
    /// its own (its `nodes/base_catalog` this weft's standard library,
    /// unmodified), the default `[build.worker]` and `NodeSet::Full`
    /// computes with [`compute_binary_hash`], wherever it sits. Every such
    /// project runs this one worker, built once per weft version and
    /// packaging.
    pub fn compute_stock_binary_hash(weft_root: &Path, packaging: &weft_core::builds::WorkerPackaging) -> Result<SourceHash> {
        let catalog = FsCatalog::discover(&weft_root.join("catalog")).map_err(|e| anyhow::anyhow!("the standard library's catalog: {e}"))?;
        let node_set = weft_core::builds::NodeSet::Full;
        let referenced = catalog.iter().map(|e| e.node_type.clone()).collect();
        // The default `[build.worker]` names no template, so no project
        // file is read: the root is never looked at.
        binary_hash_of(&Default::default(), weft_root, &referenced, weft_root, &catalog, node_set, packaging, &DISK)
    }

    /// [`compute_binary_hash`] over what it reads of a project: its
    /// `[build.worker]` (with the folder a custom template is read from)
    /// and the node types the worker compiles in.
    #[allow(clippy::too_many_arguments)]
    fn binary_hash_of(
        worker: &crate::project::WorkerBuildSection,
        project_root: &Path,
        referenced: &std::collections::BTreeSet<String>,
        weft_root: &Path,
        catalog: &FsCatalog,
        node_set: weft_core::builds::NodeSet,
        packaging: &weft_core::builds::WorkerPackaging,
        fingerprints: &Fingerprints,
    ) -> Result<SourceHash> {
        let mut hasher = Sha256::new();
        // Bump this marker whenever the code that generates the worker
        // crate changes in a way its inputs below do not show: the
        // `main.rs` and `registry.rs` templates (`codegen::write_main_rs`,
        // `codegen::write_registry_rs`) and the package crates `codegen`
        // renders (`render_package_cargo_toml`, `render_package_lib_rs`).
        // A worker is named by this hash, so without the bump one built by
        // the old generator keeps being reused.
        // v5: a worker compiled on the machine leaves out what only an
        // image is built with (v4: every package is named by its package
        // name, never its path).
        hasher.update(b"weft-binary-v5\n");
        if in_image(packaging) {
            hash_image_recipe(&mut hasher, worker, project_root, fingerprints)?;
        }
        // SET of referenced node TYPES: the dispatch table in registry.rs
        // is generated from this. The ORDER doesn't matter (we sort), and
        // the per-node config values live in main.weft (hashed by the
        // definition hash, not here).
        // The choice itself is hashed too, so the tag says what the
        // image was built AS: `weft status` can tell a full build from
        // a referenced one instead of guessing from the type list.
        hasher.update(b"node_set:");
        hasher.update(node_set.hash_marker().as_bytes());
        hasher.update(b"\n");
        hasher.update(b"node_types:\n");
        for nt in referenced {
            hasher.update(b"  ");
            hasher.update(nt.as_bytes());
            hasher.update(b"\n");
        }

        // Each referenced node's package: mod.rs + deps.toml + shared
        // files. A node implementation edit (re)compiles the binary, so
        // it flips this hash.
        hash_packages(&mut hasher, &packages_of(catalog, referenced.iter()), fingerprints)?;

        // The baked type registry: `write_main_rs` bakes EVERY
        // package's nominal type declarations into the binary (not
        // just the referenced packages'), so a type edit anywhere in
        // the catalog changes the compiled bytes and must flip this
        // digest, or a cached image would silently serve the previous
        // registry.
        hash_type_registry(&mut hasher, catalog);

        // Worker build environment: workspace source the binary links
        // against, the toolchain, and for an image the builder base it
        // compiles inside.
        hash_worker_build_env(&mut hasher, weft_root, in_image(packaging))?;

        Ok(hex(&hasher.finalize()))
    }

    /// Production implementation identity per node type. Package helpers and
    /// dependencies are shared by its members; unrelated packages stay independent.
    pub fn implementation_hashes(
        definition: &ProjectDefinition,
        project: &Project,
        weft_root: &Path,
        catalog: &FsCatalog,
        node_set: weft_core::builds::NodeSet,
        packaging: &weft_core::builds::WorkerPackaging,
        fingerprints: &Fingerprints,
    ) -> Result<std::collections::BTreeMap<String, SourceHash>> {
        let mut shared = Sha256::new();
        // v3: what the binary hash leaves out for a host build, this does too.
        shared.update(b"weft-implementation-v3\n");
        hash_worker_build_env(&mut shared, weft_root, in_image(packaging))?;
        if in_image(packaging) {
            hash_image_recipe(&mut shared, &project.manifest.build.worker, &project.root, fingerprints)?;
        }
        hash_type_registry(&mut shared, catalog);
        // Fingerprints describe the compiled worker, whose type set can be
        // wider than this graph. The types weft gives its own nodes (a
        // loop's two boundaries, the placeholder a broken include leaves)
        // always ship in the engine, and a program identity has to answer
        // for every node type its graph holds.
        let mut types = crate::codegen::node_types_for(definition, catalog, node_set);
        types.extend(weft_core::project::own_types::ALL.iter().map(|t| (*t).to_string()));
        let mut packages: std::collections::BTreeMap<Packages, SourceHash> = std::collections::BTreeMap::new();
        let mut result = std::collections::BTreeMap::new();
        for node_type in types {
            let package = packages_of(catalog, std::iter::once(&node_type));
            let hash = if let Some(hash) = packages.get(&package) {
                hash.clone()
            } else {
                let mut hasher = shared.clone();
                hash_packages(&mut hasher, &package, fingerprints)?;
                let hash = hex(&hasher.finalize());
                packages.insert(package, hash.clone());
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

    /// Hash the image RECIPE a staged image build renders through
    /// `worker_image::emit`: `weft.toml`'s build choices (base image,
    /// custom template path) and, when a custom template is set, the
    /// template file's own content, so a base-image or template edit
    /// flips the digests a worker image is named by instead of silently
    /// serving a stale cached image. Read only for a worker packaged as an
    /// image: a worker compiled on the machine reads none of it.
    fn hash_image_recipe(
        hasher: &mut Sha256,
        worker: &crate::project::WorkerBuildSection,
        project_root: &Path,
        fingerprints: &Fingerprints,
    ) -> Result<()> {
        // Names, IDs and dispatcher addresses do not change executable code.
        hasher.update(b"worker_build:\n");
        hasher.update(serde_json::to_vec(worker)?);

        // A custom Dockerfile template's CONTENT shapes the image too;
        // weft.toml only carries its path, so an edit to the template
        // file itself would otherwise never trigger a rebuild. A
        // set-but-MISSING template path is a config error: fail loudly
        // here rather than hashing the absence (which would let the
        // build proceed to a docker failure later with a less obvious
        // cause).
        if let Some(rel) = &worker.dockerfile_template {
            let template_path = project_root.join(rel);
            if !template_path.exists() {
                anyhow::bail!(
                    "weft.toml sets [build.worker] dockerfile_template = {:?} but that file does \
                     not exist (resolved to {}); fix the path or remove the setting",
                    rel,
                    template_path.display()
                );
            }
            hash_path(hasher, "dockerfile_template", &template_path, fingerprints)?;
        }
        Ok(())
    }

    /// Packages by name, each with its root: what a digest folds a set of
    /// node trees by. The name is the package's identity in its catalog
    /// (`weft_catalog` refuses two folders with one name), so a package
    /// reads the same from any folder: a project's `nodes/base_catalog`
    /// copy names the same packages as the weft checkout's `catalog/`.
    pub type Packages = std::collections::BTreeMap<String, PathBuf>;

    /// The packages of `node_types`, by name. A type the catalog does
    /// not serve has no package (the caller validates references).
    pub fn packages_of<'a>(catalog: &FsCatalog, node_types: impl Iterator<Item = &'a String>) -> Packages {
        node_types
            .filter_map(|node_type| catalog.package_of(node_type))
            .map(|package| (package.name.clone(), package.root.clone()))
            .collect()
    }

    /// The package whose root is `root`, alone, or an error naming it.
    fn package_at(catalog: &FsCatalog, root: &Path) -> Result<Packages> {
        let package = catalog
            .packages()
            .find(|p| p.root == root)
            .ok_or_else(|| anyhow::anyhow!("no package of this catalog is at {}", root.display()))?;
        Ok([(package.name.clone(), package.root.clone())].into_iter().collect())
    }

    /// Hash a set of packages, each by its name and its content (sorted
    /// by name), never by where it sits: the same package in two
    /// projects, or in a project and the weft checkout, hashes the same,
    /// while a node moving between packages changes the digest. Shared by
    /// every digest that folds "the node trees that matter", over the
    /// same node-tree walk policy (`walk_dir`).
    fn hash_packages(hasher: &mut Sha256, packages: &Packages, fingerprints: &Fingerprints) -> Result<()> {
        for (name, root) in packages {
            hasher.update(b"package:");
            hasher.update(name.as_bytes());
            hasher.update(b"\n");
            hash_package_root(hasher, name, root, fingerprints)?;
        }
        Ok(())
    }

    /// Hash one package root as the build stages it
    /// (`build::stage_project_nodes`): every file but what
    /// `weft_catalog::is_left_out_of_package_build` names, so a `.weft`
    /// file a program includes from the package flips no package hash.
    pub fn hash_package_root(hasher: &mut Sha256, label: &str, root: &Path, fingerprints: &Fingerprints) -> Result<()> {
        hash_path_skipping(hasher, label, root, &|name, _| weft_catalog::is_left_out_of_package_build(name), fingerprints)
    }

    /// Content hash of everything that can change a package's node-test
    /// OUTCOME: the package's own sources plus the catalog's nominal
    /// type registry (the test binary bakes EVERY package's type
    /// declarations and resolves this package's port types through
    /// them, so a sibling's type edit changes this package's results).
    /// Excludes the worker build environment on purpose: it rebuilds the
    /// test server without changing what a test does, and live tests spend
    /// real provider money.
    pub fn compute_node_test_outcome_hash(package_root: &Path, catalog: &FsCatalog) -> Result<SourceHash> {
        let mut hasher = Sha256::new();
        hasher.update(b"weft-node-test-outcome-v2\n");
        hash_packages(&mut hasher, &package_at(catalog, package_root)?, &DISK)?;
        hash_type_registry(&mut hasher, catalog);
        Ok(hex(&hasher.finalize()))
    }

    /// Content hash of ONE package's node-test server, the program a live
    /// test runs on the machine: the package's own sources (which include
    /// every member's `tests.rs`; the package walk hashes all files), the
    /// catalog's type registry, and the build environment of a program
    /// compiled on the machine (the workspace it links and the toolchain).
    /// This is the staleness rule for a package's test server: change a
    /// node, a test, or the engine and the hash flips; change nothing and
    /// a compiled server is current.
    pub fn compute_node_test_hash(package_root: &Path, weft_root: &Path, catalog: &FsCatalog) -> Result<SourceHash> {
        let mut hasher = Sha256::new();
        // v4: a test server is a program compiled on the machine, so no
        // image recipe or builder base (v3: its package is named by its
        // name, never its path).
        hasher.update(b"weft-node-test-v4\n");
        hash_packages(&mut hasher, &package_at(catalog, package_root)?, &DISK)?;
        // The test crate's main.rs bakes the FULL catalog type
        // registry (see the binary hash's identical step): a type
        // edit in a SIBLING package changes this package's test
        // binary, so it must flip this digest too.
        hash_type_registry(&mut hasher, catalog);
        hash_worker_build_env(&mut hasher, weft_root, false)?;
        Ok(hex(&hasher.finalize()))
    }

    /// Whether `packaging` builds an image, which is all a digest folds of
    /// it (a host build's shared folder never reaches the binary).
    fn in_image(packaging: &weft_core::builds::WorkerPackaging) -> bool {
        matches!(packaging, weft_core::builds::WorkerPackaging::Image { .. })
    }

    /// Hash everything that affects the running infrastructure.
    /// Scoped to the infra closure: every node where `requiresInfra` is
    /// true plus every node upstream of one (the chain that produces the
    /// inputs the infra node consumes during InfraSetup).
    pub fn compute_infra_hash(
        project: &ProjectDefinition,
        project_root: &Path,
        weft_root: &Path,
        catalog: &FsCatalog,
        fingerprints: &Fingerprints,
    ) -> Result<SourceHash> {
        let mut hasher = Sha256::new();
        hasher.update(b"weft-infra-v3\n");

        // Graph definition, scoped to the infra closure: which nodes are
        // infra, what config they carry, and the wires among them. An edit
        // can flip a node into / out of the closure or rewire an upstream,
        // and the closure slice sees all of that. NOT the raw main.weft:
        // hashing the whole file lit the Upgrade button for every edit to
        // the rest of the graph (an LLM prompt tweak has no bearing on the
        // running bridge process). Same canonical form as the definition hash:
        // spans / positions / file-ref paths stripped, nodes and edges
        // sorted, so a comment or a canvas drag cannot flip it either.
        let closure = upstream_closure(&ProjectGraph::new(project), &infra_ids(project));
        hash_definition_slice(&mut hasher, project, &closure)?;
        hash_path(&mut hasher, "weft.toml", &project_root.join("weft.toml"), fingerprints)?;

        // Hash each package root that owns a closure node (mod.rs /
        // metadata.json / deps.toml of every node in the package, plus
        // shared `.rs` and `package.toml`). Same folding the source hash
        // uses; multiple closure nodes from one package collapse to one
        // root via the helper's dedup.
        let closure_types: Vec<&String> = closure
            .iter()
            .filter_map(|id| project.nodes.iter().find(|n| n.id == *id))
            .map(|n| &n.node_type)
            .collect();
        hash_packages(&mut hasher, &packages_of(catalog, closure_types.into_iter()), fingerprints)?;

        // Worker build environment, the whole of it whatever packages the
        // worker (so a project's infra hash reads the same on every
        // platform): engine + core compile into the worker binary that
        // runs InfraSetup.
        hash_worker_build_env(&mut hasher, weft_root, true)?;

        Ok(hex(&hasher.finalize()))
    }

    /// Each infra place's own infra hash, by place (`db`, `one.db`): what
    /// [`compute_infra_hash`] hashes, scoped to the place's own closure
    /// (its node and everything upstream of it, their packages) and the
    /// place's spelling. A copy applied from one build carries its place's
    /// (`BuiltProgram::infra_places`); one whose place hashes otherwise in
    /// the source now is behind it, and an edit to one infra node leaves
    /// every other place where it was.
    pub fn compute_infra_place_hashes(
        project: &ProjectDefinition,
        project_root: &Path,
        weft_root: &Path,
        catalog: &FsCatalog,
        fingerprints: &Fingerprints,
    ) -> Result<std::collections::BTreeMap<String, SourceHash>> {
        let graph = ProjectGraph::new(project);
        let mut out = std::collections::BTreeMap::new();
        for place in weft_core::project::infra_places(project) {
            let spelled = weft_core::project::address_of(project, &place.id, &place.path);
            let mut hasher = Sha256::new();
            hasher.update(b"weft-infra-place-v2\n");
            hasher.update(spelled.as_bytes());
            hasher.update(b"\n");
            let closure = upstream_closure(&graph, std::slice::from_ref(&place.id));
            hash_definition_slice(&mut hasher, project, &closure)?;
            hash_path(&mut hasher, "weft.toml", &project_root.join("weft.toml"), fingerprints)?;
            let types: Vec<&String> = closure
                .iter()
                .filter_map(|id| project.nodes.iter().find(|n| n.id == *id))
                .map(|n| &n.node_type)
                .collect();
            hash_packages(&mut hasher, &packages_of(catalog, types.into_iter()), fingerprints)?;
            hash_worker_build_env(&mut hasher, weft_root, true)?;
            out.insert(spelled, hex(&hasher.finalize()));
        }
        Ok(out)
    }

    /// Hash a single image's source: Dockerfile + every file in the
    /// image source dir, scoped by node type. Used verbatim as the
    /// infra docker image tag suffix.
    pub fn compute_image_hash(node_type: &str, image_source_dir: &Path, fingerprints: &Fingerprints) -> Result<SourceHash> {
        let mut hasher = Sha256::new();
        hasher.update(b"weft-image-v1\n");
        hasher.update(node_type.as_bytes());
        hasher.update(b"\n");
        hash_path(&mut hasher, "image-source", image_source_dir, fingerprints)?;
        Ok(hex(&hasher.finalize()))
    }

    /// Hash the inputs that shape the pre-built worker builder base image:
    /// the engine workspace + rust-toolchain pin + the base Dockerfile
    /// itself. An engine bump or toolchain change flips this hash and
    /// triggers a fresh base image; per-project worker images then FROM
    /// the new tag. Scoped to engine-affecting inputs only, NOT project
    /// inputs (those don't change the base): `hash_worker_build_env`,
    /// shared with the binary / infra hashes so a base-affecting edit
    /// flips all three together, plus the installation's stdlib
    /// `deps.toml` files the stock worker it bakes compiles against.
    pub fn compute_builder_base_hash(weft_root: &Path) -> Result<SourceHash> {
        let mut hasher = Sha256::new();
        hasher.update(b"weft-builder-base-v1\n");
        hash_worker_build_env(&mut hasher, weft_root, true)?;
        hash_installed_stdlib_deps(&mut hasher)?;
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
    /// fingerprint. And `build_env`, what tells apart the caches of
    /// projects whose nodes set `[build.env]`
    /// (`worker_image::NodesBuildEnv::cache_tag`): a project that sets
    /// none (`None`) keeps the key, and the cache, every such project
    /// shares.
    pub fn compute_worker_cache_key(weft_root: &Path, builder: &str, build_env: Option<&str>) -> Result<String> {
        let mut hasher = Sha256::new();
        hasher.update(b"weft-worker-cache-v1\n");
        hasher.update(b"builder:");
        hasher.update(builder.as_bytes());
        hasher.update(b"\n");
        hash_worker_build_env(&mut hasher, weft_root, true)?;
        hash_installed_stdlib_deps(&mut hasher)?;
        if let Some(tag) = build_env {
            hasher.update(b"build-env:");
            hasher.update(tag.as_bytes());
            hasher.update(b"\n");
        }
        Ok(hex(&hasher.finalize()).chars().take(16).collect())
    }

    /// Recursive directory walk that returns every regular file under
    /// `root`, skipping the shared node-tree exclude set
    /// (`weft_catalog::is_node_tree_excluded`). Symlinked directories are
    /// followed (a cycle fails loudly via the descent chain), so a
    /// linked shared catalog is hashed at every path it appears at,
    /// matching what the stage copy materializes.
    /// This is the hash side of the one node-tree walk policy: it must
    /// see exactly the bytes the build stages, or a missed/extra file
    /// silently de/over-syncs the worker-image hash. The paths come back
    /// sorted, so a hash over them is the same on every machine.
    pub fn walk_dir(root: &Path) -> Result<Vec<PathBuf>> {
        walk_dir_skipping(root, &|_, _| false)
    }

    /// Which entries a skipping walk leaves out: asked with an entry's
    /// name and whether it sits directly under the walk's root, for
    /// every file and directory below it.
    pub type LeftOut<'a> = &'a dyn Fn(&str, bool) -> bool;

    /// [`walk_dir`], with every entry `left_out` names left out (a
    /// directory whole). For an input whose consumer reads only part of
    /// a directory (the system images compile a crate's binaries, so its
    /// `tests/` has no bearing on what comes out).
    pub fn walk_dir_skipping(root: &Path, left_out: LeftOut<'_>) -> Result<Vec<PathBuf>> {
        let mut out = Vec::new();
        let mut chain = Vec::new();
        walk_into(root, left_out, &mut out, &mut chain)?;
        // `read_dir` order is the filesystem's, and it differs between
        // machines: two release runners walked the stdlib in different
        // orders and minted two builder-base hashes for one commit.
        // Every caller feeds these paths to a hasher, so the walk owns
        // the ordering.
        out.sort();
        Ok(out)
    }

    // Symlinks are followed (a symlinked `nodes/base_catalog` has to
    // hash as the bytes it resolves to, the same view the staging copy
    // and the pack take); the descent chain turns a symlink cycle into
    // a loud error instead of an infinite walk.
    fn walk_into(
        dir: &Path,
        left_out: LeftOut<'_>,
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
                if is_node_tree_excluded(&name) || left_out(&name, at_top) {
                    continue;
                }
                let path = entry.path();
                match weft_catalog::node_tree_entry_kind(&path)
                    .with_context(|| format!("stat {}", path.display()))?
                {
                    weft_catalog::NodeTreeEntryKind::Dir => {
                        walk_into(&path, left_out, out, chain)?
                    }
                    weft_catalog::NodeTreeEntryKind::File => out.push(path),
                }
            }
            Ok(())
        })();
        chain.pop();
        result
    }

    /// [`hash_path`], with every entry `left_out` names left out.
    ///
    /// For an input whose consumer reads only part of a directory. The system
    /// images compile a crate's binaries, so its `tests/` has no bearing on
    /// what comes out; hashing it anyway would rebuild four images every time
    /// somebody edits a test.
    pub fn hash_path_skipping(
        hasher: &mut Sha256,
        label: &str,
        path: &Path,
        left_out: LeftOut<'_>,
        fingerprints: &Fingerprints,
    ) -> Result<()> {
        if !path.exists() {
            // Hash the absence so a future appearance invalidates.
            hasher.update(b"missing:");
            hasher.update(label.as_bytes());
            hasher.update(b"\n");
            return Ok(());
        }
        if path.is_file() {
            hash_file(hasher, label, path, fingerprints)
        } else if path.is_dir() {
            hash_dir(hasher, label, path, left_out, fingerprints)
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
    pub fn hash_path(hasher: &mut Sha256, label: &str, path: &Path, fingerprints: &Fingerprints) -> Result<()> {
        hash_path_skipping(hasher, label, path, &|_, _| false, fingerprints)
    }

    fn hash_file(hasher: &mut Sha256, label: &str, path: &Path, fingerprints: &Fingerprints) -> Result<()> {
        hasher.update(b"file:");
        hasher.update(label.as_bytes());
        hasher.update(b"\n");
        hasher.update(fingerprints.of(path)?.as_bytes());
        hasher.update(b"\n");
        Ok(())
    }

    fn hash_dir(hasher: &mut Sha256, label: &str, dir: &Path, left_out: LeftOut<'_>, fingerprints: &Fingerprints) -> Result<()> {
        hasher.update(b"dir:");
        hasher.update(label.as_bytes());
        hasher.update(b"\n");
        for entry in walk_dir_skipping(dir, left_out)? {
            let rel = entry
                .strip_prefix(dir)
                .unwrap_or(&entry)
                .to_string_lossy()
                .into_owned();
            hasher.update(b"path:");
            hasher.update(rel.as_bytes());
            hasher.update(b"\n");
            // `walk_dir` yields files only (dirs recursed, symlinks resolved).
            hasher.update(fingerprints.of(&entry)?.as_bytes());
            hasher.update(b"\n");
        }
        Ok(())
    }

    /// The project's program as every command compiles it
    /// ([`crate::compile_program`]: what does not compile is left in as
    /// broken parts) AND the catalog it was compiled against, read from
    /// disk, without running cargo / docker. Returns both because every
    /// caller (drift hashes, infra build) needs the same catalog the
    /// definition was built from; returning it here is one discovery per
    /// command instead of each caller re-walking the node roots.
    pub fn load_program(project: &Project) -> Result<(ProjectDefinition, FsCatalog)> {
        load_program_with_diagnostics(project).map_err(|e| anyhow::anyhow!("{e}"))
    }

    /// Same as `load_program` but on a failure of the program as a whole
    /// the Err carries the structured `Vec<Diagnostic>` so callers that
    /// surface errors to the editor (the CLI's run path emitting
    /// structured progress events) can render them one-per-line rather
    /// than as a single flattened string.
    pub fn load_program_with_diagnostics(
        project: &Project,
    ) -> std::result::Result<(ProjectDefinition, FsCatalog), CompileLoadError> {
        load_program_in(project, std::sync::Arc::new(weft_catalog::DiskTree))
    }

    /// [`load_program_with_diagnostics`] with every file read from `tree`:
    /// the entry file, the files it includes and reads, and both node
    /// roots. A command reads its project once into a snapshot and
    /// compiles from that, so nothing written to disk meanwhile reaches
    /// it.
    pub fn load_program_in(
        project: &Project,
        tree: std::sync::Arc<dyn weft_catalog::SourceTree>,
    ) -> std::result::Result<(ProjectDefinition, FsCatalog), CompileLoadError> {
        let main_weft = project.main_weft();
        let source = tree
            .read_to_string(&main_weft)
            .map_err(|e| CompileLoadError::Read(format!("read {}: {e}", main_weft.display())))?;
        let roots = crate::project::node_roots(&project.root);
        let catalog = FsCatalog::discover_roots_in(tree.clone(), &roots.iter().map(|r| r.as_path()).collect::<Vec<_>>())
            .map_err(|e| CompileLoadError::Read(format!("catalog: {e}")))?;
        let src_dir = project.src_dir();
        let reader = crate::file_reader::TreeFileReader::new(tree.as_ref());
        let fs = crate::CompileFs::with_reader(&reader, Some(&project.root)).anchored_at(Some(&src_dir));
        let mut definition =
            crate::compile_program(&source, project.id(), fs, &catalog).map_err(CompileLoadError::Diagnostics)?;
        // What weft.toml says for every run and trigger rides in the
        // program, so a version reads the defaults it was built with.
        definition.defaults = project.manifest.defaults();
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

        /// An image is named after its files' fingerprints: the same files
        /// name it the same whether read off the disk or taken from the
        /// version that holds them, and a version build keeps the
        /// version's name whatever the disk holds.
        #[test]
        fn an_image_is_named_after_its_files_fingerprints() {
            let dir = tempfile::tempdir().unwrap();
            let image = dir.path().join("nodes/media/images/studio");
            std::fs::create_dir_all(&image).unwrap();
            std::fs::write(image.join("Dockerfile"), "FROM scratch\n").unwrap();
            std::fs::write(image.join("font.ttf"), b"the version's bytes").unwrap();
            let manifest: weft_core::project::hash::Manifest = [
                ("nodes/media/images/studio/Dockerfile", DISK.of(&image.join("Dockerfile")).unwrap()),
                ("nodes/media/images/studio/font.ttf", DISK.of(&image.join("font.ttf")).unwrap()),
            ]
            .into_iter()
            .map(|(path, hash)| (path.to_string(), hash))
            .collect();
            let version = Fingerprints::of_version(dir.path().to_path_buf(), manifest);
            let from_disk = compute_image_hash("Media", &image, &DISK).unwrap();
            assert_eq!(compute_image_hash("Media", &image, &version).unwrap(), from_disk, "one name for the same files");
            std::fs::write(image.join("font.ttf"), b"bytes gone bad").unwrap();
            assert_eq!(compute_image_hash("Media", &image, &version).unwrap(), from_disk, "the version names its own image");
            assert_ne!(compute_image_hash("Media", &image, &DISK).unwrap(), from_disk, "the disk is read when nothing names the file");
        }

        #[test]
        fn implementation_identity_matches_the_compiled_type_set() {
            let dir = tempfile::tempdir().unwrap();
            std::fs::write(dir.path().join("weft.toml"), "[package]\nname = 'test'\nid = '00000000-0000-0000-0000-000000000001'\n").unwrap();
            let project = Project::load(dir.path()).unwrap();
            let catalog = FsCatalog::discover(&weft_catalog::stdlib_root().unwrap()).unwrap();
            let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap().parent().unwrap();
            let empty: ProjectDefinition = serde_json::from_value(serde_json::json!({"id":project.id(),"nodes":[],"edges":[]})).unwrap();
            let mut looped = empty.clone();
            looped.nodes.push(serde_json::from_value(serde_json::json!({
                "id":"l__in", "nodeType":"LoopIn", "inputs":[], "outputs":[], "position":{"x":0,"y":0},
                "groupBoundary":{"groupId":"l","role":"In"}
            })).unwrap());
            for set in [weft_core::builds::NodeSet::Full, weft_core::builds::NodeSet::Referenced] {
                let before = implementation_hashes(&empty, &project, root, &catalog, set, &host(), &DISK).unwrap();
                let after = implementation_hashes(&looped, &project, root, &catalog, set, &host(), &DISK).unwrap();
                assert_eq!(before, after, "adding a built-in boundary does not change the worker's implementation map");
                // Every type weft gives its own nodes, the placeholder a
                // broken include leaves among them: a program holding one
                // must still answer for its implementation when a seeded
                // run slices it (`ProgramIdentity::slice_hashes`).
                for own in weft_core::project::own_types::ALL {
                    assert!(before.contains_key(*own), "{own}");
                }
                if matches!(set, weft_core::builds::NodeSet::Full) { assert!(before.contains_key("Text")); }
            }
        }

        /// A program compiled on the machine (its shared folder is never
        /// read by a digest).
        fn host() -> weft_core::builds::WorkerPackaging {
            weft_core::builds::WorkerPackaging::Host { shared_crates: PathBuf::from("/shared") }
        }

        fn image() -> weft_core::builds::WorkerPackaging {
            weft_core::builds::WorkerPackaging::Image { builder: "weft-builder-base:b".into() }
        }

        /// The workspace slice's key is stable, short, and the same whatever
        /// the packaging around it; a host build and an image of one program
        /// are named apart, and neither moves with the builder base's ref.
        #[test]
        fn the_workspace_key_is_stable_and_programs_and_images_are_named_apart() {
            let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap().parent().unwrap();
            let key = compute_worker_workspace_key(root).unwrap();
            assert_eq!(key.len(), 16);
            assert_eq!(compute_worker_workspace_key(root).unwrap(), key);
            let host_stock = compute_stock_binary_hash(root, &host()).unwrap();
            let other_host = weft_core::builds::WorkerPackaging::Host { shared_crates: PathBuf::from("/elsewhere") };
            assert_eq!(compute_stock_binary_hash(root, &other_host).unwrap(), host_stock, "the shared folder is no input");
            let image_stock = compute_stock_binary_hash(root, &image()).unwrap();
            assert_ne!(host_stock, image_stock);
            let other_base = weft_core::builds::WorkerPackaging::Image { builder: "elsewhere/weft-builder-base:c".into() };
            assert_eq!(compute_stock_binary_hash(root, &other_base).unwrap(), image_stock, "the base's sources name it, not its ref");
        }

        /// The database's migration history is no part of a worker: a new
        /// file under a crate's `left_out_of_workers` folder moves neither the
        /// worker's name nor what its build is given, while an edit to a
        /// source file of the same crate moves the name. A listed folder the
        /// crate does not hold is refused.
        #[test]
        fn a_migration_names_no_new_worker_and_a_source_edit_does() {
            let temp = tempfile::tempdir().unwrap();
            let root = temp.path();
            std::fs::write(
                root.join("Cargo.toml"),
                "[workspace]\nmembers = []\n\n[workspace.dependencies]\nweft-task-store = { path = \"crates/weft-task-store\" }\n",
            )
            .unwrap();
            std::fs::write(root.join("Cargo.lock"), "version = 4\n").unwrap();
            for name in ["weft-engine", "weft-core", "weft-broker-client", "weft-task-store"] {
                let dir = root.join("crates").join(name);
                std::fs::create_dir_all(dir.join("src")).unwrap();
                let deps = if name == "weft-engine" { "\n[dependencies]\nweft-task-store = { workspace = true }\n" } else { "" };
                let metadata = if name == "weft-task-store" {
                    "\n[package.metadata.weft]\nleft_out_of_workers = [\"migrations\"]\n"
                } else {
                    ""
                };
                std::fs::write(dir.join("Cargo.toml"), format!("[package]\nname = \"{name}\"\nversion = \"0.1.0\"\n{deps}{metadata}")).unwrap();
                std::fs::write(dir.join("src/lib.rs"), "pub fn f() {}\n").unwrap();
            }
            let store = root.join("crates/weft-task-store");
            std::fs::create_dir_all(store.join("migrations/task")).unwrap();
            std::fs::write(store.join("migrations/task/origin.sql"), "CREATE TABLE task ();\n").unwrap();

            let staged = |at: &str| {
                let dest = temp.path().join(at);
                crate::build::stage_worker_workspace(root, &dest).unwrap();
                dest.join("crates/weft-task-store")
            };
            let before = compute_worker_workspace_key(root).unwrap();
            assert!(!staged("staged-before").join("migrations").exists(), "the history is not given to a worker build");

            std::fs::write(store.join("migrations/task/20990101T000000_task_more.sql"), "ALTER TABLE task ADD x INT;\n").unwrap();
            assert_eq!(compute_worker_workspace_key(root).unwrap(), before, "a migration names no new worker");

            std::fs::write(store.join("src/lib.rs"), "pub fn f() { let _ = 1; }\n").unwrap();
            assert_ne!(compute_worker_workspace_key(root).unwrap(), before, "a source edit does");
            assert!(staged("staged-after").join("src/lib.rs").is_file());

            std::fs::remove_dir_all(store.join("migrations")).unwrap();
            let refused = compute_worker_workspace_key(root).unwrap_err().to_string();
            assert!(refused.contains("left_out_of_workers names 'migrations'"), "{refused}");
        }

        #[test]
        fn worker_cache_key_is_stable_per_builder() {
            let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap().parent().unwrap();
            let prebuilt = compute_worker_cache_key(root, "builder-base", None).unwrap();
            assert_eq!(prebuilt.len(), 16);
            assert_eq!(compute_worker_cache_key(root, "builder-base", None).unwrap(), prebuilt);
            assert_ne!(
                compute_worker_cache_key(root, "alpine:3.19", None).unwrap(),
                prebuilt,
                "a from-scratch builder on another distro gets its own cache"
            );
        }

        /// A project whose nodes set no `[build.env]` keeps the key every
        /// such project had before the variables counted, so it shares the
        /// cache it always shared.
        #[test]
        fn worker_cache_key_without_build_env_is_the_shared_one() {
            let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap().parent().unwrap();
            let mut hasher = Sha256::new();
            hasher.update(b"weft-worker-cache-v1\n");
            hasher.update(b"builder:");
            hasher.update(b"builder-base");
            hasher.update(b"\n");
            hash_worker_build_env(&mut hasher, root, true).unwrap();
            hash_installed_stdlib_deps(&mut hasher).unwrap();
            let shared: String = hex(&hasher.finalize()).chars().take(16).collect();
            assert_eq!(compute_worker_cache_key(root, "builder-base", None).unwrap(), shared);
        }

        /// Each `[build.env]` tag gets a cache of its own.
        #[test]
        fn worker_cache_key_differs_per_build_env() {
            let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap().parent().unwrap();
            let key = |tag| compute_worker_cache_key(root, "builder-base", tag).unwrap();
            let (none, one, other) = (key(None), key(Some("0123456789abcdef")), key(Some("fedcba9876543210")));
            assert_eq!(key(Some("0123456789abcdef")), one);
            for (a, b) in [(&none, &one), (&none, &other), (&one, &other)] {
                assert_ne!(a, b);
            }
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
            let hash = |graph: &ProjectDefinition| compute_binary_hash(graph, &project, root, &catalog, weft_core::builds::NodeSet::Full, &host(), &DISK).unwrap();
            let initial = hash(&graph);
            assert_eq!(hash(&graph), initial);
            graph.nodes[0].config = serde_json::json!({"value":"after"});
            assert_eq!(hash(&graph), initial, "config belongs to the run definition");
            graph.nodes[0].node_type = "Format".into();
            assert_eq!(hash(&graph), initial, "all catalog implementations were already compiled");
        }

        /// The project's own copy of the standard library is what the
        /// binary hash reads: editing a `deps.toml` under its
        /// `nodes/base_catalog/` flips it, whatever the installation holds.
        #[test]
        fn the_projects_own_stdlib_deps_flip_the_binary_hash() {
            let dir = tempfile::tempdir().unwrap();
            std::fs::write(dir.path().join("weft.toml"), "[package]\nname = 'test'\nid = '00000000-0000-0000-0000-000000000001'\n").unwrap();
            crate::project::seed_base_catalog(dir.path()).unwrap();
            let project = Project::load(dir.path()).unwrap();
            let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap().parent().unwrap();
            let graph: ProjectDefinition = serde_json::from_value(serde_json::json!({
                "id":project.id(), "nodes":[{"id":"req", "nodeType":"HttpRequest", "config":{}, "position":{"x":0,"y":0}}], "edges":[]
            })).unwrap();
            let hash = || {
                let catalog = crate::build::build_project_catalog(dir.path()).unwrap();
                compute_binary_hash(&graph, &project, root, &catalog, weft_core::builds::NodeSet::Full, &host(), &DISK).unwrap()
            };
            let before = hash();
            let deps = dir.path().join("nodes/base_catalog/http/request/deps.toml");
            let mut text = std::fs::read_to_string(&deps).unwrap();
            text.push_str("\n# edited in the project\n");
            std::fs::write(&deps, text).unwrap();
            assert_ne!(hash(), before);
        }

        /// The digest of `roots`, each package named by its folder.
        fn digest(roots: &[PathBuf]) -> String {
            let packages: Packages = roots.iter().map(|root| (root.file_name().unwrap().to_string_lossy().into_owned(), root.clone())).collect();
            let mut hash = Sha256::new();
            hash_packages(&mut hash, &packages, &DISK).unwrap();
            hex(&hash.finalize())
        }

        /// A package is named by its name and content: the same packages in
        /// other folders, listed in any order, hash the same.
        #[test]
        fn a_package_hashes_the_same_wherever_it_sits() {
            let temp = tempfile::tempdir().unwrap();
            let mut hashes = Vec::new();
            for (local_at, shared_at) in [("a-project/nodes/local", "z-weft/catalog/shared"), ("z-project/src/deep/local", "a-weft/nodes/base_catalog/shared")] {
                let roots = [temp.path().join(local_at), temp.path().join(shared_at)];
                for (root, text) in roots.iter().zip(["local body", "shared helper"]) {
                    std::fs::create_dir_all(root).unwrap();
                    std::fs::write(root.join("mod.rs"), text).unwrap();
                }
                let hash = digest(&roots);
                assert_eq!(hash, digest(&[roots[1].clone(), roots[0].clone(), roots[0].clone()]));
                hashes.push(hash);
            }
            assert_eq!(hashes[0], hashes[1]);
        }

        /// A project whose nodes are this weft's standard library, copied
        /// in by `weft new`, names the stock worker wherever the project
        /// sits; an edit to one of its standard nodes names another.
        #[test]
        fn a_stdlib_only_project_names_the_stock_worker_wherever_it_sits() {
            let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap().parent().unwrap();
            let stock = compute_stock_binary_hash(root, &host()).unwrap();
            let temp = tempfile::tempdir().unwrap();
            let mut projects = Vec::new();
            for at in ["one", "elsewhere/two"] {
                let dir = temp.path().join(at);
                std::fs::create_dir_all(&dir).unwrap();
                std::fs::write(dir.join("weft.toml"), format!("[package]\nname = '{}'\nid = '{}'\n", at.replace('/', "-"), uuid::Uuid::new_v4())).unwrap();
                crate::project::seed_base_catalog(&dir).unwrap();
                projects.push(dir);
            }
            let hash = |dir: &Path| {
                let project = Project::load(dir).unwrap();
                let catalog = crate::build::build_project_catalog(dir).unwrap();
                let graph: ProjectDefinition = serde_json::from_value(serde_json::json!({
                    "id":project.id(), "nodes":[{"id":"t", "nodeType":"Text", "config":{}, "position":{"x":0,"y":0}}], "edges":[]
                })).unwrap();
                compute_binary_hash(&graph, &project, root, &catalog, weft_core::builds::NodeSet::Full, &host(), &DISK).unwrap()
            };
            assert_eq!(hash(&projects[0]), stock, "a stock project computes the stock hash");
            assert_eq!(hash(&projects[1]), stock, "wherever it sits");
            let text = projects[1].join("nodes/base_catalog/basic/text/mod.rs");
            let mut body = std::fs::read_to_string(&text).unwrap();
            body.push_str("\n// edited in the project\n");
            std::fs::write(&text, body).unwrap();
            assert_ne!(hash(&projects[1]), stock, "an edited standard node is no stock worker");
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
            let hash = |root: &PathBuf| digest(std::slice::from_ref(root));
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

        /// A `.weft` file a program includes from a package is read into
        /// the program, never compiled into the package: adding or editing
        /// one moves no package hash, so no worker rebuilds and no node test
        /// reruns. Any other file still does, a prompt included (node code
        /// may `include_str!` it).
        #[test]
        fn a_weft_file_in_a_package_moves_no_package_hash() {
            let temp = tempfile::tempdir().unwrap();
            let pkg = temp.path().join("nodes/computer");
            std::fs::create_dir_all(pkg.join("click")).unwrap();
            std::fs::write(pkg.join("click/mod.rs"), "node body").unwrap();
            let hash = || digest(std::slice::from_ref(&pkg));
            let before = hash();
            std::fs::write(pkg.join("agent.weft"), "Group {}").unwrap();
            std::fs::write(pkg.join("click/flow.weft"), "Group {}").unwrap();
            assert_eq!(hash(), before);
            std::fs::write(pkg.join("agent.weft"), "Group { edited }").unwrap();
            assert_eq!(hash(), before);
            std::fs::write(pkg.join("agent_prompt.md"), "Act carefully.").unwrap();
            assert_ne!(hash(), before);
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

    /// Regression: the two release runners walked the stdlib in their
    /// filesystems' own orders and hashed two different builder bases
    /// for one commit. The walk owns the order, so every hash fed from
    /// it is the same on every machine.
    #[test]
    fn walk_dir_returns_paths_in_one_order_whatever_the_filesystem_gives() {
        let dir = tempfile::tempdir().unwrap();
        // Created out of order, and in a subdirectory too.
        for rel in ["z/deps.toml", "m.rs", "a/deps.toml", "z/a.rs"] {
            let path = dir.path().join(rel);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, b"x").unwrap();
        }
        let walked = walk_dir(dir.path()).unwrap();
        let mut sorted = walked.clone();
        sorted.sort();
        assert_eq!(walked, sorted);
        assert_eq!(walked.len(), 4);
    }

    /// Regression: `created_at` / `updated_at` are stamped at
    /// compile time with `Utc::now()`. If `compute_definition_hash`
    /// hashed them verbatim, every CLI invocation would produce a
    /// different hash for the same source, the resync drift signal
    /// would always light, and the worker's per-hash definition
    /// cache would miss on every run. The hash must depend
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
