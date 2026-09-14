//! Compile pipeline orchestration.
//!
//! Given a project root (containing `weft.toml` + `src/main.weft`),
//! parse + enrich + validate + codegen the generated cargo crate
//! to `.weft/target/build/`, then emit the multi-stage
//! Dockerfile + stage the docker build context. The actual
//! `cargo build` runs INSIDE that docker build, not on the host.
//! The host only needs docker.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::codegen;
use crate::error::{CompileError, CompileResult};
use crate::project::Project;
use crate::validate::ValidationMode;
use crate::worker_image;
use weft_catalog::FsCatalog;

/// One staged docker image build, whichever binary it compiles (the
/// project's worker or a package's node-test runner). The host never
/// holds a compiled binary (cargo runs inside the docker build); what
/// a build hands back is the staged context plus the content hash
/// that NAMES the image, so the caller can build it or recognize it
/// as already present.
pub struct StagedImageBuild {
    /// Absolute path to the docker build context. Contains
    /// `Dockerfile`, `build/` (the generated cargo crate), and, when
    /// the pre-built builder base is not in play, `weft/` (the
    /// worker-linked slice of the weft workspace, see
    /// `stage_worker_workspace`).
    pub build_context: PathBuf,
    /// Content hash naming the image: the binary hash for a worker
    /// build ([`crate::hash::compute_binary_hash`]), the node-test
    /// hash for a test build
    /// ([`crate::hash::compute_node_test_hash`]).
    pub content_hash: String,
}

/// The cargo crate name of every generated worker. One constant for every
/// project, so the top crate (and its binary, `sanitize_crate_name` of
/// this) is the same unit in the shared compile cache whichever project
/// asks: the builder base precompiles the stock project's worker under
/// this name and a project's build finds every package crate it did not
/// touch already fresh.
pub const WORKER_CRATE_NAME: &str = "weft-worker";

/// A stock project (`weft new` output, untouched) held in a temp dir, with
/// its compiled definition and catalog: the input every standard-worker
/// question is answered from. The full-library image every untouched
/// project runs on is its build, and the builder base precompiles its
/// worker crate. Its `nodes/base_catalog` is a symlink to the
/// installation's catalog instead of the copy `weft new` makes: every
/// reader of a node tree follows symlinks, the hashes label package roots
/// relative to the project root, and a copy of the whole catalog per
/// question asked would be paid on every `weft build`. The project's name
/// and id never reach the worker image, so any stock project names the
/// same one.
pub struct StockProject {
    _dir: tempfile::TempDir,
    pub project: Project,
    pub definition: weft_core::project::ProjectDefinition,
    pub catalog: FsCatalog,
}

impl StockProject {
    /// Every catalog node type: the set the stock worker compiles.
    pub fn node_types(&self) -> BTreeSet<String> {
        codegen::node_types_for(&self.definition, &self.catalog, codegen::NodeSet::Full)
    }

    /// What the builder base installs and exports to compile this
    /// worker: the stdlib packages' build-stage system packages and
    /// `[build.env]`.
    pub fn builder_stage(&self) -> CompileResult<worker_image::BuilderStage> {
        worker_image::BuilderStage::for_nodes(&self.catalog, &self.node_types(), self.project.root.as_path())
    }

    pub fn materialize() -> CompileResult<Self> {
        let dir = tempfile::tempdir().map_err(CompileError::Io)?;
        for (rel, bytes) in crate::project::scaffold_files("standard-worker", uuid::Uuid::nil())? {
            let path = dir.path().join(rel);
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).map_err(CompileError::Io)?;
            }
            std::fs::write(path, bytes).map_err(CompileError::Io)?;
        }
        std::fs::create_dir_all(dir.path().join("nodes")).map_err(CompileError::Io)?;
        let stdlib = weft_catalog::stdlib_root().map_err(CompileError::Build)?;
        std::os::unix::fs::symlink(&stdlib, crate::project::base_catalog_dir(dir.path()))
            .map_err(CompileError::Io)?;
        let project = Project::load(dir.path())?;
        let (definition, catalog) = crate::hash::load_enriched_project(&project)
            .map_err(|e| CompileError::Build(format!("compile the stock project: {e}")))?;
        Ok(Self { _dir: dir, project, definition, catalog })
    }
}

/// Validate + codegen + stage from an ALREADY-COMPILED definition. Pipeline:
///
/// 1. Validate the (resolved) definition, abort on any error.
/// 2. Codegen the cargo crate at `.weft/target/build/`.
/// 3. Emit the multi-stage Dockerfile at
///    `.weft/target/Dockerfile.worker`.
/// 4. Stage the docker build context at
///    `.weft/target/worker-image/` with `build/` and, when not
///    using the pre-built builder base, `weft/` too; the Dockerfile
///    is laid out so `docker build .` works.
///
/// Takes the definition + catalog the caller already produced rather than
/// recompiling from source: the caller resolves `@asset` refs into the
/// definition (deferred file markers become concrete file values) BEFORE
/// this runs, and a fresh compile-from-source here would see the raw
/// markers instead (a file-typed `@asset` reads as a `String` at parse and
/// would fail validation against its `File` port). This is also the only
/// validation gate on the build path, so it runs on the resolved
/// definition, not the raw source. `Structural` (not `Runtime`): a project
/// may build without every secret filled; runtime-rule gaps surface at run.
///
/// `builder_base_ref` is the shared builder-base image ref the CLI
/// computed + ensured. When it kicks in (debian-family runtime, no
/// custom template), the build context omits `weft/` because the
/// engine workspace already lives inside the base image at `/weft/`.
pub fn build_project(
    project: &Project,
    definition: &weft_core::project::ProjectDefinition,
    catalog: &FsCatalog,
    builder_base_ref: &str,
    node_set: codegen::NodeSet,
) -> CompileResult<StagedImageBuild> {
    let project_root = project.root.as_path();
    crate::bail_on_errors(crate::validate::validate_with_mode(
        definition,
        catalog,
        ValidationMode::Structural,
    ))?;
    // Project identity arrives with each execution, never in the worker image.
    let crate_name = WORKER_CRATE_NAME;

    let crate_root = project_root.join(".weft").join("target").join("build");
    let referenced_nodes = codegen::node_types_for(definition, catalog, node_set);
    codegen::emit(definition, project_root, &crate_root, catalog, crate_name, node_set)?;
    let binary_name = sanitize_crate_name(crate_name);

    let dockerfile_summary = worker_image::emit(
        &project.manifest.build.worker,
        project_root,
        catalog,
        &referenced_nodes,
        &binary_name,
        builder_base_ref,
        &StockProject::materialize()?.builder_stage()?,
    )?;
    let dockerfile_path = project_root.join(".weft/target/Dockerfile.worker");
    if let Some(parent) = dockerfile_path.parent() {
        std::fs::create_dir_all(parent).map_err(CompileError::Io)?;
    }
    std::fs::write(&dockerfile_path, &dockerfile_summary.body).map_err(CompileError::Io)?;

    let weft_root = resolve_weft_root()?;
    // The pre-built builder base bakes `/weft/` (the engine workspace)
    // into its layers, so the per-project Dockerfile no longer COPYs
    // it. Skip the host-side stage of `weft/` in that case (a
    // unneeded COPY into the context would be wasted bytes + a slower
    // tarball for the docker build).
    let stage_weft = dockerfile_summary.builder_base.is_none();
    let build_context = stage_build_context(
        &project_root.join(".weft").join("target").join("worker-image"),
        project_root,
        &crate_root,
        &weft_root,
        &dockerfile_path,
        catalog,
        &referenced_nodes,
        stage_weft,
    )?;

    let content_hash =
        crate::hash::compute_binary_hash(definition, project, &weft_root, catalog, node_set)
            .map_err(|e| CompileError::Build(format!("compute binary hash: {e}")))?;

    Ok(StagedImageBuild { build_context, content_hash })
}

/// Stage the docker build context under
/// `.weft/target/worker-image/`. Layout:
///
/// ```text
/// worker-image/
///   Dockerfile           (copy of Dockerfile.worker)
///   build/               (the generated cargo crate)
///   weft/                (the weft workspace: crates, Cargo.toml, Cargo.lock)
///   project-nodes/       (the project's nodes/ dir: every node's source)
/// ```
///
/// `weft/` carries only the language runtime (the workspace crates the
/// generated binary depends on as path deps), NOT any node code. Node
/// source comes entirely from `project-nodes/`: the project owns all
/// its nodes. The copies are minimal so docker's tarball and layer
/// cache aren't churned by unrelated host artifacts (`.git`, `target/`,
/// `node_modules/`).
///
/// `project-nodes/` holds ONLY the package roots the project actually
/// references, each copied to its path relative to `nodes/` so the
/// `#[path]` shims resolve. Discovery already walked `nodes/` and
/// grouped nodes under package roots; staging copies those roots
/// rather than re-walking the tree, so there is a single directory
/// walker (discovery) and nothing for a second walker to disagree
/// with (e.g. on symlink handling). Unreferenced nodes never enter the
/// build context.
#[allow(clippy::too_many_arguments)]
fn stage_build_context(
    ctx: &Path,
    project_root: &Path,
    crate_root: &Path,
    weft_root: &Path,
    dockerfile_path: &Path,
    catalog: &FsCatalog,
    referenced_nodes: &BTreeSet<String>,
    stage_weft: bool,
) -> CompileResult<PathBuf> {
    let ctx = ctx.to_path_buf();
    if ctx.exists() {
        std::fs::remove_dir_all(&ctx).map_err(CompileError::Io)?;
    }
    std::fs::create_dir_all(&ctx).map_err(CompileError::Io)?;

    std::fs::copy(dockerfile_path, ctx.join("Dockerfile")).map_err(CompileError::Io)?;

    // `build/` = generated cargo crate. Copy target excluded; it
    // doesn't exist on the host anymore (no host cargo build), but
    // belt-and-suspenders.
    copy_dir_filtered(crate_root, &ctx.join("build"), &["target"])?;

    // `weft/` = the language runtime workspace (crates + manifest).
    // Staged into the build context only when the project Dockerfile
    // needs to COPY it in (no pre-built builder base, e.g. custom
    // template or non-debian runtime). When the pre-built base is
    // used, `/weft/` lives in the base image layers and re-COPYing
    // it would just bloat the build context tarball.
    if stage_weft {
        stage_worker_workspace(weft_root, &ctx.join("weft"))?;
    }

    stage_project_nodes(project_root, catalog, referenced_nodes, &ctx.join("project-nodes"))?;

    Ok(ctx)
}

/// `project-nodes/` = each referenced package root, placed under `dest`
/// at its path relative to the project root (`nodes/...` or, for a node
/// beside the code, `src/...`), so the emitted `#[path]` includes
/// resolve once the directory is COPYed to [`worker_image::NODES_MOUNT`].
/// The worker build context and the builder base (which precompiles the
/// stock project's worker) stage the same way, so a package crate the
/// base compiled is the same unit a project's build asks for.
fn stage_project_nodes(
    project_root: &Path,
    catalog: &FsCatalog,
    referenced_nodes: &BTreeSet<String>,
    dest: &Path,
) -> CompileResult<()> {
    for root in catalog.package_roots_for(referenced_nodes) {
        let rel = root.strip_prefix(project_root).map_err(|_| {
            CompileError::Build(format!(
                "package root {} is not under the project root {}",
                root.display(),
                project_root.display()
            ))
        })?;
        // The shared node-tree exclude (same set the source hash walks
        // over), so staging and hashing agree on a node's byte-content:
        // a file the build copies but the hash skips (or vice versa)
        // is how a stale worker image gets served.
        copy_dir_filtered(&root, &dest.join(rel), weft_catalog::NODE_TREE_EXCLUDE)?;
    }
    Ok(())
}

/// The bare content-addressed node-test image tag,
/// `weft-node-tests:<test_hash>`. Single source of truth shared by the
/// CLI (build + load) and whoever spawns the test pod, like
/// [`worker_image_tag`].
pub const NODE_TEST_IMAGE_REPO: &str = "weft-node-tests";

pub fn node_test_image_tag(test_hash: &str) -> String {
    format!("{NODE_TEST_IMAGE_REPO}:{test_hash}")
}

/// Emit + stage the per-package node-test IMAGE build. The image-bound
/// twin of the local test build: same emitted crate shape
/// ([`codegen::emit_test_crate`]) with container mount paths, the same
/// multi-stage Dockerfile machinery as the worker (`worker_image::emit`
/// with the package's own node set, so the package's system deps land
/// in the image), the same context staging. Needs only the catalog:
/// no project parse, no validation of other packages.
pub fn build_test_artifact(
    project: &Project,
    catalog: &FsCatalog,
    package_name: &str,
    builder_base_ref: &str,
) -> CompileResult<StagedImageBuild> {
    let project_root = project.root.as_path();
    let weft_root = resolve_weft_root()?;

    // Every staged path is per-package (crate root, Dockerfile,
    // context): a project's packages build test images side by side,
    // so a shared path would have one package's staging clobber
    // another's mid-build.
    let stem = sanitize_crate_name(package_name);
    let crate_root = project_root
        .join(".weft")
        .join("target")
        .join("test-build")
        .join(&stem);
    let test_crate = codegen::emit_test_crate(
        &crate_root,
        catalog,
        package_name,
        &codegen::EmitPaths::Container { project_root: project_root.to_path_buf() },
    )?;

    let referenced: BTreeSet<String> = test_crate.node_types.iter().cloned().collect();
    let dockerfile_summary = worker_image::emit(
        &project.manifest.build.worker,
        project_root,
        catalog,
        &referenced,
        &test_crate.binary_name,
        builder_base_ref,
        &StockProject::materialize()?.builder_stage()?,
    )?;
    let dockerfile_path = project_root
        .join(".weft")
        .join("target")
        .join(format!("Dockerfile.node-tests.{stem}"));
    if let Some(parent) = dockerfile_path.parent() {
        std::fs::create_dir_all(parent).map_err(CompileError::Io)?;
    }
    std::fs::write(&dockerfile_path, &dockerfile_summary.body).map_err(CompileError::Io)?;

    let stage_weft = dockerfile_summary.builder_base.is_none();
    let build_context = stage_build_context(
        &project_root
            .join(".weft")
            .join("target")
            .join("test-image")
            .join(&stem),
        project_root,
        &test_crate.crate_root,
        &weft_root,
        &dockerfile_path,
        catalog,
        &referenced,
        stage_weft,
    )?;

    let content_hash = node_test_content_hash(project, catalog, package_name)?;

    Ok(StagedImageBuild { build_context, content_hash })
}

/// The content hash naming a package's node-test build. One function
/// answers both consumers: the test-image tag (`build_test_artifact`)
/// and `weft node-test-hash`, whose printed value a scripted runner
/// records to skip re-running a package's already-passed live tests.
/// Covers the package's own files, the image recipe, the full catalog
/// type registry (a sibling's type edit changes this package's test
/// binary), and the worker build environment.
pub fn node_test_content_hash(
    project: &Project,
    catalog: &FsCatalog,
    package_name: &str,
) -> CompileResult<crate::hash::SourceHash> {
    let weft_root = resolve_weft_root()?;
    let package_root = package_root(catalog, package_name)?;
    crate::hash::compute_node_test_hash(&package_root, project, &weft_root, catalog)
        .map_err(|e| CompileError::Build(format!("compute node-test hash: {e}")))
}

/// Content hash of everything that can change a package's node-test
/// OUTCOME: the package's own sources (every member's mod.rs /
/// metadata.json / tests.rs plus the package's shared files) and the
/// catalog's type registry (a sibling's type edit changes how this
/// package's ports resolve). The staleness rule for the live-pass
/// cache and the test-listing cache, via `weft node-test-hash`.
/// Deliberately narrower than [`node_test_content_hash`] (the image
/// tag): live tests spend real provider money, so an engine or
/// image-recipe edit (which rightly rebuilds the test image) does not
/// by itself invalidate a recorded live pass.
pub fn node_test_cache_hash(
    project: &Project,
    catalog: &FsCatalog,
    package_name: &str,
) -> CompileResult<crate::hash::SourceHash> {
    let weft_root = resolve_weft_root()?;
    let package_root = package_root(catalog, package_name)?;
    crate::hash::compute_node_test_outcome_hash(
        &package_root,
        &[project.root.as_path(), weft_root.as_path()],
        catalog,
    )
    .map_err(|e| CompileError::Build(format!("compute node-test cache hash: {e}")))
}

/// The filesystem root of `package_name`, or a loud error naming the
/// project's nodes/ as the place it was looked for.
fn package_root(catalog: &FsCatalog, package_name: &str) -> CompileResult<PathBuf> {
    catalog
        .packages()
        .find(|p| p.name == package_name)
        .map(|p| p.root.clone())
        .ok_or_else(|| {
            CompileError::Build(format!("no package named '{package_name}' in this project's nodes/"))
        })
}

/// Top-level entries of a closure crate that enter the worker slice:
/// everything except the crate-ROOT `tests/` dir (integration tests cargo
/// never compiles for a path dependency, so staging or hashing them only
/// makes an unrelated test edit rebuild the base and every worker image)
/// and the shared node-tree excludes. Root-anchored on purpose: a dir named
/// `tests` deeper in the tree (weft-core's `src/tests`, reached via a
/// `#[path]` attribute) stays in, so the exclusion can never silently drop
/// a module the crate actually declares. THE one enumerator both the stager
/// (`stage_worker_workspace`) and the hasher (`hash::hash_worker_build_env`)
/// iterate, so staged and hashed bytes stay equal by construction. Sorted
/// for deterministic hashing.
pub fn worker_crate_entries(crate_dir: &Path) -> CompileResult<Vec<(String, PathBuf)>> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(crate_dir).map_err(CompileError::Io)? {
        let entry = entry.map_err(CompileError::Io)?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if name == "tests" || weft_catalog::is_node_tree_excluded(&name) {
            continue;
        }
        // Symlinks are skipped in the workspace-crate slice, by the
        // stager and the hasher alike (this enumerator is both), so
        // staged and hashed bytes stay equal. Unlike a user's node
        // tree, weft's own crates never legitimately hold one.
        if entry.file_type().map_err(CompileError::Io)?.is_symlink() {
            continue;
        }
        out.push((name, entry.path()));
    }
    out.sort();
    Ok(out)
}

/// Stage the worker-linked slice of the weft workspace at `dest`:
/// `crates/<closure>` (exactly `codegen::worker_workspace_crates`, the only
/// workspace crates a worker build links), a workspace manifest whose members
/// are rewritten to that closure, and the workspace `Cargo.lock`. Everything
/// else in the workspace (CLI, dispatcher, tests) is deliberately absent, so
/// it can neither bloat the docker context nor invalidate a worker build.
///
/// The staged set must match what `hash::hash_worker_build_env` hashes
/// (closure crates + manifests): a file staged-but-not-hashed (or vice versa)
/// is a stale-image hole. Crate copies preserve mtimes (see
/// `copy_dir_filtered`) so cargo's fingerprint inside the container sees
/// unchanged files as unchanged; the generated manifest + copied lock get the
/// SOURCE files' mtimes stamped for the same reason.
pub fn stage_worker_workspace(weft_root: &Path, dest: &Path) -> CompileResult<()> {
    std::fs::create_dir_all(dest).map_err(CompileError::Io)?;
    let closure = codegen::worker_workspace_crates(weft_root)?;
    for name in &closure {
        let crate_dest = dest.join("crates").join(name);
        std::fs::create_dir_all(&crate_dest).map_err(CompileError::Io)?;
        for (entry_name, entry_path) in
            worker_crate_entries(&weft_root.join("crates").join(name))?
        {
            let to = crate_dest.join(&entry_name);
            if entry_path.is_dir() {
                copy_dir_filtered(&entry_path, &to, weft_catalog::NODE_TREE_EXCLUDE)?;
            } else if entry_path.is_file() {
                std::fs::copy(&entry_path, &to).map_err(CompileError::Io)?;
                mirror_mtime(&entry_path, &to);
            }
        }
    }
    write_scoped_workspace_manifest(weft_root, dest, &closure)?;
    let lock_src = weft_root.join("Cargo.lock");
    let lock_dst = dest.join("Cargo.lock");
    std::fs::copy(&lock_src, &lock_dst).map_err(CompileError::Io)?;
    mirror_mtime(&lock_src, &lock_dst);
    Ok(())
}

/// Write `dest/Cargo.toml`: the real workspace manifest with `members`
/// rewritten to exactly `crates`. Everything else ([workspace.dependencies],
/// [workspace.package], profiles) is carried verbatim, so `workspace = true`
/// inheritance in the staged crates resolves identically to the full
/// workspace. Extra `[workspace.dependencies]` path entries pointing at
/// absent crates are harmless: cargo only opens a path when something in the
/// resolve graph references it, and nothing in the staged slice does.
fn write_scoped_workspace_manifest(
    weft_root: &Path,
    dest: &Path,
    crates: &[String],
) -> CompileResult<()> {
    let src = weft_root.join("Cargo.toml");
    let raw = std::fs::read_to_string(&src).map_err(CompileError::Io)?;
    let mut manifest: toml::Value = raw
        .parse()
        .map_err(|e| CompileError::Build(format!("parse {}: {e}", src.display())))?;
    let workspace = manifest
        .get_mut("workspace")
        .and_then(|w| w.as_table_mut())
        .ok_or_else(|| {
            CompileError::Build(format!("{} has no [workspace] table", src.display()))
        })?;
    workspace.insert(
        "members".into(),
        toml::Value::Array(
            crates
                .iter()
                .map(|c| toml::Value::String(format!("crates/{c}")))
                .collect(),
        ),
    );
    let body = toml::to_string_pretty(&manifest)
        .map_err(|e| CompileError::Build(format!("serialize scoped workspace manifest: {e}")))?;
    let dst = dest.join("Cargo.toml");
    std::fs::write(&dst, body).map_err(CompileError::Io)?;
    mirror_mtime(&src, &dst);
    Ok(())
}

/// Stamp `src`'s mtime onto `dst`. Cargo's fingerprint inside the docker
/// build container short-circuits a clean crate only when every source
/// file's mtime is older than its rlib; without mirroring, every staging
/// run gives cargo a wall-clock mtime and every crate looks dirty on every
/// build. Warn-only on failure: correctness is unaffected (only rebuild
/// time), but an invisible failure would look like the cache mysteriously
/// stopped working, so it must be loud.
fn mirror_mtime(src: &Path, dst: &Path) {
    match std::fs::metadata(src).and_then(|meta| meta.modified()) {
        Ok(modified) => stamp_mtime(dst, modified),
        Err(e) => tracing::warn!(
            target: "weft_compiler::build",
            src = %src.display(),
            error = %e,
            "could not read the source mtime; the copy keeps a wall-clock mtime and cargo will rebuild it"
        ),
    }
}

/// Set `dst`'s mtime to `modified`. The safe failure direction is the
/// wall-clock mtime `dst` already has (newer than anything cached, so
/// cargo recompiles), hence warn-only, and loud for the reason above.
pub(crate) fn stamp_mtime(dst: &Path, modified: std::time::SystemTime) {
    let result = filetime::set_file_mtime(dst, filetime::FileTime::from_system_time(modified));
    if let Err(e) = result {
        tracing::warn!(
            target: "weft_compiler::build",
            file = %dst.display(),
            error = %e,
            "could not mirror source mtime; the docker build will treat this \
             file as changed and rebuild its crate"
        );
    }
}

/// Stage the builder-base docker build CONTEXT into
/// `<weft_root>/.weft-base-context/` (see [`stage_builder_base_context_at`]).
pub fn stage_builder_base_context(weft_root: &Path) -> CompileResult<PathBuf> {
    stage_builder_base_context_at(weft_root, &weft_root.join(worker_image::BASE_CONTEXT_DIR))
}

/// Stage the builder-base docker build CONTEXT at `ctx`, holding exactly what
/// the rendered Dockerfile COPYs:
///
/// ```text
/// <ctx>/
///   Dockerfile           (worker-builder-base.Dockerfile with its tokens
///                         substituted; build with -f THIS file)
///   rust-toolchain.toml
///   Cargo.toml           (workspace manifest scoped to the worker closure)
///   Cargo.lock
///   crates/<closure>/    (only the worker-linked crates)
///   .weft-warmup/        (the stock project's full-library worker crate,
///                         emitted by `codegen::emit` exactly as a project
///                         build emits its own)
///   project-nodes/       (the whole stdlib catalog, staged as a project
///                         build stages its referenced packages)
/// ```
///
/// The base precompiles the STOCK PROJECT'S WORKER, not a deps-only stand-in:
/// the package crates pull in dependencies of their own (`sqlx`, `pyo3`,
/// `tungstenite`, ...) and change how cargo unifies the features of the
/// shared ones, so a base that had compiled only the engine's dependencies
/// left every first build on a host recompiling the engine, most of the
/// dependency tree and every package crate (measured at 1m07 on a warm
/// machine). Compiling the same crate the standard worker image is built
/// from, at the same paths (`/work`, `/weft/project-nodes`), makes every
/// package crate an untouched project links fingerprint-fresh in the seeded
/// compile cache; a project compiles only the packages it edited or added
/// and its thin top crate.
///
/// The Dockerfile is RENDERED, not copied: `{{target_cache_key}}` becomes a
/// hash of Cargo.lock + rust-toolchain.toml, so a dependency or toolchain
/// change starts a fresh compile cache instead of inheriting (and baking into
/// the image) every artifact ever built; `{{worker_binary}}` names the binary
/// the cache sweep keeps; `{{install_build_system_packages}}` and
/// `{{build_env_lines}}` are what the stdlib packages need to compile
/// (`StockProject::builder_stage`), the same lines a project's builder stage
/// would carry, which is why a project FROMing the base installs only what
/// lies beyond them. The base hash keeps covering the SOURCE template and the
/// packages' `deps.toml`; the rendered values are pure functions of files it
/// already covers.
///
/// Regenerated wholesale on every call (it is a derived artifact): a stale
/// leftover crate from a previous closure would otherwise linger in the
/// context and the baked image. Callers only invoke this when the
/// content-addressed base tag is absent, so the staging cost is paid exactly
/// once per base rebuild.
pub fn stage_builder_base_context_at(weft_root: &Path, ctx: &Path) -> CompileResult<PathBuf> {
    if ctx.exists() {
        std::fs::remove_dir_all(ctx).map_err(CompileError::Io)?;
    }
    stage_worker_workspace(weft_root, ctx)?;
    let toolchain_src = weft_root.join("rust-toolchain.toml");
    let toolchain_dst = ctx.join("rust-toolchain.toml");
    std::fs::copy(&toolchain_src, &toolchain_dst).map_err(CompileError::Io)?;
    mirror_mtime(&toolchain_src, &toolchain_dst);

    let stock = StockProject::materialize()?;
    let referenced = stock.node_types();
    codegen::emit(
        &stock.definition,
        stock.project.root.as_path(),
        &ctx.join(worker_image::WARMUP_CRATE_DIR),
        &stock.catalog,
        WORKER_CRATE_NAME,
        codegen::NodeSet::Full,
    )?;
    stage_project_nodes(stock.project.root.as_path(), &stock.catalog, &referenced, &ctx.join("project-nodes"))?;

    let (install_packages, env_lines) = stock.builder_stage()?.render();
    let template_path = weft_root.join(crate::hash::BUILDER_BASE_DOCKERFILE);
    let mut rendered = std::fs::read_to_string(&template_path).map_err(CompileError::Io)?;
    for (token, value) in [
        ("{{target_cache_key}}", target_cache_key(weft_root)?),
        ("{{worker_binary}}", sanitize_crate_name(WORKER_CRATE_NAME)),
        ("{{install_build_system_packages}}", install_packages),
        ("{{build_env_lines}}", env_lines),
    ] {
        if !rendered.contains(token) {
            return Err(CompileError::Build(format!(
                "{} lost its {token} token; the base would stop keying its compile cache \
                 on the lock + toolchain, stop sweeping it, or stop installing what the \
                 stock worker compiles with",
                template_path.display()
            )));
        }
        rendered = rendered.replace(token, &value);
    }
    std::fs::write(ctx.join("Dockerfile"), rendered).map_err(CompileError::Io)?;
    Ok(ctx.to_path_buf())
}

/// The builder-base compile-cache key: a short hash of `Cargo.lock` +
/// `rust-toolchain.toml`. Exactly the inputs whose change makes previous
/// cache contents dead weight (new dependency versions, new toolchain);
/// engine source edits keep the key, so they stay incremental.
fn target_cache_key(weft_root: &Path) -> CompileResult<String> {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    for rel in ["Cargo.lock", "rust-toolchain.toml"] {
        hasher.update(
            std::fs::read(weft_root.join(rel)).map_err(CompileError::Io)?,
        );
    }
    let digest = hasher.finalize();
    Ok(digest
        .iter()
        .take(8)
        .map(|b| format!("{b:02x}"))
        .collect::<String>())
}

/// Recursive directory copy with a simple exclude list matched on
/// the immediate entry name. Skips symlinks to avoid infinite
/// recursion and unexpected escapes from the staged context.
///
/// Preserves mtime on every copied file. The staged build context
/// feeds straight into docker, which preserves the host mtime on
/// `COPY`. Cargo's fingerprint short-circuits a clean crate when
/// every source file's mtime is older than the crate's rlib in the
/// target dir; without mtime preservation a fresh stage gives every
/// file a wall-clock mtime, every per-package crate looks dirty,
/// and cargo recompiles all of them on every edit. Preserving the
/// host mtime keeps unchanged sources looking unchanged inside the
/// container, so cargo only rebuilds the package whose node source
/// genuinely changed (plus the worker relink).
pub(crate) fn copy_dir_filtered(src: &Path, dst: &Path, exclude: &[&str]) -> CompileResult<()> {
    copy_dir_filtered_inner(src, dst, exclude, &mut Default::default())
}

/// Symlinks are followed and their TARGET bytes copied (`fs::copy`
/// reads through the link), so a symlinked `nodes/base_catalog` stages
/// as real files inside the build context; a cycle fails loudly via
/// the descent chain.
fn copy_dir_filtered_inner(
    src: &Path,
    dst: &Path,
    exclude: &[&str],
    chain: &mut Vec<PathBuf>,
) -> CompileResult<()> {
    let canon = weft_catalog::guard_node_tree_cycle(src, chain).map_err(CompileError::Io)?;
    chain.push(canon);
    let result = copy_dir_filtered_entries(src, dst, exclude, chain);
    chain.pop();
    result
}

fn copy_dir_filtered_entries(
    src: &Path,
    dst: &Path,
    exclude: &[&str],
    chain: &mut Vec<PathBuf>,
) -> CompileResult<()> {
    std::fs::create_dir_all(dst).map_err(CompileError::Io)?;
    for entry in std::fs::read_dir(src).map_err(CompileError::Io)? {
        let entry = entry.map_err(CompileError::Io)?;
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if exclude.iter().any(|e| *e == name_str) {
            continue;
        }
        let from = entry.path();
        let to = dst.join(&name);
        let kind = weft_catalog::node_tree_entry_kind(&from).map_err(CompileError::Io)?;
        if kind == weft_catalog::NodeTreeEntryKind::Dir {
            copy_dir_filtered_inner(&from, &to, exclude, chain)?;
        } else {
            std::fs::copy(&from, &to).map_err(CompileError::Io)?;
            mirror_mtime(&from, &to);
        }
    }
    Ok(())
}

/// Locate the weft workspace root. Delegates to
/// `weft_catalog::weft_repo_root` so this and the catalog's
/// `stdlib_root` resolve to the SAME path (they used to be two
/// independent copies; a drift would have the stdlib seed and the
/// build context disagree). Public so the CLI's hash + docker-build
/// paths share one resolver.
pub fn resolve_weft_root() -> CompileResult<PathBuf> {
    weft_catalog::weft_repo_root().map_err(CompileError::Build)
}

/// The repository segment for content-addressed worker images. No project id:
/// a tag is purely a function of what was compiled, so identical builds across
/// projects/tenants resolve to ONE image (the same shape infra images use).
pub const WORKER_IMAGE_REPO: &str = "weft-worker";

/// The bare (registry-UNqualified) content-addressed worker image tag,
/// `weft-worker:<binary_hash>`. THE single source of truth for the worker tag,
/// shared by the CLI (which builds + loads it onto the node) and the dispatcher
/// (which spawns it, prepending a registry prefix when one is configured). The
/// FULL binary hash is used (not a short prefix) so the tag is collision-free and the CLI and
/// the dispatcher agree by construction. Lives in weft-compiler because both the
/// CLI and the dispatcher depend on this crate; a second copy would be a drift
/// hazard (the exact bug that had the CLI tag a 16-char prefix while the
/// dispatcher spawned the full hash).
pub fn worker_image_tag(binary_hash: &str) -> String {
    format!("{WORKER_IMAGE_REPO}:{binary_hash}")
}

/// Sanitize a project/package name to a valid cargo crate + binary
/// name. Used by both codegen and the CLI so the two agree on what
/// binary the Dockerfile produces. Collision-free by construction: a
/// LOSSY sanitization (any character was replaced or prepended)
/// appends a short digest of the raw name, so `my-pkg` and `my.pkg`
/// can never sanitize to the same crate, directory, or binary name.
pub fn sanitize_crate_name(raw: &str) -> String {
    let lowered = raw.to_ascii_lowercase();
    let mut out = String::with_capacity(lowered.len());
    for ch in lowered.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    if out.is_empty() || out.starts_with(|c: char| c.is_ascii_digit()) {
        out.insert(0, 'p');
    }
    // Lossiness is judged against the RAW name: lowercasing is itself
    // lossy (`Slack` and `slack` must never land on the same crate,
    // directory, staging path, and image tag), so comparing against
    // the lowered form would miss exactly the case-only collisions.
    if out != raw {
        use sha2::{Digest, Sha256};
        let digest = Sha256::digest(raw.as_bytes());
        out.push('_');
        for b in digest.iter().take(8) {
            out.push_str(&format!("{b:02x}"));
        }
    }
    out
}

/// Where a host node-test build puts its emitted crates and its cargo
/// cache. The two are separate directories on purpose: the emitted
/// crates belong to ONE project, while the cargo cache is the same
/// compiled engine for every project on the machine and is shared
/// between them (see [`TestBuildDirs::shared`]).
pub struct TestBuildDirs {
    /// This project's emitted test crates, one subdirectory per
    /// package. Regenerated from scratch on every build.
    pub crates_root: PathBuf,
    /// Cargo's target dir. Machine-wide, so the engine and every
    /// third-party dependency compiles once per machine rather than
    /// once per project.
    pub target_dir: PathBuf,
}

impl TestBuildDirs {
    /// The layout under one root: `crates/` beside a SIBLING `target/`,
    /// so no package name (not even "target") can land on top of the
    /// cache. Used by callers that deliberately want a self-contained
    /// tree, such as the workspace's own node-test sweep.
    pub fn under(root: &Path) -> Self {
        Self { crates_root: root.join("crates"), target_dir: root.join("target") }
    }

    /// The layout a project's `weft test-node` uses: this project's own
    /// emitted crates, and the cargo cache SHARED by every project on
    /// the machine.
    ///
    /// Nothing lands in the user's project directory. A per-project
    /// cargo cache meant every project that ran node tests paid the
    /// full compile of the engine and every third-party dependency
    /// again, in its own folder, so a machine with six projects held
    /// six copies of the same artifacts. Shared, that cost is paid
    /// once. Cargo keeps each package's artifacts apart inside one
    /// target dir by construction, so projects cannot corrupt each
    /// other's builds.
    pub fn shared(project_root: &Path, cache_root: &Path) -> Self {
        Self {
            crates_root: cache_root.join("projects").join(project_slug(project_root)),
            target_dir: cache_root.join("target"),
        }
    }
}

/// A filesystem-safe, collision-resistant name for one project's slice
/// of the shared cache: the directory name plus a hash of the full
/// path, so two projects called `bot` in different folders never share
/// a slice and the directory is still readable by a human.
fn project_slug(project_root: &Path) -> String {
    use sha2::{Digest, Sha256};
    let name = project_root
        .file_name()
        .map(|n| sanitize_crate_name(&n.to_string_lossy()))
        .unwrap_or_else(|| "project".to_string());
    let digest = Sha256::digest(project_root.to_string_lossy().as_bytes());
    let mut slug = name;
    slug.push('-');
    for b in digest.iter().take(6) {
        slug.push_str(&format!("{b:02x}"));
    }
    slug
}

/// Every package a run will test, emitted as members of one cargo
/// workspace, ready to build.
///
/// Emitting them ALL before building ANY is what makes the shared
/// workspace pay off: cargo unifies features across a workspace's
/// members, so a dependency two packages share is compiled once. Grown
/// one member at a time instead, each addition widens the feature
/// union and rebuilds what the previous member already built.
pub struct TestWorkspace {
    crates: BTreeMap<String, codegen::TestCrate>,
    /// The workspace root cargo is invoked from.
    root: PathBuf,
    target_dir: PathBuf,
}

/// Emit every package's node-test crate into `dirs` as members of one
/// workspace. `packages` is exactly what this run will test, so a
/// package nobody targeted never joins the resolve and a broken
/// sibling stays harmless.
pub fn prepare_test_workspace(
    catalog: &FsCatalog,
    packages: &[String],
    dirs: &TestBuildDirs,
) -> CompileResult<TestWorkspace> {
    let weft_root = resolve_weft_root()?;
    let mut crates = BTreeMap::new();
    let mut members = Vec::new();
    for package in packages {
        let member = sanitize_crate_name(package);
        let crate_root = dirs.crates_root.join(&member);
        let test_crate = codegen::emit_test_crate(
            &crate_root,
            catalog,
            package,
            &codegen::EmitPaths::Local { weft_root: weft_root.clone() },
        )
        .map_err(|e| CompileError::Build(format!("emit test crate for '{package}': {e}")))?;
        crates.insert(package.clone(), test_crate);
        members.push(member);
    }
    codegen::emit_test_workspace(&dirs.crates_root, &members)?;
    Ok(TestWorkspace {
        crates,
        root: dirs.crates_root.clone(),
        target_dir: dirs.target_dir.clone(),
    })
}

/// Build EVERY prepared package in one cargo invocation and return
/// each package's built binary.
///
/// One invocation, not one per package, because cargo's v2 resolver
/// resolves features per invocation: built one at a time, two packages
/// that share a dependency with different features each get their own
/// copy of it (that is where ten copies of `sqlx-postgres` came from,
/// and merging the crates into one workspace did not change it on its
/// own). Built together, the resolver unifies them and compiles each
/// dependency once.
///
/// The trade is that a package that fails to COMPILE fails the whole
/// build rather than letting earlier packages report first. Cargo's
/// diagnostics name the offending crate on stderr as they stream, so
/// what broke is still on screen; nothing runs until the code compiles.
///
/// Artifact paths are read from cargo's
/// `--message-format=json-render-diagnostics` output, never guessed (a
/// configured default target triple or profile would move them).
pub fn build_node_test_binaries(
    workspace: &TestWorkspace,
) -> CompileResult<BTreeMap<String, PathBuf>> {
    if workspace.crates.is_empty() {
        return Ok(BTreeMap::new());
    }
    eprintln!("building tests for {} package(s)...", workspace.crates.len());
    let out = std::process::Command::new("cargo")
        .args(["build", "--workspace", "--message-format=json-render-diagnostics"])
        .current_dir(&workspace.root)
        .env("CARGO_TARGET_DIR", &workspace.target_dir)
        .stderr(std::process::Stdio::inherit())
        .output()
        .map_err(|e| {
            CompileError::Build(format!(
                "run cargo (is a Rust toolchain installed? local test runs compile \
                 on the host): {e}"
            ))
        })?;
    if !out.status.success() {
        return Err(CompileError::Build(
            "the node-test build failed (see cargo's output above for which package \
             and why)"
                .to_string(),
        ));
    }
    #[derive(Deserialize)]
    struct CargoMessage {
        reason: String,
        #[serde(default)]
        executable: Option<PathBuf>,
        #[serde(default)]
        target: Option<CargoTarget>,
    }
    #[derive(Deserialize)]
    struct CargoTarget {
        name: String,
    }
    let stdout = String::from_utf8_lossy(&out.stdout);
    let artifacts: Vec<(String, PathBuf)> = stdout
        .lines()
        .filter_map(|l| serde_json::from_str::<CargoMessage>(l).ok())
        .filter(|m| m.reason == "compiler-artifact")
        .filter_map(|m| Some((m.target?.name, m.executable?)))
        .collect();
    let mut built = BTreeMap::new();
    for (package, test_crate) in &workspace.crates {
        // Cargo names targets with `-`; the crate name uses `_`.
        let want = test_crate.binary_name.replace('_', "-");
        let found = artifacts
            .iter()
            .find(|(name, _)| *name == test_crate.binary_name || *name == want)
            .map(|(_, path)| path.clone());
        let Some(path) = found else {
            return Err(CompileError::Build(format!(
                "cargo built package '{package}' but reported no executable named \
                 '{}'; the emitted test crate's binary target is missing",
                test_crate.binary_name
            )));
        };
        built.insert(package.clone(), path);
    }
    Ok(built)
}

/// Remove a test binary once its tests have run.
///
/// A test binary is an OUTPUT, not a cache: it is ~110MB, and keeping
/// one per package held 3.2GB of the shared cache between runs for
/// nothing. What makes the next run fast is the dependency cache
/// beside it, which stays, so rebuilding this is a link step.
///
/// BOTH names have to go. Cargo writes the artifact into `deps/` as
/// `<name>-<hash>` and hardlinks `<name>` beside it, so removing only
/// the one cargo reported frees no space at all: the bytes are still
/// there under the other name.
///
/// A failure to remove is not worth failing a green test run over, so
/// it says so and carries on; the size cap is the backstop.
pub fn drop_built_binary(binary: &Path) {
    let mut paths = vec![binary.to_path_buf()];
    // `deps/<stem>-<16 hex>`, matched exactly so a package whose name
    // merely starts with another's cannot take its neighbour's
    // artifact with it.
    if let (Some(stem), Some(deps)) =
        (binary.file_stem().and_then(|s| s.to_str()), binary.parent().map(|p| p.join("deps")))
    {
        if let Ok(entries) = std::fs::read_dir(&deps) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let Some(name) = name.to_str() else { continue };
                let Some(hash) = name.strip_prefix(&format!("{stem}-")) else { continue };
                if hash.len() == 16 && hash.chars().all(|c| c.is_ascii_hexdigit()) {
                    paths.push(entry.path());
                }
            }
        }
    }
    for path in paths {
        if let Err(e) = std::fs::remove_file(&path) {
            if e.kind() != std::io::ErrorKind::NotFound {
                eprintln!("note: could not remove {} ({e})", path.display());
            }
        }
    }
}

/// Build the catalog for a project: discover every node under its
/// `nodes/` directory. That is the single source of truth (the stdlib
/// is cloned in at `weft new`), so the project is self-contained and
/// nothing reaches into the weft installation at build time.
pub fn build_project_catalog(project_root: &Path) -> CompileResult<FsCatalog> {
    let roots = crate::project::node_roots(project_root);
    FsCatalog::discover_roots_with_policy(
        &roots.iter().map(|r| r.as_path()).collect::<Vec<_>>(),
        weft_catalog::DiscoverPolicy::Strict,
    )
    .map_err(|e| CompileError::Enrich(format!("catalog: {e}")))
}

#[cfg(test)]
mod tests {
    use super::sanitize_crate_name;

    /// The builder base compiles the STOCK WORKER: the staged context
    /// carries a package crate per stdlib package (so their rlibs, and the
    /// dependencies they pull in, are baked with the feature unification a
    /// real worker triggers) and the stdlib sources those crates include,
    /// and the Dockerfile has both tokens rendered. A base that only warmed
    /// the engine's dependencies is the regression this pins against: it
    /// left every first build on a host recompiling most of the tree.
    #[test]
    fn the_builder_base_context_carries_the_stock_worker_and_the_stdlib() {
        let weft_root = super::resolve_weft_root().expect("resolve weft root");
        let dir = tempfile::tempdir().unwrap();
        let ctx = super::stage_builder_base_context_at(&weft_root, &dir.path().join("ctx")).expect("stage");
        let crate_root = ctx.join(crate::worker_image::WARMUP_CRATE_DIR);
        let manifest = std::fs::read_to_string(crate_root.join("Cargo.toml")).unwrap();
        assert!(manifest.contains("pkg_format = { path = \"./pkg_format-"), "{manifest}");
        assert!(crate_root.join(crate::worker_image::CACHE_GC_SCRIPT_NAME).is_file());
        // Staged at its project-relative path: a node under `nodes/` and
        // one beside the code under `src/` land side by side under the mount.
        assert!(ctx.join("project-nodes/nodes/base_catalog/basic/format/mod.rs").is_file());
        let dockerfile = std::fs::read_to_string(ctx.join("Dockerfile")).unwrap();
        assert!(!dockerfile.contains("{{"), "every token rendered: {dockerfile}");
        assert!(dockerfile.contains(&format!("/work {}", sanitize_crate_name(super::WORKER_CRATE_NAME))));
        assert!(dockerfile.contains("COPY project-nodes /weft/project-nodes"));
        // The stdlib's own build packages are installed in the base (the
        // Python node needs the interpreter's headers to compile pyo3).
        assert!(dockerfile.contains("libpython3-dev"), "{dockerfile}");
    }

    /// The names cargo/docker/staging key on must never collide: every
    /// pair that differs only by case, punctuation, or the lossy `_`
    /// replacement has to sanitize to DIFFERENT names.
    #[test]
    fn sanitize_crate_name_is_collision_free() {
        for (a, b) in [("slack", "Slack"), ("my-pkg", "my.pkg"), ("my-pkg", "my_pkg")] {
            assert_ne!(
                sanitize_crate_name(a),
                sanitize_crate_name(b),
                "'{a}' and '{b}' must sanitize apart"
            );
        }
        // A lossless name stays bare (no digest suffix).
        assert_eq!(sanitize_crate_name("slack"), "slack");
        assert_eq!(sanitize_crate_name("my_pkg"), "my_pkg");
        // The empty and digit-leading paths get the `p` prefix, count
        // as lossy, and stay distinct from names that already look
        // like their prefixed form.
        assert!(sanitize_crate_name("").starts_with("p_"));
        assert!(sanitize_crate_name("1pkg").starts_with("p1pkg_"));
        assert_ne!(sanitize_crate_name("1pkg"), sanitize_crate_name("p1pkg"));
    }
}
