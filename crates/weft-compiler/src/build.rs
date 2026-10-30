//! Compile pipeline orchestration.
//!
//! Given a project and its ALREADY-COMPILED definition, validate and
//! stage the worker the way its platform packages it
//! (`weft_core::builds::WorkerPackaging`): on a local install, a host
//! build ([`stage_host_build`]) the machine compiles with its own
//! toolchain; on the cloud, the generated cargo crate at
//! `.weft/target/build/` with a multi-stage Dockerfile and its build
//! context, whose `cargo build` runs inside the image build. A package's
//! node-test server is always a host build ([`build_test_artifact`]).

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::codegen;
use crate::error::{BuildError, BuildResult};
use crate::project::Project;
use crate::worker_image;
use weft_catalog::FsCatalog;

/// One staged build, whichever program it compiles (the project's worker
/// or a package's node-test server): the staged context plus the content
/// hash that NAMES what it builds, so the caller can build it or
/// recognize it as already present.
pub struct StagedImageBuild {
    /// A host build's claim on the shared crates it staged, until it is
    /// compiled: dropped with the staged build ([`SharedCratesHold`]).
    pub hold: Option<std::sync::Arc<SharedCratesHold>>,
    /// Absolute path to the build context: the host recipe and the worker
    /// crate for a host build; for an image, the `Dockerfile`, `build/`
    /// (the generated cargo crate), `project-nodes/` and, when the
    /// pre-built builder base is not in play, `weft/` (the worker-linked
    /// slice of the weft workspace, see `stage_worker_workspace`).
    pub build_context: PathBuf,
    /// Content hash naming it: the binary hash for a worker build
    /// ([`crate::hash::compute_binary_hash`]), the node-test hash for a
    /// test server ([`crate::hash::compute_node_test_hash`]).
    pub content_hash: String,
    /// How it is built: the context's Dockerfile (with where the built
    /// image keeps the record of the node packages its build left out,
    /// `weft_core::builds::LEFT_OUT_RECORD_PATH`, when its Dockerfile
    /// writes one, `WorkerDockerfile::writes_left_out_record`), or the
    /// `weft_core::builds::HostRecipe` in it.
    pub recipe: weft_core::builds::BuildRecipe,
    /// What each node type the built program holds needs from the machine
    /// it runs on, run needs only ([`run_needs`]) for both packagings: all
    /// of them for a program compiled on that machine, and for an image
    /// only the ones its image does not install
    /// (`WorkerDockerfile::left_to_machine`).
    pub node_needs: weft_core::needs::NodeNeeds,
}

/// Of `needs`, the run-stage ones: what a program's start is held to. A
/// build-stage need matters only while a package compiles, and a package
/// that does not compile is left out (`worker_isolate`).
pub fn run_needs(mut needs: weft_core::needs::NodeNeeds) -> weft_core::needs::NodeNeeds {
    for list in needs.values_mut() {
        list.retain(|need| need.stage == weft_core::needs::NeedStage::Run);
    }
    needs.retain(|_, list| !list.is_empty());
    needs
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
/// reader of a node tree follows symlinks, the hashes name packages by
/// name and content wherever they sit, and a copy of the whole catalog per
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
        codegen::node_types_for(&self.definition, &self.catalog, weft_core::builds::NodeSet::Full)
    }

    /// What the builder base installs and exports to compile this
    /// worker: the stdlib packages' build-stage system packages and
    /// `[build.env]`.
    pub fn builder_stage(&self) -> BuildResult<worker_image::BuilderStage> {
        let node_types = self.node_types();
        let needs = codegen::node_needs(&node_types, &self.catalog)?;
        worker_image::BuilderStage::for_nodes(&self.catalog, &node_types, &needs, self.project.root.as_path())
    }

    pub fn materialize() -> BuildResult<Self> {
        let dir = tempfile::tempdir().map_err(BuildError::Io)?;
        for (rel, bytes) in crate::project::scaffold_files("standard-worker", uuid::Uuid::nil())? {
            let path = dir.path().join(rel);
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).map_err(BuildError::Io)?;
            }
            std::fs::write(path, bytes).map_err(BuildError::Io)?;
        }
        std::fs::create_dir_all(dir.path().join("nodes")).map_err(BuildError::Io)?;
        let stdlib = weft_catalog::stdlib_root().map_err(BuildError::Build)?;
        std::os::unix::fs::symlink(&stdlib, crate::project::base_catalog_dir(dir.path()))
            .map_err(BuildError::Io)?;
        let project = Project::load(dir.path())?;
        let (definition, catalog) = crate::hash::load_program(&project)
            .map_err(|e| BuildError::Build(format!("compile the stock project: {e}")))?;
        Ok(Self { _dir: dir, project, definition, catalog })
    }
}

/// The binary hash of this weft's stock worker packaged as `packaging`
/// ([`crate::hash::compute_stock_binary_hash`]): the worker every project
/// whose nodes are all this weft's standard library builds to, wherever
/// it sits (`NodeSet::Full`, the default; its hash names no project). The
/// install keeps it ready so such a project's first run compiles nothing,
/// and never reclaims it. Computed once per process and packaging kind:
/// asking walks the whole catalog.
pub fn stock_binary_hash(packaging: &weft_core::builds::WorkerPackaging) -> BuildResult<String> {
    static IMAGE: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    static HOST: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    let known = match packaging {
        weft_core::builds::WorkerPackaging::Image { .. } => &IMAGE,
        weft_core::builds::WorkerPackaging::Host { .. } => &HOST,
    };
    if let Some(hash) = known.get() {
        return Ok(hash.clone());
    }
    let hash = crate::hash::compute_stock_binary_hash(&resolve_weft_root()?, packaging)
        .map_err(|e| BuildError::Build(format!("compute the stock worker's hash: {e}")))?;
    Ok(known.get_or_init(|| hash).clone())
}

/// Validate + stage from an ALREADY-COMPILED definition. Pipeline:
///
/// 1. Check the (resolved) definition was settled after its last change
///    (`crate::unsettled`): every error in it is one of its broken parts.
///    `fs` is the view the definition was compiled through, whose reader
///    places each finding's file the way the compile placed it.
/// 2. For a program compiled on the machine (`WorkerPackaging::Host`),
///    [`stage_host_build`] at `.weft/target/worker-host/`, its shared
///    crates in the packaging's folder. For an image:
///    codegen the cargo crate at `.weft/target/build/`, emit the
///    multi-stage Dockerfile at `.weft/target/Dockerfile.worker`, and
///    stage the docker build context at `.weft/target/worker-image/`
///    with `build/` and, when not using the pre-built builder base,
///    `weft/` too; the Dockerfile is laid out so `docker build .` works.
///
/// Takes the definition + catalog the caller already produced rather than
/// recompiling from source: the caller resolves `@asset` refs into the
/// definition (deferred file markers become concrete file values) BEFORE
/// this runs, and a fresh compile-from-source here would see the raw
/// markers instead of the stored files the build resolved them to. The
/// gate runs on the resolved definition, not the raw source. `Structural` (not `Runtime`): a project
/// may build without every secret filled; runtime-rule gaps surface at run.
///
/// An image's `builder` is the shared builder-base image ref the
/// platform was set up with. When it kicks in (debian-family runtime, no
/// custom template), the build context omits `weft/` because the engine
/// workspace already lives inside the base image at `/weft/`.
pub fn build_project(
    project: &Project,
    definition: &weft_core::project::ProjectDefinition,
    catalog: &FsCatalog,
    fs: crate::CompileFs,
    packaging: &weft_core::builds::WorkerPackaging,
    node_set: weft_core::builds::NodeSet,
    fingerprints: &crate::hash::Fingerprints,
) -> BuildResult<StagedImageBuild> {
    let project_root = project.root.as_path();
    // What does not compile is already in the definition as broken parts
    // (`crate::compile_program`), and the build goes on around them: a
    // part a run needs is refused when the run is cut. Anything else is a
    // definition changed since its check.
    crate::bail_on_errors(crate::unsettled(definition, catalog, fs))?;
    // Project identity arrives with each run, never in the worker image.
    let crate_name = WORKER_CRATE_NAME;

    let referenced_nodes = codegen::node_types_for(definition, catalog, node_set);
    let node_needs = codegen::node_needs(&referenced_nodes, catalog)?;
    let binary_name = sanitize_crate_name(crate_name);
    let weft_root = resolve_weft_root()?;
    let content_hash = crate::hash::compute_binary_hash(definition, project, &weft_root, catalog, node_set, packaging, fingerprints)
        .map_err(|e| BuildError::Build(format!("compute binary hash: {e}")))?;
    let builder = match packaging {
        weft_core::builds::WorkerPackaging::Host { shared_crates } => {
            let (build_context, hold) = stage_host_build(
                &project_root.join(".weft").join("target").join("worker-host"),
                shared_crates,
                definition,
                catalog,
                node_set,
            )?;
            return Ok(StagedImageBuild {
                hold: Some(std::sync::Arc::new(hold)),
                build_context,
                content_hash,
                recipe: weft_core::builds::BuildRecipe::Host,
                node_needs: run_needs(node_needs),
            });
        }
        weft_core::builds::WorkerPackaging::Image { builder } => builder,
    };

    let crate_root = project_root.join(".weft").join("target").join("build");
    codegen::emit(
        definition,
        &crate_root,
        catalog,
        crate_name,
        node_set,
        &codegen::EmitPaths::Container { project_root: project_root.to_path_buf() },
    )?;

    let dockerfile_summary = worker_image::emit(
        &project.manifest.build.worker,
        project_root,
        catalog,
        &referenced_nodes,
        &node_needs,
        &binary_name,
        builder,
        &StockProject::materialize()?.builder_stage()?,
    )?;
    let dockerfile_path = project_root.join(".weft/target/Dockerfile.worker");
    if let Some(parent) = dockerfile_path.parent() {
        std::fs::create_dir_all(parent).map_err(BuildError::Io)?;
    }
    std::fs::write(&dockerfile_path, &dockerfile_summary.body).map_err(BuildError::Io)?;

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

    Ok(StagedImageBuild {
        hold: None,
        build_context,
        content_hash,
        recipe: weft_core::builds::BuildRecipe::Dockerfile {
            result_file: dockerfile_summary.writes_left_out_record.then(|| weft_core::builds::LEFT_OUT_RECORD_PATH.to_string()),
        },
        node_needs: dockerfile_summary.left_to_machine(run_needs(node_needs)),
    })
}

/// The folder in a host build's context holding the worker crate.
pub const HOST_CRATE_DIR: &str = "build";

/// Stage a worker compiled on the machine that runs it, at `ctx`:
///
/// ```text
/// <ctx>/
///   host-recipe.json      the `weft_core::builds::HostRecipe`
///   build/                the worker crate (Cargo.toml, src/,
///                         rust-toolchain.toml, isolate.json, left-out.json,
///                         weft-cache-gc.sh, the weft-isolate helper, and
///                         `.cargo/config.toml` when a node sets [build.env])
/// <shared>/               (the machine's, every project's)
///   weft-<key>/           the worker-linked slice of this weft's
///                         workspace, its empty workspace-hack and lock
///   pkg_<name>-<slot>/    each package crate, its sources in `source/`
/// ```
///
/// The worker crate's path deps point at the slice, never at the
/// checkout itself: the checkout's crates depend on the real
/// workspace-hack, which would compile the whole workspace's feature
/// union into every worker. Everything under `<shared>` is named by its
/// content and never rewritten, so its paths are the same for every
/// project and cargo reuses what it compiled; only the thin worker crate
/// is the project's own.
///
/// `[build.env]` reaches the compile through the worker crate's
/// `.cargo/config.toml` (`[env]`, each entry forced over the runtime's
/// own environment), read by cargo run in the crate's folder,
/// `{{catalog_path}}` expanded to the node's folder in its package
/// crate's `source/`.
pub fn stage_host_build(
    ctx: &Path,
    shared: &Path,
    definition: &weft_core::project::ProjectDefinition,
    catalog: &FsCatalog,
    node_set: weft_core::builds::NodeSet,
) -> BuildResult<(PathBuf, SharedCratesHold)> {
    stage_host_crate(ctx, shared, catalog, &|crate_root, paths| {
        Ok(HostCrate {
            package_dirs: codegen::emit(definition, crate_root, catalog, WORKER_CRATE_NAME, node_set, paths)?,
            node_types: codegen::node_types_for(definition, catalog, node_set),
            binary_name: sanitize_crate_name(WORKER_CRATE_NAME),
        })
    })
}

/// Stage a package's node-test server compiled on the machine that runs
/// it, at `ctx`: the layout of [`stage_host_build`], the worker crate
/// replaced by the package's test crate (`codegen::emit_test_crate`),
/// whose package crate is the one a worker holding that package compiles
/// (same folder under `shared`, compiled with its `node-tests` feature).
pub fn stage_host_test_build(ctx: &Path, shared: &Path, catalog: &FsCatalog, package_name: &str) -> BuildResult<(PathBuf, SharedCratesHold)> {
    stage_host_crate(ctx, shared, catalog, &|crate_root, paths| {
        let test_crate = codegen::emit_test_crate(crate_root, catalog, package_name, paths)?;
        Ok(HostCrate {
            package_dirs: BTreeMap::from([(package_name.to_string(), test_crate.package_crate)]),
            node_types: test_crate.node_types.into_iter().collect(),
            binary_name: test_crate.binary_name,
        })
    })
}

/// What emitting a host build's crate made.
struct HostCrate {
    /// Each package crate's folder, by package name.
    package_dirs: BTreeMap<String, PathBuf>,
    /// The node types the crate compiles in.
    node_types: BTreeSet<String>,
    /// The program cargo builds.
    binary_name: String,
}

/// A host build at `ctx`: the shared workspace slice, the crate `emit`
/// writes at the crate root (given the paths to emit with), its
/// `[build.env]`, and the recipe.
fn stage_host_crate(
    ctx: &Path,
    shared: &Path,
    catalog: &FsCatalog,
    emit: &dyn Fn(&Path, &codegen::EmitPaths) -> BuildResult<HostCrate>,
) -> BuildResult<(PathBuf, SharedCratesHold)> {
    let hold = SharedCratesHold::of(shared)?;
    if ctx.exists() {
        std::fs::remove_dir_all(ctx).map_err(BuildError::Io)?;
    }
    std::fs::create_dir_all(ctx).map_err(BuildError::Io)?;
    std::fs::create_dir_all(shared).map_err(BuildError::Io)?;
    let weft_root = resolve_weft_root()?;
    // The slice is named by what it holds.
    let slice_key = crate::hash::compute_worker_workspace_key(&weft_root)
        .map_err(|e| BuildError::Build(format!("the weft workspace's key: {e}")))?;
    let slice = shared.join(format!("weft-{slice_key}"));
    write_once(&slice, &|dir| stage_worker_workspace(&weft_root, dir))?;

    let crate_root = ctx.join(HOST_CRATE_DIR);
    let emitted = emit(&crate_root, &codegen::EmitPaths::Host { weft: slice.clone(), package_crates: shared.to_path_buf() })?;
    let env = worker_image::collect_build_env(catalog, &emitted.node_types, &|node_type, source_dir| {
        let package = catalog
            .package_of(node_type)
            .ok_or_else(|| BuildError::Build(format!("node '{node_type}' has no package")))?;
        let rel = source_dir.strip_prefix(&package.root).map_err(|_| {
            BuildError::Build(format!("node '{node_type}' at {} is outside its package", source_dir.display()))
        })?;
        Ok(emitted.package_dirs[&package.name].join(codegen::HOST_PACKAGE_SOURCE_DIR).join(rel).display().to_string())
    })?;
    if !env.is_empty() {
        // `force`: cargo's `[env]` otherwise loses to a variable the
        // runtime's environment already sets (a machine's own
        // `PKG_CONFIG_PATH`), where an image's `ENV` always wins.
        let entry = |value: String| {
            toml::Value::Table(toml::Table::from_iter([("value".to_string(), toml::Value::String(value)), ("force".to_string(), toml::Value::Boolean(true))]))
        };
        let mut config = toml::Table::new();
        config.insert("env".into(), toml::Value::Table(env.into_iter().map(|(k, v)| (k, entry(v))).collect()));
        let config = toml::to_string(&config).map_err(|e| BuildError::Build(format!("the build's cargo config: {e}")))?;
        std::fs::create_dir_all(crate_root.join(".cargo")).map_err(BuildError::Io)?;
        std::fs::write(crate_root.join(".cargo").join("config.toml"), config).map_err(BuildError::Io)?;
    }

    let recipe = weft_core::builds::HostRecipe {
        isolate_dir: crate_root.join(codegen::isolate_dir_name()),
        isolate_candidates: crate_root.join(codegen::ISOLATE_CANDIDATES_FILE),
        left_out_record: crate_root.join(codegen::LEFT_OUT_RECORD_FILE),
        // The lock every worker on this weft starts from, the one an
        // image build without a base seeds from (`/weft/Cargo.lock`).
        seed_lock: slice.join("Cargo.lock"),
        binary_name: emitted.binary_name,
        crate_dir: crate_root,
    };
    let json = serde_json::to_vec_pretty(&recipe).map_err(|e| BuildError::Build(format!("the host recipe: {e}")))?;
    std::fs::write(ctx.join(weft_core::builds::HOST_RECIPE_FILE), json).map_err(BuildError::Io)?;
    Ok((ctx.to_path_buf(), hold))
}

/// A build's claim on the crates it staged in the folder every host build
/// on the machine shares, from before it stages them until it is compiled
/// (`weft_core::builds::shared_crates_lock`).
#[derive(Debug)]
pub struct SharedCratesHold {
    _lock: std::fs::File,
}

impl SharedCratesHold {
    fn of(shared: &Path) -> BuildResult<Self> {
        Ok(Self { _lock: weft_core::file_lock::hold_shared(&weft_core::builds::shared_crates_lock(shared)).map_err(BuildError::Io)? })
    }
}

/// Make `dest` by `write` once and never again: a folder named by its
/// content is whole as soon as it is there, and another build may be
/// reading it. `write` fills a fresh folder beside it, which is then
/// renamed into place, so `dest` is either whole or absent; when another
/// writer got there first, its copy (the same content) stays.
pub(crate) fn write_once(dest: &Path, write: &dyn Fn(&Path) -> BuildResult<()>) -> BuildResult<()> {
    if dest.is_dir() {
        return Ok(());
    }
    let parent = dest.parent().ok_or_else(|| BuildError::Build(format!("{} has no parent folder", dest.display())))?;
    std::fs::create_dir_all(parent).map_err(BuildError::Io)?;
    let name = dest.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default();
    let temp = parent.join(format!(".{name}.{}", uuid::Uuid::new_v4()));
    let written = write(&temp).and_then(|()| match std::fs::rename(&temp, dest) {
        Ok(()) => Ok(()),
        Err(_) if dest.is_dir() => Ok(()),
        Err(e) => Err(BuildError::Io(e)),
    });
    if temp.exists() {
        let _ = std::fs::remove_dir_all(&temp);
    }
    written
}

/// Copy a package's sources to `dest` as a build compiles them: what
/// `weft_catalog::is_left_out_of_package_build` leaves out stays out (the
/// package hash skips the same files), and each file carries the date
/// its content gives it ([`package_source_mtime`]).
pub(crate) fn copy_package_source(root: &Path, dest: &Path) -> BuildResult<()> {
    copy_dir_dated(root, dest, &weft_catalog::is_left_out_of_package_build, FileDates::FromContent)
}

/// Stage the docker build context under
/// `.weft/target/worker-image/`. Layout:
///
/// ```text
/// worker-image/
///   Dockerfile           (copy of Dockerfile.worker)
///   build/               (the generated cargo crate)
///   weft/                (the weft workspace: crates, Cargo.toml, Cargo.lock)
///   project-nodes/       (every node's source, at its path in the project)
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
/// references, each copied to its path relative to the project root
/// (`nodes/...`, or `src/...` for a node beside the code) so the
/// `#[path]` shims resolve. Discovery already walked both node roots and
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
) -> BuildResult<PathBuf> {
    let ctx = ctx.to_path_buf();
    if ctx.exists() {
        std::fs::remove_dir_all(&ctx).map_err(BuildError::Io)?;
    }
    std::fs::create_dir_all(&ctx).map_err(BuildError::Io)?;

    std::fs::copy(dockerfile_path, ctx.join("Dockerfile")).map_err(BuildError::Io)?;

    // `build/` = generated cargo crate. Copy target excluded; it
    // doesn't exist on the host anymore (no host cargo build), but
    // belt-and-suspenders.
    copy_dir_filtered(crate_root, &ctx.join("build"), &|name| name == "target")?;

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
) -> BuildResult<()> {
    for root in catalog.package_roots_for(referenced_nodes) {
        let rel = root.strip_prefix(project_root).map_err(|_| {
            BuildError::Build(format!(
                "package root {} is not under the project root {}",
                root.display(),
                project_root.display()
            ))
        })?;
        // The package-build exclude (same set the package hash walks
        // over, `hash::hash_package_root`), so staging and hashing agree
        // on a node's byte-content: a file the build copies but the hash
        // skips (or vice versa) is how a stale worker image gets served.
        copy_package_source(&root, &dest.join(rel))?;
    }
    Ok(())
}

/// The modification time a staged package file with `content` carries,
/// whatever the file had on disk: a date in 2020 to 2024, to the
/// nanosecond, read off the content's sha256.
///
/// Cargo reuses a compiled crate only while none of its sources is newer
/// than the compile, and a package compiles in a crate whose directory is
/// named by a digest of all of its files (`codegen::write_package_crates`),
/// so within one such crate the content never changes and a date says
/// nothing. Real dates said something wrong: the builder base is built
/// from one checkout and a cloud build stages the standard library from
/// another (the runtime image's copy), and a fresh checkout dates every
/// file at the moment it was made, so every precompiled package looked
/// edited and every cloud build compiled the whole library again. A date
/// read off the content is the same on both sides for the same file, so
/// an unchanged package is reused wherever it was compiled. It is never
/// one fixed date: Docker sends a build context's files again only when
/// their size or date changed since the last build of that context, so a
/// fixed date kept an edit that left a file's size alone (a letter's case)
/// out of the build, which then compiled the old file. The seconds and
/// the nanoseconds come from separate parts of the digest, so two
/// contents of one size share a date about once in 10^17.
pub(crate) fn package_source_mtime(content: &[u8]) -> std::time::SystemTime {
    use sha2::Digest;
    // 2020-01-01, and five years past it: always in the past, so a cached
    // compile is never older than its sources.
    const START: u64 = 1_577_836_800;
    const SPAN: u64 = 5 * 365 * 24 * 3600;
    let digest = sha2::Sha256::digest(content);
    let word = |at: usize| u64::from_be_bytes(digest[at..at + 8].try_into().expect("a sha256 is 32 bytes"));
    let secs = word(0) % SPAN;
    // The low 30 bits of another word, folded under a second.
    let nanos = (word(8) & ((1 << 30) - 1)) % 1_000_000_000;
    std::time::UNIX_EPOCH + std::time::Duration::new(START + secs, nanos as u32)
}

/// The tag of a package's node-test server, `weft-node-tests:<test_hash>`:
/// the program a live test runs on the machine
/// (`weft_platform_local::images::Program::NodeTestServer`).
pub fn node_test_image_tag(test_hash: &str) -> String {
    format!("{}:{test_hash}", weft_core::builds::NODE_TEST_REPO)
}

/// Emit + stage a package's node-test server: a program compiled on the
/// machine that runs it ([`stage_host_test_build`]), its shared crates in
/// `shared_crates` beside the workers', so a live test of the package
/// compiles only what the workers have not. Needs only the catalog: no
/// project parse, no validation of other packages.
pub fn build_test_artifact(project: &Project, catalog: &FsCatalog, package_name: &str, shared_crates: &Path) -> BuildResult<StagedImageBuild> {
    // Every staged path is per-package: a project's packages build test
    // servers side by side, so a shared path would have one package's
    // staging clobber another's mid-build.
    let stem = sanitize_crate_name(package_name);
    let (build_context, hold) = stage_host_test_build(
        &project.root.join(".weft").join("target").join("test-host").join(&stem),
        shared_crates,
        catalog,
        package_name,
    )?;
    Ok(StagedImageBuild {
        hold: Some(std::sync::Arc::new(hold)),
        build_context,
        content_hash: node_test_content_hash(catalog, package_name)?,
        recipe: weft_core::builds::BuildRecipe::Host,
        node_needs: package_run_needs(catalog, package_name)?,
    })
}

/// What package `package_name`'s node types need from the machine its test
/// server runs on while it runs: what a live test is held to before it
/// starts.
pub fn package_run_needs(catalog: &FsCatalog, package_name: &str) -> BuildResult<weft_core::needs::NodeNeeds> {
    let Some(package) = catalog.packages().find(|p| p.name == package_name) else {
        return Err(BuildError::Build(format!("no package named '{package_name}' in the catalog")));
    };
    Ok(run_needs(codegen::node_needs(&package.node_types.iter().cloned().collect(), catalog)?))
}

/// The content hash naming a package's node-test server
/// ([`crate::hash::compute_node_test_hash`]): its tag
/// (`build_test_artifact`), and what `weft node-test-hash` prints.
/// Covers the package's own files, the full catalog type registry (a
/// sibling's type edit changes this package's test binary), and the
/// workspace and toolchain it compiles with.
pub fn node_test_content_hash(catalog: &FsCatalog, package_name: &str) -> BuildResult<crate::hash::SourceHash> {
    let weft_root = resolve_weft_root()?;
    let package_root = package_root(catalog, package_name)?;
    crate::hash::compute_node_test_hash(&package_root, &weft_root, catalog)
        .map_err(|e| BuildError::Build(format!("compute node-test hash: {e}")))
}

/// Content hash of everything that can change a package's node-test
/// OUTCOME: the package's own sources (every member's mod.rs /
/// metadata.json / tests.rs plus the package's shared files) and the
/// catalog's type registry (a sibling's type edit changes how this
/// package's ports resolve). The staleness rule for the live-pass
/// cache and the test-listing cache, via `weft node-test-hash`.
/// Deliberately narrower than [`node_test_content_hash`] (the test
/// server's tag): live tests spend real provider money, so an engine
/// edit (which rightly recompiles the test server) does not by itself
/// invalidate a recorded live pass.
pub fn node_test_cache_hash(catalog: &FsCatalog, package_name: &str) -> BuildResult<crate::hash::SourceHash> {
    let package_root = package_root(catalog, package_name)?;
    crate::hash::compute_node_test_outcome_hash(&package_root, catalog)
        .map_err(|e| BuildError::Build(format!("compute node-test cache hash: {e}")))
}

/// The filesystem root of `package_name`, or a loud error naming the
/// project's nodes/ as the place it was looked for.
fn package_root(catalog: &FsCatalog, package_name: &str) -> BuildResult<PathBuf> {
    catalog
        .packages()
        .find(|p| p.name == package_name)
        .map(|p| p.root.clone())
        .ok_or_else(|| {
            BuildError::Build(format!("no package named '{package_name}' in this project (nodes/ or src/)"))
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
///
/// A crate also leaves out what its own manifest lists under
/// `[package.metadata.weft] left_out_of_workers`: data only a feature no
/// worker turns on reads (weft-task-store's `migrations/`, the database's
/// history, so a migration names no new worker and asks no project to
/// resync). Left out of the staged copy as well as the hash, so a worker
/// build that did reach for it fails on the missing file instead of
/// compiling bytes its name does not cover.
pub fn worker_crate_entries(crate_dir: &Path) -> BuildResult<Vec<(String, PathBuf)>> {
    let mut left_out = left_out_of_workers(crate_dir)?;
    let mut out = Vec::new();
    for entry in std::fs::read_dir(crate_dir).map_err(BuildError::Io)? {
        let entry = entry.map_err(BuildError::Io)?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if name == "tests" || weft_catalog::is_node_tree_excluded(&name) || left_out.remove(&name) {
            continue;
        }
        // Symlinks are skipped in the workspace-crate slice, by the
        // stager and the hasher alike (this enumerator is both), so
        // staged and hashed bytes stay equal. Unlike a user's node
        // tree, weft's own crates never legitimately hold one.
        if entry.file_type().map_err(BuildError::Io)?.is_symlink() {
            continue;
        }
        out.push((name, entry.path()));
    }
    // A listed entry the crate no longer holds is a stale declaration:
    // refuse it, so the list keeps saying what it actually leaves out.
    if let Some(stale) = left_out.into_iter().next() {
        return Err(BuildError::Build(format!(
            "{}: [package.metadata.weft] left_out_of_workers names '{stale}', which is not in the crate; \
             remove it from the list",
            crate_dir.join("Cargo.toml").display()
        )));
    }
    out.sort();
    Ok(out)
}

/// The top-level entries a crate's manifest leaves out of a worker's copy
/// (`[package.metadata.weft] left_out_of_workers`, see
/// [`worker_crate_entries`]). Empty when it lists none.
fn left_out_of_workers(crate_dir: &Path) -> BuildResult<BTreeSet<String>> {
    #[derive(Deserialize)]
    struct Manifest {
        package: Package,
    }
    #[derive(Deserialize)]
    struct Package {
        #[serde(default)]
        metadata: Metadata,
    }
    #[derive(Deserialize, Default)]
    struct Metadata {
        #[serde(default)]
        weft: Weft,
    }
    #[derive(Deserialize, Default)]
    #[serde(deny_unknown_fields)]
    struct Weft {
        #[serde(default)]
        left_out_of_workers: BTreeSet<String>,
    }
    let path = crate_dir.join("Cargo.toml");
    let raw = std::fs::read_to_string(&path).map_err(|e| BuildError::Build(format!("read {}: {e}", path.display())))?;
    let manifest: Manifest =
        toml::from_str(&raw).map_err(|e| BuildError::Build(format!("parse {}: {e}", path.display())))?;
    Ok(manifest.package.metadata.weft.left_out_of_workers)
}

/// Stage the worker-linked slice of the weft workspace at `dest`:
/// `crates/<closure>` (exactly `codegen::worker_workspace_crates`, the only
/// workspace crates a worker build links), a workspace manifest whose members
/// are rewritten to that closure, and the workspace `Cargo.lock`. Everything
/// else in the workspace (CLI, dispatcher, tests) is deliberately absent, so
/// it can neither bloat a build context nor invalidate a worker build.
///
/// The staged set must match what `hash::hash_worker_workspace` hashes
/// (closure crates + manifests): a file staged-but-not-hashed (or vice versa)
/// is a stale-worker hole. Crate copies preserve mtimes (see
/// `copy_dir_filtered`) so cargo's fingerprint sees unchanged files as
/// unchanged; the generated manifest + copied lock get the
/// SOURCE files' mtimes stamped for the same reason.
pub fn stage_worker_workspace(weft_root: &Path, dest: &Path) -> BuildResult<()> {
    std::fs::create_dir_all(dest).map_err(BuildError::Io)?;
    let closure = codegen::worker_workspace_crates(weft_root)?;
    for name in &closure {
        let crate_dest = dest.join("crates").join(name);
        std::fs::create_dir_all(&crate_dest).map_err(BuildError::Io)?;
        for (entry_name, entry_path) in
            worker_crate_entries(&weft_root.join("crates").join(name))?
        {
            let to = crate_dest.join(&entry_name);
            if entry_path.is_dir() {
                copy_dir_filtered(&entry_path, &to, &weft_catalog::is_node_tree_excluded)?;
            } else if entry_path.is_file() {
                std::fs::copy(&entry_path, &to).map_err(BuildError::Io)?;
                mirror_mtime(&entry_path, &to);
            }
        }
    }
    stage_empty_workspace_hack(dest)?;
    write_scoped_workspace_manifest(weft_root, dest, &closure)?;
    let lock_src = weft_root.join("Cargo.lock");
    let lock_dst = dest.join("Cargo.lock");
    std::fs::copy(&lock_src, &lock_dst).map_err(BuildError::Io)?;
    mirror_mtime(&lock_src, &lock_dst);
    Ok(())
}

/// Write the empty stand-in for [`codegen::WORKSPACE_HACK_CRATE`] at
/// `dest/crates/`: same name and version, no dependencies, so the closure
/// crates' path dependency on it resolves and the worker compiles none of
/// the workspace's feature union. Each file is dated from its content
/// (`package_source_mtime`): a fresh wall-clock date on every staging would
/// make cargo rebuild it, and with it every engine crate above it.
fn stage_empty_workspace_hack(dest: &Path) -> BuildResult<()> {
    let dir = dest.join("crates").join(codegen::WORKSPACE_HACK_CRATE);
    std::fs::create_dir_all(dir.join("src")).map_err(BuildError::Io)?;
    let manifest = format!(
        "[package]\nname = \"{}\"\nversion = \"0.1.0\"\nedition = \"2021\"\npublish = false\n",
        codegen::WORKSPACE_HACK_CRATE
    );
    for (path, content) in [(dir.join("Cargo.toml"), manifest.as_str()), (dir.join("src").join("lib.rs"), "")] {
        std::fs::write(&path, content).map_err(BuildError::Io)?;
        stamp_mtime(&path, package_source_mtime(content.as_bytes()));
    }
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
) -> BuildResult<()> {
    let src = weft_root.join("Cargo.toml");
    let raw = std::fs::read_to_string(&src).map_err(BuildError::Io)?;
    let mut manifest: toml::Value = toml::from_str(&raw)
        .map_err(|e| BuildError::Build(format!("parse {}: {e}", src.display())))?;
    let workspace = manifest
        .get_mut("workspace")
        .and_then(|w| w.as_table_mut())
        .ok_or_else(|| {
            BuildError::Build(format!("{} has no [workspace] table", src.display()))
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
        .map_err(|e| BuildError::Build(format!("serialize scoped workspace manifest: {e}")))?;
    let dst = dest.join("Cargo.toml");
    std::fs::write(&dst, body).map_err(BuildError::Io)?;
    mirror_mtime(&src, &dst);
    Ok(())
}

/// Stamp `src`'s mtime onto `dst`. Cargo's fingerprint short-circuits a
/// clean crate only when every source
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
            "could not set the file's modification time; the build will \
             treat it as changed and rebuild its crate"
        );
    }
}

/// Stage the builder-base docker build CONTEXT into
/// `<weft_root>/.weft-base-context/` (see [`stage_builder_base_context_at`]).
pub fn stage_builder_base_context(weft_root: &Path) -> BuildResult<PathBuf> {
    stage_builder_base_context_at(weft_root, &weft_root.join(worker_image::BASE_CONTEXT_DIR))
}

/// Stage the builder-base docker build CONTEXT at `ctx`, holding exactly what
/// the rendered Dockerfile COPYs:
///
/// ```text
/// <ctx>/
///   Dockerfile           (worker-builder-base.Dockerfile with its tokens
///                         substituted; build with -f THIS file)
///   worker-builder-base-split.sh
///                        (copied verbatim; the Dockerfile runs it to
///                         split the compiled artifacts into its two
///                         image layers)
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
/// the package crates pull in dependencies of their own (`tokio-postgres`, `reqwest`,
/// `tungstenite`, ...) and change how cargo unifies the features of the
/// shared ones, so a base that had compiled only the engine's dependencies
/// left every first build on a host recompiling the engine, most of the
/// dependency tree and every package crate (measured at 1m07 on a warm
/// machine). Compiling the same crate a stock project's worker is built
/// from, at the same paths (`/work`, `/weft/project-nodes`), makes every
/// package crate an untouched project links fingerprint-fresh in the seeded
/// compile cache; a project compiles only the packages it edited or added
/// and its thin top crate.
///
/// The base is the cloud's: `weft build-images` builds it (on CI, or on a
/// cloud install's runner), and a cloud install's worker images FROM it.
///
/// The Dockerfile is RENDERED, not copied: `{{target_cache_key}}` becomes
/// [`builder_base_cache_key`], so a toolchain change starts a fresh compile
/// cache (the sweep after the build keeps the cache, and so the image, to
/// what that build used, whatever the lock did); `{{install_build_system_packages}}` and
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
pub fn stage_builder_base_context_at(weft_root: &Path, ctx: &Path) -> BuildResult<PathBuf> {
    if ctx.exists() {
        std::fs::remove_dir_all(ctx).map_err(BuildError::Io)?;
    }
    stage_worker_workspace(weft_root, ctx)?;
    let toolchain_src = weft_root.join("rust-toolchain.toml");
    let toolchain_dst = ctx.join("rust-toolchain.toml");
    std::fs::copy(&toolchain_src, &toolchain_dst).map_err(BuildError::Io)?;
    mirror_mtime(&toolchain_src, &toolchain_dst);

    let stock = StockProject::materialize()?;
    let referenced = stock.node_types();
    codegen::emit(
        &stock.definition,
        &ctx.join(worker_image::WARMUP_CRATE_DIR),
        &stock.catalog,
        WORKER_CRATE_NAME,
        weft_core::builds::NodeSet::Full,
        &codegen::EmitPaths::Container { project_root: stock.project.root.clone() },
    )?;
    stage_project_nodes(stock.project.root.as_path(), &stock.catalog, &referenced, &ctx.join("project-nodes"))?;

    let (install_packages, env_lines) = stock.builder_stage()?.render();
    let template_path = weft_root.join(crate::hash::BUILDER_BASE_DOCKERFILE);
    let mut rendered = std::fs::read_to_string(&template_path).map_err(BuildError::Io)?;
    for (token, value) in [
        ("{{target_cache_key}}", builder_base_cache_key(weft_root)?),
        ("{{worker_rustflags}}", weft_core::builds::WORKER_RUSTFLAGS.to_string()),
        ("{{install_build_system_packages}}", install_packages),
        ("{{build_env_lines}}", env_lines),
    ] {
        if !rendered.contains(token) {
            return Err(BuildError::Build(format!(
                "{} lost its {token} token; the base would stop keying its compile cache \
                 on the toolchain, or stop installing what the stock worker compiles with",
                template_path.display()
            )));
        }
        rendered = rendered.replace(token, &value);
    }
    std::fs::write(ctx.join("Dockerfile"), rendered).map_err(BuildError::Io)?;

    // The artifact split the Dockerfile runs. Copied rather than
    // rendered (it takes its paths as arguments), and covered by the
    // base hash next to the Dockerfile, so editing it mints a new tag.
    let split_name = std::path::Path::new(crate::hash::BUILDER_BASE_SPLIT_SCRIPT)
        .file_name()
        .expect("the split script constant names a file");
    std::fs::copy(
        weft_root.join(crate::hash::BUILDER_BASE_SPLIT_SCRIPT),
        ctx.join(split_name),
    )
    .map_err(BuildError::Io)?;
    Ok(ctx.to_path_buf())
}

/// What the id of the builder base's compile cache mount starts with,
/// before [`builder_base_cache_key`] (or, on one an older weft left, an
/// [`install_cache_key`]). The runtime image's has its own prefix
/// (`weft-cargo-target-runtime-`, kept by the CLI that builds it).
// SYNC: BUILDER_BASE_CACHE_MOUNT_ID_PREFIX <-> deploy/docker/worker-builder-base.Dockerfile (weft-builder-base-target-{{target_cache_key}})
pub const BUILDER_BASE_CACHE_MOUNT_ID_PREFIX: &str = "weft-builder-base-target-";

/// A short hash of `rust-toolchain.toml`: what a compile cache of weft's
/// own crates that no install owns is keyed by.
fn toolchain_key(weft_root: &Path) -> BuildResult<String> {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(std::fs::read(weft_root.join("rust-toolchain.toml")).map_err(BuildError::Io)?);
    Ok(hasher.finalize().iter().take(8).map(|b| format!("{b:02x}")).collect())
}

/// The key of the builder base's compile cache: the toolchain alone, whose
/// change makes everything cached dead weight. Only `weft build-images`
/// builds the base, from one checkout at a time, so no install owns it.
/// NOT the lock: a dependency bump keeps the cache and recompiles only
/// what it moved, and the sweep after the build
/// (`worker_image::CACHE_GC_SCRIPT`, `this-build`) drops every unit that
/// build did not use. Source edits keep the key too, so they stay
/// incremental.
pub fn builder_base_cache_key(weft_root: &Path) -> BuildResult<String> {
    toolchain_key(weft_root)
}

/// The key of the runtime image's compile cache for `install`: its label
/// (`default`, or its name), then the toolchain's key, `<install>-<hash>`.
///
/// The install, because cargo tells a crate's sources changed by their
/// modification times alone, and every checkout's crates sit at the same
/// path inside the build: two branches' installs sharing one cache each
/// find the other's newer artifacts fresh for their own older sources, and
/// an image tagged with one branch's content carries the other's code.
/// Each branch has an install of its own, built from its one checkout, so
/// the install is the cache's owner too: `weft daemon remove` drops its
/// caches, and `weft clean --images --all` those of installs no longer on
/// the machine ([`install_of_cache_key`]). The toolchain and the lock as
/// [`builder_base_cache_key`] says.
pub fn install_cache_key(weft_root: &Path, install: &weft_core::infra::Install) -> BuildResult<String> {
    Ok(format!("{}-{}", install.label_value(), toolchain_key(weft_root)?))
}

/// The install (its label) a key of [`install_cache_key`] is for. `None`
/// for a key with no install in it: a [`builder_base_cache_key`], or one
/// a weft from before each install had caches of its own made.
pub fn install_of_cache_key(key: &str) -> Option<&str> {
    // A label holds no `-` (`Install::named`), and the hash none either.
    key.split_once('-').map(|(install, _)| install)
}

/// Recursive directory copy with a simple exclude list matched on
/// the immediate entry name. Skips symlinks to avoid infinite
/// recursion and unexpected escapes from the staged context.
///
/// Preserves mtime on every copied file. A staged build context feeds
/// straight into docker, which preserves the host mtime on `COPY`, and
/// cargo's fingerprint short-circuits a clean crate only while every
/// source file's mtime is older than the crate's rlib in the target dir:
/// with wall-clock mtimes every crate staged this way (the workspace
/// slice a worker links) would look dirty on every build. A package's
/// staged files are dated by their content instead
/// ([`package_source_mtime`], through [`copy_dir_dated`]).
/// `exclude` names the entries left out, at any depth.
pub fn copy_dir_filtered(src: &Path, dst: &Path, exclude: &dyn Fn(&str) -> bool) -> BuildResult<()> {
    copy_dir_dated(src, dst, exclude, FileDates::Mirrored)
}

/// What date a copied file carries.
#[derive(Clone, Copy)]
enum FileDates {
    /// The source file's.
    Mirrored,
    /// The one its content gives it ([`package_source_mtime`]), read off
    /// the bytes as they are copied.
    FromContent,
}

/// [`copy_dir_filtered`], each file dated as `dates` says.
fn copy_dir_dated(src: &Path, dst: &Path, exclude: &dyn Fn(&str) -> bool, dates: FileDates) -> BuildResult<()> {
    copy_dir_filtered_inner(src, dst, exclude, dates, &mut Default::default())
}

/// Symlinks are followed and their TARGET bytes copied (`fs::copy`
/// reads through the link), so a symlinked `nodes/base_catalog` stages
/// as real files inside the build context; a cycle fails loudly via
/// the descent chain.
fn copy_dir_filtered_inner(
    src: &Path,
    dst: &Path,
    exclude: &dyn Fn(&str) -> bool,
    dates: FileDates,
    chain: &mut Vec<PathBuf>,
) -> BuildResult<()> {
    let canon = weft_catalog::guard_node_tree_cycle(src, chain).map_err(BuildError::Io)?;
    chain.push(canon);
    let result = copy_dir_filtered_entries(src, dst, exclude, dates, chain);
    chain.pop();
    result
}

fn copy_dir_filtered_entries(
    src: &Path,
    dst: &Path,
    exclude: &dyn Fn(&str) -> bool,
    dates: FileDates,
    chain: &mut Vec<PathBuf>,
) -> BuildResult<()> {
    std::fs::create_dir_all(dst).map_err(BuildError::Io)?;
    for entry in std::fs::read_dir(src).map_err(BuildError::Io)? {
        let entry = entry.map_err(BuildError::Io)?;
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if exclude(&name_str) {
            continue;
        }
        let from = entry.path();
        let to = dst.join(&name);
        let kind = weft_catalog::node_tree_entry_kind(&from).map_err(BuildError::Io)?;
        if kind == weft_catalog::NodeTreeEntryKind::Dir {
            copy_dir_filtered_inner(&from, &to, exclude, dates, chain)?;
        } else {
            match dates {
                FileDates::Mirrored => {
                    std::fs::copy(&from, &to).map_err(BuildError::Io)?;
                    mirror_mtime(&from, &to);
                }
                FileDates::FromContent => {
                    // Read once, for the copy and its date alike; the
                    // mode goes along as `fs::copy` would take it.
                    let content = std::fs::read(&from).map_err(BuildError::Io)?;
                    std::fs::write(&to, &content).map_err(BuildError::Io)?;
                    let mode = std::fs::metadata(&from).map_err(BuildError::Io)?.permissions();
                    std::fs::set_permissions(&to, mode).map_err(BuildError::Io)?;
                    stamp_mtime(&to, package_source_mtime(&content));
                }
            }
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
pub fn resolve_weft_root() -> BuildResult<PathBuf> {
    weft_catalog::weft_repo_root().map_err(BuildError::Build)
}

/// The tag naming a worker, `weft-worker:<binary_hash>`, the FULL binary
/// hash so it is collision-free: what the dispatcher asks its platform's
/// image builder for the ref of (an image in a cloud's registry, a
/// program on a local install). No project id: a tag is purely a function
/// of what was compiled, so identical builds across projects and tenants
/// resolve to ONE worker (the same shape infra images use).
pub fn worker_image_tag(binary_hash: &str) -> String {
    format!("{}:{binary_hash}", weft_core::builds::WORKER_REPO)
}

/// Sanitize a project/package name to a valid cargo crate + binary
/// name. Used by codegen and every build so they agree on what binary
/// a build produces. Collision-free by construction: a
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
) -> BuildResult<TestWorkspace> {
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
        .map_err(|e| BuildError::Build(format!("emit test crate for '{package}': {e}")))?;
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
/// copy of it (that is where ten copies of one database driver came from,
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
) -> BuildResult<BTreeMap<String, PathBuf>> {
    if workspace.crates.is_empty() {
        return Ok(BTreeMap::new());
    }
    eprintln!("building tests for {} package(s)...", workspace.crates.len());
    let out = std::process::Command::new("cargo")
        .args(["build", "--workspace", "--message-format=json-render-diagnostics"])
        .current_dir(&workspace.root)
        .env("CARGO_TARGET_DIR", &workspace.target_dir)
        .env("RUSTFLAGS", weft_core::builds::WORKER_RUSTFLAGS)
        .stderr(std::process::Stdio::inherit())
        .output()
        .map_err(|e| {
            BuildError::Build(format!(
                "run cargo (is a Rust toolchain installed? local test runs compile \
                 on the host): {e}"
            ))
        })?;
    if !out.status.success() {
        return Err(BuildError::Build(
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
            return Err(BuildError::Build(format!(
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
/// `nodes/` directory and beside its code under `src/`
/// (`crate::project::node_roots`). That is the single source of truth (the stdlib
/// is cloned in at `weft new`), so the project is self-contained and
/// nothing reaches into the weft installation at build time. A node or
/// package that fails to load is left out and recorded
/// (`FsCatalog::problems`); a program that names it is told why.
pub fn build_project_catalog(project_root: &Path) -> BuildResult<FsCatalog> {
    let roots = crate::project::node_roots(project_root);
    FsCatalog::discover_roots(&roots.iter().map(|r| r.as_path()).collect::<Vec<_>>())
        .map_err(|e| BuildError::Enrich(format!("catalog: {e}")))
}

#[cfg(test)]
mod tests {
    use super::sanitize_crate_name;

    /// Two installs on the same toolchain never share a compile cache, and
    /// each key says whose it is; an install keeps its cache across source
    /// edits and dependency bumps, and moves to a fresh one when its
    /// toolchain does.
    #[test]
    fn each_install_compiles_in_a_cache_of_its_own() {
        use weft_core::infra::Install;
        let checkout = tempfile::tempdir().unwrap();
        std::fs::write(checkout.path().join("Cargo.lock"), "lock").unwrap();
        std::fs::write(checkout.path().join("rust-toolchain.toml"), "[toolchain]\nchannel = \"1.94.0\"\n").unwrap();
        let key = |install: &Install| super::install_cache_key(checkout.path(), install).unwrap();
        let (deploy, main) = (Install::named("deploy").unwrap(), Install::default_install());
        assert_ne!(key(&deploy), key(&main));
        assert_eq!(super::install_of_cache_key(&key(&deploy)), Some("deploy"));
        assert_eq!(super::install_of_cache_key(&key(&main)), Some("default"));
        assert_eq!(super::install_of_cache_key("0123456789abcdef"), None, "a key no install owns");
        let base = super::builder_base_cache_key(checkout.path()).unwrap();
        assert_eq!(super::install_of_cache_key(&base), None, "the builder base's cache is no install's");
        assert!(key(&deploy).ends_with(&format!("-{base}")), "both follow the toolchain");
        let before = key(&deploy);
        std::fs::write(checkout.path().join("lib.rs"), "fn edited() {}").unwrap();
        assert_eq!(key(&deploy), before, "a source edit keeps the cache");
        std::fs::write(checkout.path().join("Cargo.lock"), "bumped").unwrap();
        assert_eq!(key(&deploy), before, "a dependency bump keeps the cache too");
        std::fs::write(checkout.path().join("rust-toolchain.toml"), "[toolchain]\nchannel = \"1.95.0\"\n").unwrap();
        assert_ne!(key(&deploy), before, "a toolchain change starts a fresh one");
    }

    /// A staged file's date is read off its content: the same everywhere
    /// for the same file, and never the same across an edit, even one that
    /// keeps the file's size (Docker resends a context's file only when its
    /// size or date changed).
    #[test]
    fn a_package_files_date_is_its_contents() {
        let date = super::package_source_mtime;
        assert_eq!(date(b"FileHandle::key"), date(b"FileHandle::key"), "the same content, the same date, on every checkout");
        assert_ne!(date(b"FileHandle::Key"), date(b"FileHandle::key"), "an edit of the same size changes the date");
        let nanos = |content: &[u8]| date(content).duration_since(std::time::UNIX_EPOCH).unwrap().subsec_nanos();
        assert_ne!((nanos(b"FileHandle::Key"), nanos(b"FileHandle::key")), (0, 0), "dated to the nanosecond");
        let start = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_577_836_800);
        for content in [&b""[..], b"x", b"fn main() {}"] {
            let at = date(content);
            assert!(at >= start && at < std::time::SystemTime::now(), "a date in the past");
        }
    }

    /// A worker build gets an empty `weft-workspace-hack`: the real one turns
    /// on every feature the CLI and the dispatcher use, which a worker would
    /// then compile. The engine crates still name it, so the stand-in must
    /// be there, dated from its content so cargo keeps reusing what sits
    /// above it.
    #[test]
    fn a_worker_build_gets_an_empty_workspace_hack() {
        let weft_root = super::resolve_weft_root().expect("resolve weft root");
        let dir = tempfile::tempdir().unwrap();
        super::stage_worker_workspace(&weft_root, dir.path()).expect("stage");
        let hack = dir.path().join("crates").join(crate::codegen::WORKSPACE_HACK_CRATE);
        let manifest = std::fs::read_to_string(hack.join("Cargo.toml")).unwrap();
        assert!(!manifest.contains("[dependencies]"), "{manifest}");
        assert!(manifest.contains("name = \"weft-workspace-hack\""), "{manifest}");
        assert_eq!(std::fs::read_to_string(hack.join("src/lib.rs")).unwrap(), "");
        let dated = std::fs::metadata(hack.join("src/lib.rs")).unwrap().modified().unwrap();
        assert_eq!(dated, super::package_source_mtime(b""));
    }

    /// A host build writes its recipe beside the worker crate, every path
    /// in it there, and puts each package crate (its sources copied in)
    /// and the weft workspace slice into the shared folder: two projects
    /// with the same package at different paths use one package crate,
    /// which links the slice and its empty workspace-hack, never the
    /// checkout's crates.
    #[test]
    fn a_host_build_shares_its_package_crates_and_writes_its_recipe() {
        let temp = tempfile::tempdir().unwrap();
        let shared = temp.path().join("shared");
        let mut manifests = Vec::new();
        for at in ["one", "deep/two"] {
            let project = temp.path().join(at);
            let catalog = crate::codegen::tests::catalog_with_needs(&project);
            let definition: weft_core::project::ProjectDefinition = serde_json::from_value(serde_json::json!({
                "id": uuid::Uuid::nil(), "nodes": [{"id": "greet", "nodeType": "Run", "config": {}, "position": {"x": 0, "y": 0}}], "edges": []
            }))
            .unwrap();
            let (ctx, _hold) = super::stage_host_build(&project.join("ctx"), &shared, &definition, &catalog, weft_core::builds::NodeSet::Referenced).expect("stage");
            let recipe: weft_core::builds::HostRecipe =
                serde_json::from_slice(&std::fs::read(ctx.join(weft_core::builds::HOST_RECIPE_FILE)).unwrap()).unwrap();
            assert_eq!(recipe.crate_dir, ctx.join(super::HOST_CRATE_DIR));
            for file in [&recipe.isolate_candidates, &recipe.left_out_record, &recipe.seed_lock] {
                assert!(file.is_file(), "{}", file.display());
            }
            assert!(recipe.isolate_dir.join("Cargo.toml").is_file());
            assert!(recipe.seed_lock.starts_with(&shared));
            assert_eq!(recipe.binary_name, super::sanitize_crate_name(super::WORKER_CRATE_NAME));
            manifests.push(std::fs::read_to_string(recipe.crate_dir.join("Cargo.toml")).unwrap());
        }
        let package_dep = |manifest: &str| manifest.lines().find(|l| l.starts_with("pkg_run") && l.contains("path =")).expect("the package dep").to_string();
        assert_eq!(package_dep(&manifests[0]), package_dep(&manifests[1]), "one package crate for both");
        let pkg = std::fs::read_dir(&shared).unwrap().map(|e| e.unwrap().path()).find(|p| p.file_name().unwrap().to_string_lossy().starts_with("pkg_run-")).unwrap();
        assert!(package_dep(&manifests[0]).contains(&pkg.display().to_string()));
        assert!(pkg.join("source/mod.rs").is_file() && pkg.join("source/deps.toml").is_file());
        let lib = std::fs::read_to_string(pkg.join("src/lib.rs")).unwrap();
        assert!(lib.contains("#[path = \"../source/mod.rs\"]"), "{lib}");
        let slice = std::fs::read_dir(&shared).unwrap().map(|e| e.unwrap().path()).find(|p| p.file_name().unwrap().to_string_lossy().starts_with("weft-")).unwrap();
        assert!(manifests[0].contains(&format!("path = \"{}\"", slice.join("crates/weft-engine").display())), "{}", manifests[0]);
        let hack = std::fs::read_to_string(slice.join("crates").join(crate::codegen::WORKSPACE_HACK_CRATE).join("Cargo.toml")).unwrap();
        assert!(!hack.contains("[dependencies]"), "{hack}");
        let leftovers: Vec<_> = std::fs::read_dir(&shared).unwrap().map(|e| e.unwrap().file_name()).filter(|n| n.to_string_lossy().starts_with('.')).collect();
        assert!(leftovers.is_empty(), "no half-written folder stays: {leftovers:?}");
    }

    /// A package's test server compiled on the machine is its own crate
    /// with its own recipe (the program `<package>_tests`, nothing for
    /// `weft-isolate` to leave out), and links the very package crate a
    /// worker holding that package compiles, with its tests in.
    #[test]
    fn a_host_test_build_links_the_workers_package_crate() {
        let temp = tempfile::tempdir().unwrap();
        let shared = temp.path().join("shared");
        let project = temp.path().join("project");
        let catalog = crate::codegen::tests::catalog_with_needs(&project);
        let definition: weft_core::project::ProjectDefinition = serde_json::from_value(serde_json::json!({
            "id": uuid::Uuid::nil(), "nodes": [{"id": "greet", "nodeType": "Run", "config": {}, "position": {"x": 0, "y": 0}}], "edges": []
        }))
        .unwrap();
        let (worker, _worker_hold) = super::stage_host_build(&project.join("worker"), &shared, &definition, &catalog, weft_core::builds::NodeSet::Referenced).expect("stage the worker");
        let package = catalog.package_of("Run").unwrap().name.clone();
        let (tests, _tests_hold) = super::stage_host_test_build(&project.join("tests"), &shared, &catalog, &package).expect("stage the test server");
        let recipe_of = |ctx: &std::path::Path| -> weft_core::builds::HostRecipe {
            serde_json::from_slice(&std::fs::read(ctx.join(weft_core::builds::HOST_RECIPE_FILE)).unwrap()).unwrap()
        };
        let recipe = recipe_of(&tests);
        assert_eq!(recipe.crate_dir, tests.join(super::HOST_CRATE_DIR));
        assert_eq!(recipe.binary_name, format!("{}_tests", super::sanitize_crate_name(&package)));
        assert_eq!(recipe.seed_lock, recipe_of(&worker).seed_lock, "one workspace slice for both");
        for file in [&recipe.isolate_candidates, &recipe.left_out_record] {
            assert!(file.is_file(), "{}", file.display());
        }
        let candidates: serde_json::Value = serde_json::from_slice(&std::fs::read(&recipe.isolate_candidates).unwrap()).unwrap();
        assert_eq!(candidates["packages"], serde_json::json!([]));
        let manifest = std::fs::read_to_string(recipe.crate_dir.join("Cargo.toml")).unwrap();
        assert!(manifest.contains("[workspace]"), "built on its own: {manifest}");
        let path_of = |manifest: &str| -> String {
            let line = manifest.lines().find(|l| l.starts_with("pkg_run") && l.contains("path =")).expect("the package dep");
            line.split("path = \"").nth(1).unwrap().split('"').next().unwrap().to_string()
        };
        let worker_manifest = std::fs::read_to_string(recipe_of(&worker).crate_dir.join("Cargo.toml")).unwrap();
        assert_eq!(path_of(&manifest), path_of(&worker_manifest), "the worker's package crate");
        assert!(std::path::Path::new(&path_of(&manifest)).starts_with(&shared));
        assert!(manifest.lines().any(|l| l.starts_with("pkg_run") && l.contains("node-tests")), "{manifest}");
    }

    /// A node's `[build.env]` reaches a host build as a forced `[env]`
    /// entry of the worker crate's cargo config, so it wins over the same
    /// variable set on the machine, as an image's `ENV` does.
    #[test]
    fn a_host_build_forces_the_nodes_build_env() {
        let temp = tempfile::tempdir().unwrap();
        let project = temp.path().join("project");
        let at = project.join("nodes").join("vips");
        std::fs::create_dir_all(&at).unwrap();
        std::fs::write(at.join("metadata.json"), r#"{"type": "Vips", "label": "Vips", "description": "d", "inputs": [], "outputs": []}"#).unwrap();
        std::fs::write(at.join("mod.rs"), "// impl\n").unwrap();
        std::fs::write(at.join("deps.toml"), "[build.env]\nPKG_CONFIG_PATH = \"{{catalog_path}}/pkgconfig\"\n").unwrap();
        let catalog = weft_catalog::FsCatalog::discover(&project.join("nodes")).unwrap();
        let definition: weft_core::project::ProjectDefinition = serde_json::from_value(serde_json::json!({
            "id": uuid::Uuid::nil(), "nodes": [{"id": "thumb", "nodeType": "Vips", "config": {}, "position": {"x": 0, "y": 0}}], "edges": []
        }))
        .unwrap();
        let (ctx, _hold) = super::stage_host_build(&project.join("ctx"), &temp.path().join("shared"), &definition, &catalog, weft_core::builds::NodeSet::Referenced).expect("stage");
        let config: toml::Table = std::fs::read_to_string(ctx.join(super::HOST_CRATE_DIR).join(".cargo/config.toml")).unwrap().parse().unwrap();
        let entry = config["env"]["PKG_CONFIG_PATH"].as_table().expect("a table entry");
        assert_eq!(entry["force"].as_bool(), Some(true));
        let value = entry["value"].as_str().unwrap();
        assert!(
            value.starts_with(&temp.path().join("shared").display().to_string()) && value.contains("/source") && value.ends_with("/pkgconfig"),
            "the node's folder in its package crate: {value}"
        );
    }

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
        assert!(manifest.contains("pkg_format = { optional = true, path = \"./pkg_format-"), "{manifest}");
        assert!(crate_root.join(crate::worker_image::CACHE_GC_SCRIPT_NAME).is_file());
        // Staged at its project-relative path: a node under `nodes/` and
        // one beside the code under `src/` land side by side under the mount.
        assert!(ctx.join("project-nodes/nodes/base_catalog/basic/format/mod.rs").is_file());
        // Every package file, staged and generated, carries the date its
        // content gives it, so a package compiled from another checkout is
        // reused.
        let pkg_dir = std::fs::read_dir(&crate_root)
            .unwrap()
            .map(|e| e.unwrap().path())
            .find(|p| p.file_name().unwrap().to_string_lossy().starts_with("pkg_format-"))
            .expect("the format package's crate");
        for file in [
            ctx.join("project-nodes/nodes/base_catalog/basic/format/mod.rs"),
            pkg_dir.join("Cargo.toml"),
            pkg_dir.join("src/lib.rs"),
        ] {
            let modified = std::fs::metadata(&file).unwrap().modified().unwrap();
            assert_eq!(modified, super::package_source_mtime(&std::fs::read(&file).unwrap()), "{}", file.display());
        }
        let dockerfile = std::fs::read_to_string(ctx.join("Dockerfile")).unwrap();
        assert!(!dockerfile.contains("{{"), "every token rendered: {dockerfile}");
        assert!(
            dockerfile.contains("weft-cache-gc.sh this-build /cache/target/release /tmp/cargo-messages.json"),
            "the base keeps its cache to what its build used: {dockerfile}"
        );
        assert!(dockerfile.contains("COPY project-nodes /weft/project-nodes"));
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
