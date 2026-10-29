//! Codegen. Given an enriched+validated `ProjectDefinition` and the
//! project's `FsCatalog`, emit a cargo crate under
//! `.weft/target/build/` that cargo builds into the project's native
//! worker binary.
//!
//! Emitted layout:
//!
//! ```text
//! .weft/target/build/
//!   Cargo.toml          # base deps + an optional path dep on each
//!                       # package crate, one feature per package,
//!                       # all of them on by default.
//!   rust-toolchain.toml # the weft workspace's pinned toolchain.
//!   weft-cache-gc.sh    # the compile cache sweep, run after `cargo build`.
//!   isolate.json        # the package crates the build may leave out.
//!   left-out.json       # the record of what it left out (empty until
//!                       # the build overwrites it); registry.rs embeds it.
//!   weft-isolate-<slot>/ # the program that builds the crate, leaving
//!                       # out the packages that do not compile.
//!   pkg_<name>-<slot>/  # one cargo crate per referenced package,
//!     Cargo.toml        # carrying its package.toml + deps.toml deps.
//!     src/lib.rs        # #[path]-includes the package's shared .rs
//!                       # files at the top level, then each referenced
//!                       # node's mod.rs as a submodule. Nodes reach
//!                       # shared code via `use super::<shared_mod>;`.
//!   src/
//!     main.rs           # spawns weft-engine; fetches the
//!                       # ProjectDefinition per run from the
//!                       # broker. NO project.json baked in.
//!     registry.rs       # NodeCatalog impl: dispatches node_type to
//!                       # the struct in its `pkg_<name>` crate.
//! ```
//!
//! `<slot>` is a hash over the package's files and its generated
//! manifest and shim, so editing one node recompiles only its package.
//!
//! A package that does not compile costs only its own node types: the
//! worker build runs `weft-isolate` (`worker_isolate`), which builds
//! every package crate it can, turns off the features of the ones that
//! failed, builds the worker without them, and writes `left-out.json`.
//! The registry gates each package's arms on its feature and answers
//! `NodeCatalog::left_out` from that record, and the record is kept with
//! the worker for the install to read (beside the program on a local
//! install, in the image at `weft_core::builds::LEFT_OUT_RECORD_PATH` on
//! the cloud). A plain `cargo build` builds everything.
//!
//! Pruning: each package's crate is emitted only if at least one of
//! its nodes is referenced. Within a crate, only referenced node
//! subdirs are `#[path]`-included. Package-level shared .rs files
//! are always included in an emitted shim (they may be load-bearing
//! for the referenced nodes; dead-code analysis in the Rust compiler
//! prunes unused shared helpers).
//!
//! Per-run project fetch: the worker no longer carries a
//! baked-in `ProjectDefinition`. Each `Execute` / `Resume` task
//! payload carries the `definition_hash` the user clicked Run
//! against; the worker fetches the matching definition from the
//! broker, caches by hash so repeated runs of the same shape
//! pay one round trip per shape, and runs the engine on that. A
//! pure config or topology edit therefore no longer regenerates any
//! source file in this crate, and the resulting worker is a cache hit
//! on the project's binary tag.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use weft_catalog::FsCatalog;
use weft_core::builds::NodeSet;
use weft_core::ProjectDefinition;

use crate::error::{BuildError, BuildResult};

/// Emit the full cargo crate. Writes every file listed in the module
/// docstring (each package crate where `paths` puts them: under the crate
/// root, or for a host build in the machine-wide folder). Returns each
/// emitted package's crate directory, by package name.
pub fn emit(
    project: &ProjectDefinition,
    target_root: &Path,
    catalog: &FsCatalog,
    crate_name: &str,
    node_set: NodeSet,
    paths: &EmitPaths,
) -> BuildResult<BTreeMap<String, PathBuf>> {
    let crate_root = target_root.to_path_buf();
    // A pure function of its inputs: wipe any previous emission so a
    // renamed package or dropped node never leaves stale files in the
    // crate (the staged build context would faithfully copy them).
    if crate_root.exists() {
        std::fs::remove_dir_all(&crate_root).map_err(BuildError::Io)?;
    }
    let src_dir = crate_root.join("src");
    std::fs::create_dir_all(&src_dir).map_err(BuildError::Io)?;

    // Runtime-resolvable workspace root (honors `WEFT_REPO_ROOT`), NOT the raw
    // compile-time `CARGO_MANIFEST_DIR`: when the compile happens inside a built
    // image there is no compile-time path, so codegen must read the pinned
    // toolchain from the baked workspace. One resolver for every weft-root read.
    let weft_root = crate::build::resolve_weft_root()?;

    // The CATALOG node types this binary compiles in: what the project
    // references, or the whole catalog under `NodeSet::Full`. Runtime-
    // internal built-ins (LoopIn, LoopOut) are never
    // among them: the engine dispatches them inline, so they don't need
    // a catalog shim and the catalog doesn't carry them.
    let referenced = node_types_for(project, catalog, node_set);

    // Group referenced nodes by their owning package. `package_key`
    // is the package root dir; codegen emits one `pkg_<name>/` cargo
    // crate per distinct package (so editing one node's mod.rs
    // recompiles only ITS package's .rlib + relinks the worker, not
    // every package).
    let packages = group_by_package(catalog, &referenced)?;

    // The `ProjectDefinition` is NOT baked into the binary anymore;
    // workers fetch it from the broker at run claim time keyed
    // by `definition_hash`. A pure-config or pure-topology edit
    // therefore no longer re-bakes any source file into the worker
    // crate, and the binary hash stays cache-hit.
    let crate_dirs = write_package_crates(&crate_root, catalog, &packages, paths)?;
    write_worker_cargo_toml(&crate_root, &packages, &crate_dirs, crate_name, paths)?;
    write_rust_toolchain(&crate_root, &weft_root)?;
    let candidates = packages
        .iter()
        .map(|pkg| crate::worker_isolate::Candidate {
            feature: pkg.module_ident.clone(),
            crate_dir: crate_dirs[&pkg.module_ident].clone(),
            package: pkg.package.name.clone(),
            node_types: pkg.node_types.clone(),
        })
        .collect();
    write_image_build_files(&crate_root, crate::worker_isolate::Candidates { packages: candidates })?;
    write_registry_rs(&src_dir, &packages, PackageGating::Features)?;
    write_main_rs(&src_dir, catalog)?;

    Ok(packages
        .iter()
        .map(|pkg| (pkg.package.name.clone(), paths.package_crates_dir(&crate_root).join(&crate_dirs[&pkg.module_ident])))
        .collect())
}

/// Where an emitted crate's references point. The SAME shim/deps
/// emission serves three homes:
///   - `Container`: the crate compiles inside a docker build whose
///     context mounts the weft workspace at `WEFT_MOUNT` and the
///     project's nodes at `NODES_MOUNT` (a cloud's worker image, and
///     the builder base's stock worker). `#[path]` includes and path deps use the
///     mount layout; `project_root` is the on-disk project the
///     includes are rebased against (a node lives under its `nodes/`
///     or beside the code under its `src/`, and both trees are staged
///     at their project-relative path).
///   - `Local`: the crate compiles on the host with plain cargo (the
///     local node-test binary). `#[path]` includes are the real
///     absolute node paths; path deps point at the real weft checkout.
///   - `Host`: a worker compiled on the machine that runs it. Path deps
///     point at `weft`, the worker-linked slice of the weft workspace
///     (`build::stage_worker_workspace`, with its empty workspace-hack, so
///     the worker compiles none of the workspace's feature union). Each
///     package crate is written once into `package_crates` (the
///     machine-wide folder) with a copy of its package's sources beside
///     it ([`HOST_PACKAGE_SOURCE_DIR`]), its includes relative to that
///     copy: a package crate is then the same path for every project
///     whose package has the same content, and cargo reuses its compile.
pub enum EmitPaths {
    Container { project_root: PathBuf },
    Local { weft_root: PathBuf },
    Host { weft: PathBuf, package_crates: PathBuf },
}

/// The folder inside a host package crate holding its package's sources.
pub const HOST_PACKAGE_SOURCE_DIR: &str = "source";

/// From where a generated include is read.
#[derive(Clone, Copy)]
enum IncludeFrom {
    /// A package crate's `src/lib.rs` (a `#[path]` attribute).
    LibRs,
    /// A package crate's `Cargo.toml` (its `build = ...`).
    Manifest,
}

impl EmitPaths {
    /// The folder the package crates of a crate emitted at `crate_root`
    /// go in.
    pub fn package_crates_dir(&self, crate_root: &Path) -> PathBuf {
        match self {
            EmitPaths::Container { .. } | EmitPaths::Local { .. } => crate_root.to_path_buf(),
            EmitPaths::Host { package_crates, .. } => package_crates.clone(),
        }
    }

    /// The include string for a real source file of the package rooted at
    /// `package_root`, read from `from`.
    fn include_path(&self, package_root: &Path, abs: &Path, from: IncludeFrom) -> BuildResult<String> {
        match self {
            EmitPaths::Container { project_root } => {
                let rel = abs.strip_prefix(project_root).map_err(|_| {
                    BuildError::Build(format!(
                        "source file {} is not under the project root {}",
                        abs.display(),
                        project_root.display()
                    ))
                })?;
                Ok(format!(
                    "{}/{}",
                    crate::worker_image::NODES_MOUNT,
                    rel.display().to_string().replace('\\', "/"),
                ))
            }
            EmitPaths::Local { .. } => Ok(abs.display().to_string().replace('\\', "/")),
            EmitPaths::Host { .. } => {
                let rel = abs.strip_prefix(package_root).map_err(|_| {
                    BuildError::Build(format!(
                        "source file {} is not under its package root {}",
                        abs.display(),
                        package_root.display()
                    ))
                })?;
                let up = match from {
                    IncludeFrom::LibRs => "../",
                    IncludeFrom::Manifest => "",
                };
                Ok(format!("{up}{HOST_PACKAGE_SOURCE_DIR}/{}", rel.display().to_string().replace('\\', "/")))
            }
        }
    }

    /// A path-dep spec for a weft workspace crate, from a crate that
    /// sits `depth` directories below the emitted root (the root crate
    /// itself is depth 1 relative to the context, a `pkg_<name>/`
    /// crate is depth 2).
    fn weft_crate_dep(&self, crate_dir: &str, depth: usize) -> toml::Table {
        match self {
            EmitPaths::Container { .. } => {
                path_table(&format!("{}weft/crates/{crate_dir}", "../".repeat(depth)))
            }
            EmitPaths::Local { weft_root: weft } | EmitPaths::Host { weft, .. } => path_table(
                &weft
                    .join("crates")
                    .join(crate_dir)
                    .display()
                    .to_string()
                    .replace('\\', "/"),
            ),
        }
    }
}

/// Mapping from a package (identified by root dir) to the list of
/// node types from that package that this project references. Used
/// by codegen to emit one shim per package with only the referenced
/// nodes inside.
struct PackageEmit<'a> {
    package: &'a weft_catalog::Package,
    /// Referenced node types in this package, sorted.
    node_types: Vec<String>,
    /// Sanitized module identifier: `pkg_<package_name>` lowercased.
    module_ident: String,
}

fn group_by_package<'a>(
    catalog: &'a FsCatalog,
    referenced: &BTreeSet<String>,
) -> BuildResult<Vec<PackageEmit<'a>>> {
    let mut by_key: BTreeMap<std::path::PathBuf, Vec<String>> = BTreeMap::new();
    for node_type in referenced {
        let Some(pkg) = catalog.package_of(node_type) else {
            return Err(BuildError::Build(format!(
                "node '{node_type}' not found in catalog"
            )));
        };
        by_key
            .entry(pkg.root.clone())
            .or_default()
            .push(node_type.clone());
    }
    let mut out = Vec::with_capacity(by_key.len());
    for (root, mut nodes) in by_key {
        nodes.sort();
        let pkg = catalog
            .packages()
            .find(|p| p.root == root)
            .expect("package key came from catalog");
        let module_ident = sanitize_pkg_ident(&pkg.name);
        out.push(PackageEmit {
            package: pkg,
            node_types: nodes,
            module_ident,
        });
    }
    Ok(out)
}

fn sanitize_pkg_ident(raw: &str) -> String {
    // Delegates to the one sanitizer (collision-free by construction)
    // so a package's module ident, crate name, and staging paths can
    // never disagree or collide.
    format!("pkg_{}", crate::build::sanitize_crate_name(raw))
}

/// The catalog node types a build under `node_set` compiles in, sorted:
/// [`collect_node_types`] for `Referenced`, every catalog entry for
/// `Full`. The ONE resolution of the choice; codegen, the build
/// staging and the binary hash all read it.
///
/// A type the catalog does not serve is no code to compile: the node
/// using it is one of the program's broken parts
/// (`crate::compile_program`), as is the placeholder an included file
/// that does not compile leaves, and no run that needs it starts.
pub fn node_types_for(
    project: &ProjectDefinition,
    catalog: &FsCatalog,
    node_set: NodeSet,
) -> BTreeSet<String> {
    match node_set {
        NodeSet::Referenced => collect_node_types(project).into_iter().filter(|t| catalog.entry(t).is_some()).collect(),
        NodeSet::Full => catalog.iter().map(|e| e.node_type.clone()).collect(),
    }
}

/// What each node type the worker holds needs from the machine it runs
/// on (`node_types`, the worker's set: [`node_types_for`]), from each
/// type's package `deps.toml`; a type that needs nothing is left out.
/// Kept with the worker, which the program it runs is read against
/// (`weft_core::needs::program_needs`).
pub fn node_needs(node_types: &BTreeSet<String>, catalog: &FsCatalog) -> BuildResult<weft_core::needs::NodeNeeds> {
    let mut out = weft_core::needs::NodeNeeds::new();
    for node_type in node_types {
        let Some(deps) = catalog.deps(node_type).map_err(|e| BuildError::Build(format!("load deps for {node_type}: {e}")))? else {
            continue;
        };
        if !deps.needs.is_empty() {
            out.insert(node_type.clone(), deps.needs);
        }
    }
    Ok(out)
}

/// What the program's nodes need from the machine its worker runs on:
/// for every catalog node type `definition` uses (never a type it does
/// not use, whatever the catalog holds), each need its package's
/// `deps.toml` declares, with the program's nodes of that type
/// (`weft_core::needs::program_needs` over [`node_needs`]).
pub fn needs_of(definition: &ProjectDefinition, catalog: &FsCatalog) -> BuildResult<Vec<weft_core::needs::NodeNeed>> {
    let used = node_types_for(definition, catalog, NodeSet::Referenced);
    Ok(weft_core::needs::program_needs(definition, &node_needs(&used, catalog)?))
}

/// The set of CATALOG node types this project references, sorted.
/// The type names weft gives its own nodes (`own_types`: LoopIn, LoopOut,
/// the include placeholder) are excluded: they live in the engine, not the
/// catalog, and the codegen / hash / build passes that consume this
/// set would otherwise fail to resolve them. Exposed so callers
/// outside codegen (e.g. the build staging) walk the same set
/// without re-implementing the filter.
pub fn collect_node_types(project: &ProjectDefinition) -> BTreeSet<String> {
    project
        .nodes
        .iter()
        .map(|n| n.node_type.clone())
        .filter(|t| !weft_core::project::own_types::is_own(t))
        .collect()
}

/// Emit the worker binary's `Cargo.toml`. Deps are the engine
/// surface (engine + core + broker-client + platform-traits), the
/// runtime essentials (tokio, serde, async-trait, anyhow, clap,
/// tracing, uuid), and one path dep per referenced package crate
/// under `pkg_<name>/`. Per-package and per-node cargo deps live
/// inside each package crate's own `Cargo.toml`, NOT here: a node
/// that pulls in a heavy dependency stays a recompile-target of its OWN crate,
/// while the worker keeps cache-hitting on every other build.
fn write_worker_cargo_toml(
    crate_root: &Path,
    packages: &[PackageEmit<'_>],
    crate_dirs: &BTreeMap<String, String>,
    crate_name: &str,
    paths: &EmitPaths,
) -> BuildResult<()> {
    let package_name = crate::build::sanitize_crate_name(crate_name);

    let mut deps = fixed_worker_deps(paths);
    // One path dep per referenced package crate. These are the ONLY
    // per-project deps; everything else (`fixed_worker_deps`) is identical for
    // every worker. The builder base compiles the stock project's worker
    // from this same emission, so a project reuses every rlib it shares
    // with it: the engine, the dependencies, and each stock package crate
    // it did not edit.
    //
    // Each is optional behind a feature named like it, all on by default:
    // a plain `cargo build` builds everything, and `weft-isolate` can
    // build the worker without a package that failed. Features of this
    // crate never reach the package crates' own compile, so a package's
    // rlib is the same unit either way.
    for pkg in packages {
        let mut dep = path_table(&package_crate_path(paths, &crate_dirs[&pkg.module_ident]));
        dep.insert("optional".into(), toml::Value::Boolean(true));
        insert_dep(&mut deps, &pkg.module_ident, toml::Value::Table(dep));
    }

    let mut deps_fragment = String::new();
    for (name, value) in deps {
        deps_fragment.push_str(&format!("{name} = {}\n", toml_inline(&value)));
    }
    let features: Vec<String> = packages.iter().map(|pkg| toml_inline(&toml::Value::String(pkg.module_ident.clone()))).collect();
    let mut features_fragment = format!("[features]\ndefault = [{}]\n", features.join(", "));
    for pkg in packages {
        features_fragment.push_str(&format!("{ident} = [\"dep:{ident}\"]\n", ident = pkg.module_ident));
    }

    let contents = format!(
        r#"# Emitted by weft codegen. Do not edit by hand; regenerated on
# every `weft build`.

{workspace}[package]
name = "{name}"
version = "0.1.0"
edition = "2021"

[[bin]]
name = "{name}"
path = "src/main.rs"

{features}
[dependencies]
{deps}"#,
        workspace = STANDALONE_WORKSPACE_HEADER,
        name = package_name,
        features = features_fragment,
        deps = deps_fragment,
    );
    std::fs::write(crate_root.join("Cargo.toml"), contents).map_err(BuildError::Io)?;
    Ok(())
}

/// Emit one cargo crate per referenced node package and answer where
/// each landed: `module_ident -> directory name under crate_root`. The
/// package crate's `lib.rs` reuses the original `pkg_<name>.rs` shim
/// shape (`#[path]`-includes of shared `.rs` files and each referenced
/// node's `mod.rs`); the difference is that it now compiles to a
/// standalone `.rlib`, so editing one node's source rebuilds ONLY this
/// crate plus the worker's link step, leaving sibling package crates
/// cache-hit.
///
/// The directory is `pkg_<name>-<slot>`, where the slot is a digest of
/// the package's sources AND the two generated files. Cargo tells path
/// crates apart by their path, never by their content, and every
/// project's build shares one compile cache on the host: with a fixed
/// `pkg_<name>` path, a project whose copy of a stock node is edited
/// would leave its rlib where the next untouched project (whose files
/// carry older mtimes) picks it up as fresh, and that project's worker
/// would run the edit. Same content, same slot, shared rlib; a
/// difference in the package's files or in either generated file, a
/// slot of its own. The slot covers exactly those inputs: anything the
/// generator later adds to the emitted root manifest that reaches every
/// package crate (a `[profile]`, a `[patch]`) has to join the digest.
///
/// Per-package and per-node cargo deps land here, not on the worker.
///
/// A host build (`EmitPaths::Host`) writes each crate into the
/// machine-wide folder instead, with its package's sources copied in
/// beside it, and never rewrites one that is there: its name is its
/// content, and a build elsewhere may be compiling it. It is written in a
/// folder of its own first and renamed into place, so a crate is either
/// whole or absent.
fn write_package_crates(
    crate_root: &Path,
    catalog: &FsCatalog,
    packages: &[PackageEmit<'_>],
    paths: &EmitPaths,
) -> BuildResult<BTreeMap<String, String>> {
    let parent = paths.package_crates_dir(crate_root);
    let mut dirs = BTreeMap::new();
    for pkg in packages {
        let cargo_toml = render_package_cargo_toml(catalog, pkg, paths)?;
        let lib_rs = render_package_lib_rs(catalog, pkg, paths)?;
        let dir_name = format!(
            "{}-{}",
            pkg.module_ident,
            package_crate_slot(pkg, &cargo_toml, &lib_rs)?
        );
        match paths {
            EmitPaths::Host { .. } => {
                crate::build::write_once(&parent.join(&dir_name), &|dir| {
                    write_package_crate(dir, &cargo_toml, &lib_rs)?;
                    crate::build::copy_package_source(&pkg.package.root, &dir.join(HOST_PACKAGE_SOURCE_DIR))
                })?;
            }
            EmitPaths::Container { .. } | EmitPaths::Local { .. } => {
                write_package_crate(&parent.join(&dir_name), &cargo_toml, &lib_rs)?;
            }
        }
        dirs.insert(pkg.module_ident.clone(), dir_name);
    }
    Ok(dirs)
}

/// Write one package crate's two generated files into `pkg_dir`.
fn write_package_crate(pkg_dir: &Path, cargo_toml: &str, lib_rs: &str) -> BuildResult<()> {
    let pkg_src = pkg_dir.join("src");
    std::fs::create_dir_all(&pkg_src).map_err(BuildError::Io)?;
    std::fs::write(pkg_dir.join("Cargo.toml"), cargo_toml).map_err(BuildError::Io)?;
    std::fs::write(pkg_src.join("lib.rs"), lib_rs).map_err(BuildError::Io)?;
    // Dated like the staged sources it includes (see
    // `build::package_source_mtime`): written at wall-clock time it
    // would be newer than every cached rlib, and every package would
    // compile on every build.
    for (generated, content) in [(pkg_dir.join("Cargo.toml"), cargo_toml), (pkg_src.join("lib.rs"), lib_rs)] {
        crate::build::stamp_mtime(&generated, crate::build::package_source_mtime(content.as_bytes()));
    }
    Ok(())
}

/// The path dependency spec a binary crate names the package crate
/// `dir_name` by.
fn package_crate_path(paths: &EmitPaths, dir_name: &str) -> String {
    match paths {
        EmitPaths::Container { .. } | EmitPaths::Local { .. } => format!("./{dir_name}"),
        EmitPaths::Host { package_crates, .. } => package_crates.join(dir_name).display().to_string().replace('\\', "/"),
    }
}

/// The slot a package crate compiles in: sixteen hex characters over
/// the package's files (the package walk the binary hash uses, so
/// the same files count) and the generated manifest and shim.
fn package_crate_slot(
    pkg: &PackageEmit<'_>,
    cargo_toml: &str,
    lib_rs: &str,
) -> BuildResult<String> {
    use sha2::Digest as _;
    let mut hasher = sha2::Sha256::new();
    hasher.update(b"weft-package-crate-v1\n");
    crate::hash::hash_package_root(&mut hasher, &pkg.module_ident, &pkg.package.root, &crate::hash::DISK)
        .map_err(|e| BuildError::Build(format!("hash package {}: {e}", pkg.package.name)))?;
    hasher.update(b"Cargo.toml:\n");
    hasher.update(cargo_toml.as_bytes());
    hasher.update(b"\nlib.rs:\n");
    hasher.update(lib_rs.as_bytes());
    Ok(crate::hash::hex(&hasher.finalize()).chars().take(16).collect())
}

/// A package's build script, as lines of its package crate's manifest:
/// the `[package]` build line and the `[build-dependencies]` section,
/// both empty when the package ships no `build.rs` at its root. The
/// script is the package crate's own, so it runs (and its
/// build-dependencies compile) for that package alone: one that fails
/// leaves only its package unbuilt, which is what lets `weft-isolate`
/// leave just that package out. Its `cargo:` lines reach what cargo lets
/// a library's script reach: `rustc-link-lib` and `rustc-link-search`
/// carry to the binary that links it, `rustc-env`, `rustc-cfg` and
/// `OUT_DIR` to the package's own code.
fn package_build_script(
    catalog: &FsCatalog,
    pkg: &PackageEmit<'_>,
    paths: &EmitPaths,
) -> BuildResult<(String, String)> {
    let mut build_deps: BTreeMap<String, toml::Value> = BTreeMap::new();
    for node_type in &pkg.node_types {
        let node_deps = catalog
            .deps(node_type)
            .map_err(|e| BuildError::Build(format!("deps for '{node_type}': {e}")))?;
        let Some(node_deps) = node_deps else { continue };
        for (name, value) in node_deps.build_dependencies {
            add_dep(
                &mut build_deps,
                &pkg.package.name,
                name,
                value,
                &format!("node {node_type} (build-dependencies)"),
            )?;
        }
    }
    let script = pkg.package.root.join("build.rs");
    if !script.is_file() {
        if let Some(name) = build_deps.keys().next() {
            return Err(BuildError::Build(format!(
                "a deps.toml in package '{}' declares build-dependency '{name}' but the \
                 package ships no build.rs at its root; build-dependencies exist only \
                 for a package build script",
                pkg.package.name
            )));
        }
        return Ok((String::new(), String::new()));
    }
    let build_line = format!(
        "build = \"{}\"\n",
        escape_quoted_str(&paths.include_path(&pkg.package.root, &script, IncludeFrom::Manifest)?)
    );
    let mut build_deps_fragment = String::new();
    if !build_deps.is_empty() {
        build_deps_fragment.push_str("\n[build-dependencies]\n");
        for (name, value) in build_deps {
            build_deps_fragment.push_str(&format!("{name} = {}\n", toml_inline(&value)));
        }
    }
    Ok((build_line, build_deps_fragment))
}

fn render_package_cargo_toml(
    catalog: &FsCatalog,
    pkg: &PackageEmit<'_>,
    paths: &EmitPaths,
) -> BuildResult<String> {
    // Runtime essentials shared with the worker, plus the workspace
    // surface every node body uses. `paths` places the weft crates
    // (`../../weft/...` inside a build container, the real checkout
    // for a host build); depth 2 because we live under `pkg_<name>/`.
    let mut deps = base_runtime_deps();
    // The node-author surface is the `weft` crate: an ALIAS of
    // weft-core, so bodies write `use weft::{...}` (the friendly name)
    // and never see the internal crate layout. Macro-generated code
    // (the NodeManifest derive) resolves whatever name the crate gave
    // weft-core, so the alias is the ONLY name a package crate needs.
    insert_dep(&mut deps, "weft", {
        let mut t = paths.weft_crate_dep("weft-core", 2);
        t.insert("package".into(), toml::Value::String("weft-core".into()));
        toml::Value::Table(t)
    });
    // A package may define its OWN provider meter (a shared `.rs` file that
    // calls `weft_providers::register_meter!`), so every package crate can
    // reach the provider toolkit. Cargo compiles it only if a file uses it,
    // and the workspace resolves the one shared copy the worker already
    // links via weft-engine, so this adds no second build.
    insert_dep(
        &mut deps,
        "weft-providers",
        toml::Value::Table(paths.weft_crate_dep("weft-providers", 2)),
    );

    if let Some(pkg_deps) = &pkg.package.package_deps {
        for (name, value) in pkg_deps.iter() {
            add_dep(
                &mut deps,
                &pkg.package.name,
                name.clone(),
                value.clone(),
                &format!("package {}", pkg.package.name),
            )?;
        }
    }
    for node_type in &pkg.node_types {
        let node_deps = catalog
            .deps(node_type)
            .map_err(|e| BuildError::Build(format!("deps for '{node_type}': {e}")))?;
        let Some(node_deps) = node_deps else { continue };
        for (name, value) in node_deps.dependencies {
            add_dep(&mut deps, &pkg.package.name, name, value, &format!("node {node_type}"))?;
        }
    }

    let mut deps_fragment = String::new();
    for (name, value) in deps {
        deps_fragment.push_str(&format!("{name} = {}\n", toml_inline(&value)));
    }
    let (build_line, build_deps_fragment) = package_build_script(catalog, pkg, paths)?;

    let contents = format!(
        r#"# Emitted by weft codegen. Per-package crate for `{pkg_name}`.
# Do not edit by hand; regenerated on every `weft build`.

[package]
name = "{ident}"
version = "0.1.0"
edition = "2021"
{build_line}
[lib]
path = "src/lib.rs"

[features]
# Compiles each node's `tests.rs` (gated in mod.rs behind
# `#[cfg(feature = "node-tests")]`). Enabled ONLY by the per-package
# test crate; the worker leaves it off, so the worker binary never
# contains tests. Declared here unconditionally so the cfg name is
# always a known feature.
node-tests = []

[dependencies]
{deps}{build_deps}"#,
        pkg_name = pkg.package.name,
        ident = pkg.module_ident,
        deps = deps_fragment,
        build_deps = build_deps_fragment,
    );
    Ok(contents)
}

/// Per-package `lib.rs`: re-exports each referenced node's module
/// under a snake_case identifier, after pulling in any
/// shared-package `.rs` files. Same shape as the old `pkg_*.rs`
/// shim that lived in the worker crate; the only difference is it
/// compiles to a standalone `.rlib`.
fn render_package_lib_rs(
    catalog: &FsCatalog,
    pkg: &PackageEmit<'_>,
    paths: &EmitPaths,
) -> BuildResult<String> {
    let mut body = String::new();
    body.push_str(&format!(
        "//! Package crate for `{}`. Emitted by codegen; do not edit.\n\n",
        pkg.package.name
    ));
    for shared_path in &pkg.package.shared_rs {
        let mod_name = shared_path
            .file_stem()
            .and_then(|s| s.to_str())
            .ok_or_else(|| {
                BuildError::Build(format!(
                    "package {} has shared file with no stem: {}",
                    pkg.package.name,
                    shared_path.display()
                ))
            })?;
        // The stem becomes a generated `pub mod {mod_name};`: the catalog
        // refused the package at load if it is not a module name its crate
        // can hold (`weft_catalog`'s `module_problem`).
        let include = escape_quoted_str(&paths.include_path(&pkg.package.root, shared_path, IncludeFrom::LibRs)?);
        body.push_str(&format!(
            "#[path = \"{include}\"]\npub mod {mod_name};\n\n"
        ));
    }
    for node_type in &pkg.node_types {
        let entry = catalog.entry(node_type).expect("collected from catalog");
        let mod_rs = entry.source_dir.join("mod.rs");
        let include = escape_quoted_str(&paths.include_path(&pkg.package.root, &mod_rs, IncludeFrom::LibRs)?);
        let mod_name = weft_catalog::node_module_name(node_type);
        body.push_str(&format!(
            "#[path = \"{include}\"]\npub mod {mod_name};\n\n"
        ));
    }
    Ok(body)
}

/// Which way an emitted binary crate depends on its package crates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PackageGating {
    /// Each package behind a feature of its own (the worker), so the
    /// build can leave out one that does not compile.
    Features,
    /// Every package always compiled in (a node-test crate: its one
    /// package is what it tests).
    Always,
}

/// Put what the built-in templates' build RUN calls out of `/work` at the
/// root of a crate that builds inside an image (the crate root is what
/// `COPY build/ /work/` carries): the compile-cache sweep
/// (`worker_image::CACHE_GC_SCRIPT`), `isolate.json` (`candidates`, the
/// package crates the build may leave out), the empty record
/// `left-out.json` the build overwrites, and the `weft-isolate` package
/// that builds the crate ([`isolate_dir_name`]). Every crate the
/// built-in templates and the host build compile gets them: the worker
/// crate, and a host test server's crate with no candidates (its one
/// package is what it tests), for which `weft-isolate` is a plain build.
///
/// The record and the helper's sources carry the date their content gives
/// them (`build::package_source_mtime`): the registry embeds the record, so a
/// wall-clock date would recompile the worker crate on every build, and
/// the helper compiles once per lane. Content never hides behind an old
/// date: the build rewrites the record whenever it leaves something out,
/// and the helper's package is named by its content ([`isolate_dir_name`]).
fn write_image_build_files(crate_root: &Path, candidates: crate::worker_isolate::Candidates) -> BuildResult<()> {
    std::fs::write(
        crate_root.join(crate::worker_image::CACHE_GC_SCRIPT_NAME),
        crate::worker_image::CACHE_GC_SCRIPT,
    )
    .map_err(BuildError::Io)?;
    let candidates = serde_json::to_string_pretty(&candidates)
        .map_err(|e| BuildError::Build(format!("write {ISOLATE_CANDIDATES_FILE}: {e}")))?;
    std::fs::write(crate_root.join(ISOLATE_CANDIDATES_FILE), candidates).map_err(BuildError::Io)?;
    let record = crate_root.join(LEFT_OUT_RECORD_FILE);
    std::fs::write(&record, EMPTY_LEFT_OUT_RECORD).map_err(BuildError::Io)?;

    let helper = crate_root.join(isolate_dir_name());
    std::fs::create_dir_all(helper.join("src")).map_err(BuildError::Io)?;
    std::fs::write(helper.join("Cargo.toml"), isolate_cargo_toml(&isolate_dir_name())).map_err(BuildError::Io)?;
    std::fs::write(helper.join("src").join("main.rs"), ISOLATE_SOURCE).map_err(BuildError::Io)?;
    for file in [record, helper.join("Cargo.toml"), helper.join("src").join("main.rs")] {
        let content = std::fs::read(&file).map_err(BuildError::Io)?;
        crate::build::stamp_mtime(&file, crate::build::package_source_mtime(&content));
    }
    Ok(())
}

/// The candidates file `weft-isolate` reads, at the crate root.
pub const ISOLATE_CANDIDATES_FILE: &str = "isolate.json";

/// The record of the packages the build left out, at the crate root:
/// what `weft-isolate` writes, the registry embeds, and the build keeps
/// with the worker (beside the program, or copied into the image).
pub const LEFT_OUT_RECORD_FILE: &str = "left-out.json";

/// What `left-out.json` holds until a build leaves something out.
const EMPTY_LEFT_OUT_RECORD: &str = "{\"packages\":[]}";

/// The `weft-isolate` program (`worker_isolate`), emitted as its own
/// package's `main.rs`.
const ISOLATE_SOURCE: &str = include_str!("worker_isolate.rs");

/// The helper's manifest, its package named `package`: its own empty
/// workspace (so cargo never takes it for a stray member of the worker's),
/// and only serde and serde_json, on the specs every generated crate uses.
/// The program it builds is `weft-isolate` whatever the package's name.
fn isolate_cargo_toml(package: &str) -> String {
    let runtime = base_runtime_deps();
    let mut deps = String::new();
    for name in ["serde", "serde_json"] {
        deps.push_str(&format!("{name} = {}\n", toml_inline(&runtime[name])));
    }
    format!(
        "# Emitted by weft codegen: the program that builds the worker crate,\n\
         # leaving out the node packages that do not compile. Do not edit.\n\
         \n\
         [workspace]\n\
         \n\
         [package]\n\
         name = \"{package}\"\n\
         version = \"0.1.0\"\n\
         edition = \"2021\"\n\
         \n\
         [[bin]]\n\
         name = \"weft-isolate\"\n\
         path = \"src/main.rs\"\n\
         \n\
         [dependencies]\n\
         {deps}"
    )
}

/// The `weft-isolate` package's name, and the directory it is emitted to
/// under a crate root: `weft-isolate-<slot>`, the slot sixteen hex
/// characters over its manifest and source. Its sources carry the date
/// their content gives them, years back, and cargo trusts a source older
/// than its last compile; it tells the helper's compiles apart by package
/// name, never by directory (a workspace root's own path hashes the same
/// wherever it sits), so with a fixed name a kept compile cache would keep
/// running an older helper. One name per content cannot.
pub fn isolate_dir_name() -> String {
    use sha2::Digest as _;
    let mut hasher = sha2::Sha256::new();
    hasher.update(b"weft-isolate-v2\nCargo.toml:\n");
    hasher.update(isolate_cargo_toml("weft-isolate").as_bytes());
    hasher.update(b"\nmain.rs:\n");
    hasher.update(ISOLATE_SOURCE.as_bytes());
    let slot: String = crate::hash::hex(&hasher.finalize()).chars().take(16).collect();
    format!("weft-isolate-{slot}")
}

/// Propagate the workspace's pinned toolchain into the generated
/// worker crate. The worker builds in its own crate dir (`/work` in
/// the image), which is a sibling of the weft mount, so `cargo build`
/// there wouldn't otherwise find the root `rust-toolchain.toml`.
/// Copying it in keeps the root file the single source of truth, so the
/// worker compiles on the exact toolchain the rest of the system uses.
/// Fails loud if the root pin is missing: a worker built on the wrong
/// toolchain would fail confusingly deep in the node compile instead.
fn write_rust_toolchain(crate_root: &Path, weft_root: &Path) -> Result<(), BuildError> {
    let pin = std::fs::read_to_string(weft_root.join("rust-toolchain.toml"))
        .map_err(|e| BuildError::Build(format!("read rust-toolchain.toml: {e}")))?;
    std::fs::write(crate_root.join("rust-toolchain.toml"), pin).map_err(BuildError::Io)?;
    Ok(())
}

/// Render a `toml::Value` as an inline TOML literal suitable for
/// embedding on the right-hand side of a `key = VALUE` line inside
/// `[dependencies]`. Tables become `{ k = v, ... }`.
fn toml_inline(v: &toml::Value) -> String {
    match v {
        toml::Value::String(s) => format!("\"{}\"", escape_quoted_str(s)),
        toml::Value::Integer(i) => i.to_string(),
        toml::Value::Float(f) => f.to_string(),
        toml::Value::Boolean(b) => b.to_string(),
        toml::Value::Datetime(d) => d.to_string(),
        toml::Value::Array(arr) => {
            let parts: Vec<String> = arr.iter().map(toml_inline).collect();
            format!("[{}]", parts.join(", "))
        }
        toml::Value::Table(t) => {
            let parts: Vec<String> = t
                .iter()
                .map(|(k, val)| format!("{k} = {}", toml_inline(val)))
                .collect();
            format!("{{ {} }}", parts.join(", "))
        }
    }
}

/// Escape a string for interpolation into a generated double-quoted
/// literal. Serves BOTH the TOML side (`toml_inline`'s string values)
/// and the Rust side (`#[path = "..."]` includes): the escape rules
/// coincide (backslash and double quote), so one function covers
/// both, e.g. a checkout path containing a double quote.
fn escape_quoted_str(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

fn path_table(path: &str) -> toml::Table {
    let mut t = toml::Table::new();
    t.insert("path".into(), toml::Value::String(path.into()));
    t
}

fn version_with_features(version: &str, features: &[&str]) -> toml::Table {
    let mut t = toml::Table::new();
    t.insert("version".into(), toml::Value::String(version.into()));
    t.insert(
        "features".into(),
        toml::Value::Array(
            features
                .iter()
                .map(|f| toml::Value::String((*f).into()))
                .collect(),
        ),
    );
    t
}

fn insert_dep(map: &mut BTreeMap<String, toml::Value>, name: &str, value: toml::Value) {
    map.insert(name.into(), value);
}

/// The shared runtime dep pins every generated crate builds against:
/// the worker binary and each per-package crate start from this set,
/// then add their own extras on top. One definition makes the
/// "same versions everywhere" contract structural, so the workspace
/// cargo lockfile resolves to a single copy of each.
fn base_runtime_deps() -> BTreeMap<String, toml::Value> {
    let mut deps: BTreeMap<String, toml::Value> = BTreeMap::new();
    insert_dep(
        &mut deps,
        "tokio",
        toml::Value::Table(version_with_features("1", &["full"])),
    );
    insert_dep(&mut deps, "serde_json", toml::Value::String("1".into()));
    insert_dep(
        &mut deps,
        "serde",
        toml::Value::Table(version_with_features("1", &["derive"])),
    );
    insert_dep(&mut deps, "async-trait", toml::Value::String("0.1".into()));
    insert_dep(&mut deps, "anyhow", toml::Value::String("1".into()));
    insert_dep(&mut deps, "tracing", toml::Value::String("0.1".into()));
    deps
}

/// The workspace crates a generated worker names directly: the seeds of the
/// worker crate closure (`worker_workspace_crates`).
const WORKER_ENGINE_CRATES: [&str; 3] = ["weft-engine", "weft-core", "weft-broker-client"];

/// The COMPLETE project-INDEPENDENT dependency set every generated worker carries:
/// the runtime baseline (`base_runtime_deps`) plus the engine path deps
/// ([`WORKER_ENGINE_CRATES`], placed by `paths`) and the fixed crates.io deps.
/// The ONLY thing a real worker adds on top is one path dep per referenced node
/// package (`pkg_<node>`), and any crates THOSE nodes pull in live inside the
/// `pkg_<node>` crate, not here.
pub fn fixed_worker_deps(paths: &EmitPaths) -> BTreeMap<String, toml::Value> {
    let mut deps = base_runtime_deps();
    for name in WORKER_ENGINE_CRATES {
        insert_dep(&mut deps, name, toml::Value::Table(paths.weft_crate_dep(name, 1)));
    }
    insert_dep(
        &mut deps,
        "clap",
        toml::Value::Table(version_with_features("4", &["derive", "env"])),
    );
    insert_dep(
        &mut deps,
        "tracing-subscriber",
        toml::Value::Table(version_with_features("0.3", &["env-filter"])),
    );
    insert_dep(
        &mut deps,
        "uuid",
        toml::Value::Table(version_with_features("1", &["v4", "serde"])),
    );
    // Decodes the hex-encoded live-connection routing-token secret from env.
    insert_dep(&mut deps, "hex", toml::Value::String("0.4".into()));
    deps
}

/// The workspace crates a worker build actually links: the closure of local
/// path dependencies reachable from [`WORKER_ENGINE_CRATES`], resolved by reading
/// the real crate manifests under `<weft_root>/crates/`. This is the ONLY set
/// the builder base bakes, the worker build context stages, and the worker /
/// infra / builder-base hashes cover; every other workspace crate (CLI,
/// dispatcher, tests, ...) is invisible to worker builds, so editing it never
/// invalidates a worker image or the builder base.
///
/// Resolution walks `[dependencies]` + `[build-dependencies]` (including
/// per-`[target.*]` variants) and follows both direct `path = "..."` entries
/// and `workspace = true` entries whose `[workspace.dependencies]` definition
/// carries a path. Dev-dependencies are deliberately excluded: cargo ignores
/// the dev-dependencies of a path dependency, so they never reach a worker
/// build. Returned sorted for deterministic hashing / staging.
///
/// [`WORKSPACE_HACK_CRATE`] is left out although every closure crate depends
/// on it: the worker build gets an empty stand-in for it
/// (`build::stage_worker_workspace`), so the real one is neither staged nor
/// hashed.
pub fn worker_workspace_crates(weft_root: &Path) -> BuildResult<Vec<String>> {
    let seeds: Vec<String> = WORKER_ENGINE_CRATES.iter().map(|name| (*name).to_string()).collect();
    let mut closure = workspace_crate_closure(weft_root, &seeds)?;
    closure.retain(|name| name != WORKSPACE_HACK_CRATE);
    Ok(closure)
}

/// The crate `cargo hakari` keeps (`.config/hakari.toml`): it turns on, for
/// each third-party dependency, the union of the features any workspace
/// member asks for, so every build on a developer's machine shares one
/// compiled copy of each dependency. A worker must not compile that union
/// (it would pull in the CLI's and the dispatcher's dependencies), so its
/// build gets an empty crate of the same name instead.
pub const WORKSPACE_HACK_CRATE: &str = "weft-workspace-hack";

/// Closure of local path dependencies reachable from `seeds` (crate DIR names
/// under `<weft_root>/crates/`), resolved by reading the real crate manifests.
/// The generic brain behind `worker_workspace_crates` (seeded from the worker's
/// fixed deps) and the CLI's system-image staleness stamps (seeded from the
/// four system binaries). See `worker_workspace_crates` for the resolution
/// rules (dependencies + build-dependencies, dev-dependencies excluded).
pub fn workspace_crate_closure(
    weft_root: &Path,
    seeds: &[String],
) -> BuildResult<Vec<String>> {
    let root_manifest = parse_manifest(&weft_root.join("Cargo.toml"))?;
    // Dependency NAME -> crate DIR name, from the root manifest's
    // `[workspace.dependencies]` path entries, for `workspace = true` deps.
    let mut ws_paths: BTreeMap<String, String> = BTreeMap::new();
    if let Some(deps) = root_manifest
        .get("workspace")
        .and_then(|w| w.get("dependencies"))
        .and_then(|d| d.as_table())
    {
        for (name, spec) in deps {
            if let Some(dir) = spec
                .get("path")
                .and_then(|v| v.as_str())
                .and_then(crate_dir_name)
            {
                ws_paths.insert(name.clone(), dir);
            }
        }
    }

    let mut pending: Vec<String> = seeds.to_vec();
    let mut closure: BTreeSet<String> = BTreeSet::new();
    while let Some(name) = pending.pop() {
        if !closure.insert(name.clone()) {
            continue;
        }
        let manifest = parse_manifest(&weft_root.join("crates").join(&name).join("Cargo.toml"))?;
        for table in dependency_tables(&manifest) {
            for (dep_name, spec) in table {
                if let Some(dir) = spec
                    .get("path")
                    .and_then(|v| v.as_str())
                    .and_then(crate_dir_name)
                {
                    pending.push(dir);
                } else if spec.get("workspace").and_then(|v| v.as_bool()) == Some(true) {
                    if let Some(dir) = ws_paths.get(dep_name) {
                        pending.push(dir.clone());
                    }
                }
            }
        }
    }
    Ok(closure.into_iter().collect())
}

/// Last path component of a dependency `path = "..."` value: every worker-linked
/// local crate lives directly under `crates/`, so the final component IS the
/// crate's directory name whether the path is root-relative (`crates/weft-core`),
/// sibling-relative (`../weft-journal`), or container-absolute
/// (`../weft/crates/weft-engine`).
fn crate_dir_name(path: &str) -> Option<String> {
    Path::new(path)
        .file_name()
        .and_then(|s| s.to_str())
        .map(str::to_string)
}

fn parse_manifest(path: &Path) -> BuildResult<toml::Value> {
    let raw = std::fs::read_to_string(path).map_err(|e| {
        BuildError::Build(format!("read manifest {}: {e}", path.display()))
    })?;
    toml::from_str::<toml::Value>(&raw)
        .map_err(|e| BuildError::Build(format!("parse manifest {}: {e}", path.display())))
}

/// `[dependencies]` + `[build-dependencies]` tables of a crate manifest,
/// including per-`[target.<cfg>]` variants. Dev-dependencies excluded (see
/// `worker_workspace_crates`).
fn dependency_tables(manifest: &toml::Value) -> Vec<&toml::value::Table> {
    let mut out = Vec::new();
    for key in ["dependencies", "build-dependencies"] {
        if let Some(t) = manifest.get(key).and_then(|v| v.as_table()) {
            out.push(t);
        }
    }
    if let Some(targets) = manifest.get("target").and_then(|v| v.as_table()) {
        for cfg_tables in targets.values() {
            for key in ["dependencies", "build-dependencies"] {
                if let Some(t) = cfg_tables.get(key).and_then(|v| v.as_table()) {
                    out.push(t);
                }
            }
        }
    }
    out
}

/// Insert a package- or node-declared cargo dep, MERGING with any
/// already-resolved spec (the runtime baseline or an earlier
/// declaration in the same package). The merge mirrors cargo's own
/// model: if version/path/git and every non-`features` key AGREE, the
/// two `features` lists are UNIONED (so a node can legitimately add a
/// feature to a baseline crate, e.g. ask for serde's `rc` on top of
/// `derive`). A REAL conflict (different version, different source, or a
/// disagreeing non-features key) is a hard build error: silently keeping
/// either spec would let one node's deps.toml clobber a baseline pin and
/// break sibling nodes. Equality is structural, so `serde = "1"` and
/// `serde = { version = "1" }` are the same spec, not a conflict.
fn add_dep(
    deps: &mut BTreeMap<String, toml::Value>,
    pkg_name: &str,
    name: String,
    value: toml::Value,
    source: &str,
) -> BuildResult<()> {
    match deps.remove(&name) {
        Some(existing) => {
            let merged = merge_dep_specs(&existing, &value).map_err(|conflict| {
                BuildError::Build(format!(
                    "package '{pkg_name}': {source} declares {name} = {}, which conflicts with \
                     the already-resolved {name} = {} ({conflict}); align the specs in deps.toml",
                    toml_inline(&value),
                    toml_inline(&existing),
                ))
            })?;
            deps.insert(name, merged);
            Ok(())
        }
        None => {
            deps.insert(name, value);
            Ok(())
        }
    }
}

/// Normalize a cargo dep spec (bare `"1.0"` string or an inline table)
/// to table form, so two spellings of the same spec compare equal.
fn normalize_dep_spec(value: &toml::Value) -> toml::value::Table {
    match value {
        toml::Value::Table(t) => t.clone(),
        other => {
            let mut t = toml::value::Table::new();
            t.insert("version".into(), other.clone());
            t
        }
    }
}

/// Merge two dep specs per cargo's additive-feature model. Returns the
/// merged spec, or an Err(reason) string naming the disagreeing key on a
/// real conflict (version/path/git/source or any non-`features` key).
fn merge_dep_specs(a: &toml::Value, b: &toml::Value) -> Result<toml::Value, String> {
    let ta = normalize_dep_spec(a);
    let tb = normalize_dep_spec(b);

    // The non-`features` key SETS must be identical, with identical
    // values. A key present on only ONE side is a conflict, not a free
    // adoption: `{version="1"}` vs `{version="1", default-features=false}`
    // disagree (implicit default-features is true), `package`/`path`/`git`
    // on one side silently changes the source the other pinned. Only
    // `features` may differ (cargo unions them).
    let non_feature_keys = |t: &toml::value::Table| -> std::collections::BTreeSet<String> {
        t.keys().filter(|k| *k != "features").cloned().collect()
    };
    let keys_a = non_feature_keys(&ta);
    let keys_b = non_feature_keys(&tb);
    for key in keys_a.union(&keys_b) {
        match (ta.get(key), tb.get(key)) {
            (Some(av), Some(bv)) if av != bv => {
                return Err(format!(
                    "differing `{key}`: {} vs {}",
                    toml_inline(av),
                    toml_inline(bv)
                ));
            }
            (Some(_), Some(_)) => {}
            // Present on exactly one side.
            (Some(only), None) | (None, Some(only)) => {
                return Err(format!(
                    "`{key}` = {} is declared by only one spec; both specs must agree on every \
                     non-feature key",
                    toml_inline(only)
                ));
            }
            (None, None) => unreachable!("key came from the union of both"),
        }
    }

    let mut merged = ta.clone();

    // Union the feature lists (dedup, order-stable: a's features then
    // b's new ones).
    let features_of = |t: &toml::value::Table| -> Vec<toml::Value> {
        t.get("features")
            .and_then(|f| f.as_array())
            .cloned()
            .unwrap_or_default()
    };
    let mut feats = features_of(&ta);
    for f in features_of(&tb) {
        if !feats.contains(&f) {
            feats.push(f);
        }
    }
    if feats.is_empty() {
        merged.remove("features");
    } else {
        merged.insert("features".into(), toml::Value::Array(feats));
    }

    // Collapse back to the bare-string form when the only key is
    // `version` (keeps generated Cargo.toml tidy and matches how
    // baselines without features are written).
    if merged.len() == 1 {
        if let Some(v @ toml::Value::String(_)) = merged.get("version") {
            return Ok(v.clone());
        }
    }
    Ok(toml::Value::Table(merged))
}

/// The binary crate's `src/registry.rs`: the `NodeCatalog` over its
/// package crates. Under [`PackageGating::Features`] each package's arms
/// and `all()` entries compile only with its feature, and `left_out`
/// answers from the record the build embedded (`left-out.json`), so a
/// worker built without a package names it and why instead of knowing
/// nothing of its types. The generated code warns neither with every
/// feature on nor with none.
fn write_registry_rs(
    src_dir: &Path,
    packages: &[PackageEmit<'_>],
    gating: PackageGating,
) -> BuildResult<()> {
    let gate = |pkg: &PackageEmit<'_>, indent: &str| match gating {
        PackageGating::Features => format!("{indent}#[cfg(feature = \"{}\")]\n", pkg.module_ident),
        PackageGating::Always => String::new(),
    };
    let mut body = String::new();
    body.push_str("//! Per-project NodeCatalog. Dispatches node_type strings to\n");
    body.push_str("//! the node structs in each `pkg_<name>` crate. Each package\n");
    body.push_str("//! is its own cargo crate, so editing one node's source\n");
    body.push_str("//! recompiles only that crate; the registry below relinks.\n\n");
    body.push_str("use weft_core::node::{Node, NodeCatalog};\n\n");
    body.push_str("pub struct ProjectCatalog;\n\n");
    body.push_str("impl NodeCatalog for ProjectCatalog {\n");
    body.push_str("    fn lookup(&self, node_type: &str) -> Option<&'static dyn Node> {\n");
    body.push_str("        match node_type {\n");
    for pkg in packages {
        for node_type in &pkg.node_types {
            let struct_mod = weft_catalog::node_module_name(node_type);
            body.push_str(&gate(pkg, "            "));
            body.push_str(&format!(
                "            \"{nt}\" => Some(&{pkg_mod}::{struct_mod}::{nt}Node as &'static dyn Node),\n",
                nt = node_type,
                pkg_mod = pkg.module_ident,
                struct_mod = struct_mod,
            ));
        }
    }
    body.push_str("            _ => None,\n");
    body.push_str("        }\n");
    body.push_str("    }\n");
    body.push_str("    fn all(&self) -> Vec<&'static str> {\n");
    match gating {
        PackageGating::Always => {
            body.push_str("        vec![");
            for pkg in packages {
                for node_type in &pkg.node_types {
                    body.push_str(&format!("\"{node_type}\", "));
                }
            }
            body.push_str("]\n");
        }
        PackageGating::Features => {
            // `mut` is unused when every feature is off.
            body.push_str("        #[allow(unused_mut)]\n        let mut all = Vec::new();\n");
            for pkg in packages {
                let types: Vec<String> = pkg.node_types.iter().map(|t| format!("\"{t}\"")).collect();
                body.push_str(&gate(pkg, "        "));
                body.push_str(&format!("        all.extend([{}]);\n", types.join(", ")));
            }
            body.push_str("        all\n");
        }
    }
    body.push_str("    }\n");
    if gating == PackageGating::Features {
        // The record is this build's own output: one that does not read
        // is a build bug, and the worker must not run past it.
        body.push_str(&format!(
            "    fn left_out(&self) -> &'static weft_core::builds::LeftOut {{\n\
             \x20       static RECORD: std::sync::OnceLock<weft_core::builds::LeftOut> = std::sync::OnceLock::new();\n\
             \x20       RECORD.get_or_init(|| {{\n\
             \x20           weft_core::builds::LeftOut::parse(include_bytes!(\"../{LEFT_OUT_RECORD_FILE}\"))\n\
             \x20               .unwrap_or_else(|e| panic!(\"{{e}}\"))\n\
             \x20       }})\n\
             \x20   }}\n"
        ));
    }
    body.push_str("}\n\n");
    body.push_str("pub static PROJECT_CATALOG: ProjectCatalog = ProjectCatalog;\n");

    std::fs::write(src_dir.join("registry.rs"), body).map_err(BuildError::Io)?;
    Ok(())
}


// Each package crate is an EXTERNAL cargo dep (path = "./pkg_<name>-<slot>"
// in Cargo.toml), so the worker binary just uses them as crates and
// main.rs needs no package knowledge (no `mod pkg_<name>;`), but it
// bakes the project's TYPE declarations: node metadata embedded by
// `#[derive(NodeManifest)]` holds bare declared names (`ChatHistory`),
// so the process-wide registry must be installed before any manifest
// parse. The baked table is the catalog's resolved nominal entries.
/// The baked type-declaration rows a generated `main.rs` installs into
/// the process-wide `TypeRegistry` before anything parses embedded
/// node metadata. Shared by the worker main and the test-runner main:
/// both host `#[derive(NodeManifest)]` expansions whose port types may
/// name declared types.
fn baked_type_decls(catalog: &FsCatalog) -> String {
    catalog
        .type_registry()
        .nominal_entries()
        .iter()
        .map(|(name, body)| {
            format!(
                "        ({name:?}.to_string(), {body:?}.to_string(), \"baked\".to_string()),\n"
            )
        })
        .collect()
}

fn write_main_rs(src_dir: &Path, catalog: &FsCatalog) -> BuildResult<()> {
    std::fs::write(src_dir.join("main.rs"), worker_main_source(&baked_type_decls(catalog))).map_err(BuildError::Io)?;
    Ok(())
}

/// The worker binary's `main.rs`, around the project's baked type
/// declarations (`type_decls`, one registry entry per line).
fn worker_main_source(type_decls: &str) -> String {
    format!(
        r#"//! Project worker binary. Weft calls it once per run
//! (`weft_engine::worker`): it serves HTTP until it is told to stop.

use std::sync::Arc;

use clap::Parser;

use weft_broker_client::TokenSource;
use weft_core::NodeCatalog;
use weft_engine::EngineClients;

mod registry;

// jemalloc hands freed memory back to the system, so a worker past a
// burst reads as using what it uses (glibc's allocator kept it, and the
// worker turned every call away as full while idle).
#[global_allocator]
static GLOBAL: weft_engine::Jemalloc = weft_engine::Jemalloc;

// SYNC: the worker's environment <-> crates/weft-platform-local/src/runner.rs,
//       crates/weft-platform-gcp/src/runner.rs (what each sets),
//       crates/weft-engine/src/test_runner.rs (Args::Serve run_cap_secs reads WEFT_RUN_CAP_SECS too),
//       crates/weft-core/src/needs.rs (NEEDS_FROM_ENV, read by the engine's process settings)
#[derive(Debug, Parser)]
#[command(name = "weft-project-worker", version)]
struct Args {{
    /// The project this worker serves.
    #[arg(long, env = "WEFT_PROJECT_ID")]
    project_id: uuid::Uuid,

    /// The tenant the project belongs to, stamped on every task this
    /// worker enqueues.
    #[arg(long, env = "WEFT_TENANT_ID")]
    tenant_id: String,

    /// The broker's address. The worker never touches Postgres; every
    /// journal write, task claim and infra read goes through the broker,
    /// which checks the worker's identity and scopes it to its project.
    #[arg(long, env = "WEFT_BROKER_URL")]
    broker_url: String,

    /// The port the worker serves HTTP on.
    #[arg(long, env = "PORT", default_value = "8080")]
    port: u16,

    /// The binary hash of this worker's image: its door serves the routes
    /// armed for it.
    #[arg(long, env = "WEFT_BINARY_HASH")]
    binary_hash: String,

    /// The platform's hard cap on one run, in seconds, when it has one.
    #[arg(long, env = "WEFT_RUN_CAP_SECS")]
    run_cap_secs: Option<u64>,

    /// The platform may stop a worker no call holds open: a run that outlives
    /// its caller is handed back once it leaves, to carry on under weft's call.
    #[arg(long, env = "WEFT_RESUME_WHEN_CALLER_LEAVES")]
    resume_when_caller_leaves: bool,
}}

#[tokio::main]
async fn main() -> anyhow::Result<()> {{
    // Colours only on a terminal: a platform's log collector keeps the
    // escape codes as text.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "weft_engine=info,weft_core=info".into()),
        )
        .with_ansi(std::io::IsTerminal::is_terminal(&std::io::stdout()))
        .init();

    let args = Args::parse();
    // A CPU profile of this worker, when `WEFT_PROFILE` asks for one.
    weft_engine::profile::start_from_env()?;

    // The project's type declarations, baked at codegen time. Installed
    // before anything parses node metadata (embedded metadata holds
    // bare declared type names).
    weft_core::weft_type::TypeRegistry::install(
        weft_core::weft_type::TypeRegistry::build(&[
{type_decls}    ])
        .expect("baked type declarations were validated at build time"),
    )
    .expect("first registry install in this process");

    let replica = weft_engine::mint_replica_id("worker");
    let token = TokenSource::worker(weft_engine::identity_from_env()?, replica.clone());
    // The engine composes its own client bundle from the broker address and
    // the worker's identity, so this generated binary never names the
    // bundle's fields.
    let clients = EngineClients::from_broker(
        &args.broker_url,
        token,
        args.project_id,
        weft_engine::ProcessSettings::from_env()?,
    )?;
    let catalog = Arc::new(CatalogRef) as Arc<dyn NodeCatalog>;

    // This project's secret: what everything this worker signs or checks
    // is keyed from.
    let secret = weft_core::caller_token::ProjectSecret::from_env().map_err(anyhow::Error::msg)?;
    let config = weft_engine::WorkerConfig {{
        project_id: args.project_id,
        tenant_id: args.tenant_id,
        replica,
        binary_hash: args.binary_hash,
        edge: weft_engine::door::Edge::from_env()?,
        secret,
        port: args.port,
        run_cap: args.run_cap_secs.map(std::time::Duration::from_secs),
        resume_when_caller_leaves: args.resume_when_caller_leaves,
    }};
    weft_engine::serve(catalog, clients, config).await?;
    tracing::info!(target: "weft_project_worker", "worker exit");
    Ok(())
}}

struct CatalogRef;

impl NodeCatalog for CatalogRef {{
    fn lookup(&self, node_type: &str) -> Option<&'static dyn weft_core::Node> {{
        registry::PROJECT_CATALOG.lookup(node_type)
    }}
    fn all(&self) -> Vec<&'static str> {{
        registry::PROJECT_CATALOG.all()
    }}
    fn left_out(&self) -> &'static weft_core::builds::LeftOut {{
        registry::PROJECT_CATALOG.left_out()
    }}
}}
"#,
    )
}


/// What [`emit_test_crate`] produced: the identities the caller needs
/// to run and report the package's tests.
#[derive(Debug, Clone)]
pub struct TestCrate {
    /// The emitted binary's name (`<package>_tests`).
    pub binary_name: String,
    /// The package crate's folder, wherever the emission put it (under
    /// the crate root, or for a host build in the machine-wide folder).
    pub package_crate: PathBuf,
    /// Every node type the package declares (the test registry's set).
    pub node_types: Vec<String>,
}

/// The dev profile every node-test workspace build (`EmitPaths::Local`)
/// uses. (A live test's server is a standalone host build compiled
/// `--release`, so it never reads a dev profile and does not carry one.)
///
/// Cargo's own dev defaults are wrong for an emission and were what
/// let these builds grow into tens of gigabytes:
///   - full debuginfo put 286MB of debug sections in a 333MB test
///     binary, once per package. `line-tables-only` keeps the
///     file:line a failing test's backtrace is read by and drops the
///     rest, which is most of it.
///   - incremental state only pays off when somebody edits a crate and
///     rebuilds it. Nobody edits an emission (it is regenerated from
///     scratch every run), so every byte of it was dead weight.
///
/// SYNC: test-build dev profile <-> the weft workspace root's
/// [profile.dev] in Cargo.toml, which an emission cannot inherit
/// because it is not a member of that workspace.
const DEV_PROFILE_BLOCK: &str = r#"[profile.dev]
debug = "line-tables-only"
incremental = false
"#;

/// What a STANDALONE emission (the worker crate, and a host test
/// server's crate: one crate per build) puts above its `[package]`:
/// its own workspace, so an emission living under some other cargo
/// workspace (a project inside a workspace dir) is not claimed by it
/// and refused a build.
const STANDALONE_WORKSPACE_HEADER: &str = "\
# Self-contained: without this, an emission living under some other
# cargo workspace (a project inside a workspace dir) would be claimed
# by it and refuse to build. Path-dep subcrates auto-join this
# workspace.
[workspace]

";

/// Write the workspace root the host's node-test crates are members
/// of: one `Cargo.toml` listing `members`, the toolchain pin, and a
/// seeded `Cargo.lock`.
///
/// ONE workspace rather than one per package, because cargo resolves
/// features per workspace: 28 separate workspaces sharing a target dir
/// meant the same dependency was compiled once per distinct feature
/// union (ten copies of one database driver, all from one run). Members
/// unify, so each dependency is built once.
///
/// `members` is what THIS run emitted, not every package in the
/// project, which is what keeps a broken sibling harmless: a package
/// nobody targeted is not a member, so it cannot fail the resolve.
pub fn emit_test_workspace(root: &Path, members: &[String]) -> BuildResult<()> {
    std::fs::create_dir_all(root).map_err(BuildError::Io)?;
    let weft_root = crate::build::resolve_weft_root()?;
    write_rust_toolchain(root, &weft_root)?;
    // Seed the Cargo.lock so the build resolves the versions the weft
    // workspace already pins for every dep the lock covers (a node dep
    // OUTSIDE the lock still resolves fresh; only the shared engine
    // graph is pinned). Overwritten on EVERY run, so the cache follows
    // the installed weft: a lock left over from an older install would
    // keep building the engine's old dependency versions beside the
    // new ones, which is exactly the growth this cache exists to stop.
    // A standalone build seeds its own (`HostRecipe::seed_lock`, or the
    // Dockerfile's `cp`).
    let lock_src = weft_root.join("Cargo.lock");
    std::fs::copy(&lock_src, root.join("Cargo.lock")).map_err(|e| {
        BuildError::Build(format!("seed Cargo.lock from {}: {e}", lock_src.display()))
    })?;
    let member_list = members
        .iter()
        .map(|m| format!("    \"{}\",\n", escape_quoted_str(m)))
        .collect::<String>();
    let contents = format!(
        "# Emitted by weft codegen: the workspace this run's node-test\n\
         # crates are members of. Do not edit by hand; rewritten on every\n\
         # test build.\n\
         \n\
         [workspace]\n\
         resolver = \"2\"\n\
         members = [\n{member_list}]\n\
         \n\
         {DEV_PROFILE_BLOCK}"
    );
    std::fs::write(root.join("Cargo.toml"), contents).map_err(BuildError::Io)?;
    Ok(())
}

/// Emit the per-PACKAGE node-test crate at `target_root`:
///
/// ```text
/// target_root/
///   Cargo.toml            test-runner deps + the one pkg dep, with
///                         the `node-tests` feature ENABLED
///   rust-toolchain.toml
///   src/main.rs           installs the baked type registry, then
///                         hands off to weft_engine::test_runner
///   src/registry.rs       NodeCatalog over the package's nodes
///   pkg_<name>/           the package crate (same shim shape as the
///                         worker's, compiled WITH `node-tests`)
/// ```
///
/// Needs ONLY the catalog walk: no `ProjectDefinition`, no graph
/// parse, no validation of other packages. A broken `main.weft` or a
/// broken sibling package never blocks testing the package under
/// work. Packages must not depend on other packages; a violation
/// fails this crate's compile, which is the loud failure we want.
pub fn emit_test_crate(
    target_root: &Path,
    catalog: &FsCatalog,
    package_name: &str,
    paths: &EmitPaths,
) -> BuildResult<TestCrate> {
    let Some(package) = catalog.packages().find(|p| p.name == package_name) else {
        let mut known: Vec<&str> = catalog.packages().map(|p| p.name.as_str()).collect();
        known.sort();
        return Err(BuildError::Build(format!(
            "no package named '{package_name}' in this project (nodes/ or src/) (known: {})",
            known.join(", ")
        )));
    };
    let mut node_types = package.node_types.clone();
    node_types.sort();
    let pkg = PackageEmit {
        package,
        node_types: node_types.clone(),
        module_ident: sanitize_pkg_ident(&package.name),
    };

    // The emitted crate is a pure function of its inputs: wipe any
    // previous emission first so a renamed package or a dropped node
    // can never leave stale files (an old Cargo.toml, an orphaned
    // package dir) inside the crate cargo compiles.
    if target_root.exists() {
        std::fs::remove_dir_all(target_root).map_err(BuildError::Io)?;
    }
    let src_dir = target_root.join("src");
    std::fs::create_dir_all(&src_dir).map_err(BuildError::Io)?;
    // The host path builds ONE crate on its own (a live test's server,
    // a program on the machine), so it carries its own workspace and
    // toolchain pin. The local path is a MEMBER of the shared workspace
    // `emit_test_workspace` writes (see there for why), which owns those
    // and the dev profile instead. No test crate is built in an image.
    let standalone = match paths {
        EmitPaths::Host { .. } => true,
        EmitPaths::Local { .. } => false,
        EmitPaths::Container { .. } => {
            return Err(BuildError::Build(format!(
                "the test crate of '{package_name}' was asked for an image build; a node-test crate compiles on the machine (a host or local build)"
            )))
        }
    };
    if standalone {
        let weft_root = crate::build::resolve_weft_root()?;
        write_rust_toolchain(target_root, &weft_root)?;
        // Built the way a worker is, by `weft-isolate` with the cache
        // sweep after it: without the sweep script, every test build
        // reported a failed sweep and the test crates' per-package
        // artifacts never expired from the shared cache.
        write_image_build_files(target_root, Default::default())?;
    }

    let binary_name = format!("{}_tests", crate::build::sanitize_crate_name(&package.name));
    let crate_dirs = write_package_crates(target_root, catalog, std::slice::from_ref(&pkg), paths)?;

    // Root Cargo.toml: only what the generated main names directly
    // (weft-core for the registry, weft-engine for the runner) plus
    // the package crate with its tests compiled in. Everything else
    // arrives transitively with the workspace's unified versions.
    let mut deps: BTreeMap<String, toml::Value> = BTreeMap::new();
    insert_dep(&mut deps, "weft-core", toml::Value::Table(paths.weft_crate_dep("weft-core", 1)));
    insert_dep(&mut deps, "weft-engine", {
        let mut t = paths.weft_crate_dep("weft-engine", 1);
        t.insert(
            "features".into(),
            toml::Value::Array(vec![toml::Value::String("node-tests".into())]),
        );
        toml::Value::Table(t)
    });
    insert_dep(&mut deps, &pkg.module_ident, {
        let mut t = path_table(&package_crate_path(paths, &crate_dirs[&pkg.module_ident]));
        t.insert(
            "features".into(),
            toml::Value::Array(vec![toml::Value::String("node-tests".into())]),
        );
        toml::Value::Table(t)
    });
    let mut deps_fragment = String::new();
    for (name, value) in deps {
        deps_fragment.push_str(&format!("{name} = {}\n", toml_inline(&value)));
    }
    let cargo_toml = format!(
        r#"# Emitted by weft codegen: the `{pkg_name}` package's node-test
# crate. Do not edit by hand; regenerated on every test build.

{workspace_block}[package]
name = "{binary_name}"
version = "0.1.0"
edition = "2021"

[[bin]]
name = "{binary_name}"
path = "src/main.rs"

[dependencies]
{deps}"#,
        pkg_name = package.name,
        deps = deps_fragment,
        workspace_block = if standalone { STANDALONE_WORKSPACE_HEADER } else { "" },
    );
    std::fs::write(target_root.join("Cargo.toml"), cargo_toml).map_err(BuildError::Io)?;

    write_registry_rs(&src_dir, std::slice::from_ref(&pkg), PackageGating::Always)?;

    let type_decls = baked_type_decls(catalog);
    let main_rs = format!(
        r#"//! Per-package node-test binary. All runner logic lives in
//! `weft_engine::test_runner`; this file only bakes the project's
//! type declarations and the package's node registry.

mod registry;

fn main() -> std::process::ExitCode {{
    // Embedded node metadata may name declared types; the registry
    // must be installed before any manifest parses.
    weft_core::weft_type::TypeRegistry::install(
        weft_core::weft_type::TypeRegistry::build(&[
{type_decls}    ])
        .expect("baked type declarations were validated at emit time"),
    )
    .expect("first registry install in this process");

    weft_engine::test_runner::main(&registry::PROJECT_CATALOG)
}}
"#,
    );
    std::fs::write(src_dir.join("main.rs"), main_rs).map_err(BuildError::Io)?;

    Ok(TestCrate {
        binary_name,
        package_crate: paths.package_crates_dir(target_root).join(&crate_dirs[&pkg.module_ident]),
        node_types,
    })
}

#[cfg(test)]
pub(crate) mod tests {
    use super::collect_node_types;
    use super::{fixed_worker_deps, worker_workspace_crates};

    /// The worker crate closure on THIS repo. Not a tautology check: the
    /// closure is computed from the real manifests, so this pins the expected
    /// result and forces a conscious look when a workspace crate enters or
    /// leaves the worker's dependency tree (each entry widens what the builder
    /// base bakes and what invalidates every worker image).
    #[test]
    fn worker_crate_closure_is_the_expected_set() {
        let weft_root = crate::build::resolve_weft_root().expect("resolve weft root");
        let closure = worker_workspace_crates(&weft_root).expect("compute closure");
        assert_eq!(
            closure,
            vec![
                "weft-broker-client".to_string(),
                "weft-core".to_string(),
                "weft-engine".to_string(),
                "weft-journal".to_string(),
                "weft-node-derive".to_string(),
                // A worker on Cloud Run mints its own identity tokens
                // (`weft_platform_gcp::MetadataTokens`, never the control
                // side, which stays behind the crate's `control` feature).
                "weft-platform-gcp".to_string(),
                "weft-platform-traits".to_string(),
                "weft-providers".to_string(),
                "weft-task-store".to_string(),
            ],
            "worker crate closure changed; confirm the new set is really \
             worker-linked, then update this pin"
        );
    }

    /// A real worker's dependency set is `fixed_worker_deps` PLUS one path dep per
    /// referenced node package, and nothing else: nodes ADD on top of the shared
    /// base, they never alter it. (Drives `write_worker_cargo_toml` indirectly via
    /// the shared `fixed_worker_deps`; here we assert the base set is what a
    /// node-LESS worker carries.)
    #[test]
    fn fixed_worker_deps_is_the_node_independent_baseline() {
        let deps = fixed_worker_deps(&super::EmitPaths::Container { project_root: "/project".into() });
        // The engine + the always-present runtime crates are present...
        for required in ["weft-engine", "weft-core", "weft-broker-client", "tokio", "clap"] {
            assert!(deps.contains_key(required), "fixed deps missing {required}");
        }
        // ...and nothing project-specific is.
        assert!(
            !deps.keys().any(|k| k.starts_with("pkg_")),
            "fixed_worker_deps must be project-independent (no pkg_<node>)"
        );
    }

    use weft_core::project::{NodeDefinition, ProjectDefinition, Position};
    use weft_core::NodeFeatures;

    fn make_node(id: &str, node_type: &str) -> NodeDefinition {
        NodeDefinition {
            id: id.into(),
            node_type: node_type.into(),
            type_params: Vec::new(),
            place_types: Vec::new(),
            label: None,
            config: serde_json::Value::Object(Default::default()),
            position: Position { x: 0.0, y: 0.0 },
            inputs: Vec::new(),
            outputs: Vec::new(),
            features: NodeFeatures::default(),
            scope: Vec::new(),
            group_boundary: None,
            requires_infra: false, per_instance: None,
            images: Vec::new(),
            fires_with: Default::default(), baked_outputs: Default::default(),
            published_service: None,
            instance_service: None,
            instance_rules: None,
            span: None,
            header_span: None,
            config_spans: Default::default(),
            optional_ports: Default::default(),
            port_literals: Default::default(),
            port_literal_spans: Default::default(),
            port_literal_types: Default::default(),
            metadata_ports: Vec::new(),
            file_refs: Default::default(),
            include_path: None,
            include_contents: None,
            source_file: None,
        }
    }

    /// Regression: codegen's `collect_node_types` used to return every
    /// node's `node_type` verbatim, including runtime-internal built-ins
    /// (LoopIn, LoopOut). Downstream callers (worker-image
    /// build, source-hash) then fed those to the catalog and the build
    /// failed with `node 'LoopIn' not found in catalog`. The filter
    /// must drop those before the catalog lookup.
    #[test]
    fn collect_node_types_excludes_runtime_builtins() {
        let project = ProjectDefinition {
            id: uuid::Uuid::nil(),
            nodes: vec![
                make_node("user_node", "Text"),
                make_node("my_loop__in", "LoopIn"),
                make_node("my_loop__out", "LoopOut"),
            ],
            edges: Vec::new(),
            groups: Vec::new(),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            defaults: Default::default(),
            given: Default::default(),
            broken: Vec::new(),
            metadata_wires: Vec::new(),
        };
        let types = collect_node_types(&project);
        assert!(types.contains("Text"), "user catalog type included: {types:?}");
        assert!(!types.contains("LoopIn"), "LoopIn excluded: {types:?}");
        assert!(!types.contains("LoopOut"), "LoopOut excluded: {types:?}");
    }

    /// A node tree under `dir/nodes`: `Run` needs python3 and a build-time
    /// library, `Plain` needs nothing, `Unused` needs ffmpeg.
    pub(crate) fn catalog_with_needs(dir: &std::path::Path) -> weft_catalog::FsCatalog {
        let node = |name: &str, node_type: &str, deps: Option<&str>| {
            let at = dir.join("nodes").join(name);
            std::fs::create_dir_all(&at).unwrap();
            std::fs::write(at.join("metadata.json"), format!(r#"{{"type": "{node_type}", "label": "{node_type}", "description": "d", "inputs": [], "outputs": []}}"#)).unwrap();
            std::fs::write(at.join("mod.rs"), "// impl\n").unwrap();
            if let Some(deps) = deps {
                std::fs::write(at.join("deps.toml"), deps).unwrap();
            }
        };
        node("run", "Run", Some(concat!(
            "[[needs]]\nname = \"python3\"\ncheck = [\"python3\", \"--version\"]\n",
            "[needs.install.apt]\ndefault = [\"python3\"]\n[needs.install.dnf]\ndefault = [\"python3-libs\"]\n\n",
            "[[needs]]\nname = \"libvips\"\nstage = \"build\"\ncheck = [\"pkg-config\", \"--exists\", \"vips\"]\n",
            "[needs.install.apt]\ndefault = [\"libvips-dev\"]\n\n",
            "[[needs]]\nname = \"say\"\ncheck = [\"say\", \"-v\", \"?\"]\n[needs.install.brew]\ndefault = [\"say\"]\n",
        )));
        node("plain", "Plain", None);
        node("unused", "Unused", Some("[[needs]]\nname = \"ffmpeg\"\ncheck = [\"ffmpeg\", \"-version\"]\n[needs.install.apt]\ndefault = [\"ffmpeg\"]\n"));
        weft_catalog::FsCatalog::discover(&dir.join("nodes")).unwrap()
    }

    /// The needs of a program are those of the node types it uses, each
    /// with the program's nodes of that type; a catalog node it does not
    /// use is never asked about, nor a type weft gives its own nodes.
    #[test]
    fn needs_of_names_only_the_types_the_program_uses() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = catalog_with_needs(dir.path());
        let mut program = ProjectDefinition {
            id: uuid::Uuid::nil(),
            nodes: vec![make_node("greet", "Run"), make_node("plain", "Plain"), make_node("again", "Run"), make_node("l__in", "LoopIn")],
            edges: Vec::new(),
            groups: Vec::new(),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            defaults: Default::default(),
            given: Default::default(),
            broken: Vec::new(),
            metadata_wires: Vec::new(),
        };
        let rows = super::needs_of(&program, &catalog).unwrap();
        let seen: Vec<(&str, &str, Vec<&str>)> =
            rows.iter().map(|r| (r.node_type.as_str(), r.need.name.as_str(), r.nodes.iter().map(String::as_str).collect())).collect();
        assert_eq!(seen, [
            ("Run", "python3", vec!["again", "greet"]),
            ("Run", "say", vec!["again", "greet"]),
            ("Run", "libvips", vec!["again", "greet"]),
        ]);
        program.nodes.retain(|n| n.node_type == "Plain");
        assert!(super::needs_of(&program, &catalog).unwrap().is_empty());
    }

    use super::{add_dep, merge_dep_specs};
    use std::collections::BTreeMap;

    fn tbl(s: &str) -> toml::Value {
        toml::from_str::<toml::Value>(&format!("x = {s}")).unwrap()["x"].clone()
    }

    #[test]
    fn merge_unions_features_of_same_version() {
        // serde with derive, plus a node asking for rc on top -> union.
        let merged = merge_dep_specs(
            &tbl(r#"{ version = "1", features = ["derive"] }"#),
            &tbl(r#"{ version = "1", features = ["rc", "derive"] }"#),
        )
        .expect("compatible specs merge");
        let feats: Vec<&str> = merged["features"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect();
        assert_eq!(feats, vec!["derive", "rc"], "features unioned, deduped");
        assert_eq!(merged["version"].as_str(), Some("1"));
    }

    #[test]
    fn merge_treats_bare_and_table_version_as_equal() {
        // `serde = "1"` vs `serde = { version = "1" }` is NOT a conflict.
        let merged = merge_dep_specs(&tbl(r#""1""#), &tbl(r#"{ version = "1" }"#))
            .expect("same version, different spelling");
        assert_eq!(merged.as_str(), Some("1"), "collapses to bare string");
    }

    #[test]
    fn merge_errors_on_real_version_conflict() {
        let err = merge_dep_specs(&tbl(r#""1""#), &tbl(r#""2""#))
            .expect_err("different versions conflict");
        assert!(err.contains("version"), "{err}");
    }

    #[test]
    fn merge_errors_on_one_sided_non_feature_key() {
        // default-features on one side only: implicit true vs explicit
        // false is a real disagreement, not a free adoption.
        let err = merge_dep_specs(
            &tbl(r#"{ version = "1" }"#),
            &tbl(r#"{ version = "1", default-features = false }"#),
        )
        .expect_err("one-sided default-features is a conflict");
        assert!(err.contains("default-features"), "{err}");

        // A path/package on one side silently changes the source.
        let err = merge_dep_specs(&tbl(r#""1""#), &tbl(r#"{ path = "../x" }"#))
            .expect_err("one-sided path is a conflict");
        assert!(err.contains("path") || err.contains("version"), "{err}");
    }

    #[test]
    fn add_dep_unions_against_baseline() {
        let mut deps: BTreeMap<String, toml::Value> = BTreeMap::new();
        deps.insert("serde".into(), tbl(r#"{ version = "1", features = ["derive"] }"#));
        add_dep(
            &mut deps,
            "pkg",
            "serde".into(),
            tbl(r#"{ version = "1", features = ["rc"] }"#),
            "node Foo",
        )
        .expect("additive feature merges, not errors");
        let feats: Vec<&str> = deps["serde"]["features"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect();
        assert_eq!(feats, vec!["derive", "rc"]);
    }

    /// The worker binary runs on jemalloc, declared by its own `main`
    /// through the engine's re-export, so a project's manifest names
    /// nothing new and nothing else linking the engine changes allocator.
    #[test]
    fn the_worker_main_declares_jemalloc_as_its_allocator() {
        let main = super::worker_main_source("");
        assert!(
            main.contains("#[global_allocator]\nstatic GLOBAL: weft_engine::Jemalloc = weft_engine::Jemalloc;"),
            "{main}"
        );
    }

    /// The worker main starts the CPU profile `WEFT_PROFILE` asks for, before
    /// anything else of the worker runs.
    #[test]
    fn the_worker_main_starts_the_profile_its_environment_asks_for() {
        let main = super::worker_main_source("");
        let profile = main.find("weft_engine::profile::start_from_env()?").expect("the profile starts");
        assert!(profile < main.find("EngineClients::from_broker").unwrap(), "{main}");
    }

    #[test]
    fn add_dep_still_errors_on_conflict() {
        let mut deps: BTreeMap<String, toml::Value> = BTreeMap::new();
        deps.insert("tokio".into(), tbl(r#""1""#));
        let err = add_dep(&mut deps, "pkg", "tokio".into(), tbl(r#""2""#), "node Bar")
            .expect_err("version conflict is a hard error");
        assert!(matches!(err, super::BuildError::Build(_)));
    }
}
