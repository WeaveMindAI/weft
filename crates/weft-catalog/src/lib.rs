//! Filesystem-backed node catalog.
//!
//! The catalog is the project's `nodes/` directory. It is the single
//! source of truth for every node: the stdlib is cloned in at
//! `weft new`, so build / parse / run never reach outside the project.
//! A unit found while walking `nodes/` is one of two shapes:
//!
//! - **Bare node**: a directory with `metadata.json` at its root. The
//!   directory IS the node. No `package.toml`. Files: `metadata.json`,
//!   `mod.rs`, `deps.toml` (optional).
//!
//! - **Package**: a directory with `package.toml` at its root. Member
//!   nodes are auto-detected (every immediate subdir with a
//!   `metadata.json`); the author never maintains a node list.
//!   `package.toml` only names the package and carries shared cargo
//!   deps. The package root can also hold shared Rust files (`.rs`)
//!   accessible to every member via `use super::<name>;`, plus an
//!   optional PARTIAL `metadata.json` of defaults (shared keys like
//!   `provider` or `portsFromConfig`) every member inherits key-by-key;
//!   a member's own key, when present, wins.
//!
//! Units can sit at any depth under `nodes/`: directly under it or ten
//! levels deep. The walk recurses until it hits a unit, then stops
//! descending. A unit never nests inside another unit. Two units
//! declaring the same `node_type` is an ambiguous collision and fails
//! loudly (no shadowing: there is only one source).
//!
//! The compiler uses this crate to look up node metadata without
//! compiling node Rust code. The emitted project binary compiles node
//! code directly via `#[path]` includes driven by codegen; it does NOT
//! use this crate at runtime.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use weft_core::is_rust_identifier;
use weft_core::node::{MetadataCatalog, NodeMetadata};

/// Entry names that are never part of a node's source tree: build
/// outputs, VCS/dependency caches, local secrets and databases. The
/// runtime image carries weft's `catalog/` and `crates/` minus exactly
/// these (`.dockerignore`), and a dispatcher hashes the standard
/// library from that copy while the CLI hashes it from the checkout,
/// so every walk leaving out the same entries is what makes the two
/// agree on one worker hash. The single policy shared
/// by every traversal of a node directory tree (discovery's descent,
/// the build's staging copy, and the source-hash walk) so they agree
/// on exactly which bytes constitute a node. Diverging here is how a
/// stale worker image gets served (hash misses a file the build
/// copies) or a build context bloats (copies a cache the worker never
/// compiles). Symlinks are FOLLOWED by every node-tree walk through
/// `node_tree_entry_kind` (so a project's `nodes/base_catalog` can be
/// a symlink into a weft checkout's `catalog/`, e.g. this repo's own
/// `examples/`); the recursive walks refuse symlink cycles through
/// `guard_node_tree_cycle` (a descent-chain check) and fail loudly.
// SYNC: NODE_TREE_EXCLUDE + NODE_TREE_EXCLUDE_SUFFIXES <-> .dockerignore
//       (the "Even inside the allowlisted dirs" block)
// Only names that are never a real node directory: `pkg` (wasm-pack
// output) stays out because a package may well be called that.
pub const NODE_TREE_EXCLUDE: &[&str] = &["target", "node_modules", ".git", ".weft", ".svelte-kit", ".env"];

/// Name endings excluded the same way (a local SQLite database and its
/// side files).
pub const NODE_TREE_EXCLUDE_SUFFIXES: &[&str] = &[".db", ".db-journal", ".db-shm", ".db-wal"];

/// True if an entry of this name, file or directory, is never part of
/// a node tree. THE check every node-tree walk makes.
pub fn is_node_tree_excluded(name: &str) -> bool {
    NODE_TREE_EXCLUDE.contains(&name)
        || NODE_TREE_EXCLUDE_SUFFIXES.iter().any(|s| name.ends_with(s))
        || is_python_cache(name)
}

// SYNC: is_python_cache <-> .dockerignore (the `__pycache__` / `*.pyc` lines)
/// True if an entry of this name is what Python writes beside a module
/// the first time it imports it (`__pycache__/`, a `.pyc`). Never
/// source, and leaving it in would make a machine that ran a node hash
/// the same tree differently from one that did not, and build again for
/// nothing. Asked at any depth, by every walk that decides what a
/// project or a node is made of.
pub fn is_python_cache(name: &str) -> bool {
    name == "__pycache__" || name.ends_with(".pyc")
}

/// What one node-tree entry is, symlinks resolved: a symlinked
/// directory walks as a directory, a symlinked file reads as a file.
/// The shared traversal mechanic of every node-tree walk (discovery,
/// the pack into the source map, the staging copy, the source-hash
/// walk), so they agree on exactly which bytes constitute a node. A
/// broken symlink is a loud error (its target is part of the node's
/// source and it is missing).
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum NodeTreeEntryKind {
    Dir,
    File,
}

pub fn node_tree_entry_kind(path: &Path) -> std::io::Result<NodeTreeEntryKind> {
    // `fs::metadata` follows symlinks; a dangling one errors NotFound
    // here, which is the loud failure we want (the tree names bytes
    // that do not exist).
    let meta = fs::metadata(path)?;
    if meta.is_dir() {
        Ok(NodeTreeEntryKind::Dir)
    } else {
        Ok(NodeTreeEntryKind::File)
    }
}

/// Canonicalize `dir` and refuse when it already sits on the current
/// DESCENT CHAIN (the canonical directories from the walk's root down
/// to here): that is a symlink cycle, and walking on would never end.
/// Two different paths reaching the same directory (two symlinks to
/// one shared catalog) are fine: only re-entering a directory the walk
/// is currently INSIDE loops. A link to an ancestor of the walk root
/// (`nodes/x -> /home`) is caught the same way, because the walk
/// re-reaches a chain member the moment it descends back into itself.
///
/// Returns the canonical path; the caller pushes it onto its chain
/// before descending and pops it after.
pub fn guard_node_tree_cycle(dir: &Path, chain: &[PathBuf]) -> std::io::Result<PathBuf> {
    let canon = fs::canonicalize(dir)?;
    if chain.contains(&canon) {
        return Err(std::io::Error::other(format!(
            "symlink cycle in node tree: {} resolves to {}, a directory this walk is \
             already inside",
            dir.display(),
            canon.display()
        )));
    }
    Ok(canon)
}


// ----- Filesystem-backed catalog -------------------------------------

#[derive(Debug, Clone)]
pub struct CatalogEntry {
    pub node_type: String,
    pub metadata: NodeMetadata,
    /// Directory containing the node's `mod.rs` and `metadata.json`.
    pub source_dir: PathBuf,
    /// Key identifying which package this entry belongs to. Maps
    /// back into `FsCatalog::packages` for shared-file lookup.
    /// Bare nodes have package_key == source_dir.
    pub package_key: PathBuf,
}

/// A node package: either a bare-node dir or a package-root dir with
/// `package.toml`.
#[derive(Debug, Clone)]
pub struct Package {
    /// Package root directory. Bare nodes: same as the node's
    /// source_dir. Package roots: the directory holding
    /// `package.toml`.
    pub root: PathBuf,
    /// Logical name. Derived from `package.toml` (`[package].name`)
    /// if present, otherwise from the directory name.
    pub name: String,
    /// Node types declared by this package.
    pub node_types: Vec<String>,
    /// Shared `.rs` files at the package root (package roots only;
    /// empty for bare nodes). Paths are absolute.
    pub shared_rs: Vec<PathBuf>,
    /// Package-level cargo deps. Applied when any node in the
    /// package is referenced. `None` for bare nodes (their deps
    /// come from the node's `deps.toml`).
    pub package_deps: Option<toml::Table>,
}

/// Something discovery could not load: a node, or a whole package. It
/// is left out of the catalog and nothing else is: one bad folder never
/// costs a project the rest of its nodes. `node_types` are the types
/// the broken folder claims (read from its `metadata.json` files as far
/// as they can be read), so a program that names one of them is told
/// this error instead of "unknown node type". Empty when not even a
/// type name could be read; the problem is then only listed.
#[derive(Debug)]
pub struct CatalogProblem {
    pub node_types: Vec<String>,
    pub error: CatalogError,
}

#[derive(Debug)]
pub struct FsCatalog {
    entries: HashMap<String, CatalogEntry>,
    /// All discovered packages, keyed by package root. Each
    /// `CatalogEntry` has a `package_key` pointing back in here.
    packages: HashMap<PathBuf, Package>,
    /// Every node or package that failed to load, with its error.
    problems: Vec<CatalogProblem>,
    /// One line per thing left out: each problem, each node not ready
    /// yet (see `pending`), each package with no node in it yet.
    warnings: Vec<String>,
    /// Nodes seen but left out: a folder with a `metadata.json` and no
    /// `mod.rs` yet (a specialist writes the description before the
    /// code). Type name to the folder. A build never references such a
    /// node's code, so a program that does not name it builds; the
    /// compiler names the folder when a program does.
    pending: BTreeMap<String, PathBuf>,
    /// The project's resolved type registry: builtin aliases plus every
    /// `types` declaration harvested from the tree's `metadata.json`
    /// files. Built BEFORE any metadata is deserialized (port type
    /// strings may use the declared names) and carried here so
    /// compile/enrich callers can activate the same registry.
    type_registry: std::sync::Arc<weft_core::weft_type::TypeRegistry>,
}

impl FsCatalog {
    /// Walk one node tree. See `discover_roots`.
    pub fn discover(root: &Path) -> Result<Self, CatalogError> {
        Self::discover_roots(&[root])
    }

    /// One catalog over several trees. A project's nodes live in two
    /// places: `nodes/` (the standard library and anything shared) and
    /// beside the code that uses them under `src/`, so both are walked
    /// into one catalog with one namespace: a type name declared in both
    /// is the same collision it would be inside one tree. A root that
    /// does not exist contributes nothing.
    ///
    /// A node or package that cannot load is recorded in `problems` and
    /// left out; everything else loads. Two folders claiming one name
    /// (a node type, a package name, a service) are BOTH left out, each
    /// told about the other: no folder outranks another, so neither can
    /// silently win. The only `Err` is a root that exists and cannot be
    /// read at all.
    pub fn discover_roots(roots: &[&Path]) -> Result<Self, CatalogError> {
        let mut cat = Self::empty();
        let roots: Vec<&Path> = roots.iter().copied().filter(|r| r.exists()).collect();
        for root in &roots {
            fs::read_dir(root).map_err(|error| CatalogError::Io { path: root.to_path_buf(), error })?;
        }
        if roots.is_empty() {
            return Ok(cat);
        }
        // Type declarations first: port type strings in any
        // metadata.json may use the declared names, so the registry
        // must exist before a single NodeMetadata is deserialized.
        let mut ctx = DiscoverCtx {
            cat: &mut cat,
            units: Vec::new(),
            broken_files: HashMap::new(),
            chain: Default::default(),
            done: Default::default(),
        };
        let registry = std::sync::Arc::new(build_type_registry(&roots, &mut ctx));
        ctx.cat.type_registry = registry.clone();
        registry.scoped(|| {
            for root in &roots {
                visit_dir(root, &mut ctx);
            }
        });
        ctx.settle();
        Ok(cat)
    }

    /// The project's resolved type registry (builtin aliases + every
    /// harvested `types` declaration). Activate it (`scoped`) around
    /// compiling weft source against this catalog, so source-side type
    /// names resolve to the same table the metadata was loaded under.
    pub fn type_registry(&self) -> std::sync::Arc<weft_core::weft_type::TypeRegistry> {
        self.type_registry.clone()
    }

    /// An empty catalog: no nodes, no packages. The honest value for
    /// "parse this source but there is no project to resolve node
    /// types against" (every type becomes an unknown placeholder),
    /// instead of pointing discovery at a path that isn't a project.
    pub fn empty() -> Self {
        Self {
            entries: HashMap::new(),
            packages: HashMap::new(),
            problems: Vec::new(),
            warnings: Vec::new(),
            pending: BTreeMap::new(),
            type_registry: std::sync::Arc::new(weft_core::weft_type::TypeRegistry::builtin()),
        }
    }

    /// One line per thing left out of the catalog: every problem, every
    /// node not ready yet, every package with no node in it yet.
    pub fn warnings(&self) -> &[String] {
        &self.warnings
    }

    /// The nodes and packages that failed to load, with their errors.
    /// Empty means every folder in the tree loaded (or is only waiting
    /// for its code, see `pending`).
    pub fn problems(&self) -> &[CatalogProblem] {
        &self.problems
    }

    /// The nodes left out for not being ready yet: type name to the
    /// folder holding the `metadata.json` that has no `mod.rs` beside it.
    pub fn pending(&self) -> &BTreeMap<String, PathBuf> {
        &self.pending
    }

    pub fn iter(&self) -> impl Iterator<Item = &CatalogEntry> {
        self.entries.values()
    }

    /// Package that owns this node. Used by codegen to find shared
    /// Rust files and package-level deps.
    pub fn package_of(&self, node_type: &str) -> Option<&Package> {
        let entry = self.entries.get(node_type)?;
        self.packages.get(&entry.package_key)
    }

    pub fn packages(&self) -> impl Iterator<Item = &Package> {
        self.packages.values()
    }

    /// Deduped, sorted package root directories for the given node
    /// types. This is the unit of build-context staging: discovery
    /// already walked `nodes/` and grouped every node under its
    /// package root, so staging copies exactly these directories
    /// rather than re-walking the tree itself. One walker (discovery),
    /// one source of truth for what a node's directory tree is.
    /// Unknown node types are skipped (the caller validates references
    /// elsewhere; staging only needs the ones that resolved).
    pub fn package_roots_for(&self, node_types: &BTreeSet<String>) -> Vec<PathBuf> {
        let mut roots: Vec<PathBuf> = node_types
            .iter()
            .filter_map(|nt| self.package_of(nt))
            .map(|pkg| pkg.root.clone())
            .collect();
        roots.sort();
        roots.dedup();
        roots
    }

    pub fn entry(&self, node_type: &str) -> Option<&CatalogEntry> {
        self.entries.get(node_type)
    }

    /// Whether the named package declares any node self-tests (a
    /// member node carries a `tests.rs`). THE one definition of "this
    /// package has tests", so every consumer that filters packages for
    /// test builds answers the question identically.
    pub fn package_declares_tests(&self, package: &Package) -> bool {
        package.node_types.iter().any(|nt| {
            self.entry(nt).is_some_and(|e| e.source_dir.join("tests.rs").is_file())
        })
    }

    /// Read the node's optional `deps.toml`. Returns `None` if the
    /// node has no `deps.toml` (many nodes have zero extra deps).
    pub fn deps(&self, node_type: &str) -> Result<Option<NodeDeps>, CatalogError> {
        let Some(entry) = self.entries.get(node_type) else {
            return Ok(None);
        };
        let deps_path = entry.source_dir.join("deps.toml");
        if !deps_path.exists() {
            return Ok(None);
        }
        let raw = fs::read_to_string(&deps_path).map_err(|e| CatalogError::Io {
            path: deps_path.clone(),
            error: e,
        })?;
        let parsed: NodeDeps = toml::from_str(&raw).map_err(|e| CatalogError::Parse {
            path: deps_path,
            error: e.to_string(),
        })?;
        Ok(Some(parsed))
    }
}

impl MetadataCatalog for FsCatalog {
    fn lookup(&self, node_type: &str) -> Option<&NodeMetadata> {
        self.entries.get(node_type).map(|e| &e.metadata)
    }
    fn all(&self) -> Vec<&NodeMetadata> {
        self.entries.values().map(|e| &e.metadata).collect()
    }
    fn type_registry(&self) -> std::sync::Arc<weft_core::weft_type::TypeRegistry> {
        self.type_registry.clone()
    }
    fn unavailable(&self, node_type: &str) -> Option<String> {
        let reasons: Vec<String> = self
            .problems
            .iter()
            .filter(|p| p.node_types.iter().any(|t| t == node_type))
            .map(|p| p.error.to_string())
            .collect();
        if !reasons.is_empty() {
            return Some(format!("node '{node_type}' failed to load: {}", reasons.join("; ")));
        }
        self.pending
            .get(node_type)
            .map(|dir| format!("node type '{node_type}' is not ready yet: {}", pending_reason(node_type, dir)))
    }
}

/// The one sentence a pending node is described by, in the discovery
/// warnings and in the compiler's diagnostic alike.
fn pending_reason(node_type: &str, dir: &Path) -> String {
    format!("node '{node_type}' at {} has no mod.rs yet; it is left out until it does", dir.display())
}

impl FsCatalog {
    /// Source directory of a node (path to the dir containing
    /// `mod.rs`). Used by codegen to resolve `#[path]` includes and
    /// the node's `deps.toml`.
    pub fn source_dir(&self, node_type: &str) -> Option<&Path> {
        self.entries.get(node_type).map(|e| e.source_dir.as_path())
    }

}

// ----- Per-node deps.toml --------------------------------------------

/// Shape of a node's `deps.toml`.
///
/// - `[dependencies]` → cargo deps (keys are crate names,
///   values are whatever cargo accepts).
/// - `[system]` → OS-level packages to install in the worker
///   container image. One subkey per package manager
///   (`apt`/`apk`/`yum`/`brew`). Each manager's value is itself
///   a table keyed by `<distro>_<major>` (e.g. `debian_12`,
///   `ubuntu_24_04`, `alpine_3_19`, `rocky_9`) plus a special
///   `default` fallback for cases where the node doesn't
///   distinguish versions.
///
/// Codegen looks up the project's base-image distro, checks the
/// matching manager's table for the exact `<distro>_<major>`
/// key, falls through to `default` otherwise, and errors out
/// only if NEITHER is present. A node that supports every
/// distro via one install line just fills `default`; a node
/// whose package name varies from one distro to the next fills one
/// key per (distro, version) it verified.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NodeDeps {
    #[serde(default)]
    pub dependencies: toml::Table,
    /// Cargo `[build-dependencies]` for the package's own `build.rs`
    /// (a `build.rs` at the package root ships as the emitted package
    /// crate's build script).
    #[serde(default, rename = "build-dependencies")]
    pub build_dependencies: toml::Table,
    #[serde(default)]
    pub system: SystemPackages,
    #[serde(default)]
    pub build: BuildEnv,
}

/// Build-environment variables a node needs during `cargo build`.
/// Merged (union) across every referenced node and emitted as
/// `ENV` lines in the builder stage of the Dockerfile.
///
/// Keep narrow and declarative. General-purpose build logic
/// belongs in the node's own `build.rs`, not here.
///
/// Values support one substitution: `{{catalog_path}}` expands
/// to the node's directory inside the builder container, where the
/// project's `nodes/` is staged. Example, a node shipping a config:
///
/// ```toml
/// [build.env]
/// FOO_CONFIG = "{{catalog_path}}/foo-config.txt"
/// ```
///
/// resolves to `/weft/project-nodes/nodes/base_catalog/basic/exec_python/foo-config.txt`
/// if the node lives at `nodes/base_catalog/basic/exec_python/` (the
/// mount holds every node at its project-relative path, so one beside
/// the code lands under `/weft/project-nodes/src/...`).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BuildEnv {
    #[serde(default)]
    pub env: std::collections::BTreeMap<String, String>,
}

/// Per-stage, per-manager system-package tables.
///
/// Two stages, two different concerns:
///
/// - `build`: packages the BUILDER container needs to COMPILE the
///   worker binary. `pkg-config`, `libssl-dev`, and so on. These end up in the builder stage and are
///   discarded before the runtime image is sealed.
/// - `runtime`: packages the RUNTIME container needs to RUN the
///   compiled binary. `python3`, `ca-certificates`.
///
/// Each stage has the same shape: a `BTreeMap<manager, BTreeMap<
/// distro_key, Vec<String>>>`. `distro_key` is `<distro>_<major>`
/// (e.g. `debian_12`, `ubuntu_24_04`, `alpine_3_19`, `rocky_9`)
/// or the special `default` fallback.
///
/// ```toml
/// [system.build.apt]
/// default = ["libssl-dev", "pkg-config"]
///
/// [system.runtime.apt]
/// default = ["libssl3"]
/// debian_11 = ["libssl1.1"]
/// ```
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SystemPackages {
    #[serde(default)]
    pub build: StageSystemPackages,
    #[serde(default)]
    pub runtime: StageSystemPackages,
}

impl SystemPackages {
    /// True when no stage has any entry on any manager.
    pub fn is_empty(&self) -> bool {
        self.build.is_empty() && self.runtime.is_empty()
    }
}

/// Per-manager system-package table for a single build stage.
/// Manager keys are `apt`/`apk`/`yum`/`brew`. Each maps distro
/// key to the install list for THAT distro.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StageSystemPackages {
    #[serde(default)]
    pub apt: std::collections::BTreeMap<String, Vec<String>>,
    #[serde(default)]
    pub apk: std::collections::BTreeMap<String, Vec<String>>,
    #[serde(default)]
    pub yum: std::collections::BTreeMap<String, Vec<String>>,
    #[serde(default)]
    pub brew: std::collections::BTreeMap<String, Vec<String>>,
}

impl StageSystemPackages {
    pub fn is_empty(&self) -> bool {
        self.apt.is_empty() && self.apk.is_empty() && self.yum.is_empty() && self.brew.is_empty()
    }

    /// Accessor for a single manager's table. Lets
    /// worker_image.rs loop over references uniformly.
    pub fn for_manager(
        &self,
        manager: SystemManagerKey,
    ) -> &std::collections::BTreeMap<String, Vec<String>> {
        match manager {
            SystemManagerKey::Apt => &self.apt,
            SystemManagerKey::Apk => &self.apk,
            SystemManagerKey::Yum => &self.yum,
            SystemManagerKey::Brew => &self.brew,
        }
    }
}

/// Abstract name for a package manager, decoupled from
/// worker_image.rs so weft-catalog doesn't depend on it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SystemManagerKey {
    Apt,
    Apk,
    Yum,
    Brew,
}

/// Which build stage we're asking about. Used by codegen when
/// collecting package unions for the builder vs runtime
/// Dockerfile layers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuildStage {
    Build,
    Runtime,
}

// ----- Stdlib seed location ------------------------------------------

/// Filesystem path to this repo's bundled stdlib catalog:
/// `weft_repo_root()/catalog`. Consumed by `weft new` (clones the
/// catalog into the new project's `nodes/` so the project is
/// self-contained). See `weft_repo_root` for how the root resolves;
/// the error carries its carefully worded recovery, so callers
/// surface it rather than panicking over it.
pub fn stdlib_root() -> Result<PathBuf, String> {
    weft_repo_root().map(|root| root.join("catalog"))
}

/// Whether `p` is a weft checkout, judged by what the consumers of the
/// root actually read: this workspace's manifests + lockfile (staged
/// into every worker build context) and the stdlib catalog. One
/// predicate for every rung of `weft_repo_root`, so the rungs cannot
/// drift on what "valid" means.
fn looks_like_weft_root(p: &Path) -> bool {
    p.join("crates/weft-catalog").is_dir()
        && p.join("Cargo.toml").is_file()
        && p.join("Cargo.lock").is_file()
        && p.join("catalog").is_dir()
}

/// What one repo-root resolution had to work with, gathered by
/// `weft_repo_root` and resolved by the pure(-ish) `resolve_weft_root_from`
/// so the ladder order and its error wording are unit-testable without
/// touching the process env.
struct RootSources {
    env_root: Option<PathBuf>,
    cwd: Option<PathBuf>,
    built_from: Option<PathBuf>,
    recorded: RecordedInstallRoot,
}

/// The installer's recorded root, with "the file could not be read"
/// kept apart from "no file": reporting a permission error as "nothing
/// was recorded" would send the user looking in the wrong place.
enum RecordedInstallRoot {
    Absent,
    Unreadable { file: PathBuf, error: String },
    Recorded { file: PathBuf, root: PathBuf },
}

/// Resolve the on-disk weft workspace root, in order:
///
/// 1. `WEFT_REPO_ROOT` (set in a built image, where nothing else below
///    exists). An explicit override that does not point at a checkout
///    is a configuration error and fails loud rather than falling
///    through to a guess.
/// 2. The checkout the process is STANDING IN (walking up from the
///    working directory). With several worktrees on one machine this
///    is the only rung that picks the one the user means.
/// 3. The compile-time repo layout (`<repo>/crates/weft-catalog` ->
///    parent -> parent), if that directory is still a checkout. A
///    locally built binary lives in its checkout's `target/`, so this
///    is the everyday dev answer outside any checkout.
/// 4. The root the installer recorded. A PREBUILT binary (downloaded
///    by setup.sh from the rolling release) bakes its BUILDER's
///    checkout path into (3), which does not exist on this machine;
///    setup.sh records where the repo actually lives, one line in a
///    file, and every successful `weft daemon start` records the
///    checkout it installed from, so a later start from a project
///    folder finds the same manifests and shared-credentials file.
///    SYNC: repo-root file <-> setup.sh (prebuilt CLI install: repo-root write), crates/weft-cli/src/commands/daemon.rs (record_repo_root)
///
/// THE single resolver; `weft_compiler::build::resolve_weft_root`
/// delegates here so the two can't drift (they must return the same
/// path or the stdlib seed and the build context disagree). The error
/// names every source it tried and the recovery; the winning rung is
/// traced at debug so a wrong-checkout surprise (two worktrees, rung 2
/// answering) can be pinned from the logs.
pub fn weft_repo_root() -> Result<PathBuf, String> {
    let recorded = match std::env::var_os("HOME")
        .map(|h| PathBuf::from(h).join(".local/share/weft/repo-root"))
    {
        None => RecordedInstallRoot::Absent,
        Some(file) => match std::fs::read_to_string(&file) {
            Ok(recorded) => {
                RecordedInstallRoot::Recorded { root: PathBuf::from(recorded.trim()), file }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => RecordedInstallRoot::Absent,
            Err(e) => RecordedInstallRoot::Unreadable { file, error: e.to_string() },
        },
    };
    let sources = RootSources {
        env_root: std::env::var_os("WEFT_REPO_ROOT").map(PathBuf::from),
        cwd: std::env::current_dir().ok(),
        built_from: Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(|p| p.parent())
            .map(|p| p.to_path_buf()),
        recorded,
    };
    let root = resolve_weft_root_from(&sources)?;
    tracing::debug!(target: "weft_catalog", root = %root.display(), "resolved weft repo root");
    Ok(root)
}

fn resolve_weft_root_from(sources: &RootSources) -> Result<PathBuf, String> {
    if let Some(root) = &sources.env_root {
        if looks_like_weft_root(root) {
            return Ok(root.clone());
        }
        return Err(format!(
            "WEFT_REPO_ROOT is set to '{}', which is not a weft checkout \
             (no crates/weft-catalog + catalog dirs beside the workspace \
             Cargo.toml/Cargo.lock); fix or unset it",
            root.display()
        ));
    }
    if let Some(cwd) = &sources.cwd {
        let mut dir = cwd.clone();
        loop {
            if looks_like_weft_root(&dir) {
                return Ok(dir);
            }
            if !dir.pop() {
                break;
            }
        }
    }
    if let Some(built_from) = &sources.built_from {
        if looks_like_weft_root(built_from) {
            return Ok(built_from.clone());
        }
    }
    match &sources.recorded {
        RecordedInstallRoot::Recorded { file, root } => {
            if looks_like_weft_root(root) {
                return Ok(root.clone());
            }
            Err(format!(
                "cannot locate the weft checkout: WEFT_REPO_ROOT is unset, this \
                 process is not inside one, the path this binary was built from is \
                 gone, and the recorded install location '{}' (from {}) is not a \
                 checkout any more (moved or deleted?). Re-run ./setup.sh from your \
                 weft checkout to record where it lives.",
                root.display(),
                file.display()
            ))
        }
        RecordedInstallRoot::Unreadable { file, error } => Err(format!(
            "cannot locate the weft checkout: WEFT_REPO_ROOT is unset, this \
             process is not inside one, the path this binary was built from is \
             gone, and the recorded install location file '{}' could not be read \
             ({error}). Fix the file, or re-run ./setup.sh from your weft checkout.",
            file.display()
        )),
        RecordedInstallRoot::Absent => Err(format!(
            "cannot locate the weft checkout: WEFT_REPO_ROOT is unset, this process \
             is not inside one, the path this binary was built from ('{}') is gone, \
             and no install location was recorded. Re-run ./setup.sh from your weft \
             checkout, or set WEFT_REPO_ROOT.",
            sources
                .built_from
                .as_ref()
                .map(|p| p.display().to_string())
                .unwrap_or_default()
        )),
    }
}

// ----- Package discovery ---------------------------------------------

/// Shape of `package.toml` for a package root. Members are
/// auto-detected (any subdir with a `metadata.json`); `package.toml`
/// only names the package and carries shared cargo deps.
#[derive(Debug, Clone, Deserialize)]
struct PackageToml {
    package: PackageSection,
    #[serde(default)]
    dependencies: toml::Table,
}

#[derive(Debug, Clone, Deserialize)]
struct PackageSection {
    name: String,
}

/// A unit the walk loaded: a package root or a bare node, with every
/// member that loaded. Nothing registers until the whole tree is walked
/// (`DiscoverCtx::settle`), because a clash between two units can only
/// be judged once both are known, and neither may win by being first.
struct Unit {
    root: PathBuf,
    name: String,
    entries: Vec<CatalogEntry>,
    shared_rs: Vec<PathBuf>,
    package_deps: Option<toml::Table>,
}

/// Discovery state threaded through the traversal: the catalog being
/// built, the units loaded so far, and the files the type pass already
/// found broken.
struct DiscoverCtx<'a> {
    cat: &'a mut FsCatalog,
    units: Vec<Unit>,
    /// `metadata.json` files whose `types` declarations could not be
    /// taken into the registry, with why. Loading such a file fails with
    /// that reason, so the error lands on the node or package that
    /// declared the type, never on the whole catalog.
    broken_files: HashMap<PathBuf, String>,
    /// Canonical dirs of the CURRENT descent chain; re-entering one is
    /// a symlink cycle and fails loudly (see `guard_node_tree_cycle`).
    chain: Vec<PathBuf>,
    /// Canonical dirs already fully processed. A folder reachable by
    /// two paths (two symlinks to one shared catalog) registers once
    /// and is skipped on every later arrival; without this, a chain of
    /// doubled links walks a shared subtree once per path, which grows
    /// as 2^depth.
    done: std::collections::HashSet<PathBuf>,
}

impl DiscoverCtx<'_> {
    /// A node seen but not ready: left out (a half-written node is not
    /// an error in anyone's build), recorded so the compiler can name it
    /// when a program asks for it.
    fn leave_pending(&mut self, node_type: String, dir: PathBuf) {
        self.cat.warnings.push(pending_reason(&node_type, &dir));
        self.cat.pending.insert(node_type, dir);
    }

    /// A node or package that failed to load: left out, with its error,
    /// under the types it claims.
    fn record(&mut self, node_types: Vec<String>, error: CatalogError) {
        self.cat.warnings.push(error.to_string());
        self.cat.problems.push(CatalogProblem { node_types, error });
    }

    /// Register every loaded unit, after leaving out each one that
    /// shares a claim with another. Three names must each lead to one
    /// place: a package name (the test-crate emit picks its package by
    /// name), a node type, and a service (the store that keeps
    /// connections and the compiler resolving what a node publishes
    /// both find a service by name alone). Every claimant of a shared
    /// name is left out, told about all the others, because the walk's
    /// order is no reason for one folder to win.
    fn settle(mut self) {
        let mut units = std::mem::take(&mut self.units);

        // Package names: a clash takes the whole unit out.
        let mut by_name: BTreeMap<String, Vec<PathBuf>> = BTreeMap::new();
        for unit in &units {
            by_name.entry(unit.name.clone()).or_default().push(unit.root.clone());
        }
        let (clashing, mut units_ok): (Vec<Unit>, Vec<Unit>) =
            units.drain(..).partition(|u| by_name[&u.name].len() > 1);
        // Node types are judged over EVERY loaded entry, the clashing
        // units' included: a type claimed twice is ambiguous whatever
        // else is wrong with one of its claimants.
        let mut by_type: BTreeMap<String, Vec<PathBuf>> = BTreeMap::new();
        for entry in clashing.iter().chain(units_ok.iter()).flat_map(|u| &u.entries) {
            by_type.entry(entry.node_type.clone()).or_default().push(entry.source_dir.clone());
        }
        for unit in clashing {
            let node_types = unit.entries.iter().map(|e| e.node_type.clone()).collect();
            let roots = by_name[&unit.name].clone();
            self.record(node_types, CatalogError::PackageNameCollision { name: unit.name, roots });
        }
        for (node_type, dirs) in &by_type {
            if dirs.len() > 1 {
                self.record(
                    vec![node_type.clone()],
                    CatalogError::Collision { node_type: node_type.clone(), dirs: dirs.clone() },
                );
            }
        }
        for unit in &mut units_ok {
            unit.entries.retain(|e| by_type[&e.node_type].len() == 1);
        }

        // Services, over what is left.
        let mut by_service: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for entry in units_ok.iter().flat_map(|u| &u.entries) {
            if let Some(service) = &entry.metadata.service {
                by_service.entry(service.service.clone()).or_default().push(entry.node_type.clone());
            }
        }
        for (service, node_types) in &by_service {
            if node_types.len() > 1 {
                self.record(
                    node_types.clone(),
                    CatalogError::ServiceCollision { service: service.clone(), node_types: node_types.clone() },
                );
            }
        }
        for unit in &mut units_ok {
            unit.entries.retain(|e| {
                e.metadata.service.as_ref().is_none_or(|s| by_service[&s.service].len() == 1)
            });
        }

        // A unit whose every entry was taken out has nothing to serve;
        // its types are already recorded.
        for unit in units_ok.into_iter().filter(|u| !u.entries.is_empty()) {
            let mut node_types: Vec<String> = unit.entries.iter().map(|e| e.node_type.clone()).collect();
            node_types.sort();
            for entry in unit.entries {
                self.cat.entries.insert(entry.node_type.clone(), entry);
            }
            self.cat.packages.insert(
                unit.root.clone(),
                Package {
                    root: unit.root,
                    name: unit.name,
                    node_types,
                    shared_rs: unit.shared_rs,
                    package_deps: unit.package_deps,
                },
            );
        }
    }
}

/// The node type a folder's `metadata.json` claims, read as plainly as
/// possible: used only to name a folder that failed to load, so a
/// program naming that type is told why.
fn declared_type(node_dir: &Path) -> Option<String> {
    let raw = fs::read_to_string(node_dir.join("metadata.json")).ok()?;
    let value: serde_json::Value = serde_json::from_str(&raw).ok()?;
    value.get("type")?.as_str().map(str::to_string)
}

/// The node types claimed by a package's member folders (every
/// immediate subdir with a `metadata.json`), for naming a package that
/// failed to load as a whole.
fn declared_member_types(entries: &[NodeDirEntry]) -> Vec<String> {
    entries
        .iter()
        .filter_map(|e| match e {
            NodeDirEntry::Dir(path) => declared_type(path),
            NodeDirEntry::File(_) => None,
        })
        .collect()
}

/// Read a `metadata.json`, failing with the type pass's reason when that
/// pass already found the file's `types` declarations unusable.
fn read_metadata_file(path: &Path, broken_files: &HashMap<PathBuf, String>) -> Result<String, CatalogError> {
    if let Some(error) = broken_files.get(path) {
        return Err(CatalogError::Parse { path: path.to_path_buf(), error: error.clone() });
    }
    fs::read_to_string(path).map_err(|error| CatalogError::Io { path: path.to_path_buf(), error })
}

/// Harvest every `types` declaration under `root` and build the
/// project's [`weft_core::weft_type::TypeRegistry`]. This walk is
/// deliberately simpler than unit discovery: type declarations are
/// global, so it reads the `types` key of EVERY `metadata.json` in the
/// tree (member and package-root alike, same symlink-following and
/// exclusion policy via `read_node_dir`) with no unit semantics. Malformed JSON is left
/// for the main discovery pass to report (it owns metadata errors);
/// only the declarations themselves fail here.
///
/// A file whose declarations cannot be taken in (a bad shape, a cycle,
/// a name declared twice with different bodies) goes into
/// `broken_files` with the reason, and the registry is built from the
/// rest, so the failure lands on the node or package that declared the
/// type. Two files clashing on one name are BOTH left out: neither
/// outranks the other.
fn build_type_registry(roots: &[&Path], ctx: &mut DiscoverCtx<'_>) -> weft_core::weft_type::TypeRegistry {
    use weft_core::weft_type::{Redeclaration, TypeRegistry};
    let mut declarations: Vec<(String, String, String)> = Vec::new();
    for root in roots {
        harvest_type_declarations(root, &mut declarations, &mut ctx.broken_files);
    }
    // Lexical order by origin path: `fs::read_dir` order is
    // filesystem-dependent, and the messages name origins in the order
    // they are met.
    declarations.sort_by(|a, b| a.2.cmp(&b.2).then_with(|| a.0.cmp(&b.0)));
    if let Ok(registry) = TypeRegistry::build(&declarations) {
        return registry;
    }

    // Something is wrong in some file. Take the files in one by one
    // until none more can be taken, and leave out the ones that never
    // fit; when one of those restates a name an accepted file declares
    // differently, that accepted file is just as much at fault, so it
    // is left out too and the round runs again without it.
    let mut by_origin: BTreeMap<String, Vec<(String, String, String)>> = BTreeMap::new();
    for decl in declarations {
        by_origin.entry(decl.2.clone()).or_default().push(decl);
    }
    let mut excluded: BTreeMap<String, String> = BTreeMap::new();
    loop {
        let mut registry = TypeRegistry::builtin();
        let mut accepted: Vec<&String> = Vec::new();
        let mut waiting: Vec<&String> = by_origin.keys().filter(|o| !excluded.contains_key(*o)).collect();
        let mut last_error: BTreeMap<&String, String> = BTreeMap::new();
        loop {
            let before = waiting.len();
            waiting.retain(|origin| match registry.extended(&by_origin[*origin], Redeclaration::AbsorbIdentical) {
                Ok(next) => {
                    registry = next;
                    accepted.push(*origin);
                    false
                }
                Err(error) => {
                    last_error.insert(origin, error);
                    true
                }
            });
            if waiting.is_empty() || waiting.len() == before {
                break;
            }
        }
        let mut again = false;
        for origin in &waiting {
            let error = last_error.remove(origin).expect("a file still waiting failed its last try");
            for other in &accepted {
                let restated = by_origin[*origin].iter().any(|(name, body, _)| {
                    by_origin[*other].iter().any(|(n, b, _)| n == name && b.trim() != body.trim())
                });
                if restated {
                    excluded.insert((*other).clone(), error.clone());
                    again = true;
                }
            }
            excluded.insert((*origin).clone(), error);
        }
        if !again {
            for (origin, error) in excluded {
                ctx.broken_files.insert(PathBuf::from(origin), format!("type declarations: {error}"));
            }
            return registry;
        }
    }
}

/// Collect every `types` declaration under `dir` as `(name, type string,
/// origin file)`. A `types` key of the wrong shape puts its file in
/// `broken_files`; anything the walk itself cannot read is left for the
/// main pass, which reports it on the folder it belongs to.
fn harvest_type_declarations(
    dir: &Path,
    out: &mut Vec<(String, String, String)>,
    broken_files: &mut HashMap<PathBuf, String>,
) {
    let mut chain = Vec::new();
    let mut done = std::collections::HashSet::new();
    harvest_type_declarations_inner(dir, out, broken_files, &mut chain, &mut done)
}

fn harvest_type_declarations_inner(
    dir: &Path,
    out: &mut Vec<(String, String, String)>,
    broken_files: &mut HashMap<PathBuf, String>,
    chain: &mut Vec<PathBuf>,
    done: &mut std::collections::HashSet<PathBuf>,
) {
    let Ok(canon) = guard_node_tree_cycle(dir, chain) else { return };
    // Same dedupe as discovery: a folder reachable by two symlink paths
    // yields its declarations once.
    if !done.insert(canon.clone()) {
        return;
    }
    let Ok(entries) = read_node_dir(dir) else { return };
    chain.push(canon);
    for entry in entries {
        match entry {
            NodeDirEntry::Dir(path) => harvest_type_declarations_inner(&path, out, broken_files, chain, done),
            NodeDirEntry::File(path) => {
                if path.file_name().and_then(|n| n.to_str()) != Some("metadata.json") {
                    continue;
                }
                // Raw read: the typed NodeMetadata parse needs the
                // registry we are building. An unreadable or malformed
                // file is the main pass's error to report (it reads
                // every metadata.json it loads, package roots included).
                let Ok(raw) = fs::read_to_string(&path) else { continue };
                let Ok(value) = serde_json::from_str::<serde_json::Value>(&raw) else { continue };
                let Some(types) = value.get("types") else { continue };
                let Some(map) = types.as_object() else {
                    broken_files.insert(path, "`types` must be an object of name -> type string".into());
                    continue;
                };
                let bodies = map
                    .iter()
                    .map(|(name, body)| body.as_str().map(|body| (name, body)).ok_or(name))
                    .collect::<Result<Vec<_>, _>>();
                match bodies {
                    Ok(bodies) => {
                        for (name, body) in bodies {
                            out.push((name.clone(), body.to_string(), path.display().to_string()));
                        }
                    }
                    Err(name) => {
                        broken_files.insert(path, format!("`types.{name}` must be a type string"));
                    }
                }
            }
        }
    }
    chain.pop();
}

/// Recursive directory visitor under `nodes/`. For each directory:
///   - If it has `package.toml`, it's a package root.
///   - Else if it has `metadata.json`, it's a bare node.
///   - Else recurse into subdirectories.
/// A unit does not nest: once a package root or bare node is detected,
/// its interior is not re-scanned. Depth and position are irrelevant;
/// a unit can sit directly under `nodes/` or ten levels deep.
/// A classified, kept child of a node-tree directory. Symlinks and
/// `NODE_TREE_EXCLUDE` names are already filtered out by
/// `read_node_dir`, so every entry here is a real dir or file the
/// node-tree policy admits.
enum NodeDirEntry {
    Dir(PathBuf),
    File(PathBuf),
}

impl NodeDirEntry {
    fn path(&self) -> &Path {
        match self {
            Self::Dir(p) | Self::File(p) => p,
        }
    }
}

/// Read a directory's immediate children under the node-tree policy:
/// follow symlinks (see `node_tree_entry_kind` for the loop guard),
/// skip `NODE_TREE_EXCLUDE` names. The single traversal mechanic for
/// discovery; `visit_dir` recurses its dirs and `register_package`
/// matches members/shared files against the same entries, so the two
/// can't diverge on what a node tree contains (the de-sync that serves
/// a stale image or bloats the build context).
fn read_node_dir(dir: &Path) -> Result<Vec<NodeDirEntry>, CatalogError> {
    let read = fs::read_dir(dir).map_err(|e| CatalogError::Io {
        path: dir.to_path_buf(),
        error: e,
    })?;
    let mut out = Vec::new();
    for child in read {
        let child = child.map_err(|e| CatalogError::Io {
            path: dir.to_path_buf(),
            error: e,
        })?;
        if is_node_tree_excluded(&child.file_name().to_string_lossy()) {
            continue;
        }
        match node_tree_entry_kind(&child.path()).map_err(|error| CatalogError::Io {
            path: child.path(),
            error,
        })? {
            NodeTreeEntryKind::Dir => out.push(NodeDirEntry::Dir(child.path())),
            NodeTreeEntryKind::File => out.push(NodeDirEntry::File(child.path())),
        }
    }
    // Sorted, because `fs::read_dir` order is the filesystem's, not
    // the tree's. Every decision the walk makes by ARRIVING somewhere
    // first rides on this: which of two nodes claiming one identity is
    // the one reported as the original, and, under a lenient policy
    // (which warns and skips rather than failing), which of them
    // vanishes from the catalog. Unsorted, that answer changes per
    // machine.
    out.sort_by(|a, b| a.path().cmp(b.path()));
    Ok(out)
}

/// True if the entries contain a file named `name` (symlinks resolved,
/// like every node-tree walk). Unit detection goes through this rather
/// than re-`stat`ing with `Path::is_file`: detection must see the
/// SAME symlink-following view the tree walk and staging copy see, so
/// what is discovered and what reaches the build context agree.
fn has_node_file(entries: &[NodeDirEntry], name: &str) -> bool {
    entries.iter().any(|e| {
        matches!(e, NodeDirEntry::File(p)
            if p.file_name().and_then(|n| n.to_str()) == Some(name))
    })
}

/// Walk one directory. Nothing here fails the catalog: a folder that
/// cannot be read or that loops back on itself is recorded as a problem
/// and the walk goes on with its siblings.
fn visit_dir(dir: &Path, ctx: &mut DiscoverCtx<'_>) {
    let canon = match guard_node_tree_cycle(dir, &ctx.chain) {
        Ok(canon) => canon,
        Err(error) => return ctx.record(Vec::new(), CatalogError::Io { path: dir.to_path_buf(), error }),
    };
    // A folder already fully processed (reached again through a second
    // symlink path) registers nothing twice and is not re-walked.
    if !ctx.done.insert(canon.clone()) {
        return;
    }
    let entries = match read_node_dir(dir) {
        Ok(entries) => entries,
        Err(error) => return ctx.record(Vec::new(), error),
    };
    if has_node_file(&entries, "package.toml") {
        return register_package(dir, entries, ctx);
    }
    if has_node_file(&entries, "metadata.json") {
        return register_bare_node(dir, ctx);
    }
    ctx.chain.push(canon);
    for entry in entries {
        if let NodeDirEntry::Dir(path) = entry {
            visit_dir(&path, ctx);
        }
    }
    ctx.chain.pop();
}

/// A bare node: the directory IS the node. It is its own degenerate
/// package (one member, no shared code, deps from its `deps.toml`, no
/// package-level metadata defaults: its own `metadata.json` is already
/// the whole story).
fn register_bare_node(dir: &Path, ctx: &mut DiscoverCtx<'_>) {
    let entry = match load_node_entry(dir, dir, None, &ctx.broken_files) {
        Ok(Loaded::Ready(e)) => *e,
        Ok(Loaded::Pending { node_type }) => return ctx.leave_pending(node_type, dir.to_path_buf()),
        Err(e) => return ctx.record(declared_type(dir).into_iter().collect(), e),
    };
    // The package name is the directory name. A non-UTF-8 name is a
    // node weft can't compile (it becomes a Rust module ident), so it
    // is refused instead of given a placeholder that would collide with
    // any other unnameable node.
    let Some(name) = dir.file_name().and_then(|n| n.to_str()) else {
        return ctx.record(
            vec![entry.node_type],
            CatalogError::Parse { path: dir.to_path_buf(), error: "node directory name is not valid UTF-8".into() },
        );
    };
    ctx.units.push(Unit {
        root: dir.to_path_buf(),
        name: name.to_string(),
        entries: vec![entry],
        shared_rs: Vec::new(),
        package_deps: None,
    });
}

/// A package root: `package.toml` names the package and carries shared
/// cargo deps. Members are auto-detected (any immediate subdir with a
/// `metadata.json`), so the author never maintains a node list. Shared
/// `.rs` files at the root are bundled into the package module.
fn register_package(dir: &Path, entries: Vec<NodeDirEntry>, ctx: &mut DiscoverCtx<'_>) {
    // Whatever makes the package itself unloadable takes out every
    // member with it, named by the types they claim.
    let parsed = match load_package_toml(&dir.join("package.toml")) {
        Ok(parsed) => parsed,
        Err(e) => return ctx.record(declared_member_types(&entries), e),
    };

    // Package-level metadata defaults: an OPTIONAL, PARTIAL `metadata.json`
    // at the package root. Every member inherits its top-level keys unless
    // the member's own `metadata.json` carries the key (key-by-key, member
    // wins; see `load_node_entry`). One place for whatever a package's nodes
    // share (`provider`, `portsFromConfig`, future keys), instead of one
    // sidecar file per feature. `type` is a node's identity and can never be
    // shared, so its presence here is an error, not a default.
    // Presence is decided by the SAME `read_node_dir` view the walk,
    // staging, and hash use (`has_node_file`), so the catalog's view and
    // the compiled node's (whose derive reads the staged tree) agree.
    let package_defaults = if has_node_file(&entries, "metadata.json") {
        match load_package_defaults(dir, &ctx.broken_files) {
            Ok(d) => Some(d),
            Err(e) => return ctx.record(declared_member_types(&entries), e),
        }
    } else {
        None
    };

    // Auto-detect members: every immediate subdir whose own
    // `read_node_dir` view contains a `metadata.json`. Shared `.rs` files live at the
    // package root. All of this is the `read_node_dir` view (the
    // package's `entries` plus each member's), so a package's tree is
    // seen identically by discovery, staging, and hashing.
    let mut loaded: Vec<CatalogEntry> = Vec::new();
    let mut shared_rs: Vec<PathBuf> = Vec::new();
    let mut members_seen = 0usize;
    for entry in entries {
        match entry {
            NodeDirEntry::Dir(path) => {
                let member_entries = match read_node_dir(&path) {
                    Ok(member_entries) => member_entries,
                    Err(e) => {
                        ctx.record(declared_type(&path).into_iter().collect(), e);
                        continue;
                    }
                };
                if !has_node_file(&member_entries, "metadata.json") {
                    continue;
                }
                members_seen += 1;
                match load_node_entry(&path, dir, package_defaults.as_ref(), &ctx.broken_files) {
                    Ok(Loaded::Ready(entry)) => loaded.push(*entry),
                    Ok(Loaded::Pending { node_type }) => ctx.leave_pending(node_type, path),
                    Err(e) => ctx.record(declared_type(&path).into_iter().collect(), e),
                }
            }
            NodeDirEntry::File(path)
                if path.extension().and_then(|e| e.to_str()) == Some("rs") =>
            {
                shared_rs.push(path);
            }
            _ => {}
        }
    }
    shared_rs.sort();

    // A package with no node folder at all is one being started (its
    // `package.toml` written first): not ready, and no error for anyone.
    // One whose members are all pending or broken already has a line
    // for each of them, and still enters `settle` with no entries: its
    // name is claimed all the same, so a healthy package elsewhere under
    // that name is a collision rather than the quiet winner.
    if members_seen == 0 {
        ctx.cat.warnings.push(format!(
            "package '{}' at {} has no node in it yet (no subfolder with a metadata.json); it is \
             left out until it does",
            parsed.package.name,
            dir.display()
        ));
        return;
    }
    ctx.units.push(Unit {
        root: dir.to_path_buf(),
        name: parsed.package.name,
        entries: loaded,
        shared_rs,
        package_deps: Some(parsed.dependencies),
    });
}

/// Read and parse a `package.toml`.
fn load_package_toml(path: &Path) -> Result<PackageToml, CatalogError> {
    let raw = fs::read_to_string(path).map_err(|error| CatalogError::Io { path: path.to_path_buf(), error })?;
    toml::from_str(&raw).map_err(|e| CatalogError::Parse { path: path.to_path_buf(), error: e.to_string() })
}

/// Load a package root's partial `metadata.json` (the defaults its members
/// inherit key-by-key). The CALLER decides the file exists, from the
/// `read_node_dir` view it already holds (`has_node_file`), so this never
/// re-`stat`s the path. A file that is not a JSON object, or that carries an
/// identity key, is an error: silently ignoring it would make every member
/// quietly miss its inherited keys.
fn load_package_defaults(
    package_root: &Path,
    broken_files: &HashMap<PathBuf, String>,
) -> Result<serde_json::Map<String, serde_json::Value>, CatalogError> {
    let path = package_root.join("metadata.json");
    let raw = read_metadata_file(&path, broken_files)?;
    let value: serde_json::Value =
        serde_json::from_str(&raw).map_err(|e| CatalogError::Parse {
            path: path.clone(),
            error: e.to_string(),
        })?;
    let serde_json::Value::Object(obj) = value else {
        return Err(CatalogError::Parse {
            path,
            error: "package-level metadata.json must be a JSON object".into(),
        });
    };
    // Refuse an identity key ONCE here, where the offending file is known, so
    // the author is pointed at the package root rather than at whichever member
    // happened to be merged first. The merge itself re-checks (it is the shared
    // definition of the rule, also used by the derive).
    for key in weft_core::node::NON_INHERITABLE_METADATA_KEYS {
        if obj.contains_key(key) {
            return Err(CatalogError::Parse {
                path,
                error: format!(
                    "package-level metadata.json must not set `{key}`: it is one node's \
                     identity, not a package default"
                ),
            });
        }
    }
    Ok(obj)
}

/// What loading a node folder found: a node ready to catalog, or one
/// whose description exists but whose code does not yet.
enum Loaded {
    Ready(Box<CatalogEntry>),
    Pending { node_type: String },
}

/// Load a single node's metadata + form field specs.
///
/// `node_dir` = directory containing the node's `metadata.json`,
/// `mod.rs`, and `deps.toml`. `package_key` = directory identifying
/// the node's package (same as `node_dir` for a bare node, the
/// package root otherwise). `package_defaults` = the package root's
/// partial `metadata.json`, merged in key-by-key (top level only) for
/// every key the node's own file does not set.
fn load_node_entry(
    node_dir: &Path,
    package_key: &Path,
    package_defaults: Option<&serde_json::Map<String, serde_json::Value>>,
    broken_files: &HashMap<PathBuf, String>,
) -> Result<Loaded, CatalogError> {
    let meta_path = node_dir.join("metadata.json");
    let raw = read_metadata_file(&meta_path, broken_files)?;
    let mut value: serde_json::Value =
        serde_json::from_str(&raw).map_err(|e| CatalogError::Parse {
            path: meta_path.clone(),
            error: e.to_string(),
        })?;
    if let Some(defaults) = package_defaults {
        // The node's own key wins wholesale (no deep merge). Same merge the
        // derive runs at compile time, so catalog metadata and the runtime
        // `manifest()` are one document.
        //
        // An error here is about the PACKAGE ROOT's file (an identity key it
        // may not share), never the member's, so it names the package root:
        // blaming an arbitrary member would send the author to the wrong file.
        // `load_package_defaults` already refused that case once per package;
        // this is the shared backstop.
        weft_core::node::merge_package_defaults(&mut value, defaults).map_err(|error| {
            CatalogError::Parse { path: package_key.join("metadata.json"), error }
        })?;
    }
    weft_core::node::refuse_removed_metadata_keys(&value)
        .map_err(|error| CatalogError::Parse { path: meta_path.clone(), error })?;
    // A stale stdlib COPY is the common way to hold metadata this weft
    // no longer accepts (a shape serde refuses, or a setting the
    // language now owns); name the one-command re-sync.
    let parse_error = |error: String| CatalogError::Parse {
        path: meta_path.clone(),
        error: if meta_path.components().any(|c| c.as_os_str() == "base_catalog") {
            format!("{error} (a stale base_catalog copy? run `weft catalog update` in the project to re-sync it)")
        } else {
            error
        },
    };
    let mut metadata: NodeMetadata =
        serde_json::from_value(value).map_err(|e| parse_error(e.to_string()))?;
    // The settings the language owns (how runs are kept, entry limits), added
    // before the semantic check so they are checked like any input.
    metadata.add_language_ports().map_err(parse_error)?;
    // Semantic rules serde can't express (field/port name collisions).
    metadata.validate_semantics().map_err(|error| CatalogError::Parse {
        path: meta_path.clone(),
        error,
    })?;
    // Codegen interpolates the node type into generated Rust source
    // (registry match arms, `{type}Node` struct paths), so a name that
    // is not a plain identifier must fail HERE, naming this file,
    // instead of as a rustc error inside a generated crate.
    if !is_rust_identifier(&metadata.node_type) {
        return Err(CatalogError::Parse {
            path: meta_path.clone(),
            error: format!(
                "node type '{}' is not a valid Rust identifier \
                 ([A-Za-z_][A-Za-z0-9_]*)",
                metadata.node_type
            ),
        });
    }

    // The description is sound; the code may not be there yet. The
    // same `read_node_dir` view the walk and the staging use decides.
    if !has_node_file(&read_node_dir(node_dir)?, "mod.rs") {
        return Ok(Loaded::Pending { node_type: metadata.node_type });
    }

    Ok(Loaded::Ready(Box::new(CatalogEntry {
        node_type: metadata.node_type.clone(),
        metadata,
        source_dir: node_dir.to_path_buf(),
        package_key: package_key.to_path_buf(),
    })))
}

// ----- Errors --------------------------------------------------------

#[derive(Debug, Error)]
pub enum CatalogError {
    #[error("io: {path}: {error}")]
    Io {
        path: PathBuf,
        #[source]
        error: std::io::Error,
    },
    #[error("parse: {path}: {error}")]
    Parse { path: PathBuf, error: String },
    #[error(
        "node type '{node_type}' is declared by more than one folder ({}); none of them is \
         loaded until only one declares it",
        list_paths(dirs)
    )]
    Collision { node_type: String, dirs: Vec<PathBuf> },
    #[error(
        "the '{service}' service is declared by more than one node ({}); a service names one \
         sign-in, and everything that stores or publishes a connection finds it by that name \
         alone, so none of them is loaded until only one declares it",
        node_types.join(", ")
    )]
    ServiceCollision { service: String, node_types: Vec<String> },
    #[error(
        "package name '{name}' is used by more than one folder ({}); a bare node is named by its \
         folder, a package by its package.toml. None of them is loaded until the names differ",
        list_paths(roots)
    )]
    PackageNameCollision { name: String, roots: Vec<PathBuf> },
}

fn list_paths(paths: &[PathBuf]) -> String {
    paths.iter().map(|p| p.display().to_string()).collect::<Vec<_>>().join(", ")
}

#[cfg(test)]
mod root_tests {
    use super::*;

    /// A directory shaped like a weft checkout, per `looks_like_weft_root`.
    fn fake_checkout() -> tempfile::TempDir {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::create_dir_all(dir.path().join("crates/weft-catalog")).expect("mkdir");
        std::fs::create_dir_all(dir.path().join("catalog")).expect("mkdir");
        std::fs::write(dir.path().join("Cargo.toml"), "[workspace]\n").expect("write");
        std::fs::write(dir.path().join("Cargo.lock"), "").expect("write");
        dir
    }

    fn no_sources() -> RootSources {
        RootSources {
            env_root: None,
            cwd: None,
            built_from: None,
            recorded: RecordedInstallRoot::Absent,
        }
    }

    /// What Python writes beside a node's own code when it first runs
    /// is never part of the node, so two machines hash the same tree the
    /// same whether either ran it.
    #[test]
    fn a_python_cache_is_never_part_of_a_node() {
        assert!(is_node_tree_excluded("__pycache__"));
        assert!(is_node_tree_excluded("helper.cpython-312.pyc"));
        assert!(!is_node_tree_excluded("helper.py"));
        assert!(!is_node_tree_excluded("pycache"));
    }

    /// The predicate demands everything the consumers read: the crate
    /// dir, the workspace manifests, and the catalog. Any one missing
    /// means "not a checkout", or a deeper consumer would fail with a
    /// worse message later.
    #[test]
    fn a_checkout_needs_crates_manifests_and_catalog() {
        let dir = fake_checkout();
        assert!(looks_like_weft_root(dir.path()));
        for missing in ["crates/weft-catalog", "Cargo.toml", "Cargo.lock", "catalog"] {
            let dir = fake_checkout();
            let p = dir.path().join(missing);
            if p.is_dir() {
                std::fs::remove_dir_all(&p).expect("rm");
            } else {
                std::fs::remove_file(&p).expect("rm");
            }
            assert!(!looks_like_weft_root(dir.path()), "should fail without {missing}");
        }
    }

    /// Rung 1: an explicit WEFT_REPO_ROOT wins, and one that is not a
    /// checkout is a loud config error naming the path, never a fall
    /// through to a guess.
    #[test]
    fn env_root_wins_and_a_bad_one_fails_loud() {
        let dir = fake_checkout();
        let sources = RootSources { env_root: Some(dir.path().to_path_buf()), ..no_sources() };
        assert_eq!(resolve_weft_root_from(&sources).expect("resolves"), dir.path());

        let bogus = tempfile::tempdir().expect("tempdir");
        let other = fake_checkout();
        let sources = RootSources {
            env_root: Some(bogus.path().to_path_buf()),
            // Even with a perfectly good cwd rung below, the explicit
            // override failing must NOT fall through.
            cwd: Some(other.path().to_path_buf()),
            ..no_sources()
        };
        let err = resolve_weft_root_from(&sources).expect_err("bad override fails");
        assert!(err.contains("WEFT_REPO_ROOT"), "{err}");
        assert!(err.contains(&bogus.path().display().to_string()), "{err}");
    }

    /// Rung 2 walks up from the working directory; rung 3 answers when
    /// the compile-time checkout still exists; rung 4 reads what the
    /// installer recorded.
    #[test]
    fn the_ladder_answers_in_order() {
        let checkout = fake_checkout();
        let nested = checkout.path().join("catalog");
        let sources = RootSources { cwd: Some(nested), ..no_sources() };
        assert_eq!(resolve_weft_root_from(&sources).expect("walk-up"), checkout.path());

        let sources =
            RootSources { built_from: Some(checkout.path().to_path_buf()), ..no_sources() };
        assert_eq!(resolve_weft_root_from(&sources).expect("built-from"), checkout.path());

        let sources = RootSources {
            recorded: RecordedInstallRoot::Recorded {
                file: PathBuf::from("/home/x/.local/share/weft/repo-root"),
                root: checkout.path().to_path_buf(),
            },
            ..no_sources()
        };
        assert_eq!(resolve_weft_root_from(&sources).expect("recorded"), checkout.path());
    }

    /// Every terminal error names what was tried and the recovery, and
    /// the three recorded-file endings stay distinct: a stale record, an
    /// unreadable file, and no record are different situations sending
    /// the user to different places.
    #[test]
    fn terminal_errors_name_the_situation() {
        let gone = tempfile::tempdir().expect("tempdir");
        let sources = RootSources {
            recorded: RecordedInstallRoot::Recorded {
                file: PathBuf::from("/home/x/.local/share/weft/repo-root"),
                root: gone.path().to_path_buf(),
            },
            ..no_sources()
        };
        let err = resolve_weft_root_from(&sources).expect_err("stale record");
        assert!(err.contains("not a checkout any more"), "{err}");
        assert!(err.contains("repo-root"), "{err}");

        let sources = RootSources {
            recorded: RecordedInstallRoot::Unreadable {
                file: PathBuf::from("/home/x/.local/share/weft/repo-root"),
                error: "permission denied".into(),
            },
            ..no_sources()
        };
        let err = resolve_weft_root_from(&sources).expect_err("unreadable record");
        assert!(err.contains("could not be read"), "{err}");
        assert!(err.contains("permission denied"), "{err}");

        let err = resolve_weft_root_from(&no_sources()).expect_err("nothing at all");
        assert!(err.contains("no install location was recorded"), "{err}");
        assert!(err.contains("./setup.sh"), "{err}");
    }
}

#[cfg(test)]
mod package_tests {
    use super::*;

    fn copy_dir(src: &Path, dst: &Path) {
        fs::create_dir_all(dst).expect("mkdir");
        for entry in fs::read_dir(src).expect("read dir") {
            let entry = entry.expect("dir entry");
            let to = dst.join(entry.file_name());
            if entry.file_type().expect("file type").is_dir() {
                copy_dir(&entry.path(), &to);
            } else {
                fs::copy(entry.path(), &to).expect("copy file");
            }
        }
    }

    /// Two folders using one package NAME are both left out, each
    /// problem naming both folders; nothing else in the tree is lost.
    #[test]
    fn duplicate_package_name_leaves_both_out() {
        let root = tempfile::tempdir().expect("temp root");
        let stdlib = stdlib_root().expect("stdlib root");
        copy_dir(&stdlib.join("slack"), &root.path().join("a"));
        copy_dir(&stdlib.join("slack"), &root.path().join("b"));
        copy_dir(&stdlib.join("logic"), &root.path().join("logic"));

        let cat = FsCatalog::discover(root.path()).expect("a clash never fails the catalog");
        assert!(!cat.packages().any(|p| p.name == "slack"), "neither claimant wins");
        assert!(cat.packages().any(|p| p.name == "logic"), "the rest loads");
        let clashes: Vec<_> = cat
            .problems()
            .iter()
            .filter(|p| matches!(&p.error, CatalogError::PackageNameCollision { name, .. } if name == "slack"))
            .collect();
        assert_eq!(clashes.len(), 2, "one per claimant: {:?}", cat.problems());
        for clash in clashes {
            let message = clash.error.to_string();
            assert!(message.contains(&root.path().join("a").display().to_string()), "{message}");
            assert!(message.contains(&root.path().join("b").display().to_string()), "{message}");
        }
        let slack_type = cat.problems()[0].node_types.first().expect("the clash names its types").clone();
        assert!(cat.lookup(&slack_type).is_none());
        let told = cat.unavailable(&slack_type).expect("named");
        assert!(told.contains("failed to load") && told.contains("package name 'slack'"), "{told}");
    }

    /// A package named like a bare node's folder (the real case: a
    /// `feed` package beside a bare `rss/feed` node) is the same clash:
    /// both out, both named, neither folder outranks the other.
    #[test]
    fn a_package_named_like_a_bare_node_folder_leaves_both_out() {
        let root = tempfile::tempdir().expect("temp root");
        let bare = root.path().join("rss/feed");
        fs::create_dir_all(&bare).expect("mkdir");
        fs::write(bare.join("metadata.json"), r#"{"type": "Feed", "label": "Feed", "description": "d", "inputs": [], "outputs": []}"#).expect("write");
        fs::write(bare.join("mod.rs"), "// impl\n").expect("write");
        let pkg = root.path().join("mine/feeds");
        fs::create_dir_all(pkg.join("poll")).expect("mkdir");
        fs::write(pkg.join("package.toml"), "[package]\nname = \"feed\"\n").expect("write");
        fs::write(pkg.join("poll/metadata.json"), r#"{"type": "Poll", "label": "Poll", "description": "d", "inputs": [], "outputs": []}"#).expect("write");
        fs::write(pkg.join("poll/mod.rs"), "// impl\n").expect("write");
        copy_dir(&stdlib_root().expect("stdlib root").join("logic"), &root.path().join("logic"));

        let cat = FsCatalog::discover(root.path()).expect("a clash never fails the catalog");
        assert!(cat.lookup("Feed").is_none() && cat.lookup("Poll").is_none());
        assert!(cat.lookup("FirstInOrder").is_some(), "the rest loads");
        for node_type in ["Feed", "Poll"] {
            let told = cat.unavailable(node_type).expect("named");
            assert!(
                told.contains(&bare.display().to_string()) && told.contains(&pkg.display().to_string()),
                "{told}"
            );
        }
    }

    /// A folder with a `metadata.json` and no `mod.rs` is a node still
    /// being written: left out, one warning naming the folder, and
    /// answered by `unavailable`. A package whose only
    /// members are pending is left out too, without the "no members"
    /// complaint; a ready sibling registers as usual.
    #[test]
    fn a_node_without_its_code_is_left_out_and_named() {
        let root = tempfile::tempdir().expect("temp root");
        let stdlib = stdlib_root().expect("stdlib root");
        // A bare node: description only.
        let bare = root.path().join("nodes/resizer");
        fs::create_dir_all(&bare).expect("mkdir");
        fs::write(bare.join("metadata.json"), r#"{"type": "Resizer", "label": "Resizer", "description": "Shrinks a picture.", "inputs": [], "outputs": []}"#).expect("write");
        // A package: one ready member (copied from the stdlib), one pending.
        copy_dir(&stdlib.join("logic"), &root.path().join("nodes/logic"));
        let member = root.path().join("nodes/logic/later");
        fs::create_dir_all(&member).expect("mkdir");
        fs::write(member.join("metadata.json"), r#"{"type": "Later", "label": "Later", "description": "Not yet.", "inputs": [], "outputs": []}"#).expect("write");
        // A package whose only member is pending.
        let alone = root.path().join("nodes/alone");
        fs::create_dir_all(alone.join("only")).expect("mkdir");
        fs::write(alone.join("package.toml"), "[package]\nname = \"alone\"\n").expect("write");
        fs::write(alone.join("only/metadata.json"), r#"{"type": "Only", "label": "Only", "description": "Not yet.", "inputs": [], "outputs": []}"#).expect("write");

        let cat = FsCatalog::discover(&root.path().join("nodes")).expect("a pending node is no error");
        assert!(cat.lookup("Resizer").is_none() && cat.lookup("Later").is_none() && cat.lookup("Only").is_none());
        assert!(cat.lookup("FirstInOrder").is_some(), "the ready sibling registers");
        assert!(cat.packages().any(|p| p.name == "logic") && !cat.packages().any(|p| p.name == "alone"));
        assert_eq!(cat.pending().keys().collect::<Vec<_>>(), ["Later", "Only", "Resizer"]);
        let reason = cat.unavailable("Resizer").expect("named");
        assert!(reason.contains("is not ready yet") && reason.contains("no mod.rs yet") && reason.contains(&bare.display().to_string()), "{reason}");
        assert!(cat.unavailable("Nowhere").is_none());
        assert!(cat.problems().is_empty(), "not ready is no problem: {:?}", cat.problems());
        assert_eq!(cat.warnings().iter().filter(|w| w.contains("no mod.rs yet")).count(), 3, "{:?}", cat.warnings());
        assert_eq!(cat.warnings().len(), 3, "nothing else complained: {:?}", cat.warnings());
    }

    /// A `package.toml` written before any node folder (the real case
    /// that once broke every node) is a package being started: left
    /// out with one line, no problem, and every other node loads.
    #[test]
    fn a_package_with_no_node_yet_is_not_ready_not_broken() {
        let root = tempfile::tempdir().expect("temp root");
        let fresh = root.path().join("fresh");
        fs::create_dir_all(&fresh).expect("mkdir");
        fs::write(fresh.join("package.toml"), "[package]\nname = \"fresh\"\n").expect("write");
        copy_dir(&stdlib_root().expect("stdlib root").join("logic"), &root.path().join("logic"));

        let cat = FsCatalog::discover(root.path()).expect("an empty package is no error");
        assert!(cat.problems().is_empty(), "{:?}", cat.problems());
        assert!(cat.lookup("FirstInOrder").is_some());
        assert!(!cat.packages().any(|p| p.name == "fresh"));
        assert_eq!(cat.warnings().len(), 1, "{:?}", cat.warnings());
        assert!(cat.warnings()[0].contains("has no node in it yet"), "{:?}", cat.warnings());
    }

    /// A node whose metadata does not load is left out with its error,
    /// named by the type it claims; its package siblings and every other
    /// node load. A folder whose type cannot even be read is listed only.
    #[test]
    fn a_broken_node_is_recorded_and_its_siblings_load() {
        let root = tempfile::tempdir().expect("temp root");
        let stdlib = stdlib_root().expect("stdlib root");
        copy_dir(&stdlib.join("logic"), &root.path().join("logic"));
        let bad = root.path().join("logic/bad");
        fs::create_dir_all(&bad).expect("mkdir");
        fs::write(bad.join("metadata.json"), r#"{"type": "Bad", "label": "Bad", "description": "d", "inputs": [], "outputs": [], "nonsense": 1}"#).expect("write");
        fs::write(bad.join("mod.rs"), "// impl\n").expect("write");
        let garbled = root.path().join("garbled");
        fs::create_dir_all(&garbled).expect("mkdir");
        fs::write(garbled.join("metadata.json"), "{ not json").expect("write");

        let cat = FsCatalog::discover(root.path()).expect("a broken node never fails the catalog");
        assert!(cat.lookup("FirstInOrder").is_some(), "the siblings load");
        assert!(cat.package_of("FirstInOrder").is_some_and(|p| !p.node_types.iter().any(|t| t == "Bad")));
        assert!(cat.lookup("Bad").is_none());
        let told = cat.unavailable("Bad").expect("named");
        assert!(told.starts_with("node 'Bad' failed to load:") && told.contains("nonsense"), "{told}");
        assert_eq!(cat.problems().len(), 2, "{:?}", cat.problems());
        assert!(cat.problems().iter().any(|p| p.node_types.is_empty()
            && p.error.to_string().contains(&garbled.display().to_string())));
    }

    /// A package whose `package.toml` does not parse takes its members
    /// out, each named by the type it claims, and the error quotes that
    /// package's own file.
    #[test]
    fn a_broken_package_names_its_members_and_its_own_file() {
        let root = tempfile::tempdir().expect("temp root");
        let stdlib = stdlib_root().expect("stdlib root");
        copy_dir(&stdlib.join("logic"), &root.path().join("logic"));
        let pkg = root.path().join("half");
        fs::create_dir_all(pkg.join("one")).expect("mkdir");
        fs::write(pkg.join("package.toml"), "name = \"half\"\n").expect("write");
        fs::write(pkg.join("one/metadata.json"), r#"{"type": "One", "label": "One", "description": "d", "inputs": [], "outputs": []}"#).expect("write");
        fs::write(pkg.join("one/mod.rs"), "// impl\n").expect("write");

        let cat = FsCatalog::discover(root.path()).expect("never fails the catalog");
        assert!(cat.lookup("FirstInOrder").is_some());
        let told = cat.unavailable("One").expect("named");
        assert!(told.contains(&pkg.join("package.toml").display().to_string()) && told.contains("name = \"half\""), "{told}");
    }

    /// A package whose every member failed still claims its name: a
    /// healthy package of the same name elsewhere is a collision, never
    /// the quiet winner.
    #[test]
    fn an_all_broken_package_still_claims_its_name() {
        let root = tempfile::tempdir().expect("temp root");
        for (dir, node_type, extra) in [("a", "Broken", r#", "nonsense": 1"#), ("b", "Healthy", "")] {
            let pkg = root.path().join(dir);
            fs::create_dir_all(pkg.join("one")).expect("mkdir");
            fs::write(pkg.join("package.toml"), "[package]\nname = \"slack\"\n").expect("write");
            fs::write(
                pkg.join("one/metadata.json"),
                format!(r#"{{"type": "{node_type}", "label": "L", "description": "d", "inputs": [], "outputs": []{extra}}}"#),
            )
            .expect("write");
            fs::write(pkg.join("one/mod.rs"), "// impl\n").expect("write");
        }

        let cat = FsCatalog::discover(root.path()).expect("never fails the catalog");
        assert!(cat.lookup("Healthy").is_none(), "the healthy claimant is taken out too");
        assert!(
            cat.problems().iter().any(|p| matches!(&p.error, CatalogError::PackageNameCollision { name, roots } if name == "slack" && roots.len() == 2)),
            "{:?}",
            cat.problems()
        );
        assert!(cat.unavailable("Healthy").expect("named").contains("slack"));
    }

    /// Two files declaring one type name with different bodies: both
    /// declaring nodes are left out, each told about the clash, and a
    /// node that declares nothing still loads.
    #[test]
    fn a_type_declaration_clash_leaves_both_declarers_out() {
        let root = tempfile::tempdir().expect("temp root");
        for (dir, node_type, body) in [("a", "Alpha", "{ x: String }"), ("b", "Beta", "{ x: Number }")] {
            let d = root.path().join(dir);
            fs::create_dir_all(&d).expect("mkdir");
            fs::write(
                d.join("metadata.json"),
                serde_json::json!({ "type": node_type, "label": node_type, "description": "d",
                                    "inputs": [], "outputs": [], "types": { "Shared": body } })
                    .to_string(),
            )
            .expect("write");
            fs::write(d.join("mod.rs"), "// impl\n").expect("write");
        }
        copy_dir(&stdlib_root().expect("stdlib root").join("logic"), &root.path().join("logic"));

        let cat = FsCatalog::discover(root.path()).expect("never fails the catalog");
        assert!(cat.lookup("FirstInOrder").is_some());
        for node_type in ["Alpha", "Beta"] {
            let told = cat.unavailable(node_type).expect("named");
            assert!(told.contains("declared twice"), "{told}");
        }
    }

    /// A project's nodes live in two trees, `nodes/` and beside the code
    /// under `src/`, walked into ONE catalog: a node in either is found,
    /// and one name in both is the same collision it is inside one tree.
    #[test]
    fn nodes_are_found_across_both_roots_and_a_name_is_unique_across_them() {
        let root = tempfile::tempdir().expect("temp root");
        let stdlib = stdlib_root().expect("stdlib root");
        copy_dir(&stdlib.join("slack"), &root.path().join("nodes/slack"));
        copy_dir(&stdlib.join("logic"), &root.path().join("src/billing/logic"));
        let roots = [root.path().join("nodes"), root.path().join("src")];
        let roots: Vec<&Path> = roots.iter().map(|r| r.as_path()).collect();
        let cat = FsCatalog::discover_roots(&roots).expect("both trees");
        assert!(cat.packages().any(|p| p.name == "slack"), "the shared tree");
        assert!(cat.packages().any(|p| p.name == "logic"), "beside the code");
        // A root that is not there contributes nothing and is no error.
        let missing = root.path().join("nowhere");
        FsCatalog::discover_roots(&[roots[0], &missing]).expect("a missing root is empty");

        copy_dir(&stdlib.join("slack"), &root.path().join("src/slack_again"));
        let cat = FsCatalog::discover_roots(&roots).expect("a clash never fails the catalog");
        assert!(
            cat.problems().iter().any(|p| matches!(&p.error, CatalogError::PackageNameCollision { name, .. } if name == "slack")),
            "one name in both trees is a collision: {:?}",
            cat.problems()
        );
        assert!(cat.packages().any(|p| p.name == "logic"));
    }

    /// Every shipped stdlib `metadata.json` parses under the strict schema
    /// (`deny_unknown_fields` on every nested struct). A typo or stale key in
    /// any of them is a build-breaking error, not a silently-dropped value, so
    /// this test is what turns "the field vanished on the wire" bugs into a
    /// red suite the moment a catalog file drifts from the metadata types.
    #[test]
    fn every_stdlib_node_loads_strict() {
        let cat = FsCatalog::discover(&stdlib_root().expect("stdlib root")).expect("stdlib root reads");
        assert!(cat.problems().is_empty(), "every stdlib node must load: {:#?}", cat.problems());
        assert!(!cat.all().is_empty(), "catalog discovered no nodes");
    }

    /// The stdlib census for the compact wiring view: every top-level
    /// key any shipped `metadata.json` actually uses is classified,
    /// kept or dropped, in weft-core's compact lists. A new top-level
    /// key with `skip_serializing_if` is absent from a minimal parse,
    /// so the core-side classification test cannot see it; this is
    /// the net that catches it the first time a stdlib node uses it,
    /// forcing the keep-or-drop decision instead of a silent drop.
    #[test]
    fn every_stdlib_top_level_key_is_classified_for_compact() {
        use weft_core::node::{COMPACT_DROP_TOP_LEVEL, COMPACT_KEEP_TOP_LEVEL};

        let cat = FsCatalog::discover(&stdlib_root().expect("stdlib root")).unwrap();
        for entry in cat.iter() {
            let resolved = serde_json::to_value(entry.metadata.resolved()).unwrap();
            for key in resolved.as_object().unwrap().keys() {
                assert!(
                    COMPACT_KEEP_TOP_LEVEL.contains(&key.as_str())
                        || COMPACT_DROP_TOP_LEVEL.contains(&key.as_str()),
                    "unclassified top-level key `{key}` on `{}`: \
                     decide keep or drop in weft-core's compact lists",
                    entry.node_type
                );
            }
        }
    }

    /// The compact wiring view stamps each access node's access widget
    /// with the service name and `connection_optional` from its own
    /// recipe (the same stamp the compiler applies at enrich time),
    /// because the compact view drops the recipe itself and those two
    /// facts are what a wiring reader needs from it: which service,
    /// and whether the node runs with no connection picked.
    #[test]
    fn compact_view_stamps_the_access_widget() {
        let cat = FsCatalog::discover(&stdlib_root().expect("stdlib root")).unwrap();
        let mut access_nodes = 0;
        for entry in cat.iter() {
            let Some(recipe) = &entry.metadata.service else { continue };
            access_nodes += 1;
            let compact = entry.metadata.compact_json();
            let widget = compact["inputs"]
                .as_array()
                .unwrap()
                .iter()
                .find_map(|i| i["widget"].as_object().filter(|w| w["kind"] == "access"))
                .unwrap_or_else(|| panic!("{}: an access node needs an access widget", entry.node_type));
            assert_eq!(
                widget["service"].as_str(),
                Some(recipe.service.as_str()),
                "{}: the widget names the recipe's service",
                entry.node_type
            );
            if recipe.connection_optional {
                assert_eq!(
                    widget["optional"], true,
                    "{}: connection_optional is stamped onto the widget",
                    entry.node_type
                );
            } else {
                assert!(
                    widget.get("optional").is_none(),
                    "{}: a required connection omits `optional` (serde skips false)",
                    entry.node_type
                );
            }
        }
        assert!(
            access_nodes >= 10,
            "the stdlib ships a dozen access nodes; only {access_nodes} carried a recipe"
        );
    }

    #[test]
    fn human_specs_loaded() {
        let cat = FsCatalog::discover(&stdlib_root().expect("stdlib root")).unwrap();
        let q = cat.entry("HumanQuery").expect("HumanQuery missing");
        let t = cat.entry("HumanTrigger").expect("HumanTrigger missing");
        let specs = |e: &CatalogEntry| {
            e.metadata.ports_from_config.as_ref().map(|p| p.specs.len()).unwrap_or(0)
        };
        assert!(specs(q) > 0, "HumanQuery specs empty");
        assert!(specs(t) > 0, "HumanTrigger specs empty");
        assert_eq!(q.package_key, t.package_key, "both nodes should share package_key");
        let pkg = cat.package_of("HumanQuery").expect("pkg_of missing");
        assert_eq!(pkg.name, "human");
        assert_eq!(pkg.node_types.len(), 2);
        assert_eq!(pkg.shared_rs.len(), 1, "should have form_helpers.rs");
    }

    /// Bailey triad: bridge is the infra (requires_infra + locally
    /// built image), receive is a trigger (no infra), send is a
    /// normal Fire-phase node (no infra). All three must load from
    /// the catalog so the package compiles into a project binary.
    #[test]
    fn bailey_triad_loaded() {
        let cat = FsCatalog::discover(&stdlib_root().expect("stdlib root")).unwrap();
        let bridge = cat.entry("BaileyBridge").expect("BaileyBridge missing");
        assert!(bridge.metadata.requires_infra, "bridge must be infra");
        assert_eq!(
            bridge.metadata.images,
            vec!["images/bridge".to_string()],
            "bridge declares its locally-built infra image",
        );
        assert_eq!(
            bridge.metadata.features.live_endpoint.as_deref(),
            Some("api"),
            "bridge opts into /live by naming the endpoint that serves it",
        );

        let recv = cat.entry("BaileyReceive").expect("BaileyReceive missing");
        assert!(!recv.metadata.requires_infra, "receive must NOT require infra");
        assert!(recv.metadata.features.is_trigger, "receive is a trigger");

        let send = cat.entry("BaileySend").expect("BaileySend missing");
        assert!(!send.metadata.requires_infra, "send must NOT require infra");
        assert!(!send.metadata.features.is_trigger, "send is a normal node");

        // The three must live under the same package so the codegen
        // bundles them together. (`package_of` returns the package
        // descriptor for any node_type in it.)
        let pkg = cat.package_of("BaileyBridge").expect("bridge package");
        assert_eq!(pkg.name, "bailey", "the Bailey triad must share a package");
        assert!(
            pkg.node_types.iter().any(|t| t == "BaileyBridge")
                && pkg.node_types.iter().any(|t| t == "BaileyReceive")
                && pkg.node_types.iter().any(|t| t == "BaileySend"),
            "package must contain all three node types, got {:?}",
            pkg.node_types,
        );
    }
}
