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

use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use weft_core::node::{MetadataCatalog, NodeMetadata};

/// Directory names that are never part of a node's source tree:
/// build outputs and VCS/dependency caches. The single policy shared
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
pub const NODE_TREE_EXCLUDE: &[&str] = &["target", "node_modules", ".git", ".weft"];

/// True if `name` is an excluded node-tree directory. Convenience over
/// `NODE_TREE_EXCLUDE.contains(&name)` for callers matching an
/// `OsStr`/`Cow<str>` entry name.
pub fn is_node_tree_excluded(name: &str) -> bool {
    NODE_TREE_EXCLUDE.contains(&name)
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

/// True if `s` is a plain Rust identifier (`[A-Za-z_][A-Za-z0-9_]*`).
/// Every name codegen interpolates into generated Rust source (a
/// node's `node_type`, a shared file's module stem) must pass this,
/// so a bad name fails at discovery/emit with the offending file
/// named instead of surfacing as a confusing rustc error deep inside
/// generated code.
pub fn is_rust_identifier(s: &str) -> bool {
    let mut chars = s.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
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

/// How discovery reacts to malformed nodes and duplicate node types.
///
/// The traversal (what counts as a unit, how packages and nesting
/// work) is identical for both; only the error reaction differs, so
/// the editor-live path and the build path never disagree about the
/// shape of the catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiscoverPolicy {
    /// Build path: a malformed `metadata.json` or a duplicate node
    /// type is a hard error. The catalog must be sound to compile.
    Strict,
    /// Editor-live path: malformed nodes and duplicates are skipped
    /// with a warning, never an error. A node mid-rename has a
    /// transient parse error the editor should surface but not crash
    /// on. Collected in `FsCatalog::warnings`.
    Lenient,
}

#[derive(Debug)]
pub struct FsCatalog {
    entries: HashMap<String, CatalogEntry>,
    /// All discovered packages, keyed by package root. Each
    /// `CatalogEntry` has a `package_key` pointing back in here.
    packages: HashMap<PathBuf, Package>,
    /// Soft errors collected under `DiscoverPolicy::Lenient` (always
    /// empty under `Strict`, which errors instead).
    warnings: Vec<String>,
    /// The project's resolved type registry: builtin aliases plus every
    /// `types` declaration harvested from the tree's `metadata.json`
    /// files. Built BEFORE any metadata is deserialized (port type
    /// strings may use the declared names) and carried here so
    /// compile/enrich callers can activate the same registry.
    type_registry: std::sync::Arc<weft_core::weft_type::TypeRegistry>,
}

impl FsCatalog {
    /// Walk the project's `nodes/` root strictly: every node must be
    /// well-formed and every `node_type` unique. There is one source:
    /// the project owns all its nodes (the stdlib is cloned in at
    /// `weft new`). A duplicate `node_type` is an ambiguous collision,
    /// not a shadow, and fails loudly. This is the build path.
    pub fn discover(root: &Path) -> Result<Self, CatalogError> {
        Self::discover_with_policy(root, DiscoverPolicy::Strict)
    }

    /// Walk the project's `nodes/` root under an explicit policy. Both
    /// policies share one traversal; see `DiscoverPolicy`. `Lenient`
    /// never returns `Err` from a malformed node or a collision (those
    /// land in `warnings`); it can still fail on an unreadable
    /// directory.
    pub fn discover_with_policy(
        root: &Path,
        policy: DiscoverPolicy,
    ) -> Result<Self, CatalogError> {
        let mut cat = Self {
            entries: HashMap::new(),
            packages: HashMap::new(),
            warnings: Vec::new(),
            type_registry: std::sync::Arc::new(weft_core::weft_type::TypeRegistry::builtin()),
        };
        if root.exists() {
            // Type declarations first: port type strings in any
            // metadata.json may use the declared names, so the registry
            // must exist before a single NodeMetadata is deserialized.
            match build_type_registry(root, policy, &mut cat.warnings)? {
                Some(registry) => cat.type_registry = std::sync::Arc::new(registry),
                None => { /* Lenient fallback: builtin only, warned. */ }
            }
            let registry = cat.type_registry.clone();
            let mut ctx = DiscoverCtx {
                policy,
                cat: &mut cat,
                chain: Default::default(),
                done: Default::default(),
            };
            registry.scoped(|| visit_dir(root, &mut ctx))?;
        }
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
            warnings: Vec::new(),
            type_registry: std::sync::Arc::new(weft_core::weft_type::TypeRegistry::builtin()),
        }
    }

    /// Soft errors collected during a `Lenient` discover (malformed
    /// `metadata.json`, duplicate node types). Empty after `Strict`.
    pub fn warnings(&self) -> &[String] {
        &self.warnings
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
/// whose package name varies (libpython) fills one key per
/// (distro, version) it verified.
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
/// resolves to `/weft/project-nodes/base_catalog/basic/exec_python/foo-config.txt`
/// if the node lives at `nodes/base_catalog/basic/exec_python/`.
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
///   worker binary. `libpython3-dev`, `pkg-config`, `libssl-dev`,
///   and so on. These end up in the builder stage and are
///   discarded before the runtime image is sealed.
/// - `runtime`: packages the RUNTIME container needs to RUN the
///   compiled binary. `libpython3.11-minimal`, `ca-certificates`.
///
/// Each stage has the same shape: a `BTreeMap<manager, BTreeMap<
/// distro_key, Vec<String>>>`. `distro_key` is `<distro>_<major>`
/// (e.g. `debian_12`, `ubuntu_24_04`, `alpine_3_19`, `rocky_9`)
/// or the special `default` fallback.
///
/// ```toml
/// [system.build.apt]
/// default = ["libpython3-dev", "pkg-config"]
///
/// [system.runtime.apt]
/// default = ["python3-minimal"]
/// debian_12 = ["libpython3.11-minimal"]
/// debian_13 = ["libpython3.13-minimal"]
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

/// Filesystem path to this repo's bundled stdlib catalog. Resolved
/// at compile time from the crate's own `CARGO_MANIFEST_DIR`.
///
/// Consumed by `weft new` (clones the catalog into the new project's `nodes/`
/// so the project is self-contained) and by any environment that compiles at
/// RUNTIME rather than from a dev checkout. So this MUST be runtime-resolvable,
/// not just a compile-time path: honor `WEFT_REPO_ROOT` first (for environments
/// where the compile-time `CARGO_MANIFEST_DIR` does not exist), exactly like
/// `weft_compiler::build::resolve_weft_root`. Fall back to the repo layout
/// (`<weft-repo>/crates/weft-catalog` → parent → parent → `catalog`) for local
/// dev where the env is unset.
pub fn stdlib_root() -> PathBuf {
    weft_repo_root()
        .expect("stdlib_root: cannot resolve weft repo layout")
        .join("catalog")
}

/// Resolve the on-disk weft workspace root: honor `WEFT_REPO_ROOT` first (set in a
/// built image, where the compile-time `CARGO_MANIFEST_DIR` does not
/// exist), else fall back to the repo layout (`<repo>/crates/weft-catalog` ->
/// parent -> parent). THE single resolver; `weft_compiler::build::resolve_weft_root`
/// delegates here so the two can't drift (they must return the same path or the
/// stdlib seed and the build context disagree). Fallible (None when neither the env
/// nor the layout resolves) so each caller chooses panic vs error.
pub fn weft_repo_root() -> Option<PathBuf> {
    if let Ok(root) = std::env::var("WEFT_REPO_ROOT") {
        return Some(PathBuf::from(root));
    }
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .map(|p| p.to_path_buf())
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

/// Discovery state threaded through the traversal: the policy plus the
/// catalog being built. Both `Strict` and `Lenient` share this exact
/// traversal; the policy only changes how a malformed node or a
/// collision is handled (`soft_fail`).
struct DiscoverCtx<'a> {
    policy: DiscoverPolicy,
    cat: &'a mut FsCatalog,
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
    /// Resolve a soft failure (malformed node, duplicate type) per the
    /// policy: `Strict` propagates the error, `Lenient` records a
    /// warning and returns `Ok(())` so the walk continues.
    fn soft_fail(&mut self, err: CatalogError) -> Result<(), CatalogError> {
        match self.policy {
            DiscoverPolicy::Strict => Err(err),
            DiscoverPolicy::Lenient => {
                self.cat.warnings.push(err.to_string());
                Ok(())
            }
        }
    }

    /// Insert an entry. A `node_type` collision is a soft failure
    /// (there is no shadowing with a single root, so a duplicate is
    /// ambiguous): `Strict` errors, `Lenient` warns and keeps the
    /// first. Returns whether the entry was actually inserted, so the
    /// caller's `Package.node_types` lists only the types `entries`
    /// attributes to it (the two views can't disagree under Lenient).
    fn insert_entry(&mut self, entry: CatalogEntry) -> Result<bool, CatalogError> {
        if let Some(existing) = self.cat.entries.get(&entry.node_type) {
            self.soft_fail(CatalogError::Collision {
                node_type: entry.node_type.clone(),
                first: existing.source_dir.clone(),
                second: entry.source_dir.clone(),
            })?;
            return Ok(false);
        }
        // A service is found BY NAME, by the store that keeps its
        // connections and by the compiler resolving what a node
        // publishes. Two nodes claiming one name make that lookup a
        // coin toss, so it is refused here, where the other identity
        // collisions are.
        if let Some(service) = entry.metadata.service.as_ref().map(|s| s.service.clone()) {
            if let Some(existing) = self
                .cat
                .entries
                .values()
                .find(|e| e.metadata.service.as_ref().is_some_and(|s| s.service == service))
            {
                self.soft_fail(CatalogError::ServiceCollision {
                    service,
                    first: existing.node_type.clone(),
                    second: entry.node_type.clone(),
                })?;
                return Ok(false);
            }
        }
        self.cat.entries.insert(entry.node_type.clone(), entry);
        Ok(true)
    }

    /// Claim a package NAME for a unit about to register, refusing a
    /// duplicate the same way a duplicate node type is refused: every
    /// by-name package lookup (the test-crate emit picks its package
    /// by name) must resolve to exactly one root, so a second root
    /// with the same name is ambiguous. `Strict` errors, `Lenient`
    /// warns and keeps the first (the whole second unit is skipped,
    /// entries included, so no entry ever points at an unregistered
    /// package). Returns whether the unit may register.
    fn claim_package_name(&mut self, name: &str, root: &Path) -> Result<bool, CatalogError> {
        if let Some(existing) = self.cat.packages.values().find(|p| p.name == name) {
            self.soft_fail(CatalogError::PackageNameCollision {
                name: name.to_string(),
                first: existing.root.clone(),
                second: root.to_path_buf(),
            })?;
            return Ok(false);
        }
        Ok(true)
    }
}

/// Harvest every `types` declaration under `root` and build the
/// project's [`weft_core::weft_type::TypeRegistry`]. This walk is
/// deliberately simpler than unit discovery: type declarations are
/// global, so it reads the `types` key of EVERY `metadata.json` in the
/// tree (member and package-root alike, same symlink-following and
/// exclusion policy via `read_node_dir`) with no unit semantics. Malformed JSON is left
/// for the main discovery pass to report (it owns metadata errors);
/// only the declarations themselves fail here. Under `Lenient` a
/// registry build failure becomes a warning and `None` (builtin-only),
/// so the editor keeps rendering while the author fixes the clash.
fn build_type_registry(
    root: &Path,
    policy: DiscoverPolicy,
    warnings: &mut Vec<String>,
) -> Result<Option<weft_core::weft_type::TypeRegistry>, CatalogError> {
    let mut declarations: Vec<(String, String, String)> = Vec::new();
    harvest_type_declarations(root, &mut declarations)?;
    // Lexical order by origin path: `fs::read_dir` order is
    // filesystem-dependent, and a clash error names the SECOND origin,
    // so an unsorted harvest would blame a different file per machine.
    declarations.sort_by(|a, b| a.2.cmp(&b.2).then_with(|| a.0.cmp(&b.0)));
    match weft_core::weft_type::TypeRegistry::build(&declarations) {
        Ok(registry) => Ok(Some(registry)),
        Err(error) => match policy {
            DiscoverPolicy::Strict => Err(CatalogError::Parse {
                path: root.to_path_buf(),
                error: format!("type declarations: {error}"),
            }),
            DiscoverPolicy::Lenient => {
                warnings.push(format!("type declarations: {error}"));
                Ok(None)
            }
        },
    }
}

fn harvest_type_declarations(
    dir: &Path,
    out: &mut Vec<(String, String, String)>,
) -> Result<(), CatalogError> {
    let mut chain = Vec::new();
    let mut done = std::collections::HashSet::new();
    harvest_type_declarations_inner(dir, out, &mut chain, &mut done)
}

fn harvest_type_declarations_inner(
    dir: &Path,
    out: &mut Vec<(String, String, String)>,
    chain: &mut Vec<PathBuf>,
    done: &mut std::collections::HashSet<PathBuf>,
) -> Result<(), CatalogError> {
    let canon = guard_node_tree_cycle(dir, chain)
        .map_err(|error| CatalogError::Io { path: dir.to_path_buf(), error })?;
    // Same dedupe as discovery: a folder reachable by two symlink paths
    // yields its declarations once.
    if !done.insert(canon.clone()) {
        return Ok(());
    }
    chain.push(canon);
    let result = harvest_type_declarations_entries(dir, out, chain, done);
    chain.pop();
    result
}

fn harvest_type_declarations_entries(
    dir: &Path,
    out: &mut Vec<(String, String, String)>,
    chain: &mut Vec<PathBuf>,
    done: &mut std::collections::HashSet<PathBuf>,
) -> Result<(), CatalogError> {
    for entry in read_node_dir(dir)? {
        match entry {
            NodeDirEntry::Dir(path) => harvest_type_declarations_inner(&path, out, chain, done)?,
            NodeDirEntry::File(path) => {
                if path.file_name().and_then(|n| n.to_str()) != Some("metadata.json") {
                    continue;
                }
                // Raw read: the typed NodeMetadata parse needs the
                // registry we are building. Malformed JSON is the main
                // pass's error to report (it owns metadata errors), so
                // it is skipped here; an UNREADABLE file is not (the
                // main pass treats a package root's partial as defaults
                // to merge and may never report it, and a vanished
                // declaration would surface much later as an unresolved
                // port type on an innocent node).
                let raw = fs::read_to_string(&path)
                    .map_err(|error| CatalogError::Io { path: path.clone(), error })?;
                let Ok(value) = serde_json::from_str::<serde_json::Value>(&raw) else { continue };
                let Some(types) = value.get("types") else { continue };
                let Some(map) = types.as_object() else {
                    return Err(CatalogError::Parse {
                        path,
                        error: "`types` must be an object of name -> type string".into(),
                    });
                };
                for (name, body) in map {
                    let Some(body) = body.as_str() else {
                        return Err(CatalogError::Parse {
                            path,
                            error: format!("`types.{name}` must be a type string"),
                        });
                    };
                    out.push((name.clone(), body.to_string(), path.display().to_string()));
                }
            }
        }
    }
    Ok(())
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

fn visit_dir(dir: &Path, ctx: &mut DiscoverCtx<'_>) -> Result<(), CatalogError> {
    let canon = guard_node_tree_cycle(dir, &ctx.chain)
        .map_err(|error| CatalogError::Io { path: dir.to_path_buf(), error })?;
    // A folder already fully processed (reached again through a second
    // symlink path) registers nothing twice and is not re-walked.
    if !ctx.done.insert(canon.clone()) {
        return Ok(());
    }
    let entries = read_node_dir(dir)?;
    if has_node_file(&entries, "package.toml") {
        return register_package(dir, &dir.join("package.toml"), entries, ctx);
    }
    if has_node_file(&entries, "metadata.json") {
        return register_bare_node(dir, ctx);
    }
    ctx.chain.push(canon);
    let result = visit_dir_children(entries, ctx);
    ctx.chain.pop();
    result
}

fn visit_dir_children(
    entries: Vec<NodeDirEntry>,
    ctx: &mut DiscoverCtx<'_>,
) -> Result<(), CatalogError> {
    for entry in entries {
        if let NodeDirEntry::Dir(path) = entry {
            visit_dir(&path, ctx)?;
        }
    }
    Ok(())
}

/// A bare node: the directory IS the node. It is its own degenerate
/// package (one member, no shared code, deps from its `deps.toml`, no
/// package-level metadata defaults: its own `metadata.json` is already
/// the whole story).
fn register_bare_node(dir: &Path, ctx: &mut DiscoverCtx<'_>) -> Result<(), CatalogError> {
    let entry = match load_node_entry(dir, dir, None) {
        Ok(e) => e,
        Err(e) => return ctx.soft_fail(e),
    };
    // The package name is the directory name. A non-UTF-8 name is a
    // node weft can't compile (it becomes a Rust module ident), so fail
    // loudly instead of substituting a placeholder that would collide
    // with any other unnameable node.
    let package_name = match dir.file_name().and_then(|n| n.to_str()) {
        Some(n) => n.to_string(),
        None => {
            return ctx.soft_fail(CatalogError::Parse {
                path: dir.to_path_buf(),
                error: "node directory name is not valid UTF-8".into(),
            })
        }
    };
    if !ctx.claim_package_name(&package_name, dir)? {
        return Ok(());
    }
    let node_type = entry.node_type.clone();
    // Only register the package if the node's type actually landed in
    // `entries` (Lenient may drop a collision with a warning); otherwise
    // the package would claim a type owned by another package.
    if ctx.insert_entry(entry)? {
        ctx.cat.packages.insert(
            dir.to_path_buf(),
            Package {
                root: dir.to_path_buf(),
                name: package_name,
                node_types: vec![node_type],
                shared_rs: Vec::new(),
                package_deps: None,
            },
        );
    }
    Ok(())
}

/// A package root: `package.toml` names the package and carries shared
/// cargo deps. Members are auto-detected (any immediate subdir with a
/// `metadata.json`), so the author never maintains a node list. Shared
/// `.rs` files at the root are bundled into the package module.
fn register_package(
    dir: &Path,
    toml_path: &Path,
    entries: Vec<NodeDirEntry>,
    ctx: &mut DiscoverCtx<'_>,
) -> Result<(), CatalogError> {
    let raw = fs::read_to_string(toml_path).map_err(|e| CatalogError::Io {
        path: toml_path.to_path_buf(),
        error: e,
    })?;
    let parsed: PackageToml = match toml::from_str(&raw) {
        Ok(p) => p,
        Err(e) => {
            return ctx.soft_fail(CatalogError::Parse {
                path: toml_path.to_path_buf(),
                error: e.to_string(),
            })
        }
    };
    if !ctx.claim_package_name(&parsed.package.name, dir)? {
        return Ok(());
    }

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
        match load_package_defaults(dir) {
            Ok(d) => d,
            Err(e) => return ctx.soft_fail(e),
        }
    } else {
        None
    };

    // Auto-detect members: every immediate subdir whose own
    // `read_node_dir` view contains a `metadata.json`. Shared `.rs` files live at the
    // package root. All of this is the `read_node_dir` view (the
    // package's `entries` plus each member's), so a package's tree is
    // seen identically by discovery, staging, and hashing.
    let mut node_types: Vec<String> = Vec::new();
    let mut shared_rs: Vec<PathBuf> = Vec::new();
    for entry in entries {
        match entry {
            NodeDirEntry::Dir(path) => {
                let member_entries = read_node_dir(&path)?;
                if !has_node_file(&member_entries, "metadata.json") {
                    continue;
                }
                match load_node_entry(&path, dir, package_defaults.as_ref()) {
                    Ok(entry) => {
                        let node_type = entry.node_type.clone();
                        // Only list the type if it was actually inserted.
                        // Under Lenient a collision is dropped-with-warning;
                        // listing it anyway would make this package claim a
                        // type whose entry points at a different package.
                        if ctx.insert_entry(entry)? {
                            node_types.push(node_type);
                        }
                    }
                    Err(e) => ctx.soft_fail(e)?,
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
    node_types.sort();
    shared_rs.sort();

    if node_types.is_empty() {
        return ctx.soft_fail(CatalogError::Parse {
            path: toml_path.to_path_buf(),
            error: format!(
                "package '{}' has no member nodes (no subdir with metadata.json under {})",
                parsed.package.name,
                dir.display()
            ),
        });
    }

    ctx.cat.packages.insert(
        dir.to_path_buf(),
        Package {
            root: dir.to_path_buf(),
            name: parsed.package.name,
            node_types,
            shared_rs,
            package_deps: Some(parsed.dependencies),
        },
    );
    Ok(())
}

/// Load a package root's partial `metadata.json` (the defaults its members
/// inherit key-by-key). The CALLER decides the file exists, from the
/// `read_node_dir` view it already holds (`has_node_file`), so this never
/// re-`stat`s the path. A file that is not a JSON object, or that carries an
/// identity key, is an error: silently ignoring it would make every member
/// quietly miss its inherited keys.
fn load_package_defaults(
    package_root: &Path,
) -> Result<Option<serde_json::Map<String, serde_json::Value>>, CatalogError> {
    let path = package_root.join("metadata.json");
    let raw = fs::read_to_string(&path).map_err(|e| CatalogError::Io {
        path: path.clone(),
        error: e,
    })?;
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
    Ok(Some(obj))
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
) -> Result<CatalogEntry, CatalogError> {
    let meta_path = node_dir.join("metadata.json");
    let raw = fs::read_to_string(&meta_path).map_err(|e| CatalogError::Io {
        path: meta_path.clone(),
        error: e,
    })?;
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
    let metadata: NodeMetadata =
        serde_json::from_value(value).map_err(|e| CatalogError::Parse {
            path: meta_path.clone(),
            // A stale stdlib COPY is the common way to hold metadata this
            // weft no longer accepts; name the one-command re-sync.
            error: if meta_path.components().any(|c| c.as_os_str() == "base_catalog") {
                format!("{e} (a stale base_catalog copy? run `weft catalog update` in the project to re-sync it)")
            } else {
                e.to_string()
            },
        })?;
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

    Ok(CatalogEntry {
        node_type: metadata.node_type.clone(),
        metadata,
        source_dir: node_dir.to_path_buf(),
        package_key: package_key.to_path_buf(),
    })
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
    #[error("node type '{node_type}' declared twice: {first} and {second}")]
    Collision {
        node_type: String,
        first: PathBuf,
        second: PathBuf,
    },
    #[error(
        "the '{service}' service is declared by two nodes ({first} and {second}); a service \
         names one sign-in, and everything that stores or publishes a connection finds it \
         by that name alone"
    )]
    ServiceCollision {
        service: String,
        first: String,
        second: String,
    },
    #[error("package name '{name}' declared twice: {first} and {second}")]
    PackageNameCollision {
        name: String,
        first: PathBuf,
        second: PathBuf,
    },
}

#[cfg(test)]
mod package_tests {
    use super::*;

    #[test]
    fn rust_identifier_check() {
        for ok in ["Text", "SlackSendMessage", "_hidden", "a1", "A_b_2"] {
            assert!(is_rust_identifier(ok), "{ok} should pass");
        }
        for bad in ["", "1abc", "my-node", "my.node", "with space", "émoji", "a\"b"] {
            assert!(!is_rust_identifier(bad), "{bad} should fail");
        }
    }

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

    /// Two roots declaring the same package NAME are ambiguous the
    /// same way two roots declaring the same node type are: Strict
    /// errors naming both roots, Lenient warns and keeps the first.
    #[test]
    fn duplicate_package_name_is_refused() {
        let root = tempfile::tempdir().expect("temp root");
        // Same package.toml name under two different directory roots.
        // (The node-type collision inside would also fire, but the
        // name is claimed BEFORE any entry inserts, so the error must
        // be the package-name collision.)
        copy_dir(&stdlib_root().join("slack"), &root.path().join("a"));
        copy_dir(&stdlib_root().join("slack"), &root.path().join("b"));

        let err = FsCatalog::discover(root.path()).expect_err("duplicate name refused");
        assert!(
            matches!(&err, CatalogError::PackageNameCollision { name, .. } if name == "slack"),
            "expected a package-name collision, got: {err}"
        );

        let cat = FsCatalog::discover_with_policy(root.path(), DiscoverPolicy::Lenient)
            .expect("lenient never errors on a collision");
        assert_eq!(
            cat.packages().count(),
            1,
            "lenient keeps exactly the first root"
        );
        assert!(
            cat.warnings().iter().any(|w| w.contains("declared twice")),
            "the drop is warned, not silent: {:?}",
            cat.warnings()
        );
    }

    /// Every shipped stdlib `metadata.json` parses under the strict schema
    /// (`deny_unknown_fields` on every nested struct). A typo or stale key in
    /// any of them is a build-breaking error, not a silently-dropped value, so
    /// this test is what turns "the field vanished on the wire" bugs into a
    /// red suite the moment a catalog file drifts from the metadata types.
    #[test]
    fn every_stdlib_node_loads_strict() {
        let cat = FsCatalog::discover(&stdlib_root())
            .expect("all stdlib metadata.json must load under strict parse");
        assert!(!cat.all().is_empty(), "catalog discovered no nodes");
    }

    #[test]
    fn human_specs_loaded() {
        let cat = FsCatalog::discover(&stdlib_root()).unwrap();
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
        let cat = FsCatalog::discover(&stdlib_root()).unwrap();
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
