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
//!   `provider` or `formFieldSpecs`) every member inherits key-by-key;
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

use weft_core::node::{MetadataCatalog, NodeMetadata, ProviderDecl};

/// Directory names that are never part of a node's source tree:
/// build outputs and VCS/dependency caches. The single policy shared
/// by every traversal of a node directory tree (discovery's descent,
/// the build's staging copy, and the source-hash walk) so they agree
/// on exactly which bytes constitute a node. Diverging here is how a
/// stale worker image gets served (hash misses a file the build
/// copies) or a build context bloats (copies a cache the worker never
/// compiles). Symlinks are also never followed by any node-tree walk
/// (a loop under user-authored `nodes/` is a real input); that rule
/// lives at each walk site since it's a traversal mechanic, not a name.
pub const NODE_TREE_EXCLUDE: &[&str] = &["target", "node_modules", ".git", ".weft"];

/// True if `name` is an excluded node-tree directory. Convenience over
/// `NODE_TREE_EXCLUDE.contains(&name)` for callers matching an
/// `OsStr`/`Cow<str>` entry name.
pub fn is_node_tree_excluded(name: &str) -> bool {
    NODE_TREE_EXCLUDE.contains(&name)
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
        };
        if root.exists() {
            let mut ctx = DiscoverCtx { policy, cat: &mut cat };
            visit_dir(root, &mut ctx)?;
        }
        Ok(cat)
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
}

impl FsCatalog {
    /// Source directory of a node (path to the dir containing
    /// `mod.rs`). Used by codegen to resolve `#[path]` includes and
    /// the node's `deps.toml`.
    pub fn source_dir(&self, node_type: &str) -> Option<&Path> {
        self.entries.get(node_type).map(|e| e.source_dir.as_path())
    }

    /// The paid service a node's source declares (its metadata's `provider`
    /// key, own or inherited from the package defaults), when it declares
    /// one. `None` = the node type is unknown or declares nothing.
    pub fn provider_of(&self, node_type: &str) -> Option<&ProviderDecl> {
        self.entries.get(node_type).and_then(|e| e.metadata.provider.as_ref())
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
        self.cat.entries.insert(entry.node_type.clone(), entry);
        Ok(true)
    }
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

/// Read a directory's immediate children under the node-tree policy:
/// never follow symlinks, skip `NODE_TREE_EXCLUDE` names. The single
/// traversal mechanic for discovery; `visit_dir` recurses its dirs and
/// `register_package` matches members/shared files against the same
/// entries, so the two can't diverge on what a node tree contains (the
/// de-sync that serves a stale image or bloats the build context).
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
        // `file_type()` does not follow symlinks: a loop under
        // user-authored `nodes/` must never send the walk infinite,
        // and a symlinked member must not be discovered-then-dropped
        // by the (no-follow) staging copy and hash walk.
        let ft = child.file_type().map_err(|e| CatalogError::Io {
            path: child.path(),
            error: e,
        })?;
        if ft.is_symlink() {
            continue;
        }
        if ft.is_dir() {
            out.push(NodeDirEntry::Dir(child.path()));
        } else if ft.is_file() {
            out.push(NodeDirEntry::File(child.path()));
        }
    }
    Ok(out)
}

/// True if the entries contain a real (non-symlink) file named `name`.
/// Unit detection goes through this rather than re-`stat`ing with
/// `Path::is_file` (which follows symlinks): detection must see the
/// SAME no-follow view the tree walk and staging copy see, or a unit
/// defined by a symlinked `metadata.json` / `package.toml` would be
/// discovered yet have its manifest dropped from the build context.
fn has_node_file(entries: &[NodeDirEntry], name: &str) -> bool {
    entries.iter().any(|e| {
        matches!(e, NodeDirEntry::File(p)
            if p.file_name().and_then(|n| n.to_str()) == Some(name))
    })
}

fn visit_dir(dir: &Path, ctx: &mut DiscoverCtx<'_>) -> Result<(), CatalogError> {
    let entries = read_node_dir(dir)?;
    if has_node_file(&entries, "package.toml") {
        return register_package(dir, &dir.join("package.toml"), entries, ctx);
    }
    if has_node_file(&entries, "metadata.json") {
        return register_bare_node(dir, ctx);
    }
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

    // Package-level metadata defaults: an OPTIONAL, PARTIAL `metadata.json`
    // at the package root. Every member inherits its top-level keys unless
    // the member's own `metadata.json` carries the key (key-by-key, member
    // wins; see `load_node_entry`). One place for whatever a package's nodes
    // share (`provider`, `formFieldSpecs`, future keys), instead of one
    // sidecar file per feature. `type` is a node's identity and can never be
    // shared, so its presence here is an error, not a default.
    // Presence is decided by the SAME no-follow view the walk, staging, and
    // hash use (`has_node_file`), never a re-`stat` that follows symlinks: a
    // symlinked package-root metadata.json is dropped from the build context,
    // so seeing it here would split the catalog's view from the compiled
    // node's (whose derive reads the staged, no-follow tree).
    let package_defaults = if has_node_file(&entries, "metadata.json") {
        match load_package_defaults(dir) {
            Ok(d) => d,
            Err(e) => return ctx.soft_fail(e),
        }
    } else {
        None
    };

    // Auto-detect members: every immediate subdir whose own no-follow
    // view contains a `metadata.json`. Shared `.rs` files live at the
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
/// inherit key-by-key). The CALLER decides the file exists, from the no-follow
/// view it already holds (`has_node_file`), so this never re-`stat`s the path
/// (a follow-symlink check here would see a file the staging copy and the hash
/// walk both drop). A file that is not a JSON object, or that carries an
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
            error: e.to_string(),
        })?;

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
}

#[cfg(test)]
mod package_tests {
    use super::*;
    #[test]
    fn human_specs_loaded() {
        let cat = FsCatalog::discover(&stdlib_root()).unwrap();
        let q = cat.entry("HumanQuery").expect("HumanQuery missing");
        let t = cat.entry("HumanTrigger").expect("HumanTrigger missing");
        assert!(!q.metadata.form_field_specs.is_empty(), "HumanQuery specs empty");
        assert!(!t.metadata.form_field_specs.is_empty(), "HumanTrigger specs empty");
        assert_eq!(q.package_key, t.package_key, "both nodes should share package_key");
        let pkg = cat.package_of("HumanQuery").expect("pkg_of missing");
        assert_eq!(pkg.name, "human");
        assert_eq!(pkg.node_types.len(), 2);
        assert_eq!(pkg.shared_rs.len(), 1, "should have form_helpers.rs");
    }

    /// WhatsApp triad: bridge is the infra (requires_infra + locally
    /// built image), receive is a trigger (no infra), send is a
    /// normal Fire-phase node (no infra). All three must load from
    /// the catalog so the package compiles into a project binary.
    #[test]
    fn whatsapp_triad_loaded() {
        let cat = FsCatalog::discover(&stdlib_root()).unwrap();
        let bridge = cat.entry("WhatsAppBridge").expect("WhatsAppBridge missing");
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

        let recv = cat.entry("WhatsAppReceive").expect("WhatsAppReceive missing");
        assert!(!recv.metadata.requires_infra, "receive must NOT require infra");
        assert!(recv.metadata.features.is_trigger, "receive is a trigger");

        let send = cat.entry("WhatsAppSend").expect("WhatsAppSend missing");
        assert!(!send.metadata.requires_infra, "send must NOT require infra");
        assert!(!send.metadata.features.is_trigger, "send is a normal node");

        // The three must live under the same package so the codegen
        // bundles them together. (`package_of` returns the package
        // descriptor for any node_type in it.)
        let pkg = cat.package_of("WhatsAppBridge").expect("bridge package");
        assert_eq!(
            pkg.name, "whatsapp",
            "WhatsApp triad must share a package",
        );
        assert!(
            pkg.node_types.iter().any(|t| t == "WhatsAppBridge")
                && pkg.node_types.iter().any(|t| t == "WhatsAppReceive")
                && pkg.node_types.iter().any(|t| t == "WhatsAppSend"),
            "package must contain all three node types, got {:?}",
            pkg.node_types,
        );
    }
}
