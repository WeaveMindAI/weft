//! Filesystem-backed node catalog.
//!
//! The catalog is every node a project holds, walked from two roots:
//! `nodes/` (the standard library, cloned in at `weft new`, and
//! anything shared) and `src/`, where a node can sit beside the code
//! that uses it. Both walk into one namespace, and they are the single
//! source of truth for every node, so build / parse / run never reach
//! outside the project. A unit found while walking a root is one of two
//! shapes:
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
//! Units can sit at any depth under a root: directly under it or ten
//! levels deep. The walk recurses until it hits a unit, then stops
//! descending. A unit never nests inside another unit. Two units
//! declaring the same `node_type` is an ambiguous collision and fails
//! loudly (no shadowing: there is only one source).
//!
//! The compiler uses this crate to look up node metadata without
//! compiling node Rust code. The emitted project binary compiles node
//! code directly via `#[path]` includes driven by codegen; it does NOT
//! use this crate at runtime.
//!
//! Every byte the catalog reads comes through a [`SourceTree`]: the disk
//! ([`DiskTree`], what `FsCatalog::discover` walks), or a snapshot a
//! caller took into memory once, so a command compiles from one fixed
//! view of the project however its files change on disk meanwhile.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use weft_core::is_rust_identifier;
use weft_core::access::spec::{Acquisition, AppRegistration};
use weft_core::node::{MetadataCatalog, NodeMetadata};

mod spelling;

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
/// [`SourceTree::entry_kind`] (`node_tree_entry_kind` on disk), so a
/// project's `nodes/base_catalog` can be a symlink into a weft
/// checkout's `catalog/` (e.g. this repo's own `examples/`); the
/// recursive walks refuse symlink cycles through
/// `guard_cycle` (`guard_node_tree_cycle` on disk), a descent-chain
/// check, and fail loudly.
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
        || weft_core::project::coverage::is_python_cache(name)
}


/// True if an entry of this name is left out of what a node PACKAGE is
/// built from: everything [`is_node_tree_excluded`] leaves out, plus a
/// `.weft` file. A `.weft` file in a package is a program a project
/// may `@include` (`__catalog/<package>/x.weft`), read by the compiler
/// into the program's definition and never by cargo, so editing one
/// must not recompile the package, rebuild a worker image or rerun the
/// package's node tests. It stays part of the node tree everywhere
/// else: seeded into a project, published in a version, listed by
/// `include_files`. The staging copy and the package hash both ask
/// this, so they agree on a package's bytes.
pub fn is_left_out_of_package_build(name: &str) -> bool {
    is_node_tree_excluded(name) || name.ends_with(".weft")
}

/// The module a node's code compiles into inside its package's crate:
/// the node type in snake_case (`RunCommand` is `run_command`). Rules:
/// lowercase the first char; insert `_` before every later uppercase.
pub fn node_module_name(node_type: &str) -> String {
    let mut out = String::with_capacity(node_type.len() + 4);
    for (i, ch) in node_type.chars().enumerate() {
        if ch.is_ascii_uppercase() {
            if i > 0 {
                out.push('_');
            }
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push(ch);
        }
    }
    out
}

/// The first thing in a package that cannot be a module of its crate,
/// as the file to blame and why: a member or a shared file whose module
/// name Rust refuses, two members whose types make one module, or a
/// shared file named like a member's module (`computer.rs` beside the
/// node `Computer`).
fn module_problem(shared_rs: &[PathBuf], members: &[CatalogEntry]) -> Option<(PathBuf, String)> {
    let mut taken: BTreeMap<String, String> = BTreeMap::new();
    for member in members {
        let metadata = member.source_dir.join("metadata.json");
        let module = node_module_name(&member.node_type);
        if !weft_core::is_rust_module_name(&module) {
            return Some((
                metadata,
                format!(
                    "the node {} would compile into module `{module}`, which Rust cannot take as a module name (a keyword, `_`, or not [A-Za-z_][A-Za-z0-9_]*); rename the type",
                    member.node_type
                ),
            ));
        }
        if let Some(other) = taken.insert(module.clone(), member.node_type.clone()) {
            return Some((
                metadata,
                format!("the nodes {other} and {} both compile into module `{module}`; rename one", member.node_type),
            ));
        }
    }
    shared_rs.iter().find_map(|path| {
        let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or_default();
        if !weft_core::is_rust_module_name(stem) {
            return Some((
                path.clone(),
                format!(
                    "this shared file would become module `{stem}`, which Rust cannot take as a module name (a keyword, `_`, or not [A-Za-z_][A-Za-z0-9_]*); rename the file"
                ),
            ));
        }
        let member = taken.get(stem)?;
        Some((
            path.clone(),
            format!("this shared file would become module `{stem}`, which is also the module of the node {member}; rename the file"),
        ))
    })
}

/// One `.weft` file under a node root, for the listing of what a
/// program can `@include` from there.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncludeFile {
    /// Its path under the root, `/`-separated (`computer/agent.weft`).
    pub path: String,
    /// The text of its first line when that line is a `#` comment,
    /// trimmed; `None` when it is not one, or says nothing.
    pub description: Option<String>,
}

/// Every `.weft` file under `root` on disk. See [`include_files_in`].
pub fn include_files(root: &Path) -> Result<Vec<IncludeFile>, CatalogError> {
    include_files_in(&DiskTree, root)
}

/// Every `.weft` file under `root` in `tree`, by its path under it,
/// sorted, each with the description its first line gives. The walk is
/// the node tree's (`read_node_dir`: symlinks followed with a loop
/// guard, excluded names skipped), so a file in a folder the catalog
/// never reads is never listed. A missing `root` holds none.
pub fn include_files_in(tree: &dyn SourceTree, root: &Path) -> Result<Vec<IncludeFile>, CatalogError> {
    let mut out = Vec::new();
    if present(tree, root)? == Some(NodeTreeEntryKind::Dir) {
        collect_include_files(tree, root, root, &mut Vec::new(), &mut out)?;
    }
    out.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(out)
}

fn collect_include_files(
    tree: &dyn SourceTree,
    root: &Path,
    dir: &Path,
    chain: &mut Vec<PathBuf>,
    out: &mut Vec<IncludeFile>,
) -> Result<(), CatalogError> {
    let canon = guard_cycle(tree, dir, chain).map_err(|error| CatalogError::Io { path: dir.to_path_buf(), error })?;
    chain.push(canon);
    for entry in read_node_dir(tree, dir)? {
        match entry {
            NodeDirEntry::Dir(path) => collect_include_files(tree, root, &path, chain, out)?,
            NodeDirEntry::File(path) if path.extension().is_some_and(|e| e == "weft") => {
                let first = first_line(tree, &path).map_err(|error| CatalogError::Io { path: path.clone(), error })?;
                let relative = path.strip_prefix(root).expect("the walk stays under its root");
                out.push(IncludeFile {
                    path: relative.to_string_lossy().replace('\\', "/"),
                    description: first_line_comment(&first),
                });
            }
            NodeDirEntry::File(_) => {}
        }
    }
    chain.pop();
    Ok(())
}

/// The first line of the file at `path`, decoded leniently: a file that
/// is not UTF-8 is no program either, but its line still lists it.
fn first_line(tree: &dyn SourceTree, path: &Path) -> std::io::Result<String> {
    let bytes = tree.read(path)?;
    let line = bytes.split_inclusive(|b| *b == b'\n').next().unwrap_or_default();
    Ok(String::from_utf8_lossy(line).into_owned())
}

/// The text of a first line when that line is a `#` comment.
fn first_line_comment(source: &str) -> Option<String> {
    let comment = source.lines().next()?.trim_start().strip_prefix('#')?.trim();
    (!comment.is_empty()).then(|| comment.to_string())
}

/// What one node-tree entry is, symlinks resolved: a symlinked
/// directory walks as a directory, a symlinked file reads as a file.
/// The shared traversal mechanic of every node-tree walk (discovery,
/// the pack into the source map, the staging copy, the source-hash
/// walk), so they agree on exactly which bytes constitute a node. A
/// broken symlink is a loud error (its target is part of the node's
/// source and it is missing).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum NodeTreeEntryKind {
    Dir,
    File,
}

/// What `path` is on disk. See [`SourceTree::entry_kind`].
pub fn node_tree_entry_kind(path: &Path) -> std::io::Result<NodeTreeEntryKind> {
    DiskTree.entry_kind(path)
}

/// Where the bytes of a project's files come from: its node trees for
/// discovery and every later read of a catalog (`deps`,
/// `package_declares_tests`), and its program for the compiler
/// (`weft_compiler::file_reader::TreeFileReader`). One walk serves the
/// disk ([`DiskTree`]) and a snapshot a command took into memory once at
/// its start, which files edited on disk mid-command then never reach.
/// Paths are absolute, the ones the walk builds by joining child names
/// onto its roots.
///
/// Each question is answered the way the disk answers it, symlinks
/// followed: the build stages and hashes node code from a disk (the
/// install's copy of the version, which is the snapshot the command
/// published), and it must see the tree discovery saw.
pub trait SourceTree: Send + Sync {
    /// The paths of `dir`'s immediate children, in no particular order
    /// (the walk sorts). An error is about `dir` itself.
    fn read_dir(&self, dir: &Path) -> std::io::Result<Vec<PathBuf>>;

    /// What `path` is, symlinks followed. A path that is not there is a
    /// `NotFound` error, which is also how a caller asks whether something
    /// exists (`present`); a symlink whose target is not there is another
    /// error, never `NotFound`. Asked per child after `read_dir`, so a child
    /// the walk skips by name (`is_node_tree_excluded`) is never asked.
    fn entry_kind(&self, path: &Path) -> std::io::Result<NodeTreeEntryKind>;

    /// The bytes of the file at `path`.
    fn read(&self, path: &Path) -> std::io::Result<Vec<u8>>;

    /// The bytes at `path` as text. A file that is not UTF-8 fails with
    /// the `InvalidData` error `std::fs::read_to_string` gives, so a
    /// broken `metadata.json` reads the same from every backing.
    fn read_to_string(&self, path: &Path) -> std::io::Result<String> {
        String::from_utf8(self.read(path)?)
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "stream did not contain valid UTF-8"))
    }

    /// The one identity of the file or directory at `path`, however it
    /// was reached: two paths to one directory (through symlinks) give
    /// the same answer, and a path that is not there is an error. The
    /// walk dedupes on it and refuses a symlink cycle by it
    /// (`guard_cycle`); the compiler names an included file by it.
    fn canonicalize(&self, path: &Path) -> std::io::Result<PathBuf>;
}

/// What `path` is in `tree`, `None` when nothing is there: the one way a
/// catalog asks whether an optional file exists. Any other failure (a
/// dangling link, a permission refused) is an error naming the path,
/// never read as the file being absent.
fn present(tree: &dyn SourceTree, path: &Path) -> Result<Option<NodeTreeEntryKind>, CatalogError> {
    match tree.entry_kind(path) {
        Ok(kind) => Ok(Some(kind)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(CatalogError::Io { path: path.to_path_buf(), error }),
    }
}

/// The node tree as it is on disk right now.
#[derive(Debug, Clone, Copy, Default)]
pub struct DiskTree;

impl SourceTree for DiskTree {
    fn read_dir(&self, dir: &Path) -> std::io::Result<Vec<PathBuf>> {
        fs::read_dir(dir)?.map(|child| child.map(|c| c.path())).collect()
    }

    fn entry_kind(&self, path: &Path) -> std::io::Result<NodeTreeEntryKind> {
        // `fs::metadata` follows symlinks. A link to nothing is told apart
        // from nothing at all: the tree names bytes that do not exist, and
        // nobody may read it as an optional file being absent (`present`).
        match fs::metadata(path) {
            Ok(meta) => Ok(if meta.is_dir() { NodeTreeEntryKind::Dir } else { NodeTreeEntryKind::File }),
            Err(e)
                if e.kind() == std::io::ErrorKind::NotFound
                    && fs::symlink_metadata(path).is_ok_and(|link| link.file_type().is_symlink()) =>
            {
                Err(std::io::Error::other(format!("{} is a symlink to nothing", path.display())))
            }
            Err(e) => Err(e),
        }
    }

    fn read(&self, path: &Path) -> std::io::Result<Vec<u8>> {
        fs::read(path)
    }

    fn read_to_string(&self, path: &Path) -> std::io::Result<String> {
        fs::read_to_string(path)
    }

    fn canonicalize(&self, path: &Path) -> std::io::Result<PathBuf> {
        fs::canonicalize(path)
    }
}

/// [`guard_cycle`] on disk, for the walks outside this crate (the build's
/// staging copy, the source hash) that read the real tree.
pub fn guard_node_tree_cycle(dir: &Path, chain: &[PathBuf]) -> std::io::Result<PathBuf> {
    guard_cycle(&DiskTree, dir, chain)
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
fn guard_cycle(tree: &dyn SourceTree, dir: &Path, chain: &[PathBuf]) -> std::io::Result<PathBuf> {
    let canon = tree.canonicalize(dir)?;
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

/// One line of [`FsCatalog::warnings`]. Never a failure on its own.
/// Serialized as `{kind, message}` in `weft validate` and `weft
/// describe-nodes` JSON, so a reader picks a kind without parsing the line.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct CatalogWarning {
    pub kind: WarningKind,
    /// The line as a person reads it.
    pub message: String,
}

/// What a [`CatalogWarning`] is about.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WarningKind {
    /// A node or package left out of the catalog: one that failed to
    /// load, a node not ready yet, a package with no node in it yet.
    LeftOut,
    /// A name a node spells off the weft way (see `spelling`). The node
    /// loads as usual; the line names the node, its file, and the
    /// spelling that fits.
    Spelling,
}

impl std::fmt::Display for CatalogWarning {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

pub struct FsCatalog {
    entries: HashMap<String, CatalogEntry>,
    /// All discovered packages, keyed by package root. Each
    /// `CatalogEntry` has a `package_key` pointing back in here.
    packages: HashMap<PathBuf, Package>,
    /// Every node or package that failed to load, with its error.
    problems: Vec<CatalogProblem>,
    /// One line per thing left out (each problem, each node not ready
    /// yet, see `pending`, each package with no node in it yet), and one
    /// per name a node spells off the weft way, which leaves nothing out.
    warnings: Vec<CatalogWarning>,
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
    /// The tree this catalog was discovered from. Its later reads
    /// (`deps`, `package_declares_tests`) go through it too, so they see
    /// the bytes discovery saw, a snapshot's included.
    tree: std::sync::Arc<dyn SourceTree>,
}

impl std::fmt::Debug for FsCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FsCatalog")
            .field("entries", &self.entries)
            .field("packages", &self.packages)
            .field("problems", &self.problems)
            .field("warnings", &self.warnings)
            .field("pending", &self.pending)
            .field("type_registry", &self.type_registry)
            .finish_non_exhaustive()
    }
}

impl FsCatalog {
    /// Walk one node tree on disk. See `discover_roots_in`.
    pub fn discover(root: &Path) -> Result<Self, CatalogError> {
        Self::discover_roots(&[root])
    }

    /// Walk several node trees on disk. See `discover_roots_in`.
    pub fn discover_roots(roots: &[&Path]) -> Result<Self, CatalogError> {
        Self::discover_roots_in(std::sync::Arc::new(DiskTree), roots)
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
    ///
    /// Every read goes through `tree`, which the catalog keeps for its
    /// later reads; `roots` are absolute paths in it.
    pub fn discover_roots_in(tree: std::sync::Arc<dyn SourceTree>, roots: &[&Path]) -> Result<Self, CatalogError> {
        let mut cat = Self::empty();
        cat.tree = tree.clone();
        let tree = &*tree;
        let mut present_roots = Vec::new();
        for root in roots.iter().copied() {
            if present(tree, root)?.is_some() {
                tree.read_dir(root).map_err(|error| CatalogError::Io { path: root.to_path_buf(), error })?;
                present_roots.push(root);
            }
        }
        let roots = present_roots;
        if roots.is_empty() {
            return Ok(cat);
        }
        // Type declarations first: port type strings in any
        // metadata.json may use the declared names, so the registry
        // must exist before a single NodeMetadata is deserialized.
        let mut ctx = DiscoverCtx {
            tree,
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
            tree: std::sync::Arc::new(DiskTree),
        }
    }

    /// One line per thing left out of the catalog (every problem, every
    /// node not ready yet, every package with no node in it yet), and one
    /// per name a node spells off the weft way (a port in snake_case, a
    /// node type not in PascalCase), whose node still loads. `kind` tells
    /// the two apart.
    pub fn warnings(&self) -> &[CatalogWarning] {
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
    /// already walked the node roots and grouped every node under its
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
    /// A `tests.rs` that cannot be looked at (a dangling link) is an
    /// error naming it, never a package with no tests.
    pub fn package_declares_tests(&self, package: &Package) -> Result<bool, CatalogError> {
        for entry in package.node_types.iter().filter_map(|nt| self.entry(nt)) {
            let tests = entry.source_dir.join("tests.rs");
            if present(self.tree.as_ref(), &tests)? == Some(NodeTreeEntryKind::File) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Read the node's optional `deps.toml`. Returns `None` if the
    /// node has no `deps.toml` (many nodes have zero extra deps).
    pub fn deps(&self, node_type: &str) -> Result<Option<NodeDeps>, CatalogError> {
        let Some(entry) = self.entries.get(node_type) else {
            return Ok(None);
        };
        let deps_path = entry.source_dir.join("deps.toml");
        if present(self.tree.as_ref(), &deps_path)?.is_none() {
            return Ok(None);
        }
        let raw = self.tree.read_to_string(&deps_path).map_err(|e| CatalogError::Io {
            path: deps_path.clone(),
            error: e,
        })?;
        let parsed: NodeDeps = toml::from_str(&raw).map_err(|e| CatalogError::Parse {
            error: stale_copy_hint(&deps_path, e.to_string()),
            path: deps_path.clone(),
        })?;
        if let Some(key) = parsed.build.env.keys().find(|key| WEFT_OWNED_BUILD_ENV.contains(&key.as_str())) {
            return Err(CatalogError::Parse {
                error: format!(
                    "[build.env] sets {key}, which weft sets for every worker build ({}, \
                     `weft_core::builds::WORKER_RUSTFLAGS`) and would be replaced by it; a flag your package's own \
                     code needs goes in its `build.rs` (`cargo:rustc-cfg=...`, `cargo:rustc-link-arg=...`)",
                    weft_core::builds::WORKER_RUSTFLAGS
                ),
                path: deps_path,
            });
        }
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
/// - `[dependencies]`: cargo deps (keys are crate names, values are
///   whatever cargo accepts).
/// - `[build-dependencies]`: cargo deps of the package's own `build.rs`.
/// - `[[needs]]`: what the node needs from the machine its worker runs
///   on (`weft_core::needs::Need`): checked on a machine before a
///   program that uses the node starts, installed into an image.
/// - `[build.env]`: variables the package's compile needs.
///
/// Any other key is refused, naming it.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeDeps {
    #[serde(default)]
    pub dependencies: toml::Table,
    /// Cargo `[build-dependencies]` for the package's own `build.rs`
    /// (a `build.rs` at the package root ships as the emitted package
    /// crate's build script).
    #[serde(default, rename = "build-dependencies")]
    pub build_dependencies: toml::Table,
    #[serde(default)]
    pub needs: Vec<weft_core::needs::Need>,
    #[serde(default)]
    pub build: BuildEnv,
}

/// Build-environment variables a node needs during `cargo build`.
/// Merged (union) across every referenced node and emitted as
/// `ENV` lines in the builder stage of the Dockerfile, or as the `[env]`
/// of a host build's cargo config.
///
/// Keep narrow and declarative. General-purpose build logic
/// belongs in the node's own `build.rs`, not here.
///
/// Values support one substitution: `{{catalog_path}}` expands
/// to the node's directory inside the builder container, where the
/// project's node trees are staged. Example, a node shipping a config:
///
/// ```toml
/// [build.env]
/// FOO_CONFIG = "{{catalog_path}}/foo-config.txt"
/// ```
///
/// resolves to `/weft/project-nodes/nodes/base_catalog/basic/exec_python/foo-config.txt`
/// if the node lives at `nodes/base_catalog/basic/exec_python/` (the
/// mount holds every node at its project-relative path, so one beside
/// the code lands under `/weft/project-nodes/src/...`). A host build
/// expands it to the node's folder in its package crate's copy of the
/// package.
/// The build variables weft sets itself for every worker build: a node
/// declaring one in `[build.env]` is refused, since one setting would
/// replace the other.
const WEFT_OWNED_BUILD_ENV: &[&str] = &["RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS"];

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BuildEnv {
    #[serde(default)]
    pub env: std::collections::BTreeMap<String, String>,
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
///    setup.sh, when it installs a prebuilt binary, records where the
///    repo actually lives, one line in a file, so a later start from a
///    project folder finds the same manifests and shared-credentials
///    file. A binary built from source never needs it: (3) answers.
///    SYNC: repo-root file <-> setup.sh (prebuilt CLI install: repo-root write)
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
        if weft_core::checkout::is_weft_checkout(root) {
            return Ok(root.clone());
        }
        return Err(format!(
            "WEFT_REPO_ROOT is set to '{}', which is not a weft checkout \
             (no crates/weft-catalog + catalog dirs beside the workspace \
             Cargo.toml/Cargo.lock); fix or unset it",
            root.display()
        ));
    }
    if let Some(root) = sources.cwd.as_deref().and_then(weft_core::checkout::enclosing) {
        return Ok(root);
    }
    if let Some(built_from) = &sources.built_from {
        if weft_core::checkout::is_weft_checkout(built_from) {
            return Ok(built_from.clone());
        }
    }
    match &sources.recorded {
        RecordedInstallRoot::Recorded { file, root } => {
            if weft_core::checkout::is_weft_checkout(root) {
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
    /// The apps a package root's defaults declare under `accessApps`,
    /// checked once against the root's file (`settle`). Empty for a bare
    /// node and for a root that declares none.
    root_apps: BTreeMap<String, AppRegistration>,
    /// The members whose `accessApps` is the root's (their own file sets
    /// none): a refused root app leaves exactly these out.
    inheriting_apps: BTreeSet<String>,
}

/// Discovery state threaded through the traversal: the tree it reads,
/// the catalog being built, the units loaded so far, and the files the
/// type pass already found broken.
struct DiscoverCtx<'a> {
    tree: &'a dyn SourceTree,
    cat: &'a mut FsCatalog,
    units: Vec<Unit>,
    /// `metadata.json` files whose `types` declarations could not be
    /// taken into the registry, with why. Loading such a file fails with
    /// that reason, so the error lands on the node or package that
    /// declared the type, never on the whole catalog.
    broken_files: HashMap<PathBuf, String>,
    /// Canonical dirs of the CURRENT descent chain; re-entering one is
    /// a symlink cycle and fails loudly (see `guard_cycle`).
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
        self.warn(WarningKind::LeftOut, pending_reason(&node_type, &dir));
        self.cat.pending.insert(node_type, dir);
    }

    /// A node or package that failed to load: left out, with its error,
    /// under the types it claims.
    fn record(&mut self, node_types: Vec<String>, error: CatalogError) {
        self.warn(WarningKind::LeftOut, error.to_string());
        self.cat.problems.push(CatalogProblem { node_types, error });
    }

    fn warn(&mut self, kind: WarningKind, message: String) {
        self.cat.warnings.push(CatalogWarning { kind, message });
    }

    /// The names a node or a package's defaults spell off the weft way,
    /// one line each, naming `subject` and the `metadata.json` they are in.
    fn warn_spelling(&mut self, subject: &str, file: &Path, messages: Vec<String>) {
        for message in messages {
            self.warn(WarningKind::Spelling, format!("{subject} ({}): {message}", file.display()));
        }
    }

    /// What loading a node folder found, its spelling lines included:
    /// the entry, and whether its `accessApps` is its package root's.
    fn take_loaded(&mut self, loaded: Loaded, dir: &Path) -> Option<(CatalogEntry, bool)> {
        let file = dir.join("metadata.json");
        match loaded {
            Loaded::Ready { entry, misspelled, inherits_apps } => {
                self.warn_spelling(&entry.node_type, &file, misspelled);
                Some((*entry, inherits_apps))
            }
            Loaded::Pending { node_type, misspelled } => {
                self.warn_spelling(&node_type, &file, misspelled);
                self.leave_pending(node_type, dir.to_path_buf());
                None
            }
        }
    }

    /// Register every loaded unit, after leaving out each one that
    /// shares a claim with another. Three names must each lead to one
    /// place: a package name (the test-crate emit picks its package by
    /// name), a node type, and a service (the store that keeps
    /// connections and the compiler resolving what a node publishes
    /// both find a service by name alone). Every claimant of a shared
    /// name is left out, told about all the others, because the walk's
    /// order is no reason for one folder to win.
    ///
    /// Every removal also notes, against the service the removed node
    /// declared, why it went (`Withdrawn`): an app naming a service whose
    /// declarers were all left out is then refused with that cause, never
    /// with "no node declares it".
    fn settle(mut self) {
        let mut units = std::mem::take(&mut self.units);
        let mut withdrawn = Withdrawn::new();

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
        let type_clashes: BTreeMap<String, CatalogError> = by_type
            .into_iter()
            .filter(|(_, dirs)| dirs.len() > 1)
            .map(|(node_type, dirs)| (node_type.clone(), CatalogError::Collision { node_type, dirs }))
            .collect();
        for entry in clashing.iter().chain(units_ok.iter()).flat_map(|u| &u.entries) {
            if let Some(error) = type_clashes.get(&entry.node_type) {
                withdraw(&mut withdrawn, entry, error);
            }
        }
        for unit in clashing {
            let node_types = unit.entries.iter().map(|e| e.node_type.clone()).collect();
            let roots = by_name[&unit.name].clone();
            let error = CatalogError::PackageNameCollision { name: unit.name, roots };
            for entry in &unit.entries {
                withdraw(&mut withdrawn, entry, &error);
            }
            self.record(node_types, error);
        }
        for unit in &mut units_ok {
            unit.entries.retain(|e| !type_clashes.contains_key(&e.node_type));
        }
        for (node_type, error) in type_clashes {
            self.record(vec![node_type], error);
        }

        // A package compiles as one crate whose root holds a module per
        // shared file and per member, so one that cannot be such a module
        // (a name Rust refuses, two of one name) cannot build. Refused
        // here, for every program, rather than by rustc in the build of
        // whichever program happens to use the package. Its entries are
        // withdrawn after every collision was judged with them, and its
        // name stays claimed, the way a package whose members all broke
        // keeps its name.
        for unit in &mut units_ok {
            if let Some((path, error)) = module_problem(&unit.shared_rs, &unit.entries) {
                let error = CatalogError::Parse { path, error };
                for entry in &unit.entries {
                    withdraw(&mut withdrawn, entry, &error);
                }
                let node_types = unit.entries.drain(..).map(|e| e.node_type).collect();
                self.record(node_types, error);
            }
        }

        // Services, over what is left.
        let mut by_service: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for entry in units_ok.iter().flat_map(|u| &u.entries) {
            if let Some(service) = &entry.metadata.service {
                by_service.entry(service.service.clone()).or_default().push(entry.node_type.clone());
            }
        }
        let service_clashes: BTreeMap<String, (Vec<String>, CatalogError)> = by_service
            .into_iter()
            .filter(|(_, node_types)| node_types.len() > 1)
            .map(|(service, node_types)| {
                let error = CatalogError::ServiceCollision { service: service.clone(), node_types: node_types.clone() };
                (service, (node_types, error))
            })
            .collect();
        let clashing_service = |e: &CatalogEntry| {
            e.metadata.service.as_ref().and_then(|s| service_clashes.get(&s.service)).map(|(_, error)| error)
        };
        for entry in units_ok.iter().flat_map(|u| &u.entries) {
            if let Some(error) = clashing_service(entry) {
                withdraw(&mut withdrawn, entry, error);
            }
        }
        for unit in &mut units_ok {
            unit.entries.retain(|e| clashing_service(e).is_none());
        }
        for (node_types, error) in service_clashes.into_values() {
            self.record(node_types, error);
        }

        // An app declared under `accessApps` is checked now that every
        // service is known (a project's catalog holds the standard nodes, so
        // every service weft ships is): it names a service this catalog
        // declares, one whose connect takes an app, and it is held to every
        // rule for a shipped app (`AppRegistration::check_shipped`). A root's
        // app is checked once, against the root's file, and a refusal leaves
        // out the members that inherit it; a member's own app, against the
        // member's file. A refusal can take out the node declaring a service
        // another app names, so the check repeats over what is left until a
        // round refuses nothing.
        loop {
            let services: BTreeMap<&str, &weft_core::access::spec::AccessSpec> = units_ok
                .iter()
                .flat_map(|u| &u.entries)
                .filter_map(|e| e.metadata.service.as_ref())
                .map(|spec| (spec.service.as_str(), spec))
                .collect();
            // (unit index, whether it is the root's app, the types left out, the file, why)
            let mut refused: Vec<(usize, bool, Vec<String>, PathBuf, String)> = Vec::new();
            for (index, unit) in units_ok.iter().enumerate() {
                if let Some(error) = refuse_apps(&services, &withdrawn, &unit.root_apps) {
                    let inheriting = unit.entries.iter().filter(|e| unit.inheriting_apps.contains(&e.node_type));
                    let node_types = inheriting.map(|e| e.node_type.clone()).collect();
                    refused.push((index, true, node_types, unit.root.join("metadata.json"), error));
                }
                for entry in unit.entries.iter().filter(|e| !unit.inheriting_apps.contains(&e.node_type)) {
                    if let Some(error) = refuse_apps(&services, &withdrawn, &entry.metadata.access_apps) {
                        let file = entry.source_dir.join("metadata.json");
                        refused.push((index, false, vec![entry.node_type.clone()], file, error));
                    }
                }
            }
            if refused.is_empty() {
                break;
            }
            for (index, is_root, node_types, path, error) in refused {
                let unit = &mut units_ok[index];
                if is_root {
                    // Reported once: a root with no member left to inherit
                    // its app is not checked again.
                    unit.root_apps.clear();
                }
                let error = CatalogError::Parse { path, error };
                for entry in unit.entries.iter().filter(|e| node_types.contains(&e.node_type)) {
                    withdraw(&mut withdrawn, entry, &error);
                }
                unit.entries.retain(|e| !node_types.contains(&e.node_type));
                self.record(node_types, error);
            }
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

/// Each service a node declared before `settle` left that node out,
/// with every reason one of its declarers went.
type Withdrawn = BTreeMap<String, BTreeSet<String>>;

/// Notes `cause` against the service `entry` declares, if it declares one.
fn withdraw(withdrawn: &mut Withdrawn, entry: &CatalogEntry, cause: &CatalogError) {
    if let Some(spec) = &entry.metadata.service {
        withdrawn.entry(spec.service.clone()).or_default().insert(cause.to_string());
    }
}

/// The first app of `apps` (an `accessApps` map) a catalog declaring
/// `services` refuses, as one line naming the app: a service whose every
/// declarer was left out (naming why, from `withdrawn`), a service it does
/// not declare, a service whose connect takes no app, or an app that
/// breaks a rule of [`AppRegistration::check_shipped`].
fn refuse_apps(
    services: &BTreeMap<&str, &weft_core::access::spec::AccessSpec>,
    withdrawn: &Withdrawn,
    apps: &BTreeMap<String, AppRegistration>,
) -> Option<String> {
    apps.iter().find_map(|(service, app)| {
        let refusal = match services.get(service.as_str()).map(|spec| &spec.acquisition) {
            None => match withdrawn.get(service) {
                Some(causes) => Err(format!(
                    "every node declaring the '{service}' service was left out, so this app has no \
                     service to belong to: {}",
                    causes.iter().map(String::as_str).collect::<Vec<_>>().join("; and ")
                )),
                None => Err(format!(
                    "no node in this project declares a service named '{service}' (it declares: {}), so \
                     this app would never be used; name one of those, or remove the app",
                    if services.is_empty() { "none".to_string() } else { services.keys().copied().collect::<Vec<_>>().join(", ") }
                )),
            },
            Some(Acquisition::OAuth2 { registration_fields, .. }) => app.check_shipped(registration_fields),
            Some(_) => Err(format!(
                "the '{service}' service connects without an app (only a service that signs in through \
                 OAuth takes one), so this app would never be used; remove it"
            )),
        };
        refusal.err().map(|error| format!("accessApps.{service}: {error}"))
    })
}

/// The node type a folder's `metadata.json` claims, read as plainly as
/// possible: used only to name a folder that failed to load, so a
/// program naming that type is told why.
fn declared_type(tree: &dyn SourceTree, node_dir: &Path) -> Option<String> {
    let raw = tree.read_to_string(&node_dir.join("metadata.json")).ok()?;
    let value: serde_json::Value = serde_json::from_str(&raw).ok()?;
    value.get("type")?.as_str().map(str::to_string)
}

/// The node types claimed by a package's member folders (every
/// immediate subdir with a `metadata.json`), for naming a package that
/// failed to load as a whole.
fn declared_member_types(tree: &dyn SourceTree, entries: &[NodeDirEntry]) -> Vec<String> {
    entries
        .iter()
        .filter_map(|e| match e {
            NodeDirEntry::Dir(path) => declared_type(tree, path),
            NodeDirEntry::File(_) => None,
        })
        .collect()
}

/// Read a `metadata.json`, failing with the type pass's reason when that
/// pass already found the file's `types` declarations unusable.
fn read_metadata_file(
    tree: &dyn SourceTree,
    path: &Path,
    broken_files: &HashMap<PathBuf, String>,
) -> Result<String, CatalogError> {
    if let Some(error) = broken_files.get(path) {
        return Err(CatalogError::Parse { path: path.to_path_buf(), error: error.clone() });
    }
    tree.read_to_string(path).map_err(|error| CatalogError::Io { path: path.to_path_buf(), error })
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
        harvest_type_declarations(ctx.tree, root, &mut declarations, &mut ctx.broken_files);
    }
    // Lexical order by origin path: a directory listing's order is
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
    // is left out too and the round runs again without it. Each try
    // builds from the accepted files' declarations plus the new file's,
    // never on top of a registry already built, so a clash names the
    // file that declared the name first as well as the one restating it.
    let mut by_origin: BTreeMap<String, Vec<(String, String, String)>> = BTreeMap::new();
    for decl in declarations {
        by_origin.entry(decl.2.clone()).or_default().push(decl);
    }
    let mut excluded: BTreeMap<String, String> = BTreeMap::new();
    loop {
        let mut registry = TypeRegistry::builtin();
        let mut taken: Vec<(String, String, String)> = Vec::new();
        let mut accepted: Vec<&String> = Vec::new();
        let mut waiting: Vec<&String> = by_origin.keys().filter(|o| !excluded.contains_key(*o)).collect();
        let mut last_error: BTreeMap<&String, String> = BTreeMap::new();
        loop {
            let before = waiting.len();
            let mut still: Vec<&String> = Vec::new();
            for origin in waiting {
                let mut trial = taken.clone();
                trial.extend(by_origin[origin].iter().cloned());
                match TypeRegistry::builtin().extended(&trial, Redeclaration::AbsorbIdentical) {
                    Ok(next) => {
                        registry = next;
                        taken = trial;
                        accepted.push(origin);
                    }
                    Err(error) => {
                        last_error.insert(origin, error);
                        still.push(origin);
                    }
                }
            }
            waiting = still;
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
    tree: &dyn SourceTree,
    dir: &Path,
    out: &mut Vec<(String, String, String)>,
    broken_files: &mut HashMap<PathBuf, String>,
) {
    let mut chain = Vec::new();
    let mut done = std::collections::HashSet::new();
    harvest_type_declarations_inner(tree, dir, out, broken_files, &mut chain, &mut done)
}

fn harvest_type_declarations_inner(
    tree: &dyn SourceTree,
    dir: &Path,
    out: &mut Vec<(String, String, String)>,
    broken_files: &mut HashMap<PathBuf, String>,
    chain: &mut Vec<PathBuf>,
    done: &mut std::collections::HashSet<PathBuf>,
) {
    let Ok(canon) = guard_cycle(tree, dir, chain) else { return };
    // Same dedupe as discovery: a folder reachable by two symlink paths
    // yields its declarations once.
    if !done.insert(canon.clone()) {
        return;
    }
    let Ok(entries) = read_node_dir(tree, dir) else { return };
    chain.push(canon);
    for entry in entries {
        match entry {
            NodeDirEntry::Dir(path) => harvest_type_declarations_inner(tree, &path, out, broken_files, chain, done),
            NodeDirEntry::File(path) => {
                if path.file_name().and_then(|n| n.to_str()) != Some("metadata.json") {
                    continue;
                }
                // Raw read: the typed NodeMetadata parse needs the
                // registry we are building. An unreadable or malformed
                // file is the main pass's error to report (it reads
                // every metadata.json it loads, package roots included).
                let Ok(raw) = tree.read_to_string(&path) else { continue };
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

/// A classified, kept child of a node-tree directory. `read_node_dir`
/// already left out the `NODE_TREE_EXCLUDE` names and followed symlinks,
/// so every entry here is a dir or file the node-tree policy admits.
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

/// Read a directory's immediate children in `tree` under the node-tree
/// policy: follow symlinks (see `guard_cycle` for the loop guard), skip
/// `NODE_TREE_EXCLUDE` names. The single traversal mechanic for
/// discovery; `visit_dir` recurses its dirs and `register_package`
/// matches members/shared files against the same entries, so the two
/// can't diverge on what a node tree contains (the de-sync that serves
/// a stale image or bloats the build context).
fn read_node_dir(tree: &dyn SourceTree, dir: &Path) -> Result<Vec<NodeDirEntry>, CatalogError> {
    let children = tree.read_dir(dir).map_err(|error| CatalogError::Io { path: dir.to_path_buf(), error })?;
    let mut out = Vec::new();
    for child in children {
        if child.file_name().is_some_and(|name| is_node_tree_excluded(&name.to_string_lossy())) {
            continue;
        }
        match tree.entry_kind(&child).map_err(|error| CatalogError::Io { path: child.clone(), error })? {
            NodeTreeEntryKind::Dir => out.push(NodeDirEntry::Dir(child)),
            NodeTreeEntryKind::File => out.push(NodeDirEntry::File(child)),
        }
    }
    // Sorted, because a directory listing's order is the filesystem's, not
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

/// Walk one directory under a node root:
///   - if it has `package.toml`, it's a package root;
///   - else if it has `metadata.json`, it's a bare node;
///   - else recurse into subdirectories.
/// A unit does not nest: once a package root or bare node is detected,
/// its interior is not re-scanned. Depth and position are irrelevant;
/// a unit can sit directly under a root or ten levels deep.
///
/// Nothing here fails the catalog: a folder that cannot be read or that
/// loops back on itself is recorded as a problem and the walk goes on
/// with its siblings.
fn visit_dir(dir: &Path, ctx: &mut DiscoverCtx<'_>) {
    let canon = match guard_cycle(ctx.tree, dir, &ctx.chain) {
        Ok(canon) => canon,
        Err(error) => return ctx.record(Vec::new(), CatalogError::Io { path: dir.to_path_buf(), error }),
    };
    // A folder already fully processed (reached again through a second
    // symlink path) registers nothing twice and is not re-walked.
    if !ctx.done.insert(canon.clone()) {
        return;
    }
    let entries = match read_node_dir(ctx.tree, dir) {
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
    let entry = match load_node_entry(ctx.tree, dir, dir, None, &ctx.broken_files) {
        Ok(loaded) => match ctx.take_loaded(loaded, dir) {
            Some((entry, _)) => entry,
            None => return,
        },
        Err(e) => return ctx.record(declared_type(ctx.tree, dir).into_iter().collect(), e),
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
        root_apps: BTreeMap::new(),
        inheriting_apps: BTreeSet::new(),
    });
}

/// A package root: `package.toml` names the package and carries shared
/// cargo deps. Members are auto-detected (any immediate subdir with a
/// `metadata.json`), so the author never maintains a node list. Shared
/// `.rs` files at the root are bundled into the package module.
fn register_package(dir: &Path, entries: Vec<NodeDirEntry>, ctx: &mut DiscoverCtx<'_>) {
    // Whatever makes the package itself unloadable takes out every
    // member with it, named by the types they claim.
    let parsed = match load_package_toml(ctx.tree, &dir.join("package.toml")) {
        Ok(parsed) => parsed,
        Err(e) => return ctx.record(declared_member_types(ctx.tree, &entries), e),
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
        match load_package_defaults(ctx.tree, dir, &ctx.broken_files) {
            Ok(d) => Some(d),
            Err(e) => return ctx.record(declared_member_types(ctx.tree, &entries), e),
        }
    } else {
        None
    };
    // The defaults are checked once, here, against the file they are
    // written in, keys and ports alike, so a default every member
    // overrides is still checked and a member reports nothing it inherits.
    if let Some(defaults) = &package_defaults {
        let mut misspelled = Vec::new();
        spelling::check_document(defaults, &mut misspelled);
        spelling::check_defaults_ports(defaults, &mut misspelled);
        let subject = format!("package '{}' defaults", parsed.package.name);
        ctx.warn_spelling(&subject, &dir.join("metadata.json"), misspelled);
    }
    // The root's apps, checked once in `settle` against this file. One
    // that does not parse is no app to check: every member inheriting it
    // fails to load on that parse.
    let root_apps: BTreeMap<String, AppRegistration> = package_defaults
        .as_ref()
        .and_then(|d| d.get("accessApps"))
        .and_then(|apps| serde_json::from_value(apps.clone()).ok())
        .unwrap_or_default();

    // Auto-detect members: every immediate subdir whose own
    // `read_node_dir` view contains a `metadata.json`. Shared `.rs` files live at the
    // package root. All of this is the `read_node_dir` view (the
    // package's `entries` plus each member's), so a package's tree is
    // seen identically by discovery, staging, and hashing.
    let mut loaded: Vec<CatalogEntry> = Vec::new();
    let mut inheriting_apps: BTreeSet<String> = BTreeSet::new();
    let mut shared_rs: Vec<PathBuf> = Vec::new();
    let mut members_seen = 0usize;
    for entry in entries {
        match entry {
            NodeDirEntry::Dir(path) => {
                let member_entries = match read_node_dir(ctx.tree, &path) {
                    Ok(member_entries) => member_entries,
                    Err(e) => {
                        ctx.record(declared_type(ctx.tree, &path).into_iter().collect(), e);
                        continue;
                    }
                };
                if !has_node_file(&member_entries, "metadata.json") {
                    continue;
                }
                members_seen += 1;
                match load_node_entry(ctx.tree, &path, dir, package_defaults.as_ref(), &ctx.broken_files) {
                    Ok(found) => {
                        if let Some((entry, inherits_apps)) = ctx.take_loaded(found, &path) {
                            if inherits_apps {
                                inheriting_apps.insert(entry.node_type.clone());
                            }
                            loaded.push(entry);
                        }
                    }
                    Err(e) => ctx.record(declared_type(ctx.tree, &path).into_iter().collect(), e),
                }
            }
            // A package's `build.rs` is its package crate's build script
            // (`weft_compiler::codegen`), never a module of the package:
            // compiled in, it would need the build-dependencies the
            // package's library does not have.
            NodeDirEntry::File(path)
                if path.extension().and_then(|e| e.to_str()) == Some("rs")
                    && path.file_name().and_then(|n| n.to_str()) != Some("build.rs") =>
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
        ctx.warn(WarningKind::LeftOut, format!(
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
        root_apps,
        inheriting_apps,
    });
}

/// Read and parse a `package.toml`.
fn load_package_toml(tree: &dyn SourceTree, path: &Path) -> Result<PackageToml, CatalogError> {
    let raw = tree.read_to_string(path).map_err(|error| CatalogError::Io { path: path.to_path_buf(), error })?;
    toml::from_str(&raw).map_err(|e| CatalogError::Parse { path: path.to_path_buf(), error: e.to_string() })
}

/// Load a package root's partial `metadata.json` (the defaults its members
/// inherit key-by-key). The CALLER decides the file exists, from the
/// `read_node_dir` view it already holds (`has_node_file`), so this never
/// re-`stat`s the path. A file that is not a JSON object, or that carries an
/// identity key, is an error: silently ignoring it would make every member
/// quietly miss its inherited keys.
fn load_package_defaults(
    tree: &dyn SourceTree,
    package_root: &Path,
    broken_files: &HashMap<PathBuf, String>,
) -> Result<serde_json::Map<String, serde_json::Value>, CatalogError> {
    let path = package_root.join("metadata.json");
    let raw = read_metadata_file(tree, &path, broken_files)?;
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
    // A key the language dropped or renamed, refused against this file
    // rather than once per member that inherits it.
    weft_core::node::refuse_removed_metadata_keys(&serde_json::Value::Object(obj.clone()))
        .map_err(|error| CatalogError::Parse { error: stale_copy_hint(&path, error), path: path.clone() })?;
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
/// whose description exists but whose code does not yet. Either way,
/// `misspelled` holds what `spelling` found off in the names the node
/// chooses, one message each (the node is named when they become
/// warnings).
/// `inherits_apps` is whether the node's `accessApps` came from its
/// package root (its own file sets none).
enum Loaded {
    Ready { entry: Box<CatalogEntry>, misspelled: Vec<String>, inherits_apps: bool },
    Pending { node_type: String, misspelled: Vec<String> },
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
    tree: &dyn SourceTree,
    node_dir: &Path,
    package_key: &Path,
    package_defaults: Option<&serde_json::Map<String, serde_json::Value>>,
    broken_files: &HashMap<PathBuf, String>,
) -> Result<Loaded, CatalogError> {
    let meta_path = node_dir.join("metadata.json");
    let raw = read_metadata_file(tree, &meta_path, broken_files)?;
    let mut value: serde_json::Value =
        serde_json::from_str(&raw).map_err(|e| CatalogError::Parse {
            path: meta_path.clone(),
            error: e.to_string(),
        })?;
    // The names this file spells, walked on the value already in memory
    // and BEFORE the merge: an inherited key is the package root's to
    // spell, and `register_package` checks that file once.
    let mut misspelled = Vec::new();
    if let Some(own) = value.as_object() {
        spelling::check_document(own, &mut misspelled);
    }
    // A key the language dropped or renamed, refused against this file
    // and on this file's own keys: the root's are refused once, against
    // the root (`load_package_defaults`).
    weft_core::node::refuse_removed_metadata_keys(&value)
        .map_err(|error| CatalogError::Parse { error: stale_copy_hint(&meta_path, error), path: meta_path.clone() })?;
    // The top-level keys this node takes from its package root: what
    // they spell is the root's, checked once there.
    let inherited: BTreeSet<String> = match (package_defaults, value.as_object()) {
        (Some(defaults), Some(own)) => defaults.keys().filter(|k| !own.contains_key(*k)).cloned().collect(),
        _ => BTreeSet::new(),
    };
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
    let parse_error = |error: String| CatalogError::Parse { error: stale_copy_hint(&meta_path, error), path: meta_path.clone() };
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
    // The compiler gives its own nodes these types (a loop's boundaries,
    // the opaque block an `@include` it does not open up leaves): a
    // catalog node under one of them would be mistaken for the compiler's.
    if weft_core::project::own_types::is_own(&metadata.node_type) {
        return Err(CatalogError::Parse {
            path: meta_path.clone(),
            error: format!("node type '{}' is a name weft gives its own nodes; rename it", metadata.node_type),
        });
    }

    spelling::check_node(&metadata, |key| inherited.contains(key), &mut misspelled);

    // The description is sound; the code may not be there yet. The
    // same `read_node_dir` view the walk and the staging use decides.
    if !has_node_file(&read_node_dir(tree, node_dir)?, "mod.rs") {
        return Ok(Loaded::Pending { node_type: metadata.node_type, misspelled });
    }

    Ok(Loaded::Ready {
        entry: Box::new(CatalogEntry {
            node_type: metadata.node_type.clone(),
            metadata,
            source_dir: node_dir.to_path_buf(),
            package_key: package_key.to_path_buf(),
        }),
        misspelled,
        inherits_apps: inherited.contains("accessApps"),
    })
}

/// `error` about a node's `metadata.json` or `deps.toml` at `path`, with
/// the one-command re-sync named when the file sits in a project's copy
/// of the standard library: a stale `base_catalog` copy is the common way
/// to hold a file this weft no longer accepts (a shape serde refuses, a
/// key dropped or renamed, a setting the language now owns).
fn stale_copy_hint(path: &Path, error: String) -> String {
    if path.components().any(|c| c.as_os_str() == "base_catalog") {
        format!("{error} (a stale base_catalog copy? run `weft catalog update` in the project to re-sync it)")
    } else {
        error
    }
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

    /// A directory shaped like a weft checkout, per `weft_core::checkout::is_weft_checkout`.
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
        assert_eq!(cat.warnings().iter().filter(|w| w.message.contains("no mod.rs yet")).count(), 3, "{:?}", cat.warnings());
        assert_eq!(cat.warnings().len(), 3, "nothing else complained: {:?}", cat.warnings());
    }

    /// A shared file named like a member's module (`not.rs` beside the
    /// node `Not`, which compiles into module `not`) would be two modules
    /// of one name in the package's crate: the whole package is refused
    /// at load, naming the file, whatever program uses it.
    #[test]
    fn a_shared_file_named_like_a_member_module_refuses_the_package() {
        let root = tempfile::tempdir().expect("temp root");
        copy_dir(&stdlib_root().expect("stdlib root").join("logic"), &root.path().join("logic"));
        fs::write(root.path().join("logic/not.rs"), "pub fn helper() {}\n").expect("write");

        let cat = FsCatalog::discover(root.path()).expect("catalog");
        assert!(cat.lookup("Not").is_none() && cat.lookup("FirstInOrder").is_none(), "the whole package is out");
        let problem = cat.problems().iter().find(|p| p.node_types.contains(&"Not".to_string())).expect("a problem");
        let said = problem.error.to_string();
        assert!(said.contains("not.rs") && said.contains("module `not`") && said.contains("rename the file"), "{said}");

        // A shared file whose name is no Rust module name is refused the same way.
        let root = tempfile::tempdir().expect("temp root");
        copy_dir(&stdlib_root().expect("stdlib root").join("logic"), &root.path().join("logic"));
        fs::write(root.path().join("logic/my-helper.rs"), "pub fn helper() {}\n").expect("write");
        let cat = FsCatalog::discover(root.path()).expect("catalog");
        assert!(cat.lookup("Not").is_none(), "the whole package is out");
        let said = cat.problems().iter().map(|p| p.error.to_string()).collect::<Vec<_>>().join("\n");
        assert!(said.contains("my-helper.rs") && said.contains("cannot take as a module name"), "{said}");
    }

    /// Two members whose types compile into one module (`NOt` and
    /// `N_ot` are both `n_ot`) refuse the package the same way.
    #[test]
    fn two_members_of_one_module_refuse_the_package() {
        let root = tempfile::tempdir().expect("temp root");
        let logic = root.path().join("logic");
        copy_dir(&stdlib_root().expect("stdlib root").join("logic"), &logic);
        for (folder, node_type) in [("n_ot_one", "NOt"), ("n_ot_two", "N_ot")] {
            copy_dir(&logic.join("not"), &logic.join(folder));
            let meta = fs::read_to_string(logic.join(folder).join("metadata.json")).expect("read");
            fs::write(logic.join(folder).join("metadata.json"), meta.replacen("\"type\": \"Not\"", &format!("\"type\": \"{node_type}\""), 1))
                .expect("write");
        }

        let cat = FsCatalog::discover(root.path()).expect("catalog");
        assert!(cat.lookup("FirstInOrder").is_none(), "the whole package is out");
        let said = cat.problems().iter().map(|p| p.error.to_string()).collect::<Vec<_>>().join("\n");
        assert!(said.contains("both compile into module `n_ot`"), "{said}");
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
        assert!(cat.warnings()[0].message.contains("has no node in it yet"), "{:?}", cat.warnings());
    }

    /// A `.weft` file (and a prompt beside it) shipped in a package, in a
    /// member folder, or in a folder of its own is a program a project
    /// may include, never a node: the catalog loads exactly what it
    /// loads without them, and nothing parses them (a broken one changes
    /// nothing).
    #[test]
    fn weft_files_in_a_node_tree_are_never_read_as_nodes() {
        let with = tempfile::tempdir().expect("temp root");
        let without = tempfile::tempdir().expect("temp root");
        for root in [with.path(), without.path()] {
            copy_dir(&stdlib_root().expect("stdlib root").join("logic"), &root.join("logic"));
        }
        fs::write(with.path().join("logic/flow.weft"), "# A flow.\nthis is { not weft\n").expect("write");
        fs::write(with.path().join("logic/flow_prompt.md"), "Be brief.\n").expect("write");
        fs::write(with.path().join("logic/not/inside.weft"), "Group {\n").expect("write");
        fs::create_dir_all(with.path().join("flows")).expect("mkdir");
        fs::write(with.path().join("flows/only.weft"), "Group {}\n").expect("write");

        let loaded = FsCatalog::discover(with.path()).expect("catalog");
        let plain = FsCatalog::discover(without.path()).expect("catalog");
        assert!(loaded.problems().is_empty(), "{:?}", loaded.problems());
        assert!(loaded.warnings().is_empty(), "{:?}", loaded.warnings());
        let types = |cat: &FsCatalog| cat.iter().map(|e| e.node_type.clone()).collect::<BTreeSet<_>>();
        assert_eq!(types(&loaded), types(&plain));
        assert!(loaded.lookup("FirstInOrder").is_some());
    }

    /// The listing of what a program can include: every `.weft` file
    /// under the root by its path, sorted, described by its first line
    /// when that line is a comment; a folder no node tree holds is not
    /// walked, and a missing root lists nothing.
    #[test]
    fn include_files_are_listed_with_their_first_line_comment() {
        let root = tempfile::tempdir().expect("temp root");
        let at = |rel: &str, body: &str| {
            let path = root.path().join(rel);
            fs::create_dir_all(path.parent().expect("a parent")).expect("mkdir");
            fs::write(path, body).expect("write");
        };
        at("base_catalog/computer/agent.weft", "#  Run an agent on a computer.  \nGroup {}\n");
        at("base_catalog/computer/perform.weft", "Group {}\n# not the first line\n");
        at("base_catalog/computer/agent_prompt.md", "# A prompt, not a program\n");
        at("my_pkg/empty.weft", "");
        at("my_pkg/bare_hash.weft", "#\n");
        at("my_pkg/node_modules/dep/x.weft", "# never listed\n");
        let not_utf8 = root.path().join("my_pkg/latin1.weft");
        fs::write(&not_utf8, b"# caf\xe9\nGroup {}\n").expect("write");

        let listed = include_files(root.path()).expect("listing");
        let file = |path: &str, description: Option<&str>| IncludeFile {
            path: path.into(),
            description: description.map(str::to_string),
        };
        assert_eq!(
            listed,
            [
                file("base_catalog/computer/agent.weft", Some("Run an agent on a computer.")),
                file("base_catalog/computer/perform.weft", None),
                file("my_pkg/bare_hash.weft", None),
                file("my_pkg/empty.weft", None),
                file("my_pkg/latin1.weft", Some("caf\u{fffd}")),
            ]
        );
        assert_eq!(include_files(&root.path().join("absent")).expect("listing"), []);
    }

    /// A `.weft` file is part of the node tree (it is seeded and listed)
    /// and never part of what a package builds from.
    #[test]
    fn a_weft_file_is_left_out_of_a_package_build_only() {
        assert!(!is_node_tree_excluded("agent.weft"));
        assert!(is_left_out_of_package_build("agent.weft"));
        assert!(!is_left_out_of_package_build("agent_prompt.md"));
        assert!(!is_left_out_of_package_build("mod.rs"));
        assert!(is_left_out_of_package_build("node_modules"));
    }

    /// A project's own node is held to weft's spelling like the stdlib:
    /// each name that is off is one warning naming the node, its file and
    /// the fix, and the node still loads. The language's own `_should_flow`
    /// (as an output: the loader refuses it as an input) and the keys of an
    /// input's `default` (the node's data) are never flagged.
    #[test]
    fn a_project_node_spelled_off_the_weft_way_loads_with_one_warning_per_name() {
        let root = tempfile::tempdir().expect("temp root");
        let node = root.path().join("nodes/profile");
        fs::create_dir_all(&node).expect("mkdir");
        fs::write(
            node.join("metadata.json"),
            r#"{
                "type": "Profile", "label": "Profile", "description": "d",
                "inputs": [
                    {"name": "user_id", "type": "String"},
                    {"name": "prefs", "type": "JsonDict", "default": {"dark_mode": true}}
                ],
                "outputs": [{"name": "_should_flow", "type": "Boolean"}],
                "features": {"isTrigger": true},
                "firesWith": {"user_name": "String"}
            }"#,
        )
        .expect("write");
        fs::write(node.join("mod.rs"), "// impl\n").expect("write");

        let cat = FsCatalog::discover(&root.path().join("nodes")).expect("catalog");
        assert!(cat.problems().is_empty(), "{:?}", cat.problems());
        assert!(cat.lookup("Profile").is_some(), "a spelling warning never leaves the node out");
        let file = node.join("metadata.json").display().to_string();
        let mut found: Vec<&str> = cat.warnings().iter().map(|w| w.message.as_str()).collect();
        found.sort();
        assert_eq!(
            found,
            [
                format!("Profile ({file}): fire payload field 'user_name' is not camelCase: write 'userName'"),
                format!("Profile ({file}): input 'user_id' is not camelCase: write 'userId'"),
            ]
        );
        assert!(cat.warnings().iter().all(|w| w.kind == WarningKind::Spelling), "{:?}", cat.warnings());
    }

    /// A package root's defaults are checked once, against the root's own
    /// file, however many members inherit them.
    #[test]
    fn a_package_default_spelled_off_is_one_warning_on_the_package_root() {
        let root = tempfile::tempdir().expect("temp root");
        let pkg = root.path().join("tools");
        fs::create_dir_all(&pkg).expect("mkdir");
        fs::write(pkg.join("package.toml"), "[package]\nname = \"tools\"\n").expect("write");
        fs::write(
            pkg.join("metadata.json"),
            r#"{"firesWith": {"user_name": "String"}, "inputs": [{"name": "user_id", "type": "{ first_name: String }"}]}"#,
        )
        .expect("write");
        // `one` inherits the root's inputs, `two` declares its own (none):
        // either way, the root's port warns once, on the root.
        for (ty, own_inputs) in [("One", ""), ("Two", r#""inputs": [], "#)] {
            let dir = pkg.join(ty.to_lowercase());
            fs::create_dir_all(&dir).expect("mkdir");
            fs::write(
                dir.join("metadata.json"),
                format!(r#"{{"type": "{ty}", "label": "{ty}", "description": "d", {own_inputs}"outputs": [], "features": {{"isTrigger": true}}}}"#),
            )
            .expect("write");
            fs::write(dir.join("mod.rs"), "// impl\n").expect("write");
        }

        let cat = FsCatalog::discover(root.path()).expect("catalog");
        assert!(cat.problems().is_empty(), "{:?}", cat.problems());
        let file = pkg.join("metadata.json").display().to_string();
        let found: Vec<&str> = cat.warnings().iter().map(|w| w.message.as_str()).collect();
        assert_eq!(
            found,
            [
                format!("package 'tools' defaults ({file}): fire payload field 'user_name' is not camelCase: write 'userName'"),
                format!("package 'tools' defaults ({file}): input 'user_id' is not camelCase: write 'userId'"),
                format!("package 'tools' defaults ({file}): record field 'first_name' is not camelCase: write 'firstName'"),
            ]
        );
    }

    /// An app declared under `accessApps` is held to every rule for a
    /// shipped app and must name a service of this catalog that takes an
    /// app. A package root's app is reported once, against the root's
    /// file, and leaves out every member inheriting it; a node's own app
    /// is reported against its own file. A public app loads.
    #[test]
    fn an_access_app_with_a_key_its_service_does_not_declare_is_left_out() {
        let stdlib = stdlib_root().expect("stdlib root");
        let load = |app: &str| {
            let root = tempfile::tempdir().expect("temp root");
            copy_dir(&stdlib.join("slack"), &root.path().join("slack"));
            fs::write(root.path().join("slack/metadata.json"), format!(r#"{{"accessApps": {app}}}"#)).expect("write");
            (FsCatalog::discover(root.path()).expect("catalog"), root)
        };
        // The one problem a refused root app makes: the root's file, every
        // member named, none of them loaded.
        let one_problem = |cat: &FsCatalog, root: &tempfile::TempDir| -> String {
            assert_eq!(cat.problems().len(), 1, "{:?}", cat.problems());
            let problem = &cat.problems()[0];
            assert!(problem.node_types.len() > 1 && problem.node_types.iter().all(|t| cat.lookup(t).is_none()), "{problem:?}");
            let file = root.path().join("slack/metadata.json").display().to_string();
            let error = problem.error.to_string();
            error.strip_prefix(&format!("parse: {file}: ")).unwrap_or_else(|| panic!("names the root's file: {error}")).to_string()
        };

        let (cat, root) = load(r#"{"slack": {"label": "Ours", "client_id": "c", "appTokn": "x"}}"#);
        assert_eq!(
            one_problem(&cat, &root),
            "accessApps.slack: the app has a field 'appTokn' its service does not declare (known: label, client_id)"
        );

        // `appToken` is a field Slack marks secret: a shipped app is public.
        let (cat, root) = load(r#"{"slack": {"label": "Ours", "client_id": "c", "appToken": "x"}}"#);
        assert!(one_problem(&cat, &root).starts_with("accessApps.slack: 'appToken' is a secret"));

        let (cat, root) = load(r#"{"slack": {"label": "", "client_id": "c"}}"#);
        assert_eq!(one_problem(&cat, &root), "accessApps.slack: the app needs a name (shown in the connection list)");

        let (cat, root) = load(r#"{"slak": {"label": "Ours", "client_id": "c"}}"#);
        assert_eq!(
            one_problem(&cat, &root),
            "accessApps.slak: no node in this project declares a service named 'slak' (it declares: slack), so \
             this app would never be used; name one of those, or remove the app"
        );

        let (cat, _root) = load(r#"{"slack": {"label": "Ours", "client_id": "c"}}"#);
        assert!(cat.problems().is_empty(), "{:?}", cat.problems());

        // A node's own app for a service that connects without one: its
        // own file, itself left out.
        let root = tempfile::tempdir().expect("temp root");
        let node = root.path().join("keyed");
        copy_dir(&stdlib.join("ai/elevenlabs/access"), &node);
        let mut meta: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(node.join("metadata.json")).expect("read")).expect("json");
        meta["accessApps"] = serde_json::json!({ "elevenlabs": { "label": "Ours", "client_id": "c" } });
        fs::write(node.join("metadata.json"), meta.to_string()).expect("write");
        let cat = FsCatalog::discover(root.path()).expect("catalog");
        let errors: Vec<String> = cat.problems().iter().map(|p| p.error.to_string()).collect();
        assert_eq!(
            errors,
            [format!(
                "parse: {}: accessApps.elevenlabs: the 'elevenlabs' service connects without an app (only a \
                 service that signs in through OAuth takes one), so this app would never be used; remove it",
                node.join("metadata.json").display()
            )]
        );
    }

    /// Copies the standard node at `from` (under the stdlib) to `to`, its
    /// `metadata.json` changed by `edit`.
    fn copy_node_editing(from: &str, to: &Path, edit: impl FnOnce(&mut serde_json::Value)) {
        copy_dir(&stdlib_root().expect("stdlib root").join(from), to);
        let mut meta: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(to.join("metadata.json")).expect("read")).expect("json");
        edit(&mut meta);
        fs::write(to.join("metadata.json"), meta.to_string()).expect("write");
    }

    /// An app naming a service whose every declarer was left out by a
    /// collision is left out too, told that collision, never "no node in
    /// this project declares" a service the project does declare. Two
    /// ways a copied access node takes its service out: as a verbatim copy
    /// (its type clashes) and renamed (only its service clashes).
    #[test]
    fn an_access_app_whose_service_a_collision_took_out_names_that_collision() {
        for rename in [false, true] {
            let root = tempfile::tempdir().expect("temp root");
            copy_dir(&stdlib_root().expect("stdlib root").join("slack"), &root.path().join("slack"));
            let file = root.path().join("slack/metadata.json");
            fs::write(&file, r#"{"accessApps": {"slack": {"label": "Ours", "client_id": "c"}}}"#).expect("write");
            copy_node_editing("slack/access", &root.path().join("slack_copy"), |meta| {
                if rename {
                    meta["type"] = "SlackAccessCopy".into();
                }
            });

            let cat = FsCatalog::discover(root.path()).expect("catalog");
            let problems = cat.problems();
            assert_eq!(problems.len(), 2, "{problems:?}");
            if rename {
                assert!(matches!(problems[0].error, CatalogError::ServiceCollision { .. }), "{problems:?}");
            } else {
                assert!(matches!(problems[0].error, CatalogError::Collision { .. }), "{problems:?}");
            }
            assert_eq!(
                problems[1].error.to_string(),
                format!(
                    "parse: {}: accessApps.slack: every node declaring the 'slack' service was left out, so \
                     this app has no service to belong to: {}",
                    file.display(),
                    problems[0].error
                )
            );
            let members = &problems[1].node_types;
            assert!(members.contains(&"SlackSendMessage".to_string()), "{members:?}");
            assert!(members.iter().all(|t| cat.lookup(t).is_none()), "{members:?}");
        }
    }

    /// A refused app that takes out the node declaring a service takes out
    /// every app elsewhere naming that service, each told the first
    /// refusal: the app checks repeat over what is left until a round
    /// refuses nothing, so an app is never let through against a service
    /// that has already left the catalog.
    #[test]
    fn an_access_app_whose_service_a_refused_app_took_out_names_that_refusal() {
        let root = tempfile::tempdir().expect("temp root");
        copy_dir(&stdlib_root().expect("stdlib root").join("slack"), &root.path().join("slack"));
        // `SlackAccess` sets no `accessApps` of its own, so it inherits
        // this bad one and leaves with the rest of the package.
        fs::write(
            root.path().join("slack/metadata.json"),
            r#"{"accessApps": {"slack": {"label": "Ours", "client_id": "c", "appTokn": "x"}}}"#,
        )
        .expect("write");
        let keyed = root.path().join("keyed");
        copy_node_editing("ai/elevenlabs/access", &keyed, |meta| {
            meta["accessApps"] = serde_json::json!({ "slack": { "label": "Ours", "client_id": "c" } });
        });

        let cat = FsCatalog::discover(root.path()).expect("catalog");
        let problems = cat.problems();
        assert_eq!(problems.len(), 2, "{problems:?}");
        assert!(problems[0].node_types.contains(&"SlackAccess".to_string()), "{problems:?}");
        assert!(problems[0].error.to_string().ends_with("its service does not declare (known: label, client_id)"));
        let node_type = cat.problems()[1].node_types.clone();
        assert!(node_type.len() == 1 && cat.lookup(&node_type[0]).is_none(), "{node_type:?}");
        assert_eq!(
            problems[1].error.to_string(),
            format!(
                "parse: {}: accessApps.slack: every node declaring the 'slack' service was left out, so \
                 this app has no service to belong to: {}",
                keyed.join("metadata.json").display(),
                problems[0].error
            )
        );
    }

    /// A key still spelled the way it was before it became camelCase is
    /// refused naming its new spelling, against the file it is written
    /// in: a package root's once, a stale `base_catalog` copy's with the
    /// re-sync named.
    #[test]
    fn a_renamed_metadata_key_is_refused_against_its_own_file_naming_the_new_spelling() {
        let root = tempfile::tempdir().expect("temp root");
        let pkg = root.path().join("tools");
        fs::create_dir_all(&pkg).expect("mkdir");
        fs::write(pkg.join("package.toml"), "[package]\nname = \"tools\"\n").expect("write");
        fs::write(pkg.join("metadata.json"), r#"{"requires_infra": true}"#).expect("write");
        for ty in ["One", "Two"] {
            let dir = pkg.join(ty.to_lowercase());
            fs::create_dir_all(&dir).expect("mkdir");
            fs::write(
                dir.join("metadata.json"),
                format!(r#"{{"type": "{ty}", "label": "{ty}", "description": "d", "inputs": [], "outputs": []}}"#),
            )
            .expect("write");
            fs::write(dir.join("mod.rs"), "// impl\n").expect("write");
        }
        let stale = root.path().join("base_catalog/old");
        fs::create_dir_all(&stale).expect("mkdir");
        fs::write(
            stale.join("metadata.json"),
            r#"{"type": "Old", "label": "Old", "description": "d", "inputs": [{"name": "x", "type": "String", "free_text": true}], "outputs": []}"#,
        )
        .expect("write");
        fs::write(stale.join("mod.rs"), "// impl\n").expect("write");

        let cat = FsCatalog::discover(root.path()).expect("catalog");
        let mut errors: Vec<(Vec<String>, String)> =
            cat.problems().iter().map(|p| (p.node_types.clone(), p.error.to_string())).collect();
        errors.sort();
        assert_eq!(
            errors,
            [
                (
                    vec!["Old".to_string()],
                    format!(
                        "parse: {}: `free_text` (under `inputs.[]`) is spelled `freeText` now: every metadata.json key \
                         is camelCase, and the old spelling is no longer read. Rename it (a stale base_catalog copy? \
                         run `weft catalog update` in the project to re-sync it)",
                        stale.join("metadata.json").display()
                    ),
                ),
                (
                    vec!["One".to_string(), "Two".to_string()],
                    format!(
                        "parse: {}: `requires_infra` is spelled `requiresInfra` now: every metadata.json key is \
                         camelCase, and the old spelling is no longer read. Rename it",
                        pkg.join("metadata.json").display()
                    ),
                ),
            ]
        );
    }

    /// A type the package root declares is checked where it is declared:
    /// a field spelled off warns once, on the root, and the members whose
    /// ports use the type add nothing.
    #[test]
    fn a_declared_types_field_spelled_off_warns_once_where_declared() {
        let root = tempfile::tempdir().expect("temp root");
        let pkg = root.path().join("tools");
        fs::create_dir_all(&pkg).expect("mkdir");
        fs::write(pkg.join("package.toml"), "[package]\nname = \"tools\"\n").expect("write");
        fs::write(pkg.join("metadata.json"), r#"{"types": {"Place": "{ street_name: String }"}}"#).expect("write");
        for ty in ["One", "Two"] {
            let dir = pkg.join(ty.to_lowercase());
            fs::create_dir_all(&dir).expect("mkdir");
            fs::write(
                dir.join("metadata.json"),
                format!(
                    r#"{{"type": "{ty}", "label": "{ty}", "description": "d", "inputs": [], "outputs": [{{"name": "place", "type": "Place"}}]}}"#
                ),
            )
            .expect("write");
            fs::write(dir.join("mod.rs"), "// impl\n").expect("write");
        }

        let cat = FsCatalog::discover(root.path()).expect("catalog");
        assert!(cat.problems().is_empty(), "{:?}", cat.problems());
        let file = pkg.join("metadata.json").display().to_string();
        let found: Vec<&str> = cat.warnings().iter().map(|w| w.message.as_str()).collect();
        assert_eq!(
            found,
            [format!("package 'tools' defaults ({file}): record field 'street_name' is not camelCase: write 'streetName'")]
        );
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
            // Both files are named, the one that declared it first included.
            let (a, b) = (root.path().join("a/metadata.json"), root.path().join("b/metadata.json"));
            assert!(told.contains(&format!("first in {}", a.display())) && told.contains(&format!("then in {}", b.display())), "{told}");
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
    /// with the service name and `connectionOptional` from its own
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

#[cfg(test)]
mod tree_tests {
    use super::*;
    use std::sync::Arc;

    /// A node tree held in memory: files by absolute path, a directory
    /// being any path some file sits under, every path its own identity.
    struct MapTree(BTreeMap<PathBuf, Vec<u8>>);

    impl SourceTree for MapTree {
        fn read_dir(&self, dir: &Path) -> std::io::Result<Vec<PathBuf>> {
            self.entry_kind(dir)?;
            let children: BTreeSet<PathBuf> = self
                .0
                .keys()
                .filter_map(|file| file.strip_prefix(dir).ok()?.components().next())
                .map(|first| dir.join(first))
                .collect();
            Ok(children.into_iter().collect())
        }

        fn entry_kind(&self, path: &Path) -> std::io::Result<NodeTreeEntryKind> {
            if self.0.contains_key(path) {
                Ok(NodeTreeEntryKind::File)
            } else if self.0.keys().any(|file| file != path && file.starts_with(path)) {
                Ok(NodeTreeEntryKind::Dir)
            } else {
                Err(std::io::ErrorKind::NotFound.into())
            }
        }

        fn read(&self, path: &Path) -> std::io::Result<Vec<u8>> {
            self.0.get(path).cloned().ok_or_else(|| std::io::ErrorKind::NotFound.into())
        }

        fn canonicalize(&self, dir: &Path) -> std::io::Result<PathBuf> {
            self.entry_kind(dir).map(|_| dir.to_path_buf())
        }
    }

    /// A bare node under `nodes/` and a package beside the code under
    /// `src/`, by path under the project.
    const PROJECT: &[(&str, &str)] = &[
        ("nodes/x/metadata.json", r#"{"type": "Feed", "label": "Feed", "description": "d", "inputs": [], "outputs": []}"#),
        ("nodes/x/mod.rs", "// impl\n"),
        ("nodes/x/deps.toml", "[dependencies]\nserde = \"1\"\n"),
        ("src/billing/pkg/package.toml", "[package]\nname = \"billing\"\n"),
        ("src/billing/pkg/shared.rs", "pub fn helper() {}\n"),
        ("src/billing/pkg/charge/metadata.json", r#"{"type": "Charge", "label": "Charge", "description": "d", "inputs": [], "outputs": []}"#),
        ("src/billing/pkg/charge/mod.rs", "// impl\n"),
    ];

    fn map_tree(project: &Path) -> Arc<dyn SourceTree> {
        Arc::new(MapTree(PROJECT.iter().map(|(rel, body)| (project.join(rel), body.as_bytes().to_vec())).collect()))
    }

    /// Everything discovery found, in a form two catalogs compare by.
    fn found(cat: &FsCatalog) -> (Vec<String>, Vec<String>, Vec<CatalogWarning>) {
        let mut entries: Vec<String> = cat
            .iter()
            .map(|e| {
                let metadata = serde_json::to_string(&e.metadata).expect("metadata serializes");
                format!("{} {} {} {metadata}", e.node_type, e.source_dir.display(), e.package_key.display())
            })
            .collect();
        entries.sort();
        let mut packages: Vec<String> = cat
            .packages()
            .map(|p| format!("{} {} {:?} {:?} {:?}", p.name, p.root.display(), p.node_types, p.shared_rs, p.package_deps))
            .collect();
        packages.sort();
        (entries, packages, cat.warnings().to_vec())
    }

    /// The same files discovered from memory and from disk make the same
    /// catalog, across both roots.
    #[test]
    fn a_tree_in_memory_discovers_like_the_same_files_on_disk() {
        let project = tempfile::tempdir().expect("temp project");
        for (rel, body) in PROJECT {
            let path = project.path().join(rel);
            fs::create_dir_all(path.parent().expect("a parent")).expect("mkdir");
            fs::write(path, body).expect("write");
        }
        let roots = [project.path().join("nodes"), project.path().join("src")];
        let roots: Vec<&Path> = roots.iter().map(|r| r.as_path()).collect();

        let on_disk = FsCatalog::discover_roots(&roots).expect("disk catalog");
        let in_memory = FsCatalog::discover_roots_in(map_tree(project.path()), &roots).expect("memory catalog");
        assert!(on_disk.problems().is_empty(), "{:?}", on_disk.problems());
        assert!(in_memory.problems().is_empty(), "{:?}", in_memory.problems());
        assert!(in_memory.lookup("Feed").is_some() && in_memory.lookup("Charge").is_some());
        assert_eq!(found(&in_memory), found(&on_disk));
    }

    /// A package's `build.rs` is its build script, never one of the shared
    /// files its members compile with.
    #[test]
    fn a_package_build_script_is_no_shared_module() {
        let dir = tempfile::tempdir().expect("tempdir");
        let pkg = dir.path().join("nodes/tools");
        fs::create_dir_all(pkg.join("feed")).expect("mkdir");
        fs::write(pkg.join("package.toml"), "[package]\nname = \"tools\"\n").expect("write");
        fs::write(pkg.join("build.rs"), "pub fn main() {}\n").expect("write");
        fs::write(pkg.join("helpers.rs"), "pub fn help() {}\n").expect("write");
        fs::write(pkg.join("feed/metadata.json"), r#"{"type": "Feed", "label": "Feed", "description": "d", "inputs": [], "outputs": []}"#).expect("write");
        fs::write(pkg.join("feed/mod.rs"), "// impl\n").expect("write");
        let catalog = FsCatalog::discover(&dir.path().join("nodes")).expect("discover");
        let shared: Vec<String> = catalog
            .package_of("Feed")
            .expect("its package")
            .shared_rs
            .iter()
            .map(|p| p.file_name().unwrap().to_string_lossy().into_owned())
            .collect();
        assert_eq!(shared, ["helpers.rs"]);
    }

    /// A `deps.toml` or `tests.rs` that is a link to nothing is an error
    /// naming it: never a node with no deps, never a package with no tests.
    #[test]
    fn a_dangling_optional_file_is_an_error_not_an_absence() {
        let dir = tempfile::tempdir().expect("tempdir");
        let node = dir.path().join("nodes/feed");
        fs::create_dir_all(&node).expect("mkdir");
        fs::write(node.join("metadata.json"), r#"{"type": "Feed", "label": "Feed", "description": "d", "inputs": [], "outputs": []}"#).expect("write");
        fs::write(node.join("mod.rs"), "// impl\n").expect("write");
        let catalog = FsCatalog::discover(&dir.path().join("nodes")).expect("discover");
        assert!(catalog.deps("Feed").expect("no deps.toml at all").is_none());
        let package = catalog.package_of("Feed").expect("its package").clone();
        assert!(!catalog.package_declares_tests(&package).expect("no tests.rs at all"));

        std::os::unix::fs::symlink(dir.path().join("gone.toml"), node.join("deps.toml")).expect("link");
        std::os::unix::fs::symlink(dir.path().join("gone.rs"), node.join("tests.rs")).expect("link");
        let err = catalog.deps("Feed").expect_err("a link to nothing").to_string();
        assert!(err.contains("deps.toml"), "{err}");
        let err = catalog.package_declares_tests(&package).expect_err("a link to nothing").to_string();
        assert!(err.contains("tests.rs"), "{err}");
    }

    /// A node's `[[needs]]` read through the catalog, each with its stage
    /// and packages; a `deps.toml` with any other key is refused, naming
    /// it, never read as a node that needs nothing.
    #[test]
    fn a_nodes_needs_read_from_its_deps_toml_and_any_other_key_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let node = dir.path().join("nodes/run");
        fs::create_dir_all(&node).expect("mkdir");
        fs::write(node.join("metadata.json"), r#"{"type": "Run", "label": "Run", "description": "d", "inputs": [], "outputs": []}"#).expect("write");
        fs::write(node.join("mod.rs"), "// impl\n").expect("write");
        fs::write(
            node.join("deps.toml"),
            "[[needs]]\nname = \"python3\"\ncheck = [\"python3\", \"--version\"]\n[needs.install.apt]\ndefault = [\"python3\"]\n\n\
             [[needs]]\nname = \"libvips\"\nstage = \"build\"\ncheck = [\"pkg-config\", \"vips\"]\n[needs.install.apt]\ndefault = [\"libvips-dev\"]\n",
        )
        .expect("write");
        let catalog = FsCatalog::discover(&dir.path().join("nodes")).expect("discover");
        let needs = catalog.deps("Run").expect("deps read").expect("a deps.toml").needs;
        assert_eq!(needs.iter().map(|n| (n.name.as_str(), n.stage)).collect::<Vec<_>>(), [
            ("python3", weft_core::needs::NeedStage::Run),
            ("libvips", weft_core::needs::NeedStage::Build),
        ]);
        assert_eq!(needs[1].packages_for(weft_core::needs::PackageManager::Apt, Some("debian_12")), Some(&["libvips-dev".to_string()][..]));

        fs::write(node.join("deps.toml"), "[system.runtime.apt]\ndefault = [\"python3\"]\n").expect("write");
        let err = catalog.deps("Run").expect_err("an unknown key").to_string();
        assert!(err.contains("system"), "{err}");
        assert!(!err.contains("weft catalog update"), "a project's own node is no stale copy: {err}");

        // The same in a project's copy of the standard library names the re-sync.
        let copy = dir.path().join("nodes/base_catalog/basic/old");
        fs::create_dir_all(&copy).expect("mkdir");
        fs::write(copy.join("metadata.json"), r#"{"type": "Old", "label": "Old", "description": "d", "inputs": [], "outputs": []}"#).expect("write");
        fs::write(copy.join("mod.rs"), "// impl\n").expect("write");
        fs::write(copy.join("deps.toml"), "[system.runtime.apt]\ndefault = [\"python3\"]\n").expect("write");
        let catalog = FsCatalog::discover(&dir.path().join("nodes")).expect("discover");
        let err = catalog.deps("Old").expect_err("an unknown key").to_string();
        assert!(err.contains("weft catalog update"), "{err}");
    }

    /// The compiler flags of a worker build are weft's own: a node setting
    /// them in `[build.env]` is refused, naming where its flag goes, rather
    /// than having it silently replaced (or replacing weft's).
    #[test]
    fn a_node_setting_the_compiler_flags_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let node = dir.path().join("nodes/fast");
        fs::create_dir_all(&node).expect("mkdir");
        fs::write(node.join("metadata.json"), r#"{"type": "Fast", "label": "Fast", "description": "d", "inputs": [], "outputs": []}"#).expect("write");
        fs::write(node.join("mod.rs"), "// impl\n").expect("write");
        fs::write(node.join("deps.toml"), "[build.env]\nRUSTFLAGS = \"-C target-cpu=native\"\n").expect("write");
        let catalog = FsCatalog::discover(&dir.path().join("nodes")).expect("discover");
        let err = catalog.deps("Fast").expect_err("weft's own flags").to_string();
        assert!(err.contains("RUSTFLAGS") && err.contains("build.rs"), "{err}");
        fs::write(node.join("deps.toml"), "[build.env]\nFAST_CONFIG = \"{{catalog_path}}/fast.txt\"\n").expect("write");
        assert!(catalog.deps("Fast").expect("any other variable is the node's").is_some());
    }

    /// A catalog's later reads go to the tree it was discovered from:
    /// nothing of this project exists on disk, and `deps` still reads
    /// the snapshot's `deps.toml`.
    #[test]
    fn deps_read_the_tree_the_catalog_was_discovered_from() {
        let project = Path::new("/weft-catalog-test/nowhere-on-disk");
        assert!(!project.exists());
        let roots = [project.join("nodes"), project.join("src")];
        let roots: Vec<&Path> = roots.iter().map(|r| r.as_path()).collect();
        let cat = FsCatalog::discover_roots_in(map_tree(project), &roots).expect("memory catalog");

        let deps = cat.deps("Feed").expect("deps read").expect("Feed has a deps.toml");
        assert_eq!(deps.dependencies.get("serde").and_then(|v| v.as_str()), Some("1"));
        assert!(cat.deps("Charge").expect("deps read").is_none(), "Charge has no deps.toml");
    }
}
