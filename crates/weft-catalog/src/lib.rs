//! Filesystem-backed node catalog.
//!
//! A catalog source is a directory tree of node packages. A package
//! is either:
//!
//! - **Single-node package**: a directory with `metadata.json` at
//!   its root. The directory IS the node. No `package.toml` needed.
//!   Files: `metadata.json`, `mod.rs`, `deps.toml` (optional),
//!   `form_field_specs.json` (optional, for nodes declaring
//!   `has_form_schema`).
//!
//! - **Multi-node package**: a directory with `package.toml` at its
//!   root, declaring which subdirectories contain nodes. Each node
//!   subdirectory holds its own `metadata.json`, `mod.rs`, and
//!   optional `deps.toml`. The package root can also hold shared
//!   Rust files (`.rs`) and shared assets (`.json` etc.) accessible
//!   to every node in the package via `use super::<name>;` from a
//!   node's `mod.rs`.
//!
//! Packages can be arbitrarily nested under the catalog root. Any
//! subdirectory matching one of the two shapes above is a package.
//!
//! The compiler and dispatcher use this crate to look up node
//! metadata without compiling node Rust code. The emitted project
//! binary compiles node code directly via `#[path]` includes driven
//! by codegen; it does NOT use this crate at runtime.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use weft_core::node::{FormFieldSpec, MetadataCatalog, NodeMetadata};

// ----- Filesystem-backed catalog -------------------------------------

#[derive(Debug, Clone)]
pub struct CatalogEntry {
    pub node_type: String,
    pub metadata: NodeMetadata,
    /// Directory containing the node's `mod.rs` and `metadata.json`.
    pub source_dir: PathBuf,
    /// Origin of this entry. Governs shadowing order (User beats
    /// Vendor beats Stdlib when a node type is defined in multiple
    /// sources).
    pub origin: CatalogOrigin,
    /// Form field specs for nodes with `features.has_form_schema`.
    /// Loaded from a JSON file resolved relative to the package
    /// root (via metadata's `form_field_specs_ref`, defaulting to
    /// `form_field_specs.json`). Empty vec if no such file exists.
    pub form_field_specs: Vec<FormFieldSpec>,
    /// Key identifying which package this entry belongs to. Maps
    /// back into `FsCatalog::packages` for shared-file lookup.
    /// Single-node packages have package_key == source_dir.
    pub package_key: PathBuf,
}

/// A node package: either a single-node dir or a multi-node dir
/// with `package.toml`.
#[derive(Debug, Clone)]
pub struct Package {
    /// Package root directory. Single-node packages: same as the
    /// node's source_dir. Multi-node: the directory holding
    /// `package.toml`.
    pub root: PathBuf,
    /// Logical name. Derived from `package.toml` (`[package].name`)
    /// if present, otherwise from the directory name.
    pub name: String,
    /// Node types declared by this package.
    pub node_types: Vec<String>,
    /// Shared `.rs` files at the package root (multi-node only;
    /// empty for single-node). Paths are absolute.
    pub shared_rs: Vec<PathBuf>,
    /// Package-level cargo deps. Applied when any node in the
    /// package is referenced. `None` for single-node packages
    /// (their deps come from the node's `deps.toml`).
    pub package_deps: Option<toml::Table>,
    /// Origin of this package. Every node in it inherits this
    /// origin for shadowing purposes.
    pub origin: CatalogOrigin,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CatalogOrigin {
    Stdlib,
    User,
    Vendor,
}

/// Directory scanned by a `FsCatalog`. `root` is walked recursively;
/// any subdir containing `metadata.json` is treated as a node.
#[derive(Debug, Clone)]
pub struct CatalogSource {
    pub root: PathBuf,
    pub origin: CatalogOrigin,
}

pub struct FsCatalog {
    entries: HashMap<String, CatalogEntry>,
    /// All discovered packages, keyed by package root. Each
    /// `CatalogEntry` has a `package_key` pointing back in here.
    packages: HashMap<PathBuf, Package>,
}

impl FsCatalog {
    /// Walk every source, discover packages and nodes, build the
    /// type → entry map. Later sources shadow earlier ones, so pass
    /// them in priority order (low → high): stdlib, vendor, user.
    pub fn discover(sources: &[CatalogSource]) -> Result<Self, CatalogError> {
        let mut entries: HashMap<String, CatalogEntry> = HashMap::new();
        let mut packages: HashMap<PathBuf, Package> = HashMap::new();
        for source in sources {
            if !source.root.exists() {
                continue;
            }
            discover_source(&source.root, source.origin, &mut entries, &mut packages)?;
        }
        Ok(Self { entries, packages })
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
    fn form_field_specs(&self, node_type: &str) -> &[FormFieldSpec] {
        self.entries
            .get(node_type)
            .map(|e| e.form_field_specs.as_slice())
            .unwrap_or(&[])
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
/// to the node's directory inside the builder container's
/// `/catalog` mount. Example, a node shipping a config file:
///
/// ```toml
/// [build.env]
/// FOO_CONFIG = "{{catalog_path}}/foo-config.txt"
/// ```
///
/// resolves to `/catalog/basic/exec_python/foo-config.txt` if
/// the node lives at `catalog/basic/exec_python/`.
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

// ----- Standard stdlib location --------------------------------------

/// Filesystem path to this repo's bundled stdlib catalog. Resolved
/// at compile time from the crate's own `CARGO_MANIFEST_DIR`.
///
/// Layout: `<weft-repo>/crates/weft-catalog` → parent → parent →
/// `catalog`. If the repo layout changes, update this function.
pub fn stdlib_root() -> PathBuf {
    // Container images (dispatcher / worker) bake the catalog at
    // /catalog and set `WEFT_CATALOG_ROOT` so binaries don't rely
    // on cargo's compile-time source layout. Local development
    // uses the CARGO_MANIFEST_DIR fallback.
    if let Ok(override_path) = std::env::var("WEFT_CATALOG_ROOT") {
        return PathBuf::from(override_path);
    }
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .map(|p| p.join("catalog"))
        .expect("stdlib_root: cannot resolve weft repo layout")
}

/// Convenience: discover a catalog containing only stdlib. Used by
/// the dispatcher and CLI for introspection; the compiler uses the
/// fuller version that also includes user+vendor sources.
pub fn stdlib_catalog() -> Result<FsCatalog, CatalogError> {
    FsCatalog::discover(&[CatalogSource {
        root: stdlib_root(),
        origin: CatalogOrigin::Stdlib,
    }])
}

// ----- Package discovery ---------------------------------------------

/// Shape of `package.toml` for multi-node packages.
#[derive(Debug, Clone, Deserialize)]
struct PackageToml {
    package: PackageSection,
    #[serde(default)]
    dependencies: toml::Table,
}

#[derive(Debug, Clone, Deserialize)]
struct PackageSection {
    name: String,
    #[serde(default)]
    nodes: Vec<String>,
}

/// Walk one source root, detect packages, register nodes. Later
/// sources (called with higher-priority origins) shadow by
/// inserting into the same `entries` map.
fn discover_source(
    root: &Path,
    origin: CatalogOrigin,
    entries: &mut HashMap<String, CatalogEntry>,
    packages: &mut HashMap<PathBuf, Package>,
) -> Result<(), CatalogError> {
    visit_dir(root, origin, entries, packages)
}

/// Recursive directory visitor. For each directory encountered:
///   - If it has `package.toml`, treat as multi-node package.
///   - Else if it has `metadata.json`, treat as single-node package.
///   - Else recurse into subdirectories.
/// Packages do not nest: once a package is detected, its interior
/// is not re-scanned for sub-packages.
fn visit_dir(
    dir: &Path,
    origin: CatalogOrigin,
    entries: &mut HashMap<String, CatalogEntry>,
    packages: &mut HashMap<PathBuf, Package>,
) -> Result<(), CatalogError> {
    let pkg_toml = dir.join("package.toml");
    if pkg_toml.is_file() {
        register_multi_node_package(dir, &pkg_toml, origin, entries, packages)?;
        return Ok(());
    }
    if dir.join("metadata.json").is_file() {
        register_single_node_package(dir, origin, entries, packages)?;
        return Ok(());
    }
    let read = fs::read_dir(dir).map_err(|e| CatalogError::Io {
        path: dir.to_path_buf(),
        error: e,
    })?;
    for child in read {
        let child = child.map_err(|e| CatalogError::Io {
            path: dir.to_path_buf(),
            error: e,
        })?;
        let path = child.path();
        if path.is_dir() {
            visit_dir(&path, origin, entries, packages)?;
        }
    }
    Ok(())
}

fn register_single_node_package(
    dir: &Path,
    origin: CatalogOrigin,
    entries: &mut HashMap<String, CatalogEntry>,
    packages: &mut HashMap<PathBuf, Package>,
) -> Result<(), CatalogError> {
    let entry = load_node_entry(dir, dir, origin)?;
    let node_type = entry.node_type.clone();
    let package_name = dir
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("package")
        .to_string();
    packages.insert(
        dir.to_path_buf(),
        Package {
            root: dir.to_path_buf(),
            name: package_name,
            node_types: vec![node_type.clone()],
            shared_rs: Vec::new(),
            package_deps: None,
            origin,
        },
    );
    entries.insert(node_type, entry);
    Ok(())
}

fn register_multi_node_package(
    dir: &Path,
    toml_path: &Path,
    origin: CatalogOrigin,
    entries: &mut HashMap<String, CatalogEntry>,
    packages: &mut HashMap<PathBuf, Package>,
) -> Result<(), CatalogError> {
    let raw = fs::read_to_string(toml_path).map_err(|e| CatalogError::Io {
        path: toml_path.to_path_buf(),
        error: e,
    })?;
    let parsed: PackageToml = toml::from_str(&raw).map_err(|e| CatalogError::Parse {
        path: toml_path.to_path_buf(),
        error: e.to_string(),
    })?;

    // Load each declared node subdirectory.
    let mut node_types: Vec<String> = Vec::with_capacity(parsed.package.nodes.len());
    for sub in &parsed.package.nodes {
        let node_dir = dir.join(sub);
        if !node_dir.join("metadata.json").is_file() {
            return Err(CatalogError::Parse {
                path: toml_path.to_path_buf(),
                error: format!(
                    "package declares node '{sub}' but '{}' has no metadata.json",
                    node_dir.display()
                ),
            });
        }
        let entry = load_node_entry(&node_dir, dir, origin)?;
        node_types.push(entry.node_type.clone());
        entries.insert(entry.node_type.clone(), entry);
    }

    // Shared .rs files at the package root.
    let mut shared_rs: Vec<PathBuf> = Vec::new();
    let read = fs::read_dir(dir).map_err(|e| CatalogError::Io {
        path: dir.to_path_buf(),
        error: e,
    })?;
    for child in read {
        let child = child.map_err(|e| CatalogError::Io {
            path: dir.to_path_buf(),
            error: e,
        })?;
        let path = child.path();
        if path.is_file() && path.extension().and_then(|e| e.to_str()) == Some("rs") {
            shared_rs.push(path);
        }
    }
    shared_rs.sort();

    packages.insert(
        dir.to_path_buf(),
        Package {
            root: dir.to_path_buf(),
            name: parsed.package.name,
            node_types,
            shared_rs,
            package_deps: Some(parsed.dependencies),
            origin,
        },
    );
    Ok(())
}

/// Load a single node's metadata + form field specs.
///
/// `node_dir` = directory containing the node's `metadata.json`,
/// `mod.rs`, and `deps.toml`. `package_key` = directory identifying
/// the node's package (same as `node_dir` for single-node
/// packages, the multi-node package root otherwise).
fn load_node_entry(
    node_dir: &Path,
    package_key: &Path,
    origin: CatalogOrigin,
) -> Result<CatalogEntry, CatalogError> {
    let meta_path = node_dir.join("metadata.json");
    let raw = fs::read_to_string(&meta_path).map_err(|e| CatalogError::Io {
        path: meta_path.clone(),
        error: e,
    })?;
    let metadata: NodeMetadata = serde_json::from_str(&raw).map_err(|e| CatalogError::Parse {
        path: meta_path.clone(),
        error: e.to_string(),
    })?;

    // form_field_specs_ref resolves relative to the package root
    // (NOT the node dir) so multi-node packages can share one file.
    // Default filename `form_field_specs.json`, looked up in the
    // node dir first (single-node case) then the package root
    // (multi-node case).
    let specs_ref = metadata
        .form_field_specs_ref
        .clone()
        .unwrap_or_else(|| "form_field_specs.json".to_string());
    let specs_candidates = [
        package_key.join(&specs_ref),
        node_dir.join(&specs_ref),
    ];
    let mut form_field_specs: Vec<FormFieldSpec> = Vec::new();
    for candidate in &specs_candidates {
        if candidate.is_file() {
            let raw = fs::read_to_string(candidate).map_err(|e| CatalogError::Io {
                path: candidate.clone(),
                error: e,
            })?;
            form_field_specs =
                serde_json::from_str(&raw).map_err(|e| CatalogError::Parse {
                    path: candidate.clone(),
                    error: e.to_string(),
                })?;
            break;
        }
    }

    Ok(CatalogEntry {
        node_type: metadata.node_type.clone(),
        metadata,
        source_dir: node_dir.to_path_buf(),
        origin,
        form_field_specs,
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
}

#[cfg(test)]
mod package_tests {
    use super::*;
    #[test]
    fn human_specs_loaded() {
        let cat = stdlib_catalog().unwrap();
        let q = cat.entry("HumanQuery").expect("HumanQuery missing");
        let t = cat.entry("HumanTrigger").expect("HumanTrigger missing");
        assert!(!q.form_field_specs.is_empty(), "HumanQuery specs empty");
        assert!(!t.form_field_specs.is_empty(), "HumanTrigger specs empty");
        assert_eq!(q.package_key, t.package_key, "both nodes should share package_key");
        let pkg = cat.package_of("HumanQuery").expect("pkg_of missing");
        assert_eq!(pkg.name, "human");
        assert_eq!(pkg.node_types.len(), 2);
        assert_eq!(pkg.shared_rs.len(), 1, "should have form_helpers.rs");
    }
}
