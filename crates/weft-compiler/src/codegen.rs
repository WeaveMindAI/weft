//! Codegen. Given an enriched+validated `ProjectDefinition` and the
//! project's `FsCatalog`, emit a cargo crate under
//! `.weft/target/build/` that cargo builds into the project's native
//! worker binary.
//!
//! Emitted layout:
//!
//! ```text
//! .weft/target/build/
//!   Cargo.toml          # base deps + per-package deps (package.toml)
//!                       # + per-node deps (deps.toml) for nodes
//!                       # actually referenced by this project.
//!   src/
//!     main.rs           # spawns weft-engine; fetches the
//!                       # ProjectDefinition per execution from the
//!                       # broker. NO project.json baked in.
//!     registry.rs       # NodeCatalog impl: pulls in one shim
//!                       # `pkg_<name>.rs` per referenced package,
//!                       # dispatches node_type -> struct.
//!     pkg_<name>.rs     # one per referenced package. #[path]-includes
//!                       # the package's shared .rs files at the top
//!                       # level, then each referenced node's mod.rs
//!                       # as a submodule. Nodes reach shared code via
//!                       # `use super::<shared_mod>;`.
//! ```
//!
//! Pruning: each package's shim is emitted only if at least one of
//! its nodes is referenced. Within a shim, only referenced node
//! subdirs are `#[path]`-included. Package-level shared .rs files
//! are always included in an emitted shim (they may be load-bearing
//! for the referenced nodes; dead-code analysis in the Rust compiler
//! prunes unused shared helpers).
//!
//! Per-execution project fetch: the worker no longer carries a
//! baked-in `ProjectDefinition`. Each `Execute` / `Resume` task
//! payload carries the `definition_hash` the user clicked Run
//! against; the worker fetches the matching definition from the
//! broker, caches by hash so repeated executions of the same shape
//! pay one round trip per shape, and runs the engine on that. A
//! pure config or topology edit therefore no longer regenerates any
//! source file in this crate, and the resulting docker image is a
//! cache hit on the project's binary tag.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use weft_catalog::FsCatalog;
use weft_core::ProjectDefinition;

use crate::error::{CompileError, CompileResult};

/// Emit the full cargo crate. Writes every file listed in the module
/// docstring. Returns the crate root (passed to `invoke_cargo`).
pub fn emit(
    project: &ProjectDefinition,
    project_root: &Path,
    target_root: &Path,
    catalog: &FsCatalog,
    crate_name: &str,
) -> CompileResult<PathBuf> {
    let crate_root = target_root.to_path_buf();
    let src_dir = crate_root.join("src");
    std::fs::create_dir_all(&src_dir).map_err(CompileError::Io)?;

    let weft_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent() // crates/
        .and_then(|p| p.parent()) // weft/
        .ok_or_else(|| CompileError::Build("cannot resolve weft workspace root".into()))?
        .to_path_buf();

    // Every CATALOG-resolved node type referenced by the project.
    // Runtime-internal built-ins (Passthrough, LoopIn, LoopOut, ...)
    // are excluded: the engine dispatches them inline, so they don't
    // need a catalog shim and the catalog doesn't carry them.
    let referenced = collect_node_types(project);

    // Group referenced nodes by their owning package. `package_key`
    // is the package root dir; codegen emits one `pkg_<name>/` cargo
    // crate per distinct package (so editing one node's mod.rs
    // recompiles only ITS package's .rlib + relinks the worker, not
    // every package).
    let packages = group_by_package(catalog, &referenced)?;

    write_worker_cargo_toml(&crate_root, &packages, crate_name)?;
    write_rust_toolchain(&crate_root, &weft_root)?;
    // The `ProjectDefinition` is NOT baked into the binary anymore;
    // workers fetch it from the broker at execution claim time keyed
    // by `definition_hash`. A pure-config or pure-topology edit
    // therefore no longer re-bakes any source file into the worker
    // crate, and the docker image hash stays cache-hit.
    write_package_crates(&crate_root, project_root, catalog, &packages)?;
    write_registry_rs(&src_dir, &packages)?;
    write_main_rs(&src_dir)?;

    Ok(crate_root)
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
) -> CompileResult<Vec<PackageEmit<'a>>> {
    let mut by_key: BTreeMap<std::path::PathBuf, Vec<String>> = BTreeMap::new();
    for node_type in referenced {
        let Some(pkg) = catalog.package_of(node_type) else {
            return Err(CompileError::Build(format!(
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
    let lowered = raw.to_ascii_lowercase();
    let mut out = String::with_capacity(lowered.len() + 4);
    out.push_str("pkg_");
    for ch in lowered.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    out
}

/// The set of CATALOG node types this project references, sorted.
/// Runtime-internal built-in node types (Passthrough, LoopIn,
/// LoopOut, ...) are excluded: they live in the engine, not the
/// catalog, and the codegen / hash / docker passes that consume this
/// set would otherwise fail to resolve them. Exposed so callers
/// outside codegen (e.g. Dockerfile emission) walk the same set
/// without re-implementing the filter.
pub fn collect_node_types(project: &ProjectDefinition) -> BTreeSet<String> {
    project
        .nodes
        .iter()
        .map(|n| n.node_type.clone())
        .filter(|t| !crate::weft_compiler::is_reserved_type_keyword(t))
        .collect()
}

/// Emit the worker binary's `Cargo.toml`. Deps are the engine
/// surface (engine + core + broker-client + platform-traits), the
/// runtime essentials (tokio, serde, async-trait, anyhow, clap,
/// tracing, uuid), and one path dep per referenced package crate
/// under `pkg_<name>/`. Per-package and per-node cargo deps live
/// inside each package crate's own `Cargo.toml`, NOT here: a node
/// that pulls in pyo3 stays a recompile-target of its OWN crate,
/// while the worker keeps cache-hitting on every other build.
fn write_worker_cargo_toml(
    crate_root: &Path,
    packages: &[PackageEmit<'_>],
    crate_name: &str,
) -> CompileResult<()> {
    let package_name = crate::build::sanitize_crate_name(crate_name);

    let mut deps = base_runtime_deps();
    insert_dep(
        &mut deps,
        "weft-engine",
        toml::Value::Table(path_table("../weft/crates/weft-engine")),
    );
    insert_dep(
        &mut deps,
        "weft-core",
        toml::Value::Table(path_table("../weft/crates/weft-core")),
    );
    insert_dep(
        &mut deps,
        "weft-broker-client",
        toml::Value::Table(path_table("../weft/crates/weft-broker-client")),
    );
    insert_dep(
        &mut deps,
        "weft-platform-traits",
        toml::Value::Table(path_table("../weft/crates/weft-platform-traits")),
    );
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
    // One path dep per referenced package crate.
    for pkg in packages {
        insert_dep(
            &mut deps,
            &pkg.module_ident,
            toml::Value::Table(path_table(&format!("./{}", pkg.module_ident))),
        );
    }

    let mut deps_fragment = String::new();
    for (name, value) in deps {
        deps_fragment.push_str(&format!("{name} = {}\n", toml_inline(&value)));
    }

    let contents = format!(
        r#"# Emitted by weft codegen. Do not edit by hand; regenerated on
# every `weft build`.

[package]
name = "{name}"
version = "0.1.0"
edition = "2021"

[[bin]]
name = "{name}"
path = "src/main.rs"

[dependencies]
{deps}"#,
        name = package_name,
        deps = deps_fragment,
    );
    std::fs::write(crate_root.join("Cargo.toml"), contents).map_err(CompileError::Io)?;
    Ok(())
}

/// Emit one cargo crate per referenced node package. Each lives at
/// `<crate_root>/pkg_<name>/`. The package crate's `lib.rs` reuses
/// the original `pkg_<name>.rs` shim shape (`#[path]`-includes of
/// shared `.rs` files and each referenced node's `mod.rs`); the
/// difference is that it now compiles to a standalone `.rlib`, so
/// editing one node's source rebuilds ONLY this crate plus the
/// worker's link step, leaving sibling package crates cache-hit.
///
/// Per-package and per-node cargo deps land here, not on the worker.
fn write_package_crates(
    crate_root: &Path,
    project_root: &Path,
    catalog: &FsCatalog,
    packages: &[PackageEmit<'_>],
) -> CompileResult<()> {
    let nodes_root = project_root.join("nodes");
    for pkg in packages {
        let pkg_dir = crate_root.join(&pkg.module_ident);
        let pkg_src = pkg_dir.join("src");
        std::fs::create_dir_all(&pkg_src).map_err(CompileError::Io)?;
        write_package_cargo_toml(&pkg_dir, catalog, pkg)?;
        write_package_lib_rs(&pkg_src, &nodes_root, catalog, pkg)?;
    }
    Ok(())
}

fn write_package_cargo_toml(
    pkg_dir: &Path,
    catalog: &FsCatalog,
    pkg: &PackageEmit<'_>,
) -> CompileResult<()> {
    // Runtime essentials shared with the worker, plus the workspace
    // surface every node body uses. The relative path
    // (`../../weft/...`) is one level deeper than the worker's
    // (`../weft/...`) because we live under `pkg_<name>/`.
    let mut deps = base_runtime_deps();
    insert_dep(
        &mut deps,
        "weft-core",
        toml::Value::Table(path_table("../../weft/crates/weft-core")),
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
            .map_err(|e| CompileError::Build(format!("deps for '{node_type}': {e}")))?;
        let Some(node_deps) = node_deps else { continue };
        for (name, value) in node_deps.dependencies {
            add_dep(&mut deps, &pkg.package.name, name, value, &format!("node {node_type}"))?;
        }
    }

    let mut deps_fragment = String::new();
    for (name, value) in deps {
        deps_fragment.push_str(&format!("{name} = {}\n", toml_inline(&value)));
    }

    let contents = format!(
        r#"# Emitted by weft codegen. Per-package crate for `{pkg_name}`.
# Do not edit by hand; regenerated on every `weft build`.

[package]
name = "{ident}"
version = "0.1.0"
edition = "2021"

[lib]
path = "src/lib.rs"

[dependencies]
{deps}"#,
        pkg_name = pkg.package.name,
        ident = pkg.module_ident,
        deps = deps_fragment,
    );
    std::fs::write(pkg_dir.join("Cargo.toml"), contents).map_err(CompileError::Io)?;
    Ok(())
}

/// Per-package `lib.rs`: re-exports each referenced node's module
/// under a snake_case identifier, after pulling in any
/// shared-package `.rs` files. Same shape as the old `pkg_*.rs`
/// shim that lived in the worker crate; the only difference is it
/// compiles to a standalone `.rlib`.
fn write_package_lib_rs(
    pkg_src: &Path,
    nodes_root: &Path,
    catalog: &FsCatalog,
    pkg: &PackageEmit<'_>,
) -> CompileResult<()> {
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
                CompileError::Build(format!(
                    "package {} has shared file with no stem: {}",
                    pkg.package.name,
                    shared_path.display()
                ))
            })?;
        let rel = shared_path.strip_prefix(nodes_root).map_err(|_| {
            CompileError::Build(format!(
                "shared file {} is not under project nodes root {}",
                shared_path.display(),
                nodes_root.display()
            ))
        })?;
        let in_container = format!(
            "{}/{}",
            crate::worker_image::NODES_MOUNT,
            rel.display().to_string().replace('\\', "/"),
        );
        body.push_str(&format!(
            "#[path = \"{in_container}\"]\npub mod {mod_name};\n\n"
        ));
    }
    for node_type in &pkg.node_types {
        let entry = catalog.entry(node_type).expect("collected from catalog");
        let mod_rs = entry.source_dir.join("mod.rs");
        let rel = mod_rs.strip_prefix(nodes_root).map_err(|_| {
            CompileError::Build(format!(
                "node source {} is not under project nodes root {}",
                mod_rs.display(),
                nodes_root.display()
            ))
        })?;
        let in_container = format!(
            "{}/{}",
            crate::worker_image::NODES_MOUNT,
            rel.display().to_string().replace('\\', "/"),
        );
        let mod_name = ident_for_node_type(node_type);
        body.push_str(&format!(
            "#[path = \"{in_container}\"]\npub mod {mod_name};\n\n"
        ));
    }
    std::fs::write(pkg_src.join("lib.rs"), body).map_err(CompileError::Io)?;
    Ok(())
}

/// Propagate the workspace's pinned toolchain into the generated
/// worker crate. The worker builds in its own crate dir (`/work` in
/// the image), which is a sibling of the weft mount, so `cargo build`
/// there wouldn't otherwise find the root `rust-toolchain.toml`.
/// Copying it in keeps the root file the single source of truth, so the
/// worker compiles on the exact toolchain the rest of the system uses.
/// Fails loud if the root pin is missing: a worker built on the wrong
/// toolchain would fail confusingly deep in the node compile instead.
fn write_rust_toolchain(crate_root: &Path, weft_root: &Path) -> Result<(), CompileError> {
    let pin = std::fs::read_to_string(weft_root.join("rust-toolchain.toml"))
        .map_err(|e| CompileError::Build(format!("read rust-toolchain.toml: {e}")))?;
    std::fs::write(crate_root.join("rust-toolchain.toml"), pin).map_err(CompileError::Io)?;
    Ok(())
}

/// Render a `toml::Value` as an inline TOML literal suitable for
/// embedding on the right-hand side of a `key = VALUE` line inside
/// `[dependencies]`. Tables become `{ k = v, ... }`.
fn toml_inline(v: &toml::Value) -> String {
    match v {
        toml::Value::String(s) => format!("\"{}\"", escape_toml_str(s)),
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

fn escape_toml_str(s: &str) -> String {
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
) -> CompileResult<()> {
    match deps.remove(&name) {
        Some(existing) => {
            let merged = merge_dep_specs(&existing, &value).map_err(|conflict| {
                CompileError::Build(format!(
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

fn write_registry_rs(
    src_dir: &Path,
    packages: &[PackageEmit<'_>],
) -> CompileResult<()> {
    let mut body = String::new();
    body.push_str("//! Per-project NodeCatalog. Dispatches node_type strings to\n");
    body.push_str("//! the node structs in each `pkg_<name>` crate. Each package\n");
    body.push_str("//! is its own cargo crate, so editing one node's source\n");
    body.push_str("//! recompiles only that crate; the registry below relinks.\n\n");
    body.push_str("use weft_core::node::{FormFieldSpec, Node, NodeCatalog};\n\n");
    body.push_str("pub struct ProjectCatalog;\n\n");
    body.push_str("impl NodeCatalog for ProjectCatalog {\n");
    body.push_str("    fn lookup(&self, node_type: &str) -> Option<&'static dyn Node> {\n");
    body.push_str("        match node_type {\n");
    for pkg in packages {
        for node_type in &pkg.node_types {
            let struct_mod = ident_for_node_type(node_type);
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
    body.push_str("        vec![");
    for pkg in packages {
        for node_type in &pkg.node_types {
            body.push_str(&format!("\"{node_type}\", "));
        }
    }
    body.push_str("]\n    }\n");
    body.push_str("    fn form_field_specs(&self, _node_type: &str) -> &[FormFieldSpec] {\n");
    body.push_str("        // Form field specs are a compile-time concern (used by\n");
    body.push_str("        // enrich to materialize ports); the runtime catalog\n");
    body.push_str("        // doesn't need them.\n");
    body.push_str("        &[]\n    }\n}\n\n");
    body.push_str("pub static PROJECT_CATALOG: ProjectCatalog = ProjectCatalog;\n");

    std::fs::write(src_dir.join("registry.rs"), body).map_err(CompileError::Io)?;
    Ok(())
}

/// Convert a PascalCase node type (e.g. `ApiPost`, `HumanQuery`,
/// `LlmInference`) into a snake_case Rust module identifier
/// (`api_post`, `human_query`, `llm_inference`). Rules: lowercase
/// the first char; insert `_` before every subsequent uppercase.
fn ident_for_node_type(node_type: &str) -> String {
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

// Each package crate is an EXTERNAL cargo dep (path = "./pkg_<name>"
// in Cargo.toml), so the worker binary just uses them as crates and
// main.rs needs no package knowledge (no `mod pkg_<name>;`).
fn write_main_rs(src_dir: &Path) -> CompileResult<()> {
    let contents = format!(
        r#"//! Project worker binary. Spawned by the dispatcher as part of
//! a per-project pool. Claims `target=worker` tasks for its own
//! `project_id` and runs each as a tokio task in-process. Idle-shuts
//! itself down after a grace window with no pending work.

use std::sync::Arc;

use clap::Parser;

use weft_broker_client::{{
    BrokerInfraClient, BrokerInfraStateClient, BrokerJournalClient, BrokerProjectClient,
    BrokerTaskStoreClient, BrokerWorkerPodClient, TokenSource,
}};
use weft_core::NodeCatalog;
use weft_engine::EngineClients;

mod registry;

#[derive(Debug, Parser)]
#[command(name = "weft-project-worker", version)]
struct Args {{
    /// Project id this Pod serves. Worker only claims tasks scoped to
    /// this project.
    #[arg(long, env = "WEFT_PROJECT_ID")]
    project_id: String,

    /// Broker base URL. The worker never touches Postgres directly;
    /// every journal write, task enqueue/claim, worker_pod heartbeat,
    /// and infra read flows through the broker, which validates the
    /// projected SA token at WEFT_BROKER_TOKEN_PATH and runs a
    /// per-tenant scope check.
    #[arg(long, env = "WEFT_BROKER_URL")]
    broker_url: String,

    /// Filesystem path to the kubelet-projected SA token. The broker
    /// validates this token via TokenReview on every call.
    #[arg(long, env = "WEFT_BROKER_TOKEN_PATH", default_value = "/var/run/weft/sa/token")]
    broker_token_path: String,

    /// k8s Pod name (injected via downward API). Stamped on every
    /// journal write; the fencing trigger uses it to detect zombies.
    #[arg(long, env = "WEFT_POD_NAME")]
    pod_name: String,

    /// k8s namespace this Pod runs in. Recorded on the worker_pod
    /// row so the dispatcher's reaper can `kubectl delete` against
    /// the right namespace.
    #[arg(long, env = "WEFT_NAMESPACE")]
    namespace: String,

    /// Identifier of the dispatcher Pod that spawned us. Recorded on
    /// the worker_pod row for ops traceability.
    #[arg(long, env = "WEFT_OWNER_DISPATCHER", default_value = "unknown")]
    owner_dispatcher: String,

    /// Tenant this worker belongs to. Stamped on every task this
    /// worker enqueues so the dispatcher's listener reaper can tell
    /// "this tenant has work mid-flight" from "this listener is
    /// genuinely idle." Without it, the reaper races register flows.
    #[arg(long, env = "WEFT_TENANT_ID")]
    tenant_id: String,
}}

#[tokio::main]
async fn main() -> anyhow::Result<()> {{
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "weft_engine=info,weft_core=info".into()),
        )
        .init();

    let args = Args::parse();

    let token = TokenSource::new(std::path::PathBuf::from(&args.broker_token_path));
    let journal = BrokerJournalClient::new(args.broker_url.clone(), token.clone());
    let tasks = BrokerTaskStoreClient::new(args.broker_url.clone(), token.clone());
    let worker_pods = BrokerWorkerPodClient::new(args.broker_url.clone(), token.clone());
    let infra = BrokerInfraClient::new(args.broker_url.clone(), token.clone());
    let infra_state = BrokerInfraStateClient::new(args.broker_url.clone(), token.clone());
    let project = BrokerProjectClient::new(args.broker_url.clone(), token.clone());

    // The Broker*Client constructors already return `Arc<Self>`, so
    // pass them straight into the `Arc<dyn _>` fields (re-wrapping
    // would make `Arc<Arc<_>>`, which doesn't implement the trait).
    // `SystemClock` is bare, so it still needs the `Arc::new`.
    let storage = weft_engine::WorkerStorage::new(
        tasks.clone(),
        args.tenant_id.clone(),
        std::path::PathBuf::from(&args.broker_token_path),
    );
    let clients = EngineClients {{
        journal,
        tasks,
        infra,
        infra_state,
        project,
        clock: std::sync::Arc::new(weft_platform_traits::clock::SystemClock),
        storage,
    }};

    let catalog = Arc::new(CatalogRef) as Arc<dyn NodeCatalog>;

    weft_engine::run_pod(
        catalog,
        clients,
        worker_pods,
        args.pod_name,
        args.project_id,
        args.tenant_id,
        args.namespace,
    )
    .await?;

    tracing::info!(target: "weft_project_worker", "pod exit");
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
    fn form_field_specs(&self, node_type: &str) -> &[weft_core::node::FormFieldSpec] {{
        registry::PROJECT_CATALOG.form_field_specs(node_type)
    }}
}}
"#,
    );
    std::fs::write(src_dir.join("main.rs"), contents).map_err(CompileError::Io)?;
    Ok(())
}


#[cfg(test)]
mod tests {
    use super::collect_node_types;
    use weft_core::project::{NodeDefinition, ProjectDefinition, Position};
    use weft_core::NodeFeatures;

    fn make_node(id: &str, node_type: &str) -> NodeDefinition {
        NodeDefinition {
            id: id.into(),
            node_type: node_type.into(),
            label: None,
            config: serde_json::Value::Object(Default::default()),
            position: Position { x: 0.0, y: 0.0 },
            inputs: Vec::new(),
            outputs: Vec::new(),
            features: NodeFeatures::default(),
            scope: Vec::new(),
            group_boundary: None,
            requires_infra: false,
            images: Vec::new(),
            span: None,
            header_span: None,
            config_spans: Default::default(),
            file_refs: Default::default(),
            include_path: None,
        }
    }

    /// Regression: codegen's `collect_node_types` used to return every
    /// node's `node_type` verbatim, including runtime-internal built-ins
    /// (Passthrough, LoopIn, LoopOut). Downstream callers (worker-image
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
                make_node("my_group__in", "Passthrough"),
                make_node("my_group__out", "Passthrough"),
            ],
            edges: Vec::new(),
            groups: Vec::new(),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        let types = collect_node_types(&project);
        assert!(types.contains("Text"), "user catalog type included: {types:?}");
        assert!(!types.contains("LoopIn"), "LoopIn excluded: {types:?}");
        assert!(!types.contains("LoopOut"), "LoopOut excluded: {types:?}");
        assert!(!types.contains("Passthrough"), "Passthrough excluded: {types:?}");
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

    #[test]
    fn add_dep_still_errors_on_conflict() {
        let mut deps: BTreeMap<String, toml::Value> = BTreeMap::new();
        deps.insert("tokio".into(), tbl(r#""1""#));
        let err = add_dep(&mut deps, "pkg", "tokio".into(), tbl(r#""2""#), "node Bar")
            .expect_err("version conflict is a hard error");
        assert!(matches!(err, super::CompileError::Build(_)));
    }
}
