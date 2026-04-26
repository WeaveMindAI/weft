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
//!     main.rs           # spawns weft-engine with the static
//!                       # project + catalog.
//!     project.rs        # include_str! + OnceLock<ProjectDefinition>.
//!     project.json      # serialized ProjectDefinition.
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

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use weft_catalog::FsCatalog;
use weft_core::ProjectDefinition;

use crate::error::{CompileError, CompileResult};

/// Emit the full cargo crate. Writes every file listed in the module
/// docstring. Returns the crate root (passed to `invoke_cargo`).
pub fn emit(
    project: &ProjectDefinition,
    _project_root: &Path,
    target_root: &Path,
    catalog: &FsCatalog,
) -> CompileResult<PathBuf> {
    let crate_root = target_root.to_path_buf();
    let src_dir = crate_root.join("src");
    std::fs::create_dir_all(&src_dir).map_err(CompileError::Io)?;

    let weft_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent() // crates/
        .and_then(|p| p.parent()) // weft/
        .ok_or_else(|| CompileError::Build("cannot resolve weft workspace root".into()))?
        .to_path_buf();

    // Every node type referenced by the project. Passthrough IS
    // included because it's a real catalog node (the compiler emits
    // it, but the runtime runs it like any other).
    let referenced = collect_node_types(project);

    // Group referenced nodes by their owning package. `package_key`
    // is the package root dir; codegen emits one `pkg_<name>.rs`
    // shim per distinct package.
    let packages = group_by_package(catalog, &referenced)?;

    write_cargo_toml(&crate_root, project, &weft_root, catalog, &referenced, &packages)?;
    write_project_json(&src_dir, project)?;
    write_project_rs(&src_dir)?;
    write_package_shims(&src_dir, catalog, &packages)?;
    write_registry_rs(&src_dir, catalog, &packages)?;
    write_main_rs(&src_dir, &packages)?;

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
    use std::collections::BTreeMap;
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

/// The set of node types this project references, sorted. Exposed
/// so callers outside codegen (e.g. Dockerfile emission) can walk
/// the same set without re-implementing the scan.
pub fn collect_node_types(project: &ProjectDefinition) -> BTreeSet<String> {
    project.nodes.iter().map(|n| n.node_type.clone()).collect()
}

fn write_cargo_toml(
    crate_root: &Path,
    project: &ProjectDefinition,
    weft_root: &Path,
    catalog: &FsCatalog,
    referenced: &BTreeSet<String>,
    packages: &[PackageEmit<'_>],
) -> CompileResult<()> {
    let package_name = sanitize_crate_name(&project.name);

    // Inside the builder container, the weft workspace crates live
    // at `/weft/crates/*` and the generated project crate lives at
    // `/work/`. Relative path from the project crate's Cargo.toml
    // back to weft's crates is `../weft/crates/<name>`. This also
    // holds on the host: the docker build context is assembled to
    // match the same layout so `cargo check` from the IDE resolves.
    //
    // `weft_root` is captured at host time but not embedded in the
    // generated Cargo.toml (the container doesn't know the host
    // path). Only the in-container relative path matters.
    let _ = weft_root;
    let mut merged: BTreeMap<String, toml::Value> = BTreeMap::new();
    insert_dep(
        &mut merged,
        "weft-engine",
        toml::Value::Table(path_table("../weft/crates/weft-engine")),
    );
    insert_dep(
        &mut merged,
        "weft-core",
        toml::Value::Table(path_table("../weft/crates/weft-core")),
    );
    insert_dep(
        &mut merged,
        "tokio",
        toml::Value::Table(version_with_features("1", &["full"])),
    );
    insert_dep(
        &mut merged,
        "serde_json",
        toml::Value::String("1".into()),
    );
    insert_dep(
        &mut merged,
        "serde",
        toml::Value::Table(version_with_features("1", &["derive"])),
    );
    insert_dep(
        &mut merged,
        "async-trait",
        toml::Value::String("0.1".into()),
    );
    insert_dep(&mut merged, "anyhow", toml::Value::String("1".into()));
    insert_dep(
        &mut merged,
        "clap",
        toml::Value::Table(version_with_features("4", &["derive", "env"])),
    );
    insert_dep(&mut merged, "tracing", toml::Value::String("0.1".into()));
    insert_dep(
        &mut merged,
        "tracing-subscriber",
        toml::Value::Table(version_with_features("0.3", &["env-filter"])),
    );
    insert_dep(
        &mut merged,
        "uuid",
        toml::Value::Table(version_with_features("1", &["v4", "serde"])),
    );

    // Package-level deps from every referenced package's
    // `package.toml` + node-level deps from each referenced node's
    // `deps.toml`. Merge into the base deps. Duplicates follow
    // "later wins" with a warning if the specs diverge; cargo
    // enforces final compatibility.
    let mut add_dep = |name: String, value: toml::Value, source: &str| {
        match merged.get(&name) {
            Some(existing) if existing != &value => {
                tracing::warn!(
                    target: "weft_compiler::codegen",
                    "dependency '{name}' declared twice with differing specs; keeping newest ({source}). \
                     Cargo will enforce final version compatibility."
                );
                merged.insert(name, value);
            }
            _ => {
                merged.insert(name, value);
            }
        }
    };
    for pkg in packages {
        if let Some(deps) = &pkg.package.package_deps {
            for (name, value) in deps.iter() {
                add_dep(
                    name.clone(),
                    value.clone(),
                    &format!("package {}", pkg.package.name),
                );
            }
        }
    }
    for node_type in referenced {
        let deps = catalog
            .deps(node_type)
            .map_err(|e| CompileError::Build(format!("deps for '{node_type}': {e}")))?;
        let Some(deps) = deps else { continue };
        for (name, value) in deps.dependencies {
            add_dep(name, value, &format!("node {node_type}"));
        }
    }

    // Serialize the merged deps. For each key: if the value is a
    // table, emit `key = { ... inline ... }` (that's how cargo wants
    // inline dependency specs). If it's a string, emit `key = "..."`.
    let mut deps_fragment = String::new();
    for (name, value) in merged {
        let line = match &value {
            toml::Value::String(_) => {
                format!("{name} = {}", toml_inline(&value))
            }
            toml::Value::Table(_) => {
                format!("{name} = {}", toml_inline(&value))
            }
            other => format!("{name} = {}", toml_inline(other)),
        };
        deps_fragment.push_str(&line);
        deps_fragment.push('\n');
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

fn sanitize_crate_name(raw: &str) -> String {
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
    out
}

fn write_project_json(src_dir: &Path, project: &ProjectDefinition) -> CompileResult<()> {
    let json = serde_json::to_string_pretty(project)
        .map_err(|e| CompileError::Build(format!("serialize project: {e}")))?;
    std::fs::write(src_dir.join("project.json"), json).map_err(CompileError::Io)?;
    Ok(())
}

fn write_project_rs(src_dir: &Path) -> CompileResult<()> {
    let contents = r#"//! Project shape loaded from the JSON emitted by codegen. Parsed
//! once per worker invocation via `OnceLock`; subsequent access is a
//! plain pointer deref.

use std::sync::OnceLock;

use weft_core::ProjectDefinition;

static PROJECT_JSON: &str = include_str!("project.json");

static PROJECT: OnceLock<ProjectDefinition> = OnceLock::new();

/// Return the statically embedded project definition. Parses the JSON
/// on first access; subsequent calls return the cached value.
pub fn project() -> &'static ProjectDefinition {
    PROJECT.get_or_init(|| {
        serde_json::from_str(PROJECT_JSON)
            .expect("BUG: emitted project.json is not valid ProjectDefinition")
    })
}
"#;
    std::fs::write(src_dir.join("project.rs"), contents).map_err(CompileError::Io)?;
    Ok(())
}

/// One shim file per referenced package. `#[path]` includes point
/// at paths INSIDE the builder container: the build step mounts
/// the project's referenced catalog subdirectories at `/catalog/`,
/// so a shim for the `basic` package reaches its debug node at
/// `/catalog/basic/debug/mod.rs`.
///
/// Nodes reach their shared helpers via plain `use super::<shared>;`
/// because the shim wraps every node and shared file under one
/// Rust module (one module per package).
fn write_package_shims(
    src_dir: &Path,
    catalog: &FsCatalog,
    packages: &[PackageEmit<'_>],
) -> CompileResult<()> {
    // Anchor every node's `#[path]` at the catalog root. The
    // docker build copies `catalog/` verbatim to `/weft/catalog`
    // in the builder container, so a node living at
    // `catalog/basic/debug/mod.rs` resolves to
    // `/weft/catalog/basic/debug/mod.rs` via the same relative
    // suffix.
    let catalog_root = weft_catalog::stdlib_root();
    for pkg in packages {
        let mut body = String::new();
        body.push_str(&format!(
            "//! Package shim for `{}`. Emitted by codegen; do not edit.\n\n",
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
            let rel = shared_path.strip_prefix(&catalog_root).map_err(|_| {
                CompileError::Build(format!(
                    "shared file {} is not under catalog root {}",
                    shared_path.display(),
                    catalog_root.display()
                ))
            })?;
            let in_container = format!(
                "{}/{}",
                crate::worker_image::CATALOG_MOUNT,
                rel.display().to_string().replace('\\', "/"),
            );
            body.push_str(&format!(
                "#[path = \"{in_container}\"]\npub mod {mod_name};\n\n"
            ));
        }
        for node_type in &pkg.node_types {
            let entry = catalog.entry(node_type).expect("collected from catalog");
            let mod_rs = entry.source_dir.join("mod.rs");
            let rel = mod_rs.strip_prefix(&catalog_root).map_err(|_| {
                CompileError::Build(format!(
                    "node source {} is not under catalog root {}",
                    mod_rs.display(),
                    catalog_root.display()
                ))
            })?;
            let in_container = format!(
                "{}/{}",
                crate::worker_image::CATALOG_MOUNT,
                rel.display().to_string().replace('\\', "/"),
            );
            let mod_name = ident_for_node_type(node_type);
            body.push_str(&format!(
                "#[path = \"{in_container}\"]\npub mod {mod_name};\n\n"
            ));
        }
        let file = src_dir.join(format!("{}.rs", pkg.module_ident));
        std::fs::write(&file, body).map_err(CompileError::Io)?;
    }
    Ok(())
}

fn write_registry_rs(
    src_dir: &Path,
    catalog: &FsCatalog,
    packages: &[PackageEmit<'_>],
) -> CompileResult<()> {
    let mut body = String::new();
    body.push_str("//! Per-project NodeCatalog. Dispatches node_type strings to\n");
    body.push_str("//! the node structs compiled in through each package shim\n");
    body.push_str("//! (`pkg_*` modules declared in main.rs).\n\n");
    body.push_str("use weft_core::node::{FormFieldSpec, Node, NodeCatalog};\n\n");
    body.push_str("pub struct ProjectCatalog;\n\n");
    body.push_str("impl NodeCatalog for ProjectCatalog {\n");
    body.push_str("    fn lookup(&self, node_type: &str) -> Option<&'static dyn Node> {\n");
    body.push_str("        match node_type {\n");
    for pkg in packages {
        for node_type in &pkg.node_types {
            let struct_mod = ident_for_node_type(node_type);
            body.push_str(&format!(
                "            \"{nt}\" => Some(&crate::{pkg_mod}::{struct_mod}::{nt}Node as &'static dyn Node),\n",
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

    let _ = catalog;
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

fn write_main_rs(src_dir: &Path, packages: &[PackageEmit<'_>]) -> CompileResult<()> {
    let mut pkg_mods = String::new();
    for pkg in packages {
        pkg_mods.push_str(&format!("mod {};\n", pkg.module_ident));
    }
    let contents = format!(
        r#"//! Project worker binary. Spawned by the dispatcher per execution.
//!
//! Connects to `${{dispatcher}}/ws/executions/{{color}}` over WebSocket,
//! handshakes, receives the Start packet (wake + optional snapshot +
//! queued deliveries), drives the pulse loop, reports terminal state
//! (or stalls with a snapshot) over the same socket.

use std::sync::Arc;

use anyhow::Context;
use clap::Parser;
use tokio::sync::Notify;

use weft_core::NodeCatalog;

mod project;
mod registry;
{pkg_mods}
#[derive(Debug, Parser)]
#[command(name = "weft-project-worker", version)]
struct Args {{
    /// Color for this execution (mandatory in Slice 3+; dispatcher
    /// always provides it).
    #[arg(long)]
    color: String,

    /// Dispatcher base URL. The worker upgrades `${{dispatcher}}` to
    /// `ws://` or `wss://` and connects to `/ws/executions/{{color}}`.
    #[arg(long, env = "WEFT_DISPATCHER_URL")]
    dispatcher: String,
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
    let color: uuid::Uuid = args.color.parse().context("color uuid")?;

    let project = project::project().clone();
    let catalog = Arc::new(CatalogRef) as Arc<dyn NodeCatalog>;
    let cancellation = Arc::new(Notify::new());

    let outcome = weft_engine::run_with_link(
        project,
        catalog,
        color,
        &args.dispatcher,
        cancellation,
    )
    .await?;

    match outcome {{
        weft_engine::LoopOutcome::Completed {{ outputs }} => {{
            tracing::info!(target: "weft_project_worker", "completed: {{outputs}}");
        }}
        weft_engine::LoopOutcome::Failed {{ error }} => {{
            tracing::error!(target: "weft_project_worker", "failed: {{error}}");
        }}
        weft_engine::LoopOutcome::Stalled => {{
            tracing::info!(target: "weft_project_worker", "stalled: snapshot shipped");
        }}
        weft_engine::LoopOutcome::Stuck => {{
            tracing::warn!(target: "weft_project_worker", "stuck: pending pulses with no ready nodes");
        }}
    }}

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
        pkg_mods = pkg_mods,
    );
    std::fs::write(src_dir.join("main.rs"), contents).map_err(CompileError::Io)?;
    Ok(())
}

