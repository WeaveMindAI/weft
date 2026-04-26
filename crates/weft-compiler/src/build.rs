//! Compile pipeline orchestration.
//!
//! Given a project root (containing `weft.toml` + `main.weft`),
//! parse + enrich + validate + codegen the generated cargo crate
//! to `.weft/target/build/`, then emit the multi-stage
//! Dockerfile + stage the docker build context. The actual
//! `cargo build` runs INSIDE that docker build, not on the host.
//! The host only needs docker.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use crate::codegen;
use crate::enrich::enrich;
use crate::error::{CompileError, CompileResult};
use crate::project::Project;
use crate::validate::validate;
use crate::weft_compiler::compile;
use crate::worker_image;
use crate::Severity;
use weft_catalog::{CatalogOrigin, CatalogSource, FsCatalog, stdlib_root};

/// Build-phase artifact. The host never holds a compiled binary
/// (cargo runs inside the builder container). What we return is
/// the staged docker build context + metadata the CLI uses to
/// call `docker build` and surface package info to the user.
pub struct BuildResult {
    /// Absolute path to the docker build context the CLI should
    /// feed to `docker build`. Contains `Dockerfile`, `build/`
    /// (the generated cargo crate), and `weft/` (a hardlinked or
    /// copied copy of the weft workspace).
    pub build_context: PathBuf,
    /// Path to the generated Dockerfile inside the build context.
    /// Convenience for CLI callers that pass `-f` explicitly.
    pub dockerfile: PathBuf,
    /// Node types this project references, for CLI diagnostics.
    pub referenced_nodes: BTreeSet<String>,
    /// Summary of the generated Dockerfile for user-facing logs.
    pub dockerfile_summary: worker_image::WorkerDockerfile,
    /// Sanitized crate name; the CLI uses this to tag the worker
    /// image and to know what binary path the Dockerfile produces.
    pub binary_name: String,
}

/// Full host-side compile + stage. Pipeline:
///
/// 1. Parse + enrich + validate the weft source.
/// 2. Codegen the cargo crate at `.weft/target/build/`.
/// 3. Emit the multi-stage Dockerfile at
///    `.weft/target/Dockerfile.worker`.
/// 4. Stage the docker build context at
///    `.weft/target/worker-image/` with `build/`, `weft/`, and
///    the Dockerfile laid out so `docker build .` works.
///
/// `_release` is currently unused because cargo invocation moved
/// into the builder image, which always builds release. Kept on
/// the signature so future debug/release mode selection doesn't
/// require another plumbing pass.
pub fn build_project(project_root: &Path, _release: bool) -> CompileResult<BuildResult> {
    let project = Project::load(project_root)?;
    let source = project.read_main_weft()?;

    let mut definition = compile(&source, project.id()).map_err(|errors| {
        let msg = errors
            .iter()
            .map(|e| format!("{}: {}", e.line, e.message))
            .collect::<Vec<_>>()
            .join("\n");
        CompileError::Parse(msg)
    })?;
    definition.name = project.manifest.package.name.clone();

    let catalog = build_project_catalog(&project.root)?;
    enrich(&mut definition, &catalog)?;

    let diagnostics = validate(&definition, &catalog);
    let errors: Vec<_> = diagnostics
        .iter()
        .filter(|d| matches!(d.severity, Severity::Error))
        .collect();
    if !errors.is_empty() {
        let msg = errors
            .iter()
            .map(|d| format!("{}:{} {}", d.line, d.column, d.message))
            .collect::<Vec<_>>()
            .join("\n");
        return Err(CompileError::Validate(msg));
    }

    let crate_root = project_root.join(".weft").join("target").join("build");
    let referenced_nodes = codegen::collect_node_types(&definition);
    codegen::emit(&definition, project_root, &crate_root, &catalog)?;
    let binary_name = sanitize_crate_name(&definition.name);

    let dockerfile_summary = worker_image::emit(
        &project.manifest.build.worker,
        project_root,
        &catalog,
        &referenced_nodes,
        &binary_name,
    )?;
    let dockerfile_path = project_root.join(".weft/target/Dockerfile.worker");
    if let Some(parent) = dockerfile_path.parent() {
        std::fs::create_dir_all(parent).map_err(CompileError::Io)?;
    }
    std::fs::write(&dockerfile_path, &dockerfile_summary.body).map_err(CompileError::Io)?;

    let weft_root = resolve_weft_root()?;
    let build_context = stage_build_context(project_root, &crate_root, &weft_root, &dockerfile_path)?;

    Ok(BuildResult {
        build_context,
        dockerfile: dockerfile_path,
        referenced_nodes,
        dockerfile_summary,
        binary_name,
    })
}

/// Stage the docker build context under
/// `.weft/target/worker-image/`. Layout:
///
/// ```text
/// worker-image/
///   Dockerfile           (copy of Dockerfile.worker)
///   build/               (the generated cargo crate)
///   weft/                (the weft workspace: crates, catalog, Cargo.toml, Cargo.lock)
/// ```
///
/// The weft copy is minimal: only the directories the builder
/// needs. Avoids shipping `.git`, `target/`, `node_modules/`,
/// etc. into the build context (would bloat docker's tarball
/// and invalidate the layer cache every time a host artifact
/// changes).
fn stage_build_context(
    project_root: &Path,
    crate_root: &Path,
    weft_root: &Path,
    dockerfile_path: &Path,
) -> CompileResult<PathBuf> {
    let ctx = project_root.join(".weft").join("target").join("worker-image");
    if ctx.exists() {
        std::fs::remove_dir_all(&ctx).map_err(CompileError::Io)?;
    }
    std::fs::create_dir_all(&ctx).map_err(CompileError::Io)?;

    std::fs::copy(dockerfile_path, ctx.join("Dockerfile")).map_err(CompileError::Io)?;

    // `build/` = generated cargo crate. Copy target excluded; it
    // doesn't exist on the host anymore (no host cargo build), but
    // belt-and-suspenders.
    copy_dir_filtered(crate_root, &ctx.join("build"), &["target"])?;

    // `weft/` = weft workspace. Include crates, catalog, Cargo.toml,
    // Cargo.lock. Exclude target, crates-v1 (unused), node_modules
    // (extension), anything else heavy.
    let weft_stage = ctx.join("weft");
    std::fs::create_dir_all(&weft_stage).map_err(CompileError::Io)?;
    copy_dir_filtered(&weft_root.join("crates"), &weft_stage.join("crates"), &["target"])?;
    copy_dir_filtered(&weft_root.join("catalog"), &weft_stage.join("catalog"), &[])?;
    // Workspace Cargo.toml + Cargo.lock are required for path deps
    // to resolve inside the container.
    for name in ["Cargo.toml", "Cargo.lock"] {
        let src = weft_root.join(name);
        if src.exists() {
            std::fs::copy(&src, weft_stage.join(name)).map_err(CompileError::Io)?;
        }
    }

    Ok(ctx)
}

/// Recursive directory copy with a simple exclude list matched on
/// the immediate entry name. Skips symlinks to avoid infinite
/// recursion and unexpected escapes from the staged context.
fn copy_dir_filtered(src: &Path, dst: &Path, exclude: &[&str]) -> CompileResult<()> {
    std::fs::create_dir_all(dst).map_err(CompileError::Io)?;
    for entry in std::fs::read_dir(src).map_err(CompileError::Io)? {
        let entry = entry.map_err(CompileError::Io)?;
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if exclude.iter().any(|e| *e == name_str) {
            continue;
        }
        let ft = entry.file_type().map_err(CompileError::Io)?;
        let from = entry.path();
        let to = dst.join(&name);
        if ft.is_symlink() {
            continue;
        }
        if ft.is_dir() {
            copy_dir_filtered(&from, &to, exclude)?;
        } else {
            std::fs::copy(&from, &to).map_err(CompileError::Io)?;
        }
    }
    Ok(())
}

/// Locate the weft workspace root. Compile-time: the
/// `CARGO_MANIFEST_DIR` of this crate is
/// `<weft>/crates/weft-compiler`. Runtime override via
/// `WEFT_REPO_ROOT` (used by the dispatcher container where
/// CARGO_MANIFEST_DIR is baked to a path that doesn't exist).
fn resolve_weft_root() -> CompileResult<PathBuf> {
    if let Ok(p) = std::env::var("WEFT_REPO_ROOT") {
        return Ok(PathBuf::from(p));
    }
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .map(|p| p.to_path_buf())
        .ok_or_else(|| CompileError::Build("cannot resolve weft workspace root".into()))
}

/// Sanitize a project name to a valid cargo crate + binary name.
/// Used by both codegen and the CLI so the two agree on what
/// binary the Dockerfile produces.
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
    out
}

/// Build the catalog the compiler should search for a given project.
/// Order is low-priority first: stdlib < vendor < user. Later sources
/// shadow earlier ones, so a user-defined `Foo` overrides a vendored
/// or stdlib `Foo`.
pub fn build_project_catalog(project_root: &Path) -> CompileResult<FsCatalog> {
    let stdlib = CatalogSource { root: stdlib_root(), origin: CatalogOrigin::Stdlib };
    let vendor = CatalogSource {
        root: project_root.join("nodes").join("vendor"),
        origin: CatalogOrigin::Vendor,
    };
    let user = CatalogSource {
        root: project_root.join("nodes"),
        origin: CatalogOrigin::User,
    };
    FsCatalog::discover(&[stdlib, vendor, user])
        .map_err(|e| CompileError::Enrich(format!("catalog: {e}")))
}
