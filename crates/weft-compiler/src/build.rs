//! Compile pipeline orchestration.
//!
//! Given a project root (containing `weft.toml` + `main.weft`), drive
//! parse -> enrich -> validate -> codegen -> cargo build. Return the
//! path to the produced binary.

use std::path::{Path, PathBuf};
use std::process::Command;

use crate::codegen;
use crate::enrich::enrich;
use crate::error::{CompileError, CompileResult};
use crate::project::Project;
use crate::validate::validate;
use crate::weft_compiler::compile;
use crate::Severity;
use weft_catalog::{CatalogOrigin, CatalogSource, FsCatalog, stdlib_root};

/// Built project artifact. `binary_path` is the absolute path to the
/// produced executable; the dispatcher uses it to spawn workers.
pub struct BuildResult {
    pub binary_path: PathBuf,
}

/// Full compile. Loads the project, parses the weft source, enriches,
/// validates, codegens, invokes cargo. Returns the binary path.
pub fn build_project(project_root: &Path, release: bool) -> CompileResult<BuildResult> {
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
    // Preserve the user's package name on the project; `compile`
    // synthesizes a placeholder.
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

    let target_root = project_root.join(".weft").join("target").join("build");
    let crate_root = codegen::emit(&definition, project_root, &target_root, &catalog)?;
    invoke_cargo(&crate_root, release)?;

    let binary_name = sanitize_crate_name(&definition.name);
    let profile = if release { "release" } else { "debug" };
    let suffix = if cfg!(windows) { ".exe" } else { "" };
    let binary_path = crate_root
        .join("target")
        .join(profile)
        .join(format!("{binary_name}{suffix}"));
    if !binary_path.exists() {
        return Err(CompileError::Build(format!(
            "cargo succeeded but binary not found at {}",
            binary_path.display()
        )));
    }

    Ok(BuildResult { binary_path })
}

/// Reproduce codegen::sanitize_crate_name. Kept small and duplicated to
/// avoid exposing the helper publicly.
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

pub fn invoke_cargo(target_root: &Path, release: bool) -> CompileResult<()> {
    let mut cmd = Command::new("cargo");
    cmd.arg("build");
    if release {
        cmd.arg("--release");
    }
    cmd.current_dir(target_root);
    let status = cmd.status().map_err(CompileError::Io)?;
    if !status.success() {
        return Err(CompileError::Build(format!("cargo exited with {}", status)));
    }
    Ok(())
}
