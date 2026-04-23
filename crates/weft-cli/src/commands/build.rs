//! `weft build`: compile the current project to a native binary.
//!
//! Runs the full pipeline in-process via `weft-compiler`. On success,
//! prints the binary path to stdout so scripts and the dispatcher can
//! pick it up. No dispatcher interaction: build is purely local.

use std::env;

use super::Ctx;
use weft_compiler::build::build_project;

pub async fn run(_ctx: Ctx) -> anyhow::Result<()> {
    let cwd = env::current_dir()?;
    let project = weft_compiler::project::Project::discover(&cwd)
        .map_err(|e| anyhow::anyhow!("locate project: {e}"))?;

    println!("compiling {}...", project.manifest.package.name);
    let result = build_project(&project.root, true)
        .map_err(|e| anyhow::anyhow!("build failed: {e}"))?;

    println!("built: {}", result.binary_path.display());
    Ok(())
}
