//! `weft build`: compile the current project to a native binary,
//! then package it as a container image so the dispatcher can run
//! it as a Pod in kind.
//!
//! The image is named `weft-worker-<project-id>:latest`, built from
//! a minimal debian runtime, and loaded into the kind cluster. Same
//! pipeline will target a registry push in cloud deploy; only the
//! load step changes.

use std::env;

use anyhow::{Context, Result};
use tokio::process::Command;

use super::Ctx;
use crate::images;
use weft_compiler::build::build_project;
use weft_compiler::project::Project;

const CLUSTER_NAME: &str = "weft-local";

pub async fn run(_ctx: Ctx) -> Result<()> {
    let cwd = env::current_dir()?;
    let project = Project::discover(&cwd)
        .map_err(|e| anyhow::anyhow!("locate project: {e}"))?;

    println!("compiling {}...", project.manifest.package.name);
    let result = build_project(&project.root, true)
        .map_err(|e| anyhow::anyhow!("build failed: {e}"))?;

    println!("built: {}", result.binary_path.display());

    let image_tag = worker_image_tag(&project);
    ensure_worker_image(&project, &image_tag, &result.binary_path).await?;
    Ok(())
}

pub fn worker_image_tag(project: &Project) -> String {
    format!("weft-worker-{}:latest", project.id())
}

/// Build the project's worker image and, if kind is available,
/// load it into the local cluster. Shared between `weft build` and
/// the auto-register helper (`ensure_registered`) so every
/// code path that might spawn a worker ensures the image is
/// present.
pub async fn ensure_worker_image(
    project: &Project,
    tag: &str,
    binary_path: &std::path::Path,
) -> Result<()> {
    write_worker_dockerfile(project)?;
    build_worker_image(project, tag, binary_path).await?;
    if kind_available().await {
        images::kind_load(CLUSTER_NAME, tag).await?;
        println!("loaded {tag} into kind cluster '{CLUSTER_NAME}'");
    } else {
        println!("(kind cluster not available; image {tag} is in local docker only)");
    }
    Ok(())
}

fn write_worker_dockerfile(project: &Project) -> Result<()> {
    // The Dockerfile reads the pre-built binary from the build
    // output directory. Keeping it in the project so re-runs are
    // reproducible and the user can inspect it if something
    // breaks.
    let path = project.root.join(".weft/target/Dockerfile.worker");
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let body = r#"FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY worker /usr/local/bin/worker
ENTRYPOINT ["/usr/local/bin/worker"]
"#;
    std::fs::write(&path, body)?;
    Ok(())
}

async fn build_worker_image(
    project: &Project,
    tag: &str,
    binary_path: &std::path::Path,
) -> Result<()> {
    let ctx_dir = project.root.join(".weft/target/worker-image");
    std::fs::create_dir_all(&ctx_dir).context("create worker image build dir")?;
    let dest = ctx_dir.join("worker");
    std::fs::copy(binary_path, &dest)
        .with_context(|| format!("copy binary to {}", dest.display()))?;
    let dockerfile = project.root.join(".weft/target/Dockerfile.worker");
    std::fs::copy(&dockerfile, ctx_dir.join("Dockerfile"))
        .context("stage worker Dockerfile")?;

    println!("building image {tag}");
    let status = Command::new("docker")
        .args(["build", "-t", tag, "-f"])
        .arg(ctx_dir.join("Dockerfile"))
        .arg(&ctx_dir)
        .status()
        .await?;
    if !status.success() {
        anyhow::bail!("docker build {tag} exited {status}");
    }
    Ok(())
}

async fn kind_available() -> bool {
    // Treat missing kind, missing cluster, or docker unavailable as
    // "not here, skip the load step." The build still succeeds.
    let which = Command::new("which").arg("kind").output().await;
    if !matches!(which, Ok(o) if o.status.success()) {
        return false;
    }
    let out = Command::new("kind").args(["get", "clusters"]).output().await;
    matches!(out, Ok(o) if o.status.success()
        && String::from_utf8_lossy(&o.stdout).lines().any(|l| l == CLUSTER_NAME))
}
