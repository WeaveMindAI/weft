//! `weft build`: compile the current project into a worker
//! container image.
//!
//! The image is named `weft-worker-<project-id>:latest`. The
//! worker binary is produced inside a multi-stage `docker build`
//! (see `weft_compiler::worker_image`), which means the host
//! needs ONLY docker + kind + kubectl. No Rust, no Python, no
//! distro-specific libraries on the host.

use std::env;

use anyhow::Result;
use tokio::process::Command;

use super::Ctx;
use crate::commands::daemon::{cluster_config, ClusterBackend};
use crate::images;
use weft_compiler::build::{build_project, BuildResult};
use weft_compiler::project::Project;

pub async fn run(_ctx: Ctx) -> Result<()> {
    let cwd = env::current_dir()?;
    let project = Project::discover(&cwd)
        .map_err(|e| anyhow::anyhow!("locate project: {e}"))?;

    println!("compiling {}...", project.manifest.package.name);
    let result = build_project(&project.root, true)
        .map_err(|e| anyhow::anyhow!("build failed: {e}"))?;

    let image_tag = worker_image_tag(&project);
    ensure_worker_image(&project, &image_tag, &result).await?;
    Ok(())
}

pub fn worker_image_tag(project: &Project) -> String {
    format!("weft-worker-{}:latest", project.id())
}

/// Build the project's worker image and, if kind is available,
/// load it into the local cluster. Shared between `weft build`
/// and `ensure_registered` so every code path that might spawn a
/// worker ensures the image is present.
pub async fn ensure_worker_image(
    _project: &Project,
    tag: &str,
    build: &BuildResult,
) -> Result<()> {
    let summary = &build.dockerfile_summary;
    let distro = if summary.base.distro_key.is_empty() {
        "default-only".to_string()
    } else {
        summary.base.distro_key.clone()
    };
    println!(
        "worker image base: {} [{}]",
        summary.base.raw, distro,
    );
    if !summary.build_packages.is_empty() {
        println!(
            "  build: {} via {}: {}",
            summary.build_packages.len(),
            summary.base.manager.name(),
            summary.build_packages.join(", "),
        );
    }
    if !summary.runtime_packages.is_empty() {
        println!(
            "  runtime: {} via {}: {}",
            summary.runtime_packages.len(),
            summary.base.manager.name(),
            summary.runtime_packages.join(", "),
        );
    }

    let project_id = project_id_from_tag(tag);
    docker_build(tag, &build.build_context, &project_id).await?;
    // After a successful rebuild the previous content of the same
    // tag is now a dangling `<none>:<none>` image. Prune those
    // labelled with this project's id so accumulating rebuilds
    // don't leave a trail of orphaned images. Cargo build cache
    // (the heavy part) lives in BuildKit and survives this.
    prune_dangling_for_project(&project_id).await;
    let cfg = cluster_config();
    match cfg.backend {
        ClusterBackend::Kind if kind_available(&cfg.cluster_name).await => {
            images::kind_load(&cfg.cluster_name, tag).await?;
            println!("loaded {tag} into kind cluster '{}'", cfg.cluster_name);
        }
        ClusterBackend::Kind => {
            println!(
                "(kind cluster '{}' not available; image {tag} is in local docker only)",
                cfg.cluster_name,
            );
        }
        ClusterBackend::K8s => {
            // External cluster: we do NOT implicitly push to a
            // registry. That's a distinct operation that needs
            // credentials; the user wires it up separately.
            println!("(backend=k8s; push image {tag} to your registry manually)");
        }
    }
    Ok(())
}

async fn docker_build(
    tag: &str,
    ctx_dir: &std::path::Path,
    project_id: &str,
) -> Result<()> {
    println!("building image {tag}");
    let label = format!("weft.dev/project={project_id}");
    let status = Command::new("docker")
        .args(["build", "-t", tag, "--label", &label, "-f"])
        .arg(ctx_dir.join("Dockerfile"))
        .arg(ctx_dir)
        .env("DOCKER_BUILDKIT", "1")
        .status()
        .await?;
    if !status.success() {
        anyhow::bail!("docker build {tag} exited {status}");
    }
    Ok(())
}

/// `weft-worker-<id>:latest` -> `<id>`. Used as the docker label
/// value so we can prune dangling images for one project without
/// touching others.
fn project_id_from_tag(tag: &str) -> String {
    let prefix = "weft-worker-";
    let stripped = tag.strip_prefix(prefix).unwrap_or(tag);
    stripped.split(':').next().unwrap_or(stripped).to_string()
}

/// Remove every dangling image labelled with this project. Best
/// effort: a transient docker error doesn't fail the build.
async fn prune_dangling_for_project(project_id: &str) {
    let label_filter = format!("label=weft.dev/project={project_id}");
    let _ = Command::new("docker")
        .args([
            "image",
            "prune",
            "--force",
            "--filter",
            "dangling=true",
            "--filter",
            &label_filter,
        ])
        .status()
        .await;
}

async fn kind_available(cluster_name: &str) -> bool {
    let which = Command::new("which").arg("kind").output().await;
    if !matches!(which, Ok(o) if o.status.success()) {
        return false;
    }
    let out = Command::new("kind").args(["get", "clusters"]).output().await;
    matches!(out, Ok(o) if o.status.success()
        && String::from_utf8_lossy(&o.stdout).lines().any(|l| l == cluster_name))
}
