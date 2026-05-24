//! `weft build`: compile the current project into a worker
//! container image. Tagged `weft-worker-<project-id>:<short-hash>`
//! where the short-hash is the first 12 chars of the source hash
//! (so each rebuild yields a fresh tag and the cluster pulls the
//! right image without cache surprises). The build itself runs
//! in a multi-stage `docker build` so the host needs only
//! docker + kind + kubectl.

use std::env;

use anyhow::Result;
use tokio::process::Command;

use super::Ctx;
use crate::commands::daemon::{cluster_config, ClusterBackend};
use crate::images;
use crate::progress::{ActionVerb, Progress};
use weft_compiler::build::{build_project, BuildResult};
use weft_compiler::project::Project;

pub async fn run(ctx: Ctx) -> Result<()> {
    ctx.with_progress(ActionVerb::Build, |progress| async move {
        let cwd = env::current_dir()?;
        let project = Project::discover(&cwd)
            .map_err(|e| anyhow::anyhow!("locate project: {e}"))?;
        let weft_root = weft_compiler::build::resolve_weft_root()
            .map_err(|e| anyhow::anyhow!("resolve weft repo root: {e}"))?;
        let (definition, catalog) = crate::hash::load_enriched_project(&project)?;
        let source_hash =
            crate::hash::compute_source_hash(&definition, &project.root, &weft_root, &catalog)?;
        let image_tag = worker_image_tag(&project, &source_hash);
        ensure_worker_image_with_progress(&progress, &project, &image_tag).await?;
        progress.complete(&format!("worker image {}", short_hash(&source_hash)));
        Ok(())
    })
    .await
}

/// Top-level helper used by every verb that needs the worker image
/// in place. Owns the image-skip check + emits build/push events;
/// callers don't duplicate the existence check anymore. If the
/// hash-tagged image is missing, this compiles the project and
/// docker-builds it; otherwise it ensures kind has the image
/// loaded.
pub async fn ensure_worker_image_with_progress(
    progress: &Progress,
    project: &Project,
    image_tag: &str,
) -> Result<()> {
    if crate::images::image_present(image_tag).await? {
        progress.build_skip(image_tag, "hash_match");
        let cfg = cluster_config();
        match cfg.backend {
            ClusterBackend::Kind if kind_available(&cfg.cluster_name).await => {
                progress.image_push_start(image_tag);
                crate::images::kind_load(&cfg.cluster_name, image_tag).await?;
                progress.image_push_done(image_tag);
            }
            ClusterBackend::Kind => {}
            ClusterBackend::K8s => return Err(bail_k8s_push_needed(image_tag)),
        }
        return Ok(());
    }

    progress.build_start(image_tag);
    let build = build_project(&project.root, true)
        .map_err(|e| anyhow::anyhow!("build failed: {e}"))?;
    docker_build_and_kind_load(progress, image_tag, &build).await?;
    progress.build_done(image_tag);
    Ok(())
}

/// Compose a hash-tagged worker image name. The hash is the source
/// hash from `crate::hash::compute_source_hash`; the dispatcher
/// reads the same hash from the project row when picking the image
/// to spawn.
pub fn worker_image_tag(project: &Project, source_hash: &str) -> String {
    format!("weft-worker-{}:{}", project.id(), short_hash(source_hash))
}

/// 16-char prefix of the SHA-256 source hash. Enough collision
/// resistance for a per-project image namespace; short enough to
/// keep tag strings legible. The dispatcher uses the SAME prefix
/// (consumes the hash the CLI sent in /projects body and uses it
/// verbatim as the tag).
pub fn short_hash(hash: &str) -> String {
    hash.chars().take(16).collect()
}

/// One source of truth for the "K8s backend selected, image is
/// local-only, push it yourself" failure. Used by every CLI step
/// that would otherwise let a subsequent `weft activate` spawn a
/// pod that ImagePullBackOffs with no breadcrumb back to the CLI.
fn bail_k8s_push_needed(tag: &str) -> anyhow::Error {
    anyhow::anyhow!(
        "K8s backend selected but image '{tag}' is only in local docker. \
         Push it to your registry before activating, e.g.:\n\
         \n\
         \tdocker tag {tag} <your-registry>/{tag}\n\
         \tdocker push <your-registry>/{tag}\n\
         \n\
         Or run against the kind cluster (set WEFT_CLUSTER_BACKEND=kind)."
    )
}

/// Run the docker build (assumes the project has been compiled),
/// prune dangling images, and load into kind if available. Inner
/// step of `ensure_worker_image_with_progress`. Emits the
/// image_push events around the kind-load step.
async fn docker_build_and_kind_load(
    progress: &Progress,
    tag: &str,
    build: &BuildResult,
) -> Result<()> {
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
            progress.image_push_start(tag);
            images::kind_load(&cfg.cluster_name, tag).await?;
            progress.image_push_done(tag);
        }
        ClusterBackend::Kind => {
            // Kind cluster declared but not running: image stays in
            // local docker. The next ensure_worker_image_with_progress
            // call (e.g. after `weft daemon start`) will load it.
        }
        ClusterBackend::K8s => return Err(bail_k8s_push_needed(tag)),
    }
    Ok(())
}

async fn docker_build(
    tag: &str,
    ctx_dir: &std::path::Path,
    project_id: &str,
) -> Result<()> {
    let label = format!("weft.dev/project={project_id}");
    docker_build_image(tag, &ctx_dir.join("Dockerfile"), ctx_dir, Some(&label)).await
}

/// Single docker build entrypoint used by both worker images and
/// infra images. The `label` arg lets callers stamp the
/// `weft.dev/project=<id>` filter so `prune_dangling_for_project`
/// can find their leftovers later. Other callers can pass `None`
/// or a different domain label.
pub async fn docker_build_image(
    tag: &str,
    dockerfile: &std::path::Path,
    ctx_dir: &std::path::Path,
    label: Option<&str>,
) -> Result<()> {
    let mut cmd = Command::new("docker");
    cmd.args(["build", "-t", tag]);
    if let Some(l) = label {
        cmd.args(["--label", l]);
    }
    cmd.arg("-f").arg(dockerfile).arg(ctx_dir);
    cmd.env("DOCKER_BUILDKIT", "1");
    let status = cmd.status().await?;
    if !status.success() {
        anyhow::bail!("docker build {tag} exited {status}");
    }
    Ok(())
}

/// `weft-worker-<id>:<short-hash>` -> `<id>`. Used as the docker
/// label value so we can prune dangling images for one project
/// without touching others.
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

pub async fn kind_available(cluster_name: &str) -> bool {
    let which = Command::new("which").arg("kind").output().await;
    if !matches!(which, Ok(o) if o.status.success()) {
        return false;
    }
    let out = Command::new("kind").args(["get", "clusters"]).output().await;
    matches!(out, Ok(o) if o.status.success()
        && String::from_utf8_lossy(&o.stdout).lines().any(|l| l == cluster_name))
}
