//! Shared helper: discover the cwd project, compile it, register it
//! (or re-register if it already exists). Every mutating
//! project-scoped command (`run`, `activate`, `deactivate`,
//! `infra up`, `infra down`) calls this first so users don't have
//! to remember `weft run` as a prerequisite.
//!
//! Semantics:
//!   - Compile via `weft-compiler::build` (release).
//!   - Post to `POST /projects`; the dispatcher is idempotent on
//!     the `id` field (existing row gets its source + binary_path
//!     updated, new row inserted).
//!   - Build the per-project worker image and load it into the
//!     local kind cluster so spawned worker Pods can pull it.

use anyhow::{Context, Result};

use super::Ctx;
use crate::client::DispatcherClient;

pub struct ProjectHandle {
    pub id: String,
    pub name: String,
    pub client: DispatcherClient,
    pub project: weft_compiler::project::Project,
}

/// Discover the project under `cwd`, compile it, ensure it's
/// registered with the dispatcher. Emits a worker image into the
/// kind cluster so the dispatcher can spawn worker Pods.
///
/// Prints status lines as it progresses so the user sees the
/// compile + register happening (matches what `weft run` used to
/// print before this refactor).
pub async fn ensure_registered(ctx: &Ctx) -> Result<ProjectHandle> {
    let cwd = std::env::current_dir().context("cwd")?;
    let project = weft_compiler::project::Project::discover(&cwd)
        .map_err(|e| anyhow::anyhow!("discover project: {e}"))?;
    let source = project
        .read_main_weft()
        .map_err(|e| anyhow::anyhow!("read main.weft: {e}"))?;

    println!("compiling {}...", project.manifest.package.name);
    let build = weft_compiler::build::build_project(&project.root, true)
        .map_err(|e| anyhow::anyhow!("build failed: {e}"))?;
    println!("built: {}", build.binary_path.display());

    // Build and load the per-project worker image so the K8s
    // worker backend's Pods can pull it on the next spawn.
    let image_tag = crate::commands::build::worker_image_tag(&project);
    crate::commands::build::ensure_worker_image(&project, &image_tag, &build.binary_path)
        .await
        .context("build worker image")?;

    let dispatcher = ctx
        .dispatcher
        .clone()
        .unwrap_or_else(|| project.dispatcher_url());
    let client = DispatcherClient::new(&dispatcher);

    let register_body = serde_json::json!({
        "id": project.id().to_string(),
        "name": project.manifest.package.name,
        "source": source,
        "binary_path": build.binary_path.display().to_string(),
    });
    let register_resp: serde_json::Value = client
        .post_json("/projects", &register_body)
        .await
        .with_context(|| format!("register against {dispatcher}"))?;
    let id = register_resp
        .get("id")
        .and_then(|v| v.as_str())
        .context("dispatcher response missing id")?
        .to_string();

    Ok(ProjectHandle {
        id,
        name: project.manifest.package.name.clone(),
        client,
        project,
    })
}
