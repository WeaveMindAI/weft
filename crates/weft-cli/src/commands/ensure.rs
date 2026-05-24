//! Shared helper: discover the cwd project, compile it, register
//! it (or re-register if it already exists). Every mutating
//! project-scoped command (`run`, `activate`, `deactivate`,
//! `infra up`, `infra down`) calls this first so users don't
//! have to remember `weft run` as a prerequisite.
//!
//! Semantics:
//!   - Compile via `weft-compiler::build`: this stages the
//!     docker build context and emits the multi-stage Dockerfile
//!     but does NOT run cargo (cargo runs inside the builder
//!     image).
//!   - Build the per-project worker image and load it into the
//!     local kind cluster so spawned worker Pods can pull it.
//!   - Post to `POST /projects`; the dispatcher is idempotent on
//!     the `id` field (existing row gets its source updated).

use anyhow::{Context, Result};

use super::Ctx;
use crate::client::DispatcherClient;
use crate::progress::Progress;

pub struct ProjectHandle {
    pub id: String,
    pub name: String,
    pub client: DispatcherClient,
    pub project: weft_compiler::project::Project,
    /// Source hash sent to the dispatcher. Doubles as the worker
    /// docker image tag suffix and as the resync drift signal.
    pub source_hash: String,
    /// Infra hash sent to the dispatcher. Drives the upgrade drift
    /// signal. Computed from the parsed project + the workspace.
    pub infra_hash: String,
}

pub async fn ensure_registered(ctx: &Ctx, progress: &Progress) -> Result<ProjectHandle> {
    let cwd = std::env::current_dir().context("cwd")?;
    let project = weft_compiler::project::Project::discover(&cwd)
        .map_err(|e| anyhow::anyhow!("discover project: {e}"))?;

    let weft_root = weft_compiler::build::resolve_weft_root()
        .map_err(|e| anyhow::anyhow!("resolve weft repo root: {e}"))?;

    // Compile + enrich once; both hashes are scoped to the referenced
    // / infra-closure nodes, so they need the definition + catalog.
    // Cheap, and downstream code after ensure_registered needs these
    // anyway.
    let (definition, catalog) = crate::hash::load_enriched_project(&project)?;

    // Worker image: hash-skip + build + kind-load. The dispatcher
    // gets the source_hash on every spawn-relevant call so the
    // project row's `running_source_hash` stays current regardless
    // of whether we rebuilt or hit the cache.
    let source_hash =
        crate::hash::compute_source_hash(&definition, &project.root, &weft_root, &catalog)?;
    let image_tag = crate::commands::build::worker_image_tag(&project, &source_hash);
    crate::commands::build::ensure_worker_image_with_progress(progress, &project, &image_tag)
        .await
        .context("worker image")?;

    let infra_hash =
        crate::hash::compute_infra_hash(&definition, &project.root, &weft_root, &catalog)?;

    let client = ctx.client();
    let dispatcher = ctx.dispatcher_url().to_string();

    let source_short = crate::commands::build::short_hash(&source_hash);
    let infra_short = crate::commands::build::short_hash(&infra_hash);
    // Send the already compiled + enriched definition (built above for
    // the infra hash). The dispatcher can't compile it: the nodes live
    // here, not in the dispatcher pod. It stores the artifact as-is.
    let register_body = serde_json::json!({
        "id": project.id().to_string(),
        "name": project.manifest.package.name,
        "definition": definition,
        "sourceHash": source_short,
        "infraHash": infra_short,
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
        source_hash: source_short,
        infra_hash: infra_short,
    })
}
