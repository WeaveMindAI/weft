//! `weft infra start | stop | terminate | upgrade | status`. Five
//! lifecycle verbs:
//!
//!   - **start**: provision the sidecars if missing, or scale them
//!     back to 1 if they were stopped. Refuses if already running.
//!   - **stop**: scale the Deployments to 0. Keeps PVCs / Services
//!     so a later `start` resumes the same instance. Refuses if
//!     already stopped.
//!   - **terminate**: delete every k8s resource the sidecars own,
//!     PVCs included. Irreversible. Idempotent (safe to re-run).
//!   - **upgrade**: atomic stop + sidecar image swap + start.
//!   - **status**: print the current lifecycle state of every
//!     infra node for the cwd project.
//!
//! `start` / `upgrade` auto-build any sidecar images they need
//! first. The shared progress emitter feeds the VS Code action
//! bar; in --json mode the CLI never reads from stdin (the
//! extension passes --deactivate-triggers explicitly when it needs
//! to confirm a destructive verb).

use std::collections::BTreeMap;

use anyhow::Result;

use super::Ctx;
use crate::commands::daemon::{cluster_config, ClusterBackend};
use crate::images;
use crate::progress::{ActionVerb, Progress};
use weft_catalog::stdlib_catalog;

pub enum InfraAction {
    Start,
    Stop,
    Terminate,
    Upgrade,
    Status,
}

/// Whether the verb call should ask the dispatcher to drop
/// triggers as part of the operation. None = prompt the user
/// (terminal mode); Some(_) = use this verbatim (extension mode).
pub type DeactivateChoice = Option<bool>;

pub async fn run(
    ctx: Ctx,
    action: InfraAction,
    deactivate_triggers: DeactivateChoice,
) -> Result<()> {
    // Status is read-only with no progress events; route directly.
    if matches!(action, InfraAction::Status) {
        return infra_status(&ctx).await;
    }
    let verb = match action {
        InfraAction::Start => ActionVerb::InfraStart,
        InfraAction::Stop => ActionVerb::InfraStop,
        InfraAction::Terminate => ActionVerb::InfraTerminate,
        InfraAction::Upgrade => ActionVerb::InfraUpgrade,
        InfraAction::Status => unreachable!(),
    };
    let ctx_inner = ctx.clone();
    ctx.with_progress(verb, |progress| async move {
        run_inner(&ctx_inner, &progress, action, deactivate_triggers).await
    })
    .await
}

async fn run_inner(
    ctx: &Ctx,
    progress: &Progress,
    action: InfraAction,
    deactivate_choice: DeactivateChoice,
) -> Result<()> {
    match action {
        InfraAction::Start => infra_start(ctx, progress).await,
        InfraAction::Upgrade => infra_upgrade(ctx, progress, deactivate_choice).await,
        InfraAction::Stop => infra_stop(ctx, progress, deactivate_choice).await,
        InfraAction::Terminate => infra_terminate(ctx, progress, deactivate_choice).await,
        InfraAction::Status => unreachable!("status routed directly"),
    }
}

async fn infra_start(ctx: &Ctx, progress: &Progress) -> Result<()> {
    let handle = super::ensure::ensure_registered(ctx, progress).await?;
    let sidecar_hashes = ensure_sidecar_images(progress, &handle.project, &handle.id).await?;
    let path = format!("/projects/{}/infra/start", handle.id);
    let body = serde_json::json!({
        "sourceHash": handle.source_hash,
        "infraHash": handle.infra_hash,
        "sidecarHashes": sidecar_hashes,
    });
    let node_ids: Vec<String> = sidecar_hashes.keys().cloned().collect();
    progress.infra_provision_start(&node_ids);
    progress.dispatcher_call_start(&path);
    let resp: serde_json::Value = handle.client.post_json(&path, &body).await?;
    progress.dispatcher_call_done(serde_json::json!({
        "project_id": handle.id,
        "sidecars_provisioned": node_ids.len(),
    }));
    progress.infra_provision_done();
    if !ctx.json() {
        print_status(&handle.name, &handle.id, &resp);
    }
    progress.complete(&format!("infra started for {}", handle.name));
    Ok(())
}

async fn infra_upgrade(
    ctx: &Ctx,
    progress: &Progress,
    deactivate_choice: DeactivateChoice,
) -> Result<()> {
    let handle = super::ensure::ensure_registered(ctx, progress).await?;
    let sidecar_hashes = ensure_sidecar_images(progress, &handle.project, &handle.id).await?;
    let deactivate =
        resolve_deactivate(&handle.client, &handle.id, "upgrade", deactivate_choice, ctx.json()).await?;
    let path = format!("/projects/{}/infra/upgrade", handle.id);
    let body = serde_json::json!({
        "sourceHash": handle.source_hash,
        "infraHash": handle.infra_hash,
        "sidecarHashes": sidecar_hashes,
        "deactivateTriggers": deactivate,
    });
    let node_ids: Vec<String> = sidecar_hashes.keys().cloned().collect();
    progress.infra_provision_start(&node_ids);
    progress.dispatcher_call_start(&path);
    let resp: serde_json::Value = handle.client.post_json(&path, &body).await?;
    progress.dispatcher_call_done(serde_json::json!({
        "project_id": handle.id,
        "sidecars_provisioned": node_ids.len(),
    }));
    progress.infra_provision_done();
    if !ctx.json() {
        print_status(&handle.name, &handle.id, &resp);
    }
    progress.complete(&format!("infra upgraded for {}", handle.name));
    Ok(())
}

async fn infra_stop(
    ctx: &Ctx,
    progress: &Progress,
    deactivate_choice: DeactivateChoice,
) -> Result<()> {
    let (client, id, name) = super::resolve_project(ctx)?;
    let deactivate =
        resolve_deactivate(&client, &id, "stop", deactivate_choice, ctx.json()).await?;
    let path = format!("/projects/{id}/infra/stop");
    let body = serde_json::json!({ "deactivateTriggers": deactivate });
    progress.dispatcher_call_start(&path);
    let resp: serde_json::Value = client.post_json(&path, &body).await?;
    progress.dispatcher_call_done(serde_json::json!({ "project_id": id }));
    if !ctx.json() {
        print_status(&name, &id, &resp);
    }
    progress.complete(&format!("infra stopped for {name}"));
    Ok(())
}

async fn infra_terminate(
    ctx: &Ctx,
    progress: &Progress,
    deactivate_choice: DeactivateChoice,
) -> Result<()> {
    let (client, id, name) = super::resolve_project(ctx)?;
    let deactivate =
        resolve_deactivate(&client, &id, "terminate", deactivate_choice, ctx.json()).await?;
    let path = format!("/projects/{id}/infra/terminate");
    let body = serde_json::json!({ "deactivateTriggers": deactivate });
    progress.dispatcher_call_start(&path);
    let resp: serde_json::Value = client.post_json(&path, &body).await?;
    progress.dispatcher_call_done(serde_json::json!({ "project_id": id }));
    if !ctx.json() {
        print_status(&name, &id, &resp);
    }
    progress.complete(&format!("infra terminated for {name}"));
    Ok(())
}

async fn infra_status(ctx: &Ctx) -> Result<()> {
    let (client, id, name) = super::resolve_project(ctx)?;
    let resp: serde_json::Value = client
        .get_json(&format!("/projects/{id}/infra/status"))
        .await?;
    print_status(&name, &id, &resp);
    Ok(())
}

/// Pick the deactivate-triggers flag: extension always passes one
/// in (post-confirmation in its own UI), terminal users get the
/// stdin prompt. The dispatcher refuses destructive verbs against
/// an active project unless this flag is true.
async fn resolve_deactivate(
    client: &crate::client::DispatcherClient,
    project_id: &str,
    verb: &str,
    explicit: DeactivateChoice,
    json: bool,
) -> Result<bool> {
    if let Some(b) = explicit {
        return Ok(b);
    }
    if json {
        // No prompt available; the extension should have passed
        // --deactivate-triggers explicitly. Fail loud rather than
        // silently picking a default.
        anyhow::bail!(
            "--json mode requires --deactivate-triggers <true|false> (cannot prompt)"
        );
    }
    let status: serde_json::Value = client
        .get_json(&format!("/projects/{project_id}/status"))
        .await?;
    let active = status
        .get("status")
        .and_then(|v| v.as_str())
        .is_some_and(|s| s == "active");
    if !active {
        return Ok(false);
    }
    println!(
        "Project has active triggers. Running `{verb}` will deactivate them; \
         you'll need to reactivate manually after."
    );
    println!("Continue? [y/N]");
    let mut line = String::new();
    std::io::stdin().read_line(&mut line)?;
    let confirmed =
        line.trim().eq_ignore_ascii_case("y") || line.trim().eq_ignore_ascii_case("yes");
    if !confirmed {
        anyhow::bail!("aborted: triggers are active and user did not confirm");
    }
    Ok(true)
}

fn print_status(name: &str, id: &str, resp: &serde_json::Value) {
    let nodes = resp
        .get("nodes")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    if nodes.is_empty() {
        println!("no infra nodes in this project");
        return;
    }
    println!("infra for {name} ({id}):");
    for n in nodes {
        let node = n.get("node_id").and_then(|v| v.as_str()).unwrap_or("?");
        let status = n.get("status").and_then(|v| v.as_str()).unwrap_or("?");
        let url = n
            .get("endpoint_url")
            .and_then(|v| v.as_str())
            .unwrap_or("(no endpoint)");
        println!("  {node} [{status}] -> {url}");
    }
}

/// Build + kind-load every sidecar image the project's infra nodes
/// reference. Hash-tagged: skip the docker build entirely when the
/// hash-tagged image is already in the local cache. Each image's
/// build/push emits its own pair of progress events so the action
/// bar can show "building sidecar X" granularity. Returns a map of
/// (infra_node_id -> sidecar_hash) for the dispatcher.
async fn ensure_sidecar_images(
    progress: &Progress,
    project: &weft_compiler::project::Project,
    project_id: &str,
) -> Result<BTreeMap<String, String>> {
    let definition = crate::hash::load_enriched_project(project)?;
    let catalog = stdlib_catalog().map_err(|e| anyhow::anyhow!("load catalog: {e}"))?;

    let mut hashes_by_node: BTreeMap<String, String> = BTreeMap::new();
    // Cache build/load decisions per (sidecar_name, hash) so we
    // don't re-process the same image across multiple infra nodes
    // that share a sidecar.
    let mut seen_tags: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();

    for node in definition.nodes.iter().filter(|n| n.requires_infra) {
        let Some(entry) = catalog.entry(&node.node_type) else {
            anyhow::bail!(
                "node '{}' has type '{}' which is not in the catalog",
                node.id, node.node_type
            );
        };
        let Some(sidecar_spec) = entry.metadata.features.sidecar.as_ref() else {
            anyhow::bail!(
                "node '{}' (type '{}') is requires_infra but its catalog metadata has no sidecar spec",
                node.id, node.node_type
            );
        };
        let sidecar_name = sidecar_spec.name.clone();
        let sidecar_dir = entry.source_dir.join("sidecar");
        let dockerfile = sidecar_dir.join("Dockerfile");
        if !dockerfile.is_file() {
            anyhow::bail!(
                "node type '{}' declares sidecar '{sidecar_name}' but no Dockerfile at {}",
                node.node_type,
                dockerfile.display()
            );
        }

        let full_hash = crate::hash::compute_sidecar_hash(&node.node_type, &sidecar_dir)?;
        let short = crate::commands::build::short_hash(&full_hash);
        let tag = format!("weft-sidecar-{sidecar_name}:{short}");
        hashes_by_node.insert(node.id.clone(), short.clone());

        if !seen_tags.insert(tag.clone()) {
            continue;
        }

        let exists = images::image_present(&tag).await.unwrap_or(false);
        if exists {
            progress.build_skip(&tag, "hash_match");
        } else {
            progress.build_start(&tag);
            let label = format!("weft.dev/project={project_id}");
            crate::commands::build::docker_build_image(
                &tag,
                &dockerfile,
                &sidecar_dir,
                Some(&label),
            )
            .await?;
            progress.build_done(&tag);
        }
        let cfg = cluster_config();
        if cfg.backend == ClusterBackend::Kind {
            progress.image_push_start(&tag);
            images::kind_load(&cfg.cluster_name, &tag).await?;
            progress.image_push_done(&tag);
        }
    }
    Ok(hashes_by_node)
}
