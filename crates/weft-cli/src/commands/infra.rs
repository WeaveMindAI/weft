//! `weft infra start | restart | upgrade | stop | terminate | status`.
//!
//! Start / Restart / Upgrade map to the same dispatcher endpoint
//! (`/projects/{id}/infra/sync`); the label is purely UX. The
//! dispatcher decides per-node skip-vs-apply via the resolved
//! spec hash.
//!
//! Stop / Terminate are direct: they enqueue an
//! `infra_lifecycle_command` row that the tenant's supervisor pod
//! claims and executes.

use std::collections::BTreeMap;

use anyhow::Result;

use super::Ctx;
use crate::commands::daemon::{cluster_config, ClusterBackend};
use crate::images;
use crate::progress::{ActionVerb, Progress};

#[derive(Clone)]
pub enum InfraAction {
    Start,
    Restart,
    Upgrade,
    Stop,
    Terminate,
    Status,
    NodeStop { node_id: String, force: bool },
    NodeTerminate { node_id: String },
}

/// Trigger-deactivation choices for the infra verbs that take triggers
/// down (Stop, Terminate, Upgrade). Mirrors the shared
/// `prompt_trigger_deactivation` argument shape.
///
/// All fields are optional at the CLI surface; missing fields prompt
/// the user on a TTY or error in `--json` mode (per the shared
/// helper's contract). There is NO auto-reactivate: a user-triggered
/// upgrade leaves the project deactivated, and the user clicks Activate
/// when ready. Automatic reactivation belongs only to the autonomous
/// health-recovery path (deactivate -> fix infra -> reactivate with no
/// human present), not to a verb the user invoked themselves.
#[derive(Default, Clone)]
pub struct InfraOpts {
    pub mode: Option<String>,
    pub grace: Option<u32>,
    pub running_policy: Option<String>,
}

pub async fn run(ctx: Ctx, action: InfraAction, opts: InfraOpts) -> Result<()> {
    if matches!(action, InfraAction::Status) {
        return infra_status(&ctx).await;
    }
    let verb = match &action {
        InfraAction::Start => ActionVerb::InfraStart,
        InfraAction::Restart => ActionVerb::InfraRestart,
        InfraAction::Upgrade => ActionVerb::InfraUpgrade,
        InfraAction::Stop => ActionVerb::InfraStop,
        InfraAction::Terminate => ActionVerb::InfraTerminate,
        InfraAction::NodeStop { .. } => ActionVerb::InfraNodeStop,
        InfraAction::NodeTerminate { .. } => ActionVerb::InfraNodeTerminate,
        InfraAction::Status => unreachable!(),
    };
    let ctx_inner = ctx.clone();
    ctx.with_progress(verb, |progress| async move {
        run_inner(&ctx_inner, &progress, action, opts).await
    })
    .await
}

async fn run_inner(
    ctx: &Ctx,
    progress: &Progress,
    action: InfraAction,
    opts: InfraOpts,
) -> Result<()> {
    // Run the action's work. The command bodies emit phase progress
    // (dispatcher_call_start/done, provision phases) but NOT a terminal
    // `complete`: we emit exactly one `complete` here, after the whole
    // action settles. This is what keeps the action-bar overlay held
    // for the FULL verb, including multi-phase Upgrade (stop + start);
    // a per-phase `complete` would clear the overlay mid-upgrade and
    // let the bar flicker through intermediate interactive states.
    let summary = match &action {
        InfraAction::Start => "infra started",
        InfraAction::Restart => "infra restarted",
        InfraAction::Upgrade => "infra upgraded",
        InfraAction::Stop => "infra stopped",
        InfraAction::Terminate => "infra terminated",
        InfraAction::NodeStop { .. } => "infra node stopped",
        InfraAction::NodeTerminate { .. } => "infra node terminated",
        InfraAction::Status => unreachable!(),
    };
    match action {
        // Plain Start: just bring DOWN units up (apply skips up units).
        InfraAction::Start => infra_sync(ctx, progress, action, opts).await?,
        // Restart / Upgrade = stop then start. The apply path leaves
        // up units frozen, so to cycle a running unit onto a new spec
        // we stop first (respecting each unit's on_stop: NoOp units stay
        // up, ScaleToZero go down) then start (recreates the down ones).
        // `infra_stop` blocks until the stop settles, so the start sees
        // the post-stop state.
        InfraAction::Restart | InfraAction::Upgrade => {
            infra_stop(ctx, progress, opts.clone()).await?;
            infra_sync(ctx, progress, InfraAction::Start, opts).await?;
        }
        InfraAction::Stop => infra_stop(ctx, progress, opts).await?,
        InfraAction::Terminate => infra_terminate(ctx, progress, opts).await?,
        InfraAction::NodeStop { node_id, force } => {
            infra_node_verb(ctx, progress, &node_id, "stop", force).await?
        }
        InfraAction::NodeTerminate { node_id } => {
            infra_node_verb(ctx, progress, &node_id, "terminate", false).await?
        }
        InfraAction::Status => unreachable!(),
    }
    progress.complete(summary);
    Ok(())
}

async fn infra_node_verb(
    ctx: &Ctx,
    progress: &Progress,
    node_id: &str,
    verb: &str,
    force: bool,
) -> Result<()> {
    let (client, project_id, name) = super::resolve_project(ctx)?;
    let path = format!("/projects/{project_id}/infra/nodes/{node_id}/{verb}");
    let body = serde_json::json!({ "force": force });
    progress.dispatcher_call_start(&path);
    // 202 Accepted with { command_id }.
    let issued: serde_json::Value = client.post_json(&path, &body).await?;
    progress.dispatcher_call_done(serde_json::json!({ "project_id": project_id, "node_id": node_id }));
    let command_id = issued
        .get("command_id")
        .and_then(|v| v.as_i64())
        .ok_or_else(|| anyhow::anyhow!("infra node {verb}: response missing command_id"))?;
    // Wait on the command, not a per-node status: a NoOp unit staying
    // up means the node never reaches "stopped", so the command
    // outcome is the honest done signal (and a force-stop completing
    // is what we actually want to wait for).
    wait_for_command(&client, &project_id, command_id, verb).await?;
    if !ctx.json() {
        let final_resp: serde_json::Value = client
            .get_json(&format!("/projects/{project_id}/infra/status"))
            .await?;
        print_status(&name, &project_id, &final_resp);
    }
    // Terminal event emitted once by `run_inner` (see infra_sync note).
    Ok(())
}

async fn infra_sync(
    ctx: &Ctx,
    progress: &Progress,
    action: InfraAction,
    opts: InfraOpts,
) -> Result<()> {
    let handle = super::ensure::ensure_registered(ctx, progress).await?;
    let image_tags = build_infra_images(progress, &handle.project, &handle.id).await?;
    let verb_label = action_verb_label(&action);

    // Only Upgrade against an Active project actually needs the
    // trigger-deactivation choice. Start / Restart fire when infra
    // is down (project necessarily Inactive); Upgrade when infra is
    // running (project usually Active).
    let active = super::deactivate::project_is_active(&handle.client, &handle.id).await?;
    let trigger_deactivation = if active {
        Some(super::deactivate::prompt_trigger_deactivation(
            ctx.json(),
            &format!("infra {verb_label}"),
            opts.mode.as_deref(),
            opts.grace,
            opts.running_policy.as_deref(),
        )?)
    } else {
        None
    };

    // No auto-reactivate: an Upgrade/Restart leaves the project
    // deactivated (the user invoked it, they click Activate when
    // ready). The trigger-deactivation choice above still applies (it
    // governs HOW triggers come down during the stop).

    let mut body = serde_json::Map::new();
    body.insert("sourceHash".into(), serde_json::json!(handle.source_hash));
    body.insert("infraHash".into(), serde_json::json!(handle.infra_hash));
    body.insert("imageHashes".into(), serde_json::to_value(&image_tags)?);
    if let Some(td) = trigger_deactivation {
        body.insert("triggerDeactivation".into(), td);
    }
    let path = format!("/projects/{}/infra/sync", handle.id);
    let body = serde_json::Value::Object(body);
    let node_ids: Vec<String> = image_tags.keys().cloned().collect();
    progress.infra_provision_start(&node_ids);
    progress.dispatcher_call_start(&path);
    let resp: serde_json::Value = handle.client.post_json(&path, &body).await?;
    progress.dispatcher_call_done(serde_json::json!({ "project_id": handle.id }));
    progress.infra_provision_done();
    if !ctx.json() {
        print_status(&handle.name, &handle.id, &resp);
    }
    // No `progress.complete` here: the terminal event is emitted ONCE
    // by `run_inner` after the whole action. Upgrade chains stop+sync,
    // and a `complete` from a sub-phase would clear the action-bar
    // overlay mid-upgrade (the bar would flicker through intermediate
    // interactive states). This stays a phase, not a terminus.
    Ok(())
}

fn action_verb_label(a: &InfraAction) -> &'static str {
    match a {
        InfraAction::Start => "start",
        InfraAction::Restart => "restart",
        InfraAction::Upgrade => "upgrade",
        InfraAction::Stop => "stop",
        InfraAction::Terminate => "terminate",
        InfraAction::Status => "status",
        InfraAction::NodeStop { .. } => "node-stop",
        InfraAction::NodeTerminate { .. } => "node-terminate",
    }
}


async fn infra_stop(ctx: &Ctx, progress: &Progress, opts: InfraOpts) -> Result<()> {
    infra_destroy(ctx, progress, opts, "stop").await
}

async fn infra_terminate(ctx: &Ctx, progress: &Progress, opts: InfraOpts) -> Result<()> {
    infra_destroy(ctx, progress, opts, "terminate").await
}

/// Stop / Terminate share this body. Prompts for trigger
/// deactivation only when the project is active; sends an empty
/// body otherwise. Waits on the COMMAND's completion (not the rollup):
/// a stop where a NoOp unit stays up never drives the rollup to
/// "stopped", so the command outcome is the only honest done signal.
async fn infra_destroy(
    ctx: &Ctx,
    progress: &Progress,
    opts: InfraOpts,
    verb: &str,
) -> Result<()> {
    let (client, id, name) = super::resolve_project(ctx)?;
    let active = super::deactivate::project_is_active(&client, &id).await?;
    let trigger_deactivation = if active {
        Some(super::deactivate::prompt_trigger_deactivation(
            ctx.json(),
            &format!("infra {verb}"),
            opts.mode.as_deref(),
            opts.grace,
            opts.running_policy.as_deref(),
        )?)
    } else {
        None
    };
    let path = format!("/projects/{id}/infra/{verb}");
    let mut body = serde_json::Map::new();
    if let Some(td) = trigger_deactivation {
        body.insert("triggerDeactivation".into(), td);
    }
    let body = serde_json::Value::Object(body);
    progress.dispatcher_call_start(&path);
    // 202 Accepted with { command_id }.
    let issued: serde_json::Value = client.post_json(&path, &body).await?;
    progress.dispatcher_call_done(serde_json::json!({ "project_id": id }));
    let command_id = issued
        .get("command_id")
        .and_then(|v| v.as_i64())
        .ok_or_else(|| anyhow::anyhow!("infra {verb}: response missing command_id"))?;
    wait_for_command(&client, &id, command_id, verb).await?;
    if !ctx.json() {
        let final_resp: serde_json::Value = client
            .get_json(&format!("/projects/{id}/infra/status"))
            .await?;
        print_status(&name, &id, &final_resp);
    }
    // Terminal event emitted once by `run_inner` (see infra_sync note).
    Ok(())
}

/// Poll the command-status endpoint until the supervisor marks the
/// command complete. Fails loud on a `failed` outcome; `cancelled`
/// (e.g. the node was already gone) is treated as success ("no longer
/// applicable"). 300s deadline.
async fn wait_for_command(
    client: &crate::client::DispatcherClient,
    project_id: &str,
    command_id: i64,
    verb: &str,
) -> Result<()> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(300);
    loop {
        let resp: serde_json::Value = client
            .get_json(&format!("/projects/{project_id}/infra/commands/{command_id}"))
            .await?;
        if resp.get("done").and_then(|v| v.as_bool()).unwrap_or(false) {
            let outcome = resp.get("outcome").and_then(|v| v.as_str()).unwrap_or("");
            if outcome == "failed" {
                let msg = resp.get("message").and_then(|v| v.as_str()).unwrap_or("unknown error");
                anyhow::bail!("infra {verb} failed: {msg}");
            }
            return Ok(());
        }
        if std::time::Instant::now() >= deadline {
            anyhow::bail!("infra {verb} did not complete within 300s");
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
}

async fn infra_status(ctx: &Ctx) -> Result<()> {
    let (client, id, name) = super::resolve_project(ctx)?;
    let resp: serde_json::Value = client
        .get_json(&format!("/projects/{id}/infra/status"))
        .await?;
    print_status(&name, &id, &resp);
    Ok(())
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

/// Build the `(node_id -> { image_name -> hash_tag })` map for every
/// `requires_infra` node in the project. Reads `metadata.images`
/// (the list of buildable image dirs); for each image, hashes the
/// dir, builds the docker image if missing, kind-loads in local
/// dev. Returns the nested map shipped in the `/infra/sync` body.
async fn build_infra_images(
    progress: &Progress,
    project: &weft_compiler::project::Project,
    project_id: &str,
) -> Result<BTreeMap<String, BTreeMap<String, String>>> {
    let (definition, catalog) = crate::hash::load_enriched_project(project)?;

    let mut out: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
    let mut seen_tags: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();

    for node in definition.nodes.iter().filter(|n| n.requires_infra) {
        let Some(entry) = catalog.entry(&node.node_type) else {
            anyhow::bail!(
                "node '{}' has type '{}' which is not in the catalog",
                node.id, node.node_type
            );
        };
        let mut node_tags: BTreeMap<String, String> = BTreeMap::new();
        for image_path in &entry.metadata.images {
            let image_dir = entry.source_dir.join(image_path);
            // The image's local name (Image::Local.name on the
            // InfraSpec) is the path's basename. So
            // `images/bridge` → name `bridge`.
            let image_name = std::path::Path::new(image_path)
                .file_name()
                .and_then(|s| s.to_str())
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "node type '{}' declared invalid image path '{image_path}'",
                        node.node_type
                    )
                })?
                .to_string();
            let dockerfile = image_dir.join("Dockerfile");
            if !dockerfile.is_file() {
                anyhow::bail!(
                    "node type '{}' declares image '{image_name}' but no Dockerfile at {}",
                    node.node_type,
                    dockerfile.display()
                );
            }

            let full_hash = crate::hash::compute_image_hash(&node.node_type, &image_dir)?;
            let short = crate::commands::build::short_hash(&full_hash);
            let tag = format!("weft-infra-{image_name}:{short}");
            node_tags.insert(image_name.clone(), tag.clone());

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
                    &image_dir,
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
        if !node_tags.is_empty() {
            out.insert(node.id.clone(), node_tags);
        }
    }
    Ok(out)
}
