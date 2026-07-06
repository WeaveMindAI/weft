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
use anyhow::Context;

use crate::progress::{ActionVerb, Progress};

#[derive(Clone)]
pub enum InfraAction {
    Start,
    Upgrade,
    Stop,
    Terminate,
    Status,
    /// Cancel in-flight infra work (claimed lifecycle commands halt
    /// between kubectl steps; unclaimed ones cancel outright; the
    /// provisioning execution is interrupted). HALT, not rollback.
    Cancel,
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
    /// Cap on a `wait` drain in seconds (worker replacement inside
    /// sync; the supervisor's stop drain). `None` = the server
    /// default (`DEFAULT_DRAIN_TIMEOUT_SECS`).
    pub drain_timeout: Option<u64>,
}

pub async fn run(ctx: Ctx, action: InfraAction, opts: InfraOpts) -> Result<()> {
    if matches!(action, InfraAction::Status) {
        return infra_status(&ctx).await;
    }
    let verb = match &action {
        InfraAction::Start => ActionVerb::InfraStart,
        InfraAction::Upgrade => ActionVerb::InfraUpgrade,
        InfraAction::Stop => ActionVerb::InfraStop,
        InfraAction::Terminate => ActionVerb::InfraTerminate,
        InfraAction::Cancel => ActionVerb::InfraCancel,
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
        InfraAction::Upgrade => "infra upgraded",
        InfraAction::Stop => "infra stopped",
        InfraAction::Terminate => "infra terminated",
        InfraAction::Cancel => "infra cancel issued",
        InfraAction::NodeStop { .. } => "infra node stopped",
        InfraAction::NodeTerminate { .. } => "infra node terminated",
        InfraAction::Status => unreachable!(),
    };
    match action {
        // Plain Start: just bring DOWN units up (apply skips up units).
        InfraAction::Start => infra_sync(ctx, progress, action, opts).await?,
        // Upgrade: ONE `/infra/sync` POST with `upgrade: true`. The
        // SERVER owns the decomposition (deactivate per the user's
        // spec when active, stop leg, then apply), so every client
        // gets the same upgrade from a single request.
        InfraAction::Upgrade => infra_sync(ctx, progress, action, opts).await?,
        InfraAction::Stop => infra_stop(ctx, progress, opts).await?,
        InfraAction::Terminate => infra_terminate(ctx, progress, opts).await?,
        InfraAction::Cancel => infra_cancel(ctx, progress).await?,
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

/// POST `/infra/cancel`: halt/cancel in-flight infra work. 202 on
/// success (cancel reconciles, never asserts: poll `weft status` for
/// where things settled); 412 when nothing is in flight.
async fn infra_cancel(ctx: &Ctx, progress: &Progress) -> Result<()> {
    let (client, project_id, _name) = super::resolve_project(ctx)?;
    let path = format!("/projects/{project_id}/infra/cancel");
    progress.dispatcher_call_start(&path);
    client.post_empty(&path).await?;
    progress.dispatcher_call_done(serde_json::json!({ "project_id": project_id }));
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
    wait_for_command(progress, &client, &project_id, command_id, verb).await?;
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
    let image_tags = build_infra_images(progress, &handle.plan, &handle.id).await?;
    let verb_label = action_verb_label(&action);

    // A START never deactivates: an active project's triggers stay
    // live while infra comes up (only executions that actually touch
    // the not-yet-running infra fail, loudly, at the node), so it asks
    // no deactivation questions. An UPGRADE of an ACTIVE project takes
    // live infra down (the server's stop leg), so it collects the
    // user's deactivation choice (same picker as `weft deactivate`)
    // and sends it with `upgrade: true`; the server decomposes.
    let upgrade = matches!(action, InfraAction::Upgrade);
    let trigger_deactivation = if upgrade
        && super::deactivate::project_is_active(&handle.client, &handle.id).await?
    {
        Some(super::deactivate::prompt_trigger_deactivation(
            ctx.json(),
            &format!("infra {verb_label}"),
            opts.mode.as_deref(),
            opts.grace,
            opts.running_policy.as_deref(),
            opts.drain_timeout,
        )?)
    } else {
        None
    };

    // SYNC: sync body keys <-> crates/weft-dispatcher/src/api/infra.rs
    // (SyncRequest). All its fields are serde-defaulted, so a key drift here
    // would silently become the default at the receiving end; change both together.
    let mut body = serde_json::Map::new();
    handle.inject_hash_fields(&mut body);
    body.insert("imageHashes".into(), serde_json::to_value(&image_tags)?);
    if upgrade {
        body.insert("upgrade".into(), true.into());
    }
    if let Some(td) = trigger_deactivation {
        body.insert("triggerDeactivation".into(), td);
    }
    // Worker-replacement gating inside sync: the trigger-side
    // running-policy choice answers the same "what about running
    // executions" question, so reuse it; the drain cap rides along.
    if let Some(p) = opts.running_policy.as_deref() {
        body.insert("runningPolicy".into(), p.into());
    }
    if let Some(cap) = opts.drain_timeout {
        body.insert("drainTimeoutSecs".into(), cap.into());
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
        InfraAction::Upgrade => "upgrade",
        InfraAction::Stop => "stop",
        InfraAction::Terminate => "terminate",
        InfraAction::Status => "status",
        InfraAction::Cancel => "cancel",
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
            opts.drain_timeout,
        )?)
    } else {
        None
    };
    let path = format!("/projects/{id}/infra/{verb}");
    let mut body = serde_json::Map::new();
    if let Some(td) = trigger_deactivation {
        body.insert("triggerDeactivation".into(), td);
    }
    if let Some(cap) = opts.drain_timeout {
        body.insert("drainTimeoutSecs".into(), cap.into());
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
    wait_for_command(progress, &client, &id, command_id, verb).await?;
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
/// command complete. Fails loud on a `failed` outcome AND on a
/// `cancelled` one: cancelled means `weft infra cancel` halted this
/// command between steps, so the verb did NOT do what was asked and a
/// zero exit would lie about it (infra is left as-is, per-node state
/// intact). UNBOUNDED: an infra command (stop / terminate / start)
/// can depend on draining in-flight executions, which is a user-facing
/// wait that may legitimately last hours; a hard deadline would turn a
/// correct slow operation into a spurious failure (same rule as
/// `wait_for_drain`). The wait stays legible via an `InfraWait`
/// breadcrumb; Ctrl+C is the recovery. Status-endpoint errors still
/// bubble loudly.
async fn wait_for_command(
    progress: &Progress,
    client: &crate::client::DispatcherClient,
    project_id: &str,
    command_id: i64,
    verb: &str,
) -> Result<()> {
    let interval = std::time::Duration::from_millis(500);
    let breadcrumb_every = std::time::Duration::from_secs(10);
    let start = std::time::Instant::now();
    let mut next_breadcrumb = start + breadcrumb_every;
    loop {
        let resp: serde_json::Value = client
            .get_json(&format!("/projects/{project_id}/infra/commands/{command_id}"))
            .await
            .with_context(|| format!("polling infra {verb} command {command_id}"))?;
        // No `unwrap_or` on the contract fields: a missing `done` must
        // NOT be silently read as "not done" (now an UNBOUNDED wait, it
        // would loop forever), and a missing `outcome` must not be read
        // as success. A wire-contract violation fails loud with the
        // version-mismatch recovery, same posture as the drain wait.
        let done = resp.get("done").and_then(|v| v.as_bool()).ok_or_else(|| {
            anyhow::anyhow!(
                "infra {verb}: command-status response missing or non-bool `done`; a wire \
                 contract violation between this CLI and the dispatcher. Recovery: upgrade the \
                 dispatcher (or this CLI) so the versions match"
            )
        })?;
        if done {
            let outcome = resp.get("outcome").and_then(|v| v.as_str()).ok_or_else(|| {
                anyhow::anyhow!(
                    "infra {verb}: command marked done but the response is missing or has a \
                     non-string `outcome`; wire contract violation, treating it as success would \
                     hide a failed command. Recovery: upgrade the dispatcher or this CLI"
                )
            })?;
            if outcome == "failed" {
                let msg = resp.get("message").and_then(|v| v.as_str()).unwrap_or("unknown error");
                anyhow::bail!("infra {verb} failed: {msg}");
            }
            if outcome == "cancelled" {
                anyhow::bail!(
                    "infra {verb} cancelled (`weft infra cancel`): the operation was halted \
                     between steps and did NOT complete; infra is left as-is (check `weft infra \
                     status`), re-run the verb to finish or act per node"
                );
            }
            return Ok(());
        }
        let now = std::time::Instant::now();
        if now >= next_breadcrumb {
            progress.infra_wait(verb, (now - start).as_secs());
            next_breadcrumb = now + breadcrumb_every;
        }
        tokio::time::sleep(interval).await;
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
/// `requires_infra` node in the project, the nested map shipped in the
/// `/infra/sync` body. The images come straight from the `BuildPlan`
/// `ensure_registered` already produced (kind `Infra`, refs minted by the ONE
/// `TagPolicy`): no second compile, no re-enumeration, no tag re-derivation
/// that could drift from the plan. The CLI's local refs are bare
/// `weft-infra-<name>:<content_hash>` tags it docker-builds + loads onto the
/// node (full content hash, matching the worker tag `weft-worker:<binary_hash>`);
/// the supplied map value is exactly the local tag the supervisor resolves
/// `Image::Local` to.
async fn build_infra_images(
    progress: &Progress,
    plan: &weft_compiler::build_plan::BuildPlan,
    project_id: &str,
) -> Result<BTreeMap<String, BTreeMap<String, String>>> {
    let mut out: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
    let mut seen_tags: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();

    for img in plan.images.iter().filter(|i| i.kind == weft_compiler::build_plan::ImageKind::Infra)
    {
        let (Some(node_id), Some(image_name)) = (&img.node_id, &img.image_name) else {
            anyhow::bail!("planned infra image {} is missing node_id/image_name", img.image_ref);
        };
        let tag = img.image_ref.clone();
        out.entry(node_id.clone()).or_default().insert(image_name.clone(), tag.clone());

        if !seen_tags.insert(tag.clone()) {
            continue;
        }

        let exists = images::image_present(&tag).await.unwrap_or(false);
        if exists {
            progress.build_skip(&tag, "hash_match");
        } else {
            progress.build_start(&tag);
            let label = format!("weft.dev/project={project_id}");
            let dockerfile = img.context_dir.join("Dockerfile");
            crate::commands::build::docker_build_image(
                &tag,
                &dockerfile,
                &img.context_dir,
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
    Ok(out)
}
