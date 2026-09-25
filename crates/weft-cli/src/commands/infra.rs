//! `weft infra start | restart | upgrade | stop | terminate | status`.
//!
//! Start / Restart / Upgrade map to the same dispatcher endpoint
//! (`/projects/{id}/infra/sync`); the label is purely UX. The
//! dispatcher decides per-node skip-vs-apply via the resolved
//! spec hash.
//!
//! Stop / Terminate are direct: they enqueue an
//! `infra_lifecycle_command` row that the supervisor owning the project
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
    /// The doors this project's infrastructure has. Read-only: a door
    /// is part of what a node IS (its endpoint declares it), so there
    /// is nothing here to open or close.
    ListDoors,
    /// Cancel in-flight infra work (claimed lifecycle commands halt
    /// between cluster calls; unclaimed ones cancel outright; the
    /// provisioning execution is interrupted). HALT, not rollback.
    Cancel,
    /// Per-instance verbs. `node` is the instance's PLACE as a person
    /// spells it (`one.db`), checked and written canonical by
    /// `instance_named` before the daemon sees it.
    NodeStop { node: String, force: bool },
    NodeTerminate { node: String },
    /// What the infra containers wrote, read straight off the pods.
    Logs { node: Option<String>, tail: usize, follow: bool },
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
    if matches!(action, InfraAction::ListDoors) {
        return list_doors(&ctx).await;
    }
    if matches!(action, InfraAction::Status) {
        return infra_status(&ctx).await;
    }
    if let InfraAction::Logs { node, tail, follow } = action {
        let node = match node {
            Some(node) => Some(instance_named(&ctx, &node).await?),
            None => None,
        };
        return infra_logs(&ctx, node.as_deref(), tail, follow).await;
    }
    let verb = match &action {
        InfraAction::Start => ActionVerb::InfraStart,
        InfraAction::Upgrade => ActionVerb::InfraUpgrade,
        InfraAction::Stop => ActionVerb::InfraStop,
        InfraAction::Terminate => ActionVerb::InfraTerminate,
        InfraAction::Cancel => ActionVerb::InfraCancel,
        InfraAction::NodeStop { .. } => ActionVerb::InfraNodeStop,
        InfraAction::NodeTerminate { .. } => ActionVerb::InfraNodeTerminate,
        InfraAction::Status | InfraAction::ListDoors | InfraAction::Logs { .. } => unreachable!(),
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
        InfraAction::Status | InfraAction::ListDoors | InfraAction::Logs { .. } => unreachable!(),
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
        // The place is read here, inside the progress wrapper, so a
        // refusal ("names no node") reaches the graph's action bar as
        // this verb's error like every other failure of the verb; the
        // per-node menu is exactly the caller reading those events.
        InfraAction::NodeStop { node, force } => {
            let place = instance_named(ctx, &node).await?;
            infra_node_verb(ctx, progress, &place, "stop", force, &opts).await?
        }
        InfraAction::NodeTerminate { node } => {
            let place = instance_named(ctx, &node).await?;
            infra_node_verb(ctx, progress, &place, "terminate", false, &opts).await?
        }
        InfraAction::Status | InfraAction::ListDoors | InfraAction::Logs { .. } => unreachable!(),
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

/// The infra instance a person named, as the daemon keys it: the place
/// spelling (`one.db`), checked against the program and written
/// canonical, the way `weft wake` does. An instance the program no
/// longer declares (its node deleted, or the include that reached it)
/// is still live until it is stopped or terminated by hand, which is
/// what these verbs are for; so a spelling the program does not know is
/// taken as typed when the daemon lists an instance under it, and
/// refused with the program's answer otherwise.
async fn instance_named(ctx: &Ctx, spelled: &str) -> Result<String> {
    let refusal = match super::node_address_for(ctx, spelled) {
        Ok(place) => return Ok(place),
        Err(refusal) => refusal,
    };
    // The orphan lookup is best effort: the program's refusal is the
    // true answer, and only a live row carrying this exact spelling
    // overrides it. A daemon that cannot be reached, or a listing that
    // cannot be read, must not replace "'x' names no node" with
    // "connection refused".
    let live = async {
        let (client, project_id, _) = super::resolve_project(ctx)?;
        let status = client.get_json(&format!("/projects/{project_id}/infra/status")).await?;
        anyhow::Ok(
            status
                .get("nodes")
                .and_then(|n| n.as_array())
                .is_some_and(|nodes| nodes.iter().any(|n| n.get("node").and_then(|v| v.as_str()) == Some(spelled))),
        )
    }
    .await
    .unwrap_or(false);
    if live {
        return Ok(spelled.to_string());
    }
    Err(refusal)
}

async fn infra_node_verb(
    ctx: &Ctx,
    progress: &Progress,
    place: &str,
    verb: &str,
    force: bool,
    opts: &InfraOpts,
) -> Result<()> {
    let (client, project_id, name) = super::resolve_project(ctx)?;
    let path = format!("/projects/{project_id}/infra/nodes/{place}/{verb}");
    // The running-work answer, read the one way every verb reads it:
    // a person who passes nothing gets cancel, and a cap beside cancel
    // is refused here rather than sent to bound nothing.
    let (running_policy, drain_timeout) = super::ensure::parse_running_choice(
        opts.running_policy.as_deref(),
        opts.drain_timeout,
    )?;
    let mut body = super::ensure::running_choice_fields(running_policy, drain_timeout);
    body.insert("force".into(), serde_json::json!(force));
    let body = serde_json::Value::Object(body);
    progress.drain_wait(&body, drain_timeout);
    progress.dispatcher_call_start(&path);
    // 202 Accepted with { command_id }.
    let issued: serde_json::Value = client.post_json(&path, &body).await?;
    progress.dispatcher_call_done(serde_json::json!({ "project_id": project_id, "node": place }));
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
    let handle = super::ensure::ensure_registered(ctx, progress, weft_compiler::codegen::NodeSet::Full).await?;
    let image_tags =
        build_infra_images(progress, &handle.plan, &handle.id, &handle.client).await?;

    // A START never deactivates: an active project's triggers stay
    // live while infra comes up (only executions that actually touch
    // the not-yet-running infra fail, loudly, at the node), so it asks
    // no deactivation questions. An UPGRADE of an ACTIVE project takes
    // live infra down (the server's stop leg), so it collects the
    // user's deactivation choice (same picker as `weft deactivate`)
    // and sends it with `upgrade: true`; the server decomposes.
    // The running-work pair, read the one way every verb reads it (a
    // misspelt policy is refused here with the CLI's own words, a cap
    // beside cancel is refused rather than sent to bound nothing). It
    // answers the worker replacement inside sync and an upgrade's stop
    // leg; when the picker is shown its answer outranks these fields,
    // and it is built from the same pair.
    let (running_policy, drain_timeout) = super::ensure::parse_running_choice(
        opts.running_policy.as_deref(),
        opts.drain_timeout,
    )?;
    let upgrade = matches!(action, InfraAction::Upgrade);
    let trigger_deactivation = if upgrade
        && super::deactivate::project_is_active(&handle.client, &handle.id).await?
    {
        Some(serde_json::to_value(super::deactivate::prompt_trigger_deactivation(
            ctx.json(),
            opts.mode.as_deref(),
            opts.grace,
            running_policy,
            drain_timeout,
        )?)?)
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
    body.extend(super::ensure::running_choice_fields(running_policy, drain_timeout));
    let path = format!("/projects/{}/infra/sync", handle.id);
    let body = serde_json::Value::Object(body);
    // The one line that says the call may now sit for a while (an
    // upgrade's stop leg, or the worker replacement, draining up to the
    // cap), so a quiet terminal is a wait and not a hang.
    progress.drain_wait(&body, drain_timeout);
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


async fn infra_stop(ctx: &Ctx, progress: &Progress, opts: InfraOpts) -> Result<()> {
    infra_destroy(ctx, progress, opts, "stop").await
}

async fn infra_terminate(ctx: &Ctx, progress: &Progress, opts: InfraOpts) -> Result<()> {
    infra_destroy(ctx, progress, opts, "terminate").await
}

/// Stop / Terminate share this body. Prompts for trigger
/// deactivation only when the project is active; the running-work
/// choice goes on the body either way, because an inactive project
/// can still have executions running on this infra and `wait` is how
/// they get to land first. Waits on the COMMAND's completion (not the
/// rollup): a stop where a NoOp unit stays up never drives the rollup
/// to "stopped", so the command outcome is the only honest done
/// signal.
async fn infra_destroy(
    ctx: &Ctx,
    progress: &Progress,
    opts: InfraOpts,
    verb: &str,
) -> Result<()> {
    // Read the one way every verb reads it, before anything is asked
    // or sent: a misspelt policy or a cap beside cancel is refused here.
    let (running_policy, drain_timeout) = super::ensure::parse_running_choice(
        opts.running_policy.as_deref(),
        opts.drain_timeout,
    )?;
    let (client, id, name) = super::resolve_project(ctx)?;
    let active = super::deactivate::project_is_active(&client, &id).await?;
    let trigger_deactivation = if active {
        Some(serde_json::to_value(super::deactivate::prompt_trigger_deactivation(
            ctx.json(),
            opts.mode.as_deref(),
            opts.grace,
            running_policy,
            drain_timeout,
        )?)?)
    } else {
        None
    };
    let path = format!("/projects/{id}/infra/{verb}");
    let mut body = super::ensure::running_choice_fields(running_policy, drain_timeout);
    if let Some(td) = trigger_deactivation {
        body.insert("triggerDeactivation".into(), td);
    }
    let body = serde_json::Value::Object(body);
    progress.drain_wait(&body, drain_timeout);
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
    let breadcrumb_every = std::time::Duration::from_secs(10);
    let start = std::time::Instant::now();
    let mut next_breadcrumb = start + breadcrumb_every;
    loop {
        // Held by the dispatcher until the command completes or the hold
        // runs out (at most `COMMAND_HOLD`), so the loop asks again at
        // once either way.
        let resp: serde_json::Value = client
            .get_json(&format!(
                "/projects/{project_id}/infra/commands/{command_id}?wait_ms={}",
                COMMAND_HOLD.as_millis()
            ))
            .await
            .with_context(|| format!("waiting on infra {verb} command {command_id}"))?;
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
    }
}

/// How long one wait on an infra command is held by the dispatcher
/// before the CLI asks again: short enough that the breadcrumb above
/// keeps its pace.
const COMMAND_HOLD: std::time::Duration = std::time::Duration::from_secs(10);

/// `weft infra logs [node]`: the infra containers' own output, which is
/// where a service says what went wrong when it went wrong. The pods
/// carry the project and node as labels, so the selector alone finds
/// them; the project's namespace is read off the first match, because
/// `kubectl logs` takes no `--all-namespaces`. A node with no pod
/// (never provisioned, or terminated) is said so by name.
async fn infra_logs(ctx: &Ctx, node: Option<&str>, tail: usize, follow: bool) -> Result<()> {
    let (_client, project_id, _name) = super::resolve_project(ctx)?;
    let mut selector = format!("weft.dev/role=infra,weft.dev/project={project_id}");
    if let Some(node) = node {
        // `node` is the instance's place (`one.db`), the same string the
        // supervisor labelled the pod from; the label carries the
        // label-safe form of it, which `node_label_value` is.
        selector.push_str(&format!(",{}={}", weft_core::infra::NODE_LABEL, weft_core::infra::node_label_value(node)));
    }
    let found = super::daemon::kubectl(&[
        "get", "pods", "--all-namespaces", "-l", &selector,
        "-o", "jsonpath={.items[*].metadata.namespace}",
    ])
    .output()
    .await
    .context("run kubectl get pods")?;
    if !found.status.success() {
        anyhow::bail!(
            "kubectl get pods exited {}: {}",
            found.status,
            String::from_utf8_lossy(&found.stderr).trim()
        );
    }
    let namespaces = String::from_utf8_lossy(&found.stdout);
    let Some(namespace) = namespaces.split_whitespace().next() else {
        match node {
            Some(node) => anyhow::bail!(
                "no pod for infra node `{node}`: it is not provisioned (`weft infra status` \
                 says where each instance stands), or it is not an infra node"
            ),
            None => anyhow::bail!(
                "no infra pod for this project: nothing is provisioned (`weft infra start`)"
            ),
        }
    };
    let tail_arg = format!("--tail={tail}");
    let mut args: Vec<&str> = vec![
        "-n", namespace, "logs", "-l", &selector,
        "--all-containers", "--prefix", &tail_arg,
    ];
    if follow {
        args.push("-f");
    }
    let status = super::daemon::kubectl(&args).status().await.context("run kubectl logs")?;
    if !status.success() {
        anyhow::bail!("kubectl logs exited {status}");
    }
    Ok(())
}

/// The doors this project's infrastructure has, with the address each
/// answers on.
///
/// Read-only, and only ever read: a door is part of what a node IS, so
/// it is declared in the node's own spec and there is nothing here to
/// open or close. This exists because the ADDRESS is not in the source:
/// the port is the apiserver's to allocate, so the only way to know it
/// is to ask the runtime what it handed out.
async fn list_doors(ctx: &Ctx) -> Result<()> {
    let (client, id, _) = super::resolve_project(ctx)?;
    let body: serde_json::Value = client.get_json(&format!("/projects/{id}/infra/doors")).await?;
    if ctx.json_out(&body)? {
        return Ok(());
    }
    let doors = body.get("doors").and_then(|d| d.as_array()).cloned().unwrap_or_default();
    if doors.is_empty() {
        println!(
            "no doors: nothing this project runs is reachable from this machine. A node \
             opens one by declaring it on an endpoint of its own spec; `weft infra status` \
             lists what is running."
        );
        return Ok(());
    }
    for door in doors {
        println!(
            "{}.{}  127.0.0.1:{}",
            door["node"].as_str().unwrap_or(""),
            door["endpoint"].as_str().unwrap_or(""),
            door["port"].as_u64().unwrap_or(0)
        );
    }
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
        // `node` is the instance's place, spelled the way the source
        // reads it (`one.db`): the key and the label are one.
        let node = n.get("node").and_then(|v| v.as_str()).unwrap_or("?");
        let status = n.get("status").and_then(|v| v.as_str()).unwrap_or("?");
        let url = n
            .get("endpoint_url")
            .and_then(|v| v.as_str())
            .unwrap_or("(no endpoint)");
        println!("  {node} [{status}] -> {url}");
        // A public endpoint's outside address: what to hand to whoever
        // calls in (the node declared only its own path).
        if let Some(public) = n.get("public_urls").and_then(|v| v.as_object()) {
            for (endpoint, address) in public {
                if let Some(address) = address.as_str() {
                    println!("    {endpoint} is public at {address}");
                }
            }
        }
    }
}

/// Build the `(place -> { image_name -> hash_tag })` map for every
/// infra INSTANCE in the project, the nested map shipped in the
/// `/infra/sync` body: one entry per place an infra node is at (`db`,
/// or `one.db` and `two.db` for a file included twice), spelled the
/// way its row is keyed, because that is the key the supervisor reads
/// the map by when it applies that instance. The images themselves are
/// the node type's, so every place of one node carries the same tags.
/// The images come straight from the `BuildPlan`
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
    client: &crate::client::DispatcherClient,
) -> Result<BTreeMap<String, BTreeMap<String, String>>> {
    let mut out: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
    let mut seen_tags: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    // What must survive each image's post-ensure GC beyond its fresh
    // tag. Infra tags are content-addressed with no project in them,
    // so another project's running unit can be on a tag this project
    // just moved off; the dispatcher's keep-set (every project's tag
    // map + every recorded unit ref) is the only thing that knows.
    // Fetched once for the whole plan; None + a warning when the
    // answer is unlearnable, and then every GC below is skipped, never
    // guessed.
    let referenced = images::referenced_set_for_gc(client).await;
    // The plan names each image by the node's compiled id; the places
    // that node is at come from the compiled program the plan carries.
    let definition: weft_core::ProjectDefinition = serde_json::from_str(&plan.definition_json)
        .context("read the build plan's compiled program")?;
    let places = weft_core::project::selection::every_place(&definition);

    for img in plan.images.iter().filter(|i| i.kind == weft_compiler::build_plan::ImageKind::Infra)
    {
        let (Some(node_id), Some(image_name)) = (&img.node_id, &img.image_name) else {
            anyhow::bail!("planned infra image {} is missing node_id/image_name", img.image_ref);
        };
        let tag = img.image_ref.clone();
        let mut at_any_place = false;
        for place in places.iter().filter(|place| &place.id == node_id) {
            at_any_place = true;
            let spelled = weft_core::project::address_of(&definition, &place.id, &place.path);
            out.entry(spelled).or_default().insert(image_name.clone(), tag.clone());
        }
        anyhow::ensure!(
            at_any_place,
            "planned infra image {} belongs to '{}', which is at no place in the program",
            img.image_ref,
            weft_core::project::plain_id(node_id)
        );

        if !seen_tags.insert(tag.clone()) {
            continue;
        }

        let exists = images::image_present(&tag).await?;
        if exists {
            progress.build_skip(&tag, "hash_match");
        } else {
            progress.build_start(&tag);
            let label = format!("weft.dev/project={project_id}");
            let dockerfile = img.context_dir.join("Dockerfile");
            crate::images::docker_build(
                &tag,
                &dockerfile,
                &img.context_dir,
                &[label],
                None,
                &[],
            )
            .await?;
            progress.build_done(&tag);
        }
        let cfg = cluster_config();
        if cfg.backend == ClusterBackend::Kind {
            progress.image_push_start(&tag);
            images::kind_load(&cfg.cluster_name, &tag, false).await?;
            progress.image_push_done(&tag);
        }
    }
    // Same content-addressed accumulation as worker images: drop this
    // project's prior tags of each infra repo now that every fresh one
    // is ensured, keeping whatever the dispatcher still references
    // (this project's frozen units, and any other project sharing the
    // tag: the `weft.dev/project` label names whoever built the image
    // first, never every user of it). One GC per REPO over the plan's
    // whole tag set for it, after the loop: two node types shipping an
    // image directory of the same name mint one repo under two hashes,
    // and a per-image GC keyed on one fresh tag would delete the
    // sibling built a moment ago.
    let mut fresh_by_repo: BTreeMap<&str, Vec<String>> = BTreeMap::new();
    for tag in &seen_tags {
        let (repo, _) = images::ref_repo_tag(tag)?;
        fresh_by_repo.entry(repo).or_default().push(tag.clone());
    }
    for fresh in fresh_by_repo.values() {
        crate::commands::build::gc_stale_images(
            fresh,
            &[format!("weft.dev/project={project_id}")],
            referenced.as_ref(),
        )
        .await;
    }
    Ok(out)
}
