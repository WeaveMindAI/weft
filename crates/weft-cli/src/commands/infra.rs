//! `weft infra start | restart | upgrade | stop | terminate | status`.
//!
//! Start / Restart map to `/projects/{id}/infra/sync`, which answers
//! once the infra is up. Upgrade maps to `/projects/{id}/infra/upgrade`,
//! which issues a command the dispatcher runs on its own (stop leg, then
//! the start) and answers with its id at once; the CLI follows the
//! command to its outcome. The dispatcher decides per-node
//! skip-vs-apply via the resolved spec hash.
//!
//! Stop / Terminate are direct: they enqueue an
//! `infra_lifecycle_command` row that the supervisor owning the project
//! claims and executes.


use anyhow::Result;

use super::Ctx;
use weft_core::infra::wire::{
    CommandOutcome, CommandStatus, CopyRef, DoorsResponse, InfraLogs, InfraStatus, LifecycleCommandIssued, LogBlock,
    PerNodeRequest, StopRequest, SyncRequest, UpgradeRequest,
};
use anyhow::Context;

use crate::progress::{ActionVerb, Progress};

#[derive(Clone)]
pub enum InfraAction {
    Start,
    Upgrade,
    Stop,
    /// `yes`: the confirmation, answered up front (`--yes`).
    Terminate { yes: bool },
    Status,
    /// The doors this project's infrastructure has. Read-only: a door
    /// is part of what a node IS (its endpoint declares it), so there
    /// is nothing here to open or close.
    ListDoors,
    /// Cancel in-flight infra work (claimed lifecycle commands halt
    /// between platform calls; unclaimed ones cancel outright; the
    /// provisioning execution is interrupted). HALT, not rollback.
    Cancel,
    /// Per-node verbs. `node` is the node's PLACE as a person spells it
    /// (`one.db`), checked and written canonical by `place_named`
    /// before the daemon sees it.
    NodeStop { node: String, force: bool },
    /// `yes`: the confirmation, answered up front (`--yes`).
    NodeTerminate { node: String, yes: bool },
    /// What the infra containers wrote, read straight off the processes.
    Logs { node: Option<String>, tail: usize, follow: bool },
}

/// Trigger-deactivation choices for the infra verbs that take triggers
/// down (Stop, Terminate, Upgrade). Mirrors the shared
/// `prompt_trigger_deactivation` argument shape.
///
/// All fields are optional at the CLI surface. A choice given with
/// `--mode` or `--grace` is always sent; without one, the dispatcher
/// says when it needs one (a trigger reading this infra is on), and
/// then a TTY is asked and `--json` or a script fails naming the flags
/// (see `post_with_trigger_choice`). There is NO auto-reactivate: a user-triggered
/// upgrade leaves the project deactivated, and the user clicks Activate
/// when ready. Automatic reactivation belongs only to the autonomous
/// health-recovery path (deactivate -> fix infra -> reactivate with no
/// human present), not to a verb the user invoked themselves.
#[derive(Default, Clone)]
pub struct InfraOpts {
    pub trigger: super::deactivate::TriggerChoiceFlags,
    /// An instance's copies (`--instance`); the shared infra when absent.
    pub instance: Option<weft_core::instance::InstanceId>,
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
            Some(node) => Some(place_named(&ctx, &node).await?),
            None => None,
        };
        return infra_logs(&ctx, node.as_deref(), tail, follow).await;
    }
    // Terminating deletes resources, stored data included unless a node
    // keeps it, so it never runs on a bare command: a terminal is asked,
    // and a script says `--yes`.
    let copies_of = |what: &str| match &opts.instance {
        Some(instance) => format!("instance '{instance}''s copies of {what}"),
        None => what.to_string(),
    };
    let unconfirmed = match &action {
        InfraAction::Terminate { yes: false } => Some(copies_of("the program's shared infra")),
        InfraAction::NodeTerminate { node, yes: false } => Some(copies_of(&format!("infra node '{node}'"))),
        _ => None,
    };
    if let Some(whose) = unconfirmed {
        if !confirm_terminate(&ctx, &whose)? {
            println!("aborted");
            return Ok(());
        }
    }
    let verb = match &action {
        InfraAction::Start => ActionVerb::InfraStart,
        InfraAction::Upgrade => ActionVerb::InfraUpgrade,
        InfraAction::Stop => ActionVerb::InfraStop,
        InfraAction::Terminate { .. } => ActionVerb::InfraTerminate,
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
        InfraAction::Terminate { .. } => "infra terminated",
        InfraAction::Cancel => "infra cancel issued",
        InfraAction::NodeStop { .. } => "infra node stopped",
        InfraAction::NodeTerminate { .. } => "infra node terminated",
        InfraAction::Status | InfraAction::ListDoors | InfraAction::Logs { .. } => unreachable!(),
    };
    match action {
        // Plain Start: just bring DOWN units up (apply skips up units).
        InfraAction::Start => infra_sync(ctx, progress, action, opts).await?,
        // Upgrade: ONE `/infra/upgrade` POST. The SERVER owns the
        // decomposition (deactivate per the user's spec when active,
        // stop leg, then apply) and runs it as a command this follows,
        // so every client gets the same upgrade from a single request.
        InfraAction::Upgrade => infra_sync(ctx, progress, action, opts).await?,
        InfraAction::Stop => infra_stop(ctx, progress, opts).await?,
        InfraAction::Terminate { .. } => infra_terminate(ctx, progress, opts).await?,
        InfraAction::Cancel => infra_cancel(ctx, progress, opts.instance.as_ref()).await?,
        // The place is read here, inside the progress wrapper, so a
        // refusal ("names no node") reaches the graph's action bar as
        // this verb's error like every other failure of the verb; the
        // per-node menu is exactly the caller reading those events.
        InfraAction::NodeStop { node, force } => {
            let place = place_named(ctx, &node).await?;
            infra_node_verb(ctx, progress, &place, "stop", force, &opts).await?
        }
        InfraAction::NodeTerminate { node, .. } => {
            let place = place_named(ctx, &node).await?;
            infra_node_verb(ctx, progress, &place, "terminate", false, &opts).await?
        }
        InfraAction::Status | InfraAction::ListDoors | InfraAction::Logs { .. } => unreachable!(),
    }
    progress.complete(summary);
    Ok(())
}

/// POST `/infra/cancel`: halt/cancel in-flight infra work. 202 on
/// success (cancel reconciles, never asserts: poll `weft status` for
/// where things settled); 412 when nothing is in flight. With an
/// instance, only that instance's work is cancelled.
async fn infra_cancel(ctx: &Ctx, progress: &Progress, instance: Option<&weft_core::instance::InstanceId>) -> Result<()> {
    let (client, project_id, _name) = super::resolve_project(ctx)?;
    let path = match instance {
        Some(instance) => format!("/projects/{project_id}/infra/cancel?instance={instance}"),
        None => format!("/projects/{project_id}/infra/cancel"),
    };
    progress.dispatcher_call_start(&path);
    client.post_empty(&path).await?;
    progress.dispatcher_call_done(serde_json::json!({ "project_id": project_id }));
    Ok(())
}

/// The infra node a person named, as the daemon keys it: the place
/// spelling (`one.db`), checked against the program and written
/// canonical, the way `weft wake` does. A node the program no
/// longer declares (its node deleted, or the include that reached it)
/// is still live until it is stopped or terminated by hand, which is
/// what these verbs are for; so a spelling the program does not know is
/// taken as typed when the daemon lists a node under it, and
/// refused with the program's answer otherwise.
pub(crate) async fn place_named(ctx: &Ctx, spelled: &str) -> Result<String> {
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
        let status = infra_status_of(&client, &project_id).await?;
        anyhow::Ok(status.nodes.iter().any(|n| n.node == spelled))
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
        opts.trigger.running_policy.as_deref(),
        opts.trigger.drain_timeout,
    )?;
    let body = PerNodeRequest {
        running: super::ensure::running_choice(running_policy, drain_timeout),
        force,
        instance: opts.instance.clone(),
    };
    progress.drain_wait(running_policy, drain_timeout);
    progress.dispatcher_call_start(&path);
    // 202 Accepted with the command it runs as.
    let issued = issued_command(client.post_json(&path, &serde_json::to_value(&body)?).await?, verb)?;
    progress.dispatcher_call_done(serde_json::json!({ "project_id": project_id, "node": place }));
    let command_id = issued.command_id;
    // Wait on the command, not a per-node status: a NoOp unit staying
    // up means the node never reaches "stopped", so the command
    // outcome is the honest done signal (and a force-stop completing
    // is what we actually want to wait for).
    wait_for_command(progress, &client, &project_id, command_id, verb).await?;
    if !ctx.json() {
        print_status(&name, &project_id, &infra_status_of(&client, &project_id).await?);
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
    // The build registers the infra images every place runs alongside the
    // program; the sync names that build and applies it.
    let handle = super::ensure::ensure_registered(ctx, progress, weft_core::builds::NodeSet::Full).await?;

    // A START never deactivates: an active project's triggers stay
    // live while infra comes up (only executions that actually touch
    // the not-yet-running infra fail, loudly, at the node), so it asks
    // no deactivation questions. An UPGRADE of an ACTIVE project takes
    // live infra down (the server's stop leg), so it collects the
    // user's deactivation choice (same picker as `weft deactivate`)
    // and sends it to `/infra/upgrade`; the server decomposes.
    // The running-work pair, read the one way every verb reads it (a
    // misspelt policy is refused here with the CLI's own words, a cap
    // beside cancel is refused rather than sent to bound nothing). It
    // answers the worker replacement inside sync and an upgrade's stop
    // leg; when the picker is shown its answer outranks these fields,
    // and it is built from the same pair.
    let (running_policy, drain_timeout) = super::ensure::parse_running_choice(
        opts.trigger.running_policy.as_deref(),
        opts.trigger.drain_timeout,
    )?;
    // Only an upgrade carries the choice (the dispatcher refuses it on a
    // plain start), and only the dispatcher knows whether a trigger
    // reading this infra is on, the program's or an instance's: a given
    // choice always goes, and otherwise `post_with_trigger_choice` asks
    // or names the flags when the dispatcher says it needs one.
    let upgrade = matches!(action, InfraAction::Upgrade);
    let given = if upgrade {
        super::deactivate::given_trigger_deactivation(
            ctx.json(),
            opts.trigger.mode.as_deref(),
            opts.trigger.grace,
            running_policy,
            drain_timeout,
        )?
    } else {
        None
    };

    let sync = SyncRequest {
        build: handle.built.named(),
        running: super::ensure::running_choice(running_policy, drain_timeout),
        instance: opts.instance.clone(),
        nodes: Vec::new(),
    };
    let path = format!("/projects/{}/infra/{}", handle.id, if upgrade { "upgrade" } else { "sync" });
    // The one line that says the call may now sit for a while (an
    // upgrade's stop leg, or the worker replacement, draining up to the
    // cap), so a quiet terminal is a wait and not a hang.
    progress.drain_wait(running_policy, drain_timeout);
    // The places the build registered an infra image for: the ones the
    // sync provisions.
    let node_ids: Vec<String> = handle.built.infra_images.keys().cloned().collect();
    progress.infra_provision_start(&node_ids);
    progress.dispatcher_call_start(&path);
    // An upgrade answers 202 with the command the dispatcher runs it
    // as; its outcome is the upgrade's, and the infra status after it
    // is what is shown. Only an upgrade carries the trigger choice. A
    // sync answers with the status itself.
    let resp = if upgrade {
        let body = UpgradeRequest { sync, trigger_deactivation: None };
        let answer = super::deactivate::post_with_trigger_choice(
            &handle.client,
            &path,
            body,
            ctx.json(),
            given,
            running_policy,
            drain_timeout,
        )
        .await?;
        progress.dispatcher_call_done(serde_json::json!({ "project_id": handle.id }));
        let issued = issued_command(answer, "upgrade")?;
        wait_for_command(progress, &handle.client, &handle.id, issued.command_id, "upgrade").await?;
        infra_status_of(&handle.client, &handle.id).await?
    } else {
        let answer = handle.client.post_json(&path, &serde_json::to_value(&sync)?).await?;
        progress.dispatcher_call_done(serde_json::json!({ "project_id": handle.id }));
        serde_json::from_value(answer).context("read the infra status the sync answered")?
    };
    progress.infra_provision_done();
    // A copy the host runs differently from what it asked (a GPU kind a
    // local install cannot choose) is told here, to the person starting it.
    for note in resp.host_notes() {
        progress.warn(note);
    }
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

/// Stop / Terminate share this body. The trigger-deactivation choice
/// goes along when it was given; otherwise the dispatcher, which knows
/// whether a trigger reading this infra is on (the program's or a
/// instance's), asks for it and `post_with_trigger_choice` prompts or
/// names the flags. The running-work choice goes on the body either
/// way, because with no trigger on there can still be executions
/// running on this infra and `wait` is how they get to land first. Waits on the COMMAND's completion (not the
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
        opts.trigger.running_policy.as_deref(),
        opts.trigger.drain_timeout,
    )?;
    let given = super::deactivate::given_trigger_deactivation(
        ctx.json(),
        opts.trigger.mode.as_deref(),
        opts.trigger.grace,
        running_policy,
        drain_timeout,
    )?;
    let (client, id, name) = super::resolve_project(ctx)?;
    let path = format!("/projects/{id}/infra/{verb}");
    let body = StopRequest {
        trigger_deactivation: None,
        running: super::ensure::running_choice(running_policy, drain_timeout),
        instance: opts.instance.clone(),
    };
    progress.drain_wait(running_policy, drain_timeout);
    progress.dispatcher_call_start(&path);
    // 202 Accepted with the command it runs as.
    let answer =
        super::deactivate::post_with_trigger_choice(&client, &path, body, ctx.json(), given, running_policy, drain_timeout)
            .await?;
    progress.dispatcher_call_done(serde_json::json!({ "project_id": id }));
    let issued = issued_command(answer, verb)?;
    wait_for_command(progress, &client, &id, issued.command_id, verb).await?;
    if !ctx.json() {
        print_status(&name, &id, &infra_status_of(&client, &id).await?);
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
        let resp = client
            .get_json(&format!(
                "/projects/{project_id}/infra/commands/{command_id}?wait_ms={}",
                COMMAND_HOLD.as_millis()
            ))
            .await
            .with_context(|| format!("waiting on infra {verb} command {command_id}"))?;
        // A status that does not read fails loud: a missing `done` read
        // as "not done" would loop forever (the wait is unbounded).
        let status: CommandStatus = serde_json::from_value(resp).map_err(|e| {
            anyhow::anyhow!(
                "infra {verb}: the command status does not read ({e}); upgrade the dispatcher \
                 (or this CLI) so the versions match"
            )
        })?;
        if status.done {
            match status.outcome {
                Some(CommandOutcome::Succeeded) => return Ok(()),
                Some(CommandOutcome::Failed) => {
                    let msg = status.message.as_deref().unwrap_or("unknown error");
                    anyhow::bail!("infra {verb} failed: {msg}");
                }
                Some(CommandOutcome::Cancelled) => anyhow::bail!(
                    "infra {verb} cancelled (`weft infra cancel`): the operation was halted \
                     between steps and did NOT complete; infra is left as-is (check `weft infra \
                     status`), re-run the verb to finish or act per node"
                ),
                // Treating a done command with no outcome as success
                // would hide a failed one.
                None => anyhow::bail!(
                    "infra {verb}: the command is marked done with no outcome; upgrade the \
                     dispatcher or this CLI so the versions match"
                ),
            }
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
/// where a service says what went wrong when it went wrong. With
/// `--follow`, what they write next, asked for every couple of seconds
/// until interrupted: each answer carries a cursor that names, per
/// container, the last line delivered, so no line is printed twice or
/// skipped between polls.
async fn infra_logs(ctx: &Ctx, node: Option<&str>, tail: usize, follow: bool) -> Result<()> {
    let (client, project_id, _name) = super::resolve_project(ctx)?;
    let encode = |s: &str| url::form_urlencoded::byte_serialize(s.as_bytes()).collect::<String>();
    let mut after: Option<String> = None;
    loop {
        let mut path = format!("/projects/{project_id}/infra/logs?tail={tail}");
        if let Some(node) = node {
            path.push_str(&format!("&node={}", encode(node)));
        }
        if let Some(cursor) = &after {
            path.push_str(&format!("&after={}", encode(cursor)));
        }
        let logs: InfraLogs =
            serde_json::from_value(client.get_json(&path).await?).context("read the infra logs")?;
        print!("{}", format_log_blocks(&logs.blocks));
        if !follow {
            return Ok(());
        }
        after = Some(serde_json::to_string(&logs.cursor)?);
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
}

/// Each container's lines headed by what wrote them.
fn format_log_blocks(blocks: &[LogBlock]) -> String {
    let mut out = String::new();
    for block in blocks {
        out.push_str(&format!(
            "=== {} {} {} ===\n",
            copy_label(&block.copy),
            block.unit,
            block.stream.source
        ));
        for line in &block.stream.lines {
            out.push_str(&line.text);
            out.push('\n');
        }
    }
    out
}

/// A copy the way the source spells its node, with whose copy it is.
fn copy_label(copy: &CopyRef) -> String {
    match &copy.instance {
        Some(instance) => format!("{} (instance {instance})", copy.node),
        None => copy.node.clone(),
    }
}

/// The doors this project's infrastructure has, with the address each
/// answers on.
///
/// Read-only, and only ever read: a door is part of what a node IS, so
/// it is declared in the node's own spec and there is nothing here to
/// open or close. This exists because the ADDRESS is not in the source:
/// the host picks it when it applies the unit, so the only way to know
/// it is to ask the runtime what it handed out.
async fn list_doors(ctx: &Ctx) -> Result<()> {
    let (client, id, _) = super::resolve_project(ctx)?;
    let body: serde_json::Value = client.get_json(&format!("/projects/{id}/infra/doors")).await?;
    if ctx.json_out(&body)? {
        return Ok(());
    }
    let answer: DoorsResponse = serde_json::from_value(body).context("read the doors")?;
    print!("{}", format_doors(&answer));
    Ok(())
}

fn format_doors(answer: &DoorsResponse) -> String {
    if answer.doors.is_empty() && answer.applying.is_empty() {
        return "no doors: nothing this project runs is reachable from outside its instances. A node \
                opens one by declaring it on an endpoint of its own spec; `weft infra status` \
                lists what is running.\n"
            .to_string();
    }
    let mut out = String::new();
    for door in &answer.doors {
        let door_name = CopyRef { node: format!("{}.{}", door.copy.node, door.endpoint), instance: door.copy.instance.clone() };
        out.push_str(&format!("{}  {}\n", copy_label(&door_name), door.address));
    }
    for copy in &answer.applying {
        out.push_str(&format!(
            "{}  (still being applied: any door it declares gets its address once it runs; ask again then)\n",
            copy_label(copy)
        ));
    }
    out
}

/// Asks before a terminate deletes `whose` resources. Off a terminal or
/// under `--json` there is nobody to ask, so the command refuses and
/// names `--yes`. False when the person declined.
fn confirm_terminate(ctx: &Ctx, whose: &str) -> Result<bool> {
    if ctx.json() || !crate::prompt::is_interactive() {
        anyhow::bail!("terminating deletes every resource of {whose}; pass --yes");
    }
    println!("About to terminate {whose}: every resource is deleted, stored data included unless the node keeps it.");
    crate::prompt::confirm("Type 'yes' to confirm: ", "--yes")
}

async fn infra_status(ctx: &Ctx) -> Result<()> {
    let (client, id, name) = super::resolve_project(ctx)?;
    let status = infra_status_of(&client, &id).await?;
    if ctx.json_out(&status)? {
        return Ok(());
    }
    print_status(&name, &id, &status);
    for note in status.host_notes() {
        eprintln!("warning: {note}");
    }
    Ok(())
}

/// Every infra copy of the project, and its state.
async fn infra_status_of(client: &crate::client::DispatcherClient, project_id: &str) -> Result<InfraStatus> {
    serde_json::from_value(client.get_json(&format!("/projects/{project_id}/infra/status")).await?)
        .context("read the infra status")
}

/// The command a lifecycle verb was enqueued as (its 202 answer).
fn issued_command(answer: serde_json::Value, verb: &str) -> Result<LifecycleCommandIssued> {
    serde_json::from_value(answer).with_context(|| format!("infra {verb}: read the command it was issued as"))
}


fn print_status(name: &str, id: &str, status: &InfraStatus) {
    let nodes = &status.nodes;
    // The copies that exist, not the nodes the program declares (`weft
    // status` lists those, started or not).
    if nodes.is_empty() {
        println!("no infra copy exists in this project (none started, or all terminated)");
        return;
    }
    println!("infra for {name} ({id}):");
    for n in nodes {
        // `node` is the node's place, spelled the way the source
        // reads it (`one.db`): the key and the label are one.
        // An instance's copy of a `@per_instance` node is its own row.
        let node = match &n.instance {
            Some(instance) => format!("{} (instance {instance})", n.node),
            None => n.node.clone(),
        };
        let url = n.endpoint_url.as_deref().unwrap_or("(no endpoint)");
        println!("  {node} [{}] -> {url}", n.status);
        // A public endpoint's outside address: what to hand to whoever
        // calls in (the node declared only its own path).
        for (endpoint, address) in &n.public_urls {
            println!("    {endpoint} is public at {address}");
        }
    }
}



#[cfg(test)]
mod tests {
    use super::*;
    use weft_core::infra::wire::{Door, LogLine, LogMark, LogStream, Pipe};

    fn instance(id: &str) -> Option<weft_core::instance::InstanceId> {
        Some(weft_core::instance::InstanceId::new(id).unwrap())
    }

    #[test]
    fn doors_name_whose_copy_and_the_copies_still_applying() {
        let answer = DoorsResponse {
            doors: vec![
                Door { copy: CopyRef { node: "db".into(), instance: None }, endpoint: "pg".into(), address: "10.0.0.2:5432".into() },
                Door { copy: CopyRef { node: "db".into(), instance: instance("ann") }, endpoint: "pg".into(), address: "10.0.0.3:5432".into() },
            ],
            applying: vec![CopyRef { node: "cache".into(), instance: None }],
        };
        let out = format_doors(&answer);
        assert!(out.contains("db.pg  10.0.0.2:5432"), "{out}");
        assert!(out.contains("db.pg (instance ann)  10.0.0.3:5432"), "{out}");
        assert!(out.contains("cache  (still being applied"), "{out}");
    }

    #[test]
    fn log_blocks_are_headed_by_what_wrote_them() {
        let blocks = vec![LogBlock {
            copy: CopyRef { node: "db".into(), instance: instance("ann") },
            unit: "main".into(),
            stream: LogStream {
                source: "app".into(),
                lines: vec![LogLine { at: Default::default(), pipe: Pipe::Stdout, text: "ready".into() }],
                mark: LogMark::start(Default::default()),
            },
        }];
        assert_eq!(format_log_blocks(&blocks), "=== db (instance ann) main app ===\nready\n");
    }
}
