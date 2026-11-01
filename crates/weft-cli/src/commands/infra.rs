//! `weft infra start | upgrade | stop | terminate | status | ...`.
//!
//! Start maps to `/projects/{id}/infra/sync`, which answers once the
//! infra is up. Upgrade maps to `/projects/{id}/infra/upgrade`, which
//! issues a command per owner the dispatcher runs on its own (stop leg,
//! then the start) and answers with their ids at once; the CLI follows
//! each to its outcome. Stop and terminate issue a command per owner the
//! supervisor runs.
//!
//! Every one of them may name infra nodes (`--infra`, repeatable: every
//! one of the owners' kind when none is) and whose copies (`--instance`,
//! repeatable, or `--all-instances`; the shared ones when neither is).
//! Stop, terminate and upgrade first take down the triggers whose cut
//! contains the copies, as `--door`, `--held` and `--running` say
//! (`weft_core::take_down`); they come back on by themselves once the
//! copies run again.

use anyhow::Result;

use super::Ctx;
use weft_core::infra::InfraNodeStatus;
use weft_core::infra::wire::{
    CommandOutcome, CommandStatus, CopyRef, DoorsResponse, InfraLogs, InfraStatus, LifecycleCommandIssued, LogBlock,
    StopRequest, SyncRequest, UpgradeRequest,
};
use weft_core::instance::InstanceTarget;
use weft_core::TakeDownChoice;
use anyhow::Context;

use crate::progress::{ActionVerb, Progress};

#[derive(Clone)]
pub enum InfraAction {
    Start,
    Upgrade,
    /// `force`: every unit scaled to zero, its `on_stop` ignored.
    Stop { force: bool },
    /// `yes`: the confirmation, answered up front (`--yes`).
    Terminate { yes: bool },
    Status,
    /// The doors this project's infrastructure has. Read-only: a door
    /// is part of what a node IS (its endpoint declares it), so there
    /// is nothing here to open or close.
    ListDoors,
    /// Cancel in-flight infra work of one owner's copies (claimed
    /// lifecycle commands halt between platform calls; unclaimed ones
    /// cancel outright; the provisioning run is interrupted). HALT, not
    /// rollback.
    Cancel { instance: Option<weft_core::instance::InstanceId> },
    /// Ask the supervisor to look at the infra again.
    Look,
    /// What the infra containers wrote, read straight off the processes.
    Logs { node: Option<String>, tail: usize, follow: bool },
}

/// What a lifecycle verb acts on, and how the triggers whose cut contains
/// it go down.
#[derive(Default, Clone)]
pub struct InfraOpts {
    /// The infra nodes as a person spells them (`--infra`); every one of
    /// the owners' kind when empty.
    pub nodes: Vec<String>,
    /// Whose copies (`--instance`, `--all-instances`).
    pub instances: InstanceTarget,
    /// How the triggers whose cut contains the copies go down (stop,
    /// terminate and upgrade).
    pub take_down: TakeDownChoice,
}

pub async fn run(ctx: Ctx, action: InfraAction, opts: InfraOpts) -> Result<()> {
    match action {
        InfraAction::ListDoors => return list_doors(&ctx).await,
        InfraAction::Status => return infra_status(&ctx).await,
        InfraAction::Look => return infra_look(&ctx).await,
        InfraAction::Logs { node, tail, follow } => {
            let node = match node {
                Some(node) => Some(place_named(&ctx, &node).await?),
                None => None,
            };
            return infra_logs(&ctx, node.as_deref(), tail, follow).await;
        }
        _ => {}
    }
    // Refused before anything is asked or built: a contradiction in the
    // answers, or answers on a verb that takes nothing down.
    if matches!(action, InfraAction::Stop { .. } | InfraAction::Terminate { .. } | InfraAction::Upgrade) {
        opts.take_down.resolve().map_err(|why| anyhow::anyhow!("{why}"))?;
    } else if opts.take_down != TakeDownChoice::default() {
        anyhow::bail!("--door, --door-for, --held and --running say how triggers go down, and this verb takes none down");
    }
    // Terminating deletes resources, stored data included unless a node
    // keeps it, so it never runs on a bare command: a terminal is asked,
    // and a script says `--yes`.
    if let InfraAction::Terminate { yes: false } = &action {
        if !confirm_terminate(&ctx, &whose_infra(&opts))? {
            println!("aborted");
            return Ok(());
        }
    }
    let verb = match &action {
        InfraAction::Start => ActionVerb::InfraStart,
        InfraAction::Upgrade => ActionVerb::InfraUpgrade,
        InfraAction::Stop { .. } => ActionVerb::InfraStop,
        InfraAction::Terminate { .. } => ActionVerb::InfraTerminate,
        InfraAction::Cancel { .. } => ActionVerb::InfraCancel,
        InfraAction::Status | InfraAction::ListDoors | InfraAction::Look | InfraAction::Logs { .. } => unreachable!(),
    };
    let ctx_inner = ctx.clone();
    ctx.with_progress(verb, |progress| async move {
        run_inner(&ctx_inner, &progress, action, opts).await
    })
    .await
}

/// The infra a terminate would delete, in the confirmation's words.
fn whose_infra(opts: &InfraOpts) -> String {
    let what = match opts.nodes.as_slice() {
        [] => "the infra".to_string(),
        [one] => format!("infra node '{one}'"),
        many => format!("infra nodes {}", many.iter().map(|n| format!("'{n}'")).collect::<Vec<_>>().join(", ")),
    };
    match &opts.instances {
        InstanceTarget::Shared => format!("{what} the program shares"),
        InstanceTarget::Named(ids) => {
            format!("{what} of instance {}", ids.iter().map(|i| format!("'{i}'")).collect::<Vec<_>>().join(", "))
        }
        InstanceTarget::EveryInstance => format!("{what} of every instance"),
        InstanceTarget::Everything => format!("{what} the program shares and every instance's"),
    }
}

async fn run_inner(
    ctx: &Ctx,
    progress: &Progress,
    action: InfraAction,
    mut opts: InfraOpts,
) -> Result<()> {
    // The places are read here, inside the progress wrapper, so a refusal
    // ("names no node") reaches the graph's action bar as this verb's
    // error like every other failure of the verb.
    let mut places = Vec::new();
    for node in &opts.nodes {
        places.push(place_named(ctx, node).await?);
    }
    opts.nodes = places;
    // Run the action's work. The command bodies emit phase progress
    // (dispatcher_call_start/done, provision phases) but NOT a terminal
    // `complete`: we emit exactly one `complete` here, after the whole
    // action settles. This is what keeps the action-bar overlay held
    // for the FULL verb, including multi-phase Upgrade (stop + start);
    // a per-phase `complete` would clear the overlay mid-upgrade and
    // let the bar flicker through intermediate interactive states.
    let (done, report) = match action {
        InfraAction::Start => ("infra started", infra_sync(ctx, progress, false, opts).await?),
        InfraAction::Upgrade => ("infra upgraded", infra_sync(ctx, progress, true, opts).await?),
        InfraAction::Stop { force } => ("infra stopped", infra_destroy(ctx, progress, opts, "stop", force).await?),
        InfraAction::Terminate { .. } => ("infra terminated", infra_destroy(ctx, progress, opts, "terminate", false).await?),
        InfraAction::Cancel { instance } => {
            infra_cancel(ctx, progress, instance.as_ref()).await?;
            ("infra cancel issued", None)
        }
        InfraAction::Status | InfraAction::ListDoors | InfraAction::Look | InfraAction::Logs { .. } => unreachable!(),
    };
    match report.filter(|report| !report.triggers.is_empty()) {
        Some(report) => progress.complete(&format!("{done}; first {}", report.describe())),
        None => progress.complete(done),
    }
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
    progress.dispatcher_call_done(serde_json::json!({ "projectId": project_id }));
    Ok(())
}

async fn infra_look(ctx: &Ctx) -> Result<()> {
    let (client, project_id, _name) = super::resolve_project(ctx)?;
    client.post_empty(&format!("/projects/{project_id}/infra/look")).await?;
    if ctx.json_out(&serde_json::json!({ "projectId": project_id }))? {
        return Ok(());
    }
    println!("Asked the supervisor to look at the infra again; `{}` shows what it finds.", super::weft_on(ctx.on(), "status"));
    Ok(())
}

/// The infra node a person named, as the daemon keys it: the place
/// spelling (`one.db`), checked against the folder's program and written
/// canonical. Outside a project there is no program to check against,
/// and the daemon checks every node it is handed anyway, so the spelling
/// goes as it is. A node the program no longer declares (its node
/// deleted, or the include that reached it) is still live until it is
/// stopped or terminated by hand, which is what these verbs are for; so a
/// spelling the program does not know is taken as typed when the daemon
/// lists a node under it, and refused with the program's answer otherwise.
pub(crate) async fn place_named(ctx: &Ctx, spelled: &str) -> Result<String> {
    let Some(project) = ctx.project_here()? else { return Ok(spelled.to_string()) };
    let checked = weft_compiler::hash::load_program(project).and_then(|(definition, _)| {
        let (id, call_path) = super::resolve_node(project, &definition, spelled)?;
        Ok(weft_core::project::address_of(&definition, &id, &call_path))
    });
    let refusal = match checked {
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

/// Start (`upgrade` false) or upgrade the copies `opts` names. A start
/// starts what is not running (with `--infra`, those nodes and what they
/// need upstream), and the triggers a stop of it took down come back by
/// themselves once it runs; an upgrade builds, then takes down the
/// triggers whose cut contains what runs on another build, and cycles it
/// onto this one, touching nothing else. What the upgrade's take-down did
/// is answered for the summary.
async fn infra_sync(
    ctx: &Ctx,
    progress: &Progress,
    upgrade: bool,
    opts: InfraOpts,
) -> Result<Option<weft_core::deactivation::TakeDownReport>> {
    // The build registers the infra images every place runs alongside the
    // program; the sync names that build and applies it.
    let handle = super::ensure::ensure_registered(ctx, progress, weft_core::builds::NodeSet::Full).await?;
    let sync = SyncRequest { build: handle.built.named(), instances: opts.instances.clone(), nodes: opts.nodes.clone() };
    let path = format!("/projects/{}/infra/{}", handle.id, if upgrade { "upgrade" } else { "sync" });
    // The places the build registered an infra image for: the ones the
    // sync provisions.
    let node_ids: Vec<String> = match opts.nodes.is_empty() {
        true => handle.built.infra_images.keys().cloned().collect(),
        false => opts.nodes.clone(),
    };
    progress.infra_provision_start(&node_ids);
    let mut report = None;
    // An upgrade answers 202 with the commands the dispatcher runs it as;
    // their outcomes are the upgrade's, and the infra status after them is
    // what is shown. A sync answers with the status itself.
    let resp = if upgrade {
        let body = UpgradeRequest { sync, take_down: opts.take_down };
        if let Ok(take_down) = opts.take_down.resolve() {
            progress.drain_wait(take_down.running, crate::progress::DrainOn::Infra);
        }
        progress.dispatcher_call_start(&path);
        let issued = issued_command(handle.client.post_json(&path, &serde_json::to_value(&body)?).await?, "upgrade")?;
        progress.dispatcher_call_done(serde_json::json!({ "projectId": handle.id }));
        for command_id in &issued.command_ids {
            wait_for_command(progress, ctx.on(), &handle.client, &handle.id, *command_id, "upgrade").await?;
        }
        report = Some(issued.report);
        infra_status_of(&handle.client, &handle.id).await?
    } else {
        // The sync answers once every copy is up, which on a cloud install
        // is minutes of a machine booting: say what each is doing meanwhile.
        progress.dispatcher_call_start(&path);
        let body = serde_json::to_value(&sync)?;
        let answer = crate::progress::while_waiting(
            handle.client.waiting_on_its_work().post_json(&path, &body),
            std::time::Duration::from_secs(10),
            async |elapsed| progress.infra_wait("start", elapsed.as_secs(), &changing_now(&handle.client, &handle.id).await),
        )
        .await?;
        progress.dispatcher_call_done(serde_json::json!({ "projectId": handle.id }));
        serde_json::from_value(answer).context("read the infra status the sync answered")?
    };
    progress.infra_provision_done();
    // A copy the host runs differently from what it asked (a GPU kind a
    // local install cannot choose) is told here, to the person starting it.
    for note in resp.host_notes() {
        progress.warn(note);
    }
    handle.warn_broken(progress);
    if !ctx.json() {
        print_status(&handle.name, &handle.id, &resp);
    }
    // No `progress.complete` here: the terminal event is emitted ONCE
    // by `run_inner` after the whole action.
    Ok(report)
}

/// Stop or terminate the copies `opts` names: the triggers whose cut
/// contains them taken down first, as `opts.take_down` says. Waits on the
/// COMMANDS' completion (not the rollup): a stop where a NoOp unit stays
/// up never drives the rollup to "stopped", so the outcome is the only
/// honest done signal.
async fn infra_destroy(
    ctx: &Ctx,
    progress: &Progress,
    opts: InfraOpts,
    verb: &str,
    force: bool,
) -> Result<Option<weft_core::deactivation::TakeDownReport>> {
    let (client, id, name) = super::resolve_project(ctx)?;
    let path = format!("/projects/{id}/infra/{verb}");
    let body = StopRequest { take_down: opts.take_down, instances: opts.instances, nodes: opts.nodes, force };
    if let Ok(take_down) = opts.take_down.resolve() {
        progress.drain_wait(take_down.running, crate::progress::DrainOn::Infra);
    }
    progress.dispatcher_call_start(&path);
    // 202 Accepted with the commands it runs as.
    let issued = issued_command(client.post_json(&path, &serde_json::to_value(&body)?).await?, verb)?;
    progress.dispatcher_call_done(serde_json::json!({ "projectId": id }));
    for command_id in &issued.command_ids {
        wait_for_command(progress, ctx.on(), &client, &id, *command_id, verb).await?;
    }
    if !ctx.json() {
        print_status(&name, &id, &infra_status_of(&client, &id).await?);
    }
    // Terminal event emitted once by `run_inner`.
    Ok(Some(issued.report))
}

/// Poll the command-status endpoint until the supervisor marks the
/// command complete. Fails loud on a `failed` outcome AND on a
/// `cancelled` one: cancelled means `weft infra cancel` halted this
/// command between steps, so the verb did NOT do what was asked and a
/// zero exit would lie about it (infra is left as-is, per-node state
/// intact). UNBOUNDED: an infra command (stop / terminate / start)
/// can depend on draining in-flight runs, which is a user-facing
/// wait that may legitimately last hours; a hard deadline would turn a
/// correct slow operation into a spurious failure (same rule as
/// `wait_for_drain`). The wait stays legible via an `InfraWait`
/// breadcrumb; Ctrl+C is the recovery. A look the install did not answer
/// (out of reach, or never answering within the hold) is said and looked
/// at again (`look_again_after`); every other status-endpoint error still
/// bubbles loudly.
async fn wait_for_command(
    progress: &Progress,
    on: Option<&str>,
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
        let looked = client
            .waiting_up_to(COMMAND_HOLD)
            .get_json(&format!("/projects/{project_id}/infra/commands/{command_id}?wait_ms={}", COMMAND_HOLD.as_millis()))
            .await;
        let resp = match looked {
            Ok(resp) => resp,
            Err(e) => {
                let why = super::ensure::look_again_after(e).map_err(|e| e.context(format!("waiting on infra {verb} command {command_id}")))?;
                progress.warn(&format!("could not look at infra {verb} command {command_id} just now ({why}); looking again"));
                tokio::time::sleep(super::follow::TASK_LOOK_AGAIN_AFTER).await;
                continue;
            }
        };
        // A status that does not read fails loud: a missing `done` read
        // as "not done" would loop forever (the wait is unbounded).
        let status: CommandStatus = crate::client::read_answer(resp, &format!("the status of the infra {verb} command"))?;
        if status.done {
            match status.outcome {
                Some(CommandOutcome::Succeeded) => return Ok(()),
                Some(CommandOutcome::Failed) => {
                    let msg = status.message.as_deref().unwrap_or("unknown error");
                    anyhow::bail!("infra {verb} failed: {msg}");
                }
                Some(CommandOutcome::Cancelled) => anyhow::bail!(
                    "infra {verb} cancelled (`{}`): the operation was halted \
                     between steps and did NOT complete; infra is left as-is (check `{}`), \
                     re-run the verb to finish or act per node",
                    super::weft_on(on, "infra cancel"),
                    super::weft_on(on, "infra status")
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
            progress.infra_wait(verb, (now - start).as_secs(), &changing_now(client, project_id).await);
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
    print!("{}", format_doors(&answer, ctx.on()));
    Ok(())
}

fn format_doors(answer: &DoorsResponse, on: Option<&str>) -> String {
    if answer.doors.is_empty() && answer.applying.is_empty() {
        return format!(
            "no doors: nothing this project runs is reachable from outside its instances. A node \
             opens one by declaring it on an endpoint of its own spec; `{}` lists what is running.\n",
            super::weft_on(on, "infra status")
        );
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
    // Printed apart from the copies: a start stuck this way has none yet.
    for entry in &status.unanswered {
        eprintln!("warning: {}", entry.describe());
    }
    Ok(())
}

/// What each copy being changed is doing, one line each, for a wait's
/// breadcrumb, after each of weft's own calls that keeps failing; a
/// status that cannot be read is that one line instead. A copy whose
/// start reports progress says what it waits on; a copy stopping,
/// terminating, or provisioning before the host reports anything says
/// only its status, so a stop or an upgrade still names what it is on.
async fn changing_now(client: &crate::client::DispatcherClient, project_id: &str) -> Vec<String> {
    let status = match infra_status_of(client, project_id).await {
        Ok(status) => status,
        Err(e) => return vec![format!("cannot read what each copy is doing ({e:#}); the wait goes on")],
    };
    // A call of weft's own that keeps failing comes first: it is why
    // nothing below moves.
    let unanswered = status.unanswered.iter().map(|u| u.describe());
    let copies = status.nodes.iter().filter_map(|n| {
        let node = match &n.instance {
            Some(instance) => format!("{} (instance {instance})", n.node),
            None => n.node.clone(),
        };
        if let Some(progress) = &n.progress {
            return Some(format!("{node}: {} {}", n.status, progress.describe(progress.as_of_unix)));
        }
        let changing = matches!(
            InfraNodeStatus::parse(&n.status),
            Some(InfraNodeStatus::Stopping | InfraNodeStatus::Terminating | InfraNodeStatus::Provisioning)
        );
        changing.then(|| format!("{node}: {}", n.status))
    });
    unanswered.chain(copies).collect()
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
        if let Some(progress) = &n.progress {
            println!("    {}", progress.describe(progress.as_of_unix));
        }
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
        let out = format_doors(&answer, None);
        assert!(out.contains("db.pg  10.0.0.2:5432"), "{out}");
        assert!(out.contains("db.pg (instance ann)  10.0.0.3:5432"), "{out}");
        assert!(out.contains("cache  (still being applied"), "{out}");
    }

    /// A terminate's confirmation names the nodes and whose copies.
    #[test]
    fn a_terminate_asks_about_exactly_what_it_deletes() {
        let opts = |nodes: &[&str], instances| InfraOpts { nodes: nodes.iter().map(|n| n.to_string()).collect(), instances, ..Default::default() };
        assert_eq!(whose_infra(&opts(&[], InstanceTarget::Shared)), "the infra the program shares");
        assert_eq!(whose_infra(&opts(&["db"], InstanceTarget::EveryInstance)), "infra node 'db' of every instance");
        assert_eq!(whose_infra(&opts(&[], InstanceTarget::Everything)), "the infra the program shares and every instance's");
        let named = InstanceTarget::named(vec![weft_core::instance::InstanceId::new("ann").unwrap()]);
        assert_eq!(whose_infra(&opts(&["db", "cache"], named)), "infra nodes 'db', 'cache' of instance 'ann'");
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
