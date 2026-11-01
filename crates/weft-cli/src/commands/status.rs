//! `weft status`: discover the cwd project, compute current source
//! hashes, ask the dispatcher's `/projects/{id}/status` aggregator
//! about them in the request's body (drives drift detection).
//! Print a human-readable summary or, with `--json`, emit the full
//! status payload as a single JSON line for consumption by the
//! VS Code extension's action bar.

use anyhow::Result;
use weft_core::projects::{ProjectDrift, ProjectStatusResponse, ProjectTransition};

use super::Ctx;

pub async fn run(ctx: Ctx) -> Result<()> {
    let project = ctx.project()?;
    let project_id = project.id().to_string();

    // Compute desired hashes from current source. The dispatcher
    // compares them against the build each trigger and infra copy runs:
    // the code (binary hash), each trigger's cut and each infra place's
    // own infra, to say which are behind.
    let weft_root = weft_compiler::build::resolve_weft_root()
        .map_err(|e| anyhow::anyhow!("resolve weft repo root: {e}"))?;
    // Both hashes are scoped to the compiled project's referenced /
    // infra-closure nodes, so both need the definition + catalog. If
    // the project can't compile, leave the desired hashes unset and say
    // why: status is display-only and tolerates an in-progress project,
    // but a person reading no drift must know none was looked for.
    //
    // FULL hashes, never shortened: the dispatcher stores the full
    // `running_*_hash` values register sends and compares by string
    // equality, so a shortened desired hash would never match and every
    // project would permanently report drift. Short hashes are for
    // human log lines only.
    //
    // The definition is hashed AFTER its `@asset` refs resolve, exactly
    // as build / run / resync hash it before registering: hashing the
    // unresolved source would differ from every running hash of a
    // project with an asset in it, and the drift banner would never
    // clear. Resolving writes nothing: a file not stored yet stands for
    // the value its upload would give it.
    // Whether the program runs infra of its own, for the first deploy's
    // order (its infra first, then its triggers).
    let mut declares_infra = false;
    let (desired_binary_hash, desired_full_binary_hash, desired_definition_hash, desired_trigger_cuts, desired_infra_places) =
        match weft_compiler::hash::load_program(project) {
            Ok((mut def, catalog)) => {
                declares_infra = weft_core::project::has_infra(&def);
                let resolved = crate::commands::assets::resolve_project_assets(&ctx.client()?, &project.root, &mut def).await;
                // Both node sets: a worker built with the full catalog is as
                // current as one built from the referenced set, and the
                // dispatcher accepts either as "not drifted".
                use weft_core::builds::NodeSet;
                // The hash the install computes is for the packaging it
                // builds with: a program on this machine, an image elsewhere.
                let packaging = match crate::credentials::this_machines_install(ctx.client()?.base())? {
                    Some(_) => weft_platform_local::MachineLayout::here().host_packaging(),
                    None => weft_core::builds::WorkerPackaging::Image { builder: crate::images::builder_base_ref()? },
                };
                match resolved {
                    Ok(_) => (
                        or_warn(
                            "the worker code's hash",
                            weft_compiler::hash::compute_binary_hash(&def, project, &weft_root, &catalog, NodeSet::Referenced, &packaging, &weft_compiler::hash::DISK),
                        ),
                        or_warn(
                            "the worker code's hash with every node",
                            weft_compiler::hash::compute_binary_hash(&def, project, &weft_root, &catalog, NodeSet::Full, &packaging, &weft_compiler::hash::DISK),
                        ),
                        or_warn("the project definition's hash", weft_compiler::hash::compute_definition_hash(&def)),
                        // Every trigger the source has, one whose cut cannot be
                        // walked included: the install reads one left out as
                        // gone from the source.
                        Some(weft_core::run_spec::trigger_fingerprints(&weft_core::run_spec::trigger_cuts(&def))),
                        // Every infra place the source has: the install reads a
                        // copy of one left out as behind.
                        or_warn(
                            "each infra place's hash",
                            weft_compiler::hash::compute_infra_place_hashes(&def, &project.root, &weft_root, &catalog, &weft_compiler::hash::DISK),
                        ),
                    ),
                    // An asset that cannot resolve is what a build will
                    // refuse; status stays display-only and reports no
                    // desired hashes rather than a made-up drift.
                    Err(e) => {
                        warn_no_drift("its assets do not resolve", &e);
                        (None, None, None, None, None)
                    }
                }
            }
            Err(e) => {
                warn_no_drift("the project does not compile", &e);
                (None, None, None, None, None)
            }
        };

    let request = weft_core::projects::StatusRequest {
        desired_binary_hash,
        desired_full_binary_hash,
        desired_definition_hash,
        desired_trigger_cuts,
        desired_infra_places,
    };
    let path = format!("/projects/{project_id}/status");

    // A project that exists on disk but was never registered is not an error:
    // it is the state every project starts in, and the answer is the
    // command that leaves it. The marked 404 is how the dispatcher says
    // "no project I know under this id" (as opposed to a missing route).
    let Some(data) = ctx.client()?.ask_json_if_found(&path, &serde_json::to_value(&request)?).await? else {
        if !ctx.json_out(&serde_json::json!({ "registered": false, "projectId": project_id }))? {
            // A program with infra of its own has it started before its
            // triggers are switched on: a trigger reads its infra.
            let first = if declares_infra {
                format!("`{}` to start its infra, then `{}` to enable its triggers", ctx.weft("infra start"), ctx.weft("activate"))
            } else {
                format!("`{}` to enable its triggers", ctx.weft("activate"))
            };
            println!(
                "project: {} ({project_id})\n  not registered with the dispatcher yet: use `{}` to run it, or {first}",
                project.manifest.package.name,
                ctx.weft("run"),
            );
        }
        return Ok(());
    };

    let data: ProjectStatusResponse = crate::client::read_answer(data, "the project's status")?;

    // One JSON object on stdout; the extension reads it.
    if ctx.json_out(&data)? {
        return Ok(());
    }

    println!("project: {} ({project_id})", data.name);
    let (maintenance, address, database) = tokio::join!(maintenance_line(&ctx), address_line(&ctx), database_line(&ctx));
    for line in [maintenance, address, database].into_iter().flatten() {
        println!("  {line}");
    }
    // Half taken down: what follows is what is left.
    if data.removing {
        println!("  WARNING: {}", weft_core::projects::PROJECT_BEING_REMOVED);
    }
    println!("  registration: {}", data.status);
    match &data.address {
        Some(weft_core::projects::ProjectAddress::Serving { url }) => println!("  address: {url} (its routes, at the root)"),
        Some(weft_core::projects::ProjectAddress::Unavailable { why }) => println!("  address: unavailable: {why}"),
        None => {}
    }
    // The transition axis: only worth a line while in flight.
    match data.transition {
        ProjectTransition::None => {}
        ProjectTransition::Resyncing => println!("  resync: {}", resync_line(data.resync.as_ref(), crate::progress::now_unix() as i64, |verb| ctx.weft(verb))),
        building => println!("  build: {building} (cancel with `{}`)", ctx.weft("cancel-build")),
    }
    for build in &data.builds {
        match &build.log_url {
            Some(log) => println!("    {} building as {}; its log: {log}", build.image, build.build),
            None => println!("    {} building as {}", build.image, build.build),
        }
    }
    println!("  listener: {}", if data.listener_running { "running" } else { "stopped" });
    // Orphaned live infra: never silent (the never-lose-track rule).
    if data.orphaned_infra {
        println!(
            "  WARNING: live infra exists whose node the source does not declare (removed from it, \
             or inside a part that does not compile); it keeps running (and consuming resources) \
             until stopped/terminated via the infra verbs"
        );
    }

    // One entry per infra node the program declares, started or not, so
    // an empty list really means no node declares `requiresInfra`.
    if data.infra.is_empty() && !data.built {
        println!("  infra: (this install has not built the program yet, so it does not know its nodes)");
    } else if data.infra.is_empty() {
        println!("  infra: (no nodes declare requiresInfra)");
    } else {
        println!("  infra:");
        for entry in &data.infra {
            // `node` is the node's place, spelled the way the source
            // reads it (`one.db`): the key and the label are one.
            let node = &entry.node;
            match entry.status.as_str() {
                weft_core::infra::wire::INFRA_NOT_STARTED => {
                    println!("    {node}: not started (`{}` brings it up)", ctx.weft("infra start"))
                }
                weft_core::infra::wire::INFRA_PER_INSTANCE => {
                    let copies = entry.instance_copy_count.unwrap_or(0);
                    let counted = if copies == 1 { "1 instance has a copy".to_string() } else { format!("{copies} instances have a copy") };
                    println!("    {node}: one copy per instance ({counted}, listed under instance infra)");
                }
                st => {
                    let standing = copy_standing(entry.behind, entry.orphaned, |verb| ctx.weft(verb));
                    println!("    {node}: {st}{standing} ({})", entry.endpoint_url.as_deref().unwrap_or("-"));
                    if let Some(progress) = &entry.progress {
                        println!("      {}", progress.describe(progress.as_of_unix));
                    }
                    // One of its containers stopped on its own and was
                    // brought back: when, why, and what to do about it.
                    if let Some(restart) = &entry.last_restart {
                        println!("      {}", restart.describe(node));
                    }
                }
            }
            // What stopping it takes down with it: the triggers whose cut
            // contains it.
            if !entry.takes_down.is_empty() {
                println!("      taking it down takes down {}", weft_core::activation::OwnerTriggers::list_words(&entry.takes_down));
            }
        }
    }

    // Every trigger of the program, whether it is on and behind, and every
    // instance's copy of one that exists once per instance.
    if !data.triggers.is_empty() {
        println!("  triggers:");
        for entry in &data.triggers {
            for line in trigger_lines(entry) {
                println!("    {line}");
            }
        }
    }
    // Instances' own copies of the `@per_instance` infra nodes.
    if !data.instance_infra.is_empty() {
        println!("  instance infra:");
        for entry in &data.instance_infra {
            let standing = copy_standing(entry.behind, entry.orphaned, |verb| ctx.weft(verb));
            println!("    {} (instance {}): {}{standing}", entry.node, entry.instance, entry.status);
            if let Some(progress) = &entry.progress {
                println!("      {}", progress.describe(progress.as_of_unix));
            }
            if let Some(restart) = &entry.last_restart {
                println!("      {}", restart.describe(&format!("{} (instance {})", entry.node, entry.instance)));
            }
        }
    }

    let runs = &data.runs;
    // `weft tree` lists the program's own runs; the ones that set it up
    // (arming triggers, starting infra) are counted apart so the two agree.
    match runs.setup {
        0 => println!("  runs: {} runs", runs.total),
        setup => println!("  runs: {} runs (and {setup} that set the program up: its triggers, its infra)", runs.total),
    }
    if let (Some(run_id), Some(status)) = (&runs.last_run_id, &runs.last_status) {
        match runs.last_completed_at {
            Some(ts) => {
                let age = crate::progress::now_unix().saturating_sub(ts);
                println!("    last: {run_id} ({status}, completed {age}s ago)");
            }
            None => println!("    last: {run_id} ({status}, in flight)"),
        }
    }
    print_drift(&ctx, &data.drift);
    // One of weft's own calls for this project keeps failing (waking the
    // supervisor, handing a run to the workers): why infra or runs do
    // not move.
    if !data.unanswered.is_empty() {
        println!("  waiting on weft:");
        for entry in &data.unanswered {
            println!("    {}", entry.describe());
        }
    }
    // What each trigger started lately: the one trace a run kept
    // unrecorded leaves when it goes well.
    if !data.trigger_runs.is_empty() {
        println!("  runs started (the last minute or two):");
        for entry in &data.trigger_runs {
            match entry.failed {
                0 => println!("    {}: {}", entry.node, entry.started),
                failed => println!("    {}: {} ({failed} failed)", entry.node, entry.started),
            }
        }
    }
    // A public entry turning callers away, so the author knows a limit
    // is acting and which one (each is a setting on the trigger).
    if !data.limited.is_empty() {
        println!("  refused calls (the last minute or two):");
        for entry in &data.limited {
            println!("    {}: {} by {}", entry.node, entry.refused, entry.limit);
        }
    }
    // The same verb list the editor's action bar offers, so a terminal
    // reader sees what the project accepts right now (and that `resync`
    // is on the table when the listeners lag behind the code).
    if !data.available_actions.is_empty() {
        println!("  actions: {}", data.available_actions.join(", "));
    }

    Ok(())
}

/// Say on stderr (stdout may be the one JSON line the editor reads) that
/// drift cannot be looked for, and why.
/// The line `weft status` adds while the install is in maintenance, read
/// off `GET /install/maintenance`; `None` when it is not. One that could
/// not be read is said on stderr, the rest of the status still printed.
async fn maintenance_line(ctx: &Ctx) -> Option<String> {
    let read = async { serde_json::from_value::<weft_core::maintenance::MaintenanceState>(ctx.client()?.get_json("/install/maintenance").await?).map_err(anyhow::Error::from) };
    match read.await {
        Ok(state) => state.since_unix.map(|since| {
            let since = chrono::DateTime::from_timestamp(since, 0).map(|at| at.format("%Y-%m-%d %H:%M UTC").to_string()).unwrap_or_else(|| since.to_string());
            format!("maintenance: the install is in maintenance since {since}; nothing is turned on until `{}`", ctx.weft("maintenance off"))
        }),
        Err(e) => {
            eprintln!("warning: could not read whether the install is in maintenance ({e:#})");
            None
        }
    }
}

/// The line naming the address the install hands out in its links, read
/// off `GET /install`; one that could not be read is said on stderr.
async fn address_line(ctx: &Ctx) -> Option<String> {
    let read = async { serde_json::from_value::<weft_core::install::InstallInfo>(ctx.client()?.get_json("/install").await?).map_err(anyhow::Error::from) };
    match read.await {
        Ok(info) if info.public_url == info.service_url => Some(format!("install address: {}", info.public_url)),
        Ok(info) => Some(format!("install address: {} (it still answers at {} too)", info.public_url, info.service_url)),
        Err(e) => {
            eprintln!("warning: could not read the install's address ({e:#})");
            None
        }
    }
}

/// The line `weft status` adds while the install's database held it back
/// lately, read off `GET /install/database`; `None` when it did not. One
/// that could not be read is said on stderr.
async fn database_line(ctx: &Ctx) -> Option<String> {
    let read = async {
        serde_json::from_value::<weft_core::install::DatabaseLoad>(ctx.client()?.get_json("/install/database").await?).map_err(anyhow::Error::from)
    };
    match read.await {
        Ok(load) => held_back_words(&load),
        Err(e) => {
            eprintln!("warning: could not read whether the install's database held it back ({e:#})");
            None
        }
    }
}

/// [`database_line`]'s words for `load`: who waited, how often, since
/// when, what the database takes, and what gives the install more.
fn held_back_words(load: &weft_core::install::DatabaseLoad) -> Option<String> {
    use weft_core::install::DatabaseHolder;
    let first = load.held_back.iter().min_by_key(|h| h.since_unix)?;
    let at = |unix: i64| chrono::DateTime::from_timestamp(unix, 0).map(|at| at.format("%H:%M UTC").to_string()).unwrap_or_else(|| unix.to_string());
    let waits: u64 = load.held_back.iter().map(|h| h.waits).sum();
    let last = load.held_back.iter().map(|h| h.last_unix).max().unwrap_or(first.last_unix);
    let shares: Vec<String> = load
        .held_back
        .iter()
        .map(|h| match h.role {
            DatabaseHolder::Machine => format!("the install's process holds at most {}", h.share),
            DatabaseHolder::Dispatcher | DatabaseHolder::Broker => format!("each copy of the {} holds at most {}", h.role, h.share),
        })
        .collect();
    let more = load.more.as_deref().map(|more| format!("; {more}")).unwrap_or_default();
    Some(format!(
        "database: busy. Since {}, requests waited {waits} time{} for a database connection, the last at {} \
         (the database takes {} connections, and {}){more}",
        at(first.since_unix),
        if waits == 1 { "" } else { "s" },
        at(last),
        first.max_connections,
        shares.join(", "),
    ))
}

fn warn_no_drift(why: &str, e: &anyhow::Error) {
    eprintln!("warning: {why}, so whether the running program is behind the source is not checked ({e:#})");
}

/// How an infra copy stands against the source, as its line ends: behind
/// it, or an orphan of a place the source declares no more, with what
/// takes it down; nothing when it is current.
fn copy_standing(behind: bool, orphaned: bool, weft: impl Fn(&str) -> String) -> String {
    if orphaned {
        format!(", orphaned: the source declares it no more; it keeps running until `{}` or `{}` takes it down", weft("infra stop"), weft("infra terminate"))
    } else if behind {
        ", behind the source".to_string()
    } else {
        String::new()
    }
}

/// One desired hash, or `None` with a warning naming what could not be
/// worked out.
fn or_warn<T>(what: &str, hash: Result<T>) -> Option<T> {
    hash.map_err(|e| warn_no_drift(&format!("{what} cannot be worked out"), &e)).ok()
}


/// One trigger as `weft status` lists it: the shared one on one line, one
/// that exists once per instance as how many instances have it on, then
/// each instance's copy.
fn trigger_lines(entry: &weft_core::projects::TriggerEntry) -> Vec<String> {
    use weft_core::projects::{TriggerCopy, TriggerState};
    let gone = if entry.gone { " (the program has it no more; `weft deactivate --trigger` takes it down)" } else { "" };
    let reads = match entry.depends_on.as_slice() {
        [] => String::new(),
        infra => format!("; reads {}", infra.join(", ")),
    };
    let copy_words = |copy: &TriggerCopy| -> String {
        let state = match copy.state {
            TriggerState::Off => {
                let door = match copy.mode {
                    weft_core::activation::ActivationMode::Down(door) => format!("off, its door set to {}", door.as_str()),
                    _ => "off".to_string(),
                };
                match copy.down_with {
                    None => door,
                    Some(weft_core::projects::DownWith::Health) => format!(
                        "{door}, taken down by the health check (its infra's health rules bring it back once that infra is \
                         healthy; with rules that do not, `weft activate` does)"
                    ),
                    Some(weft_core::projects::DownWith::Infra) => {
                        format!("{door}, taken down with its infra (it comes back once that infra runs again)")
                    }
                    Some(weft_core::projects::DownWith::Maintenance) => {
                        format!(
                            "{door}, taken down by the install's maintenance (`weft maintenance off` brings it back; once \
                             the maintenance is over, `weft resync` does too)"
                        )
                    }
                }
            }
            TriggerState::TurningOn => match &copy.progress {
                Some(progress) => format!("turning on {}", progress.describe(progress.as_of_unix)),
                None => "turning on".to_string(),
            },
            TriggerState::On => "on".to_string(),
            TriggerState::Behind => "on, behind the source (`weft resync` brings it up to date)".to_string(),
        };
        let held_back = copy.held_back.as_deref().map(|why| format!(", held back: {why}")).unwrap_or_default();
        let held = if copy.held.is_empty() { String::new() } else { format!(", holding {}", copy.held.describe()) };
        let version = copy.version.as_deref().map(|v| format!(", version {}", super::versions::short(v))).unwrap_or_default();
        let waiting = copy.waiting.as_ref().map(waiting_words).unwrap_or_default();
        let dropped = copy.dropped.as_ref().map(|dropped| format!("; {}", dropped.describe())).unwrap_or_default();
        format!("{state}{held_back}{held}{version}{waiting}{dropped}")
    };
    let mut lines = Vec::new();
    match &entry.shared {
        Some(copy) => lines.push(format!("{}: {}{reads}{gone}", entry.node, copy_words(copy))),
        None => {
            let on = entry.copies.iter().filter(|c| c.state.is_on()).count();
            lines.push(format!("{}: once per instance, {on} of {} on{reads}{gone}", entry.node, entry.copies.len()));
        }
    }
    for copy in &entry.copies {
        let instance = copy.instance.as_ref().map(|i| i.as_str()).unwrap_or("-");
        lines.push(format!("  instance {instance}: {}", copy_words(copy)));
    }
    lines
}

/// "; 2 fires wait until ...", for fires waiting on something no clock
/// brings: what they wait on hands them over again.
fn waiting_words(waiting: &weft_core::program::WaitingFires) -> String {
    use weft_core::door_fire::UnclockedWait;
    let (fires, wait) = if waiting.fires == 1 { ("1 fire".to_string(), "waits") } else { (format!("{} fires", waiting.fires), "wait") };
    let until = match waiting.on {
        UnclockedWait::Instance => format!("{wait} until the instance's values change"),
        UnclockedWait::Infra => format!("{wait} until the infra the runs read is running again"),
        UnclockedWait::Picks => format!("{wait} until the install picks the connections the runs need"),
        UnclockedWait::Failure => "stopped after failing to start a run, and holds the trigger's queue; `weft activate`, a new \
                                   build or a change of the worker settings tries again"
            .to_string(),
    };
    format!("; {fires} {until}: {}", waiting.reason)
}

/// The resync line, at `now_unix` (`weft` spells a command for the install
/// the person acts on): stopped, with why and what comes next; under way
/// and waiting for the runs on the old version, naming them; or under way.
fn resync_line(resync: Option<&weft_core::projects::ResyncStanding>, now_unix: i64, weft: impl Fn(&str) -> String) -> String {
    let resync = resync.cloned().unwrap_or_default();
    if let Some(stop) = &resync.stopped {
        return format!("stopped with triggers it took down still down: {}. Next: {}", stop.error, stop.then);
    }
    match resync.progress.waits_for.as_slice() {
        [] => format!("under way (`{}` stops it as the triggers come back)", weft("cancel-activate")),
        runs => format!("under way; {}", crate::progress::resync_wait_line(runs, now_unix, weft)),
    }
}

/// Every drift bit the dispatcher set, each with the verb that clears
/// it. Silent when nothing drifted. A trigger behind is listed with the
/// trigger.
fn print_drift(ctx: &Ctx, drift: &ProjectDrift) {
    let lines = [
        (drift.infra_drift, format!("infra: a copy is behind the source; `{}` puts the change live", ctx.weft("infra upgrade"))),
        (drift.binary_drift, format!("binary: worker code has changed; the next run or `{}` rebuilds the image", ctx.weft("build"))),
        (drift.definition_drift, "definition: project shape has changed; the next run picks it up".to_string()),
    ];
    if lines.iter().all(|(set, _)| !set) {
        return;
    }
    println!("  drift:");
    for (_, line) in lines.iter().filter(|(set, _)| *set) {
        println!("    {line}");
    }
}

#[cfg(test)]
mod tests {
    use weft_core::activation::ActivationMode;
    use weft_core::instance::InstanceId;
    use weft_core::projects::{HeldWork, TriggerCopy, TriggerEntry, TriggerState};

    fn copy(instance: Option<&str>, state: TriggerState, mode: ActivationMode) -> TriggerCopy {
        TriggerCopy { instance: instance.map(|i| InstanceId::new(i).unwrap()), state, mode, down_with: None, held_back: None, held: HeldWork::default(), waiting: None, version: None, progress: None, dropped: None }
    }

    /// An infra copy's line says whether it is behind the source, or an
    /// orphan of a place the source declares no more with what takes it
    /// down, and nothing more when it is current.
    #[test]
    fn an_infra_copy_says_whether_it_is_behind_or_orphaned() {
        let weft = |verb: &str| format!("weft {verb}");
        assert_eq!(super::copy_standing(false, false, weft), "");
        assert_eq!(super::copy_standing(true, false, weft), ", behind the source");
        let orphaned = super::copy_standing(false, true, weft);
        assert!(orphaned.contains("orphaned") && orphaned.contains("`weft infra stop`") && orphaned.contains("`weft infra terminate`"), "{orphaned}");
    }

    /// A resync that stopped says so, why, and what comes next, never
    /// "under way"; one waiting for runs on the old version names them.
    #[test]
    fn a_resync_says_whether_it_stopped_and_what_it_waits_for() {
        use weft_core::deactivation::{ResyncProgress, RunGoing};
        use weft_core::projects::{ResyncStanding, ResyncStop};
        let weft = |verb: &str| format!("weft {verb}");
        let stopped = ResyncStanding {
            stopped: Some(ResyncStop { error: "the database was busy".into(), then: "it goes on by itself shortly".into() }),
            progress: ResyncProgress::default(),
        };
        let line = super::resync_line(Some(&stopped), 0, weft);
        assert_eq!(line, "stopped with triggers it took down still down: the database was busy. Next: it goes on by itself shortly");
        let waiting = ResyncStanding {
            stopped: None,
            progress: ResyncProgress { waits_for: vec![RunGoing { run_id: uuid::Uuid::nil(), since_unix: 40 }] },
        };
        let line = super::resync_line(Some(&waiting), 100, weft);
        assert!(line.starts_with("under way; the new version takes over once 1 run on the old one ends") && line.contains("(going 1m00s)"), "{line}");
        assert_eq!(super::resync_line(None, 0, weft), "under way (`weft cancel-activate` stops it as the triggers come back)");
    }

    /// A shared trigger is one line with what it reads; one that exists
    /// once per instance says how many are on, then each instance's.
    #[test]
    fn a_trigger_reads_as_whether_it_is_on_and_behind() {
        let door = TriggerEntry {
            node: "door".into(),
            per_instance: false,
            gone: false,
            shared: Some(TriggerCopy { held: HeldWork { arrivals: 2, paused: 0 }, ..copy(None, TriggerState::Off, ActivationMode::Down(weft_core::Door::Hide)) }),
            copies: Vec::new(),
            depends_on: vec!["db".into()],
        };
        let lines = super::trigger_lines(&door);
        assert!(lines[0].starts_with("door: off, its door set to hide, holding 2 ") && lines[0].ends_with("; reads db"), "{lines:?}");
        let inbox = TriggerEntry {
            node: "inbox".into(),
            per_instance: true,
            gone: false,
            shared: None,
            copies: vec![copy(Some("ada"), TriggerState::Behind, ActivationMode::Active), copy(Some("bob"), TriggerState::Off, ActivationMode::Registered)],
            depends_on: Vec::new(),
        };
        let lines = super::trigger_lines(&inbox);
        assert_eq!(lines[0], "inbox: once per instance, 1 of 2 on");
        assert!(lines[1].starts_with("  instance ada: on, behind the source"), "{lines:?}");
        assert_eq!(lines[2], "  instance bob: off");
    }

    /// A trigger whose door dropped fires says how many, and why the last
    /// one was dropped.
    #[test]
    fn a_trigger_that_dropped_fires_or_calls_says_how_many_and_why() {
        let door = TriggerEntry {
            node: "door".into(),
            per_instance: false,
            gone: false,
            shared: Some(TriggerCopy {
                dropped: Some(weft_core::projects::Dropped { count: 3, last_reason: "package 'relay' didn't compile".into() }),
                ..copy(None, TriggerState::On, ActivationMode::Active)
            }),
            copies: Vec::new(),
            depends_on: Vec::new(),
        };
        assert_eq!(
            super::trigger_lines(&door),
            ["door: on; 3 fires or calls dropped in the last minute or two (refused before any run started), the last because: package 'relay' didn't compile"]
        );
    }

    /// A trigger the health check took down says so, and what brings it
    /// back; one taken down with its infra says what brings it back too.
    #[test]
    fn a_trigger_down_by_the_health_check_says_so() {
        let entry = |down_with| TriggerEntry {
            node: "door".into(),
            per_instance: false,
            gone: false,
            shared: Some(TriggerCopy { down_with, ..copy(None, TriggerState::Off, ActivationMode::Down(weft_core::Door::Take)) }),
            copies: Vec::new(),
            depends_on: vec!["db".into()],
        };
        let health = super::trigger_lines(&entry(Some(weft_core::projects::DownWith::Health)));
        assert!(health[0].contains("taken down by the health check") && health[0].contains("`weft activate`"), "{health:?}");
        let infra = super::trigger_lines(&entry(Some(weft_core::projects::DownWith::Infra)));
        assert!(infra[0].contains("taken down with its infra"), "{infra:?}");
        let maintenance = super::trigger_lines(&entry(Some(weft_core::projects::DownWith::Maintenance)));
        assert!(maintenance[0].contains("`weft maintenance off` brings it back") && maintenance[0].contains("`weft resync` does too"), "{maintenance:?}");
        assert!(!super::trigger_lines(&entry(None))[0].contains("taken down"));
        let mut held_back = entry(Some(weft_core::projects::DownWith::Health));
        held_back.shared.as_mut().unwrap().held_back = Some("triggers on another build. `weft resync --all` moves them".into());
        let lines = super::trigger_lines(&held_back);
        assert!(lines[0].contains("held back: triggers on another build") && lines[0].contains("`weft resync --all`"), "{lines:?}");
    }

    /// A database that held nobody back adds no line; one that held back
    /// copies says who waited, how often, what the database takes, each
    /// role's share, and what gives the install more.
    #[test]
    fn a_database_that_held_the_install_back_says_so_and_how_to_give_it_more() {
        use weft_core::install::{DatabaseHolder, DatabaseLoad, HeldBack};
        assert_eq!(super::held_back_words(&DatabaseLoad::default()), None);
        let held = |role, since_unix, waits, share| HeldBack { role, since_unix, last_unix: since_unix + 60, waits, max_connections: 50, share };
        let load = DatabaseLoad {
            held_back: vec![held(DatabaseHolder::Broker, 3600, 40, 7), held(DatabaseHolder::Dispatcher, 7200, 2, 7)],
            more: Some("to give it more, raise WEFT_CLOUD_SQL_TIER".into()),
        };
        let line = super::held_back_words(&load).unwrap();
        assert!(line.starts_with("database: busy. Since 01:00 UTC, requests waited 42 times"), "{line}");
        assert!(line.contains("the last at 02:01 UTC") && line.contains("takes 50 connections"), "{line}");
        assert!(line.contains("each copy of the broker holds at most 7, each copy of the dispatcher holds at most 7"), "{line}");
        assert!(line.ends_with("; to give it more, raise WEFT_CLOUD_SQL_TIER"), "{line}");
        let local = DatabaseLoad { held_back: vec![held(DatabaseHolder::Machine, 3600, 3, 20)], more: None };
        let line = super::held_back_words(&local).unwrap();
        assert!(line.contains("the install's process holds at most 20"), "{line}");
    }
}
