//! `weft status`: discover the cwd project, compute current source
//! hashes, hit the dispatcher's `/projects/{id}/status` aggregator
//! with those hashes as query params (drives drift detection).
//! Print a human-readable summary or, with `--json`, emit the full
//! status payload as a single JSON line for consumption by the
//! VS Code extension's action bar.

use anyhow::{Context, Result};
use weft_core::projects::{ProjectDrift, ProjectStatusResponse, ProjectTransition};

use super::Ctx;

pub async fn run(ctx: Ctx) -> Result<()> {
    let project = ctx.project()?;
    let project_id = project.id().to_string();

    // Compute desired hashes from current source. The dispatcher
    // compares against project.running_binary_hash,
    // project.running_definition_hash, and project.running_infra_hash
    // to decide which drift bits to set.
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
    // clear. Asset resolution also publishes the current references,
    // including an empty set when the last asset was removed.
    let (desired_binary_hash, desired_full_binary_hash, desired_definition_hash, desired_infra_hash) =
        match weft_compiler::hash::load_enriched_project(project) {
            Ok((mut def, catalog)) => {
                let resolved = crate::commands::assets::resolve_project_assets(
                    &ctx.client()?,
                    &project.root,
                    &mut def,
                    true,
                )
                .await;
                // Both node sets: a worker built with the full catalog is as
                // current as one built from the referenced set, and the
                // dispatcher accepts either as "not drifted".
                use weft_core::builds::NodeSet;
                match resolved {
                    Ok(_) => (
                        or_warn(
                            "the worker code's hash",
                            weft_compiler::hash::compute_binary_hash(&def, project, &weft_root, &catalog, NodeSet::Referenced),
                        ),
                        or_warn(
                            "the worker code's hash with every node",
                            weft_compiler::hash::compute_binary_hash(&def, project, &weft_root, &catalog, NodeSet::Full),
                        ),
                        or_warn("the project definition's hash", weft_compiler::hash::compute_definition_hash(&def)),
                        or_warn(
                            "the infra's hash",
                            weft_compiler::hash::compute_infra_hash(&def, &project.root, &weft_root, &catalog),
                        ),
                    ),
                    // An asset that cannot resolve is what a build will
                    // refuse; status stays display-only and reports no
                    // desired hashes rather than a made-up drift.
                    Err(e) => {
                        warn_no_drift("its assets do not resolve", &e);
                        (None, None, None, None)
                    }
                }
            }
            Err(e) => {
                warn_no_drift("the project does not compile", &e);
                (None, None, None, None)
            }
        };

    let query = weft_core::projects::StatusQuery {
        desired_binary_hash,
        desired_full_binary_hash,
        desired_definition_hash,
        desired_infra_hash,
        ..Default::default()
    };
    let path = format!("/projects/{project_id}/status{}", query.to_query_string());

    // A project that exists on disk but was never registered is not an error:
    // it is the state every project starts in, and the answer is the
    // command that leaves it. The marked 404 is how the dispatcher says
    // "no project I know under this id" (as opposed to a missing route).
    let Some(data) = ctx.client()?.get_json_if_found(&path).await? else {
        if !ctx.json_out(&serde_json::json!({ "registered": false, "project_id": project_id }))? {
            println!(
                "project: {} ({project_id})\n  not registered with the dispatcher yet: \
                 use `{}` to run it, or `{}` to enable its triggers",
                project.manifest.package.name,
                ctx.weft("run"),
                ctx.weft("activate"),
            );
        }
        return Ok(());
    };

    let data: ProjectStatusResponse = serde_json::from_value(data)
        .context("read the project's status (the dispatcher and this CLI disagree on its shape; upgrade one of them)")?;

    // One JSON object on stdout; the extension reads it.
    if ctx.json_out(&data)? {
        return Ok(());
    }

    println!("project: {} ({project_id})", data.name);
    println!("  registration: {}", data.status);
    // The build-transition axis: only worth a line while in flight.
    if data.transition != ProjectTransition::None {
        println!("  build: {} (cancel with `{}`)", data.transition, ctx.weft("cancel-build"));
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
            "  WARNING: live infra exists whose node was removed from the source; \
             it keeps running (and consuming resources) until stopped/terminated via the infra verbs"
        );
    }

    // One entry per infra node the program declares, started or not, so
    // an empty list really means no node declares `requires_infra`.
    if data.infra.is_empty() && !data.built {
        println!("  infra: (this install has not built the program yet, so it does not know its nodes)");
    } else if data.infra.is_empty() {
        println!("  infra: (no nodes declare requires_infra)");
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
                    println!("    {node}: {st} ({})", entry.endpoint_url.as_deref().unwrap_or("-"));
                    if let Some(progress) = &entry.progress {
                        println!("      {}", progress.describe_now());
                    }
                }
            }
        }
    }

    // Each trigger's own activation: the shared ones make up the
    // registration line above, and an instance's appear only here.
    if !data.activations.is_empty() {
        println!("  triggers:");
        for entry in &data.activations {
            let (trigger, mode) = (&entry.trigger, entry.mode.as_str());
            // Which version of the source its fires run, as `weft tree`
            // names it.
            let version = entry.version.as_deref().map(|v| format!(", version {}", super::versions::short(v))).unwrap_or_default();
            match &entry.instance {
                Some(instance) => println!("    {trigger} (instance {instance}): {mode}{version}"),
                None => println!("    {trigger}: {mode}{version}"),
            }
            // Fires parked until the instance is given a value they
            // need: its next change of values routes them again.
            if let Some(waiting) = &entry.waiting {
                let counted = if waiting.fires == 1 { "1 fire waits".to_string() } else { format!("{} fires wait", waiting.fires) };
                println!("      {counted} until the instance's values change: {}", waiting.reason);
            }
        }
    }
    // Instances' own copies of the `@per_instance` infra nodes.
    if !data.instance_infra.is_empty() {
        println!("  instance infra:");
        for entry in &data.instance_infra {
            println!("    {} (instance {}): {}", entry.node, entry.instance, entry.status);
            if let Some(progress) = &entry.progress {
                println!("      {}", progress.describe_now());
            }
        }
    }

    let execs = &data.executions;
    // `weft tree` lists the program's own runs; the ones that set it up
    // (arming triggers, starting infra) are counted apart so the two agree.
    match execs.setup {
        0 => println!("  executions: {} runs", execs.total),
        setup => println!("  executions: {} runs (and {setup} that set the program up: its triggers, its infra)", execs.total),
    }
    if let (Some(execution_id), Some(status)) = (&execs.last_execution_id, &execs.last_status) {
        match execs.last_completed_at {
            Some(ts) => {
                let age = unix_now().saturating_sub(ts);
                println!("    last: {execution_id} ({status}, completed {age}s ago)");
            }
            None => println!("    last: {execution_id} ({status}, in flight)"),
        }
    }
    print_drift(&ctx, &data.drift);
    // A public entry turning callers away, so the author knows a limit
    // is acting and which one (each is a setting on the trigger).
    if !data.limited.is_empty() {
        println!("  refused calls (last two minutes):");
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
fn warn_no_drift(why: &str, e: &anyhow::Error) {
    eprintln!("warning: {why}, so whether the running program is behind the source is not checked ({e:#})");
}

/// One desired hash, or `None` with a warning naming what could not be
/// worked out.
fn or_warn(what: &str, hash: Result<String>) -> Option<String> {
    hash.map_err(|e| warn_no_drift(&format!("{what} cannot be worked out"), &e)).ok()
}

/// Every drift bit the dispatcher set, each with the verb that clears
/// it. Silent when nothing drifted.
fn print_drift(ctx: &Ctx, drift: &ProjectDrift) {
    let lines = [
        (drift.infra_drift, format!("infra: source has changed; `{}` rebuilds it", ctx.weft("infra upgrade"))),
        (drift.binary_drift, format!("binary: worker code has changed; the next run or `{}` rebuilds the image", ctx.weft("build"))),
        (drift.definition_drift, "definition: project shape has changed; the next run picks it up".to_string()),
        (drift.activation_drift, format!("activation: the listeners fire an older program; `{}` re-registers them against this one", ctx.weft("resync"))),
    ];
    if lines.iter().all(|(set, _)| !set) {
        return;
    }
    println!("  drift:");
    for (_, line) in lines.iter().filter(|(set, _)| *set) {
        println!("    {line}");
    }
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock past UNIX_EPOCH")
        .as_secs()
}
