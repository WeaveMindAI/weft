//! `weft run`: compile + register the cwd project, record the code as a
//! version, start a run under it, stream logs until completion (or
//! `--detach`).
//!
//! A run has no entry point of its own: the dispatcher kicks every root
//! of the graph (a node no wire feeds) and pulses run whatever they
//! reach. The run is shaped by a spec (`weft_core::run_spec`), read
//! from `examples/<name>.json` or built from the flags (`--from`,
//! `--emit`, `--target`, `--before`, `--group`, `--fire`); `--save <name>` writes
//! the flags as a spec. `--seed` reuses from head's run every node
//! whose slice of the program did not change. The whole catalog is compiled
//! by default; `--referenced` builds only the types used by this graph.

use anyhow::{bail, Context};
use weft_core::run_spec::{Refusal, RunSpec};

use super::versions::{spec_from_flags, RunFlags};
use super::Ctx;
use crate::progress::ActionVerb;

/// Everything `weft run` takes. One struct so the clap arm and the
/// verb never disagree on the list.
#[derive(Debug, Default, Clone)]
pub struct RunArgs {
    pub spec: Option<String>,
    pub detach: bool,
    pub node_set: Option<weft_compiler::codegen::NodeSet>,
    pub seed: bool,
    pub seed_until: Vec<String>,
    pub seed_before: Vec<String>,
    pub root: bool,
    pub flags: RunFlags,
    pub save: Option<String>,
}

pub async fn run(ctx: Ctx, args: RunArgs) -> anyhow::Result<()> {
    let ctx_inner = ctx.clone();
    ctx.with_progress(ActionVerb::Run, |progress| async move {
        run_inner(&ctx_inner, &progress, args).await
    })
    .await
}

/// Load saved parameters, apply explicit edits, and omit accepted output
/// evidence from the execution request. Graph validation sees the final edit.
fn spec_for(ctx: &Ctx, args: &RunArgs) -> anyhow::Result<Option<RunSpec>> {
    match (&args.spec, args.flags.is_empty()) {
        (Some(name), _) => {
            let spec = super::versions::read_spec(ctx.project()?, name)?;
            Ok(Some(super::versions::apply_run_flags(&spec.without_expected(), &args.flags)?))
        }
        (None, true) => Ok(args.save.as_ref().map(RunSpec::whole)),
        (None, false) => {
            let name = args.save.clone().unwrap_or_else(|| "one-off".to_string());
            Ok(Some(spec_from_flags(&name, &args.flags)?))
        }
    }
}

async fn run_inner(ctx: &Ctx, progress: &crate::progress::Progress, args: RunArgs) -> anyhow::Result<()> {
    if (!args.seed_until.is_empty() || !args.seed_before.is_empty()) && !args.seed {
        bail!("--seed-until and --seed-before need --seed");
    }
    let mut spec = spec_for(ctx, &args)?;
    if let (Some(name), Some(spec)) = (&args.save, &mut spec) {
        spec.name = name.clone();
    }
    if let Some(name) = &args.save {
        let spec = spec.as_ref().expect("saving constructs a run spec");
        if super::versions::read_spec_if_present(ctx.project()?, name)?.is_some_and(|existing| existing.is_frozen()) {
            bail!("example '{name}' contains accepted results; save parameters under another name, or freeze an accepted new run to replace it");
        }
        let path = super::versions::write_spec(ctx.project()?, &RunSpec { name: name.clone(), ..spec.clone() })?;
        if !ctx.json() {
            println!("saved {}", path.display());
        }
    }
    let compiled = super::ensure::compile_project(ctx, progress)?;
    validate_run(&compiled.definition, spec.as_ref(), &args)?;
    let node_set = args.node_set.unwrap_or(weft_compiler::codegen::NodeSet::Full);
    let handle = super::ensure::register_compiled(ctx, progress, node_set, compiled).await?;
    if !ctx.json() {
        println!("registered {} ({})", handle.name, handle.id);
    }
    // SYNC: body <-> crates/weft-dispatcher/src/api/versions.rs VersionRunRequest
    let body = serde_json::json!({
        "manifest": handle.manifest,
        "definitionHash": handle.plan.definition_hash,
        "binaryHash": handle.plan.binary_hash,
        "seed": args.seed,
        "seedUntil": args.seed_until,
        "seedBefore": args.seed_before,
        "root": args.root,
        "spec": spec,
        "example": args.save.as_ref().or(args.spec.as_ref()).and_then(|_| spec.as_ref().map(|spec| &spec.name)),
    });
    let path = format!("/projects/{}/versions/runs", handle.id);
    progress.dispatcher_call_start(&path);
    let started = start_run(&handle.client, &path, &body).await?;
    let color = started.color.clone();
    progress.dispatcher_call_done(serde_json::json!({ "color": color, "project_id": handle.id }));

    let summary = summary_line(&started, spec.as_ref());
    if !ctx.json() {
        println!("started color {color} on version {}", super::versions::short(&started.version));
    }
    // After the line that says the run started, because that is what
    // they are about ("this run started, but head moved ..."). One
    // shape for a warning whichever verb produced it: the `Warning` phase.
    for w in &started.warnings {
        progress.warn(w);
    }
    // The summary is the completion's own line: `Progress` prints it in
    // human mode, so printing it above as well said it twice.
    progress.complete(&summary);

    // --json implies --detach: the extension uses SSE for execution
    // events, so keeping the CLI alive to follow logs would just hold
    // the action-bar state machine in `cli_running` while the run is
    // actually `execution_running`.
    if args.detach || ctx.json() {
        return Ok(());
    }
    super::follow::follow_color(&handle.client, &color).await?;
    Ok(())
}

fn validate_run(definition: &weft_core::ProjectDefinition, spec: Option<&RunSpec>, args: &RunArgs) -> anyhow::Result<()> {
    if let Some(spec) = spec {
        weft_core::run_spec::resolve_spec(spec, definition)
            .map_err(|error| anyhow::anyhow!("the run cannot start:\n{error}"))?;
    }
    weft_core::project::selection::RunSelection::carve(definition, &weft_core::project::selection::SelectionBounds {
        target: args.seed_until.clone(), before: args.seed_before.clone(), ..Default::default()
    }).map_err(anyhow::Error::msg)?;
    Ok(())
}

// SYNC: Started <-> crates/weft-dispatcher/src/api/versions.rs VersionRunResponse
#[derive(Debug, serde::Deserialize)]
pub struct Started {
    pub color: String,
    pub version: String,
    pub seed: Option<String>,
    pub inherited: Vec<String>,
    pub ran: Vec<String>,
    pub warnings: Vec<String>,
}

/// Start the run, turning the resolver's 422 into its lines: every
/// missing input at once, each naming the three ways to satisfy it.
pub async fn start_run(client: &crate::client::DispatcherClient, path: &str, body: &serde_json::Value) -> anyhow::Result<Started> {
    let (status, text) = client.post_json_status(path, body).await.context("start run")?;
    match status {
        200..=299 => serde_json::from_str(&text).context("parse run response"),
        422 => {
            let refusal: Refusal = serde_json::from_str(&text).context("parse the refusal")?;
            bail!("the run cannot start:\n{refusal}")
        }
        _ => bail!("{}", if text.trim().is_empty() { format!("dispatcher returned {status}") } else { text.trim().to_string() }),
    }
}

/// The one line a run ends its start with: what was inherited and
/// what runs, or what the spec covered.
pub fn summary_line(started: &Started, spec: Option<&RunSpec>) -> String {
    let mut parts: Vec<String> = Vec::new();
    match &started.seed {
        Some(seed) => {
            parts.push(format!("{} nodes inherited from {}", started.inherited.len(), super::versions::short(seed)));
            parts.push(format!("{} ran ({})", started.ran.len(), started.ran.join(", ")));
        }
        None => parts.push(format!("{} nodes", started.ran.len())),
    }
    if let Some(spec) = spec {
        let mut what = vec![format!("spec {}", spec.name)];
        match spec.from.values().map(|ports| ports.len()).sum::<usize>() + spec.group.as_ref().map(|(_, ports)| ports.len()).unwrap_or(0) {
            0 => {}
            1 => what.push("1 input backup".to_string()),
            n => what.push(format!("{n} input backups")),
        }
        if let Some((node, _)) = &spec.fire {
            what.push(format!("fired {node}"));
        }
        parts.insert(0, what.join(", "));
    }
    parts.join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn started(seed: Option<&str>, inherited: &[&str], ran: &[&str]) -> Started {
        Started {
            color: "c".into(),
            version: "v".into(),
            seed: seed.map(str::to_string),
            inherited: inherited.iter().map(|s| s.to_string()).collect(),
            ran: ran.iter().map(|s| s.to_string()).collect(),
            warnings: vec![],
        }
    }

    #[test]
    fn the_summary_line_says_what_was_inherited_and_what_ran() {
        let s = started(Some("3f2a1111"), &["a", "b"], &["classify", "reply"]);
        assert_eq!(summary_line(&s, None), "2 nodes inherited from 3f2a1111, 2 ran (classify, reply)");
        let s = started(None, &[], &["a", "b", "c"]);
        assert_eq!(summary_line(&s, None), "3 nodes");
        let mut spec = RunSpec::whole("angry-customer");
        spec.from.entry("x".into()).or_default().insert("p".into(), serde_json::json!(1));
        spec.fire = Some(("inbound".into(), serde_json::json!({})));
        assert_eq!(summary_line(&s, Some(&spec)), "spec angry-customer, 1 input backup, fired inbound, 3 nodes");
    }
}
