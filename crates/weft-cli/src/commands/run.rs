//! `weft run`: sync this folder with its working copy, have the install
//! stamp it as a version and build it, start a run of that version, and
//! stream logs until completion (or `--detach`). A run it follows to its end sets its exit code the way
//! `weft wait` does (`wait::Outcome`): 0 when it completed, non-zero when
//! it failed or was cancelled. A run it only starts exits 0.
//!
//! A run has no entry point of its own: the dispatcher kicks every root
//! of the graph (a node no wire feeds) and pulses run whatever they
//! reach. The run is shaped by a spec (`weft_core::run_spec`), read
//! from `examples/<name>.json` or built from the flags (`--from`,
//! `--emit`, `--target`, `--before`, `--group`, `--fire`); `--save <name>` writes
//! the flags as a spec. The install reads the example from the version it
//! runs and resolves every value in it there, and refuses a run that could
//! not start before any image is built (`RunPreview`). `--seed` reuses from head's run (or from the run
//! `--seed=<run>` names) every node whose slice of the program did not
//! change. The whole catalog is compiled
//! by default; `--referenced` builds only the types used by this graph.

use anyhow::{bail, Context};
use weft_core::run_flags::{apply_run_flags, spec_from_flags, RunFlags};
use weft_core::run_spec::judge::{judge, Verdict};
use weft_core::run_spec::RunSpec;
use weft_core::builds::RunPreview;
use weft_core::versions::{RunShape, RunStarted, RunStarting, StartRun, StartingRun, VersionRunRequest};

use super::Ctx;
use crate::progress::ActionVerb;

/// Everything `weft run` takes. One struct so the clap arm and the
/// verb never disagree on the list.
#[derive(Debug, Default, Clone)]
pub struct RunArgs {
    pub spec: Option<String>,
    pub detach: bool,
    pub node_set: Option<weft_core::builds::NodeSet>,
    /// `--seed`: `Some("")` for head's run, `Some(run)` for the run named.
    pub seed: Option<String>,
    pub seed_until: Vec<String>,
    pub seed_before: Vec<String>,
    pub root: bool,
    pub flags: RunFlags,
    pub save: Option<String>,
    /// `--check`: only say whether the run could start ([`RunCheck`]).
    pub check: bool,
}

/// What `weft run --check --json` prints: whether the run could start,
/// and when not, the refusal the run itself would give. The editor reads
/// it to disable its Run button with the reason.
// SYNC: RunCheck <-> packages/weft-graph/src/protocol.ts RunCheck
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RunCheck {
    pub ok: bool,
    /// Why it could not start; absent when it could.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refusal: Option<String>,
    /// The infra copies the run would read, the ones the dispatcher's gate
    /// refuses the run for when one is not running
    /// (`weft_core::infra::run_gate::copies_read` over the program the
    /// run is cut to). The check cannot see which are running; whoever
    /// reads it holds them against the install's status. Absent when it
    /// reads none, or when the run could not start.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub infra_read: Vec<InfraCopyRead>,
    /// With more than one target, each target as a run of it alone would
    /// stand. A run of several targets is one run, refused whole when one
    /// of them cannot go; this says which of them stops it and why, and
    /// which could run alone. Absent with one target.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub targets: Vec<TargetCheck>,
}

/// One infra copy a run reads: the node, spelled the way the program
/// reads it (`one.db` for the `db` an include site `one` reaches), and
/// whose copy (absent for the shared one).
// SYNC: InfraCopyRead <-> packages/weft-graph/src/protocol.ts InfraCopyRead
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct InfraCopyRead {
    pub node: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instance: Option<weft_core::instance::InstanceId>,
}

/// One target of a checked run, as a run of it alone would stand.
// SYNC: TargetCheck <-> packages/weft-graph/src/protocol.ts TargetCheck
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TargetCheck {
    pub target: String,
    /// The triggers an input it needs only comes from: a run does not
    /// fire them, so that input gets nothing.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub unfired_triggers: Vec<String>,
    /// Every other reason it cannot start, one per line.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refusal: Option<String>,
    /// The infra copies it reads, as [`RunCheck::infra_read`] says them.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub infra_read: Vec<InfraCopyRead>,
}

impl RunCheck {
    /// The check of a run of `definition` shaped by `spec`.
    fn of(definition: &weft_core::ProjectDefinition, spec: Option<&RunSpec>, args: &RunArgs) -> Self {
        let targets = target_checks(definition, spec, args);
        match infra_read(definition, spec, args) {
            Ok(infra_read) => Self { ok: true, refusal: None, infra_read, targets },
            Err(why) => Self { targets, ..Self::refused(&why) },
        }
    }

    /// A check stopped before the run could be looked at.
    fn refused(why: &anyhow::Error) -> Self {
        Self { ok: false, refusal: Some(format!("{why:#}")), infra_read: Vec::new(), targets: Vec::new() }
    }
}

/// `weft run --check`: compile the program and resolve the run the flags
/// (or the example) describe, the way a run is refused before it starts,
/// and say whether it could. Nothing is built, registered or uploaded;
/// whatever stops the check (the program not compiling, a value that
/// cannot be read) is the refusal, so `--json` always prints its one
/// line.
async fn check(ctx: &Ctx, args: &RunArgs) -> anyhow::Result<std::process::ExitCode> {
    let checked = match prepare_check(ctx, args).await {
        Ok((definition, spec)) => RunCheck::of(&definition, spec.as_ref(), args),
        Err(why) => RunCheck::refused(&why),
    };
    if ctx.json_out(&checked)? {
        return Ok(std::process::ExitCode::SUCCESS);
    }
    match &checked.refusal {
        None => {
            println!("the run can start");
            if !checked.infra_read.is_empty() {
                let copies: Vec<String> = checked
                    .infra_read
                    .iter()
                    .map(|copy| match &copy.instance {
                        Some(instance) => format!("{} (instance '{instance}')", copy.node),
                        None => copy.node.clone(),
                    })
                    .collect();
                println!("while this infra runs, which it reads: {}", copies.join(", "));
            }
            Ok(std::process::ExitCode::SUCCESS)
        }
        Some(refusal) => {
            eprintln!("{refusal}");
            Ok(std::process::ExitCode::FAILURE)
        }
    }
}

/// [`check`]'s preparation: the program and the spec a run started now
/// would read, both from the folder as it is, compiled here: the check is
/// offline, uploads nothing, and answers while the person edits.
async fn prepare_check(ctx: &Ctx, args: &RunArgs) -> anyhow::Result<(weft_core::ProjectDefinition, Option<RunSpec>)> {
    seed_bounds_need_a_seed(args)?;
    let project = ctx.project()?;
    let mut spec = spec_for(ctx, args)?;
    let definition = match weft_compiler::hash::load_program_with_diagnostics(project) {
        Ok((definition, _)) => definition,
        Err(weft_compiler::hash::CompileLoadError::Read(msg)) => bail!("{msg}"),
        Err(weft_compiler::hash::CompileLoadError::Diagnostics(diags)) => {
            bail!("compile failed:\n{}", weft_compiler::render_diagnostics(&diags))
        }
    };
    if let Some(spec) = &mut spec {
        super::assets::resolve_run_values(&ctx.client()?, &project.root, spec).await?;
    }
    Ok((definition, spec))
}

/// The infra copies a run of `definition` shaped by `spec` reads, once
/// the run is held to everything [`judge_run`] refuses.
fn infra_read(definition: &weft_core::ProjectDefinition, spec: Option<&RunSpec>, args: &RunArgs) -> anyhow::Result<Vec<InfraCopyRead>> {
    let verdict = judge_run(definition, spec, args);
    if let Some(refusal) = verdict.refusal() {
        bail!("{refusal}");
    }
    copies_read(definition, &verdict, spec).map_err(anyhow::Error::msg)
}

/// The infra copies the run [`judge_run`] judged reads: the same places
/// the dispatcher's infra gate looks at, over the same cut. A copy no
/// status could settle (a per-instance place, and no instance named) is
/// the refusal, in the dispatcher's own words.
///
/// The cut is made with nothing saved for baked outputs: the dispatcher
/// only counts what a RUNNING copy saved, so an infra node it leaves out
/// for its saved values is running anyway, and naming it here greys
/// nothing.
fn copies_read(definition: &weft_core::ProjectDefinition, verdict: &Verdict, spec: Option<&RunSpec>) -> Result<Vec<InfraCopyRead>, String> {
    let program = verdict.resolved.as_ref().map_or(definition, |resolved| &resolved.program);
    let instance = spec.and_then(|spec| spec.instance.as_ref());
    let wanted = weft_core::infra::run_gate::copies_read(program, None, instance);
    let unnamed: Vec<_> = wanted.iter().filter(|(_, copy)| copy.is_none()).cloned().collect();
    if !unnamed.is_empty() {
        return Err(weft_core::infra::run_gate::infra_not_running(&weft_core::infra::run_gate::missing_copies(&unnamed, &[])));
    }
    Ok(wanted
        .into_iter()
        .filter_map(|(node, copy)| copy.map(|copy| InfraCopyRead { node, instance: copy.cloned() }))
        .collect())
}

/// Each target of a run of several, as a run of it alone would stand
/// ([`TargetCheck`]); none for a run of one target or none.
fn target_checks(definition: &weft_core::ProjectDefinition, spec: Option<&RunSpec>, args: &RunArgs) -> Vec<TargetCheck> {
    let Some(spec) = spec.filter(|spec| spec.target.len() > 1) else {
        return Vec::new();
    };
    spec.target
        .iter()
        .map(|target| {
            let alone = RunSpec { target: vec![target.clone()], ..spec.clone() };
            let verdict = judge_run(definition, Some(&alone), args);
            let unfired_triggers: Vec<String> = verdict
                .unfed
                .iter()
                .filter_map(|unfed| unfed.trigger.clone())
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
                .collect();
            let mut reasons: Vec<String> = verdict
                .unfed
                .iter()
                .filter(|unfed| unfed.trigger.is_none())
                .map(|unfed| unfed.message.clone())
                .chain(verdict.refused.iter().cloned())
                .collect();
            // A cut that could not be made has no infra to name.
            let infra_read = match verdict.resolved {
                None => Vec::new(),
                Some(_) => copies_read(definition, &verdict, Some(&alone)).unwrap_or_else(|why| {
                    // The instance refusal above already says it.
                    if reasons.is_empty() {
                        reasons.push(why);
                    }
                    Vec::new()
                }),
            };
            TargetCheck {
                target: target.clone(),
                unfired_triggers,
                refusal: (!reasons.is_empty()).then(|| reasons.join("\n")),
                infra_read,
            }
        })
        .collect()
}

pub async fn run(ctx: Ctx, args: RunArgs) -> anyhow::Result<std::process::ExitCode> {
    if args.check {
        return check(&ctx, &args).await;
    }
    let ctx_inner = ctx.clone();
    ctx.with_progress(ActionVerb::Run, |progress| async move {
        run_inner(&ctx_inner, &progress, args).await
    })
    .await
}

/// The spec a run of `args` starts with, as this folder holds it: the
/// saved example edited by the flags, or the flags alone. What `--save`
/// writes and what a check judges; a real run is shaped by the same
/// example and flags on the install ([`run_shape`]).
fn spec_for(ctx: &Ctx, args: &RunArgs) -> anyhow::Result<Option<RunSpec>> {
    match (&args.spec, args.flags.is_empty()) {
        (Some(name), _) => {
            let spec = super::versions::read_spec(ctx.project()?, name)?;
            Ok(Some(apply_run_flags(&spec.without_expected(), &args.flags).map_err(anyhow::Error::msg)?))
        }
        (None, true) => Ok(args.save.as_ref().map(RunSpec::whole)),
        (None, false) => {
            let name = args.save.clone().unwrap_or_else(|| "one-off".to_string());
            Ok(Some(spec_from_flags(&name, &args.flags).map_err(anyhow::Error::msg)?))
        }
    }
}

/// What a run of `args` asks the install to start with: the example the
/// version holds and the flags that edit it. Once `--save` wrote the
/// flags into the saved example, the example alone. A spec named by path
/// (`examples/chain.json`, or absolute) is named the way the version holds
/// it, relative to the project `root`.
fn run_shape(root: &std::path::Path, args: &RunArgs) -> anyhow::Result<RunShape> {
    if let Some(saved) = &args.save {
        return Ok(RunShape { example: Some(saved.clone()), flags: RunFlags::default() });
    }
    let example = args.spec.as_deref().map(|name| example_in_version(root, name)).transpose()?;
    Ok(RunShape { example, flags: args.flags.clone() })
}

/// The example `name` as the version holds it: a bare name as it is
/// (`examples/<name>.json`), a path to a `.json` file relative to the
/// project. A file outside the project is no file of the version.
fn example_in_version(root: &std::path::Path, name: &str) -> anyhow::Result<String> {
    if !name.ends_with(".json") {
        return Ok(name.to_string());
    }
    let path = std::path::Path::new(name);
    let relative = match path.strip_prefix(root) {
        Ok(inside) => inside,
        Err(_) if path.is_absolute() => bail!(
            "{name} is outside the project, and a run reads its example from the project's files: \
             move it under {}",
            root.join("examples").display()
        ),
        Err(_) => path,
    };
    let parts: Option<Vec<&str>> = relative.components().map(|part| match part {
        std::path::Component::Normal(part) => part.to_str(),
        _ => None,
    }).collect();
    match parts {
        Some(parts) if !parts.is_empty() => Ok(parts.join("/")),
        _ => bail!("{name} is not a path inside the project, and a run reads its example from the project's files"),
    }
}

/// The run a build for `args` is held to before any image is built.
fn run_preview(shape: RunShape, args: &RunArgs) -> RunPreview {
    RunPreview { shape, seeded: args.seed.is_some(), seed_until: args.seed_until.clone(), seed_before: args.seed_before.clone() }
}

/// `--seed-until` and `--seed-before` bound a seed: without `--seed`
/// they are refused before anything is read or saved.
fn seed_bounds_need_a_seed(args: &RunArgs) -> anyhow::Result<()> {
    if (!args.seed_until.is_empty() || !args.seed_before.is_empty()) && args.seed.is_none() {
        bail!("--seed-until and --seed-before need --seed");
    }
    Ok(())
}

async fn run_inner(ctx: &Ctx, progress: &crate::progress::Progress, args: RunArgs) -> anyhow::Result<std::process::ExitCode> {
    seed_bounds_need_a_seed(&args)?;
    let seed_run = match args.seed.as_deref() {
        Some(run) if !run.is_empty() => Some(super::resolve_run_id(ctx, run).await?.parse().context("a resolved run id")?),
        _ => None,
    };
    let project = ctx.project()?;
    // Read here for what `--save` writes and what the summary says; the
    // install reads the same example from the version, which the sync
    // below makes of this folder.
    let mut spec = spec_for(ctx, &args)?;
    if let (Some(name), Some(spec)) = (&args.save, &mut spec) {
        spec.name = name.clone();
    }
    if let Some(name) = &args.save {
        let spec = spec.as_ref().expect("saving constructs a run spec");
        if super::versions::read_spec_if_present(project, name)?.is_some_and(|existing| existing.is_frozen()) {
            bail!("example '{name}' contains accepted results; save parameters under another name, or freeze an accepted new run to replace it");
        }
        let path = super::versions::write_spec(project, &RunSpec { name: name.clone(), ..spec.clone() })?;
        if !ctx.json() {
            println!("saved {}", path.display());
        }
    }
    let shape = run_shape(&project.root, &args)?;
    let request = weft_core::builds::BuildRequest {
        node_set: args.node_set.unwrap_or(weft_core::builds::NodeSet::Full),
        root: args.root,
        run: Some(run_preview(shape.clone(), &args)),
        ..Default::default()
    };
    let handle = super::ensure::ensure_built(ctx, progress, request).await?;
    if !ctx.json() {
        println!("registered {} ({})", handle.name, handle.id);
    }
    let body = VersionRunRequest {
        working_copy: weft_core::working_copy::WorkingCopyRef { working_copy: Some(handle.copy.id) },
        version: handle.version.clone(),
        definition_hash: handle.definition_hash().to_string(),
        binary_hash: handle.binary_hash().to_string(),
        seed: args.seed.is_some(),
        seed_run,
        seed_until: args.seed_until.clone(),
        seed_before: args.seed_before.clone(),
        shape,
    };
    let path = format!("/projects/{}/versions/runs", handle.id);
    progress.dispatcher_call_start(&path);
    let started = start_run(&handle.client, progress, &handle.id, body).await?;
    let run_id = started.run_id;
    progress.dispatcher_call_done(serde_json::json!({ "runId": run_id, "projectId": handle.id }));

    let summary = summary_line(&started, spec.as_ref());
    if !ctx.json() {
        println!("started run {run_id} on version {}", super::versions::short(&started.version));
    }
    // After the line that says the run started, because that is what
    // they are about ("this run started, but head moved ..."). One
    // shape for a warning whichever verb produced it: the `Warning` phase.
    for w in &started.warnings {
        progress.warn(w);
    }
    handle.warn_broken(progress);
    // The summary is the completion's own line: `Progress` prints it in
    // human mode, so printing it above as well said it twice.
    progress.complete(&summary);

    // --json implies --detach: the extension uses SSE for run
    // events, so keeping the CLI alive to follow logs would just hold
    // the action-bar state machine in `cli_running` while the run is
    // actually `run_running`.
    if args.detach || ctx.json() {
        return Ok(std::process::ExitCode::SUCCESS);
    }
    let outcome = super::follow::follow_run_id(&handle.client, ctx.on(), &run_id.to_string(), Some(&handle.built.definition)).await?;
    Ok(outcome.exit_code())
}

/// [`judge`] over what `weft run` was asked: whether it seeds, and the
/// bounds of the seed.
fn judge_run(definition: &weft_core::ProjectDefinition, spec: Option<&RunSpec>, args: &RunArgs) -> Verdict {
    judge(definition, spec, args.seed.is_some(), &args.seed_until, &args.seed_before)
}

/// Start the run: hand `request` to the task that starts it, under an id
/// made here (asked again under the same id while the install is out of
/// reach, so it is started once), and follow that task, saying what it is
/// doing whenever that changes and every [`crate::progress::SAY_AGAIN_EVERY`]
/// meanwhile. The resolver's 422 reads as its lines: every missing input
/// at once, each naming the three ways to satisfy it.
// SYNC: start_run <-> weft_core::versions::StartRun
pub async fn start_run(
    client: &crate::client::DispatcherClient,
    progress: &crate::progress::Progress,
    project_id: &str,
    request: VersionRunRequest,
) -> anyhow::Result<RunStarted> {
    let ask = serde_json::to_value(StartRun { task: uuid::Uuid::now_v7(), request })?;
    let path = format!("/projects/{project_id}/versions/runs");
    let asked = std::time::Instant::now();
    let starting: StartingRun = loop {
        match client.post_json(&path, &ask).await {
            Ok(answer) => break crate::client::read_answer(answer, "the task starting the run")?,
            Err(e) => {
                let why = super::ensure::look_again_after(e).map_err(|e| e.context("start run"))?;
                progress.warn(&format!("could not hand the run to the install just now ({why}); asking again"));
                tokio::time::sleep(super::follow::TASK_LOOK_AGAIN_AFTER).await;
            }
        }
    };
    // Nothing said yet reads as no stage: the start reads what the run
    // starts with, which takes moments.
    let mut said = crate::progress::SaidLast::announced(None, std::time::Duration::ZERO);
    let answer = super::follow::follow_task(client, progress, project_id, starting.task, async |stage| {
        let stage: Option<RunStarting> = stage.map(serde_json::from_value).transpose().context("read where the run's start is")?;
        let elapsed = asked.elapsed();
        if let Some(Some(stage)) = said.hear(stage, elapsed) {
            progress.run_starting(stage, elapsed);
        }
        Ok(())
    })
    .await
    .map_err(|e| match crate::client::refusal(&e).map(|refused| refused.status()) {
        Some(reqwest::StatusCode::UNPROCESSABLE_ENTITY) => anyhow::anyhow!("the run cannot start:\n{e}"),
        _ => e.context("start run"),
    })?;
    crate::client::read_answer(answer, "the run it started")
}

/// The one line a run ends its start with: what was inherited and
/// what runs, or what the spec covered.
pub fn summary_line(started: &RunStarted, spec: Option<&RunSpec>) -> String {
    let mut parts: Vec<String> = Vec::new();
    match &started.seed {
        Some(seed) => {
            parts.push(format!("{} nodes inherited from {}", started.inherited.len(), super::versions::short(&seed.to_string())));
            parts.push(format!("{} ran ({})", started.ran.len(), started.ran.join(", ")));
        }
        None => parts.push(format!("{} nodes", started.ran.len())),
    }
    if let Some(spec) = spec {
        let mut what = vec![format!("spec {}", spec.name)];
        match spec.from.values().map(|ports| ports.len()).sum::<usize>() + spec.group.as_ref().map(|(_, ports)| ports.len()).unwrap_or(0) {
            0 => {}
            1 => what.push("1 handed value".to_string()),
            n => what.push(format!("{n} handed values")),
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

    fn started(seed: Option<&str>, inherited: &[&str], ran: &[&str]) -> RunStarted {
        RunStarted {
            run_id: uuid::Uuid::nil(),
            version: "v".into(),
            seed: seed.map(|s| format!("{s}-0000-0000-0000-000000000000").parse().expect("a uuid")),
            inherited: inherited.iter().map(|s| s.to_string()).collect(),
            ran: ran.iter().map(|s| s.to_string()).collect(),
            warnings: vec![],
        }
    }

    /// `source` compiled as `main.weft`, with `files` beside it.
    fn compiled(source: &str, files: &[(&str, &str)]) -> weft_core::ProjectDefinition {
        let dir = tempfile::tempdir().unwrap();
        for (name, text) in files {
            std::fs::write(dir.path().join(name), text).unwrap();
        }
        let catalog = weft_catalog::FsCatalog::discover(&weft_catalog::stdlib_root().unwrap()).unwrap();
        let (definition, diagnostics) = weft_compiler::compile_strict(
            source,
            uuid::Uuid::new_v4(),
            weft_compiler::CompileFs::disk(dir.path()),
            &catalog,
            weft_compiler::validate::ValidationMode::Structural,
            None,
        );
        assert!(
            !diagnostics.iter().any(|d| matches!(d.severity, weft_compiler::Severity::Error)),
            "compiles: {}",
            weft_compiler::render_diagnostics(&diagnostics)
        );
        definition
    }

    fn checked(definition: &weft_core::ProjectDefinition, targets: &[&str]) -> serde_json::Value {
        let spec = RunSpec { target: targets.iter().map(|t| t.to_string()).collect(), ..RunSpec::whole("check") };
        serde_json::to_value(super::RunCheck::of(definition, Some(&spec), &RunArgs::default())).unwrap()
    }

    #[test]
    fn a_check_prints_ok_or_the_refusal() {
        let refused = serde_json::to_value(super::RunCheck::refused(&anyhow::anyhow!("needs `door`"))).unwrap();
        assert_eq!(refused, serde_json::json!({ "ok": false, "refusal": "needs `door`" }));
        let definition = compiled("lone = Text { value: \"hi\" }\nshow = Debug\nshow.data = lone.value\n", &[]);
        assert_eq!(checked(&definition, &["show"]), serde_json::json!({ "ok": true }));
    }

    /// The check names the infra a run reads the way the dispatcher's gate
    /// does: an infra node wired straight into the target, never started
    /// or not (the check knows nothing of status), and through an include,
    /// whether the target reads out of the include site or is the site
    /// itself, spelled by its site, and only the site the run reaches.
    #[test]
    fn a_check_names_the_infra_a_run_reads() {
        let direct = compiled("postgresDatabase1 = PostgresDatabase\ndebug1 = Debug\ndebug1.data = postgresDatabase1.access\n", &[]);
        assert_eq!(checked(&direct, &["debug1"]), serde_json::json!({ "ok": true, "infraRead": [{ "node": "postgresDatabase1" }] }));

        let store = "Group() -> (conn: Access) {\n  db = PostgresDatabase\n  self.conn = db.access\n}\n";
        let included = compiled(
            "s = @include(\"store.weft\")\nt = @include(\"store.weft\")\nshow = Debug\nshow.data = s.conn\nlone = Text { value: \"hi\" }\n",
            &[("store.weft", store)],
        );
        let reads = |targets: &[&str]| checked(&included, targets)["infraRead"].clone();
        assert_eq!(reads(&["show"]), serde_json::json!([{ "node": "s.db" }]));
        assert_eq!(reads(&["s"]), serde_json::json!([{ "node": "s.db" }]));
        assert_eq!(reads(&["lone"]), serde_json::Value::Null);

        // Infra declared in the including file, read inside a group.
        let grouped = compiled(
            "pg = PostgresDatabase\nwrap = Group(conn: Access) -> () {\n  inner = Debug\n  inner.data = self.conn\n}\nwrap.conn = pg.access\n",
            &[],
        );
        assert_eq!(checked(&grouped, &["wrap.inner"])["infraRead"], serde_json::json!([{ "node": "pg" }]));
    }

    /// A run of several targets is one run, refused whole; the check also
    /// says each target alone: the one behind a trigger names that trigger,
    /// the one behind infra names the infra it reads, the free one nothing.
    #[test]
    fn a_check_of_several_targets_says_each_alone() {
        let definition = compiled(
            "door = Cron { cron: \"0 0 * * * *\" }\nreply = Format(p: String) { template: \"{{p}}\", p: door.scheduledTime }\n\
             pg = PostgresDatabase\ndebug = Debug\ndebug.data = pg.access\nlone = Text { value: \"hi\" }\n",
            &[],
        );
        let all = checked(&definition, &["reply", "debug", "lone"]);
        assert_eq!(all["ok"], false);
        assert_eq!(
            all["targets"],
            serde_json::json!([
                { "target": "reply", "unfiredTriggers": ["door"] },
                { "target": "debug", "infraRead": [{ "node": "pg" }] },
                { "target": "lone" },
            ])
        );
        // Without the trigger's side, the run of the rest goes, reading pg.
        let rest = checked(&definition, &["debug", "lone"]);
        assert_eq!(rest["ok"], true);
        assert_eq!(rest["infraRead"], serde_json::json!([{ "node": "pg" }]));
        assert_eq!(rest["targets"].as_array().map(Vec::len), Some(2));
    }

    /// What a run asks the install to start with: the example and the
    /// flags as given, the saved example alone once `--save` wrote the
    /// flags into it, and a spec named by path named as the version holds it.
    #[test]
    fn a_run_asks_for_its_example_and_flags() {
        let root = std::path::Path::new("/home/me/shop");
        let flags = RunFlags { target: vec!["reply".into()], ..Default::default() };
        let args = RunArgs { spec: Some("angry".into()), flags: flags.clone(), ..Default::default() };
        assert_eq!(run_shape(root, &args).unwrap(), RunShape { example: Some("angry".into()), flags: flags.clone() });
        let saved = RunArgs { save: Some("calm".into()), ..args.clone() };
        assert_eq!(run_shape(root, &saved).unwrap(), RunShape { example: Some("calm".into()), flags: RunFlags::default() });
        assert_eq!(run_shape(root, &RunArgs::default()).unwrap(), RunShape::default());
        let wire = serde_json::to_value(run_shape(root, &args).unwrap()).unwrap();
        assert_eq!(wire["example"], "angry");
        assert!(wire.get("flags").is_some(), "{wire}");

        assert_eq!(example_in_version(root, "examples/chain.json").unwrap(), "examples/chain.json");
        assert_eq!(example_in_version(root, "/home/me/shop/examples/chain.json").unwrap(), "examples/chain.json");
        assert!(format!("{:#}", example_in_version(root, "/tmp/chain.json").unwrap_err()).contains("outside the project"));
        assert!(example_in_version(root, "../chain.json").is_err());
    }

    /// The build is held to the run it is for: what it starts with, and
    /// whether a seed feeds it.
    #[test]
    fn a_build_for_a_run_carries_its_seed() {
        let args = RunArgs { seed: Some(String::new()), seed_until: vec!["a".into()], ..Default::default() };
        let preview = run_preview(RunShape::default(), &args);
        assert!(preview.seeded);
        assert_eq!(preview.seed_until, vec!["a"]);
        assert!(!run_preview(RunShape::default(), &RunArgs::default()).seeded);
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
        assert_eq!(summary_line(&s, Some(&spec)), "spec angry-customer, 1 handed value, fired inbound, 3 nodes");
    }
}
