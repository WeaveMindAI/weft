//! `weft freeze <name> [<execution_id>]`: write `examples/<name>.json` from a
//! run (default: head's run): its spec (or the whole-graph spec it
//! ran), its outside facts (the answers people gave, the caller
//! messages), `frozen_from`, and `expected` from every wire. Replacing an
//! example takes its parameters and accepted outputs from the named run.
//! Refuses unless the run completed,
//! and refuses to overwrite a spec file that exists but does not parse.

use anyhow::{bail, Context};
use weft_core::run_spec::{FrozenFrom, RunSpec};

use super::versions::{fetch_tree, outside_facts, read_spec_if_present, resolve_run, short, write_spec};
use super::Ctx;

pub async fn run(ctx: Ctx, name: String, execution_id: Option<String>, expect: Vec<String>) -> anyhow::Result<()> {
    // Refused up front rather than after reading the run: the same rule
    // `write_spec` enforces, said before any work is done.
    super::versions::validate_example_name(&name)?;
    let project = ctx.project()?;
    let client = ctx.client()?;
    let project_id = project.id().to_string();
    let tree = fetch_tree(&client, &project_id).await?;
    let run = match execution_id {
        Some(c) => resolve_run(&tree, &c)?.clone(),
        None => {
            let head = tree.head.head_run.ok_or_else(|| {
                anyhow::anyhow!("head has no run to freeze; name an execution (`{}` lists them) or run first", ctx.weft("tree"))
            })?;
            resolve_run(&tree, &head.to_string())?.clone()
        }
    };
    if run.status != Some(weft_core::program::RunStatus::Completed) {
        bail!(
            "run {} is {}; only a completed run freezes (`{}`, fix, run again)",
            short(&run.execution_id.to_string()),
            run.status.map_or("unknown", |s| s.as_str()),
            ctx.weft("stop")
        );
    }
    let rows = super::versions::replay_rows(&client, &run.execution_id.to_string()).await?;
    let mut expected = super::versions::output_wires(&client, &run.execution_id.to_string()).await?;
    // `--expect` and the run's node list are both spelled from the top
    // (`triage.last`, `one.strip` through its site).
    for node in &expect {
        if let Some((group, _)) = node.rsplit_once("__in").or_else(|| node.rsplit_once("__out")).filter(|(_, rest)| rest.is_empty()) {
            bail!("cannot focus '{node}': it is a group boundary the compiler made; name the group, `--expect {group}`");
        }
        if !expected.nodes.contains(node) { bail!("cannot focus '{node}': this node did not exist in run {}", short(&run.execution_id.to_string())); }
    }
    expected.focus = expect.into_iter().collect::<std::collections::BTreeSet<_>>().into_iter().collect();
    let facts = outside_facts(&rows)?;

    // A file that exists but does not parse is an
    // error, never an absence: overwriting it would destroy a spec
    // whose only problem was a typo.
    let existing = read_spec_if_present(project, &name)?;
    let mut spec = run.spec.clone().unwrap_or_else(|| RunSpec::whole(&name));
    // An example has to be self-consistent: the answers people gave and
    // the wires they produced come from ONE run, or replaying it checks
    // this run's outputs against another run's answers. So freezing
    // replaces them, and says so when it overwrote something different.
    let replaced = existing.is_some();
    spec.name = name.clone();
    spec.answers = facts.answers;
    spec.caller = facts.caller;
    spec.frozen_from = Some(FrozenFrom {
        version: run.version_id.clone(),
        execution_id: run.execution_id,
        definition_hash: run.definition_hash.clone(),
    });
    spec.expected = Some(expected);
    let path = write_spec(project, &spec)?;
    // The run remembers what it was frozen as.
    client
        .put_with_body(
            &format!("/projects/{project_id}/versions/runs/{}", run.execution_id),
            &serde_json::to_value(weft_core::versions::RunUpdate { example: Some(Some(name.clone())) })?,
        )
        .await
        .context("record the example on the run")?;
    if ctx.json_out(&serde_json::json!({ "path": path, "wires": spec.expected.as_ref().map(|e| e.wires.len()).unwrap_or(0) }))? {
        return Ok(());
    }
    if replaced {
        println!("replaced {} with this run's parameters and accepted outputs", path.display());
    }
    println!(
        "froze {} from run {} ({} wires, {} answers)",
        path.display(),
        short(&run.execution_id.to_string()),
        spec.expected.as_ref().map(|e| e.wires.len()).unwrap_or(0),
        spec.answers.len()
    );
    Ok(())
}
