//! `weft freeze <name> [<run_id>]`: write `examples/<name>.json` from a
//! run (default: this folder's head's run): its spec (or the whole-graph spec it
//! ran), its outside facts (the answers people gave, the caller
//! messages), `frozen_from`, and `expected` from every wire. Replacing an
//! example takes its parameters and accepted outputs from the named run.
//! Refuses unless the run completed,
//! and refuses to overwrite a spec file that exists but does not parse.

use anyhow::{bail, Context};
use weft_core::run_spec::{FrozenFrom, RunSpec};

use super::versions::{fetch_tree, outside_facts, read_spec_if_present, resolve_run, short, write_spec};
use super::Ctx;

pub async fn run(ctx: Ctx, name: String, run_id: Option<String>, expect: Vec<String>) -> anyhow::Result<()> {
    // Refused up front rather than after reading the run: the same rule
    // `write_spec` enforces, said before any work is done.
    super::versions::validate_example_name(&name)?;
    let project = ctx.project()?;
    // The run is read and the example written here, which the next sync
    // sends up: nothing of the folder has to go up first, so it is only
    // looked up, for its head's run.
    let here = super::copy::look_up_this_copy(&ctx).await?;
    let (client, project_id) = (&here.client, here.project_id.as_str());
    let tree = fetch_tree(client, project_id).await?;
    let run = match run_id {
        Some(c) => resolve_run(&tree, &c)?.clone(),
        None => {
            let head = here.copy.as_ref().and_then(|copy| copy.head.head_run).ok_or_else(|| {
                anyhow::anyhow!("this folder's head has no run to freeze; name a run (`{}` lists them) or run first", ctx.weft("tree"))
            })?;
            resolve_run(&tree, &head.to_string())?.clone()
        }
    };
    let status = run.listing.as_ref().map(|l| l.status);
    if status != Some(weft_core::program::RunStatus::Completed) {
        bail!(
            "run {} is {}; only a completed run freezes (`{}`, fix, run again)",
            short(&run.run_id.to_string()),
            status.map_or("unknown", |s| s.as_str()),
            ctx.weft("stop")
        );
    }
    let rows = super::versions::replay_rows(client, &run.run_id.to_string()).await?;
    // Each `--expect` is read against the program the run ran, which this
    // folder may have moved on from, and comes back in `focus` spelled the
    // way that program writes it.
    let expected = super::versions::output_wires(client, &run.run_id.to_string(), &expect).await?;
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
        run_id: run.run_id,
        definition_hash: run.definition_hash.clone(),
    });
    spec.expected = Some(expected);
    let path = write_spec(project, &spec)?;
    // The run remembers what it was frozen as.
    client
        .put_with_body(
            &format!("/projects/{project_id}/versions/runs/{}", run.run_id),
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
        short(&run.run_id.to_string()),
        spec.expected.as_ref().map(|e| e.wires.len()).unwrap_or(0),
        spec.answers.len()
    );
    Ok(())
}
