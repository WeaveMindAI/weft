//! `weft prune <version>`: delete the version, every descendant
//! version, and all their runs, then let their blobs go. Refuses on
//! head's version, on any version that is (or is an ancestor of) a
//! frozen example's origin, and while any run in the subtree is not
//! terminal. Prints what it removes before asking; `--yes` for scripts.

use anyhow::Context;

use super::versions::{fetch_tree, list_specs, resolve_version, short};
use super::Ctx;

pub async fn run(ctx: Ctx, reference: String, yes: bool) -> anyhow::Result<()> {
    let project = ctx.project()?;
    let client = ctx.client();
    let project_id = project.id().to_string();
    let tree = fetch_tree(&client, &project_id).await?;
    let version = resolve_version(&tree, &reference)?.id.clone();
    // The dispatcher refuses under a frozen example's origin; the
    // origins live in `examples/`, so they ride the request.
    let (specs, unreadable) = list_specs(project)?;
    // A file in `examples/` that does not read as a spec is named and
    // stepped over: a prune must not be stoppable by a half-written
    // `examples/notes.json`, and the frozen origins it needs come from
    // the files that DO read.
    for problem in &unreadable {
        eprintln!("warning: {problem}, so no frozen origin is taken from it");
    }
    let frozen: Vec<String> =
        specs.into_iter().filter_map(|s| s.frozen_from.map(|f| f.version)).collect();
    let base = format!("/projects/{project_id}/versions/{version}?frozen={}", frozen.join(","));
    let plan = client.delete_json(&format!("{base}&plan=true")).await.context("plan the prune")?;
    let versions = plan.pointer("/plan/versions").and_then(|v| v.as_array()).map(|a| a.len()).unwrap_or(0);
    let runs = plan.pointer("/plan/runs").and_then(|v| v.as_array()).map(|a| a.len()).unwrap_or(0);
    let blobs = plan.pointer("/plan/blobs").and_then(|v| v.as_array()).map(|a| a.len()).unwrap_or(0);
    if !yes {
        if ctx.json() {
            anyhow::bail!("prune deletes {versions} versions and {runs} runs; pass --yes");
        }
        println!("About to delete {versions} versions and {runs} runs under {}, freeing {blobs} blobs.", short(&version));
        if !crate::prompt::confirm("Type 'yes' to confirm: ", "--yes")? {
            println!("aborted");
            return Ok(());
        }
    }
    let done = client.delete_json(&base).await.context("prune")?;
    // What was actually removed, not what the plan predicted: the two
    // are separate round trips and the subtree can move in between.
    let count = |field: &str| done.pointer(&format!("/plan/{field}")).and_then(|v| v.as_array()).map(|a| a.len()).unwrap_or(0);
    let (versions, runs, blobs) = (count("versions"), count("runs"), count("blobs"));
    // The freed blobs expire once the referenced set no longer names
    // them: publish it now, from the code on disk, so they start
    // expiring today instead of at the next build.
    // Loudly, because the line below promises the blobs start
    // expiring: if the project does not load, nothing was republished
    // and that promise is false.
    let (mut definition, _) = weft_compiler::hash::load_enriched_project(project)
        .context("the versions were pruned, but the project does not load, so the freed blobs were not released; fix the project and run `weft build`")?;
    super::assets::resolve_project_assets(&client, &project.root, &mut definition, None, true).await?;
    if ctx.json_out(&done)? {
        return Ok(());
    }
    println!("pruned {versions} versions and {runs} runs under {}; {blobs} blobs will expire", short(&version));
    Ok(())
}
