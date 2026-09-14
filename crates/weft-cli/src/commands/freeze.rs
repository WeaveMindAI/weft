//! `weft freeze <name> [<color>]`: write `examples/<name>.json` from a
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

pub async fn run(ctx: Ctx, name: String, color: Option<String>, expect: Vec<String>) -> anyhow::Result<()> {
    // Refused up front rather than after reading the run: the same rule
    // `write_spec` enforces, said before any work is done.
    super::versions::validate_example_name(&name)?;
    let project = ctx.project()?;
    let client = ctx.client();
    let project_id = project.id().to_string();
    let tree = fetch_tree(&client, &project_id).await?;
    let run = match color {
        Some(c) => resolve_run(&tree, &c)?.clone(),
        None => {
            let head = tree.head.head_run.clone().ok_or_else(|| {
                anyhow::anyhow!("head has no run to freeze; name a color (`weft tree` lists them) or run first")
            })?;
            resolve_run(&tree, &head)?.clone()
        }
    };
    if run.status != "completed" {
        bail!(
            "run {} is {}; only a completed run freezes (`weft stop`, fix, run again)",
            short(&run.color),
            run.status
        );
    }
    let rows = super::versions::replay_rows(&client, &run.color).await?;
    let mut expected = super::versions::output_wires(&client, &project_id, &run.color).await?;
    // `--expect` and the run's node list are both spelled from the top
    // (`triage.last`, `one.strip` through its site).
    for node in &expect {
        if let Some((group, _)) = node.rsplit_once("__in").or_else(|| node.rsplit_once("__out")).filter(|(_, rest)| rest.is_empty()) {
            bail!("cannot focus '{node}': it is a group boundary the compiler made; name the group, `--expect {group}`");
        }
        if !expected.nodes.contains(node) { bail!("cannot focus '{node}': this node did not exist in run {}", short(&run.color)); }
    }
    expected.focus = expect.into_iter().collect::<std::collections::BTreeSet<_>>().into_iter().collect();
    let facts = outside_facts(&rows);

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
        color: run.color.parse().context("run color")?,
        definition_hash: run.definition_hash.clone(),
    });
    spec.expected = Some(expected);
    let path = write_spec(project, &spec)?;
    // The run remembers what it was frozen as.
    client
        .put_with_body(
            &format!("/projects/{project_id}/versions/runs/{}", run.color),
            &serde_json::json!({ "example": name }),
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
        short(&run.color),
        spec.expected.as_ref().map(|e| e.wires.len()).unwrap_or(0),
        spec.answers.len()
    );
    Ok(())
}
