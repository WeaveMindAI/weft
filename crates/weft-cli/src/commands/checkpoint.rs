//! `weft checkpoint [<label>]`: record the project's files as a version
//! under head, with no run and no build, and move head there. The
//! gesture outside any run: a point to branch back to.

use anyhow::Context;

use super::Ctx;

pub async fn run(ctx: Ctx, label: Option<String>, root: bool) -> anyhow::Result<()> {
    // A checkpoint is a save point, so it must work as the first thing
    // a person ever does in a project: the tree lives on the
    // dispatcher, so tell it the project exists. Source only, no image.
    super::ensure::ensure_project_known(&ctx).await?;
    let project = ctx.project()?;
    let client = ctx.client();
    let id = project.id().to_string();
    let manifest = super::versions::snapshot(&client, project).await?;
    // SYNC: body <-> crates/weft-dispatcher/src/api/versions.rs CheckpointRequest
    let resp = client
        .post_json(
            &format!("/projects/{id}/versions"),
            &serde_json::json!({ "manifest": manifest, "label": label, "root": root }),
        )
        .await
        .context("checkpoint")?;
    let version = resp.get("version").and_then(|v| v.as_str()).context("checkpoint response missing version")?;
    let created = resp.get("created").and_then(|v| v.as_bool()).unwrap_or(false);
    if ctx.json_out(&resp)? {
        return Ok(());
    }
    let short = super::versions::short(version);
    match (created, &label) {
        (true, Some(l)) => println!("checkpoint {short} ({l})"),
        (true, None) => println!("checkpoint {short}"),
        (false, Some(l)) => println!("already at {short}; labelled {l}"),
        (false, None) => println!("already at {short}"),
    }
    Ok(())
}
