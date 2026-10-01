//! `weft running-source <dir>`: write the files of the program an install
//! holds into a new folder, `--on <target>` for another install. What the
//! editor's install switch shows as that install's graph, and how a
//! person reads what prod runs without touching their own tree.

use anyhow::{bail, Context};

use super::branch::{download_into, unrecordable_paths};
use super::versions::Manifest;
use super::Ctx;
use weft_core::versions::RunningSource;

pub async fn run(ctx: Ctx, dir: std::path::PathBuf) -> anyhow::Result<()> {
    let project = ctx.project()?;
    let client = ctx.client()?;
    let id = project.id().to_string();
    if dir.exists() {
        bail!("{} already exists; name a folder that does not", dir.display());
    }
    let running: RunningSource = serde_json::from_value(client.get_json(&format!("/projects/{id}/versions/running")).await?)
        .context("parse the running program's files")?;
    let bad = unrecordable_paths(&running.manifest);
    if !bad.is_empty() {
        bail!(
            "the install lists {}, which weft would never record as a project file; nothing was written",
            bad.iter().map(|p| format!("'{p}'")).collect::<Vec<_>>().join(", ")
        );
    }
    // Downloaded beside the destination and moved in whole, so the folder
    // either holds the whole program or does not exist.
    let parent = dir.parent().filter(|p| !p.as_os_str().is_empty()).unwrap_or(std::path::Path::new("."));
    std::fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    let staged = tempfile::tempdir_in(parent).with_context(|| format!("stage in {}", parent.display()))?;
    download_into(&client, &running.manifest, &Manifest::new(), staged.path()).await?;
    // Dropping `staged` after the move deletes nothing: its path is gone.
    std::fs::rename(staged.path(), &dir).with_context(|| format!("move the files into {}", dir.display()))?;
    let answer = serde_json::json!({ "version": running.version, "dir": dir });
    if !ctx.json_out(&answer)? {
        println!("version {} written to {}", super::versions::short(&running.version), dir.display());
    }
    Ok(())
}
