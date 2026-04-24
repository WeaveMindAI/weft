//! `weft deactivate [project]`. Without arg: discover cwd project.
//! No build required; deactivate just stops trigger URLs.

use super::Ctx;

pub async fn run(ctx: Ctx, project: Option<String>) -> anyhow::Result<()> {
    let (client, id, name) = match project {
        Some(id) => (ctx.client(), id.clone(), id),
        None => {
            // Deactivate doesn't need a fresh binary. Still discover
            // the cwd project so we get the id + a usable name for
            // the output. No compile step.
            let cwd = std::env::current_dir()?;
            let project = weft_compiler::project::Project::discover(&cwd)
                .map_err(|e| anyhow::anyhow!("discover project: {e}"))?;
            (
                ctx.client(),
                project.id().to_string(),
                project.manifest.package.name.clone(),
            )
        }
    };
    client.post_empty(&format!("/projects/{id}/deactivate")).await?;
    println!("deactivated {name} ({id})");
    Ok(())
}
