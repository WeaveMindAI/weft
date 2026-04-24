//! `weft activate [project]`. Without arg: discover cwd project,
//! ensure registered, activate. With arg: treat it as a project id
//! and activate directly (assume already registered).

use super::Ctx;

pub async fn run(ctx: Ctx, project: Option<String>) -> anyhow::Result<()> {
    let (client, id, name) = match project {
        Some(id) => (ctx.client(), id.clone(), id),
        None => {
            let handle = super::ensure::ensure_registered(&ctx).await?;
            (handle.client, handle.id, handle.name)
        }
    };
    client.post_empty(&format!("/projects/{id}/activate")).await?;
    println!("activated {name} ({id})");
    Ok(())
}
