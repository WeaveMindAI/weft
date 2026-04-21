use super::Ctx;

pub async fn run(ctx: Ctx, project: String) -> anyhow::Result<()> {
    ctx.client().delete(&format!("/projects/{project}")).await?;
    println!("removed {project}");
    Ok(())
}
