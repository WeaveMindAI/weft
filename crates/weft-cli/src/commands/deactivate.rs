use super::Ctx;

pub async fn run(ctx: Ctx, project: String) -> anyhow::Result<()> {
    ctx.client().post_empty(&format!("/projects/{project}/deactivate")).await?;
    println!("deactivated {project}");
    Ok(())
}
