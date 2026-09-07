use super::Ctx;

pub async fn run(ctx: Ctx, color: String) -> anyhow::Result<()> {
    let color = super::resolve_color(&ctx, &color).await?;
    ctx.client().post_empty(&format!("/executions/{color}/cancel")).await?;
    println!("cancelled {color}");
    Ok(())
}
