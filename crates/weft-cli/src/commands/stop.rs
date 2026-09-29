use super::Ctx;

pub async fn run(ctx: Ctx, execution_id: String) -> anyhow::Result<()> {
    let execution_id = super::resolve_execution_id(&ctx, &execution_id).await?;
    ctx.client()?.post_empty(&format!("/executions/{execution_id}/cancel")).await?;
    if ctx.json_out(&serde_json::json!({ "cancelled": execution_id }))? {
        return Ok(());
    }
    println!("cancelled {execution_id}");
    Ok(())
}
