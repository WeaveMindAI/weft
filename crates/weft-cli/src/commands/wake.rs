//! `weft wake <color> <node>`: resolve a pure time wait now. Refused
//! for any wait expecting a value, naming the kind.

use super::Ctx;

pub async fn run(ctx: Ctx, color: String, node: String) -> anyhow::Result<()> {
    let color = super::resolve_color(&ctx, &color).await?;
    ctx.client().post_empty(&format!("/executions/{color}/wake/{node}")).await?;
    if ctx.json_out(&serde_json::json!({ "color": color, "node": node }))? {
        return Ok(());
    }
    println!("woke {node} in {}", super::versions::short(&color));
    Ok(())
}
