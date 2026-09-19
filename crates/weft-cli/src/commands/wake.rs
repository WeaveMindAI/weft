//! `weft wake <color> <node>`: resolve a pure time wait now. Refused
//! for any wait expecting a value, naming the kind.

use super::Ctx;

pub async fn run(ctx: Ctx, color: String, node: String) -> anyhow::Result<()> {
    let color = super::resolve_color(&ctx, &color).await?;
    // The node is named the way the program spells it (`sweep.key`),
    // and the dispatcher keys its waits by the compiler's id, so the
    // reading happens here: the same one `weft events --node` and
    // `weft infra node-stop` do. `node` stays the author's spelling,
    // because that is what gets printed back.
    let node_id = super::node_id_for(&ctx, &node)?;
    ctx.client().post_empty(&format!("/executions/{color}/wake/{node_id}")).await?;
    if ctx.json_out(&serde_json::json!({ "color": color, "node": node }))? {
        return Ok(());
    }
    println!("woke {node} in {}", super::versions::short(&color));
    Ok(())
}
