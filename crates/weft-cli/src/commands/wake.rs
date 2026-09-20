//! `weft wake <color> <node>`: resolve a pure time wait now. Refused
//! for any wait expecting a value, naming the kind.

use super::Ctx;

pub async fn run(ctx: Ctx, color: String, node: String) -> anyhow::Result<()> {
    let color = super::resolve_color(&ctx, &color).await?;
    // The node is named the way the program spells it (`sweep.key`,
    // the whole path from the entry file), which is also how the
    // dispatcher keys the wait: a node inside a file included twice
    // holds one wait per call, and the spelling says which. Checked
    // against the program here, so a typo or the compiled id is
    // refused before anything is asked of the daemon.
    let place = super::node_address_for(&ctx, &node)?;
    ctx.client().post_empty(&format!("/executions/{color}/wake/{place}")).await?;
    if ctx.json_out(&serde_json::json!({ "color": color, "node": node }))? {
        return Ok(());
    }
    println!("woke {node} in {}", super::versions::short(&color));
    Ok(())
}
