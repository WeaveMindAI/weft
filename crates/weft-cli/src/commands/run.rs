//! `weft run`: compile + register the cwd project, kick off a fresh
//! run, stream logs until completion (or `--detach`).

use anyhow::Context;

use super::Ctx;

pub async fn run(ctx: Ctx, detach: bool) -> anyhow::Result<()> {
    let handle = super::ensure::ensure_registered(&ctx).await?;
    println!("registered {} ({})", handle.name, handle.id);

    let run_resp: serde_json::Value = handle
        .client
        .post_json(
            &format!("/projects/{}/run", handle.id),
            &serde_json::json!({ "payload": serde_json::Value::Null }),
        )
        .await
        .context("run project")?;
    let color = run_resp
        .get("color")
        .and_then(|v| v.as_str())
        .context("run response missing color")?
        .to_string();

    println!("started color {color}");
    if detach {
        return Ok(());
    }

    super::follow::follow_color(&handle.client, &color).await?;
    Ok(())
}
