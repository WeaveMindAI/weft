//! `weft run`: compile (parse+enrich) the project, upload to the
//! dispatcher, fire a run, stream logs to stdout until completion.

use std::fs;
use std::path::PathBuf;

use anyhow::Context;

use super::Ctx;

pub async fn run(ctx: Ctx, _detach: bool) -> anyhow::Result<()> {
    // Phase A2 minimal: assume `main.weft` in cwd. Full project
    // manifest parsing (`weft.toml`, `nodes/`, vendor resolution)
    // lands in Phase B.
    let main_weft = PathBuf::from("main.weft");
    if !main_weft.exists() {
        anyhow::bail!("main.weft not found in cwd");
    }

    let source = fs::read_to_string(&main_weft).context("read main.weft")?;
    let project_id = uuid::Uuid::new_v4();

    let mut project = weft_compiler::weft_compiler::compile(&source, project_id)
        .map_err(|errs| {
            let joined = errs.iter().map(|e| e.to_string()).collect::<Vec<_>>().join("\n");
            anyhow::anyhow!("compile errors:\n{joined}")
        })?;

    weft_compiler::enrich::enrich(&mut project, &weft_stdlib::StdlibCatalog)
        .map_err(|e| anyhow::anyhow!("enrich: {e}"))?;

    let client = ctx.client();

    // Register with the dispatcher.
    let register_resp: serde_json::Value = client
        .post_json("/projects", &serde_json::to_value(&project)?)
        .await
        .context("register project")?;
    let id_str = register_resp
        .get("id")
        .and_then(|v| v.as_str())
        .context("dispatcher response missing id")?
        .to_string();
    eprintln!("registered project {id_str}");

    // Kick off a run.
    let run_resp: serde_json::Value = client
        .post_json(
            &format!("/projects/{id_str}/run"),
            &serde_json::json!({ "payload": serde_json::Value::Null }),
        )
        .await
        .context("run project")?;
    let color = run_resp
        .get("color")
        .and_then(|v| v.as_str())
        .context("run response missing color")?;
    println!("color: {color}");
    println!("project: {id_str}");
    Ok(())
}
