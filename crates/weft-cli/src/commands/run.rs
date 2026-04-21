//! `weft run`: compile the project in the cwd, register with the
//! dispatcher, kick off a fresh run, stream logs to stdout until the
//! execution completes (or `--detach`).

use anyhow::Context;

use super::Ctx;

pub async fn run(ctx: Ctx, detach: bool) -> anyhow::Result<()> {
    let cwd = std::env::current_dir().context("cwd")?;
    let project = weft_compiler::project::Project::discover(&cwd)
        .map_err(|e| anyhow::anyhow!("discover project: {e}"))?;

    let source = project
        .read_main_weft()
        .map_err(|e| anyhow::anyhow!("read main.weft: {e}"))?;

    let mut compiled = weft_compiler::weft_compiler::compile(&source, project.id())
        .map_err(|errs| {
            let joined = errs
                .iter()
                .map(|e| e.to_string())
                .collect::<Vec<_>>()
                .join("\n  ");
            anyhow::anyhow!("compile errors:\n  {joined}")
        })?;

    weft_compiler::enrich::enrich(&mut compiled, &weft_stdlib::StdlibCatalog)
        .map_err(|e| anyhow::anyhow!("enrich: {e}"))?;

    // Prefer a dispatcher URL explicitly given on the CLI, else the
    // project manifest's value, else the baked default.
    let dispatcher = ctx
        .dispatcher
        .clone()
        .unwrap_or_else(|| project.dispatcher_url());
    let client = crate::client::DispatcherClient::new(&dispatcher);

    // Register (idempotent: same project_id overwrites the stored
    // binary).
    let body = serde_json::to_value(&compiled).context("serialize project")?;
    let register_resp: serde_json::Value = client
        .post_json("/projects", &body)
        .await
        .with_context(|| format!("register against {dispatcher}"))?;
    let id = register_resp
        .get("id")
        .and_then(|v| v.as_str())
        .context("dispatcher response missing id")?
        .to_string();

    println!("registered {} ({})", project.manifest.package.name, id);

    // Kick a fresh run.
    let run_resp: serde_json::Value = client
        .post_json(
            &format!("/projects/{id}/run"),
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

    // Follow the color until the dispatcher reports completion.
    super::follow::follow_color(&client, &color).await?;

    Ok(())
}
