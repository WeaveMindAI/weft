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

    // Compile locally: produces the native project binary the
    // dispatcher will spawn. Heavy work lives on the client side;
    // the dispatcher itself stays thin.
    println!("compiling {}...", project.manifest.package.name);
    let build = weft_compiler::build::build_project(&project.root, true)
        .map_err(|e| anyhow::anyhow!("build failed: {e}"))?;
    println!("built: {}", build.binary_path.display());

    // Dispatcher URL: explicit CLI arg > project manifest > baked default.
    let dispatcher = ctx
        .dispatcher
        .clone()
        .unwrap_or_else(|| project.dispatcher_url());
    let client = crate::client::DispatcherClient::new(&dispatcher);

    // Register: we hand over the source (the dispatcher re-parses so
    // it has the enriched `ProjectDefinition` for run dispatch) plus
    // the absolute path to the binary we just built.
    let register_body = serde_json::json!({
        "id": project.id().to_string(),
        "name": project.manifest.package.name,
        "source": source,
        "binary_path": build.binary_path.display().to_string(),
    });
    let register_resp: serde_json::Value = client
        .post_json("/projects", &register_body)
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

    super::follow::follow_color(&client, &color).await?;

    Ok(())
}
