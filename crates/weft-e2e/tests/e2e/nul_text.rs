//! A run carrying the NUL character, which Postgres refuses in text: it
//! ends with its error written down, and searching runs afterwards finds
//! it, by the words around the character and by the node it ran through.
#![cfg(feature = "e2e")]

use serde_json::{json, Value};
use weft_e2e::{client::cli, ensure, project::Project, run};

#[tokio::test]
async fn a_run_carrying_nul_ends_and_is_found() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("nul_text", disp).await?;
    let run_id = run::start(&mut project).await?.to_string();

    let out = cli(project.dispatcher(), project.dir(), &["wait", &run_id]).await?;
    let said = format!("{}{}", out.stdout, out.stderr);
    anyhow::ensure!(out.status == Some(10) && said.contains("broke on scraped"), "exit {:?}: {said}", out.status);

    let found = |listing: &Value| listing["runs"].as_array().is_some_and(|runs| runs.iter().any(|r| r["runId"] == json!(run_id)));
    let searched: Value = serde_json::from_str(project.weft(&["runs", "--search", "scrapedpage", "--json"]).await?.trim())?;
    anyhow::ensure!(found(&searched), "{searched}");
    let through: Value = serde_json::from_str(project.weft(&["runs", "--through", "explode", "--json"]).await?.trim())?;
    anyhow::ensure!(found(&through), "{through}");
    project.finish().await
}
