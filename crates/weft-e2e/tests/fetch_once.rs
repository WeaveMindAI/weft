//! The same URL fetched twice under one identity is ONE file, proved over
//! the real worker-to-broker wire, plus everything that run carries with
//! it: the cron default registers on activate; Format and Wait do their
//! jobs and the document reaches a real chat; and the run reads back the
//! way `weft executions`, `weft events` and `weft logs` read it, with a
//! phase on the summary and on the listing filter, and a node on a log
//! line.
//!
//! Needs `WEFT_E2E_TELEGRAM_TOKEN` and `WEFT_E2E_TELEGRAM_CHAT_ID`; skips
//! without them, like the other Telegram test.
#![cfg(feature = "e2e")]

use anyhow::{Context, Result};
use serde_json::Value;

use weft_e2e::access::{catalog_spec, connect_direct, set_account};
use weft_e2e::ensure::{self, env_group_or_skip};
use weft_e2e::{project::Project, run};

#[tokio::test]
async fn one_file_two_fetches_and_a_run_that_reads_back() -> Result<()> {
    let Some(env) =
        env_group_or_skip("telegram", &["WEFT_E2E_TELEGRAM_TOKEN", "WEFT_E2E_TELEGRAM_CHAT_ID"])
    else {
        return Ok(());
    };
    let [token, chat] = <[String; 2]>::try_from(env).expect("two vars requested");
    let disp = ensure::up().await?;
    let conn = connect_direct(
        &disp,
        catalog_spec("telegram", "access")?,
        "own",
        serde_json::json!({ "token": token }),
    )
    .await?;

    let mut project = Project::prepare("fetch_once", disp.clone()).await?;
    let pid = project.id();
    set_account(&project, "bot", "account", conn.handle())?;
    project.set_node_config("send", "chatId", &format!("{chat:?}"))?;
    // The bot's own getMe answer is the file: a URL the run already has
    // the right to fetch, and no third host to depend on.
    project.substitute_in_main("__E2E_URL__", &format!("https://api.telegram.org/bot{token}/getMe"))?;

    // Activation registers the cron trigger on its shipped default. A
    // five-field default would be refused right here.
    project.activate().await?;

    let settled = run::run_and_settle(&mut project).await?;
    settled.completed()?;
    anyhow::ensure!(
        settled.input_of("out").and_then(|i| i.get("data").and_then(Value::as_i64)).is_some(),
        "no message id came back from Telegram"
    );

    // The identity lookup, over the real wire: two fetches, one file.
    fn key_of(settled: &run::SettledRun, node: &str) -> Result<String> {
        let file = settled.output_of(node).and_then(|o| o.get("file").cloned()).context(node.to_string())?;
        Ok(weft_core::storage::StoredFile::from_value(&file)?.key)
    }
    let first = key_of(&settled, "first")?;
    anyhow::ensure!(first.contains("/project/"), "stored in project scope: {first}");
    anyhow::ensure!(first == key_of(&settled, "second")?, "the second fetch is the first file");

    // Wait parked and came back with the caption intact.
    settled.assert_completed("hold")?;
    anyhow::ensure!(
        settled.replay().by_kind("node_suspended").any(|e| e.is_node("hold")),
        "the wait parked on a timer"
    );

    // The summary carries its phase, the listing filters by it, and the
    // activate left a setup run behind.
    let color = settled.color;
    let one: Value = disp.get_json(&format!("/executions/{color}")).await?;
    anyhow::ensure!(one["phase"] == "fire", "the manual run is a fire: {one}");
    let fires: Value =
        disp.get_json(&format!("/executions?project_id={pid}&phase=fire&limit=50")).await?;
    anyhow::ensure!(
        fires["executions"].as_array().is_some_and(|rows| rows.iter().any(|r| r["color"] == color.to_string())),
        "the fire listing holds the run: {fires}"
    );
    let setups: Value =
        disp.get_json(&format!("/executions?project_id={pid}&phase=trigger_setup&limit=50")).await?;
    anyhow::ensure!(setups["total"].as_u64().unwrap_or(0) >= 1, "activate made a setup run: {setups}");

    // A log line names the node that wrote it (Debug logs what it saw).
    let logs: Value = disp.get_json(&format!("/executions/{color}/logs")).await?;
    anyhow::ensure!(
        logs["lines"].as_array().is_some_and(|lines| lines.iter().any(|l| l["node"] == "out")),
        "the Debug node's line names it: {logs}"
    );

    project.finish().await?;
    conn.finish().await
}
