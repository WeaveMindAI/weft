//! Postgres end to end: a database the project runs itself, reached
//! through the connection that database published.
//!
//! What only a real run can prove:
//!   - the database node brings a real Postgres up and hands out a
//!     connection that actually opens it, with no credential anywhere
//!     in the project;
//!   - the query nodes run real SQL through it, in order, each waiting
//!     on the rows the previous one answered;
//!   - the value survives the whole round trip (the read matches on
//!     the row the write handed back, not on a constant);
//!   - a SECOND run still works, which it only can because the
//!     connection was stored the first time: by then the database
//!     refuses to hand its password over again. The row count growing
//!     also proves the data is really in a database;
//!   - terminating takes the connection away with the database.
//!
//! Needs a cluster, no external service and no credentials.
#![cfg(feature = "e2e")]

use anyhow::Result;
use serde_json::json;

use weft_e2e::{ensure, infra, project::Project, run, SettledRun};

/// The postgres connections belonging to THIS project. The listing is
/// tenant-wide, and every e2e shares one tenant, so a sibling test's
/// database (or a leftover from a crashed run) would otherwise be
/// counted here and fail this test for something it did not do.
async fn postgres_grants_of_this_project(project: &Project) -> Result<Vec<serde_json::Value>> {
    let all: Vec<serde_json::Value> =
        project.dispatcher().get_json("/access/grants?service=postgres").await?;
    let mine = json!(project.id().to_string());
    Ok(all.into_iter().filter(|g| g.get("project_id") == Some(&mine)).collect())
}

/// The one sentence this test writes, and matches on when reading
/// back. Substituted into the fixture so the two cannot drift.
const BODY: &str = "written by the postgres e2e";

/// Run the graph once and hand back the settled run to assert on.
async fn round_trip(project: &mut Project) -> Result<SettledRun> {
    let settled = run::run_and_settle(project).await?;
    settled.completed()?;
    Ok(settled)
}

#[tokio::test]
async fn a_project_runs_its_own_postgres_and_talks_to_it() -> Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("postgres_db", disp).await?;
    project.substitute_in_main("__E2E_BODY__", BODY)?;

    // Builds the credential image, applies the manifests, waits until
    // Postgres itself reports it is serving.
    infra::start_and_wait_running(&mut project, "db").await?;

    // The first run is the one that reads the password off the
    // database and publishes the connection.
    let first = round_trip(&mut project).await?;
    first.assert_input("out", "data", &json!([{ "body": BODY }]))?;

    // An optional port that stayed silent reached the database as SQL
    // NULL instead of refusing the query. The author wrote `photo?`,
    // sent nothing, and the row is written with an empty column: one
    // node, no branch, no split insert.
    first.assert_input("absent", "data", &json!([{ "body": "no picture", "tag": null }]))?;

    // The second run: the database no longer hands its password out,
    // so this only works through the stored connection. Two rows now,
    // because the first run's row is still there.
    round_trip(&mut project)
        .await?
        .assert_input("out", "data", &json!([{ "body": BODY }, { "body": BODY }]))?;

    // The connection exists while the database does, and it is the
    // node's own: nobody connected it by hand.
    let live = postgres_grants_of_this_project(&project).await?;
    anyhow::ensure!(live.len() == 1, "expected the database's one connection, got {live:?}");

    infra::terminate_and_wait_gone(&project, "db").await?;

    // And it goes with the database. A credential that outlived the
    // thing it opens would name nothing, and nothing downstream could
    // use it or tell it apart from a working one.
    let after = postgres_grants_of_this_project(&project).await?;
    anyhow::ensure!(
        after.is_empty(),
        "terminating the database left its connection behind: {after:?}"
    );

    project.finish().await
}

/// `weft infra env` writes the database's password into an env file
/// without printing it. Once a run has taken the password it refuses and
/// names `weft infra press`; `weft infra show` lists the card's reset
/// button without printing anything secret; pressing it never asks, and
/// after it `env` writes the fresh password and
/// the user in one go, printing the user, keeping the file's other lines.
#[tokio::test]
async fn the_password_goes_into_an_env_file_and_a_reset_gets_a_new_one() -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let disp = ensure::up().await?;
    let mut project = Project::prepare("postgres_db", disp).await?;
    project.substitute_in_main("__E2E_BODY__", BODY)?;
    infra::start_and_wait_running(&mut project, "db").await?;

    // Starting the database runs its setup, which takes the password: by
    // the time anyone looks, the card says it was handed over. So the
    // plain call refuses and names the press that gets a new one.
    let env = project.dir().join("web.env");
    let into = env.to_string_lossy().to_string();
    let only_secret: [&str; 7] = ["infra", "env", "db", "--into", &into, "--as", "PG_PASSWORD"];
    let refused = project.weft_refused(&only_secret).await?;
    anyhow::ensure!(refused.contains("weft infra press"), "the refusal should name `weft infra press`: {refused}");

    // The card lists the reset button, and the action to press it by.
    let shown = project.weft(&["infra", "show", "db"]).await?;
    anyhow::ensure!(shown.contains("weft infra press db reset_password"), "show should list the reset button: {shown}");

    let press: [&str; 4] = ["infra", "press", "db", "reset_password"];
    // What a failure message may show of a text holding the password:
    // the name of each line, never its value.
    let names = |text: &str| -> Vec<String> {
        text.lines().map(|l| l.split_once('=').map_or("<no name>", |(name, _)| name).to_string()).collect()
    };
    let password_in = |text: &str| -> Result<String> {
        let line = text
            .lines()
            .find_map(|l| l.strip_prefix("PG_PASSWORD="))
            .ok_or_else(|| anyhow::anyhow!("the env file does not set PG_PASSWORD, its lines: {:?}", names(text)))?;
        anyhow::ensure!(!line.trim().is_empty(), "an empty password was written");
        Ok(line.trim().to_string())
    };

    let pressed = project.weft(&press).await?;
    // Now the card holds the fresh password, and show still masks it.
    let shown = project.weft(&["infra", "show", "db"]).await?;
    let shown_json = project.weft(&["infra", "show", "db", "--json"]).await?;
    let secret: [&str; 9] =
        ["infra", "env", "db", "--into", &into, "--set", "PG_PASSWORD=Password", "--set", "PG_USER=User"];
    let written = project.weft(&secret).await?;
    anyhow::ensure!(written.contains("PG_USER="), "env should print the plain values it wrote, its lines: {:?}", names(&written));
    let user_line = std::fs::read_to_string(&env)?;
    anyhow::ensure!(
        user_line.lines().any(|l| l.starts_with("PG_USER=") && l.len() > "PG_USER=".len()),
        "the env file does not set PG_USER, its lines: {:?}",
        names(&user_line)
    );
    let first = password_in(&std::fs::read_to_string(&env)?)?;
    for (what, out) in [("press", &pressed), ("show", &shown), ("show --json", &shown_json), ("env", &written)] {
        anyhow::ensure!(!out.contains(&first), "`{what}` printed the password");
    }
    let mode = std::fs::metadata(&env)?.permissions().mode() & 0o777;
    anyhow::ensure!(mode == 0o600, "a new env file should be the owner's only, got {mode:o}");
    // The program's own next run picks the new password up by itself.
    round_trip(&mut project).await?;

    // Another reset replaces only its own line.
    let before = std::fs::read_to_string(&env)?;
    std::fs::write(&env, format!("OTHER=kept\n{before}"))?;
    project.weft(&press).await?;
    project.weft(&only_secret).await?;
    let after = std::fs::read_to_string(&env)?;
    anyhow::ensure!(after.starts_with("OTHER=kept\nPG_PASSWORD=") && after.contains("\nPG_USER="), "the other line was not kept, its lines: {:?}", names(&after));
    anyhow::ensure!(password_in(&after)? != first, "the reset wrote the old password back");

    // The program's own next run picks the new password up by itself.
    round_trip(&mut project).await?;
    infra::terminate_and_wait_gone(&project, "db").await?;
    project.finish().await
}
