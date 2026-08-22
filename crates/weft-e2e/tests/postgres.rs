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
    round_trip(&mut project)
        .await?
        .assert_input("out", "data", &json!([{ "body": BODY }]))?;

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
