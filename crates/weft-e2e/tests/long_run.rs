//! A long run: `weft run --long` gives the run a worker container of its
//! own, which lives exactly as long as the run. A cloud install needs this
//! for runs longer than its request cap; on a local install it is the same
//! path, so this proves the path.
#![cfg(feature = "e2e")]

use serde_json::json;
use weft_e2e::platform::{Platform, Role};
use weft_e2e::{client::poll_until, ensure, project::Project, run, SettledRun};

#[tokio::test]
async fn a_long_run_gets_a_worker_of_its_own_that_ends_with_it() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let platform = Platform::connect(&disp).await?;
    let mut project = Project::prepare("wait_format", disp.clone()).await?;
    project.set_node_config("hold", "seconds", "20")?;
    let pid = project.id();

    let execution_id = run::start_long(&mut project).await?;
    // The run parks on its 20 second wait, so its container is there to see.
    let own = poll_until(
        "the long run's own container",
        std::time::Duration::from_secs(60),
        std::time::Duration::from_millis(500),
        || async {
            let own = platform.containers_for_project(&pid, Role::Long).await?;
            Ok((!own.is_empty()).then_some(own))
        },
    )
    .await?;
    anyhow::ensure!(own.len() == 1, "a long run starts one container of its own, got {own:?}");
    anyhow::ensure!(
        platform.workers_for_project(&pid).await?.is_empty(),
        "a long run is served by its own container, never by the project's shared worker"
    );

    let settled = SettledRun::observe(&disp, execution_id).await?;
    settled.completed()?;
    settled.assert_input("out", "data", &json!("Hi quentin, you have 3 left"))?;

    // The container goes with the run that owned it.
    poll_until(
        "the long run's container to be gone",
        std::time::Duration::from_secs(60),
        std::time::Duration::from_millis(500),
        || async { Ok(platform.containers_for_project(&pid, Role::Long).await?.is_empty().then_some(())) },
    )
    .await?;

    project.finish().await
}
