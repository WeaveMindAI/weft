//! The listener reaches a program's own infrastructure: a trigger
//! subscribed to an infra unit's event stream, at the address the unit
//! hands its workers, fires a run for what the unit sends.
#![cfg(feature = "e2e")]

use std::time::Duration;

use weft_e2e::{ensure, infra, project::Project, run, SettledRun};

#[tokio::test]
async fn a_trigger_listens_to_the_programs_own_infra() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("infra_feed", disp.clone()).await?;
    let pid = project.id();

    infra::start_and_wait_running(&mut project, "svc").await?;
    project.activate().await?;
    let before = run::executions(&disp, &pid).await?;

    let execution_id = run::wait_for_triggered_execution(&disp, &pid, &before, Duration::from_secs(60)).await?;
    let settled = SettledRun::observe(&disp, execution_id).await?;
    settled.completed()?;
    let value = settled.input_of("out").and_then(|i| i.get("data").cloned());
    anyhow::ensure!(value.as_ref().is_some_and(|v| v.as_u64().is_some()), "a tick's value reached Debug, got {value:?}");

    project.weft(&["deactivate", "--mode", "wipe", "--running-policy", "cancel"]).await?;
    infra::terminate_and_wait_gone(&project, "svc").await?;
    project.finish().await
}
