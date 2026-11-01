//! Layer-4: what the cost trail says about a paid job whose figure is
//! stated only on a later read (a queue-shaped provider).
//!
//! The `metering_queue` fixture ships both the node and its meter, and
//! the rig serves the provider ([`QueueFake`]), so the test owns both
//! ends: the node submits a job, reads its answer back, and the trail
//! carries what the provider stated.
#![cfg(feature = "e2e")]

use weft_e2e::access::{connect_direct, service_spec_of, set_account};
use weft_e2e::fakes::QueueFake;
use weft_e2e::{ensure, project::Project, run};

/// Connect the fixture's queue service, pointing its stored base at the
/// rig's fake, and stamp the handle onto the node.
async fn connect_queue(
    project: &Project,
    disp: &weft_e2e::Dispatcher,
    fake: &QueueFake,
) -> anyhow::Result<weft_e2e::access::Connection> {
    let spec = service_spec_of(&project.dir().join("nodes/queue_job/metadata.json"))?;
    let conn = connect_direct(
        disp,
        spec,
        "own",
        serde_json::json!({ "key": "e2e", "base": fake.base() }),
    )
    .await?;
    set_account(project, "job", conn.handle()).await?;
    Ok(conn)
}

/// A submitted job, read back. The provider states the count and the
/// trail carries what it actually came to.
#[tokio::test]
async fn a_job_read_back_records_what_the_provider_stated() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("metering_queue", disp.clone()).await?;
    let fake = QueueFake::start(3).await?;
    let conn = connect_queue(&project, &disp, &fake).await?;

    let mut settled = run::run_and_settle(&mut project).await?;
    settled.completed()?;

    anyhow::ensure!(fake.reads() >= 1, "the answer was read back: {}", fake.reads());
    settled.assert_measured("queue_fake", "author", 1).await?;
    // Three units at a cent each, read off the provider's own header
    // rather than worked out from the request.
    let priced: Vec<f64> = settled
        .costs()
        .into_iter()
        .filter(|(service, _, _)| service == "queue_fake")
        .map(|(_, _, amount)| amount)
        .collect();
    anyhow::ensure!(
        priced.len() == 1 && (priced[0] - 0.03).abs() < 1e-9,
        "the figure is what the provider stated (3 units at $0.01): {priced:?}"
    );

    conn.finish().await?;
    project.finish().await
}
