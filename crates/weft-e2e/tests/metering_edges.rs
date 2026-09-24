//! Layer-4: what the cost trail says when a paid job's figure never
//! arrives, and what a worker has left after node code panics.
//!
//! Both need a fixture, because neither is reachable from outside: a
//! provider that commits money and then stays silent, and a node that
//! panics inside its own `run`. The rig owns both ends, so a test can
//! decide whether an answer is ever read back, and can make a real
//! panic unwind through the worker's own machinery.
//!
//! What is being proved:
//!   - a job submitted and never read back lands on the trail as a
//!     spend with NO figure, when the execution ends, rather than as a
//!     zero (which would claim the call was free), as nothing at all
//!     (which would claim it never happened), or only when the worker
//!     eventually dies (which used to pin the pod alive);
//!   - a job whose answer IS read back lands with what the provider
//!     actually stated;
//!   - a panicking node fails its own execution and leaves the worker
//!     able to run the next one.
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
    set_account(project, "job", "connection", conn.handle())?;
    Ok(conn)
}

/// Money out, and nothing will ever say how much.
///
/// The node submits a job and walks away, which is an ordinary thing to
/// do: a branch stops caring, a person cancels. The spend is real, and
/// the only honest record is one with no figure on it. It has to land
/// when the EXECUTION ends: tying it to the worker's own shutdown meant
/// the record arrived whenever that pod eventually died, and the pod
/// refused to retire while any such charge was open.
#[tokio::test]
async fn a_job_submitted_and_never_read_back_records_spend_with_no_figure() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("metering_queue", disp.clone()).await?;
    // No units ever: every read would say "still running", and the node
    // does not read anyway.
    let fake = QueueFake::start(None).await?;
    let conn = connect_queue(&project, &disp, &fake).await?;

    let mut settled = run::run_and_settle(&mut project).await?;
    settled.completed()?;

    anyhow::ensure!(fake.submits() == 1, "the job was submitted once: {}", fake.submits());
    anyhow::ensure!(fake.reads() == 0, "and its answer was never read: {}", fake.reads());
    settled.assert_spend_without_figure("queue_fake", 1).await?;

    conn.finish().await?;
    project.finish().await
}

/// The same call, read back. The provider states the count and the
/// trail carries what it actually came to.
#[tokio::test]
async fn a_job_read_back_records_what_the_provider_stated() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("metering_queue", disp.clone()).await?;
    let fake = QueueFake::start(Some(3)).await?;
    let conn = connect_queue(&project, &disp, &fake).await?;
    project.set_node_config("job", "readBack", "true")?;

    let mut settled = run::run_and_settle(&mut project).await?;
    settled.completed()?;

    anyhow::ensure!(fake.reads() >= 1, "the answer was read back: {}", fake.reads());
    settled.assert_measured("queue_fake", "their-own", 1).await?;
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

/// Node code panics. The execution has to end failed rather than hang,
/// and the worker has to be fit to run the next one: a panic unwinds
/// past the ordinary cleanup, so everything that execution registered on
/// the pod is released by a guard or not at all.
#[tokio::test]
async fn a_panicking_node_fails_its_run_and_leaves_the_worker_healthy() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("worker_panic", disp.clone()).await?;

    let settled = run::run_and_settle(&mut project).await?;
    anyhow::ensure!(
        settled.status == "failed",
        "a panicking node fails its execution rather than hanging it; got {}",
        settled.status
    );
    // Whatever the worker does with the node behind a panic (skip it on
    // the closed input, or never reach it at all), the one thing it must
    // NOT do is run it: a panicking node produced no value for it.
    let after: Vec<&str> = settled.replay.for_node("after").map(|e| e.kind()).collect();
    anyhow::ensure!(
        !after.contains(&"node_completed"),
        "the node behind a panicking one must not run; its events: {after:?}"
    );

    // THE SAME POD takes another run, which is the part that matters: a
    // fresh pod would pass this whatever the panic left behind. So the
    // pod serving the first run is read, the second run goes through, and
    // the pod is asserted to be that same one, alive and not draining. If
    // the panic had left the execution's state behind, this is where it
    // shows: a cancel flag and a live config for a dead colour, and a
    // worker that refuses to retire.
    let platform = weft_e2e::platform::Platform::connect(&disp).await?;
    let pid = project.id();
    let before = platform.execution_owner(&settled.color).await?
        .ok_or_else(|| anyhow::anyhow!("the panicking run has no recorded worker owner"))?;

    project.set_node_config("boom", "explode", "false")?;
    let ok = run::run_and_settle(&mut project).await?;
    ok.completed()?;
    ok.assert_completed("after")?;
    let owner = platform.execution_owner(&ok.color).await?;
    anyhow::ensure!(owner.as_deref() == Some(before.as_str()), "the second run used {owner:?}, but the panicking run used {before}");

    let after_pods = platform.worker_pods_for_project(&pid).await?;
    anyhow::ensure!(
        after_pods.iter().any(|p| before == p.pod_name && !p.draining),
        "the second run must be served by a pod that survived the panic, not by a replacement: \
         before {before:?}, after {:?}",
        after_pods.iter().map(|p| (&p.pod_name, &p.status, p.draining)).collect::<Vec<_>>()
    );

    project.finish().await
}
