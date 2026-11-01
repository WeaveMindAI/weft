//! Platform: a worker crashes mid-run and the job resumes on a FRESH
//! worker. This is the core local-reliability guarantee (a single machine's
//! worker dies; parked work must survive).
//!
//! Shape: reuse the `human_form` fixture (an event starts a flow that suspends
//! at a HumanQuery). While suspended, KILL the worker process from the host (a
//! fake crash). Then answer the form: the resume task is NOT pinned to the dead
//! worker, so a worker (fresh, re-seeded from the journal) picks it up and
//! completes.
//!
//! Proof is by INFERENCE, and airtight: we kill the ONLY live worker while the
//! run is parked at the form (before we send the answer, so it provably
//! had not finished), then assert it still completed with the right approval. A
//! dead worker cannot finish a job, so a fresh one must have resumed it.
//!
//! Why this fixture: only a resumable park (a signal/form) survives a worker
//! death; a node holding a live-caller connection is terminally
//! cancelled when its worker dies (the external connection died with it), so it is
//! the wrong shape for a resume test. A HumanQuery suspension is unpinned and
//! resumes anywhere.
#![cfg(feature = "e2e")]

use std::time::Duration;

use serde_json::json;
use weft_e2e::{ensure, fakes::SseFake, human, platform::Platform, project::Project, run, SettledRun};

#[tokio::test]
async fn worker_crash_resumes_on_fresh_worker() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let platform = Platform::connect(&disp).await?;
    let mut project = Project::prepare("human_form", disp.clone()).await?;
    let pid = project.id();

    // Bring up the entry feed + activate (a HumanQuery resume only dispatches on
    // an active project; activation also brings up the form-routing listener).
    let feed = SseFake::start().await?;
    project.substitute_in_main("__E2E_FAKE_URL__", &feed.url())?;
    project.activate().await?;

    // Start a fresh run that runs to HumanQuery and suspends.
    let before = run::runs(&disp, &pid).await?;
    feed.wait_for_subscriber(Duration::from_secs(60)).await?;
    feed.push_event("go", &json!({ "value": "the change" }).to_string())?;
    let run_id =
        run::wait_for_triggered_run(&disp, &pid, &before, Duration::from_secs(60)).await?;

    // Wait until the run is genuinely parked at the form (the worker has
    // registered the suspension), THEN kill its worker to fake a crash. We kill
    // every worker process of the project; an empty kill set would mean the
    // worker already idle-exited (also a valid resume path), but we assert at
    // least one was live so this test exercises the abrupt-crash path on
    // purpose, not by accident.
    let review = human::wait_for_form_by_node(&disp, &pid, "review").await?;
    let killed = platform.kill_workers(&pid).await?;
    assert!(
        !killed.is_empty(),
        "expected a live worker to kill while the run was suspended at the form; \
         killed none (the worker idle-exited before we caught it). Widen the window."
    );

    // Answer the form. The resume task is not pinned to the dead worker, so a
    // worker (fresh, re-seeded from the journal) picks it up and finishes.
    human::answer_form(&disp, &review, &json!({ "decision": "approve" })).await?;

    // The worker that ran it is dead, so a completed run with the right
    // approval was resumed by a fresh one (see the module docs).
    let settled = SettledRun::observe(&disp, run_id).await?;
    settled.completed()?;
    settled.assert_input("out", "data", &json!(true))?;

    project.finish().await
}
