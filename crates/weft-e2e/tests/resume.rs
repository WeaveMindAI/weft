//! Platform: a worker crashes mid-execution and the job resumes on a FRESH
//! worker. This is the core local-reliability guarantee (a single machine's
//! worker dies; parked work must survive).
//!
//! Shape: reuse the `human_form` fixture (an event starts a flow that suspends
//! at a HumanQuery). While suspended, KILL the worker pod from the host (a fake
//! crash). Then answer the form: the resume task is NOT pinned to the dead pod,
//! so a worker (fresh, re-seeded from the journal) picks it up and completes.
//!
//! Proof is by INFERENCE, and airtight: we kill the ONLY live worker while the
//! execution is parked at the form (before we send the answer, so it provably
//! had not finished), then assert it still completed with the right approval. A
//! dead worker cannot finish a job, so a fresh one must have resumed it. We do
//! NOT additionally fingerprint the new worker instance: a respawn reuses the
//! worker's deterministic name and the dead row is GC'd within seconds, so every
//! such marker is timing-flaky (see the NOTE in platform.rs).
//!
//! Why this fixture: only a resumable park (a signal/form) survives a worker
//! death; a node holding a gateway-routed live-caller connection is terminally
//! cancelled when its pod dies (the external connection died with it), so it is
//! the wrong shape for a resume test. A HumanQuery suspension is unpinned and
//! resumes anywhere.
#![cfg(feature = "e2e")]

use std::time::Duration;

use serde_json::json;
use weft_e2e::{ensure, fakes::SseFake, human, platform::Platform, project::Project, run, SettledRun};

#[tokio::test]
async fn worker_crash_resumes_on_fresh_worker() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let platform = Platform::connect().await?;
    let mut project = Project::prepare("human_form", disp.clone()).await?;
    let pid = project.id();

    // Bring up the entry feed + activate (a HumanQuery resume only dispatches on
    // an active project; activation also brings up the form-routing listener).
    let feed = SseFake::start().await?;
    project.substitute_in_main("__E2E_FAKE_URL__", &feed.url())?;
    project.activate().await?;

    // Start a fresh execution that runs to HumanQuery and suspends.
    let before = run::execution_colors(&disp, &pid).await?;
    tokio::time::sleep(Duration::from_secs(2)).await;
    feed.push_event("go", &json!({ "value": "the change" }).to_string());
    let color =
        run::wait_for_triggered_execution(&disp, &pid, &before, Duration::from_secs(60)).await?;

    // Wait until the execution is genuinely parked at the form (the worker has
    // registered the suspension), THEN kill its worker to fake a crash. We kill
    // by exact pod name read from `worker_pod`; an empty kill set would mean the
    // worker already idle-exited (also a valid resume path), but we assert at
    // least one was live so this test exercises the abrupt-crash path on
    // purpose, not by accident.
    let review = human::wait_for_form_by_node(&disp, &pid, "review").await?;
    let killed = platform.kill_workers(&pid).await?;
    assert!(
        !killed.is_empty(),
        "expected a live worker to kill while the execution was suspended at the form; \
         killed none (the worker idle-exited before we caught it). Widen the window."
    );

    // Answer the form. The resume task is not pinned to the dead pod, so a
    // worker (fresh, re-seeded from the journal) picks it up and finishes.
    human::answer_form(&disp, &review, &json!({ "decision": "approve" })).await?;

    // The proof of resume-across-a-crash is by inference, and it is airtight: we
    // killed the ONLY live worker while the execution was parked at the form
    // (before we sent the answer, so it provably had not finished), yet the
    // execution still completed with the correct approval. A dead worker cannot
    // finish a job; a fresh one re-seeded from the journal must have. We do NOT
    // additionally fingerprint the new worker instance: a respawn reuses the
    // worker's deterministic name and the dead row is GC'd within seconds, so
    // any "a different instance did it" marker is timing-flaky (see platform.rs).
    let settled = SettledRun::observe(&disp, color).await?;
    settled.completed()?;
    settled.assert_input("out", "data", &json!(true))?;

    project.finish().await
}
