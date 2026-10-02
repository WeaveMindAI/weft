//! A node that started an outside job waits for it without holding a
//! worker: it parks on a poll of the job's status address, the listener
//! polls it on the node's behalf, and the run resumes with the first answer
//! whose status says done.
//!
//! The job fake says "still running" twice, then "done". So the run
//! completing with the job's result proves the polls that did not match were
//! held back, the matching one resumed the run, and its response is what
//! `await_signal` returned. The read count staying put afterwards proves the
//! polling stopped once the run had its answer.
#![cfg(feature = "e2e")]

use std::time::Duration;

use serde_json::json;
use weft_e2e::{ensure, fakes::JobFake, project::Project, run};

#[tokio::test]
async fn a_node_waiting_on_a_job_resumes_when_the_job_is_done() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("await_poll", disp).await?;
    let job = JobFake::start(2, "https://cdn.example/out.mp4").await?;
    project.substitute_in_main("__E2E_FAKE_URL__", &job.url())?;

    let settled = run::run_and_settle(&mut project).await?;
    settled.completed()?;
    settled.assert_input("out", "data", &json!("https://cdn.example/out.mp4"))?;
    let reads = job.reads();
    anyhow::ensure!(reads >= 3, "the job was read {reads} times; two \"still running\" answers and the done one make three");

    // Longer than the 5 second poll interval: no poll follows the answer.
    tokio::time::sleep(Duration::from_secs(7)).await;
    anyhow::ensure!(job.reads() == reads, "the status was read again after the run had its answer ({reads} then {})", job.reads());

    project.finish().await
}
