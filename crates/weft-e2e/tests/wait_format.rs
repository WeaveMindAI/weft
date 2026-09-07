//! A run that parks on a timer and comes back: `Format` builds a line from its
//! own declared input ports, `Wait` suspends the branch for five seconds, and
//! the value it was holding flows on unchanged when the timer fires.
//!
//! The park is what needs a cluster: the worker records a timer suspension and
//! the execution resumes from the journal when the dispatcher fires it, so a
//! completed run with the right value on the far side of the wait is the proof.
#![cfg(feature = "e2e")]

use serde_json::json;
use weft_e2e::{ensure, project::Project, run};

#[tokio::test]
async fn a_formatted_line_survives_a_timer_park() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("wait_format", disp).await?;

    let settled = run::run_and_settle(&mut project).await?;
    settled.completed()?;
    settled.assert_input("out", "data", &json!("Hi quentin, you have 3 left"))?;

    project.finish().await
}
