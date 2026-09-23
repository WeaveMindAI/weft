//! A run that parks on a timer and comes back: `Format` builds a line from its
//! own declared input ports, `Wait` suspends the branch for five seconds, and
//! the value it was holding flows on unchanged when the timer fires.
//!
//! The park is what needs a cluster: the worker records a timer suspension and
//! the execution resumes from the journal when the dispatcher fires it, so a
//! completed run with the right value on the far side of the wait is the proof.
//!
//! It also holds the wait to its length. The seconds count from when the node
//! asked, so the trip that registers the timer (worker, dispatcher, listener)
//! is not added on top: a five-second wait resumes about five seconds after
//! the node started, where it used to take several more.
//!
//! And parking is quick once a listener is up: the worker hears its timer is
//! registered the moment that happens, not on a polling tick. The first run
//! may have to start a listener pod (none runs while nothing is listening),
//! which takes seconds of its own, so the park is timed on a second run that
//! finds that pod still there.
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
    let (started, suspended, resumed) = hold_stamps(&settled)?;
    // The wait ends five seconds after the node asked, or as soon as it is
    // registered if registering took longer (a cold listener pod), never
    // five seconds after registering. Journal stamps are whole seconds and
    // the resume itself takes a moment, hence the two seconds of room.
    let (waited, parking) = (resumed - started, suspended - started);
    anyhow::ensure!(
        waited >= 4 && waited <= parking.max(5) + 2,
        "a 5 second Wait resumed {waited}s after it started, {parking}s of it parking; the registration trip must not add to it"
    );

    // The listener the first run placed its timer on is kept for a while
    // after its last signal, so this run registers without starting one.
    let settled = run::run_and_settle(&mut project).await?;
    settled.completed()?;
    let (started, suspended, _) = hold_stamps(&settled)?;
    let parking = suspended - started;
    anyhow::ensure!(
        parking <= 1,
        "the Wait took {parking}s to park on a warm listener; registering its timer should take well under a second"
    );

    project.finish().await
}

/// When `hold` started, parked, and came back, in whole seconds.
fn hold_stamps(settled: &weft_e2e::SettledRun) -> anyhow::Result<(u64, u64, u64)> {
    let stamp = |kind: &str| -> anyhow::Result<u64> {
        settled
            .events_of("hold")
            .find(|e| e.kind() == kind)
            .and_then(|e| e.field("at_unix"))
            .and_then(|v| v.as_u64())
            .ok_or_else(|| anyhow::anyhow!("no {kind} for `hold` in the replay"))
    };
    Ok((stamp("node_started")?, stamp("node_suspended")?, stamp("node_resumed")?))
}
