//! WaitUntil self-tests: the timer the node parks on, and what comes
//! out when it fires.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::{timer_for, WaitUntilNode};

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::basic("when_must_be_an_iso_date_time", iso_only),
        NodeTest::fake("parks_on_a_timer_at_the_moment_and_passes_the_value_through", parks_and_passes),
        NodeTest::fake("a_resumed_body_takes_the_kept_wake_without_a_second_timer", resumed),
    ]
}

fn iso_only() -> WeftResult<()> {
    let err = timer_for("tomorrow at nine").expect_err("prose is not a date").to_string();
    assert!(err.contains("ISO-8601") && err.contains("tomorrow at nine"), "{err}");
    assert!(timer_for("2026-09-03").is_err(), "a date with no time and zone refuses");
    let spec = weft::signal::to_spec(timer_for("2999-09-03T11:00:00+02:00")?);
    assert_eq!(spec.config["spec"]["kind"], "at");
    assert_eq!(spec.config["spec"]["when"], "2999-09-03T09:00:00Z", "carried in UTC");
    Ok(())
}

async fn parks_and_passes(rig: FakeRig) -> WeftResult<()> {
    rig.signal(json!({ "scheduledTime": "2999-09-03T09:00:00Z", "actualTime": "2999-09-03T09:00:01Z" }));
    let outcome = rig
        .run(&WaitUntilNode, json!({ "when": "2999-09-03T09:00:00Z", "value": "later" }))
        .await
        .ok()?;
    assert_eq!(outcome.outputs["value"], json!("later"));
    assert_eq!(outcome.outputs["wokeAt"], json!("2999-09-03T09:00:01Z"));
    let awaited = rig.awaited_signals();
    assert_eq!(awaited.len(), 1);
    assert_eq!(awaited[0].config["spec"]["kind"], "at");
    Ok(())
}

/// The body runs again from its top after the timer fired (a pause or a
/// crash): the wake is in its state, so it emits again and sets no
/// second timer.
async fn resumed(rig: FakeRig) -> WeftResult<()> {
    rig.signal(json!({ "scheduledTime": "2999-09-03T09:00:00Z", "actualTime": "2999-09-03T09:00:01Z" }));
    let inputs = json!({ "when": "2999-09-03T09:00:00Z", "value": "later" });
    rig.run(&WaitUntilNode, inputs.clone()).await.ok()?;
    assert_eq!(rig.saved_state(&WaitUntilNode).expect("the wait saved the state")["wake"]["actualTime"], json!("2999-09-03T09:00:01Z"));
    // No second answer queued: a second timer would fail the run.
    let again = rig.run(&WaitUntilNode, inputs).await.ok()?;
    assert_eq!(again.outputs["wokeAt"], json!("2999-09-03T09:00:01Z"));
    assert_eq!(rig.awaited_signals().len(), 1, "one timer across the two runs");
    Ok(())
}
