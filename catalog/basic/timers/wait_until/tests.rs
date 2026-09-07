//! WaitUntil self-tests: the timer the node parks on, and what comes
//! out when it fires.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::{timer_for, WaitUntilNode};

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::basic("when_must_be_an_iso_date_time", iso_only),
        NodeTest::fake("parks_on_a_timer_at_the_moment_and_passes_the_value_through", parks_and_passes),
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
