//! Wait self-tests: the timer the node parks on, and what comes out
//! when it fires.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::{timer_for, WaitNode};

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::basic("a_wait_parks_on_its_milliseconds_rounded_up", milliseconds),
        NodeTest::fake("parks_on_a_timer_and_passes_the_value_through", parks_and_passes),
        NodeTest::fake("a_resumed_body_takes_the_kept_wake_without_a_second_timer", resumed),
        NodeTest::fake("with_no_value_it_is_a_pure_delay", pure_delay),
    ]
}

/// Zero, negative and unrepresentable waits are refused by weft (the
/// metadata rule and widget, and the timer signal's own check where the
/// run parks), so what is left to pin is the conversion.
fn milliseconds() -> WeftResult<()> {
    let spec = weft::signal::to_spec(timer_for(1.5));
    assert_eq!(spec.config["spec"]["kind"], "after");
    assert_eq!(spec.config["spec"]["duration_ms"], 1500);
    let spec = weft::signal::to_spec(timer_for(0.0001));
    assert_eq!(spec.config["spec"]["duration_ms"], 1, "a fraction of a millisecond rounds up, never to zero");
    Ok(())
}

async fn parks_and_passes(rig: FakeRig) -> WeftResult<()> {
    rig.signal(json!({ "scheduledTime": "2026-09-02T21:00:00Z", "actualTime": "2026-09-02T21:00:01Z" }));
    let outcome = rig
        .run(&WaitNode, json!({ "seconds": 10, "value": { "id": 7 } }))
        .await
        .ok()?;
    assert_eq!(outcome.outputs["value"], json!({ "id": 7 }));
    assert_eq!(outcome.outputs["wokeAt"], json!("2026-09-02T21:00:01Z"));
    let awaited = rig.awaited_signals();
    assert_eq!(awaited.len(), 1, "one timer");
    assert_eq!(awaited[0].kind, "timer");
    assert_eq!(awaited[0].config["spec"]["duration_ms"], 10_000);
    Ok(())
}

async fn pure_delay(rig: FakeRig) -> WeftResult<()> {
    rig.signal(json!({ "scheduledTime": "2026-09-02T21:00:00Z", "actualTime": "2026-09-02T21:00:01Z" }));
    let outcome = rig.run(&WaitNode, json!({ "seconds": 0.2 })).await.ok()?;
    assert!(outcome.outputs.get("value").is_none(), "no value in, no value out");
    assert_eq!(outcome.outputs["wokeAt"], json!("2026-09-02T21:00:01Z"));
    Ok(())
}

/// The body runs again from its top after the timer fired (a pause or a
/// crash): the wake is in its state, so it emits again and sets no
/// second timer.
async fn resumed(rig: FakeRig) -> WeftResult<()> {
    rig.signal(json!({ "scheduledTime": "2999-09-03T09:00:00Z", "actualTime": "2999-09-03T09:00:01Z" }));
    let inputs = json!({ "seconds": 10, "value": { "id": 7 } });
    rig.run(&WaitNode, inputs.clone()).await.ok()?;
    assert_eq!(rig.saved_state(&WaitNode).expect("the wait saved the state")["wake"]["actualTime"], json!("2999-09-03T09:00:01Z"));
    // No second answer queued: a second timer would fail the run.
    let again = rig.run(&WaitNode, inputs).await.ok()?;
    assert_eq!(again.outputs["wokeAt"], json!("2999-09-03T09:00:01Z"));
    assert_eq!(rig.awaited_signals().len(), 1, "one timer across the two runs");
    Ok(())
}
