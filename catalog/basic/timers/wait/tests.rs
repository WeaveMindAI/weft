//! Wait self-tests: the timer the node parks on, and what comes out
//! when it fires.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::{timer_for, WaitNode};

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::basic("a_wait_must_be_positive_and_representable", positive),
        NodeTest::fake("parks_on_a_timer_and_passes_the_value_through", parks_and_passes),
        NodeTest::fake("with_no_value_it_is_a_pure_delay", pure_delay),
    ]
}

fn positive() -> WeftResult<()> {
    // The top of the range as well as the bottom: a wait so long that
    // the moment cannot be represented used to wrap into the past and
    // fire immediately.
    for absurd in [1e16, 2e16, f64::MAX] {
        let err = timer_for(absurd).expect_err("an unrepresentable wait refuses").to_string();
        assert!(err.contains("no timer can hold"), "{absurd}: {err}");
    }
    assert!(timer_for(0.0).is_err());
    assert!(timer_for(-1.0).is_err());
    assert!(timer_for(f64::NAN).is_err());
    let spec = weft::signal::to_spec(timer_for(1.5)?);
    assert_eq!(spec.config["spec"]["kind"], "after");
    assert_eq!(spec.config["spec"]["duration_ms"], 1500);
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
