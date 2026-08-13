//! Cron self-tests (fake tier only: a trigger has nothing meaningful
//! to test live without the provider pushing real events at real
//! infrastructure). Covers both trigger bodies: `setup_trigger`
//! registers the cron timer, `run` forwards a fire's wake payload.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::CronNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("setup_registers_the_cron_timer", setup_registers),
        NodeTest::fake("a_fire_forwards_its_wake_payload", fire_forwards),
    ]
}

async fn setup_registers(rig: FakeRig) -> WeftResult<()> {
    rig.run_setup_trigger(&CronNode, json!({ "cron": "0 0 9 * * *" }))
        .await
        .ok()?;
    let registered = rig.registered_signals();
    assert_eq!(registered.len(), 1, "exactly one signal registered");
    let (spec, snapshot) = &registered[0];
    assert_eq!(spec.kind, "timer");
    assert_eq!(spec.config["spec"]["kind"], "cron");
    assert_eq!(spec.config["spec"]["expression"], "0 0 9 * * *");
    assert_eq!(snapshot["cron"], "0 0 9 * * *", "inputs snapshotted for replay at fire");
    Ok(())
}

async fn fire_forwards(rig: FakeRig) -> WeftResult<()> {
    rig.wake(json!({ "scheduledTime": "2026-08-08T09:00:00Z", "actualTime": "2026-08-08T09:00:01Z" }));
    let outcome = rig.run(&CronNode, json!({})).await.ok()?;
    assert_eq!(outcome.outputs["scheduledTime"], json!("2026-08-08T09:00:00Z"));
    assert_eq!(outcome.outputs["actualTime"], json!("2026-08-08T09:00:01Z"));

    // A fire whose payload lost a field is a loud error naming it,
    // never a silently-substituted clock read.
    rig.wake(json!({ "scheduledTime": "2026-08-08T09:00:00Z" }));
    let broken = rig.run(&CronNode, json!({})).await;
    let err = broken
        .result
        .expect_err("a wake missing actualTime fails loud")
        .to_string();
    assert!(err.contains("actualTime"), "the error names the missing field: {err}");
    Ok(())
}
