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
        NodeTest::fake("the_shipped_default_is_a_valid_expression", default_is_valid),
        NodeTest::fake("a_fire_forwards_its_wake_payload", fire_forwards),
        NodeTest::basic("every_zone_in_the_dropdown_is_a_zone_the_runtime_knows", zones_agree),
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
    assert_eq!(spec.config["spec"]["timezone"], "UTC", "the shipped zone is UTC");
    assert_eq!(snapshot["cron"], "0 0 9 * * *", "inputs snapshotted for replay at fire");

    let rig = FakeRig::new();
    rig.run_setup_trigger(&CronNode, json!({ "cron": "0 0 9 * * *", "timezone": "Europe/Paris" }))
        .await
        .ok()?;
    assert_eq!(rig.registered_signals()[0].0.config["spec"]["timezone"], "Europe/Paris");
    Ok(())
}

/// The dropdown is the metadata's own list, the runtime's is
/// chrono-tz's: the two are pinned to each other (UTC first, the rest
/// as the database orders them) so a zone the picker offers is never
/// one the listener refuses, and a zone the listener knows is never
/// missing from the picker.
fn zones_agree() -> WeftResult<()> {
    let meta: serde_json::Value = serde_json::from_str(include_str!("metadata.json"))
        .map_err(|e| weft::error::node_error(e.to_string()))?;
    let input = meta["inputs"]
        .as_array()
        .and_then(|i| i.iter().find(|i| i["name"] == "timezone"))
        .ok_or_else(|| weft::error::node_error("no timezone input"))?;
    let offered: Vec<&str> =
        input["widget"]["options"].as_array().into_iter().flatten().filter_map(|v| v.as_str()).collect();
    let mut known: Vec<&str> = vec!["UTC"];
    known.extend(chrono_tz::TZ_VARIANTS.iter().map(|z| z.name()).filter(|n| *n != "UTC"));
    assert_eq!(offered, known, "the dropdown and chrono-tz disagree; regenerate the options");
    assert_eq!(input["default"], "UTC");
    Ok(())
}

/// The metadata default is what a user gets by dropping the node in
/// and pressing Activate; it once shipped in the five-field form the
/// runtime refuses, so the default is validated the way the listener
/// will validate it.
async fn default_is_valid(rig: FakeRig) -> WeftResult<()> {
    rig.run_setup_trigger(&CronNode, json!({})).await.ok()?;
    let registered = rig.registered_signals();
    assert_eq!(registered.len(), 1, "the default registered a timer");
    weft::signal::validate_spec(&registered[0].0)
        .map_err(|e| weft::error::node_error(format!("the shipped default does not parse: {e}")))?;
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
