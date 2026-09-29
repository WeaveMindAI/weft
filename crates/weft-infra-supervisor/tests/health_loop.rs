//! Layer-3 integration tests for the supervisor's health loop.
//!
//! These exercise the full `tick_health` orchestration against
//! in-memory fakes: seed broker + host state, advance the clock,
//! call `tick_health()`, assert on emitted events + status writes.
//!
//! Counter-cases the pure-function tests can't catch:
//!   - lock ordering inside `tick_project`,
//!   - state reset when status leaves `running` / `flaky`,
//!   - the broker call sequence (event_record before set_status),
//!   - fired-set re-arming after recovery.

use std::time::Duration;

use weft_broker_client::protocol::InfraNodeStatus as Status;
use weft_infra_supervisor::broker_ops::{BrokerCall, BrokerSupervisorOps};
use weft_infra_supervisor::testing::SupervisorTestRig;
use weft_platform_traits::{HostCall, UnitRunState};

/// A one-unit roster (unit named `unit`, at `status`, default windows).
fn unit_map(
    unit: &str,
    status: Status,
) -> std::collections::BTreeMap<String, weft_broker_client::protocol::UnitRuntime> {
    let mut m = std::collections::BTreeMap::new();
    m.insert(
        unit.to_string(),
        weft_broker_client::protocol::UnitRuntime {
            status,
            stop_behavior: weft_core::StopBehavior::Stop,
            flaky_after_seconds: 30,
            recovery_after_seconds: 30,
            image_refs: Default::default(),
        },
    );
    m
}

const TENANT: &str = "tenant-test";
const PROJECT: uuid::Uuid = uuid::Uuid::from_u128(1);
const PROJ_USER_OFF: uuid::Uuid = uuid::Uuid::from_u128(0x5eaa6fdd30862fd);
const PROJ_INACTIVE: uuid::Uuid = uuid::Uuid::from_u128(0xb2cdde0277773ee);
const PROJ_ACTIVE: uuid::Uuid = uuid::Uuid::from_u128(0xb8a5f259a9ea55d);
const INACTIVE_BROKEN: uuid::Uuid = uuid::Uuid::from_u128(0x53bf4ec8cd4f847);
const ACTIVE_HEALTHY: uuid::Uuid = uuid::Uuid::from_u128(0xe78b1a9b48520b1);
const NODE: &str = "bridge";

/// A stop of every node's shared copies, in flight.
fn whole_project_command() -> weft_broker_client::protocol::InFlightCommand {
    weft_broker_client::protocol::InFlightCommand { node_id: None, copies: weft_core::member::Copies::Shared }
}

fn rig() -> SupervisorTestRig {
    let rig = SupervisorTestRig::with_tenant(TENANT);
    rig.broker.add_project(PROJECT);
    rig
}

/// What the host reports for `project` from now on: each `(node,
/// instance, unit, ready)`, and nothing else.
fn observe(rig: &SupervisorTestRig, project: uuid::Uuid, units: &[(&str, &str, &str, bool)]) {
    rig.host.clear_project(project);
    for (node, instance, unit, ready) in units {
        let copy = weft_core::infra::NodeRef {
            tenant: TENANT.into(),
            project,
            node: (*node).into(),
            instance: (*instance).into(),
        };
        let state = if *ready { UnitRunState::Ready } else { UnitRunState::NotReady { why: "probe failing".into() } };
        rig.host.set_state(&copy, unit, state);
    }
}

/// How many unit restarts reached the host.
fn restarts(rig: &SupervisorTestRig) -> usize {
    rig.host.calls().iter().filter(|c| matches!(c, HostCall::Restart { .. })).count()
}

// ---------- false-flaky-on-fresh-provision ----------

#[tokio::test]
async fn provisioning_node_not_yet_ready_does_not_flap_flaky() {
    // Scenario: node is `provisioning`. Supervisor's health loop
    // SHOULD skip it (the apply executor owns the status). We assert
    // no flaky event fires no matter how long the loop runs.
    let rig = rig();
    rig.broker
        .add_infra_node(PROJECT, NODE, "inst1", Status::Provisioning);
    observe(&rig, PROJECT, &[(NODE, "inst1", "bridge", false)]);

    for _ in 0..3 {
        rig.advance(Duration::from_secs(60));
        rig.tick_health().await.unwrap();
    }

    let events = rig.broker.events();
    let kinds: Vec<&str> = events.iter().map(|(_, _, _, k, _)| k.as_str()).collect();
    assert!(
        !kinds.contains(&"flaky"),
        "should not emit flaky for provisioning node"
    );
    let status = rig.broker.infra_node(PROJECT, NODE).unwrap().status;
    assert_eq!(status, Status::Provisioning);
}

// ---------- health stands down during a user infra action ----------

#[tokio::test]
async fn health_skips_project_while_infra_command_in_flight() {
    // Scenario: a node is `running` with degraded replicas (0/1) that
    // would normally flap flaky after the window. But a user infra
    // action (stop/start/terminate) is in flight for the project. The
    // health loop must STAND DOWN for the whole project: no flaky
    // event, no status write, so it can't race the user action.
    let rig = rig();
    rig.broker.add_infra_node(PROJECT, NODE, "inst1", Status::Running);
    observe(&rig, PROJECT, &[(NODE, "inst1", "bridge", false)]);
    // A user infra action is running.
    rig.broker.set_infra_commands_in_flight(PROJECT, vec![whole_project_command()]);

    // Run well past the flaky window: with no gate, this would emit
    // flaky + reconcile status. With the gate, nothing happens.
    for _ in 0..3 {
        rig.advance(Duration::from_secs(60));
        rig.tick_health().await.unwrap();
    }

    assert!(rig.broker.events().is_empty(), "no health events while a command is in flight");
    // Status untouched: the lifecycle handler owns it.
    assert_eq!(rig.broker.infra_node(PROJECT, NODE).unwrap().status, Status::Running);
    // The loop checked the gate and did NOT proceed to set_status.
    let calls = rig.broker.calls();
    assert!(
        calls.iter().any(|c| matches!(c, BrokerCall::InfraCommandsInFlight { .. })),
        "the gate was consulted"
    );
    assert!(
        !calls.iter().any(|c| matches!(c, BrokerCall::SetStatus { .. })),
        "no autonomous status write while standing down"
    );
}

/// A command in flight on one member's copy stands health down for that
/// copy only: the shared copy of the same node, degraded, still turns
/// flaky, while ada's copy (as degraded) is left to the command.
#[tokio::test]
async fn health_stands_down_only_for_the_copy_a_command_reaches() {
    let rig = rig();
    let ada = weft_core::member::MemberId::new("ada").unwrap();
    rig.broker.add_infra_node(PROJECT, NODE, "inst1", Status::Running);
    rig.broker.add_member_infra_node(PROJECT, NODE, &ada, "inst-ada", Status::Running);
    observe(&rig, PROJECT, &[(NODE, "inst1", "bridge", true), (NODE, "inst-ada", "bridge", true)]);
    rig.tick_health().await.unwrap();
    rig.broker.set_infra_commands_in_flight(
        PROJECT,
        vec![weft_broker_client::protocol::InFlightCommand {
            node_id: Some(NODE.to_string()),
            copies: weft_core::member::Copies::Member(ada.clone()),
        }],
    );
    observe(&rig, PROJECT, &[(NODE, "inst1", "bridge", false), (NODE, "inst-ada", "bridge", false)]);
    rig.tick_health().await.unwrap();
    rig.advance(Duration::from_secs(35));
    rig.tick_health().await.unwrap();

    let flaky: Vec<_> = rig.broker.events().into_iter().filter(|(_, _, _, k, _)| k == "flaky").collect();
    assert_eq!(flaky.len(), 1, "{flaky:?}");
    assert_eq!(flaky[0].2, None, "the shared copy, never ada's");
    assert_eq!(rig.broker.infra_copy(PROJECT, NODE, None).unwrap().status, Status::Flaky);
    assert_eq!(rig.broker.infra_copy(PROJECT, NODE, Some(&ada)).unwrap().status, Status::Running);
    assert!(
        !rig.broker.status_writes().iter().any(|(_, _, member, _)| member.is_some()),
        "no status write for the copy the command owns"
    );
}

#[tokio::test]
async fn health_rearms_after_infra_command_completes() {
    // Same degraded node, but the command finishes between ticks: the
    // gate clears and health resumes (flaky fires on the next windowed
    // tick). Proves the stand-down is transient, not a permanent mute.
    let rig = rig();
    rig.broker.add_infra_node(PROJECT, NODE, "inst1", Status::Running);
    observe(&rig, PROJECT, &[(NODE, "inst1", "bridge", true)]);

    // A user action runs (e.g. an upgrade): health stands down. The
    // gate also clears any in-memory window, so health resumes clean.
    rig.broker.set_infra_commands_in_flight(PROJECT, vec![whole_project_command()]);
    rig.advance(Duration::from_secs(60));
    rig.tick_health().await.unwrap();
    assert!(rig.broker.events().is_empty(), "gated: no events");

    // Action completes; node is back up and healthy. Health re-arms
    // and establishes a fresh ready baseline.
    rig.broker.set_infra_commands_in_flight(PROJECT, Vec::new());
    rig.tick_health().await.unwrap();

    // Now the node genuinely degrades, post-action. Flaky must fire
    // after the window: proof the monitor is live again, not muted.
    observe(&rig, PROJECT, &[(NODE, "inst1", "bridge", false)]);
    rig.tick_health().await.unwrap(); // first not-ready observation
    rig.advance(Duration::from_secs(35)); // past the 30s flaky window
    rig.tick_health().await.unwrap();

    let kinds: Vec<String> = rig.broker.events().iter().map(|(_, _, _, k, _)| k.clone()).collect();
    assert!(kinds.contains(&"flaky".to_string()), "health re-armed after the command completed");
}

// ---------- ready→flaky→recovered cycle ----------

#[tokio::test]
async fn full_lifecycle_emits_flaky_then_recovered() {
    let rig = rig();
    rig.broker.add_infra_node(PROJECT, NODE, "inst1", Status::Running);
    observe(&rig, PROJECT, &[(NODE, "inst1", "bridge", true)]);

    // Tick 1: ready. No event.
    rig.tick_health().await.unwrap();
    assert_eq!(rig.broker.events().len(), 0);

    // Replicas degrade.
    observe(&rig, PROJECT, &[(NODE, "inst1", "bridge", false)]);

    // Tick 2 (immediately): still inside flaky window, no flaky yet.
    rig.tick_health().await.unwrap();
    let event_kinds: Vec<String> =
        rig.broker.events().iter().map(|(_, _, _, k, _)| k.clone()).collect();
    assert!(!event_kinds.contains(&"flaky".to_string()));

    // Advance past flaky window. Tick 3: emits flaky + sets status.
    // The default `park-while-infra-broken` protocol ALSO fires here,
    // enqueueing a `deactivate` lifecycle command via
    // `enqueue_lifecycle`. We filter for `flaky` event_record entries
    // specifically; protocol firing is observed via
    // `BrokerCall::EnqueueLifecycle` counts (see the
    // `fired_set_rearms_when_the_copy_heals` test).
    rig.advance(Duration::from_secs(35));
    rig.tick_health().await.unwrap();

    let events = rig.broker.events();
    let flaky_events: Vec<_> = events.iter().filter(|(_, _, _, k, _)| k == "flaky").collect();
    assert_eq!(flaky_events.len(), 1);
    assert_eq!(flaky_events[0].1.as_deref(), Some(NODE));
    let status = rig.broker.infra_node(PROJECT, NODE).unwrap().status;
    assert_eq!(status, Status::Flaky);

    // Replicas recover.
    observe(&rig, PROJECT, &[(NODE, "inst1", "bridge", true)]);

    // Tick 4 (immediately after recovery): inside recovery window,
    // no recovered yet.
    rig.tick_health().await.unwrap();
    let event_kinds: Vec<String> =
        rig.broker.events().iter().map(|(_, _, _, k, _)| k.clone()).collect();
    assert!(!event_kinds.contains(&"recovered".to_string()));

    // Advance past recovery window. Tick 5: emits recovered.
    rig.advance(Duration::from_secs(35));
    rig.tick_health().await.unwrap();

    let events = rig.broker.events();
    let kinds: Vec<String> = events.iter().map(|(_, _, _, k, _)| k.clone()).collect();
    assert!(kinds.contains(&"recovered".to_string()));
    let status = rig.broker.infra_node(PROJECT, NODE).unwrap().status;
    assert_eq!(status, Status::Running);
}

// ---------- per-unit health on a multi-unit node ----------

#[tokio::test]
async fn one_flaky_unit_does_not_drag_down_a_healthy_sibling() {
    // A node with two units: `primary` (healthy) and `sidecar`
    // (degrades). Per-unit health means only `sidecar` goes flaky; the
    // node rolls up to flaky (worst-of-units) but `primary` stays
    // Running. Pre-per-unit, the node summed (2 desired, 1 ready) and
    // there was no way to tell which unit was down.
    let rig = rig();
    rig.broker.add_infra_node_units(
        PROJECT,
        NODE,
        "inst1",
        &[("primary", Status::Running), ("sidecar", Status::Running)],
    );
    observe(&rig, PROJECT, &[(NODE, "inst1", "primary", true), (NODE, "inst1", "sidecar", true)]);

    // Tick 1: both ready, baseline established.
    rig.tick_health().await.unwrap();
    assert!(rig.broker.events().is_empty());

    // sidecar degrades; primary stays healthy.
    observe(&rig, PROJECT, &[(NODE, "inst1", "primary", true), (NODE, "inst1", "sidecar", false)]);
    rig.tick_health().await.unwrap(); // first not-ready observation
    rig.advance(Duration::from_secs(35)); // past the 30s window
    rig.tick_health().await.unwrap();

    // A flaky event fired, naming the sidecar unit.
    let flaky: Vec<_> = rig
        .broker
        .events()
        .into_iter()
        .filter(|(_, _, _, k, _)| k == "flaky")
        .collect();
    assert_eq!(flaky.len(), 1, "exactly one flaky edge (the sidecar)");

    // Per-unit truth: sidecar flaky, primary still running.
    let row = rig.broker.infra_node(PROJECT, NODE).unwrap();
    assert_eq!(row.units.get("sidecar").unwrap().status, Status::Flaky);
    assert_eq!(row.units.get("primary").unwrap().status, Status::Running);
    // Node rollup = worst-of-units = flaky.
    assert_eq!(row.status, Status::Flaky);
}

// ---------- state reset on stop→start ----------

#[tokio::test]
async fn stop_clears_health_state_then_start_does_not_flake() {
    // Bug we hit before extraction: when status leaves `running`,
    // health state was retained, so the first re-observation of
    // not-ready (which happens during provisioning) computed
    // last_ready_at-from-the-stale-deployment, instantly flagging
    // the node as flaky. Test: simulate stop→start and assert no
    // flaky event fires.
    let rig = rig();
    rig.broker.add_infra_node(PROJECT, NODE, "inst1", Status::Running);
    observe(&rig, PROJECT, &[(NODE, "inst1", "bridge", true)]);

    // Run a few ticks to populate the health state.
    for _ in 0..2 {
        rig.advance(Duration::from_secs(10));
        rig.tick_health().await.unwrap();
    }

    // Stop: status flips to stopped. (Simulating what lifecycle's
    // stop verb would do; we just write directly here.)
    rig.broker
        .set_status("test-instance", None, PROJECT, NODE, None, Some(NODE), Status::Stopped, None, None)
        .await
        .unwrap();
    observe(&rig, PROJECT, &[(NODE, "inst1", "bridge", false)]);

    // Tick: should NOT evaluate health (status is stopped) AND
    // should clear the state.
    rig.advance(Duration::from_secs(60));
    rig.tick_health().await.unwrap();

    // Start: status flips back to running, replicas come back.
    rig.broker
        .set_status("test-instance", None, PROJECT, NODE, None, Some(NODE), Status::Running, None, None)
        .await
        .unwrap();
    observe(&rig, PROJECT, &[(NODE, "inst1", "bridge", true)]);

    // Several ticks while ready. Should not emit flaky.
    for _ in 0..3 {
        rig.advance(Duration::from_secs(10));
        rig.tick_health().await.unwrap();
    }

    let events = rig.broker.events();
    let flaky_count = events.iter().filter(|(_, _, _, k, _)| k == "flaky").count();
    assert_eq!(flaky_count, 0, "no flaky event should fire on clean start");
}

// ---------- fired-set re-arm ----------

#[tokio::test]
async fn fired_set_rearms_when_the_copy_heals() {
    // After a protocol fires (the default park enqueues a `deactivate`
    // lifecycle command aimed at the broken copy), the supervisor
    // remembers the copy in `fired` so it doesn't fire again on the
    // same degradation. Once that copy is no longer broken, it must
    // re-arm (`health_engine::rearm`) so its next degradation triggers
    // the protocol again.
    let rig = rig();
    rig.broker.add_infra_node(PROJECT, NODE, "inst1", Status::Running);
    observe(&rig, PROJECT, &[(NODE, "inst1", "bridge", true)]);
    rig.tick_health().await.unwrap();

    // Degrade past the flaky window: the park fires (one EnqueueLifecycle).
    observe(&rig, PROJECT, &[(NODE, "inst1", "bridge", false)]);
    rig.tick_health().await.unwrap();
    rig.advance(Duration::from_secs(35));
    rig.tick_health().await.unwrap();
    let enqueue_count_1 = rig
        .broker
        .calls()
        .iter()
        .filter(|c| matches!(c, BrokerCall::EnqueueLifecycle { .. }))
        .count();
    assert_eq!(enqueue_count_1, 1, "default protocol should fire once");

    // Recover: ready for the whole recovery window.
    observe(&rig, PROJECT, &[(NODE, "inst1", "bridge", true)]);
    rig.tick_health().await.unwrap();
    rig.advance(Duration::from_secs(35));
    rig.tick_health().await.unwrap();

    // Degrade again, past the flaky window.
    observe(&rig, PROJECT, &[(NODE, "inst1", "bridge", false)]);
    rig.tick_health().await.unwrap();
    rig.advance(Duration::from_secs(35));
    rig.tick_health().await.unwrap();

    let enqueue_count_2 = rig
        .broker
        .calls()
        .iter()
        .filter(|c| matches!(c, BrokerCall::EnqueueLifecycle { .. }))
        .count();
    assert_eq!(
        enqueue_count_2, 2,
        "default protocol should re-fire after recovery"
    );
}

// ---------- event-then-status ordering ----------

#[tokio::test]
async fn flaky_emits_event_then_set_status() {
    // The action bar reads `infra_event` to drive the badge animation;
    // it expects the event to land before status. Asserting the call
    // order catches a future refactor that flips them.
    let rig = rig();
    rig.broker.add_infra_node(PROJECT, NODE, "inst1", Status::Running);
    observe(&rig, PROJECT, &[(NODE, "inst1", "bridge", true)]);

    rig.tick_health().await.unwrap();
    observe(&rig, PROJECT, &[(NODE, "inst1", "bridge", false)]);
    rig.advance(Duration::from_secs(35));
    rig.tick_health().await.unwrap();

    let calls = rig.broker.calls();
    let mut saw_event = false;
    for c in &calls {
        match c {
            BrokerCall::EventRecord { kind, .. } if kind == "flaky" => {
                saw_event = true;
            }
            BrokerCall::SetStatus { status, .. } if *status == Status::Flaky => {
                assert!(
                    saw_event,
                    "set_status(flaky) must come AFTER event_record(flaky)"
                );
                return;
            }
            _ => {}
        }
    }
    panic!("never saw set_status(flaky)");
}

// ---------- empty project ----------

#[tokio::test]
async fn project_with_no_nodes_no_events() {
    let rig = rig();
    rig.tick_health().await.unwrap();
    assert_eq!(rig.broker.events().len(), 0);
}

// ---------- restart_unit ----------

#[tokio::test]
async fn restart_unit_restarts_only_the_named_unit() {
    let rig = rig_with_restart_protocol();
    rig.advance(Duration::from_secs(35));
    rig.tick_health().await.unwrap();

    let calls = rig.host.calls();
    let restarted: Vec<(String, String)> = calls
        .iter()
        .filter_map(|c| match c {
            HostCall::Restart { instance, unit, .. } => Some((instance.clone(), unit.clone())),
            _ => None,
        })
        .collect();
    assert_eq!(restarted, vec![("inst1".to_string(), "bridge".to_string())]);
    assert!(
        !calls.iter().any(|c| matches!(c, HostCall::Terminate { .. } | HostCall::Remove { .. })),
        "a restart never removes anything"
    );
}

/// Build a project + a RestartUnit protocol with `timeout_seconds=5`
/// on a not-ready unit, so a single tick (past the flaky window)
/// fires the action.
fn rig_with_restart_protocol() -> SupervisorTestRig {
    let rig = rig();
    rig.broker.add_infra_node(PROJECT, NODE, "inst1", Status::Running);
    let protocols_json = serde_json::json!({
        "protocols": [{
            "name": "restart-when-down",
            "when": { "kind": "node_not_ready", "node_id": NODE },
            "action": { "kind": "restart_unit", "node_id": NODE, "unit": "bridge" },
            "timeout_seconds": 5
        }]
    });
    rig.broker.set_health_protocols(PROJECT, protocols_json);
    observe(&rig, PROJECT, &[(NODE, "inst1", "bridge", false)]);
    rig
}

#[tokio::test(start_paused = true)]
async fn hung_action_times_out_frees_inflight_and_unlatches_fired() {
    // The wedge this prevents: a HealthProtocol action's host call
    // hangs (a wedged Docker daemon, a cloud API that never answers). Without the timeout it pins
    // `in_flight` forever and stops all future health ticks. Worse,
    // even with the timeout, if the protocol stayed latched in
    // `fired` it would never retry (the project can't go healthy
    // because recovery is what would make it healthy). This drives
    // the FULL production path (`tick` -> `tick_project` -> the
    // `tokio::time::timeout(run_action)` wrap) and asserts BOTH the
    // mechanical free (`in_flight` cleared) AND the retryability
    // (`fired` un-latched).
    //
    // Under start_paused, when the tick parks on the hung
    // restart (a never-waking `pending()`), the only live timer
    // is the 5s action timeout, so tokio auto-advances to it and
    // fires it deterministically: no real wait. The outer bound is a
    // safety net so a regression (timeout not firing) fails fast
    // instead of hanging the suite.
    let rig = rig_with_restart_protocol();
    rig.host.hang_restarts();
    rig.advance(Duration::from_secs(35)); // FakeClock past the flaky window

    tokio::time::timeout(Duration::from_secs(120), rig.tick_health())
        .await
        .expect("tick_health hung: the action timeout did not fire")
        .unwrap();

    // The action started (the restart recorded) then hung; the
    // timeout mapped it to a failed action.
    assert_eq!(restarts(&rig), 1, "the action should have started before hanging");

    let reg = rig.state.health.lock().await;
    // Mechanical free: slot released so the next tick can evaluate.
    assert!(!reg.is_in_flight(PROJECT), "timed-out action must free in_flight");
    // Retryability: the failed protocol is NOT latched in `fired`, so
    // the next tick re-fires it (the wedge fix).
    assert!(
        !reg.is_fired(PROJECT, "restart-when-down"),
        "timed-out action must un-latch from fired so it retries"
    );
    // Backoff armed: one failure recorded, so the immediate next tick
    // is skipped (no re-fire storm) until the backoff window elapses.
    assert_eq!(
        reg.backoff_failures(PROJECT, "restart-when-down"),
        1,
        "a failed action must arm exponential backoff"
    );
}

#[tokio::test(start_paused = true)]
async fn failed_action_backs_off_then_retries_after_window() {
    // After a failed action, the protocol is un-latched from `fired`
    // (so it WILL retry) but gated by exponential backoff so it does
    // NOT re-fire on every poll tick. Walk: fail once (restart
    // call #1, backoff=1, ~5s window) -> immediate tick is skipped
    // (no call #2) -> advance the clock past the window -> next tick
    // re-fires (call #2, backoff=2).
    let rig = rig_with_restart_protocol();
    rig.host.hang_restarts();
    rig.advance(Duration::from_secs(35));

    // Tick 1: fires, hangs, times out -> failure -> backoff=1.
    tokio::time::timeout(Duration::from_secs(120), rig.tick_health())
        .await
        .expect("tick 1 hung")
        .unwrap();
    let calls_after_1 = restarts(&rig);
    assert_eq!(calls_after_1, 1, "first tick fires the action");
    assert_eq!(rig.state.health.lock().await.backoff_failures(PROJECT, "restart-when-down"), 1);

    // Tick 2 immediately (no clock advance): inside the backoff
    // window -> skipped, no new action.
    rig.tick_health().await.unwrap();
    assert_eq!(
        restarts(&rig),
        1,
        "tick inside backoff window must NOT re-fire"
    );

    // Advance past the first backoff window (5s). Tick 3 re-fires.
    rig.advance(Duration::from_secs(6));
    tokio::time::timeout(Duration::from_secs(120), rig.tick_health())
        .await
        .expect("tick 3 hung")
        .unwrap();
    assert_eq!(restarts(&rig), 2, "tick past backoff window re-fires");
    assert_eq!(
        rig.state.health.lock().await.backoff_failures(PROJECT, "restart-when-down"),
        2,
        "second failure grows the backoff count"
    );
}

#[tokio::test]
async fn completed_action_frees_in_flight() {
    // The success side, through the FULL tick: the action does NOT
    // hang, the timeout wrapper is transparent, the action runs
    // (the restart called) and the in_flight slot frees. Together
    // with `hung_action_times_out_frees_inflight_and_unlatches_fired`
    // this covers both arms of the timeout match in `tick_project`.
    let rig = rig_with_restart_protocol();
    rig.advance(Duration::from_secs(35));
    rig.tick_health().await.unwrap();

    let in_flight = {
        let reg = rig.state.health.lock().await;
        reg.is_in_flight(PROJECT)
    };
    assert!(!in_flight, "completed action must free the in_flight slot");
    assert_eq!(restarts(&rig), 1, "the action should have run");
}

// ---------- regression: set_applied lands mid-flaky ----------

#[tokio::test]
async fn set_applied_landing_mid_flaky_is_re_observed_next_tick() {
    // Regression for round-4: `set_applied` is unconditional. If it
    // lands while the supervisor's in-RAM flaky tracker still sees
    // degraded replicas, the row briefly flips `flaky` → `running`.
    // The next health tick must re-observe and re-write `flaky`
    // because the in-RAM latch (`declared_flaky`, with its
    // `last_not_ready_at`) persists across the row's rewrite.
    //
    // Without that property, the row sticks at `running` while the
    // install is broken.
    let rig = rig();
    rig.broker.add_infra_node(PROJECT, NODE, "inst1", Status::Running);
    observe(&rig, PROJECT, &[(NODE, "inst1", "bridge", true)]);
    // Tick 1: healthy baseline.
    rig.tick_health().await.unwrap();

    // Replicas degrade and stay degraded.
    observe(&rig, PROJECT, &[(NODE, "inst1", "bridge", false)]);

    // Tick 2 after the flaky window: emits flaky, writes Flaky status.
    rig.advance(Duration::from_secs(35));
    rig.tick_health().await.unwrap();
    assert_eq!(rig.broker.infra_node(PROJECT, NODE).unwrap().status, Status::Flaky);

    // Simulate a parallel `set_applied` landing: external write
    // flips status back to Running with no failure_stage. (In
    // production this comes from the supervisor's own apply path;
    // here we mutate the broker fake to mimic the race.)
    rig.broker
        .add_infra_node_with(
            PROJECT,
            NODE,
            "inst1",
            Status::Running,
            Some("hash".into()),
            std::collections::BTreeMap::new(),
            unit_map(NODE, Status::Running),
        );
    assert_eq!(rig.broker.infra_node(PROJECT, NODE).unwrap().status, Status::Running);

    // Tick 3: replicas still degraded, the latch still declared flaky
    // from before. The tick derives Flaky from the latch (no new edge,
    // so no second `flaky` event) and writes Flaky status back.
    rig.advance(Duration::from_secs(1));
    rig.tick_health().await.unwrap();
    assert_eq!(
        rig.broker.infra_node(PROJECT, NODE).unwrap().status,
        Status::Flaky,
        "next tick must re-observe degraded replicas and rewrite Flaky"
    );
}

/// A frozen unit the row calls `Flaky`, still at 0 ready, must stay
/// `Flaky` on the first tick after its latch was cleared (a command in
/// flight on the copy clears it): the fresh latch is seeded
/// from the row, so the reconcile has nothing to rewrite. Before the
/// seeding, the empty latch derived `Running` and rewrote the unit,
/// and the node with it, while nothing was ready.
#[tokio::test]
async fn frozen_flaky_unit_is_not_rewritten_running_after_latch_reset() {
    let rig = rig();
    rig.broker.add_infra_node_with(
        PROJECT,
        NODE,
        "inst1",
        Status::Flaky,
        Some("hash".into()),
        std::collections::BTreeMap::new(),
        unit_map(NODE, Status::Flaky),
    );
    observe(&rig, PROJECT, &[(NODE, "inst1", "bridge", false)]);
    // A command in flight on the copy clears its latch and stands the
    // tick down for it.
    rig.broker.set_infra_commands_in_flight(PROJECT, vec![whole_project_command()]);
    rig.tick_health().await.unwrap();
    rig.broker.set_infra_commands_in_flight(PROJECT, Vec::new());

    rig.advance(Duration::from_secs(1));
    rig.tick_health().await.unwrap();
    assert_eq!(rig.broker.infra_node(PROJECT, NODE).unwrap().status, Status::Flaky);
    assert!(
        !rig.broker.calls().iter().any(|c| matches!(c, BrokerCall::SetStatus { .. })),
        "no status rewrite for a unit the row already calls Flaky; calls={:?}",
        rig.broker.calls()
    );

    // Ready for the whole recovery window: recovered, and only then
    // written Running.
    observe(&rig, PROJECT, &[(NODE, "inst1", "bridge", true)]);
    rig.advance(Duration::from_secs(1));
    rig.tick_health().await.unwrap();
    assert_eq!(rig.broker.infra_node(PROJECT, NODE).unwrap().status, Status::Flaky);
    rig.advance(Duration::from_secs(35));
    rig.tick_health().await.unwrap();
    assert_eq!(rig.broker.infra_node(PROJECT, NODE).unwrap().status, Status::Running);
}

// ---------- regression: two-stage default protocols ----------

/// The take-downs the rig's broker was asked to enqueue for `project`.
fn take_downs(rig: &SupervisorTestRig, project: uuid::Uuid) -> Vec<weft_broker_client::protocol::TakeDownReaders> {
    rig.broker
        .calls()
        .into_iter()
        .filter_map(|c| match c {
            BrokerCall::EnqueueLifecycle { project_id, spec: weft_broker_client::protocol::LifecycleSpec::Deactivate(d) }
                if project_id == project =>
            {
                Some(d)
            }
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn default_protocol_parks_what_reads_the_broken_copy() {
    // Stage 1 of the default pair: an Active project whose unit stays
    // not ready for the whole flaky window (after having been ready)
    // enqueues a park aimed at that copy, and only at it: the
    // dispatcher then parks only the triggers reading it.
    let rig = rig();
    rig.broker.add_project_with_status(PROJ_ACTIVE,
        weft_broker_client::protocol::ProjectStatus::Active,
    );
    rig.broker.add_infra_node(PROJ_ACTIVE, NODE, "inst1", Status::Running);
    observe(&rig, PROJ_ACTIVE, &[(NODE, "inst1", "bridge", true)]);
    rig.tick_health().await.unwrap();
    observe(&rig, PROJ_ACTIVE, &[(NODE, "inst1", "bridge", false)]);
    rig.tick_health().await.unwrap();
    assert!(take_downs(&rig, PROJ_ACTIVE).is_empty(), "one bad reading parks nothing");

    rig.advance(Duration::from_secs(35));
    rig.tick_health().await.unwrap();
    let parks = take_downs(&rig, PROJ_ACTIVE);
    assert_eq!(parks.len(), 1, "the latched breakage parks once");
    assert_eq!(parks[0].spec.mode, weft_broker_client::protocol::DeactivationMode::Park);
    assert_eq!(
        parks[0].reach,
        weft_broker_client::protocol::TakeDownReach::ReadersOf {
            broken: vec![weft_broker_client::protocol::InfraCopy { node_id: NODE.into(), member: None }]
        }
    );
}

/// The incident: a member's copy was just applied (its row says
/// Running) and the host has not reported it yet. Unknown is
/// not broken: however long that lasts, nothing parks and nothing is
/// called flaky. A member's copy that does break parks only that copy.
#[tokio::test]
async fn a_copy_the_host_has_not_reported_never_parks_and_a_broken_one_parks_alone() {
    let rig = rig();
    let ada = weft_core::member::MemberId::new("ada").unwrap();
    rig.broker.add_infra_node(PROJECT, "shared", "inst-shared", Status::Running);
    rig.broker.add_member_infra_node(PROJECT, NODE, &ada, "inst1", Status::Running);
    observe(&rig, PROJECT, &[("shared", "inst-shared", "shared", true)]);
    for _ in 0..4 {
        rig.advance(Duration::from_secs(60));
        rig.tick_health().await.unwrap();
    }
    assert!(take_downs(&rig, PROJECT).is_empty(), "an unseen copy is unknown, never broken");
    assert!(!rig.broker.events().iter().any(|(_, _, _, k, _)| k == "flaky"));

    // Ada's copy shows up ready, then breaks for the whole window.
    observe(&rig, PROJECT, &[("shared", "inst-shared", "shared", true), (NODE, "inst1", "bridge", true)]);
    rig.tick_health().await.unwrap();
    observe(&rig, PROJECT, &[("shared", "inst-shared", "shared", true), (NODE, "inst1", "bridge", false)]);
    rig.tick_health().await.unwrap();
    rig.advance(Duration::from_secs(35));
    rig.tick_health().await.unwrap();
    let parks = take_downs(&rig, PROJECT);
    assert_eq!(parks.len(), 1);
    assert_eq!(
        parks[0].reach,
        weft_broker_client::protocol::TakeDownReach::ReadersOf {
            broken: vec![weft_broker_client::protocol::InfraCopy { node_id: NODE.into(), member: Some(ada) }]
        },
        "only ada's copy is broken; the shared one is healthy"
    );
}

#[tokio::test]
async fn default_protocol_auto_recovers_what_it_parked_on_infra_healthy() {
    // Stage 2 of the two-stage protocol: with every seen unit healthy
    // and something the health loop parked still down, reactivate.
    let rig = rig();
    rig.broker.add_project_with_status(PROJ_INACTIVE,
        weft_broker_client::protocol::ProjectStatus::Inactive,
    );
    // The health loop parked it: auto-recover is allowed to undo it.
    rig.broker.set_health_parked(PROJ_INACTIVE, true);
    rig.broker.add_infra_node(PROJ_INACTIVE, NODE, "inst1", Status::Running);
    observe(&rig, PROJ_INACTIVE, &[(NODE, "inst1", "bridge", true)]);

    rig.tick_health().await.unwrap();

    let calls = rig.broker.calls();
    let reactivated = calls.iter().any(|c| matches!(
        c,
        BrokerCall::EnqueueLifecycle { project_id, spec }
            if *project_id == PROJ_INACTIVE
            && matches!(spec, weft_broker_client::protocol::LifecycleSpec::Reactivate(_))
    ));
    assert!(
        reactivated,
        "default protocol auto-recover-when-infra-healthy must enqueue a reactivate when infra is up and it parked something"
    );
}

#[tokio::test]
async fn default_protocol_does_not_reactivate_user_deactivated_project() {
    // Regression: the user clicked Stop infra (or Deactivate / Upgrade)
    // on a running+active project. The infra processes are still up for a
    // moment (or being brought back up by a later Start). A health tick
    // that lands then must NOT auto-reactivate, because the USER
    // deactivated (nothing is `health_parked`), not the health loop.
    // Before the gate, this raced into a reactivate and left the user
    // with active triggers but no/just-stopped infra.
    let rig = rig();
    rig.broker.add_project_with_status(PROJ_USER_OFF,
        weft_broker_client::protocol::ProjectStatus::Inactive,
    );
    // The USER deactivated: nothing is health-parked (default).
    rig.broker.add_infra_node(PROJ_USER_OFF, NODE, "inst1", Status::Running);
    observe(&rig, PROJ_USER_OFF, &[(NODE, "inst1", "bridge", true)]);

    rig.tick_health().await.unwrap();

    let any_reactivate = rig.broker.calls().iter().any(|c| matches!(
        c,
        BrokerCall::EnqueueLifecycle { project_id, spec }
            if *project_id == PROJ_USER_OFF
            && matches!(spec, weft_broker_client::protocol::LifecycleSpec::Reactivate(_))
    ));
    assert!(
        !any_reactivate,
        "a USER-deactivated project (nothing health-parked) must NOT be auto-reactivated, \
         even when infra is healthy"
    );
}

#[tokio::test]
async fn default_protocol_does_not_fire_when_status_mismatches() {
    // Cross-check: an Active project with HEALTHY infra and nothing
    // health-parked must NOT enqueue a reactivate. And an Inactive
    // project with BROKEN infra must NOT enqueue a park (the
    // first-stage condition requires Active: nothing listens to park).
    let rig = rig();
    rig.broker.add_project_with_status(ACTIVE_HEALTHY,
        weft_broker_client::protocol::ProjectStatus::Active,
    );
    rig.broker.add_infra_node(ACTIVE_HEALTHY, NODE, "inst1", Status::Running);
    observe(&rig, ACTIVE_HEALTHY, &[(NODE, "inst1", "bridge", true)]);
    rig.tick_health().await.unwrap();

    rig.broker.add_project_with_status(INACTIVE_BROKEN,
        weft_broker_client::protocol::ProjectStatus::Inactive,
    );
    rig.broker.add_infra_node(INACTIVE_BROKEN, NODE, "inst1", Status::Running);
    observe(&rig, INACTIVE_BROKEN, &[(NODE, "inst1", "bridge", true)]);
    rig.tick_health().await.unwrap();
    observe(&rig, INACTIVE_BROKEN, &[(NODE, "inst1", "bridge", false)]);
    rig.tick_health().await.unwrap();
    rig.advance(Duration::from_secs(35));
    rig.tick_health().await.unwrap();
    assert_eq!(
        rig.broker.infra_node(INACTIVE_BROKEN, NODE).unwrap().status,
        Status::Flaky,
        "the unit did break"
    );

    let calls = rig.broker.calls();
    let any_lifecycle = calls.iter().any(|c| matches!(c, BrokerCall::EnqueueLifecycle { .. }));
    assert!(
        !any_lifecycle,
        "neither (Active+healthy) nor (Inactive+broken) should fire the default two-stage protocol"
    );
}
// ---------- autonomous 3-stage recovery: deactivate -> restart -> reactivate ----------

#[tokio::test]
async fn three_stage_recovery_deactivate_restart_reactivate() {
    // A node author wires an autonomous flaky-recovery protocol set:
    //   1. flaky + Active   -> ParkTriggers   (deactivate the triggers)
    //   2. flaky + Inactive -> RestartUnit     (restart the wedged unit)
    //   3. healthy+ Inactive-> AutoRecover    (reactivate the triggers)
    // No human in the loop. This proves the framework can express + run
    // the full cycle. The supervisor enqueues the deactivate/reactivate
    // (the dispatcher's claimer would run them + flip project status);
    // here we flip status manually to simulate the claimer, since the
    // rig runs only the health loop. The flaky window is 30s; each
    // protocol gets timeout 5s. The `unit` selector targets "bridge".
    let rig = rig();
    rig.broker.add_infra_node(PROJECT, NODE, "inst1", Status::Running);
    let protocols = serde_json::json!({
        "protocols": [
            {
                "name": "park",
                "when": { "kind": "all", "conds": [
                    { "kind": "node_not_ready", "node_id": NODE, "unit": "bridge" },
                    { "kind": "project_status_eq", "status": "active" }
                ]},
                "action": { "kind": "park_triggers" },
                "timeout_seconds": 5
            },
            {
                "name": "restart",
                "when": { "kind": "all", "conds": [
                    { "kind": "node_not_ready", "node_id": NODE, "unit": "bridge" },
                    { "kind": "project_status_eq", "status": "inactive" }
                ]},
                "action": { "kind": "restart_unit", "node_id": NODE, "unit": "bridge" },
                "timeout_seconds": 5
            },
            {
                "name": "recover",
                "when": { "kind": "all", "conds": [
                    { "kind": "not", "cond": [
                        { "kind": "node_not_ready", "node_id": NODE, "unit": "bridge" }
                    ]},
                    { "kind": "project_status_eq", "status": "inactive" }
                ]},
                "action": { "kind": "auto_recover" },
                "timeout_seconds": 5
            }
        ]
    });
    rig.broker.set_health_protocols(PROJECT, protocols);

    // The unit goes flaky (0 of 1 ready). Project starts Active.
    observe(&rig, PROJECT, &[(NODE, "inst1", "bridge", false)]);

    // --- Stage 1: flaky + Active -> ParkTriggers (Deactivate enqueued) ---
    rig.advance(Duration::from_secs(35)); // past the flaky window
    rig.tick_health().await.unwrap();
    let last_lifecycle = |rig: &SupervisorTestRig| {
        rig.broker.calls().into_iter().rev().find_map(|c| match c {
            BrokerCall::EnqueueLifecycle { spec, .. } => Some(spec),
            _ => None,
        })
    };
    use weft_broker_client::protocol::LifecycleSpec;
    assert!(
        matches!(last_lifecycle(&rig), Some(LifecycleSpec::Deactivate(_))),
        "stage 1 must enqueue a Deactivate (park)"
    );
    // Simulate the claimer running the deactivate: project -> Inactive.
    rig.broker.add_project_with_status(PROJECT,
        weft_broker_client::protocol::ProjectStatus::Inactive,
    );

    // --- Stage 2: flaky + Inactive -> RestartUnit ---
    rig.advance(Duration::from_secs(35));
    rig.tick_health().await.unwrap();
    assert_eq!(restarts(&rig), 1, "stage 2 must restart the unit");

    // The restart worked: the unit is ready again.
    observe(&rig, PROJECT, &[(NODE, "inst1", "bridge", true)]);

    // --- Stage 3: healthy + Inactive -> AutoRecover (Reactivate enqueued) ---
    rig.advance(Duration::from_secs(35));
    rig.tick_health().await.unwrap();
    assert!(
        matches!(last_lifecycle(&rig), Some(LifecycleSpec::Reactivate(_))),
        "stage 3 must enqueue a Reactivate (auto-recover)"
    );

    // Exactly one of each lifecycle verb fired across the whole cycle:
    // park (deactivate) once, recover (reactivate) once. The fired-set
    // prevents re-firing within the episode.
    let calls = rig.broker.calls();
    let deactivates = calls.iter().filter(|c| {
        matches!(c, BrokerCall::EnqueueLifecycle { spec: LifecycleSpec::Deactivate(_), .. })
    }).count();
    let reactivates = calls.iter().filter(|c| {
        matches!(c, BrokerCall::EnqueueLifecycle { spec: LifecycleSpec::Reactivate(_), .. })
    }).count();
    assert_eq!(deactivates, 1, "exactly one deactivate across the cycle");
    assert_eq!(reactivates, 1, "exactly one reactivate across the cycle");
}

// ---------- ownership ----------

/// A lost project's health state is dropped at once (its latch with it),
/// and a claimed one is looked at at once, without waiting for the tick.
#[tokio::test]
async fn ownership_changes_reach_the_health_loop_at_once() {
    use weft_infra_supervisor::ownership::OwnershipChange;
    let rig = rig_with_restart_protocol();
    rig.tick_health().await.unwrap();
    assert_eq!(restarts(&rig), 1);
    assert!(rig.state.health.lock().await.is_fired(PROJECT, "restart-when-down"));

    rig.health_ownership_change(&OwnershipChange { claimed: vec![], lost: vec![PROJECT] }).await.unwrap();
    assert!(
        !rig.state.health.lock().await.is_fired(PROJECT, "restart-when-down"),
        "a lost project's latch is dropped"
    );

    rig.health_ownership_change(&OwnershipChange { claimed: vec![PROJECT], lost: vec![] }).await.unwrap();
    assert_eq!(restarts(&rig), 2, "a claimed project is looked at without waiting for the tick");
}
