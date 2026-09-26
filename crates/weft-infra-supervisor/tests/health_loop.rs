//! Layer-3 integration tests for the supervisor's health loop.
//!
//! These exercise the full `tick_health` orchestration against
//! in-memory fakes: seed broker + kube state, advance the clock,
//! call `tick_health()`, assert on emitted events + status writes.
//!
//! Counter-cases the pure-function tests can't catch:
//!   - lock ordering inside `tick_project`,
//!   - state reset when status leaves `running` / `flaky`,
//!   - the broker call sequence (event_record before set_status),
//!   - fired-set re-arming after recovery.

use std::collections::HashMap;
use std::time::Duration;

use weft_broker_client::protocol::InfraNodeStatus as Status;
use weft_infra_supervisor::broker_ops::{BrokerCall, BrokerSupervisorOps};
use weft_infra_supervisor::testing::SupervisorTestRig;
use weft_platform_traits::kube::{KubeCall, WorkloadKind, WorkloadReplicaState};

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
            stop_behavior: weft_core::StopBehavior::ScaleToZero,
            flaky_after_seconds: 30,
            recovery_after_seconds: 30,
            image_refs: Default::default(),
            watched: true,
            scaled_to: None,
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
const NAMESPACE: &str = "wft-project-test-proj1";
const NODE: &str = "bridge";

/// A stop of every node's shared copies, in flight.
fn whole_project_command() -> weft_broker_client::protocol::InFlightCommand {
    weft_broker_client::protocol::InFlightCommand { node_id: None, copies: weft_core::member::Copies::Shared }
}

fn rig() -> SupervisorTestRig {
    let rig = SupervisorTestRig::with_tenant(TENANT);
    rig.broker.add_project(PROJECT, NAMESPACE);
    rig
}

fn workload(name: &str, node_id: &str, desired: i64, ready: i64) -> WorkloadReplicaState {
    workload_with_unit(name, node_id, "bridge", desired, ready)
}

/// The one-unit workload of the copy deployed as `instance`.
fn workload_for_instance(instance: &str, node_id: &str, desired: i64, ready: i64) -> WorkloadReplicaState {
    let mut w = workload(&format!("{instance}-{node_id}"), node_id, desired, ready);
    w.labels.insert("weft.dev/instance".into(), instance.into());
    w
}

fn workload_with_unit(
    name: &str,
    node_id: &str,
    unit: &str,
    desired: i64,
    ready: i64,
) -> WorkloadReplicaState {
    let mut labels = HashMap::new();
    labels.insert("weft.dev/node".into(), weft_core::infra::node_label_value(node_id));
    labels.insert("weft.dev/instance".into(), "inst1".into());
    labels.insert("weft.dev/unit".into(), unit.into());
    labels.insert("weft.dev/role".into(), "infra".into());
    WorkloadReplicaState {
        kind: WorkloadKind::Deployment,
        name: name.into(),
        namespace: NAMESPACE.into(),
        desired,
        ready,
        labels,
    }
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
    rig.kube
        .set_workloads(NAMESPACE, vec![workload("inst1-bridge", NODE, 1, 0)]);

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
    rig.kube
        .set_workloads(NAMESPACE, vec![workload("inst1-bridge", NODE, 1, 0)]);
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
    rig.kube.set_workloads(
        NAMESPACE,
        vec![workload_for_instance("inst1", NODE, 1, 1), workload_for_instance("inst-ada", NODE, 1, 1)],
    );
    rig.tick_health().await.unwrap();
    rig.broker.set_infra_commands_in_flight(
        PROJECT,
        vec![weft_broker_client::protocol::InFlightCommand {
            node_id: Some(NODE.to_string()),
            copies: weft_core::member::Copies::Member(ada.clone()),
        }],
    );
    rig.kube.set_workloads(
        NAMESPACE,
        vec![workload_for_instance("inst1", NODE, 1, 0), workload_for_instance("inst-ada", NODE, 1, 0)],
    );
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
    rig.kube
        .set_workloads(NAMESPACE, vec![workload("inst1-bridge", NODE, 1, 1)]);

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
    rig.kube
        .set_workloads(NAMESPACE, vec![workload("inst1-bridge", NODE, 1, 0)]);
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
    rig.kube
        .set_workloads(NAMESPACE, vec![workload("inst1-bridge", NODE, 1, 1)]);

    // Tick 1: ready. No event.
    rig.tick_health().await.unwrap();
    assert_eq!(rig.broker.events().len(), 0);

    // Replicas degrade.
    rig.kube
        .set_workloads(NAMESPACE, vec![workload("inst1-bridge", NODE, 1, 0)]);

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
    rig.kube
        .set_workloads(NAMESPACE, vec![workload("inst1-bridge", NODE, 1, 1)]);

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
    rig.kube.set_workloads(
        NAMESPACE,
        vec![
            workload_with_unit("inst1-primary", NODE, "primary", 1, 1),
            workload_with_unit("inst1-sidecar", NODE, "sidecar", 1, 1),
        ],
    );

    // Tick 1: both ready, baseline established.
    rig.tick_health().await.unwrap();
    assert!(rig.broker.events().is_empty());

    // sidecar degrades; primary stays healthy.
    rig.kube.set_workloads(
        NAMESPACE,
        vec![
            workload_with_unit("inst1-primary", NODE, "primary", 1, 1),
            workload_with_unit("inst1-sidecar", NODE, "sidecar", 1, 0),
        ],
    );
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
    rig.kube
        .set_workloads(NAMESPACE, vec![workload("inst1-bridge", NODE, 1, 1)]);

    // Run a few ticks to populate the health state.
    for _ in 0..2 {
        rig.advance(Duration::from_secs(10));
        rig.tick_health().await.unwrap();
    }

    // Stop: status flips to stopped. (Simulating what lifecycle's
    // stop verb would do; we just write directly here.)
    rig.broker
        .set_status("test-pod", None, PROJECT, NODE, None, Some(NODE), Status::Stopped, None, None)
        .await
        .unwrap();
    rig.kube
        .set_workloads(NAMESPACE, vec![workload("inst1-bridge", NODE, 0, 0)]);

    // Tick: should NOT evaluate health (status is stopped) AND
    // should clear the state.
    rig.advance(Duration::from_secs(60));
    rig.tick_health().await.unwrap();

    // Start: status flips back to running, replicas come back.
    rig.broker
        .set_status("test-pod", None, PROJECT, NODE, None, Some(NODE), Status::Running, None, None)
        .await
        .unwrap();
    rig.kube
        .set_workloads(NAMESPACE, vec![workload("inst1-bridge", NODE, 1, 1)]);

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
    rig.kube
        .set_workloads(NAMESPACE, vec![workload("inst1-bridge", NODE, 1, 1)]);
    rig.tick_health().await.unwrap();

    // Degrade past the flaky window: the park fires (one EnqueueLifecycle).
    rig.kube
        .set_workloads(NAMESPACE, vec![workload("inst1-bridge", NODE, 1, 0)]);
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
    rig.kube
        .set_workloads(NAMESPACE, vec![workload("inst1-bridge", NODE, 1, 1)]);
    rig.tick_health().await.unwrap();
    rig.advance(Duration::from_secs(35));
    rig.tick_health().await.unwrap();

    // Degrade again, past the flaky window.
    rig.kube
        .set_workloads(NAMESPACE, vec![workload("inst1-bridge", NODE, 1, 0)]);
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
    rig.kube
        .set_workloads(NAMESPACE, vec![workload("inst1-bridge", NODE, 1, 1)]);

    rig.tick_health().await.unwrap();
    rig.kube
        .set_workloads(NAMESPACE, vec![workload("inst1-bridge", NODE, 1, 0)]);
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

// ---------- bounce_pods regression ----------

#[tokio::test]
async fn bounce_pods_deletes_only_pods_not_workloads() {
    // Regression test for the round-1 BouncePods bug. The action
    // MUST route through `delete_pods` (pods-only) and MUST NOT
    // touch `delete_by_label` (which nukes Deployments, Services,
    // ConfigMaps, Secrets, PVCs).
    let rig = rig();
    rig.broker.add_infra_node(PROJECT, NODE, "inst1", Status::Running);
    // Configure a project-specific protocol that triggers BouncePods
    // on zero ready replicas.
    let protocols_json = serde_json::json!({
        "protocols": [{
            "name": "bounce-on-zero",
            "when": {
                "kind": "node_ready_replicas",
                "node_id": NODE,
                "op": "eq",
                "value": 0
            },
            "action": {
                "kind": "bounce_pods",
                "node_id": NODE,
                "unit": "bridge"
            },
            "timeout_seconds": 60
        }]
    });
    rig.broker.set_health_protocols(PROJECT, protocols_json);
    rig.kube.set_workloads(
        NAMESPACE,
        vec![workload_with_unit("inst1-bridge", NODE, "bridge", 1, 0)],
    );
    rig.advance(Duration::from_secs(35));
    rig.tick_health().await.unwrap();

    let calls = rig.kube.calls();
    // The protocol fires → ActionPlan::BouncePods → KubeWriter::delete_pods.
    let saw_delete_pods = calls.iter().any(|c| matches!(c, KubeCall::DeletePods { .. }));
    let saw_delete_by_label = calls.iter().any(|c| matches!(c, KubeCall::DeleteByLabel { .. }));
    assert!(saw_delete_pods, "BouncePods must call delete_pods");
    assert!(
        !saw_delete_by_label,
        "BouncePods must NOT call delete_by_label (regression of round-1 bug)"
    );
}

/// Build a project + a BouncePods protocol with `timeout_seconds=5`
/// on a zero-ready node, so a single tick (past the flaky window)
/// fires the action.
fn rig_with_bounce_protocol() -> SupervisorTestRig {
    let rig = rig();
    rig.broker.add_infra_node(PROJECT, NODE, "inst1", Status::Running);
    let protocols_json = serde_json::json!({
        "protocols": [{
            "name": "bounce-on-zero",
            "when": { "kind": "node_ready_replicas", "node_id": NODE, "op": "eq", "value": 0 },
            "action": { "kind": "bounce_pods", "node_id": NODE, "unit": "bridge" },
            "timeout_seconds": 5
        }]
    });
    rig.broker.set_health_protocols(PROJECT, protocols_json);
    rig.kube.set_workloads(
        NAMESPACE,
        vec![workload_with_unit("inst1-bridge", NODE, "bridge", 1, 0)],
    );
    rig
}

#[tokio::test(start_paused = true)]
async fn hung_action_times_out_frees_inflight_and_unlatches_fired() {
    // The wedge this prevents: a HealthProtocol action's kube call
    // hangs (wedged apiserver). Without the timeout it pins
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
    // `delete_pods` (a never-waking `pending()`), the only live timer
    // is the 5s action timeout, so tokio auto-advances to it and
    // fires it deterministically: no real wait. The outer bound is a
    // safety net so a regression (timeout not firing) fails fast
    // instead of hanging the suite.
    let rig = rig_with_bounce_protocol();
    rig.kube.hang_delete_pods();
    rig.advance(Duration::from_secs(35)); // FakeClock past the flaky window

    tokio::time::timeout(Duration::from_secs(120), rig.tick_health())
        .await
        .expect("tick_health hung: the action timeout did not fire")
        .unwrap();

    // The action started (delete_pods recorded) then hung; the
    // timeout mapped it to a failed action.
    let saw_delete_pods = rig
        .kube
        .calls()
        .iter()
        .any(|c| matches!(c, KubeCall::DeletePods { .. }));
    assert!(saw_delete_pods, "the action should have started before hanging");

    let reg = rig.state.health.lock().await;
    // Mechanical free: slot released so the next tick can evaluate.
    assert!(!reg.is_in_flight(PROJECT), "timed-out action must free in_flight");
    // Retryability: the failed protocol is NOT latched in `fired`, so
    // the next tick re-fires it (the wedge fix).
    assert!(
        !reg.is_fired(PROJECT, "bounce-on-zero"),
        "timed-out action must un-latch from fired so it retries"
    );
    // Backoff armed: one failure recorded, so the immediate next tick
    // is skipped (no re-fire storm) until the backoff window elapses.
    assert_eq!(
        reg.backoff_failures(PROJECT, "bounce-on-zero"),
        1,
        "a failed action must arm exponential backoff"
    );
}

#[tokio::test(start_paused = true)]
async fn failed_action_backs_off_then_retries_after_window() {
    // After a failed action, the protocol is un-latched from `fired`
    // (so it WILL retry) but gated by exponential backoff so it does
    // NOT re-fire on every poll tick. Walk: fail once (delete_pods
    // call #1, backoff=1, ~5s window) -> immediate tick is skipped
    // (no call #2) -> advance the clock past the window -> next tick
    // re-fires (call #2, backoff=2).
    let rig = rig_with_bounce_protocol();
    rig.kube.hang_delete_pods();
    rig.advance(Duration::from_secs(35));

    // Tick 1: fires, hangs, times out -> failure -> backoff=1.
    tokio::time::timeout(Duration::from_secs(120), rig.tick_health())
        .await
        .expect("tick 1 hung")
        .unwrap();
    let calls_after_1 = delete_pods_count(&rig);
    assert_eq!(calls_after_1, 1, "first tick fires the action");
    assert_eq!(rig.state.health.lock().await.backoff_failures(PROJECT, "bounce-on-zero"), 1);

    // Tick 2 immediately (no clock advance): inside the backoff
    // window -> skipped, no new action.
    rig.tick_health().await.unwrap();
    assert_eq!(
        delete_pods_count(&rig),
        1,
        "tick inside backoff window must NOT re-fire"
    );

    // Advance past the first backoff window (5s). Tick 3 re-fires.
    rig.advance(Duration::from_secs(6));
    tokio::time::timeout(Duration::from_secs(120), rig.tick_health())
        .await
        .expect("tick 3 hung")
        .unwrap();
    assert_eq!(delete_pods_count(&rig), 2, "tick past backoff window re-fires");
    assert_eq!(
        rig.state.health.lock().await.backoff_failures(PROJECT, "bounce-on-zero"),
        2,
        "second failure grows the backoff count"
    );
}

fn delete_pods_count(rig: &SupervisorTestRig) -> usize {
    rig.kube
        .calls()
        .iter()
        .filter(|c| matches!(c, KubeCall::DeletePods { .. }))
        .count()
}

#[tokio::test]
async fn completed_action_frees_in_flight() {
    // The success side, through the FULL tick: the action does NOT
    // hang, the timeout wrapper is transparent, the action runs
    // (delete_pods called) and the in_flight slot frees. Together
    // with `hung_action_times_out_frees_inflight_and_unlatches_fired`
    // this covers both arms of the timeout match in `tick_project`.
    let rig = rig_with_bounce_protocol();
    rig.advance(Duration::from_secs(35));
    rig.tick_health().await.unwrap();

    let in_flight = {
        let reg = rig.state.health.lock().await;
        reg.is_in_flight(PROJECT)
    };
    assert!(!in_flight, "completed action must free the in_flight slot");
    let saw_delete_pods = rig
        .kube
        .calls()
        .iter()
        .any(|c| matches!(c, KubeCall::DeletePods { .. }));
    assert!(saw_delete_pods, "the action should have run");
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
    // cluster is broken.
    let rig = rig();
    rig.broker.add_infra_node(PROJECT, NODE, "inst1", Status::Running);
    rig.kube
        .set_workloads(NAMESPACE, vec![workload("inst1-bridge", NODE, 1, 1)]);
    // Tick 1: healthy baseline.
    rig.tick_health().await.unwrap();

    // Replicas degrade and stay degraded.
    rig.kube
        .set_workloads(NAMESPACE, vec![workload("inst1-bridge", NODE, 1, 0)]);

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
    rig.kube
        .set_workloads(NAMESPACE, vec![workload("inst1-bridge", NODE, 1, 0)]);
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
    rig.kube
        .set_workloads(NAMESPACE, vec![workload("inst1-bridge", NODE, 1, 1)]);
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
    rig.broker.add_project_with_status(
        PROJ_ACTIVE,
        "wft-project-test-active",
        weft_broker_client::protocol::ProjectStatus::Active,
    );
    rig.broker.add_infra_node(PROJ_ACTIVE, NODE, "inst1", Status::Running);
    rig.kube.set_workloads("wft-project-test-active", vec![workload("inst1-bridge", NODE, 1, 1)]);
    rig.tick_health().await.unwrap();
    rig.kube.set_workloads("wft-project-test-active", vec![workload("inst1-bridge", NODE, 1, 0)]);
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
/// Running) and its workload has not reached the watch yet. Unknown is
/// not broken: however long that lasts, nothing parks and nothing is
/// called flaky. A member's copy that does break parks only that copy.
#[tokio::test]
async fn a_copy_the_watch_has_not_seen_never_parks_and_a_broken_one_parks_alone() {
    let rig = rig();
    let ada = weft_core::member::MemberId::new("ada").unwrap();
    rig.broker.add_infra_node(PROJECT, "shared", "inst-shared", Status::Running);
    rig.broker.add_member_infra_node(PROJECT, NODE, &ada, "inst1", Status::Running);
    let mut shared = workload_with_unit("inst-shared-shared", "shared", "shared", 1, 1);
    shared.labels.insert("weft.dev/instance".into(), "inst-shared".into());
    rig.kube.set_workloads(NAMESPACE, vec![shared.clone()]);
    for _ in 0..4 {
        rig.advance(Duration::from_secs(60));
        rig.tick_health().await.unwrap();
    }
    assert!(take_downs(&rig, PROJECT).is_empty(), "an unseen copy is unknown, never broken");
    assert!(!rig.broker.events().iter().any(|(_, _, _, k, _)| k == "flaky"));

    // Ada's copy shows up ready, then breaks for the whole window.
    rig.kube.set_workloads(NAMESPACE, vec![shared.clone(), workload("inst1-bridge", NODE, 1, 1)]);
    rig.tick_health().await.unwrap();
    rig.kube.set_workloads(NAMESPACE, vec![shared, workload("inst1-bridge", NODE, 1, 0)]);
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
    rig.broker.add_project_with_status(
        PROJ_INACTIVE,
        "wft-project-test-inactive",
        weft_broker_client::protocol::ProjectStatus::Inactive,
    );
    // The health loop parked it: auto-recover is allowed to undo it.
    rig.broker.set_health_parked(PROJ_INACTIVE, true);
    rig.broker.add_infra_node(PROJ_INACTIVE, NODE, "inst1", Status::Running);
    rig.kube.set_workloads(
        "wft-project-test-inactive",
        vec![workload("inst1-bridge", NODE, 1, 1)],
    );

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
    // on a running+active project. The infra pods are still up for a
    // moment (or being brought back up by a later Start). A health tick
    // that lands then must NOT auto-reactivate, because the USER
    // deactivated (nothing is `health_parked`), not the health loop.
    // Before the gate, this raced into a reactivate and left the user
    // with active triggers but no/just-stopped infra.
    let rig = rig();
    rig.broker.add_project_with_status(
        PROJ_USER_OFF,
        "wft-project-test-user-off",
        weft_broker_client::protocol::ProjectStatus::Inactive,
    );
    // The USER deactivated: nothing is health-parked (default).
    rig.broker.add_infra_node(PROJ_USER_OFF, NODE, "inst1", Status::Running);
    rig.kube.set_workloads(
        "wft-project-test-user-off",
        vec![workload("inst1-bridge", NODE, 1, 1)], // pods still healthy
    );

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
    rig.broker.add_project_with_status(
        ACTIVE_HEALTHY,
        "wft-active-healthy",
        weft_broker_client::protocol::ProjectStatus::Active,
    );
    rig.broker.add_infra_node(ACTIVE_HEALTHY, NODE, "inst1", Status::Running);
    rig.kube.set_workloads(
        "wft-active-healthy",
        vec![workload("inst1-bridge", NODE, 1, 1)],
    );
    rig.tick_health().await.unwrap();

    rig.broker.add_project_with_status(
        INACTIVE_BROKEN,
        "wft-inactive-broken",
        weft_broker_client::protocol::ProjectStatus::Inactive,
    );
    rig.broker.add_infra_node(INACTIVE_BROKEN, NODE, "inst1", Status::Running);
    rig.kube.set_workloads(
        "wft-inactive-broken",
        vec![workload("inst1-bridge", NODE, 1, 1)],
    );
    rig.tick_health().await.unwrap();
    rig.kube.set_workloads(
        "wft-inactive-broken",
        vec![workload("inst1-bridge", NODE, 1, 0)],
    );
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
// ---------- autonomous 3-stage recovery: deactivate -> bounce -> reactivate ----------

#[tokio::test]
async fn three_stage_recovery_deactivate_bounce_reactivate() {
    // A node author wires an autonomous flaky-recovery protocol set:
    //   1. flaky + Active   -> ParkTriggers   (deactivate the triggers)
    //   2. flaky + Inactive -> BouncePods     (restart the wedged unit)
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
                    { "kind": "node_ready_replicas", "node_id": NODE, "unit": "bridge", "op": "eq", "value": 0 },
                    { "kind": "project_status_eq", "status": "active" }
                ]},
                "action": { "kind": "park_triggers" },
                "timeout_seconds": 5
            },
            {
                "name": "bounce",
                "when": { "kind": "all", "conds": [
                    { "kind": "node_ready_replicas", "node_id": NODE, "unit": "bridge", "op": "eq", "value": 0 },
                    { "kind": "project_status_eq", "status": "inactive" }
                ]},
                "action": { "kind": "bounce_pods", "node_id": NODE, "unit": "bridge" },
                "timeout_seconds": 5
            },
            {
                "name": "recover",
                "when": { "kind": "all", "conds": [
                    { "kind": "not", "cond": [
                        { "kind": "node_ready_replicas", "node_id": NODE, "unit": "bridge", "op": "eq", "value": 0 }
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
    rig.kube.set_workloads(
        NAMESPACE,
        vec![workload_with_unit("inst1-bridge", NODE, "bridge", 1, 0)],
    );

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
    rig.broker.add_project_with_status(
        PROJECT,
        NAMESPACE,
        weft_broker_client::protocol::ProjectStatus::Inactive,
    );

    // --- Stage 2: flaky + Inactive -> BouncePods (delete_pods) ---
    rig.advance(Duration::from_secs(35));
    rig.tick_health().await.unwrap();
    assert!(
        rig.kube.calls().iter().any(|c| matches!(c, KubeCall::DeletePods { .. })),
        "stage 2 must bounce pods (delete_pods)"
    );

    // The bounce worked: the unit is ready again.
    rig.kube.set_workloads(
        NAMESPACE,
        vec![workload_with_unit("inst1-bridge", NODE, "bridge", 1, 1)],
    );

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

// ---------- watch-driven looks ----------

/// A replica drop the watch hands over is evaluated when it arrives, not
/// at the next tick: once the flaky window has passed, the change alone
/// fires the same flaky event the tick would have.
#[tokio::test]
async fn a_change_the_watch_hands_over_is_evaluated_without_a_tick() {
    let rig = rig();
    rig.broker.add_infra_node(PROJECT, NODE, "inst1", Status::Running);
    rig.kube.set_workloads(NAMESPACE, vec![workload("inst1-bridge", NODE, 1, 1)]);
    rig.tick_health().await.unwrap();

    // The replica drops: evaluated on the change, the window starts.
    rig.kube.set_workloads(NAMESPACE, vec![workload("inst1-bridge", NODE, 1, 0)]);
    rig.health_change().await;
    assert!(rig.broker.events().is_empty(), "still inside the flaky window");

    // Still down past the window: the next change (any change to the
    // project's workloads) fires flaky, with no tick in between.
    rig.advance(Duration::from_secs(35));
    rig.kube.set_workloads(NAMESPACE, vec![workload("inst1-bridge", NODE, 2, 0)]);
    rig.health_change().await;
    let flaky: Vec<_> = rig.broker.events().into_iter().filter(|(_, _, _, k, _)| k == "flaky").collect();
    assert_eq!(flaky.len(), 1);
    assert_eq!(rig.broker.infra_node(PROJECT, NODE).unwrap().status, Status::Flaky);
    // No list: the watch is the only read of the cluster.
    assert!(!rig.kube.calls().iter().any(|c| matches!(c, KubeCall::ListReplicaState { .. })));
}

/// A project stops being watched when this pod stops owning it, and one
/// it starts owning is watched from the next tick.
#[tokio::test]
async fn watches_follow_what_the_pod_owns() {
    let rig = rig();
    rig.broker.add_infra_node(PROJECT, NODE, "inst1", Status::Running);
    rig.tick_health().await.unwrap();
    assert_eq!(rig.kube.live_watches(), 1);
    let watched = rig
        .kube
        .calls()
        .into_iter()
        .filter(|c| matches!(c, KubeCall::WatchReplicaState { namespace, .. } if namespace == NAMESPACE))
        .count();
    assert_eq!(watched, 1);

    // Ticks keep the one watch.
    rig.tick_health().await.unwrap();
    assert_eq!(rig.kube.live_watches(), 1);

    rig.broker.set_project_owned(PROJECT, false);
    rig.tick_health().await.unwrap();
    assert_eq!(rig.kube.live_watches(), 0, "a released project's watch is dropped");

    rig.broker.set_project_owned(PROJECT, true);
    rig.tick_health().await.unwrap();
    assert_eq!(rig.kube.live_watches(), 1);
}

/// A lost project is dropped the moment the ownership loop says so, not
/// on the next tick: its watch stops and a change to its workloads fires
/// nothing while another pod owns it. A claim starts the watch at once.
#[tokio::test(start_paused = true)]
async fn ownership_changes_reach_the_health_loop_between_ticks() {
    use weft_infra_supervisor::ownership::OwnershipChange;
    let rig = rig_with_bounce_protocol();
    rig.kube.set_workloads(NAMESPACE, vec![workload_with_unit("inst1-bridge", NODE, "bridge", 1, 1)]);
    rig.tick_health().await.unwrap();
    assert_eq!(rig.kube.live_watches(), 1);

    let (ownership, mut changes) = tokio::sync::mpsc::unbounded_channel();
    rig.broker.set_project_owned(PROJECT, false);
    ownership.send(OwnershipChange { claimed: vec![], lost: vec![PROJECT] }).unwrap();
    rig.health_between_ticks(&mut changes, tokio::time::sleep(Duration::from_secs(1))).await.unwrap();
    assert_eq!(rig.kube.live_watches(), 0, "the lost project's watch is dropped before the tick");

    // The workloads break while another pod owns the project: nothing
    // here looks at them, so no action fires.
    rig.kube.set_workloads(NAMESPACE, vec![workload_with_unit("inst1-bridge", NODE, "bridge", 1, 0)]);
    rig.advance(Duration::from_secs(35));
    rig.health_between_ticks(&mut changes, tokio::time::sleep(Duration::from_secs(1))).await.unwrap();
    assert_eq!(delete_pods_count(&rig), 0, "a project this pod lost is never acted on");

    rig.broker.set_project_owned(PROJECT, true);
    ownership.send(OwnershipChange { claimed: vec![PROJECT], lost: vec![] }).unwrap();
    rig.health_between_ticks(&mut changes, tokio::time::sleep(Duration::from_secs(1))).await.unwrap();
    assert_eq!(rig.kube.live_watches(), 1, "a claimed project is watched without waiting for a tick");

    drop(ownership);
    assert!(
        rig.health_between_ticks(&mut changes, std::future::pending()).await.is_err(),
        "with the ownership loop gone the health loop stops"
    );
}

/// A change arriving just before the tick is due fires an action that
/// runs past the tick: the tick waits for it, it is never cut halfway,
/// so the project's in-flight slot is always given back.
#[tokio::test(start_paused = true)]
async fn a_tick_due_mid_action_does_not_cut_the_action() {
    let rig = rig_with_bounce_protocol();
    rig.kube.set_workloads(NAMESPACE, vec![workload_with_unit("inst1-bridge", NODE, "bridge", 1, 1)]);
    rig.tick_health().await.unwrap();
    rig.kube.hang_delete_pods();

    // The change fires the action, which hangs until its 5s timeout;
    // the tick is due after 1s.
    rig.kube.set_workloads(NAMESPACE, vec![workload_with_unit("inst1-bridge", NODE, "bridge", 1, 0)]);
    let (_ownership, mut changes) = tokio::sync::mpsc::unbounded_channel();
    tokio::time::timeout(
        Duration::from_secs(120),
        rig.health_between_ticks(&mut changes, tokio::time::sleep(Duration::from_secs(1))),
    )
    .await
    .expect("the between-ticks half never gave way to the tick")
    .unwrap();

    assert_eq!(delete_pods_count(&rig), 1, "the change fired the action");
    let reg = rig.state.health.lock().await;
    assert!(!reg.is_in_flight(PROJECT), "the action ran to its end and freed in_flight");
    assert_eq!(reg.backoff_failures(PROJECT, "bounce-on-zero"), 1, "the action reached its timeout");
}

/// A project whose watch never answers is not waited on: the tick still
/// evaluates every other project.
#[tokio::test]
async fn a_watch_that_never_answers_does_not_block_other_projects() {
    const SILENT: uuid::Uuid = uuid::Uuid::from_u128(2);
    const SILENT_NS: &str = "wft-project-test-silent";
    let rig = rig();
    rig.broker.add_project(SILENT, SILENT_NS);
    rig.kube.leave_watches_unanswered(SILENT_NS);
    rig.broker.add_infra_node(PROJECT, NODE, "inst1", Status::Running);
    for ready in [1, 0] {
        rig.kube.set_workloads(NAMESPACE, vec![workload("inst1-bridge", NODE, 1, ready)]);
        tokio::time::timeout(Duration::from_secs(5), rig.tick_health())
            .await
            .expect("the tick waited on the silent watch")
            .unwrap();
    }
    rig.advance(Duration::from_secs(35));
    tokio::time::timeout(Duration::from_secs(5), rig.tick_health())
        .await
        .expect("the tick waited on the silent watch")
        .unwrap();

    let flaky = rig.broker.events().into_iter().filter(|(_, _, _, k, _)| k == "flaky").count();
    assert_eq!(flaky, 1, "the answering project was evaluated");
    assert_eq!(rig.kube.live_watches(), 2, "the silent watch is kept, not restarted");
}

/// A watch whose look failed is not evaluated on the set it handed out
/// before: that set may say healthy about a project that is not. Its
/// next answer makes it evaluated again.
#[tokio::test]
async fn a_failing_watch_is_not_evaluated() {
    let rig = rig();
    rig.broker.add_infra_node(PROJECT, NODE, "inst1", Status::Running);
    rig.kube.set_workloads(NAMESPACE, vec![workload("inst1-bridge", NODE, 1, 1)]);
    rig.tick_health().await.unwrap();
    rig.kube.set_workloads(NAMESPACE, vec![workload("inst1-bridge", NODE, 1, 0)]);
    rig.tick_health().await.unwrap(); // the flaky window starts

    rig.kube.fail_watches(NAMESPACE, "the API server went away");
    rig.advance(Duration::from_secs(35));
    rig.tick_health().await.unwrap();
    assert!(rig.broker.events().is_empty(), "a failing watch was evaluated on its stale set");

    rig.kube.set_workloads(NAMESPACE, vec![workload("inst1-bridge", NODE, 1, 0)]);
    rig.tick_health().await.unwrap();
    let flaky = rig.broker.events().into_iter().filter(|(_, _, _, k, _)| k == "flaky").count();
    assert_eq!(flaky, 1, "the watch answered again and is evaluated");
}

// ---------- protocol Scale: scale first, then record ----------

/// A copy whose `bridge` workload has no ready replica, with a protocol
/// that scales `unit` of it to zero.
fn rig_with_scale_protocol(unit: &str) -> SupervisorTestRig {
    let rig = rig();
    rig.broker.add_infra_node(PROJECT, NODE, "inst1", Status::Running);
    rig.broker.set_health_protocols(
        PROJECT,
        serde_json::json!({
            "protocols": [{
                "name": "scale-down",
                "when": { "kind": "node_ready_replicas", "node_id": NODE, "op": "eq", "value": 0 },
                "action": { "kind": "scale", "node_id": NODE, "unit": unit, "replicas": 0 },
                "timeout_seconds": 5
            }]
        }),
    );
    rig.kube.set_workloads(
        NAMESPACE,
        vec![
            workload_with_unit("inst1-bridge", NODE, "bridge", 1, 0),
            workload_with_unit("inst1-other", NODE, "other", 1, 1),
        ],
    );
    rig.advance(Duration::from_secs(35));
    rig
}

/// A scale whose record the broker refuses (here: the unit is not in the
/// copy's roster) fails the action, so the protocol is not latched as
/// acted and retries once its backoff passes.
#[tokio::test]
async fn a_refused_scale_record_releases_the_latch() {
    let rig = rig_with_scale_protocol("other");
    rig.tick_health().await.unwrap();
    assert!(!rig.kube.scale_calls().is_empty(), "the scale reached the cluster");
    let reg = rig.state.health.lock().await;
    assert!(!reg.is_fired(PROJECT, "scale-down"), "an unrecorded scale must not latch");
    assert_eq!(reg.backoff_failures(PROJECT, "scale-down"), 1);
}

/// A scale the cluster refuses records nothing: the row never claims a
/// zero the workload does not have, and the protocol retries.
#[tokio::test]
async fn a_failed_scale_records_nothing() {
    let rig = rig_with_scale_protocol("bridge");
    rig.kube.fail_next_scale();
    rig.tick_health().await.unwrap();
    assert!(!rig.broker.calls().iter().any(|c| matches!(c, BrokerCall::SetScaled { .. })));
    let node = rig.broker.infra_node(PROJECT, NODE).unwrap();
    assert_eq!(node.units["bridge"].scaled_to, None);
    assert!(!rig.state.health.lock().await.is_fired(PROJECT, "scale-down"));

    // Past the backoff the retry scales and records.
    rig.advance(Duration::from_secs(6));
    rig.tick_health().await.unwrap();
    let node = rig.broker.infra_node(PROJECT, NODE).unwrap();
    assert_eq!(node.units["bridge"].scaled_to, Some(0));
    assert!(rig.state.health.lock().await.is_fired(PROJECT, "scale-down"));
}
