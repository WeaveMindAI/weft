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
        },
    );
    m
}

const TENANT: &str = "tenant-test";
const PROJECT: &str = "proj1";
const NAMESPACE: &str = "wm-project-test-proj1";
const NODE: &str = "bridge";

fn rig() -> SupervisorTestRig {
    let rig = SupervisorTestRig::with_tenant(TENANT);
    rig.broker.add_project(PROJECT, NAMESPACE);
    rig
}

fn workload(name: &str, node_id: &str, desired: i64, ready: i64) -> WorkloadReplicaState {
    workload_with_unit(name, node_id, "bridge", desired, ready)
}

fn workload_with_unit(
    name: &str,
    node_id: &str,
    unit: &str,
    desired: i64,
    ready: i64,
) -> WorkloadReplicaState {
    let mut labels = HashMap::new();
    labels.insert("weft.dev/node".into(), node_id.into());
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
    let kinds: Vec<&str> = events.iter().map(|(_, _, k, _)| k.as_str()).collect();
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
    rig.broker.set_infra_command_in_flight(PROJECT, true);

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
        calls.iter().any(|c| matches!(c, BrokerCall::InfraCommandInFlight { .. })),
        "the gate was consulted"
    );
    assert!(
        !calls.iter().any(|c| matches!(c, BrokerCall::SetStatus { .. })),
        "no autonomous status write while standing down"
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
    rig.broker.set_infra_command_in_flight(PROJECT, true);
    rig.advance(Duration::from_secs(60));
    rig.tick_health().await.unwrap();
    assert!(rig.broker.events().is_empty(), "gated: no events");

    // Action completes; node is back up and healthy. Health re-arms
    // and establishes a fresh ready baseline.
    rig.broker.set_infra_command_in_flight(PROJECT, false);
    rig.tick_health().await.unwrap();

    // Now the node genuinely degrades, post-action. Flaky must fire
    // after the window: proof the monitor is live again, not muted.
    rig.kube
        .set_workloads(NAMESPACE, vec![workload("inst1-bridge", NODE, 1, 0)]);
    rig.tick_health().await.unwrap(); // first not-ready observation
    rig.advance(Duration::from_secs(35)); // past the 30s flaky window
    rig.tick_health().await.unwrap();

    let kinds: Vec<String> = rig.broker.events().iter().map(|(_, _, k, _)| k.clone()).collect();
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
        rig.broker.events().iter().map(|(_, _, k, _)| k.clone()).collect();
    assert!(!event_kinds.contains(&"flaky".to_string()));

    // Advance past flaky window. Tick 3: emits flaky + sets status.
    // The default `auto-recover-on-zero-ready` protocol ALSO fires
    // here, enqueueing a `reactivate` lifecycle command via
    // `enqueue_lifecycle`. We filter for `flaky` event_record
    // entries specifically; protocol firing is observed via
    // `BrokerCall::EnqueueLifecycle` counts (see the
    // `fired_set_rearms_when_all_nodes_healthy` test).
    rig.advance(Duration::from_secs(35));
    rig.tick_health().await.unwrap();

    let events = rig.broker.events();
    let flaky_events: Vec<_> = events.iter().filter(|(_, _, k, _)| k == "flaky").collect();
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
        rig.broker.events().iter().map(|(_, _, k, _)| k.clone()).collect();
    assert!(!event_kinds.contains(&"recovered".to_string()));

    // Advance past recovery window. Tick 5: emits recovered.
    rig.advance(Duration::from_secs(35));
    rig.tick_health().await.unwrap();

    let events = rig.broker.events();
    let kinds: Vec<String> = events.iter().map(|(_, _, k, _)| k.clone()).collect();
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
        .filter(|(_, _, k, _)| k == "flaky")
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
        .set_status("test-pod", None, PROJECT, NODE, Some(NODE), Status::Stopped, None, None)
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
        .set_status("test-pod", None, PROJECT, NODE, Some(NODE), Status::Running, None, None)
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
    let flaky_count = events.iter().filter(|(_, _, k, _)| k == "flaky").count();
    assert_eq!(flaky_count, 0, "no flaky event should fire on clean start");
}

// ---------- fired-set re-arm ----------

#[tokio::test]
async fn fired_set_rearms_when_all_nodes_healthy() {
    // After a protocol fires (the default `AutoRecover` enqueues a
    // `reactivate` lifecycle command), the supervisor remembers it
    // in `fired_set` so it doesn't fire again on the same
    // degradation. When the project becomes fully healthy again
    // (all ratios >= 1.0), the set must re-arm so the next
    // degradation triggers the protocol.
    let rig = rig();
    rig.broker.add_infra_node(PROJECT, NODE, "inst1", Status::Running);

    // Degrade first: trigger fires (one EnqueueLifecycle).
    rig.kube
        .set_workloads(NAMESPACE, vec![workload("inst1-bridge", NODE, 1, 0)]);
    rig.advance(Duration::from_secs(35));
    rig.tick_health().await.unwrap();
    let enqueue_count_1 = rig
        .broker
        .calls()
        .iter()
        .filter(|c| matches!(c, BrokerCall::EnqueueLifecycle { .. }))
        .count();
    assert_eq!(enqueue_count_1, 1, "default protocol should fire once");

    // Recover.
    rig.kube
        .set_workloads(NAMESPACE, vec![workload("inst1-bridge", NODE, 1, 1)]);
    rig.advance(Duration::from_secs(35));
    rig.tick_health().await.unwrap();

    // Degrade again.
    rig.kube
        .set_workloads(NAMESPACE, vec![workload("inst1-bridge", NODE, 1, 0)]);
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
    // with `action_timeout_fires_over_hung_kube_call` this covers
    // both arms of the timeout match in `tick_project`.
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
    // because the tracker's `last_not_ready_since` persists.
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

    // Tick 3: replicas still degraded, tracker's
    // `last_not_ready_since` still set from before. Tick must
    // re-emit `flaky` and write Flaky status.
    rig.advance(Duration::from_secs(1));
    rig.tick_health().await.unwrap();
    assert_eq!(
        rig.broker.infra_node(PROJECT, NODE).unwrap().status,
        Status::Flaky,
        "next tick must re-observe degraded replicas and rewrite Flaky"
    );
}

// ---------- regression: two-stage default protocols ----------

#[tokio::test]
async fn default_protocol_parks_active_project_on_infra_broken() {
    // Round-6 reshape: default protocol pair is
    //   - park-while-infra-broken    (Active + zero ready → Park)
    //   - auto-recover-when-healthy  (Inactive + ready    → Reactivate)
    // Confirm stage 1: an Active project with broken infra enqueues
    // a dispatcher-targeted deactivate(park) command.
    let rig = rig();
    // Project starts Active.
    rig.broker.add_project_with_status(
        "proj-active",
        "wm-project-test-active",
        weft_broker_client::protocol::ProjectStatus::Active,
    );
    rig.broker.add_infra_node("proj-active", NODE, "inst1", Status::Running);
    rig.kube.set_workloads(
        "wm-project-test-active",
        vec![workload("inst1-bridge", NODE, 1, 0)],
    );
    rig.advance(Duration::from_secs(35));
    rig.tick_health().await.unwrap();

    let calls = rig.broker.calls();
    let enqueued = calls.iter().any(|c| matches!(
        c,
        BrokerCall::EnqueueLifecycle { project_id, spec }
            if project_id == "proj-active"
            && matches!(spec, weft_broker_client::protocol::LifecycleSpec::Deactivate(_))
    ));
    assert!(
        enqueued,
        "default protocol park-while-infra-broken must enqueue a deactivate when infra degrades on an Active project"
    );
}

#[tokio::test]
async fn default_protocol_auto_recovers_inactive_project_on_infra_healthy() {
    // Stage 2 of the two-stage protocol: an Inactive project with
    // every infra node ready triggers a reactivate, BUT ONLY if the
    // health loop is the one that deactivated it (`deactivated_by_health`).
    let rig = rig();
    rig.broker.add_project_with_status(
        "proj-inactive",
        "wm-project-test-inactive",
        weft_broker_client::protocol::ProjectStatus::Inactive,
    );
    // The health loop parked it: auto-recover is allowed to undo it.
    rig.broker.set_deactivated_by_health("proj-inactive", true);
    rig.broker.add_infra_node("proj-inactive", NODE, "inst1", Status::Running);
    rig.kube.set_workloads(
        "wm-project-test-inactive",
        vec![workload("inst1-bridge", NODE, 1, 1)],
    );

    // Tick: infra is healthy, project is inactive, health parked it → reactivate.
    rig.tick_health().await.unwrap();

    let calls = rig.broker.calls();
    let reactivated = calls.iter().any(|c| matches!(
        c,
        BrokerCall::EnqueueLifecycle { project_id, spec }
            if project_id == "proj-inactive"
            && matches!(spec, weft_broker_client::protocol::LifecycleSpec::Reactivate)
    ));
    assert!(
        reactivated,
        "default protocol auto-recover-when-infra-healthy must enqueue a reactivate when infra is up on an Inactive project"
    );
}

#[tokio::test]
async fn default_protocol_does_not_reactivate_user_deactivated_project() {
    // Regression: the user clicked Stop infra (or Deactivate / Upgrade)
    // on a running+active project. The deactivation flips the project
    // Inactive, but the infra pods are still up for a moment (or being
    // brought back up by a later Start). A health tick that lands while
    // (infra healthy + Inactive) must NOT auto-reactivate, because the
    // USER deactivated (`deactivated_by_health == false`), not the
    // health loop. Before the gate, this raced into a reactivate and
    // left the user with active triggers but no/just-stopped infra.
    let rig = rig();
    rig.broker.add_project_with_status(
        "proj-user-off",
        "wm-project-test-user-off",
        weft_broker_client::protocol::ProjectStatus::Inactive,
    );
    // The USER deactivated: the flag stays false (default).
    rig.broker.add_infra_node("proj-user-off", NODE, "inst1", Status::Running);
    rig.kube.set_workloads(
        "wm-project-test-user-off",
        vec![workload("inst1-bridge", NODE, 1, 1)], // pods still healthy
    );

    rig.tick_health().await.unwrap();

    let any_reactivate = rig.broker.calls().iter().any(|c| matches!(
        c,
        BrokerCall::EnqueueLifecycle { project_id, spec }
            if project_id == "proj-user-off"
            && matches!(spec, weft_broker_client::protocol::LifecycleSpec::Reactivate)
    ));
    assert!(
        !any_reactivate,
        "a USER-deactivated project (deactivated_by_health=false) must NOT be auto-reactivated, \
         even when infra is healthy + Inactive"
    );
}

#[tokio::test]
async fn default_protocol_does_not_fire_when_status_mismatches() {
    // Cross-check: an Active project with HEALTHY infra must NOT
    // enqueue a reactivate (the second-stage condition requires
    // Inactive). And an Inactive project with BROKEN infra must
    // NOT enqueue a park (the first-stage condition requires
    // Active). Confirms the ProjectStatusEq clause actually
    // gates each stage.
    let rig = rig();
    rig.broker.add_project_with_status(
        "active-healthy",
        "wm-active-healthy",
        weft_broker_client::protocol::ProjectStatus::Active,
    );
    rig.broker.add_infra_node("active-healthy", NODE, "inst1", Status::Running);
    rig.kube.set_workloads(
        "wm-active-healthy",
        vec![workload("inst1-bridge", NODE, 1, 1)],
    );
    rig.tick_health().await.unwrap();

    rig.broker.add_project_with_status(
        "inactive-broken",
        "wm-inactive-broken",
        weft_broker_client::protocol::ProjectStatus::Inactive,
    );
    rig.broker.add_infra_node("inactive-broken", NODE, "inst1", Status::Running);
    rig.kube.set_workloads(
        "wm-inactive-broken",
        vec![workload("inst1-bridge", NODE, 1, 0)],
    );
    rig.advance(Duration::from_secs(35));
    rig.tick_health().await.unwrap();

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
        matches!(last_lifecycle(&rig), Some(LifecycleSpec::Reactivate)),
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
        matches!(c, BrokerCall::EnqueueLifecycle { spec: LifecycleSpec::Reactivate, .. })
    }).count();
    assert_eq!(deactivates, 1, "exactly one deactivate across the cycle");
    assert_eq!(reactivates, 1, "exactly one reactivate across the cycle");
}
