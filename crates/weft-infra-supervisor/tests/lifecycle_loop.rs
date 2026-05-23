//! Layer-3 integration tests for the supervisor's lifecycle loop.
//!
//! Exercises the full `lifecycle::tick` orchestration against
//! in-memory fakes. Covers stop / terminate / apply verbs and the
//! `running_policy` drain wait.

use std::collections::HashMap;

use weft_broker_client::protocol::{
    InfraLifecycleVerb as Verb, InfraNodeStatus as Status, RunningPolicy as Policy,
    SupervisorCommandRow,
};
use weft_infra_supervisor::testing::SupervisorTestRig;
use weft_platform_traits::kube::{WorkloadKind, WorkloadReplicaState};

const TENANT: &str = "tenant-test";
const PROJECT: &str = "proj1";
const NAMESPACE: &str = "wm-project-test-proj1";
const NODE: &str = "bridge";

fn rig() -> SupervisorTestRig {
    let rig = SupervisorTestRig::with_tenant(TENANT);
    rig.broker.add_project(PROJECT, NAMESPACE);
    rig
}

/// Workload with an explicit `weft.dev/unit` label.
fn workload_with_unit(
    instance: &str,
    name: &str,
    unit: &str,
    desired: i64,
    ready: i64,
) -> WorkloadReplicaState {
    let mut labels = HashMap::new();
    labels.insert("weft.dev/instance".into(), instance.into());
    labels.insert("weft.dev/role".into(), "infra".into());
    labels.insert("weft.dev/unit".into(), unit.into());
    WorkloadReplicaState {
        kind: WorkloadKind::Deployment,
        name: name.into(),
        namespace: NAMESPACE.into(),
        desired,
        ready,
        labels,
    }
}

fn workload_for(instance: &str, name: &str, desired: i64, ready: i64) -> WorkloadReplicaState {
    let mut labels = HashMap::new();
    labels.insert("weft.dev/instance".into(), instance.into());
    labels.insert("weft.dev/role".into(), "infra".into());
    // Single-unit fixture: unit name = node id (matches add_infra_node).
    labels.insert("weft.dev/unit".into(), NODE.into());
    WorkloadReplicaState {
        kind: WorkloadKind::Deployment,
        name: name.into(),
        namespace: NAMESPACE.into(),
        desired,
        ready,
        labels,
    }
}

fn cmd(id: i64, verb: Verb, node: Option<&str>) -> SupervisorCommandRow {
    SupervisorCommandRow {
        id,
        project_id: PROJECT.into(),
        node_id: node.map(|s| s.to_string()),
        verb,
        running_policy: Some(Policy::Cancel),
        spec_json: None,
        force: false,
    }
}

// ---------- empty queue ----------

#[tokio::test]
async fn tick_with_no_pending_returns_false() {
    let rig = rig();
    let did_work = rig.tick_lifecycle().await.unwrap();
    assert!(!did_work);
}

// ---------- stop verb ----------

#[tokio::test]
async fn stop_flips_status_then_scales_then_emits_stopped() {
    let rig = rig();
    rig.broker.add_infra_node(PROJECT, NODE, "inst1", Status::Running);
    rig.kube.set_workloads(
        NAMESPACE,
        vec![workload_for("inst1", "inst1-bridge", 1, 1)],
    );
    rig.broker.enqueue_command(cmd(1, Verb::Stop, Some(NODE)));

    let did_work = rig.tick_lifecycle().await.unwrap();
    assert!(did_work);

    // Status writes should be (stopping, stopped) in order.
    let writes = rig.broker.status_writes();
    assert_eq!(
        writes
            .iter()
            .filter(|(_, n, _)| n == NODE)
            .map(|(_, _, s)| *s)
            .collect::<Vec<_>>(),
        vec![Status::Stopping, Status::Stopped]
    );

    // The workload was scaled to 0 via the typed `scale_workload`.
    let scales = rig.kube.scale_calls();
    assert_eq!(
        scales,
        vec![(
            NAMESPACE.into(),
            WorkloadKind::Deployment,
            "inst1-bridge".into(),
            0
        )]
    );

    // A `stopped` event was emitted.
    let events = rig.broker.events();
    assert!(events.iter().any(|(_, n, k, _)| {
        n.as_deref() == Some(NODE) && k == "stopped"
    }));

    // Command was marked complete with no error.
    assert_eq!(rig.broker.completed_commands(), vec![(1, None)]);
}

#[tokio::test]
async fn stop_skips_noop_units_and_scales_only_scale_to_zero() {
    use weft_broker_client::protocol::UnitRuntime;
    let rig = rig();
    // Two-unit node: `web` scales to zero on stop, `license` is NoOp
    // (survives stop, only terminate removes it).
    let mut units = std::collections::BTreeMap::new();
    units.insert(
        "web".to_string(),
        UnitRuntime {
            status: Status::Running,
            stop_behavior: weft_core::StopBehavior::ScaleToZero,
            flaky_after_seconds: 30,
            recovery_after_seconds: 30,
        },
    );
    units.insert(
        "license".to_string(),
        UnitRuntime {
            status: Status::Running,
            stop_behavior: weft_core::StopBehavior::NoOp,
            flaky_after_seconds: 30,
            recovery_after_seconds: 30,
        },
    );
    rig.broker.add_infra_node_with(
        PROJECT,
        NODE,
        "inst1",
        Status::Running,
        None,
        std::collections::BTreeMap::new(),
        units,
    );
    rig.kube.set_workloads(
        NAMESPACE,
        vec![
            workload_with_unit("inst1", "inst1-web", "web", 1, 1),
            workload_with_unit("inst1", "inst1-license", "license", 1, 1),
        ],
    );
    rig.broker.enqueue_command(cmd(1, Verb::Stop, Some(NODE)));

    rig.tick_lifecycle().await.unwrap();

    // ONLY the web workload was scaled to 0; license is untouched.
    let scales = rig.kube.scale_calls();
    assert_eq!(
        scales,
        vec![(NAMESPACE.into(), WorkloadKind::Deployment, "inst1-web".into(), 0)],
        "only the ScaleToZero unit is scaled; the NoOp unit survives"
    );

    // Per-unit status: web stopped, license still running. Node rolls
    // up to flaky? No: rollup of {stopped, running} = running (running
    // outranks stopped), so the node still looks running because a unit
    // is up. The web unit is individually Stopped.
    let row = rig.broker.infra_node(PROJECT, NODE).unwrap();
    assert_eq!(row.units.get("web").unwrap().status, Status::Stopped);
    assert_eq!(row.units.get("license").unwrap().status, Status::Running);
}

#[tokio::test]
async fn force_stop_takes_down_noop_units_too() {
    use weft_broker_client::protocol::UnitRuntime;
    let rig = rig();
    // Same two-unit node, but force=true must scale BOTH down,
    // ignoring the license unit's NoOp.
    let mut units = std::collections::BTreeMap::new();
    for (name, sb) in [
        ("web", weft_core::StopBehavior::ScaleToZero),
        ("license", weft_core::StopBehavior::NoOp),
    ] {
        units.insert(
            name.to_string(),
            UnitRuntime {
                status: Status::Running,
                stop_behavior: sb,
                flaky_after_seconds: 30,
                recovery_after_seconds: 30,
            },
        );
    }
    rig.broker.add_infra_node_with(
        PROJECT,
        NODE,
        "inst1",
        Status::Running,
        None,
        std::collections::BTreeMap::new(),
        units,
    );
    rig.kube.set_workloads(
        NAMESPACE,
        vec![
            workload_with_unit("inst1", "inst1-web", "web", 1, 1),
            workload_with_unit("inst1", "inst1-license", "license", 1, 1),
        ],
    );
    // force = true on the Stop command.
    let mut command = cmd(1, Verb::Stop, Some(NODE));
    command.force = true;
    rig.broker.enqueue_command(command);

    rig.tick_lifecycle().await.unwrap();

    // BOTH workloads scaled to 0 (force overrode the NoOp).
    let mut scaled: Vec<String> = rig
        .kube
        .scale_calls()
        .into_iter()
        .map(|(_, _, name, _)| name)
        .collect();
    scaled.sort();
    assert_eq!(scaled, vec!["inst1-license".to_string(), "inst1-web".to_string()]);

    // Both units Stopped; the node rolls up to Stopped (all down).
    let row = rig.broker.infra_node(PROJECT, NODE).unwrap();
    assert_eq!(row.units.get("web").unwrap().status, Status::Stopped);
    assert_eq!(row.units.get("license").unwrap().status, Status::Stopped);
    assert_eq!(row.status, Status::Stopped);
}

// ---------- terminate verb ----------

#[tokio::test]
async fn terminate_flips_status_deletes_then_removes_row() {
    let rig = rig();
    rig.broker.add_infra_node(PROJECT, NODE, "inst1", Status::Running);
    rig.kube.set_workloads(
        NAMESPACE,
        vec![workload_for("inst1", "inst1-bridge", 1, 1)],
    );
    rig.broker.enqueue_command(cmd(2, Verb::Terminate, Some(NODE)));

    rig.tick_lifecycle().await.unwrap();

    // Status flipped to terminating before delete.
    let writes = rig.broker.status_writes();
    assert!(writes.iter().any(|(_, n, s)| n == NODE && *s == Status::Terminating));

    // Delete-by-label call issued for this instance.
    let deletes = rig.kube.delete_calls();
    assert!(deletes
        .iter()
        .any(|(_, sel, _)| sel == "weft.dev/instance=inst1"));

    // Row was removed.
    assert!(rig.broker.infra_node(PROJECT, NODE).is_none());

    // A `terminated` event was emitted.
    let events = rig.broker.events();
    assert!(events.iter().any(|(_, _, k, _)| k == "terminated"));
}

// ---------- no matching rows (soft no-op) ----------

#[tokio::test]
async fn stop_with_no_matching_infra_node_completes_cleanly() {
    // Stop fired against a node that's already gone (e.g. user
    // clicks Stop twice). Should not error.
    let rig = rig();
    rig.broker.enqueue_command(cmd(3, Verb::Stop, Some("ghost-node")));

    let did_work = rig.tick_lifecycle().await.unwrap();
    assert!(did_work);
    assert_eq!(rig.broker.completed_commands(), vec![(3, None)]);
    // No scale issued.
    assert!(rig.kube.scale_calls().is_empty());
}

// ---------- dispatcher-owned verb landed on supervisor ----------

#[tokio::test]
async fn dispatcher_verb_at_supervisor_completes_with_error() {
    // Defensive: if the broker's claim filter ever drifted and let
    // a Deactivate row through to the supervisor, the supervisor
    // must complete it as a failure (rather than silently no-op).
    // The runtime check is the only path; the typed enum already
    // prevents the supervisor's CALL SITES from constructing one.
    let rig = rig();
    rig.broker.add_infra_node(PROJECT, NODE, "inst1", Status::Running);
    rig.broker.enqueue_command(cmd(4, Verb::Deactivate, Some(NODE)));

    rig.tick_lifecycle().await.unwrap();
    let failed = rig.broker.failed_commands();
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0].0, 4);
    assert!(failed[0].1.contains("supervisor claimed dispatcher-only verb"));
}

// ---------- drain wait ----------

#[tokio::test]
async fn running_policy_wait_drains_then_proceeds() {
    // Simulate "running_count > 0 then 0". The drain loop should
    // sleep until the count reaches zero (the fake clock returns
    // instantly, so no real wall-time elapses).
    let rig = rig();
    rig.broker.add_infra_node(PROJECT, NODE, "inst1", Status::Running);
    rig.kube
        .set_workloads(NAMESPACE, vec![workload_for("inst1", "inst1-bridge", 1, 1)]);

    let mut command = cmd(5, Verb::Stop, Some(NODE));
    command.running_policy = Some(Policy::Wait);
    rig.broker.enqueue_command(command);

    // Pretend there are 0 running executions from the start (the
    // simplest path). Drain returns immediately.
    rig.broker.set_running_count(PROJECT, 0);

    let did_work = rig.tick_lifecycle().await.unwrap();
    assert!(did_work);
    assert_eq!(rig.broker.completed_commands(), vec![(5, None)]);
}

#[tokio::test]
async fn running_policy_wait_times_out_after_deadline() {
    // running_count stays > 0 forever. The drain loop must give up
    // after the deadline (~600s of FakeClock-advanced time) and
    // proceed with the lifecycle op. Without a fake clock this
    // would take 10 minutes real time; with FakeClock it's
    // microseconds.
    let rig = rig();
    rig.broker.add_infra_node(PROJECT, NODE, "inst1", Status::Running);
    rig.kube
        .set_workloads(NAMESPACE, vec![workload_for("inst1", "inst1-bridge", 1, 1)]);
    rig.broker.set_running_count(PROJECT, 5);

    let mut command = cmd(6, Verb::Stop, Some(NODE));
    command.running_policy = Some(Policy::Wait);
    rig.broker.enqueue_command(command);

    let did_work = rig.tick_lifecycle().await.unwrap();
    assert!(did_work);
    // Stop should still complete (timeout proceeds).
    assert_eq!(rig.broker.completed_commands(), vec![(6, None)]);
    let writes = rig.broker.status_writes();
    assert!(writes.iter().any(|(_, _, s)| *s == Status::Stopped));
}

// ---------- apply verb ----------

/// Minimal one-unit spec_json the supervisor can deserialize +
/// compile. One Unit with one upstream-image container; no
/// endpoints/volumes so readiness sees zero workloads in the fake
/// (empty == ready).
fn apply_cmd(id: i64) -> SupervisorCommandRow {
    let spec = serde_json::json!({
        "units": [{
            "name": "bridge",
            "kind": "deployment",
            "containers": [{
                "name": "c",
                "image": { "kind": "upstream", "reference": "nginx:1" }
            }]
        }]
    });
    SupervisorCommandRow {
        id,
        project_id: PROJECT.into(),
        node_id: Some(NODE.into()),
        verb: Verb::Apply,
        running_policy: None,
        spec_json: Some(spec),
        force: false,
    }
}

/// Pull the BrokerCall variant names in order, for ordering asserts.
fn call_names(rig: &SupervisorTestRig) -> Vec<&'static str> {
    use weft_infra_supervisor::broker_ops::BrokerCall;
    rig.broker
        .calls()
        .iter()
        .map(|c| match c {
            BrokerCall::SetProvisioning { .. } => "set_provisioning",
            BrokerCall::SetApplied { .. } => "set_applied",
            BrokerCall::SetStatus { .. } => "set_status",
            _ => "other",
        })
        .collect()
}

#[tokio::test]
async fn apply_writes_provisioning_before_kube_apply_then_applied() {
    let rig = rig();
    // Fresh apply: no prior infra_node row.
    rig.broker.enqueue_command(apply_cmd(1));

    let did_work = rig.tick_lifecycle().await.unwrap();
    assert!(did_work);

    // set_provisioning must precede set_applied in the broker call
    // log (the whole point of B5: a row exists at Provisioning
    // before any kube mutation).
    let names = call_names(&rig);
    let prov = names.iter().position(|n| *n == "set_provisioning");
    let applied = names.iter().position(|n| *n == "set_applied");
    assert!(prov.is_some(), "set_provisioning must be called; calls={names:?}");
    assert!(applied.is_some(), "set_applied must be called; calls={names:?}");
    assert!(prov < applied, "provisioning before applied; calls={names:?}");

    // And the kube apply happened (recorded in the FakeKube log).
    use weft_platform_traits::kube::KubeCall;
    assert!(
        rig.kube.calls().iter().any(|c| matches!(c, KubeCall::ApplyYaml { .. } | KubeCall::Apply { .. })),
        "expected a kube apply call"
    );

    // Command completed with no error.
    assert_eq!(rig.broker.completed_commands(), vec![(1, None)]);
}

#[tokio::test]
async fn apply_failure_flips_to_failed_and_bubbles() {
    let rig = rig();
    // Make the kube apply fail so execute_apply hits the
    // Failed-status branch.
    rig.kube.fail_next_apply();
    rig.broker.enqueue_command(apply_cmd(2));

    let did_work = rig.tick_lifecycle().await.unwrap();
    assert!(did_work);

    // The row was flipped to Failed (set_status with FailureStage).
    let writes = rig.broker.status_writes();
    assert!(
        writes.iter().any(|(_, n, s)| n == NODE && *s == Status::Failed),
        "expected a Failed status write; got {writes:?}"
    );
    // Command completed WITH an error (apply bubbled).
    let completed = rig.broker.completed_commands();
    assert_eq!(completed.len(), 1);
    assert_eq!(completed[0].0, 2);
    assert!(completed[0].1.is_some(), "apply failure must record an error");
}

#[tokio::test]
async fn apply_skip_path_no_provisioning() {
    let rig = rig();
    // Prior row at Running with a matching applied_spec_hash means
    // the Skip path: no set_provisioning, no kube apply.
    let spec = serde_json::json!({
        "units": [{
            "name": "bridge",
            "kind": "deployment",
            "containers": [{
                "name": "c",
                "image": { "kind": "upstream", "reference": "nginx:1" }
            }]
        }]
    });
    let parsed: weft_core::infra::InfraSpec = serde_json::from_value(spec.clone()).unwrap();
    let hash = weft_core::infra::hash_spec(&parsed, &std::collections::BTreeMap::new()).unwrap();
    rig.broker.add_infra_node_with(
        PROJECT,
        NODE,
        "inst1",
        Status::Running,
        Some(hash),
        std::collections::BTreeMap::new(),
        {
            let mut m = std::collections::BTreeMap::new();
            m.insert(
                NODE.to_string(),
                weft_broker_client::protocol::UnitRuntime {
                    status: Status::Running,
                    stop_behavior: weft_core::StopBehavior::ScaleToZero,
                    flaky_after_seconds: 30,
                    recovery_after_seconds: 30,
                },
            );
            m
        },
    );

    let mut command = apply_cmd(3);
    command.spec_json = Some(spec);
    rig.broker.enqueue_command(command);

    let did_work = rig.tick_lifecycle().await.unwrap();
    assert!(did_work);

    let names = call_names(&rig);
    assert!(
        !names.contains(&"set_provisioning"),
        "skip path must not provision; calls={names:?}"
    );
    use weft_platform_traits::kube::KubeCall;
    assert!(
        !rig.kube.calls().iter().any(|c| matches!(c, KubeCall::ApplyYaml { .. } | KubeCall::Apply { .. })),
        "skip path must not kube-apply"
    );
}

#[tokio::test]
async fn apply_reconciles_down_unit_and_skips_up_unit() {
    use weft_broker_client::protocol::UnitRuntime;
    let rig = rig();
    // Two-unit spec: `web` + `license`.
    let spec = serde_json::json!({
        "units": [
            { "name": "web", "kind": "deployment",
              "containers": [{ "name": "c", "image": { "kind": "upstream", "reference": "nginx:1" } }] },
            { "name": "license", "kind": "deployment",
              "containers": [{ "name": "c", "image": { "kind": "upstream", "reference": "nginx:1" } }] }
        ]
    });
    // Prior row: `license` is UP (Running, frozen), `web` is DOWN
    // (Stopped). Apply must reconcile only `web`.
    let mut prior_units = std::collections::BTreeMap::new();
    prior_units.insert("web".to_string(), UnitRuntime {
        status: Status::Stopped,
        stop_behavior: weft_core::StopBehavior::ScaleToZero,
        flaky_after_seconds: 30,
        recovery_after_seconds: 30,
    });
    prior_units.insert("license".to_string(), UnitRuntime {
        status: Status::Running,
        stop_behavior: weft_core::StopBehavior::NoOp,
        flaky_after_seconds: 30,
        recovery_after_seconds: 30,
    });
    rig.broker.add_infra_node_with(
        PROJECT, NODE, "inst1", Status::Running, Some("oldhash".into()),
        std::collections::BTreeMap::new(), prior_units,
    );
    // license is up in the cluster; web is gone (scaled to 0).
    rig.kube.set_workloads(
        NAMESPACE,
        vec![workload_with_unit("inst1", "inst1-license", "license", 1, 1)],
    );

    let mut command = apply_cmd(4);
    command.spec_json = Some(spec);
    rig.broker.enqueue_command(command);

    rig.tick_lifecycle().await.unwrap();

    // It DID provision (web is down, hash differs, so not a full skip).
    let names = call_names(&rig);
    assert!(names.contains(&"set_provisioning"), "calls={names:?}");

    // The kube apply touched ONLY web's workload manifest, never
    // license's (license is up and frozen).
    use weft_platform_traits::kube::KubeCall;
    let applied_manifests: Vec<String> = rig
        .kube
        .calls()
        .into_iter()
        .filter_map(|c| match c {
            KubeCall::Apply { manifest } => manifest
                .get("metadata")
                .and_then(|m| m.get("labels"))
                .and_then(|l| l.get("weft.dev/unit"))
                .and_then(|u| u.as_str())
                .map(|s| s.to_string()),
            _ => None,
        })
        .collect();
    assert!(
        applied_manifests.iter().all(|u| u != "license"),
        "license (up, frozen) must NOT be re-applied; applied units={applied_manifests:?}"
    );
    assert!(
        applied_manifests.iter().any(|u| u == "web"),
        "web (down) must be reconciled; applied units={applied_manifests:?}"
    );

    // Final per-unit status: web back to Running, license still Running.
    let row = rig.broker.infra_node(PROJECT, NODE).unwrap();
    assert_eq!(row.units.get("web").unwrap().status, Status::Running);
    assert_eq!(row.units.get("license").unwrap().status, Status::Running);
}
