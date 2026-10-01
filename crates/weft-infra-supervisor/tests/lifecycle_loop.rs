//! Layer-3 integration tests for the supervisor's lifecycle loop.
//!
//! Exercises the full `lifecycle::tick` orchestration against
//! in-memory fakes. Covers stop / terminate / apply verbs and the
//! `running_policy` drain wait.

use weft_broker_client::protocol::{
    InfraLifecycleVerb as Verb, InfraNodeStatus as Status, RunningPolicy as Policy,
    SupervisorCommandRow,
};
use weft_infra_supervisor::testing::SupervisorTestRig;
use weft_platform_traits::HostCall;

const TENANT: &str = "tenant-test";
const PROJECT: uuid::Uuid = uuid::Uuid::from_u128(1);
const NODE: &str = "bridge";

/// The id every apply gives the shared copy of NODE (derived, never minted),
/// for fixtures an apply works on in place.
fn shared_id() -> String {
    weft_core::infra::NodeRef::copy_id(PROJECT, NODE, None)
}

fn rig() -> SupervisorTestRig {
    let rig = SupervisorTestRig::with_tenant(TENANT);
    rig.broker.add_project(PROJECT);
    rig
}

/// The copy `copy_id` of this file's node, as the host knows it.
fn copy(copy_id: &str) -> weft_core::infra::NodeRef {
    weft_core::infra::NodeRef { tenant: TENANT.into(), project: PROJECT, node: NODE.into(), copy_id: copy_id.into() }
}

/// The hash an apply of `spec` for `copy_id` stamps.
fn applied_hash(spec: &serde_json::Value, copy_id: &str) -> String {
    let parsed: weft_core::infra::InfraSpec = serde_json::from_value(spec.clone()).unwrap();
    weft_core::infra::resolve(&parsed, &copy(copy_id), &Default::default()).unwrap().hash()
}

fn unit(status: Status, on_stop: weft_core::StopBehavior) -> weft_broker_client::protocol::UnitRuntime {
    weft_broker_client::protocol::UnitRuntime {
        status,
        stop_behavior: on_stop,
        flaky_after_seconds: 30,
        recovery_after_seconds: 30,
        image_refs: Default::default(),
    }
}

/// The units the host was asked to stop, as `copy_id/unit`.
fn stops(rig: &SupervisorTestRig) -> Vec<String> {
    rig.host
        .calls()
        .into_iter()
        .filter_map(|c| match c {
            HostCall::Stop { copy_id, unit } => Some(format!("{copy_id}/{unit}")),
            _ => None,
        })
        .collect()
}

/// The copies the host was asked to terminate, with the disks it kept.
fn terminates(rig: &SupervisorTestRig) -> Vec<(String, Vec<String>)> {
    rig.host
        .calls()
        .into_iter()
        .filter_map(|c| match c {
            HostCall::Terminate { copy_id, keep } => Some((copy_id, keep)),
            _ => None,
        })
        .collect()
}

/// The units the host was asked to apply, as `copy_id/unit`.
fn applies(rig: &SupervisorTestRig) -> Vec<String> {
    rig.host
        .calls()
        .into_iter()
        .filter_map(|c| match c {
            HostCall::Apply { copy_id, unit, .. } => Some(format!("{copy_id}/{unit}")),
            _ => None,
        })
        .collect()
}

/// A person's command: a terminate keeps the disks its node lists.
fn cmd(id: i64, verb: Verb, node: Option<&str>) -> SupervisorCommandRow {
    let spec_json = (verb == Verb::Terminate).then(|| terminate_work(weft_core::infra::TerminateDisks::KeepListed));
    SupervisorCommandRow {
        id,
        project_id: PROJECT,
        node_id: node.map(|s| s.to_string()),
        verb,
        running_policy: Some(Policy::Cancel),
        spec_json,
        force: false,
        drain_timeout_secs: weft_broker_client::protocol::DEFAULT_DRAIN_TIMEOUT_SECS,
        copies: weft_core::instance::Copies::Shared,
    }
}

fn terminate_work(disks: weft_core::infra::TerminateDisks) -> serde_json::Value {
    serde_json::to_value(weft_broker_client::protocol::TerminateWork { disks }).unwrap()
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
async fn stop_flips_status_then_stops_then_emits_stopped() {
    let rig = rig();
    rig.broker.add_infra_node(PROJECT, NODE, "inst1", Status::Running);
    rig.broker.enqueue_command(cmd(1, Verb::Stop, Some(NODE)));

    let did_work = rig.tick_lifecycle().await.unwrap();
    assert!(did_work);

    // Status writes should be (stopping, stopped) in order.
    let writes = rig.broker.status_writes();
    assert_eq!(
        writes
            .iter()
            .filter(|(_, n, _, _)| n == NODE)
            .map(|(_, _, _, s)| *s)
            .collect::<Vec<_>>(),
        vec![Status::Stopping, Status::Stopped]
    );

    // The unit was stopped on the host.
    assert_eq!(stops(&rig), vec!["inst1/bridge".to_string()]);

    // A `stopped` event was emitted.
    let events = rig.broker.events();
    assert!(events.iter().any(|(_, n, _, k, _)| {
        n.as_deref() == Some(NODE) && k == "stopped"
    }));

    // Command was marked complete with no error.
    assert_eq!(rig.broker.completed_commands(), vec![(1, None, false)]);
}

#[tokio::test]
async fn cancel_requested_halts_stop_and_records_cancelled_outcome() {
    // The user's `/infra/cancel` flagged the command before the
    // supervisor got to the per-node work: the tick halts (no stop) and
    // completes the command with the CANCELLED outcome,
    // never as a failure.
    let rig = rig();
    rig.broker.add_infra_node(PROJECT, NODE, "inst1", Status::Running);
    rig.broker.enqueue_command(cmd(1, Verb::Stop, Some(NODE)));
    rig.broker.set_cancel_requested(1);

    let did_work = rig.tick_lifecycle().await.unwrap();
    assert!(did_work);

    assert!(stops(&rig).is_empty(), "halted before any stop");
    assert_eq!(rig.broker.cancelled_commands(), vec![1]);
    assert!(rig.broker.failed_commands().is_empty(), "a cancel is not a failure");
}

#[tokio::test]
async fn stop_leaves_keep_running_units_and_stops_the_rest() {
    let rig = rig();
    // Two-unit node: `web` stops on stop, `license` keeps running
    // (survives stop, only terminate removes it).
    let units = std::collections::BTreeMap::from([
        ("web".to_string(), unit(Status::Running, weft_core::StopBehavior::Stop)),
        ("license".to_string(), unit(Status::Running, weft_core::StopBehavior::KeepRunning)),
    ]);
    rig.broker.add_infra_node_with(PROJECT, NODE, "inst1", Status::Running, None, Default::default(), units);
    rig.broker.enqueue_command(cmd(1, Verb::Stop, Some(NODE)));

    rig.tick_lifecycle().await.unwrap();

    assert_eq!(stops(&rig), vec!["inst1/web".to_string()], "only the Stop unit; KeepRunning survives");
    // Per-unit status: web stopped, license still running (a running unit
    // keeps the node rollup running).
    let row = rig.broker.infra_node(PROJECT, NODE).unwrap();
    assert_eq!(row.units.get("web").unwrap().status, Status::Stopped);
    assert_eq!(row.units.get("license").unwrap().status, Status::Running);
}

#[tokio::test]
async fn force_stop_takes_down_keep_running_units_too() {
    let rig = rig();
    let units = std::collections::BTreeMap::from([
        ("web".to_string(), unit(Status::Running, weft_core::StopBehavior::Stop)),
        ("license".to_string(), unit(Status::Running, weft_core::StopBehavior::KeepRunning)),
    ]);
    rig.broker.add_infra_node_with(PROJECT, NODE, "inst1", Status::Running, None, Default::default(), units);
    let mut command = cmd(1, Verb::Stop, Some(NODE));
    command.force = true;
    rig.broker.enqueue_command(command);

    rig.tick_lifecycle().await.unwrap();

    let mut stopped = stops(&rig);
    stopped.sort();
    assert_eq!(stopped, vec!["inst1/license".to_string(), "inst1/web".to_string()]);
    let row = rig.broker.infra_node(PROJECT, NODE).unwrap();
    assert_eq!(row.units.get("web").unwrap().status, Status::Stopped);
    assert_eq!(row.units.get("license").unwrap().status, Status::Stopped);
    assert_eq!(row.status, Status::Stopped);
}

/// A unit the host runs for the copy that the roster does not carry (a
/// unit dropped from the spec whose removal never landed) is stopped
/// too, and never stamped.
#[tokio::test]
async fn stop_also_stops_an_orphan_unit_without_stamping_it() {
    let rig = rig();
    rig.broker.add_infra_node(PROJECT, NODE, "inst1", Status::Running);
    rig.host.set_state(&copy("inst1"), "leftover", weft_platform_traits::UnitRunState::Ready);
    rig.broker.enqueue_command(cmd(1, Verb::Stop, Some(NODE)));

    rig.tick_lifecycle().await.unwrap();

    let mut stopped = stops(&rig);
    stopped.sort();
    assert_eq!(stopped, vec!["inst1/bridge".to_string(), "inst1/leftover".to_string()]);
    assert!(
        !rig.broker.calls().iter().any(|c| matches!(
            c,
            weft_infra_supervisor::broker_ops::BrokerCall::SetStatus { unit: Some(u), .. } if u == "leftover"
        )),
        "an orphan is never stamped"
    );
    assert_eq!(rig.broker.completed_commands(), vec![(1, None, false)]);
}

// ---------- terminate verb ----------

#[tokio::test]
async fn terminate_flips_status_removes_the_copy_then_the_row() {
    let rig = rig();
    rig.broker.add_infra_node(PROJECT, NODE, "inst1", Status::Running);
    rig.broker.enqueue_command(cmd(2, Verb::Terminate, Some(NODE)));

    rig.tick_lifecycle().await.unwrap();

    // Status flipped to terminating before the removal.
    let writes = rig.broker.status_writes();
    assert!(writes.iter().any(|(_, n, _, s)| n == NODE && *s == Status::Terminating));

    // The copy was terminated on the host.
    assert_eq!(terminates(&rig), vec![("inst1".to_string(), Vec::new())]);

    // Row was removed.
    assert!(rig.broker.infra_node(PROJECT, NODE).is_none());

    // A `terminated` event was emitted.
    let events = rig.broker.events();
    assert!(events.iter().any(|(_, _, _, k, _)| k == "terminated"));
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
    assert_eq!(rig.broker.completed_commands(), vec![(3, None, false)]);
    // Nothing stopped.
    assert!(stops(&rig).is_empty());
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

    let mut command = cmd(5, Verb::Stop, Some(NODE));
    command.running_policy = Some(Policy::Wait);
    rig.broker.enqueue_command(command);

    // Pretend there are 0 running executions from the start (the
    // simplest path). Drain returns immediately.
    rig.broker.set_running_count(PROJECT, &weft_core::instance::Copies::Shared, 0);

    let did_work = rig.tick_lifecycle().await.unwrap();
    assert!(did_work);
    assert_eq!(rig.broker.completed_commands(), vec![(5, None, false)]);
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
    rig.broker.set_running_count(PROJECT, &weft_core::instance::Copies::Shared, 5);

    let mut command = cmd(6, Verb::Stop, Some(NODE));
    command.running_policy = Some(Policy::Wait);
    rig.broker.enqueue_command(command);

    let did_work = rig.tick_lifecycle().await.unwrap();
    assert!(did_work);
    // Stop should still complete (timeout proceeds).
    assert_eq!(rig.broker.completed_commands(), vec![(6, None, false)]);
    let writes = rig.broker.status_writes();
    assert!(writes.iter().any(|(_, _, _, s)| *s == Status::Stopped));
}

// ---------- apply verb ----------

/// Minimal one-unit spec_json the supervisor can deserialize and
/// resolve: one unit with one upstream-image container, no endpoints or
/// volumes. The fake host lands an applied unit ready.
fn apply_cmd(id: i64) -> SupervisorCommandRow {
    let spec = serde_json::json!({
        "units": [{
            "name": "bridge",
            "containers": [{
                "name": "c",
                "image": { "kind": "upstream", "reference": "nginx:1" }
            }]
        }]
    });
    SupervisorCommandRow {
        id,
        project_id: PROJECT,
        node_id: Some(NODE.into()),
        verb: Verb::Apply,
        running_policy: None,
        spec_json: Some(spec),
        force: false,
        drain_timeout_secs: weft_broker_client::protocol::DEFAULT_DRAIN_TIMEOUT_SECS,
        copies: weft_core::instance::Copies::Shared,
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
async fn apply_writes_provisioning_before_the_host_applies_then_applied() {
    let rig = rig();
    // Fresh apply: no prior infra_node row.
    rig.broker.enqueue_command(apply_cmd(1));

    let did_work = rig.tick_lifecycle().await.unwrap();
    assert!(did_work);

    // set_provisioning must precede set_applied in the broker call
    // log: a row exists at Provisioning before anything runs.
    let names = call_names(&rig);
    let prov = names.iter().position(|n| *n == "set_provisioning");
    let applied = names.iter().position(|n| *n == "set_applied");
    assert!(prov.is_some(), "set_provisioning must be called; calls={names:?}");
    assert!(applied.is_some(), "set_applied must be called; calls={names:?}");
    assert!(prov < applied, "provisioning before applied; calls={names:?}");

    // And the host applied the unit.
    assert_eq!(applies(&rig).len(), 1, "expected one unit applied");

    // Command completed with no error.
    assert_eq!(rig.broker.completed_commands(), vec![(1, None, false)]);
}

/// What the host says it runs differently from what was asked lands on
/// the applied row, where every status read tells the person.
#[tokio::test]
async fn apply_stamps_the_host_notes_on_the_row() {
    use weft_infra_supervisor::broker_ops::BrokerCall;
    let rig = rig();
    rig.host.note(&["the container gets every GPU on this machine"]);
    rig.broker.enqueue_command(apply_cmd(1));
    assert!(rig.tick_lifecycle().await.unwrap());
    let notes: Vec<Vec<String>> = rig
        .broker
        .calls()
        .into_iter()
        .filter_map(|c| match c {
            BrokerCall::SetApplied { notes, .. } => Some(notes),
            _ => None,
        })
        .collect();
    assert_eq!(notes, vec![vec!["the container gets every GPU on this machine".to_string()]]);
}

#[tokio::test]
async fn apply_failure_flips_to_failed_and_bubbles() {
    let rig = rig();
    // Make the host's apply fail so execute_apply hits the Failed-status
    // branch.
    rig.host.fail_applies_with(Some("no room on the host"));
    rig.broker.enqueue_command(apply_cmd(2));

    let did_work = rig.tick_lifecycle().await.unwrap();
    assert!(did_work);

    // The row was flipped to Failed (set_status with FailureStage).
    let writes = rig.broker.status_writes();
    assert!(
        writes.iter().any(|(_, n, _, s)| n == NODE && *s == Status::Failed),
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
    // the Skip path: no set_provisioning, no host apply.
    let spec = serde_json::json!({
        "units": [{
            "name": "bridge",
            "containers": [{
                "name": "c",
                "image": { "kind": "upstream", "reference": "nginx:1" }
            }]
        }]
    });
    let hash = applied_hash(&spec, &shared_id());
    rig.broker.add_infra_node_with(
        PROJECT,
        NODE,
        &shared_id(),
        Status::Running,
        Some(hash),
        std::collections::BTreeMap::new(),
        {
            let mut m = std::collections::BTreeMap::new();
            m.insert(
                NODE.to_string(),
                unit(Status::Running, weft_core::StopBehavior::Stop),
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
    assert!(applies(&rig).is_empty(), "skip path must not apply anything");
}

/// Where each endpoint answers is stamped with the apply: the address
/// the workers use, the front-door path of a public one, and the install
/// network address of one open to it.
#[tokio::test]
async fn apply_stamps_every_endpoints_address() {
    let rig = rig();
    let spec = serde_json::json!({
        "units": [{
            "name": "bridge",
            "containers": [{
                "name": "c",
                "image": { "kind": "upstream", "reference": "nginx:1" },
                "ports": [{ "name": "http", "port": 8080 }]
            }]
        }],
        "endpoints": [
            { "name": "api", "target": { "kind": "unit", "unit": "bridge", "container": "c", "port": "http" },
              "expose": { "kind": "public", "path": "/hooks" } },
            { "name": "sql", "target": { "kind": "unit", "unit": "bridge", "container": "c", "port": "http" },
              "expose": { "kind": "same_network" } }
        ]
    });
    let mut command = apply_cmd(1);
    command.spec_json = Some(spec);
    rig.broker.enqueue_command(command);
    rig.tick_lifecycle().await.unwrap();

    let row = rig.broker.infra_node(PROJECT, NODE).unwrap();
    let path = row.addresses.public_paths.get("api").expect("a public endpoint has a path");
    assert_eq!(path, &format!("/infra/{PROJECT}/{}/hooks", row.copy_id));
    assert!(row.addresses.urls.contains_key("api") && row.addresses.urls.contains_key("sql"));
    assert!(row.addresses.doors.contains_key("sql"), "an endpoint open to the network has its door");
    assert!(!row.addresses.doors.contains_key("api"));
}

#[tokio::test]
async fn apply_reconciles_down_unit_and_skips_up_unit() {
    let rig = rig();
    // Two-unit spec: `web` + `license`.
    let spec = serde_json::json!({
        "units": [
            { "name": "web", "containers": [{ "name": "c", "image": { "kind": "upstream", "reference": "nginx:1" } }] },
            { "name": "license", "containers": [{ "name": "c", "image": { "kind": "upstream", "reference": "nginx:1" } }] }
        ]
    });
    // Prior row: `license` is UP (Running, frozen), `web` is DOWN
    // (Stopped). Apply must reconcile only `web`.
    let prior_units = std::collections::BTreeMap::from([
        ("web".to_string(), unit(Status::Stopped, weft_core::StopBehavior::Stop)),
        ("license".to_string(), unit(Status::Running, weft_core::StopBehavior::KeepRunning)),
    ]);
    rig.broker.add_infra_node_with(
        PROJECT, NODE, &shared_id(), Status::Running, Some("oldhash".into()),
        std::collections::BTreeMap::new(), prior_units,
    );
    // license is up on the host; web is stopped.
    rig.host.set_state(&copy(&shared_id()), "license", weft_platform_traits::UnitRunState::Ready);
    rig.host.set_state(&copy(&shared_id()), "web", weft_platform_traits::UnitRunState::Stopped);

    let mut command = apply_cmd(4);
    command.spec_json = Some(spec);
    rig.broker.enqueue_command(command);

    rig.tick_lifecycle().await.unwrap();

    // It DID provision (web is down, hash differs, so not a full skip).
    let names = call_names(&rig);
    assert!(names.contains(&"set_provisioning"), "calls={names:?}");

    // The host applied ONLY web, never license (up and frozen).
    assert_eq!(applies(&rig), vec![format!("{}/web", shared_id())]);

    // Final per-unit status: web back to Running, license still Running.
    let row = rig.broker.infra_node(PROJECT, NODE).unwrap();
    assert_eq!(row.units.get("web").unwrap().status, Status::Running);
    assert_eq!(row.units.get("license").unwrap().status, Status::Running);
}

/// A Fresh apply over a `Terminating` row (a terminate that stamped
/// the row and then failed or died before its removal landed) must first
/// terminate the PRIOR copy, keeping the disks its row recorded, and then
/// apply the copy under the SAME copy id, which is how the disks kept
/// through the terminate are found again. Without the terminate first the
/// old units would keep running while the post-readiness stamp drops their
/// image refs from the keep-set.
#[tokio::test]
async fn fresh_apply_over_terminating_row_terminates_the_prior_copy_first() {
    let rig = rig();
    let mut prior = unit(Status::Terminating, weft_core::StopBehavior::Stop);
    prior.image_refs = ["weft-infra-bridge:old".to_string()].into_iter().collect();
    let prior_units = std::collections::BTreeMap::from([("bridge".to_string(), prior)]);
    rig.broker.add_infra_node_with(
        PROJECT, NODE, &shared_id(), Status::Terminating, Some("oldhash".into()),
        std::collections::BTreeMap::new(), prior_units,
    );
    rig.broker.set_keep_disks(PROJECT, NODE, None, vec!["data".to_string()]);
    rig.broker.enqueue_command(apply_cmd(9));

    rig.tick_lifecycle().await.unwrap();

    let row = rig.broker.infra_node(PROJECT, NODE).unwrap();
    assert_eq!(row.copy_id, shared_id(), "the copy keeps its id, so its kept disks are adopted");
    // The prior copy is terminated, keeping the disks its row recorded,
    // before the new copy's unit is applied.
    assert_eq!(terminates(&rig), vec![(shared_id(), vec!["data".to_string()])]);
    let calls = rig.host.calls();
    let terminated = calls.iter().position(|c| matches!(c, HostCall::Terminate { .. })).unwrap();
    let applied = calls.iter().position(|c| matches!(c, HostCall::Apply { .. })).unwrap();
    assert!(terminated < applied, "calls={calls:?}");
    assert_eq!(row.status, Status::Running);
}

// ---------- ownership fence: a displaced owner must not finish its command ----------
//
// The supervisor's single-actor authority is the exclusive `infra_owner`
// lease. If ownership moves to another supervisor mid-command (a lease
// takeover after this one looked dead), every state write from the old
// one is rejected by the broker, so the old one must
// NOT complete the command: it stays uncompleted for the new owner to
// re-run (the user never re-acts). The fake models the move with
// `set_project_owned(false)`, which returns `Displaced` from every
// ownership-gated write exactly as the broker's `owns_project_predicate`
// would. These tests pin "a displaced owner leaves the command for the
// new owner" for each verb that changes what the host runs.

#[tokio::test]
async fn stop_aborts_without_completing_when_ownership_moves_mid_command() {
    let rig = rig();
    rig.broker.add_infra_node(PROJECT, NODE, "inst1", Status::Running);
    // Ownership moves to another supervisor right after the claim, so the
    // per-unit `set_status` write lands displaced (the broker rejects it).
    rig.broker.displace_on_claim(PROJECT);
    rig.broker.enqueue_command(cmd(1, Verb::Stop, Some(NODE)));

    let did_work = rig.tick_lifecycle().await.unwrap();
    assert!(did_work, "the command was claimed, so the tick did work");

    // The command is NOT completed: it remains for the new owner to run.
    assert!(
        rig.broker.completed_commands().is_empty(),
        "a displaced owner must not complete the command; completed={:?}",
        rig.broker.completed_commands()
    );
}

#[tokio::test]
async fn terminate_aborts_before_touching_the_host_when_ownership_moves_mid_command() {
    let rig = rig();
    rig.broker.add_infra_node(PROJECT, NODE, "inst1", Status::Running);
    rig.broker.displace_on_claim(PROJECT);
    rig.broker.enqueue_command(cmd(2, Verb::Terminate, Some(NODE)));

    rig.tick_lifecycle().await.unwrap();

    // The Terminating stamp (the first ownership-gated write) was
    // displaced, so nothing touched the host, the node row survives for
    // the new owner to terminate, and the command is left uncompleted.
    assert!(terminates(&rig).is_empty(), "a displaced owner must not remove anything");
    assert!(
        rig.broker.infra_node(PROJECT, NODE).is_some(),
        "the row must survive for the new owner"
    );
    assert!(
        rig.broker.completed_commands().is_empty(),
        "a displaced owner must not complete the terminate; completed={:?}",
        rig.broker.completed_commands()
    );
    // No `terminated` event was emitted (we aborted before it).
    assert!(
        !rig.broker.events().iter().any(|(_, _, _, k, _)| k == "terminated"),
        "no terminated event when ownership moved mid-terminate"
    );
}

#[tokio::test]
async fn apply_does_not_complete_when_ownership_moves_mid_command() {
    let rig = rig();
    rig.broker.displace_on_claim(PROJECT);
    rig.broker.enqueue_command(apply_cmd(3));

    rig.tick_lifecycle().await.unwrap();

    // set_provisioning is rejected (Displaced) before any host apply, and the
    // command is left uncompleted for the new owner.
    assert!(
        rig.broker.completed_commands().is_empty(),
        "a displaced owner must not complete the apply; completed={:?}",
        rig.broker.completed_commands()
    );
    assert!(
        rig.broker.infra_node(PROJECT, NODE).is_none(),
        "no Provisioning row should be committed by a displaced owner"
    );
}

#[tokio::test]
async fn command_left_by_displaced_owner_completes_once_new_owner_runs_it() {
    // The whole point of leaving the command uncompleted: the NEW owner
    // re-runs it and finishes it, with no user re-action. We model the
    // same rig as the new owner: ownership is restored (it now holds the
    // lease), the SAME command is claimed again, and this time it
    // completes exactly once.
    let rig = rig();
    rig.broker.add_infra_node(PROJECT, NODE, "inst1", Status::Running);

    // First owner: loses ownership mid-stop, leaves the command.
    rig.broker.displace_on_claim(PROJECT);
    rig.broker.enqueue_command(cmd(7, Verb::Stop, Some(NODE)));
    rig.tick_lifecycle().await.unwrap();
    assert!(
        rig.broker.completed_commands().is_empty(),
        "displaced owner left the command uncompleted"
    );

    // New owner: holds the lease, and the claim hands out the same
    // still-uncompleted command again; it runs to completion.
    rig.broker.set_project_owned(PROJECT, true);
    rig.tick_lifecycle().await.unwrap();

    assert_eq!(
        rig.broker.completed_commands(),
        vec![(7, None, false)],
        "the new owner completes the command exactly once, no user re-action"
    );
    let writes = rig.broker.status_writes();
    assert!(
        writes.iter().any(|(_, _, _, s)| *s == Status::Stopped),
        "the new owner actually performed the stop"
    );
}

// ---------- the running loop ----------
//
// These drive the real `lifecycle::run_loop`, not the one-command `tick`.
// A `wait`-policy stop on a project whose running count is gated stays
// mid-drain until the test opens the gate, which is how a command is
// kept running while the loop's other behavior is observed. The claim
// holds up to `MAX_HOLD` (25s) when nothing is waiting, so every wait
// below is bounded well under it: a loop that failed to ask again would
// hold past the bound and fail the test.

const P2: uuid::Uuid = uuid::Uuid::from_u128(2);

/// A stop of `project` with no infra rows: after its drain it completes
/// as a no-op.
fn stop_of(id: i64, project: uuid::Uuid, policy: Policy) -> SupervisorCommandRow {
    SupervisorCommandRow {
        id,
        project_id: project,
        node_id: None,
        verb: Verb::Stop,
        running_policy: Some(policy),
        spec_json: None,
        force: false,
        drain_timeout_secs: weft_broker_client::protocol::DEFAULT_DRAIN_TIMEOUT_SECS,
        copies: weft_core::instance::Copies::Shared,
    }
}

/// Wait until `cond` holds, failing well before a claim's full hold.
async fn until(what: &str, cond: impl Fn() -> bool) {
    let bounded = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while !cond() {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    });
    bounded.await.unwrap_or_else(|_| panic!("timed out waiting until {what}"));
}

fn completed_ids(rig: &SupervisorTestRig) -> Vec<i64> {
    rig.broker.completed_commands().iter().map(|(id, _, _)| *id).collect()
}

fn running_count_calls(rig: &SupervisorTestRig, project: uuid::Uuid) -> usize {
    use weft_infra_supervisor::broker_ops::BrokerCall;
    rig.broker
        .calls()
        .iter()
        .filter(|c| matches!(c, BrokerCall::RunningCount { project_id, .. } if *project_id == project))
        .count()
}

weft_core::stress_test!(
    name: one_projects_commands_run_in_order_beside_another_projects,
    runs: 16,
    worker_threads: 4,
    async fn body() {
        use weft_infra_supervisor::broker_ops::BrokerCall;
        let rig = rig();
        rig.broker.add_project(P2);
        rig.broker.gate_running_count(PROJECT);
        rig.broker.enqueue_command(stop_of(1, PROJECT, Policy::Wait));
        rig.broker.enqueue_command(stop_of(2, PROJECT, Policy::Cancel));
        rig.broker.enqueue_command(stop_of(3, P2, Policy::Cancel));
        let (lifecycle, _changes) = rig.spawn_lifecycle_loop();

        // P2's command runs while P1's first is held mid-drain, and P1's
        // second waits behind it: the claims name P1 busy.
        until("the other project's command completed", || completed_ids(&rig) == vec![3]).await;
        assert!(rig.broker.calls().iter().any(|c| matches!(
            c,
            BrokerCall::ClaimCommand { busy_projects, .. } if busy_projects == &vec![PROJECT]
        )));

        // The finished task frees its project: the second command runs next.
        rig.broker.open_running_count(PROJECT);
        until("both of P1's commands completed", || completed_ids(&rig).len() == 3).await;
        assert_eq!(completed_ids(&rig), vec![3, 1, 2], "P1's commands run in the order issued");
        lifecycle.abort();
    }
);

weft_core::stress_test!(
    name: a_project_taken_on_ends_the_held_claim,
    runs: 16,
    worker_threads: 4,
    async fn body() {
        let rig = rig();
        // The command was issued while nobody owned its project, so no
        // held claim of this supervisor's woke for it.
        rig.broker.set_project_unowned(PROJECT);
        rig.broker.enqueue_command(stop_of(1, PROJECT, Policy::Cancel));
        let (lifecycle, changes) = rig.spawn_lifecycle_loop();
        until("the loop holds a claim", || {
            rig.broker.calls().iter().any(|c| matches!(
                c,
                weft_infra_supervisor::broker_ops::BrokerCall::ClaimCommand { .. }
            ))
        })
        .await;

        let change = rig.tick_ownership().await.unwrap().expect("the tick took the project on");
        assert_eq!(change.claimed, vec![PROJECT]);
        changes.send(change).unwrap();
        until("the command completed", || completed_ids(&rig) == vec![1]).await;
        lifecycle.abort();
    }
);

weft_core::stress_test!(
    name: unowned_work_asks_the_ownership_loop_to_tick,
    runs: 16,
    worker_threads: 4,
    async fn body() {
        let rig = rig();
        rig.broker.set_project_unowned(PROJECT);
        let wanted = rig.state.ownership_wanted.clone();
        let (lifecycle, _changes) = rig.spawn_lifecycle_loop();
        until("the loop holds a claim", || {
            rig.broker.calls().iter().any(|c| matches!(
                c,
                weft_infra_supervisor::broker_ops::BrokerCall::ClaimCommand { .. }
            ))
        })
        .await;

        rig.broker.enqueue_command(stop_of(1, PROJECT, Policy::Cancel));
        tokio::time::timeout(std::time::Duration::from_secs(10), wanted.notified())
            .await
            .expect("unowned work must ask the ownership loop to tick");
        assert!(completed_ids(&rig).is_empty(), "nobody owns the project yet");
        lifecycle.abort();
    }
);

weft_core::stress_test!(
    name: a_lost_project_stops_its_running_command,
    runs: 16,
    worker_threads: 4,
    async fn body() {
        use weft_infra_supervisor::broker_ops::BrokerCall;
        let rig = rig();
        rig.tick_ownership().await.unwrap();
        rig.broker.gate_running_count(PROJECT);
        rig.broker.enqueue_command(stop_of(1, PROJECT, Policy::Wait));
        let (lifecycle, changes) = rig.spawn_lifecycle_loop();
        until("the command is mid-drain", || running_count_calls(&rig, PROJECT) == 1).await;

        rig.broker.set_project_owned(PROJECT, false);
        let change = rig.tick_ownership().await.unwrap().expect("the tick lost the project");
        assert_eq!(change.lost, vec![PROJECT]);
        changes.send(change).unwrap();

        // Given back, the project is not busy any more: the same command
        // is claimed and started again, which only happens once the
        // stopped task's busy entry is gone.
        rig.broker.set_project_unowned(PROJECT);
        let change = rig.tick_ownership().await.unwrap().expect("the tick took it back");
        changes.send(change).unwrap();
        until("the command started again", || running_count_calls(&rig, PROJECT) == 2).await;

        rig.broker.open_running_count(PROJECT);
        until("the command completed", || completed_ids(&rig) == vec![1]).await;
        let completions = rig
            .broker
            .calls()
            .iter()
            .filter(|c| matches!(c, BrokerCall::CommandComplete { .. }))
            .count();
        assert_eq!(completions, 1, "the stopped run never reached its completion write");
        lifecycle.abort();
    }
);

// ---------- per-instance copies ----------

fn ada() -> weft_core::instance::InstanceId {
    weft_core::instance::InstanceId::new("ada").unwrap()
}

#[tokio::test]
async fn terminating_one_instances_copy_leaves_the_shared_copy_and_the_others() {
    let rig = rig();
    let bob = weft_core::instance::InstanceId::new("bob").unwrap();
    rig.broker.add_infra_node(PROJECT, NODE, "inst-shared", Status::Running);
    rig.broker.add_instance_infra_node(PROJECT, NODE, &ada(), "inst-ada", Status::Running);
    rig.broker.add_instance_infra_node(PROJECT, NODE, &bob, "inst-bob", Status::Running);
    let mut terminate = cmd(2, Verb::Terminate, Some(NODE));
    terminate.copies = weft_core::instance::Copies::Instance(ada());
    rig.broker.enqueue_command(terminate);

    rig.tick_lifecycle().await.unwrap();

    assert_eq!(terminates(&rig), vec![("inst-ada".to_string(), Vec::new())]);
    assert!(rig.broker.infra_copy(PROJECT, NODE, Some(&ada())).is_none());
    assert!(rig.broker.infra_copy(PROJECT, NODE, Some(&bob)).is_some());
    assert!(rig.broker.infra_node(PROJECT, NODE).is_some());
}

/// An instance's wipe terminates its copy deleting every disk, the ones
/// the node keeps too, while a person's terminate of another instance's
/// copy keeps them.
#[tokio::test]
async fn a_wipe_deletes_the_kept_disks_a_persons_terminate_keeps() {
    let rig = rig();
    let bob = weft_core::instance::InstanceId::new("bob").unwrap();
    rig.broker.add_instance_infra_node(PROJECT, NODE, &ada(), "inst-ada", Status::Running);
    rig.broker.add_instance_infra_node(PROJECT, NODE, &bob, "inst-bob", Status::Running);
    rig.broker.set_keep_disks(PROJECT, NODE, Some(&ada()), vec!["data".to_string()]);
    rig.broker.set_keep_disks(PROJECT, NODE, Some(&bob), vec!["data".to_string()]);
    let mut wipe = cmd(2, Verb::Terminate, Some(NODE));
    wipe.copies = weft_core::instance::Copies::Instance(ada());
    wipe.spec_json = Some(terminate_work(weft_core::infra::TerminateDisks::DeleteAll));
    let mut terminate = cmd(3, Verb::Terminate, Some(NODE));
    terminate.copies = weft_core::instance::Copies::Instance(bob);
    rig.broker.enqueue_command(wipe);
    rig.broker.enqueue_command(terminate);

    rig.tick_lifecycle().await.unwrap();
    rig.tick_lifecycle().await.unwrap();

    assert_eq!(
        terminates(&rig),
        vec![("inst-ada".to_string(), Vec::new()), ("inst-bob".to_string(), vec!["data".to_string()])]
    );
}

/// An instance's copy an earlier terminate took down still holds the
/// disks it kept, with no row left. The instance's wipe reaches it
/// through the host and deletes them; another instance's kept copy is
/// left alone.
#[tokio::test]
async fn a_wipe_deletes_the_disks_a_copy_with_no_row_still_holds() {
    let rig = rig();
    let bob = weft_core::instance::InstanceId::new("bob").unwrap();
    for instance in [ada(), bob.clone()] {
        let copy = copy(&weft_core::infra::NodeRef::copy_id(PROJECT, NODE, Some(&instance)));
        rig.host.set_state(&copy, "main", weft_platform_traits::UnitRunState::Ready);
        weft_platform_traits::InfraHost::terminate(rig.host.as_ref(), &copy, &["data".to_string()]).await.unwrap();
    }
    let ada_copy = weft_core::infra::NodeRef::copy_id(PROJECT, NODE, Some(&ada()));
    let bob_copy = weft_core::infra::NodeRef::copy_id(PROJECT, NODE, Some(&bob));
    let before = rig.host.calls().len();
    let mut wipe = cmd(2, Verb::Terminate, Some(NODE));
    wipe.copies = weft_core::instance::Copies::Instance(ada());
    wipe.spec_json = Some(terminate_work(weft_core::infra::TerminateDisks::DeleteAll));
    rig.broker.enqueue_command(wipe);

    rig.tick_lifecycle().await.unwrap();

    let after: Vec<HostCall> = rig.host.calls().into_iter().skip(before).collect();
    assert_eq!(after, vec![HostCall::Terminate { copy_id: ada_copy.clone(), keep: vec![] }]);
    assert!(rig.host.kept_disks(&ada_copy).is_empty());
    assert_eq!(rig.host.kept_disks(&bob_copy), vec!["data".to_string()]);
    assert_eq!(rig.broker.completed_commands(), vec![(2, None, false)]);
}

/// A terminate row issued without its work is a writer bug: the command
/// fails before the host is touched.
#[tokio::test]
async fn a_terminate_without_its_work_fails_before_touching_the_host() {
    let rig = rig();
    rig.broker.add_infra_node(PROJECT, NODE, "inst1", Status::Running);
    let mut terminate = cmd(2, Verb::Terminate, Some(NODE));
    terminate.spec_json = None;
    rig.broker.enqueue_command(terminate);

    rig.tick_lifecycle().await.unwrap();

    assert!(terminates(&rig).is_empty());
    assert!(rig.broker.infra_node(PROJECT, NODE).is_some());
}

#[tokio::test]
async fn a_project_wide_terminate_for_every_copy_takes_them_all() {
    let rig = rig();
    rig.broker.add_infra_node(PROJECT, NODE, "inst-shared", Status::Running);
    rig.broker.add_instance_infra_node(PROJECT, NODE, &ada(), "inst-ada", Status::Running);
    let mut terminate = cmd(2, Verb::Terminate, None);
    terminate.copies = weft_core::instance::Copies::Every;
    rig.broker.enqueue_command(terminate);

    rig.tick_lifecycle().await.unwrap();

    assert!(rig.broker.infra_copy(PROJECT, NODE, Some(&ada())).is_none());
    assert!(rig.broker.infra_node(PROJECT, NODE).is_none());
}

#[tokio::test]
async fn a_shared_stop_never_touches_an_instances_copy() {
    let rig = rig();
    rig.broker.add_infra_node(PROJECT, NODE, "inst-shared", Status::Running);
    rig.broker.add_instance_infra_node(PROJECT, NODE, &ada(), "inst-ada", Status::Running);
    rig.broker.enqueue_command(cmd(3, Verb::Stop, Some(NODE)));

    rig.tick_lifecycle().await.unwrap();

    assert_eq!(rig.broker.infra_node(PROJECT, NODE).unwrap().status, Status::Stopped);
    assert_eq!(
        rig.broker.infra_copy(PROJECT, NODE, Some(&ada())).unwrap().status,
        Status::Running
    );
    assert_eq!(stops(&rig), vec!["inst-shared/bridge".to_string()], "only the shared copy");
}

#[tokio::test]
async fn an_instances_apply_builds_its_own_copy_under_a_fresh_host_instance() {
    let rig = rig();
    rig.broker.add_infra_node(PROJECT, NODE, "inst-shared", Status::Running);
    let mut apply = apply_cmd(4);
    apply.copies = weft_core::instance::Copies::Instance(ada());
    rig.broker.enqueue_command(apply);

    rig.tick_lifecycle().await.unwrap();

    assert_eq!(rig.broker.completed_commands(), vec![(4, None, false)]);
    let copy = rig.broker.infra_copy(PROJECT, NODE, Some(&ada())).expect("the instance's copy");
    assert_eq!(copy.instance, Some(ada()));
    assert_ne!(copy.copy_id, "inst-shared");
    assert_eq!(rig.broker.infra_node(PROJECT, NODE).unwrap().copy_id, "inst-shared");
}

#[tokio::test]
async fn an_apply_for_every_copy_is_refused() {
    let rig = rig();
    let mut apply = apply_cmd(5);
    apply.copies = weft_core::instance::Copies::Every;
    rig.broker.enqueue_command(apply);

    rig.tick_lifecycle().await.unwrap();

    let failed = rig.broker.failed_commands();
    assert_eq!(failed.len(), 1);
    assert!(failed[0].1.contains("every copy"), "{failed:?}");
}

#[tokio::test]
async fn an_instances_wait_drains_on_that_instances_runs() {
    use weft_infra_supervisor::broker_ops::BrokerCall;
    let rig = rig();
    rig.broker.add_instance_infra_node(PROJECT, NODE, &ada(), "inst-ada", Status::Running);
    let mut stop = cmd(6, Verb::Stop, Some(NODE));
    stop.running_policy = Some(Policy::Wait);
    stop.copies = weft_core::instance::Copies::Instance(ada());
    rig.broker.enqueue_command(stop);

    // The shared copies' runs are busy; ada has none. Her wait drains
    // on her own runs only, so the stop goes through at once.
    rig.broker.set_running_count(PROJECT, &weft_core::instance::Copies::Shared, 3);
    rig.broker.set_running_count(PROJECT, &weft_core::instance::Copies::Instance(ada()), 0);

    rig.tick_lifecycle().await.unwrap();

    assert!(rig.broker.calls().iter().any(|c| matches!(
        c,
        BrokerCall::RunningCount { copies: weft_core::instance::Copies::Instance(m), .. } if *m == ada()
    )));
    assert!(
        !rig.broker.calls().iter().any(|c| matches!(
            c,
            BrokerCall::RunningCount { copies: weft_core::instance::Copies::Shared | weft_core::instance::Copies::Every, .. }
        )),
        "the shared runs are never waited on"
    );
    assert_eq!(rig.broker.completed_commands(), vec![(6, None, false)]);
    assert_eq!(rig.broker.infra_copy(PROJECT, NODE, Some(&ada())).unwrap().status, Status::Stopped);
    assert!(
        rig.broker.status_writes().iter().all(|(_, _, instance, _)| instance.as_ref() == Some(&ada())),
        "every status write is ada's copy's"
    );
}
