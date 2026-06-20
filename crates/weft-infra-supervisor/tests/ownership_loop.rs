//! Layer-3 integration tests for the supervisor's ownership loop.
//!
//! The ownership loop is the single site that claims + renews this
//! pod's exclusive project leases. These tests exercise the loop's
//! broker contract against the in-memory `FakeBroker`:
//!   - the tick calls `sync_ownership` with THIS pod's name + its
//!     reported memory pressure (so the broker claims under the right
//!     identity and gates claiming on real load), and
//!   - the work loops read the owned set via `owned_projects`.
//!
//! The SQL-level exclusivity (two pods never claim one project) lives
//! in the broker's transactional claim and is exercised by the layer-4
//! e2e suite against a real Postgres; here we pin the loop's call shape.

use weft_infra_supervisor::broker_ops::BrokerCall;
use weft_infra_supervisor::testing::SupervisorTestRig;

#[tokio::test]
async fn ownership_tick_syncs_under_this_pods_identity_and_pressure() {
    let rig = SupervisorTestRig::with_tenant("alice");
    rig.broker.add_project("p1", "wm-project-alice-p1");
    rig.mem.set(0.3);

    rig.tick_ownership().await.unwrap();

    // The tick must call sync_ownership with the pod's own name and its
    // current memory pressure, so the broker claims under the identity
    // the dispatcher placed (the `supervisor_pod` key) and gates claiming
    // on real load.
    let synced = rig.broker.calls().iter().any(|c| matches!(
        c,
        BrokerCall::SyncOwnership { pod_name, mem_pressure }
            if pod_name == "test-pod" && (*mem_pressure - 0.3).abs() < 1e-9
    ));
    assert!(synced, "ownership tick must sync_ownership(test-pod, 0.3)");
}

#[tokio::test]
async fn work_loops_read_owned_projects_not_a_global_list() {
    // Both work loops must scope their work to the owned set (via
    // owned_projects), never a global all-projects read. Pin that the
    // health tick goes through owned_projects for THIS pod.
    let rig = SupervisorTestRig::with_tenant("alice");
    rig.broker.add_project("p1", "wm-project-alice-p1");

    rig.tick_health().await.unwrap();

    let read_owned = rig.broker.calls().iter().any(|c| matches!(
        c,
        BrokerCall::OwnedProjects { pod_name } if pod_name == "test-pod"
    ));
    assert!(
        read_owned,
        "health tick must read owned_projects(test-pod), not a global project list"
    );
}
