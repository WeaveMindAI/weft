//! Layer-4: WHERE a project's worker runs is a function of whether the
//! project declares infrastructure, the core of the shared-namespace change:
//!
//!   - a NO-INFRA project's worker runs in the single shared worker
//!     namespace (`wft-shared-workers`), and NO per-project namespace is ever
//!     created for it (the whole point: stop burning the namespace ceiling);
//!   - an INFRA project's worker runs in the project's OWN namespace
//!     (`wft-project-<tenant>--<project>`), next to its infra pods, and that
//!     namespace exists only after infra is applied.
//!
//! Only a real cluster can prove the pod actually lands in the right
//! namespace and that the shared namespace is created lazily, so this is an
//! e2e.
//!
//! Memory-bounded autoscale is covered at Layer-3 (real SQL,
//! tests/live_admission.rs in weft-task-store), NOT here: the placement
//! primitives (least-pressured admittable pick, saturation refusal,
//! draining-aware claim, draining idle-exit, candidate/load agreement) plus
//! the cold-start admittable predicate. See the note on the
//! integrated-saturation case at the bottom of this file for why it cannot be
//! e2e'd deterministically on kind.
//!
//! Levers are operator-style (the dispatcher's public run API + reading the
//! `worker_pod` registry + kubectl via the rig), never a test-only endpoint.
#![cfg(feature = "e2e")]

use weft_e2e::{ensure, infra, platform::Platform, project::Project, run};

/// The shared worker namespace name.
// SYNC: SHARED_WORKER_NAMESPACE <-> crates/weft-dispatcher/src/project_namespace.rs SHARED_WORKER_NAMESPACE, crates/weft-broker/src/auth.rs SHARED_WORKER_NAMESPACE
const SHARED_WORKER_NAMESPACE: &str = "wft-shared-workers";
const INFRA_NODE: &str = "svc";

/// A no-infra project's worker runs in the shared namespace, and the project
/// gets no namespace of its own.
#[tokio::test]
async fn no_infra_worker_runs_in_shared_namespace() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let platform = Platform::connect().await?;
    let mut project = Project::prepare("plain", disp).await?;
    let pid = project.id();

    // Running the project spawns its worker. `run_and_settle` waits for the
    // execution to finish, by which point the worker pod row exists.
    let settled = run::run_and_settle(&mut project).await?;
    settled.completed()?;

    // The worker landed in the shared namespace, NOT a per-project one.
    let pods = platform.worker_pods_for_project(&pid).await?;
    anyhow::ensure!(!pods.is_empty(), "no worker pod row for the project");
    for pod in &pods {
        anyhow::ensure!(
            pod.namespace == SHARED_WORKER_NAMESPACE,
            "no-infra worker must run in the shared namespace; pod {} is in {}",
            pod.pod_name,
            pod.namespace
        );
    }

    // No per-project namespace was created for a no-infra project (the row's
    // project_namespace stays empty; it would be set only on infra apply).
    let project_ns = platform.project_namespace(&pid).await?;
    anyhow::ensure!(
        project_ns.is_empty(),
        "a no-infra project must not get its own k8s namespace; got '{project_ns}'"
    );

    project.finish().await
}

/// An infra project's worker runs in the project's OWN namespace, and that
/// includes the InfraSetup provisioning execution: infra Pods are reachable
/// only from inside the project namespace (the namespace's ingress policy),
/// so `infra start` creates the namespace FIRST and every worker spawned
/// from then on (provisioning included) lands there. No worker of this
/// project ever touches the shared pool once infra is declared and synced.
#[tokio::test]
async fn infra_worker_runs_in_project_namespace() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let platform = Platform::connect().await?;
    let mut project = Project::prepare("infra_min", disp.clone()).await?;
    let pid = project.id();

    // `infra start` creates the per-project namespace, applies infra, and
    // runs the provisioning execution on an in-namespace worker.
    infra::start_and_wait_running(&mut project, INFRA_NODE).await?;

    let pods = platform.worker_pods_for_project(&pid).await?;
    anyhow::ensure!(!pods.is_empty(), "no worker pod row for the infra project");
    for pod in &pods {
        anyhow::ensure!(
            pod.namespace != SHARED_WORKER_NAMESPACE
                && pod.namespace.starts_with("wft-project-"),
            "an infra project's worker must run in the project's own namespace; \
             pod {} is in {}",
            pod.pod_name,
            pod.namespace
        );
    }
    anyhow::ensure!(
        platform.project_namespace(&pid).await?.starts_with("wft-project-"),
        "an infra project must have its own k8s namespace recorded after apply"
    );

    infra::terminate_and_wait_gone(&project, INFRA_NODE).await?;
    project.finish().await
}
// NOTE: there is deliberately NO e2e for the integrated
// spawn-on-saturation loop (a memory-saturated worker forcing a second
// worker to spawn). It cannot be made deterministic on kind: kind pods
// run with no cgroup memory limit, so a worker's `MemPressure::fraction()`
// always reads 0.0, and the worker's own 10s heartbeat continuously
// writes that 0.0 to its `worker_pod.mem_pressure` row. Forcing the row
// to a saturated value from the test is immediately clobbered by the next
// heartbeat, so the second-worker spawn would depend on admission racing
// the heartbeat: a flake by construction. Real memory saturation cannot
// be provoked either (no limit to cross). The decision logic that the
// integrated loop rests on IS covered deterministically at Layer-3
// against real SQL (tests/live_admission.rs in weft-task-store):
// `admit_refuses_saturated_pod`, `pick_admittable_*` (saturated/draining
// skipped), and the cold-start admittable predicate. The only piece left
// e2e-shaped (a real second worker boots and the gateway routes to it) is
// not worth a guaranteed-flaky test; it would need the worker to report a
// forced pressure to itself (an injected MemPressure via env), which is a
// production/ops capability, not present today.
