//! Project status + available-actions observation.
//!
//! `GET /projects/{id}/status` is the dispatcher's single reconciliation
//! surface: the lifecycle status, the transition marker, the derived infra
//! rollup, and `available_actions` (the reconciliation table as a list).
//! Transition tests drive verbs and then assert THIS payload, because the
//! action list is what both frontends render and what `require_action`
//! enforces; asserting it asserts the state machine itself.

use std::time::Duration;

use anyhow::{bail, Result};
use serde_json::Value;
use uuid::Uuid;

use crate::client::{poll_until, Dispatcher};

/// Default deadline for a status condition the rig itself provoked (a verb it
/// just fired flipping the state). Internal transitions bound legitimately.
pub const STATUS_DEADLINE: Duration = Duration::from_secs(120);
const STATUS_POLL: Duration = Duration::from_millis(300);

/// One `GET /projects/{id}/status` payload, with typed accessors over the
/// fields the transition tests assert on.
#[derive(Debug, Clone)]
pub struct ProjectStatus(pub Value);

impl ProjectStatus {
    /// The aggregate over the shared triggers (`registered` /
    /// `activating` / `active` / `inactive`).
    pub fn status(&self) -> &str {
        self.0.get("status").and_then(Value::as_str).unwrap_or("")
    }

    /// The transition marker (`none` / `building` / `cancelling_build` / `resyncing`).
    pub fn transition(&self) -> &str {
        self.0.get("transition").and_then(Value::as_str).unwrap_or("")
    }

    /// The derived infra rollup (`none` / `provisioning` / `running` /
    /// `stopping` / `terminating` / `stopped` / `partial` / `failed` / `flaky`).
    pub fn infra_rollup(&self) -> &str {
        self.0.get("infraRollup").and_then(Value::as_str).unwrap_or("")
    }

    /// Live infra rows whose node is no longer in the registered source.
    fn orphaned_infra(&self) -> bool {
        self.0.get("orphanedInfra").and_then(Value::as_bool).unwrap_or(false)
    }

    /// The program's own runs going right now (`runs.running`, a fire's
    /// phase only: the setup an infra start or an activation runs is
    /// left out), paused ones excluded.
    pub fn running_count(&self) -> usize {
        self.0
            .pointer("/runs/running")
            .and_then(Value::as_array)
            .map(|going| going.iter().filter(|run| run.get("phase").and_then(Value::as_str) == Some("fire")).count())
            .unwrap_or(0)
    }

    /// The entry of the trigger the program spells `node` (`triggers[]`).
    pub fn trigger(&self, node: &str) -> Option<&Value> {
        self.0
            .get("triggers")
            .and_then(Value::as_array)?
            .iter()
            .find(|t| t.get("node").and_then(Value::as_str) == Some(node))
    }

    /// The shared activation of `node` (`triggers[].shared`).
    pub fn trigger_copy(&self, node: &str) -> Option<&Value> {
        self.trigger(node)?.get("shared")
    }

    /// Where that activation stands (`off` / `turning_on` / `on` /
    /// `behind`), `""` when it has none.
    pub fn trigger_state(&self, node: &str) -> &str {
        self.trigger_copy(node).and_then(|c| c.get("state")).and_then(Value::as_str).unwrap_or("")
    }

    /// What that activation holds while it is down: the arrivals its door
    /// held, and the runs it fired that are paused.
    pub fn trigger_held(&self, node: &str) -> (u64, u64) {
        let held = self.trigger_copy(node).and_then(|c| c.get("held"));
        let count = |key: &str| held.and_then(|h| h.get(key)).and_then(Value::as_u64).unwrap_or(0);
        (count("arrivals"), count("paused"))
    }

    /// The infra places `node`'s cut contains (`dependsOn`): what taking
    /// one of them down takes the trigger down with.
    pub fn trigger_depends_on(&self, node: &str) -> Vec<&str> {
        self.trigger(node)
            .and_then(|t| t.get("dependsOn"))
            .and_then(Value::as_array)
            .map(|places| places.iter().filter_map(Value::as_str).collect())
            .unwrap_or_default()
    }

    /// The triggers taking the infra place `node` down takes down with it
    /// (`infra[].takesDown`, each owner's triggers), every owner's together.
    pub fn infra_takes_down(&self, node: &str) -> Vec<&str> {
        self.infra_place(node)
            .and_then(|p| p.get("takesDown"))
            .and_then(Value::as_array)
            .map(|owners| {
                owners
                    .iter()
                    .filter_map(|owned| owned.get("triggers").and_then(Value::as_array))
                    .flatten()
                    .filter_map(Value::as_str)
                    .collect()
            })
            .unwrap_or_default()
    }

    /// The entry of the infra place the program spells `node` (`infra[]`).
    fn infra_place(&self, node: &str) -> Option<&Value> {
        self.0
            .get("infra")
            .and_then(Value::as_array)?
            .iter()
            .find(|p| p.get("node").and_then(Value::as_str) == Some(node))
    }

    /// The reconciliation table's answer: the verbs the dispatcher will
    /// currently accept.
    pub fn available_actions(&self) -> Vec<&str> {
        self.0
            .get("availableActions")
            .and_then(Value::as_array)
            .map(|a| a.iter().filter_map(Value::as_str).collect())
            .unwrap_or_default()
    }

    /// Whether `verb` is currently offered.
    pub fn offers(&self, verb: &str) -> bool {
        self.available_actions().contains(&verb)
    }

    /// Assert the offered verbs are EXACTLY `expected` (order-insensitive).
    /// Set equality, not subset: a verb offered when the table says it must
    /// not be is as much a bug as a missing one.
    pub fn assert_actions_exactly(&self, expected: &[&str]) -> Result<()> {
        let mut got: Vec<&str> = self.available_actions();
        got.sort_unstable();
        let mut want: Vec<&str> = expected.to_vec();
        want.sort_unstable();
        if got != want {
            bail!(
                "available_actions mismatch: expected {want:?}, got {got:?} \
                 (status={}, transition={}, infra_rollup={}, orphaned={})",
                self.status(),
                self.transition(),
                self.infra_rollup(),
                self.orphaned_infra()
            );
        }
        Ok(())
    }
}

/// Fetch the current status snapshot.
pub async fn fetch(disp: &Dispatcher, id: &Uuid) -> Result<ProjectStatus> {
    let v: Value = disp.get_json(&format!("/projects/{id}/status")).await?;
    Ok(ProjectStatus(v))
}

/// Wait until the status snapshot satisfies `pred`, returning the satisfying
/// snapshot. `what` names the awaited condition in the timeout error.
pub async fn wait_until<F>(
    disp: &Dispatcher,
    id: &Uuid,
    what: &str,
    deadline: Duration,
    pred: F,
) -> Result<ProjectStatus>
where
    F: Fn(&ProjectStatus) -> bool,
{
    poll_until(what, deadline, STATUS_POLL, || async {
        let s = fetch(disp, id).await?;
        Ok(if pred(&s) { Some(s) } else { None })
    })
    .await
}

/// Wait until the stored lifecycle status equals `status`.
pub async fn wait_until_status(
    disp: &Dispatcher,
    id: &Uuid,
    status: &str,
    deadline: Duration,
) -> Result<ProjectStatus> {
    wait_until(
        disp,
        id,
        &format!("project status to reach '{status}'"),
        deadline,
        |s| s.status() == status,
    )
    .await
}
