//! Project status + available-actions observation.
//!
//! `GET /projects/{id}/status` is the dispatcher's single reconciliation
//! surface: the lifecycle status, the transition marker, the derived infra
//! rollup, and `available_actions` (the reconciliation table as a list).
//! Transition tests drive verbs and then assert THIS payload, because the
//! action list is what both frontends render and what `require_action`
//! enforces; asserting it asserts the state machine itself.

use std::time::Duration;

use anyhow::{bail, Context, Result};
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
    /// The stored trigger-lifecycle status (`registered` / `activating` /
    /// `active` / `deactivating` / `inactive`).
    pub fn status(&self) -> &str {
        self.0.get("status").and_then(Value::as_str).unwrap_or("")
    }

    /// The build transition marker (`none` / `building` / `cancelling_build`).
    pub fn transition(&self) -> &str {
        self.0.get("transition").and_then(Value::as_str).unwrap_or("")
    }

    /// The derived infra rollup (`none` / `provisioning` / `running` /
    /// `stopping` / `terminating` / `stopped` / `partial` / `failed` / `flaky`).
    pub fn infra_rollup(&self) -> &str {
        self.0.get("infra_rollup").and_then(Value::as_str).unwrap_or("")
    }

    /// Whether the REGISTERED definition declares infra (the source fact the
    /// dispatcher enforces with; refreshed on every register).
    pub fn has_infra(&self) -> bool {
        self.0.get("has_infra").and_then(Value::as_bool).unwrap_or(false)
    }

    /// Live infra rows whose node is no longer in the registered source.
    pub fn orphaned_infra(&self) -> bool {
        self.0.get("orphaned_infra").and_then(Value::as_bool).unwrap_or(false)
    }

    /// Running (non-suspended, non-terminal) executions.
    pub fn running_count(&self) -> u64 {
        self.0.get("running_count").and_then(Value::as_u64).unwrap_or(0)
    }

    /// The reconciliation table's answer: the verbs the dispatcher will
    /// currently accept.
    pub fn available_actions(&self) -> Vec<&str> {
        self.0
            .get("available_actions")
            .and_then(Value::as_array)
            .map(|a| a.iter().filter_map(Value::as_str).collect())
            .unwrap_or_default()
    }

    /// Whether `verb` is currently offered.
    pub fn offers(&self, verb: &str) -> bool {
        self.available_actions().iter().any(|v| *v == verb)
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

/// Wait until `verb` appears in `available_actions`.
pub async fn wait_until_action(
    disp: &Dispatcher,
    id: &Uuid,
    verb: &str,
    deadline: Duration,
) -> Result<ProjectStatus> {
    wait_until(
        disp,
        id,
        &format!("project to offer '{verb}'"),
        deadline,
        |s| s.offers(verb),
    )
    .await
    .with_context(|| format!("project never offered '{verb}'"))
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
