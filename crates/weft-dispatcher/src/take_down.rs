//! One engine for every take-down.
//!
//! `weft deactivate` (the whole project's shared triggers, or the ones
//! `--trigger` / `--instance` name), `weft resync`'s first half, the infra
//! verbs that take triggers down with their containers, the health loop
//! parking what reads a broken container, project removal, and a program's
//! own `ctx.trigger(..).deactivate(..)` / `ctx.triggers().instance(..)`
//! calls all send the same request: a TARGET, a [`DeactivateSpec`] (what
//! happens to parked work: wipe, hibernate, park; and to running work:
//! cancel, or wait up to a cap), and the run that asked, if one did.
//!
//! [`affected_runs`] decides, in one place, which runs a target reaches:
//! the runs its triggers fired (for their instance), or every run for the
//! whole project. The run that asked is never among them: a program that
//! takes its own trigger down keeps running unless it asked to be stopped
//! too (`StopSelf::Include`), which the asking side carries out once this
//! has run, the same way `ctx.stop_tagged` does.

use axum::http::StatusCode;

use weft_core::activation::ActivationKey;
use weft_core::instance::InstanceId;
use weft_core::running_policy::{DeactivateSpec, DeactivationMode, RunningPolicy};
use weft_core::ExecutionId;

use crate::activation_store::{ActivationLifecycle, LifecycleWrite, ProjectStatus, SignalsGoing};
pub use weft_broker_client::activation::{DownWith, WentDown};
use crate::events::DispatcherEvent;
use crate::state::DispatcherState;

/// What a take-down aims at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TakeDownTarget {
    /// These activations, and the runs their triggers fired.
    Activations(Vec<ActivationKey>),
    /// Everything the project has: every activation of every owner, and
    /// every run whatever started it. Project removal.
    WholeProject,
}

/// What a take-down needs to know about one live run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunFacts {
    pub execution_id: ExecutionId,
    /// Who the run is for.
    pub instance: Option<InstanceId>,
    /// The trigger that fired it (`None` for a run started by hand).
    pub fired_by: Option<String>,
    /// Parked on a wait (`state = 'parked'`), rather than working now.
    pub suspended: bool,
}

impl RunFacts {
    /// Whether this run belongs to the activation `key`: its trigger
    /// fired it, for its owner.
    pub fn fired_by(&self, key: &ActivationKey) -> bool {
        self.fired_by.as_deref() == Some(key.trigger.as_str()) && self.instance.as_ref() == key.instance()
    }
}

/// The live runs `target` reaches, never the run that asked (`asked_by`):
/// that one's fate is its own `StopSelf`, carried out by the asker.
pub fn affected_runs<'a>(target: &TakeDownTarget, runs: &'a [RunFacts], asked_by: Option<ExecutionId>) -> Vec<&'a RunFacts> {
    runs.iter()
        .filter(|run| Some(run.execution_id) != asked_by)
        .filter(|run| match target {
            TakeDownTarget::WholeProject => true,
            TakeDownTarget::Activations(keys) => keys.iter().any(|key| run.fired_by(key)),
        })
        .collect()
}

/// The live runs an infra copy going reaches, never the run that asked.
/// Which run uses which copy is not recorded, so a shared copy's going
/// reaches every run, and an instance's copy going reaches that instance's runs
/// (another instance's never touch it).
pub fn runs_using_copies<'a>(
    copies: &weft_core::instance::Copies,
    runs: &'a [RunFacts],
    asked_by: Option<ExecutionId>,
) -> Vec<&'a RunFacts> {
    runs.iter()
        .filter(|run| Some(run.execution_id) != asked_by)
        .filter(|run| match copies {
            weft_core::instance::Copies::Instance(m) => run.instance.as_ref() == Some(m),
            weft_core::instance::Copies::Shared | weft_core::instance::Copies::Every => true,
        })
        .collect()
}

/// The lifecycle a spec lands its activations at, before any drain.
pub fn landing_lifecycle(spec: &DeactivateSpec, now_unix: i64, went_down: Option<WentDown>) -> ActivationLifecycle {
    let target = match spec.mode {
        DeactivationMode::Wipe => ActivationLifecycle::wiped(),
        DeactivationMode::Hibernate => ActivationLifecycle::hibernating(now_unix + (spec.grace_minutes as i64) * 60),
        DeactivationMode::Park => ActivationLifecycle::parked(),
    };
    let target = ActivationLifecycle { went_down, ..target };
    if spec.drains() {
        let cap = spec.drain_timeout_secs.unwrap_or(weft_broker_client::protocol::DEFAULT_DRAIN_TIMEOUT_SECS);
        ActivationLifecycle::deactivating_to(target, now_unix + cap as i64)
    } else {
        target
    }
}

/// Every run of the project that has not ended, with what a take-down
/// needs to know.
pub(crate) async fn live_runs(state: &DispatcherState, project_id: uuid::Uuid) -> anyhow::Result<Vec<RunFacts>> {
    let rows: Vec<(ExecutionId, Option<String>, Option<String>, bool)> = sqlx::query_as(
        "SELECT execution_id, instance_id, fired_by, state = 'parked' FROM run \
         WHERE project_id = $1 AND state <> 'ended' ORDER BY started_at, execution_id",
    )
    .bind(project_id)
    .fetch_all(&state.pg_pool)
    .await?;
    rows.into_iter()
        .map(|(execution_id, instance, fired_by, suspended)| {
            let instance = instance.map(InstanceId::new).transpose().map_err(|e| anyhow::anyhow!("run {execution_id} holds a broken instance: {e}"))?;
            Ok(RunFacts { execution_id, instance, fired_by, suspended })
        })
        .collect()
}

/// Take `target` down under `spec`. Returns false when the project does
/// not exist.
///
/// In order, so the gate is right from the first moment and nothing is
/// left half done: refuse a target that is mid-activation (cancel that
/// first) or a project mid-build; then wipe drops what the target's
/// activations had registered and cancels every run it reaches, while
/// hibernate and park keep the rows and the listeners keep holding them
/// (what they hear waits for the trigger: `weft_core::arrival`); `cancel` stops what is
/// running now (a parked run survives hibernate and park), `wait` leaves
/// it to finish and lands the activations once it has (the drains loop
/// cancels what is left at the cap, `crate::drain`). The lifecycle write comes before any of
/// that, guarded, so a refused write leaves nothing done.
pub async fn take_down(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    target: &TakeDownTarget,
    spec: &DeactivateSpec,
    went_down_with: Option<DownWith>,
    asked_by: Option<ExecutionId>,
) -> Result<bool, (StatusCode, String)> {
    spec.validate().map_err(|m| (StatusCode::BAD_REQUEST, m.to_string()))?;
    let internal = |what: &str, e: anyhow::Error| (StatusCode::INTERNAL_SERVER_ERROR, format!("{what}: {e:#}"));
    let Some(transition) = state.projects.transition(project_id).await.map_err(|e| internal("transition", e))? else {
        return Ok(false);
    };
    if transition.is_building() {
        return Err((
            StatusCode::CONFLICT,
            format!("cannot deactivate: project is {}; cancel the build first", transition.as_str()),
        ));
    }
    let existing = state.activations.list(project_id).await.map_err(|e| internal("activations", e))?;
    let keys: Vec<ActivationKey> = match target {
        TakeDownTarget::WholeProject => existing.iter().map(|a| a.key.clone()).collect(),
        TakeDownTarget::Activations(keys) => keys.iter().filter(|k| existing.iter().any(|a| &a.key == *k)).cloned().collect(),
    };
    if let Some(busy) = existing.iter().find(|a| keys.contains(&a.key) && a.lifecycle.status == ProjectStatus::Activating) {
        return Err((
            StatusCode::CONFLICT,
            format!("cannot deactivate: trigger {} is activating; cancel the activation first", busy.key),
        ));
    }
    // The guarded lifecycle write goes first: when it is refused (a
    // trigger began activating, a build started) or the project is gone,
    // nothing has been cancelled or unregistered yet. A wipe deletes the
    // signals in that same transaction, so the activation rows (a
    // instance's wiped row goes) and their signals leave together: a
    // failure after it leaves nothing registered for a row that is gone,
    // and the cancels below are safe to repeat. A park or a hibernate
    // keeps the rows, and the listeners keep holding them: what they hear
    // waits for the trigger to be back on (`weft_core::arrival`).
    // When it went down is the database's moment, the clock an infra
    // copy's apply is stamped with, which is what it is compared with
    // (`crate::api::infra::bring_back_triggers_whose_infra_returned`).
    let went_down = match went_down_with {
        None => None,
        Some(with) => {
            let at_unix: i64 = sqlx::query_scalar("SELECT EXTRACT(EPOCH FROM NOW())::BIGINT")
                .fetch_one(&state.pg_pool)
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("read the database's clock: {e}")))?;
            Some(WentDown { with, at_unix })
        }
    };
    let landing = landing_lifecycle(spec, crate::lease::now_unix(), went_down);
    let signals = match (spec.mode, target) {
        (DeactivationMode::Wipe, TakeDownTarget::WholeProject) => SignalsGoing::Project { except: asked_by },
        (DeactivationMode::Wipe, TakeDownTarget::Activations(_)) => SignalsGoing::Activations,
        _ => SignalsGoing::Kept,
    };
    let unlisten = match state
        .activations
        .set_lifecycle_guarded(project_id, &keys, &landing, signals)
        .await
        .map_err(|e| internal("set lifecycle", e))?
    {
        LifecycleWrite::Applied { unlisten } => unlisten,
        LifecycleWrite::NotFound => return Ok(false),
        LifecycleWrite::Rejected { blocker } => {
            return Err((StatusCode::CONFLICT, format!("cannot deactivate: {blocker}; cancel it first")));
        }
    };
    // Take the listeners' copies of the wiped signals away. A retry finds
    // the rows already gone and repeats nothing.
    if !unlisten.is_empty() {
        state.listener.unregister_many(&unlisten).await;
    }

    let runs = live_runs(state, project_id).await.map_err(|e| internal("live runs", e))?;
    let affected = affected_runs(target, &runs, asked_by);

    let user = weft_core::exec::CancelCause::User;
    if spec.mode == DeactivationMode::Wipe {
        // Every run the target reaches, parked ones too. Cancelling
        // strips a run's own waits.
        let targets: Vec<(ExecutionId, &weft_core::exec::CancelCause)> = affected.iter().map(|r| (r.execution_id, &user)).collect();
        crate::api::execution::cancel_execution_ids(state, &targets).await.map_err(|e| internal("cancel", e))?;
    } else if spec.running_policy == RunningPolicy::Cancel {
        let targets: Vec<(ExecutionId, &weft_core::exec::CancelCause)> =
            affected.iter().filter(|r| !r.suspended).map(|r| (r.execution_id, &user)).collect();
        crate::api::execution::cancel_execution_ids(state, &targets).await.map_err(|e| internal("cancel", e))?;
    }

    // Nothing of the project takes work any more: its front goes.
    crate::front::let_go_if_idle(state, project_id).await.map_err(|e| internal("let the front go", e))?;

    state.events.publish(DispatcherEvent::ProjectDeactivated { project_id }).await;
    if landing.status == ProjectStatus::Deactivating {
        // Nothing to wait for lands at once, so nobody sees a
        // Deactivating that is already over; the rest land from the
        // drains loop (`crate::drain`).
        for key in &keys {
            crate::drain::land_if_drained(state, project_id, key)
                .await
                .map_err(|e| internal("finish drain", e))?;
        }
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use weft_core::instance::Owner;

    fn run(execution_id: u128, instance: Option<&str>, fired_by: Option<&str>) -> RunFacts {
        RunFacts {
            execution_id: ExecutionId::from_u128(execution_id),
            instance: instance.map(|m| InstanceId::new(m).unwrap()),
            fired_by: fired_by.map(str::to_string),
            suspended: false,
        }
    }

    #[test]
    fn an_instances_copy_going_reaches_that_instances_runs_and_never_the_asker() {
        let runs = [run(1, Some("a"), None), run(2, Some("b"), None), run(3, None, None), run(4, Some("a"), None)];
        let a = weft_core::instance::Copies::Instance(InstanceId::new("a").unwrap());
        assert_eq!(execution_ids(runs_using_copies(&a, &runs, Some(ExecutionId::from_u128(4)))), vec![1]);
        assert_eq!(execution_ids(runs_using_copies(&weft_core::instance::Copies::Shared, &runs, None)), vec![1, 2, 3, 4]);
    }

    fn execution_ids(runs: Vec<&RunFacts>) -> Vec<u128> {
        runs.into_iter().map(|r| r.execution_id.as_u128()).collect()
    }

    fn instance(id: &str) -> Owner {
        Owner::Instance(InstanceId::new(id).unwrap())
    }

    /// An instance's trigger reaches that instance's runs of it, and nothing
    /// else: not another instance's, not the shared copy's, not a run
    /// started by hand.
    #[test]
    fn an_instances_activation_reaches_only_its_runs() {
        let runs = [
            run(1, Some("a"), Some("receive")),
            run(2, Some("b"), Some("receive")),
            run(3, None, Some("receive")),
            run(4, Some("a"), None),
            run(5, Some("a"), Some("cron")),
        ];
        let target = TakeDownTarget::Activations(vec![ActivationKey::new("receive", instance("a"))]);
        assert_eq!(execution_ids(affected_runs(&target, &runs, None)), vec![1]);
        let target = TakeDownTarget::Activations(vec![ActivationKey::new("receive", Owner::Shared)]);
        assert_eq!(execution_ids(affected_runs(&target, &runs, None)), vec![3]);
    }

    /// The whole project reaches every run, and the asker is never among
    /// the affected, whatever the target.
    #[test]
    fn the_asker_is_never_affected() {
        let runs = [run(1, Some("a"), Some("receive")), run(2, None, None)];
        assert_eq!(execution_ids(affected_runs(&TakeDownTarget::WholeProject, &runs, None)), vec![1, 2]);
        assert_eq!(execution_ids(affected_runs(&TakeDownTarget::WholeProject, &runs, Some(ExecutionId::from_u128(1)))), vec![2]);
        let target = TakeDownTarget::Activations(vec![ActivationKey::new("receive", instance("a"))]);
        assert!(affected_runs(&target, &runs, Some(ExecutionId::from_u128(1))).is_empty());
    }

    #[test]
    fn a_wait_lands_draining_with_its_cap() {
        let spec = DeactivateSpec {
            mode: DeactivationMode::Park,
            grace_minutes: 15,
            running_policy: RunningPolicy::Wait,
            drain_timeout_secs: Some(30),
        };
        let landing = landing_lifecycle(&spec, 100, None);
        assert_eq!(landing.status, ProjectStatus::Deactivating);
        assert_eq!(landing.drain_deadline_unix, Some(130));
        assert_eq!(landing.mode().as_str(), "deactivating");
        let spec = DeactivateSpec { running_policy: RunningPolicy::Cancel, ..spec };
        let health = WentDown { with: DownWith::Health, at_unix: 90 };
        assert_eq!(landing_lifecycle(&spec, 100, Some(health)).mode().as_str(), "park");
        assert_eq!(
            landing_lifecycle(&spec, 100, Some(WentDown { with: DownWith::Infra, at_unix: 90 })).went_down,
            Some(WentDown { with: DownWith::Infra, at_unix: 90 })
        );
        let hibernate = DeactivateSpec { mode: DeactivationMode::Hibernate, ..spec };
        assert_eq!(landing_lifecycle(&hibernate, 100, None).fires_deadline_unix, Some(100 + 15 * 60));
    }
}
