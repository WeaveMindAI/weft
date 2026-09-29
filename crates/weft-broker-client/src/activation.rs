//! One trigger activation's lifecycle, and several as one.
//!
//! The dispatcher owns the rows (`trigger_activation`); the broker reads
//! them for the supervisor's health loop. Both answer "what state is
//! this project's listening in" with [`aggregate`], so the two can
//! never disagree.

use crate::protocol::ProjectStatus;

/// One activation's lifecycle: status plus the three axes the fire gate,
/// the consumer enumeration and the reaper read.
///
///   accepting_fires:            the gate passes or parks a fire when
///                               true, refuses it when false.
///   fires_visible_to_consumers: a token's enumeration shows the
///                               signals when true, hides them when false.
///   fires_deadline_unix:        accepting only until then.
///
/// The user-facing modes map onto them: wipe = refusing and hidden (the
/// signals are gone too), hibernate = parking until a deadline and hidden,
/// park = parking and visible, active = live and visible.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivationLifecycle {
    pub status: ProjectStatus,
    pub accepting_fires: bool,
    pub fires_visible_to_consumers: bool,
    pub fires_deadline_unix: Option<i64>,
    /// While `status = Deactivating` with `runningPolicy = wait`: the unix
    /// second past which the drain gives up, cancels what is still running,
    /// and lands Inactive (enforced by the stuck-transition reaper).
    pub drain_deadline_unix: Option<i64>,
    /// True iff the CURRENT deactivation was the health loop parking this
    /// activation because the infra it reads broke. Its auto-recover
    /// reactivates only such an activation, never one a person took down.
    pub deactivated_by_health: bool,
    /// The trigger-setup run of the activation in flight, reserved before
    /// setup is queued; every write the activation makes is guarded by it.
    /// `None` outside Activating.
    pub activating_execution_id: Option<uuid::Uuid>,
}

impl ActivationLifecycle {
    /// Never activated: what an absent row means.
    pub fn registered() -> Self {
        Self { status: ProjectStatus::Registered, ..Self::active() }
    }

    /// Live: fires execute immediately.
    pub fn active() -> Self {
        Self {
            status: ProjectStatus::Active,
            accepting_fires: true,
            fires_visible_to_consumers: true,
            fires_deadline_unix: None,
            drain_deadline_unix: None,
            deactivated_by_health: false,
            activating_execution_id: None,
        }
    }

    /// While setup runs: fires park (the listener may not have every
    /// signal yet) and consumers see nothing; the drain at the end of the
    /// activation replays what parked.
    pub fn activating(execution_id: uuid::Uuid) -> Self {
        Self {
            status: ProjectStatus::Activating,
            fires_visible_to_consumers: false,
            activating_execution_id: Some(execution_id),
            ..Self::active()
        }
    }

    /// Wipe: every signal and run is gone, the gate refuses.
    pub fn wiped() -> Self {
        Self {
            status: ProjectStatus::Inactive,
            accepting_fires: false,
            fires_visible_to_consumers: false,
            ..Self::active()
        }
    }

    /// Hibernate: parking until the deadline, then refusing; hidden the
    /// whole time.
    pub fn hibernating(deadline_unix: i64) -> Self {
        Self {
            status: ProjectStatus::Inactive,
            fires_visible_to_consumers: false,
            fires_deadline_unix: Some(deadline_unix),
            ..Self::active()
        }
    }

    /// Park: parking forever, visible so consumers can browse and submit.
    pub fn parked() -> Self {
        Self { status: ProjectStatus::Inactive, ..Self::active() }
    }

    /// While running work drains: the gate already behaves like the target
    /// from the first write; only `status` waits for the drain.
    pub fn deactivating_to(target: ActivationLifecycle, drain_deadline_unix: i64) -> Self {
        Self {
            status: ProjectStatus::Deactivating,
            drain_deadline_unix: Some(drain_deadline_unix),
            activating_execution_id: None,
            ..target
        }
    }

    /// Where the activation stands, as a person reads it: `registered`,
    /// `activating`, `active`, `deactivating`, or for an inactive
    /// activation how it went down: `wipe`, `hibernate`, `park`.
    pub fn mode(&self) -> weft_core::activation::ActivationMode {
        use weft_core::activation::ActivationMode;
        use weft_core::DeactivationMode;
        match self.status {
            ProjectStatus::Registered => ActivationMode::Registered,
            ProjectStatus::Activating => ActivationMode::Activating,
            ProjectStatus::Active => ActivationMode::Active,
            ProjectStatus::Deactivating => ActivationMode::Deactivating,
            ProjectStatus::Inactive => ActivationMode::Down(if !self.accepting_fires {
                DeactivationMode::Wipe
            } else if !self.fires_visible_to_consumers {
                DeactivationMode::Hibernate
            } else {
                DeactivationMode::Park
            }),
        }
    }
}

/// Several activations as one lifecycle: what a project shows for its
/// shared triggers, and what a verb aimed at several triggers is checked
/// against. Transitional states win (a person sees the verb working and
/// is offered its cancel): activating, then deactivating. Then anything
/// listening makes it active. All-inactive keeps the most that survived
/// (accepting if any accepts, visible if any is, the latest deadline), so
/// a project with anything parked is offered the reactivate choice. None
/// at all is registered.
pub fn aggregate<'a>(lifecycles: impl IntoIterator<Item = &'a ActivationLifecycle>) -> ActivationLifecycle {
    let all: Vec<&ActivationLifecycle> = lifecycles.into_iter().collect();
    let with = |status: ProjectStatus| all.iter().copied().filter(move |l| l.status == status);
    if let Some(first) = with(ProjectStatus::Activating).next() {
        return first.clone();
    }
    if with(ProjectStatus::Deactivating).next().is_some() {
        let draining: Vec<&ActivationLifecycle> = with(ProjectStatus::Deactivating).collect();
        return ActivationLifecycle {
            status: ProjectStatus::Deactivating,
            accepting_fires: draining.iter().any(|l| l.accepting_fires),
            fires_visible_to_consumers: draining.iter().any(|l| l.fires_visible_to_consumers),
            fires_deadline_unix: draining.iter().filter_map(|l| l.fires_deadline_unix).max(),
            drain_deadline_unix: draining.iter().filter_map(|l| l.drain_deadline_unix).max(),
            deactivated_by_health: draining.iter().all(|l| l.deactivated_by_health),
            activating_execution_id: None,
        };
    }
    if with(ProjectStatus::Active).next().is_some() {
        return ActivationLifecycle::active();
    }
    let inactive: Vec<&ActivationLifecycle> = with(ProjectStatus::Inactive).collect();
    if inactive.is_empty() {
        return ActivationLifecycle::registered();
    }
    ActivationLifecycle {
        status: ProjectStatus::Inactive,
        accepting_fires: inactive.iter().any(|l| l.accepting_fires),
        fires_visible_to_consumers: inactive.iter().any(|l| l.fires_visible_to_consumers),
        fires_deadline_unix: inactive.iter().filter_map(|l| l.fires_deadline_unix).max(),
        drain_deadline_unix: None,
        deactivated_by_health: inactive.iter().all(|l| l.deactivated_by_health),
        activating_execution_id: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nothing_activated_is_registered() {
        assert_eq!(aggregate([]).status, ProjectStatus::Registered);
    }

    #[test]
    fn a_transition_wins_the_aggregate() {
        let execution_id = uuid::Uuid::new_v4();
        let all = [ActivationLifecycle::active(), ActivationLifecycle::activating(execution_id), ActivationLifecycle::parked()];
        assert_eq!(aggregate(&all).activating_execution_id, Some(execution_id));
        let draining = ActivationLifecycle::deactivating_to(ActivationLifecycle::parked(), 50);
        let all = [ActivationLifecycle::active(), draining.clone()];
        let agg = aggregate(&all);
        assert_eq!(agg.status, ProjectStatus::Deactivating);
        assert_eq!(agg.drain_deadline_unix, Some(50));
    }

    #[test]
    fn anything_listening_is_active() {
        let all = [ActivationLifecycle::wiped(), ActivationLifecycle::active()];
        assert_eq!(aggregate(&all).mode().as_str(), "active");
    }

    #[test]
    fn all_down_keeps_the_most_that_survived() {
        let all = [ActivationLifecycle::wiped(), ActivationLifecycle::hibernating(10), ActivationLifecycle::parked()];
        assert_eq!(aggregate(&all).mode().as_str(), "park");
        let all = [ActivationLifecycle::wiped(), ActivationLifecycle::hibernating(10)];
        let agg = aggregate(&all);
        assert_eq!(agg.mode().as_str(), "hibernate");
        assert_eq!(agg.fires_deadline_unix, Some(10));
        assert_eq!(aggregate(&[ActivationLifecycle::wiped()]).mode().as_str(), "wipe");
    }


}
