//! Project-transition machinery: the driver-side pieces of the
//! transitional-state model.
//!
//! The MODEL: a transitional state is a real DB value, entered via a
//! single-flight guarded CAS: an activation's `status = activating /
//! deactivating` on its `trigger_activation` row (`activation_store`),
//! or the project row's build `transition = building/cancelling_build`
//! (`project_store`). While something sits in one, every conflicting
//! verb is REJECTED instantly; the only offered action is the matching
//! cancel. This module supplies what the DRIVING process needs around that:
//!
//! - `TransitionHeartbeat` / `ActivationHeartbeat`: drop-guarded
//!   background tasks bumping the row's heartbeat so the stuck-transition
//!   reaper (`reaper::sweep_stuck_transitions`) can tell a live transition
//!   (driver bumping) from an orphaned one (driver process died).
//! - `ProjectBuildGate` + `build_version_gated`: the `building`
//!   transition around a version's build. The gate engages ONLY when
//!   an image actually has to be built (a build whose images all exist
//!   never flips the marker, so concurrent builds of an unchanged
//!   project never serialize), and relays the user's cancel request
//!   (`transition = cancelling_build`) into the builder's await loop.

use std::sync::atomic::{AtomicBool, Ordering};

use axum::http::StatusCode;

use crate::build::BuildGate;
use crate::events::DispatcherEvent;
use crate::project_store::{ProjectStore, ProjectTransition};
use crate::state::DispatcherState;

/// How often a driving process bumps the transition heartbeat, at this
/// install's pace (`weft_core::time_scale`): 10 seconds in real time.
pub fn heartbeat_interval() -> std::time::Duration {
    weft_core::time_scale::scaled(std::time::Duration::from_secs(10))
}

/// How stale a heartbeat must be before the stuck-transition reaper
/// treats the driver as dead: six bumps, 60 seconds in real time.
/// Comfortably above the bump interval so a briefly-starved driver is
/// never false-positived, while an orphaned transition is repaired
/// within about a minute.
pub fn heartbeat_stale_secs() -> i64 {
    weft_core::time_scale::scaled_secs(60)
}

/// Drop-guarded heartbeat: bumps `transition_heartbeat_unix` every
/// [`heartbeat_interval`] until dropped. Hold it for exactly the
/// window the transition is driven in-process (the activate window,
/// the build await); dropping it stops the bumps so an orphaned row
/// goes stale and the reaper repairs it.
pub struct TransitionHeartbeat {
    handle: tokio::task::JoinHandle<()>,
}

impl TransitionHeartbeat {
    pub fn spawn(projects: ProjectStore, id: uuid::Uuid) -> Self {
        let handle = tokio::spawn(async move {
            let mut tick = tokio::time::interval(heartbeat_interval());
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            // The entry CAS already stamped `now`; skip the immediate
            // first tick.
            tick.tick().await;
            loop {
                tick.tick().await;
                if let Err(e) = projects.bump_transition_heartbeat(id).await {
                    tracing::warn!(
                        target: "weft_dispatcher::transition",
                        project_id = %id,
                        error = %e,
                        "transition heartbeat bump failed; retrying next tick"
                    );
                }
            }
        });
        Self { handle }
    }
}

impl Drop for TransitionHeartbeat {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// The same drop-guarded heartbeat for an activation in flight: bumps the
/// heartbeat of every activation row the setup execution `activation` claimed.
pub struct ActivationHeartbeat {
    handle: tokio::task::JoinHandle<()>,
}

impl ActivationHeartbeat {
    pub fn spawn(activations: crate::activation_store::ActivationStore, activation: uuid::Uuid) -> Self {
        let handle = tokio::spawn(async move {
            let mut tick = tokio::time::interval(heartbeat_interval());
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            // The claim already stamped `now`; skip the immediate first tick.
            tick.tick().await;
            loop {
                tick.tick().await;
                if let Err(e) = activations.bump_heartbeat(activation).await {
                    tracing::warn!(
                        target: "weft_dispatcher::transition",
                        %activation,
                        error = %e,
                        "activation heartbeat bump failed; retrying next tick"
                    );
                }
            }
        });
        Self { handle }
    }
}

impl Drop for ActivationHeartbeat {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// Publish the transition event for a project's CURRENT row state.
/// Called after every transition flip so both frontends observe the
/// new state in near-real-time without a verb round-trip.
pub(crate) async fn publish_transition_changed(state: &DispatcherState, id: uuid::Uuid) {
    // The project's status is its shared activations' aggregate, the
    // same one `/status` answers with.
    let status = match state.activations.list(id).await {
        Ok(activations) => crate::activation_store::aggregate(
            activations
                .iter()
                .filter(|a| a.key.owner == weft_core::instance::Owner::Shared)
                .map(|a| &a.lifecycle),
        )
        .status
        .as_str()
        .to_string(),
        // A read failure: nothing useful to broadcast; the next /status
        // read is authoritative.
        Err(e) => {
            tracing::warn!(
                target: "weft_dispatcher::transition",
                project_id = %id, error = %e,
                "read lifecycle for transition event failed; event skipped"
            );
            return;
        }
    };
    let transition = match state.projects.transition(id).await {
        Ok(Some(t)) => t.as_str().to_string(),
        Ok(None) => return,
        Err(e) => {
            tracing::warn!(
                target: "weft_dispatcher::transition",
                project_id = %id, error = %e,
                "read transition for transition event failed; event skipped"
            );
            return;
        }
    };
    state
        .events
        .publish(DispatcherEvent::ProjectTransitionChanged {
            project_id: id,
            status,
            transition,
        })
        .await;
}

/// The weft impl of the builder's `BuildGate`: ties the builder's
/// "a real build is starting" knowledge to the project row's
/// `building` transition + heartbeat + the user's cancel request.
pub struct ProjectBuildGate {
    state: DispatcherState,
    id: uuid::Uuid,
    engaged: AtomicBool,
    saw_cancel: AtomicBool,
    heartbeat: std::sync::Mutex<Option<TransitionHeartbeat>>,
}

impl ProjectBuildGate {
    fn new(state: DispatcherState, id: uuid::Uuid) -> Self {
        Self {
            state,
            id,
            engaged: AtomicBool::new(false),
            saw_cancel: AtomicBool::new(false),
            heartbeat: std::sync::Mutex::new(None),
        }
    }

    /// Whether `begin` engaged the `building` transition.
    fn engaged(&self) -> bool {
        self.engaged.load(Ordering::Acquire)
    }

    /// Whether the builder observed a cancel request via this gate.
    fn saw_cancel(&self) -> bool {
        self.saw_cancel.load(Ordering::Acquire)
    }

    /// Land the transition back at rest (if engaged) and stop the
    /// heartbeat. Idempotent; safe when the reaper already cleared.
    async fn finish(&self) {
        // Stop bumping first so a failed clear goes stale and the
        // reaper repairs it, rather than us keeping a zombie fresh.
        self.heartbeat.lock().expect("heartbeat mutex").take();
        if !self.engaged() {
            return;
        }
        if let Err(e) = self.state.projects.finish_building(self.id).await {
            tracing::error!(
                target: "weft_dispatcher::transition",
                project_id = %self.id,
                error = %e,
                "finish_building failed; the stuck-transition reaper will clear the \
                 marker once the heartbeat goes stale"
            );
            return;
        }
        publish_transition_changed(&self.state, self.id).await;
    }
}

#[async_trait::async_trait]
impl BuildGate for ProjectBuildGate {
    async fn begin(&self) -> anyhow::Result<()> {
        let won = self.state.projects.try_begin_building(self.id).await?;
        if !won {
            // Name the blocker so the verb's 409 is actionable.
            let transition = self
                .state
                .projects
                .transition(self.id)
                .await?
                .unwrap_or(ProjectTransition::None);
            let activations = self.state.activations.list(self.id).await?;
            let status = crate::activation_store::aggregate(activations.iter().map(|a| &a.lifecycle))
                .status
                .as_str();
            anyhow::bail!(
                "cannot build now: project is {} (status {status}); wait for it to \
                 finish or cancel it first",
                if transition.is_building() { transition.as_str() } else { status },
            );
        }
        self.engaged.store(true, Ordering::Release);
        *self.heartbeat.lock().expect("heartbeat mutex") = Some(TransitionHeartbeat::spawn(
            self.state.projects.clone(),
            self.id,
        ));
        publish_transition_changed(&self.state, self.id).await;
        Ok(())
    }

    async fn cancel_requested(&self) -> anyhow::Result<bool> {
        let cancelling = self
            .state
            .projects
            .transition(self.id)
            .await?
            .map(|t| t == ProjectTransition::CancellingBuild)
            .unwrap_or(false);
        if cancelling {
            self.saw_cancel.store(true, Ordering::Release);
        }
        Ok(cancelling)
    }
}

/// Build one version of a project through the `building` transition:
/// the gate engages only when an image actually has to be built
/// (single-flight per project, heartbeat, cancellable through
/// `/cancel-build`). A build that loses the single-flight is a 409
/// naming the in-flight state; a user cancel is a 409 "build cancelled"
/// rather than a 500.
pub(crate) async fn build_version_gated(
    state: &DispatcherState,
    id: uuid::Uuid,
    tenant: &crate::tenant::TenantId,
    request: &weft_core::builds::VersionBuildRequest,
    hold: &crate::build::prune::ImageHold,
) -> Result<crate::build::Build, (StatusCode, String)> {
    let gate = ProjectBuildGate::new(state.clone(), id);
    let storage = crate::storage::BrokerStorage(state);
    let result = state.builder.build(&storage, id, tenant.as_str(), request, &gate, hold).await;
    gate.finish().await;
    match result {
        Ok(built) => Ok(built),
        Err(e) if gate.saw_cancel() => Err((StatusCode::CONFLICT, format!("build cancelled by user: {e}"))),
        // A lost single-flight (gate.begin refused) is a state
        // conflict, not a server fault.
        Err(e) if !gate.engaged() && format!("{e}").starts_with("cannot build now") => {
            Err((StatusCode::CONFLICT, format!("{e}")))
        }
        // `{e:#}` (alternate) prints the FULL chain so the compile
        // diagnostics (`line:col message`) reach the client, not just
        // the outermost context.
        Err(e) => Err((StatusCode::UNPROCESSABLE_ENTITY, format!("{e:#}"))),
    }
}
