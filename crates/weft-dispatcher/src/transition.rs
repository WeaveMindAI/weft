//! Project-transition machinery: the driver-side pieces of the
//! transitional-state model.
//!
//! The MODEL (see `project_store`): a transitional state is a real DB
//! value on the project row (`status = activating/deactivating`, or
//! `transition = building/cancelling_build`), entered via a
//! single-flight guarded CAS. While a project sits in one, every
//! conflicting verb is REJECTED instantly; the only offered action is
//! the matching cancel. This module supplies what the DRIVING pod
//! needs around that:
//!
//! - `TransitionHeartbeat`: a drop-guarded background task bumping the
//!   row's `transition_heartbeat_unix` so the stuck-transition reaper
//!   (`reaper::sweep_stuck_transitions`) can tell a live transition
//!   (driver bumping) from an orphaned one (driver pod died).
//! - `ProjectBuildGate` + `ensure_built_gated`: the `building`
//!   transition around a verb's build. The gate engages
//!   ONLY when the builder actually submits a build (a cache-hit verb
//!   never flips the marker, so concurrent runs on a fresh project
//!   never serialize), and relays the user's cancel request
//!   (`transition = cancelling_build`) into the builder's await loop.

use std::sync::atomic::{AtomicBool, Ordering};

use axum::http::StatusCode;

use crate::backend::BuildGate;
use crate::events::DispatcherEvent;
use crate::project_store::{ProjectStore, ProjectTransition};
use crate::state::DispatcherState;

/// How often a driving pod bumps the transition heartbeat.
pub const HEARTBEAT_INTERVAL_SECS: u64 = 10;

/// How stale a heartbeat must be before the stuck-transition reaper
/// treats the driver as dead. Comfortably above the bump interval so
/// a briefly-starved driver is never false-positived, while an
/// orphaned transition is repaired within about a minute.
pub const HEARTBEAT_STALE_SECS: i64 = 60;

/// Drop-guarded heartbeat: bumps `transition_heartbeat_unix` every
/// `HEARTBEAT_INTERVAL_SECS` until dropped. Hold it for exactly the
/// window the transition is driven in-process (the activate window,
/// the build await); dropping it stops the bumps so an orphaned row
/// goes stale and the reaper repairs it.
pub struct TransitionHeartbeat {
    handle: tokio::task::JoinHandle<()>,
}

impl TransitionHeartbeat {
    pub fn spawn(projects: ProjectStore, id: uuid::Uuid) -> Self {
        let handle = tokio::spawn(async move {
            let mut tick =
                tokio::time::interval(std::time::Duration::from_secs(HEARTBEAT_INTERVAL_SECS));
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

/// Publish the transition event for a project's CURRENT row state.
/// Called after every transition flip so both frontends observe the
/// new state in near-real-time without a verb round-trip.
pub(crate) async fn publish_transition_changed(state: &DispatcherState, id: uuid::Uuid) {
    let status = match state.projects.lifecycle(id).await {
        Ok(Some(l)) => l.status.as_str().to_string(),
        // Row gone (removed mid-flip) or read failed: nothing useful
        // to broadcast; the next /status read is authoritative.
        Ok(None) => return,
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
            project_id: id.to_string(),
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
            let status = self
                .state
                .projects
                .lifecycle(self.id)
                .await?
                .map(|l| l.status.as_str())
                .unwrap_or("gone");
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

/// THE single build entry for every verb (run / activate / infra sync
/// all route through it, via `coherent_definition` or directly). A
/// no-op when there is no builder. Serializes real builds per project
/// through the `building` transition; a verb that loses the
/// single-flight is rejected with a 409 naming the in-flight state, and
/// a user cancel surfaces as 409 "build cancelled" rather than a 500.
pub(crate) async fn ensure_built_gated(
    state: &DispatcherState,
    id: uuid::Uuid,
) -> Result<(), (StatusCode, String)> {
    let Some(builder) = state.ensure_built.clone() else {
        return Ok(());
    };
    let gate = ProjectBuildGate::new(state.clone(), id);
    let result = builder.ensure_built(id, &gate).await;
    gate.finish().await;
    match result {
        Ok(()) => Ok(()),
        Err(e) if gate.saw_cancel() => Err((
            StatusCode::CONFLICT,
            format!("build cancelled by user: {e}"),
        )),
        // A lost single-flight (gate.begin refused) is a state
        // conflict, not a server fault.
        Err(e) if !gate.engaged() && format!("{e}").starts_with("cannot build now") => {
            Err((StatusCode::CONFLICT, format!("{e}")))
        }
        // `{e:#}` (alternate) prints the FULL chain so the underlying compile
        // diagnostics (`line:col message`) reach the client + action bar, not
        // just the outermost context.
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("build project for verb: {e:#}"),
        )),
    }
}
