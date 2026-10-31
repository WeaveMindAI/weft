//! Project-transition machinery: the driver-side pieces of the
//! transitional-state model.
//!
//! The MODEL: a transitional state is a real DB value, entered via a
//! single-flight guarded CAS: an activation's `status = activating /
//! deactivating` on its `trigger_activation` row (`activation_store`),
//! or the project row's `transition = building/cancelling_build/resyncing`
//! (`project_store`). While something sits in one, every conflicting
//! verb is REJECTED instantly; the only offered action is the matching
//! cancel. This module supplies what the DRIVING process needs around that:
//!
//! - `TransitionHeartbeat`: a drop-guarded background task bumping the
//!   project row's heartbeat so the `lapses` loop (`crate::lapses`) can
//!   tell a live build transition (driver bumping) from an orphaned one
//!   (driver process died). An activation needs none: it is driven by a
//!   task, whose claim is what keeps it alive
//!   (`crate::activation_store::DRIVER_TASK_LIVE`).
//! - `ProjectBuildGate` + `build_version_gated`: the `building`
//!   transition around a version's build. The gate engages ONLY when
//!   an image actually has to be built (a build whose images all exist
//!   never flips the marker, so concurrent builds of an unchanged
//!   project never serialize).
//! - [`settle`]: the one way a build's transition lands back at rest.
//!
//! A resync's `resyncing` transition is held by its plan on the project
//! row, from before its take-down until its task brought every trigger
//! back or a person gave it up, each of which lands it at rest in the same
//! statement, so no build registers a version between the two halves
//! (`crate::task_kinds::resync`).
//!
//! Who holds the build transition: the request that entered it, while its
//! builds are being started (its heartbeat, which beats until the last
//! start is recorded and the version waiting on them written down, even
//! when the request itself went away), and then the project's waiting
//! version (`crate::build::waiting`), until it registers or ends. The
//! request answers as soon as its builds run, so it cannot hold the
//! transition for their length; nor can a second marker or a task that
//! outlives it, which a dispatcher that scales to zero would lose. The
//! marker is cleared by whoever sees that nothing holds it any more
//! (`crate::project_store::transition_held`): the request letting go, a
//! registration, the build loop, a cancel, and the stuck-transition
//! reaper, all through [`settle`]. The marker stays the single value every
//! guard reads (an activation's CAS, the status, the editor's cancel bar).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

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

/// How stale a heartbeat must be before the `lapses` loop
/// treats the driver as dead: six bumps, 60 seconds in real time.
/// Comfortably above the bump interval so a briefly-starved driver is
/// never false-positived, while an orphaned transition is repaired
/// within about a minute.
pub fn heartbeat_stale_secs() -> i64 {
    weft_core::time_scale::scaled_secs(60)
}

/// Drop-guarded heartbeat of a project's build transition: bumps
/// `transition_heartbeat_unix` every [`heartbeat_interval`] until dropped.
/// Held for exactly the window a request starts the builds
/// ([`ProjectBuildGate`]); dropping it stops the bumps so an orphaned row
/// goes stale and the `lapses` loop repairs it.
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

/// Land at rest every project in a transition that nothing holds any
/// more (`ProjectStoreOps::settle_transition`), `project` narrowing it to
/// one, and announce each.
pub(crate) async fn settle(state: &DispatcherState, project: Option<uuid::Uuid>) -> anyhow::Result<()> {
    let stale_before = crate::lease::now_unix() - heartbeat_stale_secs();
    for id in state.projects.settle_transition(project, stale_before).await? {
        publish_transition_changed(state, id).await;
    }
    Ok(())
}

/// The weft impl of the builder's `BuildGate`: ties the builder's
/// "a real build is starting" knowledge to the project row's
/// `building` transition and its heartbeat.
pub struct ProjectBuildGate {
    state: DispatcherState,
    id: uuid::Uuid,
    engaged: AtomicBool,
    /// Beats from `begin` until `let_go`.
    heartbeat: Mutex<Option<TransitionHeartbeat>>,
}

impl ProjectBuildGate {
    fn new(state: DispatcherState, id: uuid::Uuid) -> Self {
        Self { state, id, engaged: AtomicBool::new(false), heartbeat: Mutex::new(None) }
    }

    /// Whether `begin` engaged the `building` transition.
    fn engaged(&self) -> bool {
        self.engaged.load(Ordering::Acquire)
    }
}

#[async_trait::async_trait]
impl BuildGate for ProjectBuildGate {
    async fn begin(&self) -> anyhow::Result<()> {
        let stale_before = crate::lease::now_unix() - heartbeat_stale_secs();
        let won = self.state.projects.try_begin_building(self.id, stale_before).await?;
        if !won {
            // Name the blocker so the verb's 409 is actionable.
            let transition = self
                .state
                .projects
                .transition(self.id)
                .await?
                .unwrap_or(ProjectTransition::None);
            if transition == ProjectTransition::Resyncing {
                let why = transition.refusal().unwrap_or_default();
                return Err(BuildRefused { resyncing: true, message: format!("cannot build now: {why}") }.into());
            }
            let activations = self.state.activations.list(self.id).await?;
            let status = crate::activation_store::aggregate(activations.iter().map(|a| &a.lifecycle))
                .status
                .as_str();
            let message = format!(
                "cannot build now: project is {} (status {status}); wait for it to finish or cancel it first",
                if transition.is_building() { transition.as_str() } else { status },
            );
            return Err(BuildRefused { resyncing: false, message }.into());
        }
        self.engaged.store(true, Ordering::Release);
        // Bumped until `let_go`, which may come after this request is gone
        // (`crate::build::VersionBuilder::start_images`): the build loop
        // reads a stale heartbeat as a start nobody drives any more.
        *self.heartbeat.lock().expect("the gate's heartbeat slot is never poisoned") =
            Some(TransitionHeartbeat::spawn(self.state.projects.clone(), self.id));
        // Nothing announces a driver that died: the `lapses` loop looks
        // once the beat could have gone stale.
        crate::lapses::book_after_or_log(&self.state, heartbeat_stale_secs()).await;
        publish_transition_changed(&self.state, self.id).await;
        Ok(())
    }

    /// Stop the heartbeat and let go of the transition (if engaged): from
    /// here it rests on the project's waiting version, and lands at rest
    /// now when there is none (every image was there after all, the
    /// request failed, or a cancel ended it). Idempotent; safe when the
    /// reaper already cleared. A failed release goes stale, and the reaper
    /// settles it.
    async fn let_go(&self) {
        if !self.engaged() {
            return;
        }
        drop(self.heartbeat.lock().expect("the gate's heartbeat slot is never poisoned").take());
        let released = match self.state.projects.release_driver(self.id).await {
            Ok(()) => settle(&self.state, Some(self.id)).await,
            Err(e) => Err(e),
        };
        if let Err(e) = released {
            tracing::error!(
                target: "weft_dispatcher::transition",
                project_id = %self.id,
                error = %format!("{e:#}"),
                "letting go of the build transition failed; the lapses loop \
                 settles it once the heartbeat goes stale"
            );
        }
    }
}

/// Why the gate refused to start a build: another request holds the
/// project's transition, or its triggers are mid-flip.
#[derive(Debug)]
struct BuildRefused {
    /// A resync holds it: no ask of this build is starting builds.
    resyncing: bool,
    message: String,
}

impl std::fmt::Display for BuildRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for BuildRefused {}

/// Build one version of a project through the `building` transition:
/// the gate engages only when an image actually has to be built
/// (single-flight per project, cancellable through `/cancel-build`), and
/// is let go once the version waiting on its builds is written down,
/// leaving the transition to it. A build that loses the single-flight is a
/// 409 naming the in-flight state, marked
/// [`weft_core::builds::BUILD_BUSY_HEADER`] (a caller resending an ask
/// whose answer it lost waits and sends it again) unless a resync holds
/// the project, which no ask of a build is; a user cancel is a 409 "build
/// cancelled" rather than a 500.
pub(crate) async fn build_version_gated(
    state: &DispatcherState,
    id: uuid::Uuid,
    tenant: &crate::tenant::TenantId,
    source: &weft_core::builds::VersionSource,
    prepare: &dyn crate::build::Prepare,
    ask: i64,
    hold: crate::build::prune::ImageHold,
) -> Result<crate::build::VersionBuild, BuildFailed> {
    let gate = std::sync::Arc::new(ProjectBuildGate::new(state.clone(), id));
    let storage: std::sync::Arc<dyn crate::build::ProjectStorage> = std::sync::Arc::new(crate::storage::BrokerStorage(state.clone()));
    // The gate is let go where it was entered, with the starts
    // (`crate::build::VersionBuilder::start_images`).
    match state.builder.build(storage, id, tenant.as_str(), source, prepare, ask, gate.clone(), hold).await {
        Ok(built) => Ok(built),
        // The run the build was for could not start: answered as the run
        // itself answers it, every line, before any image was built.
        Err(e) if e.downcast_ref::<crate::build::RunRefused>().is_some() => {
            let refused = e.downcast::<crate::build::RunRefused>().expect("checked just above");
            let (status, message) = crate::api::project::refusal_error(&refused.0);
            Err(BuildFailed::of(status, message))
        }
        Err(e) if crate::build::cancelled(&e) => Err(BuildFailed::of(StatusCode::CONFLICT, format!("build cancelled by user: {e:#}"))),
        // A lost single-flight (gate.begin refused) is a state conflict,
        // not a server fault. Anything else prints the FULL chain (`{e:#}`)
        // so the compile diagnostics (`line:col message`) reach the
        // client, not just the outermost context.
        Err(e) => match e.chain().find_map(|cause| cause.downcast_ref::<BuildRefused>()) {
            Some(BuildRefused { resyncing: true, message }) => Err(BuildFailed::of(StatusCode::CONFLICT, message.clone())),
            Some(BuildRefused { resyncing: false, message }) => {
                Err(BuildFailed { status: StatusCode::CONFLICT, message: message.clone(), busy: true })
            }
            None => Err(BuildFailed::of(StatusCode::UNPROCESSABLE_ENTITY, format!("{e:#}"))),
        },
    }
}

/// Why a build of a version did not register: the status and message its
/// request answers, and whether another ask of the project was starting
/// its image builds right now (`busy`: the answer carries
/// [`weft_core::builds::BUILD_BUSY_HEADER`], so a caller resending its ask
/// waits and sends it again).
#[derive(Debug)]
pub struct BuildFailed {
    pub status: StatusCode,
    pub message: String,
    pub busy: bool,
}

impl BuildFailed {
    pub fn of(status: StatusCode, message: String) -> Self {
        Self { status, message, busy: false }
    }
}

impl axum::response::IntoResponse for BuildFailed {
    fn into_response(self) -> axum::response::Response {
        match self.busy {
            true => (self.status, [(weft_core::builds::BUILD_BUSY_HEADER, "1")], self.message).into_response(),
            false => (self.status, self.message).into_response(),
        }
    }
}
