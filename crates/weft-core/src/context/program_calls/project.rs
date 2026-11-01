//! `ctx.projects()` and the project-wide calls of `ctx.project(..)`: the
//! owner's projects, making one, one's status, cancelling its build, and
//! removing it.

use std::future::{Future, IntoFuture};
use std::pin::Pin;

use serde::de::DeserializeOwned;

use crate::deactivation::Following;
use crate::program::project::ProjectCall;
use crate::projects::{DeclareRequest, ProjectRemoved, ProjectStatusResponse, ProjectSummary, QuiesceRequest, Quiesced, StatusRequest};
use crate::signal::timer::{Timer, TimerSpec};
use serde_json::Value;

use crate::error::{WeftError, WeftResult};
use crate::program::ProgramCall;
use crate::tag::StopSelf;

use super::ProjectCalls;

/// How long a call following a task (a resync, a removal's wait) waits
/// between looks at it.
const FOLLOW_LOOK_AGAIN: Timer = Timer { spec: TimerSpec::After { duration_ms: 3_000 } };

/// [`RunContext::projects`](crate::context::RunContext::projects).
pub struct ProjectList<'a> {
    pub(super) at: ProjectCalls<'a>,
}

impl<'a> ProjectList<'a> {
    /// Every project of the owner, this run's own among them (`weft ps`).
    pub async fn list(self) -> WeftResult<Vec<ProjectSummary>> {
        self.at.call(ProgramCall::Project(ProjectCall::List), StopSelf::Keep).await
    }

    /// Make a project named `name`, with nothing in it and nothing built
    /// yet. Chain `.id(..)` to choose its id; answers the project, its id
    /// included.
    pub fn declare(self, name: impl Into<String>) -> Declare<'a> {
        Declare { at: self.at, name: name.into(), id: None }
    }
}

/// [`ProjectList::declare`], awaited to carry it out.
pub struct Declare<'a> {
    at: ProjectCalls<'a>,
    name: String,
    id: Option<uuid::Uuid>,
}

impl Declare<'_> {
    /// The project's id (what its `weft.toml` will say): one of the
    /// owner's projects already under it is answered as it is, and one
    /// of another owner's is refused as if it did not exist. Unset, a new
    /// id.
    pub fn id(mut self, id: uuid::Uuid) -> Self {
        self.id = Some(id);
        self
    }
}

impl<'a> IntoFuture for Declare<'a> {
    type Output = WeftResult<ProjectSummary>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            // A new id is made here: a replay reads the answer back, and a
            // call carried again is the first one (deduplicated by its
            // journal step), so one declare never makes two projects.
            let request = DeclareRequest { id: self.id.unwrap_or_else(uuid::Uuid::now_v7), name: self.name };
            self.at.call(ProgramCall::Project(ProjectCall::Declare { request }), StopSelf::Keep).await
        })
    }
}

impl<'a> ProjectCalls<'a> {
    /// The project as the owner's list has it: its name and its status.
    pub async fn get(self) -> WeftResult<ProjectSummary> {
        self.call(ProgramCall::Project(ProjectCall::Get), StopSelf::Keep).await
    }

    /// Everything about the project now (`weft status`): its triggers,
    /// its infra, its runs, its builds in flight, what is behind the
    /// registered build, and the verbs it accepts right now.
    pub async fn status(self) -> WeftResult<ProjectStatusResponse> {
        self.call(ProgramCall::Project(ProjectCall::Status { request: StatusRequest::default() }), StopSelf::Keep).await
    }

    /// Cancel the project's build in flight (`weft cancel-build`): every
    /// image build it started stops, and what was waiting on them ends
    /// cancelled. Refused when no build is in flight.
    pub async fn cancel_build(self) -> WeftResult<()> {
        self.call::<Value>(ProgramCall::Project(ProjectCall::CancelBuild), StopSelf::Keep).await.map(drop)
    }

    /// Remove the project (`weft rm`): its triggers taken down with nothing
    /// kept, its runs cancelled, its infra terminated, its stored data
    /// reclaimed, and the project gone. Chain `.force()` or `.journal()`
    /// before awaiting it. Refused on this run's own project, whose
    /// removal would cancel this run.
    pub fn remove(self) -> Remove<'a> {
        Remove { at: self, force: false, every_run: false }
    }

    /// Follow `task` in looks until it ends, parked on a timer between
    /// them, and read what it ended with. One that failed fails this with
    /// its refusal.
    pub(super) async fn follow<T: DeserializeOwned>(self, task: uuid::Uuid) -> WeftResult<T> {
        loop {
            match self.call::<Following>(ProgramCall::Project(ProjectCall::Follow { task }), StopSelf::Keep).await? {
                Following::Going(_) => {}
                Following::Done(value) => {
                    return serde_json::from_value(value)
                        .map_err(|e| WeftError::NodeFailed(format!("task {task} ended with an answer this node cannot read: {e}")))
                }
            }
            self.ctx.await_signal(FOLLOW_LOOK_AGAIN).await?;
        }
    }
}

/// [`ProjectCalls::remove`], awaited to carry it out.
pub struct Remove<'a> {
    at: ProjectCalls<'a>,
    force: bool,
    every_run: bool,
}

impl Remove<'_> {
    /// Skip the wait for its infra to terminate, and go on past what the
    /// cloud cannot remove (named in the answer) (`weft rm --force`).
    /// Workers that cannot be stopped still stop the removal.
    pub fn force(mut self) -> Self {
        self.force = true;
        self
    }

    /// Wait for every run of the project to end before erasing its
    /// history, not only for the last calls of the ones it cancels (`weft
    /// rm --journal`).
    pub fn journal(mut self) -> Self {
        self.every_run = true;
        self
    }
}

impl<'a> IntoFuture for Remove<'a> {
    type Output = WeftResult<ProjectRemoved>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let at = self.at;
            let quiesce = ProjectCall::Quiesce { request: QuiesceRequest { every_run: self.every_run } };
            let quiesced: Quiesced = at.call(ProgramCall::Project(quiesce), StopSelf::Keep).await?;
            at.follow::<Value>(quiesced.waiting).await?;
            at.call(ProgramCall::Project(ProjectCall::Remove { force: self.force }), StopSelf::Keep).await
        })
    }
}
