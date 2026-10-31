//! What a program reaches beyond its own run: its infra copies, its
//! triggers, what its instances provide, its instances' connections, the
//! list of its instances, its costs, its runs and its instance tokens, on
//! its own project or, through `ctx.project(id)`, on any project of the
//! same owner. Each builder here ends in one call, carried to the
//! dispatcher as the calling run (`crate::program::ProgramCall`).
//!
//! Every answer is journaled (`ctx.run`), so a body replayed after a
//! durable wait reads it back instead of asking again: a program that
//! terminated a copy does not terminate it twice. A call that takes
//! something down never reaches the run that made it, unless it passes
//! `StopSelf::Include` and the run is among what it reaches; the call then
//! does not return, and the run ends with the rest.
//!
//! An instance is named by the id the program chose for it (`"user-42"`);
//! inside its own project a program may name any instance, since it is the
//! author's code. What an outsider may do is decided at the doors (an
//! instance token, the `Weft-Instance` header), never here.

mod infra;
mod instances;
mod project;
mod runs;
mod settings;
mod triggers;
mod versions;

pub use infra::{AllInfraCalls, InfraCalls};
pub use instances::{ConnectionCalls, InstanceCalls, InstanceConnections, InstanceTokens, InstanceValueCalls, TokenCalls, ValueCalls};
pub use project::{Declare, ProjectList, Remove};
pub use settings::{FrontendAdd, FrontendCalls, OneFrontend, PickCalls, PickChange, WorkerCalls};
pub use runs::{CostQuery, RunByIdCalls, RunEventsQuery, RunQuery};
pub use triggers::{Activate, Resync, TriggerCalls};
pub use versions::{BuildCalls, CodeCalls, FileEdit, RunCalls};

use serde::de::DeserializeOwned;

use crate::error::{WeftError, WeftResult};
use crate::instance::InstanceId;
use crate::program::runs::RunCall;
use crate::program::{ProgramCall, ResolvedRun};
use crate::tag::StopSelf;
use crate::RunId;

use super::RunContext;

impl RunContext {
    /// Calls on another project of the same owner, named by its id: every
    /// builder this run has on its own project, and its code
    /// ([`ProjectCalls::code`]: write its files, build it, run it).
    pub fn project(&self, id: uuid::Uuid) -> ProjectCalls<'_> {
        ProjectCalls { ctx: self, project: Some(id) }
    }

    /// Calls on this run's own project.
    pub fn own_project(&self) -> ProjectCalls<'_> {
        ProjectCalls { ctx: self, project: None }
    }

    /// The projects of this run's owner: listing them, and making a new
    /// one (whose files are then written through `ctx.project(id)`).
    pub fn projects(&self) -> ProjectList<'_> {
        ProjectList { at: self.own_project() }
    }

    /// See [`ProjectCalls::code`]: this run's own project's code (a run
    /// of it is refused: every run would start the next).
    pub fn code(&self) -> CodeCalls<'_> {
        self.own_project().code()
    }

    /// See [`ProjectCalls::infra`]: on this run's own project.
    pub fn infra(&self, node: impl Into<String>) -> InfraCalls<'_> {
        self.own_project().infra(node)
    }
    /// See [`ProjectCalls::all_infra`]: on this run's own project.
    pub fn all_infra(&self) -> AllInfraCalls<'_> {
        self.own_project().all_infra()
    }
    /// See [`ProjectCalls::trigger`]: on this run's own project.
    pub fn trigger(&self, node: impl Into<String>) -> TriggerCalls<'_> {
        self.own_project().trigger(node)
    }
    /// See [`ProjectCalls::triggers`]: on this run's own project.
    pub fn triggers(&self) -> TriggerCalls<'_> {
        self.own_project().triggers()
    }
    /// See [`ProjectCalls::values`]: on this run's own project.
    pub fn values(&self) -> ValueCalls<'_> {
        self.own_project().values()
    }
    /// See [`ProjectCalls::connections`]: on this run's own project.
    pub fn connections(&self) -> ConnectionCalls<'_> {
        self.own_project().connections()
    }
    /// See [`ProjectCalls::instances`]: on this run's own project.
    pub fn instances(&self) -> InstanceCalls<'_> {
        self.own_project().instances()
    }
    /// See [`ProjectCalls::costs`]: on this run's own project.
    pub fn costs(&self) -> CostQuery<'_> {
        self.own_project().costs()
    }
    /// See [`ProjectCalls::runs`]: on this run's own project.
    pub fn runs(&self) -> RunQuery<'_> {
        self.own_project().runs()
    }
    /// One run, named by its id: stopped, woken, read or deleted, as `weft
    /// stop`, `weft wake`, `weft logs`, `weft events`, `weft freeze` and
    /// `weft clean <run>` do. Any run of any project of the same owner,
    /// this one included (`self.run_id`), and a run whose project was
    /// removed since: a run is held to its owner, never to a project.
    pub fn run_by_id(&self, run: RunId) -> RunByIdCalls<'_> {
        RunByIdCalls { at: self.own_project(), run }
    }
    /// The one run of the owner's whose id starts with `prefix` (at least
    /// four hex characters), the way `weft logs abcd` finds it. Refused
    /// when nothing matches, and when several do, naming them.
    pub async fn resolve_run(&self, prefix: impl Into<String>) -> WeftResult<RunId> {
        let call = ProgramCall::Runs(RunCall::Resolve { prefix: prefix.into() });
        let answer: ResolvedRun = self.own_project().call(call, StopSelf::Keep).await?;
        Ok(answer.run_id)
    }
    /// See [`ProjectCalls::tokens`]: on this run's own project.
    pub fn tokens(&self) -> TokenCalls<'_> {
        self.own_project().tokens()
    }
    /// See [`ProjectCalls::frontends`]: on this run's own project.
    pub fn frontends(&self) -> FrontendCalls<'_> {
        self.own_project().frontends()
    }
    /// See [`ProjectCalls::workers`]: on this run's own project.
    pub fn workers(&self) -> WorkerCalls<'_> {
        self.own_project().workers()
    }
    /// See [`ProjectCalls::picks`]: on this run's own project.
    pub fn picks(&self) -> PickCalls<'_> {
        self.own_project().picks()
    }
}

/// Calls on one project: this run's own ([`RunContext::own_project`]) or
/// another of the same owner ([`RunContext::project`]).
#[derive(Clone, Copy)]
pub struct ProjectCalls<'a> {
    pub(super) ctx: &'a RunContext,
    /// `None`: this run's own.
    pub(super) project: Option<uuid::Uuid>,
}

impl<'a> ProjectCalls<'a> {
    /// Carry one call as this run, journaled so a replay reads the
    /// answer back, and decode its answer. The call is keyed on its
    /// journal step, which a replay reaches at the same index, so a call
    /// carried again (its answer lost) is the same call.
    pub(super) async fn call<T: DeserializeOwned>(self, call: ProgramCall, stop_self: StopSelf) -> WeftResult<T> {
        let name = call.journal_name();
        let (call_index, replayed) = self.ctx.handle.run_step(name).await?;
        let value = match replayed {
            Some(value) => value,
            None => {
                let value = self.ctx.handle.program_call(call, stop_self, call_index, self.project).await?;
                self.ctx.handle.run_record(name, call_index, &value).await?;
                value
            }
        };
        serde_json::from_value(value)
            .map_err(|e| {
                WeftError::NodeFailed(format!(
                    "{name}: the runtime answered a shape this node cannot read ({e}), which happens when the program \
                     was built by an older weft, or this run was started by one: run `weft resync` and start the run again"
                ))
            })
    }
}

/// An instance id as the program wrote it, checked when a call is made so
/// a builder chain stays a chain.
pub(super) fn instance_id(raw: &Option<String>) -> WeftResult<Option<InstanceId>> {
    raw.as_deref()
        .map(|m| InstanceId::new(m).map_err(|e| WeftError::Input(format!("instance id: {e}"))))
        .transpose()
}

pub(super) fn required_instance(raw: &str) -> WeftResult<InstanceId> {
    InstanceId::new(raw).map_err(|e| WeftError::Input(format!("instance id: {e}")))
}

