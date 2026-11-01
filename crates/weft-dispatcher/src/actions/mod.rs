//! Every action weft takes on a project, defined once.
//!
//! An action is one request type and one `async fn` holding every gate
//! the action needs: whose project it is, maintenance, the verb table, a
//! transition in flight, the build it was prepared against. It is reached
//! three ways and behaves the same from each: the CLI and the editor over
//! HTTP (`crate::api`, whose handlers only parse, build an
//! [`Actor::Operator`] and call), and a node's `ctx` through a program call
//! (`crate::task_kinds::program_call`, which only decodes, builds an
//! [`Actor::Program`] and calls). So the gates cannot differ by caller, and
//! the list of what a program may do is the list of actions
//! ([`names`]).

pub mod build;
pub mod infra;
pub mod instances;
pub mod project;
pub mod run_admin;
pub mod runs;
pub mod settings;
pub mod triggers;
pub mod versions;

use axum::http::StatusCode;
use weft_core::RunId;

use crate::state::DispatcherState;
use crate::tenant::TenantId;

/// How an action refuses: a status and the words the caller reads. The
/// same pair every HTTP handler answers with, so a handler passes it on
/// as it is and a program call carries the words.
pub type Refused = (StatusCode, String);

/// Who asks for an action.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Actor {
    /// A person, through the CLI or the editor, with their tenant's key.
    Operator { tenant: TenantId },
    /// A program's run, through `ctx`: the run, the project it is a run
    /// of, that project's owner, and the call it makes
    /// (`weft_core::program::program_call_id`, the same each time the run
    /// issues it again). It may act on any project of the same
    /// owner.
    Program { tenant: TenantId, project: uuid::Uuid, run: RunId, call: uuid::Uuid },
}

impl Actor {
    pub fn operator(tenant: TenantId) -> Self {
        Actor::Operator { tenant }
    }

    pub fn tenant(&self) -> &TenantId {
        match self {
            Actor::Operator { tenant } | Actor::Program { tenant, .. } => tenant,
        }
    }

    /// The run that asks, when a program does.
    pub fn run(&self) -> Option<RunId> {
        match self {
            Actor::Program { run, .. } => Some(*run),
            Actor::Operator { .. } => None,
        }
    }

    /// Whether a program asks about its own project.
    pub fn is_own_project(&self, project: uuid::Uuid) -> bool {
        matches!(self, Actor::Program { project: own, .. } if *own == project)
    }

    /// Hold the actor to `project`: it must be a project of the actor's
    /// owner. The answer for a project of another owner is the one for a
    /// project that does not exist, so nobody can probe which ids exist
    /// elsewhere ([`crate::authenticator::authorize_project`]).
    pub async fn may_act_on(&self, state: &DispatcherState, project: uuid::Uuid) -> Result<(), Refused> {
        crate::authenticator::authorize_project(state, self.tenant(), project).await
    }

    /// Who a take-down of `project` leaves alone: the asking run, when a
    /// program takes down part of its own project. A run of another
    /// project is never among what a take-down of this one reaches.
    pub fn take_down_asker(&self, project: uuid::Uuid) -> crate::take_down::TakeDownAsker {
        match self {
            Actor::Program { project: own, run, .. } if *own == project => crate::take_down::TakeDownAsker::Run(*run),
            _ => crate::take_down::TakeDownAsker::Outside,
        }
    }
}

/// Every action, by the name a program call is journaled under
/// (`weft_core::program::ProgramCall::journal_name`), and whether a
/// program may ask for it: each area's list (`<area>::NAMES`). The test
/// below holds this list and the program calls to each other, so an action
/// a program may ask for cannot lack its call, and one it may not is said
/// with the reason.
pub fn names() -> Vec<(&'static str, Reach)> {
    [infra::NAMES, triggers::NAMES, instances::NAMES, runs::NAMES, run_admin::NAMES, versions::NAMES, project::NAMES, settings::NAMES].concat()
}

/// Carry out one call a program makes on `project`, as `actor`: held to
/// the project first (one of the same owner's), then the area's action.
pub(crate) async fn program_call(
    state: &DispatcherState,
    actor: &Actor,
    project: uuid::Uuid,
    call: &weft_core::program::ProgramCall,
    stop_self: weft_core::StopSelf,
) -> Result<weft_core::program::ProgramCallOutcome, Refused> {
    use weft_core::program::ProgramCall;
    actor.may_act_on(state, project).await?;
    match call {
        ProgramCall::Infra(call) => infra::program_call(state, actor, project, call, stop_self).await,
        ProgramCall::Triggers(call) => triggers::program_call(state, actor, project, call, stop_self).await,
        ProgramCall::Instances(call) => instances::program_call(state, actor, project, call).await,
        ProgramCall::Runs(call) => runs::program_call(state, actor, project, call, stop_self).await,
        ProgramCall::Versions(call) => versions::program_call(state, actor, project, call).await,
        ProgramCall::Project(call) => self::project::program_call(state, actor, project, call).await,
        ProgramCall::Settings(call) => settings::program_call(state, actor, project, call).await,
    }
}

/// An answer that stops nobody.
pub(crate) fn answered(value: &impl serde::Serialize) -> Result<weft_core::program::ProgramCallOutcome, Refused> {
    Ok(weft_core::program::ProgramCallOutcome { value: serde_json::to_value(value).map_err(failed("answer"))?, stops_asker: false })
}

/// Cancel the asking run when its own call reaches it and it asked to be
/// stopped with the rest. Its worker is waiting for exactly this.
pub(crate) async fn stop_asker(state: &DispatcherState, actor: &Actor, stops: bool) -> Result<(), Refused> {
    if let (true, Some(asker)) = (stops, actor.run()) {
        crate::api::run::cancel_run_id(state, asker, &weft_core::run::CancelCause::User)
            .await
            .map_err(failed("cancel the asking run"))?;
    }
    Ok(())
}

/// A failure of `what`, as a refusal (`.map_err(failed("..."))`).
pub(crate) fn failed<E: std::fmt::Display>(what: &'static str) -> impl Fn(E) -> Refused {
    move |e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{what}: {e:#}"))
}

/// Who may ask for an action.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reach {
    Everyone,
    /// Only a person (through the CLI or the editor), for the reason given.
    Operator(&'static str),
}

/// A program asking for an action only a person may take.
pub fn operator_only(actor: &Actor, name: &str) -> Result<(), Refused> {
    let Some((_, Reach::Operator(why))) = names().into_iter().find(|(n, _)| *n == name) else { return Ok(()) };
    match actor {
        Actor::Operator { .. } => Ok(()),
        Actor::Program { .. } => Err((StatusCode::FORBIDDEN, format!("a program cannot ask for {name}: {why}"))),
    }
}

/// Hold the body as one tree write of `project`, under its transition
/// lock.
///
/// A tree write is a read-modify-write: read a head, record a version
/// under it, move the head there. Unserialised, two of them parent on the
/// same head and the second cuts the first out of the lineage, and a
/// prune deleting versions between a read and a write leaves a head
/// naming a version that is gone (`parent_id` carries no foreign key, so
/// nothing downstream rejects it). So every tree write holds the
/// project's lock, and the compare-and-set on a head stays as well: it
/// costs one `WHERE` clause and keeps a writer that somehow arrives
/// without the lock from clobbering silently. The writes that hold no lock
/// only take rows away (`weft clean` clearing a head's run), and the
/// compare-and-set names them as its cause.
///
/// Keep the body SHORT: the lock is a Postgres advisory lock held in a
/// transaction, so it holds a pooled connection while it runs. Anything
/// expensive (a seed fold, a journal read, storage) belongs outside it.
pub(crate) async fn with_tree_lock<T, F, Fut>(state: &DispatcherState, project: uuid::Uuid, body: F) -> Result<T, Refused>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<T, Refused>>,
{
    // The lock reports only its own failures through `anyhow`; the body's
    // own refusals ride through it untouched, so a CONFLICT from inside
    // stays a CONFLICT instead of collapsing to a 500.
    crate::lease::with_project_transition_lock(&state.lock_pool, project, || async move { Ok(body().await) })
        .await
        .map_err(|e| crate::lease::lock_answer("tree lock", e))?
}

pub(crate) fn internal(what: &str, e: impl std::fmt::Display) -> Refused {
    (StatusCode::INTERNAL_SERVER_ERROR, format!("{what}: {e:#}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every action a program may ask for has its program call, and every
    /// program call is an action here: the two lists cannot drift.
    #[test]
    fn every_action_a_program_may_ask_for_has_its_call() {
        let calls: std::collections::BTreeSet<&str> = weft_core::program::ProgramCall::journal_names().into_iter().collect();
        let everyone: std::collections::BTreeSet<&str> =
            names().into_iter().filter(|(_, reach)| *reach == Reach::Everyone).map(|(name, _)| name).collect();
        assert_eq!(calls, everyone, "a program call with no action, or an action a program may ask for with no call");
        for (name, reach) in names() {
            if let Reach::Operator(why) = reach {
                assert!(!calls.contains(name), "{name} is a person's only, yet a program call names it");
                assert!(!why.is_empty(), "{name} says why a program may not ask for it");
            }
        }
    }

    #[test]
    fn a_program_may_act_on_its_owners_projects_and_leaves_only_its_own_run_alone() {
        let own = uuid::Uuid::now_v7();
        let other = uuid::Uuid::now_v7();
        let run = uuid::Uuid::now_v7();
        let program = Actor::Program { tenant: TenantId::local(), project: own, run, call: uuid::Uuid::now_v7() };
        assert_eq!(program.take_down_asker(own), crate::take_down::TakeDownAsker::Run(run));
        assert_eq!(program.take_down_asker(other), crate::take_down::TakeDownAsker::Outside);
        assert!(program.is_own_project(own) && !program.is_own_project(other));
        assert!(operator_only(&program, "weft.working_copies.sync").is_err());
        assert!(operator_only(&Actor::operator(TenantId::local()), "weft.working_copies.sync").is_ok());
        assert!(operator_only(&program, "weft.runs.list").is_ok());
    }
}
