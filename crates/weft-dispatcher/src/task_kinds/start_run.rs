//! `start_run` task: a run a person asked for (`weft run`, the editor
//! through it), started apart from the request that asked
//! (`crate::actions::runs::start`).
//!
//! Preparing a run can take minutes: a seed's results are read from its
//! record, and every stored file they name is looked at. Done inside the
//! request, a run whose request landed on a dispatcher copy that stopped
//! was never started and never refused, and the person waited on an
//! answer that would not come. Carried by a task, it is driven under the
//! task's claim, which another dispatcher rescues; the person follows it
//! in short looks (`GET /projects/{id}/follow/{task}`), each of which says
//! what it is doing ([`weft_core::versions::RunStarting`]).
//!
//! The task's id is the one the ask carries, and the run is named after
//! it: the same ask sent again joins the task, and a claim rescued once
//! the run started answers that run as it recorded itself
//! (`crate::actions::runs::run`, which reads a run started under its id
//! back instead of starting another).

use anyhow::Result;
use async_trait::async_trait;
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use weft_core::versions::{RunStarted, StartRun};
use weft_task_store::executor::TaskExecutor;
use weft_task_store::tasks::Task;

use crate::actions::Refused;
use crate::state::DispatcherState;

/// The `start_run` task of project `project_id` carrying `ask`, to insert
/// under the ask's own id (`weft_task_store::tasks::enqueue_as`).
pub fn task(tenant: &str, project_id: uuid::Uuid, ask: &StartRun) -> Result<weft_task_store::tasks::NewTask, Refused> {
    Ok(weft_task_store::tasks::NewTask {
        kind: weft_task_store::TaskKind::StartRun,
        project_id: Some(project_id),
        dedup_key: None,
        run_id: None,
        tenant_id: tenant.to_string(),
        payload: serde_json::to_value(ask).map_err(|e| crate::actions::internal("the run's ask", e))?,
    })
}

/// How a `start_run` task ended, as its result keeps it: the run it
/// started, or why none started, as the request would have refused.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Ended {
    Started(RunStarted),
    Refused { status: u16, error: String },
}

pub struct StartRunExecutor;

#[async_trait]
impl TaskExecutor<DispatcherState> for StartRunExecutor {
    async fn execute(&self, state: &DispatcherState, task: &Task) -> Result<Value> {
        let id = task.project_id.ok_or_else(|| anyhow::anyhow!("a start_run task names no project"))?;
        let ask: StartRun = serde_json::from_value(task.payload.clone())?;
        let actor = crate::actions::Actor::operator(crate::tenant::TenantId(task.tenant_id.clone()));
        let starting = crate::actions::runs::Starting::Task { pool: &state.pg_pool, task: task.id };
        let ended = match crate::actions::runs::run(state, &actor, id, &ask.request, task.id, &starting).await {
            Ok(started) => Ended::Started(started),
            Err((status, error)) => Ended::Refused { status: status.as_u16(), error },
        };
        Ok(serde_json::to_value(ended)?)
    }
}

/// What a person following a completed `start_run` task is answered: the
/// run it started, or its refusal as the request would have refused. A
/// refusal saying the database did not take a step is answered as a 500,
/// never a 503: a 503 says the same look goes through later
/// (`weft_core::install::asks_again`), and a follower would ask an ending
/// that never changes forever. No run started, and running `weft run`
/// again starts it.
pub(crate) fn followed(result: Option<Value>) -> Result<Value, (StatusCode, String)> {
    let ended = match result.map(serde_json::from_value::<Ended>) {
        Some(Ok(ended)) => ended,
        Some(Err(e)) => return Err((StatusCode::INTERNAL_SERVER_ERROR, format!("the run's task answered something that does not read: {e}"))),
        None => return Err((StatusCode::INTERNAL_SERVER_ERROR, "the run's task answered nothing".into())),
    };
    match ended {
        Ended::Started(started) => serde_json::to_value(started).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("the run's answer: {e}"))),
        Ended::Refused { status, error } if status == StatusCode::SERVICE_UNAVAILABLE.as_u16() => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("{error}; no run started: run `weft run` again once the install answers"),
        )),
        Ended::Refused { status, error } => Err((StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR), error)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stored(ended: Ended) -> Option<Value> {
        Some(serde_json::to_value(ended).unwrap())
    }

    /// A follower reads the run its task started, or its refusal with the
    /// status the request would have answered, the resolver's structured
    /// 422 as it was; a database that did not take a step reads as a 500
    /// that says to ask again, never a 503 it would keep asking.
    #[test]
    fn a_follower_reads_the_run_or_the_refusal_as_the_request_answered() {
        let started = RunStarted { run_id: uuid::Uuid::from_u128(3), version: "v1".into(), seed: None, inherited: Vec::new(), ran: vec!["a".into()], warnings: Vec::new() };
        let answer: RunStarted = serde_json::from_value(followed(stored(Ended::Started(started.clone()))).unwrap()).unwrap();
        assert_eq!(answer, started);
        let refusal = r#"{"errors":["'a.text' needs a value"]}"#.to_string();
        assert_eq!(followed(stored(Ended::Refused { status: 422, error: refusal.clone() })).unwrap_err(), (StatusCode::UNPROCESSABLE_ENTITY, refusal));
        let (status, why) = followed(stored(Ended::Refused { status: 503, error: "the install's database was busy".into() })).unwrap_err();
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(why.starts_with("the install's database was busy; no run started: run `weft run` again"), "{why}");
        assert!(followed(None).is_err(), "a task that answered nothing");
    }
}
