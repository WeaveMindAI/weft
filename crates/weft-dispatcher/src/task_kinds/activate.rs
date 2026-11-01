//! `activate` task: an activation of a project's triggers, carried out
//! apart from whoever asked for it.
//!
//! An activation waits on its setup run, which may wait days on a person,
//! and whoever asks may go away meanwhile: a person's `weft activate`
//! whose request lands on a dispatcher copy that stops, weft bringing back
//! triggers whose infra returned from a loop's pass. Done inside the
//! request, an activation whose process went away stayed `activating`,
//! holding every arrival of its triggers, until a repair noticed. Carried
//! by a task, it is driven under the task's claim: a claimant that dies
//! has its claim rescued by another dispatcher, whose run of the task
//! finds the rows the first claimed naming the task
//! (`crate::activation_store::Activation::driver_task`) and carries the
//! activation on from where they say it is
//! (`crate::api::project::plan_activation`).
//!
//! A person's activate answers its task at once and is followed in short
//! looks (`GET /projects/{id}/follow/{task}`, [`followed`]), each of which
//! says where the activation is ([`progress`]). Anything else that asks
//! outside a task's claim waits on the task's end in its own process
//! ([`through_task`]). A program's call and a resync activate under the
//! claim of the task they already run in, and never come here. The
//! maintenance lift's bring-back always comes here, one task per owner,
//! whether the lift's own task or a resync's request brings it back
//! (`crate::maintenance`).

use anyhow::Result;
use async_trait::async_trait;
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use weft_core::activation::{ActivateRequest, ActivateResponse};
use weft_task_store::executor::TaskExecutor;
use weft_task_store::tasks::Task;

use crate::activation_store::ClaimRefused;
use crate::api::project::{ActivateAsker, ActivateError, Rearm};
use crate::state::DispatcherState;

/// What an `activate` task carries: the request, and who asked.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ActivatePayload {
    pub request: ActivateRequest,
    pub asker: TaskAsker,
}

/// Who asked for an activation carried by its own task: the askers of
/// [`ActivateAsker`] that ask from outside any task.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "by", rename_all = "snake_case")]
pub enum TaskAsker {
    /// A person (`weft activate`, the editor): `ActivateAsker::Outside`,
    /// with the port its request names checked first
    /// (`crate::actions::triggers::activate_checked`).
    Outside,
    /// `ActivateAsker::InfraBack`.
    InfraBack,
    /// `ActivateAsker::Rearm`: the instance's change it re-arms with, or
    /// none when what is stored already is the answer (a pick change).
    Rearm { change: Option<InstanceChange> },
    /// `ActivateAsker::Lift`: bringing back what a maintenance left down.
    Lift,
}

/// An instance's change of values a re-arm sets its triggers up with and
/// stores as they land (`crate::api::project::Rearm`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InstanceChange {
    pub overlay: weft_core::instance::ValueChanges,
    pub tenant: String,
    pub instance: weft_core::instance::InstanceId,
    pub writes: Vec<weft_access_store::InstanceValueWrite>,
    pub cleared: Vec<(String, String)>,
}

impl TaskAsker {
    /// `asker` as a task carries it. A resync's drive only ever activates
    /// under the claim of its own task, so one here is weft's own bug,
    /// said as such.
    fn of(asker: &ActivateAsker<'_>) -> Result<Self, ActivateError> {
        Ok(match asker {
            ActivateAsker::Outside => TaskAsker::Outside,
            ActivateAsker::InfraBack => TaskAsker::InfraBack,
            ActivateAsker::Rearm(rearm) => TaskAsker::Rearm {
                change: match (rearm.overlay, &rearm.store) {
                    (Some(overlay), Some(store)) => Some(InstanceChange {
                        overlay: overlay.clone(),
                        tenant: store.tenant.to_string(),
                        instance: store.instance.clone(),
                        writes: store.writes.to_vec(),
                        cleared: store.cleared.to_vec(),
                    }),
                    (None, None) => None,
                    _ => {
                        return Err(ActivateError::Failed(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "a re-arm names an instance's change to set its triggers up with but not to store, or the other way round"
                                .into(),
                        ))
                    }
                },
            },
            ActivateAsker::Lift => TaskAsker::Lift,
            ActivateAsker::Resync(_) => {
                return Err(ActivateError::Failed(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "a resync activated outside the task that drives it; this is a bug in weft".into(),
                ))
            }
        })
    }
}

/// The `activate` task of project `project_id` carrying `payload`. Never
/// deduplicated: two activates of the same triggers race for their claim,
/// and the one that loses is refused as it always was.
pub fn task(tenant: &str, project_id: uuid::Uuid, payload: &ActivatePayload) -> weft_task_store::tasks::NewTask {
    weft_task_store::tasks::NewTask {
        kind: weft_task_store::TaskKind::Activate,
        project_id: Some(project_id),
        dedup_key: None,
        run_id: None,
        tenant_id: tenant.to_string(),
        payload: serde_json::to_value(payload).expect("an activate payload serializes"),
    }
}

/// How an `activate` task ended, as its result keeps it: what the
/// activation answered, or why it did not activate
/// (`crate::api::project::ActivateError`), which whoever waits on the task
/// reads back as it was.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Ended {
    Activated(ActivateResponse),
    Refused(ClaimRefused),
    Failed { status: u16, error: String },
    Database(String),
}

impl From<Result<ActivateResponse, ActivateError>> for Ended {
    fn from(ended: Result<ActivateResponse, ActivateError>) -> Self {
        match ended {
            Ok(answer) => Ended::Activated(answer),
            Err(ActivateError::Refused(refused)) => Ended::Refused(refused),
            Err(ActivateError::Failed(status, error)) => Ended::Failed { status: status.as_u16(), error },
            Err(ActivateError::Database(error)) => Ended::Database(error),
        }
    }
}

impl From<Ended> for Result<ActivateResponse, ActivateError> {
    fn from(ended: Ended) -> Self {
        match ended {
            Ended::Activated(answer) => Ok(answer),
            Ended::Refused(refused) => Err(ActivateError::Refused(refused)),
            Ended::Failed { status, error } => {
                Err(ActivateError::Failed(StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR), error))
            }
            Ended::Database(error) => Err(ActivateError::Database(error)),
        }
    }
}

pub struct ActivateExecutor;

#[async_trait]
impl TaskExecutor<DispatcherState> for ActivateExecutor {
    async fn execute(&self, state: &DispatcherState, task: &Task) -> Result<Value> {
        let id = task.project_id.ok_or_else(|| anyhow::anyhow!("an activate task names no project"))?;
        let ActivatePayload { request, asker } = serde_json::from_value(task.payload.clone())?;
        let ended = match asker {
            TaskAsker::Outside => crate::actions::triggers::activate_checked(state, id, request).await,
            TaskAsker::InfraBack => crate::api::project::activate_with(state, id, request, ActivateAsker::InfraBack).await.map(|answer| answer.0),
            TaskAsker::Rearm { change } => {
                let store = change.as_ref().map(|change| crate::activation_store::ValuesStore {
                    tenant: &change.tenant,
                    instance: &change.instance,
                    writes: &change.writes,
                    cleared: &change.cleared,
                });
                let rearm = Rearm { overlay: change.as_ref().map(|change| &change.overlay), store };
                crate::api::project::activate_with(state, id, request, ActivateAsker::Rearm(&rearm)).await.map(|answer| answer.0)
            }
            TaskAsker::Lift => crate::api::project::activate_with(state, id, request, ActivateAsker::Lift).await.map(|answer| answer.0),
        };
        Ok(serde_json::to_value(Ended::from(ended))?)
    }
}

/// How long one wait on an `activate` task's end lasts before its row is
/// read again ([`through_task`]): the wait has no end of its own, an
/// activation waiting on a person for as long as they take.
const LOOK: std::time::Duration = std::time::Duration::from_secs(60);

/// Activate as `asker` asks, outside any task's claim: hand it to an
/// `activate` task, and wait for that task's end, however long it takes,
/// answering what it answered. The activation goes on whatever becomes of
/// this wait.
pub(crate) async fn through_task(
    state: &DispatcherState,
    id: uuid::Uuid,
    request: ActivateRequest,
    asker: &ActivateAsker<'_>,
) -> Result<ActivateResponse, ActivateError> {
    let asker = TaskAsker::of(asker)?;
    let tenant = state
        .projects
        .tenant_for(id)
        .await
        .map_err(|e| ActivateError::failed("the project's tenant", e))?
        .ok_or_else(|| ActivateError::Failed(StatusCode::NOT_FOUND, "project not found".into()))?;
    let task = weft_task_store::tasks::enqueue(&state.pg_pool, self::task(&tenant, id, &ActivatePayload { request, asker }))
        .await
        .map_err(|e| ActivateError::failed("hand the activation to its task", e))?;
    loop {
        let outcome = weft_task_store::terminal::wait_for_terminal_in_project(&state.pg_pool, &state.signals, task, id, LOOK)
            .await
            .map_err(|e| ActivateError::failed("wait for the activation's task", e))?
            .ok_or_else(|| ActivateError::Failed(StatusCode::INTERNAL_SERVER_ERROR, format!("the activation's task {task} is gone")))?;
        match outcome.status {
            weft_core::task::TaskStatus::Pending | weft_core::task::TaskStatus::Claimed => continue,
            weft_core::task::TaskStatus::Complete => return ended(outcome.result).into(),
            weft_core::task::TaskStatus::Failed => {
                return Err(ActivateError::Failed(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    outcome.error.unwrap_or_else(|| "the activation's task ended without saying why".into()),
                ))
            }
        }
    }
}

/// The ending a completed `activate` task's `result` keeps.
fn ended(result: Option<Value>) -> Ended {
    match result.map(serde_json::from_value::<Ended>) {
        Some(Ok(ended)) => ended,
        Some(Err(e)) => Ended::Failed { status: 500, error: format!("the activation's task answered something that does not read: {e}") },
        None => Ended::Failed { status: 500, error: "the activation's task answered nothing".into() },
    }
}

/// What a person following a completed `activate` task is answered: the
/// activation's answer, or its refusal as the request would have refused.
/// A step the database did not take is answered as a 500, never a 503: a
/// 503 says the same look goes through later
/// (`weft_core::install::asks_again`), and a follower would ask an ending
/// that never changes forever. The activation itself is over, and `weft
/// activate` asks again.
pub(crate) fn followed(result: Option<Value>) -> Result<Value, (StatusCode, String)> {
    let answer: Result<ActivateResponse, ActivateError> = ended(result).into();
    match answer {
        Ok(answer) => serde_json::to_value(answer).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("the activation's answer: {e}"))),
        Err(ActivateError::Database(error)) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("{error}; nothing of the activation is left, so `weft activate` tries it again once the database answers"),
        )),
        Err(error) => Err(error.into()),
    }
}

/// Where the activation task `task` of project `id` drives is, for whoever
/// follows it: the activation it drives right now, since when and what it
/// waits on (`weft_core::infra::wire::ChangeProgress`), read off the rows
/// it claimed; `None` while it drives none (before its claim, between two
/// owners, after it landed).
pub(crate) async fn progress(state: &DispatcherState, id: uuid::Uuid, task: uuid::Uuid) -> anyhow::Result<Option<Value>> {
    let rows = state.activations.list(id).await?;
    let now = crate::lease::now_unix();
    let driving = rows
        .iter()
        .filter(|a| a.driver_task == Some(task))
        .find_map(|a| Some((a.activating?, a.lifecycle.activating_run_id?)));
    Ok(match driving {
        Some((activating, run)) => Some(serde_json::to_value(weft_core::infra::wire::ChangeProgress {
            since_unix: activating.since_unix,
            as_of_unix: now,
            waiting: Some(activating.step.waiting(run)),
        })?),
        None => None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// What an activation answered, or why it did not activate, reads
    /// back as it was through the task's result, the claim's refusal
    /// included, which the callers that wait on the task tell apart.
    #[test]
    fn an_ending_reads_back_as_the_activation_answered() {
        let round = |ended: Result<ActivateResponse, ActivateError>| -> Result<ActivateResponse, ActivateError> {
            let stored = serde_json::to_value(Ended::from(ended)).unwrap();
            ended_of(stored)
        };
        assert!(matches!(round(Err(ActivateError::Refused(ClaimRefused::Claimed))), Err(ActivateError::Refused(ClaimRefused::Claimed))));
        assert!(matches!(round(Err(ActivateError::Database("busy".into()))), Err(ActivateError::Database(why)) if why == "busy"));
        let failed = round(Err(ActivateError::Failed(StatusCode::PRECONDITION_FAILED, "no triggers".into())));
        assert!(matches!(failed, Err(ActivateError::Failed(StatusCode::PRECONDITION_FAILED, why)) if why == "no triggers"));
        let answer = ActivateResponse { per_instance_left_out: vec!["inbox".into()], ..Default::default() };
        assert_eq!(round(Ok(answer)).unwrap().per_instance_left_out, ["inbox"]);
    }

    fn ended_of(stored: Value) -> Result<ActivateResponse, ActivateError> {
        ended(Some(stored)).into()
    }

    /// A follower reads an ending the database did not take as a 500 that
    /// says to ask again, never a 503 it would keep asking.
    #[test]
    fn a_follower_never_reads_a_503_for_an_ending() {
        let stored = serde_json::to_value(Ended::Database("the install's database was busy".into())).unwrap();
        let (status, why) = followed(Some(stored)).unwrap_err();
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(why.contains("`weft activate` tries it again"), "{why}");
        let refused = serde_json::to_value(Ended::Refused(ClaimRefused::Claimed)).unwrap();
        assert_eq!(followed(Some(refused)).unwrap_err().0, StatusCode::CONFLICT);
        assert!(followed(None).is_err(), "a task that answered nothing");
    }

    /// The payload a task carries reads back as it was written, a re-arm's
    /// change included.
    #[test]
    fn a_payload_reads_back_as_written() {
        let change = InstanceChange {
            overlay: weft_core::instance::ValueChanges {
                set: [("read".to_string(), [("url".to_string(), serde_json::json!("https://a"))].into_iter().collect())].into_iter().collect(),
                cleared: [("read".to_string(), "token".to_string())].into_iter().collect(),
            },
            tenant: "local".into(),
            instance: weft_core::instance::InstanceId::new("ada").unwrap(),
            writes: Vec::new(),
            cleared: vec![("read".into(), "token".into())],
        };
        for asker in [TaskAsker::Outside, TaskAsker::InfraBack, TaskAsker::Rearm { change: None }, TaskAsker::Rearm { change: Some(change) }] {
            let payload = ActivatePayload { request: ActivateRequest { port: Some(8080), ..Default::default() }, asker };
            let read: ActivatePayload = serde_json::from_value(serde_json::to_value(&payload).unwrap()).unwrap();
            assert_eq!(read, payload);
        }
    }
}
