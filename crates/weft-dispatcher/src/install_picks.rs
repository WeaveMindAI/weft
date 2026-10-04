//! The connections a program's own access nodes use on this install
//! (`weft_core::picks`): the ONE way a pick changes, whichever door asks
//! (`weft connect`, the editor's Connect button).
//!
//! A change is held to the program (`weft_core::picks::check_picks`) and
//! to the store (the connection must be the author's own, of the field's
//! service), then stored, all or none, before any trigger moves. Every
//! live trigger whose setup reads a changed pick, whoever owns it (the
//! program's own trigger or an instance's copy: every run reads the
//! install's picks), is then set up again on what is stored, each owner
//! once. A pick is the install's, not a trigger's, so it stays stored
//! when a setup fails: the answer then says so and names the triggers
//! still armed on the old connection, which `weft activate` sets up again.

use axum::http::StatusCode;

use weft_core::activation::ActivationKey;
use weft_core::instance::{Owner, ValueChanges};
use weft_core::instance_door::ValuesChanged;
use weft_core::picks::PickInput;

use crate::activation_store::{ClaimRefused, ProjectStatus};
use crate::instance_values::{access_error, owning_tenant, triggers_reading, wait_for_activations};
use crate::state::DispatcherState;

type ApiError = (StatusCode, String);

/// Pick `set` and forget `clear` (step, field) for `project_id` on this
/// install, all or none, re-arming the live triggers that read any of
/// them (see the module docs).
pub async fn change(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    set: &[PickInput],
    clear: &[(String, String)],
) -> Result<ValuesChanged, ApiError> {
    let tenant = owning_tenant(state, project_id).await?;
    // Connecting often comes before the first build: then there is no
    // program to hold a pick to, and no trigger reading one.
    let built = state
        .projects
        .running_program_identity(project_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("running program identity: {e}")))?;
    let project = match built {
        Some(_) => Some(crate::api::project::coherent_definition(state, project_id).await?.1),
        None => None,
    };
    let checked =
        weft_core::picks::check_picks(project.as_deref(), set).map_err(|refusal| crate::api::project::refusal_error(&refusal))?;
    // A clear, like a pick, is held to the program where it has the place,
    // and is spelled the way the program spells that place, which is how
    // the pick was stored.
    let mut cleared: Vec<(String, String)> = Vec::new();
    for (step, field) in clear {
        let step = match &project {
            Some(project) => {
                let (id, path) = weft_core::project::resolve_address(project, step);
                match project.nodes.iter().find(|n| n.id == id) {
                    Some(node) if !weft_core::picks::picked_fields(node).any(|f| f == field) => {
                        return Err((StatusCode::BAD_REQUEST, format!("'{step}.{field}' is no connection picked on the install")));
                    }
                    Some(_) => weft_core::project::address_of(project, &id, &path),
                    None => step.clone(),
                }
            }
            None => step.clone(),
        };
        cleared.push((step, field.clone()));
    }
    let writes: Vec<weft_access_store::PickWrite> = checked
        .iter()
        .map(|c| weft_access_store::PickWrite {
            step: c.step.clone(),
            field: c.field.clone(),
            grant_id: c.connection,
            service: c.service.clone(),
        })
        .collect();
    // Which fields change, for finding the triggers that read them.
    let mut changes = ValueChanges { cleared: cleared.iter().cloned().collect(), ..Default::default() };
    for c in &checked {
        changes.set.entry(c.step.clone()).or_default().insert(c.field.clone(), serde_json::Value::Null);
    }
    if changes.is_empty() {
        return Ok(ValuesChanged::default());
    }
    let (_, rearmed) = store_then_rearm(
        state,
        project_id,
        project.as_deref(),
        Store::Change { tenant: &tenant, writes: &writes, cleared: &cleared, changes: &changes },
    )
    .await?;
    Ok(rearmed)
}

/// Carry everything the install keeps at the place `request.from` (its
/// picks and every instance's values) to `request.to`, held first to the
/// program the install last built and to what is stored
/// (`weft_core::picks::check_move`), then re-arming the live triggers that
/// read the new place. Only ever asked for by name (`weft connect
/// --move`): a node that moved in the source leaves its picks behind, and
/// nothing guesses where it went.
pub async fn move_picks(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    request: &weft_core::picks::MovePicks,
) -> Result<weft_core::picks::PicksMoved, ApiError> {
    let tenant = owning_tenant(state, project_id).await?;
    let built = state
        .projects
        .running_program_identity(project_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("running program identity: {e}")))?;
    if built.is_none() {
        return Err((
            StatusCode::CONFLICT,
            "this program was never built on this install, so no place is known to move picks to; \
             build it first (`weft build`)"
                .into(),
        ));
    }
    let project = crate::api::project::coherent_definition(state, project_id).await?.1;
    let ((picks, instance_values), ValuesChanged { rearmed }) =
        store_then_rearm(state, project_id, Some(&project), Store::Move { tenant: &tenant, request, project: &project }).await?;
    Ok(weft_core::picks::PicksMoved { picks, instance_values, rearmed })
}

/// One write of the install's picks, done under the project lock.
enum Store<'a> {
    /// `changes` names the fields the write touches.
    Change {
        tenant: &'a str,
        writes: &'a [weft_access_store::PickWrite],
        cleared: &'a [(String, String)],
        changes: &'a ValueChanges,
    },
    /// Held to `project` and to what is stored under the lock, so two
    /// moves at once cannot both pass the check and then both write.
    Move { tenant: &'a str, request: &'a weft_core::picks::MovePicks, project: &'a weft_core::ProjectDefinition },
}

impl Store<'_> {
    /// The fields this write touches, read under the project lock: a
    /// move checks itself against what is stored now and touches every
    /// field it carries to its new place.
    async fn changes(&self, conn: &mut sqlx::PgConnection, project_id: uuid::Uuid) -> Result<ValueChanges, ApiError> {
        match self {
            Store::Change { changes, .. } => Ok((*changes).clone()),
            Store::Move { tenant, request, project } => {
                let stored = weft_access_store::stored_fields(&mut *conn, tenant, project_id).await.map_err(access_error)?;
                weft_core::picks::check_move(project, &stored, request).map_err(|why| (StatusCode::BAD_REQUEST, why))?;
                let mut changes = ValueChanges::default();
                for moved in stored.iter().filter(|s| s.step == request.from) {
                    changes.set.entry(request.to.clone()).or_default().insert(moved.field.clone(), serde_json::Value::Null);
                }
                Ok(changes)
            }
        }
    }
}

/// Store `store` once, with no trigger reading what it changes
/// mid-setup, then set up again every live trigger that reads it.
/// Answers how many picks and instance values a move carried ((0, 0)
/// for a change) and the triggers set up again.
async fn store_then_rearm(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    project: Option<&weft_core::ProjectDefinition>,
    store: Store<'_>,
) -> Result<((u64, u64), ValuesChanged), ApiError> {
    // Stored first, once: no activation claims a trigger while the
    // project row is locked, and none reading these fields is mid-setup
    // (it would land on the old picks), so every trigger found live here
    // is re-armed below on what this stores.
    let (moved, live) = loop {
        // Subscribed before looking, so a claim ending between the look
        // and the wait still wakes it.
        let mut events = state.events.subscribe_project(project_id).await;
        let mut tx = state.pg_pool.begin().await.map_err(|e| access_error(e.into()))?;
        let locked: Option<(uuid::Uuid,)> = sqlx::query_as("SELECT id FROM project WHERE id = $1 FOR UPDATE")
            .bind(project_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| access_error(e.into()))?;
        if locked.is_none() {
            return Err((StatusCode::NOT_FOUND, format!("project {project_id} not found")));
        }
        let activations = state
            .activations
            .list(project_id)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("read activations: {e}")))?;
        let changes = store.changes(&mut tx, project_id).await?;
        let reading = match project {
            Some(project) => triggers_reading(project, &activations, None, &changes),
            None => Vec::new(),
        };
        if reading.iter().any(|a| a.lifecycle.status == ProjectStatus::Activating) {
            drop(tx);
            wait_for_activations(&mut events).await;
            continue;
        }
        let moved = match &store {
            Store::Change { tenant, writes, cleared, .. } => {
                weft_access_store::change_install_picks(&mut tx, tenant, project_id, writes, cleared)
                    .await
                    .map_err(access_error)?;
                (0, 0)
            }
            Store::Move { tenant, request, .. } => {
                weft_access_store::move_stored(&mut tx, tenant, project_id, &request.from, &request.to)
                    .await
                    .map_err(access_error)?
            }
        };
        tx.commit().await.map_err(|e| access_error(e.into()))?;
        break (
            moved,
            reading
                .into_iter()
                .filter(|a| a.lifecycle.status == ProjectStatus::Active)
                .map(|a| a.key.clone())
                .collect::<Vec<ActivationKey>>(),
        );
    };
    Ok((moved, rearm(state, project_id, &live).await?))
}

/// What an activation checks of the install's picks before any trigger
/// moves (`weft_core::picks::activation_picks`): every connection the
/// program needs picked here is picked, and nothing is left under a place
/// the program moved away from. A 422 naming each gap and its fix.
pub(crate) async fn require_activation_picks(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    project: &weft_core::ProjectDefinition,
) -> Result<(), ApiError> {
    activation_picks(state, project_id, project).await?.map_err(|refusal| crate::api::project::refusal_error(&refusal))
}

/// What activating `project` would say of the install's picks: nothing, or
/// the refusal naming every gap. Activation refuses on it; `weft target
/// export` asks it ahead (`api::picks::check`), so the two never disagree.
pub(crate) async fn activation_picks(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    project: &weft_core::ProjectDefinition,
) -> Result<Result<(), weft_core::run_spec::Refusal>, ApiError> {
    let tenant = owning_tenant(state, project_id).await?;
    let picks = weft_access_store::install_picks(&state.pg_pool, &tenant, project_id).await.map_err(access_error)?;
    let stored = weft_access_store::stored_fields(&state.pg_pool, &tenant, project_id).await.map_err(access_error)?;
    Ok(weft_core::picks::activation_picks(project, &picks, &stored))
}

/// Set up `live` again on the picks now stored, each owner once. An owner
/// whose claim is busy is waited for and tried again; one whose triggers
/// are no longer expected live is done (whatever turned them off read the
/// stored picks). The triggers of an owner whose setup failed are named in
/// the answer.
async fn rearm(state: &DispatcherState, project_id: uuid::Uuid, live: &[ActivationKey]) -> Result<ValuesChanged, ApiError> {
    let mut owners: Vec<Owner> = Vec::new();
    for key in live {
        if !owners.contains(&key.owner) {
            owners.push(key.owner.clone());
        }
    }
    let mut rearmed = Vec::new();
    let mut failed: Vec<String> = Vec::new();
    for owner in owners {
        let triggers: Vec<String> = live.iter().filter(|k| k.owner == owner).map(|k| k.trigger.clone()).collect();
        // The picks are already stored: the setup reads them there.
        let rearm = crate::api::project::Rearm { overlay: None, store: None };
        loop {
            let mut events = state.events.subscribe_project(project_id).await;
            let request = weft_core::activation::ActivateRequest {
                scope: weft_core::activation::ActivationScope { triggers: triggers.clone(), instance: owner.instance().cloned() },
                ..Default::default()
            };
            match crate::api::project::activate_with(state, project_id, request, crate::api::project::ActivateAsker::Rearm(&rearm)).await {
                Ok(_) => {
                    rearmed.extend(triggers.iter().cloned());
                    break;
                }
                Err(crate::api::project::ActivateError::Refused(ClaimRefused::Claimed)) => {
                    wait_for_activations(&mut events).await;
                }
                Err(crate::api::project::ActivateError::Refused(ClaimRefused::NotExpected)) => break,
                Err(error @ (crate::api::project::ActivateError::Failed(..)
                | crate::api::project::ActivateError::Refused(ClaimRefused::Building))) => {
                    let (_, why) = <(StatusCode, String)>::from(error);
                    let whose = match owner.instance() {
                        Some(instance) => format!(" (instance {instance})"),
                        None => String::new(),
                    };
                    failed.push(format!("{}{whose}: {why}", triggers.join(", ")));
                    break;
                }
            }
        }
    }
    if failed.is_empty() {
        return Ok(ValuesChanged { rearmed });
    }
    Err((
        StatusCode::CONFLICT,
        format!(
            "the pick is stored, but these triggers are still set up with the connection they had, \
             and `weft activate` sets them up again:\n  {}",
            failed.join("\n  ")
        ),
    ))
}
