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
//! still armed on the old connection, which turning them off and on again
//! sets up again (`weft resync` leaves them: they are not behind a build).
//!
//! The program door (`crate::api::program_door`) changes picks through
//! here too, and also forgets each connection a change moves a pick off
//! once nothing on the install uses it ([`change_forgetting`]). A live
//! trigger still set up on a connection uses it, so a connection is only
//! forgotten once every trigger reading the change was set up again.

use axum::http::StatusCode;

use weft_core::activation::ActivationKey;
use weft_core::instance::{Owner, ValueChanges};
use weft_core::instance_door::ValuesChanged;
use weft_core::picks::PickInput;
use weft_task_store::db::Client;

use crate::activation_store::{ClaimRefused, ProjectStatus};
use crate::api::access_store_error;
use crate::instance_values::{owning_tenant, triggers_reading, wait_for_activations};
use crate::state::DispatcherState;

type ApiError = (StatusCode, String);

/// Pick `set` and forget `clear` (node, field) for `project_id` on this
/// install, all or none, re-arming the live triggers that read any of
/// them (see the module docs).
pub async fn change(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    set: &[PickInput],
    clear: &[(String, String)],
) -> Result<ValuesChanged, ApiError> {
    let Some(checked) = check_change(state, project_id, set, clear).await? else {
        return Ok(ValuesChanged::default());
    };
    let (_, live) = store(state, project_id, checked.project.as_deref(), checked.store(Replaced::Kept)).await?;
    settle(state, project_id, &live).await
}

/// [`change`], then forgetting each connection the change moved a pick
/// off (a clear, or a pick of another connection) once nothing on the
/// install uses it (`weft_access_store::forget_if_unused`): the program
/// door's change, whose caller manages the program's connections and no
/// other. Forgotten only after every trigger reading the change was set up
/// again: one that could not be is still set up on the connection it had
/// (a clear of a connection the program needs, say), so that connection
/// is still in use and stays. `made` is a connection made for this change
/// (a pasted key it picks): when the change is not stored, it is forgotten
/// again, so a refused pick leaves nothing behind.
pub(crate) async fn change_forgetting(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    set: &[PickInput],
    clear: &[(String, String)],
    made: Option<uuid::Uuid>,
) -> Result<ValuesChanged, ApiError> {
    let checked = match check_change(state, project_id, set, clear).await {
        Ok(Some(checked)) => checked,
        Ok(None) => return Ok(ValuesChanged::default()),
        Err(refused) => return Err(forget_made(state, project_id, made, refused).await),
    };
    let (wrote, live) =
        match store(state, project_id, checked.project.as_deref(), checked.store(Replaced::ForgottenWhenUnused)).await {
            Ok(stored) => stored,
            Err(refused) => return Err(forget_made(state, project_id, made, refused).await),
        };
    let changed = settle(state, project_id, &live).await?;
    let forgotten = forget_unused(state, &checked.tenant, &wrote.moved_off).await?;
    Ok(ValuesChanged { forgotten, ..changed })
}

/// Forget each of `grants` that nothing on the install uses any more, in
/// one transaction; answers the ones forgotten.
async fn forget_unused(state: &DispatcherState, tenant: &str, grants: &[uuid::Uuid]) -> Result<Vec<uuid::Uuid>, ApiError> {
    if grants.is_empty() {
        return Ok(Vec::new());
    }
    let mut conn = state.pg_pool.get().await.map_err(|e| access_store_error(e.into()))?;
    let tx = conn.transaction().await.map_err(|e| access_store_error(e.into()))?;
    let mut forgotten = Vec::new();
    for grant in grants {
        // A signal set up on the connection (a run waiting on its
        // provider's events, say) uses it as surely as a pick does. The
        // signal table is this crate's, so it is asked here.
        let signalled: bool = tx
            .scalar("SELECT EXISTS (SELECT 1 FROM signal WHERE access_id = $1)", &[&grant.to_string()])
            .await
            .map_err(|e| access_store_error(e.into()))?;
        if !signalled && weft_access_store::forget_if_unused(&tx, tenant, *grant).await.map_err(access_store_error)? {
            forgotten.push(*grant);
        }
    }
    tx.commit().await.map_err(|e| access_store_error(e.into()))?;
    Ok(forgotten)
}

/// Forget `made`, the connection made for a change that was not stored,
/// when nothing else uses it ([`forget_unused`]): storing a key can land
/// on a connection the owner already had (one they connected before with
/// the same app and account), which other places may pick. Answers
/// `refused`, with the connection's id when forgetting it failed, so it
/// can be forgotten by hand.
async fn forget_made(state: &DispatcherState, project_id: uuid::Uuid, made: Option<uuid::Uuid>, refused: ApiError) -> ApiError {
    let Some(made) = made else { return refused };
    let stays = |why: String| (refused.0, format!("{}; and the connection {made} made for it stays stored ({why}): forget it with `weft connect --forget {made}`", refused.1));
    let tenant = match owning_tenant(state, project_id).await {
        Ok(tenant) => tenant,
        Err((_, why)) => return stays(why),
    };
    match forget_unused(state, &tenant, &[made]).await {
        Ok(_) => refused,
        Err((_, why)) => stays(why),
    }
}

/// A change of the install's picks held to the program, ready to store.
struct CheckedChange {
    tenant: String,
    /// The program the install last built (none before the first build).
    project: Option<std::sync::Arc<weft_core::ProjectDefinition>>,
    writes: Vec<weft_access_store::PickWrite>,
    cleared: Vec<(String, String)>,
    changes: ValueChanges,
}

impl CheckedChange {
    fn store(&self, replaced: Replaced) -> Store<'_> {
        Store::Change { tenant: &self.tenant, writes: &self.writes, cleared: &self.cleared, changes: &self.changes, replaced }
    }
}

/// Hold `set` and `clear` to the program the install last built; `None`
/// when they change nothing.
async fn check_change(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    set: &[PickInput],
    clear: &[(String, String)],
) -> Result<Option<CheckedChange>, ApiError> {
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
    // A clear, like a pick, is held to the program where it has the place
    // (a place it no longer has can still be cleared of what was stored
    // for it). A place is named the way the program spells it, which is
    // how the pick was stored, so the name is the key as given.
    let mut cleared: Vec<(String, String)> = Vec::new();
    for (place, field) in clear {
        let node = project.as_deref().and_then(|project| match weft_core::project::places::named(project, place) {
            Ok(weft_core::project::places::Named::Node(at)) => project.nodes.iter().find(|n| n.id == at.id),
            _ => None,
        });
        if node.is_some_and(|node| !weft_core::picks::picked_fields(node).any(|f| f == field)) {
            return Err((StatusCode::BAD_REQUEST, format!("'{place}.{field}' is no connection picked on the install")));
        }
        cleared.push((place.clone(), field.clone()));
    }
    let writes: Vec<weft_access_store::PickWrite> = checked
        .iter()
        .map(|c| weft_access_store::PickWrite {
            node: c.node.clone(),
            field: c.field.clone(),
            grant_id: c.connection,
            service: c.service.clone(),
        })
        .collect();
    // Which fields change, for finding the triggers that read them.
    let mut changes = ValueChanges { cleared: cleared.iter().cloned().collect(), ..Default::default() };
    for c in &checked {
        changes.set.entry(c.node.clone()).or_default().insert(c.field.clone(), serde_json::Value::Null);
    }
    if changes.is_empty() {
        return Ok(None);
    }
    Ok(Some(CheckedChange { tenant, project, writes, cleared, changes }))
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
    let (wrote, live) = store(state, project_id, Some(&project), Store::Move { tenant: &tenant, request, project: &project }).await?;
    let ValuesChanged { rearmed, .. } = settle(state, project_id, &live).await?;
    let (picks, instance_values) = wrote.moved;
    Ok(weft_core::picks::PicksMoved { picks, instance_values, rearmed })
}

/// What a change of the install's picks does to a connection it moves a
/// pick off.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Replaced {
    /// Kept: `weft connect` and the editor pick among the owner's
    /// connections, and an unpicked one is theirs to keep for later.
    Kept,
    /// Forgotten once nothing on the install uses it ([`change_forgetting`]).
    ForgottenWhenUnused,
}

/// What one write of the install's picks did: how many picks and instance
/// values a move carried, and the connections a change that forgets what
/// it leaves unused moved a pick off ([`change_forgetting`] forgets them
/// once the triggers are set up again).
#[derive(Debug, Default)]
struct Wrote {
    moved: (u64, u64),
    moved_off: Vec<uuid::Uuid>,
}

/// One write of the install's picks, done under the project lock.
enum Store<'a> {
    /// `changes` names the fields the write touches.
    Change {
        tenant: &'a str,
        writes: &'a [weft_access_store::PickWrite],
        cleared: &'a [(String, String)],
        changes: &'a ValueChanges,
        replaced: Replaced,
    },
    /// Held to `project` and to what is stored under the lock, so two
    /// moves at once cannot both pass the check and then both write.
    Move { tenant: &'a str, request: &'a weft_core::picks::MovePicks, project: &'a weft_core::ProjectDefinition },
}

impl Store<'_> {
    /// The fields this write touches, read under the project lock: a
    /// move checks itself against what is stored now and touches every
    /// field it carries to its new place.
    async fn changes(&self, db: &impl Client, project_id: uuid::Uuid) -> Result<ValueChanges, ApiError> {
        match self {
            Store::Change { changes, .. } => Ok((*changes).clone()),
            Store::Move { tenant, request, project } => {
                let stored = weft_access_store::stored_fields(db, tenant, project_id).await.map_err(access_store_error)?;
                weft_core::picks::check_move(project, &stored, request).map_err(|why| (StatusCode::BAD_REQUEST, why))?;
                let mut changes = ValueChanges::default();
                for moved in stored.iter().filter(|s| s.node == request.from) {
                    changes.set.entry(request.to.clone()).or_default().insert(moved.field.clone(), serde_json::Value::Null);
                }
                Ok(changes)
            }
        }
    }
}

/// Store `store` once, with no trigger reading what it changes
/// mid-setup. Answers what it wrote and the live triggers that read it,
/// which [`settle`] sets up again; an error means nothing was stored.
async fn store(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    project: Option<&weft_core::ProjectDefinition>,
    store: Store<'_>,
) -> Result<(Wrote, Vec<ActivationKey>), ApiError> {
    // Stored first, once: no activation claims a trigger while the
    // project row is locked, and none reading these fields is mid-setup
    // (it would land on the old picks), so every trigger found live here
    // is re-armed on what this stores.
    loop {
        // Subscribed before looking, so a claim ending between the look
        // and the wait still wakes it.
        let mut heard = state.signals.subscribe();
        let mut conn = state.pg_pool.get().await.map_err(|e| access_store_error(e.into()))?;
        let tx = conn.transaction().await.map_err(|e| access_store_error(e.into()))?;
        let locked: Option<uuid::Uuid> = tx
            .scalar_opt("SELECT id FROM project WHERE id = $1 FOR UPDATE", &[&project_id])
            .await
            .map_err(|e| access_store_error(e.into()))?;
        if locked.is_none() {
            return Err((StatusCode::NOT_FOUND, format!("project {project_id} not found")));
        }
        let activations = state
            .activations
            .list(project_id)
            .await
            .map_err(|e| crate::api::unavailable_or_internal("read the project's triggers", e))?;
        let changes = store.changes(&tx, project_id).await?;
        let reading = match project {
            Some(project) => triggers_reading(project, &activations, None, &changes),
            None => Vec::new(),
        };
        if reading.iter().any(|a| a.lifecycle.status == ProjectStatus::Activating) {
            drop(tx);
            drop(conn);
            wait_for_activations(&mut heard).await?;
            continue;
        }
        let wrote = match &store {
            Store::Change { tenant, writes, cleared, replaced, .. } => {
                // What the touched fields held before, for a change that
                // forgets what it leaves unused.
                let before = match replaced {
                    Replaced::Kept => Vec::new(),
                    Replaced::ForgottenWhenUnused => {
                        let touched: Vec<(String, String)> =
                            writes.iter().map(|w| (w.node.clone(), w.field.clone())).chain(cleared.iter().cloned()).collect();
                        weft_access_store::picked_grants(&tx, tenant, project_id, &touched).await.map_err(access_store_error)?
                    }
                };
                weft_access_store::change_install_picks(&tx, tenant, project_id, writes, cleared)
                    .await
                    .map_err(access_store_error)?;
                let moved_off = before.into_iter().filter(|grant| !writes.iter().any(|w| w.grant_id == *grant)).collect();
                Wrote { moved: (0, 0), moved_off }
            }
            Store::Move { tenant, request, .. } => Wrote {
                moved: weft_access_store::move_stored(&tx, tenant, project_id, &request.from, &request.to)
                    .await
                    .map_err(access_store_error)?,
                moved_off: Vec::new(),
            },
        };
        tx.commit().await.map_err(|e| access_store_error(e.into()))?;
        return Ok((
            wrote,
            reading.into_iter().filter(|a| a.lifecycle.status == ProjectStatus::Active).map(|a| a.key.clone()).collect(),
        ));
    }
}

/// After a write [`store`] made: set up again every live trigger that
/// reads it, and hand the events that waited on a pick over again.
async fn settle(state: &DispatcherState, project_id: uuid::Uuid, live: &[ActivationKey]) -> Result<ValuesChanged, ApiError> {
    let rearmed = rearm(state, project_id, live).await;
    // The events that waited on a pick go again on what is stored now,
    // whichever trigger they wait in (a pick a run reads re-arms nothing):
    // each waits again if what it lacks is still not picked.
    crate::parked_drain::drain_waiting_on(state, weft_core::door_fire::UnclockedWait::Picks, Some(project_id)).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("the pick is stored, but the events that waited on it could not be handed over ({e:#}); the next change of a pick hands them over"),
        )
    })?;
    rearmed
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

/// What the install keeps for `project_id`'s connections: its picks, and
/// every place it stores a pick or an instance's value at.
pub(crate) async fn kept(state: &DispatcherState, project_id: uuid::Uuid) -> Result<weft_core::picks::Kept, ApiError> {
    let tenant = owning_tenant(state, project_id).await?;
    let picks = weft_access_store::install_picks(&state.pg_pool, &tenant, project_id).await.map_err(access_store_error)?;
    let stored = weft_access_store::stored_fields(&state.pg_pool, &tenant, project_id).await.map_err(access_store_error)?;
    Ok(weft_core::picks::Kept { picks, stored })
}

/// What activating `project` would say of the install's picks: nothing, or
/// the refusal naming every gap. Activation refuses on it; `weft target
/// export` runs the same check on what [`kept`] answers, so the two never
/// disagree.
pub(crate) async fn activation_picks(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    project: &weft_core::ProjectDefinition,
) -> Result<Result<(), weft_core::run_spec::Refusal>, ApiError> {
    let weft_core::picks::Kept { picks, stored } = kept(state, project_id).await?;
    Ok(weft_core::picks::activation_picks(project, &picks, &stored))
}

/// Set up `live` again on the picks now stored, each owner once. An owner
/// whose claim is busy is waited for and tried again, and so is one whose
/// setup the database did not take (busy or out of reach): busy means
/// wait, as the resync waits on it
/// (`crate::task_kinds::resync::database_wait`), for as long as it lasts.
/// One whose triggers are no longer expected live is done (whatever turned
/// them off read the stored picks). The triggers of an owner whose setup
/// failed are named in the answer.
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
        let mut database_waits = 0;
        loop {
            let mut heard = state.signals.subscribe();
            let request = weft_core::activation::ActivateRequest {
                scope: weft_core::activation::ActivationScope::of_owner(triggers.clone(), &owner),
                ..Default::default()
            };
            match crate::api::project::activate_with(state, project_id, request, crate::api::project::ActivateAsker::Rearm(&rearm)).await {
                Ok(_) => {
                    rearmed.extend(triggers.iter().cloned());
                    break;
                }
                Err(crate::api::project::ActivateError::Refused(ClaimRefused::Claimed)) => {
                    wait_for_activations(&mut heard).await?;
                }
                Err(crate::api::project::ActivateError::Refused(ClaimRefused::NotExpected)) => break,
                Err(crate::api::project::ActivateError::Database(why)) => {
                    let wait = crate::task_kinds::resync::database_wait(database_waits);
                    database_waits += 1;
                    tracing::warn!(
                        target: "weft_dispatcher::install_picks",
                        %project_id, triggers = %triggers.join(", "), wait_secs = wait.as_secs(),
                        "setting triggers up again on a changed pick waits on the database: {why}"
                    );
                    tokio::time::sleep(wait).await;
                }
                Err(error @ (crate::api::project::ActivateError::Failed(..)
                | crate::api::project::ActivateError::Refused(
                    ClaimRefused::Building | ClaimRefused::Resyncing | ClaimRefused::ResyncGone | ClaimRefused::Removing | ClaimRefused::Maintenance,
                ))) => {
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
        return Ok(ValuesChanged { rearmed, ..Default::default() });
    }
    Err((
        StatusCode::CONFLICT,
        format!(
            "the pick is stored, but these triggers are still set up with the connection they had. \
             To set them up again, turn them off and on (`weft deactivate --trigger <name>`, then \
             `weft activate --trigger <name>`, with `--instance <id>` for an instance's); the work \
             waiting on them is kept:\n  {}",
            failed.join("\n  ")
        ),
    ))
}
