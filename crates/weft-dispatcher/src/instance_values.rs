//! What an instance provides for the fields its program writes
//! `@instance_filled`: the ONE way an instance's values change, whichever door
//! asks (the instance door, a program's `ctx.values()`, the terminal).
//!
//! A change is held to the program before anything is stored
//! (`weft_core::run_spec::check_instance_values`), then stored all at once.
//! When a live trigger of that instance reads one of the changed values
//! (the trigger's own field, or anything its setup runs through, like its
//! connection), the trigger is set up again with the new values in the
//! same call: its activation is claimed (fires that arrive meanwhile park
//! and are replayed after, born with the new values), the setup runs with
//! the change, the trigger is armed on what the setup captured, and the
//! change is stored in the transaction that lands it Active. A setup that
//! fails leaves the values and the trigger exactly as they were; a failure
//! once arming began takes the trigger down and leaves the values as they
//! were. Either way the call fails with the reason. Nobody ever re-arms by
//! hand.
//!
//! Whether a live trigger reads the change is decided and the change
//! stored under the project row's lock, the one an activation's claim
//! takes (`ActivationStoreOps::try_begin_activating`), so no activation
//! on any process arms on the values this call is replacing.
//!
//! Values are keyed by the project's owning tenant ([`owning_tenant`]),
//! whichever door the change came through.
//!
//! Once a change is stored, the instance's fires that parked waiting on a
//! value they had not given (`parked_fire.instance_gap`) are routed again,
//! through the same drain every parked fire takes: each is held to what is
//! stored now, and parks again, naming what is still missing, if a gap is
//! left. Those fires are never retried on a timer, since only a change
//! like this one can close their gap.

use axum::http::StatusCode;

use weft_core::activation::ActivationKey;
use weft_core::instance::{InstanceId, ValueChanges};
use weft_core::instance_door::ValuesChanged;
use weft_core::run_spec::InstanceValueInput;

use crate::activation_store::{Activation, ClaimRefused, ProjectStatus};
use crate::state::DispatcherState;

type ApiError = (StatusCode, String);

/// The tenant `project_id`'s instance values are keyed by: the project's
/// owner, whichever door (an instance's token, the program, the terminal)
/// reads or changes them.
pub(crate) async fn owning_tenant(state: &DispatcherState, project_id: uuid::Uuid) -> Result<String, ApiError> {
    state
        .projects
        .tenant_for(project_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("project tenant: {e:#}")))?
        .ok_or((StatusCode::NOT_FOUND, format!("project {project_id} not found")))
}

/// A change held to the program, both as the setup runs with it and as
/// the store writes it.
struct Checked {
    changes: ValueChanges,
    writes: Vec<weft_access_store::InstanceValueWrite>,
    cleared: Vec<(String, String)>,
}

/// Hold `set` to the program against what `instance` has `stored`.
fn check(
    project: &weft_core::ProjectDefinition,
    instance: &InstanceId,
    stored: &weft_core::instance::InstanceValues,
    set: &[InstanceValueInput],
    clear: &[(String, String)],
) -> Result<Checked, ApiError> {
    let checked = weft_core::run_spec::check_instance_values(project, instance, stored, set)
        .map_err(|refusal| crate::api::project::refusal_error(&refusal))?;
    let mut changes = ValueChanges { cleared: clear.iter().cloned().collect(), ..Default::default() };
    let writes: Vec<weft_access_store::InstanceValueWrite> = checked
        .into_iter()
        .map(|value| {
            changes.set.entry(value.step.clone()).or_default().insert(value.field.clone(), value.value.clone());
            weft_access_store::InstanceValueWrite {
                step: value.step,
                field: value.field,
                value: value.value,
                connection: value.connection.map(|(grant_id, service)| weft_access_store::ConnectionValue { grant_id, service }),
            }
        })
        .collect();
    let cleared = changes.cleared.iter().cloned().collect();
    Ok(Checked { changes, writes, cleared })
}

/// Set `set` and clear `clear` (step, field) for `instance` of
/// `project_id`, owned by `tenant` ([`owning_tenant`]), all or none, re-arming the instance's live triggers that
/// read any of them (see the module docs).
pub async fn change(
    state: &DispatcherState,
    tenant: &str,
    project_id: uuid::Uuid,
    instance: &InstanceId,
    set: &[InstanceValueInput],
    clear: &[(String, String)],
) -> Result<ValuesChanged, ApiError> {
    apply(state, tenant, project_id, instance, Ask::Change { set, clear }).await
}

/// Forget everything `instance` provides, as ONE change: every field the
/// program still fills per instance is cleared (re-arming the instance's live
/// triggers that read one, as any change does), and what is left stored
/// for fields the program no longer fills is cleared with it, in the same
/// write under the same lock, so a change landing meanwhile is never
/// erased behind its re-armed trigger. `tenant` as for [`change`].
pub async fn forget(
    state: &DispatcherState,
    tenant: &str,
    project_id: uuid::Uuid,
    instance: &InstanceId,
) -> Result<ValuesChanged, ApiError> {
    apply(state, tenant, project_id, instance, Ask::Forget).await
}

/// What a caller asks of an instance's values.
#[derive(Clone, Copy)]
enum Ask<'a> {
    Change { set: &'a [InstanceValueInput], clear: &'a [(String, String)] },
    /// Every stored value, read under the lock.
    Forget,
}

async fn apply(
    state: &DispatcherState,
    tenant: &str,
    project_id: uuid::Uuid,
    instance: &InstanceId,
    ask: Ask<'_>,
) -> Result<ValuesChanged, ApiError> {
    let (_, project) = crate::api::project::coherent_definition(state, project_id).await?;
    if let Ask::Change { clear, .. } = ask {
        refuse_unknown_clears(&project, clear)?;
    }

    loop {
        // Subscribed before looking, so a claim ending between the look
        // and the wait still wakes it.
        let mut events = state.events.subscribe_project(project_id).await;
        // The project row, locked: no activation claims a trigger until
        // this pass has decided (and, when nothing live reads the change,
        // stored it). What is stored and what is live are read under it,
        // fresh on every pass.
        let mut tx = state.pg_pool.begin().await.map_err(|e| access_error(e.into()))?;
        let locked: Option<(uuid::Uuid,)> = sqlx::query_as("SELECT id FROM project WHERE id = $1 FOR UPDATE")
            .bind(project_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| access_error(e.into()))?;
        if locked.is_none() {
            return Err((StatusCode::NOT_FOUND, format!("project {project_id} not found")));
        }
        let stored = weft_access_store::instance_values(&mut *tx, tenant, project_id, instance)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("read instance values: {e:#}")))?;
        // A forget clears what is stored NOW: the fields the program fills
        // count as a change (a trigger reading one re-arms), and anything
        // left from fields it no longer fills is only deleted.
        let (set, clear, leftovers) = match ask {
            Ask::Change { set, clear } => (set, clear.to_vec(), Vec::new()),
            Ask::Forget => {
                let (filled, leftovers) = stored_split(&project, &stored);
                (&[][..], filled, leftovers)
            }
        };
        let Checked { changes, writes, mut cleared } = check(&project, instance, &stored, set, &clear)?;
        cleared.extend(leftovers);
        if changes.is_empty() && cleared.is_empty() {
            return Ok(ValuesChanged::default());
        }
        let activations = state
            .activations
            .list(project_id)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("read activations: {e}")))?;
        let reading = triggers_reading(&project, &activations, Some(instance), &changes);
        if reading.iter().any(|a| a.lifecycle.status == ProjectStatus::Activating) {
            // One of them is being set up right now (an earlier change of
            // this instance's, an activate): this change re-arms after it,
            // on what it leaves.
            drop(tx);
            wait_for_activations(&mut events).await;
            continue;
        }
        let live: Vec<ActivationKey> = reading
            .into_iter()
            .filter(|a| a.lifecycle.status == ProjectStatus::Active)
            .map(|a| a.key.clone())
            .collect();
        if live.is_empty() {
            weft_access_store::change_instance_values(&mut tx, tenant, project_id, instance, &writes, &cleared)
                .await
                .map_err(access_error)?;
            tx.commit().await.map_err(|e| access_error(e.into()))?;
            route_waiting_fires(state, project_id, instance).await;
            return Ok(ValuesChanged::default());
        }
        drop(tx);
        let keys = live;
        let rearm = crate::api::project::Rearm {
            overlay: Some(&changes),
            store: Some(crate::activation_store::ValuesStore { tenant, instance, writes: &writes, cleared: &cleared }),
        };
        let request = weft_core::activation::ActivateRequest {
            scope: weft_core::activation::ActivationScope {
                triggers: keys.iter().map(|k| k.trigger.clone()).collect(),
                instance: Some(instance.clone()),
            },
            ..Default::default()
        };
        match crate::api::project::activate_with(state, project_id, request, crate::api::project::ActivateAsker::Rearm(&rearm)).await {
            Ok(_) => {
                route_waiting_fires(state, project_id, instance).await;
                return Ok(ValuesChanged { rearmed: keys.into_iter().map(|k| k.trigger).collect() });
            }
            // The claim was refused between the look above and it:
            // another claim holds one of these triggers (wait for it), or
            // one left Active (a deactivate took it down: look again, it
            // is no longer re-armed). A build is the refusal itself.
            Err(crate::api::project::ActivateError::Refused(ClaimRefused::Claimed)) => {
                wait_for_activations(&mut events).await
            }
            Err(crate::api::project::ActivateError::Refused(ClaimRefused::NotExpected)) => {}
            Err(error) => return Err(error.into()),
        }
    }
}

/// Route again every fire of `instance` parked waiting on its values,
/// now that a change of them is stored (see the module docs). The change
/// stands whatever happens here; a drain that fails leaves its fires
/// parked for the instance's next change or the trigger's next activation,
/// and says so in the log.
async fn route_waiting_fires(state: &DispatcherState, project_id: uuid::Uuid, instance: &InstanceId) {
    let tokens = match crate::api::signal::instance_gap_tokens(&state.pg_pool, project_id, instance).await {
        Ok(tokens) => tokens,
        Err(e) => {
            tracing::error!(
                target: "weft_dispatcher::instance_values",
                %project_id, instance = %instance, error = %e,
                "instance values changed, but the fires waiting on them could not be found; they stay parked \
                 until the instance's next change or `weft activate --instance` drains them"
            );
            return;
        }
    };
    for token in tokens {
        if let Err(e) = crate::parked_drain::drain_token(state, &token, crate::parked_drain::Due::Now).await {
            tracing::error!(
                target: "weft_dispatcher::instance_values",
                %project_id, instance = %instance, token = %token, error = %e,
                "instance values changed, but routing the fires waiting on them failed; they stay parked \
                 until the instance's next change or `weft activate --instance` drains them"
            );
        }
    }
}

/// A (step, field) an instance's value is stored under.
type FieldKey = (String, String);

/// `stored` split into the fields the program fills per instance and the
/// ones left over from fields it no longer fills, as (step, field).
fn stored_split(
    project: &weft_core::ProjectDefinition,
    stored: &weft_core::instance::InstanceValues,
) -> (Vec<FieldKey>, Vec<FieldKey>) {
    let places = weft_core::project::instance_filled_places(project);
    stored
        .iter()
        .flat_map(|(step, fields)| fields.keys().map(move |field| (step.clone(), field.clone())))
        .partition(|(step, field)| fills(&places, step, field))
}

/// Whether `field` at `step` is one the program fills per instance.
fn fills(places: &[(String, &weft_core::project::NodeDefinition)], step: &str, field: &str) -> bool {
    places
        .iter()
        .any(|(place, node)| place == step && weft_core::instance::instance_filled_fields(node).any(|(filled, _)| filled == field))
}

/// A clear must name a field the program fills per instance, or the clear
/// is a typo that would silently change nothing.
fn refuse_unknown_clears(project: &weft_core::ProjectDefinition, clear: &[(String, String)]) -> Result<(), ApiError> {
    let places = weft_core::project::instance_filled_places(project);
    for (step, field) in clear {
        if !fills(&places, step, field) {
            return Err((StatusCode::BAD_REQUEST, format!("'{step}.{field}' is no field an instance fills")));
        }
    }
    Ok(())
}

/// The activations whose setup reads a place `changes` touches: `instance`'s
/// alone, or (`None`, a change of the install's picks, which every owner's
/// runs read) every owner's.
pub(crate) fn triggers_reading<'a>(
    project: &weft_core::ProjectDefinition,
    activations: &'a [Activation],
    instance: Option<&InstanceId>,
    changes: &ValueChanges,
) -> Vec<&'a Activation> {
    let touched = changes.places();
    activations
        .iter()
        .filter(|a| instance.is_none_or(|m| a.key.instance() == Some(m)))
        .filter(|a| {
            let (id, path) = weft_core::project::resolve_address(project, &a.key.trigger);
            weft_core::project::selection::RunSelection::setup(project, &[weft_core::frames::Located::new(id, path)])
                .map(|setup| {
                    setup.nodes.iter().any(|place| {
                        touched.contains(weft_core::project::address_of(project, &place.id, &place.path).as_str())
                    })
                })
                // A trigger whose setup cannot be computed any more (the
                // program changed under it) is re-armed with the rest, so
                // its setup's own refusal names what is wrong.
                .unwrap_or(true)
        })
        .collect()
}

/// Until the project's activations move: every claim and every end of
/// one announces itself (`transition::publish_transition_changed`, fanned
/// out across processes), and the safety interval covers an announcement lost
/// on the way.
pub(crate) async fn wait_for_activations(events: &mut tokio::sync::broadcast::Receiver<crate::events::LiveEvent>) {
    tokio::select! {
        _ = events.recv() => {}
        _ = tokio::time::sleep(weft_task_store::drain::SAFETY_POLL_INTERVAL) => {}
    }
}

/// An access-store failure as the API answers it (a refusal the store
/// names keeps its status; anything else is a 500).
pub(crate) fn access_error(e: anyhow::Error) -> ApiError {
    let (status, message) = weft_access_store::client_error(e);
    (StatusCode::from_u16(status).expect("store status codes are valid"), message)
}
