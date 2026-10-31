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
//! value they had not given (`parked_fire.waits_for`, on the instance) are
//! routed again,
//! through the same drain every parked fire takes: each is held to what is
//! stored now, and parks again, naming what is still missing, if a gap is
//! left. Those fires are never retried on a timer, since only a change
//! like this one can close their gap.

use axum::http::StatusCode;

use weft_core::activation::ActivationKey;
use weft_core::instance::{InstanceId, ValueChanges};
use weft_core::instance_door::ValuesChanged;
use weft_core::run_spec::InstanceValueInput;
use weft_task_store::db::Client as _;

use crate::activation_store::{Activation, ClaimRefused, ProjectStatus};
use crate::api::access_store_error;
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
        .map_err(|e| crate::api::unavailable_or_internal("read who owns the project", e))?
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
            changes.set.entry(value.node.clone()).or_default().insert(value.field.clone(), value.value.clone());
            weft_access_store::InstanceValueWrite {
                node: value.node,
                field: value.field,
                value: value.value,
                connection: value.connection.map(|(grant_id, service)| weft_access_store::ConnectionValue { grant_id, service }),
            }
        })
        .collect();
    let cleared = changes.cleared.iter().cloned().collect();
    Ok(Checked { changes, writes, cleared })
}

/// Set `set` and clear `clear` (node, field) for `instance` of
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
        let mut heard = state.signals.subscribe();
        // The project row, locked: no activation claims a trigger until
        // this pass has decided (and, when nothing live reads the change,
        // stored it). What is stored and what is live are read under it,
        // fresh on every pass.
        let mut conn = state.pg_pool.get().await.map_err(|e| access_store_error(e.into()))?;
        let tx = conn.transaction().await.map_err(|e| access_store_error(e.into()))?;
        let locked: Option<uuid::Uuid> = tx
            .scalar_opt("SELECT id FROM project WHERE id = $1 FOR UPDATE", &[&project_id])
            .await
            .map_err(|e| access_store_error(e.into()))?;
        if locked.is_none() {
            return Err((StatusCode::NOT_FOUND, format!("project {project_id} not found")));
        }
        let stored = weft_access_store::instance_values(&tx, tenant, project_id, instance)
            .await
            .map_err(access_store_error)?;
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
            // Already stored: maybe by this same request, sent again after
            // its commit landed but its answer was lost. The fires waiting
            // on the instance are routed all the same, so none stays parked
            // behind a change whose first answer never arrived.
            drop(tx);
            drop(conn);
            route_waiting_fires(state, project_id, instance).await;
            return Ok(ValuesChanged::default());
        }
        let activations = state
            .activations
            .list(project_id)
            .await
            .map_err(|e| crate::api::unavailable_or_internal("read the project's triggers", e))?;
        let reading = triggers_reading(&project, &activations, Some(instance), &changes);
        if reading.iter().any(|a| a.lifecycle.status == ProjectStatus::Activating) {
            // One of them is being set up right now (an earlier change of
            // this instance's, an activate): this change re-arms after it,
            // on what it leaves.
            drop(tx);
            drop(conn);
            wait_for_activations(&mut heard).await?;
            continue;
        }
        let live: Vec<ActivationKey> = reading
            .into_iter()
            .filter(|a| a.lifecycle.status == ProjectStatus::Active)
            .map(|a| a.key.clone())
            .collect();
        if live.is_empty() {
            weft_access_store::change_instance_values(&tx, tenant, project_id, instance, &writes, &cleared)
                .await
                .map_err(access_store_error)?;
            tx.commit().await.map_err(|e| access_store_error(e.into()))?;
            drop(conn);
            route_waiting_fires(state, project_id, instance).await;
            return Ok(ValuesChanged::default());
        }
        drop(tx);
        drop(conn);
        let keys = live;
        let rearm = crate::api::project::Rearm {
            overlay: Some(&changes),
            store: Some(crate::activation_store::ValuesStore { tenant, instance, writes: &writes, cleared: &cleared }),
        };
        let request = weft_core::activation::ActivateRequest {
            scope: weft_core::activation::ActivationScope {
                triggers: keys.iter().map(|k| k.trigger.clone()).collect(),
                instances: weft_core::instance::InstanceTarget::of(Some(instance.clone())),
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
                wait_for_activations(&mut heard).await?
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
    let tokens = match crate::api::signal::instance_waiting_tokens(&state.pg_pool, project_id, instance).await {
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
    if let Err(e) = crate::parked_drain::drain_tokens(state, &tokens).await {
        tracing::error!(
            target: "weft_dispatcher::instance_values",
            %project_id, instance = %instance, error = %format!("{e:#}"),
            "instance values changed, but routing the fires waiting on them failed; they stay parked \
             until the instance's next change or `weft activate --instance` drains them"
        );
    }
}

/// A (node, field) an instance's value is stored under.
type FieldKey = (String, String);

/// `stored` split into the fields the program fills per instance and the
/// ones left over from fields it no longer fills, as (node, field).
fn stored_split(
    project: &weft_core::ProjectDefinition,
    stored: &weft_core::instance::InstanceValues,
) -> (Vec<FieldKey>, Vec<FieldKey>) {
    let places = weft_core::project::instance_filled_places(project);
    stored
        .iter()
        .flat_map(|(node, fields)| fields.keys().map(move |field| (node.clone(), field.clone())))
        .partition(|(node, field)| fills(&places, node, field))
}

/// Whether `field` at `at` is one the program fills per instance.
fn fills(places: &[weft_core::project::FilledPlace<'_>], at: &str, field: &str) -> bool {
    places
        .iter()
        .any(|place| place.spelled == at && weft_core::instance::instance_filled_fields(place.node).any(|(filled, _)| filled == field))
}

/// A clear must name a field the program fills per instance, or the clear
/// is a typo that would silently change nothing.
fn refuse_unknown_clears(project: &weft_core::ProjectDefinition, clear: &[(String, String)]) -> Result<(), ApiError> {
    let places = weft_core::project::instance_filled_places(project);
    for (node, field) in clear {
        if !fills(&places, node, field) {
            return Err((StatusCode::BAD_REQUEST, format!("'{node}.{field}' is no field an instance fills")));
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
    let g = weft_core::project::graph::ProjectGraph::new(project);
    let place_of = |spelled: &str| {
        let (id, path) = weft_core::project::resolve_address(project, spelled);
        weft_core::frames::Located::new(id, path)
    };
    let touched: Vec<weft_core::frames::Located> = changes.places().into_iter().map(place_of).collect();
    // Every instance's activation of one trigger shares its setup: walked
    // once per trigger.
    let mut reads: std::collections::HashMap<&str, bool> = std::collections::HashMap::new();
    activations
        .iter()
        .filter(|a| instance.is_none_or(|m| a.key.instance() == Some(m)))
        .filter(|a| {
            *reads.entry(a.key.trigger.as_str()).or_insert_with(|| {
                weft_core::project::cut::walk_setup(&g, &[place_of(&a.key.trigger)])
                    .map(|setup| touched.iter().any(|place| setup.runs.contains(place)))
                    // A trigger whose setup cannot be computed any more (the
                    // program changed under it) is re-armed with the rest, so
                    // its setup's own refusal names what is wrong.
                    .unwrap_or(true)
            })
        })
        .collect()
}

/// Until an activation moves: every change of one's status is announced
/// on `crate::holders::HELD_SIGNALS_CHANNEL` by its row's own trigger,
/// whoever wrote it, and a watch that may have missed one says so
/// (`Heard::Recheck`, `Heard::Resumed`). `heard` is subscribed before the
/// caller looked, so a change between the look and the wait wakes it.
/// One whose task ended without landing it is ended by `crate::lapses`,
/// which writes its status and so announces it.
pub(crate) async fn wait_for_activations(heard: &mut weft_task_store::pg_signal::Subscription) -> Result<(), ApiError> {
    heard
        .woken(|channel, _| channel == crate::holders::HELD_SIGNALS_CHANNEL)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("wait for the activations to move: {e:#}")))
}

