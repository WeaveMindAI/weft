//! Background loop that claims dispatcher-owned lifecycle commands
//! (`deactivate` / `reactivate`, and a person's `upgrade`) and runs
//! them, each on its own task with its claim renewed while it runs (an
//! upgrade can wait on a drain for hours). The complementary
//! supervisor-owned verbs (`apply` / `stop` / `terminate`) are
//! claimed by the pooled supervisor process that owns the project, via the
//! broker.
//!
//! Why a separate loop rather than reusing the supervisor's claim
//! path: trigger-state deactivate/reactivate touches the signal
//! table, which only the dispatcher has Postgres write authority
//! for. Trying to fold this into the supervisor would require
//! granting the supervisor signal-write access, breaking the
//! tenant-scoping invariant.
//!
//! Concurrency: every dispatcher runs one of these loops;
//! `FOR UPDATE SKIP LOCKED` in the claim SQL keeps them from
//! double-claiming.
//!
//! Wake mechanism: every command row announces itself on
//! `INFRA_COMMAND_CHANNEL` (`issued:<project>`) from its own trigger when
//! it commits, whoever wrote it. The claim loop wakes on each and drains
//! everything pending; a long safety tick catches a lost notification
//! and a dead claimer's lapsed lease, which nothing announces.

use anyhow::Result;
use sqlx::PgPool;
use weft_broker_client::lifecycle_command::ISSUED_WAKE;

use crate::infra_lifecycle_command::InfraLifecycleVerb;
use weft_task_store::drain::{DrainLoop, DrainStep, WakeOn, SAFETY_POLL_INTERVAL};
use crate::state::DispatcherState;

/// A command being issued, for any project: the claimer serves them all.
pub(crate) static WAKE_ON: &[WakeOn] = &[ISSUED_WAKE];

pub fn drain_loop(state: DispatcherState) -> DrainLoop {
    DrainLoop::new("lifecycle_claimer", WAKE_ON, SAFETY_POLL_INTERVAL, move || {
        let state = state.clone();
        async move {
            match claim_and_run_one(&state).await? {
                true => Ok(DrainStep::More),
                false => Ok(DrainStep::Done),
            }
        }
    })
}

/// Three terminal outcomes a dispatcher-claimed verb can produce.
/// Maps 1:1 to the `LifecycleOutcome` wire enum the broker stores
/// in the `outcome` column. The Cancelled variant is the load-
/// bearing one: it lets `wait_for_command` consumers distinguish
/// "the verb errored" (Failed) from "the verb was no longer
/// applicable" (Cancelled, e.g. project removed mid-flight) and
/// render the right UX instead of a spurious failure.
enum RunOutcome {
    Succeeded,
    Failed(String),
    Cancelled(String),
}

/// Returns true when a command was claimed. The claimed command runs
/// on a task of its own, so a long one (an upgrade waiting on a drain)
/// never holds up the next claim.
///
/// Contract: if `claim_one` returns a claimed row, EXACTLY ONE
/// `complete()` write follows, by this process while it still holds the
/// claim. The handler returns a typed `RunOutcome` so "no longer
/// applicable" cancellations are distinguished from real failures.
async fn claim_and_run_one(state: &DispatcherState) -> Result<bool> {
    let Some(row) = claim_one(&state.pg_pool, &state.replica).await? else {
        return Ok(false);
    };
    let state = state.clone();
    tokio::spawn(async move { run_and_complete(&state, row).await });
    Ok(true)
}

/// Run one claimed command to its outcome and record it, renewing the
/// claim meanwhile. When the claim is lost (this process could not renew it
/// for a whole lease and another process took the command over), the run
/// stops here and the new holder answers for it.
async fn run_and_complete(state: &DispatcherState, row: ClaimedCommand) {
    let replica = state.replica.as_str();
    let outcome = tokio::select! {
        outcome = run_claimed(state, &row) => outcome.unwrap_or_else(|e| RunOutcome::Failed(format!("{e:#}"))),
        () = hold_claim(&state.pg_pool, row.id, replica) => {
            tracing::warn!(
                target: "weft_dispatcher::lifecycle_claimer",
                command_id = row.id,
                verb = %row.verb,
                "the claim on this command was taken over by another replica; it answers for it now"
            );
            return;
        }
    };
    if let Err(e) = complete(&state.pg_pool, row.id, replica, &outcome).await {
        tracing::error!(
            target: "weft_dispatcher::lifecycle_claimer",
            command_id = row.id,
            verb = %row.verb,
            error = %format!("{e:#}"),
            "the command ran but its outcome could not be recorded; the claim lapses and another replica runs it again"
        );
        return;
    }
    match &outcome {
        RunOutcome::Succeeded => {}
        RunOutcome::Failed(error) => tracing::warn!(
            target: "weft_dispatcher::lifecycle_claimer",
            command_id = row.id,
            verb = %row.verb,
            error = %error,
            "command failed"
        ),
        RunOutcome::Cancelled(reason) => tracing::info!(
            target: "weft_dispatcher::lifecycle_claimer",
            command_id = row.id,
            verb = %row.verb,
            reason = %reason,
            "command cancelled"
        ),
    }
}

/// Renew `process`'s claim on command `id` every `CLAIM_RENEW_INTERVAL`,
/// returning only once the claim is no longer this process's. A renewal that
/// cannot reach the database is retried at the next interval: the lease
/// outlives several of them.
async fn hold_claim(pool: &PgPool, id: i64, replica: &str) {
    let mut every = tokio::time::interval(weft_broker_client::lifecycle_command::CLAIM_RENEW_INTERVAL);
    every.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    every.tick().await; // the first tick fires at once: the claim is fresh
    loop {
        every.tick().await;
        let renewed = sqlx::query(
            "UPDATE infra_lifecycle_command SET claimed_at_unix = EXTRACT(EPOCH FROM NOW())::BIGINT \
             WHERE id = $1 AND claimed_by_replica = $2 AND completed_at_unix IS NULL",
        )
        .bind(id)
        .bind(replica)
        .execute(pool)
        .await;
        match renewed {
            Ok(done) if done.rows_affected() == 0 => return,
            Ok(_) => {}
            Err(e) => tracing::warn!(
                target: "weft_dispatcher::lifecycle_claimer",
                command_id = id,
                error = %e,
                "could not renew the claim on this command; trying again at the next interval"
            ),
        }
    }
}

/// Run the verb-specific handler. An upgrade carries the dispatcher's
/// own spec (`UpgradeWork`); the health verbs rebuild the typed
/// `LifecycleSpec` the supervisor wrote via
/// `LifecycleSpec::from_row_columns`, which shares its encoding with
/// `into_row_columns` in `weft-broker-client::protocol`.
async fn run_claimed(state: &DispatcherState, row: &ClaimedCommand) -> Result<RunOutcome> {
    use weft_broker_client::protocol::LifecycleSpec;
    let project_id = row.project_id;
    if row.verb == InfraLifecycleVerb::Upgrade {
        let spec = row
            .spec_json
            .clone()
            .ok_or_else(|| anyhow::anyhow!("infra_lifecycle_command.id={}: upgrade missing spec_json", row.id))?;
        let work: crate::infra_lifecycle_command::UpgradeWork = serde_json::from_value(spec)
            .map_err(|e| anyhow::anyhow!("infra_lifecycle_command.id={}: upgrade spec_json malformed: {e}", row.id))?;
        return Ok(
            match crate::api::infra::run_upgrade(state, project_id, row.id, row.instance.as_ref(), &work).await {
                Ok(crate::api::infra::UpgradeEnd::Done) => RunOutcome::Succeeded,
                Ok(crate::api::infra::UpgradeEnd::Cancelled(reason)) => RunOutcome::Cancelled(reason),
                Err((_, message)) => RunOutcome::Failed(message),
            },
        );
    }
    let spec = LifecycleSpec::from_row_columns(row.verb, row.spec_json.clone())
        .map_err(|e| anyhow::anyhow!("infra_lifecycle_command.id={}: {e}", row.id))?;
    match spec {
        LifecycleSpec::Deactivate(d) => run_deactivate(state, project_id, d).await,
        LifecycleSpec::Reactivate(restore) => run_reactivate(state, project_id, restore).await,
    }
}

/// What `claim_one` returns. Verb is parsed at claim time so any
/// parse-fail completes the row immediately (no claimed-but-poison
/// state).
pub struct ClaimedCommand {
    pub id: i64,
    project_id: uuid::Uuid,
    pub verb: InfraLifecycleVerb,
    spec_json: Option<serde_json::Value>,
    /// Whose copies an upgrade cycles (`None`: the shared ones).
    instance: Option<weft_core::instance::InstanceId>,
}

/// Atomic claim: UPDATE the row AND parse its typed columns in one
/// step. A parse failure on a successfully-claimed row would
/// otherwise leave it claimed-but-never-completed, because the
/// `WHERE claimed_by_replica IS NULL` filter excludes it from future
/// claims.
///
/// Strategy: if `try_get` / `parse` fails on a row we just claimed,
/// write `complete(failed, msg)` BEFORE returning the error. The
/// caller's contract ("exactly one complete per claim") stays
/// intact.
pub async fn claim_one(pool: &PgPool, claimer_replica: &str) -> Result<Option<ClaimedCommand>> {
    use sqlx::Row;
    // Claim predicate (shared with the broker's supervisor claim):
    // either no current claimer OR an expired lease. The lease lets
    // a dispatcher that crashed mid-execution release the row
    // automatically after `CLAIM_LEASE_TTL` instead of pinning it.
    //
    // A project's health verbs (`deactivate` / `reactivate`) run one at
    // a time, in issue order: each claimed command runs on its own task,
    // so without this a park and the recovery right after it could run
    // at once and the park land last, leaving the triggers down. An
    // upgrade is not held behind them, nor holds them.
    let sql = format!(
        "UPDATE infra_lifecycle_command \
         SET claimed_by_replica = $1, claimed_at_unix = EXTRACT(EPOCH FROM NOW())::BIGINT \
         WHERE id = ( \
            SELECT c.id FROM infra_lifecycle_command c \
            WHERE c.verb IN ({verbs}) \
              AND {predicate} \
              AND NOT (c.verb IN ({health}) AND EXISTS ( \
                SELECT 1 FROM infra_lifecycle_command o \
                WHERE o.project_id = c.project_id \
                  AND o.verb IN ({health}) \
                  AND o.completed_at_unix IS NULL \
                  AND o.id < c.id)) \
            ORDER BY c.id ASC \
            FOR UPDATE SKIP LOCKED \
            LIMIT 1 \
         ) \
         RETURNING id, project_id, verb, spec_json, instance_id",
        verbs = weft_broker_client::lifecycle_command::DISPATCHER_VERBS_SQL,
        predicate = weft_broker_client::lifecycle_command::claimable_predicate(),
        health = format!(
            "'{}', '{}'",
            InfraLifecycleVerb::Deactivate.as_str(),
            InfraLifecycleVerb::Reactivate.as_str()
        ),
    );
    let row = sqlx::query(&sql)
        .bind(claimer_replica)
        .fetch_optional(pool)
        .await?;
    let Some(r) = row else { return Ok(None) };
    let id: i64 = r.try_get("id")?;
    // Parse every typed column NOW. On error, complete the row
    // before bubbling up, so a poison-pill verb / project_id can't
    // wedge the claimer.
    match decode_row(&r) {
        Ok(cmd) => Ok(Some(cmd)),
        Err(parse_err) => {
            let outcome = RunOutcome::Failed(format!("claim parse failure: {parse_err}"));
            // Best-effort complete: if THIS write also fails we
            // surface both via the bubbled error; the safety poll
            // will retry through the listener loop.
            if let Err(complete_err) = complete(pool, id, claimer_replica, &outcome).await {
                anyhow::bail!(
                    "claim parse failed ({parse_err}); subsequent complete also failed: {complete_err}"
                );
            }
            Err(parse_err)
        }
    }
}

fn decode_row(r: &sqlx::postgres::PgRow) -> Result<ClaimedCommand> {
    use sqlx::Row;
    let id: i64 = r.try_get("id")?;
    let project_id: uuid::Uuid = r.try_get("project_id")?;
    let verb_str: String = r.try_get("verb")?;
    let verb = InfraLifecycleVerb::parse(&verb_str)
        .ok_or_else(|| anyhow::anyhow!("unknown verb '{verb_str}' on id={id}"))?;
    let spec_json: Option<serde_json::Value> =
        r.try_get::<Option<serde_json::Value>, _>("spec_json")?;
    let instance = r
        .try_get::<Option<String>, _>("instance_id")?
        .map(weft_core::instance::InstanceId::new)
        .transpose()
        .map_err(|e| anyhow::anyhow!("corrupt instance_id on id={id}: {e}"))?;
    Ok(ClaimedCommand {
        id,
        project_id,
        verb,
        spec_json,
        instance,
    })
}

/// Project a `RunOutcome` onto the (outcome, outcome_message)
/// column pair. Pure function so we can pin the mapping in a
/// unit test without standing up a DB.
fn project_outcome(
    outcome: &RunOutcome,
) -> (weft_broker_client::protocol::LifecycleOutcome, Option<&str>) {
    use weft_broker_client::protocol::LifecycleOutcome;
    match outcome {
        RunOutcome::Succeeded => (LifecycleOutcome::Succeeded, None),
        RunOutcome::Failed(e) => (LifecycleOutcome::Failed, Some(e.as_str())),
        RunOutcome::Cancelled(reason) => (LifecycleOutcome::Cancelled, Some(reason.as_str())),
    }
}

/// Record `outcome` on command `id`, only while `process` still holds its
/// claim: a process whose claim was taken over never answers for the
/// command, and one already cancelled keeps its cancel.
async fn complete(pool: &PgPool, id: i64, replica: &str, outcome: &RunOutcome) -> Result<()> {
    let (lc_outcome, message) = project_outcome(outcome);
    sqlx::query(
        "UPDATE infra_lifecycle_command \
         SET completed_at_unix = EXTRACT(EPOCH FROM NOW())::BIGINT, \
             outcome = $2, \
             outcome_message = $3 \
         WHERE id = $1 AND claimed_by_replica = $4 AND completed_at_unix IS NULL",
    )
    .bind(id)
    .bind(lc_outcome.as_str())
    .bind(message)
    .bind(replica)
    .execute(pool)
    .await?;
    Ok(())
}

async fn run_deactivate(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    take_down: weft_broker_client::protocol::TakeDownReaders,
) -> Result<RunOutcome> {
    // The take-down round-tripped through the DB (enqueued as JSON in
    // infra_lifecycle_command, deserialized on claim), so it's
    // untrusted input at this point. Validate at the consume boundary
    // so an impossible combo (e.g. wipe+wait) or a take-down aimed at
    // nothing fails loud here rather than taking the wrong branch
    // downstream.
    take_down.validate().map_err(|m| anyhow::anyhow!("invalid health take-down: {m}"))?;
    let Some(project) = state.projects.project(project_id).await? else {
        return Ok(RunOutcome::Cancelled(format!("project {project_id} no longer exists")));
    };
    let activations = state.activations.list(project_id).await?;
    let targets = health_take_down_targets(
        &crate::api::project::compute_trigger_deps(&project),
        &activations,
        &take_down.reach,
    );
    if targets.is_empty() {
        // Nothing listening is reached: nothing to park.
        return Ok(RunOutcome::Succeeded);
    }
    let existed = crate::take_down::take_down(
        state,
        project_id,
        &crate::take_down::TakeDownTarget::Activations(targets),
        &take_down.spec,
        true, // health-loop autonomous park: its auto-recover MAY reactivate this
        None,
    )
    .await
    .map_err(|(_, m)| anyhow::anyhow!("deactivate: {m}"))?;
    if !existed {
        // The project row was removed between the supervisor
        // enqueueing this command and the dispatcher claiming it.
        // Not a failure: surface as Cancelled so `wait_for_command`
        // consumers (delete_project, reap_orphans) distinguish this
        // from a real verb error and skip the failure UX.
        return Ok(RunOutcome::Cancelled(format!(
            "project {project_id} no longer exists"
        )));
    }
    Ok(RunOutcome::Succeeded)
}

/// The live activations a health take-down reaches: every one for a
/// take-down of the whole project; otherwise those whose triggers read
/// one of the `broken` copies (the same readers an infra verb on that
/// copy takes down), and nothing that reads only healthy infra. `deps`
/// is the program's `(infra, trigger)` reads.
fn health_take_down_targets(
    deps: &[(String, String)],
    activations: &[crate::activation_store::Activation],
    reach: &weft_broker_client::protocol::TakeDownReach,
) -> Vec<weft_core::activation::ActivationKey> {
    use weft_broker_client::protocol::TakeDownReach;
    activations
        .iter()
        .filter(|a| a.lifecycle.status == crate::activation_store::ProjectStatus::Active)
        .filter(|a| {
            let broken = match reach {
                TakeDownReach::Project => return true,
                TakeDownReach::ReadersOf { broken } => broken,
            };
            broken.iter().any(|copy| {
                crate::api::infra::reads(
                    deps,
                    &a.key,
                    &std::collections::BTreeSet::from([copy.node_id.clone()]),
                    &weft_core::instance::Copies::of(copy.instance.clone()),
                )
            })
        })
        .map(|a| a.key.clone())
        .collect()
}

async fn run_reactivate(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    restore: weft_broker_client::protocol::RestoreReaders,
) -> Result<RunOutcome> {
    // A NotFound anywhere below means the project was removed between
    // enqueue and claim: same Cancelled semantic as the deactivate arm.
    let Some(project) = state.projects.project(project_id).await? else {
        return Ok(RunOutcome::Cancelled(format!("project {project_id} no longer exists")));
    };
    let targets = health_restore_targets(
        &crate::api::project::compute_trigger_deps(&project),
        |place| crate::api::infra::is_per_instance_place(&project, place),
        &state.activations.list(project_id).await?,
        &restore.still_broken,
    );
    // Owner by owner (one activation per owner, since a setup run is
    // one owner's).
    let mut parked: std::collections::BTreeMap<Option<weft_core::instance::InstanceId>, Vec<String>> = Default::default();
    for key in targets {
        parked.entry(key.instance().cloned()).or_default().push(key.trigger);
    }
    for (instance, triggers) in parked {
        let request = weft_core::activation::ActivateRequest {
            scope: weft_core::activation::ActivationScope { triggers, instance },
            ..Default::default()
        };
        match crate::api::project::activate_inner(state, project_id, request).await {
            Ok(_) => {}
            Err((axum::http::StatusCode::NOT_FOUND, _)) => {
                return Ok(RunOutcome::Cancelled(format!("project {project_id} no longer exists")));
            }
            Err((_, m)) => return Err(anyhow::anyhow!("reactivate: {m}")),
        }
    }
    Ok(RunOutcome::Succeeded)
}

/// The activations a health recovery restores: those the health loop
/// took down (they carry its mark, and a person's deactivate clears it)
/// that are still down, and that read none of the `still_broken`
/// copies. A trigger reading one copy still broken stays down; one
/// instance's broken copy never holds back another's readers; a copy the
/// loop cannot see (nothing says it is broken) holds back nobody.
/// `deps` is the program's `(infra, trigger)` reads; a trigger reads
/// the shared copy of a shared node and its owner's copy of a
/// per-instance one.
fn health_restore_targets(
    deps: &[(String, String)],
    per_instance_of: impl Fn(&str) -> bool,
    activations: &[crate::activation_store::Activation],
    still_broken: &[weft_broker_client::protocol::InfraCopy],
) -> Vec<weft_core::activation::ActivationKey> {
    activations
        .iter()
        .filter(|a| a.lifecycle.deactivated_by_health && a.lifecycle.status == crate::activation_store::ProjectStatus::Inactive)
        .filter(|a| {
            !deps
                .iter()
                .filter(|(_, trigger)| *trigger == a.key.trigger)
                .map(|(infra, _)| weft_broker_client::protocol::InfraCopy {
                    node_id: infra.clone(),
                    instance: if per_instance_of(infra) { a.key.instance().cloned() } else { None },
                })
                .any(|copy| still_broken.contains(&copy))
        })
        .map(|a| a.key.clone())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use weft_broker_client::protocol::{InfraCopy, LifecycleOutcome, TakeDownReach};
    use weft_core::activation::ActivationKey;
    use weft_core::instance::{InstanceId, Owner};

    fn activation(trigger: &str, owner: Owner, status: crate::activation_store::ProjectStatus) -> crate::activation_store::Activation {
        let lifecycle = match status {
            crate::activation_store::ProjectStatus::Active => weft_broker_client::activation::ActivationLifecycle::active(),
            _ => weft_broker_client::activation::ActivationLifecycle::parked(),
        };
        crate::activation_store::Activation {
            key: ActivationKey::new(trigger, owner),
            lifecycle,
            program: None,
            source_version: None,
        }
    }

    /// An activation the health loop took down.
    fn by_health(trigger: &str, owner: Owner) -> crate::activation_store::Activation {
        let mut a = activation(trigger, owner, crate::activation_store::ProjectStatus::Inactive);
        a.lifecycle.deactivated_by_health = true;
        a
    }

    fn ada() -> Owner {
        Owner::Instance(InstanceId::new("ada").unwrap())
    }

    /// An instance's broken copy takes down only that instance's live
    /// triggers that read the node: never a trigger that reads no infra
    /// (the program's shared `mint`), never another instance's reader,
    /// never one already down. A broken shared copy takes down every
    /// owner's readers of it.
    #[test]
    fn a_health_take_down_reaches_only_the_readers_of_the_broken_copy() {
        use crate::activation_store::ProjectStatus::{Active, Inactive};
        let deps = vec![("svc".to_string(), "up".to_string()), ("db".to_string(), "look".to_string())];
        let bob = Owner::Instance(InstanceId::new("bob").unwrap());
        let activations = vec![
            activation("mint", Owner::Shared, Active),
            activation("up", ada(), Active),
            activation("up", bob.clone(), Active),
            activation("look", ada(), Active),
            activation("look", Owner::Shared, Active),
            activation("look", bob.clone(), Inactive),
        ];
        let of = |copy: InfraCopy| TakeDownReach::ReadersOf { broken: vec![copy] };
        let ada_svc = InfraCopy { node_id: "svc".into(), instance: Some(InstanceId::new("ada").unwrap()) };
        assert_eq!(
            health_take_down_targets(&deps, &activations, &of(ada_svc)),
            vec![ActivationKey::new("up", ada())]
        );
        let shared_db = InfraCopy { node_id: "db".into(), instance: None };
        assert_eq!(
            health_take_down_targets(&deps, &activations, &of(shared_db)),
            vec![ActivationKey::new("look", ada()), ActivationKey::new("look", Owner::Shared)]
        );
        // A protocol whose condition names no infra reaches every live
        // activation, as it did before copies had owners.
        assert_eq!(health_take_down_targets(&deps, &activations, &TakeDownReach::Project).len(), 5);
    }

    /// A recovery restores what the health loop parked that reads none
    /// of the copies still broken: ada's reader of her healed `svc` comes
    /// back while bob's reader of his still-broken copy stays down, a
    /// reader of the healthy shared `db` that also reads bob's broken
    /// copy stays down, and a person's own deactivate (no mark) is never
    /// overridden.
    #[test]
    fn a_health_recovery_restores_what_reads_no_broken_copy() {
        use crate::activation_store::ProjectStatus::Inactive;
        let deps = vec![
            ("svc".to_string(), "up".to_string()),
            ("db".to_string(), "look".to_string()),
            ("db".to_string(), "both".to_string()),
            ("svc".to_string(), "both".to_string()),
        ];
        let bob = Owner::Instance(InstanceId::new("bob").unwrap());
        let activations = vec![
            by_health("up", ada()),
            by_health("up", bob.clone()),
            by_health("look", Owner::Shared),
            by_health("both", bob.clone()),
            activation("look", ada(), Inactive),
        ];
        let still_broken = vec![InfraCopy { node_id: "svc".into(), instance: Some(InstanceId::new("bob").unwrap()) }];
        assert_eq!(
            health_restore_targets(&deps, |place| place == "svc", &activations, &still_broken),
            vec![ActivationKey::new("up", ada()), ActivationKey::new("look", Owner::Shared)]
        );
    }

    /// A shared-only project: a trigger parked for a broken `db` that
    /// also reads `cache`, a copy the loop has never seen (its apply
    /// failed), comes back once `db` heals. Nothing says `cache` is
    /// broken, so it holds nothing back.
    #[test]
    fn a_reader_of_an_unseen_copy_is_restored() {
        let deps = vec![("db".to_string(), "look".to_string()), ("cache".to_string(), "look".to_string())];
        let activations = vec![by_health("look", Owner::Shared)];
        assert_eq!(
            health_restore_targets(&deps, |_| false, &activations, &[]),
            vec![ActivationKey::new("look", Owner::Shared)]
        );
    }

    /// `RunOutcome::Cancelled` MUST project to `LifecycleOutcome::Cancelled`
    /// + a reason in `outcome_message`. The previous shape wrote
    /// Succeeded for "project no longer exists" cancellations,
    /// hiding the distinction from `wait_for_command` consumers.
    /// Pin the projection so a regression breaks CI.
    #[test]
    fn project_outcome_distinguishes_three_terminal_states() {
        let succeeded = RunOutcome::Succeeded;
        let (lc, msg) = project_outcome(&succeeded);
        assert_eq!(lc, LifecycleOutcome::Succeeded);
        assert_eq!(msg, None);

        let failed = RunOutcome::Failed("boom".into());
        let (lc, msg) = project_outcome(&failed);
        assert_eq!(lc, LifecycleOutcome::Failed);
        assert_eq!(msg, Some("boom"));

        let cancelled = RunOutcome::Cancelled("gone".into());
        let (lc, msg) = project_outcome(&cancelled);
        assert_eq!(lc, LifecycleOutcome::Cancelled);
        assert_eq!(msg, Some("gone"));
    }
}

