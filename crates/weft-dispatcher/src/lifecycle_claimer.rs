//! Background loop that claims dispatcher-owned lifecycle commands
//! (`deactivate` / `reactivate`, and a person's `upgrade`) and runs
//! them, each on its own task with its claim renewed while it runs (an
//! upgrade can wait on a drain for hours). The complementary
//! supervisor-owned verbs (`apply` / `stop` / `terminate`) are
//! claimed by the pooled supervisor process that owns the project, via the
//! broker; a stop or terminate that first waits for the runs reaching its
//! copies to end (`drains`) is claimed here for that wait
//! (`crate::drain`), and handed on to its supervisor once it is over.
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
//! everything pending, then looks again when the earliest of its claims
//! could lapse, since nothing announces a claimer that died.

use anyhow::Result;
use weft_broker_client::lifecycle_command::ISSUED_WAKE;
use weft_task_store::db::{self, Client as _, Pool};
use weft_task_store::drain::{DrainLoop, DrainStep, WakeOn, SAFETY_POLL_INTERVAL};

use crate::infra_lifecycle_command::InfraLifecycleVerb;
use crate::state::DispatcherState;

/// A command being issued, for any project: the claimer serves them all.
pub(crate) static WAKE_ON: &[WakeOn] = &[ISSUED_WAKE];

pub fn drain_loop(state: DispatcherState) -> DrainLoop {
    DrainLoop::new("lifecycle_claimer", "start the lifecycle commands waiting for it", WAKE_ON, SAFETY_POLL_INTERVAL, move || {
        let state = state.clone();
        async move {
            if claim_and_run_one(&state).await? {
                return Ok(DrainStep::More);
            }
            // A claim whose claimer died is taken over once its lease
            // runs out, which nothing announces.
            let now = crate::lease::now_unix();
            Ok(match earliest_claim_lapse(&state.pg_pool).await? {
                Some(at) => DrainStep::RetryIn(std::time::Duration::from_secs((at + 1 - now).max(1).unsigned_abs())),
                None => DrainStep::Done,
            })
        }
    })
}

/// When the earliest claim this loop makes runs out (unix seconds, on the
/// database's clock like the claim): what [`claim_one`] then takes over.
async fn earliest_claim_lapse(pool: &Pool) -> Result<Option<i64>> {
    Ok(pool
        .scalar(
            &format!(
                "SELECT MIN(claimed_at_unix) + {ttl} FROM infra_lifecycle_command \
                 WHERE claimed_by_replica IS NOT NULL AND completed_at_unix IS NULL \
                   AND (verb IN ({verbs}) OR drains)",
                ttl = weft_broker_client::lifecycle_command::CLAIM_LEASE_TTL.as_secs(),
                verbs = weft_broker_client::lifecycle_command::DISPATCHER_VERBS_SQL,
            ),
            &[],
        )
        .await?)
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
    // Work of its own, outliving the drain that claimed it: the database
    // is listened to until it ends, for what it writes.
    let busy = state.signals.busy().await;
    tokio::spawn(async move {
        let _busy = busy;
        run_and_complete(&state, row).await
    });
    Ok(true)
}

/// Run one claimed command to its outcome and record it, renewing the
/// claim meanwhile. When the claim is lost (this process could not renew it
/// for a whole lease and another process took the command over), the run
/// stops here and the new holder answers for it.
async fn run_and_complete(state: &DispatcherState, row: ClaimedCommand) {
    if row.waits {
        return drain_then_hand_over(state, &row).await;
    }
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

/// The wait a stop or terminate does before its supervisor takes it: the
/// runs reaching its copies, through the one drain, for as long as they
/// go (the run that asked is waited for too, never cancelled for it).
/// Once over, the command is handed on: its wait cleared and its claim let
/// go, which announces it to the supervisors. A cancel of the command ends
/// it here, cancelled, and brings back the triggers it took down
/// (`crate::api::infra::cancel`). The drain keeps the claim alive at each
/// look it takes ([`CommandClaim`]), so a wait on runs that only wait
/// touches the database when it looks, not on a clock of its own. A
/// database failure lets the claim go with the wait still to do, for the
/// next pass.
async fn drain_then_hand_over(state: &DispatcherState, row: &ClaimedCommand) {
    let replica = state.replica.as_str();
    let project = match state.projects.project(row.project_id).await {
        Ok(Some(project)) => project,
        Ok(None) => {
            let outcome = RunOutcome::Cancelled(format!("project {} no longer exists", row.project_id));
            if let Err(e) = complete(&state.pg_pool, row.id, replica, &outcome).await {
                tracing::error!(target: "weft_dispatcher::lifecycle_claimer", command_id = row.id, error = %format!("{e:#}"), "could not record a cancelled command; its claim lapses and the next pass looks again");
            }
            return;
        }
        Err(e) => {
            tracing::warn!(target: "weft_dispatcher::lifecycle_claimer", command_id = row.id, error = %format!("{e:#}"), "could not read the program this take-down waits on; it is looked at again once its claim lapses");
            return;
        }
    };
    let reach = crate::infra_reach::InfraReach::of_command(&project, row.node_id.as_deref(), row.copies.clone());
    let scope = crate::drain::DrainScope { project_id: row.project_id, reaching: crate::drain::Reaching::Infra { reach: &reach, tokens: &row.waits_on_tokens } };
    let claim = CommandClaim { id: row.id, replica };
    let ended = tokio::select! {
        drained = crate::drain::drain(state, &scope, Some(&claim), None) => drained,
        () = cancelled(state, row.id) => {
            let outcome = RunOutcome::Cancelled("cancelled while waiting for the runs reaching it".into());
            // The copies never went down: what went down with them comes
            // back once this ending is heard
            // (`crate::infra_lifecycle_command::SinceTakeDown`).
            if let Err(e) = complete(&state.pg_pool, row.id, replica, &outcome).await {
                tracing::error!(target: "weft_dispatcher::lifecycle_claimer", command_id = row.id, error = %format!("{e:#}"), "could not record a cancelled command; its claim lapses and the next pass looks again");
            }
            return;
        }
    };
    let handed = match ended {
        Ok(()) => state
            .pg_pool
            .execute(
                "UPDATE infra_lifecycle_command SET drains = FALSE, claimed_by_replica = NULL, claimed_at_unix = NULL \
                 WHERE id = $1 AND claimed_by_replica = $2 AND completed_at_unix IS NULL",
                &[&row.id, &replica],
            )
            .await
            .map(|_| ())
            .map_err(anyhow::Error::from),
        Err(e) => Err(e),
    };
    if let Err(e) = handed {
        tracing::warn!(target: "weft_dispatcher::lifecycle_claimer", command_id = row.id, error = %format!("{e:#}"), "the wait before this take-down failed; it is looked at again once its claim lapses");
    }
}

/// Until command `id` is cancelled: its flag is read now, then again each
/// time a cancel of it is announced (`InfraCommandSignal::Cancel`) or the
/// watch may have missed one. Never returns when the flag cannot be read
/// or the watch stops (both logged): the command runs on.
async fn cancelled(state: &DispatcherState, id: i64) {
    use weft_broker_client::lifecycle_command::{InfraCommandSignal, INFRA_COMMAND_CHANNEL};
    // Subscribed before the read, so a cancel between the two is heard.
    let mut heard = state.signals.subscribe();
    loop {
        match crate::infra_lifecycle_command::cancel_requested(&state.pg_pool, id).await {
            Ok(true) => return,
            Ok(false) => {}
            Err(e) => tracing::warn!(
                target: "weft_dispatcher::lifecycle_claimer",
                command_id = id, error = %format!("{e:#}"),
                "could not read whether this command was cancelled; read again at the next cancel"
            ),
        }
        let woken = heard
            .woken(|channel, payload| channel == INFRA_COMMAND_CHANNEL && InfraCommandSignal::parse(payload) == Some(InfraCommandSignal::Cancel { id }))
            .await;
        if let Err(e) = woken {
            tracing::error!(target: "weft_dispatcher::lifecycle_claimer", command_id = id, error = %format!("{e:#}"), "no cancel of this command can be heard any more");
            return std::future::pending().await;
        }
    }
}

/// A dispatcher's claim on a command whose drain keeps it alive
/// (`crate::drain::drain`): renewed at each look the drain takes, to last
/// past its next one.
pub struct CommandClaim<'a> {
    pub id: i64,
    pub replica: &'a str,
}

impl CommandClaim<'_> {
    /// Keep the claim until at least `until_unix` (the claim lapses
    /// `CLAIM_LEASE_TTL` after `claimed_at_unix`, which is set that far
    /// ahead when the drain's next look is further than one lease away).
    /// `false` when it is no longer this process's.
    pub async fn keep_until(&self, pool: &Pool, until_unix: i64) -> Result<bool> {
        let ttl = i64::try_from(weft_broker_client::lifecycle_command::CLAIM_LEASE_TTL.as_secs()).unwrap_or(i64::MAX);
        let renewed = pool
            .execute(
                "UPDATE infra_lifecycle_command SET claimed_at_unix = GREATEST(EXTRACT(EPOCH FROM NOW())::BIGINT, $3::bigint - $4::bigint) \
                 WHERE id = $1 AND claimed_by_replica = $2 AND completed_at_unix IS NULL",
                &[&self.id, &self.replica, &until_unix, &ttl],
            )
            .await?;
        Ok(renewed == 1)
    }
}

/// Renew `process`'s claim on command `id` every `CLAIM_RENEW_INTERVAL`,
/// returning only once the claim is no longer this process's. A renewal that
/// cannot reach the database is retried at the next interval: the lease
/// outlives several of them.
async fn hold_claim(pool: &Pool, id: i64, replica: &str) {
    let mut every = tokio::time::interval(weft_broker_client::lifecycle_command::CLAIM_RENEW_INTERVAL);
    every.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    every.tick().await; // the first tick fires at once: the claim is fresh
    loop {
        every.tick().await;
        let renewed = pool
            .execute(
                "UPDATE infra_lifecycle_command SET claimed_at_unix = EXTRACT(EPOCH FROM NOW())::BIGINT \
                 WHERE id = $1 AND claimed_by_replica = $2 AND completed_at_unix IS NULL",
                &[&id, &replica],
            )
            .await;
        match renewed {
            Ok(0) => return,
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
    /// The place a take-down acts on, `None` for every one of its copies'
    /// kind.
    node_id: Option<String>,
    /// The copies a take-down acts on.
    copies: weft_core::instance::Copies,
    /// A stop or terminate claimed for the wait it does first.
    waits: bool,
    /// The entry tokens of the triggers taken down with it, as they stood
    /// before (`infra_lifecycle_command.waits_on_tokens`).
    waits_on_tokens: Vec<String>,
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
pub async fn claim_one(pool: &Pool, claimer_replica: &str) -> Result<Option<ClaimedCommand>> {
    // Claim predicate (shared with the broker's supervisor claim):
    // either no current claimer OR an expired lease. The lease lets
    // a dispatcher that crashed mid-run release the row
    // automatically after `CLAIM_LEASE_TTL` instead of pinning it.
    //
    // A project's health verbs (`deactivate` / `reactivate`) run one at
    // a time, in issue order: each claimed command runs on its own task,
    // so without this a take-down and the recovery right after it could
    // run at once and the take-down land last, leaving the triggers down. An
    // upgrade is not held behind them, nor holds them.
    let sql = format!(
        "UPDATE infra_lifecycle_command \
         SET claimed_by_replica = $1, claimed_at_unix = EXTRACT(EPOCH FROM NOW())::BIGINT \
         WHERE id = ( \
            SELECT c.id FROM infra_lifecycle_command c \
            WHERE (c.verb IN ({verbs}) OR c.drains) \
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
         RETURNING id, project_id, node_id, verb, spec_json, instance_id, every_copy, drains, waits_on_tokens",
        verbs = weft_broker_client::lifecycle_command::DISPATCHER_VERBS_SQL,
        predicate = weft_broker_client::lifecycle_command::claimable_predicate(),
        health = format!(
            "'{}', '{}'",
            InfraLifecycleVerb::Deactivate.as_str(),
            InfraLifecycleVerb::Reactivate.as_str()
        ),
    );
    let row: Option<db::Row> = pool.opt(&sql, &[&claimer_replica]).await?;
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

fn decode_row(r: &db::Row) -> Result<ClaimedCommand> {
    let id: i64 = r.try_get("id")?;
    let project_id: uuid::Uuid = r.try_get("project_id")?;
    let verb_str: String = r.try_get("verb")?;
    let verb = InfraLifecycleVerb::parse(&verb_str)
        .ok_or_else(|| anyhow::anyhow!("unknown verb '{verb_str}' on id={id}"))?;
    let spec_json = r.try_get::<_, Option<db::JsonText<serde_json::Value>>>("spec_json")?.map(|spec| spec.0);
    let instance_id: Option<String> = r.try_get("instance_id")?;
    let every_copy: bool = r.try_get("every_copy")?;
    let copies = weft_core::instance::Copies::from_columns(instance_id.clone(), every_copy)
        .map_err(|e| anyhow::anyhow!("corrupt copies on id={id}: {e}"))?;
    let instance = instance_id
        .map(weft_core::instance::InstanceId::new)
        .transpose()
        .map_err(|e| anyhow::anyhow!("corrupt instance_id on id={id}: {e}"))?;
    Ok(ClaimedCommand {
        id,
        project_id,
        verb,
        spec_json,
        instance,
        node_id: r.try_get("node_id")?,
        copies,
        waits: r.try_get("drains")?,
        waits_on_tokens: r.try_get("waits_on_tokens")?,
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
async fn complete(pool: &Pool, id: i64, replica: &str, outcome: &RunOutcome) -> Result<()> {
    let (lc_outcome, message) = project_outcome(outcome);
    pool.execute(
        "UPDATE infra_lifecycle_command \
         SET completed_at_unix = EXTRACT(EPOCH FROM NOW())::BIGINT, \
             outcome = $2, \
             outcome_message = $3 \
         WHERE id = $1 AND claimed_by_replica = $4 AND completed_at_unix IS NULL",
        &[&id, &lc_outcome.as_str(), &message, &replica],
    )
    .await?;
    Ok(())
}

async fn run_deactivate(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    take_down: weft_broker_client::protocol::TakeDownReaders,
) -> Result<RunOutcome> {
    // The take-down round-tripped through the DB (enqueued as JSON in
    // infra_lifecycle_command, deserialized on claim), so it's untrusted
    // input at this point. Validate at the consume boundary so a
    // take-down aimed at nothing fails loud here rather than taking the
    // wrong branch downstream.
    take_down.validate().map_err(|m| anyhow::anyhow!("invalid health take-down: {m}"))?;
    let Some(project) = state.projects.project(project_id).await? else {
        return Ok(RunOutcome::Cancelled(format!("project {project_id} no longer exists")));
    };
    let activations = state.activations.list(project_id).await?;
    let targets = health_take_down_targets(&project, &activations, &take_down.broken);
    if targets.is_empty() {
        // Nothing listening is reached: nothing to take down.
        return Ok(RunOutcome::Succeeded);
    }
    let taken = crate::take_down::take_down(
        state,
        project_id,
        &crate::take_down::TakeDownTarget::Activations(targets),
        &weft_core::take_down::TakeDown::HEALTH,
        // The health loop's own take-down: its auto-recover may bring it back.
        crate::take_down::DownCause::Health(&take_down.broken),
        crate::take_down::TakeDownAsker::Outside,
    )
    .await
    .map_err(|(_, m)| anyhow::anyhow!("deactivate: {m}"))?;
    if taken == crate::take_down::TakenDown::ProjectGone {
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

/// The live activations a health take-down reaches: those whose
/// trigger's cut contains one of the `broken` copies (the same triggers an
/// infra verb on that copy takes down), and nothing whose cut has only
/// healthy infra.
fn health_take_down_targets(
    project: &weft_core::ProjectDefinition,
    activations: &[crate::activation_store::Activation],
    broken: &[weft_broker_client::protocol::InfraCopy],
) -> Vec<weft_core::activation::ActivationKey> {
    let mut keys: Vec<weft_core::activation::ActivationKey> = broken
        .iter()
        .flat_map(|copy| crate::infra_reach::live_triggers_reached(&copy_reach(copy), activations, project))
        .collect();
    keys.sort();
    keys.dedup();
    keys
}

/// The reach of one copy: its place, its owner's copy of it.
fn copy_reach(copy: &weft_broker_client::protocol::InfraCopy) -> crate::infra_reach::InfraReach {
    crate::infra_reach::InfraReach {
        places: std::collections::BTreeSet::from([copy.node_id.clone()]),
        copies: weft_core::instance::Copies::of(copy.instance.clone()),
    }
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
    // The copies expected to run: one stopped by a person, being worked
    // on by an infra command, or never applied is not up, whatever the
    // health check saw of the rest.
    let observed = crate::infra_node::observe(&state.pg_pool, project_id, &project).await?;
    let up: std::collections::HashSet<weft_broker_client::protocol::InfraCopy> = observed
        .rows
        .iter()
        .filter(|row| matches!(row.status, crate::infra_node::InfraNodeStatus::Running | crate::infra_node::InfraNodeStatus::Flaky))
        .map(|row| weft_broker_client::protocol::InfraCopy { node_id: row.node_id.clone(), instance: row.instance.clone() })
        .collect();
    let declared = weft_core::project::DeclaredInfra::of(&project);
    let targets = health_restore_targets(
        &project,
        |place| declared.per_instance(place) == Some(true),
        &state.activations.list(project_id).await?,
        &restore.still_broken,
        |copy| up.contains(copy),
    );
    // Owner by owner (one activation per owner, since a setup run is
    // one owner's).
    let mut down: std::collections::BTreeMap<weft_core::instance::Owner, Vec<String>> = Default::default();
    for key in targets {
        down.entry(key.owner.clone()).or_default().push(key.trigger);
    }
    for (owner, triggers) in down {
        let request = weft_core::activation::ActivateRequest {
            scope: weft_core::activation::ActivationScope::of_owner(triggers, &owner),
            ..Default::default()
        };
        let activated = match crate::api::project::activate_with(state, project_id, request, crate::api::project::ActivateAsker::InfraBack).await {
            // The install is in maintenance: nothing comes back until it is
            // lifted, and what the health check took down is not the lift's
            // to bring back.
            Err(crate::api::project::ActivateError::Refused(crate::activation_store::ClaimRefused::Maintenance)) => {
                return Ok(RunOutcome::Cancelled(weft_core::maintenance::IN_MAINTENANCE.to_string()));
            }
            activated => activated.map_err(<(axum::http::StatusCode, String)>::from),
        };
        match activated {
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
/// that are still down, whose cut contains none of the `still_broken`
/// copies, and every copy of whose cut is up (`is_up`: running, not
/// stopped by a person, worked on by an infra command or never applied,
/// which the health check does not see). A trigger whose cut has one copy
/// still broken or not up stays down; one instance's copy never holds
/// back another's triggers. A trigger's cut holds the shared copy of a
/// shared node and its owner's copy of a per-instance one.
fn health_restore_targets(
    project: &weft_core::ProjectDefinition,
    per_instance_of: impl Fn(&str) -> bool,
    activations: &[crate::activation_store::Activation],
    still_broken: &[weft_broker_client::protocol::InfraCopy],
    is_up: impl Fn(&weft_broker_client::protocol::InfraCopy) -> bool,
) -> Vec<weft_core::activation::ActivationKey> {
    activations
        .iter()
        .filter(|a| a.lifecycle.down_by_health())
        .filter(|a| {
            crate::infra_reach::depends_on(a, project)
                .into_iter()
                .map(|infra| weft_broker_client::protocol::InfraCopy {
                    instance: if per_instance_of(&infra) { a.key.instance().cloned() } else { None },
                    node_id: infra,
                })
                .all(|copy| !still_broken.contains(&copy) && is_up(&copy))
        })
        .map(|a| a.key.clone())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use weft_broker_client::protocol::{InfraCopy, LifecycleOutcome};
    use weft_core::activation::ActivationKey;
    use weft_core::instance::{InstanceId, Owner};

    fn activation(trigger: &str, owner: Owner, status: crate::activation_store::ProjectStatus, depends_on: &[&str]) -> crate::activation_store::Activation {
        let lifecycle = match status {
            crate::activation_store::ProjectStatus::Active => weft_broker_client::activation::ActivationLifecycle::active(),
            _ => weft_broker_client::activation::ActivationLifecycle::holding(),
        };
        crate::activation_store::Activation {
            key: ActivationKey::new(trigger, owner),
            lifecycle,
            program: None,
            source_version: None,
            driver_task: None,
            activating: None,
            cut: Some(crate::activation_store::RecordedCut {
                fingerprint: "f".into(),
                depends_on: depends_on.iter().map(|s| s.to_string()).collect(),
            }),
        }
    }

    /// An activation the health loop took down.
    fn by_health(trigger: &str, owner: Owner, depends_on: &[&str]) -> crate::activation_store::Activation {
        let mut a = activation(trigger, owner, crate::activation_store::ProjectStatus::Inactive, depends_on);
        a.lifecycle.went_down = Some(weft_broker_client::activation::WentDown::Health);
        a
    }

    fn ada() -> Owner {
        Owner::Instance(InstanceId::new("ada").unwrap())
    }

    fn empty() -> weft_core::ProjectDefinition {
        serde_json::from_value(serde_json::json!({ "id": uuid::Uuid::nil(), "nodes": [], "edges": [] })).unwrap()
    }

    /// An instance's broken copy takes down only that instance's live
    /// triggers whose cut contains the node: never a trigger whose cut has
    /// no infra (the program's shared `mint`), never another instance's,
    /// never one already down. A broken shared copy takes down every
    /// owner's triggers whose cut contains it.
    #[test]
    fn a_health_take_down_reaches_only_the_triggers_whose_cut_contains_the_broken_copy() {
        use crate::activation_store::ProjectStatus::{Active, Inactive};
        let bob = Owner::Instance(InstanceId::new("bob").unwrap());
        let activations = vec![
            activation("mint", Owner::Shared, Active, &[]),
            activation("up", ada(), Active, &["svc"]),
            activation("up", bob.clone(), Active, &["svc"]),
            activation("look", ada(), Active, &["db"]),
            activation("look", Owner::Shared, Active, &["db"]),
            activation("look", bob.clone(), Inactive, &["db"]),
        ];
        let ada_svc = InfraCopy { node_id: "svc".into(), instance: Some(InstanceId::new("ada").unwrap()) };
        assert_eq!(health_take_down_targets(&empty(), &activations, &[ada_svc]), vec![ActivationKey::new("up", ada())]);
        let shared_db = InfraCopy { node_id: "db".into(), instance: None };
        assert_eq!(
            health_take_down_targets(&empty(), &activations, &[shared_db]),
            vec![ActivationKey::new("look", Owner::Shared), ActivationKey::new("look", ada())]
        );
    }

    /// A recovery restores what the health loop took down whose cut
    /// contains none of the copies still broken: ada's trigger on her
    /// healed `svc` comes back while bob's on his still-broken copy stays
    /// down, a trigger on the healthy shared `db` whose cut also holds
    /// bob's broken copy stays down, and a person's own deactivate (no
    /// mark) is never overridden.
    #[test]
    fn a_health_recovery_restores_what_has_no_broken_copy_in_its_cut() {
        use crate::activation_store::ProjectStatus::Inactive;
        let bob = Owner::Instance(InstanceId::new("bob").unwrap());
        let activations = vec![
            by_health("up", ada(), &["svc"]),
            by_health("up", bob.clone(), &["svc"]),
            by_health("look", Owner::Shared, &["db"]),
            by_health("both", bob.clone(), &["db", "svc"]),
            activation("look", ada(), Inactive, &["db"]),
        ];
        let still_broken = vec![InfraCopy { node_id: "svc".into(), instance: Some(InstanceId::new("bob").unwrap()) }];
        assert_eq!(
            health_restore_targets(&empty(), |place| place == "svc", &activations, &still_broken, |_| true),
            vec![ActivationKey::new("up", ada()), ActivationKey::new("look", Owner::Shared)]
        );
    }

    /// A trigger taken down for a broken `db` whose cut also holds
    /// `cache`, a copy that is not up (a person stopped it meanwhile, an
    /// infra command works on it, or its apply failed: the health check sees
    /// none of these), stays down once `db` heals; it comes back once
    /// `cache` runs too.
    #[test]
    fn a_trigger_reading_a_copy_that_is_not_up_stays_down() {
        let activations = vec![by_health("look", Owner::Shared, &["cache", "db"])];
        let db = InfraCopy { node_id: "db".into(), instance: None };
        assert!(health_restore_targets(&empty(), |_| false, &activations, &[], |copy| copy == &db).is_empty());
        assert_eq!(
            health_restore_targets(&empty(), |_| false, &activations, &[], |_| true),
            vec![ActivationKey::new("look", Owner::Shared)]
        );
    }

    /// A program with two triggers: `door` reaches the infra node `svc`
    /// only further down its flow (`door -> parse -> svc`), and `tick`
    /// reaches no infra (`tick -> log`).
    fn door_and_tick() -> weft_core::ProjectDefinition {
        let node = |id: &str, is_trigger: bool, requires_infra: bool| {
            serde_json::json!({
                "id": id, "nodeType": "T", "label": null, "config": {}, "position": { "x": 0, "y": 0 },
                "features": { "isTrigger": is_trigger }, "requiresInfra": requires_infra,
            })
        };
        let edge = |s: &str, t: &str| {
            serde_json::json!({ "id": format!("e_{s}_{t}"), "source": s, "sourceHandle": "out", "target": t, "targetHandle": "in" })
        };
        serde_json::from_value(serde_json::json!({
            "id": uuid::Uuid::nil(),
            "nodes": [node("door", true, false), node("parse", false, false), node("svc", false, true), node("tick", true, false), node("log", false, false)],
            "edges": [edge("door", "parse"), edge("parse", "svc"), edge("tick", "log")],
            "groups": [],
        }))
        .unwrap()
    }

    /// `trigger` of `owner`, its cut recorded from `project` the way its
    /// setup records it, with `lifecycle`.
    fn set_up(
        project: &weft_core::ProjectDefinition,
        trigger: &str,
        owner: Owner,
        lifecycle: weft_broker_client::activation::ActivationLifecycle,
    ) -> crate::activation_store::Activation {
        crate::activation_store::Activation {
            key: ActivationKey::new(trigger, owner),
            lifecycle,
            program: None,
            source_version: None,
            driver_task: None,
            activating: None,
            cut: Some(weft_core::run_spec::trigger_cut(project, trigger).unwrap().into()),
        }
    }

    /// The health take-down of a broken copy reaches exactly the triggers
    /// `weft infra stop` of that copy reaches (the same per-trigger
    /// dependency list, `crate::infra_reach::activations_reached`), of
    /// those the ones that are on: `door`, which reaches `svc` only two
    /// nodes down its flow, and never `tick`, which reaches no infra. An
    /// instance's broken copy reaches only that instance's `door`; the
    /// shared copy, every owner's.
    #[test]
    fn a_health_take_down_reaches_what_weft_infra_stop_of_the_copy_reaches() {
        use weft_broker_client::activation::ActivationLifecycle;
        let project = door_and_tick();
        assert_eq!(crate::infra_reach::depends_on(&set_up(&project, "door", Owner::Shared, ActivationLifecycle::active()), &project), vec!["svc"]);
        let bob = Owner::Instance(InstanceId::new("bob").unwrap());
        let activations = vec![
            set_up(&project, "door", Owner::Shared, ActivationLifecycle::active()),
            set_up(&project, "door", ada(), ActivationLifecycle::active()),
            set_up(&project, "door", bob.clone(), ActivationLifecycle::active()),
            set_up(&project, "door", Owner::Instance(InstanceId::new("carl").unwrap()), ActivationLifecycle::holding()),
            set_up(&project, "tick", Owner::Shared, ActivationLifecycle::active()),
            set_up(&project, "tick", ada(), ActivationLifecycle::active()),
        ];
        // What `weft infra stop` of the copy takes down
        // (`crate::api::infra::triggers_reached`).
        let infra_stop_reaches = |copy: &InfraCopy| -> Vec<ActivationKey> {
            let mut keys = crate::infra_reach::live_triggers_reached(&copy_reach(copy), &activations, &project);
            keys.sort();
            keys
        };
        let ada_svc = InfraCopy { node_id: "svc".into(), instance: Some(InstanceId::new("ada").unwrap()) };
        let shared_svc = InfraCopy { node_id: "svc".into(), instance: None };
        assert_eq!(health_take_down_targets(&project, &activations, std::slice::from_ref(&ada_svc)), vec![ActivationKey::new("door", ada())]);
        assert_eq!(health_take_down_targets(&project, &activations, std::slice::from_ref(&ada_svc)), infra_stop_reaches(&ada_svc));
        let shared = health_take_down_targets(&project, &activations, std::slice::from_ref(&shared_svc));
        assert_eq!(shared, vec![ActivationKey::new("door", Owner::Shared), ActivationKey::new("door", ada()), ActivationKey::new("door", bob)]);
        assert_eq!(shared, infra_stop_reaches(&shared_svc));
    }

    /// A health recovery brings back only what the health check took
    /// down: never a trigger a person took down, nor one an infra command
    /// took down with the copies it reads (`weft infra stop`), even when
    /// nothing is still broken; and of what the health check took, not
    /// one whose cut contains a copy still broken.
    #[test]
    fn a_health_recovery_brings_back_only_what_the_health_check_took_down() {
        use weft_broker_client::activation::{ActivationLifecycle, WentDown};
        let project = door_and_tick();
        let down = |went_down: Option<WentDown>| ActivationLifecycle { went_down, ..ActivationLifecycle::holding() };
        let (bob, carl, dee) = (
            Owner::Instance(InstanceId::new("bob").unwrap()),
            Owner::Instance(InstanceId::new("carl").unwrap()),
            Owner::Instance(InstanceId::new("dee").unwrap()),
        );
        let activations = vec![
            set_up(&project, "door", ada(), down(Some(WentDown::Health))),
            set_up(&project, "door", bob, down(None)),
            set_up(&project, "door", carl, down(Some(WentDown::Infra { first_command: 4 }))),
            set_up(&project, "door", dee.clone(), down(Some(WentDown::Health))),
        ];
        let per_instance = |place: &str| place == "svc";
        assert_eq!(
            health_restore_targets(&project, per_instance, &activations, &[], |_| true),
            vec![ActivationKey::new("door", ada()), ActivationKey::new("door", dee.clone())]
        );
        let dee_svc = InfraCopy { node_id: "svc".into(), instance: Some(InstanceId::new("dee").unwrap()) };
        assert_eq!(health_restore_targets(&project, per_instance, &activations, &[dee_svc], |_| true), vec![ActivationKey::new("door", ada())]);
    }

    /// `RunOutcome::Cancelled` MUST project to `LifecycleOutcome::Cancelled`
    /// + a reason in `outcome_message`, so `wait_for_command` consumers
    /// tell "no longer applicable" from a failure.
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
