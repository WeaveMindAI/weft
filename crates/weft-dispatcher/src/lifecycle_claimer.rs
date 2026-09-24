//! Background loop that claims dispatcher-owned lifecycle commands
//! (`deactivate` / `reactivate`) and runs them. The complementary
//! supervisor-owned verbs (`apply` / `stop` / `terminate`) are
//! claimed by the pooled supervisor pod that owns the project, via the
//! broker.
//!
//! Why a separate loop rather than reusing the supervisor's claim
//! path: trigger-state deactivate/reactivate touches the signal
//! table, which only the dispatcher has Postgres write authority
//! for. Trying to fold this into the supervisor would require
//! granting the supervisor signal-write access, breaking the
//! tenant-scoping invariant.
//!
//! Concurrency: every dispatcher Pod runs one of these loops;
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
use weft_broker_client::lifecycle_command::{InfraCommandSignal, INFRA_COMMAND_CHANNEL};

use crate::infra_lifecycle_command::InfraLifecycleVerb;
use crate::pg_wake::{self, DrainStep, WakeOn};
use crate::state::DispatcherState;

/// A command being issued, for any project: the claimer serves them all.
const WAKE_ON: &[WakeOn] = &[WakeOn {
    channel: INFRA_COMMAND_CHANNEL,
    concerns: |payload| matches!(InfraCommandSignal::parse(payload), Some(InfraCommandSignal::Issued { .. })),
}];

pub fn spawn(state: DispatcherState) {
    crate::app::spawn_supervised("lifecycle_claimer", async move {
        pg_wake::run(
            state.signals.subscribe(),
            WAKE_ON,
            pg_wake::SAFETY_POLL_INTERVAL,
            "weft_dispatcher::lifecycle_claimer",
            || async {
                match claim_and_run_one(&state).await? {
                    true => Ok(DrainStep::More),
                    false => Ok(DrainStep::Done),
                }
            },
        )
        .await;
    });
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

/// Returns true when work was done.
///
/// Contract: if `claim_one` returns a claimed row, EXACTLY ONE
/// `complete()` write follows. The handler returns a typed
/// `RunOutcome` so "no longer applicable" cancellations are
/// distinguished from real failures (both used to write Succeeded
/// or Failed depending on the Result, losing the Cancelled axis).
async fn claim_and_run_one(state: &DispatcherState) -> Result<bool> {
    let Some(row) = claim_one(&state.pg_pool, state.pod_id.as_str()).await? else {
        return Ok(false);
    };
    let outcome = match run_claimed(state, &row).await {
        Ok(o) => o,
        Err(e) => RunOutcome::Failed(e.to_string()),
    };
    complete(&state.pg_pool, row.id, &outcome).await?;
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
            "command cancelled (no longer applicable)"
        ),
    }
    Ok(true)
}

/// Run the verb-specific handler. Reconstructs the typed
/// `LifecycleSpec` from the row's columns via
/// `LifecycleSpec::from_row_columns`, then dispatches on the
/// variant. Encode and decode share one source of truth in
/// `weft-broker-client::protocol`.
async fn run_claimed(state: &DispatcherState, row: &ClaimedCommand) -> Result<RunOutcome> {
    use weft_broker_client::protocol::LifecycleSpec;
    let project_id = row.project_id;
    let spec = LifecycleSpec::from_row_columns(row.verb, row.spec_json.clone())
        .map_err(|e| anyhow::anyhow!("infra_lifecycle_command.id={}: {e}", row.id))?;
    match spec {
        LifecycleSpec::Deactivate(d) => run_deactivate(state, project_id, d).await,
        LifecycleSpec::Reactivate => run_reactivate(state, project_id).await,
    }
}

/// What `claim_one` returns. Verb is parsed at claim time so any
/// parse-fail completes the row immediately (no claimed-but-poison
/// state).
struct ClaimedCommand {
    id: i64,
    project_id: uuid::Uuid,
    verb: InfraLifecycleVerb,
    spec_json: Option<serde_json::Value>,
}

/// Atomic claim: UPDATE the row AND parse its typed columns in one
/// step. A parse failure on a successfully-claimed row would
/// otherwise leave it claimed-but-never-completed, because the
/// `WHERE claimed_by_pod IS NULL` filter excludes it from future
/// claims.
///
/// Strategy: if `try_get` / `parse` fails on a row we just claimed,
/// write `complete(failed, msg)` BEFORE returning the error. The
/// caller's contract ("exactly one complete per claim") stays
/// intact.
async fn claim_one(pool: &PgPool, claimer_pod: &str) -> Result<Option<ClaimedCommand>> {
    use sqlx::Row;
    // Claim predicate (shared with the broker's supervisor claim):
    // either no current claimer OR an expired lease. The lease lets
    // a dispatcher pod that crashed mid-execution release the row
    // automatically after `CLAIM_LEASE_TTL` instead of pinning it.
    let sql = format!(
        "UPDATE infra_lifecycle_command \
         SET claimed_by_pod = $1, claimed_at_unix = EXTRACT(EPOCH FROM NOW())::BIGINT \
         WHERE id = ( \
            SELECT id FROM infra_lifecycle_command \
            WHERE verb IN ({verbs}) \
              AND {predicate} \
            ORDER BY id ASC \
            FOR UPDATE SKIP LOCKED \
            LIMIT 1 \
         ) \
         RETURNING id, project_id, verb, spec_json",
        verbs = weft_broker_client::lifecycle_command::DISPATCHER_VERBS_SQL,
        predicate = weft_broker_client::lifecycle_command::claimable_predicate(),
    );
    let row = sqlx::query(&sql)
        .bind(claimer_pod)
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
            if let Err(complete_err) = complete(pool, id, &outcome).await {
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
    Ok(ClaimedCommand {
        id,
        project_id,
        verb,
        spec_json,
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

async fn complete(pool: &PgPool, id: i64, outcome: &RunOutcome) -> Result<()> {
    let (lc_outcome, message) = project_outcome(outcome);
    sqlx::query(
        "UPDATE infra_lifecycle_command \
         SET completed_at_unix = EXTRACT(EPOCH FROM NOW())::BIGINT, \
             outcome = $2, \
             outcome_message = $3 \
         WHERE id = $1",
    )
    .bind(id)
    .bind(lc_outcome.as_str())
    .bind(message)
    .execute(pool)
    .await?;
    Ok(())
}

async fn run_deactivate(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    spec: weft_broker_client::protocol::DeactivateSpec,
) -> Result<RunOutcome> {
    // The spec round-tripped through the DB (enqueued as JSON in
    // infra_lifecycle_command, deserialized on claim), so it's
    // untrusted input at this point. Validate at the consume boundary
    // so an impossible combo (e.g. wipe+wait) fails loud here rather
    // than silently taking the wrong lifecycle branch downstream. The
    // HTTP handlers validate too; this guards the enqueue-path source.
    spec.validate().map_err(|m| anyhow::anyhow!("invalid deactivate spec: {m}"))?;
    let existed = crate::api::project::deactivate_project_with_mode(
        state,
        project_id,
        spec.mode,
        spec.grace_minutes,
        spec.running_policy,
        spec.drain_timeout_secs
            .unwrap_or(weft_broker_client::protocol::DEFAULT_DRAIN_TIMEOUT_SECS),
        true, // health-loop autonomous park: its auto-recover MAY reactivate this
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

async fn run_reactivate(
    state: &DispatcherState,
    project_id: uuid::Uuid,
) -> Result<RunOutcome> {
    // `activate_inner` returns `Json<ActivateResponse>` for the
    // HTTP path; the claimer doesn't need the body. A NotFound
    // from activate_inner means the project was removed between
    // enqueue and claim: same Cancelled semantic as the deactivate
    // arm above.
    match crate::api::project::activate_inner(
        state,
        project_id,
        crate::api::project::ActivateRequest::default(),
    )
    .await
    {
        Ok(_) => Ok(RunOutcome::Succeeded),
        Err((axum::http::StatusCode::NOT_FOUND, _)) => Ok(RunOutcome::Cancelled(format!(
            "project {project_id} no longer exists"
        ))),
        Err((_, m)) => Err(anyhow::anyhow!("reactivate: {m}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use weft_broker_client::protocol::LifecycleOutcome;

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

