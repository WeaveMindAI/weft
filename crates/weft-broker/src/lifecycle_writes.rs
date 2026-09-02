//! The fenced lifecycle writes: the SQL behind the supervisor's
//! `set_status` and `command_complete`, plus the one answer every
//! fenced write gives when it matched no row. Pool-level (no
//! `BrokerState`, no HTTP) so the layer-3 db suite drives the exact
//! statements the handlers serve; the handlers in `handlers.rs` are
//! the thin scope-checked HTTP wrappers around these.

use sqlx::PgPool;

use weft_broker_client::lifecycle_command::owns_project_predicate;
use weft_broker_client::protocol::{
    LifecycleOutcome, SupervisorCommandCompleteRequest, SupervisorSetStatusRequest,
};

/// What a fenced lifecycle write did. The two stale answers are
/// deliberately distinct because the supervisor must do different
/// things with them (see `WriteOutcome` in the client crate, the wire
/// twin of this): `Displaced`, the pod no longer owns the project, it
/// leaves the command for the new owner; `Gone`, the pod still owns
/// the project and the target itself is not there (row removed, unit
/// not in the roster, command already completed), the pod's work for
/// that target is moot and the command proceeds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FencedWrite {
    Applied,
    Displaced,
    Gone,
}

/// The answer to a fenced write that matched no row: one ownership
/// SELECT settles which predicate failed (the WHERE that just failed
/// cannot say). Displaced when `pod_name` no longer holds the
/// project's `infra_owner` lease, Gone otherwise.
pub async fn stale_answer(pool: &PgPool, pod_name: &str, project_id: &str) -> anyhow::Result<FencedWrite> {
    let owns: bool = sqlx::query_scalar(&format!(
        "SELECT {owns}",
        owns = owns_project_predicate("$1", "$2"),
    ))
    .bind(pod_name)
    .bind(project_id)
    .fetch_one(pool)
    .await?;
    Ok(if owns { FencedWrite::Gone } else { FencedWrite::Displaced })
}

/// Roll a units jsonb expression up to one node status (worst-of-units
/// by `InfraNodeStatus::rollup_rank`). Must match
/// `InfraNodeStatus::rollup_rank` in the protocol crate. Empty map ->
/// 'stopped' (the rank-0 default). `units_expr` is the SQL expression
/// holding the units map: this is parameterized (not hardcoded to the
/// `units_json` column) because in a single UPDATE every SET RHS is
/// evaluated against the OLD row, so rolling up the bare column would
/// use the PRE-update unit statuses. We must roll up the SAME new-units
/// expression we're writing.
fn rollup_sql(units_expr: &str) -> String {
    format!(
        "(SELECT COALESCE( \
            (SELECT v->>'status' FROM jsonb_each({units_expr}) AS e(k, v) \
             ORDER BY CASE v->>'status' \
               WHEN 'terminating' THEN 7 WHEN 'stopping' THEN 6 \
               WHEN 'provisioning' THEN 5 WHEN 'failed' THEN 4 \
               WHEN 'flaky' THEN 3 WHEN 'running' THEN 2 \
               WHEN 'stopped' THEN 1 ELSE 0 END DESC \
             LIMIT 1), \
            'stopped'))"
    )
}

/// Stamp a status on an `infra_node` row, per unit or node-wide, under
/// the write's fence.
///
/// Per-unit (`unit = Some`): set that unit's status inside `units_json`,
/// then recompute the node-level `status` as the rollup over the
/// UPDATED units. Node-wide (`unit = None`): set every unit's status
/// AND the node status to the same value (a lifecycle-driven uniform
/// transition like Stopping/Terminating). Both run in ONE UPDATE so the
/// per-unit write and the rollup are atomic, and so the fence WHERE
/// clause guards them together. CRITICAL: the rollup must read the NEW
/// units expression, not the `units_json` column. In one UPDATE,
/// Postgres evaluates every SET RHS against the pre-update row, so
/// `status = rollup_sql("units_json")` would roll up the OLD statuses
/// (leaving e.g. `stopping` after the unit already went `stopped`).
///
/// Per-unit, the unit's presence in the roster is part of the fence
/// (`units_json ? $1`), so a stamp for a unit the row does not carry
/// matches no row and answers Gone. That is a real race for the health
/// engine (its roster was read at the top of the tick) and never a
/// write: the earlier COALESCE-seeded shape turned it into a
/// `{"status": ...}` stub entry that fails every UnitRuntime decode of
/// the row (a permanent brick; see `decode_units_json` for the
/// repair), and a plain `(units_json->$1) || ...` would NULL the column
/// and fail the UPDATE as an error instead of a stale answer.
///
/// The fence itself: with a `command_id`, the row must still be
/// targeted by that uncompleted command AND `pod_name` must still hold
/// the project's `infra_owner` lease, evaluated inside the UPDATE's
/// WHERE so check and write share one row snapshot (no TOCTOU window);
/// the instant ownership moves, the write is rejected and the command
/// flows to the new owner. Without one (the autonomous health
/// reconcile), the write is fenced against ANY uncompleted command for
/// this project/node: the lifecycle handler owns the status while a
/// user action is in flight, and the EXISTS is evaluated atomically
/// with the write so a command that appeared after the supervisor's
/// tick-level gate still blocks here; that branch has no ownership
/// term, so its stale answer is always Gone.
pub async fn set_status(pool: &PgPool, req: &SupervisorSetStatusRequest) -> anyhow::Result<FencedWrite> {
    let (set_clause, unit_fence) = if req.unit.is_some() {
        let new_units = "jsonb_set(units_json, ARRAY[$1], \
             (units_json->$1) || jsonb_build_object('status', $2::text))";
        (
            format!(
                "units_json = {new_units}, status = {rollup}, failure_stage = $3, failure_message = $4",
                rollup = rollup_sql(new_units),
            ),
            " AND units_json ? $1",
        )
    } else {
        // Node-wide: every unit's status and the node status become
        // $2 directly. The rollup over all-equal units would be $2 too,
        // except for a unit-less roster (a spec with only shared
        // resources), where the rollup's empty-map default is 'stopped'
        // and would turn a Failed / Terminating stamp into a cleanly
        // stopped node.
        let new_units = "(SELECT COALESCE(jsonb_object_agg(k, v || jsonb_build_object('status', $2::text)), '{}'::jsonb) \
            FROM jsonb_each(units_json) AS e(k, v))";
        (
            format!(
                "units_json = {new_units}, status = $2::text, failure_stage = $3, failure_message = $4"
            ),
            "",
        )
    };
    // `$1` is the unit name (or a placeholder, unused when unit=None,
    // but the jsonb_set path needs it bound regardless).
    let unit_key = req.unit.clone().unwrap_or_default();
    let res = if let Some(cid) = req.command_id {
        sqlx::query(&format!(
            "UPDATE infra_node SET {set_clause} \
             WHERE project_id = $5 AND node_id = $6{unit_fence} AND EXISTS ( \
               SELECT 1 FROM infra_lifecycle_command \
               WHERE id = $7 \
                 AND project_id = $5 \
                 AND (node_id = $6 OR node_id IS NULL) \
                 AND completed_at_unix IS NULL \
             ) AND {owns}",
            owns = owns_project_predicate("$8", "$5"),
        ))
        .bind(&unit_key)
        .bind(req.status.as_str())
        .bind(req.failure_stage.map(|s| s.as_str()))
        .bind(req.failure_message.as_deref())
        .bind(&req.project_id)
        .bind(&req.node_id)
        .bind(cid)
        .bind(&req.pod_name)
        .execute(pool)
        .await?
    } else {
        sqlx::query(&format!(
            "UPDATE infra_node SET {set_clause} \
             WHERE project_id = $5 AND node_id = $6{unit_fence} AND NOT EXISTS ( \
               SELECT 1 FROM infra_lifecycle_command \
               WHERE project_id = $5 \
                 AND (node_id = $6 OR node_id IS NULL) \
                 AND completed_at_unix IS NULL \
             )"
        ))
        .bind(&unit_key)
        .bind(req.status.as_str())
        .bind(req.failure_stage.map(|s| s.as_str()))
        .bind(req.failure_message.as_deref())
        .bind(&req.project_id)
        .bind(&req.node_id)
        .execute(pool)
        .await?
    };
    if res.rows_affected() > 0 {
        return Ok(FencedWrite::Applied);
    }
    if req.command_id.is_some() {
        stale_answer(pool, &req.pod_name, &req.project_id).await
    } else {
        Ok(FencedWrite::Gone)
    }
}

/// Stamp a lifecycle command terminal: success (`error = None`),
/// failure (`error = Some`), or a user-requested cancellation the
/// supervisor honored mid-command (`cancelled`; `error` then carries
/// the halt point as the outcome message, never counted as a failure).
/// Only the pod that currently OWNS the project may complete it: a
/// supervisor that lost ownership mid-command must NOT, because leaving
/// the command uncompleted is exactly what lets the new owner re-run
/// and finish it. Combined with `completed_at_unix IS NULL` this is
/// "exactly the current owner, exactly once": a second completion, or
/// one for a command deleted with its project, is Gone.
pub async fn complete_command(
    pool: &PgPool,
    req: &SupervisorCommandCompleteRequest,
) -> anyhow::Result<FencedWrite> {
    let outcome = if req.cancelled {
        LifecycleOutcome::Cancelled
    } else {
        match req.error {
            Some(_) => LifecycleOutcome::Failed,
            None => LifecycleOutcome::Succeeded,
        }
    };
    let res = sqlx::query(&format!(
        "UPDATE infra_lifecycle_command \
         SET completed_at_unix = EXTRACT(EPOCH FROM NOW())::BIGINT, \
             outcome = $1, \
             outcome_message = $2 \
         WHERE id = $3 \
           AND completed_at_unix IS NULL \
           AND {owns}",
        owns = owns_project_predicate("$4", "project_id"),
    ))
    .bind(outcome.as_str())
    .bind(req.error.as_deref())
    .bind(req.command_id)
    .bind(&req.pod_name)
    .execute(pool)
    .await?;
    if res.rows_affected() > 0 {
        return Ok(FencedWrite::Applied);
    }
    // The project id lives on the command row, so the ownership answer
    // comes from there; no command row at all is a gone target.
    let project: Option<String> =
        sqlx::query_scalar("SELECT project_id FROM infra_lifecycle_command WHERE id = $1")
            .bind(req.command_id)
            .fetch_optional(pool)
            .await?;
    match project {
        Some(project_id) => stale_answer(pool, &req.pod_name, &project_id).await,
        None => Ok(FencedWrite::Gone),
    }
}
