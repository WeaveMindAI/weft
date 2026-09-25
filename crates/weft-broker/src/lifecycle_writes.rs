//! The SQL behind the supervisor's lifecycle calls: its ownership tick
//! (`sync_ownership`), which command it runs next (`next_command`), the
//! inserts that issue a command or record an infra event
//! (`issue_command`, `record_event`), and the fenced writes behind its
//! `set_status` and `command_complete`, plus the one answer every
//! fenced write gives when it matched no row. Pool-level (no
//! `BrokerState`, no HTTP) so the layer-3 db suite drives the exact
//! statements the handlers serve; the handlers in `handlers.rs` are
//! the thin scope-checked HTTP wrappers around these.

use anyhow::Context as _;
use sqlx::PgPool;

use weft_broker_client::lifecycle_command::{
    claimable_project, live_lease_exists, owns_project_predicate, pending_supervisor_command,
};
use weft_broker_client::protocol::{
    InfraLifecycleVerb, LifecycleOutcome, ProjectStatus, RunningPolicy,
    SupervisorCommandCompleteRequest, SupervisorCommandRow, SupervisorProject,
    SupervisorSetStatusRequest, SupervisorSyncOwnershipResponse,
};

/// One supervisor ownership tick, atomically, so two supervisors never
/// end up owning one project. Steps in one transaction:
///   0. Record this pod's reported memory pressure on its registry row
///      (the dispatcher's placement + scale-down read it).
///   1. Renew this pod's existing leases (it is alive and working).
///   2. Claim a BATCH of MORE projects, but only while the pod is
///      below the shared memory saturation threshold (a saturated pod
///      keeps what it owns and takes on no more; the dispatcher then
///      spawns another supervisor). Claiming is memory-gated, not
///      count-gated, so load is the SAME metric as the listener.
///      `FOR UPDATE SKIP LOCKED` + `ON CONFLICT` make concurrent
///      supervisors partition the free projects without double-claiming.
///   3. Return the full owned set (the work loops act only on these),
///      and which of them this tick took on: freshly claimed, or its
///      own lease revived after it lapsed. A command issued on such a
///      project while nobody held it woke no claim of this pod's, so
///      the pod asks again at once.
/// A project is eligible only if it is `claimable_project` (only
/// namespaced/paid-tier projects have infra). All time comes from the DB clock
/// (`EXTRACT(EPOCH FROM NOW())`), never the app clock, so a skewed
/// dispatcher/broker host can't mis-judge lease expiry.
pub async fn sync_ownership(
    pool: &PgPool,
    pod_name: &str,
    mem_pressure: f64,
) -> anyhow::Result<SupervisorSyncOwnershipResponse> {
    let lease_secs = weft_broker_client::lifecycle_command::infra_owner_lease_secs();
    let mut tx = pool.begin().await?;

    // 0. Record reported memory pressure and read back whether this pod
    //    is a working member of the pool: it has a registry row, and the
    //    dispatcher has not marked it draining (scaled down). Anything
    //    else renews and claims nothing. A draining pod's leases were
    //    released for re-adoption, and re-grabbing them would defeat
    //    consolidation. A pod with no row was reaped and is on its way
    //    out (its pod lives on until the cluster stops it, and in that
    //    time it would otherwise take back the very projects its drain
    //    handed over), or its spawn has not committed the row yet, in
    //    which case it owns nothing and the next tick finds its row.
    let member: Option<bool> = sqlx::query_scalar(
        "UPDATE supervisor_pod SET mem_pressure = $1 WHERE pod_name = $2 RETURNING NOT draining",
    )
    .bind(mem_pressure)
    .bind(pod_name)
    .fetch_optional(&mut *tx)
    .await
        .context("record supervisor mem_pressure")?;
    let working = member == Some(true);
    let mut claimed: Vec<uuid::Uuid> = Vec::new();

    // 1. Renew owned leases, only while working (a leaving pod's leases
    //    lapse or stay released so survivors adopt them; the drain
    //    already deleted them, this guards against a renew racing that
    //    delete).
    //    A lease that had lapsed comes back as taken on this tick (the
    //    self-join reads the row as it was before this statement).
    if working {
        let revived: Vec<uuid::Uuid> = sqlx::query_scalar::<_, Option<uuid::Uuid>>(
            "UPDATE infra_owner io \
             SET leased_until_unix = EXTRACT(EPOCH FROM NOW())::BIGINT + $1 \
             FROM infra_owner prior \
             WHERE io.supervisor_pod = $2 AND prior.project_id = io.project_id \
             RETURNING CASE WHEN prior.leased_until_unix < EXTRACT(EPOCH FROM NOW())::BIGINT \
                            THEN io.project_id END",
        )
        .bind(lease_secs)
        .bind(pod_name)
        .fetch_all(&mut *tx)
        .await
        .context("renew infra_owner leases")?
        .into_iter()
        .flatten()
        .collect();
        claimed.extend(revived);
    }

    // 2. Claim a batch, but only while under the memory saturation
    //    threshold AND working. At/above saturation claim nothing (a
    //    saturated pod keeps what it owns); a pod on its way out, or not
    //    registered yet, takes on nothing.
    let headroom = if takes_on_projects(working, mem_pressure) {
        weft_broker_client::lifecycle_command::SUPERVISOR_CLAIM_BATCH
    } else {
        0
    };
    if headroom > 0 {
        // Atomic claim via a CTE: `free` selects projects with no live
        // owner and LOCKS them `FOR UPDATE OF p SKIP LOCKED`, so a
        // sibling supervisor's concurrent claim takes a DISJOINT set
        // (never the same row). The INSERT then takes the EXCLUSIVE
        // `infra_owner` lease for each. ON CONFLICT covers a stale
        // (expired-lease) row still physically present: we overwrite it
        // ONLY if its lease is actually expired, so a live owner is
        // never stolen. Rows we lock are guaranteed free at insert time
        // because the lock is held to the end of the tx.
        let sql = format!(
            "WITH free AS ( \
                 SELECT p.id AS project_id, p.project_namespace, p.tenant_id \
                 FROM project p \
                 WHERE {claimable} \
                   AND NOT {leased} \
                 ORDER BY p.id \
                 LIMIT $2 \
                 FOR UPDATE OF p SKIP LOCKED \
             ) \
             INSERT INTO infra_owner \
                 (project_id, supervisor_pod, namespace, tenant_id, leased_until_unix) \
             SELECT project_id, $1, project_namespace, tenant_id, \
                    EXTRACT(EPOCH FROM NOW())::BIGINT + $3 \
             FROM free \
             ON CONFLICT (project_id) DO UPDATE \
               SET supervisor_pod = EXCLUDED.supervisor_pod, \
                   namespace = EXCLUDED.namespace, \
                   tenant_id = EXCLUDED.tenant_id, \
                   leased_until_unix = EXCLUDED.leased_until_unix \
               WHERE infra_owner.leased_until_unix < EXTRACT(EPOCH FROM NOW())::BIGINT \
             RETURNING project_id",
            claimable = claimable_project("p"),
            leased = live_lease_exists(None, "p.id"),
        );
        let taken: Vec<uuid::Uuid> = sqlx::query_scalar(&sql)
            .bind(pod_name)
            .bind(headroom)
            .bind(lease_secs)
            .fetch_all(&mut *tx)
            .await
            .context("claim infra_owner rows")?;
        claimed.extend(taken);
    }

    // 3. Return the full owned set (joined to current project state).
    let owned = owned_projects(&mut *tx, pod_name).await?;
    tx.commit().await.context("commit sync_ownership tx")?;
    claimed.sort_unstable();
    claimed.dedup();
    Ok(SupervisorSyncOwnershipResponse { owned, claimed })
}

/// Whether a supervisor pod takes on more projects: it is a working
/// member of the pool (registered, not draining) and below the shared
/// memory saturation threshold. The ownership tick claims only for such
/// a pod, and only such a pod is told a command waits on a project
/// nobody owns.
pub fn takes_on_projects(working: bool, mem_pressure: f64) -> bool {
    working
        && !weft_platform_traits::is_saturated(
            mem_pressure,
            weft_platform_traits::SATURATION_MEM_FRACTION,
        )
}

/// Whether `pod_name` takes on more projects right now, read off its
/// registry row (see [`takes_on_projects`]): no row, draining, or
/// saturated all answer false.
pub async fn pod_takes_on_projects(pool: &PgPool, pod_name: &str) -> anyhow::Result<bool> {
    let row: Option<(bool, f64)> = sqlx::query_as(
        "SELECT draining, mem_pressure FROM supervisor_pod WHERE pod_name = $1",
    )
    .bind(pod_name)
    .fetch_optional(pool)
    .await
    .context("read supervisor_pod membership")?;
    Ok(row.is_some_and(|(draining, pressure)| takes_on_projects(!draining, pressure)))
}

/// The projects a supervisor pod owns, joined to live project state,
/// against any executor (a pool or the ownership tick's transaction).
pub async fn owned_projects<'e, E>(executor: E, pod_name: &str) -> anyhow::Result<Vec<SupervisorProject>>
where
    E: sqlx::PgExecutor<'e>,
{
    use sqlx::Row;
    let sql = format!(
        "SELECT p.id AS project_id, p.tenant_id, p.project_namespace, p.status, \
                p.deactivated_by_health \
         FROM infra_owner io \
         JOIN project p ON p.id = io.project_id \
         WHERE io.supervisor_pod = $1 AND {claimable}",
        claimable = claimable_project("p"),
    );
    let rows = sqlx::query(&sql)
        .bind(pod_name)
        .fetch_all(executor)
        .await?;
    rows.iter()
        .map(|r| {
            let status: String = r.try_get("status").context("decode status")?;
            Ok(SupervisorProject {
                project_id: r.try_get("project_id").context("decode project_id")?,
                tenant_id: r.try_get("tenant_id").context("decode tenant_id")?,
                project_namespace: r.try_get("project_namespace").context("decode project_namespace")?,
                status: ProjectStatus::parse(&status)
                    .with_context(|| format!("project.status='{status}' is not a known ProjectStatus"))?,
                deactivated_by_health: r
                    .try_get("deactivated_by_health")
                    .context("decode deactivated_by_health")?,
            })
        })
        .collect()
}

/// The command `claimer_pod` runs next: the oldest uncompleted one of a
/// project it owns (the `infra_owner` exclusive lease) and is not
/// already running a command for (`busy_projects`), or `None`.
///
/// Ownership is the supervisor's one single-actor authority: exclusive
/// (one pod per project) and renewed on every ownership tick, so two
/// supervisors never change one project's cluster objects. Inside the owner, one
/// project's commands run in order, because the pod names the projects
/// it is busy with and gets none of theirs back; different projects'
/// commands run side by side.
///
/// There is no per-command claim lease and no row update: this is a
/// pure read. A lease would be redundant with exclusive ownership, and
/// its fixed expiry would wrongly let a sibling re-run a long command
/// mid-flight. A command stays uncompleted until its owner finishes it;
/// if ownership moves mid-command, every write from the old owner is
/// refused (`owns_project_predicate` on the fenced writes below) and
/// the new owner runs it again, which is safe because the supervisor's
/// cluster work is declarative.
///
/// Only the supervisor's verbs: `deactivate` and `reactivate` are the
/// dispatcher's, claimed by dispatcher pods under their own
/// `claimed_by_pod` lease.
pub async fn next_command(
    pool: &PgPool,
    claimer_pod: &str,
    busy_projects: &[uuid::Uuid],
) -> anyhow::Result<Option<SupervisorCommandRow>> {
    let sql = format!(
        "SELECT c.id, c.project_id, c.node_id, c.verb, c.running_policy, c.spec_json, c.force, \
                c.drain_timeout_secs \
         FROM infra_lifecycle_command c \
         WHERE {pending} \
           AND NOT (c.project_id = ANY($2)) \
           AND {owns} \
         ORDER BY c.id ASC \
         LIMIT 1",
        pending = pending_supervisor_command("c"),
        owns = owns_project_predicate("$1", "c.project_id"),
    );
    let row = sqlx::query(&sql)
        .bind(claimer_pod)
        .bind(busy_projects)
        .fetch_optional(pool)
        .await?;
    row.as_ref().map(decode_command).transpose()
}

/// Whether a supervisor command waits on a project no supervisor holds
/// a live lease over.
pub async fn unowned_work_waiting(pool: &PgPool) -> anyhow::Result<bool> {
    let sql = format!(
        "SELECT EXISTS ( \
             SELECT 1 FROM infra_lifecycle_command c \
             WHERE {pending} AND NOT {leased} \
         )",
        pending = pending_supervisor_command("c"),
        leased = live_lease_exists(None, "c.project_id"),
    );
    Ok(sqlx::query_scalar(&sql).fetch_one(pool).await?)
}

/// A lifecycle command to issue. The tenant is the project's, never
/// the caller's.
pub struct IssuedCommand<'a> {
    pub tenant_id: &'a str,
    pub project_id: uuid::Uuid,
    pub node_id: Option<&'a str>,
    pub verb: InfraLifecycleVerb,
    pub running_policy: Option<RunningPolicy>,
    pub spec_json: Option<&'a serde_json::Value>,
    pub issued_by_pod: &'a str,
}

/// Issue a lifecycle command; its id, or `None` when the project row is
/// gone. The caller's authorization comes from a project-to-tenant cache
/// that can outlive a removal, so the insert reads the live row itself:
/// a command for a deleted project would sit forever with nobody to run
/// it. It reads it `FOR KEY SHARE`: a removal that has deleted the row
/// but not committed makes the insert wait and then find nothing, and
/// an insert that got the lock first commits before the removal's own
/// transaction deletes the project's commands (`ProjectStore::remove`).
///
/// An apply is deduplicated against an in-flight apply for the same
/// (project, node), so a worker restart retrying the call never issues
/// it twice: the partial unique index `uq_lifecycle_cmd_pending_apply`
/// allows one pending apply per (project_id, node_id), and on a clash
/// the no-op `DO UPDATE` hands back the existing row's id in the same
/// statement (a `DO NOTHING` would return no row and need a second read
/// that races the row's completion). Other verbs never match that
/// index's predicate.
pub async fn issue_command(pool: &PgPool, cmd: &IssuedCommand<'_>) -> anyhow::Result<Option<i64>> {
    sqlx::query_scalar(
        "INSERT INTO infra_lifecycle_command \
         (tenant_id, project_id, node_id, verb, running_policy, \
          spec_json, issued_by_pod, issued_at_unix) \
         SELECT $1, p.id, $3, $4, $5, $6, $7, EXTRACT(EPOCH FROM NOW())::BIGINT \
         FROM project p WHERE p.id = $2 \
         FOR KEY SHARE OF p \
         ON CONFLICT (project_id, node_id) \
           WHERE completed_at_unix IS NULL AND verb = 'apply' \
           DO UPDATE SET issued_at_unix = infra_lifecycle_command.issued_at_unix \
         RETURNING id",
    )
    .bind(cmd.tenant_id)
    .bind(cmd.project_id)
    .bind(cmd.node_id)
    .bind(cmd.verb.as_str())
    .bind(cmd.running_policy.map(|p| p.as_str()))
    .bind(cmd.spec_json)
    .bind(cmd.issued_by_pod)
    .fetch_optional(pool)
    .await
    .context("issue infra_lifecycle_command")
}

/// Record one infra event; its id, or `None` when the project row is
/// gone (read live, for the reason `issue_command` gives).
pub async fn record_event(
    pool: &PgPool,
    tenant_id: &str,
    project_id: uuid::Uuid,
    node_id: Option<&str>,
    kind: &str,
    payload: &serde_json::Value,
) -> anyhow::Result<Option<i64>> {
    sqlx::query_scalar(
        "INSERT INTO infra_event \
         (tenant_id, project_id, node_id, kind, payload, at_unix) \
         SELECT $1, p.id, $3, $4, $5, EXTRACT(EPOCH FROM NOW())::BIGINT \
         FROM project p WHERE p.id = $2 \
         FOR KEY SHARE OF p \
         RETURNING id",
    )
    .bind(tenant_id)
    .bind(project_id)
    .bind(node_id)
    .bind(kind)
    .bind(payload)
    .fetch_optional(pool)
    .await
    .context("record infra_event")
}

/// Decode one `infra_lifecycle_command` row into the typed
/// `SupervisorCommandRow` wire shape. EVERY column read propagates
/// errors; unknown enum values (verb / running_policy) become 500s
/// rather than silent fallbacks.
fn decode_command(r: &sqlx::postgres::PgRow) -> anyhow::Result<SupervisorCommandRow> {
    use sqlx::Row;
    let id: i64 = r.try_get("id")?;
    let project_id: uuid::Uuid = r.try_get("project_id")?;
    let node_id: Option<String> = r.try_get::<Option<String>, _>("node_id")?;
    let verb_str: String = r.try_get("verb")?;
    let verb = InfraLifecycleVerb::parse(&verb_str)
        .ok_or_else(|| anyhow::anyhow!("infra_lifecycle_command.id={id}: unknown verb '{verb_str}'"))?;
    // Nullable: dispatcher verbs (deactivate / reactivate) carry
    // policy inside spec_json; Apply ignores it. Stop / Terminate
    // populate it.
    let running_policy_str: Option<String> = r.try_get("running_policy")?;
    let running_policy = match running_policy_str.as_deref() {
        None => None,
        Some(s) => Some(RunningPolicy::parse(s).ok_or_else(|| {
            anyhow::anyhow!("infra_lifecycle_command.id={id}: unknown running_policy '{s}'")
        })?),
    };
    let spec_json: Option<serde_json::Value> =
        r.try_get::<Option<serde_json::Value>, _>("spec_json")?;
    let force: bool = r.try_get("force")?;
    let drain_timeout_secs: i64 = r.try_get("drain_timeout_secs")?;
    Ok(SupervisorCommandRow {
        id,
        project_id,
        node_id,
        verb,
        running_policy,
        spec_json,
        force,
        drain_timeout_secs: drain_timeout_secs.max(0) as u64,
    })
}

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
pub async fn stale_answer(pool: &PgPool, pod_name: &str, project_id: uuid::Uuid) -> anyhow::Result<FencedWrite> {
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
/// The fence itself: `pod_name` must still hold the project's
/// `infra_owner` lease, on both branches, evaluated inside the UPDATE's
/// WHERE so check and write share one row snapshot (no TOCTOU window);
/// the instant ownership moves, the write is rejected (Displaced). With
/// a `command_id`, the row must also still be targeted by that
/// uncompleted command, so the command flows to the new owner. Without
/// one (the autonomous health reconcile), the write is also fenced
/// against ANY uncompleted command for this project/node: the lifecycle
/// handler owns the status while a user action is in flight, and the
/// EXISTS is evaluated atomically with the write so a command that
/// appeared after the supervisor's tick-level gate still blocks here.
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
        .bind(req.project_id)
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
             ) AND {owns}",
            owns = owns_project_predicate("$7", "$5"),
        ))
        .bind(&unit_key)
        .bind(req.status.as_str())
        .bind(req.failure_stage.map(|s| s.as_str()))
        .bind(req.failure_message.as_deref())
        .bind(req.project_id)
        .bind(&req.node_id)
        .bind(&req.pod_name)
        .execute(pool)
        .await?
    };
    if res.rows_affected() > 0 {
        return Ok(FencedWrite::Applied);
    }
    stale_answer(pool, &req.pod_name, req.project_id).await
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
    let project: Option<uuid::Uuid> =
        sqlx::query_scalar("SELECT project_id FROM infra_lifecycle_command WHERE id = $1")
            .bind(req.command_id)
            .fetch_optional(pool)
            .await?;
    match project {
        Some(project_id) => stale_answer(pool, &req.pod_name, project_id).await,
        None => Ok(FencedWrite::Gone),
    }
}
