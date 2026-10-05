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
    command_reaches_copy, live_lease_exists, ownable_project, owns_project_predicate, supervisor_work,
    pending_supervisor_command,
};
use weft_broker_client::protocol::{
    InfraLifecycleVerb, LifecycleOutcome, ProjectStatus, RunningPolicy,
    SupervisorCommandCompleteRequest, SupervisorCommandRow, SupervisorProject,
    SupervisorSetStatusRequest, SupervisorSetWaitingRequest, SupervisorSyncOwnershipResponse,
};

/// One supervisor ownership tick, atomically, so two supervisors never
/// end up owning one project. Steps in one transaction:
///   1. Renew this supervisor's existing leases (it is alive and working)
///      over the projects it may own.
///   2. Claim a BATCH of MORE projects it may own with no live owner. `FOR UPDATE
///      SKIP LOCKED` + `ON CONFLICT` make concurrent supervisors partition
///      the free projects without double-claiming.
///   3. Return the full owned set (the work loops act only on these), and
///      which of them this tick took on: freshly claimed, or its own lease
///      revived after it lapsed. A command issued on such a project while
///      nobody held it woke no claim of this supervisor's, so it asks
///      again at once.
/// Claiming and renewing both go through ONE condition,
/// `ownable_project` (infra to manage, copies on this host, or a
/// supervisor command waiting), and the owned set is every live lease
/// this supervisor holds. So a lease over a project with nothing left
/// to own is not renewed and lapses, and the supervisor reports a
/// project lost exactly when its lease is gone, never while a command
/// it runs still keeps the project ownable. All time comes from the DB
/// clock, never the app clock, so a skewed host can't mis-judge lease
/// expiry.
pub async fn sync_ownership(
    pool: &PgPool,
    replica: &str,
    held_projects: &[uuid::Uuid],
) -> anyhow::Result<SupervisorSyncOwnershipResponse> {
    let lease_secs = weft_broker_client::lifecycle_command::infra_owner_lease_secs();
    let mut tx = pool.begin().await?;
    // 1. Renew owned leases. A lease that had lapsed comes back as taken
    //    on this tick (the self-join reads the row as it was before this
    //    statement).
    let mut claimed: Vec<uuid::Uuid> = sqlx::query_scalar::<_, Option<uuid::Uuid>>(&format!(
        "UPDATE infra_owner io \
         SET leased_until_unix = EXTRACT(EPOCH FROM NOW())::BIGINT + $1 \
         FROM infra_owner prior \
         WHERE io.supervisor_replica = $2 AND prior.project_id = io.project_id \
           AND EXISTS (SELECT 1 FROM project p WHERE p.id = io.project_id AND {ownable}) \
         RETURNING CASE WHEN prior.leased_until_unix < EXTRACT(EPOCH FROM NOW())::BIGINT \
                        THEN io.project_id END",
        ownable = ownable_project("p", "$3"),
    ))
    .bind(lease_secs)
    .bind(replica)
    .bind(held_projects)
    .fetch_all(&mut *tx)
    .await
    .context("renew infra_owner leases")?
    .into_iter()
    .flatten()
    .collect();

    // 2. Claim a batch. `free` selects projects with no live owner and
    //    LOCKS them `FOR UPDATE OF p SKIP LOCKED`, so a sibling
    //    supervisor's concurrent claim takes a DISJOINT set. ON CONFLICT
    //    overwrites a stale (expired-lease) row still present ONLY if its
    //    lease is actually expired, so a live owner is never stolen.
    let sql = format!(
        "WITH free AS ( \
             SELECT p.id AS project_id, p.tenant_id \
             FROM project p \
             WHERE {ownable} \
               AND NOT {leased} \
             ORDER BY p.id \
             LIMIT $2 \
             FOR UPDATE OF p SKIP LOCKED \
         ) \
         INSERT INTO infra_owner \
             (project_id, supervisor_replica, tenant_id, leased_until_unix) \
         SELECT project_id, $1, tenant_id, EXTRACT(EPOCH FROM NOW())::BIGINT + $3 \
         FROM free \
         ON CONFLICT (project_id) DO UPDATE \
           SET supervisor_replica = EXCLUDED.supervisor_replica, \
               tenant_id = EXCLUDED.tenant_id, \
               leased_until_unix = EXCLUDED.leased_until_unix \
           WHERE infra_owner.leased_until_unix < EXTRACT(EPOCH FROM NOW())::BIGINT \
         RETURNING project_id",
        ownable = ownable_project("p", "$4"),
        leased = live_lease_exists(None, "p.id"),
    );
    let taken: Vec<uuid::Uuid> = sqlx::query_scalar(&sql)
        .bind(replica)
        .bind(weft_broker_client::lifecycle_command::SUPERVISOR_CLAIM_BATCH)
        .bind(lease_secs)
        .bind(held_projects)
        .fetch_all(&mut *tx)
        .await
        .context("claim infra_owner rows")?;
    claimed.extend(taken);

    // 3. When this supervisor next has something to look at: now, while a
    //    project it owns gives it work; or when the soonest lease a
    //    sibling holds over such a project, or over one the host holds
    //    copies of (which a sibling's gone-copy sweep may have left
    //    half done), lapses: the sibling may be gone, and only a lapsed
    //    lease is taken over. One of those that nobody holds (past this
    //    tick's batch) counts as lapsed.
    let (owns_work, others_lapse_in_secs): (bool, Option<i64>) = sqlx::query_as(&format!(
        "SELECT \
           EXISTS (SELECT 1 FROM project p JOIN infra_owner io ON io.project_id = p.id \
                   WHERE io.supervisor_replica = $1 AND {work}), \
           (SELECT MIN(GREATEST(COALESCE(io.leased_until_unix, 0) - EXTRACT(EPOCH FROM NOW())::BIGINT, 0)) \
              FROM project p LEFT JOIN infra_owner io ON io.project_id = p.id \
             WHERE io.supervisor_replica IS DISTINCT FROM $1 AND ({work} OR p.id = ANY($2)))",
        work = supervisor_work("p"),
    ))
    .bind(replica)
    .bind(held_projects)
    .fetch_one(&mut *tx)
    .await
    .context("look for what the supervisors have to do")?;

    // 4. Return the full owned set (joined to current project state).
    let owned = owned_projects(&mut *tx, replica).await?;
    tx.commit().await.context("commit sync_ownership tx")?;
    claimed.retain(|id| owned.iter().any(|p| p.project_id == *id));
    claimed.sort_unstable();
    claimed.dedup();
    Ok(SupervisorSyncOwnershipResponse { owned, claimed, owns_work, others_lapse_in_secs })
}

/// The projects a supervisor owns (every live `infra_owner` lease it
/// holds, whatever made the project ownable), joined to live project state,
/// against any executor (a pool or the ownership tick's transaction).
pub async fn owned_projects<'e, E>(executor: E, replica: &str) -> anyhow::Result<Vec<SupervisorProject>>
where
    E: sqlx::PgExecutor<'e>,
{
    use sqlx::Row;
    // Every activation of the owned projects rides along (a project with
    // none has one row of NULLs from the LEFT JOIN): the health loop reads
    // the project's listening as ONE lifecycle, the aggregate over all its
    // activations, every owner's, and whether any activation it took down
    // itself is still down.
    let sql = format!(
        "SELECT p.id AS project_id, p.tenant_id, \
                a.status, a.accepting_fires, a.fires_visible_to_consumers, a.fires_deadline_unix, \
                a.drain_deadline_unix, a.deactivated_by_health, a.activating_execution_id \
         FROM infra_owner io \
         JOIN project p ON p.id = io.project_id \
         LEFT JOIN trigger_activation a ON a.project_id = p.id \
         WHERE {owns} \
         ORDER BY p.id",
        owns = owns_project_predicate("$1", "p.id"),
    );
    let rows = sqlx::query(&sql)
        .bind(replica)
        .fetch_all(executor)
        .await?;
    let mut projects: Vec<(uuid::Uuid, String, Vec<weft_broker_client::activation::ActivationLifecycle>)> = Vec::new();
    for r in &rows {
        let project_id: uuid::Uuid = r.try_get("project_id").context("decode project_id")?;
        if projects.last().map(|p| p.0) != Some(project_id) {
            projects.push((
                project_id,
                r.try_get("tenant_id").context("decode tenant_id")?,
                Vec::new(),
            ));
        }
        let status: Option<String> = r.try_get("status").context("decode status")?;
        let Some(status) = status else { continue };
        let lifecycle = weft_broker_client::activation::ActivationLifecycle {
            status: ProjectStatus::parse(&status)
                .with_context(|| format!("trigger_activation.status='{status}' is not a known status"))?,
            accepting_fires: r.try_get("accepting_fires").context("decode accepting_fires")?,
            fires_visible_to_consumers: r.try_get("fires_visible_to_consumers").context("decode visibility")?,
            fires_deadline_unix: r.try_get("fires_deadline_unix").context("decode deadline")?,
            drain_deadline_unix: r.try_get("drain_deadline_unix").context("decode drain deadline")?,
            deactivated_by_health: r.try_get("deactivated_by_health").context("decode deactivated_by_health")?,
            activating_execution_id: r.try_get("activating_execution_id").context("decode activating_execution_id")?,
        };
        projects.last_mut().expect("pushed above").2.push(lifecycle);
    }
    Ok(projects
        .into_iter()
        .map(|(project_id, tenant_id, lifecycles)| {
            let aggregate = weft_broker_client::activation::aggregate(&lifecycles);
            SupervisorProject {
                project_id,
                tenant_id,
                status: aggregate.status,
                health_parked: lifecycles
                    .iter()
                    .any(|l| l.deactivated_by_health && l.status == ProjectStatus::Inactive),
            }
        })
        .collect())
}

/// The command `claimer_replica` runs next: the oldest uncompleted one of a
/// project it owns (the `infra_owner` exclusive lease) that it is not
/// already running (`busy_commands`) and that no older uncompleted command
/// of the project reaches a copy of (`commands_overlap`), or `None`.
///
/// Ownership is the supervisor's one single-actor authority: exclusive
/// (one process per project) and renewed on every ownership tick, so two
/// supervisors never change one project's infrastructure. Inside the owner,
/// the commands that touch a copy run in the order they were issued, since
/// a younger one waits until every older one touching one of its copies has
/// completed, and the ones touching different copies run side by side:
/// three instances started together come up together.
///
/// There is no per-command claim lease and no row update: this is a
/// pure read. A lease would be redundant with exclusive ownership, and
/// its fixed expiry would wrongly let a sibling re-run a long command
/// mid-flight. A command stays uncompleted until its owner finishes it;
/// if ownership moves mid-command, every write from the old owner is
/// refused (`owns_project_predicate` on the fenced writes below) and
/// the new owner runs it again, which is safe because the supervisor's
/// infrastructure work is declarative.
///
/// Only the supervisor's verbs: `deactivate` and `reactivate` are the
/// dispatcher's, claimed by dispatchers under their own
/// `claimed_by_replica` lease.
pub async fn next_command(
    pool: &PgPool,
    claimer_replica: &str,
    busy_commands: &[i64],
) -> anyhow::Result<Option<SupervisorCommandRow>> {
    let sql = format!(
        "SELECT c.id, c.project_id, c.node_id, c.verb, c.running_policy, c.spec_json, c.force, \
                c.drain_timeout_secs, c.instance_id, c.every_copy \
         FROM infra_lifecycle_command c \
         WHERE {pending} \
           AND NOT (c.id = ANY($2)) \
           AND {owns} \
           AND NOT EXISTS ( \
               SELECT 1 FROM infra_lifecycle_command o \
               WHERE {older_pending} AND o.project_id = c.project_id AND o.id < c.id AND {overlap} \
           ) \
         ORDER BY c.id ASC \
         LIMIT 1",
        pending = pending_supervisor_command("c"),
        older_pending = pending_supervisor_command("o"),
        owns = owns_project_predicate("$1", "c.project_id"),
        overlap = weft_broker_client::lifecycle_command::commands_overlap("o", "c"),
    );
    let row = sqlx::query(&sql)
        .bind(claimer_replica)
        .bind(busy_commands)
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
    /// Which copies of the node it acts on.
    pub copies: &'a weft_core::instance::Copies,
    pub verb: InfraLifecycleVerb,
    pub running_policy: Option<RunningPolicy>,
    pub spec_json: Option<&'a serde_json::Value>,
    pub issued_by_replica: &'a str,
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
/// copy, so a worker restart retrying the call never issues it twice:
/// the partial unique index `uq_lifecycle_cmd_pending_apply` allows one
/// pending apply per (project_id, node_id, instance_id), and on a clash
/// the no-op `DO UPDATE` hands back the existing row's id in the same
/// statement (a `DO NOTHING` would return no row and need a second read
/// that races the row's completion). Other verbs never match that
/// index's predicate.
pub async fn issue_command(pool: &PgPool, cmd: &IssuedCommand<'_>) -> anyhow::Result<Option<i64>> {
    let (instance_id, every_copy) = cmd.copies.columns();
    sqlx::query_scalar(
        "INSERT INTO infra_lifecycle_command \
         (tenant_id, project_id, node_id, verb, running_policy, \
          spec_json, issued_by_replica, issued_at_unix, instance_id, every_copy) \
         SELECT $1, p.id, $3, $4, $5, $6, $7, EXTRACT(EPOCH FROM NOW())::BIGINT, $8, $9 \
         FROM project p WHERE p.id = $2 \
         FOR KEY SHARE OF p \
         ON CONFLICT (project_id, node_id, instance_id) \
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
    .bind(cmd.issued_by_replica)
    .bind(instance_id)
    .bind(every_copy)
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
    instance: Option<&weft_core::instance::InstanceId>,
    kind: &str,
    payload: &serde_json::Value,
) -> anyhow::Result<Option<i64>> {
    sqlx::query_scalar(
        "INSERT INTO infra_event \
         (tenant_id, project_id, node_id, kind, payload, at_unix, instance_id) \
         SELECT $1, p.id, $3, $4, $5, EXTRACT(EPOCH FROM NOW())::BIGINT, $6 \
         FROM project p WHERE p.id = $2 \
         FOR KEY SHARE OF p \
         RETURNING id",
    )
    .bind(tenant_id)
    .bind(project_id)
    .bind(node_id)
    .bind(kind)
    .bind(payload)
    .bind(instance.map(|i| i.as_str()))
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
    let instance_id: Option<String> = r.try_get("instance_id")?;
    let every_copy: bool = r.try_get("every_copy")?;
    let copies = weft_core::instance::Copies::from_columns(instance_id, every_copy)
        .map_err(|e| anyhow::anyhow!("infra_lifecycle_command.id={id}: {e}"))?;
    let drain_timeout_secs: i64 = r.try_get("drain_timeout_secs")?;
    Ok(SupervisorCommandRow {
        id,
        project_id,
        node_id,
        copies,
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
/// twin of this): `Displaced`, the process no longer owns the project, it
/// leaves the command for the new owner; `Gone`, the process still owns
/// the project and the target itself is not there (row removed, unit
/// not in the roster, command already completed), the process's work for
/// that target is moot and the command proceeds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FencedWrite {
    Applied,
    Displaced,
    Gone,
}

/// The answer to a fenced write that matched no row: one ownership
/// SELECT settles which predicate failed (the WHERE that just failed
/// cannot say). Displaced when `replica` no longer holds the
/// project's `infra_owner` lease, Gone otherwise.
pub async fn stale_answer(pool: &PgPool, replica: &str, project_id: uuid::Uuid) -> anyhow::Result<FencedWrite> {
    let owns: bool = sqlx::query_scalar(&format!(
        "SELECT {owns}",
        owns = owns_project_predicate("$1", "$2"),
    ))
    .bind(replica)
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
/// The fence itself: `replica` must still hold the project's
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
    // An apply's progress (what it waits on, since when) belongs to the
    // copy while it provisions: a status that leaves provisioning clears
    // it, so a later start never shows an earlier one's.
    let progress = |new_status: &str| {
        format!(
            ", waiting_on = CASE WHEN {new_status} = 'provisioning' THEN waiting_on END, \
             provisioning_since_unix = CASE WHEN {new_status} = 'provisioning' THEN provisioning_since_unix END"
        )
    };
    let (set_clause, unit_fence) = if req.unit.is_some() {
        let new_units = "jsonb_set(units_json, ARRAY[$1], \
             (units_json->$1) || jsonb_build_object('status', $2::text))";
        let rollup = rollup_sql(new_units);
        (
            format!(
                "units_json = {new_units}, status = {rollup}, failure_stage = $3, failure_message = $4{}",
                progress(&rollup),
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
                "units_json = {new_units}, status = $2::text, failure_stage = $3, failure_message = $4{}",
                progress("$2::text"),
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
             WHERE project_id = $5 AND node_id = $6 \
               AND instance_id IS NOT DISTINCT FROM $9{unit_fence} AND EXISTS ( \
               SELECT 1 FROM infra_lifecycle_command c \
               WHERE c.id = $7 \
                 AND c.project_id = $5 \
                 AND {reaches} \
                 AND c.completed_at_unix IS NULL \
             ) AND {owns}",
            reaches = command_reaches_copy("c", "$6", "$9"),
            owns = owns_project_predicate("$8", "$5"),
        ))
        .bind(&unit_key)
        .bind(req.status.as_str())
        .bind(req.failure_stage.map(|s| s.as_str()))
        .bind(req.failure_message.as_deref())
        .bind(req.project_id)
        .bind(&req.node_id)
        .bind(cid)
        .bind(&req.replica)
        .bind(req.instance.as_ref().map(|m| m.as_str()))
        .execute(pool)
        .await?
    } else {
        sqlx::query(&format!(
            "UPDATE infra_node SET {set_clause} \
             WHERE project_id = $5 AND node_id = $6 \
               AND instance_id IS NOT DISTINCT FROM $8{unit_fence} AND NOT EXISTS ( \
               SELECT 1 FROM infra_lifecycle_command c \
               WHERE c.project_id = $5 \
                 AND {reaches} \
                 AND c.completed_at_unix IS NULL \
             ) AND {owns}",
            reaches = command_reaches_copy("c", "$6", "$8"),
            owns = owns_project_predicate("$7", "$5"),
        ))
        .bind(&unit_key)
        .bind(req.status.as_str())
        .bind(req.failure_stage.map(|s| s.as_str()))
        .bind(req.failure_message.as_deref())
        .bind(req.project_id)
        .bind(&req.node_id)
        .bind(&req.replica)
        .bind(req.instance.as_ref().map(|m| m.as_str()))
        .execute(pool)
        .await?
    };
    if res.rows_affected() > 0 {
        return Ok(FencedWrite::Applied);
    }
    stale_answer(pool, &req.replica, req.project_id).await
}

/// Record what the apply `req.command_id` waits on for its copy. The
/// command must still be an uncompleted apply that reaches the copy, and `replica`
/// must still own the project, all in the UPDATE's own WHERE.
pub async fn set_waiting(pool: &PgPool, req: &SupervisorSetWaitingRequest) -> anyhow::Result<FencedWrite> {
    let res = sqlx::query(&format!(
        "UPDATE infra_node SET waiting_on = $1 \
         WHERE project_id = $2 AND node_id = $3 \
           AND instance_id IS NOT DISTINCT FROM $4 AND EXISTS ( \
           SELECT 1 FROM infra_lifecycle_command c \
           WHERE c.id = $5 \
             AND c.project_id = $2 \
             AND c.verb = 'apply' \
             AND {reaches} \
             AND c.completed_at_unix IS NULL \
         ) AND {owns}",
        reaches = command_reaches_copy("c", "$3", "$4"),
        owns = owns_project_predicate("$6", "$2"),
    ))
    .bind(&req.waiting)
    .bind(req.project_id)
    .bind(&req.node_id)
    .bind(req.instance.as_ref().map(|m| m.as_str()))
    .bind(req.command_id)
    .bind(&req.replica)
    .execute(pool)
    .await?;
    if res.rows_affected() > 0 {
        return Ok(FencedWrite::Applied);
    }
    stale_answer(pool, &req.replica, req.project_id).await
}

/// Stamp a lifecycle command terminal: success (`error = None`),
/// failure (`error = Some`), or a user-requested cancellation the
/// supervisor honored mid-command (`cancelled`; `error` then carries
/// the halt point as the outcome message, never counted as a failure).
/// Only the process that currently OWNS the project may complete it: a
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
    .bind(&req.replica)
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
        Some(project_id) => stale_answer(pool, &req.replica, project_id).await,
        None => Ok(FencedWrite::Gone),
    }
}

/// How many of `project`'s runs the copies `copies` serve are live:
/// started, not terminal, and not parked on a resume. A parked run (a
/// form waiting for input, a timer waiting to fire) holds no worker and
/// does nothing until it resumes; counting it would deadlock
/// `running_policy=wait` against any project with a long-lived parked
/// trigger fire. An instance's copy serves only that instance's runs; the
/// shared copy (and every copy together) serves every run of the project.
pub async fn live_run_count(pool: &PgPool, project: uuid::Uuid, copies: &weft_core::instance::Copies) -> anyhow::Result<i64> {
    let live = |instance_clause: &str| {
        format!(
            "SELECT COUNT(*)::bigint \
             FROM execution ec \
             WHERE ec.project_id = $1 \
               {instance_clause} \
               AND {} \
               AND NOT {}",
            weft_journal::unrecorded::LIVE_RUN_SQL,
            weft_journal::RUN_PARKED_SQL
        )
    };
    let count = match copies {
        weft_core::instance::Copies::Instance(instance) => {
            sqlx::query_scalar(&live("AND ec.instance_id = $2")).bind(project).bind(instance.as_str()).fetch_one(pool).await?
        }
        weft_core::instance::Copies::Shared | weft_core::instance::Copies::Every => {
            sqlx::query_scalar(&live("")).bind(project).fetch_one(pool).await?
        }
    };
    Ok(count)
}
