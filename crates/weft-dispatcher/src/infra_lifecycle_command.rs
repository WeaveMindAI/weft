//! `infra_lifecycle_command` table.
//!
//! The single channel between whoever asks for infra work and the
//! tenant's infra-supervisor that does it. Intent rows carry one verb:
//!
//! - **Apply**: provision a new infra node OR roll an existing one
//!   to a new spec. Issued by the broker for a worker's InfraSetup
//!   run (`weft_broker::lifecycle_writes::issue_command`); the body
//!   carries only the `InfraSpec` JSON. The supervisor reads the prior
//!   `infra_node` row itself, compiles the new spec with the real
//!   image-tag map + instance id, hashes, and decides skip / fresh /
//!   replace internally.
//! - **Stop**: scale to zero, preserve PVCs. Issued here.
//! - **Terminate**: delete the copies and their disks, keeping the ones
//!   the node lists in `keepOnTerminate` unless the command's
//!   [`TakeDown::Terminate`] says every disk goes (a member's wipe).
//!   Issued here.
//!
//! The supervisor claims through the broker's held
//! `supervisor_claim_command` (woken by the row's own notification),
//! acts on the install, and writes completion via
//! `supervisor_command_complete`. Dispatcher-claimable verbs
//! (Deactivate, Reactivate, Upgrade) go through `lifecycle_claimer`
//! (this crate). No HTTP between supervisor and dispatcher; both sides
//! talk to Postgres, the supervisor through the broker.
//!
//! - **Upgrade**: a person's `weft infra upgrade`, once its gates passed.
//!   Issued here ([`issue_upgrade`]) and run by a dispatcher
//!   (`api::infra::run_upgrade`): the triggers reading the infra come
//!   down, the stop leg, then the start. The row
//!   is what the person follows, and what another process takes over when
//!   the one running it dies.

use anyhow::Result;
use sqlx::postgres::PgPool;

// `InfraLifecycleVerb` and `RunningPolicy` are the wire contract for
// the `verb` and `running_policy` columns. They live in
// `weft-broker-client::protocol` so the supervisor + dispatcher +
// broker share one source of truth. Re-export here so the rest of
// the dispatcher keeps the short module-relative path.
pub use weft_broker_client::protocol::{InfraLifecycleVerb, RunningPolicy};

pub static GROUP: weft_task_store::SchemaGroup = weft_task_store::SchemaGroup {
    name: "infra_lifecycle_command",
    tables: &["infra_lifecycle_command"],
    ddl: &[
        r#"CREATE TABLE IF NOT EXISTS infra_lifecycle_command (
            id                BIGSERIAL PRIMARY KEY,
            tenant_id         TEXT NOT NULL,
            project_id        UUID NOT NULL,
            node_id           TEXT,
            verb              TEXT NOT NULL,
            -- Nullable because dispatcher-owned verbs
            -- (deactivate / reactivate) carry their running_policy
            -- inside spec_json (Deactivate) or have none at all
            -- (Reactivate). Stop / Terminate populate this; Apply
            -- ignores it. One source of truth per verb.
            running_policy    TEXT,
            spec_json         JSONB,
            issued_by_instance     TEXT NOT NULL,
            issued_at_unix    BIGINT NOT NULL,
            claimed_by_instance    TEXT,
            claimed_at_unix   BIGINT,
            completed_at_unix BIGINT,
            -- 'succeeded' | 'failed' | 'cancelled' | NULL.
            -- NULL = "no result yet" (pending or claimed).
            -- 'failed' = the claimer hit a real error executing the
            --   verb; the worker / caller treats this as a failure.
            -- 'cancelled' = the command was abandoned (e.g. the
            --   targeted node was removed before execution). NOT a
            --   failure; surfaces as "no longer applicable".
            outcome           TEXT,
            -- Human-readable message accompanying the outcome.
            -- NULL on 'succeeded' and on still-pending rows. The
            -- error message on 'failed'; the reason on 'cancelled'.
            -- Decoded into the right typed field based on `outcome`.
            outcome_message   TEXT,
            -- Stop only: force scale-to-zero EVERY unit, ignoring each
            -- unit's `on_stop` (so a NoOp unit comes down too). The
            -- explicit "I accept the downtime, take it all down so I
            -- can update it" override. Default false.
            force             BOOLEAN NOT NULL DEFAULT FALSE,
            -- The user requested cancellation of this command while it
            -- was CLAIMED (in flight). The executing supervisor polls
            -- this between calls to the platform and halts (leaving
            -- per-node partial state visible; no platform applies a
            -- whole project transactionally).
            -- Pending unclaimed rows are cancelled outright (outcome =
            -- 'cancelled') instead of flagged.
            cancel_requested  BOOLEAN NOT NULL DEFAULT FALSE,
            -- Cap on the running_policy=wait drain before the op
            -- proceeds anyway (loud warning). Per-command: the user
            -- picks it with the wait choice; the default mirrors
            -- weft_broker_client::protocol::DEFAULT_DRAIN_TIMEOUT_SECS
            -- (SYNC: the two numbers move together, by migration here).
            drain_timeout_secs BIGINT NOT NULL DEFAULT 60,
            -- Which copies of the infra the command acts on
            -- (`weft_core::member::Copies`): the shared ones (member_id
            -- NULL, every_copy FALSE), one member's (member_id set), or
            -- every copy there is (every_copy TRUE; the project going).
            -- An apply always names exactly one copy.
            member_id         TEXT,
            every_copy        BOOLEAN NOT NULL DEFAULT FALSE
        )"#,
        r#"CREATE INDEX IF NOT EXISTS idx_lifecycle_cmd_pending
              ON infra_lifecycle_command(tenant_id)
              WHERE completed_at_unix IS NULL"#,
        // Claim path for supervisor-owned verbs (apply / stop /
        // terminate). The supervisor `claim_command` SELECT scans
        // uncompleted rows of these verbs ordered by id and gates each
        // on a live `infra_owner` lease (its single-actor authority); it
        // does NOT use the `claimed_by_instance` lease (that is the
        // dispatcher's mechanism). This partial index keyed on id covers
        // that scan and skips dispatcher-owned + completed rows.
        concat!(
            r#"CREATE INDEX IF NOT EXISTS idx_lifecycle_cmd_supervisor_claim
              ON infra_lifecycle_command(id)
              WHERE completed_at_unix IS NULL
                AND verb IN ("#,
            weft_broker_client::supervisor_verbs_sql!(),
            ")",
        ),
        // Mirror for the dispatcher claim loop (deactivate /
        // reactivate / upgrade). No tenant filter: the dispatcher pool claims
        // across all tenants.
        concat!(
            r#"CREATE INDEX IF NOT EXISTS idx_lifecycle_cmd_dispatcher_claim
              ON infra_lifecycle_command(id)
              WHERE completed_at_unix IS NULL
                AND claimed_by_instance IS NULL
                AND verb IN ("#,
            weft_broker_client::dispatcher_verbs_sql!(),
            ")",
        ),
        // Partial unique index: at most one pending apply for a
        // given (project_id, node_id). Stops a worker restart from
        // double-enqueueing the same apply; `infra_enqueue_apply`
        // catches the conflict and returns the existing row's id.
        r#"CREATE UNIQUE INDEX IF NOT EXISTS uq_lifecycle_cmd_pending_apply
              ON infra_lifecycle_command(project_id, node_id, member_id) NULLS NOT DISTINCT
              WHERE completed_at_unix IS NULL AND verb = 'apply'"#,
        // Announce a command when it is issued (its claimers wake) and
        // when it completes (whoever waits on its outcome wakes), from
        // a trigger so no writer (the dispatcher's own verbs, the
        // broker's supervisor and worker paths) can forget.
        // SYNC: the payloads <-> weft_broker_client::lifecycle_command::InfraCommandSignal
        r#"CREATE OR REPLACE FUNCTION infra_command_notify() RETURNS trigger AS $$
            BEGIN
                IF TG_OP = 'INSERT' THEN
                    PERFORM pg_notify('weft_infra_command', 'issued:' || NEW.project_id::text);
                ELSE
                    PERFORM pg_notify('weft_infra_command', 'done:' || NEW.id::text);
                END IF;
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql"#,
        r#"DROP TRIGGER IF EXISTS infra_command_notify_on_issue ON infra_lifecycle_command"#,
        r#"CREATE TRIGGER infra_command_notify_on_issue
            AFTER INSERT ON infra_lifecycle_command
            FOR EACH ROW
            EXECUTE FUNCTION infra_command_notify()"#,
        r#"DROP TRIGGER IF EXISTS infra_command_notify_on_done ON infra_lifecycle_command"#,
        r#"CREATE TRIGGER infra_command_notify_on_done
            AFTER UPDATE OF completed_at_unix ON infra_lifecycle_command
            FOR EACH ROW
            WHEN (NEW.completed_at_unix IS NOT NULL AND OLD.completed_at_unix IS NULL)
            EXECUTE FUNCTION infra_command_notify()"#,
    ],
    seed: &[],
};

/// A take-down a supervisor runs: its verb with the answers only that
/// verb has, so a force never rides a terminate and every terminate says
/// what becomes of its kept disks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TakeDown {
    /// Scale to zero, keeping the disks. `force` brings down the units
    /// whose `on_stop` would keep them up.
    Stop { force: bool },
    /// Remove the copies, their disks as `disks` says.
    Terminate { disks: weft_core::infra::TerminateDisks },
}

impl TakeDown {
    /// A person's terminate: the disks the node lists stay.
    pub const TERMINATE: TakeDown = TakeDown::Terminate { disks: weft_core::infra::TerminateDisks::KeepListed };

    pub fn verb(self) -> InfraLifecycleVerb {
        match self {
            TakeDown::Stop { .. } => InfraLifecycleVerb::Stop,
            TakeDown::Terminate { .. } => InfraLifecycleVerb::Terminate,
        }
    }

    /// The `(force, spec_json)` columns the command row carries.
    fn columns(self) -> (bool, Option<serde_json::Value>) {
        match self {
            TakeDown::Stop { force } => (force, None),
            TakeDown::Terminate { disks } => {
                let work = weft_broker_client::protocol::TerminateWork { disks };
                (false, Some(serde_json::to_value(work).expect("TerminateWork serializes")))
            }
        }
    }
}

/// Enqueue a Stop or Terminate command. Returns its id; the
/// supervisor polling for the tenant claims it on its next tick.
pub async fn issue_lifecycle(
    pool: &PgPool,
    tenant_id: &str,
    project_id: uuid::Uuid,
    node_id: Option<&str>,
    copies: &weft_core::member::Copies,
    take_down: TakeDown,
    running_policy: RunningPolicy,
    drain_timeout_secs: u64,
    issued_by_instance: &str,
) -> Result<i64> {
    let (member_id, every_copy) = copies.columns();
    let (force, spec_json) = take_down.columns();
    let row: (i64,) = sqlx::query_as(
        "INSERT INTO infra_lifecycle_command \
         (tenant_id, project_id, node_id, verb, running_policy, force, spec_json, drain_timeout_secs, \
          issued_by_instance, issued_at_unix, member_id, every_copy) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, EXTRACT(EPOCH FROM NOW())::BIGINT, $10, $11) \
         RETURNING id",
    )
    .bind(tenant_id)
    .bind(project_id)
    .bind(node_id)
    .bind(take_down.verb().as_str())
    .bind(running_policy.as_str())
    .bind(force)
    .bind(spec_json)
    .bind(drain_timeout_secs as i64)
    .bind(issued_by_instance)
    .bind(member_id)
    .bind(every_copy)
    .fetch_one(pool)
    .await?;
    Ok(row.0)
}

/// An upgrade's work, carried in its command's `spec_json`; whose
/// copies it cycles is the row's `member_id`, where a cancel of that
/// owner's infra work finds it.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct UpgradeWork {
    /// The places to cycle, every one of the owner's kind when empty.
    pub nodes: Vec<String>,
    /// The running executions' answer, resolved when the upgrade was
    /// asked for (the picker's, else the request's, else the default).
    pub running_policy: RunningPolicy,
    pub drain_timeout_secs: u64,
    /// The person's answer for the triggers reading this infra, which
    /// the run takes down before the stop leg. `None` when none was on
    /// as the upgrade was asked for.
    pub trigger_deactivation: Option<weft_broker_client::protocol::DeactivateSpec>,
    /// What the start half runs, as the request's sync body carried it.
    pub binary_hash: Option<String>,
    pub definition_hash: Option<String>,
    pub infra_hash: Option<String>,
    /// The stop leg has landed: a claimer taking the upgrade over goes
    /// straight to the start (stopping again would take down what the
    /// start may already have brought up).
    #[serde(default)]
    pub stopped: bool,
}

/// What issuing an upgrade came to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpgradeIssued {
    /// Issued; its id, what the person follows to the outcome.
    Issued(i64),
    /// An upgrade of the same copies is still in flight (its id): a
    /// second one would cycle them under the first.
    AlreadyInFlight(i64),
}

/// Issue an upgrade of `member`'s copies (the shared ones for `None`),
/// unless one of the same copies is still in flight. The check and the
/// insert run under one advisory lock keyed by the copies, so two
/// requests racing each other issue one upgrade.
pub async fn issue_upgrade(
    pool: &PgPool,
    tenant_id: &str,
    project_id: uuid::Uuid,
    member: Option<&weft_core::member::MemberId>,
    work: &UpgradeWork,
    issued_by_instance: &str,
) -> Result<UpgradeIssued> {
    let member = member.map(|m| m.as_str());
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(crate::lease::advisory_key(
            crate::lease::UPGRADE_ISSUE_DOMAIN,
            &format!("{project_id}/{}", member.unwrap_or("")),
        ))
        .execute(&mut *tx)
        .await?;
    let in_flight: Option<i64> = sqlx::query_scalar(
        "SELECT id FROM infra_lifecycle_command \
         WHERE project_id = $1 AND verb = $2 AND member_id IS NOT DISTINCT FROM $3 \
           AND completed_at_unix IS NULL \
         ORDER BY id LIMIT 1",
    )
    .bind(project_id)
    .bind(InfraLifecycleVerb::Upgrade.as_str())
    .bind(member)
    .fetch_optional(&mut *tx)
    .await?;
    if let Some(id) = in_flight {
        return Ok(UpgradeIssued::AlreadyInFlight(id));
    }
    let id: i64 = sqlx::query_scalar(
        "INSERT INTO infra_lifecycle_command \
         (tenant_id, project_id, verb, spec_json, issued_by_instance, issued_at_unix, member_id) \
         VALUES ($1, $2, $3, $4, $5, EXTRACT(EPOCH FROM NOW())::BIGINT, $6) \
         RETURNING id",
    )
    .bind(tenant_id)
    .bind(project_id)
    .bind(InfraLifecycleVerb::Upgrade.as_str())
    .bind(sqlx::types::Json(work))
    .bind(issued_by_instance)
    .bind(member)
    .fetch_one(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(UpgradeIssued::Issued(id))
}

/// Record that upgrade `command_id`'s stop leg landed, while `process`
/// still holds its claim. `false` when it does not (another process took it
/// over): the caller stops there.
pub async fn mark_upgrade_stopped(pool: &PgPool, command_id: i64, instance: &str) -> Result<bool> {
    let res = sqlx::query(
        "UPDATE infra_lifecycle_command SET spec_json = jsonb_set(spec_json, '{stopped}', 'true') \
         WHERE id = $1 AND claimed_by_instance = $2 AND completed_at_unix IS NULL",
    )
    .bind(command_id)
    .bind(instance)
    .execute(pool)
    .await?;
    Ok(res.rows_affected() > 0)
}

/// The verbs that are a person's infra work, as the SQL list inside
/// `verb IN (...)`: the supervisor's, and an upgrade. What makes a
/// project infra-transitional ([`any_in_flight`]) and what an infra
/// cancel reaches ([`request_cancel_owner`]); health's own dispatcher
/// verbs (deactivate / reactivate) are neither.
// SYNC: 'upgrade' <-> weft_broker_client::protocol::InfraLifecycleVerb::Upgrade
const INFRA_WORK_VERBS_SQL: &str = concat!(weft_broker_client::supervisor_verbs_sql!(), ", 'upgrade'");

// Note on missing `claim_one` / `complete`: claim/complete is owned
// by the broker (`supervisor_claim_command` + `supervisor_command_complete`)
// for supervisor verbs, and by the dispatcher's `lifecycle_claimer`
// for Deactivate / Reactivate / Upgrade. There is no shared helper because the
// two claim paths enforce different ownership invariants (broker
// requires SA-authenticated supervisor process; dispatcher claims by
// instance with a lease).

/// Wait for a previously-issued command to reach a terminal state.
/// Returns a typed outcome that distinguishes "the claimer hit a
/// real error" from "the command was cancelled" (e.g. the targeted
/// node was removed). Callers branch differently on the two cases:
/// a `Failed` is a user-visible problem, a `Cancelled` is "no
/// longer applicable" and shouldn't show up as a failure.
///
/// `Timeout` fires when no supervisor marks the row complete within
/// the deadline (typical when no supervisor is running; the caller
/// decides whether to proceed with cleanup anyway).
#[derive(Debug, Clone)]
pub enum WaitOutcome {
    Succeeded,
    Failed { error: String },
    Cancelled { reason: String },
    Timeout,
}

/// Wait for one command to reach a terminal state, or `timeout` to
/// pass. See [`wait_for_commands`].
pub async fn wait_for_command(
    pool: &PgPool,
    signals: &weft_task_store::pg_signal::PgSignalWatch,
    command_id: i64,
    timeout: std::time::Duration,
) -> Result<WaitOutcome> {
    let mut outcomes = wait_for_commands(pool, signals, &[command_id], timeout).await?;
    Ok(outcomes.pop().expect("one outcome per id").1)
}

/// A completed command row's outcome, or `None` while it is pending.
fn completed_outcome(command_id: i64, row: &sqlx::postgres::PgRow) -> Result<Option<WaitOutcome>> {
    use sqlx::Row;
    use weft_broker_client::protocol::LifecycleOutcome;
    let done: Option<i64> = row.try_get("completed_at_unix")?;
    if done.is_none() {
        return Ok(None);
    }
    let outcome_str: Option<String> = row.try_get("outcome")?;
    let message: Option<String> = row.try_get("outcome_message")?;
    let outcome = outcome_str.as_deref().and_then(LifecycleOutcome::parse).ok_or_else(|| {
        anyhow::anyhow!(
            "infra_lifecycle_command.id={command_id} completed with no/unknown outcome '{outcome_str:?}'"
        )
    })?;
    Ok(Some(match outcome {
        LifecycleOutcome::Succeeded => WaitOutcome::Succeeded,
        LifecycleOutcome::Failed => WaitOutcome::Failed {
            error: message.unwrap_or_else(|| "unspecified error".into()),
        },
        LifecycleOutcome::Cancelled => WaitOutcome::Cancelled {
            reason: message.unwrap_or_else(|| "cancelled".into()),
        },
    }))
}

/// Non-blocking single read of a command's terminal state. `None`
/// means still pending (or the row is gone, treated as pending). The
/// HTTP command-status endpoint uses this so clients can poll a stop /
/// terminate to completion without the CLI guessing at rollup state
/// (a NoOp unit staying up means the rollup never reaches "stopped",
/// so the command outcome is the only honest "is it done" signal).
pub async fn read_command_outcome(
    pool: &PgPool,
    project_id: uuid::Uuid,
    command_id: i64,
) -> Result<Option<WaitOutcome>> {
    // Scope by project, NOT just the (globally unique) id: the HTTP
    // endpoint is `/projects/{project}/infra/commands/{id}`, and a
    // caller authorized for one project must not read another's command
    // outcome (which carries raw supervisor error strings). A command
    // under a different project returns None, indistinguishable from
    // pending, so no existence leak.
    let row = sqlx::query(
        "SELECT completed_at_unix, outcome, outcome_message \
         FROM infra_lifecycle_command WHERE id = $1 AND project_id = $2",
    )
    .bind(command_id)
    .bind(project_id)
    .fetch_optional(pool)
    .await?;
    match row {
        Some(r) => completed_outcome(command_id, &r),
        None => Ok(None),
    }
}

/// [`read_command_outcome`], held for up to `hold` while the command is
/// still pending: the read ends when the command completes (announced by
/// its row's own trigger) or the hold runs out. For a client that waits
/// on one command and asks again after each hold.
pub async fn held_command_outcome(
    pool: &PgPool,
    signals: &weft_task_store::pg_signal::PgSignalWatch,
    project_id: uuid::Uuid,
    command_id: i64,
    hold: std::time::Duration,
) -> Result<Option<WaitOutcome>> {
    use weft_broker_client::lifecycle_command::{InfraCommandSignal, INFRA_COMMAND_CHANNEL};
    let deadline = tokio::time::Instant::now() + hold;
    let mut heard = signals.subscribe();
    loop {
        let outcome = read_command_outcome(pool, project_id, command_id).await?;
        if outcome.is_some() {
            return Ok(outcome);
        }
        let woken = heard
            .woken_before(deadline, |channel, payload| {
                channel == INFRA_COMMAND_CHANNEL
                    && InfraCommandSignal::parse(payload) == Some(InfraCommandSignal::Done { id: command_id })
            })
            .await?;
        if !woken {
            return Ok(None);
        }
    }
}

/// Wait for ALL the given commands to reach a terminal state, or
/// the deadline expires. Each look reads every row via
/// `WHERE id = ANY($1)`, and between looks the wait sleeps until one of
/// them is announced done on `INFRA_COMMAND_CHANNEL` (by the row's own
/// trigger). Returns `(id, outcome)` pairs in input order, `Timeout`
/// for any that did not finish in time.
///
/// Used by `api::infra::reap_orphans`, which fans out N terminate
/// commands and waits for the set, by the upgrade stop leg (a minute at
/// a time, so it can leave a breadcrumb), and through
/// [`wait_for_command`] by every single-command wait.
pub async fn wait_for_commands(
    pool: &PgPool,
    signals: &weft_task_store::pg_signal::PgSignalWatch,
    command_ids: &[i64],
    timeout: std::time::Duration,
) -> Result<Vec<(i64, WaitOutcome)>> {
    use weft_broker_client::lifecycle_command::{InfraCommandSignal, INFRA_COMMAND_CHANNEL};
    if command_ids.is_empty() {
        return Ok(Vec::new());
    }
    let deadline = tokio::time::Instant::now() + timeout;
    let mut done: std::collections::HashMap<i64, WaitOutcome> =
        std::collections::HashMap::with_capacity(command_ids.len());
    // Subscribed before the first read, so a command finishing between
    // a read and the wait still ends the wait.
    let mut heard = signals.subscribe();
    loop {
        use sqlx::Row;
        let rows = sqlx::query(
            "SELECT id, completed_at_unix, outcome, outcome_message \
             FROM infra_lifecycle_command WHERE id = ANY($1)",
        )
        .bind(command_ids)
        .fetch_all(pool)
        .await?;
        for r in rows {
            let id: i64 = r.try_get("id")?;
            if let Some(outcome) = completed_outcome(id, &r)? {
                done.insert(id, outcome);
            }
        }
        if done.len() == command_ids.len() {
            break;
        }
        let woken = heard
            .woken_before(deadline, |channel, payload| {
                channel == INFRA_COMMAND_CHANNEL
                    && matches!(InfraCommandSignal::parse(payload), Some(InfraCommandSignal::Done { id }) if command_ids.contains(&id))
            })
            .await?;
        if !woken {
            // Fill the remaining with Timeout outcomes so the
            // caller sees one entry per requested id.
            for id in command_ids {
                done.entry(*id).or_insert(WaitOutcome::Timeout);
            }
            break;
        }
    }
    // Return in input order. Contract: `command_ids` are distinct
    // (each is a fresh BIGSERIAL from a separate INSERT). The loop only
    // exits once every id has an entry: either all are terminal or the
    // deadline branch filled the rest with Timeout. So `.expect` is
    // honest: a missing id here is a logic bug, not a runtime case to
    // paper over.
    Ok(command_ids
        .iter()
        .map(|id| (*id, done.get(id).cloned().expect("loop filled every id")))
        .collect())
}

/// Whether any infra-work lifecycle command (apply / stop / terminate,
/// or an upgrade) is uncompleted for the project. The dispatcher-side
/// "infra operation in flight" fact: while it holds, the
/// reconciliation treats the project as infra-transitional (only
/// infra_cancel offered), which is what stops NEW runs from starving
/// a stop/terminate drain (the drain waits for the running set to
/// empty; a new run would keep refilling it). In-flight work is
/// untouched; only new launches are gated.
pub async fn any_in_flight(pool: &PgPool, project_id: uuid::Uuid) -> Result<bool> {
    let (exists,): (bool,) = sqlx::query_as(&format!(
        "SELECT EXISTS( \
             SELECT 1 FROM infra_lifecycle_command \
             WHERE project_id = $1 \
               AND completed_at_unix IS NULL \
               AND verb IN ({INFRA_WORK_VERBS_SQL}) \
         )",
    ))
    .bind(project_id)
    .fetch_one(pool)
    .await?;
    Ok(exists)
}

/// Cancel one owner's in-flight infra work (apply / stop / terminate,
/// and an upgrade, whose claimer reads the flag between its stop and
/// its start): the shared copies' (`member` `None`, which also reaches
/// a command for every copy, since that one takes the shared copies
/// down too) or one member's. Flags CLAIMED rows (`cancel_requested =
/// TRUE`; the executing supervisor polls the flag between install
/// calls and halts) and completes still-UNCLAIMED rows outright with
/// outcome `cancelled` (nothing has touched the install for them yet).
/// Flag first, then complete: a row claimed between the two statements
/// has its flag already set, so no window exists where a command
/// escapes the cancel. Dispatcher-owned verbs (deactivate /
/// reactivate) are health's work and never touched. Returns how many
/// rows were touched.
pub async fn request_cancel_owner(
    pool: &PgPool,
    project_id: uuid::Uuid,
    member: Option<&weft_core::member::MemberId>,
) -> Result<u64> {
    let owner = format!(
        "project_id = $1 AND completed_at_unix IS NULL \
         AND member_id IS NOT DISTINCT FROM $2 \
         AND verb IN ({INFRA_WORK_VERBS_SQL})",
    );
    let member = member.map(|m| m.as_str());
    let flagged = sqlx::query(&format!(
        "UPDATE infra_lifecycle_command SET cancel_requested = TRUE WHERE {owner}"
    ))
    .bind(project_id)
    .bind(member)
    .execute(pool)
    .await?
    .rows_affected();
    let completed = sqlx::query(&format!(
        "UPDATE infra_lifecycle_command \
         SET completed_at_unix = EXTRACT(EPOCH FROM NOW())::BIGINT, \
             outcome = 'cancelled', \
             outcome_message = 'cancelled by user before execution' \
         WHERE {owner} AND claimed_by_instance IS NULL"
    ))
    .bind(project_id)
    .bind(member)
    .execute(pool)
    .await?
    .rows_affected();
    // `flagged` counted the completed ones too (they were uncompleted
    // at flag time); the touched total is just `flagged`, `completed`
    // is informational.
    tracing::info!(
        target: "weft_dispatcher::infra_lifecycle_command",
        project_id = %project_id,
        member = member.unwrap_or("<shared>"),
        flagged,
        cancelled_unclaimed = completed,
        "infra cancel requested"
    );
    Ok(flagged)
}

/// Whether a claimed command's cancellation was requested. Read by the
/// broker's supervisor endpoint so the executing supervisor can poll
/// mid-command.
pub async fn cancel_requested(pool: &PgPool, command_id: i64) -> Result<bool> {
    let row: Option<(bool,)> = sqlx::query_as(
        "SELECT cancel_requested FROM infra_lifecycle_command WHERE id = $1",
    )
    .bind(command_id)
    .fetch_optional(pool)
    .await?;
    // A missing row (removed project) reads as cancelled: the executor
    // should stop working on it either way.
    Ok(row.map(|(c,)| c).unwrap_or(true))
}

/// Drop every row for a project. Called on `weft rm`.
pub async fn remove_project<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    project_id: uuid::Uuid,
) -> Result<u64> {
    let res = sqlx::query("DELETE FROM infra_lifecycle_command WHERE project_id = $1")
        .bind(project_id)
        .execute(executor)
        .await?;
    Ok(res.rows_affected())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verb_round_trips() {
        for v in [
            InfraLifecycleVerb::Apply,
            InfraLifecycleVerb::Stop,
            InfraLifecycleVerb::Terminate,
        ] {
            assert_eq!(InfraLifecycleVerb::parse(v.as_str()), Some(v));
        }
    }

    /// A terminate carries what becomes of its kept disks on the row, in
    /// the shape the supervisor reads back; a stop carries only its force.
    #[test]
    fn a_take_down_carries_its_answers_on_the_command() {
        use weft_broker_client::protocol::{SupervisorCommandRow, TerminateWork};
        use weft_core::infra::TerminateDisks;
        let read_back = |take_down: TakeDown| {
            let (force, spec_json) = take_down.columns();
            let row = SupervisorCommandRow {
                id: 1,
                project_id: uuid::Uuid::from_u128(1),
                node_id: None,
                copies: weft_core::member::Copies::Shared,
                verb: take_down.verb(),
                running_policy: Some(RunningPolicy::Cancel),
                spec_json,
                force,
                drain_timeout_secs: 60,
            };
            (row.force, row.terminate_work())
        };
        let wipe = TakeDown::Terminate { disks: TerminateDisks::DeleteAll };
        assert_eq!(wipe.verb(), InfraLifecycleVerb::Terminate);
        assert_eq!(read_back(wipe), (false, Ok(TerminateWork { disks: TerminateDisks::DeleteAll })));
        assert_eq!(read_back(TakeDown::TERMINATE), (false, Ok(TerminateWork { disks: TerminateDisks::KeepListed })));
        let stop = TakeDown::Stop { force: true };
        assert_eq!(stop.verb(), InfraLifecycleVerb::Stop);
        assert_eq!(stop.columns(), (true, None));
    }

    #[test]
    fn verb_unknown_returns_none() {
        assert_eq!(InfraLifecycleVerb::parse("reboot"), None);
        assert_eq!(InfraLifecycleVerb::parse(""), None);
    }

    /// The infra-work list is the supervisor's verbs and an upgrade,
    /// spelled the way the verb is stored.
    #[test]
    fn infra_work_is_the_supervisor_verbs_and_an_upgrade() {
        let want = [
            InfraLifecycleVerb::Apply,
            InfraLifecycleVerb::Stop,
            InfraLifecycleVerb::Terminate,
            InfraLifecycleVerb::Upgrade,
        ]
        .map(|v| format!("'{}'", v.as_str()))
        .join(", ");
        assert_eq!(INFRA_WORK_VERBS_SQL, want);
    }

    #[test]
    fn running_policy_round_trips() {
        for p in [RunningPolicy::Cancel, RunningPolicy::Wait] {
            assert_eq!(RunningPolicy::parse(p.as_str()), Some(p));
        }
    }

    /// A request that says nothing about running work does not wait on
    /// it: waiting is the choice a person makes, never the one they
    /// get for free.
    #[test]
    fn running_policy_default_is_cancel() {
        assert_eq!(RunningPolicy::default(), RunningPolicy::Cancel);
    }
}
