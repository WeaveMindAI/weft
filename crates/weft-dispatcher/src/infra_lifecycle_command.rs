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
//!   image-tag map + copy id, hashes, and decides skip / fresh /
//!   replace internally.
//! - **Stop**: stop the containers, keep their disks. Issued here.
//! - **Terminate**: delete the copies and their disks, keeping the ones
//!   the node lists in `keepOnTerminate` unless the command's
//!   [`Teardown::Terminate`] says every disk goes (an instance's wipe).
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
//!   Issued here ([`TakeDownCommands`]) and run by a dispatcher
//!   (`api::infra::run_upgrade`): the triggers reading the infra come
//!   down, the stop leg, then the start. The row
//!   is what the person follows, and what another process takes over when
//!   the one running it dies.

use anyhow::Result;
use weft_task_store::db::{self, Client, Pool, Transaction};

// `InfraLifecycleVerb` is the wire contract for the `verb` column. It
// lives in `weft-broker-client::protocol` so the supervisor + dispatcher
// + broker share one source of truth. Re-export here so the rest of the
// dispatcher keeps the short module-relative path.
pub use weft_broker_client::protocol::InfraLifecycleVerb;

pub static GROUP: weft_task_store::SchemaGroup = weft_task_store::SchemaGroup {
    name: "infra_lifecycle_command",
    tables: &["infra_lifecycle_command", "infra_health_rule", "infra_health_look"],
    ddl: &[
        r#"CREATE TABLE IF NOT EXISTS infra_lifecycle_command (
            id                BIGSERIAL PRIMARY KEY,
            tenant_id         TEXT NOT NULL,
            project_id        UUID NOT NULL,
            node_id           TEXT,
            verb              TEXT NOT NULL,
            -- As its JSON text (`db::JsonText`): an apply's carries the
            -- spec a node built.
            spec_json         TEXT,
            issued_by_replica     TEXT NOT NULL,
            issued_at_unix    BIGINT NOT NULL,
            claimed_by_replica    TEXT,
            -- A dispatcher's claim lapses `CLAIM_LEASE_TTL` after this. A
            -- drain whose next look is further away sets it ahead, so the
            -- claim lasts until then
            -- (`crate::lifecycle_claimer::CommandClaim`).
            claimed_at_unix   BIGINT,
            completed_at_unix BIGINT,
            -- 'succeeded' | 'failed' | 'cancelled' | NULL.
            -- NULL = "no result yet" (pending or claimed).
            -- 'failed' = the claimer hit a real error executing the
            --   verb; the worker / caller treats this as a failure.
            -- 'cancelled' = the command was abandoned (e.g. the
            --   targeted node was removed before run). NOT a
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
            -- was CLAIMED (in flight), announced as it is set
            -- (`cancel:<id>`). Whoever runs it halts (leaving
            -- per-node partial state visible; no platform applies a
            -- whole project transactionally).
            -- Pending unclaimed rows are cancelled outright (outcome =
            -- 'cancelled') instead of flagged.
            cancel_requested  BOOLEAN NOT NULL DEFAULT FALSE,
            -- A stop or terminate that first waits for the runs reaching
            -- its copies to end (`--running wait`), however long that
            -- takes: the dispatcher's claimer waits it out
            -- (`crate::drain`), clears it, and only then does a supervisor
            -- take the command. FALSE for every other command.
            drains            BOOLEAN NOT NULL DEFAULT FALSE,
            -- Which copies of the infra the command acts on
            -- (`weft_core::instance::Copies`): the shared ones (instance_id
            -- NULL, every_copy FALSE), one instance's (instance_id set), or
            -- every copy there is (every_copy TRUE; the project going).
            -- An apply always names exactly one copy.
            instance_id         TEXT,
            every_copy        BOOLEAN NOT NULL DEFAULT FALSE,
            -- How many times a supervisor claimed it to run. A command that
            -- is claimed again and again never ended (its run or the write
            -- of its ending keeps failing, or its supervisor keeps dying
            -- on it), so the claim gives it up once this reaches
            -- `SUPERVISOR_GIVE_UP_AFTER` (`weft_broker::lifecycle_writes::claim_next`).
            runs              INT NOT NULL DEFAULT 0,
            -- An apply's: the hash of its place's infra in the build its
            -- setup ran (`BuiltProgram::infra_places`), copied onto the
            -- copy's `infra_node.source_hash` as it succeeds. NULL for
            -- every other verb.
            source_hash       TEXT,
            -- A stop's or terminate's: the entry tokens of the triggers
            -- taken down with it, read before that take-down (one that
            -- refuses with nothing kept deletes their signals): what its
            -- wait for the runs reaching its copies counts at the
            -- workers' door (`crate::drain`).
            waits_on_tokens   TEXT[] NOT NULL DEFAULT '{}'
        )"#,
        r#"CREATE INDEX IF NOT EXISTS idx_lifecycle_cmd_pending
              ON infra_lifecycle_command(tenant_id)
              WHERE completed_at_unix IS NULL"#,
        // Claim path for supervisor-owned verbs (apply / stop /
        // terminate). The supervisor's claim scans uncompleted rows of
        // these verbs ordered by id and gates each on a live
        // `infra_owner` lease (its single-actor authority); its claim
        // marks `claimed_by_replica`, which holds only while the claimer
        // holds the project's lease
        // (`weft_broker_client::lifecycle_command::supervisor_claimable`).
        // This partial index keyed on id covers that scan and skips
        // dispatcher-owned + completed rows.
        concat!(
            r#"CREATE INDEX IF NOT EXISTS idx_lifecycle_cmd_supervisor_claim
              ON infra_lifecycle_command(id)
              WHERE completed_at_unix IS NULL
                AND verb IN ("#,
            weft_broker_client::supervisor_verbs_sql!(),
            ")",
        ),
        // Mirror for the dispatcher claim loop (deactivate /
        // reactivate / upgrade, and a take-down's drain). No tenant filter:
        // the dispatcher pool claims across all tenants.
        concat!(
            r#"CREATE INDEX IF NOT EXISTS idx_lifecycle_cmd_dispatcher_claim
              ON infra_lifecycle_command(id)
              WHERE completed_at_unix IS NULL
                AND claimed_by_replica IS NULL
                AND (drains OR verb IN ("#,
            weft_broker_client::dispatcher_verbs_sql!(),
            "))",
        ),
        // Partial unique index: at most one pending apply for a
        // given (project_id, node_id). Stops a worker restart from
        // double-enqueueing the same apply; `infra_enqueue_apply`
        // catches the conflict and returns the existing row's id.
        r#"CREATE UNIQUE INDEX IF NOT EXISTS uq_lifecycle_cmd_pending_apply
              ON infra_lifecycle_command(project_id, node_id, instance_id) NULLS NOT DISTINCT
              WHERE completed_at_unix IS NULL AND verb = 'apply'"#,
        r#"CREATE TABLE IF NOT EXISTS infra_health_rule (
            -- Where one health rule stands on one infra copy, kept across
            -- the supervisor's processes
            -- (weft_broker_client::protocol::HealthRuleState).
            project_id           UUID NOT NULL,
            -- The copy: its node, and whose (NULL for the shared one).
            node_id              TEXT NOT NULL,
            instance_id          TEXT,
            -- The rule's name, unique among its node's rules.
            rule                 TEXT NOT NULL,
            -- Its action was queued for the copy and has not failed, and
            -- the rule has held since.
            fired                BOOLEAN NOT NULL DEFAULT false,
            consecutive_failures INTEGER NOT NULL DEFAULT 0,
            -- When its failed action is tried again (unix seconds).
            next_retry_at_unix   BIGINT,
            -- When it gave up (unix seconds), shown in `weft status`; it is
            -- armed again when the units it saw then change, a command
            -- touches the project, or the rule stops holding.
            gave_up_at_unix      BIGINT,
            -- The copy's units the health loop saw when it gave up, as
            -- their JSON text (`db::JsonText`): what the platform said of
            -- them.
            gave_up_seen         TEXT,
            last_error           TEXT,
            -- The command its action queued (infra_lifecycle_command.id)
            -- that no look has seen end yet, and when it counts as failed
            -- if it has not ended (unix seconds; both set or neither).
            queued_command       BIGINT,
            queued_deadline_unix BIGINT,
            -- A take-down rule's: its take-down was queued for the copy and
            -- did not fail, and the copy has not come back since. Only
            -- such a copy, or one whose bring-back rules say it is still
            -- broken, keeps a trigger down when a bring-back runs.
            took_down            BOOLEAN NOT NULL DEFAULT false
        )"#,
        r#"CREATE UNIQUE INDEX IF NOT EXISTS idx_infra_health_rule_key
              ON infra_health_rule(project_id, node_id, instance_id, rule) NULLS NOT DISTINCT"#,
        // The broker's look for the supervisor's next due retry.
        r#"CREATE INDEX IF NOT EXISTS idx_infra_health_rule_retry
              ON infra_health_rule(next_retry_at_unix)
              WHERE gave_up_at_unix IS NULL AND next_retry_at_unix IS NOT NULL"#,
        // The broker's look for the deadline of a command a rule queued.
        r#"CREATE INDEX IF NOT EXISTS idx_infra_health_rule_queued
              ON infra_health_rule(queued_deadline_unix)
              WHERE queued_deadline_unix IS NOT NULL"#,
        r#"CREATE TABLE IF NOT EXISTS infra_health_look (
            -- How the supervisor's passes over one project stand, kept
            -- across its processes (weft_broker::lifecycle_writes::sync_ownership).
            project_id      UUID PRIMARY KEY,
            -- When a person last asked for a look (`weft infra look`, unix
            -- seconds): a health rule that gave up at or before it is
            -- armed again.
            asked_at_unix   BIGINT,
            -- Passes over the project that failed in a row since one went
            -- through (weft_broker_client::protocol::supervisor_retry_after).
            failures        INTEGER NOT NULL DEFAULT 0,
            -- When a pass tries the project again (unix seconds); NULL:
            -- nothing failed, a pass took the retry, or it gave up.
            retry_at_unix   BIGINT
        )"#,
        // The broker's look for the supervisor's next due retry.
        r#"CREATE INDEX IF NOT EXISTS idx_infra_health_look_retry
              ON infra_health_look(retry_at_unix)
              WHERE retry_at_unix IS NOT NULL"#,
        // Announce a command when it is issued or its drain is over (its
        // claimers wake) and when it completes (whoever waits on its
        // outcome wakes), from a trigger so no writer (the dispatcher's
        // own verbs, the broker's supervisor and worker paths) can forget.
        // A health take-down or bring-back that completes, however it
        // ended, also asks the supervisor for one look at its project
        // (`look:`): the rules that read what it did (the triggers'
        // listening, what the health check took down), and the one whose
        // command failed, are read again now rather than at the project's
        // next unit change, which on an idle install never comes. One
        // notice per command, so nothing is held open or looked at again.
        // SYNC: the payloads <-> weft_broker_client::lifecycle_command::InfraCommandSignal,
        //       weft_task_store::infra_command::look_payload
        // SYNC: 'deactivate', 'reactivate' <-> InfraLifecycleVerb::{Deactivate, Reactivate}
        r#"CREATE OR REPLACE FUNCTION infra_command_notify() RETURNS trigger AS $$
            BEGIN
                IF TG_OP = 'INSERT' OR (NEW.completed_at_unix IS NULL AND NOT NEW.drains) THEN
                    PERFORM pg_notify('weft_infra_command', 'issued:' || NEW.project_id::text);
                ELSE
                    PERFORM pg_notify('weft_infra_command', 'done:' || NEW.id::text);
                    IF NEW.verb IN ('deactivate', 'reactivate') THEN
                        PERFORM pg_notify('weft_infra_command', 'look:' || NEW.project_id::text);
                    END IF;
                END IF;
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql"#,
        r#"DROP TRIGGER IF EXISTS infra_command_notify_on_issue ON infra_lifecycle_command"#,
        r#"CREATE TRIGGER infra_command_notify_on_issue
            AFTER INSERT ON infra_lifecycle_command
            FOR EACH ROW
            EXECUTE FUNCTION infra_command_notify()"#,
        r#"DROP TRIGGER IF EXISTS infra_command_notify_on_drained ON infra_lifecycle_command"#,
        r#"CREATE TRIGGER infra_command_notify_on_drained
            AFTER UPDATE OF drains ON infra_lifecycle_command
            FOR EACH ROW
            WHEN (OLD.drains AND NOT NEW.drains)
            EXECUTE FUNCTION infra_command_notify()"#,
        r#"DROP TRIGGER IF EXISTS infra_command_notify_on_done ON infra_lifecycle_command"#,
        r#"CREATE TRIGGER infra_command_notify_on_done
            AFTER UPDATE OF completed_at_unix ON infra_lifecycle_command
            FOR EACH ROW
            WHEN (NEW.completed_at_unix IS NOT NULL AND OLD.completed_at_unix IS NULL)
            EXECUTE FUNCTION infra_command_notify()"#,
        // Announce a person's cancel of a command (`cancel:<id>`), so
        // whoever runs it (a drain waiting on running work, a supervisor)
        // stops at once instead of reading the flag on a clock.
        // SYNC: the payload <-> weft_broker_client::lifecycle_command::InfraCommandSignal::Cancel
        r#"CREATE OR REPLACE FUNCTION infra_command_cancel_notify() RETURNS trigger AS $$
            BEGIN
                PERFORM pg_notify('weft_infra_command', 'cancel:' || NEW.id::text);
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql"#,
        r#"DROP TRIGGER IF EXISTS infra_command_notify_on_cancel ON infra_lifecycle_command"#,
        r#"CREATE TRIGGER infra_command_notify_on_cancel
            AFTER UPDATE OF cancel_requested ON infra_lifecycle_command
            FOR EACH ROW
            WHEN (NEW.cancel_requested AND NOT OLD.cancel_requested)
            EXECUTE FUNCTION infra_command_cancel_notify()"#,
    ],
    seed: &[],
    data_steps: &[],
};

/// A take-down a supervisor runs: its verb with the answers only that
/// verb has, so a force never rides a terminate and every terminate says
/// what becomes of its kept disks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Teardown {
    /// Scale to zero, keeping the disks. `force` brings down the units
    /// whose `on_stop` would keep them up.
    Stop { force: bool },
    /// Remove the copies, their disks as `disks` says.
    Terminate { disks: weft_core::infra::TerminateDisks },
}

impl Teardown {
    /// A person's terminate: the disks the node lists stay.
    pub const TERMINATE: Teardown = Teardown::Terminate { disks: weft_core::infra::TerminateDisks::KeepListed };

    pub fn verb(self) -> InfraLifecycleVerb {
        match self {
            Teardown::Stop { .. } => InfraLifecycleVerb::Stop,
            Teardown::Terminate { .. } => InfraLifecycleVerb::Terminate,
        }
    }

    /// The `(force, spec_json)` columns the command row carries.
    fn columns(self) -> (bool, Option<serde_json::Value>) {
        match self {
            Teardown::Stop { force } => (force, None),
            Teardown::Terminate { disks } => {
                let work = weft_broker_client::protocol::TerminateWork { disks };
                (false, Some(serde_json::to_value(work).expect("TerminateWork serializes")))
            }
        }
    }
}

/// Enqueue a Stop or Terminate command. Returns its id. With `waits`, the
/// dispatcher's claimer first waits for the runs reaching its copies to
/// end (`crate::lifecycle_claimer`); the owning supervisor claims it once
/// that wait is over, or at once without.
pub async fn issue_lifecycle(
    db: &impl Client,
    tenant_id: &str,
    project_id: uuid::Uuid,
    node_id: Option<&str>,
    copies: &weft_core::instance::Copies,
    teardown: Teardown,
    waits: bool,
    waits_on_tokens: &[String],
    issued_by_replica: &str,
) -> Result<i64> {
    let (instance_id, every_copy) = copies.columns();
    let (force, spec_json) = teardown.columns();
    Ok(db
        .scalar(
            "INSERT INTO infra_lifecycle_command \
             (tenant_id, project_id, node_id, verb, force, spec_json, drains, \
              issued_by_replica, issued_at_unix, instance_id, every_copy, waits_on_tokens) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, EXTRACT(EPOCH FROM NOW())::BIGINT, $9, $10, $11) \
             RETURNING id",
            &[
                &tenant_id,
                &project_id,
                &node_id,
                &teardown.verb().as_str(),
                &force,
                &spec_json.map(db::JsonText),
                &waits,
                &issued_by_replica,
                &instance_id,
                &every_copy,
                &waits_on_tokens,
            ],
        )
        .await?)
}

/// An upgrade's work, carried in its command's `spec_json`; whose
/// copies it cycles is the row's `instance_id`, where a cancel of that
/// owner's infra work finds it.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct UpgradeWork {
    /// The places it was asked for, every one of the owner's kind when
    /// empty.
    pub nodes: Vec<String>,
    /// Of those, the ones running on another build when it was asked for:
    /// the ones it cycles (a stop, then a start), and the only ones it
    /// touches.
    pub running: Vec<String>,
    /// How the triggers whose cut contains them go down before the stop
    /// leg (they come back with the start), and what becomes of the runs
    /// reaching them: resolved when the upgrade was asked for.
    pub take_down: weft_core::take_down::TakeDown,
    /// What the start half runs, as the request's sync body carried it.
    pub binary_hash: Option<String>,
    pub definition_hash: Option<String>,
    pub infra_hash: Option<String>,
    /// The entry tokens of the triggers taken down with it, read before
    /// that take-down (which may delete their signals): what its stop
    /// leg's wait counts at the workers' door. Filled as it is issued
    /// ([`TakeDownCommands::issue_in`]).
    #[serde(default)]
    pub waits_on_tokens: Vec<String>,
    /// The stop leg has landed: a claimer taking the upgrade over goes
    /// straight to the start (stopping again would take down what the
    /// start may already have brought up).
    #[serde(default)]
    pub stopped: bool,
}

/// What issuing [`TakeDownCommands`] came to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Issued {
    /// Issued; their ids, first issued first, what the person follows to
    /// the outcome.
    Commands(Vec<i64>),
    /// An upgrade of the same copies is still in flight (its id): a
    /// second one would cycle them under the first. Nothing was issued.
    UpgradeInFlight(i64),
}

/// What a person or a program reads when an upgrade of the same copies,
/// command `command`, is still in flight.
pub fn upgrade_in_flight_refusal(command: i64) -> (axum::http::StatusCode, String) {
    (
        axum::http::StatusCode::CONFLICT,
        format!(
            "an upgrade of these copies is already in flight (command {command}); wait for it to finish or cancel it \
             (`weft infra cancel`)"
        ),
    )
}

/// The upgrade of `instance`'s copies (`None`: the shared ones) under
/// way, if one is: what a second upgrade of the same copies is refused
/// over, before it takes anything down and again as it is issued.
pub async fn upgrade_in_flight(db: &impl Client, project_id: uuid::Uuid, instance: Option<&str>) -> Result<Option<i64>> {
    Ok(db
        .scalar_opt(
            "SELECT id FROM infra_lifecycle_command \
             WHERE project_id = $1 AND verb = $2 AND instance_id IS NOT DISTINCT FROM $3 \
               AND completed_at_unix IS NULL \
             ORDER BY id LIMIT 1",
            &[&project_id, &InfraLifecycleVerb::Upgrade.as_str(), &instance],
        )
        .await?)
}

/// Issue an upgrade of `instance`'s copies (the shared ones for `None`)
/// on `conn`'s transaction, unless one of the same copies is still in
/// flight. The check and the insert run under one advisory lock keyed by
/// the copies, held until that transaction ends, so two requests racing
/// each other issue one upgrade.
async fn issue_upgrade_in(
    tx: &Transaction<'_>,
    tenant_id: &str,
    project_id: uuid::Uuid,
    instance: Option<&weft_core::instance::InstanceId>,
    work: &UpgradeWork,
    issued_by_replica: &str,
) -> Result<Issued> {
    let instance = instance.map(|m| m.as_str());
    let key = crate::lease::advisory_key(crate::lease::UPGRADE_ISSUE_DOMAIN, &format!("{project_id}/{}", instance.unwrap_or("")));
    tx.execute("SELECT pg_advisory_xact_lock($1)", &[&key]).await?;
    if let Some(id) = upgrade_in_flight(tx, project_id, instance).await? {
        return Ok(Issued::UpgradeInFlight(id));
    }
    let id: i64 = tx
        .scalar(
            "INSERT INTO infra_lifecycle_command \
             (tenant_id, project_id, verb, spec_json, issued_by_replica, issued_at_unix, instance_id) \
             VALUES ($1, $2, $3, $4, $5, EXTRACT(EPOCH FROM NOW())::BIGINT, $6) \
             RETURNING id",
            &[&tenant_id, &project_id, &InfraLifecycleVerb::Upgrade.as_str(), &db::JsonText(work), &issued_by_replica, &instance],
        )
        .await?;
    Ok(Issued::Commands(vec![id]))
}

/// The commands an infra verb issues over copies it takes down, with the
/// triggers whose cut reads those copies: issued in the same transaction
/// that takes the triggers down (`crate::take_down::DownCause::Infra`), so
/// no trigger is ever down for a command that does not exist, and no
/// command is claimable while a trigger reading its copies still listens.
/// Each trigger records the first of them (`WentDown::Infra`), what its
/// bring-back reads every later command against
/// ([`since_take_down`]).
#[derive(Debug, Clone)]
pub struct TakeDownCommands {
    pub tenant_id: String,
    pub issued_by_replica: String,
    pub work: TakeDownWork,
}

/// What [`TakeDownCommands`] issue.
#[derive(Debug, Clone)]
pub enum TakeDownWork {
    /// A stop or terminate of `copies`: one command per node in `nodes`,
    /// one over the owner's copies when it is empty. With `waits`, each
    /// waits for the runs reaching its copies first.
    Teardown { nodes: Vec<String>, copies: weft_core::instance::Copies, teardown: Teardown, waits: bool },
    /// An upgrade of `instance`'s copies.
    Upgrade { instance: Option<weft_core::instance::InstanceId>, work: UpgradeWork },
}

impl TakeDownCommands {
    /// Whether the copies they take down can come back, which is what
    /// marks the triggers going down with them (`WentDown::Infra`): all
    /// but a terminate deleting every disk, which takes the copies away
    /// for good (their owner is going).
    pub fn copies_can_return(&self) -> bool {
        !matches!(
            self.work,
            TakeDownWork::Teardown { teardown: Teardown::Terminate { disks: weft_core::infra::TerminateDisks::DeleteAll }, .. }
        )
    }

    /// Issue them on `tx`: their ids, first issued first,
    /// or the upgrade of the same copies already in flight. `tokens` are
    /// the entry tokens of the triggers taken down with them, read before
    /// that take-down: what a stop's or terminate's wait counts at the
    /// door for them (`waits_on_tokens`).
    pub async fn issue_in(&self, tx: &Transaction<'_>, project_id: uuid::Uuid, tokens: &[String]) -> Result<Issued> {
        let (tenant, replica) = (self.tenant_id.as_str(), self.issued_by_replica.as_str());
        match &self.work {
            TakeDownWork::Teardown { nodes, copies, teardown, waits } => {
                let mut ids = Vec::with_capacity(nodes.len().max(1));
                match nodes.is_empty() {
                    true => ids.push(issue_lifecycle(tx, tenant, project_id, None, copies, *teardown, *waits, tokens, replica).await?),
                    false => {
                        for node in nodes {
                            ids.push(issue_lifecycle(tx, tenant, project_id, Some(node), copies, *teardown, *waits, tokens, replica).await?);
                        }
                    }
                }
                Ok(Issued::Commands(ids))
            }
            TakeDownWork::Upgrade { instance, work } => {
                let work = UpgradeWork { waits_on_tokens: tokens.to_vec(), ..work.clone() };
                issue_upgrade_in(tx, tenant, project_id, instance.as_ref(), &work, replica).await
            }
        }
    }

    /// [`Self::issue_in`] on a transaction of its own: what a take-down
    /// that reaches no trigger on issues. `None` when the project is gone
    /// or being removed: the insert holds its row `FOR KEY SHARE`, so a
    /// removal either finds the commands and deletes them or waits for
    /// this and then finds them (`ProjectStore::remove`).
    pub async fn issue(&self, pool: &Pool, project_id: uuid::Uuid) -> Result<Option<Issued>> {
        let mut conn = pool.get().await?;
        let tx = conn.transaction().await?;
        let live: Option<bool> = tx.scalar_opt("SELECT TRUE FROM project WHERE id = $1 AND NOT removing FOR KEY SHARE", &[&project_id]).await?;
        if live.is_none() {
            return Ok(None);
        }
        let issued = self.issue_in(&tx, project_id, &[]).await?;
        if let Issued::Commands(_) = issued {
            tx.commit().await?;
        }
        Ok(Some(issued))
    }
}

/// Record that upgrade `command_id`'s stop leg landed, while `process`
/// still holds its claim. `false` when it does not (another process took it
/// over): the caller stops there.
pub async fn mark_upgrade_stopped(pool: &Pool, command_id: i64, replica: &str) -> Result<bool> {
    // Read and written back under the row's lock, in Rust: Postgres reads
    // no JSON holding a NUL character, and the column is text.
    let mut conn = pool.get().await?;
    let tx = conn.transaction().await?;
    let work: Option<db::JsonText<UpgradeWork>> = tx
        .scalar_opt(
            "SELECT spec_json FROM infra_lifecycle_command \
             WHERE id = $1 AND claimed_by_replica = $2 AND completed_at_unix IS NULL FOR UPDATE",
            &[&command_id, &replica],
        )
        .await?;
    let Some(db::JsonText(mut work)) = work else { return Ok(false) };
    work.stopped = true;
    let res = tx.execute("UPDATE infra_lifecycle_command SET spec_json = $2 WHERE id = $1", &[&command_id, &db::JsonText(&work)]).await?;
    tx.commit().await?;
    Ok(res > 0)
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
// replica with a lease).

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
    pool: &Pool,
    signals: &weft_task_store::pg_signal::PgSignalWatch,
    command_id: i64,
    timeout: std::time::Duration,
) -> Result<WaitOutcome> {
    let mut outcomes = wait_for_commands(pool, signals, &[command_id], timeout).await?;
    Ok(outcomes.pop().expect("one outcome per id").1)
}

/// A completed command row's outcome, or `None` while it is pending.
fn completed_outcome(command_id: i64, row: &db::Row) -> Result<Option<WaitOutcome>> {
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
    pool: &Pool,
    project_id: uuid::Uuid,
    command_id: i64,
) -> Result<Option<WaitOutcome>> {
    // Scope by project, NOT just the (globally unique) id: the HTTP
    // endpoint is `/projects/{project}/infra/commands/{id}`, and a
    // caller authorized for one project must not read another's command
    // outcome (which carries raw supervisor error strings). A command
    // under a different project returns None, indistinguishable from
    // pending, so no existence leak.
    let row: Option<db::Row> = pool
        .opt(
            "SELECT completed_at_unix, outcome, outcome_message \
             FROM infra_lifecycle_command WHERE id = $1 AND project_id = $2",
            &[&command_id, &project_id],
        )
        .await?;
    match row {
        Some(r) => completed_outcome(command_id, &r),
        None => Ok(None),
    }
}

/// [`read_command_outcome`], held until `deadline` while the command is
/// still pending: the read ends when the command completes (announced by
/// its row's own trigger) or the deadline comes (its hold ran out, or the
/// copy started stopping). For a client that waits on one command and
/// asks again after each hold.
pub async fn held_command_outcome(
    pool: &Pool,
    signals: &weft_task_store::pg_signal::PgSignalWatch,
    project_id: uuid::Uuid,
    command_id: i64,
    deadline: weft_task_store::stopping::Deadline,
) -> Result<Option<WaitOutcome>> {
    use weft_broker_client::lifecycle_command::{InfraCommandSignal, INFRA_COMMAND_CHANNEL};
    let mut heard = signals.subscribe();
    loop {
        let outcome = read_command_outcome(pool, project_id, command_id).await?;
        if outcome.is_some() {
            return Ok(outcome);
        }
        let woken = heard
            .woken_before(deadline.clone(), |channel, payload| {
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
    pool: &Pool,
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
        let rows = pool
            .query(
                "SELECT id, completed_at_unix, outcome, outcome_message \
                 FROM infra_lifecycle_command WHERE id = ANY($1)",
                &[&command_ids],
            )
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
/// or an upgrade) reaching the project's shared copies is uncompleted. The dispatcher-side
/// "infra operation in flight" fact: while it holds, the
/// reconciliation treats the project as infra-transitional (only
/// infra_cancel offered), which is what stops a new run started by hand
/// from keeping a stop's wait for the runs reaching its copies going
/// (each new one would be waited for too). In-flight work is untouched;
/// only new launches are gated.
pub async fn any_in_flight(pool: &Pool, project_id: uuid::Uuid) -> Result<bool> {
    // Only what reaches the shared copies, which the bar starts and stops:
    // one instance's copy booting holds none of the program's own verbs
    // back (a command for every copy names no instance, and counts).
    Ok(pool
        .scalar(
            &format!(
                "SELECT EXISTS( \
                     SELECT 1 FROM infra_lifecycle_command \
                     WHERE project_id = $1 \
                       AND completed_at_unix IS NULL \
                       AND instance_id IS NULL \
                       AND verb IN ({INFRA_WORK_VERBS_SQL}) \
                 )",
            ),
            &[&project_id],
        )
        .await?)
}

/// How the commands reaching a trigger's copies stand since the infra
/// take-down that took it down ([`since_take_down`]): what its bring-back
/// reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SinceTakeDown {
    /// A take-down (stop, terminate, upgrade) reaching them is still under
    /// way: a copy running now may be on its way down.
    pub under_way: bool,
    /// No take-down reaching them since did what it was asked and one
    /// ended without (called off, or failed), so they are as they were;
    /// or an apply reaching them succeeded after the last one that did
    /// (after the mark when none did): they came back up.
    pub returned: bool,
}

impl SinceTakeDown {
    /// Whether the trigger, whose cut's copies all run, comes back: never
    /// while a take-down reaching them is under way, then once they
    /// returned. A take-down that did what it was asked brings nothing
    /// back, even when a unit kept up by its `on_stop` leaves the copy
    /// reading as running: the person stopped it, and a start brings the
    /// trigger back.
    pub fn brings_back(self) -> bool {
        !self.under_way && self.returned
    }
}

/// [`SinceTakeDown`] for `copies` of project `project_id` (each a place
/// and whose copy: `None` the shared one), against the take-down whose
/// first command is `first_command`. Read by command ids, never by a
/// clock: a command issued after another has the larger id, and the
/// trigger's mark was written in the transaction that issued its own
/// commands. A command reaches a copy when it names its place or every
/// place of its owner, and its owner or every copy.
pub async fn since_take_down(
    pool: &Pool,
    project_id: uuid::Uuid,
    copies: &[(String, Option<weft_core::instance::InstanceId>)],
    first_command: i64,
) -> Result<SinceTakeDown> {
    let places: Vec<&str> = copies.iter().map(|(place, _)| place.as_str()).collect();
    // '' for the shared copy: unnest takes no NULL beside a text array.
    let instances: Vec<&str> = copies.iter().map(|(_, instance)| instance.as_ref().map_or("", |i| i.as_str())).collect();
    let (under_way, returned): (bool, bool) = pool
        .one(
            "WITH reaching AS ( \
                 SELECT c.id, c.verb, c.completed_at_unix, c.outcome FROM infra_lifecycle_command c \
                 WHERE c.project_id = $1 AND c.verb IN ('apply', 'stop', 'terminate', 'upgrade') \
                   AND EXISTS (SELECT 1 FROM unnest($2::text[], $3::text[]) AS t(place, inst) \
                               WHERE (c.node_id IS NULL OR c.node_id = t.place) \
                                 AND (c.every_copy OR c.instance_id IS NOT DISTINCT FROM NULLIF(t.inst, ''))) \
             ), downs AS ( \
                 SELECT id, completed_at_unix, outcome FROM reaching WHERE verb <> 'apply' AND id >= $4 \
             ), last_done AS ( \
                 SELECT MAX(id) AS id FROM downs WHERE outcome = 'succeeded' \
             ) \
             SELECT EXISTS (SELECT 1 FROM reaching WHERE verb <> 'apply' AND completed_at_unix IS NULL), \
                    ((SELECT id FROM last_done) IS NULL \
                      AND EXISTS (SELECT 1 FROM downs WHERE completed_at_unix IS NOT NULL AND outcome IS DISTINCT FROM 'succeeded')) \
                    OR EXISTS (SELECT 1 FROM reaching WHERE verb = 'apply' AND outcome = 'succeeded' \
                                 AND id > COALESCE((SELECT id FROM last_done), $4))",
            &[&project_id, &places, &instances, &first_command],
        )
        .await?;
    Ok(SinceTakeDown { under_way, returned })
}

/// Cancel one owner's in-flight infra work (apply / stop / terminate,
/// and an upgrade, whose claimer reads the flag between its stop and
/// its start): the shared copies' (`instance` `None`, which also reaches
/// a command for every copy, since that one takes the shared copies
/// down too) or one instance's. Flags CLAIMED rows (`cancel_requested =
/// TRUE`; the executing supervisor hears it through the `cancel:<id>`
/// notice the flag's trigger sends and halts at its next step; the
/// supervisor's claim marks `claimed_by_replica` like the dispatcher's
/// does) and completes still-UNCLAIMED rows outright with
/// outcome `cancelled` (nothing has touched the install for them yet).
/// Flag first, then complete: a row claimed between the two statements
/// has its flag already set, so no window exists where a command
/// escapes the cancel. Dispatcher-owned verbs (deactivate /
/// reactivate) are health's work and never touched. Returns how many
/// rows were touched.
pub async fn request_cancel_owner(
    pool: &Pool,
    project_id: uuid::Uuid,
    instance: Option<&weft_core::instance::InstanceId>,
) -> Result<u64> {
    let owner = format!(
        "project_id = $1 AND completed_at_unix IS NULL \
         AND instance_id IS NOT DISTINCT FROM $2 \
         AND verb IN ({INFRA_WORK_VERBS_SQL})",
    );
    let instance = instance.map(|m| m.as_str());
    let flagged = pool
        .execute(&format!("UPDATE infra_lifecycle_command SET cancel_requested = TRUE WHERE {owner}"), &[&project_id, &instance])
        .await?;
    let completed = pool
        .execute(
            &format!(
                "UPDATE infra_lifecycle_command \
                 SET completed_at_unix = EXTRACT(EPOCH FROM NOW())::BIGINT, \
                     outcome = 'cancelled', \
                     outcome_message = 'cancelled by user before run' \
                 WHERE {owner} AND claimed_by_replica IS NULL"
            ),
            &[&project_id, &instance],
        )
        .await?;
    // `flagged` counted the completed ones too (they were uncompleted
    // at flag time); the touched total is just `flagged`, `completed`
    // is informational.
    tracing::info!(
        target: "weft_dispatcher::infra_lifecycle_command",
        project_id = %project_id,
        instance = instance.unwrap_or("<shared>"),
        flagged,
        cancelled_unclaimed = completed,
        "infra cancel requested"
    );
    Ok(flagged)
}

/// A person asks the supervisor to look at `project_id`'s infra again:
/// the time is kept on the project's `infra_health_look`, which arms
/// again every health rule that gave up at or before it
/// (`weft_broker::lifecycle_writes::health_state`), the supervisor's
/// passes over the project start a fresh round of tries (no failure
/// counted, no retry booked: the look is the next pass), and the ask is
/// announced as that commits
/// (`weft_task_store::infra_command::ask_for_look`).
pub async fn person_asks_for_look(pool: &Pool, project_id: uuid::Uuid) -> Result<()> {
    let mut conn = pool.get().await?;
    let tx = conn.transaction().await?;
    tx.execute(
        "INSERT INTO infra_health_look (project_id, asked_at_unix) VALUES ($1, EXTRACT(EPOCH FROM NOW())::BIGINT) \
         ON CONFLICT (project_id) DO UPDATE SET asked_at_unix = EXCLUDED.asked_at_unix, failures = 0, retry_at_unix = NULL",
        &[&project_id],
    )
    .await?;
    weft_task_store::infra_command::ask_for_look(&tx, project_id).await?;
    tx.commit().await?;
    Ok(())
}

/// Whether a claimed command's cancellation was requested. Read by the
/// dispatcher's own commands (an upgrade between its stop and its start)
/// at their steps.
pub async fn cancel_requested(pool: &Pool, command_id: i64) -> Result<bool> {
    let row: Option<bool> = pool.scalar_opt("SELECT cancel_requested FROM infra_lifecycle_command WHERE id = $1", &[&command_id]).await?;
    // A missing row (removed project) reads as cancelled: the executor
    // should stop working on it either way.
    Ok(row.unwrap_or(true))
}

/// Drop every row for a project. Called on `weft rm`.
pub async fn remove_project(db: &impl Client, project_id: uuid::Uuid) -> Result<u64> {
    Ok(db
        .execute(
            "WITH health AS (DELETE FROM infra_health_rule WHERE project_id = $1), \
                  look AS (DELETE FROM infra_health_look WHERE project_id = $1) \
             DELETE FROM infra_lifecycle_command WHERE project_id = $1",
            &[&project_id],
        )
        .await?)
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
        let read_back = |teardown: Teardown| {
            let (force, spec_json) = teardown.columns();
            let row = SupervisorCommandRow {
                id: 1,
                project_id: uuid::Uuid::from_u128(1),
                node_id: None,
                copies: weft_core::instance::Copies::Shared,
                verb: teardown.verb(),
                spec_json,
                force,
                cancel_requested: false,
            };
            (row.force, row.terminate_work())
        };
        let wipe = Teardown::Terminate { disks: TerminateDisks::DeleteAll };
        assert_eq!(wipe.verb(), InfraLifecycleVerb::Terminate);
        assert_eq!(read_back(wipe), (false, Ok(TerminateWork { disks: TerminateDisks::DeleteAll })));
        assert_eq!(read_back(Teardown::TERMINATE), (false, Ok(TerminateWork { disks: TerminateDisks::KeepListed })));
        let stop = Teardown::Stop { force: true };
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

}
