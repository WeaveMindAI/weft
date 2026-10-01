//! Trigger activations: which triggers of a project are listening, and
//! for whom.
//!
//! The unit of activation is ONE trigger for ONE owner: the program's
//! own shared copy of a trigger, or one instance's copy of a per-instance
//! trigger. Each has its own row in `trigger_activation`, with its own
//! lifecycle (activating, active, deactivating, inactive, and for an
//! inactive one the way it went down: wipe, hibernate or park). So one
//! trigger can be taken down while its siblings keep listening, and one
//! instance's triggers without touching another's.
//!
//! No row means the trigger was never activated for that owner. The
//! project's own status is an aggregate over its shared activations
//! ([`aggregate`]), which is what the editor's action bar and
//! `weft status` show; per-instance activations are counted beside it.
//!
//! A signal knows which activation governs it (`signal.activation_trigger`
//! plus `signal.instance_id`): an entry signal is its trigger's own, and a
//! run's waits belong to the activation of the trigger that fired the
//! run. The fire gate reads that activation's lifecycle; a signal no
//! activation governs (a run started by hand) is always live.

use std::sync::Arc;

use async_trait::async_trait;
use sqlx::postgres::PgPool;

use weft_core::activation::ActivationKey;
use weft_core::instance::{InstanceId, Owner};
use weft_core::project::hash::ProgramIdentity;

#[cfg(any(test, feature = "test-helpers"))]
use std::collections::HashMap;
#[cfg(any(test, feature = "test-helpers"))]
use tokio::sync::RwLock;

pub use weft_broker_client::activation::{aggregate, ActivationLifecycle};
pub use weft_core::projects::ProjectStatus;

/// One activation row.
#[derive(Debug, Clone)]
pub struct Activation {
    pub key: ActivationKey,
    pub lifecycle: ActivationLifecycle,
    /// The code the activation's listeners fire (recorded when its setup
    /// finished); `None` while activating and once taken down. Compared
    /// against the source for `weft status`'s activation drift.
    pub program: Option<ProgramIdentity>,
    /// The source version its setup ran, for the version tree.
    pub source_version: Option<String>,
}

/// Outcome of a guarded lifecycle write. `Rejected` names the state
/// that blocked it so the caller can say which.
/// `Applied` hands back the signals the written activations govern, read
/// or deleted in the write's own transaction ([`SignalsGoing`]), for the
/// listener cleanup.
#[derive(Debug, Clone)]
pub enum LifecycleWrite {
    Applied { unlisten: Vec<crate::journal::SignalRegistration> },
    Rejected { blocker: String },
    NotFound,
}

/// Which signal rows a guarded lifecycle write deletes in its own
/// transaction, so the rows and the signals they govern leave together:
/// a retry after a failure later on never finds a row gone and its
/// signals still registered.
#[derive(Debug, Clone, Copy)]
pub enum SignalsGoing {
    /// None (a park or a hibernate keeps them for the reactivation). The
    /// ones the written activations govern are read in the same
    /// transaction, so a reactivation committing after it cannot slip
    /// its fresh signals into the listener cleanup.
    Kept,
    /// Every signal of the project, except the ones `except`'s run
    /// holds (the run that asked for the take-down).
    Project { except: Option<uuid::Uuid> },
    /// The signals the written activations govern.
    Activations,
}

/// A change of what a run is born with, stored in the transaction that
/// lands an activation (a re-arm, `crate::instance_values::change`): the
/// change and the triggers set up with it are stored together or not at
/// all: one instance's values. (The install's picks are stored before their
/// re-arm instead: `crate::install_picks`.)
pub struct ValuesStore<'a> {
    /// The tenant the values are keyed by: the project's owner.
    pub tenant: &'a str,
    pub instance: &'a InstanceId,
    pub writes: &'a [weft_access_store::InstanceValueWrite],
    /// The `(step, field)` pairs the change forgets.
    pub cleared: &'a [(String, String)],
}

impl ValuesStore<'_> {
    /// Store the change on `tx`.
    pub async fn store(&self, tx: &mut sqlx::PgConnection, project_id: uuid::Uuid) -> anyhow::Result<()> {
        weft_access_store::change_instance_values(tx, self.tenant, project_id, self.instance, self.writes, self.cleared).await
    }
}

/// An activation whose driver died mid-activation (its heartbeat went
/// stale): the reaper rolls it back.
#[derive(Debug, Clone)]
pub struct StuckActivation {
    pub project_id: uuid::Uuid,
    pub execution_id: uuid::Uuid,
}

/// Backing store for activations. Implementations:
/// - `PostgresActivationStore` (production)
/// - `FakeActivationStore` (tests, behind `test-helpers`).
#[async_trait]
pub trait ActivationStoreOps: Send + Sync {
    /// Every activation row of a project.
    async fn list(&self, project_id: uuid::Uuid) -> anyhow::Result<Vec<Activation>>;

    /// Single-flight entry into Activating for ALL of `keys` at once,
    /// under one setup `execution_id`. Refused ([`ClaimRefused`], decided in the
    /// claim's own transaction) when any of them is already activating,
    /// or while the project is building (the build transition lives on
    /// the project row). Every other status is a
    /// legal entry: never activated, inactive, active, deactivating (the
    /// roll-forward, and the health auto-recover). Rows are created on
    /// first activation. Stamps the heartbeat. Answers the rows among
    /// `keys` as they were just before the claim (none for a key never
    /// activated), read in the same transaction: what [`Self::restore`]
    /// puts back when the activation fails before it touched anything.
    /// With `expect`, the claim is also refused unless every one of
    /// `keys` exists and is still in that status: a re-arm that saw its
    /// triggers Active must not turn back on one a deactivate took down
    /// since.
    async fn try_begin_activating(
        &self,
        project_id: uuid::Uuid,
        keys: &[ActivationKey],
        execution_id: uuid::Uuid,
        expect: Option<ProjectStatus>,
    ) -> anyhow::Result<Result<Vec<Activation>, ClaimRefused>>;

    /// End the activation `execution_id` names (every row it claimed) at `to`; an
    /// instance's row landing wiped goes instead ([`wipe_forgets`]). With
    /// `remove_signals`, the signals those activations govern are deleted
    /// in the same transaction and handed back for the listener cleanup;
    /// with `values`, that change is stored in it too. `None`
    /// (and nothing stored) if `execution_id` no longer owns any row.
    async fn end_activating(
        &self,
        project_id: uuid::Uuid,
        execution_id: uuid::Uuid,
        to: &ActivationLifecycle,
        remove_signals: bool,
        values: Option<&ValuesStore<'_>>,
    ) -> anyhow::Result<Option<Vec<crate::journal::SignalRegistration>>>;

    /// Put the activations `execution_id` claimed back as they were before the
    /// claim (`previous`, what [`Self::try_begin_activating`] answered):
    /// an activation that failed before touching any signal leaves each
    /// trigger as it found it (a parked one parked, a live one listening
    /// on the code it fired before), and the row of one never activated
    /// before goes again, so it reads as never activated. Guarded by the
    /// claim, like [`Self::end_activating`]; `false` when `execution_id` no
    /// longer owns them.
    async fn restore(
        &self,
        project_id: uuid::Uuid,
        execution_id: uuid::Uuid,
        previous: &[Activation],
    ) -> anyhow::Result<bool>;

    /// Record the code an activation's listeners now fire. Guarded by the
    /// activation's own execution, so a cancelled driver cannot overwrite a
    /// newer activation's record.
    async fn record_activation_source(
        &self,
        project_id: uuid::Uuid,
        execution_id: uuid::Uuid,
        program: &ProgramIdentity,
        source_version: &str,
    ) -> anyhow::Result<bool>;

    /// Set the lifecycle of every row among `keys`, GUARDED: refused while
    /// any of them is activating (cancel the activation first) or while
    /// the project is building. Keys with no row are skipped: there is
    /// nothing listening to take down. `signals` names the signal rows
    /// deleted in the same transaction.
    async fn set_lifecycle_guarded(
        &self,
        project_id: uuid::Uuid,
        keys: &[ActivationKey],
        lifecycle: &ActivationLifecycle,
        signals: SignalsGoing,
    ) -> anyhow::Result<LifecycleWrite>;

    /// Compare-and-set one activation's status (the drain landing).
    async fn cas_status(
        &self,
        project_id: uuid::Uuid,
        key: &ActivationKey,
        from: ProjectStatus,
        to: ProjectStatus,
    ) -> anyhow::Result<bool>;

    /// Bump the heartbeat of the activation `execution_id` names; the driving
    /// process does this on an interval so the reaper only repairs dead ones.
    async fn bump_heartbeat(&self, execution_id: uuid::Uuid) -> anyhow::Result<()>;

    /// Activations stuck in Activating whose heartbeat went stale.
    async fn list_stuck(&self, stale_before: i64) -> anyhow::Result<Vec<StuckActivation>>;

    /// Every activation draining right now, with its project.
    async fn list_deactivating(&self) -> anyhow::Result<Vec<(uuid::Uuid, ActivationKey)>>;
}

pub type ActivationStore = Arc<dyn ActivationStoreOps>;

/// Whether an activation of `key` landing at `lifecycle` leaves its row
/// behind. An instance's copy of a trigger wiped (inactive and refusing
/// fires) is the same as one never activated, so its row goes: nothing
/// lists the instance any more, and activating it again starts fresh. The
/// shared copy keeps its row, which is what the project's status reads.
fn wipe_forgets(key: &ActivationKey, lifecycle: &ActivationLifecycle) -> bool {
    key.instance().is_some() && lands_wiped(lifecycle)
}

/// Whether `lifecycle` is a wipe (inactive, refusing fires): the half of
/// [`wipe_forgets`] about where a row lands.
fn lands_wiped(lifecycle: &ActivationLifecycle) -> bool {
    lifecycle.status == ProjectStatus::Inactive && !lifecycle.accepting_fires
}

/// The same test as [`wipe_forgets`], over a `trigger_activation` row
/// (`a`) landing at status `$1`.
const WIPE_FORGETS_SQL: &str = "a.instance_id IS NOT NULL AND $1 = 'inactive' AND NOT a.accepting_fires";

/// The `trigger_activation` table: one row per trigger per owner that
/// was ever activated. The canonical CREATE lives here, edited in place;
/// an existing database is carried to it by a migration written with
/// `./setup.sh --migration <name> --release`.
pub static GROUP: weft_task_store::SchemaGroup = weft_task_store::SchemaGroup {
    name: "trigger_activation",
    tables: &["trigger_activation"],
    ddl: &[
        r#"CREATE TABLE IF NOT EXISTS trigger_activation (
            project_id UUID NOT NULL REFERENCES project(id) ON DELETE CASCADE,
            -- The trigger, spelled the way the program reads it (`door`,
            -- `one.door` inside an included file): the same spelling the
            -- signal rows carry.
            trigger TEXT NOT NULL,
            -- Whose activation: NULL for the program's shared trigger,
            -- else the instance whose copy of a per-instance trigger it is.
            instance_id TEXT,
            -- registered | activating | active | deactivating | inactive
            status TEXT NOT NULL,
            accepting_fires BOOLEAN NOT NULL,
            fires_visible_to_consumers BOOLEAN NOT NULL,
            fires_deadline_unix BIGINT,
            drain_deadline_unix BIGINT,
            deactivated_by_health BOOLEAN NOT NULL DEFAULT FALSE,
            -- The trigger-setup run of the activation in flight; one run
            -- sets up every activation a verb names, so rows share it.
            activating_execution_id UUID,
            heartbeat_unix BIGINT NOT NULL DEFAULT 0,
            -- What the listeners fire, recorded when setup finished.
            activation_program JSONB,
            activation_version TEXT,
            updated_at BIGINT NOT NULL
        )"#,
        r#"CREATE UNIQUE INDEX IF NOT EXISTS trigger_activation_key
             ON trigger_activation (project_id, trigger, instance_id) NULLS NOT DISTINCT"#,
        r#"CREATE INDEX IF NOT EXISTS trigger_activation_execution_id
             ON trigger_activation (activating_execution_id) WHERE activating_execution_id IS NOT NULL"#,
        r#"CREATE INDEX IF NOT EXISTS trigger_activation_transitional
             ON trigger_activation (status) WHERE status IN ('activating', 'deactivating')"#,
    ],
    seed: &[],
};

#[derive(Clone)]
pub struct PostgresActivationStore {
    pool: PgPool,
}

impl PostgresActivationStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

type LifecycleRow = (
    String,
    Option<String>,
    String,
    bool,
    bool,
    Option<i64>,
    Option<i64>,
    bool,
    Option<uuid::Uuid>,
    Option<serde_json::Value>,
    Option<String>,
);

fn row_to_activation(row: LifecycleRow) -> anyhow::Result<Activation> {
    let (trigger, instance, status, accepting, visible, deadline, drain, by_health, execution_id, program, version) = row;
    let instance = instance.map(InstanceId::new).transpose().map_err(|e| anyhow::anyhow!("trigger_activation.instance_id: {e}"))?;
    Ok(Activation {
        key: ActivationKey::new(trigger, Owner::from_instance(instance)),
        lifecycle: ActivationLifecycle {
            status: ProjectStatus::parse(&status)
                .ok_or_else(|| anyhow::anyhow!("unknown trigger_activation.status '{status}'"))?,
            accepting_fires: accepting,
            fires_visible_to_consumers: visible,
            fires_deadline_unix: deadline,
            drain_deadline_unix: drain,
            deactivated_by_health: by_health,
            activating_execution_id: execution_id,
        },
        program: program.map(serde_json::from_value).transpose()?,
        source_version: version,
    })
}

/// Why [`ActivationStoreOps::try_begin_activating`] refused its claim.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimRefused {
    /// The project is building (or gone): no activation starts.
    Building,
    /// One of the keys is already activating: another claim holds it.
    Claimed,
    /// With `expect`: one of the keys is missing or not in that status.
    NotExpected,
}

/// Whether a claim of `keys` may go ahead over the rows it read
/// (`before`, one per existing key): none of them already activating,
/// and with `expect`, every key present and in that status.
fn claimable(keys:&[ActivationKey], before: &[Activation], expect: Option<ProjectStatus>) -> Result<(), ClaimRefused> {
    if before.iter().any(|a| a.lifecycle.status == ProjectStatus::Activating) {
        return Err(ClaimRefused::Claimed);
    }
    match expect {
        Some(status) if before.len() != keys.len() || before.iter().any(|a| a.lifecycle.status != status) => {
            Err(ClaimRefused::NotExpected)
        }
        _ => Ok(()),
    }
}

/// The keys as two parallel arrays, for an `unnest` join: the triggers,
/// and the instances with `''` standing for the shared owner (an array
/// cannot carry a NULL that `IS NOT DISTINCT FROM` would match on both
/// sides reliably through `unnest`, so the join compares
/// `COALESCE(instance_id, '')`; an instance id is never empty).
fn key_arrays(keys:&[ActivationKey]) -> (Vec<String>, Vec<String>) {
    keys.iter()
        .map(|k| (k.trigger.clone(), k.instance().map(|m| m.as_str().to_string()).unwrap_or_default()))
        .unzip()
}

const KEYS_JOIN: &str = "(a.trigger, COALESCE(a.instance_id, '')) IN (SELECT * FROM unnest($2::text[], $3::text[]))";

#[async_trait]
impl ActivationStoreOps for PostgresActivationStore {
    async fn list(&self, project_id: uuid::Uuid) -> anyhow::Result<Vec<Activation>> {
        let rows: Vec<LifecycleRow> = sqlx::query_as(
            "SELECT trigger, instance_id, status, accepting_fires, fires_visible_to_consumers, \
                    fires_deadline_unix, drain_deadline_unix, deactivated_by_health, activating_execution_id, \
                    activation_program, activation_version \
             FROM trigger_activation WHERE project_id = $1 ORDER BY trigger, instance_id NULLS FIRST",
        )
        .bind(project_id)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(row_to_activation).collect()
    }

    async fn try_begin_activating(
        &self,
        project_id: uuid::Uuid,
        keys: &[ActivationKey],
        execution_id: uuid::Uuid,
        expect: Option<ProjectStatus>,
    ) -> anyhow::Result<Result<Vec<Activation>, ClaimRefused>> {
        let now = crate::lease::now_unix();
        let (triggers, instances) = key_arrays(keys);
        let mut tx = self.pool.begin().await?;
        // The project row serializes activations with the build
        // transition: a build refuses while any activation is mid-flip,
        // and an activation refuses while a build is in flight.
        let transition: Option<(String,)> =
            sqlx::query_as("SELECT transition FROM project WHERE id = $1 FOR UPDATE")
                .bind(project_id)
                .fetch_optional(&mut *tx)
                .await?;
        match transition {
            Some((t,)) if t == "none" => {}
            _ => return Ok(Err(ClaimRefused::Building)),
        }
        let before: Vec<LifecycleRow> = sqlx::query_as(&format!(
            "SELECT a.trigger, a.instance_id, a.status, a.accepting_fires, a.fires_visible_to_consumers, \
                    a.fires_deadline_unix, a.drain_deadline_unix, a.deactivated_by_health, a.activating_execution_id, \
                    a.activation_program, a.activation_version \
             FROM trigger_activation a WHERE a.project_id = $1 AND {KEYS_JOIN} FOR UPDATE"
        ))
        .bind(project_id)
        .bind(&triggers)
        .bind(&instances)
        .fetch_all(&mut *tx)
        .await?;
        let before: Vec<Activation> = before.into_iter().map(row_to_activation).collect::<anyhow::Result<_>>()?;
        if let Err(refused) = claimable(keys, &before, expect) {
            return Ok(Err(refused));
        }
        let activating = ActivationLifecycle::activating(execution_id);
        sqlx::query(
            "INSERT INTO trigger_activation \
               (project_id, trigger, instance_id, status, accepting_fires, fires_visible_to_consumers, \
                fires_deadline_unix, drain_deadline_unix, deactivated_by_health, activating_execution_id, \
                heartbeat_unix, activation_program, activation_version, updated_at) \
             SELECT $1, t, NULLIF(m, ''), $4, $5, $6, NULL, NULL, FALSE, $7, $8, NULL, NULL, $8 \
             FROM unnest($2::text[], $3::text[]) AS k(t, m) \
             ON CONFLICT (project_id, trigger, instance_id) DO UPDATE SET \
                 status = EXCLUDED.status, \
                 accepting_fires = EXCLUDED.accepting_fires, \
                 fires_visible_to_consumers = EXCLUDED.fires_visible_to_consumers, \
                 fires_deadline_unix = NULL, \
                 drain_deadline_unix = NULL, \
                 deactivated_by_health = FALSE, \
                 activating_execution_id = EXCLUDED.activating_execution_id, \
                 heartbeat_unix = EXCLUDED.heartbeat_unix, \
                 activation_program = NULL, \
                 activation_version = NULL, \
                 updated_at = EXCLUDED.updated_at",
        )
        .bind(project_id)
        .bind(&triggers)
        .bind(&instances)
        .bind(activating.status.as_str())
        .bind(activating.accepting_fires)
        .bind(activating.fires_visible_to_consumers)
        .bind(execution_id)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(Ok(before))
    }

    async fn end_activating(
        &self,
        project_id: uuid::Uuid,
        execution_id: uuid::Uuid,
        to: &ActivationLifecycle,
        remove_signals: bool,
        values: Option<&ValuesStore<'_>>,
    ) -> anyhow::Result<Option<Vec<crate::journal::SignalRegistration>>> {
        let mut tx = self.pool.begin().await?;
        let owned = "project_id = $1 AND activating_execution_id = $2 AND status = 'activating'";
        // An instance's rows landing wiped go ([`wipe_forgets`]); the rest
        // move to `to`.
        let mut ended: Vec<(String, Option<String>)> = if lands_wiped(to) {
            sqlx::query_as(&format!(
                "DELETE FROM trigger_activation WHERE {owned} AND instance_id IS NOT NULL RETURNING trigger, instance_id"
            ))
            .bind(project_id)
            .bind(execution_id)
            .fetch_all(&mut *tx)
            .await?
        } else {
            Vec::new()
        };
        let moved: Vec<(String, Option<String>)> = sqlx::query_as(
            "UPDATE trigger_activation \
             SET status = $1, accepting_fires = $2, fires_visible_to_consumers = $3, \
                 fires_deadline_unix = $4, deactivated_by_health = $5, drain_deadline_unix = $6, \
                 activating_execution_id = NULL, \
                 activation_program = CASE WHEN $1 = 'active' THEN activation_program ELSE NULL END, \
                 activation_version = CASE WHEN $1 = 'active' THEN activation_version ELSE NULL END, \
                 updated_at = $7 \
             WHERE project_id = $8 AND activating_execution_id = $9 AND status = 'activating' \
             RETURNING trigger, instance_id",
        )
        .bind(to.status.as_str())
        .bind(to.accepting_fires)
        .bind(to.fires_visible_to_consumers)
        .bind(to.fires_deadline_unix)
        .bind(to.deactivated_by_health)
        .bind(to.drain_deadline_unix)
        .bind(crate::lease::now_unix())
        .bind(project_id)
        .bind(execution_id)
        .fetch_all(&mut *tx)
        .await?;
        ended.extend(moved);
        if ended.is_empty() {
            tx.commit().await?;
            return Ok(None);
        }
        if let Some(values) = values {
            values.store(&mut tx, project_id).await?;
        }
        let removed = if remove_signals {
            let keys: Vec<ActivationKey> = ended
                .into_iter()
                .map(|(trigger, instance)| {
                    Ok(ActivationKey::new(trigger, Owner::from_instance(instance.map(InstanceId::new).transpose().map_err(anyhow::Error::msg)?)))
                })
                .collect::<anyhow::Result<_>>()?;
            crate::journal::postgres::remove_activation_signals(&mut *tx, project_id, &keys).await?
        } else {
            Vec::new()
        };
        tx.commit().await?;
        Ok(Some(removed))
    }

    async fn restore(
        &self,
        project_id: uuid::Uuid,
        execution_id: uuid::Uuid,
        previous: &[Activation],
    ) -> anyhow::Result<bool> {
        let now = crate::lease::now_unix();
        let mut tx = self.pool.begin().await?;
        let mut restored = 0;
        for activation in previous {
            let before = &activation.lifecycle;
            restored += sqlx::query(
                "UPDATE trigger_activation \
                 SET status = $1, accepting_fires = $2, fires_visible_to_consumers = $3, \
                     fires_deadline_unix = $4, deactivated_by_health = $5, drain_deadline_unix = $6, \
                     activating_execution_id = NULL, activation_program = $7, activation_version = $8, updated_at = $9 \
                 WHERE project_id = $10 AND activating_execution_id = $11 AND status = 'activating' \
                   AND trigger = $12 AND instance_id IS NOT DISTINCT FROM $13",
            )
            .bind(before.status.as_str())
            .bind(before.accepting_fires)
            .bind(before.fires_visible_to_consumers)
            .bind(before.fires_deadline_unix)
            .bind(before.deactivated_by_health)
            .bind(before.drain_deadline_unix)
            .bind(activation.program.as_ref().map(sqlx::types::Json))
            .bind(activation.source_version.as_deref())
            .bind(now)
            .bind(project_id)
            .bind(execution_id)
            .bind(&activation.key.trigger)
            .bind(activation.key.instance().map(InstanceId::as_str))
            .execute(&mut *tx)
            .await?
            .rows_affected();
        }
        // What had no row before the claim was never activated: its row
        // goes again.
        restored += sqlx::query(
            "DELETE FROM trigger_activation \
             WHERE project_id = $1 AND activating_execution_id = $2 AND status = 'activating'",
        )
        .bind(project_id)
        .bind(execution_id)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        tx.commit().await?;
        Ok(restored > 0)
    }

    async fn record_activation_source(
        &self,
        project_id: uuid::Uuid,
        execution_id: uuid::Uuid,
        program: &ProgramIdentity,
        source_version: &str,
    ) -> anyhow::Result<bool> {
        let res = sqlx::query(
            "UPDATE trigger_activation SET activation_program = $3, activation_version = $4 \
             WHERE project_id = $1 AND activating_execution_id = $2 AND status = 'activating'",
        )
        .bind(project_id)
        .bind(execution_id)
        .bind(sqlx::types::Json(program))
        .bind(source_version)
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected() > 0)
    }

    async fn set_lifecycle_guarded(
        &self,
        project_id: uuid::Uuid,
        keys: &[ActivationKey],
        lifecycle: &ActivationLifecycle,
        signals: SignalsGoing,
    ) -> anyhow::Result<LifecycleWrite> {
        let (triggers, instances) = key_arrays(keys);
        let mut tx = self.pool.begin().await?;
        let transition: Option<(String,)> =
            sqlx::query_as("SELECT transition FROM project WHERE id = $1 FOR UPDATE")
                .bind(project_id)
                .fetch_optional(&mut *tx)
                .await?;
        let Some((transition,)) = transition else {
            return Ok(LifecycleWrite::NotFound);
        };
        if transition != "none" {
            return Ok(LifecycleWrite::Rejected { blocker: transition });
        }
        let activating: Option<(String, Option<String>)> = sqlx::query_as(&format!(
            "SELECT a.trigger, a.instance_id FROM trigger_activation a \
             WHERE a.project_id = $1 AND a.status = 'activating' AND {KEYS_JOIN} LIMIT 1"
        ))
        .bind(project_id)
        .bind(&triggers)
        .bind(&instances)
        .fetch_optional(&mut *tx)
        .await?;
        if let Some((trigger, instance)) = activating {
            let whose = instance.map(|m| format!(" of instance '{m}'")).unwrap_or_default();
            return Ok(LifecycleWrite::Rejected { blocker: format!("activating (trigger '{trigger}'{whose})") });
        }
        let forgotten: Vec<&ActivationKey> = keys.iter().filter(|k| wipe_forgets(k, lifecycle)).collect();
        if !forgotten.is_empty() {
            let (triggers, instances) = key_arrays(&forgotten.into_iter().cloned().collect::<Vec<_>>());
            sqlx::query(&format!("DELETE FROM trigger_activation a WHERE a.project_id = $1 AND {KEYS_JOIN}"))
                .bind(project_id)
                .bind(&triggers)
                .bind(&instances)
                .execute(&mut *tx)
                .await?;
        }
        sqlx::query(&format!(
            "UPDATE trigger_activation a \
             SET status = $4, accepting_fires = $5, fires_visible_to_consumers = $6, \
                 fires_deadline_unix = $7, deactivated_by_health = $8, drain_deadline_unix = $9, \
                 activating_execution_id = NULL, \
                 activation_program = CASE WHEN $4 IN ('inactive', 'deactivating') THEN NULL ELSE activation_program END, \
                 activation_version = CASE WHEN $4 IN ('inactive', 'deactivating') THEN NULL ELSE activation_version END, \
                 updated_at = $10 \
             WHERE a.project_id = $1 AND {KEYS_JOIN}"
        ))
        .bind(project_id)
        .bind(&triggers)
        .bind(&instances)
        .bind(lifecycle.status.as_str())
        .bind(lifecycle.accepting_fires)
        .bind(lifecycle.fires_visible_to_consumers)
        .bind(lifecycle.fires_deadline_unix)
        .bind(lifecycle.deactivated_by_health)
        .bind(lifecycle.drain_deadline_unix)
        .bind(crate::lease::now_unix())
        .execute(&mut *tx)
        .await?;
        let unlisten = match signals {
            SignalsGoing::Kept => crate::journal::postgres::activation_signals(&mut *tx, project_id, keys).await?,
            SignalsGoing::Project { except } => {
                crate::journal::postgres::remove_project_signals_except(&mut *tx, project_id, except).await?
            }
            SignalsGoing::Activations => {
                crate::journal::postgres::remove_activation_signals(&mut *tx, project_id, keys).await?
            }
        };
        tx.commit().await?;
        Ok(LifecycleWrite::Applied { unlisten })
    }

    async fn cas_status(
        &self,
        project_id: uuid::Uuid,
        key: &ActivationKey,
        from: ProjectStatus,
        to: ProjectStatus,
    ) -> anyhow::Result<bool> {
        // A wiped instance's drain landing forgets the row (`wipe_forgets`);
        // anything else moves its status. One statement each, both
        // guarded by `from`, and at most one of them matches.
        let mut tx = self.pool.begin().await?;
        let key_match = "a.project_id = $2 AND a.trigger = $3 AND a.instance_id IS NOT DISTINCT FROM $4 AND a.status = $5";
        let forgot = sqlx::query(&format!("DELETE FROM trigger_activation a WHERE {key_match} AND {WIPE_FORGETS_SQL}"))
            .bind(to.as_str())
            .bind(project_id)
            .bind(&key.trigger)
            .bind(key.instance().map(|m| m.as_str()))
            .bind(from.as_str())
            .execute(&mut *tx)
            .await?;
        let moved = sqlx::query(&format!(
            "UPDATE trigger_activation a SET status = $1, updated_at = $6, \
                 drain_deadline_unix = CASE WHEN $1 = 'deactivating' THEN a.drain_deadline_unix ELSE NULL END \
             WHERE {key_match}"
        ))
        .bind(to.as_str())
        .bind(project_id)
        .bind(&key.trigger)
        .bind(key.instance().map(|m| m.as_str()))
        .bind(from.as_str())
        .bind(crate::lease::now_unix())
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(forgot.rows_affected() + moved.rows_affected() > 0)
    }

    async fn bump_heartbeat(&self, execution_id: uuid::Uuid) -> anyhow::Result<()> {
        sqlx::query("UPDATE trigger_activation SET heartbeat_unix = $1 WHERE activating_execution_id = $2")
            .bind(crate::lease::now_unix())
            .bind(execution_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn list_stuck(&self, stale_before: i64) -> anyhow::Result<Vec<StuckActivation>> {
        let rows: Vec<(uuid::Uuid, uuid::Uuid)> = sqlx::query_as(
            "SELECT DISTINCT project_id, activating_execution_id FROM trigger_activation \
             WHERE status = 'activating' AND activating_execution_id IS NOT NULL AND heartbeat_unix < $1",
        )
        .bind(stale_before)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|(project_id, execution_id)| StuckActivation { project_id, execution_id }).collect())
    }

    async fn list_deactivating(&self) -> anyhow::Result<Vec<(uuid::Uuid, ActivationKey)>> {
        let rows: Vec<(uuid::Uuid, String, Option<String>)> = sqlx::query_as(
            "SELECT project_id, trigger, instance_id FROM trigger_activation WHERE status = 'deactivating'",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|(project, trigger, instance)| {
                let instance = instance.map(InstanceId::new).transpose().map_err(anyhow::Error::msg)?;
                Ok((project, ActivationKey::new(trigger, Owner::from_instance(instance))))
            })
            .collect()
    }
}

/// One fake row: the lifecycle, the heartbeat, and the code and version
/// its setup recorded.
#[cfg(any(test, feature = "test-helpers"))]
type FakeRow = (ActivationLifecycle, i64, Option<ProgramIdentity>, Option<String>);

/// In-memory store for the dispatcher's contract tests.
#[cfg(any(test, feature = "test-helpers"))]
#[derive(Default)]
pub struct FakeActivationStore {
    rows: RwLock<HashMap<(uuid::Uuid, ActivationKey), FakeRow>>,
    /// Projects mid-build: an activation is refused while one is.
    building: RwLock<std::collections::HashSet<uuid::Uuid>>,
}

#[cfg(any(test, feature = "test-helpers"))]
impl FakeActivationStore {
    pub fn new() -> Self {
        Self::default()
    }

}

#[cfg(any(test, feature = "test-helpers"))]
#[async_trait]
impl ActivationStoreOps for FakeActivationStore {
    async fn list(&self, project_id: uuid::Uuid) -> anyhow::Result<Vec<Activation>> {
        let mut out: Vec<Activation> = self
            .rows
            .read()
            .await
            .iter()
            .filter(|((p, _), _)| *p == project_id)
            .map(|((_, key), (lifecycle, _, program, version))| Activation {
                key: key.clone(),
                lifecycle: lifecycle.clone(),
                program: program.clone(),
                source_version: version.clone(),
            })
            .collect();
        out.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(out)
    }

    async fn try_begin_activating(
        &self,
        project_id: uuid::Uuid,
        keys: &[ActivationKey],
        execution_id: uuid::Uuid,
        expect: Option<ProjectStatus>,
    ) -> anyhow::Result<Result<Vec<Activation>, ClaimRefused>> {
        if self.building.read().await.contains(&project_id) {
            return Ok(Err(ClaimRefused::Building));
        }
        let mut rows = self.rows.write().await;
        let before: Vec<Activation> = keys
            .iter()
            .filter_map(|key| {
                let (lifecycle, _, program, version) = rows.get(&(project_id, key.clone()))?;
                Some(Activation {
                    key: key.clone(),
                    lifecycle: lifecycle.clone(),
                    program: program.clone(),
                    source_version: version.clone(),
                })
            })
            .collect();
        if let Err(refused) = claimable(keys, &before, expect) {
            return Ok(Err(refused));
        }
        for key in keys {
            rows.insert((project_id, key.clone()), (ActivationLifecycle::activating(execution_id), crate::lease::now_unix(), None, None));
        }
        Ok(Ok(before))
    }

    async fn end_activating(
        &self,
        project_id: uuid::Uuid,
        execution_id: uuid::Uuid,
        to: &ActivationLifecycle,
        _remove_signals: bool,
        values: Option<&ValuesStore<'_>>,
    ) -> anyhow::Result<Option<Vec<crate::journal::SignalRegistration>>> {
        if values.is_some() {
            anyhow::bail!("the fake activation store cannot store a re-arm's change; that takes Postgres");
        }
        let mut rows = self.rows.write().await;
        let mut ended = false;
        rows.retain(|(p, key), (lifecycle, _, program, version)| {
            if *p != project_id || lifecycle.status != ProjectStatus::Activating || lifecycle.activating_execution_id != Some(execution_id) {
                return true;
            }
            ended = true;
            if wipe_forgets(key, to) {
                return false;
            }
            *lifecycle = to.clone();
            if to.status != ProjectStatus::Active {
                *program = None;
                *version = None;
            }
            true
        });
        Ok(ended.then(Vec::new))
    }

    async fn restore(
        &self,
        project_id: uuid::Uuid,
        execution_id: uuid::Uuid,
        previous: &[Activation],
    ) -> anyhow::Result<bool> {
        let mut restored = false;
        let mut rows = self.rows.write().await;
        rows.retain(|(p, key), (lifecycle, _, program, version)| {
            if *p != project_id || lifecycle.status != ProjectStatus::Activating || lifecycle.activating_execution_id != Some(execution_id) {
                return true;
            }
            restored = true;
            match previous.iter().find(|a| &a.key == key) {
                Some(before) => {
                    *lifecycle = before.lifecycle.clone();
                    *program = before.program.clone();
                    *version = before.source_version.clone();
                    true
                }
                None => false,
            }
        });
        Ok(restored)
    }

    async fn record_activation_source(
        &self,
        project_id: uuid::Uuid,
        execution_id: uuid::Uuid,
        program: &ProgramIdentity,
        source_version: &str,
    ) -> anyhow::Result<bool> {
        let mut recorded = false;
        for ((p, _), (lifecycle, _, stored, version)) in self.rows.write().await.iter_mut() {
            if *p == project_id && lifecycle.status == ProjectStatus::Activating && lifecycle.activating_execution_id == Some(execution_id) {
                *stored = Some(program.clone());
                *version = Some(source_version.to_string());
                recorded = true;
            }
        }
        Ok(recorded)
    }

    async fn set_lifecycle_guarded(
        &self,
        project_id: uuid::Uuid,
        keys: &[ActivationKey],
        lifecycle: &ActivationLifecycle,
        _signals: SignalsGoing,
    ) -> anyhow::Result<LifecycleWrite> {
        if self.building.read().await.contains(&project_id) {
            return Ok(LifecycleWrite::Rejected { blocker: "building".into() });
        }
        let mut rows = self.rows.write().await;
        if let Some(key) = keys.iter().find(|k| {
            rows.get(&(project_id, (*k).clone())).is_some_and(|(l, ..)| l.status == ProjectStatus::Activating)
        }) {
            return Ok(LifecycleWrite::Rejected { blocker: format!("activating (trigger {key})") });
        }
        for key in keys {
            if wipe_forgets(key, lifecycle) {
                rows.remove(&(project_id, key.clone()));
                continue;
            }
            if let Some((stored, _, program, version)) = rows.get_mut(&(project_id, key.clone())) {
                *stored = ActivationLifecycle { activating_execution_id: None, ..lifecycle.clone() };
                if matches!(lifecycle.status, ProjectStatus::Inactive | ProjectStatus::Deactivating) {
                    *program = None;
                    *version = None;
                }
            }
        }
        // The signals live in the journal's tables, which this fake does
        // not hold (the same as `end_activating`'s).
        Ok(LifecycleWrite::Applied { unlisten: Vec::new() })
    }

    async fn cas_status(
        &self,
        project_id: uuid::Uuid,
        key: &ActivationKey,
        from: ProjectStatus,
        to: ProjectStatus,
    ) -> anyhow::Result<bool> {
        let mut rows = self.rows.write().await;
        match rows.get_mut(&(project_id, key.clone())) {
            Some((lifecycle, ..)) if lifecycle.status == from => {
                if wipe_forgets(key, &ActivationLifecycle { status: to, ..lifecycle.clone() }) {
                    rows.remove(&(project_id, key.clone()));
                    return Ok(true);
                }
                lifecycle.status = to;
                if to != ProjectStatus::Deactivating {
                    lifecycle.drain_deadline_unix = None;
                }
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    async fn bump_heartbeat(&self, execution_id: uuid::Uuid) -> anyhow::Result<()> {
        let now = crate::lease::now_unix();
        for (lifecycle, heartbeat, ..) in self.rows.write().await.values_mut() {
            if lifecycle.activating_execution_id == Some(execution_id) {
                *heartbeat = now;
            }
        }
        Ok(())
    }

    async fn list_stuck(&self, stale_before: i64) -> anyhow::Result<Vec<StuckActivation>> {
        let mut out: Vec<StuckActivation> = Vec::new();
        for ((project_id, _), (lifecycle, heartbeat, ..)) in self.rows.read().await.iter() {
            if let (ProjectStatus::Activating, Some(execution_id)) = (lifecycle.status, lifecycle.activating_execution_id) {
                if *heartbeat < stale_before && !out.iter().any(|s| s.execution_id == execution_id) {
                    out.push(StuckActivation { project_id: *project_id, execution_id });
                }
            }
        }
        Ok(out)
    }

    async fn list_deactivating(&self) -> anyhow::Result<Vec<(uuid::Uuid, ActivationKey)>> {
        Ok(self
            .rows
            .read()
            .await
            .iter()
            .filter(|(_, (l, ..))| l.status == ProjectStatus::Deactivating)
            .map(|((p, k), _)| (*p, k.clone()))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(instance: Option<&str>) -> ActivationKey {
        ActivationKey::new("feed", Owner::from_instance(instance.map(|m| InstanceId::new(m).unwrap())))
    }

    async fn activate(store: &FakeActivationStore, project: uuid::Uuid, key: &ActivationKey) {
        let setup = uuid::Uuid::new_v4();
        assert!(store.try_begin_activating(project, std::slice::from_ref(key), setup, None).await.unwrap().is_ok());
        store.end_activating(project, setup, &ActivationLifecycle::active(), false, None).await.unwrap().expect("owned");
    }

    async fn owners(store: &FakeActivationStore, project: uuid::Uuid) -> Vec<Option<String>> {
        store.list(project).await.unwrap().into_iter().map(|a| a.key.instance().map(|m| m.as_str().to_string())).collect()
    }

    /// An instance's trigger wiped leaves no row, at once or at the drain
    /// landing; the shared one and a parked instance keep theirs.
    #[tokio::test]
    async fn a_wiped_instances_row_is_forgotten() {
        let store = FakeActivationStore::new();
        let project = uuid::Uuid::new_v4();
        let (shared, ada, bob, cyd) = (key(None), key(Some("ada")), key(Some("bob")), key(Some("cyd")));
        for k in [&shared, &ada, &bob, &cyd] {
            activate(&store, project, k).await;
        }
        store.set_lifecycle_guarded(project, &[shared.clone(), ada.clone()], &ActivationLifecycle::wiped(), SignalsGoing::Kept).await.unwrap();
        assert_eq!(owners(&store, project).await, vec![None, Some("bob".into()), Some("cyd".into())]);

        let draining = ActivationLifecycle::deactivating_to(ActivationLifecycle::wiped(), i64::MAX);
        store.set_lifecycle_guarded(project, std::slice::from_ref(&bob), &draining, SignalsGoing::Kept).await.unwrap();
        assert_eq!(owners(&store, project).await.len(), 3, "the row stays while it drains");
        assert!(store.cas_status(project, &bob, ProjectStatus::Deactivating, ProjectStatus::Inactive).await.unwrap());
        assert!(!store.cas_status(project, &bob, ProjectStatus::Deactivating, ProjectStatus::Inactive).await.unwrap());

        store.set_lifecycle_guarded(project, std::slice::from_ref(&cyd), &ActivationLifecycle::parked(), SignalsGoing::Kept).await.unwrap();
        assert_eq!(owners(&store, project).await, vec![None, Some("cyd".into())]);

        let setup = uuid::Uuid::new_v4();
        let previous = store.try_begin_activating(project, std::slice::from_ref(&ada), setup, None).await.unwrap().expect("claimed");
        assert!(previous.is_empty(), "ada activates again as never activated");

        // That activation cancelled (or reaped) mid-way lands wiped, so
        // ada's row goes again; a shared one ending the same way stays.
        store.end_activating(project, setup, &ActivationLifecycle::wiped(), true, None).await.unwrap().expect("owned");
        assert_eq!(owners(&store, project).await, vec![None, Some("cyd".into())]);
        let setup = uuid::Uuid::new_v4();
        store.try_begin_activating(project, std::slice::from_ref(&shared), setup, None).await.unwrap().expect("claimed");
        store.end_activating(project, setup, &ActivationLifecycle::wiped(), true, None).await.unwrap().expect("owned");
        assert_eq!(owners(&store, project).await, vec![None, Some("cyd".into())]);
    }
}
