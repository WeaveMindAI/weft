//! Trigger activations: which triggers of a project are listening, and
//! for whom.
//!
//! The unit of activation is ONE trigger for ONE owner: the program's
//! own shared copy of a trigger, or one instance's copy of a per-instance
//! trigger. Each has its own row in `trigger_activation`, with its own
//! lifecycle (activating, active, inactive, and for an inactive one what
//! its door does: take, hide or refuse), and the cut its fires run as its
//! setup recorded it. So one trigger can be taken down while its siblings
//! keep listening, and one instance's triggers without touching another's.
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
use weft_task_store::db::{self, Client, Pool, Transaction};

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
    /// against the build for whether the trigger is behind.
    pub program: Option<ProgramIdentity>,
    /// The source version its setup ran, for the version tree.
    pub source_version: Option<String>,
    /// The cut its fires run, as its last setup recorded it: kept while it
    /// is down, since what it depends on is what brings it back and what
    /// an infra command takes down with it. `None` until a setup of it
    /// finished.
    pub cut: Option<RecordedCut>,
    /// The task that claimed its last activation (`driver_task`): the one
    /// driving it while it is activating, the one that turned it on when
    /// it is active since. `None` before any task claimed it.
    pub driver_task: Option<uuid::Uuid>,
    /// Where it stands while it is activating; `None` otherwise.
    pub activating: Option<Activating>,
}

/// A trigger's cut as its setup recorded it (`weft_core::run_spec::TriggerCut`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordedCut {
    pub fingerprint: String,
    /// The infra places it depends on, sorted.
    pub depends_on: Vec<String>,
}

impl From<weft_core::run_spec::TriggerCut> for RecordedCut {
    fn from(cut: weft_core::run_spec::TriggerCut) -> Self {
        Self { fingerprint: cut.fingerprint, depends_on: cut.depends_on }
    }
}

/// Outcome of a guarded lifecycle write. `Rejected` names the state
/// that blocked it so the caller can say which.
/// `Applied` hands back the signals the write deleted in its own
/// transaction ([`SignalsGoing`]), for the listener cleanup, and the work
/// held on the written activations as it stood just before the write.
/// `ResyncGone`: the write was a resync's take-down, and that drive no
/// longer holds the project.
/// `NotFound`: the project is gone, or, for an infra cause, `weft rm` is
/// removing it.
/// `UpgradeInFlight`: the write was an upgrade's take-down, and an upgrade
/// of the same copies is still in flight (its command): nothing was
/// written.
/// `StateMoved`: one of the [`SignalsGoing::Places`] rows moved past the
/// version its fresh state was computed from (a wake claimed in between):
/// nothing was written, and the caller computes them again.
#[derive(Debug, Clone)]
pub enum LifecycleWrite {
    Applied {
        /// The activations it took down: every one asked for that has a
        /// row, or for a cause that brings them back
        /// ([`DownCause::Health`], [`DownCause::Infra`]) the ones still on.
        keys: Vec<ActivationKey>,
        /// Their entry tokens as they stood before the write (it may have
        /// deleted the signals): what a wait for their runs counts at the
        /// door (`crate::drain`).
        tokens: Vec<String>,
        unlisten: Vec<crate::store::SignalRegistration>,
        held: weft_core::projects::HeldWork,
        /// The commands it issued ([`DownCause::Infra`]), first issued first.
        commands: Vec<i64>,
    },
    Rejected { blocker: String },
    NotFound,
    ResyncGone,
    UpgradeInFlight(i64),
    StateMoved,
}

/// What takes triggers down, when it is not a person: the mark a guarded
/// write leaves on their rows (`went_down_with`), which is what brings
/// them back once what took them down is over.
#[derive(Debug, Clone, Copy)]
pub enum DownCause<'a> {
    /// A person, a program or a resync: they stay down until switched on.
    Person,
    /// The health loop, over the copies it found broken (never empty):
    /// its auto-recover brings them back.
    Health(&'a [weft_broker_client::protocol::InfraCopy]),
    /// An infra verb over copies their cut reads: its commands are issued
    /// in the same write, and the copies' return brings them back (when
    /// they can return, `TakeDownCommands::copies_can_return`).
    Infra(&'a crate::infra_lifecycle_command::TakeDownCommands),
    /// `weft maintenance on` (`crate::maintenance`): the maintenance lift
    /// brings them back.
    Maintenance,
}

/// Who claims an activation ([`ActivationStoreOps::try_begin_activating`]),
/// for what the claim refuses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Claimant {
    /// A person, a program, or weft bringing triggers back on its own (an
    /// infra's return, the health check, a re-arm).
    Anyone,
    /// A resync bringing back the triggers it took down, under its drive
    /// (`crate::task_kinds::resync::ResyncPlan::driver`): the one claim
    /// that goes through while a resync holds the project.
    Resync(uuid::Uuid),
    /// The maintenance lift bringing back the triggers `weft maintenance
    /// on` took down (`crate::maintenance`): the one claim that goes
    /// through while the install is in maintenance.
    Lift,
}

impl Claimant {
    /// The resync drive that claims, if one does.
    pub fn resync(self) -> Option<uuid::Uuid> {
        match self {
            Claimant::Resync(driver) => Some(driver),
            Claimant::Anyone | Claimant::Lift => None,
        }
    }
}

/// What a guarded lifecycle write deletes in its own transaction, so the
/// rows and what they govern leave together: a retry after a failure later
/// on never finds a row gone and its signals still registered.
#[derive(Debug, Clone, Copy)]
pub enum SignalsGoing<'a> {
    /// Nothing. A door that holds goes on listening, and what it hears
    /// waits for the trigger to be back on (`weft_core::arrival`); a door
    /// that refuses stops listening while its rows stay for its next
    /// activation (`weft_broker_client::protocol::ACTIVATION_LISTENS`),
    /// each place in a source among them taken ([`Self::Places`]), except
    /// under weft's own maintenance: nobody refused anything, so what came
    /// meanwhile is caught up.
    Kept,
    /// Nothing, and the written activations' entry signals listed lose
    /// their place in what they read (a poll's cursor, what a folder
    /// held): a door that refuses with what is held kept stops listening
    /// while its rows stay, and they carry nothing of what came behind it
    /// on their next activation, or whatever else brings them up. Each is
    /// written only while its row is still at the version its fresh state
    /// was computed from ([`crate::store::FreshPlace`]); one that moved
    /// fails the whole write ([`LifecycleWrite::StateMoved`]).
    Places(&'a [crate::store::FreshPlace]),
    /// The arrivals held on the written activations' entry signals
    /// (`parked_fire`): a take-down that drops what is held while its door
    /// goes on taking. The paused runs the take-down cancels drop their
    /// own waits, and the answers queued on them.
    HeldArrivals,
    /// Every signal of the project, except the ones `except`'s run
    /// holds (the run that asked for the take-down).
    Project { except: Option<uuid::Uuid> },
    /// The entry signals of the written activations, with the arrivals
    /// held on them: a door that refuses with nothing held. The waits of
    /// the runs their triggers fired stay with those runs: a paused one's
    /// go as the take-down cancels it, and one still going keeps its own.
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
    /// The `(node, field)` pairs the change forgets.
    pub cleared: &'a [(String, String)],
}

impl ValuesStore<'_> {
    /// Store the change on `tx`.
    pub async fn store(&self, tx: &impl Client, project_id: uuid::Uuid) -> anyhow::Result<()> {
        weft_access_store::change_instance_values(tx, self.tenant, project_id, self.instance, self.writes, self.cleared).await
    }
}

/// The SQL condition "the task driving the activating row `a` is still to
/// finish": waiting to be claimed, or claimed (renewed, or resting on the
/// activation's setup run while it waits days on a person). While it is,
/// the activation is alive whatever became of the process that claimed
/// it: a claimant that died has its claim rescued by another dispatcher,
/// whose task carries the activation on from its rows
/// (`crate::api::project::activate_with`).
pub(crate) const DRIVER_TASK_LIVE: &str =
    "EXISTS (SELECT 1 FROM task driver WHERE driver.id = a.driver_task AND driver.status IN ('pending', 'claimed'))";

/// THE activations nobody will finish, as a SQL condition over a
/// `trigger_activation` row aliased `a`: activating, with no task driving
/// it any more ([`DRIVER_TASK_LIVE`]): its task ended without landing it
/// (a rollback that failed, a task run again that no longer asks for it),
/// or the row was claimed by a weft from before activations were tasks.
/// The `lapses` loop ends them ([`ActivationStoreOps::list_orphaned`]).
pub(crate) fn orphaned() -> String {
    format!("(a.status = 'activating' AND a.activating_run_id IS NOT NULL AND NOT {DRIVER_TASK_LIVE})")
}

/// An activation nobody will finish ([`orphaned`]): the `lapses` loop ends
/// it.
#[derive(Debug, Clone)]
pub struct OrphanedActivation {
    pub project_id: uuid::Uuid,
    pub run_id: uuid::Uuid,
}

/// The step an activation's driver is at, as its rows keep it
/// (`trigger_activation.activating_step`): what `weft status` says a
/// trigger turning on waits on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActivationStep {
    /// Checking the infra its triggers read, readying the registered
    /// build's workers, dropping what they held when asked to.
    Preparing,
    /// Its setup run goes: queued for a worker, or running (which may
    /// wait on a person).
    SettingUp,
    /// Arming the signals its setup captured, and landing them on.
    Arming,
}

impl ActivationStep {
    // SYNC: the spellings <-> trigger_activation.activating_step (GROUP)
    pub fn as_str(self) -> &'static str {
        match self {
            ActivationStep::Preparing => "preparing",
            ActivationStep::SettingUp => "setting_up",
            ActivationStep::Arming => "arming",
        }
    }

    pub fn parse(stored: &str) -> Option<Self> {
        [ActivationStep::Preparing, ActivationStep::SettingUp, ActivationStep::Arming].into_iter().find(|step| step.as_str() == stored)
    }

    /// What a person reads it waits on, `setup_run` being the activation's
    /// setup run.
    pub fn waiting(self, setup_run: weft_core::RunId) -> String {
        match self {
            ActivationStep::Preparing => "getting ready (the infra its triggers read, the build's workers)".into(),
            ActivationStep::SettingUp => format!("its setup run {setup_run}"),
            ActivationStep::Arming => "arming the signals its setup captured".into(),
        }
    }
}

/// Where one activating row stands: since when it is claimed, the step
/// its driver is at, and whether the trigger listened the whole time it
/// was down as the claim found it (what its signal carries over when it is
/// armed: `crate::task_kinds::register_signal::listened_while_down`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Activating {
    pub since_unix: i64,
    pub step: ActivationStep,
    pub listened: bool,
}

/// Backing store for activations. Implementations:
/// - `PostgresActivationStore` (production)
/// - `FakeActivationStore` (tests, behind `test-helpers`).
#[async_trait]
pub trait ActivationStoreOps: Send + Sync {
    /// Every activation row of a project.
    async fn list(&self, project_id: uuid::Uuid) -> anyhow::Result<Vec<Activation>>;

    /// Single-flight entry into Activating for ALL of `keys` at once,
    /// under one setup `run_id`. Refused ([`ClaimRefused`], decided in the
    /// claim's own transaction) when any of them is already activating,
    /// or while the project is building (the build transition lives on
    /// the project row). Every other status is a
    /// legal entry: never activated, inactive, active (a re-arm). Rows are created on
    /// first activation. While the install is in maintenance, only the
    /// lift's claim goes through ([`Claimant::Lift`]), decided under the
    /// project row's lock, which `weft maintenance on` waits on before it
    /// reads what is on (`crate::maintenance`), so no claim slips between.
    /// Stamps the claim with `task`, the task that drives it
    /// ([`Activation::driver_task`]), its time and its first step, and
    /// each row with whether its trigger listened while it was down, as
    /// the claim found it ([`Activating::listened`]). Answers the rows among
    /// `keys` as they were just before the claim (none for a key never
    /// activated), read in the same transaction: what [`Self::restore`]
    /// puts back when the activation fails before it touched anything.
    /// With `expect`, the claim is also refused unless every one of
    /// `keys` exists and is still in that status: a re-arm that saw its
    /// triggers Active must not turn back on one a deactivate took down
    /// since. While a resync holds the project, the one claim that goes
    /// through is the resync's own (`resync`, the drive bringing its
    /// triggers back: `crate::task_kinds::resync`), and only while that
    /// drive holds it: no other activation starts under a resync, and a
    /// resync given up turns nothing on.
    async fn try_begin_activating(
        &self,
        project_id: uuid::Uuid,
        keys: &[ActivationKey],
        run_id: uuid::Uuid,
        expect: Option<ProjectStatus>,
        claimant: Claimant,
        task: uuid::Uuid,
    ) -> anyhow::Result<Result<Vec<Activation>, ClaimRefused>>;

    /// Say the activation `run_id` names is at `step` now, on every row it
    /// claimed. `false` when it no longer owns any.
    async fn set_step(&self, project_id: uuid::Uuid, run_id: uuid::Uuid, step: ActivationStep) -> anyhow::Result<bool>;

    /// End the activation `run_id` names (every row it claimed) at `to`.
    /// With `remove_entries`, those activations' entry rows are deleted in
    /// the same transaction and handed back for the listener cleanup, and
    /// an instance's rows go too ([`Forget`]): a trigger that refuses with
    /// nothing kept is the same as one never activated. The waits of runs
    /// their triggers fired stay, since nothing here ends those runs. With
    /// `values`, that change is stored in it too. `None` (and nothing
    /// stored) if `run_id` no longer owns any row.
    async fn end_activating(
        &self,
        project_id: uuid::Uuid,
        run_id: uuid::Uuid,
        to: &ActivationLifecycle,
        remove_entries: bool,
        values: Option<&ValuesStore<'_>>,
    ) -> anyhow::Result<Option<Vec<crate::store::SignalRegistration>>>;

    /// End the activation `run_id` names that nobody will finish (a
    /// person cancelled it, or the task driving it ended without landing
    /// it). When its triggers kept work
    /// (events held on them, runs of theirs paused:
    /// `crate::store::postgres::held_on`), they land down with their door
    /// taking and every signal kept, so that work waits for their next
    /// activation; otherwise their door refuses and their entry rows go.
    /// Decided and written in one transaction under the activation's rows.
    /// `None` when `run_id` no longer owns any row.
    async fn end_unfinished(
        &self,
        project_id: uuid::Uuid,
        run_id: uuid::Uuid,
    ) -> anyhow::Result<Option<Vec<crate::store::SignalRegistration>>>;

    /// Put the activations `run_id` claimed back as they were before the
    /// claim (`previous`, what [`Self::try_begin_activating`] answered):
    /// an activation that failed before touching any signal leaves each
    /// trigger as it found it (a parked one parked, a live one listening
    /// on the code it fired before), and the row of one never activated
    /// before goes again, so it reads as never activated. Guarded by the
    /// claim, like [`Self::end_activating`]; `false` when `run_id` no
    /// longer owns them.
    async fn restore(
        &self,
        project_id: uuid::Uuid,
        run_id: uuid::Uuid,
        previous: &[Activation],
    ) -> anyhow::Result<bool>;

    /// Record the code an activation's listeners now fire, and each of its
    /// triggers' cut (`cuts`, by trigger). Guarded by the activation's own
    /// run, so a cancelled driver cannot overwrite a newer activation's
    /// record.
    async fn record_activation_source(
        &self,
        project_id: uuid::Uuid,
        run_id: uuid::Uuid,
        program: &ProgramIdentity,
        source_version: &str,
        cuts: &std::collections::BTreeMap<String, RecordedCut>,
    ) -> anyhow::Result<bool>;

    /// Set the lifecycle of every row among `keys`, GUARDED: refused while
    /// any of them is activating (cancel the activation first) or while
    /// the project is building. Keys with no row are skipped: there is
    /// nothing listening to take down. `signals` names what is deleted (or
    /// which places go) in the same transaction, and `forget` the instances' rows that go with
    /// it. With `resync`, the write is that resync drive's take-down
    /// (`crate::task_kinds::resync`), and goes through only while that
    /// drive holds the project, decided under the same project row lock a
    /// person's give-up takes: a resync given up takes nothing down after
    /// it. `cause` is the mark the rows get, and an infra cause's commands
    /// are issued in the same transaction.
    #[allow(clippy::too_many_arguments)]
    async fn set_lifecycle_guarded(
        &self,
        project_id: uuid::Uuid,
        keys: &[ActivationKey],
        lifecycle: &ActivationLifecycle,
        signals: SignalsGoing<'_>,
        forget: Forget,
        cause: DownCause<'_>,
        resync: Option<uuid::Uuid>,
    ) -> anyhow::Result<LifecycleWrite>;

    /// Activations nobody will finish: activating, with no task driving
    /// them any more ([`orphaned`]).
    async fn list_orphaned(&self) -> anyhow::Result<Vec<OrphanedActivation>>;
}

/// Whether the instances' rows a take-down writes go: a trigger that
/// refuses what arrives with nothing kept (`door: refuse`, `held: drop`)
/// is the same as one never activated, so an instance's row goes, and
/// nothing lists the instance for it any more. The shared one keeps its
/// row, which is what the project's status reads. One taken down with its
/// infra, or by the maintenance, keeps its row, which is what brings it
/// back once the infra runs again or the maintenance is lifted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Forget {
    InstanceRows,
    Nothing,
}

impl Forget {
    /// What a take-down forgets.
    pub fn of(take_down: &weft_core::take_down::TakeDown, cause: &DownCause<'_>) -> Self {
        use weft_core::take_down::{Door, Held};
        // One its infra's return or the lift brings back keeps its row:
        // the row is what brings it back.
        let comes_back = matches!(cause, DownCause::Infra(commands) if commands.copies_can_return()) || matches!(cause, DownCause::Maintenance);
        match (take_down.door, take_down.held, comes_back) {
            (Door::Refuse, Held::Drop, false) => Forget::InstanceRows,
            _ => Forget::Nothing,
        }
    }

    /// Whether `key`'s row goes.
    fn forgets(self, key: &ActivationKey) -> bool {
        self == Forget::InstanceRows && key.instance().is_some()
    }
}

/// Whether a `trigger_activation` row (`a`) is down with a door that holds
/// what arrives for a while: taking work until a deadline.
// SYNC: DOOR_WINDOW_OPEN_SQL <-> weft_core::arrival::Standing::arrival, weft_broker_client::protocol::ACTIVATION_LISTENS
const DOOR_WINDOW_OPEN_SQL: &str = "a.accepting_fires AND a.fires_deadline_unix IS NOT NULL AND a.status = 'inactive'";

/// The doors whose window is over and still take work, with their
/// project. Over by the database's clock, the one the listening condition
/// (`ACTIVATION_LISTENS`) reads.
pub async fn door_windows_over(pool: &Pool) -> anyhow::Result<Vec<(uuid::Uuid, ActivationKey)>> {
    let rows = pool
        .all(
            &format!(
                "SELECT a.project_id, a.trigger, a.instance_id FROM trigger_activation a \
                 WHERE {DOOR_WINDOW_OPEN_SQL} AND a.fires_deadline_unix < EXTRACT(EPOCH FROM NOW())::BIGINT"
            ),
            &[],
        )
        .await?;
    project_keys(rows)
}

/// What [`close_door_windows`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowsClosed {
    /// This many doors closed, their places taken.
    Closed(u64),
    /// A row among the places moved past the version its fresh state was
    /// computed from (a wake claimed in between): nothing was written, and
    /// the caller computes them again.
    StateMoved,
}

/// Close the doors of `keys` of `project_id` whose window is over: they
/// stop taking work, and their entry signals lose their place in what
/// they read, in the same transaction: `places` are their fresh states
/// (`crate::store::FreshPlace`), each written only while its row is still
/// at the version it was computed from. The arrival rule and the
/// listening condition already read the deadline against the clock; the
/// flip is what is announced (holder sizing, the routes). One activated
/// again meanwhile no longer matches and is left alone, places included.
pub async fn close_door_windows(
    pool: &Pool,
    project_id: uuid::Uuid,
    keys: &[ActivationKey],
    places: &[crate::store::FreshPlace],
) -> anyhow::Result<WindowsClosed> {
    let (triggers, instances) = key_arrays(keys);
    let mut conn = pool.get().await?;
    let tx = conn.transaction().await?;
    let closed: Vec<(String, Option<String>)> = tx
        .all(
            &format!(
                "UPDATE trigger_activation a SET accepting_fires = FALSE, updated_at = EXTRACT(EPOCH FROM NOW())::BIGINT \
                 WHERE a.project_id = $1 AND {KEYS_JOIN} \
                   AND {DOOR_WINDOW_OPEN_SQL} AND a.fires_deadline_unix < EXTRACT(EPOCH FROM NOW())::BIGINT \
                 RETURNING a.trigger, a.instance_id"
            ),
            &[&project_id, &triggers, &instances],
        )
        .await?;
    let closed = owned_keys(closed)?;
    let tokens = crate::store::postgres::entry_tokens(&tx, project_id, &closed).await?;
    let taken: Vec<crate::store::FreshPlace> = places.iter().filter(|place| tokens.contains(&place.token)).cloned().collect();
    if !crate::store::postgres::write_fresh_places(&tx, &taken).await? {
        tx.rollback().await?;
        return Ok(WindowsClosed::StateMoved);
    }
    tx.commit().await?;
    Ok(WindowsClosed::Closed(closed.len() as u64))
}

/// When the earliest door window still open ends, if one is.
pub async fn next_door_window_end(pool: &Pool) -> anyhow::Result<Option<i64>> {
    Ok(pool
        .scalar::<Option<i64>>(&format!("SELECT MIN(a.fires_deadline_unix) FROM trigger_activation a WHERE {DOOR_WINDOW_OPEN_SQL}"), &[])
        .await?)
}

pub type ActivationStore = Arc<dyn ActivationStoreOps>;

/// Activation keys as `(trigger, instance)` rows give them.
fn owned_keys(rows: Vec<(String, Option<String>)>) -> anyhow::Result<Vec<ActivationKey>> {
    rows.into_iter()
        .map(|(trigger, instance)| {
            Ok(ActivationKey::new(trigger, Owner::from_instance(instance.map(InstanceId::new).transpose().map_err(anyhow::Error::msg)?)))
        })
        .collect()
}

/// [`ActivationStoreOps::end_activating`] on the caller's transaction.
async fn end_activating_in(
    tx: &Transaction<'_>,
    project_id: uuid::Uuid,
    run_id: uuid::Uuid,
    to: &ActivationLifecycle,
    remove_entries: bool,
    values: Option<&ValuesStore<'_>>,
) -> anyhow::Result<Option<Vec<crate::store::SignalRegistration>>> {
    let owned = "project_id = $1 AND activating_run_id = $2 AND status = 'activating'";
    // With their entry rows going, an instance's rows go too ([`Forget`]);
    // the rest move to `to`.
    let mut ended: Vec<(String, Option<String>)> = if remove_entries {
        tx.all(
            &format!("DELETE FROM trigger_activation WHERE {owned} AND instance_id IS NOT NULL RETURNING trigger, instance_id"),
            &[&project_id, &run_id],
        )
        .await?
    } else {
        Vec::new()
    };
    // What took it down stays unless it lands on or `to` names another
    // cause: an activation that ends down (cancelled, failed, its driver
    // dead) keeps the trigger marked for what brings it back.
    let moved: Vec<(String, Option<String>)> = tx
        .all(
            "UPDATE trigger_activation \
             SET status = $1::text, accepting_fires = $2, fires_visible_to_consumers = $3, fires_deadline_unix = $4, \
                 went_down_with = CASE WHEN $5::TEXT IS NOT NULL OR $1::text = 'active' THEN $5::TEXT ELSE went_down_with END, \
                 went_down_by_command = CASE WHEN $5::TEXT IS NOT NULL OR $1::text = 'active' THEN $9::bigint ELSE went_down_by_command END, \
                 activating_run_id = NULL, activating_since_unix = NULL, activating_step = NULL, activating_listened = FALSE, \
                 activation_program = CASE WHEN $1::text = 'active' THEN activation_program ELSE NULL END, \
                 activation_version = CASE WHEN $1::text = 'active' THEN activation_version ELSE NULL END, \
                 updated_at = $6 \
             WHERE project_id = $7 AND activating_run_id = $8 AND status = 'activating' \
             RETURNING trigger, instance_id",
            &[
                &to.status.as_str(),
                &to.accepting_fires,
                &to.fires_visible_to_consumers,
                &to.fires_deadline_unix,
                &to.went_down.map(|w| w.with().as_str()),
                &crate::lease::now_unix(),
                &project_id,
                &run_id,
                &to.went_down.and_then(|w| w.first_command()),
            ],
        )
        .await?;
    ended.extend(moved);
    if ended.is_empty() {
        return Ok(None);
    }
    if let Some(values) = values {
        values.store(tx, project_id).await?;
    }
    let removed = match remove_entries {
        true => {
            let keys = owned_keys(ended)?;
            crate::store::postgres::remove_entry_signals(tx, project_id, &keys).await?
        }
        false => Vec::new(),
    };
    Ok(Some(removed))
}

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
            -- registered | activating | active | inactive
            status TEXT NOT NULL,
            -- An inactive one's door (`weft_core::take_down::Door`): take =
            -- accepting and visible, hide = accepting and hidden, refuse =
            -- neither; a door's minutes are the deadline.
            accepting_fires BOOLEAN NOT NULL,
            fires_visible_to_consumers BOOLEAN NOT NULL,
            fires_deadline_unix BIGINT,
            -- Why it is down when something other than a person took it
            -- down: 'health' (the health loop took it down), 'infra' (an
            -- infra verb took it down with its copies) or 'maintenance'
            -- (`weft maintenance on` took it down). NULL otherwise. What
            -- took it down coming back brings it back.
            -- SYNC: the spellings <-> weft_core::projects::DownWith
            went_down_with TEXT,
            -- For 'infra', the first command the take-down issued, in the
            -- same transaction: what ended or came back after it is what
            -- brings it back. NULL otherwise.
            went_down_by_command BIGINT,
            -- The trigger-setup run of the activation in flight; one run
            -- sets up every activation a verb names, so rows share it.
            activating_run_id UUID,
            -- The task that claimed its last activation: it drives the
            -- activation while the row is activating, so its claim is
            -- what keeps the activation alive and a task run again picks
            -- it up where it was, and it turned the trigger on when the
            -- row is active since. NULL before any task claimed it.
            driver_task UUID,
            -- While activating: when the claim was taken, the step its
            -- driver is at ('preparing', 'setting_up', 'arming'), and
            -- whether the trigger listened the whole time it was down,
            -- read off the row as the claim found it (what its signal
            -- carries over when it is armed again).
            activating_since_unix BIGINT,
            activating_step TEXT,
            activating_listened BOOLEAN NOT NULL DEFAULT FALSE,
            -- What the listeners fire, recorded when setup finished.
            activation_program JSONB,
            -- The version `activation_program` was written at
            -- (`ProgramIdentity::STORED`).
            activation_program_version INTEGER NOT NULL,
            activation_version TEXT,
            -- The cut its fires run, recorded when its setup finished
            -- (`weft_core::run_spec::TriggerCut`), and kept while it is
            -- down: its fingerprint, which tells whether it is behind the
            -- source, and the infra places it depends on, which an infra
            -- command reads to know what goes down with it.
            cut_fingerprint TEXT,
            depends_on TEXT[] NOT NULL DEFAULT '{}',
            updated_at BIGINT NOT NULL
        )"#,
        r#"CREATE UNIQUE INDEX IF NOT EXISTS trigger_activation_key
             ON trigger_activation (project_id, trigger, instance_id) NULLS NOT DISTINCT"#,
        r#"CREATE INDEX IF NOT EXISTS trigger_activation_run_id
             ON trigger_activation (activating_run_id) WHERE activating_run_id IS NOT NULL"#,
        r#"CREATE INDEX IF NOT EXISTS trigger_activation_activating
             ON trigger_activation (status) WHERE status = 'activating'"#,
        // An activation changing status, whether it takes work or until
        // when, coming or going can start or stop the holding of its held
        // signals without any signal row changing (a trigger woken, a
        // door's window set or ending, an instance's row forgotten, which a
        // signal with no activation reads as active), so each wakes the
        // holder sizing too, and the door-window sweep (`crate::reaper`),
        // which then knows when the next window ends. The
        // function is the run group's (`crate::store::postgres::GROUP`),
        // which applies first.
        r#"DROP TRIGGER IF EXISTS trigger_activation_held_on_status ON trigger_activation"#,
        r#"CREATE TRIGGER trigger_activation_held_on_status
            AFTER UPDATE OF status, accepting_fires, fires_deadline_unix ON trigger_activation
            FOR EACH ROW
            WHEN (NEW.status IS DISTINCT FROM OLD.status
                  OR NEW.accepting_fires IS DISTINCT FROM OLD.accepting_fires
                  OR NEW.fires_deadline_unix IS DISTINCT FROM OLD.fires_deadline_unix)
            EXECUTE FUNCTION signal_held_notify()"#,
        // An activation's status, whether it takes work while off and until
        // when, are part of how its routes read (an active one serves a
        // caller, one down with a door that holds holds them, until its
        // door's window ends:
        // `weft_core::arrival`), so a change tells every dispatcher to read its
        // tenant's routes again (`crate::held::Held::routes`), and wakes the
        // callers held there.
        // SYNC: 'weft_routes' <-> crate::held::ROUTES_CHANNEL
        r#"CREATE OR REPLACE FUNCTION trigger_activation_routes_notify() RETURNS trigger AS $$
            DECLARE
                tenant TEXT;
            BEGIN
                SELECT p.tenant_id INTO tenant FROM project p
                    WHERE p.id = CASE WHEN TG_OP = 'DELETE' THEN OLD.project_id ELSE NEW.project_id END;
                -- A project already gone told its tenant itself.
                IF tenant IS NOT NULL THEN
                    PERFORM pg_notify('weft_routes', tenant);
                END IF;
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql"#,
        r#"DROP TRIGGER IF EXISTS trigger_activation_routes_on_status ON trigger_activation"#,
        r#"CREATE TRIGGER trigger_activation_routes_on_status
            AFTER UPDATE OF status, accepting_fires, fires_deadline_unix ON trigger_activation
            FOR EACH ROW
            WHEN (NEW.status IS DISTINCT FROM OLD.status
                  OR NEW.accepting_fires IS DISTINCT FROM OLD.accepting_fires
                  OR NEW.fires_deadline_unix IS DISTINCT FROM OLD.fires_deadline_unix)
            EXECUTE FUNCTION trigger_activation_routes_notify()"#,
        r#"DROP TRIGGER IF EXISTS trigger_activation_routes_on_row ON trigger_activation"#,
        r#"CREATE TRIGGER trigger_activation_routes_on_row
            AFTER INSERT OR DELETE ON trigger_activation
            FOR EACH ROW
            EXECUTE FUNCTION trigger_activation_routes_notify()"#,
        // The same changes tell the project's workers, whose copy of its
        // triggers (`weft_engine::door`) carries how each one stands.
        // SYNC: 'weft_triggers' <-> weft_broker_client::line::TRIGGERS_CHANNEL
        r#"CREATE OR REPLACE FUNCTION trigger_activation_triggers_notify() RETURNS trigger AS $$
            BEGIN
                IF TG_OP = 'DELETE' THEN
                    PERFORM pg_notify('weft_triggers', OLD.project_id::text);
                ELSE
                    PERFORM pg_notify('weft_triggers', NEW.project_id::text);
                END IF;
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql"#,
        r#"DROP TRIGGER IF EXISTS trigger_activation_triggers_on_status ON trigger_activation"#,
        r#"CREATE TRIGGER trigger_activation_triggers_on_status
            AFTER UPDATE OF status, accepting_fires, fires_deadline_unix ON trigger_activation
            FOR EACH ROW
            WHEN (NEW.status IS DISTINCT FROM OLD.status
                  OR NEW.accepting_fires IS DISTINCT FROM OLD.accepting_fires
                  OR NEW.fires_deadline_unix IS DISTINCT FROM OLD.fires_deadline_unix)
            EXECUTE FUNCTION trigger_activation_triggers_notify()"#,
        r#"DROP TRIGGER IF EXISTS trigger_activation_triggers_on_row ON trigger_activation"#,
        r#"CREATE TRIGGER trigger_activation_triggers_on_row
            AFTER INSERT OR DELETE ON trigger_activation
            FOR EACH ROW
            EXECUTE FUNCTION trigger_activation_triggers_notify()"#,
        r#"DROP TRIGGER IF EXISTS trigger_activation_held_on_row ON trigger_activation"#,
        r#"CREATE TRIGGER trigger_activation_held_on_row
            AFTER INSERT OR DELETE ON trigger_activation
            FOR EACH ROW
            EXECUTE FUNCTION signal_held_notify()"#,
    ],
    seed: &[],
    data_steps: &[],
};

#[derive(Clone)]
pub struct PostgresActivationStore {
    pool: Pool,
}

impl PostgresActivationStore {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }
}

/// One `trigger_activation` row as [`ROW_COLUMNS`] reads it, by column.
struct LifecycleRow(db::Row);

impl db::FromRow for LifecycleRow {
    fn from_row(row: db::Row) -> db::Result<Self> {
        Ok(LifecycleRow(row))
    }
}

/// The columns a [`LifecycleRow`] reads, of a row aliased `a`.
const ROW_COLUMNS: &str = "a.trigger, a.instance_id, a.status, a.accepting_fires, a.fires_visible_to_consumers, \
     a.fires_deadline_unix, a.went_down_with, a.went_down_by_command, a.activating_run_id, \
     a.activation_program, a.activation_program_version, a.activation_version, a.cut_fingerprint, a.depends_on, \
     a.driver_task, a.activating_since_unix, a.activating_step, a.activating_listened";

fn row_to_activation(LifecycleRow(row): LifecycleRow) -> anyhow::Result<Activation> {
    let trigger: String = row.try_get("trigger")?;
    let instance: Option<String> = row.try_get("instance_id")?;
    let status: String = row.try_get("status")?;
    let accepting: bool = row.try_get("accepting_fires")?;
    let visible: bool = row.try_get("fires_visible_to_consumers")?;
    let deadline: Option<i64> = row.try_get("fires_deadline_unix")?;
    let went_down_with: Option<String> = row.try_get("went_down_with")?;
    let went_down_by_command: Option<i64> = row.try_get("went_down_by_command")?;
    let run_id: Option<uuid::Uuid> = row.try_get("activating_run_id")?;
    let program: Option<db::Json<serde_json::Value>> = row.try_get("activation_program")?;
    let program_version: weft_core::stored::Version = row.try_get("activation_program_version")?;
    let version: Option<String> = row.try_get("activation_version")?;
    let fingerprint: Option<String> = row.try_get("cut_fingerprint")?;
    let depends_on: Vec<String> = row.try_get("depends_on")?;
    let driver_task: Option<uuid::Uuid> = row.try_get("driver_task")?;
    let activating_since: Option<i64> = row.try_get("activating_since_unix")?;
    let activating_step: Option<String> = row.try_get("activating_step")?;
    let activating_listened: bool = row.try_get("activating_listened")?;
    let instance = instance.map(InstanceId::new).transpose().map_err(|e| anyhow::anyhow!("trigger_activation.instance_id: {e}"))?;
    let program = program
        .map(|program| ProgramIdentity::STORED.decode(program.0, program_version))
        .transpose()
        .map_err(|e| e.at(format_args!("the activation of {trigger}")))?;
    let status = ProjectStatus::parse(&status).ok_or_else(|| anyhow::anyhow!("unknown trigger_activation.status '{status}'"))?;
    let activating = match (status, activating_since, activating_step) {
        (ProjectStatus::Activating, Some(since_unix), Some(step)) => Some(Activating {
            since_unix,
            step: ActivationStep::parse(&step).ok_or_else(|| anyhow::anyhow!("unknown trigger_activation.activating_step '{step}'"))?,
            listened: activating_listened,
        }),
        // Not activating; or activating under a claim of a weft from
        // before activations said where they were, which nothing drives
        // ([`orphaned`]) and the `lapses` loop ends when it looks.
        _ => None,
    };
    Ok(Activation {
        key: ActivationKey::new(trigger, Owner::from_instance(instance)),
        lifecycle: ActivationLifecycle {
            status,
            accepting_fires: accepting,
            fires_visible_to_consumers: visible,
            fires_deadline_unix: deadline,
            went_down: weft_broker_client::activation::WentDown::of_columns(went_down_with.as_deref(), went_down_by_command).map_err(anyhow::Error::msg)?,
            activating_run_id: run_id,
        },
        program,
        source_version: version,
        cut: fingerprint.map(|fingerprint| RecordedCut { fingerprint, depends_on }),
        driver_task,
        activating,
    })
}

/// Why [`ActivationStoreOps::try_begin_activating`] refused its claim. On
/// the wire where an `activate` task answers whoever waits on it
/// (`crate::task_kinds::activate`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClaimRefused {
    /// The project is building (or gone): no activation starts.
    Building,
    /// One of the keys is already activating: another claim holds it.
    Claimed,
    /// With `expect`: one of the keys is missing or not in that status.
    NotExpected,
    /// A resync holds the project: only its own claim goes through.
    Resyncing,
    /// The claim was a resync's, and that drive no longer holds the
    /// project (a person gave it up, or took it up again under another).
    ResyncGone,
    /// `weft rm` marked the project and has not finished removing it.
    Removing,
    /// The install is in maintenance (`crate::maintenance`): only the
    /// lift's claim goes through.
    Maintenance,
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

/// Whether each of `keys`' triggers listened the whole time it was down,
/// in `keys`' order, as a claim finds its row among `before` (none: it was
/// never activated) at `now_unix`
/// (`crate::task_kinds::register_signal::listened_while_down`): read once,
/// at the claim, and kept on the row, so the driver arming it, or the one
/// that carries the activation on after it, arms it the same way.
fn listened_as_found(keys: &[ActivationKey], before: &[Activation], now_unix: i64) -> Vec<bool> {
    keys.iter()
        .map(|key| {
            let found = before.iter().find(|a| &a.key == key).map(|a| &a.lifecycle);
            crate::task_kinds::register_signal::listened_while_down(found, now_unix)
        })
        .collect()
}

/// The keys as two parallel arrays, for an `unnest` join: the triggers,
/// and the instances with `''` standing for the shared owner (an array
/// cannot carry a NULL that `IS NOT DISTINCT FROM` would match on both
/// sides reliably through `unnest`, so the join compares
/// `COALESCE(instance_id, '')`; an instance id is never empty). Every
/// statement keyed by activations unnests these: the activation rows
/// here, and the signals and runs they govern
/// (`crate::store::postgres`).
pub(crate) fn key_arrays(keys: &[ActivationKey]) -> (Vec<String>, Vec<String>) {
    keys.iter()
        .map(|k| (k.trigger.clone(), k.instance().map(|m| m.as_str().to_string()).unwrap_or_default()))
        .unzip()
}

const KEYS_JOIN: &str = "(a.trigger, COALESCE(a.instance_id, '')) IN (SELECT * FROM unnest($2::text[], $3::text[]))";

/// The project row as a guarded activation write holds it, locked for the
/// rest of the write's transaction: that lock orders the write with a
/// build, a resync's drive and a person giving a resync up.
struct HeldProject {
    transition: String,
    /// The project's resync (`project.resync`): the one under way, or the
    /// record of one a person gave up.
    resync: Option<crate::task_kinds::resync::ResyncPlan>,
    /// `weft rm` marked the project (`project.removing`): the row's lock
    /// orders this write with the mark, so nothing is turned on after it.
    removing: bool,
}

/// [`HeldProject`] as its row reads.
type HeldRow = (String, Option<String>, weft_core::stored::Version, Option<uuid::Uuid>, bool);

/// Lock and read the project row in the caller's transaction; `None`
/// when there is no such project.
async fn hold_project(tx: &Transaction<'_>, project_id: uuid::Uuid) -> anyhow::Result<Option<HeldProject>> {
    let row: Option<HeldRow> = tx
        .opt("SELECT transition, resync, resync_version, resync_driver, removing FROM project WHERE id = $1 FOR UPDATE", &[&project_id])
        .await?;
    row.map(|(transition, resync, version, driver, removing)| {
        let resync = resync.map(|json| crate::task_kinds::resync::ResyncPlan::stored(project_id, json.as_str(), version, driver)).transpose()?;
        Ok(HeldProject { transition, resync, removing })
    })
    .transpose()
}

impl HeldProject {
    /// The drive of the resync under way, when `transition` says one is.
    fn driver(&self) -> Option<uuid::Uuid> {
        self.resync.as_ref().map(|plan| plan.driver)
    }

    /// The write goes through for `keys`: someone activating or taking
    /// them down decided about them, so a given-up resync's record stops
    /// naming them (`crate::task_kinds::resync::GivenUp::forget`).
    async fn decided(self, tx: &Transaction<'_>, project_id: uuid::Uuid, keys: &[ActivationKey]) -> anyhow::Result<()> {
        let Some(mut plan) = self.resync else { return Ok(()) };
        let Some(given_up) = plan.given_up.as_mut() else { return Ok(()) };
        if given_up.forget(keys) {
            let sql = format!("UPDATE project SET {} WHERE id = $1", crate::task_kinds::resync::ResyncPlan::ROW);
            plan.row_write(tx, project_id, &sql, &[]).await?;
        }
        Ok(())
    }
}

/// [`ActivationStoreOps::list`] on the caller's connection.
pub(crate) async fn list_in(conn: &impl Client, project_id: uuid::Uuid) -> anyhow::Result<Vec<Activation>> {
    let rows: Vec<LifecycleRow> = conn
        .all(
            &format!("SELECT {ROW_COLUMNS} FROM trigger_activation a WHERE a.project_id = $1 ORDER BY a.trigger, a.instance_id NULLS FIRST"),
            &[&project_id],
        )
        .await?;
    rows.into_iter().map(row_to_activation).collect()
}

#[async_trait]
impl ActivationStoreOps for PostgresActivationStore {
    async fn list(&self, project_id: uuid::Uuid) -> anyhow::Result<Vec<Activation>> {
        list_in(&self.pool, project_id).await
    }

    async fn try_begin_activating(
        &self,
        project_id: uuid::Uuid,
        keys: &[ActivationKey],
        run_id: uuid::Uuid,
        expect: Option<ProjectStatus>,
        claimant: Claimant,
        task: uuid::Uuid,
    ) -> anyhow::Result<Result<Vec<Activation>, ClaimRefused>> {
        use crate::project_store::ProjectTransition;
        let now = crate::lease::now_unix();
        let (triggers, instances) = key_arrays(keys);
        let mut conn = self.pool.get().await?;
        let tx = conn.transaction().await?;
        // The project row serializes activations with its transition: a
        // build refuses while any activation is mid-flip, and an activation
        // refuses while a build is in flight; under a resync, only the
        // resync's own drive turns triggers on.
        let Some(held) = hold_project(&tx, project_id).await? else { return Ok(Err(ClaimRefused::Building)) };
        let transition = crate::project_store::project_transition_from_str(&held.transition)?;
        // Read under the project row's lock: `weft maintenance on` takes
        // that lock after it records the maintenance, so a claim either
        // sees it or is one `on` sees activating.
        let in_maintenance = claimant != Claimant::Lift && crate::maintenance::is_on(&tx).await?;
        let refused = match (transition, claimant.resync()) {
            _ if held.removing => Some(ClaimRefused::Removing),
            (t, _) if t.is_building() => Some(ClaimRefused::Building),
            _ if in_maintenance => Some(ClaimRefused::Maintenance),
            (ProjectTransition::Resyncing, Some(own)) if held.driver() == Some(own) => None,
            (_, Some(_)) => Some(ClaimRefused::ResyncGone),
            (ProjectTransition::Resyncing, None) => Some(ClaimRefused::Resyncing),
            _ => None,
        };
        if let Some(refused) = refused {
            return Ok(Err(refused));
        }
        let before: Vec<LifecycleRow> = tx
            .all(
                &format!("SELECT {ROW_COLUMNS} FROM trigger_activation a WHERE a.project_id = $1 AND {KEYS_JOIN} FOR UPDATE"),
                &[&project_id, &triggers, &instances],
            )
            .await?;
        let before: Vec<Activation> = before.into_iter().map(row_to_activation).collect::<anyhow::Result<_>>()?;
        if let Err(refused) = claimable(keys, &before, expect) {
            return Ok(Err(refused));
        }
        held.decided(&tx, project_id, keys).await?;
        let activating = ActivationLifecycle::activating(run_id);
        let listened = listened_as_found(keys, &before, now);
        // The cut a row recorded stays until the setup records the new
        // one: an activation that fails puts the row back as it was. So
        // does what took it down (`went_down_with`): it goes only when the
        // activation lands it on ([`end_activating_in`]), so one that
        // fails, however it ends, leaves the trigger marked for what
        // brings it back (the maintenance lift, its infra's return).
        tx.execute(
            "INSERT INTO trigger_activation \
               (project_id, trigger, instance_id, status, accepting_fires, fires_visible_to_consumers, \
                fires_deadline_unix, went_down_with, went_down_by_command, activating_run_id, \
                driver_task, activating_since_unix, activating_step, activating_listened, \
                activation_program, activation_program_version, activation_version, updated_at) \
             SELECT $1::uuid, t, NULLIF(m, ''), $4::text, $5::boolean, $6::boolean, NULL, NULL, NULL, $7::uuid, \
                    $10::uuid, $8::bigint, $11::text, l, NULL, $9::integer, NULL, $8::bigint \
             FROM unnest($2::text[], $3::text[], $12::boolean[]) AS k(t, m, l) \
             ON CONFLICT (project_id, trigger, instance_id) DO UPDATE SET \
                 status = EXCLUDED.status, \
                 accepting_fires = EXCLUDED.accepting_fires, \
                 fires_visible_to_consumers = EXCLUDED.fires_visible_to_consumers, \
                 fires_deadline_unix = NULL, \
                 activating_run_id = EXCLUDED.activating_run_id, \
                 driver_task = EXCLUDED.driver_task, \
                 activating_since_unix = EXCLUDED.activating_since_unix, \
                 activating_step = EXCLUDED.activating_step, \
                 activating_listened = EXCLUDED.activating_listened, \
                 activation_program = NULL, \
                 activation_version = NULL, \
                 updated_at = EXCLUDED.updated_at",
            &[
                &project_id,
                &triggers,
                &instances,
                &activating.status.as_str(),
                &activating.accepting_fires,
                &activating.fires_visible_to_consumers,
                &run_id,
                &now,
                &ProgramIdentity::STORED.version(),
                &task,
                &ActivationStep::Preparing.as_str(),
                &listened,
            ],
        )
        .await?;
        tx.commit().await?;
        Ok(Ok(before))
    }

    async fn end_activating(
        &self,
        project_id: uuid::Uuid,
        run_id: uuid::Uuid,
        to: &ActivationLifecycle,
        remove_entries: bool,
        values: Option<&ValuesStore<'_>>,
    ) -> anyhow::Result<Option<Vec<crate::store::SignalRegistration>>> {
        let mut conn = self.pool.get().await?;
        let tx = conn.transaction().await?;
        let ended = end_activating_in(&tx, project_id, run_id, to, remove_entries, values).await?;
        tx.commit().await?;
        Ok(ended)
    }

    async fn end_unfinished(
        &self,
        project_id: uuid::Uuid,
        run_id: uuid::Uuid,
    ) -> anyhow::Result<Option<Vec<crate::store::SignalRegistration>>> {
        let mut conn = self.pool.get().await?;
        let tx = conn.transaction().await?;
        let owned: Vec<(String, Option<String>)> = tx
            .all(
                "SELECT trigger, instance_id FROM trigger_activation \
                 WHERE project_id = $1 AND activating_run_id = $2 AND status = 'activating' FOR UPDATE",
                &[&project_id, &run_id],
            )
            .await?;
        if owned.is_empty() {
            return Ok(None);
        }
        let keys = owned_keys(owned)?;
        let kept = crate::store::postgres::held_on(&tx, project_id, &keys).await?;
        let ended = match kept.is_empty() {
            false => end_activating_in(&tx, project_id, run_id, &ActivationLifecycle::holding(), false, None).await?,
            true => end_activating_in(&tx, project_id, run_id, &ActivationLifecycle::refused(), true, None).await?,
        };
        tx.commit().await?;
        Ok(ended)
    }

    async fn restore(
        &self,
        project_id: uuid::Uuid,
        run_id: uuid::Uuid,
        previous: &[Activation],
    ) -> anyhow::Result<bool> {
        let now = crate::lease::now_unix();
        let mut conn = self.pool.get().await?;
        let tx = conn.transaction().await?;
        let mut restored = 0;
        for activation in previous {
            let before = &activation.lifecycle;
            restored += tx
                .execute(
                    "UPDATE trigger_activation \
                     SET status = $1, accepting_fires = $2, fires_visible_to_consumers = $3, \
                         fires_deadline_unix = $4, went_down_with = $5, went_down_by_command = $13, \
                         activating_run_id = NULL, activation_program = $6, activation_program_version = $16, \
                         activation_version = $7, updated_at = $8, cut_fingerprint = $14, depends_on = $15, \
                         driver_task = $17, activating_since_unix = NULL, activating_step = NULL, activating_listened = FALSE \
                     WHERE project_id = $9 AND activating_run_id = $10 AND status = 'activating' \
                       AND trigger = $11 AND instance_id IS NOT DISTINCT FROM $12",
                    &[
                        &before.status.as_str(),
                        &before.accepting_fires,
                        &before.fires_visible_to_consumers,
                        &before.fires_deadline_unix,
                        &before.went_down.map(|w| w.with().as_str()),
                        &activation.program.as_ref().map(db::Json),
                        &activation.source_version.as_deref(),
                        &now,
                        &project_id,
                        &run_id,
                        &activation.key.trigger,
                        &activation.key.instance().map(InstanceId::as_str),
                        &before.went_down.and_then(|w| w.first_command()),
                        &activation.cut.as_ref().map(|cut| cut.fingerprint.as_str()),
                        &activation.cut.as_ref().map(|cut| cut.depends_on.clone()).unwrap_or_default(),
                        &ProgramIdentity::STORED.version(),
                        &activation.driver_task,
                    ],
                )
                .await?;
        }
        // What had no row before the claim was never activated: its row
        // goes again.
        restored += tx
            .execute(
                "DELETE FROM trigger_activation \
                 WHERE project_id = $1 AND activating_run_id = $2 AND status = 'activating'",
                &[&project_id, &run_id],
            )
            .await?;
        tx.commit().await?;
        Ok(restored > 0)
    }

    async fn record_activation_source(
        &self,
        project_id: uuid::Uuid,
        run_id: uuid::Uuid,
        program: &ProgramIdentity,
        source_version: &str,
        cuts: &std::collections::BTreeMap<String, RecordedCut>,
    ) -> anyhow::Result<bool> {
        let mut conn = self.pool.get().await?;
        let tx = conn.transaction().await?;
        let res = tx
            .execute(
                "UPDATE trigger_activation SET activation_program = $3, activation_program_version = $5, activation_version = $4 \
                 WHERE project_id = $1 AND activating_run_id = $2 AND status = 'activating'",
                &[&project_id, &run_id, &db::Json(program), &source_version, &ProgramIdentity::STORED.version()],
            )
            .await?;
        for (trigger, cut) in cuts {
            tx.execute(
                "UPDATE trigger_activation SET cut_fingerprint = $4, depends_on = $5 \
                 WHERE project_id = $1 AND activating_run_id = $2 AND status = 'activating' AND trigger = $3",
                &[&project_id, &run_id, trigger, &cut.fingerprint, &cut.depends_on],
            )
            .await?;
        }
        tx.commit().await?;
        Ok(res > 0)
    }

    async fn set_lifecycle_guarded(
        &self,
        project_id: uuid::Uuid,
        keys: &[ActivationKey],
        lifecycle: &ActivationLifecycle,
        signals: SignalsGoing<'_>,
        forget: Forget,
        cause: DownCause<'_>,
        resync: Option<uuid::Uuid>,
    ) -> anyhow::Result<LifecycleWrite> {
        use crate::project_store::ProjectTransition;
        use weft_broker_client::activation::WentDown;
        let (triggers, instances) = key_arrays(keys);
        let mut conn = self.pool.get().await?;
        let tx = conn.transaction().await?;
        let Some(held_project) = hold_project(&tx, project_id).await? else {
            return Ok(LifecycleWrite::NotFound);
        };
        let parsed = crate::project_store::project_transition_from_str(&held_project.transition)?;
        if parsed.is_building() {
            return Ok(LifecycleWrite::Rejected { blocker: held_project.transition });
        }
        // An infra verb's commands are never issued for a project `weft rm`
        // is removing: its own terminate is the last word on its copies.
        if held_project.removing && matches!(cause, DownCause::Infra(_)) {
            return Ok(LifecycleWrite::NotFound);
        }
        // A resync takes its own triggers down under its transition, only
        // while its drive holds it.
        if let Some(own) = resync {
            if parsed != ProjectTransition::Resyncing || held_project.driver() != Some(own) {
                return Ok(LifecycleWrite::ResyncGone);
            }
        }
        let activating: Option<(String, Option<String>)> = tx
            .opt(
                &format!(
                    "SELECT a.trigger, a.instance_id FROM trigger_activation a \
                     WHERE a.project_id = $1 AND a.status = 'activating' AND {KEYS_JOIN} LIMIT 1"
                ),
                &[&project_id, &triggers, &instances],
            )
            .await?;
        if let Some((trigger, instance)) = activating {
            let whose = instance.map(|m| format!(" of instance '{m}'")).unwrap_or_default();
            return Ok(LifecycleWrite::Rejected { blocker: format!("activating (trigger '{trigger}'{whose})") });
        }
        // A cause that brings them back takes down only the ones still on,
        // as this lock sees them: one a person took down since it was
        // listed stays theirs, never marked to come back.
        let keys: Vec<ActivationKey> = match cause {
            DownCause::Person => keys.to_vec(),
            DownCause::Health(_) | DownCause::Infra(_) | DownCause::Maintenance => {
                let on: Vec<(String, Option<String>)> = tx
                    .all(
                        &format!(
                            "SELECT a.trigger, a.instance_id FROM trigger_activation a \
                             WHERE a.project_id = $1 AND a.status = 'active' AND {KEYS_JOIN} FOR UPDATE"
                        ),
                        &[&project_id, &triggers, &instances],
                    )
                    .await?;
                owned_keys(on)?
            }
        };
        let (triggers, instances) = key_arrays(&keys);
        let keys = keys.as_slice();
        // Their entry tokens, read before anything of them goes: what a wait
        // for the runs they fired counts at the workers' door.
        let tokens = crate::store::postgres::entry_tokens(&tx, project_id, keys).await?;
        // The mark, and an infra verb's commands with it: claimable only
        // once the triggers reading their copies are down.
        let (went_down, commands) = match cause {
            DownCause::Person => (None, Vec::new()),
            DownCause::Health(_) => (Some(WentDown::Health), Vec::new()),
            DownCause::Maintenance => (Some(WentDown::Maintenance), Vec::new()),
            DownCause::Infra(commands) => match commands.issue_in(&tx, project_id, &tokens).await? {
                crate::infra_lifecycle_command::Issued::Commands(ids) => {
                    let first_command = *ids.first().ok_or_else(|| anyhow::anyhow!("an infra take-down issued no command"))?;
                    (commands.copies_can_return().then_some(WentDown::Infra { first_command }), ids)
                }
                crate::infra_lifecycle_command::Issued::UpgradeInFlight(command) => return Ok(LifecycleWrite::UpgradeInFlight(command)),
            },
        };
        held_project.decided(&tx, project_id, keys).await?;
        // What they hold, as it stands before anything of it goes: what the
        // take-down says it kept or dropped.
        let held = crate::store::postgres::held_on(&tx, project_id, keys).await?;
        let forgotten: Vec<ActivationKey> = keys.iter().filter(|k| forget.forgets(k)).cloned().collect();
        if !forgotten.is_empty() {
            let (triggers, instances) = key_arrays(&forgotten);
            tx.execute(&format!("DELETE FROM trigger_activation a WHERE a.project_id = $1 AND {KEYS_JOIN}"), &[&project_id, &triggers, &instances])
                .await?;
        }
        tx.execute(
            &format!(
                "UPDATE trigger_activation a \
                 SET status = $4::text, accepting_fires = $5, fires_visible_to_consumers = $6, \
                     fires_deadline_unix = $7, went_down_with = $8, went_down_by_command = $10, \
                     activating_run_id = NULL, \
                     activation_program = CASE WHEN $4::text = 'inactive' THEN NULL ELSE activation_program END, \
                     activation_version = CASE WHEN $4::text = 'inactive' THEN NULL ELSE activation_version END, \
                     updated_at = $9 \
                 WHERE a.project_id = $1 AND {KEYS_JOIN}"
            ),
            &[
                &project_id,
                &triggers,
                &instances,
                &lifecycle.status.as_str(),
                &lifecycle.accepting_fires,
                &lifecycle.fires_visible_to_consumers,
                &lifecycle.fires_deadline_unix,
                &went_down.map(|w| w.with().as_str()),
                &crate::lease::now_unix(),
                &went_down.and_then(|w| w.first_command()),
            ],
        )
        .await?;
        let unlisten = match signals {
            SignalsGoing::Kept => Vec::new(),
            SignalsGoing::Places(places) => {
                if !crate::store::postgres::write_fresh_places(&tx, places).await? {
                    tx.rollback().await?;
                    return Ok(LifecycleWrite::StateMoved);
                }
                Vec::new()
            }
            SignalsGoing::HeldArrivals => {
                crate::store::postgres::drop_held_arrivals(&tx, project_id, keys).await?;
                Vec::new()
            }
            SignalsGoing::Project { except } => {
                crate::store::postgres::remove_project_signals_except(&tx, project_id, except).await?
            }
            SignalsGoing::Activations => crate::store::postgres::remove_entry_signals(&tx, project_id, keys).await?,
        };
        tx.commit().await?;
        Ok(LifecycleWrite::Applied { keys: keys.to_vec(), tokens, unlisten, held, commands })
    }

    async fn set_step(&self, project_id: uuid::Uuid, run_id: uuid::Uuid, step: ActivationStep) -> anyhow::Result<bool> {
        let set = self
            .pool
            .execute(
                "UPDATE trigger_activation SET activating_step = $3 \
                 WHERE project_id = $1 AND activating_run_id = $2 AND status = 'activating'",
                &[&project_id, &run_id, &step.as_str()],
            )
            .await?;
        Ok(set > 0)
    }

    async fn list_orphaned(&self) -> anyhow::Result<Vec<OrphanedActivation>> {
        let rows: Vec<(uuid::Uuid, uuid::Uuid)> = self
            .pool
            .all(&format!("SELECT DISTINCT a.project_id, a.activating_run_id FROM trigger_activation a WHERE {}", orphaned()), &[])
            .await?;
        Ok(rows.into_iter().map(|(project_id, run_id)| OrphanedActivation { project_id, run_id }).collect())
    }
}

/// `(project_id, trigger, instance_id)` rows as the activations they key.
fn project_keys(rows: Vec<(uuid::Uuid, String, Option<String>)>) -> anyhow::Result<Vec<(uuid::Uuid, ActivationKey)>> {
    rows.into_iter()
        .map(|(project, trigger, instance)| {
            let instance = instance.map(InstanceId::new).transpose().map_err(anyhow::Error::msg)?;
            Ok((project, ActivationKey::new(trigger, Owner::from_instance(instance))))
        })
        .collect()
}

/// One fake row: the lifecycle, the code and version its setup recorded,
/// its cut, and the task that claimed it with where its activation stands.
#[cfg(any(test, feature = "test-helpers"))]
#[derive(Clone)]
struct FakeRow {
    lifecycle: ActivationLifecycle,
    program: Option<ProgramIdentity>,
    version: Option<String>,
    cut: Option<RecordedCut>,
    driver_task: Option<uuid::Uuid>,
    activating: Option<Activating>,
}

#[cfg(any(test, feature = "test-helpers"))]
impl FakeRow {
    fn activation(&self, key: &ActivationKey) -> Activation {
        Activation {
            key: key.clone(),
            lifecycle: self.lifecycle.clone(),
            program: self.program.clone(),
            source_version: self.version.clone(),
            cut: self.cut.clone(),
            driver_task: self.driver_task,
            activating: self.activating,
        }
    }
}

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
            .map(|((_, key), row)| row.activation(key))
            .collect();
        out.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(out)
    }

    async fn try_begin_activating(
        &self,
        project_id: uuid::Uuid,
        keys: &[ActivationKey],
        run_id: uuid::Uuid,
        expect: Option<ProjectStatus>,
        // The project's transition and the install's maintenance live in
        // tables this fake does not see: it guards the build axis alone.
        _claimant: Claimant,
        task: uuid::Uuid,
    ) -> anyhow::Result<Result<Vec<Activation>, ClaimRefused>> {
        if self.building.read().await.contains(&project_id) {
            return Ok(Err(ClaimRefused::Building));
        }
        let mut rows = self.rows.write().await;
        let before: Vec<Activation> =
            keys.iter().filter_map(|key| rows.get(&(project_id, key.clone())).map(|row| row.activation(key))).collect();
        if let Err(refused) = claimable(keys, &before, expect) {
            return Ok(Err(refused));
        }
        let now = crate::lease::now_unix();
        for (key, listened) in keys.iter().zip(listened_as_found(keys, &before, now)) {
            let found = rows.get(&(project_id, key.clone()));
            let cut = found.and_then(|row| row.cut.clone());
            let went_down = found.and_then(|row| row.lifecycle.went_down);
            let lifecycle = ActivationLifecycle { went_down, ..ActivationLifecycle::activating(run_id) };
            let activating = Some(Activating { since_unix: now, step: ActivationStep::Preparing, listened });
            let row = FakeRow { lifecycle, program: None, version: None, cut, driver_task: Some(task), activating };
            rows.insert((project_id, key.clone()), row);
        }
        Ok(Ok(before))
    }

    async fn set_step(&self, project_id: uuid::Uuid, run_id: uuid::Uuid, step: ActivationStep) -> anyhow::Result<bool> {
        let mut set = false;
        for ((p, _), row) in self.rows.write().await.iter_mut() {
            if let (true, Some(activating)) = (*p == project_id && row.lifecycle.activating_run_id == Some(run_id), row.activating.as_mut()) {
                activating.step = step;
                set = true;
            }
        }
        Ok(set)
    }

    async fn end_activating(
        &self,
        project_id: uuid::Uuid,
        run_id: uuid::Uuid,
        to: &ActivationLifecycle,
        remove_entries: bool,
        values: Option<&ValuesStore<'_>>,
    ) -> anyhow::Result<Option<Vec<crate::store::SignalRegistration>>> {
        if values.is_some() {
            anyhow::bail!("the fake activation store cannot store a re-arm's change; that takes Postgres");
        }
        let mut rows = self.rows.write().await;
        let mut ended = false;
        rows.retain(|(p, key), row| {
            if *p != project_id || row.lifecycle.status != ProjectStatus::Activating || row.lifecycle.activating_run_id != Some(run_id) {
                return true;
            }
            ended = true;
            if remove_entries && key.instance().is_some() {
                return false;
            }
            let went_down = match (to.went_down, to.status) {
                (None, status) if status != ProjectStatus::Active => row.lifecycle.went_down,
                (went_down, _) => went_down,
            };
            row.lifecycle = ActivationLifecycle { went_down, ..to.clone() };
            row.activating = None;
            if to.status != ProjectStatus::Active {
                row.program = None;
                row.version = None;
            }
            true
        });
        Ok(ended.then(Vec::new))
    }

    async fn end_unfinished(
        &self,
        project_id: uuid::Uuid,
        run_id: uuid::Uuid,
    ) -> anyhow::Result<Option<Vec<crate::store::SignalRegistration>>> {
        // The fake keeps no signals, so no trigger kept any work.
        self.end_activating(project_id, run_id, &ActivationLifecycle::refused(), true, None).await
    }

    async fn restore(
        &self,
        project_id: uuid::Uuid,
        run_id: uuid::Uuid,
        previous: &[Activation],
    ) -> anyhow::Result<bool> {
        let mut restored = false;
        let mut rows = self.rows.write().await;
        rows.retain(|(p, key), row| {
            if *p != project_id || row.lifecycle.status != ProjectStatus::Activating || row.lifecycle.activating_run_id != Some(run_id) {
                return true;
            }
            restored = true;
            match previous.iter().find(|a| &a.key == key) {
                Some(before) => {
                    row.lifecycle = before.lifecycle.clone();
                    row.program = before.program.clone();
                    row.version = before.source_version.clone();
                    row.cut = before.cut.clone();
                    row.driver_task = before.driver_task;
                    row.activating = None;
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
        run_id: uuid::Uuid,
        program: &ProgramIdentity,
        source_version: &str,
        cuts: &std::collections::BTreeMap<String, RecordedCut>,
    ) -> anyhow::Result<bool> {
        let mut recorded = false;
        for ((p, key), row) in self.rows.write().await.iter_mut() {
            if *p == project_id && row.lifecycle.status == ProjectStatus::Activating && row.lifecycle.activating_run_id == Some(run_id) {
                row.program = Some(program.clone());
                row.version = Some(source_version.to_string());
                if let Some(cut) = cuts.get(&key.trigger) {
                    row.cut = Some(cut.clone());
                }
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
        _signals: SignalsGoing<'_>,
        forget: Forget,
        cause: DownCause<'_>,
        _resync: Option<uuid::Uuid>,
    ) -> anyhow::Result<LifecycleWrite> {
        let went_down = match cause {
            DownCause::Person => None,
            DownCause::Health(_) => Some(weft_broker_client::activation::WentDown::Health),
            DownCause::Maintenance => Some(weft_broker_client::activation::WentDown::Maintenance),
            DownCause::Infra(_) => anyhow::bail!("the in-memory store issues no infra command; an infra take-down needs Postgres"),
        };
        if self.building.read().await.contains(&project_id) {
            return Ok(LifecycleWrite::Rejected { blocker: "building".into() });
        }
        let mut rows = self.rows.write().await;
        if let Some(key) = keys.iter().find(|k| {
            rows.get(&(project_id, (*k).clone())).is_some_and(|row| row.lifecycle.status == ProjectStatus::Activating)
        }) {
            return Ok(LifecycleWrite::Rejected { blocker: format!("activating (trigger {key})") });
        }
        // A cause that brings them back takes down only the ones still on.
        let keys: Vec<ActivationKey> = keys
            .iter()
            .filter(|k| {
                went_down.is_none() || rows.get(&(project_id, (*k).clone())).is_some_and(|row| row.lifecycle.status == ProjectStatus::Active)
            })
            .cloned()
            .collect();
        for key in &keys {
            if forget.forgets(key) {
                rows.remove(&(project_id, key.clone()));
                continue;
            }
            if let Some(row) = rows.get_mut(&(project_id, key.clone())) {
                row.lifecycle = ActivationLifecycle { activating_run_id: None, went_down, ..lifecycle.clone() };
                if lifecycle.status == ProjectStatus::Inactive {
                    row.program = None;
                    row.version = None;
                }
            }
        }
        // The signals live in the `SignalStore`'s tables, which this fake does
        // not hold (the same as `end_activating`'s), so nothing is held.
        Ok(LifecycleWrite::Applied { keys, tokens: Vec::new(), unlisten: Vec::new(), held: Default::default(), commands: Vec::new() })
    }

    async fn list_orphaned(&self) -> anyhow::Result<Vec<OrphanedActivation>> {
        anyhow::bail!("the in-memory store keeps no tasks, so it cannot tell whose driver ended; orphans need Postgres")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use weft_core::take_down::{Door, Held, Running, TakeDown};

    fn key(instance: Option<&str>) -> ActivationKey {
        ActivationKey::new("feed", Owner::from_instance(instance.map(|m| InstanceId::new(m).unwrap())))
    }

    async fn activate(store: &FakeActivationStore, project: uuid::Uuid, key: &ActivationKey) {
        let setup = uuid::Uuid::new_v4();
        assert!(store.try_begin_activating(project, std::slice::from_ref(key), setup, None, Claimant::Anyone, uuid::Uuid::new_v4()).await.unwrap().is_ok());
        store.end_activating(project, setup, &ActivationLifecycle::active(), false, None).await.unwrap().expect("owned");
    }

    async fn owners(store: &FakeActivationStore, project: uuid::Uuid) -> Vec<Option<String>> {
        store.list(project).await.unwrap().into_iter().map(|a| a.key.instance().map(|m| m.as_str().to_string())).collect()
    }

    /// An instance's trigger refusing with nothing kept leaves no row; the
    /// shared one, one that holds, and one taken down with its infra keep
    /// theirs.
    #[tokio::test]
    async fn an_instance_refusing_with_nothing_kept_is_forgotten() {
        let store = FakeActivationStore::new();
        let project = uuid::Uuid::new_v4();
        let (shared, ada, bob, cyd) = (key(None), key(Some("ada")), key(Some("bob")), key(Some("cyd")));
        for k in [&shared, &ada, &bob, &cyd] {
            activate(&store, project, k).await;
        }
        let gone = TakeDown { door: Door::Refuse, door_for_minutes: None, held: Held::Drop, running: Running::Cancel };
        let down = |take_down: &TakeDown, cause: &DownCause<'_>| (ActivationLifecycle::down(take_down, 0), Forget::of(take_down, cause));
        let (lifecycle, forget) = down(&gone, &DownCause::Person);
        store.set_lifecycle_guarded(project, &[shared.clone(), ada.clone()], &lifecycle, SignalsGoing::Activations, forget, DownCause::Person, None).await.unwrap();
        assert_eq!(owners(&store, project).await, vec![None, Some("bob".into()), Some("cyd".into())]);

        let stop = crate::infra_lifecycle_command::TakeDownCommands {
            tenant_id: "t".into(),
            issued_by_replica: "r".into(),
            work: crate::infra_lifecycle_command::TakeDownWork::Teardown {
                nodes: Vec::new(),
                copies: weft_core::instance::Copies::Shared,
                teardown: crate::infra_lifecycle_command::Teardown::Stop { force: false },
                waits: false,
            },
        };
        let (lifecycle, forget) = down(&gone, &DownCause::Infra(&stop));
        assert_eq!(forget, Forget::Nothing, "taken down with its infra, its row is what brings it back");
        store.set_lifecycle_guarded(project, std::slice::from_ref(&bob), &lifecycle, SignalsGoing::Kept, forget, DownCause::Person, None).await.unwrap();
        let kept = TakeDown { held: Held::Keep, ..gone };
        let (lifecycle, forget) = down(&kept, &DownCause::Person);
        store.set_lifecycle_guarded(project, std::slice::from_ref(&cyd), &lifecycle, SignalsGoing::Kept, forget, DownCause::Person, None).await.unwrap();
        assert_eq!(owners(&store, project).await, vec![None, Some("bob".into()), Some("cyd".into())]);

        let setup = uuid::Uuid::new_v4();
        let previous = store.try_begin_activating(project, std::slice::from_ref(&ada), setup, None, Claimant::Anyone, uuid::Uuid::new_v4()).await.unwrap().expect("claimed");
        assert!(previous.is_empty(), "ada activates again as never activated");

        // That activation cancelled (or reaped) mid-way with nothing kept
        // lands refusing, so ada's row goes again; a shared one ending the
        // same way stays.
        store.end_activating(project, setup, &ActivationLifecycle::refused(), true, None).await.unwrap().expect("owned");
        assert_eq!(owners(&store, project).await, vec![None, Some("bob".into()), Some("cyd".into())]);
        let setup = uuid::Uuid::new_v4();
        store.try_begin_activating(project, std::slice::from_ref(&shared), setup, None, Claimant::Anyone, uuid::Uuid::new_v4()).await.unwrap().expect("claimed");
        store.end_activating(project, setup, &ActivationLifecycle::refused(), true, None).await.unwrap().expect("owned");
        assert_eq!(owners(&store, project).await, vec![None, Some("bob".into()), Some("cyd".into())]);
    }

    /// A setup records each trigger's cut, which stays while the trigger is
    /// down, and goes back with a failed activation put back.
    #[tokio::test]
    async fn a_triggers_cut_is_recorded_kept_while_down_and_restored() {
        let store = FakeActivationStore::new();
        let project = uuid::Uuid::new_v4();
        let feed = key(None);
        let setup = uuid::Uuid::new_v4();
        store.try_begin_activating(project, std::slice::from_ref(&feed), setup, None, Claimant::Anyone, uuid::Uuid::new_v4()).await.unwrap().expect("claimed");
        let program = ProgramIdentity { definition_hash: "d".into(), binary_hash: "b".into(), implementations: Default::default() };
        let cut = RecordedCut { fingerprint: "f1".into(), depends_on: vec!["db".into()] };
        let cuts = std::collections::BTreeMap::from([("feed".to_string(), cut.clone())]);
        assert!(store.record_activation_source(project, setup, &program, "v1", &cuts).await.unwrap());
        store.end_activating(project, setup, &ActivationLifecycle::active(), false, None).await.unwrap().expect("owned");
        let (lifecycle, forget) = (ActivationLifecycle::holding(), Forget::Nothing);
        store.set_lifecycle_guarded(project, std::slice::from_ref(&feed), &lifecycle, SignalsGoing::Kept, forget, DownCause::Person, None).await.unwrap();
        let row = store.list(project).await.unwrap().remove(0);
        assert_eq!(row.cut, Some(cut.clone()), "kept while down");
        assert!(row.program.is_none(), "what it fires goes with it");

        let again = uuid::Uuid::new_v4();
        let previous = store.try_begin_activating(project, std::slice::from_ref(&feed), again, None, Claimant::Anyone, uuid::Uuid::new_v4()).await.unwrap().expect("claimed");
        assert!(store.restore(project, again, &previous).await.unwrap());
        assert_eq!(store.list(project).await.unwrap()[0].cut, Some(cut));
    }
}
