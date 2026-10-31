//! Project store. Keyed by project id. Holds the registered
//! ProjectDefinition, its running hashes and its build transition. Which
//! of its triggers are listening is the activation store's
//! (`activation_store`).
//!
//! Its one implementation is Postgres-backed (`PostgresProjectStore`),
//! so every dispatcher reads and writes the same `project` table.

use std::sync::Arc;

use async_trait::async_trait;
use weft_task_store::db::{self, Client, Pool};

use weft_core::ProjectDefinition;

use crate::task_kinds::resync::ResyncPlan;

/// The complete per-node infra image-tag map: `node_id -> { image_name ->
/// image_ref }`. Written atomically alongside the running hashes by the
/// build that made the images (`ProjectStoreOps::register_with_hashes`);
/// the supervisor reads it per node
/// (through the broker) to resolve `Image::Local { name }`. The column's
/// canonical decode is `weft_broker_client::protocol::decode_infra_image_tags`
/// (shared with the broker's read so the two cannot drift).
pub type InfraImageTags =
    std::collections::BTreeMap<String, std::collections::HashMap<String, String>>;

/// What a registered worker binary carries for its project: the
/// implementation fingerprints of its node types, and the node packages its
/// build left out because they did not compile, with where that build's
/// log is read. Kept beside the binary (`project_code`), never in the
/// definition, which is stored and cached under its own hash.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProgramCode {
    pub implementations: std::collections::BTreeMap<String, String>,
    pub left_out: Vec<weft_core::builds::LeftOutPackage>,
    pub worker_log: Option<String>,
    pub node_needs: weft_core::needs::NodeNeeds,
}

/// The project's running version as a run of it needs it: its identity,
/// and what its worker left out ([`ProgramCode`]).
#[derive(Debug, Clone)]
pub struct RunningProgram {
    pub identity: weft_core::project::hash::ProgramIdentity,
    pub left_out: Vec<weft_core::builds::LeftOutPackage>,
    pub worker_log: Option<String>,
    /// What each node type its worker holds needs from the machine.
    pub node_needs: weft_core::needs::NodeNeeds,
}

/// Backing store for project metadata, implemented by
/// `PostgresProjectStore`.
#[async_trait]
pub trait ProjectStoreOps: Send + Sync {
    /// Atomic register-and-hash-advance. Wraps every write of the
    /// register path (project row insert, project_definition history
    /// insert, running-hash pointer advance) in a single transaction
    /// so a process crash mid-sequence can't leave the project row
    /// partially advanced (the earlier shape ran each write
    /// standalone; a crash between the history insert and the
    /// pointer advance left the history row written but the pointer
    /// unmoved, and a `/run` between the two would have seen the OLD
    /// definition_hash with a fresh row already landed). All writes
    /// commit together or none do.
    ///
    /// `binary_hash`, `definition_hash`, and `infra_hash` are
    /// optional: a register with only some hashes set leaves the
    /// others unchanged.
    ///
    /// `has_infra` (derived from the definition via
    /// `weft_core::has_infra`) is stored on the row and refreshed on
    /// every register/sync, so it tracks edits that add or remove
    /// infra.
    /// `infra_image_tags`: the COMPLETE infra image-tag map to persist in the
    /// SAME transaction as the row + definition history + hashes (`None` leaves
    /// it untouched). A build that stamps the project runnable
    /// passes it here so the runnable stamp and the infra tags land together
    /// (never a runnable project with missing/half-written tags); plain
    /// registration (no build) passes `None`.
    ///
    /// `settles`: what a build's registration settles about the project's
    /// waiting version (`crate::build::waiting::Settles`), read and
    /// written under the project's version lock in this same transaction,
    /// so a registration and the waiting version's end commit together and
    /// an older version never lands over a newer one. `Ok(None)` only when
    /// it names a waiting version that no longer waits: nothing registered.
    #[allow(clippy::too_many_arguments)]
    async fn register_with_hashes(
        &self,
        project: ProjectDefinition,
        name: &str,
        description: &str,
        tenant_id: &str,
        binary_hash: Option<&str>,
        definition_hash: Option<&str>,
        infra_hash: Option<&str>,
        infra_image_tags: Option<&InfraImageTags>,
        code: Option<&ProgramCode>,
        source: Option<&RunningSource<'_>>,
        settles: Option<&crate::build::waiting::Settles<'_>>,
        infra_places: Option<&std::collections::BTreeMap<String, String>>,
    ) -> anyhow::Result<Option<StoredProjectSummary>>;

    /// The version the project's running build came from (its files and
    /// what they were compiled with), for the install to build the same
    /// version again. `None` when it runs no build, or one an
    /// older weft registered, which kept no inputs.
    async fn running_build(&self, id: uuid::Uuid) -> anyhow::Result<Option<weft_core::builds::VersionSource>>;

    /// Sources registered with this exact running program. Refuse a concurrent build.
    async fn program_source(&self, id: uuid::Uuid, program: &weft_core::project::hash::ProgramIdentity) -> anyhow::Result<weft_core::project::hash::Manifest>;

    /// Read graph and worker identity from one database snapshot.
    async fn running_program_identity(&self, id: uuid::Uuid) -> anyhow::Result<Option<weft_core::project::hash::ProgramIdentity>> {
        Ok(self.running_program(id).await?.map(|running| running.identity))
    }

    /// The running version's identity and what its worker left out
    /// ([`ProgramCode`]), from one database snapshot.
    async fn running_program(&self, id: uuid::Uuid) -> anyhow::Result<Option<RunningProgram>>;

    /// The program identity a run names by its two hashes (its
    /// `RunStarted`'s `definition_hash` and `binary_hash`): the
    /// implementation fingerprints are stored once per worker binary with
    /// the project, never copied into a run. `None` when this project
    /// never registered that binary.
    async fn program_identity(
        &self,
        id: uuid::Uuid,
        definition_hash: &str,
        binary_hash: &str,
    ) -> anyhow::Result<Option<weft_core::project::hash::ProgramIdentity>>;

    // Every reader returns `Result<Option<T>>` or `Result<Vec<T>>`:
    // `Ok(None)` / `Ok(vec![])` means "no such row" (legal), `Err`
    // means "DB failure" (callers MUST surface). Earlier this trait
    // returned bare `Option<T>` / `Vec<T>` / `bool`; transient DB
    // hiccups silently looked like "no rows" and led to wrong
    // decisions downstream (kill a healthy process, show "no projects",
    // etc).

    async fn tenant_for(&self, id: uuid::Uuid) -> anyhow::Result<Option<String>>;
    /// List the projects owned by `tenant`. Scoping is in the query (not a
    /// post-filter) so one tenant can never see another's projects or their
    /// count. Per-resource reads (`get`, `lifecycle`, ...) are authorized at
    /// the handler via `authenticator::authorize_project`; only the
    /// list-everything endpoint needs the tenant pushed into the store.
    async fn list(&self, tenant: &str) -> anyhow::Result<Vec<StoredProjectSummary>>;
    async fn get(&self, id: uuid::Uuid) -> anyhow::Result<Option<StoredProjectSummary>>;
    /// Mark the project as being removed, `weft rm`'s first write: no run
    /// of it is born from then on. `false` when there is no such row.
    async fn mark_removing(&self, id: uuid::Uuid) -> anyhow::Result<bool>;
    /// Remove the project and everything kept under it, in one
    /// transaction: its runs with everything of theirs, its recorded
    /// programs and version tree, its trigger setups and bakes, its
    /// signals, its tasks, its infra rows and infra lease, its instances'
    /// access. Answers the signals it removed, for
    /// the listener to let go of once this committed; `Ok(None)` = no such
    /// row (caller decides whether to 404). `Err` = DB failure, and
    /// nothing was removed.
    async fn remove(&self, id: uuid::Uuid) -> anyhow::Result<Option<Vec<crate::store::SignalRegistration>>>;
    async fn project(&self, id: uuid::Uuid) -> anyhow::Result<Option<ProjectDefinition>>;

    /// Read the stored binary hash. `Ok(None)` if never set
    /// (project registered but never built / activated /
    /// infra-started). `Err` ONLY on DB failure: a transient hiccup
    /// must NOT be observed as "no hash" (which would read every trigger
    /// on as current, and every run as on a build nobody registered).
    async fn running_binary_hash(&self, id: uuid::Uuid) -> anyhow::Result<Option<String>>;

    /// Read the stored definition hash. Same Result contract as
    /// `running_binary_hash`.
    async fn running_definition_hash(&self, id: uuid::Uuid) -> anyhow::Result<Option<String>>;

    /// The program project `id` recorded under `hash` (a registered
    /// version or a run's cut), read at the version it was written at;
    /// `Ok(None)` when no program with that hash was recorded. One that no
    /// longer reads is a `weft_core::stored::DecodeError` naming it, which
    /// a caller that shows runs tells apart from the store failing.
    /// Readers go through `DispatcherState::program`, which keeps what it
    /// read.
    async fn program(&self, id: uuid::Uuid, hash: &str) -> anyhow::Result<Option<ProjectDefinition>>;

    /// Read the stored infra hash. Same Result contract as
    /// `running_binary_hash`.
    async fn running_infra_hash(&self, id: uuid::Uuid) -> anyhow::Result<Option<String>>;

    /// The hash of each infra place's infra in the registered version
    /// `definition_hash` (`BuiltProgram::infra_places`); empty for a
    /// version registered without them.
    async fn infra_places(&self, id: uuid::Uuid, definition_hash: &str) -> anyhow::Result<std::collections::BTreeMap<String, String>>;

    /// The infra image tags the last build registered (empty before the
    /// first build, or for a program without infra). `Ok(None)` = no such
    /// project.
    async fn running_infra_image_tags(&self, id: uuid::Uuid) -> anyhow::Result<Option<InfraImageTags>>;

    /// The next number in the order of the project's build asks, taken by
    /// a build request as it arrives (`build_asks`): a version registers
    /// only over an older ask (`crate::build::waiting::Settles`). Errs on
    /// no such project.
    async fn next_build_ask(&self, id: uuid::Uuid) -> anyhow::Result<i64>;

    /// Read the project's verb-transition marker (a build or a resync,
    /// orthogonal to `status`). `Ok(None)` = no such project.
    async fn transition(&self, id: uuid::Uuid) -> anyhow::Result<Option<ProjectTransition>>;

    /// Single-flight entry into the `building` transition. Atomically
    /// flips `transition` to building IFF no request is starting builds
    /// for the project right now (the transition is none, or building held
    /// only by its waiting version or by a request gone: its heartbeat is
    /// older than `stale_before`), no resync is under way (its plan holds
    /// the transition until its task brought its triggers back or a
    /// person gave it up) AND the trigger lifecycle is not mid-flip
    /// (activating / deactivating). Stamps the transition heartbeat. Taking
    /// over from a request gone ends, in the same transaction, the starts
    /// it left waiting to be made on the builder
    /// (`crate::build::ledger::LostStarts::Of`), so this request never
    /// joins a start nothing will finish. A request entering while builds
    /// run joins them (the ledger finds them running), which is how a
    /// person who stopped following a build picks it up again.
    /// `Ok(false)` = lost: another request is starting builds, a cancel is
    /// under way, or the lifecycle is transitional; the caller rejects
    /// (409).
    async fn try_begin_building(&self, id: uuid::Uuid, stale_before: i64) -> anyhow::Result<bool>;

    /// Single-flight entry into the `resyncing` transition: flips
    /// `transition` to resyncing and writes down `plan` IFF the project's
    /// transition is at rest, the build it registered is the one `plan`
    /// names (a build registering meanwhile moved it) AND the trigger
    /// lifecycle is not mid-flip (activating / deactivating: a manual
    /// activate that passed its own checks first must not run under the
    /// resync), and in the same transaction enqueues `task`, the resync's
    /// durable driver under `plan.driver` (`crate::task_kinds::resync`).
    /// The plan holds the transition until the task brought every trigger
    /// back ([`Self::end_resync`]), or a person gave it up
    /// ([`Self::give_up_resync`]). Answers the task's id; `Ok(None)` =
    /// lost: a build or another resync is under way, the build moved, or a
    /// trigger is mid-flip.
    async fn try_begin_resyncing(
        &self,
        id: uuid::Uuid,
        plan: &crate::task_kinds::resync::ResyncPlan,
        task: weft_task_store::tasks::NewTask,
    ) -> anyhow::Result<Option<uuid::Uuid>>;

    /// The resync the project holds under way (`project.resync` while its
    /// transition is `resyncing`), and whether a task of its drive runs
    /// now (pending or claimed); `None` with none.
    async fn resync(&self, id: uuid::Uuid) -> anyhow::Result<Option<crate::task_kinds::resync::ResyncState>>;

    /// The project's resync as its drive `driver` reads it: the plan, and
    /// whether it is still under way (`false`: that drive ended it, and the
    /// plan is what it ended with). `None` when the project holds no plan
    /// of that drive (a person gave it up, or took it up again under
    /// another).
    async fn drive(&self, id: uuid::Uuid, driver: uuid::Uuid) -> anyhow::Result<Option<(crate::task_kinds::resync::ResyncPlan, bool)>>;

    /// Write `plan` over the project's resync under way, the one writer
    /// being the task of its drive (`plan.driver`). `Ok(false)` when that
    /// drive holds none any more.
    async fn write_resync(&self, id: uuid::Uuid, plan: &crate::task_kinds::resync::ResyncPlan) -> anyhow::Result<bool>;

    /// Take the project's resync under way up again, in one transaction:
    /// one that stopped under a new drive, its stop forgotten and a task
    /// of `tenant` enqueued for it (the stopped drive's task, still ending,
    /// writes nothing more); one going on, or whose task died without a
    /// stop, with the task of its drive, enqueued unless one runs. Answers
    /// the task that drives it; `Ok(None)` when the project holds no
    /// resync under way.
    async fn resume_resync(&self, id: uuid::Uuid, tenant: &str) -> anyhow::Result<Option<uuid::Uuid>>;

    /// The resyncs that stopped, with their plans: what
    /// `crate::task_kinds::resync::retry_stopped` reads. A held plan that
    /// does not read comes as its `Err`, beside the others, so one
    /// project's unreadable plan holds back no other's retry.
    async fn stopped_resyncs(&self) -> anyhow::Result<Vec<(uuid::Uuid, anyhow::Result<crate::task_kinds::resync::ResyncPlan>)>>;

    /// End the project's resync, its task under `driver` having brought
    /// every trigger back: the transition lands at rest in the same
    /// statement, and the plan stays as that drive's record (a run of its
    /// task again answers from it). `false` when that drive holds none.
    async fn end_resync(&self, id: uuid::Uuid, driver: uuid::Uuid) -> anyhow::Result<bool>;

    /// Give the project's resync under way up (a person's cancel), in one
    /// transaction: the triggers it took down and has not brought back are
    /// read off the activation rows (`crate::task_kinds::resync::left_down`)
    /// and written into its plan with the moment
    /// (`crate::task_kinds::resync::GivenUp`), and the transition lands at
    /// rest, so its task writes, takes down and turns on nothing more. The
    /// plan stays on the row as the given-up drive's record, which `weft
    /// status` shows ([`Self::given_up_resync`]) until the next resync
    /// begins. Answers the plan as given up; `None` when no resync is
    /// under way.
    async fn give_up_resync(&self, id: uuid::Uuid) -> anyhow::Result<Option<crate::task_kinds::resync::ResyncPlan>>;

    /// Give up the project's resync under way whose plan no longer reads,
    /// in one transaction: read again under the row lock, a plan that does
    /// not read is replaced by the record of its giving up, which keeps
    /// the document and why it did not read
    /// (`crate::task_kinds::resync::ResyncPlan::given_up_unreadable`), and
    /// the transition lands at rest. `weft status` shows it as
    /// [`Self::given_up_resync`] answers it. `false` when there was
    /// nothing to give up: no resync under way, or one that reads.
    async fn give_up_unreadable_resync(&self, id: uuid::Uuid) -> anyhow::Result<bool>;

    /// What the project's last resync left down, when a person gave it up
    /// ([`Self::give_up_resync`]), less the triggers activated or taken
    /// down since (`crate::activation_store`); `None` when it was not
    /// given up, or the project never resynced.
    async fn given_up_resync(&self, id: uuid::Uuid) -> anyhow::Result<Option<crate::task_kinds::resync::GivenUp>>;

    /// Request cancellation of the in-flight build: `transition` building
    /// (or cancelling_build already, so a cancel asked twice is the same
    /// cancel) → cancelling_build. From then on an image claim and a
    /// version written down as waiting are refused
    /// (`crate::build::ledger::refuse_once_cancelled`), and the cancel stops
    /// the builds already running and ends the waiting version itself.
    /// `Ok(false)` = no build in flight (already finished, or never
    /// started).
    async fn request_cancel_build(&self, id: uuid::Uuid) -> anyhow::Result<bool>;

    /// The request that entered the build transition is done with it
    /// (answered, or failed): from here it rests on the project's waiting
    /// version alone, so
    /// [`Self::settle_transition`] may clear it as soon as nothing else
    /// holds it. Stamps the heartbeat to zero, which no live driver ever
    /// leaves it at.
    async fn release_driver(&self, id: uuid::Uuid) -> anyhow::Result<()>;

    /// Land at rest (`transition` → none) every project in a transition
    /// that nothing holds any more ([`transition_held`]: for a build, no
    /// request drives it and no version of it waits; for a resync, its
    /// plan is forgotten). `project` narrows it to one. Answers the
    /// projects it landed, for the caller to announce. Called by the build
    /// loop, by a request letting go, by a registration, by a cancel, by a
    /// resync's task at its end, and by the
    /// `lapses` loop, so one definition of "held" holds everywhere.
    async fn settle_transition(&self, project: Option<uuid::Uuid>, stale_before: i64) -> anyhow::Result<Vec<uuid::Uuid>>;

    /// Bump the heartbeat of a transition still driven (never one
    /// released). Called on an interval by the process DRIVING it (a
    /// build request starting its builds) so the
    /// `lapses` loop only repairs transitions whose driver
    /// actually died.
    async fn bump_transition_heartbeat(&self, id: uuid::Uuid) -> anyhow::Result<()>;

    /// The project's own worker levers (`Ok(None)` when the project does
    /// not exist).
    async fn worker_overrides(&self, id: uuid::Uuid) -> anyhow::Result<Option<weft_platform_traits::WorkerOverrides>>;

    /// Replace the project's own worker levers; the project's queued runs
    /// start their tries again, the ones waiting to be tried taken at once
    /// (`crate::delivery`).
    async fn set_worker_overrides(&self, id: uuid::Uuid, overrides: &weft_platform_traits::WorkerOverrides) -> anyhow::Result<()>;

    /// Whether the project declares infrastructure. `Ok(Some(has_infra))`
    /// for any registered project; `Ok(None)` ONLY when the project
    /// doesn't exist; `Err` on DB failure.
    async fn project_has_infra(&self, id: uuid::Uuid) -> anyhow::Result<Option<bool>>;

    // NOTE: the infra image-tag map is written ONLY through
    // `register_with_hashes` (atomically alongside the running hashes), never
    // as a standalone per-node write, so a project can never be stamped
    // runnable with its infra tags missing/half-written. See that method.
    // The map has no per-node reader on the store: the supervisor reads it
    // through the broker, and the referenced-images keep-set reads the
    // whole column (api/project.rs), both via the canonical decode in
    // weft-broker-client.
}

/// The verb-transition marker on the project row (a build or a resync),
/// orthogonal to the trigger lifecycle (`status`). weft-core's, since the
/// status answer carries it.
pub use weft_core::projects::ProjectTransition;

/// Decode a `project.transition` column string. Unknown values are
/// schema drift and fail loud, mirroring `project_status_from_str`.
pub fn project_transition_from_str(s: &str) -> anyhow::Result<ProjectTransition> {
    ProjectTransition::parse(s)
        .ok_or_else(|| anyhow::anyhow!("unknown project.transition column value '{s}'"))
}

/// The SQL condition "a request drives the project row `p` through its
/// build transition right now": the row is in one and its heartbeat is no
/// older than `stale_before` (`crate::transition::heartbeat_stale_secs`).
/// The heartbeat beats while the request's starts run
/// (`crate::transition::ProjectBuildGate`), so a start-pending build of a
/// project not driven has lost its starter
/// (`crate::build::ledger::end_lost_starts`). One definition for every
/// reader, so a settle and the build loop never disagree about whether a
/// start is alive.
pub(crate) fn build_driven(p: &str, stale_before: &str) -> String {
    format!("({p}.transition IN ('building', 'cancelling_build') AND {p}.transition_heartbeat_unix >= {stale_before})")
}

/// The SQL condition "something holds the project row `p` in its
/// transition": for a build, the request that entered it still beats (the
/// starter of its builds) or a version of it waits on its builds
/// (`crate::build::waiting`); for a resync, its plan is written down
/// (`project.resync`), until its task brought every trigger back or a
/// person stopped it (`crate::task_kinds::resync`). What a settle reads.
pub(crate) fn transition_held(p: &str, stale_before: &str) -> String {
    format!(
        "(({p}.transition = 'resyncing' AND {p}.resync IS NOT NULL) \
          OR ({p}.transition IN ('building', 'cancelling_build') AND ({p}.transition_heartbeat_unix >= {stale_before} OR NOT {})))",
        held_by_driver_alone(p)
    )
}

/// Land at rest every project in a transition nothing holds any more
/// ([`transition_held`]): `$1` the oldest heartbeat still alive, `$2` one
/// project, or NULL for every one. Answers the ids it landed.
pub(crate) fn settle_transition_sql() -> String {
    format!(
        "UPDATE project p SET transition = 'none' \
         WHERE p.transition <> 'none' \
           AND NOT {} \
           AND ($2::UUID IS NULL OR p.id = $2) \
         RETURNING p.id",
        transition_held("p", "$1")
    )
}

/// The SQL condition "the project row `p` is in a transition only the
/// request driving it holds": a build no version of which waits. Once
/// that request's heartbeat is stale nothing holds it
/// ([`transition_held`]), and the `lapses` loop looks when the earliest of
/// these could (`crate::lapses::earliest_lapse`).
pub(crate) fn held_by_driver_alone(p: &str) -> String {
    format!(
        "({p}.transition IN ('building', 'cancelling_build') AND NOT {})",
        crate::build::waiting::has_waiting_version(&format!("{p}.id"))
    )
}

/// Cloneable handle to whatever the dispatcher uses as project
/// storage. The thing on `DispatcherState` is this, not the
/// concrete impl.
pub type ProjectStore = Arc<dyn ProjectStoreOps>;

#[derive(Clone)]
pub struct PostgresProjectStore {
    pool: Pool,
}

/// The wire-typed `ProjectStatus` (weft-core's): the dispatcher
/// reads and writes the same enum the broker, the supervisor and the
/// CLI see.
pub use weft_core::projects::ProjectStatus;

/// Decode a `project.status` column string into the typed enum.
/// Returns `Err` on unknown values rather than silently coercing
/// to `Registered` (the old `from_str` shape): a stray column
/// value is schema drift and must fail loud.
pub fn project_status_from_str(s: &str) -> anyhow::Result<ProjectStatus> {
    ProjectStatus::parse(s)
        .ok_or_else(|| anyhow::anyhow!("unknown project.status column value '{s}'"))
}

/// The source a registration records with the project's running build:
/// its files, and what they were compiled with (`None` for a version an
/// older weft asked for, which kept none).
#[derive(Debug, Clone, Copy)]
pub struct RunningSource<'a> {
    pub manifest: &'a weft_core::project::hash::Manifest,
    pub inputs: Option<&'a weft_core::builds::BuildInputs>,
}

#[derive(Debug, Clone)]
pub struct StoredProjectSummary {
    pub id: uuid::Uuid,
    pub name: String,
    pub description: String,
    /// `weft rm` marked it and has not finished removing it
    /// ([`ProjectStoreOps::mark_removing`]).
    pub removing: bool,
}

impl StoredProjectSummary {
    /// The project, unless `weft rm` marked it: then the refusal a verb on
    /// it answers.
    pub fn unless_removing(self) -> Result<Self, (axum::http::StatusCode, String)> {
        if self.removing {
            return Err(ProjectRemoving::refusal());
        }
        Ok(self)
    }
}

/// A write refused because `weft rm` marked the project and has not
/// finished removing it: what the caller is told, as a 409.
#[derive(Debug)]
pub struct ProjectRemoving;

impl std::fmt::Display for ProjectRemoving {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(weft_core::projects::PROJECT_BEING_REMOVED)
    }
}

impl std::error::Error for ProjectRemoving {}

impl ProjectRemoving {
    /// Whether `e` is (or wraps) this refusal.
    pub fn is(e: &anyhow::Error) -> bool {
        e.chain().any(|cause| cause.downcast_ref::<ProjectRemoving>().is_some())
    }

    /// The answer a verb on a project being removed gets.
    pub fn refusal() -> (axum::http::StatusCode, String) {
        (axum::http::StatusCode::CONFLICT, Self.to_string())
    }
}

impl PostgresProjectStore {
    /// Plain wrap; the schema is the boot's job (its one pass applies
    /// [`GROUP`] with every other group, so a stale `project` table fails
    /// the SAME boot error as its siblings).
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }
}

/// The project's resync under way, its row locked for the rest of the
/// caller's transaction; `None` when none is.
async fn held_resync(conn: &impl Client, id: uuid::Uuid) -> anyhow::Result<Option<ResyncPlan>> {
    let row: Option<(String, weft_core::stored::Version, Option<uuid::Uuid>)> = conn
        .opt(
            "SELECT resync, resync_version, resync_driver FROM project \
             WHERE id = $1 AND transition = 'resyncing' AND resync IS NOT NULL FOR UPDATE",
            &[&id],
        )
        .await?;
    row.map(|(json, version, driver)| ResyncPlan::stored(id, json.as_str(), version, driver)).transpose()
}

/// Project `id`'s registered program `hash`, read on `conn` (a caller's
/// transaction, or a connection of its own): `None` when it never
/// registered that program; a program that no longer reads is a
/// [`weft_core::stored::DecodeError`] naming its row.
pub(crate) async fn program_in(conn: &impl Client, id: uuid::Uuid, hash: &str) -> anyhow::Result<Option<ProjectDefinition>> {
    let row: Option<(String, weft_core::stored::Version)> = conn
        .opt(
            "SELECT project_json, project_json_version FROM project_definition \
             WHERE project_id = $1 AND definition_hash = $2",
            &[&id, &hash],
        )
        .await?;
    let Some((json, version)) = row else { return Ok(None) };
    Ok(Some(
        ProjectDefinition::STORED.decode(json.as_str(), version).map_err(|e| e.at(format_args!("project {id}'s program {hash}")))?,
    ))
}

/// What `project.resync`'s version 2 moved out of the JSON
/// (`ResyncPlan::STORED`): its drive, into `resync_driver`. Every row at
/// version 1 was written while the column was `JSONB`, so its text casts
/// back; the cast sits in the `SET`, which reads only the rows the
/// `WHERE` keeps (a later row's text may hold a NUL, which the cast
/// refuses).
pub const RESYNC_FILLS: &[weft_task_store::documents::Fill] = &[weft_task_store::documents::Fill {
    from: 1,
    sql: "UPDATE project SET resync_driver = (resync::jsonb->>'driver')::uuid WHERE resync_version = 1",
}];

/// The `project` + `project_definition` tables. The canonical CREATEs
/// live here, edited in place; an existing database is carried to them
/// by a migration written with `./setup.sh --migration <name>` (the
/// whole contract is `weft_task_store::schema_guard`'s header). The
/// boot applies this group with every other (`crate::app::DISPATCHER_GROUPS`).
pub static GROUP: weft_task_store::SchemaGroup = weft_task_store::SchemaGroup {
    name: "project",
    tables: &["project", "project_definition", "project_code"],
    // project: one row per registered project.
    //
    // Which triggers listen, and how the ones that stopped went down,
    // is per trigger per owner in `trigger_activation`
    // (`crate::activation_store`).
    //
    // running_binary_hash / running_definition_hash /
    // running_infra_hash drive drift detection + image tagging:
    //   - running_binary_hash: names the worker program (its image's
    //     tag in the cloud, its folder on a machine). Flips on engine /
    //     node-impl / node-type-set / weft.toml edits; selects the
    //     program when starting a fresh worker.
    //   - running_definition_hash: identifies the runtime project
    //     shape (topology + configs). Workers fetch the
    //     definition at run claim time keyed by
    //     `(project_id, definition_hash)`.
    //   - running_infra_hash: drives the Upgrade button when the
    //     CLI's freshly-computed infra hash drifts.
    //
    // tenant_id pins each project to its tenant. The broker scopes
    // every worker's request by it: a worker proves which project it
    // is, and every project_id it references must resolve to the same
    // tenant.
    ddl: &[
        r#"CREATE TABLE IF NOT EXISTS project (
                id UUID PRIMARY KEY,
                name TEXT NOT NULL,
                -- Free-text project description (metadata only; never affects
                -- the graph, build, or runtime). Set at create time on the
                -- website; empty string when unset (NOT NULL keeps reads simple).
                description TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL,
                project_json TEXT NOT NULL,
                -- The version `project_json` was written at
                -- (`ProjectDefinition::STORED`).
                project_json_version INTEGER NOT NULL,
                updated_at BIGINT NOT NULL,
                running_binary_hash TEXT,
                running_definition_hash TEXT,
                running_infra_hash TEXT,
                running_source JSONB,
                -- What the running build's files were compiled with
                -- (weft_core::builds::BuildInputs), written with
                -- `running_source`: with the name and those files, the
                -- request the build came from, which the maintenance lift
                -- builds again on a newer weft (`crate::maintenance`). NULL
                -- for a build an older weft registered, which kept none.
                -- As its JSON text (`db::JsonText`): a text asset is
                -- inlined in it, whatever the file or the page held.
                running_build_inputs TEXT,
                -- The version `running_build_inputs` was written at
                -- (`BuildInputs::STORED`).
                running_build_inputs_version INTEGER NOT NULL,
                tenant_id TEXT NOT NULL,
                -- Whether this project DECLARES infrastructure (any node
                -- with requires_infra). Derived from the definition and
                -- refreshed on every register/sync, so it tracks edits
                -- that add or remove infra.
                has_infra BOOLEAN NOT NULL DEFAULT FALSE,
                -- Per-(project, node) image hash maps for Image::Local
                -- references in InfraSpecs. CLI ships these in /sync;
                -- supervisor reads them.
                -- Shape: { "<node_id>": { "<image_name>": "<tag>" } }, as
                -- its JSON text (`db::JsonText`): the image names are the
                -- ones the node's spec wrote.
                infra_image_tags_json TEXT NOT NULL DEFAULT '{}',
                -- The project's own worker levers, each one it sets
                -- replacing the install's (`WorkerOverrides`); empty
                -- runs on the install's. As its JSON text (`db::JsonText`):
                -- a person writes them.
                worker_settings_json TEXT NOT NULL DEFAULT '{}',
                -- Verb-transition marker, orthogonal to `status`:
                -- 'none' | 'building' | 'cancelling_build' | 'resyncing'.
                -- Written only by its own single-flight methods
                -- (try_begin_building / try_begin_resyncing /
                -- request_cancel_build / settle_transition), never by
                -- lifecycle writes, so a deactivate can't stomp an
                -- in-flight marker. A build's is held by the request
                -- starting the builds, then by the project's waiting
                -- version until it ends; a resync's by its plan (`resync`).
                transition TEXT NOT NULL DEFAULT 'none',
                -- Heartbeat for a build transition: the request starting
                -- its builds bumps this on an interval and zeroes it when it
                -- lets go; once it is stale, only a waiting version holds
                -- it (settle_transition).
                transition_heartbeat_unix BIGINT NOT NULL DEFAULT 0,
                -- The project's resync (`crate::task_kinds::resync::ResyncPlan`):
                -- the drive under way, whose triggers it takes down and
                -- brings back, with the person's take-down choice and the
                -- build it brings live, the owners it already brought
                -- back, and why it stopped when it did. Under way while
                -- `transition` is 'resyncing'; once its task ended it, or a
                -- person gave it up (`given_up`: when, and the triggers it
                -- left down, which `weft status` shows), kept as that
                -- drive's record until the next resync. Its drive is never in
                -- it (it lives in `resync_driver`). As its JSON text
                -- (`db::JsonText`): why it stopped quotes the failure.
                resync TEXT,
                -- The version `resync` was written at (`ResyncPlan::STORED`).
                resync_version INTEGER NOT NULL,
                -- Its drive (`ResyncPlan::driver`): what every write of the
                -- plan names, and the key of the task driving it.
                resync_driver UUID,
                -- The project's own port on a local install
                -- (`crate::project_ports`): given at its first activation,
                -- or named with `weft activate --port`, and kept, moved to a free one when another program took
                -- it. NULL until then, and on a platform that gives
                -- projects no port.
                api_port INTEGER,
                -- Where the project's front answers its callers, or why it
                -- cannot (`weft_core::projects::ProjectAddress`, written by
                -- `crate::front`): what `weft status` shows and where the
                -- listener hands the project's events. NULL while nothing
                -- of the project takes work. As its JSON text
                -- (`db::JsonText`): why it cannot quotes the failure.
                api_address TEXT,
                -- The order of the project's build asks: each build request
                -- takes the next one as it arrives, before it compiles
                -- (`next_build_ask`), from this row, so every replica
                -- agrees on it.
                build_asks BIGINT NOT NULL DEFAULT 0,
                -- The ask whose version the project runs: a registration
                -- lands only over an older ask
                -- (`crate::build::waiting::settle`).
                registered_ask BIGINT NOT NULL DEFAULT 0,
                -- `weft rm` started on it (`mark_removing`, its first
                -- write): no run of it is born from then on (the run
                -- births check it in the statement that inserts the run,
                -- `weft_record_batch` and `weft_journal::record::insert_queued_in`),
                -- so the take-down that follows reaches every run it has.
                removing BOOLEAN NOT NULL DEFAULT FALSE
            )"#,
        "CREATE INDEX IF NOT EXISTS idx_project_tenant ON project(tenant_id)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_project_api_port ON project(api_port) WHERE api_port IS NOT NULL",
        // A project coming or going changes how its tenant's routes read
        // (a route whose project is gone is inactive); the function is the
        // run group's (`crate::store::postgres::GROUP`), which applies first.
        r#"DROP TRIGGER IF EXISTS project_routes_on_row ON project"#,
        r#"CREATE TRIGGER project_routes_on_row
            AFTER INSERT OR DELETE ON project
            FOR EACH ROW
            EXECUTE FUNCTION routes_notify_tenant()"#,
        // Tell every dispatcher a project's worker levers changed (or the
        // project went), so its copy of them (`crate::held::Held`) is read
        // again.
        // SYNC: 'weft_worker_settings' <-> crate::held::WORKER_SETTINGS_CHANNEL
        r#"CREATE OR REPLACE FUNCTION project_worker_settings_notify() RETURNS trigger AS $$
            BEGIN
                PERFORM pg_notify('weft_worker_settings', OLD.id::text);
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql"#,
        r#"DROP TRIGGER IF EXISTS project_worker_settings_on_change ON project"#,
        r#"CREATE TRIGGER project_worker_settings_on_change
            AFTER UPDATE OF worker_settings_json ON project
            FOR EACH ROW
            WHEN (NEW.worker_settings_json IS DISTINCT FROM OLD.worker_settings_json)
            EXECUTE FUNCTION project_worker_settings_notify()"#,
        r#"DROP TRIGGER IF EXISTS project_worker_settings_on_delete ON project"#,
        r#"CREATE TRIGGER project_worker_settings_on_delete
            AFTER DELETE ON project
            FOR EACH ROW
            EXECUTE FUNCTION project_worker_settings_notify()"#,
        // A project registered again may declare other infra (a node gone,
        // a node now one copy per instance), and which handles the broker
        // answers for follows what it declares: tell whoever keeps an
        // answer about the project's infra (a worker's addresses) to drop it.
        // SYNC: 'weft_infra_status' <-> weft_broker_client::line::INFRA_STATUS_CHANNEL, crates/weft-dispatcher/src/infra_node.rs (infra_node_status_notify), crates/weft-broker/src/line.rs (audience)
        r#"CREATE OR REPLACE FUNCTION project_declared_infra_notify() RETURNS trigger AS $$
            BEGIN
                PERFORM pg_notify('weft_infra_status', NEW.id::text);
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql"#,
        r#"DROP TRIGGER IF EXISTS project_declared_infra_on_change ON project"#,
        r#"CREATE TRIGGER project_declared_infra_on_change
            AFTER UPDATE OF project_json ON project
            FOR EACH ROW
            WHEN (NEW.project_json IS DISTINCT FROM OLD.project_json)
            EXECUTE FUNCTION project_declared_infra_notify()"#,
        // Append-only definition-version history. Workers fetch by
        // (project_id, definition_hash) so a suspended run
        // can always resume on the EXACT shape it was started on,
        // even after the user has edited and re-registered. Without
        // this, the `project.project_json` column would only carry
        // the LATEST shape and a resume after edit would run the
        // wrong topology against the journal's old state.
        //
        // A project's source lives as a folder on disk, edited via the
        // CLI / VS Code; the dispatcher tracks no version chain for it.
        //
        // NO boot-time status touch-up here. Recovery of a project
        // interrupted mid-transition (a process died while activating /
        // building / deactivating) is the `lapses` loop's
        // job (`crate::lapses`): per-project,
        // heartbeat-gated, and status-guarded, so it never wipes
        // another process's live state. A constructor-time bulk downgrade
        // would run in EVERY replica on EVERY boot and reset live
        // status for all tenants (a multi-process correctness bug).
        r#"CREATE TABLE IF NOT EXISTS project_code (
            project_id UUID NOT NULL REFERENCES project(id) ON DELETE CASCADE,
            binary_hash TEXT NOT NULL,
            implementations JSONB NOT NULL,
            -- The node packages the worker's build left out because they
            -- did not compile (weft_core::builds::LeftOutPackage list):
            -- a run whose cut uses one of their types is refused. As its
            -- JSON text (`db::JsonText`): each quotes the compiler.
            left_out TEXT NOT NULL DEFAULT '[]',
            -- Where that build's log is read, for their full errors.
            worker_log TEXT,
            -- What each node type the worker holds needs from the machine
            -- (weft_core::needs::NodeNeeds): a program using one whose
            -- need is unmet is refused before it starts. As its JSON text
            -- (`db::JsonText`): each node's metadata writes its own.
            node_needs TEXT NOT NULL DEFAULT '{}',
            PRIMARY KEY (project_id, binary_hash)
        )"#,
        r#"CREATE TABLE IF NOT EXISTS project_definition (
            project_id UUID NOT NULL,
            definition_hash TEXT NOT NULL,
            project_json TEXT NOT NULL,
            -- The version `project_json` was written at
            -- (`ProjectDefinition::STORED`).
            project_json_version INTEGER NOT NULL,
            recorded_at_unix BIGINT NOT NULL,
            -- For a cut a run runs (`weft_journal::record::RunsProgram`),
            -- the registered program it was cut from; NULL for a version
            -- a build registered. A cut is no version: the running hashes
            -- never point at one (`advance_running_hashes`).
            cut_from TEXT,
            -- For a version a build registered, `place -> hash` of each
            -- infra place's own infra (`BuiltProgram::infra_places`): what
            -- an apply of that place stamps on its copy, and what tells a
            -- copy behind. NULL for a cut, and for a build that named none.
            infra_places JSONB,
            PRIMARY KEY (project_id, definition_hash)
            -- No foreign key to `project`: the project's removal erases
            -- its runs first and drops these in its own transaction
            -- (`ProjectStoreOps::remove`), so no run is ever left without
            -- the program it ran against.
        )"#,
    ],
    seed: &[],
    data_steps: &[
        // A project's head moved off its row onto its working copies
        // (`crate::working_copies`): the head it had becomes its program
        // copy's, with that version's files as the copy's working files,
        // so the next run seeds and parents where the last one left off.
        // The folder a person works in registers itself on its next
        // command and carries on from there.
        weft_task_store::DataStep {
            table: "project",
            until: weft_task_store::Until::Dropped("head_version"),
            work: weft_task_store::StepWork::Sql(
                "INSERT INTO working_copy (id, project_id, kind, head_version, head_run, working_manifest, synced, conflicts, revision, created_at, updated_at) \
                  SELECT gen_random_uuid(), p.id, 'program', p.head_version, p.head_run, \
                         COALESCE((SELECT v.manifest FROM project_version v WHERE v.project_id = p.id AND v.id = p.head_version), '{}'::jsonb), \
                         '{}'::jsonb, '{}', 0, EXTRACT(EPOCH FROM now())::bigint, EXTRACT(EPOCH FROM now())::bigint \
                  FROM project p WHERE p.head_version IS NOT NULL OR p.head_run IS NOT NULL \
                  ON CONFLICT DO NOTHING",
            ),
        },
    ],
};

/// THE running-hash pointer advance: ONE atomic UPDATE of the trio of hashes
/// PLUS the complete infra image-tag map, with the definition-history EXISTS
/// guard. Its one writer is `register_with_hashes`, inside its transaction
/// (after the history INSERT, which the same-snapshot EXISTS check then sees). A `None` argument
/// leaves that field untouched; `Some(tags)` REPLACES the whole infra tag map.
/// Folding the tags into this one statement is what guarantees a project is
/// never observed stamped runnable (new binary hash) with its infra tags absent
/// or half-written. Zero rows updated fails loudly: either the project row is
/// missing, or the definition hash has no history row a build registered (the
/// project must be registered with that definition first; a cut a run keeps
/// there, marked `cut_from`, is no registered version).
async fn advance_running_hashes(
    conn: &impl Client,
    id: uuid::Uuid,
    binary_hash: Option<&str>,
    definition_hash: Option<&str>,
    infra_hash: Option<&str>,
    infra_image_tags: Option<&InfraImageTags>,
) -> anyhow::Result<()> {
    if binary_hash.is_none()
        && definition_hash.is_none()
        && infra_hash.is_none()
        && infra_image_tags.is_none()
    {
        return Ok(());
    }
    // Serialize the tag map to JSON once (NULL when not being written, so the
    // COALESCE leaves the stored column untouched).
    let tags_json = infra_image_tags.map(db::JsonText);
    let rows = conn
        .execute(
            "UPDATE project SET \
                 running_source = CASE WHEN ($1::text IS NOT NULL AND running_binary_hash IS DISTINCT FROM $1) \
                    OR ($2::text IS NOT NULL AND running_definition_hash IS DISTINCT FROM $2) THEN NULL ELSE running_source END, \
                 running_build_inputs = CASE WHEN ($1 IS NOT NULL AND running_binary_hash IS DISTINCT FROM $1) \
                    OR ($2 IS NOT NULL AND running_definition_hash IS DISTINCT FROM $2) THEN NULL ELSE running_build_inputs END, \
                 running_binary_hash     = COALESCE($1, running_binary_hash), \
                 running_definition_hash = COALESCE($2, running_definition_hash), \
                 running_infra_hash      = COALESCE($3, running_infra_hash), \
                 infra_image_tags_json   = COALESCE($6, infra_image_tags_json), \
                 updated_at = $4 \
             WHERE id = $5 AND ($2::text IS NULL OR EXISTS ( \
                 SELECT 1 FROM project_definition \
                 WHERE project_id = $5 AND definition_hash = $2 AND cut_from IS NULL \
             ))",
            &[&binary_hash, &definition_hash, &infra_hash, &crate::lease::now_unix(), &id, &tags_json],
        )
        .await?;
    if rows == 0 {
        let project_exists: bool = conn.scalar("SELECT EXISTS(SELECT 1 FROM project WHERE id = $1)", &[&id]).await?;
        if !project_exists {
            anyhow::bail!("advance_running_hashes: project {id} not found");
        }
        anyhow::bail!(
            "refuse to set running_definition_hash to {hash} for project {id}: \
             no registered project_definition history row exists for that hash; \
             register the project with this definition first",
            hash = definition_hash.unwrap_or("<none>"),
        );
    }
    Ok(())
}

#[async_trait]
impl ProjectStoreOps for PostgresProjectStore {
    async fn register_with_hashes(
        &self,
        project: ProjectDefinition,
        name: &str,
        description: &str,
        tenant_id: &str,
        binary_hash: Option<&str>,
        definition_hash: Option<&str>,
        infra_hash: Option<&str>,
        infra_image_tags: Option<&InfraImageTags>,
        code: Option<&ProgramCode>,
        source: Option<&RunningSource<'_>>,
        settles: Option<&crate::build::waiting::Settles<'_>>,
        infra_places: Option<&std::collections::BTreeMap<String, String>>,
    ) -> anyhow::Result<Option<StoredProjectSummary>> {
        let id = project.id;
        let name = name.to_string();
        let description = description.to_string();
        // Derived from the definition and refreshed on every register/
        // sync so it tracks edits that add or remove infra. The conflict
        // arm below re-derives it from EXCLUDED, so a re-register that
        // adds (or drops) an infra node flips placement.
        let has_infra = weft_core::has_infra(&project);
        let project_json = serde_json::to_string(&project)?;
        let mut conn = self.pool.get().await?;
        let tx = conn.transaction().await?;
        let now = crate::lease::now_unix();
        if let Some(settles) = settles {
            if !crate::build::waiting::settle(&tx, id, settles, now).await? {
                tx.commit().await?;
                return Ok(None);
            }
        }
        // The conflict arm is GUARDED by `WHERE project.tenant_id =
        // EXCLUDED.tenant_id`: a re-register may only update a row that already
        // belongs to the same tenant. Without this guard the upsert would let
        // any tenant re-register an existing project id and overwrite its
        // `tenant_id`, seizing another tenant's project. When the tenants
        // differ the UPDATE matches no row and (because the id already exists,
        // so the INSERT is suppressed) the statement returns NO row; we detect
        // that and fail loudly as a cross-tenant collision. A project `weft
        // rm` marked is not registered over either (`ProjectRemoving`).
        let registered: Option<uuid::Uuid> = tx
            .scalar_opt(
                "INSERT INTO project \
                    (id, name, description, status, project_json, updated_at, \
                     tenant_id, has_infra, project_json_version, resync_version, running_build_inputs_version) \
                 VALUES ($1, $2, $3, 'registered', $4, $5, $6, $7, $8, $9, $10) \
                 ON CONFLICT (id) DO UPDATE SET \
                    name = EXCLUDED.name, \
                    description = EXCLUDED.description, \
                    project_json = EXCLUDED.project_json, \
                    project_json_version = EXCLUDED.project_json_version, \
                    updated_at = EXCLUDED.updated_at, \
                    has_infra = EXCLUDED.has_infra \
                 WHERE project.tenant_id = EXCLUDED.tenant_id AND NOT project.removing \
                 RETURNING id",
                &[
                    &id,
                    &name,
                    &description,
                    &project_json,
                    &now,
                    &tenant_id,
                    &has_infra,
                    &ProjectDefinition::STORED.version(),
                    &ResyncPlan::STORED.version(),
                    &weft_core::builds::BuildInputs::STORED.version(),
                ],
            )
            .await?;
        if registered.is_none() {
            let removing: Option<bool> = tx.scalar_opt("SELECT removing FROM project WHERE id = $1", &[&id]).await?;
            if removing == Some(true) {
                return Err(ProjectRemoving.into());
            }
            anyhow::bail!("project {id} already exists under a different tenant; register refused (cross-tenant id collision)");
        }
        if let Some(hash) = definition_hash {
            // History row FIRST (idempotent on (project_id, hash)),
            // then pointer advance. Inside the transaction the FK
            // precondition is satisfied as of the same snapshot, so
            // the pointer-setter's existence check sees the freshly
            // inserted history row. A cut some run kept under the same
            // hash is the same program, and is now a registered version
            // too.
            tx.execute(
                "INSERT INTO project_definition (project_id, definition_hash, project_json, project_json_version, recorded_at_unix, infra_places) \
                 VALUES ($1, $2, $3, $6, $4, $5) \
                 ON CONFLICT (project_id, definition_hash) DO UPDATE SET cut_from = NULL, \
                     infra_places = COALESCE(EXCLUDED.infra_places, project_definition.infra_places)",
                &[&id, &hash, &project_json, &now, &infra_places.map(|places| db::Json(db::WeftMade(places))), &ProjectDefinition::STORED.version()],
            )
            .await?;
        }
        if let Some(code) = code {
            let binary = binary_hash.ok_or_else(|| anyhow::anyhow!("implementation fingerprints require a worker binary hash"))?;
            let encoded = db::Json(db::WeftMade(&code.implementations));
            // What the worker left out is its image's (one content, one
            // build's record), so a registration of the same worker again
            // says the same; it is taken as the latest registration says.
            let inserted = tx
                .execute(
                    "INSERT INTO project_code (project_id, binary_hash, implementations, left_out, worker_log, node_needs) VALUES ($1, $2, $3, $4, $5, $6) \
                     ON CONFLICT (project_id, binary_hash) DO UPDATE SET implementations = EXCLUDED.implementations, \
                     left_out = EXCLUDED.left_out, worker_log = EXCLUDED.worker_log, node_needs = EXCLUDED.node_needs \
                     WHERE project_code.implementations = EXCLUDED.implementations",
                    &[
                        &id,
                        &binary,
                        &encoded,
                        &db::JsonText(&code.left_out),
                        &code.worker_log,
                        &db::JsonText(&code.node_needs),
                    ],
                )
                .await?;
            anyhow::ensure!(inserted == 1, "worker hash {binary} already has different implementation fingerprints");
        }
        // Runnable stamp + infra tags in ONE statement inside this same tx, so a
        // build that registers-and-stamps never leaves a runnable
        // project with missing/half-written infra tags. Plain registration
        // passes `infra_image_tags = None`.
        advance_running_hashes(
            &tx,
            id,
            binary_hash,
            definition_hash,
            infra_hash,
            infra_image_tags,
        )
        .await?;
        if binary_hash.is_some() || definition_hash.is_some() {
            tx.execute(
                "UPDATE project SET running_source = $2, running_build_inputs = $3, running_build_inputs_version = $4 WHERE id = $1",
                &[
                    &id,
                    &source.map(|source| db::Json(db::WeftMade(source.manifest))),
                    &source.and_then(|source| source.inputs).map(db::JsonText),
                    &weft_core::builds::BuildInputs::STORED.version(),
                ],
            )
            .await?;
        }
        if binary_hash.is_some() {
            // A new image of the project: its queued runs whose workers
            // could not be reached or kept answering busy are tried again.
            crate::delivery::retry_stopped(&tx, id).await?;
        }
        tx.commit().await?;
        Ok(Some(StoredProjectSummary { id, name, description, removing: false }))
    }

    async fn running_build(&self, id: uuid::Uuid) -> anyhow::Result<Option<weft_core::builds::VersionSource>> {
        let row: Option<(Option<db::Json<serde_json::Value>>, Option<String>, weft_core::stored::Version)> = self
            .pool
            .opt("SELECT running_source, running_build_inputs, running_build_inputs_version FROM project WHERE id = $1", &[&id])
            .await?;
        let Some((Some(db::Json(manifest)), Some(inputs), version)) = row else { return Ok(None) };
        let inputs = weft_core::builds::BuildInputs::STORED
            .decode(inputs.as_str(), version)
            .map_err(|e| e.at(format_args!("project {id}'s running build")))?;
        let manifest = serde_json::from_value(manifest).map_err(|e| anyhow::anyhow!("project {id}'s running source does not read: {e}"))?;
        Ok(Some(weft_core::builds::VersionSource { manifest, inputs }))
    }

    async fn program_source(&self, id: uuid::Uuid, program: &weft_core::project::hash::ProgramIdentity) -> anyhow::Result<weft_core::project::hash::Manifest> {
        let row: Option<db::Json<serde_json::Value>> = self
            .pool
            .scalar_opt(
                "SELECT running_source FROM project WHERE id = $1 AND running_binary_hash = $2 AND running_definition_hash = $3 AND running_source IS NOT NULL",
                &[&id, &program.binary_hash, &program.definition_hash],
            )
            .await?;
        let db::Json(source) = row.ok_or_else(|| anyhow::anyhow!("project sources are missing or changed during preparation; register the project and retry"))?;
        Ok(serde_json::from_value(source)?)
    }

    async fn running_program(&self, id: uuid::Uuid) -> anyhow::Result<Option<RunningProgram>> {
        #[allow(clippy::type_complexity)]
        let row: Option<(String, String, db::Json<serde_json::Value>, db::JsonText<Vec<weft_core::builds::LeftOutPackage>>, Option<String>, db::JsonText<weft_core::needs::NodeNeeds>)> = self
            .pool
            .opt(
                "SELECT p.running_definition_hash, p.running_binary_hash, c.implementations, c.left_out, c.worker_log, c.node_needs \
                 FROM project p JOIN project_code c ON c.project_id = p.id AND c.binary_hash = p.running_binary_hash \
                 WHERE p.id = $1 AND p.running_definition_hash IS NOT NULL",
                &[&id],
            )
            .await?;
        row.map(|(definition_hash, binary_hash, implementations, left_out, worker_log, node_needs)| Ok(RunningProgram {
            identity: weft_core::project::hash::ProgramIdentity {
                definition_hash, binary_hash, implementations: serde_json::from_value(implementations.0)?,
            },
            left_out: left_out.0,
            worker_log,
            node_needs: node_needs.0,
        })).transpose()
    }

    async fn program_identity(
        &self,
        id: uuid::Uuid,
        definition_hash: &str,
        binary_hash: &str,
    ) -> anyhow::Result<Option<weft_core::project::hash::ProgramIdentity>> {
        let row: Option<db::Json<serde_json::Value>> = self
            .pool
            .scalar_opt("SELECT implementations FROM project_code WHERE project_id = $1 AND binary_hash = $2", &[&id, &binary_hash])
            .await?;
        row.map(|db::Json(implementations)| {
            Ok(weft_core::project::hash::ProgramIdentity {
                definition_hash: definition_hash.to_string(),
                binary_hash: binary_hash.to_string(),
                implementations: serde_json::from_value(implementations)?,
            })
        })
        .transpose()
    }

    async fn tenant_for(&self, id: uuid::Uuid) -> anyhow::Result<Option<String>> {
        Ok(self.pool.scalar_opt("SELECT tenant_id FROM project WHERE id = $1", &[&id]).await?)
    }

    async fn list(&self, tenant: &str) -> anyhow::Result<Vec<StoredProjectSummary>> {
        let rows: Vec<(uuid::Uuid, String, String, bool)> = self
            .pool
            .all("SELECT id, name, description, removing FROM project WHERE tenant_id = $1 ORDER BY name", &[&tenant])
            .await?;
        Ok(rows
            .into_iter()
            .map(|(id, name, description, removing)| StoredProjectSummary { id, name, description, removing })
            .collect())
    }

    async fn get(&self, id: uuid::Uuid) -> anyhow::Result<Option<StoredProjectSummary>> {
        let row: Option<(uuid::Uuid, String, String, bool)> =
            self.pool.opt("SELECT id, name, description, removing FROM project WHERE id = $1", &[&id]).await?;
        Ok(row.map(|(id, name, description, removing)| StoredProjectSummary { id, name, description, removing }))
    }

    async fn mark_removing(&self, id: uuid::Uuid) -> anyhow::Result<bool> {
        let marked = self.pool.execute("UPDATE project SET removing = TRUE WHERE id = $1", &[&id]).await?;
        Ok(marked > 0)
    }

    async fn remove(&self, id: uuid::Uuid) -> anyhow::Result<Option<Vec<crate::store::SignalRegistration>>> {
        let mut conn = self.pool.get().await?;
        let tx = conn.transaction().await?;
        if tx.execute("DELETE FROM project WHERE id = $1", &[&id]).await? == 0 {
            return Ok(None);
        }
        // Its runs, every event and every recorded value, so the space
        // comes back: what a person keeps of a removed project is their
        // files on disk. Under the project row's removal, in its
        // transaction: no run was born since the project was marked
        // removing, and none is born after this commits
        // (`weft_record_batch`, `weft_journal::record::insert_queued_in`
        // refuse both), so none outlives the project.
        let (erased, mut signals) = crate::store::postgres::erase_project_runs(&tx, id).await?;
        tracing::info!(target: "weft_dispatcher::project", project_id = %id, runs = erased, "erasing the project's runs");
        // What the project's runs needed and its triggers ran on: nothing
        // reads them any more. Work queued for it goes too, a task being
        // run included (its claimant finds its claim gone and stands down),
        // except the last calls its cancelled runs queued: those tell
        // outside services to stop working and charging for runs that are
        // gone now. `weft rm` waits for the ones queued before it starts
        // deleting (`crate::api::project::remove`); the ones queued here,
        // for the runs this removal cuts short (`erase_project_runs`), go
        // out with whatever of what they need is still there when they are
        // sent (an author's connection stays; an instance's goes with the
        // project right after this, and a call sent later fails, logged).
        signals.extend(crate::store::postgres::remove_project_signals(&tx, id).await?);
        tx.execute(
            "DELETE FROM task WHERE project_id = $1 AND kind <> $2",
            &[&id, &weft_task_store::TaskKind::CancelCall.as_str()],
        )
        .await?;
        weft_task_store::unanswered::forget_project(&tx, id).await?;
        tx.execute("DELETE FROM project_definition WHERE project_id = $1", &[&id]).await?;
        crate::infra_owner::release_project(&tx, id).await?;
        tx.execute("DELETE FROM trigger_setup WHERE project_id = $1", &[&id]).await?;
        tx.execute("DELETE FROM trigger_bake WHERE project_id = $1", &[&id]).await?;
        // The version tree goes with the project (its runs cascade):
        // versions left behind kept naming stored files for a project
        // that no longer existed, and the same id registered again
        // inherited a tree it never made.
        tx.execute("DELETE FROM project_version WHERE project_id = $1", &[&id]).await?;
        tx.execute("DELETE FROM version_runs WHERE project_id = $1", &[&id]).await?;
        tx.execute("DELETE FROM working_copy WHERE project_id = $1", &[&id]).await?;
        // The infra rows go in the same transaction, AFTER the project
        // row: the broker's command and event inserts lock the project
        // row they read (`FOR KEY SHARE`), so an insert racing this
        // either commits first and is deleted below, or waits for this
        // commit and then finds no project to insert for.
        crate::infra_node::remove_project(&tx, id).await?;
        crate::infra_event::remove_project(&tx, id).await?;
        crate::infra_lifecycle_command::remove_project(&tx, id).await?;
        // An instance lives inside this project only: its connections,
        // picks and tokens reach nothing once the project is gone, and
        // nobody could list them to delete them.
        weft_access_store::forget_project_access(&tx, id).await?;
        crate::store::postgres::revoke_project_instance_tokens(&tx, id).await?;
        tx.commit().await?;
        Ok(Some(signals))
    }


    async fn running_binary_hash(&self, id: uuid::Uuid) -> anyhow::Result<Option<String>> {
        let row: Option<Option<String>> = self.pool.scalar_opt("SELECT running_binary_hash FROM project WHERE id = $1", &[&id]).await?;
        Ok(row.flatten())
    }

    async fn running_definition_hash(&self, id: uuid::Uuid) -> anyhow::Result<Option<String>> {
        let row: Option<Option<String>> =
            self.pool.scalar_opt("SELECT running_definition_hash FROM project WHERE id = $1", &[&id]).await?;
        Ok(row.flatten())
    }

    async fn program(&self, id: uuid::Uuid, hash: &str) -> anyhow::Result<Option<ProjectDefinition>> {
        program_in(&self.pool, id, hash).await
    }

    async fn infra_places(&self, id: uuid::Uuid, definition_hash: &str) -> anyhow::Result<std::collections::BTreeMap<String, String>> {
        let row: Option<Option<db::Json<std::collections::BTreeMap<String, String>>>> = self
            .pool
            .scalar_opt(
                "SELECT infra_places FROM project_definition WHERE project_id = $1 AND definition_hash = $2",
                &[&id, &definition_hash],
            )
            .await?;
        Ok(row.flatten().map(|places| places.0).unwrap_or_default())
    }

    async fn running_infra_hash(&self, id: uuid::Uuid) -> anyhow::Result<Option<String>> {
        let row: Option<Option<String>> = self.pool.scalar_opt("SELECT running_infra_hash FROM project WHERE id = $1", &[&id]).await?;
        Ok(row.flatten())
    }

    async fn running_infra_image_tags(&self, id: uuid::Uuid) -> anyhow::Result<Option<InfraImageTags>> {
        let row: Option<db::JsonText<serde_json::Value>> =
            self.pool.scalar_opt("SELECT infra_image_tags_json FROM project WHERE id = $1", &[&id]).await?;
        row.map(|db::JsonText(value)| {
            let tags = weft_broker_client::protocol::decode_infra_image_tags(value, &format!("project {id}"))?;
            Ok(tags.into_iter().map(|(place, images)| (place, images.into_iter().collect())).collect())
        })
        .transpose()
    }

    async fn next_build_ask(&self, id: uuid::Uuid) -> anyhow::Result<i64> {
        self.pool
            .scalar_opt("UPDATE project SET build_asks = build_asks + 1 WHERE id = $1 RETURNING build_asks", &[&id])
            .await?
            .ok_or_else(|| anyhow::anyhow!("no project {id} to take a build ask of"))
    }

    async fn transition(&self, id: uuid::Uuid) -> anyhow::Result<Option<ProjectTransition>> {
        let row: Option<String> = self.pool.scalar_opt("SELECT transition FROM project WHERE id = $1", &[&id]).await?;
        row.map(|t| project_transition_from_str(&t)).transpose()
    }

    async fn try_begin_building(&self, id: uuid::Uuid, stale_before: i64) -> anyhow::Result<bool> {
        let now = crate::lease::now_unix();
        let mut conn = self.pool.get().await?;
        let tx = conn.transaction().await?;
        let began = tx
            .execute(
                &format!(
                    "UPDATE project \
                     SET transition = 'building', transition_heartbeat_unix = $1 \
                     WHERE id = $2 \
                       AND (transition = 'none' OR (transition = 'building' AND transition_heartbeat_unix < $3) \
                            OR (transition = 'resyncing' AND NOT {})) \
                       AND NOT EXISTS (SELECT 1 FROM trigger_activation a \
                                       WHERE a.project_id = $2 AND a.status IN ('activating', 'deactivating'))",
                    transition_held("project", "$3")
                ),
                &[&now, &id, &stale_before],
            )
            .await?;
        if began == 0 {
            return Ok(false);
        }
        crate::build::ledger::end_lost_starts(&tx, crate::build::ledger::LostStarts::Of(id), now).await?;
        tx.commit().await?;
        Ok(true)
    }

    async fn try_begin_resyncing(
        &self,
        id: uuid::Uuid,
        plan: &crate::task_kinds::resync::ResyncPlan,
        task: weft_task_store::tasks::NewTask,
    ) -> anyhow::Result<Option<uuid::Uuid>> {
        let mut conn = self.pool.get().await?;
        let tx = conn.transaction().await?;
        // The named build checked as `crate::api::project::require_registered_build`
        // checks it, under the row lock every registration takes
        // (`crate::build::waiting::settle`), so no build lands between the
        // check and the resync holding the project.
        let sql = format!(
            "UPDATE project SET transition = 'resyncing', transition_heartbeat_unix = 0, {} \
             WHERE id = $1 AND transition = 'none' \
               AND ($5::text IS NULL OR running_binary_hash = $5) \
               AND ($6::text IS NULL OR running_definition_hash = $6) \
               AND ($7::text IS NULL OR running_infra_hash = $7) \
               AND NOT EXISTS (SELECT 1 FROM trigger_activation a \
                               WHERE a.project_id = $1 AND a.status IN ('activating', 'deactivating'))",
            ResyncPlan::ROW
        );
        let entered = plan
            .row_write(&tx, id, &sql, &[&plan.build.binary_hash, &plan.build.definition_hash, &plan.build.infra_hash])
            .await?;
        if entered == 0 {
            return Ok(None);
        }
        let task = weft_task_store::tasks::enqueue_dedup_in(&tx, task).await?.id();
        tx.commit().await?;
        weft_task_store::announce::committed(&self.pool);
        Ok(Some(task))
    }

    async fn resync(&self, id: uuid::Uuid) -> anyhow::Result<Option<crate::task_kinds::resync::ResyncState>> {
        // Whether a task of its drive is pending or claimed, and where that
        // task last said it is (NULL until it says).
        let row: Option<(String, weft_core::stored::Version, Option<uuid::Uuid>, bool, Option<db::JsonText<serde_json::Value>>)> = self
            .pool
            .opt(
                "WITH driving AS ( \
                     SELECT t.progress FROM task t JOIN project p ON t.tenant_id = p.tenant_id \
                     WHERE p.id = $1 AND t.kind = $2 AND t.dedup_key = p.resync_driver::text AND t.status IN ('pending', 'claimed') \
                     LIMIT 1) \
                 SELECT p.resync, p.resync_version, p.resync_driver, EXISTS (SELECT 1 FROM driving), (SELECT progress FROM driving) \
                 FROM project p WHERE p.id = $1 AND p.transition = 'resyncing' AND p.resync IS NOT NULL",
                &[&id, &weft_task_store::TaskKind::Resync.as_str()],
            )
            .await?;
        row.map(|(json, version, driver, driven, progress)| {
            let plan = ResyncPlan::stored(id, json.as_str(), version, driver)?;
            let progress = match progress.filter(|_| driven) {
                Some(progress) => serde_json::from_value(progress.0)
                    .map_err(|e| anyhow::anyhow!("project {id}'s resync task says where it is in a shape this weft does not read: {e}"))?,
                None => Default::default(),
            };
            Ok(crate::task_kinds::resync::ResyncState { plan, driven, progress })
        })
        .transpose()
    }

    async fn drive(&self, id: uuid::Uuid, driver: uuid::Uuid) -> anyhow::Result<Option<(crate::task_kinds::resync::ResyncPlan, bool)>> {
        let row: Option<(String, weft_core::stored::Version, bool)> = self
            .pool
            .opt(
                "SELECT resync, resync_version, transition = 'resyncing' FROM project \
                 WHERE id = $1 AND resync_driver = $2 AND resync IS NOT NULL",
                &[&id, &driver],
            )
            .await?;
        let Some((json, version, under_way)) = row else { return Ok(None) };
        let plan = ResyncPlan::stored(id, json.as_str(), version, Some(driver))?;
        // A person gave it up: no drive of it goes on.
        Ok(plan.given_up.is_none().then_some((plan, under_way)))
    }

    async fn write_resync(&self, id: uuid::Uuid, plan: &crate::task_kinds::resync::ResyncPlan) -> anyhow::Result<bool> {
        // Only the drive the plan names writes it.
        let sql = format!("UPDATE project SET {} WHERE id = $1 AND transition = 'resyncing' AND resync_driver = $4", ResyncPlan::ROW);
        let written = plan.row_write(&self.pool, id, &sql, &[]).await?;
        Ok(written > 0)
    }

    async fn resume_resync(&self, id: uuid::Uuid, tenant: &str) -> anyhow::Result<Option<uuid::Uuid>> {
        let mut conn = self.pool.get().await?;
        let tx = conn.transaction().await?;
        let Some(mut plan) = held_resync(&tx, id).await? else { return Ok(None) };
        if plan.stopped.is_some() {
            plan.driver = uuid::Uuid::now_v7();
            plan.stopped = None;
            plan.row_write(&tx, id, &format!("UPDATE project SET {} WHERE id = $1", ResyncPlan::ROW), &[]).await?;
        }
        let task = crate::task_kinds::resync::task(tenant, id, plan.driver);
        let task = weft_task_store::tasks::enqueue_dedup_in(&tx, task).await?.id();
        tx.commit().await?;
        weft_task_store::announce::committed(&self.pool);
        Ok(Some(task))
    }

    async fn stopped_resyncs(&self) -> anyhow::Result<Vec<(uuid::Uuid, anyhow::Result<crate::task_kinds::resync::ResyncPlan>)>> {
        let rows: Vec<(uuid::Uuid, String, weft_core::stored::Version, Option<uuid::Uuid>)> = self
            .pool
            .all(
                "SELECT id, resync, resync_version, resync_driver FROM project WHERE transition = 'resyncing' AND resync IS NOT NULL",
                &[],
            )
            .await?;
        Ok(rows
            .into_iter()
            .map(|(id, json, version, driver)| (id, ResyncPlan::stored(id, json.as_str(), version, driver)))
            .filter(|(_, plan)| plan.as_ref().map_or(true, |plan| plan.stopped.is_some()))
            .collect())
    }

    async fn end_resync(&self, id: uuid::Uuid, driver: uuid::Uuid) -> anyhow::Result<bool> {
        let ended = self
            .pool
            .execute(
                "UPDATE project SET transition = 'none', transition_heartbeat_unix = 0 \
                 WHERE id = $1 AND transition = 'resyncing' AND resync_driver = $2",
                &[&id, &driver],
            )
            .await?;
        Ok(ended > 0)
    }

    async fn give_up_resync(&self, id: uuid::Uuid) -> anyhow::Result<Option<crate::task_kinds::resync::ResyncPlan>> {
        let mut conn = self.pool.get().await?;
        let tx = conn.transaction().await?;
        // Held first: its drive's take-downs and claims lock this row too,
        // so the rows read below hold every one it made. A trigger whose
        // activation it started is still landing counts as down: the
        // person's cancel ends that activation.
        let Some(mut plan) = held_resync(&tx, id).await? else { return Ok(None) };
        let rows = crate::activation_store::list_in(&tx, id).await?;
        plan.given_up = Some(crate::task_kinds::resync::GivenUp {
            at_unix: crate::lease::now_unix(),
            left_down: crate::task_kinds::resync::left_down(&plan, &rows),
            unreadable: None,
        });
        let sql = format!("UPDATE project SET transition = 'none', transition_heartbeat_unix = 0, {} WHERE id = $1", ResyncPlan::ROW);
        plan.row_write(&tx, id, &sql, &[]).await?;
        // Told with the write: a drive waiting for the runs on the old
        // version, on whichever dispatcher, stops waiting
        // (`crate::task_kinds::resync::given_up`).
        tx.execute("SELECT pg_notify($1, $2)", &[&crate::task_kinds::resync::RESYNC_GIVEN_UP_CHANNEL, &id.to_string()]).await?;
        tx.commit().await?;
        Ok(Some(plan))
    }

    async fn give_up_unreadable_resync(&self, id: uuid::Uuid) -> anyhow::Result<bool> {
        let mut conn = self.pool.get().await?;
        let tx = conn.transaction().await?;
        let row: Option<(db::JsonText<serde_json::Value>, weft_core::stored::Version, Option<uuid::Uuid>)> = tx
            .opt(
                "SELECT resync, resync_version, resync_driver FROM project \
                 WHERE id = $1 AND transition = 'resyncing' AND resync IS NOT NULL FOR UPDATE",
                &[&id],
            )
            .await?;
        let Some((db::JsonText(document), version, driver)) = row else { return Ok(false) };
        let error = match ResyncPlan::stored(id, document.clone(), version, driver) {
            Ok(_) => return Ok(false),
            Err(e) => e,
        };
        let unreadable = crate::task_kinds::resync::UnreadablePlan { error: format!("{error:#}"), document, version };
        let plan = ResyncPlan::given_up_unreadable(unreadable, crate::lease::now_unix());
        let sql = format!("UPDATE project SET transition = 'none', transition_heartbeat_unix = 0, {} WHERE id = $1", ResyncPlan::ROW);
        plan.row_write(&tx, id, &sql, &[]).await?;
        // Told with the write, as a person's give-up is: a drive of the
        // plan waiting for the runs on the old version stops waiting.
        tx.execute("SELECT pg_notify($1, $2)", &[&crate::task_kinds::resync::RESYNC_GIVEN_UP_CHANNEL, &id.to_string()]).await?;
        tx.commit().await?;
        Ok(true)
    }

    async fn given_up_resync(&self, id: uuid::Uuid) -> anyhow::Result<Option<crate::task_kinds::resync::GivenUp>> {
        let row: Option<(String, weft_core::stored::Version, Option<uuid::Uuid>)> = self
            .pool
            .opt("SELECT resync, resync_version, resync_driver FROM project WHERE id = $1 AND resync IS NOT NULL", &[&id])
            .await?;
        row.map(|(json, version, driver)| Ok(ResyncPlan::stored(id, json.as_str(), version, driver)?.given_up))
            .transpose()
            .map(Option::flatten)
    }

    async fn request_cancel_build(&self, id: uuid::Uuid) -> anyhow::Result<bool> {
        let requested = self
            .pool
            .execute(
                "UPDATE project SET transition = 'cancelling_build' \
                 WHERE id = $1 AND transition IN ('building', 'cancelling_build')",
                &[&id],
            )
            .await?;
        Ok(requested > 0)
    }

    async fn release_driver(&self, id: uuid::Uuid) -> anyhow::Result<()> {
        self.pool.execute("UPDATE project SET transition_heartbeat_unix = 0 WHERE id = $1 AND transition <> 'none'", &[&id]).await?;
        Ok(())
    }

    async fn settle_transition(&self, project: Option<uuid::Uuid>, stale_before: i64) -> anyhow::Result<Vec<uuid::Uuid>> {
        Ok(self.pool.scalars(&settle_transition_sql(), &[&stale_before, &project]).await?)
    }

    async fn bump_transition_heartbeat(&self, id: uuid::Uuid) -> anyhow::Result<()> {
        // A released heartbeat (0, `release_driver`) stays released: the
        // beat's task is stopped without waiting for it, so a bump already
        // on its way may land after the release, and must not make the
        // project driven again.
        self.pool
            .execute(
                "UPDATE project SET transition_heartbeat_unix = $1 \
                 WHERE id = $2 AND transition <> 'none' AND transition_heartbeat_unix > 0",
                &[&crate::lease::now_unix(), &id],
            )
            .await?;
        Ok(())
    }

    /// Read-only access to the full ProjectDefinition. JSON decode
    /// failure propagates: a corrupt `project_json` is schema drift,
    /// not "no project".
    async fn project(&self, id: uuid::Uuid) -> anyhow::Result<Option<ProjectDefinition>> {
        let row: Option<(String, weft_core::stored::Version)> =
            self.pool.opt("SELECT project_json, project_json_version FROM project WHERE id = $1", &[&id]).await?;
        Ok(row
            .map(|(json, version)| {
                ProjectDefinition::STORED.decode(json.as_str(), version).map_err(|e| e.at(format_args!("project {id}'s program")))
            })
            .transpose()?)
    }

    async fn project_has_infra(&self, id: uuid::Uuid) -> anyhow::Result<Option<bool>> {
        Ok(self.pool.scalar_opt("SELECT has_infra FROM project WHERE id = $1", &[&id]).await?)
    }

    async fn worker_overrides(&self, id: uuid::Uuid) -> anyhow::Result<Option<weft_platform_traits::WorkerOverrides>> {
        let row: Option<db::JsonText<serde_json::Value>> =
            self.pool.scalar_opt("SELECT worker_settings_json FROM project WHERE id = $1", &[&id]).await?;
        row.map(|db::JsonText(v)| {
            serde_json::from_value(v).map_err(|e| anyhow::anyhow!("project {id}'s worker settings are not valid: {e}"))
        })
        .transpose()
    }

    async fn set_worker_overrides(&self, id: uuid::Uuid, overrides: &weft_platform_traits::WorkerOverrides) -> anyhow::Result<()> {
        // The project's queued runs start their tries again on the new
        // levers: the change is what a person does about workers that
        // refused them, could not be reached or kept answering busy. One
        // waiting to be tried
        // again is taken at once; one out with a delivery right now is
        // left to it. The delivery hears the change
        // (`crate::held::WORKER_SETTINGS_CHANNEL`). What `weft status`
        // showed of the project's workers goes with the tries it was about:
        // a queued run they fail again writes it afresh.
        let mut conn = self.pool.get().await?;
        let tx = conn.transaction().await?;
        let changed: i64 = tx
            .scalar(
                "WITH changed AS (UPDATE project SET worker_settings_json = $2 WHERE id = $1 RETURNING id), \
                      retried AS (UPDATE run SET refusals = 0, unreached = 0, busy_since = NULL, waited_on_workers = FALSE, waits = FALSE, \
                                      delivered_until = CASE WHEN waits THEN NULL ELSE delivered_until END \
                                  WHERE project_id IN (SELECT id FROM changed) AND state = 'queued') \
                 SELECT count(*) FROM changed",
                &[&id, &db::JsonText(overrides)],
            )
            .await?;
        anyhow::ensure!(changed == 1, "no project {id}");
        weft_task_store::unanswered::answered(&tx, weft_task_store::unanswered::Callee::Workers(id)).await?;
        tx.commit().await?;
        Ok(())
    }
}

// Canonical wall-clock helper lives in `crate::lease::now_unix`.
// All UNIX-timestamp readers across the dispatcher route through
// that one function.
