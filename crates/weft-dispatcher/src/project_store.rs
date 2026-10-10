//! Project store. Keyed by project id. Holds the registered
//! ProjectDefinition, its running hashes and its build transition. Which
//! of its triggers are listening is the activation store's
//! (`activation_store`).
//!
//! Default impl is Postgres-backed (`PostgresProjectStore`), so
//! every dispatcher reads/writes the same `project` table.
//! Tests use `FakeProjectStore` (in-memory HashMap).

use std::sync::Arc;

use async_trait::async_trait;
use sqlx::postgres::PgPool;

use weft_core::ProjectDefinition;

#[cfg(any(test, feature = "test-helpers"))]
use std::collections::HashMap;
#[cfg(any(test, feature = "test-helpers"))]
use tokio::sync::RwLock;

/// The complete per-node infra image-tag map: `node_id -> { image_name ->
/// image_ref }`. Written atomically alongside the running hashes by the
/// build that made the images (`ProjectStoreOps::register_with_hashes`);
/// the supervisor reads it per node
/// (through the broker) to resolve `Image::Local { name }`. The column's
/// canonical decode is `weft_broker_client::protocol::decode_infra_image_tags`
/// (shared with the broker's read so the two cannot drift).
pub type InfraImageTags =
    std::collections::BTreeMap<String, std::collections::HashMap<String, String>>;

/// Backing store for project metadata. Implementations:
/// - `PostgresProjectStore` (production)
/// - `FakeProjectStore` (tests, behind `test-helpers`).
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
        implementations: Option<&std::collections::BTreeMap<String, String>>,
        source: Option<&weft_core::project::hash::Manifest>,
        settles: Option<&crate::build::waiting::Settles<'_>>,
    ) -> anyhow::Result<Option<StoredProjectSummary>>;

    /// Sources registered with this exact running program. Refuse a concurrent build.
    async fn program_source(&self, id: uuid::Uuid, program: &weft_core::project::hash::ProgramIdentity) -> anyhow::Result<weft_core::project::hash::Manifest>;

    /// Read graph and worker identity from one database snapshot.
    async fn running_program_identity(&self, id: uuid::Uuid) -> anyhow::Result<Option<weft_core::project::hash::ProgramIdentity>>;

    /// The program identity a run names by its two hashes (its
    /// `ExecutionStarted`'s `definition_hash` and `binary_hash`): the
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
    /// Returns `Ok(true)` iff a row was removed. `Ok(false)` = no
    /// such row (caller decides whether to 404). `Err` = DB failure.
    async fn remove(&self, id: uuid::Uuid) -> anyhow::Result<bool>;
    async fn project(&self, id: uuid::Uuid) -> anyhow::Result<Option<ProjectDefinition>>;

    /// Read the stored binary hash. `Ok(None)` if never set
    /// (project registered but never built / activated /
    /// infra-started). `Err` ONLY on DB failure: a transient hiccup
    /// must NOT be observed as "no hash" (which would trigger an
    /// unnecessary stale-worker kill in `reconcile_worker`).
    async fn running_binary_hash(&self, id: uuid::Uuid) -> anyhow::Result<Option<String>>;

    /// Read the stored definition hash. Same Result contract as
    /// `running_binary_hash`.
    async fn running_definition_hash(&self, id: uuid::Uuid) -> anyhow::Result<Option<String>>;

    /// Look up a definition by hash. Returns `Ok(None)` when no
    /// version with that hash was recorded. Used by the broker's
    /// `fetch_definition` handler: workers and listeners pass
    /// `(project_id, expected_hash)` and get back the exact JSON
    /// that was registered under that hash, regardless of what the
    /// project row's current `running_definition_hash` says.
    async fn definition_for_hash(
        &self,
        id: uuid::Uuid,
        hash: &str,
    ) -> anyhow::Result<Option<String>>;

    /// Forget every program version recorded for a REMOVED project
    /// except the ones named. What a project removal and the last
    /// `weft clean` after it call with the versions the surviving runs
    /// were started against, so a deleted project leaves behind exactly
    /// what keeps its past runs readable and nothing else. Answers how
    /// many versions it dropped.
    ///
    /// Refuses on a project that still exists: while it is registered
    /// its own running version is in play and the caller cannot know
    /// what is safe to drop.
    async fn retire_unused_definitions(
        &self,
        id: uuid::Uuid,
        still_in_use: &[String],
    ) -> anyhow::Result<u64>;

    /// Every project id that still has recorded programs while the
    /// project itself is gone.
    ///
    /// What the reaper sweeps. Retiring on removal is best-effort (the
    /// removal has already happened and must not fail over unread rows),
    /// and once the project row is gone no request can ever reach those
    /// rows again: `weft rm` refuses a project it cannot find, and with no
    /// runs left `weft clean` has nothing to be called on either. This is
    /// what keeps a single failed retirement from being junk forever.
    async fn projects_with_orphan_definitions(&self) -> anyhow::Result<Vec<uuid::Uuid>>;

    /// Read the stored infra hash. Same Result contract as
    /// `running_binary_hash`.
    async fn running_infra_hash(&self, id: uuid::Uuid) -> anyhow::Result<Option<String>>;

    /// The infra image tags the last build registered (empty before the
    /// first build, or for a program without infra). `Ok(None)` = no such
    /// project.
    async fn running_infra_image_tags(&self, id: uuid::Uuid) -> anyhow::Result<Option<InfraImageTags>>;

    /// The next number in the order of the project's build asks, taken by
    /// a build request as it arrives (`build_asks`): a version registers
    /// only over an older ask (`crate::build::waiting::Settles`). Errs on
    /// no such project.
    async fn next_build_ask(&self, id: uuid::Uuid) -> anyhow::Result<i64>;

    /// Read the project's verb-transition marker (the build axis,
    /// orthogonal to `status`). `Ok(None)` = no such project.
    async fn transition(&self, id: uuid::Uuid) -> anyhow::Result<Option<ProjectTransition>>;

    /// Single-flight entry into the `building` transition. Atomically
    /// flips `transition` to building IFF no request is starting builds
    /// for the project right now (the transition is none, or building held
    /// only by its waiting version or by a request gone: its heartbeat is
    /// older than `stale_before`) AND the trigger lifecycle is not mid-flip
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
    /// (answered, or failed): from here the transition rests on the
    /// project's waiting version alone, so [`Self::settle_building`] may
    /// clear it as soon as none waits. Stamps the heartbeat to zero, which
    /// no live driver ever leaves it at.
    async fn release_build_driver(&self, id: uuid::Uuid) -> anyhow::Result<()>;

    /// Land at rest (`transition` → none) every project in a build
    /// transition that nothing holds any more ([`build_held`]: no request
    /// drives it and no version of it waits). `project` narrows it to one.
    /// Answers the projects it landed, for the caller to announce. Called
    /// by the build loop, by a request letting go, by a registration, by a
    /// cancel, and by the stuck-transition reaper, so one definition of
    /// "building" holds everywhere.
    async fn settle_building(&self, project: Option<uuid::Uuid>, stale_before: i64) -> anyhow::Result<Vec<uuid::Uuid>>;

    /// Bump the transition heartbeat of a build transition still driven
    /// (never one released). Called on an interval by the process DRIVING
    /// it (a build request starting its builds) so the
    /// stuck-transition reaper only repairs transitions whose driver
    /// actually died.
    async fn bump_transition_heartbeat(&self, id: uuid::Uuid) -> anyhow::Result<()>;

    /// The project's own worker levers (`Ok(None)` when the project does
    /// not exist).
    async fn worker_overrides(&self, id: uuid::Uuid) -> anyhow::Result<Option<weft_platform_traits::WorkerOverrides>>;

    /// Replace the project's own worker levers.
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

/// The verb-transition marker on the project row: the BUILD axis,
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

/// The SQL condition "something holds the project row `p` in its build
/// transition": a request drives it ([`build_driven`]), or a version of it
/// waits on its builds (`crate::build::waiting`). What a settle reads.
pub(crate) fn build_held(p: &str, stale_before: &str) -> String {
    format!("({} OR {})", build_driven(p, stale_before), crate::build::waiting::has_waiting_version(&format!("{p}.id")))
}

/// Cloneable handle to whatever the dispatcher uses as project
/// storage. The thing on `DispatcherState` is this, not the
/// concrete impl.
pub type ProjectStore = Arc<dyn ProjectStoreOps>;

#[derive(Clone)]
pub struct PostgresProjectStore {
    pool: PgPool,
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

#[derive(Debug, Clone)]
pub struct StoredProjectSummary {
    pub id: uuid::Uuid,
    pub name: String,
    pub description: String,
}

impl PostgresProjectStore {
    /// Plain wrap; the schema is the boot's job (`apply_core_schema`
    /// applies [`GROUP`] with every other core group, so a stale
    /// `project` table fails the SAME boot error as its siblings).
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

/// The `project` + `project_definition` tables. The canonical CREATEs
/// live here, edited in place; an existing database is carried to them
/// by a migration written with `./setup.sh --migration <name>` (the
/// whole contract is `weft_task_store::schema_guard`'s header). The
/// boot's `apply_core_schema` applies this group via the schema guard.
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
    //   - running_binary_hash: worker docker image tag suffix.
    //     Flips on engine / node-impl / node-type-set / weft.toml
    //     edits; selects the image when spawning a fresh process.
    //   - running_definition_hash: identifies the runtime project
    //     shape (topology + configs). Workers fetch the
    //     definition at execution claim time keyed by
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
                updated_at BIGINT NOT NULL,
                running_binary_hash TEXT,
                running_definition_hash TEXT,
                running_infra_hash TEXT,
                running_source JSONB,
                tenant_id TEXT NOT NULL,
                -- Whether this project DECLARES infrastructure (any node
                -- with requires_infra). Derived from the definition and
                -- refreshed on every register/sync, so it tracks edits
                -- that add or remove infra.
                has_infra BOOLEAN NOT NULL DEFAULT FALSE,
                -- Per-(project, node) image hash maps for Image::Local
                -- references in InfraSpecs. CLI ships these in /sync;
                -- supervisor reads them.
                -- Shape: { "<node_id>": { "<image_name>": "<tag>" } }
                infra_image_tags_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                -- The project's own worker levers, each one it sets
                -- replacing the install's (`WorkerOverrides`); empty
                -- runs on the install's.
                worker_settings_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                -- Per-project health protocols overriding the weft
                -- default. NULL = use default. Schema per
                -- weft_infra_supervisor::protocol::HealthProtocols.
                health_protocols_json JSONB,
                -- Verb-transition marker, orthogonal to `status` (the
                -- BUILD axis): 'none' | 'building' | 'cancelling_build'.
                -- Written only by its own single-flight methods
                -- (try_begin_building / request_cancel_build /
                -- settle_building), never by lifecycle writes, so a
                -- deactivate can't stomp an in-flight build marker. Held
                -- by the request starting the builds, then by the
                -- project's waiting version until it ends.
                transition TEXT NOT NULL DEFAULT 'none',
                -- Heartbeat for the build transition
                -- (transition='building'/'cancelling_build'): the request
                -- starting the builds bumps this on an interval and zeroes
                -- it when it lets go; once it is stale, only a waiting
                -- version holds the transition (settle_building).
                transition_heartbeat_unix BIGINT NOT NULL DEFAULT 0,
                -- The version tree's HEAD (`crate::versions`): the version
                -- the next checkpoint or run parents on, the run the next
                -- `--seed` inherits from (NULL when head is a bare
                -- version). Moved by checkpoint, run and branch; nothing
                -- lives on disk.
                head_version TEXT,
                head_run UUID,
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
                -- of the project takes work.
                api_address JSONB,
                -- The order of the project's build asks: each build request
                -- takes the next one as it arrives, before it compiles
                -- (`next_build_ask`), from this row, so every replica
                -- agrees on it.
                build_asks BIGINT NOT NULL DEFAULT 0,
                -- The ask whose version the project runs: a registration
                -- lands only over an older ask
                -- (`crate::build::waiting::settle`).
                registered_ask BIGINT NOT NULL DEFAULT 0
            )"#,
        "CREATE INDEX IF NOT EXISTS idx_project_tenant ON project(tenant_id)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_project_api_port ON project(api_port) WHERE api_port IS NOT NULL",
        // A project coming or going changes how its tenant's routes read
        // (a route whose project is gone is inactive); the function is the
        // journal group's, which applies first.
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
        // (project_id, definition_hash) so a suspended execution
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
        // building / deactivating) is the stuck-transition reaper's
        // job (`reaper::sweep_stuck_transitions`): per-project,
        // heartbeat-gated, and status-guarded, so it never wipes
        // another process's live state. A constructor-time bulk downgrade
        // would run in EVERY replica on EVERY boot and reset live
        // status for all tenants (a multi-process correctness bug).
        r#"CREATE TABLE IF NOT EXISTS project_code (
            project_id UUID NOT NULL REFERENCES project(id) ON DELETE CASCADE,
            binary_hash TEXT NOT NULL,
            implementations JSONB NOT NULL,
            PRIMARY KEY (project_id, binary_hash)
        )"#,
        r#"CREATE TABLE IF NOT EXISTS project_definition (
            project_id UUID NOT NULL,
            definition_hash TEXT NOT NULL,
            project_json TEXT NOT NULL,
            recorded_at_unix BIGINT NOT NULL,
            PRIMARY KEY (project_id, definition_hash)
            -- Deliberately NO foreign key to `project`. An execution's
            -- journal outlives the project it ran (that is the point of
            -- a journal), and a run without the program it ran against
            -- cannot be read back: the rows are there and every input
            -- and output is underivable, so the graph paints an empty
            -- run and the reader goes hunting for a bug in the viewer.
            -- The history is therefore kept as long as anything points
            -- at it, and `retire_unused_definitions` drops the versions
            -- no surviving run needs once the project itself is gone.
        )"#,
    ],
    seed: &[],
};

/// THE running-hash pointer advance: ONE atomic UPDATE of the trio of hashes
/// PLUS the complete infra image-tag map, with the definition-history EXISTS
/// guard. Its one writer is `register_with_hashes`, inside its transaction
/// (after the history INSERT, which the same-snapshot EXISTS check then sees). A `None` argument
/// leaves that field untouched; `Some(tags)` REPLACES the whole infra tag map.
/// Folding the tags into this one statement is what guarantees a project is
/// never observed stamped runnable (new binary hash) with its infra tags absent
/// or half-written. Zero rows updated fails loudly: either the project row is
/// missing, or the definition hash has no history row (the project must be
/// registered with that definition first; registering is the only writer of
/// history rows).
async fn advance_running_hashes(
    conn: &mut sqlx::PgConnection,
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
    let tags_json: Option<serde_json::Value> = match infra_image_tags {
        Some(tags) => Some(
            serde_json::to_value(tags)
                .map_err(|e| anyhow::anyhow!("serialize infra_image_tags for running-hash advance: {e}"))?,
        ),
        None => None,
    };
    let rows = sqlx::query(
        "UPDATE project SET \
             running_source = CASE WHEN ($1 IS NOT NULL AND running_binary_hash IS DISTINCT FROM $1) \
                OR ($2 IS NOT NULL AND running_definition_hash IS DISTINCT FROM $2) THEN NULL ELSE running_source END, \
             running_binary_hash     = COALESCE($1, running_binary_hash), \
             running_definition_hash = COALESCE($2, running_definition_hash), \
             running_infra_hash      = COALESCE($3, running_infra_hash), \
             infra_image_tags_json   = COALESCE($6, infra_image_tags_json), \
             updated_at = $4 \
         WHERE id = $5 AND ($2 IS NULL OR EXISTS ( \
             SELECT 1 FROM project_definition \
             WHERE project_id = $5 AND definition_hash = $2 \
         ))",
    )
    .bind(binary_hash)
    .bind(definition_hash)
    .bind(infra_hash)
    .bind(crate::lease::now_unix())
    .bind(id)
    .bind(tags_json)
    .execute(&mut *conn)
    .await?;
    if rows.rows_affected() == 0 {
        let (project_exists,): (bool,) =
            sqlx::query_as("SELECT EXISTS(SELECT 1 FROM project WHERE id = $1)")
                .bind(id)
                .fetch_one(&mut *conn)
                .await?;
        if !project_exists {
            anyhow::bail!("advance_running_hashes: project {id} not found");
        }
        anyhow::bail!(
            "refuse to set running_definition_hash to {hash} for project {id}: \
             no project_definition history row exists for that hash; \
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
        implementations: Option<&std::collections::BTreeMap<String, String>>,
        source: Option<&weft_core::project::hash::Manifest>,
        settles: Option<&crate::build::waiting::Settles<'_>>,
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
        let mut tx = self.pool.begin().await?;
        let now = crate::lease::now_unix();
        if let Some(settles) = settles {
            if !crate::build::waiting::settle(&mut tx, id, settles, now).await? {
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
        // that and fail loudly as a cross-tenant collision.
        let registered: Option<(uuid::Uuid,)> = sqlx::query_as(
            "INSERT INTO project \
                (id, name, description, status, project_json, updated_at, \
                 tenant_id, has_infra) \
             VALUES ($1, $2, $3, 'registered', $4, $5, $6, $7) \
             ON CONFLICT (id) DO UPDATE SET \
                name = EXCLUDED.name, \
                description = EXCLUDED.description, \
                project_json = EXCLUDED.project_json, \
                updated_at = EXCLUDED.updated_at, \
                has_infra = EXCLUDED.has_infra \
             WHERE project.tenant_id = EXCLUDED.tenant_id \
             RETURNING id",
        )
        .bind(id)
        .bind(&name)
        .bind(&description)
        .bind(&project_json)
        .bind(now)
        .bind(tenant_id)
        .bind(has_infra)
        .fetch_optional(&mut *tx)
        .await?;
        registered.ok_or_else(|| {
            anyhow::anyhow!(
                "project {id} already exists under a different tenant; \
                 register refused (cross-tenant id collision)"
            )
        })?;
        if let Some(hash) = definition_hash {
            // History row FIRST (idempotent on (project_id, hash)),
            // then pointer advance. Inside the transaction the FK
            // precondition is satisfied as of the same snapshot, so
            // the pointer-setter's existence check sees the freshly
            // inserted history row.
            sqlx::query(
                "INSERT INTO project_definition (project_id, definition_hash, project_json, recorded_at_unix) \
                 VALUES ($1, $2, $3, $4) \
                 ON CONFLICT (project_id, definition_hash) DO NOTHING",
            )
            .bind(id)
            .bind(hash)
            .bind(&project_json)
            .bind(now)
            .execute(&mut *tx)
            .await?;
        }
        if let Some(implementations) = implementations {
            let binary = binary_hash.ok_or_else(|| anyhow::anyhow!("implementation fingerprints require a worker binary hash"))?;
            let encoded = serde_json::to_value(implementations)?;
            let inserted = sqlx::query(
                "INSERT INTO project_code (project_id, binary_hash, implementations) VALUES ($1, $2, $3) \
                 ON CONFLICT (project_id, binary_hash) DO UPDATE SET implementations = EXCLUDED.implementations \
                 WHERE project_code.implementations = EXCLUDED.implementations"
            ).bind(id).bind(binary).bind(encoded).execute(&mut *tx).await?;
            anyhow::ensure!(inserted.rows_affected() == 1, "worker hash {binary} already has different implementation fingerprints");
        }
        // Runnable stamp + infra tags in ONE statement inside this same tx, so a
        // build that registers-and-stamps never leaves a runnable
        // project with missing/half-written infra tags. Plain registration
        // passes `infra_image_tags = None`.
        advance_running_hashes(
            &mut tx,
            id,
            binary_hash,
            definition_hash,
            infra_hash,
            infra_image_tags,
        )
        .await?;
        if binary_hash.is_some() || definition_hash.is_some() {
            sqlx::query("UPDATE project SET running_source = $2 WHERE id = $1")
                .bind(id).bind(source.map(serde_json::to_value).transpose()?)
                .execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(Some(StoredProjectSummary { id, name, description }))
    }

    async fn program_source(&self, id: uuid::Uuid, program: &weft_core::project::hash::ProgramIdentity) -> anyhow::Result<weft_core::project::hash::Manifest> {
        let row: Option<(serde_json::Value,)> = sqlx::query_as(
            "SELECT running_source FROM project WHERE id = $1 AND running_binary_hash = $2 AND running_definition_hash = $3 AND running_source IS NOT NULL"
        ).bind(id).bind(&program.binary_hash).bind(&program.definition_hash).fetch_optional(&self.pool).await?;
        let (source,) = row.ok_or_else(|| anyhow::anyhow!("project sources are missing or changed during preparation; register the project and retry"))?;
        Ok(serde_json::from_value(source)?)
    }

    async fn running_program_identity(&self, id: uuid::Uuid) -> anyhow::Result<Option<weft_core::project::hash::ProgramIdentity>> {
        let row: Option<(String, String, serde_json::Value)> = sqlx::query_as(
            "SELECT p.running_definition_hash, p.running_binary_hash, c.implementations \
             FROM project p JOIN project_code c ON c.project_id = p.id AND c.binary_hash = p.running_binary_hash \
             WHERE p.id = $1 AND p.running_definition_hash IS NOT NULL"
        ).bind(id).fetch_optional(&self.pool).await?;
        row.map(|(definition_hash, binary_hash, implementations)| Ok(weft_core::project::hash::ProgramIdentity {
            definition_hash, binary_hash, implementations: serde_json::from_value(implementations)?,
        })).transpose()
    }

    async fn program_identity(
        &self,
        id: uuid::Uuid,
        definition_hash: &str,
        binary_hash: &str,
    ) -> anyhow::Result<Option<weft_core::project::hash::ProgramIdentity>> {
        let row: Option<(serde_json::Value,)> = sqlx::query_as(
            "SELECT implementations FROM project_code WHERE project_id = $1 AND binary_hash = $2",
        )
        .bind(id)
        .bind(binary_hash)
        .fetch_optional(&self.pool)
        .await?;
        row.map(|(implementations,)| {
            Ok(weft_core::project::hash::ProgramIdentity {
                definition_hash: definition_hash.to_string(),
                binary_hash: binary_hash.to_string(),
                implementations: serde_json::from_value(implementations)?,
            })
        })
        .transpose()
    }

    async fn tenant_for(&self, id: uuid::Uuid) -> anyhow::Result<Option<String>> {
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT tenant_id FROM project WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|(t,)| t))
    }

    async fn list(&self, tenant: &str) -> anyhow::Result<Vec<StoredProjectSummary>> {
        let rows: Vec<(uuid::Uuid, String, String)> = sqlx::query_as(
            "SELECT id, name, description FROM project WHERE tenant_id = $1 ORDER BY name",
        )
        .bind(tenant)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|(id, name, description)| StoredProjectSummary { id, name, description })
            .collect())
    }

    async fn get(&self, id: uuid::Uuid) -> anyhow::Result<Option<StoredProjectSummary>> {
        let row: Option<(uuid::Uuid, String, String)> = sqlx::query_as(
            "SELECT id, name, description FROM project WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|(id, name, description)| StoredProjectSummary { id, name, description }))
    }

    async fn remove(&self, id: uuid::Uuid) -> anyhow::Result<bool> {
        let mut tx = self.pool.begin().await?;
        let res = sqlx::query("DELETE FROM project WHERE id = $1")
            .bind(id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM trigger_setup WHERE project_id = $1")
            .bind(id).execute(&mut *tx).await?;
        sqlx::query("DELETE FROM trigger_bake WHERE project_id = $1")
            .bind(id).execute(&mut *tx).await?;
        // The version tree goes with the project (its runs cascade):
        // versions left behind kept naming stored files for a project
        // that no longer existed, and the same id registered again
        // inherited a tree it never made.
        sqlx::query("DELETE FROM project_version WHERE project_id = $1")
            .bind(id).execute(&mut *tx).await?;
        // The infra rows go in the same transaction, AFTER the project
        // row: the broker's command and event inserts lock the project
        // row they read (`FOR KEY SHARE`), so an insert racing this
        // either commits first and is deleted below, or waits for this
        // commit and then finds no project to insert for.
        crate::infra_node::remove_project(&mut *tx, id).await?;
        crate::infra_event::remove_project(&mut *tx, id).await?;
        crate::infra_lifecycle_command::remove_project(&mut *tx, id).await?;
        // An instance lives inside this project only: its connections,
        // picks and tokens reach nothing once the project is gone, and
        // nobody could list them to delete them.
        weft_access_store::forget_project_access(&mut tx, id).await?;
        crate::journal::postgres::revoke_project_instance_tokens(&mut *tx, id).await?;
        tx.commit().await?;
        Ok(res.rows_affected() > 0)
    }


    async fn running_binary_hash(&self, id: uuid::Uuid) -> anyhow::Result<Option<String>> {
        let row: Option<(Option<String>,)> = sqlx::query_as(
            "SELECT running_binary_hash FROM project WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.and_then(|(h,)| h))
    }

    async fn running_definition_hash(&self, id: uuid::Uuid) -> anyhow::Result<Option<String>> {
        let row: Option<(Option<String>,)> = sqlx::query_as(
            "SELECT running_definition_hash FROM project WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.and_then(|(h,)| h))
    }

    async fn definition_for_hash(
        &self,
        id: uuid::Uuid,
        hash: &str,
    ) -> anyhow::Result<Option<String>> {
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT project_json FROM project_definition \
             WHERE project_id = $1 AND definition_hash = $2",
        )
        .bind(id)
        .bind(hash)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|(j,)| j))
    }

    async fn projects_with_orphan_definitions(&self) -> anyhow::Result<Vec<uuid::Uuid>> {
        let rows: Vec<(uuid::Uuid,)> = sqlx::query_as(
            "SELECT DISTINCT pd.project_id FROM project_definition pd \
             WHERE NOT EXISTS (SELECT 1 FROM project p WHERE p.id = pd.project_id)",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|(id,)| id).collect())
    }

    async fn retire_unused_definitions(
        &self,
        id: uuid::Uuid,
        still_in_use: &[String],
    ) -> anyhow::Result<u64> {
        // The guard is part of the same statement as the delete, so a
        // project registered again in between cannot have its history
        // dropped by a removal that was already under way.
        let done = sqlx::query(
            "DELETE FROM project_definition \
             WHERE project_id = $1 \
               AND NOT (definition_hash = ANY($2)) \
               AND NOT EXISTS (SELECT 1 FROM project WHERE id = $1)",
        )
        .bind(id)
        .bind(still_in_use)
        .execute(&self.pool)
        .await?;
        Ok(done.rows_affected())
    }

    async fn running_infra_hash(&self, id: uuid::Uuid) -> anyhow::Result<Option<String>> {
        let row: Option<(Option<String>,)> = sqlx::query_as(
            "SELECT running_infra_hash FROM project WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.and_then(|(h,)| h))
    }

    async fn running_infra_image_tags(&self, id: uuid::Uuid) -> anyhow::Result<Option<InfraImageTags>> {
        let row: Option<(serde_json::Value,)> =
            sqlx::query_as("SELECT infra_image_tags_json FROM project WHERE id = $1")
                .bind(id)
                .fetch_optional(&self.pool)
                .await?;
        row.map(|(value,)| {
            let tags = weft_broker_client::protocol::decode_infra_image_tags(value, &format!("project {id}"))?;
            Ok(tags.into_iter().map(|(place, images)| (place, images.into_iter().collect())).collect())
        })
        .transpose()
    }

    async fn next_build_ask(&self, id: uuid::Uuid) -> anyhow::Result<i64> {
        sqlx::query_scalar("UPDATE project SET build_asks = build_asks + 1 WHERE id = $1 RETURNING build_asks")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| anyhow::anyhow!("no project {id} to take a build ask of"))
    }

    async fn transition(&self, id: uuid::Uuid) -> anyhow::Result<Option<ProjectTransition>> {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT transition FROM project WHERE id = $1")
                .bind(id)
                .fetch_optional(&self.pool)
                .await?;
        row.map(|(t,)| project_transition_from_str(&t)).transpose()
    }

    async fn try_begin_building(&self, id: uuid::Uuid, stale_before: i64) -> anyhow::Result<bool> {
        let now = crate::lease::now_unix();
        let mut tx = self.pool.begin().await?;
        let res = sqlx::query(
            "UPDATE project \
             SET transition = 'building', transition_heartbeat_unix = $1 \
             WHERE id = $2 \
               AND (transition = 'none' OR (transition = 'building' AND transition_heartbeat_unix < $3)) \
               AND NOT EXISTS (SELECT 1 FROM trigger_activation a \
                               WHERE a.project_id = $2 AND a.status IN ('activating', 'deactivating'))",
        )
        .bind(now)
        .bind(id)
        .bind(stale_before)
        .execute(&mut *tx)
        .await?;
        if res.rows_affected() == 0 {
            return Ok(false);
        }
        crate::build::ledger::end_lost_starts(&mut tx, crate::build::ledger::LostStarts::Of(id), now).await?;
        tx.commit().await?;
        Ok(true)
    }

    async fn request_cancel_build(&self, id: uuid::Uuid) -> anyhow::Result<bool> {
        let res = sqlx::query(
            "UPDATE project SET transition = 'cancelling_build' \
             WHERE id = $1 AND transition IN ('building', 'cancelling_build')",
        )
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected() > 0)
    }

    async fn release_build_driver(&self, id: uuid::Uuid) -> anyhow::Result<()> {
        sqlx::query(
            "UPDATE project SET transition_heartbeat_unix = 0 \
             WHERE id = $1 AND transition IN ('building', 'cancelling_build')",
        )
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn settle_building(&self, project: Option<uuid::Uuid>, stale_before: i64) -> anyhow::Result<Vec<uuid::Uuid>> {
        // NOTE (compiler-invisible status set): this WHERE names the
        // build transition's states by string, like the CAS methods above.
        Ok(sqlx::query_scalar(&format!(
            "UPDATE project p SET transition = 'none' \
             WHERE p.transition IN ('building', 'cancelling_build') \
               AND NOT {} \
               AND ($2::UUID IS NULL OR p.id = $2) \
             RETURNING p.id",
            build_held("p", "$1")
        ))
        .bind(stale_before)
        .bind(project)
        .fetch_all(&self.pool)
        .await?)
    }

    async fn bump_transition_heartbeat(&self, id: uuid::Uuid) -> anyhow::Result<()> {
        // A released heartbeat (0, `release_build_driver`) stays released:
        // the beat's task is stopped without waiting for it, so a bump
        // already on its way may land after the release, and must not make
        // the project driven again.
        sqlx::query(
            "UPDATE project SET transition_heartbeat_unix = $1 \
             WHERE id = $2 AND transition IN ('building', 'cancelling_build') AND transition_heartbeat_unix > 0",
        )
        .bind(crate::lease::now_unix())
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Read-only access to the full ProjectDefinition. JSON decode
    /// failure propagates: a corrupt `project_json` is schema drift,
    /// not "no project".
    async fn project(&self, id: uuid::Uuid) -> anyhow::Result<Option<ProjectDefinition>> {
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT project_json FROM project WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        match row {
            None => Ok(None),
            Some((json,)) => {
                let def = serde_json::from_str(&json)
                    .map_err(|e| anyhow::anyhow!("decode project_json for id={id}: {e}"))?;
                Ok(Some(def))
            }
        }
    }

    async fn project_has_infra(&self, id: uuid::Uuid) -> anyhow::Result<Option<bool>> {
        let row: Option<(bool,)> =
            sqlx::query_as("SELECT has_infra FROM project WHERE id = $1")
                .bind(id)
                .fetch_optional(&self.pool)
                .await?;
        Ok(row.map(|(b,)| b))
    }

    async fn worker_overrides(&self, id: uuid::Uuid) -> anyhow::Result<Option<weft_platform_traits::WorkerOverrides>> {
        let row: Option<(serde_json::Value,)> =
            sqlx::query_as("SELECT worker_settings_json FROM project WHERE id = $1").bind(id).fetch_optional(&self.pool).await?;
        row.map(|(v,)| {
            serde_json::from_value(v).map_err(|e| anyhow::anyhow!("project {id}'s worker settings are not valid: {e}"))
        })
        .transpose()
    }

    async fn set_worker_overrides(&self, id: uuid::Uuid, overrides: &weft_platform_traits::WorkerOverrides) -> anyhow::Result<()> {
        let done = sqlx::query("UPDATE project SET worker_settings_json = $2 WHERE id = $1")
            .bind(id)
            .bind(serde_json::to_value(overrides)?)
            .execute(&self.pool)
            .await?;
        anyhow::ensure!(done.rows_affected() == 1, "no project {id}");
        Ok(())
    }
}

// Canonical wall-clock helper lives in `crate::lease::now_unix`.
// All UNIX-timestamp readers across the dispatcher route through
// that one function.

#[cfg(any(test, feature = "test-helpers"))]
pub struct FakeProjectStore {
    sources: RwLock<HashMap<uuid::Uuid, weft_core::project::hash::Manifest>>,
    inner: RwLock<HashMap<uuid::Uuid, (String, ProjectDefinition)>>,
    binary_hashes: RwLock<HashMap<uuid::Uuid, String>>,
    implementations: RwLock<HashMap<(uuid::Uuid, String), std::collections::BTreeMap<String, String>>>,
    definition_hashes: RwLock<HashMap<uuid::Uuid, String>>,
    infra_hashes: RwLock<HashMap<uuid::Uuid, String>>,
    /// Mirror of `infra_image_tags_json`, what the last build registered.
    infra_image_tags: RwLock<HashMap<uuid::Uuid, InfraImageTags>>,
    /// In-memory mirror of the `project_definition` history table:
    /// keyed by `(project_id, definition_hash)`, value is the
    /// `project_json` registered under that hash.
    definition_versions: RwLock<HashMap<(uuid::Uuid, String), String>>,
    tenants: RwLock<HashMap<uuid::Uuid, String>>,
    /// Free-text project description, mirroring the `project.description`
    /// column. A separate map (like `tenants` / `has_infra`) so the `inner`
    /// tuple's many destructure sites stay untouched.
    descriptions: RwLock<HashMap<uuid::Uuid, String>>,
    has_infra: RwLock<HashMap<uuid::Uuid, bool>>,
    worker_overrides: RwLock<HashMap<uuid::Uuid, weft_platform_traits::WorkerOverrides>>,
    /// Mirror of the `transition` + `transition_heartbeat_unix`
    /// columns. Missing entry = (None, 0), matching the column
    /// defaults on a fresh row.
    transitions: RwLock<HashMap<uuid::Uuid, (ProjectTransition, i64)>>,
    /// Mirror of `build_asks`.
    build_asks: RwLock<HashMap<uuid::Uuid, i64>>,
}

#[cfg(any(test, feature = "test-helpers"))]
impl FakeProjectStore {
    pub fn new() -> Self {
        Self {
            sources: RwLock::new(HashMap::new()),
            inner: RwLock::new(HashMap::new()),
            binary_hashes: RwLock::new(HashMap::new()),
            implementations: RwLock::new(HashMap::new()),
            definition_hashes: RwLock::new(HashMap::new()),
            infra_hashes: RwLock::new(HashMap::new()),
            infra_image_tags: RwLock::new(HashMap::new()),
            definition_versions: RwLock::new(HashMap::new()),
            tenants: RwLock::new(HashMap::new()),
            descriptions: RwLock::new(HashMap::new()),
            has_infra: RwLock::new(HashMap::new()),
            worker_overrides: RwLock::new(HashMap::new()),
            transitions: RwLock::new(HashMap::new()),
            build_asks: RwLock::new(HashMap::new()),
        }
    }
}

#[cfg(any(test, feature = "test-helpers"))]
impl Default for FakeProjectStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(any(test, feature = "test-helpers"))]
#[async_trait]
impl ProjectStoreOps for FakeProjectStore {
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
        implementations: Option<&std::collections::BTreeMap<String, String>>,
        source: Option<&weft_core::project::hash::Manifest>,
        // The fake holds no version builds (they live in Postgres): a
        // registration always lands.
        _settles: Option<&crate::build::waiting::Settles<'_>>,
    ) -> anyhow::Result<Option<StoredProjectSummary>> {
        let id = project.id;
        let name_owned = name.to_string();
        let description_owned = description.to_string();
        // Cross-tenant collision guard, mirroring the Postgres conflict arm's
        // `WHERE project.tenant_id = EXCLUDED.tenant_id`: a re-register may only
        // touch a row already owned by the same tenant, so one tenant cannot
        // seize another's project id. Checked before any mutation.
        if let Some(existing) = self.tenants.read().await.get(&id) {
            if existing != tenant_id {
                anyhow::bail!(
                    "project {id} already exists under a different tenant; \
                     register refused (cross-tenant id collision)"
                );
            }
        }
        // Compute before `project` is moved into `inner`. Re-derived on
        // every register so a re-register that adds/drops infra updates
        // it, mirroring the Postgres conflict arm.
        let has_infra = weft_core::has_infra(&project);
        // Serialize for the history row before moving `project`.
        if let Some(implementations) = implementations {
            let binary = binary_hash.ok_or_else(|| anyhow::anyhow!("implementation fingerprints require a worker binary hash"))?;
            let mut stored = self.implementations.write().await;
            let key = (id, binary.to_string());
            anyhow::ensure!(stored.get(&key).is_none_or(|old| old == implementations), "worker hash {binary} already has different implementation fingerprints");
            stored.insert(key, implementations.clone());
        }
        let project_json = serde_json::to_string(&project)?;
        self.inner.write().await.insert(id, (name_owned.clone(), project));
        self.tenants.write().await.insert(id, tenant_id.to_string());
        self.descriptions.write().await.insert(id, description_owned.clone());
        self.has_infra.write().await.insert(id, has_infra);
        if let Some(h) = binary_hash {
            self.binary_hashes.write().await.insert(id, h.to_string());
        }
        if let Some(h) = definition_hash {
            // Record history row first, then advance the pointer.
            // The fake cannot truly atomicize the five `RwLock` writes
            // (they're independent locks taken in sequence); the
            // ordering invariant is preserved so a fold in any test
            // sees the history row before the pointer, matching the
            // production Postgres transaction's visibility, but a
            // panic mid-sequence would leave partial state. Tests
            // that need true atomicity should exercise the Postgres
            // path directly.
            self.definition_versions
                .write()
                .await
                .insert((id, h.to_string()), project_json.clone());
            self.definition_hashes.write().await.insert(id, h.to_string());
        }
        if let Some(h) = infra_hash {
            self.infra_hashes.write().await.insert(id, h.to_string());
        }
        if let Some(tags) = infra_image_tags {
            self.infra_image_tags.write().await.insert(id, tags.clone());
        }
        if binary_hash.is_some() || definition_hash.is_some() {
            let mut sources = self.sources.write().await;
            match source {
                Some(source) => { sources.insert(id, source.clone()); }
                None => { sources.remove(&id); }
            }
        }
        Ok(Some(StoredProjectSummary {
            id,
            name: name_owned,
            description: description_owned,
        }))
    }

    async fn running_program_identity(&self, id: uuid::Uuid) -> anyhow::Result<Option<weft_core::project::hash::ProgramIdentity>> {
        let Some(binary_hash) = self.binary_hashes.read().await.get(&id).cloned() else { return Ok(None) };
        let Some(definition_hash) = self.definition_hashes.read().await.get(&id).cloned() else { return Ok(None) };
        let Some(implementations) = self.implementations.read().await.get(&(id, binary_hash.clone())).cloned() else { return Ok(None) };
        Ok(Some(weft_core::project::hash::ProgramIdentity { definition_hash, binary_hash, implementations }))
    }

    async fn program_identity(
        &self,
        id: uuid::Uuid,
        definition_hash: &str,
        binary_hash: &str,
    ) -> anyhow::Result<Option<weft_core::project::hash::ProgramIdentity>> {
        Ok(self.implementations.read().await.get(&(id, binary_hash.to_string())).cloned().map(|implementations| {
            weft_core::project::hash::ProgramIdentity {
                definition_hash: definition_hash.to_string(),
                binary_hash: binary_hash.to_string(),
                implementations,
            }
        }))
    }

    async fn program_source(&self, id: uuid::Uuid, program: &weft_core::project::hash::ProgramIdentity) -> anyhow::Result<weft_core::project::hash::Manifest> {
        anyhow::ensure!(self.running_program_identity(id).await?.as_ref() == Some(program), "project changed during preparation; register the project and retry");
        self.sources.read().await.get(&id).cloned().ok_or_else(|| anyhow::anyhow!("project sources are missing; register the project and retry"))
    }

    async fn tenant_for(&self, id: uuid::Uuid) -> anyhow::Result<Option<String>> {
        Ok(self.tenants.read().await.get(&id).cloned())
    }

    async fn list(&self, tenant: &str) -> anyhow::Result<Vec<StoredProjectSummary>> {
        let tenants = self.tenants.read().await;
        let descriptions = self.descriptions.read().await;
        Ok(self
            .inner
            .read()
            .await
            .iter()
            .filter(|(id, _)| tenants.get(id).map(|t| t == tenant).unwrap_or(false))
            .map(|(id, (name, _))| StoredProjectSummary {
                id: *id,
                name: name.clone(),
                description: descriptions.get(id).cloned().unwrap_or_default(),
            })
            .collect())
    }

    async fn get(&self, id: uuid::Uuid) -> anyhow::Result<Option<StoredProjectSummary>> {
        let descriptions = self.descriptions.read().await;
        Ok(self
            .inner
            .read()
            .await
            .get(&id)
            .map(|(name, _)| StoredProjectSummary {
                id,
                name: name.clone(),
                description: descriptions.get(&id).cloned().unwrap_or_default(),
            }))
    }

    async fn remove(&self, id: uuid::Uuid) -> anyhow::Result<bool> {
        // Mirror Postgres FK CASCADE: removing a project clears every
        // per-id side-map (binary/definition/infra hashes,
        // tenants, has_infra, transitions). Without this the
        // fake diverges from production: a test that re-registers under
        // the same id, or asserts cleanup, would see ghost state
        // Postgres does not have.
        //
        // `definition_versions` is the exception, exactly as in
        // Postgres: the recorded programs have NO foreign key to the
        // project, because the journal outlives it and a run cannot be
        // read back without the program it ran.
        // `retire_unused_definitions` is what drops them.
        let was_present = self.inner.write().await.remove(&id).is_some();
        self.sources.write().await.remove(&id);
        self.binary_hashes.write().await.remove(&id);
        self.definition_hashes.write().await.remove(&id);
        self.infra_hashes.write().await.remove(&id);
        self.tenants.write().await.remove(&id);
        self.has_infra.write().await.remove(&id);
        self.worker_overrides.write().await.remove(&id);
        self.transitions.write().await.remove(&id);
        self.build_asks.write().await.remove(&id);
        Ok(was_present)
    }

    async fn project(&self, id: uuid::Uuid) -> anyhow::Result<Option<ProjectDefinition>> {
        Ok(self
            .inner
            .read()
            .await
            .get(&id)
            .map(|(_, project)| project.clone()))
    }


    async fn running_binary_hash(&self, id: uuid::Uuid) -> anyhow::Result<Option<String>> {
        Ok(self.binary_hashes.read().await.get(&id).cloned())
    }

    async fn running_definition_hash(&self, id: uuid::Uuid) -> anyhow::Result<Option<String>> {
        Ok(self.definition_hashes.read().await.get(&id).cloned())
    }

    async fn definition_for_hash(
        &self,
        id: uuid::Uuid,
        hash: &str,
    ) -> anyhow::Result<Option<String>> {
        Ok(self
            .definition_versions
            .read()
            .await
            .get(&(id, hash.to_string()))
            .cloned())
    }

    async fn projects_with_orphan_definitions(&self) -> anyhow::Result<Vec<uuid::Uuid>> {
        let live = self.inner.read().await;
        let versions = self.definition_versions.read().await;
        let mut ids: Vec<uuid::Uuid> =
            versions.keys().map(|(id, _)| *id).filter(|id| !live.contains_key(id)).collect();
        ids.sort();
        ids.dedup();
        Ok(ids)
    }

    async fn retire_unused_definitions(
        &self,
        id: uuid::Uuid,
        still_in_use: &[String],
    ) -> anyhow::Result<u64> {
        if self.inner.read().await.contains_key(&id) {
            return Ok(0);
        }
        let mut versions = self.definition_versions.write().await;
        let before = versions.len();
        versions.retain(|(p, hash), _| *p != id || still_in_use.contains(hash));
        Ok((before - versions.len()) as u64)
    }

    async fn running_infra_hash(&self, id: uuid::Uuid) -> anyhow::Result<Option<String>> {
        Ok(self.infra_hashes.read().await.get(&id).cloned())
    }

    async fn running_infra_image_tags(&self, id: uuid::Uuid) -> anyhow::Result<Option<InfraImageTags>> {
        if !self.inner.read().await.contains_key(&id) {
            return Ok(None);
        }
        Ok(Some(self.infra_image_tags.read().await.get(&id).cloned().unwrap_or_default()))
    }

    async fn next_build_ask(&self, id: uuid::Uuid) -> anyhow::Result<i64> {
        anyhow::ensure!(self.inner.read().await.contains_key(&id), "no project {id} to take a build ask of");
        let mut asks = self.build_asks.write().await;
        let ask = asks.entry(id).or_insert(0);
        *ask += 1;
        Ok(*ask)
    }

    async fn transition(&self, id: uuid::Uuid) -> anyhow::Result<Option<ProjectTransition>> {
        if !self.inner.read().await.contains_key(&id) {
            return Ok(None);
        }
        Ok(Some(
            self.transitions
                .read()
                .await
                .get(&id)
                .map(|(t, _)| *t)
                .unwrap_or(ProjectTransition::None),
        ))
    }

    async fn try_begin_building(&self, id: uuid::Uuid, stale_before: i64) -> anyhow::Result<bool> {
        // The half of the guard that reads activations lives in
        // Postgres (a build refuses while one is mid-flip); the fake
        // project store holds no activations, so it guards the build
        // axis alone.
        let mut transitions = self.transitions.write().await;
        if !self.inner.read().await.contains_key(&id) {
            return Ok(false);
        }
        let entry = transitions.entry(id).or_insert((ProjectTransition::None, 0));
        let at_rest = entry.0 == ProjectTransition::None || (entry.0 == ProjectTransition::Building && entry.1 < stale_before);
        if !at_rest {
            return Ok(false);
        }
        *entry = (ProjectTransition::Building, crate::lease::now_unix());
        Ok(true)
    }

    async fn request_cancel_build(&self, id: uuid::Uuid) -> anyhow::Result<bool> {
        let mut transitions = self.transitions.write().await;
        match transitions.get_mut(&id) {
            Some(entry) if entry.0.is_building() => {
                entry.0 = ProjectTransition::CancellingBuild;
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    async fn release_build_driver(&self, id: uuid::Uuid) -> anyhow::Result<()> {
        if let Some(entry) = self.transitions.write().await.get_mut(&id) {
            if entry.0.is_building() {
                entry.1 = 0;
            }
        }
        Ok(())
    }

    async fn settle_building(&self, project: Option<uuid::Uuid>, stale_before: i64) -> anyhow::Result<Vec<uuid::Uuid>> {
        // The fake project store sees no version builds (they live in
        // Postgres), so a transition nothing drives is at rest.
        let mut settled = Vec::new();
        for (id, entry) in self.transitions.write().await.iter_mut() {
            if entry.0.is_building() && entry.1 < stale_before && project.is_none_or(|p| p == *id) {
                entry.0 = ProjectTransition::None;
                settled.push(*id);
            }
        }
        Ok(settled)
    }

    async fn bump_transition_heartbeat(&self, id: uuid::Uuid) -> anyhow::Result<()> {
        // As the real store: only a build transition still driven.
        if let Some(entry) = self.transitions.write().await.get_mut(&id) {
            if entry.0.is_building() && entry.1 > 0 {
                entry.1 = crate::lease::now_unix();
            }
        }
        Ok(())
    }

    async fn project_has_infra(&self, id: uuid::Uuid) -> anyhow::Result<Option<bool>> {
        Ok(self.has_infra.read().await.get(&id).copied())
    }

    async fn worker_overrides(&self, id: uuid::Uuid) -> anyhow::Result<Option<weft_platform_traits::WorkerOverrides>> {
        if !self.inner.read().await.contains_key(&id) {
            return Ok(None);
        }
        Ok(Some(self.worker_overrides.read().await.get(&id).cloned().unwrap_or_default()))
    }

    async fn set_worker_overrides(&self, id: uuid::Uuid, overrides: &weft_platform_traits::WorkerOverrides) -> anyhow::Result<()> {
        anyhow::ensure!(self.inner.read().await.contains_key(&id), "no project {id}");
        self.worker_overrides.write().await.insert(id, overrides.clone());
        Ok(())
    }

}
