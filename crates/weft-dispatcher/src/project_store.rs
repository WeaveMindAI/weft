//! Project store. Keyed by project id. Holds the registered
//! ProjectDefinition, its running hashes and its build transition. Which
//! of its triggers are listening is the activation store's
//! (`activation_store`).
//!
//! Default impl is Postgres-backed (`PostgresProjectStore`), so
//! every dispatcher Pod reads/writes the same `project` table.
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
/// image_ref }`. Written atomically alongside the running hashes (see
/// `ProjectStoreOps::set_running_hashes`); the supervisor reads it per node
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
    /// so a pod crash mid-sequence can't leave the project row
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
    /// infra. It is the single fact that decides worker placement: the
    /// worker namespace is computed on demand from it
    /// (`project_namespace::worker_namespace`), never stored, so adding
    /// or removing infra moves the worker to the right namespace
    /// without a stale stored value to reconcile.
    /// `infra_image_tags`: the COMPLETE infra image-tag map to persist in the
    /// SAME transaction as the row + definition history + hashes (`None` leaves
    /// it untouched). A build that stamps the project runnable
    /// passes it here so the runnable stamp and the infra tags land together
    /// (never a runnable project with missing/half-written tags); plain
    /// registration (no build) passes `None`.
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
    ) -> anyhow::Result<StoredProjectSummary>;

    /// Sources registered with this exact running program. Refuse a concurrent build.
    async fn program_source(&self, id: uuid::Uuid, program: &weft_core::project::hash::ProgramIdentity) -> anyhow::Result<weft_core::project::hash::Manifest>;

    /// Read graph and worker identity from one database snapshot.
    async fn running_program_identity(&self, id: uuid::Uuid) -> anyhow::Result<Option<weft_core::project::hash::ProgramIdentity>>;

    // Every reader returns `Result<Option<T>>` or `Result<Vec<T>>`:
    // `Ok(None)` / `Ok(vec![])` means "no such row" (legal), `Err`
    // means "DB failure" (callers MUST surface). Earlier this trait
    // returned bare `Option<T>` / `Vec<T>` / `bool`; transient DB
    // hiccups silently looked like "no rows" and led to wrong
    // decisions downstream (kill a healthy pod, show "no projects",
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

    /// Persist the running-hash pointers the user just built, in ONE
    /// ATOMIC write (a single UPDATE). `None` leaves a pointer
    /// untouched. The hashes:
    ///
    /// - binary: the worker docker image tag suffix (k8s manifest
    ///   builder reads it back on spawn). Flips only when something
    ///   binary-affecting changes (engine, node implementations,
    ///   node-type set, `weft.toml` build config).
    /// - definition: identifies the runtime project shape (topology +
    ///   configs). Workers fetch the definition by `(project_id,
    ///   definition_hash)`, so the row must already exist in the
    ///   `project_definition` history: setting a definition hash with
    ///   no history row is REFUSED loudly (the project must be
    ///   registered with that definition first; registering is the
    ///   only writer of history rows).
    /// - infra: drives the upgrade drift signal.
    ///
    /// - infra_image_tags: the COMPLETE per-node infra image-tag map
    ///   (`node_id -> { image_name -> image_ref }`) the supervisor
    ///   reads to resolve `Image::Local { name }`. `None` leaves the
    ///   stored map untouched; `Some(map)` REPLACES it wholesale
    ///   (every build/apply recomputes the whole set, so a merge would
    ///   only strand tags for nodes the current source no longer has).
    ///
    /// Atomicity is the point: writing the trio of hashes AND the infra
    /// tags as separate statements opens a window where a crash (or a
    /// sibling Pod's `/run` between two writes) observes a project
    /// already stamped runnable (new binary hash) but with the infra
    /// image tags absent or half-written, so a supervisor apply
    /// resolves `Image::Local { name }` to nothing and dangles. One
    /// UPDATE writes hashes + tags together: either the project becomes
    /// runnable WITH its complete infra tags, or nothing changes.
    async fn set_running_hashes(
        &self,
        id: uuid::Uuid,
        binary_hash: Option<&str>,
        definition_hash: Option<&str>,
        infra_hash: Option<&str>,
        infra_image_tags: Option<&InfraImageTags>,
    ) -> anyhow::Result<()>;

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

    /// Read the project's verb-transition marker (the build axis,
    /// orthogonal to `status`). `Ok(None)` = no such project.
    async fn transition(&self, id: uuid::Uuid) -> anyhow::Result<Option<ProjectTransition>>;

    /// Single-flight entry into the `building` transition. Atomically
    /// flips `transition` none → building IFF no other transition is
    /// in flight AND the trigger lifecycle is not mid-flip
    /// (activating / deactivating). Stamps the transition heartbeat.
    /// `Ok(false)` = lost: another verb is building, or the lifecycle
    /// is transitional; the caller rejects (409).
    async fn try_begin_building(&self, id: uuid::Uuid) -> anyhow::Result<bool>;

    /// Request cancellation of the in-flight build: CAS `transition`
    /// building → cancelling_build. The pod driving the build polls
    /// this (via `transition`) and interrupts the builder. `Ok(false)`
    /// = no build in flight (already finished, or never started).
    async fn request_cancel_build(&self, id: uuid::Uuid) -> anyhow::Result<bool>;

    /// Land the build transition back at rest: `transition` → none
    /// from either building or cancelling_build. Idempotent (a no-op
    /// when already none, e.g. the stuck-transition reaper got there
    /// first). `Ok(true)` iff this call performed the flip.
    async fn finish_building(&self, id: uuid::Uuid) -> anyhow::Result<bool>;

    /// Bump the transition heartbeat. Called on an interval by the
    /// pod DRIVING an in-process transitional state (an activation
    /// window, a build) so the stuck-transition reaper only repairs
    /// transitions whose driver actually died.
    async fn bump_transition_heartbeat(&self, id: uuid::Uuid) -> anyhow::Result<()>;

    /// Projects stuck in a build transition whose heartbeat went stale
    /// before `stale_before`: the driving pod died mid-build. The
    /// stuck-transition reaper lands each back at rest. (An activation
    /// stuck the same way lives on its own row:
    /// `ActivationStoreOps::list_stuck`.)
    async fn list_stuck_transitions(
        &self,
        stale_before: i64,
    ) -> anyhow::Result<Vec<StuckTransition>>;

    /// Whether the project declares infrastructure, by string-id (used
    /// by task executors that only see the project_id string to compute
    /// the worker namespace via `project_namespace::worker_namespace`).
    /// `Ok(Some(has_infra))` for any registered project; `Ok(None)` ONLY
    /// when the project doesn't exist; `Err` on DB failure.
    async fn project_has_infra(&self, id: uuid::Uuid) -> anyhow::Result<Option<bool>>;

    /// The project's OWN k8s namespace (where its infra pods live), or
    /// `Ok(Some(""))` / `Ok(None)` when it has none. EMPTY string means
    /// "no per-project namespace provisioned" (a no-infra project, or an
    /// infra project whose namespace hasn't been created yet); callers
    /// that need it for infra teardown treat empty as "nothing to
    /// delete". `Ok(None)` ONLY when the project doesn't exist. This is
    /// the INFRA namespace, NOT the worker namespace: for worker
    /// placement use `project_has_infra` + `worker_namespace`.
    async fn project_namespace(&self, id: uuid::Uuid) -> anyhow::Result<Option<String>>;

    /// Set the project's own k8s namespace, called when the per-project
    /// namespace is provisioned (first infra apply). Idempotent.
    async fn set_project_namespace(&self, id: uuid::Uuid, namespace: &str) -> anyhow::Result<()>;

    /// Clear the project's own k8s namespace back to empty, called when
    /// infra is torn down (project removed, or last infra node deleted),
    /// so the broker's supervisor-claim (`project_namespace <> ''`) stops
    /// managing it. Idempotent.
    async fn clear_project_namespace(&self, id: uuid::Uuid) -> anyhow::Result<()>;

    // NOTE: the infra image-tag map is written ONLY through
    // `set_running_hashes` (atomically alongside the running hashes), never
    // as a standalone per-node write, so a project can never be stamped
    // runnable with its infra tags missing/half-written. See that method.
    // The map has no per-node reader on the store: the supervisor reads it
    // through the broker, and the referenced-images keep-set reads the
    // whole column (api/project.rs), both via the canonical decode in
    // weft-broker-client.
}

/// The verb-transition marker on the project row: the BUILD axis,
/// orthogonal to the trigger lifecycle (`status`). A project is
/// `Building` while a verb's image build is in flight;
/// `CancellingBuild` after the user requested cancel and before the
/// driving pod lands the transition back at `None`. Both are
/// transitional: the reconciliation offers only `cancel_build`.
///
/// SYNC: ProjectTransition <-> packages/weft-graph/src/protocol.ts ProjectTransition,
///       packages/weft-graph/src/status.ts VALID_TRANSITIONS,
///       crates/weft-dispatcher/src/api/project.rs ProjectStatusResponse.transition
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectTransition {
    None,
    Building,
    CancellingBuild,
}

impl ProjectTransition {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Building => "building",
            Self::CancellingBuild => "cancelling_build",
        }
    }

    /// True while a build transition is in flight (either phase).
    pub fn is_building(self) -> bool {
        matches!(self, Self::Building | Self::CancellingBuild)
    }
}

/// Decode a `project.transition` column string. Unknown values are
/// schema drift and fail loud, mirroring `project_status_from_str`.
pub fn project_transition_from_str(s: &str) -> anyhow::Result<ProjectTransition> {
    match s {
        "none" => Ok(ProjectTransition::None),
        "building" => Ok(ProjectTransition::Building),
        "cancelling_build" => Ok(ProjectTransition::CancellingBuild),
        other => Err(anyhow::anyhow!("unknown project.transition column value '{other}'")),
    }
}

/// One project stuck in a build transition (stale heartbeat).
#[derive(Debug, Clone)]
pub struct StuckTransition {
    pub id: uuid::Uuid,
    pub transition: ProjectTransition,
}

/// Cloneable handle to whatever the dispatcher uses as project
/// storage. The thing on `DispatcherState` is this, not the
/// concrete impl.
pub type ProjectStore = Arc<dyn ProjectStoreOps>;

#[derive(Clone)]
pub struct PostgresProjectStore {
    pool: PgPool,
}

/// Re-export the wire-typed `ProjectStatus` so the dispatcher
/// reads/writes the same enum the broker + supervisor see over
/// HTTP. A single source of truth, generated by the `wire_enum!`
/// macro: adding a variant in `weft-broker-client::protocol`
/// shows up everywhere as a compile error. The macro also gives
/// us `as_str()`, `parse(s) -> Option<Self>`, and `Display`.
pub use weft_broker_client::protocol::ProjectStatus;

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
    // (`crate::activation_store`). The lifecycle columns this row still
    // carries (`status`, `accepting_fires`, `fires_visible_to_consumers`,
    // `fires_deadline_unix`, `deactivated_by_health`,
    // `activating_ts_color`, `drain_deadline_unix`, `activation_version`,
    // `activation_program`) are no longer read or written past the
    // insert's `status = 'registered'`; they go in a later release, once
    // no running dispatcher reads them. The `trigger_activation` group's
    // seed (`crate::activation_store::GROUP`) reads them to carry an old
    // database's lifecycle onto its triggers, so it goes in the same
    // release as the columns.
    //
    // running_binary_hash / running_definition_hash /
    // running_infra_hash drive drift detection + image tagging:
    //   - running_binary_hash: worker docker image tag suffix.
    //     Flips on engine / node-impl / node-type-set / weft.toml
    //     edits; selects the image when spawning a fresh pod.
    //   - running_definition_hash: identifies the runtime project
    //     shape (topology + configs). Workers fetch the
    //     definition at execution claim time keyed by
    //     `(project_id, definition_hash)`.
    //   - running_infra_hash: drives the Upgrade button when the
    //     CLI's freshly-computed infra hash drifts.
    //
    // tenant_id pins each project to its isolation namespace.
    // The broker uses it for scoping every user-pod-issued
    // request: a worker / listener / infra token authenticates
    // as a tenant, and any project_id it references must resolve
    // to the same tenant.
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
                accepting_fires BOOLEAN NOT NULL DEFAULT TRUE,
                fires_visible_to_consumers BOOLEAN NOT NULL DEFAULT TRUE,
                fires_deadline_unix BIGINT,
                -- True iff the CURRENT deactivation was performed by the
                -- health loop (autonomous park), not the user. Gates the
                -- health auto-recover reactivate so it never overrides a
                -- user-initiated stop/deactivate. Cleared by every
                -- non-health lifecycle write.
                deactivated_by_health BOOLEAN NOT NULL DEFAULT FALSE,
                -- The TriggerSetup color the CURRENT activation started,
                -- recorded before the run starts; NULL outside Activating.
                -- Cancel-activate and the reaper cancel exactly this run
                -- with the true cause, and treat any other non-terminal
                -- setup run as a leftover of an older, dead activation.
                activating_ts_color UUID,
                tenant_id TEXT NOT NULL,
                -- Whether this project DECLARES infrastructure (any node
                -- with requires_infra). Derived from the definition and
                -- refreshed on every register/sync, so it tracks edits
                -- that add or remove infra. Decides WORKER placement: an
                -- infra project's worker runs in the project's own k8s
                -- namespace (next to its infra pods), a no-infra
                -- project's worker runs in the shared worker namespace.
                -- The worker namespace is computed from this on demand
                -- (project_namespace::worker_namespace), never stored, so
                -- there is no stale worker-namespace value to reconcile.
                -- Set true the instant infra is declared, which is BEFORE
                -- the per-project namespace below is provisioned, so it
                -- cannot be replaced by `project_namespace <> ''`.
                has_infra BOOLEAN NOT NULL DEFAULT FALSE,
                -- The project's OWN k8s namespace
                -- (wft-project-<tenant>--<project>), where its INFRA pods
                -- and its worker live. Distinct concept from has_infra:
                -- this is the namespace string the supervisor applies
                -- into, EMPTY until the namespace is actually
                -- provisioned (first infra apply) and re-emptied when
                -- infra is torn down. The broker's supervisor-claim
                -- filters `project_namespace <> ''` to manage only
                -- projects whose namespace exists. A no-infra project
                -- keeps this empty forever (its worker lives in the
                -- shared namespace, which is not project-owned).
                project_namespace TEXT NOT NULL DEFAULT '',
                -- Per-(project, node) image hash maps for Image::Local
                -- references in InfraSpecs. CLI ships these in /sync;
                -- supervisor reads them.
                -- Shape: { "<node_id>": { "<image_name>": "<tag>" } }
                infra_image_tags_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                -- Per-project health protocols overriding the weft
                -- default. NULL = use default. Schema per
                -- weft_infra_supervisor::protocol::HealthProtocols.
                health_protocols_json JSONB,
                -- Verb-transition marker, orthogonal to `status` (the
                -- BUILD axis): 'none' | 'building' | 'cancelling_build'.
                -- Written only by its own single-flight CAS methods
                -- (try_begin_building / request_cancel_build /
                -- finish_building), never by lifecycle writes, so a
                -- deactivate can't stomp an in-flight build marker.
                transition TEXT NOT NULL DEFAULT 'none',
                -- While status='deactivating' with runningPolicy=wait:
                -- the unix second past which the drain gives up (the
                -- reaper cancels the remaining executions and the
                -- drain-watcher lands the row). NULL elsewhere.
                drain_deadline_unix BIGINT,
                -- Heartbeat for driver-backed transitional states
                -- (status='activating', transition='building'/
                -- 'cancelling_build'): the pod driving the transition
                -- bumps this on an interval; the stuck-transition
                -- reaper repairs rows whose heartbeat went stale
                -- (the driver died mid-transition). Per-project and
                -- status-guarded: this replaces the old boot-time
                -- blind bulk downgrade, which wiped live status for
                -- every tenant's projects on any Pod restart.
                transition_heartbeat_unix BIGINT NOT NULL DEFAULT 0,
                -- The version tree's HEAD (`crate::versions`): the version
                -- the next checkpoint or run parents on, the run the next
                -- `--seed` inherits from (NULL when head is a bare
                -- version), and the version the triggers were activated
                -- on (NULL while inactive). Moved by checkpoint, run,
                -- branch and activate; nothing lives on disk.
                head_version TEXT,
                head_run UUID,
                activation_version TEXT,
                activation_program JSONB
            )"#,
        "CREATE INDEX IF NOT EXISTS idx_project_tenant ON project(tenant_id)",
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
        // interrupted mid-transition (a pod died while activating /
        // building / deactivating) is the stuck-transition reaper's
        // job (`reaper::sweep_stuck_transitions`): per-project,
        // heartbeat-gated, and status-guarded, so it never wipes
        // another Pod's live state. A constructor-time bulk downgrade
        // would run in EVERY replica on EVERY boot and reset live
        // status for all tenants (a multi-Pod correctness bug).
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
/// guard. Both writers go through here: `set_running_hashes` on a pool
/// connection, `register_with_hashes` inside its transaction (after the history
/// INSERT, which the same-snapshot EXISTS check then sees). A `None` argument
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
    ) -> anyhow::Result<StoredProjectSummary> {
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
        Ok(StoredProjectSummary { id, name, description })
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
        // A member is a member of this project only: their connections,
        // picks and tokens reach nothing once it is gone, and neither the
        // author nor the member could list them to delete them.
        weft_access_store::forget_project_members(&mut tx, id).await?;
        crate::journal::postgres::revoke_project_member_tokens(&mut *tx, id).await?;
        tx.commit().await?;
        Ok(res.rows_affected() > 0)
    }

    async fn set_running_hashes(
        &self,
        id: uuid::Uuid,
        binary_hash: Option<&str>,
        definition_hash: Option<&str>,
        infra_hash: Option<&str>,
        infra_image_tags: Option<&InfraImageTags>,
    ) -> anyhow::Result<()> {
        // ONE UPDATE = atomic trio of hashes PLUS the infra tag map: a crash
        // (or a sibling Pod's /run between statements) can never observe a
        // half-advanced pointer set, nor a runnable project whose infra tags
        // are missing/half-written. The definition-history precondition and the
        // loud zero-rows failure live in `advance_running_hashes`, shared with
        // `register_with_hashes`' transaction.
        let mut conn = self.pool.acquire().await?;
        advance_running_hashes(
            &mut conn,
            id,
            binary_hash,
            definition_hash,
            infra_hash,
            infra_image_tags,
        )
        .await
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

    async fn transition(&self, id: uuid::Uuid) -> anyhow::Result<Option<ProjectTransition>> {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT transition FROM project WHERE id = $1")
                .bind(id)
                .fetch_optional(&self.pool)
                .await?;
        row.map(|(t,)| project_transition_from_str(&t)).transpose()
    }

    async fn try_begin_building(&self, id: uuid::Uuid) -> anyhow::Result<bool> {
        let res = sqlx::query(
            "UPDATE project \
             SET transition = 'building', transition_heartbeat_unix = $1 \
             WHERE id = $2 \
               AND transition = 'none' \
               AND NOT EXISTS (SELECT 1 FROM trigger_activation a \
                               WHERE a.project_id = $2 AND a.status IN ('activating', 'deactivating'))",
        )
        .bind(crate::lease::now_unix())
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected() > 0)
    }

    async fn request_cancel_build(&self, id: uuid::Uuid) -> anyhow::Result<bool> {
        let res = sqlx::query(
            "UPDATE project SET transition = 'cancelling_build' \
             WHERE id = $1 AND transition = 'building'",
        )
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected() > 0)
    }

    async fn finish_building(&self, id: uuid::Uuid) -> anyhow::Result<bool> {
        let res = sqlx::query(
            "UPDATE project SET transition = 'none' \
             WHERE id = $1 AND transition IN ('building', 'cancelling_build')",
        )
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected() > 0)
    }

    async fn bump_transition_heartbeat(&self, id: uuid::Uuid) -> anyhow::Result<()> {
        sqlx::query("UPDATE project SET transition_heartbeat_unix = $1 WHERE id = $2")
            .bind(crate::lease::now_unix())
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn list_stuck_transitions(
        &self,
        stale_before: i64,
    ) -> anyhow::Result<Vec<StuckTransition>> {
        // NOTE (compiler-invisible status set): this WHERE names the
        // driver-backed transitional states by string. Adding a new
        // driver-backed transitional state means adding it HERE too;
        // the Rust exhaustiveness checker cannot flag this SQL.
        let rows: Vec<(uuid::Uuid, String)> = sqlx::query_as(
            "SELECT id, transition FROM project \
             WHERE transition IN ('building', 'cancelling_build') \
               AND transition_heartbeat_unix < $1",
        )
        .bind(stale_before)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|(id, transition)| {
                Ok(StuckTransition { id, transition: project_transition_from_str(&transition)? })
            })
            .collect()
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

    async fn project_namespace(&self, id: uuid::Uuid) -> anyhow::Result<Option<String>> {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT project_namespace FROM project WHERE id = $1")
                .bind(id)
                .fetch_optional(&self.pool)
                .await?;
        Ok(row.map(|(s,)| s))
    }

    async fn set_project_namespace(&self, id: uuid::Uuid, namespace: &str) -> anyhow::Result<()> {
        sqlx::query("UPDATE project SET project_namespace = $2 WHERE id = $1")
            .bind(id)
            .bind(namespace)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn clear_project_namespace(&self, id: uuid::Uuid) -> anyhow::Result<()> {
        sqlx::query("UPDATE project SET project_namespace = '' WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await?;
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
    /// The project's own infra namespace, empty until provisioned. Set
    /// by `set_project_namespace`, cleared by `clear_project_namespace`.
    namespaces: RwLock<HashMap<uuid::Uuid, String>>,
    /// Mirror of the `transition` + `transition_heartbeat_unix`
    /// columns. Missing entry = (None, 0), matching the column
    /// defaults on a fresh row.
    transitions: RwLock<HashMap<uuid::Uuid, (ProjectTransition, i64)>>,
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
            definition_versions: RwLock::new(HashMap::new()),
            tenants: RwLock::new(HashMap::new()),
            descriptions: RwLock::new(HashMap::new()),
            has_infra: RwLock::new(HashMap::new()),
            namespaces: RwLock::new(HashMap::new()),
            transitions: RwLock::new(HashMap::new()),
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
        // The fake does not model the infra image-tag column (the real
        // readers are SQL-direct: the broker handler and the
        // referenced-images keep-set); accepted to satisfy the trait,
        // ignored.
        _infra_image_tags: Option<&InfraImageTags>,
        implementations: Option<&std::collections::BTreeMap<String, String>>,
        source: Option<&weft_core::project::hash::Manifest>,
    ) -> anyhow::Result<StoredProjectSummary> {
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
        if binary_hash.is_some() || definition_hash.is_some() {
            let mut sources = self.sources.write().await;
            match source {
                Some(source) => { sources.insert(id, source.clone()); }
                None => { sources.remove(&id); }
            }
        }
        Ok(StoredProjectSummary {
            id,
            name: name_owned,
            description: description_owned,
        })
    }

    async fn running_program_identity(&self, id: uuid::Uuid) -> anyhow::Result<Option<weft_core::project::hash::ProgramIdentity>> {
        let Some(binary_hash) = self.binary_hashes.read().await.get(&id).cloned() else { return Ok(None) };
        let Some(definition_hash) = self.definition_hashes.read().await.get(&id).cloned() else { return Ok(None) };
        let Some(implementations) = self.implementations.read().await.get(&(id, binary_hash.clone())).cloned() else { return Ok(None) };
        Ok(Some(weft_core::project::hash::ProgramIdentity { definition_hash, binary_hash, implementations }))
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
        // tenants, has_infra, namespaces, transitions). Without this the
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
        self.namespaces.write().await.remove(&id);
        self.transitions.write().await.remove(&id);
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

    async fn set_running_hashes(
        &self,
        id: uuid::Uuid,
        binary_hash: Option<&str>,
        definition_hash: Option<&str>,
        infra_hash: Option<&str>,
        // The fake does not model the infra image-tag column (the real
        // readers are SQL-direct: the broker handler and the
        // referenced-images keep-set); accepted to satisfy the trait,
        // ignored.
        _infra_image_tags: Option<&InfraImageTags>,
    ) -> anyhow::Result<()> {
        if !self.inner.read().await.contains_key(&id) {
            anyhow::bail!("set_running_hashes: project {id} not found");
        }
        // Mirror the Postgres precondition BEFORE any write: refuse to
        // advance the pointer to a hash with no history row, leaving
        // the trio untouched (the production statement is one atomic
        // UPDATE, so a refused definition also never advances binary /
        // infra).
        if let Some(hash) = definition_hash {
            if !self
                .definition_versions
                .read()
                .await
                .contains_key(&(id, hash.to_string()))
            {
                anyhow::bail!(
                    "refuse to set running_definition_hash to {hash} for project {id}: \
                     no project_definition history row exists for that hash; \
                     register the project with this definition first"
                );
            }
        }
        if let Some(h) = binary_hash {
            let old = self.binary_hashes.write().await.insert(id, h.to_string());
            if old.as_deref() != Some(h) { self.sources.write().await.remove(&id); }
        }
        if let Some(h) = definition_hash {
            let old = self.definition_hashes.write().await.insert(id, h.to_string());
            if old.as_deref() != Some(h) { self.sources.write().await.remove(&id); }
        }
        if let Some(h) = infra_hash {
            self.infra_hashes.write().await.insert(id, h.to_string());
        }
        Ok(())
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

    async fn try_begin_building(&self, id: uuid::Uuid) -> anyhow::Result<bool> {
        // The half of the guard that reads activations lives in
        // Postgres (a build refuses while one is mid-flip); the fake
        // project store holds no activations, so it guards the build
        // axis alone.
        let mut transitions = self.transitions.write().await;
        if !self.inner.read().await.contains_key(&id) {
            return Ok(false);
        }
        let entry = transitions.entry(id).or_insert((ProjectTransition::None, 0));
        if entry.0 != ProjectTransition::None {
            return Ok(false);
        }
        *entry = (ProjectTransition::Building, crate::lease::now_unix());
        Ok(true)
    }

    async fn request_cancel_build(&self, id: uuid::Uuid) -> anyhow::Result<bool> {
        let mut transitions = self.transitions.write().await;
        match transitions.get_mut(&id) {
            Some(entry) if entry.0 == ProjectTransition::Building => {
                entry.0 = ProjectTransition::CancellingBuild;
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    async fn finish_building(&self, id: uuid::Uuid) -> anyhow::Result<bool> {
        let mut transitions = self.transitions.write().await;
        match transitions.get_mut(&id) {
            Some(entry) if entry.0.is_building() => {
                entry.0 = ProjectTransition::None;
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    async fn bump_transition_heartbeat(&self, id: uuid::Uuid) -> anyhow::Result<()> {
        self.transitions
            .write()
            .await
            .entry(id)
            .or_insert((ProjectTransition::None, 0))
            .1 = crate::lease::now_unix();
        Ok(())
    }

    async fn list_stuck_transitions(
        &self,
        stale_before: i64,
    ) -> anyhow::Result<Vec<StuckTransition>> {
        Ok(self
            .transitions
            .read()
            .await
            .iter()
            .filter(|(_, (transition, hb))| transition.is_building() && *hb < stale_before)
            .map(|(id, (transition, _))| StuckTransition { id: *id, transition: *transition })
            .collect())
    }

    async fn project_has_infra(&self, id: uuid::Uuid) -> anyhow::Result<Option<bool>> {
        Ok(self.has_infra.read().await.get(&id).copied())
    }

    async fn project_namespace(&self, id: uuid::Uuid) -> anyhow::Result<Option<String>> {
        // Mirror Postgres: a registered project always has a row (empty
        // string until its namespace is provisioned); only an
        // unregistered project returns None.
        if !self.inner.read().await.contains_key(&id) {
            return Ok(None);
        }
        Ok(Some(
            self.namespaces.read().await.get(&id).cloned().unwrap_or_default(),
        ))
    }

    async fn set_project_namespace(&self, id: uuid::Uuid, namespace: &str) -> anyhow::Result<()> {
        self.namespaces.write().await.insert(id, namespace.to_string());
        Ok(())
    }

    async fn clear_project_namespace(&self, id: uuid::Uuid) -> anyhow::Result<()> {
        self.namespaces.write().await.insert(id, String::new());
        Ok(())
    }

}
