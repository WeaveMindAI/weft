//! Project store. Keyed by project id. Holds the registered
//! ProjectDefinition + the lifecycle status.
//!
//! Default impl is Postgres-backed (`PostgresProjectStore`), so
//! every dispatcher Pod reads/writes the same `project` table.
//! Tests use `MockProjectStore` (in-memory HashMap).

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
/// `ProjectStoreOps::set_running_hashes`); the supervisor reads it per node to
/// resolve `Image::Local { name }`.
pub type InfraImageTags =
    std::collections::BTreeMap<String, std::collections::HashMap<String, String>>;

/// Backing store for project metadata. Implementations:
/// - `PostgresProjectStore` (production)
/// - `MockProjectStore` (tests, behind `test-helpers`).
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
    ) -> anyhow::Result<StoredProjectSummary>;

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

    /// Read the stored infra hash. Same Result contract as
    /// `running_binary_hash`.
    async fn running_infra_hash(&self, id: uuid::Uuid) -> anyhow::Result<Option<String>>;

    /// Read the project's full lifecycle: status + the three
    /// orthogonal axes that control fire acceptance, consumer
    /// visibility, and the optional acceptance deadline.
    /// `Ok(None)` = no such project; `Err` = DB failure.
    async fn lifecycle(&self, id: uuid::Uuid) -> anyhow::Result<Option<ProjectLifecycle>>;

    /// Atomically set every lifecycle field, GUARDED: the write is
    /// refused while the project is `Activating` (cancel the
    /// activation first) or while a build transition is in flight
    /// (cancel the build first). Deactivate's writer; there is no
    /// blind lifecycle write anywhere (the CAS variants below cover
    /// every other transition).
    async fn set_lifecycle_guarded(
        &self,
        id: uuid::Uuid,
        lifecycle: &ProjectLifecycle,
    ) -> anyhow::Result<LifecycleWrite>;

    /// Compare-and-set on status. `Ok(true)` iff a row matched
    /// `from` and was updated to `to`. Used by the drain-watcher to
    /// flip deactivating → inactive without racing a concurrent
    /// activate.
    async fn cas_status(
        &self,
        id: uuid::Uuid,
        from: ProjectStatus,
        to: ProjectStatus,
    ) -> anyhow::Result<bool>;

    /// CAS that swaps every lifecycle field at once when the row's
    /// current status matches `from`. Used by activate to flip
    /// Activating → Active (with full lifecycle: accepting=true,
    /// visible=true) and by cancel-activate to flip Activating →
    /// Inactive. `Ok(true)` iff the swap landed.
    async fn cas_lifecycle(
        &self,
        id: uuid::Uuid,
        from: ProjectStatus,
        to: &ProjectLifecycle,
    ) -> anyhow::Result<bool>;

    /// Single-flight entry into activation. Atomically set the full
    /// `activating()` lifecycle IFF the project is NOT already
    /// `Activating`. `Activating` is the one mutual-exclusion state:
    /// while a project is activating (registering trigger signals),
    /// nothing may start another activation. `Ok(true)` iff this call
    /// won the transition; `Ok(false)` means an activation is already
    /// in flight (the caller should reject, e.g. 409).
    ///
    /// Every other status is a legal entry: Registered/Inactive (the
    /// action bar), and Active/Deactivating (sync's auto-reactivate
    /// after an infra upgrade, which may call while a wait-mode
    /// deactivate is still draining; the roll-forward `resume_active`
    /// verb). A drain-watcher CAS that loses to this transition
    /// already tolerates the loss and retries. Also refused while a
    /// build transition is in flight (`transition <> 'none'`).
    /// Stamps the transition heartbeat so the stuck-transition reaper
    /// can tell a live activation (driver bumping) from an orphaned
    /// one (driver pod died).
    async fn try_begin_activating(&self, id: uuid::Uuid) -> anyhow::Result<bool>;

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

    /// Projects stuck in a driver-backed transitional state (status
    /// `activating`, or a build transition) whose heartbeat went stale
    /// before `stale_before`: the driving pod died mid-transition.
    /// The stuck-transition reaper repairs each, status-guarded.
    async fn list_stuck_transitions(
        &self,
        stale_before: i64,
    ) -> anyhow::Result<Vec<StuckTransition>>;

    /// Projects currently in status `deactivating`. The reaper feeds
    /// each through the drain-watcher CAS so a deactivation whose
    /// terminal events were missed (dispatcher restart) still lands.
    async fn list_deactivating(&self) -> anyhow::Result<Vec<uuid::Uuid>>;

    /// Whether the project declares infrastructure, by string-id (used
    /// by task executors that only see the project_id string to compute
    /// the worker namespace via `project_namespace::worker_namespace`).
    /// `Ok(Some(has_infra))` for any registered project; `Ok(None)` ONLY
    /// when the project doesn't exist; `Err` on DB failure.
    async fn project_has_infra(&self, id_str: &str) -> anyhow::Result<Option<bool>>;

    /// The project's OWN k8s namespace (where its infra pods live), or
    /// `Ok(Some(""))` / `Ok(None)` when it has none. EMPTY string means
    /// "no per-project namespace provisioned" (a no-infra project, or an
    /// infra project whose namespace hasn't been created yet); callers
    /// that need it for infra teardown treat empty as "nothing to
    /// delete". `Ok(None)` ONLY when the project doesn't exist. This is
    /// the INFRA namespace, NOT the worker namespace: for worker
    /// placement use `project_has_infra` + `worker_namespace`.
    async fn project_namespace(&self, id_str: &str) -> anyhow::Result<Option<String>>;

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

    /// Read the per-(project, node) image-tag map. Empty map if
    /// never set. `Err` on DB failure (vs empty-map = "set to
    /// empty", which is legal if a project has no Local images).
    async fn infra_image_tags(
        &self,
        project_id_str: &str,
        node_id: &str,
    ) -> anyhow::Result<std::collections::HashMap<String, String>>;

    /// Persist the project's HealthProtocols override (JSON shape
    /// per supervisor `HealthProtocols`). `None` payload = use weft
    /// defaults. `Ok(true)` iff a row was updated.
    async fn set_health_protocols(
        &self,
        id: uuid::Uuid,
        protocols: Option<serde_json::Value>,
    ) -> anyhow::Result<bool>;

    /// Read the project's HealthProtocols override. `Ok(None)` when
    /// the project uses defaults OR no such project; `Err` on DB
    /// failure.
    async fn health_protocols(
        &self,
        project_id_str: &str,
    ) -> anyhow::Result<Option<serde_json::Value>>;
}

/// Snapshot of every lifecycle field on a project row. Returned
/// from `lifecycle(id)`; passed to `set_lifecycle(id, &)`.
///
/// `ProjectStatus::Registered` projects always carry the default
/// "fresh" axes (accepting=true, visible=true, no deadline) since
/// they have no signals registered yet; the gate never sees them
/// because no signal row exists.
#[derive(Debug, Clone)]
pub struct ProjectLifecycle {
    pub status: ProjectStatus,
    pub accepting_fires: bool,
    pub fires_visible_to_consumers: bool,
    pub fires_deadline_unix: Option<i64>,
    /// While `status = Deactivating` with `runningPolicy = wait`:
    /// the unix second past which the drain gives up, cancels the
    /// remaining executions, and lands Inactive (enforced by the
    /// stuck-transition reaper). The user's "wait at most N, then
    /// proceed" cap, same semantics as the infra drains. `None` on
    /// every other state (the non-deactivating constructors clear it).
    pub drain_deadline_unix: Option<i64>,
    /// True iff the CURRENT deactivation was performed by the health
    /// loop (autonomous park because infra broke), NOT by the user.
    /// The health loop's auto-recover reactivate fires ONLY when this
    /// is true, so it never overrides a deactivation the user did
    /// themselves (stop / upgrade / terminate / manual deactivate):
    /// the user is present for those and doesn't want a surprise
    /// reactivation. Every non-health lifecycle constructor sets it
    /// false; only `deactivate_project_with_mode(by_health=true)`
    /// (the claimer's health-park path) sets it true.
    pub deactivated_by_health: bool,
}

impl ProjectLifecycle {
    /// Live ("active"): worker spawns, fires execute immediately.
    pub fn active() -> Self {
        Self {
            status: ProjectStatus::Active,
            accepting_fires: true,
            fires_visible_to_consumers: true,
            fires_deadline_unix: None,
            deactivated_by_health: false,
            drain_deadline_unix: None,
        }
    }

    /// Transient state while TriggerSetup runs. The gate parks
    /// incoming fires (the listener may not have every signal
    /// registered yet, so relaying could 404). Consumer
    /// enumeration is hidden because the trigger set is not yet
    /// canonical. The drain at the end of activate replays every
    /// parked payload through the now-Active gate.
    pub fn activating() -> Self {
        Self {
            status: ProjectStatus::Activating,
            accepting_fires: true,
            fires_visible_to_consumers: false,
            fires_deadline_unix: None,
            deactivated_by_health: false,
            drain_deadline_unix: None,
        }
    }

    /// "wipe": every signal row + execution gone; the gate refuses
    /// any fire. Equivalent to "the project was never activated."
    pub fn wiped() -> Self {
        Self {
            status: ProjectStatus::Inactive,
            accepting_fires: false,
            fires_visible_to_consumers: false,
            fires_deadline_unix: None,
            deactivated_by_health: false,
            drain_deadline_unix: None,
        }
    }

    /// "hibernate": parking until the deadline, then refuses.
    /// Hidden from consumer enumeration the entire time.
    pub fn hibernating(deadline_unix: i64) -> Self {
        Self {
            status: ProjectStatus::Inactive,
            accepting_fires: true,
            fires_visible_to_consumers: false,
            fires_deadline_unix: Some(deadline_unix),
            deactivated_by_health: false,
            drain_deadline_unix: None,
        }
    }

    /// "park": parking forever, visible to consumers so they can
    /// browse + submit. Submissions still get parked.
    pub fn parked() -> Self {
        Self {
            status: ProjectStatus::Inactive,
            accepting_fires: true,
            fires_visible_to_consumers: true,
            fires_deadline_unix: None,
            deactivated_by_health: false,
            drain_deadline_unix: None,
        }
    }

    /// Transient state while waiting for running executions to
    /// drain. Carries the same accepting/visible/deadline values
    /// as the deactivate target so the gate behavior is already
    /// correct from the moment the user clicks deactivate; only
    /// `status` flips to Inactive once the drain completes.
    pub fn deactivating_to(target: ProjectLifecycle) -> Self {
        Self {
            status: ProjectStatus::Deactivating,
            accepting_fires: target.accepting_fires,
            fires_visible_to_consumers: target.fires_visible_to_consumers,
            fires_deadline_unix: target.fires_deadline_unix,
            // Carry the target's flag so a health-park that drains
            // through Deactivating keeps `deactivated_by_health` set
            // the whole way (the gate is correct from the first write).
            deactivated_by_health: target.deactivated_by_health,
            // Set by the deactivate path from the user's drain cap
            // (this constructor doesn't know it).
            drain_deadline_unix: None,
        }
    }

    /// User-facing mode label derived from the axes. Used by the
    /// status response so the CLI / extension can render a single
    /// string ("active", "wipe", "hibernate", "park", "deactivating")
    /// without reverse-engineering the booleans.
    pub fn mode_label(&self) -> &'static str {
        match self.status {
            ProjectStatus::Registered => "registered",
            ProjectStatus::Activating => "activating",
            ProjectStatus::Active => "active",
            ProjectStatus::Deactivating => "deactivating",
            ProjectStatus::Inactive => {
                if !self.accepting_fires {
                    "wipe"
                } else if !self.fires_visible_to_consumers {
                    "hibernate"
                } else {
                    "park"
                }
            }
        }
    }
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

/// Outcome of a guarded lifecycle write. `Rejected` carries the state
/// that blocked it so the caller can name it in the error message.
#[derive(Debug, Clone)]
pub enum LifecycleWrite {
    Applied,
    Rejected {
        status: ProjectStatus,
        transition: ProjectTransition,
    },
    NotFound,
}

/// One project stuck in a driver-backed transitional state (stale
/// heartbeat). What "repair" means depends on which state it is stuck
/// in; the reaper branches on the pair.
#[derive(Debug, Clone)]
pub struct StuckTransition {
    pub id: uuid::Uuid,
    pub status: ProjectStatus,
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
    pub status: ProjectStatus,
}

impl PostgresProjectStore {
    pub async fn new(pool: PgPool) -> anyhow::Result<Self> {
        migrate(&pool).await?;
        Ok(Self { pool })
    }
}

/// Create the `project` + `project_definition` tables. The canonical CREATEs
/// live here (edited in place, fresh DB on rebuild); `PostgresProjectStore::new`
/// runs this, and any caller that needs `project(id)` present runs it first
/// (idempotent, safe to repeat).
pub async fn migrate(pool: &PgPool) -> anyhow::Result<()> {
    // project: one row per registered project. Lifecycle is the
    // status enum plus three orthogonal axes that the gate,
    // enumeration filter, and reaper read from a single source
    // of truth:
    //
    //   accepting_fires:            gate passes/parks fires when
    //                               true; refuses when false.
    //   fires_visible_to_consumers: token-scoped enumeration
    //                               returns the project's
    //                               signals when true; hides
    //                               them when false.
    //   fires_deadline_unix:        Some(t) means "accepting
    //                               only until t"; gate refuses
    //                               after the deadline. None =
    //                               no deadline.
    //
    // User-facing wipe/hibernate/park modes map onto these:
    //   wipe          → status=inactive, accepting=false,
    //                    visible=false, deadline=None (rows gone)
    //   hibernate     → status=inactive, accepting=true,
    //                    visible=false, deadline=Some(now+grace)
    //   park          → status=inactive, accepting=true,
    //                    visible=true,  deadline=None
    //   active        → status=active, accepting=true,
    //                    visible=true,  deadline=None
    //   deactivating  → status=deactivating; accepting/visible/
    //                    deadline already set to the target
    //                    mode's values; new fires park
    //                    immediately while running execs drain.
    //                    Journal bridge CASes status to
    //                    inactive once the running set empties.
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
    sqlx::query(
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
                accepting_fires BOOLEAN NOT NULL DEFAULT TRUE,
                fires_visible_to_consumers BOOLEAN NOT NULL DEFAULT TRUE,
                fires_deadline_unix BIGINT,
                -- True iff the CURRENT deactivation was performed by the
                -- health loop (autonomous park), not the user. Gates the
                -- health auto-recover reactivate so it never overrides a
                -- user-initiated stop/deactivate. Cleared by every
                -- non-health lifecycle write.
                deactivated_by_health BOOLEAN NOT NULL DEFAULT FALSE,
                tenant_id TEXT NOT NULL DEFAULT 'local',
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
                -- this is the namespace string the supervisor runs
                -- kubectl against, EMPTY until the namespace is actually
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
                transition_heartbeat_unix BIGINT NOT NULL DEFAULT 0
            )"#,
    )
    .execute(pool)
    .await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_project_tenant ON project(tenant_id)")
        .execute(pool)
        .await?;

    // Append-only definition-version history. Workers fetch by
    // (project_id, definition_hash) so a suspended execution
    // can always resume on the EXACT shape it was started on,
    // even after the user has edited and re-registered. Without
    // this, the `project.project_json` column would only carry
    // the LATEST shape and a resume after edit would run the
    // wrong topology against the journal's old state.
    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS project_definition (
            project_id UUID NOT NULL,
            definition_hash TEXT NOT NULL,
            project_json TEXT NOT NULL,
            recorded_at_unix BIGINT NOT NULL,
            PRIMARY KEY (project_id, definition_hash),
            FOREIGN KEY (project_id) REFERENCES project(id) ON DELETE CASCADE
        )"#,
    )
    .execute(pool)
    .await?;

    // A project's source lives as a folder on disk, edited via the CLI / VS
    // Code; the dispatcher tracks no version chain for it.

    // NO boot-time status touch-up here. Recovery of a project
    // interrupted mid-transition (a pod died while activating /
    // building / deactivating) is the stuck-transition reaper's
    // job (`reaper::sweep_stuck_transitions`): per-project,
    // heartbeat-gated, and status-guarded, so it never wipes
    // another Pod's live state. A constructor-time bulk downgrade
    // would run in EVERY replica on EVERY boot and reset live
    // status for all tenants (a multi-Pod correctness bug).

    Ok(())
}

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
        // The conflict arm deliberately does NOT touch `status`: an
        // Active project stays active through a re-register.
        // RETURNING the (possibly preserved) status keeps the summary
        // honest instead of hardcoding "registered".
        //
        // The conflict arm is GUARDED by `WHERE project.tenant_id =
        // EXCLUDED.tenant_id`: a re-register may only update a row that already
        // belongs to the same tenant. Without this guard the upsert would let
        // any tenant re-register an existing project id and overwrite its
        // `tenant_id`, seizing another tenant's project. When the tenants
        // differ the UPDATE matches no row and (because the id already exists,
        // so the INSERT is suppressed) the statement returns NO row; we detect
        // that and fail loudly as a cross-tenant collision.
        let status_str: Option<(String,)> = sqlx::query_as(
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
             RETURNING status",
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
        let (status_str,) = status_str.ok_or_else(|| {
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
        // Runnable stamp + infra tags in ONE statement inside this same tx, so a
        // build that registers-and-stamps never leaves a runnable
        // project with missing/half-written infra tags. Plain registration
        // passes `infra_image_tags = None`.
        advance_running_hashes(
            &mut *tx,
            id,
            binary_hash,
            definition_hash,
            infra_hash,
            infra_image_tags,
        )
        .await?;
        tx.commit().await?;
        Ok(StoredProjectSummary {
            id,
            name,
            description,
            status: project_status_from_str(&status_str)?,
        })
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
        let rows: Vec<(uuid::Uuid, String, String, String)> = sqlx::query_as(
            "SELECT id, name, description, status FROM project WHERE tenant_id = $1 ORDER BY name",
        )
        .bind(tenant)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|(id, name, description, status)| {
                Ok(StoredProjectSummary {
                    id,
                    name,
                    description,
                    status: project_status_from_str(&status)?,
                })
            })
            .collect()
    }

    async fn get(&self, id: uuid::Uuid) -> anyhow::Result<Option<StoredProjectSummary>> {
        let row: Option<(uuid::Uuid, String, String, String)> = sqlx::query_as(
            "SELECT id, name, description, status FROM project WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(|(id, name, description, status)| {
            Ok(StoredProjectSummary {
                id,
                name,
                description,
                status: project_status_from_str(&status)?,
            })
        })
        .transpose()
    }

    async fn remove(&self, id: uuid::Uuid) -> anyhow::Result<bool> {
        let res = sqlx::query("DELETE FROM project WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await?;
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
            &mut *conn,
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

    async fn running_infra_hash(&self, id: uuid::Uuid) -> anyhow::Result<Option<String>> {
        let row: Option<(Option<String>,)> = sqlx::query_as(
            "SELECT running_infra_hash FROM project WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.and_then(|(h,)| h))
    }

    async fn lifecycle(&self, id: uuid::Uuid) -> anyhow::Result<Option<ProjectLifecycle>> {
        let row: Option<(String, bool, bool, Option<i64>, bool, Option<i64>)> = sqlx::query_as(
            "SELECT status, accepting_fires, fires_visible_to_consumers, fires_deadline_unix, \
                    deactivated_by_health, drain_deadline_unix \
             FROM project WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(|(status, accepting, visible, deadline, by_health, drain_deadline)| {
            Ok(ProjectLifecycle {
                status: project_status_from_str(&status)?,
                accepting_fires: accepting,
                fires_visible_to_consumers: visible,
                fires_deadline_unix: deadline,
                deactivated_by_health: by_health,
                drain_deadline_unix: drain_deadline,
            })
        })
        .transpose()
    }

    async fn set_lifecycle_guarded(
        &self,
        id: uuid::Uuid,
        lifecycle: &ProjectLifecycle,
    ) -> anyhow::Result<LifecycleWrite> {
        let res = sqlx::query(
            "UPDATE project \
             SET status = $1, \
                 accepting_fires = $2, \
                 fires_visible_to_consumers = $3, \
                 fires_deadline_unix = $4, \
                 deactivated_by_health = $5, \
                 drain_deadline_unix = $6, \
                 updated_at = $7 \
             WHERE id = $8 \
               AND status <> 'activating' \
               AND transition = 'none'",
        )
        .bind(lifecycle.status.as_str())
        .bind(lifecycle.accepting_fires)
        .bind(lifecycle.fires_visible_to_consumers)
        .bind(lifecycle.fires_deadline_unix)
        .bind(lifecycle.deactivated_by_health)
        .bind(lifecycle.drain_deadline_unix)
        .bind(crate::lease::now_unix())
        .bind(id)
        .execute(&self.pool)
        .await?;
        if res.rows_affected() > 0 {
            return Ok(LifecycleWrite::Applied);
        }
        // Zero rows: no project, or the guard refused. Re-read to say
        // which (and which state blocked) so the caller can 404 vs 409
        // with a message that names the blocker.
        let row: Option<(String, String)> =
            sqlx::query_as("SELECT status, transition FROM project WHERE id = $1")
                .bind(id)
                .fetch_optional(&self.pool)
                .await?;
        match row {
            None => Ok(LifecycleWrite::NotFound),
            Some((status, transition)) => Ok(LifecycleWrite::Rejected {
                status: project_status_from_str(&status)?,
                transition: project_transition_from_str(&transition)?,
            }),
        }
    }

    async fn cas_status(
        &self,
        id: uuid::Uuid,
        from: ProjectStatus,
        to: ProjectStatus,
    ) -> anyhow::Result<bool> {
        let res = sqlx::query(
            "UPDATE project SET status = $1, updated_at = $2 \
             WHERE id = $3 AND status = $4",
        )
        .bind(to.as_str())
        .bind(crate::lease::now_unix())
        .bind(id)
        .bind(from.as_str())
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected() > 0)
    }

    async fn cas_lifecycle(
        &self,
        id: uuid::Uuid,
        from: ProjectStatus,
        to: &ProjectLifecycle,
    ) -> anyhow::Result<bool> {
        let res = sqlx::query(
            "UPDATE project \
             SET status = $1, \
                 accepting_fires = $2, \
                 fires_visible_to_consumers = $3, \
                 fires_deadline_unix = $4, \
                 deactivated_by_health = $5, \
                 drain_deadline_unix = $6, \
                 updated_at = $7 \
             WHERE id = $8 AND status = $9",
        )
        .bind(to.status.as_str())
        .bind(to.accepting_fires)
        .bind(to.fires_visible_to_consumers)
        .bind(to.fires_deadline_unix)
        .bind(to.deactivated_by_health)
        .bind(to.drain_deadline_unix)
        .bind(crate::lease::now_unix())
        .bind(id)
        .bind(from.as_str())
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected() > 0)
    }

    async fn try_begin_activating(&self, id: uuid::Uuid) -> anyhow::Result<bool> {
        let activating = ProjectLifecycle::activating();
        let now = crate::lease::now_unix();
        let res = sqlx::query(
            "UPDATE project \
             SET status = $1, \
                 accepting_fires = $2, \
                 fires_visible_to_consumers = $3, \
                 fires_deadline_unix = $4, \
                 deactivated_by_health = $5, \
                 drain_deadline_unix = NULL, \
                 updated_at = $6, \
                 transition_heartbeat_unix = $6 \
             WHERE id = $7 AND status <> 'activating' AND transition = 'none'",
        )
        .bind(activating.status.as_str())
        .bind(activating.accepting_fires)
        .bind(activating.fires_visible_to_consumers)
        .bind(activating.fires_deadline_unix)
        .bind(activating.deactivated_by_health)
        .bind(now)
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected() > 0)
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
               AND status NOT IN ('activating', 'deactivating')",
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
        let rows: Vec<(uuid::Uuid, String, String)> = sqlx::query_as(
            "SELECT id, status, transition FROM project \
             WHERE (status = 'activating' \
                    OR transition IN ('building', 'cancelling_build')) \
               AND transition_heartbeat_unix < $1",
        )
        .bind(stale_before)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|(id, status, transition)| {
                Ok(StuckTransition {
                    id,
                    status: project_status_from_str(&status)?,
                    transition: project_transition_from_str(&transition)?,
                })
            })
            .collect()
    }

    async fn list_deactivating(&self) -> anyhow::Result<Vec<uuid::Uuid>> {
        let rows: Vec<(uuid::Uuid,)> =
            sqlx::query_as("SELECT id FROM project WHERE status = 'deactivating'")
                .fetch_all(&self.pool)
                .await?;
        Ok(rows.into_iter().map(|(id,)| id).collect())
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

    async fn project_has_infra(&self, id_str: &str) -> anyhow::Result<Option<bool>> {
        let id = id_str
            .parse::<uuid::Uuid>()
            .map_err(|e| anyhow::anyhow!("bad project_id '{id_str}': {e}"))?;
        let row: Option<(bool,)> =
            sqlx::query_as("SELECT has_infra FROM project WHERE id = $1")
                .bind(id)
                .fetch_optional(&self.pool)
                .await?;
        Ok(row.map(|(b,)| b))
    }

    async fn project_namespace(&self, id_str: &str) -> anyhow::Result<Option<String>> {
        let id = id_str
            .parse::<uuid::Uuid>()
            .map_err(|e| anyhow::anyhow!("bad project_id '{id_str}': {e}"))?;
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


    async fn infra_image_tags(
        &self,
        project_id_str: &str,
        node_id: &str,
    ) -> anyhow::Result<std::collections::HashMap<String, String>> {
        use sqlx::Row;
        let id = project_id_str
            .parse::<uuid::Uuid>()
            .map_err(|e| anyhow::anyhow!("bad project_id '{project_id_str}': {e}"))?;
        let row = sqlx::query("SELECT infra_image_tags_json FROM project WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;
        let Some(r) = row else {
            return Ok(Default::default());
        };
        let value: serde_json::Value = r
            .try_get("infra_image_tags_json")
            .map_err(|e| anyhow::anyhow!("decode infra_image_tags_json: {e}"))?;
        // Typed decode through the canonical shape:
        // `{ "<node_id>": { "<image_name>": "<tag>" } }`. Shape
        // drift is a 500-level bug; silently returning empty would
        // mask schema corruption as "no images registered."
        let by_node: std::collections::HashMap<
            String,
            std::collections::HashMap<String, String>,
        > = serde_json::from_value(value).map_err(|e| {
            anyhow::anyhow!(
                "infra_image_tags_json for project={project_id_str} has wrong shape \
                 (expected {{node: {{image: tag}}}}): {e}"
            )
        })?;
        // Missing this node IS a legitimate empty (the project's
        // image-tag map exists but this node hasn't been written).
        Ok(by_node.get(node_id).cloned().unwrap_or_default())
    }

    async fn set_health_protocols(
        &self,
        id: uuid::Uuid,
        protocols: Option<serde_json::Value>,
    ) -> anyhow::Result<bool> {
        let res = sqlx::query("UPDATE project SET health_protocols_json = $1 WHERE id = $2")
            .bind(protocols)
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(res.rows_affected() > 0)
    }

    async fn health_protocols(
        &self,
        project_id_str: &str,
    ) -> anyhow::Result<Option<serde_json::Value>> {
        use sqlx::Row;
        let id = project_id_str
            .parse::<uuid::Uuid>()
            .map_err(|e| anyhow::anyhow!("bad project_id '{project_id_str}': {e}"))?;
        let row = sqlx::query("SELECT health_protocols_json FROM project WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;
        let Some(r) = row else {
            return Ok(None);
        };
        let value: Option<serde_json::Value> = r
            .try_get("health_protocols_json")
            .map_err(|e| anyhow::anyhow!("decode health_protocols_json: {e}"))?;
        Ok(value)
    }
}

// Canonical wall-clock helper lives in `crate::lease::now_unix`.
// All UNIX-timestamp readers across the dispatcher route through
// that one function.

#[cfg(any(test, feature = "test-helpers"))]
pub struct MockProjectStore {
    inner: RwLock<HashMap<uuid::Uuid, (String, ProjectStatus, ProjectDefinition)>>,
    binary_hashes: RwLock<HashMap<uuid::Uuid, String>>,
    definition_hashes: RwLock<HashMap<uuid::Uuid, String>>,
    infra_hashes: RwLock<HashMap<uuid::Uuid, String>>,
    /// In-memory mirror of the `project_definition` history table:
    /// keyed by `(project_id, definition_hash)`, value is the
    /// `project_json` registered under that hash.
    definition_versions: RwLock<HashMap<(uuid::Uuid, String), String>>,
    lifecycles: RwLock<HashMap<uuid::Uuid, ProjectLifecycle>>,
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
impl MockProjectStore {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
            binary_hashes: RwLock::new(HashMap::new()),
            definition_hashes: RwLock::new(HashMap::new()),
            infra_hashes: RwLock::new(HashMap::new()),
            definition_versions: RwLock::new(HashMap::new()),
            lifecycles: RwLock::new(HashMap::new()),
            tenants: RwLock::new(HashMap::new()),
            descriptions: RwLock::new(HashMap::new()),
            has_infra: RwLock::new(HashMap::new()),
            namespaces: RwLock::new(HashMap::new()),
            transitions: RwLock::new(HashMap::new()),
        }
    }
}

#[cfg(any(test, feature = "test-helpers"))]
impl Default for MockProjectStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(any(test, feature = "test-helpers"))]
#[async_trait]
impl ProjectStoreOps for MockProjectStore {
    async fn register_with_hashes(
        &self,
        project: ProjectDefinition,
        name: &str,
        description: &str,
        tenant_id: &str,
        binary_hash: Option<&str>,
        definition_hash: Option<&str>,
        infra_hash: Option<&str>,
        // The mock does not model infra image tags (its reader returns empty).
        _infra_image_tags: Option<&InfraImageTags>,
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
        let project_json = serde_json::to_string(&project)?;
        // Mirror Postgres exactly: the upsert's conflict arm does NOT
        // touch `status` (an Active project stays active through a
        // re-register), and a FRESH row gets the column defaults the
        // lifecycle CAS methods read (status='registered',
        // accepting/visible true). Without the seeding, mock-backed
        // register -> activate tests would 409 where production
        // activates; without the preservation, the mock's status
        // mirror and `lifecycles` would disagree after a re-register.
        let status = {
            let mut inner = self.inner.write().await;
            let preserved = inner.get(&id).map(|(_, s, _)| *s);
            let status = preserved.unwrap_or(ProjectStatus::Registered);
            inner.insert(id, (name_owned.clone(), status, project));
            status
        };
        {
            let mut lifecycles = self.lifecycles.write().await;
            lifecycles.entry(id).or_insert(ProjectLifecycle {
                status: ProjectStatus::Registered,
                accepting_fires: true,
                fires_visible_to_consumers: true,
                fires_deadline_unix: None,
                deactivated_by_health: false,
                drain_deadline_unix: None,
            });
        }
        self.tenants.write().await.insert(id, tenant_id.to_string());
        self.descriptions.write().await.insert(id, description_owned.clone());
        self.has_infra.write().await.insert(id, has_infra);
        if let Some(h) = binary_hash {
            self.binary_hashes.write().await.insert(id, h.to_string());
        }
        if let Some(h) = definition_hash {
            // Record history row first, then advance the pointer.
            // The mock cannot truly atomicize the five `RwLock` writes
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
        Ok(StoredProjectSummary {
            id,
            name: name_owned,
            description: description_owned,
            status,
        })
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
            .map(|(id, (name, status, _))| StoredProjectSummary {
                id: *id,
                name: name.clone(),
                description: descriptions.get(id).cloned().unwrap_or_default(),
                status: *status,
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
            .map(|(name, status, _)| StoredProjectSummary {
                id,
                name: name.clone(),
                description: descriptions.get(&id).cloned().unwrap_or_default(),
                status: *status,
            }))
    }

    async fn remove(&self, id: uuid::Uuid) -> anyhow::Result<bool> {
        // Mirror Postgres FK CASCADE: removing a project clears every
        // per-id side-map (binary/definition/infra hashes,
        // definition_versions history, lifecycles, tenants, has_infra,
        // namespaces, transitions). Without this the mock diverges from
        // production: a test that re-registers under the same id, or asserts
        // cleanup, would see ghost state Postgres does not have.
        let was_present = self.inner.write().await.remove(&id).is_some();
        self.binary_hashes.write().await.remove(&id);
        self.definition_hashes.write().await.remove(&id);
        self.infra_hashes.write().await.remove(&id);
        self.definition_versions.write().await.retain(|(p, _), _| *p != id);
        self.lifecycles.write().await.remove(&id);
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
            .map(|(_, _, project)| project.clone()))
    }

    async fn set_running_hashes(
        &self,
        id: uuid::Uuid,
        binary_hash: Option<&str>,
        definition_hash: Option<&str>,
        infra_hash: Option<&str>,
        // The mock does not model infra image tags (its `infra_image_tags`
        // reader returns empty); accepted to satisfy the trait, ignored.
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
            self.binary_hashes.write().await.insert(id, h.to_string());
        }
        if let Some(h) = definition_hash {
            self.definition_hashes.write().await.insert(id, h.to_string());
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

    async fn running_infra_hash(&self, id: uuid::Uuid) -> anyhow::Result<Option<String>> {
        Ok(self.infra_hashes.read().await.get(&id).cloned())
    }

    async fn lifecycle(&self, id: uuid::Uuid) -> anyhow::Result<Option<ProjectLifecycle>> {
        // The mock seeds a default for unregistered ids; in
        // production the PG impl returns Ok(None). Tests that rely
        // on the seeded default should expect Some(default) here.
        // (Behaviour preserved from the pre-Result mock.)
        Ok(Some(
            self.lifecycles
                .read()
                .await
                .get(&id)
                .cloned()
                .unwrap_or_else(|| ProjectLifecycle {
                    status: ProjectStatus::Registered,
                    accepting_fires: true,
                    fires_visible_to_consumers: true,
                    fires_deadline_unix: None,
                    deactivated_by_health: false,
                    drain_deadline_unix: None,
                }),
        ))
    }

    async fn set_lifecycle_guarded(
        &self,
        id: uuid::Uuid,
        lifecycle: &ProjectLifecycle,
    ) -> anyhow::Result<LifecycleWrite> {
        if !self.inner.read().await.contains_key(&id) {
            return Ok(LifecycleWrite::NotFound);
        }
        // Mirror the Postgres guard: refused while activating or while
        // a build transition is in flight.
        let current_status = self
            .lifecycles
            .read()
            .await
            .get(&id)
            .map(|l| l.status)
            .unwrap_or(ProjectStatus::Registered);
        let current_transition = self
            .transitions
            .read()
            .await
            .get(&id)
            .map(|(t, _)| *t)
            .unwrap_or(ProjectTransition::None);
        if current_status == ProjectStatus::Activating
            || current_transition != ProjectTransition::None
        {
            return Ok(LifecycleWrite::Rejected {
                status: current_status,
                transition: current_transition,
            });
        }
        // Mirror the row's `status` so callers that only consult
        // get()/list() see the correct project status without
        // needing to consult lifecycle().
        if let Some(entry) = self.inner.write().await.get_mut(&id) {
            entry.1 = lifecycle.status;
        }
        self.lifecycles
            .write()
            .await
            .insert(id, lifecycle.clone());
        Ok(LifecycleWrite::Applied)
    }

    async fn cas_status(
        &self,
        id: uuid::Uuid,
        from: ProjectStatus,
        to: ProjectStatus,
    ) -> anyhow::Result<bool> {
        let mut lifecycles = self.lifecycles.write().await;
        let mut inner = self.inner.write().await;
        let Some(lifecycle) = lifecycles.get_mut(&id) else {
            return Ok(false);
        };
        if lifecycle.status != from {
            return Ok(false);
        }
        lifecycle.status = to;
        if let Some(entry) = inner.get_mut(&id) {
            entry.1 = to;
        }
        Ok(true)
    }

    async fn cas_lifecycle(
        &self,
        id: uuid::Uuid,
        from: ProjectStatus,
        to: &ProjectLifecycle,
    ) -> anyhow::Result<bool> {
        let mut lifecycles = self.lifecycles.write().await;
        let mut inner = self.inner.write().await;
        let Some(lifecycle) = lifecycles.get_mut(&id) else {
            return Ok(false);
        };
        if lifecycle.status != from {
            return Ok(false);
        }
        *lifecycle = to.clone();
        if let Some(entry) = inner.get_mut(&id) {
            entry.1 = to.status;
        }
        Ok(true)
    }

    async fn try_begin_activating(&self, id: uuid::Uuid) -> anyhow::Result<bool> {
        let mut lifecycles = self.lifecycles.write().await;
        let mut inner = self.inner.write().await;
        let mut transitions = self.transitions.write().await;
        let Some(lifecycle) = lifecycles.get_mut(&id) else {
            return Ok(false);
        };
        if lifecycle.status == ProjectStatus::Activating {
            return Ok(false);
        }
        if transitions
            .get(&id)
            .map(|(t, _)| *t != ProjectTransition::None)
            .unwrap_or(false)
        {
            return Ok(false);
        }
        *lifecycle = ProjectLifecycle::activating();
        if let Some(entry) = inner.get_mut(&id) {
            entry.1 = ProjectStatus::Activating;
        }
        transitions
            .entry(id)
            .or_insert((ProjectTransition::None, 0))
            .1 = crate::lease::now_unix();
        Ok(true)
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
        let lifecycles = self.lifecycles.read().await;
        let mut transitions = self.transitions.write().await;
        if !self.inner.read().await.contains_key(&id) {
            return Ok(false);
        }
        let status = lifecycles
            .get(&id)
            .map(|l| l.status)
            .unwrap_or(ProjectStatus::Registered);
        if matches!(status, ProjectStatus::Activating | ProjectStatus::Deactivating) {
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
        let lifecycles = self.lifecycles.read().await;
        let transitions = self.transitions.read().await;
        let mut out = Vec::new();
        for (id, lifecycle) in lifecycles.iter() {
            let (transition, hb) = transitions
                .get(id)
                .copied()
                .unwrap_or((ProjectTransition::None, 0));
            let driver_backed = lifecycle.status == ProjectStatus::Activating
                || transition.is_building();
            if driver_backed && hb < stale_before {
                out.push(StuckTransition {
                    id: *id,
                    status: lifecycle.status,
                    transition,
                });
            }
        }
        Ok(out)
    }

    async fn list_deactivating(&self) -> anyhow::Result<Vec<uuid::Uuid>> {
        Ok(self
            .lifecycles
            .read()
            .await
            .iter()
            .filter(|(_, l)| l.status == ProjectStatus::Deactivating)
            .map(|(id, _)| *id)
            .collect())
    }

    async fn project_has_infra(&self, id_str: &str) -> anyhow::Result<Option<bool>> {
        let id = id_str
            .parse::<uuid::Uuid>()
            .map_err(|e| anyhow::anyhow!("bad project_id '{id_str}': {e}"))?;
        Ok(self.has_infra.read().await.get(&id).copied())
    }

    async fn project_namespace(&self, id_str: &str) -> anyhow::Result<Option<String>> {
        let id = id_str
            .parse::<uuid::Uuid>()
            .map_err(|e| anyhow::anyhow!("bad project_id '{id_str}': {e}"))?;
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

    async fn infra_image_tags(
        &self,
        _project_id_str: &str,
        _node_id: &str,
    ) -> anyhow::Result<std::collections::HashMap<String, String>> {
        Ok(std::collections::HashMap::new())
    }

    async fn set_health_protocols(
        &self,
        _id: uuid::Uuid,
        _protocols: Option<serde_json::Value>,
    ) -> anyhow::Result<bool> {
        Ok(true)
    }

    async fn health_protocols(
        &self,
        _project_id_str: &str,
    ) -> anyhow::Result<Option<serde_json::Value>> {
        Ok(None)
    }
}
