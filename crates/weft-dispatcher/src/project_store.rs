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

/// Backing store for project metadata. Implementations:
/// - `PostgresProjectStore` (production)
/// - `MockProjectStore` (tests, behind `test-helpers`).
#[async_trait]
pub trait ProjectStoreOps: Send + Sync {
    /// Insert a freshly registered project. `project_namespace` is
    /// required (NOT NULL in the schema) so the row is born with the
    /// k8s placement decided. Callers compute it via
    /// `NamespaceMapper::project_namespace_for(...)`.
    async fn register(
        &self,
        project: ProjectDefinition,
        tenant_id: &str,
        project_namespace: &str,
    ) -> anyhow::Result<StoredProjectSummary>;

    // Every reader returns `Result<Option<T>>` or `Result<Vec<T>>`:
    // `Ok(None)` / `Ok(vec![])` means "no such row" (legal), `Err`
    // means "DB failure" (callers MUST surface). Earlier this trait
    // returned bare `Option<T>` / `Vec<T>` / `bool`; transient DB
    // hiccups silently looked like "no rows" and led to wrong
    // decisions downstream (kill a healthy pod, show "no projects",
    // etc).

    async fn tenant_for(&self, id: uuid::Uuid) -> anyhow::Result<Option<String>>;
    async fn list(&self) -> anyhow::Result<Vec<StoredProjectSummary>>;
    async fn get(&self, id: uuid::Uuid) -> anyhow::Result<Option<StoredProjectSummary>>;
    /// Returns `Ok(true)` iff a row was removed. `Ok(false)` = no
    /// such row (caller decides whether to 404). `Err` = DB failure.
    async fn remove(&self, id: uuid::Uuid) -> anyhow::Result<bool>;
    async fn project(&self, id: uuid::Uuid) -> anyhow::Result<Option<ProjectDefinition>>;

    /// Persist the source hash the user just built. Doubles as the
    /// worker docker image tag suffix (k8s manifest builder reads
    /// it back on spawn) AND as the resync drift signal (status
    /// compares it against the CLI's freshly-computed source hash).
    ///
    /// Returns `Err` on DB failure. `Ok(())` even if the row didn't
    /// exist; sync writes hash before the project row is guaranteed
    /// to exist in the dispatcher's view (the broker is the source
    /// of truth for "does this project exist at all").
    async fn set_running_source_hash(&self, id: uuid::Uuid, hash: &str) -> anyhow::Result<()>;

    /// Read the stored source hash. `Ok(None)` if never set
    /// (project registered but never built / activated /
    /// infra-started). `Err` ONLY on DB failure: a transient hiccup
    /// must NOT be observed as "no hash" (which would trigger an
    /// unnecessary stale-worker kill in `replace_stale_worker_if_needed`).
    async fn running_source_hash(&self, id: uuid::Uuid) -> anyhow::Result<Option<String>>;

    /// Persist the infra hash the user just built. Drives the
    /// upgrade drift signal: status compares this against the
    /// CLI's freshly-computed infra hash. Set only by paths that
    /// touch infra (infra/start, infra/upgrade) plus register /
    /// activate / resync, which all carry it for completeness.
    /// Same Result contract as `set_running_source_hash`.
    async fn set_running_infra_hash(&self, id: uuid::Uuid, hash: &str) -> anyhow::Result<()>;

    /// Read the stored infra hash. Same Result contract as
    /// `running_source_hash`.
    async fn running_infra_hash(&self, id: uuid::Uuid) -> anyhow::Result<Option<String>>;

    /// Read the project's full lifecycle: status + the three
    /// orthogonal axes that control fire acceptance, consumer
    /// visibility, and the optional acceptance deadline.
    /// `Ok(None)` = no such project; `Err` = DB failure.
    async fn lifecycle(&self, id: uuid::Uuid) -> anyhow::Result<Option<ProjectLifecycle>>;

    /// Atomically set every lifecycle field. Used by activate,
    /// deactivate, and the journal-bridge drain-watcher when it
    /// flips status from deactivating → inactive. `Ok(true)` iff a
    /// row was updated.
    async fn set_lifecycle(
        &self,
        id: uuid::Uuid,
        lifecycle: &ProjectLifecycle,
    ) -> anyhow::Result<bool>;

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
    /// deactivate is still draining). A drain-watcher CAS that loses
    /// to this transition already tolerates the loss and retries.
    async fn try_begin_activating(&self, id: uuid::Uuid) -> anyhow::Result<bool>;

    /// Look up the project namespace by string-id (used by task
    /// executors that only see the project_id string). `Ok(Some(ns))`
    /// for any registered project; `Ok(None)` ONLY when the project
    /// doesn't exist; `Err` on DB failure.
    async fn project_namespace(&self, id_str: &str) -> anyhow::Result<Option<String>>;

    /// Arm the sync-in-flight sentinel under a per-tenant xact-scoped
    /// advisory lock, in one tx. Used by `sync` to atomically:
    ///   1. take `pg_advisory_xact_lock(advisory_key(domain, scope))`,
    ///   2. write `sync_in_flight_until_unix = now + ttl`,
    ///   3. COMMIT (auto-releases the lock).
    ///
    /// xact-scoped lock means no session-leak back into the
    /// connection pool. The reaper takes the same key with
    /// `pg_try_advisory_xact_lock` non-blocking; both sides see a
    /// consistent view.
    async fn arm_sync_with_advisory_lock(
        &self,
        id: uuid::Uuid,
        advisory_key: i64,
        until_unix: i64,
    ) -> anyhow::Result<()>;

    /// Replace the per-(project, node) infra image-tag map. The CLI
    /// sends the tags in /infra/sync; the supervisor reads them
    /// back via `infra_image_tags(project_id, node_id)`. `Err` on
    /// DB failure: a silently-dropped write means the supervisor
    /// resolves `Image::Local` against stale tags at the next apply.
    async fn set_infra_image_tags(
        &self,
        id: uuid::Uuid,
        node_id: &str,
        tags: std::collections::HashMap<String, String>,
    ) -> anyhow::Result<()>;

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
}

impl ProjectLifecycle {
    /// Live ("active"): worker spawns, fires execute immediately.
    pub fn active() -> Self {
        Self {
            status: ProjectStatus::Active,
            accepting_fires: true,
            fires_visible_to_consumers: true,
            fires_deadline_unix: None,
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
    pub status: ProjectStatus,
}

impl PostgresProjectStore {
    pub async fn new(pool: PgPool) -> anyhow::Result<Self> {
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
        // running_source_hash / running_infra_hash drive drift
        // detection + image tagging (worker docker tag suffix +
        // resync compare against the CLI's freshly-computed hash).
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
                status TEXT NOT NULL,
                project_json TEXT NOT NULL,
                updated_at BIGINT NOT NULL,
                running_source_hash TEXT,
                running_infra_hash TEXT,
                accepting_fires BOOLEAN NOT NULL DEFAULT TRUE,
                fires_visible_to_consumers BOOLEAN NOT NULL DEFAULT TRUE,
                fires_deadline_unix BIGINT,
                tenant_id TEXT NOT NULL DEFAULT 'local',
                -- Project namespace (wm-project-<tenant>--<project>).
                -- Set on register (NOT NULL: no default; populated by
                -- the writer at registration time).
                project_namespace TEXT NOT NULL,
                -- Per-(project, node) image hash maps for Image::Local
                -- references in InfraSpecs. CLI ships these in /sync;
                -- supervisor reads them.
                -- Shape: { "<node_id>": { "<image_name>": "<tag>" } }
                infra_image_tags_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                -- Per-project health protocols overriding the weft
                -- default. NULL = use default. Schema per
                -- weft_infra_supervisor::protocol::HealthProtocols.
                health_protocols_json JSONB,
                -- Sentinel "this project has a sync handler mid-flight."
                -- Set on entry to /infra/sync (via a short tx with an
                -- advisory lock), heartbeated during the handler's
                -- slow work, cleared on exit. The supervisor reaper
                -- reads it (and pending counts + node counts) before
                -- scaling the per-tenant supervisor to 0.
                -- Stored per project, not per tenant: a multi-project
                -- tenant's reaper considers every project's sentinel.
                sync_in_flight_until_unix BIGINT
            )"#,
        )
        .execute(&pool)
        .await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_project_tenant ON project(tenant_id)")
            .execute(&pool)
            .await?;

        // Downgrade in-flight rows on dispatcher restart so every
        // project is cleanly re-activatable on the new pod:
        // - `active`: trigger-setup must re-bootstrap (only `/activate`
        //   fires TriggerSetup, which lives in-process).
        // - `deactivating`: a crash mid-deactivate needs clean re-entry.
        // - `activating`: a crash mid-activate would otherwise STRAND
        //   the row forever, because in-process rollback never ran and
        //   `try_begin_activating` is gated on `status <> 'activating'`,
        //   so every future activate would 409. Status-downgrade is
        //   sufficient: the next activate's `sweep_orphan_trigger_setup_colors`
        //   reaps any leaked TriggerSetup color and its register tasks
        //   UPSERT signal rows in place, so we don't duplicate that wipe
        //   here (it would need k8s/journal access this constructor
        //   doesn't have).
        sqlx::query(
            "UPDATE project SET status = 'inactive', \
                accepting_fires = FALSE, \
                fires_visible_to_consumers = FALSE, \
                fires_deadline_unix = NULL \
             WHERE status IN ('active', 'activating', 'deactivating')",
        )
        .execute(&pool)
        .await?;

        Ok(Self { pool })
    }
}

#[async_trait]
impl ProjectStoreOps for PostgresProjectStore {
    /// Register (or update) a project. Idempotent on id.
    async fn register(
        &self,
        project: ProjectDefinition,
        tenant_id: &str,
        project_namespace: &str,
    ) -> anyhow::Result<StoredProjectSummary> {
        let id = project.id;
        let name = project.name.clone();
        let project_json = serde_json::to_string(&project)?;
        sqlx::query(
            "INSERT INTO project \
                (id, name, status, project_json, updated_at, \
                 tenant_id, project_namespace) \
             VALUES ($1, $2, 'registered', $3, $4, $5, $6) \
             ON CONFLICT (id) DO UPDATE SET \
                name = EXCLUDED.name, \
                project_json = EXCLUDED.project_json, \
                updated_at = EXCLUDED.updated_at, \
                tenant_id = EXCLUDED.tenant_id, \
                project_namespace = EXCLUDED.project_namespace",
        )
        .bind(id)
        .bind(&name)
        .bind(&project_json)
        .bind(crate::lease::now_unix())
        .bind(tenant_id)
        .bind(project_namespace)
        .execute(&self.pool)
        .await?;
        Ok(StoredProjectSummary {
            id,
            name,
            status: ProjectStatus::Registered,
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

    async fn list(&self) -> anyhow::Result<Vec<StoredProjectSummary>> {
        let rows: Vec<(uuid::Uuid, String, String)> = sqlx::query_as(
            "SELECT id, name, status FROM project ORDER BY name",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|(id, name, status)| {
                Ok(StoredProjectSummary {
                    id,
                    name,
                    status: project_status_from_str(&status)?,
                })
            })
            .collect()
    }

    async fn get(&self, id: uuid::Uuid) -> anyhow::Result<Option<StoredProjectSummary>> {
        let row: Option<(uuid::Uuid, String, String)> = sqlx::query_as(
            "SELECT id, name, status FROM project WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(|(id, name, status)| {
            Ok(StoredProjectSummary {
                id,
                name,
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

    async fn set_running_source_hash(&self, id: uuid::Uuid, hash: &str) -> anyhow::Result<()> {
        sqlx::query(
            "UPDATE project SET running_source_hash = $1, updated_at = $2 WHERE id = $3",
        )
        .bind(hash)
        .bind(crate::lease::now_unix())
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn running_source_hash(&self, id: uuid::Uuid) -> anyhow::Result<Option<String>> {
        let row: Option<(Option<String>,)> = sqlx::query_as(
            "SELECT running_source_hash FROM project WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.and_then(|(h,)| h))
    }

    async fn set_running_infra_hash(&self, id: uuid::Uuid, hash: &str) -> anyhow::Result<()> {
        sqlx::query(
            "UPDATE project SET running_infra_hash = $1, updated_at = $2 WHERE id = $3",
        )
        .bind(hash)
        .bind(crate::lease::now_unix())
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
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
        let row: Option<(String, bool, bool, Option<i64>)> = sqlx::query_as(
            "SELECT status, accepting_fires, fires_visible_to_consumers, fires_deadline_unix \
             FROM project WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(|(status, accepting, visible, deadline)| {
            Ok(ProjectLifecycle {
                status: project_status_from_str(&status)?,
                accepting_fires: accepting,
                fires_visible_to_consumers: visible,
                fires_deadline_unix: deadline,
            })
        })
        .transpose()
    }

    async fn set_lifecycle(
        &self,
        id: uuid::Uuid,
        lifecycle: &ProjectLifecycle,
    ) -> anyhow::Result<bool> {
        let res = sqlx::query(
            "UPDATE project \
             SET status = $1, \
                 accepting_fires = $2, \
                 fires_visible_to_consumers = $3, \
                 fires_deadline_unix = $4, \
                 updated_at = $5 \
             WHERE id = $6",
        )
        .bind(lifecycle.status.as_str())
        .bind(lifecycle.accepting_fires)
        .bind(lifecycle.fires_visible_to_consumers)
        .bind(lifecycle.fires_deadline_unix)
        .bind(crate::lease::now_unix())
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected() > 0)
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
                 updated_at = $5 \
             WHERE id = $6 AND status = $7",
        )
        .bind(to.status.as_str())
        .bind(to.accepting_fires)
        .bind(to.fires_visible_to_consumers)
        .bind(to.fires_deadline_unix)
        .bind(crate::lease::now_unix())
        .bind(id)
        .bind(from.as_str())
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected() > 0)
    }

    async fn try_begin_activating(&self, id: uuid::Uuid) -> anyhow::Result<bool> {
        let activating = ProjectLifecycle::activating();
        let res = sqlx::query(
            "UPDATE project \
             SET status = $1, \
                 accepting_fires = $2, \
                 fires_visible_to_consumers = $3, \
                 fires_deadline_unix = $4, \
                 updated_at = $5 \
             WHERE id = $6 AND status <> 'activating'",
        )
        .bind(activating.status.as_str())
        .bind(activating.accepting_fires)
        .bind(activating.fires_visible_to_consumers)
        .bind(activating.fires_deadline_unix)
        .bind(crate::lease::now_unix())
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected() > 0)
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

    async fn project_namespace(&self, id_str: &str) -> anyhow::Result<Option<String>> {
        let id = id_str
            .parse::<uuid::Uuid>()
            .map_err(|e| anyhow::anyhow!("bad project_id '{id_str}': {e}"))?;
        let row: Option<(String,)> =
            sqlx::query_as("SELECT project_namespace FROM project WHERE id = $1")
                .bind(id)
                .fetch_optional(&self.pool)
                .await?;
        Ok(row.map(|(s,)| s).filter(|s| !s.is_empty()))
    }

    async fn arm_sync_with_advisory_lock(
        &self,
        id: uuid::Uuid,
        advisory_key: i64,
        until_unix: i64,
    ) -> anyhow::Result<()> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(advisory_key)
            .execute(&mut *tx)
            .await?;
        sqlx::query("UPDATE project SET sync_in_flight_until_unix = $1 WHERE id = $2")
            .bind(until_unix)
            .bind(id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn set_infra_image_tags(
        &self,
        id: uuid::Uuid,
        node_id: &str,
        tags: std::collections::HashMap<String, String>,
    ) -> anyhow::Result<()> {
        // Merge: read current map, replace just the node_id entry,
        // write back. Idempotent. Fail loud on decode of the
        // existing map: silently coercing to empty would overwrite
        // every OTHER node's tags with nothing on the next write.
        use sqlx::Row;
        let row = sqlx::query("SELECT infra_image_tags_json FROM project WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;
        let mut current: serde_json::Map<String, serde_json::Value> = match row {
            None => serde_json::Map::new(),
            Some(r) => {
                let value: serde_json::Value = r
                    .try_get("infra_image_tags_json")
                    .map_err(|e| anyhow::anyhow!("decode infra_image_tags_json: {e}"))?;
                match value {
                    serde_json::Value::Null => serde_json::Map::new(),
                    serde_json::Value::Object(m) => m,
                    other => anyhow::bail!(
                        "infra_image_tags_json is not an object for project_id={id}: {other:?}"
                    ),
                }
            }
        };
        current.insert(
            node_id.to_string(),
            serde_json::to_value(&tags).expect("HashMap<String,String> serializes"),
        );
        let merged = serde_json::Value::Object(current);
        sqlx::query("UPDATE project SET infra_image_tags_json = $1 WHERE id = $2")
            .bind(merged)
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
    source_hashes: RwLock<HashMap<uuid::Uuid, String>>,
    infra_hashes: RwLock<HashMap<uuid::Uuid, String>>,
    lifecycles: RwLock<HashMap<uuid::Uuid, ProjectLifecycle>>,
    tenants: RwLock<HashMap<uuid::Uuid, String>>,
    namespaces: RwLock<HashMap<uuid::Uuid, String>>,
    sync_in_flight: RwLock<HashMap<uuid::Uuid, i64>>,
}

#[cfg(any(test, feature = "test-helpers"))]
impl MockProjectStore {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
            source_hashes: RwLock::new(HashMap::new()),
            infra_hashes: RwLock::new(HashMap::new()),
            lifecycles: RwLock::new(HashMap::new()),
            tenants: RwLock::new(HashMap::new()),
            namespaces: RwLock::new(HashMap::new()),
            sync_in_flight: RwLock::new(HashMap::new()),
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
    async fn register(
        &self,
        project: ProjectDefinition,
        tenant_id: &str,
        project_namespace: &str,
    ) -> anyhow::Result<StoredProjectSummary> {
        let id = project.id;
        let name = project.name.clone();
        self.inner
            .write()
            .await
            .insert(id, (name.clone(), ProjectStatus::Registered, project));
        self.tenants.write().await.insert(id, tenant_id.to_string());
        self.namespaces
            .write()
            .await
            .insert(id, project_namespace.to_string());
        Ok(StoredProjectSummary {
            id,
            name,
            status: ProjectStatus::Registered,
        })
    }

    async fn tenant_for(&self, id: uuid::Uuid) -> anyhow::Result<Option<String>> {
        Ok(self.tenants.read().await.get(&id).cloned())
    }

    async fn list(&self) -> anyhow::Result<Vec<StoredProjectSummary>> {
        Ok(self
            .inner
            .read()
            .await
            .iter()
            .map(|(id, (name, status, _))| StoredProjectSummary {
                id: *id,
                name: name.clone(),
                status: *status,
            })
            .collect())
    }

    async fn get(&self, id: uuid::Uuid) -> anyhow::Result<Option<StoredProjectSummary>> {
        Ok(self
            .inner
            .read()
            .await
            .get(&id)
            .map(|(name, status, _)| StoredProjectSummary {
                id,
                name: name.clone(),
                status: *status,
            }))
    }

    async fn remove(&self, id: uuid::Uuid) -> anyhow::Result<bool> {
        Ok(self.inner.write().await.remove(&id).is_some())
    }

    async fn project(&self, id: uuid::Uuid) -> anyhow::Result<Option<ProjectDefinition>> {
        Ok(self
            .inner
            .read()
            .await
            .get(&id)
            .map(|(_, _, project)| project.clone()))
    }

    async fn set_running_source_hash(&self, id: uuid::Uuid, hash: &str) -> anyhow::Result<()> {
        self.source_hashes.write().await.insert(id, hash.to_string());
        Ok(())
    }

    async fn running_source_hash(&self, id: uuid::Uuid) -> anyhow::Result<Option<String>> {
        Ok(self.source_hashes.read().await.get(&id).cloned())
    }

    async fn set_running_infra_hash(&self, id: uuid::Uuid, hash: &str) -> anyhow::Result<()> {
        self.infra_hashes.write().await.insert(id, hash.to_string());
        Ok(())
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
                }),
        ))
    }

    async fn set_lifecycle(
        &self,
        id: uuid::Uuid,
        lifecycle: &ProjectLifecycle,
    ) -> anyhow::Result<bool> {
        if !self.inner.read().await.contains_key(&id) {
            return Ok(false);
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
        Ok(true)
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
        let Some(lifecycle) = lifecycles.get_mut(&id) else {
            return Ok(false);
        };
        if lifecycle.status == ProjectStatus::Activating {
            return Ok(false);
        }
        *lifecycle = ProjectLifecycle::activating();
        if let Some(entry) = inner.get_mut(&id) {
            entry.1 = ProjectStatus::Activating;
        }
        Ok(true)
    }

    async fn project_namespace(&self, id_str: &str) -> anyhow::Result<Option<String>> {
        let id = id_str
            .parse::<uuid::Uuid>()
            .map_err(|e| anyhow::anyhow!("bad project_id '{id_str}': {e}"))?;
        Ok(self.namespaces.read().await.get(&id).cloned())
    }

    async fn arm_sync_with_advisory_lock(
        &self,
        id: uuid::Uuid,
        _advisory_key: i64,
        until_unix: i64,
    ) -> anyhow::Result<()> {
        // No advisory locking in the mock; the production impl
        // serializes against the reaper via Postgres. Tests don't
        // exercise that path. Just write the sentinel.
        self.sync_in_flight.write().await.insert(id, until_unix);
        Ok(())
    }

    async fn set_infra_image_tags(
        &self,
        _id: uuid::Uuid,
        _node_id: &str,
        _tags: std::collections::HashMap<String, String>,
    ) -> anyhow::Result<()> {
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
