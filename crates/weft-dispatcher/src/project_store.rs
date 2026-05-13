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
    async fn register(
        &self,
        project: ProjectDefinition,
        tenant_id: &str,
    ) -> anyhow::Result<StoredProjectSummary>;
    async fn tenant_for(&self, id: uuid::Uuid) -> Option<String>;
    async fn list(&self) -> Vec<StoredProjectSummary>;
    async fn get(&self, id: uuid::Uuid) -> Option<StoredProjectSummary>;
    async fn remove(&self, id: uuid::Uuid) -> bool;
    async fn project(&self, id: uuid::Uuid) -> Option<ProjectDefinition>;

    /// Persist the source hash the user just built. Doubles as the
    /// worker docker image tag suffix (k8s manifest builder reads
    /// it back on spawn) AND as the resync drift signal (status
    /// compares it against the CLI's freshly-computed source hash).
    async fn set_running_source_hash(&self, id: uuid::Uuid, hash: &str) -> bool;

    /// Read the stored source hash. None if never set (project
    /// registered but never built / activated / infra-started).
    async fn running_source_hash(&self, id: uuid::Uuid) -> Option<String>;

    /// Persist the infra hash the user just built. Drives the
    /// upgrade drift signal: status compares this against the
    /// CLI's freshly-computed infra hash. Set only by paths that
    /// touch infra (infra/start, infra/upgrade) plus register /
    /// activate / resync, which all carry it for completeness.
    async fn set_running_infra_hash(&self, id: uuid::Uuid, hash: &str) -> bool;

    /// Read the stored infra hash. None if never set.
    async fn running_infra_hash(&self, id: uuid::Uuid) -> Option<String>;

    /// Read the project's full lifecycle: status + the three
    /// orthogonal axes that control fire acceptance, consumer
    /// visibility, and the optional acceptance deadline.
    async fn lifecycle(&self, id: uuid::Uuid) -> ProjectLifecycle;

    /// Atomically set every lifecycle field. Used by activate,
    /// deactivate, and the journal-bridge drain-watcher when it
    /// flips status from deactivating → inactive.
    async fn set_lifecycle(&self, id: uuid::Uuid, lifecycle: &ProjectLifecycle) -> bool;

    /// Compare-and-set on status. Returns true iff a row matched
    /// `from` and was updated to `to`. Used by the drain-watcher to
    /// flip deactivating → inactive without racing a concurrent
    /// activate.
    async fn cas_status(
        &self,
        id: uuid::Uuid,
        from: ProjectStatus,
        to: ProjectStatus,
    ) -> bool;

    /// CAS that swaps every lifecycle field at once when the row's
    /// current status matches `from`. Used by activate to flip
    /// Activating → Active (with full lifecycle: accepting=true,
    /// visible=true) and by cancel-activate to flip Activating →
    /// Inactive. Returns true iff the swap landed.
    async fn cas_lifecycle(
        &self,
        id: uuid::Uuid,
        from: ProjectStatus,
        to: &ProjectLifecycle,
    ) -> bool;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectStatus {
    /// Fresh row, never activated.
    Registered,
    /// Transient: user clicked activate; the dispatcher is running
    /// TriggerSetup (spawn worker, register every entry signal).
    /// Fires arriving in this window get parked (the listener may
    /// not yet have all signals registered). On TriggerSetup
    /// completion the row CASes to Active; on failure or user
    /// cancel, every signal row for the project is wiped and the
    /// row CASes to Inactive.
    Activating,
    /// Live; worker pool spawns on demand and fires execute.
    Active,
    /// Transient: user clicked deactivate with runningPolicy=wait;
    /// the gate already behaves like the target mode (parking new
    /// fires) but running executions are still draining. The
    /// journal-bridge CASes status to Inactive once they all finish.
    Deactivating,
    /// Idle; lifecycle axes drive what the gate does (refuse,
    /// park, or park-with-deadline) and whether the project's
    /// signals are visible to consumers.
    Inactive,
}

impl ProjectStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Registered => "registered",
            Self::Activating => "activating",
            Self::Active => "active",
            Self::Deactivating => "deactivating",
            Self::Inactive => "inactive",
        }
    }
    fn from_str(s: &str) -> Self {
        match s {
            "activating" => Self::Activating,
            "active" => Self::Active,
            "deactivating" => Self::Deactivating,
            "inactive" => Self::Inactive,
            _ => Self::Registered,
        }
    }
}

/// Public accessor for the private `ProjectStatus::from_str`. Used
/// by the lifecycle gate to decode the status column it reads from
/// a JOIN without going through the full `ProjectStore` interface.
pub fn project_status_from_str(s: &str) -> ProjectStatus {
    ProjectStatus::from_str(s)
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
        // request: a worker / listener / sidecar token authenticates
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
                tenant_id TEXT NOT NULL DEFAULT 'local'
            )"#,
        )
        .execute(&pool)
        .await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_project_tenant ON project(tenant_id)")
            .execute(&pool)
            .await?;

        // Downgrade any "active" rows on dispatcher restart:
        // trigger-setup must re-bootstrap on the new dispatcher pod
        // (only `/activate` fires TriggerSetup), and rows left
        // mid-deactivate when the dispatcher crashed need a clean
        // re-entry.
        sqlx::query(
            "UPDATE project SET status = 'inactive', \
                accepting_fires = FALSE, \
                fires_visible_to_consumers = FALSE, \
                fires_deadline_unix = NULL \
             WHERE status IN ('active', 'deactivating')",
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
    ) -> anyhow::Result<StoredProjectSummary> {
        let id = project.id;
        let name = project.name.clone();
        let project_json = serde_json::to_string(&project)?;
        sqlx::query(
            "INSERT INTO project (id, name, status, project_json, updated_at, tenant_id) \
             VALUES ($1, $2, 'registered', $3, $4, $5) \
             ON CONFLICT (id) DO UPDATE SET \
                name = EXCLUDED.name, \
                project_json = EXCLUDED.project_json, \
                updated_at = EXCLUDED.updated_at, \
                tenant_id = EXCLUDED.tenant_id",
        )
        .bind(id)
        .bind(&name)
        .bind(&project_json)
        .bind(now_unix() as i64)
        .bind(tenant_id)
        .execute(&self.pool)
        .await?;
        Ok(StoredProjectSummary {
            id,
            name,
            status: ProjectStatus::Registered,
        })
    }

    async fn tenant_for(&self, id: uuid::Uuid) -> Option<String> {
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT tenant_id FROM project WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .ok()
        .flatten();
        row.map(|(t,)| t)
    }

    async fn list(&self) -> Vec<StoredProjectSummary> {
        let rows: Vec<(uuid::Uuid, String, String)> = sqlx::query_as(
            "SELECT id, name, status FROM project ORDER BY name",
        )
        .fetch_all(&self.pool)
        .await
        .unwrap_or_default();
        rows.into_iter()
            .map(|(id, name, status)| StoredProjectSummary {
                id,
                name,
                status: ProjectStatus::from_str(&status),
            })
            .collect()
    }

    async fn get(&self, id: uuid::Uuid) -> Option<StoredProjectSummary> {
        let row: Option<(uuid::Uuid, String, String)> = sqlx::query_as(
            "SELECT id, name, status FROM project WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .ok()
        .flatten();
        row.map(|(id, name, status)| StoredProjectSummary {
            id,
            name,
            status: ProjectStatus::from_str(&status),
        })
    }

    async fn remove(&self, id: uuid::Uuid) -> bool {
        let res = sqlx::query("DELETE FROM project WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await;
        res.map(|r| r.rows_affected() > 0).unwrap_or(false)
    }

    async fn set_running_source_hash(&self, id: uuid::Uuid, hash: &str) -> bool {
        let res = sqlx::query(
            "UPDATE project SET running_source_hash = $1, updated_at = $2 WHERE id = $3",
        )
        .bind(hash)
        .bind(now_unix() as i64)
        .bind(id)
        .execute(&self.pool)
        .await;
        res.map(|r| r.rows_affected() > 0).unwrap_or(false)
    }

    async fn running_source_hash(&self, id: uuid::Uuid) -> Option<String> {
        let row: Option<(Option<String>,)> = sqlx::query_as(
            "SELECT running_source_hash FROM project WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .ok()
        .flatten();
        row.and_then(|(h,)| h)
    }

    async fn set_running_infra_hash(&self, id: uuid::Uuid, hash: &str) -> bool {
        let res = sqlx::query(
            "UPDATE project SET running_infra_hash = $1, updated_at = $2 WHERE id = $3",
        )
        .bind(hash)
        .bind(now_unix() as i64)
        .bind(id)
        .execute(&self.pool)
        .await;
        res.map(|r| r.rows_affected() > 0).unwrap_or(false)
    }

    async fn running_infra_hash(&self, id: uuid::Uuid) -> Option<String> {
        let row: Option<(Option<String>,)> = sqlx::query_as(
            "SELECT running_infra_hash FROM project WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .ok()
        .flatten();
        row.and_then(|(h,)| h)
    }

    async fn lifecycle(&self, id: uuid::Uuid) -> ProjectLifecycle {
        let row: Option<(String, bool, bool, Option<i64>)> = sqlx::query_as(
            "SELECT status, accepting_fires, fires_visible_to_consumers, fires_deadline_unix \
             FROM project WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .ok()
        .flatten();
        match row {
            Some((status, accepting, visible, deadline)) => ProjectLifecycle {
                status: ProjectStatus::from_str(&status),
                accepting_fires: accepting,
                fires_visible_to_consumers: visible,
                fires_deadline_unix: deadline,
            },
            None => ProjectLifecycle {
                status: ProjectStatus::Registered,
                accepting_fires: true,
                fires_visible_to_consumers: true,
                fires_deadline_unix: None,
            },
        }
    }

    async fn set_lifecycle(&self, id: uuid::Uuid, lifecycle: &ProjectLifecycle) -> bool {
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
        .bind(now_unix() as i64)
        .bind(id)
        .execute(&self.pool)
        .await;
        res.map(|r| r.rows_affected() > 0).unwrap_or(false)
    }

    async fn cas_status(
        &self,
        id: uuid::Uuid,
        from: ProjectStatus,
        to: ProjectStatus,
    ) -> bool {
        let res = sqlx::query(
            "UPDATE project SET status = $1, updated_at = $2 \
             WHERE id = $3 AND status = $4",
        )
        .bind(to.as_str())
        .bind(now_unix() as i64)
        .bind(id)
        .bind(from.as_str())
        .execute(&self.pool)
        .await;
        res.map(|r| r.rows_affected() > 0).unwrap_or(false)
    }

    async fn cas_lifecycle(
        &self,
        id: uuid::Uuid,
        from: ProjectStatus,
        to: &ProjectLifecycle,
    ) -> bool {
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
        .bind(now_unix() as i64)
        .bind(id)
        .bind(from.as_str())
        .execute(&self.pool)
        .await;
        res.map(|r| r.rows_affected() > 0).unwrap_or(false)
    }

    /// Read-only access to the full ProjectDefinition.
    async fn project(&self, id: uuid::Uuid) -> Option<ProjectDefinition> {
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT project_json FROM project WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .ok()
        .flatten();
        row.and_then(|(json,)| serde_json::from_str(&json).ok())
    }
}

fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(any(test, feature = "test-helpers"))]
pub struct MockProjectStore {
    inner: RwLock<HashMap<uuid::Uuid, (String, ProjectStatus, ProjectDefinition)>>,
    source_hashes: RwLock<HashMap<uuid::Uuid, String>>,
    infra_hashes: RwLock<HashMap<uuid::Uuid, String>>,
    lifecycles: RwLock<HashMap<uuid::Uuid, ProjectLifecycle>>,
    tenants: RwLock<HashMap<uuid::Uuid, String>>,
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
    ) -> anyhow::Result<StoredProjectSummary> {
        let id = project.id;
        let name = project.name.clone();
        self.inner
            .write()
            .await
            .insert(id, (name.clone(), ProjectStatus::Registered, project));
        self.tenants.write().await.insert(id, tenant_id.to_string());
        Ok(StoredProjectSummary {
            id,
            name,
            status: ProjectStatus::Registered,
        })
    }

    async fn tenant_for(&self, id: uuid::Uuid) -> Option<String> {
        self.tenants.read().await.get(&id).cloned()
    }

    async fn list(&self) -> Vec<StoredProjectSummary> {
        self.inner
            .read()
            .await
            .iter()
            .map(|(id, (name, status, _))| StoredProjectSummary {
                id: *id,
                name: name.clone(),
                status: *status,
            })
            .collect()
    }

    async fn get(&self, id: uuid::Uuid) -> Option<StoredProjectSummary> {
        self.inner
            .read()
            .await
            .get(&id)
            .map(|(name, status, _)| StoredProjectSummary {
                id,
                name: name.clone(),
                status: *status,
            })
    }

    async fn remove(&self, id: uuid::Uuid) -> bool {
        self.inner.write().await.remove(&id).is_some()
    }

    async fn project(&self, id: uuid::Uuid) -> Option<ProjectDefinition> {
        self.inner
            .read()
            .await
            .get(&id)
            .map(|(_, _, project)| project.clone())
    }

    async fn set_running_source_hash(&self, id: uuid::Uuid, hash: &str) -> bool {
        if !self.inner.read().await.contains_key(&id) {
            return false;
        }
        self.source_hashes.write().await.insert(id, hash.to_string());
        true
    }

    async fn running_source_hash(&self, id: uuid::Uuid) -> Option<String> {
        self.source_hashes.read().await.get(&id).cloned()
    }

    async fn set_running_infra_hash(&self, id: uuid::Uuid, hash: &str) -> bool {
        if !self.inner.read().await.contains_key(&id) {
            return false;
        }
        self.infra_hashes.write().await.insert(id, hash.to_string());
        true
    }

    async fn running_infra_hash(&self, id: uuid::Uuid) -> Option<String> {
        self.infra_hashes.read().await.get(&id).cloned()
    }

    async fn lifecycle(&self, id: uuid::Uuid) -> ProjectLifecycle {
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
            })
    }

    async fn set_lifecycle(&self, id: uuid::Uuid, lifecycle: &ProjectLifecycle) -> bool {
        if !self.inner.read().await.contains_key(&id) {
            return false;
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
        true
    }

    async fn cas_status(
        &self,
        id: uuid::Uuid,
        from: ProjectStatus,
        to: ProjectStatus,
    ) -> bool {
        let mut lifecycles = self.lifecycles.write().await;
        let mut inner = self.inner.write().await;
        let Some(lifecycle) = lifecycles.get_mut(&id) else {
            return false;
        };
        if lifecycle.status != from {
            return false;
        }
        lifecycle.status = to;
        if let Some(entry) = inner.get_mut(&id) {
            entry.1 = to;
        }
        true
    }

    async fn cas_lifecycle(
        &self,
        id: uuid::Uuid,
        from: ProjectStatus,
        to: &ProjectLifecycle,
    ) -> bool {
        let mut lifecycles = self.lifecycles.write().await;
        let mut inner = self.inner.write().await;
        let Some(lifecycle) = lifecycles.get_mut(&id) else {
            return false;
        };
        if lifecycle.status != from {
            return false;
        }
        *lifecycle = to.clone();
        if let Some(entry) = inner.get_mut(&id) {
            entry.1 = to.status;
        }
        true
    }
}
