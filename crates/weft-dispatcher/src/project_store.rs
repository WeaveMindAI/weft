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
    ) -> anyhow::Result<StoredProjectSummary>;
    async fn list(&self) -> Vec<StoredProjectSummary>;
    async fn get(&self, id: uuid::Uuid) -> Option<StoredProjectSummary>;
    async fn remove(&self, id: uuid::Uuid) -> bool;
    async fn set_status(&self, id: uuid::Uuid, status: ProjectStatus) -> bool;
    async fn entry_nodes(&self, id: uuid::Uuid) -> Vec<EntryNodeRef>;
    async fn project(&self, id: uuid::Uuid) -> Option<ProjectDefinition>;
}

/// Cloneable handle to whatever the dispatcher uses as project
/// storage. The thing on `DispatcherState` is this, not the
/// concrete impl.
pub type ProjectStore = Arc<dyn ProjectStoreOps>;

#[derive(Clone)]
pub struct PostgresProjectStore {
    pool: PgPool,
}

pub struct StoredProject {
    pub id: uuid::Uuid,
    pub name: String,
    pub status: ProjectStatus,
    pub project: ProjectDefinition,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectStatus {
    Registered,
    Active,
    Inactive,
}

impl ProjectStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Registered => "registered",
            Self::Active => "active",
            Self::Inactive => "inactive",
        }
    }
    fn from_str(s: &str) -> Self {
        match s {
            "active" => Self::Active,
            "inactive" => Self::Inactive,
            _ => Self::Registered,
        }
    }
}

#[derive(Debug, Clone)]
pub struct StoredProjectSummary {
    pub id: uuid::Uuid,
    pub name: String,
    pub status: ProjectStatus,
}

#[derive(Debug, Clone)]
pub struct EntryNodeRef {
    pub id: String,
    pub node_type: String,
}

impl PostgresProjectStore {
    pub async fn new(pool: PgPool) -> anyhow::Result<Self> {
        sqlx::query(
            r#"CREATE TABLE IF NOT EXISTS project (
                id UUID PRIMARY KEY,
                name TEXT NOT NULL,
                status TEXT NOT NULL,
                project_json TEXT NOT NULL,
                updated_at BIGINT NOT NULL
            )"#,
        )
        .execute(&pool)
        .await?;

        // Downgrade any "active" rows from a previous run: a
        // dispatcher restart wiped the in-memory listener pool;
        // anything that was Active needs to be re-activated by an
        // explicit call so trigger setup runs fresh.
        sqlx::query(
            "UPDATE project SET status = 'inactive' WHERE status = 'active'",
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
    ) -> anyhow::Result<StoredProjectSummary> {
        let id = project.id;
        let name = project.name.clone();
        let project_json = serde_json::to_string(&project)?;
        sqlx::query(
            "INSERT INTO project (id, name, status, project_json, updated_at) \
             VALUES ($1, $2, 'registered', $3, $4) \
             ON CONFLICT (id) DO UPDATE SET \
                name = EXCLUDED.name, \
                project_json = EXCLUDED.project_json, \
                updated_at = EXCLUDED.updated_at",
        )
        .bind(id)
        .bind(&name)
        .bind(&project_json)
        .bind(now_unix() as i64)
        .execute(&self.pool)
        .await?;
        Ok(StoredProjectSummary {
            id,
            name,
            status: ProjectStatus::Registered,
        })
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

    async fn set_status(&self, id: uuid::Uuid, status: ProjectStatus) -> bool {
        let res = sqlx::query(
            "UPDATE project SET status = $1, updated_at = $2 WHERE id = $3",
        )
        .bind(status.as_str())
        .bind(now_unix() as i64)
        .bind(id)
        .execute(&self.pool)
        .await;
        res.map(|r| r.rows_affected() > 0).unwrap_or(false)
    }

    async fn entry_nodes(&self, id: uuid::Uuid) -> Vec<EntryNodeRef> {
        let Some(project) = self.project(id).await else {
            return Vec::new();
        };
        project
            .nodes
            .iter()
            .map(|n| EntryNodeRef {
                id: n.id.clone(),
                node_type: n.node_type.clone(),
            })
            .collect()
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
}

#[cfg(any(test, feature = "test-helpers"))]
impl MockProjectStore {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
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
    ) -> anyhow::Result<StoredProjectSummary> {
        let id = project.id;
        let name = project.name.clone();
        self.inner
            .write()
            .await
            .insert(id, (name.clone(), ProjectStatus::Registered, project));
        Ok(StoredProjectSummary {
            id,
            name,
            status: ProjectStatus::Registered,
        })
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

    async fn set_status(&self, id: uuid::Uuid, status: ProjectStatus) -> bool {
        let mut g = self.inner.write().await;
        if let Some(entry) = g.get_mut(&id) {
            entry.1 = status;
            true
        } else {
            false
        }
    }

    async fn entry_nodes(&self, id: uuid::Uuid) -> Vec<EntryNodeRef> {
        let g = self.inner.read().await;
        let Some((_, _, project)) = g.get(&id) else {
            return Vec::new();
        };
        project
            .nodes
            .iter()
            .map(|n| EntryNodeRef {
                id: n.id.clone(),
                node_type: n.node_type.clone(),
            })
            .collect()
    }

    async fn project(&self, id: uuid::Uuid) -> Option<ProjectDefinition> {
        self.inner
            .read()
            .await
            .get(&id)
            .map(|(_, _, project)| project.clone())
    }
}
