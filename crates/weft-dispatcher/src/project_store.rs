//! In-memory project store. Keyed by project id. Holds the registered
//! ProjectDefinition + the path to the binary/JSON the worker
//! backend expects to spawn against.
//!
//! Phase A2: in-memory only. Phase-B: persists in restate so the
//! dispatcher can restart without losing state.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::RwLock;

use weft_core::ProjectDefinition;

#[derive(Clone)]
pub struct ProjectStore {
    inner: Arc<RwLock<HashMap<uuid::Uuid, StoredProject>>>,
    /// Directory where project JSON files are written for workers.
    data_dir: PathBuf,
}

pub struct StoredProject {
    pub id: uuid::Uuid,
    pub name: String,
    pub status: ProjectStatus,
    pub binary_path: PathBuf,
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
}

impl ProjectStore {
    pub fn new(data_dir: PathBuf) -> anyhow::Result<Self> {
        std::fs::create_dir_all(&data_dir)?;
        Ok(Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
            data_dir,
        })
    }

    /// Register a project. `binary_path` is the absolute path to the
    /// compiled project binary (emitted by `weft build`); the
    /// dispatcher spawns it per wake. If `None`, we fall back to a
    /// placeholder JSON path so `/projects/{id}` still returns
    /// something (spawn will fail later with a readable error).
    pub async fn register(
        &self,
        project: ProjectDefinition,
        binary_path: Option<PathBuf>,
    ) -> anyhow::Result<StoredProjectSummary> {
        let id = project.id;
        let name = project.name.clone();
        let binary_path = match binary_path {
            Some(p) => p,
            None => self.data_dir.join(format!("{id}.missing-binary")),
        };

        let mut lock = self.inner.write().await;
        lock.insert(
            id,
            StoredProject {
                id,
                name: name.clone(),
                status: ProjectStatus::Registered,
                binary_path: binary_path.clone(),
                project,
            },
        );
        Ok(StoredProjectSummary { id, name, status: ProjectStatus::Registered, binary_path })
    }

    pub async fn list(&self) -> Vec<StoredProjectSummary> {
        let lock = self.inner.read().await;
        lock.values()
            .map(|p| StoredProjectSummary {
                id: p.id,
                name: p.name.clone(),
                status: p.status,
                binary_path: p.binary_path.clone(),
            })
            .collect()
    }

    pub async fn get(&self, id: uuid::Uuid) -> Option<StoredProjectSummary> {
        let lock = self.inner.read().await;
        lock.get(&id).map(|p| StoredProjectSummary {
            id: p.id,
            name: p.name.clone(),
            status: p.status,
            binary_path: p.binary_path.clone(),
        })
    }

    pub async fn remove(&self, id: uuid::Uuid) -> bool {
        let mut lock = self.inner.write().await;
        if let Some(p) = lock.remove(&id) {
            let _ = std::fs::remove_file(&p.binary_path);
            true
        } else {
            false
        }
    }

    pub async fn set_status(&self, id: uuid::Uuid, status: ProjectStatus) -> bool {
        let mut lock = self.inner.write().await;
        if let Some(p) = lock.get_mut(&id) {
            p.status = status;
            true
        } else {
            false
        }
    }

    pub async fn entry_nodes(&self, id: uuid::Uuid) -> Vec<EntryNodeRef> {
        let lock = self.inner.read().await;
        let Some(p) = lock.get(&id) else { return Vec::new() };
        p.project
            .nodes
            .iter()
            .map(|n| EntryNodeRef { id: n.id.clone(), node_type: n.node_type.clone() })
            .collect()
    }

    /// Read-only access to the full ProjectDefinition. Returns a
    /// clone so the caller doesn't hold the store lock.
    pub async fn project(&self, id: uuid::Uuid) -> Option<ProjectDefinition> {
        let lock = self.inner.read().await;
        lock.get(&id).map(|p| p.project.clone())
    }
}

#[derive(Debug, Clone)]
pub struct StoredProjectSummary {
    pub id: uuid::Uuid,
    pub name: String,
    pub status: ProjectStatus,
    pub binary_path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct EntryNodeRef {
    pub id: String,
    pub node_type: String,
}
