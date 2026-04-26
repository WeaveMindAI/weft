//! Project store. Keyed by project id. Holds the registered
//! ProjectDefinition + the lifecycle status.
//!
//! Persists as one JSON file per project under `{data_dir}`.
//! That buys us durability across dispatcher restarts without
//! pulling in a database. Phase B replaces this with postgres
//! once multi-user / multi-tenant work lands.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use weft_core::ProjectDefinition;

#[derive(Clone)]
pub struct ProjectStore {
    inner: Arc<RwLock<HashMap<uuid::Uuid, StoredProject>>>,
    /// Directory where `{id}.json` files land. Survives
    /// dispatcher restarts via the PVC; see
    /// `deploy/k8s/dispatcher.yaml`.
    data_dir: PathBuf,
}

pub struct StoredProject {
    pub id: uuid::Uuid,
    pub name: String,
    pub status: ProjectStatus,
    pub project: ProjectDefinition,
}

#[derive(Serialize, Deserialize)]
struct PersistedProject {
    id: uuid::Uuid,
    name: String,
    status: String,
    project: ProjectDefinition,
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

impl ProjectStore {
    pub fn new(data_dir: PathBuf) -> anyhow::Result<Self> {
        std::fs::create_dir_all(&data_dir)?;
        let mut map: HashMap<uuid::Uuid, StoredProject> = HashMap::new();
        for entry in std::fs::read_dir(&data_dir)? {
            let entry = match entry {
                Ok(e) => e,
                Err(_) => continue,
            };
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) != Some("json") {
                continue;
            }
            let text = match std::fs::read_to_string(&path) {
                Ok(t) => t,
                Err(e) => {
                    tracing::warn!("project_store: cannot read {}: {e}", path.display());
                    continue;
                }
            };
            let persisted: PersistedProject = match serde_json::from_str(&text) {
                Ok(p) => p,
                Err(e) => {
                    tracing::warn!("project_store: cannot parse {}: {e}", path.display());
                    continue;
                }
            };
            map.insert(
                persisted.id,
                StoredProject {
                    id: persisted.id,
                    name: persisted.name,
                    // Any "active" from a previous session is
                    // stale: the dispatcher pod restarted, the
                    // listener Pod it spawned may be gone, the
                    // signal tracker is empty. Downgrade to
                    // inactive so the user (or an activate call)
                    // brings it back up cleanly.
                    status: {
                        let s = ProjectStatus::from_str(&persisted.status);
                        if s == ProjectStatus::Active {
                            ProjectStatus::Inactive
                        } else {
                            s
                        }
                    },
                    project: persisted.project,
                },
            );
        }
        Ok(Self {
            inner: Arc::new(RwLock::new(map)),
            data_dir,
        })
    }

    /// Register (or update) a project. Called by `POST /projects`
    /// with a parsed ProjectDefinition. Idempotent on id.
    pub async fn register(
        &self,
        project: ProjectDefinition,
    ) -> anyhow::Result<StoredProjectSummary> {
        let id = project.id;
        let name = project.name.clone();

        {
            let mut lock = self.inner.write().await;
            lock.insert(
                id,
                StoredProject {
                    id,
                    name: name.clone(),
                    status: ProjectStatus::Registered,
                    project,
                },
            );
            self.persist_locked(&lock, id)?;
        }
        Ok(StoredProjectSummary { id, name, status: ProjectStatus::Registered })
    }

    pub async fn list(&self) -> Vec<StoredProjectSummary> {
        let lock = self.inner.read().await;
        lock.values()
            .map(|p| StoredProjectSummary {
                id: p.id,
                name: p.name.clone(),
                status: p.status,
            })
            .collect()
    }

    pub async fn get(&self, id: uuid::Uuid) -> Option<StoredProjectSummary> {
        let lock = self.inner.read().await;
        lock.get(&id).map(|p| StoredProjectSummary {
            id: p.id,
            name: p.name.clone(),
            status: p.status,
        })
    }

    pub async fn remove(&self, id: uuid::Uuid) -> bool {
        let mut lock = self.inner.write().await;
        if lock.remove(&id).is_some() {
            let _ = std::fs::remove_file(self.file_for(id));
            true
        } else {
            false
        }
    }

    pub async fn set_status(&self, id: uuid::Uuid, status: ProjectStatus) -> bool {
        let mut lock = self.inner.write().await;
        if let Some(p) = lock.get_mut(&id) {
            p.status = status;
            let _ = self.persist_locked(&lock, id);
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

    fn file_for(&self, id: uuid::Uuid) -> PathBuf {
        self.data_dir.join(format!("{id}.json"))
    }

    /// Write the single record for `id` to disk. Caller holds
    /// the map's write lock, so we pass a ref in to avoid
    /// re-locking.
    fn persist_locked(
        &self,
        map: &HashMap<uuid::Uuid, StoredProject>,
        id: uuid::Uuid,
    ) -> anyhow::Result<()> {
        let Some(p) = map.get(&id) else { return Ok(()) };
        let persisted = PersistedProject {
            id: p.id,
            name: p.name.clone(),
            status: p.status.as_str().to_string(),
            project: p.project.clone(),
        };
        let json = serde_json::to_string_pretty(&persisted)?;
        std::fs::write(self.file_for(id), json)?;
        Ok(())
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
