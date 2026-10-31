//! Working copies: where a project's code is being edited
//! (`weft_core::working_copy` says what one is).
//!
//! One row per working copy, in the `working_copy` table of the versions
//! group (`crate::versions::GROUP`): its head, its working manifest, what
//! its folder held the last time a syncer saw the two agree (`synced`),
//! the conflict copies that syncer found, and a revision bumped on every
//! change of the working manifest. A change commits with a
//! `pg_notify(WORKING_COPY_CHANNEL, '<project>/<working copy>')`, which is
//! how the local runtime's folder sync hears a program wrote into a folder's
//! copy, and how the editor's view of the tree learns it moved.
//!
//! The decisions (what a set of changes makes of a working manifest,
//! which copy a project's default is) are pure functions; the stores only
//! read and write rows. Every change of a working manifest is a
//! compare-and-set inside one transaction: a change names what it expects
//! each path to hold ([`weft_core::working_copy::Expect`]), so two writers
//! of one file cannot silently overwrite each other.

use std::sync::Arc;

use async_trait::async_trait;
use weft_task_store::db::{self, Client, Pool};
use weft_core::project::hash::Manifest;
use weft_core::working_copy::{Head, WorkingChange, WorkingCopy, WorkingCopyId, WorkingCopyKind};
use weft_core::RunId;

/// What a working copy's change announces, as `<project>/<working copy>`.
// SYNC: WORKING_COPY_CHANNEL <-> crate::app::DISPATCHER_CHANNELS
pub const WORKING_COPY_CHANNEL: &str = "weft_working_copy";

/// One working copy's row: what the wire shows, and its two manifests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkingCopyRow {
    pub id: WorkingCopyId,
    pub project_id: uuid::Uuid,
    pub kind: WorkingCopyKind,
    pub host: Option<String>,
    pub path: Option<String>,
    pub head: Head,
    pub working: Manifest,
    pub synced: Manifest,
    pub conflicts: Vec<String>,
    pub revision: u64,
    pub created_at: u64,
    pub updated_at: u64,
}

impl WorkingCopyRow {
    /// The row as every answer shows it.
    pub fn summary(&self) -> WorkingCopy {
        WorkingCopy {
            id: self.id,
            kind: self.kind,
            host: self.host.clone(),
            path: self.path.clone(),
            head: self.head.clone(),
            working_version: weft_core::project::hash::manifest_version_id(&self.working),
            revision: self.revision,
            conflicts: self.conflicts.clone(),
            updated_at: self.updated_at,
        }
    }
}

/// What a write to a working copy's files came to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Written {
    /// It landed; the row as it is now.
    Landed(Box<WorkingCopyRow>),
    /// Nothing landed: these paths do not hold what their change expected.
    Stale(Vec<String>),
}

/// A sync pass's write ([`WorkingCopyStoreOps::write`]): the working
/// manifest's changes, and when a folder's syncer wrote it, what the folder
/// holds now for the paths it looked at and every conflict copy it saw.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Write<'a> {
    pub changes: &'a [WorkingChange],
    pub synced: Option<&'a std::collections::BTreeMap<String, Option<String>>>,
    pub conflicts: Option<&'a [String]>,
}

#[async_trait]
pub trait WorkingCopyStoreOps: Send + Sync {
    /// Every working copy of the project, oldest first.
    async fn list(&self, project: uuid::Uuid) -> anyhow::Result<Vec<WorkingCopyRow>>;
    async fn get(&self, id: WorkingCopyId) -> anyhow::Result<Option<WorkingCopyRow>>;
    /// The folder working copy at `path` on `host`, if the project has one.
    async fn folder(&self, project: uuid::Uuid, host: &str, path: &str) -> anyhow::Result<Option<WorkingCopyRow>>;
    /// Every folder working copy of every project, oldest first: what a
    /// local install's folder service keeps in sync.
    async fn all_folders(&self) -> anyhow::Result<Vec<WorkingCopyRow>>;
    /// Make the folder working copy at `path` on `host` with `working` as
    /// its files (and what the folder holds) and `head` as its head, unless
    /// the project has it already. Answers the row either way: an existing
    /// one is left exactly as it was.
    async fn register_folder(
        &self,
        project: uuid::Uuid,
        host: &str,
        path: &str,
        working: &Manifest,
        head: &Head,
        now: u64,
    ) -> anyhow::Result<WorkingCopyRow>;
    /// The project's program copy, made with `working` and `head` when it
    /// has none yet.
    async fn program_copy(&self, project: uuid::Uuid, working: &Manifest, head: &Head, now: u64) -> anyhow::Result<WorkingCopyRow>;
    /// Change files of a working copy, all or none (see [`Write`]).
    async fn write(&self, id: WorkingCopyId, write: Write<'_>, now: u64) -> anyhow::Result<Written>;
    /// Replace a working copy's files and head together (a switch), but
    /// only while its revision is still `revision`. Answers the row, or
    /// `None` when somebody changed it since.
    async fn replace(
        &self,
        id: WorkingCopyId,
        revision: u64,
        working: &Manifest,
        head: &Head,
        now: u64,
    ) -> anyhow::Result<Option<WorkingCopyRow>>;
    /// Move a working copy's head, but only if it is still where
    /// `expected` says: every caller decided where to move it by reading
    /// it first, so a blind write is a lost update. False when it moved
    /// underneath.
    async fn move_head(&self, id: WorkingCopyId, expected: &Head, to: &Head, now: u64) -> anyhow::Result<bool>;
    /// Clear every head's run pointer that names `run_id`, a run being
    /// erased: the working copies keep their head's version.
    async fn forget_head_run(&self, run_id: RunId) -> anyhow::Result<()>;
    /// Remove one working copy (a folder that is gone, a program copy
    /// nobody uses), but only while its revision is still `revision`, so a
    /// write that landed after the caller read it is not lost with it.
    /// False when somebody changed it since.
    async fn remove(&self, id: WorkingCopyId, revision: u64) -> anyhow::Result<bool>;
}

pub type WorkingCopyStore = Arc<dyn WorkingCopyStoreOps>;

/// The copy a call names when it names none: the project's most
/// recently changed folder, so a person's folder follows what a program
/// writes for it, else its program copy (`None`: it has none yet).
pub fn default_copy(copies: &[WorkingCopyRow]) -> Option<&WorkingCopyRow> {
    copies
        .iter()
        .filter(|c| c.kind == WorkingCopyKind::Folder)
        .max_by_key(|c| (c.updated_at, c.created_at))
        .or_else(|| copies.iter().find(|c| c.kind == WorkingCopyKind::Program))
}

/// The folder of `copies` on the machine named `here` a project's worker
/// runs in when it runs on that machine: the default copy among the
/// folders there ([`default_copy`]: the most recently changed). `None`
/// when none of the project's folders is on that machine.
pub fn folder_on<'a>(copies: &'a [WorkingCopyRow], here: &str) -> Option<&'a str> {
    let on_machine: Vec<WorkingCopyRow> = copies
        .iter()
        .filter(|c| c.kind == WorkingCopyKind::Folder && c.host.as_deref() == Some(here) && c.path.is_some())
        .cloned()
        .collect();
    let chosen = default_copy(&on_machine)?.id;
    copies.iter().find(|c| c.id == chosen).and_then(|c| c.path.as_deref())
}

/// Each project's folder on this machine, the one its worker process runs
/// in ([`folder_on`]), read from the working copies when a worker starts
/// (`weft_platform_traits::ProjectFolders`, which the local runner asks).
pub struct FoldersHere {
    copies: WorkingCopyStore,
    /// This machine's host name, as folders register it.
    here: String,
}

impl FoldersHere {
    pub fn new(copies: WorkingCopyStore) -> anyhow::Result<Self> {
        Ok(Self { copies, here: weft_core::working_copy::this_machine().map_err(anyhow::Error::msg)? })
    }
}

#[async_trait]
impl weft_platform_traits::ProjectFolders for FoldersHere {
    async fn folder(&self, project: uuid::Uuid) -> anyhow::Result<Option<std::path::PathBuf>> {
        let copies = self.copies.list(project).await?;
        Ok(folder_on(&copies, &self.here).map(std::path::PathBuf::from))
    }
}

/// The head a new working copy starts on: the head of the project's most
/// recently changed copy, so a folder registered where somebody already
/// worked carries on from there.
pub fn adopted_head(copies: &[WorkingCopyRow]) -> Head {
    copies.iter().max_by_key(|c| (c.updated_at, c.created_at)).map(|c| c.head.clone()).unwrap_or_default()
}

pub struct PostgresWorkingCopyStore {
    pool: Pool,
}

impl PostgresWorkingCopyStore {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }
}

const COLUMNS: &str = "id, project_id, kind, host, path, head_version, head_run, working_manifest, synced, conflicts, revision, created_at, updated_at";

type RowTuple = (
    uuid::Uuid,
    uuid::Uuid,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<uuid::Uuid>,
    db::Json<serde_json::Value>,
    db::Json<serde_json::Value>,
    Vec<String>,
    i64,
    i64,
    i64,
);

fn row_from(row: RowTuple) -> anyhow::Result<WorkingCopyRow> {
    let (id, project_id, kind, host, path, head_version, head_run, db::Json(working), db::Json(synced), conflicts, revision, created_at, updated_at) = row;
    Ok(WorkingCopyRow {
        id,
        project_id,
        kind: WorkingCopyKind::parse(&kind).map_err(anyhow::Error::msg)?,
        host,
        path,
        head: Head { head_version, head_run },
        working: serde_json::from_value(working).map_err(|e| anyhow::anyhow!("working copy {id}'s working manifest: {e}"))?,
        synced: serde_json::from_value(synced).map_err(|e| anyhow::anyhow!("working copy {id}'s synced files: {e}"))?,
        conflicts,
        revision: revision.max(0) as u64,
        created_at: created_at.max(0) as u64,
        updated_at: updated_at.max(0) as u64,
    })
}

async fn notify(conn: &impl Client, project: uuid::Uuid, id: WorkingCopyId) -> anyhow::Result<()> {
    conn.execute("SELECT pg_notify($1, $2)", &[&WORKING_COPY_CHANNEL, &format!("{project}/{id}")]).await?;
    Ok(())
}

impl PostgresWorkingCopyStore {
    async fn insert(
        &self,
        project: uuid::Uuid,
        kind: WorkingCopyKind,
        place: Option<(&str, &str)>,
        working: &Manifest,
        head: &Head,
        now: u64,
    ) -> anyhow::Result<()> {
        let synced = match kind {
            WorkingCopyKind::Folder => working.clone(),
            WorkingCopyKind::Program => Manifest::new(),
        };
        let mut conn = self.pool.get().await?;
        let tx = conn.transaction().await?;
        let inserted = tx
            .execute(
                "INSERT INTO working_copy (id, project_id, kind, host, path, head_version, head_run, working_manifest, synced, conflicts, revision, created_at, updated_at) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, '{}', 0, $10, $10) ON CONFLICT DO NOTHING",
                &[
                    &uuid::Uuid::now_v7(),
                    &project,
                    &kind.as_str(),
                    &place.map(|(host, _)| host),
                    &place.map(|(_, path)| path),
                    &head.head_version.as_deref(),
                    &head.head_run,
                    &db::Json(db::WeftMade(working)),
                    &db::Json(db::WeftMade(&synced)),
                    &(now as i64),
                ],
            )
            .await?;
        if inserted == 1 {
            tx.execute("SELECT pg_notify($1, $2)", &[&WORKING_COPY_CHANNEL, &format!("{project}/new")]).await?;
        }
        tx.commit().await?;
        Ok(())
    }
}

#[async_trait]
impl WorkingCopyStoreOps for PostgresWorkingCopyStore {
    async fn list(&self, project: uuid::Uuid) -> anyhow::Result<Vec<WorkingCopyRow>> {
        let rows: Vec<RowTuple> =
            self.pool.all(&format!("SELECT {COLUMNS} FROM working_copy WHERE project_id = $1 ORDER BY created_at, id"), &[&project]).await?;
        rows.into_iter().map(row_from).collect()
    }

    async fn get(&self, id: WorkingCopyId) -> anyhow::Result<Option<WorkingCopyRow>> {
        let row: Option<RowTuple> = self.pool.opt(&format!("SELECT {COLUMNS} FROM working_copy WHERE id = $1"), &[&id]).await?;
        row.map(row_from).transpose()
    }

    async fn folder(&self, project: uuid::Uuid, host: &str, path: &str) -> anyhow::Result<Option<WorkingCopyRow>> {
        let row: Option<RowTuple> = self
            .pool
            .opt(
                &format!("SELECT {COLUMNS} FROM working_copy WHERE project_id = $1 AND kind = 'folder' AND host = $2 AND path = $3"),
                &[&project, &host, &path],
            )
            .await?;
        row.map(row_from).transpose()
    }

    async fn all_folders(&self) -> anyhow::Result<Vec<WorkingCopyRow>> {
        let rows: Vec<RowTuple> = self.pool.all(&format!("SELECT {COLUMNS} FROM working_copy WHERE kind = 'folder' ORDER BY created_at, id"), &[]).await?;
        rows.into_iter().map(row_from).collect()
    }

    async fn register_folder(
        &self,
        project: uuid::Uuid,
        host: &str,
        path: &str,
        working: &Manifest,
        head: &Head,
        now: u64,
    ) -> anyhow::Result<WorkingCopyRow> {
        self.insert(project, WorkingCopyKind::Folder, Some((host, path)), working, head, now).await?;
        self.folder(project, host, path).await?.ok_or_else(|| anyhow::anyhow!("the folder working copy at {path} went as it was made"))
    }

    async fn program_copy(&self, project: uuid::Uuid, working: &Manifest, head: &Head, now: u64) -> anyhow::Result<WorkingCopyRow> {
        self.insert(project, WorkingCopyKind::Program, None, working, head, now).await?;
        let row: Option<RowTuple> =
            self.pool.opt(&format!("SELECT {COLUMNS} FROM working_copy WHERE project_id = $1 AND kind = 'program'"), &[&project]).await?;
        row.map(row_from).transpose()?.ok_or_else(|| anyhow::anyhow!("the program copy of project {project} went as it was made"))
    }

    async fn write(&self, id: WorkingCopyId, write: Write<'_>, now: u64) -> anyhow::Result<Written> {
        let mut conn = self.pool.get().await?;
        let tx = conn.transaction().await?;
        let row: Option<RowTuple> = tx.opt(&format!("SELECT {COLUMNS} FROM working_copy WHERE id = $1 FOR UPDATE"), &[&id]).await?;
        let row = row_from(row.ok_or_else(|| anyhow::anyhow!("no working copy {id}"))?)?;
        let working = match weft_core::working_copy::apply_changes(&row.working, write.changes) {
            Ok(working) => working,
            Err(stale) => return Ok(Written::Stale(stale)),
        };
        let moved = working != row.working;
        let synced = write.synced.map(|report| weft_core::working_copy::fold_synced(&row.synced, report)).unwrap_or_else(|| row.synced.clone());
        let conflicts = write.conflicts.map(<[String]>::to_vec).unwrap_or_else(|| row.conflicts.clone());
        let revision = row.revision + u64::from(moved);
        // A pass that changed nothing at all writes nothing: every sync of
        // an unchanged folder would otherwise bump `updated_at`, and the
        // default copy (the most recently changed folder) would follow
        // whichever folder was merely looked at last.
        if !moved && synced == row.synced && conflicts == row.conflicts {
            return Ok(Written::Landed(Box::new(row)));
        }
        tx.execute(
            "UPDATE working_copy SET working_manifest = $2, synced = $3, conflicts = $4, revision = $5, \
             updated_at = CASE WHEN $6::boolean THEN $7::bigint ELSE updated_at END WHERE id = $1",
            &[
                &id,
                &db::Json(db::WeftMade(&working)),
                &db::Json(db::WeftMade(&synced)),
                &conflicts,
                &(revision as i64),
                &moved,
                &(now as i64),
            ],
        )
        .await?;
        notify(&tx, row.project_id, id).await?;
        tx.commit().await?;
        let updated_at = if moved { now } else { row.updated_at };
        Ok(Written::Landed(Box::new(WorkingCopyRow { working, synced, conflicts, revision, updated_at, ..row })))
    }

    async fn replace(
        &self,
        id: WorkingCopyId,
        revision: u64,
        working: &Manifest,
        head: &Head,
        now: u64,
    ) -> anyhow::Result<Option<WorkingCopyRow>> {
        let mut conn = self.pool.get().await?;
        let tx = conn.transaction().await?;
        let project: Option<uuid::Uuid> = tx
            .scalar_opt(
                "UPDATE working_copy SET working_manifest = $3, head_version = $4, head_run = $5, revision = revision + 1, updated_at = $6 \
                 WHERE id = $1 AND revision = $2 RETURNING project_id",
                &[&id, &(revision as i64), &db::Json(db::WeftMade(working)), &head.head_version.as_deref(), &head.head_run, &(now as i64)],
            )
            .await?;
        let Some(project) = project else { return Ok(None) };
        notify(&tx, project, id).await?;
        tx.commit().await?;
        self.get(id).await
    }

    async fn move_head(&self, id: WorkingCopyId, expected: &Head, to: &Head, now: u64) -> anyhow::Result<bool> {
        // One statement, so the check and the write cannot be split by a
        // sibling process. `IS NOT DISTINCT FROM` because both columns are
        // nullable and NULL = NULL is not true in SQL.
        let mut conn = self.pool.get().await?;
        let tx = conn.transaction().await?;
        let project: Option<uuid::Uuid> = tx
            .scalar_opt(
                "UPDATE working_copy SET head_version = $2, head_run = $3, updated_at = $6 \
                 WHERE id = $1 AND head_version IS NOT DISTINCT FROM $4 AND head_run IS NOT DISTINCT FROM $5 \
                 RETURNING project_id",
                &[
                    &id,
                    &to.head_version.as_deref(),
                    &to.head_run,
                    &expected.head_version.as_deref(),
                    &expected.head_run,
                    &(now as i64),
                ],
            )
            .await?;
        if let Some(project) = project {
            notify(&tx, project, id).await?;
            tx.commit().await?;
            return Ok(true);
        }
        let exists: bool = tx.scalar("SELECT EXISTS(SELECT 1 FROM working_copy WHERE id = $1)", &[&id]).await?;
        anyhow::ensure!(exists, "no working copy {id}");
        Ok(false)
    }

    async fn forget_head_run(&self, run_id: RunId) -> anyhow::Result<()> {
        self.pool.execute("UPDATE working_copy SET head_run = NULL WHERE head_run = $1", &[&run_id]).await?;
        Ok(())
    }

    async fn remove(&self, id: WorkingCopyId, revision: u64) -> anyhow::Result<bool> {
        let mut conn = self.pool.get().await?;
        let tx = conn.transaction().await?;
        let project: Option<uuid::Uuid> =
            tx.scalar_opt("DELETE FROM working_copy WHERE id = $1 AND revision = $2 RETURNING project_id", &[&id, &(revision as i64)]).await?;
        let Some(project) = project else { return Ok(false) };
        notify(&tx, project, id).await?;
        tx.commit().await?;
        Ok(true)
    }
}

/// In-memory store for layer-3 tests: plain rows, the same refusals as
/// the table (one folder per place, one program copy per project). Each
/// write it lands is told on `changed`, standing in for the notification.
#[cfg(any(test, feature = "test-helpers"))]
#[derive(Default)]
pub struct FakeWorkingCopyStore {
    rows: std::sync::Mutex<Vec<WorkingCopyRow>>,
    pub changed: std::sync::Mutex<Vec<WorkingCopyId>>,
}

#[cfg(any(test, feature = "test-helpers"))]
impl FakeWorkingCopyStore {
    pub fn new() -> Self {
        Self::default()
    }

    fn insert(&self, project: uuid::Uuid, kind: WorkingCopyKind, place: Option<(&str, &str)>, working: &Manifest, head: &Head, now: u64) -> WorkingCopyRow {
        let mut rows = self.rows.lock().unwrap();
        let existing = rows.iter().find(|r| {
            r.project_id == project
                && r.kind == kind
                && match place {
                    Some((host, path)) => r.host.as_deref() == Some(host) && r.path.as_deref() == Some(path),
                    None => true,
                }
        });
        if let Some(existing) = existing {
            return existing.clone();
        }
        let row = WorkingCopyRow {
            id: uuid::Uuid::now_v7(),
            project_id: project,
            kind,
            host: place.map(|(host, _)| host.to_string()),
            path: place.map(|(_, path)| path.to_string()),
            head: head.clone(),
            working: working.clone(),
            synced: if kind == WorkingCopyKind::Folder { working.clone() } else { Manifest::new() },
            conflicts: Vec::new(),
            revision: 0,
            created_at: now,
            updated_at: now,
        };
        rows.push(row.clone());
        row
    }
}

#[cfg(any(test, feature = "test-helpers"))]
#[async_trait]
impl WorkingCopyStoreOps for FakeWorkingCopyStore {
    async fn list(&self, project: uuid::Uuid) -> anyhow::Result<Vec<WorkingCopyRow>> {
        Ok(self.rows.lock().unwrap().iter().filter(|r| r.project_id == project).cloned().collect())
    }

    async fn get(&self, id: WorkingCopyId) -> anyhow::Result<Option<WorkingCopyRow>> {
        Ok(self.rows.lock().unwrap().iter().find(|r| r.id == id).cloned())
    }

    async fn folder(&self, project: uuid::Uuid, host: &str, path: &str) -> anyhow::Result<Option<WorkingCopyRow>> {
        Ok(self
            .rows
            .lock()
            .unwrap()
            .iter()
            .find(|r| r.project_id == project && r.kind == WorkingCopyKind::Folder && r.host.as_deref() == Some(host) && r.path.as_deref() == Some(path))
            .cloned())
    }

    async fn all_folders(&self) -> anyhow::Result<Vec<WorkingCopyRow>> {
        Ok(self.rows.lock().unwrap().iter().filter(|r| r.kind == WorkingCopyKind::Folder).cloned().collect())
    }

    async fn register_folder(&self, project: uuid::Uuid, host: &str, path: &str, working: &Manifest, head: &Head, now: u64) -> anyhow::Result<WorkingCopyRow> {
        Ok(self.insert(project, WorkingCopyKind::Folder, Some((host, path)), working, head, now))
    }

    async fn program_copy(&self, project: uuid::Uuid, working: &Manifest, head: &Head, now: u64) -> anyhow::Result<WorkingCopyRow> {
        Ok(self.insert(project, WorkingCopyKind::Program, None, working, head, now))
    }

    async fn write(&self, id: WorkingCopyId, write: Write<'_>, now: u64) -> anyhow::Result<Written> {
        let mut rows = self.rows.lock().unwrap();
        let row = rows.iter_mut().find(|r| r.id == id).ok_or_else(|| anyhow::anyhow!("no working copy {id}"))?;
        let working = match weft_core::working_copy::apply_changes(&row.working, write.changes) {
            Ok(working) => working,
            Err(stale) => return Ok(Written::Stale(stale)),
        };
        let moved = working != row.working;
        if let Some(report) = write.synced {
            row.synced = weft_core::working_copy::fold_synced(&row.synced, report);
        }
        if let Some(conflicts) = write.conflicts {
            row.conflicts = conflicts.to_vec();
        }
        if moved {
            row.working = working;
            row.revision += 1;
            row.updated_at = now;
            self.changed.lock().unwrap().push(id);
        }
        Ok(Written::Landed(Box::new(row.clone())))
    }

    async fn replace(&self, id: WorkingCopyId, revision: u64, working: &Manifest, head: &Head, now: u64) -> anyhow::Result<Option<WorkingCopyRow>> {
        let mut rows = self.rows.lock().unwrap();
        let Some(row) = rows.iter_mut().find(|r| r.id == id && r.revision == revision) else { return Ok(None) };
        row.working = working.clone();
        row.head = head.clone();
        row.revision += 1;
        row.updated_at = now;
        self.changed.lock().unwrap().push(id);
        Ok(Some(row.clone()))
    }

    async fn move_head(&self, id: WorkingCopyId, expected: &Head, to: &Head, now: u64) -> anyhow::Result<bool> {
        let mut rows = self.rows.lock().unwrap();
        let row = rows.iter_mut().find(|r| r.id == id).ok_or_else(|| anyhow::anyhow!("no working copy {id}"))?;
        if &row.head != expected {
            return Ok(false);
        }
        row.head = to.clone();
        row.updated_at = now;
        Ok(true)
    }

    async fn forget_head_run(&self, run_id: RunId) -> anyhow::Result<()> {
        for row in self.rows.lock().unwrap().iter_mut() {
            if row.head.head_run == Some(run_id) {
                row.head.head_run = None;
            }
        }
        Ok(())
    }

    async fn remove(&self, id: WorkingCopyId, revision: u64) -> anyhow::Result<bool> {
        let mut rows = self.rows.lock().unwrap();
        let before = rows.len();
        rows.retain(|r| !(r.id == id && r.revision == revision));
        Ok(rows.len() < before)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(kind: WorkingCopyKind, created: u64, updated: u64, head: Option<&str>) -> WorkingCopyRow {
        WorkingCopyRow {
            id: uuid::Uuid::now_v7(),
            project_id: uuid::Uuid::nil(),
            kind,
            host: (kind == WorkingCopyKind::Folder).then(|| "box".into()),
            path: (kind == WorkingCopyKind::Folder).then(|| format!("/p{created}")),
            head: Head { head_version: head.map(str::to_string), head_run: None },
            working: Manifest::new(),
            synced: Manifest::new(),
            conflicts: Vec::new(),
            revision: 0,
            created_at: created,
            updated_at: updated,
        }
    }

    #[test]
    fn a_worker_runs_in_the_most_recently_changed_folder_on_its_machine() {
        let older = row(WorkingCopyKind::Folder, 1, 5, None);
        let newer = row(WorkingCopyKind::Folder, 2, 9, None);
        let elsewhere = WorkingCopyRow { host: Some("other".into()), ..row(WorkingCopyKind::Folder, 3, 50, None) };
        let program = row(WorkingCopyKind::Program, 4, 99, None);
        let copies = [older, newer, elsewhere, program];
        assert_eq!(folder_on(&copies, "box"), Some("/p2"), "the newest folder on this machine, never another machine's");
        assert_eq!(folder_on(&copies, "nowhere"), None, "no folder here, and the program copy is no folder");
    }

    #[test]
    fn the_default_copy_is_the_most_recently_changed_folder_else_the_program_copy() {
        let program = row(WorkingCopyKind::Program, 1, 50, Some("p"));
        let old = row(WorkingCopyKind::Folder, 2, 10, Some("a"));
        let fresh = row(WorkingCopyKind::Folder, 3, 20, Some("b"));
        assert_eq!(default_copy(&[program.clone(), old.clone(), fresh.clone()]).map(|c| c.id), Some(fresh.id));
        assert_eq!(default_copy(std::slice::from_ref(&program)).map(|c| c.id), Some(program.id));
        assert!(default_copy(&[]).is_none());
        // A new copy carries on from wherever somebody last worked, the
        // program copy included.
        assert_eq!(adopted_head(&[program, old, fresh]).head_version.as_deref(), Some("p"));
        assert_eq!(adopted_head(&[]), Head::default());
    }

    /// Two writers of one file: the one that read it before the other
    /// wrote is refused, and nothing of its write lands.
    #[tokio::test]
    async fn a_stale_writer_is_refused_and_lands_nothing() {
        let store = FakeWorkingCopyStore::new();
        let start: Manifest = [("a".to_string(), "1".to_string())].into();
        let copy = store.program_copy(uuid::Uuid::nil(), &start, &Head::default(), 1).await.unwrap();
        let change = |hash: &str, read: &str| WorkingChange {
            path: "a".into(),
            hash: Some(hash.into()),
            expected: weft_core::working_copy::Expect::Hash { hash: read.into() },
        };
        let first = store.write(copy.id, Write { changes: &[change("2", "1")], ..Default::default() }, 2).await.unwrap();
        assert!(matches!(&first, Written::Landed(row) if row.revision == 1 && row.working["a"] == "2"), "{first:?}");
        let second = store.write(copy.id, Write { changes: &[change("3", "1")], ..Default::default() }, 3).await.unwrap();
        assert_eq!(second, Written::Stale(vec!["a".into()]));
        assert_eq!(store.get(copy.id).await.unwrap().unwrap().working["a"], "2");
    }
}
