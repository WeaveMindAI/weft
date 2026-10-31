//! The version tree: what code a project has been, and what ran on it.
//!
//! Two kinds of node, one tree. A **version** is the project's files,
//! content-addressed: `id = sha256(canonical manifest)` where the
//! manifest maps every covered path to the sha256 of its bytes, so
//! identical code is one row however often it is run, and a version
//! points at the version it was edited from (`parent_id`; a tree root
//! has none). A **run** is one run under a version: a run started
//! by hand (`weft run`) is shown with the run it was seeded from
//! (`seed_run_id`), the stale set of that seed edge, and the spec it
//! ran, all read off its `run` row; a trigger's runs are counted per
//! version (`version_runs`), since a busy trigger runs far too often for
//! the tree to show each one. The files themselves live in the tenant's assets,
//! published through `weft_assets::publish_files` under `asset/<sha>`: a
//! blob identical to one any version of any of the tenant's projects held
//! costs nothing to record again, and `weft prune` reclaims what no
//! surviving manifest of any of them names.
//!
//! A head belongs to each working copy (`crate::working_copies`), git's
//! HEAD and nothing more: moved by a stamp, a run and a switch. Beside
//! them, [`VersionStoreOps::activated_versions`] is read off the project's
//! trigger activations: every version a listening trigger was set up on.
//! What keeps a version from a prune or a sweep is all of those together
//! ([`Kept`]).
//!
//! Every write goes through [`VersionStoreOps`]; the Postgres store
//! runs SQL, the fake keeps maps, and the decisions (which run seeds
//! the next one, what a prune removes, what changed between two
//! versions) are pure functions over the rows so they are tested at
//! layer 1 and the handlers only gather and dispatch.

use std::collections::{BTreeMap, BTreeSet, HashSet};

use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use weft_task_store::db::{self, Client, Pool};
use weft_core::run_spec::RunSpec;
use weft_core::versions::PrunePlan;
use weft_core::working_copy::Head;
use weft_core::RunId;

use crate::working_copies::WorkingCopyRow;

/// The `project_version` + `version_runs` tables. Canonical DDL edited in
/// place; an existing database is carried forward by `./setup.sh
/// --migration <name>` (the contract is `weft_task_store::schema_guard`'s
/// header). Applied by the boot's one schema pass after
/// `project_store::GROUP`, which owns the `project` row every table
/// here hangs off.
pub static GROUP: weft_task_store::SchemaGroup = weft_task_store::SchemaGroup {
    name: "versions",
    tables: &["project_version", "version_runs", "working_copy"],
    ddl: &[
        // project_version: one row per distinct file set a project has
        // been. `id` is the sha256 of the canonical manifest, so the
        // same code is one row; `parent_id` is the version it was
        // edited from (NULL = a tree root; `weft run --root` starts
        // one); `label` is a checkpoint's name.
        r#"CREATE TABLE IF NOT EXISTS project_version (
            id           TEXT NOT NULL,
            -- Deliberately NO foreign key to `project`: the tree's life is
            -- the store's to decide, not the database's. A project's
            -- removal (`ProjectStore::remove`) drops the tree explicitly,
            -- so the same id registered again never inherits a tree it did
            -- not make and no version keeps naming stored files of a
            -- project that no longer exists. The journal's runs go
            -- just before, so a removal leaves the person their files and
            -- nothing else.
            project_id   UUID NOT NULL,
            parent_id    TEXT,
            manifest     JSONB NOT NULL,
            label        TEXT,
            created_at   BIGINT NOT NULL,
            -- Recording order. `created_at` is whole seconds, so two
            -- versions recorded in one second tie on it; `versions()`
            -- lists by this instead.
            seq          BIGSERIAL,
            PRIMARY KEY (project_id, id)
        )"#,
        "CREATE INDEX IF NOT EXISTS idx_project_version_parent ON project_version(project_id, parent_id)",
        // version_runs: how many runs a version had, and its newest, on
        // one row per worker lane writing runs (`weft_record_batch`, the
        // one writer), summed on read: one row shared by every lane would
        // be locked through every batch's wait for the disk.
        // The runs themselves are `run` rows (`source_version`). No foreign
        // key to the version: a batch of records never fails over a version
        // pruned under it; `delete_versions` drops their rows with them.
        r#"CREATE TABLE IF NOT EXISTS version_runs (
            project_id     UUID NOT NULL,
            source_version TEXT NOT NULL,
            lane           TEXT NOT NULL,
            runs           BIGINT NOT NULL,
            last_run       UUID NOT NULL,
            PRIMARY KEY (project_id, source_version, lane)
        )"#,
        // working_copy: one place a project's code is being edited
        // (`crate::working_copies`). No foreign key to `project`, for the
        // reason `project_version` has none: a project's removal drops its
        // working copies explicitly (`ProjectStore::remove`).
        // SYNC: the kind CHECK <-> weft_core::working_copy::WorkingCopyKind
        r#"CREATE TABLE IF NOT EXISTS working_copy (
            id               UUID PRIMARY KEY,
            project_id       UUID NOT NULL,
            kind             TEXT NOT NULL CHECK (kind IN ('folder', 'program')),
            -- The machine and the folder, for a folder; neither for a
            -- program's copy.
            host             TEXT,
            path             TEXT,
            -- Its head: the version the next stamp parents on, and the run
            -- the next `--seed` inherits from (NULL on a bare version).
            head_version     TEXT,
            head_run         UUID,
            -- Its files as they are now, `path -> sha256`, every blob in
            -- the tenant's assets.
            working_manifest JSONB NOT NULL,
            -- What the folder held, path by path, the last time a syncer
            -- saw it and the working copy agree; `{}` for a program's copy.
            synced           JSONB NOT NULL,
            -- The conflict copies the last sync found in the folder.
            conflicts        TEXT[] NOT NULL,
            -- Bumped on every change of `working_manifest`.
            revision         BIGINT NOT NULL,
            created_at       BIGINT NOT NULL,
            updated_at       BIGINT NOT NULL,
            CHECK ((kind = 'folder') = (host IS NOT NULL AND path IS NOT NULL))
        )"#,
        "CREATE INDEX IF NOT EXISTS idx_working_copy_project ON working_copy(project_id)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_working_copy_folder ON working_copy(project_id, host, path) WHERE kind = 'folder'",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_working_copy_program ON working_copy(project_id) WHERE kind = 'program'",
    ],
    seed: &[],
    data_steps: &[],
};

/// `path -> sha256` of every file a version covers.
pub use weft_core::project::hash::Manifest;

/// The id of the version `manifest` describes (`weft_core::project::
/// hash::manifest_version_id`; the dispatcher recomputes it and never
/// trusts a sent id).
pub fn version_id(manifest: &Manifest) -> String {
    weft_core::project::hash::manifest_version_id(manifest)
}

// The database row. NOT a wire type: `tree` answers `VersionSummary`
// (`api/versions.rs`), which is what the CLI and the editor read.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VersionRow {
    pub id: String,
    pub project_id: uuid::Uuid,
    pub parent_id: Option<String>,
    pub manifest: Manifest,
    pub label: Option<String>,
    pub created_at: u64,
}

/// One run started by hand (`weft run`), as the version tree shows it: read
/// off its `run` row (`source_version`, `seed_of`, `stale`, `spec`,
/// `example`), written once when it was queued. The record stays
/// authoritative (`weft_journal::seed_chain` walks the births, never these
/// columns); if a reader ever needs to DECIDE something from them, read the
/// record instead.
// The database row. NOT a wire type: `tree` answers `VersionRun`
// (`api/versions.rs`), which is what the CLI and the editor read.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunRow {
    pub run_id: RunId,
    pub project_id: uuid::Uuid,
    pub version_id: String,
    pub seed_run_id: Option<RunId>,
    pub stale: Vec<String>,
    pub spec: Option<RunSpec>,
    pub definition_hash: String,
    pub example: Option<String>,
    pub created_at: u64,
}

/// How many runs a version's triggers started, and the newest
/// (`version_runs`, summed over its writers).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TriggerRuns {
    pub runs: u64,
    pub last: RunId,
}

#[async_trait]
pub trait VersionStoreOps: Send + Sync {
    /// Record `version` unless the project already has it. Answers
    /// whether a row was created; an existing row keeps its parent and
    /// label (identical code is one version, whoever reaches it).
    async fn upsert_version(&self, version: &VersionRow) -> anyhow::Result<bool>;
    async fn version(&self, project: uuid::Uuid, id: &str) -> anyhow::Result<Option<VersionRow>>;
    /// Every version of the project, in the order they were recorded.
    async fn versions(&self, project: uuid::Uuid) -> anyhow::Result<Vec<VersionRow>>;
    async fn set_label(&self, project: uuid::Uuid, id: &str, label: Option<&str>) -> anyhow::Result<()>;
    /// Delete the named versions and their trigger run counts. The runs
    /// under them are the record's to erase.
    async fn delete_versions(&self, project: uuid::Uuid, ids: &[String]) -> anyhow::Result<()>;
    /// The versions something still depends on, which a delete refuses
    /// ([`VERSIONS_IN_USE_REFUSAL`]): an armed trigger's settings came
    /// from one, or a run not yet safely in the tree runs one.
    /// `include_bakes` also keeps the versions completed trigger bakes
    /// came from (what an automatic sweep keeps and a prune may drop).
    async fn versions_in_use(&self, project: uuid::Uuid, include_bakes: bool) -> anyhow::Result<BTreeSet<String>>;
    /// The source manifest the project's running build was registered
    /// with, if any: its blobs stay alive whatever the tree drops.
    async fn registered_source(&self, project: uuid::Uuid) -> anyhow::Result<Option<Manifest>>;

    /// The run `run_id` as the tree reads it, when it is a run of a
    /// program version (any run but a node test).
    async fn run(&self, run_id: RunId) -> anyhow::Result<Option<RunRow>>;
    /// Every run of the project started by hand, in the order they started.
    async fn runs(&self, project: uuid::Uuid) -> anyhow::Result<Vec<RunRow>>;
    /// How many runs each version's triggers started.
    async fn trigger_runs(&self, project: uuid::Uuid) -> anyhow::Result<BTreeMap<String, TriggerRuns>>;
    /// Set or clear the saved example associated with one run started by
    /// hand.
    async fn set_run_example(
        &self,
        run_id: RunId,
        example: Option<&str>,
    ) -> anyhow::Result<()>;

    /// Every version some activation's listeners run (one per trigger and
    /// owner at most), deduplicated, sorted. Empty while nothing listens.
    async fn activated_versions(&self, project: uuid::Uuid) -> anyhow::Result<Vec<String>>;
}

pub type VersionStore = Arc<dyn VersionStoreOps>;

pub struct PostgresVersionStore {
    pool: Pool,
}

impl PostgresVersionStore {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }
}

/// Why a version something still depends on cannot be deleted: the one
/// wording the store's own refusal and the prune endpoint's answer.
pub const VERSIONS_IN_USE_REFUSAL: &str =
    "these versions still supply trigger settings or running runs; `weft stop` the runs and `weft deactivate` the triggers (which wipes their settings) before pruning";

/// Source versions needed by armed settings or runs not yet safely
/// represented in the tree. Automatic sweeps also retain completed bakes.
async fn source_versions_in_use(conn: &impl Client, project: uuid::Uuid, include_bakes: bool) -> anyhow::Result<BTreeSet<String>> {
    let versions: Vec<Option<String>> = conn
        .scalars(
            "SELECT source_version FROM signal WHERE project_id = $1 \
             UNION SELECT r.source_version FROM run r \
             WHERE r.project_id = $1 \
               AND (r.state <> 'ended' OR EXISTS (SELECT 1 FROM trigger_setup s WHERE s.run_id = r.run_id)) \
             UNION SELECT source_version FROM trigger_bake WHERE project_id = $1 AND $2::boolean",
            &[&project, &include_bakes],
        )
        .await?;
    Ok(versions.into_iter().flatten().collect())
}

pub(crate) async fn retain_source_version(conn: &impl Client, project: uuid::Uuid, version: &str) -> anyhow::Result<()> {
    let found: Option<String> =
        conn.scalar_opt("SELECT id FROM project_version WHERE project_id = $1 AND id = $2 FOR KEY SHARE", &[&project, &version]).await?;
    anyhow::ensure!(found.is_some(), "source version {version} was removed during preparation; run the command again");
    Ok(())
}

type VersionTuple = (String, uuid::Uuid, Option<String>, db::Json<serde_json::Value>, Option<String>, i64);
type RunTuple = (
    uuid::Uuid,
    uuid::Uuid,
    String,
    Option<uuid::Uuid>,
    Vec<String>,
    Option<String>,
    weft_core::stored::Version,
    String,
    Option<String>,
    i64,
);

fn version_from(row: VersionTuple) -> anyhow::Result<VersionRow> {
    let (id, project_id, parent_id, db::Json(manifest), label, created_at) = row;
    Ok(VersionRow {
        id,
        project_id,
        parent_id,
        manifest: serde_json::from_value(manifest)?,
        label,
        created_at: created_at as u64,
    })
}

fn run_from(row: RunTuple) -> anyhow::Result<RunRow> {
    let (run_id, project_id, version_id, seed_run_id, stale, spec, spec_version, definition_hash, example, created_at) = row;
    Ok(RunRow {
        run_id,
        project_id,
        version_id,
        seed_run_id,
        stale,
        spec: spec
            .map(|spec| RunSpec::STORED.decode(spec.as_str(), spec_version))
            .transpose()
            .map_err(|e| e.at(format_args!("run {run_id}'s spec")))?,
        definition_hash,
        example,
        created_at: created_at as u64,
    })
}

const VERSION_COLUMNS: &str = "id, project_id, parent_id, manifest, label, created_at";
/// A run's tree columns, off its `run` row, in [`RunTuple`] order.
const RUN_COLUMNS: &str =
    "run_id, project_id, source_version, seed_of, stale, spec, spec_version, definition_hash, example, started_at";
/// A run the tree shows: a run of a program whose version is still in the
/// tree (a run outlives a version pruned under it, until retention).
const IN_TREE: &str = "source_version IS NOT NULL AND definition_hash IS NOT NULL \
     AND EXISTS (SELECT 1 FROM project_version v WHERE v.project_id = run.project_id AND v.id = run.source_version)";

#[async_trait]
impl VersionStoreOps for PostgresVersionStore {
    async fn upsert_version(&self, version: &VersionRow) -> anyhow::Result<bool> {
        let rows = self
            .pool
            .execute(
                "INSERT INTO project_version (id, project_id, parent_id, manifest, label, created_at) \
                 VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (project_id, id) DO NOTHING",
                &[
                    &version.id,
                    &version.project_id,
                    &version.parent_id,
                    &db::Json(db::WeftMade(&version.manifest)),
                    &version.label,
                    &(version.created_at as i64),
                ],
            )
            .await?;
        Ok(rows == 1)
    }

    async fn version(&self, project: uuid::Uuid, id: &str) -> anyhow::Result<Option<VersionRow>> {
        let row: Option<VersionTuple> =
            self.pool.opt(&format!("SELECT {VERSION_COLUMNS} FROM project_version WHERE project_id = $1 AND id = $2"), &[&project, &id]).await?;
        row.map(version_from).transpose()
    }

    async fn versions(&self, project: uuid::Uuid) -> anyhow::Result<Vec<VersionRow>> {
        let rows: Vec<VersionTuple> =
            self.pool.all(&format!("SELECT {VERSION_COLUMNS} FROM project_version WHERE project_id = $1 ORDER BY seq ASC"), &[&project]).await?;
        rows.into_iter().map(version_from).collect()
    }

    async fn set_label(&self, project: uuid::Uuid, id: &str, label: Option<&str>) -> anyhow::Result<()> {
        let rows = self.pool.execute("UPDATE project_version SET label = $3 WHERE project_id = $1 AND id = $2", &[&project, &id, &label]).await?;
        anyhow::ensure!(rows == 1, "version {id} not found");
        Ok(())
    }

    async fn delete_versions(&self, project: uuid::Uuid, ids: &[String]) -> anyhow::Result<()> {
        let mut conn = self.pool.get().await?;
        let tx = conn.transaction().await?;
        tx.execute("SELECT id FROM project_version WHERE project_id = $1 AND id = ANY($2) ORDER BY id FOR UPDATE", &[&project, &ids]).await?;
        let protected = source_versions_in_use(&tx, project, false).await?;
        anyhow::ensure!(!ids.iter().any(|id| protected.contains(id)), VERSIONS_IN_USE_REFUSAL);
        tx.execute("DELETE FROM trigger_bake WHERE project_id = $1 AND source_version = ANY($2)", &[&project, &ids]).await?;
        tx.execute("DELETE FROM version_runs WHERE project_id = $1 AND source_version = ANY($2)", &[&project, &ids]).await?;
        tx.execute("DELETE FROM project_version WHERE project_id = $1 AND id = ANY($2)", &[&project, &ids]).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn versions_in_use(&self, project: uuid::Uuid, include_bakes: bool) -> anyhow::Result<BTreeSet<String>> {
        source_versions_in_use(&self.pool, project, include_bakes).await
    }

    async fn registered_source(&self, project: uuid::Uuid) -> anyhow::Result<Option<Manifest>> {
        let source: Option<Option<db::Json<serde_json::Value>>> =
            self.pool.scalar_opt("SELECT running_source FROM project WHERE id = $1", &[&project]).await?;
        Ok(source.flatten().map(|source| serde_json::from_value(source.0)).transpose()?)
    }

    async fn run(&self, run_id: RunId) -> anyhow::Result<Option<RunRow>> {
        let row: Option<RunTuple> = self.pool.opt(&format!("SELECT {RUN_COLUMNS} FROM run WHERE run_id = $1 AND {IN_TREE}"), &[&run_id]).await?;
        row.map(run_from).transpose()
    }

    async fn runs(&self, project: uuid::Uuid) -> anyhow::Result<Vec<RunRow>> {
        let rows: Vec<RunTuple> = self
            .pool
            .all(
                &format!(
                    "SELECT {RUN_COLUMNS} FROM run \
                     WHERE project_id = $1 AND spec IS NOT NULL AND {IN_TREE} \
                     ORDER BY started_at ASC, run_id ASC"
                ),
                &[&project],
            )
            .await?;
        rows.into_iter().map(run_from).collect()
    }

    async fn trigger_runs(&self, project: uuid::Uuid) -> anyhow::Result<BTreeMap<String, TriggerRuns>> {
        let rows: Vec<(String, i64, uuid::Uuid)> = self
            .pool
            .all(
                // Postgres has no `max(uuid)`; run ids are v7, so the newest
                // one sorts highest.
                "SELECT source_version, SUM(runs)::bigint, (ARRAY_AGG(last_run ORDER BY last_run DESC))[1] \
                 FROM version_runs WHERE project_id = $1 GROUP BY source_version",
                &[&project],
            )
            .await?;
        Ok(rows.into_iter().map(|(version, runs, last)| (version, TriggerRuns { runs: runs.max(0) as u64, last })).collect())
    }

    async fn set_run_example(
        &self,
        run_id: RunId,
        example: Option<&str>,
    ) -> anyhow::Result<()> {
        let rows = self.pool.execute("UPDATE run SET example = $2 WHERE run_id = $1 AND spec IS NOT NULL", &[&run_id, &example]).await?;
        anyhow::ensure!(rows == 1, "run {run_id} is not a run started by hand");
        Ok(())
    }

    async fn activated_versions(&self, project: uuid::Uuid) -> anyhow::Result<Vec<String>> {
        Ok(self
            .pool
            .scalars(
                "SELECT DISTINCT activation_version FROM trigger_activation \
                 WHERE project_id = $1 AND activation_version IS NOT NULL ORDER BY 1",
                &[&project],
            )
            .await?)
    }
}

/// In-memory store for layer-3 tests: plain maps, no logic.
///
/// It refuses exactly what the schema refuses (a duplicate run, a run
/// under a version that does not exist, a head on an unknown project),
/// because a fake looser than its constraints is how a violation
/// reaches production with the tests green.
#[cfg(any(test, feature = "test-helpers"))]
#[derive(Default)]
pub struct FakeVersionStore {
    versions: std::sync::Mutex<Vec<VersionRow>>,
    runs: std::sync::Mutex<Vec<RunRow>>,
    trigger_runs: std::sync::Mutex<BTreeMap<(uuid::Uuid, String), TriggerRuns>>,
    activated: std::sync::Mutex<BTreeMap<uuid::Uuid, BTreeSet<String>>>,
    /// The projects that exist, standing in for the `project` table.
    projects: std::sync::Mutex<BTreeSet<uuid::Uuid>>,
    /// Versions something still depends on: an armed trigger's settings
    /// came from one, or a live run is running one. A plain set
    /// because that is all this has to be; the real store works it out
    /// from the signal rows and the journal, and a fake that tried to
    /// reproduce THAT would be a second implementation to get wrong.
    ///
    /// It exists because the real `delete_versions` REFUSES these, and a
    /// fake that deletes them happily makes every test of the refusal
    /// pass while production errors. Put the versions a case wants
    /// protected in here with `mark_in_use`.
    in_use: std::sync::Mutex<BTreeSet<String>>,
}

#[cfg(any(test, feature = "test-helpers"))]
impl FakeVersionStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Say that something still depends on this version, so a prune of
    /// it is refused exactly as the real store refuses one.
    pub fn mark_in_use(&self, version: &str) {
        self.in_use.lock().unwrap().insert(version.to_string());
    }

    /// Say that some activation's listeners run `version`, as a
    /// `trigger_activation` row carrying it: `activated_versions` then
    /// lists it (deduplicated, sorted, like the real read).
    pub fn mark_activated(&self, project: uuid::Uuid, version: &str) {
        self.activated.lock().unwrap().entry(project).or_default().insert(version.to_string());
    }

    /// A run started by hand, as its queued `run` row holds it.
    pub fn add_run(&self, run: RunRow) {
        self.runs.lock().unwrap().push(run);
    }

    /// Runs a trigger started on `version`, as the writes of their records
    /// count them.
    pub fn add_trigger_runs(&self, project: uuid::Uuid, version: &str, runs: TriggerRuns) {
        self.trigger_runs.lock().unwrap().insert((project, version.to_string()), runs);
    }

    /// Register a project, as `project` holding a row for it.
    pub fn add_project(&self, project: uuid::Uuid) {
        self.projects.lock().unwrap().insert(project);
    }

    /// Drop the project and its whole tree, as `weft rm` does
    /// (`PostgresProjectStore::remove` deletes every `project_version`
    /// row in the same transaction as the project, and its runs are
    /// erased with it).
    ///
    /// The tree rows used to stay here, and the tests that rode on that
    /// certified a guarantee production does not give. A fake that
    /// erases less than the real store is a fake that hides exactly the
    /// leak it should catch.
    pub fn remove_project(&self, project: uuid::Uuid) {
        self.projects.lock().unwrap().remove(&project);
        self.activated.lock().unwrap().remove(&project);
        self.versions.lock().unwrap().retain(|v| v.project_id != project);
        self.runs.lock().unwrap().retain(|r| r.project_id != project);
    }

    /// A run the tree shows: its version is still in the tree.
    fn in_tree(&self, run: &RunRow) -> bool {
        self.versions.lock().unwrap().iter().any(|v| v.project_id == run.project_id && v.id == run.version_id)
    }

    fn require_project(&self, project: uuid::Uuid) -> anyhow::Result<()> {
        anyhow::ensure!(self.projects.lock().unwrap().contains(&project), "project {project} not found");
        Ok(())
    }
}

#[cfg(any(test, feature = "test-helpers"))]
#[async_trait]
impl VersionStoreOps for FakeVersionStore {
    async fn upsert_version(&self, version: &VersionRow) -> anyhow::Result<bool> {
        let mut versions = self.versions.lock().unwrap();
        if versions.iter().any(|v| v.project_id == version.project_id && v.id == version.id) {
            return Ok(false);
        }
        versions.push(version.clone());
        Ok(true)
    }

    async fn version(&self, project: uuid::Uuid, id: &str) -> anyhow::Result<Option<VersionRow>> {
        Ok(self.versions.lock().unwrap().iter().find(|v| v.project_id == project && v.id == id).cloned())
    }

    async fn versions(&self, project: uuid::Uuid) -> anyhow::Result<Vec<VersionRow>> {
        Ok(self.versions.lock().unwrap().iter().filter(|v| v.project_id == project).cloned().collect())
    }

    async fn set_label(&self, project: uuid::Uuid, id: &str, label: Option<&str>) -> anyhow::Result<()> {
        let mut versions = self.versions.lock().unwrap();
        let row = versions
            .iter_mut()
            .find(|v| v.project_id == project && v.id == id)
            .ok_or_else(|| anyhow::anyhow!("version {id} not found"))?;
        row.label = label.map(str::to_string);
        Ok(())
    }

    async fn delete_versions(&self, project: uuid::Uuid, ids: &[String]) -> anyhow::Result<()> {
        // The same refusal, in the same words, as the real store: a
        // version something still depends on is not deletable. Without
        // it a test could prune a version out from under an armed
        // trigger and see it work.
        let in_use = self.in_use.lock().unwrap();
        anyhow::ensure!(!ids.iter().any(|id| in_use.contains(id)), VERSIONS_IN_USE_REFUSAL);
        drop(in_use);
        self.versions.lock().unwrap().retain(|v| !(v.project_id == project && ids.contains(&v.id)));
        self.trigger_runs.lock().unwrap().retain(|(p, version), _| !(*p == project && ids.contains(version)));
        Ok(())
    }

    async fn versions_in_use(&self, _project: uuid::Uuid, _include_bakes: bool) -> anyhow::Result<BTreeSet<String>> {
        Ok(self.in_use.lock().unwrap().clone())
    }

    async fn registered_source(&self, project: uuid::Uuid) -> anyhow::Result<Option<Manifest>> {
        self.require_project(project)?;
        // No test registers a build's source here; the real column is
        // written by the project store, not the tree.
        Ok(None)
    }

    async fn run(&self, run_id: RunId) -> anyhow::Result<Option<RunRow>> {
        Ok(self.runs.lock().unwrap().iter().find(|r| r.run_id == run_id && self.in_tree(r)).cloned())
    }

    async fn runs(&self, project: uuid::Uuid) -> anyhow::Result<Vec<RunRow>> {
        Ok(self.runs.lock().unwrap().iter().filter(|r| r.project_id == project && r.spec.is_some() && self.in_tree(r)).cloned().collect())
    }

    async fn trigger_runs(&self, project: uuid::Uuid) -> anyhow::Result<BTreeMap<String, TriggerRuns>> {
        Ok(self
            .trigger_runs
            .lock()
            .unwrap()
            .iter()
            .filter(|((p, _), _)| *p == project)
            .map(|((_, version), runs)| (version.clone(), *runs))
            .collect())
    }

    async fn set_run_example(
        &self,
        run_id: RunId,
        example: Option<&str>,
    ) -> anyhow::Result<()> {
        let mut runs = self.runs.lock().unwrap();
        let row = runs
            .iter_mut()
            .find(|r| r.run_id == run_id && r.spec.is_some())
            .ok_or_else(|| anyhow::anyhow!("run {run_id} is not a run started by hand"))?;
        row.example = example.map(str::to_string);
        Ok(())
    }

    async fn activated_versions(&self, project: uuid::Uuid) -> anyhow::Result<Vec<String>> {
        self.require_project(project)?;
        Ok(self.activated.lock().unwrap().get(&project).map(|set| set.iter().cloned().collect()).unwrap_or_default())
    }
}

// ----- The decisions, as pure functions over the rows -----------------

/// What a run's `--seed` resolves to: the run to inherit from, or why
/// none.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SeedChoice {
    /// Inherit from this run.
    Run(RunId),
    /// Nothing to inherit: no head, `--root`, or no settled run
    /// anywhere on head's ancestry.
    Nothing,
    /// Head's run is still running; nothing is cancelled on the user's
    /// behalf. The message names `weft stop <run_id>`.
    HeadRunning(RunId),
}

/// Pick the seed: head's run, or when head has no run the newest
/// settled run on head's version or its nearest ancestor with one.
/// `settled` answers whether a run is terminal or suspended (a
/// running run is never a seed).
pub fn resolve_seed(
    head: &Head,
    versions: &[VersionRow],
    runs: &[RunRow],
    settled: impl Fn(RunId) -> bool,
) -> SeedChoice {
    if let Some(run) = head.head_run {
        return if settled(run) { SeedChoice::Run(run) } else { SeedChoice::HeadRunning(run) };
    }
    let Some(mut version) = head.head_version.clone() else {
        return SeedChoice::Nothing;
    };
    let by_id: BTreeMap<&str, &VersionRow> = versions.iter().map(|v| (v.id.as_str(), v)).collect();
    let mut seen: HashSet<String> = HashSet::new();
    loop {
        if !seen.insert(version.clone()) {
            return SeedChoice::Nothing;
        }
        // Newest first: the run that best describes the version.
        let newest_settled = runs
            .iter()
            .filter(|r| r.version_id == version)
            .rev()
            .find(|r| settled(r.run_id));
        if let Some(run) = newest_settled {
            return SeedChoice::Run(run.run_id);
        }
        match by_id.get(version.as_str()).and_then(|v| v.parent_id.clone()) {
            Some(parent) => version = parent,
            None => return SeedChoice::Nothing,
        }
    }
}

/// Every version in the subtree rooted at `root`, `root` included,
/// parents before children.
pub fn subtree(versions: &[VersionRow], root: &str) -> Vec<String> {
    let mut out = vec![root.to_string()];
    let mut i = 0;
    while i < out.len() {
        let parent = out[i].clone();
        for v in versions.iter().filter(|v| v.parent_id.as_deref() == Some(parent.as_str())) {
            if !out.contains(&v.id) {
                out.push(v.id.clone());
            }
        }
        i += 1;
    }
    out
}

/// Every ancestor of `version`, nearest first, `version` excluded.
pub fn ancestors(versions: &[VersionRow], version: &str) -> Vec<String> {
    let by_id: BTreeMap<&str, &VersionRow> = versions.iter().map(|v| (v.id.as_str(), v)).collect();
    let mut out = Vec::new();
    let mut cur = by_id.get(version).and_then(|v| v.parent_id.clone());
    while let Some(id) = cur {
        if out.contains(&id) {
            break;
        }
        cur = by_id.get(id.as_str()).and_then(|v| v.parent_id.clone());
        out.push(id);
    }
    out
}

/// Why a prune is refused, each naming the verb that fixes it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PruneRefusal {
    pub reasons: Vec<String>,
}


/// What keeps a version from a prune and a sweep: every working copy
/// (its head's version, and the files it holds now, which a prune never
/// reclaims), every version some activation's listeners run, and the
/// version of the build the project runs (a run the install starts itself,
/// a setup run, is recorded under it).
#[derive(Debug, Clone, Copy)]
pub struct Kept<'a> {
    pub copies: &'a [WorkingCopyRow],
    pub activated: &'a [String],
    pub running: Option<&'a str>,
}

impl Kept<'_> {
    fn is_head(&self, version: &str) -> bool {
        self.copies.iter().any(|c| c.head.head_version.as_deref() == Some(version))
    }
}

/// How a reason names a working copy: its folder, or the program copy.
pub fn copy_name(copy: &WorkingCopyRow) -> String {
    match &copy.path {
        Some(path) => format!("the working copy at {path}"),
        None => "the program copy".to_string(),
    }
}

/// Plan a prune, or refuse it: a working copy's head inside the subtree
/// (switch it away first), an activated version (deactivate first), a
/// version that is or is an ancestor of a frozen example's origin
/// (unfreeze the example first), or a subtree with a run still in flight
/// (stop or wait). `frozen_from` is every `frozen_from.version` in the
/// project's examples; `in_flight` says whether a run is neither terminal
/// nor suspended. A blob any working copy holds now is never in the plan.
pub fn plan_prune(
    root: &str,
    versions: &[VersionRow],
    runs: &[RunRow],
    kept: Kept<'_>,
    frozen_from: &[String],
    in_flight: impl Fn(RunId) -> bool,
) -> Result<PrunePlan, PruneRefusal> {
    let mut reasons = Vec::new();
    if !versions.iter().any(|v| v.id == root) {
        return Err(PruneRefusal { reasons: vec![format!("no version {root} in this project")] });
    }
    let doomed = subtree(versions, root);
    for copy in kept.copies {
        if let Some(head) = copy.head.head_version.as_deref().filter(|h| doomed.iter().any(|d| d == h)) {
            let named = copy.path.as_deref().unwrap_or("program");
            reasons.push(format!(
                "{} is on version {head}, inside the subtree; move it away first (`weft branch <version> --copy {named}`), \
                 or, if nothing uses it any more, `weft forget-copy {named}`",
                copy_name(copy)
            ));
        }
    }
    if let Some(active) = kept.activated.iter().find(|a| doomed.contains(a)) {
        reasons.push(format!("version {active} is activated and inside the subtree; `weft deactivate` the triggers on it first"));
    }
    if let Some(running) = kept.running.filter(|r| doomed.iter().any(|d| d == r)) {
        reasons.push(format!("version {running} is the one the project's build runs, inside the subtree; build another version first"));
    }
    for origin in frozen_from {
        let protected: Vec<String> = std::iter::once(origin.clone()).chain(ancestors(versions, origin)).collect();
        if let Some(hit) = doomed.iter().find(|d| protected.contains(d)) {
            reasons.push(format!(
                "version {hit} is (or is an ancestor of) the origin {origin} of a frozen example; \
                 remove the example's `frozenFrom` (`weft freeze` again from another run) first"
            ));
        }
    }
    let doomed_runs: Vec<&RunRow> = runs.iter().filter(|r| doomed.contains(&r.version_id)).collect();
    for run in &doomed_runs {
        if in_flight(run.run_id) {
            reasons.push(format!("run {} is still running; `weft stop {}` or wait", run.run_id, run.run_id));
        }
    }
    if !reasons.is_empty() {
        return Err(PruneRefusal { reasons });
    }
    let surviving: BTreeSet<&str> = versions
        .iter()
        .filter(|v| !doomed.contains(&v.id))
        .flat_map(|v| v.manifest.values().map(String::as_str))
        .chain(kept.copies.iter().flat_map(|c| c.working.values().map(String::as_str)))
        .collect();
    let mut blobs: BTreeSet<String> = BTreeSet::new();
    for v in versions.iter().filter(|v| doomed.contains(&v.id)) {
        for hash in v.manifest.values() {
            if !surviving.contains(hash.as_str()) {
                blobs.insert(hash.clone());
            }
        }
    }
    Ok(PrunePlan {
        versions: doomed,
        runs: doomed_runs.iter().map(|r| r.run_id).collect(),
        blobs: blobs.into_iter().collect(),
    })
}

/// The versions a bulk clean may drop after their runs went: no runs
/// left, no descendants, no label, not any working copy's head and not
/// activated.
pub fn sweepable_versions(versions: &[VersionRow], runs: &[RunRow], kept: Kept<'_>) -> Vec<String> {
    versions
        .iter()
        .filter(|v| v.label.is_none())
        .filter(|v| !kept.is_head(&v.id))
        .filter(|v| !kept.activated.contains(&v.id))
        .filter(|v| kept.running != Some(v.id.as_str()))
        .filter(|v| !runs.iter().any(|r| r.version_id == v.id))
        .filter(|v| !versions.iter().any(|c| c.parent_id.as_deref() == Some(v.id.as_str())))
        .map(|v| v.id.clone())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run_id(n: u8) -> RunId {
        RunId::from_bytes([n; 16])
    }

    fn version(id: &str, parent: Option<&str>, files: &[(&str, &str)], at: u64) -> VersionRow {
        VersionRow {
            id: id.into(),
            project_id: uuid::Uuid::nil(),
            parent_id: parent.map(str::to_string),
            manifest: files.iter().map(|(p, h)| (p.to_string(), h.to_string())).collect(),
            label: None,
            created_at: at,
        }
    }

    fn run(n: u8, version: &str, at: u64) -> RunRow {
        RunRow {
            run_id: run_id(n),
            project_id: uuid::Uuid::nil(),
            version_id: version.into(),
            seed_run_id: None,
            stale: vec![],
            spec: Some(RunSpec::whole("run")),
            definition_hash: "d".into(),
            example: None,
            created_at: at,
        }
    }

    #[test]
    fn identical_manifests_are_one_version() {
        let a: Manifest = [("main.weft", "1"), ("weft.toml", "2")].iter().map(|(p, h)| (p.to_string(), h.to_string())).collect();
        let b: Manifest = [("weft.toml", "2"), ("main.weft", "1")].iter().map(|(p, h)| (p.to_string(), h.to_string())).collect();
        assert_eq!(version_id(&a), version_id(&b));
        let mut c = a.clone();
        c.insert("main.weft".into(), "3".into());
        assert_ne!(version_id(&a), version_id(&c));
    }

    #[test]
    fn the_seed_is_heads_run_when_settled_and_a_refusal_when_running() {
        let head = Head { head_version: Some("v1".into()), head_run: Some(run_id(1)) };
        assert_eq!(resolve_seed(&head, &[], &[], |_| true), SeedChoice::Run(run_id(1)));
        assert_eq!(resolve_seed(&head, &[], &[], |_| false), SeedChoice::HeadRunning(run_id(1)));
    }

    #[test]
    fn without_a_head_run_the_nearest_ancestor_with_a_settled_run_seeds() {
        let versions = vec![version("v1", None, &[], 1), version("v2", Some("v1"), &[], 2), version("v3", Some("v2"), &[], 3)];
        let runs = vec![run(1, "v1", 1), run(2, "v1", 2), run(3, "v2", 3)];
        let head = Head { head_version: Some("v3".into()), head_run: None };
        assert_eq!(resolve_seed(&head, &versions, &runs, |_| true), SeedChoice::Run(run_id(3)));
        // v2's run still running: skip to v1's newest settled.
        assert_eq!(resolve_seed(&head, &versions, &runs, |c| c != run_id(3)), SeedChoice::Run(run_id(2)));
        let head = Head { head_version: None, head_run: None };
        assert_eq!(resolve_seed(&head, &versions, &runs, |_| true), SeedChoice::Nothing);
    }

    /// A working copy whose head is on `version`, holding `files`.
    fn copy_on(version: &str, files: &[(&str, &str)]) -> WorkingCopyRow {
        WorkingCopyRow {
            id: uuid::Uuid::now_v7(),
            project_id: uuid::Uuid::nil(),
            kind: weft_core::working_copy::WorkingCopyKind::Folder,
            host: Some("box".into()),
            path: Some("/home/me/shop".into()),
            head: Head { head_version: Some(version.into()), head_run: None },
            working: files.iter().map(|(p, h)| (p.to_string(), h.to_string())).collect(),
            synced: Manifest::new(),
            conflicts: Vec::new(),
            revision: 0,
            created_at: 0,
            updated_at: 0,
        }
    }

    fn kept<'a>(copies: &'a [WorkingCopyRow], activated: &'a [String]) -> Kept<'a> {
        Kept { copies, activated, running: None }
    }

    /// The version the project's build runs is neither pruned nor swept.
    #[test]
    fn the_running_builds_version_is_kept() {
        let copies = [copy_on("side", &[])];
        let kept = Kept { copies: &copies, activated: &[], running: Some("v3") };
        let err = plan_prune("v2", &tree(), &[], kept, &[], |_| false).unwrap_err();
        assert!(err.reasons[0].contains("build another version first"), "{err:?}");
        assert!(sweepable_versions(&tree(), &[], kept).is_empty(), "v3, the only bare leaf, is the running build's");
    }

    fn tree() -> Vec<VersionRow> {
        vec![
            version("root", None, &[("a", "1")], 1),
            version("v2", Some("root"), &[("a", "2")], 2),
            version("v3", Some("v2"), &[("a", "3")], 3),
            version("side", Some("root"), &[("a", "1"), ("b", "9")], 4),
        ]
    }

    #[test]
    fn subtree_and_ancestors_walk_the_tree() {
        assert_eq!(subtree(&tree(), "v2"), vec!["v2", "v3"]);
        assert_eq!(subtree(&tree(), "root").len(), 4);
        assert_eq!(ancestors(&tree(), "v3"), vec!["v2", "root"]);
        assert!(ancestors(&tree(), "root").is_empty());
    }

    #[test]
    fn prune_refuses_any_head_a_frozen_ancestor_and_an_in_flight_subtree() {
        let runs = vec![run(1, "v2", 1), run(2, "v3", 2)];
        // Any working copy's head inside the subtree stops it, not only
        // one of them.
        let on_head = [copy_on("side", &[]), copy_on("v3", &[])];
        let err = plan_prune("v2", &tree(), &runs, kept(&on_head, &[]), &[], |_| false).unwrap_err();
        assert_eq!(err.reasons.len(), 1, "{err:?}");
        assert!(err.reasons[0].contains("/home/me/shop is on version v3") && err.reasons[0].contains("weft branch <version> --copy /home/me/shop") && err.reasons[0].contains("weft forget-copy /home/me/shop"), "{err:?}");
        let away = [copy_on("side", &[])];
        let err = plan_prune("v2", &tree(), &runs, kept(&away, &[]), &["v3".into()], |_| false).unwrap_err();
        assert!(err.reasons[0].contains("frozen example"), "{:?}", err);
        let err = plan_prune("v2", &tree(), &runs, kept(&away, &[]), &[], |c| c == run_id(2)).unwrap_err();
        assert!(err.reasons[0].contains("weft stop"), "{:?}", err);
        assert!(plan_prune("nope", &tree(), &runs, kept(&away, &[]), &[], |_| false).is_err());
    }

    /// Any activation's version protects its subtree and keeps a bare
    /// leaf from a bulk clean: one instance's trigger on an old version
    /// counts as much as the shared triggers on the newest.
    #[test]
    fn every_activated_version_is_protected() {
        let copies = [copy_on("side", &[])];
        let activated = ["root".to_string(), "v3".to_string()];
        let err = plan_prune("v2", &tree(), &[], kept(&copies, &activated), &[], |_| false).unwrap_err();
        assert!(err.reasons[0].contains("version v3 is activated"), "{:?}", err);
        assert!(sweepable_versions(&tree(), &[], kept(&copies, &activated)).is_empty(), "v3 is the only bare leaf, and it is activated");
    }

    #[test]
    fn prune_removes_the_subtree_its_runs_and_the_orphaned_blobs() {
        let runs = vec![run(1, "v2", 1), run(2, "v3", 2), run(3, "side", 3)];
        let away = [copy_on("side", &[])];
        let plan = plan_prune("v2", &tree(), &runs, kept(&away, &[]), &[], |_| false).unwrap();
        assert_eq!(plan.versions, vec!["v2", "v3"]);
        assert_eq!(plan.runs, vec![run_id(1), run_id(2)]);
        assert_eq!(plan.blobs, vec!["2", "3"], "blob 1 survives on root and side");
        // A blob a working copy holds now is never reclaimed, whatever
        // version it came from.
        let holding = [copy_on("side", &[("a", "3")])];
        let plan = plan_prune("v2", &tree(), &runs, kept(&holding, &[]), &[], |_| false).unwrap();
        assert_eq!(plan.blobs, vec!["2"], "blob 3 is in a working copy");
    }

    #[test]
    fn a_bulk_clean_sweeps_only_bare_leaves() {
        let mut versions = tree();
        versions[3].label = Some("keep".into());
        let runs = vec![run(2, "v3", 2)];
        let copies = [copy_on("root", &[])];
        // v2 has a descendant, v3 has a run, side is labelled, root is head.
        assert!(sweepable_versions(&versions, &runs, kept(&copies, &[])).is_empty());
        assert_eq!(sweepable_versions(&versions, &[], kept(&copies, &[])), vec!["v3"]);
        // Every working copy's head is kept, not only one.
        let two = [copy_on("root", &[]), copy_on("v3", &[])];
        assert!(sweepable_versions(&versions, &[], kept(&two, &[])).is_empty());
    }

    /// A version some activation runs is kept by the sweep and refused by
    /// a prune, read through the store's own `activated_versions` the way
    /// both endpoints read it.
    #[tokio::test]
    async fn an_activated_version_is_neither_swept_nor_pruned() {
        let store = FakeVersionStore::new();
        let project = uuid::Uuid::nil();
        store.add_project(project);
        store.upsert_version(&version("root", None, &[("main.weft", "1")], 1)).await.unwrap();
        store.upsert_version(&version("armed", Some("root"), &[("main.weft", "2")], 2)).await.unwrap();
        store.upsert_version(&version("bare", Some("root"), &[("main.weft", "3")], 3)).await.unwrap();
        store.mark_activated(project, "armed");

        let activated = store.activated_versions(project).await.unwrap();
        assert_eq!(activated, vec!["armed"]);
        let versions = store.versions(project).await.unwrap();
        assert_eq!(sweepable_versions(&versions, &[], kept(&[], &activated)), vec!["bare"], "the activated leaf stays");
        let refusal = plan_prune("armed", &versions, &[], kept(&[], &activated), &[], |_| false).expect_err("activated");
        assert_eq!(refusal.reasons.len(), 1);
        assert!(refusal.reasons[0].contains("version armed is activated"), "{:?}", refusal.reasons);
    }

    /// `weft rm` takes the whole tree with the project, in the same
    /// transaction, its runs erased just before: nothing of a removed
    /// project is left.
    #[tokio::test]
    async fn removing_a_project_takes_its_whole_tree() {
        let store = FakeVersionStore::new();
        let project = uuid::Uuid::nil();
        store.add_project(project);
        store.upsert_version(&version("ran", None, &[("main.weft", "1")], 1)).await.unwrap();
        store.upsert_version(&version("never-ran", Some("ran"), &[("main.weft", "2")], 2)).await.unwrap();
        store.add_run(run(1, "ran", 3));

        store.remove_project(project);
        assert!(store.versions(project).await.unwrap().is_empty(), "the tree went with it");
        assert!(store.runs(project).await.unwrap().is_empty(), "and the runs with it");
    }
}
