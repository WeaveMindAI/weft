//! The version tree: what code a project has been, and what ran on it.
//!
//! Two kinds of node, one tree. A **version** is the project's files,
//! content-addressed: `id = sha256(canonical manifest)` where the
//! manifest maps every covered path to the sha256 of its bytes, so
//! identical code is one row however often it is run, and a version
//! points at the version it was edited from (`parent_id`; a tree root
//! has none). A **run** is one color under a version, with the run it
//! was seeded from (`seed_color`), the stale set of that seed edge, and
//! the spec it ran. The files themselves live in the project's asset
//! plane, published through `weft_assets::publish_files` under
//! `asset/<project>/<sha>`: a blob identical to one any earlier version
//! held costs nothing to record again, and `weft prune` reclaims what no
//! surviving manifest names.
//!
//! Head is three nullable columns on the `project` row (`head_version`,
//! `head_run`, `activation_version`), git's HEAD and nothing more: moved
//! by `checkpoint`, `run`, `branch`, and `activate`. No state on disk.
//!
//! Every write goes through [`VersionStoreOps`]; the Postgres store
//! runs SQL, the mock keeps maps, and the decisions (which run seeds
//! the next one, what a prune removes, what changed between two
//! versions) are pure functions over the rows so they are tested at
//! layer 1 and the handlers only gather and dispatch.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use weft_core::run_spec::RunSpec;
use weft_core::Color;

/// Postgres SQLSTATE for a foreign key violation.
const FOREIGN_KEY_VIOLATION: &str = "23503";

/// The `project_version` + `version_run` tables. Canonical DDL edited in
/// place; an existing database is carried forward by `./setup.sh
/// --migration <name>` (the contract is `weft_task_store::schema_guard`'s
/// header). Applied by the boot's `apply_core_schema` after
/// `project_store::GROUP`, which owns the `project` row both tables
/// hang off (and the head columns).
pub static GROUP: weft_task_store::SchemaGroup = weft_task_store::SchemaGroup {
    name: "versions",
    tables: &["project_version", "version_run"],
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
            -- project that no longer exists. The journal's executions go
            -- the same way, at the same moment, so a removal leaves the
            -- person their files and nothing else.
            -- `retire_unused_versions` is the backstop for a removal that
            -- failed halfway, not a step of the ordinary one.
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
        // version_run: one row per color started on a version. `spec`
        // is the run spec as resolved (NULL for a plain whole-graph run
        // or a real event's run); `example` names its saved example,
        // if any. Removed by `weft clean` through
        // `VersionStoreOps::delete_run` (this table is the version
        // store's; the journal owns the journal) and with its version
        // by prune's cascade.
        r#"CREATE TABLE IF NOT EXISTS version_run (
            color            UUID PRIMARY KEY,
            project_id       UUID NOT NULL,
            version_id       TEXT NOT NULL,
            seed_color       UUID,
            stale            TEXT[] NOT NULL DEFAULT '{}',
            spec             JSONB,
            definition_hash  TEXT NOT NULL,
            example          TEXT,
            created_at       BIGINT NOT NULL,
            -- Recording order: what "newest run" means (the seed a
            -- bare head resolves to). Seconds tie, this never does.
            seq              BIGSERIAL,
            FOREIGN KEY (project_id, version_id) REFERENCES project_version(project_id, id) ON DELETE CASCADE
        )"#,
        "CREATE INDEX IF NOT EXISTS idx_version_run_version ON version_run(project_id, version_id)",
    ],
    seed: &[],
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

/// The tree row a run would hang off is gone: the project was removed
/// (`weft rm`) or the version pruned, after the run's birth reached the
/// journal. Both stores answer an `insert_run` for such a run with this,
/// so a reader can tell "nothing to record in" from a storage failure.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("version {version} of project {project} is not in the tree (the project was removed, or the version pruned)")]
pub struct VersionMissing {
    pub project: uuid::Uuid,
    pub version: String,
}

// The database row. NOT a wire type: `tree` answers `RunSummary`
// (`api/versions.rs`), which is what the CLI and the editor read.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
/// One recorded run.
///
/// `seed_color`, `stale`, `definition_hash` and `created_at` are also
/// on that color's birth row in the journal. That is deliberate, not
/// drift: they are written once, in the same handler, from the same
/// values, and are never updated afterwards, and they exist here so
/// `weft tree` can draw the whole tree without opening one journal per
/// run. The journal stays authoritative (`weft_journal::seed_chain`
/// walks the birth rows, never these columns); these are a display
/// copy. If a reader ever needs to DECIDE something from them, read the
/// journal instead.
pub struct RunRow {
    pub color: Color,
    pub project_id: uuid::Uuid,
    pub version_id: String,
    pub seed_color: Option<Color>,
    pub stale: Vec<String>,
    pub spec: Option<RunSpec>,
    pub definition_hash: String,
    pub example: Option<String>,
    pub created_at: u64,
}

/// The project's head: where the next version parents and the next
/// seed comes from, plus the version the triggers are activated on.
// SYNC: Head <-> crates/weft-cli/src/commands/versions.rs Head, extension-vscode/src/sidebar/version-tree.ts TreeJson.head
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Head {
    pub head_version: Option<String>,
    pub head_run: Option<Color>,
    pub activation_version: Option<String>,
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
    /// Delete the named versions and, by cascade, their runs. The
    /// caller has already removed the runs' journals.
    async fn delete_versions(&self, project: uuid::Uuid, ids: &[String]) -> anyhow::Result<()>;

    /// Once the project row is gone, drop every tree row no surviving run
    /// needs, and answer how many went.
    ///
    /// `known` is the set of colors the JOURNAL still holds for this
    /// project. A run row outside it is a row nothing can ever read: the
    /// journal never had it (a start that failed between recording the run
    /// and journaling it) or no longer does. Those go first, because
    /// nothing else can reach them once the project row is gone: `weft rm`
    /// refuses a project it cannot find, and `weft clean` enumerates
    /// journal colors, so such a row used to pin its version for ever and
    /// the sweep reported the project every hour while dropping nothing.
    ///
    /// A version is then kept when a surviving run is recorded under it OR
    /// when a kept version descends from it: a run's place in the tree is
    /// its lineage, and deleting an ancestor would leave the survivor
    /// parented on a row that is gone.
    ///
    /// The mirror of `ProjectStore::retire_unused_definitions`, and for
    /// the same reason: a run outlives its project, so what describes that
    /// run has to outlive it too, while the versions nothing points at any
    /// more are of no use to anybody. Refuses to do anything while the
    /// project still exists, so a live project's tree can never be
    /// swept by this path.
    async fn retire_unused_versions(&self, project: uuid::Uuid, known: &[Color]) -> anyhow::Result<u64>;

    /// Every project id that still has tree rows while the project itself
    /// is gone. What the reaper sweeps; see
    /// `ProjectStore::projects_with_orphan_definitions` for why one
    /// best-effort retirement is not enough.
    async fn projects_with_orphan_versions(&self) -> anyhow::Result<Vec<uuid::Uuid>>;

    async fn insert_run(&self, run: &RunRow) -> anyhow::Result<()>;
    async fn run(&self, color: Color) -> anyhow::Result<Option<RunRow>>;
    /// Every run of the project, in the order they were recorded.
    async fn runs(&self, project: uuid::Uuid) -> anyhow::Result<Vec<RunRow>>;
    /// Set or clear the saved example associated with one recorded run.
    async fn set_run_example(
        &self,
        color: Color,
        example: Option<&str>,
    ) -> anyhow::Result<()>;

    async fn head(&self, project: uuid::Uuid) -> anyhow::Result<Head>;
    /// Drop one run's row, and clear head's run pointer if it named it.
    ///
    /// The version tree's own table is the version store's to write.
    /// This used to live inside the journal's Postgres `delete_execution`,
    /// which meant the journal wrote two tables belonging to two other
    /// stores, and the in-memory journal could not model it at all: every
    /// test of `weft clean` and of prune ran against a world where the run
    /// row outlived its journal and head still pointed at a deleted run,
    /// while production deleted both.
    async fn delete_run(&self, color: Color) -> anyhow::Result<()>;

    /// Move head, but only if it is still where `expected` says.
    ///
    /// Every caller decided WHERE to move head by first reading it (a
    /// checkpoint parents its version on head, a run seeds from head's
    /// run), so a blind write is a lost update: two Pods reading head A
    /// both parent on A and the second overwrites the first, and that
    /// run's version and color vanish from the lineage the next seeded
    /// run walks. Answers false when head moved underneath.
    ///
    /// What the handler does with a false is its own call: `checkpoint`
    /// and `weft branch` refuse (nothing has happened yet, so "try
    /// again" is honest), while `run` reports it and lets the run stand,
    /// because by the time head moves the execution is already queued
    /// and refusing would hide a live run from the person who started
    /// it.
    async fn move_head(
        &self,
        project: uuid::Uuid,
        expected: &Head,
        version: Option<&str>,
        run: Option<Color>,
    ) -> anyhow::Result<bool>;
}

pub type VersionStore = Arc<dyn VersionStoreOps>;

pub struct PostgresVersionStore {
    pool: PgPool,
}

impl PostgresVersionStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

/// Source versions needed by armed settings or executions not yet safely
/// represented in the tree. Automatic sweeps also retain completed bakes.
pub(crate) async fn source_versions_in_use(
    conn: &mut sqlx::PgConnection,
    project: uuid::Uuid,
    include_bakes: bool,
) -> anyhow::Result<BTreeSet<String>> {
    let versions: Vec<Option<String>> = sqlx::query_scalar(
        "SELECT source_version FROM signal WHERE project_id = $1::uuid::text \
         UNION SELECT e.payload_json::jsonb ->> 'source_version' FROM exec_event e \
         WHERE e.kind = 'execution_started' AND e.payload_json::jsonb ->> 'project_id' = $1::uuid::text \
           AND (EXISTS (SELECT 1 FROM trigger_setup s WHERE s.color = e.color) \
             OR NOT EXISTS (SELECT 1 FROM exec_event t WHERE t.color = e.color \
                 AND t.kind IN ('execution_completed', 'execution_failed', 'execution_cancelled')) \
             OR (e.payload_json::jsonb ->> 'phase' = 'fire' AND e.payload_json::jsonb ->> 'node_test' IS DISTINCT FROM 'true' \
                 AND NOT EXISTS (SELECT 1 FROM version_run r WHERE r.color::text = e.color))) \
         UNION SELECT bake_json::jsonb ->> 'source_version' FROM trigger_bake WHERE project_id = $1::uuid::text AND $2"
    ).bind(project).bind(include_bakes).fetch_all(conn).await?;
    Ok(versions.into_iter().flatten().collect())
}

pub(crate) async fn retain_source_version(conn: &mut sqlx::PgConnection, project: &str, version: &str) -> anyhow::Result<()> {
    let found: Option<String> = sqlx::query_scalar(
        "SELECT id FROM project_version WHERE project_id = $1::uuid AND id = $2 FOR KEY SHARE"
    ).bind(project).bind(version).fetch_optional(conn).await?;
    anyhow::ensure!(found.is_some(), "source version {version} was removed during preparation; run the command again");
    Ok(())
}

type VersionTuple = (String, uuid::Uuid, Option<String>, serde_json::Value, Option<String>, i64);
type RunTuple = (uuid::Uuid, uuid::Uuid, String, Option<uuid::Uuid>, Vec<String>, Option<serde_json::Value>, String, Option<String>, i64);

fn version_from(row: VersionTuple) -> anyhow::Result<VersionRow> {
    let (id, project_id, parent_id, manifest, label, created_at) = row;
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
    let (color, project_id, version_id, seed_color, stale, spec, definition_hash, example, created_at) = row;
    Ok(RunRow {
        color,
        project_id,
        version_id,
        seed_color,
        stale,
        spec: spec.map(serde_json::from_value).transpose()?,
        definition_hash,
        example,
        created_at: created_at as u64,
    })
}

const VERSION_COLUMNS: &str = "id, project_id, parent_id, manifest, label, created_at";
const RUN_COLUMNS: &str =
    "color, project_id, version_id, seed_color, stale, spec, definition_hash, example, created_at";

#[async_trait]
impl VersionStoreOps for PostgresVersionStore {
    async fn upsert_version(&self, version: &VersionRow) -> anyhow::Result<bool> {
        let rows = sqlx::query(
            "INSERT INTO project_version (id, project_id, parent_id, manifest, label, created_at) \
             VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (project_id, id) DO NOTHING",
        )
        .bind(&version.id)
        .bind(version.project_id)
        .bind(&version.parent_id)
        .bind(serde_json::to_value(&version.manifest)?)
        .bind(&version.label)
        .bind(version.created_at as i64)
        .execute(&self.pool)
        .await?;
        Ok(rows.rows_affected() == 1)
    }

    async fn version(&self, project: uuid::Uuid, id: &str) -> anyhow::Result<Option<VersionRow>> {
        let row: Option<VersionTuple> = sqlx::query_as(&format!(
            "SELECT {VERSION_COLUMNS} FROM project_version WHERE project_id = $1 AND id = $2"
        ))
        .bind(project)
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(version_from).transpose()
    }

    async fn versions(&self, project: uuid::Uuid) -> anyhow::Result<Vec<VersionRow>> {
        let rows: Vec<VersionTuple> = sqlx::query_as(&format!(
            "SELECT {VERSION_COLUMNS} FROM project_version WHERE project_id = $1 ORDER BY seq ASC"
        ))
        .bind(project)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(version_from).collect()
    }

    async fn set_label(&self, project: uuid::Uuid, id: &str, label: Option<&str>) -> anyhow::Result<()> {
        let rows = sqlx::query("UPDATE project_version SET label = $3 WHERE project_id = $1 AND id = $2")
            .bind(project)
            .bind(id)
            .bind(label)
            .execute(&self.pool)
            .await?;
        anyhow::ensure!(rows.rows_affected() == 1, "version {id} not found");
        Ok(())
    }

    async fn delete_versions(&self, project: uuid::Uuid, ids: &[String]) -> anyhow::Result<()> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("SELECT id FROM project_version WHERE project_id = $1 AND id = ANY($2) ORDER BY id FOR UPDATE")
            .bind(project).bind(ids).execute(&mut *tx).await?;
        let protected = source_versions_in_use(&mut tx, project, false).await?;
        anyhow::ensure!(!ids.iter().any(|id| protected.contains(id)),
            "these versions still supply trigger settings or running executions; stop the runs and replace or wipe those trigger settings before pruning");
        sqlx::query("DELETE FROM trigger_bake WHERE project_id = $1::uuid::text AND bake_json::jsonb ->> 'source_version' = ANY($2)")
            .bind(project).bind(ids).execute(&mut *tx).await?;
        sqlx::query("DELETE FROM project_version WHERE project_id = $1 AND id = ANY($2)")
            .bind(project)
            .bind(ids)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn projects_with_orphan_versions(&self) -> anyhow::Result<Vec<uuid::Uuid>> {
        let rows: Vec<(uuid::Uuid,)> = sqlx::query_as(
            "SELECT DISTINCT pv.project_id FROM project_version pv \
             WHERE NOT EXISTS (SELECT 1 FROM project p WHERE p.id = pv.project_id)",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|(id,)| id).collect())
    }

    async fn retire_unused_versions(&self, project: uuid::Uuid, known: &[Color]) -> anyhow::Result<u64> {
        // Nothing at all while the project row is there. Checked in the
        // same statement as each DELETE, so a project registered again in
        // between cannot lose its history.
        let live: Option<uuid::Uuid> =
            sqlx::query_scalar("SELECT id FROM project WHERE id = $1").bind(project).fetch_optional(&self.pool).await?;
        if live.is_some() {
            return Ok(0);
        }
        let known: Vec<uuid::Uuid> = known.to_vec();
        // First the run rows the journal has never heard of.
        let runs = sqlx::query(
            "DELETE FROM version_run vr \
             WHERE vr.project_id = $1 \
               AND NOT (vr.color = ANY($2)) \
               AND NOT EXISTS (SELECT 1 FROM project p WHERE p.id = $1)",
        )
        .bind(project)
        .bind(&known)
        .execute(&self.pool)
        .await?;
        // Then the versions nothing needs. `version_run` cascades off
        // `project_version`, so a version with a run must never be in this
        // DELETE; and a version a KEPT version descends from must not be
        // either, or the survivor's `parent_id` points at a row that is
        // gone (nothing rejects that, there is no foreign key on it).
        let versions = sqlx::query(
            "WITH RECURSIVE kept AS ( \
                 SELECT pv.id FROM project_version pv \
                 WHERE pv.project_id = $1 \
                   AND EXISTS (SELECT 1 FROM version_run vr \
                               WHERE vr.project_id = pv.project_id AND vr.version_id = pv.id) \
                 UNION \
                 SELECT parent.id FROM project_version parent \
                 JOIN project_version child ON child.parent_id = parent.id \
                                           AND child.project_id = parent.project_id \
                 JOIN kept ON kept.id = child.id \
                 WHERE parent.project_id = $1 \
             ) \
             DELETE FROM project_version pv \
             WHERE pv.project_id = $1 \
               AND NOT EXISTS (SELECT 1 FROM kept WHERE kept.id = pv.id) \
               AND NOT EXISTS (SELECT 1 FROM project p WHERE p.id = $1)",
        )
        .bind(project)
        .execute(&self.pool)
        .await?;
        Ok(runs.rows_affected() + versions.rows_affected())
    }

    async fn insert_run(&self, run: &RunRow) -> anyhow::Result<()> {
        sqlx::query(&format!(
            "INSERT INTO version_run ({RUN_COLUMNS}) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"
        ))
        .bind(run.color)
        .bind(run.project_id)
        .bind(&run.version_id)
        .bind(run.seed_color)
        .bind(&run.stale)
        .bind(run.spec.as_ref().map(serde_json::to_value).transpose()?)
        .bind(&run.definition_hash)
        .bind(&run.example)
        .bind(run.created_at as i64)
        .execute(&self.pool)
        .await
        .map_err(|error| match &error {
            // The composite foreign key onto `project_version`: the only
            // constraint this insert can trip besides its primary key.
            sqlx::Error::Database(db) if db.code().as_deref() == Some(FOREIGN_KEY_VIOLATION) => {
                anyhow::Error::new(VersionMissing { project: run.project_id, version: run.version_id.clone() })
            }
            _ => anyhow::Error::new(error),
        })?;
        Ok(())
    }

    async fn run(&self, color: Color) -> anyhow::Result<Option<RunRow>> {
        let row: Option<RunTuple> =
            sqlx::query_as(&format!("SELECT {RUN_COLUMNS} FROM version_run WHERE color = $1"))
                .bind(color)
                .fetch_optional(&self.pool)
                .await?;
        row.map(run_from).transpose()
    }

    async fn runs(&self, project: uuid::Uuid) -> anyhow::Result<Vec<RunRow>> {
        let rows: Vec<RunTuple> = sqlx::query_as(&format!(
            "SELECT {RUN_COLUMNS} FROM version_run WHERE project_id = $1 ORDER BY seq ASC"
        ))
        .bind(project)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(run_from).collect()
    }

    async fn delete_run(&self, color: Color) -> anyhow::Result<()> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("DELETE FROM version_run WHERE color = $1").bind(color).execute(&mut *tx).await?;
        // A project whose head pointed at this run keeps its version and
        // loses the run pointer, so the next `--seed` walks the tree.
        sqlx::query("UPDATE project SET head_run = NULL WHERE head_run = $1")
            .bind(color)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn set_run_example(
        &self,
        color: Color,
        example: Option<&str>,
    ) -> anyhow::Result<()> {
        let rows = sqlx::query(
            "UPDATE version_run SET example = $2 WHERE color = $1",
        )
        .bind(color)
        .bind(example)
        .execute(&self.pool)
        .await?;
        anyhow::ensure!(rows.rows_affected() == 1, "run {color} is not recorded in the version tree");
        Ok(())
    }

    async fn head(&self, project: uuid::Uuid) -> anyhow::Result<Head> {
        let row: Option<(Option<String>, Option<uuid::Uuid>, Option<String>)> = sqlx::query_as(
            "SELECT head_version, head_run, activation_version FROM project WHERE id = $1",
        )
        .bind(project)
        .fetch_optional(&self.pool)
        .await?;
        let (head_version, head_run, activation_version) =
            row.ok_or_else(|| anyhow::anyhow!("project {project} not found"))?;
        Ok(Head { head_version, head_run, activation_version })
    }

    async fn move_head(
        &self,
        project: uuid::Uuid,
        expected: &Head,
        version: Option<&str>,
        run: Option<Color>,
    ) -> anyhow::Result<bool> {
        // One statement, so the check and the write cannot be split by
        // a sibling Pod. `IS NOT DISTINCT FROM` because both columns
        // are nullable and NULL = NULL is not true in SQL.
        let rows = sqlx::query(
            "UPDATE project SET head_version = $2, head_run = $3 \
             WHERE id = $1 \
               AND head_version IS NOT DISTINCT FROM $4 \
               AND head_run IS NOT DISTINCT FROM $5",
        )
        .bind(project)
        .bind(version)
        .bind(run)
        .bind(expected.head_version.as_deref())
        .bind(expected.head_run)
        .execute(&self.pool)
        .await?;
        if rows.rows_affected() == 1 {
            return Ok(true);
        }
        // Nothing moved: either head is elsewhere now, or there is no
        // such project. Only the second is an error.
        // `EXISTS(...)` so the value decodes as a bool: a bare
        // `SELECT 1` is INT4, which does not decode into an i64, so
        // this probe used to ERROR on every lost race and turn the 409
        // it exists to produce into a 500.
        let (exists,): (bool,) = sqlx::query_as("SELECT EXISTS(SELECT 1 FROM project WHERE id = $1)")
            .bind(project)
            .fetch_one(&self.pool)
            .await?;
        anyhow::ensure!(exists, "project {project} not found");
        Ok(false)
    }

}

/// In-memory store for layer-3 tests: plain maps, no logic.
///
/// It refuses exactly what the schema refuses (a duplicate color, a run
/// under a version that does not exist, a head on an unknown project),
/// because a fake looser than its constraints is how a violation
/// reaches production with the tests green.
#[cfg(any(test, feature = "test-helpers"))]
#[derive(Default)]
pub struct MockVersionStore {
    versions: std::sync::Mutex<Vec<VersionRow>>,
    runs: std::sync::Mutex<Vec<RunRow>>,
    heads: std::sync::Mutex<BTreeMap<uuid::Uuid, Head>>,
    /// The projects that exist, standing in for the `project` table the
    /// head columns live on and the version FK points at.
    projects: std::sync::Mutex<BTreeSet<uuid::Uuid>>,
    /// Versions something still depends on: an armed trigger's settings
    /// came from one, or a live execution is running one. A plain set
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
impl MockVersionStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Say that something still depends on this version, so a prune of
    /// it is refused exactly as the real store refuses one.
    pub fn mark_in_use(&self, version: &str) {
        self.in_use.lock().unwrap().insert(version.to_string());
    }

    /// Register a project, as `project` holding a row for it.
    pub fn add_project(&self, project: uuid::Uuid) {
        self.projects.lock().unwrap().insert(project);
        self.heads.lock().unwrap().entry(project).or_default();
    }

    /// Drop the project and its whole tree, as `weft rm` does
    /// (`PostgresProjectStore::remove` deletes every `project_version`
    /// row in the same transaction as the project, and `version_run`
    /// cascades off it).
    ///
    /// The tree rows used to stay here, and the tests that rode on that
    /// certified a guarantee production does not give. A fake that
    /// erases less than the real store is a fake that hides exactly the
    /// leak it should catch.
    pub fn remove_project(&self, project: uuid::Uuid) {
        self.projects.lock().unwrap().remove(&project);
        self.heads.lock().unwrap().remove(&project);
        self.versions.lock().unwrap().retain(|v| v.project_id != project);
        self.runs.lock().unwrap().retain(|r| r.project_id != project);
    }

    /// Drop ONLY the project row, leaving its tree behind: the state a
    /// removal that failed halfway leaves, and the only state the
    /// retirement sweep can still find work in. Not a shape `weft rm`
    /// produces, which is why it is spelled differently from
    /// [`Self::remove_project`].
    pub fn forget_project_row(&self, project: uuid::Uuid) {
        self.projects.lock().unwrap().remove(&project);
        self.heads.lock().unwrap().remove(&project);
    }

    fn require_project(&self, project: uuid::Uuid) -> anyhow::Result<()> {
        anyhow::ensure!(self.projects.lock().unwrap().contains(&project), "project {project} not found");
        Ok(())
    }
}

#[cfg(any(test, feature = "test-helpers"))]
#[async_trait]
impl VersionStoreOps for MockVersionStore {
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
        anyhow::ensure!(!ids.iter().any(|id| in_use.contains(id)),
            "these versions still supply trigger settings or running executions; stop the runs and replace or wipe those trigger settings before pruning");
        drop(in_use);
        self.versions.lock().unwrap().retain(|v| !(v.project_id == project && ids.contains(&v.id)));
        self.runs.lock().unwrap().retain(|r| !(r.project_id == project && ids.contains(&r.version_id)));
        Ok(())
    }

    async fn projects_with_orphan_versions(&self) -> anyhow::Result<Vec<uuid::Uuid>> {
        let live = self.projects.lock().unwrap();
        let mut ids: Vec<uuid::Uuid> = self
            .versions
            .lock()
            .unwrap()
            .iter()
            .map(|v| v.project_id)
            .filter(|id| !live.contains(id))
            .collect();
        ids.sort();
        ids.dedup();
        Ok(ids)
    }

    async fn retire_unused_versions(&self, project: uuid::Uuid, known: &[Color]) -> anyhow::Result<u64> {
        // Nothing at all while the project row is there, which is what
        // Postgres's `NOT EXISTS (SELECT 1 FROM project ...)` says.
        if self.projects.lock().unwrap().contains(&project) {
            return Ok(0);
        }
        let mut runs = self.runs.lock().unwrap();
        let mut versions = self.versions.lock().unwrap();
        let before = runs.len() + versions.len();
        // The run rows the journal has never heard of.
        runs.retain(|r| r.project_id != project || known.contains(&r.color));
        // Then every version a surviving run needs, closed up the lineage
        // so a survivor's parent is never deleted out from under it.
        let mut kept: BTreeSet<String> = runs
            .iter()
            .filter(|r| r.project_id == project)
            .map(|r| r.version_id.clone())
            .collect();
        loop {
            let parents: BTreeSet<String> = versions
                .iter()
                .filter(|v| v.project_id == project && kept.contains(&v.id))
                .filter_map(|v| v.parent_id.clone())
                .filter(|p| !kept.contains(p))
                .collect();
            if parents.is_empty() {
                break;
            }
            kept.extend(parents);
        }
        versions.retain(|v| v.project_id != project || kept.contains(&v.id));
        Ok((before - runs.len() - versions.len()) as u64)
    }

    async fn insert_run(&self, run: &RunRow) -> anyhow::Result<()> {
        // The primary key and the composite foreign key, as Postgres
        // enforces them.
        let mut runs = self.runs.lock().unwrap();
        anyhow::ensure!(!runs.iter().any(|r| r.color == run.color), "run {} is already recorded", run.color);
        let known = self
            .versions
            .lock()
            .unwrap()
            .iter()
            .any(|v| v.project_id == run.project_id && v.id == run.version_id);
        if !known {
            return Err(VersionMissing { project: run.project_id, version: run.version_id.clone() }.into());
        }
        runs.push(run.clone());
        Ok(())
    }

    async fn delete_run(&self, color: Color) -> anyhow::Result<()> {
        self.runs.lock().unwrap().retain(|r| r.color != color);
        for head in self.heads.lock().unwrap().values_mut() {
            if head.head_run == Some(color) {
                head.head_run = None;
            }
        }
        Ok(())
    }

    async fn run(&self, color: Color) -> anyhow::Result<Option<RunRow>> {
        Ok(self.runs.lock().unwrap().iter().find(|r| r.color == color).cloned())
    }

    async fn runs(&self, project: uuid::Uuid) -> anyhow::Result<Vec<RunRow>> {
        Ok(self.runs.lock().unwrap().iter().filter(|r| r.project_id == project).cloned().collect())
    }

    async fn set_run_example(
        &self,
        color: Color,
        example: Option<&str>,
    ) -> anyhow::Result<()> {
        let mut runs = self.runs.lock().unwrap();
        let row = runs
            .iter_mut()
            .find(|r| r.color == color)
            .ok_or_else(|| anyhow::anyhow!("run {color} is not recorded in the version tree"))?;
        row.example = example.map(str::to_string);
        Ok(())
    }

    async fn head(&self, project: uuid::Uuid) -> anyhow::Result<Head> {
        self.require_project(project)?;
        Ok(self.heads.lock().unwrap().get(&project).cloned().unwrap_or_default())
    }

    async fn move_head(
        &self,
        project: uuid::Uuid,
        expected: &Head,
        version: Option<&str>,
        run: Option<Color>,
    ) -> anyhow::Result<bool> {
        self.require_project(project)?;
        let mut heads = self.heads.lock().unwrap();
        let head = heads.entry(project).or_default();
        if head.head_version.as_deref() != expected.head_version.as_deref() || head.head_run != expected.head_run {
            return Ok(false);
        }
        head.head_version = version.map(str::to_string);
        head.head_run = run;
        Ok(true)
    }

}

// ----- The decisions, as pure functions over the rows -----------------

/// What a run's `--seed` resolves to: the run to inherit from, or why
/// none.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SeedChoice {
    /// Inherit from this run.
    Run(Color),
    /// Nothing to inherit: no head, `--root`, or no settled run
    /// anywhere on head's ancestry.
    Nothing,
    /// Head's run is still running; nothing is cancelled on the user's
    /// behalf. The message names `weft stop <color>`.
    HeadRunning(Color),
}

/// Pick the seed: head's run, or when head has no run the newest
/// settled run on head's version or its nearest ancestor with one.
/// `settled` answers whether a color is terminal or suspended (a
/// running run is never a seed).
pub fn resolve_seed(
    head: &Head,
    versions: &[VersionRow],
    runs: &[RunRow],
    settled: impl Fn(Color) -> bool,
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
            .find(|r| settled(r.color));
        if let Some(run) = newest_settled {
            return SeedChoice::Run(run.color);
        }
        match by_id.get(version.as_str()).and_then(|v| v.parent_id.clone()) {
            Some(parent) => version = parent,
            None => return SeedChoice::Nothing,
        }
    }
}

/// The paths that differ between `parent` and `child`: added, removed,
/// and changed, sorted. What `weft tree` prints next to a version.
// SYNC: ManifestDiff <-> crates/weft-cli/src/commands/versions.rs ManifestDiff
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestDiff {
    pub added: Vec<String>,
    pub removed: Vec<String>,
    pub changed: Vec<String>,
}

pub fn manifest_diff(parent: &Manifest, child: &Manifest) -> ManifestDiff {
    let mut diff = ManifestDiff::default();
    for (path, hash) in child {
        match parent.get(path) {
            None => diff.added.push(path.clone()),
            Some(h) if h != hash => diff.changed.push(path.clone()),
            Some(_) => {}
        }
    }
    for path in parent.keys() {
        if !child.contains_key(path) {
            diff.removed.push(path.clone());
        }
    }
    diff
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

/// What `weft prune <version>` removes: the subtree's versions, every
/// run under them, and the blobs no surviving manifest names.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrunePlan {
    pub versions: Vec<String>,
    pub runs: Vec<Color>,
    /// `sha256` of every blob only the pruned versions held.
    pub blobs: Vec<String>,
}

/// Plan a prune, or refuse it: head's version (branch away first), a
/// version that is or is an ancestor of a frozen example's origin
/// (unfreeze the example first), or a subtree with a run still in
/// flight (stop or wait). `frozen_from` is every `frozen_from.version`
/// in the project's examples; `in_flight` says whether a color is
/// neither terminal nor suspended.
pub fn plan_prune(
    root: &str,
    versions: &[VersionRow],
    runs: &[RunRow],
    head: &Head,
    frozen_from: &[String],
    in_flight: impl Fn(Color) -> bool,
) -> Result<PrunePlan, PruneRefusal> {
    let mut reasons = Vec::new();
    if !versions.iter().any(|v| v.id == root) {
        return Err(PruneRefusal { reasons: vec![format!("no version {root} in this project")] });
    }
    let doomed = subtree(versions, root);
    if head.head_version.as_deref().is_some_and(|h| doomed.iter().any(|d| d == h)) {
        reasons.push(format!(
            "head is on version {}, inside the subtree; `weft branch <version>` away from it first",
            head.head_version.as_deref().unwrap_or("")
        ));
    }
    if head.activation_version.as_deref().is_some_and(|a| doomed.iter().any(|d| d == a)) {
        reasons.push("the activated version is inside the subtree; `weft deactivate` first".to_string());
    }
    for origin in frozen_from {
        let protected: Vec<String> = std::iter::once(origin.clone()).chain(ancestors(versions, origin)).collect();
        if let Some(hit) = doomed.iter().find(|d| protected.contains(d)) {
            reasons.push(format!(
                "version {hit} is (or is an ancestor of) the origin {origin} of a frozen example; \
                 remove the example's `frozen_from` (`weft freeze` again from another run) first"
            ));
        }
    }
    let doomed_runs: Vec<&RunRow> = runs.iter().filter(|r| doomed.contains(&r.version_id)).collect();
    for run in &doomed_runs {
        if in_flight(run.color) {
            reasons.push(format!("run {} is still running; `weft stop {}` or wait", run.color, run.color));
        }
    }
    if !reasons.is_empty() {
        return Err(PruneRefusal { reasons });
    }
    let surviving: BTreeSet<&str> = versions
        .iter()
        .filter(|v| !doomed.contains(&v.id))
        .flat_map(|v| v.manifest.values().map(String::as_str))
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
        runs: doomed_runs.iter().map(|r| r.color).collect(),
        blobs: blobs.into_iter().collect(),
    })
}

/// The versions a bulk clean may drop after their runs went: no runs
/// left, no descendants, no label, and not head.
pub fn sweepable_versions(versions: &[VersionRow], runs: &[RunRow], head: &Head) -> Vec<String> {
    versions
        .iter()
        .filter(|v| v.label.is_none())
        .filter(|v| head.head_version.as_deref() != Some(v.id.as_str()))
        .filter(|v| head.activation_version.as_deref() != Some(v.id.as_str()))
        .filter(|v| !runs.iter().any(|r| r.version_id == v.id))
        .filter(|v| !versions.iter().any(|c| c.parent_id.as_deref() == Some(v.id.as_str())))
        .map(|v| v.id.clone())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn color(n: u8) -> Color {
        Color::from_bytes([n; 16])
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
            color: color(n),
            project_id: uuid::Uuid::nil(),
            version_id: version.into(),
            seed_color: None,
            stale: vec![],
            spec: None,
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
        let head = Head { head_version: Some("v1".into()), head_run: Some(color(1)), activation_version: None };
        assert_eq!(resolve_seed(&head, &[], &[], |_| true), SeedChoice::Run(color(1)));
        assert_eq!(resolve_seed(&head, &[], &[], |_| false), SeedChoice::HeadRunning(color(1)));
    }

    #[test]
    fn without_a_head_run_the_nearest_ancestor_with_a_settled_run_seeds() {
        let versions = vec![version("v1", None, &[], 1), version("v2", Some("v1"), &[], 2), version("v3", Some("v2"), &[], 3)];
        let runs = vec![run(1, "v1", 1), run(2, "v1", 2), run(3, "v2", 3)];
        let head = Head { head_version: Some("v3".into()), head_run: None, activation_version: None };
        assert_eq!(resolve_seed(&head, &versions, &runs, |_| true), SeedChoice::Run(color(3)));
        // v2's run still running: skip to v1's newest settled.
        assert_eq!(resolve_seed(&head, &versions, &runs, |c| c != color(3)), SeedChoice::Run(color(2)));
        let head = Head { head_version: None, head_run: None, activation_version: None };
        assert_eq!(resolve_seed(&head, &versions, &runs, |_| true), SeedChoice::Nothing);
    }

    #[test]
    fn manifest_diff_names_every_kind_of_change() {
        let parent = version("p", None, &[("a", "1"), ("b", "2"), ("c", "3")], 0).manifest;
        let child = version("c", None, &[("a", "1"), ("b", "X"), ("d", "4")], 0).manifest;
        let diff = manifest_diff(&parent, &child);
        assert_eq!(diff.added, vec!["d"]);
        assert_eq!(diff.removed, vec!["c"]);
        assert_eq!(diff.changed, vec!["b"]);
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
    fn prune_refuses_head_a_frozen_ancestor_and_an_in_flight_subtree() {
        let runs = vec![run(1, "v2", 1), run(2, "v3", 2)];
        let on_head = Head { head_version: Some("v3".into()), head_run: None, activation_version: None };
        let err = plan_prune("v2", &tree(), &runs, &on_head, &[], |_| false).unwrap_err();
        assert!(err.reasons[0].contains("weft branch"), "{:?}", err);
        let away = Head { head_version: Some("side".into()), head_run: None, activation_version: None };
        let err = plan_prune("v2", &tree(), &runs, &away, &["v3".into()], |_| false).unwrap_err();
        assert!(err.reasons[0].contains("frozen example"), "{:?}", err);
        let err = plan_prune("v2", &tree(), &runs, &away, &[], |c| c == color(2)).unwrap_err();
        assert!(err.reasons[0].contains("weft stop"), "{:?}", err);
        assert!(plan_prune("nope", &tree(), &runs, &away, &[], |_| false).is_err());
    }

    #[test]
    fn prune_removes_the_subtree_its_runs_and_the_orphaned_blobs() {
        let runs = vec![run(1, "v2", 1), run(2, "v3", 2), run(3, "side", 3)];
        let away = Head { head_version: Some("side".into()), head_run: None, activation_version: None };
        let plan = plan_prune("v2", &tree(), &runs, &away, &[], |_| false).unwrap();
        assert_eq!(plan.versions, vec!["v2", "v3"]);
        assert_eq!(plan.runs, vec![color(1), color(2)]);
        assert_eq!(plan.blobs, vec!["2", "3"], "blob 1 survives on root and side");
    }

    #[test]
    fn a_bulk_clean_sweeps_only_bare_leaves() {
        let mut versions = tree();
        versions[3].label = Some("keep".into());
        let runs = vec![run(2, "v3", 2)];
        let head = Head { head_version: Some("root".into()), head_run: None, activation_version: None };
        // v2 has a descendant, v3 has a run, side is labelled, root is head.
        assert!(sweepable_versions(&versions, &runs, &head).is_empty());
        assert_eq!(sweepable_versions(&versions, &[], &head), vec!["v3"]);
    }

    /// A run on a version the tree does not hold answers the one error a
    /// reader can act on, the same way Postgres's foreign key does.
    #[tokio::test]
    async fn a_run_on_an_unknown_version_names_the_missing_version() {
        let store = MockVersionStore::new();
        let project = uuid::Uuid::nil();
        store.add_project(project);
        let err = store.insert_run(&run(1, "never-committed", 3)).await.expect_err("no such version");
        let missing = err.downcast_ref::<VersionMissing>().expect("the typed error, not a message");
        assert_eq!(missing.version, "never-committed");
        assert_eq!(missing.project, project);
    }

    /// A removed project keeps exactly the tree its surviving runs need.
    ///
    /// `weft rm` takes the whole tree with the project, in the same
    /// transaction, and the runs cascade off it. Nothing of a removed
    /// project is left to prune, which is the point: what used to be
    /// kept could not be reached or freed by anything afterwards.
    #[tokio::test]
    async fn removing_a_project_takes_its_whole_tree() {
        let store = MockVersionStore::new();
        let project = uuid::Uuid::nil();
        store.add_project(project);
        store.upsert_version(&version("ran", None, &[("main.weft", "1")], 1)).await.unwrap();
        store.upsert_version(&version("never-ran", Some("ran"), &[("main.weft", "2")], 2)).await.unwrap();
        store.insert_run(&run(1, "ran", 3)).await.unwrap();

        // Nothing is pruned while the project is still registered: a
        // version with no run is still a version you can go back to.
        assert_eq!(store.retire_unused_versions(project, &[color(1)]).await.unwrap(), 0);
        assert_eq!(store.versions(project).await.unwrap().len(), 2);

        store.remove_project(project);
        assert!(store.versions(project).await.unwrap().is_empty(), "the tree went with it");
        assert!(store.runs(project).await.unwrap().is_empty(), "and the runs cascaded");
        // So the prune finds nothing rather than something: it exists
        // for a removal that failed halfway, not for the ordinary one.
        assert_eq!(store.retire_unused_versions(project, &[]).await.unwrap(), 0);
    }

    /// A run row the journal has never heard of goes, and the lineage of a
    /// surviving run stays whole.
    ///
    /// The first is what a start that failed between recording the run and
    /// journaling it leaves. Nothing else can reach such a row once the
    /// project is gone (`weft rm` refuses a project it cannot find, `weft
    /// clean` enumerates journal colors), so it used to pin its version for
    /// ever. The second is `parent_id`, which carries no foreign key:
    /// deleting a run-less ancestor would leave the survivor parented on a
    /// row that is gone, and nothing would reject it.
    ///
    /// This uses `forget_project_row` rather than `remove_project`:
    /// an ordinary removal takes the whole tree, so the only way tree
    /// rows outlive their project is a removal that failed halfway,
    /// which is the case this sweep exists for.
    #[tokio::test]
    async fn retirement_drops_what_no_journal_knows_and_keeps_the_lineage() {
        let store = MockVersionStore::new();
        let project = uuid::Uuid::nil();
        store.add_project(project);
        // root (no runs) -> middle (no runs) -> leaf (one real run)
        store.upsert_version(&version("root", None, &[("main.weft", "1")], 1)).await.unwrap();
        store.upsert_version(&version("middle", Some("root"), &[("main.weft", "2")], 2)).await.unwrap();
        store.upsert_version(&version("leaf", Some("middle"), &[("main.weft", "3")], 3)).await.unwrap();
        store.insert_run(&run(1, "leaf", 4)).await.unwrap();
        // And a run whose execution never started, on a version of its own.
        store.upsert_version(&version("stillborn", Some("root"), &[("main.weft", "4")], 5)).await.unwrap();
        store.insert_run(&run(2, "stillborn", 6)).await.unwrap();

        store.forget_project_row(project);
        // The journal knows only the first color.
        let dropped = store.retire_unused_versions(project, &[color(1)]).await.unwrap();
        assert_eq!(dropped, 2, "the unknown run row and its now-bare version");
        let left: Vec<String> = store.versions(project).await.unwrap().into_iter().map(|v| v.id).collect();
        assert_eq!(left, vec!["root", "middle", "leaf"], "the survivor's ancestors stay");
        let runs: Vec<Color> = store.runs(project).await.unwrap().into_iter().map(|r| r.color).collect();
        assert_eq!(runs, vec![color(1)]);
    }
}
