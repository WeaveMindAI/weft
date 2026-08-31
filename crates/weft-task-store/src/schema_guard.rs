//! The schema runner: canonical table definitions plus the ordered changes
//! that carry an existing database to them.
//!
//! Every table is described twice, on purpose, and the two answer different
//! questions.
//!
//! The **canonical DDL** on a [`SchemaGroup`] says what the table IS. It is
//! `CREATE TABLE` text edited in place, it lives beside the code that reads
//! and writes the table, and it carries the comments explaining why the shape
//! is what it is. A fresh database is built from it in one shot.
//!
//! A **migration** says how the table CHANGED. One file per change, named so
//! the files sort in the order they were written, never edited once merged. A
//! database that already exists is carried forward by running the ones it has
//! not seen yet, in order. Because each database records what it has run,
//! rather than which release it came from, any old database reaches today by
//! the same path, and two people merging changes in either order converge.
//!
//! Keeping both is what lets the schema stay readable in one place AND lets a
//! running database survive an upgrade. The risk of keeping both is that they
//! drift: a column added to the canonical DDL with no migration written gives
//! a fresh install one shape and an upgraded install another. Two things stop
//! that. Here, a group whose DDL text changed with no new migration to explain
//! it fails the boot. And in CI, [`assert_schema_agrees`] builds a database
//! each way and compares them, which is what catches a migration that runs
//! but does not land on the same shape.
//!
//! Applied in one transaction under an advisory lock, in the same order
//! the agreement test replays: first the DDL of every group new to this
//! database (its migrations are recorded as history it was born with),
//! then every PENDING migration across every group in one global id
//! order (ids are timestamps, so this is the order the changes were
//! actually written, and what a migration referencing another group's
//! table relies on), then every group's DDL re-run (idempotent, and it
//! repairs a hand-dropped table), and every group's seeds re-run.
//!
//! **A stamp that moved is refreshed on evidence, never on faith**: the
//! canonical DDL is built into a scratch schema in the same transaction
//! and compared, object by object, with what the live schema holds. A
//! match restamps silently (the fingerprint moved with no shape change,
//! or a migration provably landed the shape). A difference is the drift
//! case: the boot fails naming the group and the exact objects that
//! differ, with the SQL to reset it for anyone iterating locally who
//! does not want to write the migration yet.

use std::collections::HashMap;

use sha2::{Digest, Sha256};
use sqlx::PgPool;

/// One change to one group's tables: the file's SQL, the group it sits under,
/// and the id it is filed by. Ids sort chronologically
/// (`20260823T1412_add_owner`), which is the order they run in.
///
/// An id is recorded the moment its SQL runs, so it never runs twice, and its
/// checksum is recorded with it, so editing a file that has already run
/// somewhere fails loudly instead of leaving two databases silently apart.
pub struct Migration {
    pub group: &'static str,
    pub id: &'static str,
    /// A draft exists only on the machine that wrote it (its file is
    /// gitignored). It keeps that one database in step with a shape still
    /// being decided, and is collapsed into a released migration when the
    /// shape settles. Ids start with `draft_`, so drafts sort after every
    /// released file and run last.
    pub draft: bool,
    pub sql: &'static str,
}

// Every file under `migrations/`, gathered at build time. Writing a migration
// is dropping a file in there; nothing registers it.
include!(concat!(env!("OUT_DIR"), "/migrations.rs"));

/// One module's schema: the canonical DDL, the tables it creates, and the
/// changes that carry an older database to it. Declared as a `static` next to
/// the DDL's owning module.
pub struct SchemaGroup {
    /// Short stable slug, usually the main table name. Keys the stored
    /// fingerprint in `weft_schema_stamp` and the applied ids in
    /// `weft_migration`.
    pub name: &'static str,
    /// Every table the group's ddl creates.
    pub tables: &'static [&'static str],
    /// The exact statements the group runs. An entry may hold multiple
    /// semicolon-separated statements (executed via `raw_sql`).
    pub ddl: &'static [&'static str],
    /// Idempotent DML the group needs present (a cursor row, a seed
    /// record), re-run on EVERY boot after the DDL. Seeds are not
    /// schema: they are outside the fingerprint and outside the
    /// migration history, so editing one is a non-event rather than a
    /// "write the migration" dead end (a seed row is invisible to the
    /// schema reader, so no migration could ever be generated for it).
    pub seed: &'static [&'static str],
}

/// Group and table names are interpolated into the mismatch error's
/// paste-ready reset SQL, so they must be plain SQL identifiers; a stray
/// quote would hand the operator broken SQL at the worst moment. Static
/// declarations, so this fails the first boot of the build that
/// introduced the bad name.
fn validate_identifiers(groups: &[&SchemaGroup]) -> anyhow::Result<()> {
    for group in groups {
        for name in std::iter::once(group.name).chain(group.tables.iter().copied()) {
            anyhow::ensure!(
                !name.is_empty()
                    && name.chars().next().is_some_and(|c| c.is_ascii_lowercase() || c == '_')
                    && name
                        .chars()
                        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'),
                "schema group '{}' declares a non-identifier name '{name}'; group and \
                 table names must match [a-z_][a-z0-9_]*",
                group.name
            );
        }
        // The guard's own bookkeeping tables are excluded from every
        // shape reading (see THING_QUERIES), so a group table by
        // either name would be invisible to the drift check.
        for table in group.tables {
            anyhow::ensure!(
                *table != "weft_schema_stamp" && *table != "weft_migration",
                "schema group '{}' declares '{table}', which is the schema guard's \
                 own bookkeeping table; pick another name",
                group.name
            );
        }
    }
    // Two groups declaring the same function/type/trigger name would
    // make ownership of its changes ambiguous (the migration filer and
    // the shape reader both key on the declaring group). Within one
    // group a repeated declaration (a recreate-in-place CREATE OR
    // REPLACE pattern) is fine and deduped first.
    let mut seen: HashMap<String, &str> = HashMap::new();
    for group in groups {
        let mut mine: std::collections::HashSet<String> = Default::default();
        for name in declared_names(group) {
            if !mine.insert(name.clone()) {
                continue;
            }
            if let Some(other) = seen.insert(name.clone(), group.name) {
                anyhow::bail!(
                    "schema groups '{other}' and '{}' both declare '{name}'; \
                     one group must own each function, type, and trigger",
                    group.name
                );
            }
        }
    }
    Ok(())
}

/// The functions, enum types, and triggers a group's DDL declares, read
/// from the CREATE statements themselves, so ownership never depends on
/// a name merely appearing somewhere in another group's text (a comment,
/// a column name).
fn declared_names(group: &SchemaGroup) -> Vec<String> {
    const HEADS: &[&str] = &[
        "create or replace function ",
        "create function ",
        "create type ",
        "create or replace trigger ",
        "create trigger ",
    ];
    let mut out = Vec::new();
    for stmt in group.ddl {
        for line in stmt.lines() {
            let line = line.trim();
            let lower = line.to_ascii_lowercase();
            for head in HEADS {
                if let Some(rest) = lower.strip_prefix(head) {
                    let name: String = rest
                        .chars()
                        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                        .collect();
                    if !name.is_empty() {
                        out.push(name);
                    }
                }
            }
        }
    }
    out
}

/// Whether `thing` belongs to `group`: its table is one the group
/// declares, or, for a table-less object and for a trigger (owned by
/// the group whose DDL DECLARES it, not the group owning the table it
/// fires on), its name is one the group's CREATE statements declare.
fn owned_by(group: &SchemaGroup, thing: &Thing) -> bool {
    if thing.table.is_empty() || thing.kind == "trigger" {
        declared_names(group).iter().any(|n| n == &thing.name)
    } else {
        group.tables.contains(&thing.table.as_str())
    }
}

/// This group's migrations, out of every migration there is.
fn for_group<'a>(migrations: &'a [Migration], group: &str) -> Vec<&'a Migration> {
    migrations.iter().filter(|m| m.group == group).collect()
}

/// Hex sha256 over the group's DDL statements, each followed by a newline.
/// Any textual edit to the DDL (a new column, a changed default, a comment)
/// changes the fingerprint; that is the point: the stamp certifies "the
/// database's tables were created by exactly this text".
fn fingerprint(ddl: &[&str]) -> String {
    let mut hasher = Sha256::new();
    for stmt in ddl {
        hasher.update(stmt.as_bytes());
        hasher.update(b"\n");
    }
    hex(hasher.finalize().as_slice())
}

/// Hex sha256 over one migration file, so an edit to a file that already ran
/// is caught rather than silently ignored.
fn checksum(sql: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(sql.as_bytes());
    hex(hasher.finalize().as_slice())
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(out, "{byte:02x}").expect("write to a String");
    }
    out
}

/// The migrations this database has not run yet, in the order they run.
///
/// Pure so the ordering and the already-applied filter are unit-testable
/// without a database. Sorting by id here, rather than trusting the order the
/// group declares them in, is what makes two branches that each add a
/// migration converge whichever way round they merge.
fn pending<'a>(
    declared: &[&'a Migration],
    applied: &HashMap<String, String>,
) -> Vec<&'a Migration> {
    let mut out: Vec<&Migration> =
        declared.iter().copied().filter(|m| !applied.contains_key(m.id)).collect();
    out.sort_by_key(|m| m.id);
    out
}

/// A migration file that was edited after it ran somewhere, which would leave
/// two databases on different shapes with no way to tell.
fn edited_after_running(
    declared: &[&Migration],
    applied: &HashMap<String, String>,
) -> Vec<&'static str> {
    declared
        .iter()
        .filter(|m| applied.get(m.id).is_some_and(|stored| *stored != checksum(m.sql)))
        .map(|m| m.id)
        .collect()
}

/// Bring `pool`'s schema up to what `groups` describe.
///
/// The whole run executes in one transaction under an advisory lock:
/// `IF NOT EXISTS` is idempotent but not concurrency-safe in Postgres
/// (two backends racing the same CREATE on a fresh database both pass the
/// existence check, then one fails on a duplicate catalog key), so
/// concurrent boots serialize and the losers see the tables present. The
/// transaction also makes each stamp atomic with its DDL: a stamp is never
/// written for DDL that did not run.
pub async fn apply_groups(pool: &PgPool, groups: &[&SchemaGroup]) -> anyhow::Result<()> {
    apply_groups_with(pool, groups, MIGRATIONS).await
}

/// [`apply_groups`], against a migration set the caller names. The boot always
/// uses every migration there is; a test names its own.
pub async fn apply_groups_with(
    pool: &PgPool,
    groups: &[&SchemaGroup],
    migrations: &[Migration],
) -> anyhow::Result<()> {
    validate_identifiers(groups)?;
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended('weft:schema-migrate', 0))")
        .execute(&mut *tx)
        .await?;
    sqlx::raw_sql(
        "CREATE TABLE IF NOT EXISTS weft_schema_stamp (\
             group_name TEXT PRIMARY KEY, fingerprint TEXT NOT NULL);\
         CREATE TABLE IF NOT EXISTS weft_migration (\
             group_name TEXT NOT NULL, \
             id TEXT NOT NULL, \
             checksum TEXT NOT NULL, \
             applied_at TIMESTAMPTZ NOT NULL DEFAULT now(), \
             PRIMARY KEY (group_name, id))",
    )
    .execute(&mut *tx)
    .await?;

    // Pass 1: read each group's state and refuse rewritten history, so
    // nothing runs before every group has been checked.
    struct GroupState<'a> {
        group: &'a SchemaGroup,
        fp: String,
        stamped: Option<String>,
        applied: HashMap<String, String>,
    }
    let mut states: Vec<GroupState> = Vec::new();
    for group in groups {
        let stored: Option<(String,)> =
            sqlx::query_as("SELECT fingerprint FROM weft_schema_stamp WHERE group_name = $1")
                .bind(group.name)
                .fetch_optional(&mut *tx)
                .await?;
        let applied: HashMap<String, String> =
            sqlx::query_as::<_, (String, String)>(
                "SELECT id, checksum FROM weft_migration WHERE group_name = $1",
            )
            .bind(group.name)
            .fetch_all(&mut *tx)
            .await?
            .into_iter()
            .collect();

        let mine = for_group(migrations, group.name);
        let edited = edited_after_running(&mine, &applied);
        anyhow::ensure!(
            edited.is_empty(),
            "schema group '{}': migration file(s) {} were edited after running against \
             this database. A migration that has run is history and cannot be rewritten: \
             restore the file and write a new migration for the change you wanted.",
            group.name,
            edited.join(", ")
        );
        // The frozen origin is history too. It was recorded when this
        // database first built the group; an edited origin.sql would
        // silently move where every future replay starts from. A
        // database from before origins were recorded adopts the current
        // embedded text NOW, so the check is armed on the next boot
        // instead of staying inert forever on long-lived databases.
        if let Some(embedded) = embedded_origin(group.name) {
            match applied.get(ORIGIN_ID) {
                Some(stored) => anyhow::ensure!(
                    *stored == checksum(embedded),
                    "schema group '{}': origin.sql was edited after this database was built \
                     from it. The origin is the frozen past every replay starts from; \
                     restore it and express the change as a migration instead.",
                    group.name
                ),
                None if stored.is_some() => {
                    record_migration(&mut tx, group.name, ORIGIN_ID, embedded).await?;
                }
                None => {}
            }
        }
        states.push(GroupState {
            group,
            fp: fingerprint(group.ddl),
            stamped: stored.map(|(fp,)| fp),
            applied,
        });
    }

    // Pass 2: build every group that is new to this database from its
    // canonical DDL; its migrations (and origin) are history it was
    // born with, recorded without running.
    for st in &states {
        if st.stamped.is_some() {
            continue;
        }
        for stmt in st.group.ddl {
            sqlx::raw_sql(stmt).execute(&mut *tx).await.map_err(|e| {
                anyhow::anyhow!("schema group '{}': DDL failed: {e}", st.group.name)
            })?;
        }
        sqlx::query("INSERT INTO weft_schema_stamp (group_name, fingerprint) VALUES ($1, $2)")
            .bind(st.group.name)
            .bind(&st.fp)
            .execute(&mut *tx)
            .await?;
        for m in for_group(migrations, st.group.name) {
            record_migration(&mut tx, st.group.name, m.id, m.sql).await?;
        }
        if let Some(origin) = embedded_origin(st.group.name) {
            record_migration(&mut tx, st.group.name, ORIGIN_ID, origin).await?;
        }
    }

    // Pass 3: one global run of everything pending across every stamped
    // group, sorted by id. Ids are timestamps, so this is the order the
    // changes were written, the same order the agreement test replays,
    // and the order a migration touching another group's table relies
    // on.
    let mut todo: Vec<(&SchemaGroup, &Migration)> = Vec::new();
    for st in &states {
        if st.stamped.is_none() {
            continue;
        }
        for m in pending(&for_group(migrations, st.group.name), &st.applied) {
            todo.push((st.group, m));
        }
    }
    todo.sort_by_key(|(_, m)| m.id);
    for (group, m) in todo {
        sqlx::raw_sql(m.sql).execute(&mut *tx).await.map_err(|e| {
            anyhow::anyhow!("schema group '{}': migration '{}' failed: {e}", group.name, m.id)
        })?;
        record_migration(&mut tx, group.name, m.id, m.sql).await?;
    }

    // Pass 4: re-run every stamped group's DDL (idempotent; repairs a
    // hand-dropped table). A stamp that moved is only NOTED here; it is
    // refreshed by pass 5 on evidence, never on faith.
    let mut moved: Vec<&GroupState> = Vec::new();
    for st in &states {
        let Some(stored_fp) = &st.stamped else { continue };
        for stmt in st.group.ddl {
            sqlx::raw_sql(stmt).execute(&mut *tx).await.map_err(|e| {
                anyhow::anyhow!("schema group '{}': DDL failed: {e}", st.group.name)
            })?;
        }
        if *stored_fp != st.fp {
            moved.push(st);
        }
    }

    // Pass 5: verify every moved stamp against the database itself. The
    // canonical DDL is built into a scratch schema inside this same
    // transaction and compared, object by object, with what the live
    // schema now holds (after pass 3's migrations and pass 4's
    // idempotent re-run). A group whose live shape matches is restamped:
    // a fingerprint that moved with no shape change (a comment, a seed
    // moved out of the DDL) is a non-event, and a migration that landed
    // the shape is proven rather than assumed. A group whose live shape
    // differs is the drift case, whatever migrations ran: "something was
    // pending" must never launder a DDL edit nothing carried across.
    let mut mismatched: Vec<(&SchemaGroup, String)> = Vec::new();
    if !moved.is_empty() {
        sqlx::raw_sql(
            "DROP SCHEMA IF EXISTS weft_shape_check CASCADE; \
             CREATE SCHEMA weft_shape_check; \
             SET LOCAL search_path TO weft_shape_check",
        )
        .execute(&mut *tx)
        .await?;
        // Every group in the call, so cross-group references (a trigger
        // on another group's table) resolve, in the same dependency
        // order a fresh database builds in.
        for group in groups {
            for stmt in group.ddl {
                sqlx::raw_sql(stmt).execute(&mut *tx).await.map_err(|e| {
                    anyhow::anyhow!(
                        "schema group '{}': DDL failed in the shape check: {e}",
                        group.name
                    )
                })?;
            }
        }
        sqlx::raw_sql("SET LOCAL search_path TO public").execute(&mut *tx).await?;
        let want = read_things(&mut tx, "weft_shape_check").await?;
        let live = read_things(&mut tx, "public").await?;
        for st in &moved {
            let want_g: Vec<Thing> =
                want.iter().filter(|t| owned_by(st.group, t)).cloned().collect();
            let live_g: Vec<Thing> =
                live.iter().filter(|t| owned_by(st.group, t)).cloned().collect();
            match diff_things(&want_g, &live_g) {
                Some(diff) => mismatched.push((st.group, diff)),
                None => {
                    sqlx::query(
                        "UPDATE weft_schema_stamp SET fingerprint = $2 WHERE group_name = $1",
                    )
                    .bind(st.group.name)
                    .bind(&st.fp)
                    .execute(&mut *tx)
                    .await?;
                }
            }
        }
        sqlx::raw_sql("DROP SCHEMA weft_shape_check CASCADE").execute(&mut *tx).await?;
    }

    // The drift bail fires BEFORE the seeds: a seed touching a drifted
    // table would otherwise fail first with a raw Postgres error and
    // swallow the one message this mechanism exists to produce.
    if !mismatched.is_empty() {
        let names: Vec<&str> = mismatched.iter().map(|(g, _)| g.name).collect();
        let mut reset_sql = String::new();
        let mut diffs = String::new();
        for (group, diff) in &mismatched {
            diffs.push_str(&format!("group '{}':\n{diff}", group.name));
            for table in group.tables {
                reset_sql.push_str(&format!("DROP TABLE IF EXISTS {table} CASCADE;\n"));
            }
            reset_sql.push_str(&format!(
                "DELETE FROM weft_schema_stamp WHERE group_name = '{}';\n\
                 DELETE FROM weft_migration WHERE group_name = '{}';\n",
                group.name, group.name
            ));
        }
        anyhow::bail!(
            "the canonical schema changed and this database does not hold the shape it \
             declares, for group(s): {}.\n{}\
             Add the migration that makes the change to an existing database, beside \
             the group's other migration files (./setup.sh --migration <name>).\n\
             If you are iterating locally and are not ready to write it, reset exactly \
             these tables instead:\n\n{}\n\
             then restart the service so they are rebuilt from the current DDL.",
            names.join(", "),
            diffs,
            reset_sql
        );
    }

    // Pass 6: seeds, idempotent DML re-run on every boot, after every
    // table exists.
    for group in groups {
        for stmt in group.seed {
            sqlx::raw_sql(stmt).execute(&mut *tx).await.map_err(|e| {
                anyhow::anyhow!("schema group '{}': seed failed: {e}", group.name)
            })?;
        }
    }

    tx.commit().await?;
    Ok(())
}

/// The reserved `weft_migration` id under which a group's frozen
/// `origin.sql` checksum is recorded, so an edited origin is caught at
/// boot exactly like an edited migration. Never a real file stem (the
/// build script drops `origin` stems from `MIGRATIONS`).
const ORIGIN_ID: &str = "origin";

/// This group's frozen `origin.sql`, embedded at build time (same walk
/// as `MIGRATIONS`). `None` for a group that has not frozen one yet.
fn embedded_origin(group: &str) -> Option<&'static str> {
    ORIGINS.iter().find(|(g, _)| *g == group).map(|(_, sql)| *sql)
}

async fn record_migration(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    group: &str,
    id: &str,
    sql: &str,
) -> anyhow::Result<()> {
    sqlx::query("INSERT INTO weft_migration (group_name, id, checksum) VALUES ($1, $2, $3)")
        .bind(group)
        .bind(id)
        .bind(checksum(sql))
        .execute(&mut **tx)
        .await?;
    Ok(())
}

/// Where the schema's history lives: this crate's own `migrations/`, one
/// directory per group holding `origin.sql` and every migration file since.
///
/// Inside the crate rather than at the repo root so it travels wherever the
/// crate does. The build script EMBEDS everything in it (`MIGRATIONS`,
/// `ORIGINS`), so the runtime never reads the disk; only the generator
/// tooling below writes here.
#[cfg(feature = "db-tests")]
pub fn migrations_root() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations")
}

#[cfg(feature = "db-tests")]
fn group_dir(group: &SchemaGroup) -> std::path::PathBuf {
    migrations_root().join(group.name)
}

/// Write `origin.sql` for any group that has none yet.
///
/// A group's origin is its canonical DDL frozen on the day the group shipped,
/// and it is what [`replay_from_origin`] starts an old database from. It is
/// written once, by this function, and never edited again: editing it would
/// rewrite the past that every real database actually lived through.
///
/// Run it after adding a new group. It reports what it wrote.
#[cfg(feature = "db-tests")]
pub fn write_missing_origins(groups: &[&SchemaGroup]) -> anyhow::Result<Vec<String>> {
    let mut written = Vec::new();
    for group in groups {
        let dir = group_dir(group);
        let path = dir.join("origin.sql");
        if path.exists() {
            continue;
        }
        std::fs::create_dir_all(&dir)?;
        let mut body = String::new();
        for stmt in group.ddl {
            body.push_str(stmt.trim());
            if !stmt.trim_end().ends_with(';') {
                body.push(';');
            }
            body.push('\n');
        }
        std::fs::write(&path, body)?;
        written.push(path.display().to_string());
    }
    Ok(written)
}

/// Build the schema the way an existing database reaches it: every group's
/// frozen origin, then every migration file across every group in id order.
///
/// Ids are timestamps, so one global ordering across groups replays the
/// changes in the order they were actually written, which is what an old
/// database booting today does.
#[cfg(feature = "db-tests")]
pub async fn replay_from_origin(
    pool: &PgPool,
    groups: &[&SchemaGroup],
    include_drafts: bool,
) -> anyhow::Result<()> {
    for group in groups {
        // Embedded at build time, like the migrations: the replay must
        // start from the SAME frozen text the boot checksums, so an
        // on-disk edit cannot make the test and the boot disagree.
        let sql = embedded_origin(group.name).ok_or_else(|| {
            anyhow::anyhow!(
                "schema group '{}' has no embedded origin.sql. If the file was just \
                 written (a new group), rebuild so the build script embeds it, then \
                 re-run; the file must be committed with the PR that adds the group.",
                group.name
            )
        })?;
        if !sql.trim().is_empty() {
            sqlx::raw_sql(sql)
                .execute(pool)
                .await
                .map_err(|e| anyhow::anyhow!("origin of '{}' failed: {e}", group.name))?;
        }
    }
    let names: std::collections::HashSet<&str> = groups.iter().map(|g| g.name).collect();
    let mut all: Vec<&Migration> = MIGRATIONS
        .iter()
        .filter(|m| names.contains(m.group) && (include_drafts || !m.draft))
        .collect();
    all.sort_by_key(|m| m.id);
    for m in all {
        let group = m.group;
        sqlx::raw_sql(m.sql)
            .execute(pool)
            .await
            .map_err(|e| anyhow::anyhow!("migration '{}' of '{group}' failed: {e}", m.id))?;
    }
    Ok(())
}

/// One thing a schema holds: a column, an index, a constraint, a trigger, a
/// function, an enum type. Keyed so the same thing on two sides compares
/// equal, and carrying the table it belongs to so a change can be filed
/// under the right group.
#[derive(sqlx::FromRow, Clone)]
pub struct Thing {
    kind: String,
    table: String,
    name: String,
    body: String,
}

impl std::fmt::Display for Thing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}.{}: {}", self.kind, self.table, self.name, self.body)
    }
}

/// Everything the database currently holds, ready to compare against the same
/// reading taken after the schema was built the other way. Generator
/// tooling only; the boot's shape check reads through [`read_things`].
#[cfg(feature = "db-tests")]
pub async fn read_schema(pool: &PgPool) -> anyhow::Result<Vec<Thing>> {
    let mut conn = pool.acquire().await?;
    read_things(&mut conn, "public").await
}

/// [`read_schema`], on one connection and in one named schema, so the
/// boot's shape verification can read a scratch schema inside its own
/// transaction.
async fn read_things(
    conn: &mut sqlx::PgConnection,
    schema: &str,
) -> anyhow::Result<Vec<Thing>> {
    let mut out: Vec<Thing> = Vec::new();
    for sql in THING_QUERIES {
        out.extend(sqlx::query_as::<_, Thing>(sql).bind(schema).fetch_all(&mut *conn).await?);
    }
    // Postgres prints definitions schema-qualified, and each reading is
    // taken in one schema, so the prefix is noise on both sides.
    for thing in &mut out {
        thing.body = thing.body.replace(&format!("{schema}."), "");
    }
    out.sort_by(|a, b| (&a.kind, &a.table, &a.name).cmp(&(&b.kind, &b.table, &b.name)));
    Ok(out)
}

const THING_QUERIES: &[&str] = &[
    // Types come from `format_type`, never information_schema's
    // `data_type`: the latter renders an array column as the literal
    // word ARRAY and an enum as USER-DEFINED, which the planner would
    // paste into DDL as invalid SQL. `format_type` yields `uuid[]`,
    // `mood`, `character varying(30)` uniformly.
    // Identity and generated columns carry their marker in the body: a
    // generated column's expression lives in pg_attrdef exactly like a
    // default, so without the markers the planner would render it as a
    // DEFAULT clause, which is different (and wrong) SQL.
    "SELECT 'column' AS kind, rel.relname AS \"table\", a.attname AS name, \
         format('%s null=%s default=%s%s', \
             format_type(a.atttypid, a.atttypmod), \
             CASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END, \
             CASE WHEN a.attgenerated = 's' THEN '-' \
                  ELSE coalesce(pg_get_expr(d.adbin, d.adrelid), '-') END, \
             CASE WHEN a.attidentity = 'a' THEN ' identity=ALWAYS' \
                  WHEN a.attidentity = 'd' THEN ' identity=DEFAULT' \
                  WHEN a.attgenerated = 's' \
                      THEN ' generated=' || pg_get_expr(d.adbin, d.adrelid) \
                  ELSE '' END) AS body \
     FROM pg_attribute a \
     JOIN pg_class rel ON rel.oid = a.attrelid \
     JOIN pg_namespace n ON n.oid = rel.relnamespace \
     LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum \
     WHERE n.nspname = $1 AND rel.relkind = 'r' \
       AND a.attnum > 0 AND NOT a.attisdropped \
       AND rel.relname NOT IN ('weft_schema_stamp', 'weft_migration')",
    "SELECT 'index' AS kind, tablename AS \"table\", indexname AS name, indexdef AS body \
     FROM pg_indexes WHERE schemaname = $1 \
       AND tablename NOT IN ('weft_schema_stamp', 'weft_migration')",
    "SELECT 'constraint' AS kind, rel.relname AS \"table\", c.conname AS name, \
         pg_get_constraintdef(c.oid) AS body \
     FROM pg_constraint c JOIN pg_class rel ON rel.oid = c.conrelid \
     JOIN pg_namespace n ON n.oid = rel.relnamespace \
     WHERE n.nspname = $1 \
       AND rel.relname NOT IN ('weft_schema_stamp', 'weft_migration')",
    "SELECT 'trigger' AS kind, rel.relname AS \"table\", t.tgname AS name, \
         pg_get_triggerdef(t.oid) AS body \
     FROM pg_trigger t JOIN pg_class rel ON rel.oid = t.tgrelid \
     JOIN pg_namespace n ON n.oid = rel.relnamespace \
     WHERE n.nspname = $1 AND NOT t.tgisinternal",
    "SELECT 'function' AS kind, '' AS \"table\", p.proname AS name, \
         pg_get_functiondef(p.oid) AS body \
     FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = $1",
    "SELECT 'type' AS kind, '' AS \"table\", t.typname AS name, \
         string_agg(e.enumlabel, ',' ORDER BY e.enumsortorder) AS body \
     FROM pg_type t JOIN pg_enum e ON e.enumtypid = t.oid \
     JOIN pg_namespace n ON n.oid = t.typnamespace \
     WHERE n.nspname = $1 GROUP BY t.typname",
];

/// Who a planned statement belongs to, so [`file_per_group`] can file it
/// under the right group without scraping the SQL text.
#[cfg(feature = "db-tests")]
#[derive(Clone, PartialEq, Eq)]
pub enum Owner {
    /// The table the statement touches; owned by the group whose
    /// `tables` list it.
    Table(String),
    /// A table-less object (a function, an enum type); owned by the
    /// group whose DDL text mentions its name.
    Named(String),
}

/// One statement of a planned migration, with its owner.
#[cfg(feature = "db-tests")]
pub struct Planned {
    pub owner: Owner,
    pub stmt: String,
}

#[cfg(feature = "db-tests")]
fn owner_of(thing: &Thing) -> Owner {
    // A trigger files under the group whose DDL DECLARES it, not the
    // group owning the table it fires on: `worker_pod`'s liveness
    // triggers attach to `task`, and their change is `worker_pod`'s.
    if thing.table.is_empty() || thing.kind == "trigger" {
        Owner::Named(thing.name.clone())
    } else {
        Owner::Table(thing.table.clone())
    }
}

/// The SQL that turns the `before` schema into the `after` schema, one
/// statement per entry, each carrying its owner.
///
/// This is a first draft for a person to read, not an oracle. It reads
/// what Postgres itself says the two schemas hold, so every statement is
/// exact and runnable; the destructive and lossy ones (a dropped table, a
/// changed column type) carry a comment above them saying what they cost,
/// so the person releasing the migration decides with the price in view.
#[cfg(feature = "db-tests")]
pub fn plan_migration(old: &[Thing], new: &[Thing]) -> Vec<Planned> {
    let key = |t: &Thing| (t.kind.clone(), t.table.clone(), t.name.clone());
    let old_by: HashMap<_, _> = old.iter().map(|t| (key(t), t.clone())).collect();
    let new_by: HashMap<_, _> = new.iter().map(|t| (key(t), t.clone())).collect();

    let mut plan: Vec<Planned> = Vec::new();
    let old_tables: std::collections::HashSet<&str> =
        old.iter().map(|t| t.table.as_str()).collect();
    let mut created: std::collections::HashSet<&str> = Default::default();
    // Indexes that BACK a constraint (a primary key, a unique): the
    // ADD CONSTRAINT creates them, so planning the index too would fail
    // on a duplicate name.
    let constraint_backed: std::collections::HashSet<(&str, &str)> = new
        .iter()
        .chain(old.iter())
        .filter(|t| t.kind == "constraint")
        .map(|t| (t.table.as_str(), t.name.as_str()))
        .collect();
    for thing in new {
        if thing.kind == "index"
            && constraint_backed.contains(&(thing.table.as_str(), thing.name.as_str()))
        {
            continue;
        }
        // A table nobody had before: its columns arrive with the CREATE
        // (emitted once, keyed on the table name); its indexes,
        // constraints and triggers still fall through to `added` so the
        // new table arrives whole, not bare.
        if !thing.table.is_empty() && !old_tables.contains(thing.table.as_str()) {
            if created.insert(thing.table.as_str()) {
                let cols: Vec<String> = new
                    .iter()
                    .filter(|t| t.kind == "column" && t.table == thing.table)
                    .map(|t| format!("    {} {}", t.name, describe_column(&t.body)))
                    .collect();
                plan.push(Planned {
                    owner: Owner::Table(thing.table.clone()),
                    stmt: format!("CREATE TABLE {} (\n{}\n);", thing.table, cols.join(",\n")),
                });
            }
            if thing.kind == "column" {
                continue;
            }
            plan.push(Planned { owner: owner_of(thing), stmt: added(thing) });
            continue;
        }
        match old_by.get(&key(thing)) {
            None => plan.push(Planned { owner: owner_of(thing), stmt: added(thing) }),
            Some(was) if was.body != thing.body => {
                plan.push(Planned { owner: owner_of(thing), stmt: changed(was, thing) })
            }
            Some(_) => {}
        }
    }
    // A table that has gone entirely: one DROP TABLE, rather than a DROP
    // COLUMN for each of its columns.
    let new_tables: std::collections::HashSet<&str> =
        new.iter().map(|t| t.table.as_str()).collect();
    let mut dropped: std::collections::HashSet<&str> = Default::default();
    for thing in old {
        if thing.kind == "index"
            && constraint_backed.contains(&(thing.table.as_str(), thing.name.as_str()))
        {
            // Its constraint's DROP (or survival) owns it.
            continue;
        }
        if !thing.table.is_empty() && !new_tables.contains(thing.table.as_str()) {
            if dropped.insert(thing.table.as_str()) {
                plan.push(Planned {
                    owner: Owner::Table(thing.table.clone()),
                    stmt: format!(
                        "-- Throws away every row in {}.\nDROP TABLE {};",
                        thing.table, thing.table
                    ),
                });
            }
            continue;
        }
        if !new_by.contains_key(&key(thing)) {
            plan.push(Planned { owner: owner_of(thing), stmt: removed(thing) });
        }
    }
    plan
}

/// The pieces of a column as Postgres reports them, parsed back out of
/// the body format the column THING query builds.
#[cfg(feature = "db-tests")]
struct ColumnParts {
    ty: String,
    not_null: bool,
    default: Option<String>,
    /// `ALWAYS` or `DEFAULT` for an identity column.
    identity: Option<String>,
    /// The generation expression of a `GENERATED ... STORED` column.
    generated: Option<String>,
}

#[cfg(feature = "db-tests")]
fn column_parts(body: &str) -> ColumnParts {
    let ty = body.split(" null=").next().unwrap_or(body).to_string();
    let not_null = body.contains("null=NO");
    let rest = body.split("default=").nth(1).unwrap_or("");
    let (default, marker) = match rest.split_once(" identity=") {
        Some((d, m)) => (d, Some((true, m))),
        None => match rest.split_once(" generated=") {
            Some((d, m)) => (d, Some((false, m))),
            None => (rest, None),
        },
    };
    ColumnParts {
        ty,
        not_null,
        default: Some(default).filter(|d| !d.is_empty() && *d != "-").map(str::to_string),
        identity: marker.filter(|(id, _)| *id).map(|(_, m)| m.to_string()),
        generated: marker.filter(|(id, _)| !id).map(|(_, m)| m.to_string()),
    }
}

/// The column clause of a CREATE TABLE, rebuilt from what Postgres
/// reports. Rendering of [`column_parts`], so there is one parser of
/// the body format.
#[cfg(feature = "db-tests")]
fn describe_column(body: &str) -> String {
    let parts = column_parts(body);
    let mut out;
    if let Some(expr) = &parts.generated {
        out = format!("{} GENERATED ALWAYS AS ({expr}) STORED", parts.ty);
    } else if let Some(kind) = &parts.identity {
        let when = if kind == "ALWAYS" { "ALWAYS" } else { "BY DEFAULT" };
        out = format!("{} GENERATED {when} AS IDENTITY", parts.ty);
    } else if let Some(serial) = serial_form(&parts) {
        // A default of nextval on the column's own sequence is how
        // Postgres reports a serial; rendering the default literally
        // would reference a sequence the CREATE never made.
        out = serial.to_string();
    } else {
        out = parts.ty.clone();
        if let Some(d) = &parts.default {
            out.push_str(&format!(" DEFAULT {d}"));
        }
    }
    if parts.not_null {
        out.push_str(" NOT NULL");
    }
    out
}

/// `bigserial`/`serial`/`smallserial` when the column is an integer
/// whose default draws from a sequence; `None` for everything else.
#[cfg(feature = "db-tests")]
fn serial_form(parts: &ColumnParts) -> Option<&'static str> {
    if !parts.default.as_deref().is_some_and(|d| d.starts_with("nextval(")) {
        return None;
    }
    match parts.ty.as_str() {
        "bigint" => Some("bigserial"),
        "integer" => Some("serial"),
        "smallint" => Some("smallserial"),
        _ => None,
    }
}

#[cfg(feature = "db-tests")]
fn added(thing: &Thing) -> String {
    match thing.kind.as_str() {
        "type" => format!(
            "CREATE TYPE {} AS ENUM ({});",
            thing.name,
            thing.body.split(',').map(|l| format!("'{l}'")).collect::<Vec<_>>().join(", ")
        ),
        "column" => {
            let parts = column_parts(&thing.body);
            let price = if parts.not_null && parts.default.is_none() && parts.identity.is_none()
            {
                "-- Fails if the table holds any row: add it nullable, backfill, \
                 then SET NOT NULL.\n"
            } else {
                ""
            };
            format!(
                "{price}ALTER TABLE {} ADD COLUMN {} {};",
                thing.table,
                thing.name,
                describe_column(&thing.body)
            )
        }
        "constraint" => {
            format!("ALTER TABLE {} ADD CONSTRAINT {} {};", thing.table, thing.name, thing.body)
        }
        _ => format!("{};", thing.body),
    }
}

#[cfg(feature = "db-tests")]
fn changed(was: &Thing, now: &Thing) -> String {
    match now.kind.as_str() {
        "function" => format!("{};", now.body),
        "type" => {
            // Postgres only ever ADDS enum labels; a removed or
            // reordered one needs a rebuild, priced in the comment.
            let old_labels: std::collections::HashSet<&str> = was.body.split(',').collect();
            let adds: Vec<String> = now
                .body
                .split(',')
                .filter(|l| !old_labels.contains(l))
                .map(|l| format!("ALTER TYPE {} ADD VALUE '{l}';", now.name))
                .collect();
            let removed_any =
                was.body.split(',').any(|l| !now.body.split(',').any(|n| n == l));
            let mut out = adds;
            if removed_any {
                out.insert(
                    0,
                    format!(
                        "-- {} lost or reordered labels ({} -> {}). Postgres cannot drop \
                         an enum value:\n-- rebuild the type (new type, cast columns, \
                         drop old) by hand here.",
                        now.name, was.body, now.body
                    ),
                );
            }
            out.join("\n")
        }
        "trigger" => format!(
            "DROP TRIGGER {} ON {};\n{};",
            now.name, now.table, now.body
        ),
        "index" => format!("DROP INDEX {};\n{};", now.name, now.body),
        "constraint" => format!(
            "ALTER TABLE {t} DROP CONSTRAINT {n};\nALTER TABLE {t} ADD CONSTRAINT {n} {b};",
            t = now.table,
            n = now.name,
            b = now.body
        ),
        _ => {
            let was = column_parts(&was.body);
            let now_p = column_parts(&now.body);
            let (old_ty, old_not_null, old_default) = (was.ty, was.not_null, was.default);
            let (ty, not_null, default) = (now_p.ty, now_p.not_null, now_p.default);
            let mut out = Vec::new();
            if was.identity != now_p.identity || was.generated != now_p.generated {
                out.push(format!(
                    "-- {}.{} changed its identity/generated form; Postgres has no \
                     single ALTER for this.\n-- Write the ALTER TABLE ... ALTER COLUMN \
                     ... [ADD|DROP|SET] GENERATED steps by hand here.",
                    now.table, now.name
                ));
            }
            if old_ty != ty {
                // The cast is the plain one. A column whose old values need
                // real conversion (a date out of a string, a unit change)
                // wants a different USING, which is why this one is spelled
                // out rather than hidden.
                out.push(format!(
                    "-- {}.{} was {old_ty}. The cast below is a plain one; if the rows \
                     already there\n-- need converting differently, change the USING.\n\
                     ALTER TABLE {} ALTER COLUMN {} TYPE {ty} USING {}::{ty};",
                    now.table, now.name, now.table, now.name, now.name
                ));
            }
            if old_default != default {
                out.push(match &default {
                    Some(d) => format!(
                        "ALTER TABLE {} ALTER COLUMN {} SET DEFAULT {d};",
                        now.table, now.name
                    ),
                    None => format!(
                        "ALTER TABLE {} ALTER COLUMN {} DROP DEFAULT;",
                        now.table, now.name
                    ),
                });
            }
            if old_not_null != not_null {
                out.push(if not_null {
                    format!(
                        "-- Fails if any row still holds a null here.\n\
                         ALTER TABLE {} ALTER COLUMN {} SET NOT NULL;",
                        now.table, now.name
                    )
                } else {
                    format!("ALTER TABLE {} ALTER COLUMN {} DROP NOT NULL;", now.table, now.name)
                });
            }
            out.join("\n")
        }
    }
}

#[cfg(feature = "db-tests")]
fn removed(thing: &Thing) -> String {
    match thing.kind.as_str() {
        "index" => format!("DROP INDEX {};", thing.name),
        "trigger" => format!("DROP TRIGGER {} ON {};", thing.name, thing.table),
        // CASCADE, because anything still hanging off it (a trigger this
        // same plan also drops) must not wedge the file on ordering.
        "function" => format!("DROP FUNCTION {} CASCADE;", thing.name),
        "type" => format!("DROP TYPE {};", thing.name),
        "constraint" => {
            format!("ALTER TABLE {} DROP CONSTRAINT {};", thing.table, thing.name)
        }
        _ => format!(
            "-- Throws away what is in {}.{}. Ship this in a later release than the one \n\
             -- that stopped reading the column, so the old pods do not fall over.\n\
             ALTER TABLE {} DROP COLUMN {};",
            thing.table, thing.name, thing.table, thing.name
        ),
    }
}

/// Split a plan into one file per group, and say where each file goes.
///
/// A statement is filed under the group that owns it: for a table, the
/// group listing it in `tables`; for a named table-less object (a
/// function, a type), the group whose DDL text declares its name.
/// Anything that matches no group is refused rather than filed somewhere
/// plausible, since a migration in the wrong group runs against
/// databases that never had the table.
///
/// A path that already exists is refused rather than overwritten:
/// silently replacing a file that may already have run somewhere would
/// break its recorded checksum with the original content gone.
///
/// Returns the group, the path to write, and the file's contents.
#[cfg(feature = "db-tests")]
pub fn file_per_group(
    groups: &[&SchemaGroup],
    plan: &[Planned],
    name: &str,
    draft: bool,
) -> anyhow::Result<Vec<(String, std::path::PathBuf, String)>> {
    let stamp = chrono::Utc::now().format("%Y%m%dT%H%M%S");
    let mut by_group: std::collections::BTreeMap<&str, Vec<&str>> = Default::default();
    for planned in plan {
        let owner = groups
            .iter()
            .find(|g| match &planned.owner {
                Owner::Table(table) => g.tables.contains(&table.as_str()),
                Owner::Named(name) => declared_names(g).iter().any(|n| n == name),
            })
            .ok_or_else(|| {
                let what = match &planned.owner {
                    Owner::Table(t) => format!("table '{t}'"),
                    Owner::Named(n) => format!("'{n}'"),
                };
                anyhow::anyhow!(
                    "no schema group owns {what}, so this cannot be filed:\n{}\n\
                     Add the table to its group's `tables`, or write the migration by hand.",
                    planned.stmt
                )
            })?;
        by_group.entry(owner.name).or_default().push(planned.stmt.as_str());
    }
    by_group
        .into_iter()
        .map(|(group, stmts)| {
            // The group is part of the id, so one release touching two
            // groups writes two DISTINCT ids: both run-orderings sort
            // by id with different tie-breaks, and identical ids would
            // let a live upgrade and the agreement replay apply a
            // paired release in opposite orders.
            let id = if draft {
                format!("draft_{stamp}_{group}_{name}")
            } else {
                format!("{stamp}_{group}_{name}")
            };
            let dir = migrations_root().join(group);
            let path =
                if draft { dir.join("drafts") } else { dir }.join(format!("{id}.sql"));
            anyhow::ensure!(
                !path.exists(),
                "{} already exists; refusing to overwrite a migration that may have \
                 run somewhere. Pick a different name.",
                path.display()
            );
            Ok((group.to_string(), path, format!("{}\n", stmts.join("\n\n"))))
        })
        .collect()
}

/// Swap a database's drafts for the released migration they collapsed into.
///
/// The database already holds the shape, so nothing runs: the draft rows go
/// and the released id is recorded as though it had. Without this, the next
/// boot would try to run the released file and fail on a column that is
/// already there.
///
/// Recording without running is only honest if the database really holds
/// the shape, so this PROVES it first: `expected` is the canonical
/// schema's reading (from the generator's throwaway build), and a live
/// database that differs (it never ran the drafts: another machine's, a
/// re-created cluster's) is refused with the differences, so it runs the
/// released file at its next boot instead of forever claiming it did.
///
/// Draft rows are cleared for EVERY group in `groups`, not only groups
/// the release wrote a file for: two drafts that cancel out produce no
/// released file, and their rows would otherwise be junk nothing cleans.
/// Refuse unless the live database holds exactly the shape `expected`
/// describes, comparing only what `groups` own (the live database holds
/// every crate's tables, and another crate's are not this release's
/// business).
#[cfg(feature = "db-tests")]
async fn assert_live_holds_shape(
    pool: &PgPool,
    groups: &[&SchemaGroup],
    expected: &[Thing],
) -> anyhow::Result<()> {
    let ours = |t: &&Thing| groups.iter().any(|g| owned_by(g, t));
    let live_all = read_schema(pool).await?;
    let live: Vec<Thing> = live_all.iter().filter(|t| ours(t)).cloned().collect();
    let expected: Vec<Thing> = expected.iter().filter(|t| ours(t)).cloned().collect();
    if let Some(diff) = diff_things(&expected, &live) {
        anyhow::bail!(
            "the live database does not hold the shape this release describes; it never \
             ran the drafts, so let it run the released file at its next boot instead of \
             recording it as done:\n{diff}"
        );
    }
    Ok(())
}

#[cfg(feature = "db-tests")]
pub async fn adopt_release(
    pool: &PgPool,
    groups: &[&SchemaGroup],
    expected: &[Thing],
    released: &[(String, String, String)],
) -> anyhow::Result<()> {
    assert_live_holds_shape(pool, groups, expected).await?;
    let mut tx = pool.begin().await?;
    for group in groups {
        sqlx::query("DELETE FROM weft_migration WHERE group_name = $1 AND id LIKE 'draft\\_%'")
            .bind(group.name)
            .execute(&mut *tx)
            .await?;
    }
    for (group, id, sql) in released {
        // Never overwrite a recorded checksum: that row is what the
        // edited-history guard compares against. A pre-existing row
        // either already matches (a re-run of the same release) or is
        // a genuine id collision, refused loudly.
        let existing: Option<(String,)> = sqlx::query_as(
            "SELECT checksum FROM weft_migration WHERE group_name = $1 AND id = $2",
        )
        .bind(group)
        .bind(id)
        .fetch_optional(&mut *tx)
        .await?;
        if let Some((stored,)) = existing {
            anyhow::ensure!(
                stored == checksum(sql),
                "migration '{id}' of '{group}' is already recorded with different \
                 content; refusing to rewrite recorded history. Pick a different name."
            );
            continue;
        }
        record_migration(&mut tx, group, id, sql).await?;
    }
    tx.commit().await?;
    Ok(())
}

/// Every draft file there is, so a release can delete them once it has
/// collapsed them.
#[cfg(feature = "db-tests")]
pub fn draft_files(groups: &[&SchemaGroup]) -> Vec<std::path::PathBuf> {
    groups
        .iter()
        .flat_map(|g| {
            let dir = group_dir(g).join("drafts");
            std::fs::read_dir(dir)
                .into_iter()
                .flatten()
                .filter_map(|e| e.ok().map(|e| e.path()))
                .filter(|p| p.extension().is_some_and(|x| x == "sql"))
                .collect::<Vec<_>>()
        })
        .collect()
}

/// What the second reading has that the first does not, and the reverse.
/// `None` when the two databases hold the same schema.
///
/// One reading is [`read_schema`]'s, so the agreement tests, the
/// migration planner, and the release verification all look at the
/// database through the same eyes. Thing by thing rather than
/// whole-dump, because the useful failure to read is the one column
/// that differs, and with multiplicity, so a thing present twice on one
/// side and once on the other is a difference.
pub fn diff_things(fresh: &[Thing], upgraded: &[Thing]) -> Option<String> {
    let count = |things: &[Thing]| {
        let mut m: std::collections::BTreeMap<String, usize> = Default::default();
        for t in things {
            *m.entry(t.to_string()).or_default() += 1;
        }
        m
    };
    let a = count(fresh);
    let b = count(upgraded);
    let mut out = String::new();
    for (line, n) in &a {
        if b.get(line).copied().unwrap_or(0) < *n {
            out.push_str(&format!("  only a fresh install has: {line}\n"));
        }
    }
    for (line, n) in &b {
        if a.get(line).copied().unwrap_or(0) < *n {
            out.push_str(&format!("  only an upgraded install has: {line}\n"));
        }
    }
    if out.is_empty() {
        None
    } else {
        Some(out)
    }
}


/// Prove one crate's canonical DDL and migration history agree: build a
/// database each way and compare what Postgres ended up holding. The
/// body of every crate's `schema_agreement` test, so the crates cannot
/// drift on what "agrees" means.
///
/// Also the enforcement point for origins: a group with no committed
/// `origin.sql` gets one WRITTEN here and the test fails naming it, so
/// the file lands in the same PR that adds the group, while canonical
/// and shipped DDL are still identical by construction. Freezing it any
/// later would fold newer canonical changes into the "past" and lose
/// their migration.
#[cfg(feature = "db-tests")]
pub async fn assert_schema_agrees(pool: &PgPool, groups: &[&SchemaGroup]) {
    let frozen = write_missing_origins(groups).expect("write missing origins");
    assert!(
        frozen.is_empty(),
        "these groups had no committed origin.sql; the files were just written, so \
         commit them with the PR that adds the groups and re-run:\n{}",
        frozen.join("\n")
    );

    apply_groups(pool, groups).await.expect("build from canonical DDL");
    let fresh = read_schema(pool).await.expect("read the fresh database");

    sqlx::raw_sql("DROP SCHEMA public CASCADE; CREATE SCHEMA public")
        .execute(pool)
        .await
        .expect("empty the database");

    // Drafts are deliberately left out of the replay, which is what
    // makes an unreleased draft fail here rather than reaching a PR.
    replay_from_origin(pool, groups, false).await.expect("replay the history");
    let upgraded = read_schema(pool).await.expect("read the upgraded database");

    if let Some(diff) = diff_things(&fresh, &upgraded) {
        panic!(
            "the canonical schema and the migration history disagree:\n{diff}\n\
             Write the migration that makes this change to an existing database \
             (./setup.sh --migration <name>) and RELEASE it \
             (./setup.sh --migration <name> --release) so it lands under \
             migrations/<group>/. A draft alone stays on your machine \
             (gitignored), so this test cannot see it until it is released."
        );
    }
}

/// Prove every group's canonical DDL is scratch-buildable by the
/// boot's shape check: build the schema, move every stamp, and boot
/// again. The re-boot rebuilds the DDL in the scratch schema, compares
/// it against the live one, and restamps. The shared body of each
/// crate's second `schema_agreement` test, so the first place a
/// non-scratch-buildable statement (a hardcoded `public.`, a reference
/// outside the group set) surfaces is CI, never a production boot.
#[cfg(feature = "db-tests")]
pub async fn assert_shape_check_restamps(pool: &PgPool, groups: &[&SchemaGroup]) {
    apply_groups(pool, groups).await.expect("build from canonical DDL");
    sqlx::query("UPDATE weft_schema_stamp SET fingerprint = 'moved'")
        .execute(pool)
        .await
        .expect("move every stamp");
    apply_groups(pool, groups).await.expect("the shape check verifies and restamps");
    let stale: Vec<(String,)> = sqlx::query_as(
        "SELECT group_name FROM weft_schema_stamp WHERE fingerprint = 'moved'",
    )
    .fetch_all(pool)
    .await
    .expect("read stamps");
    assert!(
        stale.is_empty(),
        "these groups were not restamped by the shape check: {stale:?}"
    );
}

/// The whole `--migration` generator, shared by every crate's
/// `schema_migration` example: freeze missing origins, build the schema
/// the databases out there hold (origins + released migrations + this
/// machine's drafts), build the schema the code declares, diff them,
/// file the difference, and on `--release` collapse the drafts and
/// record the released file on the live database (after PROVING it
/// holds the shape).
///
/// Reads the same env contract `./setup.sh --migration` provides:
/// `DATABASE_URL` (a throwaway Postgres to build schemas in), the
/// change name in argv, `--release`, and `WEFT_LIVE_DATABASE_URL` when
/// releasing.
#[cfg(feature = "db-tests")]
pub async fn write_migration(groups: &[&SchemaGroup]) -> anyhow::Result<()> {
    use anyhow::Context;
    let name = std::env::args().nth(1).context(
        "name the change, as it will be part of the file name: \
         ./setup.sh --migration add_owner",
    )?;
    anyhow::ensure!(
        !name.is_empty()
            && name.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'),
        "'{name}' cannot name a migration: the name becomes part of a file under \
         migrations/, so it must match [a-z0-9_]+"
    );
    let release = std::env::args().any(|a| a == "--release");
    let url = std::env::var("DATABASE_URL")
        .context("set DATABASE_URL to a throwaway Postgres this can build schemas in")?;

    // A group that has never shipped a change has no history to replay
    // from yet; freeze its DDL now. The frozen file is embedded by the
    // NEXT build, so this run cannot continue past it.
    let frozen = write_missing_origins(groups)?;
    if !frozen.is_empty() {
        for path in &frozen {
            println!("froze {path}");
        }
        anyhow::bail!(
            "origin file(s) were just written; rebuild (re-run the same command) so \
             they are embedded, and commit them with the group they freeze"
        );
    }

    let pool = PgPool::connect(&url).await?;
    // Releasing compares against what a database WITHOUT your drafts
    // holds, so the one file it writes carries the whole change rather
    // than the last slice of it.
    sqlx::raw_sql("DROP SCHEMA public CASCADE; CREATE SCHEMA public").execute(&pool).await?;
    replay_from_origin(&pool, groups, !release).await.context("build what exists")?;
    let before = read_schema(&pool).await?;

    sqlx::raw_sql("DROP SCHEMA public CASCADE; CREATE SCHEMA public").execute(&pool).await?;
    apply_groups(&pool, groups).await.context("build what the code declares")?;
    let after = read_schema(&pool).await?;

    let plan = plan_migration(&before, &after);
    if plan.is_empty() {
        println!("nothing changed: every group's migrations already reach its CREATE TABLE");
        return Ok(());
    }

    // On a release, prove the live database holds the shape BEFORE any
    // file is written or draft deleted, so a refusal leaves everything
    // as it was.
    let live = if release {
        let live_url = std::env::var("WEFT_LIVE_DATABASE_URL").context(
            "releasing needs WEFT_LIVE_DATABASE_URL, the database you have been \
             working against, so its drafts can be swapped for the released file",
        )?;
        Some(PgPool::connect(&live_url).await?)
    } else {
        None
    };

    if let Some(live) = &live {
        // Prove it BEFORE the file is written, so a refusal leaves the
        // working tree untouched (adopt_release checks again on its
        // own, for any caller that skips this path).
        assert_live_holds_shape(live, groups, &after).await?;
    }

    let written = file_per_group(groups, &plan, &name, !release)?;
    let mut released = Vec::new();
    for (group, path, body) in &written {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        std::fs::write(path, body)?;
        println!("wrote {}", path.display());
        let id = path.file_stem().expect("a named file").to_string_lossy().to_string();
        released.push((group.clone(), id, body.clone()));
    }

    if let Some(live) = live {
        // The live database already holds the shape the released file
        // describes (verified inside), so it is recorded rather than
        // run, and the drafts it collapsed are cleared. Draft FILES go
        // only after the adopt succeeds.
        adopt_release(&live, groups, &after, &released).await?;
        for path in draft_files(groups) {
            std::fs::remove_file(&path)?;
        }
        println!("your database now records the released migration instead of the drafts");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    // Layer-1 tests for the pure parts: the fingerprint, and which migrations
    // a database still owes.
    use std::collections::HashMap;

    use super::{checksum, edited_after_running, fingerprint, pending, Migration};

    static A: Migration = Migration { group: "g", id: "20260101_a", draft: false, sql: "SELECT 1" };
    static B: Migration = Migration { group: "g", id: "20260202_b", draft: false, sql: "SELECT 2" };
    static C: Migration = Migration { group: "g", id: "20260303_c", draft: false, sql: "SELECT 3" };

    #[test]
    fn same_ddl_same_fingerprint() {
        let a = fingerprint(&["CREATE TABLE t (id INT)", "CREATE INDEX i ON t(id)"]);
        let b = fingerprint(&["CREATE TABLE t (id INT)", "CREATE INDEX i ON t(id)"]);
        assert_eq!(a, b);
        // Hex sha256: 64 lowercase hex chars.
        assert_eq!(a.len(), 64);
        assert!(a.chars().all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()));
    }

    #[test]
    fn changed_ddl_changes_fingerprint() {
        let base = fingerprint(&["CREATE TABLE t (id INT)"]);
        let edited = fingerprint(&["CREATE TABLE t (id INT, name TEXT)"]);
        assert_ne!(base, edited);
    }

    #[test]
    fn a_non_identifier_name_is_refused() {
        let bad = super::SchemaGroup {
            name: "worker'; DROP TABLE task; --",
            tables: &["worker_pod"],
            ddl: &["CREATE TABLE IF NOT EXISTS worker_pod (id INT)"],
            seed: &[],
        };
        let err = super::validate_identifiers(&[&bad]).expect_err("a quoted name must refuse");
        assert!(err.to_string().contains("non-identifier"), "{err}");
        let good = super::SchemaGroup {
            name: "worker_pod",
            tables: &["worker_pod", "infra_owner2"],
            ddl: &[],
            seed: &[],
        };
        super::validate_identifiers(&[&good]).expect("plain identifiers pass");
    }

    #[test]
    fn statement_boundaries_matter() {
        // The per-statement newline separator keeps ["ab"] distinct from
        // ["a", "b"]: concatenation without a separator would collide.
        let joined = fingerprint(&["ab"]);
        let split = fingerprint(&["a", "b"]);
        assert_ne!(joined, split);
    }

    #[test]
    fn pending_skips_what_ran_and_sorts_the_rest() {
        // Declared out of order, and one already applied: the two that are
        // left come back oldest first whatever order they were written in.
        let declared = [&C, &A, &B];
        let applied = HashMap::from([(B.id.to_string(), checksum(B.sql))]);
        let ids: Vec<&str> = pending(&declared, &applied).iter().map(|m| m.id).collect();
        assert_eq!(ids, vec![A.id, C.id]);
    }

    #[test]
    fn pending_on_a_database_that_has_run_nothing_is_everything() {
        let ids: Vec<&str> =
            pending(&[&A, &B], &HashMap::new()).iter().map(|m| m.id).collect();
        assert_eq!(ids, vec![A.id, B.id]);
    }

    #[test]
    fn an_edited_migration_is_caught_and_an_untouched_one_is_not() {
        static A_EDITED: Migration =
            Migration { group: "g", id: "20260101_a", draft: false, sql: "SELECT 999" };
        let applied = HashMap::from([
            (A.id.to_string(), checksum(A.sql)),
            (B.id.to_string(), checksum(B.sql)),
        ]);
        assert_eq!(edited_after_running(&[&A_EDITED, &B], &applied), vec![A.id]);
    }

    #[test]
    fn a_migration_that_has_not_run_yet_can_still_be_edited() {
        static A_EDITED: Migration =
            Migration { group: "g", id: "20260101_a", draft: false, sql: "SELECT 999" };
        assert!(edited_after_running(&[&A_EDITED], &HashMap::new()).is_empty());
    }
}
