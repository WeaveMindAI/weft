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
//! A database that already holds what the code declares (every group
//! stamped with its current fingerprint, every table there, no migration
//! pending, its history intact) is left alone: a boot only reads, takes no
//! lock, and runs no DDL. That is every boot but the first of a new release, and it matters
//! because a boot can land on a database serving traffic: another copy of
//! the role starting under load. Running a group's DDL locks its tables
//! (a trigger is dropped and created again), so a boot that re-ran it
//! stalled every write and, under enough of them, deadlocked with them.
//!
//! Every migration comes in two parts ([`Part`]), which the generator
//! splits and the boot runs apart, with the stored JSON documents carried
//! between them (`crate::documents`): first the EXPAND work, in one
//! transaction under an advisory lock (the DDL of every group new to this
//! database, its migrations recorded as history it was born with, then
//! every PENDING expand part across every group in one global id order:
//! ids are timestamps, so this is the order the changes were actually
//! written, and what a migration referencing another group's table relies
//! on); then the documents; then the CONTRACT work, in a second such
//! transaction (every group's [`DataStep`]s still with work on the rows
//! already there, every pending contract part, in the same order, the shape
//! check below, every group's seeds). The expand part only adds, so a
//! value a document's step moves out of its JSON has somewhere to go; the
//! contract part drops and tightens, so it runs once the documents are
//! carried and nothing it takes away still holds a value. Each transaction waits a short while
//! for each table lock it needs, never behind the writes queued on it;
//! one that cannot get its locks, or meets a write in a deadlock, is
//! rolled back whole and tried again, since a release's work on a busy
//! database is exactly that.
//!
//! **A stamp that moved is refreshed on evidence, never on faith**: once
//! the contract parts ran, the canonical DDL is built into a scratch
//! schema in the contract transaction and compared, object by object,
//! with what the live schema holds. A
//! match restamps silently (the fingerprint moved with no shape change,
//! or a migration provably landed the shape). A difference is the drift
//! case: the boot fails naming the group and the exact objects that
//! differ, with the SQL to reset it for anyone iterating locally who
//! does not want to write the migration yet.

use std::collections::HashMap;

use anyhow::Context as _;
use sha2::{Digest, Sha256};

use crate::db::{Client, Conn, FromRow, Pool, Row, Transaction, Waits};
use crate::documents::Document;
use crate::schema_group::{SchemaGroup, Until};

/// One part of one change to one group's tables: the file's SQL, the group
/// it sits under, the id it is filed by, and which [`Part`] of the change
/// it is. Ids sort chronologically (`20260823T1412_add_owner`), which is
/// the order they run in within their part.
///
/// An id is recorded the moment its SQL runs, so it never runs twice, and its
/// checksum is recorded with it, so editing a file that has already run
/// somewhere fails loudly instead of leaving two databases silently apart.
#[derive(Clone, Copy)]
pub struct Migration<'a> {
    pub group: &'a str,
    /// The file's name without `.sql`: the change's id for its expand
    /// part, and the id followed by [`CONTRACT_SUFFIX`] for its contract
    /// part, so the two are recorded apart.
    pub id: &'a str,
    pub part: Part,
    /// A draft exists only on the machine that wrote it (its file is
    /// gitignored). It keeps that one database in step with a shape still
    /// being decided, and is collapsed into a released migration when the
    /// shape settles. Ids start with `draft_`, so drafts sort after every
    /// released file and run last.
    pub draft: bool,
    pub sql: &'a str,
}

/// Which half of a change a migration file holds. The generator splits
/// every change in two, and the boot runs every pending expand part, then
/// carries the stored documents (`crate::documents`), then runs every
/// pending contract part ([`apply_schema`]). The expand part only adds
/// (a table, a nullable column, an index, a function, a trigger, an enum
/// label, a rename): what a document's step moves values out to. The
/// contract part takes away and tightens (a dropped table, column, index,
/// constraint, trigger or function, a NOT NULL, a CHECK or UNIQUE on a
/// table that already holds rows), once the documents are carried and the
/// data steps moved what it takes away.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Part {
    Expand,
    Contract,
}

#[path = "migration_files.rs"]
pub mod migration_files;
pub use migration_files::CONTRACT_SUFFIX;

// Every file under `migrations/`, gathered at build time. Writing a migration
// is dropping a file in there; nothing registers it.
include!(concat!(env!("OUT_DIR"), "/migrations.rs"));

/// Refuse a group declaration the schema work could not run safely. Group
/// and table names are interpolated into the mismatch error's paste-ready
/// reset SQL, so they must be plain SQL identifiers; a stray quote would
/// hand the operator broken SQL at the worst moment. Static declarations,
/// so this fails the first boot (or release) of the build that introduced
/// the bad one.
fn validate_groups(groups: &[&SchemaGroup]) -> anyhow::Result<()> {
    for group in groups {
        for name in std::iter::once(group.name).chain(group.tables.iter().copied()) {
            anyhow::ensure!(
                is_identifier(name),
                "schema group '{}' declares a non-identifier name '{name}'; group and \
                 table names must match [a-z_][a-z0-9_]*",
                group.name
            );
        }
        // The guard's own bookkeeping tables are excluded from every
        // shape reading (see THING_QUERIES), so a group table by
        // one of their names would be invisible to the drift check.
        for table in group.tables {
            anyhow::ensure!(
                !BOOKKEEPING.contains(table),
                "schema group '{}' declares '{table}', which is the schema guard's \
                 own bookkeeping table; pick another name",
                group.name
            );
        }
        // A data step's table is spliced into the lock it takes, and a
        // step only ever finds work on a table of its own group: on any
        // other (a typo, another group's table) it would find none on
        // every boot, and the contract part would then drop what it
        // was meant to carry.
        for step in group.data_steps {
            for name in [step.table, step.until.name()] {
                anyhow::ensure!(
                    is_identifier(name),
                    "schema group '{}' has a data step on '{name}', which is not a plain SQL identifier ([a-z_][a-z0-9_]*)",
                    group.name
                );
            }
            anyhow::ensure!(
                group.tables.contains(&step.table),
                "schema group '{}' has a data step on {}, but '{}' is not one of its tables ({}); \
                 declare the step on the group whose tables hold that column",
                group.name,
                step.target(),
                step.table,
                group.tables.join(", ")
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

/// A plain SQL identifier (`[a-z_][a-z0-9_]*`): safe to splice into SQL
/// and into the reset text an error prints.
pub(crate) fn is_identifier(name: &str) -> bool {
    !name.is_empty()
        && name.chars().next().is_some_and(|c| c.is_ascii_lowercase() || c == '_')
        && name.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

/// The guard's own tables: the stamps and the migrations it recorded, and
/// the version each column of documents is at (`crate::documents`). Left
/// out of every shape reading.
const BOOKKEEPING: &[&str] = &["weft_schema_stamp", "weft_migration", "weft_document_version"];

/// What makes the guard's own tables ([`BOOKKEEPING`]).
const BOOKKEEPING_DDL: &str = "CREATE TABLE IF NOT EXISTS weft_schema_stamp (\
         group_name TEXT PRIMARY KEY, fingerprint TEXT NOT NULL);\
     CREATE TABLE IF NOT EXISTS weft_migration (\
         group_name TEXT NOT NULL, \
         id TEXT NOT NULL, \
         checksum TEXT NOT NULL, \
         applied_at TIMESTAMPTZ NOT NULL DEFAULT now(), \
         PRIMARY KEY (group_name, id));\
     CREATE TABLE IF NOT EXISTS weft_document_version (\
         table_name TEXT NOT NULL, \
         column_name TEXT NOT NULL, \
         version INTEGER NOT NULL, \
         PRIMARY KEY (table_name, column_name))";

/// The functions, enum types, and triggers a group's DDL declares, read
/// from the CREATE statements themselves, so ownership never depends on
/// a name merely appearing somewhere in another group's text (a comment,
/// a column name).
fn declared_names(group: &SchemaGroup) -> Vec<String> {
    declared_in(group.ddl.iter().copied())
}

/// The functions, enum types, and triggers the CREATE statements of
/// `sqls` declare.
fn declared_in<'a>(sqls: impl Iterator<Item = &'a str>) -> Vec<String> {
    const HEADS: &[&str] = &[
        "create or replace function ",
        "create function ",
        "create type ",
        "create or replace trigger ",
        "create trigger ",
    ];
    let mut out = Vec::new();
    for stmt in sqls {
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
        declared_names(group).iter().any(|n| n == bare_name(&thing.name))
    } else {
        group.tables.contains(&thing.table.as_str())
    }
}

/// A thing's name as a CREATE statement declares it: a function's without
/// the arguments it is read with (`weft_door_tick(text, uuid)`).
fn bare_name(name: &str) -> &str {
    name.split_once('(').map_or(name, |(bare, _)| bare)
}

/// This group's migrations, out of every migration there is.
fn for_group<'a, 'm>(migrations: &'a [Migration<'m>], group: &str) -> Vec<&'a Migration<'m>> {
    migrations.iter().filter(|m| m.group == group).collect()
}

/// Hex sha256 over the group's DDL statements, each followed by a newline,
/// then its seeds the same way. Any textual edit to the DDL (a new column, a
/// changed default, a comment) changes the fingerprint; that is the point:
/// the stamp certifies "the database's tables were created by exactly this
/// text". The seeds are in it so a changed seed is a moved stamp, which is
/// what makes a boot run it: a boot whose stamps all match runs nothing.
fn fingerprint(group: &SchemaGroup) -> String {
    fingerprint_of(group.ddl, group.seed)
}

fn fingerprint_of(ddl: &[&str], seed: &[&str]) -> String {
    let mut hasher = Sha256::new();
    for stmt in ddl {
        hasher.update(stmt.as_bytes());
        hasher.update(b"\n");
    }
    hasher.update(b"-- seeds\n");
    for stmt in seed {
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
fn pending<'a, 'm>(
    declared: &[&'a Migration<'m>],
    applied: &HashMap<String, String>,
) -> Vec<&'a Migration<'m>> {
    let mut out: Vec<&Migration<'m>> =
        declared.iter().copied().filter(|m| !applied.contains_key(m.id)).collect();
    out.sort_by_key(|m| m.id);
    out
}

/// The released migrations a database records that this build carries no
/// file for: drafts and the origin are the database's own bookkeeping,
/// everything else must have a file or the history cannot be replayed.
fn recorded_without_file(declared: &[&Migration], applied: &HashMap<String, String>) -> Vec<String> {
    let mut out: Vec<String> = applied
        .keys()
        .filter(|id| *id != ORIGIN_ID && !id.starts_with("draft_"))
        .filter(|id| !declared.iter().any(|m| m.id == id.as_str()))
        .cloned()
        .collect();
    out.sort();
    out
}

/// A migration file that was edited after it ran somewhere, which would leave
/// two databases on different shapes with no way to tell.
fn edited_after_running<'a>(
    declared: &[&Migration<'a>],
    applied: &HashMap<String, String>,
) -> Vec<&'a str> {
    declared
        .iter()
        .filter(|m| applied.get(m.id).is_some_and(|stored| *stored != checksum(m.sql)))
        .map(|m| m.id)
        .collect()
}

/// Where the migration history starts. The history has restarted twice,
/// when weft moved off Kubernetes and when executions were renamed runs:
/// each time every group's `origin.sql` was frozen again from the canonical
/// DDL of that day and the migrations before it were deleted, so no
/// database built earlier can be carried forward. Every database built
/// since records this id (under [`HISTORY_GROUP`]) when its first group is
/// built, and one without it is refused with the way out.
const HISTORY_EPOCH: &str = "run";
const HISTORY_GROUP: &str = "_history";

/// Record the history's start on a new database, and refuse one that was
/// built before it.
async fn refuse_a_database_from_before_the_history(tx: &Transaction<'_>) -> anyhow::Result<()> {
    let (built, current): (bool, bool) = tx
        .one(
            "SELECT EXISTS (SELECT 1 FROM weft_schema_stamp), \
                    EXISTS (SELECT 1 FROM weft_migration WHERE group_name = $1 AND id = $2)",
            &[&HISTORY_GROUP, &HISTORY_EPOCH],
        )
        .await?;
    anyhow::ensure!(
        !built || current,
        "this database was built by an older weft (from before runs were renamed, or before \
         it moved off Kubernetes), and its tables cannot be carried forward. Run ./setup.sh, \
         which offers to wipe it (this deletes its projects' run history, versions and stored \
         connections; your project folders are untouched), or wipe everything weft left on \
         this machine with scripts/scrub-old-install.sh."
    );
    if !built {
        tx.execute(
            "INSERT INTO weft_migration (group_name, id, checksum) VALUES ($1, $2, '') ON CONFLICT DO NOTHING",
            &[&HISTORY_GROUP, &HISTORY_EPOCH],
        )
        .await?;
    }
    Ok(())
}

/// Bring `pool`'s schema up to what `groups` describe, and its stored
/// documents to the shape this build reads: the boot.
///
/// Three steps, in this order (see [`Part`]): the expand work, in one
/// transaction under the advisory lock (every group new to the database
/// built from its canonical DDL, every pending expand part run); then
/// every column of `documents` carried to today's shape
/// (`crate::documents`); then the contract work, in one transaction under
/// the lock (every [`DataStep`] that still has work run, every
/// pending contract part run, every moved stamp proved
/// against the shape the database now holds, every seed). One transaction
/// each, because `IF NOT EXISTS` is idempotent but not concurrency-safe in
/// Postgres (two backends racing the same CREATE on a fresh database both
/// pass the existence check, then one fails on a duplicate catalog key),
/// so concurrent boots serialize and the losers see the tables present,
/// and because a stamp is then never written for DDL that did not run. A
/// database that already holds everything takes no lock and runs nothing.
pub async fn apply_schema(pool: &Pool, groups: &[&SchemaGroup], documents: &[&Document]) -> anyhow::Result<()> {
    apply_schema_with(pool, groups, documents, MIGRATIONS).await
}

/// [`apply_schema`], against a migration set the caller names. The boot
/// always uses every migration there is; a test names its own.
pub async fn apply_schema_with(
    pool: &Pool,
    groups: &[&SchemaGroup],
    documents: &[&Document],
    migrations: &[Migration<'static>],
) -> anyhow::Result<()> {
    validate_groups(groups)?;
    let holds = holds_everything(pool, groups, migrations).await?;
    if !holds {
        while_contended(|| expand_under_lock(pool, groups, migrations)).await?;
    }
    crate::documents::apply_documents(pool, documents).await.context("carry the stored documents to today's shape")?;
    if !holds {
        while_contended(|| contract_under_lock(pool, groups, migrations)).await?;
    }
    Ok(())
}

/// [`apply_schema`] with no stored documents: a group's tables alone, as a
/// test or a role that keeps no documents builds them.
pub async fn apply_groups(pool: &Pool, groups: &[&SchemaGroup]) -> anyhow::Result<()> {
    apply_schema_with(pool, groups, &[], MIGRATIONS).await
}

/// [`apply_groups`], against a migration set the caller names.
pub async fn apply_groups_with(
    pool: &Pool,
    groups: &[&SchemaGroup],
    migrations: &[Migration<'static>],
) -> anyhow::Result<()> {
    apply_schema_with(pool, groups, &[], migrations).await
}

/// Run `work` (one transaction of schema work), and again, waiting longer
/// each time, while it gives way to the writes holding its tables
/// ([`contended`]), for at most [`CONTENDED_FOR`].
pub(crate) async fn while_contended<T, F, Fut>(mut work: F) -> anyhow::Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<T>>,
{
    let started = std::time::Instant::now();
    let mut wait = CONTENDED_FIRST;
    loop {
        match work().await {
            Ok(done) => return Ok(done),
            Err(e) if contended(&e) && started.elapsed() < CONTENDED_FOR => {
                tracing::warn!(
                    target: "weft_task_store::schema_guard",
                    error = %format!("{e:#}"),
                    retry_in_ms = wait.as_millis() as u64,
                    "the schema's changes met writes holding their tables; trying again"
                );
                tokio::time::sleep(wait).await;
                wait = (wait * 2).min(CONTENDED_LONGEST);
            }
            Err(e) => return Err(e),
        }
    }
}

/// How long a release's schema work waits for one table lock before it
/// gives way to the writes queued behind it, and how long, in all, it keeps
/// trying again before the boot fails naming the contention.
pub(crate) const LOCK_WAIT: &str = "5s";
const CONTENDED_FIRST: std::time::Duration = std::time::Duration::from_millis(500);
const CONTENDED_LONGEST: std::time::Duration = std::time::Duration::from_secs(10);
const CONTENDED_FOR: std::time::Duration = std::time::Duration::from_secs(180);

/// Whether `e` is the database giving up a lock (its wait ran out, or it
/// broke a deadlock with a write): the whole transaction rolled back and can
/// be tried again as it was.
fn contended(e: &anyhow::Error) -> bool {
    e.chain().any(|cause| {
        cause
            .downcast_ref::<crate::db::Error>()
            .and_then(|e| e.code())
            .is_some_and(|code| code == "55P03" || code == "40P01")
    })
}

/// Whether the database already holds everything `groups` and `migrations`
/// declare, read without a lock: every group stamped with its current
/// fingerprint, every table there, no migration pending, and its history
/// intact (what ran
/// unedited, nothing recorded this build has no file for, the frozen origin
/// recorded as built). Anything else, including a history this build
/// refuses, is the locked path's to do or to explain.
async fn holds_everything(pool: &Pool, groups: &[&SchemaGroup], migrations: &[Migration<'static>]) -> anyhow::Result<bool> {
    let ledgers: bool = pool
        .scalar(
            "SELECT to_regclass('weft_schema_stamp') IS NOT NULL AND to_regclass('weft_migration') IS NOT NULL \
             AND to_regclass('weft_document_version') IS NOT NULL",
            &[],
        )
        .await?;
    if !ledgers {
        return Ok(false);
    }
    let stamps: HashMap<String, String> =
        pool.all::<(String, String)>("SELECT group_name, fingerprint FROM weft_schema_stamp", &[]).await?.into_iter().collect();
    let mut applied: HashMap<String, HashMap<String, String>> = HashMap::new();
    for (group, id, sum) in pool.all::<(String, String, String)>("SELECT group_name, id, checksum FROM weft_migration", &[]).await? {
        applied.entry(group).or_default().insert(id, sum);
    }
    if !applied.get(HISTORY_GROUP).is_some_and(|ids| ids.contains_key(HISTORY_EPOCH)) {
        return Ok(false);
    }
    if !missing_tables(pool, groups).await?.is_empty() {
        return Ok(false);
    }
    let none = HashMap::new();
    Ok(groups.iter().all(|group| {
        let applied = applied.get(group.name).unwrap_or(&none);
        let mine = for_group(migrations, group.name);
        stamps.get(group.name).is_some_and(|stamped| *stamped == fingerprint(group))
            && pending(&mine, applied).is_empty()
            && edited_after_running(&mine, applied).is_empty()
            && recorded_without_file(&mine, applied).is_empty()
            && embedded_origin(group.name).is_none_or(|origin| applied.get(ORIGIN_ID) == Some(&checksum(origin)))
    }))
}

/// What resets `group` on a database someone is iterating on: its tables
/// dropped and its records gone, so the next boot builds it from the
/// current DDL.
fn reset_sql(group: &SchemaGroup) -> String {
    let mut sql = String::new();
    for table in group.tables {
        sql.push_str(&format!("DROP TABLE IF EXISTS {table} CASCADE;\n"));
    }
    sql.push_str(&format!(
        "DELETE FROM weft_schema_stamp WHERE group_name = '{}';\n\
         DELETE FROM weft_migration WHERE group_name = '{}';\n",
        group.name, group.name
    ));
    sql
}

/// The tables `groups` declare that the database does not hold.
async fn missing_tables(db: &impl Client, groups: &[&SchemaGroup]) -> anyhow::Result<Vec<String>> {
    let tables: Vec<&str> = groups.iter().flat_map(|g| g.tables.iter().copied()).collect();
    Ok(db.scalars("SELECT t FROM unnest($1::text[]) AS t WHERE to_regclass(t) IS NULL", &[&tables]).await?)
}

/// One group as a locked transaction finds it: its stamp, and the
/// migrations the database records for it.
struct GroupState<'a> {
    group: &'a SchemaGroup,
    fp: String,
    stamped: Option<String>,
    applied: HashMap<String, String>,
}

/// A transaction on `conn` holding the schema's advisory lock, whose every table
/// lock waits [`LOCK_WAIT`] at most, so the work never sits at the head of
/// a table's queue holding back the writes behind it ([`while_contended`]
/// tries again). The schema's work and the documents' carry
/// (`crate::documents`) both run in one.
///
/// Taking the advisory lock itself waits as long as another schema pass
/// (a replica booting beside this one, a release) holds it, on purpose:
/// that wait holds no table lock, so no write of the install queues behind
/// it, and the other pass ends on its own (a long carry of documents can
/// take minutes, which [`CONTENDED_FOR`] would cut short into a failed
/// boot). Only the table locks taken once it is held are bounded, which is
/// why `lock_timeout` is set after it.
pub(crate) async fn locked_tx(conn: &mut Conn) -> anyhow::Result<Transaction<'_>> {
    let tx = conn.transaction().await?;
    tx.execute("SELECT pg_advisory_xact_lock(hashtextextended('weft:schema-migrate', 0))", &[]).await?;
    tx.batch_execute(&format!("SET LOCAL lock_timeout = '{LOCK_WAIT}'")).await?;
    Ok(tx)
}

/// Read each group's state and refuse rewritten history, so nothing runs
/// before every group has been checked.
async fn group_states<'a>(
    tx: &Transaction<'_>,
    groups: &[&'a SchemaGroup],
    migrations: &[Migration<'static>],
) -> anyhow::Result<Vec<GroupState<'a>>> {
    let mut states: Vec<GroupState> = Vec::new();
    for group in groups {
        let stored: Option<String> =
            tx.scalar_opt("SELECT fingerprint FROM weft_schema_stamp WHERE group_name = $1", &[&group.name]).await?;
        let applied: HashMap<String, String> = tx
            .all::<(String, String)>("SELECT id, checksum FROM weft_migration WHERE group_name = $1", &[&group.name])
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
        // A recorded release this build carries no file for is history
        // this build cannot replay: usually a build older than the
        // release (the daemon that was running while `--release`
        // recorded it, a branch checked out from before it), said now
        // rather than left as a row nothing reads until the two
        // histories disagree. The rows stay: the DDL they made is in
        // the database, and a build that carries the file is the one
        // thing that makes both agree again.
        let stray = recorded_without_file(&mine, &applied);
        anyhow::ensure!(
            stray.is_empty(),
            "schema group '{}': this database records migration(s) {} that this build has \
             no file for. Either this build is older than the release (rebuild from the \
             tree that carries the file with ./setup.sh, or check out the branch that does), \
             or a release was interrupted before writing its file: `./setup.sh --migration \
             <name> --release` tells the two apart and says what to do. A `<file>.sql.releasing` \
             left beside where the file belongs is a release interrupted after its commit: \
             rename it to `<file>.sql` to finish it.",
            group.name,
            stray.join(", ")
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
                    record_migration(tx, group.name, ORIGIN_ID, embedded).await?;
                }
                None => {}
            }
        }
        states.push(GroupState {
            group,
            fp: fingerprint(group),
            stamped: stored,
            applied,
        });
    }
    Ok(states)
}

/// Run, in id order across every group, the pending migrations of `part`
/// of the groups `states` holds stamped, recording each. Ids are
/// timestamps, so this is the order the changes were written, the same
/// order the agreement test replays, and the order a migration touching
/// another group's table relies on.
async fn run_pending(
    tx: &Transaction<'_>,
    states: &[GroupState<'_>],
    migrations: &[Migration<'static>],
    part: Part,
) -> anyhow::Result<()> {
    let mut todo: Vec<(&SchemaGroup, &Migration<'static>)> = Vec::new();
    for st in states {
        if st.stamped.is_none() {
            continue;
        }
        for m in pending(&for_group(migrations, st.group.name), &st.applied) {
            if m.part == part {
                todo.push((st.group, m));
            }
        }
    }
    todo.sort_by_key(|(_, m)| m.id);
    for (group, m) in todo {
        tx.batch_execute(m.sql).await.map_err(|e| {
            anyhow::Error::from(e).context(format!("schema group '{}': migration '{}' failed", group.name, m.id))
        })?;
        record_migration(tx, group.name, m.id, m.sql).await?;
    }
    Ok(())
}

/// Run every [`DataStep`] of `groups` that still has work, in the
/// transaction the contract parts then run in: its table is locked first,
/// as the contract part would lock it, so nothing written to the table
/// after the step runs is left out of it.
async fn run_data_steps(tx: &impl Client, groups: &[&SchemaGroup]) -> anyhow::Result<()> {
    for group in groups {
        for step in group.data_steps {
            let has_work = match step.until {
                Until::Dropped(column) => column_not_null(tx, step.table, column).await?.is_some(),
                Until::NotNull(column) => column_not_null(tx, step.table, column).await? == Some(false),
                Until::Indexed(index) => !index_present(tx, index).await?,
            };
            if !has_work {
                continue;
            }
            tx.batch_execute(&format!("LOCK TABLE {} IN ACCESS EXCLUSIVE MODE", step.table)).await?;
            tx.batch_execute(step.sql).await.map_err(|e| {
                anyhow::Error::from(e).context(format!(
                    "schema group '{}': the data step on {} failed",
                    group.name,
                    step.target()
                ))
            })?;
        }
    }
    Ok(())
}

/// Whether `table.column` is NOT NULL: `None` when it is gone.
async fn column_not_null(db: &impl Client, table: &str, column: &str) -> anyhow::Result<Option<bool>> {
    Ok(db
        .scalar_opt(
            "SELECT attnotnull FROM pg_attribute \
             WHERE attrelid = to_regclass($1) AND attname = $2 AND attnum > 0 AND NOT attisdropped",
            &[&table, &column],
        )
        .await?)
}

/// Whether an index named `index` exists.
async fn index_present(db: &impl Client, index: &str) -> anyhow::Result<bool> {
    Ok(db.scalar("SELECT EXISTS (SELECT 1 FROM pg_class WHERE oid = to_regclass($1) AND relkind = 'i')", &[&index]).await?)
}

/// Whether `table` holds `column`.
pub(crate) async fn column_present(db: &impl Client, table: &str, column: &str) -> anyhow::Result<bool> {
    Ok(db
        .scalar(
            "SELECT EXISTS (SELECT 1 FROM pg_attribute \
             WHERE attrelid = to_regclass($1) AND attname = $2 AND attnum > 0 AND NOT attisdropped)",
            &[&table, &column],
        )
        .await?)
}

/// The expand work (see [`apply_schema`]), in one transaction under the
/// advisory lock.
async fn expand_under_lock(pool: &Pool, groups: &[&SchemaGroup], migrations: &[Migration<'static>]) -> anyhow::Result<()> {
    let mut conn = pool.get().await?;
    let tx = locked_tx(&mut conn).await?;
    tx.batch_execute(BOOKKEEPING_DDL).await?;
    refuse_a_database_from_before_the_history(&tx).await?;
    let states = group_states(&tx, groups, migrations).await?;

    // Every group that is new to this database is built from its
    // canonical DDL; its migrations (both parts) and origin are history it
    // was born with, recorded without running.
    for st in &states {
        if st.stamped.is_some() {
            continue;
        }
        for stmt in st.group.ddl {
            tx.batch_execute(stmt)
                .await
                .map_err(|e| anyhow::Error::from(e).context(format!("schema group '{}': DDL failed", st.group.name)))?;
        }
        tx.execute("INSERT INTO weft_schema_stamp (group_name, fingerprint) VALUES ($1, $2)", &[&st.group.name, &st.fp])
            .await?;
        for m in for_group(migrations, st.group.name) {
            record_migration(&tx, st.group.name, m.id, m.sql).await?;
        }
        if let Some(origin) = embedded_origin(st.group.name) {
            record_migration(&tx, st.group.name, ORIGIN_ID, origin).await?;
        }
    }

    // A group this database holds at its current shape (stamp current,
    // nothing pending) whose table is gone (dropped by hand) is built
    // again from its canonical DDL, every statement of which is idempotent
    // on what is still there (it runs on the group's other tables too, and
    // on any table its triggers sit on). A group with a migration pending
    // is not: a table its migration creates is missing until it runs, and
    // the canonical DDL is the shape after every migration, which a
    // pending one would then run on. Its missing tables are the
    // migrations' to make, or the contract work's shape check to name.
    for st in &states {
        if st.stamped.as_deref() != Some(st.fp.as_str())
            || !pending(&for_group(migrations, st.group.name), &st.applied).is_empty()
            || missing_tables(&tx, &[st.group]).await?.is_empty()
        {
            continue;
        }
        tracing::warn!(target: "weft_task_store::schema_guard", group = st.group.name, "a table of this group is gone; building it again");
        for stmt in st.group.ddl {
            tx.batch_execute(stmt).await.map_err(|e| {
                anyhow::Error::from(e).context(format!("schema group '{}': rebuilding a table that is gone failed", st.group.name))
            })?;
        }
    }

    run_pending(&tx, &states, migrations, Part::Expand).await?;
    tx.commit().await?;
    Ok(())
}

/// The contract work (see [`apply_schema`]), in one transaction under the
/// advisory lock, once the stored documents are carried.
async fn contract_under_lock(pool: &Pool, groups: &[&SchemaGroup], migrations: &[Migration<'static>]) -> anyhow::Result<()> {
    let mut conn = pool.get().await?;
    let tx = locked_tx(&mut conn).await?;
    let states = group_states(&tx, groups, migrations).await?;
    run_data_steps(&tx, groups).await?;
    run_pending(&tx, &states, migrations, Part::Contract).await?;

    // Every moved stamp is verified against the database itself, as it
    // stands once both parts of every pending migration ran: the only
    // point where the canonical DDL, which is the shape after every
    // migration, is what the database must hold. The canonical DDL is
    // built into a scratch schema inside this same transaction and
    // compared, object by object, with what the live schema holds. A group
    // whose live shape matches is restamped: a fingerprint that moved with
    // no shape change (a comment, a seed moved out of the DDL) is a
    // non-event, and a migration that landed the shape is proven rather
    // than assumed. A group whose live shape differs is the drift case,
    // whatever migrations ran: "something was pending" must never launder
    // a DDL edit nothing carried across. The expand work stands either
    // way (it only added, and is recorded); this transaction, and with it
    // every contract part it ran, rolls back.
    let moved: Vec<&GroupState> =
        states.iter().filter(|st| st.stamped.as_ref().is_some_and(|stored_fp| *stored_fp != st.fp)).collect();
    let mut mismatched: Vec<(&SchemaGroup, String)> = Vec::new();
    if !moved.is_empty() {
        tx.batch_execute(
            "DROP SCHEMA IF EXISTS weft_shape_check CASCADE; \
             CREATE SCHEMA weft_shape_check; \
             SET LOCAL search_path TO weft_shape_check",
        )
        .await?;
        // Every group in the call, so cross-group references (a trigger
        // on another group's table) resolve, in the same dependency
        // order a fresh database builds in.
        for group in groups {
            for stmt in group.ddl {
                tx.batch_execute(stmt).await.map_err(|e| {
                    anyhow::Error::from(e).context(format!("schema group '{}': DDL failed in the shape check", group.name))
                })?;
            }
        }
        tx.batch_execute("SET LOCAL search_path TO public").await?;
        let want = read_things(&tx, "weft_shape_check").await?;
        let live = read_things(&tx, "public").await?;
        for st in &moved {
            let want_g: Vec<Thing> =
                want.iter().filter(|t| owned_by(st.group, t)).cloned().collect();
            let live_g: Vec<Thing> =
                live.iter().filter(|t| owned_by(st.group, t)).cloned().collect();
            match diff_things(&want_g, "the code", &live_g, "this database") {
                Some(diff) => mismatched.push((st.group, diff)),
                None => {
                    tx.execute("UPDATE weft_schema_stamp SET fingerprint = $2 WHERE group_name = $1", &[&st.group.name, &st.fp])
                        .await?;
                }
            }
        }
        tx.batch_execute("DROP SCHEMA weft_shape_check CASCADE").await?;
    }

    // The drift bail fires BEFORE the seeds: a seed touching a drifted
    // table would otherwise fail first with a raw Postgres error and
    // swallow the one message this mechanism exists to produce.
    if !mismatched.is_empty() {
        let names: Vec<&str> = mismatched.iter().map(|(g, _)| g.name).collect();
        let mut reset = String::new();
        let mut diffs = String::new();
        for (group, diff) in &mismatched {
            diffs.push_str(&format!("group '{}':\n{diff}", group.name));
            reset.push_str(&reset_sql(group));
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
            reset
        );
    }

    // Seeds, idempotent DML, after every table exists.
    for group in groups {
        for stmt in group.seed {
            tx.batch_execute(stmt)
                .await
                .map_err(|e| anyhow::Error::from(e).context(format!("schema group '{}': seed failed", group.name)))?;
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

/// A schema history: every group's frozen origin and every migration
/// file since, drafts included.
///
/// Two of them exist. The boot and every test read the one this build
/// embedded ([`History::embedded`]): the runtime carries its own files
/// and never reads the disk. The migration generator reads today's
/// files off the disk ([`HistoryOnDisk`]), so what it writes before
/// planning (a new group's frozen origin) is part of what it plans
/// from, in the same run, with no rebuild in between.
pub struct History<'a> {
    origins: Vec<(&'a str, &'a str)>,
    migrations: Vec<Migration<'a>>,
}

impl History<'static> {
    /// What the build script embedded from `migrations/`.
    pub fn embedded() -> Self {
        History { origins: ORIGINS.to_vec(), migrations: MIGRATIONS.to_vec() }
    }
}

impl<'a> History<'a> {
    /// `group`'s frozen `origin.sql`; `None` for a group that has not
    /// frozen one yet.
    fn origin(&self, group: &str) -> Option<&'a str> {
        self.origins.iter().find(|(g, _)| *g == group).map(|(_, sql)| *sql)
    }

    pub fn migrations(&self) -> &[Migration<'a>] {
        &self.migrations
    }

    /// `group`'s released history: its frozen origin, then every released
    /// migration.
    fn released(&self, group: &str) -> Vec<&'a str> {
        self.origin(group)
            .into_iter()
            .chain(self.migrations.iter().filter(|m| m.group == group && !m.draft).map(|m| m.sql))
            .collect()
    }

    /// Every table `group`'s released history creates: how a table the
    /// canonical DDL dropped or renamed is still known to be that group's.
    fn tables_created(&self, group: &str) -> std::collections::HashSet<String> {
        let mut out = std::collections::HashSet::new();
        for sql in self.released(group) {
            let lower = sql.to_ascii_lowercase();
            let mut rest = lower.as_str();
            while let Some(at) = rest.find("create table ") {
                rest = &rest[at + "create table ".len()..];
                let name = rest.strip_prefix("if not exists ").unwrap_or(rest);
                let name: String = name.chars().take_while(|c| c.is_ascii_alphanumeric() || *c == '_').collect();
                if !name.is_empty() {
                    out.insert(name);
                }
            }
        }
        out
    }

    /// Whether `group`'s released history creates `table`.
    fn created(&self, group: &str, table: &str) -> bool {
        self.tables_created(group).contains(table)
    }

    /// Every function, type and trigger `group`'s released history
    /// declares: how an object the canonical DDL dropped is still known
    /// to be that group's.
    fn declared_names(&self, group: &str) -> Vec<String> {
        declared_in(self.released(group).into_iter())
    }

    /// Whether `group`'s released history declares the function, type or
    /// trigger `name`.
    fn declared(&self, group: &str, name: &str) -> bool {
        self.declared_names(group).iter().any(|n| n == name)
    }
}

/// The history as it is on disk right now, read through the same walk
/// the build script embeds ([`migration_files`]): what the generator
/// plans from. Owns the files' text; [`HistoryOnDisk::history`] lends it
/// out as a [`History`].
pub struct HistoryOnDisk {
    origins: Vec<(String, String)>,
    migrations: Vec<(String, String, Part, bool, String)>,
}

impl HistoryOnDisk {
    pub fn read(root: &std::path::Path) -> anyhow::Result<Self> {
        let read = |path: &std::path::Path| {
            std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))
        };
        let mut origins = Vec::new();
        let mut migrations = Vec::new();
        for group in migration_files::read_groups(root).map_err(anyhow::Error::msg)? {
            if let Some(origin) = &group.origin {
                origins.push((group.group.clone(), read(origin)?));
            }
            for file in &group.files {
                let part = if file.contract { Part::Contract } else { Part::Expand };
                migrations.push((group.group.clone(), file.id.clone(), part, file.draft, read(&file.path)?));
            }
        }
        Ok(HistoryOnDisk { origins, migrations })
    }

    pub fn history(&self) -> History<'_> {
        History {
            origins: self.origins.iter().map(|(g, sql)| (g.as_str(), sql.as_str())).collect(),
            migrations: self
                .migrations
                .iter()
                .map(|(group, id, part, draft, sql)| Migration { group, id, part: *part, draft: *draft, sql })
                .collect(),
        }
    }
}

async fn record_migration(tx: &Transaction<'_>, group: &str, id: &str, sql: &str) -> anyhow::Result<()> {
    tx.execute("INSERT INTO weft_migration (group_name, id, checksum) VALUES ($1, $2, $3)", &[&group, &id, &checksum(sql)])
        .await?;
    Ok(())
}

/// Where the schema's history lives: this crate's own `migrations/`, one
/// directory per group holding `origin.sql` and every migration file since.
///
/// Inside the crate rather than at the repo root so it travels wherever the
/// crate does, except into a worker's build, which leaves it out
/// (`crate::schema_group`). The build script EMBEDS everything in it
/// (`MIGRATIONS`, `ORIGINS`), so the runtime never reads the disk; only the
/// generator below reads it ([`HistoryOnDisk`]) and writes here.
pub fn migrations_root() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations")
}

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
pub fn write_missing_origins(groups: &[&SchemaGroup]) -> anyhow::Result<Vec<String>> {
    let mut written = Vec::new();
    for group in groups {
        let dir = group_dir(group);
        let path = dir.join(format!("{}.sql", migration_files::ORIGIN_STEM));
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
pub async fn replay_from_origin(
    pool: &Pool,
    history: &History<'_>,
    groups: &[&SchemaGroup],
    include_drafts: bool,
) -> anyhow::Result<()> {
    replay_origins(pool, history, groups).await?;
    replay_migrations(pool, history, groups, |m| include_drafts || !m.draft).await
}

/// Every group's frozen `origin.sql`: the database each group was on the
/// day it shipped, before any migration ran.
pub async fn replay_origins(pool: &Pool, history: &History<'_>, groups: &[&SchemaGroup]) -> anyhow::Result<()> {
    for group in groups {
        // A test replays the embedded history, the SAME frozen text the
        // boot checksums, so an on-disk edit cannot make the test and the
        // boot disagree. The generator freezes every missing origin
        // before it reads the disk, so it never lands here.
        let sql = history.origin(group.name).ok_or_else(|| {
            anyhow::anyhow!(
                "schema group '{}' has no origin.sql in the history this replays. If the \
                 file was just written (a new group), the build that embeds it has not run \
                 yet; the file must be committed with the PR that adds the group.",
                group.name
            )
        })?;
        if !sql.trim().is_empty() {
            pool.batch_execute(sql).await.map_err(|e| anyhow::anyhow!("origin of '{}' failed: {e}", group.name))?;
        }
    }
    Ok(())
}

/// Run the migrations of `groups` that `keep` picks as one boot runs
/// them: every expand part, then every contract part, each in id order
/// across every group. A test that stops the history at one release (to
/// write rows the way an older build wrote them) and then plays the rest
/// picks the two halves by id.
pub async fn replay_migrations(
    pool: &Pool,
    history: &History<'_>,
    groups: &[&SchemaGroup],
    keep: impl Fn(&Migration) -> bool,
) -> anyhow::Result<()> {
    let names: std::collections::HashSet<&str> = groups.iter().map(|g| g.name).collect();
    let mut all: Vec<&Migration> = history
        .migrations()
        .iter()
        .filter(|m| names.contains(m.group) && keep(m))
        .collect();
    // As one boot runs them: every expand part, then every contract part,
    // each in id order.
    all.sort_by_key(|m| (m.part == Part::Contract, m.id));
    for m in all {
        let group = m.group;
        pool.batch_execute(m.sql).await.map_err(|e| anyhow::anyhow!("migration '{}' of '{group}' failed: {e}", m.id))?;
    }
    Ok(())
}

/// Build the database an older weft left behind, one whose history held
/// the migrations `keep` picks: every group's frozen origin, those
/// migrations run as one boot runs them, and each recorded as that weft's
/// boot recorded it, with stamps this build no longer matches (so the next
/// boot runs what is pending, carries the documents, and proves the
/// shape). What a test of an upgrade fills, the way that weft wrote its
/// rows, before booting this one on it.
pub async fn build_as_of(pool: &Pool, groups: &[&SchemaGroup], keep: impl Fn(&Migration) -> bool) -> anyhow::Result<()> {
    let history = History::embedded();
    replay_origins(pool, &history, groups).await?;
    replay_migrations(pool, &history, groups, |m| !m.draft && keep(m)).await?;
    let mut conn = pool.get().await?;
    let tx = conn.transaction().await?;
    tx.batch_execute(BOOKKEEPING_DDL).await?;
    refuse_a_database_from_before_the_history(&tx).await?;
    for group in groups {
        tx.execute("INSERT INTO weft_schema_stamp (group_name, fingerprint) VALUES ($1, 'an older weft')", &[&group.name])
            .await?;
        if let Some(origin) = history.origin(group.name) {
            record_migration(&tx, group.name, ORIGIN_ID, origin).await?;
        }
        for m in history.migrations().iter().filter(|m| m.group == group.name && !m.draft && keep(m)) {
            record_migration(&tx, group.name, m.id, m.sql).await?;
        }
    }
    tx.commit().await?;
    Ok(())
}

/// One thing a schema holds: a column, an index, a constraint, a trigger, a
/// function, an enum type. Keyed so the same thing on two sides compares
/// equal, and carrying the table it belongs to so a change can be filed
/// under the right group.
#[derive(Clone)]
pub struct Thing {
    kind: String,
    table: String,
    name: String,
    body: String,
}

impl FromRow for Thing {
    fn from_row(row: Row) -> crate::db::Result<Self> {
        Ok(Self { kind: row.try_get("kind")?, table: row.try_get("table")?, name: row.try_get("name")?, body: row.try_get("body")? })
    }
}

impl std::fmt::Display for Thing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}.{}: {}", self.kind, self.table, self.name, self.body)
    }
}

/// Everything the database currently holds, ready to compare against the same
/// reading taken after the schema was built the other way. Generator
/// tooling only; the boot's shape check reads through [`read_things`].
pub async fn read_schema(pool: &Pool) -> anyhow::Result<Vec<Thing>> {
    let conn = pool.get().await?;
    read_things(&conn, "public").await
}

/// [`read_schema`], on one connection and in one named schema, so the
/// boot's shape verification can read a scratch schema inside its own
/// transaction.
async fn read_things(db: &impl Client, schema: &str) -> anyhow::Result<Vec<Thing>> {
    let mut out: Vec<Thing> = Vec::new();
    for sql in THING_QUERIES {
        out.extend(db.all::<Thing>(sql, &[&schema]).await?);
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
       AND rel.relname NOT IN ('weft_schema_stamp', 'weft_migration', 'weft_document_version')",
    "SELECT 'index' AS kind, tablename AS \"table\", indexname AS name, indexdef AS body \
     FROM pg_indexes WHERE schemaname = $1 \
       AND tablename NOT IN ('weft_schema_stamp', 'weft_migration', 'weft_document_version')",
    "SELECT 'constraint' AS kind, rel.relname AS \"table\", c.conname AS name, \
         pg_get_constraintdef(c.oid) AS body \
     FROM pg_constraint c JOIN pg_class rel ON rel.oid = c.conrelid \
     JOIN pg_namespace n ON n.oid = rel.relnamespace \
     WHERE n.nspname = $1 \
       AND rel.relname NOT IN ('weft_schema_stamp', 'weft_migration', 'weft_document_version')",
    "SELECT 'trigger' AS kind, rel.relname AS \"table\", t.tgname AS name, \
         pg_get_triggerdef(t.oid) AS body \
     FROM pg_trigger t JOIN pg_class rel ON rel.oid = t.tgrelid \
     JOIN pg_namespace n ON n.oid = rel.relnamespace \
     WHERE n.nspname = $1 AND NOT t.tgisinternal",
    // Named with the argument TYPES that tell one overload from another, as
    // Postgres itself does: a function whose types changed is the old one
    // dropped and the new one made, and one whose types stayed (a parameter
    // renamed, a result changed) is the same function changed.
    "SELECT 'function' AS kind, '' AS \"table\", \
         p.proname || '(' || oidvectortypes(p.proargtypes) || ')' AS name, \
         pg_get_functiondef(p.oid) AS body \
     FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = $1",
    "SELECT 'type' AS kind, '' AS \"table\", t.typname AS name, \
         string_agg(e.enumlabel, ',' ORDER BY e.enumsortorder) AS body \
     FROM pg_type t JOIN pg_enum e ON e.enumtypid = t.oid \
     JOIN pg_namespace n ON n.oid = t.typnamespace \
     WHERE n.nspname = $1 GROUP BY t.typname",
    // A table written without the write-ahead log (`CREATE UNLOGGED
    // TABLE`): its rows go with a crash, which is part of what the table
    // promises, so a table that lost or gained it differs.
    "SELECT 'persistence' AS kind, rel.relname AS \"table\", 'unlogged' AS name, 'UNLOGGED' AS body \
     FROM pg_class rel JOIN pg_namespace n ON n.oid = rel.relnamespace \
     WHERE n.nspname = $1 AND rel.relkind = 'r' AND rel.relpersistence = 'u'",
];

/// Who a planned statement belongs to, so [`file_per_group`] can file it
/// under the right group without scraping the SQL text.
#[derive(Clone, PartialEq, Eq)]
pub enum Owner {
    /// The table the statement touches; owned by the group whose
    /// `tables` list it.
    Table(String),
    /// A table-less object (a function, an enum type); owned by the
    /// group whose DDL text mentions its name.
    Named(String),
}

/// One statement of a planned migration, with its owner and the part of
/// the migration it goes in.
pub struct Planned {
    pub owner: Owner,
    pub part: Part,
    pub stmt: String,
}

fn owner_of(thing: &Thing) -> Owner {
    // A trigger files under the group whose DDL DECLARES it, not the
    // group owning the table it fires on: a group may attach a trigger
    // to another group's table, and the change is the declaring group's.
    if thing.table.is_empty() || thing.kind == "trigger" {
        Owner::Named(bare_name(&thing.name).to_string())
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
///
/// A rename cannot be read off two schemas (a dropped `note` and an added
/// `title` of the same type may be one column renamed or two unrelated
/// ones), so the person names each one in `renames`; they run first, in
/// the order given, and the rest is diffed against the schema as it
/// stands after them. A constraint or index whose definition then matches
/// under another name is renamed rather than rebuilt.
///
/// A function whose arguments changed, the one function of its name on each
/// side, is the same function replaced, in the expand part
/// ([`replaced_function`]), where an add beside the old one would make
/// every call ambiguous until the contract part dropped the old.
///
/// A NOT NULL column with no default in `backfills` is added holding the
/// value given there on the rows already in its table, a default the
/// contract part then drops.
///
/// Postgres refuses to change the type of a column a trigger holds
/// (`UPDATE OF` it, or `WHEN` it), so a column whose type changes takes
/// down every trigger of its table that holds it, in its own statement,
/// and every one the canonical DDL has holding it is put back where
/// triggers are planned, after every column and function, filed with the
/// table so it runs in the same file as the change, and making first a
/// function the plan makes for it ([`Retyped`]): a trigger added holding
/// it arrives there, and one removed leaves with the take-down.
pub fn plan_migration(old: &[Thing], new: &[Thing], renames: &[Rename], backfills: &[Backfill]) -> anyhow::Result<Vec<Planned>> {
    let key = |t: &Thing| (t.kind.clone(), t.table.clone(), t.name.clone());
    let (old, mut plan) = apply_renames(old, renames)?;
    let paired = pair_renamed_objects(&old, new);
    plan.extend(paired.plan);
    // Indexes that BACK a constraint (a primary key, a unique): the
    // ADD CONSTRAINT creates them and a RENAME CONSTRAINT renames them,
    // so planning the index too would fail on a duplicate name. Read
    // before the paired constraints leave the lists below.
    let constraint_backed: std::collections::HashSet<(String, String)> = new
        .iter()
        .chain(old.iter())
        .filter(|t| t.kind == "constraint")
        .map(|t| (t.table.clone(), t.name.clone()))
        .collect();
    let backs_constraint =
        |t: &Thing| t.kind == "index" && constraint_backed.contains(&(t.table.clone(), t.name.clone()));
    let old: Vec<Thing> = old.into_iter().filter(|t| !paired.old.contains(&key(t))).collect();
    let new: Vec<Thing> = new.iter().filter(|t| !paired.new.contains(&key(t))).cloned().collect();
    let (old, new) = (old.as_slice(), new.as_slice());
    let old_by: HashMap<_, _> = old.iter().map(|t| (key(t), t.clone())).collect();
    let new_by: HashMap<_, _> = new.iter().map(|t| (key(t), t.clone())).collect();
    let old_tables: std::collections::HashSet<&str> =
        old.iter().map(|t| t.table.as_str()).collect();
    let replaced = replaced_functions(old, new);
    let retyped = Retyped::of(old, new, &old_by);
    // A column's NOT NULL constraint (Postgres names one per NOT NULL
    // column) arrives with the statement that adds the column or makes it
    // NOT NULL: one of its own would be a second, which fails, or a no-op
    // when the names happen to match.
    let implied_not_null = |thing: &Thing| {
        thing.kind == "constraint"
            && thing.body.strip_prefix("NOT NULL ").is_some_and(|column| {
                let column = ("column".to_string(), thing.table.clone(), column.trim().to_string());
                new_by.get(&column).is_some_and(|now| column_parts(&now.body).not_null)
                    && old_by.get(&column).is_none_or(|was| !column_parts(&was.body).not_null)
            })
    };
    let mut created: std::collections::HashSet<&str> = Default::default();
    // Types first: a column or a table may be made of a type added in the
    // same plan, and things otherwise come in name order of their kind.
    let types_first = new.iter().filter(|t| t.kind == "type").chain(new.iter().filter(|t| t.kind != "type"));
    for thing in types_first {
        if backs_constraint(thing) || implied_not_null(thing) {
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
                    part: Part::Expand,
                    stmt: format!("CREATE TABLE {} (\n{}\n);", thing.table, cols.join(",\n")),
                });
            }
            if thing.kind == "column" {
                continue;
            }
            // A new table holds no row, so all of it arrives in the
            // expand part.
            plan.push(Planned { owner: owner_of(thing), part: Part::Expand, stmt: added_whole(thing) });
            continue;
        }
        if retyped.puts_up(thing) {
            plan.push(Planned { owner: Owner::Table(thing.table.clone()), part: Part::Expand, stmt: retyped.put_up(thing, new, &old_by) });
            continue;
        }
        match old_by.get(&key(thing)) {
            None => match replaced.get(&key(thing)) {
                Some(was) => plan.push(Planned { owner: owner_of(thing), part: Part::Expand, stmt: replaced_function(was, thing) }),
                None => {
                    let backfill = backfills.iter().find(|b| b.table == thing.table && b.column == thing.name);
                    plan.extend(added(thing, backfill).into_iter().map(|(part, stmt)| Planned { owner: owner_of(thing), part, stmt }))
                }
            },
            Some(was) if was.body != thing.body => {
                plan.extend(changed(was, thing, &retyped).into_iter().map(|(part, stmt)| Planned { owner: owner_of(thing), part, stmt }))
            }
            Some(_) => {}
        }
    }
    // A table that has gone entirely: one DROP TABLE, rather than a DROP
    // COLUMN for each of its columns.
    let new_tables: std::collections::HashSet<&str> =
        new.iter().map(|t| t.table.as_str()).collect();
    let mut dropped: std::collections::HashSet<&str> = Default::default();
    // Removals go in three passes: triggers first (one firing on a column,
    // `UPDATE OF` it or `WHEN` it, holds that column, so the column's DROP
    // would fail while it stands), then everything else, then functions.
    let mut dropped_triggers: Vec<Planned> = Vec::new();
    let mut dropped_rest: Vec<Planned> = Vec::new();
    let mut dropped_functions: Vec<Planned> = Vec::new();
    for thing in old {
        if backs_constraint(thing) {
            // Its constraint's DROP, RENAME or survival owns it.
            continue;
        }
        if !thing.table.is_empty() && !new_tables.contains(thing.table.as_str()) {
            if dropped.insert(thing.table.as_str()) {
                plan.push(Planned {
                    owner: Owner::Table(thing.table.clone()),
                    part: Part::Contract,
                    stmt: format!(
                        "-- Throws away every row in {}.\nDROP TABLE {};",
                        thing.table, thing.table
                    ),
                });
            }
            continue;
        }
        if new_by.contains_key(&key(thing)) || replaced.values().any(|was| key(was) == key(thing)) || retyped.holds_retyped(thing) {
            continue;
        }
        // A column's NOT NULL constraint (Postgres names one per NOT NULL
        // column) leaves with the column: dropping it separately fails
        // once the DROP COLUMN has run, or is a no-op step before it.
        if thing.kind == "constraint" {
            if let Some(column) = thing.body.strip_prefix("NOT NULL ") {
                let column_key = ("column".to_string(), thing.table.clone(), column.trim().to_string());
                if !new_by.contains_key(&column_key) {
                    continue;
                }
            }
        }
        let (part, stmt) = removed(thing);
        let planned = Planned { owner: owner_of(thing), part, stmt };
        match thing.kind.as_str() {
            "trigger" => dropped_triggers.push(planned),
            "function" => dropped_functions.push(planned),
            _ => dropped_rest.push(planned),
        }
    }
    plan.extend(dropped_triggers);
    plan.extend(dropped_rest);
    // A function goes after every trigger the plan drops: its CASCADE
    // would take a trigger still hanging off it, and the trigger's own
    // DROP would then fail on a trigger that is gone.
    plan.extend(dropped_functions);
    Ok(plan)
}

/// The columns whose type changes, and the triggers taken down before
/// and put back after ([`plan_migration`]).
struct Retyped<'t> {
    /// `(table, column)` of each column whose type changes.
    columns: std::collections::HashSet<(String, String)>,
    old: &'t [Thing],
}

impl<'t> Retyped<'t> {
    fn of(old: &'t [Thing], new: &[Thing], old_by: &HashMap<(String, String, String), Thing>) -> Self {
        let columns = new
            .iter()
            .filter(|now| now.kind == "column")
            .filter(|now| {
                old_by
                    .get(&(now.kind.clone(), now.table.clone(), now.name.clone()))
                    .is_some_and(|was| column_parts(&was.body).ty != column_parts(&now.body).ty)
            })
            .map(|now| (now.table.clone(), now.name.clone()))
            .collect();
        Retyped { columns, old }
    }

    /// Whether `trigger` holds a column of its table whose type changes.
    fn holds_retyped(&self, trigger: &Thing) -> bool {
        trigger.kind == "trigger"
            && self.columns.iter().any(|(table, column)| *table == trigger.table && mentions(&trigger.body, column))
    }

    /// The statement putting `trigger` back. It goes in its table's
    /// group's file, which may run before the file of the group declaring
    /// the function it calls; so a function made in the same plan (added,
    /// or made again because its arguments changed) is made here first too.
    /// Postgres prints a function as `CREATE OR REPLACE`, so its own
    /// statement making it again, before or after this one, changes nothing.
    fn put_up(&self, trigger: &Thing, new: &[Thing], old_by: &HashMap<(String, String, String), Thing>) -> String {
        let made = trigger_function(&trigger.body).and_then(|called| {
            new.iter().find(|f| {
                // A trigger function takes no arguments, so its name read
                // back is `called()`, whatever overloads stand beside it.
                f.kind == "function" && f.name == format!("{called}()") && !old_by.contains_key(&(f.kind.clone(), f.table.clone(), f.name.clone()))
            })
        });
        let mut lines: Vec<String> = made.map(|f| format!("{};", f.body)).into_iter().collect();
        lines.push(format!("DROP TRIGGER IF EXISTS {} ON {};\n{};", trigger.name, trigger.table, trigger.body));
        lines.join("\n")
    }

    /// Whether `trigger`, as the canonical DDL has it, is put back after a
    /// retype instead of being planned on its own: it holds a retyped
    /// column, or a trigger of its name was taken down (so a change of its
    /// own, which drops it first, would find it gone).
    fn puts_up(&self, trigger: &Thing) -> bool {
        self.holds_retyped(trigger)
            || (trigger.kind == "trigger"
                && self.old.iter().any(|was| was.table == trigger.table && was.name == trigger.name && self.holds_retyped(was)))
    }

    /// `alter`, the statement retyping `table.column`, after the drop of
    /// every trigger holding the column as the database holds it. `IF
    /// EXISTS`: two columns of one table retyped in one plan take the same
    /// trigger down each in turn.
    fn around(&self, table: &str, column: &str, alter: String) -> String {
        let downs: Vec<String> = self
            .old
            .iter()
            .filter(|t| t.kind == "trigger" && t.table == table && mentions(&t.body, column))
            .map(|t| format!("DROP TRIGGER IF EXISTS {} ON {};", t.name, t.table))
            .collect();
        if downs.is_empty() {
            return alter;
        }
        let mut lines = vec!["-- Postgres changes no type of a column a trigger holds: they come down before it, and go back up once every column and function is in.".to_string()];
        lines.extend(downs);
        lines.push(alter);
        lines.join("\n")
    }
}

/// The function a trigger's definition calls, as `pg_get_triggerdef`
/// prints it (`EXECUTE FUNCTION name(...)`, or `EXECUTE PROCEDURE` from
/// an older server), unquoted.
fn trigger_function(body: &str) -> Option<String> {
    let rest = body.split_once("EXECUTE FUNCTION ").or_else(|| body.split_once("EXECUTE PROCEDURE "))?.1;
    let name = rest.split('(').next()?.trim();
    Some(match name.strip_prefix('"').and_then(|n| n.strip_suffix('"')) {
        Some(quoted) => quoted.replace("\"\"", "\""),
        None => name.to_string(),
    })
}

/// Whether `body` names `name` as a whole identifier: a trigger holding a
/// column names it in its `UPDATE OF` list or its `WHEN`. A name that
/// only happens to match (a column called like the trigger's function)
/// takes a trigger down and up for nothing, which costs nothing.
fn mentions(body: &str, name: &str) -> bool {
    let is_word = |c: char| c.is_ascii_alphanumeric() || c == '_';
    body.match_indices(name).any(|(at, _)| {
        body[..at].chars().next_back().is_none_or(|c| !is_word(c)) && body[at + name.len()..].chars().next().is_none_or(|c| !is_word(c))
    })
}

/// The functions `new` holds under arguments `old` does not, each the one
/// function of its name on both sides, keyed by the new one, with the old
/// one it replaces. Two functions of one name in the canonical DDL are
/// overloads meant to stand side by side, and are planned one by one.
fn replaced_functions(old: &[Thing], new: &[Thing]) -> HashMap<(String, String, String), Thing> {
    let only = |things: &[Thing], name: &str| -> Option<Thing> {
        let mut named = things.iter().filter(|t| t.kind == "function" && bare_name(&t.name) == name);
        match (named.next(), named.next()) {
            (Some(one), None) => Some(one.clone()),
            _ => None,
        }
    };
    new.iter()
        .filter(|t| t.kind == "function")
        .filter_map(|now| {
            let was = only(old, bare_name(&now.name))?;
            (was.name != now.name && only(new, bare_name(&now.name)).is_some())
                .then(|| ((now.kind.clone(), now.table.clone(), now.name.clone()), was))
        })
        .collect()
}

/// The statements that replace `was` with `now`, a function of the same
/// name whose arguments changed, in one go: the old one dropped (no
/// CASCADE, so anything still on it fails the migration loudly) and the
/// new one made.
fn replaced_function(was: &Thing, now: &Thing) -> String {
    format!(
        "-- {} takes other arguments now: a call with the old ones fails loudly (no such function).\n\
         DROP FUNCTION {};\n{};",
        bare_name(&now.name),
        was.name,
        now.body
    )
}

/// What a column added NOT NULL with no default holds on the rows already
/// in its table, given by the one who knows (a document's version column:
/// `crate::documents::Document::backfill`). The migration adds it with
/// this as its default, so every row there takes it, and the contract
/// part drops the default, so a write that leaves the column out fails.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Backfill {
    pub table: String,
    pub column: String,
    /// SQL for the value.
    pub value: String,
}

/// One rename the person releasing a migration names, because no diff
/// can tell a renamed column from a dropped one and an added one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Rename {
    /// `--rename old_table=new_table`
    Table { from: String, to: String },
    /// `--rename table.old_column=new_column`
    Column { table: String, from: String, to: String },
}

impl std::str::FromStr for Rename {
    type Err = anyhow::Error;
    fn from_str(spec: &str) -> anyhow::Result<Self> {
        let valid = |s: &str| !s.is_empty() && s.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
        let (left, to) = spec.split_once('=').ok_or_else(|| {
            anyhow::anyhow!("'{spec}' is no rename: write table.old_column=new_column, or old_table=new_table")
        })?;
        let rename = match left.split_once('.') {
            Some((table, from)) => Rename::Column { table: table.into(), from: from.into(), to: to.into() },
            None => Rename::Table { from: left.into(), to: to.into() },
        };
        let names: Vec<&str> = match &rename {
            Rename::Table { from, to } => vec![from, to],
            Rename::Column { table, from, to } => vec![table, from, to],
        };
        anyhow::ensure!(names.iter().all(|n| valid(n)), "'{spec}': names are lowercase letters, digits and _");
        Ok(rename)
    }
}

/// `old` as it reads once `renames` have run, in order, and the RENAME
/// statements that get it there. A rename naming something `old` does
/// not hold is refused: it is a typo, and running past it would plan the
/// drop and add the rename was meant to prevent.
fn apply_renames(old: &[Thing], renames: &[Rename]) -> anyhow::Result<(Vec<Thing>, Vec<Planned>)> {
    let mut things = old.to_vec();
    let mut plan = Vec::new();
    for rename in renames {
        match rename {
            Rename::Table { from, to } => {
                anyhow::ensure!(
                    things.iter().any(|t| t.table == *from),
                    "--rename {from}={to}: there is no table '{from}' to rename"
                );
                // A serial column's sequence keeps its name when its
                // table is renamed; a fresh database names it after the
                // new table, so it is renamed along.
                let sequences: Vec<(String, String)> = things
                    .iter()
                    .filter(|t| t.kind == "column" && t.table == *from)
                    .filter_map(|t| owned_sequence_of(t, &format!("{from}_{}_seq", t.name)).map(|seq| (seq, format!("{to}_{}_seq", t.name))))
                    .collect();
                for thing in &mut things {
                    if thing.table == *from {
                        thing.table = to.clone();
                    }
                    thing.body = replace_ident(&thing.body, from, to);
                    for (was, now) in &sequences {
                        thing.body = replace_ident(&thing.body, was, now);
                    }
                }
                plan.push(Planned {
                    owner: Owner::Table(to.clone()),
                    part: Part::Expand,
                    stmt: format!("ALTER TABLE {from} RENAME TO {to};"),
                });
                for (was, now) in sequences {
                    plan.push(Planned { owner: Owner::Table(to.clone()), part: Part::Expand, stmt: format!("ALTER SEQUENCE {was} RENAME TO {now};") });
                }
            }
            Rename::Column { table, from, to } => {
                anyhow::ensure!(
                    things.iter().any(|t| t.kind == "column" && t.table == *table && t.name == *from),
                    "--rename {table}.{from}={to}: table '{table}' has no column '{from}' to rename"
                );
                // The same for a serial column renamed: its sequence is
                // named after the column on a fresh database.
                let sequence = things
                    .iter()
                    .find(|t| t.kind == "column" && t.table == *table && t.name == *from)
                    .and_then(|t| owned_sequence_of(t, &format!("{table}_{from}_seq")))
                    .map(|seq| (seq, format!("{table}_{to}_seq")));
                for thing in things.iter_mut().filter(|t| t.table == *table) {
                    if thing.kind == "column" && thing.name == *from {
                        thing.name = to.clone();
                    }
                    thing.body = replace_ident(&thing.body, from, to);
                    if let Some((was, now)) = &sequence {
                        thing.body = replace_ident(&thing.body, was, now);
                    }
                }
                plan.push(Planned {
                    owner: Owner::Table(table.clone()),
                    part: Part::Expand,
                    stmt: format!("ALTER TABLE {table} RENAME COLUMN {from} TO {to};"),
                });
                if let Some((was, now)) = sequence {
                    plan.push(Planned { owner: Owner::Table(table.clone()), part: Part::Expand, stmt: format!("ALTER SEQUENCE {was} RENAME TO {now};") });
                }
            }
        }
    }
    Ok((things, plan))
}

/// `name` when `column` draws its default from the sequence of that
/// name (a serial column's own sequence, as Postgres names it).
fn owned_sequence_of(column: &Thing, name: &str) -> Option<String> {
    column_parts(&column.body)
        .default
        .is_some_and(|d| d == format!("nextval('{name}'::regclass)"))
        .then(|| name.to_string())
}

/// Columns and tables that leave while others arrive beside them: each
/// could be a rename nobody named, which the plan would carry out as a
/// drop (throwing the rows away) and an empty add. Empty when every such
/// pair is accounted for.
pub fn unnamed_renames(old: &[Thing], new: &[Thing], renames: &[Rename]) -> anyhow::Result<Vec<String>> {
    let (old, _) = apply_renames(old, renames)?;
    let tables = |things: &[Thing]| -> std::collections::BTreeSet<String> {
        things.iter().filter(|t| t.kind == "column").map(|t| t.table.clone()).collect()
    };
    let columns = |things: &[Thing], table: &str| -> std::collections::BTreeSet<String> {
        things.iter().filter(|t| t.kind == "column" && t.table == table).map(|t| t.name.clone()).collect()
    };
    let (old_tables, new_tables) = (tables(&old), tables(new));
    let mut out = Vec::new();
    let gone: Vec<&String> = old_tables.difference(&new_tables).collect();
    let came: Vec<&String> = new_tables.difference(&old_tables).collect();
    if !gone.is_empty() && !came.is_empty() {
        out.push(format!("tables {gone:?} go while {came:?} arrive"));
    }
    for table in old_tables.intersection(&new_tables) {
        let (was, now) = (columns(&old, table), columns(new, table));
        let gone: Vec<&String> = was.difference(&now).collect();
        let came: Vec<&String> = now.difference(&was).collect();
        if !gone.is_empty() && !came.is_empty() {
            out.push(format!("{table}: columns {gone:?} go while {came:?} arrive"));
        }
    }
    Ok(out)
}

/// `text` with the identifier `from` replaced by `to` wherever it stands
/// as a whole word.
fn replace_ident(text: &str, from: &str, to: &str) -> String {
    let is_ident = |c: char| c.is_ascii_alphanumeric() || c == '_';
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    let mut prev: Option<char> = None;
    while let Some(at) = rest.find(from) {
        let before = rest[..at].chars().next_back().or(prev);
        let after = rest[at + from.len()..].chars().next();
        out.push_str(&rest[..at]);
        if before.is_some_and(is_ident) || after.is_some_and(is_ident) {
            out.push_str(from);
        } else {
            out.push_str(to);
        }
        prev = from.chars().next_back();
        rest = &rest[at + from.len()..];
    }
    out.push_str(rest);
    out
}

/// What [`pair_renamed_objects`] found: the RENAME statements, and the
/// old and new things they account for.
struct Paired {
    plan: Vec<Planned>,
    old: std::collections::HashSet<(String, String, String)>,
    new: std::collections::HashSet<(String, String, String)>,
}

/// Constraints and indexes that left under one name and arrived under
/// another on the same table with the same definition (once the named
/// renames have run): renamed, never rebuilt. They hold no rows, so
/// pairing them is safe to do without being asked; a pair is made only
/// when it is the one candidate on both sides.
fn pair_renamed_objects(old: &[Thing], new: &[Thing]) -> Paired {
    fn unique_match<'a>(
        from: &Thing,
        pool: &[&'a Thing],
        same: &dyn Fn(&Thing, &Thing) -> bool,
    ) -> Option<&'a Thing> {
        let hits: Vec<&'a Thing> = pool
            .iter()
            .copied()
            .filter(|c| c.kind == from.kind && c.table == from.table && same(from, c))
            .collect();
        (hits.len() == 1).then(|| hits[0])
    }
    let key = |t: &Thing| (t.kind.clone(), t.table.clone(), t.name.clone());
    let old_keys: std::collections::HashSet<_> = old.iter().map(key).collect();
    let new_keys: std::collections::HashSet<_> = new.iter().map(key).collect();
    // An index that backs a constraint (a primary key, a unique) is
    // renamed by its constraint's RENAME.
    let backs_constraint = |t: &Thing| {
        old.iter().chain(new.iter()).any(|c| c.kind == "constraint" && c.table == t.table && c.name == t.name)
    };
    let candidate = |t: &&Thing| {
        (t.kind == "constraint" || (t.kind == "index" && !backs_constraint(t))) && !t.table.is_empty()
    };
    let gone: Vec<&Thing> = old.iter().filter(|t| !new_keys.contains(&key(t))).filter(candidate).collect();
    let came: Vec<&Thing> = new.iter().filter(|t| !old_keys.contains(&key(t))).filter(candidate).collect();
    // An index's definition carries its own name.
    let same = |a: &Thing, b: &Thing| replace_ident(&a.body, &a.name, &b.name) == b.body;
    let mut out = Paired { plan: Vec::new(), old: Default::default(), new: Default::default() };
    for was in &gone {
        let Some(now) = unique_match(was, &came, &same) else { continue };
        if unique_match(now, &gone, &|b: &Thing, a: &Thing| same(a, b)).map(|t| t.name.as_str())
            != Some(was.name.as_str())
        {
            continue;
        }
        let stmt = if was.kind == "index" {
            format!("ALTER INDEX {} RENAME TO {};", was.name, now.name)
        } else {
            format!("ALTER TABLE {} RENAME CONSTRAINT {} TO {};", now.table, was.name, now.name)
        };
        out.plan.push(Planned { owner: owner_of(now), part: Part::Expand, stmt });
        out.old.insert(key(was));
        out.new.insert(key(now));
    }
    out
}

/// The pieces of a column as Postgres reports them, parsed back out of
/// the body format the column THING query builds.
struct ColumnParts {
    ty: String,
    not_null: bool,
    default: Option<String>,
    /// `ALWAYS` or `DEFAULT` for an identity column.
    identity: Option<String>,
    /// The generation expression of a `GENERATED ... STORED` column.
    generated: Option<String>,
}

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
fn describe_column(body: &str) -> String {
    render_column(&column_parts(body))
}

/// The column clause of a CREATE TABLE or an ADD COLUMN for `parts`.
fn render_column(parts: &ColumnParts) -> String {
    let mut out;
    if let Some(expr) = &parts.generated {
        out = format!("{} GENERATED ALWAYS AS ({expr}) STORED", parts.ty);
    } else if let Some(kind) = &parts.identity {
        let when = if kind == "ALWAYS" { "ALWAYS" } else { "BY DEFAULT" };
        out = format!("{} GENERATED {when} AS IDENTITY", parts.ty);
    } else if let Some(serial) = serial_form(parts) {
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

/// The statement that makes `thing` whole: as it arrives with a table
/// nobody had before, which holds no row.
fn added_whole(thing: &Thing) -> String {
    match thing.kind.as_str() {
        "type" => format!(
            "CREATE TYPE {} AS ENUM ({});",
            thing.name,
            thing.body.split(',').map(|l| format!("'{l}'")).collect::<Vec<_>>().join(", ")
        ),
        "column" => format!("ALTER TABLE {} ADD COLUMN {} {};", thing.table, thing.name, describe_column(&thing.body)),
        "constraint" => {
            format!("ALTER TABLE {} ADD CONSTRAINT {} {};", thing.table, thing.name, thing.body)
        }
        "persistence" => format!("ALTER TABLE {} SET UNLOGGED;", thing.table),
        _ => format!("{};", thing.body),
    }
}

/// The statements that add `thing` to a schema whose tables may already
/// hold rows, each with the part it goes in ([`Part`]). A constraint, or
/// a unique index, tightens what the rows may hold, so it waits for the
/// contract part. A NOT NULL column with no default is added with
/// `backfill`'s value as its default when there is one, which the
/// contract part drops; with none, it is added nullable in the expand
/// part, so whatever fills it (a document's fill, the code) runs before
/// the contract part makes it NOT NULL.
fn added(thing: &Thing, backfill: Option<&Backfill>) -> Vec<(Part, String)> {
    match thing.kind.as_str() {
        "column" => {
            let parts = column_parts(&thing.body);
            if parts.not_null && parts.default.is_none() && parts.identity.is_none() && parts.generated.is_none() {
                if let Some(backfill) = backfill {
                    let filled = ColumnParts { default: Some(backfill.value.clone()), ..parts };
                    return vec![
                        (
                            Part::Expand,
                            format!(
                                "-- The rows already here hold {}; the contract part drops the default.\n\
                                 ALTER TABLE {} ADD COLUMN {} {};",
                                backfill.value,
                                thing.table,
                                thing.name,
                                render_column(&filled)
                            ),
                        ),
                        (Part::Contract, format!("ALTER TABLE {} ALTER COLUMN {} DROP DEFAULT;", thing.table, thing.name)),
                    ];
                }
                let nullable = ColumnParts { not_null: false, ..parts };
                vec![
                    (
                        Part::Expand,
                        format!(
                            "-- Added nullable: fill it before the contract part makes it NOT NULL.\n\
                             ALTER TABLE {} ADD COLUMN {} {};",
                            thing.table,
                            thing.name,
                            render_column(&nullable)
                        ),
                    ),
                    (Part::Contract, set_not_null(&thing.table, &thing.name)),
                ]
            } else {
                vec![(Part::Expand, added_whole(thing))]
            }
        }
        "constraint" => vec![(Part::Contract, added_whole(thing))],
        "index" if is_unique_index(thing) => vec![(Part::Contract, added_whole(thing))],
        _ => vec![(Part::Expand, added_whole(thing))],
    }
}

/// Whether `thing` is an index that refuses two rows holding the same key.
fn is_unique_index(thing: &Thing) -> bool {
    thing.kind == "index" && thing.body.starts_with("CREATE UNIQUE INDEX")
}

/// The statement that makes a column NOT NULL, with what it costs.
fn set_not_null(table: &str, column: &str) -> String {
    format!(
        "-- Fails if any row still holds a null here.\n\
         ALTER TABLE {table} ALTER COLUMN {column} SET NOT NULL;"
    )
}

/// The statements that turn `was` into `now`, each with the part it goes
/// in ([`Part`]): what only loosens or replaces in place (a function, an
/// enum label, a trigger, a column's type or default, a dropped NOT NULL)
/// in the expand part, what tightens (a constraint, a unique index, a new
/// NOT NULL) in the contract part.
fn changed(was: &Thing, now: &Thing, retyped: &Retyped<'_>) -> Vec<(Part, String)> {
    match now.kind.as_str() {
        // CREATE OR REPLACE cannot change what a function returns nor rename
        // its parameters: one whose signature changed is dropped first (no
        // CASCADE, so a trigger still on it fails the migration loudly
        // instead of going with it).
        "function" if function_signature(&was.body) != function_signature(&now.body) => {
            vec![(Part::Expand, format!("DROP FUNCTION {};\n{};", now.name, now.body))]
        }
        "function" => vec![(Part::Expand, format!("{};", now.body))],
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
            vec![(Part::Expand, out.join("\n"))]
        }
        "trigger" => vec![(Part::Expand, format!(
            "DROP TRIGGER {} ON {};\n{};",
            now.name, now.table, now.body
        ))],
        "index" => {
            let part = if is_unique_index(now) { Part::Contract } else { Part::Expand };
            vec![(part, format!("DROP INDEX {};\n{};", now.name, now.body))]
        }
        "constraint" => vec![(Part::Contract, format!(
            "ALTER TABLE {t} DROP CONSTRAINT {n};\nALTER TABLE {t} ADD CONSTRAINT {n} {b};",
            t = now.table,
            n = now.name,
            b = now.body
        ))],
        _ => {
            let was = column_parts(&was.body);
            let now_p = column_parts(&now.body);
            let (old_ty, old_not_null, old_default) = (was.ty, was.not_null, was.default);
            let (ty, not_null, default) = (now_p.ty, now_p.not_null, now_p.default);
            let mut out = Vec::new();
            if was.identity != now_p.identity || was.generated != now_p.generated {
                out.push((Part::Expand, format!(
                    "-- {}.{} changed its identity/generated form; Postgres has no \
                     single ALTER for this.\n-- Write the ALTER TABLE ... ALTER COLUMN \
                     ... [ADD|DROP|SET] GENERATED steps by hand here.",
                    now.table, now.name
                )));
            }
            if old_ty != ty {
                // The cast is the plain one. A column whose old values need
                // real conversion (a date out of a string, a unit change)
                // wants a different USING, which is why this one is spelled
                // out rather than hidden.
                let alter = format!(
                    "-- {}.{} was {old_ty}. The cast below is a plain one; if the rows \
                     already there\n-- need converting differently, change the USING.\n\
                     ALTER TABLE {} ALTER COLUMN {} TYPE {ty} USING {}::{ty};",
                    now.table, now.name, now.table, now.name, now.name
                );
                out.push((Part::Expand, retyped.around(&now.table, &now.name, alter)));
            }
            if old_default != default {
                out.push((Part::Expand, match &default {
                    Some(d) => format!(
                        "ALTER TABLE {} ALTER COLUMN {} SET DEFAULT {d};",
                        now.table, now.name
                    ),
                    None => format!(
                        "ALTER TABLE {} ALTER COLUMN {} DROP DEFAULT;",
                        now.table, now.name
                    ),
                }));
            }
            if old_not_null != not_null {
                out.push(if not_null {
                    (Part::Contract, set_not_null(&now.table, &now.name))
                } else {
                    (Part::Expand, format!("ALTER TABLE {} ALTER COLUMN {} DROP NOT NULL;", now.table, now.name))
                });
            }
            out
        }
    }
}

/// A function's head as `pg_get_functiondef` prints it: its first line
/// (name and named parameters) and its `RETURNS` line.
fn function_signature(definition: &str) -> (Option<&str>, Option<&str>) {
    let mut lines = definition.lines().map(str::trim);
    let head = lines.next();
    (head, lines.find(|line| line.starts_with("RETURNS ")))
}

/// The statement that takes `thing` away, with the part it goes in: every
/// drop waits for the contract part, so what the documents' steps and the
/// data steps read is there until they ran.
fn removed(thing: &Thing) -> (Part, String) {
    match thing.kind.as_str() {
        // IF EXISTS on an index or a constraint: one that names a column
        // this same plan drops leaves with that column, whichever
        // statement runs first.
        "index" => (Part::Contract, format!("DROP INDEX IF EXISTS {};", thing.name)),
        "trigger" => (Part::Contract, format!("DROP TRIGGER {} ON {};", thing.name, thing.table)),
        // CASCADE, for anything still hanging off it that the plan does
        // not drop by name; the plan drops functions last, after the
        // triggers it names.
        "function" => (Part::Contract, format!("DROP FUNCTION {} CASCADE;", thing.name)),
        "type" => (Part::Contract, format!("DROP TYPE {};", thing.name)),
        "constraint" => {
            (Part::Contract, format!("ALTER TABLE {} DROP CONSTRAINT IF EXISTS {};", thing.table, thing.name))
        }
        "persistence" => (Part::Expand, format!("ALTER TABLE {} SET LOGGED;", thing.table)),
        _ => (
            Part::Contract,
            format!("-- Throws away what is in {}.{}.\nALTER TABLE {} DROP COLUMN {};", thing.table, thing.name, thing.table, thing.name),
        ),
    }
}

/// Split a plan into one file per group and part, and say where each
/// file goes: a group's expand statements in `<id>.sql`, its contract
/// statements in `<id>.contract.sql` ([`CONTRACT_SUFFIX`]), each written
/// only when it holds a statement.
///
/// A statement is filed under the group that owns it: for a table, the
/// group listing it in `tables`, or, for a table the canonical DDL no
/// longer has (the plan drops it), the group whose history created it
/// (its `origin.sql` or a released migration); for a named table-less
/// object (a function, a type), the group whose DDL text declares its
/// name, or whose history did for one the canonical DDL dropped.
/// Anything that matches no group is refused rather than filed somewhere
/// plausible, since a migration in the wrong group runs against
/// databases that never had the table.
///
/// A path that already exists is refused rather than overwritten:
/// silently replacing a file that may already have run somewhere would
/// break its recorded checksum with the original content gone.
///
/// Returns the group, the part, the path to write, and the file's
/// contents.
pub fn file_per_group(
    history: &History<'_>,
    groups: &[&SchemaGroup],
    plan: &[Planned],
    name: &str,
    draft: bool,
) -> anyhow::Result<Vec<(String, Part, std::path::PathBuf, String)>> {
    let stamp = chrono::Utc::now().format("%Y%m%dT%H%M%S");
    let mut by_group: std::collections::BTreeMap<(&str, bool), Vec<&str>> = Default::default();
    for planned in plan {
        let owner = groups
            .iter()
            .find(|g| match &planned.owner {
                Owner::Table(table) => g.tables.contains(&table.as_str()),
                Owner::Named(name) => declared_names(g).iter().any(|n| n == name),
            })
            .or_else(|| match &planned.owner {
                Owner::Table(table) => groups.iter().find(|g| history.created(g.name, table)),
                Owner::Named(name) => groups.iter().find(|g| history.declared(g.name, name)),
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
        by_group.entry((owner.name, planned.part == Part::Contract)).or_default().push(planned.stmt.as_str());
    }
    by_group
        .into_iter()
        .map(|((group, contract), stmts)| {
            // The group is part of the id, so one release touching two
            // groups writes two DISTINCT ids: both run-orderings sort
            // by id with different tie-breaks, and identical ids would
            // let a live upgrade and the agreement replay apply a
            // paired release in opposite orders.
            let id = if draft {
                format!("{}{stamp}_{group}_{name}", migration_files::DRAFT_PREFIX)
            } else {
                format!("{stamp}_{group}_{name}")
            };
            let id = if contract { format!("{id}{CONTRACT_SUFFIX}") } else { id };
            let dir = migrations_root().join(group);
            let path =
                if draft { dir.join(migration_files::DRAFTS_DIR) } else { dir }.join(format!("{id}.sql"));
            anyhow::ensure!(
                !path.exists(),
                "{} already exists; refusing to overwrite a migration that may have \
                 run somewhere. Pick a different name.",
                path.display()
            );
            let part = if contract { Part::Contract } else { Part::Expand };
            Ok((group.to_string(), part, path, format!("{}\n", stmts.join("\n\n"))))
        })
        .collect()
}

/// How a database differs from `expected`, over the things these
/// groups own now or once owned (a table a rename or a drop took away
/// is still theirs to compare: the shape a release starts from holds it
/// under its old name); `None` when it holds exactly that shape. Read
/// through `db` so a caller mid-transaction sees its own uncommitted
/// DDL.
async fn shape_diff(
    history: &History<'_>,
    db: &impl Client,
    groups: &[&SchemaGroup],
    expected: &[Thing],
) -> anyhow::Result<Option<String>> {
    let once: Vec<(std::collections::HashSet<String>, Vec<String>)> =
        groups.iter().map(|g| (history.tables_created(g.name), history.declared_names(g.name))).collect();
    let ours = |t: &&Thing| {
        groups.iter().zip(&once).any(|(g, (tables, declared))| {
            owned_by(g, t)
                || if t.table.is_empty() || t.kind == "trigger" {
                    declared.iter().any(|n| n == bare_name(&t.name))
                } else {
                    tables.contains(&t.table)
                }
        })
    };
    let live_all = read_things(db, "public").await?;
    let live: Vec<Thing> = live_all.iter().filter(|t| ours(t)).cloned().collect();
    let expected: Vec<Thing> = expected.iter().filter(|t| ours(t)).cloned().collect();
    Ok(diff_things(&expected, "the release", &live, "your live database"))
}

/// The groups the live database has never built: no row in
/// `weft_schema_stamp`, which the boot writes the first time it builds
/// a group from its canonical DDL. A database from before the stamp
/// table existed has built nothing this way, so every group is unborn
/// to it and the boot builds them all.
pub async fn unborn_groups(db: &impl Client, groups: &[&SchemaGroup]) -> anyhow::Result<Vec<&'static str>> {
    let stamped: Vec<String> = db.scalars("SELECT group_name FROM weft_schema_stamp", &[]).await.or_else(|e| {
        if e.code() == Some("42P01") {
            Ok(Vec::new())
        } else {
            Err(e)
        }
    })?;
    Ok(groups.iter().map(|g| g.name).filter(|name| !stamped.iter().any(|s| s == name)).collect())
}

/// One part of a released migration as it settles on the live database:
/// its group, its id, which part it is, its SQL, and whether the database
/// still has to RUN it (it never saw the drafts) or only has it RECORDED
/// (it ran them, so it already holds the result).
pub struct Settling {
    pub group: String,
    pub id: String,
    pub part: Part,
    pub sql: String,
    pub run: bool,
}

/// Bring the live database to a release, in ONE transaction under the
/// migration lock, in the order a boot runs it: clear the draft rows of
/// EVERY group (two drafts that cancel out produce no released file, and
/// their rows would otherwise be junk nothing cleans), run and record the
/// expand parts, carry `documents` to today's shape, run the groups'
/// data steps ([`DataStep`]), run and record the contract parts, then verify it
/// now holds exactly `expected` over what these groups own before
/// committing. A failure anywhere rolls all of it
/// back, so the database is never recorded as holding a migration it does
/// not, and a recorded migration only ever means "this database holds its
/// result".
///
/// The live database is usually a running install, so the transaction is
/// a [`locked_tx`]: a table lock it cannot get within [`LOCK_WAIT`] rolls
/// it back whole instead of stalling the install's writes queued behind
/// it, and [`while_contended`] runs it again from the start, as the boot
/// does. A retry is the same transaction on the same database: nothing of
/// the attempt that gave way was kept.
pub async fn settle_release(
    history: &History<'_>,
    pool: &Pool,
    groups: &[&SchemaGroup],
    documents: &[&Document],
    expected: &[Thing],
    released: &[Settling],
) -> anyhow::Result<()> {
    validate_groups(groups)?;
    while_contended(|| settle_release_once(history, pool, groups, documents, expected, released)).await
}

/// One attempt of [`settle_release`], in one transaction.
async fn settle_release_once(
    history: &History<'_>,
    pool: &Pool,
    groups: &[&SchemaGroup],
    documents: &[&Document],
    expected: &[Thing],
    released: &[Settling],
) -> anyhow::Result<()> {
    let mut conn = pool.get().await?;
    let tx = locked_tx(&mut conn).await?;
    for group in groups {
        tx.execute("DELETE FROM weft_migration WHERE group_name = $1 AND id LIKE 'draft\\_%'", &[&group.name]).await?;
    }
    settle_part(&tx, released, Part::Expand).await?;
    crate::documents::apply_documents_within(&tx, documents).await.context("carry the stored documents to today's shape")?;
    run_data_steps(&tx, groups).await?;
    settle_part(&tx, released, Part::Contract).await?;
    for group in groups {
        if let Some(diff) = shape_diff(history, &tx, &[group], expected).await? {
            anyhow::bail!(
                "after the release ran, group '{}' on the live database still does not hold \
                 the shape the code declares, so nothing was recorded:\n{diff}",
                group.name
            );
        }
    }
    tx.commit().await?;
    Ok(())
}

/// Run what `released` has to run of `part`, in id order, and record each.
async fn settle_part(tx: &Transaction<'_>, released: &[Settling], part: Part) -> anyhow::Result<()> {
    let mut mine: Vec<&Settling> = released.iter().filter(|m| m.part == part).collect();
    mine.sort_by(|a, b| a.id.cmp(&b.id));
    for m in mine {
        if m.run {
            tx.batch_execute(&m.sql).await.with_context(|| format!("running released migration '{}' of '{}'", m.id, m.group))?;
        }
        // Never overwrite a recorded checksum: that row is what the
        // edited-history guard compares against. A pre-existing row
        // either already matches (a re-run of the same release) or is
        // a genuine id collision, refused loudly.
        let existing: Option<String> =
            tx.scalar_opt("SELECT checksum FROM weft_migration WHERE group_name = $1 AND id = $2", &[&m.group, &m.id]).await?;
        if let Some(stored) = existing {
            anyhow::ensure!(
                stored == checksum(&m.sql),
                "migration '{}' of '{}' is already recorded with different \
                 content; refusing to rewrite recorded history. Pick a different name.",
                m.id,
                m.group
            );
            continue;
        }
        record_migration(tx, &m.group, &m.id, &m.sql).await?;
    }
    Ok(())
}

/// Every draft file there is, so a release can delete them once it has
/// collapsed them.
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

/// What each reading has that the other does not, each side named by
/// its caller (a fresh install against an upgraded one, the release
/// against the live database). `None` when the two hold the same
/// schema.
///
/// One reading is [`read_schema`]'s, so the agreement tests, the
/// migration planner, and the release verification all look at the
/// database through the same eyes. Thing by thing rather than
/// whole-dump, because the useful failure to read is the one column
/// that differs, and with multiplicity, so a thing present twice on one
/// side and once on the other is a difference.
pub fn diff_things(a: &[Thing], only_a: &str, b: &[Thing], only_b: &str) -> Option<String> {
    let count = |things: &[Thing]| {
        let mut m: std::collections::BTreeMap<String, usize> = Default::default();
        for t in things {
            *m.entry(t.to_string()).or_default() += 1;
        }
        m
    };
    let a = count(a);
    let b = count(b);
    let mut out = String::new();
    for (line, n) in &a {
        if b.get(line).copied().unwrap_or(0) < *n {
            out.push_str(&format!("  only {only_a} has: {line}\n"));
        }
    }
    for (line, n) in &b {
        if a.get(line).copied().unwrap_or(0) < *n {
            out.push_str(&format!("  only {only_b} has: {line}\n"));
        }
    }
    if out.is_empty() {
        None
    } else {
        Some(out)
    }
}


/// Prove the canonical DDL and the migration history agree: build a
/// database each way and compare what Postgres ended up holding. The
/// body of the `schema_agreement` test (in `weft-runtime`, over every
/// group the install's database holds).
///
/// Also the enforcement point for origins: a group with no committed
/// `origin.sql` gets one WRITTEN here and the test fails naming it, so
/// the file lands in the same PR that adds the group, while canonical
/// and shipped DDL are still identical by construction. Freezing it any
/// later would fold newer canonical changes into the "past" and lose
/// their migration.
pub async fn assert_schema_agrees(pool: &Pool, groups: &[&SchemaGroup]) {
    let frozen = write_missing_origins(groups).expect("write missing origins");
    assert!(
        frozen.is_empty(),
        "these groups had no committed origin.sql; the files were just written, so \
         commit them with the PR that adds the groups and re-run:\n{}",
        frozen.join("\n")
    );

    apply_groups(pool, groups).await.expect("build from canonical DDL");
    let fresh = read_schema(pool).await.expect("read the fresh database");

    pool.batch_execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public").await.expect("empty the database");

    // Drafts are deliberately left out of the replay, which is what
    // makes an unreleased draft fail here rather than reaching a PR.
    replay_from_origin(pool, &History::embedded(), groups, false).await.expect("replay the history");
    let upgraded = read_schema(pool).await.expect("read the upgraded database");

    if let Some(diff) = diff_things(&fresh, "a fresh install", &upgraded, "an upgraded install") {
        panic!(
            "the canonical schema and the migration history disagree:\n{diff}\n\
             Write the migration that carries an existing database to this \
             change, in ONE command: ./setup.sh --migration <name> --release. \
             It writes the file under migrations/<group>/, runs it on your \
             database and records it as applied. A DRAFT (the same command \
             without --release) stays on your machine, gitignored, so this \
             test cannot see it until it is released."
        );
    }
}

/// Prove every group's canonical DDL is scratch-buildable by the
/// boot's shape check: build the schema, move every stamp, and boot
/// again. The re-boot rebuilds the DDL in the scratch schema, compares
/// it against the live one, and restamps. The body of the second
/// `schema_agreement` test, so the first place a
/// non-scratch-buildable statement (a hardcoded `public.`, a reference
/// outside the group set) surfaces is CI, never a production boot.
pub async fn assert_shape_check_restamps(pool: &Pool, groups: &[&SchemaGroup]) {
    apply_groups(pool, groups).await.expect("build from canonical DDL");
    pool.execute("UPDATE weft_schema_stamp SET fingerprint = 'moved'", &[]).await.expect("move every stamp");
    apply_groups(pool, groups).await.expect("the shape check verifies and restamps");
    let stale: Vec<String> =
        pool.scalars("SELECT group_name FROM weft_schema_stamp WHERE fingerprint = 'moved'", &[]).await.expect("read stamps");
    assert!(
        stale.is_empty(),
        "these groups were not restamped by the shape check: {stale:?}"
    );
}

/// The database a release runs on and records itself in: the one you
/// have been working against, which is the database of the install this
/// command acts on from where it runs (`WEFT_INSTALL` when set, else the
/// install of the weft checkout it runs in, `weft_core::infra::Install::wanted`).
/// Its address is the `WEFT_DATABASE_URL` line `weft daemon start` writes
/// into the install's `secrets.env`, the address its runtime reaches it
/// at. Returns a description of the install for messages, and the address.
fn live_database_url() -> anyhow::Result<(String, String)> {
    use weft_core::infra::{Install, WantedFrom, INSTALL_ENV};
    let cwd = std::env::current_dir()?;
    let wanted = Install::wanted(std::env::var(INSTALL_ENV).ok().as_deref(), &cwd).map_err(anyhow::Error::msg)?;
    let install = match &wanted.from {
        WantedFrom::Checkout { branch, .. } => format!("install '{}' (branch '{branch}')", wanted.install.env_value()),
        WantedFrom::Env => format!("install '{}' ({INSTALL_ENV})", wanted.install.env_value()),
        WantedFrom::Outside => "the default install".to_string(),
    };
    let url = wanted.install.secret("WEFT_DATABASE_URL").map_err(|e| anyhow::anyhow!("releasing runs on {install}'s database: {e}"))?;
    Ok((install, url))
}

/// The most connections the generator's pools hold (the throwaway
/// database it builds schemas in, and a release's live one), and how long
/// a statement waits for one, or for the database to answer.
const GENERATOR_POOL: u32 = 10;
const GENERATOR_WAIT: std::time::Duration = std::time::Duration::from_secs(30);

/// The whole `--migration` generator, the body of `weft-runtime`'s
/// `schema_migration` example (over every group the install's database
/// holds): freeze missing origins, build the schema
/// the databases out there hold (origins + released migrations + this
/// machine's drafts, read off the disk: [`HistoryOnDisk`]), build the schema the code declares, diff them,
/// file the difference (an expand file and a contract file per group,
/// [`Part`]), and on `--release` collapse the drafts and settle the
/// released files on the live database as a boot runs them, `documents`
/// carried and the groups' data steps run between the two parts (after PROVING
/// it holds the shape).
///
/// Reads the same env contract `./setup.sh --migration` provides:
/// `DATABASE_URL` (a throwaway Postgres to build schemas in), the
/// change name in argv, `--release`, every `--rename <spec>`, and
/// `--unrelated`. A release finds the database you have been working
/// against itself ([`live_database_url`]): the one of the install the
/// command acts on from where it runs.
pub async fn write_migration(groups: &[&SchemaGroup], documents: &[&Document]) -> anyhow::Result<()> {
    // Every declared group, before anything is planned, built or written:
    // a draft pass files what a bad declaration plans as surely as a
    // release does, and a release settles only the groups the live
    // database holds.
    validate_groups(groups)?;
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
    // from yet; freeze its DDL now. The history is then read off the
    // disk, not out of this binary, so the file just frozen (and every
    // migration file, whatever this binary was built with) is what the
    // plan starts from, in this same run.
    for path in write_missing_origins(groups)? {
        println!("froze {path}; commit it with the group it freezes");
    }
    let on_disk = HistoryOnDisk::read(&migrations_root())?;
    let history = on_disk.history();

    let pool = Pool::open(crate::db::config(&url)?, GENERATOR_POOL, GENERATOR_WAIT, Waits::Expected);
    // Releasing compares against what a database WITHOUT your drafts
    // holds, so the one file it writes carries the whole change rather
    // than the last slice of it.
    pool.batch_execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public").await?;
    replay_from_origin(&pool, &history, groups, !release).await.context("build what exists")?;
    let before = read_schema(&pool).await?;

    pool.batch_execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public").await?;
    apply_groups(&pool, groups).await.context("build what the code declares")?;
    let after = read_schema(&pool).await?;

    let args: Vec<String> = std::env::args().collect();
    let renames: Vec<Rename> = args
        .windows(2)
        .filter(|w| w[0] == "--rename")
        .map(|w| w[1].parse::<Rename>())
        .collect::<anyhow::Result<Vec<_>>>()?;
    // Every group is here, so a rename naming a table that no group
    // declares (or that a table rename in the list moves away from) is a
    // typo, never another generator's to carry.
    for rename in &renames {
        let table = match rename {
            Rename::Table { to, .. } => to,
            Rename::Column { table, .. } => table,
        };
        let known = groups.iter().any(|g| g.tables.contains(&table.as_str()))
            || renames.iter().any(|r| matches!(r, Rename::Table { from, .. } if from == table));
        anyhow::ensure!(known, "--rename names the table '{table}', which no CREATE TABLE declares");
    }
    let unrelated = args.iter().any(|a| a == "--unrelated");
    let unnamed = unnamed_renames(&before, &after, &renames)?;
    anyhow::ensure!(
        unnamed.is_empty() || unrelated,
        "nothing was written: some things go while others arrive beside them, and each \
         pair may be one thing renamed. Planned as they stand, the ones that go are \
         dropped with their rows:\n  {}\nName every rename, in the order they happen \
         (a swap is two, the one freeing a name first), and run the same command again:\n  \
         ./setup.sh --migration {name}{} --rename table.old_column=new_column --rename \
         old_table=new_table\nIf they really are unrelated, add --unrelated instead and \
         the rows of the ones that go are thrown away.",
        unnamed.join("\n  "),
        if release { " --release" } else { "" }
    );
    let backfills: Vec<Backfill> = documents.iter().map(|d| d.backfill()).collect();
    let plan = plan_migration(&before, &after, &renames, &backfills)?;
    if plan.is_empty() {
        println!("nothing changed: every group's migrations already reach its CREATE TABLE");
        return Ok(());
    }

    let written = file_per_group(&history, groups, &plan, &name, !release)?;
    let released: Vec<(String, String, Part, String)> = written
        .iter()
        .map(|(group, part, path, body)| {
            let id = path.file_stem().expect("a named file").to_string_lossy().to_string();
            (group.clone(), id, *part, body.clone())
        })
        .collect();

    // On a release, the live database is settled BEFORE any file is
    // put in place or draft deleted, so a refusal or a failed run leaves
    // the tree as it was: a released file on disk with the database
    // never carried forward would make the re-run answer "nothing
    // changed" and leave the drafts to break every later draft pass.
    // The files are written first, waiting beside their place
    // ([`write_files`]), so a file that cannot be written fails the
    // release before anything is recorded.
    // Each changed GROUP is decided on its own, since one can have run
    // its drafts while another never had any: a group that already
    // holds the new shape has the release RECORDED on it, one sitting
    // exactly where the last release left it has the file RUN on it
    // now (one command, instead of a draft pass first), and one that
    // is somewhere else (half its drafts, a hand edit) nothing here
    // can make honest, so the whole release is refused naming it. A
    // group this release writes no file for has to hold the shape the
    // code declares already (a draft whose change was then reverted
    // leaves a column the code no longer declares, and no file to
    // carry that database anywhere). The running and recording then
    // land in one transaction.
    let mut settle: Option<(Pool, Vec<&SchemaGroup>, Vec<Settling>)> = None;
    if release {
        let (install, live_url) = live_database_url()?;
        let live = Pool::open(crate::db::config(&live_url)?, GENERATOR_POOL, GENERATOR_WAIT, Waits::Expected);
        let conn = live.get().await.with_context(|| {
            format!(
                "releasing runs on {install}'s database, and it cannot be reached; start the \
                 install with ./setup.sh --daemon from its checkout and run the same command again"
            )
        })?;
        // A release records itself on the database and THEN puts its
        // files in place; a release cut between the two (Ctrl-C) leaves
        // a recorded id no tree carries, which the boot refuses and
        // only this command can tell apart from a build that is
        // merely older than its file. Its file waits beside its place
        // (written before the record), so the remedy is to put it
        // there; one with no waiting file is named with the one remedy
        // that fits: the row goes, and this release records its result
        // under the new id.
        for group in groups {
            let applied: HashMap<String, String> = conn
                .all::<(String, String)>("SELECT id, checksum FROM weft_migration WHERE group_name = $1", &[&group.name])
                .await?
                .into_iter()
                .collect();
            let stray = recorded_without_file(&for_group(history.migrations(), group.name), &applied);
            let waiting: Vec<String> = stray
                .iter()
                .map(|id| group_dir(group).join(format!("{id}.sql")))
                .filter_map(|path| {
                    let at = waiting_path(&path);
                    at.exists().then(|| move_back(&at, &path))
                })
                .collect();
            anyhow::ensure!(
                waiting.is_empty(),
                "group '{}': your database records migration(s) {} whose release was cut before \
                 its file was put in place; the file waits beside it. Put it there and rebuild:\n  {}",
                group.name,
                stray.join(", "),
                waiting.join("\n  ")
            );
            anyhow::ensure!(
                stray.is_empty(),
                "group '{}': your database records migration(s) {} that no file carries. If \
                 a release was interrupted before writing its file, that is its row; remove \
                 it (DELETE FROM weft_migration WHERE group_name = '{}' AND id IN ({})) and \
                 release again. If the file exists on another branch, check that branch out \
                 instead.",
                group.name,
                stray.join(", "),
                group.name,
                stray.iter().map(|id| format!("'{id}'")).collect::<Vec<_>>().join(", ")
            );
        }
        // A group the live database has never built (no stamp: it was
        // added to the code after this database last booted) has no
        // history to bring along; the next boot builds it from its
        // canonical DDL and records its origin, so the release neither
        // checks nor settles it.
        let unborn = unborn_groups(&conn, groups).await?;
        for name in &unborn {
            println!("group '{name}' is new to the live database; the next boot builds it");
        }
        let born: Vec<&SchemaGroup> =
            groups.iter().copied().filter(|g| !unborn.contains(&g.name)).collect();
        let groups = born.as_slice();
        let mut settling: Vec<Settling> = Vec::new();
        for group in groups {
            if released.iter().any(|(g, ..)| g == group.name) {
                continue;
            }
            if let Some(diff) = shape_diff(&history, &conn, &[group], &after).await? {
                anyhow::bail!(
                    "group '{}' changes nothing in this release, yet the live database does \
                     not hold the shape the code declares for it (a draft whose change was \
                     reverted, a hand edit); nothing was written. Put the group back to what \
                     the code declares (drop what the draft added; the draft's row and file \
                     are cleared by the release that then succeeds), or write the migration \
                     for what it holds:\n{diff}",
                    group.name
                );
            }
        }
        for (group_name, id, part, sql) in &released {
            if unborn.contains(&group_name.as_str()) {
                continue;
            }
            let group = *groups
                .iter()
                .find(|g| g.name == group_name)
                .expect("a released file belongs to one of the groups it was planned from");
            let run = if shape_diff(&history, &conn, &[group], &after).await?.is_none() {
                false
            } else if shape_diff(&history, &conn, &[group], &before).await?.is_none() {
                true
            } else {
                let diff = shape_diff(&history, &conn, &[group], &after)
                    .await?
                    .expect("a database that differs from both shapes differs from this one");
                anyhow::bail!(
                    "for group '{}', the live database holds neither the shape this release \
                     describes nor the one the last release left, so it cannot be brought \
                     along; nothing was written. Either put the group back to the shape the \
                     last release left (drop what the drafts added) and release again, or \
                     write the migration for what it holds:\n{diff}",
                    group.name
                );
            };
            settling.push(Settling { group: group_name.clone(), id: id.clone(), part: *part, sql: sql.clone(), run });
        }
        drop(conn);
        settle = Some((live, groups.to_vec(), settling));
    }

    let files: Vec<(&std::path::Path, &str)> = written.iter().map(|(_, _, path, body)| (path.as_path(), body.as_str())).collect();
    let settled = async {
        let Some((live, groups, settling)) = &settle else { return Ok(()) };
        settle_release(&history, live, groups, documents, &after, settling).await?;
        for m in settling {
            println!(
                "your database {} the released migration '{}' of '{}'",
                if m.run { "ran and records" } else { "records, instead of the drafts," },
                m.id,
                m.group
            );
        }
        Ok(())
    };
    write_files(&files, release, settled).await?;
    for (path, _) in &files {
        println!("wrote {}", path.display());
    }
    if release {
        // Draft FILES go only after the database is settled and the
        // released file is on disk.
        for path in draft_files(groups) {
            std::fs::remove_file(&path)?;
        }
    }
    Ok(())
}

/// Where a file of a migration waits beside its place, written but not
/// yet put there ([`write_files`]). The build script embeds `.sql` files
/// only, so a waiting one is never read as a migration.
fn waiting_path(path: &std::path::Path) -> std::path::PathBuf {
    let mut waiting = path.as_os_str().to_owned();
    waiting.push(".releasing");
    waiting.into()
}

/// The command that puts the waiting file `waiting` at `path`.
fn move_back(waiting: &std::path::Path, path: &std::path::Path) -> String {
    format!("mv '{}' '{}'", waiting.display(), path.display())
}

/// Write `files` (each path and its text), then await `settle` (a
/// release's commit on the live database, `recorded` true; nothing for a
/// draft), then put them in place. Each is first written waiting beside
/// its place ([`waiting_path`]), so a file that cannot be written fails
/// before `settle` records anything, and a `settle` that fails removes
/// them all: the tree is left as it was. A file that cannot be put in
/// place once `settle` recorded it fails naming the rename that finishes
/// the release (the database records it, and boots refuse it until its
/// file is there).
async fn write_files(
    files: &[(&std::path::Path, &str)],
    recorded: bool,
    settle: impl std::future::Future<Output = anyhow::Result<()>>,
) -> anyhow::Result<()> {
    let mut waiting: Vec<std::path::PathBuf> = Vec::new();
    if let Err(e) = write_waiting(files, &mut waiting) {
        return Err(remove_waiting(&waiting, e));
    }
    if let Err(e) = settle.await {
        return Err(remove_waiting(&waiting, e));
    }
    let stuck: Vec<String> = files
        .iter()
        .filter_map(|(path, _)| {
            let at = waiting_path(path);
            std::fs::rename(&at, path).err().map(|e| format!("{} ({e})", move_back(&at, path)))
        })
        .collect();
    anyhow::ensure!(
        stuck.is_empty(),
        "{} could not be put in place{}; run:\n  {}",
        if stuck.len() == 1 { "a migration file" } else { "migration files" },
        if recorded {
            ". Your database already records the release, and boots refuse it until its files are there"
        } else {
            ""
        },
        stuck.join("\n  ")
    );
    Ok(())
}

/// Write each of `files` waiting beside its place, each one written added
/// to `waiting`.
fn write_waiting(files: &[(&std::path::Path, &str)], waiting: &mut Vec<std::path::PathBuf>) -> anyhow::Result<()> {
    for (path, body) in files {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
        }
        let at = waiting_path(path);
        std::fs::write(&at, body).with_context(|| format!("write {}", at.display()))?;
        waiting.push(at);
    }
    Ok(())
}

/// `error`, once the files `waiting` are removed; one that could not be
/// removed is named in it, to remove by hand.
fn remove_waiting(waiting: &[std::path::PathBuf], error: anyhow::Error) -> anyhow::Error {
    let left: Vec<String> = waiting
        .iter()
        .filter_map(|at| std::fs::remove_file(at).err().map(|e| format!("{} ({e})", at.display())))
        .collect();
    if left.is_empty() {
        return error;
    }
    error.context(format!("nothing was recorded, and these files were left behind; remove them: {}", left.join(", ")))
}

#[cfg(test)]
mod tests {

    #[test]
    fn a_plan_drops_a_function_after_the_triggers_it_backs() {
        use super::{plan_migration, Thing};
        let thing = |kind: &str, table: &str, name: &str| Thing { kind: kind.into(), table: table.into(), name: name.into(), body: String::new() };
        let kept = thing("column", "run_event", "run_id");
        let old = vec![thing("function", "", "check"), kept.clone(), thing("trigger", "run_event", "check_on_insert")];
        let plan: Vec<String> = plan_migration(&old, &[kept], &[], &[]).unwrap().into_iter().map(|p| p.stmt).collect();
        assert_eq!(plan, ["DROP TRIGGER check_on_insert ON run_event;", "DROP FUNCTION check CASCADE;"]);
    }
    /// A function whose result changed is dropped before it is made again
    /// (CREATE OR REPLACE refuses that); one whose body alone changed is
    /// replaced in place.
    #[test]
    fn a_plan_drops_a_function_whose_signature_changed() {
        use super::{plan_migration, Thing};
        let func = |body: &str| Thing { kind: "function".into(), table: String::new(), name: "f(integer)".into(), body: body.into() };
        let was = func("CREATE OR REPLACE FUNCTION public.f(p integer)\n RETURNS integer\n LANGUAGE sql\nAS $$ SELECT 1 $$");
        let wider = func("CREATE OR REPLACE FUNCTION public.f(p integer)\n RETURNS TABLE(a integer, b boolean)\n LANGUAGE sql\nAS $$ SELECT 1, true $$");
        let plan: Vec<String> = plan_migration(std::slice::from_ref(&was), std::slice::from_ref(&wider), &[], &[]).unwrap().into_iter().map(|p| p.stmt).collect();
        assert_eq!(plan, [format!("DROP FUNCTION f(integer);\n{};", wider.body)]);
        let rewritten = func("CREATE OR REPLACE FUNCTION public.f(p integer)\n RETURNS integer\n LANGUAGE sql\nAS $$ SELECT 2 $$");
        let plan: Vec<String> = plan_migration(std::slice::from_ref(&was), std::slice::from_ref(&rewritten), &[], &[]).unwrap().into_iter().map(|p| p.stmt).collect();
        assert_eq!(plan, [format!("{};", rewritten.body)]);
        let renamed = func("CREATE OR REPLACE FUNCTION public.f(q integer)\n RETURNS integer\n LANGUAGE sql\nAS $$ SELECT 1 $$");
        let plan: Vec<String> = plan_migration(&[was], std::slice::from_ref(&renamed), &[], &[]).unwrap().into_iter().map(|p| p.stmt).collect();
        assert_eq!(plan, [format!("DROP FUNCTION f(integer);\n{};", renamed.body)]);
    }
    /// A column whose type changes takes every trigger of its table that
    /// holds it down in its own statement, and each trigger the canonical
    /// DDL has holding it goes back up after every column and function of
    /// the plan (a column it names may be added in the same plan), filed with
    /// the table: one kept, one changed to name an added column, one added
    /// holding it, and one removed (taken down there, never dropped again).
    /// A trigger that holds another column is left alone.
    #[test]
    fn a_retyped_column_takes_the_triggers_holding_it_down_and_up() {
        use super::{plan_migration, Owner, Part, Thing};
        let column = |name: &str, ty: &str| Thing { kind: "column".into(), table: "t".into(), name: name.into(), body: format!("{ty} null=YES default=-") };
        let trigger = |name: &str, body: &str| Thing { kind: "trigger".into(), table: "t".into(), name: name.into(), body: body.into() };
        let function = Thing { kind: "function".into(), table: String::new(), name: "g()".into(), body: "CREATE OR REPLACE FUNCTION public.g()".into() };
        let kept = trigger("t_on_doc", "CREATE TRIGGER t_on_doc AFTER UPDATE OF state, doc ON public.t FOR EACH ROW EXECUTE FUNCTION f()");
        let widened = trigger("t_on_doc", "CREATE TRIGGER t_on_doc AFTER UPDATE OF state, doc, zz ON public.t FOR EACH ROW EXECUTE FUNCTION f()");
        let gone = trigger("t_when_doc", "CREATE TRIGGER t_when_doc AFTER UPDATE ON public.t FOR EACH ROW WHEN ((new.doc IS DISTINCT FROM old.doc)) EXECUTE FUNCTION f()");
        let added = trigger("t_doc_again", "CREATE TRIGGER t_doc_again AFTER UPDATE OF doc ON public.t FOR EACH ROW EXECUTE FUNCTION f()");
        let other = trigger("t_on_docs", "CREATE TRIGGER t_on_docs AFTER UPDATE OF docs ON public.t FOR EACH ROW EXECUTE FUNCTION f()");
        let old = [column("doc", "jsonb"), kept, gone, other.clone()];
        let new = [column("doc", "text"), column("zz", "text"), function, widened.clone(), added.clone(), other];
        let plan = plan_migration(&old, &new, &[], &[]).unwrap();
        let stmts: Vec<&str> = plan.iter().map(|p| p.stmt.as_str()).collect();
        assert!(plan.iter().all(|p| p.part == Part::Expand), "{stmts:?}");
        let at = |what: &str| stmts.iter().position(|s| s.contains(what)).unwrap_or_else(|| panic!("no {what} in {stmts:#?}"));
        let alter = at("ALTER TABLE t ALTER COLUMN doc TYPE text USING doc::text;");
        let downs = &stmts[alter];
        assert!(downs.find("DROP TRIGGER IF EXISTS t_on_doc ON t;") < downs.find("ALTER TABLE t ALTER COLUMN doc"), "{downs}");
        assert!(downs.find("DROP TRIGGER IF EXISTS t_when_doc ON t;") < downs.find("ALTER TABLE t ALTER COLUMN doc"), "{downs}");
        let after = at("ADD COLUMN zz").max(at("FUNCTION public.g()"));
        for up in [&widened.body, &added.body] {
            let put = at(up);
            assert!(put > alter && put > after, "{up} goes up after every column and function: {stmts:#?}");
            assert!(plan[put].owner == Owner::Table("t".into()), "filed with the table");
            assert_eq!(stmts.iter().filter(|s| s.contains(up.as_str())).count(), 1, "planned once: {stmts:#?}");
        }
        assert!(!stmts.iter().any(|s| s.contains("t_on_docs")), "a trigger on another column stays up: {stmts:#?}");
        assert_eq!(stmts.iter().filter(|s| s.contains("t_when_doc")).count(), 1, "a removed trigger only comes down: {stmts:#?}");
    }

    /// A trigger put back after a type change makes the function it calls
    /// first when the plan makes that function (its own file may run
    /// later); one whose body alone changes is replaced in place and left
    /// to its own statement.
    #[test]
    fn a_retyped_columns_trigger_makes_a_function_made_with_it() {
        use super::{plan_migration, Thing};
        let column = |ty: &str| Thing { kind: "column".into(), table: "t".into(), name: "doc".into(), body: format!("{ty} null=YES default=-") };
        let function = |body: &str| Thing { kind: "function".into(), table: String::new(), name: "f()".into(), body: body.into() };
        let trigger = |call: &str| Thing {
            kind: "trigger".into(),
            table: "t".into(),
            name: "t_on_doc".into(),
            body: format!("CREATE TRIGGER t_on_doc AFTER UPDATE OF doc ON t FOR EACH ROW EXECUTE FUNCTION {call}"),
        };
        let made = "CREATE OR REPLACE FUNCTION f() two";
        let put_up = |old: &[Thing], new: &[Thing]| -> String {
            let plan = plan_migration(old, new, &[], &[]).unwrap();
            plan.into_iter().map(|p| p.stmt).find(|s| s.contains("CREATE TRIGGER t_on_doc")).expect("the trigger goes back up")
        };
        let old = [column("jsonb"), trigger("g()")];
        for call in ["f()", "\"f\"()"] {
            let stmt = put_up(&old, &[column("text"), function(made), trigger(call)]);
            assert!(stmt.find(made) < stmt.find("CREATE TRIGGER"), "the function comes first: {stmt}");
        }
        let old = [column("jsonb"), function("CREATE OR REPLACE FUNCTION f() one"), trigger("f()")];
        let stmt = put_up(&old, &[column("text"), function(made), trigger("f()")]);
        assert!(!stmt.contains(made), "a body replaced in place is left to its own statement: {stmt}");
    }

    /// A type added in a plan is made before the columns and tables made
    /// of it.
    #[test]
    fn an_added_type_comes_before_what_is_made_of_it() {
        use super::{plan_migration, Thing};
        let mood = Thing { kind: "type".into(), table: String::new(), name: "mood".into(), body: "sad,happy".into() };
        let column = |table: &str, ty: &str| Thing { kind: "column".into(), table: table.into(), name: "m".into(), body: format!("{ty} null=YES default=-") };
        let old = [column("t", "text")];
        let new = [column("t", "mood"), column("fresh", "mood"), mood];
        let plan: Vec<String> = plan_migration(&old, &new, &[], &[]).unwrap().into_iter().map(|p| p.stmt).collect();
        let at = |what: &str| plan.iter().position(|s| s.contains(what)).unwrap_or_else(|| panic!("no {what} in {plan:#?}"));
        let made = at("CREATE TYPE mood");
        assert!(made < at("ALTER COLUMN m TYPE mood") && made < at("CREATE TABLE fresh"), "{plan:#?}");
    }

    /// A function whose arguments changed, alone under its name on both
    /// sides, is replaced in the expand part; an overload added beside one
    /// the canonical DDL keeps is only added.
    #[test]
    fn a_function_whose_arguments_changed_is_replaced_and_an_added_overload_is_not() {
        use super::{plan_migration, Part, Thing};
        let func = |name: &str| Thing { kind: "function".into(), table: String::new(), name: name.into(), body: format!("CREATE OR REPLACE FUNCTION {name}") };
        let (one, two) = (func("f(integer)"), func("f(integer, integer)"));
        let plan = plan_migration(std::slice::from_ref(&one), std::slice::from_ref(&two), &[], &[]).unwrap();
        assert_eq!(plan.len(), 1);
        assert_eq!(plan[0].part, Part::Expand);
        assert!(plan[0].stmt.contains("DROP FUNCTION f(integer);\nCREATE OR REPLACE FUNCTION f(integer, integer);"), "{}", plan[0].stmt);
        let plan: Vec<(Part, String)> = plan_migration(std::slice::from_ref(&one), &[one.clone(), two.clone()], &[], &[])
            .unwrap()
            .into_iter()
            .map(|p| (p.part, p.stmt))
            .collect();
        assert_eq!(plan, [(Part::Expand, format!("{};", two.body))]);
    }
    /// A trigger that fires on a column holds it: the trigger goes before the
    /// column, whatever order the old schema lists them in.
    #[test]
    fn a_plan_drops_a_trigger_before_the_column_it_fires_on() {
        use super::{plan_migration, Thing};
        let thing = |kind: &str, name: &str, body: &str| Thing { kind: kind.into(), table: "signal".into(), name: name.into(), body: body.into() };
        let kept = thing("column", "token", "text null=NO default=-");
        let old = vec![kept.clone(), thing("column", "parked_fires", "jsonb null=NO default=-"), thing("trigger", "notify_on_grow", "")];
        let plan: Vec<String> = plan_migration(&old, &[kept], &[], &[]).unwrap().into_iter().map(|p| p.stmt).collect();
        let trigger = plan.iter().position(|s| s.starts_with("DROP TRIGGER notify_on_grow")).expect("the trigger goes");
        let column = plan.iter().position(|s| s.contains("DROP COLUMN parked_fires")).expect("the column goes");
        assert!(trigger < column, "{plan:?}");
    }

    #[test]
    fn a_renamed_column_keeps_its_rows_and_its_constraint_and_index_follow() {
        use super::{plan_migration, Thing};
        let t = |kind: &str, name: &str, body: &str| Thing { kind: kind.into(), table: "g".into(), name: name.into(), body: body.into() };
        let old = vec![
            t("column", "id", "text null=NO default=-"),
            t("column", "member_id", "text null=YES default=-"),
            t("constraint", "g_member_has_id", "CHECK ((member_id IS NULL) OR (id IS NOT NULL))"),
            t("index", "g_member", "CREATE INDEX g_member ON g USING btree (member_id)"),
            t("index", "g_by_id", "CREATE INDEX g_by_id ON g USING btree (id, member_id)"),
        ];
        let new = vec![
            t("column", "id", "text null=NO default=-"),
            t("column", "instance_id", "text null=YES default=-"),
            t("constraint", "g_instance_has_id", "CHECK ((instance_id IS NULL) OR (id IS NOT NULL))"),
            t("index", "g_instance", "CREATE INDEX g_instance ON g USING btree (instance_id)"),
            t("index", "g_by_id", "CREATE INDEX g_by_id ON g USING btree (id, instance_id)"),
        ];
        let renames = ["g.member_id=instance_id".parse().unwrap()];
        let plan: Vec<String> = plan_migration(&old, &new, &renames, &[]).unwrap().into_iter().map(|p| p.stmt).collect();
        assert_eq!(plan, [
            "ALTER TABLE g RENAME COLUMN member_id TO instance_id;",
            "ALTER TABLE g RENAME CONSTRAINT g_member_has_id TO g_instance_has_id;",
            "ALTER INDEX g_member RENAME TO g_instance;",
        ]);
    }

    #[test]
    fn a_swap_named_in_order_keeps_both_columns() {
        use super::{plan_migration, unnamed_renames, Thing};
        let c = |name: &str, body: &str| Thing { kind: "column".into(), table: "g".into(), name: name.into(), body: body.into() };
        let old = [c("instance_id", "text null=NO default=''::text"), c("member_id", "text null=YES default=-")];
        let new = [c("copy_id", "text null=NO default=''::text"), c("instance_id", "text null=YES default=-")];
        let renames = ["g.instance_id=copy_id".parse().unwrap(), "g.member_id=instance_id".parse().unwrap()];
        assert!(unnamed_renames(&old, &new, &renames).unwrap().is_empty());
        let plan: Vec<String> = plan_migration(&old, &new, &renames, &[]).unwrap().into_iter().map(|p| p.stmt).collect();
        assert_eq!(plan, [
            "ALTER TABLE g RENAME COLUMN instance_id TO copy_id;",
            "ALTER TABLE g RENAME COLUMN member_id TO instance_id;",
        ]);
    }

    #[test]
    fn a_column_or_table_going_while_another_arrives_is_reported_until_named() {
        use super::{unnamed_renames, Thing};
        let c = |table: &str, name: &str| Thing { kind: "column".into(), table: table.into(), name: name.into(), body: "text null=YES default=-".into() };
        let old = [c("g", "note"), c("member_value", "field")];
        let new = [c("g", "title"), c("instance_value", "field")];
        assert_eq!(unnamed_renames(&old, &new, &[]).unwrap().len(), 2);
        let renames = ["g.note=title".parse().unwrap(), "member_value=instance_value".parse().unwrap()];
        assert!(unnamed_renames(&old, &new, &renames).unwrap().is_empty());
        // A column only added, or only dropped, is no rename to name.
        assert!(unnamed_renames(&[c("g", "a")], &[c("g", "a"), c("g", "b")], &[]).unwrap().is_empty());
    }

    #[test]
    fn a_renamed_table_takes_its_indexes_along() {
        use super::{plan_migration, Thing};
        let t = |table: &str, kind: &str, name: &str, body: &str| Thing { kind: kind.into(), table: table.into(), name: name.into(), body: body.into() };
        let old = [
            t("member_value", "column", "grant_id", "uuid null=YES default=-"),
            t("member_value", "index", "member_value_grant", "CREATE INDEX member_value_grant ON member_value USING btree (grant_id)"),
            t("member_value", "constraint", "member_value_pkey", "PRIMARY KEY (grant_id)"),
            t("member_value", "index", "member_value_pkey", "CREATE UNIQUE INDEX member_value_pkey ON member_value USING btree (grant_id)"),
        ];
        let new = [
            t("instance_value", "column", "grant_id", "uuid null=YES default=-"),
            t("instance_value", "index", "instance_value_grant", "CREATE INDEX instance_value_grant ON instance_value USING btree (grant_id)"),
            t("instance_value", "constraint", "instance_value_pkey", "PRIMARY KEY (grant_id)"),
            t("instance_value", "index", "instance_value_pkey", "CREATE UNIQUE INDEX instance_value_pkey ON instance_value USING btree (grant_id)"),
        ];
        let renames = ["member_value=instance_value".parse().unwrap()];
        let plan: Vec<String> = plan_migration(&old, &new, &renames, &[]).unwrap().into_iter().map(|p| p.stmt).collect();
        // The primary key's index follows its constraint's RENAME; planning
        // it on its own would create a duplicate.
        assert_eq!(plan, [
            "ALTER TABLE member_value RENAME TO instance_value;",
            "ALTER INDEX member_value_grant RENAME TO instance_value_grant;",
            "ALTER TABLE instance_value RENAME CONSTRAINT member_value_pkey TO instance_value_pkey;",
        ]);
    }

    #[test]
    fn a_renamed_table_or_column_takes_its_serial_sequence_along() {
        use super::{plan_migration, Thing};
        let t = |table: &str, kind: &str, name: &str, body: &str| Thing { kind: kind.into(), table: table.into(), name: name.into(), body: body.into() };
        let old = [t("execution_tag", "column", "seq", "bigint null=NO default=nextval('execution_tag_seq_seq'::regclass)")];
        let new = [t("run_tag", "column", "seq", "bigint null=NO default=nextval('run_tag_seq_seq'::regclass)")];
        let plan: Vec<String> =
            plan_migration(&old, &new, &["execution_tag=run_tag".parse().unwrap()], &[]).unwrap().into_iter().map(|p| p.stmt).collect();
        assert_eq!(plan, ["ALTER TABLE execution_tag RENAME TO run_tag;", "ALTER SEQUENCE execution_tag_seq_seq RENAME TO run_tag_seq_seq;"]);

        let old = [t("tag", "column", "n", "bigint null=NO default=nextval('tag_n_seq'::regclass)")];
        let new = [t("tag", "column", "seq", "bigint null=NO default=nextval('tag_seq_seq'::regclass)")];
        let plan: Vec<String> =
            plan_migration(&old, &new, &["tag.n=seq".parse().unwrap()], &[]).unwrap().into_iter().map(|p| p.stmt).collect();
        assert_eq!(plan, ["ALTER TABLE tag RENAME COLUMN n TO seq;", "ALTER SEQUENCE tag_n_seq RENAME TO tag_seq_seq;"]);
    }

    #[test]
    fn a_rename_of_something_that_is_not_there_is_refused() {
        use super::{plan_migration, Thing};
        let c = Thing { kind: "column".into(), table: "g".into(), name: "a".into(), body: "text null=YES default=-".into() };
        let Err(err) = plan_migration(std::slice::from_ref(&c), std::slice::from_ref(&c), &["g.typo=b".parse().unwrap()], &[]) else {
            panic!("a rename of a missing column must be refused");
        };
        let err = err.to_string();
        assert!(err.contains("has no column 'typo'"), "{err}");
    }

    /// What a plan adds goes in the expand part, what it drops or
    /// tightens in the contract part: a NOT NULL column with no default is
    /// added nullable first and made NOT NULL after, and a new table arrives
    /// whole, its key with it.
    #[test]
    fn a_plan_splits_into_what_adds_and_what_drops_or_tightens() {
        use super::{plan_migration, Part, Thing};
        let t = |table: &str, kind: &str, name: &str, body: &str| Thing { kind: kind.into(), table: table.into(), name: name.into(), body: body.into() };
        let id = t("p", "column", "id", "integer null=NO default=-");
        let old = vec![id.clone(), t("p", "column", "doomed", "text null=YES default=-")];
        let new = vec![
            id,
            t("p", "column", "owner", "text null=NO default=-"),
            t("p", "constraint", "p_owner_short", "CHECK ((length(owner) < 10))"),
            t("q", "column", "k", "integer null=NO default=-"),
            t("q", "constraint", "q_pkey", "PRIMARY KEY (k)"),
        ];
        let plan: Vec<(Part, String)> = plan_migration(&old, &new, &[], &[]).unwrap().into_iter().map(|p| (p.part, p.stmt)).collect();
        let of = |part: Part| plan.iter().filter(|(p, _)| *p == part).map(|(_, s)| s.as_str()).collect::<Vec<_>>();
        assert_eq!(
            of(Part::Expand),
            [
                "-- Added nullable: fill it before the contract part makes it NOT NULL.\nALTER TABLE p ADD COLUMN owner text;",
                "CREATE TABLE q (\n    k integer NOT NULL\n);",
                "ALTER TABLE q ADD CONSTRAINT q_pkey PRIMARY KEY (k);",
            ]
        );
        assert_eq!(
            of(Part::Contract),
            [
                "-- Fails if any row still holds a null here.\nALTER TABLE p ALTER COLUMN owner SET NOT NULL;",
                "ALTER TABLE p ADD CONSTRAINT p_owner_short CHECK ((length(owner) < 10));",
                "-- Throws away what is in p.doomed.\nALTER TABLE p DROP COLUMN doomed;",
            ]
        );
    }

    #[test]
    fn a_whole_identifier_is_replaced_never_part_of_one() {
        assert_eq!(super::replace_ident("old_member_id, member_id)", "member_id", "instance_id"), "old_member_id, instance_id)");
    }

    // Layer-1 tests for the pure parts: the fingerprint, and which migrations
    // a database still owes.
    use std::collections::HashMap;

    use super::{
        checksum, edited_after_running, fingerprint_of, migrations_root, pending, recorded_without_file, History, HistoryOnDisk, Migration,
        Part, ORIGIN_ID,
    };

    static A: Migration = Migration { group: "g", id: "20260101_a", part: Part::Expand, draft: false, sql: "SELECT 1" };
    static B: Migration = Migration { group: "g", id: "20260202_b", part: Part::Expand, draft: false, sql: "SELECT 2" };
    static C: Migration = Migration { group: "g", id: "20260303_c", part: Part::Expand, draft: false, sql: "SELECT 3" };

    #[test]
    fn same_ddl_same_fingerprint() {
        let a = fingerprint_of(&["CREATE TABLE t (id INT)", "CREATE INDEX i ON t(id)"], &[]);
        let b = fingerprint_of(&["CREATE TABLE t (id INT)", "CREATE INDEX i ON t(id)"], &[]);
        assert_eq!(a, b);
        // Hex sha256: 64 lowercase hex chars.
        assert_eq!(a.len(), 64);
        assert!(a.chars().all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()));
    }

    #[test]
    fn changed_ddl_changes_fingerprint() {
        let base = fingerprint_of(&["CREATE TABLE t (id INT)"], &[]);
        let edited = fingerprint_of(&["CREATE TABLE t (id INT, name TEXT)"], &[]);
        assert_ne!(base, edited);
    }

    #[test]
    fn a_non_identifier_name_is_refused() {
        let bad = super::SchemaGroup {
            name: "worker'; DROP TABLE task; --",
            tables: &["worker_replica"],
            ddl: &["CREATE TABLE IF NOT EXISTS worker_replica (id INT)"],
            seed: &[],
            data_steps: &[],
        };
        let err = super::validate_groups(&[&bad]).expect_err("a quoted name must refuse");
        assert!(err.to_string().contains("non-identifier"), "{err}");
        let good = super::SchemaGroup {
            name: "worker_replica",
            tables: &["worker_replica", "infra_owner2"],
            ddl: &[],
            seed: &[],
            data_steps: &[],
        };
        super::validate_groups(&[&good]).expect("plain identifiers pass");
    }

    /// `write_migration` refuses a bad declaration before it plans,
    /// builds or writes anything (here, before it even reads its
    /// arguments or a database): a draft pass as surely as a release.
    #[tokio::test]
    async fn a_migration_refuses_a_bad_data_step_before_planning() {
        static STEP: &[crate::DataStep] =
            &[crate::DataStep { table: "elsewhere", until: super::Until::Dropped("owner"), sql: "SELECT 1" }];
        static BAD: super::SchemaGroup =
            super::SchemaGroup { name: "worker_replica", tables: &["worker_replica"], ddl: &[], seed: &[], data_steps: STEP };
        let err = super::write_migration(&[&BAD], &[]).await.expect_err("refused");
        assert!(err.to_string().contains("elsewhere.owner"), "{err}");
    }

    /// A scratch directory of its own for one test.
    fn scratch() -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("weft-write-files-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// The files land once `settle` is done, and none waits behind.
    #[tokio::test]
    async fn written_files_land_after_the_release_is_recorded() {
        let dir = scratch();
        let path = dir.join("group").join("1_a.sql");
        super::write_files(&[(&path, "SELECT 1;\n")], true, async {
            assert!(!path.exists(), "nothing is in place before the release is recorded");
            assert!(super::waiting_path(&path).exists(), "it is written, waiting");
            Ok(())
        })
        .await
        .unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "SELECT 1;\n");
        assert!(!super::waiting_path(&path).exists());
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// A file that cannot be written fails before anything is recorded,
    /// and a release that fails to record removes what it wrote: the tree
    /// is as it was either way.
    #[tokio::test]
    async fn a_file_not_written_or_a_release_not_recorded_leaves_nothing() {
        let dir = scratch();
        let fine = dir.join("1_a.sql");
        // A file where the second one's directory would go.
        std::fs::write(dir.join("blocked"), "").unwrap();
        let blocked = dir.join("blocked").join("1_b.sql");
        let recorded = std::cell::Cell::new(false);
        let err = super::write_files(&[(&fine, "a"), (&blocked, "b")], true, async {
            recorded.set(true);
            Ok(())
        })
        .await
        .expect_err("the second file cannot be written");
        assert!(!recorded.get(), "the release was never recorded");
        assert!(format!("{err:#}").contains("blocked"), "{err:#}");
        assert!(!fine.exists() && !super::waiting_path(&fine).exists(), "what was written is gone");

        let err = super::write_files(&[(&fine, "a")], true, async { Err(anyhow::anyhow!("the commit failed")) })
            .await
            .expect_err("the release failed");
        assert_eq!(err.to_string(), "the commit failed");
        assert!(!fine.exists() && !super::waiting_path(&fine).exists(), "what was written is gone");
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// A file that cannot be put in place once its release is recorded
    /// names the rename that finishes the release, and stays waiting for it.
    #[tokio::test]
    async fn a_file_not_put_in_place_names_the_rename_that_finishes_it() {
        let dir = scratch();
        let path = dir.join("1_a.sql");
        // A directory with something in it where the file goes.
        std::fs::create_dir_all(path.join("in_the_way")).unwrap();
        let err = super::write_files(&[(&path, "a")], true, async { Ok(()) }).await.expect_err("it cannot be put in place");
        let waiting = super::waiting_path(&path);
        let err = err.to_string();
        assert!(err.contains(&super::move_back(&waiting, &path)) && err.contains("already records the release"), "{err}");
        assert_eq!(std::fs::read_to_string(&waiting).unwrap(), "a", "the file waits for the rename");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_data_step_on_a_table_outside_its_group_is_refused() {
        static STEP: &[crate::DataStep] = &[crate::DataStep {
            table: "worker_replicas",
            until: super::Until::Dropped("owner"),
            sql: "SELECT 1",
        }];
        let typo = super::SchemaGroup {
            name: "worker_replica",
            tables: &["worker_replica"],
            ddl: &[],
            seed: &[],
            data_steps: STEP,
        };
        let err = super::validate_groups(&[&typo]).expect_err("a step's table must be one of its group's");
        assert!(
            err.to_string().contains("worker_replicas.owner") && err.to_string().contains("not one of its tables"),
            "{err}"
        );
        let own = super::SchemaGroup { tables: &["worker_replica", "worker_replicas"], ..typo };
        super::validate_groups(&[&own]).expect("a step on its own group's table passes");
    }

    #[test]
    fn statement_boundaries_matter() {
        // The per-statement newline separator keeps ["ab"] distinct from
        // ["a", "b"]: concatenation without a separator would collide.
        let joined = fingerprint_of(&["ab"], &[]);
        let split = fingerprint_of(&["a", "b"], &[]);
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
            Migration { group: "g", id: "20260101_a", part: Part::Expand, draft: false, sql: "SELECT 999" };
        let applied = HashMap::from([
            (A.id.to_string(), checksum(A.sql)),
            (B.id.to_string(), checksum(B.sql)),
        ]);
        assert_eq!(edited_after_running(&[&A_EDITED, &B], &applied), vec![A.id]);
    }

    #[test]
    fn a_migration_that_has_not_run_yet_can_still_be_edited() {
        static A_EDITED: Migration =
            Migration { group: "g", id: "20260101_a", part: Part::Expand, draft: false, sql: "SELECT 999" };
        assert!(edited_after_running(&[&A_EDITED], &HashMap::new()).is_empty());
    }

    /// A recorded release with no file is named; the origin row and
    /// drafts are the database's own and never are.
    #[test]
    fn a_recorded_release_with_no_file_is_named() {
        let applied: HashMap<String, String> = [
            (A.id.to_string(), checksum(A.sql)),
            ("20990101T000000_lost".to_string(), "x".to_string()),
            ("draft_20990101T000000_wip".to_string(), "y".to_string()),
            (ORIGIN_ID.to_string(), "z".to_string()),
        ]
        .into_iter()
        .collect();
        assert_eq!(recorded_without_file(&[&A], &applied), vec!["20990101T000000_lost".to_string()]);
    }

    /// A table files under the group whose history created it, whether
    /// its `origin.sql` or a released migration did (which is what lets a
    /// table the canonical DDL later drops still find its group), and a
    /// table no history created is nobody's.
    #[test]
    fn a_table_is_owned_by_the_group_whose_history_created_it() {
        let history = History::embedded();
        assert!(history.created("access_grant", "access_grant"));
        assert!(history.created("access_grant", "instance_value"));
        assert!(!history.created("project", "instance_value"));
        assert!(!history.created("access_grant", "instance"));
    }

    /// A fresh directory under the system temp dir, its own per test.
    fn scratch_migrations(name: &str) -> std::path::PathBuf {
        let root = std::env::temp_dir().join(format!("weft-migrations-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("make the scratch migrations dir");
        root
    }

    /// The generator plans from what is on disk NOW: an origin written a
    /// moment ago (a new group's, frozen in the same run) and a draft are
    /// both read, each file's part and draft-ness read off its name.
    #[test]
    fn the_generator_reads_the_history_off_the_disk() {
        let root = scratch_migrations("disk");
        let group = root.join("widget");
        std::fs::create_dir_all(group.join("drafts")).unwrap();
        std::fs::write(group.join("origin.sql"), "CREATE TABLE widget (id INT);\n").unwrap();
        std::fs::write(group.join("20260101T000000_widget_a.sql"), "SELECT 1;\n").unwrap();
        std::fs::write(group.join("20260101T000000_widget_a.contract.sql"), "SELECT 2;\n").unwrap();
        std::fs::write(group.join("drafts/draft_20260202T000000_widget_b.sql"), "SELECT 3;\n").unwrap();
        std::fs::write(group.join("notes.txt"), "not a migration").unwrap();

        let on_disk = HistoryOnDisk::read(&root).unwrap();
        let history = on_disk.history();
        assert_eq!(history.origin("widget"), Some("CREATE TABLE widget (id INT);\n"));
        assert_eq!(history.origin("gadget"), None);
        let seen: Vec<(&str, &str, Part, bool, &str)> =
            history.migrations().iter().map(|m| (m.group, m.id, m.part, m.draft, m.sql)).collect();
        assert_eq!(
            seen,
            vec![
                ("widget", "20260101T000000_widget_a", Part::Expand, false, "SELECT 1;\n"),
                ("widget", "20260101T000000_widget_a.contract", Part::Contract, false, "SELECT 2;\n"),
                ("widget", "draft_20260202T000000_widget_b", Part::Expand, true, "SELECT 3;\n"),
            ]
        );
        assert!(history.created("widget", "widget"));
        std::fs::remove_dir_all(&root).unwrap();
    }

    /// A released file and a draft with one id would race at boot, so the
    /// walk refuses them, naming both, for the generator as for the build.
    #[test]
    fn two_files_with_one_id_are_refused() {
        let root = scratch_migrations("dup");
        let group = root.join("widget");
        std::fs::create_dir_all(group.join("drafts")).unwrap();
        std::fs::write(group.join("draft_x.sql"), "SELECT 1;\n").unwrap();
        std::fs::write(group.join("drafts/draft_x.sql"), "SELECT 2;\n").unwrap();
        let err = HistoryOnDisk::read(&root).err().expect("refused").to_string();
        assert!(err.contains("two files share the id 'draft_x'"), "{err}");
        std::fs::remove_dir_all(&root).unwrap();
    }

    /// The build embeds the very walk the generator reads, so on a tree
    /// the build has seen, the two histories are the same files.
    #[test]
    fn the_embedded_history_is_the_disk_history() {
        let on_disk = HistoryOnDisk::read(&migrations_root()).unwrap();
        let disk = on_disk.history();
        let embedded = History::embedded();
        let key = |h: &History<'_>| -> Vec<(String, String, bool, String)> {
            h.migrations().iter().map(|m| (m.group.to_string(), m.id.to_string(), m.draft, m.sql.to_string())).collect()
        };
        assert_eq!(key(&disk), key(&embedded));
        assert_eq!(disk.origins, embedded.origins);
    }
}
