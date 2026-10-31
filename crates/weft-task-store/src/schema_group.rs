//! What a module declares about its tables: a [`SchemaGroup`] and its
//! [`DataStep`]s.
//!
//! Apart from the machinery that applies them (`crate::schema_guard`,
//! `crate::documents`) because a worker links this crate and never touches
//! the schema. The declarations sit beside code a worker runs; the runner,
//! the generator and the migration history the build script embeds are
//! behind the `migrate` feature, which only the roles that boot a database
//! turn on. The worker's copy of this crate leaves `migrations/` out
//! (`[package.metadata.weft]` in `Cargo.toml`), so writing a migration
//! names no new worker and asks nobody to resync.

use futures::future::BoxFuture;

use crate::db::Transaction;

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
    /// record), run after the DDL whenever a boot has work to do. Seeds
    /// are in the fingerprint, so editing one moves the stamp and the next
    /// boot runs it; they are outside the migration history, so editing
    /// one is a non-event rather than a "write the migration" dead end (a
    /// seed row is invisible to the schema reader, so the shape check
    /// finds nothing to differ and restamps).
    pub seed: &'static [&'static str],
    /// What the rows already there need done before a contract part
    /// takes away or tightens a column, declared by the group whose
    /// tables the data lands in: see [`DataStep`].
    pub data_steps: &'static [DataStep],
}

/// Work on the rows a database already holds that a contract part needs
/// done before it runs: a column's content copied to where it lives now
/// before the column is dropped, a column the expand part added filled
/// on the rows already there before it is made NOT NULL, or rows folded
/// so a unique index the contract builds takes them. It runs at the start
/// of the contract transaction (`crate::schema_guard::apply_schema`), after the expand parts
/// made the place it writes to and the documents were carried, and only
/// while what its [`Until`] names (a column of `table`, or an index) is
/// still the way the contract part has not left it yet: a fresh database, built
/// from the DDL, never needs it, and neither does one the contract part
/// ran on.
/// `table` is locked first, so no write lands between the step and the
/// contract part.
///
/// A `crate::documents::Fill` is the other way a value leaves where it
/// was: it runs before a step of a JSON column that stays, on the rows
/// still at the step's version. A data step has no version to key on.
pub struct DataStep {
    /// The table the step works on (and locks). It is one of its group's
    /// `tables`, which the boot checks.
    pub table: &'static str,
    /// What the contract part does, which ends the step's work, and to
    /// what (a column of `table`, or an index).
    pub until: Until,
    /// What does the work. Nothing records that it ran: only the state
    /// [`Until`] names stops it, so every boot with schema work to do runs
    /// it again until a contract part changes that state, and it must give
    /// the same result run twice (`ON CONFLICT DO NOTHING`, a `SET` on the
    /// rows still `NULL`).
    pub work: StepWork,
}

/// What does a [`DataStep`]'s work.
pub enum StepWork {
    /// One statement (or several, `;`-separated).
    Sql(&'static str),
    /// Rust, for what SQL cannot read: a run's record, say, which only
    /// weft's code decodes (`weft_journal::stored`). Given the contract
    /// transaction with the step's table already locked; it reads in
    /// bounded pages, since it may walk every row the install holds.
    Code(StepCode),
}

/// The Rust a [`StepWork::Code`] runs, given the contract transaction.
pub type StepCode = for<'c> fn(&'c Transaction<'c>) -> BoxFuture<'c, anyhow::Result<()>>;

/// What a contract part does, which leaves a [`DataStep`] nothing to do.
/// Each variant carries the name its condition is about, so a step names
/// only what it waits on (an index step has no column to name).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Until {
    /// It drops this column of the step's table: the step copies what the
    /// column holds to where it lives now, while the column is there.
    Dropped(&'static str),
    /// It makes this column of the step's table NOT NULL: the step fills
    /// the column on the rows already there, while it may still hold `NULL`.
    NotNull(&'static str),
    /// It builds the index named (a unique one, say): the step makes the
    /// rows already there fit it (folds the duplicates it would refuse),
    /// while no index of that name exists.
    Indexed(&'static str),
}

#[cfg(feature = "migrate")]
impl Until {
    /// The column or index the condition is about.
    pub(crate) fn name(self) -> &'static str {
        match self {
            Until::Dropped(name) | Until::NotNull(name) | Until::Indexed(name) => name,
        }
    }
}

#[cfg(feature = "migrate")]
impl DataStep {
    /// What the step is on, for its errors: `table.column`, or the table
    /// and the index it waits on.
    pub(crate) fn target(&self) -> String {
        match self.until {
            Until::Dropped(column) | Until::NotNull(column) => format!("{}.{column}", self.table),
            Until::Indexed(index) => format!("{} (until index {index} is built)", self.table),
        }
    }
}
