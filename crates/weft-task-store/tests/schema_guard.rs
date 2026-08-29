//! Layer-3 tests for the schema guard against a REAL Postgres: the
//! stamp/verify/refuse behavior lives IN the SQL it runs, so a faked pool
//! would not catch it. Each test gets a fresh isolated database via
//! `#[sqlx::test]` (reads `$DATABASE_URL`, creates a random DB, drops it
//! after); skipped when `DATABASE_URL` is unset.
//!
//! Gated behind the `db-tests` feature (off by default) so a plain
//! `cargo test --workspace` needs no Postgres; CI runs
//! `cargo test -p weft-task-store --features db-tests` with `$DATABASE_URL` set.
#![cfg(feature = "db-tests")]

use sqlx::PgPool;

use weft_task_store::schema_guard::apply_groups_with as apply;
use weft_task_store::{Migration, SchemaGroup};

/// Most of these tests are about the canonical DDL, so they run with no
/// migrations at all.
static NONE: &[Migration] = &[];

static GUARDED: SchemaGroup = SchemaGroup {
    name: "guard_probe",
    tables: &["guard_probe"],
    ddl: &[
        "CREATE TABLE IF NOT EXISTS guard_probe (id INT PRIMARY KEY, note TEXT)",
        "CREATE INDEX IF NOT EXISTS idx_guard_probe_note ON guard_probe(note)",
    ],
    seed: &[],
};

static OTHER: SchemaGroup = SchemaGroup {
    name: "guard_other",
    tables: &["guard_other"],
    ddl: &["CREATE TABLE IF NOT EXISTS guard_other (id INT PRIMARY KEY)"],
    seed: &[],
};

#[sqlx::test]
async fn fresh_apply_stamps_and_reruns_are_idempotent(pool: PgPool) {
    apply(&pool, &[&GUARDED, &OTHER], NONE).await.expect("fresh apply");
    let (fp,): (String,) = sqlx::query_as(
        "SELECT fingerprint FROM weft_schema_stamp WHERE group_name = 'guard_probe'",
    )
    .fetch_one(&pool)
    .await
    .expect("stamp row exists");
    assert_eq!(fp.len(), 64, "hex sha256 stamp");

    // Re-running with the same groups succeeds and leaves the stamp.
    apply(&pool, &[&GUARDED, &OTHER], NONE).await.expect("re-apply");
    let (fp2,): (String,) = sqlx::query_as(
        "SELECT fingerprint FROM weft_schema_stamp WHERE group_name = 'guard_probe'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(fp, fp2);

    // A matching stamp still re-runs the DDL, so a manually dropped table
    // is repaired on the next boot.
    sqlx::raw_sql("DROP TABLE guard_probe").execute(&pool).await.unwrap();
    apply(&pool, &[&GUARDED], NONE).await.expect("repair apply");
    sqlx::query("SELECT 1 FROM guard_probe")
        .execute(&pool)
        .await
        .expect("table recreated");
}

#[sqlx::test]
async fn mismatched_fingerprint_fails_naming_group_and_drops(pool: PgPool) {
    apply(&pool, &[&GUARDED, &OTHER], NONE).await.expect("fresh apply");
    // Simulate a database created by a different build of the DDL: the
    // stamp differs AND the live shape genuinely disagrees with the
    // canonical build. (A stale stamp over a MATCHING shape is a
    // non-event: the boot proves the shape and restamps silently.)
    sqlx::query(
        "UPDATE weft_schema_stamp SET fingerprint = 'stale' WHERE group_name = 'guard_probe'",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::raw_sql("ALTER TABLE guard_probe ADD COLUMN sneaky TEXT")
        .execute(&pool)
        .await
        .unwrap();

    let err = apply(&pool, &[&GUARDED, &OTHER], NONE)
        .await
        .expect_err("stale stamp must fail the apply");
    let msg = format!("{err:#}");
    assert!(msg.contains("guard_probe"), "names the mismatched group: {msg}");
    assert!(
        msg.contains("DROP TABLE IF EXISTS guard_probe"),
        "prints the ready-to-run DROP: {msg}"
    );
    assert!(
        msg.contains("DELETE FROM weft_schema_stamp WHERE group_name = 'guard_probe';"),
        "prints the stamp reset: {msg}"
    );
    assert!(!msg.contains("guard_other"), "clean groups are not named: {msg}");

    // The failed run must not have restamped the mismatched group.
    let (fp,): (String,) = sqlx::query_as(
        "SELECT fingerprint FROM weft_schema_stamp WHERE group_name = 'guard_probe'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(fp, "stale");

    // Running the printed reset then re-applying recovers cleanly.
    sqlx::raw_sql(
        "DROP TABLE IF EXISTS guard_probe CASCADE;\
         DELETE FROM weft_schema_stamp WHERE group_name = 'guard_probe';",
    )
    .execute(&pool)
    .await
    .unwrap();
    apply(&pool, &[&GUARDED, &OTHER], NONE).await.expect("recovered apply");
}

#[sqlx::test]
async fn a_mismatch_rolls_back_every_group_in_the_call(pool: PgPool) {
    // Stamp only GUARDED, then poison it: the next call carries a FRESH
    // group alongside the mismatched one. The fresh group's DDL and stamp
    // run before the bail inside the same transaction, so the failure
    // must discard BOTH: a half-applied boot would leave the fresh group
    // stamped by a run that reported failure.
    apply(&pool, &[&GUARDED], NONE).await.expect("fresh apply");
    // A moved stamp AND a genuinely different live shape (see the
    // naming test above for why both are needed).
    sqlx::query(
        "UPDATE weft_schema_stamp SET fingerprint = 'stale' WHERE group_name = 'guard_probe'",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::raw_sql("ALTER TABLE guard_probe ADD COLUMN sneaky TEXT")
        .execute(&pool)
        .await
        .unwrap();

    apply(&pool, &[&GUARDED, &OTHER], NONE)
        .await
        .expect_err("the mismatch fails the whole call");

    let other_stamp: Option<(String,)> = sqlx::query_as(
        "SELECT fingerprint FROM weft_schema_stamp WHERE group_name = 'guard_other'",
    )
    .fetch_optional(&pool)
    .await
    .unwrap();
    assert!(other_stamp.is_none(), "the fresh group's stamp rolled back with the failure");
    let table_exists: Option<(i32,)> = sqlx::query_as(
        "SELECT 1 FROM information_schema.tables WHERE table_name = 'guard_other'",
    )
    .fetch_optional(&pool)
    .await
    .unwrap();
    assert!(table_exists.is_none(), "the fresh group's table rolled back with the failure");
}

#[sqlx::test]
async fn concurrent_applies_serialize_on_the_advisory_lock(pool: PgPool) {
    // The exact scenario the lock exists for: two replicas booting the
    // same fresh database at once. IF NOT EXISTS alone is not
    // concurrency-safe in Postgres (both pass the existence check, one
    // dies on a duplicate catalog key); under the lock both must
    // succeed.
    let groups: [&SchemaGroup; 2] = [&GUARDED, &OTHER];
    let (a, b) = tokio::join!(apply(&pool, &groups, NONE), apply(&pool, &groups, NONE));
    a.expect("first concurrent apply");
    b.expect("second concurrent apply");
    sqlx::query("SELECT 1 FROM guard_probe").execute(&pool).await.expect("table exists");
}

// The same group after a change: a column added to the canonical DDL, and
// the migration that adds it to a database that already exists. The ALTER
// carries no IF NOT EXISTS on purpose, so a run that should have been
// skipped fails loudly instead of passing quietly.
static ADD_EXTRA: &[Migration] = &[Migration {
    group: "guard_probe",
    id: "20260823T1200_add_extra",
    draft: false,
    sql: "ALTER TABLE guard_probe ADD COLUMN extra TEXT",
}];

static GUARDED_V2: SchemaGroup = SchemaGroup {
    name: "guard_probe",
    tables: &["guard_probe"],
    ddl: &[
        "CREATE TABLE IF NOT EXISTS guard_probe (id INT PRIMARY KEY, note TEXT, extra TEXT)",
        "CREATE INDEX IF NOT EXISTS idx_guard_probe_note ON guard_probe(note)",
    ],
    seed: &[],
};

async fn has_extra_column(pool: &PgPool) -> bool {
    sqlx::query_as::<_, (i32,)>(
        "SELECT 1 FROM information_schema.columns \
         WHERE table_name = 'guard_probe' AND column_name = 'extra'",
    )
    .fetch_optional(pool)
    .await
    .unwrap()
    .is_some()
}

#[sqlx::test]
async fn a_database_that_already_exists_is_carried_forward_by_the_migration(pool: PgPool) {
    apply(&pool, &[&GUARDED], NONE).await.expect("the old shape");
    assert!(!has_extra_column(&pool).await);
    sqlx::query("INSERT INTO guard_probe (id, note) VALUES (1, 'keep me')")
        .execute(&pool)
        .await
        .expect("a row somebody cares about");

    apply(&pool, &[&GUARDED_V2], ADD_EXTRA).await.expect("upgrade");
    assert!(has_extra_column(&pool).await, "the migration ran");

    // The whole point: upgrading carries the data across rather than
    // rebuilding the table around it.
    let (note,): (String,) = sqlx::query_as("SELECT note FROM guard_probe WHERE id = 1")
        .fetch_one(&pool)
        .await
        .expect("the row is still there");
    assert_eq!(note, "keep me");

    let (id,): (String,) =
        sqlx::query_as("SELECT id FROM weft_migration WHERE group_name = 'guard_probe'")
            .fetch_one(&pool)
            .await
            .expect("the migration was recorded");
    assert_eq!(id, "20260823T1200_add_extra");

    // Booting again runs nothing a second time.
    apply(&pool, &[&GUARDED_V2], ADD_EXTRA).await.expect("re-apply");
}

#[sqlx::test]
async fn a_new_database_is_built_whole_and_inherits_the_history(pool: PgPool) {
    // Nothing to carry forward, so the canonical DDL builds the table with
    // the column already in it and the migration is recorded without running.
    // Running it here would fail on a duplicate column.
    apply(&pool, &[&GUARDED_V2], ADD_EXTRA).await.expect("fresh apply");
    assert!(has_extra_column(&pool).await);
    let (count,): (i64,) =
        sqlx::query_as("SELECT count(*) FROM weft_migration WHERE group_name = 'guard_probe'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(count, 1, "recorded as history it was born with");
}

#[sqlx::test]
async fn changing_the_ddl_with_no_migration_refuses(pool: PgPool) {
    static DRIFTED: SchemaGroup = SchemaGroup {
        name: "guard_probe",
        tables: &["guard_probe"],
        ddl: &["CREATE TABLE IF NOT EXISTS guard_probe (id INT PRIMARY KEY, note TEXT, extra TEXT)"],
        seed: &[],
    };
    apply(&pool, &[&GUARDED], NONE).await.expect("the old shape");
    let err = apply(&pool, &[&DRIFTED], NONE).await.expect_err("must refuse");
    let msg = err.to_string();
    assert!(msg.contains("does not hold the shape"), "{msg}");
    assert!(msg.contains("guard_probe"), "{msg}");
    // The refusal names the exact object that differs, read from the
    // catalogs, not just the group.
    assert!(msg.contains("extra"), "{msg}");
    assert!(!has_extra_column(&pool).await, "the drifted DDL did not run");
}

/// A fingerprint that moved with NO shape change (a comment edit, a
/// seed statement moved out of the DDL) restamps silently: the shape
/// check proves the live schema already matches the canonical build,
/// so demanding a migration for a non-change would brick every
/// existing database over text.
#[sqlx::test]
async fn a_fingerprint_move_with_no_shape_change_restamps_silently(pool: PgPool) {
    static COMMENTED: SchemaGroup = SchemaGroup {
        name: "guard_probe",
        tables: &["guard_probe"],
        // Same shape as GUARDED, different text.
        ddl: &[
            "-- a comment that moves the fingerprint\n\
             CREATE TABLE IF NOT EXISTS guard_probe (id INT PRIMARY KEY, note TEXT)",
            "CREATE INDEX IF NOT EXISTS idx_guard_probe_note ON guard_probe(note)",
        ],
        seed: &[],
    };
    apply(&pool, &[&GUARDED], NONE).await.expect("the old shape");
    let stamp = |pool: PgPool| async move {
        let (fp,): (String,) = sqlx::query_as(
            "SELECT fingerprint FROM weft_schema_stamp WHERE group_name = 'guard_probe'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        fp
    };
    let before = stamp(pool.clone()).await;
    apply(&pool, &[&COMMENTED], NONE).await.expect("a text-only change boots clean");
    // And the stamp really moved, so later boots are a plain
    // stamp-match rather than a shape re-verification.
    assert_ne!(stamp(pool.clone()).await, before, "the shape check restamped");
    apply(&pool, &[&COMMENTED], NONE).await.expect("restamped");
}

/// A pending migration must never launder an unrelated DDL edit: the
/// migration runs, but the extra canonical column it does NOT create
/// still fails the boot loudly.
#[sqlx::test]
async fn a_pending_migration_does_not_launder_an_uncovered_ddl_edit(pool: PgPool) {
    static V2_PLUS_STOWAWAY: SchemaGroup = SchemaGroup {
        name: "guard_probe",
        tables: &["guard_probe"],
        // ADD_EXTRA's migration covers `extra`; `stowaway` has no
        // migration and IF NOT EXISTS will never add it.
        ddl: &["CREATE TABLE IF NOT EXISTS guard_probe \
                (id INT PRIMARY KEY, note TEXT, extra TEXT, stowaway TEXT)"],
        seed: &[],
    };
    apply(&pool, &[&GUARDED], NONE).await.expect("the old shape");
    let err = apply(&pool, &[&V2_PLUS_STOWAWAY], ADD_EXTRA)
        .await
        .expect_err("the stowaway column must fail the boot");
    let msg = err.to_string();
    assert!(msg.contains("does not hold the shape"), "{msg}");
    assert!(msg.contains("stowaway"), "{msg}");
}

#[sqlx::test]
async fn editing_a_migration_that_already_ran_refuses(pool: PgPool) {
    static REWRITTEN: &[Migration] = &[Migration {
        group: "guard_probe",
        id: "20260823T1200_add_extra",
        draft: false,
        sql: "ALTER TABLE guard_probe ADD COLUMN something_else TEXT",
    }];
    apply(&pool, &[&GUARDED], NONE).await.expect("the old shape");
    apply(&pool, &[&GUARDED_V2], ADD_EXTRA).await.expect("upgrade");

    let err = apply(&pool, &[&GUARDED_V2], REWRITTEN).await.expect_err("must refuse");
    assert!(err.to_string().contains("edited after running"), "{err}");
}

// Two branches that each added a migration, merged in either order. Both
// files sit in the directory afterwards, and a database that has run one of
// them has to pick up the other, whichever way round.
static BRANCH_A: Migration = Migration {
    group: "guard_probe",
    id: "20260901T1000_from_branch_a",
    draft: false,
    sql: "ALTER TABLE guard_probe ADD COLUMN from_a TEXT",
};
static BRANCH_B: Migration = Migration {
    group: "guard_probe",
    id: "20260902T1000_from_branch_b",
    draft: false,
    sql: "ALTER TABLE guard_probe ADD COLUMN from_b TEXT",
};
static B_ONLY: &[Migration] = &[Migration { ..BRANCH_B }];
static BOTH: &[Migration] = &[Migration { ..BRANCH_A }, Migration { ..BRANCH_B }];

async fn columns(pool: &PgPool) -> Vec<String> {
    sqlx::query_as::<_, (String,)>(
        "SELECT column_name FROM information_schema.columns \
         WHERE table_name = 'guard_probe' ORDER BY column_name",
    )
    .fetch_all(pool)
    .await
    .unwrap()
    .into_iter()
    .map(|(c,)| c)
    .collect()
}

#[sqlx::test]
async fn a_database_on_one_branch_picks_up_the_other_branchs_migration(pool: PgPool) {
    // This database followed branch B, so it has run the LATER id and never
    // saw the earlier one. Merging brings both files in, and the earlier one
    // still has to run: what a database owes is what it has not recorded, not
    // whatever sorts after the newest id it holds.
    apply(&pool, &[&GUARDED], NONE).await.expect("the shape both branched from");
    apply(&pool, &[&GUARDED], B_ONLY).await.expect("branch B");
    assert!(columns(&pool).await.contains(&"from_b".to_string()));
    assert!(!columns(&pool).await.contains(&"from_a".to_string()));

    apply(&pool, &[&GUARDED], BOTH).await.expect("after the merge");
    let after = columns(&pool).await;
    assert!(after.contains(&"from_a".to_string()), "branch A's migration ran late: {after:?}");
    assert!(after.contains(&"from_b".to_string()));

    // And it is recorded, so a third boot runs nothing.
    apply(&pool, &[&GUARDED], BOTH).await.expect("idempotent");
    let (count,): (i64,) =
        sqlx::query_as("SELECT count(*) FROM weft_migration WHERE group_name = 'guard_probe'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(count, 2);
}

// The generator has to write runnable SQL for every kind of change, including
// the destructive ones, rather than leaving a note asking somebody to write it.
#[sqlx::test]
async fn the_plan_writes_sql_for_a_drop_a_type_change_and_a_new_column(pool: PgPool) {
    use weft_task_store::schema_guard::{plan_migration, read_schema};

    sqlx::raw_sql(
        "CREATE TABLE probe (id INT PRIMARY KEY, doomed TEXT, count TEXT NOT NULL DEFAULT '0')",
    )
    .execute(&pool)
    .await
    .unwrap();
    let before = read_schema(&pool).await.unwrap();

    sqlx::raw_sql(
        "DROP TABLE probe; \
         CREATE TABLE probe (id INT PRIMARY KEY, count BIGINT, added TEXT)",
    )
    .execute(&pool)
    .await
    .unwrap();
    let after = read_schema(&pool).await.unwrap();

    let sql: String =
        plan_migration(&before, &after).into_iter().map(|p| p.stmt).collect::<Vec<_>>().join("\n");
    assert!(sql.contains("ALTER TABLE probe DROP COLUMN doomed;"), "{sql}");
    assert!(sql.contains("ALTER TABLE probe ADD COLUMN added text;"), "{sql}");
    assert!(
        sql.contains("ALTER TABLE probe ALTER COLUMN count TYPE bigint USING count::bigint;"),
        "{sql}"
    );
    assert!(sql.contains("ALTER TABLE probe ALTER COLUMN count DROP DEFAULT;"), "{sql}");
    assert!(sql.contains("ALTER TABLE probe ALTER COLUMN count DROP NOT NULL;"), "{sql}");
    assert!(!sql.contains("yourself"), "nothing is left for a human to write: {sql}");
}

// Array and enum columns are the shapes information_schema renders as
// the words ARRAY / USER-DEFINED; the planner must emit their REAL type
// names, proven by executing what it wrote.
#[sqlx::test]
async fn the_plan_renders_array_and_enum_types_as_runnable_sql(pool: PgPool) {
    use weft_task_store::schema_guard::{plan_migration, read_schema};

    sqlx::raw_sql(
        "CREATE TYPE mood AS ENUM ('calm', 'stormy'); \
         CREATE TABLE probe (id INT PRIMARY KEY)",
    )
    .execute(&pool)
    .await
    .unwrap();
    let before = read_schema(&pool).await.unwrap();

    sqlx::raw_sql(
        "ALTER TABLE probe ADD COLUMN tags TEXT[] NOT NULL DEFAULT '{}'::text[]; \
         ALTER TABLE probe ADD COLUMN m mood",
    )
    .execute(&pool)
    .await
    .unwrap();
    let after = read_schema(&pool).await.unwrap();

    let stmts: Vec<String> =
        plan_migration(&before, &after).into_iter().map(|p| p.stmt).collect();
    let sql = stmts.join("\n");
    assert!(sql.contains("text[]"), "array type by its real name: {sql}");
    assert!(sql.contains("mood"), "enum type by its real name: {sql}");
    assert!(!sql.contains("ARRAY") && !sql.contains("USER-DEFINED"), "{sql}");

    // The proof: roll the columns back and let the PLAN re-add them.
    sqlx::raw_sql("ALTER TABLE probe DROP COLUMN tags; ALTER TABLE probe DROP COLUMN m")
        .execute(&pool)
        .await
        .unwrap();
    for stmt in &stmts {
        sqlx::raw_sql(stmt).execute(&pool).await.unwrap_or_else(|e| {
            panic!("planned SQL must run: {e}\n{stmt}");
        });
    }
    if let Some(diff) =
        weft_task_store::schema_guard::diff_things(&after, &read_schema(&pool).await.unwrap())
    {
        panic!("running the plan must land on the target shape:\n{diff}");
    }
}
