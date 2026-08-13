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

use weft_task_store::{apply_groups, SchemaGroup};

static GUARDED: SchemaGroup = SchemaGroup {
    name: "guard_probe",
    tables: &["guard_probe"],
    ddl: &[
        "CREATE TABLE IF NOT EXISTS guard_probe (id INT PRIMARY KEY, note TEXT)",
        "CREATE INDEX IF NOT EXISTS idx_guard_probe_note ON guard_probe(note)",
    ],
};

static OTHER: SchemaGroup = SchemaGroup {
    name: "guard_other",
    tables: &["guard_other"],
    ddl: &["CREATE TABLE IF NOT EXISTS guard_other (id INT PRIMARY KEY)"],
};

#[sqlx::test]
async fn fresh_apply_stamps_and_reruns_are_idempotent(pool: PgPool) {
    apply_groups(&pool, &[&GUARDED, &OTHER]).await.expect("fresh apply");
    let (fp,): (String,) = sqlx::query_as(
        "SELECT fingerprint FROM weft_schema_stamp WHERE group_name = 'guard_probe'",
    )
    .fetch_one(&pool)
    .await
    .expect("stamp row exists");
    assert_eq!(fp.len(), 64, "hex sha256 stamp");

    // Re-running with the same groups succeeds and leaves the stamp.
    apply_groups(&pool, &[&GUARDED, &OTHER]).await.expect("re-apply");
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
    apply_groups(&pool, &[&GUARDED]).await.expect("repair apply");
    sqlx::query("SELECT 1 FROM guard_probe")
        .execute(&pool)
        .await
        .expect("table recreated");
}

#[sqlx::test]
async fn mismatched_fingerprint_fails_naming_group_and_drops(pool: PgPool) {
    apply_groups(&pool, &[&GUARDED, &OTHER]).await.expect("fresh apply");
    // Simulate a database created by a different build of the DDL.
    sqlx::query(
        "UPDATE weft_schema_stamp SET fingerprint = 'stale' WHERE group_name = 'guard_probe'",
    )
    .execute(&pool)
    .await
    .unwrap();

    let err = apply_groups(&pool, &[&GUARDED, &OTHER])
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
    apply_groups(&pool, &[&GUARDED, &OTHER]).await.expect("recovered apply");
}

#[sqlx::test]
async fn a_mismatch_rolls_back_every_group_in_the_call(pool: PgPool) {
    // Stamp only GUARDED, then poison it: the next call carries a FRESH
    // group alongside the mismatched one. The fresh group's DDL and stamp
    // run before the bail inside the same transaction, so the failure
    // must discard BOTH: a half-applied boot would leave the fresh group
    // stamped by a run that reported failure.
    apply_groups(&pool, &[&GUARDED]).await.expect("fresh apply");
    sqlx::query(
        "UPDATE weft_schema_stamp SET fingerprint = 'stale' WHERE group_name = 'guard_probe'",
    )
    .execute(&pool)
    .await
    .unwrap();

    apply_groups(&pool, &[&GUARDED, &OTHER])
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
    let (a, b) = tokio::join!(apply_groups(&pool, &groups), apply_groups(&pool, &groups));
    a.expect("first concurrent apply");
    b.expect("second concurrent apply");
    sqlx::query("SELECT 1 FROM guard_probe").execute(&pool).await.expect("table exists");
}
