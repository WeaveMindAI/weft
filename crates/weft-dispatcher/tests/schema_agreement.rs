//! The proof that the two ways of describing the dispatcher's schema
//! agree: the canonical `CREATE TABLE`s and the migration history build
//! the same database. The shared body (what is compared, what a failure
//! means, how origins are enforced) is
//! `weft_task_store::schema_guard::assert_schema_agrees`.
//!
//! Gated behind `db-tests` (off by default) like the other suites that
//! need a real Postgres: `scripts/run-db-tests.sh` runs it.
#![cfg(feature = "db-tests")]

use sqlx::PgPool;

#[sqlx::test]
async fn an_upgraded_database_lands_where_a_fresh_one_starts(pool: PgPool) {
    weft_task_store::schema_guard::assert_schema_agrees(&pool, weft_dispatcher::app::ALL_GROUPS)
        .await;
}

#[sqlx::test]
async fn every_group_survives_the_boot_shape_check(pool: PgPool) {
    weft_task_store::schema_guard::assert_shape_check_restamps(
        &pool,
        weft_dispatcher::app::ALL_GROUPS,
    )
    .await;
}
