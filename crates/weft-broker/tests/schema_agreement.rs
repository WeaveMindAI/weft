//! The broker owns the runtime-file tables, so it proves its own
//! canonical DDL and migration history agree. The shared body (what is
//! compared, what a failure means, how origins are enforced) is
//! `weft_task_store::schema_guard::assert_schema_agrees`.
#![cfg(feature = "db-tests")]

use sqlx::PgPool;

#[sqlx::test]
async fn an_upgraded_database_lands_where_a_fresh_one_starts(pool: PgPool) {
    weft_task_store::schema_guard::assert_schema_agrees(
        &pool,
        &[&weft_broker::runtime_store::GROUP],
    )
    .await;
}

#[sqlx::test]
async fn every_group_survives_the_boot_shape_check(pool: PgPool) {
    weft_task_store::schema_guard::assert_shape_check_restamps(
        &pool,
        &[&weft_broker::runtime_store::GROUP],
    )
    .await;
}
