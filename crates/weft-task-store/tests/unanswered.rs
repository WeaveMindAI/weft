//! weft's own calls that keep failing, against a real Postgres: what a
//! project's status is shown is what the SQL keeps and reads back.
//!
//! Gated behind `db-tests`; `scripts/run-db-tests.sh weft-task-store`
//! runs them.
#![cfg(feature = "db-tests")]

use sqlx::PgPool;
use uuid::Uuid;

use weft_task_store::unanswered::{self, Callee};

/// A failing role shows for every project, a project's workers only for
/// that project; a second failure keeps when the stretch began and takes
/// the newest error; an answer clears it; a row whose last failure is old
/// is no longer shown.
#[sqlx::test]
async fn a_call_that_keeps_failing_is_shown_to_the_projects_it_holds_up(pool: PgPool) {
    weft_task_store::apply_groups(&pool, &[&unanswered::GROUP]).await.expect("schema");
    let (mine, theirs) = (Uuid::from_u128(1), Uuid::from_u128(2));

    unanswered::failed(&pool, Callee::Role("supervisor"), "411 Length Required").await.unwrap();
    unanswered::failed(&pool, Callee::Workers(theirs), "connection refused").await.unwrap();
    let shown = unanswered::failing_for(&pool, mine).await.unwrap();
    assert_eq!(shown.len(), 1, "another project's workers are not mine: {shown:?}");
    assert_eq!(shown[0].role.as_deref(), Some("supervisor"));

    let workers_of = async |project| unanswered::failing_for(&pool, project).await.unwrap().into_iter().find(|f| f.role.is_none());
    unanswered::failed(&pool, Callee::Workers(mine), "first").await.unwrap();
    let since = workers_of(mine).await.unwrap().since_ms;
    unanswered::failed(&pool, Callee::Workers(mine), "second").await.unwrap();
    let workers = workers_of(mine).await.unwrap();
    assert_eq!((workers.error.as_str(), workers.since_ms), ("second", since), "the stretch keeps its start");

    unanswered::answered(&pool, Callee::Role("supervisor")).await.unwrap();
    let shown = unanswered::failing_for(&pool, mine).await.unwrap();
    assert!(shown.iter().all(|f| f.role.is_none()), "an answered ring is cleared: {shown:?}");

    sqlx::query("UPDATE unanswered_call SET last_ms = last_ms - $1 - 1").bind(unanswered::RECENT_MS).execute(&pool).await.unwrap();
    assert!(unanswered::failing_for(&pool, mine).await.unwrap().is_empty(), "an old failure is not shown");
}

/// A ring given up on stays written down however old it is, so the next
/// process that starts rings that role again; a project's workers are not
/// a role and are never in that list.
#[sqlx::test]
async fn a_ring_given_up_on_is_found_again_at_the_next_start(pool: PgPool) {
    weft_task_store::apply_groups(&pool, &[&unanswered::GROUP]).await.expect("schema");
    unanswered::failed(&pool, Callee::Role("supervisor"), "503").await.unwrap();
    unanswered::failed(&pool, Callee::Workers(Uuid::from_u128(1)), "connection refused").await.unwrap();
    sqlx::query("UPDATE unanswered_call SET last_ms = last_ms - $1 - 1").bind(unanswered::RECENT_MS).execute(&pool).await.unwrap();
    assert_eq!(unanswered::unanswered_roles(&pool).await.unwrap(), vec!["supervisor".to_string()]);
    unanswered::answered(&pool, Callee::Role("supervisor")).await.unwrap();
    assert!(unanswered::unanswered_roles(&pool).await.unwrap().is_empty());
}
