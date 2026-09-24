//! Layer-3 tests for asking a task to run again while it is claimed
//! (`enqueue_or_rearm`), against a REAL Postgres: the ask and the
//! claimant's completion race on one row, and only the database's row
//! lock decides which came first.
//!
//! Gated behind `db-tests` like the rest of this crate's database tests;
//! `scripts/run-db-tests.sh weft-task-store` runs them.
#![cfg(feature = "db-tests")]

mod support;

use serde_json::json;
use sqlx::PgPool;

use weft_task_store::tasks::{self, claim_one, enqueue_or_rearm, ClaimFilter, DedupOutcome};
use weft_task_store::TaskTarget;

use support::setup;

fn resume() -> tasks::NewTask {
    tasks::NewTask {
        kind: "resume".to_string(),
        target: TaskTarget::Dispatcher,
        project_id: None,
        dedup_key: Some("color-1:resume".to_string()),
        color: None,
        tenant_id: Some("tenant-1".to_string()),
        target_pod_name: None,
        binary_hash: None,
        payload: json!({}),
    }
}

async fn status_of(pool: &PgPool, id: uuid::Uuid) -> String {
    sqlx::query_scalar("SELECT status FROM task WHERE id = $1").bind(id).fetch_one(pool).await.unwrap()
}

/// Asked again while pending: nothing changes, the pending run will do.
/// Asked again while claimed: the finish puts it back to pending, once,
/// and a finish with no ask ends it. Asked after the finish: a new task.
#[sqlx::test]
async fn a_task_asked_for_again_while_claimed_runs_once_more(pool: PgPool) {
    setup(&pool).await;
    let DedupOutcome::Inserted(id) = enqueue_or_rearm(&pool, resume()).await.unwrap() else {
        panic!("the first ask inserts");
    };
    assert!(matches!(enqueue_or_rearm(&pool, resume()).await.unwrap(), DedupOutcome::AlreadyLive(same) if same == id));

    claim_one(&pool, "disp-1", &ClaimFilter::Dispatcher).await.unwrap().expect("claimed");
    assert!(matches!(enqueue_or_rearm(&pool, resume()).await.unwrap(), DedupOutcome::AlreadyLive(same) if same == id));
    tasks::complete(&pool, id, "disp-1", json!(1)).await.unwrap();
    assert_eq!(status_of(&pool, id).await, "pending", "the ask that came while it ran runs it again");

    claim_one(&pool, "disp-1", &ClaimFilter::Dispatcher).await.unwrap().expect("claimed again");
    tasks::fail(&pool, id, "disp-1", "boom".into()).await.unwrap();
    assert_eq!(status_of(&pool, id).await, "failed", "with no new ask, a finish ends it");

    let DedupOutcome::Inserted(next) = enqueue_or_rearm(&pool, resume()).await.unwrap() else {
        panic!("an ask after the finish is a new task");
    };
    assert_ne!(next, id);
}

/// A claim that ends without finishing (here a requeue) leaves the ask
/// to the next claim, which is the run asked for: its finish ends the
/// task instead of running it a third time.
#[sqlx::test]
async fn the_next_claim_serves_an_ask_left_by_a_claim_that_never_finished(pool: PgPool) {
    setup(&pool).await;
    let DedupOutcome::Inserted(id) = enqueue_or_rearm(&pool, resume()).await.unwrap() else {
        panic!("the first ask inserts");
    };
    claim_one(&pool, "disp-1", &ClaimFilter::Dispatcher).await.unwrap().expect("claimed");
    assert!(matches!(enqueue_or_rearm(&pool, resume()).await.unwrap(), DedupOutcome::AlreadyLive(same) if same == id));
    assert!(tasks::requeue(&pool, id, "disp-1").await.unwrap());

    claim_one(&pool, "disp-2", &ClaimFilter::Dispatcher).await.unwrap().expect("claimed again");
    tasks::complete(&pool, id, "disp-2", json!(1)).await.unwrap();
    assert_eq!(status_of(&pool, id).await, "complete", "the second claim already ran for the ask");
}
