//! Layer-3 tests for how the dispatcher holds a task it claimed, against a
//! REAL Postgres: a result kept on a task still claimed, and a claim given
//! back. It all lives IN the SQL, so a faked store would not catch what
//! these catch.
//!
//! Gated behind `db-tests`; `scripts/run-db-tests.sh weft-task-store`
//! runs them.
#![cfg(feature = "db-tests")]

mod support;

use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

use weft_task_store::tasks::{self, claim_one};

use support::setup;

fn dispatcher_task(dedup: &str) -> tasks::NewTask {
    tasks::NewTask {
        kind: "run_node_test".to_string(),
        project_id: Some(Uuid::from_u128(0x1)),
        dedup_key: Some(dedup.to_string()),
        execution_id: None,
        tenant_id: "tenant-1".to_string(),
        payload: json!({}),
    }
}

/// The partial-result surface a non-re-runnable executor uses: a
/// still-claimed row records its harvested result without completing,
/// guarded on the claimant; a later read returns it; a write from a
/// claimant that no longer holds the claim fails loudly.
#[sqlx::test]
async fn partial_result_round_trips_on_the_task_row(pool: PgPool) {
    setup(&pool).await;
    let task_id = tasks::enqueue(&pool, dispatcher_task("t1")).await.expect("enqueue");
    assert_eq!(tasks::stored_result(&pool, task_id).await.expect("read"), None);
    let report = json!({"passed": true, "node": "X", "test": "t"});
    assert!(tasks::store_result_partial(&pool, task_id, "disp-1", &report).await.is_err(), "unclaimed");
    let claimed = claim_one(&pool, "disp-1").await.expect("claim").expect("the task");
    assert_eq!(claimed.id, task_id);
    assert_eq!(claimed.attempts, 1, "first claim");
    assert!(tasks::store_result_partial(&pool, task_id, "disp-2", &report).await.is_err(), "not the claimant");
    tasks::store_result_partial(&pool, task_id, "disp-1", &report).await.expect("store partial");
    assert_eq!(tasks::stored_result(&pool, task_id).await.expect("read"), Some(report));
    let (status,): (String,) = sqlx::query_as("SELECT status FROM task WHERE id = $1").bind(task_id).fetch_one(&pool).await.unwrap();
    assert_eq!(status, "claimed", "recording is not completing");
    assert_eq!(tasks::stored_result(&pool, Uuid::new_v4()).await.expect("read"), None);
}

/// Surrender-requeue: the claimer that can no longer renew its lease puts
/// the row back to `pending`, and a requeue from a claimant that lost the
/// row is a no-op that never clobbers the new claim.
#[sqlx::test]
async fn surrender_requeues_only_while_claim_is_ours(pool: PgPool) {
    setup(&pool).await;
    let task_id = tasks::enqueue(&pool, dispatcher_task("t1")).await.expect("enqueue");
    claim_one(&pool, "disp-1").await.expect("claim").expect("the task");
    assert!(!tasks::surrender(&pool, task_id, "disp-2").await.expect("requeue"));
    assert!(tasks::surrender(&pool, task_id, "disp-1").await.expect("requeue"));
    let (status, claimed_by): (String, Option<String>) =
        sqlx::query_as("SELECT status, claimed_by FROM task WHERE id = $1").bind(task_id).fetch_one(&pool).await.unwrap();
    assert_eq!((status.as_str(), claimed_by), ("pending", None));
    let reclaimed = claim_one(&pool, "disp-2").await.expect("claim").expect("requeued");
    assert_eq!(reclaimed.attempts, 2);
    assert!(!tasks::surrender(&pool, task_id, "disp-1").await.expect("requeue"));
    let (status, claimed_by): (String, Option<String>) =
        sqlx::query_as("SELECT status, claimed_by FROM task WHERE id = $1").bind(task_id).fetch_one(&pool).await.unwrap();
    assert_eq!((status.as_str(), claimed_by.as_deref()), ("claimed", Some("disp-2")));
}
