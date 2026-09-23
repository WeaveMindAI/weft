//! Layer-3 tests for waiting on a task, against a REAL Postgres: the
//! wake-up is a Postgres notification sent by the terminal write itself,
//! so a faked store could not tell whether it is sent, or sent at commit.
//!
//! Gated behind `db-tests` like the rest of this crate's database tests;
//! `scripts/run-db-tests.sh weft-task-store` runs them.
#![cfg(feature = "db-tests")]

mod support;

use std::time::{Duration, Instant};

use serde_json::json;
use sqlx::PgPool;

use weft_task_store::tasks::{self, claim_one, ClaimFilter};
use weft_task_store::{PostgresTaskStoreClient, TaskStatus, TaskStoreClient, TaskTarget};

use support::setup;

/// Far below the 30s the waits are given, far above any wake-up: a wait
/// that ends before this was woken, not timed out.
const WOKEN: Duration = Duration::from_secs(10);

fn task(dedup: &str) -> tasks::NewTask {
    tasks::NewTask {
        kind: "register_signal".to_string(),
        target: TaskTarget::Dispatcher,
        project_id: None,
        dedup_key: Some(dedup.to_string()),
        color: None,
        tenant_id: Some("tenant-1".to_string()),
        target_pod_name: None,
        binary_hash: None,
        payload: json!({}),
    }
}

/// The wait ends when the task does: nothing polls, so without the
/// notification the wait would run to its 30s timeout. Anything well
/// short of that (the bound leaves room for a loaded machine) is the
/// notification waking it.
#[sqlx::test]
async fn a_wait_ends_the_moment_the_task_completes(pool: PgPool) {
    setup(&pool).await;
    let client = PostgresTaskStoreClient::new(pool.clone());
    let id = tasks::enqueue(&pool, task("complete")).await.expect("enqueue");

    let finisher = {
        let pool = pool.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            claim_one(&pool, "disp-1", ClaimFilter::Dispatcher).await.expect("claim").expect("the task");
            tasks::complete(&pool, id, "disp-1", json!({"token": "t"})).await.expect("complete");
        })
    };
    let started = Instant::now();
    let outcome = client.wait_for_terminal(id, Duration::from_secs(30)).await.expect("wait");
    let waited = started.elapsed();
    finisher.await.unwrap();

    assert_eq!(outcome.status, TaskStatus::Complete);
    assert_eq!(outcome.result, Some(json!({"token": "t"})));
    assert!(waited < WOKEN, "woken by the notification, not the timeout: {waited:?}");
}

/// A failed task wakes its waiter the same way, and so does failing a
/// task nobody ever claimed.
#[sqlx::test]
async fn a_wait_ends_the_moment_the_task_fails(pool: PgPool) {
    setup(&pool).await;
    let client = PostgresTaskStoreClient::new(pool.clone());
    let claimed = tasks::enqueue(&pool, task("fail")).await.expect("enqueue");
    let pending = tasks::enqueue(&pool, task("fail-pending")).await.expect("enqueue");

    let finisher = {
        let pool = pool.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            let first = claim_one(&pool, "disp-1", ClaimFilter::Dispatcher).await.expect("claim").expect("a task");
            tasks::fail(&pool, first.id, "disp-1", "boom".into()).await.expect("fail");
            let other = if first.id == claimed { pending } else { claimed };
            // Whichever is left: failed where it stands if pending, or
            // after claiming it.
            if !tasks::fail_pending(&pool, other, "never ran").await.expect("fail pending") {
                tasks::fail(&pool, other, "disp-1", "boom".into()).await.expect("fail");
            }
        })
    };
    let started = Instant::now();
    let (a, b) = tokio::join!(
        client.wait_for_terminal(claimed, Duration::from_secs(30)),
        client.wait_for_terminal(pending, Duration::from_secs(30)),
    );
    finisher.await.unwrap();
    assert_eq!(a.expect("wait").status, TaskStatus::Failed);
    assert_eq!(b.expect("wait").status, TaskStatus::Failed);
    assert!(started.elapsed() < WOKEN, "{:?}", started.elapsed());
}

/// A task that is already done answers at once, and one that never
/// finishes answers when the timeout passes, with its live status.
#[sqlx::test]
async fn a_done_task_answers_at_once_and_an_unfinished_one_at_the_timeout(pool: PgPool) {
    setup(&pool).await;
    let client = PostgresTaskStoreClient::new(pool.clone());
    let done = tasks::enqueue(&pool, task("done")).await.expect("enqueue");
    claim_one(&pool, "disp-1", ClaimFilter::Dispatcher).await.expect("claim").expect("the task");
    tasks::complete(&pool, done, "disp-1", json!(1)).await.expect("complete");
    assert_eq!(client.wait_for_terminal(done, Duration::from_secs(30)).await.unwrap().status, TaskStatus::Complete);

    let open = tasks::enqueue(&pool, task("open")).await.expect("enqueue");
    let started = Instant::now();
    let outcome = client.wait_for_terminal(open, Duration::from_millis(500)).await.expect("wait");
    assert_eq!(outcome.status, TaskStatus::Pending);
    assert!(started.elapsed() >= Duration::from_millis(500));
}
