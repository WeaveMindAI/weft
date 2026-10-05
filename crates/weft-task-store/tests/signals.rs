//! Layer-3 tests for the signals that wake waiters, against a REAL
//! Postgres: what matters is that a notification goes out exactly when a
//! write commits (never for one that rolls back), that the triggers fire
//! on exactly the writes someone waits on, and that a waiter hears about
//! a lost connection. A faked store could tell none of that.
//!
//! Gated behind `db-tests` like the rest of this crate's database tests;
//! `scripts/run-db-tests.sh weft-task-store` runs them.
#![cfg(feature = "db-tests")]

mod support;

use std::time::Duration;

use serde_json::json;
use sqlx::PgPool;

use weft_task_store::pg_signal::{Heard, Subscription};
use weft_task_store::tasks::{self, claim_one, TASK_READY_CHANNEL};
use weft_task_store::{PostgresTaskStoreClient, TaskStoreClient, TaskTarget};

use support::{setup, signals};

const PROJECT: uuid::Uuid = uuid::Uuid::from_u128(0xa);

/// Long enough for a notification that was sent to arrive on a loaded
/// machine, so "nothing arrived" means nothing was sent.
const QUIET: Duration = Duration::from_millis(700);

fn task(target: TaskTarget, dedup: &str) -> tasks::NewTask {
    tasks::NewTask {
        kind: "register_signal".to_string(),
        target,
        project_id: (target == TaskTarget::Worker).then_some(PROJECT),
        dedup_key: Some(dedup.to_string()),
        execution_id: None,
        tenant_id: "tenant-1".to_string(),
        target_replica: None,
        binary_hash: None,
        payload: json!({}),
    }
}

/// Everything heard until the line goes quiet.
async fn drain(subscription: &mut Subscription) -> Vec<Heard> {
    let mut heard = Vec::new();
    while let Ok(next) = tokio::time::timeout(QUIET, subscription.next()).await {
        heard.push(next.expect("the watch is running"));
    }
    heard
}

fn on(heard: &[Heard], channel: &str) -> Vec<String> {
    heard
        .iter()
        .filter_map(|h| match h {
            Heard::Signal { channel: c, payload } if *c == channel => Some(payload.to_string()),
            _ => None,
        })
        .collect()
}

#[sqlx::test]
async fn a_committed_notification_is_heard_and_a_rolled_back_one_never_is(pool: PgPool) {
    setup(&pool).await;
    let watch = signals(&pool).await;
    let mut heard = watch.subscribe();

    let mut tx = pool.begin().await.unwrap();
    sqlx::query("SELECT pg_notify($1, 'rolled back')").bind(TASK_READY_CHANNEL).execute(&mut *tx).await.unwrap();
    tx.rollback().await.unwrap();
    let mut tx = pool.begin().await.unwrap();
    sqlx::query("SELECT pg_notify($1, 'committed')").bind(TASK_READY_CHANNEL).execute(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();

    assert_eq!(on(&drain(&mut heard).await, TASK_READY_CHANNEL), vec!["committed".to_string()]);
}

/// A dropped listening connection loses whatever was sent meanwhile, so
/// every waiter is told it is lost, and to look again once it is back;
/// whether it listens can be read in between.
#[sqlx::test]
async fn a_lost_connection_tells_every_waiter_to_recheck(pool: PgPool) {
    setup(&pool).await;
    let watch = signals(&pool).await;
    let mut heard = watch.subscribe();
    let killed: Vec<(bool,)> = sqlx::query_as(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
         WHERE datname = current_database() AND application_name = $1",
    )
    .bind(weft_task_store::pg_signal::WATCH_APPLICATION_NAME)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(killed.len(), 1, "exactly one listening connection per watch");
    let next = tokio::time::timeout(Duration::from_secs(20), heard.next()).await.expect("heard in time");
    assert_eq!(next.unwrap(), Heard::Lost);
    assert!(!heard.listening(), "not listening while it is lost");
    let next = tokio::time::timeout(Duration::from_secs(20), heard.next()).await.expect("heard in time");
    assert_eq!(next.unwrap(), Heard::Recheck);
    assert!(heard.listening(), "listening again");
}

/// A task announces itself when it becomes claimable, and only then:
/// claims, heartbeats and completions are the hot path and stay silent,
/// while a requeue is loud.
#[sqlx::test]
async fn a_task_is_announced_exactly_when_it_becomes_claimable(pool: PgPool) {
    setup(&pool).await;
    let watch = signals(&pool).await;
    let mut heard = watch.subscribe();

    let dispatcher = tasks::enqueue(&pool, task(TaskTarget::Dispatcher, "d")).await.unwrap();
    tasks::enqueue_dedup(&pool, task(TaskTarget::Worker, "w")).await.unwrap();
    assert_eq!(
        on(&drain(&mut heard).await, TASK_READY_CHANNEL),
        vec!["dispatcher".to_string(), format!("worker:{PROJECT}")],
    );

    claim_one(&pool, "disp-1").await.unwrap().expect("claimed");
    tasks::heartbeat(&pool, dispatcher, "disp-1").await.unwrap();
    assert!(on(&drain(&mut heard).await, TASK_READY_CHANNEL).is_empty(), "claim and heartbeat are silent");

    assert!(tasks::requeue(&pool, dispatcher, "disp-1").await.unwrap());
    assert_eq!(on(&drain(&mut heard).await, TASK_READY_CHANNEL), vec!["dispatcher".to_string()]);

    claim_one(&pool, "disp-1").await.unwrap().expect("claimed");
    tasks::complete(&pool, dispatcher, "disp-1", json!(1)).await.unwrap();
    assert!(on(&drain(&mut heard).await, TASK_READY_CHANNEL).is_empty(), "completing is silent");
}

fn cancel(execution_id: &str) -> tasks::NewTask {
    tasks::NewTask {
        kind: "cancel_execution".to_string(),
        target: TaskTarget::Worker,
        project_id: Some(PROJECT),
        dedup_key: Some(format!("{execution_id}:cancel")),
        execution_id: Some(execution_id.to_string()),
        tenant_id: "tenant-1".to_string(),
        target_replica: None,
        binary_hash: None,
        payload: json!({ "project_id": PROJECT, "execution_id": execution_id, "cause": { "kind": "user" } }),
    }
}

/// A worker's cancel wait held open ends the moment a cancel for one of
/// its executions is enqueued, and comes back empty at its deadline when none
/// is.
#[sqlx::test]
async fn a_held_cancel_wait_ends_when_a_cancel_arrives_or_at_its_deadline(pool: PgPool) {
    setup(&pool).await;
    let client = PostgresTaskStoreClient::new(pool.clone(), signals(&pool).await).expect("client");
    let execution_ids = vec!["c1".to_string()];

    let started = tokio::time::Instant::now();
    assert!(client.wait_cancels(PROJECT, execution_ids.clone(), Duration::from_millis(500)).await.unwrap().is_empty());
    assert!(started.elapsed() >= Duration::from_millis(500));

    let enqueuer = {
        let pool = pool.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            tasks::enqueue_dedup(&pool, cancel("c1")).await.unwrap();
        })
    };
    let started = tokio::time::Instant::now();
    let taken = client.wait_cancels(PROJECT, execution_ids, Duration::from_secs(20)).await.unwrap();
    enqueuer.await.unwrap();
    assert_eq!(taken.len(), 1);
    assert!(started.elapsed() < Duration::from_secs(10), "woken, not timed out: {:?}", started.elapsed());
}

/// The subscription is taken before the first look, so a cancel that
/// lands anywhere around it (before it, during it, just after the empty
/// answer) still ends the hold. Raced many times with the enqueue at
/// shifting offsets, since a subscribe-after-look window is only a few
/// microseconds wide.
#[sqlx::test]
async fn a_cancel_landing_around_the_wait_is_never_missed(pool: PgPool) {
    setup(&pool).await;
    let client = std::sync::Arc::new(PostgresTaskStoreClient::new(pool.clone(), signals(&pool).await).expect("client"));
    for round in 0..40u64 {
        let execution_id = format!("race-{round}");
        let waiter = {
            let client = client.clone();
            let execution_ids = vec![execution_id.clone()];
            tokio::spawn(async move { client.wait_cancels(PROJECT, execution_ids, Duration::from_secs(20)).await })
        };
        tokio::time::sleep(Duration::from_micros(round * 150)).await;
        tasks::enqueue_dedup(&pool, cancel(&execution_id)).await.unwrap();
        let taken = tokio::time::timeout(Duration::from_secs(10), waiter)
            .await
            .unwrap_or_else(|_| panic!("round {round}: the wait missed its cancel"))
            .unwrap()
            .unwrap();
        assert_eq!(taken.len(), 1, "round {round}");
    }
}
