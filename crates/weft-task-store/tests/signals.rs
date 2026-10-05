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
    // Sent in one batch or two (`announce`), in no promised order.
    let mut ready = on(&drain(&mut heard).await, TASK_READY_CHANNEL);
    ready.sort();
    assert_eq!(ready, vec!["dispatcher".to_string(), format!("worker:{PROJECT}")]);

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

/// A cancel is announced to the workers of its project with the
/// execution it stops, never as work to claim.
#[sqlx::test]
async fn a_cancel_is_announced_with_its_execution(pool: PgPool) {
    setup(&pool).await;
    let watch = signals(&pool).await;
    let mut heard = watch.subscribe();
    let client = PostgresTaskStoreClient::new(pool.clone(), watch.clone()).expect("client");

    tasks::enqueue_dedup(&pool, cancel("c1")).await.unwrap();
    let heard = drain(&mut heard).await;
    assert_eq!(on(&heard, tasks::CANCEL_CHANNEL), vec![tasks::cancel_payload(PROJECT, "c1")]);
    assert!(on(&heard, TASK_READY_CHANNEL).is_empty(), "a cancel is no work for a picker");
    assert_eq!(tasks::parse_cancel_payload(&tasks::cancel_payload(PROJECT, "c1")), Some((PROJECT, "c1")));

    assert!(client.cancels_asked(PROJECT, vec!["other".into()]).await.unwrap().is_empty());
    assert_eq!(client.cancels_asked(PROJECT, vec!["c1".into()]).await.unwrap().len(), 1);
}

/// What a write leaves in the outbox goes out once it commits and its
/// writer pokes the flusher, each announcement once however many times it
/// was left; what a rolled-back write left never does.
#[sqlx::test]
async fn an_announcement_goes_out_once_its_write_commits(pool: PgPool) {
    setup(&pool).await;
    let watch = signals(&pool).await;
    let mut heard = watch.subscribe();

    let mut tx = pool.begin().await.unwrap();
    sqlx::query("SELECT weft_announce($1, 'rolled back')").bind(TASK_READY_CHANNEL).execute(&mut *tx).await.unwrap();
    tx.rollback().await.unwrap();
    let mut tx = pool.begin().await.unwrap();
    for _ in 0..3 {
        sqlx::query("SELECT weft_announce($1, 'committed')").bind(TASK_READY_CHANNEL).execute(&mut *tx).await.unwrap();
    }
    tx.commit().await.unwrap();
    assert!(on(&drain(&mut heard).await, TASK_READY_CHANNEL).is_empty(), "nothing goes out before the flusher is poked");
    weft_task_store::announce::committed(&pool);
    assert_eq!(on(&drain(&mut heard).await, TASK_READY_CHANNEL), vec!["committed".to_string()]);
    let left: i64 = sqlx::query_scalar("SELECT count(*) FROM weft_announcement").fetch_one(&pool).await.unwrap();
    assert_eq!(left, 0, "the outbox is empty once sent");
}
