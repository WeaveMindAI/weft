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
use weft_task_store::tasks::{self, claim_one, ClaimFilter, TASK_READY_CHANNEL};
use weft_task_store::worker_pod::{self, register_alive, AliveTransition, WORKER_POD_CHANNEL};
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
        color: None,
        tenant_id: Some("tenant-1".to_string()),
        target_pod_name: None,
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

async fn alive_pod(pool: &PgPool, pod: &str) {
    worker_pod::insert_spawning(pool, pod, PROJECT, "ns", "disp-1", Some("bin"), "worker", None)
        .await
        .expect("insert_spawning");
    register_alive(pool, pod, PROJECT, AliveTransition::FromSpawning).await.expect("register_alive");
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
/// every waiter is told to look again once it is back.
#[sqlx::test]
async fn a_lost_connection_tells_every_waiter_to_recheck(pool: PgPool) {
    setup(&pool).await;
    let watch = signals(&pool).await;
    let mut heard = watch.subscribe();
    let killed: Vec<(bool,)> = sqlx::query_as(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
         WHERE datname = current_database() AND query LIKE 'LISTEN%'",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(killed.len(), 1, "exactly one listening connection per watch");
    let next = tokio::time::timeout(Duration::from_secs(20), heard.next()).await.expect("heard in time");
    assert_eq!(next.unwrap(), Heard::Recheck);
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

    claim_one(&pool, "disp-1", &ClaimFilter::Dispatcher).await.unwrap().expect("claimed");
    tasks::heartbeat(&pool, dispatcher, "disp-1").await.unwrap();
    assert!(on(&drain(&mut heard).await, TASK_READY_CHANNEL).is_empty(), "claim and heartbeat are silent");

    assert!(tasks::requeue(&pool, dispatcher, "disp-1").await.unwrap());
    assert_eq!(on(&drain(&mut heard).await, TASK_READY_CHANNEL), vec!["dispatcher".to_string()]);

    claim_one(&pool, "disp-1", &ClaimFilter::Dispatcher).await.unwrap().expect("claimed");
    tasks::complete(&pool, dispatcher, "disp-1", json!(1)).await.unwrap();
    assert!(on(&drain(&mut heard).await, TASK_READY_CHANNEL).is_empty(), "completing is silent");
}

/// A pod row speaks up when the project's capacity changes, never on a
/// plain heartbeat.
#[sqlx::test]
async fn a_worker_pod_is_announced_when_capacity_changes(pool: PgPool) {
    setup(&pool).await;
    let watch = signals(&pool).await;
    let mut heard = watch.subscribe();
    let sat = weft_platform_traits::SATURATION_MEM_FRACTION;

    alive_pod(&pool, "pod-a").await;
    assert_eq!(on(&drain(&mut heard).await, WORKER_POD_CHANNEL), vec![PROJECT.to_string(); 2]);

    worker_pod::heartbeat(&pool, "pod-a", sat - 0.05).await.unwrap();
    assert!(on(&drain(&mut heard).await, WORKER_POD_CHANNEL).is_empty(), "a heartbeat under the line is silent");

    worker_pod::heartbeat(&pool, "pod-a", sat).await.unwrap();
    assert_eq!(on(&drain(&mut heard).await, WORKER_POD_CHANNEL), vec![PROJECT.to_string()], "crossing the line");

    worker_pod::mark_dead(&pool, "pod-a").await.unwrap();
    assert_eq!(on(&drain(&mut heard).await, WORKER_POD_CHANNEL), vec![PROJECT.to_string()]);
}

/// A worker's claim held open ends the moment a task for its project is
/// enqueued, and comes back empty at its deadline when none is.
#[sqlx::test]
async fn a_held_claim_ends_when_a_task_arrives_or_at_its_deadline(pool: PgPool) {
    setup(&pool).await;
    alive_pod(&pool, "pod-a").await;
    let client = PostgresTaskStoreClient::new(pool.clone(), signals(&pool).await).expect("client");
    let filter = ClaimFilter::Worker { project_id: PROJECT };

    let started = tokio::time::Instant::now();
    assert!(client.claim_one("pod-a", filter.clone(), Duration::from_millis(500)).await.unwrap().is_none());
    assert!(started.elapsed() >= Duration::from_millis(500));

    let enqueuer = {
        let pool = pool.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            tasks::enqueue(&pool, task(TaskTarget::Worker, "late")).await.unwrap();
        })
    };
    let started = tokio::time::Instant::now();
    let claimed = client.claim_one("pod-a", filter, Duration::from_secs(20)).await.unwrap();
    enqueuer.await.unwrap();
    assert!(claimed.is_some());
    assert!(started.elapsed() < Duration::from_secs(10), "woken, not timed out: {:?}", started.elapsed());
}

/// The subscription is taken before the first claim, so a task that
/// lands anywhere around the claim (before it, during it, just after
/// the empty answer) still ends the hold. Raced many times with the
/// enqueue at shifting offsets, since a subscribe-after-claim window is
/// only a few microseconds wide.
#[sqlx::test]
async fn a_task_landing_around_the_claim_is_never_missed(pool: PgPool) {
    setup(&pool).await;
    alive_pod(&pool, "pod-a").await;
    let client = std::sync::Arc::new(
        PostgresTaskStoreClient::new(pool.clone(), signals(&pool).await).expect("client"),
    );
    let filter = ClaimFilter::Worker { project_id: PROJECT };
    for round in 0..40u64 {
        let claimer = {
            let client = client.clone();
            let filter = filter.clone();
            tokio::spawn(async move { client.claim_one("pod-a", filter, Duration::from_secs(20)).await })
        };
        tokio::time::sleep(Duration::from_micros(round * 150)).await;
        tasks::enqueue(&pool, task(TaskTarget::Worker, &format!("race-{round}"))).await.unwrap();
        let claimed = tokio::time::timeout(Duration::from_secs(10), claimer)
            .await
            .unwrap_or_else(|_| panic!("round {round}: the hold missed its task"))
            .unwrap()
            .unwrap();
        let claimed = claimed.expect("claimed");
        tasks::complete(&pool, claimed.id, "pod-a", json!(null)).await.unwrap();
    }
}
