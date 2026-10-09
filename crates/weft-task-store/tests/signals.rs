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

use support::{setup, signals};

/// Long enough for a notification that was sent to arrive on a loaded
/// machine, so "nothing arrived" means nothing was sent.
const QUIET: Duration = Duration::from_millis(700);

fn task(dedup: &str) -> tasks::NewTask {
    tasks::NewTask {
        kind: "register_signal".to_string(),
        project_id: None,
        dedup_key: Some(dedup.to_string()),
        execution_id: None,
        tenant_id: "tenant-1".to_string(),
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

/// Cut every listening connection of the test database, as a database
/// going to sleep does.
async fn cut_the_listener(pool: &PgPool) {
    let killed: Vec<(bool,)> = sqlx::query_as(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
         WHERE datname = current_database() AND application_name = $1",
    )
    .bind(weft_task_store::pg_signal::WATCH_APPLICATION_NAME)
    .fetch_all(pool)
    .await
    .unwrap();
    assert_eq!(killed.len(), 1, "exactly one listening connection per watch");
}

/// A process that scales to zero listens only while it has work: a
/// connection that drops while nothing is busy (the database going to
/// sleep) stays down, so listening does not wake the database straight
/// back up, until work arrives, which waits for it to listen again and is
/// told it resumed; a drop while something is busy reconnects at once.
#[sqlx::test]
async fn a_quiet_watch_listens_again_only_when_work_arrives(pool: PgPool) {
    setup(&pool).await;
    let watch = weft_task_store::pg_signal::PgSignalWatch::start(
        &pool.connect_options(),
        support::CHANNELS,
        weft_task_store::pg_signal::Listening::WhileBusy,
    )
    .await
    .unwrap();
    let mut heard = watch.subscribe();
    // Past the moment just after work, when a drop still counts as busy.
    tokio::time::sleep(Duration::from_secs(6)).await;
    cut_the_listener(&pool).await;
    let next = tokio::time::timeout(Duration::from_secs(20), heard.next()).await.expect("heard in time");
    assert_eq!(next.unwrap(), Heard::Lost);
    assert!(tokio::time::timeout(Duration::from_secs(3), heard.next()).await.is_err(), "nothing while quiet");
    assert!(!heard.listening());

    let busy = watch.busy().await;
    assert!(heard.listening(), "work waits for the watch to listen");
    let next = tokio::time::timeout(Duration::from_secs(20), heard.next()).await.expect("heard in time");
    assert_eq!(next.unwrap(), Heard::Resumed);

    cut_the_listener(&pool).await;
    let next = tokio::time::timeout(Duration::from_secs(20), heard.next()).await.expect("heard in time");
    assert_eq!(next.unwrap(), Heard::Lost);
    let next = tokio::time::timeout(Duration::from_secs(20), heard.next()).await.expect("heard in time");
    assert_eq!(next.unwrap(), Heard::Recheck, "busy: listening again at once");
    drop(busy);
}

/// A task announces itself when it becomes claimable, and only then:
/// claims, heartbeats and completions are the hot path and stay silent,
/// while a requeue is loud.
#[sqlx::test]
async fn a_task_is_announced_exactly_when_it_becomes_claimable(pool: PgPool) {
    setup(&pool).await;
    let watch = signals(&pool).await;
    let mut heard = watch.subscribe();

    let work = tasks::enqueue(&pool, task("d")).await.unwrap();
    assert!(!on(&drain(&mut heard).await, TASK_READY_CHANNEL).is_empty(), "new work wakes the pickers");

    assert_eq!(claim_one(&pool, "disp-1").await.unwrap().expect("claimed").id, work);
    tasks::heartbeat(&pool, work, "disp-1").await.unwrap();
    assert!(on(&drain(&mut heard).await, TASK_READY_CHANNEL).is_empty(), "claim and heartbeat are silent");

    assert!(tasks::surrender(&pool, work, "disp-1").await.unwrap());
    assert_eq!(on(&drain(&mut heard).await, TASK_READY_CHANNEL), vec![String::new()]);

    assert_eq!(claim_one(&pool, "disp-1").await.unwrap().expect("claimed").id, work);
    tasks::complete(&pool, work, "disp-1", json!(1)).await.unwrap();
    assert!(on(&drain(&mut heard).await, TASK_READY_CHANNEL).is_empty(), "completing is silent");
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
