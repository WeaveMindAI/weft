//! Layer-3 tests for the workers' door tables against a REAL Postgres: a
//! worker's lease coming alive is announced once per stretch of work, never
//! per tick, and the rows the ticks leave behind are pruned in bounded
//! batches.

use crate::support;

use std::time::Duration;

use uuid::Uuid;
use weft_task_store::db::{self, Client as _, Pool, Session};
use weft_task_store::worker_door::{prune, LEASE_ALIVE_CHANNEL, LEASES_KEPT_SECS};

use support::setup;

const PROJECT: Uuid = Uuid::from_u128(1);

/// One door tick of `replica`, its lease held until `until`, counting
/// `counts` this minute, saying whether it `shortens` a lease it vouched
/// for; what the tick answers.
async fn tick_with(pool: &Pool, replica: &str, until: i64, counts: &[(&str, i64)], shortens: bool) -> serde_json::Value {
    let counts: Vec<(&str, i64, Option<&str>)> = counts.iter().map(|(k, h)| (*k, *h, None)).collect();
    tick_counting(pool, replica, until, &counts, shortens).await
}

/// [`tick_with`], each count with the reason it keeps, if any.
async fn tick_counting(pool: &Pool, replica: &str, until: i64, counts: &[(&str, i64, Option<&str>)], shortens: bool) -> serde_json::Value {
    let keys: Vec<&str> = counts.iter().map(|(k, _, _)| *k).collect();
    let hits: Vec<i64> = counts.iter().map(|(_, h, _)| *h).collect();
    let reasons: Vec<Option<&str>> = counts.iter().map(|(_, _, r)| *r).collect();
    let none = std::collections::BTreeMap::<String, u32>::new();
    let room: Option<i64> = None;
    pool.scalar::<db::Json<serde_json::Value>>(
        "SELECT weft_door_tick($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)",
        &[&replica, &PROJECT, &"tenant-1", &"bin-A", &until, &db::Json(db::WeftMade(&none)), &0i64, &keys, &hits, &reasons, &shortens, &room, &room, &room],
    )
    .await
    .unwrap()
    .0
}

/// One door tick of `replica`, its lease held until `until`.
async fn tick(pool: &Pool, replica: &str, until: i64) {
    tick_with(pool, replica, until, &[], false).await;
}

/// The next announcement of a lease coming alive, if one comes soon.
async fn heard(listener: &mut Session) -> Option<String> {
    tokio::time::timeout(Duration::from_millis(500), listener.notified())
        .await
        .ok()
        .map(|n| n.unwrap().expect("the listening connection is open").payload().to_string())
}

/// A worker's first tick announces its lease; the ticks that renew it say
/// nothing; a tick renewing a lease that had lapsed (the worker went quiet
/// and works again) announces it once more. The lease keeps when it last
/// came alive.
#[weft_db_macros::test]
async fn a_lease_coming_alive_is_announced_once_per_stretch(pool: Pool) {
    setup(&pool).await;
    let mut listener = Session::connect(pool.config()).await.unwrap();
    listener.listen([LEASE_ALIVE_CHANNEL]).await.unwrap();
    let now = chrono::Utc::now().timestamp();
    tick(&pool, "worker-a", now + 15).await;
    assert_eq!(heard(&mut listener).await.as_deref(), Some("worker-a"), "made");
    let made = alive_since(&pool).await;
    assert!(made >= now - 1, "alive since it was made: {made}");
    pool.execute("UPDATE worker_lease SET alive_since = 1", &[]).await.unwrap();
    tick(&pool, "worker-a", now + 16).await;
    assert_eq!(heard(&mut listener).await, None, "renewed: nothing to say");
    assert_eq!(alive_since(&pool).await, 1, "a renewal keeps when it came alive");
    pool.execute("UPDATE worker_lease SET leased_until_unix = $1", &[&(now - 30)]).await.unwrap();
    assert_eq!(heard(&mut listener).await, None, "a lease running out says nothing either");
    tick(&pool, "worker-a", now + 15).await;
    assert_eq!(heard(&mut listener).await.as_deref(), Some("worker-a"), "alive again");
    assert!(alive_since(&pool).await >= now - 1, "and alive since then");
}

/// When the one worker's lease last came alive.
async fn alive_since(pool: &Pool) -> i64 {
    pool.scalar("SELECT alive_since FROM worker_lease", &[]).await.unwrap()
}

/// Old minutes' counts and leases long lapsed go, the rest stay.
#[weft_db_macros::test]
async fn old_counts_and_long_lapsed_leases_are_pruned(pool: Pool) {
    setup(&pool).await;
    let now = chrono::Utc::now().timestamp();
    tick(&pool, "gone", now - LEASES_KEPT_SECS - 10).await;
    tick(&pool, "alive", now + 15).await;
    for (window, replica) in [(now - 3600, "gone"), (now, "alive")] {
        pool.execute(
            "INSERT INTO door_count (key, window_start, replica, project_id, hits) VALUES ('e:t', $1, $2, $3, 1)",
            &[&window, &replica, &PROJECT],
        )
        .await
        .unwrap();
    }
    prune(&pool, now).await.unwrap();
    let leases: Vec<String> = pool.scalars("SELECT replica FROM worker_lease", &[]).await.unwrap();
    assert_eq!(leases, vec!["alive"]);
    let counts: Vec<String> = pool.scalars("SELECT replica FROM door_count", &[]).await.unwrap();
    assert_eq!(counts, vec!["alive"]);
}

/// A tick that shortens a lease the worker vouched for rings the look at
/// lapsed leases again, though the lease is alive and was renewed.
#[weft_db_macros::test]
async fn a_tick_that_shortens_a_vouched_lease_is_announced(pool: Pool) {
    setup(&pool).await;
    let mut listener = Session::connect(pool.config()).await.unwrap();
    listener.listen([LEASE_ALIVE_CHANNEL]).await.unwrap();
    let now = chrono::Utc::now().timestamp();
    tick(&pool, "worker-a", now + 3600).await;
    assert_eq!(heard(&mut listener).await.as_deref(), Some("worker-a"), "made");
    tick_with(&pool, "worker-a", now + 15, &[], true).await;
    assert_eq!(heard(&mut listener).await.as_deref(), Some("worker-a"), "shortened: rung again");
}

/// A tick reads back every key the project's other copies counted, except
/// the refused-token keys each copy bounds alone.
#[weft_db_macros::test]
async fn a_tick_hears_the_other_copies_counts_but_their_refused_tokens(pool: Pool) {
    setup(&pool).await;
    let now = chrono::Utc::now().timestamp();
    tick_with(&pool, "worker-b", now + 15, &[("e:route:1", 3), ("t:-:1.2.3.4", 9)], false).await;
    let answer = tick_with(&pool, "worker-a", now + 15, &[], false).await;
    assert_eq!(answer["others"], serde_json::json!([{ "key": "e:route:1", "hits": 3 }]));
}

/// A tick keeps the reason a count carries, replaced by the next tick's
/// along with its hits, and none for a count that carries none.
#[weft_db_macros::test]
async fn a_tick_keeps_the_reason_a_count_carries(pool: Pool) {
    setup(&pool).await;
    let now = chrono::Utc::now().timestamp();
    tick_counting(&pool, "worker-a", now + 15, &[("dropped:t1", 1, Some("the trigger is gone")), ("ran:t1", 2, None)], false).await;
    tick_counting(&pool, "worker-a", now + 15, &[("dropped:t1", 3, Some("package 'relay' didn't compile")), ("ran:t1", 2, None)], false).await;
    let rows: Vec<(String, i64, Option<String>)> =
        pool.all("SELECT key, hits, reason FROM door_count ORDER BY key", &[]).await.unwrap();
    assert_eq!(
        rows,
        [("dropped:t1".to_string(), 3, Some("package 'relay' didn't compile".to_string())), ("ran:t1".to_string(), 2, None)]
    );
}

/// A drop the install counts itself adds to its own row of the minute,
/// keeping the last reason, beside a worker's count of the same key.
#[weft_db_macros::test]
async fn a_drop_the_install_counts_adds_up_beside_the_workers(pool: Pool) {
    setup(&pool).await;
    let now = chrono::Utc::now().timestamp();
    tick_counting(&pool, "worker-a", now + 15, &[("dropped:t1", 2, Some("the trigger is gone"))], false).await;
    weft_task_store::worker_door::count_drop(&pool, PROJECT, "t1", "dispatcher-1", "no armed program", 0).await.unwrap();
    weft_task_store::worker_door::count_drop(&pool, PROJECT, "t1", "dispatcher-1", "still no armed program", 30).await.unwrap();
    let rows: Vec<(String, i64, Option<String>)> =
        pool.all("SELECT replica, hits, reason FROM door_count WHERE key = 'dropped:t1' ORDER BY replica", &[]).await.unwrap();
    assert_eq!(
        rows,
        [
            ("dispatcher-1".to_string(), 2, Some("still no armed program".to_string())),
            ("worker-a".to_string(), 2, Some("the trigger is gone".to_string())),
        ]
    );
}
