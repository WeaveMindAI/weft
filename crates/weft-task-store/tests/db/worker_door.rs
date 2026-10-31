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
    let (keys, hits): (Vec<String>, Vec<i64>) = counts.iter().map(|(k, h)| (k.to_string(), *h)).unzip();
    let none = std::collections::BTreeMap::<String, u32>::new();
    pool.scalar::<db::Json<serde_json::Value>>(
        "SELECT weft_door_tick($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::boolean)",
        &[&replica, &PROJECT, &"tenant-1", &"bin-A", &until, &db::Json(db::WeftMade(&none)), &0i64, &keys, &hits, &shortens],
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

/// The tick as the previous release's brokers call it (the tokens it
/// serves named, no lease shortened) still renews the lease and answers
/// the counts of exactly the tokens it names, a refused one included, as
/// that release did.
#[weft_db_macros::test]
async fn the_previous_releases_tick_still_answers_as_it_did(pool: Pool) {
    setup(&pool).await;
    let now = chrono::Utc::now().timestamp();
    tick_with(&pool, "worker-b", now + 15, &[("e:route:1", 3), ("e:other:1", 4), ("t:-:1.2.3.4", 9)], false).await;
    let none = std::collections::BTreeMap::<String, u32>::new();
    let db::Json(answer): db::Json<serde_json::Value> = pool
        .scalar(
            "SELECT weft_door_tick($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::text[])",
            &[
                &"worker-a",
                &PROJECT,
                &"tenant-1",
                &"bin-A",
                &(now + 15),
                &db::Json(db::WeftMade(&none)),
                &0i64,
                &Vec::<String>::new(),
                &Vec::<i64>::new(),
                &vec!["route".to_string(), "-".to_string()],
            ],
        )
        .await
        .unwrap();
    let mut others: Vec<(String, i64)> = serde_json::from_value::<Vec<serde_json::Value>>(answer["others"].clone())
        .unwrap()
        .into_iter()
        .map(|count| (count["key"].as_str().unwrap().to_string(), count["hits"].as_i64().unwrap()))
        .collect();
    others.sort();
    assert_eq!(others, [("e:route:1".to_string(), 3), ("t:-:1.2.3.4".to_string(), 9)], "only the tokens it names");
    assert_eq!(answer["copies"], serde_json::json!(2));
}
