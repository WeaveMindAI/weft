//! The public edge's counters against a real database: every dispatcher
//! replica counts in the same rows, so a limit holds however many of them
//! answer at once.
//!
//! Same rig as `db_versions.rs`: `#[sqlx::test]` hands each test a fresh
//! database; the dispatcher's whole schema is applied as a boot does.
#![cfg(feature = "db-tests")]

use sqlx::PgPool;
use weft_core::signal::{EntryLimits, ResolvedLimits};
use weft_dispatcher::entry_limits::{self, Limited};

async fn setup(pool: &PgPool) {
    weft_dispatcher::app::apply_core_schema(pool).await.expect("core schema");
}

fn limits(per_caller: Option<u32>, per_entry: Option<u32>, at_once: Option<u32>) -> ResolvedLimits {
    EntryLimits { per_caller_per_minute: per_caller.or(Some(0)), per_minute: per_entry.or(Some(0)), at_once: at_once.or(Some(0)) }
        .resolve()
}

/// Many callers hitting one entry at once, as two replicas would (two
/// separate pools on the same database): exactly the limit gets through.
#[sqlx::test]
async fn concurrent_callers_across_replicas_share_one_count(pool: PgPool) {
    setup(&pool).await;
    let other_replica = PgPool::connect_with((*pool.connect_options()).clone()).await.expect("second pool");
    let l = limits(None, Some(25), None);
    let now = 1_000_040;
    let mut calls = Vec::new();
    for i in 0..80 {
        let p = if i % 2 == 0 { pool.clone() } else { other_replica.clone() };
        calls.push(tokio::spawn(async move {
            entry_limits::admit_call(&p, "tok", &format!("ip:10.0.0.{i}"), &l, now).await.expect("count")
        }));
    }
    let mut admitted = 0;
    for call in calls {
        match call.await.unwrap() {
            Ok(()) => admitted += 1,
            Err(refused) => {
                assert_eq!(refused.reason, Limited::PerEntry);
                assert_eq!(refused.retry_after_secs, 40, "the rest of the minute");
            }
        }
    }
    assert_eq!(admitted, 25);
}

/// One caller at its limit is refused; another caller, and the next
/// minute, are not.
#[sqlx::test]
async fn one_caller_is_limited_alone_and_only_for_the_minute(pool: PgPool) {
    setup(&pool).await;
    let l = limits(Some(2), None, None);
    let now = 600;
    for _ in 0..2 {
        entry_limits::admit_call(&pool, "tok", "ip:a", &l, now).await.unwrap().unwrap();
    }
    let refused = entry_limits::admit_call(&pool, "tok", "ip:a", &l, now).await.unwrap().unwrap_err();
    assert_eq!(refused.reason, Limited::PerCaller);
    entry_limits::admit_call(&pool, "tok", "ip:b", &l, now).await.unwrap().expect("another caller");
    entry_limits::admit_call(&pool, "tok", "ip:a", &l, now + 60).await.unwrap().expect("the next minute");
}

/// A fire the entry picked up itself counts once, however often its
/// task is retried: a retry reads back the first decision, admitted or
/// refused, and the entry's count does not move.
#[sqlx::test]
async fn a_retried_fire_is_counted_once(pool: PgPool) {
    setup(&pool).await;
    let l = limits(None, Some(2), None);
    let now = 600;
    for _ in 0..3 {
        entry_limits::admit_fire(&pool, "tok", Some("fire-1"), &l, now).await.unwrap().expect("admitted once");
    }
    entry_limits::admit_fire(&pool, "tok", Some("fire-2"), &l, now).await.unwrap().expect("the second fire fits");
    let refused = entry_limits::admit_fire(&pool, "tok", Some("fire-3"), &l, now).await.unwrap().unwrap_err();
    assert_eq!(refused.reason, Limited::PerEntry);
    // The retry of a refused fire stays refused, and of an admitted one
    // stays admitted, without spending the count.
    assert!(entry_limits::admit_fire(&pool, "tok", Some("fire-3"), &l, now).await.unwrap().is_err());
    entry_limits::admit_fire(&pool, "tok", Some("fire-1"), &l, now).await.unwrap().expect("still admitted");
    assert_eq!(entry_limits::recent_refusals(&pool, "tok", now).await.unwrap(), vec![(Limited::PerEntry, 1)]);
}

/// No limit means no limit, however many calls.
#[sqlx::test]
async fn an_entry_with_every_limit_off_admits_everything(pool: PgPool) {
    setup(&pool).await;
    let l = limits(None, None, None);
    assert_eq!(l, ResolvedLimits { per_caller_per_minute: None, per_minute: None, at_once: None });
    for _ in 0..200 {
        entry_limits::admit_call(&pool, "tok", "ip:a", &l, 60).await.unwrap().unwrap();
    }
}

/// The at-once slots: racing admissions take exactly the free ones, a
/// retry of the same run keeps its slot, an ended run frees one, and a
/// handshake nobody followed stops counting once it expires.
#[sqlx::test]
async fn at_once_slots_hold_under_contention_and_free_up(pool: PgPool) {
    setup(&pool).await;
    let now = 1_000;
    let mut takes = Vec::new();
    for i in 0..40 {
        let p = pool.clone();
        takes.push(tokio::spawn(async move {
            entry_limits::take_slot(&p, "tok", &format!("execution_id-{i}"), 10, now + 100, now).await.expect("take")
        }));
    }
    let mut taken = Vec::new();
    for (i, t) in takes.into_iter().enumerate() {
        if t.await.unwrap().is_ok() {
            taken.push(i);
        }
    }
    assert_eq!(taken.len(), 10);
    let first = format!("execution_id-{}", taken[0]);
    entry_limits::take_slot(&pool, "tok", &first, 10, now + 100, now)
        .await
        .unwrap()
        .expect("a retry of a run holding a slot keeps it");
    assert!(entry_limits::at_once_full(&pool, "tok", 10, now).await.unwrap().is_some());
    entry_limits::release_slot(&pool, &first).await.unwrap();
    assert!(entry_limits::at_once_full(&pool, "tok", 10, now).await.unwrap().is_none());
    // Every remaining slot is a run that never started: past its expiry
    // none counts, and the next take drops them.
    assert!(entry_limits::at_once_full(&pool, "tok", 10, now + 101).await.unwrap().is_none());
    entry_limits::take_slot(&pool, "tok", "late", 1, now + 300, now + 101).await.unwrap().expect("abandoned slots dropped");
}

/// A run that started keeps its slot past the unborn expiry until it ends.
#[sqlx::test]
async fn a_started_run_keeps_its_slot_until_it_ends(pool: PgPool) {
    setup(&pool).await;
    sqlx::query(
        "INSERT INTO execution (execution_id, project_id, tenant_id, started_at_unix, phase) \
         VALUES ('born', gen_random_uuid(), 't', 0, 'fire')",
    )
    .execute(&pool)
    .await
    .unwrap();
    entry_limits::take_slot(&pool, "tok", "born", 1, 10, 0).await.unwrap().unwrap();
    assert!(entry_limits::at_once_full(&pool, "tok", 1, 1_000).await.unwrap().is_some());
    entry_limits::sweep(&pool, 1_000).await.unwrap();
    assert!(entry_limits::take_slot(&pool, "tok", "other", 1, 2_000, 1_000).await.unwrap().is_err());
    entry_limits::release_slot(&pool, "born").await.unwrap();
    entry_limits::take_slot(&pool, "tok", "other", 1, 2_000, 1_000).await.unwrap().unwrap();
}

/// A run that ended frees its slot even when its cleanup never released
/// it: the sweep reads the end in the journal.
#[sqlx::test]
async fn the_sweep_frees_the_slot_of_a_run_that_ended(pool: PgPool) {
    setup(&pool).await;
    sqlx::query(
        "INSERT INTO execution (execution_id, project_id, tenant_id, started_at_unix, phase) \
         VALUES ('ended', gen_random_uuid(), 't', 0, 'fire')",
    )
    .execute(&pool)
    .await
    .unwrap();
    entry_limits::take_slot(&pool, "tok", "ended", 1, 10, 0).await.unwrap().unwrap();
    entry_limits::sweep(&pool, 1_000).await.unwrap();
    assert!(entry_limits::at_once_full(&pool, "tok", 1, 1_000).await.unwrap().is_some(), "a live run keeps it");
    sqlx::query(
        "INSERT INTO exec_event (execution_id, kind, payload_json, created_at) VALUES ('ended', 'execution_completed', '{}', 0)",
    )
    .execute(&pool)
    .await
    .unwrap();
    entry_limits::sweep(&pool, 1_000).await.unwrap();
    entry_limits::take_slot(&pool, "tok", "next", 1, 2_000, 1_000).await.unwrap().unwrap();
}

/// An address past the bound of refused tokens is blocked for the rest of
/// the minute; the bound off blocks nobody.
#[sqlx::test]
async fn token_guessing_blocks_the_address_for_the_minute(pool: PgPool) {
    setup(&pool).await;
    let edge = entry_limits::EdgeConfig { trusted_proxy_hops: weft_platform_traits::config::ProxyHops { public: 1, outside: 1 }, invalid_tokens_per_minute: Some(3) };
    let addr: std::net::IpAddr = "203.0.113.9".parse().unwrap();
    for _ in 0..3 {
        assert!(entry_limits::token_guessing_blocked(&pool, &edge, addr, 120).await.unwrap().is_none());
        entry_limits::note_invalid_token(&pool, &edge, addr, 120).await.unwrap();
    }
    let blocked = entry_limits::token_guessing_blocked(&pool, &edge, addr, 150).await.unwrap().expect("blocked");
    assert_eq!(blocked.retry_after_secs, 30);
    assert!(entry_limits::token_guessing_blocked(&pool, &edge, addr, 180).await.unwrap().is_none());
    let off = entry_limits::EdgeConfig { trusted_proxy_hops: weft_platform_traits::config::ProxyHops { public: 1, outside: 1 }, invalid_tokens_per_minute: None };
    assert!(entry_limits::token_guessing_blocked(&pool, &off, addr, 150).await.unwrap().is_none());
}

/// Refusals are counted per entry and limit for `weft status`.
#[sqlx::test]
async fn refusals_are_reported_for_two_minutes(pool: PgPool) {
    setup(&pool).await;
    entry_limits::note_refusal(&pool, "tok", Limited::AtOnce, 60).await.unwrap();
    entry_limits::note_refusal(&pool, "tok", Limited::AtOnce, 130).await.unwrap();
    let recent = entry_limits::recent_refusals(&pool, "tok", 130).await.unwrap();
    assert_eq!(recent, vec![(Limited::AtOnce, 2)]);
    assert!(entry_limits::recent_refusals(&pool, "tok", 300).await.unwrap().is_empty());
}

/// An unrecorded run whose costs kept its row is stamped ended rather
/// than given a terminal event: its slot stops counting all the same, so
/// a page polling an unrecorded route never fills the entry with runs
/// that already finished.
#[sqlx::test]
async fn an_ended_unrecorded_run_stops_counting(pool: PgPool) {
    setup(&pool).await;
    sqlx::query(
        "INSERT INTO execution (execution_id, project_id, tenant_id, started_at_unix, phase, kind, ended_at_unix) \
         VALUES ('quiet', gen_random_uuid(), 't', 0, 'fire', 'unrecorded', 5)",
    )
    .execute(&pool)
    .await
    .unwrap();
    entry_limits::take_slot(&pool, "tok", "quiet", 1, 10_000, 0).await.unwrap().unwrap();
    assert!(entry_limits::at_once_full(&pool, "tok", 1, 6).await.unwrap().is_none(), "it ended");
    entry_limits::take_slot(&pool, "tok", "next", 1, 10_000, 6).await.unwrap().expect("its slot is free");
}
