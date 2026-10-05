//! The public edge's counters against a real database: every dispatcher
//! replica counts in the same rows, so a limit holds however many of them
//! answer at once.
//!
//! Same rig as `db_versions.rs`: `#[sqlx::test]` hands each test a fresh
//! database; the dispatcher's whole schema is applied as a boot does.
#![cfg(feature = "db-tests")]

use sqlx::PgPool;
use weft_core::signal::{EntryLimits, ResolvedLimits};
use weft_dispatcher::entry_limits::{self, Admission, EdgeConfig, Limited, Refused};

async fn setup(pool: &PgPool) {
    weft_dispatcher::app::apply_core_schema(pool).await.expect("core schema");
}

fn edge(invalid_tokens_per_minute: Option<u32>) -> EdgeConfig {
    EdgeConfig {
        trusted_proxy_hops: weft_platform_traits::config::ProxyHops { public: 1, outside: 1, domains: 2 },
        invalid_tokens_per_minute,
    }
}

/// One call by `caller` to the entry `tok`, admitted on its own.
async fn call(pool: &PgPool, caller: &str, l: &ResolvedLimits, now: i64) -> Result<(), Refused> {
    entry_limits::admit(pool, &Admission::call(&edge(None), None, "tok", caller, l, None, now)).await.expect("admit")
}

/// A slot of `tok` (at most `max` at once) taken for the run `execution_id`,
/// holding until `unborn_until` if it never starts.
async fn take(pool: &PgPool, execution_id: &str, max: u32, unborn_until: i64, now: i64) -> Result<(), Refused> {
    let l = limits(None, None, Some(max));
    entry_limits::admit(pool, &Admission::call(&edge(None), None, "tok", "ip:a", &l, Some((execution_id, unborn_until)), now))
        .await
        .expect("take")
}

/// Whether `tok` has no room for one more run, taking nothing.
async fn full(pool: &PgPool, max: u32, now: i64) -> bool {
    let l = limits(None, None, Some(max));
    entry_limits::admit(pool, &Admission::call(&edge(None), None, "tok", "ip:a", &l, None, now)).await.expect("check").is_err()
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
            call(&p, &format!("ip:10.0.0.{i}"), &l, now).await
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
        call(&pool, "ip:a", &l, now).await.unwrap();
    }
    let refused = call(&pool, "ip:a", &l, now).await.unwrap_err();
    assert_eq!(refused.reason, Limited::PerCaller);
    call(&pool, "ip:b", &l, now).await.expect("another caller");
    call(&pool, "ip:a", &l, now + 60).await.expect("the next minute");
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
        call(&pool, "ip:a", &l, 60).await.unwrap();
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
            take(&p, &format!("execution_id-{i}"), 10, now + 100, now).await
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
    take(&pool, &first, 10, now + 100, now).await.expect("a retry of a run holding a slot keeps it");
    assert!(full(&pool, 10, now).await);
    entry_limits::release_slot(&pool, &first).await.unwrap();
    assert!(!full(&pool, 10, now).await);
    // Every remaining slot is a run that never started: past its expiry
    // none counts, so the next take finds the entry free.
    assert!(!full(&pool, 10, now + 101).await);
    take(&pool, "late", 1, now + 300, now + 101).await.expect("abandoned slots stop counting");
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
    take(&pool, "born", 1, 10, 0).await.unwrap();
    assert!(full(&pool, 1, 1_000).await);
    entry_limits::sweep(&pool, 1_000).await.unwrap();
    assert!(take(&pool, "other", 1, 2_000, 1_000).await.is_err());
    entry_limits::release_slot(&pool, "born").await.unwrap();
    take(&pool, "other", 1, 2_000, 1_000).await.unwrap();
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
    take(&pool, "ended", 1, 10, 0).await.unwrap();
    entry_limits::sweep(&pool, 1_000).await.unwrap();
    assert!(full(&pool, 1, 1_000).await, "a live run keeps it");
    sqlx::query(
        "INSERT INTO exec_event (execution_id, kind, payload_json, created_at) VALUES ('ended', 'execution_completed', '{}', 0)",
    )
    .execute(&pool)
    .await
    .unwrap();
    entry_limits::sweep(&pool, 1_000).await.unwrap();
    take(&pool, "next", 1, 2_000, 1_000).await.unwrap();
}

/// An address past the bound of refused tokens is blocked for the rest of
/// the minute; the bound off blocks nobody.
#[sqlx::test]
async fn token_guessing_blocks_the_address_for_the_minute(pool: PgPool) {
    setup(&pool).await;
    let edge = edge(Some(3));
    let addr: std::net::IpAddr = "203.0.113.9".parse().unwrap();
    for _ in 0..3 {
        assert!(entry_limits::token_guessing_blocked(&pool, &edge, addr, 120).await.unwrap().is_none());
        let already = entry_limits::note_invalid_token(&pool, &edge, addr, 120).await.unwrap();
        assert!(already.is_none(), "an address under the bound is counted, not refused");
    }
    let blocked = entry_limits::token_guessing_blocked(&pool, &edge, addr, 150).await.unwrap().expect("blocked");
    assert_eq!(blocked.retry_after_secs, 30);
    // Counting one more says, in the same trip, that it was already blocked.
    let counted = entry_limits::note_invalid_token(&pool, &edge, addr, 150).await.unwrap().expect("already blocked");
    assert_eq!((counted.reason, counted.retry_after_secs), (Limited::InvalidTokens, 30));
    assert!(entry_limits::token_guessing_blocked(&pool, &edge, addr, 180).await.unwrap().is_none());
    assert!(entry_limits::token_guessing_blocked(&pool, &self::edge(None), addr, 150).await.unwrap().is_none());
}

/// A live call's admission checks the address the door's guard left to
/// it (`/connect/`), before it counts or takes anything: a blocked
/// address is refused and spends none of the entry's allowance.
#[sqlx::test]
async fn a_blocked_address_is_refused_by_the_calls_admission(pool: PgPool) {
    setup(&pool).await;
    let edge = edge(Some(1));
    let addr: std::net::IpAddr = "203.0.113.9".parse().unwrap();
    let l = limits(Some(5), None, Some(1));
    let admit = |execution_id: &'static str| {
        let (pool, edge, l) = (pool.clone(), edge.clone(), l.clone());
        async move {
            entry_limits::admit(&pool, &Admission::call(&edge, Some(addr), "tok", "ip:a", &l, Some((execution_id, 500)), 120))
                .await
                .expect("admit")
        }
    };
    entry_limits::note_invalid_token(&pool, &edge, addr, 120).await.unwrap();
    let refused = admit("first").await.unwrap_err();
    assert_eq!(refused.reason, Limited::InvalidTokens);
    assert!(!full(&pool, 1, 120).await, "a blocked call took no slot");
    let unblocked = Admission::call(&self::edge(None), Some(addr), "tok", "ip:a", &l, Some(("second", 500)), 120);
    entry_limits::admit(&pool, &unblocked).await.unwrap().expect("with the bound off the address is not checked");
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
    take(&pool, "quiet", 1, 10_000, 0).await.unwrap();
    assert!(!full(&pool, 1, 6).await, "it ended");
    take(&pool, "next", 1, 10_000, 6).await.expect("its slot is free");
}
