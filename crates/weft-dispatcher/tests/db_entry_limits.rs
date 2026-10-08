//! The install's own flood guard against a real database: every
//! dispatcher replica counts in the same rows, so an address guessing
//! tokens is blocked however many of them answer. A trigger's limits are
//! counted at its worker's door; their refusals are read here for
//! `weft status`.
//!
//! Same rig as `db_versions.rs`: `#[sqlx::test]` hands each test a fresh
//! database; the dispatcher's whole schema is applied as a boot does.
#![cfg(feature = "db-tests")]

use sqlx::PgPool;
use weft_dispatcher::entry_limits::{self, EdgeConfig, Limited};

async fn setup(pool: &PgPool) {
    weft_dispatcher::app::apply_core_schema(pool).await.expect("core schema");
}

fn edge(invalid_tokens_per_minute: Option<u32>) -> EdgeConfig {
    EdgeConfig {
        trusted_proxy_hops: weft_platform_traits::config::ProxyHops { public: 1, outside: 1, domains: 2 },
        invalid_tokens_per_minute,
    }
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
        entry_limits::note_invalid_token(&pool, &edge, addr, 120).await.unwrap();
    }
    let blocked = entry_limits::token_guessing_blocked(&pool, &edge, addr, 150).await.unwrap().expect("blocked");
    assert_eq!(blocked.retry_after_secs, 30);
    assert_eq!(blocked.reason, Limited::InvalidTokens);
    assert!(entry_limits::token_guessing_blocked(&pool, &edge, addr, 180).await.unwrap().is_none());
    assert!(entry_limits::token_guessing_blocked(&pool, &self::edge(None), addr, 150).await.unwrap().is_none());
}

/// The refusals the project's workers' doors counted are reported per
/// entry and limit for two minutes, summed over the copies, and a worker
/// of another project naming the entry's token counts for nothing.
#[sqlx::test]
async fn refusals_are_reported_for_two_minutes(pool: PgPool) {
    setup(&pool).await;
    let (project, other) = (uuid::Uuid::from_u128(1), uuid::Uuid::from_u128(2));
    let key = Limited::AtOnce.refusal_key("tok");
    for (window, replica, owner, hits) in [(60, "w1", project, 1), (120, "w1", project, 1), (120, "w2", project, 2), (120, "x", other, 9)] {
        sqlx::query("INSERT INTO door_count (key, window_start, replica, project_id, hits) VALUES ($1, $2, $3, $4, $5)")
            .bind(&key)
            .bind(window)
            .bind(replica)
            .bind(owner)
            .bind(hits as i64)
            .execute(&pool)
            .await
            .unwrap();
    }
    let recent = entry_limits::recent_refusals(&pool, project, "tok", 130).await.unwrap();
    assert_eq!(recent, vec![(Limited::AtOnce, 4)]);
    assert!(entry_limits::recent_refusals(&pool, project, "tok", 300).await.unwrap().is_empty());
}

/// The runs a trigger started, and how many failed, are reported for two
/// minutes per trigger, summed over the copies, for the project alone.
#[sqlx::test]
async fn the_runs_a_trigger_started_are_reported_for_two_minutes(pool: PgPool) {
    setup(&pool).await;
    let (project, other) = (uuid::Uuid::from_u128(1), uuid::Uuid::from_u128(2));
    let rows = [
        (weft_core::signal::limits::ran_key("tick"), 60, "w1", project, 3),
        (weft_core::signal::limits::ran_key("tick"), 120, "w2", project, 4),
        (weft_core::signal::limits::failed_key("tick"), 120, "w2", project, 1),
        (weft_core::signal::limits::ran_key("tick"), 120, "x", other, 9),
        (weft_core::signal::limits::ran_key("old"), 0, "w1", project, 5),
    ];
    for (key, window, replica, owner, hits) in rows {
        sqlx::query("INSERT INTO door_count (key, window_start, replica, project_id, hits) VALUES ($1, $2, $3, $4, $5)")
            .bind(&key)
            .bind(window)
            .bind(replica)
            .bind(owner)
            .bind(hits as i64)
            .execute(&pool)
            .await
            .unwrap();
    }
    let runs = entry_limits::recent_runs(&pool, project, 130).await.unwrap();
    assert_eq!(runs.len(), 1, "{runs:?}");
    assert_eq!(runs["tick"], (7, 1));
}
