//! The install's own flood guard against a real database: every
//! dispatcher replica counts in the same rows, so an address guessing
//! tokens is blocked however many of them answer. A trigger's limits are
//! counted at its worker's door; their refusals are read here for
//! `weft status`.
//!
//! Same rig as `db_versions.rs`: `#[weft_db_macros::test]` hands each test a fresh
//! database; the dispatcher's whole schema is applied as a boot does.

use weft_dispatcher::entry_limits::{self, EdgeConfig, Limited};
use weft_task_store::db::{Client as _, Pool};

async fn setup(pool: &Pool) {
    weft_task_store::apply_groups(pool, weft_dispatcher::app::DISPATCHER_GROUPS).await.expect("core schema");
}

fn edge(invalid_tokens_per_minute: Option<u32>) -> EdgeConfig {
    EdgeConfig {
        trusted_proxy_hops: weft_platform_traits::config::ProxyHops { public: 1, outside: 1, domains: 2 },
        invalid_tokens_per_minute,
    }
}

/// An address past the bound of refused tokens is blocked for the rest of
/// the minute; the bound off blocks nobody.
#[weft_db_macros::test]
async fn token_guessing_blocks_the_address_for_the_minute(pool: Pool) {
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
#[weft_db_macros::test]
async fn refusals_are_reported_for_two_minutes(pool: Pool) {
    setup(&pool).await;
    let (project, other) = (uuid::Uuid::from_u128(1), uuid::Uuid::from_u128(2));
    let key = Limited::AtOnce.refusal_key("tok");
    for (window, replica, owner, hits) in [(60, "w1", project, 1), (120, "w1", project, 1), (120, "w2", project, 2), (120, "x", other, 9)] {
        pool.execute(
            "INSERT INTO door_count (key, window_start, replica, project_id, hits) VALUES ($1, $2, $3, $4, $5)",
            &[&key, &(window as i64),&replica, &owner, &(hits as i64)],
        )
        .await
        .unwrap();
    }
    let recent = entry_limits::recent_refusals(&pool, project, "tok", 130).await.unwrap();
    assert_eq!(recent, vec![(Limited::AtOnce, 4)]);
    assert!(entry_limits::recent_refusals(&pool, project, "tok", 300).await.unwrap().is_empty());
}

/// The runs a trigger started, and how many failed, are reported for two
/// minutes per trigger, summed over the copies, for the project alone.
#[weft_db_macros::test]
async fn the_runs_a_trigger_started_are_reported_for_two_minutes(pool: Pool) {
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
        pool.execute(
            "INSERT INTO door_count (key, window_start, replica, project_id, hits) VALUES ($1, $2, $3, $4, $5)",
            &[&key, &(window as i64),&replica, &owner, &(hits as i64)],
        )
        .await
        .unwrap();
    }
    let runs = entry_limits::recent_runs(&pool, project, 130).await.unwrap();
    assert_eq!(runs.len(), 1, "{runs:?}");
    assert_eq!(runs["tick"], (7, 1));
}

/// The fires a trigger's doors dropped are reported for two minutes per
/// activation, summed over the copies, with the newest minute's reason,
/// for the project alone; a token no activation governs counts for nothing.
#[weft_db_macros::test]
async fn the_fires_a_trigger_dropped_are_reported_with_the_last_reason(pool: Pool) {
    use weft_core::activation::ActivationKey;
    use weft_core::instance::{InstanceId, Owner};
    use weft_core::signal::limits::dropped_key;
    setup(&pool).await;
    let (project, other) = (uuid::Uuid::from_u128(1), uuid::Uuid::from_u128(2));
    for (token, trigger, instance) in [("tick", Some("tick"), None), ("ada-inbox", Some("inbox"), Some("ada")), ("loose", None, None)] {
        pool.execute(
            "INSERT INTO signal (token, tenant_id, project_id, node_id, is_resume, spec_json, created_at, \
             spec_json_version, program_json_version, activation_trigger, instance_id) \
             VALUES ($1, 'tenant-1', $2, $1, FALSE, '{}', 1, $3, 1, $4, $5)",
            &[&token, &project, &weft_core::primitive::SignalSpec::STORED.version(), &trigger, &instance],
        )
        .await
        .unwrap();
    }
    let rows = [
        (dropped_key("tick"), 60, "w1", project, 2, "the trigger is gone"),
        (dropped_key("tick"), 120, "w2", project, 1, "package 'relay' didn't compile"),
        (dropped_key("tick"), 120, "x", other, 9, "another project's"),
        (dropped_key("ada-inbox"), 120, "w1", project, 4, "its trigger takes no work (inactive)"),
        (dropped_key("loose"), 120, "w1", project, 5, "no activation"),
        (dropped_key("tick"), 0, "w1", project, 7, "too old"),
    ];
    for (key, window, replica, owner, hits, reason) in rows {
        pool.execute(
            "INSERT INTO door_count (key, window_start, replica, project_id, hits, reason) VALUES ($1, $2, $3, $4, $5, $6)",
            &[&key, &(window as i64), &replica, &owner, &(hits as i64), &reason],
        )
        .await
        .unwrap();
    }
    let dropped = entry_limits::dropped(&pool, project, 130).await.unwrap();
    let shared = ActivationKey::new("tick", Owner::Shared);
    let ada = ActivationKey::new("inbox", Owner::Instance(InstanceId::new("ada").unwrap()));
    assert_eq!(dropped.len(), 2, "{dropped:?}");
    assert_eq!((dropped[&shared].count, dropped[&shared].last_reason.as_str()), (3, "package 'relay' didn't compile"));
    assert_eq!((dropped[&ada].count, dropped[&ada].last_reason.as_str()), (4, "its trigger takes no work (inactive)"));
    assert!(entry_limits::dropped(&pool, project, 300).await.unwrap().is_empty());
}

/// The minutes that no longer count go with the next refused token, so the
/// counts are kept small by the writes that fill them and no timer looks.
#[weft_db_macros::test]
async fn a_refused_token_prunes_the_minutes_that_no_longer_count(pool: Pool) {
    setup(&pool).await;
    let edge = edge(Some(3));
    let addr: std::net::IpAddr = "203.0.113.9".parse().unwrap();
    entry_limits::note_invalid_token(&pool, &edge, addr, 120).await.unwrap();
    entry_limits::note_invalid_token(&pool, &edge, addr, 180).await.unwrap();
    entry_limits::note_invalid_token(&pool, &edge, addr, 300).await.unwrap();
    let minutes: Vec<i64> = pool.scalars("SELECT window_start FROM entry_rate ORDER BY window_start", &[]).await.unwrap();
    assert_eq!(minutes, vec![300], "the minutes before the last two went");
    entry_limits::note_invalid_token(&pool, &edge, addr, 360).await.unwrap();
    let minutes: Vec<i64> = pool.scalars("SELECT window_start FROM entry_rate ORDER BY window_start", &[]).await.unwrap();
    assert_eq!(minutes, vec![300, 360], "the minute before stays");
}
