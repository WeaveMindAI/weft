//! Layer-3 contract tests for `held_signals`, the statements the listener
//! depends on, against a REAL Postgres: the rule "a parked trigger's rows
//! never come back into a listener" lives in the SQL's status filter, and
//! "of two listeners woken for the same moment exactly one acts" lives in
//! the write's WHERE, so only the statements themselves can prove them.
//!
//! Gated behind `db-tests` (off by default) so a plain `cargo test` needs no PG.
#![cfg(feature = "db-tests")]

use sqlx::PgPool;

use weft_broker::held_signals::{signal_held, signals_held, write_kind_state};
use weft_broker_client::protocol::ProjectStatus;

async fn schema(pool: &PgPool) {
    weft_dispatcher::app::apply_core_schema(pool).await.expect("core schema");
}

async fn project(pool: &PgPool, id: uuid::Uuid) {
    sqlx::query(
        "INSERT INTO project (id, name, tenant_id, status, project_json, updated_at) \
         VALUES ($1, $2, 'local', 'registered', '{}', 0)",
    )
    .bind(id)
    .bind(id.to_string())
    .execute(pool)
    .await
    .expect("project row");
}

/// One entry signal governed by its own trigger's activation, in
/// `status`; the node id is the token, since a project holds one entry
/// row per node.
async fn signal(pool: &PgPool, token: &str, project_id: uuid::Uuid, status: ProjectStatus) {
    sqlx::query(
        "INSERT INTO trigger_activation \
           (project_id, trigger, status, accepting_fires, fires_visible_to_consumers, updated_at) \
         VALUES ($1, $2, $3, TRUE, TRUE, 0)",
    )
    .bind(project_id)
    .bind(token)
    .bind(status.as_str())
    .execute(pool)
    .await
    .expect("activation row");
    sqlx::query(
        "INSERT INTO signal (token, tenant_id, project_id, node_id, is_resume, spec_json, created_at, activation_trigger) \
         VALUES ($1, 'local', $2, $1, FALSE, '{}', 0, $1)",
    )
    .bind(token)
    .bind(project_id)
    .execute(pool)
    .await
    .expect("signal row");
}

const ACTIVE: uuid::Uuid = uuid::Uuid::from_u128(0xa);
const ACTIVATING: uuid::Uuid = uuid::Uuid::from_u128(0xb);
const PARKED: uuid::Uuid = uuid::Uuid::from_u128(0xc);
const DEACTIVATING: uuid::Uuid = uuid::Uuid::from_u128(0xd);

#[sqlx::test]
async fn the_listener_holds_live_projects_and_never_a_parked_one(pool: PgPool) {
    schema(&pool).await;
    for id in [ACTIVE, ACTIVATING, PARKED, DEACTIVATING] {
        project(&pool, id).await;
    }
    signal(&pool, "active", ACTIVE, ProjectStatus::Active).await;
    signal(&pool, "activating", ACTIVATING, ProjectStatus::Activating).await;
    signal(&pool, "parked", PARKED, ProjectStatus::Inactive).await;
    signal(&pool, "deactivating", DEACTIVATING, ProjectStatus::Deactivating).await;

    let mut tokens: Vec<String> = signals_held(&pool, None).await.expect("query").into_iter().map(|r| r.token).collect();
    tokens.sort();
    assert_eq!(tokens, vec!["activating".to_string(), "active".to_string()]);

    // An activation's rehydrate reads its own project's rows only.
    let one: Vec<String> =
        signals_held(&pool, Some(ACTIVATING)).await.expect("query").into_iter().map(|r| r.token).collect();
    assert_eq!(one, vec!["activating".to_string()]);
    assert!(signals_held(&pool, Some(PARKED)).await.expect("query").is_empty(), "scoped, still only held rows");

    // One by token follows the same rule.
    assert_eq!(signal_held(&pool, "active").await.unwrap().map(|r| r.token).as_deref(), Some("active"));
    assert!(signal_held(&pool, "parked").await.unwrap().is_none(), "a parked row never comes back");
    assert!(signal_held(&pool, "nothing").await.unwrap().is_none());
}

#[sqlx::test]
async fn a_kind_state_claim_has_one_winner(pool: PgPool) {
    schema(&pool).await;
    project(&pool, ACTIVE).await;
    signal(&pool, "tick", ACTIVE, ProjectStatus::Active).await;
    let state = |n: i64| serde_json::json!({ "n": n });
    let at = signal_held(&pool, "tick").await.unwrap().unwrap().kind_state_seq;

    // Two listeners woken for the same moment both read `at` and claim
    // from it: exactly one wins, and the row moves one past.
    let first = write_kind_state(&pool, "tick", &state(1), at).await.unwrap();
    let second = write_kind_state(&pool, "tick", &state(1), at).await.unwrap();
    assert!(first && !second);
    // A claim from a stale read loses.
    assert!(!write_kind_state(&pool, "tick", &state(9), at).await.unwrap());

    let row = signal_held(&pool, "tick").await.unwrap().unwrap();
    assert_eq!((row.kind_state, row.kind_state_seq), (state(1), at + 1));
}
