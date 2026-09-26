//! Layer-3 contract test for `signal_placement::signals_held_by_pod`,
//! the query a listener pod rehydrates from, against a REAL Postgres:
//! the rule "a parked trigger's rows stay placed but never come back
//! into a registry" lives in the SQL's status filter, so only the
//! statement itself can prove it.
//!
//! Gated behind `db-tests` (off by default) so a plain `cargo test` needs no PG.
#![cfg(feature = "db-tests")]

use sqlx::PgPool;

use weft_broker::signal_placement::signals_held_by_pod;
use weft_broker_client::protocol::ProjectStatus;

const POD: &str = "listener-a";

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
async fn signal(pool: &PgPool, token: &str, project_id: uuid::Uuid, pod: Option<&str>, status: ProjectStatus) {
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
        "INSERT INTO signal (token, tenant_id, project_id, node_id, is_resume, spec_json, created_at, listener_pod, activation_trigger) \
         VALUES ($1, 'local', $2, $1, FALSE, '{}', 0, $3, $1)",
    )
    .bind(token)
    .bind(project_id)
    .bind(pod)
    .execute(pool)
    .await
    .expect("signal row");
}

const ACTIVE: uuid::Uuid = uuid::Uuid::from_u128(0xa);
const ACTIVATING: uuid::Uuid = uuid::Uuid::from_u128(0xb);
const PARKED: uuid::Uuid = uuid::Uuid::from_u128(0xc);
const DEACTIVATING: uuid::Uuid = uuid::Uuid::from_u128(0xd);

#[sqlx::test]
async fn a_pod_rehydrates_live_projects_and_never_a_parked_one(pool: PgPool) {
    schema(&pool).await;
    for id in [ACTIVE, ACTIVATING, PARKED, DEACTIVATING] {
        project(&pool, id).await;
    }
    signal(&pool, "active-here", ACTIVE, Some(POD), ProjectStatus::Active).await;
    signal(&pool, "active-elsewhere", ACTIVE, Some("listener-b"), ProjectStatus::Active).await;
    signal(&pool, "active-unplaced", ACTIVE, None, ProjectStatus::Active).await;
    signal(&pool, "activating-here", ACTIVATING, Some(POD), ProjectStatus::Activating).await;
    signal(&pool, "parked-here", PARKED, Some(POD), ProjectStatus::Inactive).await;
    signal(&pool, "deactivating-here", DEACTIVATING, Some(POD), ProjectStatus::Deactivating).await;

    let mut tokens: Vec<String> = signals_held_by_pod(&pool, POD)
        .await
        .expect("query")
        .into_iter()
        .map(|r| r.token)
        .collect();
    tokens.sort();
    assert_eq!(tokens, vec!["activating-here".to_string(), "active-here".to_string()]);
}
