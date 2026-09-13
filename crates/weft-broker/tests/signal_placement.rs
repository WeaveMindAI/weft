//! Layer-3 contract test for `signal_placement::signals_held_by_pod`,
//! the query a listener pod rehydrates from, against a REAL Postgres:
//! the rule "a parked project's rows stay placed but never come back
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

async fn project(pool: &PgPool, id: &str, status: ProjectStatus) {
    sqlx::query(
        "INSERT INTO project (id, name, status, project_json, updated_at) \
         VALUES ($1::uuid, $1, $2, '{}', 0)",
    )
    .bind(id)
    .bind(status.as_str())
    .execute(pool)
    .await
    .expect("project row");
}

/// One entry signal; the node id is the token, since a project holds
/// one entry row per node.
async fn signal(pool: &PgPool, token: &str, project_id: &str, pod: Option<&str>) {
    sqlx::query(
        "INSERT INTO signal (token, tenant_id, project_id, node_id, is_resume, spec_json, created_at, listener_pod) \
         VALUES ($1, 'local', $2, $1, FALSE, '{}', 0, $3)",
    )
    .bind(token)
    .bind(project_id)
    .bind(pod)
    .execute(pool)
    .await
    .expect("signal row");
}

const ACTIVE: &str = "00000000-0000-0000-0000-00000000000a";
const ACTIVATING: &str = "00000000-0000-0000-0000-00000000000b";
const PARKED: &str = "00000000-0000-0000-0000-00000000000c";
const DEACTIVATING: &str = "00000000-0000-0000-0000-00000000000d";

#[sqlx::test]
async fn a_pod_rehydrates_live_projects_and_never_a_parked_one(pool: PgPool) {
    schema(&pool).await;
    project(&pool, ACTIVE, ProjectStatus::Active).await;
    project(&pool, ACTIVATING, ProjectStatus::Activating).await;
    project(&pool, PARKED, ProjectStatus::Inactive).await;
    project(&pool, DEACTIVATING, ProjectStatus::Deactivating).await;
    signal(&pool, "active-here", ACTIVE, Some(POD)).await;
    signal(&pool, "active-elsewhere", ACTIVE, Some("listener-b")).await;
    signal(&pool, "active-unplaced", ACTIVE, None).await;
    signal(&pool, "activating-here", ACTIVATING, Some(POD)).await;
    signal(&pool, "parked-here", PARKED, Some(POD)).await;
    signal(&pool, "deactivating-here", DEACTIVATING, Some(POD)).await;

    let mut tokens: Vec<String> = signals_held_by_pod(&pool, POD)
        .await
        .expect("query")
        .into_iter()
        .map(|r| r.token)
        .collect();
    tokens.sort();
    assert_eq!(tokens, vec!["activating-here".to_string(), "active-here".to_string()]);
}
