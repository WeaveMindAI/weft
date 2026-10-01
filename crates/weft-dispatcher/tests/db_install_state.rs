//! Layer-3 tests against a REAL Postgres for the install's own state: the
//! domains it answers at (`weft_dispatcher::domains`).
//!
//! Same rig as `db_lifecycle.rs`: `#[sqlx::test]` hands each test a fresh
//! database, the real boot-time migration path builds the schema, and the
//! tests are gated behind `db-tests` (`scripts/run-db-tests.sh weft-dispatcher`).
#![cfg(feature = "db-tests")]

use sqlx::PgPool;
use uuid::Uuid;
use weft_core::install::{Domain, DomainServes};
use weft_dispatcher::ProjectStoreOps as _;

async fn project(pool: &PgPool) -> Uuid {
    weft_dispatcher::app::apply_core_schema(pool).await.expect("core schema");
    let store = weft_dispatcher::PostgresProjectStore::new(pool.clone());
    let id = Uuid::new_v4();
    let definition = serde_json::from_value(serde_json::json!({ "id": id, "nodes": [], "edges": [] })).expect("a definition");
    store
        .register_with_hashes(definition, "p", "", "tenant-1", Some("bin"), Some("def"), None, None, None, None)
        .await
        .expect("register");
    id
}

#[sqlx::test]
async fn a_domain_is_stored_once_and_goes_with_its_project(pool: PgPool) {
    let id = project(&pool).await;
    let install = Domain { name: "weft.example.com".into(), serves: DomainServes::Install };
    let api = Domain { name: "api.example.com".into(), serves: DomainServes::Api { project: id } };
    assert!(!weft_dispatcher::domains::install_has_domain(&pool).await.unwrap());
    weft_dispatcher::domains::add(&pool, &install, 1).await.unwrap();
    weft_dispatcher::domains::add(&pool, &api, 1).await.unwrap();
    assert!(weft_dispatcher::domains::install_has_domain(&pool).await.unwrap());

    let again = Domain { name: "api.example.com".into(), serves: DomainServes::Install };
    let refused = weft_dispatcher::domains::add(&pool, &again, 2).await.unwrap_err().to_string();
    assert!(refused.contains("already"), "a stored name never quietly moves: {refused}");
    assert_eq!(weft_dispatcher::domains::list(&pool).await.unwrap(), vec![api.clone(), install.clone()]);

    sqlx::query("DELETE FROM project WHERE id = $1").bind(id).execute(&pool).await.unwrap();
    assert_eq!(weft_dispatcher::domains::list(&pool).await.unwrap(), vec![install], "a project's domain goes with it");
    assert!(weft_dispatcher::domains::remove(&pool, "weft.example.com").await.unwrap());
    assert!(!weft_dispatcher::domains::remove(&pool, "weft.example.com").await.unwrap());
}
