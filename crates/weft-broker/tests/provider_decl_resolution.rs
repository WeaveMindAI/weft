//! Layer-3 tests for the provider-declaration resolution the proxy trusts,
//! against a REAL Postgres: the dispatcher-side write
//! (`weft_dispatcher::provider_decl::record`, at registration, from the
//! build's own walk of the node sources) and the broker-side read
//! (`weft_broker::provider_proxy::declared_base_url`, joining the calling
//! pod's dispatcher-stamped `worker_pod.binary_hash`). The trust property
//! under test lives IN the SQL join, so a faked DB would not catch it.
//!
//! Gated behind `db-tests` (off by default) so a plain `cargo test` needs no
//! PG; `#[sqlx::test]` provisions a fresh DB per test from `$DATABASE_URL`.
#![cfg(feature = "db-tests")]

use std::collections::BTreeMap;

use sqlx::PgPool;

use weft_broker::provider_proxy::declared_base_url;
use weft_core::node::ProviderDecl;

const BINARY: &str = "binhash-1";
const POD: &str = "pod-1";
const OPENROUTER_URL: &str = "https://openrouter.ai/api/v1";

async fn setup(pool: &PgPool) {
    weft_dispatcher::provider_decl::migrate(pool).await.expect("provider_declaration schema");
    // `worker_pod::migrate` creates fencing triggers on journal-owned tables
    // (created at journal connect in production, which these tests never
    // touch); the resolution join only needs the `worker_pod` TABLE, so stand
    // it up minimally.
    // SYNC: columns in step with crates/weft-task-store/src/worker_pod.rs `worker_pod`.
    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS worker_pod (
            pod_name TEXT PRIMARY KEY,
            project_id TEXT NOT NULL,
            namespace TEXT NOT NULL,
            status TEXT NOT NULL,
            owner_dispatcher TEXT NOT NULL,
            last_heartbeat_unix BIGINT NOT NULL,
            created_at_unix BIGINT NOT NULL,
            binary_hash TEXT NOT NULL DEFAULT ''
        )"#,
    )
    .execute(pool)
    .await
    .expect("worker_pod stub");
    sqlx::query(
        "INSERT INTO worker_pod (pod_name, project_id, namespace, status, owner_dispatcher,
                                 last_heartbeat_unix, created_at_unix, binary_hash)
         VALUES ($1, 'p1', 'ns', 'alive', 'd1', 0, 0, $2)",
    )
    .bind(POD)
    .bind(BINARY)
    .execute(pool)
    .await
    .expect("seed pod");
}

fn decls() -> BTreeMap<String, ProviderDecl> {
    [(
        "OpenRouterInference".to_string(),
        ProviderDecl { name: "openrouter".into(), base_url: OPENROUTER_URL.into() },
    )]
    .into()
}

/// The happy path: the calling pod's binary recorded a declaration for
/// exactly this (node type, provider), and the resolved URL is the one the
/// node's own source declared.
#[sqlx::test]
async fn a_declared_provider_resolves_to_the_declared_url(pool: PgPool) {
    setup(&pool).await;
    weft_dispatcher::provider_decl::record(&pool, BINARY, &decls()).await.unwrap();
    let url = declared_base_url(&pool, Some(POD), "OpenRouterInference", "openrouter")
        .await
        .unwrap();
    assert_eq!(url.as_deref(), Some(OPENROUTER_URL));
}

/// Everything off the declared coordinates resolves to nothing (a loud
/// refusal at the caller): a provider the node never declared, a node type
/// with no declaration, an unknown pod, and a caller with no pod identity at
/// all. None of these may ever fall back to a default host.
#[sqlx::test]
async fn everything_undeclared_resolves_to_nothing(pool: PgPool) {
    setup(&pool).await;
    weft_dispatcher::provider_decl::record(&pool, BINARY, &decls()).await.unwrap();

    let q = |pod: Option<&'static str>, nt: &'static str, prov: &'static str| {
        let pool = pool.clone();
        async move { declared_base_url(&pool, pod, nt, prov).await.unwrap() }
    };
    assert_eq!(q(Some(POD), "OpenRouterInference", "shape").await, None, "undeclared provider");
    assert_eq!(q(Some(POD), "SomeOtherNode", "openrouter").await, None, "undeclared node type");
    assert_eq!(q(Some("pod-unknown"), "OpenRouterInference", "openrouter").await, None, "unknown pod");
    assert_eq!(q(None, "OpenRouterInference", "openrouter").await, None, "pod-unbound caller");
}

/// Content-addressing: re-recording the same binary is a no-op, and a
/// DIFFERENT binary (say, one whose node re-aimed the URL) resolves through
/// ITS OWN rows only; the pod's stamped binary decides which apply.
#[sqlx::test]
async fn resolution_follows_the_pods_stamped_binary(pool: PgPool) {
    setup(&pool).await;
    weft_dispatcher::provider_decl::record(&pool, BINARY, &decls()).await.unwrap();
    // Re-record: no error, no change.
    weft_dispatcher::provider_decl::record(&pool, BINARY, &decls()).await.unwrap();

    // Another binary declares the same (node type, provider) with another
    // URL; a pod stamped with THAT binary resolves to it, while our pod's
    // resolution is untouched.
    let reaimed: BTreeMap<String, ProviderDecl> = [(
        "OpenRouterInference".to_string(),
        ProviderDecl { name: "openrouter".into(), base_url: "https://other.example".into() },
    )]
    .into();
    weft_dispatcher::provider_decl::record(&pool, "binhash-2", &reaimed).await.unwrap();
    sqlx::query(
        "INSERT INTO worker_pod (pod_name, project_id, namespace, status, owner_dispatcher,
                                 last_heartbeat_unix, created_at_unix, binary_hash)
         VALUES ('pod-2', 'p2', 'ns', 'alive', 'd1', 0, 0, 'binhash-2')",
    )
    .execute(&pool)
    .await
    .unwrap();

    let ours = declared_base_url(&pool, Some(POD), "OpenRouterInference", "openrouter")
        .await
        .unwrap();
    assert_eq!(ours.as_deref(), Some(OPENROUTER_URL));
    let theirs = declared_base_url(&pool, Some("pod-2"), "OpenRouterInference", "openrouter")
        .await
        .unwrap();
    assert_eq!(theirs.as_deref(), Some("https://other.example"));
}
