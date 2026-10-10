//! A local project's own port against a real database: a port the person
//! names is checked before anything is activated, and a project keeps the
//! port it was given, whoever picked it.
#![cfg(feature = "db-tests")]

use std::net::SocketAddr;

use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

use weft_core::ProjectDefinition;
use weft_dispatcher::project_ports::ProjectPorts;

async fn project(projects: &weft_dispatcher::ProjectStore) -> Uuid {
    let id = Uuid::new_v4();
    let definition: ProjectDefinition = serde_json::from_value(json!({ "id": id, "nodes": [], "edges": [] })).unwrap();
    projects
        .register_with_hashes(definition, "ports-rig", "", "tenant-1", Some("bin-A"), Some("def-1"), None, None, None, None, None)
        .await
        .expect("register project");
    id
}

/// A port the machine has free right now.
async fn free_port() -> u16 {
    tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap().local_addr().unwrap().port()
}

#[sqlx::test]
async fn a_named_port_is_refused_while_taken_and_kept_once_given(pool: PgPool) {
    weft_dispatcher::app::apply_core_schema(&pool).await.expect("core schema");
    let projects: weft_dispatcher::ProjectStore = std::sync::Arc::new(weft_dispatcher::PostgresProjectStore::new(pool.clone()));
    let (a, b) = (project(&projects).await, project(&projects).await);
    let ports = ProjectPorts::new(SocketAddr::from(([127, 0, 0, 1], 0)), None);

    // A free port is the person's to name, and the project keeps it.
    let port = free_port().await;
    assert_eq!(ports.check_asked(&pool, a, port).await.unwrap(), None);
    assert_eq!(ports.port_of(&pool, a, Some(port)).await.unwrap(), port);
    assert_eq!(ports.port_of(&pool, a, None).await.unwrap(), port, "a later activate keeps it");
    assert_eq!(ports.check_asked(&pool, a, port).await.unwrap(), None, "a project's own port is its own to name again");

    // Another project's port refuses, naming it.
    let refused = ports.check_asked(&pool, b, port).await.unwrap().expect("another project's port");
    assert!(refused.contains(&a.to_string()), "{refused}");

    // A port another program holds refuses.
    let held = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let held_port = held.local_addr().unwrap().port();
    let refused = ports.check_asked(&pool, b, held_port).await.unwrap().expect("a program holds it");
    assert!(refused.contains("another program"), "{refused}");

    // A project's own kept port, its front down, that another program
    // took meanwhile: refused like any taken port.
    let kept = held_port;
    sqlx::query("UPDATE project SET api_port = $2 WHERE id = $1").bind(a).bind(i32::from(kept)).execute(&pool).await.unwrap();
    let refused = ports.check_asked(&pool, a, kept).await.unwrap().expect("its front is down and a program holds it");
    assert!(refused.contains("another program"), "{refused}");
}
