//! An endpoint inside another infra node's endpoint, read through at
//! every lookup (`PostgresInfraReader::reach`): its row keeps the other
//! node's handle and the path, and a lookup reads where that node answers
//! now, so a node made again on another port moves it along.

use weft_task_store::db::{Client as _, Pool};
use weft_task_store::{PostgresInfraReader, Reached};

const PROJECT: uuid::Uuid = uuid::Uuid::from_u128(1);

async fn schema(pool: &Pool) {
    weft_task_store::apply_groups(pool, weft_dispatcher::app::DISPATCHER_GROUPS).await.expect("core schema");
}

/// The copy of `node` is `status`, its endpoints `endpoints` (name to
/// what `endpoints_json` keeps).
async fn row(pool: &Pool, node: &str, status: &str, endpoints: serde_json::Value) {
    pool.execute(
        "INSERT INTO infra_node (project_id, node_id, copy_id, status, endpoints_json, health_rules_json_version) \
         VALUES ($1, $2, $2, $3, $4, 1) \
         ON CONFLICT (project_id, node_id, instance_id) DO UPDATE SET status = EXCLUDED.status, endpoints_json = EXCLUDED.endpoints_json",
        &[&PROJECT, &node, &status, &endpoints.to_string()],
    )
    .await
    .expect("infra_node row");
}

fn within(place: &str) -> serde_json::Value {
    let handle = weft_core::infra::InfraHandle::new(place, "api", None);
    serde_json::json!({ "api": { "within": handle, "path": "/apps/claude" } })
}

/// A lookup reads where the endpoint the agent lives inside answers at
/// that moment, the other node's new port included; a flaky node still
/// answers, on either side; one inside a node that is stopped says so,
/// naming it and how to start it; one inside a node that declares no
/// such endpoint says that instead; one the caller may not reach is
/// refused with the caller's words; an endpoint inside one that is
/// itself inside another is refused.
#[weft_db_macros::test]
async fn an_endpoint_inside_another_is_read_through_at_every_lookup(pool: Pool) {
    schema(&pool).await;
    let reader = PostgresInfraReader::new(pool.clone());
    row(&pool, "box", "running", serde_json::json!({ "api": "http://127.0.0.1:5000" })).await;
    row(&pool, "agent", "running", within("box")).await;
    let reach = || reader.reach(PROJECT, "agent", None, "api", |_| Ok(()));
    assert_eq!(reach().await.unwrap(), Some(Reached::At { url: "http://127.0.0.1:5000/apps/claude".into(), public_path: None }));

    row(&pool, "box", "running", serde_json::json!({ "api": "http://127.0.0.1:6001/" })).await;
    assert_eq!(
        reach().await.unwrap(),
        Some(Reached::At { url: "http://127.0.0.1:6001/apps/claude".into(), public_path: None }),
        "the other node's new address, never one kept"
    );

    row(&pool, "box", "flaky", serde_json::json!({ "api": "http://127.0.0.1:6001" })).await;
    row(&pool, "agent", "flaky", within("box")).await;
    assert_eq!(
        reach().await.unwrap(),
        Some(Reached::At { url: "http://127.0.0.1:6001/apps/claude".into(), public_path: None }),
        "a failed health check is never read as a node that is down"
    );

    row(&pool, "box", "running", serde_json::json!({ "web": "http://127.0.0.1:6001" })).await;
    assert_eq!(
        reach().await.unwrap(),
        Some(Reached::Unreachable("this endpoint runs inside endpoint 'api' of 'box', which that node does not declare".into()))
    );

    row(&pool, "box", "stopped", serde_json::json!({ "api": "http://127.0.0.1:6001" })).await;
    assert_eq!(
        reach().await.unwrap(),
        Some(Reached::Unreachable("the infra node 'box' this endpoint runs inside is not running: start it with `weft infra start`".into()))
    );

    let refused = reader.reach(PROJECT, "agent", None, "api", |inner| Err(format!("{inner} is not yours"))).await.unwrap();
    assert_eq!(refused, Some(Reached::Unreachable("endpoint 'api' of 'box' is not yours".into())));

    row(&pool, "box", "running", within("deeper")).await;
    let Some(Reached::Unreachable(why)) = reach().await.unwrap() else { panic!("a chain is refused") };
    assert!(why.contains("followed one step only"), "{why}");

    assert_eq!(reader.reach(PROJECT, "agent", None, "other", |_| Ok(())).await.unwrap(), None);
}
