//! A database the release before `infra_unit` left, booted by this one: each
//! infra copy's units, which that weft kept inside the copy's row in
//! `infra_node.units_json` (a column with no version beside it), become rows
//! of `infra_unit` (`weft_dispatcher::infra_node`'s move), and the column is
//! dropped. A second boot finds nothing to move.

use serde_json::{json, Value};
use weft_task_store::db::{Client as _, Pool};

use crate::stored_documents::OLDER_WEFT_HAD;

/// A copy's units as that weft wrote them: two real units, and a
/// status-only stub an older broker could leave.
fn older_units() -> Value {
    json!({
        "postgres": {
            "status": "running",
            "stop_behavior": { "kind": "stop" },
            "flaky_after_seconds": 120,
            "recovery_after_seconds": 60,
            "image_refs": ["postgres:16"]
        },
        "backup": {
            "status": "flaky",
            "stop_behavior": { "kind": "keep_running" },
            "flaky_after_seconds": 120,
            "recovery_after_seconds": 60,
            "ready_since_unix": 1760000100
        },
        "stub": { "status": "stopped" }
    })
}

async fn unit_rows(pool: &Pool, project_id: uuid::Uuid) -> Vec<(String, String, String, Vec<String>)> {
    pool.all("SELECT unit, status, stop_behavior, image_refs FROM infra_unit WHERE project_id = $1 ORDER BY unit", &[&project_id])
        .await
        .unwrap()
}

#[weft_db_macros::test]
async fn an_older_copys_units_become_rows_and_their_column_goes(pool: Pool) {
    weft_task_store::schema_guard::build_as_of(&pool, &weft_runtime::schema::groups(), |m| m.id <= OLDER_WEFT_HAD)
        .await
        .unwrap();
    let project_id = uuid::Uuid::new_v4();
    pool.execute(
        "INSERT INTO infra_node (project_id, node_id, copy_id, status, units_json) VALUES ($1, 'db', 'wn-db', 'running', $2::text::jsonb)",
        &[&project_id, &older_units().to_string()],
    )
    .await
    .unwrap();

    weft_runtime::schema::apply(&pool).await.unwrap();

    // Each real unit is a row of its own; the stub is left out.
    let moved = vec![
        ("backup".to_string(), "flaky".to_string(), "keep_running".to_string(), Vec::new()),
        ("postgres".to_string(), "running".to_string(), "stop".to_string(), vec!["postgres:16".to_string()]),
    ];
    assert_eq!(unit_rows(&pool, project_id).await, moved);
    let weft_task_store::db::Json(read): weft_task_store::db::Json<Value> = pool
        .scalar(
            &format!("SELECT {} FROM infra_node n WHERE n.project_id = $1", weft_broker_client::protocol::units_of_copy_sql("n")),
            &[&project_id],
        )
        .await
        .unwrap();
    let units = weft_broker_client::protocol::decode_units(read, project_id, "db").expect("the units read");
    let mut want = older_units();
    want.as_object_mut().unwrap().remove("stub");
    assert_eq!(units, weft_broker_client::protocol::decode_units(want, project_id, "db").unwrap());

    let column: Option<String> = pool
        .scalar_opt("SELECT column_name::text FROM information_schema.columns WHERE table_name = 'infra_node' AND column_name = 'units_json'", &[])
        .await
        .unwrap();
    assert_eq!(column, None, "the contract part dropped the column");

    // A second boot has nothing to move, and leaves the units as they are.
    weft_runtime::schema::apply(&pool).await.expect("a boot with nothing to do");
    assert_eq!(unit_rows(&pool, project_id).await, moved);
}
