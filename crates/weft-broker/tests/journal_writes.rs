//! Layer-3 contract tests for a worker's journal write as the broker
//! makes it (`weft_journal::record_events`), against a REAL Postgres: the
//! rows of one write go in as one statement, in the order given, and only
//! while the writing replica owns the execution's claim. The fence lives
//! in the statement itself, so only a real database can prove it.
//!
//! Gated behind `db-tests` (off by default) so a plain `cargo test` needs no PG.
#![cfg(feature = "db-tests")]

use sqlx::PgPool;
use weft_journal::{ExecEvent, RecordError};

const PROJECT: uuid::Uuid = uuid::Uuid::from_u128(1);

async fn schema(pool: &PgPool) {
    weft_dispatcher::app::apply_core_schema(pool).await.expect("core schema");
}

/// An execution claimed by `owner`.
async fn claimed_by(pool: &PgPool, owner: &str) -> weft_core::ExecutionId {
    let execution_id = weft_core::ExecutionId::new_v4();
    sqlx::query(
        "INSERT INTO execution (execution_id, project_id, tenant_id, started_at_unix, phase, owner_replica) \
         VALUES ($1, $2, 't1', 0, 'fire', $3)",
    )
    .bind(execution_id.to_string())
    .bind(PROJECT)
    .bind(owner)
    .execute(pool)
    .await
    .expect("seed execution");
    execution_id
}

fn started(execution_id: weft_core::ExecutionId, node: &str) -> ExecEvent {
    ExecEvent::NodeStarted { execution_id, node_id: node.into(), frames: Vec::new(), at_unix: 0 }
}

async fn kinds_and_nodes(pool: &PgPool, execution_id: weft_core::ExecutionId) -> Vec<String> {
    let rows: Vec<(String,)> = sqlx::query_as("SELECT payload_json FROM exec_event WHERE execution_id = $1 ORDER BY id")
        .bind(execution_id.to_string())
        .fetch_all(pool)
        .await
        .expect("read rows");
    rows.into_iter()
        .map(|(payload,)| {
            let value: serde_json::Value = serde_json::from_str(&payload).expect("row is json");
            value["node_id"].as_str().expect("a node row").to_string()
        })
        .collect()
}

/// The owner's rows land, all of them, in the order it wrote them.
#[sqlx::test]
async fn an_owner_writes_its_rows_in_one_go_and_in_order(pool: PgPool) {
    schema(&pool).await;
    let execution_id = claimed_by(&pool, "worker-a").await;
    let events = ["first", "second", "third"].map(|node| started(execution_id, node));
    let written = weft_journal::record_events(&pool, &events, Some("worker-a"), Some("worker-a")).await.unwrap();
    assert_eq!(written, 3);
    assert_eq!(kinds_and_nodes(&pool, execution_id).await, ["first", "second", "third"]);
}

/// A replica that does not own the claim writes nothing, and says so by
/// writing no row; nor does one writing for a run nobody claimed.
#[sqlx::test]
async fn a_replica_that_lost_the_claim_writes_nothing(pool: PgPool) {
    schema(&pool).await;
    let execution_id = claimed_by(&pool, "worker-a").await;
    let events = [started(execution_id, "late")];
    let written = weft_journal::record_events(&pool, &events, Some("worker-b"), Some("worker-b")).await.unwrap();
    assert_eq!(written, 0);
    assert!(kinds_and_nodes(&pool, execution_id).await.is_empty());

    let unclaimed = weft_core::ExecutionId::new_v4();
    let written =
        weft_journal::record_events(&pool, &[started(unclaimed, "x")], Some("worker-a"), Some("worker-a")).await.unwrap();
    assert_eq!(written, 0, "no execution row, so no owner");
}

/// One write is one run's rows; a mix is refused before anything lands.
#[sqlx::test]
async fn one_write_never_mixes_two_runs(pool: PgPool) {
    schema(&pool).await;
    let one = claimed_by(&pool, "worker-a").await;
    let other = claimed_by(&pool, "worker-a").await;
    let err = weft_journal::record_events(&pool, &[started(one, "a"), started(other, "b")], Some("worker-a"), Some("worker-a"))
        .await
        .unwrap_err();
    assert!(matches!(err, RecordError::MixedExecutions { .. }), "{err}");
    assert!(kinds_and_nodes(&pool, one).await.is_empty());
}
