//! Layer-3 contract tests for a worker letting go of a run
//! (`weft_broker::door::let_go`), against a REAL Postgres: the run is
//! parked on its wait or queued for another worker, with no owner, only
//! while the asking worker drives it; and an answer handed to it while it
//! ran goes on its record as it is let go of.
//!
//! Gated behind `db-tests` (off by default) so a plain `cargo test` needs no PG.
#![cfg(feature = "db-tests")]

use sqlx::PgPool;
use weft_broker::door::{let_go, DoorWorker};
use weft_broker_client::protocol::LetGo;
use weft_dispatcher::journal::Journal;
use weft_journal::ExecEvent;

const PROJECT: uuid::Uuid = uuid::Uuid::from_u128(1);
const TENANT: &str = "t1";

/// The schema, and the project the runs belong to.
async fn schema(pool: &PgPool) {
    weft_dispatcher::app::apply_core_schema(pool).await.expect("core schema");
    sqlx::query(
        "INSERT INTO project (id, name, tenant_id, status, project_json, updated_at) \
         VALUES ($1, 'p', $2, 'registered', '{}', 0)",
    )
    .bind(PROJECT)
    .bind(TENANT)
    .execute(pool)
    .await
    .expect("project row");
}

fn worker(replica: &str) -> DoorWorker<'_> {
    DoorWorker { tenant: TENANT, project: PROJECT, replica }
}

/// A run driven by `owner`: queued, then claimed by it.
async fn driven_by(pool: &PgPool, owner: &str) -> weft_core::ExecutionId {
    let execution_id = weft_core::new_execution_id();
    let birth = ExecEvent::ExecutionStarted {
        execution_id,
        project_id: PROJECT,
        entry_node: "route".into(),
        phase: weft_core::context::Phase::Fire,
        definition_hash: Some("def-1".into()),
        binary_hash: Some("bin-1".into()),
        source_version: None,
        run_kind: weft_core::exec::RunKind::Execution,
        selection: None,
        seed: None,
        instance: None,
        stand_in: None,
        fired_trigger: None,
        instance_values: Default::default(),
        picks: Default::default(),
        settings: Default::default(),
        at_unix: 0,
    };
    let journal = weft_dispatcher::journal::postgres::PostgresJournal::from_pool(pool.clone());
    let queued = weft_journal::record::Queued {
        events: std::slice::from_ref(&birth),
        tenant: TENANT,
        keep_for: weft_core::run_settings::KeepFor::WEFT_DEFAULT,
        watch_end: false,
        stale: &[],
        spec: None,
        example: None,
    };
    assert!(journal.queue_run(queued, false).await.expect("queue the run"));
    let mut conn = pool.acquire().await.unwrap();
    weft_journal::record::claim(&mut conn, execution_id, PROJECT, owner).await.unwrap().expect("claimed");
    execution_id
}

/// The run's state and owner.
async fn standing(pool: &PgPool, execution_id: weft_core::ExecutionId) -> (String, Option<String>) {
    sqlx::query_as("SELECT state, owner FROM run WHERE execution_id = $1")
        .bind(execution_id)
        .fetch_one(pool)
        .await
        .expect("the run's row")
}

#[sqlx::test]
async fn a_run_handed_back_is_queued_for_another_worker(pool: PgPool) {
    schema(&pool).await;
    let execution_id = driven_by(&pool, "w1").await;
    assert!(let_go(&pool, worker("w1"), execution_id, LetGo::HandedBack).await.unwrap());
    assert_eq!(standing(&pool, execution_id).await, ("queued".into(), None));
}

#[sqlx::test]
async fn only_its_owner_lets_go_of_a_run(pool: PgPool) {
    schema(&pool).await;
    let execution_id = driven_by(&pool, "w1").await;
    assert!(!let_go(&pool, worker("w2"), execution_id, LetGo::HandedBack).await.unwrap());
    assert!(!let_go(&pool, worker("w2"), execution_id, LetGo::Parked).await.unwrap());
    assert_eq!(standing(&pool, execution_id).await, ("running".into(), Some("w1".into())));
}

#[sqlx::test]
async fn a_run_parked_waits_with_no_owner(pool: PgPool) {
    schema(&pool).await;
    let execution_id = driven_by(&pool, "w1").await;
    assert!(let_go(&pool, worker("w1"), execution_id, LetGo::Parked).await.unwrap());
    assert_eq!(standing(&pool, execution_id).await, ("parked".into(), None));
}

/// A run that ended is nobody's to let go of.
#[sqlx::test]
async fn a_run_that_ended_meanwhile_is_left_alone(pool: PgPool) {
    schema(&pool).await;
    let execution_id = driven_by(&pool, "w1").await;
    sqlx::query("UPDATE run SET state = 'ended', owner = NULL, outcome = 'cancelled', ended_at = 1 WHERE execution_id = $1")
        .bind(execution_id)
        .execute(&pool)
        .await
        .unwrap();
    assert!(!let_go(&pool, worker("w1"), execution_id, LetGo::HandedBack).await.unwrap());
    assert_eq!(standing(&pool, execution_id).await.0, "ended");
}

/// A letting go asked again after its answer was lost finds the run let go
/// of already, and answers as before.
#[sqlx::test]
async fn a_let_go_asked_again_is_answered_the_same(pool: PgPool) {
    schema(&pool).await;
    let execution_id = driven_by(&pool, "w1").await;
    assert!(let_go(&pool, worker("w1"), execution_id, LetGo::HandedBack).await.unwrap());
    assert!(let_go(&pool, worker("w1"), execution_id, LetGo::HandedBack).await.unwrap());
    assert!(!let_go(&pool, worker("w1"), execution_id, LetGo::Parked).await.unwrap(), "it was queued, not parked");
}

/// An answer handed to the run while its worker drove it, and not taken
/// before it parked, goes on its record as it is let go of, and the run is
/// queued to carry on with it.
#[sqlx::test]
async fn an_answer_handed_to_a_run_that_parks_reaches_its_record(pool: PgPool) {
    schema(&pool).await;
    let execution_id = driven_by(&pool, "w1").await;
    let mut tx = pool.begin().await.unwrap();
    weft_task_store::parked_fires::hand_answer_in(&mut tx, "form", execution_id, &weft_core::primitive::WaitAnswer::Given { value: serde_json::json!("yes") }).await.unwrap();
    tx.commit().await.unwrap();
    assert!(let_go(&pool, worker("w1"), execution_id, LetGo::Parked).await.unwrap());
    assert_eq!(standing(&pool, execution_id).await, ("queued".into(), None));
    let journal = weft_dispatcher::journal::postgres::PostgresJournal::from_pool(pool.clone());
    let events = journal.events_log(execution_id).await.unwrap();
    assert!(
        matches!(events.last(), Some(ExecEvent::SuspensionResolved { token, .. }) if token == "form"),
        "{events:?}"
    );
    assert!(weft_task_store::parked_fires::answers_for(&pool, execution_id).await.unwrap().is_empty(), "taken once");
}
