//! Layer-3 tests for how executions reach workers, against a REAL
//! Postgres: delivering an execution, a worker claiming exactly the
//! execution it was called for, a live run pinned to the worker its caller
//! reached, cancels reaching the worker that drives the execution, and the
//! orphans a vanished worker leaves. It all lives IN the SQL, so a faked
//! store would not catch what these catch.
//!
//! Gated behind `db-tests`; `scripts/run-db-tests.sh weft-task-store`
//! runs them.
#![cfg(feature = "db-tests")]

mod support;

use serde_json::{json, Value};
use sqlx::PgPool;
use uuid::Uuid;

use weft_core::run_class::RunClass;
use weft_task_store::tasks::{self, claim_one};
use weft_task_store::{TaskKind, TaskTarget};

use support::setup;

const PROJECT: Uuid = Uuid::from_u128(0x1);
const TENANT: &str = "tenant-1";

async fn seed_execution_id(pool: &PgPool, execution_id: &str) {
    sqlx::query(
        "INSERT INTO execution (execution_id, project_id, tenant_id, started_at_unix, phase) \
         VALUES ($1, $2, 'tenant-1', 0, 'running') ON CONFLICT (execution_id) DO NOTHING",
    )
    .bind(execution_id)
    .bind(PROJECT)
    .execute(pool)
    .await
    .expect("seed execution");
}

async fn execution_id_owner(pool: &PgPool, execution_id: &str) -> Option<String> {
    let row: Option<(Option<String>,)> = sqlx::query_as("SELECT owner_replica FROM execution WHERE execution_id = $1")
        .bind(execution_id)
        .fetch_optional(pool)
        .await
        .expect("read owner");
    row.and_then(|(p,)| p)
}

fn execute(execution_id: &str, run_class: &str) -> tasks::NewTask {
    let payload = json!({ "project_id": PROJECT, "execution_id": execution_id, "definition_hash": "h", "run_class": run_class });
    tasks::NewTask {
        kind: TaskKind::Execute.into(),
        target: TaskTarget::Worker,
        project_id: Some(PROJECT),
        dedup_key: Some(format!("{execution_id}:execute")),
        execution_id: Some(execution_id.to_string()),
        tenant_id: TENANT.to_string(),
        target_replica: None,
        binary_hash: Some("bin-1".into()),
        payload,
    }
}

/// A live run born at its caller's handshake, waiting for them until
/// `arrive_by`.
fn live_payload(execution_id: &str, arrive_by: i64) -> Value {
    json!({
        "project_id": PROJECT,
        "execution_id": execution_id,
        "definition_hash": "h",
        "run_class": "short",
        "live_connection": {
            "spec": { "kind": "socket", "config": {} },
            "request": { "method": "GET", "path": "" },
            "arrive_by": arrive_by
        }
    })
}

/// Queue the execute task of a live run whose caller is on the way.
async fn born_for_a_caller(pool: &PgPool, execution_id: &str, arrive_by: i64) -> Uuid {
    let spec = tasks::NewTask { payload: live_payload(execution_id, arrive_by), ..execute(execution_id, "short") };
    tasks::enqueue_dedup(pool, spec).await.unwrap().id()
}

fn far_future() -> i64 {
    i64::MAX / 2
}

/// The task a worker handed `execution_id` claims, as `replica`.
async fn claim(pool: &PgPool, replica: &str, execution_id: &str) -> anyhow::Result<Option<tasks::Task>> {
    Ok(tasks::claim_execution(pool, replica, PROJECT, execution_id).await?.map(|claimed| claimed.task))
}

async fn lapse_claim(pool: &PgPool, task_id: Uuid) {
    sqlx::query("UPDATE task SET claimed_until_unix = 0 WHERE id = $1")
        .bind(task_id)
        .execute(pool)
        .await
        .expect("lapse");
}

// ----- delivery ------------------------------------------------------------

/// A pending execution is delivered once, with the image and run class it
/// runs under, and not again while the delivery is outstanding.
#[sqlx::test]
async fn an_execution_is_delivered_once_with_its_image_and_class(pool: PgPool) {
    setup(&pool).await;
    let short = Uuid::new_v4().to_string();
    let long = Uuid::new_v4().to_string();
    tasks::enqueue_dedup(&pool, execute(&short, "short")).await.unwrap();
    tasks::enqueue_dedup(&pool, execute(&long, "long")).await.unwrap();
    let taken = tasks::take_deliveries(&pool, 10).await.unwrap().deliveries;
    assert_eq!(taken.len(), 2);
    let find = |c: &str| taken.iter().find(|d| d.execution_id == c).expect("delivered").clone();
    assert_eq!(find(&short).run_class, RunClass::Short);
    assert_eq!(find(&long).run_class, RunClass::Long);
    assert_eq!(find(&short).binary_hash, "bin-1");
    assert!(tasks::take_deliveries(&pool, 10).await.unwrap().is_empty(), "outstanding deliveries are not repeated");

    tasks::release_delivery(&pool, find(&short).task_id).await.unwrap();
    let again = tasks::take_deliveries(&pool, 10).await.unwrap().deliveries;
    assert_eq!(again.iter().map(|d| d.execution_id.clone()).collect::<Vec<_>>(), vec![short], "a released delivery is made again");
}

/// The status and error a task ended with.
async fn task_outcome(pool: &PgPool, task_id: Uuid) -> (String, Option<String>) {
    sqlx::query_as("SELECT status, error FROM task WHERE id = $1")
        .bind(task_id)
        .fetch_one(pool)
        .await
        .expect("task")
}

/// A corrupt worker task (no run class, no image) taken in the same batch
/// as good ones comes back undeliverable with its reason and its
/// execution (the caller writes the execution's terminal), the good ones
/// are delivered, and once failed the corrupt one is never taken again.
#[sqlx::test]
async fn a_corrupt_task_is_handed_back_and_never_holds_back_its_batch(pool: PgPool) {
    setup(&pool).await;
    let classless = Uuid::new_v4().to_string();
    let mut task = execute(&classless, "short");
    task.payload.as_object_mut().unwrap().remove("run_class");
    let classless_id = tasks::enqueue_dedup(&pool, task).await.unwrap().id();
    let blind = Uuid::new_v4().to_string();
    let blind_id = tasks::enqueue_dedup(&pool, tasks::NewTask { binary_hash: None, ..execute(&blind, "short") })
        .await
        .unwrap()
        .id();
    let good = Uuid::new_v4().to_string();
    tasks::enqueue_dedup(&pool, execute(&good, "long")).await.unwrap();

    let taken = tasks::take_deliveries(&pool, 10).await.unwrap();
    assert_eq!(taken.deliveries.iter().map(|d| d.execution_id.clone()).collect::<Vec<_>>(), vec![good], "the good row goes");
    let find = |id: Uuid| taken.undeliverable.iter().find(|u| u.task_id == id).expect("handed back").clone();
    let c = find(classless_id);
    assert_eq!(c.execution_id, Some(classless.parse().unwrap()));
    assert!(c.reason.contains("names no run class"), "{}", c.reason);
    let b = find(blind_id);
    assert_eq!(b.execution_id, Some(blind.parse().unwrap()));
    assert!(b.reason.contains("names no worker image"), "{}", b.reason);
    assert!(tasks::take_deliveries(&pool, 10).await.unwrap().is_empty(), "a handed-back row stays taken for its lease");

    for u in &taken.undeliverable {
        tasks::fail_undeliverable(&pool, u.task_id, &u.reason).await.unwrap();
        // A second dispatcher ending the same row changes nothing.
        tasks::fail_undeliverable(&pool, u.task_id, "another reason").await.unwrap();
    }
    let (status, error) = task_outcome(&pool, classless_id).await;
    assert_eq!(status, "failed");
    assert!(error.unwrap_or_default().contains("names no run class"));
    let (status, error) = task_outcome(&pool, blind_id).await;
    assert_eq!(status, "failed");
    assert!(error.unwrap_or_default().contains("names no worker image"));
}

/// A claim ends the delivery; a claim that lapses (its worker died) makes
/// the execution deliverable again.
#[sqlx::test]
async fn a_lapsed_claim_is_delivered_again(pool: PgPool) {
    setup(&pool).await;
    let execution_id = Uuid::new_v4().to_string();
    seed_execution_id(&pool, &execution_id).await;
    tasks::enqueue_dedup(&pool, execute(&execution_id, "short")).await.unwrap();
    assert_eq!(tasks::take_deliveries(&pool, 10).await.unwrap().len(), 1);
    let claimed = claim(&pool, "worker-a", &execution_id).await.unwrap().expect("claimed");
    assert!(tasks::take_deliveries(&pool, 10).await.unwrap().is_empty(), "a claimed execution needs no delivery");
    lapse_claim(&pool, claimed.id).await;
    let again = tasks::take_deliveries(&pool, 10).await.unwrap();
    assert_eq!(again.len(), 1, "the worker died: deliver it again");
    let reclaimed = claim(&pool, "worker-b", &execution_id).await.unwrap().expect("rescued");
    assert_eq!(reclaimed.attempts, 2);
    assert_eq!(execution_id_owner(&pool, &execution_id).await.as_deref(), Some("worker-b"), "ownership follows the claim");
}

/// A worker's claim hands back the execution's journal as it stood, in
/// order, and only that execution's: the worker drives from it without
/// reading it again.
#[sqlx::test]
async fn a_claim_hands_back_the_execution_journal(pool: PgPool) {
    setup(&pool).await;
    let (execution_id, other) = (Uuid::new_v4().to_string(), Uuid::new_v4().to_string());
    seed_execution_id(&pool, &execution_id).await;
    for (execution, payload) in [(&execution_id, "first"), (&other, "elsewhere"), (&execution_id, "second")] {
        sqlx::query("INSERT INTO exec_event (execution_id, payload_json) VALUES ($1, $2)")
            .bind(execution)
            .bind(payload)
            .execute(&pool)
            .await
            .unwrap();
    }
    tasks::enqueue_dedup(&pool, execute(&execution_id, "short")).await.unwrap();
    let claimed = tasks::claim_execution(&pool, "worker-a", PROJECT, &execution_id).await.unwrap().expect("claimed");
    let payloads: Vec<&str> = claimed.journal.iter().map(|row| row.payload.as_str()).collect();
    assert_eq!(payloads, ["first", "second"]);
    assert!(claimed.journal[0].id < claimed.journal[1].id, "in the journal's order");
    assert_eq!(claimed.task.attempts, 1);
    assert!(tasks::claim_execution(&pool, "worker-b", PROJECT, &execution_id).await.unwrap().is_none(), "claimed once");
}

/// A live run waiting for its caller is never delivered: only the worker
/// the caller reaches claims it.
#[sqlx::test]
async fn a_delivery_skips_runs_waiting_for_their_caller(pool: PgPool) {
    setup(&pool).await;
    let live = Uuid::new_v4().to_string();
    born_for_a_caller(&pool, &live, far_future()).await;
    assert!(tasks::take_deliveries(&pool, 10).await.unwrap().is_empty(), "a live run rides its caller's connection");
}

// ----- claiming one execution ------------------------------------------------

/// A worker called for one execution claims that execution and nothing
/// else, and ownership of the execution follows the claim.
#[sqlx::test]
async fn a_worker_claims_only_the_execution_it_was_called_for(pool: PgPool) {
    setup(&pool).await;
    let mine = Uuid::new_v4().to_string();
    let other = Uuid::new_v4().to_string();
    seed_execution_id(&pool, &mine).await;
    tasks::enqueue_dedup(&pool, execute(&mine, "short")).await.unwrap();
    tasks::enqueue_dedup(&pool, execute(&other, "short")).await.unwrap();
    let claimed = claim(&pool, "worker-a", &mine).await.unwrap().expect("claimed");
    assert_eq!(claimed.execution_id.as_deref(), Some(mine.as_str()));
    assert!(claim(&pool, "worker-b", &mine).await.unwrap().is_none(), "a duplicate delivery claims nothing");
    assert_eq!(execution_id_owner(&pool, &mine).await.as_deref(), Some("worker-a"));
}

/// A live run is claimed by the first worker its caller reaches, and the
/// claim pins it there: a copy of the request on another worker finds
/// nothing, even once the first claim lapses (the caller's socket was on
/// the first worker, so the run cannot move).
#[sqlx::test]
async fn a_live_run_is_pinned_to_the_worker_its_caller_reached(pool: PgPool) {
    setup(&pool).await;
    let execution_id = Uuid::new_v4().to_string();
    seed_execution_id(&pool, &execution_id).await;
    born_for_a_caller(&pool, &execution_id, far_future()).await;
    let claimed = claim(&pool, "worker-a", &execution_id).await.unwrap().expect("claimed");
    assert!(claim(&pool, "worker-b", &execution_id).await.unwrap().is_none(), "claimed here");
    lapse_claim(&pool, claimed.id).await;
    assert!(claim(&pool, "worker-b", &execution_id).await.unwrap().is_none(), "pinned to worker-a");
    assert!(tasks::take_deliveries(&pool, 10).await.unwrap().is_empty(), "and never delivered");
}

/// A live run whose caller never came is found once their ticket expires,
/// and not before; one whose caller did come is never found.
#[sqlx::test]
async fn a_caller_who_never_came_is_found_after_their_ticket_expires(pool: PgPool) {
    setup(&pool).await;
    let absent = Uuid::new_v4().to_string();
    let present = Uuid::new_v4().to_string();
    seed_execution_id(&pool, &present).await;
    let absent_task = born_for_a_caller(&pool, &absent, 1_000).await;
    born_for_a_caller(&pool, &present, 1_000).await;
    claim(&pool, "worker-a", &present).await.unwrap().expect("the caller arrived");
    assert!(tasks::callers_never_arrived(&pool, 999).await.unwrap().is_empty(), "the ticket is still good");
    let gone = tasks::callers_never_arrived(&pool, 1_001).await.unwrap();
    assert_eq!(gone.len(), 1);
    assert_eq!((gone[0].task_id, gone[0].execution_id.as_str()), (absent_task, absent.as_str()));
}

/// A resume asked for while its execution is still being driven waits:
/// it is neither delivered nor claimable until that drive ends, so one
/// execution is never driven twice at once.
#[sqlx::test]
async fn a_resume_waits_for_the_live_drive_of_its_execution_id(pool: PgPool) {
    setup(&pool).await;
    let execution_id = Uuid::new_v4().to_string();
    seed_execution_id(&pool, &execution_id).await;
    tasks::enqueue_dedup(&pool, execute(&execution_id, "short")).await.unwrap();
    let driving = claim(&pool, "worker-a", &execution_id).await.unwrap().expect("claimed");
    tasks::enqueue_dedup(
        &pool,
        tasks::NewTask { kind: TaskKind::Resume.into(), dedup_key: Some(format!("{execution_id}:resume")), ..execute(&execution_id, "short") },
    )
    .await
    .unwrap();
    assert!(tasks::take_deliveries(&pool, 10).await.unwrap().is_empty(), "not delivered while driven");
    assert!(claim(&pool, "worker-b", &execution_id).await.unwrap().is_none(), "not claimable while driven");
    tasks::complete(&pool, driving.id, "worker-a", json!(null)).await.unwrap();
    assert_eq!(tasks::take_deliveries(&pool, 10).await.unwrap().len(), 1, "the drive ended: deliver the resume");
    let resumed = claim(&pool, "worker-b", &execution_id).await.unwrap().expect("the resume");
    assert_eq!(resumed.kind, "resume");
}

// ----- cancels ---------------------------------------------------------------

fn cancel(execution_id: &str) -> tasks::NewTask {
    tasks::NewTask {
        kind: TaskKind::CancelExecution.into(),
        target: TaskTarget::Worker,
        project_id: Some(PROJECT),
        dedup_key: Some(format!("{execution_id}:cancel")),
        execution_id: Some(execution_id.to_string()),
        tenant_id: TENANT.to_string(),
        target_replica: None,
        binary_hash: None,
        payload: json!({ "project_id": PROJECT, "execution_id": execution_id, "cause": { "kind": "user" } }),
    }
}

/// A cancel is taken only by a worker driving its execution, once, and never
/// delivered as work.
#[sqlx::test]
async fn a_cancel_reaches_the_worker_driving_its_execution_id_once(pool: PgPool) {
    setup(&pool).await;
    let execution_id = Uuid::new_v4().to_string();
    tasks::enqueue_dedup(&pool, cancel(&execution_id)).await.unwrap();
    assert!(tasks::take_deliveries(&pool, 10).await.unwrap().is_empty(), "a cancel is never delivered");
    assert!(tasks::take_cancels(&pool, PROJECT, &["someone-else".into()]).await.unwrap().is_empty());
    let taken = tasks::take_cancels(&pool, PROJECT, std::slice::from_ref(&execution_id)).await.unwrap();
    assert_eq!(taken.len(), 1);
    assert_eq!(taken[0].execution_id, execution_id);
    assert!(tasks::take_cancels(&pool, PROJECT, &[execution_id]).await.unwrap().is_empty(), "taken once");
}

/// A cancel nobody will take (its execution is not driven any more) is
/// dropped once it is older than a claim; one waiting on a live drive is
/// kept.
#[sqlx::test]
async fn a_cancel_nobody_will_take_is_dropped(pool: PgPool) {
    setup(&pool).await;
    let stale = Uuid::new_v4().to_string();
    let driven = Uuid::new_v4().to_string();
    seed_execution_id(&pool, &driven).await;
    tasks::enqueue_dedup(&pool, execute(&driven, "short")).await.unwrap();
    claim(&pool, "worker-a", &driven).await.unwrap().expect("driven");
    tasks::enqueue_dedup(&pool, cancel(&stale)).await.unwrap();
    tasks::enqueue_dedup(&pool, cancel(&driven)).await.unwrap();
    sqlx::query("UPDATE task SET created_at_unix = 0 WHERE kind = 'cancel_execution'").execute(&pool).await.unwrap();
    assert_eq!(tasks::drop_stale_cancels(&pool).await.unwrap(), 1);
    assert_eq!(tasks::take_cancels(&pool, PROJECT, &[driven]).await.unwrap().len(), 1, "the driven execution's cancel stays");
}

// ----- orphans -------------------------------------------------------------

/// A live run whose worker went away is an orphan: its claim lapsed, or it
/// was put back pending (still pinned to that worker) and not claimed again
/// within a claim's duration. A live run being driven is not, and neither
/// is one still waiting for its caller (that one is the reaper's other
/// sweep, `callers_never_arrived`).
#[sqlx::test]
async fn a_live_run_whose_worker_vanished_is_an_orphan(pool: PgPool) {
    setup(&pool).await;
    let driven = Uuid::new_v4().to_string();
    let lapsed = Uuid::new_v4().to_string();
    let put_back = Uuid::new_v4().to_string();
    let waiting = Uuid::new_v4().to_string();
    for c in [&driven, &lapsed, &put_back, &waiting] {
        seed_execution_id(&pool, c).await;
        born_for_a_caller(&pool, c, far_future()).await;
    }
    claim(&pool, "worker-a", &driven).await.unwrap().expect("driven");
    let lapsing = claim(&pool, "worker-a", &lapsed).await.unwrap().expect("claimed");
    lapse_claim(&pool, lapsing.id).await;
    let back = claim(&pool, "worker-a", &put_back).await.unwrap().expect("claimed");
    assert!(tasks::requeue(&pool, back.id, "worker-a").await.unwrap());
    sqlx::query("UPDATE task SET created_at_unix = 0 WHERE execution_id = ANY($1)")
        .bind(vec![put_back.clone(), waiting.clone()])
        .execute(&pool)
        .await
        .unwrap();
    let mut orphans: Vec<String> = tasks::orphaned_live_executions(&pool).await.unwrap().into_iter().map(|o| o.execution_id).collect();
    orphans.sort();
    let mut expected = vec![lapsed, put_back];
    expected.sort();
    assert_eq!(orphans, expected);
}

// ----- the claim's own record ----------------------------------------------

fn dispatcher_task(dedup: &str) -> tasks::NewTask {
    tasks::NewTask {
        kind: "run_node_test".to_string(),
        target: TaskTarget::Dispatcher,
        project_id: Some(PROJECT),
        dedup_key: Some(dedup.to_string()),
        execution_id: None,
        tenant_id: TENANT.to_string(),
        target_replica: None,
        binary_hash: None,
        payload: json!({}),
    }
}

/// The partial-result surface a non-re-runnable executor uses: a
/// still-claimed row records its harvested result without completing,
/// guarded on the claimant; a later read returns it; a write from a
/// claimant that no longer holds the claim fails loudly.
#[sqlx::test]
async fn partial_result_round_trips_on_the_task_row(pool: PgPool) {
    setup(&pool).await;
    let task_id = tasks::enqueue(&pool, dispatcher_task("t1")).await.expect("enqueue");
    assert_eq!(tasks::stored_result(&pool, task_id).await.expect("read"), None);
    let report = json!({"passed": true, "node": "X", "test": "t"});
    assert!(tasks::store_result_partial(&pool, task_id, "disp-1", &report).await.is_err(), "unclaimed");
    let claimed = claim_one(&pool, "disp-1").await.expect("claim").expect("the task");
    assert_eq!(claimed.id, task_id);
    assert_eq!(claimed.attempts, 1, "first claim");
    assert!(tasks::store_result_partial(&pool, task_id, "disp-2", &report).await.is_err(), "not the claimant");
    tasks::store_result_partial(&pool, task_id, "disp-1", &report).await.expect("store partial");
    assert_eq!(tasks::stored_result(&pool, task_id).await.expect("read"), Some(report));
    let (status,): (String,) = sqlx::query_as("SELECT status FROM task WHERE id = $1").bind(task_id).fetch_one(&pool).await.unwrap();
    assert_eq!(status, "claimed", "recording is not completing");
    assert_eq!(tasks::stored_result(&pool, Uuid::new_v4()).await.expect("read"), None);
}

/// Surrender-requeue: the claimer that can no longer renew its lease puts
/// the row back to `pending`, and a requeue from a claimant that lost the
/// row is a no-op that never clobbers the new claim.
#[sqlx::test]
async fn surrender_requeues_only_while_claim_is_ours(pool: PgPool) {
    setup(&pool).await;
    let task_id = tasks::enqueue(&pool, dispatcher_task("t1")).await.expect("enqueue");
    claim_one(&pool, "disp-1").await.expect("claim").expect("the task");
    assert!(!tasks::requeue(&pool, task_id, "disp-2").await.expect("requeue"));
    assert!(tasks::requeue(&pool, task_id, "disp-1").await.expect("requeue"));
    let (status, claimed_by): (String, Option<String>) =
        sqlx::query_as("SELECT status, claimed_by FROM task WHERE id = $1").bind(task_id).fetch_one(&pool).await.unwrap();
    assert_eq!((status.as_str(), claimed_by), ("pending", None));
    let reclaimed = claim_one(&pool, "disp-2").await.expect("claim").expect("requeued");
    assert_eq!(reclaimed.attempts, 2);
    assert!(!tasks::requeue(&pool, task_id, "disp-1").await.expect("requeue"));
    let (status, claimed_by): (String, Option<String>) =
        sqlx::query_as("SELECT status, claimed_by FROM task WHERE id = $1").bind(task_id).fetch_one(&pool).await.unwrap();
    assert_eq!((status.as_str(), claimed_by.as_deref()), ("claimed", Some("disp-2")));
}
