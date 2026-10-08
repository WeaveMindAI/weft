//! Layer-3 contract tests for the one write of a worker's records
//! (`weft_journal::record::record_batch`, the `weft_record_batch` function),
//! against a REAL Postgres: a run born, carried on and ended by batches; a
//! batch sent again changing nothing; a run two workers bore; a batch that
//! does not follow on; two lanes of one worker writing side by side; and
//! the fence between a late batch and the sweep that let go of its run.
//! The fences live in the statement itself, so only a real database can
//! prove them.
//!
//! Gated behind `db-tests` (off by default) so a plain `cargo test` needs no PG.
#![cfg(feature = "db-tests")]

use std::time::Duration;

use sqlx::PgPool;
use weft_core::run_settings::{KeepFor, Keeping, RunSettings};
use weft_dispatcher::journal::{Journal, Lost};
use weft_journal::frame::{Batch, BatchHead, RunHead};
use weft_journal::record::{record_batch, Born, Fate, Written};
use weft_journal::ExecEvent;

const PROJECT: uuid::Uuid = uuid::Uuid::from_u128(1);
const TENANT: &str = "t1";

async fn schema(pool: &PgPool) {
    weft_dispatcher::app::apply_core_schema(pool).await.expect("core schema");
}

fn birth(execution_id: weft_core::ExecutionId, keeping: Keeping) -> ExecEvent {
    ExecEvent::ExecutionStarted {
        execution_id,
        project_id: PROJECT,
        entry_node: "route".into(),
        phase: weft_core::context::Phase::Fire,
        definition_hash: Some("def-1".into()),
        binary_hash: Some("bin-1".into()),
        source_version: Some("v1".into()),
        run_kind: weft_core::exec::RunKind::Execution,
        selection: None,
        seed: None,
        instance: None,
        stand_in: None,
        fired_trigger: Some("route".into()),
        instance_values: Default::default(),
        picks: Default::default(),
        settings: RunSettings::new(keeping, true).unwrap(),
        at_unix: 1,
    }
}

fn started(execution_id: weft_core::ExecutionId, node: &str) -> ExecEvent {
    ExecEvent::NodeStarted { execution_id, node_id: node.into(), frames: Vec::new(), at_unix: 2 }
}

fn completed(execution_id: weft_core::ExecutionId) -> ExecEvent {
    ExecEvent::ExecutionCompleted { execution_id, at_unix: 3 }
}

fn cost(execution_id: weft_core::ExecutionId) -> ExecEvent {
    ExecEvent::CostReported {
        execution_id,
        node_id: "llm".into(),
        frames: Vec::new(),
        cost_id: "c1".into(),
        service: "llm".into(),
        model: None,
        amount_usd: Some(0.25),
        billed: false,
        origin: weft_core::CredentialOwner::Platform,
        metadata: serde_json::Value::Null,
        at_unix: 2,
    }
}

/// One run's share of a batch: its rows from `first_seq` on, under
/// `epoch`.
struct Part {
    execution_id: weft_core::ExecutionId,
    epoch: i32,
    first_seq: i32,
    rows: Vec<Vec<ExecEvent>>,
}

fn part(execution_id: weft_core::ExecutionId, epoch: i32, first_seq: i32, rows: Vec<Vec<ExecEvent>>) -> Part {
    Part { execution_id, epoch, first_seq, rows }
}

/// Write `parts` as one batch of `writer`'s lane `lane`, the way the broker
/// writes what a lane sent; each run's fate, in order.
async fn send(pool: &PgPool, writer: &str, lane: &str, parts: &[Part]) -> Vec<Fate> {
    let mut conn = pool.acquire().await.unwrap();
    send_on(&mut conn, writer, lane, parts).await
}

async fn send_on(conn: &mut sqlx::PgConnection, writer: &str, lane: &str, parts: &[Part]) -> Vec<Fate> {
    let encoded: Vec<Vec<Vec<u8>>> =
        parts.iter().map(|part| part.rows.iter().map(|row| weft_journal::stored::encode(row)).collect()).collect();
    let head = BatchHead {
        durable: true,
        lane: 0,
        runs: parts
            .iter()
            .zip(&encoded)
            .map(|(part, rows)| RunHead {
                execution_id: part.execution_id,
                epoch: part.epoch,
                first_seq: part.first_seq,
                row_sizes: rows.iter().map(|row| row.len() as u32).collect(),
                born: (part.first_seq == 0).then(|| Born::of(&part.rows[0][0], KeepFor::WEFT_DEFAULT).expect("a birth")),
                written: Written::of(&part.rows.concat()),
                wrote_files: false,
            })
            .collect(),
        selections: Vec::new(),
    };
    let batch = Batch { head, rows: encoded.iter().flatten().map(Vec::as_slice).collect() };
    record_batch(conn, writer, TENANT, lane, &batch).await.unwrap().into_iter().map(|recorded| recorded.fate).collect()
}

/// A run's row as the tests read it: state, owner, epoch, last row, cost.
async fn row(pool: &PgPool, execution_id: weft_core::ExecutionId) -> (String, Option<String>, i32, i32, i64) {
    sqlx::query_as("SELECT state, owner, epoch, last_seq, cost_micro_usd FROM run WHERE execution_id = $1")
        .bind(execution_id)
        .fetch_one(pool)
        .await
        .expect("the run's row")
}

async fn rows_of(pool: &PgPool, execution_id: weft_core::ExecutionId) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM run_log WHERE execution_id = $1").bind(execution_id).fetch_one(pool).await.unwrap()
}

/// A run is born by its first batch, owned by its writer; the next batch
/// carries it on from its last row; and a run born and ended in one batch
/// is born ended, its row holding how it ended.
#[sqlx::test]
async fn a_run_is_born_carried_on_and_ended_by_its_batches(pool: PgPool) {
    schema(&pool).await;
    let run = weft_core::new_execution_id();
    let quick = weft_core::new_execution_id();
    let fates = send(
        &pool,
        "w1",
        "w1:0",
        &[part(run, 1, 0, vec![vec![birth(run, Keeping::Fast)]]), part(quick, 1, 0, vec![vec![birth(quick, Keeping::Fast), completed(quick)]])],
    )
    .await;
    assert_eq!(fates, [Fate::Accepted, Fate::Accepted]);
    assert_eq!(row(&pool, run).await, ("running".into(), Some("w1".into()), 1, 0, 0));
    assert_eq!(row(&pool, quick).await, ("ended".into(), None, 1, 0, 0));
    let outcome: Option<String> = sqlx::query_scalar("SELECT outcome FROM run WHERE execution_id = $1").bind(quick).fetch_one(&pool).await.unwrap();
    assert_eq!(outcome.as_deref(), Some("completed"));

    let fates = send(&pool, "w1", "w1:0", &[part(run, 1, 1, vec![vec![started(run, "a")], vec![cost(run)]])]).await;
    assert_eq!(fates, [Fate::Accepted]);
    assert_eq!(row(&pool, run).await, ("running".into(), Some("w1".into()), 1, 2, 250_000));
    assert_eq!(send(&pool, "w1", "w1:0", &[part(run, 1, 3, vec![vec![completed(run)]])]).await, [Fate::Accepted]);
    assert_eq!(row(&pool, run).await, ("ended".into(), None, 1, 3, 250_000));
}

/// A batch sent again after its answer was lost changes nothing and says
/// so: a first batch, a continuing batch carrying a cost (counted once),
/// and an ending.
#[sqlx::test]
async fn a_batch_sent_again_changes_nothing(pool: PgPool) {
    schema(&pool).await;
    let run = weft_core::new_execution_id();
    let first = [part(run, 1, 0, vec![vec![birth(run, Keeping::Fast)]])];
    let paid = [part(run, 1, 1, vec![vec![cost(run)]])];
    let ending = [part(run, 1, 2, vec![vec![completed(run)]])];
    for batch in [&first, &paid, &ending] {
        assert_eq!(send(&pool, "w1", "w1:0", batch).await, [Fate::Accepted]);
        let before = (row(&pool, run).await, rows_of(&pool, run).await);
        assert_eq!(send(&pool, "w1", "w1:0", batch).await, [Fate::AlreadyApplied]);
        assert_eq!((row(&pool, run).await, rows_of(&pool, run).await), before, "nothing changed the second time");
    }
    assert_eq!(row(&pool, run).await.4, 250_000, "the cost counted once");
    let counted: i64 = sqlx::query_scalar("SELECT SUM(runs)::bigint FROM version_runs").fetch_one(&pool).await.unwrap();
    assert_eq!(counted, 1, "the version counted the run once");
}

/// Two workers that took one fire both bear its run: the first is the
/// run's, and the second hears its run was born elsewhere, with nothing of
/// its own written.
#[sqlx::test]
async fn a_run_two_workers_bore_is_the_first_ones(pool: PgPool) {
    schema(&pool).await;
    let run = weft_core::new_execution_id();
    assert_eq!(send(&pool, "w1", "w1:0", &[part(run, 1, 0, vec![vec![birth(run, Keeping::Fast)]])]).await, [Fate::Accepted]);
    let other = [part(run, 1, 0, vec![vec![birth(run, Keeping::Fast), started(run, "b")]])];
    assert_eq!(send(&pool, "w2", "w2:0", &other).await, [Fate::BornElsewhere]);
    assert_eq!(row(&pool, run).await.1.as_deref(), Some("w1"));
    assert_eq!(rows_of(&pool, run).await, 1);
}

/// Rows that do not follow on from the run's record are refused: a gap in
/// `first_seq`, another writer, an epoch the run has moved past. Nothing
/// goes in.
#[sqlx::test]
async fn a_batch_that_does_not_follow_on_is_refused(pool: PgPool) {
    schema(&pool).await;
    let run = weft_core::new_execution_id();
    send(&pool, "w1", "w1:0", &[part(run, 1, 0, vec![vec![birth(run, Keeping::Fast)]])]).await;
    assert_eq!(send(&pool, "w1", "w1:0", &[part(run, 1, 2, vec![vec![started(run, "gap")]])]).await, [Fate::Refused], "a gap");
    assert_eq!(send(&pool, "w2", "w2:0", &[part(run, 1, 1, vec![vec![started(run, "x")]])]).await, [Fate::Refused], "not its writer");
    assert_eq!(send(&pool, "w1", "w1:0", &[part(run, 0, 1, vec![vec![started(run, "old")]])]).await, [Fate::Refused], "an old epoch");
    assert_eq!(rows_of(&pool, run).await, 1);
    assert_eq!(row(&pool, run).await.3, 0);
}

/// Two lanes of one worker count runs of the same version on rows of
/// their own: one lane's batch still open never holds the other's up.
#[sqlx::test]
async fn two_lanes_counting_one_version_never_block_each_other(pool: PgPool) {
    schema(&pool).await;
    let (a, b) = (weft_core::new_execution_id(), weft_core::new_execution_id());
    let mut open = pool.begin().await.unwrap();
    assert_eq!(send_on(&mut open, "w1", "w1:0", &[part(a, 1, 0, vec![vec![birth(a, Keeping::Fast)]])]).await, [Fate::Accepted]);
    let other = tokio::time::timeout(
        Duration::from_secs(5),
        send(&pool, "w1", "w1:1", &[part(b, 1, 0, vec![vec![birth(b, Keeping::Fast)]])]),
    )
    .await
    .expect("the second lane's batch went in while the first one's was still open");
    assert_eq!(other, [Fate::Accepted]);
    open.commit().await.unwrap();
    let counted: i64 = sqlx::query_scalar("SELECT SUM(runs)::bigint FROM version_runs WHERE source_version = 'v1'").fetch_one(&pool).await.unwrap();
    assert_eq!(counted, 2);
}

/// Claim `run` for `owner` on a lapsed lease, the way a worker that then
/// died had it.
async fn driven_by_a_dead_worker(pool: &PgPool, run: weft_core::ExecutionId) {
    sqlx::query(
        "INSERT INTO worker_lease (replica, project_id, tenant_id, leased_until_unix) VALUES ('w1', $1, $2, 0) \
         ON CONFLICT (replica) DO NOTHING",
    )
    .bind(PROJECT)
    .bind(TENANT)
    .execute(pool)
    .await
    .unwrap();
    assert_eq!(send(pool, "w1", "w1:0", &[part(run, 1, 0, vec![vec![birth(run, Keeping::Durable)]])]).await, [Fate::Accepted]);
}

/// The sweep letting go of a lost run raises its epoch first: a late batch
/// of the worker it lost is refused, and the run is queued for the next.
#[sqlx::test]
async fn a_late_batch_after_the_lost_sweep_is_refused(pool: PgPool) {
    schema(&pool).await;
    let journal = weft_dispatcher::journal::postgres::PostgresJournal::from_pool(pool.clone());
    let run = weft_core::new_execution_id();
    driven_by_a_dead_worker(&pool, run).await;
    assert_eq!(journal.let_go_of_lost(run, i64::MAX, None).await.unwrap(), Lost::Requeued);
    assert_eq!(send(&pool, "w1", "w1:0", &[part(run, 1, 1, vec![vec![started(run, "late")]])]).await, [Fate::Refused]);
    assert_eq!(row(&pool, run).await, ("queued".into(), None, 2, 0, 0));
}

/// A late batch that holds the run's row when the sweep comes goes in
/// first, and the sweep then lets go of the run from where the batch left
/// it, without either waiting on the other for ever.
#[sqlx::test]
async fn a_late_batch_before_the_lost_sweep_goes_in_and_the_sweep_follows(pool: PgPool) {
    schema(&pool).await;
    let journal = weft_dispatcher::journal::postgres::PostgresJournal::from_pool(pool.clone());
    let run = weft_core::new_execution_id();
    driven_by_a_dead_worker(&pool, run).await;
    let mut open = pool.begin().await.unwrap();
    assert_eq!(send_on(&mut open, "w1", "w1:0", &[part(run, 1, 1, vec![vec![started(run, "late")]])]).await, [Fate::Accepted]);
    let sweep = tokio::spawn(async move { journal.let_go_of_lost(run, i64::MAX, None).await.unwrap() });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!sweep.is_finished(), "the sweep waits for the batch holding the run's row");
    open.commit().await.unwrap();
    let lost = tokio::time::timeout(Duration::from_secs(5), sweep).await.expect("no deadlock").unwrap();
    assert_eq!(lost, Lost::Requeued);
    assert_eq!(row(&pool, run).await, ("queued".into(), None, 2, 1, 0), "let go of from the batch's last row");
}
