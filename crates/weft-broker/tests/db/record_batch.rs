//! Layer-3 contract tests for the one write of a worker's records
//! (`weft_journal::record::record_batch`, the `weft_record_batch` function),
//! against a REAL Postgres: a run born, carried on and ended by batches; a
//! batch sent again changing nothing; a run two workers bore; a batch that
//! does not follow on; two lanes of one worker writing side by side; the
//! fence between a late batch and the sweep that let go of its run; and the
//! sealed secrets a batch carries.
//! The fences live in the statement itself, so only a real database can
//! prove them.

use std::time::Duration;

use weft_task_store::db::{Client, JsonText, Pool};
use weft_core::run_settings::{KeepFor, Keeping, RunSettings};
use weft_dispatcher::store::{Lost, RunStore};
use weft_journal::frame::{Batch, BatchHead, KeptSecret, RunHead};
use weft_journal::record::{record_batch, Born, Fate, Written};
use weft_journal::RunEvent;

const PROJECT: uuid::Uuid = uuid::Uuid::from_u128(1);
const TENANT: &str = "t1";

/// The schema, and the project the runs are born in: a birth for a
/// project that is not there is refused.
async fn schema(pool: &Pool) {
    weft_task_store::apply_groups(pool, weft_dispatcher::app::DISPATCHER_GROUPS).await.expect("core schema");
    pool.execute(
        "INSERT INTO project (id, name, tenant_id, status, project_json, updated_at, project_json_version, resync_version, running_build_inputs_version) \
         VALUES ($1, 'p', $2, 'inactive', '{}', 0, $3, 1, 1)",
        &[&PROJECT, &TENANT, &weft_core::ProjectDefinition::STORED.version()],
    )
    .await
    .expect("project row");
}

fn birth(run_id: weft_core::RunId, keeping: Keeping) -> RunEvent {
    RunEvent::RunStarted { given: Default::default(),
        run_id,
        project_id: PROJECT,
        entry_node: "route".into(),
        phase: weft_core::context::Phase::Fire,
        definition_hash: Some("def-1".into()),
        binary_hash: Some("bin-1".into()),
        source_version: Some("v1".into()),
        run_kind: weft_core::run::RunKind::Project,
        cut_from: None,
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

fn started(run_id: weft_core::RunId, node: &str) -> RunEvent {
    RunEvent::NodeStarted { run_id, node_id: node.into(), frames: Vec::new(), at_unix: 2 }
}

fn completed(run_id: weft_core::RunId) -> RunEvent {
    RunEvent::RunCompleted { run_id, at_unix: 3 }
}

fn cost(run_id: weft_core::RunId) -> RunEvent {
    RunEvent::CostReported {
        run_id,
        node_id: "llm".into(),
        frames: Vec::new(),
        cost_id: "c1".into(),
        service: "llm".into(),
        model: None,
        amount_usd: Some(0.25),
        billed: false,
        origin: weft_core::CredentialOwner::Platform,
        metadata: serde_json::Value::Null,
        charge: None,
        at_unix: 2,
    }
}

/// One run's share of a batch: its rows from `first_seq` on, under
/// `epoch`.
struct Part {
    run_id: weft_core::RunId,
    epoch: i32,
    first_seq: i32,
    rows: Vec<Vec<RunEvent>>,
    secrets: Vec<KeptSecret>,
}

fn part(run_id: weft_core::RunId, epoch: i32, first_seq: i32, rows: Vec<Vec<RunEvent>>) -> Part {
    Part { run_id, epoch, first_seq, rows, secrets: Vec::new() }
}

/// Write `parts` as one batch of `writer`'s lane `lane`, the way the broker
/// writes what a lane sent; each run's fate, in order.
async fn send(pool: &Pool, writer: &str, lane: &str, parts: &[Part]) -> Vec<Fate> {
    let conn = pool.get().await.unwrap();
    send_on(&conn, writer, lane, parts).await
}

async fn send_on(db: &impl Client, writer: &str, lane: &str, parts: &[Part]) -> Vec<Fate> {
    answers_on(db, writer, lane, parts).await.into_iter().map(|recorded| recorded.fate).collect()
}

/// Write `parts` as [`send_on`] does; each run's whole answer, in order.
async fn answers_on(db: &impl Client, writer: &str, lane: &str, parts: &[Part]) -> Vec<weft_journal::record::Recorded> {
    answers_with(db, writer, lane, parts, Vec::new()).await
}

/// [`answers_on`], the batch also carrying `programs`, the cuts its runs run.
async fn answers_with(
    db: &impl Client,
    writer: &str,
    lane: &str,
    parts: &[Part],
    programs: Vec<weft_journal::record::StoredProgram>,
) -> Vec<weft_journal::record::Recorded> {
    let encoded: Vec<Vec<Vec<u8>>> =
        parts.iter().map(|part| part.rows.iter().map(|row| weft_journal::stored::encode(row)).collect()).collect();
    let head = BatchHead {
        durable: true,
        lane: 0,
        runs: parts
            .iter()
            .zip(&encoded)
            .map(|(part, rows)| RunHead {
                run_id: part.run_id,
                epoch: part.epoch,
                first_seq: part.first_seq,
                row_sizes: rows.iter().map(|row| row.len() as u32).collect(),
                born: (part.first_seq == 0).then(|| Born::of(&part.rows[0][0], KeepFor::WEFT_DEFAULT).expect("a birth")),
                written: Written::of(&part.rows.concat()),
                secrets: part.secrets.clone(),
            })
            .collect(),
        programs,
        events_version: weft_journal::stored::STORED_EVENTS.version(),
    };
    let batch = Batch { head, rows: encoded.iter().flatten().map(Vec::as_slice).collect() };
    record_batch(db, writer, TENANT, lane, &batch, &weft_access_store::seal_json).await.unwrap()
}

/// A run that ended keeps its files: nothing is queued to delete them
/// until its record goes.
#[weft_db_macros::test]
async fn a_run_keeps_its_files_past_its_end(pool: Pool) {
    schema(&pool).await;
    let run = weft_core::new_run_id();
    let ended = part(run, 1, 0, vec![vec![birth(run, Keeping::Fast), completed(run)]]);
    assert_eq!(send(&pool, "w1", "w1:0", &[ended]).await, [Fate::Accepted]);
    let queued: i64 = pool.scalar("SELECT COUNT(*) FROM storage_sweep WHERE run_id = $1", &[&run]).await.unwrap();
    assert_eq!(queued, 0, "ending a run deletes none of its files");
}

/// [`birth`] of a run of `program`, a cut of the registered `def-1`, for
/// `project`: its birth names the cut and what it was cut from.
fn cut_birth(run_id: weft_core::RunId, project: uuid::Uuid, program: &weft_journal::record::RunsProgram) -> RunEvent {
    let mut born = birth(run_id, Keeping::Fast);
    if let RunEvent::RunStarted { project_id, definition_hash, cut_from, .. } = &mut born {
        *project_id = project;
        *definition_hash = Some(program.definition_hash().to_string());
        *cut_from = program.cut_from().map(str::to_string);
    }
    born
}

/// The cut program of `project`: no node at all, so never `def-1`.
fn cut_of(project: uuid::Uuid) -> weft_journal::record::RunsProgram {
    let cut: weft_core::ProjectDefinition = serde_json::from_value(serde_json::json!({
        "id": project, "nodes": [], "edges": [], "groups": [],
    })).unwrap();
    let program = weft_journal::record::RunsProgram::of("def-1", project, &cut).unwrap();
    assert_eq!(program.cut_from(), Some("def-1"), "a cut, not the registered program");
    program
}

/// What `project_definition` keeps under `hash` for `project`: each row's
/// program and what it was cut from.
async fn kept(pool: &Pool, project: uuid::Uuid, hash: &str) -> Vec<(String, Option<String>)> {
    pool.all("SELECT project_json, cut_from FROM project_definition WHERE project_id = $1 AND definition_hash = $2", &[&project, &hash])
        .await
        .unwrap()
}

/// A run of a cut names the cut by its hash, and the batch that bears it
/// carries the cut: the write keeps it with the project's programs, once
/// however often a batch carries it again, marked with what it was cut
/// from, so the run reads back what it ran and the cut never reads as a
/// registered version.
#[weft_db_macros::test]
async fn a_batch_keeps_the_cut_its_runs_run(pool: Pool) {
    schema(&pool).await;
    let program = cut_of(PROJECT);
    let carried = program.kept().expect("a cut is carried").as_ref().clone();
    for _ in 0..2 {
        let run = weft_core::new_run_id();
        let conn = pool.get().await.unwrap();
        let answers = answers_with(&conn, "w1", "w1:0", &[part(run, 1, 0, vec![vec![cut_birth(run, PROJECT, &program)]])], vec![carried.clone()]).await;
        assert_eq!(answers[0].fate, Fate::Accepted);
    }
    let rows = kept(&pool, PROJECT, program.definition_hash()).await;
    assert_eq!(rows.len(), 1, "one row however often it is carried");
    assert_eq!(rows[0].1.as_deref(), Some("def-1"), "kept as a cut of def-1");
    let read: weft_core::ProjectDefinition = serde_json::from_str(&rows[0].0).unwrap();
    assert_eq!(weft_core::project::hash::compute_definition_hash(&read).unwrap(), program.definition_hash());
}

/// A cut is kept only with a run that went in naming it: a birth refused
/// (its project is not there) keeps nothing of its cut, and a cut carried
/// beside a birth that runs the registered program is not kept either.
#[weft_db_macros::test]
async fn a_batch_keeps_no_cut_without_a_run_that_went_in(pool: Pool) {
    schema(&pool).await;
    let gone = uuid::Uuid::from_u128(2);
    let program = cut_of(gone);
    let run = weft_core::new_run_id();
    let conn = pool.get().await.unwrap();
    let carried = program.kept().expect("a cut is carried").as_ref().clone();
    let answers = answers_with(&conn, "w1", "w1:0", &[part(run, 1, 0, vec![vec![cut_birth(run, gone, &program)]])], vec![carried]).await;
    assert_eq!(answers[0].fate, Fate::Refused);
    assert!(kept(&pool, gone, program.definition_hash()).await.is_empty(), "a refused birth keeps nothing");

    let unnamed = cut_of(PROJECT);
    let run = weft_core::new_run_id();
    let carried = unnamed.kept().expect("a cut is carried").as_ref().clone();
    let answers = answers_with(&conn, "w1", "w1:0", &[part(run, 1, 0, vec![vec![birth(run, Keeping::Fast)]])], vec![carried]).await;
    assert_eq!(answers[0].fate, Fate::Accepted);
    assert!(kept(&pool, PROJECT, unnamed.definition_hash()).await.is_empty(), "no birth runs it as a cut");
}

/// A run born running under a writer whose lease was never written (it
/// has not ticked yet) is answered unwatched, so the broker wakes the
/// look at lapsed holds for it; one born under a live lease, or born
/// ended, is not. A birth for a project that is not there is refused.
#[weft_db_macros::test]
async fn a_run_born_before_its_writers_first_tick_is_answered_unwatched(pool: Pool) {
    schema(&pool).await;
    let (early, ended) = (weft_core::new_run_id(), weft_core::new_run_id());
    let conn = pool.get().await.unwrap();
    let answers = answers_on(
        &conn,
        "w1",
        "w1:0",
        &[part(early, 1, 0, vec![vec![birth(early, Keeping::Fast)]]), part(ended, 1, 0, vec![vec![birth(ended, Keeping::Fast), completed(ended)]])],
    )
    .await;
    assert_eq!(answers.iter().map(|a| (a.fate, a.unwatched)).collect::<Vec<_>>(), [(Fate::Accepted, true), (Fate::Accepted, false)]);

    pool.execute(
        "INSERT INTO worker_lease (replica, project_id, tenant_id, leased_until_unix) \
         VALUES ('w2', $1, $2, EXTRACT(EPOCH FROM NOW())::BIGINT + 60)",
        &[&PROJECT, &TENANT],
    )
    .await
    .unwrap();
    let watched = weft_core::new_run_id();
    let answers = answers_on(&conn, "w2", "w2:0", &[part(watched, 1, 0, vec![vec![birth(watched, Keeping::Fast)]])]).await;
    assert_eq!(answers.iter().map(|a| (a.fate, a.unwatched)).collect::<Vec<_>>(), [(Fate::Accepted, false)]);

    pool.execute("DELETE FROM project", &[]).await.unwrap();
    let orphan = weft_core::new_run_id();
    let answers = answers_on(&conn, "w1", "w1:0", &[part(orphan, 1, 0, vec![vec![birth(orphan, Keeping::Fast)]])]).await;
    assert_eq!(answers.iter().map(|a| (a.fate, a.unwatched)).collect::<Vec<_>>(), [(Fate::Refused, false)]);
}

/// A run's row as the tests read it: state, owner, epoch, last row, and
/// what its cost rows add up to, in micro-dollars.
async fn row(pool: &Pool, run_id: weft_core::RunId) -> (String, Option<String>, i32, i32, i64) {
    pool.one(
        "SELECT state, owner, epoch, last_seq, \
                (SELECT COALESCE(round(sum(c.amount_usd) * 1000000), 0)::bigint FROM run_cost c WHERE c.run_id = run.run_id) \
         FROM run WHERE run_id = $1",
        &[&run_id],
    )
    .await
    .expect("the run's row")
}

async fn rows_of(pool: &Pool, run_id: weft_core::RunId) -> i64 {
    pool.scalar("SELECT COUNT(*) FROM run_log WHERE run_id = $1", &[&run_id]).await.unwrap()
}

/// A run is born by its first batch, owned by its writer; the next batch
/// carries it on from its last row; and a run born and ended in one batch
/// is born ended, its row holding how it ended.
#[weft_db_macros::test]
async fn a_run_is_born_carried_on_and_ended_by_its_batches(pool: Pool) {
    schema(&pool).await;
    let run = weft_core::new_run_id();
    let quick = weft_core::new_run_id();
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
    let outcome: Option<String> = pool.scalar("SELECT outcome FROM run WHERE run_id = $1", &[&quick]).await.unwrap();
    assert_eq!(outcome.as_deref(), Some("completed"));

    let fates = send(&pool, "w1", "w1:0", &[part(run, 1, 1, vec![vec![started(run, "a")], vec![cost(run)]])]).await;
    assert_eq!(fates, [Fate::Accepted]);
    assert_eq!(row(&pool, run).await, ("running".into(), Some("w1".into()), 1, 2, 250_000));
    assert_eq!(send(&pool, "w1", "w1:0", &[part(run, 1, 3, vec![vec![completed(run)]])]).await, [Fate::Accepted]);
    assert_eq!(row(&pool, run).await, ("ended".into(), None, 1, 3, 250_000));
}

/// A run that ends on text holding NUL characters (a node's error built
/// from a scraped page, a cancel whose detail quotes one) ends like any
/// other, and so does every run beside it in the batch: its error is
/// written without them, its cause exactly as it was.
#[weft_db_macros::test]
async fn an_ending_holding_nul_characters_refuses_no_run_of_its_batch(pool: Pool) {
    schema(&pool).await;
    let (failed, cancelled, clean, going) =
        (weft_core::new_run_id(), weft_core::new_run_id(), weft_core::new_run_id(), weft_core::new_run_id());
    let fails = RunEvent::RunFailed { run_id: failed, error: "page\0text".into(), at_unix: 3 };
    let cause = weft_core::run::CancelCause::Runtime { detail: "health said \0 broken".into() };
    let cancels = RunEvent::RunCancelled { run_id: cancelled, reason: cause.to_string(), cause: Some(cause.clone()), at_unix: 3 };
    let fates = send(&pool, "w1", "w1:0", &[part(going, 1, 0, vec![vec![birth(going, Keeping::Fast)]])]).await;
    assert_eq!(fates, [Fate::Accepted]);
    let going_ends = RunEvent::RunFailed { run_id: going, error: "carried\0on".into(), at_unix: 4 };
    let fates = send(
        &pool,
        "w1",
        "w1:0",
        &[
            part(failed, 1, 0, vec![vec![birth(failed, Keeping::Fast), fails]]),
            part(cancelled, 1, 0, vec![vec![birth(cancelled, Keeping::Fast), cancels]]),
            part(clean, 1, 0, vec![vec![birth(clean, Keeping::Fast), completed(clean)]]),
            part(going, 1, 1, vec![vec![going_ends]]),
        ],
    )
    .await;
    assert_eq!(fates, [Fate::Accepted, Fate::Accepted, Fate::Accepted, Fate::Accepted]);
    assert_eq!(ending(&pool, failed).await, ("failed".into(), Some("pagetext".into()), None));
    assert_eq!(ending(&pool, cancelled).await, ("cancelled".into(), None, Some(cause)));
    assert_eq!(ending(&pool, clean).await, ("completed".into(), None, None));
    assert_eq!(ending(&pool, going).await, ("failed".into(), Some("carriedon".into()), None));
}

/// How a run's row says it ended: its outcome, error and cause.
async fn ending(pool: &Pool, run_id: weft_core::RunId) -> (String, Option<String>, Option<weft_core::run::CancelCause>) {
    let (outcome, error, cause): (String, Option<String>, Option<JsonText<weft_core::run::CancelCause>>) = pool
        .one("SELECT outcome, error, cancel_cause FROM run WHERE run_id = $1 AND state = 'ended'", &[&run_id])
        .await
        .expect("the run ended");
    (outcome, error, cause.map(|cause| cause.0))
}

/// A batch sent again after its answer was lost changes nothing and says
/// so: a first batch, a continuing batch carrying a cost (counted once),
/// and an ending.
#[weft_db_macros::test]
async fn a_batch_sent_again_changes_nothing(pool: Pool) {
    schema(&pool).await;
    let run = weft_core::new_run_id();
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
    let counted: i64 = pool.scalar("SELECT SUM(runs)::bigint FROM version_runs", &[]).await.unwrap();
    assert_eq!(counted, 1, "the version counted the run once");
}

/// A cost row as the tests read it: its id, place, node, call path,
/// service, model, amount, whose credential, instance, moment.
type CostRow = (String, i32, i32, String, Vec<String>, String, Option<String>, Option<f64>, String, Option<String>, i64);

async fn cost_rows(pool: &Pool, run_id: weft_core::RunId) -> Vec<CostRow> {
    pool.all(
        "SELECT cost_id, seq, idx, node_id, call_path, service, model, amount_usd, paid_by, paid_by_instance, at_unix \
         FROM run_cost WHERE run_id = $1 ORDER BY seq, idx",
        &[&run_id],
    )
    .await
    .unwrap()
}

/// Each cost a batch carries is a row of its own, with what its event
/// says and its place in the record; written again (a batch resent, or a
/// write that lands the same cost twice) it stays one row; and a run the
/// batch did not take keeps none of the costs it carried.
#[weft_db_macros::test]
async fn a_batch_writes_each_of_its_costs_once(pool: Pool) {
    schema(&pool).await;
    let run = weft_core::new_run_id();
    let ada = weft_core::instance::InstanceId::new("ada").unwrap();
    let priced = |cost_id: &str, amount_usd: Option<f64>, origin: weft_core::CredentialOwner, frames: weft_core::LoopFrames| {
        let mut event = cost(run);
        if let RunEvent::CostReported { cost_id: id, amount_usd: amount, origin: paid_by, frames: at, .. } = &mut event {
            *id = cost_id.into();
            *amount = amount_usd;
            *paid_by = origin;
            *at = frames;
        }
        event
    };
    let sites = vec![
        weft_core::frames::Frame::Call { site: "one".into() },
        weft_core::frames::Frame::Loop { index: 2 },
        weft_core::frames::Frame::Call { site: "One.two".into() },
    ];
    assert_eq!(send(&pool, "w1", "w1:0", &[part(run, 1, 0, vec![vec![birth(run, Keeping::Fast)]])]).await, [Fate::Accepted]);
    let paid = [part(
        run,
        1,
        1,
        vec![vec![
            started(run, "a"),
            priced("c1", Some(0.25), weft_core::CredentialOwner::Platform, sites),
            priced("c2", None, weft_core::CredentialOwner::Instance(ada), Vec::new()),
        ]],
    )];
    assert_eq!(send(&pool, "w1", "w1:0", &paid).await, [Fate::Accepted]);
    let landed = vec![
        (
            "c1".to_string(), 1, 1, "llm".to_string(), vec!["one".to_string(), "One.two".to_string()], "llm".to_string(), None,
            Some(0.25), "platform".to_string(), None, 2,
        ),
        ("c2".to_string(), 1, 2, "llm".to_string(), Vec::new(), "llm".to_string(), None, None, "instance".to_string(), Some("ada".to_string()), 2),
    ];
    assert_eq!(cost_rows(&pool, run).await, landed);

    assert_eq!(send(&pool, "w1", "w1:0", &paid).await, [Fate::AlreadyApplied]);
    let again = weft_journal::record::Written::of(&paid[0].rows[0]).costs;
    weft_journal::costs::insert_in(&pool.get().await.unwrap(), run, 1, &again).await.unwrap();
    assert_eq!(cost_rows(&pool, run).await, landed, "a cost written again stays one row");

    // A batch under a stale epoch is refused, and nothing it carried lands.
    let stale = [part(run, 7, 2, vec![vec![priced("c3", Some(1.0), weft_core::CredentialOwner::Author, Vec::new())]])];
    assert_eq!(send(&pool, "w1", "w1:0", &stale).await, [Fate::Refused]);
    assert_eq!(cost_rows(&pool, run).await.len(), 2, "a refused run's costs go nowhere");
}

/// What `run_secret` keeps of a run's secrets: each id and its sealed text.
async fn secret_rows(pool: &Pool, run_id: weft_core::RunId) -> Vec<(uuid::Uuid, String)> {
    pool.all("SELECT secret_id, sealed FROM run_secret WHERE run_id = $1 ORDER BY secret_id", &[&run_id]).await.unwrap()
}

fn secret_of(value: serde_json::Value) -> KeptSecret {
    KeptSecret { secret_id: weft_core::secret::SecretId::mint(), value }
}

/// A secret a batch carries is kept sealed with its run, under its
/// project: the database holds no plaintext, a worker of the project
/// reveals it, and one of another project finds nothing. Sent again it
/// stays one row; a run the batch did not take keeps none.
#[weft_db_macros::test]
async fn a_batch_keeps_its_runs_secrets_sealed_once(pool: Pool) {
    schema(&pool).await;
    let run = weft_core::new_run_id();
    let token = secret_of(serde_json::json!("Bearer hunter2-\u{0}-tail"));
    let mut first = part(run, 1, 0, vec![vec![birth(run, Keeping::Fast)]]);
    first.secrets.push(token.clone());
    let first = [first];
    assert_eq!(send(&pool, "w1", "w1:0", &first).await, [Fate::Accepted]);
    let rows = secret_rows(&pool, run).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, token.secret_id.as_uuid());
    assert!(!rows[0].1.contains("hunter2"), "the database holds the value sealed");
    let revealed = weft_broker::records::revealed(&pool, PROJECT, token.secret_id).await.unwrap();
    assert_eq!(revealed, Some(token.value.clone()));
    assert_eq!(weft_broker::records::revealed(&pool, uuid::Uuid::from_u128(9), token.secret_id).await.unwrap(), None);

    assert_eq!(send(&pool, "w1", "w1:0", &first).await, [Fate::AlreadyApplied]);
    assert_eq!(secret_rows(&pool, run).await, rows, "a batch sent again keeps its secret once");

    let mut stale = part(run, 7, 1, vec![vec![started(run, "a")]]);
    stale.secrets.push(secret_of(serde_json::json!("never kept")));
    assert_eq!(send(&pool, "w1", "w1:0", &[stale]).await, [Fate::Refused]);
    assert_eq!(secret_rows(&pool, run).await, rows, "a refused run keeps none of what its batch carried");
}

/// A run's secrets go with its record: once it is deleted, revealing one
/// finds nothing, which a worker reports naming the secret.
#[weft_db_macros::test]
async fn a_runs_secrets_go_with_its_record(pool: Pool) {
    schema(&pool).await;
    let run = weft_core::new_run_id();
    let token = secret_of(serde_json::json!("short-lived"));
    let mut first = part(run, 1, 0, vec![vec![birth(run, Keeping::Fast), completed(run)]]);
    first.secrets.push(token.clone());
    assert_eq!(send(&pool, "w1", "w1:0", &[first]).await, [Fate::Accepted]);
    let store = weft_dispatcher::store::postgres::PostgresStore::from_pool(pool.clone());
    store.delete_run(run).await.unwrap();
    assert!(secret_rows(&pool, run).await.is_empty());
    assert_eq!(weft_broker::records::revealed(&pool, PROJECT, token.secret_id).await.unwrap(), None);
}

/// Two workers that took one fire both bear its run: the first is the
/// run's, and the second hears its run was born elsewhere, with nothing of
/// its own written.
#[weft_db_macros::test]
async fn a_run_two_workers_bore_is_the_first_ones(pool: Pool) {
    schema(&pool).await;
    let run = weft_core::new_run_id();
    assert_eq!(send(&pool, "w1", "w1:0", &[part(run, 1, 0, vec![vec![birth(run, Keeping::Fast)]])]).await, [Fate::Accepted]);
    let other = [part(run, 1, 0, vec![vec![birth(run, Keeping::Fast), started(run, "b")]])];
    assert_eq!(send(&pool, "w2", "w2:0", &other).await, [Fate::BornElsewhere]);
    assert_eq!(row(&pool, run).await.1.as_deref(), Some("w1"));
    assert_eq!(rows_of(&pool, run).await, 1);
}

/// Rows that do not follow on from the run's record are refused: a gap in
/// `first_seq`, another writer, an epoch the run has moved past. Nothing
/// goes in.
#[weft_db_macros::test]
async fn a_batch_that_does_not_follow_on_is_refused(pool: Pool) {
    schema(&pool).await;
    let run = weft_core::new_run_id();
    send(&pool, "w1", "w1:0", &[part(run, 1, 0, vec![vec![birth(run, Keeping::Fast)]])]).await;
    assert_eq!(send(&pool, "w1", "w1:0", &[part(run, 1, 2, vec![vec![started(run, "gap")]])]).await, [Fate::Refused], "a gap");
    assert_eq!(send(&pool, "w2", "w2:0", &[part(run, 1, 1, vec![vec![started(run, "x")]])]).await, [Fate::Refused], "not its writer");
    assert_eq!(send(&pool, "w1", "w1:0", &[part(run, 0, 1, vec![vec![started(run, "old")]])]).await, [Fate::Refused], "an old epoch");
    assert_eq!(rows_of(&pool, run).await, 1);
    assert_eq!(row(&pool, run).await.3, 0);
}

/// Two lanes of one worker count runs of the same version on rows of
/// their own: one lane's batch still open never holds the other's up.
#[weft_db_macros::test]
async fn two_lanes_counting_one_version_never_block_each_other(pool: Pool) {
    schema(&pool).await;
    let (a, b) = (weft_core::new_run_id(), weft_core::new_run_id());
    let mut held = pool.get().await.unwrap();
    let open = held.transaction().await.unwrap();
    assert_eq!(send_on(&open, "w1", "w1:0", &[part(a, 1, 0, vec![vec![birth(a, Keeping::Fast)]])]).await, [Fate::Accepted]);
    let other = tokio::time::timeout(
        Duration::from_secs(5),
        send(&pool, "w1", "w1:1", &[part(b, 1, 0, vec![vec![birth(b, Keeping::Fast)]])]),
    )
    .await
    .expect("the second lane's batch went in while the first one's was still open");
    assert_eq!(other, [Fate::Accepted]);
    open.commit().await.unwrap();
    let counted: i64 = pool.scalar("SELECT SUM(runs)::bigint FROM version_runs WHERE source_version = 'v1'", &[]).await.unwrap();
    assert_eq!(counted, 2);
}

/// Claim `run` for `owner` on a lapsed lease, the way a worker that then
/// died had it.
async fn driven_by_a_dead_worker(pool: &Pool, run: weft_core::RunId) {
    pool.execute(
        "INSERT INTO worker_lease (replica, project_id, tenant_id, leased_until_unix) VALUES ('w1', $1, $2, 0) \
         ON CONFLICT (replica) DO NOTHING",
        &[&PROJECT, &TENANT],
    )
    .await
    .unwrap();
    assert_eq!(send(pool, "w1", "w1:0", &[part(run, 1, 0, vec![vec![birth(run, Keeping::Durable)]])]).await, [Fate::Accepted]);
}

/// The sweep letting go of a lost run raises its epoch first: a late batch
/// of the worker it lost is refused, and the run is queued for the next.
#[weft_db_macros::test]
async fn a_late_batch_after_the_lost_sweep_is_refused(pool: Pool) {
    schema(&pool).await;
    let store = weft_dispatcher::store::postgres::PostgresStore::from_pool(pool.clone());
    let run = weft_core::new_run_id();
    driven_by_a_dead_worker(&pool, run).await;
    assert_eq!(store.let_go_of_lost(run, i64::MAX, None).await.unwrap(), Lost::Requeued);
    assert_eq!(send(&pool, "w1", "w1:0", &[part(run, 1, 1, vec![vec![started(run, "late")]])]).await, [Fate::Refused]);
    assert_eq!(row(&pool, run).await, ("queued".into(), None, 2, 0, 0));
}

/// The memory note of a worker that went away full is read by the worker
/// that claims the run it lost, with its claim, and by a claim given again,
/// though the lost worker's lease row was pruned before the claim (the
/// sweep put the note on the run), and though the worker crossed before
/// its lease row was written: the tick that wrote the row carried it. A
/// run its worker handed back, or one whose worker had room again when it
/// went away, reads none.
#[weft_db_macros::test]
async fn the_worker_that_carries_on_a_lost_run_reads_its_last_workers_memory_note(pool: Pool) {
    use weft_task_store::worker_door::{keep_memory_state, MemoryNote};
    schema(&pool).await;
    let store = weft_dispatcher::store::postgres::PostgresStore::from_pool(pool.clone());
    let run = weft_core::new_run_id();
    let conn = pool.get().await.unwrap();
    let claim = |owner: &'static str| {
        let conn = &conn;
        async move { weft_journal::record::claim(conn, run, PROJECT, owner).await.unwrap().expect("claimed").lost_full }
    };
    driven_by_a_dead_worker(&pool, run).await;
    let note = MemoryNote { used_bytes: 990, limit_bytes: 1000, at_unix: 12 };
    keep_memory_state(&pool, "w1", PROJECT, Some(&note)).await.unwrap();
    keep_memory_state(&pool, "nobody", PROJECT, Some(&note)).await.unwrap();
    let kept: i64 = pool.scalar("SELECT COUNT(*) FROM worker_lease", &[]).await.unwrap();
    assert_eq!(kept, 1, "a worker with no lease row keeps its note nowhere");
    assert_eq!(store.let_go_of_lost(run, i64::MAX, None).await.unwrap(), Lost::Requeued);
    pool.execute("DELETE FROM worker_lease WHERE replica = 'w1'", &[]).await.unwrap();
    assert_eq!(claim("w2").await, Some(note), "the note is on the run, whatever became of w1's lease row");
    assert_eq!(claim("w2").await, Some(note), "a claim given again reads it again");

    // w2 hands it back: it did not go away, so the next claim reads no
    // note.
    let w2 = weft_broker::door::DoorWorker { tenant: TENANT, project: PROJECT, replica: "w2" };
    assert!(weft_broker::door::let_go(&pool, w2, run, weft_broker_client::protocol::LetGo::HandedBack).await.unwrap());
    assert_eq!(claim("w3").await, None);

    // w3 was full for a while, had room again, then went away.
    pool.execute(
        "INSERT INTO worker_lease (replica, project_id, tenant_id, leased_until_unix) VALUES ('w3', $1, $2, 0)",
        &[&PROJECT, &TENANT],
    )
    .await
    .unwrap();
    keep_memory_state(&pool, "w3", PROJECT, Some(&note)).await.unwrap();
    keep_memory_state(&pool, "w3", PROJECT, None).await.unwrap();
    assert_eq!(store.let_go_of_lost(run, i64::MAX, None).await.unwrap(), Lost::Requeued);
    assert_eq!(claim("w4").await, None);

    // w4 crossed before its first tick wrote its lease row: told at once,
    // the note lands nowhere, and the tick that writes the row carries it.
    keep_memory_state(&pool, "w4", PROJECT, Some(&note)).await.unwrap();
    let rows: i64 = pool.scalar("SELECT COUNT(*) FROM worker_lease WHERE replica = 'w4'", &[]).await.unwrap();
    assert_eq!(rows, 0);
    let w4 = weft_broker::door::DoorWorker { tenant: TENANT, project: PROJECT, replica: "w4" };
    let tick = weft_broker_client::protocol::DoorTickRequest {
        binary_hash: Some("bin-1".into()),
        in_flight: Default::default(),
        window_start: weft_core::signal::limits::window(chrono::Utc::now().timestamp()).0,
        counts: Vec::new(),
        vouch_until: None,
        shortens: false,
        memory_full: Some(note),
    };
    weft_broker::door::tick(&pool, w4, &tick).await.unwrap();
    assert_eq!(store.let_go_of_lost(run, i64::MAX, None).await.unwrap(), Lost::Requeued);
    assert_eq!(claim("w5").await, Some(note));
}

/// A late batch that holds the run's row when the sweep comes goes in
/// first, and the sweep then lets go of the run from where the batch left
/// it, without either waiting on the other for ever.
#[weft_db_macros::test]
async fn a_late_batch_before_the_lost_sweep_goes_in_and_the_sweep_follows(pool: Pool) {
    schema(&pool).await;
    let store = weft_dispatcher::store::postgres::PostgresStore::from_pool(pool.clone());
    let run = weft_core::new_run_id();
    driven_by_a_dead_worker(&pool, run).await;
    let mut held = pool.get().await.unwrap();
    let open = held.transaction().await.unwrap();
    assert_eq!(send_on(&open, "w1", "w1:0", &[part(run, 1, 1, vec![vec![started(run, "late")]])]).await, [Fate::Accepted]);
    let sweep = tokio::spawn(async move { store.let_go_of_lost(run, i64::MAX, None).await.unwrap() });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!sweep.is_finished(), "the sweep waits for the batch holding the run's row");
    open.commit().await.unwrap();
    let lost = tokio::time::timeout(Duration::from_secs(5), sweep).await.expect("no deadlock").unwrap();
    assert_eq!(lost, Lost::Requeued);
    assert_eq!(row(&pool, run).await, ("queued".into(), None, 2, 1, 0), "let go of from the batch's last row");
}

/// A run whose worker could not write its record, given up with a charge
/// its record leaves open, ends with that charge booked unknown: one
/// `CostReported` closing it, before the ending, in the same write.
#[weft_db_macros::test]
async fn a_given_up_run_books_its_open_charge_unknown(pool: Pool) {
    schema(&pool).await;
    let run = weft_core::new_run_id();
    let opened = RunEvent::ChargeOpened {
        run_id: run,
        node_id: "dub".into(),
        frames: Vec::new(),
        service: "jobs".into(),
        charge: "job-1".into(),
        scratch: serde_json::json!({ "model": "m" }),
        origin: weft_core::CredentialOwner::Platform,
        at_unix: 2,
    };
    assert_eq!(send(&pool, "w1", "w1:0", &[part(run, 1, 0, vec![vec![birth(run, Keeping::Durable)], vec![opened]])]).await, [Fate::Accepted]);

    let mut conn = pool.get().await.unwrap();
    let tx = conn.transaction().await.unwrap();
    let gave_up = weft_journal::record::give_up_in(&tx, run, "w1", "a write failed").await.unwrap();
    tx.commit().await.unwrap();
    drop(conn);
    assert_eq!(gave_up, weft_journal::record::GaveUp::Ended(2));

    let events = weft_journal::record::read_page(&pool, run, None).await.unwrap().events(run).unwrap();
    let costs: Vec<&RunEvent> = events.iter().filter(|event| matches!(event, RunEvent::CostReported { .. })).collect();
    assert_eq!(costs.len(), 1, "{events:?}");
    assert!(
        matches!(costs[0], RunEvent::CostReported { node_id, service, charge: Some(charge), amount_usd: None, model: Some(model), .. }
            if node_id == "dub" && service == "jobs" && charge == "job-1" && model == "m"),
        "{:?}", costs[0]
    );
    assert!(matches!(&events[events.len() - 2..], [RunEvent::CostReported { .. }, RunEvent::RunFailed { .. }]), "{events:?}");
    assert_eq!(weft_journal::ChargeBook::of_record(&events).into_open().count(), 0, "the record holds no open charge");
    assert_eq!(row(&pool, run).await.0, "ended");
}

/// A run's record reads back a page at a time: at most
/// `weft_journal::record::PAGE_ROWS` rows, cut sooner once the rows weigh
/// `PAGE_BYTES` (a row heavier than that alone), always one row while any
/// is left, from past any row, empty once none is; its first row alone;
/// any of its rows by place.
#[weft_db_macros::test]
async fn a_record_reads_back_a_page_at_a_time(pool: Pool) {
    use weft_journal::record::{read_first_row, read_page, read_rows, PAGE_BYTES, PAGE_ROWS};
    schema(&pool).await;
    let empty = weft_core::new_run_id();
    assert!(read_page(&pool, empty, None).await.unwrap().rows.is_empty(), "a run with no record has no page");
    assert!(read_first_row(&pool, empty).await.unwrap().is_none());

    let run = weft_core::new_run_id();
    let light = PAGE_ROWS as usize + 10;
    let mut rows = vec![vec![birth(run, Keeping::Fast)]];
    rows.extend((1..light).map(|_| vec![started(run, "a")]));
    assert_eq!(send(&pool, "w1", "w1:0", &[part(run, 1, 0, rows)]).await, [Fate::Accepted]);
    let seqs = |page: &weft_journal::record::RawRecord| page.rows.iter().map(|row| row.seq).collect::<Vec<_>>();
    let first = read_page(&pool, run, None).await.unwrap();
    assert_eq!(seqs(&first), (0..PAGE_ROWS as i32).collect::<Vec<_>>(), "a full page of light rows");
    assert!(matches!(first.events(run).unwrap()[0], RunEvent::RunStarted { .. }));
    let next = read_page(&pool, run, Some(PAGE_ROWS as i32 - 1)).await.unwrap();
    assert_eq!(seqs(&next), (PAGE_ROWS as i32..light as i32).collect::<Vec<_>>(), "the rest past the last row read");
    assert!(read_page(&pool, run, Some(light as i32 - 1)).await.unwrap().rows.is_empty(), "nothing past the last row");
    assert_eq!(seqs(&read_page(&pool, run, Some(100)).await.unwrap())[0], 101, "from past any row");
    assert_eq!(read_first_row(&pool, run).await.unwrap().map(|row| row.seq), Some(0));
    assert_eq!(seqs(&read_rows(&pool, run, &[3, 7, 100_000]).await.unwrap()), [3, 7], "the rows asked for that exist");

    // Rows that barely compress, near half a page's weight each once
    // stored: a page holds what fits, at least one row.
    let heavy = weft_core::new_run_id();
    let noise = |n: u64| -> String {
        const SYMBOLS: &[u8; 64] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_";
        let mut state = n.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        (0..PAGE_BYTES as usize / 2).map(|_| {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            char::from(SYMBOLS[(state >> 58) as usize])
        }).collect()
    };
    let log = |n: u64| RunEvent::LogLine {
        run_id: heavy, node_id: "a".into(), frames: vec![], level: "info".into(), message: noise(n), at_unix: 2, at_unix_ms: None, seq: None,
    };
    let rows = vec![vec![birth(heavy, Keeping::Fast)], vec![log(1)], vec![log(2)], vec![log(3)], vec![log(4)]];
    assert_eq!(send(&pool, "w1", "w1:0", &[part(heavy, 1, 0, rows)]).await, [Fate::Accepted]);
    let mut pages = Vec::new();
    let mut after = None;
    loop {
        let page = read_page(&pool, heavy, after).await.unwrap();
        let Some(last) = page.rows.last().map(|row| row.seq) else { break };
        pages.push(seqs(&page));
        after = Some(last);
    }
    assert_eq!(pages.concat(), [0, 1, 2, 3, 4], "every row once, in order: {pages:?}");
    assert!(pages.len() >= 2, "the weight cut the record into pages: {pages:?}");
    assert!(pages.iter().all(|page| !page.is_empty()));
}
