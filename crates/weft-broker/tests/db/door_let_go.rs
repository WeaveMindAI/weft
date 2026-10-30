//! Layer-3 contract tests for a worker letting go of a run
//! (`weft_broker::door::let_go`), against a REAL Postgres: the run is
//! parked on its wait or queued for another worker, with no owner, only
//! while the asking worker drives it; and an answer handed to it while it
//! ran goes on its record as it is let go of.

use weft_task_store::db::{self, Client as _, Pool, Session};
use weft_broker::door::{let_go, DoorWorker};
use weft_broker_client::protocol::LetGo;
use weft_dispatcher::store::RunStore;
use weft_journal::RunEvent;

const PROJECT: uuid::Uuid = uuid::Uuid::from_u128(1);
const TENANT: &str = "t1";

/// The schema, and the project the runs belong to.
async fn schema(pool: &Pool) {
    weft_task_store::apply_groups(pool, weft_dispatcher::app::DISPATCHER_GROUPS).await.expect("core schema");
    pool.execute(
        "INSERT INTO project (id, name, tenant_id, status, project_json, updated_at, project_json_version, resync_version, running_build_inputs_version) \
         VALUES ($1, 'p', $2, 'registered', '{}', 0, $3, 1, 1)",
        &[&PROJECT, &TENANT, &weft_core::ProjectDefinition::STORED.version()],
    )
    .await
    .expect("project row");
}

fn worker(replica: &str) -> DoorWorker<'_> {
    DoorWorker { tenant: TENANT, project: PROJECT, replica }
}

/// A run driven by `owner`: queued, then claimed by it.
async fn driven_by(pool: &Pool, owner: &str) -> weft_core::RunId {
    let run_id = weft_core::new_run_id();
    let birth = RunEvent::RunStarted { given: Default::default(),
        run_id,
        project_id: PROJECT,
        entry_node: "route".into(),
        phase: weft_core::context::Phase::Fire,
        definition_hash: Some("def-1".into()),
        binary_hash: Some("bin-1".into()),
        source_version: None,
        run_kind: weft_core::run::RunKind::Project,
        cut_from: None,
        seed: None,
        instance: None,
        stand_in: None,
        fired_trigger: None,
        instance_values: Default::default(),
        picks: Default::default(),
        settings: Default::default(),
        at_unix: 0,
    };
    let store = weft_dispatcher::store::postgres::PostgresStore::from_pool(pool.clone());
    let queued = weft_journal::record::Queued {
        events: std::slice::from_ref(&birth),
        tenant: TENANT,
        keep_for: weft_core::run_settings::KeepFor::WEFT_DEFAULT,
        watch_end: false,
        stale: &[],
        spec: None,
        example: None,
        program: None,
    };
    assert!(store.queue_run(queued, false).await.expect("queue the run"));
    let conn = pool.get().await.unwrap();
    weft_journal::record::claim(&conn, run_id, PROJECT, owner).await.unwrap().expect("claimed");
    run_id
}

/// The run's state and owner.
async fn standing(pool: &Pool, run_id: weft_core::RunId) -> (String, Option<String>) {
    pool.one("SELECT state, owner FROM run WHERE run_id = $1", &[&run_id]).await.expect("the run's row")
}

#[weft_db_macros::test]
async fn a_run_handed_back_is_queued_for_another_worker(pool: Pool) {
    schema(&pool).await;
    let run_id = driven_by(&pool, "w1").await;
    assert!(let_go(&pool, worker("w1"), run_id, LetGo::HandedBack).await.unwrap());
    assert_eq!(standing(&pool, run_id).await, ("queued".into(), None));
}

#[weft_db_macros::test]
async fn only_its_owner_lets_go_of_a_run(pool: Pool) {
    schema(&pool).await;
    let run_id = driven_by(&pool, "w1").await;
    assert!(!let_go(&pool, worker("w2"), run_id, LetGo::HandedBack).await.unwrap());
    assert!(!let_go(&pool, worker("w2"), run_id, LetGo::Parked).await.unwrap());
    assert_eq!(standing(&pool, run_id).await, ("running".into(), Some("w1".into())));
}

#[weft_db_macros::test]
async fn a_run_parked_waits_with_no_owner(pool: Pool) {
    schema(&pool).await;
    let run_id = driven_by(&pool, "w1").await;
    assert!(let_go(&pool, worker("w1"), run_id, LetGo::Parked).await.unwrap());
    assert_eq!(standing(&pool, run_id).await, ("parked".into(), None));
}

/// A run that parks is announced to every dispatcher, for whoever follows
/// it (`weft wait --waiting-for-input`); a run handed back is not parked,
/// nor one that parks with an answer already handed to it (it is queued
/// to carry on at once), and neither says anything of the kind.
#[weft_db_macros::test]
async fn a_run_that_parks_is_announced(pool: Pool) {
    schema(&pool).await;
    let mut heard = Session::connect(pool.config()).await.unwrap();
    heard.listen([weft_task_store::runs::EVENTS_CHANNEL]).await.unwrap();
    let handed = driven_by(&pool, "w1").await;
    assert!(let_go(&pool, worker("w1"), handed, LetGo::HandedBack).await.unwrap());
    let answered = driven_by(&pool, "w1").await;
    let mut conn = pool.get().await.unwrap();
    let tx = conn.transaction().await.unwrap();
    weft_task_store::parked_fires::hand_answer_in(&tx, "form", answered, &weft_core::primitive::WaitAnswer::Given { value: serde_json::json!("yes") })
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert!(let_go(&pool, worker("w1"), answered, LetGo::Parked).await.unwrap());
    assert_eq!(standing(&pool, answered).await, ("queued".into(), None), "it carries on with its answer");
    let parked = driven_by(&pool, "w1").await;
    assert!(let_go(&pool, worker("w1"), parked, LetGo::Parked).await.unwrap());
    let told = tokio::time::timeout(std::time::Duration::from_secs(10), heard.notified())
        .await
        .expect("announced")
        .unwrap()
        .expect("the session is open");
    let event: weft_core::live_event::LiveEvent = serde_json::from_str(told.payload()).unwrap();
    assert!(
        matches!(event.event, weft_core::live_event::DispatcherEvent::RunParked { run_id, project_id } if run_id == parked && project_id == PROJECT),
        "{event:?}"
    );
    assert!(tokio::time::timeout(std::time::Duration::from_millis(300), heard.notified()).await.is_err(), "one announcement, the park's");
}

/// A run asked to stop while its worker drove it, after the drive had
/// already decided to park, is queued rather than parked: the cancel took
/// its wake signals, so parked it would wait for good with the ask unread.
/// The worker that claims it next reads the cancel with its claim.
#[weft_db_macros::test]
async fn a_run_asked_to_stop_is_queued_when_its_worker_parks_it(pool: Pool) {
    schema(&pool).await;
    let mut heard = Session::connect(pool.config()).await.unwrap();
    heard.listen([weft_task_store::runs::RUN_QUEUED_CHANNEL]).await.unwrap();
    let run_id = driven_by(&pool, "w1").await;
    let cause = weft_core::run::CancelCause::Run { by: weft_core::new_run_id(), tag: Some("user_7".into()) };
    pool.execute("UPDATE run SET cancel_requested = $2 WHERE run_id = $1", &[&run_id, &db::JsonText(&cause)]).await.unwrap();
    // The birth's own queueing was announced before the listener could
    // have missed it; drain it so only the let-go's is left to hear.
    while tokio::time::timeout(std::time::Duration::from_millis(200), heard.notified()).await.is_ok() {}
    assert!(let_go(&pool, worker("w1"), run_id, LetGo::Parked).await.unwrap());
    assert_eq!(standing(&pool, run_id).await, ("queued".into(), None));
    tokio::time::timeout(std::time::Duration::from_secs(10), heard.notified())
        .await
        .expect("its queueing is announced")
        .unwrap()
        .expect("the session is open");
    let conn = pool.get().await.unwrap();
    let claimed = weft_journal::record::claim(&conn, run_id, PROJECT, "w1").await.unwrap().expect("claimed again");
    assert_eq!(claimed.cancel_requested, Some(cause), "the claim carries the cancel");
}

/// A run that ended is nobody's to let go of.
#[weft_db_macros::test]
async fn a_run_that_ended_meanwhile_is_left_alone(pool: Pool) {
    schema(&pool).await;
    let run_id = driven_by(&pool, "w1").await;
    pool.execute("UPDATE run SET state = 'ended', owner = NULL, outcome = 'cancelled', ended_at = 1 WHERE run_id = $1", &[&run_id])
        .await
        .unwrap();
    assert!(!let_go(&pool, worker("w1"), run_id, LetGo::HandedBack).await.unwrap());
    assert_eq!(standing(&pool, run_id).await.0, "ended");
}

/// A letting go asked again after its answer was lost finds the run let go
/// of already, and answers as before.
#[weft_db_macros::test]
async fn a_let_go_asked_again_is_answered_the_same(pool: Pool) {
    schema(&pool).await;
    let run_id = driven_by(&pool, "w1").await;
    assert!(let_go(&pool, worker("w1"), run_id, LetGo::HandedBack).await.unwrap());
    assert!(let_go(&pool, worker("w1"), run_id, LetGo::HandedBack).await.unwrap());
    assert!(!let_go(&pool, worker("w1"), run_id, LetGo::Parked).await.unwrap(), "it was queued, not parked");
}

/// An answer handed to the run while its worker drove it, and not taken
/// before it parked, goes on its record as it is let go of, and the run is
/// queued to carry on with it.
#[weft_db_macros::test]
async fn an_answer_handed_to_a_run_that_parks_reaches_its_record(pool: Pool) {
    schema(&pool).await;
    let run_id = driven_by(&pool, "w1").await;
    let mut conn = pool.get().await.unwrap();
    let tx = conn.transaction().await.unwrap();
    weft_task_store::parked_fires::hand_answer_in(&tx, "form", run_id, &weft_core::primitive::WaitAnswer::Given { value: serde_json::json!("yes") }).await.unwrap();
    tx.commit().await.unwrap();
    assert!(let_go(&pool, worker("w1"), run_id, LetGo::Parked).await.unwrap());
    assert_eq!(standing(&pool, run_id).await, ("queued".into(), None));
    let store = weft_dispatcher::store::postgres::PostgresStore::from_pool(pool.clone());
    let events = store.events_log(run_id).await.unwrap();
    assert!(
        matches!(events.last(), Some(RunEvent::SuspensionResolved { token, .. }) if token == "form"),
        "{events:?}"
    );
    assert!(weft_task_store::parked_fires::answers_for(&pool, run_id).await.unwrap().is_empty(), "taken once");
}

/// An answer holding NUL characters (a person pasted a scraped page) is
/// handed to its run and taken exactly as it was given.
#[weft_db_macros::test]
async fn an_answer_holding_nul_reaches_its_run_exactly(pool: Pool) {
    schema(&pool).await;
    let run_id = driven_by(&pool, "w1").await;
    let answer = weft_core::primitive::WaitAnswer::Given { value: serde_json::json!({ "pasted": "a\u{0}b" }) };
    let mut conn = pool.get().await.unwrap();
    let tx = conn.transaction().await.unwrap();
    weft_task_store::parked_fires::hand_answer_in(&tx, "form", run_id, &answer).await.unwrap();
    tx.commit().await.unwrap();
    assert_eq!(weft_task_store::parked_fires::answers_for(&pool, run_id).await.unwrap(), vec![("form".to_string(), answer)]);
}

/// After its line may have missed some, a worker reads the cancels and the
/// answers of every run it drives in one call: its own runs only, answers
/// oldest first.
#[weft_db_macros::test]
async fn one_recheck_reads_the_cancels_and_answers_of_every_run_a_worker_drives(pool: Pool) {
    schema(&pool).await;
    let cancelled = driven_by(&pool, "w1").await;
    let answered = driven_by(&pool, "w1").await;
    let elsewhere = driven_by(&pool, "w2").await;
    pool.execute("UPDATE run SET cancel_requested = $2 WHERE run_id = $1", &[&cancelled, &db::JsonText(weft_core::run::CancelCause::User)])
        .await
        .unwrap();
    let mut conn = pool.get().await.unwrap();
    let tx = conn.transaction().await.unwrap();
    for (token, run) in [("first", answered), ("second", answered), ("theirs", elsewhere)] {
        let answer = weft_core::primitive::WaitAnswer::Given { value: serde_json::json!(token) };
        weft_task_store::parked_fires::hand_answer_in(&tx, token, run, &answer).await.unwrap();
    }
    tx.commit().await.unwrap();

    let rechecked = weft_broker::records::rechecked(&pool, "w1", PROJECT).await.unwrap();
    assert_eq!(rechecked.cancels.len(), 1);
    assert_eq!((rechecked.cancels[0].run_id, &rechecked.cancels[0].cause), (cancelled, &weft_core::run::CancelCause::User));
    let answers: Vec<_> = rechecked.answers.iter().map(|a| (a.run_id, a.token.as_str())).collect();
    assert_eq!(answers, vec![(answered, "first"), (answered, "second")], "its own, oldest first; nothing of w2's");
}

/// A worker of the previous release, once its line came back, reads the
/// cancels of the runs it drives from `/v1/run/cancels`: its own runs only,
/// in the shape that release reads (`{"cancels": [...]}`, nothing else).
#[weft_db_macros::test]
async fn a_previous_release_worker_still_reads_its_cancels(pool: Pool) {
    schema(&pool).await;
    let cancelled = driven_by(&pool, "w1").await;
    driven_by(&pool, "w1").await;
    let elsewhere = driven_by(&pool, "w2").await;
    for run in [cancelled, elsewhere] {
        pool.execute("UPDATE run SET cancel_requested = $2 WHERE run_id = $1", &[&run, &db::JsonText(weft_core::run::CancelCause::User)])
            .await
            .unwrap();
    }

    let cancels = weft_broker::records::previous_release_cancels(&pool, "w1", PROJECT).await.unwrap();
    let wire = serde_json::to_value(&cancels).unwrap();
    let expected = vec![weft_broker_client::protocol::RunCancel { run_id: cancelled, cause: weft_core::run::CancelCause::User }];
    assert_eq!(wire, serde_json::json!({ "cancels": serde_json::to_value(&expected).unwrap() }));
}
