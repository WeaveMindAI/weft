//! Layer-3 tests for run tags against a REAL Postgres: the
//! `run_tag` rows the broker writes on a worker's behalf, the
//! live-tagged read the dispatcher's `stop_tagged` task selects on, and
//! the cancel terminals a tag stop writes. The selection RULE itself is
//! pure (`weft_journal::tags::select_stop_targets`, layer-1 tested in
//! its own crate); what this rig proves is the SQL underneath it: the
//! BIGSERIAL order, the (run, tag) idempotency, the live filter, the
//! project wall, the row's life ending with the journal's, and the
//! one-transaction cancel (`RunStore::cancel_run`) that a tag
//! stop and the stop button both end in.
//!
//! Same rig as `db_lifecycle.rs`: `#[weft_db_macros::test]` hands each test a fresh
//! database, and the real boot-time migration path builds the schema.

use serde_json::json;
use uuid::Uuid;

use weft_core::run::CancelCause;
use weft_core::{ProjectDefinition, StopSelf};
use weft_dispatcher::store::postgres::PostgresStore;
use weft_dispatcher::store::{RunStore, SignalRegistration, SignalStore, TokenStore};
use weft_journal::tags::{live_tagged_runs, max_tag_seq, select_stop_targets, tag_run_in, tag_seq};
use weft_journal::record::{Queued, Then};
use weft_journal::RunEvent;
use weft_task_store::db::{Client as _, Pool};

const TENANT: &str = "tenant-1";

async fn setup(pool: &Pool) -> (PostgresStore, weft_dispatcher::ProjectStore) {
    weft_task_store::apply_groups(pool, weft_dispatcher::app::DISPATCHER_GROUPS).await.expect("core schema");
    let store = PostgresStore::from_pool(pool.clone());
    let projects: weft_dispatcher::ProjectStore =
        std::sync::Arc::new(weft_dispatcher::PostgresProjectStore::new(pool.clone()));
    (store, projects)
}

/// The program every run here runs: one node, `wait`, the one a
/// parked run is found on. The cancel's per-node terminals come off a
/// fold over this program, so the node has to exist in it.
fn rig_project(id: Uuid) -> ProjectDefinition {
    serde_json::from_value(json!({
        "id": id,
        "nodes": [{
            "id": "wait", "nodeType": "Wait", "label": null, "config": null,
            "position": { "x": 0.0, "y": 0.0 }, "inputs": [], "outputs": [],
            "features": {}, "scope": [], "groupBoundary": null, "requiresInfra": false, "images": []
        }],
        "edges": []
    }))
    .expect("rig ProjectDefinition")
}

async fn seed_project(projects: &weft_dispatcher::ProjectStore, id: Uuid) {
    projects
        .register_with_hashes(rig_project(id), "db-rig", "", TENANT, Some("bin-A"), Some("def-1"), None, None, None, None, None, None)
        .await
        .expect("register project");
}

/// Queue a fresh run of `project`, the way the dispatcher starts one.
/// `run_kind` starts another kind instead (a node test runs no program),
/// which the live read must never select no matter what it carries.
async fn start_run(
    store: &PostgresStore,
    project: Uuid,
    run_kind: weft_core::run::RunKind,
) -> weft_core::RunId {
    let run_id = weft_core::new_run_id();
    let birth = RunEvent::RunStarted { given: Default::default(),
        run_id,
        project_id: project,
        entry_node: "start".into(),
        phase: weft_core::context::Phase::Fire,
        definition_hash: run_kind.is_project().then(|| "def-1".into()),
        binary_hash: run_kind.is_project().then(|| "bin-A".into()),
        run_kind,
        source_version: None,
        cut_from: None,
        seed: None,
        instance: None, stand_in: None, fired_trigger: None, instance_values: Default::default(), picks: Default::default(), at_unix: 1,
        settings: Default::default(),
    };
    let queued = Queued {
        events: std::slice::from_ref(&birth), tenant: TENANT, keep_for: weft_core::run_settings::KeepFor::WEFT_DEFAULT,
        watch_end: false, stale: &[], spec: None, example: None, program: None,
    };
    assert!(store.queue_run(queued, false).await.expect("queue the run"));
    run_id
}

/// Write `events` into the record of a run nobody drives.
async fn write(store: &PostgresStore, run_id: weft_core::RunId, events: &[RunEvent]) {
    let written = store.append(run_id, events, Then::Stays).await.expect("write the run's record");
    assert!(matches!(written, weft_journal::record::Appended::At(_)), "{written:?}");
}

/// Mark run `run_id` parked on a wait, as its worker's letting go
/// leaves it.
async fn park_run(pool: &Pool, run_id: weft_core::RunId) {
    pool.execute("UPDATE run SET state = 'parked' WHERE run_id = $1", &[&run_id]).await.unwrap();
}

/// The broker's write, as `/v1/run/tag` performs it: the rows, one
/// transaction (the run's worker records the act in its own record).
async fn tag(pool: &Pool, run_id: weft_core::RunId, tags: &[&str], at: u64) {
    let tags: Vec<String> = tags.iter().map(|t| t.to_string()).collect();
    let mut conn = pool.get().await.unwrap();
    let tx = conn.transaction().await.unwrap();
    tag_run_in(&tx, run_id, &tags, at).await.expect("tag_run_in");
    tx.commit().await.unwrap();
}

/// The order tags are written is the order the rule compares, and a
/// re-tag keeps its place.
#[weft_db_macros::test]
async fn tag_rows_are_ordered_by_write_and_idempotent(pool: Pool) {
    let (store, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project).await;
    let first = start_run(&store, project, weft_core::run::RunKind::Project).await;
    let second = start_run(&store, project, weft_core::run::RunKind::Project).await;

    tag(&pool, first, &["user_7"], 10).await;
    tag(&pool, second, &["user_7", "batch_a"], 11).await;
    // Re-tagging the first run (a body replayed after a durable wait) must not
    // move it after the second in the order.
    tag(&pool, first, &["user_7"], 12).await;

    let s1 = tag_seq(&pool, first, "user_7").await.unwrap().expect("first is tagged");
    let s2 = tag_seq(&pool, second, "user_7").await.unwrap().expect("second is tagged");
    assert!(s1 < s2, "first tagged first: {s1} < {s2}");
    assert_eq!(tag_seq(&pool, first, "batch_a").await.unwrap(), None);
    assert!(max_tag_seq(&pool).await.unwrap() >= s2);

    // The summaries carry the tags in claim order.
    let summary = store.run_summary(second).await.unwrap().expect("summary");
    assert_eq!(summary.tags, vec!["user_7".to_string(), "batch_a".to_string()]);
    let page = store
        .list_runs(TENANT, &weft_dispatcher::store::RunListQuery { limit: 10, ..Default::default() })
        .await
        .unwrap();
    let listed_first = page.runs.iter().find(|s| s.run_id == first).expect("listed");
    assert_eq!(listed_first.tags, vec!["user_7".to_string()]);
}

/// The live read is the stop's candidate set: only this project's
/// non-terminal project runs carrying the tag, oldest tag first;
/// and the pure rule on top of it leaves the later of two concurrent
/// "keep me" runs alive.
#[weft_db_macros::test]
async fn live_tagged_read_respects_the_project_wall_and_terminals(pool: Pool) {
    let (store, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    let other_project = Uuid::new_v4();
    seed_project(&projects, project).await;
    seed_project(&projects, other_project).await;

    let a = start_run(&store, project, weft_core::run::RunKind::Project).await;
    let b = start_run(&store, project, weft_core::run::RunKind::Project).await;
    let done = start_run(&store, project, weft_core::run::RunKind::Project).await;
    let elsewhere = start_run(&store, other_project, weft_core::run::RunKind::Project).await;
    // Same project, same tag, but a node-test run: never selectable.
    let probe = start_run(&store, project, weft_core::run::RunKind::NodeTest).await;
    tag(&pool, a, &["user_7"], 1).await;
    tag(&pool, done, &["user_7"], 2).await;
    tag(&pool, elsewhere, &["user_7"], 3).await;
    tag(&pool, b, &["user_7"], 4).await;
    tag(&pool, probe, &["user_7"], 5).await;
    write(&store, done, &[RunEvent::RunCompleted { run_id: done, at_unix: 5 }]).await;

    let live = live_tagged_runs(&pool, project, "user_7").await.unwrap();
    let run_ids: Vec<_> = live.iter().map(|t| t.run_id).collect();
    assert_eq!(
        run_ids,
        vec![a, b],
        "oldest tag first, no terminal, no other project, no node test: {live:?}"
    );

    // b says "stop the others, keep me": only a goes.
    let b_seq = tag_seq(&pool, b, "user_7").await.unwrap().unwrap();
    assert_eq!(select_stop_targets(&live, b, Some(b_seq), StopSelf::Keep), vec![a]);
    // a says the same: nothing newer than a exists below its seq.
    let a_seq = tag_seq(&pool, a, "user_7").await.unwrap().unwrap();
    assert_eq!(select_stop_targets(&live, a, Some(a_seq), StopSelf::Keep), Vec::<weft_core::RunId>::new());
    // "we are all busted" from a takes both.
    assert_eq!(select_stop_targets(&live, a, None, StopSelf::Include), vec![a, b]);

    // The dispatcher's RunStore trait reads the same rows.
    let via_trait = store.live_tagged_runs(project, "user_7").await.unwrap();
    assert_eq!(via_trait, live);
}

/// A tag stop's terminal names who asked and which tag matched, on the
/// run row and on every per-node cancel it writes; and `weft
/// clean` takes the tag rows with the store.
#[weft_db_macros::test]
async fn cancel_terminals_carry_the_cause_and_clean_removes_tags(pool: Pool) {
    let (store, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project).await;
    let victim = start_run(&store, project, weft_core::run::RunKind::Project).await;
    let by = start_run(&store, project, weft_core::run::RunKind::Project).await;
    tag(&pool, victim, &["user_7"], 1).await;
    write(&store, victim, &[RunEvent::NodeStarted { run_id: victim, node_id: "wait".into(), frames: vec![], at_unix: 2 }]).await;

    // The run is parked on a form: a resume signal whose wake the
    // cancel must erase in the same step that ends the run.
    park_run(&pool, victim).await;
    store
        .signal_insert(
            &resume_signal("form-victim", project, victim)
        )
        .await
        .unwrap();

    let cause = CancelCause::Run { by, tag: Some("user_7".into()) };
    let program = rig_project(project);
    let write = store.cancel_run(victim, Some(&program), &cause).await.unwrap();
    assert_eq!(write.removed.len(), 1, "the parked run's form is gone: {write:?}");
    assert_eq!(write.removed[0].token, "form-victim");
    assert_eq!(write.node_cancellations, Some(1), "{write:?}");
    assert!(!write.requested, "no worker drives a parked run: it is ended here");
    assert!(store.signal_get("form-victim").await.unwrap().is_none());
    let after = crate::events(store.events_log(victim).await.unwrap());
    let node_cancel = after
        .iter()
        .find_map(|e| match e {
            RunEvent::NodeCancelled { node_id, reason, .. } if node_id == "wait" => Some(reason.clone()),
            _ => None,
        })
        .expect("the running node was cancelled");
    assert_eq!(node_cancel, cause.to_string());
    let terminal = after
        .iter()
        .find_map(|e| match e {
            RunEvent::RunCancelled { reason, cause, .. } => Some((reason.clone(), cause.clone())),
            _ => None,
        })
        .expect("terminal written");
    assert_eq!(terminal, (cause.to_string(), Some(cause.clone())));
    assert_eq!(
        store.run_summary(victim).await.unwrap().unwrap().status.as_str(),
        "cancelled"
    );
    // Terminal now: out of the live set.
    assert!(live_tagged_runs(&pool, project, "user_7").await.unwrap().is_empty());

    // A second cancel finds the terminal and writes nothing new.
    let again = store.cancel_run(victim, Some(&program), &CancelCause::User).await.unwrap();
    assert!(again.removed.is_empty() && again.node_cancellations.is_none(), "{again:?}");
    assert_eq!(crate::events(store.events_log(victim).await.unwrap()).len(), after.len());

    store.delete_run(victim).await.unwrap();
    assert_eq!(tag_seq(&pool, victim, "user_7").await.unwrap(), None, "tag rows go with the journal");
}

/// The cancel is one transaction with one outcome per state: a
/// finished run keeps its own terminal (nothing written, nothing
/// asked), and a run that never started has nothing: not even a
/// wait, since a wait is registered only for a run whose row exists.
#[weft_db_macros::test]
async fn cancel_leaves_a_finished_run_alone_and_strips_an_unstarted_run_id(pool: Pool) {
    let (store, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project).await;

    let done = start_run(&store, project, weft_core::run::RunKind::Project).await;
    write(&store, done, &[RunEvent::RunCompleted { run_id: done, at_unix: 5 }]).await;
    let before = crate::events(store.events_log(done).await.unwrap());
    let program = rig_project(project);
    let write = store.cancel_run(done, Some(&program), &CancelCause::User).await.unwrap();
    assert!(write.removed.is_empty() && write.node_cancellations.is_none() && !write.requested, "{write:?}");
    assert_eq!(crate::events(store.events_log(done).await.unwrap()).len(), before.len(), "a finished run keeps its terminal");
    assert_eq!(store.run_summary(done).await.unwrap().unwrap().status.as_str(), "completed");

    // Never started: no wait can be registered for it, and its cancel
    // opens no record.
    let ghost = weft_core::new_run_id();
    let refused = store.signal_insert(&resume_signal("form-ghost", project, ghost)).await.expect_err("no run row");
    assert!(refused.to_string().contains("has no record"), "{refused:#}");
    let write = store.cancel_run(ghost, None, &CancelCause::User).await.unwrap();
    assert!(write.removed.is_empty(), "{write:?}");
    assert!(write.node_cancellations.is_none() && !write.requested, "{write:?}");
    assert!(crate::events(store.events_log(ghost).await.unwrap()).is_empty());
}

/// The listing's status filter: a run parked on a wait is
/// `waiting_for_input` and also reached by `running`; a run queued or
/// driven is only `running`; an ended run is neither.
#[weft_db_macros::test]
async fn the_status_filter_finds_a_parked_run_under_waiting_and_running(pool: Pool) {
    use weft_core::program::RunStatus;
    let (store, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project).await;
    let parked = start_run(&store, project, weft_core::run::RunKind::Project).await;
    let plain = start_run(&store, project, weft_core::run::RunKind::Project).await;
    let ended = start_run(&store, project, weft_core::run::RunKind::Project).await;
    park_run(&pool, parked).await;
    write(&store, ended, &[RunEvent::RunCompleted { run_id: ended, at_unix: 2 }]).await;

    let listed = |status: RunStatus| {
        let store = &store;
        async move {
            let query = weft_dispatcher::store::RunListQuery { limit: 10, status: Some(status), ..Default::default() };
            let mut ids: Vec<_> =
                store.list_runs(TENANT, &query).await.unwrap().runs.into_iter().map(|s| s.run_id).collect();
            ids.sort();
            ids
        }
    };
    assert_eq!(listed(RunStatus::WaitingForInput).await, vec![parked]);
    let mut running = vec![parked, plain];
    running.sort();
    assert_eq!(listed(RunStatus::Running).await, running);
    assert_eq!(listed(RunStatus::Completed).await, vec![ended]);
}

/// The live view's recheck of its idle runs, one read for them all: of
/// the runs it names, only those whose record got a row past the one named
/// beside them come back, a run named with no row painted coming back as
/// soon as its record has one.
#[weft_db_macros::test]
async fn the_idle_runs_with_rows_past_what_was_painted_are_found_in_one_read(pool: Pool) {
    let (store, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project).await;
    let started = |n: u64, run_id| RunEvent::NodeStarted { run_id, node_id: "wait".into(), frames: vec![], at_unix: n };
    let (caught_up, behind, unpainted) = (
        start_run(&store, project, weft_core::run::RunKind::Project).await,
        start_run(&store, project, weft_core::run::RunKind::Project).await,
        start_run(&store, project, weft_core::run::RunKind::Project).await,
    );
    for run_id in [caught_up, behind] {
        write(&store, run_id, &[started(2, run_id)]).await;
    }
    let last_row = |run_id| {
        let pool = pool.clone();
        async move { pool.scalar::<i32>("SELECT max(seq) FROM run_log WHERE run_id = $1", &[&run_id]).await.unwrap() }
    };
    let (caught_up_at, behind_at) = (last_row(caught_up).await, last_row(behind).await);
    let mut woken = store.written_past(&[(caught_up, Some(caught_up_at)), (behind, Some(behind_at - 1)), (unpainted, None)]).await.unwrap();
    woken.sort();
    let mut expected = vec![behind, unpainted];
    expected.sort();
    assert_eq!(woken, expected);
    assert!(store.written_past(&[]).await.unwrap().is_empty(), "naming no run finds none");
}

/// A run whose record holds a row that no longer decodes still lists
/// with what its row says (the listing reads the run's row, never its
/// record). Its display read keeps every row that decodes and names the
/// one that does not, and its strict read refuses the whole record.
#[weft_db_macros::test]
async fn a_damaged_record_lists_from_its_row_and_reads_what_decodes(pool: Pool) {
    use weft_core::program::RunStatus;
    let (store, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project).await;
    let damaged = start_run(&store, project, weft_core::run::RunKind::Project).await;
    write(&store, damaged, &[RunEvent::NodeStarted { run_id: damaged, node_id: "wait".into(), frames: vec![], at_unix: 2 }]).await;
    pool.execute("UPDATE run_log SET events = '\\x00' WHERE run_id = $1 AND seq = 0", &[&damaged]).await.unwrap();

    let query = weft_dispatcher::store::RunListQuery { limit: 10, status: Some(RunStatus::Running), ..Default::default() };
    let running = store.list_runs(TENANT, &query).await.unwrap();
    assert_eq!(running.total, 1, "{running:?}");
    assert_eq!(running.runs[0].status, RunStatus::Running);

    let (shown, bad) = store.record_page(damaged, None).await.unwrap().decode_lossy(damaged);
    assert!(matches!(shown.iter().map(|row| &row.event).collect::<Vec<_>>()[..], [RunEvent::NodeStarted { .. }]), "{shown:?}");
    assert_eq!(bad.len(), 1);
    assert!(store.events_log(damaged).await.is_err(), "a fold never runs over part of a record");
}

/// A resume (form) signal parked on `run_id`.
fn resume_signal(token: &str, project_id: Uuid, run_id: weft_core::RunId) -> SignalRegistration {
    SignalRegistration {
        instance: None,
        activation_trigger: None,
        source_version: None,
        setup_run_id: None,
        program: None,
        token: token.to_string(),
        tenant_id: TENANT.to_string(),
        project_id,
        run_id: Some(run_id),
        node_id: "wait".to_string(),
        is_resume: true,
        spec_json: "{}".to_string(),
        consumer_kind: None,
        tags: Vec::new(),
        consumer_payload: None,
        surface_kind: "task_callback".to_string(),
        mount_path: None,
        mount_methods: Vec::new(),
        auth_kind: "none".to_string(),
        auth_config: None,
        kind_state: json!({}),
        kind_state_seq: 0,
        access_id: None,
        port_snapshot: None,
        holds: false,
        generation: 0,
    }
}

/// An instance token names exactly one project and always expires, and
/// revoking an instance's tokens takes its own and nobody else's.
#[weft_db_macros::test]
async fn an_instance_token_is_one_project_and_expires(pool: Pool) {
    use weft_core::instance::InstanceId;
    use weft_dispatcher::store::SignalToken;
    let (store, _) = setup(&pool).await;
    let project = Uuid::new_v4();
    let ada = InstanceId::new("ada").unwrap();
    let token = |hash: &str, instance: Option<&InstanceId>, projects: Vec<Uuid>, expires_at: Option<u64>| SignalToken {
        id: Uuid::new_v4(),
        token_hash: hash.into(),
        recognizer: "wft-test-...".into(),
        tenant_id: TENANT.into(),
        name: None,
        allowed_projects: projects,
        allowed_tags: Vec::new(),
        allowed_displays: Vec::new(),
        all_displays: false,
        created_at: 0,
        instance: instance.cloned(),
        expires_at,
        kind: weft_core::signal_token::TokenKind::Caller,
    };
    let err = store.mint_signal_token(&token("h1", Some(&ada), vec![], Some(10))).await.unwrap_err();
    assert!(format!("{err:#}").contains("signal_token_instance_has_one_project"), "{err:#}");
    let err = store.mint_signal_token(&token("h2", Some(&ada), vec![project], None)).await.unwrap_err();
    assert!(format!("{err:#}").contains("signal_token_instance_expires"), "{err:#}");
    let operator_instance = SignalToken {
        kind: weft_core::signal_token::TokenKind::Operator,
        ..token("h3", Some(&ada), vec![project], Some(10))
    };
    let err = store.mint_signal_token(&operator_instance).await.unwrap_err();
    assert!(format!("{err:#}").contains("signal_token_operator_is_nobody"), "{err:#}");

    store.mint_signal_token(&token("ada-1", Some(&ada), vec![project], Some(10))).await.unwrap();
    store.mint_signal_token(&token("ada-2", Some(&ada), vec![project], Some(20))).await.unwrap();
    let bob = InstanceId::new("bob").unwrap();
    store.mint_signal_token(&token("bob-1", Some(&bob), vec![project], Some(10))).await.unwrap();
    store.mint_signal_token(&token("author", None, vec![project], None)).await.unwrap();
    let read = store.get_signal_token("ada-1").await.unwrap().expect("stored");
    assert_eq!((read.instance.as_ref(), read.expires_at), (Some(&ada), Some(10)));
    assert!(read.expired(10) && !read.expired(9));

    let other_project = Uuid::new_v4();
    let revoke = weft_access_store::instance_tokens::revoke;
    assert_eq!(revoke(&pool, TENANT, other_project, &ada, None).await.unwrap(), 0);
    assert_eq!(revoke(&pool, "tenant-2", project, &ada, None).await.unwrap(), 0);
    assert_eq!(revoke(&pool, TENANT, project, &ada, Some(read.id)).await.unwrap(), 1);
    assert_eq!(revoke(&pool, TENANT, project, &ada, None).await.unwrap(), 1);
    let left: Vec<String> = store.list_signal_tokens(TENANT).await.unwrap().into_iter().map(|t| t.token_hash).collect();
    assert_eq!(left.len(), 2, "{left:?}");
    assert!(left.contains(&"bob-1".to_string()) && left.contains(&"author".to_string()));
}

/// The listing's `node` and `search` filters, both read from the search
/// index: `node` finds the runs a node started in, `search` the runs whose
/// recorded values carry every word asked for. A run is indexed after it
/// ended, once, by the first listing that reads the index, with nothing
/// running in the background; a run still going is in neither.
#[weft_db_macros::test]
async fn runs_are_found_by_a_node_they_ran_and_by_what_went_through_them(pool: Pool) {
    use std::sync::Arc;
    let (store, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project).await;
    let ran = |run_id, value: &'static str| {
        let store = &store;
        async move {
            let events = [
                RunEvent::NodeStarted { run_id, node_id: "wait".into(), frames: vec![], at_unix: 1 },
                RunEvent::PortEmitted {
                    run_id,
                    emission_id: Uuid::new_v4(),
                    node_id: "wait".into(),
                    frames: vec![],
                    port: "out".into(),
                    value: Arc::new(json!({ "email": value, "order": 4217 })),
                    at_unix: 2,
                },
            ];
            write(store, run_id, &events).await;
        }
    };
    let ada = start_run(&store, project, weft_core::run::RunKind::Project).await;
    let bob = start_run(&store, project, weft_core::run::RunKind::Project).await;
    let idle = start_run(&store, project, weft_core::run::RunKind::Project).await;
    let going = start_run(&store, project, weft_core::run::RunKind::Project).await;
    ran(ada, "ada@example.com").await;
    ran(bob, "bob@example.com").await;
    ran(going, "ada@example.com").await;
    for finished in [ada, bob, idle] {
        write(&store, finished, &[RunEvent::RunCompleted { run_id: finished, at_unix: 3 }]).await;
    }

    let listed = |node: Option<&'static str>, search: Option<&'static str>| {
        let store = &store;
        async move {
            let query = weft_dispatcher::store::RunListQuery {
                limit: 10,
                node: node.map(Into::into),
                search: search.map(Into::into),
                ..Default::default()
            };
            let page = store.list_runs(TENANT, &query).await.unwrap();
            let mut ids: Vec<_> = page.runs.into_iter().map(|s| s.run_id).collect();
            assert_eq!(page.total, ids.len() as u64, "the count agrees with the list");
            ids.sort();
            ids
        }
    };
    let sorted = |mut ids: Vec<weft_core::RunId>| {
        ids.sort();
        ids
    };
    assert_eq!(listed(Some("wait"), None).await, sorted(vec![ada, bob]), "the run still going is not indexed yet");
    let queued: i64 = pool.scalar("SELECT count(*) FROM run_search_queue", &[]).await.unwrap();
    assert_eq!(queued, 0, "the first search indexed every ended run");
    assert_eq!(listed(Some("elsewhere"), None).await, Vec::<weft_core::RunId>::new());
    assert_eq!(listed(None, Some("ada@example.com")).await, vec![ada], "the run still going has no document yet");
    assert_eq!(listed(None, Some("4217")).await, sorted(vec![ada, bob]), "numbers are words too");
    assert_eq!(listed(None, Some("ada@example.com 4217")).await, vec![ada], "every word asked for");
    assert_eq!(listed(None, Some("email")).await, Vec::<weft_core::RunId>::new(), "a field's name is not one of its values");
    assert_eq!(listed(Some("wait"), Some("bob@example.com")).await, vec![bob]);
}

/// A run found by a node it ran inside an included file, by the name a
/// person gives it: through the site that called it, so the file's two
/// calls are told apart, and never by its compiled id.
#[weft_db_macros::test]
async fn a_run_is_found_through_the_site_of_the_included_node_it_ran(pool: Pool) {
    let (store, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    let program: ProjectDefinition = serde_json::from_value(json!({
        "id": project,
        "nodes": [{
            "id": "@src:lib.strip", "nodeType": "T", "label": null, "config": null,
            "position": { "x": 0.0, "y": 0.0 }, "inputs": [], "outputs": [],
            "features": {}, "scope": ["@src:lib"], "groupBoundary": null, "requiresInfra": false, "images": []
        }],
        "edges": [],
        "groups": [
            { "id": "@src:lib", "kind": "body", "nodeIds": ["@src:lib.strip"] },
            { "id": "one", "kind": "call", "body": "@src:lib", "nodeIds": [] },
            { "id": "two", "kind": "call", "body": "@src:lib", "nodeIds": [] }
        ]
    }))
    .expect("the program including lib");
    projects
        .register_with_hashes(program, "db-rig", "", TENANT, Some("bin-A"), Some("def-1"), None, None, None, None, None, None)
        .await
        .expect("register project");
    let run = start_run(&store, project, weft_core::run::RunKind::Project).await;
    let in_two = vec![weft_core::frames::Frame::Call { site: "two".into() }];
    write(&store, run, &[
        RunEvent::NodeStarted { run_id: run, node_id: "@src:lib.strip".into(), frames: in_two, at_unix: 2 },
        RunEvent::RunCompleted { run_id: run, at_unix: 3 },
    ]).await;
    let through = |node: &'static str| {
        let store = &store;
        async move {
            let query = weft_dispatcher::store::RunListQuery { limit: 10, node: Some(node.into()), ..Default::default() };
            store.list_runs(TENANT, &query).await.unwrap().runs.into_iter().map(|s| s.run_id).collect::<Vec<_>>()
        }
    };
    assert_eq!(through("two.strip").await, vec![run]);
    assert!(through("one.strip").await.is_empty(), "the other call of the file did not run");
    assert!(through("@src:lib.strip").await.is_empty(), "a compiled id is no name a person asks with");

    // An entry an older weft built (format 1, the compiled ids) is built
    // again by the next search, so a run indexed before is found too.
    pool.execute("UPDATE run_search SET nodes = ARRAY['@src:lib.strip'], format = 1 WHERE run_id = $1", &[&run]).await.unwrap();
    assert_eq!(through("two.strip").await, vec![run]);
}


/// A run whose program no longer reads never fails the search: it is
/// indexed by its words, without its nodes (there is no program to spell
/// them by), and the healthy run beside it is found as ever.
#[weft_db_macros::test]
async fn a_run_whose_program_does_not_read_is_indexed_by_its_words(pool: Pool) {
    let (store, projects) = setup(&pool).await;
    let (project, unread_project) = (Uuid::new_v4(), Uuid::new_v4());
    seed_project(&projects, project).await;
    seed_project(&projects, unread_project).await;
    let healthy = ended_run_carrying(&store, project, "ada@example.com").await;
    let unread_program = ended_run_carrying(&store, unread_project, "dave@example.com").await;
    pool.execute("UPDATE project_definition SET project_json = '{\"damaged\": true}' WHERE project_id = $1", &[&unread_project])
        .await
        .unwrap();

    assert_eq!(found_carrying(&store, "dave@example.com").await, vec![unread_program], "its words without its program");
    let by_node = weft_dispatcher::store::RunListQuery { limit: 10, node: Some("wait".into()), ..Default::default() };
    let page = store.list_runs(TENANT, &by_node).await.expect("a search by node");
    assert_eq!(page.runs.iter().map(|s| s.run_id).collect::<Vec<_>>(), vec![healthy], "only the healthy run's nodes are spelled");
    assert_eq!(found_carrying(&store, "ada@example.com").await, vec![healthy]);
}

/// A run whose record carries NUL characters (a scraped page in a node's
/// output) is indexed like any other: the search finds it by the words
/// around them, its queue drains, and the run beside it is found too.
#[weft_db_macros::test]
async fn a_run_carrying_nul_characters_is_indexed(pool: Pool) {
    let (store, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project).await;
    let scraped = ended_run_carrying(&store, project, "scraped\u{0}page").await;
    let clean = ended_run_carrying(&store, project, "clean@example.com").await;
    assert_eq!(found_carrying(&store, "scrapedpage").await, vec![scraped], "found by the words around the NUL");
    let queued: i64 = pool.scalar("SELECT count(*) FROM run_search_queue", &[]).await.unwrap();
    assert_eq!(queued, 0, "the queue drained");
    assert_eq!(found_carrying(&store, "clean@example.com").await, vec![clean]);
}

/// One ended run of `project`, whose record carries `word`.
async fn ended_run_carrying(store: &PostgresStore, project: Uuid, word: &str) -> weft_core::RunId {
    let run_id = start_run(store, project, weft_core::run::RunKind::Project).await;
    write(store, run_id, &[
        RunEvent::NodeStarted { run_id, node_id: "wait".into(), frames: vec![], at_unix: 1 },
        RunEvent::PortEmitted {
            run_id,
            emission_id: Uuid::new_v4(),
            node_id: "wait".into(),
            frames: vec![],
            port: "out".into(),
            value: std::sync::Arc::new(json!({ "word": word })),
            at_unix: 2,
        },
        RunEvent::RunCompleted { run_id, at_unix: 3 },
    ])
    .await;
    run_id
}

/// The runs the listing finds carrying `word`.
async fn found_carrying(store: &PostgresStore, word: &str) -> Vec<weft_core::RunId> {
    let query = weft_dispatcher::store::RunListQuery { limit: 10, search: Some(word.into()), ..Default::default() };
    let mut ids: Vec<_> = store.list_runs(TENANT, &query).await.expect("the search").runs.into_iter().map(|s| s.run_id).collect();
    ids.sort();
    ids
}

/// A search reads every run's program on its round's own connection, so
/// it needs one connection however many programs it reads: on a pool of
/// one, it finds the runs. A program read the store fails fails the
/// search and leaves its runs queued; the next search, the store well
/// again, builds them.
#[weft_db_macros::test]
async fn a_search_reads_programs_on_its_own_connection(pool: Pool) {
    let (_, projects) = setup(&pool).await;
    let one = Pool::open(pool.config().clone(), 1, std::time::Duration::from_secs(5), weft_task_store::db::Waits::Expected);
    let store = PostgresStore::from_pool(one.clone());
    let project = Uuid::new_v4();
    seed_project(&projects, project).await;
    let ada = ended_run_carrying(&store, project, "ada@example.com").await;
    assert_eq!(found_carrying(&store, "ada@example.com").await, vec![ada], "one connection is enough");

    let first = ended_run_carrying(&store, project, "bob@example.com").await;
    let second = ended_run_carrying(&store, project, "bob@example.com").await;
    pool.execute("ALTER TABLE project_definition RENAME COLUMN project_json TO project_json_away", &[]).await.unwrap();
    let query = weft_dispatcher::store::RunListQuery { limit: 10, search: Some("bob@example.com".into()), ..Default::default() };
    store.list_runs(TENANT, &query).await.expect_err("the program read failed");
    let mut queued: Vec<weft_core::RunId> = pool.scalars("SELECT run_id FROM run_search_queue", &[]).await.unwrap();
    queued.sort();
    let mut both = vec![first, second];
    both.sort();
    assert_eq!(queued, both, "both stay queued");

    pool.execute("ALTER TABLE project_definition RENAME COLUMN project_json_away TO project_json", &[]).await.unwrap();
    assert_eq!(found_carrying(&store, "bob@example.com").await, both, "the next search builds them");
    // Closed here: the announcement flusher keeps a pool it was poked
    // with, which would otherwise keep its connection open past the test.
    one.close();
}

/// A flagged ending is read off its run's row with the instance it ran
/// inside, even when the row holds an outcome this weft does not know,
/// so a trigger setup whose record does not read still
/// names its own `weft bake`. Clearing one job's flag leaves the other's,
/// and the run's keep time is answered only once no flag is left.
#[weft_db_macros::test]
async fn a_flagged_ending_reads_its_instance_and_keeps_each_jobs_flag(pool: Pool) {
    use weft_dispatcher::run_ends::{clear_flags, take_flagged, Jobs};
    let (store, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project).await;
    let run_id = start_run(&store, project, weft_core::run::RunKind::Project).await;
    pool.execute(
        "UPDATE run SET state = 'ended', outcome = 'vanished', instance_id = 'ada', watch_end = TRUE, \
         holds_signals = TRUE, keep_until = 42 WHERE run_id = $1",
        &[&run_id],
    )
    .await
    .unwrap();
    assert!(store.run_summary(run_id).await.is_err(), "the whole summary refuses the outcome");

    let mut conn = pool.get().await.unwrap();
    let tx = conn.transaction().await.unwrap();
    let ending = take_flagged(&tx, run_id).await.unwrap().expect("the run is flagged");
    assert_eq!(ending.instance_id.as_deref(), Some("ada"));
    assert!(ending.watch_end && ending.holds_signals);
    let kept = clear_flags(&tx, run_id, Jobs { watch_end: false, holds_signals: true }).await.unwrap();
    assert_eq!(kept, None, "a flag is left, so retention does not count the run yet");
    tx.commit().await.unwrap();
    let flags: (bool, bool) = pool.one("SELECT watch_end, holds_signals FROM run WHERE run_id = $1", &[&run_id]).await.unwrap();
    assert_eq!(flags, (true, false), "only the done job's flag went");

    let tx = conn.transaction().await.unwrap();
    let ending = take_flagged(&tx, run_id).await.unwrap().expect("still flagged for its setup");
    assert!(ending.watch_end && !ending.holds_signals);
    let kept = clear_flags(&tx, run_id, Jobs { watch_end: true, holds_signals: false }).await.unwrap();
    assert_eq!(kept, Some(42));
    tx.commit().await.unwrap();
    let tx = conn.transaction().await.unwrap();
    assert!(take_flagged(&tx, run_id).await.unwrap().is_none(), "nothing is left to do");
}
