//! Layer-3 tests for execution tags against a REAL Postgres: the
//! `execution_tag` rows the broker writes on a worker's behalf, the
//! live-tagged read the dispatcher's `stop_tagged` task selects on, and
//! the cancel terminals a tag stop writes. The selection RULE itself is
//! pure (`weft_journal::tags::select_stop_targets`, layer-1 tested in
//! its own crate); what this rig proves is the SQL underneath it: the
//! BIGSERIAL order, the (execution, tag) idempotency, the live filter, the
//! project wall, the row's life ending with the journal's, and the
//! one-transaction cancel (`Journal::cancel_execution`) that a tag
//! stop and the stop button both end in.
//!
//! Same rig as `db_lifecycle.rs`: `#[sqlx::test]` hands each test a fresh
//! database, the real boot-time migration path builds the schema, and the
//! tests are gated behind `db-tests` so a plain `cargo test` needs no
//! Postgres (`scripts/run-db-tests.sh weft-dispatcher` runs them).
#![cfg(feature = "db-tests")]

use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

use weft_core::exec::CancelCause;
use weft_core::{ProjectDefinition, StopSelf};
use weft_dispatcher::journal::postgres::PostgresJournal;
use weft_dispatcher::journal::{Journal, SignalRegistration};
use weft_journal::tags::{live_tagged_executions, max_tag_seq, select_stop_targets, tag_execution_in, tag_seq};
use weft_journal::record::{Queued, Then};
use weft_journal::ExecEvent;

const TENANT: &str = "tenant-1";

async fn setup(pool: &PgPool) -> (PostgresJournal, weft_dispatcher::ProjectStore) {
    weft_dispatcher::app::apply_core_schema(pool).await.expect("core schema");
    let journal = PostgresJournal::from_pool(pool.clone());
    let projects: weft_dispatcher::ProjectStore =
        std::sync::Arc::new(weft_dispatcher::PostgresProjectStore::new(pool.clone()));
    (journal, projects)
}

/// The program every execution here runs: one node, `wait`, the one a
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
        .register_with_hashes(rig_project(id), "db-rig", "", TENANT, Some("bin-A"), Some("def-1"), None, None, None, None)
        .await
        .expect("register project");
}

/// Queue a fresh run of `project`, the way the dispatcher starts one.
/// `run_kind` starts another kind instead (a node test runs no program),
/// which the live read must never select no matter what it carries.
async fn start_execution(
    journal: &PostgresJournal,
    project: Uuid,
    run_kind: weft_core::exec::RunKind,
) -> weft_core::ExecutionId {
    let execution_id = weft_core::new_execution_id();
    let birth = ExecEvent::ExecutionStarted {
        execution_id,
        project_id: project,
        entry_node: "start".into(),
        phase: weft_core::context::Phase::Fire,
        definition_hash: run_kind.is_execution().then(|| "def-1".into()),
        binary_hash: run_kind.is_execution().then(|| "bin-A".into()),
        run_kind,
        source_version: None,
        selection: None,
        seed: None,
        instance: None, stand_in: None, fired_trigger: None, instance_values: Default::default(), picks: Default::default(), at_unix: 1,
        settings: Default::default(),
    };
    let queued = Queued {
        events: std::slice::from_ref(&birth), tenant: TENANT, keep_for: weft_core::run_settings::KeepFor::WEFT_DEFAULT,
        watch_end: false, stale: &[], spec: None, example: None,
    };
    assert!(journal.queue_run(queued, false).await.expect("queue the run"));
    execution_id
}

/// Write `events` into the record of a run nobody drives.
async fn write(journal: &PostgresJournal, execution_id: weft_core::ExecutionId, events: &[ExecEvent]) {
    let written = journal.append(execution_id, events, Then::Stays).await.expect("write the run's record");
    assert!(matches!(written, weft_journal::record::Appended::At(_)), "{written:?}");
}

/// Mark run `execution_id` parked on a wait, as its worker's letting go
/// leaves it.
async fn park_run(pool: &PgPool, execution_id: weft_core::ExecutionId) {
    sqlx::query("UPDATE run SET state = 'parked' WHERE execution_id = $1").bind(execution_id).execute(pool).await.unwrap();
}

/// The broker's write, as `/v1/execution/tag` performs it: the rows, one
/// transaction (the run's worker records the act in its own record).
async fn tag(pool: &PgPool, execution_id: weft_core::ExecutionId, tags: &[&str], at: u64) {
    let tags: Vec<String> = tags.iter().map(|t| t.to_string()).collect();
    let mut tx = pool.begin().await.unwrap();
    tag_execution_in(&mut tx, execution_id, &tags, at).await.expect("tag_execution_in");
    tx.commit().await.unwrap();
}

/// The order tags are written is the order the rule compares, and a
/// re-tag keeps its place.
#[sqlx::test]
async fn tag_rows_are_ordered_by_write_and_idempotent(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project).await;
    let first = start_execution(&journal, project, weft_core::exec::RunKind::Execution).await;
    let second = start_execution(&journal, project, weft_core::exec::RunKind::Execution).await;

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
    let summary = journal.execution_summary(second).await.unwrap().expect("summary");
    assert_eq!(summary.tags, vec!["user_7".to_string(), "batch_a".to_string()]);
    let page = journal
        .list_executions(TENANT, &weft_dispatcher::journal::ExecutionQuery { limit: 10, ..Default::default() })
        .await
        .unwrap();
    let listed_first = page.executions.iter().find(|s| s.execution_id == first).expect("listed");
    assert_eq!(listed_first.tags, vec!["user_7".to_string()]);
}

/// The live read is the stop's candidate set: only this project's
/// non-terminal project executions carrying the tag, oldest tag first;
/// and the pure rule on top of it leaves the later of two concurrent
/// "keep me" runs alive.
#[sqlx::test]
async fn live_tagged_read_respects_the_project_wall_and_terminals(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    let other_project = Uuid::new_v4();
    seed_project(&projects, project).await;
    seed_project(&projects, other_project).await;

    let a = start_execution(&journal, project, weft_core::exec::RunKind::Execution).await;
    let b = start_execution(&journal, project, weft_core::exec::RunKind::Execution).await;
    let done = start_execution(&journal, project, weft_core::exec::RunKind::Execution).await;
    let elsewhere = start_execution(&journal, other_project, weft_core::exec::RunKind::Execution).await;
    // Same project, same tag, but a node-test run: never selectable.
    let probe = start_execution(&journal, project, weft_core::exec::RunKind::NodeTest).await;
    tag(&pool, a, &["user_7"], 1).await;
    tag(&pool, done, &["user_7"], 2).await;
    tag(&pool, elsewhere, &["user_7"], 3).await;
    tag(&pool, b, &["user_7"], 4).await;
    tag(&pool, probe, &["user_7"], 5).await;
    write(&journal, done, &[ExecEvent::ExecutionCompleted { execution_id: done, at_unix: 5 }]).await;

    let live = live_tagged_executions(&pool, project, "user_7").await.unwrap();
    let execution_ids: Vec<_> = live.iter().map(|t| t.execution_id).collect();
    assert_eq!(
        execution_ids,
        vec![a, b],
        "oldest tag first, no terminal, no other project, no node test: {live:?}"
    );

    // b says "stop the others, keep me": only a goes.
    let b_seq = tag_seq(&pool, b, "user_7").await.unwrap().unwrap();
    assert_eq!(select_stop_targets(&live, b, Some(b_seq), StopSelf::Keep), vec![a]);
    // a says the same: nothing newer than a exists below its seq.
    let a_seq = tag_seq(&pool, a, "user_7").await.unwrap().unwrap();
    assert_eq!(select_stop_targets(&live, a, Some(a_seq), StopSelf::Keep), Vec::<weft_core::ExecutionId>::new());
    // "we are all busted" from a takes both.
    assert_eq!(select_stop_targets(&live, a, None, StopSelf::Include), vec![a, b]);

    // The dispatcher's Journal trait reads the same rows.
    let via_trait = journal.live_tagged_executions(project, "user_7").await.unwrap();
    assert_eq!(via_trait, live);
}

/// A tag stop's terminal names who asked and which tag matched, on the
/// execution row and on every per-node cancel it writes; and `weft
/// clean` takes the tag rows with the journal.
#[sqlx::test]
async fn cancel_terminals_carry_the_cause_and_clean_removes_tags(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project).await;
    let victim = start_execution(&journal, project, weft_core::exec::RunKind::Execution).await;
    let by = start_execution(&journal, project, weft_core::exec::RunKind::Execution).await;
    tag(&pool, victim, &["user_7"], 1).await;
    write(&journal, victim, &[ExecEvent::NodeStarted { execution_id: victim, node_id: "wait".into(), frames: vec![], at_unix: 2 }]).await;

    // The run is parked on a form: a resume signal whose wake the
    // cancel must erase in the same step that ends the run.
    park_run(&pool, victim).await;
    journal
        .signal_insert(
            &resume_signal("form-victim", project, victim)
        )
        .await
        .unwrap();

    let cause = CancelCause::Execution { by, tag: "user_7".into() };
    let program = rig_project(project);
    let write = journal.cancel_execution(victim, Some(&program), &cause).await.unwrap();
    assert_eq!(write.removed.len(), 1, "the parked run's form is gone: {write:?}");
    assert_eq!(write.removed[0].token, "form-victim");
    assert_eq!(write.node_cancellations, Some(1), "{write:?}");
    assert!(!write.requested, "no worker drives a parked run: it is ended here");
    assert!(journal.signal_get("form-victim").await.unwrap().is_none());
    let after = journal.events_log(victim).await.unwrap();
    let node_cancel = after
        .iter()
        .find_map(|e| match e {
            ExecEvent::NodeCancelled { node_id, reason, .. } if node_id == "wait" => Some(reason.clone()),
            _ => None,
        })
        .expect("the running node was cancelled");
    assert_eq!(node_cancel, cause.to_string());
    let terminal = after
        .iter()
        .find_map(|e| match e {
            ExecEvent::ExecutionCancelled { reason, cause, .. } => Some((reason.clone(), cause.clone())),
            _ => None,
        })
        .expect("terminal written");
    assert_eq!(terminal, (cause.to_string(), Some(cause.clone())));
    assert_eq!(
        journal.execution_summary(victim).await.unwrap().unwrap().status.as_str(),
        "cancelled"
    );
    // Terminal now: out of the live set.
    assert!(live_tagged_executions(&pool, project, "user_7").await.unwrap().is_empty());

    // A second cancel finds the terminal and writes nothing new.
    let again = journal.cancel_execution(victim, Some(&program), &CancelCause::User).await.unwrap();
    assert!(again.removed.is_empty() && again.node_cancellations.is_none(), "{again:?}");
    assert_eq!(journal.events_log(victim).await.unwrap().len(), after.len());

    journal.delete_execution(victim).await.unwrap();
    assert_eq!(tag_seq(&pool, victim, "user_7").await.unwrap(), None, "tag rows go with the journal");
}

/// The cancel is one transaction with one outcome per state: a
/// finished run keeps its own terminal (nothing written, nothing
/// asked), and an execution that never started has nothing: not even a
/// wait, since a wait is registered only for a run whose row exists.
#[sqlx::test]
async fn cancel_leaves_a_finished_run_alone_and_strips_an_unstarted_execution_id(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project).await;

    let done = start_execution(&journal, project, weft_core::exec::RunKind::Execution).await;
    write(&journal, done, &[ExecEvent::ExecutionCompleted { execution_id: done, at_unix: 5 }]).await;
    let before = journal.events_log(done).await.unwrap();
    let program = rig_project(project);
    let write = journal.cancel_execution(done, Some(&program), &CancelCause::User).await.unwrap();
    assert!(write.removed.is_empty() && write.node_cancellations.is_none() && !write.requested, "{write:?}");
    assert_eq!(journal.events_log(done).await.unwrap().len(), before.len(), "a finished run keeps its terminal");
    assert_eq!(journal.execution_summary(done).await.unwrap().unwrap().status.as_str(), "completed");

    // Never started: no wait can be registered for it, and its cancel
    // opens no record.
    let ghost = weft_core::new_execution_id();
    let refused = journal.signal_insert(&resume_signal("form-ghost", project, ghost)).await.expect_err("no run row");
    assert!(refused.to_string().contains("has no record"), "{refused:#}");
    let write = journal.cancel_execution(ghost, None, &CancelCause::User).await.unwrap();
    assert!(write.removed.is_empty(), "{write:?}");
    assert!(write.node_cancellations.is_none() && !write.requested, "{write:?}");
    assert!(journal.events_log(ghost).await.unwrap().is_empty());
}

/// The listing's status filter: a run parked on a wait is
/// `waiting_for_input` and also reached by `running`; a run queued or
/// driven is only `running`; an ended run is neither.
#[sqlx::test]
async fn the_status_filter_finds_a_parked_run_under_waiting_and_running(pool: PgPool) {
    use weft_core::program::RunStatus;
    let (journal, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project).await;
    let parked = start_execution(&journal, project, weft_core::exec::RunKind::Execution).await;
    let plain = start_execution(&journal, project, weft_core::exec::RunKind::Execution).await;
    let ended = start_execution(&journal, project, weft_core::exec::RunKind::Execution).await;
    park_run(&pool, parked).await;
    write(&journal, ended, &[ExecEvent::ExecutionCompleted { execution_id: ended, at_unix: 2 }]).await;

    let listed = |status: RunStatus| {
        let journal = &journal;
        async move {
            let query = weft_dispatcher::journal::ExecutionQuery { limit: 10, status: Some(status), ..Default::default() };
            let mut ids: Vec<_> =
                journal.list_executions(TENANT, &query).await.unwrap().executions.into_iter().map(|s| s.execution_id).collect();
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

/// A run whose record holds a row that no longer decodes still lists
/// with what its row says (the listing reads the run's row, never its
/// record). Its display read keeps every row that decodes and names the
/// one that does not, and its strict read refuses the whole record.
#[sqlx::test]
async fn a_damaged_record_lists_from_its_row_and_reads_what_decodes(pool: PgPool) {
    use weft_core::program::RunStatus;
    let (journal, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project).await;
    let damaged = start_execution(&journal, project, weft_core::exec::RunKind::Execution).await;
    write(&journal, damaged, &[ExecEvent::NodeStarted { execution_id: damaged, node_id: "wait".into(), frames: vec![], at_unix: 2 }]).await;
    sqlx::query("UPDATE run_log SET events = '\\x00' WHERE execution_id = $1 AND seq = 0")
        .bind(damaged)
        .execute(&pool)
        .await
        .unwrap();

    let query = weft_dispatcher::journal::ExecutionQuery { limit: 10, status: Some(RunStatus::Running), ..Default::default() };
    let running = journal.list_executions(TENANT, &query).await.unwrap();
    assert_eq!(running.total, 1, "{running:?}");
    assert_eq!(running.executions[0].status, RunStatus::Running);

    let (shown, bad) = journal.events_log_lossy(damaged).await.unwrap();
    assert!(matches!(shown.iter().map(|row| &row.event).collect::<Vec<_>>()[..], [ExecEvent::NodeStarted { .. }]), "{shown:?}");
    assert_eq!(bad.len(), 1);
    assert!(journal.events_log(damaged).await.is_err(), "a fold never runs over part of a record");
}

/// A resume (form) signal parked on `execution_id`.
fn resume_signal(token: &str, project_id: Uuid, execution_id: weft_core::ExecutionId) -> SignalRegistration {
    SignalRegistration {
        instance: None,
        activation_trigger: None,
        source_version: None,
        setup_execution_id: None,
        program: None,
        token: token.to_string(),
        tenant_id: TENANT.to_string(),
        project_id,
        execution_id: Some(execution_id),
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
    }
}

/// An instance token names exactly one project and always expires, and
/// revoking an instance's tokens takes its own and nobody else's.
#[sqlx::test]
async fn an_instance_token_is_one_project_and_expires(pool: PgPool) {
    use weft_core::instance::InstanceId;
    use weft_dispatcher::journal::SignalToken;
    let (journal, _) = setup(&pool).await;
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
    let err = journal.mint_signal_token(&token("h1", Some(&ada), vec![], Some(10))).await.unwrap_err();
    assert!(format!("{err:#}").contains("signal_token_instance_has_one_project"), "{err:#}");
    let err = journal.mint_signal_token(&token("h2", Some(&ada), vec![project], None)).await.unwrap_err();
    assert!(format!("{err:#}").contains("signal_token_instance_expires"), "{err:#}");
    let operator_instance = SignalToken {
        kind: weft_core::signal_token::TokenKind::Operator,
        ..token("h3", Some(&ada), vec![project], Some(10))
    };
    let err = journal.mint_signal_token(&operator_instance).await.unwrap_err();
    assert!(format!("{err:#}").contains("signal_token_operator_is_nobody"), "{err:#}");

    journal.mint_signal_token(&token("ada-1", Some(&ada), vec![project], Some(10))).await.unwrap();
    journal.mint_signal_token(&token("ada-2", Some(&ada), vec![project], Some(20))).await.unwrap();
    let bob = InstanceId::new("bob").unwrap();
    journal.mint_signal_token(&token("bob-1", Some(&bob), vec![project], Some(10))).await.unwrap();
    journal.mint_signal_token(&token("author", None, vec![project], None)).await.unwrap();
    let read = journal.get_signal_token("ada-1").await.unwrap().expect("stored");
    assert_eq!((read.instance.as_ref(), read.expires_at), (Some(&ada), Some(10)));
    assert!(read.expired(10) && !read.expired(9));

    let other_project = Uuid::new_v4();
    let revoke = weft_dispatcher::journal::postgres::revoke_instance_tokens;
    assert_eq!(revoke(&pool, TENANT, other_project, &ada, None).await.unwrap(), 0);
    assert_eq!(revoke(&pool, "tenant-2", project, &ada, None).await.unwrap(), 0);
    assert_eq!(revoke(&pool, TENANT, project, &ada, Some(read.id)).await.unwrap(), 1);
    assert_eq!(revoke(&pool, TENANT, project, &ada, None).await.unwrap(), 1);
    let left: Vec<String> = journal.list_signal_tokens(TENANT).await.unwrap().into_iter().map(|t| t.token_hash).collect();
    assert_eq!(left.len(), 2, "{left:?}");
    assert!(left.contains(&"bob-1".to_string()) && left.contains(&"author".to_string()));
}

/// The listing's `node` and `search` filters, both read from the search
/// index: `node` finds the runs a node started in, `search` the runs whose
/// recorded values carry every word asked for. A run is indexed after it
/// ended, once, by the search loop; a run still going is in neither.
#[sqlx::test]
async fn runs_are_found_by_a_node_they_ran_and_by_what_went_through_them(pool: PgPool) {
    use std::sync::Arc;
    use weft_dispatcher::search_index::{round, Round};
    let (journal, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project).await;
    let ran = |execution_id, value: &'static str| {
        let journal = &journal;
        async move {
            let events = [
                ExecEvent::NodeStarted { execution_id, node_id: "wait".into(), frames: vec![], at_unix: 1 },
                ExecEvent::PortEmitted {
                    execution_id,
                    emission_id: Uuid::new_v4(),
                    node_id: "wait".into(),
                    frames: vec![],
                    port: "out".into(),
                    value: Arc::new(json!({ "email": value, "order": 4217 })),
                    provided: false,
                    at_unix: 2,
                },
            ];
            write(journal, execution_id, &events).await;
        }
    };
    let ada = start_execution(&journal, project, weft_core::exec::RunKind::Execution).await;
    let bob = start_execution(&journal, project, weft_core::exec::RunKind::Execution).await;
    let idle = start_execution(&journal, project, weft_core::exec::RunKind::Execution).await;
    let going = start_execution(&journal, project, weft_core::exec::RunKind::Execution).await;
    ran(ada, "ada@example.com").await;
    ran(bob, "bob@example.com").await;
    ran(going, "ada@example.com").await;
    for finished in [ada, bob, idle] {
        write(&journal, finished, &[ExecEvent::ExecutionCompleted { execution_id: finished, at_unix: 3 }]).await;
    }
    assert_eq!(round(&pool, 1_000).await.unwrap(), Round::Indexed(3), "each ended run, once");
    assert_eq!(round(&pool, 1_000).await.unwrap(), Round::Indexed(0));

    let listed = |node: Option<&'static str>, search: Option<&'static str>| {
        let journal = &journal;
        async move {
            let query = weft_dispatcher::journal::ExecutionQuery {
                limit: 10,
                node: node.map(Into::into),
                search: search.map(Into::into),
                ..Default::default()
            };
            let page = journal.list_executions(TENANT, &query).await.unwrap();
            let mut ids: Vec<_> = page.executions.into_iter().map(|s| s.execution_id).collect();
            assert_eq!(page.total, ids.len() as u64, "the count agrees with the list");
            ids.sort();
            ids
        }
    };
    let sorted = |mut ids: Vec<weft_core::ExecutionId>| {
        ids.sort();
        ids
    };
    assert_eq!(listed(Some("wait"), None).await, sorted(vec![ada, bob]), "the run still going is not indexed yet");
    assert_eq!(listed(Some("elsewhere"), None).await, Vec::<weft_core::ExecutionId>::new());
    assert_eq!(listed(None, Some("ada@example.com")).await, vec![ada], "the run still going has no document yet");
    assert_eq!(listed(None, Some("4217")).await, sorted(vec![ada, bob]), "numbers are words too");
    assert_eq!(listed(None, Some("ada@example.com 4217")).await, vec![ada], "every word asked for");
    assert_eq!(listed(None, Some("email")).await, Vec::<weft_core::ExecutionId>::new(), "a field's name is not one of its values");
    assert_eq!(listed(Some("wait"), Some("bob@example.com")).await, vec![bob]);
}
