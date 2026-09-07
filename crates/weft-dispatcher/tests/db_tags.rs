//! Layer-3 tests for execution tags against a REAL Postgres: the
//! `execution_tag` rows the broker writes on a worker's behalf, the
//! live-tagged read the dispatcher's `stop_tagged` task selects on, and
//! the cancel terminals a tag stop writes. The selection RULE itself is
//! pure (`weft_journal::tags::select_stop_targets`, layer-1 tested in
//! its own crate); what this rig proves is the SQL underneath it: the
//! BIGSERIAL order, the (color, tag) idempotency, the live filter, the
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
use weft_dispatcher::journal::{Journal, SignalPlacement, SignalRegistration};
use weft_journal::tags::{live_tagged_executions, max_tag_seq, select_stop_targets, tag_execution_in, tag_seq};
use weft_journal::ExecEvent;

const TENANT: &str = "tenant-1";

async fn setup(pool: &PgPool) -> (PostgresJournal, weft_dispatcher::ProjectStore) {
    weft_dispatcher::app::apply_core_schema(pool).await.expect("core schema");
    let journal = PostgresJournal::from_pool(pool.clone());
    let projects: weft_dispatcher::ProjectStore =
        std::sync::Arc::new(weft_dispatcher::PostgresProjectStore::new(pool.clone()));
    (journal, projects)
}

fn empty_project(id: Uuid) -> ProjectDefinition {
    serde_json::from_value(json!({ "id": id, "nodes": [], "edges": [] }))
        .expect("minimal ProjectDefinition")
}

async fn seed_project(projects: &weft_dispatcher::ProjectStore, id: Uuid) {
    projects
        .register_with_hashes(empty_project(id), "db-rig", "", TENANT, Some("bin-A"), Some("def-1"), None, None)
        .await
        .expect("register project");
}

/// Journal a fresh execution for `project` (the `execution_color` seed
/// rides in the same transaction, exactly like production). `node_test`
/// seeds the node-test kind instead, which the live read must never
/// select no matter what it carries.
async fn start_execution(
    journal: &PostgresJournal,
    project: Uuid,
    node_test: bool,
) -> weft_core::Color {
    let color = weft_core::Color::new_v4();
    journal
        .record_event(&ExecEvent::ExecutionStarted {
            color,
            project_id: project.to_string(),
            entry_node: "start".into(),
            phase: weft_core::context::Phase::Fire,
            definition_hash: Some("def-1".into()),
            node_test,
            subgraph: None,
            at_unix: 1,
        })
        .await
        .expect("ExecutionStarted");
    color
}

/// The broker's write, as `/v1/execution/tag` performs it: event plus
/// rows, one transaction, no pod (the rig has no fencing row to match).
async fn tag(pool: &PgPool, color: weft_core::Color, tags: &[&str], at: u64) {
    let tags: Vec<String> = tags.iter().map(|t| t.to_string()).collect();
    let mut tx = pool.begin().await.unwrap();
    tag_execution_in(&mut tx, color, &tags, at, None).await.expect("tag_execution_in");
    tx.commit().await.unwrap();
}

/// The order tags are written is the order the rule compares, a re-tag
/// keeps its place, and the event lands with the rows.
#[sqlx::test]
async fn tag_rows_are_ordered_by_write_and_idempotent(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project).await;
    let first = start_execution(&journal, project, false).await;
    let second = start_execution(&journal, project, false).await;

    tag(&pool, first, &["user_7"], 10).await;
    tag(&pool, second, &["user_7", "batch_a"], 11).await;
    // Re-tagging the first run (a body re-run after a crash) must not
    // move it after the second in the order.
    tag(&pool, first, &["user_7"], 12).await;

    let s1 = tag_seq(&pool, first, "user_7").await.unwrap().expect("first is tagged");
    let s2 = tag_seq(&pool, second, "user_7").await.unwrap().expect("second is tagged");
    assert!(s1 < s2, "first tagged first: {s1} < {s2}");
    assert_eq!(tag_seq(&pool, first, "batch_a").await.unwrap(), None);
    assert!(max_tag_seq(&pool).await.unwrap() >= s2);

    // Two ExecutionTagged events for the first run (one per call), the
    // record of the act even though the row was kept.
    let events = journal.events_log(first).await.unwrap();
    let tagged: Vec<_> = events
        .iter()
        .filter(|e| matches!(e, ExecEvent::ExecutionTagged { .. }))
        .collect();
    assert_eq!(tagged.len(), 2, "{events:?}");

    // The summaries carry the tags in claim order.
    let summary = journal.execution_summary(second).await.unwrap().expect("summary");
    assert_eq!(summary.tags, vec!["user_7".to_string(), "batch_a".to_string()]);
    let page = journal
        .list_executions(TENANT, &weft_dispatcher::journal::ExecutionQuery { limit: 10, ..Default::default() })
        .await
        .unwrap();
    let listed_first = page.executions.iter().find(|s| s.color == first).expect("listed");
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

    let a = start_execution(&journal, project, false).await;
    let b = start_execution(&journal, project, false).await;
    let done = start_execution(&journal, project, false).await;
    let elsewhere = start_execution(&journal, other_project, false).await;
    // Same project, same tag, but a node-test run: never selectable.
    let probe = start_execution(&journal, project, true).await;
    tag(&pool, a, &["user_7"], 1).await;
    tag(&pool, done, &["user_7"], 2).await;
    tag(&pool, elsewhere, &["user_7"], 3).await;
    tag(&pool, b, &["user_7"], 4).await;
    tag(&pool, probe, &["user_7"], 5).await;
    journal
        .record_event(&ExecEvent::ExecutionCompleted { color: done, outputs: json!({}), at_unix: 5 })
        .await
        .unwrap();

    let live = live_tagged_executions(&pool, &project.to_string(), "user_7").await.unwrap();
    let colors: Vec<_> = live.iter().map(|t| t.color).collect();
    assert_eq!(
        colors,
        vec![a, b],
        "oldest tag first, no terminal, no other project, no node test: {live:?}"
    );

    // b says "stop the others, keep me": only a goes.
    let b_seq = tag_seq(&pool, b, "user_7").await.unwrap().unwrap();
    assert_eq!(select_stop_targets(&live, b, Some(b_seq), StopSelf::Keep), vec![a]);
    // a says the same: nothing newer than a exists below its seq.
    let a_seq = tag_seq(&pool, a, "user_7").await.unwrap().unwrap();
    assert_eq!(select_stop_targets(&live, a, Some(a_seq), StopSelf::Keep), Vec::<weft_core::Color>::new());
    // "we are all busted" from a takes both.
    assert_eq!(select_stop_targets(&live, a, None, StopSelf::Include), vec![a, b]);

    // The dispatcher's Journal trait reads the same rows.
    let via_trait = journal.live_tagged_executions(&project.to_string(), "user_7").await.unwrap();
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
    let victim = start_execution(&journal, project, false).await;
    let by = start_execution(&journal, project, false).await;
    tag(&pool, victim, &["user_7"], 1).await;
    journal
        .record_event(&ExecEvent::NodeStarted {
            color: victim,
            node_id: "wait".into(),
            frames: vec![],
            input: json!({}),
            closed_ports: vec![],
            pulses_absorbed: vec![],
            at_unix: 2,
        })
        .await
        .unwrap();

    // The run is parked on a form: a resume signal whose wake the
    // cancel must erase in the same step that ends the run.
    seed_listener_pod(&pool, "listener-a", "disp-1").await;
    journal
        .signal_insert(
            &resume_signal("form-victim", project, victim),
            &SignalPlacement { listener_pod: "listener-a".into(), generation: 1 },
        )
        .await
        .unwrap();

    let cause = CancelCause::Execution { by, tag: "user_7".into() };
    let write = journal.cancel_execution(victim, &cause).await.unwrap();
    assert_eq!(write.removed.len(), 1, "the parked run's form is gone: {write:?}");
    assert_eq!(write.removed[0].token, "form-victim");
    assert_eq!(write.node_cancellations, Some(1), "{write:?}");
    assert!(!write.task_enqueued, "no pod owns a parked run");
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
        journal.execution_summary(victim).await.unwrap().unwrap().status,
        "cancelled"
    );
    // Terminal now: out of the live set.
    assert!(live_tagged_executions(&pool, &project.to_string(), "user_7").await.unwrap().is_empty());

    // A second cancel finds the terminal and writes nothing new.
    let again = journal.cancel_execution(victim, &CancelCause::User).await.unwrap();
    assert!(again.removed.is_empty() && again.node_cancellations.is_none(), "{again:?}");
    assert_eq!(journal.events_log(victim).await.unwrap().len(), after.len());

    journal.delete_execution(victim).await.unwrap();
    assert_eq!(tag_seq(&pool, victim, "user_7").await.unwrap(), None, "tag rows go with the journal");
}

/// The cancel is one transaction with one outcome per state: a
/// finished run keeps its own terminal (nothing written, nothing
/// queued), and a color that never started has only its signals to
/// lose.
#[sqlx::test]
async fn cancel_leaves_a_finished_run_alone_and_strips_an_unstarted_color(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project).await;
    seed_listener_pod(&pool, "listener-a", "disp-1").await;
    let placement = SignalPlacement { listener_pod: "listener-a".into(), generation: 1 };

    let done = start_execution(&journal, project, false).await;
    journal
        .record_event(&ExecEvent::ExecutionCompleted { color: done, outputs: json!({}), at_unix: 5 })
        .await
        .unwrap();
    let before = journal.events_log(done).await.unwrap();
    let write = journal.cancel_execution(done, &CancelCause::User).await.unwrap();
    assert!(write.removed.is_empty() && write.node_cancellations.is_none() && !write.task_enqueued, "{write:?}");
    assert_eq!(journal.events_log(done).await.unwrap().len(), before.len(), "a finished run keeps its terminal");
    assert_eq!(journal.execution_summary(done).await.unwrap().unwrap().status, "completed");

    // Never started: a stray resume signal on an unknown color goes,
    // and no journal is opened for it.
    let ghost = weft_core::Color::new_v4();
    journal.signal_insert(&resume_signal("form-ghost", project, ghost), &placement).await.unwrap();
    let write = journal.cancel_execution(ghost, &CancelCause::User).await.unwrap();
    assert_eq!(write.removed.len(), 1, "{write:?}");
    assert!(write.node_cancellations.is_none() && !write.task_enqueued, "{write:?}");
    assert!(journal.events_log(ghost).await.unwrap().is_empty());
}

/// A resume (form) signal parked on `color`.
fn resume_signal(token: &str, project_id: Uuid, color: weft_core::Color) -> SignalRegistration {
    SignalRegistration {
        token: token.to_string(),
        tenant_id: TENANT.to_string(),
        project_id: project_id.to_string(),
        color: Some(color),
        node_id: "wait".to_string(),
        is_resume: true,
        spec_json: "{}".to_string(),
        consumer_kind: None,
        tags: Vec::new(),
        consumer_payload: None,
        surface_kind: "task_callback".to_string(),
        mount_path: None,
        auth_kind: "none".to_string(),
        auth_config: None,
        kind_state: json!({}),
        kind_state_seq: 0,
        access_id: None,
        port_snapshot: None,
    }
}

/// A live listener pod row for the placement stamp (`signal_insert`
/// refuses a pod it does not know).
async fn seed_listener_pod(pool: &PgPool, pod_name: &str, owner: &str) {
    let now = weft_dispatcher::lease::now_unix();
    sqlx::query(
        "INSERT INTO listener_pod \
         (pod_name, admin_url, namespace, owner_pod_id, leased_until_unix, grace_until_unix) \
         VALUES ($1, $2, 'weft-system', $3, $4, $5)",
    )
    .bind(pod_name)
    .bind(format!("http://{pod_name}.weft-system.svc.cluster.local:8080"))
    .bind(owner)
    .bind(now + 3600)
    .bind(now - 1)
    .execute(pool)
    .await
    .expect("insert listener_pod");
}
