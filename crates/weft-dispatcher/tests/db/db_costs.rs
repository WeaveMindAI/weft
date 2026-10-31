//! Layer-3 tests for a run's cost rows (`run_cost`) against a REAL
//! Postgres: what the dispatcher's writers of a record put there, what
//! `ctx.costs()` reads back (`weft_journal::costs::list`, every filter in
//! its query) measured against filtering the records themselves, the boot's
//! data step that fills the table from the records an older weft left, and
//! the rows going with their run.
//!
//! Same rig as `db_lifecycle.rs`: `#[weft_db_macros::test]` hands each
//! test a fresh database, and the real boot-time migration path builds the
//! schema.

use serde_json::json;
use uuid::Uuid;

use weft_core::instance::InstanceId;
use weft_core::program::{CostFilter, CostRecord, PaidBy};
use weft_core::{CredentialOwner, ProjectDefinition, RunId};
use weft_dispatcher::store::postgres::PostgresStore;
use weft_dispatcher::store::RunStore;
use weft_journal::record::{Queued, Then};
use weft_journal::RunEvent;
use weft_task_store::db::{Client as _, Pool};

const TENANT: &str = "tenant-1";

async fn setup(pool: &Pool) -> (PostgresStore, weft_dispatcher::ProjectStore) {
    weft_task_store::apply_groups(pool, weft_dispatcher::app::DISPATCHER_GROUPS).await.expect("core schema");
    let store = PostgresStore::from_pool(pool.clone());
    let projects: weft_dispatcher::ProjectStore = std::sync::Arc::new(weft_dispatcher::PostgresProjectStore::new(pool.clone()));
    (store, projects)
}

/// The program the runs run: the two nodes that report costs.
fn rig_project(id: Uuid) -> ProjectDefinition {
    let node = |id: &str| {
        json!({
            "id": id, "nodeType": "Llm", "label": null, "config": null,
            "position": { "x": 0.0, "y": 0.0 }, "inputs": [], "outputs": [],
            "features": {}, "scope": [], "groupBoundary": null, "requiresInfra": false, "images": []
        })
    };
    serde_json::from_value(json!({ "id": id, "nodes": [node("llm"), node("search")], "edges": [] })).expect("rig ProjectDefinition")
}

async fn seed_project(projects: &weft_dispatcher::ProjectStore, id: Uuid) {
    projects
        .register_with_hashes(rig_project(id), "db-rig", "", TENANT, Some("bin-A"), Some("def-1"), None, None, None, None, None, None)
        .await
        .expect("register project");
}

fn birth(run_id: RunId, project: Uuid, instance: Option<&str>, at_unix: u64) -> RunEvent {
    RunEvent::RunStarted {
        given: Default::default(),
        run_id,
        project_id: project,
        entry_node: "start".into(),
        phase: weft_core::context::Phase::Fire,
        definition_hash: Some("def-1".into()),
        binary_hash: Some("bin-A".into()),
        run_kind: weft_core::run::RunKind::Project,
        source_version: None,
        cut_from: None,
        seed: None,
        instance: instance.map(|id| InstanceId::new(id).unwrap()),
        stand_in: None,
        fired_trigger: None,
        instance_values: Default::default(),
        picks: Default::default(),
        at_unix,
        settings: Default::default(),
    }
}

/// A cost `node` reported in `run_id`.
fn cost(run_id: RunId, cost_id: &str, node: &str, service: &str, amount_usd: Option<f64>, origin: CredentialOwner, at_unix: u64) -> RunEvent {
    RunEvent::CostReported {
        run_id,
        node_id: node.into(),
        frames: Vec::new(),
        cost_id: cost_id.into(),
        service: service.into(),
        model: Some("m-1".into()),
        amount_usd,
        billed: false,
        origin,
        metadata: serde_json::Value::Null,
        charge: None,
        at_unix,
    }
}

/// Queue a run of `project` born at `at_unix` with `events` after its
/// birth in its start (`insert_queued_in`), then write `later` into its
/// record as a writer that does not drive it (`append_locked_in`).
async fn run_with(
    store: &PostgresStore,
    project: Uuid,
    instance: Option<&str>,
    at_unix: u64,
    started_with: impl FnOnce(RunId) -> Vec<RunEvent>,
    later: impl FnOnce(RunId) -> Vec<RunEvent>,
) -> RunId {
    let run_id = weft_core::new_run_id();
    let mut events = vec![birth(run_id, project, instance, at_unix)];
    events.extend(started_with(run_id));
    let queued = Queued {
        events: &events,
        tenant: TENANT,
        keep_for: weft_core::run_settings::KeepFor::WEFT_DEFAULT,
        watch_end: false,
        stale: &[],
        spec: None,
        example: None,
        program: None,
    };
    assert!(store.queue_run(queued, false).await.expect("queue the run"));
    let later = later(run_id);
    if !later.is_empty() {
        let written = store.append(run_id, &later, Then::Stays).await.expect("write the run's record");
        assert!(matches!(written, weft_journal::record::Appended::At(_)), "{written:?}");
    }
    run_id
}

/// Two runs of one project, one for instance `ada`, with costs of every
/// kind: known and unknown figures, three kinds of credential, two nodes,
/// two services, spread in time.
async fn fixture(store: &PostgresStore, project: Uuid) -> (RunId, RunId) {
    let ada = || CredentialOwner::Instance(InstanceId::new("ada").unwrap());
    let first = run_with(
        store,
        project,
        Some("ada"),
        10,
        |run| vec![cost(run, "a1", "llm", "openai", Some(0.25), ada(), 11)],
        |run| {
            vec![
                cost(run, "a2", "search", "serp", None, CredentialOwner::Platform, 20),
                RunEvent::NodeStarted { run_id: run, node_id: "llm".into(), frames: Vec::new(), at_unix: 21 },
                cost(run, "a3", "llm", "openai", Some(1e-7), CredentialOwner::Author, 30),
            ]
        },
    )
    .await;
    let second = run_with(
        store,
        project,
        None,
        15,
        |_| Vec::new(),
        |run| vec![cost(run, "b1", "llm", "anthropic", Some(2.5), CredentialOwner::Platform, 40)],
    )
    .await;
    (first, second)
}

/// What `ctx.costs()` answered before the table: every run of the
/// project the filter reaches, in the order they started, each record's
/// costs in its order, filtered in Rust. The measure the table's query is
/// held to.
async fn read_off_the_records(pool: &Pool, project: &ProjectDefinition, filter: &CostFilter) -> Vec<CostRecord> {
    let runs: Vec<(RunId, Option<String>)> = pool
        .all("SELECT run_id, instance_id FROM run WHERE project_id = $1 ORDER BY started_at, run_id", &[&project.id])
        .await
        .unwrap();
    let mut out = Vec::new();
    for (run_id, instance) in runs {
        if filter.run.is_some_and(|run| run != run_id) || filter.instance.as_ref().is_some_and(|i| Some(i.as_str()) != instance.as_deref()) {
            continue;
        }
        for event in crate::recorded(&pool.get().await.unwrap(), run_id).await {
            let RunEvent::CostReported { node_id, frames, service, model, amount_usd, origin, at_unix, .. } = event else { continue };
            let call_path: Vec<String> = weft_core::frames::call_path(&frames).into_iter().map(str::to_string).collect();
            let node = weft_core::project::address_of(project, &node_id, &call_path);
            if filter.since_unix.is_some_and(|since| at_unix < since)
                || filter.node.as_deref().is_some_and(|n| n != node)
                || filter.service.as_deref().is_some_and(|s| s != service)
                || filter.paid_by.as_ref().is_some_and(|paid_by| *paid_by != PaidBy::of(&origin))
            {
                continue;
            }
            out.push(CostRecord {
                run: run_id,
                instance: instance.clone().map(|i| InstanceId::new(i).unwrap()),
                node,
                service,
                model,
                amount_usd,
                paid_by: origin,
                at_unix,
            });
        }
    }
    out
}

/// Every filter `ctx.costs()` takes, alone and together, answers what
/// filtering the records themselves answers, in the same order.
#[weft_db_macros::test]
async fn costs_answer_what_the_records_report(pool: Pool) {
    let (store, projects) = setup(&pool).await;
    let project_id = Uuid::new_v4();
    seed_project(&projects, project_id).await;
    let (first, second) = fixture(&store, project_id).await;
    let project = rig_project(project_id);
    let ada = InstanceId::new("ada").unwrap();
    let filters = [
        CostFilter::default(),
        CostFilter { instance: Some(ada.clone()), ..Default::default() },
        CostFilter { instance: Some(InstanceId::new("bob").unwrap()), ..Default::default() },
        CostFilter { run: Some(second), ..Default::default() },
        CostFilter { since_unix: Some(20), ..Default::default() },
        CostFilter { node: Some("llm".into()), ..Default::default() },
        CostFilter { node: Some("nowhere".into()), ..Default::default() },
        CostFilter { service: Some("openai".into()), ..Default::default() },
        CostFilter { paid_by: Some(PaidBy::Instance), ..Default::default() },
        CostFilter { paid_by: Some(PaidBy::Platform), ..Default::default() },
        CostFilter { paid_by: Some(PaidBy::Author), ..Default::default() },
        CostFilter {
            instance: Some(ada),
            node: Some("llm".into()),
            service: Some("openai".into()),
            run: Some(first),
            since_unix: Some(11),
            paid_by: Some(PaidBy::Author),
        },
    ];
    for filter in &filters {
        let listed = weft_journal::costs::list(&pool, project_id, Some(&project), filter).await.unwrap();
        assert_eq!(listed, read_off_the_records(&pool, &project, filter).await, "{filter:?}");
    }
    let everything = weft_journal::costs::list(&pool, project_id, Some(&project), &CostFilter::default()).await.unwrap();
    assert_eq!(
        everything.iter().map(|record| (record.run, record.amount_usd)).collect::<Vec<_>>(),
        [(first, Some(0.25)), (first, None), (first, Some(1e-7)), (second, Some(2.5))],
        "every cost of every run, in order, each figure exactly as reported"
    );
    let elsewhere = weft_journal::costs::list(&pool, Uuid::new_v4(), None, &CostFilter::default()).await.unwrap();
    assert!(elsewhere.is_empty(), "another project's costs are not these: {elsewhere:?}");
}

/// The boot's data step fills the table with exactly the rows the writers
/// wrote, from the records alone, and running it again changes nothing.
#[weft_db_macros::test]
async fn the_data_step_fills_the_table_from_the_records(pool: Pool) {
    let (store, projects) = setup(&pool).await;
    let project_id = Uuid::new_v4();
    seed_project(&projects, project_id).await;
    fixture(&store, project_id).await;
    // Every column of every row, as text.
    let every_row = "SELECT count(*), string_agg(c::text, E'\\n' ORDER BY c.run_id, c.cost_id) FROM run_cost c";
    let written: (i64, Option<String>) = pool.one(every_row, &[]).await.unwrap();
    assert_eq!(written.0, 4);

    // What a database an older weft kept looks like: records, no rows.
    pool.execute("DELETE FROM run_cost", &[]).await.unwrap();
    for _ in 0..2 {
        let mut conn = pool.get().await.unwrap();
        let tx = conn.transaction().await.unwrap();
        weft_journal::costs::carry_from_records(&tx).await.expect("the data step");
        tx.commit().await.unwrap();
        assert_eq!(pool.one::<(i64, Option<String>)>(every_row, &[]).await.unwrap(), written);
    }
}

/// A run's costs go with it, and only its own.
#[weft_db_macros::test]
async fn deleting_a_run_deletes_its_costs(pool: Pool) {
    let (store, projects) = setup(&pool).await;
    let project_id = Uuid::new_v4();
    seed_project(&projects, project_id).await;
    let (first, second) = fixture(&store, project_id).await;
    store.delete_run(first).await.expect("delete the run");
    let left: Vec<(RunId, i64)> =
        pool.all("SELECT run_id, count(*) FROM run_cost GROUP BY run_id", &[]).await.unwrap();
    assert_eq!(left, [(second, 1)]);
}
