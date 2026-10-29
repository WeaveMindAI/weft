//! Multi-tenant isolation against a REAL Postgres: two tenants register
//! projects, queue runs and mint tokens, and neither can see, reach, or
//! seize the other's. What this proves is the SQL itself: every `WHERE
//! tenant_id = $caller` in the listings, the register's conflict guard
//! (`WHERE project.tenant_id = EXCLUDED.tenant_id`), and the run row's
//! own stamped owner that `authenticator::authorize_run` reads. A query
//! that lost its tenant filter fails here.
//!
//! Same rig as `db_lifecycle.rs`: `#[weft_db_macros::test]` hands each test a fresh
//! database, and the real boot-time migration path builds the schema.

use chrono::Utc;
use uuid::Uuid;

use weft_core::ProjectDefinition;
use weft_dispatcher::authenticator::authorize_run;
use weft_dispatcher::store::postgres::PostgresStore;
use weft_dispatcher::store::{RunListQuery, RunStore, SignalToken, TokenStore};
use weft_dispatcher::tenant::TenantId;
use weft_journal::record::Queued;
use weft_journal::RunEvent;
use weft_task_store::db::Pool;

const TENANT_A: &str = "tenant-a";
const TENANT_B: &str = "tenant-b";

async fn setup(pool: &Pool) -> (PostgresStore, weft_dispatcher::ProjectStore) {
    weft_task_store::apply_groups(pool, weft_dispatcher::app::DISPATCHER_GROUPS).await.expect("core schema");
    let store = PostgresStore::from_pool(pool.clone());
    let projects: weft_dispatcher::ProjectStore =
        std::sync::Arc::new(weft_dispatcher::PostgresProjectStore::new(pool.clone()));
    (store, projects)
}

fn definition(id: Uuid) -> ProjectDefinition {
    ProjectDefinition {
        id,
        nodes: vec![],
        edges: vec![],
        groups: vec![],
        created_at: Utc::now(),
        updated_at: Utc::now(),
        defaults: Default::default(),
        given: Default::default(),
        broken: Vec::new(),
        metadata_wires: Vec::new(),
    }
}

async fn register(projects: &weft_dispatcher::ProjectStore, id: Uuid, name: &str, tenant: &str) -> anyhow::Result<()> {
    projects
        .register_with_hashes(definition(id), name, "", tenant, Some("bin-A"), Some("def-1"), None, None, None, None, None, None)
        .await
        .map(drop)
}

/// Queue a fresh run of `project` for `tenant`, the way the dispatcher
/// starts one: the run row carries the tenant it was queued for.
async fn start_run(store: &PostgresStore, project: Uuid, tenant: &str) -> weft_core::RunId {
    let run_id = weft_core::new_run_id();
    let birth = RunEvent::RunStarted {
        given: Default::default(),
        run_id,
        project_id: project,
        entry_node: "entry".into(),
        phase: weft_core::context::Phase::Fire,
        definition_hash: Some("def-1".into()),
        binary_hash: Some("bin-A".into()),
        run_kind: weft_core::run::RunKind::Project,
        source_version: None,
        cut_from: None,
        seed: None,
        instance: None,
        stand_in: None,
        fired_trigger: None,
        instance_values: Default::default(),
        picks: Default::default(),
        at_unix: 1,
        settings: Default::default(),
    };
    let queued = Queued {
        events: std::slice::from_ref(&birth),
        tenant,
        keep_for: weft_core::run_settings::KeepFor::WEFT_DEFAULT,
        watch_end: false,
        stale: &[],
        spec: None,
        example: None,
        program: None,
    };
    assert!(store.queue_run(queued, false).await.expect("queue the run"));
    run_id
}

fn token(hash: &str, tenant: &str) -> SignalToken {
    SignalToken {
        id: Uuid::new_v4(),
        kind: weft_core::signal_token::TokenKind::Caller,
        token_hash: hash.to_string(),
        recognizer: "wft-test-…".to_string(),
        tenant_id: tenant.to_string(),
        name: None,
        allowed_projects: vec![],
        allowed_tags: vec![],
        allowed_displays: vec![],
        all_displays: false,
        created_at: 0,
        instance: None,
        expires_at: None,
    }
}

#[weft_db_macros::test]
async fn list_projects_is_scoped_to_the_caller_tenant(pool: Pool) {
    let (_, projects) = setup(&pool).await;
    let (a1, a2, b1) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    register(&projects, a1, "a-one", TENANT_A).await.expect("register a1");
    register(&projects, a2, "a-two", TENANT_A).await.expect("register a2");
    register(&projects, b1, "b-one", TENANT_B).await.expect("register b1");

    let a_ids: Vec<Uuid> = projects.list(TENANT_A).await.unwrap().iter().map(|p| p.id).collect();
    assert!(a_ids.contains(&a1) && a_ids.contains(&a2), "A sees its own: {a_ids:?}");
    assert!(!a_ids.contains(&b1), "A must NOT see B's project");

    let b_ids: Vec<Uuid> = projects.list(TENANT_B).await.unwrap().iter().map(|p| p.id).collect();
    assert_eq!(b_ids, vec![b1], "B sees only its own");
}

/// `authorize_project` authorizes iff `tenant_for(id) == caller`: the
/// primitive returns the true owner, and a missing project reads as
/// nothing (the gate answers 404 for both it and a foreign one).
#[weft_db_macros::test]
async fn tenant_for_drives_the_per_resource_gate(pool: Pool) {
    let (_, projects) = setup(&pool).await;
    let a1 = Uuid::new_v4();
    register(&projects, a1, "a-one", TENANT_A).await.expect("register");

    assert_eq!(projects.tenant_for(a1).await.unwrap().as_deref(), Some(TENANT_A));
    assert_eq!(projects.tenant_for(Uuid::new_v4()).await.unwrap(), None);
}

/// Tenant B may not re-register tenant A's project id to seize it: the
/// upsert's conflict guard refuses the collision, and A's ownership stands.
#[weft_db_macros::test]
async fn cross_tenant_project_id_takeover_is_refused(pool: Pool) {
    let (_, projects) = setup(&pool).await;
    let shared = Uuid::new_v4();
    register(&projects, shared, "a-owned", TENANT_A).await.expect("register");

    let takeover = register(&projects, shared, "b-grab", TENANT_B).await;
    assert!(takeover.is_err(), "cross-tenant re-register must be refused");
    assert_eq!(projects.tenant_for(shared).await.unwrap().as_deref(), Some(TENANT_A), "ownership unchanged");
    let a_names: Vec<String> = projects.list(TENANT_A).await.unwrap().into_iter().map(|p| p.name).collect();
    assert_eq!(a_names, vec!["a-owned".to_string()], "the refused register wrote nothing");
    assert!(projects.list(TENANT_B).await.unwrap().is_empty(), "B gained nothing");

    // The owner CAN re-register its own project.
    register(&projects, shared, "a-owned-v2", TENANT_A).await.expect("owner re-register allowed");
}

/// Each tenant's listing holds its own runs only, and asking for the
/// other tenant's project by id finds nothing either.
#[weft_db_macros::test]
async fn list_runs_is_scoped_to_the_caller_tenant(pool: Pool) {
    let (store, projects) = setup(&pool).await;
    let (proj_a, proj_b) = (Uuid::new_v4(), Uuid::new_v4());
    register(&projects, proj_a, "a", TENANT_A).await.expect("register a");
    register(&projects, proj_b, "b", TENANT_B).await.expect("register b");
    let run_a = start_run(&store, proj_a, TENANT_A).await;
    let run_b = start_run(&store, proj_b, TENANT_B).await;

    let all = RunListQuery { limit: 100, ..Default::default() };
    let a = store.list_runs(TENANT_A, &all).await.unwrap();
    assert_eq!((a.total, a.runs.iter().map(|r| r.run_id).collect::<Vec<_>>()), (1, vec![run_a]), "A sees only its run");
    let b = store.list_runs(TENANT_B, &all).await.unwrap();
    assert_eq!((b.total, b.runs.iter().map(|r| r.run_id).collect::<Vec<_>>()), (1, vec![run_b]), "B sees only its run");

    let a_project_as_b = RunListQuery { limit: 100, project_id: Some(proj_a), ..Default::default() };
    let peek = store.list_runs(TENANT_B, &a_project_as_b).await.unwrap();
    assert_eq!((peek.total, peek.runs.len()), (0, 0), "B naming A's project still finds nothing");
}

/// The run gate reads the owner off the run's own row, with no project
/// store in reach: the owner is authorized, and a stranger gets the same
/// 404 an unknown run gets.
#[weft_db_macros::test]
async fn the_run_gate_reads_the_runs_own_owner(pool: Pool) {
    let (store, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    register(&projects, project, "a", TENANT_A).await.expect("register");
    let run_id = start_run(&store, project, TENANT_A).await;

    let owner = store.run_owner(run_id).await.unwrap().expect("the run has an owner");
    assert_eq!((owner.tenant.as_str(), owner.project_id), (TENANT_A, project));

    let granted = authorize_run(&store, &TenantId(TENANT_A.to_string()), run_id).await.expect("the owner reaches its run");
    assert_eq!(granted.project_id, project);
    let (refused, _) = authorize_run(&store, &TenantId(TENANT_B.to_string()), run_id).await.expect_err("a stranger may not");
    assert_eq!(refused, axum::http::StatusCode::NOT_FOUND);
    let (unknown, _) =
        authorize_run(&store, &TenantId(TENANT_A.to_string()), Uuid::new_v4()).await.expect_err("an unknown run is refused");
    assert_eq!(unknown, axum::http::StatusCode::NOT_FOUND, "no existence leak");
}

#[weft_db_macros::test]
async fn signal_tokens_are_scoped_to_the_caller_tenant(pool: Pool) {
    let (store, _) = setup(&pool).await;
    let tok_a = token("hash-a", TENANT_A);
    let tok_b = token("hash-b", TENANT_B);
    store.mint_signal_token(&tok_a).await.unwrap();
    store.mint_signal_token(&tok_b).await.unwrap();

    let a = store.list_signal_tokens(TENANT_A).await.unwrap();
    assert_eq!(a.iter().map(|t| t.id).collect::<Vec<_>>(), vec![tok_a.id], "A lists only its token");
    let b = store.list_signal_tokens(TENANT_B).await.unwrap();
    assert_eq!(b.iter().map(|t| t.id).collect::<Vec<_>>(), vec![tok_b.id], "B lists only its token");

    assert!(!store.revoke_signal_token(tok_a.id, TENANT_B).await.unwrap(), "B must not revoke A's token");
    assert_eq!(store.list_signal_tokens(TENANT_A).await.unwrap().len(), 1, "A's token survives");

    assert!(store.revoke_signal_token(tok_a.id, TENANT_A).await.unwrap());
    assert!(store.list_signal_tokens(TENANT_A).await.unwrap().is_empty());
    assert_eq!(store.list_signal_tokens(TENANT_B).await.unwrap().len(), 1, "B's token is untouched");
}
