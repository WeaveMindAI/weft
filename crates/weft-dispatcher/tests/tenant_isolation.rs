//! Layer-3 contract test: multi-tenant isolation, proven against the in-memory
//! fakes (`MockProjectStore`, `MockJournal`) that are the real isolation
//! enforcers. Two tenants register projects, run executions, and mint tokens;
//! the test asserts neither can see, reach, or seize the other's resources.
//!
//! These exercise the SAME scoping code the Postgres impls run (the `WHERE
//! tenant_id = $caller` filters and the cross-tenant register guard), since the
//! fakes mirror those queries. The HTTP gate (`authenticator::authorize_project`)
//! is a thin wrapper over `ProjectStore::tenant_for`, also covered here.

use chrono::Utc;
use uuid::Uuid;
use weft_core::ProjectDefinition;
use weft_dispatcher::journal::{ExecutionQuery, SignalToken, Journal, MockJournal};
use weft_dispatcher::project_store::{MockProjectStore, ProjectStoreOps};

const TENANT_A: &str = "tenant-a";
const TENANT_B: &str = "tenant-b";

fn definition(id: Uuid) -> ProjectDefinition {
    ProjectDefinition {
        id,
        nodes: vec![],
        edges: vec![],
        groups: vec![],
        created_at: Utc::now(),
        updated_at: Utc::now(),
    }
}

async fn register(store: &MockProjectStore, id: Uuid, name: &str, tenant: &str) {
    store
        .register_with_hashes(definition(id), name, "", tenant, None, None, None, None)
        .await
        .expect("register");
}

#[tokio::test]
async fn list_projects_is_scoped_to_the_caller_tenant() {
    let store = MockProjectStore::new();
    let a1 = Uuid::new_v4();
    let a2 = Uuid::new_v4();
    let b1 = Uuid::new_v4();
    register(&store, a1, "a-one", TENANT_A).await;
    register(&store, a2, "a-two", TENANT_A).await;
    register(&store, b1, "b-one", TENANT_B).await;

    let a_list = store.list(TENANT_A).await.unwrap();
    let a_ids: Vec<Uuid> = a_list.iter().map(|p| p.id).collect();
    assert!(a_ids.contains(&a1) && a_ids.contains(&a2), "A sees its own");
    assert!(!a_ids.contains(&b1), "A must NOT see B's project");

    let b_list = store.list(TENANT_B).await.unwrap();
    let b_ids: Vec<Uuid> = b_list.iter().map(|p| p.id).collect();
    assert_eq!(b_ids, vec![b1], "B sees only its own");
}

#[tokio::test]
async fn tenant_for_drives_the_per_resource_gate() {
    // `authorize_project` authorizes iff `tenant_for(id) == caller`. Prove the
    // primitive it relies on returns the true owner, so a cross-tenant caller is
    // rejected and a missing project is indistinguishable from a foreign one.
    let store = MockProjectStore::new();
    let a1 = Uuid::new_v4();
    register(&store, a1, "a-one", TENANT_A).await;

    assert_eq!(store.tenant_for(a1).await.unwrap().as_deref(), Some(TENANT_A));
    // A missing project: None (the gate maps both None and wrong-tenant to 404).
    assert_eq!(store.tenant_for(Uuid::new_v4()).await.unwrap(), None);
}

#[tokio::test]
async fn cross_tenant_project_id_takeover_is_refused() {
    // Tenant B may not re-register tenant A's project id to seize it. The
    // register guard (mirroring the Postgres `WHERE project.tenant_id =
    // EXCLUDED.tenant_id`) rejects the collision, and A's ownership stands.
    let store = MockProjectStore::new();
    let shared_id = Uuid::new_v4();
    register(&store, shared_id, "a-owned", TENANT_A).await;

    let takeover = store
        .register_with_hashes(definition(shared_id), "b-grab", "", TENANT_B, None, None, None, None)
        .await;
    assert!(takeover.is_err(), "cross-tenant re-register must be refused");
    assert_eq!(
        store.tenant_for(shared_id).await.unwrap().as_deref(),
        Some(TENANT_A),
        "ownership unchanged after a refused takeover"
    );

    // The owner CAN re-register its own project (idempotent update).
    store
        .register_with_hashes(definition(shared_id), "a-owned-v2", "", TENANT_A, None, None, None, None)
        .await
        .expect("owner re-register allowed");
}

#[tokio::test]
async fn list_executions_is_scoped_to_the_caller_tenant() {
    let journal = MockJournal::new();
    let proj_a = Uuid::new_v4().to_string();
    let proj_b = Uuid::new_v4().to_string();
    // Mirror the project->tenant mapping the Postgres execution_color seed reads.
    journal.set_project_tenant(&proj_a, TENANT_A);
    journal.set_project_tenant(&proj_b, TENANT_B);

    let color_a = Uuid::new_v4();
    let color_b = Uuid::new_v4();
    journal
        .record_event(&started(color_a, &proj_a))
        .await
        .unwrap();
    journal
        .record_event(&started(color_b, &proj_b))
        .await
        .unwrap();

    let q = ExecutionQuery { limit: 100, ..Default::default() };
    let a = journal.list_executions(TENANT_A, &q).await.unwrap();
    assert_eq!(a.total, 1, "A sees only its execution");
    assert_eq!(a.executions.len(), 1);
    assert_eq!(a.executions[0].color, color_a);

    let b = journal.list_executions(TENANT_B, &q).await.unwrap();
    assert_eq!(b.total, 1, "B sees only its execution");
    assert_eq!(b.executions.len(), 1);
    assert_eq!(b.executions[0].color, color_b);
}

#[tokio::test]
async fn signal_tokens_are_scoped_to_the_caller_tenant() {
    let journal = MockJournal::new();
    let tok_a = token("hash-a", TENANT_A);
    let tok_b = token("hash-b", TENANT_B);
    journal.mint_signal_token(&tok_a).await.unwrap();
    journal.mint_signal_token(&tok_b).await.unwrap();

    let a = journal.list_signal_tokens(TENANT_A).await.unwrap();
    assert_eq!(a.len(), 1);
    assert_eq!(a[0].id, tok_a.id);

    // B cannot revoke A's token (wrong-tenant id matches nothing).
    assert!(
        !journal.revoke_signal_token(tok_a.id, TENANT_B).await.unwrap(),
        "B must not revoke A's token"
    );
    assert_eq!(journal.list_signal_tokens(TENANT_A).await.unwrap().len(), 1, "A's token survives");

    // A revokes its own token.
    assert!(journal.revoke_signal_token(tok_a.id, TENANT_A).await.unwrap());
    assert!(journal.list_signal_tokens(TENANT_A).await.unwrap().is_empty());
}


// ── helpers ─────────────────────────────────────────────────────────────────

fn started(color: Uuid, project_id: &str) -> weft_journal::ExecEvent {
    weft_journal::ExecEvent::ExecutionStarted {
        color,
        project_id: project_id.to_string(),
        entry_node: "entry".to_string(),
        phase: weft_core::context::Phase::Fire,
        definition_hash: "h".to_string(),
        at_unix: 0,
    }
}

fn token(hash: &str, tenant: &str) -> SignalToken {
    SignalToken {
        id: uuid::Uuid::new_v4(),
        token_hash: hash.to_string(),
        recognizer: "wft-test-…".to_string(),
        tenant_id: tenant.to_string(),
        name: None,
        allowed_projects: vec![],
        allowed_tags: vec![],
        created_at: 0,
    }
}
