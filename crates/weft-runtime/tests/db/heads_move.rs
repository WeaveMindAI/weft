//! A database the release before working copies left, booted by this one:
//! each project's head, which that weft kept on the project's own row
//! (`project.head_version`, `head_run`), becomes the head of the project's
//! program copy, holding that version's files (`working_copy`, through the
//! project group's data step), and the columns are dropped. A project that
//! had no head gets no copy; a second boot moves nothing.

use serde_json::json;
use weft_task_store::db::{Client as _, Pool};

/// The newest migration the weft before working copies shipped.
const BEFORE_WORKING_COPIES: &str = "20261023T202848_trigger_activation_connect_fixes";

#[weft_db_macros::test]
async fn a_projects_head_becomes_its_program_copys(pool: Pool) {
    weft_task_store::schema_guard::build_as_of(&pool, &weft_runtime::schema::groups(), |m| m.id <= BEFORE_WORKING_COPIES)
        .await
        .unwrap();
    let projects: weft_dispatcher::ProjectStore = std::sync::Arc::new(weft_dispatcher::PostgresProjectStore::new(pool.clone()));
    let (with_head, without) = (uuid::Uuid::new_v4(), uuid::Uuid::new_v4());
    for id in [with_head, without] {
        let empty = serde_json::from_value(json!({ "id": id, "nodes": [], "edges": [] })).unwrap();
        projects.register_with_hashes(empty, "p", "", "local", None, None, None, None, None, None, None, None).await.unwrap();
    }
    let manifest = json!({ "main.weft": "a".repeat(64) });
    let (version, run) = ("v1".to_string(), uuid::Uuid::new_v4());
    pool.execute(
        "INSERT INTO project_version (id, project_id, parent_id, manifest, label, created_at) VALUES ($1, $2, NULL, $3::text::jsonb, NULL, 1)",
        &[&version, &with_head, &manifest.to_string()],
    )
    .await
    .unwrap();
    pool.execute("UPDATE project SET head_version = $2, head_run = $3 WHERE id = $1", &[&with_head, &version, &run]).await.unwrap();

    weft_runtime::schema::apply(&pool).await.unwrap();

    let copies = weft_dispatcher::working_copies::PostgresWorkingCopyStore::new(pool.clone());
    use weft_dispatcher::working_copies::WorkingCopyStoreOps;
    let moved = copies.list(with_head).await.unwrap();
    assert_eq!(moved.len(), 1, "{moved:?}");
    let copy = &moved[0];
    assert_eq!(copy.kind, weft_core::working_copy::WorkingCopyKind::Program);
    assert_eq!(copy.head, weft_core::working_copy::Head { head_version: Some(version.clone()), head_run: Some(run) });
    assert_eq!(serde_json::to_value(&copy.working).unwrap(), manifest, "the copy holds its head version's files");
    assert!(copies.list(without).await.unwrap().is_empty(), "a project with no head gets no copy");
    let column: Option<String> = pool
        .scalar_opt("SELECT column_name::text FROM information_schema.columns WHERE table_name = 'project' AND column_name = 'head_version'", &[])
        .await
        .unwrap();
    assert_eq!(column, None, "the contract part dropped the column");

    weft_runtime::schema::apply(&pool).await.expect("a boot with nothing to do");
    assert_eq!(copies.list(with_head).await.unwrap(), moved, "a second boot moves nothing");
}
