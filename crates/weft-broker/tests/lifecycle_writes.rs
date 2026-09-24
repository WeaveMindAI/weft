//! Layer-3 contract tests for the broker's fenced lifecycle writes
//! (`lifecycle_writes`) against a REAL Postgres. The fence, the roster
//! membership check, the rollup and the Displaced-versus-Gone answer
//! all live IN SQL (one UPDATE evaluated on one row snapshot), so the
//! supervisor's in-memory fake cannot prove them; these tests drive the
//! exact statements the handlers serve. Also proves the repair statement
//! `decode_units_json` prints actually heals a bricked row.
//!
//! Gated behind `db-tests` (off by default) so a plain `cargo test` needs no PG.
#![cfg(feature = "db-tests")]

use sqlx::PgPool;

use weft_broker::lifecycle_writes::{
    complete_command, issue_command, next_command, pod_takes_on_projects, record_event,
    set_status, sync_ownership, unowned_work_waiting, FencedWrite, IssuedCommand,
};
use weft_broker_client::protocol::{
    decode_units_json, InfraLifecycleVerb, units_json_repair_sql, FailureStage, InfraNodeStatus as Status,
    SupervisorCommandCompleteRequest, SupervisorSetStatusRequest,
};

const TENANT: &str = "t1";
const PROJECT: uuid::Uuid = uuid::Uuid::from_u128(1);
const NODE: &str = "bridge";
const OWNER: &str = "supervisor-a";
const OTHER: &str = "supervisor-b";

async fn schema(pool: &PgPool) {
    weft_dispatcher::app::apply_core_schema(pool).await.expect("core schema");
}

/// `pod` holds the project's `infra_owner` lease for the next hour.
async fn lease(pool: &PgPool, pod: &str) {
    lease_of(pool, PROJECT, pod).await
}

/// `pod` holds `project`'s `infra_owner` lease for the next hour.
async fn lease_of(pool: &PgPool, project: uuid::Uuid, pod: &str) {
    sqlx::query(
        "INSERT INTO infra_owner (project_id, supervisor_pod, namespace, tenant_id, leased_until_unix) \
         VALUES ($1, $2, 'ns', $3, EXTRACT(EPOCH FROM NOW())::BIGINT + 3600) \
         ON CONFLICT (project_id) DO UPDATE SET supervisor_pod = EXCLUDED.supervisor_pod, \
         leased_until_unix = EXCLUDED.leased_until_unix",
    )
    .bind(project)
    .bind(pod)
    .bind(TENANT)
    .execute(pool)
    .await
    .expect("lease");
}

/// An uncompleted lifecycle command targeting the node; returns its id.
async fn command(pool: &PgPool) -> i64 {
    command_of(pool, PROJECT).await
}

/// An uncompleted lifecycle command of `project`; returns its id.
async fn command_of(pool: &PgPool, project: uuid::Uuid) -> i64 {
    let (id,): (i64,) = sqlx::query_as(
        "INSERT INTO infra_lifecycle_command \
         (tenant_id, project_id, node_id, verb, issued_by_pod, issued_at_unix) \
         VALUES ($1, $2, $3, 'stop', 'dispatcher', EXTRACT(EPOCH FROM NOW())::BIGINT) \
         RETURNING id",
    )
    .bind(TENANT)
    .bind(project)
    .bind(NODE)
    .fetch_one(pool)
    .await
    .expect("command");
    id
}

/// A `project` row with `namespace` (empty = no supervisor may take it).
async fn project_row(pool: &PgPool, id: uuid::Uuid, namespace: &str) {
    sqlx::query(
        "INSERT INTO project (id, name, status, project_json, updated_at, project_namespace) \
         VALUES ($1, 'p', 'inactive', '{}', 0, $2)",
    )
    .bind(id)
    .bind(namespace)
    .execute(pool)
    .await
    .expect("project row");
}

/// `pod`'s registry row in the supervisor pool.
async fn register(pool: &PgPool, pod: &str, draining: bool) {
    sqlx::query(
        "INSERT INTO supervisor_pod \
         (pod_name, admin_url, namespace, owner_pod_id, leased_until_unix, grace_until_unix, draining) \
         VALUES ($1, 'http://x', 'ns', 'd', EXTRACT(EPOCH FROM NOW())::BIGINT + 60, 0, $2)",
    )
    .bind(pod)
    .bind(draining)
    .execute(pool)
    .await
    .expect("register supervisor pod");
}

fn unit(status: &str) -> serde_json::Value {
    serde_json::json!({
        "status": status,
        "stop_behavior": { "kind": "scale_to_zero" },
        "flaky_after_seconds": 30,
        "recovery_after_seconds": 30
    })
}

async fn node_row(pool: &PgPool, status: &str, units: serde_json::Value) {
    sqlx::query(
        "INSERT INTO infra_node (project_id, node_id, instance_id, namespace, status, units_json) \
         VALUES ($1, $2, 'inst1', 'ns', $3, $4)",
    )
    .bind(PROJECT)
    .bind(NODE)
    .bind(status)
    .bind(units)
    .execute(pool)
    .await
    .expect("node row");
}

async fn row(pool: &PgPool) -> (String, serde_json::Value) {
    sqlx::query_as("SELECT status, units_json FROM infra_node WHERE project_id = $1 AND node_id = $2")
        .bind(PROJECT)
        .bind(NODE)
        .fetch_one(pool)
        .await
        .expect("row")
}

fn stamp(pod: &str, command_id: Option<i64>, unit: Option<&str>, status: Status) -> SupervisorSetStatusRequest {
    SupervisorSetStatusRequest {
        pod_name: pod.into(),
        command_id,
        project_id: PROJECT,
        node_id: NODE.into(),
        unit: unit.map(String::from),
        status,
        failure_stage: None,
        failure_message: None,
    }
}

/// A per-unit stamp patches that unit and re-rolls the node status
/// from the UPDATED roster (Flaky outranks Running; the stamped unit's
/// old status is not what gets rolled up).
#[sqlx::test]
async fn per_unit_stamp_patches_the_unit_and_rolls_up_the_new_roster(pool: PgPool) {
    schema(&pool).await;
    lease(&pool, OWNER).await;
    let cmd = command(&pool).await;
    node_row(&pool, "running", serde_json::json!({ "a": unit("running"), "b": unit("running") })).await;

    let out = set_status(&pool, &stamp(OWNER, Some(cmd), Some("a"), Status::Flaky)).await.unwrap();
    assert_eq!(out, FencedWrite::Applied);
    let (status, units) = row(&pool).await;
    assert_eq!(status, "flaky");
    assert_eq!(units["a"]["status"], "flaky");
    assert_eq!(units["b"]["status"], "running");
    // The patch kept every other field of the entry.
    assert_eq!(units["a"]["stop_behavior"]["kind"], "scale_to_zero");
}

/// A per-unit stamp for a unit the roster does not carry is Gone and
/// writes NOTHING: no status-only stub entry (the corruption that
/// bricked every decode of the row), no status change.
#[sqlx::test]
async fn per_unit_stamp_on_a_missing_unit_is_gone_and_writes_nothing(pool: PgPool) {
    schema(&pool).await;
    lease(&pool, OWNER).await;
    let cmd = command(&pool).await;
    node_row(&pool, "running", serde_json::json!({ "a": unit("running") })).await;

    let out = set_status(&pool, &stamp(OWNER, Some(cmd), Some("ghost"), Status::Stopped)).await.unwrap();
    assert_eq!(out, FencedWrite::Gone);
    let (status, units) = row(&pool).await;
    assert_eq!(status, "running");
    assert_eq!(units, serde_json::json!({ "a": unit("running") }));
    // The autonomous (no command) branch answers the same.
    let out = set_status(&pool, &stamp(OWNER, None, Some("ghost"), Status::Running)).await.unwrap();
    assert_eq!(out, FencedWrite::Gone);
}

/// A stamp by a pod that does not hold the lease is Displaced (not
/// Gone: the target is there, the writer is not the owner), and the
/// row is untouched.
#[sqlx::test]
async fn stamp_by_a_displaced_pod_is_displaced(pool: PgPool) {
    schema(&pool).await;
    lease(&pool, OWNER).await;
    let cmd = command(&pool).await;
    node_row(&pool, "running", serde_json::json!({ "a": unit("running") })).await;

    let out = set_status(&pool, &stamp(OTHER, Some(cmd), Some("a"), Status::Stopped)).await.unwrap();
    assert_eq!(out, FencedWrite::Displaced);
    assert_eq!(row(&pool).await.0, "running");
    // Node-wide from the displaced pod: same answer.
    let out = set_status(&pool, &stamp(OTHER, Some(cmd), None, Status::Terminating)).await.unwrap();
    assert_eq!(out, FencedWrite::Displaced);
    assert_eq!(row(&pool).await.0, "running");
}

/// The autonomous (health) stamp is fenced by ownership too: a pod that
/// lost the project cannot stamp it while no command is in flight.
#[sqlx::test]
async fn autonomous_stamp_by_a_displaced_pod_is_displaced(pool: PgPool) {
    schema(&pool).await;
    lease(&pool, OWNER).await;
    node_row(&pool, "running", serde_json::json!({ "a": unit("running") })).await;

    let out = set_status(&pool, &stamp(OTHER, None, Some("a"), Status::Flaky)).await.unwrap();
    assert_eq!(out, FencedWrite::Displaced);
    assert_eq!(row(&pool).await.0, "running");
    assert_eq!(
        set_status(&pool, &stamp(OWNER, None, Some("a"), Status::Flaky)).await.unwrap(),
        FencedWrite::Applied
    );
}

/// A node-wide stamp writes every unit AND the node status directly,
/// so a unit-less roster still takes it (the rollup would have said
/// 'stopped'); a completed command no longer fences anything (Gone).
#[sqlx::test]
async fn node_wide_stamp_sets_every_unit_and_the_node_even_with_no_units(pool: PgPool) {
    schema(&pool).await;
    lease(&pool, OWNER).await;
    let cmd = command(&pool).await;
    node_row(&pool, "running", serde_json::json!({ "a": unit("running"), "b": unit("flaky") })).await;

    let mut req = stamp(OWNER, Some(cmd), None, Status::Failed);
    req.failure_stage = Some(FailureStage::Apply);
    req.failure_message = Some("boom".into());
    assert_eq!(set_status(&pool, &req).await.unwrap(), FencedWrite::Applied);
    let (status, units) = row(&pool).await;
    assert_eq!(status, "failed");
    assert_eq!(units["a"]["status"], "failed");
    assert_eq!(units["b"]["status"], "failed");

    // Unit-less roster.
    sqlx::query("UPDATE infra_node SET units_json = '{}'::jsonb").execute(&pool).await.unwrap();
    assert_eq!(
        set_status(&pool, &stamp(OWNER, Some(cmd), None, Status::Terminating)).await.unwrap(),
        FencedWrite::Applied
    );
    assert_eq!(row(&pool).await.0, "terminating");

    // Completed command: the fence no longer matches, owner still owns.
    complete_command(
        &pool,
        &SupervisorCommandCompleteRequest { pod_name: OWNER.into(), command_id: cmd, error: None, cancelled: false },
    )
    .await
    .unwrap();
    assert_eq!(
        set_status(&pool, &stamp(OWNER, Some(cmd), None, Status::Stopped)).await.unwrap(),
        FencedWrite::Gone
    );
}

/// The autonomous (health) stamp is fenced by ANY uncompleted command
/// for the node and answers Gone while one is in flight; it applies
/// once the command completes.
#[sqlx::test]
async fn autonomous_stamp_is_fenced_by_an_in_flight_command(pool: PgPool) {
    schema(&pool).await;
    lease(&pool, OWNER).await;
    node_row(&pool, "running", serde_json::json!({ "a": unit("running") })).await;
    let cmd = command(&pool).await;

    assert_eq!(
        set_status(&pool, &stamp(OWNER, None, Some("a"), Status::Flaky)).await.unwrap(),
        FencedWrite::Gone
    );
    assert_eq!(row(&pool).await.0, "running");
    complete_command(
        &pool,
        &SupervisorCommandCompleteRequest { pod_name: OWNER.into(), command_id: cmd, error: None, cancelled: false },
    )
    .await
    .unwrap();
    assert_eq!(
        set_status(&pool, &stamp(OWNER, None, Some("a"), Status::Flaky)).await.unwrap(),
        FencedWrite::Applied
    );
    assert_eq!(row(&pool).await.0, "flaky");
}

/// Completing a command is exactly-once by exactly the owner: the
/// first completion applies and records the outcome, a second is Gone,
/// a displaced pod's is Displaced, and an unknown id is Gone.
#[sqlx::test]
async fn command_completion_is_once_and_owner_only(pool: PgPool) {
    schema(&pool).await;
    lease(&pool, OWNER).await;
    let cmd = command(&pool).await;
    let req = |pod: &str, id: i64, error: Option<&str>| SupervisorCommandCompleteRequest {
        pod_name: pod.into(),
        command_id: id,
        error: error.map(String::from),
        cancelled: false,
    };

    assert_eq!(complete_command(&pool, &req(OTHER, cmd, None)).await.unwrap(), FencedWrite::Displaced);
    assert_eq!(complete_command(&pool, &req(OWNER, cmd, Some("boom"))).await.unwrap(), FencedWrite::Applied);
    let (outcome, message): (String, Option<String>) =
        sqlx::query_as("SELECT outcome, outcome_message FROM infra_lifecycle_command WHERE id = $1")
            .bind(cmd)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!((outcome.as_str(), message.as_deref()), ("failed", Some("boom")));
    assert_eq!(complete_command(&pool, &req(OWNER, cmd, None)).await.unwrap(), FencedWrite::Gone);
    assert_eq!(complete_command(&pool, &req(OWNER, cmd + 1000, None)).await.unwrap(), FencedWrite::Gone);
}

/// The owner is handed its projects' commands oldest first, never one of
/// a project it names busy (that project's next command waits for the
/// running one), and never one of a project it does not own.
#[sqlx::test]
async fn next_command_skips_busy_and_foreign_projects(pool: PgPool) {
    schema(&pool).await;
    let second = uuid::Uuid::from_u128(2);
    let foreign = uuid::Uuid::from_u128(3);
    for project in [PROJECT, second, foreign] {
        project_row(&pool, project, "ns").await;
    }
    lease_of(&pool, PROJECT, OWNER).await;
    lease_of(&pool, second, OWNER).await;
    lease_of(&pool, foreign, OTHER).await;
    let foreign_cmd = command_of(&pool, foreign).await;
    let first_a = command_of(&pool, PROJECT).await;
    let first_b = command_of(&pool, second).await;
    let second_a = command_of(&pool, PROJECT).await;

    let next = |busy: Vec<uuid::Uuid>| {
        let pool = pool.clone();
        async move { next_command(&pool, OWNER, &busy).await.unwrap().map(|c| c.id) }
    };
    assert_eq!(next(vec![]).await, Some(first_a));
    // Busy with PROJECT: its second command waits, the other project's runs.
    assert_eq!(next(vec![PROJECT]).await, Some(first_b));
    assert_eq!(next(vec![PROJECT, second]).await, None);
    // The other supervisor's project is never handed out.
    assert_ne!(next(vec![]).await, Some(foreign_cmd));

    complete_command(
        &pool,
        &SupervisorCommandCompleteRequest {
            pod_name: OWNER.into(),
            command_id: first_a,
            error: None,
            cancelled: false,
        },
    )
    .await
    .unwrap();
    assert_eq!(next(vec![second]).await, Some(second_a));
}

/// A command on a project a supervisor may take (it has a namespace) and
/// nobody holds is unowned work; once a live lease covers it, or the
/// command completes, it is not. A project with no namespace yet is no
/// supervisor's to take, so its command is not reported either.
#[sqlx::test]
async fn unowned_work_is_a_waiting_command_on_a_takeable_unleased_project(pool: PgPool) {
    schema(&pool).await;
    let unplaced = uuid::Uuid::from_u128(9);
    project_row(&pool, unplaced, "").await;
    command_of(&pool, unplaced).await;
    assert!(!unowned_work_waiting(&pool).await.unwrap(), "a project with no namespace is no supervisor's to take");

    project_row(&pool, PROJECT, "ns").await;
    let cmd = command(&pool).await;
    assert!(unowned_work_waiting(&pool).await.unwrap());
    lease(&pool, OWNER).await;
    assert!(!unowned_work_waiting(&pool).await.unwrap(), "an owned project's command is its owner's");
    sqlx::query("UPDATE infra_owner SET leased_until_unix = 0").execute(&pool).await.unwrap();
    assert!(unowned_work_waiting(&pool).await.unwrap(), "an expired lease owns nothing");
    sqlx::query("UPDATE infra_lifecycle_command SET completed_at_unix = 1 WHERE id = $1")
        .bind(cmd)
        .execute(&pool)
        .await
        .unwrap();
    assert!(!unowned_work_waiting(&pool).await.unwrap(), "a completed command waits on nobody");
}

/// Only a working pool member takes ownership: a registered pod claims a
/// free project; a draining one, or one whose row is gone (reaped, its
/// pod not stopped yet), claims nothing and does not renew what it held,
/// so the project its drain handed over stays free for a survivor.
#[sqlx::test]
async fn only_a_working_supervisor_claims_and_renews(pool: PgPool) {
    schema(&pool).await;
    project_row(&pool, PROJECT, "ns").await;
    let owned = |pod: &'static str| {
        let pool = pool.clone();
        async move { sync_ownership(&pool, pod, 0.0).await.unwrap().owned.len() }
    };
    let lease_left = || {
        let pool = pool.clone();
        async move {
            sqlx::query_scalar::<_, i64>(
                "SELECT leased_until_unix - EXTRACT(EPOCH FROM NOW())::BIGINT FROM infra_owner WHERE project_id = $1",
            )
            .bind(PROJECT)
            .fetch_optional(&pool)
            .await
            .unwrap()
        }
    };

    register(&pool, OTHER, true).await;
    assert_eq!(owned(OTHER).await, 0, "a draining pod takes nothing");
    assert_eq!(owned("never-registered").await, 0, "a pod with no row takes nothing");
    register(&pool, OWNER, false).await;
    assert_eq!(owned(OWNER).await, 1, "a working pod takes the free project");

    // The pod is reaped: its row goes, its lease is released short.
    sqlx::query("DELETE FROM supervisor_pod WHERE pod_name = $1").bind(OWNER).execute(&pool).await.unwrap();
    sqlx::query("UPDATE infra_owner SET leased_until_unix = EXTRACT(EPOCH FROM NOW())::BIGINT + 1")
        .execute(&pool)
        .await
        .unwrap();
    owned(OWNER).await;
    assert!(lease_left().await.unwrap() <= 1, "a reaped pod must not renew what it held");
}

/// A tick reports the projects it took on: a fresh claim, and the pod's
/// own lease revived after it lapsed. A lease it merely renewed is not
/// news, and neither is a project another pod holds.
#[sqlx::test]
async fn a_tick_reports_the_projects_it_took_on(pool: PgPool) {
    schema(&pool).await;
    let foreign = uuid::Uuid::from_u128(3);
    project_row(&pool, PROJECT, "ns").await;
    project_row(&pool, foreign, "ns").await;
    lease_of(&pool, foreign, OTHER).await;
    register(&pool, OWNER, false).await;
    let claimed = || {
        let pool = pool.clone();
        async move { sync_ownership(&pool, OWNER, 0.0).await.unwrap().claimed }
    };

    assert_eq!(claimed().await, vec![PROJECT], "a free project is claimed");
    assert!(claimed().await.is_empty(), "a renewed lease is not news");
    sqlx::query("UPDATE infra_owner SET leased_until_unix = 0 WHERE project_id = $1")
        .bind(PROJECT)
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(claimed().await, vec![PROJECT], "a lapsed lease taken back is news");
}

/// Only a pod that can take on projects is one the claim sends to claim
/// them: registered, not draining, below saturation.
#[sqlx::test]
async fn only_a_registered_idle_enough_pod_takes_on_projects(pool: PgPool) {
    schema(&pool).await;
    assert!(!pod_takes_on_projects(&pool, OWNER).await.unwrap(), "no registry row");
    register(&pool, OWNER, false).await;
    assert!(pod_takes_on_projects(&pool, OWNER).await.unwrap());
    sqlx::query("UPDATE supervisor_pod SET mem_pressure = 0.99 WHERE pod_name = $1")
        .bind(OWNER)
        .execute(&pool)
        .await
        .unwrap();
    assert!(!pod_takes_on_projects(&pool, OWNER).await.unwrap(), "saturated");
    register(&pool, OTHER, true).await;
    assert!(!pod_takes_on_projects(&pool, OTHER).await.unwrap(), "draining");
}

/// A command or event for a deleted project writes nothing (the caller
/// was authorized from a cache that can outlive the removal); for a live
/// project it is written, and a retried apply hands back the same id.
#[sqlx::test]
async fn nothing_is_issued_or_recorded_for_a_deleted_project(pool: PgPool) {
    schema(&pool).await;
    let spec = serde_json::json!({ "units": [] });
    let apply = |project_id: uuid::Uuid| IssuedCommand {
        tenant_id: TENANT,
        project_id,
        node_id: Some(NODE),
        verb: InfraLifecycleVerb::Apply,
        running_policy: None,
        spec_json: Some(&spec),
        issued_by_pod: "worker-1",
    };
    let gone = uuid::Uuid::from_u128(9);
    assert_eq!(issue_command(&pool, &apply(gone)).await.unwrap(), None);
    let event = record_event(&pool, TENANT, gone, Some(NODE), "recovered", &serde_json::json!({})).await;
    assert_eq!(event.unwrap(), None);

    project_row(&pool, PROJECT, "ns").await;
    let first = issue_command(&pool, &apply(PROJECT)).await.unwrap().expect("issued");
    assert_eq!(issue_command(&pool, &apply(PROJECT)).await.unwrap(), Some(first), "a retried apply is the same command");
    let reactivate = IssuedCommand {
        node_id: None,
        verb: InfraLifecycleVerb::Reactivate,
        spec_json: None,
        ..apply(PROJECT)
    };
    assert!(issue_command(&pool, &reactivate).await.unwrap().is_some_and(|id| id != first));
    let event = record_event(&pool, TENANT, PROJECT, Some(NODE), "recovered", &serde_json::json!({})).await;
    assert!(event.unwrap().is_some());
}

/// The repair statement the decode error prints heals a row carrying a
/// status-only stub: before it the canonical decode fails, after it
/// the stub is gone and the real entries are intact.
#[sqlx::test]
async fn repair_sql_drops_status_only_stubs(pool: PgPool) {
    schema(&pool).await;
    node_row(
        &pool,
        "running",
        serde_json::json!({ "a": unit("running"), "stub": { "status": "stopped" } }),
    )
    .await;
    let (_, units) = row(&pool).await;
    let err = decode_units_json(units, PROJECT, NODE).unwrap_err().to_string();
    assert!(err.contains(&units_json_repair_sql(PROJECT, NODE)), "{err}");

    sqlx::query(&units_json_repair_sql(PROJECT, NODE)).execute(&pool).await.expect("repair");
    let (_, units) = row(&pool).await;
    let decoded = decode_units_json(units, PROJECT, NODE).expect("healed row decodes");
    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded["a"].status, Status::Running);
}
