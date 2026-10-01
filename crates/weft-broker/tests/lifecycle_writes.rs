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
    complete_command, issue_command, next_command, record_event, set_status, sync_ownership,
    unowned_work_waiting, FencedWrite, IssuedCommand,
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

/// `process` holds the project's `infra_owner` lease for the next hour.
async fn lease(pool: &PgPool, replica: &str) {
    lease_of(pool, PROJECT, replica).await
}

/// `process` holds `project`'s `infra_owner` lease for the next hour.
async fn lease_of(pool: &PgPool, project: uuid::Uuid, replica: &str) {
    sqlx::query(
        "INSERT INTO infra_owner (project_id, supervisor_replica, tenant_id, leased_until_unix) \
         VALUES ($1, $2, $3, EXTRACT(EPOCH FROM NOW())::BIGINT + 3600) \
         ON CONFLICT (project_id) DO UPDATE SET supervisor_replica = EXCLUDED.supervisor_replica, \
         leased_until_unix = EXCLUDED.leased_until_unix",
    )
    .bind(project)
    .bind(replica)
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
         (tenant_id, project_id, node_id, verb, issued_by_replica, issued_at_unix) \
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

/// A `project` row; `has_infra` false = no supervisor may take it (it
/// declares no infra and runs none).
async fn project_row(pool: &PgPool, id: uuid::Uuid, has_infra: bool) {
    sqlx::query(
        "INSERT INTO project (id, name, tenant_id, status, project_json, updated_at, has_infra) \
         VALUES ($1, 'p', 'tenant', 'inactive', '{}', 0, $2)",
    )
    .bind(id)
    .bind(has_infra)
    .execute(pool)
    .await
    .expect("project row");
}

fn unit(status: &str) -> serde_json::Value {
    serde_json::json!({
        "status": status,
        "stop_behavior": { "kind": "stop" },
        "flaky_after_seconds": 30,
        "recovery_after_seconds": 30
    })
}

async fn node_row(pool: &PgPool, status: &str, units: serde_json::Value) {
    sqlx::query(
        "INSERT INTO infra_node (project_id, node_id, copy_id, status, units_json) \
         VALUES ($1, $2, 'inst1', $3, $4)",
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

fn stamp(replica: &str, command_id: Option<i64>, unit: Option<&str>, status: Status) -> SupervisorSetStatusRequest {
    SupervisorSetStatusRequest {
        replica: replica.into(),
        command_id,
        project_id: PROJECT,
        node_id: NODE.into(),
        unit: unit.map(String::from),
        status,
        failure_stage: None,
        failure_message: None,
        instance: None,
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
    assert_eq!(units["a"]["stop_behavior"]["kind"], "stop");
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

/// A stamp by a process that does not hold the lease is Displaced (not
/// Gone: the target is there, the writer is not the owner), and the
/// row is untouched.
#[sqlx::test]
async fn stamp_by_a_displaced_replica_is_displaced(pool: PgPool) {
    schema(&pool).await;
    lease(&pool, OWNER).await;
    let cmd = command(&pool).await;
    node_row(&pool, "running", serde_json::json!({ "a": unit("running") })).await;

    let out = set_status(&pool, &stamp(OTHER, Some(cmd), Some("a"), Status::Stopped)).await.unwrap();
    assert_eq!(out, FencedWrite::Displaced);
    assert_eq!(row(&pool).await.0, "running");
    // Node-wide from the displaced process: same answer.
    let out = set_status(&pool, &stamp(OTHER, Some(cmd), None, Status::Terminating)).await.unwrap();
    assert_eq!(out, FencedWrite::Displaced);
    assert_eq!(row(&pool).await.0, "running");
}

/// The autonomous (health) stamp is fenced by ownership too: a process that
/// lost the project cannot stamp it while no command is in flight.
#[sqlx::test]
async fn autonomous_stamp_by_a_displaced_replica_is_displaced(pool: PgPool) {
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
        &SupervisorCommandCompleteRequest { replica: OWNER.into(), command_id: cmd, error: None, cancelled: false },
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
        &SupervisorCommandCompleteRequest { replica: OWNER.into(), command_id: cmd, error: None, cancelled: false },
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
/// a displaced process's is Displaced, and an unknown id is Gone.
#[sqlx::test]
async fn command_completion_is_once_and_owner_only(pool: PgPool) {
    schema(&pool).await;
    lease(&pool, OWNER).await;
    let cmd = command(&pool).await;
    let req = |replica: &str, id: i64, error: Option<&str>| SupervisorCommandCompleteRequest {
        replica: replica.into(),
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
        project_row(&pool, project, true).await;
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
            replica: OWNER.into(),
            command_id: first_a,
            error: None,
            cancelled: false,
        },
    )
    .await
    .unwrap();
    assert_eq!(next(vec![second]).await, Some(second_a));
}

/// A command nobody holds a lease over is unowned work; once a live
/// lease covers it, or the command completes, it is not.
#[sqlx::test]
async fn unowned_work_is_a_waiting_command_on_an_unleased_project(pool: PgPool) {
    schema(&pool).await;
    project_row(&pool, PROJECT, true).await;
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

/// A tick reports the projects it took on: a fresh claim, and the process's
/// own lease revived after it lapsed. A lease it merely renewed is not
/// news, and neither is a project another process holds.
#[sqlx::test]
async fn a_tick_reports_the_projects_it_took_on(pool: PgPool) {
    schema(&pool).await;
    let foreign = uuid::Uuid::from_u128(3);
    project_row(&pool, PROJECT, true).await;
    project_row(&pool, foreign, true).await;
    lease_of(&pool, foreign, OTHER).await;
    let claimed = || {
        let pool = pool.clone();
        async move { sync_ownership(&pool, OWNER, &[]).await.unwrap().claimed }
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

/// A project with no infra, no row and nothing on the host is owned only
/// while a supervisor command waits on it: claimed for it, renewed and
/// reported owned until it completes, then no longer renewed.
#[sqlx::test]
async fn a_pending_command_alone_makes_a_project_owned(pool: PgPool) {
    schema(&pool).await;
    let idle = uuid::Uuid::from_u128(9);
    project_row(&pool, idle, false).await;
    let sync = || {
        let pool = pool.clone();
        async move { sync_ownership(&pool, OWNER, &[]).await.unwrap() }
    };
    assert!(sync().await.owned.is_empty(), "nothing to own, nothing claimed");

    let cmd = command_of(&pool, idle).await;
    assert!(unowned_work_waiting(&pool).await.unwrap(), "the command waits for an owner");
    let synced = sync().await;
    assert_eq!(synced.claimed, vec![idle]);
    assert_eq!(synced.owned.iter().map(|p| p.project_id).collect::<Vec<_>>(), vec![idle]);
    assert_eq!(next_command(&pool, OWNER, &[]).await.unwrap().map(|c| c.id), Some(cmd));

    // Renewed while pending: expire it, and the tick takes it back.
    sqlx::query("UPDATE infra_owner SET leased_until_unix = 0").execute(&pool).await.unwrap();
    assert_eq!(sync().await.claimed, vec![idle], "a pending command's lease is renewed");

    complete_command(
        &pool,
        &SupervisorCommandCompleteRequest { replica: OWNER.into(), command_id: cmd, error: None, cancelled: false },
    )
    .await
    .unwrap();
    sqlx::query("UPDATE infra_owner SET leased_until_unix = 0").execute(&pool).await.unwrap();
    assert!(sync().await.owned.is_empty(), "with the command done, the lease is not renewed");
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
        copies: &weft_core::instance::Copies::Shared,
        verb: InfraLifecycleVerb::Apply,
        running_policy: None,
        spec_json: Some(&spec),
        issued_by_replica: "worker-1",
    };
    let gone = uuid::Uuid::from_u128(9);
    assert_eq!(issue_command(&pool, &apply(gone)).await.unwrap(), None);
    let event = record_event(&pool, TENANT, gone, Some(NODE), None, "recovered", &serde_json::json!({})).await;
    assert_eq!(event.unwrap(), None);

    project_row(&pool, PROJECT, true).await;
    let first = issue_command(&pool, &apply(PROJECT)).await.unwrap().expect("issued");
    assert_eq!(issue_command(&pool, &apply(PROJECT)).await.unwrap(), Some(first), "a retried apply is the same command");
    let reactivate = IssuedCommand {
        node_id: None,
        verb: InfraLifecycleVerb::Reactivate,
        spec_json: None,
        ..apply(PROJECT)
    };
    assert!(issue_command(&pool, &reactivate).await.unwrap().is_some_and(|id| id != first));
    let event = record_event(&pool, TENANT, PROJECT, Some(NODE), None, "recovered", &serde_json::json!({})).await;
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

/// An instance's copy is its own row: a stamp naming the instance moves
/// only that copy, and a command naming copies hands the same copies
/// back to the supervisor that claims it.
#[sqlx::test]
async fn an_instances_copy_is_stamped_and_commanded_on_its_own(pool: PgPool) {
    use weft_core::instance::{Copies, InstanceId};
    schema(&pool).await;
    project_row(&pool, PROJECT, true).await;
    lease(&pool, OWNER).await;
    let ada = InstanceId::new("ada").unwrap();
    node_row(&pool, "running", serde_json::json!({ "a": unit("running") })).await;
    sqlx::query(
        "INSERT INTO infra_node (project_id, node_id, copy_id, status, units_json, instance_id) \
         VALUES ($1, $2, 'inst-ada', 'running', $3, 'ada')",
    )
    .bind(PROJECT)
    .bind(NODE)
    .bind(serde_json::json!({ "a": unit("running") }))
    .execute(&pool)
    .await
    .expect("ada's copy");
    let duplicate = sqlx::query(
        "INSERT INTO infra_node (project_id, node_id, status, instance_id) VALUES ($1, $2, 'running', 'ada')",
    )
    .bind(PROJECT)
    .bind(NODE)
    .execute(&pool)
    .await;
    assert!(duplicate.is_err(), "one copy per instance");

    let stamp_ada = SupervisorSetStatusRequest { instance: Some(ada.clone()), ..stamp(OWNER, None, Some("a"), Status::Flaky) };
    assert_eq!(set_status(&pool, &stamp_ada).await.unwrap(), FencedWrite::Applied);
    let statuses: Vec<(Option<String>, String)> =
        sqlx::query_as("SELECT instance_id, status FROM infra_node WHERE project_id = $1 ORDER BY instance_id NULLS FIRST")
            .bind(PROJECT)
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(statuses, vec![(None, "running".to_string()), (Some("ada".to_string()), "flaky".to_string())]);
    let stamp_bob = SupervisorSetStatusRequest { instance: Some(InstanceId::new("bob").unwrap()), ..stamp_ada.clone() };
    assert_eq!(set_status(&pool, &stamp_bob).await.unwrap(), FencedWrite::Gone, "bob has no copy");

    let spec = serde_json::json!({ "units": [] });
    let copies = Copies::Instance(ada.clone());
    let apply = IssuedCommand {
        tenant_id: TENANT,
        project_id: PROJECT,
        node_id: Some(NODE),
        copies: &copies,
        verb: InfraLifecycleVerb::Apply,
        running_policy: None,
        spec_json: Some(&spec),
        issued_by_replica: "worker-1",
    };
    let id = issue_command(&pool, &apply).await.unwrap().expect("issued");
    let shared = IssuedCommand { copies: &Copies::Shared, ..apply };
    assert_ne!(issue_command(&pool, &shared).await.unwrap(), Some(id), "the shared copy's apply is another command");
    let claimed = next_command(&pool, OWNER, &[]).await.unwrap().expect("a command");
    assert_eq!((claimed.id, claimed.copies), (id, Copies::Instance(ada)));
}


/// What `running_policy=wait` waits on: the live runs a copy serves. A
/// finished run and a run parked on a resume hold nothing; an instance's
/// copy counts only that instance's runs, the shared copy every run.
#[sqlx::test]
async fn the_running_count_is_the_live_runs_a_copy_serves(pool: PgPool) {
    use weft_core::instance::{Copies, InstanceId};
    schema(&pool).await;
    let run = |execution_id: &'static str, instance: Option<&'static str>| {
        let pool = pool.clone();
        async move {
            sqlx::query(
                "INSERT INTO execution (execution_id, project_id, tenant_id, started_at_unix, phase, instance_id) \
                 VALUES ($1, $2, $3, 0, 'fire', $4)",
            )
            .bind(execution_id)
            .bind(PROJECT)
            .bind(TENANT)
            .bind(instance)
            .execute(&pool)
            .await
            .unwrap();
        }
    };
    run("11111111-1111-1111-1111-111111111111", None).await;
    run("22222222-2222-2222-2222-222222222222", Some("ada")).await;
    run("33333333-3333-3333-3333-333333333333", Some("ada")).await;
    let count = |copies: Copies| {
        let pool = pool.clone();
        async move { weft_broker::lifecycle_writes::live_run_count(&pool, PROJECT, &copies).await.unwrap() }
    };
    assert_eq!(count(Copies::Shared).await, 3);
    assert_eq!(count(Copies::Instance(InstanceId::new("ada").unwrap())).await, 2);

    // One of ada's runs ends, the other parks on a form.
    sqlx::query(
        "INSERT INTO exec_event (execution_id, kind, payload_json, created_at) \
         VALUES ('22222222-2222-2222-2222-222222222222', 'execution_completed', '{}', 1)",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO signal (token, tenant_id, project_id, node_id, execution_id, is_resume, spec_json, created_at) \
         VALUES ('form', $1, $2, 'ask', '33333333-3333-3333-3333-333333333333', TRUE, '{}', 1)",
    )
    .bind(TENANT)
    .bind(PROJECT)
    .execute(&pool)
    .await
    .unwrap();
    assert_eq!(count(Copies::Instance(InstanceId::new("ada").unwrap())).await, 0);
    assert_eq!(count(Copies::Every).await, 1, "the shared copy's run is still live");
}
