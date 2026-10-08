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
    complete_command, issue_command, next_command, record_event, set_status, set_waiting, sync_ownership,
    unowned_work_waiting, FencedWrite, IssuedCommand,
};
use weft_broker_client::protocol::{
    decode_units_json, InfraLifecycleVerb, units_json_repair_sql, FailureStage, InfraNodeStatus as Status,
    SupervisorCommandCompleteRequest, SupervisorSetStatusRequest, SupervisorSetWaitingRequest,
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
    command_on(pool, project, None).await
}

/// An uncompleted lifecycle command of `project` on one copy of the node:
/// the shared one (`None`) or an instance's.
async fn command_on(pool: &PgPool, project: uuid::Uuid, instance: Option<&str>) -> i64 {
    let (id,): (i64,) = sqlx::query_as(
        "INSERT INTO infra_lifecycle_command \
         (tenant_id, project_id, node_id, verb, issued_by_replica, issued_at_unix, instance_id) \
         VALUES ($1, $2, $3, 'stop', 'dispatcher', EXTRACT(EPOCH FROM NOW())::BIGINT, $4) \
         RETURNING id",
    )
    .bind(TENANT)
    .bind(project)
    .bind(NODE)
    .bind(instance)
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

/// What an apply waits on is recorded by the owner under its apply only,
/// refused (the row untouched) for a process that lost the project, and
/// cleared, with when it began, once the copy leaves provisioning.
#[sqlx::test]
async fn what_an_apply_waits_on_is_its_own_and_ends_with_it(pool: PgPool) {
    schema(&pool).await;
    lease(&pool, OWNER).await;
    let stop = command(&pool).await;
    let (cmd,): (i64,) = sqlx::query_as(
        "INSERT INTO infra_lifecycle_command (tenant_id, project_id, node_id, verb, issued_by_replica, issued_at_unix) \
         VALUES ($1, $2, $3, 'apply', 'dispatcher', EXTRACT(EPOCH FROM NOW())::BIGINT) RETURNING id",
    )
    .bind(TENANT)
    .bind(PROJECT)
    .bind(NODE)
    .fetch_one(&pool)
    .await
    .unwrap();
    node_row(&pool, "provisioning", serde_json::json!({ "a": unit("provisioning") })).await;
    sqlx::query("UPDATE infra_node SET provisioning_since_unix = 100 WHERE project_id = $1").bind(PROJECT).execute(&pool).await.unwrap();
    let waiting = |replica: &str, text: &str| SupervisorSetWaitingRequest {
        replica: replica.into(),
        command_id: cmd,
        project_id: PROJECT,
        node_id: NODE.into(),
        instance: None,
        waiting: text.into(),
    };
    let read = || async {
        let (w,): (Option<String>,) = sqlx::query_as("SELECT waiting_on FROM infra_node WHERE project_id = $1")
            .bind(PROJECT)
            .fetch_one(&pool)
            .await
            .unwrap();
        w
    };
    assert_eq!(set_waiting(&pool, &waiting(OTHER, "theirs")).await.unwrap(), FencedWrite::Displaced);
    assert_eq!(read().await, None);
    assert_eq!(
        set_waiting(&pool, &SupervisorSetWaitingRequest { command_id: stop, ..waiting(OWNER, "under a stop") }).await.unwrap(),
        FencedWrite::Gone,
        "only an apply records what it waits on"
    );
    assert_eq!(set_waiting(&pool, &waiting(OWNER, "a: its machine's agent does not answer yet")).await.unwrap(), FencedWrite::Applied);
    assert_eq!(read().await.as_deref(), Some("a: its machine's agent does not answer yet"));
    // The start fails: its progress goes with it, so a later start never
    // shows this one's.
    let failed = SupervisorSetStatusRequest { command_id: Some(cmd), ..stamp(OWNER, None, None, Status::Failed) };
    assert_eq!(set_status(&pool, &failed).await.unwrap(), FencedWrite::Applied);
    assert_eq!(read().await, None);
    let (since,): (Option<i64>,) = sqlx::query_as("SELECT provisioning_since_unix FROM infra_node WHERE project_id = $1")
        .bind(PROJECT)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(since, None);
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

/// The owner is handed its projects' commands oldest first. A command it
/// is already running is never handed out again, a command waits while an
/// older one on the same copy is uncompleted, commands on different copies
/// of one node are handed out side by side, and a project it does not own
/// is never handed out at all.
#[sqlx::test]
async fn next_command_runs_copies_side_by_side_and_one_copy_in_order(pool: PgPool) {
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
    let ada = command_on(&pool, PROJECT, Some("ada")).await;
    let bob = command_on(&pool, PROJECT, Some("bob")).await;
    let ada_again = command_on(&pool, PROJECT, Some("ada")).await;
    let other_project = command_of(&pool, second).await;

    let next = |busy: Vec<i64>| {
        let pool = pool.clone();
        async move { next_command(&pool, OWNER, &busy).await.unwrap().map(|c| c.id) }
    };
    assert_eq!(next(vec![]).await, Some(ada));
    // Running ada's command: bob's copy is another copy, so it goes now.
    assert_eq!(next(vec![ada]).await, Some(bob));
    // ada's second command waits for her first; the other project goes.
    assert_eq!(next(vec![ada, bob]).await, Some(other_project));
    assert_eq!(next(vec![ada, bob, other_project]).await, None);
    // The other supervisor's project is never handed out.
    assert_ne!(next(vec![]).await, Some(foreign_cmd));

    complete_command(
        &pool,
        &SupervisorCommandCompleteRequest {
            replica: OWNER.into(),
            command_id: ada,
            error: None,
            cancelled: false,
        },
    )
    .await
    .unwrap();
    assert_eq!(next(vec![bob, other_project]).await, Some(ada_again));
}

/// A command on every copy (the project going) overlaps every copy's
/// command, so it waits for the older ones and the younger ones wait for it.
#[sqlx::test]
async fn a_command_on_every_copy_waits_for_and_holds_up_each_copy(pool: PgPool) {
    schema(&pool).await;
    project_row(&pool, PROJECT, true).await;
    lease(&pool, OWNER).await;
    let ada = command_on(&pool, PROJECT, Some("ada")).await;
    let (every,): (i64,) = sqlx::query_as(
        "INSERT INTO infra_lifecycle_command \
         (tenant_id, project_id, node_id, verb, issued_by_replica, issued_at_unix, every_copy) \
         VALUES ($1, $2, NULL, 'terminate', 'dispatcher', EXTRACT(EPOCH FROM NOW())::BIGINT, TRUE) \
         RETURNING id",
    )
    .bind(TENANT)
    .bind(PROJECT)
    .fetch_one(&pool)
    .await
    .expect("every-copy command");
    let bob = command_on(&pool, PROJECT, Some("bob")).await;

    let next = |busy: Vec<i64>| {
        let pool = pool.clone();
        async move { next_command(&pool, OWNER, &busy).await.unwrap().map(|c| c.id) }
    };
    let done = |command_id: i64| {
        let pool = pool.clone();
        async move {
            let req = SupervisorCommandCompleteRequest { replica: OWNER.into(), command_id, error: None, cancelled: false };
            complete_command(&pool, &req).await.unwrap();
        }
    };
    assert_eq!(next(vec![]).await, Some(ada));
    assert_eq!(next(vec![ada]).await, None, "everything waits on ada");
    done(ada).await;
    assert_eq!(next(vec![]).await, Some(every));
    assert_eq!(next(vec![every]).await, None, "bob waits on the project going");
    done(every).await;
    assert_eq!(next(vec![]).await, Some(bob));
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

/// A tick says when its supervisor next has something to do: never over a
/// project of its own whose infra runs (a machine saying how its unit
/// stands changed wakes it instead), at the lapse of a sibling's lease over
/// one with infra copies expected to run (the word that one changed may
/// have woken this replica, not the sibling; a stopped copy says nothing),
/// and now once a command waits on one of its own.
#[sqlx::test]
async fn a_tick_says_when_there_is_next_something_to_look_at(pool: PgPool) {
    schema(&pool).await;
    project_row(&pool, PROJECT, true).await;
    let declared = sync_ownership(&pool, OWNER, &[]).await.unwrap();
    assert_eq!(declared.owned.len(), 1, "declared infra is ownable");
    assert!(!declared.owns_work && declared.others_lapse_in_secs.is_none(), "but gives nothing to look at: {declared:?}");

    node_row(&pool, "running", serde_json::json!({})).await;
    let running = sync_ownership(&pool, OWNER, &[]).await.unwrap();
    assert!(!running.owns_work && running.others_lapse_in_secs.is_none(), "infra that runs needs no look on a clock: {running:?}");

    lease(&pool, OTHER).await;
    let sibling = sync_ownership(&pool, OWNER, &[]).await.unwrap();
    assert!(sibling.owned.is_empty() && !sibling.owns_work, "a sibling holds the lease");
    let lapse = sibling.others_lapse_in_secs.expect("its lease over a project with infra copies may lapse");
    assert!((3590..=3600).contains(&lapse), "the sibling's lease lapses in an hour: {lapse}");
    sqlx::query("UPDATE infra_node SET status = 'stopped'").execute(&pool).await.unwrap();
    let stopped = sync_ownership(&pool, OWNER, &[]).await.unwrap();
    assert!(stopped.others_lapse_in_secs.is_none(), "a stopped copy says nothing to wake for: {stopped:?}");

    command_of(&pool, PROJECT).await;
    lease(&pool, OWNER).await;
    let own = sync_ownership(&pool, OWNER, &[]).await.unwrap();
    assert!(own.owns_work && own.others_lapse_in_secs.is_none(), "{own:?}");
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
        spec_json: Some(&spec),
        issued_by_replica: "worker-1",
    };
    let id = issue_command(&pool, &apply).await.unwrap().expect("issued");
    let shared = IssuedCommand { copies: &Copies::Shared, ..apply };
    assert_ne!(issue_command(&pool, &shared).await.unwrap(), Some(id), "the shared copy's apply is another command");
    let claimed = next_command(&pool, OWNER, &[]).await.unwrap().expect("a command");
    assert_eq!((claimed.id, claimed.copies), (id, Copies::Instance(ada)));
}

/// A take-down that drains first is the dispatcher's until its running
/// work is done: the supervisor is handed it only once the drain hands it
/// over, and a command on the same copy behind it waits for it.
#[sqlx::test]
async fn a_command_still_draining_is_not_handed_to_the_supervisor(pool: PgPool) {
    schema(&pool).await;
    lease(&pool, OWNER).await;
    let draining = command(&pool).await;
    let behind = command(&pool).await;
    sqlx::query("UPDATE infra_lifecycle_command SET drain_by_unix = EXTRACT(EPOCH FROM NOW())::BIGINT + 60 WHERE id = $1")
        .bind(draining)
        .execute(&pool)
        .await
        .unwrap();
    assert!(next_command(&pool, OWNER, &[]).await.unwrap().is_none(), "draining, and the one behind it waits");
    sqlx::query("UPDATE infra_lifecycle_command SET drain_by_unix = NULL WHERE id = $1").bind(draining).execute(&pool).await.unwrap();
    assert_eq!(next_command(&pool, OWNER, &[]).await.unwrap().map(|c| c.id), Some(draining));
    assert_eq!(next_command(&pool, OWNER, &[draining]).await.unwrap().map(|c| c.id), None, "{behind} waits for it");
}
