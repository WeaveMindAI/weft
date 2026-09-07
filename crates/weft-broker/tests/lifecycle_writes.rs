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

use weft_broker::lifecycle_writes::{complete_command, set_status, FencedWrite};
use weft_broker_client::protocol::{
    decode_units_json, units_json_repair_sql, FailureStage, InfraNodeStatus as Status,
    SupervisorCommandCompleteRequest, SupervisorSetStatusRequest,
};

const TENANT: &str = "t1";
const PROJECT: &str = "p1";
const NODE: &str = "bridge";
const OWNER: &str = "supervisor-a";
const OTHER: &str = "supervisor-b";

async fn schema(pool: &PgPool) {
    weft_dispatcher::app::apply_core_schema(pool).await.expect("core schema");
}

/// `pod` holds the project's `infra_owner` lease for the next hour.
async fn lease(pool: &PgPool, pod: &str) {
    sqlx::query(
        "INSERT INTO infra_owner (project_id, supervisor_pod, namespace, tenant_id, leased_until_unix) \
         VALUES ($1, $2, 'ns', $3, EXTRACT(EPOCH FROM NOW())::BIGINT + 3600) \
         ON CONFLICT (project_id) DO UPDATE SET supervisor_pod = EXCLUDED.supervisor_pod, \
         leased_until_unix = EXCLUDED.leased_until_unix",
    )
    .bind(PROJECT)
    .bind(pod)
    .bind(TENANT)
    .execute(pool)
    .await
    .expect("lease");
}

/// An uncompleted lifecycle command targeting the node; returns its id.
async fn command(pool: &PgPool) -> i64 {
    let (id,): (i64,) = sqlx::query_as(
        "INSERT INTO infra_lifecycle_command \
         (tenant_id, project_id, node_id, verb, issued_by_pod, issued_at_unix) \
         VALUES ($1, $2, $3, 'stop', 'dispatcher', EXTRACT(EPOCH FROM NOW())::BIGINT) \
         RETURNING id",
    )
    .bind(TENANT)
    .bind(PROJECT)
    .bind(NODE)
    .fetch_one(pool)
    .await
    .expect("command");
    id
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
        project_id: PROJECT.into(),
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
