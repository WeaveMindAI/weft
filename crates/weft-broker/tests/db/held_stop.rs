//! A held call on a broker copy that starts stopping, through the
//! broker's real router: it answers at once what its wait running out
//! answers, so its caller asks again on a copy that runs.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use weft_broker_client::protocol::{SupervisorClaim, SupervisorClaimCommandRequest};
use weft_platform_traits::object_store::fake::FakeObjectStore;
use weft_task_store::db::Pool;

use crate::old_shapes::{answer, broker_state_on, CORE_TOKEN};

/// A supervisor's claim, held up to a minute for a command to be issued.
fn held_claim() -> Request<Body> {
    let req = SupervisorClaimCommandRequest {
        claimer_replica: "supervisor-1".into(),
        busy_commands: Vec::new(),
        cancelling: Vec::new(),
        wait_ms: 60_000,
    };
    Request::post("/v1/supervisor/claim_command")
        .header("authorization", format!("Bearer {CORE_TOKEN}"))
        .header(weft_platform_traits::identity::REPLICA_HEADER, "supervisor-1")
        .header(weft_platform_traits::identity::ROLE_HEADER, "supervisor")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&req).unwrap()))
        .unwrap()
}

#[weft_db_macros::test]
async fn a_held_call_answers_at_once_when_its_copy_starts_stopping(pool: Pool) {
    weft_task_store::apply_groups(&pool, weft_dispatcher::app::DISPATCHER_GROUPS).await.expect("core schema");
    let state = broker_state_on(pool, Arc::new(FakeObjectStore::new())).await;
    let router = weft_broker::router(state.clone());
    let held = tokio::spawn(async move { answer(&router, held_claim()).await });
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(!held.is_finished(), "nothing is issued, so the claim holds");

    state.stopping.begin();
    let (status, body) = tokio::time::timeout(Duration::from_secs(5), held)
        .await
        .expect("answered at the stop, not at the end of its minute")
        .unwrap();
    assert_eq!(status, StatusCode::OK, "{body}");
    let claim: SupervisorClaim = serde_json::from_str(&body).unwrap();
    assert!(matches!(claim, SupervisorClaim::Nothing), "what a wait that ran out answers: {body}");
}
