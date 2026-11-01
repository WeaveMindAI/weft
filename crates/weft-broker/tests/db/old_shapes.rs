//! A call in a shape this weft no longer reads, through the broker's real
//! router: what a worker the previous weft built sends after an update
//! (its program keeps running until its project is resynced).

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::json;
use tower::ServiceExt;

use weft_platform_traits::identity::{IdentityRefused, Principal};
use weft_task_store::db::Pool;

const WORKER_TOKEN: &str = "worker-token";
pub(crate) const CORE_TOKEN: &str = "core-token";

/// One project's worker, for its one token, and weft's own account.
struct OneWorker;

#[async_trait::async_trait]
impl weft_platform_traits::CallerIdentity for OneWorker {
    async fn verify(&self, bearer: &str, _audiences: &[String]) -> Result<Principal, IdentityRefused> {
        match bearer {
            WORKER_TOKEN => Ok(Principal::Worker { tenant: "tenant-a".into(), project: uuid::Uuid::from_u128(7) }),
            CORE_TOKEN => Ok(Principal::Core),
            _ => Err(IdentityRefused::BadSignature),
        }
    }
}

async fn broker(pool: Pool) -> axum::Router {
    weft_broker::router(broker_state(pool).await)
}

async fn broker_state(pool: Pool) -> Arc<weft_broker::BrokerState> {
    broker_state_on(pool, Arc::new(weft_platform_traits::object_store::fake::FakeObjectStore::new())).await
}

/// The broker's state on `object_store`, for a test that puts objects in
/// the bucket first.
pub(crate) async fn broker_state_on(pool: Pool, object_store: Arc<dyn weft_platform_traits::ObjectStore>) -> Arc<weft_broker::BrokerState> {
    let signals = weft_task_store::pg_signal::PgSignalWatch::start(
        pool.config(),
        weft_broker::state::BROKER_CHANNELS,
        weft_task_store::pg_signal::Listening::Always,
    )
    .await
    .expect("the broker listens");
    weft_broker::BrokerState::new(
        pool.clone(),
        pool.clone(),
        pool,
        signals,
        weft_broker::state::BrokerSettings {
            auth: weft_broker::AuthConfig { audiences: vec!["http://broker".into()] },
            identity: Arc::new(OneWorker),
            object_store,
            entitlements: Arc::new(weft_broker::entitlement::LocalEntitlementSource::from_env()),
            credentials: Arc::new(weft_broker::credential::FileCredentialSource::from_env()),
            app_provider: Arc::new(weft_broker::app_provider::FileAppProvider::from_env()),
            service_url: "http://broker".into(),
            internet: weft_platform_traits::config::InternetAddress::None,
            own_base_url: "http://broker".into(),
            container_files_base: None,
            object_store_public_internet: false,
            slot_key: weft_core::storage::slot::SlotKey::of(b"a test secret"),
            line_max_age: None,
            dispatcher_alarm: None,
            supervisor_booking: None,
            stopping: weft_task_store::stopping::Stopping::new(),
        },
    )
    .expect("the broker builds")
}

/// `/v1/caller/verify` as the previous weft's worker sent it: its headers
/// as a map, which this weft reads as a list of pairs.
fn old_caller_verify(bearer: &str) -> Request<Body> {
    let body = json!({
        "tenant": "tenant-a",
        "access_id": "00000000-0000-0000-0000-000000000001",
        "service": "api_key_auth",
        "method": "POST",
        "path": "chat",
        "headers": { "x-api-key": "k" },
        "body_b64": ""
    });
    Request::post("/v1/caller/verify")
        .header("authorization", format!("Bearer {bearer}"))
        .header("x-weft-replica", "worker-1")
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

pub(crate) async fn answer(router: &axum::Router, request: Request<Body>) -> (StatusCode, String) {
    let response = router.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (status, String::from_utf8_lossy(&body).into_owned())
}

/// The old shape is answered the one way every endpoint answers it: the
/// agreed status, the fix, and what did not read.
#[weft_db_macros::test]
async fn an_old_shaped_caller_verify_is_answered_naming_the_resync(pool: Pool) {
    let router = broker(pool).await;
    let (status, body) = answer(&router, old_caller_verify(WORKER_TOKEN)).await;
    assert_eq!(status, weft_broker_client::protocol::UNREADABLE_SHAPE);
    assert!(body.contains("run `weft resync` on its project"), "{body}");
    assert!(body.contains("/v1/caller/verify: invalid type: map, expected a sequence"), "{body}");
}

/// A worker of an older weft read a claimed run's record whole, from the
/// claim's answer or by asking for all of it; both of its calls are
/// answered naming the resync, before anything is claimed or read.
#[weft_db_macros::test]
async fn an_older_workers_claim_and_record_read_are_answered_naming_the_resync(pool: Pool) {
    let router = broker(pool).await;
    let run_id = uuid::Uuid::from_u128(9);
    for (path, missing) in [("/v1/run/claim", "reads"), ("/v1/journal/record_of", "after")] {
        let call = Request::post(path)
            .header("authorization", format!("Bearer {WORKER_TOKEN}"))
            .header("x-weft-replica", "worker-1")
            .header("content-type", "application/json")
            .body(Body::from(json!({ "run_id": run_id }).to_string()))
            .unwrap();
        let (status, body) = answer(&router, call).await;
        assert_eq!(status, weft_broker_client::protocol::UNREADABLE_SHAPE, "{path}: {body}");
        assert!(body.contains("run `weft resync` on its project"), "{path}: {body}");
        assert!(body.contains(&format!("{path}: missing field `{missing}`")), "{path}: {body}");
    }
}

/// A worker of an older weft sends batches whose runs carry no `secrets`:
/// answered naming the resync, never read as a run that made none.
#[weft_db_macros::test]
async fn an_older_workers_batch_without_secrets_is_answered_naming_the_resync(pool: Pool) {
    let router = broker(pool).await;
    let head = weft_journal::frame::BatchHead {
        durable: false,
        lane: 0,
        runs: vec![weft_journal::frame::RunHead {
            run_id: uuid::Uuid::from_u128(9),
            epoch: 1,
            first_seq: 1,
            row_sizes: Vec::new(),
            born: None,
            written: Default::default(),
            secrets: Vec::new(),
        }],
        programs: Vec::new(),
        events_version: weft_journal::stored::STORED_EVENTS.version(),
    };
    let mut header = serde_json::to_value(&head).unwrap();
    header["runs"][0].as_object_mut().unwrap().remove("secrets");
    let header = serde_json::to_vec(&header).unwrap();
    let mut body = (header.len() as u32).to_be_bytes().to_vec();
    body.extend_from_slice(&header);
    let call = Request::post(weft_broker_client::protocol::JOURNAL_RECORD_PATH)
        .header("authorization", format!("Bearer {WORKER_TOKEN}"))
        .header("x-weft-replica", "worker-1")
        .body(Body::from(body))
        .unwrap();
    let (status, body) = answer(&router, call).await;
    assert_eq!(status, weft_broker_client::protocol::UNREADABLE_SHAPE, "{body}");
    assert!(body.contains("run `weft resync` on its project"), "{body}");
    assert!(body.contains("missing field `secrets`"), "{body}");
}

/// A caller the broker does not know learns nothing of the shapes: its
/// token is refused first.
#[weft_db_macros::test]
async fn an_old_shaped_call_from_a_stranger_is_refused_as_a_stranger(pool: Pool) {
    let router = broker(pool).await;
    let (status, _) = answer(&router, old_caller_verify("somebody-else")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

/// `/v1/caller/verify` with `bearer`, carrying `body` as it is.
fn caller_verify_with(bearer: &str, body: &'static str) -> Request<Body> {
    Request::post("/v1/caller/verify")
        .header("authorization", format!("Bearer {bearer}"))
        .header("x-weft-replica", "worker-1")
        .header("content-type", "application/json")
        .body(Body::from(body))
        .unwrap()
}

/// A body that is not JSON at all is a `400` for a caller the broker
/// knows, and the identity refusal for one it does not, which so never
/// learns the endpoint is there and reads JSON.
#[weft_db_macros::test]
async fn a_body_that_is_not_json_is_refused_as_a_stranger_before_it_is_read(pool: Pool) {
    let router = broker(pool).await;
    let (status, body) = answer(&router, caller_verify_with(WORKER_TOKEN, "{not json")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(body.contains("/v1/caller/verify is not JSON"), "{body}");
    let (status, body) = answer(&router, caller_verify_with("somebody-else", "{not json")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "{body}");
    assert!(!body.contains("JSON"), "{body}");
}

/// A holder's check in a shape this broker does not read is answered as
/// every call of another shape is (which the alarm drops as a call that
/// can never be taken), and one that reads is handled.
#[weft_db_macros::test]
async fn a_holders_check_of_another_shape_is_answered_as_unreadable(pool: Pool) {
    weft_task_store::apply_groups(&pool, weft_dispatcher::app::DISPATCHER_GROUPS).await.expect("core schema");
    let router = weft_broker::held_check_route(broker_state(pool).await);
    let check = |body: serde_json::Value| {
        Request::post(weft_platform_traits::HELD_CHECK_PATH)
            .header("authorization", format!("Bearer {CORE_TOKEN}"))
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap()
    };
    let (status, body) = answer(&router, check(json!({ "at_unix_ms": 1, "body": { "holder": "h-1" } }))).await;
    assert_eq!(status, weft_broker_client::protocol::UNREADABLE_SHAPE, "{body}");
    assert!(body.contains("missing field `replica`"), "{body}");
    assert_eq!(
        weft_platform_traits::alarm::WakeRefusal::of_status(status.as_u16()),
        weft_platform_traits::alarm::WakeRefusal::Permanent,
        "the alarm never delivers it again"
    );
    let (status, body) =
        answer(&router, check(json!({ "at_unix_ms": 1, "body": { "replica": "h-1", "generation": 1, "due_at_ms": 1 } }))).await;
    assert_eq!((status, body.as_str()), (StatusCode::OK, "{}"));
}
