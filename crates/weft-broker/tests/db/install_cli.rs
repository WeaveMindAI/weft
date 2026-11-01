//! The weft CLI the install workflow stored, through the broker's real
//! router: weft's own account gets a link to it with its sha256, and a
//! tree with none stored, or a name that is no tree, is refused plainly.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use weft_platform_traits::object_store::fake::FakeObjectStore;
use weft_platform_traits::ObjectStore as _;
use weft_task_store::db::Pool;

use crate::old_shapes::{answer, broker_state_on, CORE_TOKEN};

fn asked(tree: &str) -> Request<Body> {
    Request::get(format!("/v1/storage/admin/install-cli/{tree}"))
        .header("authorization", format!("Bearer {CORE_TOKEN}"))
        .header(weft_platform_traits::identity::REPLICA_HEADER, "dispatcher-1")
        .header(weft_platform_traits::identity::ROLE_HEADER, "dispatcher")
        .body(Body::empty())
        .unwrap()
}

#[weft_db_macros::test]
async fn the_stored_cli_is_linked_with_its_sha256_and_a_missing_one_is_a_404(pool: Pool) {
    let bucket = Arc::new(FakeObjectStore::new());
    let tree = "ab".repeat(20);
    let (cli, sha256) = weft_core::install::install_cli_keys(&tree);
    bucket.put(&cli, bytes::Bytes::from_static(b"the cli")).await.unwrap();
    bucket.put(&sha256, bytes::Bytes::from_static(b"5eed\n")).await.unwrap();
    let router = weft_broker::router(broker_state_on(pool, bucket).await);

    let (status, body) = answer(&router, asked(&tree)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let stored: weft_core::install::InstallCli = serde_json::from_str(&body).unwrap();
    assert_eq!(stored.sha256, "5eed", "read as written, without its line break");
    assert!(stored.url.contains(&cli), "{}", stored.url);

    let (status, body) = answer(&router, asked(&"cd".repeat(20))).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert!(body.contains("no weft CLI is stored"), "{body}");

    let (status, body) = answer(&router, asked("..%2Fsecret")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
}
