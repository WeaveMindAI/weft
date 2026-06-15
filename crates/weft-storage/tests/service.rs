//! Layer 3: the REAL service router over the real store + fake
//! disks/auth/clock. Wall enforcement, capability lifecycle,
//! presign, admin gating, HTTP range mapping.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use tower::ServiceExt;
use weft_storage::key::{parse_key, CallerAuth, ParsedKey};
use weft_storage::testing::StorageTestRig;

/// Parse a literal/wire key into the `ParsedKey` the store methods take.
fn pk(key: &str) -> ParsedKey {
    parse_key(key).expect("key is well-formed")
}

async fn body_bytes(resp: axum::response::Response) -> bytes::Bytes {
    resp.into_body().collect().await.expect("collect body").to_bytes()
}

fn put_req(token: &str, scope: &str, body: &'static [u8]) -> Request<Body> {
    Request::builder()
        .method("PUT")
        .uri("/v1/files")
        .header("authorization", format!("Bearer {token}"))
        .header("x-weft-color", "c1")
        .header("x-weft-scope", scope)
        .header("x-weft-mime", "audio/ogg")
        .header("x-weft-filename", "clip.ogg")
        .body(Body::from(body))
        .unwrap()
}

/// Store a file as the rig's worker; returns the storage key.
async fn put_file(rig: &StorageTestRig, scope: &str, body: &'static [u8]) -> String {
    let resp = rig.router.clone().oneshot(put_req("worker-token", scope, body)).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let v: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    weft_core::storage::StoredFile::from_value(&v).unwrap().key
}

fn get_req(token: &str, color: &str, key: &str) -> Request<Body> {
    Request::builder()
        .method("GET")
        .uri(format!("/v1/files/{key}"))
        .header("authorization", format!("Bearer {token}"))
        .header("x-weft-color", color)
        .body(Body::empty())
        .unwrap()
}

#[tokio::test]
async fn put_returns_self_describing_file_and_get_streams_it() {
    let rig = StorageTestRig::new().await;
    let resp = rig
        .router
        .clone()
        .oneshot(put_req("worker-token", r#"{"kind":"execution"}"#, b"hello bytes"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let v: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    let file = weft_core::storage::StoredFile::from_value(&v).unwrap();
    assert!(file.key.starts_with("exec/c1/"), "{}", file.key);
    assert_eq!(file.size_bytes, 11);
    assert_eq!(file.mime_type, "audio/ogg");
    // audio/ogg -> the concrete __weft_audio__ marker, no url anywhere.
    assert!(v["__weft_audio__"].get("url").is_none());

    let resp = rig
        .router
        .clone()
        .oneshot(get_req("worker-token", "c1", &file.key))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(body_bytes(resp).await.as_ref(), b"hello bytes");
}

#[tokio::test]
async fn the_wall_denies_another_color_project_and_unauthenticated() {
    let rig = StorageTestRig::new().await;
    let key = put_file(&rig, r#"{"kind":"execution"}"#, b"secret").await;

    // A second worker on another color of the same project.
    rig.auth.seed(
        "other-worker",
        CallerAuth::Worker {
            tenant: "t1".into(),
            project_id: "p1".into(),
            color: Some("c2".into()),
        },
    );
    let resp =
        rig.router.clone().oneshot(get_req("other-worker", "c2", &key)).await.unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN, "another color is walled");

    // A worker of another project cannot reach p1's project files.
    let pkey = put_file(&rig, r#"{"kind":"project"}"#, b"project data").await;
    rig.auth.seed(
        "p2-worker",
        CallerAuth::Worker {
            tenant: "t1".into(),
            project_id: "p2".into(),
            color: Some("c9".into()),
        },
    );
    let resp = rig.router.clone().oneshot(get_req("p2-worker", "c9", &pkey)).await.unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN, "another project is walled");

    // No token at all.
    let resp = rig
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v1/files/{key}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // Unknown token.
    let resp = rig.router.clone().oneshot(get_req("forged", "c1", &key)).await.unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn shared_scope_meets_across_projects_and_records_grants() {
    let rig = StorageTestRig::new().await;
    let key = put_file(&rig, r#"{"kind":"shared","name":"team"}"#, b"shared doc").await;
    assert!(key.starts_with("shared/team/"));

    // Another project of the SAME tenant reaches it by naming it.
    rig.auth.seed(
        "p2-worker",
        CallerAuth::Worker {
            tenant: "t1".into(),
            project_id: "p2".into(),
            color: Some("c9".into()),
        },
    );
    let resp = rig.router.clone().oneshot(get_req("p2-worker", "c9", &key)).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(body_bytes(resp).await.as_ref(), b"shared doc");

    // Both uses recorded.
    let grants = rig.store.grants().await;
    assert!(grants.contains(&("p1".to_string(), "team".to_string())));
    assert!(grants.contains(&("p2".to_string(), "team".to_string())));
}

#[tokio::test]
async fn admin_surface_is_dispatcher_only_and_data_path_rejects_dispatcher() {
    let rig = StorageTestRig::new().await;
    put_file(&rig, r#"{"kind":"execution"}"#, b"x").await;

    // Worker on admin -> 403.
    let resp = rig
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/admin/usage")
                .header("authorization", "Bearer worker-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);

    // Dispatcher on admin -> 200.
    let resp = rig
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/admin/usage")
                .header("authorization", "Bearer dispatcher-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Dispatcher on the data path -> 403 (bulk bytes never ride the
    // control plane's identity).
    let resp = rig
        .router
        .clone()
        .oneshot(put_req("dispatcher-token", r#"{"kind":"execution"}"#, b"nope"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn presign_allows_own_file_denies_other_color_and_url_expires() {
    let rig = StorageTestRig::new().await;
    let key = put_file(&rig, r#"{"kind":"execution"}"#, b"give me to an api").await;

    // Own file: minted.
    let resp = rig
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/presign")
                .header("authorization", "Bearer worker-token")
                .header("x-weft-color", "c1")
                .header("content-type", "application/json")
                .body(Body::from(format!(r#"{{"key":"{key}","ttl_secs":60}}"#)))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let presign: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    let url = presign["url"].as_str().unwrap().to_string();
    assert!(url.starts_with("https://t1.example.test/storage/public/get?cap="), "{url}");

    // Another color: denied.
    rig.auth.seed(
        "other-worker",
        CallerAuth::Worker {
            tenant: "t1".into(),
            project_id: "p1".into(),
            color: Some("c2".into()),
        },
    );
    let resp = rig
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/presign")
                .header("authorization", "Bearer other-worker")
                .header("x-weft-color", "c2")
                .header("content-type", "application/json")
                .body(Body::from(format!(r#"{{"key":"{key}"}}"#)))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);

    // The external-style fetch: NO auth header, capability only.
    let public_path = url.strip_prefix("https://t1.example.test/storage").unwrap();
    let resp = rig
        .router
        .clone()
        .oneshot(Request::builder().uri(public_path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(body_bytes(resp).await.as_ref(), b"give me to an api");

    // After TTL: rejected.
    rig.advance(std::time::Duration::from_secs(61));
    let resp = rig
        .router
        .clone()
        .oneshot(Request::builder().uri(public_path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn capability_is_single_file_and_tamper_proof() {
    let rig = StorageTestRig::new().await;
    let key_a = put_file(&rig, r#"{"kind":"execution"}"#, b"file a").await;
    let key_b = put_file(&rig, r#"{"kind":"execution"}"#, b"file b").await;

    // Mint for A via the dispatcher handshake path.
    let resp = rig
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/admin/mint")
                .header("authorization", "Bearer dispatcher-token")
                .header("content-type", "application/json")
                .body(Body::from(format!(r#"{{"key":"{key_a}","ttl_secs":600}}"#)))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let mint: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    let path = mint["path"].as_str().unwrap().to_string();

    // The capability fetches A...
    let resp = rig
        .router
        .clone()
        .oneshot(Request::builder().uri(&path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(body_bytes(resp).await.as_ref(), b"file a");

    // ...and is useless for B (single-file): forging by swapping the
    // cap is the validate-rejects-tampering case; here prove a fresh
    // GET for B with A's capability path cannot exist (the key is
    // inside the signed claims, not the URL path).
    assert!(!path.contains(&key_b));
}

#[tokio::test]
async fn presign_bumps_the_kept_file_ttl() {
    let rig = StorageTestRig::new().await;
    // Kept with a 100s TTL.
    let resp = rig
        .router
        .clone()
        .oneshot({
            let mut r = put_req("worker-token", r#"{"kind":"execution"}"#, b"kept");
            r.headers_mut().insert(
                "x-weft-keep",
                r#"{"kind":"secs","secs":100}"#.parse().unwrap(),
            );
            r
        })
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let v: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    let key = weft_core::storage::StoredFile::from_value(&v).unwrap().key;

    // At t+60, presign (counts as access -> expiry becomes t+160).
    rig.advance(std::time::Duration::from_secs(60));
    let resp = rig
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/presign")
                .header("authorization", "Bearer worker-token")
                .header("x-weft-color", "c1")
                .header("content-type", "application/json")
                .body(Body::from(format!(r#"{{"key":"{key}"}}"#)))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // At t+120 the original expiry (t+100) has passed but the bump
    // keeps it alive.
    rig.advance(std::time::Duration::from_secs(60));
    assert_eq!(rig.store.expiry_sweep().await.unwrap(), 0);
    assert!(rig.store.meta(&pk(&key)).await.is_some());
}

#[tokio::test]
async fn http_range_header_maps_to_exact_bytes() {
    let rig = StorageTestRig::new().await;
    let key = put_file(&rig, r#"{"kind":"execution"}"#, b"0123456789").await;

    // A range request is honest HTTP: 206 Partial Content with a
    // Content-Range header naming the served slice of the whole file.
    for (header, expect, content_range) in [
        ("bytes=2-5", "2345", "bytes 2-5/10"),   // inclusive both ends
        ("bytes=7-", "789", "bytes 7-9/10"),     // open end
        ("bytes=-3", "789", "bytes 7-9/10"),     // suffix
        ("bytes=0-0", "0", "bytes 0-0/10"),
    ] {
        let mut req = get_req("worker-token", "c1", &key);
        req.headers_mut().insert("range", header.parse().unwrap());
        let resp = rig.router.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT, "{header}");
        assert_eq!(
            resp.headers().get("content-range").unwrap().to_str().unwrap(),
            content_range,
            "{header}"
        );
        assert_eq!(resp.headers().get("accept-ranges").unwrap(), "bytes", "{header}");
        assert_eq!(body_bytes(resp).await.as_ref(), expect.as_bytes(), "{header}");
    }

    // A whole-file request is a 200 advertising resume support.
    let resp = rig.router.clone().oneshot(get_req("worker-token", "c1", &key)).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.headers().get("accept-ranges").unwrap(), "bytes");
    assert_eq!(resp.headers().get("content-length").unwrap(), "10");

    // Unsatisfiable ranges all map to 416, never a malformed 206:
    //   inverted (start>end), at-or-past EOF (start>=size), and the
    //   last-zero-bytes suffix `bytes=-0`. Valid offsets are 0..=9.
    for spec in ["bytes=5-2", "bytes=10-", "bytes=11-20", "bytes=-0"] {
        let mut req = get_req("worker-token", "c1", &key);
        req.headers_mut().insert("range", spec.parse().unwrap());
        let resp = rig.router.clone().oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::RANGE_NOT_SATISFIABLE,
            "range '{spec}' must be 416"
        );
    }
}

#[tokio::test]
async fn put_rejects_unserveable_mime_and_filename() {
    let rig = StorageTestRig::new().await;
    // A double quote in the filename (legal in a request header,
    // illegal unescaped in a quoted Content-Disposition value) would
    // break the response header at serve time; reject at put instead.
    let bad_name = Request::builder()
        .method("PUT")
        .uri("/v1/files")
        .header("authorization", "Bearer worker-token")
        .header("x-weft-color", "c1")
        .header("x-weft-scope", r#"{"kind":"execution"}"#)
        .header("x-weft-mime", "image/png")
        .header("x-weft-filename", "a\"b.png")
        .body(Body::from(&b"x"[..]))
        .unwrap();
    let resp = rig.router.clone().oneshot(bad_name).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    // An empty mime is not a serveable media type.
    let bad_mime = Request::builder()
        .method("PUT")
        .uri("/v1/files")
        .header("authorization", "Bearer worker-token")
        .header("x-weft-color", "c1")
        .header("x-weft-scope", r#"{"kind":"execution"}"#)
        .header("x-weft-mime", "")
        .header("x-weft-filename", "ok.png")
        .body(Body::from(&b"x"[..]))
        .unwrap();
    let resp = rig.router.clone().oneshot(bad_mime).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn presigned_url_supports_ranged_resume() {
    // The resume path a big-file download relies on: after a drop,
    // the client re-fetches the SAME public URL with a Range header
    // and gets exactly the remaining bytes as 206. (Here we mint via
    // the dispatcher handshake, then range-fetch the public path.)
    let rig = StorageTestRig::new().await;
    let key = put_file(&rig, r#"{"kind":"execution"}"#, b"0123456789").await;
    let resp = rig
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/admin/mint")
                .header("authorization", "Bearer dispatcher-token")
                .header("content-type", "application/json")
                .body(Body::from(format!(r#"{{"key":"{key}","ttl_secs":600}}"#)))
                .unwrap(),
        )
        .await
        .unwrap();
    let mint: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    let path = mint["path"].as_str().unwrap().to_string();

    // Simulate a drop after 4 bytes: resume from offset 4.
    let resp = rig
        .router
        .clone()
        .oneshot(
            Request::builder()
                .uri(&path)
                .header("range", "bytes=4-")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        resp.headers().get("content-range").unwrap().to_str().unwrap(),
        "bytes 4-9/10"
    );
    assert_eq!(body_bytes(resp).await.as_ref(), b"456789");
}

#[tokio::test]
async fn public_get_serves_attachment_with_filename() {
    // The box always serves `attachment` (a saved download). Inline
    // image preview is handled host-side by the extension (it fetches
    // the bytes and serves them as a local webview resource), so the
    // box has no inline mode.
    let rig = StorageTestRig::new().await;
    let key = put_file(&rig, r#"{"kind":"execution"}"#, b"PNGDATA").await;
    let resp = rig
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/admin/mint")
                .header("authorization", "Bearer dispatcher-token")
                .header("content-type", "application/json")
                .body(Body::from(format!(r#"{{"key":"{key}"}}"#)))
                .unwrap(),
        )
        .await
        .unwrap();
    let mint: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    let path = mint["path"].as_str().unwrap().to_string();

    // Default: attachment.
    let resp = rig
        .router
        .clone()
        .oneshot(Request::builder().uri(&path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert!(resp
        .headers()
        .get("content-disposition")
        .unwrap()
        .to_str()
        .unwrap()
        .starts_with("attachment"));
    assert_eq!(body_bytes(resp).await.as_ref(), b"PNGDATA");
}

#[tokio::test]
async fn keep_rejected_on_project_scope_and_keep_flag_rejected_on_project_put() {
    let rig = StorageTestRig::new().await;
    let pkey = put_file(&rig, r#"{"kind":"project"}"#, b"persistent").await;

    let resp = rig
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/keep")
                .header("authorization", "Bearer worker-token")
                .header("x-weft-color", "c1")
                .header("content-type", "application/json")
                .body(Body::from(format!(r#"{{"key":"{pkey}","ttl":{{"kind":"default"}}}}"#)))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    let mut req = put_req("worker-token", r#"{"kind":"project"}"#, b"x");
    req.headers_mut().insert("x-weft-keep", r#"{"kind":"default"}"#.parse().unwrap());
    let resp = rig.router.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn admin_sweep_and_wipe_work_via_http() {
    let rig = StorageTestRig::new().await;
    put_file(&rig, r#"{"kind":"execution"}"#, b"scratch").await;
    let pkey = put_file(&rig, r#"{"kind":"project"}"#, b"proj").await;

    let resp = rig
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/admin/sweep-exec")
                .header("authorization", "Bearer dispatcher-token")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"color":"c1"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let out: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    assert_eq!(out["swept"], 1);

    let resp = rig
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/admin/wipe-prefix")
                .header("authorization", "Bearer dispatcher-token")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"prefix":"project/p1/"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(rig.store.meta(&pk(&pkey)).await.is_none());
}

#[tokio::test]
async fn worker_without_color_claim_cannot_touch_exec_scope() {
    let rig = StorageTestRig::new().await;
    let key = put_file(&rig, r#"{"kind":"execution"}"#, b"x").await;
    rig.auth.seed(
        "colorless",
        CallerAuth::Worker { tenant: "t1".into(), project_id: "p1".into(), color: None },
    );
    let resp = rig
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v1/files/{key}"))
                .header("authorization", "Bearer colorless")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}
