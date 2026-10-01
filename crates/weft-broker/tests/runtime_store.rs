//! Layer-3 contract tests for the broker's runtime-file plane against a REAL
//! Postgres + an in-memory object store. The quota accounting (charged at
//! reservation time, under the tenant lock), the keep/expiry math, the
//! terminate + expiry sweeps, the prefix list/wipe, AND the multipart upload
//! lifecycle (begin -> reserve signed parts -> record -> complete, with resume
//! and abort) all live IN SQL, so a faked DB would not catch them. Bytes never
//! flow through the broker: the worker PUTs each part to a presigned URL,
//! which the test simulates with `FakeObjectStore::put_part`; the fake
//! enforces the SIGNED part length exactly like the real bucket, so these
//! tests prove the quota lock, not just the bookkeeping.
//!
//! Gated behind `db-tests` (off by default) so a plain `cargo test` needs no PG.
#![cfg(feature = "db-tests")]

use std::sync::Arc;
use std::time::Duration;

use sqlx::PgPool;

use weft_broker::entitlement::{Entitlement, EntitlementSource};
use weft_broker::runtime_store::{
    charged_bytes_for, BeginUpload, RuntimeStore, RuntimeStoreError, UploadSpec,
    COMPLETING_LEASE_SECS, DEFAULT_KEEP_TTL_SECS, DEFAULT_PART_SIZE_BYTES, EXEC_LINGER_TTL_SECS,
};
use weft_core::storage::key::CallerAuth;
use weft_core::storage::{bytes_stream, ByteRange, KeepTtl, PartAsk, StorageScope, StoredFileMeta};
use weft_platform_traits::clock::{Clock, FakeClock};
use weft_platform_traits::object_store::fake::FakeObjectStore;
use weft_platform_traits::{ObjectStore, PresignAudience};

/// Every contract test here drives the WORKER upload path, whose part URLs are
/// presigned for the internal (Internal) endpoint. Named once so the many
/// One part asked for by number and size. A part number IS the part's
/// position in the file, so a test that reserves the first part says 1.
fn ask(part_number: i32, size_bytes: u64) -> PartAsk {
    PartAsk { part_number, size_bytes }
}

/// `reserve_parts` / `resume_upload` call sites read cleanly. (The External
/// audience, the editor upload's browser-facing URLs, is exercised at Layer 4
/// in `weft-e2e/tests/config_media.rs`.)
const WORKER: PresignAudience = PresignAudience::Internal;

/// A worker caller in (tenant t1, project p1, execution c1).
fn worker(tenant: &str, project: &str, execution_id: Option<&str>) -> CallerAuth {
    CallerAuth::Worker {
        tenant: tenant.into(),
        project_id: project.into(),
        execution_id: execution_id.map(String::from),
        instance: None,
    }
}

/// A test entitlement source: fixed caps, and (to exercise the account-wide
/// budget) a fixed count of bytes charged in ANOTHER plane, which the runtime
/// store adds to its own charged bytes exactly as a multi-plane source
/// would. `extra_other_plane_bytes = 0` is the single-plane default.
struct TestEntitlements {
    caps: Entitlement,
    extra_other_plane_bytes: u64,
}

#[async_trait::async_trait]
impl EntitlementSource for TestEntitlements {
    async fn caps(&self, _tenant: &str) -> anyhow::Result<Entitlement> {
        Ok(self.caps)
    }
    async fn account_used_bytes(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        tenant: &str,
    ) -> anyhow::Result<u64> {
        let mine = charged_bytes_for(&mut **tx, tenant).await?;
        Ok(mine.saturating_add(self.extra_other_plane_bytes))
    }
}

/// Caps-only source (nothing charged in another plane): the default case.
fn budget(caps: Entitlement) -> TestEntitlements {
    TestEntitlements { caps, extra_other_plane_bytes: 0 }
}

/// A generous source (caps not under test) unless a test overrides it.
fn big() -> TestEntitlements {
    budget(Entitlement::from_disk_bytes(1 << 40)) // 1 TiB
}

async fn store(pool: &PgPool) -> (Arc<RuntimeStore>, Arc<FakeObjectStore>, Arc<FakeClock>) {
    weft_task_store::apply_groups(pool, &[&weft_broker::runtime_store::GROUP])
        .await
        .unwrap();
    let clock = FakeClock::new();
    let bucket = Arc::new(FakeObjectStore::new());
    (
        Arc::new(RuntimeStore::new(pool.clone(), bucket.clone(), clock.clone())),
        bucket,
        clock,
    )
}

fn body(b: &[u8]) -> bytes::Bytes {
    bytes::Bytes::copy_from_slice(b)
}

/// The bucket object key for a runtime storage key (mirrors the store's private
/// `object_key`: every runtime object lives under the `runtime/` prefix).
fn object_key(key: &str) -> String {
    format!("runtime/{key}")
}

/// Upload `bytes` to an in-flight upload the way the worker does: slice into
/// parts of `part_size`, reserve each (the URL comes back signed to the exact
/// size), PUT it to the fake bucket, record its etag.
async fn upload_parts(
    s: &RuntimeStore,
    bucket: &FakeObjectStore,
    caller: &CallerAuth,
    key: &str,
    part_size: u64,
    bytes: &bytes::Bytes,
    budget: &TestEntitlements,
) -> Result<(), RuntimeStoreError> {
    // Empty content uploads ZERO parts, exactly like the real worker: an empty
    // object is not a multipart part; `complete` writes it directly.
    let mut offset = 0usize;
    while offset < bytes.len() {
        let end = (offset + part_size as usize).min(bytes.len());
        let slice = bytes.slice(offset..end);
        let part_number = (offset / part_size as usize) as i32 + 1;
        let parts =
            s.reserve_parts(caller, key, &[ask(part_number, slice.len() as u64)], budget, WORKER).await?;
        let part = &parts[0];
        let etag = bucket.put_part(&part.url, slice).expect("fake part PUT");
        s.record_part(caller, key, part.part_number, &etag).await?;
        offset = end;
    }
    Ok(())
}

/// Drive the full worker upload: begin (declared size, charged up front),
/// upload every part, complete. Returns the stored metadata.
#[allow(clippy::too_many_arguments)]
async fn put_via(
    s: &RuntimeStore,
    bucket: &FakeObjectStore,
    caller: &CallerAuth,
    scope: &StorageScope,
    mime: &str,
    filename: &str,
    keep: Option<KeepTtl>,
    budget: &TestEntitlements,
    bytes: bytes::Bytes,
) -> Result<StoredFileMeta, RuntimeStoreError> {
    let BeginUpload::Ready { key, part_size } = s
        .begin_upload(
            caller,
            &UploadSpec {
                scope,
                mime,
                filename,
                keep,
                declared_size: Some(bytes.len() as u64),
                content_hash: None,
                identity: None,
            },
            budget,
        )
        .await?
    else {
        panic!("an unidentified uuid-id begin never answers already-stored");
    };
    upload_parts(s, bucket, caller, &key, part_size, &bytes, budget).await?;
    s.complete_upload(caller, &key).await
}

/// `begin_upload` in the common uuid-id shape (no content hash), the form
/// every non-asset test drives. Same positional args the old signature took,
/// so the many call sites stay one-liners.
#[allow(clippy::too_many_arguments)]
async fn begin_via(
    s: &RuntimeStore,
    caller: &CallerAuth,
    scope: &StorageScope,
    mime: &str,
    filename: &str,
    keep: Option<KeepTtl>,
    budget: &TestEntitlements,
    declared_size: Option<u64>,
) -> Result<(String, u64), RuntimeStoreError> {
    match s
        .begin_upload(
            caller,
            &UploadSpec { scope, mime, filename, keep, declared_size, content_hash: None, identity: None },
            budget,
        )
        .await?
    {
        BeginUpload::Ready { key, part_size } => Ok((key, part_size)),
        BeginUpload::AlreadyStored { .. } => {
            panic!("an unidentified uuid-id begin never answers already-stored")
        }
        BeginUpload::Resume { .. } => {
            panic!("only a content-addressed begin can answer resumable")
        }
    }
}

/// Fetch bytes the way the worker does: get the metadata + presigned GET URL, then
/// read the object DIRECTLY from the bucket (optionally a range).
async fn get_via(
    s: &RuntimeStore,
    bucket: &FakeObjectStore,
    parsed: &weft_core::storage::key::ParsedKey,
    range: Option<weft_core::storage::ByteRange>,
) -> Result<(StoredFileMeta, bytes::Bytes), RuntimeStoreError> {
    let (meta, _url) = s
        .download_url(parsed, weft_platform_traits::PresignAudience::Internal, None)
        .await?;
    let key = parsed.to_key();
    let bytes = match range {
        None => bucket.get(&object_key(&key)).await.unwrap().expect("object present"),
        Some(r) => {
            let end = r.end.unwrap_or(meta.size_bytes).min(meta.size_bytes);
            bucket
                .get_range(&object_key(&key), r.start, end)
                .await
                .unwrap()
                .expect("object present")
        }
    };
    Ok((meta, bytes))
}

#[sqlx::test]
async fn put_then_get_round_trips_and_records_metadata(pool: PgPool) {
    let (s, bucket, _clock) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let meta = put_via(&s, &bucket, &w, &StorageScope::Execution, "text/plain", "hi.txt", None, &big(), body(b"hello"))
        .await
        .expect("put");
    assert_eq!(meta.mime_type, "text/plain");
    assert_eq!(meta.size_bytes, 5);
    assert!(meta.key.starts_with("t1/exec/c1/"));
    assert!(bucket.in_progress_uploads().is_empty(), "no lingering multipart upload");

    // get round-trips the bytes + meta.
    let parsed = weft_core::storage::key::parse_key(&meta.key).unwrap();
    let (got_meta, bytes) = get_via(&s, &bucket, &parsed, None).await.expect("get");
    assert_eq!(bytes, body(b"hello"));
    assert_eq!(got_meta.filename, "hi.txt");

    // a range get returns the slice.
    let (_m, slice) = get_via(&s, &bucket, &parsed, Some(ByteRange { start: 1, end: Some(3) }))
        .await
        .expect("range get");
    assert_eq!(slice, body(b"el"));

    // per-tenant usage reflects the one file (an ACTIVE row).
    assert_eq!(s.tenant_usage("t1").await.unwrap(), (1, 5));
}

/// An identified put is idempotent within its scope: the same source
/// asked for twice is one file and one upload, another project asking
/// for the same source is its own file, and a begin racing an upload
/// of the same identity is a conflict rather than a second file.
#[sqlx::test]
async fn an_identified_put_stores_one_file_per_scope(pool: PgPool) {
    let (s, bucket, _clock) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let spec = |identity: Option<&'static str>| UploadSpec {
        scope: &StorageScope::Project,
        mime: "audio/ogg",
        filename: "voice.ogg",
        keep: None,
        declared_size: Some(5),
        content_hash: None,
        identity,
    };
    let BeginUpload::Ready { key, part_size } =
        s.begin_upload(&w, &spec(Some("whatsapp:m1")), &big()).await.expect("first begin")
    else {
        panic!("nothing stored yet")
    };
    // Mid-upload, the same identity is a conflict, not a second file.
    assert!(matches!(
        s.begin_upload(&w, &spec(Some("whatsapp:m1")), &big()).await,
        Err(RuntimeStoreError::Conflict(_))
    ));
    upload_parts(&s, &bucket, &w, &key, part_size, &body(b"hello"), &big()).await.expect("parts");
    let first = s.complete_upload(&w, &key).await.expect("complete");

    // Stored: the second begin answers the file, and opens no upload.
    let BeginUpload::AlreadyStored { key: again } =
        s.begin_upload(&w, &spec(Some("whatsapp:m1")), &big()).await.expect("second begin")
    else {
        panic!("the identity is stored")
    };
    assert_eq!(again, first.key);
    assert!(bucket.in_progress_uploads().is_empty(), "no lingering multipart upload");
    assert_eq!(s.tenant_usage("t1").await.unwrap(), (1, 5), "one file");

    // Another project's copy of the same source is its own file.
    let other = worker("t1", "p2", Some("c2"));
    assert!(matches!(
        s.begin_upload(&other, &spec(Some("whatsapp:m1")), &big()).await,
        Ok(BeginUpload::Ready { .. })
    ));

    // The asset scope is addressed by content, never by identity.
    let asset = UploadSpec { scope: &StorageScope::Asset, identity: Some("x"), ..spec(None) };
    assert!(matches!(
        s.begin_upload(&w, &asset, &big()).await,
        Err(RuntimeStoreError::Invalid(_))
    ));
}

#[sqlx::test]
async fn an_empty_file_round_trips_as_a_direct_empty_object(pool: PgPool) {
    // An empty object is NOT a multipart part (S3 rejects an empty part): it
    // uploads ZERO parts, and `complete` writes the object directly. Assert the
    // file exists, is empty, AND that no multipart upload lingers (the empty
    // multipart opened at begin was aborted).
    let (s, bucket, _clock) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let meta = put_via(&s, &bucket, &w, &StorageScope::Project, "text/plain", "empty", None, &big(), body(b""))
        .await
        .expect("empty put");
    assert_eq!(meta.size_bytes, 0);
    assert!(bucket.in_progress_uploads().is_empty(), "empty multipart aborted, not left open");
    let parsed = weft_core::storage::key::parse_key(&meta.key).unwrap();
    let (_m, bytes) = get_via(&s, &bucket, &parsed, None).await.expect("get");
    assert!(bytes.is_empty());
}

#[sqlx::test]
async fn a_zero_byte_part_reservation_is_rejected(pool: PgPool) {
    // The store must never reserve a zero-byte part (S3 would reject it at
    // complete). An empty file goes through zero parts instead.
    let (s, _bucket, _c) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let (key, _ps) = begin_via(&s, &w, &StorageScope::Project, "b", "f", None, &big(), None)
        .await
        .unwrap();
    let err = s.reserve_parts(&w, &key, &[ask(1, 0)], &big(), WORKER).await.unwrap_err();
    assert!(matches!(err, RuntimeStoreError::Invalid(_)), "{err:?}");
}

#[sqlx::test]
async fn a_multi_part_upload_assembles_in_order(pool: PgPool) {
    // A payload larger than one part exercises the real slicing: full parts of
    // exactly part_size plus a short final part, assembled in order.
    let (s, bucket, _clock) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let mut payload = vec![0u8; DEFAULT_PART_SIZE_BYTES as usize * 2 + 3];
    for (i, b) in payload.iter_mut().enumerate() {
        *b = (i % 251) as u8;
    }
    let payload = bytes::Bytes::from(payload);
    let meta = put_via(&s, &bucket, &w, &StorageScope::Project, "application/octet-stream", "big", None, &big(), payload.clone())
        .await
        .expect("multi-part put");
    assert_eq!(meta.size_bytes, payload.len() as u64);
    let assembled = bucket.get(&object_key(&meta.key)).await.unwrap().expect("object");
    assert_eq!(assembled, payload, "parts assembled in order");
    assert!(bucket.in_progress_uploads().is_empty());
}

#[sqlx::test]
async fn wall_denies_cross_execution_id_get(pool: PgPool) {
    let (s, bucket, _c) = store(&pool).await;
    let w1 = worker("t1", "p1", Some("c1"));
    let meta = put_via(&s, &bucket, &w1, &StorageScope::Execution, "text/plain", "f", None, &big(), body(b"x"))
        .await
        .unwrap();
    // The key is under execution c1; a parsed key for ANOTHER execution under the same
    // tenant must be denied by the wall (the store applies check_key_access via
    // the route, but here we assert the key the put minted is execution-scoped).
    assert!(meta.key.contains("/exec/c1/"));
    // A worker with a different execution cannot mint a key for c1's file: the put
    // wall already proved that; here confirm the access check directly.
    let parsed = weft_core::storage::key::parse_key(&meta.key).unwrap();
    let w2 = worker("t1", "p1", Some("c2"));
    assert!(weft_core::storage::key::check_key_access(&w2, &parsed).is_err());
}

#[sqlx::test]
async fn quota_rejects_over_file_count_and_over_declared_bytes(pool: PgPool) {
    let (s, bucket, _c) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    // file cap 2, byte cap 100 (a hand-built entitlement, not via the floor rule).
    let cap = budget(Entitlement { disk_bytes_cap: 100, file_cap: 2 });
    put_via(&s, &bucket, &w, &StorageScope::Project, "b", "a", None, &cap, body(b"aa")).await.unwrap();
    put_via(&s, &bucket, &w, &StorageScope::Project, "b", "b", None, &cap, body(b"bb")).await.unwrap();
    // third file exceeds the file cap (checked at begin, before any bytes).
    let err = put_via(&s, &bucket, &w, &StorageScope::Project, "b", "c", None, &cap, body(b"cc")).await.unwrap_err();
    assert!(matches!(err, RuntimeStoreError::QuotaExceeded(_)), "{err:?}");

    // byte cap: a DECLARED over-cap size is rejected AT BEGIN, before a part
    // URL exists, before a multipart upload is even opened. This is the hole
    // the reshape closed: no byte can land unquota'd.
    let w2 = worker("t2", "p2", Some("c2"));
    let over = budget(Entitlement { disk_bytes_cap: 4, file_cap: 100 });
    let err = begin_via(&s, &w2, &StorageScope::Project, "b", "big", None, &over, Some(5))
        .await
        .unwrap_err();
    assert!(matches!(err, RuntimeStoreError::QuotaExceeded(_)), "{err:?}");
    assert!(
        bucket.keys().iter().all(|k| !k.contains("t2/")),
        "no object landed for the rejected tenant"
    );
    assert!(bucket.in_progress_uploads().is_empty(), "no multipart upload opened");
    assert_eq!(s.tenant_usage("t2").await.unwrap(), (0, 0), "nothing charged");
}

#[sqlx::test]
async fn other_plane_bytes_count_against_the_same_cap(pool: PgPool) {
    // The disk cap is one account-wide budget: bytes charged in ANOTHER plane
    // (here 95, via the source's account_used_bytes) shrink what this plane may
    // accept, and they are read UNDER THE LOCK inside begin/reserve, not
    // pre-sampled. So even though the runtime plane alone is empty, only 5 more
    // bytes fit before the account cap of 100.
    let (s, bucket, _c) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let terms = TestEntitlements {
        caps: Entitlement { disk_bytes_cap: 100, file_cap: 100 },
        extra_other_plane_bytes: 95,
    };
    // 5 bytes fit (95 + 5 = 100, exactly at cap)...
    put_via(&s, &bucket, &w, &StorageScope::Project, "b", "fits", None, &terms, body(b"12345"))
        .await
        .unwrap();
    // ...but one more byte crosses the account-wide cap even though the
    // runtime plane alone is far under it.
    let err = begin_via(&s, &w, &StorageScope::Project, "b", "over", None, &terms, Some(1))
        .await
        .unwrap_err();
    assert!(matches!(err, RuntimeStoreError::QuotaExceeded(_)), "{err:?}");
}

#[sqlx::test]
async fn a_streaming_upload_that_crosses_the_cap_is_aborted(pool: PgPool) {
    // Unknown-length stream: each part is charged as it is reserved. The
    // reservation that would cross the cap aborts the WHOLE upload: multipart
    // gone from the bucket, row gone, reservation freed.
    let (s, bucket, _c) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let cap = budget(Entitlement { disk_bytes_cap: DEFAULT_PART_SIZE_BYTES + 10, file_cap: 100 });
    let (key, part_size) = begin_via(&s, &w, &StorageScope::Project, "b", "stream", None, &cap, None)
        .await
        .unwrap();
    // First full part fits under the cap.
    s.reserve_parts(&w, &key, &[ask(1, part_size)], &cap, WORKER).await.expect("first part fits");
    assert_eq!(s.tenant_usage("t1").await.unwrap().1, part_size, "in-flight bytes are charged");
    // The next full part would cross the cap: rejected AND the upload aborted.
    let err = s.reserve_parts(&w, &key, &[ask(2, part_size)], &cap, WORKER).await.unwrap_err();
    assert!(matches!(err, RuntimeStoreError::QuotaExceeded(_)), "{err:?}");
    assert!(bucket.in_progress_uploads().is_empty(), "multipart aborted");
    assert_eq!(s.tenant_usage("t1").await.unwrap(), (0, 0), "reservation freed");
    // The upload is gone: further reservations are rejected.
    let err = s.reserve_parts(&w, &key, &[ask(1, 1)], &cap, WORKER).await.unwrap_err();
    assert!(matches!(err, RuntimeStoreError::Invalid(_)), "{err:?}");
}

#[sqlx::test]
async fn a_part_with_the_wrong_byte_count_is_rejected_by_the_signed_length(pool: PgPool) {
    // The quota lock itself: the URL is signed for an exact size, so a body of
    // any other length is rejected by the bucket and nothing lands.
    let (s, bucket, _c) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let (key, _part_size) = begin_via(&s, &w, &StorageScope::Project, "b", "f", None, &big(), Some(5))
        .await
        .unwrap();
    let parts = s.reserve_parts(&w, &key, &[ask(1, 5)], &big(), WORKER).await.unwrap();
    let err = bucket.put_part(&parts[0].url, body(b"way too many bytes")).unwrap_err();
    assert!(err.to_string().contains("signature mismatch"), "{err}");
    let err = bucket.put_part(&parts[0].url, body(b"srt")).unwrap_err();
    assert!(err.to_string().contains("signature mismatch"), "{err}");
    // The exact size lands.
    bucket.put_part(&parts[0].url, body(b"12345")).unwrap();
}

#[sqlx::test]
async fn part_sizes_must_slice_the_declared_total_exactly(pool: PgPool) {
    let (s, _bucket, _c) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let (key, _ps) = begin_via(&s, &w, &StorageScope::Project, "b", "f", None, &big(), Some(10))
        .await
        .unwrap();
    // The only valid slicing of a 10-byte declared total is one 10-byte part.
    let err = s.reserve_parts(&w, &key, &[ask(1, 7)], &big(), WORKER).await.unwrap_err();
    assert!(matches!(err, RuntimeStoreError::Invalid(_)), "{err:?}");
    let parts = s.reserve_parts(&w, &key, &[ask(1, 10)], &big(), WORKER).await.unwrap();
    assert_eq!(parts[0].size_bytes, 10);
    // Nothing can be reserved after the final part.
    let err = s.reserve_parts(&w, &key, &[ask(2, 1)], &big(), WORKER).await.unwrap_err();
    assert!(matches!(err, RuntimeStoreError::Invalid(_)), "{err:?}");
}

#[sqlx::test]
async fn resume_re_presigns_exactly_the_missing_parts(pool: PgPool) {
    // Stream two parts; land only the second. Resume must offer exactly the
    // first (size preserved), and completing before it lands must fail loud.
    let (s, bucket, _c) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let (key, part_size) = begin_via(&s, &w, &StorageScope::Project, "b", "f", None, &big(), None)
        .await
        .unwrap();
    let p1 = &s.reserve_parts(&w, &key, &[ask(1, part_size)], &big(), WORKER).await.unwrap()[0];
    let p2 = &s.reserve_parts(&w, &key, &[ask(2, 5)], &big(), WORKER).await.unwrap()[0];
    let etag2 = bucket.put_part(&p2.url, body(b"tail!")).unwrap();
    s.record_part(&w, &key, p2.part_number, &etag2).await.unwrap();

    // Complete refuses while part 1 is missing, and names the recovery.
    let err = s.complete_upload(&w, &key).await.unwrap_err();
    assert!(err.to_string().contains("resume"), "{err}");

    let (resumed_part_size, missing, carved) = s.resume_upload(&w, &key, &big(), WORKER).await.unwrap();
    assert_eq!(resumed_part_size, part_size);
    assert_eq!(missing.len(), 1);
    assert_eq!(missing[0].part_number, p1.part_number);
    assert_eq!(missing[0].size_bytes, part_size);
    // The missing part is the FIRST one, with a landed part after it, so
    // what has landed is not a prefix of the file. The offset is the
    // part's own place, not the number of bytes that happen to be
    // stored: an uploader told "5 bytes are in" would have sent the file
    // from byte 5 under part 1 and stored a scrambled object under an
    // honest content hash.
    assert_eq!(missing[0].offset_bytes, 0);
    // And new parts would begin after everything already carved, which
    // is past the missing part, not where it ends.
    assert_eq!(carved, part_size + 5);

    // Land it through the fresh URL and complete.
    let head = bytes::Bytes::from(vec![9u8; part_size as usize]);
    let etag1 = bucket.put_part(&missing[0].url, head.clone()).unwrap();
    s.record_part(&w, &key, missing[0].part_number, &etag1).await.unwrap();
    let meta = s.complete_upload(&w, &key).await.unwrap();
    assert_eq!(meta.size_bytes, part_size + 5);
    let assembled = bucket.get(&object_key(&key)).await.unwrap().unwrap();
    assert_eq!(&assembled[..part_size as usize], &head[..], "parts in order");
}

#[sqlx::test]
async fn record_part_is_idempotent_and_rejects_unreserved_parts(pool: PgPool) {
    let (s, bucket, _c) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let (key, _ps) = begin_via(&s, &w, &StorageScope::Project, "b", "f", None, &big(), Some(3))
        .await
        .unwrap();
    let part = &s.reserve_parts(&w, &key, &[ask(1, 3)], &big(), WORKER).await.unwrap()[0];
    let etag = bucket.put_part(&part.url, body(b"abc")).unwrap();
    s.record_part(&w, &key, part.part_number, &etag).await.unwrap();
    // Re-reporting the same part is fine (retry of a lost response).
    s.record_part(&w, &key, part.part_number, &etag).await.unwrap();
    // A part number that was never reserved is rejected loud.
    let err = s.record_part(&w, &key, 99, &etag).await.unwrap_err();
    assert!(matches!(err, RuntimeStoreError::Invalid(_)), "{err:?}");
    s.complete_upload(&w, &key).await.unwrap();
}

#[sqlx::test]
async fn abort_frees_the_reservation(pool: PgPool) {
    // A declared upload charges at begin; abort must free the charge so the
    // tenant can immediately begin again under the same cap. Abort is
    // idempotent, and aborting a COMPLETED file is refused (delete instead).
    let (s, bucket, _c) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let cap = budget(Entitlement { disk_bytes_cap: 10, file_cap: 100 });
    let (key, _ps) = begin_via(&s, &w, &StorageScope::Project, "b", "f", None, &cap, Some(8))
        .await
        .unwrap();
    // The charge blocks a second 8-byte begin.
    let err = begin_via(&s, &w, &StorageScope::Project, "b", "g", None, &cap, Some(8))
        .await
        .unwrap_err();
    assert!(matches!(err, RuntimeStoreError::QuotaExceeded(_)), "{err:?}");
    s.abort_upload(&w, &key).await.unwrap();
    s.abort_upload(&w, &key).await.unwrap(); // idempotent
    assert!(bucket.in_progress_uploads().is_empty(), "multipart aborted");
    assert_eq!(s.tenant_usage("t1").await.unwrap(), (0, 0), "charge freed");
    // Now the same size fits, and once completed it cannot be aborted.
    let meta = put_via(&s, &bucket, &w, &StorageScope::Project, "b", "g", None, &cap, body(b"12345678"))
        .await
        .unwrap();
    let err = s.abort_upload(&w, &meta.key).await.unwrap_err();
    assert!(matches!(err, RuntimeStoreError::Invalid(_)), "{err:?}");
}

#[sqlx::test]
async fn terminate_sweep_lingers_unkept_and_spares_kept(pool: PgPool) {
    let (s, bucket, clock) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    // two exec files: one kept, one not.
    let kept = put_via(&s, &bucket, &w, &StorageScope::Execution, "b", "keep", Some(KeepTtl::Default), &big(), body(b"k"))
        .await
        .unwrap();
    let scratch = put_via(&s, &bucket, &w, &StorageScope::Execution, "b", "scratch", None, &big(), body(b"s"))
        .await
        .unwrap();
    let (swept, lingering) = s.sweep_exec("t1", "c1").await.unwrap();
    assert_eq!((swept, lingering), (0, 1), "the un-kept file lingers, nothing is reaped");
    // Both are still readable right after terminate: the un-kept file carries
    // the linger deadline (what the file lists surface as remaining lifetime).
    let kept_parsed = weft_core::storage::key::parse_key(&kept.key).unwrap();
    let scratch_parsed = weft_core::storage::key::parse_key(&scratch.key).unwrap();
    assert!(get_via(&s, &bucket, &kept_parsed, None).await.is_ok());
    assert!(get_via(&s, &bucket, &scratch_parsed, None).await.is_ok(), "downloadable during the linger");
    let stamped = s.meta(&scratch_parsed).await.unwrap();
    assert_eq!(stamped.expires_at_unix, Some(clock.now_unix() + EXEC_LINGER_TTL_SECS));
    // A re-delivered terminate sweep (the queue is idempotent) must not push
    // the deadline out.
    clock.advance(Duration::from_secs(60));
    assert_eq!(s.sweep_exec("t1", "c1").await.unwrap(), (0, 0), "re-sweep restamps nothing");
    assert_eq!(s.meta(&scratch_parsed).await.unwrap().expires_at_unix, stamped.expires_at_unix);
    // Past the linger, the expiry sweep reclaims the scratch; kept survives.
    clock.advance(Duration::from_secs(EXEC_LINGER_TTL_SECS as u64));
    assert_eq!(s.sweep_expired().await.unwrap(), 1);
    assert!(get_via(&s, &bucket, &kept_parsed, None).await.is_ok(), "kept survives");
    assert!(matches!(get_via(&s, &bucket, &scratch_parsed, None).await, Err(RuntimeStoreError::NotFound(_))));
}

#[sqlx::test]
async fn public_link_outlives_terminate_and_dies_with_its_file(pool: PgPool) {
    let (s, bucket, clock) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let file = put_via(&s, &bucket, &w, &StorageScope::Execution, "b", "img", None, &big(), body(b"png"))
        .await
        .unwrap();
    let parsed = weft_core::storage::key::parse_key(&file.key).unwrap();
    let link_ttl: u64 = 900;
    let token = s.mint_public_link(&parsed, Some(link_ttl)).await.unwrap();
    let link_expiry = clock.now_unix() + link_ttl as i64;
    assert!(s.resolve_public_link(&token).await.unwrap().is_some());

    // Terminate: the linger deadline honors the live link (a minted link
    // is a promise the bytes stay fetchable for its stated lifetime).
    assert!(link_ttl as i64 > EXEC_LINGER_TTL_SECS, "the test needs the link to outlast the linger");
    let (_, lingering) = s.sweep_exec("t1", "c1").await.unwrap();
    assert_eq!(lingering, 1);
    assert_eq!(s.meta(&parsed).await.unwrap().expires_at_unix, Some(link_expiry));

    // Past the plain linger but inside the link's life: file + link live.
    clock.advance(Duration::from_secs(EXEC_LINGER_TTL_SECS as u64 + 1));
    assert_eq!(s.sweep_expired().await.unwrap(), 0);
    assert!(s.resolve_public_link(&token).await.unwrap().is_some());

    // Past the link's expiry: the token resolves to nothing, and the
    // sweep reclaims the file WITH its link row (cascade), so a re-mint
    // sweep has nothing left to find.
    clock.advance(Duration::from_secs(link_ttl));
    assert!(s.resolve_public_link(&token).await.unwrap().is_none(), "expired token is gone");
    assert_eq!(s.sweep_expired().await.unwrap(), 1);
    assert!(matches!(get_via(&s, &bucket, &parsed, None).await, Err(RuntimeStoreError::NotFound(_))));

    // A deleted file takes a still-live link with it: dead token, clean miss.
    let file2 = put_via(&s, &bucket, &w, &StorageScope::Project, "b", "img2", None, &big(), body(b"x"))
        .await
        .unwrap();
    let parsed2 = weft_core::storage::key::parse_key(&file2.key).unwrap();
    let token2 = s.mint_public_link(&parsed2, Some(600)).await.unwrap();
    s.delete(&parsed2).await.unwrap();
    assert!(s.resolve_public_link(&token2).await.unwrap().is_none(), "cascade removed the link");
}

#[sqlx::test]
async fn no_expiry_write_shortens_a_file_below_its_live_link(pool: PgPool) {
    let (s, bucket, clock) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    // A kept file on a SHORT keep TTL, carrying a LONG-lived link.
    let file = put_via(&s, &bucket, &w, &StorageScope::Execution, "b", "img", Some(KeepTtl::Secs { secs: 60 }), &big(), body(b"png"))
        .await
        .unwrap();
    let parsed = weft_core::storage::key::parse_key(&file.key).unwrap();
    // The token itself is unused: this test is about the expiry floor the
    // mint leaves on the file row, not about resolving the link.
    let _token = s.mint_public_link(&parsed, Some(3600)).await.unwrap();
    let link_expiry = clock.now_unix() + 3600;
    assert_eq!(s.meta(&parsed).await.unwrap().expires_at_unix, Some(link_expiry), "mint covered");

    // An ACCESS bumps to now + keep TTL, which must not undercut the link.
    clock.advance(Duration::from_secs(10));
    get_via(&s, &bucket, &parsed, None).await.unwrap();
    assert_eq!(
        s.meta(&parsed).await.unwrap().expires_at_unix,
        Some(link_expiry),
        "access bump must not shorten below a live link"
    );

    // A re-KEEP with a short TTL must not undercut it either.
    s.keep(&parsed, KeepTtl::Secs { secs: 60 }).await.unwrap();
    assert_eq!(
        s.meta(&parsed).await.unwrap().expires_at_unix,
        Some(link_expiry),
        "keep must not shorten below a live link"
    );

    // Past the link's cover the normal clocks apply again: the sweep
    // reclaims the file once its (link-extended) deadline passes.
    clock.advance(Duration::from_secs(3601));
    assert_eq!(s.sweep_expired().await.unwrap(), 1, "past the link cover the file dies normally");
}

#[sqlx::test]
async fn keep_then_expiry_sweep_reclaims_after_ttl(pool: PgPool) {
    let (s, bucket, clock) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let f = put_via(&s, &bucket, &w, &StorageScope::Execution, "b", "f", Some(KeepTtl::Secs { secs: 100 }), &big(), body(b"x"))
        .await
        .unwrap();
    let parsed = weft_core::storage::key::parse_key(&f.key).unwrap();
    // Before the TTL, the expiry sweep keeps it.
    clock.advance(Duration::from_secs(50));
    assert_eq!(s.sweep_expired().await.unwrap(), 0);
    assert!(get_via(&s, &bucket, &parsed, None).await.is_ok());
    // A get bumps the expiry to now+100, so advancing another 80s (total 130 >
    // 100 from put, but only 80 since the access-bump) still keeps it.
    clock.advance(Duration::from_secs(80));
    assert_eq!(s.sweep_expired().await.unwrap(), 0, "access bumped the expiry");
    // Now let it sit past the bumped TTL.
    clock.advance(Duration::from_secs(101));
    assert_eq!(s.sweep_expired().await.unwrap(), 1);
    assert!(matches!(get_via(&s, &bucket, &parsed, None).await, Err(RuntimeStoreError::NotFound(_))));
}

#[sqlx::test]
async fn keep_default_resolves_to_default_ttl(pool: PgPool) {
    let (s, bucket, _c) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let f = put_via(&s, &bucket, &w, &StorageScope::Execution, "b", "f", Some(KeepTtl::Default), &big(), body(b"x"))
        .await
        .unwrap();
    assert_eq!(f.keep_ttl_secs, Some(DEFAULT_KEEP_TTL_SECS));
    assert!(f.expires_at_unix.is_some());
}

#[sqlx::test]
async fn keep_never_has_no_expiry(pool: PgPool) {
    // KeepTtl::Never must resolve to NO expiry (ttl None -> expires_at None), so a
    // "keep forever" file is never silently reclaimed by the expiry sweep.
    let (s, bucket, _c) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let f = put_via(&s, &bucket, &w, &StorageScope::Execution, "b", "f", Some(KeepTtl::Never), &big(), body(b"x"))
        .await
        .unwrap();
    assert_eq!(f.keep_ttl_secs, None);
    assert_eq!(f.expires_at_unix, None);
}

#[sqlx::test]
async fn list_is_scoped_and_wipe_prefix_clears_it(pool: PgPool) {
    let (s, bucket, _c) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    put_via(&s, &bucket, &w, &StorageScope::Project, "b", "a", None, &big(), body(b"a")).await.unwrap();
    put_via(&s, &bucket, &w, &StorageScope::Project, "b", "b", None, &big(), body(b"b")).await.unwrap();
    put_via(&s, &bucket, &w, &StorageScope::Execution, "b", "e", None, &big(), body(b"e")).await.unwrap();
    // list project scope sees only project files.
    let prefix = weft_core::storage::key::prefix_for_list(&w, &StorageScope::Project).unwrap();
    assert_eq!(s.list(&prefix).await.unwrap().len(), 2);
    // wipe the project prefix clears them, leaves exec.
    let wiped = s.wipe_prefix(&prefix).await.unwrap();
    assert_eq!(wiped, 2);
    assert_eq!(s.list(&prefix).await.unwrap().len(), 0);
    assert_eq!(s.tenant_usage("t1").await.unwrap().0, 1); // the exec file remains
}

#[sqlx::test]
async fn assemble_concatenates_existing_objects_into_an_asset(pool: PgPool) {
    let (s, bucket, _c) = store(&pool).await;
    let w = acting("t1");

    // Two "chunk" objects already in the bucket (another storage plane);
    // assembly must concatenate them in order into one ledgered asset without
    // any caller moving bytes.
    bucket.put("chunks/c1", body(b"HELLO ")).await.unwrap();
    bucket.put("chunks/c2", body(b"WORLD")).await.unwrap();
    let sha = "cd".repeat(32);
    let sha_static: &'static str = Box::leak(sha.clone().into_boxed_str());
    let spec = UploadSpec {
        scope: &StorageScope::Asset,
        mime: "text/plain",
        filename: "assets/greeting.txt",
        keep: None,
        declared_size: Some(11),
        content_hash: Some(sha_static),
        identity: None,
    };
    let sources = vec![("chunks/c1".to_string(), 6u64), ("chunks/c2".to_string(), 5u64)];
    let meta = s.assemble(&w, &spec, &sources, &big()).await.unwrap();
    assert_eq!(meta.size_bytes, 11);
    assert_eq!(meta.key, format!("t1/asset/{sha}"));

    // The assembled object is byte-exact and ledgered (listed + downloadable).
    let obj = bucket.get(&object_key(&meta.key)).await.unwrap().expect("assembled object");
    assert_eq!(&obj[..], b"HELLO WORLD");
    let prefix = "t1/asset/";
    assert_eq!(s.list(prefix).await.unwrap().len(), 1);

    // Re-assembling ACTIVE content is the idempotent success (content
    // addressed: same bytes = same asset): the existing file's meta comes
    // back, nothing re-transfers, no second copy, no new pending row.
    let again = s.assemble(&w, &spec, &sources, &big()).await.unwrap();
    assert_eq!(again.key, meta.key);
    assert_eq!(again.size_bytes, 11);
    assert_eq!(s.list(prefix).await.unwrap().len(), 1, "no second copy");

    // A missing source aborts loudly and leaves NOTHING: no pending row, no
    // partial object, reservation freed.
    let sha2: &'static str = Box::leak("ef".repeat(32).into_boxed_str());
    let bad_spec = UploadSpec { content_hash: Some(sha2), ..spec };
    let bad = vec![("chunks/nope".to_string(), 6u64), ("chunks/c2".to_string(), 5u64)];
    assert!(matches!(
        s.assemble(&w, &bad_spec, &bad, &big()).await,
        Err(RuntimeStoreError::Invalid(_))
    ));
    assert_eq!(s.list(prefix).await.unwrap().len(), 1, "only the first asset exists");
    let pending: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM runtime_file WHERE status = 'pending'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(pending, 0, "failed assembly left no reservation");
}

#[sqlx::test]
async fn asset_uploads_are_content_addressed_and_conflict_on_duplicates(pool: PgPool) {
    let (s, bucket, _c) = store(&pool).await;
    let w = acting("t1");
    let sha = "ab".repeat(32);
    let asset_spec = |hash: Option<&'static str>| UploadSpec {
        scope: &StorageScope::Asset,
        mime: "image/png",
        filename: "assets/pic.png",
        keep: None,
        declared_size: Some(4),
        content_hash: hash,
        identity: None,
    };

    // A missing or malformed hash is refused loud (assets ARE their hash).
    assert!(matches!(
        s.begin_upload(&w, &asset_spec(None), &big()).await,
        Err(RuntimeStoreError::Invalid(_))
    ));
    assert!(matches!(
        s.begin_upload(&w, &asset_spec(Some("nothex")), &big()).await,
        Err(RuntimeStoreError::Invalid(_))
    ));
    // A hash on a non-asset scope is refused too (uuid minting is the contract).
    let bad = UploadSpec { scope: &StorageScope::Project, content_hash: Some("aa"), ..asset_spec(None) };
    assert!(matches!(s.begin_upload(&worker("t1", "p1", None), &bad, &big()).await, Err(RuntimeStoreError::Invalid(_))));

    // A real asset upload lands under `<tenant>/asset/<sha>`.
    let sha_static: &'static str = Box::leak(sha.clone().into_boxed_str());
    let BeginUpload::Ready { key, part_size } =
        s.begin_upload(&w, &asset_spec(Some(sha_static)), &big()).await.unwrap()
    else {
        panic!("fresh asset content must reserve a real upload");
    };
    assert_eq!(key, format!("t1/asset/{sha}"));

    // Re-beginning while the first upload is PENDING hands back that same
    // upload to carry on with, under the store's own key. It used to be a
    // loud conflict, which left the second publish of one asset failing for
    // as long as the first one's leftovers sat there. Carrying on is safe
    // because a part is reserved by NUMBER and both writers hold identical
    // bytes (the key is their hash).
    let BeginUpload::Resume { key: resume_key, part_size: resume_part_size } =
        s.begin_upload(&w, &asset_spec(Some(sha_static)), &big()).await.unwrap()
    else {
        panic!("a pending upload of the same content is resumable");
    };
    assert_eq!(resume_key, key, "the store's own key, not one the caller guessed");
    assert_eq!(resume_part_size, part_size);

    upload_parts(&s, &bucket, &w, &key, part_size, &bytes::Bytes::from_static(b"weft"), &big())
        .await
        .unwrap();
    let meta = s.complete_upload(&w, &key).await.unwrap();
    assert_eq!(meta.size_bytes, 4);

    // Re-beginning ACTIVE content is the idempotent success: the existing
    // key with nothing to transfer, not a PK explosion, not a second copy,
    // not an error the caller must string-match.
    assert_eq!(
        s.begin_upload(&w, &asset_spec(Some(sha_static)), &big()).await.unwrap(),
        BeginUpload::AlreadyStored { key: key.clone() }
    );

    // The asset lists under the tenant's assets and deletes like any file.
    assert_eq!(s.list("t1/asset/").await.unwrap().len(), 1);
    let parsed = weft_core::storage::key::parse_key(&key).unwrap();
    s.delete(&parsed).await.unwrap();
    assert_eq!(s.list("t1/asset/").await.unwrap().len(), 0);
}

#[sqlx::test]
async fn delete_removes_and_presign_requires_existing(pool: PgPool) {
    let (s, bucket, _c) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let f = put_via(&s, &bucket, &w, &StorageScope::Project, "b", "f", None, &big(), body(b"x")).await.unwrap();
    let parsed = weft_core::storage::key::parse_key(&f.key).unwrap();
    // presign mints a URL for the existing file.
    assert!(s.presign(&parsed, Some(60)).await.unwrap().contains(&f.key));
    s.delete(&parsed).await.unwrap();
    assert!(matches!(s.delete(&parsed).await, Err(RuntimeStoreError::NotFound(_))));
    assert!(matches!(s.presign(&parsed, None).await, Err(RuntimeStoreError::NotFound(_))));
}

/// The dispatcher acting for `tenant` on its assets: the admin upload
/// surface's caller.
fn acting(tenant: &str) -> CallerAuth {
    CallerAuth::Tenant { tenant: tenant.into() }
}

/// The asset named `id` (its hash is `id` spelled as 64 hex digits).
fn asset_key(tenant: &str, id: u64) -> String {
    format!("{tenant}/asset/{id:064x}")
}

async fn asset_via(s: &RuntimeStore, bucket: &FakeObjectStore, tenant: &str, id: u64) -> StoredFileMeta {
    let caller = acting(tenant);
    let hash = format!("{id:064x}");
    let spec = UploadSpec {
        scope: &StorageScope::Asset,
        mime: "image/png",
        filename: "cat.png",
        keep: None,
        declared_size: Some(3),
        content_hash: Some(&hash),
        identity: None,
    };
    let BeginUpload::Ready { key, part_size } = s.begin_upload(&caller, &spec, &big()).await.unwrap() else {
        panic!("test asset must be new");
    };
    upload_parts(s, bucket, &caller, &key, part_size, &body(b"cat"), &big()).await.unwrap();
    s.complete_upload(&caller, &key).await.unwrap()
}

#[sqlx::test]
async fn asset_lifetime_keeps_current_and_expires_removed_files_after_last_access(pool: PgPool) {
    let (s, bucket, clock) = store(&pool).await;
    let old = asset_via(&s, &bucket, "t1", 1).await;
    let current = asset_via(&s, &bucket, "t1", 2).await;
    let old_key = weft_core::storage::key::parse_key(&old.key).unwrap();
    s.set_asset_references("t1", "p1", &[old.key.clone(), current.key.clone()], &[]).await.unwrap();
    clock.advance(Duration::from_secs(400 * 86400));
    assert_eq!(s.sweep_expired().await.unwrap(), 0, "current source may be idle for months");

    s.set_asset_references("t1", "p1", std::slice::from_ref(&current.key), &[]).await.unwrap();
    let deadline = s.meta(&old_key).await.unwrap().expires_at_unix.unwrap();
    assert_eq!(deadline, clock.now_unix() + DEFAULT_KEEP_TTL_SECS as i64);
    clock.advance(Duration::from_secs(20 * 86400));
    s.set_asset_references("t1", "p1", std::slice::from_ref(&current.key), &[]).await.unwrap();
    assert_eq!(s.meta(&old_key).await.unwrap().expires_at_unix, Some(deadline), "sync is not file access");

    get_via(&s, &bucket, &old_key, None).await.unwrap();
    assert_eq!(s.meta(&old_key).await.unwrap().expires_at_unix, Some(clock.now_unix() + DEFAULT_KEEP_TTL_SECS as i64));
    clock.advance(Duration::from_secs(DEFAULT_KEEP_TTL_SECS - 1));
    assert_eq!(s.sweep_expired().await.unwrap(), 0);
    clock.advance(Duration::from_secs(2));
    assert_eq!(s.sweep_expired().await.unwrap(), 1);
    assert!(matches!(s.download_url(&old_key, WORKER, None).await, Err(RuntimeStoreError::NotFound(_))));
    assert!(bucket.get(&object_key(&old.key)).await.unwrap().is_none());
    assert!(bucket.get(&object_key(&current.key)).await.unwrap().is_some());
}

#[sqlx::test]
async fn an_asset_the_sync_never_publishes_expires_on_its_own(pool: PgPool) {
    // The build failed after the transfer: nothing ever referenced the
    // upload, so it counts down from completion. Publishing clears it.
    let (s, bucket, clock) = store(&pool).await;
    let orphan = asset_via(&s, &bucket, "t1", 1).await;
    let published = asset_via(&s, &bucket, "t1", 2).await;
    let orphan_key = weft_core::storage::key::parse_key(&orphan.key).unwrap();
    assert_eq!(orphan.expires_at_unix, Some(clock.now_unix() + DEFAULT_KEEP_TTL_SECS as i64));
    s.set_asset_references("t1", "p1", std::slice::from_ref(&published.key), &[]).await.unwrap();
    let published_key = weft_core::storage::key::parse_key(&published.key).unwrap();
    assert_eq!(s.meta(&published_key).await.unwrap().expires_at_unix, None, "publishing clears the countdown");
    assert_eq!(s.meta(&orphan_key).await.unwrap().expires_at_unix, orphan.expires_at_unix, "the orphan's countdown runs on");
    clock.advance(Duration::from_secs(DEFAULT_KEEP_TTL_SECS + 1));
    assert_eq!(s.sweep_expired().await.unwrap(), 1);
    assert!(bucket.get(&object_key(&orphan.key)).await.unwrap().is_none());
    assert!(bucket.get(&object_key(&published.key)).await.unwrap().is_some());
}

#[sqlx::test]
async fn asset_lifetime_removing_last_reference_and_restoring_it_are_both_supported(pool: PgPool) {
    let (s, bucket, clock) = store(&pool).await;
    let file = asset_via(&s, &bucket, "t1", 1).await;
    let parsed = weft_core::storage::key::parse_key(&file.key).unwrap();
    s.set_asset_references("t1", "p1", std::slice::from_ref(&file.key), &[]).await.unwrap();
    s.set_asset_references("t1", "p1", &[], &[]).await.unwrap();
    assert_eq!(s.meta(&parsed).await.unwrap().keep_ttl_secs, Some(DEFAULT_KEEP_TTL_SECS));
    s.set_asset_references("t1", "p1", std::slice::from_ref(&file.key), &[]).await.unwrap();
    let meta = s.meta(&parsed).await.unwrap();
    assert_eq!(meta.expires_at_unix, None);
    assert_eq!(meta.keep_ttl_secs, None);
    // Access must use the current lifetime, not resurrect an old countdown.
    s.presign(&parsed, None).await.unwrap();
    clock.advance(Duration::from_secs(DEFAULT_KEEP_TTL_SECS + 1));
    assert_eq!(s.sweep_expired().await.unwrap(), 0);
}

/// One content is one file per tenant: a second project storing it uploads
/// nothing, the tenant pays for it once, and another tenant's copy of the
/// same bytes is that tenant's own file.
#[sqlx::test]
async fn an_asset_is_stored_and_charged_once_per_tenant(pool: PgPool) {
    let (s, bucket, _) = store(&pool).await;
    let first = asset_via(&s, &bucket, "t1", 1).await;
    assert_eq!(first.key, asset_key("t1", 1));
    let hash = format!("{:064x}", 1);
    let spec = UploadSpec {
        scope: &StorageScope::Asset,
        mime: "image/png",
        filename: "cat.png",
        keep: None,
        declared_size: Some(3),
        content_hash: Some(&hash),
        identity: None,
    };
    assert_eq!(
        s.begin_upload(&acting("t1"), &spec, &big()).await.unwrap(),
        BeginUpload::AlreadyStored { key: first.key.clone() },
        "the tenant already stores this content, whichever project stored it"
    );
    s.set_asset_references("t1", "p1", std::slice::from_ref(&first.key), &[]).await.unwrap();
    s.set_asset_references("t1", "p2", std::slice::from_ref(&first.key), &[]).await.unwrap();
    assert_eq!(s.tenant_usage("t1").await.unwrap(), (1, 3), "one file, charged once");

    // Another tenant: nothing of t1's answers for it.
    let held = s.held_assets("t2", std::slice::from_ref(&hash)).await.unwrap();
    assert!(held.is_empty(), "{held:?}");
    let theirs = asset_via(&s, &bucket, "t2", 1).await;
    assert_eq!(theirs.key, asset_key("t2", 1));
    assert_eq!(s.tenant_usage("t2").await.unwrap(), (1, 3));
    assert_eq!(s.tenant_usage("t1").await.unwrap(), (1, 3));
    assert_eq!(
        s.held_assets("t1", &[hash.clone(), format!("{:064x}", 2)]).await.unwrap(),
        [(hash.clone(), first.key.clone())].into_iter().collect(),
        "only what the tenant stores whole, under its own key"
    );
    assert!(matches!(s.held_assets("t1", &["nothex".into()]).await, Err(RuntimeStoreError::Invalid(_))));
}

/// An asset lives while any project of its tenant references it: one
/// project dropping it leaves it pinned, the last one starts its countdown.
#[sqlx::test]
async fn an_asset_lives_while_any_project_references_it(pool: PgPool) {
    let (s, bucket, clock) = store(&pool).await;
    let shared = asset_via(&s, &bucket, "t1", 1).await;
    let parsed = weft_core::storage::key::parse_key(&shared.key).unwrap();
    s.set_asset_references("t1", "p1", std::slice::from_ref(&shared.key), &[]).await.unwrap();
    s.set_asset_references("t1", "p2", &[], std::slice::from_ref(&shared.key)).await.unwrap();
    s.set_asset_references("t1", "p1", &[], &[]).await.unwrap();
    assert_eq!(s.meta(&parsed).await.unwrap().expires_at_unix, None, "p2 still references it");
    clock.advance(Duration::from_secs(DEFAULT_KEEP_TTL_SECS + 1));
    assert_eq!(s.sweep_expired().await.unwrap(), 0);

    s.set_asset_references("t1", "p2", &[], &[]).await.unwrap();
    assert_eq!(
        s.meta(&parsed).await.unwrap().expires_at_unix,
        Some(clock.now_unix() + DEFAULT_KEEP_TTL_SECS as i64),
        "the last reference gone, the countdown starts"
    );
    clock.advance(Duration::from_secs(DEFAULT_KEEP_TTL_SECS + 1));
    assert_eq!(s.sweep_expired().await.unwrap(), 1);
    assert!(bucket.get(&object_key(&shared.key)).await.unwrap().is_none());
}

#[sqlx::test]
async fn asset_references_are_walled_by_tenant_and_preserve_node_selected_ttls(pool: PgPool) {
    let (s, bucket, _) = store(&pool).await;
    let own = asset_via(&s, &bucket, "t1", 1).await;
    let foreign = asset_via(&s, &bucket, "t2", 2).await;
    let w = worker("t1", "p1", Some("c1"));
    let generated = put_via(&s, &bucket, &w, &StorageScope::Execution, "image/png", "generated.png",
        Some(KeepTtl::Secs { secs: 60 }), &big(), body(b"png")).await.unwrap();
    for forbidden in [&foreign.key, &generated.key] {
        assert!(matches!(s.set_asset_references("t1", "p1", std::slice::from_ref(forbidden), &[]).await,
            Err(RuntimeStoreError::Denied(_))));
        assert!(matches!(s.set_asset_references("t1", "p1", &[], std::slice::from_ref(forbidden)).await,
            Err(RuntimeStoreError::Denied(_))));
    }
    s.set_asset_references("t1", "p1", std::slice::from_ref(&own.key), &[]).await.unwrap();
    s.set_asset_references("t2", "p1", &[], &[]).await.unwrap();
    for unaffected in [&foreign, &generated] {
        let parsed = weft_core::storage::key::parse_key(&unaffected.key).unwrap();
        let meta = s.meta(&parsed).await.unwrap();
        assert_eq!(meta.keep_ttl_secs, unaffected.keep_ttl_secs);
        assert_eq!(meta.expires_at_unix, unaffected.expires_at_unix);
    }
    let own = weft_core::storage::key::parse_key(&own.key).unwrap();
    assert_eq!(s.meta(&own).await.unwrap().expires_at_unix, None, "another tenant's publish never touches t1's references");
}

/// Wiping a whole tenant takes its asset references with its assets, so
/// nothing is left claiming files that are gone.
#[sqlx::test]
async fn a_tenant_wipe_drops_its_asset_references(pool: PgPool) {
    let (s, bucket, _) = store(&pool).await;
    let own = asset_via(&s, &bucket, "t1", 1).await;
    let other = asset_via(&s, &bucket, "t2", 1).await;
    s.set_asset_references("t1", "p1", std::slice::from_ref(&own.key), &[]).await.unwrap();
    s.set_asset_references("t2", "p1", std::slice::from_ref(&other.key), &[]).await.unwrap();
    assert_eq!(s.wipe_prefix("t1/").await.unwrap(), 1);
    let left: Vec<String> = sqlx::query_scalar("SELECT key FROM asset_reference ORDER BY key").fetch_all(&pool).await.unwrap();
    assert_eq!(left, vec![other.key]);
}

/// A kept file (one an older version names) that is gone is reported,
/// not fatal: the build's own file is pinned and the missing key comes
/// back so the caller can say which version lost it.
#[sqlx::test]
async fn asset_lifetime_missing_kept_file_is_reported_and_pins_the_rest(pool: PgPool) {
    let (s, bucket, _) = store(&pool).await;
    let own = asset_via(&s, &bucket, "t1", 1).await;
    let gone = format!("t1/asset/{}", "f".repeat(64));
    let missing = s.set_asset_references("t1", "p1", std::slice::from_ref(&own.key), std::slice::from_ref(&gone)).await.unwrap();
    assert_eq!(missing, vec![gone]);
    let parsed = weft_core::storage::key::parse_key(&own.key).unwrap();
    assert_eq!(s.meta(&parsed).await.unwrap().expires_at_unix, None, "the build's own file is pinned");
}

#[sqlx::test]
async fn asset_lifetime_missing_current_file_does_not_retire_other_files(pool: PgPool) {
    let (s, bucket, _) = store(&pool).await;
    let own = asset_via(&s, &bucket, "t1", 1).await;
    s.set_asset_references("t1", "p1", std::slice::from_ref(&own.key), &[]).await.unwrap();
    let missing = format!("t1/asset/{}", "f".repeat(64));
    assert!(matches!(s.set_asset_references("t1", "p1", &[missing], &[]).await,
        Err(RuntimeStoreError::NotFound(_))));
    // The failed publish touched nothing: the asset is still referenced,
    // so it keeps no countdown.
    let parsed = weft_core::storage::key::parse_key(&own.key).unwrap();
    assert_eq!(s.meta(&parsed).await.unwrap().expires_at_unix, None);
}

#[sqlx::test]
async fn a_lifetime_expires_a_project_file_like_a_kept_one(pool: PgPool) {
    // Expiry works in every scope a node writes: a project file stored with
    // a lifetime expires once nobody touched it for that long, an access
    // renews it, and it never carries the execution-only keep flag.
    let (s, bucket, clock) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let f = put_via(&s, &bucket, &w, &StorageScope::Project, "b", "f", Some(KeepTtl::Secs { secs: 100 }), &big(), body(b"x"))
        .await
        .unwrap();
    assert!(!f.keep, "the keep flag is the execution sweep's exemption only");
    assert_eq!(f.keep_ttl_secs, Some(100));
    let parsed = weft_core::storage::key::parse_key(&f.key).unwrap();
    clock.advance(Duration::from_secs(80));
    assert!(get_via(&s, &bucket, &parsed, None).await.is_ok(), "an access renews it");
    clock.advance(Duration::from_secs(80));
    assert_eq!(s.sweep_expired().await.unwrap(), 0, "renewed by the access");
    clock.advance(Duration::from_secs(30));
    assert_eq!(s.sweep_expired().await.unwrap(), 1);
    assert!(matches!(get_via(&s, &bucket, &parsed, None).await, Err(RuntimeStoreError::NotFound(_))));

    // With no lifetime a project file lives until deleted, and a keep after
    // the fact gives it one; KeepTtl::Never takes it away again.
    let forever = put_via(&s, &bucket, &w, &StorageScope::Project, "b", "g", None, &big(), body(b"y"))
        .await
        .unwrap();
    assert_eq!(forever.expires_at_unix, None);
    let parsed = weft_core::storage::key::parse_key(&forever.key).unwrap();
    let kept = s.keep(&parsed, KeepTtl::Secs { secs: 50 }).await.unwrap();
    assert_eq!(kept.keep_ttl_secs, Some(50));
    assert!(!kept.keep);
    assert_eq!(kept.expires_at_unix, Some(clock.now_unix() + 50));
    let cleared = s.keep(&parsed, KeepTtl::Never).await.unwrap();
    assert_eq!((cleared.keep_ttl_secs, cleared.expires_at_unix), (None, None));
}

#[sqlx::test]
async fn an_asset_takes_no_node_lifetime(pool: PgPool) {
    let (s, _bucket, _c) = store(&pool).await;
    let w = acting("t1");
    let err = begin_via(&s, &w, &StorageScope::Asset, "b", "f", Some(KeepTtl::Default), &big(), Some(1))
        .await
        .unwrap_err();
    assert!(matches!(err, RuntimeStoreError::Invalid(_)), "{err:?}");
}

#[sqlx::test]
async fn replace_overwrites_the_file_in_place(pool: PgPool) {
    // Same key, name, type and lifetime; new content and size. The old
    // bytes stay readable until the new ones land, the charge follows the
    // new size, and no second file is left behind.
    let (s, bucket, clock) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let f = put_via(&s, &bucket, &w, &StorageScope::Project, "application/json", "chat.json", Some(KeepTtl::Secs { secs: 100 }), &big(), body(b"[]"))
        .await
        .unwrap();
    let parsed = weft_core::storage::key::parse_key(&f.key).unwrap();
    clock.advance(Duration::from_secs(60));

    assert_eq!(f.version, 1, "a file starts at its first version");
    let (upload_key, part_size) = s.begin_replace(&w, &f.key, Some(10), Some(1), &big()).await.unwrap();
    assert_ne!(upload_key, f.key, "the replacement uploads under a key of its own");
    assert_eq!(s.tenant_usage("t1").await.unwrap(), (2, 2 + 10), "both versions count while it is in flight");
    assert_eq!(get_via(&s, &bucket, &parsed, None).await.unwrap().1, body(b"[]"), "readers see the old bytes meanwhile");
    assert_eq!(s.list("t1/project/p1/").await.unwrap().len(), 1, "the upload is not a file anyone lists");
    upload_parts(&s, &bucket, &w, &upload_key, part_size, &body(b"[{\"a\": 1}]"), &big()).await.unwrap();
    let replaced = s.complete_upload(&w, &upload_key).await.unwrap();

    assert_eq!(replaced.key, f.key);
    assert_eq!((replaced.filename.as_str(), replaced.mime_type.as_str()), ("chat.json", "application/json"));
    assert_eq!(replaced.size_bytes, 10);
    assert_eq!(replaced.version, 2, "one write, the next version");
    assert_eq!(replaced.keep_ttl_secs, Some(100), "the lifetime is kept");
    assert_eq!(replaced.expires_at_unix, Some(clock.now_unix() + 100), "a replace is an access");
    assert_eq!(get_via(&s, &bucket, &parsed, None).await.unwrap().1, body(b"[{\"a\": 1}]"));
    assert_eq!(s.tenant_usage("t1").await.unwrap(), (1, 10), "one file, charged at its new size");
}

#[sqlx::test]
async fn a_retried_replacement_complete_answers_with_the_replaced_file(pool: PgPool) {
    // The replacement's own row is gone once it folds, so the retry has
    // to find the file it became, at the version it made.
    let (s, bucket, _c) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let f = put_via(&s, &bucket, &w, &StorageScope::Project, "b", "f", None, &big(), body(b"old"))
        .await
        .unwrap();
    let (upload_key, part_size) = s.begin_replace(&w, &f.key, Some(3), Some(1), &big()).await.unwrap();
    upload_parts(&s, &bucket, &w, &upload_key, part_size, &body(b"new"), &big()).await.unwrap();
    let first = s.complete_upload(&w, &upload_key).await.unwrap();
    let retry = s.complete_upload(&w, &upload_key).await.unwrap();
    assert_eq!((retry.key.as_str(), retry.version), (f.key.as_str(), 2));
    assert_eq!(retry.version, first.version, "a retry moves nothing");
}

/// A file with `old` content and a replacement of it whose parts carrying
/// `new` have all landed, ready to complete: (file, replacement key).
async fn replacement_ready(
    s: &RuntimeStore,
    bucket: &FakeObjectStore,
    w: &CallerAuth,
    old: &[u8],
    new: &[u8],
) -> (StoredFileMeta, String) {
    let f = put_via(s, bucket, w, &StorageScope::Project, "b", "f", None, &big(), body(old)).await.unwrap();
    let (key, part_size) = s.begin_replace(w, &f.key, Some(new.len() as u64), Some(1), &big()).await.unwrap();
    upload_parts(s, bucket, w, &key, part_size, &body(new), &big()).await.unwrap();
    (f, key)
}

/// A new upload whose parts carrying `bytes` have all landed: its key.
async fn upload_ready(s: &RuntimeStore, bucket: &FakeObjectStore, w: &CallerAuth, bytes: &[u8]) -> String {
    let (key, part_size) =
        begin_via(s, w, &StorageScope::Project, "b", "f", None, &big(), Some(bytes.len() as u64)).await.unwrap();
    upload_parts(s, bucket, w, &key, part_size, &body(bytes), &big()).await.unwrap();
    key
}

/// Start a completion and hold it after the bucket assembled the object,
/// before it folds the rows: (the running completion, its release).
async fn completion_held(
    s: &Arc<RuntimeStore>,
    bucket: &FakeObjectStore,
    w: &CallerAuth,
    key: &str,
) -> (tokio::task::JoinHandle<Result<StoredFileMeta, RuntimeStoreError>>, tokio::sync::oneshot::Sender<()>) {
    let (entered, release) = bucket.hold_next_complete();
    let task = tokio::spawn({
        let (s, w, k) = (s.clone(), w.clone(), key.to_string());
        async move { s.complete_upload(&w, &k).await }
    });
    entered.await.unwrap();
    (task, release)
}

async fn status_of(pool: &PgPool, key: &str) -> Option<String> {
    sqlx::query_scalar("SELECT status FROM runtime_file WHERE key = $1").bind(key).fetch_optional(pool).await.unwrap()
}

#[sqlx::test]
async fn concurrent_completions_move_the_version_once(pool: PgPool) {
    // The second complete arrives while the first is mid-way: it finds the
    // upload already gone from the bucket, reads the object, and folds; the
    // first then finds the fold done and answers with it.
    let (s, bucket, _c) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let (f, key) = replacement_ready(&s, &bucket, &w, b"old", b"new").await;
    let completes = || {
        bucket.calls().iter().filter(|c| matches!(c, weft_platform_traits::object_store::fake::FakeCall::CompleteMultipart { .. })).count()
    };
    let before = completes();
    let (first, release) = completion_held(&s, &bucket, &w, &key).await;
    let second = s.complete_upload(&w, &key).await.unwrap();
    release.send(()).unwrap();
    let first = first.await.unwrap().unwrap();
    assert_eq!((first.version, second.version), (2, 2));
    let parsed = weft_core::storage::key::parse_key(&f.key).unwrap();
    assert_eq!(s.meta(&parsed).await.unwrap().version, 2, "one replacement, one version");
    assert_eq!(completes() - before, 1, "the bucket is never asked to complete an upload twice");
}

#[sqlx::test]
async fn an_abort_during_a_completion_is_refused(pool: PgPool) {
    // Once a completion has claimed the upload the bucket may already hold
    // its object: an abort then is refused instead of removing the row.
    let (s, bucket, _c) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let (f, replacement) = replacement_ready(&s, &bucket, &w, b"old", b"newer").await;
    let fresh = upload_ready(&s, &bucket, &w, b"fresh").await;
    for key in [&replacement, &fresh] {
        let (task, release) = completion_held(&s, &bucket, &w, key).await;
        assert!(matches!(s.abort_upload(&w, key).await, Err(RuntimeStoreError::Completing(_))));
        release.send(()).unwrap();
        task.await.unwrap().unwrap();
    }
    let parsed = weft_core::storage::key::parse_key(&f.key).unwrap();
    let meta = s.meta(&parsed).await.unwrap();
    assert_eq!((meta.version, meta.size_bytes), (2, 5), "the row matches the bytes, moved once");
    assert_eq!(get_via(&s, &bucket, &parsed, None).await.unwrap().1, body(b"newer"));
    let parsed = weft_core::storage::key::parse_key(&fresh).unwrap();
    assert_eq!(get_via(&s, &bucket, &parsed, None).await.unwrap().1, body(b"fresh"));
}

#[sqlx::test]
async fn a_delete_during_a_replacement_completion_is_refused(pool: PgPool) {
    let (s, bucket, _c) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let (f, key) = replacement_ready(&s, &bucket, &w, b"old", b"new").await;
    let parsed = weft_core::storage::key::parse_key(&f.key).unwrap();
    let (task, release) = completion_held(&s, &bucket, &w, &key).await;
    match s.delete(&parsed).await {
        Err(RuntimeStoreError::Completing(msg)) => assert!(msg.contains(&key), "names the replacement: {msg}"),
        other => panic!("expected a refusal, got {other:?}"),
    }
    // A wipe of the scope leaves it too, and says so.
    assert!(s.wipe_prefix("t1/").await.is_err());
    release.send(()).unwrap();
    task.await.unwrap().unwrap();
    s.delete(&parsed).await.unwrap();
    assert!(bucket.get(&object_key(&f.key)).await.unwrap().is_none(), "no object outlives its row");
}

#[sqlx::test]
async fn a_resumed_reserve_during_a_completion_is_refused_at_once(pool: PgPool) {
    // The completion holds no lock across the bucket call, so the tenant's
    // other uploads and this one's reservations never wait on it.
    let (s, bucket, _c) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let key = upload_ready(&s, &bucket, &w, b"abc").await;
    let other = begin_via(&s, &w, &StorageScope::Project, "b", "g", None, &big(), None).await.unwrap().0;
    let (task, release) = completion_held(&s, &bucket, &w, &key).await;
    let quick = Duration::from_secs(5);
    let reserve = tokio::time::timeout(quick, s.reserve_parts(&w, &key, &[ask(1, 3)], &big(), WORKER)).await.unwrap();
    assert!(matches!(reserve, Err(RuntimeStoreError::Completing(_))));
    let report = tokio::time::timeout(quick, s.record_part(&w, &key, 1, "\"x\"")).await.unwrap();
    assert!(matches!(report, Err(RuntimeStoreError::Completing(_))));
    // The upload itself cannot be deleted either: it is no file yet, and
    // the answer says why rather than "not found".
    let parsed = weft_core::storage::key::parse_key(&key).unwrap();
    assert!(matches!(s.delete(&parsed).await, Err(RuntimeStoreError::Completing(_))));
    tokio::time::timeout(quick, s.reserve_parts(&w, &other, &[ask(1, 3)], &big(), WORKER)).await.unwrap().unwrap();
    release.send(()).unwrap();
    assert_eq!(task.await.unwrap().unwrap().size_bytes, 3);
}

#[sqlx::test]
async fn a_dropped_completion_the_bucket_finished_is_recovered(pool: PgPool) {
    let (s, bucket, clock) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let (f, key) = replacement_ready(&s, &bucket, &w, b"old", b"new").await;
    let (task, _release) = completion_held(&s, &bucket, &w, &key).await;
    task.abort();
    let _ = task.await;
    assert_eq!(status_of(&pool, &key).await.as_deref(), Some("completing"));
    s.sweep_expired().await.unwrap();
    assert_eq!(status_of(&pool, &key).await.as_deref(), Some("completing"), "a live claim is left alone");
    clock.advance(Duration::from_secs(COMPLETING_LEASE_SECS as u64 + 1));
    s.sweep_expired().await.unwrap();
    assert_eq!(status_of(&pool, &key).await, None, "folded into the file");
    let parsed = weft_core::storage::key::parse_key(&f.key).unwrap();
    let meta = s.meta(&parsed).await.unwrap();
    assert_eq!((meta.version, meta.size_bytes), (2, 3));
    assert_eq!(get_via(&s, &bucket, &parsed, None).await.unwrap().1, body(b"new"));
    // A retried complete from the caller that was cut off answers with it.
    assert_eq!(s.complete_upload(&w, &key).await.unwrap().version, 2);
}

#[sqlx::test]
async fn an_execution_upload_landing_after_its_run_ended_lingers(pool: PgPool) {
    // The run ends while the upload is completing and the bucket is
    // unreachable for it; it lands later, and it carries
    // the same linger deadline as the run's other files.
    let (s, bucket, clock) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let (key, part_size) =
        begin_via(&s, &w, &StorageScope::Execution, "b", "f", None, &big(), Some(3)).await.unwrap();
    upload_parts(&s, &bucket, &w, &key, part_size, &body(b"abc"), &big()).await.unwrap();
    sqlx::query("UPDATE runtime_file SET status = 'completing', progressed_at_unix = $2 WHERE key = $1")
        .bind(&key)
        .bind(clock.now_unix())
        .execute(&pool)
        .await
        .unwrap();
    let ended = clock.now_unix();
    bucket.fail_next_complete();
    s.sweep_exec("t1", "c1").await.unwrap();
    assert_eq!(status_of(&pool, &key).await.as_deref(), Some("completing"), "not landed at the run's end");
    // Landed afterwards (a retried complete; the stale-claim sweep folds
    // the same way), it keeps the linger deadline the run's end stamped.
    s.complete_upload(&w, &key).await.unwrap();
    let expires: Option<i64> = sqlx::query_scalar("SELECT expires_at_unix FROM runtime_file WHERE key = $1 AND status = 'active'")
        .bind(&key)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(expires, Some(ended + EXEC_LINGER_TTL_SECS));
}

#[sqlx::test]
async fn a_dropped_completion_before_the_bucket_is_recovered(pool: PgPool) {
    // Claimed, then the process died before asking the bucket: the sweep
    // finds the multipart still open and completes it.
    let (s, bucket, clock) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let key = upload_ready(&s, &bucket, &w, b"abc").await;
    sqlx::query("UPDATE runtime_file SET status = 'completing', progressed_at_unix = $2 WHERE key = $1")
        .bind(&key)
        .bind(clock.now_unix())
        .execute(&pool)
        .await
        .unwrap();
    clock.advance(Duration::from_secs(COMPLETING_LEASE_SECS as u64 + 1));
    s.sweep_expired().await.unwrap();
    assert_eq!(status_of(&pool, &key).await.as_deref(), Some("active"));
    let parsed = weft_core::storage::key::parse_key(&key).unwrap();
    assert_eq!(get_via(&s, &bucket, &parsed, None).await.unwrap().1, body(b"abc"));
}

#[sqlx::test]
async fn a_completion_the_bucket_cannot_account_for_is_ended(pool: PgPool) {
    // The multipart is gone but no object of the right size is there (it
    // was aborted outside weft): the verdict is final, the upload is
    // reaped and its reservation freed. A replaced file whose object was
    // never overwritten keeps its row and bytes.
    let (s, bucket, clock) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let (f, replacement) = replacement_ready(&s, &bucket, &w, b"old", b"newer").await;
    let fresh = upload_ready(&s, &bucket, &w, b"abc").await;
    for key in [&replacement, &fresh] {
        let upload_id: String = sqlx::query_scalar("SELECT upload_id FROM runtime_file WHERE key = $1")
            .bind(key)
            .fetch_one(&pool)
            .await
            .unwrap();
        sqlx::query("UPDATE runtime_file SET status = 'completing', progressed_at_unix = $2 WHERE key = $1")
            .bind(key)
            .bind(clock.now_unix())
            .execute(&pool)
            .await
            .unwrap();
        let object = if key == &fresh { object_key(key) } else { object_key(&f.key) };
        bucket.abort_multipart(&object, &upload_id).await.unwrap();
        assert!(matches!(s.complete_upload(&w, key).await, Err(RuntimeStoreError::Lost(_))));
        assert_eq!(status_of(&pool, key).await, None, "reaped");
    }
    let parsed = weft_core::storage::key::parse_key(&f.key).unwrap();
    assert_eq!(get_via(&s, &bucket, &parsed, None).await.unwrap().1, body(b"old"));
    assert_eq!(charged_bytes_for(&pool, "t1").await.unwrap(), 3, "only the old file is charged");
}

#[sqlx::test]
async fn a_refused_completion_stays_completing_until_the_sweep_ends_it(pool: PgPool) {
    // Never back to 'pending': another drive may be landing it. The live
    // caller hears "completing"; the sweep past the lease gives the verdict.
    let (s, bucket, clock) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let (key, _) = begin_via(&s, &w, &StorageScope::Project, "b", "f", None, &big(), Some(3)).await.unwrap();
    let part = s.reserve_parts(&w, &key, &[ask(1, 3)], &big(), WORKER).await.unwrap().remove(0);
    bucket.put_part(&part.url, body(b"abc")).unwrap();
    s.record_part(&w, &key, 1, "\"not-the-etag\"").await.unwrap();
    assert!(matches!(s.complete_upload(&w, &key).await, Err(RuntimeStoreError::Completing(_))));
    assert_eq!(status_of(&pool, &key).await.as_deref(), Some("completing"));
    assert!(matches!(s.abort_upload(&w, &key).await, Err(RuntimeStoreError::Completing(_))));
    clock.advance(Duration::from_secs(COMPLETING_LEASE_SECS as u64 + 1));
    s.sweep_expired().await.unwrap();
    assert_eq!(status_of(&pool, &key).await, None, "ended and reaped");
    assert!(bucket.in_progress_uploads().is_empty());
    assert_eq!(charged_bytes_for(&pool, "t1").await.unwrap(), 0);
}

#[sqlx::test]
async fn a_size_mismatched_completion_removes_rows_before_bytes(pool: PgPool) {
    // A part whose bytes differ from its reservation (only a bucket anomaly
    // could do it; here a direct part upload skips the signed length).
    let (s, bucket, _c) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let (key, _) = begin_via(&s, &w, &StorageScope::Project, "b", "f", None, &big(), Some(3)).await.unwrap();
    s.reserve_parts(&w, &key, &[ask(1, 3)], &big(), WORKER).await.unwrap();
    let upload_id = bucket.in_progress_uploads().pop().unwrap();
    let etag = bucket.upload_part(&object_key(&key), &upload_id, 1, body(b"abcd")).await.unwrap();
    s.record_part(&w, &key, 1, &etag).await.unwrap();
    assert!(matches!(s.complete_upload(&w, &key).await, Err(RuntimeStoreError::Lost(_))));
    assert_eq!(status_of(&pool, &key).await, None);
    assert!(bucket.get(&object_key(&key)).await.unwrap().is_none());
    assert_eq!(charged_bytes_for(&pool, "t1").await.unwrap(), 0);
}

#[sqlx::test]
async fn an_abandoned_replacement_leaves_the_file_untouched(pool: PgPool) {
    // Aborted, or reaped by a sweep: the upload goes, the file it was
    // replacing keeps its object and its bytes.
    let (s, bucket, _c) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let f = put_via(&s, &bucket, &w, &StorageScope::Execution, "b", "f", None, &big(), body(b"old"))
        .await
        .unwrap();
    let parsed = weft_core::storage::key::parse_key(&f.key).unwrap();
    let (aborted, _) = s.begin_replace(&w, &f.key, Some(3), None, &big()).await.unwrap();
    s.abort_upload(&w, &aborted).await.unwrap();
    assert_eq!(get_via(&s, &bucket, &parsed, None).await.unwrap().1, body(b"old"));
    // The bytes may have been swapped before an abort (a completion that
    // failed after the bucket took them), so the file moves on anyway.
    assert_eq!(s.meta(&parsed).await.unwrap().version, 2, "an ended replacement always moves the version");

    let (swept, _) = s.begin_replace(&w, &f.key, Some(3), None, &big()).await.unwrap();
    s.sweep_exec("t1", "c1").await.unwrap();
    assert!(bucket.in_progress_uploads().is_empty(), "the replacement's multipart is aborted");
    assert!(bucket.get(&object_key(&f.key)).await.unwrap().is_some(), "the file's object survives the reap");
    assert!(matches!(s.complete_upload(&w, &swept).await, Err(RuntimeStoreError::NotFound(_))));
}

#[sqlx::test]
async fn an_edit_from_an_old_version_is_refused_and_one_write_runs_at_a_time(pool: PgPool) {
    let (s, bucket, _c) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let f = put_via(&s, &bucket, &w, &StorageScope::Project, "application/json", "chat.json", None, &big(), body(b"[]"))
        .await
        .unwrap();
    // A write in flight holds the file: a second one waits its turn.
    let (first, part_size) = s.begin_replace(&w, &f.key, Some(3), Some(1), &big()).await.unwrap();
    assert!(matches!(s.begin_replace(&w, &f.key, Some(2), Some(1), &big()).await, Err(RuntimeStoreError::Conflict(_))));
    assert!(matches!(s.begin_replace(&w, &f.key, Some(2), None, &big()).await, Err(RuntimeStoreError::Conflict(_))));
    upload_parts(&s, &bucket, &w, &first, part_size, &body(b"[1]"), &big()).await.unwrap();
    assert_eq!(s.complete_upload(&w, &first).await.unwrap().version, 2);
    // The second was made from version 1: the file has moved on.
    assert!(matches!(s.begin_replace(&w, &f.key, Some(2), Some(1), &big()).await, Err(RuntimeStoreError::Stale(_))));
    // Made from the version now there, it goes through.
    let (second, part_size) = s.begin_replace(&w, &f.key, Some(5), Some(2), &big()).await.unwrap();
    upload_parts(&s, &bucket, &w, &second, part_size, &body(b"[1,2]"), &big()).await.unwrap();
    let after = s.complete_upload(&w, &second).await.unwrap();
    assert_eq!((after.version, after.size_bytes), (3, 5));
    let parsed = weft_core::storage::key::parse_key(&f.key).unwrap();
    assert_eq!(get_via(&s, &bucket, &parsed, None).await.unwrap().1, body(b"[1,2]"), "both writes landed, in order");
}

#[sqlx::test]
async fn replace_refuses_what_it_cannot_overwrite(pool: PgPool) {
    let (s, bucket, _c) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    // A file that is not there.
    let missing = "t1/project/p1/00000000-0000-0000-0000-000000000000";
    assert!(matches!(s.begin_replace(&w, missing, Some(1), None, &big()).await, Err(RuntimeStoreError::NotFound(_))));
    // Another execution's file: the same wall as a read.
    let f = put_via(&s, &bucket, &w, &StorageScope::Execution, "b", "f", None, &big(), body(b"x"))
        .await
        .unwrap();
    let other = worker("t1", "p1", Some("c2"));
    assert!(matches!(s.begin_replace(&other, &f.key, Some(1), None, &big()).await, Err(RuntimeStoreError::Denied(_))));
    // A file deleted while its replacement was on the way: the new object
    // is removed again and the caller hears the file is gone.
    let (key, part_size) = s.begin_replace(&w, &f.key, Some(1), None, &big()).await.unwrap();
    s.delete(&weft_core::storage::key::parse_key(&f.key).unwrap()).await.unwrap();
    upload_parts(&s, &bucket, &w, &key, part_size, &body(b"y"), &big()).await.unwrap();
    assert!(matches!(s.complete_upload(&w, &key).await, Err(RuntimeStoreError::NotFound(_))));
    assert!(bucket.get(&object_key(&f.key)).await.unwrap().is_none(), "no object without a row");
    assert_eq!(s.tenant_usage("t1").await.unwrap(), (0, 0), "nothing is left charged");
}

#[sqlx::test]
async fn an_in_flight_upload_is_invisible_until_completed(pool: PgPool) {
    // begin reserves a 'pending' row. Before complete, the file must NOT be
    // visible (not in list, get/presign 404) even though a row exists.
    let (s, bucket, _c) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let (key, part_size) =
        begin_via(&s, &w, &StorageScope::Project, "text/plain", "f", None, &big(), Some(2))
            .await
            .unwrap();
    let parsed = weft_core::storage::key::parse_key(&key).unwrap();
    // A pending file is not listed, and reads 404.
    let prefix = weft_core::storage::key::prefix_for_list(&w, &StorageScope::Project).unwrap();
    assert_eq!(s.list(&prefix).await.unwrap().len(), 0, "pending file is not listed");
    assert!(matches!(get_via(&s, &bucket, &parsed, None).await, Err(RuntimeStoreError::NotFound(_))));
    // Upload + complete -> now visible.
    upload_parts(&s, &bucket, &w, &key, part_size, &body(b"hi"), &big()).await.unwrap();
    s.complete_upload(&w, &key).await.unwrap();
    assert_eq!(s.list(&prefix).await.unwrap().len(), 1, "completed file is listed");
}

#[sqlx::test]
async fn upload_verbs_reject_a_key_begin_never_minted(pool: PgPool) {
    // A worker cannot touch an upload the broker never opened: reserving,
    // recording, resuming, and completing an unknown key all fail loud.
    let (s, _bucket, _c) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let key = "t1/exec/c1/forged";
    let err = s.reserve_parts(&w, key, &[ask(1, 1)], &big(), WORKER).await.unwrap_err();
    assert!(matches!(err, RuntimeStoreError::Invalid(_)), "{err:?}");
    let err = s.record_part(&w, key, 1, "\"etag\"").await.unwrap_err();
    assert!(matches!(err, RuntimeStoreError::Invalid(_)), "{err:?}");
    let err = s.resume_upload(&w, key, &big(), WORKER).await.unwrap_err();
    assert!(matches!(err, RuntimeStoreError::NotFound(_)), "{err:?}");
    let err = s.complete_upload(&w, key).await.unwrap_err();
    assert!(matches!(err, RuntimeStoreError::NotFound(_)), "{err:?}");
}

#[sqlx::test]
async fn an_abandoned_upload_is_reaped_after_grace(pool: PgPool) {
    use weft_broker::runtime_store::PENDING_RESERVE_GRACE_SECS;
    // A crashed upload: begin + land a part, never complete. The reservation
    // holds quota and an in-flight multipart. The expiry sweep reaps it once
    // it has made no progress past the grace: multipart aborted, row gone,
    // charge freed.
    let (s, bucket, clock) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    // A project-scoped upload (no exec sweep, no expiry) is the case only this
    // reap covers.
    let (key, _ps) = begin_via(&s, &w, &StorageScope::Project, "b", "f", None, &big(), Some(6))
        .await
        .unwrap();
    let part = &s.reserve_parts(&w, &key, &[ask(1, 6)], &big(), WORKER).await.unwrap()[0];
    let etag = bucket.put_part(&part.url, body(b"orphan")).unwrap();
    s.record_part(&w, &key, part.part_number, &etag).await.unwrap();
    assert_eq!(s.tenant_usage("t1").await.unwrap().1, 6, "in-flight charge visible");
    // Within the grace: not reaped (an upload could still legitimately finish).
    clock.advance(Duration::from_secs((PENDING_RESERVE_GRACE_SECS - 1) as u64));
    assert_eq!(s.sweep_expired().await.unwrap(), 0, "not reaped within grace");
    // Past the grace: reaped, multipart aborted, charge freed.
    clock.advance(Duration::from_secs(2));
    assert_eq!(s.sweep_expired().await.unwrap(), 1, "reaped past grace");
    assert!(bucket.in_progress_uploads().is_empty(), "multipart aborted");
    assert_eq!(s.tenant_usage("t1").await.unwrap(), (0, 0), "no leftover row/charge");
}

#[sqlx::test]
async fn progress_defers_the_abandoned_reap(pool: PgPool) {
    use weft_broker::runtime_store::PENDING_RESERVE_GRACE_SECS;
    // A slow but MOVING upload (parts keep landing) is never reaped mid-flight:
    // each reservation refreshes the progress clock the reap keys on.
    let (s, _bucket, clock) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let (key, part_size) = begin_via(&s, &w, &StorageScope::Project, "b", "slow", None, &big(), None)
        .await
        .unwrap();
    for n in 1..=3 {
        clock.advance(Duration::from_secs((PENDING_RESERVE_GRACE_SECS - 10) as u64));
        s.reserve_parts(&w, &key, &[ask(n, part_size)], &big(), WORKER).await.expect("still alive");
        assert_eq!(s.sweep_expired().await.unwrap(), 0, "progressing upload not reaped");
    }
}

#[sqlx::test]
async fn terminate_sweep_reaps_an_abandoned_exec_upload(pool: PgPool) {
    // The common case: an exec-scoped upload crashes mid-flight. The terminate
    // sweep for that execution aborts the multipart and frees everything
    // immediately (no grace).
    let (s, bucket, _c) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let (key, _ps) = begin_via(&s, &w, &StorageScope::Execution, "b", "f", None, &big(), Some(6))
        .await
        .unwrap();
    let part = &s.reserve_parts(&w, &key, &[ask(1, 6)], &big(), WORKER).await.unwrap()[0];
    bucket.put_part(&part.url, body(b"orphan")).unwrap();
    let (swept, lingering) = s.sweep_exec("t1", "c1").await.unwrap();
    assert_eq!((swept, lingering), (1, 0), "the abandoned exec upload is swept, no linger");
    assert!(bucket.in_progress_uploads().is_empty(), "multipart aborted");
    assert_eq!(s.tenant_usage("t1").await.unwrap(), (0, 0));
}

/// A buffered stream helper is unnecessary (put takes Bytes), but assert the
/// public bytes_stream round-trips so the contract surface stays exercised.
#[tokio::test]
async fn bytes_stream_helper_round_trips() {
    let b = body(b"hello");
    let out = weft_core::storage::collect_stream(bytes_stream(b.clone())).await.unwrap();
    assert_eq!(out, b);
}

#[sqlx::test]
async fn complete_retry_after_success_is_idempotent(pool: PgPool) {
    // A lost-response retry of complete on an already-finalized key returns
    // the existing metadata unchanged and never touches the object.
    let (s, bucket, _clock) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let first = put_via(&s, &bucket, &w, &StorageScope::Execution, "text/plain", "a.txt", None, &big(), body(b"finalized bytes"))
        .await
        .unwrap();
    let retry = s.complete_upload(&w, &first.key).await.unwrap();
    assert_eq!(first.size_bytes, retry.size_bytes);
    assert_eq!(first.filename, retry.filename);
    assert!(bucket.get(&object_key(&first.key)).await.unwrap().is_some(), "object untouched");
}

#[sqlx::test]
async fn a_completed_file_is_immutable(pool: PgPool) {
    // No upload verb can touch a finalized file: its upload state is gone.
    let (s, bucket, _clock) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let meta = put_via(&s, &bucket, &w, &StorageScope::Execution, "text/plain", "a.txt", None, &big(), body(b"original"))
        .await
        .unwrap();
    let err = s.reserve_parts(&w, &meta.key, &[ask(1, 3)], &big(), WORKER).await.unwrap_err();
    assert!(matches!(err, RuntimeStoreError::Invalid(_)), "{err:?}");
    let err = s.resume_upload(&w, &meta.key, &big(), WORKER).await.unwrap_err();
    assert!(matches!(err, RuntimeStoreError::Invalid(_)), "{err:?}");
    let err = s.record_part(&w, &meta.key, 1, "\"e\"").await.unwrap_err();
    assert!(matches!(err, RuntimeStoreError::Invalid(_)), "{err:?}");
}

#[sqlx::test]
async fn wipe_prefix_does_not_touch_a_sibling_execution_id_prefix(pool: PgPool) {
    // The trailing-slash invariant the wall rests on: sweeping execution `c1` must
    // never match a sibling execution whose name merely starts with `c1`.
    let (s, bucket, _clock) = store(&pool).await;
    let scope = StorageScope::Execution;
    let w_short = worker("t1", "p1", Some("c1"));
    let w_long = worker("t1", "p1", Some("c1x"));
    put_via(&s, &bucket, &w_short, &scope, "text/plain", "a.txt", None, &big(), body(b"short"))
        .await
        .unwrap();
    let kept = put_via(&s, &bucket, &w_long, &scope, "text/plain", "b.txt", None, &big(), body(b"long"))
        .await
        .unwrap();
    let (swept, lingering) = s.sweep_exec("t1", "c1").await.unwrap();
    assert_eq!((swept, lingering), (0, 1), "only c1's file is stamped to linger");
    // The sibling's row is untouched: no linger deadline landed on it.
    let sibling = weft_core::storage::key::parse_key(&kept.key).unwrap();
    assert_eq!(s.meta(&sibling).await.unwrap().expires_at_unix, None, "sibling execution c1x untouched");
}

#[sqlx::test]
async fn concurrent_begins_cannot_blow_past_the_byte_cap(pool: PgPool) {
    // The byte-quota check and the reservation are atomic under the tenant
    // lock AT BEGIN: with a cap admitting only one of two declared uploads,
    // exactly one begin wins, before any byte could move.
    let (s, _bucket, _clock) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let scope = StorageScope::Execution;
    let cap = budget(Entitlement { disk_bytes_cap: 10, file_cap: 100 });
    let (r1, r2) = tokio::join!(
        begin_via(&s, &w, &scope, "application/octet-stream", "f1", None, &cap, Some(8)),
        begin_via(&s, &w, &scope, "application/octet-stream", "f2", None, &cap, Some(8)),
    );
    let oks = [r1.is_ok(), r2.is_ok()].iter().filter(|b| **b).count();
    assert_eq!(oks, 1, "exactly one declared upload fits under the cap");
    // The LOSER must fail for being over quota SPECIFICALLY, not for some
    // spurious reason (a serialization conflict, a torn write) that would also
    // leave exactly-one-ok true. This pins that the tenant lock rejected it on
    // the cap.
    let loser = if r1.is_err() { r1.unwrap_err() } else { r2.unwrap_err() };
    assert!(
        matches!(loser, RuntimeStoreError::QuotaExceeded(_)),
        "the losing begin must be rejected as over-quota, got {loser:?}"
    );
    let (_count, bytes) = s.tenant_usage("t1").await.unwrap();
    assert!(bytes <= 10, "charged {bytes} within the cap");
}

#[sqlx::test]
async fn sweep_reap_failure_self_heals_on_retry(pool: PgPool) {
    // A sweep's bucket reap can fail mid-flight (transient store error). The
    // fenced three-step reap (row -> 'reaping', reap bucket, delete row) must
    // leave a residue the NEXT sweep re-finds and finishes: no orphan object
    // without a row, no charged row pointing at deleted bytes.
    let (s, bucket, clock) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let f = put_via(&s, &bucket, &w, &StorageScope::Execution, "b", "f", Some(KeepTtl::Secs { secs: 10 }), &big(), body(b"xyz"))
        .await
        .unwrap();
    let parsed = weft_core::storage::key::parse_key(&f.key).unwrap();
    clock.advance(Duration::from_secs(11));
    // First sweep: the object delete fails after the row is fenced.
    bucket.fail_next_delete(&format!("runtime/{}", f.key));
    let err = s.sweep_expired().await.unwrap_err();
    assert!(format!("{err:#}").contains("injected delete failure"), "{err:?}");
    // Residue: the object is still in the bucket AND a row still points at it
    // (fenced as 'reaping', so reads/keep are locked out but the sweep can
    // re-find it). Nothing is orphaned.
    assert_eq!(bucket.keys(), vec![format!("runtime/{}", f.key)]);
    assert!(matches!(get_via(&s, &bucket, &parsed, None).await, Err(RuntimeStoreError::NotFound(_))));
    assert!(matches!(s.keep(&parsed, KeepTtl::Default).await, Err(RuntimeStoreError::NotFound(_))));
    // Second sweep: re-finds the 'reaping' row and finishes the reap.
    assert_eq!(s.sweep_expired().await.unwrap(), 1);
    assert!(bucket.is_empty(), "object reclaimed on retry");
    assert_eq!(s.tenant_usage("t1").await.unwrap(), (0, 0), "nothing left charged");
}

#[sqlx::test]
async fn naming_a_part_twice_reserves_it_once(pool: PgPool) {
    // Two publishes of the same content upload IDENTICAL bytes (the key is
    // the content's hash), and they used to each ask for "the next part",
    // between them reserve more parts than the file has, and both fail on a
    // total that no longer added up, leaving the upload unfinishable until
    // the hourly sweep removed it. Naming the part is what makes the second
    // asker agree with the first.
    let (s, bucket, _c) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let (key, part_size) =
        begin_via(&s, &w, &StorageScope::Project, "b", "f", None, &big(), Some(9)).await.unwrap();
    assert!(part_size >= 9, "this test wants a single-part file");

    let first = s.reserve_parts(&w, &key, &[ask(1, 9)], &big(), WORKER).await.unwrap();
    let second = s.reserve_parts(&w, &key, &[ask(1, 9)], &big(), WORKER).await.unwrap();
    assert_eq!(first[0].part_number, 1);
    assert_eq!(second[0].part_number, 1, "the same part, not the next one");
    assert_eq!(second[0].offset_bytes, 0);
    // One row, so the reserved total still slices the declared size and the
    // upload can be completed.
    let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM runtime_file_part WHERE key = $1")
        .bind(&key)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(rows, 1, "asking twice reserved once");

    // Either URL lands the part, and the upload completes.
    let etag = bucket.put_part(&second[0].url, body(b"nine byte")).unwrap();
    s.record_part(&w, &key, 1, &etag).await.unwrap();
    let meta = s.complete_upload(&w, &key).await.unwrap();
    assert_eq!(meta.size_bytes, 9);
}

#[sqlx::test]
async fn a_stream_part_is_charged_once_however_often_it_is_named(pool: PgPool) {
    // A stream is charged per part as it is reserved, so a re-ask must not
    // charge again: a retry would otherwise inch the tenant's usage up every
    // time.
    let (s, _bucket, _c) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let (key, part_size) =
        begin_via(&s, &w, &StorageScope::Project, "b", "stream", None, &big(), None).await.unwrap();
    s.reserve_parts(&w, &key, &[ask(1, part_size)], &big(), WORKER).await.unwrap();
    let after_first = s.tenant_usage("t1").await.unwrap().1;
    s.reserve_parts(&w, &key, &[ask(1, part_size)], &big(), WORKER).await.unwrap();
    assert_eq!(s.tenant_usage("t1").await.unwrap().1, after_first, "charged once");
}

#[sqlx::test]
async fn reserved_stream_part_size_cannot_change(pool: PgPool) {
    let (s, _bucket, _clock) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let (key, part_size) = begin_via(&s, &w, &StorageScope::Project, "b", "stream", None, &big(), None).await.unwrap();
    s.reserve_parts(&w, &key, &[ask(1, 1)], &big(), WORKER).await.unwrap();
    let before = s.tenant_usage("t1").await.unwrap();
    assert!(s.reserve_parts(&w, &key, &[ask(1, part_size)], &big(), WORKER).await.is_err());
    assert_eq!(s.tenant_usage("t1").await.unwrap(), before);
    let (_, missing, _) = s.resume_upload(&w, &key, &big(), WORKER).await.unwrap();
    assert_eq!(missing[0].size_bytes, 1);
}

#[sqlx::test]
async fn known_size_parts_keep_their_offsets_when_reserved_out_of_order(pool: PgPool) {
    let (s, bucket, _clock) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let (key, part_size) = begin_via(&s, &w, &StorageScope::Project, "b", "large", None, &big(), Some(20 * 1024 * 1024)).await.unwrap();
    let second = s.reserve_parts(&w, &key, &[ask(2, part_size)], &big(), WORKER).await.unwrap();
    assert_eq!(second[0].offset_bytes, part_size);
    let (_, missing, carved) = s.resume_upload(&w, &key, &big(), WORKER).await.unwrap();
    assert_eq!(missing.iter().map(|part| part.part_number).collect::<Vec<_>>(), vec![1, 2]);
    assert_eq!(missing[0].offset_bytes, 0);
    assert_eq!(carved, 2 * part_size);
    let first = s.reserve_parts(&w, &key, &[ask(1, part_size)], &big(), WORKER).await.unwrap();
    assert_eq!(first[0].offset_bytes, 0);
    let (_, missing, _) = s.resume_upload(&w, &key, &big(), WORKER).await.unwrap();
    assert_eq!(missing.iter().find(|part| part.part_number == 2).unwrap().offset_bytes, part_size);
    let mut parts = missing;
    let total = 20_u64 * 1024 * 1024;
    let tail: Vec<_> = (3..=total.div_ceil(part_size) as i32).map(|number| {
        ask(number, part_size.min(total - (number as u64 - 1) * part_size))
    }).collect();
    if !tail.is_empty() { parts.extend(s.reserve_parts(&w, &key, &tail, &big(), WORKER).await.unwrap()); }
    for part in parts {
        let bytes = vec![part.part_number as u8; part.size_bytes as usize];
        let etag = bucket.put_part(&part.url, body(&bytes)).unwrap();
        s.record_part(&w, &key, part.part_number, &etag).await.unwrap();
    }
    assert_eq!(s.complete_upload(&w, &key).await.unwrap().size_bytes, total);
}
