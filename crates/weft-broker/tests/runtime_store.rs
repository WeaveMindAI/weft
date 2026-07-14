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
    charged_bytes_for, RuntimeStore, RuntimeStoreError, DEFAULT_KEEP_TTL_SECS,
    DEFAULT_PART_SIZE_BYTES, EXEC_LINGER_TTL_SECS,
};
use weft_core::storage::key::CallerAuth;
use weft_core::storage::{bytes_stream, ByteRange, KeepTtl, StorageScope, StoredFileMeta};
use weft_platform_traits::clock::{Clock, FakeClock};
use weft_platform_traits::object_store::fake::FakeObjectStore;
use weft_platform_traits::ObjectStore;

/// A worker caller in (tenant t1, project p1, color c1).
fn worker(tenant: &str, project: &str, color: Option<&str>) -> CallerAuth {
    CallerAuth::Worker {
        tenant: tenant.into(),
        project_id: project.into(),
        color: color.map(String::from),
    }
}

/// A test entitlement source: fixed caps, and (to exercise the account-wide
/// budget) a fixed count of bytes charged in ANOTHER plane, which the runtime
/// store adds to its own charged bytes exactly as the cloud's version-chunk
/// pool would. `extra_other_plane_bytes = 0` is the single-plane default.
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
    weft_broker::runtime_store::migrate(pool).await.unwrap();
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
        let parts = s.reserve_parts(caller, key, &[slice.len() as u64], budget).await?;
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
    let (key, part_size) = s
        .begin_upload(caller, scope, mime, filename, keep, budget, Some(bytes.len() as u64))
        .await?;
    upload_parts(s, bucket, caller, &key, part_size, &bytes, budget).await?;
    s.complete_upload(caller, &key).await
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
    let (key, _ps) = s
        .begin_upload(&w, &StorageScope::Project, "b", "f", None, &big(), None)
        .await
        .unwrap();
    let err = s.reserve_parts(&w, &key, &[0], &big()).await.unwrap_err();
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
async fn wall_denies_cross_color_get(pool: PgPool) {
    let (s, bucket, _c) = store(&pool).await;
    let w1 = worker("t1", "p1", Some("c1"));
    let meta = put_via(&s, &bucket, &w1, &StorageScope::Execution, "text/plain", "f", None, &big(), body(b"x"))
        .await
        .unwrap();
    // The key is under color c1; a parsed key for ANOTHER color under the same
    // tenant must be denied by the wall (the store applies check_key_access via
    // the route, but here we assert the key the put minted is color-scoped).
    assert!(meta.key.contains("/exec/c1/"));
    // A worker with a different color cannot mint a key for c1's file: the put
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
    let err = s
        .begin_upload(&w2, &StorageScope::Project, "b", "big", None, &over, Some(5))
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
    let err = s
        .begin_upload(&w, &StorageScope::Project, "b", "over", None, &terms, Some(1))
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
    let (key, part_size) = s
        .begin_upload(&w, &StorageScope::Project, "b", "stream", None, &cap, None)
        .await
        .unwrap();
    // First full part fits under the cap.
    s.reserve_parts(&w, &key, &[part_size], &cap).await.expect("first part fits");
    assert_eq!(s.tenant_usage("t1").await.unwrap().1, part_size, "in-flight bytes are charged");
    // The next full part would cross the cap: rejected AND the upload aborted.
    let err = s.reserve_parts(&w, &key, &[part_size], &cap).await.unwrap_err();
    assert!(matches!(err, RuntimeStoreError::QuotaExceeded(_)), "{err:?}");
    assert!(bucket.in_progress_uploads().is_empty(), "multipart aborted");
    assert_eq!(s.tenant_usage("t1").await.unwrap(), (0, 0), "reservation freed");
    // The upload is gone: further reservations are rejected.
    let err = s.reserve_parts(&w, &key, &[1], &cap).await.unwrap_err();
    assert!(matches!(err, RuntimeStoreError::Invalid(_)), "{err:?}");
}

#[sqlx::test]
async fn a_part_with_the_wrong_byte_count_is_rejected_by_the_signed_length(pool: PgPool) {
    // The quota lock itself: the URL is signed for an exact size, so a body of
    // any other length is rejected by the bucket and nothing lands.
    let (s, bucket, _c) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let (key, _part_size) = s
        .begin_upload(&w, &StorageScope::Project, "b", "f", None, &big(), Some(5))
        .await
        .unwrap();
    let parts = s.reserve_parts(&w, &key, &[5], &big()).await.unwrap();
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
    let (key, _ps) = s
        .begin_upload(&w, &StorageScope::Project, "b", "f", None, &big(), Some(10))
        .await
        .unwrap();
    // The only valid slicing of a 10-byte declared total is one 10-byte part.
    let err = s.reserve_parts(&w, &key, &[7], &big()).await.unwrap_err();
    assert!(matches!(err, RuntimeStoreError::Invalid(_)), "{err:?}");
    let parts = s.reserve_parts(&w, &key, &[10], &big()).await.unwrap();
    assert_eq!(parts[0].size_bytes, 10);
    // Nothing can be reserved after the final part.
    let err = s.reserve_parts(&w, &key, &[1], &big()).await.unwrap_err();
    assert!(matches!(err, RuntimeStoreError::Invalid(_)), "{err:?}");
}

#[sqlx::test]
async fn resume_re_presigns_exactly_the_missing_parts(pool: PgPool) {
    // Stream two parts; land only the second. Resume must offer exactly the
    // first (size preserved), and completing before it lands must fail loud.
    let (s, bucket, _c) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let (key, part_size) = s
        .begin_upload(&w, &StorageScope::Project, "b", "f", None, &big(), None)
        .await
        .unwrap();
    let p1 = &s.reserve_parts(&w, &key, &[part_size], &big()).await.unwrap()[0];
    let p2 = &s.reserve_parts(&w, &key, &[5], &big()).await.unwrap()[0];
    let etag2 = bucket.put_part(&p2.url, body(b"tail!")).unwrap();
    s.record_part(&w, &key, p2.part_number, &etag2).await.unwrap();

    // Complete refuses while part 1 is missing, and names the recovery.
    let err = s.complete_upload(&w, &key).await.unwrap_err();
    assert!(err.to_string().contains("resume"), "{err}");

    let (resumed_part_size, missing) = s.resume_upload(&w, &key).await.unwrap();
    assert_eq!(resumed_part_size, part_size);
    assert_eq!(missing.len(), 1);
    assert_eq!(missing[0].part_number, p1.part_number);
    assert_eq!(missing[0].size_bytes, part_size);

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
    let (key, _ps) = s
        .begin_upload(&w, &StorageScope::Project, "b", "f", None, &big(), Some(3))
        .await
        .unwrap();
    let part = &s.reserve_parts(&w, &key, &[3], &big()).await.unwrap()[0];
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
    let (key, _ps) = s
        .begin_upload(&w, &StorageScope::Project, "b", "f", None, &cap, Some(8))
        .await
        .unwrap();
    // The charge blocks a second 8-byte begin.
    let err = s
        .begin_upload(&w, &StorageScope::Project, "b", "g", None, &cap, Some(8))
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

#[sqlx::test]
async fn keep_rejects_non_exec_scope(pool: PgPool) {
    let (s, bucket, _c) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    // project files are persistent without a flag: keep at upload is rejected.
    let err = put_via(&s, &bucket, &w, &StorageScope::Project, "b", "f", Some(KeepTtl::Default), &big(), body(b"x"))
        .await
        .unwrap_err();
    assert!(matches!(err, RuntimeStoreError::Invalid(_)), "{err:?}");
}

#[sqlx::test]
async fn an_in_flight_upload_is_invisible_until_completed(pool: PgPool) {
    // begin reserves a 'pending' row. Before complete, the file must NOT be
    // visible (not in list, get/presign 404) even though a row exists.
    let (s, bucket, _c) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let (key, part_size) = s
        .begin_upload(&w, &StorageScope::Project, "text/plain", "f", None, &big(), Some(2))
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
    let err = s.reserve_parts(&w, key, &[1], &big()).await.unwrap_err();
    assert!(matches!(err, RuntimeStoreError::Invalid(_)), "{err:?}");
    let err = s.record_part(&w, key, 1, "\"etag\"").await.unwrap_err();
    assert!(matches!(err, RuntimeStoreError::Invalid(_)), "{err:?}");
    let err = s.resume_upload(&w, key).await.unwrap_err();
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
    let (key, _ps) = s
        .begin_upload(&w, &StorageScope::Project, "b", "f", None, &big(), Some(6))
        .await
        .unwrap();
    let part = &s.reserve_parts(&w, &key, &[6], &big()).await.unwrap()[0];
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
    let (key, part_size) = s
        .begin_upload(&w, &StorageScope::Project, "b", "slow", None, &big(), None)
        .await
        .unwrap();
    for _ in 0..3 {
        clock.advance(Duration::from_secs((PENDING_RESERVE_GRACE_SECS - 10) as u64));
        s.reserve_parts(&w, &key, &[part_size], &big()).await.expect("still alive");
        assert_eq!(s.sweep_expired().await.unwrap(), 0, "progressing upload not reaped");
    }
}

#[sqlx::test]
async fn terminate_sweep_reaps_an_abandoned_exec_upload(pool: PgPool) {
    // The common case: an exec-scoped upload crashes mid-flight. The terminate
    // sweep for that color aborts the multipart and frees everything
    // immediately (no grace).
    let (s, bucket, _c) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let (key, _ps) = s
        .begin_upload(&w, &StorageScope::Execution, "b", "f", None, &big(), Some(6))
        .await
        .unwrap();
    let part = &s.reserve_parts(&w, &key, &[6], &big()).await.unwrap()[0];
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
    let err = s.reserve_parts(&w, &meta.key, &[3], &big()).await.unwrap_err();
    assert!(matches!(err, RuntimeStoreError::Invalid(_)), "{err:?}");
    let err = s.resume_upload(&w, &meta.key).await.unwrap_err();
    assert!(matches!(err, RuntimeStoreError::Invalid(_)), "{err:?}");
    let err = s.record_part(&w, &meta.key, 1, "\"e\"").await.unwrap_err();
    assert!(matches!(err, RuntimeStoreError::Invalid(_)), "{err:?}");
}

#[sqlx::test]
async fn wipe_prefix_does_not_touch_a_sibling_color_prefix(pool: PgPool) {
    // The trailing-slash invariant the wall rests on: sweeping color `c1` must
    // never match a sibling color whose name merely starts with `c1`.
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
    assert_eq!(s.meta(&sibling).await.unwrap().expires_at_unix, None, "sibling color c1x untouched");
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
        s.begin_upload(&w, &scope, "application/octet-stream", "f1", None, &cap, Some(8)),
        s.begin_upload(&w, &scope, "application/octet-stream", "f2", None, &cap, Some(8)),
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
