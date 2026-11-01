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

use std::sync::Arc;
use std::time::Duration;

use weft_broker::entitlement::{Entitlement, EntitlementSource};
use weft_broker::runtime_store::{
    charged_bytes_for, BeginUpload, RuntimeStore, RuntimeStoreError, UploadSpec,
    CLAIM_LEASE_SECS, DEFAULT_KEEP_TTL_SECS, DEFAULT_PART_SIZE_BYTES, PART_LINK_SECS,
};
use weft_core::storage::key::CallerAuth;
use weft_core::storage::{bytes_stream, ByteRange, KeepTtl, PartAsk, StorageScope, StoredFileMeta};
use weft_platform_traits::clock::{Clock, FakeClock};
use weft_platform_traits::object_store::fake::FakeObjectStore;
use weft_platform_traits::{CopyProgress, ObjectStore, PresignAudience};
use weft_task_store::db::{Client as _, Pool, Session, Transaction};

/// One part asked for by number and size. A part number IS the part's
/// position in the file, so a test that reserves the first part says 1.
fn ask(part_number: i32, size_bytes: u64) -> PartAsk {
    PartAsk { part_number, size_bytes }
}

/// Every upload test here drives the WORKER upload path, whose part URLs are
/// presigned for the install's own endpoint. Named once so the many
/// `reserve_parts` / `resume_upload` call sites read cleanly. (The External
/// audience, the browser-facing URLs, is what the `link` tests below ask for.)
const WORKER: PresignAudience = PresignAudience::Install;

/// A worker caller in (tenant t1, project p1, run c1).
fn worker(tenant: &str, project: &str, run_id: Option<&str>) -> CallerAuth {
    CallerAuth::Worker {
        tenant: tenant.into(),
        project_id: project.into(),
        run_id: run_id.map(String::from),
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
        tx: &Transaction<'_>,
        tenant: &str,
        now: i64,
    ) -> anyhow::Result<u64> {
        let mine = charged_bytes_for(tx, tenant, now).await?;
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

async fn store(pool: &Pool) -> (Arc<RuntimeStore>, Arc<FakeObjectStore>, Arc<FakeClock>) {
    weft_task_store::apply_groups(pool, &[&weft_broker::runtime_store::GROUP, &weft_task_store::unanswered::GROUP])
        .await
        .unwrap();
    // Every write of a failure (a reap the bucket refused) reads these.
    weft_task_store::unanswered::judged_run_columns(pool).await.unwrap();
    let clock = FakeClock::new();
    let bucket = Arc::new(FakeObjectStore::new());
    (
        Arc::new(RuntimeStore::new(pool.clone(), bucket.clone(), clock.clone())),
        bucket,
        clock,
    )
}

/// The deletion of `run`'s files, which the bucket refused nothing of:
/// how many rows went.
async fn delete_run_files(s: &RuntimeStore, tenant: &str, run: &str) -> u64 {
    s.delete_run_files(tenant, &[run.to_string()]).await.unwrap()
}

/// One pass of the expiry loop: how many rows it removed.
async fn expire(s: &RuntimeStore, pool: &Pool) -> u64 {
    let rows = || async {
        let rows: i64 = pool.scalar("SELECT COUNT(*) FROM runtime_file", &[]).await.unwrap();
        rows
    };
    let before = rows().await;
    s.expire().await.unwrap();
    (before - rows().await) as u64
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
            s.reserve_parts(caller, key, &[ask(part_number, slice.len() as u64)], budget, &WORKER).await?;
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
                slot: None,
                imported: None,
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
            &UploadSpec { scope, mime, filename, keep, declared_size, content_hash: None, identity: None, slot: None, imported: None },
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
    let (meta, _url, _) = s.link(parsed, PresignAudience::Install, None, false).await?;
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

#[weft_db_macros::test]
async fn put_then_get_round_trips_and_records_metadata(pool: Pool) {
    let (s, bucket, _clock) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let meta = put_via(&s, &bucket, &w, &StorageScope::Run, "text/plain", "hi.txt", None, &big(), body(b"hello"))
        .await
        .expect("put");
    assert_eq!(meta.mime_type, "text/plain");
    assert_eq!(meta.size_bytes, 5);
    assert!(meta.key.starts_with("t1/run/c1/"));
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
#[weft_db_macros::test]
async fn an_identified_put_stores_one_file_per_scope(pool: Pool) {
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
        slot: None,
        imported: None,
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

#[weft_db_macros::test]
async fn an_empty_file_round_trips_as_a_direct_empty_object(pool: Pool) {
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

#[weft_db_macros::test]
async fn a_zero_byte_part_reservation_is_rejected(pool: Pool) {
    // The store must never reserve a zero-byte part (S3 would reject it at
    // complete). An empty file goes through zero parts instead.
    let (s, _bucket, _c) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let (key, _ps) = begin_via(&s, &w, &StorageScope::Project, "b", "f", None, &big(), None)
        .await
        .unwrap();
    let err = s.reserve_parts(&w, &key, &[ask(1, 0)], &big(), &WORKER).await.unwrap_err();
    assert!(matches!(err, RuntimeStoreError::Invalid(_)), "{err:?}");
}

#[weft_db_macros::test]
async fn a_multi_part_upload_assembles_in_order(pool: Pool) {
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

#[weft_db_macros::test]
async fn another_run_reads_a_runs_file_and_another_tenant_cannot(pool: Pool) {
    let (s, bucket, _c) = store(&pool).await;
    let w1 = worker("t1", "p1", Some("c1"));
    let meta = put_via(&s, &bucket, &w1, &StorageScope::Run, "text/plain", "f", None, &big(), body(b"x"))
        .await
        .unwrap();
    // The put minted a key under its own run.
    assert!(meta.key.contains("/run/c1/"));
    // Any run of any project of the tenant reads it; another tenant's
    // worker never does.
    let parsed = weft_core::storage::key::parse_key(&meta.key).unwrap();
    for reader in [worker("t1", "p1", Some("c2")), worker("t1", "p2", None)] {
        assert!(weft_core::storage::key::check_key_access(&reader, &parsed).is_ok(), "{reader:?}");
    }
    assert!(weft_core::storage::key::check_key_access(&worker("t2", "p1", Some("c1")), &parsed).is_err());
    assert_eq!(get_via(&s, &bucket, &parsed, None).await.unwrap().1, body(b"x"));
}

#[weft_db_macros::test]
async fn quota_rejects_over_file_count_and_over_declared_bytes(pool: Pool) {
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

#[weft_db_macros::test]
async fn other_plane_bytes_count_against_the_same_cap(pool: Pool) {
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

#[weft_db_macros::test]
async fn a_streaming_upload_that_crosses_the_cap_is_aborted(pool: Pool) {
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
    s.reserve_parts(&w, &key, &[ask(1, part_size)], &cap, &WORKER).await.expect("first part fits");
    assert_eq!(s.tenant_usage("t1").await.unwrap().1, part_size, "in-flight bytes are charged");
    // The next full part would cross the cap: rejected AND the upload aborted.
    let err = s.reserve_parts(&w, &key, &[ask(2, part_size)], &cap, &WORKER).await.unwrap_err();
    assert!(matches!(err, RuntimeStoreError::QuotaExceeded(_)), "{err:?}");
    assert!(bucket.in_progress_uploads().is_empty(), "multipart aborted");
    assert_eq!(s.tenant_usage("t1").await.unwrap(), (0, 0), "reservation freed");
    // The upload is gone: further reservations are rejected.
    let err = s.reserve_parts(&w, &key, &[ask(1, 1)], &cap, &WORKER).await.unwrap_err();
    assert!(matches!(err, RuntimeStoreError::Invalid(_)), "{err:?}");
}

#[weft_db_macros::test]
async fn a_part_with_the_wrong_byte_count_is_rejected_by_the_signed_length(pool: Pool) {
    // The quota lock itself: the URL is signed for an exact size, so a body of
    // any other length is rejected by the bucket and nothing lands.
    let (s, bucket, _c) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let (key, _part_size) = begin_via(&s, &w, &StorageScope::Project, "b", "f", None, &big(), Some(5))
        .await
        .unwrap();
    let parts = s.reserve_parts(&w, &key, &[ask(1, 5)], &big(), &WORKER).await.unwrap();
    let err = bucket.put_part(&parts[0].url, body(b"way too many bytes")).unwrap_err();
    assert!(err.to_string().contains("signature mismatch"), "{err}");
    let err = bucket.put_part(&parts[0].url, body(b"srt")).unwrap_err();
    assert!(err.to_string().contains("signature mismatch"), "{err}");
    // The exact size lands.
    bucket.put_part(&parts[0].url, body(b"12345")).unwrap();
}

#[weft_db_macros::test]
async fn part_sizes_must_slice_the_declared_total_exactly(pool: Pool) {
    let (s, _bucket, _c) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let (key, _ps) = begin_via(&s, &w, &StorageScope::Project, "b", "f", None, &big(), Some(10))
        .await
        .unwrap();
    // The only valid slicing of a 10-byte declared total is one 10-byte part.
    let err = s.reserve_parts(&w, &key, &[ask(1, 7)], &big(), &WORKER).await.unwrap_err();
    assert!(matches!(err, RuntimeStoreError::Invalid(_)), "{err:?}");
    let parts = s.reserve_parts(&w, &key, &[ask(1, 10)], &big(), &WORKER).await.unwrap();
    assert_eq!(parts[0].size_bytes, 10);
    // Nothing can be reserved after the final part.
    let err = s.reserve_parts(&w, &key, &[ask(2, 1)], &big(), &WORKER).await.unwrap_err();
    assert!(matches!(err, RuntimeStoreError::Invalid(_)), "{err:?}");
}

#[weft_db_macros::test]
async fn resume_re_presigns_exactly_the_missing_parts(pool: Pool) {
    // Stream two parts; land only the second. Resume must offer exactly the
    // first (size preserved), and completing before it lands must fail loud.
    let (s, bucket, _c) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let (key, part_size) = begin_via(&s, &w, &StorageScope::Project, "b", "f", None, &big(), None)
        .await
        .unwrap();
    let p1 = &s.reserve_parts(&w, &key, &[ask(1, part_size)], &big(), &WORKER).await.unwrap()[0];
    let p2 = &s.reserve_parts(&w, &key, &[ask(2, 5)], &big(), &WORKER).await.unwrap()[0];
    let etag2 = bucket.put_part(&p2.url, body(b"tail!")).unwrap();
    s.record_part(&w, &key, p2.part_number, &etag2).await.unwrap();

    // Complete refuses while part 1 is missing, and names the recovery.
    let err = s.complete_upload(&w, &key).await.unwrap_err();
    assert!(err.to_string().contains("resume"), "{err}");

    let (resumed_part_size, missing, carved) = s.resume_upload(&w, &key, &big(), &WORKER).await.unwrap();
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

#[weft_db_macros::test]
async fn record_part_is_idempotent_and_rejects_unreserved_parts(pool: Pool) {
    let (s, bucket, _c) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let (key, _ps) = begin_via(&s, &w, &StorageScope::Project, "b", "f", None, &big(), Some(3))
        .await
        .unwrap();
    let part = &s.reserve_parts(&w, &key, &[ask(1, 3)], &big(), &WORKER).await.unwrap()[0];
    let etag = bucket.put_part(&part.url, body(b"abc")).unwrap();
    s.record_part(&w, &key, part.part_number, &etag).await.unwrap();
    // Re-reporting the same part is fine (retry of a lost response).
    s.record_part(&w, &key, part.part_number, &etag).await.unwrap();
    // A part number that was never reserved is rejected loud.
    let err = s.record_part(&w, &key, 99, &etag).await.unwrap_err();
    assert!(matches!(err, RuntimeStoreError::Invalid(_)), "{err:?}");
    s.complete_upload(&w, &key).await.unwrap();
}

#[weft_db_macros::test]
async fn abort_frees_the_reservation(pool: Pool) {
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

/// A run's files outlive the run's end: they have no deadline unless one
/// was given, and they go, every one, when the run's files are deleted
/// (its record went), whatever their deadline. Another run's files and the
/// project's stay.
#[weft_db_macros::test]
async fn a_runs_files_go_with_its_record_and_nothing_else_does(pool: Pool) {
    let (s, bucket, clock) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let plain = put_via(&s, &bucket, &w, &StorageScope::Run, "b", "plain", None, &big(), body(b"s")).await.unwrap();
    let dated = put_via(&s, &bucket, &w, &StorageScope::Run, "b", "dated", Some(KeepTtl::Default), &big(), body(b"k"))
        .await
        .unwrap();
    let other = put_via(&s, &bucket, &worker("t1", "p1", Some("c2")), &StorageScope::Run, "b", "other", None, &big(), body(b"o"))
        .await
        .unwrap();
    let project = put_via(&s, &bucket, &w, &StorageScope::Project, "b", "proj", None, &big(), body(b"p")).await.unwrap();
    assert_eq!(plain.expires_at_unix, None, "a run file with no deadline has none");
    // Long after the run, nothing expired on its own.
    clock.advance(Duration::from_secs(DEFAULT_KEEP_TTL_SECS / 2));
    assert_eq!(expire(&s, &pool).await, 0);
    assert_eq!(delete_run_files(&s, "t1", "c1").await, 2);
    for gone in [&plain, &dated] {
        let parsed = weft_core::storage::key::parse_key(&gone.key).unwrap();
        assert!(matches!(get_via(&s, &bucket, &parsed, None).await, Err(RuntimeStoreError::NotFound(_))), "{}", gone.key);
    }
    for kept in [&other, &project] {
        let parsed = weft_core::storage::key::parse_key(&kept.key).unwrap();
        assert!(get_via(&s, &bucket, &parsed, None).await.is_ok(), "{}", kept.key);
    }
    assert_eq!(delete_run_files(&s, "t1", "c1").await, 0, "deleting again finds nothing");
}

/// A deadline never cuts a link short: a file whose deadline comes before
/// the end of a link handed out for it lives until the link's end, then
/// goes.
#[weft_db_macros::test]
async fn a_link_outlives_a_deadline_and_the_file_goes_after_it(pool: Pool) {
    let (s, bucket, clock) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let file = put_via(&s, &bucket, &w, &StorageScope::Run, "b", "img", None, &big(), body(b"png"))
        .await
        .unwrap();
    let parsed = weft_core::storage::key::parse_key(&file.key).unwrap();
    let link_ttl: u64 = 900;
    let (_, _, link_end) = s.link(&parsed, PresignAudience::External, Some(link_ttl), true).await.unwrap();
    let deadline: u64 = 60;
    assert!(link_ttl > deadline, "the test needs the link to outlast the deadline");
    s.keep(&parsed, KeepTtl::Secs { secs: deadline }).await.unwrap();
    assert_eq!(s.meta(&parsed).await.unwrap().expires_at_unix, Some(link_end));

    // Past the deadline but inside the link's life: the file lives.
    clock.advance(Duration::from_secs(deadline + 1));
    assert_eq!(expire(&s, &pool).await, 0);

    // Past the link's end, the expiry loop reclaims the file.
    clock.advance(Duration::from_secs((link_end - clock.now_unix()) as u64 + 1));
    assert_eq!(expire(&s, &pool).await, 1);
    assert!(matches!(get_via(&s, &bucket, &parsed, None).await, Err(RuntimeStoreError::NotFound(_))));
}

/// A link is signed at the start of its window, so the same file asked
/// for again within the window is the same link, for every asker, and a
/// new one after it; each works at least as long as asked from when it
/// was asked, and answers as a download of the file under its type.
#[weft_db_macros::test]
async fn the_same_file_gets_the_same_link_within_its_window(pool: Pool) {
    let (s, bucket, clock) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let file = put_via(&s, &bucket, &w, &StorageScope::Project, "video/mp4", "clip.mp4", None, &big(), body(b"v"))
        .await
        .unwrap();
    let parsed = weft_core::storage::key::parse_key(&file.key).unwrap();
    let ask = || async { s.link(&parsed, PresignAudience::External, Some(900), true).await.unwrap() };
    let window = weft_broker::runtime_store::link_window(clock.now_unix(), 900);
    let (_, first, until) = ask().await;
    clock.advance(Duration::from_secs(1));
    let (_, again, _) = ask().await;
    assert_eq!(first, again, "the same link within the window");
    assert!(until >= clock.now_unix() + 900 - 1);
    clock.advance(Duration::from_secs(window.works_for));
    let (_, later, later_until) = ask().await;
    assert_ne!(first, later, "a new window, a new link");
    assert!(later_until >= clock.now_unix() + 900);
    let signed = bucket
        .calls()
        .into_iter()
        .find_map(|call| match call {
            weft_platform_traits::object_store::fake::FakeCall::PresignGet { link, .. } => Some(link),
            _ => None,
        })
        .expect("a link was signed");
    assert_eq!(signed.content_type.as_deref(), Some("video/mp4"));
    assert_eq!(signed.disposition.as_deref(), Some(weft_core::storage::download_disposition("clip.mp4").as_str()));
    assert!(matches!(
        s.link(&parsed, PresignAudience::External, Some(8 * 24 * 3600), true).await,
        Err(RuntimeStoreError::Invalid(_))
    ));
}

#[weft_db_macros::test]
async fn no_expiry_write_shortens_a_file_below_its_live_link(pool: Pool) {
    let (s, bucket, clock) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    // A kept file on a SHORT keep TTL, carrying a LONG-lived link.
    let file = put_via(&s, &bucket, &w, &StorageScope::Run, "b", "img", Some(KeepTtl::Secs { secs: 60 }), &big(), body(b"png"))
        .await
        .unwrap();
    let parsed = weft_core::storage::key::parse_key(&file.key).unwrap();
    let (_, _, link_expiry) = s.link(&parsed, PresignAudience::External, Some(3600), true).await.unwrap();
    assert_eq!(s.meta(&parsed).await.unwrap().expires_at_unix, Some(link_expiry), "the link covered");

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
    clock.advance(Duration::from_secs((link_expiry - clock.now_unix()) as u64 + 1));
    assert_eq!(expire(&s, &pool).await, 1, "past the link cover the file dies normally");
}

#[weft_db_macros::test]
async fn keep_then_expiry_sweep_reclaims_after_ttl(pool: Pool) {
    let (s, bucket, clock) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let f = put_via(&s, &bucket, &w, &StorageScope::Run, "b", "f", Some(KeepTtl::Secs { secs: 100 }), &big(), body(b"x"))
        .await
        .unwrap();
    let parsed = weft_core::storage::key::parse_key(&f.key).unwrap();
    // Before the TTL, the expiry sweep keeps it.
    clock.advance(Duration::from_secs(50));
    assert_eq!(expire(&s, &pool).await, 0);
    assert!(get_via(&s, &bucket, &parsed, None).await.is_ok());
    // A get bumps the expiry to now+100, so advancing another 80s (total 130 >
    // 100 from put, but only 80 since the access-bump) still keeps it.
    clock.advance(Duration::from_secs(80));
    assert_eq!(expire(&s, &pool).await, 0, "access bumped the expiry");
    // Now let it sit past the bumped TTL.
    clock.advance(Duration::from_secs(101));
    assert_eq!(expire(&s, &pool).await, 1);
    assert!(matches!(get_via(&s, &bucket, &parsed, None).await, Err(RuntimeStoreError::NotFound(_))));
}

#[weft_db_macros::test]
async fn keep_default_resolves_to_default_ttl(pool: Pool) {
    let (s, bucket, _c) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let f = put_via(&s, &bucket, &w, &StorageScope::Run, "b", "f", Some(KeepTtl::Default), &big(), body(b"x"))
        .await
        .unwrap();
    assert_eq!(f.keep_ttl_secs, Some(DEFAULT_KEEP_TTL_SECS));
    assert!(f.expires_at_unix.is_some());
}

#[weft_db_macros::test]
async fn keep_never_has_no_expiry(pool: Pool) {
    // KeepTtl::Never must resolve to NO expiry (ttl None -> expires_at None), so a
    // "keep forever" file is never silently reclaimed by the expiry sweep.
    let (s, bucket, _c) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let f = put_via(&s, &bucket, &w, &StorageScope::Run, "b", "f", Some(KeepTtl::Never), &big(), body(b"x"))
        .await
        .unwrap();
    assert_eq!(f.keep_ttl_secs, None);
    assert_eq!(f.expires_at_unix, None);
}

#[weft_db_macros::test]
async fn list_is_scoped_and_wipe_prefix_clears_it(pool: Pool) {
    let (s, bucket, _c) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    put_via(&s, &bucket, &w, &StorageScope::Project, "b", "a", None, &big(), body(b"a")).await.unwrap();
    put_via(&s, &bucket, &w, &StorageScope::Project, "b", "b", None, &big(), body(b"b")).await.unwrap();
    put_via(&s, &bucket, &w, &StorageScope::Run, "b", "e", None, &big(), body(b"e")).await.unwrap();
    // list project scope sees only project files.
    let prefix = weft_core::storage::key::prefix_for_list(&w, &StorageScope::Project).unwrap();
    assert_eq!(s.list(&prefix).await.unwrap().len(), 2);
    // wipe the project prefix clears them, leaves the run's.
    let wiped = s.wipe_prefix(&prefix).await.unwrap();
    assert_eq!(wiped, 2);
    assert_eq!(s.list(&prefix).await.unwrap().len(), 0);
    assert_eq!(s.tenant_usage("t1").await.unwrap().0, 1); // the run file remains
}

#[weft_db_macros::test]
async fn assemble_concatenates_existing_objects_into_an_asset(pool: Pool) {
    let (s, bucket, _c) = store(&pool).await;
    let w = acting("t1");

    // Two "chunk" objects already in the bucket (another storage plane);
    // assembly must concatenate them in order into one ledgered asset without
    // any caller moving bytes.
    bucket.put("chunks/c1", body(b"HELLO ")).await.unwrap();
    bucket.put("chunks/c2", body(b"WORLD")).await.unwrap();
    let sha = sha256_hex(b"HELLO WORLD");
    let sha_static: &'static str = Box::leak(sha.clone().into_boxed_str());
    let spec = UploadSpec {
        scope: &StorageScope::Asset,
        mime: "text/plain",
        filename: "assets/greeting.txt",
        keep: None,
        declared_size: Some(11),
        content_hash: Some(sha_static),
        identity: None,
        slot: None,
        imported: None,
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
    let pending: i64 = pool.scalar("SELECT COUNT(*) FROM runtime_file WHERE status = 'pending'", &[]).await.unwrap();
    assert_eq!(pending, 0, "failed assembly left no reservation");
}

#[weft_db_macros::test]
async fn asset_uploads_are_content_addressed_and_conflict_on_duplicates(pool: Pool) {
    let (s, bucket, _c) = store(&pool).await;
    let w = acting("t1");
    let sha = sha256_hex(b"weft");
    let asset_spec = |hash: Option<&'static str>| UploadSpec {
        scope: &StorageScope::Asset,
        mime: "image/png",
        filename: "assets/pic.png",
        keep: None,
        declared_size: Some(4),
        content_hash: hash,
        identity: None,
        slot: None,
        imported: None,
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

    // Once another publish of the same bytes has claimed the completion, a
    // begin is told so (the marked "ask again" refusal), not that the key
    // is being cleared: the asset lands in a moment, and the caller waits
    // for it by its hash.
    pool.execute("UPDATE runtime_file SET status = 'completing' WHERE key = $1", &[&key]).await.unwrap();
    assert!(
        matches!(s.begin_upload(&w, &asset_spec(Some(sha_static)), &big()).await, Err(RuntimeStoreError::Completing(_))),
        "a begin that meets a completing upload of the same content is told to wait"
    );
    pool.execute("UPDATE runtime_file SET status = 'pending' WHERE key = $1", &[&key]).await.unwrap();

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

/// An asset's key is the sha256 of its bytes, and the store holds every
/// upload to it: bytes that hash to anything else are refused at complete,
/// naming the file and both hashes, and leave neither a row nor an object,
/// so the key is free for the right bytes, which then complete.
#[weft_db_macros::test]
async fn an_asset_whose_bytes_are_not_its_hash_is_refused_and_leaves_nothing(pool: Pool) {
    let (s, bucket, _c) = store(&pool).await;
    let w = acting("t1");
    let sha = sha256_hex(b"weft");
    let spec = UploadSpec {
        scope: &StorageScope::Asset,
        mime: "text/plain",
        filename: "assets/word.txt",
        keep: None,
        declared_size: Some(4),
        content_hash: Some(&sha),
        identity: None,
        slot: None,
        imported: None,
    };
    let BeginUpload::Ready { key, part_size } = s.begin_upload(&w, &spec, &big()).await.unwrap() else {
        panic!("fresh asset content must reserve a real upload");
    };
    upload_parts(&s, &bucket, &w, &key, part_size, &body(b"waft"), &big()).await.unwrap();
    let refused = s.complete_upload(&w, &key).await;
    let Err(RuntimeStoreError::NotItsContent(why)) = refused else { panic!("{refused:?}") };
    for named in ["assets/word.txt", sha.as_str(), sha256_hex(b"waft").as_str()] {
        assert!(why.contains(named), "the refusal names {named}: {why}");
    }
    let rows: i64 = pool.scalar("SELECT COUNT(*) FROM runtime_file", &[]).await.unwrap();
    assert_eq!(rows, 0, "the refused upload left no row");
    assert!(bucket.keys().is_empty(), "the refused upload left no object: {:?}", bucket.keys());
    assert_eq!(s.tenant_usage("t1").await.unwrap(), (0, 0), "nothing is charged");

    let BeginUpload::Ready { key: again, part_size } = s.begin_upload(&w, &spec, &big()).await.unwrap() else {
        panic!("the key is free again, so the right bytes start a fresh upload");
    };
    assert_eq!(again, key);
    upload_parts(&s, &bucket, &w, &key, part_size, &body(b"weft"), &big()).await.unwrap();
    let meta = s.complete_upload(&w, &key).await.unwrap();
    assert_eq!((meta.key.as_str(), meta.size_bytes), (key.as_str(), 4));
    assert_eq!(bucket.get(&object_key(&key)).await.unwrap().as_deref(), Some(&b"weft"[..]));
}

#[weft_db_macros::test]
async fn delete_removes_and_a_link_requires_the_file(pool: Pool) {
    let (s, bucket, _c) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let f = put_via(&s, &bucket, &w, &StorageScope::Project, "b", "f", None, &big(), body(b"x")).await.unwrap();
    let parsed = weft_core::storage::key::parse_key(&f.key).unwrap();
    // A link is made for the existing file.
    assert!(s.link(&parsed, PresignAudience::External, Some(60), true).await.unwrap().1.contains(&f.key));
    s.delete(&parsed).await.unwrap();
    assert!(matches!(s.delete(&parsed).await, Err(RuntimeStoreError::NotFound(_))));
    assert!(matches!(s.link(&parsed, PresignAudience::External, None, true).await, Err(RuntimeStoreError::NotFound(_))));
}

/// The dispatcher acting for `tenant` on its assets: the admin upload
/// surface's caller.
fn acting(tenant: &str) -> CallerAuth {
    CallerAuth::Tenant { tenant: tenant.into() }
}

/// The sha256 of `bytes`, in hex: an asset's id.
fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::Digest;
    weft_core::access::hex_of(&sha2::Sha256::digest(bytes))
}

/// The three bytes of the test asset `id` (a single digit).
fn asset_bytes(id: u64) -> Vec<u8> {
    format!("ca{id}").into_bytes()
}

/// The content hash of the test asset `id`.
fn asset_hash(id: u64) -> String {
    sha256_hex(&asset_bytes(id))
}

/// The key of the test asset `id`.
fn asset_key(tenant: &str, id: u64) -> String {
    format!("{tenant}/asset/{}", asset_hash(id))
}

async fn asset_via(s: &RuntimeStore, bucket: &FakeObjectStore, tenant: &str, id: u64) -> StoredFileMeta {
    let caller = acting(tenant);
    let hash = asset_hash(id);
    let spec = UploadSpec {
        scope: &StorageScope::Asset,
        mime: "image/png",
        filename: "cat.png",
        keep: None,
        declared_size: Some(3),
        content_hash: Some(&hash),
        identity: None,
        slot: None,
        imported: None,
    };
    let BeginUpload::Ready { key, part_size } = s.begin_upload(&caller, &spec, &big()).await.unwrap() else {
        panic!("test asset must be new");
    };
    upload_parts(s, bucket, &caller, &key, part_size, &body(&asset_bytes(id)), &big()).await.unwrap();
    s.complete_upload(&caller, &key).await.unwrap()
}

#[weft_db_macros::test]
async fn asset_lifetime_keeps_current_and_expires_removed_files_after_last_access(pool: Pool) {
    let (s, bucket, clock) = store(&pool).await;
    let old = asset_via(&s, &bucket, "t1", 1).await;
    let current = asset_via(&s, &bucket, "t1", 2).await;
    let old_key = weft_core::storage::key::parse_key(&old.key).unwrap();
    s.set_asset_references("t1", "p1", &[old.key.clone(), current.key.clone()], &[]).await.unwrap();
    clock.advance(Duration::from_secs(400 * 86400));
    assert_eq!(expire(&s, &pool).await, 0, "current source may be idle for months");

    s.set_asset_references("t1", "p1", std::slice::from_ref(&current.key), &[]).await.unwrap();
    let deadline = s.meta(&old_key).await.unwrap().expires_at_unix.unwrap();
    assert_eq!(deadline, clock.now_unix() + DEFAULT_KEEP_TTL_SECS as i64);
    clock.advance(Duration::from_secs(20 * 86400));
    s.set_asset_references("t1", "p1", std::slice::from_ref(&current.key), &[]).await.unwrap();
    assert_eq!(s.meta(&old_key).await.unwrap().expires_at_unix, Some(deadline), "sync is not file access");

    get_via(&s, &bucket, &old_key, None).await.unwrap();
    assert_eq!(s.meta(&old_key).await.unwrap().expires_at_unix, Some(clock.now_unix() + DEFAULT_KEEP_TTL_SECS as i64));
    clock.advance(Duration::from_secs(DEFAULT_KEEP_TTL_SECS - 1));
    assert_eq!(expire(&s, &pool).await, 0);
    clock.advance(Duration::from_secs(2));
    assert_eq!(expire(&s, &pool).await, 1);
    assert!(matches!(s.link(&old_key, WORKER, None, false).await, Err(RuntimeStoreError::NotFound(_))));
    assert!(bucket.get(&object_key(&old.key)).await.unwrap().is_none());
    assert!(bucket.get(&object_key(&current.key)).await.unwrap().is_some());
}

#[weft_db_macros::test]
async fn an_asset_the_sync_never_publishes_expires_on_its_own(pool: Pool) {
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
    assert_eq!(expire(&s, &pool).await, 1);
    assert!(bucket.get(&object_key(&orphan.key)).await.unwrap().is_none());
    assert!(bucket.get(&object_key(&published.key)).await.unwrap().is_some());
}

#[weft_db_macros::test]
async fn asset_lifetime_removing_last_reference_and_restoring_it_are_both_supported(pool: Pool) {
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
    s.link(&parsed, PresignAudience::External, None, true).await.unwrap();
    clock.advance(Duration::from_secs(DEFAULT_KEEP_TTL_SECS + 1));
    assert_eq!(expire(&s, &pool).await, 0);
}

/// One content is one file per tenant: a second project storing it uploads
/// nothing, the tenant pays for it once, and another tenant's copy of the
/// same bytes is that tenant's own file.
#[weft_db_macros::test]
async fn an_asset_is_stored_and_charged_once_per_tenant(pool: Pool) {
    let (s, bucket, _) = store(&pool).await;
    let first = asset_via(&s, &bucket, "t1", 1).await;
    assert_eq!(first.key, asset_key("t1", 1));
    let hash = asset_hash(1);
    let spec = UploadSpec {
        scope: &StorageScope::Asset,
        mime: "image/png",
        filename: "cat.png",
        keep: None,
        declared_size: Some(3),
        content_hash: Some(&hash),
        identity: None,
        slot: None,
        imported: None,
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
        s.held_assets("t1", &[hash.clone(), asset_hash(2)]).await.unwrap(),
        [(hash.clone(), first.key.clone())].into_iter().collect(),
        "only what the tenant stores whole, under its own key"
    );
    assert!(matches!(s.held_assets("t1", &["nothex".into()]).await, Err(RuntimeStoreError::Invalid(_))));
}

/// An asset lives while any project of its tenant references it: one
/// project dropping it leaves it pinned, the last one starts its countdown.
#[weft_db_macros::test]
async fn an_asset_lives_while_any_project_references_it(pool: Pool) {
    let (s, bucket, clock) = store(&pool).await;
    let shared = asset_via(&s, &bucket, "t1", 1).await;
    let parsed = weft_core::storage::key::parse_key(&shared.key).unwrap();
    s.set_asset_references("t1", "p1", std::slice::from_ref(&shared.key), &[]).await.unwrap();
    s.set_asset_references("t1", "p2", &[], std::slice::from_ref(&shared.key)).await.unwrap();
    s.set_asset_references("t1", "p1", &[], &[]).await.unwrap();
    assert_eq!(s.meta(&parsed).await.unwrap().expires_at_unix, None, "p2 still references it");
    clock.advance(Duration::from_secs(DEFAULT_KEEP_TTL_SECS + 1));
    assert_eq!(expire(&s, &pool).await, 0);

    s.set_asset_references("t1", "p2", &[], &[]).await.unwrap();
    assert_eq!(
        s.meta(&parsed).await.unwrap().expires_at_unix,
        Some(clock.now_unix() + DEFAULT_KEEP_TTL_SECS as i64),
        "the last reference gone, the countdown starts"
    );
    clock.advance(Duration::from_secs(DEFAULT_KEEP_TTL_SECS + 1));
    assert_eq!(expire(&s, &pool).await, 1);
    assert!(bucket.get(&object_key(&shared.key)).await.unwrap().is_none());
}

#[weft_db_macros::test]
async fn asset_references_are_walled_by_tenant_and_preserve_node_selected_ttls(pool: Pool) {
    let (s, bucket, _) = store(&pool).await;
    let own = asset_via(&s, &bucket, "t1", 1).await;
    let foreign = asset_via(&s, &bucket, "t2", 2).await;
    let w = worker("t1", "p1", Some("c1"));
    let generated = put_via(&s, &bucket, &w, &StorageScope::Run, "image/png", "generated.png",
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
#[weft_db_macros::test]
async fn a_tenant_wipe_drops_its_asset_references(pool: Pool) {
    let (s, bucket, _) = store(&pool).await;
    let own = asset_via(&s, &bucket, "t1", 1).await;
    let other = asset_via(&s, &bucket, "t2", 1).await;
    s.set_asset_references("t1", "p1", std::slice::from_ref(&own.key), &[]).await.unwrap();
    s.set_asset_references("t2", "p1", std::slice::from_ref(&other.key), &[]).await.unwrap();
    assert_eq!(s.wipe_prefix("t1/").await.unwrap(), 1);
    let left: Vec<String> = pool.scalars("SELECT key FROM asset_reference ORDER BY key", &[]).await.unwrap();
    assert_eq!(left, vec![other.key]);
}

/// A kept file (one an older version names) that is gone is reported,
/// not fatal: the build's own file is pinned and the missing key comes
/// back so the caller can say which version lost it.
#[weft_db_macros::test]
async fn asset_lifetime_missing_kept_file_is_reported_and_pins_the_rest(pool: Pool) {
    let (s, bucket, _) = store(&pool).await;
    let own = asset_via(&s, &bucket, "t1", 1).await;
    let gone = format!("t1/asset/{}", "f".repeat(64));
    let missing = s.set_asset_references("t1", "p1", std::slice::from_ref(&own.key), std::slice::from_ref(&gone)).await.unwrap();
    assert_eq!(missing, vec![gone]);
    let parsed = weft_core::storage::key::parse_key(&own.key).unwrap();
    assert_eq!(s.meta(&parsed).await.unwrap().expires_at_unix, None, "the build's own file is pinned");
}

#[weft_db_macros::test]
async fn asset_lifetime_missing_current_file_does_not_retire_other_files(pool: Pool) {
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

#[weft_db_macros::test]
async fn a_deadline_deletes_a_project_file_and_an_access_renews_it(pool: Pool) {
    // A deadline works in every scope a node writes: a project file stored
    // with one goes once nobody touched it for that long, and an access
    // renews it.
    let (s, bucket, clock) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let f = put_via(&s, &bucket, &w, &StorageScope::Project, "b", "f", Some(KeepTtl::Secs { secs: 100 }), &big(), body(b"x"))
        .await
        .unwrap();
    assert_eq!(f.keep_ttl_secs, Some(100));
    let parsed = weft_core::storage::key::parse_key(&f.key).unwrap();
    clock.advance(Duration::from_secs(80));
    assert!(get_via(&s, &bucket, &parsed, None).await.is_ok(), "an access renews it");
    clock.advance(Duration::from_secs(80));
    assert_eq!(expire(&s, &pool).await, 0, "renewed by the access");
    clock.advance(Duration::from_secs(30));
    assert_eq!(expire(&s, &pool).await, 1);
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
    assert_eq!(kept.expires_at_unix, Some(clock.now_unix() + 50));
    let cleared = s.keep(&parsed, KeepTtl::Never).await.unwrap();
    assert_eq!((cleared.keep_ttl_secs, cleared.expires_at_unix), (None, None));
}

#[weft_db_macros::test]
async fn an_asset_takes_no_node_lifetime(pool: Pool) {
    let (s, _bucket, _c) = store(&pool).await;
    let w = acting("t1");
    let err = begin_via(&s, &w, &StorageScope::Asset, "b", "f", Some(KeepTtl::Default), &big(), Some(1))
        .await
        .unwrap_err();
    assert!(matches!(err, RuntimeStoreError::Invalid(_)), "{err:?}");
}

#[weft_db_macros::test]
async fn replace_overwrites_the_file_in_place(pool: Pool) {
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

#[weft_db_macros::test]
async fn a_retried_replacement_complete_answers_with_the_replaced_file(pool: Pool) {
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
    let mut task = tokio::spawn({
        let (s, w, k) = (s.clone(), w.clone(), key.to_string());
        async move { s.complete_upload(&w, &k).await }
    });
    // A completion that ends before it assembles anything fails the test
    // here, instead of leaving it waiting for the hold forever.
    tokio::select! {
        entered = entered => entered.unwrap(),
        ended = &mut task => panic!("the completion ended before the bucket assembled the object: {ended:?}"),
    }
    (task, release)
}

async fn status_of(pool: &Pool, key: &str) -> Option<String> {
    pool.scalar_opt("SELECT status FROM runtime_file WHERE key = $1", &[&key]).await.unwrap()
}

#[weft_db_macros::test]
async fn concurrent_completions_move_the_version_once(pool: Pool) {
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

#[weft_db_macros::test]
async fn an_abort_during_a_completion_is_refused(pool: Pool) {
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

#[weft_db_macros::test]
async fn a_delete_during_a_replacement_completion_is_refused(pool: Pool) {
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

#[weft_db_macros::test]
async fn a_resumed_reserve_during_a_completion_is_refused_at_once(pool: Pool) {
    // The completion holds no lock across the bucket call, so the tenant's
    // other uploads and this one's reservations never wait on it.
    let (s, bucket, _c) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let key = upload_ready(&s, &bucket, &w, b"abc").await;
    let other = begin_via(&s, &w, &StorageScope::Project, "b", "g", None, &big(), None).await.unwrap().0;
    let (task, release) = completion_held(&s, &bucket, &w, &key).await;
    let quick = Duration::from_secs(5);
    let reserve = tokio::time::timeout(quick, s.reserve_parts(&w, &key, &[ask(1, 3)], &big(), &WORKER)).await.unwrap();
    assert!(matches!(reserve, Err(RuntimeStoreError::Completing(_))));
    let report = tokio::time::timeout(quick, s.record_part(&w, &key, 1, "\"x\"")).await.unwrap();
    assert!(matches!(report, Err(RuntimeStoreError::Completing(_))));
    // The upload itself cannot be deleted either: it is no file yet, and
    // the answer says why rather than "not found".
    let parsed = weft_core::storage::key::parse_key(&key).unwrap();
    assert!(matches!(s.delete(&parsed).await, Err(RuntimeStoreError::Completing(_))));
    tokio::time::timeout(quick, s.reserve_parts(&w, &other, &[ask(1, 3)], &big(), &WORKER)).await.unwrap().unwrap();
    release.send(()).unwrap();
    assert_eq!(task.await.unwrap().unwrap().size_bytes, 3);
}

#[weft_db_macros::test]
async fn a_dropped_completion_the_bucket_finished_is_recovered(pool: Pool) {
    let (s, bucket, clock) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let (f, key) = replacement_ready(&s, &bucket, &w, b"old", b"new").await;
    let (task, _release) = completion_held(&s, &bucket, &w, &key).await;
    task.abort();
    let _ = task.await;
    assert_eq!(status_of(&pool, &key).await.as_deref(), Some("completing"));
    s.expire().await.unwrap();
    assert_eq!(status_of(&pool, &key).await.as_deref(), Some("completing"), "a live claim is left alone");
    clock.advance(Duration::from_secs(CLAIM_LEASE_SECS as u64 + 1));
    s.expire().await.unwrap();
    assert_eq!(status_of(&pool, &key).await, None, "folded into the file");
    let parsed = weft_core::storage::key::parse_key(&f.key).unwrap();
    let meta = s.meta(&parsed).await.unwrap();
    assert_eq!((meta.version, meta.size_bytes), (2, 3));
    assert_eq!(get_via(&s, &bucket, &parsed, None).await.unwrap().1, body(b"new"));
    // A retried complete from the caller that was cut off answers with it.
    assert_eq!(s.complete_upload(&w, &key).await.unwrap().version, 2);
}

#[weft_db_macros::test]
async fn a_dropped_completion_before_the_bucket_is_recovered(pool: Pool) {
    // Claimed, then the process died before asking the bucket: the sweep
    // finds the multipart still open and completes it.
    let (s, bucket, clock) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let key = upload_ready(&s, &bucket, &w, b"abc").await;
    pool.execute("UPDATE runtime_file SET status = 'completing', progressed_at_unix = $2 WHERE key = $1", &[&key, &clock.now_unix()]).await.unwrap();
    clock.advance(Duration::from_secs(CLAIM_LEASE_SECS as u64 + 1));
    s.expire().await.unwrap();
    assert_eq!(status_of(&pool, &key).await.as_deref(), Some("active"));
    let parsed = weft_core::storage::key::parse_key(&key).unwrap();
    assert_eq!(get_via(&s, &bucket, &parsed, None).await.unwrap().1, body(b"abc"));
}

#[weft_db_macros::test]
async fn a_completion_the_bucket_cannot_account_for_is_ended(pool: Pool) {
    // The multipart is gone but no object of the right size is there (it
    // was aborted outside weft): the verdict is final, the upload is
    // reaped and its reservation freed. A replaced file whose object was
    // never overwritten keeps its row and bytes.
    let (s, bucket, clock) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let (f, replacement) = replacement_ready(&s, &bucket, &w, b"old", b"newer").await;
    let fresh = upload_ready(&s, &bucket, &w, b"abc").await;
    for key in [&replacement, &fresh] {
        let upload_id: String = pool.scalar("SELECT upload_id FROM runtime_file WHERE key = $1", &[key]).await.unwrap();
        pool.execute("UPDATE runtime_file SET status = 'completing', progressed_at_unix = $2 WHERE key = $1", &[key, &clock.now_unix()]).await.unwrap();
        let object = if key == &fresh { object_key(key) } else { object_key(&f.key) };
        bucket.abort_multipart(&object, &upload_id).await.unwrap();
        assert!(matches!(s.complete_upload(&w, key).await, Err(RuntimeStoreError::Lost(_))));
        assert_eq!(status_of(&pool, key).await, None, "reaped");
    }
    let parsed = weft_core::storage::key::parse_key(&f.key).unwrap();
    assert_eq!(get_via(&s, &bucket, &parsed, None).await.unwrap().1, body(b"old"));
    assert_eq!(charged_bytes_for(&pool, "t1", clock.now_unix()).await.unwrap(), 3, "only the old file is charged");
}

#[weft_db_macros::test]
async fn a_refused_completion_stays_completing_until_the_sweep_ends_it(pool: Pool) {
    // Never back to 'pending': another drive may be landing it. The live
    // caller hears "completing"; the expiry loop, its look booked at the
    // claim's lapse, gives the verdict with no call of the tenant's.
    let (s, bucket, clock) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let (key, _) = begin_via(&s, &w, &StorageScope::Project, "b", "f", None, &big(), Some(3)).await.unwrap();
    let part = s.reserve_parts(&w, &key, &[ask(1, 3)], &big(), &WORKER).await.unwrap().remove(0);
    bucket.put_part(&part.url, body(b"abc")).unwrap();
    s.record_part(&w, &key, 1, "\"not-the-etag\"").await.unwrap();
    assert!(matches!(s.complete_upload(&w, &key).await, Err(RuntimeStoreError::Completing(_))));
    assert_eq!(status_of(&pool, &key).await.as_deref(), Some("completing"));
    assert!(matches!(s.abort_upload(&w, &key).await, Err(RuntimeStoreError::Completing(_))));
    assert_eq!(
        s.expire().await.unwrap(),
        weft_task_store::drain::DrainStep::RetryIn(Duration::from_secs(CLAIM_LEASE_SECS as u64 + 1)),
        "the look is booked at the claim's lapse"
    );
    assert_eq!(status_of(&pool, &key).await.as_deref(), Some("completing"), "left to its call until then");
    clock.advance(Duration::from_secs(CLAIM_LEASE_SECS as u64 + 1));
    s.expire().await.unwrap();
    assert_eq!(status_of(&pool, &key).await, None, "ended and reaped");
    assert!(bucket.in_progress_uploads().is_empty());
    assert_eq!(charged_bytes_for(&pool, "t1", clock.now_unix()).await.unwrap(), 0);
}

#[weft_db_macros::test]
async fn a_size_mismatched_completion_removes_rows_before_bytes(pool: Pool) {
    // A part whose bytes differ from its reservation (only a bucket anomaly
    // could do it; here a direct part upload skips the signed length).
    let (s, bucket, clock) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let (key, _) = begin_via(&s, &w, &StorageScope::Project, "b", "f", None, &big(), Some(3)).await.unwrap();
    s.reserve_parts(&w, &key, &[ask(1, 3)], &big(), &WORKER).await.unwrap();
    let upload_id = bucket.in_progress_uploads().pop().unwrap();
    let etag = bucket.upload_part(&object_key(&key), &upload_id, 1, body(b"abcd")).await.unwrap();
    s.record_part(&w, &key, 1, &etag).await.unwrap();
    assert!(matches!(s.complete_upload(&w, &key).await, Err(RuntimeStoreError::Lost(_))));
    assert_eq!(status_of(&pool, &key).await, None);
    assert!(bucket.get(&object_key(&key)).await.unwrap().is_none());
    assert_eq!(charged_bytes_for(&pool, "t1", clock.now_unix()).await.unwrap(), 0);
}

#[weft_db_macros::test]
async fn an_abandoned_replacement_leaves_the_file_untouched(pool: Pool) {
    // Aborted, or reaped by a wipe of its run's files: the upload goes,
    // the file it was replacing keeps its object and its bytes until the
    // wipe takes it too.
    let (s, bucket, _c) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let f = put_via(&s, &bucket, &w, &StorageScope::Run, "b", "f", None, &big(), body(b"old"))
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
    delete_run_files(&s, "t1", "c1").await;
    assert!(bucket.in_progress_uploads().is_empty(), "the replacement's multipart is aborted");
    assert!(matches!(s.complete_upload(&w, &swept).await, Err(RuntimeStoreError::NoUpload(_))));
}

#[weft_db_macros::test]
async fn an_edit_from_an_old_version_is_refused_and_one_write_runs_at_a_time(pool: Pool) {
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

#[weft_db_macros::test]
async fn replace_refuses_what_it_cannot_overwrite(pool: Pool) {
    let (s, bucket, _c) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    // A file that is not there.
    let missing = "t1/project/p1/00000000-0000-0000-0000-000000000000";
    assert!(matches!(s.begin_replace(&w, missing, Some(1), None, &big()).await, Err(RuntimeStoreError::NotFound(_))));
    // Another tenant's file: the same wall as a read.
    let f = put_via(&s, &bucket, &w, &StorageScope::Run, "b", "f", None, &big(), body(b"x"))
        .await
        .unwrap();
    let other = worker("t2", "p1", Some("c2"));
    assert!(matches!(s.begin_replace(&other, &f.key, Some(1), None, &big()).await, Err(RuntimeStoreError::Denied(_))));
    // A file deleted while its replacement was on the way: the new object
    // is removed again and the caller hears the file is gone.
    let (key, part_size) = s.begin_replace(&w, &f.key, Some(1), None, &big()).await.unwrap();
    s.delete(&weft_core::storage::key::parse_key(&f.key).unwrap()).await.unwrap();
    upload_parts(&s, &bucket, &w, &key, part_size, &body(b"y"), &big()).await.unwrap();
    assert!(matches!(s.complete_upload(&w, &key).await, Err(RuntimeStoreError::NoUpload(_))));
    assert!(bucket.get(&object_key(&f.key)).await.unwrap().is_none(), "no object without a row");
    assert_eq!(s.tenant_usage("t1").await.unwrap(), (0, 0), "nothing is left charged");
}

#[weft_db_macros::test]
async fn an_in_flight_upload_is_invisible_until_completed(pool: Pool) {
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

#[weft_db_macros::test]
async fn upload_verbs_reject_a_key_begin_never_minted(pool: Pool) {
    // A worker cannot touch an upload the broker never opened: reserving,
    // recording, resuming, and completing an unknown key all fail loud.
    let (s, _bucket, _c) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let key = "t1/run/c1/forged";
    let err = s.reserve_parts(&w, key, &[ask(1, 1)], &big(), &WORKER).await.unwrap_err();
    assert!(matches!(err, RuntimeStoreError::Invalid(_)), "{err:?}");
    let err = s.record_part(&w, key, 1, "\"etag\"").await.unwrap_err();
    assert!(matches!(err, RuntimeStoreError::Invalid(_)), "{err:?}");
    let err = s.resume_upload(&w, key, &big(), &WORKER).await.unwrap_err();
    assert!(matches!(err, RuntimeStoreError::NoUpload(_)), "{err:?}");
    let err = s.complete_upload(&w, key).await.unwrap_err();
    assert!(matches!(err, RuntimeStoreError::NoUpload(_)), "{err:?}");
}

#[weft_db_macros::test]
async fn an_abandoned_upload_is_ended_by_the_expiry_loop_after_grace(pool: Pool) {
    use weft_broker::runtime_store::PENDING_RESERVE_GRACE_SECS;
    let lapse = PART_LINK_SECS + PENDING_RESERVE_GRACE_SECS as u64;
    // A crashed upload: begin + land a part, never complete. The reservation
    // holds quota and an in-flight multipart. The expiry loop, its look
    // booked at the lapse (the last part link's life, then a part's time to
    // land), reaps it once it has made no progress for that long, with no
    // call of the tenant's: multipart aborted, row gone, charge freed.
    let (s, bucket, clock) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    // A project-scoped upload (no run sweep, no expiry) is the case only this
    // reap covers.
    let (key, _ps) = begin_via(&s, &w, &StorageScope::Project, "b", "f", None, &big(), Some(6))
        .await
        .unwrap();
    let part = &s.reserve_parts(&w, &key, &[ask(1, 6)], &big(), &WORKER).await.unwrap()[0];
    let etag = bucket.put_part(&part.url, body(b"orphan")).unwrap();
    s.record_part(&w, &key, part.part_number, &etag).await.unwrap();
    assert_eq!(s.tenant_usage("t1").await.unwrap().1, 6, "in-flight charge visible");
    assert_eq!(
        s.expire().await.unwrap(),
        weft_task_store::drain::DrainStep::RetryIn(Duration::from_secs(lapse + 1)),
        "the look is booked at the lapse"
    );
    // Within the lapse: not reaped (a PUT begun on the last link may still be running).
    clock.advance(Duration::from_secs(lapse - 1));
    s.expire().await.unwrap();
    assert_eq!(s.tenant_usage("t1").await.unwrap(), (1, 6), "not reaped within the lapse");
    // Past the lapse: reaped, multipart aborted, charge freed.
    clock.advance(Duration::from_secs(2));
    assert_eq!(expire(&s, &pool).await, 1);
    assert_eq!(s.tenant_usage("t1").await.unwrap(), (0, 0), "reaped past grace: no leftover row/charge");
    assert!(bucket.in_progress_uploads().is_empty(), "multipart aborted");
}

#[weft_db_macros::test]
async fn progress_defers_the_abandoned_reap(pool: Pool) {
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
        s.reserve_parts(&w, &key, &[ask(n, part_size)], &big(), &WORKER).await.expect("still alive");
        s.expire().await.unwrap();
        assert_eq!(s.tenant_usage("t1").await.unwrap().0, 1, "progressing upload not reaped");
    }
}

#[weft_db_macros::test]
async fn deleting_a_runs_files_reaps_its_abandoned_upload(pool: Pool) {
    // A run-scoped upload crashed mid-flight: deleting the run's files
    // aborts the multipart and frees everything.
    let (s, bucket, _c) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let (key, _ps) = begin_via(&s, &w, &StorageScope::Run, "b", "f", None, &big(), Some(6))
        .await
        .unwrap();
    let part = &s.reserve_parts(&w, &key, &[ask(1, 6)], &big(), &WORKER).await.unwrap()[0];
    bucket.put_part(&part.url, body(b"orphan")).unwrap();
    assert_eq!(delete_run_files(&s, "t1", "c1").await, 1, "the abandoned run upload is reaped");
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

#[weft_db_macros::test]
async fn complete_retry_after_success_is_idempotent(pool: Pool) {
    // A lost-response retry of complete on an already-finalized key returns
    // the existing metadata unchanged and never touches the object.
    let (s, bucket, _clock) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let first = put_via(&s, &bucket, &w, &StorageScope::Run, "text/plain", "a.txt", None, &big(), body(b"finalized bytes"))
        .await
        .unwrap();
    let retry = s.complete_upload(&w, &first.key).await.unwrap();
    assert_eq!(first.size_bytes, retry.size_bytes);
    assert_eq!(first.filename, retry.filename);
    assert!(bucket.get(&object_key(&first.key)).await.unwrap().is_some(), "object untouched");
}

#[weft_db_macros::test]
async fn a_completed_file_is_immutable(pool: Pool) {
    // No upload verb can touch a finalized file: its upload state is gone.
    let (s, bucket, _clock) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let meta = put_via(&s, &bucket, &w, &StorageScope::Run, "text/plain", "a.txt", None, &big(), body(b"original"))
        .await
        .unwrap();
    let err = s.reserve_parts(&w, &meta.key, &[ask(1, 3)], &big(), &WORKER).await.unwrap_err();
    assert!(matches!(err, RuntimeStoreError::Invalid(_)), "{err:?}");
    let err = s.resume_upload(&w, &meta.key, &big(), &WORKER).await.unwrap_err();
    assert!(matches!(err, RuntimeStoreError::Invalid(_)), "{err:?}");
    let err = s.record_part(&w, &meta.key, 1, "\"e\"").await.unwrap_err();
    assert!(matches!(err, RuntimeStoreError::Invalid(_)), "{err:?}");
}

#[weft_db_macros::test]
async fn wipe_prefix_does_not_touch_a_sibling_run_id_prefix(pool: Pool) {
    // The trailing-slash invariant: deleting run `c1`'s files must never
    // match a sibling run whose name merely starts with `c1`.
    let (s, bucket, _clock) = store(&pool).await;
    let scope = StorageScope::Run;
    let w_short = worker("t1", "p1", Some("c1"));
    let w_long = worker("t1", "p1", Some("c1x"));
    put_via(&s, &bucket, &w_short, &scope, "text/plain", "a.txt", None, &big(), body(b"short"))
        .await
        .unwrap();
    let kept = put_via(&s, &bucket, &w_long, &scope, "text/plain", "b.txt", None, &big(), body(b"long"))
        .await
        .unwrap();
    assert_eq!(delete_run_files(&s, "t1", "c1").await, 1, "only c1's file goes");
    let sibling = weft_core::storage::key::parse_key(&kept.key).unwrap();
    assert!(s.meta(&sibling).await.is_ok(), "sibling run c1x untouched");
}

/// One deletion takes a page of runs: each named run's files go, a run
/// that stored nothing costs nothing, and a run not named keeps its own.
#[weft_db_macros::test]
async fn one_deletion_takes_the_files_of_every_run_it_names(pool: Pool) {
    let (s, bucket, _clock) = store(&pool).await;
    let scope = StorageScope::Run;
    let mut keys = Vec::new();
    for run in ["c1", "c2", "c3"] {
        let put = put_via(&s, &bucket, &worker("t1", "p1", Some(run)), &scope, "text/plain", "f.txt", None, &big(), body(b"x"))
            .await
            .unwrap();
        keys.push(weft_core::storage::key::parse_key(&put.key).unwrap());
    }
    let named = ["c1", "c2", "nothing-stored"].map(String::from);
    assert_eq!(s.delete_run_files("t1", &named).await.unwrap(), 2);
    assert!(s.meta(&keys[0]).await.is_err() && s.meta(&keys[1]).await.is_err(), "c1's and c2's files went");
    assert!(s.meta(&keys[2]).await.is_ok(), "c3 was not named");
}

/// Three multi-part uploads of one run, their parts reserved, sent and
/// recorded in turn, one part of each at a time: each lands as its own
/// bytes, the tenant is charged each once, and no upload is left in
/// flight.
#[weft_db_macros::test]
async fn interleaved_uploads_stay_their_own_and_are_charged_once(pool: Pool) {
    let (s, bucket, _clock) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let patterned = |len: usize, seed: u8| bytes::Bytes::from((0..len).map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed)).collect::<Vec<u8>>());
    let ps = DEFAULT_PART_SIZE_BYTES as usize;
    let payloads = [patterned(2 * ps + 100, 1), patterned(ps + ps / 2 + 55, 2), patterned(3 * ps + 9, 3)];
    let mut uploads = Vec::new();
    for (n, payload) in payloads.iter().enumerate() {
        let (key, part_size) = begin_via(&s, &w, &StorageScope::Run, "b", &format!("f{n}"), None, &big(), Some(payload.len() as u64))
            .await
            .unwrap();
        uploads.push((key, part_size as usize, payload.clone()));
    }
    let most_parts = uploads.iter().map(|(_, size, payload)| payload.len().div_ceil(*size)).max().unwrap();
    for part in 0..most_parts {
        for (key, size, payload) in &uploads {
            let start = part * size;
            if start >= payload.len() { continue; }
            let slice = payload.slice(start..(start + size).min(payload.len()));
            let number = part as i32 + 1;
            let reserved = s.reserve_parts(&w, key, &[ask(number, slice.len() as u64)], &big(), &WORKER).await.unwrap();
            let etag = bucket.put_part(&reserved[0].url, slice).expect("fake part PUT");
            s.record_part(&w, key, number, &etag).await.unwrap();
        }
    }
    for (key, _, _) in &uploads {
        s.complete_upload(&w, key).await.unwrap();
    }
    for (key, _, payload) in &uploads {
        assert_eq!(bucket.get(&object_key(key)).await.unwrap().as_deref(), Some(payload.as_ref()), "{key} is its own bytes");
    }
    let total: u64 = payloads.iter().map(|p| p.len() as u64).sum();
    assert_eq!(s.tenant_usage("t1").await.unwrap(), (3, total), "each upload charged once");
    let pending: i64 = pool.scalar("SELECT COUNT(*) FROM runtime_file WHERE status <> 'active'", &[]).await.unwrap();
    assert_eq!(pending, 0, "no upload left in flight");
    assert!(bucket.in_progress_uploads().is_empty());
}

#[weft_db_macros::test]
async fn concurrent_begins_cannot_blow_past_the_byte_cap(pool: Pool) {
    // The byte-quota check and the reservation are atomic under the tenant
    // lock AT BEGIN: with a cap admitting only one of two declared uploads,
    // exactly one begin wins, before any byte could move.
    let (s, _bucket, _clock) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let scope = StorageScope::Run;
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

#[weft_db_macros::test]
async fn one_failing_row_does_not_block_the_others(pool: Pool) {
    // A reap can keep failing (the bucket refuses one object). The fenced
    // three-step reap (row -> 'reaping', reap bucket, delete row) leaves a
    // residue nothing orphans: the object and a row still pointing at it,
    // marked with the error, while every other expired file goes in the same
    // pass. The marked row books no look (nothing retries it on a timer),
    // and its failure is written down for `weft status` naming it, on a
    // record of its own beside the loop's. A wipe finishes it and the
    // failure is cleared.
    let (s, bucket, clock) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let put = |name: &'static str| {
        let (s, bucket, w) = (s.clone(), bucket.clone(), w.clone());
        async move {
            put_via(&s, &bucket, &w, &StorageScope::Run, "b", name, Some(KeepTtl::Secs { secs: 10 }), &big(), body(b"xyz"))
                .await
                .unwrap()
        }
    };
    let stuck = put("stuck").await;
    let fine = put("fine").await;
    let later = put_via(&s, &bucket, &w, &StorageScope::Run, "b", "later", Some(KeepTtl::Secs { secs: 1000 }), &big(), body(b"z"))
        .await
        .unwrap();
    let parsed = weft_core::storage::key::parse_key(&stuck.key).unwrap();
    clock.advance(Duration::from_secs(11));
    bucket.fail_next_deletes(&format!("runtime/{}", stuck.key), 3);
    let step = s.expire().await.unwrap();
    assert_eq!(
        step,
        weft_task_store::drain::DrainStep::RetryIn(Duration::from_secs((later.expires_at_unix.unwrap() + 1 - clock.now_unix()) as u64)),
        "the next look is the next expiry; the stuck row books nothing"
    );
    assert_eq!(status_of(&pool, &fine.key).await, None, "the other expired file went");
    assert_eq!(status_of(&pool, &stuck.key).await.as_deref(), Some("reaping"));
    let error: Option<String> = pool.scalar("SELECT reap_error FROM runtime_file WHERE key = $1", &[&stuck.key]).await.unwrap();
    assert!(error.unwrap().contains("injected delete failure"), "marked with why");
    // Nothing is orphaned, and the row is locked out of reads and keeps.
    assert!(bucket.keys().contains(&format!("runtime/{}", stuck.key)));
    assert!(matches!(get_via(&s, &bucket, &parsed, None).await, Err(RuntimeStoreError::NotFound(_))));
    assert!(matches!(s.keep(&parsed, KeepTtl::Default).await, Err(RuntimeStoreError::NotFound(_))));
    let failing = |callee: &'static str| {
        let pool = pool.clone();
        async move {
            let error: Option<String> = pool.scalar_opt("SELECT error FROM unanswered_call WHERE callee = $1", &[&callee]).await.unwrap();
            error
        }
    };
    let shown = failing("reaps:").await.expect("the failed reap is written down");
    assert!(shown.contains(&stuck.key) && shown.contains("injected delete failure"), "{shown}");
    // The loop's own record is its passes': a pass that goes through
    // clears it, and leaves the reap's failure shown.
    weft_task_store::unanswered::answered(
        &pool,
        weft_task_store::unanswered::Callee::Loop { role: "broker", name: weft_broker::runtime_store::FILE_EXPIRY_LOOP },
    )
    .await
    .unwrap();
    assert!(failing("reaps:").await.is_some(), "a good pass of the loop does not hide it");
    // The pass never retries it on its own, however long it waits; a wipe
    // takes it at once and clears the failure.
    clock.advance(Duration::from_secs(CLAIM_LEASE_SECS as u64 + 1));
    assert_eq!(expire(&s, &pool).await, 0);
    assert_eq!(s.wipe_prefix(&format!("t1/run/c1/{}", parsed.id)).await.unwrap(), 1);
    assert!(!bucket.keys().contains(&format!("runtime/{}", stuck.key)), "object reclaimed on retry");
    assert_eq!(failing("reaps:").await, None, "nothing failing is shown any more");
    assert_eq!(failing("loop:broker:runtime_file_expiry").await, None, "the loop's own record was never written");
}

/// A reap a call holds the claim of books the expiry loop's look at the
/// claim's lapse, so a reap whose process died is finished then; a pass
/// before the lapse leaves it to its call.
#[weft_db_macros::test]
async fn a_live_reap_claim_books_a_look_at_its_lapse(pool: Pool) {
    let (s, bucket, clock) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let f = put_via(&s, &bucket, &w, &StorageScope::Project, "b", "f", None, &big(), body(b"xyz")).await.unwrap();
    // A reap fenced the row, then its process died.
    pool.execute("UPDATE runtime_file SET status = 'reaping', progressed_at_unix = $2 WHERE key = $1", &[&f.key, &clock.now_unix()]).await.unwrap();
    assert_eq!(
        s.expire().await.unwrap(),
        weft_task_store::drain::DrainStep::RetryIn(Duration::from_secs(CLAIM_LEASE_SECS as u64 + 1)),
        "the look is booked at the claim's lapse"
    );
    assert_eq!(status_of(&pool, &f.key).await.as_deref(), Some("reaping"), "left to its call until then");
    clock.advance(Duration::from_secs(CLAIM_LEASE_SECS as u64 + 1));
    assert_eq!(expire(&s, &pool).await, 1, "the lapsed claim is taken over");
    assert!(!bucket.keys().contains(&format!("runtime/{}", f.key)));
}

/// A row whose reap failed and that a call took over again keeps its mark
/// while that call reaps it, and still books the look at the claim's
/// lapse, so a call that dies mid-reap does not leave it waiting on an
/// unrelated wake.
#[weft_db_macros::test]
async fn a_failed_reap_taken_over_books_a_look_at_its_lapse(pool: Pool) {
    let (s, bucket, clock) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let f = put_via(&s, &bucket, &w, &StorageScope::Project, "b", "f", None, &big(), body(b"xyz")).await.unwrap();
    // A reap failed and marked the row, then a call fenced it again and died.
    pool.execute("UPDATE runtime_file SET status = 'reaping', reap_error = 'the bucket refused', progressed_at_unix = $2 WHERE key = $1", &[&f.key, &clock.now_unix()]).await.unwrap();
    assert_eq!(
        s.expire().await.unwrap(),
        weft_task_store::drain::DrainStep::RetryIn(Duration::from_secs(CLAIM_LEASE_SECS as u64 + 1)),
        "the look is booked at the claim's lapse"
    );
}

/// A wipe takes over a row whose reap failed at once, and fails naming a
/// row another call is reaping right now instead of saying everything
/// went.
#[weft_db_macros::test]
async fn a_wipe_takes_a_failed_reap_and_fails_on_a_live_one(pool: Pool) {
    let (s, bucket, clock) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let failed = put_via(&s, &bucket, &w, &StorageScope::Project, "b", "failed", None, &big(), body(b"xyz")).await.unwrap();
    let live = put_via(&s, &bucket, &w, &StorageScope::Project, "b", "live", None, &big(), body(b"xyz")).await.unwrap();
    bucket.fail_next_deletes(&format!("runtime/{}", failed.key), 1);
    assert!(s.delete(&weft_core::storage::key::parse_key(&failed.key).unwrap()).await.is_err(), "the bucket refused");
    pool.execute("UPDATE runtime_file SET status = 'reaping', progressed_at_unix = $2 WHERE key = $1", &[&live.key, &clock.now_unix()]).await.unwrap();
    let wiped = s.wipe_prefix("t1/").await.expect_err("a row another call is reaping is left");
    assert!(matches!(wiped, RuntimeStoreError::Completing(_)), "only a live claim is left: ask again: {wiped:?}");
    assert!(format!("{wiped:#}").contains("wiped 1 files") && format!("{wiped:#}").contains("1 another removal in progress holds"), "{wiped:#}");
    assert_eq!(status_of(&pool, &failed.key).await, None, "the failed reap was taken over");
    assert!(!bucket.keys().contains(&format!("runtime/{}", failed.key)));
    assert_eq!(status_of(&pool, &live.key).await.as_deref(), Some("reaping"), "the live one is its call's");
}

/// A replacement landing on a file the expiry loop left out of its look
/// (its replacement was completing) rings the loop, so the file's renewed
/// expiry gets a look booked.
#[weft_db_macros::test]
async fn a_replacement_landing_rings_the_expiry_loop(pool: Pool) {
    let (s, bucket, clock) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let f = put_via(&s, &bucket, &w, &StorageScope::Project, "b", "f", Some(KeepTtl::Secs { secs: 10 }), &big(), body(b"old"))
        .await
        .unwrap();
    let (key, part_size) = s.begin_replace(&w, &f.key, Some(3), Some(1), &big()).await.unwrap();
    upload_parts(&s, &bucket, &w, &key, part_size, &body(b"new"), &big()).await.unwrap();
    let (task, release) = completion_held(&s, &bucket, &w, &key).await;
    clock.advance(Duration::from_secs(11));
    assert_eq!(
        s.expire().await.unwrap(),
        weft_task_store::drain::DrainStep::RetryIn(Duration::from_secs(CLAIM_LEASE_SECS as u64 + 1 - 11)),
        "the file is left out while it is replaced: the look is at the completion's lapse"
    );
    let mut heard = Session::connect(pool.config()).await.unwrap();
    heard.listen([weft_broker::runtime_store::FILE_EXPIRY_CHANNEL]).await.unwrap();
    release.send(()).unwrap();
    let replaced = task.await.unwrap().unwrap();
    assert_eq!(replaced.expires_at_unix, Some(clock.now_unix() + 10));
    tokio::time::timeout(Duration::from_secs(10), heard.notified()).await.expect("rung").unwrap();
    assert_eq!(
        s.expire().await.unwrap(),
        weft_task_store::drain::DrainStep::RetryIn(Duration::from_secs(11)),
        "the next look is at the renewed expiry"
    );
}

#[weft_db_macros::test]
async fn naming_a_part_twice_reserves_it_once(pool: Pool) {
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

    let first = s.reserve_parts(&w, &key, &[ask(1, 9)], &big(), &WORKER).await.unwrap();
    let second = s.reserve_parts(&w, &key, &[ask(1, 9)], &big(), &WORKER).await.unwrap();
    assert_eq!(first[0].part_number, 1);
    assert_eq!(second[0].part_number, 1, "the same part, not the next one");
    assert_eq!(second[0].offset_bytes, 0);
    // One row, so the reserved total still slices the declared size and the
    // upload can be completed.
    let rows: i64 = pool.scalar("SELECT COUNT(*) FROM runtime_file_part WHERE key = $1", &[&key]).await.unwrap();
    assert_eq!(rows, 1, "asking twice reserved once");

    // Either URL lands the part, and the upload completes.
    let etag = bucket.put_part(&second[0].url, body(b"nine byte")).unwrap();
    s.record_part(&w, &key, 1, &etag).await.unwrap();
    let meta = s.complete_upload(&w, &key).await.unwrap();
    assert_eq!(meta.size_bytes, 9);
}

#[weft_db_macros::test]
async fn a_stream_part_is_charged_once_however_often_it_is_named(pool: Pool) {
    // A stream is charged per part as it is reserved, so a re-ask must not
    // charge again: a retry would otherwise inch the tenant's usage up every
    // time.
    let (s, _bucket, _c) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let (key, part_size) =
        begin_via(&s, &w, &StorageScope::Project, "b", "stream", None, &big(), None).await.unwrap();
    s.reserve_parts(&w, &key, &[ask(1, part_size)], &big(), &WORKER).await.unwrap();
    let after_first = s.tenant_usage("t1").await.unwrap().1;
    s.reserve_parts(&w, &key, &[ask(1, part_size)], &big(), &WORKER).await.unwrap();
    assert_eq!(s.tenant_usage("t1").await.unwrap().1, after_first, "charged once");
}

#[weft_db_macros::test]
async fn reserved_stream_part_size_cannot_change(pool: Pool) {
    let (s, _bucket, _clock) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let (key, part_size) = begin_via(&s, &w, &StorageScope::Project, "b", "stream", None, &big(), None).await.unwrap();
    s.reserve_parts(&w, &key, &[ask(1, 1)], &big(), &WORKER).await.unwrap();
    let before = s.tenant_usage("t1").await.unwrap();
    assert!(s.reserve_parts(&w, &key, &[ask(1, part_size)], &big(), &WORKER).await.is_err());
    assert_eq!(s.tenant_usage("t1").await.unwrap(), before);
    let (_, missing, _) = s.resume_upload(&w, &key, &big(), &WORKER).await.unwrap();
    assert_eq!(missing[0].size_bytes, 1);
}

#[weft_db_macros::test]
async fn known_size_parts_keep_their_offsets_when_reserved_out_of_order(pool: Pool) {
    let (s, bucket, _clock) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let (key, part_size) = begin_via(&s, &w, &StorageScope::Project, "b", "large", None, &big(), Some(20 * 1024 * 1024)).await.unwrap();
    let second = s.reserve_parts(&w, &key, &[ask(2, part_size)], &big(), &WORKER).await.unwrap();
    assert_eq!(second[0].offset_bytes, part_size);
    let (_, missing, carved) = s.resume_upload(&w, &key, &big(), &WORKER).await.unwrap();
    assert_eq!(missing.iter().map(|part| part.part_number).collect::<Vec<_>>(), vec![1, 2]);
    assert_eq!(missing[0].offset_bytes, 0);
    assert_eq!(carved, 2 * part_size);
    let first = s.reserve_parts(&w, &key, &[ask(1, part_size)], &big(), &WORKER).await.unwrap();
    assert_eq!(first[0].offset_bytes, 0);
    let (_, missing, _) = s.resume_upload(&w, &key, &big(), &WORKER).await.unwrap();
    assert_eq!(missing.iter().find(|part| part.part_number == 2).unwrap().offset_bytes, part_size);
    let mut parts = missing;
    let total = 20_u64 * 1024 * 1024;
    let tail: Vec<_> = (3..=total.div_ceil(part_size) as i32).map(|number| {
        ask(number, part_size.min(total - (number as u64 - 1) * part_size))
    }).collect();
    if !tail.is_empty() { parts.extend(s.reserve_parts(&w, &key, &tail, &big(), &WORKER).await.unwrap()); }
    for part in parts {
        let bytes = vec![part.part_number as u8; part.size_bytes as usize];
        let etag = bucket.put_part(&part.url, body(&bytes)).unwrap();
        s.record_part(&w, &key, part.part_number, &etag).await.unwrap();
    }
    assert_eq!(s.complete_upload(&w, &key).await.unwrap().size_bytes, total);
}

/// A file past its expiry is gone the moment it expires, before the expiry
/// loop removes it: no read, list or identity lookup finds it, a keep or
/// an access does not bring it back, and it counts for nothing against the
/// tenant's quota or file cap.
#[weft_db_macros::test]
async fn an_expired_file_is_gone_before_the_loop_removes_it(pool: Pool) {
    let (s, bucket, clock) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let spec = UploadSpec {
        scope: &StorageScope::Project,
        mime: "b",
        filename: "f",
        keep: Some(KeepTtl::Secs { secs: 10 }),
        declared_size: Some(3),
        content_hash: None,
        identity: Some("source:1"),
        slot: None,
        imported: None,
    };
    let BeginUpload::Ready { key, part_size } = s.begin_upload(&w, &spec, &big()).await.unwrap() else { panic!("fresh") };
    upload_parts(&s, &bucket, &w, &key, part_size, &body(b"abc"), &big()).await.unwrap();
    let file = s.complete_upload(&w, &key).await.unwrap();
    let parsed = weft_core::storage::key::parse_key(&file.key).unwrap();
    assert_eq!(s.tenant_usage("t1").await.unwrap(), (1, 3));

    clock.advance(Duration::from_secs(11));
    assert!(matches!(s.meta(&parsed).await, Err(RuntimeStoreError::NotFound(_))), "not readable");
    assert!(matches!(get_via(&s, &bucket, &parsed, None).await, Err(RuntimeStoreError::NotFound(_))), "no access bump");
    assert!(matches!(s.keep(&parsed, KeepTtl::Default).await, Err(RuntimeStoreError::NotFound(_))), "not kept back to life");
    let prefix = weft_core::storage::key::prefix_for_list(&w, &StorageScope::Project).unwrap();
    assert!(s.list(&prefix).await.unwrap().is_empty(), "not listed");
    assert!(s.find_identity(&w, &StorageScope::Project, "source:1").await.unwrap().is_none(), "not found by identity");
    assert_eq!(s.tenant_usage("t1").await.unwrap(), (0, 0), "counted for nothing");
    let expires: Option<i64> = pool.scalar("SELECT expires_at_unix FROM runtime_file WHERE key = $1", &[&file.key]).await.unwrap();
    assert_eq!(expires, file.expires_at_unix, "nothing moved its expiry");

    // Storing the same source again takes its place.
    let again = s.begin_upload(&w, &spec, &big()).await.unwrap();
    assert!(matches!(again, BeginUpload::Ready { .. }), "a fresh upload, not the expired file: {again:?}");
    assert_eq!(status_of(&pool, &file.key).await, None, "the expired file was removed to make way");
}

// ---------- a page's upload through a slot ----------

/// The type of the place a received upload is sent to.
fn fits(text: &str) -> weft_core::weft_type::WeftType {
    weft_core::weft_type::WeftType::try_parse(text).unwrap()
}

/// What an MP4 starts with: enough for its first bytes to say `video/mp4`.
const MP4: &[u8] = b"\x00\x00\x00\x18ftypmp42\x00\x00\x00\x00isommp42";

fn slot_claims(sid: &str, scope: StorageScope, exp: i64) -> weft_core::storage::slot::UploadSlotClaims {
    weft_core::storage::slot::UploadSlotClaims {
        sid: sid.into(),
        tenant: "t1".into(),
        project: "p1".into(),
        scope,
        max_bytes: 1 << 20,
        accepts: "Video".into(),
        exp,
        keep: None,
    }
}

/// Receive one slot's upload, by a claim naming only it.
async fn claim_one(
    s: &RuntimeStore,
    run: &CallerAuth,
    claims: &weft_core::storage::slot::UploadSlotClaims,
    fits: &weft_core::weft_type::WeftType,
) -> Result<StoredFileMeta, RuntimeStoreError> {
    let mut files = s.claim_uploads(run, &[weft_broker::runtime_store::ClaimedSlot { slot: &claims.sid, claims, fits }]).await?;
    assert_eq!(files.len(), 1, "one file per slot");
    Ok(files.remove(0))
}

/// A page's upload through slot `sid` (ending at `ends`), driven to the
/// end the way `uploadFile` does: begin (with the content's hash when
/// given), the parts, complete. Answers the upload's key and whether it
/// was made from a file the install already held.
async fn slot_upload_via(
    s: &RuntimeStore,
    bucket: &FakeObjectStore,
    sid: &str,
    ends: i64,
    bytes: &[u8],
    sha256: Option<&str>,
) -> (String, bool) {
    let page = worker("t1", "p1", None);
    let spec = UploadSpec {
        scope: &StorageScope::Project,
        mime: "video/mp4",
        filename: "clip.mp4",
        keep: None,
        declared_size: Some(bytes.len() as u64),
        content_hash: None,
        identity: None,
        slot: Some(weft_broker::runtime_store::SlotUpload { sid, ends_unix: ends, max_bytes: 1 << 20, accepts: "Video", sha256 }),
        imported: None,
    };
    match s.begin_slot_upload(&page, &spec, &big()).await.unwrap() {
        BeginUpload::Ready { key, part_size } => {
            upload_parts(s, bucket, &page, &key, part_size, &body(bytes), &big()).await.unwrap();
            s.complete_upload(&page, &key).await.unwrap();
            (key, false)
        }
        BeginUpload::AlreadyStored { key } => (key, true),
        BeginUpload::Resume { .. } => panic!("a fresh slot has nothing to resume"),
    }
}

/// An upload through a slot waits, unread by anyone, until a run receives
/// it: then it is that run's file in the slot's scope, the same run asking
/// again gets the same file, and another run is refused.
#[weft_db_macros::test]
async fn a_slots_upload_is_received_once(pool: Pool) {
    let (s, bucket, clock) = store(&pool).await;
    let ends = clock.now_unix() + 3600;
    let (key, _) = slot_upload_via(&s, &bucket, "s1", ends, MP4, None).await;
    let parsed = weft_core::storage::key::parse_key(&key).unwrap();
    assert!(matches!(s.meta(&parsed).await, Err(RuntimeStoreError::NotFound(_))), "nothing reads it before it is received");
    let claims = slot_claims("s1", StorageScope::Project, ends);
    let received = claim_one(&s, &worker("t1", "p1", Some("c1")), &claims, &fits("Video")).await.unwrap();
    assert_eq!(received.key, key);
    assert_eq!((received.mime_type.as_str(), received.size_bytes), ("video/mp4", MP4.len() as u64));
    assert_eq!(received.expires_at_unix, None, "a project file lives until deleted");
    assert_eq!(claim_one(&s, &worker("t1", "p1", Some("c1")), &claims, &fits("Video")).await.unwrap().key, key, "the same run, the same file");
    assert!(matches!(
        claim_one(&s, &worker("t1", "p1", Some("c2")), &claims, &fits("Video")).await,
        Err(RuntimeStoreError::Conflict(_))
    ));
    assert!(matches!(
        claim_one(&s, &worker("t1", "other", Some("c1")), &claims, &fits("Video")).await,
        Err(RuntimeStoreError::Conflict(_))
    ));
}

/// Two runs receiving the same upload at once: exactly one gets it.
#[weft_db_macros::test]
async fn two_runs_at_once_receive_a_slots_upload_once(pool: Pool) {
    let (s, bucket, clock) = store(&pool).await;
    let ends = clock.now_unix() + 3600;
    slot_upload_via(&s, &bucket, "s1", ends, MP4, None).await;
    let claims = slot_claims("s1", StorageScope::Project, ends);
    let (one, two) = (worker("t1", "p1", Some("c1")), worker("t1", "p1", Some("c2")));
    let video = fits("Video");
    let (a, b) = tokio::join!(claim_one(&s, &one, &claims, &video), claim_one(&s, &two, &claims, &video));
    assert_eq!([a.is_ok(), b.is_ok()].iter().filter(|ok| **ok).count(), 1, "{a:?} {b:?}");
}

/// A run-scoped slot's upload becomes a file of the run that receives it,
/// copied inside the bucket, and the upload it came from goes.
#[weft_db_macros::test]
async fn a_run_scoped_upload_moves_into_the_run(pool: Pool) {
    let (s, bucket, clock) = store(&pool).await;
    let ends = clock.now_unix() + 3600;
    let (key, _) = slot_upload_via(&s, &bucket, "s1", ends, MP4, None).await;
    let claims = slot_claims("s1", StorageScope::Run, ends);
    let run = worker("t1", "p1", Some("c1"));
    let received = claim_one(&s, &run, &claims, &fits("Video")).await.unwrap();
    assert!(received.key.starts_with("t1/run/c1/"), "{}", received.key);
    assert_eq!(bucket.get(&object_key(&received.key)).await.unwrap().as_deref(), Some(MP4));
    assert_eq!(bucket.get(&object_key(&key)).await.unwrap(), None, "the upload it came from is gone");
    assert_eq!(claim_one(&s, &run, &claims, &fits("Video")).await.unwrap().key, received.key, "asked again, the same file");
    assert_eq!(delete_run_files(&s, "t1", "c1").await, 1, "it goes with its run");
}

/// An upload nobody receives goes when its slot ends.
#[weft_db_macros::test]
async fn an_unreceived_upload_goes_with_its_slot(pool: Pool) {
    let (s, bucket, clock) = store(&pool).await;
    let ends = clock.now_unix() + 100;
    slot_upload_via(&s, &bucket, "s1", ends, MP4, None).await;
    assert_eq!(expire(&s, &pool).await, 0);
    clock.advance(Duration::from_secs(101));
    assert_eq!(expire(&s, &pool).await, 1);
    assert!(matches!(
        claim_one(&s, &worker("t1", "p1", Some("c1")), &slot_claims("s1", StorageScope::Project, ends), &fits("Video")).await,
        Err(RuntimeStoreError::NoUpload(_))
    ));
}

/// Bytes that are not what the slot accepts are refused as they land,
/// and the upload goes.
#[weft_db_macros::test]
async fn an_upload_of_another_type_is_refused_and_goes(pool: Pool) {
    let (s, bucket, clock) = store(&pool).await;
    let ends = clock.now_unix() + 3600;
    let refused = typed_slot_upload(&s, &bucket, "s1", ends, "Video", "video/mp4", b"just some text, not a video").await;
    assert!(matches!(refused, Err(RuntimeStoreError::Invalid(_))), "{refused:?}");
    assert!(bucket.is_empty(), "the upload's object went");
    assert!(matches!(
        claim_one(&s, &worker("t1", "p1", Some("c1")), &slot_claims("s1", StorageScope::Project, ends), &fits("Video")).await,
        Err(RuntimeStoreError::NoUpload(_))
    ));
}

/// A slot's upload whose page's complete died is landed by the expiry
/// loop's drive, which holds it to the slot's type just as the page's complete
/// does: bytes that are not what the slot accepts never wait to be
/// received, and the upload goes.
#[weft_db_macros::test]
async fn a_slot_upload_landed_by_a_drive_is_held_to_its_type(pool: Pool) {
    let (s, bucket, clock) = store(&pool).await;
    let ends = clock.now_unix() + 3600;
    let page = worker("t1", "p1", None);
    let bytes = b"just some text, not a video";
    let spec = UploadSpec {
        scope: &StorageScope::Project,
        mime: "video/mp4",
        filename: "clip.mp4",
        keep: None,
        declared_size: Some(bytes.len() as u64),
        content_hash: None,
        identity: None,
        slot: Some(weft_broker::runtime_store::SlotUpload { sid: "s1", ends_unix: ends, max_bytes: 1 << 20, accepts: "Video", sha256: None }),
        imported: None,
    };
    let BeginUpload::Ready { key, part_size } = s.begin_slot_upload(&page, &spec, &big()).await.unwrap() else {
        panic!("a fresh slot begins a fresh upload");
    };
    upload_parts(&s, &bucket, &page, &key, part_size, &body(bytes), &big()).await.unwrap();
    bucket.fail_next_complete();
    assert!(matches!(s.complete_upload(&page, &key).await, Err(RuntimeStoreError::Completing(_))));
    assert_eq!(status_of(&pool, &key).await.as_deref(), Some("completing"), "the page's complete died");
    clock.advance(Duration::from_secs(CLAIM_LEASE_SECS as u64 + 1));
    s.expire().await.unwrap();
    assert_eq!(s.tenant_usage("t1").await.unwrap(), (0, 0), "the expiry loop's drive refused it");
    assert_eq!(status_of(&pool, &key).await, None);
    assert!(bucket.is_empty(), "the upload's object went");
    assert!(matches!(
        claim_one(&s, &worker("t1", "p1", Some("c1")), &slot_claims("s1", StorageScope::Project, ends), &fits("Video")).await,
        Err(RuntimeStoreError::NoUpload(_))
    ));
}

/// Content weft hashed itself is matched when a page states the same
/// sha256: the upload is made from it inside the bucket and no part is
/// sent. A hash nobody computed matches nothing.
#[weft_db_macros::test]
async fn the_same_content_is_not_uploaded_twice(pool: Pool) {
    use sha2::Digest;
    let (s, bucket, clock) = store(&pool).await;
    let ends = clock.now_unix() + 3600;
    let hash = weft_core::access::hex_of(&sha2::Sha256::digest(MP4));
    let (first, made_from) = slot_upload_via(&s, &bucket, "s1", ends, MP4, Some(&hash)).await;
    assert!(!made_from, "nothing hashed yet: the first upload is whole");
    s.hash_upload(&first).await.unwrap();
    // Waiting to be received, it is no source: it goes once a run
    // receives it or refuses it.
    let (unmatched, made_from) = slot_upload_via(&s, &bucket, "s3", ends, MP4, Some(&hash)).await;
    assert!(!made_from, "an upload nobody received yet is not copied from");
    assert_ne!(unmatched, first);
    claim_one(&s, &worker("t1", "p1", Some("c0")), &slot_claims("s1", StorageScope::Project, ends), &fits("Video")).await.unwrap();
    let parts_before = bucket.calls().iter().filter(|c| matches!(c, weft_platform_traits::object_store::fake::FakeCall::PresignPart { .. })).count();
    let (second, made_from) = slot_upload_via(&s, &bucket, "s2", ends, MP4, Some(&hash)).await;
    assert!(made_from, "the same content is made inside the bucket");
    assert_ne!(second, first);
    assert_eq!(bucket.get(&object_key(&second)).await.unwrap().as_deref(), Some(MP4));
    let parts_after = bucket.calls().iter().filter(|c| matches!(c, weft_platform_traits::object_store::fake::FakeCall::PresignPart { .. })).count();
    assert_eq!(parts_before, parts_after, "no part was asked for");
    let received = claim_one(&s, &worker("t1", "p1", Some("c1")), &slot_claims("s2", StorageScope::Project, ends), &fits("Video")).await.unwrap();
    assert_eq!(received.key, second);
}

/// A copy is made inside the bucket into the asked scope, a new file with
/// the source's name, type and bytes.
#[weft_db_macros::test]
async fn a_copy_is_a_new_file_made_inside_the_bucket(pool: Pool) {
    let (s, bucket, _clock) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let source = put_via(&s, &bucket, &w, &StorageScope::Run, "image/png", "a.png", None, &big(), body(b"PNG!")).await.unwrap();
    let parsed = weft_core::storage::key::parse_key(&source.key).unwrap();
    let copy = s.copy_file(&w, &parsed, &StorageScope::Project, None, None, &big()).await.unwrap();
    assert!(copy.key.starts_with("t1/project/p1/"), "{}", copy.key);
    assert_eq!((copy.filename.as_str(), copy.mime_type.as_str(), copy.size_bytes), ("a.png", "image/png", 4));
    assert_eq!(bucket.get(&object_key(&copy.key)).await.unwrap().as_deref(), Some(&b"PNG!"[..]));
    assert!(bucket.calls().iter().any(|c| matches!(c, weft_platform_traits::object_store::fake::FakeCall::Copy { .. })));
    assert_eq!(s.tenant_usage("t1").await.unwrap(), (2, 8), "the copy is charged like any file");
}

/// A page's upload of `bytes` stated as `mime` through slot `sid` that
/// takes `accepts`, landed (and so held to the slot's type). Answers the upload's key and the slot's claims.
async fn typed_slot_upload(
    s: &RuntimeStore,
    bucket: &FakeObjectStore,
    sid: &str,
    ends: i64,
    accepts: &str,
    mime: &str,
    bytes: &[u8],
) -> Result<(String, weft_core::storage::slot::UploadSlotClaims), RuntimeStoreError> {
    let page = worker("t1", "p1", None);
    let mut claims = slot_claims(sid, StorageScope::Project, ends);
    claims.accepts = accepts.into();
    let spec = UploadSpec {
        scope: &StorageScope::Project,
        mime,
        filename: "upload",
        keep: None,
        declared_size: Some(bytes.len() as u64),
        content_hash: None,
        identity: None,
        slot: Some(weft_broker::runtime_store::SlotUpload { sid, ends_unix: ends, max_bytes: claims.max_bytes, accepts, sha256: None }),
        imported: None,
    };
    let BeginUpload::Ready { key, part_size } = s.begin_slot_upload(&page, &spec, &big()).await? else {
        panic!("a fresh slot begins a fresh upload");
    };
    upload_parts(s, bucket, &page, &key, part_size, &body(bytes), &big()).await?;
    s.complete_upload(&page, &key).await?;
    Ok((key, claims))
}

/// A claim naming several slots receives them all, in its order, or
/// none: one refused slot (named as the call sent it) leaves the others
/// open, with no copy left of a run-scoped one, for a corrected call. A
/// slot named twice is one file.
#[weft_db_macros::test]
async fn a_claim_of_several_slots_receives_all_or_none(pool: Pool) {
    use weft_broker::runtime_store::ClaimedSlot;
    let (s, bucket, clock) = store(&pool).await;
    let ends = clock.now_unix() + 3600;
    let (first, _) = slot_upload_via(&s, &bucket, "s1", ends, MP4, None).await;
    let into_run = slot_claims("s1", StorageScope::Run, ends);
    let empty = slot_claims("s2", StorageScope::Project, ends);
    let video = fits("Video");
    let run = worker("t1", "p1", Some("c1"));
    let to_run = ClaimedSlot { slot: "weft-upload:one", claims: &into_run, fits: &video };
    let to_project = ClaimedSlot { slot: "weft-upload:two", claims: &empty, fits: &video };
    let refused = s.claim_uploads(&run, &[to_run, to_project]).await;
    let Err(RuntimeStoreError::NoUpload(why)) = refused else { panic!("{refused:?}") };
    assert!(why.starts_with("upload slot 'weft-upload:two': "), "{why}");
    assert_eq!(bucket.keys(), vec![object_key(&first)], "the run's copy of the first slot was deleted");
    assert_eq!(status_of(&pool, &first).await.as_deref(), Some("uploaded"), "the first slot is still open");
    let (second, _) = slot_upload_via(&s, &bucket, "s2", ends, MP4, None).await;
    let other = worker("t1", "p1", Some("c2"));
    let both = s.claim_uploads(&other, &[to_project, to_run, to_run]).await.unwrap();
    assert_eq!(both.len(), 3);
    assert_eq!(both[0].key, second, "in the order of the claims");
    assert_ne!(both[1].key, first, "the run-scoped one is the run's own copy");
    assert_eq!(both[1], both[2], "a slot named twice is one file");
    assert_eq!(status_of(&pool, &first).await, None, "the upload it came from went");
}

/// An empty file has no first bytes to read: a slot that takes any file
/// lands it, and one that takes only images refuses it.
#[weft_db_macros::test]
async fn an_empty_slot_upload_lands_for_any_file_and_is_refused_for_an_image(pool: Pool) {
    let (s, bucket, clock) = store(&pool).await;
    let ends = clock.now_unix() + 3600;
    let (key, claims) = typed_slot_upload(&s, &bucket, "s1", ends, "File", "", b"").await.unwrap();
    let received = claim_one(&s, &worker("t1", "p1", Some("c1")), &claims, &fits("File")).await.unwrap();
    assert_eq!((received.key.as_str(), received.size_bytes), (key.as_str(), 0));
    let refused = typed_slot_upload(&s, &bucket, "s2", ends, "Image", "", b"").await;
    assert!(matches!(refused, Err(RuntimeStoreError::Invalid(_))), "{refused:?}");
}

/// A file that does not fit the place it is sent to is refused before it
/// is received: the slot stays open, and a call sending it where it fits
/// receives it (a container as the half the place takes).
#[weft_db_macros::test]
async fn a_claim_into_a_place_the_file_does_not_fit_leaves_the_slot_open(pool: Pool) {
    let (s, bucket, clock) = store(&pool).await;
    let ends = clock.now_unix() + 3600;
    let (key, claims) = typed_slot_upload(&s, &bucket, "s1", ends, "Media", "video/mp4", MP4).await.unwrap();
    let refused = claim_one(&s, &worker("t1", "p1", Some("c1")), &claims, &fits("Image")).await;
    let Err(RuntimeStoreError::Invalid(why)) = refused else { panic!("{refused:?}") };
    assert!(why.contains("Media") && why.contains("Image"), "names both types: {why}");
    let received = claim_one(&s, &worker("t1", "p1", Some("c2")), &claims, &fits("Audio | Null")).await.unwrap();
    assert_eq!((received.key.as_str(), received.mime_type.as_str()), (key.as_str(), "audio/mp4"));
}

/// A begin or a check through a slot is refused past the slot's largest,
/// and past the largest file the store takes, before anything is
/// reserved; a check reserves nothing either way.
#[weft_db_macros::test]
async fn a_slot_refuses_a_file_heavier_than_it_takes(pool: Pool) {
    let (s, _bucket, clock) = store(&pool).await;
    let page = worker("t1", "p1", None);
    let slot = weft_broker::runtime_store::SlotUpload { sid: "s1", ends_unix: clock.now_unix() + 3600, max_bytes: 10, accepts: "Video", sha256: None };
    let spec = |size: u64| UploadSpec {
        scope: &StorageScope::Project,
        mime: "video/mp4",
        filename: "clip.mp4",
        keep: None,
        declared_size: Some(size),
        content_hash: None,
        identity: None,
        slot: Some(slot),
        imported: None,
    };
    for size in [11, weft_broker::runtime_store::LARGEST_FILE_BYTES + 1] {
        assert!(s.begin_slot_upload(&page, &spec(size), &big()).await.is_err(), "{size}");
        assert!(s.check_slot_upload(&page, &spec(size), &big()).await.is_err(), "{size}");
    }
    assert!(matches!(s.begin_slot_upload(&page, &spec(11), &big()).await, Err(RuntimeStoreError::QuotaExceeded(_))));
    s.check_slot_upload(&page, &spec(10), &big()).await.unwrap();
    let tight = budget(Entitlement::from_disk_bytes(5));
    assert!(matches!(s.check_slot_upload(&page, &spec(10), &tight).await, Err(RuntimeStoreError::QuotaExceeded(_))));
    assert_eq!(s.tenant_usage("t1").await.unwrap(), (0, 0), "a check reserves nothing");
}

/// A copy is of a stored file only: an upload still on its way up, or one
/// waiting to be received, is not one.
#[weft_db_macros::test]
async fn a_copy_of_a_file_not_stored_yet_is_refused(pool: Pool) {
    let (s, bucket, clock) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let (pending, _) = begin_via(&s, &w, &StorageScope::Project, "image/png", "a.png", None, &big(), Some(4)).await.unwrap();
    let (uploaded, _) = slot_upload_via(&s, &bucket, "s1", clock.now_unix() + 3600, MP4, None).await;
    for key in [pending, uploaded] {
        let parsed = weft_core::storage::key::parse_key(&key).unwrap();
        let copied = s.copy_file(&w, &parsed, &StorageScope::Project, None, None, &big()).await;
        assert!(matches!(copied, Err(RuntimeStoreError::NotFound(_))), "{key}: {copied:?}");
    }
}

/// A copy that lands another size than the source's is ended: nothing
/// stays stored or reserved for it.
#[weft_db_macros::test]
async fn a_copy_of_another_size_than_reserved_is_ended(pool: Pool) {
    let (s, bucket, _clock) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let source = put_via(&s, &bucket, &w, &StorageScope::Run, "image/png", "a.png", None, &big(), body(b"PNG!")).await.unwrap();
    bucket.put(&object_key(&source.key), body(b"PNG! and more")).await.unwrap();
    let parsed = weft_core::storage::key::parse_key(&source.key).unwrap();
    assert!(s.copy_file(&w, &parsed, &StorageScope::Project, None, None, &big()).await.is_err());
    assert_eq!(s.tenant_usage("t1").await.unwrap(), (1, 4), "only the source is stored and charged");
}

/// A copy whose bucket copy fails leaves no row and no reserved bytes.
#[weft_db_macros::test]
async fn a_failed_bucket_copy_leaves_nothing_behind(pool: Pool) {
    let (s, bucket, _clock) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let source = put_via(&s, &bucket, &w, &StorageScope::Run, "image/png", "a.png", None, &big(), body(b"PNG!")).await.unwrap();
    bucket.delete(&object_key(&source.key)).await.unwrap();
    let parsed = weft_core::storage::key::parse_key(&source.key).unwrap();
    assert!(s.copy_file(&w, &parsed, &StorageScope::Project, None, None, &big()).await.is_err());
    let rows: i64 = pool.scalar("SELECT COUNT(*) FROM runtime_file", &[]).await.unwrap();
    assert_eq!(rows, 1, "only the source's row");
    assert_eq!(s.tenant_usage("t1").await.unwrap(), (1, 4));
}

/// A copy is charged like any file: one past the quota is refused, and
/// one naming an identity the scope holds answers that file and copies
/// nothing.
#[weft_db_macros::test]
async fn a_copy_is_held_to_the_quota_and_to_its_identity(pool: Pool) {
    let (s, bucket, _clock) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let source = put_via(&s, &bucket, &w, &StorageScope::Run, "image/png", "a.png", None, &big(), body(b"PNG!")).await.unwrap();
    let parsed = weft_core::storage::key::parse_key(&source.key).unwrap();
    let tight = budget(Entitlement::from_disk_bytes(6));
    assert!(matches!(
        s.copy_file(&w, &parsed, &StorageScope::Project, None, None, &tight).await,
        Err(RuntimeStoreError::QuotaExceeded(_))
    ));
    let first = s.copy_file(&w, &parsed, &StorageScope::Project, Some("doc-1"), None, &big()).await.unwrap();
    let copies = || bucket.calls().iter().filter(|c| matches!(c, weft_platform_traits::object_store::fake::FakeCall::Copy { .. })).count();
    let before = copies();
    let again = s.copy_file(&w, &parsed, &StorageScope::Project, Some("doc-1"), None, &big()).await.unwrap();
    assert_eq!(again.key, first.key);
    assert_eq!(copies(), before, "nothing was copied the second time");
}

/// A copy carries the source's content hash: the same bytes, matched by
/// a later page sending them.
#[weft_db_macros::test]
async fn a_copy_keeps_the_content_hash(pool: Pool) {
    let (s, bucket, clock) = store(&pool).await;
    let (uploaded, _) = slot_upload_via(&s, &bucket, "s1", clock.now_unix() + 3600, MP4, None).await;
    s.hash_upload(&uploaded).await.unwrap();
    let run = worker("t1", "p1", Some("c1"));
    let received = claim_one(&s, &run, &slot_claims("s1", StorageScope::Project, clock.now_unix() + 3600), &fits("Video")).await.unwrap();
    let parsed = weft_core::storage::key::parse_key(&received.key).unwrap();
    let copy = s.copy_file(&run, &parsed, &StorageScope::Project, None, None, &big()).await.unwrap();
    let hash = |key: String| {
        let pool = pool.clone();
        async move {
            let hash: Option<String> = pool.scalar("SELECT content_sha256 FROM runtime_file WHERE key = $1", &[&key]).await.unwrap();
            hash
        }
    };
    let source_hash = hash(received.key.clone()).await;
    assert!(source_hash.is_some());
    assert_eq!(hash(copy.key).await, source_hash);
}

/// A run asking again for a file it received is handed it only where it
/// fits as the type it was received as: one file is one type, so a
/// container received as audio is refused for a video place.
#[weft_db_macros::test]
async fn a_received_file_asked_for_again_is_held_to_its_type(pool: Pool) {
    let (s, bucket, clock) = store(&pool).await;
    let ends = clock.now_unix() + 3600;
    let (key, claims) = typed_slot_upload(&s, &bucket, "s1", ends, "Media", "video/mp4", MP4).await.unwrap();
    let run = worker("t1", "p1", Some("c1"));
    let received = claim_one(&s, &run, &claims, &fits("Audio")).await.unwrap();
    assert_eq!((received.key.as_str(), received.mime_type.as_str()), (key.as_str(), "audio/mp4"));
    let refused = claim_one(&s, &run, &claims, &fits("Video")).await;
    let Err(RuntimeStoreError::Invalid(why)) = refused else { panic!("{refused:?}") };
    assert!(why.contains("audio/mp4") && why.contains("takes it only as video/mp4"), "{why}");
    let refused = claim_one(&s, &run, &claims, &fits("Image")).await;
    let Err(RuntimeStoreError::Invalid(why)) = refused else { panic!("{refused:?}") };
    assert!(why.contains("Image"), "{why}");
    assert_eq!(claim_one(&s, &run, &claims, &fits("Media")).await.unwrap().key, key, "a place taking audio gets it again");
}

/// A check through a slot whose upload is already on its way up, or
/// landed, is accepted whatever the tenant's caps: the begin carries on
/// with that upload and reserves nothing new.
#[weft_db_macros::test]
async fn a_check_on_a_slot_that_holds_an_upload_is_accepted(pool: Pool) {
    let (s, bucket, clock) = store(&pool).await;
    let ends = clock.now_unix() + 3600;
    slot_upload_via(&s, &bucket, "s1", ends, MP4, None).await;
    let page = worker("t1", "p1", None);
    let spec = |sid: &'static str| UploadSpec {
        scope: &StorageScope::Project,
        mime: "video/mp4",
        filename: "clip.mp4",
        keep: None,
        declared_size: Some(MP4.len() as u64),
        content_hash: None,
        identity: None,
        slot: Some(weft_broker::runtime_store::SlotUpload { sid, ends_unix: ends, max_bytes: 1 << 20, accepts: "Video", sha256: None }),
        imported: None,
    };
    let full = budget(Entitlement::from_disk_bytes(MP4.len() as u64));
    s.check_slot_upload(&page, &spec("s1"), &full).await.unwrap();
    assert!(matches!(s.check_slot_upload(&page, &spec("s2"), &full).await, Err(RuntimeStoreError::QuotaExceeded(_))));
}

/// Where a held copy pauses, in the order a copy meets them: before it
/// does anything, after it reported one piece done (one more than the
/// fake's own report, so a test can stage a copy that keeps going), or
/// after the bytes are copied.
#[derive(Clone, Copy, PartialEq)]
enum CopyHold {
    Before,
    Reported,
    After,
}

/// A paused copy: where it pauses, the signal that it got there, and
/// the signal that lets it go.
type CopyPause = (CopyHold, tokio::sync::oneshot::Sender<()>, tokio::sync::oneshot::Receiver<()>);

/// The fake bucket, with its next `copy` paused (at one or more of the
/// [`CopyHold`] points) until the test lets it go: what stages a row
/// changing while a copy runs.
struct HeldCopyBucket {
    inner: Arc<FakeObjectStore>,
    hold: parking_lot::Mutex<Vec<CopyPause>>,
}

impl HeldCopyBucket {
    /// Pause the next copy at `at` too: answers (the copy reached the
    /// pause, let it go).
    fn hold_next_copy(&self, at: CopyHold) -> (tokio::sync::oneshot::Receiver<()>, tokio::sync::oneshot::Sender<()>) {
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        self.hold.lock().push((at, entered_tx, release_rx));
        (entered_rx, release_tx)
    }

    /// Run `work`, then drop every pause it never reached: a test whose
    /// code under test never copies fails at its wait for the pause
    /// instead of waiting forever.
    async fn running<T>(&self, work: impl std::future::Future<Output = T>) -> T {
        let out = work.await;
        self.hold.lock().clear();
        out
    }
}

/// Stop at `at` when one of `pauses` is there: say so, and wait to be let go.
async fn pause_at(pauses: &mut Vec<CopyPause>, at: CopyHold) {
    if let Some(i) = pauses.iter().position(|(point, _, _)| *point == at) {
        let (_, entered, release) = pauses.remove(i);
        entered.send(()).unwrap();
        release.await.unwrap();
    }
}

#[async_trait::async_trait]
impl ObjectStore for HeldCopyBucket {
    async fn put(&self, key: &str, bytes: bytes::Bytes) -> anyhow::Result<()> {
        self.inner.put(key, bytes).await
    }
    async fn get(&self, key: &str) -> anyhow::Result<Option<bytes::Bytes>> {
        self.inner.get(key).await
    }
    async fn get_range(&self, key: &str, start: u64, end: u64) -> anyhow::Result<Option<bytes::Bytes>> {
        self.inner.get_range(key, start, end).await
    }
    async fn exists(&self, key: &str) -> anyhow::Result<bool> {
        self.inner.exists(key).await
    }
    async fn size(&self, key: &str) -> anyhow::Result<Option<u64>> {
        self.inner.size(key).await
    }
    async fn delete(&self, key: &str) -> anyhow::Result<()> {
        self.inner.delete(key).await
    }
    async fn list(&self, prefix: &str) -> anyhow::Result<Vec<weft_platform_traits::ObjectEntry>> {
        self.inner.list(prefix).await
    }
    async fn presign_get(&self, key: &str, link: &weft_platform_traits::GetLink) -> anyhow::Result<String> {
        self.inner.presign_get(key, link).await
    }
    async fn create_multipart(&self, key: &str, content_type: &str) -> anyhow::Result<String> {
        self.inner.create_multipart(key, content_type).await
    }
    async fn copy(&self, from: &str, to: &str, progress: &dyn CopyProgress) -> anyhow::Result<u64> {
        let mut pauses = std::mem::take(&mut *self.hold.lock());
        pause_at(&mut pauses, CopyHold::Before).await;
        if pauses.iter().any(|(point, _, _)| *point == CopyHold::Reported) {
            progress.copied_piece().await?;
            pause_at(&mut pauses, CopyHold::Reported).await;
        }
        let copied = self.inner.copy(from, to, progress).await;
        pause_at(&mut pauses, CopyHold::After).await;
        copied
    }
    async fn presign_part(
        &self,
        key: &str,
        upload_id: &str,
        part_number: i32,
        part_size: u64,
        audience: &PresignAudience,
        ttl_secs: u64,
    ) -> anyhow::Result<String> {
        self.inner.presign_part(key, upload_id, part_number, part_size, audience, ttl_secs).await
    }
    async fn upload_part(&self, key: &str, upload_id: &str, part_number: i32, bytes: bytes::Bytes) -> anyhow::Result<String> {
        self.inner.upload_part(key, upload_id, part_number, bytes).await
    }
    async fn complete_multipart(&self, key: &str, upload_id: &str, parts: &[(i32, String)]) -> anyhow::Result<u64> {
        self.inner.complete_multipart(key, upload_id, parts).await
    }
    async fn abort_multipart(&self, key: &str, upload_id: &str) -> anyhow::Result<()> {
        self.inner.abort_multipart(key, upload_id).await
    }
    async fn multipart_exists(&self, key: &str, upload_id: &str) -> anyhow::Result<bool> {
        self.inner.multipart_exists(key, upload_id).await
    }
}

/// [`store`] over a bucket whose copies a test can pause.
async fn store_with_held_copies(pool: &Pool) -> (Arc<RuntimeStore>, Arc<HeldCopyBucket>, Arc<FakeObjectStore>, Arc<FakeClock>) {
    let (_, inner, clock) = store(pool).await;
    let bucket = Arc::new(HeldCopyBucket { inner: inner.clone(), hold: Default::default() });
    (Arc::new(RuntimeStore::new(pool.clone(), bucket.clone(), clock.clone())), bucket, inner, clock)
}

/// While a copy fills its upload, the upload is claimed: a page beginning
/// its slot again is told it is completing, never handed the multipart
/// the copy aborted.
#[weft_db_macros::test]
async fn a_slot_begun_again_while_its_copy_runs_is_told_it_is_completing(pool: Pool) {
    use sha2::Digest;
    let (s, held, bucket, clock) = store_with_held_copies(&pool).await;
    let ends = clock.now_unix() + 3600;
    let hash = weft_core::access::hex_of(&sha2::Sha256::digest(MP4));
    let (first, _) = slot_upload_via(&s, &bucket, "s1", ends, MP4, Some(&hash)).await;
    s.hash_upload(&first).await.unwrap();
    claim_one(&s, &worker("t1", "p1", Some("c0")), &slot_claims("s1", StorageScope::Project, ends), &fits("Video")).await.unwrap();
    let page = worker("t1", "p1", None);
    let spec = UploadSpec {
        scope: &StorageScope::Project,
        mime: "video/mp4",
        filename: "clip.mp4",
        keep: None,
        declared_size: Some(MP4.len() as u64),
        content_hash: None,
        identity: None,
        slot: Some(weft_broker::runtime_store::SlotUpload { sid: "s2", ends_unix: ends, max_bytes: 1 << 20, accepts: "Video", sha256: Some(&hash) }),
        imported: None,
    };
    let (entered, release) = held.hold_next_copy(CopyHold::Before);
    let meanwhile = async {
        entered.await.unwrap();
        let again = s.begin_slot_upload(&page, &spec, &big()).await;
        assert!(matches!(again, Err(RuntimeStoreError::Completing(_))), "{again:?}");
        release.send(()).unwrap();
    };
    let ents = big();
    let (begun, ()) = tokio::join!(held.running(s.begin_slot_upload(&page, &spec, &ents)), meanwhile);
    let BeginUpload::AlreadyStored { key } = begun.unwrap() else { panic!("the copy lands it") };
    assert_eq!(bucket.get(&object_key(&key)).await.unwrap().as_deref(), Some(MP4));
}

/// A copy whose upload was ended while it ran (its claim lapsed and the
/// expiry loop took the copy for dead) deletes the object it made: no object
/// stays that no row points at.
#[weft_db_macros::test]
async fn a_copy_whose_upload_was_reaped_while_it_ran_leaves_nothing(pool: Pool) {
    let (s, held, bucket, clock) = store_with_held_copies(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let source = put_via(&s, &bucket, &w, &StorageScope::Run, "image/png", "a.png", None, &big(), body(b"PNG!")).await.unwrap();
    let parsed = weft_core::storage::key::parse_key(&source.key).unwrap();
    let (entered, release) = held.hold_next_copy(CopyHold::Before);
    let meanwhile = async {
        entered.await.unwrap();
        clock.advance(Duration::from_secs(CLAIM_LEASE_SECS as u64 + 1));
        s.expire().await.unwrap();
        assert_eq!(s.tenant_usage("t1").await.unwrap(), (1, 4), "the expiry loop ended the copy's upload");
        release.send(()).unwrap();
    };
    let ents = big();
    let (copied, ()) = tokio::join!(held.running(s.copy_file(&w, &parsed, &StorageScope::Project, None, None, &ents)), meanwhile);
    assert!(copied.is_err(), "{copied:?}");
    assert_eq!(bucket.keys(), vec![object_key(&source.key)], "only the source's object");
    let rows: i64 = pool.scalar("SELECT COUNT(*) FROM runtime_file", &[]).await.unwrap();
    assert_eq!(rows, 1, "only the source's row");
}

/// A copy whose upload a drive landed while it ran (the claim lapsed and
/// the expiry loop found the copy whole) answers the file that drive landed,
/// with the source's content hash.
#[weft_db_macros::test]
async fn a_copy_landed_by_a_drive_while_it_ran_answers_that_file(pool: Pool) {
    let (s, held, bucket, clock) = store_with_held_copies(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let source = put_via(&s, &bucket, &w, &StorageScope::Run, "image/png", "a.png", None, &big(), body(b"PNG!")).await.unwrap();
    let hash = "ab".repeat(32);
    pool.execute("UPDATE runtime_file SET content_sha256 = $2 WHERE key = $1", &[&source.key, &hash]).await.unwrap();
    let parsed = weft_core::storage::key::parse_key(&source.key).unwrap();
    let (entered, release) = held.hold_next_copy(CopyHold::After);
    let meanwhile = async {
        entered.await.unwrap();
        clock.advance(Duration::from_secs(CLAIM_LEASE_SECS as u64 + 1));
        s.expire().await.unwrap();
        assert_eq!(s.tenant_usage("t1").await.unwrap(), (2, 8), "the expiry loop landed the whole copy");
        release.send(()).unwrap();
    };
    let ents = big();
    let (copied, ()) = tokio::join!(held.running(s.copy_file(&w, &parsed, &StorageScope::Project, None, None, &ents)), meanwhile);
    let copy = copied.unwrap();
    assert_eq!((copy.size_bytes, copy.mime_type.as_str()), (4, "image/png"));
    assert_eq!(bucket.get(&object_key(&copy.key)).await.unwrap().as_deref(), Some(&b"PNG!"[..]));
    let copied_hash: Option<String> =
        pool.scalar("SELECT content_sha256 FROM runtime_file WHERE key = $1", &[&copy.key]).await.unwrap();
    assert_eq!(copied_hash.as_deref(), Some(hash.as_str()));
}

/// A copy that keeps reporting pieces done keeps its claim: each report
/// renews the claim's stamp, so an expiry pass a whole lease after the copy
/// began, and less than one after its last report, leaves it running.
#[weft_db_macros::test]
async fn a_copy_that_keeps_reporting_progress_is_never_taken_for_dead(pool: Pool) {
    let (s, held, bucket, clock) = store_with_held_copies(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let source = put_via(&s, &bucket, &w, &StorageScope::Run, "image/png", "a.png", None, &big(), body(b"PNG!")).await.unwrap();
    let parsed = weft_core::storage::key::parse_key(&source.key).unwrap();
    let (started, go_on) = held.hold_next_copy(CopyHold::Before);
    let (reported, finish) = held.hold_next_copy(CopyHold::Reported);
    let copy_row = || {
        let (pool, source) = (pool.clone(), source.key.clone());
        async move {
            let row: (String, i64) =
                pool.one("SELECT status, progressed_at_unix FROM runtime_file WHERE key <> $1", &[&source]).await.unwrap();
            row
        }
    };
    let meanwhile = async {
        started.await.unwrap();
        let began = clock.now_unix();
        clock.advance(Duration::from_secs(CLAIM_LEASE_SECS as u64));
        go_on.send(()).unwrap();
        reported.await.unwrap();
        assert_eq!(copy_row().await, ("completing".to_string(), began + CLAIM_LEASE_SECS), "the report renewed the claim");
        clock.advance(Duration::from_secs(CLAIM_LEASE_SECS as u64));
        s.expire().await.unwrap();
        assert_eq!(copy_row().await.0, "completing", "the expiry loop left the live copy alone");
        finish.send(()).unwrap();
    };
    let ents = big();
    let (copied, ()) = tokio::join!(held.running(s.copy_file(&w, &parsed, &StorageScope::Project, None, None, &ents)), meanwhile);
    let copy = copied.unwrap();
    assert_eq!(bucket.get(&object_key(&copy.key)).await.unwrap().as_deref(), Some(&b"PNG!"[..]));
}

/// A begin of `filename`, `size` bytes, through slot "s1".
fn slot_spec<'a>(filename: &'a str, size: u64, sha256: Option<&'a str>, ends: i64) -> UploadSpec<'a> {
    UploadSpec {
        scope: &StorageScope::Project,
        mime: "video/mp4",
        filename,
        keep: None,
        declared_size: Some(size),
        content_hash: None,
        identity: None,
        slot: Some(weft_broker::runtime_store::SlotUpload { sid: "s1", ends_unix: ends, max_bytes: 1 << 20, accepts: "Video", sha256 }),
        imported: None,
    }
}

/// A slot carries on only with the file it holds: begun or checked again
/// for another file (another name, size, or stated sha256 than the one
/// weft hashed), it is refused, naming the file it holds.
#[weft_db_macros::test]
async fn a_slot_begun_again_for_another_file_is_refused(pool: Pool) {
    use sha2::Digest;
    let (s, bucket, clock) = store(&pool).await;
    let ends = clock.now_unix() + 3600;
    let hash = weft_core::access::hex_of(&sha2::Sha256::digest(MP4));
    let (key, _) = slot_upload_via(&s, &bucket, "s1", ends, MP4, None).await;
    s.hash_upload(&key).await.unwrap();
    let page = worker("t1", "p1", None);
    let other = "cd".repeat(32);
    let size = MP4.len() as u64;
    for spec in [slot_spec("dog.mp4", size, None, ends), slot_spec("clip.mp4", size + 1, None, ends), slot_spec("clip.mp4", size, Some(&other), ends)] {
        let begun = s.begin_slot_upload(&page, &spec, &big()).await;
        let Err(RuntimeStoreError::Conflict(why)) = begun else { panic!("{begun:?}") };
        assert!(why.contains("'clip.mp4'"), "{why}");
        assert!(matches!(s.check_slot_upload(&page, &spec, &big()).await, Err(RuntimeStoreError::Conflict(_))));
    }
    let again = s.begin_slot_upload(&page, &slot_spec("clip.mp4", size, Some(&hash), ends), &big()).await.unwrap();
    assert_eq!(again, BeginUpload::AlreadyStored { key }, "the same file carries on");
}

/// A run-scoped claim holds its slot's upload while it copies it into the
/// run: the slot ending meanwhile waits for the claim, which lands the
/// file, and the upload it came from goes.
#[weft_db_macros::test]
async fn a_slot_ending_while_a_run_claim_copies_waits_for_it(pool: Pool) {
    let (s, held, bucket, clock) = store_with_held_copies(&pool).await;
    let ends = clock.now_unix() + 100;
    let (upload, _) = slot_upload_via(&s, &bucket, "s1", ends, MP4, None).await;
    let claims = slot_claims("s1", StorageScope::Run, ends);
    let run = worker("t1", "p1", Some("c1"));
    let (entered, release) = held.hold_next_copy(CopyHold::After);
    let meanwhile = async {
        entered.await.unwrap();
        clock.advance(Duration::from_secs(101));
        let expiry = tokio::spawn({
            let s = s.clone();
            async move { s.expire().await.map(|_| ()) }
        });
        release.send(()).unwrap();
        expiry.await.unwrap().unwrap();
    };
    let video = fits("Video");
    let (claimed, ()) = tokio::join!(held.running(claim_one(&s, &run, &claims, &video)), meanwhile);
    let file = claimed.unwrap();
    assert_eq!(bucket.get(&object_key(&file.key)).await.unwrap().as_deref(), Some(MP4));
    assert_eq!(status_of(&pool, &upload).await, None, "the upload it came from went");
}

/// A run-scoped claim whose copy was taken for dead (no piece landed for a
/// whole lease, and the expiry loop ended it) is refused, and the copy it
/// then lands is deleted, even while the ended copy's row is still being
/// removed: the slot's upload stays, and a call again receives it into a
/// copy of its own at once.
#[weft_db_macros::test]
async fn a_run_claim_whose_copy_was_ended_leaves_nothing_and_the_slot_open(pool: Pool) {
    let (s, held, bucket, clock) = store_with_held_copies(&pool).await;
    let ends = clock.now_unix() + 3600;
    let (upload, _) = slot_upload_via(&s, &bucket, "s1", ends, MP4, None).await;
    let claims = slot_claims("s1", StorageScope::Run, ends);
    let run = worker("t1", "p1", Some("c1"));
    let (entered, release) = held.hold_next_copy(CopyHold::Before);
    let target = std::sync::Mutex::new(String::new());
    let meanwhile = async {
        entered.await.unwrap();
        let copy = copy_of(&pool, &upload).await;
        assert_eq!(status_of(&pool, &copy).await.as_deref(), Some("completing"), "the run's copy is claimed while it is made");
        // The ended copy's reap fails, so its row stays 'reaping', marked,
        // when the copy lands.
        bucket.fail_next_deletes(&object_key(&copy), 1);
        clock.advance(Duration::from_secs(CLAIM_LEASE_SECS as u64 + 1));
        s.expire().await.unwrap();
        assert_eq!(status_of(&pool, &copy).await.as_deref(), Some("reaping"));
        *target.lock().unwrap() = copy;
        release.send(()).unwrap();
    };
    let video = fits("Video");
    let (claimed, ()) = tokio::join!(held.running(claim_one(&s, &run, &claims, &video)), meanwhile);
    let target = target.into_inner().unwrap();
    assert!(claimed.is_err(), "{claimed:?}");
    assert_eq!(bucket.keys(), vec![object_key(&upload)], "the copy it landed was deleted");
    assert_eq!(status_of(&pool, &upload).await.as_deref(), Some("uploaded"), "the slot is still open");
    let received = claim_one(&s, &run, &claims, &video).await.unwrap();
    assert_ne!(received.key, target, "a copy of its own, while the ended one's row is still being removed");
    assert!(received.key.starts_with("t1/run/c1/"), "{}", received.key);
    assert_eq!(bucket.get(&object_key(&received.key)).await.unwrap().as_deref(), Some(MP4));
    delete_run_files(&s, "t1", "c1").await;
    assert_eq!(status_of(&pool, &target).await, None, "deleting the run's files removed the ended copy's marked row");
}

/// The key of the row of a run's copy of `upload` being made.
async fn copy_of(pool: &Pool, upload: &str) -> String {
    pool.scalar("SELECT key FROM runtime_file WHERE receives = $1", &[&upload]).await.unwrap()
}

/// A run-scoped claim whose broker died mid-copy leaves the row of its
/// copy, which holds the slot's upload. The expiry loop books a look at
/// the copy's lapse and, with no tenant call at all, ends the copy then;
/// the upload, its slot over, goes in the same pass, and nothing is left.
#[weft_db_macros::test]
async fn a_run_copy_whose_claim_died_is_ended_by_the_expiry_loop(pool: Pool) {
    let (s, held, bucket, clock) = store_with_held_copies(&pool).await;
    let ends = clock.now_unix() + 100;
    let (upload, _) = slot_upload_via(&s, &bucket, "s1", ends, MP4, None).await;
    let claims = slot_claims("s1", StorageScope::Run, ends);
    let run = worker("t1", "p1", Some("c1"));
    let video = fits("Video");
    let (entered, _release) = held.hold_next_copy(CopyHold::Before);
    tokio::select! {
        _ = claim_one(&s, &run, &claims, &video) => panic!("the copy is held"),
        _ = entered => {}
    }
    // The claim was dropped mid-copy, as when its broker dies.
    let copy = copy_of(&pool, &upload).await;
    clock.advance(Duration::from_secs(101));
    assert_eq!(
        s.expire().await.unwrap(),
        weft_task_store::drain::DrainStep::RetryIn(Duration::from_secs(CLAIM_LEASE_SECS as u64 - 100)),
        "the copy holds the expired upload, and the look is booked at its claim's lapse"
    );
    assert_eq!(status_of(&pool, &upload).await.as_deref(), Some("uploaded"));
    assert_eq!(status_of(&pool, &copy).await.as_deref(), Some("completing"));
    clock.advance(Duration::from_secs(CLAIM_LEASE_SECS as u64));
    assert_eq!(expire(&s, &pool).await, 2, "the copy and the upload it held");
    assert!(bucket.is_empty(), "{:?}", bucket.keys());
}

/// An upload landing while the claim copies another slot's upload, after
/// the claim looked at it, is copied in a second round of the same call:
/// the claim receives both, and never answers to call again.
#[weft_db_macros::test]
async fn a_run_claim_receives_an_upload_that_landed_while_it_copied(pool: Pool) {
    let (s, held, bucket, clock) = store_with_held_copies(&pool).await;
    let ends = clock.now_unix() + 3600;
    let (first, _) = slot_upload_via(&s, &bucket, "s1", ends, MP4, None).await;
    let page = worker("t1", "p1", None);
    let spec = UploadSpec {
        scope: &StorageScope::Project,
        mime: "video/mp4",
        filename: "clip.mp4",
        keep: None,
        declared_size: Some(MP4.len() as u64),
        content_hash: None,
        identity: None,
        slot: Some(weft_broker::runtime_store::SlotUpload { sid: "s2", ends_unix: ends, max_bytes: 1 << 20, accepts: "Video", sha256: None }),
        imported: None,
    };
    let BeginUpload::Ready { key: second, part_size } = s.begin_slot_upload(&page, &spec, &big()).await.unwrap() else {
        panic!("a fresh slot begins");
    };
    upload_parts(&s, &bucket, &page, &second, part_size, &body(MP4), &big()).await.unwrap();
    let (one, two) = (slot_claims("s1", StorageScope::Run, ends), slot_claims("s2", StorageScope::Run, ends));
    let video = fits("Video");
    let named = [
        weft_broker::runtime_store::ClaimedSlot { slot: "s2", claims: &two, fits: &video },
        weft_broker::runtime_store::ClaimedSlot { slot: "s1", claims: &one, fits: &video },
    ];
    let run = worker("t1", "p1", Some("c1"));
    let (entered, release) = held.hold_next_copy(CopyHold::Before);
    let meanwhile = async {
        entered.await.unwrap();
        s.complete_upload(&page, &second).await.unwrap();
        release.send(()).unwrap();
    };
    let (claimed, ()) = tokio::join!(held.running(s.claim_uploads(&run, &named)), meanwhile);
    let files = claimed.unwrap();
    assert_eq!(files.len(), 2);
    for file in &files {
        assert!(file.key.starts_with("t1/run/c1/"), "{}", file.key);
        assert_eq!(bucket.get(&object_key(&file.key)).await.unwrap().as_deref(), Some(MP4));
    }
    assert_eq!((status_of(&pool, &first).await, status_of(&pool, &second).await), (None, None), "both uploads went");
}

/// Two calls of one run receiving a run-scoped slot at once each make a
/// copy of their own: the first to commit receives the slot, the other
/// answers that same file (its copy failed as the upload went under it)
/// and removes its own copy. Neither fails.
#[weft_db_macros::test]
async fn a_run_claiming_a_slot_twice_at_once_gets_one_file(pool: Pool) {
    let (s, held, bucket, clock) = store_with_held_copies(&pool).await;
    let ends = clock.now_unix() + 3600;
    let (upload, _) = slot_upload_via(&s, &bucket, "s1", ends, MP4, None).await;
    let claims = slot_claims("s1", StorageScope::Run, ends);
    let run = worker("t1", "p1", Some("c1"));
    let video = fits("Video");
    let (entered, release) = held.hold_next_copy(CopyHold::Before);
    let meanwhile = async {
        entered.await.unwrap();
        let file = claim_one(&s, &run, &claims, &video).await.unwrap();
        release.send(()).unwrap();
        file
    };
    let (held_claim, first) = tokio::join!(held.running(claim_one(&s, &run, &claims, &video)), meanwhile);
    assert_eq!(held_claim.unwrap().key, first.key, "the same file");
    assert_eq!(bucket.keys(), vec![object_key(&first.key)], "only the received file's object");
    let rows: Vec<String> = pool.scalars("SELECT key FROM runtime_file", &[]).await.unwrap();
    assert_eq!(rows, vec![first.key.clone()], "only the received file's row");
    assert_eq!(status_of(&pool, &upload).await, None);
}

/// A reap the bucket refuses for good leaves its row marked: still
/// charged and shown in `weft status`, and with no look booked for it, so
/// the expiry loop never wakes for it, however often a pass runs.
#[weft_db_macros::test]
async fn a_marked_reap_that_keeps_failing_books_no_look(pool: Pool) {
    let (s, bucket, clock) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let f = put_via(&s, &bucket, &w, &StorageScope::Project, "b", "f", None, &big(), body(b"xyz")).await.unwrap();
    bucket.fail_next_deletes(&object_key(&f.key), 1000);
    assert!(s.delete(&weft_core::storage::key::parse_key(&f.key).unwrap()).await.is_err(), "the bucket refused");
    for _ in 0..3 {
        assert_eq!(s.expire().await.unwrap(), weft_task_store::drain::DrainStep::Done, "nothing booked for it");
        clock.advance(Duration::from_secs(10 * CLAIM_LEASE_SECS as u64));
    }
    assert_eq!(status_of(&pool, &f.key).await.as_deref(), Some("reaping"));
    assert_eq!(s.tenant_usage("t1").await.unwrap(), (1, 3), "still charged while it waits");
}

/// A begin refused for space first retries its tenant's marked reaps,
/// once each: one the bucket now removes frees the space and the begin
/// goes through; one it still refuses leaves the begin refused, tried
/// only once.
#[weft_db_macros::test]
async fn a_begin_over_quota_retries_a_marked_reap_once(pool: Pool) {
    let (s, bucket, _clock) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let tight = budget(Entitlement::from_disk_bytes(5));
    let f = put_via(&s, &bucket, &w, &StorageScope::Project, "b", "f", None, &tight, body(b"xyz")).await.unwrap();
    // The delete fails, and so does the first begin's retry of it.
    bucket.fail_next_deletes(&object_key(&f.key), 2);
    assert!(s.delete(&weft_core::storage::key::parse_key(&f.key).unwrap()).await.is_err(), "the bucket refused");
    let refused = begin_via(&s, &w, &StorageScope::Project, "b", "g", None, &tight, Some(3)).await;
    assert!(matches!(refused, Err(RuntimeStoreError::QuotaExceeded(_))), "{refused:?}");
    assert_eq!(status_of(&pool, &f.key).await.as_deref(), Some("reaping"), "tried once, still marked");
    // The bucket works again: the next begin short of space frees it.
    begin_via(&s, &w, &StorageScope::Project, "b", "g", None, &tight, Some(3)).await.unwrap();
    assert_eq!(status_of(&pool, &f.key).await, None, "the marked reap went");
    assert!(!bucket.keys().contains(&object_key(&f.key)));
}

/// A wipe takes a marked row at once, goes on past a row whose reap the
/// bucket refuses, and names that row (with the bucket's words) apart from
/// one another call is removing right now.
#[weft_db_macros::test]
async fn a_wipe_goes_past_a_failing_reap_and_names_it(pool: Pool) {
    let (s, bucket, clock) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let file = |name: &'static str| {
        let (s, bucket, w) = (s.clone(), bucket.clone(), w.clone());
        async move { put_via(&s, &bucket, &w, &StorageScope::Project, "b", name, None, &big(), body(b"xyz")).await.unwrap() }
    };
    let (marked, refusing, fine, live) = (file("marked").await, file("refusing").await, file("fine").await, file("live").await);
    bucket.fail_next_deletes(&object_key(&marked.key), 1);
    assert!(s.delete(&weft_core::storage::key::parse_key(&marked.key).unwrap()).await.is_err(), "the bucket refused");
    bucket.fail_next_deletes(&object_key(&refusing.key), 1);
    pool.execute("UPDATE runtime_file SET status = 'reaping', progressed_at_unix = $2 WHERE key = $1", &[&live.key, &clock.now_unix()]).await.unwrap();
    let wiped = s.wipe_prefix("t1/").await.expect_err("two rows are left");
    assert!(matches!(wiped, RuntimeStoreError::RemovalIncomplete(_)), "{wiped:?}");
    let wiped = format!("{wiped:#}");
    assert!(wiped.contains("wiped 2 files"), "{wiped}");
    assert!(wiped.contains("1 the storage bucket refused to remove") && wiped.contains(&refusing.key), "{wiped}");
    assert!(!wiped.contains("injected delete failure"), "the bucket's words stay off the answer: {wiped}");
    let why: Option<String> =
        pool.scalar("SELECT reap_error FROM runtime_file WHERE key = $1", &[&refusing.key]).await.unwrap();
    assert!(why.is_some_and(|w| w.contains("injected delete failure")), "kept on the row");
    assert!(wiped.contains("1 another removal in progress holds"), "{wiped}");
    assert_eq!((status_of(&pool, &marked.key).await, status_of(&pool, &fine.key).await), (None, None));
    assert_eq!(status_of(&pool, &refusing.key).await.as_deref(), Some("reaping"), "left marked");
    assert_eq!(status_of(&pool, &live.key).await.as_deref(), Some("reaping"), "the live one is its call's");
}

/// A claim copying several run-scoped slots holds one lease for all its
/// copies: a small copy finished first is renewed while a long one after
/// it runs, so the expiry loop never ends it, and the claim receives both.
#[weft_db_macros::test]
async fn a_run_claim_of_several_slots_holds_every_copy_while_one_runs(pool: Pool) {
    let (s, held, bucket, clock) = store_with_held_copies(&pool).await;
    let ends = clock.now_unix() + 3600;
    slot_upload_via(&s, &bucket, "s1", ends, MP4, None).await;
    slot_upload_via(&s, &bucket, "s2", ends, MP4, None).await;
    let (one, two) = (slot_claims("s1", StorageScope::Run, ends), slot_claims("s2", StorageScope::Run, ends));
    let video = fits("Video");
    let named = [
        weft_broker::runtime_store::ClaimedSlot { slot: "s1", claims: &one, fits: &video },
        weft_broker::runtime_store::ClaimedSlot { slot: "s2", claims: &two, fits: &video },
    ];
    let run = worker("t1", "p1", Some("c1"));
    let (first_started, first_go) = held.hold_next_copy(CopyHold::Before);
    let meanwhile = async {
        first_started.await.unwrap();
        // The second copy's pauses, taken by it when it starts.
        let (second_started, second_go) = held.hold_next_copy(CopyHold::Before);
        let (second_reported, second_done) = held.hold_next_copy(CopyHold::Reported);
        first_go.send(()).unwrap();
        second_started.await.unwrap();
        clock.advance(Duration::from_secs(CLAIM_LEASE_SECS as u64));
        second_go.send(()).unwrap();
        second_reported.await.unwrap();
        // Past the first copy's lease from when it finished, within the one
        // the second copy's report renewed for both.
        clock.advance(Duration::from_secs(CLAIM_LEASE_SECS as u64 - 1));
        s.expire().await.unwrap();
        let copying: i64 =
            pool.scalar("SELECT COUNT(*) FROM runtime_file WHERE status = 'completing'", &[]).await.unwrap();
        assert_eq!(copying, 2, "both copies are still held");
        second_done.send(()).unwrap();
    };
    let (claimed, ()) = tokio::join!(held.running(s.claim_uploads(&run, &named)), meanwhile);
    let files = claimed.unwrap();
    assert_eq!(files.len(), 2);
    for file in &files {
        assert_eq!(bucket.get(&object_key(&file.key)).await.unwrap().as_deref(), Some(MP4));
    }
}

/// Resuming an upload signs fresh part links, and a PUT may start on them
/// until they die: the resume is progress, so the upload is not abandoned
/// a lapse after its last reservation but a lapse after the resume.
#[weft_db_macros::test]
async fn a_resume_defers_the_abandoned_reap(pool: Pool) {
    use weft_broker::runtime_store::PENDING_RESERVE_GRACE_SECS;
    let lapse = PART_LINK_SECS + PENDING_RESERVE_GRACE_SECS as u64;
    let (s, _bucket, clock) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let (key, _) = begin_via(&s, &w, &StorageScope::Project, "b", "f", None, &big(), Some(6)).await.unwrap();
    s.reserve_parts(&w, &key, &[ask(1, 6)], &big(), &WORKER).await.unwrap();
    clock.advance(Duration::from_secs(lapse - 10));
    let (_, missing, _) = s.resume_upload(&w, &key, &big(), &WORKER).await.unwrap();
    assert_eq!(missing.len(), 1, "the part is signed again");
    clock.advance(Duration::from_secs(20));
    assert_eq!(expire(&s, &pool).await, 0, "the resume's links are still live");
    assert_eq!(status_of(&pool, &key).await.as_deref(), Some("pending"));
    clock.advance(Duration::from_secs(lapse));
    assert_eq!(expire(&s, &pool).await, 1, "abandoned a lapse after the resume");
}

/// A delete the bucket refuses says so in its own kind of answer (the
/// file is gone from listings, marked in `weft status`), and deleting the
/// same key again tries its removal again.
#[weft_db_macros::test]
async fn a_delete_the_bucket_refuses_says_so_and_deleting_again_retries(pool: Pool) {
    let (s, bucket, _clock) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let f = put_via(&s, &bucket, &w, &StorageScope::Project, "b", "f", None, &big(), body(b"xyz")).await.unwrap();
    let parsed = weft_core::storage::key::parse_key(&f.key).unwrap();
    bucket.fail_next_deletes(&object_key(&f.key), 1);
    let refused = s.delete(&parsed).await;
    assert!(matches!(&refused, Err(RuntimeStoreError::RemovalIncomplete(m)) if m.contains(&f.key)), "{refused:?}");
    assert_eq!(status_of(&pool, &f.key).await.as_deref(), Some("reaping"));
    s.delete(&parsed).await.unwrap();
    assert_eq!(status_of(&pool, &f.key).await, None);
    assert!(bucket.is_empty());
}

/// A begin short of space retries its tenant's marked reaps in order and
/// stops at the first the bucket still refuses: the rest are left for a
/// later need, untried.
#[weft_db_macros::test]
async fn a_begin_over_quota_stops_at_the_first_refused_marked_reap(pool: Pool) {
    let (s, bucket, _clock) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let tight = budget(Entitlement { disk_bytes_cap: 7, file_cap: 10 });
    let a = put_via(&s, &bucket, &w, &StorageScope::Project, "b", "a", None, &tight, body(b"xyz")).await.unwrap();
    let b = put_via(&s, &bucket, &w, &StorageScope::Project, "b", "b", None, &tight, body(b"xyz")).await.unwrap();
    let (first, second) = if a.key < b.key { (a, b) } else { (b, a) };
    // The first refuses its delete and the retry; the second only its delete.
    bucket.fail_next_deletes(&object_key(&first.key), 2);
    bucket.fail_next_deletes(&object_key(&second.key), 1);
    for f in [&first, &second] {
        assert!(s.delete(&weft_core::storage::key::parse_key(&f.key).unwrap()).await.is_err(), "the bucket refused");
    }
    let refused = begin_via(&s, &w, &StorageScope::Project, "b", "c", None, &tight, Some(3)).await;
    assert!(matches!(refused, Err(RuntimeStoreError::QuotaExceeded(_))), "{refused:?}");
    assert_eq!(status_of(&pool, &first.key).await.as_deref(), Some("reaping"), "refused again");
    assert_eq!(status_of(&pool, &second.key).await.as_deref(), Some("reaping"), "never tried");
    assert!(bucket.keys().contains(&object_key(&second.key)), "its object was never touched");
}

/// Deleting a run's files names a row the bucket refuses (the row stays
/// marked, the one record of it in `weft status`), and deleting them again
/// retries it.
#[weft_db_macros::test]
async fn a_refused_row_of_a_runs_files_is_named_and_retried(pool: Pool) {
    let (s, bucket, _clock) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let (key, _) = begin_via(&s, &w, &StorageScope::Run, "b", "f", None, &big(), Some(6)).await.unwrap();
    let part = &s.reserve_parts(&w, &key, &[ask(1, 6)], &big(), &WORKER).await.unwrap()[0];
    bucket.put_part(&part.url, body(b"orphan")).unwrap();
    bucket.fail_next_deletes(&object_key(&key), 1);
    let refused = s.delete_run_files("t1", &["c1".to_string()]).await.unwrap_err();
    assert!(
        matches!(&refused, RuntimeStoreError::RemovalIncomplete(why) if why.contains(&key)),
        "{refused:?}"
    );
    assert_eq!(status_of(&pool, &key).await.as_deref(), Some("reaping"), "left marked");
    let shown: Option<String> =
        pool.scalar_opt("SELECT error FROM unanswered_call WHERE callee = 'reaps:'", &[]).await.unwrap();
    assert!(shown.is_some_and(|e| e.contains(&key)), "the one record of it");
    assert_eq!(delete_run_files(&s, "t1", "c1").await, 1, "retried and removed");
    assert_eq!(status_of(&pool, &key).await, None);
}

/// A pass over many rows that marks a row and then meets a database error
/// still writes the marked row down for `weft status` before it answers
/// the error: a marked row books no look, so nothing else would soon.
#[weft_db_macros::test]
async fn a_pass_that_marks_then_fails_still_records_the_mark(pool: Pool) {
    let (s, bucket, _clock) = store(&pool).await;
    let w = worker("t1", "p1", Some("c1"));
    let a = put_via(&s, &bucket, &w, &StorageScope::Project, "b", "a", None, &big(), body(b"xyz")).await.unwrap();
    let b = put_via(&s, &bucket, &w, &StorageScope::Project, "b", "b", None, &big(), body(b"xyz")).await.unwrap();
    // The wipe goes in key order: the first row's reap is refused (marked),
    // then fencing the second meets a database error.
    let (first, second) = if a.key < b.key { (a, b) } else { (b, a) };
    bucket.fail_next_deletes(&object_key(&first.key), 1);
    pool.batch_execute(
        "CREATE FUNCTION weft_test_refuse() RETURNS trigger AS $$ BEGIN RAISE EXCEPTION 'staged database error'; END $$ \
         LANGUAGE plpgsql",
    )
    .await
    .unwrap();
    pool.batch_execute(&format!(
        "CREATE TRIGGER weft_test_refuse BEFORE UPDATE ON runtime_file FOR EACH ROW \
         WHEN (NEW.key = '{}' AND NEW.status = 'reaping') EXECUTE FUNCTION weft_test_refuse()",
        second.key
    ))
    .await
    .unwrap();
    let wiped = s.wipe_prefix("t1/").await;
    assert!(matches!(wiped, Err(RuntimeStoreError::Other(_))), "{wiped:?}");
    assert_eq!(status_of(&pool, &first.key).await.as_deref(), Some("reaping"), "marked");
    let shown: Option<String> =
        pool.scalar_opt("SELECT error FROM unanswered_call WHERE callee = 'reaps:'", &[]).await.unwrap();
    assert!(shown.is_some_and(|e| e.contains(&first.key)), "the mark is written down");
}

/// A file copied in from another install keeps its key (under this
/// tenant), its version and its birth; the same copy begun again is
/// already there; anything else under the key is refused. Only the
/// tenant's copying caller may name a key like that.
#[weft_db_macros::test]
async fn a_copied_file_keeps_its_key_version_and_birth(pool: Pool) {
    use weft_broker::runtime_store::ImportedFile;
    let (s, bucket, _clock) = store(&pool).await;
    let copying = CallerAuth::Copying { tenant: "t2".into() };
    let key = weft_core::storage::key::parse_key("t1/project/p1/0b1c").unwrap();
    let here = weft_core::storage::key::ParsedKey { tenant: "t2".into(), ..key };
    let bytes = body(b"the notes");
    let spec = |version, size| UploadSpec {
        scope: &StorageScope::Project,
        mime: "text/plain",
        filename: "notes.txt",
        keep: None,
        declared_size: Some(size),
        content_hash: None,
        identity: None,
        slot: None,
        imported: Some(ImportedFile { key: &here, version, created_at_unix: 1_700_000_000 }),
    };

    let refused = s.begin_upload(&worker("t2", "p1", None), &spec(3, bytes.len() as u64), &big()).await.unwrap_err();
    assert!(matches!(refused, RuntimeStoreError::Denied(_)), "{refused:?}");
    let BeginUpload::Ready { key, part_size } = s.begin_upload(&copying, &spec(3, bytes.len() as u64), &big()).await.unwrap() else {
        panic!("a new copy is uploaded");
    };
    assert_eq!(key, "t2/project/p1/0b1c");
    upload_parts(&s, &bucket, &copying, &key, part_size, &bytes, &big()).await.unwrap();
    let meta = s.complete_upload(&copying, &key).await.unwrap();
    assert_eq!((meta.version, meta.created_at_unix, meta.size_bytes), (3, 1_700_000_000, bytes.len() as u64));

    let again = s.begin_upload(&copying, &spec(3, bytes.len() as u64), &big()).await.unwrap();
    assert!(matches!(again, BeginUpload::AlreadyStored { key: k } if k == key), "the same copy is already there");
    let other = s.begin_upload(&copying, &spec(4, bytes.len() as u64), &big()).await.unwrap_err();
    assert!(matches!(&other, RuntimeStoreError::Conflict(why) if why.contains("weft files rm")), "{other:?}");
    assert!(bucket.in_progress_uploads().is_empty(), "a refused begin leaves no upload open");
}

// ---------- a busy database before the first write ----------

/// A store on `pool`'s database whose one connection the test can hold,
/// on the same bucket and clock: a call through it that finds the
/// connection taken gets none within a second.
async fn on_one_connection(pool: &Pool, bucket: &Arc<FakeObjectStore>, clock: &Arc<FakeClock>) -> (RuntimeStore, Pool) {
    let one = Pool::open(pool.config().clone(), 1, Duration::from_secs(1), weft_task_store::db::Waits::Expected);
    (RuntimeStore::new(one.clone(), bucket.clone(), clock.clone()), one)
}

/// A claim of a page's upload that cannot get its first connection has
/// done nothing, and says so (sent again, it receives the upload); so
/// does an upload under an identity that cannot look for an expired file
/// of it first.
#[weft_db_macros::test]
async fn a_claim_or_an_identity_upload_with_no_first_connection_says_nothing_was_done(pool: Pool) {
    let (s, bucket, clock) = store(&pool).await;
    let ends = clock.now_unix() + 3600;
    let (upload, _) = slot_upload_via(&s, &bucket, "s1", ends, MP4, None).await;
    let into_run = slot_claims("s1", StorageScope::Run, ends);
    let video = fits("Video");
    let run = worker("t1", "p1", Some("c1"));
    let (busy, one) = on_one_connection(&pool, &bucket, &clock).await;

    let taken = one.get().await.unwrap();
    let refused = claim_one(&busy, &run, &into_run, &video).await;
    assert!(matches!(refused, Err(RuntimeStoreError::BeforeWrites(_))), "{refused:?}");
    let spec = UploadSpec {
        scope: &StorageScope::Project,
        mime: "b",
        filename: "f",
        keep: None,
        declared_size: Some(3),
        content_hash: None,
        identity: Some("source:1"),
        slot: None,
        imported: None,
    };
    let begun = busy.begin_upload(&run, &spec, &big()).await;
    assert!(matches!(begun, Err(RuntimeStoreError::BeforeWrites(_))), "{begun:?}");
    assert_eq!(status_of(&pool, &upload).await.as_deref(), Some("uploaded"), "the slot is still open");
    assert_eq!(bucket.keys(), vec![object_key(&upload)], "no copy was made");

    drop(taken);
    let received = claim_one(&busy, &run, &into_run, &video).await.unwrap();
    assert_ne!(received.key, upload, "sent again, it receives the run's own copy");
}
